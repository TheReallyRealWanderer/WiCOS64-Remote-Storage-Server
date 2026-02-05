package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"wicos64-server/internal/config"
	"wicos64-server/internal/diskimage"
	"wicos64-server/internal/fsops"
	"wicos64-server/internal/pathutil"
	"wicos64-server/internal/proto"
	"wicos64-server/internal/version"
)

type Server struct {
	cfgMu   sync.RWMutex
	cfg     config.Config
	cfgPath string

	// initOncePerRoot tracks roots we already initialized with recommended dirs.
	inited sync.Map // map[string]struct{}

	// global write lock to optionally return BUSY instead of blocking.
	writeMu sync.Mutex

	// recent request logs for the admin UI.
	logs *logHub

	// optional caches/metrics for QoL features
	usage *usageCache
	stats *statsHub

	// LAN discovery responder (UDP, WDP1)
	discOnce sync.Once
}

func New(cfg config.Config, cfgPath string) *Server {
	s := &Server{
		cfg:     cfg,
		cfgPath: cfgPath,
		logs:    newLogHub(1024),
		usage:   newUsageCache(3 * time.Second),
		stats:   newStatsHub(),
	}
	s.startMaintenanceLoop()
	s.StartDiscovery()
	return s
}

func (s *Server) cfgSnapshot() config.Config {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg
}

func (s *Server) setCfg(cfg config.Config) {
	s.cfgMu.Lock()
	s.cfg = cfg
	s.cfgMu.Unlock()
}

func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	cfg := s.cfgSnapshot()
	mux.HandleFunc(cfg.Endpoint, s.handleRPC)
	// Optional LAN-only bootstrap helper (API URL + token per WiC64 MAC).
	mux.HandleFunc("/wicos64/bootstrap", s.handleBootstrap)
	s.mountAdmin(mux)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// Lightweight health endpoint.
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("wicos64-go-backend " + version.Get().String() + "\n"))
	})
	return mux
}

func (s *Server) handleRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	startTime := time.Now()
	cfg := s.cfgSnapshot()
	remoteIP := clientIP(r)

	// Log entry (filled progressively). We only log if cfg.LogRequests is true.
	var le LogEntry
	le.TimeUnixMs = startTime.UnixMilli()
	le.RemoteIP = remoteIP
	le.HTTPStatus = 200

	// Avoid (proxy) response transforms that could break the binary protocol.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-transform")
	w.Header().Set("Content-Encoding", "identity")

	// Enforce an upper bound (header + payload). If the client sends more, we still
	// try to produce a W64F response if we already have >=10 bytes (spec).
	maxRead := int64(proto.HeaderSize) + int64(cfg.MaxPayload)
	r.Body = http.MaxBytesReader(w, r.Body, maxRead)
	body, readErr := io.ReadAll(r.Body)
	_ = r.Body.Close()
	le.ReqBytes = len(body)
	ct := r.Header.Get("Content-Type")
	// For debugging: record a short body prefix for parse errors (without leaking tokens).
	if len(body) > 0 {
		pfx := body
		if len(pfx) > 16 {
			pfx = pfx[:16]
		}
		le.Info = fmt.Sprintf("ct=%s head=%x", ct, pfx)
	}

	if len(body) < proto.HeaderSize {
		// Spec allows HTTP 4xx/5xx if we cannot build a W64F response (e.g. body < 10).
		w.WriteHeader(http.StatusBadRequest)
		le.HTTPStatus = http.StatusBadRequest
		le.OpName = "<short>"
		le.StatusName = "HTTP_400"
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}

	// Parse header (magic, version, payload_len). If the magic is bad, attempt to
	// unwrap WiC64-style HTTP POST bodies that embed binary data in a field named "data".
	// This keeps the server compatible with both raw octet-stream and form/multipart posts.
	hdr, magicOK, hdrErr := proto.ParseReqHeader(body)
	if hdrErr != nil {
		if unwrapped, uwInfo, ok := tryUnwrapW64FBody(body, ct); ok {
			body = unwrapped
			le.ReqBytes = len(body)
			if uwInfo != "" {
				// Append unwrap info for easier troubleshooting.
				le.Info = strings.TrimSpace(le.Info + " " + uwInfo)
			}
			hdr, magicOK, hdrErr = proto.ParseReqHeader(body)
		}
	}
	opEcho := byte(0xFF)
	versionEcho := byte(proto.Version)
	if magicOK {
		opEcho = hdr.Op
		versionEcho = hdr.Version
	}
	le.Op = opEcho
	le.OpName = opName(opEcho)

	// If reading failed (e.g. request too large), respond with TOO_LARGE.
	if readErr != nil {
		status := proto.StatusTooLarge
		errMsg := "request body too large"
		le.Status = status
		le.StatusName = statusName(status)
		le.RespPreview = buildRespPreview(cfg, opEcho, status, nil, errMsg)
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, errMsg)
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}

	if hdrErr != nil {
		// Header parse error (bad magic) – still respond with BAD_REQUEST and op_echo=0xFF.
		status := proto.StatusBadRequest
		le.Status = status
		le.StatusName = statusName(status)
		le.RespPreview = buildRespPreview(cfg, opEcho, status, nil, hdrErr.Error())
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, hdrErr.Error())
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}

	// Validate fixed header constraints.
	if hdr.Reserved != 0 {
		status := proto.StatusBadRequest
		le.Status = status
		le.StatusName = statusName(status)
		le.RespPreview = buildRespPreview(cfg, opEcho, status, nil, "reserved must be 0")
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, "reserved must be 0")
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}

	if hdr.Version != proto.Version {
		status := proto.StatusNotSupported
		le.Status = status
		le.StatusName = statusName(status)
		msg := fmt.Sprintf("unsupported rpc version %d", hdr.Version)
		le.RespPreview = buildRespPreview(cfg, opEcho, status, nil, msg)
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, msg)
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}

	// Validate payload length.
	//
	// Some WiC64 firmware / stacks (notably on real hardware vs emulator setups) may deliver
	// one or more trailing bytes around multipart/form extraction (e.g. leftover CR/LF).
	// The RPC itself is self-delimiting via hdr.PayloadLen, so if we already have at least
	// the required amount of bytes we can safely ignore any extra tail bytes.
	expectedTotal := proto.HeaderSize + int(hdr.PayloadLen)
	if len(body) < expectedTotal {
		// Not enough bytes for the declared payload -> BAD_REQUEST.
		status := proto.StatusBadRequest
		le.Status = status
		le.StatusName = statusName(status)
		le.RespPreview = buildRespPreview(cfg, opEcho, status, nil, "payload_len mismatch")
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, "payload_len mismatch")
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}
	trimmed := 0
	if len(body) > expectedTotal {
		trimmed = len(body) - expectedTotal
		body = body[:expectedTotal]
		le.ReqBytes = len(body)
	}
	if hdr.PayloadLen > cfg.MaxPayload {
		status := proto.StatusTooLarge
		le.Status = status
		le.StatusName = statusName(status)
		le.RespPreview = buildRespPreview(cfg, opEcho, status, nil, "payload too large")
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, "payload too large")
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}

	payload := body[proto.HeaderSize:]

	le.Info = summarizeRequest(cfg, hdr.Op, hdr.Flags, payload)
	if trimmed > 0 {
		le.Info = strings.TrimSpace(le.Info + fmt.Sprintf(" trim=%d", trimmed))
	}
	le.ReqPreview = buildReqPreview(cfg, hdr.Op, hdr.Flags, payload)

	// Resolve token -> root (sandbox). Token is passed via query parameter.
	token := r.URL.Query().Get("token")
	ctx, ok := cfg.ResolveTokenContext(token)
	if !ok {
		status := proto.StatusAccessDenied
		le.Status = status
		le.StatusName = statusName(status)
		le.RespPreview = buildRespPreview(cfg, hdr.Op, status, nil, "access denied")
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, "access denied")
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}
	rootAbs, err := filepath.Abs(ctx.Root)
	if err != nil {
		status := proto.StatusInternal
		le.Status = status
		le.StatusName = statusName(status)
		le.RespPreview = buildRespPreview(cfg, hdr.Op, status, nil, "bad root")
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, "bad root")
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}
	if err := config.EnsureRoot(rootAbs); err != nil {
		status := proto.StatusInternal
		le.Status = status
		le.StatusName = statusName(status)
		le.RespPreview = buildRespPreview(cfg, hdr.Op, status, nil, "cannot create root")
		le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, nil, "cannot create root")
		le.DurationMs = time.Since(startTime).Milliseconds()
		s.record(cfg, le)
		return
	}
	_ = s.ensureRecommendedDirs(cfg, rootAbs)

	// (request summary already computed above)

	limits := Limits{ReadOnly: ctx.ReadOnly, QuotaBytes: ctx.QuotaBytes, MaxFileBytes: ctx.MaxFileBytes, DiskImagesEnabled: ctx.DiskImagesEnabled, DiskImagesWriteEnabled: ctx.DiskImagesWriteEnabled, DiskImagesAutoResizeEnabled: ctx.DiskImagesAutoResizeEnabled, DiskImagesAllowRenameConvert: ctx.DiskImagesAllowRenameConvert}

	status, respPayload, errMsg := s.dispatch(cfg, limits, hdr.Op, hdr.Flags, payload, rootAbs)
	le.RespPreview = buildRespPreview(cfg, hdr.Op, status, respPayload, errMsg)
	le.Status = status
	le.StatusName = statusName(status)
	le.RespBytes = s.writeResponse(w, cfg, versionEcho, opEcho, status, respPayload, errMsg)
	le.DurationMs = time.Since(startTime).Milliseconds()
	s.record(cfg, le)
}

func (s *Server) ensureRecommendedDirs(cfg config.Config, rootAbs string) error {
	if !cfg.CreateRecommendedDirs {
		return nil
	}
	if _, loaded := s.inited.LoadOrStore(rootAbs, struct{}{}); loaded {
		return nil
	}
	// Recommended layout (server MAY create at first login).
	dirs := []string{"BIN", "USR", "ETC", ".TMP"}
	if cfg.TrashEnabled {
		td := strings.TrimSpace(cfg.TrashDir)
		if td == "" {
			td = ".TRASH"
		}
		if !strings.EqualFold(td, ".TMP") {
			dirs = append(dirs, td)
		}
	}
	for _, dir := range dirs {
		_ = os.MkdirAll(filepath.Join(rootAbs, dir), 0o755)
	}
	return nil
}

func (s *Server) writeResponse(w http.ResponseWriter, cfg config.Config, versionEcho, opEcho, status byte, payload []byte, errMsg string) int {
	respPayload := payload
	if status != proto.StatusOK && cfg.EnableErrMsg {
		// Optional debug message payload on errors.
		e := proto.NewEncoder(64)
		// Keep messages short to avoid blowing max_payload.
		msg := errMsg
		if len(msg) > 200 {
			msg = msg[:200]
		}
		_ = e.WriteString(msg)
		respPayload = e.Bytes()
	}
	resp, err := proto.BuildResponse(versionEcho, opEcho, status, respPayload)
	if err != nil {
		// Last resort: we cannot build a response -> HTTP 500 is allowed.
		w.WriteHeader(http.StatusInternalServerError)
		return 0
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(resp)
	return len(resp)
}

// tryUnwrapW64FBody attempts to extract the raw W64F RPC blob from WiC64-style HTTP POST bodies.
//
// Some WiC64 firmware / helper stacks wrap the binary payload in a form field named "data".
// Depending on implementation, this can appear as:
//   - application/x-www-form-urlencoded: data=<percent-encoded-bytes>
//   - multipart/form-data: part name="data" contains raw bytes
//
// If extraction succeeds, (unwrapped, info, true) is returned.
func tryUnwrapW64FBody(body []byte, contentType string) ([]byte, string, bool) {
	// Fast path: urlencoded bodies often start with "data=".
	if bytes.HasPrefix(body, []byte("data=")) || strings.HasPrefix(strings.ToLower(contentType), "application/x-www-form-urlencoded") {
		// ParseQuery expects a query-string, not a full HTTP header.
		vals, err := url.ParseQuery(string(body))
		if err == nil {
			v := vals.Get("data")
			if v != "" {
				b := []byte(v)
				if len(b) >= 4 && bytes.Equal(b[:4], []byte("W64F")) {
					return b, "unwrapped=urlencoded", true
				}
				// Even if it doesn't start with W64F, still return it so the caller can re-parse.
				return b, "unwrapped=urlencoded", true
			}
		}
	}

	// Multipart form-data.
	if strings.Contains(strings.ToLower(contentType), "multipart/form-data") {
		_, params, err := mime.ParseMediaType(contentType)
		if err == nil {
			boundary := params["boundary"]
			if boundary != "" {
				mr := multipart.NewReader(bytes.NewReader(body), boundary)
				for {
					part, perr := mr.NextPart()
					if perr != nil {
						break
					}
					if part.FormName() == "data" {
						pb, _ := io.ReadAll(part)
						_ = part.Close()
						if len(pb) > 0 {
							return pb, "unwrapped=multipart", true
						}
					}
					_ = part.Close()
				}
			}
		}
	}

	return nil, "", false
}

func (s *Server) dispatch(cfg config.Config, limits Limits, op byte, flags byte, payload []byte, rootAbs string) (status byte, respPayload []byte, errMsg string) {
	if limits.ReadOnly && isWriteOp(op) {
		return proto.StatusAccessDenied, nil, "read-only mode"
	}
	switch op {
	case proto.OpCAPS:
		return s.opCAPS(cfg, payload)
	case proto.OpSTATFS:
		return s.opSTATFS(cfg, payload, rootAbs)
	case proto.OpLS:
		return s.opLS(cfg, limits, payload, rootAbs)
	case proto.OpSTAT:
		return s.opSTAT(cfg, limits, payload, rootAbs)
	case proto.OpREAD_RANGE:
		return s.opREAD_RANGE(cfg, limits, payload, rootAbs)
	case proto.OpWRITE_RANGE:
		return s.opWRITE_RANGE(cfg, limits, flags, payload, rootAbs)
	case proto.OpAPPEND:
		return s.opAPPEND(cfg, limits, flags, payload, rootAbs)
	case proto.OpMKDIR:
		return s.opMKDIR(cfg, limits, flags, payload, rootAbs)
	case proto.OpRMDIR:
		return s.opRMDIR(cfg, limits, flags, payload, rootAbs)
	case proto.OpRM:
		return s.opRM(cfg, limits, payload, rootAbs)
	case proto.OpCP:
		return s.opCP(cfg, limits, flags, payload, rootAbs)
	case proto.OpSEARCH:
		return s.opSEARCH(cfg, flags, payload, rootAbs)
	case proto.OpHASH:
		return s.opHASH(cfg, limits, flags, payload, rootAbs)
	case proto.OpMV:
		return s.opMV(cfg, limits, flags, payload, rootAbs)
	case proto.OpPING:
		return s.opPING(cfg, payload)
	default:
		return proto.StatusNotSupported, nil, "opcode not supported"
	}
}

func (s *Server) opCAPS(cfg config.Config, payload []byte) (byte, []byte, string) {
	if len(payload) != 0 {
		return proto.StatusBadRequest, nil, "CAPS request payload must be empty"
	}
	// Implemented operations (CAPS itself is always supported and is not listed as a feature).
	features := proto.FeatSTATFS | proto.FeatAPPEND | proto.FeatSEARCH | proto.FeatHASH_CRC32
	if cfg.EnableMkdirParents {
		features |= proto.FeatMKDIR_PARENTS
	}
	if cfg.EnableRmdirRecursive {
		features |= proto.FeatRMDIR_RECURSIVE
	}
	if cfg.EnableCpRecursive {
		features |= proto.FeatCP_RECURSIVE
	}
	if cfg.EnableOverwrite {
		features |= proto.FeatOVERWRITE
	}
	if cfg.EnableErrMsg {
		features |= proto.FeatERRMSG
	}

	// CAPS payload layout (v0.2.1+): max_chunk,u16 max_payload,u16 max_path,u16 max_name,u16 max_entries,u16 features_lo,u32 server_time_unix,u32 server_name,string.
	//
	e := proto.NewEncoder(64)
	e.WriteU16(cfg.MaxChunk)
	e.WriteU16(cfg.MaxPayload)
	e.WriteU16(cfg.MaxPath)
	e.WriteU16(cfg.MaxName)
	e.WriteU16(cfg.MaxEntries)
	e.WriteU32(features)
	e.WriteU32(uint32(time.Now().Unix()))
	_ = e.WriteString(cfg.ServerName)
	return proto.StatusOK, e.Bytes(), ""
}

func (s *Server) readPathString(cfg config.Config, d *proto.Decoder) (string, error) {
	p, err := d.ReadString(cfg.MaxPath)
	if err != nil {
		return "", err
	}
	p, err = pathutil.Normalize(p, cfg.MaxPath, cfg.MaxName)
	if err != nil {
		return "", err
	}
	return pathutil.Canonicalize(p), nil
}

// readPathStringRead is like readPathString, but optionally allows Commodore-style
// wildcard characters ('*' and '?') in the final path segment when the
// compatibility option cfg.Compat.WildcardLoad is enabled.
//
// Wildcards are intentionally *not* allowed in directory segments.
func (s *Server) readPathStringRead(cfg config.Config, d *proto.Decoder) (string, error) {
	raw, err := d.ReadString(cfg.MaxPath)
	if err != nil {
		return "", err
	}

	var p string
	if cfg.Compat.WildcardLoad {
		p, err = pathutil.NormalizeAllowWildcards(raw, cfg.MaxPath, cfg.MaxName)
	} else {
		p, err = pathutil.Normalize(raw, cfg.MaxPath, cfg.MaxName)
	}
	if err != nil {
		return "", err
	}

	if cfg.Compat.WildcardLoad {
		// Safety: only allow wildcards in the filename segment.
		if i := strings.LastIndex(p, "/"); i > 0 {
			if strings.ContainsAny(p[:i], "*?") {
				return "", fmt.Errorf("wildcards only allowed in final segment")
			}
		}
	}

	return pathutil.Canonicalize(p), nil
}

func (s *Server) opSTATFS(cfg config.Config, payload []byte, rootAbs string) (byte, []byte, string) {
	// path string optional; if empty -> "/".
	d := proto.NewDecoder(payload)
	p := "/"
	if d.Remaining() == 0 {
		// Allow empty payload for convenience.
		p = "/"
	} else {
		sp, err := s.readPathString(cfg, d)
		if err != nil {
			return proto.StatusInvalidPath, nil, err.Error()
		}
		p = sp
	}
	if d.Remaining() != 0 {
		return proto.StatusBadRequest, nil, "extra bytes in STATFS"
	}

	abs, err := fsops.ToOSPath(rootAbs, p)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, abs, false); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		return proto.StatusInvalidPath, nil, err.Error()
	}
	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !st.Exists {
		return proto.StatusNotFound, nil, "not found"
	}

	total, free, err := fsops.DiskUsage(abs)
	if err != nil {
		// Spec allows 0 when unknown.
		total = 0
		free = 0
	}
	used := uint64(0)
	if total >= free {
		used = total - free
	}

	e := proto.NewEncoder(12)
	e.WriteU32(clampU32(total))
	e.WriteU32(clampU32(free))
	e.WriteU32(clampU32(used))
	return proto.StatusOK, e.Bytes(), ""
}

func (s *Server) opLS(cfg config.Config, limits Limits, payload []byte, rootAbs string) (byte, []byte, string) {
	// Payload: path string (leer -> "/"), start_index u16, max_entries u16.
	d := proto.NewDecoder(payload)
	p, err := s.readPathStringRead(cfg, d)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	start, err := d.ReadU16()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	maxEntriesReq, err := d.ReadU16()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if d.Remaining() != 0 {
		return proto.StatusBadRequest, nil, "extra bytes in LS"
	}

	maxEntries := cfg.MaxEntries
	if maxEntriesReq != 0 && maxEntriesReq < maxEntries {
		maxEntries = maxEntriesReq
	}
	if maxEntries == 0 {
		maxEntries = 1
	}

	// --- Disk image virtual directories (.d64/.d71/.d81) ---
	// If the requested path points to a supported disk image, list the image contents.
	if limits.DiskImagesEnabled {
		if mountPath, inner, ok := splitD64Path(p); ok {
			// Inside a disk image we support a flat namespace (no subdirectories).
			// For compatibility, inner may be empty (list image root), a wildcard pattern
			// (e.g. "*" or "DEMO*"), or an exact filename.
			if strings.Contains(inner, "/") {
				return proto.StatusNotADir, nil, "not a directory"
			}
			_, img, st, msg := resolveD64Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			files := img.SortedEntries() // []*diskimage.FileEntry
			// Optional filter (wildcard or exact)
			if inner != "" {
				if strings.ContainsAny(inner, "*?") {
					pat := inner
					filtered := make([]*diskimage.FileEntry, 0, len(files))
					for _, fe := range files {
						name := strings.ToUpper(fe.Name)
						if wildcardMatch(pat, name) {
							filtered = append(filtered, fe)
						}
					}
					files = filtered
				} else {
					// exact match (+ optional .PRG fallback)
					want := inner
					fe, ok := img.Lookup(want)
					if !ok && !strings.HasSuffix(want, ".PRG") {
						fe, ok = img.Lookup(want + ".PRG")
					}
					if ok {
						files = []*diskimage.FileEntry{fe}
					} else {
						files = nil
					}
				}
			}
			idx := int(start)
			if idx < 0 || idx >= len(files) {
				e := proto.NewEncoder(4)
				e.WriteU16(0)      // count
				e.WriteU16(0xFFFF) // next_index
				return proto.StatusOK, e.Bytes(), ""
			}

			count := uint16(0)
			buf := make([]byte, 0, 32*int(maxEntries)+2)
			buf = proto.AppendU16(buf, 0) // placeholder count

			for idx < len(files) && count < maxEntries {
				fe := files[idx]
				idx++
				name := strings.ToUpper(fe.Name)
				if uint16(len(name)) > cfg.MaxName {
					// Truncate defensively (disk images can contain odd names).
					name = name[:int(cfg.MaxName)]
				}

				enc := proto.NewEncoder(32)
				enc.WriteU8(0) // file
				enc.WriteU32(clampU32(fe.Size))
				enc.WriteU32(uint32(img.ModTime.Unix()))
				if err := enc.WriteString(name); err != nil {
					return proto.StatusInternal, nil, err.Error()
				}

				entryBytes := enc.Bytes()
				if len(buf)+len(entryBytes)+2 > int(cfg.MaxPayload) {
					// stop early; still return a valid partial page
					idx--
					break
				}
				buf = append(buf, entryBytes...)
				count++
			}

			nextIndex := uint16(0xFFFF)
			if idx < len(files) {
				nextIndex = uint16(idx)
			}
			buf = proto.AppendU16(buf, nextIndex)
			binary.LittleEndian.PutUint16(buf[0:2], count)
			return proto.StatusOK, buf, ""
		}
		if mountPath, inner, ok := splitD71Path(p); ok {
			// Inside a disk image we support a flat namespace (no subdirectories).
			// For compatibility, inner may be empty (list image root), a wildcard pattern
			// (e.g. "*" or "DEMO*"), or an exact filename.
			if strings.Contains(inner, "/") {
				return proto.StatusNotADir, nil, "not a directory"
			}
			_, img, st, msg := resolveD71Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			files := img.SortedEntries() // []*diskimage.FileEntry
			// Optional filter (wildcard or exact)
			if inner != "" {
				if strings.ContainsAny(inner, "*?") {
					pat := inner
					filtered := make([]*diskimage.FileEntry, 0, len(files))
					for _, fe := range files {
						name := strings.ToUpper(fe.Name)
						if wildcardMatch(pat, name) {
							filtered = append(filtered, fe)
						}
					}
					files = filtered
				} else {
					// exact match (+ optional .PRG fallback)
					want := inner
					fe, ok := img.Lookup(want)
					if !ok && !strings.HasSuffix(want, ".PRG") {
						fe, ok = img.Lookup(want + ".PRG")
					}
					if ok {
						files = []*diskimage.FileEntry{fe}
					} else {
						files = nil
					}
				}
			}
			idx := int(start)
			if idx < 0 || idx >= len(files) {
				e := proto.NewEncoder(4)
				e.WriteU16(0)      // count
				e.WriteU16(0xFFFF) // next_index
				return proto.StatusOK, e.Bytes(), ""
			}

			count := uint16(0)
			buf := make([]byte, 0, 32*int(maxEntries)+2)
			buf = proto.AppendU16(buf, 0) // placeholder count

			for idx < len(files) && count < maxEntries {
				fe := files[idx]
				idx++
				name := strings.ToUpper(fe.Name)
				if uint16(len(name)) > cfg.MaxName {
					// Truncate defensively (disk images can contain odd names).
					name = name[:int(cfg.MaxName)]
				}

				enc := proto.NewEncoder(32)
				enc.WriteU8(0) // file
				enc.WriteU32(clampU32(fe.Size))
				enc.WriteU32(uint32(img.ModTime.Unix()))
				if err := enc.WriteString(name); err != nil {
					return proto.StatusInternal, nil, err.Error()
				}

				entryBytes := enc.Bytes()
				if len(buf)+len(entryBytes)+2 > int(cfg.MaxPayload) {
					// stop early; still return a valid partial page
					idx--
					break
				}
				buf = append(buf, entryBytes...)
				count++
			}

			nextIndex := uint16(0xFFFF)
			if idx < len(files) {
				nextIndex = uint16(idx)
			}
			buf = proto.AppendU16(buf, nextIndex)
			binary.LittleEndian.PutUint16(buf[0:2], count)
			return proto.StatusOK, buf, ""
		}
		if mountPath, inner, ok := splitD81Path(p); ok {
			_, img, st, msg := resolveD81Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			// Determine the directory inside the image to list, and an optional filter pattern.
			//
			// Examples:
			//   /disk.d81                 -> dir=""          pattern=""
			//   /disk.d81/FOO*            -> dir=""          pattern="FOO*"
			//   /disk.d81/SUBDIR          -> dir="SUBDIR"    pattern="" (if SUBDIR is a DIR)
			//   /disk.d81/SUBDIR/FOO*     -> dir="SUBDIR"    pattern="FOO*"
			//   /disk.d81/SUBDIR/FILE.PRG -> dir="SUBDIR"    pattern="FILE.PRG"
			dirPath := ""
			pattern := ""
			if inner != "" {
				if strings.ContainsAny(inner, "*?") {
					// Wildcards: treat the last segment as a pattern and everything before as dir path.
					if idx := strings.LastIndex(inner, "/"); idx >= 0 {
						dirPath = inner[:idx]
						pattern = inner[idx+1:]
					} else {
						pattern = inner
					}
				} else {
					// No wildcards: first try if the full inner path is a directory.
					if _, _, _, _, stDir, _ := resolveD81Dir(img, inner); stDir == proto.StatusOK {
						dirPath = inner
						pattern = ""
					} else {
						// Otherwise treat the last segment as a filter within its parent directory.
						if idx := strings.LastIndex(inner, "/"); idx >= 0 {
							dirPath = inner[:idx]
							pattern = inner[idx+1:]
						} else {
							pattern = inner
						}
					}
				}
			}

			dirFiles, dirByName, _, _, st, msg := resolveD81Dir(img, dirPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			files := img.SortedDirEntries(dirFiles)
			if pattern != "" {
				filtered := make([]*diskimage.FileEntry, 0, len(files))
				if strings.ContainsAny(pattern, "*?") {
					pat := strings.ToUpper(pattern)
					for _, fe := range files {
						if wildcardMatch(pat, strings.ToUpper(fe.Name)) {
							filtered = append(filtered, fe)
						}
					}
				} else {
					want := strings.ToUpper(strings.TrimSpace(pattern))
					if fe, ok := dirByName[want]; ok {
						filtered = append(filtered, fe)
					} else if !strings.HasSuffix(want, ".PRG") {
						// Optional convenience: if inner has no .PRG, try .PRG
						if fe, ok := dirByName[want+".PRG"]; ok {
							filtered = append(filtered, fe)
						}
					}
				}
				files = filtered
			}

			idx := int(start)
			if idx < 0 || idx >= len(files) {
				// Clarified behavior: OK, count=0, next_index=0xFFFF.
				e := proto.NewEncoder(4)
				e.WriteU16(0)
				e.WriteU16(0xFFFF)
				return proto.StatusOK, e.Bytes(), ""
			}

			count := uint16(0)
			buf := make([]byte, 0, int(maxEntries)*32+4)
			buf = proto.AppendU16(buf, 0) // placeholder for count
			for idx < len(files) && count < maxEntries {
				fe := files[idx]
				mtime := uint32(0)
				if !img.ModTime.IsZero() {
					mtime = uint32(img.ModTime.Unix())
				}

				// IMPORTANT: LS entry format must match the normal filesystem LS.
				// Format: [type u8][size u32][mtime u32][name string]
				// (name string = u8 length + bytes)
				name := strings.ToUpper(fe.Name)
				if uint16(len(name)) > cfg.MaxName {
					name = name[:int(cfg.MaxName)]
				}
				etype := byte(0) // file
				sz := fe.Size
				// D81 can contain directory entries (type 6). We show them as DIRs.
				if fe.Type == 6 || fe.Type == 5 {
					etype = 1
					sz = 0
				}
				enc := proto.NewEncoder(32)
				enc.WriteU8(etype)
				enc.WriteU32(clampU32(sz))
				enc.WriteU32(mtime)
				if err := enc.WriteString(name); err != nil {
					return proto.StatusInternal, nil, err.Error()
				}

				entryBytes := enc.Bytes()
				// +2 for next_index u16 at end
				if len(buf)+len(entryBytes)+2 > int(cfg.MaxPayload) {
					break
				}
				buf = append(buf, entryBytes...)
				idx++
				count++
			}

			nextIndex := uint16(0xFFFF)
			if idx < len(files) {
				nextIndex = uint16(idx)
			}
			buf = proto.AppendU16(buf, nextIndex)
			binary.LittleEndian.PutUint16(buf[0:2], count)
			return proto.StatusOK, buf, ""
		}
	}

	listPattern := ""
	listPath := p
	if cfg.Compat.WildcardLoad && strings.ContainsAny(p, "*?") {
		dirNorm, base := splitDirBase(p)
		if strings.ContainsAny(base, "*?") {
			listPattern = base
			listPath = dirNorm
		}
	}

	abs, err := fsops.ToOSPath(rootAbs, listPath)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	// Ensure the path does not contain symlinks.
	if err := fsops.LstatNoSymlink(rootAbs, abs, false); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		return proto.StatusInvalidPath, nil, err.Error()
	}

	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !st.Exists {
		return proto.StatusNotFound, nil, "not found"
	}
	if !st.IsDir {
		return proto.StatusNotADir, nil, "not a directory"
	}

	entries, err := os.ReadDir(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if listPattern != "" {
		filtered := make([]os.DirEntry, 0, len(entries))
		for _, ent := range entries {
			name := strings.ToUpper(ent.Name())
			if wildcardMatch(listPattern, name) {
				filtered = append(filtered, ent)
			}
		}
		entries = filtered
	}

	// Stable sort by name (case-insensitive).
	sort.SliceStable(entries, func(i, j int) bool {
		return strings.ToUpper(entries[i].Name()) < strings.ToUpper(entries[j].Name())
	})

	if int(start) >= len(entries) {
		// Clarified behavior: OK, count=0, next_index=0xFFFF.
		e := proto.NewEncoder(4)
		e.WriteU16(0)
		e.WriteU16(0xFFFF)
		return proto.StatusOK, e.Bytes(), ""
	}

	// Build response with payload limit protection.
	buf := make([]byte, 0, 256)
	// count placeholder
	buf = append(buf, 0x00, 0x00)
	count := uint16(0)
	idx := int(start)
	for idx < len(entries) && count < maxEntries {
		e := entries[idx]
		info, err := e.Info()
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return proto.StatusInvalidPath, nil, "symlink not allowed"
		}
		name := strings.ToUpper(e.Name())
		etype := byte(0)
		size := uint32(0)
		isImage := limits.DiskImagesEnabled && !info.IsDir() && (strings.HasSuffix(name, ".D64") || strings.HasSuffix(name, ".D71") || strings.HasSuffix(name, ".D81"))
		if info.IsDir() || isImage {
			etype = 1
			size = 0
		} else {
			etype = 0
			sz := uint64(info.Size())
			if sz > 0xFFFFFFFF {
				size = 0xFFFFFFFF
			} else {
				size = uint32(sz)
			}
		}
		mtime := uint32(0)
		if !info.ModTime().IsZero() {
			mtime = uint32(info.ModTime().Unix())
		}

		// Encode entry into temp buffer.
		enc := proto.NewEncoder(32)
		enc.WriteU8(etype)
		enc.WriteU32(size)
		enc.WriteU32(mtime)
		_ = enc.WriteString(name)
		entryBytes := enc.Bytes()

		// Need room for entry + trailing next_index (2 bytes).
		if len(buf)+len(entryBytes)+2 > int(cfg.MaxPayload) {
			break
		}
		buf = append(buf, entryBytes...)
		count++
		idx++
	}

	nextIndex := uint16(0xFFFF)
	if int(start)+int(count) < len(entries) {
		nextIndex = start + count
	}

	// Append next_index.
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], nextIndex)
	if len(buf)+2 > int(cfg.MaxPayload) {
		return proto.StatusTooLarge, nil, "LS response too large"
	}
	buf = append(buf, tmp[:]...)

	// Patch count.
	binary.LittleEndian.PutUint16(buf[0:2], count)
	return proto.StatusOK, buf, ""
}

func (s *Server) opSTAT(cfg config.Config, limits Limits, payload []byte, rootAbs string) (byte, []byte, string) {
	// Payload: path string (leer -> "/").
	d := proto.NewDecoder(payload)
	p, err := s.readPathStringRead(cfg, d)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if d.Remaining() != 0 {
		return proto.StatusBadRequest, nil, "extra bytes in STAT"
	}

	// Disk image virtual directories (.d64/.d71/.d81)
	if limits.DiskImagesEnabled {
		if mountPath, inner, ok := splitD64Path(p); ok {
			_, img, st, msg := resolveD64Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			mtime := uint32(img.ModTime.Unix())
			if inner == "" {
				e := proto.NewEncoder(9)
				e.WriteU8(1) // dir
				e.WriteU32(0)
				e.WriteU32(mtime)
				return proto.StatusOK, e.Bytes(), ""
			}
			_, fe, st, msg := resolveD64Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			e := proto.NewEncoder(9)
			e.WriteU8(0) // file
			e.WriteU32(clampU32(fe.Size))
			e.WriteU32(mtime)
			return proto.StatusOK, e.Bytes(), ""
		}
		if mountPath, inner, ok := splitD71Path(p); ok {
			_, img, st, msg := resolveD71Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			mtime := uint32(img.ModTime.Unix())
			if inner == "" {
				e := proto.NewEncoder(9)
				e.WriteU8(1) // dir
				e.WriteU32(0)
				e.WriteU32(mtime)
				return proto.StatusOK, e.Bytes(), ""
			}
			_, fe, st, msg := resolveD71Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			e := proto.NewEncoder(9)
			e.WriteU8(0) // file
			e.WriteU32(clampU32(fe.Size))
			e.WriteU32(mtime)
			return proto.StatusOK, e.Bytes(), ""
		}
		if mountPath, inner, ok := splitD81Path(p); ok {
			_, img, st, msg := resolveD81Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			mtime := uint32(img.ModTime.Unix())
			if inner == "" {
				e := proto.NewEncoder(9)
				e.WriteU8(1) // dir
				e.WriteU32(0)
				e.WriteU32(mtime)
				return proto.StatusOK, e.Bytes(), ""
			}

			// D81 supports subdirectories: if the inner path is a directory, return directory stat.
			if _, _, _, _, stDir, _ := resolveD81Dir(img, inner); stDir == proto.StatusOK {
				e := proto.NewEncoder(9)
				e.WriteU8(1) // dir
				e.WriteU32(0)
				e.WriteU32(mtime)
				return proto.StatusOK, e.Bytes(), ""
			}

			_, fe, st, msg := resolveD81Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st == proto.StatusIsADir {
				e := proto.NewEncoder(9)
				e.WriteU8(1) // dir
				e.WriteU32(0)
				e.WriteU32(mtime)
				return proto.StatusOK, e.Bytes(), ""
			}
			if st != proto.StatusOK {
				return st, nil, msg
			}
			e := proto.NewEncoder(9)
			e.WriteU8(0) // file
			e.WriteU32(clampU32(fe.Size))
			e.WriteU32(mtime)
			return proto.StatusOK, e.Bytes(), ""
		}
	}
	abs, _, err := resolveReadPathWithCompat(cfg, rootAbs, p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		return proto.StatusInvalidPath, nil, err.Error()
	}
	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !st.Exists {
		return proto.StatusNotFound, nil, "not found"
	}
	typeByte := byte(0)
	if st.IsDir {
		typeByte = 1
	}
	e := proto.NewEncoder(9)
	e.WriteU8(typeByte)
	e.WriteU32(clampU32(st.Size))
	e.WriteU32(st.MTimeUnix)
	return proto.StatusOK, e.Bytes(), ""
}

func (s *Server) opREAD_RANGE(cfg config.Config, limits Limits, payload []byte, rootAbs string) (byte, []byte, string) {
	// Payload: path string, offset u32, length u16. Response: raw bytes.
	d := proto.NewDecoder(payload)
	p, err := s.readPathStringRead(cfg, d)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	offset, err := d.ReadU32()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	ln, err := d.ReadU16()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if d.Remaining() != 0 {
		return proto.StatusBadRequest, nil, "extra bytes in READ_RANGE"
	}
	if ln > cfg.MaxChunk {
		return proto.StatusTooLarge, nil, "chunk too large"
	}

	// Disk image virtual directories (.d64/.d71/.d81)
	if limits.DiskImagesEnabled {
		if mountPath, inner, ok := splitD64Path(p); ok {
			imgAbs, img, st, msg := resolveD64Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "is a directory"
			}
			_, fe, st, msg := resolveD64Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			off := uint64(offset)
			want := uint64(ln)
			if off > fe.Size {
				return proto.StatusRangeInvalid, nil, "offset beyond EOF"
			}
			if off == fe.Size {
				return proto.StatusOK, []byte{}, ""
			}
			if want > fe.Size-off {
				return proto.StatusRangeInvalid, nil, "range exceeds EOF"
			}

			data, err := readD64FileRange(imgAbs, fe, off, want)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			return proto.StatusOK, data, ""
		}
		if mountPath, inner, ok := splitD71Path(p); ok {
			imgAbs, img, st, msg := resolveD71Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "is a directory"
			}
			_, fe, st, msg := resolveD71Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			off := uint64(offset)
			want := uint64(ln)
			if off > fe.Size {
				return proto.StatusRangeInvalid, nil, "offset beyond EOF"
			}
			if off == fe.Size {
				return proto.StatusOK, []byte{}, ""
			}
			if want > fe.Size-off {
				return proto.StatusRangeInvalid, nil, "range exceeds EOF"
			}

			data, err := readD71FileRange(imgAbs, fe, off, want)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			return proto.StatusOK, data, ""
		}
		if mountPath, inner, ok := splitD81Path(p); ok {
			imgAbs, img, st, msg := resolveD81Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "is a directory"
			}
			_, fe, st, msg := resolveD81Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			off := uint64(offset)
			want := uint64(ln)
			if off > fe.Size {
				return proto.StatusRangeInvalid, nil, "offset beyond EOF"
			}
			if off == fe.Size {
				return proto.StatusOK, []byte{}, ""
			}
			if want > fe.Size-off {
				return proto.StatusRangeInvalid, nil, "range exceeds EOF"
			}

			data, err := readD81FileRange(imgAbs, fe, off, want)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			return proto.StatusOK, data, ""
		}
	}
	abs, _, err := resolveReadPathWithCompat(cfg, rootAbs, p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		return proto.StatusInvalidPath, nil, err.Error()
	}
	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !st.Exists {
		return proto.StatusNotFound, nil, "not found"
	}
	if st.IsDir {
		return proto.StatusIsADir, nil, "is a directory"
	}

	sz := st.Size
	if uint64(offset) > sz {
		return proto.StatusRangeInvalid, nil, "offset beyond EOF"
	}
	if uint64(offset) == sz {
		return proto.StatusOK, []byte{}, ""
	}

	f, err := os.Open(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	defer f.Close()
	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		return proto.StatusRangeInvalid, nil, err.Error()
	}
	buf := make([]byte, int(ln))
	n, err := io.ReadFull(f, buf)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return proto.StatusOK, buf[:n], ""
		}
		return proto.StatusInternal, nil, err.Error()
	}
	return proto.StatusOK, buf[:n], ""
}

func (s *Server) opWRITE_RANGE(cfg config.Config, limits Limits, flags byte, payload []byte, rootAbs string) (byte, []byte, string) {
	// WRITE_RANGE flags: TRUNCATE|CREATE. Payload: path string, offset u32, data_len u16, data bytes.
	if !s.writeMu.TryLock() {
		return proto.StatusBusy, nil, "server busy"
	}
	defer s.writeMu.Unlock()

	d := proto.NewDecoder(payload)
	p, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	offset, err := d.ReadU32()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	dataLen, err := d.ReadU16()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if dataLen > cfg.MaxChunk {
		return proto.StatusTooLarge, nil, "chunk too large"
	}
	if d.Remaining() != int(dataLen) {
		return proto.StatusBadRequest, nil, "data_len mismatch"
	}
	data, err := d.ReadBytes(int(dataLen))
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if flags&proto.FlagWR_TRUNCATE != 0 {
		if offset != 0 {
			return proto.StatusBadRequest, nil, "TRUNCATE requires offset=0"
		}
	}

	// Root is a directory; cannot write to it.
	if p == "/" {
		return proto.StatusIsADir, nil, "cannot write to /"
	}

	// Disk images: if enabled, treat "/.../DISK.D64/FILE" as a file inside the image.
	if limits.DiskImagesEnabled {
		if mountPath, inner, ok := splitD64Path(p); ok {
			if !limits.DiskImagesWriteEnabled {
				return proto.StatusAccessDenied, nil, "disk images are read-only"
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "disk image is a directory"
			}
			if strings.Contains(inner, "/") {
				return proto.StatusNotSupported, nil, "subdirectories inside .d64 are not supported"
			}
			if strings.ContainsAny(inner, "*?") {
				return proto.StatusBadRequest, nil, "wildcards not allowed for disk image writes"
			}
			inner = normalizeDiskImageLeafName(inner, cfg.Compat.FallbackPRGExtension)

			imgAbs, _, st, msg := resolveD64Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			truncate := (flags & proto.FlagWR_TRUNCATE) != 0
			create := (flags & proto.FlagWR_CREATE) != 0
			overwrite := (flags & proto.FlagWR_OVERWRITE) != 0
			allowOverwrite := cfg.EnableOverwrite && overwrite
			written, err := diskimage.WriteFileRangeD64(imgAbs, inner, offset, data, truncate, create, allowOverwrite)
			if err != nil {
				var se *diskimage.StatusError
				if errors.As(err, &se) {
					return se.Status(), nil, se.Error()
				}
				return proto.StatusInternal, nil, err.Error()
			}
			resp := make([]byte, 4)
			binary.LittleEndian.PutUint32(resp, written)
			return proto.StatusOK, resp, "written to d64"
		}
		if mountPath, inner, ok := splitD71Path(p); ok {
			if !limits.DiskImagesWriteEnabled {
				return proto.StatusAccessDenied, nil, "disk images are read-only"
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "disk image is a directory"
			}
			if strings.Contains(inner, "/") {
				return proto.StatusNotSupported, nil, "subdirectories inside .d71 are not supported"
			}
			if strings.ContainsAny(inner, "*?") {
				return proto.StatusBadRequest, nil, "wildcards not allowed for disk image writes"
			}
			inner = normalizeDiskImageLeafName(inner, cfg.Compat.FallbackPRGExtension)

			imgAbs, _, st, msg := resolveD71Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			truncate := (flags & proto.FlagWR_TRUNCATE) != 0
			create := (flags & proto.FlagWR_CREATE) != 0
			overwrite := (flags & proto.FlagWR_OVERWRITE) != 0
			allowOverwrite := cfg.EnableOverwrite && overwrite
			written, err := diskimage.WriteFileRangeD71(imgAbs, inner, offset, data, truncate, create, allowOverwrite)
			if err != nil {
				var se *diskimage.StatusError
				if errors.As(err, &se) {
					return se.Status(), nil, se.Error()
				}
				return proto.StatusInternal, nil, err.Error()
			}
			resp := make([]byte, 4)
			binary.LittleEndian.PutUint32(resp, written)
			return proto.StatusOK, resp, "written to d71"
		}

		if mountPath, inner, ok := splitD81Path(p); ok {
			if !limits.DiskImagesWriteEnabled {
				return proto.StatusAccessDenied, nil, "disk images are read-only"
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "disk image is a directory"
			}
			if strings.ContainsAny(inner, "*?") {
				return proto.StatusBadRequest, nil, "wildcards not allowed for disk image writes"
			}
			inner = normalizeDiskImageLeafName(inner, cfg.Compat.FallbackPRGExtension)

			imgAbs, _, st, msg := resolveD81Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}

			truncate := (flags & proto.FlagWR_TRUNCATE) != 0
			create := (flags & proto.FlagWR_CREATE) != 0
			overwrite := (flags & proto.FlagWR_OVERWRITE) != 0
			allowOverwrite := cfg.EnableOverwrite && overwrite
			written, err := diskimage.WriteFileRangeD81(imgAbs, inner, offset, data, truncate, create, allowOverwrite)
			if err != nil {
				var se *diskimage.StatusError
				if errors.As(err, &se) {
					return se.Status(), nil, se.Error()
				}
				return proto.StatusInternal, nil, err.Error()
			}
			resp := make([]byte, 4)
			binary.LittleEndian.PutUint32(resp, written)
			return proto.StatusOK, resp, "written to d81"
		}
	}

	abs, err := fsops.ToOSPath(rootAbs, p)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, abs, true); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		return proto.StatusInvalidPath, nil, err.Error()
	}

	// Check existence.
	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}

	truncate := flags&proto.FlagWR_TRUNCATE != 0
	create := flags&proto.FlagWR_CREATE != 0
	overwrite := flags&proto.FlagWR_OVERWRITE != 0
	var oldSize uint64
	if !st.Exists {
		if !create {
			return proto.StatusNotFound, nil, "not found"
		}
		// Parent must exist.
		parent := filepath.Dir(abs)
		pst, err := fsops.Stat(parent)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		if !pst.Exists || !pst.IsDir {
			return proto.StatusNotFound, nil, "parent directory missing"
		}
		oldSize = 0
	} else {
		if st.IsDir {
			return proto.StatusIsADir, nil, "is a directory"
		}
		oldSize = st.Size
	}

	// Overwrite protection: writing from offset 0 into an existing non-empty file is only
	// allowed if the client explicitly set the TRUNCATE flag. In addition, if the server
	// has overwrite protection enabled, the client must also set the OVERWRITE flag to
	// confirm that replacing an existing file is intended.
	if st.Exists && !st.IsDir {
		if oldSize > 0 && offset == 0 && !truncate && len(data) > 0 {
			return proto.StatusAlreadyExists, nil, "file exists; set TRUNCATE (+OVERWRITE) to replace"
		}
		if truncate && oldSize > 0 {
			if !cfg.EnableOverwrite {
				return proto.StatusAccessDenied, nil, "overwrite disabled by server"
			}
			if !overwrite {
				return proto.StatusAccessDenied, nil, "overwrite requires OVERWRITE flag"
			}
		}
	}

	if uint64(offset) > oldSize {
		return proto.StatusRangeInvalid, nil, "no sparse writes"
	}

	// Pre-check sizes for limits.
	var newSize uint64
	if truncate {
		newSize = uint64(len(data))
	} else {
		end := uint64(offset) + uint64(len(data))
		if end > oldSize {
			newSize = end
		} else {
			newSize = oldSize
		}
	}

	if limits.MaxFileBytes > 0 && newSize > limits.MaxFileBytes {
		return proto.StatusTooLarge, nil, "max file size exceeded"
	}

	delta := int64(newSize) - int64(oldSize)
	var usedBefore uint64
	var haveUsed bool
	if limits.QuotaBytes > 0 && delta > 0 {
		used, err := s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		haveUsed = true
		usedBefore = used
		if usedBefore+uint64(delta) > limits.QuotaBytes {
			return proto.StatusTooLarge, nil, "quota exceeded"
		}
	} else if s.usage != nil {
		// Keep cache warm for future checks.
		used, err := s.rootUsageBytes(rootAbs)
		if err == nil {
			haveUsed = true
			usedBefore = used
		}
	}

	// Open file.
	openFlags := os.O_WRONLY
	if create {
		openFlags |= os.O_CREATE
	}
	if flags&proto.FlagWR_TRUNCATE != 0 {
		openFlags |= os.O_TRUNC
	}
	f, err := os.OpenFile(abs, openFlags, 0o644)
	if err != nil {
		// A directory might have appeared between stat and open.
		if errors.Is(err, fs.ErrPermission) {
			return proto.StatusAccessDenied, nil, "access denied"
		}
		return proto.StatusInternal, nil, err.Error()
	}
	defer f.Close()

	if _, err := f.Seek(int64(offset), io.SeekStart); err != nil {
		s.invalidateRootUsage(rootAbs)
		return proto.StatusRangeInvalid, nil, err.Error()
	}
	if _, err := f.Write(data); err != nil {
		s.invalidateRootUsage(rootAbs)
		return proto.StatusInternal, nil, err.Error()
	}
	_ = f.Sync()

	if haveUsed && s.usage != nil {
		s.setRootUsageBytes(rootAbs, applyDeltaBytes(usedBefore, delta))
	}
	return proto.StatusOK, nil, ""
}

func (s *Server) opAPPEND(cfg config.Config, limits Limits, flags byte, payload []byte, rootAbs string) (byte, []byte, string) {
	// APPEND flags: CREATE (bit1). Payload: path string, data_len u16, data bytes.
	if !s.writeMu.TryLock() {
		return proto.StatusBusy, nil, "busy"
	}
	defer s.writeMu.Unlock()

	d := proto.NewDecoder(payload)
	p, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	ln, err := d.ReadU16()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if ln > cfg.MaxChunk {
		return proto.StatusTooLarge, nil, "append chunk too large"
	}
	if d.Remaining() != int(ln) {
		return proto.StatusBadRequest, nil, "length mismatch"
	}
	data, err := d.ReadBytes(int(ln))
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if p == "/" {
		return proto.StatusIsADir, nil, "is a directory"
	}

	// Disk image append support (D64)
	if mountPath, inner, ok := splitD64Path(p); ok && inner != "" {
		inner = normalizeDiskImageLeafName(inner, cfg.Compat.FallbackPRGExtension)
		if !limits.DiskImagesEnabled {
			return proto.StatusNotSupported, nil, "disk images are disabled"
		}
		if !limits.DiskImagesWriteEnabled {
			return proto.StatusAccessDenied, nil, "disk images are read-only"
		}
		imgAbs, img, st, msg := resolveD64Mount(rootAbs, mountPath)
		if st != proto.StatusOK {
			return st, nil, msg
		}
		if strings.Contains(inner, "/") {
			return proto.StatusNotSupported, nil, "D64 subdirectories are not supported"
		}
		inner = normalizeDiskImageLeafName(inner, cfg.Compat.FallbackPRGExtension)

		create := flags&proto.FlagAP_CREATE != 0
		var offset uint32
		truncate := false
		if ent, ok := img.Lookup(inner); ok && ent != nil {
			offset = uint32(ent.Size)
		} else {
			if !create {
				return proto.StatusNotFound, nil, "not found"
			}
			offset = 0
			truncate = true
		}

		if _, err := diskimage.WriteFileRangeD64(imgAbs, inner, offset, data, truncate, create, false); err != nil {
			var se *diskimage.StatusError
			if errors.As(err, &se) {
				return se.Status(), nil, se.Error()
			}
			return proto.StatusInternal, nil, err.Error()
		}
		return proto.StatusOK, nil, ""
	}

	abs, err := fsops.ToOSPath(rootAbs, p)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, abs, true); err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}

	create := flags&proto.FlagAP_CREATE != 0

	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	var oldSize uint64
	if !st.Exists {
		if !create {
			return proto.StatusNotFound, nil, "not found"
		}
		parent := filepath.Dir(abs)
		pst, err := fsops.Stat(parent)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		if !pst.Exists || !pst.IsDir {
			return proto.StatusNotFound, nil, "parent directory missing"
		}
		oldSize = 0
	} else {
		if st.IsDir {
			return proto.StatusIsADir, nil, "is a directory"
		}
		oldSize = st.Size
	}

	newSize := oldSize + uint64(len(data))
	if limits.MaxFileBytes > 0 && newSize > limits.MaxFileBytes {
		return proto.StatusTooLarge, nil, "max file size exceeded"
	}

	delta := int64(len(data))
	var usedBefore uint64
	var haveUsed bool
	if limits.QuotaBytes > 0 && delta > 0 {
		used, err := s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		haveUsed = true
		usedBefore = used
		if usedBefore+uint64(delta) > limits.QuotaBytes {
			return proto.StatusTooLarge, nil, "quota exceeded"
		}
	} else if s.usage != nil {
		used, err := s.rootUsageBytes(rootAbs)
		if err == nil {
			haveUsed = true
			usedBefore = used
		}
	}

	openFlags := os.O_WRONLY | os.O_APPEND
	if create {
		openFlags |= os.O_CREATE
	}
	f, err := os.OpenFile(abs, openFlags, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrPermission) {
			return proto.StatusAccessDenied, nil, "access denied"
		}
		return proto.StatusInternal, nil, err.Error()
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		s.invalidateRootUsage(rootAbs)
		return proto.StatusInternal, nil, err.Error()
	}
	_ = f.Sync()

	if haveUsed && s.usage != nil {
		s.setRootUsageBytes(rootAbs, applyDeltaBytes(usedBefore, delta))
	}
	return proto.StatusOK, nil, ""
}

func (s *Server) opHASH(cfg config.Config, limits Limits, flags byte, payload []byte, rootAbs string) (byte, []byte, string) {
	// HASH flags: bit0 selects algo (0=CRC32, 1=SHA1). Only CRC32 is implemented.
	d := proto.NewDecoder(payload)
	p, err := s.readPathStringRead(cfg, d)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if d.Remaining() != 0 {
		return proto.StatusBadRequest, nil, "extra bytes in HASH"
	}
	if flags&proto.FlagH_ALGO != 0 {
		return proto.StatusNotSupported, nil, "SHA1 not supported"
	}

	// Disk image virtual directories (.d64/.d71/.d81)
	if limits.DiskImagesEnabled {
		if mountPath, inner, ok := splitD64Path(p); ok {
			imgAbs, img, st, msg := resolveD64Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "is a directory"
			}
			_, fe, st, msg := resolveD64Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			sum, err := crc32D64File(imgAbs, fe)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			e := proto.NewEncoder(4)
			e.WriteU32(sum)
			return proto.StatusOK, e.Bytes(), ""
		}
		if mountPath, inner, ok := splitD71Path(p); ok {
			imgAbs, img, st, msg := resolveD71Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "is a directory"
			}
			_, fe, st, msg := resolveD71Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			sum, err := crc32D71File(imgAbs, fe)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			e := proto.NewEncoder(4)
			e.WriteU32(sum)
			return proto.StatusOK, e.Bytes(), ""
		}
		if mountPath, inner, ok := splitD81Path(p); ok {
			imgAbs, img, st, msg := resolveD81Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			if inner == "" {
				return proto.StatusIsADir, nil, "is a directory"
			}
			_, fe, st, msg := resolveD81Inner(img, inner, cfg.Compat.FallbackPRGExtension)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			sum, err := crc32D81File(imgAbs, fe)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			e := proto.NewEncoder(4)
			e.WriteU32(sum)
			return proto.StatusOK, e.Bytes(), ""
		}
	}
	abs, _, err := resolveReadPathWithCompat(cfg, rootAbs, p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		return proto.StatusInvalidPath, nil, err.Error()
	}
	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !st.Exists {
		return proto.StatusNotFound, nil, "not found"
	}
	if st.IsDir {
		return proto.StatusIsADir, nil, "is a directory"
	}
	f, err := os.Open(abs)
	if err != nil {
		if errors.Is(err, fs.ErrPermission) {
			return proto.StatusAccessDenied, nil, "access denied"
		}
		return proto.StatusInternal, nil, err.Error()
	}
	defer f.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, f); err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	sum := h.Sum32()
	e := proto.NewEncoder(4)
	e.WriteU32(sum)
	return proto.StatusOK, e.Bytes(), ""
}

func (s *Server) opSEARCH(cfg config.Config, flags byte, payload []byte, rootAbs string) (byte, []byte, string) {
	// SEARCH payload: base_path string, query string, start_index u16, max_results u16, max_scan_bytes u32.
	// Response: count u16, hits[], next_index u16.
	const (
		defaultMaxScanBytes uint32 = 4 * 1024 * 1024  // 4 MiB
		maxMaxScanBytes     uint32 = 32 * 1024 * 1024 // 32 MiB
		previewMax                 = 32
	)

	d := proto.NewDecoder(payload)
	base, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	q, err := d.ReadString(cfg.MaxPath)
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if err := validateASCIIQuery(q); err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if q == "" {
		return proto.StatusBadRequest, nil, "query must not be empty"
	}
	start, err := d.ReadU16()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	maxRes, err := d.ReadU16()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	maxScan, err := d.ReadU32()
	if err != nil {
		return proto.StatusBadRequest, nil, err.Error()
	}
	if d.Remaining() != 0 {
		return proto.StatusBadRequest, nil, "extra bytes in SEARCH"
	}

	if maxRes == 0 {
		maxRes = 10
	}
	if cfg.MaxEntries > 0 && maxRes > cfg.MaxEntries {
		maxRes = cfg.MaxEntries
	}
	if maxRes == 0 {
		maxRes = 1
	}

	if maxScan == 0 {
		maxScan = defaultMaxScanBytes
	}
	maxScan = clampU32(uint64(maxScan))
	if maxScan > maxMaxScanBytes {
		maxScan = maxMaxScanBytes
	}
	if maxScan < uint32(len(q)) {
		maxScan = uint32(len(q))
	}

	caseInsensitive := flags&proto.FlagS_CASE_INSENSITIVE != 0
	recursive := flags&proto.FlagS_RECURSIVE != 0
	wholeWord := flags&proto.FlagS_WHOLE_WORD != 0

	baseAbs, err := fsops.ToOSPath(rootAbs, base)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, baseAbs, false); err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	st, err := fsops.Stat(baseAbs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !st.Exists {
		return proto.StatusNotFound, nil, "not found"
	}
	if !st.IsDir {
		return proto.StatusNotADir, nil, "not a directory"
	}

	// Collect candidate files (abs path + canonical w64 path).
	type fileEnt struct {
		abs string
		w64 string
		key string
	}
	var files []fileEnt

	if recursive {
		walkErr := filepath.WalkDir(baseAbs, func(p string, de fs.DirEntry, werr error) error {
			if werr != nil {
				return werr
			}
			// Reject symlinks.
			if de.Type()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink not allowed")
			}
			if de.IsDir() {
				return nil
			}
			w64p, err := osAbsToW64Path(rootAbs, p)
			if err != nil {
				return err
			}
			files = append(files, fileEnt{abs: p, w64: w64p, key: strings.ToUpper(w64p)})
			return nil
		})
		if walkErr != nil {
			// treat as invalid path if we encounter symlinks or traversal problems
			if strings.Contains(walkErr.Error(), "symlink") {
				return proto.StatusInvalidPath, nil, walkErr.Error()
			}
			return proto.StatusInternal, nil, walkErr.Error()
		}
	} else {
		ents, err := os.ReadDir(baseAbs)
		if err != nil {
			if errors.Is(err, fs.ErrPermission) {
				return proto.StatusAccessDenied, nil, "access denied"
			}
			return proto.StatusInternal, nil, err.Error()
		}
		sort.Slice(ents, func(i, j int) bool {
			return strings.ToUpper(ents[i].Name()) < strings.ToUpper(ents[j].Name())
		})
		for _, de := range ents {
			if de.Type()&os.ModeSymlink != 0 {
				return proto.StatusInvalidPath, nil, "symlink not allowed"
			}
			if de.IsDir() {
				continue
			}
			p := filepath.Join(baseAbs, de.Name())
			w64p, err := osAbsToW64Path(rootAbs, p)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			files = append(files, fileEnt{abs: p, w64: w64p, key: strings.ToUpper(w64p)})
		}
	}

	sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })

	qBytes := []byte(q)
	qFold := qBytes
	if caseInsensitive {
		qFold = foldASCIIUpper(qBytes)
	}
	if len(qFold) == 0 {
		return proto.StatusBadRequest, nil, "query must not be empty"
	}

	startIdx := uint32(start)
	maxResU := uint32(maxRes)
	var globalIdx uint32 = 0
	var count uint16 = 0
	var hasMore bool = false
	var incomplete bool = false
	var scanBudget uint32 = maxScan

	resp := make([]byte, 0, 256)
	resp = append(resp, 0, 0) // count placeholder

	for _, fe := range files {
		if hasMore {
			break
		}
		if scanBudget == 0 {
			incomplete = true
			break
		}

		// Re-check symlink safety right before opening (best effort).
		if err := fsops.LstatNoSymlink(rootAbs, fe.abs, false); err != nil {
			return proto.StatusInvalidPath, nil, err.Error()
		}

		f, err := os.Open(fe.abs)
		if err != nil {
			// File might have disappeared; skip.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if errors.Is(err, fs.ErrPermission) {
				return proto.StatusAccessDenied, nil, "access denied"
			}
			return proto.StatusInternal, nil, err.Error()
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return proto.StatusInternal, nil, err.Error()
		}
		fileSize := uint64(fi.Size())

		queryLen := len(qFold)
		tailLen := queryLen - 1

		bufSize := cfg.MaxChunk
		if bufSize < 4096 {
			bufSize = 4096
		}
		if bufSize < uint16(queryLen*2) {
			bufSize = uint16(queryLen * 2)
		}
		buf := make([]byte, bufSize)
		foldBuf := make([]byte, bufSize)

		prevFold := make([]byte, 0, tailLen)
		prevRaw := make([]byte, 0, tailLen)
		hayFold := make([]byte, 0, tailLen+int(bufSize))
		hayRaw := make([]byte, 0, tailLen+int(bufSize))

		var filePos uint64 = 0

		for scanBudget > 0 {
			readSize := int(bufSize)
			if uint32(readSize) > scanBudget {
				readSize = int(scanBudget)
			}
			n, rerr := f.Read(buf[:readSize])
			if n > 0 {
				scanBudget -= uint32(n)
				raw := buf[:n]
				folded := raw
				if caseInsensitive {
					for i := 0; i < n; i++ {
						b := raw[i]
						if b >= 'a' && b <= 'z' {
							b = b - 'a' + 'A'
						}
						foldBuf[i] = b
					}
					folded = foldBuf[:n]
				}

				prevLen := len(prevRaw)
				hayFold = hayFold[:0]
				hayFold = append(hayFold, prevFold...)
				hayFold = append(hayFold, folded...)
				hayRaw = hayRaw[:0]
				hayRaw = append(hayRaw, prevRaw...)
				hayRaw = append(hayRaw, raw...)

				searchStart := 0
				for {
					idx := bytes.Index(hayFold[searchStart:], qFold)
					if idx < 0 {
						break
					}
					mpos := searchStart + idx
					matchOff := (filePos - uint64(prevLen)) + uint64(mpos)

					if wholeWord {
						if !wholeWordOK(f, fileSize, matchOff, uint64(queryLen)) {
							searchStart = mpos + queryLen
							continue
						}
					}

					if globalIdx < startIdx {
						globalIdx++
					} else if uint32(count) < maxResU {
						preview, perr := readPreviewAt(f, fileSize, matchOff, previewMax)
						if perr != nil {
							f.Close()
							return proto.StatusInternal, nil, perr.Error()
						}
						tmp := proto.NewEncoder(64)
						_ = tmp.WriteString(fe.w64)
						tmp.WriteU32(clampU32(matchOff))
						tmp.WriteU16(uint16(len(preview)))
						tmp.WriteBytes(preview)
						hit := tmp.Bytes()
						if len(resp)+len(hit)+2 > int(cfg.MaxPayload) {
							hasMore = true
							break
						}
						resp = append(resp, hit...)
						count++
						globalIdx++
					} else {
						hasMore = true
						globalIdx++
						break
					}

					searchStart = mpos + queryLen
				}
				if hasMore {
					break
				}

				if tailLen > 0 {
					if len(hayFold) > tailLen {
						prevFold = prevFold[:0]
						prevFold = append(prevFold, hayFold[len(hayFold)-tailLen:]...)
						prevRaw = prevRaw[:0]
						prevRaw = append(prevRaw, hayRaw[len(hayRaw)-tailLen:]...)
					} else {
						prevFold = prevFold[:0]
						prevFold = append(prevFold, hayFold...)
						prevRaw = prevRaw[:0]
						prevRaw = append(prevRaw, hayRaw...)
					}
				}

				filePos += uint64(n)
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				f.Close()
				return proto.StatusInternal, nil, rerr.Error()
			}
		}
		f.Close()

		if scanBudget == 0 && !hasMore {
			// We might still have more files/hits.
			incomplete = true
			break
		}
	}

	// Patch count and append next_index.
	binary.LittleEndian.PutUint16(resp[0:2], count)
	var next uint16 = 0xFFFF
	if hasMore || incomplete {
		nc := uint32(start) + uint32(count)
		if nc > 0xFFFE {
			nc = 0xFFFE
		}
		next = uint16(nc)
	}
	resp = append(resp, 0, 0)
	binary.LittleEndian.PutUint16(resp[len(resp)-2:], next)

	if len(resp) > int(cfg.MaxPayload) {
		return proto.StatusTooLarge, nil, "response too large"
	}
	return proto.StatusOK, resp, ""
}

func validateASCIIQuery(q string) error {
	for i := 0; i < len(q); i++ {
		b := q[i]
		if b == 0x00 {
			return fmt.Errorf("query contains NUL")
		}
		if b < 0x20 || b == 0x7F {
			return fmt.Errorf("query contains non-printable ASCII")
		}
	}
	return nil
}

func foldASCIIUpper(b []byte) []byte {
	out := make([]byte, len(b))
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			c = c - 'a' + 'A'
		}
		out[i] = c
	}
	return out
}

func isWordChar(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}

func wholeWordOK(f *os.File, fileSize uint64, matchOff uint64, qlen uint64) bool {
	// Check left boundary
	if matchOff > 0 {
		var b [1]byte
		if _, err := f.ReadAt(b[:], int64(matchOff-1)); err == nil {
			if isWordChar(b[0]) {
				return false
			}
		} else {
			// If we can't read it (e.g. race), be conservative.
			return false
		}
	}
	// Check right boundary
	next := matchOff + qlen
	if next < fileSize {
		var b [1]byte
		if _, err := f.ReadAt(b[:], int64(next)); err == nil {
			if isWordChar(b[0]) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func readPreviewAt(f *os.File, fileSize uint64, off uint64, max int) ([]byte, error) {
	if off >= fileSize || max <= 0 {
		return []byte{}, nil
	}
	remain := fileSize - off
	n := uint64(max)
	if remain < n {
		n = remain
	}
	if n == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, n)
	r, err := f.ReadAt(buf, int64(off))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:r], nil
}

func osAbsToW64Path(rootAbs string, abs string) (string, error) {
	rel, err := filepath.Rel(rootAbs, abs)
	if err != nil {
		return "", err
	}
	rel = filepath.ToSlash(rel)
	if rel == "." {
		return "/", nil
	}
	// Ensure leading slash.
	if !strings.HasPrefix(rel, "/") {
		rel = "/" + rel
	}
	// The server canonicalizes paths to uppercase.
	return strings.ToUpper(rel), nil
}

func (s *Server) opMKDIR(cfg config.Config, limits Limits, flags byte, payload []byte, rootAbs string) (byte, []byte, string) {
	// MKDIR flags: PARENTS (mkdir -p).
	d := proto.NewDecoder(payload)
	p, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if d.Remaining() != 0 {
		return proto.StatusBadRequest, nil, "extra bytes in MKDIR"
	}

	// Special case: when disk images are enabled and images are treated as directories,
	// MKDIR on "foo.d64" should create an empty image file.
	imgKind, isImg := detectDiskImageMountRootPath(p)
	if isImg {
		// Do not allow creating images *inside* other mounted images.
		if hasAnyDiskImageParent(p) {
			isImg = false
		}
		// Only enabled when the disk image feature is enabled for this request.
		if !limits.DiskImagesEnabled {
			isImg = false
		}
	}

	// MKDIR inside mounted disk images:
	//   - D64/D71: no subdirectories (NOT_SUPPORTED)
	//   - D81: create 1581 partition directories
	if limits.DiskImagesEnabled {
		if _, inner, ok := splitD64Path(p); ok && inner != "" {
			return proto.StatusNotSupported, nil, "D64 subdirectories are not supported"
		}
		if _, inner, ok := splitD71Path(p); ok && inner != "" {
			return proto.StatusNotSupported, nil, "D71 subdirectories are not supported"
		}
		if mountPath, inner, ok := splitD81Path(p); ok && inner != "" {
			if !limits.DiskImagesWriteEnabled {
				return proto.StatusAccessDenied, nil, "disk images are read-only"
			}

			// If the target already exists as a directory inside the image, behave like mkdir on an existing dir: OK.
			imgAbs, img, st, msg := resolveD81Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			inner = strings.Trim(strings.TrimSpace(inner), "/")
			if d81InnerIsDirEntry(img, inner) {
				return proto.StatusOK, nil, ""
			}
			// If the leaf exists as a file, report ALREADY_EXISTS.
			parent := ""
			leaf := inner
			if i := strings.LastIndex(inner, "/"); i >= 0 {
				parent = inner[:i]
				leaf = inner[i+1:]
			}
			_, m, _, _, stDir, _ := resolveD81Dir(img, parent)
			if stDir == proto.StatusOK {
				key := strings.ToUpper(strings.TrimSpace(leaf))
				if fe, ok := m[key]; ok && fe != nil {
					return proto.StatusAlreadyExists, nil, "already exists"
				}
			}

			parents := flags&proto.FlagMK_PARENTS != 0
			if parents && !cfg.EnableMkdirParents {
				return proto.StatusNotSupported, nil, "MKDIR PARENTS not supported"
			}

			// Repack-based directory creation inside the image.
			if err := diskimage.MkdirDirD81(imgAbs, inner, parents); err != nil {
				var se *diskimage.StatusError
				if errors.As(err, &se) {
					return se.Status(), nil, se.Error()
				}
				return proto.StatusInternal, nil, err.Error()
			}
			return proto.StatusOK, nil, ""
		}
	}

	abs, err := fsops.ToOSPath(rootAbs, p)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, abs, true); err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}

	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if st.Exists {
		// v0.2.2 clarification: if dir exists => OK; if file exists => ALREADY_EXISTS.
		// When disk images are enabled, an image file is treated like a directory mount,
		// therefore MKDIR on an existing "image-dir" is OK.
		if st.IsDir || isImg {
			return proto.StatusOK, nil, ""
		}
		return proto.StatusAlreadyExists, nil, "already exists"
	}

	parents := flags&proto.FlagMK_PARENTS != 0
	if parents && !cfg.EnableMkdirParents {
		return proto.StatusNotSupported, nil, "MKDIR PARENTS not supported"
	}

	if isImg {
		// Creating a disk image is a write operation; serialize with other writes.
		s.writeMu.Lock()
		defer s.writeMu.Unlock()

		// Ensure parent directory exists.
		parent := filepath.Dir(abs)
		if parents {
			if err := os.MkdirAll(parent, 0o755); err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
		} else {
			pst, err := fsops.Stat(parent)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			if !pst.Exists || !pst.IsDir {
				return proto.StatusNotFound, nil, "parent directory missing"
			}
		}

		label := diskImageLabelFromPath(p)
		imgBytes, err := emptyDiskImageBytes(imgKind, label)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		newSize := uint64(len(imgBytes))
		if limits.MaxFileBytes > 0 && newSize > limits.MaxFileBytes {
			return proto.StatusTooLarge, nil, "max file size exceeded"
		}

		var usedBefore uint64
		haveUsed := false
		if limits.QuotaBytes > 0 {
			used, err := s.rootUsageBytes(rootAbs)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			usedBefore = used
			haveUsed = true
			if usedBefore+newSize > limits.QuotaBytes {
				return proto.StatusTooLarge, nil, "quota exceeded"
			}
		} else if s.usage != nil {
			used, err := s.rootUsageBytes(rootAbs)
			if err == nil {
				usedBefore = used
				haveUsed = true
			}
		}

		f, err := os.OpenFile(abs, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				return proto.StatusOK, nil, ""
			}
			if errors.Is(err, fs.ErrPermission) {
				return proto.StatusAccessDenied, nil, "access denied"
			}
			return proto.StatusInternal, nil, err.Error()
		}
		ok := false
		defer func() {
			_ = f.Close()
			if !ok {
				_ = os.Remove(abs)
			}
		}()
		if _, err := f.Write(imgBytes); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, nil, err.Error()
		}
		_ = f.Sync()
		ok = true
		if haveUsed && s.usage != nil {
			s.setRootUsageBytes(rootAbs, usedBefore+newSize)
		}
		return proto.StatusOK, nil, ""
	}

	if parents {
		if err := os.MkdirAll(abs, 0o755); err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		return proto.StatusOK, nil, ""
	}
	// Non-parents: parent must exist.
	parent := filepath.Dir(abs)
	pst, err := fsops.Stat(parent)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !pst.Exists || !pst.IsDir {
		return proto.StatusNotFound, nil, "parent directory missing"
	}
	if err := os.Mkdir(abs, 0o755); err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	return proto.StatusOK, nil, ""
}

func (s *Server) opRMDIR(cfg config.Config, limits Limits, flags byte, payload []byte, rootAbs string) (byte, []byte, string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	d := proto.NewDecoder(payload)

	p, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusBadReq, nil, err.Error()
	}
	if d.Len() != 0 {
		return proto.StatusBadReq, nil, "extra payload"
	}
	if p == "/" {
		return proto.StatusBadReq, nil, "cannot remove root"
	}

	recursive := (flags & proto.FlagRD_RECURSIVE) != 0
	if recursive && !cfg.EnableRmdirRecursive {
		return proto.StatusNotSupported, nil, "recursive rmdir disabled"
	}

	// Special case: when disk images are enabled and images are treated as directories,
	// RMDIR on "foo.d64" should delete the image file.
	_, isImg := detectDiskImageMountRootPath(p)
	if isImg {
		if hasAnyDiskImageParent(p) {
			isImg = false
		}
		if !limits.DiskImagesEnabled {
			isImg = false
		}
	}

	// RMDIR inside mounted disk images:
	//   - D64/D71: no subdirectories (NOT_SUPPORTED)
	//   - D81: remove 1581 partition directories
	if limits.DiskImagesEnabled {
		if _, inner, ok := splitD64Path(p); ok && inner != "" {
			return proto.StatusNotSupported, nil, "D64 subdirectories are not supported"
		}
		if _, inner, ok := splitD71Path(p); ok && inner != "" {
			return proto.StatusNotSupported, nil, "D71 subdirectories are not supported"
		}
		if mountPath, inner, ok := splitD81Path(p); ok && inner != "" {
			if !limits.DiskImagesWriteEnabled {
				return proto.StatusAccessDenied, nil, "disk images are read-only"
			}
			imgAbs, _, st, msg := resolveD81Mount(rootAbs, mountPath)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			inner = strings.Trim(strings.TrimSpace(inner), "/")
			if inner == "" {
				return proto.StatusBadRequest, nil, "cannot remove image root"
			}
			if err := diskimage.RmdirDirD81(imgAbs, inner, recursive); err != nil {
				var se *diskimage.StatusError
				if errors.As(err, &se) {
					return se.Status(), nil, se.Error()
				}
				return proto.StatusInternal, nil, err.Error()
			}
			return proto.StatusOK, nil, ""
		}
	}

	abs, err := fsops.ToOSPath(rootAbs, p)
	if err != nil {
		return proto.StatusBadPath, nil, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, abs, false); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		if errors.Is(err, fs.ErrPermission) {
			return proto.StatusAccessDenied, nil, "access denied"
		}
		return proto.StatusBadPath, nil, err.Error()
	}
	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !st.Exists {
		return proto.StatusNotFound, nil, "not found"
	}
	if isImg && !st.IsDir {
		// Trash behavior: keep data under TrashDir instead of deleting permanently.
		if shouldUseTrash(cfg, rootAbs, abs) {
			if _, err := s.moveToTrash(cfg, rootAbs, abs); err != nil {
				if errors.Is(err, fs.ErrPermission) {
					return proto.StatusAccessDenied, nil, "access denied"
				}
				return proto.StatusInternal, nil, err.Error()
			}
			s.invalidateRootUsage(rootAbs)
			return proto.StatusOK, nil, ""
		}
		if err := os.Remove(abs); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return proto.StatusNotFound, nil, "not found"
			}
			if errors.Is(err, fs.ErrPermission) {
				return proto.StatusAccessDenied, nil, "access denied"
			}
			return proto.StatusInternal, nil, err.Error()
		}
		s.invalidateRootUsage(rootAbs)
		return proto.StatusOK, nil, ""
	}
	if !st.IsDir {
		return proto.StatusNotDir, nil, "not a dir"
	}

	// Trash behavior: keep data under TrashDir instead of deleting permanently.
	if shouldUseTrash(cfg, rootAbs, abs) {
		if !recursive {
			ents, err := os.ReadDir(abs)
			if err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			if len(ents) > 0 {
				return proto.StatusDirNotEmpty, nil, "dir not empty"
			}
		}
		if _, err := s.moveToTrash(cfg, rootAbs, abs); err != nil {
			if errors.Is(err, fs.ErrPermission) {
				return proto.StatusAccessDenied, nil, "access denied"
			}
			return proto.StatusInternal, nil, err.Error()
		}
		return proto.StatusOK, nil, ""
	}

	if recursive {
		if err := os.RemoveAll(abs); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return proto.StatusNotFound, nil, "not found"
			}
			if errors.Is(err, fs.ErrPermission) {
				return proto.StatusAccessDenied, nil, "access denied"
			}
			return proto.StatusInternal, nil, err.Error()
		}
		// Removing frees space, so invalidate quota usage cache.
		s.invalidateRootUsage(rootAbs)
		return proto.StatusOK, nil, ""
	}

	if err := os.Remove(abs); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		if errors.Is(err, fs.ErrPermission) {
			return proto.StatusAccessDenied, nil, "access denied"
		}
		if strings.Contains(strings.ToLower(err.Error()), "not empty") {
			return proto.StatusDirNotEmpty, nil, "dir not empty"
		}
		return proto.StatusInternal, nil, err.Error()
	}
	return proto.StatusOK, nil, ""
}

func (s *Server) opRM(cfg config.Config, limits Limits, payload []byte, rootAbs string) (byte, []byte, string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	d := proto.NewDecoder(payload)
	p, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusBadReq, nil, err.Error()
	}
	if d.Len() != 0 {
		return proto.StatusBadReq, nil, "extra payload"
	}
	if p == "/" {
		return proto.StatusBadReq, nil, "cannot remove root"
	}

	// Disk image delete support (D64)
	if mountPath, inner, ok := splitD64Path(p); ok && inner != "" {
		inner = normalizeDiskImageLeafName(inner, cfg.Compat.FallbackPRGExtension)
		if !limits.DiskImagesEnabled {
			return proto.StatusNotSupported, nil, "disk images are disabled"
		}
		if !limits.DiskImagesWriteEnabled {
			return proto.StatusAccessDenied, nil, "disk images are read-only"
		}
		imgAbs, _, st, msg := resolveD64Mount(rootAbs, mountPath)
		if st != proto.StatusOK {
			return st, nil, msg
		}
		if strings.Contains(inner, "/") {
			return proto.StatusNotSupported, nil, "D64 subdirectories are not supported"
		}
		if err := diskimage.DeleteFileD64(imgAbs, inner); err != nil {
			var se *diskimage.StatusError
			if errors.As(err, &se) {
				return se.Status(), nil, se.Error()
			}
			return proto.StatusInternal, nil, err.Error()
		}
		return proto.StatusOK, nil, ""
	}

	// Disk image delete support (D71)
	if mountPath, inner, ok := splitD71Path(p); ok && inner != "" {
		inner = normalizeDiskImageLeafName(inner, cfg.Compat.FallbackPRGExtension)
		if !limits.DiskImagesEnabled {
			return proto.StatusNotSupported, nil, "disk images are disabled"
		}
		if !limits.DiskImagesWriteEnabled {
			return proto.StatusAccessDenied, nil, "disk images are read-only"
		}
		if strings.Contains(inner, "/") {
			return proto.StatusNotSupported, nil, "D71 has no subdirectories"
		}
		if strings.ContainsAny(inner, "*?") {
			return proto.StatusBadRequest, nil, "wildcards not allowed"
		}
		imgAbs, _, st, msg := resolveD71Mount(rootAbs, mountPath)
		if st != proto.StatusOK {
			return st, nil, msg
		}
		if err := diskimage.DeleteFileD71(imgAbs, inner); err != nil {
			var se *diskimage.StatusError
			if errors.As(err, &se) {
				return se.Status(), nil, se.Error()
			}
			return proto.StatusInternal, nil, err.Error()
		}
		return proto.StatusOK, nil, ""
	}

	// Disk image delete support (D81)
	if mountPath, inner, ok := splitD81Path(p); ok && inner != "" {
		inner = normalizeDiskImageLeafName(inner, cfg.Compat.FallbackPRGExtension)
		if !limits.DiskImagesEnabled {
			return proto.StatusNotSupported, nil, "disk images are disabled"
		}
		if !limits.DiskImagesWriteEnabled {
			return proto.StatusAccessDenied, nil, "disk images are read-only"
		}
		if strings.ContainsAny(inner, "*?") {
			return proto.StatusBadRequest, nil, "wildcards not allowed"
		}
		imgAbs, _, st, msg := resolveD81Mount(rootAbs, mountPath)
		if st != proto.StatusOK {
			return st, nil, msg
		}
		if err := diskimage.DeleteFileD81(imgAbs, inner); err != nil {
			var se *diskimage.StatusError
			if errors.As(err, &se) {
				return se.Status(), nil, se.Error()
			}
			return proto.StatusInternal, nil, err.Error()
		}
		return proto.StatusOK, nil, ""
	}

	abs, err := fsops.ToOSPath(rootAbs, p)
	if err != nil {
		return proto.StatusBadPath, nil, err.Error()
	}
	if err := fsops.LstatNoSymlink(rootAbs, abs, false); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		if errors.Is(err, fs.ErrPermission) {
			return proto.StatusAccessDenied, nil, "access denied"
		}
		return proto.StatusBadPath, nil, err.Error()
	}
	st, err := fsops.Stat(abs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !st.Exists {
		return proto.StatusNotFound, nil, "not found"
	}
	if st.IsDir {
		return proto.StatusIsDir, nil, "is a dir"
	}

	// Trash behavior: keep data under TrashDir instead of deleting permanently.
	if shouldUseTrash(cfg, rootAbs, abs) {
		if _, err := s.moveToTrash(cfg, rootAbs, abs); err != nil {
			if errors.Is(err, fs.ErrPermission) {
				return proto.StatusAccessDenied, nil, "access denied"
			}
			return proto.StatusInternal, nil, err.Error()
		}
		return proto.StatusOK, nil, ""
	}

	// Hard delete.
	oldSize := st.Size
	if err := os.Remove(abs); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return proto.StatusNotFound, nil, "not found"
		}
		if errors.Is(err, fs.ErrPermission) {
			return proto.StatusAccessDenied, nil, "access denied"
		}
		return proto.StatusInternal, nil, err.Error()
	}
	// Update cached used bytes (if present).
	if s.usage != nil {
		usedBefore, err := s.rootUsageBytes(rootAbs)
		if err == nil {
			s.setRootUsageBytes(rootAbs, applyDeltaBytes(usedBefore, -int64(oldSize)))
		}
	}
	return proto.StatusOK, nil, ""
}

func (s *Server) opCP(cfg config.Config, limits Limits, flags byte, payload []byte, rootAbs string) (byte, []byte, string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	overwrite := (flags & 0x01) != 0
	recursive := (flags & 0x02) != 0

	d := proto.NewDecoder(payload)

	// NOTE: src uses the "read" path rules so wildcard patterns (*, ?) can be used
	// in the last path segment (same rule as LOAD wildcard compatibility).
	srcNorm, err := s.readPathStringRead(cfg, d)
	if err != nil {
		return proto.StatusBadRequest, nil, "invalid src path: " + err.Error()
	}
	dstNorm, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusBadRequest, nil, "invalid dst path: " + err.Error()
	}
	if d.Remaining() != 0 {
		return proto.StatusBadRequest, nil, "trailing payload data"
	}

	// Cannot copy the virtual root itself.
	if srcNorm == "/" {
		return proto.StatusIsADir, nil, "source is a directory"
	}

	// Disk images:
	//   - allow extracting FROM a mounted image to the filesystem
	//   - allow copying INTO a mounted image (write-enabled)
	//   - allow copying files between mounted images (write-enabled)
	if limits.DiskImagesEnabled {
		// Disk image -> disk image copy (single files).
		if srcMount, srcInner, ok := splitD64Path(srcNorm); ok && srcInner != "" {
			if dstMount, dstInner, ok2 := splitD64Path(dstNorm); ok2 {
				st, msg := s.cpD64ToD64(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
			if dstMount, dstInner, ok2 := splitD71Path(dstNorm); ok2 {
				st, msg := s.cpD64ToD71(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
			if dstMount, dstInner, ok2 := splitD81Path(dstNorm); ok2 {
				st, msg := s.cpD64ToD81(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
		}
		if srcMount, srcInner, ok := splitD71Path(srcNorm); ok && srcInner != "" {
			if dstMount, dstInner, ok2 := splitD64Path(dstNorm); ok2 {
				st, msg := s.cpD71ToD64(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
			if dstMount, dstInner, ok2 := splitD71Path(dstNorm); ok2 {
				st, msg := s.cpD71ToD71(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
			if dstMount, dstInner, ok2 := splitD81Path(dstNorm); ok2 {
				st, msg := s.cpD71ToD81(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
		}
		if srcMount, srcInner, ok := splitD81Path(srcNorm); ok && srcInner != "" {
			if dstMount, dstInner, ok2 := splitD64Path(dstNorm); ok2 {
				st, msg := s.cpD81ToD64(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
			if dstMount, dstInner, ok2 := splitD71Path(dstNorm); ok2 {
				st, msg := s.cpD81ToD71(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
			if dstMount, dstInner, ok2 := splitD81Path(dstNorm); ok2 {
				st, msg := s.cpD81ToD81(cfg, limits, rootAbs, srcMount, srcInner, dstMount, dstInner, overwrite)
				return st, nil, msg
			}
		}

		// Copy/extract FROM a disk image to the filesystem.
		if mountPath, inner, ok := splitD64Path(srcNorm); ok && inner != "" {
			st, msg := s.cpFromD64(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite, recursive)
			return st, nil, msg
		}
		if mountPath, inner, ok := splitD71Path(srcNorm); ok && inner != "" {
			st, msg := s.cpFromD71(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite, recursive)
			return st, nil, msg
		}
		if mountPath, inner, ok := splitD81Path(srcNorm); ok && inner != "" {
			st, msg := s.cpFromD81(cfg, limits, rootAbs, mountPath, inner, dstNorm, overwrite, recursive)
			return st, nil, msg
		}

		// Copy INTO a D64 image.
		//
		// Important: if dstInner is empty it refers to the mount root (directory view).
		// We treat that as a directory target, not as "overwrite the .d64 file".
		//
		// To still allow copying disk images around, we fall back to normal filesystem
		// semantics if the *source* itself is a disk image file and the destination is
		// the image root.
		if dstMount, dstInner, ok := splitD64Path(dstNorm); ok {
			srcBase := path.Base(srcNorm)
			srcLooksLikeImage := strings.HasSuffix(strings.ToLower(srcBase), ".d64") ||
				strings.HasSuffix(strings.ToLower(srcBase), ".d71") ||
				strings.HasSuffix(strings.ToLower(srcBase), ".d81")
			if !(dstInner == "" && srcLooksLikeImage) {
				if !limits.DiskImagesWriteEnabled {
					return proto.StatusAccessDenied, nil, "disk images are read-only"
				}

				// Bulk copy (wildcards) into D64 root is supported.
				lastSeg := srcNorm
				if i := strings.LastIndex(srcNorm, "/"); i >= 0 {
					lastSeg = srcNorm[i+1:]
				}
				hasWildcard := strings.ContainsAny(lastSeg, "*?")
				if hasWildcard {
					if dstInner != "" {
						return proto.StatusBadRequest, nil, "bulk copy destination must be the image root"
					}
					// Split src into parent dir + wildcard pattern (last segment).
					srcDir := "/"
					pat := srcNorm
					if j := strings.LastIndex(srcNorm, "/"); j >= 0 {
						pat = srcNorm[j+1:]
						if j == 0 {
							srcDir = "/"
						} else {
							srcDir = srcNorm[:j]
						}
					}
					st, msg := s.cpBulkFSToD64(cfg, limits, rootAbs, srcDir, pat, dstMount, overwrite)
					return st, nil, msg
				}

				st, msg := s.cpToD64(cfg, limits, rootAbs, dstMount, dstInner, srcNorm, overwrite)
				return st, nil, msg
			}
		}
		// D71 destination: allow copy from filesystem into D71 (root only).
		if dstMount, dstInner, ok := splitD71Path(dstNorm); ok {
			srcBase := path.Base(srcNorm)
			srcUpper := strings.ToUpper(srcBase)
			srcLooksLikeImage := strings.HasSuffix(srcUpper, ".D64") || strings.HasSuffix(srcUpper, ".D71") || strings.HasSuffix(srcUpper, ".D81")
			// If both sides look like disk images and the destination points to the mount root,
			// we assume the user wants to copy the image file itself (filesystem-level).
			if !(dstInner == "" && srcLooksLikeImage) {
				if !limits.DiskImagesWriteEnabled {
					return proto.StatusAccessDenied, nil, "disk images are read-only"
				}
				if strings.Contains(dstInner, "/") {
					return proto.StatusNotSupported, nil, "D71 has no subdirectories"
				}

				// Bulk copy (wildcards) into D71 root.
				lastSeg := srcNorm
				if i := strings.LastIndex(srcNorm, "/"); i >= 0 {
					lastSeg = srcNorm[i+1:]
				}
				hasWildcard := strings.ContainsAny(lastSeg, "*?")
				if hasWildcard {
					if dstInner != "" {
						return proto.StatusBadRequest, nil, "bulk copy destination must be the image root"
					}
					// Split src into parent dir + wildcard pattern (last segment).
					srcDir := "/"
					pat := srcNorm
					if j := strings.LastIndex(srcNorm, "/"); j >= 0 {
						pat = srcNorm[j+1:]
						if j == 0 {
							srcDir = "/"
						} else {
							srcDir = srcNorm[:j]
						}
					}
					st, msg := s.cpBulkFSToD71(cfg, limits, rootAbs, srcDir, pat, dstMount, overwrite)
					return st, nil, msg
				}

				st, msg := s.cpToD71(cfg, limits, rootAbs, dstMount, dstInner, srcNorm, overwrite)
				return st, nil, msg
			}
		}
		if dstMount, dstInner, ok := splitD81Path(dstNorm); ok {
			// If the destination is the D81 mount root and the source looks like a
			// disk image, assume the user wants to copy the image file itself, not
			// write into the image.
			srcBase := path.Base(srcNorm)
			srcUpper := strings.ToUpper(srcBase)
			srcLooksLikeImage := strings.HasSuffix(srcUpper, ".D64") || strings.HasSuffix(srcUpper, ".D71") || strings.HasSuffix(srcUpper, ".D81")
			if dstInner == "" && srcLooksLikeImage {
				// Let filesystem copy logic handle it.
			} else {
				if !limits.DiskImagesWriteEnabled {
					return proto.StatusAccessDenied, nil, "disk images are read-only"
				}
				if !limits.DiskImagesEnabled {
					return proto.StatusAccessDenied, nil, "disk images are disabled"
				}

				// Detect wildcard copy (bulk) on the source.
				lastSeg := srcNorm
				if i := strings.LastIndex(srcNorm, "/"); i >= 0 {
					lastSeg = srcNorm[i+1:]
				}
				if strings.ContainsAny(lastSeg, "*?") {
					if dstInner != "" {
						return proto.StatusBadRequest, nil, "destination inside disk image must be the image root for wildcard copy"
					}
					// Split src into parent dir + wildcard pattern (last segment).
					srcDir := "/"
					pat := srcNorm
					if j := strings.LastIndex(srcNorm, "/"); j >= 0 {
						pat = srcNorm[j+1:]
						if j == 0 {
							srcDir = "/"
						} else {
							srcDir = srcNorm[:j]
						}
					}
					st, msg := s.cpBulkFSToD81(cfg, limits, rootAbs, srcDir, pat, dstMount, overwrite)
					return st, nil, msg
				}

				st, msg := s.cpToD81(cfg, limits, rootAbs, dstMount, dstInner, srcNorm, overwrite, recursive)
				return st, nil, msg
			}
		}
	}

	// Bulk copy on the filesystem: wildcard patterns in the last segment.
	if dirNorm, leaf := splitDirBase(srcNorm); strings.ContainsAny(leaf, "*?") {
		st, msg := s.cpBulkFS(cfg, limits, rootAbs, dirNorm, leaf, dstNorm, overwrite, recursive)
		return st, nil, msg
	}

	// Single-path copy (filesystem -> filesystem), unchanged behavior.
	srcAbs, err := fsops.ToOSPath(rootAbs, srcNorm)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	dstAbs, err := fsops.ToOSPath(rootAbs, dstNorm)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}

	if err := fsops.LstatNoSymlink(rootAbs, srcAbs, false); err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	srcSt, err := fsops.Stat(srcAbs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !srcSt.Exists {
		return proto.StatusNotFound, nil, "source not found"
	}

	dstSt, err := fsops.Stat(dstAbs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	dstIsDir := dstSt.Exists && dstSt.IsDir

	if dstIsDir {
		dstAbs = filepath.Join(dstAbs, filepath.Base(srcAbs))
		dstSt, err = fsops.Stat(dstAbs)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
	}

	var usedBefore uint64
	haveUsed := limits.QuotaBytes > 0 && s.usage != nil
	if haveUsed {
		usedBefore, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
	}

	srcTotal, srcMax, err := pathSizeBytes(srcAbs)
	if err != nil {
		return proto.StatusInvalidPath, nil, err.Error()
	}
	if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
		return proto.StatusTooLarge, nil, "file too large"
	}

	// If overwriting, either move to trash or delete-in-place depending on config.
	trashOverwrite := cfg.TrashEnabled

	var dstOldTotal uint64
	if dstSt.Exists && !trashOverwrite {
		dstOldTotal, _, err = pathSizeBytes(dstAbs)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
	}

	delta := int64(srcTotal) - int64(dstOldTotal)
	if trashOverwrite && dstSt.Exists {
		delta = int64(srcTotal) // old file stays in trash => usage increases by full src size
	}

	if delta > 0 && haveUsed && usedBefore+uint64(delta) > limits.QuotaBytes {
		return proto.StatusTooLarge, nil, "quota exceeded"
	}

	if dstSt.Exists {
		if !overwrite {
			return proto.StatusAccessDenied, nil, "destination exists"
		}
		if trashOverwrite {
			if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
		} else {
			if err := os.RemoveAll(dstAbs); err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
			s.invalidateRootUsage(rootAbs)
		}
	}

	if srcSt.IsDir {
		if err := fsops.CopyDirRecursive(srcAbs, dstAbs); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, nil, err.Error()
		}
	} else {
		if err := fsops.CopyFile(srcAbs, dstAbs); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, nil, err.Error()
		}
	}

	if haveUsed && s.usage != nil {
		s.setRootUsageBytes(rootAbs, applyDeltaBytes(usedBefore, delta))
	}

	return proto.StatusOK, nil, ""
}

func (s *Server) opMV(cfg config.Config, limits Limits, flags byte, payload []byte, rootAbs string) (byte, []byte, string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	d := proto.NewDecoder(payload)

	src, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusBadReq, nil, err.Error()
	}
	dst, err := s.readPathString(cfg, d)
	if err != nil {
		return proto.StatusBadReq, nil, err.Error()
	}
	if d.Len() != 0 {
		return proto.StatusBadReq, nil, "extra payload"
	}
	if src == "/" {
		return proto.StatusBadPath, nil, "cannot move root"
	}

	// Disk image move/rename support (D64)
	if srcMount, srcInner, ok := splitD64Path(src); ok && srcInner != "" {
		if dstMount, dstInner, ok2 := splitD64Path(dst); ok2 && dstInner != "" && dstMount == srcMount {
			srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
			dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)
			if !limits.DiskImagesEnabled {
				return proto.StatusNotSupported, nil, "disk images are disabled"
			}
			if !limits.DiskImagesWriteEnabled {
				return proto.StatusAccessDenied, nil, "disk images are read-only"
			}

			overwrite := flags&proto.FlagMV_OVERWRITE != 0
			if overwrite && !cfg.EnableOverwrite {
				return proto.StatusNotSupported, nil, "overwrite disabled by server"
			}
			allowOverwrite := cfg.EnableOverwrite && overwrite

			imgAbs, _, st, msg := resolveD64Mount(rootAbs, srcMount)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			if strings.Contains(srcInner, "/") || strings.Contains(dstInner, "/") {
				return proto.StatusNotSupported, nil, "D64 subdirectories are not supported"
			}

			if err := diskimage.RenameFileD64(imgAbs, srcInner, dstInner, allowOverwrite); err != nil {
				var se *diskimage.StatusError
				if errors.As(err, &se) {
					return se.Status(), nil, se.Error()
				}
				return proto.StatusInternal, nil, err.Error()
			}
			return proto.StatusOK, nil, ""
		}

		// Source is inside a D64, but destination is not (or is another image).
		return proto.StatusNotSupported, nil, "move across disk images is not supported"
	}
	if _, dstInner, ok := splitD64Path(dst); ok && dstInner != "" {
		// Destination is inside a D64, but source is not.
		return proto.StatusNotSupported, nil, "move into disk images is not supported"
	}

	// Disk image move/rename support (D71)
	if srcMount, srcInner, ok := splitD71Path(src); ok && srcInner != "" {
		if dstMount, dstInner, ok2 := splitD71Path(dst); ok2 && dstInner != "" && dstMount == srcMount {
			srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
			dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)
			if !limits.DiskImagesEnabled {
				return proto.StatusNotSupported, nil, "disk images are disabled"
			}
			if !limits.DiskImagesWriteEnabled {
				return proto.StatusAccessDenied, nil, "disk images are read-only"
			}

			overwrite := flags&proto.FlagMV_OVERWRITE != 0
			if overwrite && !cfg.EnableOverwrite {
				return proto.StatusNotSupported, nil, "overwrite disabled by server"
			}
			allowOverwrite := cfg.EnableOverwrite && overwrite

			imgAbs, _, st, msg := resolveD71Mount(rootAbs, srcMount)
			if st != proto.StatusOK {
				return st, nil, msg
			}
			if strings.Contains(srcInner, "/") || strings.Contains(dstInner, "/") {
				return proto.StatusNotSupported, nil, "D71 subdirectories are not supported"
			}

			if err := diskimage.RenameFileD71(imgAbs, srcInner, dstInner, allowOverwrite); err != nil {
				var se *diskimage.StatusError
				if errors.As(err, &se) {
					return se.Status(), nil, se.Error()
				}
				return proto.StatusInternal, nil, err.Error()
			}
			return proto.StatusOK, nil, ""
		}

		// Source is inside a D71, but destination is not (or is another image).
		return proto.StatusNotSupported, nil, "move across disk images is not supported"
	}
	if _, dstInner, ok := splitD71Path(dst); ok && dstInner != "" {
		// Destination is inside a D71, but source is not.
		return proto.StatusNotSupported, nil, "move into disk images is not supported"
	}
	if srcMount, srcInner, ok := splitD81Path(src); ok && srcInner != "" {
		if !limits.DiskImagesWriteEnabled {
			return proto.StatusAccessDenied, nil, "disk images are read-only"
		}
		if !limits.DiskImagesEnabled {
			return proto.StatusAccessDenied, nil, "disk images are disabled"
		}

		srcInner = normalizeDiskImageLeafName(srcInner, cfg.Compat.FallbackPRGExtension)
		if strings.ContainsAny(srcInner, "*?") {
			return proto.StatusBadRequest, nil, "wildcards are not allowed in mv"
		}

		imgAbs, _, st, msg := resolveD81Mount(rootAbs, srcMount)
		if st != proto.StatusOK {
			return st, nil, msg
		}

		if dstMount, dstInner, ok := splitD81Path(dst); ok && dstInner != "" {
			if dstMount != srcMount {
				return proto.StatusNotSupported, nil, "move across disk images is not supported"
			}
			dstInner = normalizeDiskImageLeafName(dstInner, cfg.Compat.FallbackPRGExtension)
			if strings.ContainsAny(dstInner, "*?") {
				return proto.StatusBadRequest, nil, "wildcards are not allowed in mv"
			}

			allowOverwrite := (flags & proto.FlagMV_OVERWRITE) != 0
			if allowOverwrite && !cfg.EnableOverwrite {
				return proto.StatusNotSupported, nil, "overwrite disabled"
			}
			if err := diskimage.RenameFileD81(imgAbs, srcInner, dstInner, allowOverwrite); err != nil {
				// If the source is a directory/partition, try directory rename.
				var se *diskimage.StatusError
				if errors.As(err, &se) {
					if se.Status() == proto.StatusIsADir {
						if err2 := diskimage.RenameDirD81(imgAbs, srcInner, dstInner, allowOverwrite); err2 == nil {
							return proto.StatusOK, nil, ""
						} else {
							var se2 *diskimage.StatusError
							if errors.As(err2, &se2) {
								// If the directory rename failed because the source is not a directory,
								// fall back to the original error from the file-rename path.
								if se2.Status() != proto.StatusNotADir {
									return se2.Status(), nil, se2.Error()
								}
							}
						}
					}
					return se.Status(), nil, se.Error()
				}
				return proto.StatusInternal, nil, err.Error()
			}
			return proto.StatusOK, nil, ""
		}

		// Source is inside a D81, but destination is not (or is another image).
		return proto.StatusNotSupported, nil, "move across disk images is not supported"
	}
	if _, dstInner, ok := splitD81Path(dst); ok && dstInner != "" {
		// Destination is inside a D81, but source is not.
		return proto.StatusNotSupported, nil, "move into disk images is not supported"
	}

	srcAbs, err := fsops.ToOSPath(rootAbs, src)
	if err != nil {
		return proto.StatusBadPath, nil, err.Error()
	}
	srcSt, err := fsops.Stat(srcAbs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}
	if !srcSt.Exists {
		return proto.StatusNotFound, nil, "src not found"
	}

	// Guard against a common edge case when disk images are enabled:
	// renaming "foo.d64" -> "foo" would turn the image into a normal file.
	// Likewise, renaming a normal file -> "foo.d64" would make it look like an image.
	//
	// Default: conversions are NOT allowed. A token can explicitly enable them.
	if limits.DiskImagesEnabled && !limits.DiskImagesAllowRenameConvert {
		srcBase := path.Base(src)
		dstBase := path.Base(dst)
		srcExt := strings.ToLower(path.Ext(srcBase))
		dstExt := strings.ToLower(path.Ext(dstBase))

		isImgExt := func(ext string) bool {
			switch ext {
			case ".d64", ".d71", ".d81":
				return true
			default:
				return false
			}
		}

		srcIsImgFile := !srcSt.IsDir && isImgExt(srcExt)
		dstIsImg := isImgExt(dstExt)

		switch {
		case srcIsImgFile:
			// Destination is an image name.
			if dstIsImg {
				// Disallow changing the image type by rename (.d64 -> .d71 etc).
				if dstExt != srcExt {
					return proto.StatusNotSupported, nil, "changing disk image extension on rename is disabled"
				}
				break
			}
			// Destination is NOT an image name.
			if dstExt == "" {
				// If the user omitted the extension, keep the original.
				dst2 := dst + srcExt
				if len(dst2) > int(cfg.MaxPath) {
					return proto.StatusBadPath, nil, "dst path too long"
				}
				if len(path.Base(dst2)) > int(cfg.MaxName) {
					return proto.StatusBadPath, nil, "dst name too long"
				}
				dst = dst2
			} else {
				return proto.StatusNotSupported, nil, "renaming disk images to non-image names is disabled"
			}
		case !srcIsImgFile && dstIsImg:
			// Prevent turning a normal file into a mounted image just by adding an extension.
			return proto.StatusNotSupported, nil, "renaming normal files to disk image extensions is disabled"
		}
	}

	dstAbs, err := fsops.ToOSPath(rootAbs, dst)
	if err != nil {
		return proto.StatusBadPath, nil, err.Error()
	}
	// No-op rename.
	if srcAbs == dstAbs {
		return proto.StatusOK, nil, ""
	}

	dstSt, err := fsops.Stat(dstAbs)
	if err != nil {
		return proto.StatusInternal, nil, err.Error()
	}

	overwrite := (flags & proto.FlagMV_OVERWRITE) != 0
	if dstSt.Exists {
		if overwrite && !cfg.EnableOverwrite {
			return proto.StatusNotSupported, nil, "overwrite disabled"
		}
		if !overwrite {
			return proto.StatusAlreadyExists, nil, "dst exists"
		}
	}

	// If we overwrite existing destination, remove (or trash) it first.
	if dstSt.Exists && overwrite {
		if shouldUseTrash(cfg, rootAbs, dstAbs) {
			if _, err := s.moveToTrash(cfg, rootAbs, dstAbs); err != nil {
				return proto.StatusInternal, nil, err.Error()
			}
		} else {
			if dstSt.IsDir {
				if err := os.RemoveAll(dstAbs); err != nil {
					return proto.StatusInternal, nil, err.Error()
				}
			} else {
				if err := os.Remove(dstAbs); err != nil {
					return proto.StatusInternal, nil, err.Error()
				}
			}
			// Removing frees space, so invalidate usage cache.
			s.invalidateRootUsage(rootAbs)
		}
	}

	// Fast path: rename.
	if err := os.Rename(srcAbs, dstAbs); err == nil {
		return proto.StatusOK, nil, ""
	}

	// Slow path fallback: copy + delete.
	if srcSt.IsDir {
		// Directory move fallback requires both recursive copy and recursive remove to be enabled.
		if !cfg.EnableCpRecursive || !cfg.EnableRmdirRecursive {
			return proto.StatusNotSupported, nil, "dir mv fallback disabled"
		}
	}

	var srcTotal, srcMax uint64
	if limits.MaxFileBytes > 0 || limits.QuotaBytes > 0 {
		srcTotal, srcMax, err = pathSizeBytes(srcAbs)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		if limits.MaxFileBytes > 0 && srcMax > limits.MaxFileBytes {
			return proto.StatusTooLarge, nil, "max file size exceeded"
		}
	}

	var usedBefore uint64
	var haveUsed bool
	if limits.QuotaBytes > 0 {
		usedBefore, err = s.rootUsageBytes(rootAbs)
		if err != nil {
			return proto.StatusInternal, nil, err.Error()
		}
		haveUsed = true
		// Peak usage while copying: source still exists, plus the full copy.
		if usedBefore+srcTotal > limits.QuotaBytes {
			return proto.StatusTooLarge, nil, "quota exceeded"
		}
	}

	if srcSt.IsDir {
		if err := fsops.CopyDirRecursive(srcAbs, dstAbs); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, nil, err.Error()
		}
		if err := os.RemoveAll(srcAbs); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, nil, err.Error()
		}
	} else {
		if err := fsops.CopyFile(srcAbs, dstAbs); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, nil, err.Error()
		}
		if err := os.Remove(srcAbs); err != nil {
			s.invalidateRootUsage(rootAbs)
			return proto.StatusInternal, nil, err.Error()
		}
	}

	if haveUsed && s.usage != nil {
		// After move fallback, total usage remains the same as before the copy.
		s.setRootUsageBytes(rootAbs, usedBefore)
	}

	return proto.StatusOK, nil, ""
}

func (s *Server) opPING(cfg config.Config, payload []byte) (byte, []byte, string) {
	// Legacy optional. Request empty; response may contain a string.
	if len(payload) != 0 {
		return proto.StatusBadRequest, nil, "PING payload must be empty"
	}
	e := proto.NewEncoder(32)
	_ = e.WriteString("WICOS64-API 0.2.2")
	return proto.StatusOK, e.Bytes(), ""
}

func clampU32(v uint64) uint32 {
	if v > 0xFFFFFFFF {
		return 0xFFFFFFFF
	}
	return uint32(v)
}
