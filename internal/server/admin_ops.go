package server

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"wicos64-server/internal/config"
	"wicos64-server/internal/proto"
)

// Admin Ops Playground (used by the Admin UI only).
//
// This endpoint is intentionally *not* part of the public WiCOS64 API.
// It is protected by the Admin authentication.

type adminOpsRunRequest struct {
	TokenKind string `json:"token_kind"`
	TokenID   string `json:"token_id"`

	Line    string `json:"line"`
	Data    string `json:"data"`
	DataEnc string `json:"data_enc"` // text|hex|base64
}

type adminOpsRunResponse struct {
	OK bool `json:"ok"`

	Line string `json:"line"`

	TokenKind string `json:"token_kind"`
	TokenID   string `json:"token_id"`
	Root      string `json:"root"`

	Op       string `json:"op"`
	OpCode   int    `json:"op_code"`
	Flags    int    `json:"flags"`
	ReqBytes int    `json:"req_bytes"`

	Status     string `json:"status"`
	StatusCode int    `json:"status_code"`
	ErrMsg     string `json:"err_msg,omitempty"`

	DurationMs int64 `json:"duration_ms"`

	RespBytes int    `json:"resp_bytes"`
	RespB64   string `json:"resp_b64,omitempty"`
	RespHex   string `json:"resp_hex,omitempty"`

	Pretty string `json:"pretty,omitempty"`
}

func (s *Server) handleAdminOpsRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req adminOpsRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	req.Line = strings.TrimSpace(req.Line)
	if req.Line == "" {
		http.Error(w, "missing line", http.StatusBadRequest)
		return
	}

	// Use the currently active in-memory config snapshot.
	// (Admin UI may modify it without persisting to disk yet.)
	cfg := s.cfgSnapshot()

	token, ok := resolveTokenByRef(&cfg, req.TokenKind, req.TokenID)
	if !ok {
		http.Error(w, "unknown token context", http.StatusBadRequest)
		return
	}

	ctx, ok := cfg.ResolveTokenContext(token)
	if !ok {
		http.Error(w, "token not active", http.StatusBadRequest)
		return
	}

	limits := Limits{
		ReadOnly:                     ctx.ReadOnly,
		QuotaBytes:                   ctx.QuotaBytes,
		MaxFileBytes:                 ctx.MaxFileBytes,
		DiskImagesEnabled:            ctx.DiskImagesEnabled,
		DiskImagesWriteEnabled:       ctx.DiskImagesWriteEnabled,
		DiskImagesAutoResizeEnabled:  ctx.DiskImagesAutoResizeEnabled,
		DiskImagesAllowRenameConvert: ctx.DiskImagesAllowRenameConvert,
	}

	op, flags, payload, parseErr := parseOpsCLI(req.Line, req.Data, req.DataEnc)
	if parseErr != nil {
		http.Error(w, parseErr.Error(), http.StatusBadRequest)
		return
	}

	if len(payload) > int(cfg.MaxPayload) {
		http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}

	rootAbs := ctx.Root
	if rootAbs == "" {
		rootAbs = cfg.BasePath
	}

	t0 := time.Now()
	status, respPayload, errMsg := s.dispatch(cfg, limits, op, flags, payload, rootAbs)
	dur := time.Since(t0)

	// Record into the Live Log (without exposing any raw token).
	info := "playground"
	if ctx.Name != "" {
		info += " token=" + ctx.Name
	} else {
		// Stable but does not reveal the secret token itself.
		info += " token_id=" + tokenID(token)
	}
	le := LogEntry{
		TimeUnixMs:  time.Now().UnixMilli(),
		RemoteIP:    "ADMIN",
		Op:          op,
		OpName:      opName(op),
		Status:      status,
		StatusName:  statusName(status),
		ReqBytes:    len(payload),
		RespBytes:   len(respPayload),
		DurationMs:  dur.Milliseconds(),
		HTTPStatus:  200,
		Info:        info,
		ReqPreview:  summarizeRequest(cfg, op, flags, payload),
		RespPreview: opsPretty(op, status, respPayload, errMsg),
	}
	s.record(cfg, le)

	pretty := opsPretty(op, status, respPayload, errMsg)

	out := adminOpsRunResponse{
		OK: status == proto.StatusOK,

		Line: req.Line,

		TokenKind: req.TokenKind,
		TokenID:   req.TokenID,
		Root:      rootAbs,

		Op:       opName(op),
		OpCode:   int(op),
		Flags:    int(flags),
		ReqBytes: len(payload),

		Status:     statusName(status),
		StatusCode: int(status),
		ErrMsg:     errMsg,

		DurationMs: dur.Milliseconds(),

		RespBytes: len(respPayload),
		RespB64: func() string {
			if len(respPayload) == 0 {
				return ""
			}
			return base64.StdEncoding.EncodeToString(respPayload)
		}(),
		RespHex: func() string {
			if len(respPayload) == 0 {
				return ""
			}
			// keep it compact; UI can copy it.
			return strings.ToUpper(hex.EncodeToString(respPayload))
		}(),

		Pretty: pretty,
	}

	writeJSON(w, http.StatusOK, out)
}

func resolveTokenByRef(cfg *config.Config, kind, id string) (string, bool) {
	kind = strings.TrimSpace(kind)
	id = strings.TrimSpace(id)

	if kind == "" {
		// Backwards-compatible: treat empty as "no_auth" if enabled.
		kind = "no_auth"
	}

	// NOTE: Admin token listing uses kind values:
	//   token        (tokens[])
	//   legacy_map   (token_roots)
	//   legacy_token (token)
	//   no_auth
	// For backwards compatibility, we also accept older aliases:
	//   tokens_v2, token_roots, token_legacy.
	switch kind {
	case "no_auth":
		return "", true

	case "token", "tokens_v2":
		for _, t := range cfg.Tokens {
			if tokenID(t.Token) == id {
				return t.Token, true
			}
		}
		return "", false

	case "legacy_map", "token_roots":
		for tok := range cfg.TokenRoots {
			if tokenID(tok) == id {
				return tok, true
			}
		}
		return "", false

	case "legacy_token", "token_legacy":
		if cfg.Token != "" && tokenID(cfg.Token) == id {
			return cfg.Token, true
		}
		return "", false

	default:
		// Unknown kind.
		return "", false
	}
}

func parseOpsCLI(line, data, dataEnc string) (byte, byte, []byte, error) {
	args, err := splitArgs(line)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(args) == 0 {
		return 0, 0, nil, fmt.Errorf("empty command")
	}

	cmd := strings.ToLower(args[0])
	rest := args[1:]

	// helper to pull -f / --flags
	parseFlagsOverride := func(in []string) (byte, []string, bool, error) {
		out := make([]string, 0, len(in))
		var flagsSet bool
		var flagsVal byte
		for i := 0; i < len(in); i++ {
			a := in[i]
			if a == "-f" || a == "--flags" {
				if i+1 >= len(in) {
					return 0, nil, false, fmt.Errorf("missing value for %s", a)
				}
				v, perr := parseByte(in[i+1])
				if perr != nil {
					return 0, nil, false, fmt.Errorf("invalid flags: %v", perr)
				}
				flagsSet = true
				flagsVal = v
				i++
				continue
			}
			out = append(out, a)
		}
		return flagsVal, out, flagsSet, nil
	}

	flagsOverride, restNoF, flagsSet, err := parseFlagsOverride(rest)
	if err != nil {
		return 0, 0, nil, err
	}
	rest = restNoF

	var flags byte
	if flagsSet {
		flags = flagsOverride
	}

	// parse common short opts (only if -f wasn't used)
	takeOpts := func(valid map[string]byte, in []string) ([]string, error) {
		out := in
		if flagsSet {
			return out, nil
		}

		for len(out) > 0 {
			a := out[0]
			if !strings.HasPrefix(a, "-") {
				break
			}
			bit, ok := valid[a]
			if !ok {
				// long forms
				if strings.HasPrefix(a, "--") {
					if b2, ok2 := valid[a]; ok2 {
						flags |= b2
						out = out[1:]
						continue
					}
				}
				return nil, fmt.Errorf("unknown option: %s", a)
			}
			flags |= bit
			out = out[1:]
		}
		return out, nil
	}

	var op byte
	var payload []byte
	var e proto.Encoder

	switch cmd {
	case "caps":
		op = proto.OpCAPS

	case "ping":
		op = proto.OpPING

	case "statfs":
		op = proto.OpSTATFS
		path := "/"
		if len(rest) >= 1 {
			path = rest[0]
		}
		e.WriteString(path)
		payload = e.Bytes()

	case "ls":
		op = proto.OpLS
		if len(rest) < 1 {
			return 0, 0, nil, fmt.Errorf("usage: ls <path> [start] [max]")
		}
		path := rest[0]
		start := uint16(0)
		max := uint16(256)
		if len(rest) >= 2 {
			v, perr := parseU16(rest[1])
			if perr != nil {
				return 0, 0, nil, fmt.Errorf("invalid start: %v", perr)
			}
			start = v
		}
		if len(rest) >= 3 {
			v, perr := parseU16(rest[2])
			if perr != nil {
				return 0, 0, nil, fmt.Errorf("invalid max: %v", perr)
			}
			max = v
		}
		e.WriteString(path)
		e.WriteU16(start)
		e.WriteU16(max)
		payload = e.Bytes()

	case "stat":
		op = proto.OpSTAT
		if len(rest) != 1 {
			return 0, 0, nil, fmt.Errorf("usage: stat <path>")
		}
		e.WriteString(rest[0])
		payload = e.Bytes()

	case "read":
		op = proto.OpREAD_RANGE
		if len(rest) != 3 {
			return 0, 0, nil, fmt.Errorf("usage: read <path> <offset> <len>")
		}
		off, perr := parseU32(rest[1])
		if perr != nil {
			return 0, 0, nil, fmt.Errorf("invalid offset: %v", perr)
		}
		ln, perr := parseU16(rest[2])
		if perr != nil {
			return 0, 0, nil, fmt.Errorf("invalid len: %v", perr)
		}
		e.WriteString(rest[0])
		e.WriteU32(off)
		e.WriteU16(ln)
		payload = e.Bytes()

	case "write":
		op = proto.OpWRITE_RANGE
		// write supports opts: -t (truncate), -c (create)
		var err error
		rest, err = takeOpts(map[string]byte{
			"-t":         proto.FlagWR_TRUNCATE,
			"--truncate": proto.FlagWR_TRUNCATE,
			"-c":         proto.FlagWR_CREATE,
			"--create":   proto.FlagWR_CREATE,
		}, rest)
		if err != nil {
			return 0, 0, nil, err
		}
		if len(rest) < 2 {
			return 0, 0, nil, fmt.Errorf("usage: write [-t] [-c] <path> <offset> [data]")
		}
		path := rest[0]
		off, perr := parseU32(rest[1])
		if perr != nil {
			return 0, 0, nil, fmt.Errorf("invalid offset: %v", perr)
		}

		var bytes []byte
		if len(rest) >= 3 {
			bytes, perr = decodeData(strings.Join(rest[2:], " "), "text")
		} else {
			bytes, perr = decodeData(data, dataEnc)
		}
		if perr != nil {
			return 0, 0, nil, perr
		}
		if len(bytes) > 0xFFFF {
			return 0, 0, nil, fmt.Errorf("data too large")
		}

		e.WriteString(path)
		e.WriteU32(off)
		e.WriteU16(uint16(len(bytes)))
		e.WriteBytes(bytes)
		payload = e.Bytes()

	case "append":
		op = proto.OpAPPEND
		// append supports opts: -c (create)
		var err error
		rest, err = takeOpts(map[string]byte{
			"-c":       proto.FlagAP_CREATE,
			"--create": proto.FlagAP_CREATE,
		}, rest)
		if err != nil {
			return 0, 0, nil, err
		}
		if len(rest) < 1 {
			return 0, 0, nil, fmt.Errorf("usage: append [-c] <path> [data]")
		}
		path := rest[0]

		var bytes []byte
		if len(rest) >= 2 {
			bytes, err = decodeData(strings.Join(rest[1:], " "), "text")
		} else {
			bytes, err = decodeData(data, dataEnc)
		}
		if err != nil {
			return 0, 0, nil, err
		}
		if len(bytes) > 0xFFFF {
			return 0, 0, nil, fmt.Errorf("data too large")
		}

		e.WriteString(path)
		e.WriteU16(uint16(len(bytes)))
		e.WriteBytes(bytes)
		payload = e.Bytes()

	case "mkdir":
		op = proto.OpMKDIR
		// mkdir supports opts: -p
		var err error
		rest, err = takeOpts(map[string]byte{
			"-p":        proto.FlagMK_PARENTS,
			"--parents": proto.FlagMK_PARENTS,
		}, rest)
		if err != nil {
			return 0, 0, nil, err
		}
		if len(rest) != 1 {
			return 0, 0, nil, fmt.Errorf("usage: mkdir [-p] <path>")
		}
		e.WriteString(rest[0])
		payload = e.Bytes()

	case "rmdir":
		op = proto.OpRMDIR
		// rmdir supports opts: -r
		var err error
		rest, err = takeOpts(map[string]byte{
			"-r":          proto.FlagRD_RECURSIVE,
			"--recursive": proto.FlagRD_RECURSIVE,
		}, rest)
		if err != nil {
			return 0, 0, nil, err
		}
		if len(rest) != 1 {
			return 0, 0, nil, fmt.Errorf("usage: rmdir [-r] <path>")
		}
		e.WriteString(rest[0])
		payload = e.Bytes()

	case "rm":
		op = proto.OpRM
		if len(rest) != 1 {
			return 0, 0, nil, fmt.Errorf("usage: rm <path>")
		}
		e.WriteString(rest[0])
		payload = e.Bytes()

	case "cp":
		op = proto.OpCP
		// cp supports opts: -o, -r
		var err error
		rest, err = takeOpts(map[string]byte{
			"-o":          proto.FlagCP_OVERWRITE,
			"--overwrite": proto.FlagCP_OVERWRITE,
			"-r":          proto.FlagCP_RECURSIVE,
			"--recursive": proto.FlagCP_RECURSIVE,
		}, rest)
		if err != nil {
			return 0, 0, nil, err
		}
		if len(rest) != 2 {
			return 0, 0, nil, fmt.Errorf("usage: cp [-o] [-r] <src> <dst>")
		}
		e.WriteString(rest[0])
		e.WriteString(rest[1])
		payload = e.Bytes()

	case "mv":
		op = proto.OpMV
		// mv supports opts: -o
		var err error
		rest, err = takeOpts(map[string]byte{
			"-o":          proto.FlagMV_OVERWRITE,
			"--overwrite": proto.FlagMV_OVERWRITE,
		}, rest)
		if err != nil {
			return 0, 0, nil, err
		}
		if len(rest) != 2 {
			return 0, 0, nil, fmt.Errorf("usage: mv [-o] <src> <dst>")
		}
		e.WriteString(rest[0])
		e.WriteString(rest[1])
		payload = e.Bytes()

	case "search":
		op = proto.OpSEARCH
		if len(rest) < 2 {
			return 0, 0, nil, fmt.Errorf("usage: search <base> <query> [start] [max] [maxScan] [-f <flags>]")
		}
		base := rest[0]
		query := rest[1]
		start := uint16(0)
		max := uint16(25)
		maxScan := uint32(0)
		if len(rest) >= 3 {
			v, perr := parseU16(rest[2])
			if perr != nil {
				return 0, 0, nil, fmt.Errorf("invalid start: %v", perr)
			}
			start = v
		}
		if len(rest) >= 4 {
			v, perr := parseU16(rest[3])
			if perr != nil {
				return 0, 0, nil, fmt.Errorf("invalid max: %v", perr)
			}
			max = v
		}
		if len(rest) >= 5 {
			v, perr := parseU32(rest[4])
			if perr != nil {
				return 0, 0, nil, fmt.Errorf("invalid maxScan: %v", perr)
			}
			maxScan = v
		}
		e.WriteString(base)
		e.WriteString(query)
		e.WriteU16(start)
		e.WriteU16(max)
		e.WriteU32(maxScan)
		e.WriteU8(flags) // search flags live in the flags byte for SEARCH.
		payload = e.Bytes()

		// SEARCH uses payload flags field inside payload, not W64F header flags.
		flags = 0

	case "hash":
		op = proto.OpHASH
		if len(rest) != 1 {
			return 0, 0, nil, fmt.Errorf("usage: hash <path> [-f <flags>]")
		}
		e.WriteString(rest[0])
		payload = e.Bytes()

	default:
		return 0, 0, nil, fmt.Errorf("unknown command: %s", cmd)
	}

	return op, flags, payload, nil
}

func splitArgs(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	var args []string
	var cur strings.Builder
	inQuotes := false
	escaped := false

	flush := func() {
		if cur.Len() > 0 {
			args = append(args, cur.String())
			cur.Reset()
		}
	}

	for _, r := range s {
		if escaped {
			cur.WriteRune(r)
			escaped = false
			continue
		}
		switch r {
		case '\\':
			escaped = true
		case '"':
			inQuotes = !inQuotes
		case ' ', '\t', '\n', '\r':
			if inQuotes {
				cur.WriteRune(r)
			} else {
				flush()
			}
		default:
			cur.WriteRune(r)
		}
	}

	if escaped {
		// dangling backslash; keep it.
		cur.WriteRune('\\')
	}

	if inQuotes {
		return nil, fmt.Errorf("unterminated quote")
	}

	flush()
	return args, nil
}

func parseU16(s string) (uint16, error) {
	v, err := parseUint(s, 16)
	if err != nil {
		return 0, err
	}
	if v > 0xFFFF {
		return 0, fmt.Errorf("out of range")
	}
	return uint16(v), nil
}

func parseU32(s string) (uint32, error) {
	v, err := parseUint(s, 32)
	if err != nil {
		return 0, err
	}
	if v > 0xFFFFFFFF {
		return 0, fmt.Errorf("out of range")
	}
	return uint32(v), nil
}

func parseByte(s string) (byte, error) {
	v, err := parseUint(s, 8)
	if err != nil {
		return 0, err
	}
	if v > 0xFF {
		return 0, fmt.Errorf("out of range")
	}
	return byte(v), nil
}

func parseUint(s string, bits int) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}

	base := 10
	if strings.HasPrefix(strings.ToLower(s), "0x") {
		base = 16
		s = s[2:]
	}

	return strconv.ParseUint(s, base, bits)
}

func decodeData(s, enc string) ([]byte, error) {
	enc = strings.ToLower(strings.TrimSpace(enc))
	if enc == "" {
		enc = "text"
	}

	switch enc {
	case "text", "utf8", "utf-8":
		return []byte(s), nil

	case "hex":
		cleaned := strings.ReplaceAll(strings.TrimSpace(s), " ", "")
		cleaned = strings.ReplaceAll(cleaned, "\n", "")
		cleaned = strings.ReplaceAll(cleaned, "\r", "")
		cleaned = strings.ReplaceAll(cleaned, "\t", "")
		if cleaned == "" {
			return nil, nil
		}
		if len(cleaned)%2 != 0 {
			return nil, fmt.Errorf("hex length must be even")
		}
		b, err := hex.DecodeString(cleaned)
		if err != nil {
			return nil, fmt.Errorf("invalid hex: %v", err)
		}
		return b, nil

	case "base64", "b64":
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, nil
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("invalid base64: %v", err)
		}
		return b, nil

	default:
		return nil, fmt.Errorf("unknown data_enc: %s", enc)
	}
}

type prettyDecoder struct {
	d   *proto.Decoder
	Err error
}

func newPrettyDecoder(b []byte) *prettyDecoder {
	return &prettyDecoder{d: proto.NewDecoder(b)}
}

func (p *prettyDecoder) ReadU8() byte {
	if p.Err != nil {
		return 0
	}
	v, err := p.d.ReadU8()
	if err != nil {
		p.Err = err
		return 0
	}
	return v
}

func (p *prettyDecoder) ReadU16() uint16 {
	if p.Err != nil {
		return 0
	}
	v, err := p.d.ReadU16()
	if err != nil {
		p.Err = err
		return 0
	}
	return v
}

func (p *prettyDecoder) ReadU32() uint32 {
	if p.Err != nil {
		return 0
	}
	v, err := p.d.ReadU32()
	if err != nil {
		p.Err = err
		return 0
	}
	return v
}

func (p *prettyDecoder) ReadString() string {
	if p.Err != nil {
		return ""
	}
	// Use a generous limit here. The overall response is limited by max_payload anyway.
	v, err := p.d.ReadString(4096)
	if err != nil {
		p.Err = err
		return ""
	}
	return v
}

func (p *prettyDecoder) ReadBytes(n int) []byte {
	if p.Err != nil {
		return nil
	}
	v, err := p.d.ReadBytes(n)
	if err != nil {
		p.Err = err
		return nil
	}
	return v
}

func opsPretty(op byte, status byte, resp []byte, errMsg string) string {
	if status != proto.StatusOK {
		if errMsg != "" {
			return errMsg
		}
		return fmt.Sprintf("%s (%d)", statusName(status), status)
	}

	d := newPrettyDecoder(resp)

	switch op {
	case proto.OpCAPS:
		maxChunk := d.ReadU16()
		maxPayload := d.ReadU16()
		maxPath := d.ReadU16()
		maxName := d.ReadU16()
		maxEntries := d.ReadU16()
		feats := d.ReadU32()
		srvTime := d.ReadU32()
		srvName := d.ReadString()
		if d.Err != nil {
			return fmt.Sprintf("decode error: %v", d.Err)
		}

		var featNames []string
		add := func(bit uint32, name string) {
			if feats&bit != 0 {
				featNames = append(featNames, name)
			}
		}
		add(proto.FeatSTATFS, "STATFS")
		add(proto.FeatAPPEND, "APPEND")
		add(proto.FeatSEARCH, "SEARCH")
		add(proto.FeatHASH_CRC32, "HASH_CRC32")
		add(proto.FeatHASH_SHA1, "HASH_SHA1")
		add(proto.FeatMKDIR_PARENTS, "MKDIR_PARENTS")
		add(proto.FeatRMDIR_RECURSIVE, "RMDIR_RECURSIVE")
		add(proto.FeatCP_RECURSIVE, "CP_RECURSIVE")
		add(proto.FeatOVERWRITE, "OVERWRITE")
		add(proto.FeatERRMSG, "ERRMSG")

		t := time.Unix(int64(srvTime), 0).UTC()

		return fmt.Sprintf(
			"max_chunk=%d\nmax_payload=%d\nmax_path=%d\nmax_name=%d\nmax_entries=%d\nfeatures=0x%08X\nfeatures_list=%s\nserver_time=%s\nserver_name=%s",
			maxChunk, maxPayload, maxPath, maxName, maxEntries,
			feats,
			strings.Join(featNames, ","),
			t.Format(time.RFC3339),
			srvName,
		)

	case proto.OpSTATFS:
		total := d.ReadU32()
		free := d.ReadU32()
		used := d.ReadU32()
		if d.Err != nil {
			return fmt.Sprintf("decode error: %v", d.Err)
		}
		return fmt.Sprintf("total=%d\nfree=%d\nused=%d", total, free, used)

	case proto.OpLS:
		cnt := d.ReadU16()
		lines := make([]string, 0, int(cnt)+1)
		lines = append(lines, fmt.Sprintf("count=%d", cnt))
		for i := 0; i < int(cnt); i++ {
			typ := d.ReadU8()
			size := d.ReadU32()
			mtime := d.ReadU32()
			name := d.ReadString()
			if d.Err != nil {
				return fmt.Sprintf("decode error: %v", d.Err)
			}
			t := time.Unix(int64(mtime), 0).UTC()
			kind := "F"
			if typ == 1 {
				kind = "D"
			}
			lines = append(lines, fmt.Sprintf("%s %10d %s %s", kind, size, t.Format("2006-01-02 15:04:05"), name))
		}
		next := d.ReadU16()
		if d.Err == nil {
			lines = append(lines, fmt.Sprintf("next=%d", next))
		}
		return strings.Join(lines, "\n")

	case proto.OpSTAT:
		typ := d.ReadU8()
		size := d.ReadU32()
		mtime := d.ReadU32()
		if d.Err != nil {
			return fmt.Sprintf("decode error: %v", d.Err)
		}
		kind := "file"
		if typ == 1 {
			kind = "dir"
		}
		t := time.Unix(int64(mtime), 0).UTC()
		return fmt.Sprintf("type=%s\nsize=%d\nmtime=%s", kind, size, t.Format(time.RFC3339))

	case proto.OpHASH:
		crc := d.ReadU32()
		if d.Err != nil {
			return fmt.Sprintf("decode error: %v", d.Err)
		}
		return fmt.Sprintf("crc32=0x%08X", crc)

	case proto.OpSEARCH:
		cnt := d.ReadU16()
		lines := make([]string, 0, int(cnt)+1)
		lines = append(lines, fmt.Sprintf("count=%d", cnt))
		for i := 0; i < int(cnt); i++ {
			p := d.ReadString()
			off := d.ReadU32()
			plen := d.ReadU16()
			prev := d.ReadBytes(int(plen))
			if d.Err != nil {
				return fmt.Sprintf("decode error: %v", d.Err)
			}
			// show preview as printable-ish
			preview := string(prev)
			preview = strings.ReplaceAll(preview, "\n", "\\n")
			preview = strings.ReplaceAll(preview, "\r", "\\r")
			lines = append(lines, fmt.Sprintf("%s @%d : %s", p, off, preview))
		}
		return strings.Join(lines, "\n")

	case proto.OpREAD_RANGE:
		if len(resp) == 0 {
			return "(empty)"
		}
		// show a small printable preview + hex length
		prev := resp
		if len(prev) > 256 {
			prev = prev[:256]
		}
		s := string(prev)
		s = strings.ReplaceAll(s, "\r", "\\r")
		s = strings.ReplaceAll(s, "\n", "\\n")
		return fmt.Sprintf("bytes=%d\npreview=%s", len(resp), s)

	default:
		// For most ops the response is empty.
		if len(resp) == 0 {
			return "OK"
		}
		return fmt.Sprintf("bytes=%d", len(resp))
	}
}
