package server

import (
	"fmt"
	"hash/crc32"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"wicos64-server/internal/version"
)

type adminTokenStatus struct {
	Name        string `json:"name,omitempty"`
	Kind        string `json:"kind"` // token|legacy_token|legacy_map|no_auth
	TokenMask   string `json:"token_mask"`
	TokenID     string `json:"token_id,omitempty"` // crc32 hex
	Enabled     bool   `json:"enabled"`
	Ignored     bool   `json:"ignored,omitempty"` // present in config but not effective due to precedence
	RootAbs     string `json:"root_abs"`
	ReadOnly    bool   `json:"read_only"`
	QuotaBytes  uint64 `json:"quota_bytes"`
	MaxFileByte uint64 `json:"max_file_bytes"`

	UsedBytes      uint64 `json:"used_bytes,omitempty"`
	TmpBytes       uint64 `json:"tmp_bytes,omitempty"`
	RemainingBytes uint64 `json:"remaining_bytes,omitempty"`
	UsedPct        int    `json:"used_pct,omitempty"`
	Error          string `json:"error,omitempty"`
}

type adminTokensResponse struct {
	OK       bool               `json:"ok"`
	Build    string             `json:"build"`
	TSUnix   int64              `json:"ts_unix"`
	Warnings []string           `json:"warnings,omitempty"`
	Tokens   []adminTokenStatus `json:"tokens"`
}

func (s *Server) handleAdminTokens(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	cfg := s.cfgSnapshot()
	resp := adminTokensResponse{OK: true, Build: version.Get().String(), TSUnix: time.Now().Unix(), Warnings: configWarnings(cfg)}

	// Collect token-like entries from all supported config modes.
	// NOTE: ResolveTokenContext precedence is:
	//   1) tokens[] (if non-empty)
	//   2) token_roots map
	//   3) token (single)
	//   4) enable_noauth
	// We still show "ignored" entries for transparency.
	tokensListActive := len(cfg.Tokens) > 0

	var out []adminTokenStatus

	// tokens[]
	for _, t := range cfg.Tokens {
		if strings.TrimSpace(t.Token) == "" {
			continue
		}
		enabled := true
		if t.Enabled != nil {
			enabled = *t.Enabled
		}
		st := adminTokenStatus{Kind: "token", Name: t.Name, TokenMask: maskToken(t.Token), TokenID: tokenID(t.Token), Enabled: enabled}
		ctx, ok := cfg.ResolveTokenContext(t.Token)
		if ok {
			rootAbs, err := filepath.Abs(ctx.Root)
			if err == nil {
				st.RootAbs = rootAbs
			} else {
				st.RootAbs = ctx.Root
				st.Error = err.Error()
			}
			st.ReadOnly = ctx.ReadOnly
			st.QuotaBytes = ctx.QuotaBytes
			st.MaxFileByte = ctx.MaxFileBytes
		} else {
			// Disabled token or mismatch; still show configured root.
			rootAbs, err := filepath.Abs(t.Root)
			if err == nil {
				st.RootAbs = rootAbs
			} else {
				st.RootAbs = t.Root
			}
			st.ReadOnly = t.ReadOnly || cfg.GlobalReadOnly
			st.QuotaBytes = minNonZeroU64(t.QuotaBytes, cfg.GlobalQuotaBytes)
			st.MaxFileByte = minNonZeroU64(t.MaxFileBytes, cfg.GlobalMaxFileBytes)
			st.Ignored = !enabled
		}
		out = append(out, st)
	}

	// legacy token_roots
	if len(cfg.TokenRoots) > 0 {
		keys := make([]string, 0, len(cfg.TokenRoots))
		for k := range cfg.TokenRoots {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, tok := range keys {
			root := cfg.TokenRoots[tok]
			st := adminTokenStatus{Kind: "legacy_map", TokenMask: maskToken(tok), TokenID: tokenID(tok), Enabled: true, Ignored: tokensListActive}
			ctx, ok := cfg.ResolveTokenContext(tok)
			if ok {
				rootAbs, err := filepath.Abs(ctx.Root)
				if err == nil {
					st.RootAbs = rootAbs
				} else {
					st.RootAbs = ctx.Root
					st.Error = err.Error()
				}
				st.ReadOnly = ctx.ReadOnly
				st.QuotaBytes = ctx.QuotaBytes
				st.MaxFileByte = ctx.MaxFileBytes
			} else {
				rootAbs, err := filepath.Abs(root)
				if err == nil {
					st.RootAbs = rootAbs
				} else {
					st.RootAbs = root
				}
				st.ReadOnly = cfg.GlobalReadOnly
				st.QuotaBytes = cfg.GlobalQuotaBytes
				st.MaxFileByte = cfg.GlobalMaxFileBytes
			}
			out = append(out, st)
		}
	}

	// legacy single token
	if cfg.Token != "" {
		st := adminTokenStatus{Kind: "legacy_token", TokenMask: maskToken(cfg.Token), TokenID: tokenID(cfg.Token), Enabled: true, Ignored: tokensListActive || len(cfg.TokenRoots) > 0}
		ctx, ok := cfg.ResolveTokenContext(cfg.Token)
		if ok {
			rootAbs, err := filepath.Abs(ctx.Root)
			if err == nil {
				st.RootAbs = rootAbs
			} else {
				st.RootAbs = ctx.Root
				st.Error = err.Error()
			}
			st.ReadOnly = ctx.ReadOnly
			st.QuotaBytes = ctx.QuotaBytes
			st.MaxFileByte = ctx.MaxFileBytes
		} else {
			// If ignored, still show base.
			rootAbs, _ := filepath.Abs(cfg.BasePath)
			st.RootAbs = rootAbs
			st.ReadOnly = cfg.GlobalReadOnly
			st.QuotaBytes = cfg.GlobalQuotaBytes
			st.MaxFileByte = cfg.GlobalMaxFileBytes
		}
		out = append(out, st)
	}

	// no-auth (active when no token configuration is present)
	noAuthActive := cfg.Token == "" && len(cfg.TokenRoots) == 0 && len(cfg.Tokens) == 0
	if noAuthActive {
		st := adminTokenStatus{Kind: "no_auth", TokenMask: "<no-auth>", Enabled: true}
		rootAbs, err := filepath.Abs(cfg.BasePath)
		if err == nil {
			st.RootAbs = rootAbs
		} else {
			st.RootAbs = cfg.BasePath
			st.Error = err.Error()
		}
		st.ReadOnly = cfg.GlobalReadOnly
		st.QuotaBytes = cfg.GlobalQuotaBytes
		st.MaxFileByte = cfg.GlobalMaxFileBytes
		out = append(out, st)
	}

	// Compute usage per root (dedup) for visible entries that have a root.
	usedByRoot := make(map[string]uint64)
	tmpByRoot := make(map[string]uint64)
	errByRoot := make(map[string]string)

	for _, st := range out {
		if st.RootAbs == "" {
			continue
		}
		if _, ok := usedByRoot[st.RootAbs]; ok {
			continue
		}
		used, err := s.rootUsageBytes(st.RootAbs)
		if err != nil {
			errByRoot[st.RootAbs] = err.Error()
			usedByRoot[st.RootAbs] = 0
		} else {
			usedByRoot[st.RootAbs] = used
		}
		// .TMP share (best-effort).
		tmpAbs := filepath.Join(st.RootAbs, ".TMP")
		if fi, terr := os.Lstat(tmpAbs); terr == nil && fi != nil {
			if tbytes, _, terr2 := pathSizeBytes(tmpAbs); terr2 == nil {
				tmpByRoot[st.RootAbs] = tbytes
			}
		}
	}

	for i := range out {
		root := out[i].RootAbs
		if root == "" {
			continue
		}
		out[i].UsedBytes = usedByRoot[root]
		out[i].TmpBytes = tmpByRoot[root]
		if out[i].QuotaBytes > 0 {
			if out[i].UsedBytes >= out[i].QuotaBytes {
				out[i].RemainingBytes = 0
				out[i].UsedPct = 100
			} else {
				out[i].RemainingBytes = out[i].QuotaBytes - out[i].UsedBytes
				out[i].UsedPct = int((out[i].UsedBytes * 100) / out[i].QuotaBytes)
			}
		}
		if out[i].Error == "" {
			out[i].Error = errByRoot[root]
		}
	}

	// Sort: by kind, name, token mask.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].TokenMask < out[j].TokenMask
	})

	resp.Tokens = out
	writeJSON(w, http.StatusOK, resp)
}

func maskToken(token string) string {
	t := strings.TrimSpace(token)
	if t == "" {
		return ""
	}
	// Keep it stable but avoid leaking full secrets.
	if len(t) <= 4 {
		return strings.Repeat("*", len(t))
	}
	if len(t) <= 8 {
		return t[:1] + strings.Repeat("*", len(t)-2) + t[len(t)-1:]
	}
	return t[:4] + "..." + t[len(t)-4:]
}

func tokenID(token string) string {
	if token == "" {
		return ""
	}
	sum := crc32.ChecksumIEEE([]byte(token))
	return fmtU32Hex(sum)
}

func fmtU32Hex(v uint32) string {
	return strings.ToUpper(fmt.Sprintf("%08x", v))
}

func minNonZeroU64(a, b uint64) uint64 {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	if a < b {
		return a
	}
	return b
}
