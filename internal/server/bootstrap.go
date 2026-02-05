package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// handleBootstrap serves a small plaintext config "snippet" that allows a WiCOS64
// client to auto-configure the API URL + token in a LAN/WLAN setting.
//
// Endpoints (fixed):
//   - GET  /wicos64/bootstrap?cfg=<bootstrap_token>&mac=<mac>
//   - POST /wicos64/bootstrap   (body: cfg=<token>&mac=<mac> or newline-separated)
//
// The response format is a tiny line-based format that's easy to parse on a 6502:
//
//	WICOS64CFG
//	API_URL=http://<host>:<port>/wicos64/api
//	TOKEN=...
//	RO=0|1
//	END
//
// Notes:
//   - This endpoint is independent of the W64F binary API and does not change the
//     wire protocol in any way.
//   - It is intended for LAN only. If cfg.Bootstrap.LanOnly is true (default),
//     non-private clients get 403.
func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgSnapshot()
	bc := cfg.Bootstrap
	if !bc.Enabled {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	// Method gating (toggleable).
	switch r.Method {
	case http.MethodGet:
		if !bc.AllowGET {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	case http.MethodPost:
		if !bc.AllowPOST {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	remoteIPStr := clientIP(r)
	if bc.LanOnly {
		ip := net.ParseIP(remoteIPStr)
		if !isLANIP(ip) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("forbidden\n"))
			return
		}
	}

	cfgTok, macRaw, parseErr := bootstrapParams(r)
	if parseErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad request\n"))
		return
	}
	if cfgTok != bc.Token {
		// Do not leak whether bootstrap exists.
		w.WriteHeader(http.StatusNotFound)
		return
	}

	mac, ok := normalizeMAC(macRaw)
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad mac\n"))
		return
	}

	// Resolve the token to return.
	token := ""
	if bc.MacTokens != nil {
		token = bc.MacTokens[mac]
	}
	if token == "" {
		// Unknown MAC.
		switch strings.ToLower(strings.TrimSpace(bc.UnknownMACPolicy)) {
		case "legacy":
			// Fall back to the legacy single-token field, if set.
			token = cfg.Token
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if token == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}

	// Make sure the token is actually accepted by the server config.
	ctx, ok := cfg.ResolveTokenContext(token)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("bootstrap token is not accepted by server config\n"))
		return
	}

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = "127.0.0.1"
	}
	apiURL := fmt.Sprintf("%s://%s%s", scheme, host, cfg.Endpoint)

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)

	// Keep it tiny & deterministic.
	_, _ = w.Write([]byte("WICOS64CFG\n"))
	_, _ = w.Write([]byte("API_URL=" + apiURL + "\n"))
	_, _ = w.Write([]byte("TOKEN=" + token + "\n"))
	if ctx.ReadOnly {
		_, _ = w.Write([]byte("RO=1\n"))
	} else {
		_, _ = w.Write([]byte("RO=0\n"))
	}
	_, _ = w.Write([]byte("MAC=" + mac + "\n"))
	if cfg.ServerName != "" {
		_, _ = w.Write([]byte("SERVER_NAME=" + cfg.ServerName + "\n"))
	}
	if ctx.QuotaBytes > 0 {
		_, _ = w.Write([]byte(fmt.Sprintf("QUOTA_BYTES=%d\n", ctx.QuotaBytes)))
	}
	if ctx.MaxFileBytes > 0 {
		_, _ = w.Write([]byte(fmt.Sprintf("MAX_FILE_BYTES=%d\n", ctx.MaxFileBytes)))
	}

	// Optional per-MAC extra key/value pairs.
	if bc.MacExtra != nil {
		if kv := bc.MacExtra[mac]; len(kv) > 0 {
			keys := make([]string, 0, len(kv))
			for k := range kv {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := kv[k]
				// Keys/values are validated/normalized on config load.
				_, _ = w.Write([]byte(k + "=" + v + "\n"))
			}
		}
	}
	_, _ = w.Write([]byte("END\n"))
}

// bootstrapParams returns (cfg, mac) from either the query string (GET/POST)
// or the POST body.
//
// For POST, we support a tiny ASCII format so 6502 code can generate it easily:
//
//	cfg=CFG-1234&mac=AABBCCDDEEFF
//
// or newline-separated:
//
//	cfg=CFG-1234\nmac=AABBCCDDEEFF
func bootstrapParams(r *http.Request) (cfgTok string, macRaw string, err error) {
	q := r.URL.Query()
	cfgTok = q.Get("cfg")
	macRaw = q.Get("mac")
	if r.Method != http.MethodPost {
		return cfgTok, macRaw, nil
	}
	// If both already present in query params, do not touch the body.
	if cfgTok != "" && macRaw != "" {
		return cfgTok, macRaw, nil
	}

	body, readErr := io.ReadAll(io.LimitReader(r.Body, 1024))
	_ = r.Body.Close()
	if readErr != nil {
		return "", "", readErr
	}
	s := strings.TrimSpace(string(body))
	if s == "" {
		return cfgTok, macRaw, nil
	}

	// Accept either a classic querystring, or newline separated key/value pairs.
	s = strings.ReplaceAll(s, "\r\n", "&")
	s = strings.ReplaceAll(s, "\n", "&")
	s = strings.ReplaceAll(s, "\r", "&")

	vals, parseErr := url.ParseQuery(s)
	if parseErr != nil {
		// Fallback: best-effort manual split.
		m := map[string]string{}
		parts := strings.Split(s, "&")
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			kv := strings.SplitN(p, "=", 2)
			if len(kv) != 2 {
				continue
			}
			k := strings.ToLower(strings.TrimSpace(kv[0]))
			v := strings.TrimSpace(kv[1])
			m[k] = v
		}
		if cfgTok == "" {
			cfgTok = m["cfg"]
		}
		if macRaw == "" {
			macRaw = m["mac"]
		}
		return cfgTok, macRaw, nil
	}
	if cfgTok == "" {
		cfgTok = vals.Get("cfg")
	}
	if macRaw == "" {
		macRaw = vals.Get("mac")
	}
	return cfgTok, macRaw, nil
}

// normalizeMAC extracts a 12-hex-digit MAC address from a string.
//
// The WiC64 "%mac" placeholder usually expands to 12 hex chars (no separators).
// However, some firmwares/emulators may append additional hex-like identifiers.
// To be robust, we accept any input that contains *at least* 12 hex digits and
// return the first 12 (uppercase), ignoring common separators.
func normalizeMAC(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	buf := make([]byte, 0, 12)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			buf = append(buf, c)
		case c >= 'a' && c <= 'f':
			buf = append(buf, c-32)
		case c >= 'A' && c <= 'F':
			buf = append(buf, c)
		case c == ':' || c == '-' || c == '.' || c == ' ' || c == '\t':
			continue
		default:
			return "", false
		}
		if len(buf) == 12 {
			// Return early to tolerate trailing junk (e.g. extra hex identifiers).
			return string(buf), true
		}
	}
	// Not enough hex digits.
	return "", false
}

func isLANIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	if ip.IsPrivate() {
		return true
	}
	if ip.IsLinkLocalUnicast() {
		return true
	}
	return false
}
