package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// TokenEntry defines an optional, policy-based token mapping.
//
// This is fully backwards-compatible with the legacy fields (token / token_roots).
// If Tokens is non-empty, it is checked first.
//
// NOTE: Enabled defaults to true when omitted.
type TokenEntry struct {
	Token             string `json:"token"`
	Name              string `json:"name,omitempty"`
	Root              string `json:"root,omitempty"`
	Enabled           *bool  `json:"enabled,omitempty"`
	ReadOnly          bool   `json:"read_only,omitempty"`
	QuotaBytes        uint64 `json:"quota_bytes,omitempty"`
	MaxFileBytes      uint64 `json:"max_file_bytes,omitempty"`
	DiskImagesEnabled *bool  `json:"disk_images_enabled,omitempty"`
	// DiskImagesWriteEnabled overrides the global disk_images_write_enabled for this token.
	// If omitted, the global setting is used.
	DiskImagesWriteEnabled *bool `json:"disk_images_write_enabled,omitempty"`
	// DiskImagesAutoResizeEnabled overrides the global disk_images_auto_resize_enabled for
	// this token. If omitted, the global setting is used.
	DiskImagesAutoResizeEnabled *bool `json:"disk_images_auto_resize_enabled,omitempty"`
	// DiskImagesAllowRenameConvert allows renaming normal files/directories to (or from)
	// disk image extensions (.d64/.d71/.d81).
	//
	// Default is false (omitted). When false, the server prevents edge cases where
	// a disk image would silently become a normal file by dropping its extension,
	// or where a normal file would become a mounted disk image by adding one.
	DiskImagesAllowRenameConvert *bool `json:"disk_images_allow_rename_convert,omitempty"`
}

// TokenContext is the resolved on-disk root and effective policy for a request.
type TokenContext struct {
	Root                         string
	Name                         string
	ReadOnly                     bool
	QuotaBytes                   uint64
	MaxFileBytes                 uint64
	DiskImagesEnabled            bool
	DiskImagesWriteEnabled       bool
	DiskImagesAutoResizeEnabled  bool
	DiskImagesAllowRenameConvert bool
	Legacy                       bool
}

// BootstrapConfig controls an optional LAN-only bootstrap endpoint that can
// return API_URL + TOKEN for a given WiC64 MAC address.
//
// This is intentionally kept separate from the binary W64F API to avoid any
// breaking changes. It is meant as a convenience helper for WiCOS64 to find
// the server on a local network.
type BootstrapConfig struct {
	// Enabled controls whether the bootstrap endpoint is active.
	Enabled bool `json:"enabled"`
	// AllowGET controls whether bootstrap requests via HTTP GET are accepted.
	// Default: true (backwards-compatible with existing clients).
	AllowGET bool `json:"allow_get"`
	// AllowPOST controls whether bootstrap requests via HTTP POST are accepted.
	// Default: true.
	AllowPOST bool `json:"allow_post"`
	// Token is the shared "config token" required to call the bootstrap endpoint.
	Token string `json:"token"`
	// LanOnly restricts the bootstrap endpoint to private/link-local/loopback IPs.
	LanOnly bool `json:"lan_only"`
	// UnknownMACPolicy decides what to do if the MAC is not found.
	// Supported values: "deny" (default), "legacy".
	UnknownMACPolicy string `json:"unknown_mac_policy"`
	// MacTokens maps normalized MAC (AABBCCDDEEFF) to a token string.
	MacTokens map[string]string `json:"mac_tokens"`
	// MacExtra optionally appends additional key/value lines to the bootstrap
	// response for a given MAC.
	//
	// The bootstrap response is a small, line-based KEY=VALUE format. These
	// additional lines are appended before the final END line so older clients
	// can safely ignore them.
	//
	// Keys are case-insensitive and will be normalized to UPPER_SNAKE_CASE on
	// load. Values must not contain newlines.
	MacExtra map[string]map[string]string `json:"mac_extra,omitempty"`
}

// DiscoveryConfig controls the optional UDP LAN discovery responder.
//
// When enabled, the server listens on UDP (default: 0.0.0.0:6464) for WDP1
// discovery packets and replies with enough information for a client to find
// the server and then call the HTTP bootstrap endpoint.
//
// This feature is meant for LAN/WLAN use only. If LanOnly is true (default),
// the server will ignore requests from non-private IP addresses.
type DiscoveryConfig struct {
	Enabled bool `json:"enabled"`
	// UDPPort is the UDP port to listen on (default: 6464).
	UDPPort int `json:"udp_port"`
	// LanOnly ignores discovery packets coming from non-LAN IPs.
	LanOnly bool `json:"lan_only"`
	// RateLimitPerSec is a simple per-source-IP limit to reduce spam (default: 5).
	RateLimitPerSec int `json:"rate_limit_per_sec"`
}

// CompatConfig contains optional compatibility toggles.
//
// These toggles MUST NOT change the W64F binary protocol. They only adjust
// server-side behavior in a backwards-compatible way.
type CompatConfig struct {
	// FallbackPRGExtension enables a small convenience feature for C64-style
	// file naming: if a file is requested without an extension and it does not
	// exist, the server will try the same path with the ".PRG" extension.
	//
	// Example:
	//   requested: /USR/LMAN  -> fallback: /USR/LMAN.PRG
	//
	// This applies only to *read-like* operations (STAT/READ_RANGE/HASH) and
	// never to write or delete operations.
	FallbackPRGExtension bool `json:"fallback_prg_extension"`

	// WildcardLoad enables Commodore-style wildcard matching for LOAD/READ.
	//
	// If enabled, READ-like operations accept '*' and '?' in the final path segment
	// and resolve it to the first matching file in that directory.
	//
	// This applies only to *read-like* operations (STAT/READ_RANGE/HASH) and
	// never to write or delete operations.
	WildcardLoad bool `json:"wildcard_load"`
}

// Config controls the backend behavior.
// It is intentionally simple: a single endpoint that maps tokens to per-user roots.
type Config struct {
	// Listen address, e.g. ":8080" or "127.0.0.1:8080".
	Listen string `json:"listen"`
	// Endpoint path, e.g. "/wicos64/api".
	Endpoint string `json:"endpoint"`

	// BasePath is the directory that contains per-token roots (unless an entry in TokenRoots / Tokens is absolute).
	BasePath string `json:"base_path"`

	// --- Legacy token configuration (still supported) ---

	// Token is a convenience field for single-user setups. If set, this token maps to BasePath.
	Token string `json:"token"`

	// TokenRoots optionally maps token -> root path.
	// If a value is relative, it is interpreted relative to BasePath.
	TokenRoots map[string]string `json:"token_roots"`

	// --- New token configuration (preferred) ---

	// Tokens optionally defines multiple tokens, each with its own root and policy.
	// If non-empty, this list is checked first.
	Tokens []TokenEntry `json:"tokens"`

	// Global (optional) policy applied in addition to token-specific policy.
	GlobalReadOnly     bool   `json:"global_read_only"`
	GlobalQuotaBytes   uint64 `json:"global_quota_bytes"`
	GlobalMaxFileBytes uint64 `json:"global_max_file_bytes"`

	// --- Limits advertised via CAPS and enforced by the server ---
	MaxPayload uint16 `json:"max_payload"`
	MaxChunk   uint16 `json:"max_chunk"`
	MaxPath    uint16 `json:"max_path"`
	MaxName    uint16 `json:"max_name"`
	MaxEntries uint16 `json:"max_entries"`

	// Feature toggles.
	EnableMkdirParents   bool `json:"enable_mkdir_parents"`
	EnableRmdirRecursive bool `json:"enable_rmdir_recursive"`
	EnableCpRecursive    bool `json:"enable_cp_recursive"`
	EnableOverwrite      bool `json:"enable_overwrite"`
	EnableErrMsg         bool `json:"enable_errmsg"`

	// If true, create recommended base directories (/bin,/usr,/etc,/.tmp) inside each token root.
	CreateRecommendedDirs bool `json:"create_recommended_dirs"`

	// Optional build/name string exposed via CAPS.server_name.
	ServerName string `json:"server_name"`

	// --- Optional Admin UI (local configuration / live log) ---
	//
	// The admin UI is a small web dashboard served by the same process.
	// IMPORTANT: By default, it is only accessible from localhost.
	EnableAdminUI    bool   `json:"enable_admin_ui"`
	AdminAllowRemote bool   `json:"admin_allow_remote"`
	AdminUser        string `json:"admin_user"`
	AdminPassword    string `json:"admin_password"`

	// LogRequests controls whether the server collects a per-request log.
	LogRequests bool `json:"log_requests"`

	// --- Optional LAN-only bootstrap (API URL + per-MAC token) ---
	Bootstrap BootstrapConfig `json:"bootstrap"`

	// --- Optional LAN discovery (UDP, WDP1) ---
	Discovery DiscoveryConfig `json:"discovery"`

	// --- Compatibility toggles (do not change the binary protocol) ---
	Compat CompatConfig `json:"compat"`

	// --- Optional disk image mounting ---
	// If enabled, supported disk image files (currently: .d64/.d71/.d81) are
	// exposed as virtual directories. Example:
	//   /games/collection.d64/LOADER
	DiskImagesEnabled bool `json:"disk_images_enabled"`
	// If enabled, disk images are writable (SAVE/WRITE operations) via the
	// virtual directory view. This is potentially destructive and therefore
	// disabled by default.
	DiskImagesWriteEnabled bool `json:"disk_images_write_enabled"`
	// If enabled, the server may automatically resize disk image subdirs/
	// partitions (primarily relevant for .d81) when they run out of space.
	// This can be I/O-heavy and may rewrite the image.
	DiskImagesAutoResizeEnabled bool `json:"disk_images_auto_resize_enabled"`

	// --- Optional housekeeping ---
	TmpCleanupEnabled         bool `json:"tmp_cleanup_enabled"`
	TmpCleanupIntervalSec     int  `json:"tmp_cleanup_interval_sec"`
	TmpCleanupMaxAgeSec       int  `json:"tmp_cleanup_max_age_sec"`
	TmpCleanupDeleteEmptyDirs bool `json:"tmp_cleanup_delete_empty_dirs"`

	// --- Optional "trash" (recycle bin) ---
	// If enabled, RM/RMDIR and overwrite behavior (CP/MV with overwrite) moves
	// the existing destination into TrashDir instead of deleting it permanently.
	TrashEnabled bool   `json:"trash_enabled"`
	TrashDir     string `json:"trash_dir"`

	// Optional trash cleanup (delete old entries under TrashDir).
	TrashCleanupEnabled         bool `json:"trash_cleanup_enabled"`
	TrashCleanupIntervalSec     int  `json:"trash_cleanup_interval_sec"`
	TrashCleanupMaxAgeSec       int  `json:"trash_cleanup_max_age_sec"`
	TrashCleanupDeleteEmptyDirs bool `json:"trash_cleanup_delete_empty_dirs"`
}

func Default() Config {
	return Config{
		Listen:                ":8080",
		Endpoint:              "/wicos64/api",
		BasePath:              "./wicos64-data",
		Token:                 "",
		TokenRoots:            map[string]string{},
		Tokens:                nil,
		GlobalReadOnly:        false,
		GlobalQuotaBytes:      0,
		GlobalMaxFileBytes:    0,
		MaxPayload:            16384,
		MaxChunk:              4096,
		MaxPath:               255,
		MaxName:               64,
		MaxEntries:            50,
		EnableMkdirParents:    true,
		EnableRmdirRecursive:  true,
		EnableCpRecursive:     true,
		EnableOverwrite:       true,
		EnableErrMsg:          true,
		CreateRecommendedDirs: true,
		ServerName:            "wicos64-go-backend",
		EnableAdminUI:         true,
		AdminAllowRemote:      false,
		AdminUser:             "admin",
		AdminPassword:         "",
		LogRequests:           true,
		Bootstrap: BootstrapConfig{
			Enabled:          false,
			AllowGET:         true,
			AllowPOST:        true,
			Token:            "",
			LanOnly:          true,
			UnknownMACPolicy: "deny",
			MacTokens:        map[string]string{},
		},
		Discovery: DiscoveryConfig{
			Enabled:         true,
			UDPPort:         6464,
			LanOnly:         true,
			RateLimitPerSec: 5,
		},
		Compat: CompatConfig{
			FallbackPRGExtension: true,
			WildcardLoad:         true,
		},
		DiskImagesEnabled:         true,
		TmpCleanupEnabled:         true,
		TmpCleanupIntervalSec:     15 * 60,      // 15 minutes
		TmpCleanupMaxAgeSec:       24 * 60 * 60, // 24 hours
		TmpCleanupDeleteEmptyDirs: true,

		TrashEnabled: false,
		TrashDir:     ".TRASH",

		TrashCleanupEnabled:         false,
		TrashCleanupIntervalSec:     6 * 60 * 60,      // 6 hours
		TrashCleanupMaxAgeSec:       7 * 24 * 60 * 60, // 7 days
		TrashCleanupDeleteEmptyDirs: true,
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.Endpoint == "" {
		c.Endpoint = "/wicos64/api"
	}
	if !strings.HasPrefix(c.Endpoint, "/") {
		return fmt.Errorf("endpoint must start with '/'")
	}
	if c.BasePath == "" {
		c.BasePath = "./wicos64-data"
	}
	if c.MaxPath == 0 {
		c.MaxPath = 255
	}
	if c.MaxName == 0 {
		c.MaxName = 64
	}
	if c.MaxEntries == 0 {
		c.MaxEntries = 50
	}
	if c.MaxPayload == 0 {
		c.MaxPayload = 16384
	}
	if c.MaxChunk == 0 {
		c.MaxChunk = 4096
	}
	if c.MaxChunk > c.MaxPayload {
		return fmt.Errorf("max_chunk (%d) must be <= max_payload (%d)", c.MaxChunk, c.MaxPayload)
	}
	if c.ServerName == "" {
		c.ServerName = "wicos64-go-backend"
	}
	if c.AdminUser == "" {
		c.AdminUser = "admin"
	}

	// Housekeeping defaults (only if enabled).
	if c.TmpCleanupEnabled {
		if c.TmpCleanupIntervalSec <= 0 {
			c.TmpCleanupIntervalSec = 15 * 60
		}
		if c.TmpCleanupMaxAgeSec <= 0 {
			c.TmpCleanupMaxAgeSec = 24 * 60 * 60
		}
		// Clamp to sane bounds.
		if c.TmpCleanupIntervalSec < 10 {
			c.TmpCleanupIntervalSec = 10
		}
		if c.TmpCleanupMaxAgeSec < 60 {
			c.TmpCleanupMaxAgeSec = 60
		}
	}

	// Trash defaults/validation.
	c.TrashDir = strings.TrimSpace(c.TrashDir)
	if c.TrashDir == "" {
		c.TrashDir = ".TRASH"
	}
	if strings.ContainsAny(c.TrashDir, "/\\") {
		return fmt.Errorf("trash_dir must be a single directory name (no slashes)")
	}
	if c.TrashDir == "." || c.TrashDir == ".." {
		return fmt.Errorf("trash_dir must not be '.' or '..'")
	}
	if c.TrashCleanupEnabled {
		if c.TrashCleanupIntervalSec <= 0 {
			c.TrashCleanupIntervalSec = 6 * 60 * 60
		}
		if c.TrashCleanupIntervalSec < 60 {
			c.TrashCleanupIntervalSec = 60
		}
		if c.TrashCleanupMaxAgeSec <= 0 {
			c.TrashCleanupMaxAgeSec = 7 * 24 * 60 * 60
		}
		if c.TrashCleanupMaxAgeSec < 60 {
			c.TrashCleanupMaxAgeSec = 60
		}
	}
	// Bootstrap defaults/validation.
	if c.Bootstrap.UnknownMACPolicy == "" {
		c.Bootstrap.UnknownMACPolicy = "deny"
	}
	if c.Bootstrap.MacTokens == nil {
		c.Bootstrap.MacTokens = map[string]string{}
	}
	if c.Bootstrap.MacExtra == nil {
		c.Bootstrap.MacExtra = map[string]map[string]string{}
	}
	if c.Bootstrap.Enabled {
		if strings.TrimSpace(c.Bootstrap.Token) == "" {
			return fmt.Errorf("bootstrap.enabled=true but bootstrap.token is empty")
		}
		// Normalize MAC keys to the canonical AABBCCDDEEFF form.
		norm := map[string]string{}
		for k, v := range c.Bootstrap.MacTokens {
			nk, ok := normalizeMACKey(k)
			if !ok {
				return fmt.Errorf("invalid MAC in bootstrap.mac_tokens: %q", k)
			}
			// v may be empty to intentionally disable a MAC mapping.
			norm[nk] = v
		}
		c.Bootstrap.MacTokens = norm

		// Normalize MAC keys and validate/normalize extra key/value pairs.
		normExtra := map[string]map[string]string{}
		for k, kv := range c.Bootstrap.MacExtra {
			nk, ok := normalizeMACKey(k)
			if !ok {
				return fmt.Errorf("invalid MAC in bootstrap.mac_extra: %q", k)
			}
			if kv == nil {
				continue
			}
			nm := map[string]string{}
			for kk, vv := range kv {
				nkk, ok := normalizeBootstrapKVKey(kk)
				if !ok {
					return fmt.Errorf("invalid key in bootstrap.mac_extra[%s]: %q", nk, kk)
				}
				if isReservedBootstrapKVKey(nkk) {
					return fmt.Errorf("bootstrap.mac_extra[%s] uses reserved key: %q", nk, nkk)
				}
				vv = strings.TrimSpace(vv)
				if strings.ContainsAny(vv, "\r\n") {
					return fmt.Errorf("bootstrap.mac_extra[%s][%s] must not contain newlines", nk, nkk)
				}
				nm[nkk] = vv
			}
			normExtra[nk] = nm
		}
		c.Bootstrap.MacExtra = normExtra
	}

	// Discovery defaults/validation.
	if c.Discovery.UDPPort == 0 {
		c.Discovery.UDPPort = 6464
	}
	if c.Discovery.RateLimitPerSec == 0 {
		// Default is intentionally low; this is a LAN discovery helper.
		c.Discovery.RateLimitPerSec = 5
	}
	if c.Discovery.UDPPort < 1 || c.Discovery.UDPPort > 65535 {
		return fmt.Errorf("discovery.udp_port out of range: %d", c.Discovery.UDPPort)
	}
	if c.Discovery.RateLimitPerSec < 0 {
		c.Discovery.RateLimitPerSec = 0
	}

	// Validate tokens list (if present).
	seen := map[string]struct{}{}
	for _, t := range c.Tokens {
		if strings.TrimSpace(t.Token) == "" {
			continue
		}
		if _, ok := seen[t.Token]; ok {
			return fmt.Errorf("duplicate token in tokens[]")
		}
		seen[t.Token] = struct{}{}
	}

	return nil
}

func minNonZero(a, b uint64) uint64 {
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

// normalizeMACKey converts common MAC representations (aa:bb:cc:dd:ee:ff,
// aabbccddeeff, aa-bb-..., etc.) to the canonical AABBCCDDEEFF form.
func normalizeMACKey(s string) (string, bool) {
	if strings.TrimSpace(s) == "" {
		return "", false
	}
	// Keep only hex digits.
	buf := make([]byte, 0, 12)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			buf = append(buf, c)
		case c >= 'a' && c <= 'f':
			buf = append(buf, c-32) // upper
		case c >= 'A' && c <= 'F':
			buf = append(buf, c)
		case c == ':' || c == '-' || c == '.' || c == ' ' || c == '\t':
			continue
		default:
			return "", false
		}
		if len(buf) > 12 {
			return "", false
		}
	}
	if len(buf) != 12 {
		return "", false
	}
	return string(buf), true
}

// normalizeBootstrapKVKey normalizes a bootstrap KEY in the KEY=VALUE response.
//
// Rules:
//   - trim spaces
//   - convert to upper-case
//   - allow only A-Z, 0-9 and '_' (hyphen is normalized to '_')
//   - reject empty keys
func normalizeBootstrapKVKey(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	buf := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
			buf = append(buf, c-32)
		case c >= 'A' && c <= 'Z':
			buf = append(buf, c)
		case c >= '0' && c <= '9':
			buf = append(buf, c)
		case c == '_' || c == '-':
			buf = append(buf, '_')
		default:
			return "", false
		}
	}
	// Trim leading/trailing underscores (common user input like "_foo").
	k := strings.Trim(string(buf), "_")
	if k == "" {
		return "", false
	}
	if len(k) > 48 {
		return "", false
	}
	return k, true
}

func isReservedBootstrapKVKey(k string) bool {
	switch k {
	case "WICOS64CFG", "END", "API_URL", "TOKEN", "RO", "MAC", "SERVER_NAME", "QUOTA_BYTES", "MAX_FILE_BYTES":
		return true
	default:
		return false
	}
}

// ResolveTokenContext returns the on-disk root path and effective policy for the given token.
// ok=false means the token is not accepted.
func (c Config) ResolveTokenContext(token string) (TokenContext, bool) {
	// New token mapping (preferred).
	if len(c.Tokens) > 0 {
		for _, t := range c.Tokens {
			if t.Token == "" {
				continue
			}
			if token != t.Token {
				continue
			}
			enabled := true
			if t.Enabled != nil {
				enabled = *t.Enabled
			}
			if !enabled {
				return TokenContext{}, false
			}
			root := t.Root
			if root == "" {
				root = c.BasePath
			} else if !filepath.IsAbs(root) {
				root = filepath.Join(c.BasePath, root)
			}
			diskImages := c.DiskImagesEnabled
			if t.DiskImagesEnabled != nil {
				diskImages = *t.DiskImagesEnabled
			}
			diskImagesWrite := c.DiskImagesWriteEnabled
			if t.DiskImagesWriteEnabled != nil {
				diskImagesWrite = *t.DiskImagesWriteEnabled
			}
			diskImagesAutoResize := c.DiskImagesAutoResizeEnabled
			if t.DiskImagesAutoResizeEnabled != nil {
				diskImagesAutoResize = *t.DiskImagesAutoResizeEnabled
			}
			allowRenameConvert := false
			if t.DiskImagesAllowRenameConvert != nil {
				allowRenameConvert = *t.DiskImagesAllowRenameConvert
			}
			return TokenContext{
				Root:                         root,
				Name:                         t.Name,
				ReadOnly:                     c.GlobalReadOnly || t.ReadOnly,
				QuotaBytes:                   minNonZero(t.QuotaBytes, c.GlobalQuotaBytes),
				MaxFileBytes:                 minNonZero(t.MaxFileBytes, c.GlobalMaxFileBytes),
				Legacy:                       false,
				DiskImagesEnabled:            diskImages,
				DiskImagesWriteEnabled:       diskImagesWrite,
				DiskImagesAutoResizeEnabled:  diskImagesAutoResize,
				DiskImagesAllowRenameConvert: allowRenameConvert,
			}, true
		}
		return TokenContext{}, false
	}

	// Legacy multi-user mapping.
	if len(c.TokenRoots) > 0 {
		r, exists := c.TokenRoots[token]
		if !exists || r == "" {
			return TokenContext{}, false
		}
		if filepath.IsAbs(r) {
			return TokenContext{Root: r, ReadOnly: c.GlobalReadOnly, QuotaBytes: c.GlobalQuotaBytes, MaxFileBytes: c.GlobalMaxFileBytes, DiskImagesEnabled: c.DiskImagesEnabled, DiskImagesWriteEnabled: c.DiskImagesWriteEnabled, DiskImagesAutoResizeEnabled: c.DiskImagesAutoResizeEnabled, Legacy: true}, true
		}
		return TokenContext{Root: filepath.Join(c.BasePath, r), ReadOnly: c.GlobalReadOnly, QuotaBytes: c.GlobalQuotaBytes, MaxFileBytes: c.GlobalMaxFileBytes, DiskImagesEnabled: c.DiskImagesEnabled, DiskImagesWriteEnabled: c.DiskImagesWriteEnabled, DiskImagesAutoResizeEnabled: c.DiskImagesAutoResizeEnabled, Legacy: true}, true
	}

	// Legacy single token mapping.
	if c.Token != "" {
		if token != c.Token {
			return TokenContext{}, false
		}
		return TokenContext{Root: c.BasePath, ReadOnly: c.GlobalReadOnly, QuotaBytes: c.GlobalQuotaBytes, MaxFileBytes: c.GlobalMaxFileBytes, DiskImagesEnabled: c.DiskImagesEnabled, DiskImagesWriteEnabled: c.DiskImagesWriteEnabled, DiskImagesAutoResizeEnabled: c.DiskImagesAutoResizeEnabled, Legacy: true}, true
	}

	// No auth (NOT RECOMMENDED) – treat everything as one root.
	return TokenContext{Root: c.BasePath, ReadOnly: c.GlobalReadOnly, QuotaBytes: c.GlobalQuotaBytes, MaxFileBytes: c.GlobalMaxFileBytes, DiskImagesEnabled: c.DiskImagesEnabled, DiskImagesWriteEnabled: c.DiskImagesWriteEnabled, DiskImagesAutoResizeEnabled: c.DiskImagesAutoResizeEnabled, Legacy: true}, true
}

// ResolveTokenRoot returns the absolute on-disk root path for the given token.
// ok=false means the token is not accepted.
func (c Config) ResolveTokenRoot(token string) (root string, ok bool) {
	ctx, ok := c.ResolveTokenContext(token)
	if !ok {
		return "", false
	}
	return ctx.Root, true
}

// EnsureRoot makes sure the root directory exists.
func EnsureRoot(path string) error {
	return os.MkdirAll(path, 0o755)
}
