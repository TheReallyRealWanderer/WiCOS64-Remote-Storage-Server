package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"wicos64-server/internal/config"
	"wicos64-server/internal/server"
	"wicos64-server/internal/version"
)

func main() {
	var configPath string
	var showVersion bool
	var openAdmin bool
	var logFile string

	// Default config location: ./config/config.json next to the EXE.
	// (We still support legacy ./config.json via auto-detection.)
	flag.StringVar(&configPath, "config", filepath.Join("config", "config.json"), "Path to config json file")
	flag.BoolVar(&showVersion, "version", false, "Print version information and exit")
	flag.BoolVar(&openAdmin, "open-admin", false, "Open the admin UI in the default browser after startup")
	flag.StringVar(&logFile, "log-file", "", "Optional log file path (Windows default: ./logs/server.log next to the EXE)")
	flag.Parse()

	if showVersion {
		fmt.Println(version.Get().String())
		return
	}

	// Determine whether -config was provided explicitly.
	configProvided := false
	flag.CommandLine.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			configProvided = true
		}
	})

	exeDir := safeExeDir()

	// In "no-console" Windows builds stdout/stderr are often not visible.
	// As a safety net, default to a log file under ./logs/server.log next to the EXE.
	if logFile == "" && runtime.GOOS == "windows" {
		logFile = filepath.Join(exeDir, "logs", "server.log")
	}
	if logFile != "" {
		_ = setupLogFile(logFile)
	}

	// Resolve config path.
	resolvedCfgPath, err := resolveConfigPath(configPath, configProvided)
	if err != nil {
		log.Printf("FATAL: resolve config path: %v", err)
		fmt.Fprintln(os.Stderr, "Failed to resolve config:", err)
		os.Exit(1)
	}
	configPath = resolvedCfgPath

	// Load + validate config.
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Printf("FATAL: load config %q: %v", configPath, err)
		fmt.Fprintln(os.Stderr, "Failed to load config:", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		log.Printf("FATAL: invalid config %q: %v", configPath, err)
		fmt.Fprintln(os.Stderr, "Invalid config:", err)
		os.Exit(1)
	}

	srv := server.New(cfg, configPath)

	log.Printf("WiCOS64 backend %s", version.Get().String())
	log.Printf("Config: %s", configPath)
	log.Printf("Listening on %s%s", cfg.Listen, cfg.Endpoint)
	log.Printf("Base path: %s", cfg.BasePath)
	if cfg.EnableAdminUI {
		log.Printf("Admin UI: %s (localhost-only by default)", adminURLFromListen(cfg.Listen)+"/admin")
	}

	h := srv.HTTPHandler()

	// Bind first (so we can fail early), then serve.
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Printf("FATAL: listen %q failed: %v", cfg.Listen, err)
		fmt.Fprintln(os.Stderr, "Listen failed:", err)
		os.Exit(1)
	}

	// Optionally open the admin UI after the server is up.
	if openAdmin && cfg.EnableAdminUI {
		url := adminURLFromListen(cfg.Listen) + "/admin"
		go func() {
			// Small delay so the listener has time to accept.
			time.Sleep(250 * time.Millisecond)
			_ = openBrowser(url)
		}()
	}

	// Serve forever.
	if err := http.Serve(ln, h); err != nil {
		log.Fatal(err)
	}
}

func safeExeDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	d := filepath.Dir(exe)
	if d == "" {
		return "."
	}
	return d
}

func exists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

// resolveConfigPath implements the recommended behavior:
// - if -config was provided, use it as-is.
// - otherwise, prefer ./config/config.json next to the EXE.
// - compatibility: fall back to legacy ./config.json next to the EXE.
// - if missing, create it from config.example.json (same dir) or from defaults.
func resolveConfigPath(flagValue string, configProvided bool) (string, error) {
	exeDir := safeExeDir()

	if configProvided {
		// Use the caller-provided path (relative to current working dir, as usual).
		return flagValue, nil
	}

	preferred := filepath.Join(exeDir, "config", "config.json")
	legacy := filepath.Join(exeDir, "config.json")
	if exists(preferred) {
		return preferred, nil
	}
	if exists(legacy) {
		return legacy, nil
	}

	// Compatibility fallback: if a config.json exists in the current working directory, use it.
	if exists("config.json") {
		return "config.json", nil
	}

	// Create the preferred config path.
	// We support both of these example locations:
	//   - ./config/config.example.json
	//   - ./config.example.json (legacy)
	exampleCandidates := []string{
		filepath.Join(exeDir, "config", "config.example.json"),
		filepath.Join(exeDir, "config.example.json"),
	}
	for _, ex := range exampleCandidates {
		if exists(ex) {
			if err := copyFile(ex, preferred); err != nil {
				return "", fmt.Errorf("copy %s -> %s: %w", ex, preferred, err)
			}
			return preferred, nil
		}
	}
	// If there is an example in CWD, use it to seed the CWD config.json.
	// (Useful for running from source.)
	if exists(filepath.Join("config", "config.example.json")) {
		if err := copyFile(filepath.Join("config", "config.example.json"), filepath.Join("config", "config.json")); err != nil {
			return "", fmt.Errorf("copy config/config.example.json -> config/config.json: %w", err)
		}
		return filepath.Join("config", "config.json"), nil
	}
	if exists("config.example.json") {
		if err := copyFile("config.example.json", filepath.Join("config", "config.json")); err != nil {
			return "", fmt.Errorf("copy config.example.json -> config/config.json: %w", err)
		}
		return filepath.Join("config", "config.json"), nil
	}

	// Finally: write a default config.json to the preferred location.
	def := config.Default()
	b, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(preferred), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(preferred, append(b, '\n'), 0o644); err != nil {
		return "", err
	}
	return preferred, nil
}

func setupLogFile(path string) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	// Log to file and (if present) stdout.
	log.SetOutput(io.MultiWriter(os.Stdout, f))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	return nil
}

func adminURLFromListen(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		// Best-effort fallback.
		if strings.HasPrefix(listen, ":") {
			return "http://127.0.0.1" + listen
		}
		return "http://127.0.0.1:8080"
	}
	// If Listen binds to all interfaces, keep the admin URL on localhost.
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	// Some people might set "localhost".
	if host == "localhost" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func openBrowser(url string) error {
	url = strings.TrimSpace(url)
	if url == "" {
		return errors.New("empty url")
	}
	switch runtime.GOOS {
	case "windows":
		// Use the shell to open the URL.
		return exec.Command("cmd", "/c", "start", "", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default:
		// linux, *bsd, etc.
		return exec.Command("xdg-open", url).Start()
	}
}
