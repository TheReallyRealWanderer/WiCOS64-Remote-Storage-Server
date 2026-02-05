//go:build windows

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
	"sync"
	"syscall"
	"time"
	"unsafe"

	"wicos64-server/internal/config"
	"wicos64-server/internal/version"
)

// This is a small Windows tray controller for WiCOS64 Remote Storage.
// It intentionally uses only the Go standard library (no external deps),
// so it can be built fully offline.

// ---- Win32 constants ----

const (
	wmDestroy       = 0x0002
	wmClose         = 0x0010
	wmCommand       = 0x0111
	wmUser          = 0x0400
	wmNull          = 0x0000
	wmContextMenu   = 0x007B
	wmLButtonUp     = 0x0202
	wmLButtonDblClk = 0x0203
	wmRButtonDown   = 0x0204
	wmRButtonUp     = 0x0205

	// For NOTIFYICONDATA.uCallbackMessage the valid range is WM_USER..0x7FFF.
	trayCallbackMsg = wmUser + 1

	// Window class style.
	csDblclks = 0x0008
)

const (
	notifyIconVersion4 = 4 // NOTIFYICON_VERSION_4
)

const (
	nimAdd        = 0x00000000
	nimModify     = 0x00000001
	nimDelete     = 0x00000002
	nimSetVersion = 0x00000004

	nifMessage = 0x00000001
	nifIcon    = 0x00000002
	nifTip     = 0x00000004
)

const (
	mfString    = 0x0000
	mfSeparator = 0x0800
	mfGrayed    = 0x0001
	mfDisabled  = 0x0002
)

const (
	errorAlreadyExists = 183
	mbOK               = 0x00000000
	mbYesNo            = 0x00000004
	mbIconInfo         = 0x00000040
	mbIconQuestion     = 0x00000020
	mbIconError        = 0x00000010
	// MessageBox return codes.
	idYes = 6
	idNo  = 7
)

const (
	imageIcon      = 1
	lrLoadFromFile = 0x00000010
	lrDefaultSize  = 0x00000040
)

const (
	tpmRightButton = 0x0002
	tpmNonotify    = 0x0080
	tpmReturnCmd   = 0x0100
)

const (
	swShownormal = 1
)

// Command IDs.
const (
	cmdOpenAdmin = 1001 + iota
	cmdStartServer
	cmdStopServer
	cmdRestartServer
	cmdReloadConfig
	cmdRunSelfTest
	cmdRunCleanup
	cmdOpenConfig
	cmdOpenDataFolder
	cmdViewLog
	cmdExit
)

// ---- Win32 DLL procs ----

var (
	modUser32   = syscall.NewLazyDLL("user32.dll")
	modShell32  = syscall.NewLazyDLL("shell32.dll")
	modKernel32 = syscall.NewLazyDLL("kernel32.dll")

	procRegisterClassExW    = modUser32.NewProc("RegisterClassExW")
	procCreateWindowExW     = modUser32.NewProc("CreateWindowExW")
	procDefWindowProcW      = modUser32.NewProc("DefWindowProcW")
	procGetMessageW         = modUser32.NewProc("GetMessageW")
	procTranslateMessage    = modUser32.NewProc("TranslateMessage")
	procDispatchMessageW    = modUser32.NewProc("DispatchMessageW")
	procPostQuitMessage     = modUser32.NewProc("PostQuitMessage")
	procLoadIconW           = modUser32.NewProc("LoadIconW")
	procCreatePopupMenu     = modUser32.NewProc("CreatePopupMenu")
	procAppendMenuW         = modUser32.NewProc("AppendMenuW")
	procTrackPopupMenu      = modUser32.NewProc("TrackPopupMenu")
	procSetForegroundWindow = modUser32.NewProc("SetForegroundWindow")
	procGetCursorPos        = modUser32.NewProc("GetCursorPos")
	procDestroyMenu         = modUser32.NewProc("DestroyMenu")
	procMessageBoxW         = modUser32.NewProc("MessageBoxW")
	procPostMessageW        = modUser32.NewProc("PostMessageW")
	procLoadImageW          = modUser32.NewProc("LoadImageW")

	procShellNotifyIconW = modShell32.NewProc("Shell_NotifyIconW")
	procShellExecuteW    = modShell32.NewProc("ShellExecuteW")

	procGetModuleHandleW = modKernel32.NewProc("GetModuleHandleW")
	procCreateMutexW     = modKernel32.NewProc("CreateMutexW")
	procGetLastError     = modKernel32.NewProc("GetLastError")
	procCloseHandle      = modKernel32.NewProc("CloseHandle")
)

// ---- Win32 structs ----

type point struct {
	x int32
	y int32
}

type msg struct {
	hWnd    syscall.Handle
	message uint32
	wParam  uintptr
	lParam  uintptr
	time    uint32
	pt      point
}

type guid struct {
	Data1 uint32
	Data2 uint16
	Data3 uint16
	Data4 [8]byte
}

type wndclassex struct {
	cbSize        uint32
	style         uint32
	lpfnWndProc   uintptr
	cbClsExtra    int32
	cbWndExtra    int32
	hInstance     syscall.Handle
	hIcon         syscall.Handle
	hCursor       syscall.Handle
	hbrBackground syscall.Handle
	lpszMenuName  *uint16
	lpszClassName *uint16
	hIconSm       syscall.Handle
}

type notifyIconData struct {
	cbSize           uint32
	hWnd             syscall.Handle
	uID              uint32
	uFlags           uint32
	uCallbackMessage uint32
	hIcon            syscall.Handle
	szTip            [128]uint16

	dwState      uint32
	dwStateMask  uint32
	szInfo       [256]uint16
	uVersion     uint32 // union uTimeout/uVersion
	szInfoTitle  [64]uint16
	dwInfoFlags  uint32
	guidItem     guid
	hBalloonIcon syscall.Handle
}

// ---- App ----

type trayApp struct {
	mu sync.Mutex

	logf   *os.File
	logger *log.Logger

	exeDir     string
	configPath string
	serverExe  string
	autoStart  bool

	// last-loaded config snapshot
	cfg config.Config

	// derived
	baseURL    string // preferred base URL for admin actions (usually http://127.0.0.1:PORT)
	altBaseURL string // fallback base URL based on the raw listen address (e.g. http://LAN-IP:PORT)
	adminURL   string // .../admin
	logPath    string
	dataPath   string
	adminUser  string
	adminPass  string

	// process started by tray (optional)
	serverCmd *exec.Cmd

	// tray
	hWnd  syscall.Handle
	nid   notifyIconData
	mutex syscall.Handle
	hIcon syscall.Handle
}

var gApp *trayApp

// Keep the Win32 callback pointer alive for the lifetime of the process.
//
// Important: syscall.NewCallback returns a value that must stay reachable.
// If it gets GC'ed, Windows will silently stop delivering messages to our
// window procedure (especially visible on Win11 tray behavior).
var wndProcPtr = syscall.NewCallback(wndProc)

func main() {
	var configFlag string
	var serverFlag string
	var autoStart bool
	var debug bool
	var showVersion bool

	flag.StringVar(&configFlag, "config", "", "Path to config.json (default: config\\config.json next to the EXE; falls back to legacy config.json)")
	flag.StringVar(&serverFlag, "server", "", "Path to wicos64-server.exe (default: same folder as tray EXE)")
	flag.BoolVar(&autoStart, "autostart", true, "Auto-start the server if it is not running")
	flag.BoolVar(&debug, "debug", false, "Enable tray debug logging to logs/tray.log")
	flag.BoolVar(&showVersion, "version", false, "Print version information and exit")
	flag.Parse()

	if showVersion {
		fmt.Println(version.Get().String())
		return
	}

	// Win32 tray GUI requires a stable OS thread: window creation and message loop
	// must run on the same thread, otherwise click events may not arrive (Win11).
	runtime.LockOSThread()

	// Single-instance guard (prevents multiple tray icons / broken state after
	// accidental double starts).
	hMutex, alreadyRunning, err := singleInstanceMutex("Local\\WiCOS64RemoteStorageTray")
	if err != nil {
		showError("WiCOS64 Tray", err.Error())
		return
	}
	if alreadyRunning {
		showInfo("WiCOS64 Tray", "Tray is already running.")
		return
	}
	defer closeHandle(hMutex)

	app, err := newTrayApp(configFlag, serverFlag, autoStart, debug)
	if err != nil {
		showError("WiCOS64 Tray", err.Error())
		return
	}
	gApp = app

	if err := app.initWindowAndTray(); err != nil {
		showError("WiCOS64 Tray", err.Error())
		return
	}

	app.updateTooltip()

	// Keep the UI responsive: start server in the background and refresh the tooltip.
	go func() {
		if app.autoStart {
			_ = app.refreshConfig()
			if !app.isServerRunning() {
				_ = app.startServer()
			}
			app.updateTooltip()
		}
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			app.updateTooltip()
		}
	}()

	app.messageLoop()
}

func newTrayApp(configFlag, serverFlag string, autoStart bool, debug bool) (*trayApp, error) {
	exeDir := safeExeDir()
	configPath := strings.TrimSpace(configFlag)
	if configPath == "" {
		configPath = defaultConfigPath(exeDir)
	}
	serverExe := strings.TrimSpace(serverFlag)
	if serverExe == "" {
		serverExe = filepath.Join(exeDir, "wicos64-server.exe")
	}

	// Ensure config exists (seed from config.example.json if possible).
	if err := ensureConfigFile(exeDir, configPath); err != nil {
		return nil, err
	}

	app := &trayApp{
		exeDir:     exeDir,
		configPath: configPath,
		serverExe:  serverExe,
		autoStart:  autoStart,
		logPath:    filepath.Join(exeDir, "logs", "server.log"),
	}
	// Optional tray debug log (off by default).
	if debug {
		_ = os.MkdirAll(filepath.Join(exeDir, "logs"), 0o755)
		if f, err := os.OpenFile(filepath.Join(exeDir, "logs", "tray.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			app.logf = f
			app.logger = log.New(f, "[tray] ", log.LstdFlags|log.Lmicroseconds)
			app.logger.Printf("starting tray %s (config=%s server=%s)", version.Get().String(), configPath, serverExe)
		}
	}
	_ = app.refreshConfig()
	return app, nil
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

func defaultConfigPath(exeDir string) string {
	// New default location:
	preferred := filepath.Join(exeDir, "config", "config.json")
	if fileExists(preferred) {
		return preferred
	}
	// Backwards-compatible location:
	legacy := filepath.Join(exeDir, "config.json")
	if fileExists(legacy) {
		return legacy
	}
	// If neither exists yet, create the preferred one.
	return preferred
}

func ensureConfigFile(exeDir, configPath string) error {
	// If config exists already: done.
	if fileExists(configPath) {
		return nil
	}

	// Try to seed from a config.example.json next to the EXE.
	// We support both:
	//   - .\\config\\config.example.json
	//   - .\\config.example.json (legacy)
	exampleCandidates := []string{
		filepath.Join(exeDir, "config", "config.example.json"),
		filepath.Join(exeDir, "config.example.json"),
	}
	for _, example := range exampleCandidates {
		if fileExists(example) {
			if err := copyFile(example, configPath); err != nil {
				return fmt.Errorf("copy %s -> %s: %w", example, configPath, err)
			}
			return nil
		}
	}

	// Otherwise: write defaults.
	def := config.Default()
	b, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(configPath, append(b, '\n'), 0o644)
}

func fileExists(path string) bool {
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
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}

func (a *trayApp) refreshConfig() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	cfg, err := config.Load(a.configPath)
	if err != nil {
		return err
	}
	// Validate again (Load already validates, but keep explicit).
	if err := cfg.Validate(); err != nil {
		return err
	}

	a.cfg = cfg
	a.adminUser = cfg.AdminUser
	a.adminPass = cfg.AdminPassword

	baseAdmin := baseURLForAdmin(cfg.Listen, cfg.AdminAllowRemote)
	baseRaw := baseURLFromListenRaw(cfg.Listen)
	a.baseURL = baseAdmin
	a.altBaseURL = baseRaw
	a.adminURL = baseAdmin + "/admin"

	// Resolve data folder relative to EXE dir (tray starts the server with cmd.Dir=exeDir).
	if filepath.IsAbs(cfg.BasePath) {
		a.dataPath = cfg.BasePath
	} else {
		a.dataPath = filepath.Join(a.exeDir, cfg.BasePath)
	}
	return nil
}

// baseURLForAdmin chooses a base URL the tray should use for admin requests.
//
// If the Admin UI is localhost-only, we prefer 127.0.0.1 even if the server
// listens on a LAN IP.
func baseURLForAdmin(listen string, allowRemote bool) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		if strings.HasPrefix(listen, ":") {
			return "http://127.0.0.1" + listen
		}
		return "http://127.0.0.1:8080"
	}
	// Prefer localhost for admin actions.
	if host == "" || host == "0.0.0.0" || host == "::" || host == "localhost" {
		host = "127.0.0.1"
	}
	if host != "127.0.0.1" && !allowRemote {
		// Admin is localhost-only by default, so use localhost in the tray.
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

// baseURLFromListenRaw derives a base URL from the listen address without
// forcing localhost-only behaviour.
func baseURLFromListenRaw(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		if strings.HasPrefix(listen, ":") {
			return "http://127.0.0.1" + listen
		}
		return "http://127.0.0.1:8080"
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "localhost" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func (a *trayApp) isServerRunning() bool {
	a.mu.Lock()
	base := a.baseURL
	alt := a.altBaseURL
	a.mu.Unlock()
	if base == "" && alt == "" {
		return false
	}
	if pingHTTP(base) {
		return true
	}
	if alt != "" && alt != base {
		return pingHTTP(alt)
	}
	return false
}

func pingHTTP(base string) bool {
	if base == "" {
		return false
	}
	c := &http.Client{Timeout: 800 * time.Millisecond}
	resp, err := c.Get(base + "/")
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 500
}

func singleInstanceMutex(name string) (syscall.Handle, bool, error) {
	ptr, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return 0, false, err
	}
	h, _, _ := procCreateMutexW.Call(0, 0, uintptr(unsafe.Pointer(ptr)))
	if h == 0 {
		return 0, false, errors.New("CreateMutexW failed")
	}
	le, _, _ := procGetLastError.Call()
	if le == errorAlreadyExists {
		return syscall.Handle(h), true, nil
	}
	return syscall.Handle(h), false, nil
}

func closeHandle(h syscall.Handle) {
	if h == 0 {
		return
	}
	procCloseHandle.Call(uintptr(h))
}

func loadIconFromFile(path string) syscall.Handle {
	ptr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0
	}
	h, _, _ := procLoadImageW.Call(0, uintptr(unsafe.Pointer(ptr)), imageIcon, 0, 0, lrLoadFromFile|lrDefaultSize)
	return syscall.Handle(h)
}

func (a *trayApp) startServer() error {
	a.mu.Lock()
	serverExe := a.serverExe
	configPath := a.configPath
	exeDir := a.exeDir
	a.mu.Unlock()

	if a.isServerRunning() {
		return nil
	}
	if !fileExists(serverExe) {
		return fmt.Errorf("server EXE not found: %s", serverExe)
	}

	cmd := exec.Command(serverExe, "-config", configPath)
	cmd.Dir = exeDir
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if err := cmd.Start(); err != nil {
		return err
	}

	a.mu.Lock()
	a.serverCmd = cmd
	a.mu.Unlock()

	// Wait briefly until it responds.
	for i := 0; i < 25; i++ {
		time.Sleep(200 * time.Millisecond)
		if a.isServerRunning() {
			a.updateTooltip()
			return nil
		}
	}
	a.updateTooltip()
	return errors.New("server did not come up in time (check logs/server.log)")
}

func (a *trayApp) stopServer() error {
	_ = a.refreshConfig()
	if !a.isServerRunning() {
		a.mu.Lock()
		a.serverCmd = nil
		a.mu.Unlock()
		a.updateTooltip()
		return nil
	}

	// Prefer a graceful shutdown via the admin endpoint if available.
	if a.cfg.EnableAdminUI {
		_ = a.adminPost("/admin/api/shutdown")
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
			if !a.isServerRunning() {
				a.mu.Lock()
				a.serverCmd = nil
				a.mu.Unlock()
				a.updateTooltip()
				return nil
			}
		}
	}

	// Fallback: if the tray started the server, we can terminate by PID.
	a.mu.Lock()
	cmd := a.serverCmd
	a.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		a.mu.Lock()
		a.serverCmd = nil
		a.mu.Unlock()
		a.updateTooltip()
		return nil
	}

	if !a.cfg.EnableAdminUI {
		return errors.New("cannot stop server: enable_admin_ui=false and server was not started by tray")
	}
	return errors.New("server did not stop (check admin credentials and logs/server.log)")
}

func (a *trayApp) restartServer() error {
	// Best effort: stop if running, then start again.
	if a.isServerRunning() {
		if err := a.stopServer(); err != nil {
			return err
		}
		// tiny delay to allow socket release
		time.Sleep(200 * time.Millisecond)
	}
	return a.startServer()
}

func (a *trayApp) adminPost(path string) error {
	if err := a.refreshConfig(); err != nil {
		return err
	}
	base := a.bestBaseURL()
	a.mu.Lock()
	url := base + path
	u := a.adminUser
	p := a.adminPass
	a.mu.Unlock()

	req, _ := http.NewRequest(http.MethodPost, url, nil)
	if p != "" {
		req.SetBasicAuth(u, p)
	}
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("admin request failed: %s", resp.Status)
}

func (a *trayApp) bestBaseURL() string {
	a.mu.Lock()
	base := a.baseURL
	alt := a.altBaseURL
	a.mu.Unlock()
	if pingHTTP(base) {
		return base
	}
	if alt != "" && alt != base && pingHTTP(alt) {
		return alt
	}
	if base != "" {
		return base
	}
	return alt
}

func (a *trayApp) openAdmin() error {
	_ = a.refreshConfig()
	base := a.bestBaseURL()
	if base == "" {
		return errors.New("no base URL available (check listen in config.json)")
	}
	return a.openURL(base + "/admin")
}

func (a *trayApp) openURL(url string) error {
	return shellOpen(url)
}

func (a *trayApp) openFile(path string) error {
	return shellOpen(path)
}

func (a *trayApp) openFolder(path string) error {
	return shellOpen(path)
}

func (a *trayApp) updateTooltip() {
	// Keep it short; Windows tooltips are limited.
	tip := "WiCOS64 Remote Storage"
	if a.isServerRunning() {
		tip += " - running"
	} else {
		tip += " - stopped"
	}
	_ = a.setTooltip(tip)
}

// ---- Win32 tray plumbing ----

func (a *trayApp) initWindowAndTray() error {
	if runtime.GOOS != "windows" {
		return errors.New("tray is windows-only")
	}

	className, _ := syscall.UTF16PtrFromString("WiCOS64TrayWindow")
	inst := getModuleHandle()

	wc := wndclassex{
		cbSize:        uint32(unsafe.Sizeof(wndclassex{})),
		style:         csDblclks,
		lpfnWndProc:   wndProcPtr,
		hInstance:     inst,
		lpszClassName: className,
	}
	if r, _, err := procRegisterClassExW.Call(uintptr(unsafe.Pointer(&wc))); r == 0 {
		return fmt.Errorf("RegisterClassExW failed: %v", err)
	}

	hWnd, _, err := procCreateWindowExW.Call(
		0,
		uintptr(unsafe.Pointer(className)),
		uintptr(unsafe.Pointer(className)),
		0,
		0, 0, 0, 0,
		0,
		0,
		uintptr(inst),
		0,
	)
	if hWnd == 0 {
		return fmt.Errorf("CreateWindowExW failed: %v", err)
	}
	a.hWnd = syscall.Handle(hWnd)

	// Tray icon:
	// 1) prefer .\img\wicos64.ico next to the tray EXE
	// 2) fallback to legacy .\wicos64.ico
	// 3) fallback to the default application icon
	var hIcon syscall.Handle
	if a.exeDir != "" {
		preferredIcon := filepath.Join(a.exeDir, "img", "wicos64.ico")
		legacyIcon := filepath.Join(a.exeDir, "wicos64.ico")
		if fileExists(preferredIcon) {
			hIcon = loadIconFromFile(preferredIcon)
		} else if fileExists(legacyIcon) {
			hIcon = loadIconFromFile(legacyIcon)
		}
	}
	if hIcon == 0 {
		const idiApplication = 32512
		hi, _, _ := procLoadIconW.Call(0, uintptr(idiApplication))
		hIcon = syscall.Handle(hi)
	}
	if hIcon == 0 {
		return errors.New("could not load tray icon")
	}

	a.nid = notifyIconData{}
	a.nid.cbSize = uint32(unsafe.Sizeof(a.nid))
	a.nid.hWnd = a.hWnd
	a.nid.uID = 1
	a.nid.uFlags = nifMessage | nifIcon | nifTip
	a.nid.uCallbackMessage = trayCallbackMsg
	a.nid.hIcon = hIcon
	setUTF16Array(a.nid.szTip[:], "WiCOS64 Remote Storage")

	if a.logger != nil {
		a.logger.Printf("NIM_ADD cbSize=%d flags=0x%X cbMsg=0x%X hWnd=0x%X", a.nid.cbSize, a.nid.uFlags, a.nid.uCallbackMessage, uintptr(a.nid.hWnd))
	}
	if ok := shellNotifyIcon(nimAdd, &a.nid); !ok {
		le := getLastErr()
		if a.logger != nil {
			a.logger.Printf("NIM_ADD failed GetLastError=%d", le)
		}
		return fmt.Errorf("Shell_NotifyIcon(NIM_ADD) failed (err=%d)", le)
	}

	// Request modern tray behavior (NOTIFYICON_VERSION_4) on Windows 10/11.
	a.nid.uVersion = notifyIconVersion4
	if ok := shellNotifyIcon(nimSetVersion, &a.nid); !ok {
		if a.logger != nil {
			a.logger.Printf("NIM_SETVERSION failed GetLastError=%d", getLastErr())
		}
	} else {
		if a.logger != nil {
			a.logger.Printf("NIM_SETVERSION ok (v%d)", notifyIconVersion4)
		}
	}
	if a.logger != nil {
		a.logger.Printf("tray initialized ok")
	}

	return nil
}

func (a *trayApp) setTooltip(text string) error {
	if a.hWnd == 0 {
		return nil
	}
	setUTF16Array(a.nid.szTip[:], text)
	// For NIM_MODIFY, uFlags tells Windows which fields are valid.
	// Be explicit and only update the tooltip to avoid Win11 quirks.
	oldFlags := a.nid.uFlags
	a.nid.uFlags = nifTip
	ok := shellNotifyIcon(nimModify, &a.nid)
	a.nid.uFlags = oldFlags
	if !ok {
		le := getLastErr()
		if a.logger != nil {
			a.logger.Printf("NIM_MODIFY failed (tooltip) GetLastError=%d", le)
		}
		return fmt.Errorf("Shell_NotifyIcon(NIM_MODIFY) failed (err=%d)", le)
	}
	return nil
}

func (a *trayApp) showMenu() {
	_ = a.refreshConfig()
	running := a.isServerRunning()
	a.mu.Lock()
	startedByTray := a.serverCmd != nil
	a.mu.Unlock()

	hMenu, _, _ := procCreatePopupMenu.Call()
	if hMenu == 0 {
		return
	}
	defer procDestroyMenu.Call(hMenu)

	appendMenu := func(flags uint32, id uint32, title string) {
		t, _ := syscall.UTF16PtrFromString(title)
		procAppendMenuW.Call(hMenu, uintptr(flags), uintptr(id), uintptr(unsafe.Pointer(t)))
	}
	sep := func() { procAppendMenuW.Call(hMenu, uintptr(mfSeparator), 0, 0) }

	// Header-ish items.
	appendMenu(mfString|mfDisabled|mfGrayed, 0, "WiCOS64 Remote Storage")
	appendMenu(mfString|mfDisabled|mfGrayed, 0, "Version: "+version.Get().Version)
	sep()

	if running {
		appendMenu(mfString|mfDisabled|mfGrayed, cmdStartServer, "Start server (already running)")
		canStop := startedByTray || a.cfg.EnableAdminUI
		if canStop {
			appendMenu(mfString, cmdStopServer, "Stop server")
			appendMenu(mfString, cmdRestartServer, "Restart server")
		} else {
			appendMenu(mfString|mfDisabled|mfGrayed, cmdStopServer, "Stop server (admin disabled)")
			appendMenu(mfString|mfDisabled|mfGrayed, cmdRestartServer, "Restart server (admin disabled)")
		}
	} else {
		appendMenu(mfString, cmdStartServer, "Start server")
		appendMenu(mfString|mfDisabled|mfGrayed, cmdStopServer, "Stop server")
		appendMenu(mfString|mfDisabled|mfGrayed, cmdRestartServer, "Restart server")
	}

	if running {
		appendMenu(mfString, cmdOpenAdmin, "Open Admin UI")
	} else {
		appendMenu(mfString|mfDisabled|mfGrayed, cmdOpenAdmin, "Open Admin UI")
	}
	sep()
	appendMenu(mfString, cmdReloadConfig, "Reload config (soft)")
	appendMenu(mfString, cmdRunSelfTest, "Run self-test")
	appendMenu(mfString, cmdRunCleanup, "Run .TMP cleanup")
	sep()
	appendMenu(mfString, cmdOpenConfig, "Open config")
	appendMenu(mfString, cmdOpenDataFolder, "Open data folder")
	appendMenu(mfString, cmdViewLog, "View server.log")
	sep()
	appendMenu(mfString, cmdExit, "Exit tray")

	// Cursor position.
	var pt point
	procGetCursorPos.Call(uintptr(unsafe.Pointer(&pt)))
	procSetForegroundWindow.Call(uintptr(a.hWnd))

	// Show menu.
	cmd, _, _ := procTrackPopupMenu.Call(
		hMenu,
		uintptr(tpmRightButton|tpmNonotify|tpmReturnCmd),
		uintptr(pt.x),
		uintptr(pt.y),
		0,
		uintptr(a.hWnd),
		0,
	)
	// Required so the menu closes correctly and the tray keeps receiving clicks.
	procPostMessageW.Call(uintptr(a.hWnd), uintptr(wmNull), 0, 0)
	if cmd == 0 {
		return
	}
	a.handleCommand(int(cmd))
}

func (a *trayApp) handleCommand(id int) {
	switch id {
	case cmdOpenAdmin:
		if err := a.openAdmin(); err != nil {
			showError("WiCOS64 Tray", err.Error())
		}
	case cmdStartServer:
		if err := a.startServer(); err != nil {
			showError("WiCOS64 Tray", err.Error())
		}
	case cmdStopServer:
		if err := a.stopServer(); err != nil {
			showError("WiCOS64 Tray", err.Error())
		}
	case cmdRestartServer:
		if err := a.restartServer(); err != nil {
			showError("WiCOS64 Tray", err.Error())
		}
	case cmdReloadConfig:
		if err := a.adminPost("/admin/api/reload"); err != nil {
			showError("WiCOS64 Tray", err.Error())
		}
	case cmdRunSelfTest:
		if err := a.adminPost("/admin/api/selftest"); err != nil {
			showError("WiCOS64 Tray", err.Error())
		}
	case cmdRunCleanup:
		if err := a.adminPost("/admin/api/cleanup/run"); err != nil {
			showError("WiCOS64 Tray", err.Error())
		}
	case cmdOpenConfig:
		a.mu.Lock()
		p := a.configPath
		a.mu.Unlock()
		_ = a.openFile(p)
	case cmdOpenDataFolder:
		_ = a.refreshConfig()
		a.mu.Lock()
		p := a.dataPath
		a.mu.Unlock()
		_ = a.openFolder(p)
	case cmdViewLog:
		_ = a.openFile(a.logPath)
	case cmdExit:
		// If the server is running, warn the user that exiting the tray will
		// also stop the server.
		running := a.isServerRunning()
		if running {
			if !confirmYesNo("WiCOS64 Tray", "Exit tray?\n\nThe WiCOS64 server will also be stopped.") {
				return
			}
			if err := a.stopServer(); err != nil {
				showError("WiCOS64 Tray", "Failed to stop server:\n"+err.Error())
				return
			}
		}
		a.cleanup()
		postQuit(0)
	}
	a.updateTooltip()
}

func (a *trayApp) cleanup() {
	if a.hWnd != 0 {
		_ = shellNotifyIcon(nimDelete, &a.nid)
		a.hWnd = 0
	}
	if a.logf != nil {
		_ = a.logf.Close()
		a.logf = nil
		a.logger = nil
	}
}

func (a *trayApp) messageLoop() {
	var m msg
	for {
		r, _, _ := procGetMessageW.Call(uintptr(unsafe.Pointer(&m)), 0, 0, 0)
		if int32(r) == -1 {
			break
		}
		if r == 0 {
			break // WM_QUIT
		}
		procTranslateMessage.Call(uintptr(unsafe.Pointer(&m)))
		procDispatchMessageW.Call(uintptr(unsafe.Pointer(&m)))
	}
}

func wndProc(hWnd uintptr, msg uint32, wParam, lParam uintptr) uintptr {
	// Ensure cleanup on close.
	switch msg {
	case trayCallbackMsg:
		// Important: on modern Windows (NOTIFYICON_VERSION_4) the message code
		// can be stored in the LOWORD of lParam, with additional flags/data in
		// the high word. We must mask to LOWORD, otherwise we won't match WM_*.
		evt := uint32(lParam) & 0xFFFF

		// Log only the relevant events (avoid spamming tray.log with WM_MOUSEMOVE).
		if gApp != nil && gApp.logger != nil {
			if evt != 0x0200 { // WM_MOUSEMOVE
				gApp.logger.Printf("event cbMsg=0x%X wParam=0x%X lParam=0x%X evt=0x%X", msg, wParam, lParam, evt)
			}
		}

		switch evt {
		case wmRButtonDown, wmRButtonUp, wmContextMenu:
			if gApp != nil {
				gApp.showMenu()
			}
		case wmLButtonUp:
			// Win11 sometimes prefers left-click for tray interactions.
			// Showing the menu on left-click gives a robust fallback.
			if gApp != nil {
				gApp.showMenu()
			}
		case wmLButtonDblClk:
			if gApp != nil {
				_ = gApp.openAdmin()
			}
		case wmUser + 1: // NIN_KEYSELECT (keyboard selection)
			if gApp != nil {
				gApp.showMenu()
			}
		}
		return 0
	case wmCommand:
		// In case we ever use non-TPM_RETURNCMD.
		if gApp != nil {
			id := int(wParam & 0xFFFF)
			gApp.handleCommand(id)
		}
		return 0
	case wmClose:
		if gApp != nil {
			gApp.cleanup()
		}
		postQuit(0)
		return 0
	case wmDestroy:
		if gApp != nil {
			gApp.cleanup()
		}
		postQuit(0)
		return 0
	}
	r, _, _ := procDefWindowProcW.Call(hWnd, uintptr(msg), wParam, lParam)
	return r
}

func shellNotifyIcon(message uint32, nid *notifyIconData) bool {
	r, _, _ := procShellNotifyIconW.Call(uintptr(message), uintptr(unsafe.Pointer(nid)))
	return r != 0
}

func getLastErr() uint32 {
	r, _, _ := procGetLastError.Call()
	return uint32(r)
}

func shellOpen(target string) error {
	target = strings.TrimSpace(target)
	if target == "" {
		return errors.New("empty target")
	}
	op, _ := syscall.UTF16PtrFromString("open")
	file, _ := syscall.UTF16PtrFromString(target)
	r, _, _ := procShellExecuteW.Call(0, uintptr(unsafe.Pointer(op)), uintptr(unsafe.Pointer(file)), 0, 0, swShownormal)
	if r <= 32 {
		return fmt.Errorf("ShellExecute failed (code=%d)", r)
	}
	return nil
}

func setUTF16Array(dst []uint16, s string) {
	u := syscall.StringToUTF16(s)
	// Copy up to len(dst)-1 and ensure NUL termination.
	n := len(dst)
	if n == 0 {
		return
	}
	for i := 0; i < n; i++ {
		dst[i] = 0
	}
	for i := 0; i < n-1 && i < len(u)-1; i++ {
		dst[i] = u[i]
	}
}

func getModuleHandle() syscall.Handle {
	r, _, _ := procGetModuleHandleW.Call(0)
	return syscall.Handle(r)
}

func postQuit(code int32) {
	procPostQuitMessage.Call(uintptr(code))
}

func showError(title, text string) {
	t, _ := syscall.UTF16PtrFromString(title)
	m, _ := syscall.UTF16PtrFromString(text)
	// MB_OK|MB_ICONERROR
	procMessageBoxW.Call(0, uintptr(unsafe.Pointer(m)), uintptr(unsafe.Pointer(t)), 0x00000000|0x00000010)
}

func showInfo(title, text string) {
	t, _ := syscall.UTF16PtrFromString(title)
	m, _ := syscall.UTF16PtrFromString(text)
	// MB_OK|MB_ICONINFORMATION
	procMessageBoxW.Call(0, uintptr(unsafe.Pointer(m)), uintptr(unsafe.Pointer(t)), 0x00000000|0x00000040)
}

func confirmYesNo(title, text string) bool {
	t, _ := syscall.UTF16PtrFromString(title)
	m, _ := syscall.UTF16PtrFromString(text)
	// MB_YESNO|MB_ICONQUESTION
	r, _, _ := procMessageBoxW.Call(0, uintptr(unsafe.Pointer(m)), uintptr(unsafe.Pointer(t)), mbYesNo|mbIconQuestion)
	return int32(r) == idYes
}
