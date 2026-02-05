package server

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"wicos64-server/internal/config"
	"wicos64-server/internal/version"
)

type adminOKResponse struct {
	OK       bool     `json:"ok"`
	Message  string   `json:"message,omitempty"`
	Build    string   `json:"build"`
	TSUnix   int64    `json:"ts_unix"`
	Payload  any      `json:"payload,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func (s *Server) handleAdminStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.stats == nil {
		writeJSON(w, http.StatusOK, StatsSnapshot{StartedUnix: time.Now().Unix(), NowUnix: time.Now().Unix()})
		return
	}
	writeJSON(w, http.StatusOK, s.stats.snapshot())
}

func (s *Server) handleAdminStatsReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.stats != nil {
		s.stats.reset()
	}
	writeJSON(w, http.StatusOK, adminOKResponse{OK: true, Build: version.Get().String(), TSUnix: time.Now().Unix(), Message: "stats reset"})
}

func (s *Server) handleAdminReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.cfgPath == "" {
		writeJSON(w, http.StatusBadRequest, adminOKResponse{OK: false, Build: version.Get().String(), TSUnix: time.Now().Unix(), Message: "no config file path (start server with -config)"})
		return
	}

	newCfg, err := config.Load(s.cfgPath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, adminOKResponse{OK: false, Build: version.Get().String(), TSUnix: time.Now().Unix(), Message: err.Error()})
		return
	}
	// Keep network settings stable during a soft reload.
	cur := s.cfgSnapshot()
	newCfg.Listen = cur.Listen
	newCfg.Endpoint = cur.Endpoint

	s.setCfg(newCfg)

	writeJSON(w, http.StatusOK, adminOKResponse{OK: true, Build: version.Get().String(), TSUnix: time.Now().Unix(), Message: "config reloaded", Payload: newCfg, Warnings: configWarnings(newCfg)})
}

// handleAdminShutdown terminates the server process.
//
// This is primarily used by the Windows tray controller to stop the server
// even if it was started outside of the tray.
//
// Note: We intentionally do not implement a "hard" restart here. The admin UI
// already offers a "soft restart" via config reload, and the tray can start
// the process again after it has terminated.
func (s *Server) handleAdminShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, adminOKResponse{OK: true, Build: version.Get().String(), TSUnix: time.Now().Unix(), Message: "shutting down"})
	go func() {
		// Give the HTTP response a moment to be sent.
		time.Sleep(200 * time.Millisecond)
		os.Exit(0)
	}()
}

func (s *Server) handleAdminCleanupRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	cfg := s.cfgSnapshot()
	start := time.Now()

	// TMP cleanup
	tmpStart := time.Now()
	tmpReps := s.runTmpCleanupOnce(cfg)
	tmpDur := time.Since(tmpStart).Milliseconds()
	var tmpFiles, tmpDirs int
	var tmpBytes uint64
	for _, rep := range tmpReps {
		tmpFiles += rep.DeletedFiles
		tmpDirs += rep.DeletedDirs
		tmpBytes += rep.FreedBytes
	}

	// Trash cleanup
	trashStart := time.Now()
	trashReps := s.runTrashCleanupOnce(cfg)
	trashDur := time.Since(trashStart).Milliseconds()
	var trashFiles, trashDirs int
	var trashBytes uint64
	for _, rep := range trashReps {
		trashFiles += rep.DeletedFiles
		trashDirs += rep.DeletedDirs
		trashBytes += rep.FreedBytes
	}

	payload := map[string]any{
		// Backwards compatible keys for existing consumers:
		"reports": tmpReps,
		"summary": map[string]any{
			"roots":         len(tmpReps),
			"deleted_files": tmpFiles,
			"deleted_dirs":  tmpDirs,
			"freed_bytes":   tmpBytes,
			"duration_ms":   tmpDur,
		},
		// New: trash cleanup results
		"trash_reports": trashReps,
		"trash_summary": map[string]any{
			"enabled":         cfg.TrashEnabled,
			"cleanup_enabled": cfg.TrashCleanupEnabled,
			"roots":           len(trashReps),
			"deleted_files":   trashFiles,
			"deleted_dirs":    trashDirs,
			"freed_bytes":     trashBytes,
			"duration_ms":     trashDur,
		},
		"combined_summary": map[string]any{
			"duration_ms": time.Since(start).Milliseconds(),
		},
	}
	writeJSON(w, http.StatusOK, adminOKResponse{OK: true, Build: version.Get().String(), TSUnix: time.Now().Unix(), Message: "cleanup done", Payload: payload})
}

func (s *Server) handleAdminSelfTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	cfg := s.cfgSnapshot()
	start := time.Now()
	reps := s.runSelfTestOnce(cfg)
	ok := 0
	for _, rep := range reps {
		if rep.OK {
			ok++
		}
	}
	payload := map[string]any{
		"reports": reps,
		"summary": map[string]any{
			"roots":       len(reps),
			"ok":          ok,
			"failed":      len(reps) - ok,
			"duration_ms": time.Since(start).Milliseconds(),
		},
	}
	writeJSON(w, http.StatusOK, adminOKResponse{OK: true, Build: version.Get().String(), TSUnix: time.Now().Unix(), Message: "selftest done", Payload: payload})
}
