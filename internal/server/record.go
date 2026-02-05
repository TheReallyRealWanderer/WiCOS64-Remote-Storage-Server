package server

import "wicos64-server/internal/config"

func (s *Server) record(cfg config.Config, le LogEntry) {
	if cfg.LogRequests {
		s.logs.add(le)
	}
	if s.stats != nil {
		s.stats.add(le.Op, le.Status, le.ReqBytes, le.RespBytes, le.DurationMs)
	}
}
