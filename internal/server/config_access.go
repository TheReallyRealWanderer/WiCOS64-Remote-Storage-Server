package server

import "wicos64-server/internal/config"

// getCfg is a small alias used by background maintenance code.
func (s *Server) getCfg() config.Config {
	return s.cfgSnapshot()
}
