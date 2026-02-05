package version

import (
	"fmt"
	"runtime"
)

// Build-time variables (override via -ldflags -X ...).
// Example:
//
//	go build -ldflags "-X wicos64-server/internal/version.Version=0.1.4.2 -X wicos64-server/internal/version.Commit=abcd123 -X wicos64-server/internal/version.BuildDate=2026-01-10"
var (
	Version   = "v1.0.1.33"
	Commit    = ""
	BuildDate = ""
)

type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit,omitempty"`
	BuildDate string `json:"build_date,omitempty"`
	GoVersion string `json:"go_version"`
}

func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		GoVersion: runtime.Version(),
	}
}

func (i Info) String() string {
	// Keep this stable for CLI output.
	s := i.Version
	if s == "" {
		s = "dev"
	}
	if i.Commit != "" {
		s += fmt.Sprintf(" (%s)", i.Commit)
	}
	if i.BuildDate != "" {
		s += fmt.Sprintf(" built %s", i.BuildDate)
	}
	s += fmt.Sprintf(" [%s]", i.GoVersion)
	return s
}
