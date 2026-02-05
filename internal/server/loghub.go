package server

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
)

// LogEntry is a compact per-request record for debugging and the admin UI.
//
// IMPORTANT: Never include the raw token in here (spec recommends not logging it).
// The admin UI may show masked/derived token identifiers elsewhere.
type LogEntry struct {
	ID         uint64 `json:"id"`
	TimeUnixMs int64  `json:"time_unix_ms"`
	RemoteIP   string `json:"remote_ip"`
	Op         byte   `json:"op"`
	OpName     string `json:"op_name"`
	Status     byte   `json:"status"`
	StatusName string `json:"status_name"`
	ReqBytes   int    `json:"req_bytes"`
	RespBytes  int    `json:"resp_bytes"`
	DurationMs int64  `json:"duration_ms"`
	Info       string `json:"info,omitempty"`
	HTTPStatus int    `json:"http_status"`

	// Human readable previews for the admin UI (best-effort, capped).
	ReqPreview  string `json:"req_preview,omitempty"`
	RespPreview string `json:"resp_preview,omitempty"`
}

// logHub keeps a ring buffer of recent logs and allows streaming them via SSE.
type logHub struct {
	mu      sync.Mutex
	ring    []LogEntry
	cap     int
	nextPos int
	count   int
	nextID  uint64
	subs    map[chan LogEntry]struct{}
}

func newLogHub(capacity int) *logHub {
	if capacity <= 0 {
		capacity = 512
	}
	return &logHub{
		ring: make([]LogEntry, capacity),
		cap:  capacity,
		subs: make(map[chan LogEntry]struct{}),
	}
}

func (h *logHub) add(e LogEntry) {
	if e.TimeUnixMs == 0 {
		e.TimeUnixMs = time.Now().UnixMilli()
	}

	h.mu.Lock()
	h.nextID++
	e.ID = h.nextID

	// Ring insert.
	h.ring[h.nextPos] = e
	h.nextPos = (h.nextPos + 1) % h.cap
	if h.count < h.cap {
		h.count++
	}
	// Broadcast (best-effort, non-blocking).
	for ch := range h.subs {
		select {
		case ch <- e:
		default:
			// Drop if subscriber is too slow.
		}
	}
	h.mu.Unlock()
}

func (h *logHub) snapshot(limit int) []LogEntry {
	h.mu.Lock()
	defer h.mu.Unlock()

	if limit <= 0 || limit > h.count {
		limit = h.count
	}
	if limit == 0 {
		return nil
	}

	// Oldest index.
	start := h.nextPos - h.count
	if start < 0 {
		start += h.cap
	}
	// We want only the last `limit`.
	start = (start + (h.count - limit)) % h.cap

	out := make([]LogEntry, 0, limit)
	for i := 0; i < limit; i++ {
		idx := (start + i) % h.cap
		out = append(out, h.ring[idx])
	}
	return out
}

func (h *logHub) clear() {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Keep capacity, just reset counters.
	h.nextPos = 0
	h.count = 0
	h.nextID = 0
	for i := range h.ring {
		h.ring[i] = LogEntry{}
	}
}

type LogFilter struct {
	Op           *byte
	OnlyErrors   bool
	RemoteIPSub  string
	InfoContains string
	SinceUnixMs  int64
	UntilUnixMs  int64
	Limit        int
}

func (h *logHub) filteredSnapshot(f LogFilter) []LogEntry {
	// Snapshot all and filter.
	all := h.snapshot(0)
	if len(all) == 0 {
		return nil
	}
	limit := f.Limit
	if limit <= 0 || limit > len(all) {
		limit = len(all)
	}

	// We want the most recent matching entries. Walk from newest -> oldest,
	// collect up to limit, then reverse to keep chronological order.
	out := make([]LogEntry, 0, limit)
	for i := len(all) - 1; i >= 0; i-- {
		e := all[i]
		if f.Op != nil && e.Op != *f.Op {
			continue
		}
		if f.OnlyErrors && e.Status == 0 {
			continue
		}
		if f.RemoteIPSub != "" && e.RemoteIP != "" {
			if !containsFold(e.RemoteIP, f.RemoteIPSub) {
				continue
			}
		}
		if f.InfoContains != "" {
			// Search across multiple fields to make filtering useful.
			hay := e.Info
			if e.ReqPreview != "" {
				hay += "\n" + e.ReqPreview
			}
			if e.RespPreview != "" {
				hay += "\n" + e.RespPreview
			}
			if !containsFold(hay, f.InfoContains) {
				continue
			}
		}
		if f.SinceUnixMs > 0 && e.TimeUnixMs < f.SinceUnixMs {
			continue
		}
		if f.UntilUnixMs > 0 && e.TimeUnixMs > f.UntilUnixMs {
			continue
		}

		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}

	// Reverse in place.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func containsFold(hay, needle string) bool {
	if needle == "" {
		return true
	}
	return strings.Contains(strings.ToLower(hay), strings.ToLower(needle))
}

func (h *logHub) subscribe() (ch chan LogEntry, cancel func()) {
	ch = make(chan LogEntry, 32)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		delete(h.subs, ch)
		h.mu.Unlock()
		close(ch)
	}
}

func (e LogEntry) jsonLine() []byte {
	b, _ := json.Marshal(e)
	return b
}
