package server

import (
	"sync"
	"time"
)

// StatsPoint is an aggregated per-minute counter used for dashboards.
type StatsPoint struct {
	MinuteUnix int64  `json:"minute_unix"`
	Requests   uint64 `json:"requests"`
	Errors     uint64 `json:"errors"`
	BytesIn    uint64 `json:"bytes_in"`
	BytesOut   uint64 `json:"bytes_out"`
}

// StatsSnapshot is a JSON-friendly snapshot of collected stats.
type StatsSnapshot struct {
	StartedUnix int64             `json:"started_unix"`
	NowUnix     int64             `json:"now_unix"`
	UptimeSec   int64             `json:"uptime_sec"`
	TotalReq    uint64            `json:"total_requests"`
	TotalErr    uint64            `json:"total_errors"`
	BytesIn     uint64            `json:"bytes_in"`
	BytesOut    uint64            `json:"bytes_out"`
	AvgMs       uint64            `json:"avg_ms"`
	ByOp        map[string]uint64 `json:"by_op"`
	Recent      []StatsPoint      `json:"recent"`
}

// statsHub keeps lightweight counters for an admin dashboard.
//
// It is intentionally simple and dependency-free.
type statsHub struct {
	mu sync.Mutex

	started time.Time

	// totals
	totalReq   uint64
	totalErr   uint64
	bytesIn    uint64
	bytesOut   uint64
	totalDurMs uint64

	byOp [256]uint64

	// per-minute ring (last 60 minutes)
	curMin  int64
	idx     int
	minUnix [60]int64
	req     [60]uint64
	err     [60]uint64
	in      [60]uint64
	out     [60]uint64
}

func newStatsHub() *statsHub {
	now := time.Now()
	m := now.Unix() / 60
	h := &statsHub{started: now, curMin: m, idx: 0}
	h.minUnix[0] = m * 60
	return h
}

func (h *statsHub) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	m := now.Unix() / 60

	// Re-init in place to avoid changing any references.
	h.started = now
	h.totalReq = 0
	h.totalErr = 0
	h.bytesIn = 0
	h.bytesOut = 0
	h.totalDurMs = 0
	h.byOp = [256]uint64{}

	h.curMin = m
	h.idx = 0
	h.minUnix = [60]int64{}
	h.req = [60]uint64{}
	h.err = [60]uint64{}
	h.in = [60]uint64{}
	h.out = [60]uint64{}
	h.minUnix[0] = m * 60
}

func (h *statsHub) advanceLocked(targetMin int64) {
	if targetMin <= h.curMin {
		return
	}
	for h.curMin < targetMin {
		h.curMin++
		h.idx = (h.idx + 1) % len(h.req)
		h.minUnix[h.idx] = h.curMin * 60
		h.req[h.idx] = 0
		h.err[h.idx] = 0
		h.in[h.idx] = 0
		h.out[h.idx] = 0
	}
}

func (h *statsHub) add(op byte, status byte, reqBytes, respBytes int, durMs int64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	nowMin := time.Now().Unix() / 60
	h.advanceLocked(nowMin)

	h.totalReq++
	h.byOp[op]++
	h.req[h.idx]++

	if status != 0 {
		h.totalErr++
		h.err[h.idx]++
	}
	if reqBytes > 0 {
		h.bytesIn += uint64(reqBytes)
		h.in[h.idx] += uint64(reqBytes)
	}
	if respBytes > 0 {
		h.bytesOut += uint64(respBytes)
		h.out[h.idx] += uint64(respBytes)
	}
	if durMs > 0 {
		h.totalDurMs += uint64(durMs)
	}
}

func (h *statsHub) snapshot() StatsSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	nowMin := now.Unix() / 60
	h.advanceLocked(nowMin)

	by := make(map[string]uint64)
	for i, c := range h.byOp {
		if c == 0 {
			continue
		}
		by[opName(byte(i))] = c
	}

	// Oldest -> newest.
	recent := make([]StatsPoint, 0, len(h.req))
	n := len(h.req)
	for i := 0; i < n; i++ {
		j := (h.idx + 1 + i) % n
		if h.minUnix[j] == 0 {
			continue
		}
		recent = append(recent, StatsPoint{
			MinuteUnix: h.minUnix[j],
			Requests:   h.req[j],
			Errors:     h.err[j],
			BytesIn:    h.in[j],
			BytesOut:   h.out[j],
		})
	}

	avg := uint64(0)
	if h.totalReq > 0 {
		avg = h.totalDurMs / h.totalReq
	}

	return StatsSnapshot{
		StartedUnix: h.started.Unix(),
		NowUnix:     now.Unix(),
		UptimeSec:   int64(now.Sub(h.started).Seconds()),
		TotalReq:    h.totalReq,
		TotalErr:    h.totalErr,
		BytesIn:     h.bytesIn,
		BytesOut:    h.bytesOut,
		AvgMs:       avg,
		ByOp:        by,
		Recent:      recent,
	}
}
