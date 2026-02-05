package server

import (
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"wicos64-server/internal/config"
	"wicos64-server/internal/pathutil"
	"wicos64-server/internal/proto"
)

const (
	previewMaxBytes   = 64
	previewMaxChars   = 1600
	previewLineBytes  = 16
	previewMaxEntries = 6
)

func buildReqPreview(cfg config.Config, op byte, flags byte, payload []byte) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("<req preview panic: %v>", r)
		}
	}()

	readPath := func(d *proto.Decoder) string {
		p, err := d.ReadString(cfg.MaxPath)
		if err != nil {
			return ""
		}
		if np, err := pathutil.Normalize(p, cfg.MaxPath, cfg.MaxName); err == nil {
			p = np
		}
		p = pathutil.Canonicalize(p)
		p = strings.ReplaceAll(p, "\n", "")
		p = strings.ReplaceAll(p, "\r", "")
		return p
	}

	d := proto.NewDecoder(payload)
	switch op {
	case proto.OpCAPS, proto.OpPING:
		if len(payload) == 0 {
			return "(empty)"
		}
		return fmt.Sprintf("unexpected payload len=%d\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
	case proto.OpSTAT, proto.OpRM:
		p := readPath(d)
		return fmt.Sprintf("path=%s", p)
	case proto.OpSTATFS:
		p := "/"
		if d.Remaining() > 0 {
			p = readPath(d)
			if p == "" {
				p = "/"
			}
		}
		return fmt.Sprintf("path=%s", p)
	case proto.OpLS:
		p := readPath(d)
		start, _ := d.ReadU16()
		max, _ := d.ReadU16()
		return fmt.Sprintf("path=%s\nstart_index=%d max_entries=%d", p, start, max)
	case proto.OpREAD_RANGE:
		p := readPath(d)
		off, _ := d.ReadU32()
		ln, _ := d.ReadU16()
		return fmt.Sprintf("path=%s\noffset=%d len=%d", p, off, ln)
	case proto.OpWRITE_RANGE:
		p := readPath(d)
		off, _ := d.ReadU32()
		ln, _ := d.ReadU16()
		data, _ := d.ReadBytes(int(minU16(ln, cfg.MaxChunk)))
		fl := []string{}
		if flags&proto.FlagWR_TRUNCATE != 0 {
			fl = append(fl, "TRUNCATE")
		}
		if flags&proto.FlagWR_CREATE != 0 {
			fl = append(fl, "CREATE")
		}
		fs := ""
		if len(fl) > 0 {
			fs = " flags=" + strings.Join(fl, "|")
		}
		text := fmt.Sprintf("path=%s\noffset=%d data_len=%d%s", p, off, ln, fs)
		if len(data) > 0 {
			text += "\n\nDATA (preview)\n" + dumpBytes(data, previewMaxBytes)
		}
		return text
	case proto.OpAPPEND:
		p := readPath(d)
		ln, _ := d.ReadU16()
		data, _ := d.ReadBytes(int(minU16(ln, cfg.MaxChunk)))
		fl := ""
		if flags&proto.FlagAP_CREATE != 0 {
			fl = " flags=CREATE"
		}
		text := fmt.Sprintf("path=%s\ndata_len=%d%s", p, ln, fl)
		if len(data) > 0 {
			text += "\n\nDATA (preview)\n" + dumpBytes(data, previewMaxBytes)
		}
		return text
	case proto.OpMKDIR:
		p := readPath(d)
		fl := ""
		if flags&proto.FlagMK_PARENTS != 0 {
			fl = " flags=PARENTS"
		}
		return fmt.Sprintf("path=%s%s", p, fl)
	case proto.OpRMDIR:
		p := readPath(d)
		fl := ""
		if flags&proto.FlagRD_RECURSIVE != 0 {
			fl = " flags=RECURSIVE"
		}
		return fmt.Sprintf("path=%s%s", p, fl)
	case proto.OpCP:
		src := readPath(d)
		dst := readPath(d)
		fl := []string{}
		if flags&proto.FlagCP_OVERWRITE != 0 {
			fl = append(fl, "OVERWRITE")
		}
		if flags&proto.FlagCP_RECURSIVE != 0 {
			fl = append(fl, "RECURSIVE")
		}
		fs := ""
		if len(fl) > 0 {
			fs = " flags=" + strings.Join(fl, "|")
		}
		return fmt.Sprintf("src=%s\ndst=%s%s", src, dst, fs)
	case proto.OpMV:
		src := readPath(d)
		dst := readPath(d)
		fl := ""
		if flags&proto.FlagMV_OVERWRITE != 0 {
			fl = " flags=OVERWRITE"
		}
		return fmt.Sprintf("src=%s\ndst=%s%s", src, dst, fl)
	case proto.OpHASH:
		p := readPath(d)
		algo := "CRC32"
		if flags&proto.FlagH_ALGO != 0 {
			algo = "SHA1"
		}
		return fmt.Sprintf("path=%s\nalgo=%s", p, algo)
	case proto.OpSEARCH:
		base := readPath(d)
		q, _ := d.ReadString(cfg.MaxPath)
		start, _ := d.ReadU16()
		max, _ := d.ReadU16()
		maxScan, _ := d.ReadU32()
		fl := []string{}
		if flags&proto.FlagS_CASE_INSENSITIVE != 0 {
			fl = append(fl, "CI")
		}
		if flags&proto.FlagS_RECURSIVE != 0 {
			fl = append(fl, "RECURSIVE")
		}
		if flags&proto.FlagS_WHOLE_WORD != 0 {
			fl = append(fl, "WHOLE_WORD")
		}
		fs := ""
		if len(fl) > 0 {
			fs = " flags=" + strings.Join(fl, "|")
		}
		return fmt.Sprintf("base=%s\nquery=%q\nstart_index=%d max_results=%d max_scan_bytes=%d%s", base, trunc(q, 80), start, max, maxScan, fs)
	default:
		if len(payload) == 0 {
			return "(empty)"
		}
		return fmt.Sprintf("len=%d\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
	}
}

func buildRespPreview(cfg config.Config, op byte, status byte, payload []byte, errMsg string) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("<resp preview panic: %v>", r)
		}
	}()

	if status != proto.StatusOK {
		msg := errMsg
		if msg == "" {
			msg = statusName(status)
		}
		return fmt.Sprintf("%s\n%s", statusName(status), msg)
	}
	if len(payload) == 0 {
		return "OK"
	}

	d := proto.NewDecoder(payload)
	switch op {
	case proto.OpCAPS:
		maxChunk, _ := d.ReadU16()
		maxPayload, _ := d.ReadU16()
		maxPath, _ := d.ReadU16()
		maxName, _ := d.ReadU16()
		maxEntries, _ := d.ReadU16()
		features, _ := d.ReadU32()
		serverTime, _ := d.ReadU32()
		sname, _ := d.ReadString(cfg.MaxName)

		ft := time.Unix(int64(serverTime), 0).UTC().Format(time.RFC3339)
		return fmt.Sprintf(
			"CAPS\nmax_chunk=%d\nmax_payload=%d\nmax_path=%d\nmax_name=%d\nmax_entries=%d\nfeatures=0x%08X\nserver_time_utc=%s\nserver_name=%q",
			maxChunk, maxPayload, maxPath, maxName, maxEntries, features, ft, sname,
		)
	case proto.OpSTATFS:
		if len(payload) < 12 {
			return fmt.Sprintf("STATFS payload too short (%d)\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
		}
		total, _ := d.ReadU32()
		free, _ := d.ReadU32()
		used, _ := d.ReadU32()
		return fmt.Sprintf("STATFS\ntotal=%s\nfree=%s\nused=%s", humanBytes(uint64(total)), humanBytes(uint64(free)), humanBytes(uint64(used)))
	case proto.OpSTAT:
		if len(payload) < 9 {
			return fmt.Sprintf("STAT payload too short (%d)\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
		}
		t, _ := d.ReadU8()
		sz, _ := d.ReadU32()
		mt, _ := d.ReadU32()
		typ := "FILE"
		if t != 0 {
			typ = "DIR"
		}
		mtime := time.Unix(int64(mt), 0).UTC().Format(time.RFC3339)
		return fmt.Sprintf("STAT\ntype=%s\nsize=%s (%d)\nmtime_utc=%s", typ, humanBytes(uint64(sz)), sz, mtime)
	case proto.OpLS:
		// count u16, entries..., next_index u16
		if len(payload) < 4 {
			return fmt.Sprintf("LS payload too short (%d)\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
		}
		count, _ := d.ReadU16()
		lines := []string{fmt.Sprintf("LS\ncount=%d", count)}
		shown := 0
		for i := 0; i < int(count) && shown < previewMaxEntries && d.Remaining() > 2; i++ {
			et, _ := d.ReadU8()
			sz, _ := d.ReadU32()
			mt, _ := d.ReadU32()
			name, _ := d.ReadString(cfg.MaxName)
			suffix := ""
			if et != 0 {
				suffix = "/"
			}
			_ = sz
			_ = mt
			lines = append(lines, fmt.Sprintf("- %s%s", name, suffix))
			shown++
		}
		// next index is last 2 bytes
		if len(payload) >= 2 {
			next := binary.LittleEndian.Uint16(payload[len(payload)-2:])
			lines = append(lines, fmt.Sprintf("next_index=%d", next))
		}
		if int(count) > shown {
			lines = append(lines, fmt.Sprintf("(+%d more)", int(count)-shown))
		}
		return strings.Join(lines, "\n")
	case proto.OpHASH:
		if len(payload) != 4 {
			return fmt.Sprintf("HASH payload len=%d\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
		}
		sum := binary.LittleEndian.Uint32(payload)
		return fmt.Sprintf("HASH\ncrc32=0x%08X (%d)", sum, sum)
	case proto.OpREAD_RANGE:
		return fmt.Sprintf("READ_RANGE\nbytes=%d\n\nDATA (preview)\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
	case proto.OpSEARCH:
		if len(payload) < 4 {
			return fmt.Sprintf("SEARCH payload too short (%d)\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
		}
		dd := proto.NewDecoder(payload)
		count, _ := dd.ReadU16()
		lines := []string{fmt.Sprintf("SEARCH\ncount=%d", count)}
		shown := 0
		for i := 0; i < int(count) && shown < previewMaxEntries && dd.Remaining() > 2; i++ {
			p, _ := dd.ReadString(cfg.MaxPath)
			off, _ := dd.ReadU32()
			pln, _ := dd.ReadU16()
			pv, _ := dd.ReadBytes(int(pln))
			lines = append(lines, fmt.Sprintf("- %s @%d: %q", p, off, trunc(asciiSanitize(string(pv)), 40)))
			shown++
		}
		if len(payload) >= 2 {
			next := binary.LittleEndian.Uint16(payload[len(payload)-2:])
			lines = append(lines, fmt.Sprintf("next_index=%d", next))
		}
		if int(count) > shown {
			lines = append(lines, fmt.Sprintf("(+%d more)", int(count)-shown))
		}
		return strings.Join(lines, "\n")
	default:
		// Unknown/other: show compact dump.
		return fmt.Sprintf("len=%d\n%s", len(payload), dumpBytes(payload, previewMaxBytes))
	}
}

func minU16(a, b uint16) uint16 {
	if a < b {
		return a
	}
	return b
}

func dumpBytes(b []byte, max int) string {
	if max <= 0 {
		max = previewMaxBytes
	}
	if len(b) > max {
		b = b[:max]
	}
	var sb strings.Builder
	for i := 0; i < len(b); i += previewLineBytes {
		end := i + previewLineBytes
		if end > len(b) {
			end = len(b)
		}
		chunk := b[i:end]
		sb.WriteString(fmt.Sprintf("%04X: ", i))
		for j := 0; j < previewLineBytes; j++ {
			if i+j < end {
				sb.WriteString(fmt.Sprintf("%02X ", chunk[j]))
			} else {
				sb.WriteString("   ")
			}
		}
		sb.WriteString("|")
		for _, c := range chunk {
			if c >= 0x20 && c <= 0x7E {
				sb.WriteByte(c)
			} else {
				sb.WriteByte('.')
			}
		}
		sb.WriteString("|\n")
	}
	out := sb.String()
	if len(out) > previewMaxChars {
		out = out[:previewMaxChars] + "…"
	}
	return out
}

func humanBytes(b uint64) string {
	const (
		KiB = 1024
		MiB = 1024 * KiB
		GiB = 1024 * MiB
	)
	if b >= GiB {
		return fmt.Sprintf("%.2f GiB", float64(b)/GiB)
	}
	if b >= MiB {
		return fmt.Sprintf("%.2f MiB", float64(b)/MiB)
	}
	if b >= KiB {
		return fmt.Sprintf("%.2f KiB", float64(b)/KiB)
	}
	return fmt.Sprintf("%d B", b)
}

func asciiSanitize(s string) string {
	// Keep printable ASCII only.
	var sb strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x20 && c <= 0x7E {
			sb.WriteByte(c)
		} else {
			sb.WriteByte('.')
		}
	}
	return sb.String()
}
