package server

import (
	"fmt"
	"strings"

	"wicos64-server/internal/config"
	"wicos64-server/internal/pathutil"
	"wicos64-server/internal/proto"
)

func opName(op byte) string {
	switch op {
	case proto.OpCAPS:
		return "CAPS"
	case proto.OpSTATFS:
		return "STATFS"
	case proto.OpLS:
		return "LS"
	case proto.OpSTAT:
		return "STAT"
	case proto.OpREAD_RANGE:
		return "READ_RANGE"
	case proto.OpWRITE_RANGE:
		return "WRITE_RANGE"
	case proto.OpAPPEND:
		return "APPEND"
	case proto.OpMKDIR:
		return "MKDIR"
	case proto.OpRMDIR:
		return "RMDIR"
	case proto.OpRM:
		return "RM"
	case proto.OpCP:
		return "CP"
	case proto.OpSEARCH:
		return "SEARCH"
	case proto.OpHASH:
		return "HASH"
	case proto.OpMV:
		return "MV"
	case proto.OpPING:
		return "PING"
	default:
		if op == 0xFF {
			return "<none>"
		}
		return fmt.Sprintf("OP_0x%02X", op)
	}
}

func statusName(st byte) string {
	switch st {
	case proto.StatusOK:
		return "OK"
	case proto.StatusInternal:
		return "INTERNAL"
	case proto.StatusBadRequest:
		return "BAD_REQUEST"
	case proto.StatusNotSupported:
		return "NOT_SUPPORTED"
	case proto.StatusAccessDenied:
		return "ACCESS_DENIED"
	case proto.StatusNotFound:
		return "NOT_FOUND"
	case proto.StatusNotADir:
		return "NOT_A_DIR"
	case proto.StatusIsADir:
		return "IS_A_DIR"
	case proto.StatusAlreadyExists:
		return "ALREADY_EXISTS"
	case proto.StatusDirNotEmpty:
		return "DIR_NOT_EMPTY"
	case proto.StatusRangeInvalid:
		return "RANGE_INVALID"
	case proto.StatusInvalidPath:
		return "INVALID_PATH"
	case proto.StatusTooLarge:
		return "TOO_LARGE"
	case proto.StatusBusy:
		return "BUSY"
	default:
		return fmt.Sprintf("STATUS_0x%02X", st)
	}
}

func summarizeRequest(cfg config.Config, op byte, flags byte, payload []byte) string {
	// Best-effort parsing of common fields for debugging.
	// This must never return an error.
	readPath := func(d *proto.Decoder) string {
		p, err := d.ReadString(cfg.MaxPath)
		if err != nil {
			return ""
		}
		// Normalize/canonicalize for readability; ignore errors.
		if np, err := pathutil.Normalize(p, cfg.MaxPath, cfg.MaxName); err == nil {
			p = np
		}
		p = pathutil.Canonicalize(p)
		p = strings.ReplaceAll(p, "\n", "")
		p = strings.ReplaceAll(p, "\r", "")
		return trunc(p, 96)
	}

	flagList := func(parts ...string) string {
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if p != "" {
				out = append(out, p)
			}
		}
		if len(out) == 0 {
			return ""
		}
		return strings.Join(out, "|")
	}

	d := proto.NewDecoder(payload)
	switch op {
	case proto.OpLS:
		p := readPath(d)
		start, _ := d.ReadU16()
		max, _ := d.ReadU16()
		return fmt.Sprintf("path=%s start=%d max=%d", p, start, max)
	case proto.OpSTAT:
		p := readPath(d)
		return fmt.Sprintf("path=%s", p)
	case proto.OpSTATFS:
		if len(payload) == 0 {
			return "path=/"
		}
		p := readPath(d)
		return fmt.Sprintf("path=%s", p)
	case proto.OpREAD_RANGE:
		p := readPath(d)
		off, _ := d.ReadU32()
		ln, _ := d.ReadU16()
		return fmt.Sprintf("path=%s off=%d len=%d", p, off, ln)
	case proto.OpWRITE_RANGE:
		p := readPath(d)
		off, _ := d.ReadU32()
		ln, _ := d.ReadU16()
		fl := flagList(
			choose(flags&proto.FlagWR_TRUNCATE != 0, "TRUNC", ""),
			choose(flags&proto.FlagWR_CREATE != 0, "CREATE", ""),
		)
		if fl != "" {
			fl = " flags=" + fl
		}
		return fmt.Sprintf("path=%s off=%d len=%d%s", p, off, ln, fl)
	case proto.OpAPPEND:
		p := readPath(d)
		ln, _ := d.ReadU16()
		fl := choose(flags&proto.FlagAP_CREATE != 0, "CREATE", "")
		if fl != "" {
			fl = " flags=" + fl
		}
		return fmt.Sprintf("path=%s len=%d%s", p, ln, fl)
	case proto.OpMKDIR:
		p := readPath(d)
		fl := choose(flags&proto.FlagMK_PARENTS != 0, "PARENTS", "")
		if fl != "" {
			fl = " flags=" + fl
		}
		return fmt.Sprintf("path=%s%s", p, fl)
	case proto.OpRMDIR:
		p := readPath(d)
		fl := choose(flags&proto.FlagRD_RECURSIVE != 0, "RECURSIVE", "")
		if fl != "" {
			fl = " flags=" + fl
		}
		return fmt.Sprintf("path=%s%s", p, fl)
	case proto.OpRM:
		p := readPath(d)
		return fmt.Sprintf("path=%s", p)
	case proto.OpCP:
		src := readPath(d)
		dst := readPath(d)
		fl := flagList(
			choose(flags&proto.FlagCP_OVERWRITE != 0, "OVERWRITE", ""),
			choose(flags&proto.FlagCP_RECURSIVE != 0, "RECURSIVE", ""),
		)
		if fl != "" {
			fl = " flags=" + fl
		}
		return fmt.Sprintf("src=%s dst=%s%s", src, dst, fl)
	case proto.OpHASH:
		p := readPath(d)
		algo := choose(flags&proto.FlagH_ALGO != 0, "SHA1", "CRC32")
		return fmt.Sprintf("path=%s algo=%s", p, algo)
	case proto.OpSEARCH:
		base := readPath(d)
		q, _ := d.ReadString(64)
		start, _ := d.ReadU16()
		max, _ := d.ReadU16()
		maxScan, _ := d.ReadU32()
		fl := flagList(
			choose(flags&proto.FlagS_CASE_INSENSITIVE != 0, "CI", ""),
			choose(flags&proto.FlagS_RECURSIVE != 0, "RECURSIVE", ""),
			choose(flags&proto.FlagS_WHOLE_WORD != 0, "WHOLE", ""),
		)
		if fl != "" {
			fl = " flags=" + fl
		}
		return fmt.Sprintf("base=%s q=%q start=%d max=%d scan=%d%s", base, trunc(q, 60), start, max, maxScan, fl)
	case proto.OpMV:
		src := readPath(d)
		dst := readPath(d)
		fl := choose(flags&proto.FlagMV_OVERWRITE != 0, "OVERWRITE", "")
		if fl != "" {
			fl = " flags=" + fl
		}
		return fmt.Sprintf("src=%s dst=%s%s", src, dst, fl)
	default:
		return ""
	}
}

func choose[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func trunc(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}
	if max <= 1 {
		return s[:max]
	}
	return s[:max-1] + "…"
}
