package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"wicos64-server/internal/proto"
	"wicos64-server/internal/version"
)

func main() {
	var url string
	var showVersion bool
	flag.StringVar(&url, "url", "http://127.0.0.1:8080/wicos64/api?token=CHANGE-ME", "W64F endpoint URL (including token)")
	flag.BoolVar(&showVersion, "version", false, "Print version information and exit")
	flag.Parse()

	if showVersion {
		fmt.Println(version.Get().String())
		return
	}

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}

	cmd := strings.ToLower(args[0])
	switch cmd {
	case "version":
		fmt.Println(version.Get().String())
		return
	case "caps":
		req := buildReq(proto.OpCAPS, 0, nil)
		resp, status, errMsg := post(url, req)
		if status != proto.StatusOK {
			printErr(status, errMsg, resp)
			os.Exit(1)
		}
		printCaps(resp)
	case "ping":
		req := buildReq(proto.OpPING, 0, nil)
		resp, status, errMsg := post(url, req)
		if status != proto.StatusOK {
			printErr(status, errMsg, resp)
			os.Exit(1)
		}
		if len(resp) == 0 {
			fmt.Println("(no payload)")
			return
		}
		d := proto.NewDecoder(resp)
		s, _ := d.ReadString(256)
		fmt.Println(s)
	case "ls":
		if len(args) < 2 {
			fmt.Println("ls <path> [start_index] [max_entries]")
			os.Exit(2)
		}
		p := args[1]
		start := uint16(0)
		max := uint16(50)
		if len(args) >= 3 {
			v, _ := strconv.ParseUint(args[2], 10, 16)
			start = uint16(v)
		}
		if len(args) >= 4 {
			v, _ := strconv.ParseUint(args[3], 10, 16)
			max = uint16(v)
		}
		pl := buildLS(p, start, max)
		req := buildReq(proto.OpLS, 0, pl)
		resp, status, errMsg := post(url, req)
		if status != proto.StatusOK {
			printErr(status, errMsg, resp)
			os.Exit(1)
		}
		printLS(resp)
	case "append":
		if len(args) < 3 {
			fmt.Println("append <path> <text>")
			os.Exit(2)
		}
		p := args[1]
		data := []byte(args[2])
		pl := buildAppend(p, data)
		req := buildReq(proto.OpAPPEND, proto.FlagAP_CREATE, pl)
		resp, status, errMsg := post(url, req)
		if status != proto.StatusOK {
			printErr(status, errMsg, resp)
			os.Exit(1)
		}
		fmt.Println("OK")
	case "hash":
		if len(args) < 2 {
			fmt.Println("hash <path>")
			os.Exit(2)
		}
		pl := buildPathOnly(args[1])
		req := buildReq(proto.OpHASH, 0, pl)
		resp, status, errMsg := post(url, req)
		if status != proto.StatusOK {
			printErr(status, errMsg, resp)
			os.Exit(1)
		}
		if len(resp) != 4 {
			fmt.Printf("unexpected payload len=%d\n", len(resp))
			os.Exit(1)
		}
		sum := binary.LittleEndian.Uint32(resp)
		fmt.Printf("CRC32=0x%08X (%d)\n", sum, sum)
	case "search":
		if len(args) < 3 {
			fmt.Println("search <base_path> <query> [start_index] [max_results] [max_scan_bytes] [flags]")
			fmt.Println("flags: i=case-insensitive, r=recursive, w=whole-word (default: ir)")
			os.Exit(2)
		}
		base := args[1]
		q := args[2]
		start := uint16(0)
		maxRes := uint16(20)
		maxScan := uint32(0)
		flagsStr := "ir"
		if len(args) >= 4 {
			v, _ := strconv.ParseUint(args[3], 10, 16)
			start = uint16(v)
		}
		if len(args) >= 5 {
			v, _ := strconv.ParseUint(args[4], 10, 16)
			maxRes = uint16(v)
		}
		if len(args) >= 6 {
			v, _ := strconv.ParseUint(args[5], 10, 32)
			maxScan = uint32(v)
		}
		if len(args) >= 7 {
			flagsStr = strings.ToLower(args[6])
		}
		var fl byte
		if strings.Contains(flagsStr, "i") {
			fl |= proto.FlagS_CASE_INSENSITIVE
		}
		if strings.Contains(flagsStr, "r") {
			fl |= proto.FlagS_RECURSIVE
		}
		if strings.Contains(flagsStr, "w") {
			fl |= proto.FlagS_WHOLE_WORD
		}

		pl := buildSearch(base, q, start, maxRes, maxScan)
		req := buildReq(proto.OpSEARCH, fl, pl)
		resp, status, errMsg := post(url, req)
		if status != proto.StatusOK {
			printErr(status, errMsg, resp)
			os.Exit(1)
		}
		printSearch(resp)
	default:
		fmt.Printf("unknown command: %s\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println("Usage: w64tool -url <endpoint> <command> [args]")
	fmt.Println("Commands:")
	fmt.Println("  caps")
	fmt.Println("  ping")
	fmt.Println("  ls <path> [start_index] [max_entries]")
	fmt.Println("  append <path> <text>")
	fmt.Println("  hash <path>")
	fmt.Println("  search <base_path> <query> [start_index] [max_results] [max_scan_bytes] [flags]")
}

func buildReq(op byte, flags byte, payload []byte) []byte {
	// W64F request header (10 bytes): magic(4) + ver(1) + op(1) + flags(1) + reserved(1) + payload_len(2)
	buf := make([]byte, 0, 10+len(payload))
	buf = append(buf, 'W', '6', '4', 'F')
	buf = append(buf, proto.Version)
	buf = append(buf, op)
	buf = append(buf, flags)
	buf = append(buf, 0) // reserved
	ln := uint16(len(payload))
	buf = append(buf, byte(ln), byte(ln>>8))
	buf = append(buf, payload...)
	return buf
}

func buildPathOnly(p string) []byte {
	e := proto.NewEncoder(2 + len(p))
	_ = e.WriteString(p)
	return e.Bytes()
}

func buildLS(p string, start, max uint16) []byte {
	e := proto.NewEncoder(64)
	_ = e.WriteString(p)
	e.WriteU16(start)
	e.WriteU16(max)
	return e.Bytes()
}

func buildAppend(p string, data []byte) []byte {
	e := proto.NewEncoder(2 + len(p) + 2 + len(data))
	_ = e.WriteString(p)
	e.WriteU16(uint16(len(data)))
	e.WriteBytes(data)
	return e.Bytes()
}

func buildSearch(base, q string, start, max uint16, maxScan uint32) []byte {
	e := proto.NewEncoder(128)
	_ = e.WriteString(base)
	_ = e.WriteString(q)
	e.WriteU16(start)
	e.WriteU16(max)
	e.WriteU32(maxScan)
	return e.Bytes()
}

func post(url string, req []byte) (respPayload []byte, status byte, errMsg string) {
	r, err := http.Post(url, "application/octet-stream", bytes.NewReader(req))
	if err != nil {
		fmt.Println("http error:", err)
		os.Exit(1)
	}
	defer r.Body.Close()
	data, _ := io.ReadAll(r.Body)
	if len(data) < proto.HeaderSize {
		fmt.Printf("invalid response (len=%d)\n", len(data))
		os.Exit(1)
	}
	if string(data[0:4]) != proto.Magic {
		fmt.Printf("invalid magic: %q\n", string(data[0:4]))
		os.Exit(1)
	}
	// Response header layout matches request header:
	// magic(4) ver(1) op_echo(1) status(1) reserved(1) payload_len(2)
	status = data[6]
	ln := binary.LittleEndian.Uint16(data[8:10])
	respPayload = data[proto.HeaderSize:]
	if int(ln) != len(respPayload) {
		fmt.Printf("length mismatch header=%d body=%d\n", ln, len(respPayload))
		// still return what we got
	}
	if status != proto.StatusOK {
		errMsg = printErrMsgIfAny(respPayload)
	}
	return
}

func printErr(status byte, errMsg string, payload []byte) {
	fmt.Printf("ERROR status=%d\n", status)
	if errMsg != "" {
		fmt.Println("message:", errMsg)
	} else if len(payload) > 0 {
		fmt.Printf("payload (%d bytes): %x\n", len(payload), payload)
	}
}

func printErrMsgIfAny(payload []byte) string {
	// Server may return errmsg as string payload on errors if enable_errmsg=true.
	d := proto.NewDecoder(payload)
	s, err := d.ReadString(512)
	if err != nil {
		return ""
	}
	return s
}

func printCaps(payload []byte) {
	d := proto.NewDecoder(payload)
	protoVer, _ := d.ReadU16()
	maxPayload, _ := d.ReadU16()
	maxChunk, _ := d.ReadU16()
	maxPath, _ := d.ReadU16()
	maxName, _ := d.ReadU16()
	features, _ := d.ReadU32()
	serverName, _ := d.ReadString(128)

	fmt.Printf("proto_ver:   %d\n", protoVer)
	fmt.Printf("max_payload: %d\n", maxPayload)
	fmt.Printf("max_chunk:   %d\n", maxChunk)
	fmt.Printf("max_path:    %d\n", maxPath)
	fmt.Printf("max_name:    %d\n", maxName)
	fmt.Printf("features:    0x%08X\n", features)
	fmt.Printf("server_name: %q\n", serverName)
}

func printLS(payload []byte) {
	d := proto.NewDecoder(payload)
	count, err := d.ReadU16()
	if err != nil {
		fmt.Println("decode error:", err)
		return
	}
	fmt.Printf("count=%d\n", count)
	for i := 0; i < int(count); i++ {
		typ, _ := d.ReadU8()
		sz, _ := d.ReadU32()
		mt, _ := d.ReadU32()
		name, _ := d.ReadString(255)
		kind := "FILE"
		if typ == 1 {
			kind = "DIR"
		}
		fmt.Printf("  %-4s %10d mtime=%d name=%s\n", kind, sz, mt, name)
	}
	next, _ := d.ReadU16()
	if next == 0xFFFF {
		fmt.Println("next_index=END")
	} else {
		fmt.Printf("next_index=%d\n", next)
	}
}

func printSearch(payload []byte) {
	d := proto.NewDecoder(payload)
	count, err := d.ReadU16()
	if err != nil {
		fmt.Println("decode error:", err)
		return
	}
	fmt.Printf("hits=%d\n", count)
	for i := 0; i < int(count); i++ {
		p, _ := d.ReadString(512)
		off, _ := d.ReadU32()
		pln, _ := d.ReadU16()
		pv, _ := d.ReadBytes(int(pln))
		fmt.Printf("  %s @%d  preview=%q\n", p, off, string(pv))
	}
	next, _ := d.ReadU16()
	if next == 0xFFFF {
		fmt.Println("next_index=END")
	} else {
		fmt.Printf("next_index=%d\n", next)
	}
}
