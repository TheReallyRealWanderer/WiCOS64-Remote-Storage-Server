package proto

import (
	"encoding/binary"
	"fmt"
)

const (
	Magic      = "W64F"
	Version    = 1
	HeaderSize = 10
)

// Opcodes (v0.2.x)
const (
	OpLS          = 0x01
	OpSTAT        = 0x02
	OpREAD_RANGE  = 0x03
	OpWRITE_RANGE = 0x04
	OpAPPEND      = 0x05 // optional
	OpMKDIR       = 0x06
	OpRMDIR       = 0x07
	OpRM          = 0x08
	OpCP          = 0x09
	OpMV          = 0x0A
	OpSEARCH      = 0x0B // optional
	OpHASH        = 0x0C // optional
	OpPING        = 0x0D // legacy optional
	OpCAPS        = 0x0E
	OpSTATFS      = 0x0F
)

// ReqHeader is the fixed 10-byte request header.
type ReqHeader struct {
	Version    byte
	Op         byte
	Flags      byte
	Reserved   byte
	PayloadLen uint16
}

// ParseReqHeader parses the fixed 10-byte header. The returned boolean indicates whether
// the magic was correct ("W64F").
func ParseReqHeader(body []byte) (ReqHeader, bool, error) {
	var h ReqHeader
	if len(body) < HeaderSize {
		return h, false, fmt.Errorf("body too short for header")
	}
	if string(body[0:4]) != Magic {
		return h, false, fmt.Errorf("bad magic")
	}
	h.Version = body[4]
	h.Op = body[5]
	h.Flags = body[6]
	h.Reserved = body[7]
	h.PayloadLen = binary.LittleEndian.Uint16(body[8:10])
	return h, true, nil
}

// BuildResponse builds a full W64F response body (10-byte header + payload).
func BuildResponse(version, opEcho, status byte, payload []byte) ([]byte, error) {
	if len(payload) > 0xFFFF {
		return nil, fmt.Errorf("response payload too large: %d", len(payload))
	}
	out := make([]byte, HeaderSize+len(payload))
	copy(out[0:4], []byte(Magic))
	out[4] = version
	out[5] = opEcho
	out[6] = status
	out[7] = 0
	binary.LittleEndian.PutUint16(out[8:10], uint16(len(payload)))
	copy(out[HeaderSize:], payload)
	return out, nil
}
