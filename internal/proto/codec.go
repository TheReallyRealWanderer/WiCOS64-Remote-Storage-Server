package proto

import (
	"encoding/binary"
	"fmt"
)

// Decoder reads little-endian primitives from a byte slice.
// It is intentionally minimal to keep dependencies low and behavior deterministic.
type Decoder struct {
	b []byte
	o int
}

func NewDecoder(b []byte) *Decoder {
	return &Decoder{b: b, o: 0}
}

func (d *Decoder) Remaining() int { return len(d.b) - d.o }

// Len is a convenience alias for Remaining().
// Some older server code used Len() to check for leftover payload bytes.
func (d *Decoder) Len() int { return d.Remaining() }

func (d *Decoder) ReadU8() (byte, error) {
	if d.Remaining() < 1 {
		return 0, fmt.Errorf("need 1 byte")
	}
	v := d.b[d.o]
	d.o++
	return v, nil
}

func (d *Decoder) ReadU16() (uint16, error) {
	if d.Remaining() < 2 {
		return 0, fmt.Errorf("need 2 bytes")
	}
	v := binary.LittleEndian.Uint16(d.b[d.o : d.o+2])
	d.o += 2
	return v, nil
}

func (d *Decoder) ReadU32() (uint32, error) {
	if d.Remaining() < 4 {
		return 0, fmt.Errorf("need 4 bytes")
	}
	v := binary.LittleEndian.Uint32(d.b[d.o : d.o+4])
	d.o += 4
	return v, nil
}

func (d *Decoder) ReadBytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative length")
	}
	if d.Remaining() < n {
		return nil, fmt.Errorf("need %d bytes", n)
	}
	v := d.b[d.o : d.o+n]
	d.o += n
	return v, nil
}

// ReadString reads a u16 length-prefixed string.
// maxLen is a protocol / server limit (e.g. max_path/max_name).
func (d *Decoder) ReadString(maxLen uint16) (string, error) {
	ln, err := d.ReadU16()
	if err != nil {
		return "", err
	}
	if ln > maxLen {
		return "", fmt.Errorf("string length %d exceeds limit %d", ln, maxLen)
	}
	b, err := d.ReadBytes(int(ln))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// Encoder builds little-endian protocol payloads.
type Encoder struct {
	b []byte
}

func NewEncoder(capacity int) *Encoder {
	if capacity < 0 {
		capacity = 0
	}
	return &Encoder{b: make([]byte, 0, capacity)}
}

func (e *Encoder) Bytes() []byte { return e.b }

func (e *Encoder) WriteU8(v byte) {
	e.b = append(e.b, v)
}

func (e *Encoder) WriteU16(v uint16) {
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], v)
	e.b = append(e.b, tmp[:]...)
}

func (e *Encoder) WriteU32(v uint32) {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	e.b = append(e.b, tmp[:]...)
}

func (e *Encoder) WriteBytes(b []byte) {
	e.b = append(e.b, b...)
}

// WriteString writes a u16 length-prefixed string.
// This does not validate character ranges; path validation is handled elsewhere.
func (e *Encoder) WriteString(s string) error {
	b := []byte(s)
	if len(b) > 0xFFFF {
		return fmt.Errorf("string too long: %d", len(b))
	}
	e.WriteU16(uint16(len(b)))
	e.WriteBytes(b)
	return nil
}
