package proto

// AppendU16 appends v as little-endian uint16.
//
// This helper exists because some parts of the server build certain payloads
// using manual slice appends instead of the Encoder type.
func AppendU16(b []byte, v uint16) []byte {
	return append(b, byte(v), byte(v>>8))
}
