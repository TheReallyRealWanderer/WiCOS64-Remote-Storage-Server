package server

import "wicos64-server/internal/proto"

func isWriteOp(op byte) bool {
	switch op {
	case proto.OpWRITE_RANGE, proto.OpAPPEND, proto.OpMKDIR, proto.OpRMDIR, proto.OpRM, proto.OpCP, proto.OpMV:
		return true
	default:
		return false
	}
}
