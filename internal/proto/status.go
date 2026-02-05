package proto

// App status codes (W64F response header 'status')
const (
	StatusOK            byte = 0
	StatusNotFound      byte = 1
	StatusNotADir       byte = 2
	StatusIsADir        byte = 3
	StatusAlreadyExists byte = 4
	StatusDirNotEmpty   byte = 5
	StatusAccessDenied  byte = 6
	StatusInvalidPath   byte = 7
	StatusRangeInvalid  byte = 8
	StatusTooLarge      byte = 9
	StatusNotSupported  byte = 10
	StatusBusy          byte = 11
	StatusBadRequest    byte = 12
	StatusInternal      byte = 13
)

// Backwards-compatible aliases (older internal code used shorter names).
const (
	StatusNotDir  = StatusNotADir
	StatusIsDir   = StatusIsADir
	StatusBadReq  = StatusBadRequest
	StatusBadPath = StatusInvalidPath
)

// Feature bits (CAPS.features_lo)
const (
	FeatSTATFS          uint32 = 1 << 0
	FeatAPPEND          uint32 = 1 << 1
	FeatSEARCH          uint32 = 1 << 2
	FeatHASH_CRC32      uint32 = 1 << 3
	FeatHASH_SHA1       uint32 = 1 << 4
	FeatMKDIR_PARENTS   uint32 = 1 << 5
	FeatRMDIR_RECURSIVE uint32 = 1 << 6
	FeatCP_RECURSIVE    uint32 = 1 << 7
	FeatOVERWRITE       uint32 = 1 << 8
	FeatERRMSG          uint32 = 1 << 9
)

// Flags (op-specific)
const (
	// WRITE_RANGE flags
	FlagWR_TRUNCATE  = 1 << 0
	FlagWR_CREATE    = 1 << 1
	FlagWR_OVERWRITE = 1 << 2

	// MKDIR flags
	FlagMK_PARENTS = 1 << 0

	// RMDIR flags
	FlagRD_RECURSIVE = 1 << 0

	// CP flags
	FlagCP_OVERWRITE = 1 << 0
	FlagCP_RECURSIVE = 1 << 1

	// MV flags
	FlagMV_OVERWRITE = 1 << 0

	// APPEND flags
	// Bit1 CREATE (aligns with WRITE_RANGE CREATE semantics)
	FlagAP_CREATE = 1 << 1

	// SEARCH flags
	FlagS_CASE_INSENSITIVE = 1 << 0
	FlagS_RECURSIVE        = 1 << 1
	FlagS_WHOLE_WORD       = 1 << 2

	// HASH flags
	// Bit0 ALGO: 0=CRC32, 1=SHA1
	FlagH_ALGO = 1 << 0
)
