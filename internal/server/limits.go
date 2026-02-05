package server

// Limits are effective, per-request policy values derived from config + token.
type Limits struct {
	ReadOnly                     bool
	QuotaBytes                   uint64
	MaxFileBytes                 uint64
	DiskImagesEnabled            bool
	DiskImagesWriteEnabled       bool
	DiskImagesAutoResizeEnabled  bool
	DiskImagesAllowRenameConvert bool
}
