package mtfdb

type Changer struct {
	Name      string `json:"name"`
	Device    string `json:"device"`
	Comment   string `json:"comment"`
	CreatedAt int64  `json:"created_at"`
}

type Drive struct {
	Name       string `json:"name"`
	Device     string `json:"device"`
	Changer    string `json:"changer"`
	DriveIndex int    `json:"drive_index"`
	Comment    string `json:"comment"`
	CreatedAt  int64  `json:"created_at"`
}

type Cartridge struct {
	Barcode         string `json:"barcode"`
	Label           string `json:"label"`
	MediaFamilyID   int64  `json:"media_family_id"`
	MediaFamilyName string `json:"media_family_name"`
	Sequence        int    `json:"sequence"`
	PbaOffset       int64  `json:"pba_offset"`
	Role            string `json:"role"`
	CatalogType     int    `json:"catalog_type"`
	IsBkfFile       bool   `json:"is_bkf_file"`
	SourcePath      string `json:"source_path"`
	Volumes         int    `json:"volumes"`
	Directories     int    `json:"directories"`
	Files           int    `json:"files"`
	EmptyFiles      int    `json:"empty_files"`
	FileBytes       int64  `json:"file_bytes"`
	SparseFiles     int    `json:"sparse_files"`
	CompressedFiles int    `json:"compressed_files"`
	EncryptedFiles  int    `json:"encrypted_files"`
	HasCatalog      bool   `json:"has_catalog"`
	CatalogBytes    int64  `json:"catalog_bytes"`
	SetsClosed      int    `json:"sets_closed"`
	Status          string `json:"status"`
	LastScanned     int64  `json:"last_scanned"`
	CreatedAt       int64  `json:"created_at"`
}

type MediaFamily struct {
	ID             int64  `json:"id"`
	Name           string `json:"name"`
	TotalTapes     int    `json:"total_tapes"`
	CartridgeCount int    `json:"cartridge_count"`
	HasCatalog     bool   `json:"has_catalog"`
	DataSetCount   int    `json:"data_set_count"`
	LastScanned    int64  `json:"last_scanned"`
	CreatedAt      int64  `json:"created_at"`
}
