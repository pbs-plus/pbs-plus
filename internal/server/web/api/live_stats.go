package api

type LiveStats struct {
	FileCount   int64
	FolderCount int64
	FilesSpeed  int
	BytesSpeed  int
	BytesTotal  int64
}

func FillSpeedFields(stats *LiveStats, readSpeedHuman, readTotalHuman, processingSpeedHuman *string) {
	if stats == nil {
		return
	}
	if stats.BytesSpeed > 0 {
		*readSpeedHuman = HumanReadableSpeed(stats.BytesSpeed)
	}
	if stats.BytesTotal > 0 {
		*readTotalHuman = HumanReadableBytes(int(stats.BytesTotal))
	}
	if stats.FilesSpeed > 0 {
		*processingSpeedHuman = FormatSpeed(stats.FilesSpeed)
	}
}
