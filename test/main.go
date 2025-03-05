package main

import (
	"log"
	"os"

	"github.com/Microsoft/go-winio"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/vssfs/types"
	"golang.org/x/sys/windows"
)

func main() {
	fullPath := `D:\GitOps\it-applications\.git\HEAD`
	stat, err := os.Stat(fullPath)
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Open(fullPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	log.Print(stat.Size())

	standardInfo, err := winio.GetFileStandardInfo(file)
	if err == nil {
	}
	blockSize := 4096
	blocks := uint64((standardInfo.AllocationSize + int64(blockSize) - 1) / int64(blockSize))
	log.Print(blocks)

	info := types.VSSFileInfo{
		Name:    stat.Name(),
		Size:    stat.Size(),
		Mode:    uint32(stat.Mode()),
		ModTime: stat.ModTime(),
		IsDir:   stat.IsDir(),
		Blocks:  blocks,
	}

	data, err := info.Encode()
	if err != nil {
		log.Fatal(err)
	}

	var test types.VSSFileInfo
	if err := test.Decode(data); err != nil {
		log.Fatal(err)
	}

	log.Print(test.Blocks)

	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(fullPath),
		windows.GENERIC_READ,
		windows.FILE_SHARE_READ,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_SEQUENTIAL_SCAN,
		0,
	)
	if err != nil {
		log.Fatal(err)
	}

	defer windows.CloseHandle(handle)
}
