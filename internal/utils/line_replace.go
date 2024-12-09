package utils

import (
	"os"
	"strings"
)

func ReplaceLastLine(filePath string, newLine string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	size := stat.Size()
	buf := make([]byte, 1024)
	offset := size
	lastNewline := -1

	for offset > 0 {
		chunkSize := int64(len(buf))
		if chunkSize > offset {
			chunkSize = offset
		}
		offset -= chunkSize
		_, err := file.ReadAt(buf[:chunkSize], offset)
		if err != nil {
			return err
		}

		if idx := strings.LastIndexByte(string(buf[:chunkSize]), '\n'); idx != -1 {
			lastNewline = int(offset) + idx
			break
		}
	}

	var originalLastLine string
	if lastNewline == -1 {
		file.Seek(0, 0)
		content, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		originalLastLine = string(content)
		lastNewline = 0
	} else {
		file.Seek(int64(lastNewline+1), 0)
		content, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		originalLastLine = strings.TrimSuffix(string(content), "\n")
	}

	if strings.Contains(originalLastLine, "TASK ERROR: connection error: not connected") {
		err = file.Truncate(int64(lastNewline + 1))
		if err != nil {
			return err
		}

		completeNewLine := strings.ReplaceAll(originalLastLine, "connection error: not connected", newLine)

		_, err = file.WriteAt([]byte(completeNewLine+"\n"), int64(lastNewline+1))
		if err != nil {
			return err
		}
	}

	return nil
}
