package utils

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"
)

func TailFile(ctx context.Context, filePath string, newLineChan chan<- string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Start reading the file line by line
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				// Wait briefly and try again if there are no new lines
				if os.IsTimeout(err) {
					time.Sleep(100 * time.Millisecond)
					continue
				} else if err.Error() == "EOF" {
					// Pause to allow new data to be written to the file
					time.Sleep(100 * time.Millisecond)
					continue
				} else {
					return fmt.Errorf("error reading file: %w", err)
				}
			}
			// Send the new line to the channel
			newLineChan <- line
		}
	}
}
