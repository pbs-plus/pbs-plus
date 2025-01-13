package utils

import (
	"fmt"
	"math"
	"os"
	"time"
)

func WaitForLogFile(logFilePath string, maxWait time.Duration) (*os.File, error) {
	start := time.Now()

	for {
		logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0644)
		if err == nil {
			return logFile, nil // Successfully opened the file
		}

		if time.Since(start) > maxWait {
			return nil, fmt.Errorf("timeout waiting for file %s to become writable: %v", logFilePath, err)
		}

		// Sleep for a short duration before retrying
		// Using exponential backoff with a maximum of 100ms
		backoff := time.Duration(math.Min(100, math.Pow(2, float64(time.Since(start).Milliseconds()/100)))) * time.Millisecond
		time.Sleep(backoff)
	}
}
