package conf

import (
	"errors"
	"regexp"
)

var jobIdRegex = regexp.MustCompile(`^(?:[A-Za-z0-9_][A-Za-z0-9._\-]*)$`)

func ValidateJobId(jobId string) error {
	if jobId == "" {
		return errors.New("jobId cannot be empty")
	}

	if len(jobId) > 255 {
		return errors.New("jobId exceeds maximum length")
	}

	if !jobIdRegex.MatchString(jobId) {
		return errors.New("jobId contains invalid characters")
	}

	return nil
}
