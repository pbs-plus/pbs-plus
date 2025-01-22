package utils

func IsMounted(path string) bool {
	// Open /proc/self/mountinfo to check mounts
	mountInfoFile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer mountInfoFile.Close()

	scanner := bufio.NewScanner(mountInfoFile)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 5 && fields[4] == path {
			return true
		}
	}

	return false
}
