package pxarmount

func joinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

func splitPath(path string) []string {
	if path == "/" || path == "" {
		return nil
	}
	p := path
	if p[0] == '/' {
		p = p[1:]
	}
	var parts []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			parts = append(parts, p[start:i])
			start = i + 1
		}
	}
	parts = append(parts, p[start:])
	return parts
}
