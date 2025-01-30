package vssfs

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// Testing constants
const (
	numPaths      = 1000000
	maxDepth      = 15
	maxNameLength = 255
)

var (
	// Common directory names found in real projects
	commonDirs = []string{
		"src", "pkg", "cmd", "internal", "api", "web", "ui", "docs",
		"test", "tests", "scripts", "tools", "vendor", "third_party",
		"examples", "build", "dist", "public", "private", "tmp", "temp",
		"log", "logs", "config", "configs", "deploy", "deployments",
		"migrations", "seeds", "data", "assets", "images", "css", "js",
		"fonts", "icons", "media", "video", "audio", "downloads",
		"uploads", "backup", "cache", "lib", "libs", "node_modules",
		"packages", "modules", "components", "containers", "layouts",
		"middlewares", "models", "views", "controllers", "services",
		"helpers", "utils", "common", "shared", "core", "base",
	}

	// Common file extensions (without dots)
	commonExts = []string{
		"go", "mod", "sum", "txt", "md", "json", "yaml", "yml",
		"toml", "ini", "cfg", "conf", "config", "xml", "html",
		"htm", "css", "scss", "sass", "less", "js", "jsx", "ts",
		"tsx", "vue", "php", "py", "rb", "rs", "java", "class",
		"jar", "c", "h", "cpp", "hpp", "cc", "hh", "cs", "fs",
		"sql", "db", "sqlite", "mysql", "pgsql", "log", "pid",
		"lock", "tmp", "bak", "swp", "zip", "tar", "gz", "tgz",
		"rar", "7z", "png", "jpg", "jpeg", "gif", "svg", "ico",
		"mp3", "mp4", "avi", "mkv", "pdf", "doc", "docx", "xls",
		"xlsx", "ppt", "pptx",
	}

	// Common file names
	commonFiles = []string{
		"README", "LICENSE", "CHANGELOG", "Makefile", "Dockerfile",
		"docker-compose", "requirements", "package", "composer",
		"index", "main", "app", "server", "client", "test", "spec",
		"setup", "config", "settings", "environment", "env",
		".gitignore", ".dockerignore", ".eslintrc", ".prettierrc",
		"tsconfig", "webpack.config", "babel.config", "jest.config",
		"nginx.conf", "supervisor.conf", "prometheus.yml", "grafana.ini",
	}
)

func TestPathIDCollisions(t *testing.T) {
	paths := generateRealisticPaths(numPaths)

	idMap := make(map[uint64]string)
	collisions := 0

	for _, path := range paths {
		id := generateFullPathID(path)
		if existing, exists := idMap[id]; exists {
			collisions++
			t.Logf("Collision found!\nPath 1: %s\nPath 2: %s\nID: %016x\n",
				existing, path, id)
		} else {
			idMap[id] = path
		}

		// Test quick match
		if !quickMatch(id, path) {
			t.Errorf("Quick match failed for path: %s", path)
		}
	}

	collisionRate := float64(collisions) / float64(len(paths)) * 100
	t.Logf("Tested %d paths", len(paths))
	t.Logf("Found %d collisions (%.6f%%)", collisions, collisionRate)

	if collisions > 0 {
		t.Errorf("Found %d collisions, expected 0", collisions)
	}
}

func generateRealisticPaths(count int) []string {
	paths := make(map[string]bool, count) // Use map to ensure uniqueness

	for len(paths) < count {
		path := generateRandomPath()
		if len(path) <= maxNameLength && path != "" {
			paths[path] = true
		}
	}

	result := make([]string, 0, len(paths))
	for path := range paths {
		result = append(result, path)
	}
	return result
}

func generateRandomPath() string {
	components := make([]string, 0)

	// Special cases (20% chance)
	if rand.Float32() < 0.2 {
		switch rand.Intn(3) {
		case 0: // Git object
			hash := fmt.Sprintf("%02x%038x", rand.Intn(256), rand.Uint64())
			return fmt.Sprintf(".git/objects/%s/%s", hash[:2], hash[2:])
		case 1: // Node module
			scope := ""
			if rand.Float32() < 0.3 {
				scope = fmt.Sprintf("@%s/", randomString(5, 10))
			}
			return fmt.Sprintf("node_modules/%s%s", scope, randomString(5, 15))
		case 2: // Deeply nested path
			depth := 5 + rand.Intn(10)
			for i := 0; i < depth; i++ {
				components = append(components, randomString(3, 10))
			}
		}
	} else {
		// Regular path
		depth := 1 + rand.Intn(maxDepth)

		// Add directories
		for i := 0; i < depth; i++ {
			if rand.Float32() < 0.7 {
				components = append(components, commonDirs[rand.Intn(len(commonDirs))])
			} else {
				components = append(components, randomString(3, 10))
			}
		}
	}

	// Add filename
	var filename string
	if rand.Float32() < 0.3 {
		filename = commonFiles[rand.Intn(len(commonFiles))]
	} else {
		filename = randomString(1, 20)
	}

	// Add extension (95% chance)
	if rand.Float32() < 0.95 {
		ext := commonExts[rand.Intn(len(commonExts))]
		filename = filename + "." + ext
	}

	components = append(components, filename)
	return strings.Join(components, "/")
}

func randomString(minLen, maxLen int) string {
	length := minLen + rand.Intn(maxLen-minLen+1)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func TestSimilarPaths(t *testing.T) {
	// Generate sets of similar paths
	similarSets := [][]string{
		{
			"very/long/path/to/some/deeply/nested/file/structure/document.pdf",
			"very/long/path/to/some/deeply/nested/file/structure/document2.pdf",
			"very/long/path/to/some/deeply/nested/file/structure2/document.pdf",
		},
		{
			"project/src/component/Button.tsx",
			"project/src/component/Button.test.tsx",
			"project/src/component/Button.styles.tsx",
			"project/src/component/ButtonGroup.tsx",
		},
		{
			"node_modules/@scope/package/index.js",
			"node_modules/@scope/package/index.d.ts",
			"node_modules/@scope/package-utils/index.js",
		},
	}

	for i, paths := range similarSets {
		ids := make(map[uint64]string)
		t.Logf("\nTesting similar paths set %d:", i+1)

		for _, path := range paths {
			id := generateFullPathID(path)
			if existing, exists := ids[id]; exists {
				t.Errorf("Collision in similar paths!\nPath 1: %s\nPath 2: %s\nID: %016x",
					existing, path, id)
			} else {
				ids[id] = path
				t.Logf("%s -> %016x", path, id)
			}
		}
	}
}

func TestDeepNestedPaths(t *testing.T) {
	paths := generateDeepNestedPaths(10000)
	idMap := make(map[uint64]string)
	collisions := 0

	for _, path := range paths {
		id := generateFullPathID(path)
		if existing, exists := idMap[id]; exists {
			collisions++
			t.Errorf("Collision found in deep paths!\nPath 1: %s\nPath 2: %s\nID: %016x",
				existing, path, id)
		} else {
			idMap[id] = path
		}
	}

	if collisions > 0 {
		t.Errorf("Found %d collisions in deep paths", collisions)
	}
}

func generateDeepNestedPaths(count int) []string {
	paths := make(map[string]bool, count)

	basePaths := []string{
		"project/src/components/ui/widgets/forms/inputs/validation/rules/custom",
		"node_modules/@babel/runtime/helpers/esm/extends/impl/core/utils",
		"build/release/x64/optimized/packages/compiled/minified/vendor",
		".git/objects/pack/streaming/delta/compressed/chunks/cache",
		"test/integration/scenarios/complex/fixtures/mock/data",
	}

	fileTypes := []string{
		"component", "helper", "util", "service", "model",
		"controller", "view", "template", "schema", "config",
	}

	extensions := []string{
		"ts", "tsx", "js", "jsx", "css", "scss", "json", "md",
		"test.ts", "spec.ts", "d.ts", "min.js", "module.js",
	}

	for len(paths) < count {
		basePath := basePaths[rand.Intn(len(basePaths))]

		// Add some random subdirectories
		extraDepth := rand.Intn(5) // 0-4 extra levels
		for i := 0; i < extraDepth; i++ {
			basePath += "/" + randomString(3, 10)
		}

		fileType := fileTypes[rand.Intn(len(fileTypes))]
		ext := extensions[rand.Intn(len(extensions))]

		variations := []string{
			fmt.Sprintf("%s.%s", fileType, ext),
			fmt.Sprintf("%s.v%d.%s", fileType, rand.Intn(100), ext),
			fmt.Sprintf("%s.%s.%s", fileType, randomString(3, 8), ext),
			fmt.Sprintf("%s_%d.%s", fileType, rand.Intn(1000), ext),
		}

		filename := variations[rand.Intn(len(variations))]
		path := basePath + "/" + filename

		if len(path) <= maxNameLength {
			paths[path] = true
		}
	}

	result := make([]string, 0, len(paths))
	for path := range paths {
		result = append(result, path)
	}
	return result
}
