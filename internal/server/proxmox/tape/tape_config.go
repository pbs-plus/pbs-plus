//go:build linux

// Package tape reads Proxmox Backup Server tape device configuration
// (the /etc/proxmox-backup/tape.cfg SectionConfig file) and resolves SCSI
// tape device paths. It is independent of the mtf/store persistence layer.
package tape

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Config holds the parsed tape.cfg configuration.
type Config struct {
	Changers []Changer `json:"changers"`
	Drives   []Drive   `json:"drives"`
}

// Changer represents a changer entry from PBS tape.cfg.
type Changer struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	ExportSlots string `json:"export-slots,omitempty"`
}

// Drive represents a drive entry from PBS tape.cfg.
type Drive struct {
	Name            string `json:"name"`
	Path            string `json:"path"`
	Changer         string `json:"changer,omitempty"`
	ChangerDrivenum int    `json:"changer-drivenum,omitempty"`
}

const pbsTapeCfgPath = "/etc/proxmox-backup/tape.cfg"

// ReadConfig reads and parses the PBS tape.cfg SectionConfig file.
func ReadConfig() (*Config, error) {
	file, err := os.Open(pbsTapeCfgPath)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{}, nil
		}
		return nil, fmt.Errorf("open tape.cfg: %w", err)
	}
	defer func() { _ = file.Close() }()

	sections := parseSectionConfig(file)

	cfg := &Config{}
	for _, sec := range sections {
		switch sec.sectionType {
		case "changer":
			cfg.Changers = append(cfg.Changers, Changer{
				Name:        sec.sectionName,
				Path:        sec.get("path"),
				ExportSlots: sec.get("export-slots"),
			})
		case "lto":
			cfg.Drives = append(cfg.Drives, Drive{
				Name:            sec.sectionName,
				Path:            sec.get("path"),
				Changer:         sec.get("changer"),
				ChangerDrivenum: parseDrivenum(sec.get("changer-drivenum")),
			})
		}
	}
	return cfg, nil
}

func parseDrivenum(s string) int {
	if s == "" {
		return 0
	}
	var v int
	if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
		return 0
	}
	return v
}

type pbsSection struct {
	sectionType string
	sectionName string
	properties  map[string]string
}

func (s *pbsSection) get(key string) string {
	return s.properties[key]
}

// parseSectionConfig parses Proxmox SectionConfig format:
//
//	type: name
//	    key value
//	    key value
//
//	type: name
//	    key "quoted value"
func parseSectionConfig(f *os.File) []pbsSection {
	var sections []pbsSection
	var cur *pbsSection

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// Section header: "type: name" (not indented)
		if !strings.HasPrefix(line, "\t") && !strings.HasPrefix(line, " ") {
			parts := strings.SplitN(trimmed, ":", 2)
			if len(parts) == 2 {
				if cur != nil {
					sections = append(sections, *cur)
				}
				cur = &pbsSection{
					sectionType: strings.TrimSpace(parts[0]),
					sectionName: strings.TrimSpace(parts[1]),
					properties:  make(map[string]string),
				}
			}
			continue
		}

		// Property line (indented): "key value"
		if cur == nil {
			continue
		}

		trimmed = strings.TrimSpace(line)
		idx := strings.IndexFunc(trimmed, func(r rune) bool {
			return r == ' ' || r == '\t'
		})
		if idx < 0 {
			continue
		}

		key := trimmed[:idx]
		val := strings.TrimSpace(trimmed[idx+1:])

		if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
			val = val[1 : len(val)-1]
		}

		cur.properties[key] = val
	}

	if cur != nil {
		sections = append(sections, *cur)
	}

	return sections
}
