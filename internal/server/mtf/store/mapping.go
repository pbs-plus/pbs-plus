package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store/mtfquery"
)

type compiledRule struct {
	id       int64
	re       *regexp.Regexp
	tmpl     string
	namedCap []string
	tokens   []string
}

type Mapper struct {
	db *Database

	mu    sync.RWMutex
	rules []compiledRule
	dirty bool
}

func NewMapper(db *Database) *Mapper {
	return &Mapper{db: db, dirty: true}
}

func (m *Mapper) Invalidate() {
	m.mu.Lock()
	m.dirty = true
	m.mu.Unlock()
}

func (m *Mapper) load(ctx context.Context) error {
	rows, err := m.db.readQueries.ListEnabledMappings(ctx)
	if err != nil {
		return err
	}
	rules := make([]compiledRule, 0, len(rows))
	for _, r := range rows {
		cr, err := compileRule(r)
		if err != nil {
			return fmt.Errorf("mapping rule %d: %w", r.ID, err)
		}
		rules = append(rules, cr)
	}
	m.mu.Lock()
	m.rules = rules
	m.dirty = false
	m.mu.Unlock()
	return nil
}

func compileRule(r mtfquery.NamespaceMapping) (compiledRule, error) {
	cr := compiledRule{
		id:   r.ID,
		tmpl: r.Template,
	}
	if pat := ns(r.MatchRegex); pat != "" {
		re, err := regexp.Compile(pat)
		if err != nil {
			return cr, fmt.Errorf("invalid regex %q: %w", pat, err)
		}
		cr.re = re
		cr.namedCap = re.SubexpNames()
	}
	cr.tokens = scanTokens(r.Template)
	return cr, nil
}

func (m *Mapper) Map(ctx context.Context, vol DataSetVolume) (string, error) {
	m.mu.RLock()
	dirty := m.dirty
	rules := m.rules
	m.mu.RUnlock()

	if dirty {
		if err := m.load(ctx); err != nil {
			return "", err
		}
		m.mu.RLock()
		rules = m.rules
		m.mu.RUnlock()
	}

	tok := volumeTokens(vol)
	for _, r := range rules {
		ns, ok, err := r.apply(tok, vol)
		if err != nil {
			return "", err
		}
		if ok {
			return sanitizeNS(ns), nil
		}
	}
	return "", nil
}

func (m *Mapper) MapAll(ctx context.Context, vols []DataSetVolume) ([]string, error) {
	m.mu.RLock()
	dirty := m.dirty
	rules := m.rules
	m.mu.RUnlock()

	if dirty {
		if err := m.load(ctx); err != nil {
			return nil, err
		}
		m.mu.RLock()
		rules = m.rules
		m.mu.RUnlock()
	}

	out := make([]string, len(vols))
	for i, vol := range vols {
		tok := volumeTokens(vol)
		mapped := ""
		for _, r := range rules {
			ns, ok, err := r.apply(tok, vol)
			if err != nil {
				return nil, err
			}
			if ok {
				mapped = sanitizeNS(ns)
				break
			}
		}
		out[i] = mapped
	}
	return out, nil
}

func (r compiledRule) apply(tok map[string]string, vol DataSetVolume) (string, bool, error) {
	if r.re != nil {
		matches := r.re.FindStringSubmatchIndex(vol.Device)
		if matches == nil {
			machineMatches := r.re.FindStringSubmatchIndex(vol.MachineName)
			if machineMatches == nil {
				return "", false, nil
			}
			matches = machineMatches
		}
		expanded := r.re.ExpandString(nil, r.tmpl, vol.Device, matches)
		out := string(expanded)
		if len(r.tokens) > 0 {
			out = applyTokens(out, tok)
		}
		return out, true, nil
	}
	return applyTokens(r.tmpl, tok), true, nil
}

func applyTokens(tmpl string, tok map[string]string) string {
	var b strings.Builder
	b.Grow(len(tmpl))
	i := 0
	for i < len(tmpl) {
		if tmpl[i] == '{' {
			if end := strings.IndexByte(tmpl[i:], '}'); end >= 0 {
				key := tmpl[i+1 : i+end]
				if v, ok := tok[key]; ok {
					b.WriteString(v)
				}
				i += end + 1
				continue
			}
		}
		b.WriteByte(tmpl[i])
		i++
	}
	return b.String()
}

func scanTokens(tmpl string) []string {
	var toks []string
	i := 0
	for i < len(tmpl) {
		if tmpl[i] == '{' {
			if end := strings.IndexByte(tmpl[i:], '}'); end >= 0 {
				toks = append(toks, tmpl[i+1:i+end])
				i += end + 1
				continue
			}
		}
		i++
	}
	return toks
}

func volumeTokens(vol DataSetVolume) map[string]string {
	machine := vol.MachineName
	host := machine
	short := machine
	label := ""
	if idx := strings.IndexByte(machine, '.'); idx > 0 {
		short = machine[:idx]
	}
	if idx := strings.IndexByte(machine, '.'); idx > 0 {
		host = machine[:idx]
	}
	if hy := strings.IndexByte(short, '-'); hy > 0 {
		label = short[:hy]
	}
	drive := driveLetter(vol.Device)
	return map[string]string{
		"machine":       machine,
		"machine.short": short,
		"machine.host":  host,
		"machine.label": label,
		"device":        vol.Device,
		"drive":         drive,
		"label":         vol.VolumeLabel,
	}
}

func driveLetter(device string) string {
	trimmed := strings.Trim(device, "\\/")
	if idx := strings.LastIndexByte(trimmed, '\\'); idx >= 0 {
		trimmed = trimmed[idx+1:]
	}
	trimmed = strings.TrimSuffix(trimmed, ":")
	trimmed = strings.TrimSuffix(trimmed, "$")
	if trimmed != "" {
		return trimmed
	}
	return ""
}

func sanitizeNS(ns string) string {
	ns = strings.TrimSpace(ns)
	ns = strings.Trim(ns, "/")
	if ns == "" {
		return ns
	}
	parts := strings.Split(ns, "/")
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || p == "." || p == ".." {
			p = "_"
		}
		parts[i] = p
	}
	return strings.Join(parts, "/")
}

func (d *Database) ListMappings(ctx context.Context) ([]NamespaceMapping, error) {
	rows, err := d.readQueries.ListMappings(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]NamespaceMapping, 0, len(rows))
	for _, r := range rows {
		out = append(out, mappingFromRow(r))
	}
	return out, nil
}

func (d *Database) GetMapping(ctx context.Context, id int64) (NamespaceMapping, error) {
	r, err := d.readQueries.GetMapping(ctx, id)
	if err != nil {
		return NamespaceMapping{}, mapErr(err, "mapping")
	}
	return mappingFromRow(r), nil
}

func (d *Database) CreateMapping(ctx context.Context, m NamespaceMapping) (int64, error) {
	if m.Template == "" {
		return 0, ErrInvalidMapping
	}
	id, err := d.queries.CreateMapping(ctx, mtfquery.CreateMappingParams{
		Name:       sql.NullString{String: m.Name, Valid: m.Name != ""},
		Priority:   sql.NullInt64{Int64: int64(m.Priority), Valid: true},
		MatchRegex: sql.NullString{String: m.MatchRegex, Valid: m.MatchRegex != ""},
		Template:   m.Template,
		IsDefault:  sql.NullInt64{Int64: boolToInt(m.IsDefault), Valid: true},
		Enabled:    sql.NullInt64{Int64: boolToInt(m.Enabled), Valid: true},
		Comment:    sql.NullString{String: m.Comment, Valid: m.Comment != ""},
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (d *Database) UpdateMapping(ctx context.Context, m NamespaceMapping) error {
	if m.Template == "" {
		return ErrInvalidMapping
	}
	return d.queries.UpdateMapping(ctx, mtfquery.UpdateMappingParams{
		Name:       sql.NullString{String: m.Name, Valid: m.Name != ""},
		Priority:   sql.NullInt64{Int64: int64(m.Priority), Valid: true},
		MatchRegex: sql.NullString{String: m.MatchRegex, Valid: m.MatchRegex != ""},
		Template:   m.Template,
		IsDefault:  sql.NullInt64{Int64: boolToInt(m.IsDefault), Valid: true},
		Enabled:    sql.NullInt64{Int64: boolToInt(m.Enabled), Valid: true},
		Comment:    sql.NullString{String: m.Comment, Valid: m.Comment != ""},
		ID:         m.ID,
	})
}

func (d *Database) DeleteMapping(ctx context.Context, id int64) error {
	_, err := d.queries.DeleteMapping(ctx, id)
	return err
}

func (d *Database) MappingExists(ctx context.Context, id int64) (bool, error) {
	_, err := d.readQueries.MappingExists(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func mappingFromRow(r mtfquery.NamespaceMapping) NamespaceMapping {
	return NamespaceMapping{
		ID:         r.ID,
		Name:       ns(r.Name),
		Priority:   ni(r.Priority),
		MatchRegex: ns(r.MatchRegex),
		Template:   r.Template,
		IsDefault:  nb(r.IsDefault),
		Enabled:    nb(r.Enabled),
		Comment:    ns(r.Comment),
		CreatedAt:  ni64(r.CreatedAt),
	}
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
