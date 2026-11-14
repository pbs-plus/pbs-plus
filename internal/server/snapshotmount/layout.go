//go:build linux

package snapshotmount

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
)

const maxLayoutPaths = 32768

// PathLayout applies one directory layout to local mounts and shared NFS/S3 exports.
type PathLayout struct {
	Template         string `json:"template,omitempty"`
	Fallback         string `json:"fallback,omitempty"`
	Selection        string `json:"selection,omitempty"`
	ResolverURL      string `json:"resolver_url,omitempty"`
	ResolverTokenEnv string `json:"resolver_token_env,omitempty"`
	WindowsNames     bool   `json:"windows_names,omitempty"`
}

type PlannedMount struct {
	Group      NamespaceGroup `json:"group"`
	BackupTime string         `json:"backup_time"`
	FileName   string         `json:"file_name"`
	Paths      []string       `json:"paths"`
}

func validateLayout(p Profile) error {
	l := p.Layout
	if l == nil {
		return nil
	}
	if p.Mode == ModeRW {
		return errors.New("custom layouts require read-only mounts")
	}
	if p.Outpost != "" {
		o, found, err := outpost.LoadOutpost(p.Outpost)
		if err != nil {
			return err
		}
		if !found {
			return errors.New("layout outpost does not exist")
		}
		if o.Type != outpost.TypeNFS && o.Type != outpost.TypeS3Export {
			return errors.New("custom layouts require a local mount, NFS or S3 snapshot export")
		}
	}
	if l.Selection != "" && l.Selection != "group" && l.Selection != "namespace" {
		return errors.New("layout selection must be group or namespace")
	}
	if l.Template == "" {
		return errors.New("layout template is required")
	}
	for _, template := range []string{l.Template, l.Fallback} {
		if template == "" {
			continue
		}
		if _, err := renderProfilePath(l, template, layoutVariables(p, PlannedMount{Group: NamespaceGroup{Namespace: "example", BackupType: "host", BackupID: "sample"}, FileName: "root.mpxar.didx", BackupTime: "2026-01-01T00:00:00Z"}, "Project")); err != nil {
			return err
		}
	}
	if strings.Contains(l.Fallback, "{resolve}") {
		return errors.New("layout fallback cannot contain {resolve}")
	}
	if l.ResolverURL == "" {
		if strings.Contains(l.Template, "{resolve}") || l.ResolverTokenEnv != "" {
			return errors.New("layout resolver URL is required")
		}
		return nil
	}
	if !strings.Contains(l.Template, "{resolve}") {
		return errors.New("resolver layout template must contain {resolve}")
	}
	u, err := url.Parse(l.ResolverURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.Fragment != "" {
		return errors.New("resolver URL must be HTTP(S), without userinfo or a fragment")
	}
	if l.ResolverTokenEnv != "" {
		for i, r := range l.ResolverTokenEnv {
			if !(r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9') {
				return errors.New("resolver token must name an environment variable")
			}
		}
		if u.Scheme != "https" {
			return errors.New("authenticated resolvers require HTTPS")
		}
	}
	return nil
}

func layoutSegment(s string, windows bool) string {
	s = strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || unicode.IsControl(r) || windows && strings.ContainsRune(`<>:"|?*`, r) {
			return '_'
		}
		return r
	}, s)
	if windows {
		s = strings.TrimRight(s, " .")
		base := strings.ToUpper(strings.SplitN(s, ".", 2)[0])
		if base == "CON" || base == "PRN" || base == "AUX" || base == "NUL" || len(base) == 4 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) && base[3] >= '1' && base[3] <= '9' {
			s = "_" + s
		}
	}
	if s == "" || s == "." || s == ".." {
		return "_"
	}
	return s
}

func layoutVariables(p Profile, m PlannedMount, resolved string) map[string]string {
	segment := func(s string) string { return layoutSegment(s, p.Layout.WindowsNames) }
	namespace := func(s string) string {
		parts := strings.Split(s, "/")
		for i := range parts {
			parts[i] = segment(parts[i])
		}
		return strings.Join(parts, "/")
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(m.Group.Namespace, p.Namespace), "/")
	archive := strings.TrimSuffix(strings.TrimSuffix(m.FileName, ".didx"), ".mpxar")
	archive = strings.TrimSuffix(archive, ".pxar")
	return map[string]string{
		"ns": namespace(m.Group.Namespace), "ns.rel": namespace(rel), "ns.leaf": segment(path.Base(m.Group.Namespace)),
		"type": segment(m.Group.BackupType), "id": segment(m.Group.BackupID), "archive": segment(archive),
		"time": segment(strings.ReplaceAll(m.BackupTime, ":", "-")), "resolve": segment(resolved),
	}
}

func renderLayout(template string, vars map[string]string) (string, error) {
	if len(template) > 1024 {
		return "", errors.New("layout template exceeds 1024 bytes")
	}
	var b strings.Builder
	for len(template) > 0 {
		start := strings.IndexByte(template, '{')
		if start < 0 {
			b.WriteString(template)
			break
		}
		b.WriteString(template[:start])
		template = template[start+1:]
		end := strings.IndexByte(template, '}')
		if end < 0 {
			return "", errors.New("unclosed layout token")
		}
		value, ok := vars[template[:end]]
		if !ok {
			return "", errors.New("unknown layout token")
		}
		b.WriteString(value)
		template = template[end+1:]
	}
	result := b.String()
	if len(result) > 4096 || strings.ContainsAny(result, "{}") || result == "" {
		return "", errors.New("invalid generated layout path")
	}
	for part := range strings.SplitSeq(result, "/") {
		if len(part) > 255 || strings.IndexFunc(part, unicode.IsControl) >= 0 {
			return "", errors.New("invalid generated layout path segment")
		}
	}
	if err := ValidateSubPath(result); err != nil {
		return "", err
	}
	return result, nil
}

func renderProfilePath(l *PathLayout, template string, vars map[string]string) (string, error) {
	result, err := renderLayout(template, vars)
	if err != nil || !l.WindowsNames {
		return result, err
	}
	parts := strings.Split(result, "/")
	for i, part := range parts {
		parts[i] = layoutSegment(part, true)
		if len(parts[i]) > 255 {
			return "", errors.New("generated Windows path segment exceeds 255 bytes")
		}
	}
	return strings.Join(parts, "/"), nil
}

var layoutHTTPClient = &http.Client{
	Timeout:       5 * time.Second,
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
}

func resolveLayout(ctx context.Context, p Profile, m PlannedMount) (values []string, err error) {
	payload := struct {
		Datastore  string         `json:"datastore"`
		Namespace  string         `json:"namespace"`
		Group      NamespaceGroup `json:"group"`
		BackupTime string         `json:"backup_time"`
		Archive    string         `json:"archive"`
	}{p.Datastore, m.Group.Namespace, m.Group, m.BackupTime, m.FileName}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Layout.ResolverURL, bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("invalid resolver request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if name := p.Layout.ResolverTokenEnv; name != "" {
		token := os.Getenv(name)
		if token == "" {
			return nil, errors.New("resolver token environment variable is unset")
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := layoutHTTPClient.Do(req)
	if err != nil {
		return nil, errors.New("layout resolver request failed")
	}
	defer func() { err = errors.Join(err, resp.Body.Close()) }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("layout resolver returned HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(body) > 65536 {
		return nil, errors.New("layout resolver response exceeds limit or cannot be read")
	}
	var result struct {
		Values []string `json:"values"`
	}
	if err := json.Unmarshal(body, &result); err != nil || result.Values == nil || len(result.Values) > 64 {
		return nil, errors.New("layout resolver must return a values array with at most 64 strings")
	}
	for _, value := range result.Values {
		if value == "" || len(value) > 255 {
			return nil, errors.New("layout resolver values must be nonempty segments of at most 255 bytes")
		}
	}
	return result.Values, nil
}

// PlanProfile previews the same bounded plan used by reconciliation, without mounting or changing state.
func PlanProfile(ctx context.Context, p Profile) ([]PlannedMount, error) {
	if err := ValidateProfile(p); err != nil {
		return nil, err
	}
	root := mustStoreRoot(p)
	if root == "" {
		return nil, errors.New("datastore has no path")
	}
	return planProfileIn(ctx, p, root)
}

func planProfileIn(ctx context.Context, p Profile, root string) ([]PlannedMount, error) {
	if err := validateLayout(p); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	groups, err := ListNamespaceGroups(root, p.Namespace)
	if err != nil {
		return nil, err
	}
	if len(groups) > maxLayoutPaths {
		return nil, errors.New("profile exceeds mount plan limit")
	}
	legacy := planBatch(groups, p.Namespace)
	plans := make([]PlannedMount, 0, len(groups))
	for _, g := range groups {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		bt, file, err := LatestSnapshotIn(root, g.Namespace, g.BackupType, g.BackupID)
		if err != nil {
			if errors.Is(err, errNoMountableSnapshot) {
				continue
			}
			return nil, err
		}
		plans = append(plans, PlannedMount{Group: g, BackupTime: bt, FileName: file})
	}
	if p.Layout != nil && p.Layout.Selection == "namespace" {
		selected := make(map[string]int, len(plans))
		kept := plans[:0]
		for _, m := range plans {
			if i, ok := selected[m.Group.Namespace]; ok {
				if m.BackupTime > kept[i].BackupTime {
					kept[i] = m
				}
			} else {
				selected[m.Group.Namespace] = len(kept)
				kept = append(kept, m)
			}
		}
		plans = kept
	}
	var allPaths []string
	totalPathBytes := 0
	for i := range plans {
		m := &plans[i]
		if p.Layout == nil {
			m.Paths = []string{legacy[groupKeyOf(m.Group.Namespace, m.Group.BackupType, m.Group.BackupID)]}
		} else {
			values := []string{""}
			template := p.Layout.Template
			if p.Layout.ResolverURL != "" {
				values, err = resolveLayout(ctx, p, *m)
				if err != nil {
					return nil, err
				}
				if len(values) == 0 {
					template = p.Layout.Fallback
					if template == "" {
						template = "Uncategorized/{ns.leaf}"
					}
					values = []string{""}
				}
			}
			vars := layoutVariables(p, *m, "")
			for _, value := range values {
				vars["resolve"] = layoutSegment(value, p.Layout.WindowsNames)
				name, err := renderProfilePath(p.Layout, template, vars)
				if err != nil {
					return nil, err
				}
				m.Paths = append(m.Paths, name)
			}
			sort.Strings(m.Paths)
			m.Paths = slices.Compact(m.Paths)
		}
		for _, name := range m.Paths {
			totalPathBytes += len(name)
		}
		allPaths = append(allPaths, m.Paths...)
		if len(allPaths) > maxLayoutPaths || totalPathBytes > 8<<20 {
			return nil, errors.New("profile exceeds path limit")
		}
	}
	seen := make(map[string]struct{}, len(allPaths))
	for _, name := range allPaths {
		key := name
		if p.Layout != nil && p.Layout.WindowsNames {
			key = strings.ToLower(key)
		}
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("layout path collision at %q", name)
		}
		seen[key] = struct{}{}
	}
	for key := range seen {
		for parent := path.Dir(key); parent != "."; parent = path.Dir(parent) {
			if _, ok := seen[parent]; ok {
				return nil, fmt.Errorf("layout path overlap at %q", key)
			}
		}
	}
	return plans, nil
}
