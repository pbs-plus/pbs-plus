package pbsapi

import (
	"context"
	"net/http"
	"net/url"
)

type SnapshotFile struct {
	Filename string `json:"filename"`
}

type SnapshotVerification struct {
	State string `json:"state"`
	UPID  string `json:"upid"`
}

type Snapshot struct {
	BackupType   string                `json:"backup-type"`
	BackupID     string                `json:"backup-id"`
	BackupTime   int64                 `json:"backup-time"`
	Files        []SnapshotFile        `json:"files"`
	Size         int64                 `json:"size"`
	Comment      string                `json:"comment"`
	Verification *SnapshotVerification `json:"verification"`
	Protected    bool                  `json:"protected"`
}

func (c *Client) HostSnapshots(ctx context.Context, auth *http.Request, store, ns string) ([]Snapshot, error) {
	query := url.Values{"backup-type": {"host"}}
	if ns != "" {
		query.Set("ns", ns)
	}
	var snapshots []Snapshot
	err := c.Get(ctx, auth, "/api2/json/admin/datastore/"+url.PathEscape(store)+"/snapshots", query, &snapshots)
	return snapshots, err
}
