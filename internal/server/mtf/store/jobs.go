package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/store/mtfquery"
)

const mtfJobMaxAttempts = 100

func (d *Database) generateUniqueMtfJobID(ctx context.Context, base string) (string, error) {
	if base == "" {
		base = "tape-job"
	}
	for idx := range mtfJobMaxAttempts {
		var id string
		if idx == 0 {
			id = base
		} else {
			id = fmt.Sprintf("%s-%d", base, idx)
		}
		_, err := d.readQueries.MtfJobExists(ctx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return id, nil
		}
		if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("failed to generate unique tape job id after %d attempts", mtfJobMaxAttempts)
}

func (d *Database) CreateMtfJob(ctx context.Context, j MTFJob) (MTFJob, error) {
	if j.Datastore == "" {
		return MTFJob{}, errors.New("datastore is required")
	}
	if j.SourceKind == "" {
		return MTFJob{}, errors.New("source_kind is required")
	}
	if j.SourceRef == "" {
		return MTFJob{}, errors.New("source_ref is required")
	}
	if j.RetryInterval <= 0 {
		j.RetryInterval = 1
	}
	if j.Retry < 0 {
		j.Retry = 0
	}

	var err error
	if j.ID == "" {
		j.ID, err = d.generateUniqueMtfJobID(ctx, j.ID)
		if err != nil {
			return MTFJob{}, err
		}
	} else {
		_, err := d.readQueries.MtfJobExists(ctx, j.ID)
		if err == nil {
			return MTFJob{}, fmt.Errorf("tape job %s already exists", j.ID)
		}
	}

	if err := d.queries.CreateMtfJob(ctx, mtfJobCreateParams(j)); err != nil {
		return MTFJob{}, err
	}
	return d.GetMtfJob(ctx, j.ID)
}

func (d *Database) UpdateMtfJob(ctx context.Context, j MTFJob) error {
	return d.queries.UpdateMtfJob(ctx, mtfJobUpdateParams(j))
}

func (d *Database) GetMtfJob(ctx context.Context, id string) (MTFJob, error) {
	r, err := d.readQueries.GetMtfJob(ctx, id)
	if err != nil {
		return MTFJob{}, mapErr(err, "tape job")
	}
	j := mtfJobFromRow(r)
	j.SourceLabel = d.sourceLabel(ctx, j.SourceKind, j.SourceRef)
	return j, nil
}

func (d *Database) ListMtfJobs(ctx context.Context) ([]MTFJob, error) {
	rows, err := d.readQueries.ListAllMtfJobs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MTFJob, 0, len(rows))
	for _, r := range rows {
		j := mtfJobFromRow(r)
		j.SourceLabel = d.sourceLabel(ctx, j.SourceKind, j.SourceRef)
		out = append(out, j)
	}
	return out, nil
}

func (d *Database) ListQueuedMtfJobs(ctx context.Context) ([]MTFJob, error) {
	rows, err := d.readQueries.ListQueuedMtfJobs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]MTFJob, 0, len(rows))
	for _, r := range rows {
		out = append(out, mtfJobFromRow(r))
	}
	return out, nil
}

func (d *Database) DeleteMtfJob(ctx context.Context, id string) error {
	_, err := d.queries.DeleteMtfJob(ctx, id)
	return err
}

func (d *Database) MtfJobExists(ctx context.Context, id string) (bool, error) {
	_, err := d.readQueries.MtfJobExists(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (d *Database) UpdateMtfJobHistory(ctx context.Context, id string, h JobHistory, currentPID string) error {
	return d.queries.UpdateMtfJobHistory(ctx, mtfquery.UpdateMtfJobHistoryParams{
		CurrentPid:            sql.NullString{String: currentPID, Valid: currentPID != ""},
		LastRunUpid:           sql.NullString{String: h.LastRunUpid, Valid: h.LastRunUpid != ""},
		LastSuccessfulUpid:    sql.NullString{String: h.LastSuccessfulUpid, Valid: h.LastSuccessfulUpid != ""},
		LastRunStatus:         sql.NullInt64{Int64: int64(h.LastRunStatus), Valid: true},
		RetryCount:            sql.NullInt64{Int64: int64(h.RetryCount), Valid: true},
		LastRunStarttime:      sql.NullInt64{Int64: h.LastRunStarttime, Valid: true},
		LastRunEndtime:        sql.NullInt64{Int64: h.LastRunEndtime, Valid: true},
		LastSuccessfulEndtime: sql.NullInt64{Int64: h.LastSuccessfulEndtime, Valid: true},
		Duration:              sql.NullInt64{Int64: h.Duration, Valid: true},
		ID:                    id,
	})
}

func (d *Database) sourceLabel(ctx context.Context, kind, ref string) string {
	switch kind {
	case "cartridge":
		c, err := d.GetCartridge(ctx, ref)
		if err != nil {
			return ref
		}
		return c.Barcode
	case "family":
		fam, err := d.GetMediaFamily(ctx, toInt64(ref))
		if err != nil {
			return ref
		}
		return fam.Name
	case "dataset":
		ds, err := d.GetDataSet(ctx, toInt64(ref))
		if err != nil {
			return ref
		}
		return ds.MachineName
	}
	return ref
}

func mtfJobFromRow(r mtfquery.MtfJob) MTFJob {
	return MTFJob{
		ID:                r.ID,
		SourceKind:        r.SourceKind,
		SourceRef:         r.SourceRef,
		Datastore:         r.Datastore,
		Namespace:         ns(r.Namespace),
		Schedule:          ns(r.Schedule),
		Comment:           ns(r.Comment),
		NotificationMode:  ns(r.NotificationMode),
		Spanning:          nb(r.Spanning),
		OverwriteMappings: nb(r.OverwriteMappings),
		Changer:           ns(r.Changer),
		Drive:             ns(r.Drive),
		Retry:             ni(r.Retry),
		RetryInterval:     ni(r.RetryInterval),
		CurrentPID:        ns(r.CurrentPid),
		History: JobHistory{
			LastRunUpid:           ns(r.LastRunUpid),
			LastRunStarttime:      ni64(r.LastRunStarttime),
			LastRunState:          jobStatusState(JobStatus(ni64(r.LastRunStatus))),
			LastRunStatus:         JobStatus(ni64(r.LastRunStatus)),
			LastRunEndtime:        ni64(r.LastRunEndtime),
			LastSuccessfulEndtime: ni64(r.LastSuccessfulEndtime),
			LastSuccessfulUpid:    ns(r.LastSuccessfulUpid),
			RetryCount:            ni(r.RetryCount),
			Duration:              ni64(r.Duration),
		},
		CreatedAt: ni64(r.CreatedAt),
	}
}

func jobStatusState(s JobStatus) string {
	switch s {
	case 1:
		return "OK"
	case 2:
		return "WARNINGS"
	case 3:
		return "FAILED"
	case 4:
		return "operation canceled"
	case 0:
		return ""
	}
	return ""
}

func mtfJobCreateParams(j MTFJob) mtfquery.CreateMtfJobParams {
	return mtfquery.CreateMtfJobParams{
		ID:                 j.ID,
		SourceKind:         j.SourceKind,
		SourceRef:          j.SourceRef,
		Datastore:          j.Datastore,
		Namespace:          sql.NullString{String: j.Namespace, Valid: j.Namespace != ""},
		Schedule:           sql.NullString{String: j.Schedule, Valid: j.Schedule != ""},
		Comment:            sql.NullString{String: j.Comment, Valid: j.Comment != ""},
		NotificationMode:   sql.NullString{String: j.NotificationMode, Valid: j.NotificationMode != ""},
		Spanning:           sql.NullInt64{Int64: boolToInt(j.Spanning), Valid: true},
		OverwriteMappings:  sql.NullInt64{Int64: boolToInt(j.OverwriteMappings), Valid: true},
		Changer:            sql.NullString{String: j.Changer, Valid: j.Changer != ""},
		Drive:              sql.NullString{String: j.Drive, Valid: j.Drive != ""},
		CurrentPid:         sql.NullString{},
		LastRunUpid:        sql.NullString{},
		LastSuccessfulUpid: sql.NullString{},
		LastRunStatus:      sql.NullInt64{},
		RetryCount:         sql.NullInt64{},
		Retry:              sql.NullInt64{Int64: int64(j.Retry), Valid: true},
		RetryInterval:      sql.NullInt64{Int64: int64(j.RetryInterval), Valid: true},
	}
}

func mtfJobUpdateParams(j MTFJob) mtfquery.UpdateMtfJobParams {
	return mtfquery.UpdateMtfJobParams{
		SourceKind:            j.SourceKind,
		SourceRef:             j.SourceRef,
		Datastore:             j.Datastore,
		Namespace:             sql.NullString{String: j.Namespace, Valid: j.Namespace != ""},
		Schedule:              sql.NullString{String: j.Schedule, Valid: j.Schedule != ""},
		Comment:               sql.NullString{String: j.Comment, Valid: j.Comment != ""},
		NotificationMode:      sql.NullString{String: j.NotificationMode, Valid: j.NotificationMode != ""},
		Spanning:              sql.NullInt64{Int64: boolToInt(j.Spanning), Valid: true},
		OverwriteMappings:     sql.NullInt64{Int64: boolToInt(j.OverwriteMappings), Valid: true},
		Changer:               sql.NullString{String: j.Changer, Valid: j.Changer != ""},
		Drive:                 sql.NullString{String: j.Drive, Valid: j.Drive != ""},
		LastRunUpid:           sql.NullString{String: j.History.LastRunUpid, Valid: j.History.LastRunUpid != ""},
		LastSuccessfulUpid:    sql.NullString{String: j.History.LastSuccessfulUpid, Valid: j.History.LastSuccessfulUpid != ""},
		LastRunStatus:         sql.NullInt64{Int64: int64(j.History.LastRunStatus), Valid: true},
		RetryCount:            sql.NullInt64{Int64: int64(j.History.RetryCount), Valid: true},
		Retry:                 sql.NullInt64{Int64: int64(j.Retry), Valid: true},
		RetryInterval:         sql.NullInt64{Int64: int64(j.RetryInterval), Valid: true},
		LastRunStarttime:      sql.NullInt64{Int64: j.History.LastRunStarttime, Valid: true},
		LastRunEndtime:        sql.NullInt64{Int64: j.History.LastRunEndtime, Valid: true},
		LastSuccessfulEndtime: sql.NullInt64{Int64: j.History.LastSuccessfulEndtime, Valid: true},
		Duration:              sql.NullInt64{Int64: j.History.Duration, Valid: true},
		ID:                    j.ID,
	}
}

func toInt64(s string) int64 {
	return ToInt64(s)
}

func ToInt64(s string) int64 {
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		log.Error(err, "")
	}
	return n
}
