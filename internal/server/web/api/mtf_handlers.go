//go:build linux

package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/server/mtfinv"
	"github.com/pbs-plus/pbs-plus/internal/server/mtfstore"
	jobrpc "github.com/pbs-plus/pbs-plus/internal/server/rpc"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func mtfStore(st *store.Store) *mtfstore.Database {
	if st == nil {
		return nil
	}
	return st.MtfStore
}

// --- MTF jobs ---

func ExtJsMtfJobRunHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		jobIDs := r.URL.Query()["job"]
		if len(jobIDs) == 0 {
			http.Error(w, "Missing job parameter(s)", http.StatusBadRequest)
			return
		}

		decoded := make([]string, 0, len(jobIDs))
		for _, id := range jobIDs {
			d := validate.DecodePath(id)
			if err := validate.ValidateJobId(d); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			decoded = append(decoded, d)
		}

		stop := r.Method == http.MethodDelete

		go func() {
			conn, err := net.DialTimeout("unix", conf.JobMutateSocketPath, 5*time.Minute)
			if err != nil {
				syslog.L.Error(err).WithField("mtfJobs", decoded).Write()
				return
			}
			rpcClient := rpc.NewClient(conn)
			defer func() { _ = rpcClient.Close() }()

			for _, id := range decoded {
				args := &jobrpc.MtfJobQueueArgs{JobID: id, Stop: stop, Web: true}
				var reply jobrpc.QueueReply
				if err := rpcClient.Call("JobRPCService.MtfQueue", args, &reply); err != nil {
					syslog.L.Error(err).WithField("mtfJobID", id).Write()
					continue
				}
				if reply.Status != 200 {
					syslog.L.Error(fmt.Errorf("%s", reply.Message)).WithField("mtfJobID", id).Write()
				}
			}
		}()

		writeJSON(w, map[string]any{"data": nil, "success": true})
	}
}

func ExtJsMtfJobHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			jobs, err := ms.ListMtfJobs(r.Context())
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			out := make([]flatMtfJob, 0, len(jobs))
			for _, j := range jobs {
				out = append(out, flattenMtfJob(j))
			}
			writeJSON(w, map[string]any{"data": out, "success": true})

		case http.MethodPost:
			if err := r.ParseForm(); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			job, err := mtfJobFromForm(r)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			created, err := ms.CreateMtfJob(r.Context(), job)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			ApplyJobBatchAssignment(storeInstance, "backup", created.ID, r.FormValue("notification-batch"))
			writeJSON(w, map[string]any{"data": flattenMtfJob(created), "success": true})

		default:
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}
	}
}

func ExtJsMtfJobSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}

		id := validate.DecodePath(r.PathValue("job"))
		if err := validate.ValidateJobId(id); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		switch r.Method {
		case http.MethodGet:
			job, err := ms.GetMtfJob(r.Context(), id)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			writeJSON(w, map[string]any{"data": flattenMtfJob(job), "success": true})

		case http.MethodPut:
			job, err := ms.GetMtfJob(r.Context(), id)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if err := r.ParseForm(); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			updated, err := mtfJobMergeForm(job, r)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if err := ms.UpdateMtfJob(r.Context(), updated); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			writeJSON(w, map[string]any{"data": flattenMtfJob(updated), "success": true})

		case http.MethodDelete:
			if err := ms.DeleteMtfJob(r.Context(), id); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			writeJSON(w, map[string]any{"data": nil, "success": true})
		}
	}
}

func ExtJsMtfJobUPIDsHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		id := validate.DecodePath(r.PathValue("job"))
		if err := validate.ValidateJobId(id); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		job, err := ms.GetMtfJob(r.Context(), id)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}
		upids := []string{}
		if job.History.LastRunUpid != "" {
			upids = append(upids, job.History.LastRunUpid)
		}
		writeJSON(w, map[string]any{"data": upids, "success": true})
	}
}

func mtfJobFromForm(r *http.Request) (mtfstore.MTFJob, error) {
	j := mtfstore.MTFJob{
		ID:                r.FormValue("id"),
		SourceKind:        r.FormValue("source_kind"),
		SourceRef:         r.FormValue("source_ref"),
		Datastore:         r.FormValue("datastore"),
		Namespace:         r.FormValue("namespace"),
		Schedule:          r.FormValue("schedule"),
		Comment:           r.FormValue("comment"),
		NotificationMode:  r.FormValue("notification-mode"),
		Changer:           r.FormValue("changer"),
		Drive:             r.FormValue("drive"),
		Spanning:          r.FormValue("spanning") == "1" || r.FormValue("spanning") == "true",
		OverwriteMappings: r.FormValue("overwrite_mappings") == "1" || r.FormValue("overwrite_mappings") == "true",
		Retry:             atoiDefault(r.FormValue("retry"), 0),
		RetryInterval:     atoiDefault(r.FormValue("retry-interval"), 1),
	}

	if j.SourceKind != "cartridge" && j.SourceKind != "family" && j.SourceKind != "dataset" {
		return j, fmt.Errorf("invalid source_kind %q", j.SourceKind)
	}
	if j.SourceRef == "" {
		return j, fmt.Errorf("source_ref is required")
	}
	if err := validate.ValidateJobId(j.ID); err != nil && j.ID != "" {
		return j, err
	}
	if err := validate.ValidateDatastore(j.Datastore); err != nil {
		return j, err
	}
	if err := validate.ValidateNamespace(j.Namespace); err != nil {
		return j, err
	}
	return j, nil
}

func mtfJobMergeForm(job mtfstore.MTFJob, r *http.Request) (mtfstore.MTFJob, error) {
	if v := r.FormValue("datastore"); v != "" {
		if err := validate.ValidateDatastore(v); err != nil {
			return job, err
		}
		job.Datastore = v
	}
	if v := r.FormValue("namespace"); v != "" {
		if err := validate.ValidateNamespace(v); err != nil {
			return job, err
		}
		job.Namespace = v
	}
	if v := r.FormValue("source_kind"); v != "" {
		if v != "cartridge" && v != "family" && v != "dataset" {
			return job, fmt.Errorf("invalid source_kind %q", v)
		}
		job.SourceKind = v
	}
	if v := r.FormValue("source_ref"); v != "" {
		job.SourceRef = v
	}
	if v := r.FormValue("schedule"); v != "" {
		job.Schedule = v
	}
	if v := r.FormValue("comment"); v != "" {
		job.Comment = v
	}
	if v := r.FormValue("notification-mode"); v != "" {
		job.NotificationMode = v
	}
	if v := r.FormValue("changer"); v != "" {
		job.Changer = v
	}
	if v := r.FormValue("drive"); v != "" {
		job.Drive = v
	}
	if r.FormValue("spanning") != "" {
		job.Spanning = r.FormValue("spanning") == "1" || r.FormValue("spanning") == "true"
	}
	if r.FormValue("overwrite_mappings") != "" {
		job.OverwriteMappings = r.FormValue("overwrite_mappings") == "1" || r.FormValue("overwrite_mappings") == "true"
	}
	if v := r.FormValue("retry"); v != "" {
		job.Retry = atoiDefault(v, 0)
	}
	if v := r.FormValue("retry-interval"); v != "" {
		job.RetryInterval = atoiDefault(v, 1)
	}
	return job, nil
}

// --- MTF changers / drives ---

func ExtJsMtfChangerHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		switch r.Method {
		case http.MethodGet:
			list, err := ms.ListChangers(r.Context())
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			writeJSON(w, map[string]any{"data": list, "success": true})
		case http.MethodPost:
			c, err := mtfChangerFromForm(r)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if err := ms.CreateChanger(r.Context(), c); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			writeJSON(w, map[string]any{"data": c, "success": true})
		default:
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}
	}
}

func ExtJsMtfChangerSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		name := validate.DecodePath(r.PathValue("name"))
		if name == "" {
			WriteErrorResponse(w, fmt.Errorf("invalid changer name"))
			return
		}
		if err := ms.DeleteChanger(r.Context(), name); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		writeJSON(w, map[string]any{"data": nil, "success": true})
	}
}

func ExtJsMtfDriveHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		switch r.Method {
		case http.MethodGet:
			list, err := ms.ListDrives(r.Context())
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			writeJSON(w, map[string]any{"data": list, "success": true})
		case http.MethodPost:
			d, err := mtfDriveFromForm(r)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if err := ms.CreateDrive(r.Context(), d); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			writeJSON(w, map[string]any{"data": d, "success": true})
		default:
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}
	}
}

func ExtJsMtfDriveSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		name := validate.DecodePath(r.PathValue("name"))
		if name == "" {
			WriteErrorResponse(w, fmt.Errorf("invalid drive name"))
			return
		}
		if err := ms.DeleteDrive(r.Context(), name); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		writeJSON(w, map[string]any{"data": nil, "success": true})
	}
}

func mtfChangerFromForm(r *http.Request) (mtfstore.Changer, error) {
	c := mtfstore.Changer{
		Name:    r.FormValue("name"),
		Device:  r.FormValue("device"),
		Comment: r.FormValue("comment"),
	}
	if c.Name == "" || c.Device == "" {
		return c, fmt.Errorf("name and device are required")
	}
	return c, nil
}

func mtfDriveFromForm(r *http.Request) (mtfstore.Drive, error) {
	d := mtfstore.Drive{
		Name:       r.FormValue("name"),
		Device:     r.FormValue("device"),
		Changer:    r.FormValue("changer"),
		DriveIndex: atoiDefault(r.FormValue("drive_index"), 0),
		Comment:    r.FormValue("comment"),
	}
	if d.Name == "" || d.Device == "" {
		return d, fmt.Errorf("name and device are required")
	}
	return d, nil
}

// --- Inventory listing ---

func ExtJsMtfInventoryHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		ctx := r.Context()
		resp := map[string]any{"success": true}
		switch r.URL.Query().Get("type") {
		case "cartridges":
			list, err := ms.ListCartridges(ctx)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			resp["data"] = list
		case "families":
			list, err := ms.ListMediaFamilies(ctx)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			resp["data"] = list
		case "datasets":
			famID, _ := strconv.ParseInt(r.URL.Query().Get("family"), 10, 64)
			var list []mtfstore.DataSet
			var err error
			if famID > 0 {
				list, err = ms.ListDataSetsByFamily(ctx, famID)
			} else {
				list, err = ms.ListAllDataSets(ctx)
			}
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			resp["data"] = list
		default:
			families, err := ms.ListMediaFamilies(ctx)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			cartridges, err := ms.ListCartridges(ctx)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			resp["data"] = map[string]any{
				"families":   families,
				"cartridges": cartridges,
			}
		}
		writeJSON(w, resp)
	}
}

func ExtJsMtfScanHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		if err := r.ParseForm(); err != nil {
			WriteErrorResponse(w, err)
			return
		}

		opts := mtfinv.Options{
			ChangerDevice: r.FormValue("changer"),
			TapeDevice:    r.FormValue("drive"),
			DriveIndex:    atoiDefault(r.FormValue("drive_index"), 0),
			BKFPath:       r.FormValue("bkf_path"),
			Label:         r.FormValue("label"),
		}

		go func() {
			sc := mtfinv.NewScanner(ms)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
			defer cancel()
			if _, err := sc.Scan(ctx, opts); err != nil {
				syslog.L.Error(err).WithMessage("mtf inventory scan failed").Write()
			}
		}()

		writeJSON(w, map[string]any{"data": map[string]any{"status": "queued"}, "success": true})
	}
}

// --- Namespace mappings ---

func ExtJsMtfMappingHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		if err := r.ParseForm(); err != nil {
			WriteErrorResponse(w, err)
			return
		}
		m := mtfstore.NamespaceMapping{
			Name:       r.FormValue("name"),
			Priority:   atoiDefault(r.FormValue("priority"), 0),
			MatchRegex: r.FormValue("match_regex"),
			Template:   r.FormValue("template"),
			IsDefault:  r.FormValue("is_default") == "1" || r.FormValue("is_default") == "true",
			Enabled:    r.FormValue("enabled") == "1" || r.FormValue("enabled") == "true",
			Comment:    r.FormValue("comment"),
		}
		if m.Template == "" {
			WriteErrorResponse(w, fmt.Errorf("template is required"))
			return
		}
		id, err := ms.CreateMapping(r.Context(), m)
		if err != nil {
			WriteErrorResponse(w, err)
			return
		}
		if mapper := storeInstance.MtfMapper; mapper != nil {
			mapper.Invalidate()
		}
		writeJSON(w, map[string]any{"data": mtfstore.NamespaceMapping{ID: id}, "success": true})
	}
}

func ExtJsMtfMappingSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil || id <= 0 {
			WriteErrorResponse(w, fmt.Errorf("invalid mapping id"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			m, err := ms.GetMapping(r.Context(), id)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			writeJSON(w, map[string]any{"data": m, "success": true})

		case http.MethodPut:
			m, err := ms.GetMapping(r.Context(), id)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if err := r.ParseForm(); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if v := r.FormValue("name"); v != "" {
				m.Name = v
			}
			if v := r.FormValue("priority"); v != "" {
				m.Priority = atoiDefault(v, 0)
			}
			if v := r.FormValue("match_regex"); v != "" {
				m.MatchRegex = v
			}
			if v := r.FormValue("template"); v != "" {
				m.Template = v
			}
			if r.FormValue("enabled") != "" {
				m.Enabled = r.FormValue("enabled") == "1" || r.FormValue("enabled") == "true"
			}
			if r.FormValue("is_default") != "" {
				m.IsDefault = r.FormValue("is_default") == "1" || r.FormValue("is_default") == "true"
			}
			if v := r.FormValue("comment"); v != "" {
				m.Comment = v
			}
			if err := ms.UpdateMapping(r.Context(), m); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if mapper := storeInstance.MtfMapper; mapper != nil {
				mapper.Invalidate()
			}
			writeJSON(w, map[string]any{"data": m, "success": true})

		case http.MethodDelete:
			if err := ms.DeleteMapping(r.Context(), id); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if mapper := storeInstance.MtfMapper; mapper != nil {
				mapper.Invalidate()
			}
			writeJSON(w, map[string]any{"data": nil, "success": true})
		}
	}
}

// --- helpers ---

// flatMtfJob is the flattened API response for an MTF job. The history block
// is promoted to top-level fields (matching the ExtJS model + grid columns)
// and a pre-parsed status is attached so the shared task-status renderer works.
type flatMtfJob struct {
	mtfstore.MTFJob
	LastRunUpid           string           `json:"last-run-upid"`
	LastRunStarttime      int64            `json:"last-run-starttime"`
	LastRunState          string           `json:"last-run-state"`
	LastRunStatus         int              `json:"last-run-status"`
	LastRunEndtime        int64            `json:"last-run-endtime"`
	LastSuccessfulEndtime int64            `json:"last-successful-endtime"`
	LastSuccessfulUpid    string           `json:"last-successful-upid"`
	RetryCount            int              `json:"retry-count"`
	Duration              int64            `json:"duration"`
	StatusParsed          ParsedTaskStatus `json:"status_parsed"`
}

func flattenMtfJob(j mtfstore.MTFJob) flatMtfJob {
	return flatMtfJob{
		MTFJob:                j,
		LastRunUpid:           j.History.LastRunUpid,
		LastRunStarttime:      j.History.LastRunStarttime,
		LastRunState:          j.History.LastRunState,
		LastRunStatus:         int(j.History.LastRunStatus),
		LastRunEndtime:        j.History.LastRunEndtime,
		LastSuccessfulEndtime: j.History.LastSuccessfulEndtime,
		LastSuccessfulUpid:    j.History.LastSuccessfulUpid,
		RetryCount:            j.History.RetryCount,
		Duration:              j.History.Duration,
		StatusParsed:          ParseTaskStatus(j.History.LastRunState),
	}
}

func atoiDefault(s string, def int) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
