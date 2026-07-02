//go:build linux

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/changer"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tape"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf"
	mtfdb "github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	jobrpc "github.com/pbs-plus/pbs-plus/internal/server/rpc"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func mtfStore(st *store.Store) *mtfdb.Database {
	if st == nil {
		return nil
	}
	return st.MtfStore
}

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

		// Single job run: synchronous — return UPID so frontend can open TaskViewer.
		if !stop && len(decoded) == 1 {
			conn, err := net.DialTimeout("unix", conf.JobMutateSocketPath, 30*time.Second)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			rpcClient := rpc.NewClient(conn)
			defer rpcClient.Close()

			args := &jobrpc.MtfJobQueueArgs{JobID: decoded[0], Stop: false}
			var reply jobrpc.QueueReply
			if err := rpcClient.Call("JobRPCService.MtfQueue", args, &reply); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if reply.Status != 200 {
				WriteErrorResponse(w, fmt.Errorf("%s", reply.Message))
				return
			}

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(MtfJobRunResponse{
				Data:    reply.UPID,
				Status:  http.StatusOK,
				Success: true,
			})
			return
		}

		// Batch run or stop: fire-and-forget async.
		go func() {
			conn, err := net.DialTimeout("unix", conf.JobMutateSocketPath, 5*time.Minute)
			if err != nil {
				log.Error(err, "", "mtfJobs", decoded)
				return
			}
			rpcClient := rpc.NewClient(conn)
			defer rpcClient.Close()

			for _, id := range decoded {
				args := &jobrpc.MtfJobQueueArgs{JobID: id, Stop: stop}
				var reply jobrpc.QueueReply
				if err := rpcClient.Call("JobRPCService.MtfQueue", args, &reply); err != nil {
					log.Error(err, "", "mtfJobID", id)
					continue
				}
				if reply.Status != 200 {
					log.Error(fmt.Errorf("%s", reply.Message), "", "mtfJobID", id)
				}
			}
		}()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(MtfJobRunResponse{
			Status:  http.StatusOK,
			Success: true,
		})
	}
}

func ExtJsMtfJobHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}

		if r.Method == http.MethodGet {
			jobs, err := ms.ListMtfJobs(r.Context())
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			out := make([]flatMtfJob, 0, len(jobs))
			for _, j := range jobs {
				out = append(out, flattenMtfJob(j))
			}

			digest, err := calculateDigest(out)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			toReturn := map[string]any{
				"data":    out,
				"digest":  digest,
				"success": true,
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(toReturn); err != nil {
				log.Error(err, "")
			}
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		response := MtfJobConfigResponse{}
		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
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

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
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

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			job, err := ms.GetMtfJob(r.Context(), id)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response := MtfJobConfigResponse{}
			response.Status = http.StatusOK
			response.Success = true
			flat := flattenMtfJobForEdit(job)
			flat["notification-batch"] = GetJobBatchName(storeInstance, "backup", job.ID)
			response.Data = flat
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
			return
		}

		if r.Method == http.MethodPut {
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

			ApplyJobBatchAssignment(storeInstance, "backup", updated.ID, r.FormValue("notification-batch"))

			response := MtfJobConfigResponse{}
			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
			return
		}

		if r.Method == http.MethodDelete {
			if err := ms.DeleteMtfJob(r.Context(), id); err != nil {
				WriteErrorResponse(w, err)
				return
			}

			response := MtfJobConfigResponse{}
			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
			return
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

		response := MtfJobConfigResponse{}
		w.Header().Set("Content-Type", "application/json")
		response.Status = http.StatusOK
		response.Success = true
		response.Data = upids
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func mtfJobFromForm(r *http.Request) (mtfdb.MTFJob, error) {
	j := mtfdb.MTFJob{
		ID:                r.FormValue("id"),
		SourceKind:        r.FormValue("source_kind"),
		SourceRef:         r.FormValue("source_ref"),
		Datastore:         r.FormValue("datastore"),
		Namespace:         r.FormValue("namespace"),
		Comment:           r.FormValue("comment"),
		NotificationMode:  r.FormValue("notification-mode"),
		Changer:           r.FormValue("changer"),
		Drive:             r.FormValue("drive"),
		Spanning:          true,
		OverwriteMappings: r.FormValue("overwrite_mappings") == "1" || r.FormValue("overwrite_mappings") == "true",
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

func mtfJobMergeForm(job mtfdb.MTFJob, r *http.Request) (mtfdb.MTFJob, error) {
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
	if r.FormValue("overwrite_mappings") != "" {
		job.OverwriteMappings = r.FormValue("overwrite_mappings") == "1" || r.FormValue("overwrite_mappings") == "true"
	}

	if delArr, ok := r.Form["delete"]; ok {
		for _, attr := range delArr {
			switch attr {
			case "datastore":
				job.Datastore = ""
			case "namespace":
				job.Namespace = ""
			case "source_ref":
				job.SourceRef = ""
			case "source_kind":
				job.SourceKind = ""
			case "comment":
				job.Comment = ""
			case "notification-mode":
				job.NotificationMode = ""
			case "changer":
				job.Changer = ""
			case "drive":
				job.Drive = ""
			case "overwrite_mappings":
				job.OverwriteMappings = false
			}
		}
	}

	return job, nil
}

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
		resp := MtfInventoryResponse{Success: true}

		switch r.URL.Query().Get("type") {
		case "cartridges":
			list, err := ms.ListCartridges(ctx)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			resp.Data = list
		case "families":
			list, err := ms.ListMediaFamilies(ctx)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			resp.Data = list
		case "datasets":
			famID, parseErr := strconv.ParseInt(r.URL.Query().Get("family"), 10, 64)
			if parseErr != nil {
				log.Error(parseErr, "")
			}
			var list []mtfdb.DataSet
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
			resp.Data = list
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
			resp.Data = map[string]any{
				"families":   families,
				"cartridges": cartridges,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsMtfScanHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			if r.URL.Query().Get("type") == "barcodes" {
				listBarcodes(w, r)
				return
			}
			active, upid := mtfScanInProgress()
			resp := map[string]any{"success": true, "data": map[string]any{"active": active, "upid": upid}}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				log.Error(err, "")
			}
			return
		}
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

		driveIdx := 0
		if v := r.FormValue("drive_index"); v != "" {
			driveIdx = atoiDefault(v, 0)
		} else {
			driveIdx = tape.ResolveDriveIndex(r.FormValue("drive"))
		}

		opts := mtf.Options{
			ChangerDevice: tape.ResolveChanger(r.FormValue("changer")),
			TapeDevice:    tape.ResolveDrive(r.FormValue("drive")),
			DriveIndex:    driveIdx,
			BKFPath:       r.FormValue("bkf_path"),
			Label:         r.FormValue("label"),
			Barcodes:      parseBarcodes(r),
		}

		if active, _ := mtfScanInProgress(); active {
			WriteErrorResponse(w, fmt.Errorf("an MTF inventory scan is already in progress"))
			return
		}

		st, err := mtf.NewScanTask(opts)
		if err != nil {
			WriteErrorResponse(w, fmt.Errorf("create scan task: %w", err))
			return
		}
		src := "changer " + opts.ChangerDevice
		if opts.BKFPath != "" {
			src = ".bkf: " + opts.BKFPath
		} else if opts.TapeDevice != "" {
			src = "drive: " + opts.TapeDevice
		}
		st.LogString("MTF inventory scan started (" + src + ")")

		go func() {
			sc := mtf.NewScanner(ms)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
			defer cancel()
			tl := log.WithScope(log.Scope{Task: st.WorkerTask})
			defer func() {
				if r := recover(); r != nil {
					tl.LogString(fmt.Sprintf("panic: %v", r))
					st.CloseErr(fmt.Errorf("scan panic: %v", r))
				}
			}()
			res, scanErr := sc.ScanWithLog(ctx, opts, tl)
			if scanErr != nil {
				tl.LogString(scanErr.Error())
				st.CloseErr(scanErr)
				return
			}
			tl.LogString(fmt.Sprintf("Scan completed: %d cartridges, %d families (%s)",
				res.Cartridges, res.Families, res.Duration.Truncate(time.Second)))
			st.CloseOK(res)
		}()

		response := MtfJobRunResponse{}
		w.Header().Set("Content-Type", "application/json")
		response.Status = http.StatusOK
		response.Success = true
		response.Data = st.UPID()
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsMtfMappingHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ms := mtfStore(storeInstance)
		if ms == nil {
			WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			list, err := ms.ListMappings(r.Context())
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			response := MtfMappingConfigResponse{}
			w.Header().Set("Content-Type", "application/json")
			response.Status = http.StatusOK
			response.Success = true
			response.Data = list
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

		case http.MethodPost:
			response := MtfMappingConfigResponse{}
			w.Header().Set("Content-Type", "application/json")

			if err := r.ParseForm(); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			m := mtfdb.NamespaceMapping{
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

			response.Status = http.StatusOK
			response.Success = true
			response.Data = mtfdb.NamespaceMapping{ID: id}
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

		default:
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
		}
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

		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			m, err := ms.GetMapping(r.Context(), id)
			if err != nil {
				WriteErrorResponse(w, err)
				return
			}
			response := MtfMappingConfigResponse{}
			response.Status = http.StatusOK
			response.Success = true
			response.Data = m
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

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

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "name":
						m.Name = ""
					case "match_regex":
						m.MatchRegex = ""
					case "template":
						m.Template = ""
					case "comment":
						m.Comment = ""
					}
				}
			}

			if err := ms.UpdateMapping(r.Context(), m); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if mapper := storeInstance.MtfMapper; mapper != nil {
				mapper.Invalidate()
			}

			response := MtfMappingConfigResponse{}
			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}

		case http.MethodDelete:
			if err := ms.DeleteMapping(r.Context(), id); err != nil {
				WriteErrorResponse(w, err)
				return
			}
			if mapper := storeInstance.MtfMapper; mapper != nil {
				mapper.Invalidate()
			}

			response := MtfMappingConfigResponse{}
			response.Status = http.StatusOK
			response.Success = true
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
		}
	}
}

// flatMtfJob is the flattened API response for an MTF job. The history block
type flatMtfJob struct {
	mtfdb.MTFJob
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
	CurrentFilesSpeed     int              `json:"current_files_speed,omitempty"`
	CurrentBytesSpeed     int              `json:"current_bytes_speed,omitempty"`
	CurrentBytesTotal     int64            `json:"current_bytes_total,omitempty"`
	CurrentFileCount      int64            `json:"current_file_count,omitempty"`
	CurrentFolderCount    int64            `json:"current_folder_count,omitempty"`
	ReadSpeedHuman        string           `json:"read_speed_human"`
	ReadTotalHuman        string           `json:"read_total_human"`
	ProcessingSpeedHuman  string           `json:"processing_speed_human"`
}

func flattenMtfJob(j mtfdb.MTFJob) flatMtfJob {
	f := flatMtfJob{
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
	if j.History.LastRunUpid != "" && (int(j.History.LastRunStatus) == 0 || j.History.LastRunState == "") {
		if r, ok := tasklog.ResolveHistoryFields(j.History.LastRunUpid); ok {
			tasklog.ApplyResolved(r, &f.LastRunStarttime, &f.LastRunEndtime, &f.Duration, &f.LastRunState)
			if f.LastRunState != "" {
				f.StatusParsed = ParseTaskStatus(f.LastRunState)
			}
		}
	}
	if p, ok := mtf.ProgressFor(j.ID); ok {
		f.CurrentFileCount = p.Files
		f.CurrentFolderCount = p.Dirs
		f.CurrentBytesTotal = p.Bytes
		f.CurrentBytesSpeed = int(p.PhysInst * 1e6)
		f.CurrentFilesSpeed = int(p.FilesInst)
		FillSpeedFields(&LiveStats{
			FileCount:   p.Files,
			FolderCount: p.Dirs,
			BytesTotal:  p.Bytes,
			BytesSpeed:  int(p.PhysInst * 1e6),
			FilesSpeed:  int(p.FilesInst),
		}, &f.ReadSpeedHuman, &f.ReadTotalHuman, &f.ProcessingSpeedHuman)
	}
	return f
}

func flattenMtfJobForEdit(j mtfdb.MTFJob) map[string]any {
	return map[string]any{
		"id":                 j.ID,
		"source_kind":        j.SourceKind,
		"source_ref":         j.SourceRef,
		"datastore":          j.Datastore,
		"namespace":          j.Namespace,
		"comment":            j.Comment,
		"notification-mode":  j.NotificationMode,
		"notification-batch": "",
		"changer":            j.Changer,
		"drive":              j.Drive,
		"spanning":           j.Spanning,
		"overwrite_mappings": j.OverwriteMappings,
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

func mtfScanInProgress() (bool, string) {
	tasks, err := tasklog.ListTasks(true)
	if err != nil {
		return false, ""
	}
	for _, t := range tasks {
		if t.Task.WorkerType == "mtfscan" {
			return true, t.UPID
		}
	}
	return false, ""
}

func parseBarcodes(r *http.Request) []string {
	var out []string
	for _, raw := range r.Form["barcodes"] {
		for bc := range strings.SplitSeq(raw, ",") {
			bc = strings.TrimSpace(bc)
			if bc != "" {
				out = append(out, bc)
			}
		}
	}
	if v := strings.TrimSpace(r.FormValue("barcodes")); v != "" && len(out) == 0 {
		for bc := range strings.SplitSeq(v, ",") {
			bc = strings.TrimSpace(bc)
			if bc != "" {
				out = append(out, bc)
			}
		}
	}
	return out
}

func listBarcodes(w http.ResponseWriter, r *http.Request) {
	changerPath := tape.ResolveChanger(r.URL.Query().Get("changer"))
	if changerPath == "" {
		WriteErrorResponse(w, fmt.Errorf("changer parameter is required"))
		return
	}
	ch, err := changer.Open(changerPath)
	if err != nil {
		WriteErrorResponse(w, fmt.Errorf("open changer %s: %w", changerPath, err))
		return
	}
	defer ch.Close()
	st, err := ch.Status()
	if err != nil {
		WriteErrorResponse(w, fmt.Errorf("read changer status: %w", err))
		return
	}
	seen := make(map[string]bool)
	var barcodes []string
	add := func(bc string) {
		bc = strings.TrimSpace(bc)
		if bc == "" || seen[bc] {
			return
		}
		seen[bc] = true
		barcodes = append(barcodes, bc)
	}
	for _, s := range st.Slots {
		if s.Full {
			add(s.VolumeTag)
		}
	}
	for _, d := range st.Drives {
		if d.Full {
			add(d.VolumeTag)
		}
	}
	resp := map[string]any{"success": true, "data": barcodes}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Error(err, "")
	}
}
