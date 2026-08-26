//go:build linux

package mtfapi

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/digest"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/notificationapi"
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
	"github.com/pbs-plus/pbs-plus/internal/server/rpc/jobrpc"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func ExtJsMtfJobRunHandler(app *application.Runtime) http.HandlerFunc {
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
				respond.WriteErrorResponse(w, err)
				return
			}
			decoded = append(decoded, d)
		}

		stop := r.Method == http.MethodDelete

		// Single job run: synchronous — return UPID so frontend can open TaskViewer.
		if !stop && len(decoded) == 1 {
			conn, err := net.DialTimeout("unix", conf.JobMutateSocketPath, 30*time.Second)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			rpcClient := rpc.NewClient(conn)
			defer rpcClient.Close()

			args := &jobrpc.MtfJobQueueArgs{JobID: decoded[0], Stop: false}
			var reply jobrpc.QueueReply
			if err := rpcClient.Call(jobrpc.ServiceName+".MtfQueue", args, &reply); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			if reply.Status != 200 {
				respond.WriteErrorResponse(w, fmt.Errorf("%s", reply.Message))
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
				if err := rpcClient.Call(jobrpc.ServiceName+".MtfQueue", args, &reply); err != nil {
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

func ExtJsMtfJobHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ms := mtfStore(app)
		if ms == nil {
			respond.WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}

		if r.Method == http.MethodGet {
			jobs, err := ms.ListMtfJobs(r.Context())
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			out := make([]flatMtfJob, 0, len(jobs))
			for _, j := range jobs {
				out = append(out, flattenMtfJob(j))
			}

			digest, err := digest.Calculate(out)
			if err != nil {
				respond.WriteErrorResponse(w, err)
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
			respond.WriteErrorResponse(w, err)
			return
		}

		job, err := mtfJobFromForm(r)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		created, err := ms.CreateMtfJob(r.Context(), job)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		notificationapi.ApplyJobBatchAssignment(app, "backup", created.ID, r.FormValue("notification-batch"))

		response.Status = http.StatusOK
		response.Success = true
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
}

func ExtJsMtfJobSingleHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPut && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(app)
		if ms == nil {
			respond.WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}

		id := validate.DecodePath(r.PathValue("job"))
		if err := validate.ValidateJobId(id); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			job, err := ms.GetMtfJob(r.Context(), id)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			response := MtfJobConfigResponse{}
			response.Status = http.StatusOK
			response.Success = true
			flat := flattenMtfJobForEdit(job)
			flat["notification-batch"] = notificationapi.GetJobBatchName(app, "backup", job.ID)
			response.Data = flat
			if err := json.NewEncoder(w).Encode(response); err != nil {
				log.Error(err, "")
			}
			return
		}

		if r.Method == http.MethodPut {
			job, err := ms.GetMtfJob(r.Context(), id)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := r.ParseForm(); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			updated, err := mtfJobMergeForm(job, r)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			if err := ms.UpdateMtfJob(r.Context(), updated); err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}

			notificationapi.ApplyJobBatchAssignment(app, "backup", updated.ID, r.FormValue("notification-batch"))

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
				respond.WriteErrorResponse(w, err)
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

func ExtJsMtfJobUPIDsHandler(app *application.Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}
		ms := mtfStore(app)
		if ms == nil {
			respond.WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		id := validate.DecodePath(r.PathValue("job"))
		if err := validate.ValidateJobId(id); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		job, err := ms.GetMtfJob(r.Context(), id)
		if err != nil {
			respond.WriteErrorResponse(w, err)
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

type MtfJobConfigResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    any               `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}

type MtfJobRunResponse struct {
	Errors  map[string]string `json:"errors"`
	Message string            `json:"message"`
	Data    string            `json:"data"`
	Status  int               `json:"status"`
	Success bool              `json:"success"`
}
