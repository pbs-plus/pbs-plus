//go:build linux

package jobs

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func D2DJobHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		allJobs, err := storeInstance.Database.GetAllJobs()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		digest, err := utils.CalculateDigest(allJobs)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		toReturn := JobsResponse{
			Data:   allJobs,
			Digest: digest,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(toReturn)
	}
}

func ExtJsJobRunHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := JobRunResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		job, err := storeInstance.Database.GetJob(utils.DecodePath(r.PathValue("job")))
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		attempts := 0
		for attempts <= job.Retry {
			op, err := backup.RunBackup(job, storeInstance, false)
			if err != nil {
				if !strings.Contains(err.Error(), "A job is still running.") {
					job.LastRunPlusError = err.Error()
					job.LastRunPlusTime = int(time.Now().Unix())
					if uErr := storeInstance.Database.UpdateJob(*job); uErr != nil {
						syslog.L.Errorf("LastRunPlusError update: %v", uErr)
					}
				}
				attempts++
				if attempts > job.Retry {
					controllers.WriteErrorResponse(w, err)
					return
				}
				continue
			}
			task := op.Task

			w.Header().Set("Content-Type", "application/json")

			response.Data = task.UPID
			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)

			return // Success
		}
	}
}

func ExtJsJobHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := JobConfigResponse{}
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		err := r.ParseForm()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		retry, err := strconv.Atoi(r.FormValue("retry"))
		if err != nil {
			if r.FormValue("retry") == "" {
				retry = 0
			} else {
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		newJob := types.Job{
			ID:               r.FormValue("id"),
			Store:            r.FormValue("store"),
			Target:           r.FormValue("target"),
			Subpath:          r.FormValue("subpath"),
			Schedule:         r.FormValue("schedule"),
			Comment:          r.FormValue("comment"),
			Namespace:        r.FormValue("ns"),
			NotificationMode: r.FormValue("notification-mode"),
			Retry:            retry,
			Exclusions:       []types.Exclusion{},
		}

		rawExclusions := r.FormValue("rawexclusions")
		for _, exclusion := range strings.Split(rawExclusions, "\n") {
			exclusion = strings.TrimSpace(exclusion)
			if exclusion == "" {
				continue
			}

			exclusionInst := types.Exclusion{
				Path:  exclusion,
				JobID: newJob.ID,
			}

			newJob.Exclusions = append(newJob.Exclusions, exclusionInst)
		}

		err = storeInstance.Database.CreateJob(newJob)
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
	}
}

func ExtJsJobSingleHandler(storeInstance *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response := JobConfigResponse{}
		if r.Method != http.MethodPut && r.Method != http.MethodGet && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPut {
			job, err := storeInstance.Database.GetJob(utils.DecodePath(r.PathValue("job")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			err = r.ParseForm()
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			if r.FormValue("retry") != "" {
				retry, err := strconv.Atoi(r.FormValue("retry"))
				if err != nil {
					controllers.WriteErrorResponse(w, err)
					return
				}

				job.Retry = retry
			}
			if r.FormValue("store") != "" {
				job.Store = r.FormValue("store")
			}
			if r.FormValue("target") != "" {
				job.Target = r.FormValue("target")
			}
			if r.FormValue("subpath") != "" {
				job.Subpath = r.FormValue("subpath")
			}
			if r.FormValue("schedule") != "" {
				job.Schedule = r.FormValue("schedule")
			}
			if r.FormValue("comment") != "" {
				job.Comment = r.FormValue("comment")
			}
			if r.FormValue("ns") != "" {
				job.Namespace = r.FormValue("ns")
			}
			if r.FormValue("notification-mode") != "" {
				job.NotificationMode = r.FormValue("notification-mode")
			}

			if r.FormValue("rawexclusions") != "" {
				job.Exclusions = []types.Exclusion{}

				rawExclusions := r.FormValue("rawexclusions")
				for _, exclusion := range strings.Split(rawExclusions, "\n") {
					exclusion = strings.TrimSpace(exclusion)
					if exclusion == "" {
						continue
					}

					exclusionInst := types.Exclusion{
						Path:  exclusion,
						JobID: job.ID,
					}

					job.Exclusions = append(job.Exclusions, exclusionInst)
				}
			}

			if delArr, ok := r.Form["delete"]; ok {
				for _, attr := range delArr {
					switch attr {
					case "store":
						job.Store = ""
					case "target":
						job.Target = ""
					case "subpath":
						job.Subpath = ""
					case "schedule":
						job.Schedule = ""
					case "comment":
						job.Comment = ""
					case "ns":
						job.Namespace = ""
					case "retry":
						job.Retry = 0
					case "notification-mode":
						job.NotificationMode = ""
					case "rawexclusions":
						job.Exclusions = []types.Exclusion{}
					}
				}
			}

			err = storeInstance.Database.UpdateJob(*job)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodGet {
			job, err := storeInstance.Database.GetJob(utils.DecodePath(r.PathValue("job")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			response.Data = job
			json.NewEncoder(w).Encode(response)

			return
		}

		if r.Method == http.MethodDelete {
			err := storeInstance.Database.DeleteJob(utils.DecodePath(r.PathValue("job")))
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}

			response.Status = http.StatusOK
			response.Success = true
			json.NewEncoder(w).Encode(response)
			return
		}
	}
}
