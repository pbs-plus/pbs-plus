//go:build linux

package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/backend/backup"
	"github.com/sonroyaalmerol/pbs-plus/internal/proxy/controllers"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/system"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
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

		p := message.NewPrinter(language.English)
		for i, job := range allJobs {
			arpcfs := storeInstance.GetARPCFS(job.ID)
			if arpcfs == nil {
				continue
			}

			stats := arpcfs.GetStats()

			allJobs[i].CurrentFileCount = p.Sprintf("%d", stats.FilesAccessed)
			allJobs[i].CurrentFolderCount = p.Sprintf("%d", stats.FoldersAccessed)
			allJobs[i].CurrentBytesTotal = utils.HumanReadableBytes(int64(stats.TotalBytes))
			allJobs[i].CurrentBytesSpeed = utils.HumanReadableSpeed(stats.ByteReadSpeed)
			allJobs[i].CurrentFilesSpeed = fmt.Sprintf("%.2f files/s", stats.FileAccessSpeed)
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

		op, err := backup.RunBackup(context.Background(), job, storeInstance, false)
		if err != nil {
			syslog.L.Error(err).WithField("jobId", job.ID).Write()
			if task, err := proxmox.GenerateTaskErrorFile(job, err, []string{"Error handling from a web job run request", "Job ID: " + job.ID, "Source Mode: " + job.SourceMode}); err != nil {
				syslog.L.Error(err).WithField("jobId", job.ID).Write()
			} else {
				// Update job status
				latestJob, err := storeInstance.Database.GetJob(job.ID)
				if err != nil {
					latestJob = job
				}

				latestJob.LastRunUpid = task.UPID
				latestJob.LastRunState = task.Status
				latestJob.LastRunEndtime = task.EndTime

				err = storeInstance.Database.UpdateJob(latestJob)
				if err != nil {
					syslog.L.Error(err).WithField("jobId", latestJob.ID).WithField("upid", task.UPID).Write()
				}
			}

			if !errors.Is(err, backup.ErrOneInstance) {
				if err := system.SetRetrySchedule(job); err != nil {
					syslog.L.Error(err).WithField("jobId", job.ID).Write()
				}
			}

			controllers.WriteErrorResponse(w, err)
			return
		}

		task := op.Task

		w.Header().Set("Content-Type", "application/json")

		response.Data = task.UPID
		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
		return
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
			SourceMode:       r.FormValue("sourcemode"),
			Mode:             r.FormValue("mode"),
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

			if r.FormValue("store") != "" {
				job.Store = r.FormValue("store")
			}
			if r.FormValue("mode") != "" {
				job.Mode = r.FormValue("mode")
			}
			if r.FormValue("sourcemode") != "" {
				job.SourceMode = r.FormValue("sourcemode")
			}
			if r.FormValue("target") != "" {
				job.Target = r.FormValue("target")
			}
			if r.FormValue("schedule") != "" {
				job.Schedule = r.FormValue("schedule")
			}
			if r.FormValue("comment") != "" {
				job.Comment = r.FormValue("comment")
			}
			if r.FormValue("notification-mode") != "" {
				job.NotificationMode = r.FormValue("notification-mode")
			}

			retry, err := strconv.Atoi(r.FormValue("retry"))
			if err != nil {
				retry = 0
			}

			job.Retry = retry

			job.Subpath = r.FormValue("subpath")
			job.Namespace = r.FormValue("ns")
			job.Exclusions = []types.Exclusion{}

			if r.FormValue("rawexclusions") != "" {
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
					case "mode":
						job.Mode = ""
					case "sourcemode":
						job.SourceMode = ""
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

			err = storeInstance.Database.UpdateJob(job)
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
