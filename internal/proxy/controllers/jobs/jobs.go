//go:build linux

package jobs

import (
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/utils"
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

		for i, job := range allJobs {
			splittedTargetName := strings.Split(job.Target, " - ")
			targetHostname := splittedTargetName[0]
			childKey := targetHostname + "|" + job.ID
			arpcfs := store.GetSessionFS(childKey)
			if arpcfs == nil {
				continue
			}

			stats := arpcfs.GetStats()

			allJobs[i].CurrentFileCount = int(stats.FilesAccessed)
			allJobs[i].CurrentFolderCount = int(stats.FoldersAccessed)
			allJobs[i].CurrentBytesTotal = int(stats.TotalBytes)
			allJobs[i].CurrentBytesSpeed = int(stats.ByteReadSpeed)
			allJobs[i].CurrentFilesSpeed = int(stats.FileAccessSpeed)
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
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		var response JobRunResponse

		// Get all job IDs from query parameters: ?job=job1&job=job2
		jobIDs := r.URL.Query()["job"]
		if len(jobIDs) == 0 {
			http.Error(w, "Missing job parameter(s)", http.StatusBadRequest)
			return
		}

		for _, jobID := range jobIDs {
			jobIDDecoded := utils.DecodePath(jobID)
			job, err := storeInstance.Database.GetJob(jobIDDecoded)
			if err != nil {
				controllers.WriteErrorResponse(w, err)
				return
			}
			system.RemoveAllRetrySchedules(job)
		}

		execPath, err := os.Executable()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		args := []string{}
		for _, jobId := range jobIDs {
			args = append(args, "-job", jobId)
		}
		args = append(args, "-web")
		if r.Method == http.MethodDelete {
			args = append(args, "-stop")
		} else if r.Method != http.MethodPost {
			http.Error(w, "Invalid HTTP method", http.StatusBadRequest)
			return
		}

		cmd := exec.Command(execPath, args...)
		cmd.Env = os.Environ()
		err = cmd.Start()
		if err != nil {
			controllers.WriteErrorResponse(w, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		response.Status = http.StatusOK
		response.Success = true
		json.NewEncoder(w).Encode(response)
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

		retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
		if err != nil {
			if r.FormValue("retry-interval") == "" {
				retryInterval = 1
			} else {
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		maxDirEntries, err := strconv.Atoi(r.FormValue("max-dir-entries"))
		if err != nil {
			if r.FormValue("max-dir-entries") == "" {
				maxDirEntries = 1048576
			} else {
				controllers.WriteErrorResponse(w, err)
				return
			}
		}

		newJob := types.Job{
			ID:               r.FormValue("id"),
			Store:            r.FormValue("store"),
			SourceMode:       r.FormValue("sourcemode"),
			ReadMode:         r.FormValue("readmode"),
			Mode:             r.FormValue("mode"),
			Target:           r.FormValue("target"),
			Subpath:          r.FormValue("subpath"),
			Schedule:         r.FormValue("schedule"),
			Comment:          r.FormValue("comment"),
			Namespace:        r.FormValue("ns"),
			MaxDirEntries:    maxDirEntries,
			NotificationMode: r.FormValue("notification-mode"),
			Retry:            retry,
			RetryInterval:    retryInterval,
			Exclusions:       []types.Exclusion{},
			PreScript:        r.FormValue("pre_script"),
			PostScript:       r.FormValue("post_script"),
		}

		rawExclusions := r.FormValue("rawexclusions")
		for exclusion := range strings.SplitSeq(rawExclusions, "\n") {
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

		err = storeInstance.Database.CreateJob(nil, newJob)
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
			if r.FormValue("readmode") != "" {
				job.ReadMode = r.FormValue("readmode")
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

			job.PreScript = r.FormValue("pre_script")
			job.PostScript = r.FormValue("post_script")

			retry, err := strconv.Atoi(r.FormValue("retry"))
			if err != nil {
				retry = 0
			}

			retryInterval, err := strconv.Atoi(r.FormValue("retry-interval"))
			if err != nil {
				retryInterval = 1
			}

			maxDirEntries, err := strconv.Atoi(r.FormValue("max-dir-entries"))
			if err != nil {
				maxDirEntries = 1048576
			}

			job.Retry = retry
			job.RetryInterval = retryInterval
			job.MaxDirEntries = maxDirEntries

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
					case "readmode":
						job.ReadMode = ""
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
					case "retry-interval":
						job.RetryInterval = 1
					case "max-dir-entries":
						job.MaxDirEntries = 1048576
					case "notification-mode":
						job.NotificationMode = ""
					case "pre_script":
						job.PreScript = ""
					case "post_script":
						job.PostScript = ""
					case "rawexclusions":
						job.Exclusions = []types.Exclusion{}
					}
				}
			}

			err = storeInstance.Database.UpdateJob(nil, job)
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
			err := storeInstance.Database.DeleteJob(nil, utils.DecodePath(r.PathValue("job")))
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
