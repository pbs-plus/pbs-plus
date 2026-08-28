//go:build linux

package mtfapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/respond"

	"github.com/pbs-plus/pbs-plus/internal/changer"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tape"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf/mtfdb"
)

func ExtJsMtfInventoryHandler(app *application.Runtime) http.HandlerFunc {
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
		ctx := r.Context()
		resp := MtfInventoryResponse{Success: true}

		switch r.URL.Query().Get("type") {
		case "cartridges":
			list, err := ms.ListCartridges(ctx)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			resp.Data = list
		case "families":
			list, err := ms.ListMediaFamilies(ctx)
			if err != nil {
				respond.WriteErrorResponse(w, err)
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
				respond.WriteErrorResponse(w, err)
				return
			}
			resp.Data = list
		default:
			families, err := ms.ListMediaFamilies(ctx)
			if err != nil {
				respond.WriteErrorResponse(w, err)
				return
			}
			cartridges, err := ms.ListCartridges(ctx)
			if err != nil {
				respond.WriteErrorResponse(w, err)
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

func ExtJsMtfScanHandler(app *application.Runtime) http.HandlerFunc {
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
		ms := mtfStore(app)
		if ms == nil {
			respond.WriteErrorResponse(w, fmt.Errorf("MTF store unavailable"))
			return
		}
		if err := r.ParseForm(); err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}

		driveIdx := 0
		if v := r.FormValue("drive_index"); v != "" {
			driveIdx = atoiDefault(v, 0)
		} else {
			driveIdx = tape.ResolveDriveIndex(r.FormValue("drive"))
		}

		input := jobs.MtfScanInput{
			ChangerDevice: tape.ResolveChanger(r.FormValue("changer")),
			TapeDevice:    tape.ResolveDrive(r.FormValue("drive")),
			DriveIndex:    driveIdx,
			BKFPath:       r.FormValue("bkf_path"),
			Label:         r.FormValue("label"),
			Barcodes:      parseBarcodes(r),
		}

		request, err := jobs.NewWorkflowSubmit(
			jobs.WorkflowMtfScan,
			"inventory-scan",
			"manual",
			"",
			input,
			[]string{"mtf-scan", "mtf-tape"},
			1,
			time.Minute,
		)
		if err != nil {
			respond.WriteErrorResponse(w, err)
			return
		}
		execution, created, err := app.Engine.Submit(context.Background(), request)
		if err != nil || !created {
			if err == nil {
				err = fmt.Errorf("an MTF inventory scan is already in progress")
			}
			respond.WriteErrorResponse(w, err)
			return
		}

		response := MtfJobRunResponse{}
		w.Header().Set("Content-Type", "application/json")
		response.Status = http.StatusOK
		response.Success = true
		response.Data = execution.ID
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Error(err, "")
		}
	}
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
		respond.WriteErrorResponse(w, fmt.Errorf("changer parameter is required"))
		return
	}
	ch, err := changer.Open(changerPath)
	if err != nil {
		respond.WriteErrorResponse(w, fmt.Errorf("open changer %s: %w", changerPath, err))
		return
	}
	defer ch.Close()
	st, err := ch.Status()
	if err != nil {
		respond.WriteErrorResponse(w, fmt.Errorf("read changer status: %w", err))
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

type MtfInventoryResponse struct {
	Data    any    `json:"data"`
	Digest  string `json:"digest"`
	Success bool   `json:"success"`
}
