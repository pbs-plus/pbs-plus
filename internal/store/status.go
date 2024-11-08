//go:build linux

package store

import (
	"fmt"
	"net/http"
)

type PBSStatus struct {
	BootInfo      map[string]interface{} `json:"boot-info"`
	CPU           float64                `json:"cpu"`
	CPUInfo       map[string]interface{} `json:"cpuinfo"`
	CurrentKernel map[string]interface{} `json:"current-kernel"`
	Info          map[string]string      `json:"info"`
	KVersion      string                 `json:"kversion"`
	LoadAvg       []float32              `json:"loadavg"`
	Memory        map[string]int64       `json:"memory"`
	Root          map[string]int64       `json:"root"`
	Swap          map[string]int64       `json:"swap"`
	Uptime        int64                  `json:"uptime"`
	Wait          float32                `json:"wait"`
}

type PBSStatusResponse struct {
	Data PBSStatus `json:"data"`
}

func (storeInstance *Store) GetPBSStatus() (*PBSStatus, error) {
	var resp PBSStatusResponse

	err := storeInstance.ProxmoxHTTPRequest(
		http.MethodGet,
		"/api2/json/nodes/localhost/status",
		nil,
		&resp,
	)
	if err != nil {
		return nil, fmt.Errorf("GetPBSStatus: error creating http request -> %w", err)
	}

	return &resp.Data, nil
}
