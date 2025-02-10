//go:build linux

package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
)

type NamespaceReq struct {
	Name   string `json:"name"`
	Parent string `json:"parent"`
}

type PBSStoreGroups struct {
	Owner string `json:"owner"`
}

type PBSStoreGroupsResponse struct {
	Data PBSStoreGroups `json:"data"`
}

func CreateNamespace(namespace string, job *types.Job, storeInstance *store.Store) error {
	if storeInstance == nil {
		return fmt.Errorf("CreateNamespace: store is required")
	}

	if proxmox.Session.APIToken == nil {
		return fmt.Errorf("CreateNamespace: api token is required")
	}

	namespaceSplit := strings.Split(namespace, "/")

	for i, ns := range namespaceSplit {
		var reqBody []byte
		var err error

		if i == 0 {
			reqBody, err = json.Marshal(&NamespaceReq{
				Name: ns,
			})
			if err != nil {
				return fmt.Errorf("CreateNamespace: error creating req body -> %w", err)
			}
		} else {
			reqBody, err = json.Marshal(&NamespaceReq{
				Name:   ns,
				Parent: strings.Join(namespaceSplit[:i], "/"),
			})
			if err != nil {
				return fmt.Errorf("CreateNamespace: error creating req body -> %w", err)
			}
		}

		_ = proxmox.Session.ProxmoxHTTPRequest(
			http.MethodPost,
			fmt.Sprintf("/api2/json/admin/datastore/%s/namespace", job.Store),
			bytes.NewBuffer(reqBody),
			nil,
		)
	}

	job.Namespace = namespace
	err := storeInstance.Database.UpdateJob(*job)
	if err != nil {
		return fmt.Errorf("CreateNamespace: error updating job to namespace -> %w", err)
	}

	return nil
}

func GetCurrentOwner(job *types.Job, storeInstance *store.Store) (string, error) {
	if storeInstance == nil {
		return "", fmt.Errorf("GetCurrentOwner: store is required")
	}

	if proxmox.Session.APIToken == nil {
		return "", fmt.Errorf("GetCurrentOwner: api token is required")
	}

	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		return "", fmt.Errorf("GetCurrentOwner -> %w", err)
	}

	if target == nil {
		return "", fmt.Errorf("GetCurrentOwner: Target '%s' does not exist.", job.Target)
	}

	if !target.ConnectionStatus {
		return "", fmt.Errorf("GetCurrentOwner: Target '%s' is unreachable or does not exist.", job.Target)
	}

	params := url.Values{}
	params.Add("ns", job.Namespace)

	groupsResp := PBSStoreGroupsResponse{}
	err = proxmox.Session.ProxmoxHTTPRequest(
		http.MethodGet,
		fmt.Sprintf("/api2/json/admin/datastore/%s/groups?%s", job.Store, params.Encode()),
		nil,
		&groupsResp,
	)
	if err != nil {
		return "", fmt.Errorf("GetCurrentOwner: error creating http request -> %w", err)
	}

	return groupsResp.Data.Owner, nil
}

func SetDatastoreOwner(job *types.Job, storeInstance *store.Store, owner string) error {
	if storeInstance == nil {
		return fmt.Errorf("SetDatastoreOwner: store is required")
	}

	if proxmox.Session.APIToken == nil {
		return fmt.Errorf("SetDatastoreOwner: api token is required")
	}

	target, err := storeInstance.Database.GetTarget(job.Target)
	if err != nil {
		return fmt.Errorf("SetDatastoreOwner -> %w", err)
	}

	if target == nil {
		return fmt.Errorf("SetDatastoreOwner: Target '%s' does not exist.", job.Target)
	}

	if !target.ConnectionStatus {
		return fmt.Errorf("SetDatastoreOwner: Target '%s' is unreachable or does not exist.", job.Target)
	}

	jobStore := fmt.Sprintf(
		"%s@localhost:%s",
		proxmox.Session.APIToken.TokenId,
		job.Store,
	)

	hostname, err := os.Hostname()
	if err != nil {
		hostnameFile, err := os.ReadFile("/etc/hostname")
		if err != nil {
			hostname = "localhost"
		}

		hostname = strings.TrimSpace(string(hostnameFile))
	}

	isAgent := strings.HasPrefix(target.Path, "agent://")
	backupId := hostname
	if isAgent {
		backupId = strings.TrimSpace(strings.Split(target.Name, " - ")[0])
	}

	cmdArgs := []string{
		"change-owner",
		fmt.Sprintf("host/%s", backupId),
		owner,
		"--repository",
		jobStore,
	}

	if job.Namespace != "" {
		cmdArgs = append(cmdArgs, "--ns")
		cmdArgs = append(cmdArgs, job.Namespace)
	} else if isAgent && job.Namespace == "" {
		cmdArgs = append(cmdArgs, "--ns")
		cmdArgs = append(cmdArgs, strings.ReplaceAll(job.Target, " - ", "/"))
	}

	cmd := exec.Command("/usr/bin/proxmox-backup-client", cmdArgs...)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_PASSWORD=%s", proxmox.Session.APIToken.Value))

	pbsStatus, err := proxmox.Session.GetPBSStatus()
	if err == nil {
		if fingerprint, ok := pbsStatus.Info["fingerprint"]; ok {
			cmd.Env = append(cmd.Env, fmt.Sprintf("PBS_FINGERPRINT=%s", fingerprint))
		}
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("SetDatastoreOwner: proxmox-backup-client change-owner error (%s) -> %w", cmd.String(), err)
	}

	return nil
}

func FixDatastore(job *types.Job, storeInstance *store.Store) error {
	newOwner := ""
	if proxmox.Session.APIToken != nil {
		newOwner = proxmox.Session.APIToken.TokenId
	}

	return SetDatastoreOwner(job, storeInstance, newOwner)
}
