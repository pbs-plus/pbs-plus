//go:build linux

package proxmox

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
)

type Token struct {
	CSRFToken string `json:"CSRFPreventionToken"`
	Ticket    string `json:"ticket"`
	Username  string `json:"username"`
}

type TokenResponse struct {
	Data Token `json:"data"`
}

type TokenRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type APITokenRequest struct {
	Comment string `json:"comment"`
}

type APITokenResponse struct {
	Data APIToken `json:"data"`
}

type ACLRequest struct {
	Path   string `json:"path"`
	Role   string `json:"role"`
	AuthId string `json:"auth-id"`
}

type APIToken struct {
	TokenId string `json:"tokenid"`
	Value   string `json:"value"`
}

func (proxmoxSess *ProxmoxSession) CreateAPIToken() (*APIToken, error) {
	if proxmoxSess.HTTPToken == nil {
		return nil, fmt.Errorf("CreateAPIToken: token required")
	}

	_ = proxmoxSess.ProxmoxHTTPRequest(
		http.MethodDelete,
		fmt.Sprintf("/api2/json/access/users/%s/token/pbs-plus-auth", proxmoxSess.HTTPToken.Username),
		nil,
		nil,
	)

	reqBody, err := json.Marshal(&APITokenRequest{
		Comment: "Autogenerated API token for PBS Plus",
	})
	if err != nil {
		return nil, fmt.Errorf("CreateAPIToken: error creating req body -> %w", err)
	}

	var tokenResp APITokenResponse
	err = proxmoxSess.ProxmoxHTTPRequest(
		http.MethodPost,
		fmt.Sprintf("/api2/json/access/users/%s/token/pbs-plus-auth", proxmoxSess.HTTPToken.Username),
		bytes.NewBuffer(reqBody),
		&tokenResp,
	)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return nil, fmt.Errorf("CreateAPIToken: error executing http request token post -> %w", err)
		}
	}

	aclBody, err := json.Marshal(&ACLRequest{
		AuthId: tokenResp.Data.TokenId,
		Role:   "Admin",
		Path:   "/",
	})
	if err != nil {
		return nil, fmt.Errorf("CreateAPIToken: error creating acl body -> %w", err)
	}

	err = proxmoxSess.ProxmoxHTTPRequest(
		http.MethodPut,
		"/api2/json/access/acl",
		bytes.NewBuffer(aclBody),
		nil,
	)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return nil, fmt.Errorf("CreateAPIToken: error executing http request acl put -> %w", err)
		}
	}

	return &tokenResp.Data, nil
}

func (token *APIToken) SaveToFile() error {
	if token == nil {
		return nil
	}

	tokenFileContent, _ := json.Marshal(token)
	file, err := os.OpenFile(filepath.Join(constants.DbBasePath, "pbs-plus-token.json"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(string(tokenFileContent))
	if err != nil {
		return err
	}

	return nil
}

func GetAPITokenFromFile() (*APIToken, error) {
	jsonFile, err := os.Open(filepath.Join(constants.DbBasePath, "pbs-plus-token.json"))
	if err != nil {
		return nil, err
	}
	defer jsonFile.Close()

	byteValue, err := io.ReadAll(jsonFile)
	if err != nil {
		return nil, err
	}

	var result APIToken
	err = json.Unmarshal([]byte(byteValue), &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}
