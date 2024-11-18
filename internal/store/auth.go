package store

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func CheckAgentAuth(r *http.Request) error {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "PBSPlusAPIAgent=") {
		return fmt.Errorf("CheckAgentAuth: invalid auth prefix")
	}

	privKeyDir := filepath.Join(DbBasePath, "agent_keys")

	authTok := strings.TrimPrefix(auth, "PBSPlusAPIAgent=")
	authSplit := strings.Split(authTok, ":")

	privKeyFilePath := filepath.Join(
		privKeyDir,
		fmt.Sprintf("%s.key", authSplit[0]),
	)

	privKeyFile, err := os.ReadFile(privKeyFilePath)
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: error opening private key file \"%s\" -> %w", privKeyFilePath, err)
	}

	pubKey, err := utils.GeneratePublicKeyFromPrivateKey(privKeyFile)
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: error generating pub key \"%s\" -> %w", privKeyFilePath, err)
	}

	passedPub, err := base64.StdEncoding.DecodeString(authSplit[1])
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: error pub key -> %w", err)
	}

	if !reflect.DeepEqual(pubKey, passedPub) {
		return fmt.Errorf("CheckAgentAuth: invalid auth")
	}

	return nil
}
