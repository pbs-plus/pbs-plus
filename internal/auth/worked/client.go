package auth

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"os"
)

type Agent struct {
	id        string
	token     string
	TLSConfig *tls.Config
}

func NewAgent(id string, certFile, keyFile, caFile string) (*Agent, error) {
	// Load client cert
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	// Load CA cert
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Configure TLS
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	return &Agent{
		id:        id,
		TLSConfig: tlsConfig,
	}, nil
}

func (a *Agent) bootstrap(serverURL string) error {
	req := AgentRequest{
		AgentID: a.id,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: a.TLSConfig,
		},
	}

	resp, err := client.Post(serverURL+"/bootstrap", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var agentResp AgentResponse
	if err := json.NewDecoder(resp.Body).Decode(&agentResp); err != nil {
		return err
	}

	a.token = agentResp.Token
	return nil
}

func (a *Agent) sendData(serverURL, data string) error {
	req := AgentRequest{
		AgentID: a.id,
		Data:    data,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return err
	}

	request, err := http.NewRequest("POST", serverURL+"/secure", bytes.NewBuffer(reqBody))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", a.token)
	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: a.TLSConfig,
		},
	}

	resp, err := client.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var agentResp AgentResponse
	return json.NewDecoder(resp.Body).Decode(&agentResp)
}
