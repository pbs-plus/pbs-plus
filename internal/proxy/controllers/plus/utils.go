package plus

import (
	"io"
	"net/http"
)

func proxyUrl(targetURL string, w http.ResponseWriter, r *http.Request) {
	// Proxy the request
	req, err := http.NewRequest(http.MethodGet, targetURL, nil)
	if err != nil {
		http.Error(w, "failed to create proxy request", http.StatusInternalServerError)
		return
	}

	// Copy headers from the original request to the proxy request
	copyHeaders(r.Header, req.Header)

	// Perform the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, "failed to fetch binary", http.StatusInternalServerError)
		return
	}
	defer resp.Body.Close()

	// Copy headers from the upstream response to the client response
	copyHeaders(resp.Header, w.Header())

	// Set the status code and copy the body
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		http.Error(w, "failed to write response body", http.StatusInternalServerError)
		return
	}
}
