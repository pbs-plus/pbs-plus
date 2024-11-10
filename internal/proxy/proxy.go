//go:build linux

package proxy

import (
	"bytes"
	"compress/flate"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func CreateProxy(target *url.URL, storeInstance *store.Store) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director

	proxy.Transport = utils.BaseTransport
	proxy.Director = func(req *http.Request) {
		ExtractTokenFromRequest(req, storeInstance)
		originalDirector(req)
	}

	proxy.ModifyResponse = func(resp *http.Response) error {
		if strings.HasSuffix(resp.Request.URL.Path, store.ModifiedFilePath) {
			var body []byte
			var err error

			body, err = io.ReadAll(flate.NewReader(resp.Body))
			if err != nil {
				return err
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			modifiedContent := append(body, []byte("\n// Modified by PBS Plus Overlay\n")...)
			modifiedContent = append(modifiedContent, compileCustomJS()...)

			resp.Body = io.NopCloser(bytes.NewReader(modifiedContent))
			resp.Header.Del("Content-Length")   // Remove Content-Length to enable chunked encoding
			resp.Header.Del("Content-Encoding") // Clear Content-Encoding to send plain text
		}

		return nil
	}

	return proxy
}
