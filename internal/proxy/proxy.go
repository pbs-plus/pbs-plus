//go:build linux

package proxy

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/andybalholm/brotli"
	"github.com/sonroyaalmerol/pbs-plus/internal/store"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func decompressBody(resp *http.Response) ([]byte, error) {
	var body []byte
	var err error

	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gzReader.Close()
		body, err = io.ReadAll(gzReader)

	case "deflate":
		deflateReader := flate.NewReader(resp.Body)
		defer deflateReader.Close()
		body, err = io.ReadAll(deflateReader)

	case "br": // Brotli
		brReader := brotli.NewReader(resp.Body)
		body, err = io.ReadAll(brReader)

	case "identity", "": // No compression
		body, err = io.ReadAll(resp.Body)

	default:
		// Unknown encoding, return an error
		err = io.ErrUnexpectedEOF
	}

	return body, err
}

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
			body, err := decompressBody(resp)
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
