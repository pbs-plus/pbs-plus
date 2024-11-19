//go:build linux

package proxy

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
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
		// Handle Gzip encoding
		gzReader, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
		defer gzReader.Close()
		body, err = io.ReadAll(gzReader)

	case "deflate":
		// Try raw deflate first
		deflateReader := flate.NewReader(resp.Body)
		body, err = io.ReadAll(deflateReader)
		deflateReader.Close()

		if err != nil {
			// Retry with zlib if raw deflate fails
			resp.Body.Close()
			return tryZlibDecompression(resp)
		}

	case "br": // Brotli encoding
		brReader := brotli.NewReader(resp.Body)
		body, err = io.ReadAll(brReader)

	case "identity", "":
		// No compression
		body, err = io.ReadAll(resp.Body)

	default:
		err = errors.New("unsupported Content-Encoding: " + resp.Header.Get("Content-Encoding"))
	}

	return body, err
}

// tryZlibDecompression attempts to decompress using zlib for cases where deflate fails.
func tryZlibDecompression(resp *http.Response) ([]byte, error) {
	// Close and retry to get a fresh body stream
	resp.Body.Close()
	newResp, err := http.DefaultTransport.RoundTrip(resp.Request)
	if err != nil {
		return nil, err
	}
	defer newResp.Body.Close()

	zlibReader, err := zlib.NewReader(newResp.Body)
	if err != nil {
		return nil, err
	}
	defer zlibReader.Close()

	return io.ReadAll(zlibReader)
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
