//go:build linux

package web

import (
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/log"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
)

func CORS(app *application.Runtime, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		allowedOrigin := r.Header.Get("Origin")
		if allowedOrigin != "" {
			allowedHeaders := r.Header.Get("Access-Control-Request-Headers")
			if allowedHeaders == "" {
				allowedHeaders = "*"
			}

			allowedMethods := r.Header.Get("Access-Control-Request-Method")
			if allowedMethods == "" {
				allowedMethods = "POST, GET, OPTIONS, PUT, DELETE"
			}

			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Set("Access-Control-Allow-Methods", allowedMethods)
			w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Expose-Headers", "Set-Cookie")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte{})
			if err != nil {
				log.Error(err, "cannot send 200 answer")
			}
			return
		}

		next.ServeHTTP(w, r)
	}
}
