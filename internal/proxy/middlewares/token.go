//go:build linux

package middlewares

import (
	"log"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/store"
)

func CORS(store *store.Store, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		allowedOrigin := r.Header.Get("Origin")
		if allowedOrigin != "" {
			allowedHeaders := r.Header.Get("Access-Control-Request-Headers")
			if allowedHeaders == "" {
				allowedHeaders = "Content-Type, *"
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
				log.Printf("cannot send 200 answer → %v", err)
			}
			return
		}

		next.ServeHTTP(w, r)
	}
}
