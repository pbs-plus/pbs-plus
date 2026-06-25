package web

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

// WatchAndServe starts the HTTPS server and restarts it if the certificate files change.
// It exits when the done channel is closed.
func WatchAndServe(apiServer *http.Server, certFile, keyFile string, watcherFiles []string, done <-chan struct{}) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Error(err, "api server watcher error")
		return
	}
	defer func() {
		if err := watcher.Close(); err != nil {
			log.Error(err, "")
		}
	}()
	for _, f := range watcherFiles {
		if err := watcher.Add(f); err != nil {
			log.Error(err, "api server watcher error")
			return
		}
	}
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) != 0 {
					log.Info("certificate file has changed")
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := apiServer.Shutdown(ctx)
					cancel()
					if err != nil {
						log.Error(err, "api server shutdown error")
					}
				}
			case err := <-watcher.Errors:
				log.Error(err, "api server watcher error")
			}
		}
	}()
	for {
		select {
		case <-done:
			log.Info("watchAndServe: shutting down")
			return
		default:
		}
		log.Info(fmt.Sprintf("Starting HTTPS server on %s...", apiServer.Addr))
		err := apiServer.ListenAndServeTLS(certFile, keyFile)
		if err != nil && err != http.ErrServerClosed {
			log.Error(err, "server failed")
		}

		select {
		case <-done:
			return
		case <-time.After(time.Second):
		}
	}
}
