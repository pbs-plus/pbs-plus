package web

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// WatchAndServe starts the HTTPS server and restarts it if the certificate files change.
// It exits when the done channel is closed.
func WatchAndServe(apiServer *http.Server, certFile, keyFile string, watcherFiles []string, done <-chan struct{}) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		syslog.L.Error(err).WithMessage("api server watcher error").Write()
		return
	}
	defer watcher.Close()
	for _, f := range watcherFiles {
		if err := watcher.Add(f); err != nil {
			syslog.L.Error(err).WithMessage("api server watcher error").Write()
			return
		}
	}
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) != 0 {
					syslog.L.Info().WithMessage("certificate file has changed").WithFields(map[string]any{"name": event.Name, "operation": event.Op}).Write()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err := apiServer.Shutdown(ctx)
					cancel()
					if err != nil {
						syslog.L.Error(err).WithMessage("api server shutdown error").Write()
					}
				}
			case err := <-watcher.Errors:
				syslog.L.Error(err).WithMessage("api server watcher error").Write()
			}
		}
	}()
	for {
		select {
		case <-done:
			syslog.L.Info().WithMessage("WatchAndServe: shutting down").Write()
			return
		default:
		}

		syslog.L.Info().WithMessage(fmt.Sprintf("Starting HTTPS server on %s...", apiServer.Addr)).Write()
		err := apiServer.ListenAndServeTLS(certFile, keyFile)
		if err != nil && err != http.ErrServerClosed {
			syslog.L.Error(err).WithMessage("server failed").Write()
		}

		select {
		case <-done:
			return
		case <-time.After(time.Second):
		}
	}
}
