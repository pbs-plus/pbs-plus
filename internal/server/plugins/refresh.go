//go:build linux

package plugins

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

// Refresh authenticates one repository index and records its cache validators.
func Refresh(ctx context.Context, db *coredb.Store, fetcher targetplugin.Fetcher, repositoryID string) (targetplugin.RepositoryIndex, bool, error) {
	repository, err := db.GetPluginRepository(ctx, repositoryID)
	if err != nil {
		return targetplugin.RepositoryIndex{}, false, err
	}
	if !repository.Enabled {
		return targetplugin.RepositoryIndex{}, false, fmt.Errorf("plugin repository %q is disabled", repository.ID)
	}
	publicKey, err := parsePublisherKey(repository.PublicKey)
	if err != nil {
		return targetplugin.RepositoryIndex{}, false, recordRefreshFailure(ctx, db, repository, err)
	}

	document, err := fetcher.Index(ctx, repository.URL, targetplugin.PluginRepositoryCache{
		ETag:         repository.ETag,
		LastModified: repository.LastModified,
	})
	if errors.Is(err, targetplugin.ErrRepositoryUnchanged) {
		return targetplugin.RepositoryIndex{}, false, recordRefresh(ctx, db, repository, targetplugin.PluginRepositoryCache{
			ETag:         repository.ETag,
			LastModified: repository.LastModified,
		}, "")
	}
	if err != nil {
		return targetplugin.RepositoryIndex{}, false, recordRefreshFailure(ctx, db, repository, err)
	}

	index, err := targetplugin.ParseRepositoryIndex(document.Index, document.Signature, publicKey)
	if err != nil {
		return targetplugin.RepositoryIndex{}, false, recordRefreshFailure(ctx, db, repository, err)
	}
	if index.RepositoryID != repository.ID {
		return targetplugin.RepositoryIndex{}, false, recordRefreshFailure(ctx, db, repository,
			fmt.Errorf("repository index identifies %q, not %q", index.RepositoryID, repository.ID))
	}
	if err := recordRefresh(ctx, db, repository, targetplugin.PluginRepositoryCache{
		ETag:         document.ETag,
		LastModified: document.LastModified,
	}, ""); err != nil {
		return targetplugin.RepositoryIndex{}, false, err
	}
	return index, true, nil
}

func recordRefreshFailure(ctx context.Context, db *coredb.Store, repository coredb.PluginRepository, cause error) error {
	if err := recordRefresh(ctx, db, repository, targetplugin.PluginRepositoryCache{
		ETag:         repository.ETag,
		LastModified: repository.LastModified,
	}, cause.Error()); err != nil {
		log.Error(err, "failed to record plugin repository refresh failure", "id", repository.ID)
	}
	return cause
}

func recordRefresh(ctx context.Context, db *coredb.Store, repository coredb.PluginRepository, cache targetplugin.PluginRepositoryCache, lastError string) error {
	updated, err := db.RecordPluginRepositoryRefresh(ctx, repository.ID, coredb.PluginRepositoryRefresh{
		ETag:         cache.ETag,
		LastModified: cache.LastModified,
		RefreshedAt:  time.Now(),
		LastError:    lastError,
	})
	if err != nil {
		return err
	}
	if !updated {
		return fmt.Errorf("plugin repository %q was removed during refresh", repository.ID)
	}
	return nil
}
