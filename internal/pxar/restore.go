package pxar

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

type RestoreMode int

const (
	RestoreModeNormal RestoreMode = iota
	RestoreModeZip
)

type RestoreOptions struct {
	Mode    RestoreMode
	DestDir string
}

type restoreJob struct {
	dest string
	info EntryInfo
}

func Restore(ctx context.Context, client *Client, sources []string, destDir string) error {
	return RestoreWithOptions(ctx, client, sources, RestoreOptions{
		Mode:    RestoreModeNormal,
		DestDir: destDir,
	})
}

func RestoreWithOptions(ctx context.Context, client *Client, sources []string, opts RestoreOptions) error {
	if err := os.MkdirAll(opts.DestDir, 0o755); err != nil {
		return fmt.Errorf("mkdir root: %w", err)
	}

	if opts.Mode == RestoreModeZip {
		return restoreAsZips(ctx, client, sources, opts.DestDir)
	}

	return restoreNormal(ctx, client, sources, opts.DestDir)
}

func restoreAsZips(ctx context.Context, client *Client, sources []string, destDir string) error {
	var sourcesWg sync.WaitGroup
	errCh := make(chan error, len(sources))

	for _, source := range sources {
		if ctx.Err() != nil {
			break
		}

		sourcesWg.Add(1)
		go func(src string) {
			defer sourcesWg.Done()

			if err := createZipForSource(ctx, client, src, destDir); err != nil {
				select {
				case errCh <- err:
					_ = client.SendError(ctx, err)
				default:
				}
			}
		}(source)
	}

	sourcesWg.Wait()
	close(errCh)

	for err := range errCh {
		return err
	}

	return ctx.Err()
}

func createZipForSource(ctx context.Context, client *Client, source, destDir string) error {
	sourceAttr, err := client.LookupByPath(ctx, source)
	if err != nil {
		return fmt.Errorf("lookup source %q: %w", source, err)
	}

	zipName := sourceAttr.Name() + ".zip"
	zipPath := filepath.Join(destDir, zipName)

	zipFile, err := os.Create(zipPath)
	if err != nil {
		return fmt.Errorf("create zip %q: %w", zipPath, err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	zc := &zipContext{
		ctx:    ctx,
		client: client,
		writer: zipWriter,
		base:   sourceAttr.Name(),
	}

	if sourceAttr.IsDir() {
		return zc.addDirectory("", sourceAttr)
	} else if sourceAttr.IsFile() {
		return zc.addFile("", sourceAttr)
	} else if sourceAttr.IsSymlink() {
		return zc.addSymlink("", sourceAttr)
	}

	return nil
}

type zipContext struct {
	ctx    context.Context
	client *Client
	writer *zip.Writer
	base   string
}

func (zc *zipContext) addDirectory(relPath string, dirEntry EntryInfo) error {
	dirPath := filepath.Join(relPath, dirEntry.Name()) + "/"
	if relPath == "" {
		dirPath = dirEntry.Name() + "/"
	}

	header := &zip.FileHeader{
		Name:     dirPath,
		Method:   zip.Deflate,
		Modified: dirEntry.ToFileInfo().ModTime(),
	}
	header.SetMode(os.FileMode(dirEntry.Mode) | os.ModeDir)

	if _, err := zc.writer.CreateHeader(header); err != nil {
		return fmt.Errorf("create dir header %q: %w", dirPath, err)
	}

	entries, err := zc.client.ReadDir(zc.ctx, dirEntry.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("read dir %q: %w", dirPath, err)
	}

	for _, e := range entries {
		if zc.ctx.Err() != nil {
			return zc.ctx.Err()
		}

		currentPath := dirPath[:len(dirPath)-1] // remove trailing slash

		if e.IsDir() {
			if err := zc.addDirectory(currentPath, e); err != nil {
				return err
			}
		} else if e.IsFile() {
			if err := zc.addFile(currentPath, e); err != nil {
				return err
			}
		} else if e.IsSymlink() {
			if err := zc.addSymlink(currentPath, e); err != nil {
				return err
			}
		}
	}

	return nil
}

func (zc *zipContext) addFile(relPath string, fileEntry EntryInfo) error {
	filePath := filepath.Join(relPath, fileEntry.Name())

	header := &zip.FileHeader{
		Name:     filePath,
		Method:   zip.Deflate,
		Modified: fileEntry.ToFileInfo().ModTime(),
	}
	header.SetMode(os.FileMode(fileEntry.Mode))

	writer, err := zc.writer.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("create file header %q: %w", filePath, err)
	}

	if fileEntry.Size > 0 && fileEntry.ContentRange != nil {
		rr := &rangeReader{
			ctx:          zc.ctx,
			client:       zc.client,
			contentStart: fileEntry.ContentRange[0],
			contentEnd:   fileEntry.ContentRange[1],
			totalSize:    fileEntry.Size,
		}

		if _, err := io.Copy(writer, rr); err != nil {
			return fmt.Errorf("copy file data %q: %w", filePath, err)
		}
	}

	return nil
}

func (zc *zipContext) addSymlink(relPath string, symlinkEntry EntryInfo) error {
	linkPath := filepath.Join(relPath, symlinkEntry.Name())

	target, err := zc.client.ReadLink(zc.ctx, symlinkEntry.EntryRangeStart, symlinkEntry.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("read symlink %q: %w", linkPath, err)
	}

	header := &zip.FileHeader{
		Name:     linkPath,
		Method:   zip.Deflate,
		Modified: symlinkEntry.ToFileInfo().ModTime(),
	}
	header.SetMode(os.FileMode(symlinkEntry.Mode) | os.ModeSymlink)

	writer, err := zc.writer.CreateHeader(header)
	if err != nil {
		return fmt.Errorf("create symlink header %q: %w", linkPath, err)
	}

	if _, err := writer.Write(target); err != nil {
		return fmt.Errorf("write symlink target %q: %w", linkPath, err)
	}

	return nil
}

func restoreNormal(ctx context.Context, client *Client, sources []string, destDir string) error {
	fsCap := getFilesystemCapabilities(destDir)

	numCPU := runtime.NumCPU()
	numWorkers := numCPU * 2
	if fsCap.prefersSequentialOps {
		numWorkers = min(numCPU, 2)
	}

	jobs := make(chan restoreJob, 1024)
	var wg sync.WaitGroup

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var workersWg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workersWg.Go(func() {
			for job := range jobs {
				if err := processJob(workerCtx, client, job, jobs, fsCap, &wg); err != nil {
					_ = client.SendError(workerCtx, err)
				}
				wg.Done()
			}
		})
	}

	var sourcesWg sync.WaitGroup
	for _, source := range sources {
		if workerCtx.Err() != nil {
			break
		}

		sourcesWg.Add(1)
		go func(src string) {
			defer sourcesWg.Done()
			if workerCtx.Err() != nil {
				return
			}

			sourceAttr, err := client.LookupByPath(workerCtx, src)
			if err != nil {
				_ = client.SendError(workerCtx, err)
				return
			}
			path := filepath.Join(destDir, sourceAttr.Name())

			wg.Add(1)
			select {
			case jobs <- restoreJob{dest: path, info: sourceAttr}:
			case <-workerCtx.Done():
				wg.Done()
			}
		}(source)
	}

	sourcesWg.Wait()
	wg.Wait()
	close(jobs)
	workersWg.Wait()

	return ctx.Err()
}

func processJob(ctx context.Context, client *Client, job restoreJob, jobs chan<- restoreJob, fsCap filesystemCapabilities, wg *sync.WaitGroup) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if job.info.IsDir() {
		return restoreDir(ctx, client, job.dest, job.info, jobs, fsCap, wg)
	}
	if job.info.IsSymlink() {
		return restoreSymlink(ctx, client, job.dest, job.info, fsCap)
	}
	if job.info.IsFile() {
		return restoreFile(ctx, client, job.dest, job.info, fsCap)
	}
	return nil
}

func restoreFile(ctx context.Context, client *Client, path string, e EntryInfo, fsCap filesystemCapabilities) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}

	if e.Size > 0 && e.ContentRange != nil {
		rr := &rangeReader{
			ctx:          ctx,
			client:       client,
			contentStart: e.ContentRange[0],
			contentEnd:   e.ContentRange[1],
			totalSize:    e.Size,
		}

		if _, err := io.Copy(f, rr); err != nil {
			f.Close()
			return fmt.Errorf("copy data %q: %w", path, err)
		}
	}

	return applyMeta(ctx, client, f, e, fsCap)
}

func restoreSymlink(ctx context.Context, client *Client, path string, e EntryInfo, fsCap filesystemCapabilities) error {
	target, err := client.ReadLink(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("readlink data %q: %w", path, err)
	}
	if err := os.Symlink(string(target), path); err != nil {
		return fmt.Errorf("symlink %q: %w", path, err)
	}
	return applyMetaSymlink(ctx, client, path, e, fsCap)
}
