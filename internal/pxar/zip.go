package pxar

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/klauspost/compress/zip"
)

func restoreAsZips(ctx context.Context, client *Client, sources []string, opts RestoreOptions) error {
	errCh := make(chan error, 100)
	var errWg sync.WaitGroup
	errWg.Go(func() {
		for err := range errCh {
			_ = client.SendError(ctx, err)
		}
	})

	var sourcesWg sync.WaitGroup
	for _, source := range sources {
		if ctx.Err() != nil {
			break
		}

		sourcesWg.Add(1)
		go func(src string) {
			defer sourcesWg.Done()

			if err := createZipForSource(ctx, client, src, opts, errCh); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}(source)
	}

	sourcesWg.Wait()
	close(errCh)
	errWg.Wait()

	return ctx.Err()
}

func createZipForSource(ctx context.Context, client *Client, source string, opts RestoreOptions, errCh chan<- error) error {
	sourceAttr, err := client.LookupByPath(ctx, source)
	if err != nil {
		return fmt.Errorf("lookup source %q: %w", source, err)
	}

	baseName := sourceAttr.Name()
	if strings.TrimSpace(baseName) == "" {
		baseName = client.name
	}

	zipPath := filepath.Join(opts.DestDir, baseName+".zip")
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return fmt.Errorf("create zip %q: %w", zipPath, err)
	}
	defer zipFile.Close()

	bufferedWriter := bufio.NewWriterSize(zipFile, 16*1024*1024)
	zipWriter := zip.NewWriter(bufferedWriter)

	zc := &zipContext{
		ctx:    ctx,
		client: client,
		writer: zipWriter,
		base:   sourceAttr.Name(),
		errCh:  errCh,
	}

	if sourceAttr.IsDir() {
		zc.addDirectory("", sourceAttr)
	} else {
		if sourceAttr.IsFile() {
			zc.addFile("", sourceAttr)
		} else if sourceAttr.IsSymlink() {
			zc.addSymlink("", sourceAttr)
		}
	}

	if err := zipWriter.Close(); err != nil {
		return err
	}

	return bufferedWriter.Flush()
}

type zipContext struct {
	ctx    context.Context
	client *Client
	writer *zip.Writer
	base   string
	errCh  chan<- error
}

func (zc *zipContext) addDirectory(relPath string, dirEntry EntryInfo) {
	dirPath := filepath.Join(relPath, dirEntry.Name()) + "/"

	header := &zip.FileHeader{
		Name:     dirPath,
		Method:   zip.Deflate,
		Modified: dirEntry.ToFileInfo().ModTime(),
	}
	header.SetMode(os.FileMode(dirEntry.Mode) | os.ModeDir)

	if _, err := zc.writer.CreateHeader(header); err != nil {
		zc.sendError(fmt.Errorf("create dir header %q: %w", dirPath, err))
		return
	}

	entries, err := zc.client.ReadDir(zc.ctx, dirEntry.EntryRangeEnd)
	if err != nil {
		zc.sendError(fmt.Errorf("read dir %q: %w", dirPath, err))
		return
	}

	for _, e := range entries {
		if zc.ctx.Err() != nil {
			return
		}

		currentPath := dirPath[:len(dirPath)-1]

		if e.IsDir() {
			zc.addDirectory(currentPath, e)
		} else if e.IsFile() {
			zc.addFile(currentPath, e)
		} else if e.IsSymlink() {
			zc.addSymlink(currentPath, e)
		}
	}
}

func (zc *zipContext) addFile(relPath string, fileEntry EntryInfo) {
	filePath := filepath.Join(relPath, fileEntry.Name())

	header := &zip.FileHeader{
		Name:     filePath,
		Method:   zip.Deflate,
		Modified: fileEntry.ToFileInfo().ModTime(),
	}
	header.SetMode(os.FileMode(fileEntry.Mode))

	writer, err := zc.writer.CreateHeader(header)
	if err != nil {
		zc.sendError(fmt.Errorf("create file header %q: %w", filePath, err))
		return
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
			zc.sendError(fmt.Errorf("copy file data %q: %w", filePath, err))
			return
		}
	}
}

func (zc *zipContext) addSymlink(relPath string, symlinkEntry EntryInfo) {
	linkPath := filepath.Join(relPath, symlinkEntry.Name())

	target, err := zc.client.ReadLink(zc.ctx, symlinkEntry.EntryRangeStart, symlinkEntry.EntryRangeEnd)
	if err != nil {
		zc.sendError(fmt.Errorf("read symlink %q: %w", linkPath, err))
		return
	}

	header := &zip.FileHeader{
		Name:     linkPath,
		Method:   zip.Deflate,
		Modified: symlinkEntry.ToFileInfo().ModTime(),
	}
	header.SetMode(os.FileMode(symlinkEntry.Mode) | os.ModeSymlink)

	writer, err := zc.writer.CreateHeader(header)
	if err != nil {
		zc.sendError(fmt.Errorf("create symlink header %q: %w", linkPath, err))
		return
	}

	if _, err := writer.Write(target); err != nil {
		zc.sendError(fmt.Errorf("write symlink target %q: %w", linkPath, err))
		return
	}
}

func (zc *zipContext) sendError(err error) {
	select {
	case zc.errCh <- err:
	default:
	}
}
