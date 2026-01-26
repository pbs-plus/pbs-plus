package pxar

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const ZipBufferSize = 16 * 1024 * 1024

type asyncWriter struct {
	file      *os.File
	bufSize   int
	writeCh   chan []byte
	errCh     chan error
	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func newAsyncWriter(file *os.File) *asyncWriter {
	aw := &asyncWriter{
		file:    file,
		bufSize: ZipBufferSize,
		writeCh: make(chan []byte, 8),
		errCh:   make(chan error, 1),
		closeCh: make(chan struct{}),
	}

	aw.wg.Add(1)
	go aw.writerLoop()

	return aw
}

func (aw *asyncWriter) writerLoop() {
	defer aw.wg.Done()

	buffer := make([]byte, 0, aw.bufSize)

	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		_, err := aw.file.Write(buffer)
		buffer = buffer[:0]
		return err
	}

	for {
		select {
		case data, ok := <-aw.writeCh:
			if !ok {
				if err := flush(); err != nil {
					select {
					case aw.errCh <- err:
					default:
					}
				}
				return
			}

			if len(buffer)+len(data) > cap(buffer) {
				if err := flush(); err != nil {
					select {
					case aw.errCh <- err:
					default:
					}
					return
				}
			}

			buffer = append(buffer, data...)

		case <-aw.closeCh:
			for len(aw.writeCh) > 0 {
				data := <-aw.writeCh
				if len(buffer)+len(data) > cap(buffer) {
					if err := flush(); err != nil {
						select {
						case aw.errCh <- err:
						default:
						}
						return
					}
				}
				buffer = append(buffer, data...)
			}
			if err := flush(); err != nil {
				select {
				case aw.errCh <- err:
				default:
				}
			}
			return
		}
	}
}

func (aw *asyncWriter) Write(p []byte) (n int, err error) {
	select {
	case err := <-aw.errCh:
		return 0, err
	default:
	}

	data := make([]byte, len(p))
	copy(data, p)

	select {
	case aw.writeCh <- data:
		return len(p), nil
	case err := <-aw.errCh:
		return 0, err
	}
}

func (aw *asyncWriter) Close() error {
	var closeErr error
	aw.closeOnce.Do(func() {
		close(aw.writeCh)
		aw.wg.Wait()

		select {
		case err := <-aw.errCh:
			closeErr = err
		default:
		}

		if err := aw.file.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

func restoreAsZips(ctx context.Context, client *Client, sources []string, opts RestoreOptions) error {
	var sourcesWg sync.WaitGroup
	errCh := make(chan error, len(sources))

	for _, source := range sources {
		if ctx.Err() != nil {
			break
		}

		sourcesWg.Add(1)
		go func(src string) {
			defer sourcesWg.Done()

			if err := createZipForSource(ctx, client, src, opts); err != nil {
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

func createZipForSource(ctx context.Context, client *Client, source string, opts RestoreOptions) error {
	sourceAttr, err := client.LookupByPath(ctx, source)
	if err != nil {
		return fmt.Errorf("lookup source %q: %w", source, err)
	}

	zipName := sourceAttr.Name() + ".zip"
	zipPath := filepath.Join(opts.DestDir, zipName)

	zipFile, err := os.Create(zipPath)
	if err != nil {
		return fmt.Errorf("create zip %q: %w", zipPath, err)
	}

	asyncWriter := newAsyncWriter(zipFile)
	defer asyncWriter.Close()

	zipWriter := zip.NewWriter(asyncWriter)
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
