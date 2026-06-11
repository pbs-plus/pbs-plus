package pxar

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	pxar "github.com/pbs-plus/pxar"
)

type Client struct {
	pipe  *arpc.StreamPipe
	pr    *PxarReader
	errCh chan error
	name  string
}

func NewRemoteClient(pipe *arpc.StreamPipe, name string) *Client {
	return &Client{pipe: pipe, name: name}
}

func NewLocalClient(pr *PxarReader, name string) (*Client, chan error) {
	errCh := make(chan error, 16)
	return &Client{pr: pr, errCh: errCh, name: name}, errCh
}

func (c *Client) SendError(ctx context.Context, err error) error {
	if c.pipe != nil {
		params := map[string]any{
			"error": err.Error(),
		}
		syslog.L.Error(err).WithField("restore", "error").Write()
		if err := c.pipe.Call(ctx, "pxar.Error", params, nil); err != nil {
			return err
		}
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.errCh <- err:
		return nil
	default:
		return nil
	}
}

func (c *Client) GetRoot(ctx context.Context) (pxar.FileInfo, error) {
	if c.pipe != nil {
		var info pxar.FileInfo
		if err := c.pipe.Call(ctx, "pxar.GetRoot", nil, &info); err != nil {
			return pxar.FileInfo{}, err
		}
		return info, nil
	}

	info, err := c.pr.GetRoot(ctx)
	if err != nil {
		return pxar.FileInfo{}, err
	}

	return *info, nil
}

func (c *Client) LookupByPath(ctx context.Context, path string) (pxar.FileInfo, error) {
	if c.pipe != nil {
		params := map[string]any{
			"path": path,
		}
		var info pxar.FileInfo
		if err := c.pipe.Call(ctx, "pxar.LookupByPath", params, &info); err != nil {
			return pxar.FileInfo{}, err
		}
		return info, nil
	}

	info, err := c.pr.LookupByPath(ctx, path)
	if err != nil {
		return pxar.FileInfo{}, err
	}

	return *info, nil
}

func (c *Client) ReadDir(ctx context.Context, entryEnd uint64) ([]pxar.FileInfo, error) {
	if c.pipe != nil {
		params := map[string]any{
			"entry_end": entryEnd,
		}
		var entries []pxar.FileInfo
		if err := c.pipe.Call(ctx, "pxar.ReadDir", params, &entries); err != nil {
			return nil, err
		}
		return entries, nil
	}

	info, err := c.pr.ReadDir(ctx, entryEnd)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (c *Client) GetAttr(ctx context.Context, entryStart, entryEnd uint64) (pxar.FileInfo, error) {
	if c.pipe != nil {
		params := map[string]any{
			"entry_start": entryStart,
			"entry_end":   entryEnd,
		}
		var info pxar.FileInfo
		if err := c.pipe.Call(ctx, "pxar.GetAttr", params, &info); err != nil {
			return pxar.FileInfo{}, err
		}
		return info, nil
	}

	info, err := c.pr.GetAttr(ctx, entryStart, entryEnd)
	if err != nil {
		return pxar.FileInfo{}, err
	}

	return *info, nil
}

func (c *Client) ReadLink(ctx context.Context, entryStart, entryEnd uint64) ([]byte, error) {
	if c.pipe != nil {
		params := map[string]any{
			"entry_start": entryStart,
			"entry_end":   entryEnd,
		}
		var target []byte
		if err := c.pipe.Call(ctx, "pxar.ReadLink", params, &target); err != nil {
			return nil, err
		}
		return target, nil
	}

	info, err := c.pr.ReadLink(ctx, entryStart, entryEnd)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (c *Client) ListXAttrs(ctx context.Context, entryStart, entryEnd uint64) (map[string][]byte, error) {
	if c.pipe != nil {
		params := map[string]any{
			"entry_start": entryStart,
			"entry_end":   entryEnd,
		}
		var xattrs map[string][]byte
		if err := c.pipe.Call(ctx, "pxar.ListXAttrs", params, &xattrs); err != nil {
			return nil, err
		}
		return xattrs, nil
	}

	info, err := c.pr.ListXAttrs(ctx, entryStart, entryEnd)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (c *Client) Close() error {
	if c.pipe != nil {
		if err := c.pipe.Call(context.Background(), "pxar.Done", nil, nil); err != nil {
			return err
		}
		if c.pipe != nil {
			c.pipe.Close()
		}
		return nil
	}

	return c.pr.Close()
}

// ReadFileContentReader returns a streaming reader for file content identified
// by content offset range. Works for both local and remote clients.
// fileSize is used for the remote streaming protocol.
// The caller must close the returned reader.
func (c *Client) ReadFileContentReader(ctx context.Context, contentStart, contentEnd, fileSize uint64) (io.ReadCloser, error) {
	if c.pipe != nil {
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()

			params := map[string]uint64{
				"content_start": contentStart,
				"content_end":   contentEnd,
				"file_size":     fileSize,
			}
			handler := arpc.RawStreamHandler(func(stream arpc.ARPCStream) error {
				defer stream.Close()

				// Read the 14-byte SendDataFromReader header
				var hdr [14]byte
				if _, err := io.ReadFull(stream, hdr[:]); err != nil {
					return fmt.Errorf("read stream header: %w", err)
				}
				if binary.LittleEndian.Uint32(hdr[0:4]) != 0x4E465353 {
					return fmt.Errorf("invalid stream magic")
				}
				totalLength := binary.LittleEndian.Uint64(hdr[6:14])
				if totalLength == 0 {
					return nil
				}

				// Stream content to pipe writer
				_, err := io.CopyN(pw, stream, int64(totalLength))
				return err
			})

			if err := c.pipe.Call(ctx, "pxar.ReadStream", params, handler); err != nil {
				pw.CloseWithError(err)
			}
		}()
		return pr, nil
	}
	return c.pr.ReadFileContentReader(ctx, contentStart, contentEnd)
}
