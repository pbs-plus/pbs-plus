package pxar

import (
	"context"
	"io"

	"github.com/fxamacker/cbor/v2"
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
		syslog.L.Error(err).WithField("restore", "error").Write()
		if err := c.pipe.Call(ctx, "pxar.Error", errorReq{Error: err.Error()}, nil); err != nil {
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
		params := lookupByPathReq{Path: path}
		var info pxar.FileInfo
		if err := c.pipe.Call(ctx, "pxar.LookupByPath", &params, &info); err != nil {
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
		params := readDirReq{EntryEnd: entryEnd}
		var entries []pxar.FileInfo
		if err := c.pipe.Call(ctx, "pxar.ReadDir", &params, &entries); err != nil {
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
		params := getAttrReq{EntryStart: entryStart, EntryEnd: entryEnd}
		var info pxar.FileInfo
		if err := c.pipe.Call(ctx, "pxar.GetAttr", &params, &info); err != nil {
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
		params := readLinkReq{EntryStart: entryStart, EntryEnd: entryEnd}
		var target []byte
		if err := c.pipe.Call(ctx, "pxar.ReadLink", &params, &target); err != nil {
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
		params := listXAttrsReq{EntryStart: entryStart, EntryEnd: entryEnd}
		var xattrs map[string][]byte
		if err := c.pipe.Call(ctx, "pxar.ListXAttrs", &params, &xattrs); err != nil {
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

func (c *Client) ReadFileContentReader(ctx context.Context, contentStart, contentEnd, fileSize uint64) (io.ReadCloser, error) {
	if c.pipe != nil {
		pr, pw := io.Pipe()

		streamDone := make(chan struct{})

		go func() {
			select {
			case <-ctx.Done():
				pw.CloseWithError(ctx.Err())
			case <-streamDone:
			}
		}()

		go func() {
			defer close(streamDone)
			const chunkSize = 4 << 20

			buf := make([]byte, chunkSize)
			reqLen := chunkSize
			if int64(reqLen) > int64(fileSize) {
				reqLen = int(fileSize)
			}

			req := readContentReq{
				ContentStart: contentStart,
				ContentEnd:   contentEnd,
				FileSize:     fileSize,
				Length:       reqLen,
			}

			n, resp, err := c.pipe.CallBinaryWithMeta(ctx, "pxar.ReadContent", &req, buf)
			if err != nil {
				pw.CloseWithError(err)
				return
			}

			if n > 0 {
				if _, err := pw.Write(buf[:n]); err != nil {
					pw.CloseWithError(err)
					return
				}
			}

			offset := int64(n)

			if offset >= int64(fileSize) {
				pw.Close()
				return
			}

			var handleResp handleIDResp
			if err := cbor.Unmarshal(resp.Data, &handleResp); err != nil {
				pw.CloseWithError(err)
				return
			}

			defer func() {
				closeReq := closeContentReq{HandleID: handleResp.HandleID}
				_ = c.pipe.Call(context.Background(), "pxar.CloseContent", &closeReq, nil)
			}()

			readReq := readContentAtReq{HandleID: handleResp.HandleID}

			for offset < int64(fileSize) {
				reqLen := chunkSize
				if offset+int64(reqLen) > int64(fileSize) {
					reqLen = int(int64(fileSize) - offset)
				}

				readReq.Offset = offset
				readReq.Length = reqLen

				n, err := c.pipe.CallBinary(ctx, "pxar.ReadContentAt", &readReq, buf)
				if err != nil {
					pw.CloseWithError(err)
					return
				}
				if n == 0 {
					break
				}
				if _, err := pw.Write(buf[:n]); err != nil {
					pw.CloseWithError(err)
					return
				}
				offset += int64(n)
			}
			pw.Close()
		}()
		return pr, nil
	}
	return c.pr.ReadFileContentReader(ctx, contentStart, contentEnd)
}
