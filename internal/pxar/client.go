package pxar

import (
	"context"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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

	if c.errCh != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.errCh <- err:
			return nil
		default:
			return nil
		}
	}

	return fmt.Errorf("no pipe or errCh available")
}

func (c *Client) GetRoot(ctx context.Context) (EntryInfo, error) {
	if c.pipe != nil {
		var info EntryInfo
		if err := c.pipe.Call(ctx, "pxar.GetRoot", nil, &info); err != nil {
			return EntryInfo{}, err
		}
		return info, nil
	}

	if c.pr != nil {
		info, err := c.pr.GetRoot(ctx)
		if err != nil {
			return EntryInfo{}, err
		}
		return *info, nil
	}

	return EntryInfo{}, fmt.Errorf("no pipe or pr available")
}

func (c *Client) LookupByPath(ctx context.Context, path string) (EntryInfo, error) {
	if c.pipe != nil {
		params := map[string]any{
			"path": path,
		}
		var info EntryInfo
		if err := c.pipe.Call(ctx, "pxar.LookupByPath", params, &info); err != nil {
			return EntryInfo{}, err
		}
		return info, nil
	}

	if c.pr != nil {
		info, err := c.pr.LookupByPath(ctx, path)
		if err != nil {
			return EntryInfo{}, err
		}
		return *info, nil
	}

	return EntryInfo{}, fmt.Errorf("no pipe or pr available")
}

func (c *Client) ReadDir(ctx context.Context, entryEnd uint64) ([]EntryInfo, error) {
	if c.pipe != nil {
		params := map[string]any{
			"entry_end": entryEnd,
		}
		var entries []EntryInfo
		if err := c.pipe.Call(ctx, "pxar.ReadDir", params, &entries); err != nil {
			return nil, err
		}
		return entries, nil
	}

	if c.pr != nil {
		info, err := c.pr.ReadDir(ctx, entryEnd)
		if err != nil {
			return nil, err
		}
		return info, nil
	}

	return nil, fmt.Errorf("no pipe or pr available")
}

func (c *Client) GetAttr(ctx context.Context, entryStart, entryEnd uint64) (EntryInfo, error) {
	if c.pipe != nil {
		params := map[string]any{
			"entry_start": entryStart,
			"entry_end":   entryEnd,
		}
		var info EntryInfo
		if err := c.pipe.Call(ctx, "pxar.GetAttr", params, &info); err != nil {
			return EntryInfo{}, err
		}
		return info, nil
	}

	if c.pr != nil {
		info, err := c.pr.GetAttr(ctx, entryStart, entryEnd)
		if err != nil {
			return EntryInfo{}, err
		}
		return *info, nil
	}

	return EntryInfo{}, fmt.Errorf("no pipe or pr available")
}

func (c *Client) Read(ctx context.Context, contentStart, contentEnd, offset uint64, size uint, data []byte) (int, error) {
	if c.pipe != nil {
		params := map[string]any{
			"content_start": contentStart,
			"content_end":   contentEnd,
			"offset":        offset,
			"size":          size,
			"buf_capacity":  len(data),
		}
		return c.pipe.CallBinary(ctx, "pxar.Read", params, data)
	}

	if c.pr != nil {
		raw, err := c.pr.Read(ctx, contentStart, contentEnd, offset, size)
		if err != nil {
			return 0, err
		}
		n := copy(data, raw)
		return n, nil
	}

	return 0, fmt.Errorf("no pipe or pr available")
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

	if c.pr != nil {
		info, err := c.pr.ReadLink(ctx, entryStart, entryEnd)
		if err != nil {
			return nil, err
		}
		return info, nil
	}

	return nil, fmt.Errorf("no pipe or pr available")
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

	if c.pr != nil {
		info, err := c.pr.ListXAttrs(ctx, entryStart, entryEnd)
		if err != nil {
			return nil, err
		}
		return info, nil
	}

	return nil, fmt.Errorf("no pipe or pr available")
}

func (c *Client) Close() error {
	if c.pipe != nil {
		if err := c.pipe.Call(context.Background(), "pxar.Done", nil, nil); err != nil {
			return err
		}
		c.pipe.Close()
		return nil
	}

	if c.pr != nil {
		return c.pr.Close()
	}

	return fmt.Errorf("no pipe or pr available")
}
