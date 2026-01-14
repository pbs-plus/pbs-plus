package pxar

import (
	"context"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

type RemoteClient struct {
	pipe *arpc.StreamPipe
}

func NewRemoteClient(pipe *arpc.StreamPipe) *RemoteClient {
	return &RemoteClient{pipe: pipe}
}

func (c *RemoteClient) GetRoot(ctx context.Context) (EntryInfo, error) {
	var info EntryInfo
	if err := c.pipe.Call(ctx, "pxar.GetRoot", nil, &info); err != nil {
		return EntryInfo{}, err
	}
	return info, nil
}

func (c *RemoteClient) LookupByPath(ctx context.Context, path string) (EntryInfo, error) {
	params := map[string]any{
		"path": path,
	}
	var info EntryInfo
	if err := c.pipe.Call(ctx, "pxar.LookupByPath", params, &info); err != nil {
		return EntryInfo{}, err
	}
	return info, nil
}

func (c *RemoteClient) ReadDir(ctx context.Context, entryEnd uint64) ([]EntryInfo, error) {
	params := map[string]any{
		"entry_end": entryEnd,
	}
	var entries []EntryInfo
	if err := c.pipe.Call(ctx, "pxar.ReadDir", params, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (c *RemoteClient) GetAttr(ctx context.Context, entryStart, entryEnd uint64) (EntryInfo, error) {
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

func (c *RemoteClient) Read(ctx context.Context, contentStart, contentEnd, offset uint64, size uint) ([]byte, error) {
	params := map[string]any{
		"content_start": contentStart,
		"content_end":   contentEnd,
		"offset":        offset,
		"size":          size,
	}
	var data []byte
	if err := c.pipe.Call(ctx, "pxar.Read", params, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func (c *RemoteClient) ReadLink(ctx context.Context, entryStart, entryEnd uint64) ([]byte, error) {
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

func (c *RemoteClient) ListXAttrs(ctx context.Context, entryStart, entryEnd uint64) (map[string][]byte, error) {
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

func (c *RemoteClient) Close() error {
	if err := c.pipe.Call(context.Background(), "pxar.Done", nil, nil); err != nil {
		return err
	}
	if c.pipe != nil {
		c.pipe.Close()
	}
	return nil
}
