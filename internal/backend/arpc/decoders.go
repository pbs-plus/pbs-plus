package arpcfs

import (
	"fmt"
	"os"

	"github.com/valyala/fastjson"
)

func decodeFileInfoResponse(v *fastjson.Value, fi *FileInfoResponse) error {
	// Use fastjson’s getters directly.
	// Make sure that the expected field names and types match what the server sends.
	fi.ModTimeUnix = v.Get("modTimeUnix").GetInt64()
	fi.Size = v.Get("size").GetInt64()
	// For mode, we assume it is sent as an integer.
	fi.Mode = os.FileMode(v.Get("mode").GetInt())
	fi.IsDir = v.Get("isDir").GetBool()
	return nil
}

func decodeReadDirResponse(v *fastjson.Value, resp *ReadDirResponse) error {
	// Assume that the JSON object has an "entries" field that is an array.
	entriesVal := v.Get("entries")
	if entriesVal == nil {
		return fmt.Errorf("no entries field in ReadDir response")
	}
	// Get all array elements.
	arr, err := entriesVal.Array()
	if err != nil {
		return err
	}
	resp.Entries = make([]FileInfoResponse, len(arr))
	for i, el := range arr {
		// For each entry, extract the expected fields.
		name := string(el.Get("name").GetStringBytes())
		size := el.Get("size").GetInt64()
		mode := el.Get("mode").GetInt()
		modTimeUni := el.Get("modTimeUnix").GetInt64()
		isDir := el.Get("isDir").GetBool()
		resp.Entries[i] = FileInfoResponse{
			Name:        name,
			Size:        size,
			Mode:        os.FileMode(mode),
			ModTimeUnix: modTimeUni,
			IsDir:       isDir,
		}
	}
	return nil
}
