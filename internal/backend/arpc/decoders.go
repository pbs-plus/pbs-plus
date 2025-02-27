package arpcfs

import "github.com/goccy/go-json"

// decodeFileInfoResponse unmarshals data into fi.
func decodeFileInfoResponse(data json.RawMessage, fi *FileInfoResponse) error {
	return json.Unmarshal(data, fi)
}

// decodeReadDirResponse unmarshals data into resp.
func decodeReadDirResponse(data json.RawMessage, resp *ReadDirResponse) error {
	return json.Unmarshal(data, resp)
}
