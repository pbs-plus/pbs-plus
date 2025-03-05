package sb

import "unsafe"

func ToBytes(s string) []byte {
	// unsafe.StringData is unspecified for the empty string, so we provide a strict interpretation
	if len(s) == 0 {
		return nil
	}
	// Copied from go 1.20.1 os.File.WriteString
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/os/file.go#L246
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func ToString(b []byte) string {
	// unsafe.SliceData relies on cap whereas we want to rely on len
	if len(b) == 0 {
		return ""
	}
	// Copied from go 1.20.1 strings.Builder.String
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/strings/builder.go#L48
	return unsafe.String(unsafe.SliceData(b), len(b))
}
