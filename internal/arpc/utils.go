package arpc

import (
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/xtaci/smux"
)

func writeErrorResponse(stream io.Writer, status int, err error) {
	serErr := WrapError(err)

	errBytes, encodeErr := cbor.Marshal(serErr)
	if encodeErr != nil {
		stream.Write([]byte(fmt.Sprintf("failed to encode error: %v", encodeErr)))
		return
	}

	resp := Response{
		Status:  status,
		Message: err.Error(),
		Data:    errBytes,
	}

	respBytes, encodeErr := cbor.Marshal(resp)
	if encodeErr != nil {
		stream.Write([]byte(fmt.Sprintf("failed to encode response: %v", encodeErr)))
		return
	}

	stream.Write(respBytes)
}

func defaultConfig() *smux.Config {
	defaults := smux.DefaultConfig()
	defaults.Version = 2
	defaults.MaxReceiveBuffer = utils.MaxReceiveBuffer
	defaults.MaxStreamBuffer = utils.MaxStreamBuffer
	defaults.MaxFrameSize = 65535

	return defaults
}
