package arpc

import (
	"fmt"
	"net"
)

// writeErrorResponse sends an error response over the connection
func writeErrorResponse(conn net.Conn, status int, err error) {
	// Wrap the error in a SerializableError
	serErr := WrapError(err)

	// Encode the SerializableError
	errBytes, encodeErr := serErr.Encode()
	if encodeErr != nil {
		conn.Write([]byte(fmt.Sprintf("failed to encode error: %v", encodeErr)))
		return
	}

	// Build the error response
	resp := Response{
		Status:  status,
		Message: err.Error(),
		Data:    errBytes,
	}

	// Encode and write the error response
	respBytes, encodeErr := resp.Encode()
	if encodeErr != nil {
		conn.Write([]byte(fmt.Sprintf("failed to encode response: %v", encodeErr)))
		return
	}

	conn.Write(respBytes)
}
