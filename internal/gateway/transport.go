package gateway

// Transport abstracts the underlying connection (WebSocket or raw TCP).
// It provides the minimal interface needed by Client for write and close.
type Transport interface {
	// Close closes the connection.
	Close() error
	// Write sends raw bytes on the connection.
	Write(p []byte) error
}
