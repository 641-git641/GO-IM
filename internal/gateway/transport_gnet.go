package gateway

import (
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// gnetTransport wraps a gnet.Conn as a Transport.
// Outbound messages are framed with a 4-byte big-endian length prefix
// (same format as the inbound gnet TCP wire protocol).
type gnetTransport struct {
	conn         gnet.Conn
	writeTimeout time.Duration
}

// newGnetTransport creates a gnet Transport.
func newGnetTransport(conn gnet.Conn) *gnetTransport {
	return &gnetTransport{conn: conn, writeTimeout: 10 * time.Second}
}

func (t *gnetTransport) Close() error {
	return t.conn.Close()
}

// frameBufPool reduces allocations for the 4-byte header + payload framing.
var frameBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

func (t *gnetTransport) Write(p []byte) error {
	// Frame with 4-byte big-endian length prefix.
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(p)))

	// Use pool for frame buffer to reduce per-write allocations.
	bufPtr := frameBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	buf = append(buf, header...)
	buf = append(buf, p...)
	frame := make([]byte, len(buf))
	copy(frame, buf)
	*bufPtr = buf[:0]
	frameBufPool.Put(bufPtr)

	// Set write deadline so a stuck TCP connection doesn't block forever.
	if t.writeTimeout > 0 {
		if err := t.conn.SetWriteDeadline(time.Now().Add(t.writeTimeout)); err != nil {
			log.Printf("[gnet] set write deadline error fd=%d: %v", t.conn.Fd(), err)
		}
	}

	// AsyncWrite with error callback so silent write failures are logged.
	return t.conn.AsyncWrite(frame, func(c gnet.Conn, err error) error {
		if err != nil {
			log.Printf("[gnet] async write error fd=%d: %v", c.Fd(), err)
		}
		return nil
	})
}
