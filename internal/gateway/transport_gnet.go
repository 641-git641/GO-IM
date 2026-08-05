package gateway

import (
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// gnetTransport 将 gnet.Conn 包装为 Transport。
// 出站消息使用 4 字节大端长度前缀进行封帧
// (与入站 gnet TCP 线上协议格式相同)。
type gnetTransport struct {
	conn         gnet.Conn
	writeTimeout time.Duration
}

// newGnetTransport 创建一个 gnet Transport。
func newGnetTransport(conn gnet.Conn) *gnetTransport {
	return &gnetTransport{conn: conn, writeTimeout: 10 * time.Second}
}

func (t *gnetTransport) Close() error {
	return t.conn.Close()
}

// frameBufPool 减少 4 字节头 + 负载封帧时的内存分配。
var frameBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

func (t *gnetTransport) Write(p []byte) error {
	// 使用 4 字节大端长度前缀封帧。
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(p)))

	// 使用池分配帧缓冲区,减少每次写入的内存分配。
	bufPtr := frameBufPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	buf = append(buf, header...)
	buf = append(buf, p...)
	frame := make([]byte, len(buf))
	copy(frame, buf)
	*bufPtr = buf[:0]
	frameBufPool.Put(bufPtr)

	// 设置写超时,使卡住的 TCP 连接不会永久阻塞。
	if t.writeTimeout > 0 {
		if err := t.conn.SetWriteDeadline(time.Now().Add(t.writeTimeout)); err != nil {
			log.Printf("[gnet] set write deadline error fd=%d: %v", t.conn.Fd(), err)
		}
	}

	// 带错误回调的 AsyncWrite,使静默的写入失败被记录日志。
	return t.conn.AsyncWrite(frame, func(c gnet.Conn, err error) error {
		if err != nil {
			log.Printf("[gnet] async write error fd=%d: %v", c.Fd(), err)
		}
		return nil
	})
}

// Ping 为 gnet TCP 传输的空操作:TCP 保活使用应用层 CmdHeartbeat,
// 不存在 WebSocket 那样的传输层 Ping 帧。
func (t *gnetTransport) Ping() error { return nil }
