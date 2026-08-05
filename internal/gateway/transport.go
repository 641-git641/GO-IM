package gateway

// Transport 抽象底层连接(WebSocket 或原始 TCP)。
// 它提供 Client 写入和关闭所需的最小接口。
type Transport interface {
	// Close 关闭连接。
	Close() error
	// Write 在连接上发送原始字节。
	Write(p []byte) error
	// Ping 发送传输层保活信号(WebSocket 为 Ping 帧,gnet TCP 为空操作)。
	Ping() error
}
