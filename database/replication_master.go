// 本文件实现了 Godis 服务器作为主节点（master）时，处理主从复制相关的所有逻辑。
// 主要功能包括：
// 1. 管理从节点（slave）的连接和状态。
// 2. 实现 `PSYNC` 命令，处理全量同步（full resynchronization）和部分同步（partial resynchronization）。
// 3. 生成 RDB 快照并发送给从节点
// 4. 维护一个复制积压缓冲区，用于高效的部分同步。
// 5. 将写命令实时传播给所有在线的从节点。
// 6. 定期任务维持与从节点的连接和管理积压缓冲区。

package database

import (
	"myredis/aof"
	"myredis/interface/myredis"
	"sync"
	"time"
)

/* 用于缓存主节点执行的写命令，以便在从节点断线重连时能够进行部分重同步而不是完整重同步 */
type replBacklog struct {
	buf           []byte // 存储命令字节的缓冲区
	beginOffset   int64  // 缓冲区的起始偏移量，标记缓冲区中最早数据的位置
	currentOffset int64  // 当前偏移量，标记缓冲区中最新数据的位置
}

// 向缓冲区追加新的字节数据
func (backLog *replBacklog) appendBytes(bin []byte) {
	backLog.buf = append(backLog.buf, bin...)
	backLog.currentOffset += int64(len(bin))
}

// 返回缓冲区的完整数据快照
func (backLog *replBacklog) getSnapshot() ([]byte, int64) {
	return backLog.buf[:], backLog.currentOffset
}

// 获取指定偏移量之后的缓冲区数据
func (backLog *replBacklog) getSnapshotAfter(beginOffset int64) ([]byte, int64) {
	begin := beginOffset - backLog.beginOffset
	return backLog.buf[begin:], backLog.currentOffset
}

// 检查给定偏移量是否在有效范围内
func (backLog *replBacklog) isVaildOffset(offset int64) bool {
	return offset >= backLog.beginOffset && offset < backLog.currentOffset
}

/* 从节点的全部状态信息 */
type slaveClient struct {
	conn         myredis.Connection
	state        uint8 // 从节点的当前复制状态
	offset       int64 // 从节点已确认的复制偏移量
	lastAckTime  time.Time
	announceIP   string
	announcePort int
	capacity     uint8 // 从节点支持的功能
}

type replAofListener struct {
	mdb         *Server
	backlog     *replBacklog
	readyToSend bool
}

// AOF 持久化模块中，收到命令时调用的回调函数
func (listener *replAofListener) Callback(cmdLines []CmdLine) {

}

type masterStatus struct {
	mu           sync.RWMutex
	replID       string
	backlog      *replBacklog // 复制积压的缓冲区
	slaveMap     map[myredis.Connection]*slaveClient
	waitSlaves   map[*slaveClient]struct{} // 正在等待 RDB 生成的从节点集合
	onlineSlaves map[*slaveClient]struct{} // 已经完成同步，处于在线状态的从节点集合
	rdbFilename  string
}
