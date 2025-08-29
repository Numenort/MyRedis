// 本文件实现了 Godis 服务器作为主节点（master）时，处理主从复制相关的所有逻辑。
// 主要功能包括：
// 1. 管理从节点（slave）的连接和状态。
// 2. 实现 `PSYNC` 命令，处理全量同步（full resynchronization）和部分同步（partial resynchronization）。
// 3. 生成 RDB 快照并发送给从节点
// 4. 维护一个复制积压缓冲区，用于高效的部分同步。
// 5. 将写命令实时传播给所有在线的从节点。
// 6. 定期任务维持与从节点的连接和管理积压缓冲区。
/*
+------------------------------------+
|   从节点发送 PSYNC <replid> <off>    |
+------------------+-----------------+
                   |
                   v
+------------------------------------+
|  主节点入口点: execPSync()         |
| -> 根据 master.bgSaveState 切换    |
+------------------------------------+
   |               |                |
   | (空闲)        | (运行中)       | (已完成)
   v               v                v
+---------------------------------+  +---------------------------------+
| 将从节点加入 'waitSlaves' 等待集合|  | 为同步启动一个新的 Goroutine... |
+---------------------------------+  +---------------------------------+
   |                                 |
   | (仅当'空闲'状态时)              |
   v                                 |
+---------------------------------+  |
| 调用: backgroundSaveForReplication() .. |----+
+---------------------------------+    |
   |                                   |
   v                                   v
+------------------------------------------------------+
| 同步处理结束: execPSync() 函数立即返回 NoReply        |
+------------------------------------------------------+


//============================ 异步 / 后台任务区域 ============================//


+----------------------------------+      +------------------------------------------+
|        后台 BGSAVE 任务          |      |    异步同步 Goroutine (当状态为 bgSaveFinish 时) |
|  由函数触发:                     |      +------------------------------------------+
|    backgroundSaveForReplication()        |      | 1. 调用: masterTryPartialSyncWithSlave() |
|    -> go saveForReplication()    |      |    (检查 replid 和 offset)               |
+----------------+-----------------+      +--------------------+---------------------+
                 |                                             |
                 v                                  (成功) +------------+ (失败)
+----------------------------------+                        |            |
| 1. 生成 RDB 快照                 |                        v            v
|    (状态 -> bgSaveRunning)       |      +---------------------------------+  +-----------------------------+
+----------------+-----------------+      |     [部分重同步成功]            |  |    [回退到全量重同步]       |
                 |                        | -> 发送积压区差异数据           |  |                             |
                 v                        +------------------+--------------+  +---------------+-------------+
+----------------------------------+                         |                            |
| 2. RDB 生成完毕                  |                         |                            |
|    (状态 -> bgSaveFinish)        |                         |                            v
+----------------+-----------------+                         |            +------------------------------------+
                 |                                           |            | 2. 调用: masterFullReSyncWithSlave() |
                 v                                           |            +------------------------------------+
+----------------------------------+                         |                            |
| 3. 遍历 'waitSlaves' 集合并对每个: |                       |                            |
|    调用: masterFullReSyncWithSlave() |---------------------+----------------------------+
+----------------------------------+                         |
                 |                                           |
                 v (在循环内部执行)                          v
  +---------------------------------------------------------------------------------------+
  |                                                                                       |
  |                            [全量重同步逻辑]                                           |
  |                       执行函数: masterFullReSyncWithSlave()                             |
  |  -----------------------------------------------------------------------------------  |
  |  a. 发送: "+FULLRESYNC <replid> <offset>"                                             |
  |  b. 发送: RDB 文件内容 (来自 master.rdbFilename)                                      |
  |  c. 发送: 完整的复制积压区内容                                                        |
  |                                                                                       |
  +-----------------------------------------+---------------------------------------------+
                                            |
                                            v
                      +------------------------------------------+
                      |   公共的最后步骤 (两种同步类型均执行)    |
                      |   调用: setSlaveOnline()                 |
                      +-------------------+----------------------+
                                          |
                                          v
                                 +------------------+
                                 |    (同步完成)    |
                                 +------------------+
*/

package database

import (
	"errors"
	"fmt"
	"io"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/sync/atomic"
	"myredis/lib/utils"
	"myredis/protocol"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	slaveStateHandshake   = uint8(iota) // 从节点正在握手
	slaveStateWaitSaveEnd               // 等待主节点生成 RDB 文件
	slaveStateSendingRDB                // 主节点正在向从节点发送 RDB 文件
	slaveStateOnline                    // 从节点已完成同步，可以接收更新
)

const (
	bgSaveIdle = uint8(iota)
	bgSaveRunning
	bgSaveFinish
)

const (
	slaveCapacityNone = 0
	slaveCapacityEOF  = 1 << iota
	slaveCapacityPsync2
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
func (backLog *replBacklog) isValidOffset(offset int64) bool {
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
	mdb         *Server // 主节点连接
	backlog     *replBacklog
	readyToSend bool // 是否可以将捕获的命令发送给从节点
}

// AOF 持久化模块中，收到命令时调用的回调函数
func (listener *replAofListener) Callback(cmdLines []CmdLine) {
	listener.mdb.masterStatus.mu.Lock()
	// 持久化收到命令时，主节点将命令写入缓冲区，后续复制给从节点
	for _, cmdLine := range cmdLines {
		reply := protocol.MakeMultiBulkReply(cmdLine)
		listener.backlog.appendBytes(reply.ToBytes())
	}
	listener.mdb.masterStatus.mu.Unlock()
	// 全量同步已完成
	if listener.readyToSend {
		if err := listener.mdb.masterSendUpdatesToSlave(); err != nil {
			logger.Errorf("masterSendUpdatesToSlave after receive aof error: %v", err)
		}
	}
}

type masterStatus struct {
	mu           sync.RWMutex
	replID       string       // 复制 ID
	backlog      *replBacklog // 复制积压的缓冲区
	slaveMap     map[myredis.Connection]*slaveClient
	waitSlaves   map[*slaveClient]struct{} // 正在等待 RDB 生成的从节点集合
	onlineSlaves map[*slaveClient]struct{} // 已经完成同步，处于在线状态的从节点集合
	bgSaveState  uint8                     // 后台 RDB 保存的状态
	rdbFilename  string
	aofListener  *replAofListener // 用于监听 AOF 持久化，并将命令写入积压缓冲区
	rewriting    atomic.Boolean   // 是否正在重写 RDB
}

func (server *Server) initMasterStatus() {
	server.masterStatus = &masterStatus{
		mu:           sync.RWMutex{},
		replID:       utils.RandString(40),
		backlog:      &replBacklog{},
		slaveMap:     make(map[myredis.Connection]*slaveClient),
		waitSlaves:   make(map[*slaveClient]struct{}),
		onlineSlaves: make(map[*slaveClient]struct{}),
		bgSaveState:  bgSaveIdle,
		rdbFilename:  "",
	}
}

// 停止主节点功能并清理所有相关资源
func (server *Server) stopMaster() {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()

	// 关闭与从节点的连接
	for _, slave := range server.masterStatus.slaveMap {
		_ = slave.conn.Close()
		delete(server.masterStatus.onlineSlaves, slave)
		delete(server.masterStatus.waitSlaves, slave)
		delete(server.masterStatus.slaveMap, slave.conn)
	}

	// 清理 master 状态
	if server.persister != nil && server.masterStatus.aofListener != nil {
		server.persister.RemoveListener(server.masterStatus.aofListener)
	}
	if server.masterStatus.rdbFilename != "" {
		_ = os.Remove(server.masterStatus.rdbFilename)
	}

	// 重置状态字段
	server.masterStatus.rdbFilename = ""
	server.masterStatus.replID = ""
	server.masterStatus.backlog = &replBacklog{}
	server.masterStatus.slaveMap = make(map[myredis.Connection]*slaveClient)
	server.masterStatus.waitSlaves = make(map[*slaveClient]struct{})
	server.masterStatus.onlineSlaves = make(map[*slaveClient]struct{})
	server.masterStatus.bgSaveState = bgSaveIdle
}

// 启动协程在后台生成 RDB 文件
func (server *Server) backgroundSaveForReplication() {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				logger.Errorf("panic: %v", e)
			}
		}()
		if err := server.saveForReplication(); err != nil {
			logger.Errorf("save for replication error: %v", err)
		}
	}()
}

// 生成 RDB 文件，将其发送给等待中的从节点，执行全量同步
func (server *Server) saveForReplication() error {
	rdbFile, err := os.CreateTemp("", ".rdb")
	if err != nil {
		return fmt.Errorf("create temp rdb failed")
	}
	rdbFilename := rdbFile.Name()

	// 修改主节点状态
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveRunning
	server.masterStatus.rdbFilename = rdbFilename
	aofListener := &replAofListener{
		mdb:     server,
		backlog: server.masterStatus.backlog,
	}
	server.masterStatus.aofListener = aofListener
	server.masterStatus.mu.Unlock()

	// 生成 RDB 文件
	err = server.persister.GenerateRDBForReplication(rdbFilename, aofListener, nil)
	if err != nil {
		return err
	}
	aofListener.readyToSend = true

	// 获取所有等待的从节点
	waitSlaves := make(map[*slaveClient]struct{})
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveFinish
	for salve := range server.masterStatus.waitSlaves {
		waitSlaves[salve] = struct{}{}
	}
	server.masterStatus.waitSlaves = nil
	server.masterStatus.mu.Unlock()

	// 遍历等待列表，执行全量同步
	for slave := range waitSlaves {
		err = server.masterFullReSyncWithSlave(slave)
		if err != nil {
			server.removeSlave(slave)
			logger.Errorf("masterFullReSyncWithSlave error: %v", err)
			continue
		}
	}
	return nil
}

// 对指定的从节点执行全量同步的具体实现
// 主要包含：发送 RDB 文件、发送增量数据
func (server *Server) masterFullReSyncWithSlave(slave *slaveClient) error {
	// 1.首先发送响应头
	header := "+FULLRESYNC " + server.masterStatus.replID + " " +
		strconv.FormatInt(server.masterStatus.backlog.beginOffset, 10) + protocol.CRLF
	_, err := slave.conn.Write([]byte(header))
	if err != nil {
		return fmt.Errorf("write replication header to slave failed: %v", err)
	}

	// 2.发送 RDB 文件
	rdbFile, err := os.Open(server.masterStatus.rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb file %s for replication error: %v", rdbFile.Name(), err)
	}
	defer rdbFile.Close()

	// 3.设置主节点状态
	slave.state = slaveStateSendingRDB
	rdbInfo, _ := os.Stat(server.masterStatus.rdbFilename)
	rdbSize := rdbInfo.Size()

	// 4.发送 rdb 文件 header
	rdbHeader := "$" + strconv.FormatInt(rdbSize, 10) + protocol.CRLF
	_, err = slave.conn.Write([]byte(rdbHeader))
	if err != nil {
		return fmt.Errorf("write rdb header to slave failed: %v", err)
	}

	// 5.发送 rdb 文件体
	_, err = io.Copy(slave.conn, rdbFile)
	if err != nil {
		return fmt.Errorf("write rdb file to slave failed: %v", err)
	}

	// 6.发送缓冲区中的所有数据，用于增量同步
	server.masterStatus.mu.RLock()
	backlog, currentOffset := server.masterStatus.backlog.getSnapshot()
	server.masterStatus.mu.RUnlock()
	// 发送数据
	_, err = slave.conn.Write(backlog)
	if err != nil {
		return fmt.Errorf("full resync write backlog to slave failed: %v", err)
	}

	// 设置从节点状态
	server.setSlaveOnline(slave, currentOffset)
	return nil
}

var errCannotPartialSync = errors.New("cannot do partial sync")

// 尝试对从节点进行部分同步
func (server *Server) masterTryPartialSyncWithSlave(slave *slaveClient, replID string, slaveOffset int64) error {
	server.masterStatus.mu.RLock()
	// replID 不匹配
	if replID != server.masterStatus.replID {
		server.masterStatus.mu.RUnlock()
		return errCannotPartialSync
	}
	// 检查请求的偏移量是否有效
	if !server.masterStatus.backlog.isValidOffset(slaveOffset) {
		server.masterStatus.mu.RUnlock()
		return errCannotPartialSync
	}
	// 获取增量数据
	backlog, currentOffset := server.masterStatus.backlog.getSnapshotAfter(slaveOffset)
	server.masterStatus.mu.RUnlock()

	// 发送响应头
	header := "+CONTINUE " + server.masterStatus.replID + protocol.CRLF
	_, err := slave.conn.Write([]byte(header))
	if err != nil {
		return fmt.Errorf("write replication header to slave failed: %v", err)
	}
	// 发送缓冲区数据
	_, err = slave.conn.Write(backlog)
	if err != nil {
		return fmt.Errorf("partial resync write backlog to slave failed: %v", err)
	}
	server.setSlaveOnline(slave, currentOffset)
	return nil
}

// 处理从节点发送的 PSYNC 命令，实现主从复制的同步机制
// 命令格式：execPsync <replID> <replOffset>
func (server *Server) execPSync(c myredis.Connection, args [][]byte) myredis.Reply {
	replID := string(args[0])
	replOffset, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	// 创建从节点客户端
	slave := server.masterStatus.slaveMap[c]
	if slave == nil {
		slave = &slaveClient{
			conn: c,
		}
		c.SetSlave()
		server.masterStatus.slaveMap[c] = slave
	}

	if server.masterStatus.bgSaveState == bgSaveIdle {
		// 生成新的 RDB 文件
		slave.state = slaveStateWaitSaveEnd
		server.masterStatus.waitSlaves[slave] = struct{}{}
		server.backgroundSaveForReplication()
	} else if server.masterStatus.bgSaveState == bgSaveRunning {
		// 正在生成 RDB 文件
		slave.state = slaveStateWaitSaveEnd
		server.masterStatus.waitSlaves[slave] = struct{}{}
	} else if server.masterStatus.bgSaveState == bgSaveFinish {
		// 生成完毕，开始同步
		go func() {
			defer func() {
				if e := recover(); e != nil {
					logger.Errorf("panic: %v", e)
				}
			}()
			// 首先尝试部分同步
			err = server.masterTryPartialSyncWithSlave(slave, replID, replOffset)
			if err == nil {
				return
			}
			if err != errCannotPartialSync {
				// 发生其他错误
				server.removeSlave(slave)
				logger.Errorf("masterTryPartialSyncWithSlave error: %v", err)
				return
			}
			// 否则进行全量同步
			if err := server.masterFullReSyncWithSlave(slave); err != nil {
				server.removeSlave(slave)
				logger.Errorf("masterFullReSyncWithSlave error: %v", err)
				return
			}
		}()
	}
	return &protocol.NoReply{}
}

// 命令格式：execReplConf [key1] [val1] ... [key n] [val n]
func (server *Server) execReplConf(c myredis.Connection, args [][]byte) myredis.Reply {
	if len(args)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}
	server.masterStatus.mu.Lock()
	slave := server.masterStatus.slaveMap[c]
	server.masterStatus.mu.Unlock()

	for i := 0; i < len(args); i += 2 {
		key := strings.ToLower(string(args[i]))
		val := string(args[i+1])
		switch key {
		case "ack":
			// 处理 ACK 命令
			offset, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			slave.offset = offset
			slave.lastAckTime = time.Now()
			return &protocol.NoReply{}
		case "listening-port":
			port, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			slave.announcePort = int(port)
		case "ip-address":
			ip := string(val)
			slave.announceIP = ip
		case "capa":
			cap := strings.ToLower(string(val))
			if cap == "psync2" {
				slave.capacity = slaveCapacityPsync2
			}
		}
	}
	return protocol.MakeOkReply()
}

var pingBytes = protocol.MakeMultiBulkReply(utils.ToCmdLine("ping")).ToBytes()

// 缓冲区积压任务数量最大值
const maxBacklogSize = 10 * 1024 * 1024 // 10MB

// 主节点周期执行的任务
// 添加 PING 命令维护状态、对从节点发送更新、
func (server *Server) masterCron() {
	server.masterStatus.mu.Lock()
	if len(server.masterStatus.slaveMap) == 0 {
		// 如果没有从节点
		server.masterStatus.mu.Unlock()
		return
	}
	if server.masterStatus.bgSaveState == bgSaveFinish {
		// 后台 RDB 保存完成，添加 PING 命令
		server.masterStatus.backlog.appendBytes(pingBytes)
	}
	backlogSize := len(server.masterStatus.backlog.buf)
	server.masterStatus.mu.Unlock()

	// 对每个从节点发送更新
	if err := server.masterSendUpdatesToSlave(); err != nil {
		logger.Errorf("masterSendUpdatesToSlave error: %v", err)
	}

	// 如果积压的缓冲区过大（即待执行的命令过多）且不处于重写状态
	if backlogSize > maxBacklogSize && !server.masterStatus.rewriting.Get() {
		go func() {
			server.masterStatus.rewriting.Set(true)
			defer server.masterStatus.rewriting.Set(false)
			// 重写
			if err := server.rewriteRDB(); err != nil {
				// 再次检查，防止 err 导致的未执行
				server.masterStatus.rewriting.Set(false)
				logger.Errorf("rewrite error: %v", err)
			}
		}()
	}
}

// 将缓冲区中暂存的更新数据发送给从节点
func (server *Server) masterSendUpdatesToSlave() error {
	// 获取在线状态节点集合
	onlineSlaves := make(map[*slaveClient]struct{})
	server.masterStatus.mu.RLock()
	beginOffset := server.masterStatus.backlog.beginOffset
	// 获取更新数据
	backlog, currentOffset := server.masterStatus.backlog.getSnapshot()
	for slave := range server.masterStatus.onlineSlaves {
		onlineSlaves[slave] = struct{}{}
	}
	server.masterStatus.mu.RUnlock()

	for slave := range onlineSlaves {
		// 需要发送的缓冲区数据起点
		slaveBeginOffset := slave.offset - beginOffset
		// 遍历每个从节点，发送数据
		_, err := slave.conn.Write(backlog[slaveBeginOffset:])
		if err != nil {
			logger.Errorf("send updates backlog to slave failed: %v", err)
			// 移除从节点
			server.removeSlave(slave)
			continue
		}
		// 更新已复制的偏移量
		slave.offset = currentOffset
	}
	return nil
}

// 在积压缓冲区过大时，通过生成新的 RDB 和积压缓冲区来减小其大小
func (server *Server) rewriteRDB() error {
	rbdFile, err := os.CreateTemp("", "*.rdb")
	if err != nil {
		return fmt.Errorf("create temp rdb failed: %v", err)
	}
	rdbFilename := rbdFile.Name()
	newBacklog := &replBacklog{}
	aofListener := &replAofListener{
		backlog: newBacklog,
		mdb:     server,
	}
	// 在 RDB 生成前执行，安全更新缓冲区
	hook := func() {
		server.masterStatus.mu.Lock()
		defer server.masterStatus.mu.Unlock()
		// 新积压缓冲区的起始偏移量是旧缓冲区的结束偏移量
		newBacklog.beginOffset = server.masterStatus.backlog.currentOffset
	}
	// 生成 RDB 文件
	err = server.persister.GenerateRDBForReplication(rdbFilename, aofListener, hook)
	if err != nil {
		return err
	}

	// 使用新的缓冲区和 RDB 文件替代旧的
	server.masterStatus.mu.Lock()
	server.masterStatus.rdbFilename = rdbFilename
	server.masterStatus.backlog = newBacklog
	// 移除原有监听器，设置新的监听器
	server.persister.RemoveListener(server.masterStatus.aofListener)
	server.masterStatus.aofListener = aofListener
	server.masterStatus.mu.Unlock()
	// 这里在 master.mu 解锁后才能设置 readyToSend，防止 aof 服务中，同时对 master.mu 加锁
	aofListener.readyToSend = true
	return nil
}

// 将一个从节点的状态设置为在线
func (server *Server) setSlaveOnline(slave *slaveClient, currentOffset int64) {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	slave.state = slaveStateOnline
	slave.offset = currentOffset
	// // 从等待集合中移除
	// delete(server.masterStatus.waitSlaves, slave)
	server.masterStatus.onlineSlaves[slave] = struct{}{}
}

// 从主节点移除一个从节点，关闭连接，清理资源
func (server *Server) removeSlave(slave *slaveClient) {
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	// 关闭从节点连接
	_ = slave.conn.Close()
	delete(server.masterStatus.slaveMap, slave.conn)
	delete(server.masterStatus.onlineSlaves, slave)
	delete(server.masterStatus.waitSlaves, slave)
	logger.Info("disconnect with slave " + slave.conn.Name())
}
