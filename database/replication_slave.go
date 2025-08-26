// 本文件实现了 Godis 服务器作为从节点（slave）时，与主节点（master）进行数据同步和复制的核心逻辑。
// 主要功能包括：
// 1. 处理 SLAVEOF 命令，将服务器设置为某个主节点的从节点，或取消复制。
// 2. 实现与主节点的连接和握手协议（PING, AUTH, REPLCONF）。
// 3. 实现 PSYNC 命令的逻辑，用于发起全量同步（FULLRESYNC）或部分同步（CONTINUE）。
// 4. 接收并加载主节点发送的 RDB 文件以完成全量同步。
// 5. 在全量同步后，持续接收并执行主节点发送的 AOF 命令流，以保持数据一致。
// 6. 通过定时任务（cron）与主节点保持心跳，并上报复制偏移量。
// 7. 实现断线重连机制。

package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"myredis/aof"
	"myredis/config"
	"myredis/interface/myredis"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/myredis/parser"
	"myredis/protocol"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rdb "github.com/hdt3213/rdb/parser"
)

const (
	masterRole = iota
	slaveRole
)

var configChangedErr = errors.New("slaveStatus config changed")

type slaveStatus struct {
	mutex  sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc

	configVersion int32 // 标识复制配置的版本，在复制过程中，会检查此版本号，如果发现变化，则会中止当前的复制流程。

	masterHost string // 主节点主机地址
	masterPort int    // 主节点端口号

	masterConn net.Conn
	masterChan <-chan *parser.Payload // 从主节点接收数据的通道
	replID     string                 // 标识主从复制会话
	replOffset int64                  // 当前已同步的复制偏移量

	lastRecvTime time.Time
	running      sync.WaitGroup // 等待所有正在运行的复制任务完成
}

func initReplSlaveStatus() *slaveStatus {
	return &slaveStatus{}
}

// 执行SLAVEOF命令，将当前节点设置为指定主节点的从节点
// SLAVEOF host port: 将当前服务器设置为指定主节点的从节点。
// SLAVEOF NO ONE: 取消当前的复制状态，使服务器变回主节点。
func (server *Server) execSlaveOf(c myredis.Connection, args [][]byte) myredis.Reply {
	// SLAVEOF NO ONE，设置为主节点
	if strings.ToLower(string(args[0])) == "no" && strings.ToLower(string(args[1])) == "one" {
		server.slaveOfNone()
		return protocol.MakeOkReply()
	}

	// parse 主机和端口
	host := string(args[0])
	port, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	// 更新状态
	server.slaveStatus.mutex.Lock()
	atomic.StoreInt32(&server.role, slaveRole)
	server.slaveStatus.masterHost = host
	server.slaveStatus.masterPort = port
	// 增加版本号，使旧的复制任务失效
	atomic.AddInt32(&server.slaveStatus.configVersion, 1)
	server.slaveStatus.mutex.Unlock()

	go server.setupMaster()
	return protocol.MakeOkReply()
}

// 取消服务器的从节点状态，使其变回主节点
func (server *Server) slaveOfNone() {
	server.slaveStatus.mutex.Lock()
	defer server.slaveStatus.mutex.Unlock()
	server.slaveStatus.masterHost = ""
	server.slaveStatus.masterPort = 0
	server.slaveStatus.replID = ""
	server.slaveStatus.replOffset = -1
	server.slaveStatus.stopSlaveWithMutex()
	server.role = masterRole
}

// 停止正在进行的与主节点的连接、全量同步或AOF接收。
// 调用前必须持有 slaveStatus 的互斥锁
func (repl *slaveStatus) stopSlaveWithMutex() {
	// 原子更新 configVersion
	atomic.AddInt32(&repl.configVersion, 1)
	// 如果存在取消函数，发送取消信号
	if repl.cancel != nil {
		repl.cancel()
		repl.running.Wait()
	}
	// 恢复状态
	repl.ctx = context.Background()
	repl.cancel = nil
	// 关闭连接
	if repl.masterConn != nil {
		_ = repl.masterConn.Close()
	}
	repl.masterConn = nil
	repl.masterChan = nil
}

func (repl *slaveStatus) close() error {
	repl.mutex.Lock()
	defer repl.mutex.Unlock()
	repl.stopSlaveWithMutex()
	return nil
}

// 与主节点建立连接，进行全量同步，接收AOF增量数据
func (server *Server) setupMaster() {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	var configVersion int32
	// 控制后续复制流程
	ctx, cancel := context.WithCancel(context.Background())

	server.slaveStatus.mutex.Lock()
	server.slaveStatus.ctx = ctx
	server.slaveStatus.cancel = cancel
	configVersion = server.slaveStatus.configVersion
	server.slaveStatus.mutex.Unlock()

	// 与主节点建立连接
	isFullReSync, err := server.connectionWithMaster(configVersion)
	if err != nil {
		logger.Error(err)
		// 设置为主节点
		server.slaveOfNone()
		return
	}
	// 如果需要全量同步
	if isFullReSync {
		err = server.loadMasterRDB(configVersion)
		// 同步失败，变为主节点
		if err != nil {
			logger.Error(err)
			server.slaveOfNone()
			return
		}
	}
	// 部分同步
	err = server.receiveAOF(ctx, configVersion)
	if err != nil {
		logger.Error(err)
		// 这里不调用 slaveOfNone，可能是网络临时中断
		return
	}

}

// 与主节点建立TCP连接并完成握手协议
// 返回值: (是否需要全量同步, 错误信息)
func (server *Server) connectionWithMaster(configVersion int32) (bool, error) {
	// 建立与主节点的连接，返回解析结果的通道
	addr := server.slaveStatus.masterHost + ":" + strconv.Itoa(server.slaveStatus.masterPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		server.slaveOfNone()
		return false, errors.New("connect to master failed: " + err.Error())
	}
	masterChan := parser.ParseStream(conn)

	// 向主节点发送 ping 命令
	pingCmdLine := utils.ToCmdLine("ping")
	pingReq := protocol.MakeMultiBulkReply(pingCmdLine)
	_, err = conn.Write(pingReq.ToBytes())
	if err != nil {
		return false, errors.New("send failed: " + err.Error())
	}
	// 获取命令结果
	pingResp := <-masterChan
	if pingResp.Err != nil {
		return false, errors.New("read response failed: " + pingResp.Err.Error())
	}
	switch reply := pingResp.Data.(type) {
	case *protocol.StandardErrReply:
		// 如果是认证相关的错误，则继续尝试认证，否则认为连接有问题
		if !strings.HasPrefix(reply.Error(), "NOAUTH") &&
			!strings.HasPrefix(reply.Error(), "NOPERM") &&
			!strings.HasPrefix(reply.Error(), "ERR operation not permitted") {
			logger.Error("Error reply to PING from master: " + string(reply.ToBytes()))
			server.slaveOfNone()
			return false, nil
		}
	}
	// 封装向主节点发送命令的请求函数
	sendCmdToMaster := func(conn net.Conn, cmdLine CmdLine, masterChan <-chan *parser.Payload) error {
		req := protocol.MakeMultiBulkReply(cmdLine)
		_, err := conn.Write(req.ToBytes())
		if err != nil {
			server.slaveOfNone()
			return errors.New("send failed " + err.Error())
		}
		resp := <-masterChan
		if resp.Err != nil {
			server.slaveOfNone()
			return errors.New("read response failed: " + resp.Err.Error())
		}
		if !protocol.IsOKReply(resp.Data) {
			server.slaveOfNone()
			return errors.New("unexpected auth response: " + string(resp.Data.ToBytes()))
		}
		return nil
	}

	// 如果配置了主节点密码，需要认证
	if config.Properties.MasterAuth != "" {
		authCmdLine := utils.ToCmdLine("auth", config.Properties.MasterAuth)
		err = sendCmdToMaster(conn, authCmdLine, masterChan)
		if err != nil {
			return false, err
		}
	}

	// 告知主节点自己的监听端口
	var port int
	if config.Properties.SlaveAnnouncePort != 0 {
		port = config.Properties.SlaveAnnouncePort
	} else {
		port = config.Properties.Port
	}
	portCmdLine := utils.ToCmdLine("REPLCONF", "listening-port", strconv.Itoa(port))
	err = sendCmdToMaster(conn, portCmdLine, masterChan)
	if err != nil {
		return false, err
	}

	// 告知主节点自己的 IP 地址
	if config.Properties.SlaveAnnounceIP != "" {
		ipCmdLine := utils.ToCmdLine("REPLCONF", "ip-address", config.Properties.SlaveAnnounceIP)
		err := sendCmdToMaster(conn, ipCmdLine, masterChan)
		if err != nil {
			return false, err
		}
	}

	// 告知主节点自己支持 psync2 协议
	capaCmdLine := utils.ToCmdLine("REPLCONF", "capa", "psync2")
	err = sendCmdToMaster(conn, capaCmdLine, masterChan)
	if err != nil {
		return false, err
	}

	// 更新节点状态
	server.slaveStatus.mutex.Lock()
	defer server.slaveStatus.mutex.Unlock()
	// 检查配置是否发生变化
	if server.slaveStatus.configVersion != configVersion {
		return false, configChangedErr
	}
	server.slaveStatus.masterConn = conn
	server.slaveStatus.masterChan = masterChan
	server.slaveStatus.lastRecvTime = time.Now()

	// 发送 PSYNC 命令，开始同步主节点过程
	return server.psyncHandshake()
}

// 发送 `psync` 命令给主节点，并与主节点同步 repl-id 和 offset
// 调用此函数前应持有 slaveStatus.mutex 锁
func (server *Server) psyncHandshake() (bool, error) {
	replID := "?"
	var replOffset int64 = -1
	// 如果之前存在复制信息，获取 ID 和 Offset
	if server.slaveStatus.replID != "" {
		replID = server.slaveStatus.replID
		replOffset = server.slaveStatus.replOffset
	}
	// "psync <replID> <replOffset>"
	psyncCmdLine := utils.ToCmdLine("psync", replID, strconv.FormatInt(replOffset, 10))
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	// 发送命令
	_, err := server.slaveStatus.masterConn.Write(psyncReq.ToBytes())
	if err != nil {
		return false, errors.New("send failed: " + err.Error())
	}
	return server.parsePsyncHandshake()
}

// 解析 psync 命令的返回
// 返回 是否需要全量同步 / 错误信息
func (server *Server) parsePsyncHandshake() (bool, error) {
	var err error
	// 命令返回
	psyncPayload := <-server.slaveStatus.masterChan
	if psyncPayload.Err != nil {
		return false, errors.New("read psync reponse failed: " + psyncPayload.Err.Error())
	}
	psyncHeader, ok := psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		return false, errors.New("illegal psync payload header not a status reply")
	}
	// 解析响应头
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 && len(headers) != 2 {
		return false, errors.New("illegal psync payload header: " + psyncHeader.Status)
	}
	logger.Info("receive psync header from master")

	var isFullReSync bool
	if headers[0] == "FULLRESYNC" {
		// 主节点要求全量同步
		logger.Info("full re-sync with master")
		server.slaveStatus.replID = headers[1]
		server.slaveStatus.replOffset, err = strconv.ParseInt(headers[2], 10, 64)
		if err != nil {
			return false, errors.New("parse replOffset error: " + err.Error())
		}
		isFullReSync = true
	} else if headers[0] == "CONTINUE" {
		// 部分同步
		logger.Info("continue partial sync")
		server.slaveStatus.replID = headers[1]
		isFullReSync = false
	} else {
		return false, errors.New("get illegal psync resp: " + psyncHeader.Status)
	}

	logger.Info(fmt.Sprintf("repl id: %s, current offset: %d", server.slaveStatus.replID, server.slaveStatus.replOffset))
	return isFullReSync, nil
}

// 创建一个临时的 mydis 服务器实例用于加载 RDB 文件
// upgradeAof 若为 true，会同时生成新的 AOF 文件
func makeRdbLoader(upgradeAof bool) (*Server, string, error) {
	rdbLoader := MakeAuxiliaryServer()
	if !upgradeAof {
		return rdbLoader, "", nil
	}
	// 如果需要新建 AOF 文件
	newAofFile, err := os.CreateTemp("", "*.aof")
	if err != nil {
		return nil, "", fmt.Errorf("create temp rdb failed: %v", err)
	}
	newAofFilename := newAofFile.Name()

	aofHandler, err := NewPersister(rdbLoader, newAofFilename, false, aof.FsyncNo)
	if err != nil {
		return nil, "", err
	}
	rdbLoader.bindPersister(aofHandler)
	return rdbLoader, newAofFilename, nil
}

// 建立连接后，加载主节点发送的RDB文件
func (server *Server) loadMasterRDB(configVersion int32) error {
	// 接收主节点发送的RDB文件
	rdbPayload := <-server.slaveStatus.masterChan
	if rdbPayload.Err != nil {
		return errors.New("read response failed: " + rdbPayload.Err.Error())
	}
	rdbReply, ok := rdbPayload.Data.(*protocol.BulkReply)
	if !ok {
		return errors.New("illegal payload header: " + string(rdbPayload.Data.ToBytes()))
	}

	// 解析 RDB 文件
	logger.Info(fmt.Sprintf("received %d bytes of rdb from master", len(rdbReply.Arg)))
	rdbDec := rdb.NewDecoder(bytes.NewReader(rdbReply.Arg))
	// 创建临时的服务器实例加载 RDB 文件
	rdbLoader, newAofFilename, err := makeRdbLoader(config.Properties.AppendOnly)
	if err != nil {
		return err
	}
	// 加载 RDB 文件
	err = rdbLoader.LoadRDB(rdbDec)
	if err != nil {
		return errors.New("dump rdb failed: " + err.Error())
	}

	server.slaveStatus.mutex.Lock()
	defer server.slaveStatus.mutex.Unlock()
	// 检查是否有新的复制命令，如果有即中断
	if server.slaveStatus.configVersion != configVersion {
		return configChangedErr
	}

	// 将临时数据库的内容替换当前服务器的数据库
	for i, db := range rdbLoader.dbSet {
		newDB := db.Load().(*DB)
		server.loadDB(i, newDB)
	}

	// 如果开启了 AOF，需要用加载 RDB 期间生成的新 AOF 文件替换旧的
	if config.Properties.AppendOnly {
		// 关闭旧的 persister
		server.persister.Close()
		err = os.Rename(newAofFilename, config.Properties.AppendFilename)
		if err != nil {
			return err
		}
		// 利用新的 AOF 文件创建新的 persister（此时已经根据RDB文件恢复数据库）
		persister, err := NewPersister(server, config.Properties.AppendFilename, false, config.Properties.AppendFsync)
		if err != nil {
			return err
		}
		server.bindPersister(persister)
	}
	return nil
}

// 持续接收来自主节点的 AOF 命令流，用于增量同步
func (server *Server) receiveAOF(ctx context.Context, configVersion int32) error {
	// 创建用于执行命令的连接（需要执行命令）
	conn := connection.NewConn(server.slaveStatus.masterConn)
	conn.SetMaster()
	// 后台协程正在运行
	server.slaveStatus.running.Add(1)
	defer server.slaveStatus.running.Done()
	for {
		select {
		case payload, open := <-server.slaveStatus.masterChan:
			if !open {
				return errors.New("master channel unexpected close")
			}
			if payload.Err != nil {
				return payload.Err
			}
			// 尝试解析为命令
			cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
			if !ok {
				return errors.New("unexpected payload: " + string(payload.Data.ToBytes()))
			}

			server.slaveStatus.mutex.Lock()
			// 检查版本号是否修改
			if server.slaveStatus.configVersion != configVersion {
				return configChangedErr
			}

			server.Exec(conn, cmdLine.Args)
			size := len(cmdLine.ToBytes())
			server.slaveStatus.replOffset += int64(size)
			server.slaveStatus.lastRecvTime = time.Now()

			logger.Info(fmt.Sprintf(
				"receive %d bytes from master, current offset %d, %s",
				size, server.slaveStatus.replOffset, strconv.Quote(string(cmdLine.ToBytes())),
			))
			server.slaveStatus.mutex.Unlock()
		case <-ctx.Done():
			_ = conn.Close()
			return nil
		}
	}
}

// 定期检查与主节点的连接情况，若超时尝试重连
func (server *Server) slaveCron() {
	repl := server.slaveStatus
	if repl.masterConn == nil {
		return
	}

	// 检查与主节点连接是否超时
	replTimeout := 60 * time.Second
	if config.Properties.ReplTimeout != 0 {
		replTimeout = time.Duration(config.Properties.ReplTimeout) * time.Second
	}
	minLastRecvTime := time.Now().Add(-replTimeout)
	if repl.lastRecvTime.Before(minLastRecvTime) {
		// 超时，尝试重连
		err := server.reconnectWithMaster()
		if err != nil {
			logger.Error("send failed " + err.Error())
		}
		return
	}
	// 发送 ACK 命令，告知当前的偏移量
	err := repl.sendAck2Master()
	if err != nil {
		logger.Error("send failed " + err.Error())
	}
}

// 与主节点进行重连
func (server *Server) reconnectWithMaster() error {
	logger.Info("reconnectioning with master")
	server.slaveStatus.mutex.Lock()
	defer server.slaveStatus.mutex.Unlock()

	// 停止原有的复制相关活动
	server.slaveStatus.stopSlaveWithMutex()
	go server.setupMaster()
	return nil
}

// 向主节点发送 ACK 命令，发送节点同步进度
func (repl *slaveStatus) sendAck2Master() error {
	psyncCmdLine := utils.ToCmdLine("REPLCONF", "ACK", strconv.FormatInt(repl.replOffset, 10))
	psyncReq := protocol.MakeMultiBulkReply(psyncCmdLine)
	_, err := repl.masterConn.Write(psyncReq.ToBytes())
	return err
}
