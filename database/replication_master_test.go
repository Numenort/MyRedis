package database

import (
	"bytes"
	"myredis/aof"
	"myredis/config"
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/myredis/parser"
	"myredis/protocol"
	"myredis/protocol/assert"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rdb "github.com/hdt3213/rdb/parser"
)

func mockServer() *Server {
	server := &Server{}
	server.dbSet = make([]*atomic.Value, 16)
	for i := 0; i < 16; i++ {
		singleDB := makeDB()
		singleDB.index = i
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	server.slaveStatus = initReplSlaveStatus()
	server.initMasterStatus()
	return server
}

// 测试主服务器端的复制功能
// 包括：全量同步、增量同步、部分重连等场景
func TestReplicationMasterSide(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		Databases:      16,
		AppendOnly:     true,
		AppendFilename: aofFilename,
	}
	master := mockServer()
	aofHandler, err := NewPersister(master, config.Properties.AppendFilename, true, config.Properties.AppendFsync)
	if err != nil {
		panic(err)
	}
	master.bindPersister(aofHandler)
	slave := mockServer()
	replConn := connection.NewSimpleConn()

	// set data to master
	masterConn := connection.NewSimpleConn()
	resp := master.Exec(masterConn, utils.ToCmdLine("SET", "a", "a"))
	assert.AssertNotError(t, resp)
	time.Sleep(time.Millisecond * 100) // wait write aof

	/*---- 全量同步测试 ----*/
	master.Exec(replConn, utils.ToCmdLine("psync", "?", "-1"))
	// 解析主服务器返回的数据流
	masterChan := parser.ParseStream(replConn)
	psyncPayload := <-masterChan
	if psyncPayload.Err != nil {
		t.Errorf("master bad protocol: %v", psyncPayload.Err)
		return
	}
	// 读取PSYNC响应头
	psyncHeader, ok := psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		t.Error("psync header is not a status reply")
		return
	}
	// 解析PSYNC响应头信息：FULLRESYNC <repl_id> <offset>
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 {
		t.Errorf("illegal psync header: %s", psyncHeader.Status)
		return
	}

	replId := headers[1]
	replOffset, err := strconv.ParseInt(headers[2], 10, 64)
	if err != nil {
		t.Errorf("illegal offset: %s", headers[2])
		return
	}
	t.Logf("repl id: %s, offset: %d", replId, replOffset)

	// 读取 RDB 文件数据
	rdbPayload := <-masterChan
	if rdbPayload.Err != nil {
		t.Error("read response failed: " + rdbPayload.Err.Error())
		return
	}
	rdbReply, ok := rdbPayload.Data.(*protocol.BulkReply)
	if !ok {
		t.Error("illegal payload header: " + string(rdbPayload.Data.ToBytes()))
		return
	}

	// 从服务器加载 RDB 数据
	rdbDec := rdb.NewDecoder(bytes.NewReader(rdbReply.Arg))
	err = slave.LoadRDB(rdbDec)
	if err != nil {
		t.Error("import rdb failed: " + err.Error())
		return
	}

	// 验证从服务器是否正确加载了数据
	slaveConn := connection.NewSimpleConn()
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "a"))
	assert.AssertBulkReply(t, resp, "a")

	/*---- 增量AOF同步测试 ----*/
	masterConn = connection.NewSimpleConn()
	resp = master.Exec(masterConn, utils.ToCmdLine("SET", "b", "b"))
	time.Sleep(time.Millisecond * 100) // wait write aof
	assert.AssertNotError(t, resp)
	master.masterCron()

	// 从服务器接收并执行增量命令
	for {
		payload := <-masterChan
		if payload.Err != nil {
			t.Error(payload.Err)
			return
		}
		cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			t.Error("unexpected payload: " + string(payload.Data.ToBytes()))
			return
		}
		slave.Exec(replConn, cmdLine.Args)
		n := len(cmdLine.ToBytes())
		slave.slaveStatus.replOffset += int64(n) // 更新复制偏移量
		if string(cmdLine.Args[0]) != "ping" {
			break
		}
	}

	// 验证增量同步是否成功
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "b"))
	assert.AssertBulkReply(t, resp, "b")

	/*---- 部分重连测试 ----*/
	_ = replConn.Close() // 模拟网络断开

	replConn = connection.NewSimpleConn()

	// 从服务器发起部分重连请求，携带上次的复制ID和偏移量
	master.Exec(replConn, utils.ToCmdLine("psync", replId,
		strconv.FormatInt(slave.slaveStatus.replOffset, 10)))
	masterChan = parser.ParseStream(replConn)
	psyncPayload = <-masterChan
	if psyncPayload.Err != nil {
		t.Errorf("master bad protocol: %v", psyncPayload.Err)
		return
	}
	psyncHeader, ok = psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		t.Error("psync header is not a status reply")
		return
	}
	headers = strings.Split(psyncHeader.Status, " ")
	if len(headers) != 2 {
		t.Errorf("illegal psync header: %s", psyncHeader.Status)
		return
	}
	// 验证响应是否为 CONTINUE
	if headers[0] != "CONTINUE" {
		t.Errorf("expect CONTINUE actual %s", headers[0])
		return
	}
	replId = headers[1]
	t.Logf("partial resync repl id: %s, offset: %d", replId, slave.slaveStatus.replOffset)

	// 在主服务器上设置新数据
	resp = master.Exec(masterConn, utils.ToCmdLine("SET", "c", "c"))
	time.Sleep(time.Millisecond * 100) // wait write aof
	assert.AssertNotError(t, resp)
	master.masterCron()
	// 从服务器接收增量数据
	for {
		payload := <-masterChan
		if payload.Err != nil {
			t.Error(payload.Err)
			return
		}
		cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			t.Error("unexpected payload: " + string(payload.Data.ToBytes()))
			return
		}
		slave.Exec(replConn, cmdLine.Args)
		if string(cmdLine.Args[0]) != "ping" {
			break
		}
	}
	// 验证部分重连后的数据同步
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "c"))
	assert.AssertBulkReply(t, resp, "c")
}

// 测试主服务器 RDB 重写后的复制功能
func TestReplicationMasterRewriteRDB(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		Databases:      16,
		AppendOnly:     true,
		AppendFilename: aofFilename,
		AppendFsync:    aof.FsyncAlways,
	}
	master := mockServer()
	aofHandler, err := NewPersister(master, config.Properties.AppendFilename, true, config.Properties.AppendFsync)
	if err != nil {
		panic(err)
	}
	master.bindPersister(aofHandler)

	masterConn := connection.NewSimpleConn()
	resp := master.Exec(masterConn, utils.ToCmdLine("SET", "a", "a"))
	assert.AssertNotError(t, resp)
	resp = master.Exec(masterConn, utils.ToCmdLine("SET", "b", "b"))
	assert.AssertNotError(t, resp)
	time.Sleep(time.Millisecond * 100) // wait write aof

	// 执行RDB重写操作
	err = master.rewriteRDB()
	if err != nil {
		t.Error(err)
		return
	}
	// 重写后继续设置数据
	resp = master.Exec(masterConn, utils.ToCmdLine("SET", "c", "c"))
	assert.AssertNotError(t, resp)
	time.Sleep(time.Millisecond * 100) // wait write aof

	// set slave
	slave := mockServer()
	replConn := connection.NewSimpleConn()
	// 从服务器发起全量同步请求
	master.Exec(replConn, utils.ToCmdLine("psync", "?", "-1"))
	masterChan := parser.ParseStream(replConn)
	psyncPayload := <-masterChan
	if psyncPayload.Err != nil {
		t.Errorf("master bad protocol: %v", psyncPayload.Err)
		return
	}
	psyncHeader, ok := psyncPayload.Data.(*protocol.StatusReply)
	if !ok {
		t.Error("psync header is not a status reply")
		return
	}
	headers := strings.Split(psyncHeader.Status, " ")
	if len(headers) != 3 {
		t.Errorf("illegal psync header: %s", psyncHeader.Status)
		return
	}

	replId := headers[1]
	replOffset, err := strconv.ParseInt(headers[2], 10, 64)
	if err != nil {
		t.Errorf("illegal offset: %s", headers[2])
		return
	}
	t.Logf("repl id: %s, offset: %d", replId, replOffset)

	// 读取 RDB 快照数据
	rdbPayload := <-masterChan
	if rdbPayload.Err != nil {
		t.Error("read response failed: " + rdbPayload.Err.Error())
		return
	}
	rdbReply, ok := rdbPayload.Data.(*protocol.BulkReply)
	if !ok {
		t.Error("illegal payload header: " + string(rdbPayload.Data.ToBytes()))
		return
	}

	rdbDec := rdb.NewDecoder(bytes.NewReader(rdbReply.Arg))
	err = slave.LoadRDB(rdbDec)
	if err != nil {
		t.Error("import rdb failed: " + err.Error())
		return
	}
	// 验证从服务器是否正确加载了重写前的所有数据
	slaveConn := connection.NewSimpleConn()
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "a"))
	assert.AssertBulkReply(t, resp, "a")
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "b"))
	assert.AssertBulkReply(t, resp, "b")
	// 同步重写后的数据
	master.masterCron()
	// 从服务器接收并执行重写后的增量命令
	for {
		payload := <-masterChan
		if payload.Err != nil {
			t.Error(payload.Err)
			return
		}
		cmdLine, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			t.Error("unexpected payload: " + string(payload.Data.ToBytes()))
			return
		}
		slave.Exec(replConn, cmdLine.Args)
		n := len(cmdLine.ToBytes())
		slave.slaveStatus.replOffset += int64(n)
		if string(cmdLine.Args[0]) != "ping" {
			break
		}
	}
	// 验证重写后的增量数据是否正确同步
	resp = slave.Exec(slaveConn, utils.ToCmdLine("get", "c"))
	assert.AssertBulkReply(t, resp, "c")
}
