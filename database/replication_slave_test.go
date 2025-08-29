package database

import (
	"bytes"
	"context"
	"myredis/aof"
	"myredis/config"
	"myredis/lib/utils"
	"myredis/myredis/client"
	"myredis/myredis/connection"
	"myredis/myredis/parser"
	"myredis/protocol"
	"myredis/protocol/assert"
	"os"
	"path"
	"testing"
	"time"
)

// 测试与主服务器同步、接收AOF更新、重连机制、停止复制等功能
func TestReplicationSlaveSide(t *testing.T) {
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
	conn := connection.NewSimpleConn()
	server := mockServer()
	masterCli, err := client.NewClient("127.0.0.1:6379")
	if err != nil {
		t.Error(err)
		return
	}
	aofHandler, err := NewPersister(server, config.Properties.AppendFilename, true, aof.FsyncNo)
	if err != nil {
		t.Error(err)
		return
	}
	server.bindPersister(aofHandler)
	server.Exec(conn, utils.ToCmdLine("set", "zz", "zz"))
	masterCli.Start()

	// 与主服务器建立复制关系并同步初始数据
	ret := masterCli.Send(utils.ToCmdLine("set", "1", "1"))
	assert.AssertStatusReply(t, ret, "OK")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", "6379"))
	assert.AssertStatusReply(t, ret, "OK")
	success := false
	for i := 0; i < 30; i++ {
		// 等待同步完成
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("1")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// 测试接收主服务器的 AOF 更新
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "2"))
	assert.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 10; i++ {
		// 等待 AOF 更新同步完成
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("2")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}
	// 发送ACK确认给主服务器
	err = server.slaveStatus.sendAck2Master()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(3 * time.Second)

	// 测试重连机制
	config.Properties.ReplTimeout = 1
	_ = server.slaveStatus.masterConn.Close()
	// 模拟连接超时
	server.slaveStatus.lastRecvTime = time.Now().Add(-time.Hour)
	server.slaveCron()
	time.Sleep(3 * time.Second)

	// 验证重连后能否继续同步数据
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "3"))
	assert.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 10; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("3")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// 测试停止复制功能
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "NO", "ONE"))
	assert.AssertStatusReply(t, ret, "OK")
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "4"))
	assert.AssertStatusReply(t, ret, "OK")
	ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
	assert.AssertBulkReply(t, ret, "3")

	// 重新建立复制关系
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", "6379"))
	assert.AssertStatusReply(t, ret, "OK")

	// 等待重新同步完成
	success = false
	for i := 0; i < 30; i++ {
		// wait for sync
		time.Sleep(time.Second)
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("4")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// 验证AOF文件内容的正确性
	aofCheckServer := MakeAuxiliaryServer()
	aofHandler2, err := NewPersister(aofCheckServer, config.Properties.AppendFilename, true, aof.FsyncNo)
	if err != nil {
		t.Error("create persister failed")
	}
	aofCheckServer.bindPersister(aofHandler2)
	ret = aofCheckServer.Exec(conn, utils.ToCmdLine("get", "zz"))
	assert.AssertNullBulk(t, ret)
	ret = aofCheckServer.Exec(conn, utils.ToCmdLine("get", "1"))
	assert.AssertBulkReply(t, ret, "4")

	err = server.slaveStatus.close()
	if err != nil {
		t.Error("cannot close")
	}
}

// 测试故障转移场景：模拟从服务器接管主服务器的角色
func TestReplicationFailover(t *testing.T) {
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
	conn := connection.NewSimpleConn()
	server := mockServer()
	aofHandler, err := NewPersister(server, aofFilename, true, aof.FsyncAlways)
	if err != nil {
		t.Error(err)
		return
	}
	server.bindPersister(aofHandler)

	masterCli, err := client.NewClient("127.0.0.1:6379")
	if err != nil {
		t.Error(err)
		return
	}
	masterCli.Start()

	// 从服务器从主服务器同步数据
	ret := masterCli.Send(utils.ToCmdLine("set", "1", "1"))
	assert.AssertStatusReply(t, ret, "OK")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", "6379"))
	assert.AssertStatusReply(t, ret, "OK")
	success := false
	for i := 0; i < 30; i++ {
		// 等待同步完成
		time.Sleep(time.Second)
		// 测试同步是否完成
		ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
		bulkRet, ok := ret.(*protocol.BulkReply)
		if ok {
			if bytes.Equal(bulkRet.Arg, []byte("1")) {
				success = true
				break
			}
		}
	}
	if !success {
		t.Error("sync failed")
		return
	}

	// 停止复制，使当前服务器成为独立服务器
	t.Log("slave of no one")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "no", "one"))
	assert.AssertStatusReply(t, ret, "OK")
	// 在当前服务器上设置新数据
	server.Exec(conn, utils.ToCmdLine("set", "2", "2"))

	// 创建复制连接，模拟其他从服务器连接到它
	replConn := connection.NewSimpleConn()
	// 执行PSYNC命令，模拟从服务器发起部分重同步请求
	server.Exec(replConn, utils.ToCmdLine("psync", "?", "-1"))
	masterChan := parser.ParseStream(replConn)
	// 创建新的服务器实例 B，模拟从服务器
	serverB := mockServer()
	serverB.slaveStatus.masterChan = masterChan
	serverB.slaveStatus.configVersion = 0
	serverB.parsePsyncHandshake()
	serverB.loadMasterRDB(0)
	server.masterCron()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go serverB.receiveAOF(ctx, 0)

	time.Sleep(3 * time.Second)
	ret = serverB.Exec(conn, utils.ToCmdLine("get", "1"))
	assert.AssertBulkReply(t, ret, "1")
	ret = serverB.Exec(conn, utils.ToCmdLine("get", "2"))
	assert.AssertBulkReply(t, ret, "2")
}
