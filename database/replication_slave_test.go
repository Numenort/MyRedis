package database

import (
	"bytes"
	"myredis/aof"
	"myredis/config"
	"myredis/lib/utils"
	"myredis/myredis/client"
	"myredis/myredis/connection"
	"myredis/protocol"
	"myredis/protocol/assert"
	"os"
	"path"
	"testing"
	"time"
)

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

	// sync with master
	ret := masterCli.Send(utils.ToCmdLine("set", "1", "1"))
	assert.AssertStatusReply(t, ret, "OK")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", "6379"))
	assert.AssertStatusReply(t, ret, "OK")
	success := false
	for i := 0; i < 30; i++ {
		// wait for sync
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

	// receive aof
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "2"))
	assert.AssertStatusReply(t, ret, "OK")
	success = false
	for i := 0; i < 10; i++ {
		// wait for sync
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
	err = server.slaveStatus.sendAck2Master()
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(3 * time.Second)

	// test reconnect
	config.Properties.ReplTimeout = 1
	_ = server.slaveStatus.masterConn.Close()
	server.slaveStatus.lastRecvTime = time.Now().Add(-time.Hour) // mock timeout
	server.slaveCron()
	time.Sleep(3 * time.Second)
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

	// test slave of no one
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "NO", "ONE"))
	assert.AssertStatusReply(t, ret, "OK")
	ret = masterCli.Send(utils.ToCmdLine("set", "1", "4"))
	assert.AssertStatusReply(t, ret, "OK")
	ret = server.Exec(conn, utils.ToCmdLine("GET", "1"))
	assert.AssertBulkReply(t, ret, "3")
	ret = server.Exec(conn, utils.ToCmdLine("SLAVEOF", "127.0.0.1", "6379"))
	assert.AssertStatusReply(t, ret, "OK")
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

	// check slave aof file
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
