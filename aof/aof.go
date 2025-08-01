package aof

import (
	"context"
	"io"
	"myredis/config"
	"myredis/interface/database"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/myredis/connection"
	"myredis/myredis/parser"
	"myredis/protocol"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	rdb "github.com/hdt3213/rdb/core"
)

const (
	// 为每个命令执行 Fsync
	FsyncAlways = "always"
	// 每秒执行一次 Fsync
	FsyncEverySec = "everysrc"
	FsyncNo       = "no"
)

type CmdLine = [][]byte

// 待写入 AOF 文件的命令以及所属数据库索引
type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

// 收到 AOF 的 payload 之后通知其他组件
type Listener interface {
	Callback([]CmdLine)
}

type Persister struct {
	/* 上下文控制，用于扫描任务的控制 */
	ctx    context.Context
	cancel context.CancelFunc
	/* 数据库实例，用于执行命令 */
	db database.DBEngine
	/* AOF 重写过程中创建临时数据库实例，通过加载 AOF 文件实现 */
	tmpDBMaker func() database.DBEngine
	/* 接收写入 AOF 的命令 */
	aofChan chan *payload
	/* 管理 AOF 文件 */
	aofFile     *os.File
	aofFilename string
	/* fsync 策略 */
	aofFsyncStrategy string
	aofFinshed       chan struct{}
	/* 用于暂停/恢复 AOF 写入，保护访问共享资源 */
	pausingAof sync.Mutex
	/* 记录当前的数据库编号，避免写入不必要的 SELECT 命令，减少数据库切换开销 */
	currentDB int

	// 监听器集合，用于写入 AOF 后通知其他组件
	listeners map[Listener]struct{}
	buffer    []CmdLine
}

func NewPersister(db database.DBEngine, filename string, load bool, fsyncStrategy string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	persister := &Persister{
		aofFilename:      filename,
		aofFsyncStrategy: fsyncStrategy,
		db:               db,
		tmpDBMaker:       tmpDBMaker,
		currentDB:        0,
	}

	// 如果加载 aof file
	if load {
		persister.LoadAof(0)
	}
	// 打开或创建一个 AOF 日志文件，支持读写，每次写入自动追加到末尾，并确保文件权限安全
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	persister.aofChan = make(chan *payload)
	persister.aofFinshed = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel

	if persister.aofFsyncStrategy == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// 将命令行保存到指定数据库中，通过 AOF 通道
func (persister *Persister) SaveCmdLine(dbIndex int, cmdline CmdLine) {
	if persister.aofChan == nil {
		return
	}
	// 每执行一个命令，保存一次
	if persister.aofFsyncStrategy == FsyncAlways {
		payload := &payload{
			cmdLine: cmdline,
			dbIndex: dbIndex,
		}
		persister.WriteAof(payload)
		return
	}
	// 利用 aofchan 异步发送数据
	persister.aofChan <- &payload{
		cmdLine: cmdline,
		dbIndex: dbIndex,
	}
}

// 监听通道，异步保存 AOF 文件
func (persister *Persister) listenCmdLine() {
	for payload := range persister.aofChan {
		persister.WriteAof(payload)
	}
	persister.aofFinshed <- struct{}{}
}

func (persister *Persister) WriteAof(payload *payload) {
	// 设置为空切片
	persister.buffer = persister.buffer[:0]
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	// 判断是否需要写入数据库切换
	if payload.dbIndex != persister.currentDB {
		// 修改当前数据库
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(payload.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		// 写入 AOF 文件
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return
		}
		persister.currentDB = payload.dbIndex
	}

	data := protocol.MakeMultiBulkReply(payload.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, payload.cmdLine)
	// 将格式化的命令行写入 AOF 文件
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}

	// 通知其他组件对应的命令行
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	// 启用额外的同步操作
	if persister.aofFsyncStrategy == FsyncAlways {
		_ = persister.aofFile.Sync()
	}
}

// 加载 AOF 文件，重建数据库
func (persister *Persister) LoadAof(maxBytes int) {
	// 确保在加载 AOF 文件时的 aofChan 不会发送新的数据
	aofChan := persister.aofChan
	persister = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	// 读取 AOF 文件
	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// 解码可能的 RDB 二进制文件前缀
	decoder := rdb.NewDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// 没有 RDB 文件前缀
		file.Seek(0, io.SeekStart)
	} else {
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount()
	}

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	// 从 AOF 文件流解析 redis 协议，需要为 MultiBulkReply
	channel := parser.ParseStream(reader)
	// 用于重建数据库的临时连接
	simpleConn := connection.NewSimpleConn()
	// 监听 redis 协议
	for ch := range channel {
		if ch.Err != nil {
			if ch.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + ch.Err.Error())
			continue
		}
		if ch.Data == nil {
			logger.Error("empty payload")
			continue
		}
		// 得到对应的命令
		reply, ok := ch.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		// 执行对应命令，重建数据库
		res := persister.db.Exec(simpleConn, reply.Args)
		if protocol.IsErrorReply(res) {
			logger.Error("exec err", string(res.ToBytes()))
		}
		// 确保当前数据库索引正确
		if strings.ToLower(string(reply.Args[0])) == "select" {
			// execSelect success, here must be no error
			dbIndex, err := strconv.Atoi(string(reply.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
			}
		}

	}

}

// 停止 aof channel，保存文件
func (persister *Persister) Fsync() {
	persister.pausingAof.Lock()
	if err := persister.aofFile.Sync(); err != nil {
		logger.Errorf("fysnc failed: %v", err)
	}
	persister.pausingAof.Unlock()
}

func (persister *Persister) fsyncEverySecond() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.Fsync()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

func (persister *Persister) Close() {
	if persister == nil {
		return
	}
	if persister.aofFile != nil {
		close(persister.aofChan)
		// 等待 aofFinshed 信号
		<-persister.aofFinshed
		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
	// 关闭 fsyncEverySecond 的扫描
	persister.cancel()
}

// 用于重写过程中，将临时文件写入 AOF
func (persister *Persister) generateAof(ctx *RewriteContext) error {
	// 获取临时文件、临时保存实例
	tempFile := ctx.tempFile
	tempAofHandler := persister.newRewriteHandler()
	// 加载临时文件
	tempAofHandler.LoadAof(int(ctx.fileSize))
	for i := 0; i < config.Properties.Databases; i++ {
		// Select database
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(i))).ToBytes()
		_, err := tempFile.Write(data)
		if err != nil {
			return err
		}
		// 遍历每个数据库
		tempAofHandler.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			cmd := EntityToCmd(key, entity)
			// 写入维护数据库内容的最简命令
			if cmd != nil {
				_, _ = tempFile.Write(cmd.ToBytes())
			}
			if expiration != nil {
				cmd := MakeExpiredCmd(key, *expiration)
				if cmd != nil {
					_, _ = tempFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}
