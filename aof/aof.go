package aof

import (
	"context"
	"io"
	"myredis/interface/database"
	"myredis/lib/logger"
	"myredis/parser"
	"os"
	"sync"

	rdb "github.com/hdt3213/rdb/core"
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
	/* 上下文控制 */
	ctx    context.Context
	cancel context.CancelFunc
	/* 数据库实例，用于执行命令 */
	db database.DBEngine
	/* 用于 AOF 重写过程中创建临时数据库实例 */
	tmpDBMaker func() database.DBEngine
	/* 接收写入 AOF 的命令 */
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	/* fsync 策略 */
	aofFsync   string
	aofFinshed chan struct{}
	/* 用于暂停/恢复 AOF 写入 */
	pausingAof sync.Mutex
	currentDB  int

	// 监听器集合，用于写入 AOF 后通知其他组件
	listeners map[Listener]struct{}
	buffer    []CmdLine
}

func NewPersister(db database.DBEngine, filename string, load bool, fsync string, tmpDBMaker func() database.DBEngine) (*Persister, error) {
	Persister := &Persister{
		aofFilename: filename,
		aofFsync:    fsync,
		db:          db,
		tmpDBMaker:  tmpDBMaker,
		currentDB:   0,
	}
	// 如果加载 aof file
	return Persister, nil
}

// // maxBytes: 最大可读字节
// func (persister *Persister) LoadAof(maxBytes int) {
// 	// 加载 AOF 文件时关闭该通道防止冲突
// 	aofChan := persister.aofChan
// 	persister.aofChan = nil
// 	defer func(aofChan chan *payload) {
// 		persister.aofChan = aofChan
// 	}(aofChan)

// 	file, err := os.Open(persister.aofFilename)
// 	// 如果是路径问题
// 	if err != nil {
// 		if _, ok := err.(*os.PathError); ok {
// 			return
// 		}
// 		logger.Warn(err)
// 		return
// 	}
// 	defer file.Close()

// 	// 尝试加载 rdb 快照
// 	decoder := rdb.NewDecoder(file)
// 	err = persister.db.LoadRDB(decoder)
// 	if err != nil {
// 		// 没有 rdb 快照，从 0 开始
// 		file.Seek(0, io.SeekStart)
// 	} else {
// 		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
// 		maxBytes = maxBytes - decoder.GetReadCount()
// 	}
// 	var reader io.Reader
// 	if maxBytes > 0 {
// 		reader = io.LimitReader(reader, int64(maxBytes))
// 	} else {
// 		reader = file
// 	}

// 	ch := parser.ParseStream(reader)

// }

func (persister *Persister) generateAof(ctx *RewriteContext) error {
	return nil
}
