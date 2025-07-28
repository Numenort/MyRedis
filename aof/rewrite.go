package aof

import (
	"io"
	"myredis/config"
	"myredis/lib/logger"
	"myredis/lib/utils"
	"myredis/protocol"
	"os"
	"strconv"
)

/* 重写所需实例，包含 AOF 文件名称以及临时数据库 */
func (persister *Persister) newRewriteHandler() *Persister {
	h := &Persister{}
	h.aofFilename = persister.aofFilename
	h.db = persister.tmpDBMaker()
	return h
}

/*
重写操作：

	用一个包含了当前内存数据状态的、更紧凑的新AOF文件，
	去替换掉体积庞大、含有冗余命令的旧AOF文件
*/
func (persister *Persister) Rewrite() error {
	ctx, err := persister.PrepareRewrite()
	if err != nil {
		return err
	}
	err = persister.DoRewrite(ctx)
	if err != nil {
		return err
	}

	persister.FinishRewrite(ctx)
	return nil
}

// 重写操作所需要的上下文
type RewriteContext struct {
	tempFile *os.File // 存储精简命令的 AOF 文件
	fileSize int64
	dbIndex  int // 开始重写时，当前数据库索引
}

// 为重写操作准备上下文
func (persister *Persister) PrepareRewrite() (*RewriteContext, error) {
	// 暂停 AOF 文件的写入
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 确保操作系统缓冲区里所有待写入数据写入磁盘
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// 保存旧 AOF 文件的最后写入位置
	fileInfo, _ := os.Stat(persister.aofFilename)
	fileSize := fileInfo.Size()

	// 创建临时 AOF 文件
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("temp file create failed")
		return nil, err
	}

	return &RewriteContext{
		tempFile: file,
		fileSize: fileSize,
		dbIndex:  persister.currentDB,
	}, nil
}

// 执行重写操作
func (persister *Persister) DoRewrite(ctx *RewriteContext) (err error) {
	// 旧 AOF 文件重写时，将精简的命令保存到临时文件
	if !config.Properties.AofUseRdbPreamble {
		// 使用 AOF
		logger.Info("generate aof preamble")
		err = persister.generateAof(ctx)
	} else {
		// 使用 RDB
		logger.Info("generate rdb preamble")
		err = persister.generateRDB(ctx)
	}
	return err
}

// 将重写期间产生的新命令追加到新文件中，并以原子操作安全地替换掉旧文件
func (persister *Persister) FinishRewrite(ctx *RewriteContext) {
	// 暂停 AOF 文件的写入
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	tempFile := ctx.tempFile

	errOccurs := func() bool {
		// 读取重写期间执行的写命令（写入到旧的 AOF 文件中）
		src, err := os.Open(persister.aofFilename)
		if err != nil {
			logger.Error("open aofFilename failed: " + err.Error())
			return true
		}
		defer func() {
			_ = src.Close()
			_ = tempFile.Close()
		}()
		// 定位到快照点，这之后的数据需要更新
		_, err = src.Seek(ctx.fileSize, 0)
		if err != nil {
			logger.Error("seek failed: " + err.Error())
			return true
		}

		// 将增量数据复制到临时文件之前，确保增量命令的数据库上下文正确
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIndex))).ToBytes()
		_, err = tempFile.Write(data)
		if err != nil {
			logger.Error("tmp file rewrite failed: " + err.Error())
			return true
		}

		// 复制增量数据（即重写期间的新命令）
		_, err = io.Copy(tempFile, src)
		if err != nil {
			logger.Error("copy aof filed failed: " + err.Error())
			return true
		}
		return false
	}()

	// 如果在复制命令过程中发生错误，则直接返回，不进行文件替换
	if errOccurs {
		return
	}

	_ = persister.aofFile.Close()
	// 原子性替换
	if err := os.Rename(tempFile.Name(), persister.aofFilename); err != nil {
		logger.Warn(err)
	}

	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err) // 如果重新打开文件失败，程序无法继续运行，直接panic
	}

	persister.aofFile = aofFile
	// 确保新激活的 AOF 文件的数据库上下文正确
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic(err) // 如果写入失败，程序无法正常工作，直接panic
	}
}
