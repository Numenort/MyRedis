package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type LogLevel int

type ILogger interface {
	OUTPUT(level LogLevel, callerDepth int, msg string)
}

const (
	flags              = log.LstdFlags
	defaultCallerDepth = 2
	bufferSize         = 1e5
)

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

var levelFlags = []string{"DEBUG", "INFO", "WARNING", "ERROR", "FATAL"}

// 日志:(信息 + 级别)
type logEntry struct {
	msg   string
	level LogLevel
}

type Logger struct {
	logger    *log.Logger
	logFile   *os.File
	entryChan chan *logEntry
	entryPool *sync.Pool // sync.pool 管理对象的创建
}

var DefaultLogger ILogger = NewStdoutLogger()

// 定向到标准控制台输出
func NewStdoutLogger() *Logger {
	logger := &Logger{
		logFile:   nil,
		logger:    log.New(os.Stdout, "", flags),
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() interface{} {
				return &logEntry{}
			},
		},
	}
	// 复用资源，使用 range 监听chan
	go func() {
		for e := range logger.entryChan {
			// 不断读取日志条目
			_ = logger.logger.Output(0, e.msg)
			logger.entryPool.Put(e)
		}
	}()
	return logger
}

// 用于文件形式存储的日志，包含路径/名称/时间/扩展名
type Settings struct {
	Path       string
	Name       string
	Ext        string
	TimeFormat string
}

// 文件存储日志
func NewFileLogger(settings *Settings) (*Logger, error) {
	fileName := fmt.Sprintf("%s-%s-%s",
		settings.Name,
		time.Now().Format(settings.TimeFormat),
		settings.Ext,
	)

	// 打开本地文件
	logFile, err := mustOpen(fileName, settings.Path)
	if err != nil {
		return nil, fmt.Errorf("logging.Join err:%s", err)
	}

	// 写入控制台以及本地文件
	mv := io.MultiWriter(os.Stdout, logFile)
	logger := &Logger{
		logger:    log.New(mv, "", flags),
		logFile:   logFile,
		entryChan: make(chan *logEntry, bufferSize),
		entryPool: &sync.Pool{
			New: func() interface{} {
				return &logEntry{}
			},
		},
	}
	// 异步监听chan，有内容就打印并存储（通过 MultiWriter）
	go func() {
		for e := range logger.entryChan {
			logFilename := fmt.Sprintf(
				"%s-%s.%s",
				settings.Name,
				time.Now().Format(settings.TimeFormat),
				settings.Ext,
			)
			// 按时间自动滚动日志
			if path.Join(settings.Path, logFilename) != logger.logFile.Name() {
				logFile, err := mustOpen(logFilename, settings.Path)
				if err != nil {
					panic("open log " + logFilename + " failed: " + err.Error())
				}
				logger.logFile = logFile
				logger.logger = log.New(io.MultiWriter(os.Stdout, logFile), "", flags)
			}
			_ = logger.logger.Output(0, e.msg)
			logger.entryPool.Put(e)
		}
	}()
	return logger, nil
}

func Setup(setting *Settings) {
	logger, err := NewFileLogger(setting)
	if err != nil {
		panic(err)
	}
	DefaultLogger = logger
}

func (logger *Logger) OUTPUT(level LogLevel, callerDepth int, msg string) {
	var formattedMsg string

	// 获取调用栈信息，用于在日志中显示 哪一行代码打印了这条日志
	_, file, line, ok := runtime.Caller(callerDepth)
	if ok {
		formattedMsg = fmt.Sprintf("[%s][%s:%d] %s", levelFlags[level], filepath.Base(file), line, msg)
	} else {
		formattedMsg = fmt.Sprintf("[%s] %s", levelFlags[level], msg)
	}

	entry := logger.entryPool.Get().(*logEntry)
	entry.msg = formattedMsg
	entry.level = level
	logger.entryChan <- entry
}

func Debug(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.OUTPUT(DEBUG, defaultCallerDepth, msg)
}

func Debugf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.OUTPUT(DEBUG, defaultCallerDepth, msg)
}

func Info(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.OUTPUT(INFO, defaultCallerDepth, msg)
}

func Infof(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.OUTPUT(INFO, defaultCallerDepth, msg)
}

func Warn(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.OUTPUT(WARNING, defaultCallerDepth, msg)
}

func Error(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.OUTPUT(ERROR, defaultCallerDepth, msg)
}

func Errorf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	DefaultLogger.OUTPUT(ERROR, defaultCallerDepth, msg)
}

func Fatal(v ...interface{}) {
	msg := fmt.Sprintln(v...)
	DefaultLogger.OUTPUT(FATAL, defaultCallerDepth, msg)
}
