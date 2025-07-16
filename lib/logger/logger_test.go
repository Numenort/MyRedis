package logger

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLoggerBasic 测试基本的日志输出功能
func TestLoggerBasic(t *testing.T) {
	// 创建日志目录
	logDir := "./test_logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("Failed to create log directory: %v", err)
	}

	// 设置日志配置
	settings := &Settings{
		Path:       logDir,
		Name:       "test",
		Ext:        "log",
		TimeFormat: "2006-01-02",
	}

	// 初始化日志器
	Setup(settings)

	var a string = "man!"

	// 输出各种级别日志
	Debug("This is a debug message")
	Info("This is an info message")
	Warn("This is a warning message")
	Errorf(a, "This is an error message")

	// FATAL 日志会导致程序退出，不建议在单元测试中使用
	Fatal("This is a fatal message")

	t.Log("Logs should be written to test_logs/test-*.log")
	time.Sleep(100 * time.Millisecond) // 等待异步写入完成
}

// TestLoggerLogContent 测试日志内容是否包含期望字段
func TestLoggerLogContent(t *testing.T) {
	logDir := "./test_logs"
	files, _ := filepath.Glob(filepath.Join(logDir, "test-*.log"))
	if len(files) == 0 {
		t.Fatal("No log file found")
	}

	content, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	expectedLevels := []string{"DEBUG", "INFO", "WARN", "ERROR"}
	for _, level := range expectedLevels {
		if !bytes.Contains(content, []byte(level)) {
			t.Errorf("Log content missing level: %s", level)
		}
	}

	fmt.Printf("Log content:\n%s\n", content)
}
