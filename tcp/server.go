package tcp

import (
	"context"
	"fmt"

	"myredis/interface/tcp"
	"myredis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Config struct {
	Address    string
	MaxConnect uint32
	Timeout    time.Duration
}

var ClientCount int32

// 绑定地址并启动 TCP 服务，监听系统信号实现优雅关闭
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	// 监听操作系统信号，实现优雅关闭
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	// 启动 goroutine 监听信号
	go func() {
		sig := <-sigCh
		// 发送关闭信号
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	// 开始监听指定地址
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	// 处理连接
	ListenAndServe(listener, handler, closeChan)
	return nil
}

// 监听 TCP 连接并处理请求：持续接受新连接，并为每个连接启动一个 goroutine 处理
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	errCh := make(chan error, 1)
	defer close(errCh)

	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case err := <-errCh:
			logger.Info(fmt.Sprintf("accept error: %s", err.Error()))
		}
		logger.Info("Shuting down...")
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup

	// 每一个连接由一个独立的 goroutine 处理
	for {
		conn, err := listener.Accept()
		if err != nil {
			// 如果是超时错误，重新尝试
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Infof("accept occurs timeout error: %v", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errCh <- err
			break
		}
		// 建立连接
		logger.Info("accept link")
		ClientCount++
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
				atomic.AddInt32(&ClientCount, -1)
			}()
			// 处理当前连接
			handler.Handle(ctx, conn)
		}()
	}
	waitDone.Wait()
}
