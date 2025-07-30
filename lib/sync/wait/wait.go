package wait

import (
	"context"
	"sync"
	"time"
)

type Wait struct {
	wg sync.WaitGroup
}

func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) Wait() {
	w.wg.Wait()
}

// 等待超时后返回 true，否则返回 false
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return w.waitWithTimeoutDuration(ctx)
}

func (w *Wait) waitWithTimeoutDuration(ctx context.Context) bool {
	done := make(chan struct{})
	go func() {
		// 等待完成后发送关闭信号
		defer close(done)
		w.Wait()
	}()
	select {
	case <-done:
		return false
	case <-ctx.Done():
		return true
	}
}
