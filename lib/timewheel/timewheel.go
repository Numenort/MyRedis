package timewheel

import (
	"container/list"
	"myredis/lib/logger"
	"sync"
	"time"
)

type location struct {
	slot  int
	etask *list.Element
}

type TimeWheel struct {
	interval time.Duration
	ticker   *time.Ticker
	slots    []*list.List

	timer             map[string]*location
	currentPos        int
	slotNum           int // 时间槽个数
	addTaskChannel    chan task
	removeTaskChannel chan string
	stopChannel       chan bool

	mu sync.RWMutex
}

type task struct {
	delay  time.Duration
	circle int // 需要等待的圈数
	key    string
	job    func()
}

// ******************** Initize timeWheel ********************

func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]*location),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()
	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// ******************** functional ********************
// 对外暴露的方法，包含启动、关闭、增加Job、移除Job

func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval) // 启动定时器
	go tw.start()
}

func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{delay: delay, key: key, job: job}
}

func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

// ******************** function implement ********************
// 通过 select 循环实现时间轮转功能

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		case task := <-tw.removeTaskChannel:
			tw.removeTask(task)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

// 时间轮转，列表中的每个链表存储任务
func (tw *TimeWheel) tickHandler() {
	tw.mu.Lock()
	l := tw.slots[tw.currentPos]
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0 // One Circle
	} else {
		tw.currentPos++
	}
	tw.mu.Unlock()

	go tw.scanAndRunTask(l)
}

// 具体执行任务
func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	var taskToRemove []string
	tw.mu.RLock()
	// 访问对应的任务链表
	for s := l.Front(); s != nil; {
		task := s.Value.(*task)
		// 轮转次数为 0 时，执行任务
		if task.circle > 0 {
			task.circle--
			s = s.Next()
			continue
		}

		// 执行当前任务
		go func(job func()) {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job()
		}(task.job)

		// 执行命令，加入移除名单
		if task.key != "" {
			taskToRemove = append(taskToRemove, task.key)
		}

		next := s.Next()
		l.Remove(s)
		s = next
	}
	tw.mu.RUnlock()

	// 删除任务
	tw.mu.Lock()
	for _, key := range taskToRemove {
		delete(tw.timer, key)
	}
	tw.mu.Unlock()
}

func (tw *TimeWheel) addTask(task *task) {
	// 计算任务位置
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 该 key 已经存在一个定时任务，移除旧的任务
	if task.key != "" {
		if _, ok := tw.timer[task.key]; ok {
			tw.removeTaskInternal(task.key)
		}
	}

	e := tw.slots[pos].PushBack(task)
	loc := &location{
		slot:  pos,
		etask: e,
	}
	tw.timer[task.key] = loc
}

func (tw *TimeWheel) removeTask(key string) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.removeTaskInternal(key)
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = delaySeconds / intervalSeconds / tw.slotNum
	pos = (tw.currentPos + delaySeconds/intervalSeconds) % tw.slotNum
	return
}

func (tw *TimeWheel) removeTaskInternal(key string) {
	pos, ok := tw.timer[key]
	if !ok {
		return
	}
	l := tw.slots[pos.slot]
	l.Remove(pos.etask)
	delete(tw.timer, key)
}
