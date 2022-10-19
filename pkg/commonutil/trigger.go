package commonutil

import (
	"context"
	"errors"
	"sync"

	"github.com/entertainment-venue/sm/pkg/logutil"
	"go.uber.org/zap"
)

var (
	defaultWorkerSize = 1
	defaultBufferSize = 128
)

var (
	ErrCallbackNil   = errors.New("trigger: callback nil")
	ErrCallbackExist = errors.New("trigger: callback exist")
	ErrEventIllegal  = errors.New("trigger: event error")
)

// https://github.com/grpc/grpc-go/blob/689f7b154ee8a3f3ab6a6107ff7ad78189baae06/internal/transport/controlbuf.go#L40
type itemNode struct {
	it   interface{}
	next *itemNode
}

type itemList struct {
	head *itemNode
	tail *itemNode
}

func (il *itemList) enqueue(i interface{}) {
	n := &itemNode{it: i}
	if il.tail == nil {
		il.head, il.tail = n, n
		return
	}
	il.tail.next = n
	il.tail = n
}

func (il *itemList) dequeue() interface{} {
	if il.head == nil {
		return nil
	}
	i := il.head.it
	il.head = il.head.next
	if il.head == nil {
		il.tail = nil
	}
	return i
}

func (il *itemList) ForEach(visitor func(it interface{}) error) error {
	if il.head == nil {
		return nil
	}

	// 保持head位置不变，临时变量ih走到tail，逐个调用visitor
	ih := il.head
	for ih != il.tail {
		if err := visitor(ih.it); err != nil {
			return err
		}
		ih = ih.next
	}
	return nil
}

type Trigger interface {
	Register(key string, callback TriggerCallback) error
	Put(event *TriggerEvent) error
	ForEach(visitor func(it interface{}) error) error
	Close()
}

type TriggerEvent struct {
	Key   string
	Value interface{}
}

type trigger struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	// wg 管理goroutine退出
	wg sync.WaitGroup

	listMu sync.Mutex
	// list 无边界的缓存，使用chan需要有边界，存储外部事件。
	// 这里的 itemList 没有按照key做区分，同key的事件在goroutine池有资源的情况下，
	// 可能并发执行，所以调用方提供的callback需要保证threadsafe，即便做到同key顺序下发，
	// 不同key的配置更新，对于调用方的callback方法也可能访问相同的资源。
	list *itemList

	// ch 和 consumerWaiting 的设定参考：
	// https://github.com/grpc/grpc-go/blob/689f7b154ee8a3f3ab6a6107ff7ad78189baae06/internal/transport/controlbuf.go#L286
	// ch 触发等待 list 中新进元素的goroutine
	ch chan struct{}
	// consumerWaiting 在grpc-go中的设定是loopyWriter的run在调用get(获取事件)这个事情上花费的时间相比网络io较少，所以不能让事件的进入
	// 卡在ch的写入上。用ch做串联，即便提供一定的buffer，还在response写出和回复数据的进入上做了一定耦合。
	consumerWaiting bool

	// callbacks 记录需要callback的event
	callbacks map[string]TriggerCallback

	// buffer 存储提交给goroutine池的
	buffer chan *TriggerEvent
}

// TriggerCallback 把event的value给到调用方
type TriggerCallback func(key string, value interface{}) error

type triggerOptions struct {
	// workerSize 处理callback的goroutine数量
	workerSize int
}

type TriggerOption func(options *triggerOptions)

func WithWorkerSize(v int) TriggerOption {
	return func(options *triggerOptions) {
		options.workerSize = v
	}
}

func NewTrigger(opts ...TriggerOption) (Trigger, error) {
	ops := &triggerOptions{}
	for _, opt := range opts {
		opt(ops)
	}

	// ctx和cancel
	ctx, cancel := context.WithCancel(context.Background())
	tgr := trigger{
		ctx:        ctx,
		cancelFunc: cancel,
		wg:         sync.WaitGroup{},

		list:      &itemList{},
		buffer:    make(chan *TriggerEvent, defaultBufferSize),
		ch:        make(chan struct{}, 1),
		callbacks: make(map[string]TriggerCallback),
	}

	// 运行worker
	workerSize := ops.workerSize
	if ops.workerSize <= 0 {
		workerSize = defaultWorkerSize
	}
	for i := 0; i < workerSize; i++ {
		tgr.wg.Add(1)
		go tgr.run()
	}

	// 运行event获取
	tgr.wg.Add(1)
	go tgr.get()

	return &tgr, nil
}

func (tgr *trigger) Register(key string, callback TriggerCallback) error {
	if callback == nil {
		return ErrCallbackNil
	}

	_, ok := tgr.callbacks[key]
	if ok {
		logutil.Warn(
			"key %s already exist",
			zap.String("key", key),
		)
		return ErrCallbackExist
	}
	tgr.callbacks[key] = callback
	return nil
}

func (tgr *trigger) Close() {
	if tgr.cancelFunc != nil {
		tgr.cancelFunc()
	}
	tgr.wg.Wait()
}

func (tgr *trigger) Put(event *TriggerEvent) error {
	if event == nil || event.Key == "" {
		return ErrEventIllegal
	}

	var wakeUp bool
	tgr.listMu.Lock()
	if tgr.consumerWaiting {
		wakeUp = true
		tgr.consumerWaiting = false
	}
	tgr.list.enqueue(event)
	tgr.listMu.Unlock()
	if wakeUp {
		select {
		case tgr.ch <- struct{}{}:
		default:
		}
	}
	return nil
}

func (tgr *trigger) get() {
	defer tgr.wg.Done()
	for {
		tgr.listMu.Lock()
		h := tgr.list.dequeue()
		if h != nil {
			tgr.listMu.Unlock()
			ev := h.(*TriggerEvent)
			tgr.buffer <- ev
			continue
		}

		tgr.consumerWaiting = true
		tgr.listMu.Unlock()

		select {
		case <-tgr.ch:
			// 解决通过轮询等待的问题，否则就需要在 trigger.list 为空的时候，等待一个可配置的事件
		case <-tgr.ctx.Done():
			logutil.Info("get exit")
			return
		}
	}
}

func (tgr *trigger) run() {
	defer tgr.wg.Done()
	for {
		select {
		case <-tgr.ctx.Done():
			logutil.Info("run exit")
			return
		case ev := <-tgr.buffer:
			if err := tgr.callbacks[ev.Key](ev.Key, ev.Value); err != nil {
				logutil.Error(
					"callback error",
					zap.Error(err),
					zap.String("ev-key", ev.Key),
				)
			}
		}
	}
}

func (tgr *trigger) ForEach(visitor func(it interface{}) error) error {
	return tgr.list.ForEach(visitor)
}
