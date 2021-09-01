package server

import (
	"container/heap"
	"context"
	"time"
)

type eventType int

const (
	evTypeShardUpdate eventType = iota
	evTypeShardDel
	evTypeContainerUpdate
	evTypeContainerDel
)

type event struct {
	typ eventType

	// 扔进queue的时间
	start int64

	// 期望被处理的时间
	expect int64

	// shard或者server的负载
	load string
}

type eventQueue struct {
	ctx context.Context

	// 异步event处理
	evChan chan *event

	// 处理goroutine数量可调节
	workerCount int

	// 延迟队列: 不能立即处理的先放这里，启动单独的goroutine把event根据时间拿出来，再放到异步队列中
	pq *priorityQueue
}

func newEventQueue(ctx context.Context) *eventQueue {
	evq := eventQueue{
		ctx:         ctx,
		evChan:      make(chan *event, 32),
		workerCount: 32,
	}

	pq := priorityQueue{}
	heap.Init(&pq)
	evq.pq = &pq

	go evq.pqLoop()

	return &evq
}

func (q *eventQueue) push(ev *event) {
	switch ev.typ {
	case evTypeShardUpdate, evTypeContainerUpdate:
		q.evChan <- ev
	case evTypeShardDel, evTypeContainerDel:
		if time.Now().Unix() < ev.expect {
			heap.Push(q.pq, ev)
		} else {
			q.evChan <- ev
		}
	}
}

func (q *eventQueue) pqLoop() {
	ticker := time.Tick(1 * time.Second)
	for {
		select {
		case <-q.ctx.Done():
			Logger.Println("pqLoop exit")
		case <-ticker:
		popASAP:
			item := heap.Pop(q.pq)
			if item == nil {
				continue
			}

			ev := item.(*event)
			if time.Now().Unix() < ev.expect {
				continue
			}
			q.push(ev)

			// 存在需要处理的事件，立即pop，减小延迟
			goto popASAP
		}
	}
}

func (q *eventQueue) startWorker() {
	for i := 0; i < q.workerCount; i++ {
		go func() {
			for {
				select {
				case <-q.ctx.Done():
					Logger.Println("startWorker exit")
				case ev := <-q.evChan:
					switch ev.typ {
					case evTypeShardUpdate:
						// TODO 解析load，确定shard的load超出阈值，触发shard move
					case evTypeShardDel:
						// TODO 检查shard是否都处于有container的状态
					case evTypeContainerUpdate:
						// TODO
					case evTypeContainerDel:
						// TODO
					}
				}
			}
		}()
	}
}

type priorityQueue struct {
	events []*event
}

func (pq *priorityQueue) Swap(i, j int) {
	pq.events[i], pq.events[j] = pq.events[j], pq.events[i]
}
func (pq *priorityQueue) Len() int { return len(pq.events) }
func (pq *priorityQueue) Less(i, j int) bool {
	return pq.events[i].expect < pq.events[j].expect
}
func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*event)
	pq.events = append(pq.events, item)
}
func (pq *priorityQueue) Pop() interface{} {
	if len(pq.events) == 0 {
		return nil
	}

	old := pq.events
	n := len(old)
	item := pq.events[n-1]
	pq.events = old[0 : n-1]
	return item
}
