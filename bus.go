package event_bus

import (
	"binlog_agent/common/cerror"
	"context"
	cmap "github.com/orcaman/concurrent-map/v2"
	"log"
	"sync"
)

type Subscriber interface {
	OnEventHandler
	BizId() string
}

type OnEventHandler interface {
	OnStart(topic string) error
	OnEvent(event Event, topic string) error
	OnClose(err error, topic string) error
}

type EventBus struct {
	entries    sync.Map
	Ch         chan Event // channel transfer event
	cancelFunc context.CancelFunc
	CloseErr   error `json:"closeErr"`

	logger log.Logger
}

func (e *EventBus) Open(ctx context.Context, initSize int) error {
	e.Ch = make(chan Event, initSize)
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	go func() {
		err := e.loopEvent(cancelCtx)
		if err != nil {
			e.CloseErr = err
			return
		}
	}()
	e.cancelFunc = cancelFunc
	return nil
}

func (e *EventBus) AddTopic(ctx context.Context) error {

}

func (e *EventBus) Close() error {
	e.cancelFunc()
	return e.CloseErr
}

func (e *EventBus) loopEvent(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			close(e.Ch)
			return nil
		case event := <-e.Ch:
			if topicEntry, ok := e.entries.Get(event.Topic); ok {
				for _, subscriber := range topicEntry.Subscribers {
					err := subscriber.OnEvent(event, event.Topic)
					if err != nil {
						return err
					}
				}
			}
			// ignore when is not oksw
		}
	}
}

// Subscribe 订阅
func (e *EventBus) Subscribe(topic string, subscriber Subscriber) error {
	topicEntry, ok := e.entries.Get(topic)
	if !ok {
		topicEntry = &TopicEntry{
			Topic:       topic,
			Subscribers: []Subscriber{subscriber},
		}
	} else {
		// check if subscribe exist
		exist := false
		for _, sub := range topicEntry.Subscribers {
			if sub.BizId() == subscriber.BizId() {
				exist = true
				break
			}
		}
		if exist {
			return cerror.SubscriberExistErr
		}
		topicEntry.Subscribers = append(topicEntry.Subscribers, subscriber)
	}
	e.entries.Set(topic, topicEntry)
	err := subscriber.OnStart(topic)
	if err != nil {
		return err
	}
	return nil
}

func (e *EventBus) UnSubscribe(topic string, subscriber Subscriber) error {
	topicEntry, ok := e.entries.Get(topic)
	// check if in topicEntry
	if !ok {
		return cerror.TopicNotFoundErr
	}

	exist := false
	for _, sub := range topicEntry.Subscribers {
		if sub.BizId() == subscriber.BizId() {
			exist = true
			break
		}
	}
	if !exist {
		return cerror.SubscriberNotExistErr
	}

	topicEntry.Lock()
	defer topicEntry.Unlock()

}

func (e *EventBus) Send(event Event, topic string) error {
	_, ok := e.entries.Get(topic)
	if !ok {
		return nil
	}
	e.Ch <- event
	return nil
}

func NewEventBus() EventBus {
	c := cmap.New[*TopicEntry]()
	bus := EventBus{entries: c}
	return bus
}
