package event_bus

import (
	"github.com/pkg/errors"
	"sync"
)

type Topic struct {
	Name  string
	Limit int
}

type TopicEntry struct {
	Topic       string `json:"topic"`
	Subscribers []Subscriber
	Limit       int
	parent      EventBus

	sync.Mutex
}

// AddSubscriber copy on write
func (e *TopicEntry) AddSubscriber(subscriber Subscriber) error {
	if e.Limit < len(e.Subscribers)+1 {
		return errors.Errorf("subscriber num over limit, limit: %d", e.Limit)
	}
	e.Lock()
	defer func() {
		e.Unlock()
	}()

	newSubs := make([]Subscriber, len(e.Subscribers))
	copy(newSubs, e.Subscribers)

	newSubs = append(newSubs, subscriber)
	e.setNewSubscribers(newSubs)
	return nil
}

func (e *TopicEntry) setNewSubscribers(newSubs []Subscriber) {
	e.Subscribers = newSubs
}

func (e *TopicEntry) GetAllSubscriber() []Subscriber {
	res := make([]Subscriber, len(e.Subscribers))
	copy(res, e.Subscribers)
	return res
}

func (e *TopicEntry) NotifyEvent(event Event) {
	if len(e.Subscribers) == 0 {
		return
	}

	for _, subscriber := range e.GetAllSubscriber() {
		subscriber := subscriber
		go func() {
			err := subscriber.OnEvent(event, e.Topic)
			if err != nil {
				e.parent.logger.Println("EventBus|NotifyEvent OnEvent event: %v, err: %s", event, err)
			}
		}()
	}
}
