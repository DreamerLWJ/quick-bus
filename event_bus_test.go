package event_bus

import (
	"context"
	"fmt"
	"testing"
)

type TestTopicSubscriber struct {
	Id string
}

func (t TestTopicSubscriber) BizId() string {
	return t.Id
}

func (t TestTopicSubscriber) OnStart(topic string) error {
	fmt.Printf("topic: %s, started\n", topic)
	return nil
}

func (t TestTopicSubscriber) OnEvent(event Event, topic string) error {
	fmt.Printf("topic: %s, recv event: %v\n", topic, event)
	return nil
}

func (t TestTopicSubscriber) OnClose(err error, topic string) error {
	fmt.Printf("topic: %s, close\n", topic)
	return nil
}

func TestEventBus_Subscribe(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus()
	err := bus.Init(ctx, 10)
	if err != nil {
		panic(err)
	}

	err = bus.Send(Event{
		Topic: "test1",
		Data:  "good",
		Desc:  "good",
	}, "test1")
	if err != nil {
		panic(err)
	}

	err = bus.Subscribe("test1", TestTopicSubscriber{Id: "test1"})
	if err != nil {
		panic(err)
	}

	err = bus.Send(Event{
		Topic: "test1",
		Data:  "good",
		Desc:  "good",
	}, "test1")
	if err != nil {
		panic(err)
	}
}
