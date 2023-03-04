package event_bus

type Event struct {
	Topic string      `json:"topic"`
	Data  interface{} `json:"data"`
	Desc  string      `json:"desc"`
}
