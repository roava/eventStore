package pulse

import (
	"github.com/roava/bifrost"
)

type event struct {
	raw      bifrost.Message
	consumer bifrost.Consumer
}

func NewEvent(message bifrost.Message, consumer bifrost.Consumer) bifrost.Event {
	return &event{raw: message, consumer: consumer}
}

func (e *event) Data() []byte {
	return e.raw.Payload()
}

func (e *event) Topic() string {
	t := e.raw.Topic()
	// Manually agree what we want the topic to look like from pulsar.
	return t
}

func (e *event) Ack() {
	e.consumer.Ack(e.raw.ID())
}
