package pulse

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/roava/bifrost"
)

type event struct {
	raw      pulsar.Message
	consumer pulsar.Consumer
}

func NewEvent(message pulsar.Message, consumer pulsar.Consumer) bifrost.Event {
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
	e.consumer.AckID(e.raw.ID())
}
