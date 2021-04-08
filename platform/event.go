package platform

import (
	"encoding/json"
	"errors"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

// Event is the Roava platform event according to https://fcmbuk.atlassian.net/wiki/spaces/ROAV/pages/393740319/Events
type platformEvent struct {
	consumer pulsar.Consumer
	message  Message
}

type Data struct {
	SpecVersion string          `json:"spec_version"`
	Type        string          `json:"type"`
	Source      string          `json:"source"`
	Id          string          `json:"id"`
	Time        time.Time       `json:"time"`
	ContentType string          `json:"content_type"`
	Data        json.RawMessage `json:"data"`
}

type Event interface {
	Ack()
	NAck()
	Parse(value interface{}) (*Data, error)
	Topic() string
}

func NewEvent(message Message, consumer pulsar.Consumer) Event {
	return &platformEvent{consumer: consumer, message: message}
}

func (p *platformEvent) Ack() {
	if p.consumer == nil || p.message == nil {
		return
	}
	p.consumer.AckID(p.message.ID())
}

func (p *platformEvent) NAck() {
	if p.consumer == nil || p.message == nil {
		return
	}
	p.consumer.NackID(p.message.ID())
}

func (p *platformEvent) Parse(value interface{}) (*Data, error) {
	d := &Data{}
	payload := p.message.Payload()
	if err := json.Unmarshal(payload, d); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(d.Data, value); err != nil {
		return nil, err
	}
	if value == nil {
		return d, errors.New("event payload.data is empty")
	}
	return d, nil
}

func (p *platformEvent) Topic() string {
	return p.message.Topic()
}

type Message interface {
	ID() pulsar.MessageID
	Payload() []byte
	Topic() string
}

type PlatformMessage struct {
	id    pulsar.MessageID
	topic string
	data  []byte
}

func NewPlatformMessage(id pulsar.MessageID, topic string, data []byte) *PlatformMessage {
	return &PlatformMessage{id: id, data: data, topic: topic}
}

func (pm *PlatformMessage) ID() pulsar.MessageID {
	return pm.id
}

func (pm *PlatformMessage) Payload() []byte {
	return pm.data
}

func (pm *PlatformMessage) Topic() string {
	return pm.topic
}
