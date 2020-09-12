package bifrost

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
	"log"
	"time"
)

type SubscriptionHandler func(event Event)
type EventHandler func() error

var (
	ErrEmptyStoreName          = errors.New("Sorry, you must provide a valid store name")
	ErrInvalidURL              = errors.New("Sorry, you must provide a valid store URL")
	ErrInvalidTlsConfiguration = errors.New("Sorry, you have provided an invalid tls configuration")
	ErrCloseConn = errors.New("connection closed")
)

type EventStore interface {
	Publish(topic string, message []byte) error
	Subscribe(topic string, handler SubscriptionHandler) error
	GetServiceName() string
	Run(ctx context.Context, handlers ...EventHandler)
}

type Message interface {
	ID() pulsar.MessageID
	Payload() []byte
	Topic() string
}

type Consumer interface {
	Recv(ctx context.Context) (Message, error)
	Ack(pulsar.MessageID)
	Close()
}

type Producer interface {
	Send(context.Context, []byte) (pulsar.MessageID, error)
	Close()
}

type Client interface {
	CreateProducer(pulsar.ProducerOptions) (Producer, error)
	Subscribe(pulsar.ConsumerOptions) (Consumer, error)
	CreateReader(pulsar.ReaderOptions) (pulsar.Reader, error)
	TopicPartitions(string) ([]string, error)
	Close()
}

func (f EventHandler) Run() {
	for {
		err := f()
		if err != nil {
			log.Printf("creating a consumer returned error: %v. Reconnecting in 3secs...", err)
			time.Sleep(3 * time.Second)
			continue
		}
	}
}

