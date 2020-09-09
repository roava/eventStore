package bifrost

import "github.com/pkg/errors"

type SubscriptionHandler func(event Event)
type EventHandler func()

var (
	ErrEmptyStoreName          = errors.New("Sorry, you must provide a valid store name")
	ErrInvalidURL              = errors.New("Sorry, you must provide a valid store URL")
	ErrInvalidTlsConfiguration = errors.New("Sorry, you have provided an invalid tls configuration")
)

type EventStore interface {
	Publish(topic string, message []byte) error
	Subscribe(topic string, handler SubscriptionHandler) error
	GetServiceName() string
	Run(handlers...EventHandler)
}
