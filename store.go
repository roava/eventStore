package bifrost

import "github.com/pkg/errors"

type SubscriptionHandler func(event Event)

var (
	EmptyStoreNameErr            = errors.New("Sorry, you must provide a valid store name")
	InvalidURLErr                = errors.New("Sorry, you must provide a valid store URL")
	InvalidTlsConfigurationError = errors.New("Sorry, you have provided an invalid tls configuration")
)
type EventStore interface {
	Publish(topic string, message []byte) error
	Subscribe(topic string, handler SubscriptionHandler) error
	GetServiceName() string
}
