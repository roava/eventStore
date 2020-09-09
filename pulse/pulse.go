package pulse

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/roava/bifrost"
	"log"
	"strings"
	"time"
)

type pulsarStore struct {
	serviceName string
	client      pulsar.Client
	opts        bifrost.Options
}

// Note: If you need a more controlled init func, write your pulsar lib to implement the EventStore interface.
func Init(opts bifrost.Options) (bifrost.EventStore, error) {
	addr := strings.TrimSpace(opts.Address)
	if addr == "" {
		return nil, bifrost.ErrInvalidURL
	}

	name := strings.TrimSpace(opts.ServiceName)
	if name == "" {
		return nil, bifrost.ErrEmptyStoreName
	}

	clientOptions := pulsar.ClientOptions{URL: addr}
	if opts.TLSCertificate != nil {
		clientOptions.Authentication = pulsar.NewAuthenticationFromTLSCertSupplier(func() (*tls.Certificate, error) {
			// TODO: Really test connecting via Certificates & TLS conf.
			return opts.TLSCertificate, nil
		})
	}

	p, err := pulsar.NewClient(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to connect with Pulsar with provided configuration. failed with error: %v", err)
	}
	return &pulsarStore{client: p, serviceName: name}, nil
}

func InitTestEventStore(mockClient pulsar.Client, serviceName string) (bifrost.EventStore, error) {
	return &pulsarStore{client: mockClient, serviceName: serviceName}, nil
}

func (s *pulsarStore) GetServiceName() string {
	return s.serviceName
}

func (s *pulsarStore) Publish(topic string, message []byte) error {
	sn := s.GetServiceName()
	// fqtn: Fully Qualified Topic Name
	fqtn := fmt.Sprintf("%s.%s", sn, topic) // eventRoot is: io.roava.serviceName, topic is whatever is passed.

	producer, err := s.client.CreateProducer(pulsar.ProducerOptions{
		Topic: fqtn,
		Name:  sn,
	})
	if err != nil {
		return fmt.Errorf("failed to create new producer with the following error: %v", err)
	}
	// Always close producer after successful production of packets so as not to get the error of
	// ProducerBusy from pulsar.
	defer producer.Close()

	id, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload:   message,
		EventTime: time.Now(),
	})

	if err != nil {
		return fmt.Errorf("failed to send message. %v", err)
	}

	log.Printf("Published message to %s id ==>> %s", fqtn, byteToHex(id.Serialize()))
	return nil
}

// Manually put the fqdn of your topics.
func (s *pulsarStore) Subscribe(topic string, handler bifrost.SubscriptionHandler) error {
	serviceName := s.GetServiceName()
	consumer, err := s.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		AutoDiscoveryPeriod:         0,
		SubscriptionName:            serviceName,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		Name:                        serviceName,
	})
	if err != nil {
		return fmt.Errorf("error subscribing to topic. %v", err)
	}

	defer consumer.Close()
	for {
		if val, ok := <-consumer.Chan(); ok {
			event := NewEvent(val)
			// TODO: Ensure event struct is according to the Roava Ecosystem.
			go handler(event)
			// TODO: Decide if we want to add something to stream this data to another place as backup.
		}
	}
}

func (s *pulsarStore) Run(handlers... bifrost.EventHandler) {
	stop := make(chan bool, 1)
	for _, handler := range handlers {
		go handler()
	}
	<- stop
}

func byteToHex(b []byte) string {
	var out struct{}
	_ = json.Unmarshal(b, &out)
	return hex.EncodeToString(b)
}
