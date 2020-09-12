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
	"math/rand"
	"strings"
	"time"
)

type pulsarStore struct {
	serviceName string
	client      bifrost.Client
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

	rand.Seed(time.Now().UnixNano())
	p, err := pulsar.NewClient(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to connect with Pulsar with provided configuration. failed with error: %v", err)
	}
	return &pulsarStore{client: newClientWrapper(p), serviceName: name}, nil
}

func InitTestEventStore(mockClient bifrost.Client, serviceName string) (bifrost.EventStore, error) {
	return &pulsarStore{client: mockClient, serviceName: serviceName}, nil
}

func (s *pulsarStore) GetServiceName() string {
	return s.serviceName
}

func (s *pulsarStore) Publish(topic string, message []byte) error {
	sn := s.GetServiceName()
	// fqtn: Fully Qualified Topic Name
	// fqtn := fmt.Sprintf("%s.%s", sn, topic) // eventRoot is: io.roava.serviceName, topic is whatever is passed.
	// TODO: compute topic name across Producer and Consumer

	producer, err := s.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name:  sn,
	})
	if err != nil {
		return fmt.Errorf("failed to create new producer with the following error: %v", err)
	}
	// Always close producer after successful production of packets so as not to get the error of
	// ProducerBusy from pulsar.
	defer producer.Close()

	id, err := producer.Send(context.Background(), message)
	if err != nil {
		return fmt.Errorf("failed to send message. %v", err)
	}

	log.Printf("Published message to %s id ==>> %s", topic, byteToHex(id.Serialize()))
	return nil
}

// Manually put the fqdn of your topics.
func (s *pulsarStore) Subscribe(topic string, handler bifrost.SubscriptionHandler) error {
	serviceName := s.GetServiceName()
	consumer, err := s.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		AutoDiscoveryPeriod:         0,
		SubscriptionName:            fmt.Sprintf("subscription-%s", generateRandomName()),
		Type:                        pulsar.Exclusive,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		Name:                        serviceName,
	})
	if err != nil {
		return fmt.Errorf("error subscribing to topic. %v", err)
	}

	defer consumer.Close()
	for {
		message, err := consumer.Recv(context.Background())
		if err == bifrost.ErrCloseConn {
			break
		}
		if err != nil {
			continue
		}

		event := NewEvent(message, consumer)
		go handler(event)
	}
	return nil
}

func (s *pulsarStore) Run(ctx context.Context, handlers ...bifrost.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}
	for  {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func byteToHex(b []byte) string {
	var out struct{}
	_ = json.Unmarshal(b, &out)
	return hex.EncodeToString(b)
}

func generateRandomName() string {
	chars := "abcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, 10)
	for i := range bytes {
		bytes[i] = chars[rand.Intn(len(chars))]
	}
	return string(bytes)
}
