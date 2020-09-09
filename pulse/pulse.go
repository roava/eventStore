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
		return nil, bifrost.InvalidURLErr
	}

	name := strings.TrimSpace(opts.ServiceName)
	if name == "" {
		return nil, bifrost.EmptyStoreNameErr
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
		log.Print("Unable to connect with Pulsar with provided configuration. Failed with error: ", err)
		return nil, err
	}
	return &pulsarStore{client: p, serviceName: name}, nil
}

func (s *pulsarStore) GetServiceName() string {
	return s.serviceName
}

func (s *pulsarStore) Publish(topic string, message []byte) error {
	sn := s.GetServiceName()
	__topic__ := fmt.Sprintf("%s.%s", sn, topic) // eventRoot is: io.roava.serviceName, topic is whatever is passed.

	producer, err := s.client.CreateProducer(pulsar.ProducerOptions{
		Topic: __topic__,
		Name:  sn,
	})
	if err != nil {
		log.Println("Failed to create new producer with the following error", err)
		return err
	}
	// Always close producer after successful production of packets so as not to get the error of
	// ProducerBusy from pulsar.
	defer producer.Close()

	id, e := producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload:   message,
		EventTime: time.Now(),
	})

	if e != nil {
		log.Println(e, " Failed to send message.")
		return e
	}

	log.Printf("Published message to %s id ==>> %s", __topic__, byteToHex(id.Serialize()))
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
		fmt.Print(err, " Subscribing to unknown topic.")
		return err
	}

	for {
		if val, ok := <-consumer.Chan(); ok {
			event := NewEvent(val)
			// TODO: Ensure event struct is according to the Roava Ecosystem.
			go handler(event)
			// TODO: Decide if we want to add something to stream this data to another place as backup.
		}
	}
}

func byteToHex(b []byte) string {
	var out struct{}
	_ = json.Unmarshal(b, &out)
	return hex.EncodeToString(b)
}
