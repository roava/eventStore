package pulse

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/roava/bifrost"
	"github.com/roava/bifrost/platform"
	"go.uber.org/zap"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type pulsarStore struct {
	serviceName string
	client      pulsar.Client
	opts        bifrost.Options
	logger      *zap.Logger
	debug       bool

	// the below values are only useful for testing
	testMode  bool
	consumers map[string]chan []byte
	mtx       sync.Mutex
}

var _ bifrost.EventStore = &pulsarStore{}

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
	if opts.CertContent != "" {
		certPath, err := initCert(opts.CertContent)
		if err != nil {
			return nil, err
		}
		clientOptions.TLSAllowInsecureConnection = true
		clientOptions.TLSTrustCertsFilePath = certPath
	}
	if opts.AuthenticationToken != "" {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(opts.AuthenticationToken)
	}

	rand.Seed(time.Now().UnixNano())
	p, err := pulsar.NewClient(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to Pulsar with provided configuration. failed with error: %v", err)
	}
	logger, _ := zap.NewProduction()
	return &pulsarStore{client: p, serviceName: name, logger: logger, testMode: false, debug: opts.Debug}, nil
}

func InitTestEventStore(serviceName string, logger *zap.Logger) (bifrost.EventStore, error) {
	return &pulsarStore{serviceName: serviceName, testMode: true,
		consumers: map[string]chan []byte{}, logger: logger}, nil
}

func (s *pulsarStore) GetServiceName() string {
	return s.serviceName
}

func (s *pulsarStore) Publish(topic string, message []byte) error {
	if s.testMode {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
				s.mtx.Lock()
				ch, ok := s.consumers[topic]
				s.mtx.Unlock()
				if ok {
					ch <- message
					break loop
				}
			}
		}
		return nil
	}
	// sn here is the topic root prefix eg: is: io.roava.kyc
	// topic is whatever is passed eg: application.started
	sn := s.serviceName
	// fqtn: Fully Qualified Topic Name eg: io.roava.kyc.application.started
	// fqtn := fmt.Sprintf("%s.%s", sn, topic)
	// TODO: compute FQTN. We need to think of a way to form topicName. So that we don't confuse Consumers
	// TODO: also, i think it makes sense to delegate topic naming to the calling services.
	producer, err := s.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		Name:  fmt.Sprintf("%s-producer-%s", sn, generateRandomName()), // the servicename-producer-randomstring
	})
	if err != nil {
		return fmt.Errorf("failed to create new producer with the following error: %v", err)
	}
	// Always close producer after successful production of packets so as not to get the error of
	// ProducerBusy from pulsar.
	defer producer.Close()

	id, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: message, EventTime: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("failed to send message. %v", err)
	}
	if s.debug {
		s.logger.With(zap.String(
			"topic", topic),
			zap.String("message_hex_id", byteToHex(id.Serialize()))).Info("message published")
	}
	return nil
}

// Subscribe a default context is created to be used with the internal subscribe method
func (s *pulsarStore) Subscribe(topic string, handler bifrost.SubscriptionHandler) error {
	ctx := context.Background()
	return s.subscribeWithCtx(ctx, topic, handler)
}

// SubscribeCtx new subscribe method that takes a context and allows a user close the connection by using the done signal
func (s *pulsarStore) SubscribeCtx(context context.Context, topic string, handler bifrost.SubscriptionHandler) error {
	return s.subscribeWithCtx(context, topic, handler)
}

// subscribeWithCtx creates a new pulsar consumer and listens to events
// closes the consumer on ctx.Done()
func (s *pulsarStore) subscribeWithCtx(context context.Context, topic string, handler bifrost.SubscriptionHandler) error {
	if s.testMode {
		s.mtx.Lock()
		ch := s.consumers[topic]
		s.mtx.Unlock()
		for {
			select {
			case msg, ok := <-ch:
				if ok {
					if s.debug {
						s.logger.With(zap.String("data", string(msg))).Info("recv message in test mode")
					}
					// consumer in platform can be nil, we don't use it because we don't ack test event
					ev := platform.NewEvent(platform.NewPlatformMessage(pulsar.LatestMessageID(), topic,
						msg), nil)
					handler(ev)
				}
			default:
				return nil
			}
		}
	}

	serviceName := s.GetServiceName()
	consumer, err := s.client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		AutoDiscoveryPeriod:         0,
		SubscriptionName:            fmt.Sprintf("%s-%s", serviceName, topic),
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		Name:                        serviceName,
	})
	if err != nil {
		return fmt.Errorf("error subscribing to topic. %v", err)
	}

	defer consumer.Close()
	for {
		select {
		case cm, ok := <-consumer.Chan():
			if !ok {
				return bifrost.ErrCloseConn
			}
			if s.debug {
				s.logger.With(zap.String(
					"data", string(cm.Payload())),
					zap.String("topic", cm.Topic())).Info("new event received")
			}
			event := platform.NewEvent(cm, cm)
			go handler(event)
		case <-context.Done():
			consumer.Close()
		default:
		}
	}
}

func (s *pulsarStore) Run(ctx context.Context, handlers ...bifrost.EventHandler) {
	for _, handler := range handlers {
		go handler.Run()
	}
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

func (s *pulsarStore) PublishRaw(topic string, messages ...interface{}) error {
	if len(messages) == 0 {
		return errors.New("invalid message size")
	}
	if s.testMode {
		s.mtx.Lock()
		s.consumers[topic] = make(chan []byte, len(messages))
		s.mtx.Unlock()
	}
	for idx, message := range messages {
		data, err := json.Marshal(message)
		if err != nil {
			return err
		}
		if s.debug {
			s.logger.With(zap.Int("index", idx)).Info("publishing message")
		}
		err = s.Publish(topic, data)
		if err != nil {
			return err
		}
	}
	return nil
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

func initCert(content string) (string, error) {
	if len(content) == 0 {
		return "", errors.New("cert content is empty")
	}

	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	certPath := filepath.Join(pwd, "tls.crt")
	if err := ioutil.WriteFile(certPath, []byte(content), os.ModePerm); err != nil {
		return "", err
	}
	return certPath, nil
}
