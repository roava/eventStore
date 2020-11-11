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
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type pulsarStore struct {
	serviceName string
	client      pulsar.Client
	opts        bifrost.Options
	logger      *logrus.Logger
	debug       bool

	// the below values are only useful for testing
	testMode bool
	ch       chan []byte
}

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
	lg := logrus.New()
	lg.SetFormatter(&logrus.JSONFormatter{})
	return &pulsarStore{client: p, serviceName: name, logger: lg, testMode: false, debug: opts.Debug}, nil
}

func InitTestEventStore(serviceName string, logger *logrus.Logger) (bifrost.EventStore, error) {
	return &pulsarStore{serviceName: serviceName, testMode: true,
		ch: make(chan []byte, 1), logger: logger}, nil
}

func (s *pulsarStore) GetServiceName() string {
	return s.serviceName
}

func (s *pulsarStore) Publish(topic string, message []byte) error {
	if s.testMode {
		s.logger.Info("pushing message into test channel")
		s.ch <- message
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
		Name:  sn,
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
		s.logger.WithFields(logrus.Fields{
			"topic":          topic,
			"message_hex_id": byteToHex(id.Serialize()),
		}).Info("message published")
	}
	return nil
}

func (s *pulsarStore) Subscribe(topic string, handler bifrost.SubscriptionHandler) error {
	if s.testMode {
		for {
			select {
			case msg, ok := <-s.ch:
				if ok {
					s.logger.WithField("data", string(msg)).Info("recv message in test mode")
					// consumer in platform can be nil, we don't use it because we don't ack test event
					ev := platform.NewEvent(platform.NewPlatformMessage(pulsar.LatestMessageID(), topic,
						msg), nil)
					go handler(ev)
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
		select {
		case cm, ok := <-consumer.Chan():
			if !ok {
				return bifrost.ErrCloseConn
			}
			if s.debug {
				s.logger.WithFields(logrus.Fields{
					"data":  string(cm.Payload()),
					"topic": cm.Topic(),
				}).Info("new event received")
			}
			event := platform.NewEvent(cm, cm)
			go handler(event)
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
		s.ch = make(chan []byte, len(messages))
	}
	for idx, message := range messages {
		data, err := json.Marshal(message)
		if err != nil {
			return err
		}
		if s.debug {
			s.logger.WithField("index", idx).Info("publishing message")
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
