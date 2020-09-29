package pulse

import (
	"context"
	"github.com/roava/bifrost"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var topic = "test-topic-xxx" // for random topic name.
var store, _ = Init(bifrost.Options{
	ServiceName: "test-service",
	Address:     "pulsar://localhost:6650",
})

const cert =
	`-----BEGIN CERTIFICATE-----
MIIDQDCCAiigAwIBAgIQFmNdB6eBqmhjHzmFqeOHqDANBgkqhkiG9w0BAQsFADA6
MRUwEwYDVQQKEwxjZXJ0LW1hbmFnZXIxITAfBgNVBAMTGHB1bHNhci5zdmMuY2x1
c3Rlci5sb2NhbDAeFw0yMDA5MjgxMDEwMDdaFw0yMDEyMjcxMDEwMDdaMDoxFTAT
BgNVBAoTDGNlcnQtbWFuYWdlcjEhMB8GA1UEAxMYcHVsc2FyLnN2Yy5jbHVzdGVy
LmxvY2FsMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0iK4ymBozdj0
BWYafdAXdDYELWMObp7Vckb613ZMMwNJtjhqJuLVTVuXBsdjOOea0TYf+K2Ie0m2
9Ie2fc+42srqLAU2oiEInivx5a6cnPPlf2TZmhrK591/cz+ReMk7MCWcNTnv4+XZ
FLzWQyI7+iBWy0KfkkeEvrghueF9E74T9rr530AJEvmnXq9fdAo2NCI5vZNBokS4
G7Bqy1RClUMwmDd9pWoZDyhTziCuiWGufPI0jhxYM/n8/cA5KSwxzRQ2BxBscYRw
FlvA04aK70WprF3y6IcdDaUJMQM2qBakUz3OfAewq+bPWk2/mTtZBXKkWHDYFzW6
X0owoKKdxwIDAQABo0IwQDAOBgNVHQ8BAf8EBAMCAgQwHQYDVR0lBBYwFAYIKwYB
BQUHAwEGCCsGAQUFBwMCMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQAD
ggEBAFd4W0V4gLgDHjWJ65XL4iLDI8ly1CrQKbf6sk4rkAicEz6NDgNWzi2fJP7H
FlM3abNFq9Spva+CecikCthvMim8yvsGiUIyLJb7ISiDdgaymr7f8Pyx0QYPyHnF
BTKx7kJwBuzaHTumZ6U99tJvOuQSNK00Ej30jS7bQLWO+wcaUd00leYIjwZ33D7I
1VC/Y4if8w2L18t+f5LJ8K4xl+PmH5kGFabJLJdQLbUIUm+lW7+gndNl6IVeIte5
cNomZDoquhKWaSpVP/zk8vuQsVOMAy28BG0JBHFqTyCdS5+05oAZievjJGh2EaYX
+MKuUn0nwMl/6bg0n4kVN/H+5zM=
-----END CERTIFICATE-----`

func TestInit(t *testing.T) {
	addrs := "pulsar+ssl://pulsar.roava.io:6651"
	opts := bifrost.Options{
		ServiceName:           "test-service",
		Address:               addrs,
		CertContent: cert,
		AuthenticationToken: "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.vGEsDKZNolLbP7PWlhzzAZMaO4MrsswkDf9eMb6S8ME",
	}
	store, err := Init(opts)
	assert.Nil(t, err)

	go func() {
		time.Sleep(4 * time.Second)
		err = store.Publish("test-topic", []byte("Hello, pulsar"))
	}()

	c := make(chan struct{}, 1)
	go func() {
		err = store.Subscribe("test-topic", func(event bifrost.Event) {
			t.Log(string(event.Data()))
			assert.Equal(t, string(event.Data()), "Hello, pulsar")
			c <- struct{}{}
		})
	}()

	<-c
}

func TestStore_Publish(t *testing.T) {
	if err := store.Publish(topic, []byte("Hello World!")); err != nil {
		t.Errorf("Failed to publish data to event store topic %s. Failed with error: %v", topic, err)
	}
}

func TestStore_Subscribe(t *testing.T) {
	timer := time.AfterFunc(3*time.Second, func() {
		if err := store.Subscribe(topic, func(event bifrost.Event) {
			data := event.Data()

			eventTopic := event.Topic()

			if topic != eventTopic {
				t.Errorf("Event topic is not the same as subscription topic. Why?: Expected %s, instead got: %s \n", topic, eventTopic)
				return
			}

			t.Logf("Received data: %s on topic: %s \n", string(data), eventTopic)
			event.Ack() // Acknowledge event.
			return
		}); err != nil {
			t.Errorf("Failed to subscribe to topic: %s, with the following error: %v \n", topic, err)
			return
		}
	})

	defer timer.Stop()
}

func TestPulsarStore_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _ := InitTestEventStore(nil, "svc")

	now := time.Now()
	// cancel after 3secs
	time.AfterFunc(3 * time.Second, func() {
		t.Log("cancelling...")
		cancel()
	})
	store.Run(ctx, func() error {
		t.Log("first function")
		return nil
	}, func() error { t.Log("second function ")
		return nil
	})
	interval := time.Now().Sub(now)
	if interval.Seconds() < 3 {
		t.Fail()
	}
}

func Test_generateRandomName(t *testing.T) {
	t.Log(generateRandomName())
}
