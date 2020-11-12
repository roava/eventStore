package pulse

import (
	"context"
	"github.com/roava/bifrost"
	"github.com/roava/bifrost/events"
	"github.com/roava/bifrost/platform"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestInit(t *testing.T) {
	addrs := "pulsar://localhost:6650"
	opts := bifrost.Options{
		ServiceName:         "test-service",
		Address:             addrs,
		CertContent:         "",
		AuthenticationToken: "",
	}
	store, err := Init(opts)
	assert.Nil(t, err)

	type S struct {
		Message string `json:"message"`
	}
	go func() {
		time.Sleep(4 * time.Second)
		value := &S{Message: "Hello, World!"}
		e := &events.Event{Data: value, Id: generateRandomName()}
		err = store.PublishRaw("test-topic", e)
	}()

	c := make(chan struct{}, 1)
	go func() {
		err = store.Subscribe("test-topic", func(event platform.Event) {
			value := &S{}
			data, err := event.Parse(value)
			assert.Nil(t, err)
			assert.NotNil(t, data)
			assert.Equal(t, value.Message, "Hello, World!")
			t.Log(event.Topic())
			c <- struct{}{}
			event.Ack()
		})
	}()

	<-c
}

func TestPulsarStore_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store, _ := InitTestEventStore("nil", logrus.StandardLogger())

	now := time.Now()
	// cancel after 3secs
	time.AfterFunc(3*time.Second, func() {
		t.Log("cancelling...")
		cancel()
	})
	store.Run(ctx, func() error {
		t.Log("first function")
		return nil
	}, func() error {
		t.Log("second function ")
		return nil
	})
	interval := time.Now().Sub(now)
	if interval.Seconds() < 3 {
		t.Fail()
	}
}

func Test_generateRandomName(t *testing.T) {
	assert.NotEmpty(t, generateRandomName())
}

func TestIntegration(t *testing.T) {
	logger := logrus.StandardLogger()
	bf, err := InitTestEventStore("test-event-store", logger)
	assert.Nil(t, err)
	assert.NotNil(t, bf)

	type e struct {
		Message string `json:"message"`
	}
	h := &eventHandler{bf: bf}
	err = bf.PublishRaw("test-topic",
		&e{Message: "Yello"}, &e{Message: "Jello"}, &e{Message: "Mello"},
		&e{Message: "Yello"}, &e{Message: "Jello"}, &e{Message: "Mello"},
		&e{Message: "Yello"}, &e{Message: "Jello"}, &e{Message: "Mello"})

	err = h.handleEvent()
	assert.Nil(t, err)
}

type eventHandler struct {
	bf bifrost.EventStore
}

func (e *eventHandler) handleEvent() error {
	return e.bf.Subscribe("test-topic", func(event platform.Event) {
		type e struct {
			Message string `json:"message"`
		}
		value := &e{}
		d, err := event.Parse(value)
		log.Println(err)
		log.Println(d)
	})
}
