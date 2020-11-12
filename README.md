# bifrost
An internal lib to help with pulsar usage inside Roava. 

## Features
* helper method to create event schema
* fake event producer - for unit testing
* helper method to parse event json data
* validation of event `payload.data`, a nil value will return an error
 
## Usage

```go
package main

import (
 "github.com/roava/bifrost"
 "github.com/roava/bifrost/events"
"github.com/roava/bifrost/platform"
"github.com/roava/bifrost/pulse"
 "log"
 "time"
)
type Account struct{
    FirstName string `json:"first_name"`
    LastName string `json:"last_name"`
    Email string `json:"email"`
}

func main() {
	
    // publishing an event to a topic
    // assuming we're publishing to a topic name `io.roava.account.created`
    topic := "io.roava.account.created"
    store, err := pulse.Init(bifrost.Options{
        ServiceName: "test-event-store",
	    Address: "pulsar://localhost:6650",
        Debug: true,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    account := &Account{
        FirstName: "Foo",
        LastName: "Gopher",
        Email: "go@gopher.com",
    }
    // you can build an event like below
    event := events.New().
    		WithContentType("application/json").
    		WithId("id").
    		WithSource("ms.person").
    		WithSpecVersion("v2").
    		WithType("type").
    		WithTime(time.Now()).
    		WithData(account)
    // you can also do it directly like below 
    ev := &events.Event{
    		SpecVersion: "v2",
    		Type:        "type",
    		Source:      "ms.test",
    		Id:          "id",
    		Time:        time.Now(),
    		ContentType: "application/json",
    		Data: account,
    	}
    // publish the event
	if err := store.PublishRaw(topic, ev|event); err != nil {
		log.Fatalf("Could not publish message to %s", topic)
	}
	
    // handling event or creating subscriber
    err = store.Subscribe(topic, func(event platform.Event) {
        // you can parse event data like below
        a := &Account{}
        data, err := event.Parse(a)
        if err != nil {
            // handle error
            return
        }
        // you can access event metadata from `data`
        source := data.Source
        Type := data.Type
        
        // ack event
        event.Ack()
    })
}

```

### Unit Testing
##### Publisher

To test event publishing, you can always mock `bifrost.EventStore`.

##### Consumer/Subscriber
Below is an example of how you can test a subscriber.
```go
type eventHandler struct {
    bf bifrost.EventStore
    store db.Datastore
}
func (e *eventHandler) handleAccountCreatedEvent() error {
    topic := "io.roava.account.created"
    return e.bf.Subscribe(topic, func(event platform.Event)) {
        a := &Account{}
        data, err := event.Parse(a)
        if err != nil {
            // handle error
            return
        }
        return e.store.CreateAccount(account)
    }   
}

// event_handler_test.go
func Test_handleAccountCreatedEvent(t *testing.T) {
    // prepare
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()

    storeMock := mocks.NewMockDatastore(ctrl)
    storeMock.Expect().CreateAccount(gomock.Any()).Return(nil).Times(1)
    
    // create an bifrost instance. Not the `InitTestEventStore` function
    bf, err := pulse.InitTestEventStore("ms.test", logrus.StandardLogger())
    assert.Nil(t, err)
    
    handler := &eventHandler{bf: bf, store: storeMock}
    // publish an event
    topic := "io.roava.account.created"
     account := &Account{
        FirstName: "Foo",
        LastName: "Gopher",
        Email: "go@gopher.com",
     }
    event := events.New().
    		WithContentType("application/json").
    		WithId("id").
    		WithSource("ms.person").
    		WithSpecVersion("v2").
    		WithType("type").
    		WithTime(time.Now()).
    		WithData(account)
    // act.
    // publish an event to this topic
    err = bf.PublishRaw(topic, event)
    
    err = handler.handleAccountCreatedEvent()
    assert.Nil(t, err)
}
```

```shell script
# For tests

$ make test 
```
