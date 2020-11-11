package platform

import (
	"encoding/json"
	"github.com/roava/bifrost/events"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPlatformEvent_Parse(t *testing.T) {
	type user struct {
		Name     string `json:"name"`
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	ev := &events.Event{
		SpecVersion: "v2",
		Type:        "type",
		Source:      "ms.test",
		Id:          "id",
		Time:        time.Now(),
		ContentType: "application/json",
		Data: &user{
			Name:     "Foo",
			Email:    "Bar",
			Password: "$uper$ecret",
		},
	}
	data, err := json.Marshal(ev)
	assert.Nil(t, err)
	message := NewPlatformMessage(nil, "", data)
	e := NewEvent(message, nil)

	u := &user{}
	d, err := e.Parse(u)
	assert.Nil(t, err)
	assert.Equal(t, u, &user{
		Name:     "Foo",
		Email:    "Bar",
		Password: "$uper$ecret",
	})
	assert.Equal(t, d.Type, "type")
	assert.Equal(t, d.SpecVersion, "v2")
	assert.Equal(t, d.Source, "ms.test")
	assert.Equal(t, d.Id, "id")
	assert.Equal(t, d.ContentType, "application/json")
}
