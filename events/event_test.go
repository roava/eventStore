package events

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	type S struct {
		Value string `json:"value"`
		Key   string `json:"key"`
	}
	e := New().
		WithContentType("application/json").
		WithId("id").
		WithSource("ms.person").
		WithSpecVersion("v2").
		WithType("type").
		WithTime(time.Now()).
		WithData(&S{Key: "key", Value: "value"})

	assert.Equal(t, e.ContentType, "application/json")
	assert.Equal(t, e.Id, "id")
	assert.Equal(t, e.Source, "ms.person")
	assert.Equal(t, e.SpecVersion, "v2")
	assert.Equal(t, e.Type, "type")
	assert.Equal(t, e.Data, &S{Key: "key", Value: "value"})
}
