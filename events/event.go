package events

import (
	"github.com/roava/bifrost/idgenerator"
	"time"
)

type Event struct {
	SpecVersion string      `json:"spec_version"`
	Type        string      `json:"type"`
	Source      string      `json:"source"`
	Id          string      `json:"id"`
	Time        time.Time   `json:"time"`
	ContentType string      `json:"content_type"`
	Data        interface{} `json:"data"`
}

var (
	contentTypeJson = "application/json"
)

func New() *Event {
	return &Event{Id: idgenerator.New().Generate(), Time: time.Now(), ContentType: contentTypeJson}
}

func (e *Event) WithSpecVersion(specVersion string) *Event {
	e.SpecVersion = specVersion
	return e
}

func (e *Event) WithType(t string) *Event {
	e.Type = t
	return e
}

func (e *Event) WithSource(source string) *Event {
	e.Source = source
	return e
}

func (e *Event) WithId(id string) *Event {
	e.Id = id
	return e
}

func (e *Event) WithTime(eventTime time.Time) *Event {
	e.Time = eventTime
	return e
}

func (e *Event) WithContentType(contentType string) *Event {
	e.ContentType = contentType
	return e
}

func (e *Event) WithData(data interface{}) *Event {
	e.Data = data
	return e
}
