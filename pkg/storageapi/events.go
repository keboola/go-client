package storageapi

import (
	jsonLib "encoding/json"

	"github.com/keboola/go-client/pkg/client"
)

// EventID represents an ID of an event in Storage API.
type EventID string

func (v EventID) String() string {
	return string(v)
}

// Event https://keboola.docs.apiary.io/#reference/events/events/create-event
type Event struct {
	ID          EventID         `json:"id" readonly:"true"`
	ComponentID ComponentID     `json:"component"`
	Message     string          `json:"message"`
	Type        string          `json:"type"`
	Duration    DurationSeconds `json:"duration"`
	Params      JSONString      `json:"params"`
	Results     JSONString      `json:"results"`
}

// CreatEventRequest https://keboola.docs.apiary.io/#reference/events/events/create-event
func (a *Api) CreatEventRequest(event *Event) client.APIRequest[*Event] {
	request := a.
		newRequest(StorageAPI).
		WithResult(event).
		WithPost("events").
		WithFormBody(client.ToFormBody(client.StructToMap(event, nil)))
	return client.NewAPIRequest(event, request)
}

// JSONString is Json encoded as string, see CreatEventRequest.
type JSONString map[string]any

// UnmarshalJSON implements JSON decoding.
func (v *JSONString) UnmarshalJSON(data []byte) (err error) {
	out := make(map[string]any)
	err = jsonLib.Unmarshal(data, &out)
	*v = out
	return err
}

// MarshalJSON implements JSON encoding.
func (v *JSONString) MarshalJSON() ([]byte, error) {
	return jsonLib.Marshal(map[string]any(*v))
}

func (v JSONString) String() string {
	bytes, err := jsonLib.Marshal(map[string]any(v))
	if err != nil {
		panic(err)
	}
	return string(bytes)
}
