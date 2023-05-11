package keboola

import (
	jsonLib "encoding/json"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
)

// EventID represents an ID of an event in Storage API.
type EventID string

func (v EventID) String() string {
	return string(v)
}

// Event https://keboola.docs.apiary.io/#reference/events/events/create-event
type Event struct {
	ID          EventID                `json:"id" readonly:"true"`
	ComponentID ComponentID            `json:"component"`
	Message     string                 `json:"message"`
	Type        string                 `json:"type"`
	Duration    client.DurationSeconds `json:"duration"`
	Params      JSONString             `json:"params"`
	Results     JSONString             `json:"results"`
}

// CreateEventRequest https://keboola.docs.apiary.io/#reference/events/events/create-event
func (a *API) CreateEventRequest(event *Event) request.APIRequest[*Event] {
	// Params and results must be a JSON value encoded as string
	body := request.StructToMap(event, nil)
	pValue, err := jsonLib.Marshal(event.Params)
	if err != nil {
		return request.NewAPIRequest(event, request.NewReqDefinitionError(err))
	}
	rValue, err := jsonLib.Marshal(event.Results)
	if err != nil {
		return request.NewAPIRequest(event, request.NewReqDefinitionError(err))
	}
	body["params"] = string(pValue)
	body["results"] = string(rValue)
	req := a.
		newRequest(StorageAPI).
		WithResult(event).
		WithPost("events").
		WithJSONBody(body)
	return request.NewAPIRequest(event, req)
}

// JSONString is Json encoded as string, see CreateEventRequest.
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
