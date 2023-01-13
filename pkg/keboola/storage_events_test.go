package keboola_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client"
	. "github.com/keboola/go-client/pkg/keboola"
)

func TestSendEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, api := APIClientForRandomProject(t)
	event, err := api.CreateEventRequest(&Event{
		ComponentID: "keboola.keboola-as-code",
		Type:        "info",
		Message:     "Test event",
		Params:      map[string]any{"command": "bar1"},
		Results:     map[string]any{"projectId": 123, "error": "err"},
		Duration:    client.DurationSeconds(123456 * time.Millisecond),
	}).Send(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, event.ID)
}
