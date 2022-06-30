package storageapi_test

import (
	"context"
	"testing"
	"time"

	. "github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"
)

func TestSendEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := clientForRandomProject(t)
	event, err := CreatEventRequest(&Event{
		ComponentID: "keboola.keboola-as-code",
		Type:        "info",
		Message:     "Test event",
		Params:      map[string]any{"command": "bar1"},
		Results:     map[string]any{"projectId": 123, "error": "err"},
		Duration:    DurationSeconds(123456 * time.Millisecond),
	}).Send(ctx, c)
	assert.NoError(t, err)
	assert.NotEmpty(t, event.ID)
}
