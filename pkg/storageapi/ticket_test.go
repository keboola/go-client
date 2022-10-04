package storageapi_test

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/go-client/pkg/storageapi"
)

func TestGenerateNewId(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := clientForRandomProject(t)

	ticket, err := GenerateIDRequest().Send(ctx, c)
	assert.NoError(t, err)
	assert.NotNil(t, ticket)
	assert.NotEmpty(t, ticket.ID)
}

func TestTicketProvider(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, c := clientForRandomProject(t)

	provider := NewTicketProvider(ctx, c)
	tickets := make([]string, 0)

	// Request 3 tickets
	for i := 0; i < 3; i++ {
		provider.Request(func(ticket *Ticket) {
			tickets = append(tickets, ticket.ID)
		})
	}

	// Get tickets
	assert.NoError(t, provider.Resolve())

	// Assert order
	expected := make([]string, len(tickets))
	copy(expected, tickets)
	sort.Strings(expected)
	assert.Equal(t, expected, tickets)
}
