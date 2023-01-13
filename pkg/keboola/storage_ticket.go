package keboola

import (
	"context"
	"sort"
	"sync"

	"github.com/keboola/go-client/pkg/client"
)

// Ticket https://keboola.docs.apiary.io/#reference/tickets/generate-unique-id/generate-new-id
type Ticket struct {
	ID string `json:"id"`
}

// GenerateIDRequest https://keboola.docs.apiary.io/#reference/tickets/generate-unique-id/generate-new-id
func (a *API) GenerateIDRequest() client.APIRequest[*Ticket] {
	result := &Ticket{}
	request := a.
		newRequest(StorageAPI).
		WithResult(result).
		WithPost("tickets")
	return client.NewAPIRequest(result, request)
}

// TicketProvider generates new IDs and GUARANTEES that the IDs will be returned with the same order as the Request method was called.
type TicketProvider struct {
	api       *API
	group     *client.RunGroup
	callbacks []func(ticket *Ticket)
	lock      *sync.Mutex
	tickets   []*Ticket
}

// NewTicketProvider creates TicketProvider.
func NewTicketProvider(ctx context.Context, api *API) *TicketProvider {
	return &TicketProvider{api: api, group: client.NewRunGroup(ctx, api.Client()), lock: &sync.Mutex{}}
}

// Request queues ID generation requests and registers callback.
func (t *TicketProvider) Request(onSuccess func(ticket *Ticket)) {
	t.callbacks = append(t.callbacks, onSuccess)
	t.group.Add(t.api.
		GenerateIDRequest().
		WithOnSuccess(func(ctx context.Context, ticket *Ticket) error {
			t.lock.Lock()
			defer t.lock.Unlock()
			t.tickets = append(t.tickets, ticket)
			return nil
		}),
	)
}

// Resolve sends all ID generation requests and then calls all callbacks.
func (t *TicketProvider) Resolve() error {
	// Get tickets
	if err := t.group.RunAndWait(); err != nil {
		return err
	}

	// Sort tickets
	sort.SliceStable(t.tickets, func(i, j int) bool {
		return t.tickets[i].ID < t.tickets[j].ID
	})

	// Invoke callbacks
	for index, ticket := range t.tickets {
		t.callbacks[index](ticket)
	}

	return nil
}
