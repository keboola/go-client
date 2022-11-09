package storageapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestClock struct {
	now time.Time
}

func (c *TestClock) Now() time.Time {
	return c.now
}

func (c *TestClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func TestJobBackoff(t *testing.T) {
	t.Parallel()
	clock := &TestClock{now: time.Now()}
	backoff := newJobBackoff()
	backoff.Clock = clock

	// Get all delays without sleep
	var delays []time.Duration
	for i := 0; i < 10; i++ {
		delay := backoff.NextBackOff()
		if delay == backoff.Stop {
			break
		}
		delays = append(delays, delay)
		clock.Advance(delay)
	}

	// Assert
	assert.Equal(t, []time.Duration{
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
		3000 * time.Millisecond,
	}, delays)
}
