package client

import (
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// RetriesCount - default retries count.
const RetriesCount = 5

// RequestTimeout - default request timeout.
const RequestTimeout = 30 * time.Second

// RetryWaitTimeStart - default retry interval.
const RetryWaitTimeStart = 100 * time.Millisecond

// RetryWaitTimeMax - default maximum retry interval.
const RetryWaitTimeMax = 3 * time.Second

// RetryConfig configures Client retries.
type RetryConfig struct {
	Condition           RetryCondition
	Count               int
	TotalRequestTimeout time.Duration
	WaitTimeStart       time.Duration
	WaitTimeMax         time.Duration
}

// RetryCondition defines which responses should retry.
type RetryCondition func(*http.Response, error) bool

// TestingRetry - fast retry for use in tests.
func TestingRetry() RetryConfig {
	v := DefaultRetry()
	v.WaitTimeStart = 1 * time.Millisecond
	v.WaitTimeMax = 1 * time.Millisecond
	return v
}

// DefaultRetry returns a default RetryConfig.
func DefaultRetry() RetryConfig {
	return RetryConfig{
		TotalRequestTimeout: RequestTimeout,
		Count:               RetriesCount,
		WaitTimeStart:       RetryWaitTimeStart,
		WaitTimeMax:         RetryWaitTimeMax,
		Condition:           DefaultRetryCondition(),
	}
}

// DefaultRetryCondition retries on common network and HTTP errors.
func DefaultRetryCondition() RetryCondition {
	return func(response *http.Response, err error) bool {
		// On network errors - except hostname not found
		if response == nil || response.StatusCode == 0 {
			switch {
			case strings.Contains(err.Error(), "No address associated with hostname"):
				return false
			case strings.Contains(err.Error(), "no such host"):
				return false
			default:
				return true
			}
		}

		// On HTTP status codes
		switch response.StatusCode {
		case
			http.StatusRequestTimeout,
			http.StatusConflict,
			http.StatusLocked,
			http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
}

// NewBackoff returns an exponential backoff for HTTP retries.
func (c RetryConfig) NewBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = c.WaitTimeStart
	b.MaxInterval = c.WaitTimeMax
	b.MaxElapsedTime = c.TotalRequestTimeout
	b.Multiplier = 2
	b.RandomizationFactor = 0
	b.Reset()
	return b
}
