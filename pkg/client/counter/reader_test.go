package counter_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/client/counter"
)

func TestNewMeasuredReadCloser(t *testing.T) {
	t.Parallel()

	cases := []testCase{
		{
			name:    "empty",
			content: "",
		},
		{
			name:    "no error",
			content: "abcdef",
		},
		{
			name:             "close error",
			content:          "abcdef",
			closeErr:         errors.New("close error"),
			expectedCloseErr: "close error",
		},
		{
			name:            "read error",
			content:         "abcdef",
			readErr:         errors.New("read error"),
			expectedReadErr: "read error",
		},
		{
			name:             "read and close error",
			content:          "abcdef",
			readErr:          errors.New("read error"),
			closeErr:         errors.New("close error"),
			expectedReadErr:  "read error",
			expectedCloseErr: "close error",
		},
	}

	for _, tc := range cases {
		// Setup onClose callback
		onCloseCalled := false
		onCloseFn := func(bytes int64, err error) {
			onCloseCalled = true

			// All bytes must be read
			assert.Equal(t, (int64)(len(tc.content)), bytes, tc.name)

			// Check expected error, in the onClose callback, the readErr has priority over closeErr.
			if tc.expectedReadErr != "" {
				if assert.Error(t, err, tc.name) {
					assert.Equal(t, tc.expectedReadErr, err.Error(), tc.name)
				}
			} else if tc.expectedCloseErr != "" {
				if assert.Error(t, err, tc.name) {
					assert.Equal(t, tc.expectedCloseErr, err.Error(), tc.name)
				}
			} else {
				assert.NoError(t, err, tc.name)
			}
		}

		// Create measured reader
		r := counter.NewReadCloser(
			&testReader{content: strings.NewReader(tc.content), readErr: tc.readErr, closeErr: tc.closeErr},
			onCloseFn,
		)

		// Test Read
		outBytes, err := io.ReadAll(r)
		assert.Equal(t, tc.content, string(outBytes))
		assert.Equal(t, int64(len(tc.content)), r.Bytes())
		if tc.expectedReadErr == "" {
			assert.NoError(t, err, tc.name)
		} else {
			if assert.Error(t, err, tc.name) {
				assert.Equal(t, tc.expectedReadErr, err.Error(), tc.name)
			}
		}

		// Test Close
		err = r.Close()
		if tc.expectedCloseErr == "" {
			assert.NoError(t, err, tc.name)
		} else {
			if assert.Error(t, err, tc.name) {
				assert.Equal(t, tc.expectedCloseErr, err.Error(), tc.name)
			}
		}
		assert.True(t, onCloseCalled)
	}
}

type testCase struct {
	name             string
	content          string
	readErr          error
	closeErr         error
	expectedReadErr  string
	expectedCloseErr string
}

type testReader struct {
	content  io.Reader
	readErr  error
	closeErr error
}

func (r *testReader) Read(p []byte) (n int, err error) {
	n, err = r.content.Read(p)

	// Return read error, if any
	if err == nil {
		err = r.readErr
	}

	return n, err
}

func (r *testReader) Close() error {
	// Return close error, if any
	return r.closeErr
}
