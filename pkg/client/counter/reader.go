package counter

import (
	"errors"
	"io"
)

// ReadCloser wraps an io.ReadCloser (request/response body) to count bytes read the reader.
// Optionally, an OnClose callback can be registered.
type ReadCloser struct {
	wrapped io.ReadCloser
	onClose OnClose
	bytes   int64
	readErr error
}

type OnClose func(bytes int64, err error)

func NewReadCloser(wrapped io.ReadCloser, onClose OnClose) *ReadCloser {
	return &ReadCloser{wrapped: wrapped, onClose: onClose}
}

func (w *ReadCloser) Bytes() int64 {
	return w.bytes
}

func (w *ReadCloser) Read(b []byte) (int, error) {
	n, err := w.wrapped.Read(b)
	w.bytes += int64(n)
	w.readErr = err
	return n, err
}

func (w *ReadCloser) Close() error {
	closeErr := w.wrapped.Close()
	if w.onClose != nil {
		// Prefer read error before close error for onClose callback, it is usually more useful
		var onCloseErr error
		if !errors.Is(w.readErr, io.EOF) {
			onCloseErr = w.readErr
		} else if closeErr != nil {
			onCloseErr = closeErr
		}
		w.onClose(w.bytes, onCloseErr)
	}
	return closeErr
}
