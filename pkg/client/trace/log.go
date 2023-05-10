package trace

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/keboola/go-client/pkg/request"
)

type logTrace struct {
	ClientTrace
	wr io.Writer
}

func LogTracer(wr io.Writer) Factory {
	var idGenerator uint64
	return func(ctx context.Context, reqDef request.HTTPRequest) (context.Context, *ClientTrace) {
		requestID := atomic.AddUint64(&idGenerator, 1)

		var req *http.Request
		var connStartTime time.Time
		var startTime time.Time
		var doneTime time.Time
		var statusCode int

		t := &logTrace{wr: wr}
		t.ConnectStart = func(network, addr string) {
			connStartTime = time.Now()
		}
		t.GotConn = func(info httptrace.GotConnInfo) {
			var infoStr string
			if info.Reused {
				if info.WasIdle {
					infoStr = "reused conn"
				} else {
					infoStr = fmt.Sprintf("reused conn (was idle=%s)", info.IdleTime)
				}
			} else {
				infoStr = fmt.Sprintf("new conn | %s", time.Since(connStartTime))
			}
			t.log(requestID, fmt.Sprintf(`CONN  %s "%s" | %s`, req.Method, req.URL.String(), infoStr))
		}
		t.HTTPRequestStart = func(r *http.Request) {
			req = r
			startTime = time.Now()
			t.log(requestID, fmt.Sprintf(`START %s "%s"`, req.Method, req.URL.String()))
		}
		t.HTTPRequestDone = func(r *http.Response, err error) {
			doneTime = time.Now()
			var errorStr string
			if err == nil {
				statusCode = r.StatusCode
			} else {
				errorStr = fmt.Sprintf(" | error=%s", err)
			}
			t.log(requestID, fmt.Sprintf(`DONE  %s "%s" | %d | %s%s`, req.Method, req.URL.String(), statusCode, doneTime.Sub(startTime).String(), errorStr))
		}
		t.HTTPRequestRetry = func(attempt int, delay time.Duration) {
			t.log(requestID, fmt.Sprintf(`RETRY %s "%s" | %dx | %s`, req.Method, req.URL.String(), attempt, delay))
		}
		t.RequestProcessed = func(result any, err error) {
			var errorStr string
			if err != nil {
				errorStr = fmt.Sprintf(" | error=%s", err)
			}
			t.log(requestID, fmt.Sprintf(`BODY  %s "%s" | %s%s`, req.Method, req.URL.String(), time.Since(doneTime).String(), errorStr))
		}
		return ctx, &t.ClientTrace
	}
}

func (t *logTrace) log(requestID uint64, a ...any) {
	a = append([]any{fmt.Sprintf("HTTP_REQUEST[%04d]", requestID)}, a...)
	_, _ = fmt.Fprintln(t.wr, a...)
}
