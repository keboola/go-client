package client

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

// DialTimeout specifies the default maximum connection initialization time.
const DialTimeout = 3 * time.Second

// KeepAlive specifies the default interval between keep-alive probes.
const KeepAlive = 10 * time.Second

// TLSHandshakeTimeout specifies the default timeout of TLS handshake.
const TLSHandshakeTimeout = 5 * time.Second

// ResponseHeaderTimeout specifies the default amount of time to wait for a server's response headers.
const ResponseHeaderTimeout = 20 * time.Second

// MaxIdleConnections specifies the default maximum number of open connections at all.
const MaxIdleConnections = 128

// MaxConnectionsPerHost specifies the default maximum number of open connections to a host.
const MaxConnectionsPerHost = 32

// HTTP2ReadIdleTimeout is the timeout after which a health check using ping frame will be carried out.
const HTTP2ReadIdleTimeout = 3 * time.Second

// HTTP2PingTimeout is the timeout after which the connection will be closed  if a response to Ping is not received.
const HTTP2PingTimeout = 3 * time.Second

// HTTP2WriteByteTimeout is the timeout after which the connection will be closed no data can be written to it.
const HTTP2WriteByteTimeout = 3 * time.Second

// DefaultTransport is the default Client transport with reasonable limits.
func DefaultTransport() http.RoundTripper {
	dialer := defaultDialer()
	dialTLS := func(ctx context.Context, network, addr string) (net.Conn, error) {
		tlsDialer := &tls.Dialer{NetDialer: dialer}
		return tlsDialer.DialContext(ctx, network, addr)
	}
	t1 := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		DialTLSContext:        dialTLS,
		ForceAttemptHTTP2:     true, // HTTP2 is preferred.
		TLSHandshakeTimeout:   TLSHandshakeTimeout,
		ResponseHeaderTimeout: ResponseHeaderTimeout,
		MaxIdleConns:          MaxIdleConnections,
		MaxConnsPerHost:       MaxConnectionsPerHost,
		MaxIdleConnsPerHost:   MaxConnectionsPerHost,
	}
	t2, err := http2.ConfigureTransports(t1)
	if err != nil {
		panic(err)
	}
	configureHTTP2(t2)
	return t1
}

// HTTP2Transport forces HTTP2 protocol.
func HTTP2Transport() http.RoundTripper {
	dialer := defaultDialer()
	dialTLS := func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
		tlsDialer := &tls.Dialer{NetDialer: dialer, Config: cfg}
		return tlsDialer.DialContext(ctx, network, addr)
	}
	t2 := &http2.Transport{DialTLSContext: dialTLS}
	configureHTTP2(t2)
	return t2
}

func configureHTTP2(t2 *http2.Transport) {
	t2.ReadIdleTimeout = HTTP2ReadIdleTimeout
	t2.PingTimeout = HTTP2PingTimeout
	t2.WriteByteTimeout = HTTP2WriteByteTimeout
}

func defaultDialer() *net.Dialer {
	return &net.Dialer{
		Timeout:   DialTimeout,
		KeepAlive: KeepAlive,
	}
}
