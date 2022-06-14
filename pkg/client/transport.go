package client

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

// DialTimeout specifies default maximum connection initialization time.
const DialTimeout = 3 * time.Second

// KeepAlive specifies default interval between keep-alive probes.
const KeepAlive = 10 * time.Second

// TLSHandshakeTimeout specifies default timeout of TLS handshake.
const TLSHandshakeTimeout = 5 * time.Second

// ResponseHeaderTimeout specifies default amount of time to wait for a server's response headers.
const ResponseHeaderTimeout = 20 * time.Second

// MaxConnectionsPerHost specifies default maximum number of open connections to a host.
const MaxConnectionsPerHost = 32

// DefaultTransport default transport with reasonable limits.
func DefaultTransport() http.RoundTripper {
	dialer := Dialer()
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true, // HTTP2 is preferred.
		TLSHandshakeTimeout:   TLSHandshakeTimeout,
		ResponseHeaderTimeout: ResponseHeaderTimeout,
		MaxConnsPerHost:       MaxConnectionsPerHost,
		MaxIdleConnsPerHost:   MaxConnectionsPerHost,
	}
}

// HTTP2Transport forces HTTP2 protocol.
func HTTP2Transport() http.RoundTripper {
	dialer := Dialer()
	return &http2.Transport{
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			return tls.DialWithDialer(dialer, network, addr, cfg)
		},
		ReadIdleTimeout:  3 * time.Second,
		PingTimeout:      3 * time.Second,
		WriteByteTimeout: 3 * time.Second,
	}
}

// Dialer - default dialer.
func Dialer() *net.Dialer {
	return &net.Dialer{
		Timeout:   DialTimeout,
		KeepAlive: KeepAlive,
	}
}
