package proxy

import (
	"golang.org/x/net/proxy"
	"net"
	"net/http"
	"net/url"
	"time"
)

func TransportFromURL(proxyURL string) (ret *http.Transport, err error) {
	// mostly copy of http.DefaultTransport
	ret = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if proxyURL == "" {
		return
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}
	switch parsed.Scheme {
	case "http", "https":
		ret.Proxy = http.ProxyURL(parsed)
	default:
		// DialContext in x/net/proxy is on review for now
		ret.DialContext = nil

		var dialer proxy.Dialer
		// correctly supports only socks5
		dialer, err = proxy.FromURL(parsed, &net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		})
		if err != nil {
			return
		}
		ret.Dial = dialer.Dial
	}
	return
}
