package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Handler interface {
	Execute(context.Context, json.RawMessage) (json.RawMessage, error)
}
type HandlerFunc func(context.Context, json.RawMessage) (json.RawMessage, error)

func (f HandlerFunc) Execute(ctx context.Context, p json.RawMessage) (json.RawMessage, error) {
	return f(ctx, p)
}

// Registry is built before worker startup and immutable while workers execute.
type Registry struct{ handlers map[string]Handler }

func NewRegistry() *Registry { return &Registry{handlers: map[string]Handler{}} }
func (r *Registry) Register(name string, h Handler) error {
	if name == "" || h == nil {
		return errors.New("invalid handler")
	}
	if _, ok := r.handlers[name]; ok {
		return errors.New("duplicate handler")
	}
	r.handlers[name] = h
	return nil
}
func (r *Registry) Execute(ctx context.Context, name string, p json.RawMessage) (out json.RawMessage, e error) {
	h, ok := r.handlers[name]
	if !ok {
		return nil, fmt.Errorf("unknown handler type: %s", name)
	}
	defer func() {
		if v := recover(); v != nil {
			out = nil
			e = fmt.Errorf("handler panic: %v", v)
		}
	}()
	return h.Execute(ctx, p)
}
func Sleep(ctx context.Context, p json.RawMessage) (json.RawMessage, error) {
	var v struct {
		DurationMS int `json:"durationMs"`
	}
	if e := json.Unmarshal(p, &v); e != nil {
		return nil, e
	}
	if v.DurationMS < 0 || v.DurationMS > 86400000 {
		return nil, errors.New("durationMs out of range")
	}
	t := time.NewTimer(time.Duration(v.DurationMS) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.C:
		return json.RawMessage(`{"slept":true}`), nil
	}
}
func Unstable(random func() float64) Handler {
	return HandlerFunc(func(ctx context.Context, p json.RawMessage) (json.RawMessage, error) {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		var v struct {
			Probability float64 `json:"failureProbability"`
		}
		if e := json.Unmarshal(p, &v); e != nil {
			return nil, e
		}
		if v.Probability < 0 || v.Probability > 1 {
			return nil, errors.New("failureProbability out of range")
		}
		if random() < v.Probability {
			return nil, errors.New("simulated transient failure")
		}
		return json.RawMessage(`{"ok":true}`), nil
	})
}

// Dial the validated IP, not the hostname: DNS rebinding cannot bypass this check.
func SafeHTTP(allowlist []string) Handler {
	allowed := map[string]bool{}
	for _, host := range allowlist {
		allowed[strings.ToLower(host)] = true
	}
	transport := &http.Transport{Proxy: nil, ResponseHeaderTimeout: 5 * time.Second, TLSHandshakeTimeout: 5 * time.Second, IdleConnTimeout: 30 * time.Second, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, e := net.SplitHostPort(address)
		if e != nil {
			return nil, e
		}
		if !allowed[strings.ToLower(host)] {
			return nil, errors.New("HTTP host not allowed")
		}
		ips, e := net.DefaultResolver.LookupIPAddr(ctx, host)
		if e != nil {
			return nil, e
		}
		if len(ips) == 0 {
			return nil, errors.New("no destination IP")
		}
		for _, ip := range ips {
			if !publicIP(ip.IP) {
				return nil, errors.New("non-public destination IP")
			}
		}
		var dial net.Dialer
		dial.Timeout = 5 * time.Second
		return dial.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	return HandlerFunc(func(ctx context.Context, p json.RawMessage) (json.RawMessage, error) {
		var v struct{ Method, URL string }
		if e := json.Unmarshal(p, &v); e != nil {
			return nil, e
		}
		u, e := url.Parse(v.URL)
		if e != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") {
			return nil, errors.New("invalid HTTP URL")
		}
		if !allowed[strings.ToLower(u.Hostname())] {
			return nil, errors.New("HTTP host not allowed")
		}
		port := u.Port()
		if port != "" && port != "80" && port != "443" {
			return nil, errors.New("HTTP port not allowed")
		}
		if v.Method == "" {
			v.Method = "GET"
		}
		if v.Method != "GET" && v.Method != "HEAD" {
			return nil, errors.New("only GET/HEAD allowed")
		}
		req, e := http.NewRequestWithContext(ctx, v.Method, u.String(), nil)
		if e != nil {
			return nil, e
		}
		res, e := client.Do(req)
		if e != nil {
			return nil, e
		}
		defer res.Body.Close()
		body, e := io.ReadAll(io.LimitReader(res.Body, (1<<16)+1))
		if e != nil {
			return nil, e
		}
		if len(body) > 1<<16 {
			return nil, errors.New("HTTP response exceeds 64 KiB")
		}
		if res.StatusCode >= 500 {
			return nil, fmt.Errorf("HTTP status %d", res.StatusCode)
		}
		return json.Marshal(map[string]any{"status": res.StatusCode, "bytes": len(body)})
	})
}
func publicIP(ip net.IP) bool {
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "2001:db8::/32"} {
		_, n, _ := net.ParseCIDR(cidr)
		if n.Contains(ip) {
			return false
		}
	}
	return true
}
func DemoRegistry(allowlist []string) *Registry {
	r := NewRegistry()
	_ = r.Register("sleep", HandlerFunc(Sleep))
	_ = r.Register("unstable", Unstable(rand.Float64))
	_ = r.Register("http_request", SafeHTTP(allowlist))
	return r
}
