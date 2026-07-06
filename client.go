// Package tyomq provides a Socket.IO v4 client for the tyo-mq message broker.
//
// Protocol: Engine.IO v4 over WebSocket, then Socket.IO v4 events.
// Wire format: `42["event_name",{...payload...}]`
package tyomq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Handler is called when a matching Socket.IO event is received.
type Handler func(payload json.RawMessage)

// Client is a Socket.IO v4 client for tyo-mq.
//
// Usage:
//
//	c := tyomq.NewClient("http://localhost:17352", logger)
//	c.On("AUTH_OK", func(p json.RawMessage) { ... })
//	ready := make(chan struct{})
//	go c.Connect(ctx, ready)
//	<-ready  // wait for namespace "/" to be joined
//	c.Emit("AUTHENTICATION", tyomq.AuthenticationReq{Token: "..."})
type Client struct {
	serverURL string
	log       *slog.Logger

	handlersMu sync.RWMutex
	handlers   map[string][]Handler

	// namespace is the Socket.IO namespace this client joins (default "/").
	namespace string

	// mu protects conn, pingIntvl, pingTO.
	mu        sync.Mutex
	conn      *websocket.Conn
	sendMu    sync.Mutex // serialises writes on the current conn
	pingIntvl time.Duration
	pingTO    time.Duration
}

// NewClient returns an unconnected client for the default namespace "/".
func NewClient(serverURL string, log *slog.Logger) *Client {
	return NewClientNS(serverURL, "/", log)
}

// NewClientNS returns an unconnected client for a specific Socket.IO namespace
// (e.g. "/remote"). Call Connect to establish the connection.
func NewClientNS(serverURL, namespace string, log *slog.Logger) *Client {
	if log == nil {
		log = slog.Default()
	}
	if namespace == "" {
		namespace = "/"
	}
	return &Client{
		serverURL: serverURL,
		namespace: namespace,
		log:       log,
		handlers:  make(map[string][]Handler),
	}
}

// On registers a handler for the given Socket.IO event name.
// Safe to call concurrently. Multiple handlers for one event run in order.
func (c *Client) On(event string, h Handler) {
	c.handlersMu.Lock()
	defer c.handlersMu.Unlock()
	c.handlers[event] = append(c.handlers[event], h)
}

// Connect dials the server, performs the Engine.IO + Socket.IO handshake, and
// runs the read loop until ctx is cancelled or the connection drops.
//
// ready is closed exactly once when namespace "/" is joined and the client is
// ready to Emit. The caller must create ready before starting this goroutine:
//
//	ready := make(chan struct{})
//	go client.Connect(ctx, ready)
//	<-ready
func (c *Client) Connect(ctx context.Context, ready chan<- struct{}) error {
	wsURL, err := buildWsURL(c.serverURL)
	if err != nil {
		return fmt.Errorf("build URL: %w", err)
	}
	c.log.Info("dialing tyo-mq", "url", wsURL)

	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	defer func() {
		conn.Close()
		c.mu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.mu.Unlock()
	}()

	// Step 1 — Engine.IO OPEN handshake.
	if err := c.readHandshake(conn); err != nil {
		return fmt.Errorf("EIO handshake: %w", err)
	}

	// Step 2 — Socket.IO CONNECT to the client's namespace.
	if err := c.writeText(conn, connectFrame(c.namespace)); err != nil {
		return fmt.Errorf("SIO connect send: %w", err)
	}

	// Step 3 — Read Socket.IO CONNECTED acknowledgment.
	if err := c.readSIOConnected(conn); err != nil {
		return fmt.Errorf("SIO connect ack: %w", err)
	}

	c.log.Info("tyo-mq connected", "server", c.serverURL)
	close(ready) // signal: namespace joined, caller may now Emit

	// Step 4 — Read loop.
	return c.readLoop(ctx, conn)
}

// Emit sends a Socket.IO event to the server. Returns an error if the client
// is not currently connected (caller should wait for Connected() first, or retry).
func (c *Client) Emit(event string, payload interface{}) error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("not connected")
	}

	name, err := json.Marshal(event)
	if err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	frame := emitPrefix(c.namespace) + "[" + string(name) + "," + string(body) + "]"
	c.log.Info("→ emit", "event", event)
	return c.writeText(conn, frame)
}

// Close sends a WebSocket close frame and tears down the connection.
func (c *Client) Close() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}
}

// ─── Internal ──────────────────────────────────────────────────────────────

func (c *Client) readHandshake(conn *websocket.Conn) error {
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	// Engine.IO OPEN: `0{...json...}`
	if len(msg) == 0 || msg[0] != '0' {
		return fmt.Errorf("expected EIO OPEN (0{…}), got %q", truncate(msg, 64))
	}
	var hs struct {
		PingInterval int `json:"pingInterval"` // ms
		PingTimeout  int `json:"pingTimeout"`  // ms
	}
	if err := json.Unmarshal(msg[1:], &hs); err != nil {
		return fmt.Errorf("parse EIO handshake: %w", err)
	}
	pi := time.Duration(hs.PingInterval) * time.Millisecond
	pt := time.Duration(hs.PingTimeout) * time.Millisecond
	if pi == 0 {
		pi = 25 * time.Second
	}
	if pt == 0 {
		pt = 20 * time.Second
	}
	c.mu.Lock()
	c.pingIntvl = pi
	c.pingTO = pt
	c.mu.Unlock()
	c.log.Info("EIO handshake ok", "pingInterval", pi, "pingTimeout", pt)
	return nil
}

func (c *Client) readSIOConnected(conn *websocket.Conn) error {
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		return err
	}
	// Expect `40` or `40{...}` (EIO MESSAGE + SIO CONNECT)
	if len(msg) < 2 || msg[0] != '4' || msg[1] != '0' {
		return fmt.Errorf("expected SIO CONNECT response (40…), got %q", truncate(msg, 64))
	}
	return nil
}

func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn) error {
	c.mu.Lock()
	pi, pt := c.pingIntvl, c.pingTO
	c.mu.Unlock()
	// Give the server time for one full ping/pong cycle plus a margin.
	readTimeout := pi + pt + 5*time.Second

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if err := c.handleFrame(conn, msg); err != nil {
			c.log.Warn("frame error", "error", err)
		}
	}
}

func (c *Client) handleFrame(conn *websocket.Conn, msg []byte) error {
	if len(msg) == 0 {
		return nil
	}
	switch msg[0] {
	case '2': // EIO PING — server-initiated; reply with PONG
		c.log.Debug("← PING, sending PONG")
		return c.writeText(conn, "3")
	case '1': // EIO CLOSE
		return fmt.Errorf("server sent EIO CLOSE")
	case '4': // EIO MESSAGE → Socket.IO packet
		return c.handleSIO(conn, msg[1:])
	}
	return nil
}

func (c *Client) handleSIO(_ *websocket.Conn, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	switch data[0] {
	case '1': // SIO DISCONNECT
		return fmt.Errorf("server disconnected namespace")
	case '4': // SIO CONNECT_ERROR
		return fmt.Errorf("SIO connect error: %s", data[1:])
	case '2': // SIO EVENT (possibly namespaced: "2/remote,[...]")
		ns, payload := stripNamespace(string(data[1:]))
		if ns != c.namespace {
			return nil // event for a different namespace — ignore
		}
		return c.dispatch([]byte(payload))
	case '3': // SIO ACK — not used by tyo-mq protocol for our events
		return nil
	}
	return nil
}

// dispatch parses `["event_name", payload]` and calls registered handlers.
func (c *Client) dispatch(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("dispatch parse: %w", err)
	}
	if len(raw) == 0 {
		return fmt.Errorf("dispatch: empty event array")
	}
	var name string
	if err := json.Unmarshal(raw[0], &name); err != nil {
		return fmt.Errorf("dispatch: event name: %w", err)
	}
	var payload json.RawMessage = []byte("null")
	if len(raw) > 1 {
		payload = raw[1]
	}

	c.handlersMu.RLock()
	hs := append([]Handler(nil), c.handlers[name]...)
	c.handlersMu.RUnlock()

	if len(hs) == 0 {
		c.log.Info("← event (no handler)", "name", name, "payload", truncate(payload, 120))
	} else {
		c.log.Info("← event", "name", name)
	}

	for _, h := range hs {
		h(payload)
	}
	return nil
}

// ─── Namespace framing helpers ─────────────────────────────────────────────────

// connectFrame returns the Socket.IO CONNECT packet for a namespace.
// "/" → "40"; "/remote" → "40/remote,".
func connectFrame(ns string) string {
	if ns == "" || ns == "/" {
		return "40"
	}
	return "40" + ns + ","
}

// emitPrefix returns the Socket.IO EVENT packet prefix for a namespace.
// "/" → "42"; "/remote" → "42/remote,".
func emitPrefix(ns string) string {
	if ns == "" || ns == "/" {
		return "42"
	}
	return "42" + ns + ","
}

// splitNamespace separates a full "42<ns>,<json>" frame into (namespace, json).
func splitNamespace(frame string) (ns, payload string) {
	return stripNamespace(frame[2:])
}

// stripNamespace separates a Socket.IO EVENT body (the part after the "2" type
// byte) into (namespace, json). A leading "/" up to the first comma is the
// namespace; otherwise the default namespace "/" is assumed.
func stripNamespace(body string) (ns, payload string) {
	if len(body) > 0 && body[0] == '/' {
		if comma := indexByte(body, ','); comma >= 0 {
			return body[:comma], body[comma+1:]
		}
	}
	return "/", body
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func (c *Client) writeText(conn *websocket.Conn, msg string) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return conn.WriteMessage(websocket.TextMessage, []byte(msg))
}

func buildWsURL(serverURL string) (string, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
		// already correct
	default:
		u.Scheme = "ws"
	}
	u.Path = "/socket.io/"
	q := u.Query()
	q.Set("EIO", "4")
	q.Set("transport", "websocket")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
