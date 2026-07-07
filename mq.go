package tyomq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// Well-known tyo-mq protocol constants.
const (
	DefaultPort = 17352

	// AllProducers subscribes to an event (or topic pattern) from any producer.
	AllProducers = "TYO-MQ-ALL"

	// EventAll is the suffix used when subscribing to all events of a producer.
	EventAll = "TM-ALL"
)

// ConsumeEventName returns the Socket.IO event name that tyo-mq emits when
// delivering a message from the given producer with the given event name.
//
//	scope="" (default) → "CONSUME-<lower(producer-event)>"
//	scope="all"        → "CONSUME-<lower(producer)>-TM-ALL"
func ConsumeEventName(producer, event, scope string) string {
	if scope == "all" {
		return "CONSUME-" + strings.ToLower(producer) + "-" + EventAll
	}
	return "CONSUME-" + strings.ToLower(producer+"-"+event)
}

// WaitFor registers a one-shot handler for event and returns a channel that
// receives its first payload. Useful for handshake-style events:
//
//	ok := c.WaitFor("AUTH_OK")
//	c.Emit("AUTHENTICATION", tyomq.AuthenticationReq{Token: token})
//	select { case <-ok: ... case <-ctx.Done(): ... }
func (c *Client) WaitFor(event string) <-chan json.RawMessage {
	ch := make(chan json.RawMessage, 1)
	var once sync.Once
	c.On(event, func(payload json.RawMessage) {
		once.Do(func() { ch <- payload })
	})
	return ch
}

// Authenticate sends the AUTHENTICATION message and waits for AUTH_OK or
// AUTH_FAIL. Call it right after Connect signals ready, before registering as
// a producer or consumer. Not needed when the server runs with auth disabled.
func (c *Client) Authenticate(ctx context.Context, token string) error {
	ok := c.WaitFor("AUTH_OK")
	fail := c.WaitFor("AUTH_FAIL")
	if err := c.Emit("AUTHENTICATION", AuthenticationReq{Token: token}); err != nil {
		return err
	}
	select {
	case <-ok:
		return nil
	case p := <-fail:
		return fmt.Errorf("authentication failed: %s", p)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RegisterProducer announces this connection as a producer named name.
func (c *Client) RegisterProducer(name string) error {
	return c.Emit("PRODUCER", RegisterProducer{Name: name})
}

// RegisterConsumer announces this connection as a consumer named name. The
// name doubles as the durable consumer identity: reconnect with the same name
// to replay queued messages of a durable subscription.
func (c *Client) RegisterConsumer(name string) error {
	return c.Emit("CONSUMER", RegisterConsumer{Name: name, ID: name, ConsumerID: name})
}

// Produce publishes one fire-and-forget message. For durable/broadcast/TTL
// options emit a full ProduceReq: c.Emit("PRODUCE", tyomq.ProduceReq{...}).
func (c *Client) Produce(from, event string, message interface{}) error {
	return c.Emit("PRODUCE", ProduceReq{Event: event, Message: message, From: from})
}

// Ack acknowledges one ACK-enabled delivery by its MsgID.
func (c *Client) Ack(msgID string) error {
	return c.Emit("ACK", AckReq{MsgID: msgID})
}

// Subscribe sends a SUBSCRIBE request and dispatches matching deliveries to
// handler. req.Consumer defaults to the name passed to RegisterConsumer being
// required — set it explicitly. For topic-pattern subscriptions set
// Mode: "topic"; req.Producer then defaults to AllProducers.
//
// When req.Ack is true and req.ManualAck is false, Subscribe acknowledges each
// delivery automatically after handler returns. With ManualAck, call
// c.Ack(msg.MsgID) yourself once the work has truly succeeded.
func (c *Client) Subscribe(req SubscribeReq, handler func(ConsumedMessage)) error {
	if req.Mode == "topic" && req.Producer == "" {
		req.Producer = AllProducers
	}
	if req.Producer == "" || req.Event == "" || req.Consumer == "" {
		return fmt.Errorf("subscribe: Producer, Event, and Consumer are required")
	}
	// Durable queues are keyed by consumer_id on the server; when it is
	// omitted the server falls back to the ephemeral socket id and replay
	// is lost across reconnects. Default it to the stable consumer name,
	// matching the Node/Python/Ruby clients.
	if req.ConsumerID == "" {
		req.ConsumerID = req.Consumer
	}

	consumeEvent := ConsumeEventName(req.Producer, req.Event, req.Scope)
	autoAck := req.Ack && !req.ManualAck

	c.On(consumeEvent, func(payload json.RawMessage) {
		var msg ConsumedMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			c.log.Warn("subscribe: bad delivery payload", "event", consumeEvent, "error", err)
			return
		}
		handler(msg)
		if autoAck && msg.MsgID != "" {
			if err := c.Ack(msg.MsgID); err != nil {
				c.log.Warn("subscribe: auto-ack failed", "msgId", msg.MsgID, "error", err)
			}
		}
	})

	return c.Emit("SUBSCRIBE", req)
}
