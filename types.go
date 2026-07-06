package tyomq

import "encoding/json"

// ─── Authentication ───────────────────────────────────────────────────────────

// AuthenticationReq is emitted as "AUTHENTICATION" when the server has auth
// enabled. The server answers with "AUTH_OK" or "AUTH_FAIL".
type AuthenticationReq struct {
	Token string `json:"token"`
}

type AuthOK struct {
	Realm string `json:"realm"`
	Role  string `json:"role"`
}

type AuthFail struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ─── Authorization request flow ───────────────────────────────────────────────

// AuthorizationRequest is emitted as "AUTHORIZATION_REQUEST" by a new client
// asking to be admitted to a realm. The client generates its own ClientToken;
// once an operator approves the request, that token authenticates normally.
type AuthorizationRequest struct {
	Realm             string      `json:"realm"`
	Role              string      `json:"role"`
	ClientID          string      `json:"client_id"`
	ClientName        string      `json:"client_name"`
	ClientToken       string      `json:"client_token"`
	ChallengeResponse interface{} `json:"challenge_response,omitempty"`
}

type AuthorizationRequestOK struct {
	OK        bool   `json:"ok"`
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
}

type AuthorizationApproved struct {
	RequestID string `json:"request_id"`
	Realm     string `json:"realm"`
	Role      string `json:"role"`
}

type AuthorizationRejected struct {
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"`
}

// PendingAuthRequest is a pending authorization request returned by
// "AUTHORIZATION_NEXT_RESULT".
type PendingAuthRequest struct {
	RequestID      string `json:"request_id"`
	Status         string `json:"status"`
	Realm          string `json:"realm"`
	Role           string `json:"role"`
	ClientID       string `json:"client_id"`
	ClientName     string `json:"client_name"`
	CreatedAt      string `json:"created_at"`
	DecisionReason string `json:"decision_reason,omitempty"`
}

// AuthorizationNextResult is the server response to "AUTHORIZATION_NEXT".
type AuthorizationNextResult struct {
	OK      bool                `json:"ok"`
	Request *PendingAuthRequest `json:"request"` // nil when the queue is empty
}

// AuthorizationDecideResult is the server response to "AUTHORIZATION_DECIDE".
type AuthorizationDecideResult struct {
	OK      bool                `json:"ok"`
	Request *PendingAuthRequest `json:"request"`
}

// AdminProof is the HMAC-SHA256 signed proof required for manager actions.
// Build one with CreateAdminProof.
type AdminProof struct {
	Timestamp int64  `json:"timestamp"`
	Nonce     string `json:"nonce"`
	Signature string `json:"signature"`
}

// ─── Producer / Consumer registration ─────────────────────────────────────────

// RegisterProducer is emitted as "PRODUCER" after connecting.
type RegisterProducer struct {
	Name string `json:"name"`
}

// RegisterConsumer is emitted as "CONSUMER" after connecting.
type RegisterConsumer struct {
	Name       string `json:"name"`
	ID         string `json:"id,omitempty"`
	ConsumerID string `json:"consumer_id,omitempty"`
}

// ─── Subscribe ────────────────────────────────────────────────────────────────

// RetryPolicy configures server-side re-delivery for ACK-enabled durable
// subscriptions. Delay accepts duration strings like "5s", "200ms".
type RetryPolicy struct {
	MaxAttempts int    `json:"max_attempts,omitempty"`
	Delay       string `json:"delay,omitempty"`
	Backoff     string `json:"backoff,omitempty"` // "" | "exponential"
}

// SubscribeReq is emitted as "SUBSCRIBE".
//
// The zero value of the optional fields gives plain fire-and-forget delivery.
// Durable + Ack turn on guaranteed delivery: the server queues messages while
// the consumer is offline, waits for "ACK" per message, retries on the
// RetryPolicy schedule, and dead-letters messages that exhaust their attempts.
// Mode "topic" treats Event as an MQTT-style pattern ("orders/+/status",
// "factory/#") matched against events from any producer. Group makes the
// subscription part of a consumer group that load-balances deliveries.
type SubscribeReq struct {
	Event      string       `json:"event"`
	Producer   string       `json:"producer"`
	Consumer   string       `json:"consumer"`
	Scope      string       `json:"scope,omitempty"` // "all" or "" (default)
	Durable    bool         `json:"durable,omitempty"`
	Ack        bool         `json:"ack,omitempty"`
	ManualAck  bool         `json:"manual_ack,omitempty"`
	AckTimeout string       `json:"ack_timeout,omitempty"` // e.g. "30s"
	Retry      *RetryPolicy `json:"retry,omitempty"`
	Mode       string       `json:"mode,omitempty"` // "" | "topic"
	Group      string       `json:"group,omitempty"`
	ConsumerID string       `json:"consumer_id,omitempty"`
}

// AckReq is emitted as "ACK" to acknowledge one delivered message.
type AckReq struct {
	MsgID string `json:"msgId"`
}

// ─── Publish ──────────────────────────────────────────────────────────────────

// ProduceReq is emitted as "PRODUCE".
//
// Broadcast "realm" delivers one copy to every connected member of the
// producer's realm; "group" (with Group set) delivers one copy to every member
// of that consumer group. TTL bounds how long a durable copy may wait.
type ProduceReq struct {
	Event      string      `json:"event"`
	Message    interface{} `json:"message"`
	From       string      `json:"from"`
	Guaranteed bool        `json:"guaranteed,omitempty"` // persist until consumed
	TTL        interface{} `json:"ttl,omitempty"`        // e.g. "1h" or ms
	Method     string      `json:"method,omitempty"`     // "broadcast" when broadcasting
	Broadcast  string      `json:"broadcast,omitempty"`  // "realm" | "group"
	Group      string      `json:"group,omitempty"`
}

// ─── Incoming consumed message ────────────────────────────────────────────────

// ConsumedMessage is the payload of a "CONSUME-…" event. MsgID is present only
// on ACK-enabled deliveries; send AckReq with it (or use Subscribe, which can
// do so automatically).
type ConsumedMessage struct {
	Event   string          `json:"event"`
	Message json.RawMessage `json:"message"`
	From    string          `json:"from"`
	MsgID   string          `json:"msgId"`
}

// ─── Server error ─────────────────────────────────────────────────────────────

type ServerError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
