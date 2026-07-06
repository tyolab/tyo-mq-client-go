package tyomq

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// CreateAdminProof creates an HMAC-SHA256 signed proof for a tyo-mq manager action.
// Matches the JS adminSignature.createAdminProof implementation exactly.
func CreateAdminProof(adminToken, action string, body interface{}) AdminProof {
	timestamp := time.Now().UnixMilli()
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	nonce := hex.EncodeToString(b)
	return AdminProof{
		Timestamp: timestamp,
		Nonce:     nonce,
		Signature: adminSign(adminToken, action, body, timestamp, nonce),
	}
}

func adminSign(adminToken, action string, body interface{}, timestamp int64, nonce string) string {
	// Must match JS: [action, timestamp, nonce, stableStringify(body)].join("\n")
	base := strings.Join([]string{
		action,
		fmt.Sprintf("%d", timestamp),
		nonce,
		adminStableStringify(body),
	}, "\n")
	h := hmac.New(sha256.New, []byte(adminToken))
	h.Write([]byte(base))
	return hex.EncodeToString(h.Sum(nil))
}

// adminStableStringify produces canonical JSON with sorted keys,
// matching the tyo-mq server's stableStringify JS function.
// Go's json.Marshal sorts map keys alphabetically, so this works for map inputs.
func adminStableStringify(v interface{}) string {
	data, _ := json.Marshal(v)
	return string(data)
}

// NewAuthNextReq builds the signed payload for AUTHORIZATION_NEXT.
// Pass realmFilter="" to get the next request across all realms.
func NewAuthNextReq(adminToken, realmFilter string) map[string]interface{} {
	body := map[string]interface{}{}
	if realmFilter != "" {
		body["realm"] = realmFilter
	}
	return map[string]interface{}{
		"body":  body,
		"proof": CreateAdminProof(adminToken, "AUTHORIZATION_NEXT", body),
	}
}

// NewAuthDecideReq builds the signed payload for AUTHORIZATION_DECIDE.
func NewAuthDecideReq(adminToken, requestID string, approved bool, role, reason string) map[string]interface{} {
	body := map[string]interface{}{
		"approved":   approved,
		"request_id": requestID,
	}
	if role != "" {
		body["role"] = role
	}
	if reason != "" {
		body["reason"] = reason
	}
	return map[string]interface{}{
		"body":  body,
		"proof": CreateAdminProof(adminToken, "AUTHORIZATION_DECIDE", body),
	}
}
