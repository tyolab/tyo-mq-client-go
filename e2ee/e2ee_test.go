package e2ee

import (
	"bytes"
	"crypto/ecdh"
	"encoding/base64"
	"encoding/json"
	"os"
	"testing"
)

// vectors mirrors tests/e2ee-vectors.json in the tyo-mq repo — the
// cross-language conformance contract.
type vectors struct {
	Suite               string `json:"suite"`
	RecipientPrivateB64 string `json:"recipient_private_b64"`
	RecipientPublicB64  string `json:"recipient_public_b64"`
	Routing             struct {
		Event string `json:"event"`
		To    string `json:"to"`
		From  string `json:"from"`
		Kid   string `json:"kid"`
	} `json:"routing"`
	PlaintextUTF8 string `json:"plaintext_utf8"`
	Enc           Enc    `json:"enc"`
	MessageB64    string `json:"message_b64"`
}

func loadVectors(t *testing.T) *vectors {
	t.Helper()
	raw, err := os.ReadFile("testdata/e2ee-vectors.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var v vectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	if v.Suite != ALG {
		t.Fatalf("vector suite %q != %q", v.Suite, ALG)
	}
	return &v
}

func b64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("bad base64 in vectors: %v", err)
	}
	return b
}

// The committed ciphertext must open to the committed plaintext.
func TestOpenCommittedVector(t *testing.T) {
	v := loadVectors(t)
	pt, err := Open(b64(t, v.RecipientPrivateB64), &v.Enc,
		v.Routing.Event, v.Routing.To, v.Routing.From, v.MessageB64)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(pt) != v.PlaintextUTF8 {
		t.Fatalf("plaintext mismatch: got %q want %q", pt, v.PlaintextUTF8)
	}
}

// Recomputing the KDF from the vector's fixed keys must reproduce the
// committed ciphertext byte-for-byte (proves KDF + AAD + encodings match).
func TestSealReproducesCommittedVector(t *testing.T) {
	v := loadVectors(t)

	// Recover the ephemeral SHARED secret from the recipient side: the seal
	// direction used a fixed ephemeral key we don't ship, but ECDH is
	// symmetric — recipient_priv × epk equals eph_priv × recipient_pub.
	priv, err := ecdh.P256().NewPrivateKey(b64(t, v.RecipientPrivateB64))
	if err != nil {
		t.Fatalf("recipient key: %v", err)
	}
	x, err := sharedX(priv, b64(t, v.Enc.Epk))
	if err != nil {
		t.Fatalf("ECDH: %v", err)
	}
	key, err := DeriveKey(x, v.Enc.Kid)
	if err != nil {
		t.Fatalf("DeriveKey: %v", err)
	}
	gcm, err := newGCM(key)
	if err != nil {
		t.Fatalf("GCM: %v", err)
	}
	box := gcm.Seal(nil, b64(t, v.Enc.IV), []byte(v.PlaintextUTF8),
		AAD(v.Routing.Event, v.Routing.To, v.Routing.From))
	if got := base64.StdEncoding.EncodeToString(box); got != v.MessageB64 {
		t.Fatalf("ciphertext mismatch:\n got %s\nwant %s", got, v.MessageB64)
	}
}

func TestRoundTrip(t *testing.T) {
	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatalf("GenerateKeyPair: %v", err)
	}
	plaintext := []byte(`{"cmd":"uptime"}`)
	enc, msg, err := Seal(pub, "command", "dev-9", "op-1", plaintext, "dev-9-enc")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	pt, err := Open(priv, enc, "command", "dev-9", "op-1", msg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !bytes.Equal(pt, plaintext) {
		t.Fatalf("round-trip mismatch: %q", pt)
	}
}

// A ciphertext moved onto different routing (a swapped envelope) must fail:
// the AAD binds event/to/from.
func TestAADBindsRouting(t *testing.T) {
	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc, msg, err := Seal(pub, "command", "dev-9", "op-1", []byte("secret"), "k1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(priv, enc, "command", "dev-OTHER", "op-1", msg); err == nil {
		t.Fatal("Open must fail when 'to' differs from the sealed routing")
	}
}

func TestTamperedCiphertextFails(t *testing.T) {
	priv, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc, msg, err := Seal(pub, "e", "t", "f", []byte("payload"), "k1")
	if err != nil {
		t.Fatal(err)
	}
	box := b64(t, msg)
	box[0] ^= 0x01
	if _, err := Open(priv, enc, "e", "t", "f", base64.StdEncoding.EncodeToString(box)); err == nil {
		t.Fatal("Open must fail on a tampered ciphertext")
	}
}

func TestWrongKeyFails(t *testing.T) {
	_, pub, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	otherPriv, _, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	enc, msg, err := Seal(pub, "e", "t", "f", []byte("payload"), "k1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(otherPriv, enc, "e", "t", "f", msg); err == nil {
		t.Fatal("Open must fail with the wrong private key")
	}
}

func TestPublicKeyFromPrivate(t *testing.T) {
	v := loadVectors(t)
	pub, err := PublicKeyFromPrivate(b64(t, v.RecipientPrivateB64))
	if err != nil {
		t.Fatal(err)
	}
	if got := base64.StdEncoding.EncodeToString(pub); got != v.RecipientPublicB64 {
		t.Fatalf("derived public key mismatch:\n got %s\nwant %s", got, v.RecipientPublicB64)
	}
}
