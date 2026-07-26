// Package e2ee implements tyo-mq's end-to-end encrypted payload suite
// ecdh-es-p256-a256gcm (see E2EE.md in the tyo-mq repo): ephemeral-static
// ECDH on P-256 → HKDF-SHA256 → AES-256-GCM, with the cleartext routing
// (event, to, from) bound into the AAD so a sealed payload cannot be
// cut-and-pasted onto a different envelope.
//
// This is the byte-for-byte contract shared by every tyo-mq client language;
// conformance is pinned by the committed vectors (tests/e2ee-vectors.json in
// the tyo-mq repo, mirrored here in testdata/).
package e2ee

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
)

// ALG is the only suite currently defined.
const ALG = "ecdh-es-p256-a256gcm"

const (
	infoPrefix = "tyo-mq-e2ee-v1:"
	ivBytes    = 12
	keyBytes   = 32
)

// Enc is the encryption envelope carried alongside the (now ciphertext)
// message field: {v, alg, epk, iv, kid}.
type Enc struct {
	V   int    `json:"v"`
	Alg string `json:"alg"`
	Epk string `json:"epk"` // base64 uncompressed P-256 point (65 bytes)
	IV  string `json:"iv"`  // base64 12-byte GCM nonce
	Kid string `json:"kid"` // recipient key id the sender encrypted to
}

// AAD builds the additional authenticated data binding a ciphertext to its
// cleartext routing. Bytes: event "\n" to "\n" from.
func AAD(event, to, from string) []byte {
	return []byte(event + "\n" + to + "\n" + from)
}

// DeriveKey turns the ECDH shared X-coordinate into the AES-256 key.
// HKDF-SHA256 (RFC 5869), empty salt, info = "tyo-mq-e2ee-v1:<alg>:<kid>".
// Inlined rather than depending on crypto/hkdf (Go 1.24+) so the module
// stays dependency-free on Go 1.22.
func DeriveKey(sharedX []byte, kid string) ([]byte, error) {
	info := []byte(infoPrefix + ALG + ":" + kid)
	// Extract: empty salt means a zero-filled key of hash length per RFC 5869.
	extract := hmac.New(sha256.New, make([]byte, sha256.Size))
	extract.Write(sharedX)
	prk := extract.Sum(nil)
	// Expand: one block (T1) covers the full 32-byte output for SHA-256.
	expand := hmac.New(sha256.New, prk)
	expand.Write(info)
	expand.Write([]byte{0x01})
	return expand.Sum(nil)[:keyBytes], nil
}

// GenerateKeyPair returns a fresh P-256 keypair as raw bytes:
// a 32-byte private scalar and a 65-byte uncompressed public point.
func GenerateKeyPair() (priv, pub []byte, err error) {
	key, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	return key.Bytes(), key.PublicKey().Bytes(), nil
}

// PublicKeyFromPrivate derives the uncompressed public point from a raw
// 32-byte private scalar.
func PublicKeyFromPrivate(priv []byte) ([]byte, error) {
	key, err := ecdh.P256().NewPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	return key.PublicKey().Bytes(), nil
}

// sharedX runs ECDH and returns the X coordinate of the shared point as
// 32 big-endian bytes (which is exactly what crypto/ecdh.ECDH returns).
func sharedX(priv *ecdh.PrivateKey, peerPub []byte) ([]byte, error) {
	pub, err := ecdh.P256().NewPublicKey(peerPub)
	if err != nil {
		return nil, fmt.Errorf("e2ee: bad peer public key: %w", err)
	}
	return priv.ECDH(pub)
}

// Seal encrypts plaintext to the recipient's static public key (65-byte
// uncompressed P-256 point). It returns the enc envelope and the base64
// ciphertext||tag that replaces the message field on the wire.
func Seal(recipientPub []byte, event, to, from string, plaintext []byte, kid string) (*Enc, string, error) {
	eph, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, "", err
	}
	iv := make([]byte, ivBytes)
	if _, err := rand.Read(iv); err != nil {
		return nil, "", err
	}
	return sealWith(eph, iv, recipientPub, event, to, from, plaintext, kid)
}

// sealWith is Seal with the ephemeral key and IV injected — deterministic,
// for vector tests.
func sealWith(eph *ecdh.PrivateKey, iv, recipientPub []byte, event, to, from string, plaintext []byte, kid string) (*Enc, string, error) {
	x, err := sharedX(eph, recipientPub)
	if err != nil {
		return nil, "", err
	}
	key, err := DeriveKey(x, kid)
	if err != nil {
		return nil, "", err
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, "", err
	}
	box := gcm.Seal(nil, iv, plaintext, AAD(event, to, from)) // ct||tag
	enc := &Enc{
		V:   1,
		Alg: ALG,
		Epk: base64.StdEncoding.EncodeToString(eph.PublicKey().Bytes()),
		IV:  base64.StdEncoding.EncodeToString(iv),
		Kid: kid,
	}
	return enc, base64.StdEncoding.EncodeToString(box), nil
}

// Open reverses Seal with the recipient's raw 32-byte private scalar. It
// returns the plaintext, or an error on a bad tag / AAD / key / envelope.
func Open(myPriv []byte, enc *Enc, event, to, from, message string) ([]byte, error) {
	if enc == nil || enc.Alg != ALG {
		return nil, errors.New("e2ee: unsupported alg")
	}
	priv, err := ecdh.P256().NewPrivateKey(myPriv)
	if err != nil {
		return nil, fmt.Errorf("e2ee: bad private key: %w", err)
	}
	epk, err := base64.StdEncoding.DecodeString(enc.Epk)
	if err != nil {
		return nil, fmt.Errorf("e2ee: bad epk: %w", err)
	}
	x, err := sharedX(priv, epk)
	if err != nil {
		return nil, err
	}
	key, err := DeriveKey(x, enc.Kid)
	if err != nil {
		return nil, err
	}
	iv, err := base64.StdEncoding.DecodeString(enc.IV)
	if err != nil {
		return nil, fmt.Errorf("e2ee: bad iv: %w", err)
	}
	box, err := base64.StdEncoding.DecodeString(message)
	if err != nil {
		return nil, fmt.Errorf("e2ee: bad ciphertext: %w", err)
	}
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	pt, err := gcm.Open(nil, iv, box, AAD(event, to, from))
	if err != nil {
		return nil, fmt.Errorf("e2ee: decrypt failed: %w", err)
	}
	return pt, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
