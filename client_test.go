package tyomq

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

// TestLiveBinaryEmit round-trips EmitBinary against a real socket.io server to
// prove the BINARY_EVENT wire format decodes to a Buffer with the exact bytes.
// Skips unless TYO_MQ_BINARY_TEST_URL points at a running server (see
// scratchpad/bin_echo_server.js). Not a CI dependency.
func TestLiveBinaryEmit(t *testing.T) {
	url := os.Getenv("TYO_MQ_BINARY_TEST_URL")
	if url == "" {
		t.Skip("set TYO_MQ_BINARY_TEST_URL to run the live binary round-trip")
	}
	c := NewClient(url, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	ready := make(chan struct{})
	go c.Connect(ctx, ready)
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("connect timed out")
	}
	defer c.Close()

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x54, 0x01, 0x00, 0x2A}
	if err := c.EmitBinary("frame", payload); err != nil {
		t.Fatalf("EmitBinary: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // let the server capture it
}

func TestNamespaceFraming(t *testing.T) {
	if got := connectFrame("/"); got != "40" {
		t.Errorf("connectFrame(/) = %q, want 40", got)
	}
	if got := connectFrame("/remote"); got != "40/remote," {
		t.Errorf("connectFrame(/remote) = %q, want 40/remote,", got)
	}
	if got := connectFrame(""); got != "40" {
		t.Errorf("connectFrame(\"\") = %q, want 40", got)
	}
	if got := emitPrefix("/"); got != "42" {
		t.Errorf("emitPrefix(/) = %q, want 42", got)
	}
	if got := emitPrefix("/remote"); got != "42/remote," {
		t.Errorf("emitPrefix(/remote) = %q, want 42/remote,", got)
	}

	ns, payload := splitNamespace(`42/remote,["frame",{}]`)
	if ns != "/remote" || payload != `["frame",{}]` {
		t.Errorf("splitNamespace(/remote) = (%q,%q)", ns, payload)
	}
	ns, payload = splitNamespace(`42["announce",{}]`)
	if ns != "/" || payload != `["announce",{}]` {
		t.Errorf("splitNamespace(default) = (%q,%q)", ns, payload)
	}
}

func TestBinaryEmitPrefix(t *testing.T) {
	if got := binaryEmitPrefix("/"); got != "451-" {
		t.Errorf("binaryEmitPrefix(/) = %q, want 451-", got)
	}
	if got := binaryEmitPrefix(""); got != "451-" {
		t.Errorf("binaryEmitPrefix(\"\") = %q, want 451-", got)
	}
	if got := binaryEmitPrefix("/remote"); got != "451-/remote," {
		t.Errorf("binaryEmitPrefix(/remote) = %q, want 451-/remote,", got)
	}

	// The full header packet a BINARY_EVENT for "frame" must produce on the
	// /remote namespace: EIO MESSAGE + SIO BINARY_EVENT + 1 attachment + nsp +
	// the event array with a placeholder for the (separate) binary frame.
	header := binaryEmitPrefix("/remote") + `["frame",{"_placeholder":true,"num":0}]`
	want := `451-/remote,["frame",{"_placeholder":true,"num":0}]`
	if header != want {
		t.Errorf("binary header = %q, want %q", header, want)
	}
}
