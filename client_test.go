package tyomq

import "testing"

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
