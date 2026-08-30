package proxy

import (
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestPassthroughCodec_RoundTrip(t *testing.T) {
	codec := passthroughCodec{}
	payload := []byte{0x00, 0xff, 0x10, 'h', 'i'}

	encoded, err := codec.Marshal(&RawMessage{Data: payload})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var out RawMessage
	if err := codec.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(out.Data) != string(payload) {
		t.Errorf("round trip changed payload: got %v want %v", out.Data, payload)
	}
}

// The codec must copy, not alias: the decoded result is cached and shared with
// every follower, so it must not point at a buffer gRPC may reuse.
func TestPassthroughCodec_CopiesBuffers(t *testing.T) {
	codec := passthroughCodec{}
	src := []byte("original")

	var out RawMessage
	if err := codec.Unmarshal(src, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	src[0] = 'X'
	if string(out.Data) != "original" {
		t.Errorf("Unmarshal aliased the source buffer: got %q", out.Data)
	}

	in := &RawMessage{Data: []byte("payload")}
	encoded, err := codec.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	in.Data[0] = 'X'
	if string(encoded) != "payload" {
		t.Errorf("Marshal aliased the message buffer: got %q", encoded)
	}
}

func TestPassthroughCodec_RejectsForeignTypes(t *testing.T) {
	codec := passthroughCodec{}
	if _, err := codec.Marshal("not a raw message"); err == nil {
		t.Error("Marshal accepted a non-RawMessage")
	}
	if err := codec.Unmarshal([]byte("x"), &struct{}{}); err == nil {
		t.Error("Unmarshal accepted a non-RawMessage")
	}
}

// The codec deliberately registers itself under the name "proto" so it displaces
// the default codec for the whole process. If that ever changes, the proxy
// silently stops being schema-agnostic.
func TestPassthroughCodec_Name(t *testing.T) {
	if got := (passthroughCodec{}).Name(); got != "proto" {
		t.Errorf("Name() = %q, want %q", got, "proto")
	}
}

func TestGenerateKey_SamePayloadCollapses(t *testing.T) {
	h := NewHandler(nil, nil, nil)
	md := metadata.New(nil)

	a := h.generateKey("/svc/Method", []byte("body"), md)
	b := h.generateKey("/svc/Method", []byte("body"), md)
	if a != b {
		t.Error("identical requests produced different keys, they would not collapse")
	}
}

func TestGenerateKey_DifferentInputsDoNotCollapse(t *testing.T) {
	h := NewHandler(nil, nil, nil)
	md := metadata.New(nil)

	base := h.generateKey("/svc/Method", []byte("body"), md)
	if h.generateKey("/svc/Other", []byte("body"), md) == base {
		t.Error("different methods collapsed to the same key")
	}
	if h.generateKey("/svc/Method", []byte("other"), md) == base {
		t.Error("different payloads collapsed to the same key")
	}
}

// Without key headers configured, two tenants sending the same payload collapse
// into one backend call. With the header configured they must not.
func TestGenerateKey_KeyHeadersSeparateTenants(t *testing.T) {
	tenantA := metadata.Pairs("x-tenant-id", "a")
	tenantB := metadata.Pairs("x-tenant-id", "b")

	unaware := NewHandler(nil, nil, nil)
	if unaware.generateKey("/svc/M", []byte("b"), tenantA) != unaware.generateKey("/svc/M", []byte("b"), tenantB) {
		t.Error("expected tenants to collapse together when no key headers are set")
	}

	aware := NewHandler(nil, nil, []string{"x-tenant-id"})
	keyA := aware.generateKey("/svc/M", []byte("b"), tenantA)
	keyB := aware.generateKey("/svc/M", []byte("b"), tenantB)
	if keyA == keyB {
		t.Error("tenants collapsed together despite x-tenant-id being a key header")
	}
	if keyA != aware.generateKey("/svc/M", []byte("b"), metadata.Pairs("x-tenant-id", "a")) {
		t.Error("same tenant produced unstable keys")
	}
}

func TestNewHandler_NormalizesKeyHeaders(t *testing.T) {
	h := NewHandler(nil, nil, []string{"  X-Tenant-ID ", "", "   "})
	if len(h.keyHeaders) != 1 || h.keyHeaders[0] != "x-tenant-id" {
		t.Errorf("keyHeaders = %v, want [x-tenant-id]", h.keyHeaders)
	}
}

// A header value must not be confusable with a different header/value split.
func TestGenerateKey_HeaderValuesAreDelimited(t *testing.T) {
	h := NewHandler(nil, nil, []string{"a", "b"})
	x := h.generateKey("/m", nil, metadata.Pairs("a", "1", "b", "2"))
	y := h.generateKey("/m", nil, metadata.Pairs("a", "12", "b", ""))
	if x == y {
		t.Error("header values are ambiguously concatenated into the key")
	}
}
