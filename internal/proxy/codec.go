package proxy

import (
	"fmt"

	"google.golang.org/grpc/encoding"
)

func init() {
	encoding.RegisterCodec(passthroughCodec{})
}

// passthroughCodec replaces the default "proto" codec so the proxy can
// transparently forward any gRPC message as raw bytes without needing
// schema knowledge.
type passthroughCodec struct{}

func (passthroughCodec) Name() string { return "proto" }

func (passthroughCodec) Marshal(v interface{}) ([]byte, error) {
	m, ok := v.(*RawMessage)
	if !ok {
		return nil, fmt.Errorf("passthroughCodec: unsupported type %T", v)
	}
	if m == nil {
		return nil, nil
	}
	b := make([]byte, len(m.Data))
	copy(b, m.Data)
	return b, nil
}

func (passthroughCodec) Unmarshal(data []byte, v interface{}) error {
	m, ok := v.(*RawMessage)
	if !ok {
		return fmt.Errorf("passthroughCodec: unsupported type %T", v)
	}
	m.Data = make([]byte, len(data))
	copy(m.Data, data)
	return nil
}
