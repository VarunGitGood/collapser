package proxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"

	"github.com/VarunGitGood/collapser-grpc/internal/collapser"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Handler struct {
	conn      *grpc.ClientConn
	collapser *collapser.Collapser
}

func NewHandler(c *collapser.Collapser, conn *grpc.ClientConn) *Handler {
	return &Handler{
		conn:      conn,
		collapser: c,
	}
}

// NewServer creates a gRPC server configured to use this handler.
// The caller owns the server lifecycle; call GracefulStop for clean shutdown.
func (h *Handler) NewServer() *grpc.Server {
	// passthroughCodec is registered globally via init() in codec.go.
	return grpc.NewServer(grpc.UnknownServiceHandler(h.Handle))
}

func (h *Handler) Handle(srv interface{}, stream grpc.ServerStream) error {
	method, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Errorf(codes.Internal, "cannot extract method")
	}

	first := &RawMessage{}
	if err := stream.RecvMsg(first); err != nil {
		if err == io.EOF {
			return status.Errorf(codes.InvalidArgument, "empty request")
		}
		return err
	}

	// Peek for a second message to detect client-streaming or bidi RPCs.
	// For unary and server-streaming RPCs the client half-closes after a single
	// message, so the second RecvMsg returns io.EOF immediately.
	second := &RawMessage{}
	recvErr := stream.RecvMsg(second)
	if recvErr != nil && recvErr != io.EOF {
		return recvErr
	}

	if recvErr == io.EOF {
		// Unary (or server-streaming with a single request): apply the collapser.
		return h.handleUnary(stream, method, first)
	}

	// Client-streaming or bidi-streaming: bypass the collapser and pipe directly.
	return h.handleStreaming(stream, method, first, second)
}

func (h *Handler) handleUnary(stream grpc.ServerStream, method string, in *RawMessage) error {
	key := h.generateKey(method, in.Data)
	resp, err := h.collapser.Execute(stream.Context(), key, func(ctx context.Context) ([]byte, error) {
		var out RawMessage
		if err := h.conn.Invoke(ctx, method, in, &out, grpc.ForceCodec(passthroughCodec{})); err != nil {
			return nil, err
		}
		return out.Data, nil
	})
	if err != nil {
		return err
	}
	return stream.SendMsg(&RawMessage{Data: resp})
}

// handleStreaming pipes a client-streaming or bidi-streaming RPC directly to the
// backend without collapsing. buffered contains messages already read from the client.
func (h *Handler) handleStreaming(stream grpc.ServerStream, method string, buffered ...*RawMessage) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	outCtx := metadata.NewOutgoingContext(context.Background(), md)

	backendStream, err := h.conn.NewStream(outCtx, &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}, method, grpc.ForceCodec(passthroughCodec{}))
	if err != nil {
		return status.Errorf(codes.Unavailable, "backend stream: %v", err)
	}

	for _, msg := range buffered {
		if err := backendStream.SendMsg(msg); err != nil {
			return err
		}
	}

	// Pipe client → backend in background.
	clientErrCh := make(chan error, 1)
	go func() {
		for {
			msg := &RawMessage{}
			if err := stream.RecvMsg(msg); err != nil {
				if err == io.EOF {
					clientErrCh <- backendStream.CloseSend()
				} else {
					clientErrCh <- err
				}
				return
			}
			if err := backendStream.SendMsg(msg); err != nil {
				clientErrCh <- err
				return
			}
		}
	}()

	// Pipe backend → client in foreground.
	for {
		msg := &RawMessage{}
		if err := backendStream.RecvMsg(msg); err != nil {
			if err == io.EOF {
				return <-clientErrCh
			}
			return err
		}
		if err := stream.SendMsg(msg); err != nil {
			return err
		}
	}
}

func (h *Handler) generateKey(method string, data []byte) string {
	hash := sha256.Sum256(data)
	return method + ":" + hex.EncodeToString(hash[:])
}

type RawMessage struct {
	Data []byte
}

func (m *RawMessage) Reset()         { m.Data = nil }
func (m *RawMessage) String() string { return string(m.Data) }
func (m *RawMessage) ProtoMessage()  {}
