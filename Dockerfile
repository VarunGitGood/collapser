# Multi-stage: `--target proxy` builds the sidecar, `--target backend` the demo
# backend the deployment uses to generate load.
FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies first so a code-only change does not re-download the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off and the symbol table stripped so the result runs on scratch.
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/proxy ./cmd/proxy && \
    CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/backend ./cmd/backend

FROM scratch AS proxy
COPY --from=build /out/proxy /proxy
EXPOSE 50052 2112
USER 65532:65532
ENTRYPOINT ["/proxy"]

FROM scratch AS backend
COPY --from=build /out/backend /backend
EXPOSE 50051
USER 65532:65532
ENTRYPOINT ["/backend"]
