# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath \
      -ldflags="-s -w -X main.version=$VERSION -X main.commit=$COMMIT -X main.buildDate=$BUILD_DATE" \
      -o /out/speko-gateway ./cmd/speko-gateway
RUN mkdir -p /out/runtime && chmod 0700 /out/runtime

FROM gcr.io/distroless/static-debian12:nonroot
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown
LABEL org.opencontainers.image.title="Speko Gateway" \
      org.opencontainers.image.description="Open customer-side data plane for real-time voice AI" \
      org.opencontainers.image.url="https://github.com/SpekoAI/gateway" \
      org.opencontainers.image.source="https://github.com/SpekoAI/gateway" \
      org.opencontainers.image.documentation="https://github.com/SpekoAI/gateway#readme" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$COMMIT \
      org.opencontainers.image.created=$BUILD_DATE
COPY --from=build --chown=65532:65532 /out/speko-gateway /usr/local/bin/speko-gateway
COPY --from=build --chown=65532:65532 /out/runtime /run/speko
COPY --from=build --chown=65532:65532 /src/integrations/python /opt/speko/python
USER 65532:65532
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/speko-gateway"]
