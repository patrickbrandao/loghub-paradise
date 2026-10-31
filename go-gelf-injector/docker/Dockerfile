# syntax=docker/dockerfile:1
#
# go-gelf-injector -- imagem do injetor GELF (cliente de teste e carga).
#
# Binario estatico (CGO_ENABLED=0, so' a stdlib). Util para injetar de dentro
# da rede de um compose, onde o servidor e' alcancado pelo nome do servico:
#
#   docker build -f docker/Dockerfile -t go-gelf-injector:local .
#   docker run --rm --network <rede> -v "$PWD:/w" go-gelf-injector:local \
#     --transport udp --target gelf:12201 --pcap /w/prod.pcap
#
# O contexto de build e' a raiz do modulo (go-gelf-injector/).

ARG GO_VERSION=1.25
ARG ALPINE_VERSION=3.22

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
ARG VERSION=dev
ARG COMMIT=none
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -tags osusergo,netgo \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /app/go-gelf-injector ./cmd/go-gelf-injector

FROM alpine:${ALPINE_VERSION}
# ca-certificates: verificacao do servidor no --transport tls e https.
RUN apk add --no-cache ca-certificates
COPY --from=build /app/go-gelf-injector /app/go-gelf-injector
USER 65532:65532
WORKDIR /w
ENTRYPOINT ["/app/go-gelf-injector"]
