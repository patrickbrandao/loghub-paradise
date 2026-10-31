# syntax=docker/dockerfile:1
#
# go-webhook-injector -- imagem do injetor HTTP de webhooks (cliente de teste e carga).
#
# Binario estatico (CGO_ENABLED=0, so' a stdlib). Util para injetar de dentro
# da rede de um compose, onde o servidor e' alcancado pelo nome do servico:
#
#   docker build -f docker/Dockerfile -t go-webhook-injector:local .
#   docker run --rm --network <rede> -v "$PWD:/w" go-webhook-injector:local \
#     --url http://webhook:8080/hook --pcap /w/prod.pcap
#
# O contexto de build e' a raiz do modulo (go-webhook-injector/).

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
      -o /app/go-webhook-injector ./cmd/go-webhook-injector

FROM alpine:${ALPINE_VERSION}
# ca-certificates: verificacao do servidor numa --url https://.
RUN apk add --no-cache ca-certificates
COPY --from=build /app/go-webhook-injector /app/go-webhook-injector
USER 65532:65532
WORKDIR /w
ENTRYPOINT ["/app/go-webhook-injector"]
