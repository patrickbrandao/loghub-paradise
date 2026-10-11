# syntax=docker/dockerfile:1
#
# go-syslog-server -- imagem de deploy (LogHub, envelope JLH type=syslog).
#
# Build totalmente CGO-free: SQLite e' modernc.org/sqlite (Go puro), e os
# clientes de Redis (rueidis), PostgreSQL (pgx), MySQL e NATS tambem sao Go puro.
# Binario estatico, imagem final sem toolchain.
#
#   docker build -f docker/Dockerfile -t go-syslog-server:local .
#
# O contexto de build e' a raiz do modulo (go-syslog-server/), NAO esta pasta.

ARG GO_VERSION=1.25
ARG ALPINE_VERSION=3.22

# ----------------------------------------------------------------- build stage
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build

WORKDIR /src

# go.mod/go.sum primeiro: a camada de download so' invalida quando as
# dependencias mudam, nao a cada edicao de codigo.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG TARGETOS
ARG TARGETARCH

# -trimpath: build reproduzivel. -s -w: ~30% menor; o stack trace do Go
# continua legivel. time/tzdata e' embutido pelo proprio binario (SPEC §15.4).
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -tags osusergo,netgo \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /app/go-syslog-server \
      ./cmd/go-syslog-server

# ---------------------------------------------------------------- runtime stage
FROM alpine:${ALPINE_VERSION}

# ca-certificates: TLS de saida (NATS, Redis, PostgreSQL, MySQL).
# wget (busybox): usado pelo HEALTHCHECK do compose contra /healthz.
# Sem tzdata: o binario importa time/tzdata (cmd/go-syslog-server e
# internal/config), e nada mais na imagem le /usr/share/zoneinfo.
#
# uid/gid 65532 e' o usuario do processo: o que os compose montam so' leitura
# (/etc/syslog/syslog.ini, /etc/syslog/tls/server.key) precisa ser legivel por
# ele, e /data gravavel.
RUN apk add --no-cache ca-certificates \
 && addgroup -g 65532 -S sls \
 && adduser -u 65532 -S -G sls -H -h /data sls \
 && mkdir -p /data /etc/syslog/tls \
 && chown -R 65532:65532 /data

# Binario em /app, dono root: o processo (uid 65532) executa e nao reescreve.
COPY --from=build /app/go-syslog-server /app/go-syslog-server
ENV PATH="/app:${PATH}"

# /etc/syslog: o INI (SYSLOG_CONFIG=/etc/syslog/syslog.ini) e o TLS em
# /etc/syslog/tls, montados so' leitura pelos compose. A imagem nao traz INI:
# sem SYSLOG_CONFIG, tudo vem do env e dos padroes.
#
# /data e' a UNICA area gravavel do container (docs/SPEC.md §16.6): a
# identidade do go-loghub-ident (agent_uuid, machine_id), ring.db, events.db e
# emergency/. DATADIR e' a variavel que o servidor E a biblioteca de identidade
# leem; os dois gravam na mesma raiz.
#
# Sem `VOLUME /data`: criaria um volume ANONIMO a cada `docker run` sem -v, e a
# identidade mudaria a cada recriacao, sem aviso. Os compose declaram um volume
# NOMEADO por instancia -- nunca o mesmo volume em duas replicas, ou elas
# publicam o mesmo `from`.
ENV DATADIR=/data
# Sem isto o AgentName seria o nome base de argv[0]; o binario se chama
# go-syslog-server, mas fixar evita surpresa num ENTRYPOINT diferente.
ENV AGENT_NAME=go-syslog-server

USER 65532:65532
WORKDIR /data

# As portas NAO sao 514 (SPEC §4.1): a imagem roda como uid 65532.
#
#   1514/udp  syslog UDP (RFC 5426)
#   1514/tcp  syslog TCP (RFC 6587)
#   6514/tcp  syslog TLS (RFC 5425), desligado ate haver certificado
#   9102/tcp  admin (/healthz, /readyz, /metrics)
EXPOSE 1514/udp 1514/tcp 6514/tcp 9102/tcp

# Caminho absoluto, sempre. O binario trata SIGTERM/SIGINT e roda como PID 1.
ENTRYPOINT ["/app/go-syslog-server"]
