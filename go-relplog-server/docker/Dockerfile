# syntax=docker/dockerfile:1
#
# go-relplog-server -- imagem de deploy (LogHub: RELP do rsyslog/omrelp na
# entrada, envelope JLH type=syslog com "x-transport":"relp" na saida).
#
# Build totalmente CGO-free: SQLite e' modernc.org/sqlite (Go puro), e os
# clientes de Redis (rueidis), PostgreSQL (pgx), MySQL, ClickHouse (clickhouse-go)
# e NATS tambem sao Go puro.
# Binario estatico, imagem final sem toolchain.
#
#   docker build -f docker/Dockerfile -t go-relplog-server:local .
#
# O contexto de build e' a raiz do modulo (go-relplog-server/), NAO esta pasta.

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
      -o /app/go-relplog-server \
      ./cmd/go-relplog-server

# ---------------------------------------------------------------- runtime stage
FROM alpine:${ALPINE_VERSION}

# ca-certificates: TLS de saida (NATS, Redis, PostgreSQL, MySQL).
# wget (busybox): usado pelo HEALTHCHECK do compose contra /healthz.
# Sem tzdata: o binario importa time/tzdata (cmd/go-relplog-server e
# internal/config), e nada mais na imagem le /usr/share/zoneinfo.
#
# uid/gid 65532 e' o usuario do processo: o que os compose montam so' leitura
# (/etc/relplog/relplog.ini, /etc/relplog/tls/server.key) precisa ser legivel
# por ele, e /data gravavel. Nome rls (o go-syslog-server usa sls): o uid e' o
# mesmo, o que importa para as permissoes de um volume ou de uma chave.
RUN apk add --no-cache ca-certificates \
 && addgroup -g 65532 -S rls \
 && adduser -u 65532 -S -G rls -H -h /data rls \
 && mkdir -p /data /etc/relplog/tls \
 && chown -R 65532:65532 /data

# Binario em /app, dono root: o processo (uid 65532) executa e nao reescreve.
COPY --from=build /app/go-relplog-server /app/go-relplog-server
ENV PATH="/app:${PATH}"

# /etc/relplog: o INI (RELPLOG_CONFIG=/etc/relplog/relplog.ini) e o TLS em
# /etc/relplog/tls, montados so' leitura pelos compose. A imagem nao traz INI:
# sem RELPLOG_CONFIG, tudo vem do env e dos padroes.
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
# go-relplog-server, mas fixar evita surpresa num ENTRYPOINT diferente.
ENV AGENT_NAME=go-relplog-server

USER 65532:65532
WORKDIR /data

# As portas padrao do RELP (SPEC §4.1), altas: a imagem roda como uid 65532.
# Sem UDP e sem unix: o RELP so' existe sobre fluxo (SPEC §4).
#
#   20514/tcp  RELP em claro (omrelp sem tls), listen.tcp
#   20515/tcp  RELP sobre TLS (omrelp com tls="on"), listen.tls; desligado ate'
#              haver certificado e RELPLOG_LISTEN_TLS
#   9103/tcp   admin (/healthz, /readyz, /metrics); o go-syslog-server usa 9102
EXPOSE 20514/tcp 20515/tcp 9103/tcp

# Caminho absoluto, sempre. O binario trata SIGTERM/SIGINT e roda como PID 1;
# o stop_grace_period dos compose cobre pre_stop_delay + drain_timeout + os 6 s
# em que cada sessao RELP esvazia as respostas ja' decididas (SPEC §5.8).
ENTRYPOINT ["/app/go-relplog-server"]
