# syntax=docker/dockerfile:1
#
# go-relplog-pg-server -- imagem de deploy (LogHub: RELP do rsyslog/omrelp na
# entrada, envelope JLH type=syslog com "x-transport":"relp" na fila
# loghub.inbox do PostgreSQL).
#
# Build totalmente CGO-free: as tres dependencias de producao (pgx,
# go-loghub-ident, go-loghub-uuidv7) sao Go puro. Binario estatico, imagem
# final sem toolchain.
#
#   docker build -f docker/Dockerfile -t go-relplog-pg-server:local .
#
# O contexto de build e' a raiz do modulo (go-relplog-pg-server/), NAO esta pasta.

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
      -o /app/go-relplog-pg-server \
      ./cmd/go-relplog-pg-server

# ---------------------------------------------------------------- runtime stage
FROM alpine:${ALPINE_VERSION}

# Sem ca-certificates: nao ha TLS em lugar nenhum (docs/SPEC.md §1.2), nem na
# entrada (RELP em claro) nem ate' o PostgreSQL (sempre sslmode=disable).
# wget (busybox): usado pelo healthcheck do compose contra /healthz.
# Sem tzdata: o binario importa time/tzdata (cmd/go-relplog-pg-server), e nada
# mais na imagem le /usr/share/zoneinfo.
#
# uid/gid 65532 e' o usuario do processo: o que o compose monta so' leitura
# (/etc/relplog/relplog.ini) precisa ser legivel por ele, e /data gravavel.
# Nome rlpg (o go-relplog-server usa rls, o go-syslog-pg-server slpg): o uid e'
# o mesmo, o que importa para as permissoes de um volume.
RUN addgroup -g 65532 -S rlpg \
 && adduser -u 65532 -S -G rlpg -H -h /data rlpg \
 && mkdir -p /data /etc/relplog \
 && chown -R 65532:65532 /data

# Binario em /app, dono root: o processo (uid 65532) executa e nao reescreve.
COPY --from=build /app/go-relplog-pg-server /app/go-relplog-pg-server
ENV PATH="/app:${PATH}"

# /etc/relplog: o INI (RELPLOG_CONFIG=/etc/relplog/relplog.ini), montado so'
# leitura pelo compose. A imagem nao traz INI: sem RELPLOG_CONFIG, tudo vem do
# env e dos padroes.
#
# /data e' a UNICA area gravavel do container (docs/SPEC.md §16.6): a
# identidade do go-loghub-ident (agent_uuid, machine_id) e emergency/ (os dumps
# .jsonl). DATADIR e' a variavel que o servidor E a biblioteca de identidade
# leem; os dois gravam na mesma raiz.
#
# Sem `VOLUME /data`: criaria um volume ANONIMO a cada `docker run` sem -v, e a
# identidade mudaria a cada recriacao, sem aviso. O compose declara um volume
# NOMEADO por instancia -- nunca o mesmo volume em duas replicas, ou elas
# enfileiram o mesmo `from`.
ENV DATADIR=/data
# Sem isto o AgentName seria o nome base de argv[0]; o binario se chama
# go-relplog-pg-server, mas fixar evita surpresa num ENTRYPOINT diferente.
ENV AGENT_NAME=go-relplog-pg-server

USER 65532:65532
WORKDIR /data

# As portas padrao (SPEC §4.1), altas: a imagem roda como uid 65532.
# Sem UDP, sem unix e sem TLS: o RELP so' existe sobre fluxo (SPEC §4), e o
# TLS saiu (SPEC §1.2).
#
#   20514/tcp  RELP em claro (omrelp sem tls), listen.tcp
#   9103/tcp   admin (/healthz, /readyz, /metrics); o go-syslog-pg-server usa 9102
EXPOSE 20514/tcp 9103/tcp

# Caminho absoluto, sempre. O binario trata SIGTERM/SIGINT e roda como PID 1;
# o stop_grace_period do compose cobre pre_stop_delay + drain_timeout + os 6 s
# em que cada sessao RELP esvazia as respostas ja' decididas (SPEC §5.8).
ENTRYPOINT ["/app/go-relplog-pg-server"]
