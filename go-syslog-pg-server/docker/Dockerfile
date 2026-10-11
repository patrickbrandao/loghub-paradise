# syntax=docker/dockerfile:1
#
# go-syslog-pg-server -- imagem de deploy (LogHub, envelope JLH type=syslog,
# fila loghub.inbox do PostgreSQL).
#
# Build totalmente CGO-free: as tres dependencias de producao (pgx,
# go-loghub-ident, go-loghub-uuidv7) sao Go puro. Binario estatico, imagem
# final sem toolchain.
#
#   docker build -f docker/Dockerfile -t go-syslog-pg-server:local .
#
# O contexto de build e' a raiz do modulo (go-syslog-pg-server/), NAO esta pasta.

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
      -o /app/go-syslog-pg-server \
      ./cmd/go-syslog-pg-server

# ---------------------------------------------------------------- runtime stage
FROM alpine:${ALPINE_VERSION}

# Sem ca-certificates: nao ha TLS em lugar nenhum (docs/SPEC.md §1.2); a
# conexao com o PostgreSQL e' sempre sslmode=disable.
# wget (busybox): usado pelo healthcheck do compose contra /healthz.
# Sem tzdata: o binario importa time/tzdata (cmd/go-syslog-pg-server), e nada
# mais na imagem le /usr/share/zoneinfo.
#
# uid/gid 65532 e' o usuario do processo: o que o compose monta so' leitura
# (/etc/syslog/syslog.ini) precisa ser legivel por ele, e /data gravavel.
RUN addgroup -g 65532 -S slpg \
 && adduser -u 65532 -S -G slpg -H -h /data slpg \
 && mkdir -p /data /etc/syslog \
 && chown -R 65532:65532 /data

# Binario em /app, dono root: o processo (uid 65532) executa e nao reescreve.
COPY --from=build /app/go-syslog-pg-server /app/go-syslog-pg-server
ENV PATH="/app:${PATH}"

# /etc/syslog: o INI (SYSLOG_CONFIG=/etc/syslog/syslog.ini), montado so'
# leitura pelo compose. A imagem nao traz INI: sem SYSLOG_CONFIG, tudo vem do
# env e dos padroes.
#
# /data e' a UNICA area gravavel do container (docs/SPEC.md §16.6): a
# identidade do go-loghub-ident (agent_uuid, machine_id) e emergency/ (os dumps
# .jsonl). DATADIR e' a variavel que o servidor E a biblioteca de identidade
# leem; os dois gravam na mesma raiz.
#
# Sem `VOLUME /data`: criaria um volume ANONIMO a cada `docker run` sem -v, e a
# identidade mudaria a cada recriacao, sem aviso. Os compose declaram um volume
# NOMEADO por instancia -- nunca o mesmo volume em duas replicas, ou elas
# enfileiram o mesmo `from`.
ENV DATADIR=/data
# Sem isto o AgentName seria o nome base de argv[0]; o binario se chama
# go-syslog-pg-server, mas fixar evita surpresa num ENTRYPOINT diferente.
ENV AGENT_NAME=go-syslog-pg-server

USER 65532:65532
WORKDIR /data

# As portas NAO sao 514 (SPEC §4.1): a imagem roda como uid 65532.
#
#   1514/udp  syslog UDP (RFC 5426)
#   1514/tcp  syslog TCP (RFC 6587)
#   9102/tcp  admin (/healthz, /readyz, /metrics)
# Sem 6514: nao ha TLS (RFC 5425) neste servidor.
EXPOSE 1514/udp 1514/tcp 9102/tcp

# Caminho absoluto, sempre. O binario trata SIGTERM/SIGINT e roda como PID 1.
ENTRYPOINT ["/app/go-syslog-pg-server"]
