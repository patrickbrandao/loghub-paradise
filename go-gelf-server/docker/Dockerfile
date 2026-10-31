# syntax=docker/dockerfile:1
#
# go-gelf-server -- imagem de deploy (LogHub, envelope JLH type=gelf).
#
# Build totalmente CGO-free: SQLite e' modernc.org/sqlite (Go puro), e os
# clientes de Redis (rueidis), PostgreSQL (pgx), MySQL, ClickHouse e NATS tambem
# sao Go puro.
# Binario estatico, imagem final sem toolchain.
#
#   docker build -f docker/Dockerfile -t go-gelf-server:local .
#
# O contexto de build e' a raiz do modulo (go-gelf-server/), NAO esta pasta.

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
      -o /app/go-gelf-server \
      ./cmd/go-gelf-server

# ---------------------------------------------------------------- runtime stage
FROM alpine:${ALPINE_VERSION}

# ca-certificates: TLS de saida (NATS, Redis, PostgreSQL, MySQL).
# wget (busybox): usado pelo HEALTHCHECK do compose contra /healthz.
# Sem tzdata: o binario importa time/tzdata (cmd/go-gelf-server e
# internal/config), e nada mais na imagem le /usr/share/zoneinfo.
#
# uid/gid 65532 e' o usuario do processo: o que os compose montam so' leitura
# (/etc/gelf/gelf.ini, /etc/gelf/tls/server.key) precisa ser legivel por
# ele, e /data gravavel.
RUN apk add --no-cache ca-certificates \
 && addgroup -g 65532 -S gls \
 && adduser -u 65532 -S -G gls -H -h /data gls \
 && mkdir -p /data /etc/gelf/tls \
 && chown -R 65532:65532 /data

# Binario em /app, dono root: o processo (uid 65532) executa e nao reescreve.
COPY --from=build /app/go-gelf-server /app/go-gelf-server
ENV PATH="/app:${PATH}"

# /etc/gelf: o INI (GELF_CONFIG=/etc/gelf/gelf.ini) e o TLS em /etc/gelf/tls,
# montados so' leitura pelos compose. A imagem nao traz INI: sem GELF_CONFIG,
# tudo vem do env e dos padroes.
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
# go-gelf-server, mas fixar evita surpresa num ENTRYPOINT diferente.
ENV AGENT_NAME=go-gelf-server

USER 65532:65532
WORKDIR /data

# As portas sao as do GELF (SPEC §4.1), todas altas: a imagem roda como uid
# 65532.
#
#   12201/udp  GELF UDP (chunks, gzip/zlib)
#   12201/tcp  GELF TCP (moldura \0)
#   12202/tcp  GELF HTTP (POST /gelf)
#   12203/tcp  GELF TCP+TLS, desligado ate haver certificado
#   12204/tcp  GELF HTTPS, desligado ate haver certificado
#   9104/tcp   admin (/healthz, /readyz, /metrics)
EXPOSE 12201/udp 12201/tcp 12202/tcp 12203/tcp 12204/tcp 9104/tcp

# Caminho absoluto, sempre. O binario trata SIGTERM/SIGINT e roda como PID 1.
ENTRYPOINT ["/app/go-gelf-server"]
