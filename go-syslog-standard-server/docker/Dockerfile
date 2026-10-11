# syntax=docker/dockerfile:1
#
# go-syslog-server -- imagem de deploy.
#
# Build totalmente CGO-free: o driver SQLite e' modernc.org/sqlite (Go puro), o
# cliente Redis e' rueidis e o do PostgreSQL e' pgx, ambos Go puro. Isso permite
# um binario estatico e uma imagem final sem toolchain nenhuma.
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

# -trimpath remove caminhos absolutos do binario (build reproduzivel).
# -s -w descartam tabela de simbolos e DWARF: ~30% menor, e o stack trace do Go
# continua legivel porque nao depende deles.
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

# ca-certificates: necessario para sink.redis.tls=true e sink.postgres.tls=true.
# Nada a ver com a ingestao, onde TLS esta' fora de escopo na v1 (SPEC §1.2).
# tzdata: a rotacao do sink SQLite e o ano da RFC 3164 sao avaliados em UTC,
# mas os logs ficam legiveis.
# wget (busybox): usado pelo HEALTHCHECK do compose contra /healthz.
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -g 65532 -S sls \
 && adduser -u 65532 -S -G sls -H -h /data sls \
 && mkdir -p /data/ring /data/events /etc/syslog \
 && chown -R 65532:65532 /data

# Binario pronto em /app, nao em /usr/local/bin: regra de TODA imagem do
# loghub-paradise. O diretorio fica com dono root e modo 0755, herdado do
# estagio de build: o processo (uid 65532) executa o binario e nao tem como
# reescreve-lo.
COPY --from=build /app/go-syslog-server /app/go-syslog-server

# /app no PATH so' por ergonomia de `docker exec`. O ENTRYPOINT usa caminho
# absoluto e nao depende disto.
ENV PATH="/app:${PATH}"

# /data e' a UNICA area gravavel do container, regra do ecossistema inteiro
# (docs/SPEC.md §15.6, invariante §19.15). Aqui cobre ring.db, events.db e os
# dumps de emergencia.
#
# ring/ e events/ existem so' como pontos de montagem para
# compose.sqlite-sqlite.yml, que separa os dois bancos em volumes distintos. Um
# ponto de montagem que NAO exista na imagem e' criado pelo Docker como root, e o
# processo (uid 65532) nao conseguiria escrever nele.
#
# Sem `VOLUME /data`: a instrucao criaria um volume ANONIMO a cada `docker run`
# sem -v, acumulando dados orfaos. Os compose declaram o volume nomeado.
#
# /etc/syslog e' o ponto de montagem do INI, sempre :ro.
ENV SYSLOG_RING_DATA_DIR=/data

USER 65532:65532

# WORKDIR e' /data em TODA imagem do loghub-paradise. Alem da convencao, e'
# rede de seguranca: o padrao do binario e' ring.data_dir=./data, correto no
# host. Resolvido a partir daqui, um caminho relativo cai dentro do volume em vez
# da camada efemera do container.
WORKDIR /data

# As portas NAO sao 514. A imagem roda como uid 65532 e nao abre porta
# privilegiada; os compose publicam "514:1514" (SPEC §4.1).
#
#   1514/udp  syslog UDP (RFC 5426)
#   1514/tcp  syslog TCP (RFC 6587)
#   20514/tcp RELP
#   9102/tcp  admin (/healthz, /readyz, /metrics) -- 9102 e nao 9101, para nao
#             colidir com o go-webhook-server no mesmo host
EXPOSE 1514/udp 1514/tcp 20514/tcp 9102/tcp

# Caminho absoluto, sempre: /app/<nome-do-binario>, nunca o nome solto.
#
# O binario trata SIGTERM/SIGINT diretamente, entao roda como PID 1 sem init
# externo. Nao ha processos filhos para reap.
ENTRYPOINT ["/app/go-syslog-server"]
