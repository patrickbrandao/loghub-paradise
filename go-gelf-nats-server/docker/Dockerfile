# syntax=docker/dockerfile:1
#
# go-gelf-nats-server -- imagem de deploy (LogHub, envelope JLH type=gelf,
# publicado no NATS em loghub.<space>.gelf).
#
# Build totalmente CGO-free: as tres dependencias diretas de producao (nats.go,
# go-loghub-ident, go-loghub-uuidv7) e as transitivas do nats.go sao Go puro;
# gzip, zlib, deflate e o servidor HTTP sao da stdlib. Binario estatico, imagem
# final sem toolchain.
#
#   docker build -f docker/Dockerfile -t tmsoftbrasil/go-gelf-nats-server:dev .
#
# O contexto de build e' a raiz do modulo (go-gelf-nats-server/), NAO esta pasta.

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
      -o /app/go-gelf-nats-server \
      ./cmd/go-gelf-nats-server

# ---------------------------------------------------------------- runtime stage
FROM alpine:${ALPINE_VERSION}

# Sem ca-certificates: nao ha TLS em lugar nenhum (docs/SPEC.md §1.2); o
# cliente NATS e' sempre texto claro, e um servidor que exige TLS e' recusado
# no boot sem nenhum handshake (SPEC §10.7).
# wget (busybox): usado pelo healthcheck do compose contra /healthz.
# Sem tzdata: o binario importa time/tzdata (cmd/go-gelf-nats-server), e nada
# mais na imagem le /usr/share/zoneinfo.
#
# uid/gid 65532 e' o usuario do processo: o que o compose monta so' leitura
# (/etc/gelf/gelf.ini) precisa ser legivel por ele, e /data gravavel.
RUN addgroup -g 65532 -S glnats \
 && adduser -u 65532 -S -G glnats -H -h /data glnats \
 && mkdir -p /data /etc/gelf \
 && chown -R 65532:65532 /data

# Binario em /app, dono root: o processo (uid 65532) executa e nao reescreve.
COPY --from=build /app/go-gelf-nats-server /app/go-gelf-nats-server
ENV PATH="/app:${PATH}"

# /etc/gelf: o INI (GELF_CONFIG=/etc/gelf/gelf.ini), montado so' leitura pelo
# compose. A imagem nao traz INI: sem GELF_CONFIG, tudo vem do env e dos
# padroes.
#
# /data e' a UNICA area gravavel do container (docs/SPEC.md §16.6): a
# identidade do go-loghub-ident (agent_uuid, machine_id) e emergency/ (os dumps
# .jsonl). DATADIR e' a variavel que o servidor E a biblioteca de identidade
# leem; os dois gravam na mesma raiz.
#
# Sem `VOLUME /data`: criaria um volume ANONIMO a cada `docker run` sem -v, e a
# identidade mudaria a cada recriacao, sem aviso. O compose declara um volume
# NOMEADO por instancia -- nunca o mesmo volume em duas replicas, ou elas
# publicam o mesmo `from`.
ENV DATADIR=/data
# Sem isto o AgentName seria o nome base de argv[0]; o binario se chama
# go-gelf-nats-server, mas fixar evita surpresa num ENTRYPOINT diferente.
ENV AGENT_NAME=go-gelf-nats-server

USER 65532:65532
WORKDIR /data

# As portas sao as do GELF (SPEC §4.1), todas altas: a imagem roda como uid
# 65532.
#
#   12201/udp  GELF UDP (chunks, gzip/zlib)
#   12201/tcp  GELF TCP (moldura \0)
#   12202/tcp  GELF HTTP (POST /gelf)
#   9104/tcp   admin (/healthz, /readyz, /metrics)
# Sem 12203/12204: nao ha TLS nem HTTPS neste servidor.
EXPOSE 12201/udp 12201/tcp 12202/tcp 9104/tcp

# Caminho absoluto, sempre. O binario trata SIGTERM/SIGINT e roda como PID 1.
ENTRYPOINT ["/app/go-gelf-nats-server"]
