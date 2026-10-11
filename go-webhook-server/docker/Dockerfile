# syntax=docker/dockerfile:1
#
# go-webhook-server -- imagem de deploy.
#
# O build e' totalmente CGO-free: o driver SQLite e' modernc.org/sqlite (Go puro)
# e o cliente Redis e' rueidis (Go puro). Isso permite um binario estatico que
# roda em qualquer base, e mantem a imagem final sem toolchain nenhuma.
#
#   docker build -f docker/Dockerfile -t go-webhook-server:local .
#
# O contexto de build e' a raiz do modulo (go-webhook-server/), NAO esta pasta.

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
      -o /app/go-webhook-server \
      ./cmd/go-webhook-server

# ---------------------------------------------------------------- runtime stage
FROM alpine:${ALPINE_VERSION}

# ca-certificates: necessario para sink.redis.tls=true e sink.postgres.tls=true.
# tzdata: rotacao do sink SQLite e' avaliada em UTC, mas os logs ficam legiveis.
# wget (busybox): usado pelo HEALTHCHECK do compose contra /healthz.
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -g 65532 -S whs \
 && adduser -u 65532 -S -G whs -H -h /data whs \
 && mkdir -p /data/ring /data/events /etc/webhook \
 && chown -R 65532:65532 /data

# Binario pronto em /app, nao em /usr/local/bin: regra de TODA imagem do
# loghub-paradise, para que "onde esta' o binario deste servico?" tenha uma
# resposta so' em todo o ecossistema. O mesmo caminho vale nos dois estagios.
#
# O diretorio fica com dono root e modo 0755, herdado do estagio de build: o
# processo (uid 65532) executa o binario e nao tem como reescreve-lo. Isso
# complementa a regra do /data -- a area gravavel nao guarda executavel, e o
# executavel nao esta' em area gravavel.
COPY --from=build /app/go-webhook-server /app/go-webhook-server

# /app no PATH so' por ergonomia: mantem `docker exec <c> go-webhook-server
# --version` funcionando depois da mudanca de /usr/local/bin. O ENTRYPOINT usa
# caminho absoluto e nao depende disto.
ENV PATH="/app:${PATH}"

# /data e' a UNICA area gravavel do container. Regra do ecossistema
# loghub-paradise inteiro, nao so' deste servico: toda imagem do projeto grava
# em /data e em lugar nenhum alem (docs/SPEC.md 13.6, invariante 13). Aqui isso
# cobre ring SQLite, events SQLite e dumps de emergencia.
#
# Um ponto de montagem so', com o mesmo nome em toda imagem, faz backup, rotacao
# de disco e politica de volume serem decididos uma vez, e nao por servico. A
# imagem oficial do Redis ja' grava em /data; isto alinha o resto com ela.
#
# O volume nomeado do compose herda dono/modo daqui na primeira montagem, o que
# e' o que permite rodar como nao-root sem chown externo.
#
# ring/ e events/ existem so' para servir de ponto de montagem em
# compose.sqlite-sqlite.yml, que separa os dois bancos em volumes distintos.
# Um ponto de montagem que NAO exista na imagem e' criado pelo Docker como
# root, e o processo (uid 65532) nao conseguiria escrever nele. Nas demais
# combinacoes eles ficam vazios e nao custam nada.
#
# Sem `VOLUME /data`: a instrucao criaria um volume ANONIMO a cada `docker run`
# sem -v, acumulando dados orfaos que ninguem sabe de qual execucao vieram. Os
# compose declaram o volume nomeado explicitamente, que e' o que se quer.
#
# /etc/webhook e' o ponto de montagem do INI, sempre :ro -- leitura, nunca
# escrita.
ENV WHS_RING_DATA_DIR=/data
# A identidade LogHub (SPEC §3.5): sem isto o AgentName seria o nome base de
# argv[0]. O binario exporta DATADIR=ring.data_dir antes de chamar o
# go-loghub-ident, que grava agent_uuid e machine_id em /data.
ENV AGENT_NAME=go-webhook-server

USER 65532:65532

# WORKDIR e' /data em TODA imagem do loghub-paradise, pela mesma regra que poe
# o binario em /app: um lugar so', igual em todo servico -- a imagem oficial do
# Redis ja' faz exatamente isto.
#
# Alem da convencao, e' rede de seguranca: o padrao do binario e'
# ring.data_dir=./data, correto quando ele roda no host. Resolvido a partir
# daqui, um caminho relativo cai dentro do volume em vez da camada efemera do
# container, onde a escrita funcionaria e sumiria no proximo `docker rm`.
WORKDIR /data

# 8080 trafego publico, 9101 admin (/healthz, /readyz, /metrics).
EXPOSE 8080 9101

# Caminho absoluto, sempre: /app/<nome-do-binario>, nunca o nome solto. O
# runtime ate' resolveria o nome pelo PATH, mas ai' o container passaria a
# depender de um ENV que qualquer camada, base nova ou `docker run -e` pode
# mudar -- e a falha seria "executable file not found", sem dizer o que
# procurava. O ENV PATH acima e' conveniencia de `docker exec` interativo e
# nada mais depende dele.
#
# O binario trata SIGTERM/SIGINT diretamente (signal.NotifyContext), entao roda
# como PID 1 sem init externo. Nao ha processos filhos para reap.
ENTRYPOINT ["/app/go-webhook-server"]
