# go-gelf-pg-server

Servidor de ingestão **GELF** (Graylog Extended Log Format 1.1) do ecossistema
**LogHub**, cortado para o mínimo: recebe documentos GELF por UDP (com chunks e
gzip/zlib), TCP (moldura `\0`) e HTTP (`POST /gelf`), converte cada um num
envelope **JLH** `type=gelf` e o **enfileira** na `loghub.inbox` do PostgreSQL.
Ring só em memória, emergência em JSON Lines, **sem TLS em lugar nenhum**, três
dependências de produção.

É ao [`go-gelf-server`](../go-gelf-server/) exatamente o que o
[`go-syslog-pg-server`](../go-syslog-pg-server/) é ao `go-syslog-server`: a
recepção e a conversão GELF são as do pai (código copiado literalmente); ring,
sink, dump, registry, validação e admin são os do corte syslog.

```
Docker gelf driver, fluent-bit, Logstash, graypy, logstash-gelf ...
   │  UDP :12201 (chunks, gzip/zlib)   TCP :12201 (\0)   HTTP :12202 POST /gelf
   ▼
go-gelf-pg-server ──JLH type=gelf──▶ PostgreSQL 18: loghub.inbox (INSERT ... ON CONFLICT (uuid))
```

## Em uma página

| | |
|---|---|
| Transportes | `listen.udp` (`:12201`), `listen.tcp` (`:12201`), `listen.http` (`:12202`); PROXY protocol v1/v2 opcional no TCP e no HTTP. Sem TCP+TLS, sem HTTPS |
| Envelope | JLH `type=gelf`, payload em kebab-case validado por `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json` (SPEC §3.2) |
| `level` ausente | `6` (info) com aviso `level-default` — desvio deliberado da GELF 1.1 (D1) |
| Tamanho | `convert.max_message_bytes` = **128 KiB** do documento descomprimido; acima disso o documento é **recusado e contado**, nunca truncado (HTTP `413`) |
| Ring | só `memory`; FATAL e dreno estourado viram dump JSON Lines em `$DATADIR/emergency`, reingerido no boot |
| Destino | só a fila `loghub.inbox` (DDL literal em `docker/postgres/inbox.sql`), sempre `sslmode=disable` |
| Confirmação | UDP: nenhuma (perda contada em `gelf_messages_dropped_total`); TCP: continuar lendo (ring cheio = parar de ler); HTTP: `202` depois do ring, `503` + `Retry-After` quando não dá agora |
| Operação | `/healthz`, `/readyz`, `/metrics` (prefixo `gelf_`) em `127.0.0.1:9104`; `SIGHUP` recarrega `[log]` |

## Rodar

A fila precisa existir: o servidor nunca faz DDL.

```bash
psql "$DSN" -f docker/postgres/inbox.sql
go build -o go-gelf-pg-server ./cmd/go-gelf-pg-server
mkdir -p /tmp/gelfpg-data
DATADIR=/tmp/gelfpg-data AGENT_NAME=go-gelf-pg-server HOSTNAME=dev-mac \
  LOGHUB_EB_URL='postgres://gelf:gelf@127.0.0.1:5432/gelf' \
  ./go-gelf-pg-server --listen.udp=127.0.0.1:12201 --listen.tcp=127.0.0.1:12201 \
  --listen.http=127.0.0.1:12202 --admin.bind=127.0.0.1:19304
```

Mandando GELF:

```bash
# UDP, documento inteiro
printf '{"version":"1.1","host":"dev","short_message":"oi","level":4}' | nc -u -w1 127.0.0.1 12201
# TCP, moldura \0
printf '{"version":"1.1","host":"dev","short_message":"oi"}\0' | nc -w1 127.0.0.1 12201
# HTTP, gzip
printf '{"version":"1.1","host":"dev","short_message":"oi","_pedido":42}' | gzip |
  curl -sS -w '%{http_code}\n' -H 'Content-Encoding: gzip' --data-binary @- http://127.0.0.1:12202/gelf
# o driver de log do Docker
docker run --rm --log-driver=gelf --log-opt gelf-address=udp://127.0.0.1:12201 alpine echo oi
```

Os eventos ficam na fila:

```bash
psql "$DSN" -c "select payload->>'transport', payload->>'short-message' from loghub.inbox order by uuid"
```

Configuração: INI (`GELF_CONFIG`), flags (`--secao.chave`) e env
(`GELF_<SEÇÃO>_<CHAVE>`), com precedência flag > env > INI > padrão; `--help`
lista tudo e `--dump-config` mostra o efetivo. As chaves do `go-gelf-server`
que não existem aqui (TLS, HTTPS, filtro, rate-limit, outros rings e sinks,
beacon) são exit 1 na linha de comando e no INI, e um `WARN` no ambiente.
A tabela completa está na SPEC §16.5.

## Docker

```bash
docker build -f docker/Dockerfile -t tmsoftbrasil/go-gelf-pg-server:dev .
cd docker && docker compose up -d                 # servidor + PostgreSQL 18 com a fila
docker compose -f docker-compose.yml -f compose.demo.yml --profile demo up -d   # + emissor fluent-bit
docker compose -f docker-compose.yml -f compose.demo.yml --profile demo down -v
```

O compose publica GELF em `12301` (UDP e TCP) e `12302` (HTTP), o admin em
`127.0.0.1:19304` e o PostgreSQL em `127.0.0.1:15494`, para não colidir com os
compose do `go-gelf-server`. As chaves recarregáveis ficam em
`docker/conf/gelf.ini` (`docker kill -s HUP gelf-pg`); o resto, em
`docker/.env.example`. O emissor de demonstração (fluent-bit 4.1.1) manda UDP
com gzip em vários chunks e TCP com `\0`.

## Documentos

| Arquivo | Conteúdo |
|---|---|
| [`docs/DECISOES.md`](docs/DECISOES.md) | As decisões da família GELF que valem aqui e as do corte |
| [`docs/SPEC.md`](docs/SPEC.md) | A especificação, fonte da verdade (numeração do `go-gelf-server`) |
| [`docs/TESTING.md`](docs/TESTING.md) | Como testar, o que cada suíte prova, a rodada de emissores reais e o que ainda não está provado |
| [`AGENTS.md`](AGENTS.md) | Instruções para agentes de IA que trabalham nesta pasta |
