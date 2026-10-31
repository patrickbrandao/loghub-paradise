# go-gelf-server

Servidor de ingestão **GELF** (Graylog Extended Log Format 1.1) do ecossistema
**LogHub**. Recebe documentos GELF por UDP (com chunks e gzip/zlib), TCP
(moldura `\0`), TCP+TLS, HTTP (`POST /gelf`) e HTTPS, converte cada um num
envelope **JLH** `type=gelf` e o entrega a um destino: NATS JetStream (padrão,
subject `loghub.{space}.gelf`), Redis, SQLite, PostgreSQL (`loghub.inbox`),
MySQL, ClickHouse ou nenhum.

É uma cópia adaptada do [`go-syslog-server`](../go-syslog-server/): mesmos
rings, mesmos destinos, mesmo beacon, mesmo dump de emergência, mesma porta
administrativa. O que muda é a recepção (chunks UDP, descompressão com teto,
`\0`, HTTP) e a conversão: um parser JSON próprio, que nunca passa um número
por `float64` e nunca falha — o pior resultado é um envelope
`format: "malformed"` com o documento em `raw`.

```
Docker gelf driver, fluent-bit, Logstash, graypy, logstash-gelf ...
   │  UDP :12201 (chunks, gzip/zlib)   TCP :12201 (\0)   HTTP :12202 POST /gelf
   │  TCP+TLS / HTTPS (opcionais, sugestão :12203 / :12204)
   ▼
go-gelf-server ──JLH type=gelf──▶ NATS | Redis | SQLite | PostgreSQL | MySQL | ClickHouse
```

## Em uma página

| | |
|---|---|
| Transportes | `listen.udp` (`:12201`), `listen.tcp` (`:12201`), `listen.http` (`:12202`); `listen.tls` e `listen.https` desligados até haver certificado; PROXY protocol v1/v2 opcional nos de conexão |
| Envelope | JLH `type=gelf`, payload em kebab-case validado por `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json` (SPEC §3.2): `host`, `transport`, `compression`, `chunks`, `level {code,name}`, `timestamp`, `short-message`, `full-message`, `fields` (os `_campos`), `extra`, `size`, `raw`, `parse` |
| `level` ausente | `6` (info) com aviso `level-default` — desvio deliberado da GELF 1.1 (D1) |
| Tamanho | `convert.max_message_bytes` = **128 KiB** do documento descomprimido; acima disso a mensagem é **recusada e contada**, nunca truncada (HTTP `413`) |
| Rings | `memory` (padrão), `sqlite` (com `fsync`), `redis` (compartilhado entre réplicas), `none` |
| Confirmação | UDP: nenhuma (perda contada em `gelf_messages_dropped_total`); TCP/TLS: continuar lendo (ring cheio = parar de ler); HTTP/HTTPS: `202` depois do ring, `503` + `Retry-After` quando não dá agora |
| Filtro / rate-limit | por `level` (`filter.min_level`, `filter.levels`) e por prefixo de origem; recarregáveis por `SIGHUP` |
| Operação | `/healthz`, `/readyz`, `/metrics` (prefixo `gelf_`) em `127.0.0.1:9104` |

## Rodar

```bash
go build -o go-gelf-server ./cmd/go-gelf-server
mkdir -p /tmp/gls-data
DATADIR=/tmp/gls-data AGENT_NAME=go-gelf-server HOSTNAME=dev-mac \
  ./go-gelf-server --sink.type=sqlite --listen.udp=127.0.0.1:12201 \
  --listen.tcp=127.0.0.1:12201 --listen.http=127.0.0.1:12202
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

Os eventos ficam em `$DATADIR/events.db`, tabela `gelf_events`
(`sqlite3 /tmp/gls-data/events.db 'select env from gelf_events'`).

Configuração: INI (`GELF_CONFIG`), flags (`--secao.chave`) e env
(`GELF_<SEÇÃO>_<CHAVE>`), nessa precedência invertida (flag > env > INI >
padrão); `--help` lista tudo e `--dump-config` mostra o efetivo. A tabela
completa está na SPEC §16.5.

Com Docker: [`docker/README.md`](docker/README.md) — imagem
`tmsoftbrasil/go-gelf-server`, um compose por combinação ring × destino e um
emissor fluent-bit de demonstração (UDP com chunks e gzip, e TCP).

## Documentos

| Arquivo | Conteúdo |
|---|---|
| [`docs/DECISOES.md`](docs/DECISOES.md) | As decisões que definiram este software (entrevista da família GELF de 2026-10-31 e as da implementação) |
| [`docs/SPEC.md`](docs/SPEC.md) | A especificação, fonte da verdade; §4–§6 são o GELF |
| [`docs/TESTING.md`](docs/TESTING.md) | Como testar, o que cada suíte prova, a rodada de emissores reais e o que ainda não está provado |
| [`AGENTS.md`](AGENTS.md) | Instruções para agentes de IA (e para quem vai mexer no código) |
