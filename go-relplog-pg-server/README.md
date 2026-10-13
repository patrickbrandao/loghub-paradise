# go-relplog-pg-server

Servidor de syslog por **RELP** do ecossistema **LogHub**, enxuto. Recebe o que
o `rsyslog` envia com o `omrelp`, em claro, confirma **cada mensagem** só depois
de ela estar no ring em memória, converte cada uma num envelope **JLH**
`type=syslog` e o enfileira na `loghub.inbox` do PostgreSQL.

É um **corte** do [`go-relplog-server`](../go-relplog-server/) (RELP em claro e
sobre TLS, quatro rings, sete destinos, filtro, rate-limit, beacon) no molde do
[`go-syslog-pg-server`](../go-syslog-pg-server/): um ring (memória), um destino
(PostgreSQL), emergência em JSON Lines, **sem TLS** e com três dependências de
produção. A recepção RELP é a do pai, intacta.

```
rsyslog (omrelp) ──RELP :20514──▶ go-relplog-pg-server ──JLH──▶ PostgreSQL (loghub.inbox)
                 ◀── 200 OK por mensagem, só depois do ring
```

## Em uma página

| | |
|---|---|
| Transporte | `listen.tcp` (`:20514`, RELP em claro); PROXY protocol v1/v2 opcional. Sem TLS |
| Envelope | JLH com `payload.transport` = `tcp`, `framing` = `octet-counting` e a extensão `"x-transport": "relp"` |
| Ring | memória (`ring.capacity_items`, `ring.max_bytes`) |
| Destino | a fila `loghub.inbox` (PostgreSQL 18, `INSERT ... ON CONFLICT (uuid) DO NOTHING`), sempre `sslmode=disable`; configurada por `[sink.postgres]` ou `LOGHUB_EB_URL` (`postgres://`) |
| Garantia | `200 OK` só depois do ring; `SIGTERM` drena, FATAL do banco vai para o dump `.jsonl` e volta no boot seguinte; `kill -9` perde o backlog |
| Ring cheio / banco fora | a transação é **segurada**; passando `relp.backpressure_timeout` (30 s), a sessão fecha **sem** resposta e o rsyslog retransmite — nunca um `500`, que o `librelp` descartaria |
| `500` | só para um cliente fora do protocolo (`syslog` antes do `open`, segundo `open`, comando desconhecido) e para a guarda de origem, que um par TCP não alcança |
| Memória | teto global das cópias em voo (`relp.inflight_max_bytes`, 64 MiB), janela por conexão (`relp.window`, 128) |
| Operação | `/healthz`, `/readyz`, `/metrics` (prefixo `relplog_`) em `127.0.0.1:9103`; `SIGHUP` recarrega `log.level`, `log.access` e `log.access_sample` (o `log.format` exige restart) |

## Rodar

A fila precisa existir: o servidor nunca faz DDL (`docker/postgres/inbox.sql`).

```bash
go build -o go-relplog-pg-server ./cmd/go-relplog-pg-server
mkdir -p /tmp/rlpg-data
DATADIR=/tmp/rlpg-data LOGHUB_EB_URL='postgres://syslog:syslog@127.0.0.1:5432/syslog' \
  ./go-relplog-pg-server --listen.tcp=127.0.0.1:20514
```

No rsyslog (`rsyslog-relp` instalado):

```
module(load="omrelp")
action(type="omrelp" target="127.0.0.1" port="20514"
       queue.type="LinkedList" queue.size="100000"
       action.resumeRetryCount="-1")
```

Com Docker: [`docker/README.md`](docker/README.md) (o servidor, um PostgreSQL 18
com a fila e um emissor rsyslog de demonstração).

## Documentos

| Arquivo | Conteúdo |
|---|---|
| [`docs/DECISOES.md`](docs/DECISOES.md) | As decisões do corte, com o porquê, e as herdadas dos dois pais |
| [`docs/SPEC.md`](docs/SPEC.md) | A especificação, fonte da verdade, com a numeração do pai; §5 é o RELP |
| [`docs/TESTING.md`](docs/TESTING.md) | Como testar, o que cada suíte prova e o que ainda não está provado |
| [`AGENTS.md`](AGENTS.md) | Instruções para agentes de IA (e para quem vai mexer no código) |
