# go-relplog-server

Servidor de syslog por **RELP** do ecossistema **LogHub**. Recebe o que o
`rsyslog` envia com o `omrelp` — em claro ou sobre TLS —, confirma **cada
mensagem** só depois de ela estar guardada segundo a garantia configurada,
converte cada uma num envelope **JLH** `type=syslog` e entrega a um destino:
NATS JetStream (padrão), Redis, SQLite, PostgreSQL (`loghub.inbox`), MySQL,
ClickHouse ou nenhum.

É o irmão do [`go-syslog-server`](../go-syslog-server/) (UDP, TCP, TLS, unix):
mesma conversão, mesmos rings, mesmos destinos, mesmo `type` e mesmo subject
(`loghub.{space}.syslog`). O que muda é a recepção: o RELP é o único transporte
de syslog difundido em que o emissor sabe **qual** mensagem foi aceita.

```
rsyslog (omrelp) ──RELP :20514 / TLS──▶ go-relplog-server ──JLH──▶ NATS | Redis | SQLite | PostgreSQL | MySQL | ClickHouse
                  ◀── 200 OK por mensagem, só depois do ring
```

## Em uma página

| | |
|---|---|
| Transportes | `listen.tcp` (`:20514`, RELP em claro) e `listen.tls` (RELP sobre TLS, mTLS opcional); PROXY protocol v1/v2 opcional |
| Envelope | JLH com `payload.transport` = `tcp`/`tls`, `framing` = `octet-counting` e a extensão `"x-transport": "relp"` |
| Rings | `memory` (padrão), `sqlite` (com `fsync`), `redis` (compartilhado entre réplicas), `none` |
| Garantia | `200 OK` só depois do ring (ou do destino, com `ring=none`) |
| Ring cheio ou indisponível / destino fora | a transação é **segurada**; passando `relp.backpressure_timeout` (30 s), a sessão fecha **sem** resposta e o rsyslog retransmite — nunca um `500`, que o `librelp` descartaria |
| Rate-limit | o emissor acima do limite é **desacelerado** (a transação espera uma ficha), nunca recusado |
| `500` | só para a recusa definitiva do destino (`ring=none`), rara, e para um cliente fora do protocolo (`syslog` antes do `open`, segundo `open`, comando desconhecido): o `librelp` descarta a mensagem e quebra a sessão |
| Memória | teto global das cópias em voo (`relp.inflight_max_bytes`, 64 MiB), janela por conexão (`relp.window`, 128) |
| Operação | `/healthz`, `/readyz`, `/metrics` (prefixo `relplog_`) em `127.0.0.1:9103`; `SIGHUP` recarrega filtro, rate-limit, log e `sink.sqlite.keep_files` |

## Rodar

```bash
go build -o go-relplog-server ./cmd/go-relplog-server
mkdir -p /tmp/rls-data
DATADIR=/tmp/rls-data ./go-relplog-server --sink.type=sqlite --listen.tcp=127.0.0.1:20514
```

No rsyslog (`rsyslog-relp` instalado):

```
module(load="omrelp")
action(type="omrelp" target="127.0.0.1" port="20514"
       queue.type="LinkedList" queue.size="100000"
       action.resumeRetryCount="-1")
```

Com Docker: [`docker/README.md`](docker/README.md) (todas as combinações ring ×
destino e um emissor rsyslog de demonstração).

## Documentos

| Arquivo | Conteúdo |
|---|---|
| [`docs/DECISOES.md`](docs/DECISOES.md) | As decisões que definiram este software, com o porquê e o que o `librelp` mostrou |
| [`docs/SPEC.md`](docs/SPEC.md) | A especificação, fonte da verdade; §5 é o RELP |
| [`docs/TESTING.md`](docs/TESTING.md) | Como testar, o que cada suíte prova e o que ainda não está provado |
| [`AGENTS.md`](AGENTS.md) | Instruções para agentes de IA (e para quem vai mexer no código) |
