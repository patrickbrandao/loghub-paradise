# go-relplog-nats-server

Servidor de syslog por **RELP** do ecossistema **LogHub**, com destino único no
**NATS**. Recebe o que o `rsyslog` envia com o `omrelp`, **em claro**, confirma
**cada mensagem** só depois de ela estar no ring em memória, converte cada uma
num envelope **JLH** `type=syslog` e o publica em `loghub.{space}.syslog` — por
padrão num stream JetStream, com `PubAck` por mensagem.

É o corte do [`go-relplog-server`](../go-relplog-server/) no molde do
[`go-syslog-nats-server`](../go-syslog-nats-server/): o mesmo RELP (respostas,
janela, teto em voo, fim de sessão), com ring só em memória, sink só no NATS,
dump de emergência em JSON Lines, **sem TLS** em lugar nenhum, sem filtro e sem
rate-limit. Três dependências diretas, binário de 12,6 MB. Mesmo `type`, mesmo
subject e mesmo stream (`LOGHUB_SYSLOG`) do `go-syslog-server` e do
`go-relplog-server`.

```
rsyslog (omrelp) ──RELP :20514──▶ go-relplog-nats-server ──JLH──▶ NATS JetStream (loghub.<space>.syslog)
                 ◀── 200 OK por mensagem, só depois do ring
```

## Em uma página

| | |
|---|---|
| Transporte | `listen.tcp` (`:20514`, RELP em claro); PROXY protocol v1/v2 opcional. Sem RELP sobre TLS: para cifrar, um terminador TLS na frente, com PROXY protocol |
| Envelope | JLH com `payload.transport` = `tcp`, `framing` = `octet-counting` e a extensão `"x-transport": "relp"` |
| Ring | só `memory` |
| Destino | só NATS: `jetstream` (padrão; o stream é criado pelo operador e validado no boot e em toda checagem) ou `core` (*best effort*); sempre texto claro, só `nats://` |
| Garantia | `200 OK` só depois do ring; FATAL do NATS e `SIGTERM` não perdem (dump e reingestão); `kill -9` e OOM perdem o backlog do ring |
| Ring cheio / NATS fora | a transação é **segurada**; passando `relp.backpressure_timeout` (30 s), a sessão fecha **sem** resposta e o rsyslog retransmite — nunca um `500` |
| `500` | no fluxo normal, **nenhum**; só para um cliente fora do protocolo (`syslog` antes do `open`, segundo `open`, comando desconhecido) e para a guarda de origem não endereçável |
| Memória | teto global das cópias em voo (`relp.inflight_max_bytes`, 64 MiB), janela por conexão (`relp.window`, 128) |
| Operação | `/healthz`, `/readyz`, `/metrics` (prefixo `relplog_`) em `127.0.0.1:9103` (a mesma porta do `go-relplog-server` e do `go-relplog-pg-server`: a regra da família); heartbeat em `loghub._ctl.beacon`; `SIGHUP` recarrega o `[log]` |

## Rodar

```bash
go build -o go-relplog-nats-server ./cmd/go-relplog-nats-server
mkdir -p /tmp/rlnats-data
DATADIR=/tmp/rlnats-data AGENT_NAME=go-relplog-nats-server HOSTNAME=dev-mac \
  ./go-relplog-nats-server --listen.tcp=127.0.0.1:20514 \
  --sink.nats.urls=nats://127.0.0.1:4222
```

O stream precisa existir (`nats stream add LOGHUB_SYSLOG --config
docker/nats/stream.json`); o servidor o valida e nunca o cria. No rsyslog
(`rsyslog-relp` instalado):

```
module(load="omrelp")
action(type="omrelp" target="127.0.0.1" port="20514"
       queue.type="LinkedList" queue.size="100000"
       action.resumeRetryCount="-1")
```

Com Docker: [`docker/README.md`](docker/README.md) (servidor, NATS com o stream
criado e um emissor rsyslog de demonstração).

## Documentos

| Arquivo | Conteúdo |
|---|---|
| [`docs/DECISOES.md`](docs/DECISOES.md) | As decisões do corte, com o porquê, e a conferência das correções dos dois pais |
| [`docs/SPEC.md`](docs/SPEC.md) | A especificação, fonte da verdade; numeração do `go-relplog-server`, §5 é o RELP e §10.7 o NATS |
| [`docs/TESTING.md`](docs/TESTING.md) | Como testar, o que cada suíte prova e o que ainda não está provado |
| [`AGENTS.md`](AGENTS.md) | Instruções para agentes de IA (e para quem vai mexer no código) |
