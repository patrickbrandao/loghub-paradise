# go-gelf-nats-server

Servidor de ingestão **GELF** (Graylog Extended Log Format 1.1) do ecossistema
**LogHub**, cortado para o NATS. Recebe documentos GELF por UDP (com chunks e
gzip/zlib), TCP (moldura `\0`) e HTTP (`POST /gelf`), converte cada um num
envelope **JLH** `type=gelf` e o publica no NATS em `loghub.{space}.gelf`
(JetStream com `PubAck` por padrão, `core` opcional).

É para o [`go-gelf-server`](../go-gelf-server/) exatamente o que o
[`go-syslog-nats-server`](../go-syslog-nats-server/) é para o
`go-syslog-server`: ring só em memória, destino só NATS, heartbeat beacon,
dump de emergência em JSON Lines, **sem TLS em lugar nenhum**, sem filtro e sem
rate-limit — três dependências diretas e um binário de 12 MB (contra 30 MB do
completo). A conversão GELF (parser JSON próprio, números sem `float64`, nunca
falha) é a mesma, copiada literalmente.

```
Docker gelf driver, fluent-bit, Logstash, graypy, logstash-gelf ...
   │  UDP :12201 (chunks, gzip/zlib)   TCP :12201 (\0)   HTTP :12202 POST /gelf
   ▼
go-gelf-nats-server ──JLH type=gelf──▶ NATS loghub.<space>.gelf ──▶ stream LOGHUB_GELF
                    └─beacon (NATS core)──▶ loghub._ctl.beacon
```

## Em uma página

| | |
|---|---|
| Transportes | `listen.udp` (`:12201`), `listen.tcp` (`:12201`), `listen.http` (`:12202`); PROXY protocol v1/v2 opcional no TCP e no HTTP. Sem TLS/HTTPS |
| Envelope | JLH `type=gelf`, payload em kebab-case (`jlh-gelf-payload.schema.json`): `host`, `transport`, `compression`, `chunks`, `level {code,name}`, `timestamp`, `short-message`, `full-message`, `fields`, `extra`, `size`, `raw`, `parse` |
| Destino | NATS: `jetstream` (padrão; `PubAck`, `Nats-Msg-Id = <uuid>.0`) ou `core`. O stream `LOGHUB_GELF` (`docker/nats/stream.json`) é criado pelo operador, nunca pelo servidor. Só `nats://` |
| Tamanho | `convert.max_message_bytes` = **128 KiB** do documento descomprimido; acima, recusa contada (HTTP `413`), nunca truncamento |
| Confirmação | UDP: nenhuma (perda contada); TCP: continuar lendo (ring cheio = parar de ler); HTTP: `202` depois do ring, `503` + `Retry-After` quando não dá agora |
| Emergência | FATAL do NATS ou dreno estourado: o ring vira `emg-*.jsonl` em `/data/emergency`, reingerido no boot seguinte |
| Operação | `/healthz`, `/readyz`, `/metrics` (prefixo `gelf_`) em `127.0.0.1:9104`; `SIGHUP` recarrega `[log]` |

## Rodar

```bash
go build -o go-gelf-nats-server ./cmd/go-gelf-nats-server
nats --server nats://127.0.0.1:4222 stream add LOGHUB_GELF --config docker/nats/stream.json
mkdir -p /tmp/glnats-data
DATADIR=/tmp/glnats-data AGENT_NAME=go-gelf-nats-server HOSTNAME=dev-mac \
  LOGHUB_EB_URL=nats://127.0.0.1:4222 ./go-gelf-nats-server
```

Mandando GELF:

```bash
printf '{"version":"1.1","host":"dev","short_message":"oi","level":4}' | nc -u -w1 127.0.0.1 12201
printf '{"version":"1.1","host":"dev","short_message":"oi"}\0' | nc -w1 127.0.0.1 12201
printf '{"version":"1.1","host":"dev","short_message":"oi","_pedido":42}' | gzip |
  curl -sS -w '%{http_code}\n' -H 'Content-Encoding: gzip' --data-binary @- http://127.0.0.1:12202/gelf
docker run --rm --log-driver=gelf --log-opt gelf-address=udp://127.0.0.1:12201 alpine echo oi
nats --server nats://127.0.0.1:4222 stream view LOGHUB_GELF
```

Configuração: flags (`--secao.chave`) > env (`GELF_<SEÇÃO>_<CHAVE>`) > INI
(`GELF_CONFIG`) > padrão; `--help` lista tudo e `--dump-config` mostra o
efetivo. As chaves do `go-gelf-server` que não existem aqui são recusadas na
linha de comando e no INI, e avisadas (`WARN`) no ambiente.

Com Docker: [`docker/README.md`](docker/README.md) — imagem
`tmsoftbrasil/go-gelf-nats-server`, o compose com NATS JetStream e o emissor
fluent-bit de demonstração (`--profile demo`).

## Documentos

| Arquivo | Conteúdo |
|---|---|
| [`docs/DECISOES.md`](docs/DECISOES.md) | As decisões da família GELF que valem aqui e as tomadas no corte |
| [`docs/SPEC.md`](docs/SPEC.md) | A especificação, fonte da verdade (numeração do `go-gelf-server`) |
| [`docs/TESTING.md`](docs/TESTING.md) | Como testar, o que cada suíte prova, a rodada de emissores reais e o que ainda não está provado |
| [`AGENTS.md`](AGENTS.md) | Instruções para agentes de IA (e para quem vai mexer no código) |
