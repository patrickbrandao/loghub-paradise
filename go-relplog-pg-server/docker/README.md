# go-relplog-pg-server em contêiner

Uma combinação só (docs/SPEC.md §2.1): **RELP do rsyslog → ring em memória →
PostgreSQL**, a fila `loghub.inbox` do LogHub. O `docker-compose.yml` sobe o
servidor e um PostgreSQL 18 com a fila já criada; com `--profile demo`, também
um rsyslog de verdade mandando linhas pelo `omrelp`.

```bash
cd docker
cp .env.example .env            # opcional: todo valor tem default
docker compose up -d --build
docker compose logs -f relplog
docker compose --profile demo up -d --build      # mais o rsyslog de demonstração
```

| Publicado no host | Padrão | Variável |
|---|---|---|
| RELP em claro | `20580` | `RELPLOG_PORT` |
| admin (`/healthz`, `/readyz`, `/metrics`) | `127.0.0.1:19380` | `RELPLOG_ADMIN_PORT` |
| PostgreSQL | `127.0.0.1:15380` | `RELPLOG_POSTGRES_PORT` |

Os padrões não coincidem com nenhuma porta publicada pelos compose do
`go-relplog-server` (RELP `20514+n`, admin `9300+n`), do `go-syslog-pg-server`
(5180, 19180, 15480) nem do `go-relplog-nats-server` (20581, 19381, 14381), para
todos rodarem no mesmo host. Dentro do contêiner a porta é a padrão do RELP,
20514.

| Pasta | Montada em | Para quê |
|---|---|---|
| `conf/` | `/etc/relplog:ro` | `relplog.ini`, as chaves recarregáveis ([abaixo](#configuração-e-recarga-sighup)) |
| `postgres/inbox.sql` | `/docker-entrypoint-initdb.d` do PostgreSQL | a DDL da fila, aplicada na criação do volume |
| `rsyslog/` | o serviço `emitter` | o rsyslog de demonstração (`Dockerfile`, `rsyslog.conf`, `demo.sh`) |
| volume `rlpg-data` | `/data` | a única área gravável: identidade e `emergency/` |

**Não há TLS** (SPEC §1.2): nem RELP sobre TLS, nem conexão cifrada com o
PostgreSQL. A imagem não traz `ca-certificates` nem a porta 20515. Quem precisa
de RELP cifrado põe um terminador TLS na frente (com PROXY protocol,
`RELPLOG_PROXY_PROTOCOL=optional`, para manter o `src-ip`) ou usa o
`go-relplog-server`.

## Apontar um rsyslog real

O cliente é o `omrelp` (pacote `rsyslog-relp` no Alpine, Debian/Ubuntu e RHEL).
Um arquivo em `/etc/rsyslog.d/`, com a porta **publicada no host** (20580 por
padrão). `rsyslog/rsyslog.conf` é o mesmo modelo, comentado.

```
module(load="omrelp")
action(type="omrelp" name="relp-loghub"
       target="logs.exemplo" port="20580"
       # a fila que nao perde: RAM transbordando para disco, gravada no shutdown
       queue.type="LinkedList" queue.filename="relp-loghub"
       queue.size="100000" queue.maxDiskSpace="1g" queue.saveOnShutdown="on"
       # retentar para sempre; sem isto o rsyslog descarta ao desistir
       action.resumeRetryCount="-1" action.resumeInterval="5")
```

**Sem `tls="on"`**: este servidor só fala RELP em claro. `global(workDirectory=
"/var/spool/rsyslog")` precisa existir (é o padrão das distribuições) para a fila
em disco. Uma ação sem filtro (`*.*`) encaminha também as mensagens do próprio
`rsyslogd` (facility `syslog`), inclusive os erros do `omrelp` sobre a própria
ação: envolva-a num filtro do que deve ir (`if prifilt("local0.*") then {
action(...) }`, como em `rsyslog/rsyslog.conf`) ou exclua `syslog.*`.

| `omrelp` | Padrão | Servidor | Por quê |
|---|---|---|---|
| `windowSize` | 128 | `RELPLOG_RELP_WINDOW` (128) | transações sem resposta antes de o cliente parar de mandar |
| `timeout` | 90 s | `RELPLOG_RELP_BACKPRESSURE_TIMEOUT` (30 s) | o servidor fecha a sessão segurada **antes** de o cliente desistir sozinho; mantenha o do servidor abaixo |
| `rebindInterval` | 0 | — | reconectar a cada N mensagens espalha a carga atrás de um balanceador |
| `template` | `RSYSLOG_ForwardFormat` | — | RFC 3164 com timestamp de alta precisão; `RSYSLOG_SyslogProtocol23Format` manda RFC 5424 |

## O que o rsyslog vê

O `librelp` faz exatamente três coisas com uma resposta (SPEC §5.4):

| O servidor | O rsyslog | Quando |
|---|---|---|
| `200 OK` | apaga a mensagem da fila: **entregue** | a mensagem está na RAM deste nó; também para a vazia, que não tem o que entregar |
| nenhuma resposta **enquanto dura** | para de mandar quando a janela (128) enche; as mensagens esperam **na fila dele** | ring cheio ou PostgreSQL DOWN |
| nenhuma resposta, e a sessão fecha (`serverclose` ou socket) | reconecta e **retransmite** tudo o que não teve `200` | a espera acima passou de `relp.backpressure_timeout` (30 s); shutdown; frame malformado; ociosidade (`idle_timeout`) |
| `500 ...` | **destrói** essa mensagem e loga `peer sent error response`; suspende a ação e quebra a sessão | nunca para uma mensagem bem formada vinda de um par TCP. Só fora do protocolo: `500 command disabled` (`syslog` antes do `open`), `500 protocol error: connection already open` (segundo `open`), `500 command not supported` |

Consequências:

- **Nada do que passa sozinho recebe `500`.** Ring cheio e banco fora
  **seguram**: a janela enche, o servidor para de ler, o TCP segura o rsyslog e
  as mensagens esperam na fila dele (por isso a fila em disco e
  `action.resumeRetryCount="-1"`). Passados 30 s sem escoar, o servidor fecha a
  sessão; o rsyslog reconecta e retransmite.
- **Não há `500 refused by the destination`**: no pai ele só existia com
  `ring=none`. Aqui uma recusa `logic` da fila acontece no worker, **depois** do
  `200`, e é um descarte contado em `relplog_events_dropped_total{reason="sink_logic"}`
  com `ERROR` e os `uuid` (SPEC §2.3).
- **Uma retransmissão é mensagem nova**: o RELP não tem id de mensagem, e o
  envelope sai com `uuid` novo. É at-least-once (SPEC §19, "Duplicatas").
- A reconciliação do lado do servidor (§5.4):
  `relplog_messages_total = accepted + dropped{empty,unaddressable} + relp_unanswered_total`.

## Emissor de demonstração

Com `--profile demo` sobe o serviço `emitter`: um rsyslog do Alpine 3.22 com
`rsyslog-relp` (imagem construída de `rsyslog/Dockerfile`), que manda pelo
`omrelp` ao serviço `relplog` na porta do contêiner (20514).

```bash
docker compose --profile demo up -d --build
docker logs -f rlpg-demo-emitter                  # rsyslogd e o gerador
curl -s http://127.0.0.1:19380/metrics | grep -E '^relplog_messages_(total|accepted_total)'
docker compose exec postgres psql -U syslog -d syslog -c \
  "SELECT count(*) FROM loghub.inbox WHERE payload->>'message' LIKE 'seq=%'"
docker compose --profile demo down -v
```

- `rsyslog/demo.sh` sobe o `rsyslogd` e manda, pelo `logger` (`/dev/log` →
  `imuxsock`), uma rajada de `RELPLOG_DEMO_BURST` (100) linhas e depois uma a
  cada `RELPLOG_DEMO_INTERVAL` (1) segundo, com TAG `rlpg-demo` e `seq=N` na
  mensagem: um buraco na sequência é perda, um `seq` repetido é retransmissão.
- A fila do `omrelp` é disk-assisted, num volume (`rlpg-emitter-spool`), gravada
  no `SIGTERM`, com `action.resumeRetryCount="-1"`. O emissor sobe sem esperar o
  servidor, de propósito: os primeiros segundos do log mostram
  `could not connect to remote server` e `action 'relp-loghub' suspended`, e
  depois `resumed`, sem nenhuma linha perdida.

## Configuração e recarga (SIGHUP)

| Onde | O quê | Quando vale |
|---|---|---|
| env: `common.yml`, `docker-compose.yml`, `.env` | tudo o que **exige restart** (listen, relp, ring, sink, `[syslog]`, shutdown...) | no `up` que recria o contêiner |
| `conf/relplog.ini`, lido via `RELPLOG_CONFIG=/etc/relplog/relplog.ini` | **só** as recarregáveis (SPEC §16.3): `log.level`, `log.access`, `log.access_sample` | no `SIGHUP` |

```bash
vi conf/relplog.ini                      # ex.: [log] level = debug
docker kill -s HUP rlpg                  # ou: docker compose kill -s HUP relplog
docker logs --since 1m rlpg 2>&1 | grep SIGHUP
```

| Linha do log | Significado |
|---|---|
| `SIGHUP: applied` com `keys=[...]` | trocado a quente |
| `SIGHUP: configuration unchanged` | nada mudou no arquivo |
| `ERROR SIGHUP: the new configuration is invalid and was NOT applied` | arquivo inválido (inclusive uma seção do `go-relplog-server` que não existe aqui, como `[filter]` ou `[tls]`); o servidor segue com a config anterior |
| `WARN SIGHUP: this key requires a restart and was ignored` | uma chave de restart foi posta no INI (e o env não a define); repete em todo HUP até o restart |
| `WARN SIGHUP: this key is set in the config file, but an environment variable or a flag decides its value` | a chave também está no env, que vence |

**Por que não pelo env.** Env vence o INI (SPEC §16.1) e o env de um contêiner
não muda depois do `up`: uma chave recarregável definida por env nunca mudaria
com o `HUP`. Por isso nenhum arquivo deste diretório define `RELPLOG_LOG_LEVEL`,
`RELPLOG_LOG_ACCESS` ou `RELPLOG_LOG_ACCESS_SAMPLE`.

**A pasta, não o arquivo.** O bind é de `conf/`, não de `conf/relplog.ini`: um
bind de arquivo prende o inode, e um editor que salva por *rename* deixaria o
contêiner lendo a versão antiga para sempre.

**Variáveis do `go-relplog-server`.** Um `.env` compartilhado com o pai pode ter
`RELPLOG_LISTEN_TLS`, `RELPLOG_FILTER_*`, `RELPLOG_SINK_TYPE`... O compose só
repassa as variáveis que conhece; se uma delas chegar ao contêiner por outro
caminho, o boot emite **um** `WARN` que as nomeia e diz que são ignoradas. Na
linha de comando ou no INI elas são parâmetro desconhecido: exit 1.

## Identidade LogHub

O servidor chama o `go-loghub-ident` no boot. No contêiner:

| Variável | Valor | Por quê |
|---|---|---|
| `DATADIR` | `/data` (da imagem) | `agent_uuid` e `machine_id` são gerados e persistidos ali, junto de `emergency/` |
| `AGENT_NAME` | `go-relplog-pg-server` (da imagem) | sem ele o nome viria de `argv[0]` |
| `HOSTNAME` | `rlpg` (o `hostname:` do serviço) | sem ele seria o id do contêiner, que muda a cada recriação |
| `WORKSPACE` | `RELPLOG_WORKSPACE` do `.env` (`default`) | o **tenant**: vira o `space` de todo envelope. O compose nunca lê o `WORKSPACE` do shell, que o Jenkins usa para o caminho do job |

A conexão aparece em `pg_stat_activity` como
`application_name = rlpg/go-relplog-pg-server@rlpg`.

**Um volume por instância.** O `from` de cada envelope é o `agent_uuid` gravado no
volume. Duas réplicas montando o mesmo `/data` enfileiram o mesmo `from`, sem
aviso.

Falhas de identidade encerram o processo com os códigos 100–114 do
`go-loghub-ident` v0.5.2, com a causa em stderr (`lib-loghub-ident:
<VARIÁVEL>: <motivo>`). `AGENT_NAME`, `HOSTNAME` e `WORKSPACE` são passados
para minúsculas antes da validação.

| Código | Variável | Causa |
|---|---|---|
| 100 | `DATADIR` | diretório ausente, que não é diretório, caminho relativo, sem permissão ou erro de I/O, link simbólico recusado; leitura de um arquivo de identidade em `/data` que falha |
| 100 | `MACHINE_ID_FILE` | caminho relativo ou com caractere de controle, inacessível, ilegível, ou que não é arquivo comum utilizável (diretório, fifo, device, mais de 4 KiB). Inexistente **não** é erro: cai para o padrão do sistema |
| 102 | `MACHINE_ID` | env presente fora de `^[0-9a-f]{32}$` |
| 103 | `AGENT_NAME` | env vazia, sem `/data/agent_name`, e `argv[0]` vazio |
| 104 | `AGENT_NAME` | env ou `/data/agent_name` fora de `^[a-z0-9._-]+$`, `.`/`..`, ou acima de 64 |
| 105 | `AGENT_UUID` | a geração do UUIDv7 falhou |
| 106 | `AGENT_UUID` | gravação de `/data/agent_uuid` falhou (volume sem permissão para o uid 65532) |
| 107 | `AGENT_UUID` | valor fora do UUIDv7 canônico |
| 108 | `HOSTNAME` | env vazia e `os.Hostname()` falhou |
| 109 | `HOSTNAME` | fora da RFC 1123: `_`, ponto final ou rótulo vazio, hífen na borda de um rótulo, rótulo acima de 63, nome acima de 253 |
| 111 | `WORKSPACE` | env ou `/data/workspace` fora de `^[a-z0-9.-]+$` (use hífen, não `_`), `.`/`..`, ou acima de 64 |
| 112 | — | `Initialize()` duas vezes (bug) |
| 113 | `MACHINE_ID` | gravação de `/data/machine_id` falhou |
| 114 | `MACHINE_ID` | a geração do UUIDv7 base do machine-id falhou |

O próprio servidor sai com 1 por um `WORKSPACE` que a biblioteca aceita mas a
convenção LogHub não: um segmento vazio, como `a..b`, `.x` ou `x.`.

## PostgreSQL: a fila `loghub.inbox`

O servidor **enfileira** na `loghub.inbox`, a fila comum de envelopes JLH do
LogHub (SPEC §3.7.1). O serviço `postgres` (PostgreSQL 18) monta
`postgres/inbox.sql` — a DDL literal da skill `loghub-postgres-table-inbox`, que
um teste confere byte a byte — em `/docker-entrypoint-initdb.d`, que a imagem
executa **só na criação do volume**. O servidor **nunca** faz DDL: valida a
tabela no boot e sai com 3 se algo faltar.

**Sem TLS.** A conexão é sempre `sslmode=disable`, e `PGSSLMODE` (como todo o
ambiente `PG*`) é ignorado. No compose o banco está na rede interna; fora dele,
use uma rede confiável ou um socket unix
(`RELPLOG_SINK_POSTGRES_ADDR=/var/run/postgresql`).

O consumo é de outro software, sempre em ordem de `uuid` e retirando a linha na
mesma transação que grava o resultado:

```sql
WITH job AS (
  SELECT uuid FROM loghub.inbox ORDER BY uuid LIMIT 100 FOR UPDATE SKIP LOCKED
)
DELETE FROM loghub.inbox i USING job WHERE i.uuid = job.uuid RETURNING i.*;
```

Tamanho e atraso da fila:

```bash
docker compose exec postgres psql -U syslog -d syslog -c \
  "SELECT count(*) AS na_fila, now() - min(received_at) AS atraso FROM loghub.inbox"
```

## LOGHUB_EB_URL

`LOGHUB_EB_URL` (SPEC §10.9) é a URL do eventbus da convenção LogHub. Aqui só
`postgres://` e `postgresql://` são aceitas; qualquer outro esquema é exit 1.

O compose não a repassa por padrão: um `LOGHUB_EB_URL` exportado no shell
colidiria com as chaves de conexão do arquivo, e o boot sairia com 1. Para
usá-la, edite o `environment:` do serviço `relplog` no `docker-compose.yml`:
apague as quatro chaves que ela substitui e descomente a linha da URL.

```bash
echo "LOGHUB_EB_URL=postgres://syslog:syslog@postgres:5432/syslog" >> .env
docker compose up -d --build
```

- **Sem `sslmode` que peça TLS**: `require`, `verify-ca` e `verify-full` são
  **exit 1**. Omita o `sslmode`, ou use `disable` (`allow` e `prefer` também
  passam).
- **Uma fonte só**: com a URL, `RELPLOG_SINK_POSTGRES_ADDR`, `_USERNAME`,
  `_PASSWORD` ou `_DATABASE` definidas também são exit 1, **mesmo que a URL não
  traga aquela parte**. `_TABLE` e `_POOL_SIZE` convivem com a URL.
- **Usuário e banco são obrigatórios** na URL (`postgres://usuario:senha@host/banco`):
  sem um deles é exit 1 (SPEC §10.9).
- **Senha com `/`, `?` ou `#`**: escape como `%2F`, `%3F`, `%23`. Sem o escape
  a URL é recusada (exit 1) quando um `@` aparece depois do caractere, ou
  quando ele abre um fragmento; o erro nunca cita a URL. Um `@` cru na senha
  **passa** (o usuário vai até o último `@`), mas escape-o assim mesmo
  (`%40`): outras ferramentas leem a URL de outro jeito.
- **Um servidor só**; socket unix com `?host=/dir` (um diretório só, sem `,` nem
  `:`; a porta vai na URL: `postgres://u:s@:5433/banco?host=/run/pg`). Parâmetro
  de consulta desconhecido, repetido ou um `sslmode` vazio, e fragmento
  (`#...`), são recusados. Uma `LOGHUB_EB_URL` só com espaços também.

## Arquivo de emergência (`.jsonl`)

O ring é memória. Se o banco ficar inutilizável (FATAL, SPEC §13.3) ou o dreno
do `SIGTERM` estourar `RELPLOG_SHUTDOWN_DRAIN_TIMEOUT`, o que estava pendente é
gravado em `/data/emergency/emg-<agent_uuid>-<unixnano>.jsonl` e o processo sai
com 3. **Um envelope JLH pronto por linha**, byte a byte o que o worker teria
escrito na fila. Essas mensagens já tinham `200 OK`: o rsyslog não as tem mais.

No boot seguinte, antes de abrir o RELP, todo `emg-*.jsonl` volta para a fila
**sem conversão** e o arquivo é removido depois de confirmado. Linha inválida ou
cauda truncada: as linhas íntegras são entregues e o arquivo vira `.corrupt`. Um
`.emg-…jsonl.tmp` que sobre é um dump que **não terminou**: nunca é reingerido,
fica para o operador.

```bash
docker compose exec relplog ls -l /data/emergency
docker compose cp relplog:/data/emergency ./emergency
cat emergency/emg-*.jsonl | wc -l
jq -r 'select(.payload.host.hostname == "srv101") | "\(.event.iso) \(.payload.message)"' \
  emergency/emg-*.jsonl
```

Métricas: `relplog_emergency_files` e `relplog_emergency_events_pending`
(recontados do disco no fim da reingestão); um dump que falhou conta em
`relplog_events_dropped_total{reason="emergency_failed"}`.

## Saúde

O `healthcheck` do compose consulta **`/healthz`**: liveness, `200` enquanto o
processo vive. **`/readyz` não é o healthcheck, de propósito.** Ele dá `503`,
com o motivo em texto, quando o banco não está UP, quando o ring passa de
`ring.high_water`, durante a reingestão de dumps no boot, antes de o RELP abrir e
no shutdown. Nenhuma dessas condições se resolve reiniciando o contêiner.

```bash
curl -sS -w '%{http_code}\n' http://127.0.0.1:19380/readyz     # 200 "ready", ou 503 e o motivo
curl -s http://127.0.0.1:19380/metrics | grep -E '^relplog_(sink_up|ring_items|relp_unanswered_total)'
```

Para alertar, use `/metrics` (`relplog_sink_up`, `relplog_ring_items` contra
`relplog_ring_capacity_items`, `relplog_relp_unanswered_total`,
`relplog_events_dropped_total`). `relplog_config_info` diz `ring_mode="memory"`
e `sink_type="postgres"`, para que os painéis do `go-relplog-server` sirvam aqui.

**O `admin.bind` padrão é o mesmo do pai e do irmão** (`127.0.0.1:9103`, o do
`go-relplog-server` e o do `go-relplog-nats-server`). Dentro de contêineres não
há conflito; rodando dois desses binários direto no mesmo host, troque o de um
deles.

## Memória

`GOMEMLIMIT` (512 MiB) e o limite do contêiner (768M) vêm de `common.yml`. O
`GOMEMLIMIT` precisa cobrir duas vezes `RELPLOG_RING_MAX_BYTES` (256 MiB no
compose); suba os três juntos. A recepção RELP soma fora do ring (SPEC §15.3a):
até `relp.inflight_max_bytes` (64 MiB) de cópias em voo para o processo
inteiro, mais ~10 KiB por conexão.

## Desligamento

`docker stop` manda `SIGTERM`: `/readyz` falha, `pre_stop_delay` (0 s), as
sessões RELP entregam as respostas já decididas e fecham com `serverclose` (o
segurado fica sem resposta e o rsyslog o retransmite), o ring drena para a fila
em até `drain_timeout` (30 s), e o que sobrar vai para o dump (exit 3). O
`stop_grace_period` de `common.yml` (45 s) precisa ser **maior** que
`pre_stop_delay + drain_timeout + 6 s` (o `closeGrace` das sessões, §5.8).

## Códigos de saída

| Código | Significado |
|---|---|
| `0` | shutdown completo: ring drenado |
| `1` | erro de configuração ou de inicialização não relacionada ao banco (inclui a `LOGHUB_EB_URL` com `sslmode` que pede TLS ou sem usuário/banco, os tetos de SPEC §13.2, `pool_size` abaixo de `workers + 1` e as chaves do pai na linha de comando ou no INI) |
| `3` | banco indisponível ou inutilizável: sink FATAL (inclusive uma regra `ON INSERT`/`ON UPDATE` na fila), `boot_timeout` ou `drain_timeout` estourados (com o dump de emergência quando há o que guardar) |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante o boot (do *bind* da porta admin à abertura do RELP, passos 5–12) |

## Garantias

`docs/SPEC.md` §19. Resumo: o `200 OK` de cada mensagem diz "está na RAM deste
nó"; `SIGTERM` e FATAL do banco não perdem (dreno e dump), `kill -9` e OOM
perdem o backlog do ring, que o rsyslog já apagou. O que não pode ser aceito
agora fica sem resposta e volta ao rsyslog pela retransmissão se passar de 30 s.
