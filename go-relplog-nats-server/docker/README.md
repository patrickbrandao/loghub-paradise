# go-relplog-nats-server em contêiner

Uma combinação só (docs/SPEC.md §2.1): **RELP em claro → ring em memória →
NATS**, publicando cada envelope JLH em `loghub.<space>.syslog` (JetStream, com
`PubAck`, por padrão). O `docker-compose.yml` sobe o servidor, um `nats-server`
com JetStream e o job `nats-init`, que cria o stream `LOGHUB_SYSLOG` a partir de
`nats/stream.json`; com `--profile demo`, também um rsyslog de verdade mandando
pelo `omrelp`.

```bash
cd docker
cp .env.example .env            # opcional: todo valor tem default
docker compose up -d --build
docker compose logs -f relplog
docker compose --profile demo up -d --build     # com o emissor de demonstração
```

| Publicado no host | Padrão | Variável |
|---|---|---|
| RELP | `20581` | `RELPLOG_PORT` |
| admin (`/healthz`, `/readyz`, `/metrics`) | `127.0.0.1:19381` | `RELPLOG_ADMIN_PORT` |
| cliente NATS | `127.0.0.1:14381` | `RELPLOG_NATS_PORT` |
| monitor do NATS (`/healthz`, `/connz`, `/jsz`) | `127.0.0.1:18381` | `RELPLOG_NATS_MONITOR_PORT` |

Os padrões não coincidem com as portas publicadas pelos compose do
`go-relplog-server` (RELP 20514 e vizinhas, admin 9300 e vizinhas), do
`go-syslog-nats-server` (5181, 19181, 14281, 18281) nem do `go-relplog-pg-server`,
para rodarem no mesmo host. Para receber na porta RELP de costume,
`RELPLOG_PORT=20514`.

| Pasta | Montada em | Para quê |
|---|---|---|
| `conf/` | `/etc/relplog:ro` | `relplog.ini`, as chaves recarregáveis ([abaixo](#configuração-e-recarga-sighup)) |
| `nats/nats-server.conf` | `/etc/nats/` do `nats` | porta, monitor, `max_payload`, JetStream em `/data` |
| `nats/stream.json` | `/etc/nats/` do `nats-init` | o contrato do stream `LOGHUB_SYSLOG`, aplicado na primeira subida |
| `rsyslog/` | o emissor `demo` | `rsyslog.conf` e `demo.sh` do rsyslog de demonstração |
| volume `rlnats-data` | `/data` | a única área gravável do servidor: identidade e `emergency/` |
| volume `rlnats-nats-data` | `/data` do `nats` | o armazenamento do JetStream |
| volume `rlnats-emitter-spool` | `/var/spool/rsyslog` do emissor | a fila em disco do `omrelp` |

## Apontar um rsyslog real

Com `rsyslog-relp` instalado no emissor (o pacote que traz o `omrelp`):

```
module(load="omrelp")
action(type="omrelp" name="relp-loghub" target="<host do docker>" port="20581"
       windowSize="128" timeout="90"
       queue.type="LinkedList" queue.size="100000"
       queue.filename="relp-loghub" queue.saveOnShutdown="on"
       action.resumeRetryCount="-1")
```

- **`action.resumeRetryCount="-1"`**: sem ele, o rsyslog desiste de uma
  mensagem depois de algumas tentativas, e uma queda do servidor vira perda na
  origem.
- **A fila em disco** (`queue.filename`) é onde as mensagens esperam enquanto o
  servidor segura as transações (ring cheio, NATS fora): o servidor nunca as
  recusa com `500`; depois de `RELPLOG_RELP_BACKPRESSURE_TIMEOUT` (30 s) ele fecha
  a sessão sem resposta e o rsyslog retransmite (SPEC §5.7).
- **`timeout="90"`** (o padrão do `omrelp`) precisa ficar **acima** do
  `RELPLOG_RELP_BACKPRESSURE_TIMEOUT`: o servidor decide antes de o cliente
  desistir sozinho.
- **Sem TLS**: `tls="on"` no `omrelp` faz o primeiro frame ser um *handshake*,
  que o servidor lê como frame malformado e fecha. Para cifrar, um terminador TLS
  na frente (stunnel, HAProxy), com `RELPLOG_PROXY_PROTOCOL=optional` para manter
  o `src-ip` do emissor.

## O que o rsyslog vê

| No log do rsyslog | Significa |
|---|---|
| `action 'relp-loghub' suspended` seguido de `resumed` | a sessão fechou (servidor segurando além do prazo, shutdown, rede) e o rsyslog reconectou; as não confirmadas foram retransmitidas |
| `could not connect to remote server` | o servidor não está escutando (boot esperando o NATS, reingestão de dumps): o rsyslog guarda na fila |
| `peer sent error response` e `librelp error 10020` | um `500`: a mensagem foi **descartada** na origem. No fluxo normal deste servidor não acontece; só com um cliente fora do protocolo (SPEC §5.4) |

## Emissor de demonstração

O serviço `emitter` (só com `--profile demo`) é um rsyslog 8.2410 do Alpine 3.22
com `rsyslog-relp` (imagem construída de `rsyslog/Dockerfile`), que manda pelo
`omrelp` ao serviço `relplog` na porta do contêiner (20514).

```bash
docker compose --profile demo up -d --build
docker logs -f rlnats-demo-emitter                # rsyslogd e o gerador
curl -s http://127.0.0.1:19381/metrics | grep -E '^relplog_messages_(total|accepted_total)'
docker run --rm --network rlnats_default natsio/nats-box:0.20.0 \
  nats -s nats://nats:4222 stream info LOGHUB_SYSLOG
docker compose --profile demo down -v
```

- `rsyslog/demo.sh` sobe o `rsyslogd` e manda, pelo `logger` (`/dev/log` →
  `imuxsock`), uma rajada de `RELPLOG_DEMO_BURST` (100) linhas e depois uma a
  cada `RELPLOG_DEMO_INTERVAL` (1) segundo, com TAG `rlnats-demo` e `seq=N` na
  mensagem: um buraco na sequência é perda, um `seq` repetido é retransmissão.
- A fila do `omrelp` é disk-assisted, num volume, gravada no `SIGTERM`, com
  `action.resumeRetryCount="-1"`. O emissor sobe sem esperar o servidor, de
  propósito: os primeiros segundos do log mostram `could not connect to remote
  server` e `suspended`, e depois `resumed`, sem nenhuma linha perdida.
- Só a facility `local0` (a do gerador) vai pelo RELP; as mensagens do próprio
  `rsyslogd` vão para o `docker logs`.

## Sem TLS

**Não há TLS em lugar nenhum** (SPEC §1.2): nem RELP sobre TLS, nem conexão
cifrada com o NATS. A imagem não traz `ca-certificates` nem a porta 20515.

- Toda URL do NATS precisa ser `nats://`. `tls://`, `ws://`, `wss://` e um
  `host:porta` sem esquema são **exit 1**.
- Um `nats-server` com bloco `tls {}` (que anuncia `tls_required`) é recusado no
  boot com **exit 3**, sem nenhum *handshake*: o cliente `nats.go` se
  promoveria a TLS sozinho, e o servidor o impede.
- Senha, token e eventos trafegam em claro: o NATS precisa estar numa rede
  confiável (aqui, a rede interna do compose).

Quem precisa de RELP cifrado põe um terminador TLS na frente (com PROXY
protocol, `RELPLOG_PROXY_PROTOCOL=optional`, para manter o `src-ip`) ou usa o
`go-relplog-server`.

## Configuração e recarga (SIGHUP)

A configuração chega por dois caminhos, cada um com um tipo de chave:

| Onde | O quê | Quando vale |
|---|---|---|
| env: `common.yml`, `docker-compose.yml`, `.env` | tudo o que **exige restart** (listen, relp, ring, sink, `[syslog]`, `[loghub]`, shutdown...) | no `up` que recria o contêiner |
| `conf/relplog.ini`, lido via `RELPLOG_CONFIG=/etc/relplog/relplog.ini` | **só** as recarregáveis (SPEC §16.3): `log.level`, `log.access`, `log.access_sample` | no `SIGHUP` |

```bash
vi conf/relplog.ini                     # ex.: [log] level = debug
docker kill -s HUP rlnats               # ou: docker compose kill -s HUP relplog
docker logs --since 1m rlnats 2>&1 | grep SIGHUP
```

| Linha do log | Significado |
|---|---|
| `SIGHUP: applied` com `keys=[...]` | trocado a quente |
| `SIGHUP: configuration unchanged` | nada mudou no arquivo |
| `ERROR SIGHUP: the new configuration is invalid and was NOT applied` | arquivo inválido (inclusive uma seção do `go-relplog-server` que não existe aqui, como `[filter]` ou `[tls]`, ou `sink.nats.subjects`); o servidor segue com a config anterior |
| `WARN SIGHUP: this key requires a restart and was ignored` | uma chave de restart foi posta no INI (e o env não a define); repete em todo HUP até o restart |
| `WARN SIGHUP: this key is set in the config file, but an environment variable or a flag decides its value` | a chave também está no env, que vence |

**Por que não pelo env.** Env vence o INI (SPEC §16.1) e o env de um contêiner
não muda depois do `up`: uma chave recarregável definida por env nunca mudaria
com o `HUP`. Por isso nenhum arquivo deste diretório define `RELPLOG_LOG_LEVEL`,
`RELPLOG_LOG_ACCESS` ou `RELPLOG_LOG_ACCESS_SAMPLE`.

**A pasta, não o arquivo.** O bind é de `conf/`, não de `conf/relplog.ini`: um
bind de arquivo prende o inode, e um editor que salva por *rename* deixaria o
contêiner lendo a versão antiga para sempre.

**`SIGHUP` no boot.** Fora da janela entre o fim do boot e o início do shutdown
(por exemplo, enquanto o servidor espera o NATS ou reingere dumps), o `HUP` é
logado em `WARN` e ignorado; nunca derruba o processo.

**Variáveis do `go-relplog-server`.** Um `.env` compartilhado com ele pode ter
`RELPLOG_LISTEN_TLS`, `RELPLOG_TLS_*`, `RELPLOG_FILTER_*`, `RELPLOG_RATELIMIT_*`,
`RELPLOG_SINK_TYPE`, `RELPLOG_RING_MODE`, `RELPLOG_SINK_NATS_SUBJECTS`... O compose só repassa as
variáveis que conhece; se uma delas chegar ao contêiner por outro caminho, o
boot emite **um** `WARN` que as nomeia e diz que são ignoradas. Na linha de
comando ou no INI elas são parâmetro desconhecido: exit 1.

## Identidade LogHub

O servidor chama o `go-loghub-ident` no boot. No contêiner:

| Variável | Valor | Por quê |
|---|---|---|
| `DATADIR` | `/data` (da imagem) | `agent_uuid` e `machine_id` são gerados e persistidos ali, junto de `emergency/` |
| `AGENT_NAME` | `go-relplog-nats-server` (da imagem) | sem ele o nome viria de `argv[0]` |
| `HOSTNAME` | `rlnats` (o `hostname:` do serviço) | sem ele seria o id do contêiner, que muda a cada recriação |
| `WORKSPACE` | `RELPLOG_WORKSPACE` do `.env` (`default`) | o **tenant**: vira o `space` de todo envelope e o subject `loghub.<space>.syslog`. O compose nunca lê o `WORKSPACE` do shell, que o Jenkins usa para o caminho do job |

A conexão aparece no `connz` do NATS como `rlnats/go-relplog-nats-server@rlnats`:

```bash
curl -s 'http://127.0.0.1:18381/connz' | jq '.connections[] | {name, ip, in_msgs}'
```

**Trocou o `WORKSPACE`? Troque o stream junto.** O `nats/stream.json` cobre
`loghub.default.syslog`; com outro `space` o boot sai com **3**, porque o stream
não cobre o subject (SPEC §10.7). Edite os `subjects` e recrie o stream (ou o
volume do NATS). Nunca use `loghub.>`: ele capturaria `loghub._ctl.beacon`, e o
boot também o recusa.

**Um volume por instância.** O `from` de cada envelope é o `agent_uuid` gravado no
volume. Duas réplicas montando o mesmo `/data` publicam o mesmo `from`, sem
aviso. Volume anônimo ou `--rm` sem volume gera identidade nova a cada
recriação, também sem aviso.

Falhas de identidade encerram o processo com os códigos do `go-loghub-ident`
v0.5.2, com a causa em stderr (`lib-loghub-ident: <VARIÁVEL>: <motivo>`).
`AGENT_NAME`, `HOSTNAME` e `WORKSPACE` são passados para minúsculas antes da
validação.

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

**E o próprio servidor sai com 1** (SPEC §13.2) por um `WORKSPACE` que a
biblioteca aceita mas a convenção LogHub não: um segmento vazio, como `a..b`,
`.x` ou `x.`.

## NATS: o stream `LOGHUB_SYSLOG`

O servidor **publica**; quem guarda é o stream. O `nats-init`
(`natsio/nats-box`, versão fixada em `RELPLOG_NATS_BOX_IMAGE`) cria
`LOGHUB_SYSLOG` a partir de `nats/stream.json` se ele ainda não existir. O
servidor **nunca** cria nem altera um stream: no boot ele valida e sai com
**3, na hora**, se o stream:

- não existir, ou não cobrir `loghub.<space>.syslog`;
- cobrir `loghub._ctl.beacon` (um `loghub.>` reproduziria beacons antigos para
  todo consumidor);
- estiver selado, tiver `no_ack: true` ou `retention: interest` (que confirma
  e não guarda nada sem consumidor);
- não couber a maior mensagem (`max_msg_size`, o teto de 32 MiB por registro
  do `storage: file`, ou um `max_bytes` com `discard: new` menor que um
  registro).

A mesma validação roda de novo em **toda** checagem de saúde: um stream
editado depois do boot (subjects estreitados, `no_ack` ligado, tetos baixados)
leva o servidor a FATAL, dump em `/data/emergency` e saída 3, em vez de retentar
para sempre com o ring enchendo (SPEC §10.7).

O `nats-server` também precisa de `max_payload` para a maior mensagem: 1 MiB
(o padrão, explícito em `nats/nats-server.conf`) basta até
`SYSLOG_MAX_MESSAGE_BYTES=84KiB`; acima disso, suba os dois juntos.

Cada mensagem tem `Nats-Msg-Id = <uuid>.0`: um reenvio dentro da
`duplicate_window` do stream (10 min) é confirmado como duplicata e guardado uma
vez só (`relplog_sink_nats_duplicates_total`).

```bash
# o stream e o que chegou
docker compose run --rm nats-init 'nats --server nats://nats:4222 stream info LOGHUB_SYSLOG'
nats --server nats://127.0.0.1:14381 stream info LOGHUB_SYSLOG       # com o nats CLI no host

# as últimas mensagens, com os headers X-LogHub-*
nats --server nats://127.0.0.1:14381 stream view LOGHUB_SYSLOG --since 1m

# um consumidor efêmero que acompanha em tempo real
nats --server nats://127.0.0.1:14381 sub 'loghub.default.syslog'
```

**`RELPLOG_SINK_NATS_MODE=core`** publica sem JetStream (*best effort*): o
sucesso é "o servidor recebeu"; sem stream, uma mensagem sem assinante some, e
nada deduplica. Com RELP isso desfaz a promessa do `200 OK`; o boot registra um
`WARN` dizendo isso.

## Heartbeat beacon

O servidor emite o heartbeat da skill LogHub em `loghub._ctl.beacon`
(`LOGHUB_CTL_NAMESPACE`), por **NATS core** (fanout, nunca num stream), na
mesma conexão dos dados: na primeira conexão, em toda reconexão e a cada
`LOGHUB_BEACON_INTERVAL` segundos (15; `0` desliga só o periódico). O envelope
tem `type=beacon`, `level=9`, `sid` = PID e o payload com `name`, `hostname`,
`machine`, `address`, `uptime` e `meta` (versão, `ring=memory`, `sink=nats`,
transportes).

```bash
nats --server nats://127.0.0.1:14381 sub 'loghub._ctl.beacon'
curl -s http://127.0.0.1:19381/metrics | grep '^relplog_beacons_total'
```

Uma falha do beacon é contada em `relplog_beacons_total{result="error"}` e
logada com amostragem; **nunca** afeta a saúde do sink. O servidor não responde
a sonar, `ping` nem `pong`.

## LOGHUB_EB_URL

`LOGHUB_EB_URL` (SPEC §10.9) é a URL do eventbus da convenção LogHub. Aqui só
**`nats://`** é aceita (uma lista separada por vírgula é um cluster);
`tls://` é **exit 1**, com uma mensagem que nomeia o TLS, e qualquer outro
esquema (`postgres://`, `redis://`...) também é exit 1. A URL preenche
`sink.nats.urls`, com usuário e senha (ou token) dentro dela.

O compose não a repassa por padrão: um `LOGHUB_EB_URL` exportado no shell
colidiria com as chaves de conexão do arquivo, e o boot sairia com 1. Para
usá-la, edite o `environment:` do serviço `relplog` no `docker-compose.yml`:
apague as quatro chaves que ela substitui e descomente a linha da URL.

```yaml
      # antes
      RELPLOG_SINK_NATS_URLS: nats://nats:4222
      RELPLOG_SINK_NATS_USERNAME:
      RELPLOG_SINK_NATS_PASSWORD:
      RELPLOG_SINK_NATS_TOKEN:
      #LOGHUB_EB_URL: ${LOGHUB_EB_URL:?defina LOGHUB_EB_URL no .env ou no shell}

      # depois
      LOGHUB_EB_URL: ${LOGHUB_EB_URL:?defina LOGHUB_EB_URL no .env ou no shell}
```

```bash
echo "LOGHUB_EB_URL=nats://usuario:senha@nats-1.intranet:4222,nats://usuario:senha@nats-2.intranet:4222" >> .env
docker compose up -d --build
```

As regras:

- **Só `nats://`, em toda entrada da lista**, com host e sem caminho, consulta
  ou fragmento. Sem TLS: `tls://` é exit 1. `%`, `/`, `#`, `?` e `,` de uma
  senha ou token vão codificados (`%2F`...): um `@` depois de `/`, `?` ou `#` é
  recusado.
- **Uma fonte só.** Com a URL preenchida, `RELPLOG_SINK_NATS_URLS`, `_USERNAME`,
  `_PASSWORD` ou `_TOKEN` definidas também (env, flag ou INI) são exit 1 com as
  duas origens no log, **mesmo que a URL não traga aquela parte**: uma URL sem
  senha quer dizer "sem senha". `_CREDENTIALS` (um `.creds`), `_MODE` e
  `_STREAM` convivem com a URL.
- O `--dump-config` mostra a URL com o *userinfo* redigido
  (`nats://usuario:***@host`), e nenhuma mensagem de erro cita a URL.
- Apontando para um NATS externo, o `nats` e o `nats-init` locais sobem do mesmo
  jeito e ficam ociosos; o stream do NATS externo precisa existir, porque o
  servidor nunca o cria.

## Arquivo de emergência (`.jsonl`)

O ring é memória. Se o NATS ficar inutilizável (FATAL, SPEC §13.3) ou o dreno
do `SIGTERM` estourar `RELPLOG_SHUTDOWN_DRAIN_TIMEOUT`, o que estava pendente é
gravado em `/data/emergency/emg-<agent_uuid>-<unixnano>.jsonl` e o processo sai
com 3. **Um envelope JLH pronto por linha**, byte a byte o que o worker teria
publicado. O arquivo nasce como `.emg-…jsonl.tmp` e só ganha o nome final depois
de gravado e sincronizado: um `.tmp` que sobre de uma queda nunca é reingerido.

No boot seguinte, antes de abrir qualquer transporte, todo `emg-*.jsonl` volta
para o stream **sem conversão** e lido em fluxo (o subject, o `Nats-Msg-Id` e
os headers saem do cabeçalho de cada linha), e o arquivo é removido depois de
confirmado. Linha inválida ou cauda truncada: as linhas íntegras são entregues e
o arquivo vira `.corrupt`, para inspeção. O prazo da reingestão é um
`RELPLOG_SINK_BOOT_TIMEOUT` sem progresso: todo pedaço entregue o adia. Um dump de outro `WORKSPACE` só volta
se um stream cobrir o subject dele; senão o boot sai com 3 e o arquivo fica.

```bash
# os arquivos pendentes
docker compose exec relplog ls -l /data/emergency
docker compose cp relplog:/data/emergency ./emergency

# quantos envelopes, e por severidade
cat emergency/emg-*.jsonl | wc -l
jq -r '.level' emergency/emg-*.jsonl | sort | uniq -c

# as mensagens de um host, com o instante de recepção
jq -r 'select(.payload.host.hostname == "srv101") | "\(.event.iso) \(.payload.message)"' \
  emergency/emg-*.jsonl
```

Métricas: `relplog_emergency_files` e `relplog_emergency_events_pending` (o que
está no disco, recontado no fim da reingestão); um dump que falhou conta em
`relplog_events_dropped_total{reason="emergency_failed"}`, e um segundo sinal
no meio do dump conta em `reason="abort"` e abandona o arquivo.
Para reingerir um arquivo à mão (um `.corrupt` reparado, por exemplo), copie-o
para `/data/emergency` com um nome `emg-*.jsonl` e reinicie o contêiner.

## Saúde

O `healthcheck` do compose consulta **`/healthz`**: liveness, `200` enquanto o
processo vive. **`/readyz` não é o healthcheck, de propósito.** Ele dá `503`,
com o motivo em texto, quando o NATS não está UP, quando o ring passa de
`ring.high_water`, durante a reingestão de dumps no boot, antes de o
transporte abrir e no shutdown. Nenhuma dessas condições se resolve
reiniciando o contêiner: um restart não conserta um NATS inalcançável, só troca
o backlog da RAM por um dump e uma reingestão.

`/readyz` é para o balanceador (tirar a instância de rotação) e para o
operador. A porta admin só é publicada no loopback do host:

```bash
curl -sS -w '%{http_code}\n' http://127.0.0.1:19381/readyz     # 200 "ready", ou 503 e o motivo
curl -s http://127.0.0.1:19381/metrics | grep -E '^relplog_(sink_up|ring_items|ring_bytes|events_written_total|sink_nats_|relp_)'
docker exec rlnats wget -qO- http://127.0.0.1:9103/readyz      # de dentro
```

Para alertar, use `/metrics` (`relplog_sink_up`, `relplog_ring_items` contra
`relplog_ring_capacity_items`, `relplog_relp_unanswered_total`,
`relplog_relp_responses_total{code="500"}`, `relplog_events_dropped_total`,
`relplog_sink_nats_reconnects_total`), não o estado do healthcheck. Os nomes são
os do `go-relplog-server`; `relplog_config_info` diz `ring_mode="memory"` e
`sink_type="nats"`.

**O `admin.bind` padrão é o mesmo do `go-relplog-server` e do `go-relplog-pg-server`**
(`127.0.0.1:9103`, a regra da família, como os três servidores syslog na 9102).
Dentro de contêineres não há conflito; rodando dois binários da família direto
no mesmo host, troque o de um deles.

## Memória

`GOMEMLIMIT` (512 MiB) e o limite do contêiner (768M) vêm de `common.yml`. O
`GOMEMLIMIT` precisa cobrir duas vezes `RELPLOG_RING_MAX_BYTES` (256 MiB no
compose); suba os três juntos. Fora do ring, a recepção RELP segura até
`RELPLOG_RELP_INFLIGHT_MAX_BYTES` (64 MiB) de cópias em voo, mais ~10 KiB por
conexão (SPEC §15.3a).

## Garantias

`docs/SPEC.md` §19. Resumo: o `200 OK` de cada transação é "está na RAM";
ring cheio e NATS fora seguram o rsyslog, nunca recusam.
`SIGTERM` e FATAL do NATS não perdem (dreno e dump), `kill -9` e OOM perdem o
backlog do ring. Depois do `PubAck`, a durabilidade é a do stream (`storage`,
réplicas); em `core`, uma mensagem sem assinante some.
