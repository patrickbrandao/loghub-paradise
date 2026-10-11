# go-syslog-nats-server em contêiner

Uma combinação só (docs/SPEC.md §2.1): **ring em memória → NATS**, publicando
cada envelope JLH em `loghub.<space>.syslog` (JetStream, com `PubAck`, por
padrão). O `docker-compose.yml` sobe o servidor, um `nats-server` com JetStream
e o job `nats-init`, que cria o stream `LOGHUB_SYSLOG` a partir de
`nats/stream.json`.

```bash
cd docker
cp .env.example .env            # opcional: todo valor tem default
docker compose up -d --build
docker compose logs -f syslog
```

| Publicado no host | Padrão | Variável |
|---|---|---|
| syslog UDP e TCP | `5181` | `SYSLOG_PORT` |
| admin (`/healthz`, `/readyz`, `/metrics`) | `127.0.0.1:19181` | `SYSLOG_ADMIN_PORT` |
| cliente NATS | `127.0.0.1:14281` | `SYSLOG_NATS_PORT` |
| monitor do NATS (`/healthz`, `/connz`, `/jsz`) | `127.0.0.1:18281` | `SYSLOG_NATS_MONITOR_PORT` |

Os padrões não coincidem com nenhuma porta publicada pelos compose do
`go-syslog-server` (syslog 514 e 5141–5162, admin 9102 e 19101–19122) nem pelo
do `go-syslog-pg-server` (5180, 19180, 15480), para rodarem no mesmo host. Para
receber na porta padrão do syslog, `SYSLOG_PORT=514`.

```bash
logger -n 127.0.0.1 -P 5181 -d 'teste UDP'               # util-linux
logger -n 127.0.0.1 -P 5181 -T 'teste TCP'
printf '<13>1 - - app - - - teste' | nc -u -w1 127.0.0.1 5181   # macOS
```

| Pasta | Montada em | Para quê |
|---|---|---|
| `conf/` | `/etc/syslog:ro` | `syslog.ini`, as chaves recarregáveis ([abaixo](#configuração-e-recarga-sighup)) |
| `nats/nats-server.conf` | `/etc/nats/` do `nats` | porta, monitor, `max_payload`, JetStream em `/data` |
| `nats/stream.json` | `/etc/nats/` do `nats-init` | o contrato do stream `LOGHUB_SYSLOG`, aplicado na primeira subida |
| volume `slnats-data` | `/data` | a única área gravável do servidor: identidade e `emergency/` |
| volume `slnats-nats-data` | `/data` do `nats` | o armazenamento do JetStream |

## Sem TLS

**Não há TLS em lugar nenhum** (SPEC §1.2): nem listener RFC 5425, nem conexão
cifrada com o NATS. A imagem não traz `ca-certificates` nem a porta 6514.

- Toda URL do NATS precisa ser `nats://`. `tls://`, `ws://`, `wss://` e um
  `host:porta` sem esquema são **exit 1**.
- Um `nats-server` com bloco `tls {}` (que anuncia `tls_required`) é recusado no
  boot com **exit 3**, sem nenhum *handshake*: o cliente `nats.go` se
  promoveria a TLS sozinho, e o servidor o impede.
- Senha, token e eventos trafegam em claro: o NATS precisa estar numa rede
  confiável (aqui, a rede interna do compose).

Quem precisa de syslog cifrado põe um terminador TLS na frente (com PROXY
protocol, `SYSLOG_PROXY_PROTOCOL=optional`, para manter o `src-ip`) ou usa o
`go-syslog-server`.

## Configuração e recarga (SIGHUP)

A configuração chega por dois caminhos, cada um com um tipo de chave:

| Onde | O quê | Quando vale |
|---|---|---|
| env: `common.yml`, `docker-compose.yml`, `.env` | tudo o que **exige restart** (listen, ring, sink, `[syslog]`, `[loghub]`, shutdown...) | no `up` que recria o contêiner |
| `conf/syslog.ini`, lido via `SYSLOG_CONFIG=/etc/syslog/syslog.ini` | **só** as recarregáveis (SPEC §16.3): `log.level`, `log.access`, `log.access_sample` | no `SIGHUP` |

```bash
vi conf/syslog.ini                      # ex.: [log] level = debug
docker kill -s HUP slnats               # ou: docker compose kill -s HUP syslog
docker logs --since 1m slnats 2>&1 | grep SIGHUP
```

| Linha do log | Significado |
|---|---|
| `SIGHUP: applied` com `keys=[...]` | trocado a quente |
| `SIGHUP: configuration unchanged` | nada mudou no arquivo |
| `ERROR SIGHUP: the new configuration is invalid and was NOT applied` | arquivo inválido (inclusive uma seção do `go-syslog-server` que não existe aqui, como `[filter]`, ou `sink.nats.subjects`); o servidor segue com a config anterior |
| `WARN SIGHUP: this key requires a restart and was ignored` | uma chave de restart foi posta no INI (e o env não a define); repete em todo HUP até o restart |
| `WARN SIGHUP: this key is set in the config file, but an environment variable or a flag decides its value` | a chave também está no env, que vence |

**Por que não pelo env.** Env vence o INI (SPEC §16.1) e o env de um contêiner
não muda depois do `up`: uma chave recarregável definida por env nunca mudaria
com o `HUP`. Por isso nenhum arquivo deste diretório define `SYSLOG_LOG_LEVEL`,
`SYSLOG_LOG_ACCESS` ou `SYSLOG_LOG_ACCESS_SAMPLE`.

**A pasta, não o arquivo.** O bind é de `conf/`, não de `conf/syslog.ini`: um
bind de arquivo prende o inode, e um editor que salva por *rename* deixaria o
contêiner lendo a versão antiga para sempre.

**`SIGHUP` no boot.** Fora da janela entre o fim do boot e o início do shutdown
(por exemplo, enquanto o servidor espera o NATS ou reingere dumps), o `HUP` é
logado em `WARN` e ignorado; nunca derruba o processo.

**Variáveis do `go-syslog-server`.** Um `.env` compartilhado com o pai pode ter
`SYSLOG_LISTEN_TLS`, `SYSLOG_FILTER_*`, `SYSLOG_SINK_TYPE`,
`SYSLOG_SINK_NATS_SUBJECTS`, `SYSLOG_SINK_NATS_TLS`... O compose só repassa as
variáveis que conhece; se uma delas chegar ao contêiner por outro caminho, o
boot emite **um** `WARN` que as nomeia e diz que são ignoradas. Na linha de
comando ou no INI elas são parâmetro desconhecido: exit 1.

## Identidade LogHub

O servidor chama o `go-loghub-ident` no boot. No contêiner:

| Variável | Valor | Por quê |
|---|---|---|
| `DATADIR` | `/data` (da imagem) | `agent_uuid` e `machine_id` são gerados e persistidos ali, junto de `emergency/` |
| `AGENT_NAME` | `go-syslog-nats-server` (da imagem) | sem ele o nome viria de `argv[0]` |
| `HOSTNAME` | `slnats` (o `hostname:` do serviço) | sem ele seria o id do contêiner, que muda a cada recriação |
| `WORKSPACE` | `SYSLOG_WORKSPACE` do `.env` (`default`) | o **tenant**: vira o `space` de todo envelope e o subject `loghub.<space>.syslog`. O compose nunca lê o `WORKSPACE` do shell, que o Jenkins usa para o caminho do job |

A conexão aparece no `connz` do NATS como `slnats/go-syslog-nats-server@slnats`:

```bash
curl -s 'http://127.0.0.1:18281/connz' | jq '.connections[] | {name, ip, in_msgs}'
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
(`natsio/nats-box`, versão fixada em `SYSLOG_NATS_BOX_IMAGE`) cria
`LOGHUB_SYSLOG` a partir de `nats/stream.json` se ele ainda não existir. O
servidor **nunca** cria nem altera um stream: no boot ele valida e sai com
**3, na hora**, se o stream:

- não existir, ou não cobrir `loghub.<space>.syslog`;
- cobrir `loghub._ctl.beacon` (um `loghub.>` reproduziria beacons antigos para
  todo consumidor);
- estiver selado, ou tiver `no_ack: true`;
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
vez só (`syslog_sink_nats_duplicates_total`).

```bash
# o stream e o que chegou
docker compose run --rm nats-init 'nats --server nats://nats:4222 stream info LOGHUB_SYSLOG'
nats --server nats://127.0.0.1:14281 stream info LOGHUB_SYSLOG       # com o nats CLI no host

# as últimas mensagens, com os headers X-LogHub-*
nats --server nats://127.0.0.1:14281 stream view LOGHUB_SYSLOG --since 1m

# um consumidor efêmero que acompanha em tempo real
nats --server nats://127.0.0.1:14281 sub 'loghub.default.syslog'
```

**`SYSLOG_SINK_NATS_MODE=core`** publica sem JetStream (*best effort*): o
sucesso é "o servidor recebeu"; sem stream, uma mensagem sem assinante some, e
nada deduplica. O boot registra um `WARN` dizendo isso.

## Heartbeat beacon

O servidor emite o heartbeat da skill LogHub em `loghub._ctl.beacon`
(`LOGHUB_CTL_NAMESPACE`), por **NATS core** (fanout, nunca num stream), na
mesma conexão dos dados: na primeira conexão, em toda reconexão e a cada
`LOGHUB_BEACON_INTERVAL` segundos (15; `0` desliga só o periódico). O envelope
tem `type=beacon`, `level=9`, `sid` = PID e o payload com `name`, `hostname`,
`machine`, `address`, `uptime` e `meta` (versão, `ring=memory`, `sink=nats`,
transportes).

```bash
nats --server nats://127.0.0.1:14281 sub 'loghub._ctl.beacon'
curl -s http://127.0.0.1:19181/metrics | grep '^syslog_beacons_total'
```

Uma falha do beacon é contada em `syslog_beacons_total{result="error"}` e
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
usá-la, edite o `environment:` do serviço `syslog` no `docker-compose.yml`:
apague as quatro chaves que ela substitui e descomente a linha da URL.

```yaml
      # antes
      SYSLOG_SINK_NATS_URLS: nats://nats:4222
      SYSLOG_SINK_NATS_USERNAME:
      SYSLOG_SINK_NATS_PASSWORD:
      SYSLOG_SINK_NATS_TOKEN:
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
  ou fragmento. Sem TLS: `tls://` é exit 1.
- **Uma fonte só.** Com a URL preenchida, `SYSLOG_SINK_NATS_URLS`, `_USERNAME`,
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
do `SIGTERM` estourar `SYSLOG_SHUTDOWN_DRAIN_TIMEOUT`, o que estava pendente é
gravado em `/data/emergency/emg-<agent_uuid>-<unixnano>.jsonl` e o processo sai
com 3. **Um envelope JLH pronto por linha**, byte a byte o que o worker teria
publicado. O arquivo nasce como `.emg-…jsonl.tmp` e só ganha o nome final depois
de gravado e sincronizado: um `.tmp` que sobre de uma queda nunca é reingerido.

No boot seguinte, antes de abrir qualquer transporte, todo `emg-*.jsonl` volta
para o stream **sem conversão** e lido em fluxo (o subject, o `Nats-Msg-Id` e
os headers saem do cabeçalho de cada linha), e o arquivo é removido depois de
confirmado. Linha inválida ou cauda truncada: as linhas íntegras são entregues e
o arquivo vira `.corrupt`, para inspeção. Um dump de outro `WORKSPACE` só volta
se um stream cobrir o subject dele; senão o boot sai com 3 e o arquivo fica.

```bash
# os arquivos pendentes
docker compose exec syslog ls -l /data/emergency
docker compose cp syslog:/data/emergency ./emergency

# quantos envelopes, e por severidade
cat emergency/emg-*.jsonl | wc -l
jq -r '.level' emergency/emg-*.jsonl | sort | uniq -c

# as mensagens de um host, com o instante de recepção
jq -r 'select(.payload.host.hostname == "srv101") | "\(.event.iso) \(.payload.message)"' \
  emergency/emg-*.jsonl
```

Métricas: `syslog_emergency_files` e `syslog_emergency_events_pending`;
um dump que falhou conta em `syslog_events_dropped_total{reason="emergency_failed"}`.
Para reingerir um arquivo à mão (um `.corrupt` reparado, por exemplo), copie-o
para `/data/emergency` com um nome `emg-*.jsonl` e reinicie o contêiner.

## Saúde

O `healthcheck` do compose consulta **`/healthz`**: liveness, `200` enquanto o
processo vive. **`/readyz` não é o healthcheck, de propósito.** Ele dá `503`,
com o motivo em texto, quando o NATS não está UP, quando o ring passa de
`ring.high_water`, durante a reingestão de dumps no boot, antes de os
transportes abrirem e no shutdown. Nenhuma dessas condições se resolve
reiniciando o contêiner: um restart não conserta um NATS inalcançável, só troca
o backlog da RAM por um dump e uma reingestão.

`/readyz` é para o balanceador (tirar a instância de rotação) e para o
operador. A porta admin só é publicada no loopback do host:

```bash
curl -sS -w '%{http_code}\n' http://127.0.0.1:19181/readyz     # 200 "ready", ou 503 e o motivo
curl -s http://127.0.0.1:19181/metrics | grep -E '^syslog_(sink_up|ring_items|ring_bytes|events_written_total|sink_nats_)'
docker exec slnats wget -qO- http://127.0.0.1:9102/readyz      # de dentro
```

Para alertar, use `/metrics` (`syslog_sink_up`, `syslog_ring_items` contra
`syslog_ring_capacity_items`, `syslog_messages_dropped_total`,
`syslog_events_dropped_total`, `syslog_sink_nats_reconnects_total`), não o
estado do healthcheck. Os nomes são os do `go-syslog-server`;
`syslog_config_info` diz `ring_mode="memory"` e `sink_type="nats"`.

**O `admin.bind` padrão é o mesmo do pai** (`127.0.0.1:9102`). Dentro de
contêineres não há conflito; rodando dois binários da família direto no mesmo
host, troque o de um deles.

## UDP

O parâmetro que mais importa é `SYSLOG_LISTEN_UDP_READ_BUFFER`. Dentro do
contêiner o teto é o `net.core.rmem_max` do **host**; sem subi-lo, o pedido é
reduzido em silêncio (e o boot avisa). O valor efetivo aparece em
`syslog_udp_read_buffer_bytes`.

```bash
sysctl -w net.core.rmem_max=16777216
```

## Memória

`GOMEMLIMIT` (512 MiB) e o limite do contêiner (768M) vêm de `common.yml`. O
`GOMEMLIMIT` precisa cobrir duas vezes `SYSLOG_RING_MAX_BYTES` (256 MiB no
compose); suba os três juntos.

## Garantias

`docs/SPEC.md` §19. Resumo: só o TCP segura o emissor; UDP e unix contam o que
descartam em `syslog_messages_dropped_total`. A confirmação é "está na RAM":
`SIGTERM` e FATAL do NATS não perdem (dreno e dump), `kill -9` e OOM perdem o
backlog do ring. Depois do `PubAck`, a durabilidade é a do stream (`storage`,
réplicas); em `core`, uma mensagem sem assinante some.
