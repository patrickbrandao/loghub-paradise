# go-relplog-server em contêiner

O servidor recebe **RELP**, o protocolo confiável do rsyslog (`omrelp`, do
`librelp`), em claro ou sobre TLS, confirma **cada mensagem** só depois da
garantia do ring e entrega um envelope JLH `type=syslog` (com
`"x-transport":"relp"`) ao destino. Não há UDP, unix datagram nem as molduras da
RFC 6587: quem precisa delas usa o `go-syslog-server` (docs/SPEC.md §4).

Um compose por combinação de **ring** × **sink** (docs/SPEC.md §2.1, §9, §10).
A combinação padrão, e a de referência, é `ring=memory` → `sink=nats`
(JetStream), o eventbus do LogHub:

```bash
cd docker
docker compose up -d --build                         # memory -> nats
docker compose --profile demo up -d --build          # o mesmo, com um rsyslog de demonstração
docker compose -f compose.sqlite-postgres.yml up -d --build
docker compose -f compose.sqlite-postgres.yml -f compose.emitter.yml --profile demo up -d --build
```

| Arquivo | Ring | Sink |
|---|---|---|
| `compose.memory-{nats,redis,sqlite,postgres,mysql,clickhouse,none}.yml` | memória (dump de emergência em FATAL e no `SIGTERM` que estoura o dreno) | o do nome |
| `compose.sqlite-{...}.yml` | `ring.db` com `fsync` | o do nome |
| `compose.none-{...}.yml` | sem ring (escrita síncrona, dentro da transação RELP) | o do nome |
| `compose.redis-nats.yml`, `compose.redis-postgres.yml` | stream Redis **compartilhado** entre réplicas | o do nome |
| `compose.emitter.yml` | — | camada com o rsyslog de demonstração, profile `demo` ([abaixo](#emissor-de-demonstração)) |

Os `*-none.yml` são **benchmark**: `sink=none` converte, serializa e descarta
tudo (§10.8, §19), e o rsyslog recebe `200 OK` assim mesmo. Não guardam nada.

O cabeçalho de cada compose diz a sua **garantia**, que no RELP é por
mensagem: o `200 OK` de cada transação só sai depois dela (§19).

| Pasta | Montada em | Para quê |
|---|---|---|
| `conf/` | `/etc/relplog:ro` | `relplog.ini`, as chaves recarregáveis ([abaixo](#configuração-e-recarga-sighup)) |
| `tls/` | `/etc/relplog/tls:ro` | certificados do RELP/TLS (`tls/README.md`) |
| volume `rls-data` | `/data` | a única área gravável: identidade, `ring.db`, `events.db`, `emergency/` |
| `rsyslog/` | (emissor) `/etc/rsyslog.conf`, `/etc/rls-demo/` | o rsyslog de demonstração e o modelo de config de um emissor real |

## Portas

Dentro do contêiner as portas são sempre as padrão (SPEC §4.1): **20514** (RELP
em claro), **20515** (RELP/TLS, só com `RELPLOG_LISTEN_TLS`) e **9103** (admin).
No host, cada combinação publica portas distintas, para que várias rodem lado a
lado, e **disjuntas das do `go-syslog-server`** (que publica 514/6514/9102 na de
referência e 5141–5162, 16501–16522, 19101–19122 nas outras), para que os dois
rodem no mesmo host. Os nomes também não colidem: projeto, contêiner e volume
deste levam o prefixo `rls-`, os do irmão `sls-`.

Com `n` o índice da combinação: RELP `20514+n`, RELP/TLS `20614+n`, admin
`127.0.0.1:9300+n`; os serviços de apoio, só no loopback, seguem o esquema do
irmão mais 10000 (nats `24200+n`, redis `26300+n`, ring-redis `26400+n`,
postgres `25400+n`, mysql `23300+n`; clickhouse `29000+n`).

| n | Arquivo | Ring | Sink | RELP | RELP/TLS | Admin (loopback) | Apoio (loopback) |
|---|---|---|---|---|---|---|---|
| 0 | `compose.memory-nats.yml` | `memory` | `nats` | 20514 | 20614 | 9300 | nats 24200 |
| 1 | `compose.memory-redis.yml` | `memory` | `redis` | 20515 | 20615 | 9301 | redis 26301 |
| 2 | `compose.memory-sqlite.yml` | `memory` | `sqlite` | 20516 | 20616 | 9302 | — |
| 3 | `compose.memory-postgres.yml` | `memory` | `postgres` | 20517 | 20617 | 9303 | postgres 25403 |
| 4 | `compose.memory-mysql.yml` | `memory` | `mysql` | 20518 | 20618 | 9304 | mysql 23304 |
| 5 | `compose.memory-none.yml` | `memory` | `none` | 20519 | 20619 | 9305 | — |
| 6 | `compose.sqlite-nats.yml` | `sqlite` | `nats` | 20520 | 20620 | 9306 | nats 24206 |
| 7 | `compose.sqlite-redis.yml` | `sqlite` | `redis` | 20521 | 20621 | 9307 | redis 26307 |
| 8 | `compose.sqlite-sqlite.yml` | `sqlite` | `sqlite` | 20522 | 20622 | 9308 | — |
| 9 | `compose.sqlite-postgres.yml` | `sqlite` | `postgres` | 20523 | 20623 | 9309 | postgres 25409 |
| 10 | `compose.sqlite-mysql.yml` | `sqlite` | `mysql` | 20524 | 20624 | 9310 | mysql 23310 |
| 11 | `compose.sqlite-none.yml` | `sqlite` | `none` | 20525 | 20625 | 9311 | — |
| 12 | `compose.none-nats.yml` | `none` | `nats` | 20526 | 20626 | 9312 | nats 24212 |
| 13 | `compose.none-redis.yml` | `none` | `redis` | 20527 | 20627 | 9313 | redis 26313 |
| 14 | `compose.none-sqlite.yml` | `none` | `sqlite` | 20528 | 20628 | 9314 | — |
| 15 | `compose.none-postgres.yml` | `none` | `postgres` | 20529 | 20629 | 9315 | postgres 25415 |
| 16 | `compose.none-mysql.yml` | `none` | `mysql` | 20530 | 20630 | 9316 | mysql 23316 |
| 17 | `compose.none-none.yml` | `none` | `none` | 20531 | 20631 | 9317 | — |
| 18 | `compose.redis-nats.yml` | `redis` | `nats` | 20532 | 20632 | 9318 | nats 24218, ring-redis 26418 |
| 19 | `compose.redis-postgres.yml` | `redis` | `postgres` | 20533 | 20633 | 9319 | ring-redis 26419, postgres 25419 |
| 20 | `compose.memory-clickhouse.yml` | `memory` | `clickhouse` | 20534 | 20634 | 9320 | clickhouse 29020 |
| 21 | `compose.sqlite-clickhouse.yml` | `sqlite` | `clickhouse` | 20535 | 20635 | 9321 | clickhouse 29021 |
| 22 | `compose.none-clickhouse.yml` | `none` | `clickhouse` | 20536 | 20636 | 9322 | clickhouse 29022 |

Cada porta é sobrescrita por uma variável (`RELPLOG_PORT`, `RELPLOG_TLS_PORT`,
`RELPLOG_ADMIN_PORT`, `RELPLOG_NATS_PORT`, `RELPLOG_REDIS_PORT`,
`RELPLOG_RING_REDIS_PORT`, `RELPLOG_POSTGRES_PORT`, `RELPLOG_MYSQL_PORT`,
`RELPLOG_CLICKHOUSE_PORT`). No `.env` ela vale para **todo** compose que subir
com ele: duas combinações de pé passariam a disputar a porta; prefira o shell
(`RELPLOG_PORT=2514 docker compose -f ... up -d`).

## Apontar um rsyslog real

O cliente é o `omrelp` (pacote `rsyslog-relp` no Alpine, `rsyslog-relp` no
Debian/Ubuntu, `rsyslog-relp` no RHEL). Um arquivo em `/etc/rsyslog.d/`, com a
porta **publicada no host** da combinação (tabela acima; 20514 na de
referência). `rsyslog/rsyslog.conf` é o mesmo modelo, comentado.

**Em claro:**

```
module(load="omrelp")
action(type="omrelp" name="relp-loghub"
       target="logs.exemplo" port="20514"
       # a fila que nao perde: RAM transbordando para disco, gravada no shutdown
       queue.type="LinkedList" queue.filename="relp-loghub"
       queue.size="100000" queue.maxDiskSpace="1g" queue.saveOnShutdown="on"
       # retentar para sempre; sem isto o rsyslog descarta ao desistir
       action.resumeRetryCount="-1" action.resumeInterval="5")
```

`global(workDirectory="/var/spool/rsyslog")` precisa existir (é o padrão das
distribuições) para a fila em disco. Uma ação sem filtro (`*.*`) encaminha
também as mensagens do próprio `rsyslogd` (facility `syslog`), inclusive os
erros do `omrelp` sobre a própria ação: envolva-a num filtro do que deve ir
(`if prifilt("local0.*") then { action(...) }`, como em `rsyslog/rsyslog.conf`)
ou exclua `syslog.*` (`if $syslogfacility-text != "syslog" then { ... }`).

**Sobre TLS** (servidor com `RELPLOG_LISTEN_TLS=0.0.0.0:20515` dentro do
contêiner; no host, a coluna RELP/TLS da tabela de portas: 20614 na combinação
0; certificados e o mTLS em `tls/README.md`):

```
action(type="omrelp" name="relp-loghub-tls"
       target="logs.exemplo" port="20614" tls="on"
       tls.caCert="/etc/rsyslog.d/tls/ca.crt"
       tls.myCert="/etc/rsyslog.d/tls/client.crt"        # só para mTLS
       tls.myPrivKey="/etc/rsyslog.d/tls/client.key"     # só para mTLS
       tls.authMode="name" tls.permittedPeer=["logs.exemplo"]
       queue.type="LinkedList" queue.filename="relp-loghub-tls"
       queue.size="100000" queue.maxDiskSpace="1g" queue.saveOnShutdown="on"
       action.resumeRetryCount="-1" action.resumeInterval="5")
```

`tls.authMode="name"` confere o nome do certificado do servidor (SAN `dNSName`
ou CN) contra `tls.permittedPeer`; `certvalid` só a cadeia; `fingerprint`
dispensa CA. O certificado apresentado pelo cliente vai para `payload.tls`
(`peer-subject`, `peer-fingerprint`).

Parâmetros do `omrelp` que conversam com o servidor:

| `omrelp` | Padrão | Servidor | Por quê |
|---|---|---|---|
| `windowSize` | 128 | `RELPLOG_RELP_WINDOW` (128) | transações sem resposta antes de o cliente parar de mandar |
| `timeout` | 90 s | `RELPLOG_RELP_BACKPRESSURE_TIMEOUT` (30 s) | o servidor fecha a sessão segurada **antes** de o cliente desistir sozinho; mantenha o do servidor abaixo |
| `rebindInterval` | 0 | — | reconectar a cada N mensagens espalha a carga atrás de um balanceador |
| `template` | `RSYSLOG_ForwardFormat` | — | RFC 3164 com timestamp de alta precisão; `RSYSLOG_SyslogProtocol23Format` manda RFC 5424 |

## O que o rsyslog vê

O `librelp` faz exatamente três coisas com uma resposta (SPEC §5.4), e é isso
que o operador do emissor precisa saber:

| O servidor | O rsyslog | Quando |
|---|---|---|
| `200 OK` | apaga a mensagem da fila: **entregue** | a garantia do ring foi cumprida (cabeçalho de cada compose); também para a filtrada (`[filter]`) e a vazia, que o operador mandou descartar |
| nenhuma resposta **enquanto dura** | para de mandar quando a janela (128) enche; as mensagens esperam **na fila dele** | ring cheio, ring Redis fora, sink DOWN e **rate-limit** (`[ratelimit]` ligado: o emissor acima do limite é desacelerado até o limite, `relplog_ratelimit_held_total`) |
| nenhuma resposta, e a sessão fecha (`serverclose` ou socket) | reconecta e **retransmite** tudo o que não teve `200` | a espera acima passou de `relp.backpressure_timeout` (30 s); shutdown; frame malformado; ociosidade (`idle_timeout`) |
| `500 ...` (raro) | **destrói** essa mensagem e loga `peer sent error response`; depois **suspende a ação e quebra a sessão** (`librelp error 10020 ... suspending`), e as vizinhas da janela já aceitas pelo servidor voltam como **duplicatas** | só a recusa **final** do destino com `ring=none` (`500 refused by the destination`, recusa `logic`, `relplog_events_dropped_total{reason="sink_logic"}`), e a guarda `500 source address unusable`, que um par TCP não alcança. Fora do fluxo normal: `500 command disabled` (`syslog` antes do `open`, a sessão continua), `500 protocol error: connection already open` (segundo `open`, a sessão fecha) e `500 command not supported` (comando desconhecido) |

Consequências:

- **Nada do que passa sozinho recebe `500`.** Ring cheio, ring indisponível,
  destino fora e rate-limit **seguram**: a transação fica sem resposta, a janela de 128 enche,
  o servidor para de ler, o TCP segura o rsyslog e as mensagens esperam **na
  fila dele** (por isso a fila em disco e `action.resumeRetryCount="-1"`).
  Passados 30 s sem escoar, o servidor fecha a sessão; o rsyslog reconecta e
  retransmite. O rate-limit, portanto, **modela** o emissor (ele desacelera até
  o limite), não descarta.
- **Um `500` custa caro**: a mensagem recusada se perde na origem (logada lá) e
  a sessão quebra, com uma reconexão e algumas duplicatas das vizinhas (5 a 6
  por `500` na janela de 128, medido com o rsyslog 8.2410). Por isso ele fica
  reservado à recusa final do destino com `ring=none`, que é rara.
- **Uma retransmissão é mensagem nova**: o RELP não tem id de mensagem, e o
  envelope sai com `uuid` novo. É at-least-once (SPEC §19, "Duplicatas"): só
  duplica o que já tinha sido aceito e não teve o `200 OK` entregue (`kill -9`,
  rede caindo, resposta que não saiu). O shutdown gracioso e o fechamento por
  backpressure entregam as respostas decididas antes de fechar.
- A reconciliação do lado do rsyslog é: enviadas = `200` + `500` (perdidas,
  logadas lá) + retransmitidas (as de uma sessão fechada sem resposta e as
  vizinhas de um `500`). Do lado do servidor (§5.4):
  `relplog_messages_total = accepted + dropped{filter,empty,unaddressable} + relp_unanswered_total` (mais as `sink_logic` com `ring=none`).
- `rate-limit` ligado com um `rate` muito abaixo da taxa do emissor por mais de
  30 s fecha a sessão (como um ring cheio), e o rsyslog retransmite: nada se
  perde, mas a fila do emissor cresce. Fica desligado nestes compose
  (`conf/relplog.ini`).

## Emissor de demonstração

`compose.emitter.yml` acrescenta o serviço `emitter` a qualquer combinação: um
rsyslog 8.2410 do Alpine 3.22 com `rsyslog-relp` (imagem construída de
`rsyslog/Dockerfile`), que manda pelo `omrelp` ao serviço `relplog` na porta do
contêiner (20514). Só sobe com `--profile demo`: um `up` sem o profile não o
inicia.

```bash
docker compose -f compose.memory-sqlite.yml -f compose.emitter.yml --profile demo up -d --build
docker logs -f rls-memory-sqlite-emitter-1        # rsyslogd e o gerador
curl -s http://127.0.0.1:9302/metrics | grep -E '^relplog_messages_(total|accepted_total)'
docker compose -f compose.memory-sqlite.yml -f compose.emitter.yml --profile demo down -v
```

- `rsyslog/demo.sh` sobe o `rsyslogd` e manda, pelo `logger` (`/dev/log` →
  `imuxsock`), uma rajada de `RELPLOG_DEMO_BURST` (100) linhas e depois uma a
  cada `RELPLOG_DEMO_INTERVAL` (1) segundo, com TAG `rls-demo` e `seq=N` na
  mensagem: um buraco na sequência é perda, um `seq` repetido é retransmissão.
- A fila do `omrelp` é disk-assisted, num volume (`emitter-spool`), gravada no
  `SIGTERM`, com `action.resumeRetryCount="-1"`. O emissor sobe sem esperar o
  servidor, de propósito: os primeiros segundos do log mostram
  `could not connect to remote server` e `action 'relp-loghub' suspended`, e
  depois `resumed`, sem nenhuma linha perdida.
- Só a facility `local0` (a do gerador) vai pelo RELP; as mensagens do próprio
  `rsyslogd` vão para o `docker logs`. Os erros aparecem duas vezes (uma crua,
  do stderr, e outra com data).
- Para o TLS, troque a ação em `rsyslog/rsyslog.conf` pela variante comentada,
  `RELPLOG_DEMO_PORT=20515` (a porta TLS **do contêiner**: o emissor fala com o
  serviço pela rede do compose), e suba o servidor com `RELPLOG_LISTEN_TLS`
  (`tls/README.md`).

Conferir o que chegou: com `sink=nats`,

```bash
docker run --rm --network rls-memory-nats_default natsio/nats-box:0.20.0 \
  nats -s nats://nats:4222 stream info LOGHUB_SYSLOG
docker run --rm --network rls-memory-nats_default natsio/nats-box:0.20.0 \
  nats -s nats://nats:4222 stream get LOGHUB_SYSLOG 1
```

com `sink=sqlite` (o arquivo é `events-AAAA-MM-DD.db` com `rotate=daily`):

```bash
docker run --rm -v rls-memory-sqlite_rls-data:/data alpine:3.22 sh -c \
  'apk add -q sqlite && sqlite3 /data/events-*.db "select count(*), max(transport) from syslog_events"'
```

## Configuração e recarga (SIGHUP)

A configuração chega por dois caminhos, cada um com um tipo de chave:

| Onde | O quê | Quando vale |
|---|---|---|
| env: `common.yml`, cada compose, `.env` | tudo o que **exige restart** (listen, relp, tcp, tls, ring, sink, `[syslog]`, shutdown...) | no `up` que recria o contêiner |
| `conf/relplog.ini`, lido via `RELPLOG_CONFIG=/etc/relplog/relplog.ini` | **só** as recarregáveis (SPEC §16.3): `filter.*`, `ratelimit.*`, `log.level`, `log.access`, `log.access_sample`, `sink.sqlite.keep_files` | no `SIGHUP` |

Os nomes de env são `RELPLOG_<SEÇÃO>_<CHAVE>` (`RELPLOG_RELP_INFLIGHT_MAX_BYTES`),
com as exceções das skills: `[syslog]` é `SYSLOG_<CHAVE>` (os **mesmos** nomes
do `go-syslog-server`, para que um `.env` comum dê a mesma conversão), `[loghub]`
é `LOGHUB_<CHAVE>` e `server.data_dir` é `DATADIR`.

Para mudar uma recarregável sem reiniciar:

```bash
vi conf/relplog.ini                     # ex.: [filter] min_severity = warning
docker kill -s HUP rls-memory-nats      # ou: docker compose -f compose.memory-nats.yml kill -s HUP relplog
docker logs --since 1m rls-memory-nats 2>&1 | grep SIGHUP
```

O log diz o que aconteceu:

| Linha | Significado |
|---|---|
| `SIGHUP: applied` com `keys=[...]` | trocado a quente |
| `SIGHUP: configuration unchanged` | nada mudou no arquivo |
| `ERROR SIGHUP: the new configuration is invalid and was NOT applied` | arquivo inválido; o servidor segue com a config anterior |
| `WARN SIGHUP: this key requires a restart and was ignored` | uma chave de restart foi posta no INI (e o env não a define); repete em todo HUP até o restart |
| `WARN SIGHUP: this key is set in the config file, but an environment variable or a flag decides its value` | a chave também está no env, que vence (veja abaixo). Vale para recarregável e para de restart: a do arquivo não vale nem no HUP nem no próximo boot |

Uma mensagem segurada no backpressure no momento do `HUP` não é reavaliada:
filtro e rate-limit decidem uma vez por mensagem (§5.7).

**Por que não pelo env.** Env vence o INI (SPEC §16.1) e o env de um contêiner
não muda depois do `up`: uma chave recarregável definida por env nunca mudaria
com o `HUP`. Por isso nenhum arquivo deste diretório define chaves recarregáveis
por env, e uma `RELPLOG_FILTER_*` ou `RELPLOG_RATELIMIT_*` no `.env` não chega ao
contêiner. Não as acrescente ao `environment:` de um compose.

**A pasta, não o arquivo.** O bind é de `conf/`, não de `conf/relplog.ini`: um
bind de arquivo prende o inode, e um editor que salva por *rename* deixaria o
contêiner lendo a versão antiga para sempre. A subpasta `conf/tls/` existe só
como ponto de montagem de `tls/` (`conf/tls/README.md`): sem ela, com `conf/`
só leitura, o Docker não consegue criar `/etc/relplog/tls` e o contêiner não sobe.

Todos os compose montam o **mesmo** `conf/`: uma edição vale para toda
combinação de pé, no `HUP` de cada uma. Para configs distintas, copie a pasta
(com `conf/tls/`) e troque o `./conf` do volume em `common.yml`.

## Identidade LogHub

O servidor chama o `go-loghub-ident` no boot. No contêiner:

| Variável | Valor | Por quê |
|---|---|---|
| `DATADIR` | `/data` (da imagem) | `agent_uuid` e `machine_id` são gerados e persistidos ali, junto de `ring.db`, `events.db` e `emergency/` |
| `AGENT_NAME` | `go-relplog-server` (da imagem) | sem ele o nome viria de `argv[0]` |
| `HOSTNAME` | o `hostname:` de cada serviço (`rls-<ring>-<sink>`) | sem ele seria o id do contêiner, que muda a cada recriação |
| `WORKSPACE` | `RELPLOG_WORKSPACE` do `.env` (`default`) | o **tenant**: vira o `space` de todo envelope e o subject `loghub.<space>.syslog`. O compose nunca lê o `WORKSPACE` do shell, que o Jenkins usa para o caminho do job |

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
biblioteca aceita mas o subject `loghub.<space>.syslog` não: um segmento vazio,
como `a..b`, `.x` ou `x.`. A gramática do servidor é
`^[a-z0-9-]+(\.[a-z0-9-]+)*$` com até 64 caracteres; acima de 64 a biblioteca já
recusou com 111.

## NATS

O stream `LOGHUB_SYSLOG` e o subject `loghub.<space>.syslog` são **os mesmos do
`go-syslog-server`**: os dois publicam `type=syslog`, e o consumidor separa pelo
`from` ou pelo `payload.x-transport` (`"relp"` em todo envelope deste). Os
compose daqui sobem um NATS próprio; apontando os dois servidores para o mesmo
eventbus, um stream só serve aos dois.

O stream é criado pelo serviço `nats-init` (`natsio/nats-box`, versão fixada em
`RELPLOG_NATS_BOX_IMAGE`) a partir de `nats/stream.json`, que cobre
`loghub.default.syslog` e `loghub.default.syslog.>` (subjects granulares). O
servidor **nunca** cria nem altera um stream: ele valida no boot e sai com 3 se
o stream não cobrir o subject, ou se ele cobrir `loghub._ctl.*` (um stream em
`loghub.>` reproduziria beacons antigos para todo consumidor). Com outro
`WORKSPACE`, troque os subjects do `stream.json` junto.

O heartbeat beacon sai em `loghub._ctl.beacon` a cada `LOGHUB_BEACON_INTERVAL`
segundos, por NATS core (fanout), só com `sink=nats`.

`RELPLOG_SINK_NATS_MODE=core` publica sem JetStream (*best effort*: uma mensagem
sem assinante some, depois de o rsyslog já ter recebido o `200 OK`).

## PostgreSQL: a fila `loghub.inbox`

Com `sink=postgres` o servidor **enfileira** na `loghub.inbox`, a fila comum de
envelopes JLH do LogHub (SPEC §3.7.1), e não numa tabela própria. Os
`compose.*-postgres.yml` sobem o PostgreSQL 18 e montam `postgres/inbox.sql` (a
DDL literal da skill `loghub-postgres-table-inbox`) em
`/docker-entrypoint-initdb.d`, que a imagem executa **só na criação do volume**.

A imagem 18 guarda os dados em `/var/lib/postgresql/18/docker`: o volume é
montado em `/var/lib/postgresql`, e um volume no antigo
`/var/lib/postgresql/data` é recusado por ela.

O consumo é de outro software, sempre em ordem de `uuid` e retirando a linha na
mesma transação que grava o resultado:

```sql
WITH job AS (
  SELECT uuid FROM loghub.inbox ORDER BY uuid LIMIT 100 FOR UPDATE SKIP LOCKED
)
DELETE FROM loghub.inbox i USING job WHERE i.uuid = job.uuid RETURNING i.*;
```

Tamanho e atraso da fila:

```sql
SELECT count(*) AS na_fila, now() - min(received_at) AS atraso FROM loghub.inbox;
```

## ClickHouse: a tabela `inbox_syslog`

Com `sink=clickhouse` o servidor grava na `inbox_syslog` (SPEC §3.7.2), o
espelho da `loghub.inbox` num `ReplacingMergeTree` ordenado por
`(event_at, uuid)`, com o `payload` no tipo `JSON`. Os
`compose.{memory,sqlite,none}-clickhouse.yml` sobem o ClickHouse 25.8 ao lado
(protocolo nativo publicado em `127.0.0.1:29020`, `29021` e `29022`).

- **Não é a `inbox` dos servidores de webhook:** o *hint* do `payload` é o do
  syslog, `` `structured-data` Map(String, Map(String, Dynamic)) ``. O tipo
  `JSON` descarta objeto vazio, e sem ele um `[origin]` sem parâmetros
  sumiria; os SD-IDs e PARAM-NAMEs ficam literais como chaves de `Map`.
- **Leitura:** uma linha sem SD volta com `"structured-data":{}`, que o
  servidor nunca escreve: trate `{}` como ausente. Até o merge, leia com
  `FINAL`:

  ```bash
  docker compose -f compose.memory-clickhouse.yml exec clickhouse \
    clickhouse-client -u syslog --password syslog -d syslog \
    -q 'SELECT count(), max(event_at) FROM inbox_syslog FINAL'
  ```

- **O servidor nunca cria nem altera a tabela.** `clickhouse/init.sh` aplica
  `clickhouse/inbox_syslog.sql` uma vez, na criação do volume, dentro de
  `CLICKHOUSE_DB` (a imagem rodaria um `.sql` do `initdb.d` no banco
  `default`), e o healthcheck só fica verde quando a tabela existe. O boot
  valida versão, motor, chave, tipos, o *hint*, ausência de CHECK, o
  `max_memory_usage` contra o pior pedaço e um `INSERT` vazio de sonda, e sai
  com 3 na hora se algo faltar.
- **`async_insert` e a durabilidade são do perfil do usuário** no ClickHouse;
  os compose deixam o padrão da imagem (`async_insert = 0`). Com `ring=none`, o
  `200 OK` RELP sai quando o `INSERT` é respondido: com `async_insert = 1` e
  `wait_for_async_insert = 0`, isso é o buffer do ClickHouse.
- Não há esquema `clickhouse://` na `LOGHUB_EB_URL`: configure pelas chaves
  `RELPLOG_SINK_CLICKHOUSE_*`.

## LOGHUB_EB_URL

`LOGHUB_EB_URL` (SPEC §10.9) é a URL do eventbus da convenção LogHub: o
**esquema** escolhe o sink e a URL preenche a conexão. Os compose não a repassam
por padrão: um `LOGHUB_EB_URL` exportado no shell de quem roda o compose (comum
num ambiente LogHub) colidiria com as chaves de conexão de cada arquivo, e o boot
sairia com 1.

Para usá-la num compose de `nats`, `redis` ou `postgres`, edite o
`environment:` do serviço `relplog`: o comentário `# Conexao:` abre as quatro
chaves que a URL substitui, e logo abaixo delas vem a linha da URL, comentada.
Apague as quatro, descomente a da URL e defina o valor no `.env` (ou no shell):

```yaml
      # antes (compose.memory-nats.yml)
      RELPLOG_SINK_NATS_URLS: nats://nats:4222
      RELPLOG_SINK_NATS_USERNAME:
      RELPLOG_SINK_NATS_PASSWORD:
      RELPLOG_SINK_NATS_TLS:
      #LOGHUB_EB_URL: ${LOGHUB_EB_URL:?defina LOGHUB_EB_URL no .env ou no shell}

      # depois
      LOGHUB_EB_URL: ${LOGHUB_EB_URL:?defina LOGHUB_EB_URL no .env ou no shell}
```

```bash
echo "LOGHUB_EB_URL=nats://usuario:senha@nats.exemplo:4222" >> .env
docker compose -f compose.memory-nats.yml up -d --build
```

Não há atalho por arquivo de override: o `!reset` do Compose não remove uma
chave de um serviço que usa `extends`, e todos estes usam.

| Esquema | Sink | Chaves donas (env) |
|---|---|---|
| `nats://`, `tls://` (lista com vírgula = cluster) | `nats` | `RELPLOG_SINK_NATS_URLS`, `_TLS`, `_USERNAME`, `_PASSWORD`, `_TOKEN` |
| `redis://`, `rediss://` | `redis` | `RELPLOG_SINK_REDIS_ADDRS`, `_USERNAME`, `_PASSWORD`, `_DB`, `_TLS` |
| `postgres://`, `postgresql://` | `postgres` | `RELPLOG_SINK_POSTGRES_ADDR`, `_USERNAME`, `_PASSWORD`, `_DATABASE`, `_TLS` |

As regras:

- **Uma fonte só.** Com a URL preenchida, uma chave dona definida também por
  env, flag ou INI é exit 1 com as duas origens no log, **mesmo que a URL não
  traga aquela parte**: uma URL sem senha quer dizer "sem senha". Por isso as
  quatro linhas saem do compose, e nenhuma chave dona pode ficar no
  `environment:` nem em `conf/relplog.ini`.
- **Usuário e senha vão dentro da URL.** TLS também: `tls://` no NATS,
  `rediss://` no Redis, `sslmode=require` (ou `verify-ca`, `verify-full`) no
  PostgreSQL. No compose de postgres, a URL precisa da senha que o serviço
  `postgres` criou: `postgres://syslog:syslog@postgres:5432/syslog`.
- **O esquema tem de bater com o sink do compose**: cada compose fixa
  `RELPLOG_SINK_TYPE`, e um esquema diferente é exit 1. Não há esquema para
  MySQL nem para ClickHouse: `compose.*-mysql.yml`, `*-clickhouse.yml`,
  `*-sqlite.yml` e `*-none.yml` não usam a URL.
- As demais chaves do sink (`stream`, `mode`, `subjects`, `table`, `pool_size`,
  `credentials`, `tls_ca`, `tls_insecure`, `sentinel_master`) convivem com a URL.
- Apontando para um eventbus externo, o `nats`/`redis`/`postgres` local e o
  `nats-init` do compose sobem do mesmo jeito e ficam ociosos; o stream (ou a
  tabela) do destino externo precisa existir, porque o servidor nunca o cria.

Sem a URL valem as chaves `RELPLOG_SINK_*`. As opcionais (usuário e senha,
`db` do Redis, `tls` do NATS) aparecem nos compose **sem valor** e só entram no
contêiner quando definidas no `.env` ou no shell: nunca vazias, porque uma env
vazia ainda conta como explícita e travaria a URL.

## TLS

```bash
# certificados: tls/README.md (CA, servidor com SAN, cliente para mTLS)
RELPLOG_LISTEN_TLS=0.0.0.0:20515 docker compose up -d
```

O listener TLS fica na 20515 do contêiner, publicada na `20614+n` do host. O
RELP em claro continua ligado ao lado; para só TLS, acrescente
`RELPLOG_LISTEN_TCP: ""` ao `environment:` do serviço `relplog` no compose (os
dois desligados é exit 1).

A `server.key` precisa ser legível pelo **uid 65532**, o usuário da imagem;
`tls/README.md` diz como.

`RELPLOG_TLS_CLIENT_AUTH=optional` (padrão) pede o certificado do cliente e, com
`RELPLOG_TLS_CLIENT_CA`, o verifica quando ele vem; `require` exige mTLS. O
`payload.tls` registra versão, suíte, subject e fingerprint SHA-256. O
certificado é lido no boot: trocá-lo exige restart.

## Ring Redis compartilhado

`compose.redis-*.yml` sobe um Redis só para o ring (`ring-redis`), separado do
destino, no stream `relplog:ring` com o grupo `relplog-ring` (o
`go-syslog-server` usa `syslog:ring`: os dois podem dividir um Redis sem se
misturar). Várias réplicas apontando para o mesmo stream compartilham o teto de
capacidade e se recuperam umas às outras (`XAUTOCLAIM`). Duas regras:

- **todas as réplicas com a mesma config de conversão**, que é o `[syslog]`
  inteiro (SPEC §3.5): `SYSLOG_DEFAULT_PRIORITY`, `SYSLOG_RFC3164_TIMEZONE`,
  `SYSLOG_PAYLOAD_RAW`, `SYSLOG_LEGACY_CHARSET`, `SYSLOG_ACCEPT_MALFORMED`,
  `SYSLOG_SD_MAX_ELEMENTS`, `SYSLOG_SD_MAX_PARAMS` e `SYSLOG_MAX_MESSAGE_BYTES`
  (este atua na recepção, mas também exige restart); mais o mesmo `WORKSPACE` e
  a mesma versão da imagem. Uma réplica pode drenar o que outra recebeu, e o
  envelope precisa sair igual, byte a byte, com o mesmo `uuid`. O boot não tem
  como conferir isso;
- `ring=redis` e `sink=redis` no **mesmo** servidor viram `ring=memory` com
  `WARN`: o ring não protegeria nada que o destino já não perca junto.

Com o ring Redis fora do ar, as transações ficam seguradas como com o ring
cheio (sem resposta; depois de 30 s a sessão fecha e o rsyslog retransmite).

Reconciliar o contador de bytes à mão (com o tráfego parado):

```bash
redis-cli EVAL "local t=0 for _,e in ipairs(redis.call('XRANGE',KEYS[1],'-','+')) do \
  for i=1,#e[2],2 do if e[2][i]=='b' then t=t+#e[2][i+1] end end end \
  redis.call('SET',KEYS[2],t) return t" 2 relplog:ring relplog:ring:bytes
```

## Saúde

O `healthcheck` dos compose consulta **`/healthz`** na 9103: liveness, `200`
enquanto o processo vive. **`/readyz` não é o healthcheck, de propósito.** Ele dá
`503`, com o motivo em texto, quando o sink não está UP, quando o ring passa de
`ring.high_water`, durante o dreno do backlog de boot, com o ring Redis
inalcançável, antes de os transportes abrirem e no shutdown. Nenhuma dessas
condições se resolve reiniciando o contêiner: um restart não conserta um destino
inalcançável, só interrompe o dreno e fecha as sessões RELP (e, com
`ring=memory`, troca o backlog da RAM por um dump e uma reingestão). E quem olha
o healthcheck age sobre um contêiner `unhealthy`: o
`depends_on: condition: service_healthy` não sobe os dependentes, o Swarm e as
ferramentas de *autoheal* o reiniciam.

`/readyz` é para o balanceador (tirar a instância de rotação) e para o
operador. A porta admin só é publicada no loopback do host:

```bash
curl -sS -w '%{http_code}\n' http://127.0.0.1:9300/readyz     # 200 "ready", ou 503 e o motivo
curl -s http://127.0.0.1:9300/metrics | grep -E '^relplog_(sink_up|ring_items|ring_bytes|messages_accepted_total|relp_unanswered_total)'
docker exec rls-memory-nats wget -qO- http://127.0.0.1:9103/readyz   # de dentro
```

Para alertar, use `/metrics`, não o estado do healthcheck:

| Métrica | O que diz |
|---|---|
| `relplog_sink_up` | destino UP |
| `relplog_ring_items` contra `relplog_ring_capacity_items` | ocupação do ring |
| `relplog_messages_dropped_total{reason}` | descartes de propósito: só `filter`, `empty` e `unaddressable` (o rate-limit não descarta) |
| `relplog_ratelimit_held_total` | transações seguradas pelo rate-limit: emissores acima do limite sendo desacelerados |
| `relplog_backpressure_seconds_total` | tempo com transações seguradas (ring cheio, sink DOWN, rate-limit), somado por transação |
| `relplog_relp_unanswered_total{reason}` | transações devolvidas ao rsyslog sem resposta (`backpressure`, `shutdown`, `conn_lost`): serão retransmitidas |
| `relplog_relp_frame_errors_total{reason}` | frames malformados ou acima de `relp.max_frame_bytes` (a sessão fecha) |
| `relplog_relp_inflight_bytes`, `relplog_relp_inflight_waits_total` | cópias em voo contra `relp.inflight_max_bytes` |
| `relplog_tls_handshake_errors_total` | cliente sem TLS na porta TLS, CA errada, mTLS recusado |

## Avisos esperados no boot

| Compose | `WARN` | Por que fica |
|---|---|---|
| todos | `ratelimit.enabled=false with listen.tcp=0.0.0.0:20514 reachable off this host` | o listener precisa de `0.0.0.0` para receber as portas publicadas, e o rate-limit fica desligado porque, atrás do NAT do Docker, o IP de origem pode ser o do gateway para todos (`[ratelimit]` de `conf/relplog.ini`): um limitador ligado desaceleraria todos os emissores juntos |
| `compose.sqlite-sqlite.yml` | `ring.mode=sqlite with sink.type=sqlite: every message pays TWO fsyncs` | é a combinação (SPEC §9.7); o cabeçalho do arquivo diz quando ela compensa |

Os compose de postgres, mysql e clickhouse fixam `RELPLOG_SINK_WORKERS=4`, e os
`compose.none-{postgres,mysql,clickhouse}.yml` fixam `RELPLOG_RING_MAX_INFLIGHT=4`, igual ao
pool derivado (`workers`; a sonda de saúde tem conexão própria), para não subirem com os `WARN` de §10.5 e §9.6.

## Memória

`GOMEMLIMIT` (512 MiB) e o limite do contêiner (768M) vêm de `common.yml`. Com
`ring=memory`, o `GOMEMLIMIT` precisa cobrir duas vezes `RELPLOG_RING_MAX_BYTES`
(256 MiB nesses compose); suba os três juntos. A recepção RELP soma fora do
ring (SPEC §15.3a): até `relp.inflight_max_bytes` (64 MiB) de cópias em voo
para o processo inteiro, mais ~10 KiB por conexão.

## Desligamento

`docker stop` manda `SIGTERM`: `/readyz` falha, `pre_stop_delay` (0 s), as
sessões RELP entregam as respostas já decididas e fecham com `serverclose` (o
segurado fica sem resposta e o rsyslog o retransmite), o ring drena para o sink
em até `drain_timeout` (30 s). O `stop_grace_period` de `common.yml` (45 s)
precisa ser **maior** que `pre_stop_delay + drain_timeout + 6 s` (o `closeGrace`
das sessões, §5.8): menor, o Docker mata o processo no meio do dreno e perde
mensagens que já tiveram `200 OK`. Quem aumenta um aumenta o outro.

## Problemas comuns

| No rsyslog (`docker logs`, journal) | Causa | O que fazer |
|---|---|---|
| `could not connect to remote server, librelp error 10014` e `action 'relp-loghub' suspended` | servidor fora, porta errada (no host é a publicada, `20514+n`; dentro da rede do compose, 20514), ou `tcp.max_conns` cheio | nada se perde com a fila em disco e `resumeRetryCount=-1`: o rsyslog retenta a cada `resumeInterval` e loga `resumed` |
| `peer sent error response`, seguido de `librelp error 10020` e da ação `suspended` | o servidor respondeu `500`: aquela mensagem foi **destruída na origem**, a sessão quebrou, e as vizinhas da janela voltam como duplicatas | `relplog_events_dropped_total{reason="sink_logic"}`: o destino recusou a linha para sempre (`ring=none`); o log do servidor diz qual erro. Fora disso, só um emissor fora do protocolo (`command disabled`, `connection already open`, `command not supported`) |
| emissor lento, fila do rsyslog crescendo, sem erro | rate-limit segurando (`relplog_ratelimit_held_total` subindo) ou backpressure (`relplog_backpressure_seconds_total`) | suba `rate`/`burst` em `conf/relplog.ini` e mande `HUP` (as seguradas saem na retentativa seguinte), ou olhe o destino em `/readyz` |
| `librelp error 10008 forwarding to server ... - suspending`, a ação `suspended` e, um segundo depois, `resumed` | o servidor fechou a sessão: ring cheio, sink DOWN ou rate-limit segurando por mais de `relp.backpressure_timeout` (log do servidor: `closing a RELP session after holding a transaction`), `idle_timeout` (sessão ociosa por 120 s), frame malformado, shutdown | normal no ocioso e no deploy; se for backpressure, `relplog_relp_unanswered_total{reason="backpressure"}` sobe e `/readyz` diz o motivo: o problema é o destino, não o rsyslog |
| reconexões e erro de TLS (`handshake`, `certificate`) | cliente sem `tls="on"` na porta TLS (ou com TLS na porta em claro), CA errada, `permittedPeer` que não casa com o SAN, mTLS exigido sem `tls.myCert` | `relplog_tls_handshake_errors_total`; confira os nomes com `openssl x509 -noout -text -in server.crt` |
| mensagens repetidas no destino | retransmissão sem id (at-least-once, §19); com o rsyslog 8.2410 / librelp 1.11, a mensagem que levou a ação a `suspended` é reenviada uma vez no `resumed`, mesmo já confirmada | deduplique no consumidor por conteúdo + janela de tempo, se precisar |

No servidor: `docker logs rls-<ring>-<sink>` (JSON); `relplog_messages_total` sem
`relplog_messages_accepted_total` acompanhando aponta filtro, rate-limit
(segurando) ou backpressure.

## Códigos de saída

| Código | Significado |
|---|---|
| `0` | shutdown completo: ring vazio, ou `sqlite`/`redis` com o resto guardado |
| `1` | erro de configuração ou de inicialização não relacionada a destino |
| `3` | destino indisponível: sink FATAL, `boot_timeout` ou `drain_timeout` estourados, ring Redis indisponível no boot, maior evento que não cabe, guarda de não-progresso do dreno |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` ([acima](#identidade-loghub)) |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 7–10 do boot |

## Garantias

`docs/SPEC.md` §19. Resumo: o `200 OK` de cada mensagem só sai depois da
garantia do ring (cabeçalho de cada compose); o que não pode ser cumprido agora
fica sem resposta (o rate-limit também: ele desacelera, não descarta) e volta
ao rsyslog pela retransmissão se passar de 30 s; só a recusa final do destino
com `ring=none` dá `500`, que o rsyslog descarta e loga, quebrando a sessão; e
`sink=none` descarta tudo.
