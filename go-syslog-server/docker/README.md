# go-syslog-server em contêiner

Um compose por combinação de **ring** × **sink** (docs/SPEC.md §2.1, §9, §10).
A combinação padrão, e a de referência, é `ring=memory` → `sink=nats` (JetStream),
o eventbus do LogHub:

```bash
cd docker
docker compose up -d --build                       # memory -> nats
docker compose -f compose.sqlite-postgres.yml up -d --build
```

| Arquivo | Ring | Sink |
|---|---|---|
| `compose.memory-{nats,redis,sqlite,postgres,mysql,clickhouse,none}.yml` | memória (dump de emergência em FATAL e no `SIGTERM` que estoura o dreno) | o do nome |
| `compose.sqlite-{...}.yml` | `ring.db` com `fsync` | o do nome |
| `compose.none-{...}.yml` | sem ring (escrita síncrona) | o do nome |
| `compose.redis-nats.yml`, `compose.redis-postgres.yml` | stream Redis **compartilhado** entre réplicas | o do nome |

Os `*-none.yml` são **benchmark**: `sink=none` converte, serializa e descarta
tudo (§10.8, §19). Não guardam nada.

Cada compose publica portas distintas para que várias combinações rodem lado a
lado; a de referência usa `514` (UDP e TCP), `6514` (TLS, quando ligado) e
`127.0.0.1:9102` (admin). O cabeçalho de cada arquivo diz as suas.

| Pasta | Montada em | Para quê |
|---|---|---|
| `conf/` | `/etc/syslog:ro` | `syslog.ini`, as chaves recarregáveis ([abaixo](#configuração-e-recarga-sighup)) |
| `tls/` | `/etc/syslog/tls:ro` | certificados do TLS (`tls/README.md`) |
| volume `sls-data` | `/data` | a única área gravável: identidade, `ring.db`, `events.db`, `emergency/` |

## Configuração e recarga (SIGHUP)

A configuração chega por dois caminhos, cada um com um tipo de chave:

| Onde | O quê | Quando vale |
|---|---|---|
| env: `common.yml`, cada compose, `.env` | tudo o que **exige restart** (listen, ring, sink, `[syslog]`, shutdown...) | no `up` que recria o contêiner |
| `conf/syslog.ini`, lido via `SYSLOG_CONFIG=/etc/syslog/syslog.ini` | **só** as recarregáveis (SPEC §16.3): `filter.*`, `ratelimit.*`, `log.level`, `log.access`, `log.access_sample`, `sink.sqlite.keep_files` | no `SIGHUP` |

Para mudar uma recarregável sem reiniciar:

```bash
vi conf/syslog.ini                      # ex.: [filter] min_severity = warning
docker kill -s HUP sls-memory-nats      # ou: docker compose -f compose.memory-nats.yml kill -s HUP syslog
docker logs --since 1m sls-memory-nats 2>&1 | grep SIGHUP
```

O log diz o que aconteceu:

| Linha | Significado |
|---|---|
| `SIGHUP: applied` com `keys=[...]` | trocado a quente |
| `SIGHUP: configuration unchanged` | nada mudou no arquivo |
| `ERROR SIGHUP: the new configuration is invalid and was NOT applied` | arquivo inválido; o servidor segue com a config anterior |
| `WARN SIGHUP: this key requires a restart and was ignored` | uma chave de restart foi posta no INI (e o env não a define); repete em todo HUP até o restart |
| `WARN SIGHUP: this key is set in the config file, but an environment variable or a flag decides its value` | a chave também está no env, que vence (veja abaixo). Vale para recarregável e para de restart: a do arquivo não vale nem no HUP nem no próximo boot |

**Por que não pelo env.** Env vence o INI (SPEC §16.1) e o env de um contêiner
não muda depois do `up`: uma chave recarregável definida por env nunca mudaria
com o `HUP`. Por isso nenhum arquivo deste diretório define chaves recarregáveis
por env, e uma `SYSLOG_FILTER_*` ou `SYSLOG_RATELIMIT_*` no `.env` não chega ao
contêiner. Não as acrescente ao `environment:` de um compose.

**A pasta, não o arquivo.** O bind é de `conf/`, não de `conf/syslog.ini`: um
bind de arquivo prende o inode, e um editor que salva por *rename* deixaria o
contêiner lendo a versão antiga para sempre. A subpasta `conf/tls/` existe só
como ponto de montagem de `tls/` (`conf/tls/README.md`): sem ela, com `conf/`
só leitura, o Docker não consegue criar `/etc/syslog/tls` e o contêiner não sobe.

Todos os compose montam o **mesmo** `conf/`: uma edição vale para toda
combinação de pé, no `HUP` de cada uma. Para configs distintas, copie a pasta
(com `conf/tls/`) e troque o `./conf` do volume em `common.yml`.

## Identidade LogHub

O servidor chama o `go-loghub-ident` no boot. No contêiner:

| Variável | Valor | Por quê |
|---|---|---|
| `DATADIR` | `/data` (da imagem) | `agent_uuid` e `machine_id` são gerados e persistidos ali, junto de `ring.db`, `events.db` e `emergency/` |
| `AGENT_NAME` | `go-syslog-server` (da imagem) | sem ele o nome viria de `argv[0]` |
| `HOSTNAME` | o `hostname:` de cada serviço | sem ele seria o id do contêiner, que muda a cada recriação |
| `WORKSPACE` | `SYSLOG_WORKSPACE` do `.env` (`default`) | o **tenant**: vira o `space` de todo envelope e o subject `loghub.<space>.syslog`. O compose nunca lê o `WORKSPACE` do shell, que o Jenkins usa para o caminho do job |

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

O stream `LOGHUB_SYSLOG` é criado pelo serviço `nats-init` (`natsio/nats-box`,
versão fixada em `SYSLOG_NATS_BOX_IMAGE`) a partir de `nats/stream.json`, que
cobre `loghub.default.syslog` e `loghub.default.syslog.>` (subjects granulares).
O servidor **nunca** cria nem altera um stream: ele valida no boot e sai com 3
se o stream não cobrir o subject, ou se ele cobrir `loghub._ctl.*` (um stream em
`loghub.>` reproduziria beacons antigos para todo consumidor). Com outro
`WORKSPACE`, troque os subjects do `stream.json` junto.

O heartbeat beacon sai em `loghub._ctl.beacon` a cada `LOGHUB_BEACON_INTERVAL`
segundos, por NATS core (fanout), só com `sink=nats`.

`SYSLOG_SINK_NATS_MODE=core` publica sem JetStream (*best effort*: uma mensagem
sem assinante some).

## PostgreSQL: a fila `loghub.inbox`

Com `sink=postgres` o servidor **enfileira** na `loghub.inbox`, a fila comum de
envelopes JLH do LogHub (SPEC §3.7.1), e não numa tabela própria. Os
`compose.*-postgres.yml` sobem o PostgreSQL 18 e montam `postgres/inbox.sql` (a
DDL literal da skill `loghub-postgres-table-inbox`) em
`/docker-entrypoint-initdb.d`, que a imagem executa **só na criação do volume**.
**Um volume criado pelos compose antigos não serve:** ele tem dados do
PostgreSQL 17 (montados em `/var/lib/postgresql/data`), e a imagem 18 se recusa
a subir sobre eles ("there appears to be PostgreSQL data … pg_upgrade"); o
serviço `syslog` nem chega a iniciar, porque espera o banco saudável. A tabela
`syslog_events` antiga não é migrada para a fila de qualquer modo: recrie o
volume (`docker compose -f compose.<ring>-postgres.yml down -v`) ou faça o
`pg_upgrade` por conta própria e aplique `postgres/inbox.sql` com `psql`.

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
(protocolo nativo publicado em `127.0.0.1:9010`, `9011` e `9012`; syslog em
`5160`–`5162`, admin em `19120`–`19122`).

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
  os compose deixam o padrão da imagem (`async_insert = 0`).
- Não há esquema `clickhouse://` na `LOGHUB_EB_URL`: configure pelas chaves
  `SYSLOG_SINK_CLICKHOUSE_*`.

## LOGHUB_EB_URL

`LOGHUB_EB_URL` (SPEC §10.9) é a URL do eventbus da convenção LogHub: o
**esquema** escolhe o sink e a URL preenche a conexão. Os compose não a repassam
por padrão: um `LOGHUB_EB_URL` exportado no shell de quem roda o compose (comum
num ambiente LogHub) colidiria com as chaves de conexão de cada arquivo, e o boot
sairia com 1.

Para usá-la num compose de `nats`, `redis` ou `postgres`, edite o
`environment:` do serviço `syslog`: o comentário `# Conexao:` abre as quatro
chaves que a URL substitui, e logo abaixo delas vem a linha da URL, comentada. Apague as quatro,
descomente a da URL e defina o valor no `.env` (ou no shell):

```yaml
      # antes (compose.memory-nats.yml)
      SYSLOG_SINK_NATS_URLS: nats://nats:4222
      SYSLOG_SINK_NATS_USERNAME:
      SYSLOG_SINK_NATS_PASSWORD:
      SYSLOG_SINK_NATS_TLS:
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
| `nats://`, `tls://` (lista com vírgula = cluster) | `nats` | `SYSLOG_SINK_NATS_URLS`, `_TLS`, `_USERNAME`, `_PASSWORD`, `_TOKEN` |
| `redis://`, `rediss://` | `redis` | `SYSLOG_SINK_REDIS_ADDRS`, `_USERNAME`, `_PASSWORD`, `_DB`, `_TLS` |
| `postgres://`, `postgresql://` | `postgres` | `SYSLOG_SINK_POSTGRES_ADDR`, `_USERNAME`, `_PASSWORD`, `_DATABASE`, `_TLS` |

As regras:

- **Uma fonte só.** Com a URL preenchida, uma chave dona definida também por
  env, flag ou INI é exit 1 com as duas origens no log, **mesmo que a URL não
  traga aquela parte**: uma URL sem senha quer dizer "sem senha". Por isso as
  quatro linhas saem do compose, e nenhuma chave dona pode ficar no
  `environment:` nem em `conf/syslog.ini`.
- **Usuário e senha vão dentro da URL.** TLS também: `tls://` no NATS,
  `rediss://` no Redis, `sslmode=require` (ou `verify-ca`, `verify-full`) no
  PostgreSQL. No compose de postgres, a URL precisa da senha que o serviço
  `postgres` criou: `postgres://syslog:syslog@postgres:5432/syslog`.
- **O esquema tem de bater com o sink do compose**: cada compose fixa
  `SYSLOG_SINK_TYPE`, e um esquema diferente é exit 1. Não há esquema para
  MySQL nem para ClickHouse: `compose.*-mysql.yml`, `*-clickhouse.yml`,
  `*-sqlite.yml` e `*-none.yml` não usam a URL.
- As demais chaves do sink (`stream`, `mode`, `subjects`, `table`, `pool_size`,
  `credentials`, `tls_ca`, `tls_insecure`, `sentinel_master`) convivem com a URL.
- Apontando para um eventbus externo, o `nats`/`redis`/`postgres` local e o
  `nats-init` do compose sobem do mesmo jeito e ficam ociosos; o stream (ou a
  tabela) do destino externo precisa existir, porque o servidor nunca o cria.

Sem a URL valem as chaves `SYSLOG_SINK_*`. As opcionais (usuário e senha,
`db` do Redis, `tls` do NATS) aparecem nos compose **sem valor** e só entram no
contêiner quando definidas no `.env` ou no shell: nunca vazias, porque uma env
vazia ainda conta como explícita e travaria a URL.

## TLS (RFC 5425)

```bash
cp server.crt server.key docker/tls/          # e clients-ca.crt, para mTLS
SYSLOG_LISTEN_TLS=0.0.0.0:6514 docker compose up -d
```

A `server.key` precisa ser legível pelo **uid 65532**, o usuário da imagem;
`tls/README.md` diz como.

`SYSLOG_TLS_CLIENT_AUTH=optional` (padrão) pede o certificado do cliente e, com
`SYSLOG_TLS_CLIENT_CA`, o verifica quando ele vem; `require` exige mTLS. O
`payload.tls` registra versão, suíte, subject e fingerprint SHA-256.

## Ring Redis compartilhado

`compose.redis-*.yml` sobe um Redis só para o ring (`ring-redis`), separado do
destino. Várias réplicas apontando para o mesmo stream compartilham o teto de
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

Reconciliar o contador de bytes à mão (com o tráfego parado):

```bash
redis-cli EVAL "local t=0 for _,e in ipairs(redis.call('XRANGE',KEYS[1],'-','+')) do \
  for i=1,#e[2],2 do if e[2][i]=='b' then t=t+#e[2][i+1] end end end \
  redis.call('SET',KEYS[2],t) return t" 2 syslog:ring syslog:ring:bytes
```

## Saúde

O `healthcheck` dos compose consulta **`/healthz`**: liveness, `200` enquanto o
processo vive. **`/readyz` não é o healthcheck, de propósito.** Ele dá `503`,
com o motivo em texto, quando o sink não está UP, quando o ring passa de
`ring.high_water`, durante o dreno do backlog de boot, com o ring Redis
inalcançável, antes de os transportes abrirem e no shutdown. Nenhuma dessas
condições se resolve reiniciando o contêiner: um restart não conserta um destino
inalcançável, só interrompe o dreno (e, com `ring=memory`, troca o backlog da RAM
por um dump e uma reingestão). E quem olha o healthcheck age sobre um contêiner
`unhealthy`: o `depends_on: condition: service_healthy` não sobe os dependentes,
o Swarm e as ferramentas de *autoheal* o reiniciam.

`/readyz` é para o balanceador (tirar a instância de rotação) e para o
operador. A porta admin só é publicada no loopback do host:

```bash
curl -sS -w '%{http_code}\n' http://127.0.0.1:9102/readyz     # 200 "ready", ou 503 e o motivo
watch -n 2 "curl -s http://127.0.0.1:9102/readyz"
curl -s http://127.0.0.1:9102/metrics | grep -E '^syslog_(sink_up|ring_items|ring_bytes)'
docker exec sls-memory-nats wget -qO- http://127.0.0.1:9102/readyz   # de dentro
```

Para alertar, use `/metrics` (`syslog_sink_up`, `syslog_ring_items` contra
`syslog_ring_capacity_items`, `syslog_messages_dropped_total`), não o estado do
healthcheck.

## Avisos esperados no boot

Conferidos carregando o env de cada compose com o `conf/syslog.ini` pelo loader
real; nenhum compose sai com erro de configuração.

| Compose | `WARN` | Por que fica |
|---|---|---|
| todos | `ratelimit.enabled=false with listen.udp=0.0.0.0:1514, listen.tcp=0.0.0.0:1514 reachable off this host` | o listener precisa de `0.0.0.0` para receber as portas publicadas, e o rate-limit fica desligado porque, atrás do NAT do Docker, o IP de origem pode ser o do gateway para todos (`[ratelimit]` de `conf/syslog.ini`) |
| `compose.sqlite-sqlite.yml` | `ring.mode=sqlite with sink.type=sqlite: every message pays TWO fsyncs` | é a combinação (SPEC §9.7); o cabeçalho do arquivo diz quando ela compensa |
| todos, conforme o host | `the socket receive buffer is smaller than requested` | o `net.core.rmem_max` do host está abaixo dos 8 MiB pedidos; suba-o ([UDP](#udp)). O Docker Desktop sobe com ele |

Os compose de postgres, mysql e clickhouse fixam `SYSLOG_SINK_WORKERS=4`, e os
`compose.none-{postgres,mysql,clickhouse}.yml` fixam `SYSLOG_RING_MAX_INFLIGHT=4`, igual ao
pool derivado (`workers`; a sonda de saúde tem conexão própria, fora do pool),
para não subirem com os `WARN` de §10.5 e §9.6.

## UDP

O parâmetro que mais importa é `SYSLOG_LISTEN_UDP_READ_BUFFER`. Dentro do
contêiner o teto é o `net.core.rmem_max` do **host**; sem subi-lo, o pedido é
reduzido em silêncio. O valor efetivo aparece em `syslog_udp_read_buffer_bytes`.

```bash
sysctl -w net.core.rmem_max=16777216
```

## Memória

`GOMEMLIMIT` (512 MiB) e o limite do contêiner (768M) vêm de `common.yml`. Com
`ring=memory`, o `GOMEMLIMIT` precisa cobrir duas vezes `SYSLOG_RING_MAX_BYTES`
(256 MiB nesses compose); suba os três juntos.

## Garantias

`docs/SPEC.md` §19. Resumo: só TCP e TLS seguram o emissor; UDP e unix contam o
que descartam em `syslog_messages_dropped_total`; e `sink=none` descarta tudo.
