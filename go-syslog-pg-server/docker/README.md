# go-syslog-pg-server em contêiner

Uma combinação só (docs/SPEC.md §2.1): **ring em memória → PostgreSQL**, a fila
`loghub.inbox` do LogHub. O `docker-compose.yml` sobe o servidor e um
PostgreSQL 18 com a fila já criada.

```bash
cd docker
cp .env.example .env            # opcional: todo valor tem default
docker compose up -d --build
docker compose logs -f syslog
```

| Publicado no host | Padrão | Variável |
|---|---|---|
| syslog UDP e TCP | `5180` | `SYSLOG_PORT` |
| admin (`/healthz`, `/readyz`, `/metrics`) | `127.0.0.1:19180` | `SYSLOG_ADMIN_PORT` |
| PostgreSQL | `127.0.0.1:15480` | `SYSLOG_POSTGRES_PORT` |

Os padrões não coincidem com nenhuma porta publicada pelos compose do
`go-syslog-server` (syslog 514 e 5141–5162, admin 9102 e 19101–19122), para os
dois rodarem no mesmo host. Para receber na porta padrão do syslog,
`SYSLOG_PORT=514`.

```bash
logger -n 127.0.0.1 -P 5180 -d 'teste UDP'               # util-linux
logger -n 127.0.0.1 -P 5180 -T 'teste TCP'
printf '<13>1 - - app - - - teste' | nc -u -w1 127.0.0.1 5180   # macOS
```

| Pasta | Montada em | Para quê |
|---|---|---|
| `conf/` | `/etc/syslog:ro` | `syslog.ini`, as chaves recarregáveis ([abaixo](#configuração-e-recarga-sighup)) |
| `postgres/inbox.sql` | `/docker-entrypoint-initdb.d` do PostgreSQL | a DDL da fila, aplicada na criação do volume |
| volume `slpg-data` | `/data` | a única área gravável: identidade e `emergency/` |

**Não há TLS** (SPEC §1.2): nem listener RFC 5425, nem conexão cifrada com o
PostgreSQL. A imagem não traz `ca-certificates` nem a porta 6514. Quem precisa
de syslog cifrado põe um terminador TLS na frente (com PROXY protocol,
`SYSLOG_PROXY_PROTOCOL=optional`, para manter o `src-ip`) ou usa o
`go-syslog-server`.

## Configuração e recarga (SIGHUP)

A configuração chega por dois caminhos, cada um com um tipo de chave:

| Onde | O quê | Quando vale |
|---|---|---|
| env: `common.yml`, `docker-compose.yml`, `.env` | tudo o que **exige restart** (listen, ring, sink, `[syslog]`, shutdown...) | no `up` que recria o contêiner |
| `conf/syslog.ini`, lido via `SYSLOG_CONFIG=/etc/syslog/syslog.ini` | **só** as recarregáveis (SPEC §16.3): `log.level`, `log.access`, `log.access_sample` | no `SIGHUP` |

```bash
vi conf/syslog.ini                      # ex.: [log] level = debug
docker kill -s HUP slpg                 # ou: docker compose kill -s HUP syslog
docker logs --since 1m slpg 2>&1 | grep SIGHUP
```

| Linha do log | Significado |
|---|---|
| `SIGHUP: applied` com `keys=[...]` | trocado a quente |
| `SIGHUP: configuration unchanged` | nada mudou no arquivo |
| `ERROR SIGHUP: the new configuration is invalid and was NOT applied` | arquivo inválido (inclusive uma seção do `go-syslog-server` que não existe aqui, como `[filter]`); o servidor segue com a config anterior |
| `WARN SIGHUP: this key requires a restart and was ignored` | uma chave de restart foi posta no INI (e o env não a define); repete em todo HUP até o restart |
| `WARN SIGHUP: this key is set in the config file, but an environment variable or a flag decides its value` | a chave também está no env, que vence |

**Por que não pelo env.** Env vence o INI (SPEC §16.1) e o env de um contêiner
não muda depois do `up`: uma chave recarregável definida por env nunca mudaria
com o `HUP`. Por isso nenhum arquivo deste diretório define `SYSLOG_LOG_LEVEL`,
`SYSLOG_LOG_ACCESS` ou `SYSLOG_LOG_ACCESS_SAMPLE`.

**A pasta, não o arquivo.** O bind é de `conf/`, não de `conf/syslog.ini`: um
bind de arquivo prende o inode, e um editor que salva por *rename* deixaria o
contêiner lendo a versão antiga para sempre.

**Variáveis do `go-syslog-server`.** Um `.env` compartilhado com o pai pode ter
`SYSLOG_LISTEN_TLS`, `SYSLOG_FILTER_*`, `SYSLOG_SINK_TYPE`... O compose só repassa
as variáveis que conhece; se uma delas chegar ao contêiner por outro caminho, o
boot emite **um** `WARN` que as nomeia e diz que são ignoradas. Na linha de
comando ou no INI elas são parâmetro desconhecido: exit 1.

## Identidade LogHub

O servidor chama o `go-loghub-ident` no boot. No contêiner:

| Variável | Valor | Por quê |
|---|---|---|
| `DATADIR` | `/data` (da imagem) | `agent_uuid` e `machine_id` são gerados e persistidos ali, junto de `emergency/` |
| `AGENT_NAME` | `go-syslog-pg-server` (da imagem) | sem ele o nome viria de `argv[0]` |
| `HOSTNAME` | `slpg` (o `hostname:` do serviço) | sem ele seria o id do contêiner, que muda a cada recriação |
| `WORKSPACE` | `SYSLOG_WORKSPACE` do `.env` (`default`) | o **tenant**: vira o `space` de todo envelope. O compose nunca lê o `WORKSPACE` do shell, que o Jenkins usa para o caminho do job |

A conexão aparece em `pg_stat_activity` como
`application_name = slpg/go-syslog-pg-server@slpg`.

**Um volume por instância.** O `from` de cada envelope é o `agent_uuid` gravado no
volume. Duas réplicas montando o mesmo `/data` enfileiram o mesmo `from`, sem
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

## PostgreSQL: a fila `loghub.inbox`

O servidor **enfileira** na `loghub.inbox`, a fila comum de envelopes JLH do
LogHub (SPEC §3.7.1). O serviço `postgres` (PostgreSQL 18) monta
`postgres/inbox.sql` — a DDL literal da skill `loghub-postgres-table-inbox`, que
um teste confere byte a byte — em `/docker-entrypoint-initdb.d`, que a imagem
executa **só na criação do volume**. O servidor **nunca** faz DDL: valida a
tabela no boot e sai com 3 se algo faltar.

A imagem 18 guarda os dados em `/var/lib/postgresql/18/docker`: o volume é
montado em `/var/lib/postgresql`, e um volume no antigo
`/var/lib/postgresql/data` é recusado por ela.

**Sem TLS.** A conexão é sempre `sslmode=disable`, e `PGSSLMODE` do ambiente é
ignorado. No compose o banco está na rede interna; fora dele, use uma rede
confiável ou um socket unix (`SYSLOG_SINK_POSTGRES_ADDR=/var/run/postgresql`). Um
`pg_hba.conf` só com linhas `hostssl` recusa o login, e o boot sai com 3 com a
mensagem do servidor.

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
`postgres://` e `postgresql://` são aceitas; qualquer outro esquema (`nats://`,
`redis://`...) é exit 1. A URL preenche a conexão: endereço, usuário, senha e
banco.

O compose não a repassa por padrão: um `LOGHUB_EB_URL` exportado no shell
colidiria com as chaves de conexão do arquivo, e o boot sairia com 1. Para
usá-la, edite o `environment:` do serviço `syslog` no `docker-compose.yml`:
apague as quatro chaves que ela substitui e descomente a linha da URL.

```yaml
      # antes
      SYSLOG_SINK_POSTGRES_ADDR: postgres:5432
      SYSLOG_SINK_POSTGRES_PASSWORD: ${SYSLOG_SINK_POSTGRES_PASSWORD:-syslog}
      SYSLOG_SINK_POSTGRES_USERNAME:
      SYSLOG_SINK_POSTGRES_DATABASE:
      #LOGHUB_EB_URL: ${LOGHUB_EB_URL:?defina LOGHUB_EB_URL no .env ou no shell}

      # depois
      LOGHUB_EB_URL: ${LOGHUB_EB_URL:?defina LOGHUB_EB_URL no .env ou no shell}
```

```bash
echo "LOGHUB_EB_URL=postgres://syslog:syslog@postgres:5432/syslog" >> .env
docker compose up -d --build
```

As regras:

- **Sem `sslmode` que peça TLS.** `sslmode=require`, `verify-ca` e
  `verify-full` são **exit 1**: o servidor nunca usa TLS, e aceitá-los mandaria
  a senha em claro a quem pediu o contrário. Omita o `sslmode`, ou use
  `disable` (`allow` e `prefer` também passam).
- **Uma fonte só.** Com a URL preenchida, `SYSLOG_SINK_POSTGRES_ADDR`,
  `_USERNAME`, `_PASSWORD` ou `_DATABASE` definidas também (env, flag ou INI)
  são exit 1 com as duas origens no log, **mesmo que a URL não traga aquela
  parte**: uma URL sem senha quer dizer "sem senha". `_TABLE` e `_POOL_SIZE`
  convivem com a URL.
- **Um servidor só**; socket unix com `?host=/dir` e sem host antes do caminho
  (`postgres://syslog@/syslog?host=/var/run/postgresql`). Qualquer outro
  parâmetro de consulta é recusado.

## Arquivo de emergência (`.jsonl`)

O ring é memória. Se o banco ficar inutilizável (FATAL, SPEC §13.3) ou o dreno
do `SIGTERM` estourar `SYSLOG_SHUTDOWN_DRAIN_TIMEOUT`, o que estava pendente é
gravado em `/data/emergency/emg-<agent_uuid>-<unixnano>.jsonl` e o processo sai
com 3. **Um envelope JLH pronto por linha**, byte a byte o que o worker teria
escrito na fila.

No boot seguinte, antes de abrir qualquer transporte, todo `emg-*.jsonl` volta
para a fila **sem conversão** (as colunas saem do cabeçalho de cada linha) e o
arquivo é removido depois de confirmado. A leitura é em fluxo, pedaço a pedaço:
um dump maior que a memória do contêiner volta sem OOM. Linha inválida ou cauda
truncada: as linhas íntegras são entregues e o arquivo vira `.corrupt`, para
inspeção. Um `.emg-…jsonl.tmp` que sobre é um dump que **não terminou** (o
processo morreu no meio da escrita): nunca é reingerido, fica para o operador.

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
com o motivo em texto, quando o banco não está UP, quando o ring passa de
`ring.high_water`, durante a reingestão de dumps no boot, antes de os
transportes abrirem e no shutdown. Nenhuma dessas condições se resolve
reiniciando o contêiner: um restart não conserta um banco inalcançável, só troca
o backlog da RAM por um dump e uma reingestão.

`/readyz` é para o balanceador (tirar a instância de rotação) e para o
operador. A porta admin só é publicada no loopback do host:

```bash
curl -sS -w '%{http_code}\n' http://127.0.0.1:19180/readyz     # 200 "ready", ou 503 e o motivo
curl -s http://127.0.0.1:19180/metrics | grep -E '^syslog_(sink_up|ring_items|ring_bytes|events_written_total)'
docker exec slpg wget -qO- http://127.0.0.1:9102/readyz        # de dentro
```

Para alertar, use `/metrics` (`syslog_sink_up`, `syslog_ring_items` contra
`syslog_ring_capacity_items`, `syslog_messages_dropped_total`,
`syslog_events_dropped_total`), não o estado do healthcheck. Os nomes são os do
`go-syslog-server`; `syslog_config_info` diz `ring_mode="memory"` e
`sink_type="postgres"`.

**O `admin.bind` padrão é o mesmo do pai** (`127.0.0.1:9102`). Dentro de
contêineres não há conflito; rodando os dois binários direto no mesmo host,
troque o de um deles.

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
`SIGTERM` e FATAL do banco não perdem (dreno e dump), `kill -9` e OOM perdem o
backlog do ring.
