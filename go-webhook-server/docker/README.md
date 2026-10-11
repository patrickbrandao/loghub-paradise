# Deploy em Docker

Build e execução do `go-webhook-server` em contêiner, com um arquivo compose
para cada combinação suportada de **ring** (onde o evento espera) e **sink**
(para onde ele vai).

```bash
# combinação recomendada (ring na RAM -> Redis Streams)
docker compose up -d --build

# qualquer outra combinação
docker compose -f compose.memory-sqlite.yml up -d --build
```

---

## Sobre "ring no Redis"

**Não existe.** O `ring.mode` aceita apenas `memory`, `sqlite` e `none`
(a entrada `ring.mode` do registry em `internal/config/registry.go`, validada
por `Validate` em `internal/config/validate.go`); o Redis é implementado apenas
como **sink** (`internal/sink/redis.go`). Um
`ring.mode=redis` faz o servidor recusar o boot com exit code 1.

Isso é coerente com o desenho: o ring é a fila local que absorve a
indisponibilidade do destino. Se o ring morasse no Redis, o Redis fora do ar
levaria fila e destino juntos e a fila não protegeria de nada.

O que mais se aproxima do que "ring no Redis" costuma querer dizer:

| intenção | use |
|---|---|
| fila fora do processo, sobrevive a restart | `compose.sqlite-redis.yml` (ring durável em disco, destino no Redis) |
| o Redis é a própria fila que outro consumidor lê | `compose.none-redis.yml` (200 só depois do `XADD` confirmado) |

As vinte e uma combinações abaixo são todas as que o software suporta.

---

## As combinações

`compose.<ring>-<sink>.yml`

| arquivo | ring | sink | 200 significa | perda em queda abrupta |
|---|---|---|---|---|
| `compose.memory-redis.yml` ★ | memory | redis | aceito na RAM | backlog do ring (mitigado pelo dump de emergência) |
| `compose.memory-sqlite.yml` | memory | sqlite | aceito na RAM | backlog do ring (mitigado pelo dump de emergência) |
| `compose.memory-none.yml` | memory | none | **descartado** | tudo — benchmark |
| `compose.sqlite-redis.yml` | sqlite | redis | gravado com fsync no `ring.db` | nada |
| `compose.sqlite-sqlite.yml` | sqlite | sqlite | gravado com fsync no `ring.db` | nada — mas paga **dois** fsyncs |
| `compose.sqlite-none.yml` | sqlite | none | **descartado** após o fsync | tudo — benchmark |
| `compose.none-redis.yml` | none | redis | `XADD` confirmado pelo Redis | nada |
| `compose.none-sqlite.yml` | none | sqlite | `COMMIT` no `events.db` concluído | nada |
| `compose.none-none.yml` | none | none | **descartado** | tudo — benchmark puro de HTTP |
| `compose.memory-postgres.yml` | memory | postgres | aceito na RAM | backlog do ring (mitigado pelo dump de emergência) |
| `compose.sqlite-postgres.yml` | sqlite | postgres | gravado com fsync no `ring.db` | nada |
| `compose.none-postgres.yml` | none | postgres | `INSERT` confirmado pelo servidor † | nada † |
| `compose.memory-mysql.yml` | memory | mysql | aceito na RAM | backlog do ring (mitigado pelo dump de emergência) |
| `compose.sqlite-mysql.yml` | sqlite | mysql | gravado com fsync no `ring.db` | nada |
| `compose.none-mysql.yml` | none | mysql | `INSERT` confirmado pelo servidor † | nada † |
| `compose.memory-nats.yml` | memory | nats | aceito na RAM | backlog do ring (mitigado pelo dump de emergência) |
| `compose.sqlite-nats.yml` | sqlite | nats | gravado com fsync no `ring.db` | nada |
| `compose.none-nats.yml` | none | nats | `PubAck` do stream JetStream † | nada † |
| `compose.memory-clickhouse.yml` | memory | clickhouse | aceito na RAM | backlog do ring (mitigado pelo dump de emergência) |
| `compose.sqlite-clickhouse.yml` | sqlite | clickhouse | gravado com fsync no `ring.db` | nada |
| `compose.none-clickhouse.yml` | none | clickhouse | `INSERT` respondido pelo ClickHouse † | nada † |

★ padrão de `docker-compose.yml`.

† **Linhas cuja durabilidade depende de configuração fora do processo.**
"Confirmado" é o que o servidor de destino disser: no PostgreSQL, o
`synchronous_commit` (`on` nos compose: WAL sincronizado antes da resposta;
`docs/SPEC.md` §8.5.6); no MySQL, o `innodb_flush_log_at_trx_commit` (`1` nos
compose: redo log sincronizado; §8.6.5); no JetStream, o `storage`, as réplicas
e o `sync_interval` do stream (§8.7.4); no ClickHouse, o `async_insert` e o
`wait_for_async_insert` do perfil do usuário (padrão da imagem nos compose:
`async_insert = 0`, a parte escrita antes da resposta; §8.8.6). O servidor de
webhooks não lê nem altera nenhum desses valores. Ver
[PostgreSQL: o esquema é seu](#postgresql-o-esquema-é-seu),
[MySQL: o esquema é seu](#mysql-o-esquema-é-seu),
[NATS: o stream é seu](#nats-o-stream-é-seu) e
[ClickHouse: a tabela `inbox`](#clickhouse-a-tabela-inbox).

Três delas (`*-none.yml`) existem para **benchmark** e o servidor emite warning
de boot dizendo que nunca devem rodar em produção: o cliente recebe 200 e o
evento deixa de existir. Servem para isolar o custo de cada camada — rode
`none-none` para o teto da máquina, depois `memory-none` para o custo do ring,
depois `memory-redis` para o custo do destino.

### Escolhendo

- **Vazão máxima, perda tolerável em crash:** `memory-redis`.
- **Nó único, sem infraestrutura externa:** `memory-sqlite`.
- **Nenhuma perda, destino remoto:** `sqlite-redis`.
- **Nenhuma perda, e o cliente precisa saber:** `none-redis` ou `none-sqlite` —
  aqui uma falha de entrega vira 5xx e o cliente reenvia.
- **`sqlite-sqlite`:** só com os dois arquivos em discos físicos diferentes.
  Fora disso, `memory-sqlite` dá a mesma durabilidade do que já foi gravado pela
  metade do custo. O próprio servidor avisa isso no boot.
- **Banco compartilhado, consultável por SQL, várias instâncias escrevendo na
  mesma tabela:** `memory-postgres` ou `memory-mysql`. Com fila local que
  sobrevive a restart e absorve o banco fora do ar por muito tempo:
  `sqlite-postgres` / `sqlite-mysql`. Sem fila nenhuma, com o cliente sabendo
  de cada falha: `none-postgres` / `none-mysql` — leia o `†` acima antes.
- **Um barramento com consumidores próprios, sem o `webhook-worker` ler Redis:**
  `memory-nats`. O stream JetStream confirma cada publicação e desduplica por
  `Nats-Msg-Id` dentro da janela configurada; quem consome tolera o mesmo id
  duas vezes fora dela.
- **Análise sobre o histórico, com vazão alta:** `memory-clickhouse`. A tabela
  é um `ReplacingMergeTree` consultável por subcoluna do `payload` JSON; uma
  reentrega é descartada pelo token ou colapsada no merge (leia com `FINAL`).
  `none-clickhouse` faz uma parte por requisição: só com `async_insert` no
  perfil do usuário.

---

## Portas

Cada combinação publica em portas distintas, de propósito: dá para subir várias
lado a lado e comparar. Sobrescreva com `WHS_HTTP_PORT` / `WHS_ADMIN_PORT` /
`WHS_REDIS_PORT` / `WHS_POSTGRES_PORT` / `WHS_MYSQL_PORT` / `WHS_NATS_PORT` /
`WHS_CLICKHOUSE_PORT`.

| combinação | HTTP | admin | redis / postgres / mysql / nats / clickhouse |
|---|---|---|---|
| memory-redis | 8080 | 9101 | 6379 |
| memory-sqlite | 8081 | 9111 | — |
| memory-none | 8082 | 9112 | — |
| sqlite-redis | 8083 | 9113 | 6383 |
| sqlite-sqlite | 8084 | 9114 | — |
| sqlite-none | 8085 | 9115 | — |
| none-redis | 8086 | 9116 | 6386 |
| none-sqlite | 8087 | 9117 | — |
| none-none | 8088 | 9118 | — |
| memory-postgres | 8089 | 9119 | 5432 |
| sqlite-postgres | 8090 | 9120 | 5433 |
| none-postgres | 8091 | 9121 | 5434 |
| memory-nats | 8092 | 9122 | 4222 |
| sqlite-nats | 8093 | 9123 | 4223 |
| none-nats | 8094 | 9124 | 4224 |
| memory-mysql | 8095 | 9125 | 3306 |
| sqlite-mysql | 8096 | 9126 | 3307 |
| none-mysql | 8097 | 9127 | 3308 |
| memory-clickhouse | 8098 | 9128 | 9000 (nativo) |
| sqlite-clickhouse | 8099 | 9129 | 9001 (nativo) |
| none-clickhouse | 8100 | 9130 | 9002 (nativo) |

A porta admin (`/healthz`, `/readyz`, `/metrics`) é publicada **somente em
`127.0.0.1` do host**. Dentro do contêiner ela escuta em `0.0.0.0` — não há
como publicar de outra forma — e por isso o servidor emite este warning no boot,
que é esperado neste contexto:

```
admin.bind="0.0.0.0:9101" is not loopback: /metrics and /readyz are reachable off-host
```

---

## Memória

O servidor **não define `GOMEMLIMIT` sozinho** e avisa no boot se ele faltar
(`docs/SPEC.md` §12.4). Sem ele o coletor de lixo do Go não sabe do teto do
contêiner e o OOM killer chega antes do GC — e com `ring=memory` leva o
backlog junto, porque o dump de emergência não roda num processo morto pelo
kernel. Por isso `common.yml` define os dois valores, parametrizáveis pelo
`.env`:

| variável | padrão | papel |
|---|---|---|
| `WHS_GOMEMLIMIT` | `512MiB` | `GOMEMLIMIT` do processo |
| `WHS_MEMORY_LIMIT` | `768M` | limite de memória do contêiner (`deploy.resources.limits.memory`), **acima** do `GOMEMLIMIT` |
| `WHS_RING_MAX_BYTES` | `256MiB` nos compose de `ring=memory` | teto de bytes do ring, que com `ring=memory` fica todo na RAM |

Com `ring=memory` o boot avisa se `GOMEMLIMIT` for menor que o dobro de
`ring.max_bytes`; os padrões acima respeitam isso. Com `ring=sqlite` e
`ring=none` o ring não ocupa RAM e o aviso só sai se a variável faltar. Quem
subir um dos três, suba os outros junto.

As duas variáveis têm gramáticas diferentes: `*_MEMORY_LIMIT` é lido pelo
Compose (`768M`, `1g`); `*_GOMEMLIMIT` é lido pelo runtime do Go, que aceita
só um inteiro com sufixo `B`, `KiB`, `MiB`, `GiB` ou `TiB` (maiúsculas
importam), ou `off`. Um `GOMEMLIMIT=768M` derruba o processo antes do boot com
`malformed GOMEMLIMIT`, e com `restart: unless-stopped` o contêiner entra em
laço.

---

## Arquivos

| arquivo | papel |
|---|---|
| `Dockerfile` | build multi-stage, binário estático (`CGO_ENABLED=0`), imagem final Alpine rodando como uid 65532 |
| `common.yml` | fragmento herdado por todos os compose via `extends`: build, healthcheck, hardening, limites, logging |
| `docker-compose.yml` | `include` da combinação recomendada, para `docker compose up` sem `-f` |
| `compose.<ring>-<sink>.yml` | uma combinação cada, com a configuração específica e o porquê dela |
| `postgres/inbox.sql` | a fila `loghub.inbox` de `sink=postgres`, DDL literal da skill e de `docs/SPEC.md` §3.6; montada em `/docker-entrypoint-initdb.d` pelos três `*-postgres.yml` |
| `mysql/inbox.sql` | a tabela `inbox` de `sink=mysql`, espelho da fila, cópia canônica de `docs/SPEC.md` §3.7; montada em `/docker-entrypoint-initdb.d` pelos três `*-mysql.yml` |
| `clickhouse/inbox.sql` | a tabela `inbox` de `sink=clickhouse`, espelho da fila com os *hints* `Map`, cópia canônica de `docs/SPEC.md` §3.9; montada em `/etc/whs` pelos três `*-clickhouse.yml` |
| `clickhouse/init.sh` | aplica `clickhouse/inbox.sql` dentro de `CLICKHOUSE_DB` na criação do volume (a imagem rodaria um `.sql` do `initdb.d` no banco `default`) |
| `nats/stream.json` | o stream JetStream de `sink=nats`; criado uma vez pelo serviço `nats-init` dos três `*-nats.yml`, com a janela de desduplicação de 10 min |
| `nats/nats-server.conf` | JetStream em `/data` e `max_payload: 8MB`, montado por cima do arquivo padrão da imagem `nats` |
| `.env.example` | knobs compartilhados (portas, imagem, proxy, rate limit) |
| `../.dockerignore` | mantém o contexto de build mínimo; precisa ficar na raiz do módulo, que é o contexto |

O que distingue uma combinação da outra fica **visível no arquivo dela**; o que
é idêntico entre todas mora em `common.yml`. Regras de herança do Compose:
`environment` e `healthcheck` são mapas e o filho sobrescreve chave a chave;
`ports` concatena (por isso ficam só no filho); `depends_on` **não** é herdado.

O build usa a raiz do módulo como contexto (`context: ..`), não esta pasta.

---

## Configuração

Todo parâmetro do registro aceita variável de ambiente `WHS_<SEÇÃO>_<CHAVE>`
com `.` virando `_` — `ring.mode` → `WHS_RING_MODE`,
`sink.redis.addrs` → `WHS_SINK_REDIS_ADDRS`. A precedência é:

```
defaults  <  arquivo INI  <  ambiente  <  linha de comando
```

Os compose usam ambiente. Para ver a configuração efetiva de uma stack:

```bash
docker compose -f compose.memory-redis.yml run --rm --no-deps webhook --dump-config
```

E a lista completa de parâmetros com defaults e descrições:

```bash
docker compose -f compose.memory-redis.yml run --rm --no-deps webhook --help
```

Para usar arquivo INI em vez de ambiente, monte-o e aponte o comando:

```yaml
    volumes:
      - ./meu-server.ini:/etc/webhook/server.ini:ro
    command: ["--config=/etc/webhook/server.ini"]
```

O `:ro` não é decoração: `/etc/webhook` é ponto de montagem de configuração, e
escrita nenhuma acontece lá.

---

## Layout do contêiner: binário em `/app`, dados em `/data`

**Toda escrita do serviço acontece em `/data`, dentro do contêiner.** É regra do
`loghub-paradise` inteiro, não só deste serviço: toda imagem do projeto grava em
`/data` e em lugar nenhum além (`docs/SPEC.md` §13.6, invariante 13). Um ponto de
montagem só, com o mesmo nome em toda imagem, faz backup, dimensionamento de
disco e política de volume serem decididos uma vez em vez de um por serviço — e é
o mesmo caminho que a imagem oficial do Redis já usa.

São três escritas, todas derivadas de `WHS_RING_DATA_DIR`, fixado em `/data` na
imagem e repetido em `common.yml`:

| escrita | caminho | só existe quando |
|---|---|---|
| fila durável do ring | `/data/ring.db` + `-wal` + `-shm` | `ring.mode=sqlite` a escreve; um boot com outro modo a entrega e a remove (com `sink=none`, as linhas ficam) |
| destino append-only | `/data/events.db` + rotações | `sink.type=sqlite` |
| dumps de emergência | `/data/emergency/emg-*.bin` | `ring.mode=memory` os escreve; todo boot os reingere, em qualquer modo |

Fora disso o serviço não escreve em disco: log vai para stdout, métricas são
servidas pela porta admin e o corpo da requisição nunca vira arquivo temporário.

O resto do sistema de arquivos é **read-only** (`read_only: true` em
`common.yml`), e é isso que transforma a regra em garantia. Um `WHS_*_PATH`
apontando para fora de `/data` falha no boot — exit 1 no `ring.db`, exit 3 no
`events.db` — em vez de gravar na camada efêmera do contêiner, onde a escrita
**funciona** e some no próximo `docker rm` sem erro nenhum para avisar.

O caso mais caro é o `emergency.dir`: esse diretório só é criado no instante do
dump, então um caminho errado nele não aparece no boot. Aparece **durante o
incidente**, com o ring inteiro virando
`whs_events_dropped_total{reason="emergency_failed"}`.

O layout completo, igual em toda imagem do `loghub-paradise`:

| caminho | papel | gravável |
|---|---|---|
| `/app` | o binário, dono `root`, modo `0755` | não |
| `/data` | ring, events.db (tabela `inbox`), dumps de emergência, identidade LogHub (`agent_uuid`, `machine_id`), e o `WORKDIR` | **sim**, e só ele |
| `/etc/webhook` | INI montado com `:ro` | não |
| `/tmp` | `tmpfs`, rede de segurança do `read_only` | sim, mas volátil |

Duas convenções vêm junto, e valem para todo serviço do `loghub-paradise`:

- **`WORKDIR` é `/data`.** O processo nasce dentro da única área gravável, então
  um caminho relativo mal configurado cai no volume em vez da camada efêmera do
  contêiner. A imagem oficial do Redis já faz o mesmo.
- **O binário é invocado como `/app/<nome>`**, caminho absoluto, nunca pelo nome
  solto. O runtime resolveria o nome pelo `PATH`, mas aí o contêiner passaria a
  depender de uma variável que qualquer camada ou `docker run -e` pode mudar, e
  a falha seria um `executable file not found` sem dizer o que procurava.

A regra vale para as imagens construídas aqui. A do Redis é de terceiros e
mantém o layout dela, com o `redis-server` onde a imagem oficial o põe; `/data` é
o ponto em que as duas coincidem.

`/tmp` não é área de trabalho: o servidor não escreve nele.

Para mudar onde os dados ficam **no host**, troque o volume — nunca o caminho
dentro do contêiner:

```yaml
    volumes:
      # o diretório precisa existir e pertencer ao uid 65532
      - /mnt/nvme0/whs:/data
```

---

## Identidade LogHub e o envelope

Todo evento é um envelope JLH `type=webhook.request` (`docs/SPEC.md` §3): o
mesmos bytes em todos os destinos, com o corpo da requisição dentro dele
(`payload.content`, em texto ou base64). O `200` devolve o `uuid` do envelope no
cabeçalho `X-LogHub-UUID` e num corpo JSON
(`{"uuid":…,"mode":"fast|bucket","status":"accepted|stored"}`).

`from` e `space` vêm do `go-loghub-ident` (`docs/SPEC.md` §3.5):

| Variável | De onde vem | Observação |
|---|---|---|
| `AGENT_NAME` | a imagem (`go-webhook-server`) | nomeia as conexões aos destinos |
| `WORKSPACE` | `WHS_WORKSPACE` do `.env` (`default`), via `common.yml` | o **tenant**, não o ambiente: vira o `space` e o subject/stream `loghub.<space>.webhook.request`; sem `_`. O compose nunca lê o `WORKSPACE` do shell, que o Jenkins usa para outra coisa |
| `HOSTNAME` | o `hostname:` de cada serviço | RFC 1123; sem ele seria o id do contêiner |
| `AGENT_UUID`, `MACHINE_ID` | gerados e persistidos em `/data` na primeira subida | **um volume por instância**: duas réplicas no mesmo volume publicam o mesmo `from` |

Uma identidade inválida encerra o processo com um código entre 100 e 114 antes
de qualquer porta abrir (a tabela está na skill `go-loghub-ident`).
`WHS_SERVER_NODE_ID` não existe mais.

## PostgreSQL: a fila `loghub.inbox`

Com `sink=postgres` o servidor **enfileira** cada envelope JLH na `loghub.inbox`,
a fila comum do LogHub (skill `loghub-postgres-table-inbox`, `docs/SPEC.md`
§3.6), e não numa tabela própria. Os três `compose.*-postgres.yml` sobem um
PostgreSQL **18** ao lado, e a divisão de responsabilidade é a mesma que vale
contra um banco de produção:

- **O servidor de webhooks nunca cria nem altera a fila.** Ele valida no boot
  que `loghub.inbox` existe com as dez colunas que ele escreve, que nenhuma
  coluna extra é obrigatória, que há um índice único exatamente sobre `(uuid)` e
  que o papel tem `INSERT` e `SELECT (uuid)`, e sai com código 3 na hora se
  algo faltar. Também recusa um banco que não seja UTF8 e uma tabela
  particionada.
- **`postgres/inbox.sql` é a DDL literal da skill.** O compose o monta em
  `/docker-entrypoint-initdb.d`, e a imagem oficial o executa **uma** vez, ao
  criar o volume.
- **Um volume dos compose antigos não serve:** ele tem dados do PostgreSQL 17
  (montados em `/var/lib/postgresql/data`), e a imagem 18 se recusa a subir
  sobre eles ("there appears to be PostgreSQL data … pg_upgrade"); o `webhook`
  nem inicia, porque espera o banco saudável. A tabela `events` antiga não é
  migrada para a fila de qualquer modo: recrie o volume
  (`docker compose -f compose.<ring>-postgres.yml down -v`), ou faça o
  `pg_upgrade` por conta própria e aplique `postgres/inbox.sql` com `psql`.
- **A imagem 18 guarda os dados em `/var/lib/postgresql/18/docker`**: o volume
  é montado em `/var/lib/postgresql`, e ela recusa um volume no antigo
  `/var/lib/postgresql/data`.
- **O papel precisa de `INSERT` e de `SELECT (uuid)`** (`GRANT INSERT, SELECT
  (uuid) ON loghub.inbox`): o `ON CONFLICT (uuid)` lê a coluna árbitra, e só
  com `INSERT` o primeiro lote falharia com `42501`. `UPDATE` não.
- **Cada worker é uma conexão no servidor**, e o `max_connections` (100 por
  padrão) é dividido por todas as instâncias. O padrão de `sink.workers` cai de
  8 para 4 com este destino, e os compose fixam o valor para o aviso de boot não
  aparecer.
- **Um failover do banco derruba o serviço com código 3**, de propósito
  (`docs/SPEC.md` §8.5.7): uma sessão somente leitura é fatal, o ring em RAM vai
  para `/data/emergency`, o supervisor reinicia, e o boot espera um primário.
- **O consumo é de outro software**, sempre em ordem de `uuid`, retirando a
  linha na mesma transação que grava o resultado:

  ```sql
  WITH job AS (
    SELECT uuid FROM loghub.inbox ORDER BY uuid LIMIT 100 FOR UPDATE SKIP LOCKED
  )
  DELETE FROM loghub.inbox i USING job WHERE i.uuid = job.uuid RETURNING i.*;
  ```

O `payload` é `jsonb`: a fila não é fiel ao byte (o `jsonb` reordena chaves),
mas o corpo da requisição continua exato em `payload.content.body`. Um `%00` na
query vira U+FFFD na coluna, porque o `jsonb` recusa o NUL (`docs/SPEC.md`
§3.6).

---

## MySQL: a tabela `inbox`

Os três `compose.*-mysql.yml` sobem um MySQL 8.4 ao lado. A tabela `inbox`
**espelha** a `loghub.inbox` do PostgreSQL (`docs/SPEC.md` §3.7): as mesmas onze
colunas, a chave `uuid` e as mesmas CHECKs, nos tipos do MySQL (`BINARY(16)`,
`DATETIME(6)` em UTC, `JSON`).

- **O servidor de webhooks nunca cria nem altera a tabela.** Ele valida no boot
  que `inbox` existe em **InnoDB** com as dez colunas que ele escreve, que há um
  índice único exatamente sobre `(uuid)`, que o `max_allowed_packet` comporta o
  maior envelope e que o usuário consegue escrever — com um `INSERT` de sonda
  dentro de uma transação revertida, que também pega uma CHECK ou uma coluna
  extra sem default — e sai com código 3 na hora se algo faltar.
- **`mysql/inbox.sql` é a cópia canônica**, executada uma vez pela imagem
  oficial ao criar o volume, dentro de `MYSQL_DATABASE`. Um volume antigo, com
  a tabela `events`, não ganha a `inbox`: recrie o volume ou aplique o arquivo.
- **O usuário precisa de `INSERT` e `UPDATE`**, não só `INSERT`: a escrita
  idempotente é `ON DUPLICATE KEY UPDATE uuid = uuid`, e o MySQL exige o
  privilégio mesmo com a atualização vazia. O `UPDATE` pode ficar restrito à
  coluna `uuid` (`GRANT INSERT, UPDATE (uuid) ON webhook.inbox TO ...`). Nunca
  `INSERT IGNORE`: ele truncaria dados em silêncio.
- **`max_allowed_packet`** precisa comportar o maior envelope (`docs/SPEC.md`
  §3.2, ~7,7 MiB com os padrões); o compose fixa `64M` e o boot confere.
- **Cada worker é uma conexão no servidor**: mesmo padrão de 4 workers do
  PostgreSQL, pela mesma conta.
- **Um servidor somente leitura derruba o serviço com código 3** (`1290`).

## ClickHouse: a tabela `inbox`

Os três `compose.*-clickhouse.yml` sobem um ClickHouse 25.8 ao lado. A tabela
`inbox` **espelha** a `loghub.inbox` (`docs/SPEC.md` §3.9): as mesmas onze
colunas, num `ReplacingMergeTree` ordenado por `(event_at, uuid)` e
particionado por mês, com o `payload` no tipo `JSON`.

- **O servidor de webhooks nunca cria nem altera a tabela.** Ele valida no boot
  a versão (25.8+), o motor `*ReplacingMergeTree`, `uuid` na chave de
  ordenação, os tipos exatos das dez colunas, os dois *hints* do `payload`,
  nenhuma `CHECK`, o `max_memory_usage` contra o pior lote e um `INSERT`
  **vazio** de sonda (o ClickHouse não tem transação para desfazer uma linha), e
  sai com código 3 na hora se algo faltar.
- **`params` e `query` são `Map`** dentro do `JSON`: o tipo `JSON` descarta
  objeto vazio, e `params` é sempre `{}`. As chaves da query (as únicas do
  cliente) ficam literais. O *hint* é do `webhook.request`: o
  `go-syslog-server` usa outra tabela.
- **`clickhouse/inbox.sql` é a cópia canônica**, aplicada uma vez pelo
  `init.sh` ao criar o volume, dentro de `CLICKHOUSE_DB`. O healthcheck do
  serviço só fica verde quando a tabela existe, para o webhook não subir antes
  do `init.sh` terminar.
- **O usuário precisa só de `INSERT`** nas dez colunas e de um perfil com
  `readonly = 0`. Nos compose, por simplicidade, é o mesmo usuário que a imagem
  cria.
- **Deduplicação em duas camadas**: o `insert_deduplication_token` de cada
  lote descarta o retry na hora (a DDL fixa `non_replicated_deduplication_window`;
  num `Replicated*`, apague a linha), e o merge colapsa uma reentrega fora da
  janela. Até o merge, leia com `FINAL`.
- **Durabilidade e `async_insert` são do administrador do ClickHouse**: os
  compose deixam o padrão da imagem. Com `ring=none`, cada requisição é uma
  parte; para vazão ali, ligue `async_insert` no perfil, com
  `wait_for_async_insert = 1` se o `200` ainda deve esperar a escrita.
- **Um usuário `readonly`, uma réplica sem Keeper (`242`) ou um
  `MEMORY_LIMIT_EXCEEDED` (`241`) derrubam o serviço com código 3.**

---

## NATS: o stream é seu

Os três `compose.*-nats.yml` sobem um `nats-server` com JetStream ao lado, mais
um serviço de uma execução, `nats-init`, que cria o stream `LOGHUB_WEBHOOK_REQUEST` a partir
de `nats/stream.json` se ele ainda não existir:

- **O servidor de webhooks nunca cria nem altera o stream.** Ele valida no boot
  que o stream existe, que o subject do Workspace (`loghub.<space>.webhook.request`)
  pertence a ele, que não está
  selado e que o `max_payload` do servidor (e o `max_msg_size` do stream, se
  houver) comporta o maior evento aceitável — e sai com 3 na hora se algo
  faltar (`docs/SPEC.md` §8.7.5). Contra um NATS de produção, `nats-init` é o
  papel de quem administra o servidor: `nats stream add LOGHUB_WEBHOOK_REQUEST --config
  nats/stream.json`.
- **`max_payload` do `nats-server` é 1 MiB por padrão**, abaixo do maior
  envelope com os 5 MiB de `server.max_body_bytes` (~7,7 MiB: o corpo em base64
  mais cabeçalhos e URI, `docs/SPEC.md` §3.2). `nats/nats-server.conf` sobe para
  8 MB; quem subir o teto de corpo sobe os dois juntos. Sem isso o boot recusa, de propósito:
  aceitar um webhook com 200 e descobrir na publicação que ele não cabe seria
  descartar evento confirmado.
- **Cada evento é uma mensagem** (`docs/SPEC.md` §3.8) no subject
  `loghub.<space>.webhook.request`: o envelope JLH inteiro em `Data` (o corpo
  vai dentro dele), headers `X-LogHub-Type`, `X-LogHub-UUID` e
  `X-LogHub-Space`, e `Nats-Msg-Id` = `<uuid>.<step>`, que o stream usa para
  desduplicar dentro de `duplicate_window` (10 min em `stream.json`). Uma
  redelivery fora da janela duplica: o pipeline é at-least-once, e o consumidor
  deduplica por `(uuid, step)`.
- **`stream.json` cobre só o Workspace `default`.** Quem muda `WORKSPACE` muda
  o subject do stream junto — e mantém o antigo enquanto houver backlog dele em
  `ring.db` ou num dump: um envelope num subject que o stream não cobre é fatal,
  com o subject na mensagem (SPEC §8.7.5); nunca `loghub.>`, que capturaria o plano de
  controle.
- **Durabilidade é a do stream**: `storage: file` e réplicas. Um único
  `nats-server` com file store só sincroniza com o disco a cada
  `sync_interval` (2 min); contra a queda do próprio servidor NATS, ligue
  `sync_always` ou use réplicas. O webhook não lê nem altera nada disso.
- **Sem autenticação nem TLS na rede do compose.** Contra um NATS gerenciado,
  monte um arquivo `.creds` em `/etc/webhook` (somente leitura) e aponte
  `WHS_SINK_NATS_CREDENTIALS`; ou `WHS_SINK_NATS_USERNAME`/`PASSWORD`, ou
  `WHS_SINK_NATS_TOKEN` — um método só por vez.

---

## Antes de expor à internet

1. **`WHS_PROXY_TRUSTED`.** O default (`127.0.0.0/8,::1`) é fail-closed: nenhum
   header de encaminhamento é acreditado, e o IP do cliente é o peer do socket.
   Atrás de proxy/ingress isso faz o servidor enxergar o proxy como cliente —
   quebra `network.ip.src` no envelope, rate limit e exempt. Declare os CIDRs do
   proxy. E o proxy precisa **reescrever** (ou anexar a) `X-Forwarded-For` e os
   demais cabeçalhos de encaminhamento: um que só preenche `X-Real-IP` e repassa
   o `X-Forwarded-For` do cliente deixa o cliente escolher o próprio IP
   (`docs/SPEC.md` §5.2).
2. **`WHS_RATELIMIT_ENABLED=true`**, mas **só depois** do passo 1. Um limitador
   com `proxy.trusted` errado joga o mundo inteiro num balde só e vira auto-DoS.
   Por isso vem desligado e o servidor avisa no boot enquanto estiver assim.
3. **`stop_grace_period`.** Está em 45s em `common.yml`, acima do
   pior caso até o dump começar, `pre_stop_delay + drain_timeout +
   sink.op_timeout` (0s + 30s + 5s com os padrões: o `drain_timeout` é um prazo
   só para as conexões e o ring, e o lote em voo de um worker ainda leva até o
   `op_timeout`), com folga para escrever o próprio dump (`docs/SPEC.md`
   §10.3). Os 10s default do Docker matariam o processo no meio do dreno —
   perdendo eventos já respondidos com 200. Se aumentar qualquer um dos três,
   aumente também o grace period. Com
   `sink=redis` conte 10s no lugar do `op_timeout`: num *pipe* travado o
   cliente só devolve o lote no próprio `ConnWriteTimeout` (`docs/TESTING.md`
   §8 item 37).
4. **`WHS_SHUTDOWN_PRE_STOP_DELAY`.** Atrás de balanceador, ponha o tempo que
   ele leva para tirar o endpoint da rotação; o servidor continua atendendo
   nesse intervalo em vez de recusar conexões que o LB ainda está mandando.
5. **`sink.redis.maxlen`.** Deixe em 0. Ligar o trim faz o Redis descartar
   eventos antigos em silêncio, o oposto da backpressure que o servidor
   implementa (que é responder 429).
6. **`WHS_SINK_POSTGRES_PASSWORD`.** O padrão `webhook` dos três `*-postgres.yml`
   serve ao ambiente de desenvolvimento e a mais nada. Em produção, defina no
   `.env`; e contra um banco fora do compose, ligue `WHS_SINK_POSTGRES_TLS` e
   monte a CA em `/etc/webhook` (somente leitura) apontando
   `WHS_SINK_POSTGRES_TLS_CA` para ela.
7. **`WHS_SINK_MYSQL_PASSWORD` e `WHS_MYSQL_ROOT_PASSWORD`.** Mesma situação:
   `webhook` e `root` são de desenvolvimento. Em produção, `.env`, e
   `WHS_SINK_MYSQL_TLS` com a CA montada em `/etc/webhook`.
8. **Autenticação do NATS.** Os compose sobem o `nats-server` sem credenciais,
   na rede interna. Fora dela, `WHS_SINK_NATS_CREDENTIALS` (arquivo `.creds`
   montado em `/etc/webhook`) e `WHS_SINK_NATS_TLS`.

---

## Operação

```bash
S="-f compose.memory-redis.yml"

docker compose $S up -d --build     # sobe
docker compose $S logs -f webhook   # logs (JSON por padrão)
docker compose $S ps                # estado + health
docker compose $S down              # derruba, preserva volumes
docker compose $S down -v           # derruba e APAGA os dados
```

Verificação rápida:

```bash
curl -fsS -X POST localhost:8080/qualquer/caminho -d '{"ping":1}'
curl -fsS localhost:9101/readyz
curl -fsS localhost:9101/metrics | grep -E '^whs_(requests_total|events_written_total|ring_items|sink_up)'
```

As métricas que dizem se a stack está saudável:

| métrica | leitura |
|---|---|
| `whs_sink_up` | 1 = destino alcançável. Zerou, o backlog começa a crescer |
| `whs_ring_items` / `whs_ring_capacity_items` | ocupação da fila; a razão entre elas é o que dispara `/readyz` no `high_water` |
| `whs_events_written_total` | eventos confirmados pelo destino. Com `ring.mode=none` quem escreve é o próprio handler, e a série é alimentada do mesmo jeito (`docs/SPEC.md` §10.2, §11.1) |
| `whs_rejected_total` | recusas — inclui os 429 de ring cheio |
| `whs_events_dropped_total` | **deve ficar em zero**; qualquer valor aqui é evento perdido |
| `whs_emergency_files` | dumps de emergência pendentes de reingestão |

As métricas de sink (`whs_events_written_total`, `whs_sink_duration_seconds`,
`whs_sink_batch_size`) são alimentadas pelo dreno em `internal/sink/workers.go`
e, com `ring.mode=none`, pelo invólucro de `internal/sink/instrumented.go` que
observa a escrita síncrona do handler. Os três modos reportam as três séries;
uma delas em zero com tráfego chegando **é** falha.

Conferindo que o evento chegou no Redis:

```bash
docker compose -f compose.memory-redis.yml exec redis redis-cli XLEN loghub.default.webhook.request
docker compose -f compose.memory-redis.yml exec redis redis-cli XRANGE loghub.default.webhook.request - + COUNT 1
```

Ou no SQLite:

```bash
docker compose -f compose.memory-sqlite.yml exec webhook \
  sh -c 'ls -la /data'
```

Ou no PostgreSQL:

```bash
docker compose -f compose.memory-postgres.yml exec postgres \
  psql -U webhook -d webhook -c 'SELECT count(*), now() - min(received_at) AS atraso FROM loghub.inbox'
```

Com este destino, duas métricas a mais dizem se a stack está saudável:

| métrica | leitura |
|---|---|
| `whs_sink_postgres_duplicates_total` | linhas ignoradas pelo `ON CONFLICT`: quanto o at-least-once está custando em redelivery. Cresce após um restart com dump de emergência; crescendo em regime, o dreno está retentando lotes já gravados |
| `whs_sink_postgres_pool_conns{state}` | `acquired` encostado em `max` com `whs_sink_postgres_pool_waits_total` subindo é o pool pequeno para os workers, ou, com `ring=none`, o `max_inflight` acima do pool |

Ou no MySQL:

```bash
docker compose -f compose.memory-mysql.yml exec mysql \
  mysql -u webhook -pwebhook webhook -e 'SELECT COUNT(*), MAX(event_at) FROM inbox'
```

As métricas `whs_sink_mysql_duplicates_total`, `whs_sink_mysql_pool_conns{state}`
e `whs_sink_mysql_pool_waits_total` leem-se como as do PostgreSQL acima.

Ou no ClickHouse (`FINAL` colapsa as reentregas que o merge ainda não juntou):

```bash
docker compose -f compose.memory-clickhouse.yml exec clickhouse \
  clickhouse-client -u webhook --password webhook -d webhook \
  -q 'SELECT count(), max(event_at) FROM inbox FINAL'
```

| métrica | leitura |
|---|---|
| `whs_sink_clickhouse_dedup_blocks_total` | blocos (um por partição do lote) que o servidor descartou pelo `insert_deduplication_token`: retries de lotes já gravados. Cresce depois de timeouts; um reagrupamento (dump, `ring.db`) não conta aqui, colapsa no merge |
| `whs_sink_clickhouse_pool_conns{state}` | como no PostgreSQL; não há contador de esperas, que o driver não expõe |

Ou no NATS:

```bash
docker compose -f compose.memory-nats.yml exec nats \
  wget -qO- http://127.0.0.1:8222/jsz?streams=true
docker compose -f compose.memory-nats.yml run --rm nats-init \
  nats --server nats://nats:4222 stream info LOGHUB_WEBHOOK_REQUEST
```

| métrica | leitura |
|---|---|
| `whs_sink_nats_duplicates_total` | publicações que o stream respondeu como duplicata pelo `Nats-Msg-Id`: o custo do at-least-once em redelivery, como no PostgreSQL |
| `whs_sink_nats_reconnects_total` | reconexões do cliente; crescendo em regime, a rede entre o webhook e o NATS está instável |

`/healthz` é *liveness* pura: responde 200 enquanto o processo consegue
responder, e de propósito não olha o sink — reiniciar não conserta um Redis
inalcançável. É ele que o `HEALTHCHECK` usa. Quem decide se a instância deve
receber tráfego é `/readyz`, que falha durante o boot, quando o sink está DOWN
ou FATAL, quando o ring passa do `high_water` e durante o desligamento.

### Exit codes

| código | significado |
|---|---|
| 0 | saída limpa |
| 1 | erro de configuração — o processo não chega a servir |
| 3 | o sink não ficou disponível dentro de `sink.boot_timeout` (300s); ou, com `sink=postgres`, `sink=mysql`, `sink=nats` e `sink=clickhouse`, a tabela ou o stream, o índice único ou a chave, a permissão, o modo de escrita, o teto de payload ou o limite de memória foram recusados no boot — nesse caso a saída é **imediata** |
| 130 | interrompido |

Um contêiner reiniciando em loop com exit 1 é erro de configuração, não de
infraestrutura: os logs trazem o parâmetro exato.

---

## Segurança da imagem

- Binário estático, sem CGO — o driver SQLite (`modernc.org/sqlite`), o cliente
  Redis (`rueidis`), o cliente PostgreSQL (`pgx`), o driver MySQL
  (`go-sql-driver/mysql`), o cliente NATS (`nats.go`) e o cliente ClickHouse
  (`clickhouse-go`) são Go puro, então a
  imagem final não tem toolchain nem bibliotecas nativas.
- Roda como uid/gid **65532**, nunca root. O volume nomeado herda dono e modo do
  diretório da imagem na primeira montagem, o que dispensa `chown` externo.
- O binário em `/app` pertence a `root`: o processo o executa e não tem como
  reescrever a si mesmo. `/app` entra no `PATH` da imagem, então
  `docker compose exec webhook go-webhook-server --version` funciona.
- `read_only: true` com `/tmp` em tmpfs, `cap_drop: [ALL]`,
  `no-new-privileges:true`.
- **Toda escrita vai para `/data`** e o resto do sistema de arquivos é
  read-only. Se algum ajuste seu parecer precisar escrever fora de `/data`, o
  ajuste está errado: mude o caminho de volta para dentro do volume em vez de
  remover o `read_only` de `common.yml`.

Para uma imagem versionada:

```bash
WHS_IMAGE_TAG=$(git describe --tags --always) \
WHS_VERSION=$(git describe --tags --always) \
WHS_COMMIT=$(git rev-parse --short HEAD) \
docker compose -f compose.memory-redis.yml build
```
