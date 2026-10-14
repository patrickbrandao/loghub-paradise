# Deploy em Docker

Build e execução do `go-webhook-pg-server` em contêiner. Há **uma** combinação:
ring na RAM, fila `loghub.inbox` no PostgreSQL (`docs/SPEC.md` §0). O compose
sobe o servidor e um PostgreSQL 18 com a fila já criada.

```bash
docker compose up -d --build
# o mesmo, pelo nome do arquivo
docker compose -f compose.memory-postgres.yml up -d --build
```

Os arquivos têm a forma dos do `go-webhook-fast-server` (um `common.yml` herdado
por `extends`), com nomes `whp-*` e portas próprias, para os dois rodarem lado a
lado no mesmo host. As variáveis (`WHS_*`) e as métricas (`whs_*`) são as do
fast: um `.env` ou um painel de lá serve aqui (`docs/SPEC.md` §0, decisão 3).

---

## Portas

| serviço | porta do host | sobrescreva com |
|---|---|---|
| HTTP público | `8200` | `WHS_HTTP_PORT` |
| admin (`/healthz`, `/readyz`, `/metrics`) | `127.0.0.1:9201` | `WHS_ADMIN_PORT` |
| PostgreSQL | `127.0.0.1:55432` | `WHS_POSTGRES_PORT` |

A porta admin é publicada **somente em `127.0.0.1` do host**. Dentro do
contêiner ela escuta em `0.0.0.0` — não há como publicar de outra forma — e por
isso o servidor emite este warning no boot, que é esperado neste contexto:

```
admin.bind="0.0.0.0:9101" is not loopback: /metrics and /readyz are reachable off-host
```

---

## Memória

O servidor **não define `GOMEMLIMIT` sozinho** e avisa no boot se ele faltar
(`docs/SPEC.md` §12.4). Sem ele o coletor de lixo do Go não sabe do teto do
contêiner e o OOM killer chega antes do GC — levando o ring junto, porque o dump
de emergência não roda num processo morto pelo kernel. Por isso `common.yml`
define os dois valores, parametrizáveis pelo `.env`:

| variável | padrão | papel |
|---|---|---|
| `WHS_GOMEMLIMIT` | `512MiB` | `GOMEMLIMIT` do processo |
| `WHS_MEMORY_LIMIT` | `768M` | limite de memória do contêiner, **acima** do `GOMEMLIMIT` |
| `WHS_RING_MAX_BYTES` | `256MiB` | teto de bytes do ring, todo na RAM |

O boot avisa se `GOMEMLIMIT` for menor que o dobro de `ring.max_bytes`; os
padrões respeitam isso. Quem subir um dos três, suba os outros junto.

As duas variáveis têm gramáticas diferentes: `WHS_MEMORY_LIMIT` é lido pelo
Compose (`768M`, `1g`); `WHS_GOMEMLIMIT` pelo runtime do Go, que aceita só um
inteiro com sufixo `B`, `KiB`, `MiB`, `GiB` ou `TiB`, ou `off`. Um
`GOMEMLIMIT=768M` derruba o processo antes do boot com `malformed GOMEMLIMIT`, e
com `restart: unless-stopped` o contêiner entra em laço.

---

## Arquivos

| arquivo | papel |
|---|---|
| `Dockerfile` | build multi-stage, binário estático (`CGO_ENABLED=0`), imagem final Alpine rodando como uid 65532 |
| `common.yml` | fragmento herdado via `extends`: build, healthcheck, hardening, limites, logging |
| `compose.memory-postgres.yml` | a combinação, com o PostgreSQL 18 ao lado |
| `docker-compose.yml` | `include` do compose acima, para `docker compose up` sem `-f` |
| `postgres/inbox.sql` | a fila `loghub.inbox`, DDL literal da skill e de `docs/SPEC.md` §3.6; montada em `/docker-entrypoint-initdb.d` |
| `../.dockerignore` | mantém o contexto de build mínimo; precisa ficar na raiz do módulo, que é o contexto |

O build usa a raiz do módulo como contexto (`context: ..`), não esta pasta.

---

## Configuração

Todo parâmetro aceita variável de ambiente `WHS_<SEÇÃO>_<CHAVE>` com `.`
virando `_` — `sink.postgres.addr` → `WHS_SINK_POSTGRES_ADDR`. A precedência é:

```
defaults  <  arquivo INI  <  ambiente  <  linha de comando
```

Os compose usam ambiente. Para ver a configuração efetiva e a lista completa de
parâmetros:

```bash
docker compose run --rm --no-deps webhook --dump-config
docker compose run --rm --no-deps webhook --help
```

**Vindo do fast.** `WHS_RING_MODE=memory`, `WHS_SINK_TYPE=postgres` e
`WHS_RATELIMIT_ENABLED=false` continuam aceitos; qualquer outro valor deles
encerra o boot com código 1 dizendo o porquê. Uma chave de recurso retirado
num INI ou numa flag (`[sink.redis]`, `ratelimit.rate`, `ring.max_inflight`...)
também é código 1; **no ambiente**, uma variável dessas (`WHS_SINK_REDIS_*`,
`WHS_RATELIMIT_RATE`...) é ignorada, como toda `WHS_*` que o servidor não
conhece (`docs/SPEC.md` §0). Não há recarga por `SIGHUP`: o sinal é ignorado com um
aviso, e mudar a configuração é recriar o contêiner. Não mande o sinal com
`docker kill -s HUP`: o Docker marca o contêiner como parado à mão por **qualquer**
`docker kill`, e a partir daí o `restart: unless-stopped` deixa de valer — uma
saída com código 3 mais tarde não reinicia sozinha (observado em 2026-10-07).

Para usar arquivo INI em vez de ambiente, monte-o e aponte o comando:

```yaml
    volumes:
      - ./meu-server.ini:/etc/webhook/server.ini:ro
    command: ["--config=/etc/webhook/server.ini"]
```

---

## Layout do contêiner: binário em `/app`, dados em `/data`

**Toda escrita do serviço acontece em `/data`.** É regra do `loghub-paradise`
inteiro (`docs/SPEC.md` §13.6, invariante 13). Aqui são duas escritas, ambas
derivadas de `WHS_RING_DATA_DIR`, fixado em `/data` na imagem:

| escrita | caminho | quando |
|---|---|---|
| identidade LogHub | `/data/agent_uuid`, `/data/machine_id` | na primeira subida |
| dumps de emergência | `/data/emergency/emg-*.jsonl` | sink FATAL ou `drain_timeout` estourado com ring não vazio |

O resto do sistema de arquivos é **read-only** (`read_only: true` em
`common.yml`). O caso caro é o `emergency.dir`: o diretório só é criado no
instante do dump, então um caminho errado nele aparece **durante o incidente**,
com o ring inteiro virando `whs_events_dropped_total{reason="emergency_failed"}`.
Nunca o aponte para fora de `/data`.

| caminho | papel | gravável |
|---|---|---|
| `/app` | o binário, dono `root`, modo `0755` | não |
| `/data` | identidade, dumps de emergência, e o `WORKDIR` | **sim**, e só ele |
| `/etc/webhook` | INI montado com `:ro` | não |
| `/tmp` | `tmpfs`, rede de segurança do `read_only` | sim, mas volátil |

Para mudar onde os dados ficam **no host**, troque o volume — nunca o caminho
dentro do contêiner:

```yaml
    volumes:
      # o diretório precisa existir e pertencer ao uid 65532
      - /mnt/nvme0/whp:/data
```

---

## Identidade LogHub e o envelope

Todo evento é um envelope JLH `type=webhook` (`docs/SPEC.md` §3), com o corpo da
requisição dentro dele (`payload.content`, em texto ou base64). O `200` devolve
o `uuid` do envelope no cabeçalho `X-LogHub-UUID` e num corpo JSON
`{"uuid":…,"mode":"fast","status":"accepted","stage":"ring"}`.

| Variável | De onde vem | Observação |
|---|---|---|
| `AGENT_NAME` | a imagem (`go-webhook-pg-server`) | nomeia as conexões ao banco (`application_name`) |
| `WORKSPACE` | `WHS_WORKSPACE` do `.env` (`default`), via `common.yml` | o **tenant**: vira o `space` de todo envelope; sem `_` |
| `HOSTNAME` | o `hostname:` do serviço | RFC 1123; sem ele seria o id do contêiner |
| `AGENT_UUID`, `MACHINE_ID` | gerados e persistidos em `/data` na primeira subida | **um volume por instância**: duas réplicas no mesmo volume publicam o mesmo `from` |

Uma identidade inválida encerra o processo com um código entre 100 e 114 antes
de qualquer porta abrir (a tabela está na skill `go-loghub-ident`).

---

## PostgreSQL: a fila `loghub.inbox`

O servidor **enfileira** cada envelope na `loghub.inbox`, a fila comum do LogHub
(skill `loghub-postgres-table-inbox`, `docs/SPEC.md` §3.6).

- **O servidor nunca cria nem altera a fila.** Ele valida no boot que ela existe
  com as dez colunas que ele escreve, que nenhuma coluna extra é obrigatória,
  que há um índice único exatamente sobre `(uuid)` e nenhum outro, e que o papel
  tem `INSERT` e `SELECT (uuid)`; recusa também um banco que não seja UTF8 e uma
  tabela particionada. Qualquer falha é código 3 **na hora**, sem esperar
  `sink.boot_timeout`.
- **`postgres/inbox.sql` é a DDL literal da skill.** A imagem oficial o executa
  **uma** vez, ao criar o volume. Depois disso o esquema é de quem administra o
  banco.
- **A imagem 18 guarda os dados em `/var/lib/postgresql/18/docker`**: o volume é
  montado em `/var/lib/postgresql`.
- **O papel precisa de `INSERT` e de `SELECT (uuid)`** (`GRANT INSERT, SELECT
  (uuid) ON loghub.inbox TO ...`): o `ON CONFLICT (uuid)` lê a coluna árbitra.
- **Cada worker é uma conexão**, e o `max_connections` (100 por padrão) é
  dividido por todas as instâncias. O padrão é 4 workers.
- **Um failover do banco derruba o serviço com código 3**, de propósito
  (`docs/SPEC.md` §8.5.7): uma sessão somente leitura é fatal, o ring vai para
  `/data/emergency`, o supervisor reinicia, e o boot espera um primário e
  reingere o dump.
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
query vira U+FFFD na coluna, porque o `jsonb` recusa o NUL.

---

## O dump de emergência (JSON Lines)

Quando o PostgreSQL fica FATAL, ou o desligamento estoura o `drain_timeout`, o
que o ring ainda tem vai para `/data/emergency/emg-<agent_uuid>-<unixnano>.jsonl`:
**um envelope JLH por linha**, exatamente os bytes cujo `uuid` o cliente
recebeu (`docs/SPEC.md` §9). O boot seguinte reingere o arquivo direto na fila,
antes de abrir a porta pública, e o apaga.

É JSON Lines comum, então as ferramentas de sempre servem:

```bash
docker compose exec webhook sh -c 'ls -la /data/emergency; wc -l /data/emergency/*.jsonl'
docker compose exec webhook sh -c 'head -1 /data/emergency/*.jsonl' | jq .uuid
```

Um `.emg-*.jsonl.tmp` é um dump interrompido: o boot o informa num `WARN` e não
o lê. Renomeado para o nome final (sem o ponto inicial e sem o `.tmp`), as linhas
íntegras dele são reingeridas no boot seguinte (SPEC §9.1).

Uma linha danificada (um temporário de dump renomeado à mão, uma edição à mão) é
pulada; as linhas íntegras antes e depois dela são entregues, e o arquivo vira
`.corrupt` em vez de ser apagado. Ao editar um dump, mantenha `\n` puro: um
editor que grave `\r\n` torna **todas** as linhas danificadas. Uma linha que o
PostgreSQL recuse mesmo assim fica no arquivo em quarentena, e o boot segue. Reingeri-lo de novo (renomeando de volta) é
seguro: o que já foi entregue é absorvido pelo `ON CONFLICT (uuid)`. Um
`emg-*.bin` do `go-webhook-fast-server` **não** é lido por este servidor: o boot
avisa e o deixa intocado, para ser reingerido pelo fast.

---

## Antes de expor à internet

1. **`WHS_PROXY_TRUSTED`.** O default (`127.0.0.0/8,::1`) é fail-closed: nenhum
   header de encaminhamento é acreditado, e o IP do cliente é o peer do socket.
   Atrás de proxy/ingress isso faz o servidor enxergar o proxy como cliente —
   quebra `network.ip.src` no envelope. Declare os CIDRs do proxy.
2. **Limite de taxa no proxy.** Este servidor não tem rate limit
   (`docs/SPEC.md` §6); o proxy reverso, que vê o IP real, é o lugar dele.
3. **`stop_grace_period`.** Está em 45s em `common.yml`, acima do
   pior caso até o dump começar, `pre_stop_delay + drain_timeout +
   sink.op_timeout` (0s + 30s + 5s com os padrões: o `drain_timeout` é um prazo
   só para as conexões e o ring, e o lote em voo de um worker ainda leva até o
   `op_timeout`), com folga para escrever o próprio dump (`docs/SPEC.md`
   §10.3). Os 10s default do Docker matariam o processo no meio do dreno —
   perdendo eventos já respondidos com 200. Se aumentar qualquer um dos três,
   aumente também o grace period.
4. **`WHS_SHUTDOWN_PRE_STOP_DELAY`.** Atrás de balanceador, ponha o tempo que
   ele leva para tirar o endpoint da rotação.
5. **`WHS_SINK_POSTGRES_PASSWORD`.** O padrão `webhook` serve ao ambiente de
   desenvolvimento e a mais nada. Em produção, defina no `.env`; e contra um
   banco fora do compose, ligue `WHS_SINK_POSTGRES_TLS` e monte a CA em
   `/etc/webhook` (somente leitura) apontando `WHS_SINK_POSTGRES_TLS_CA` para ela
   — o volume `./certs:/etc/webhook:ro` já está comentado no
   `compose.memory-postgres.yml`, e o compose repassa a variável (vazia por
   padrão); sem o volume, o serviço `read_only` não tem a CA e o boot falha.

---

## Operação

```bash
docker compose up -d --build     # sobe
docker compose logs -f webhook   # logs (JSON por padrão)
docker compose ps                # estado + health
docker compose down              # derruba, preserva volumes
docker compose down -v           # derruba e APAGA os dados (a fila inclusive)
```

Verificação rápida:

```bash
curl -fsS -X POST localhost:8200/qualquer/caminho -d '{"ping":1}'
curl -fsS localhost:9201/readyz
curl -fsS localhost:9201/metrics | grep -E '^whs_(requests_total|events_written_total|ring_items|sink_up)'
docker compose exec postgres \
  psql -U webhook -d webhook -c 'SELECT count(*), now() - min(received_at) AS atraso FROM loghub.inbox'
```

As métricas que dizem se a stack está saudável:

| métrica | leitura |
|---|---|
| `whs_sink_up` | 1 = destino alcançável. Zerou, a porta pública responde 503 |
| `whs_ring_items` / `whs_ring_capacity_items` | ocupação do ring; a razão entre elas dispara `/readyz` no `high_water` |
| `whs_events_written_total` | eventos confirmados pela fila |
| `whs_rejected_total` | recusas — inclui os 429 de ring cheio |
| `whs_events_dropped_total` | **deve ficar em zero**; qualquer valor aqui é evento perdido |
| `whs_emergency_files` | dumps de emergência pendentes de reingestão |
| `whs_sink_postgres_duplicates_total` | linhas ignoradas pelo `ON CONFLICT`: o custo do at-least-once. Cresce depois de uma reingestão; crescendo em regime, o dreno está retentando lotes já gravados |
| `whs_sink_postgres_pool_conns{state}` | `acquired` encostado em `max` com `whs_sink_postgres_pool_waits_total` subindo é o pool pequeno para os workers |

`/healthz` é *liveness* pura: responde 200 enquanto o processo consegue
responder, e de propósito não olha o banco — reiniciar não conserta um
PostgreSQL inalcançável. Quem decide se a instância deve receber tráfego é
`/readyz`, que falha durante o boot, quando o sink está DOWN ou FATAL, quando o
ring passa do `high_water` e durante o desligamento.

### Exit codes

| código | significado |
|---|---|
| 0 | saída limpa |
| 1 | erro de configuração — inclusive um recurso retirado pedido (`docs/SPEC.md` §0) |
| 3 | o PostgreSQL não ficou disponível dentro de `sink.boot_timeout` (300s); ou a fila, o índice, a permissão ou o modo de escrita foram recusados no boot — nesse caso a saída é **imediata**; ou o sink ficou FATAL em operação (o ring foi para o dump) |
| 100–114 | identidade LogHub inválida |
| 130 | interrompido por um segundo sinal |

Um contêiner reiniciando em loop com exit 1 é erro de configuração, não de
infraestrutura: os logs trazem o parâmetro exato.

---

## Segurança da imagem

- Binário estático, sem CGO — o cliente PostgreSQL (`pgx`) é Go puro, então a
  imagem final não tem toolchain nem bibliotecas nativas.
- Roda como uid/gid **65532**, nunca root. O volume nomeado herda dono e modo do
  diretório da imagem na primeira montagem, o que dispensa `chown` externo.
- O binário em `/app` pertence a `root`: o processo o executa e não tem como
  reescrever a si mesmo.
- `read_only: true` com `/tmp` em tmpfs, `cap_drop: [ALL]`,
  `no-new-privileges:true`.

Para uma imagem versionada:

```bash
WHS_IMAGE_TAG=$(git describe --tags --always) \
WHS_VERSION=$(git describe --tags --always) \
WHS_COMMIT=$(git rev-parse --short HEAD) \
docker compose build
```
