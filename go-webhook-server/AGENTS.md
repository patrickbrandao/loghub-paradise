# AGENTS.md — go-webhook-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes de
editar qualquer arquivo.

---

## 1. Onde você está

Você está em `go-webhook-server/`, **uma subpasta** do monorepo
`loghub-paradise/`. Não é um projeto isolado.

```
loghub-paradise/                  <- monorepo, SEM go.mod, mantido por outro agente
  go-webhook-server/              <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-webhook-server
    cmd/go-webhook-server/
    internal/
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/
  webhook-worker/                 <- futuro, ainda não existe
  webhook-admin/                  <- futuro, ainda não existe
  webhook-shots/                  <- futuro, ainda não existe
  tmp/                            <- rascunhos históricos, NÃO use como referência
```

O servidor recebe webhooks arbitrários da internet, encapsula cada requisição num
envelope **JLH do LogHub** (`type=webhook.request`, `docs/SPEC.md` §3) e entrega
a um destino configurável. É o *bridge-in* HTTP do LogHub. Escala
horizontalmente; instâncias não compartilham estado.

Fontes normativas (skills do servidor MCP `loghub-mcp`): `loghub-json-schema`
(envelope), `loghub-type-webhook-schema` (payload `webhook.request`, respostas),
`loghub-postgres-table-inbox` (`sink=postgres`), `go-loghub-uuidv7` e
`go-loghub-ident`. Cópias dos schemas em `internal/jlhcheck/loghub/`. O irmão
`../go-webhook-fast-server/` emite `type=webhook`/`mode=fast` com o mesmo payload.

## 2. Fronteira de responsabilidade

**Seu escopo é esta pasta e nada acima dela.**

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| `docs/SPEC.md`, `docs/TESTING.md` | você |
| Diretório raiz `loghub-paradise/` e seus arquivos | agente do monorepo |
| **Git: init, commit, branch, tag, remoto, `.gitignore`** | agente do monorepo |
| Integração contínua, publicação de imagem, releases | agente do monorepo |
| Os outros serviços do ecossistema | agente do monorepo |

### Git não é seu

O versionamento vive no nível de `loghub-paradise/`, não aqui. Regras:

- **Nunca** rode `git init`, `git add`, `git commit`, `git tag`, `git push` ou
  `git remote` a partir desta pasta.
- Se precisar que algo seja versionado, **descreva o que mudou** e deixe o commit
  para o agente do monorepo.
- Um `.gitignore` desta pasta é aceitável apenas para padrões estritamente locais
  (`data/`, `*.db`, `*.db-wal`, `*.db-shm`); o `.gitignore` principal é do nível
  acima.
- As tags de release seguem o formato `go-webhook-server/vX.Y.Z`, criadas do nível
  acima.

> O `.git` que existia **dentro** desta pasta (zero commits) não existe mais:
> desde 2026-09-20 o versionamento vive na raiz `loghub-paradise/`.

## 3. A especificação manda

`docs/SPEC.md` está marcada como **congelada para implementação**. Ela não é
documentação descritiva do código: é o contrato que o código cumpre.

- Discrepância entre código e spec é **bug no código**, salvo justificativa
  explícita registrada na própria spec (há precedentes, como a decisão sobre o
  gerador de identificadores em §14.3).
- Para mudar comportamento: **altere a spec primeiro**, com o porquê, depois o
  código, depois o teste.
- Ao citar a spec em comentário, commit ou resposta, use o número da seção
  (`§4.3`, `§10.2`). O código já faz isso e a convenção deve continuar.
- `docs/TESTING.md` §8 registra as lacunas conhecidas com honestidade. Se você
  fechar uma, atualize a lista. Se descobrir outra, acrescente.
- `docs/SPEC.md` §19.1 lista as **pendências conhecidas**: problemas reais que o
  código ainda tem, com a direção da correção. Corrigir uma é mudar a seção
  normativa citada, depois o código e o teste, e tirar o item de lá.

## 4. O modelo mental: dois eixos ortogonais

Não confunda os dois. Quase todo mal-entendido sobre este projeto começa aqui.

- **RING** — *onde o evento espera* entre ser aceito e ser entregue.
  `memory` | `sqlite` | `none`
- **SINK** — *para onde o evento vai*, o destino final.
  `redis` | `sqlite` | `postgres` | `mysql` | `nats` | `clickhouse` | `none`

As vinte e uma combinações são válidas e têm significado distinto. `ring=sqlite` é
uma **fila** (linhas são apagadas após entrega); `sink=sqlite` é um **destino
final** append-only. São bancos diferentes, em arquivos diferentes, e o servidor
recusa subir se apontarem para o mesmo arquivo. `sink=postgres` e `sink=mysql`
são destinos finais **compartilhados entre instâncias**: o servidor **nunca cria
a tabela**, só a valida no boot (`SPEC.md` §8.5, §8.6). No PostgreSQL o destino é
a fila `loghub.inbox` da skill (§3.6); no MySQL e no SQLite, uma tabela `inbox`
que a espelha (§3.7). `sink=nats` exige
JetStream e valida o stream do mesmo jeito (§8.7); NATS "core" não confirma
nada e não serve. `sink=clickhouse` é o armazém analítico: a mesma `inbox`
espelhada num `ReplacingMergeTree`, validada e nunca criada (§3.9, §8.8).

**Nenhuma implementação de ring consulta o tipo do sink, e vice-versa.** Se você
escrever um `if sinkType == ...` dentro de um ring, quebrou o desenho.

## 5. Mapa do código

```
cmd/go-webhook-server/main.go   versão/commit, lhident.Initialize() (UMA vez), internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão
  config/     registry único -> ini + flags + env + --help + dump; validação
  server/     http.Server, pipeline da requisição, admin, listeners, limites
  clientip/   X-Forwarded-For / X-Real-IP + proxies confiáveis   [lógica pura]
  ratelimit/  token bucket com shards e sweep                    [lógica pura]
  ident/      Identity congelada e gramática de space             [lógica pura]
  envelope/   escritor JLH, payload webhook.request, ParseHead, query, pools [lógica pura, fuzz]
  jlhcheck/   oráculo de teste: schemas + regras semânticas       [só em teste]
  ring/       interface Ring + memory.go, sqlite.go, none.go
  sink/       interface Sink + redis.go, sqlite.go, postgres.go, mysql.go, nats.go, clickhouse.go, none.go, workers.go, health.go
  emergency/  dump binário e reingestão pós-crash
  obs/        métricas Prometheus próprias + slog
```

| Pacote | Linhas | Onde mexer com mais cuidado |
|---|---|---|
| `app` | ~4.400 | ordem do boot e as cinco fases do shutdown |
| `sink` | ~6.900 | posse de buffer nos caminhos de erro dos workers; em `postgres.go` e `mysql.go`, as tabelas de código de erro e o reset dos slices do pool; em `nats.go`, as três opções que não são as padrões da biblioteca (§8.7.2) |
| `config` | ~2.900 | o registry é a fonte única; não duplique um parâmetro |
| `server` | ~1.600 | ordem do pipeline em `handler.go` |
| `ring` | ~1.500 | contabilidade de bytes e itens |
| `envelope` | ~1.300 | pools: liberação dupla é o pior bug possível aqui |
| `obs` | ~1.300 | fronteiras de histograma são estáticas, nunca derivadas da config |
| `ratelimit` | ~890 | evicção exata: só apaga bucket cheio |
| `emergency` | ~780 | decoder tem que tolerar arquivo truncado sem pânico |
| `clientip` | ~650 | único parser de entrada hostil; cobertura por fuzz é obrigatória |

`internal/` impede import externo acidental. `clientip`, `ratelimit` e `envelope`
são lógica pura: testáveis sem rede, sem disco e sem Redis.

## 6. Invariantes que nenhuma mudança pode violar

Resumo de `SPEC.md` §17. Se sua alteração conflita com um destes, ela está errada.

1. O `200` nunca sai antes de a garantia da combinação `ring` × `sink` ser cumprida.
2. Ring cheio responde `429`. Nunca `200` seguido de descarte, exceto com `sink=none`.
3. Um buffer volta ao pool **uma vez**, e só após confirmação.
4. Evento aceito só sai do ring após confirmação do sink; nunca é descartado para
   liberar espaço.
5. `network.ip.src` (e todo cabeçalho de proxy: `X-Forwarded-Proto/-Host/-Port`,
   `X-Client-Port`, `X-Server-Addr`, fingerprints) nunca vem de header quando o
   peer TCP não está em `proxy.trusted`.
6. Nenhum label de métrica deriva de conteúdo controlado pelo cliente.
7. O corpo nunca é parseado, decodificado, validado ou modificado.
8. `network.meta` `peer-addr` sempre registra a ponta TCP real, independente de headers.
9. Nenhum log síncrono por requisição no caminho padrão.
10. Um comando do cliente Redis nunca é reusado após execução (ele é reciclado
    para um pool interno; um retry precisa **reconstruir** os comandos).
11. O envelope JLH é idêntico em todos os sinks, escrito UMA vez no handler. A
    única alteração é a da cópia que vai para o `jsonb` da `loghub.inbox`
    (`\u0000` → `\ufffd`), nunca no JLH do item.
12. `ring` e `sink` são ortogonais.
13. Toda escrita em disco fica sob `ring.data_dir` — e dentro de um contêiner
    esse diretório é `/data`, em toda imagem do ecossistema.
14. `uuid` e `event` saem de UMA amostra de relógio (`GenerateAt`); o `uuid`
    devolvido em `X-LogHub-UUID` é o do envelope em todo destino, e nunca é
    regerado (retry, ring, dump).

## 7. Comandos

Todos rodam a partir desta pasta.

Suíte rápida, a cada mudança:

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**: os bugs de perda de dados deste projeto vivem em
caminhos concorrentes. A suíte leva cerca de 45 segundos (2026-09-15, num
MacBook Air M2; eram 66 antes das correções de setembro).

Cobertura real (por pacote engana; a suíte de `app` exercita `server`, `ring` e
`sink` sem que isso lhes seja atribuído):

```bash
go test -coverpkg=./internal/... -coverprofile=cov.out ./... && go tool cover -func=cov.out | tail -1
```

Referência atual (2026-09-15): **87,9%** sem servidor externo; **90,4%** com
PostgreSQL de teste (`WHS_TEST_PG_DSN`, abaixo).

Fuzzing, obrigatório antes de qualquer release, 10 minutos cada:

```bash
go test ./internal/clientip/  -run=XXX -fuzz=FuzzResolve -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=FuzzRead    -fuzztime=60s
go test ./internal/envelope/  -run=XXX -fuzz=^FuzzEnvelope$ -fuzztime=60s
go test ./internal/server/    -run=XXX -fuzz=FuzzServeHTTP   -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$ -fuzztime=60s
```

Destino Redis — **é o padrão de produção e os testes pulam sem um servidor**:

```bash
docker run --rm -d -p 16379:6379 --name whs-test-redis redis:7-alpine
WHS_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race -timeout 600s ./internal/app/
docker rm -f whs-test-redis
```

Se você mexeu em `internal/sink/redis.go`, `workers.go`, `health.go` ou no boot,
**rode esta suíte**. Sem ela, o caminho padrão fica sem prova.

Destino PostgreSQL — **os testes pulam sem um servidor**, e o servidor não cria a
fila (a suíte cria as dela, cada uma num esquema próprio, a partir de
`docker/postgres/inbox.sql`, a DDL literal da skill; PostgreSQL **18**):

```bash
docker run --rm -d -p 15432:5432 -e POSTGRES_USER=whs -e POSTGRES_PASSWORD=whs \
  -e POSTGRES_DB=whs_test --name whs-test-pg postgres:18-alpine
WHS_TEST_PG_DSN='postgres://whs:whs@127.0.0.1:15432/whs_test?sslmode=disable' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f whs-test-pg
```

Sem Docker, `docs/TESTING.md` §5.2 mostra como subir um `initdb`/`pg_ctl`
efêmero. Se você mexeu em `internal/sink/postgres.go`, em `health.go`, na
validação de `config` ou em `docker/postgres/inbox.sql`, **rode esta suíte**.

Destino MySQL — **os testes pulam sem um servidor**, e o servidor não cria a
tabela (a suíte cria as dela a partir de `docker/mysql/inbox.sql`; o DSN
precisa de `CREATE TABLE` e, para os subtestes de privilégio, `CREATE USER`):

```bash
docker run --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=whs -e MYSQL_DATABASE=whs_test \
  --name whs-test-mysql mysql:8.4
WHS_TEST_MYSQL_DSN='root:whs@tcp(127.0.0.1:13306)/whs_test' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f whs-test-mysql
```

Destino NATS — **os testes pulam sem um servidor**, JetStream ligado é
obrigatório e o servidor não cria o stream (a suíte cria os dela a partir de
`docker/nats/stream.json`). Sem Docker, o próprio `nats-server` é um módulo Go:

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &       # sem JetStream
WHS_TEST_NATS_URL=nats://127.0.0.1:14222 WHS_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

Destino ClickHouse — **os testes pulam sem um servidor**, protocolo nativo,
25.8 ou mais novo, e o servidor não cria a tabela (a suíte cria as dela a
partir de `docker/clickhouse/inbox.sql`; os subtestes de privilégio e memória
criam usuários e pulam sem `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`):

```bash
docker run --rm -d -p 19000:9000 -e CLICKHOUSE_USER=whs -e CLICKHOUSE_PASSWORD=whs \
  -e CLICKHOUSE_DB=whs_test -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --ulimit nofile=262144:262144 --name whs-test-clickhouse clickhouse/clickhouse-server:25.8
WHS_TEST_CLICKHOUSE_DSN='clickhouse://whs:whs@127.0.0.1:19000/whs_test' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f whs-test-clickhouse
```

Se você mexeu em `internal/sink/mysql.go`, `nats.go`, `clickhouse.go`,
`health.go`, na validação de `config`, em `docker/mysql/inbox.sql`,
`docker/nats/` ou `docker/clickhouse/`, **rode a suíte correspondente**.

Executar e inspecionar:

```bash
go run ./cmd/go-webhook-server --help
go run ./cmd/go-webhook-server --dump-config
# o hostname do macOS não pode ter `_`, ou o go-loghub-ident sai com 109
HOSTNAME=dev-mac go run ./cmd/go-webhook-server --ring.mode=memory --sink.type=sqlite \
  --ring.data_dir=./data --server.bind=127.0.0.1:8080 --admin.bind=127.0.0.1:9101
```

Docker, a partir de `docker/`:

```bash
docker compose up -d --build                          # memory -> redis (padrão)
docker compose -f compose.memory-sqlite.yml up -d --build
docker compose -f compose.memory-postgres.yml up -d --build
```

## 8. Convenções

**Idioma.** Código, identificadores e comentários em **inglês**. Documentação
(`docs/`, `docker/README.md`, este arquivo) e conversa com o usuário em
**português**. Não misture dentro de um mesmo arquivo.

**Comentários explicam o porquê, não o quê.** O padrão deste projeto é um
comentário que justifica a decisão e diz o que aconteceria com a alternativa
rejeitada. Leia `internal/server/handler.go` ou `internal/sink/workers.go` antes
de escrever o seu; imite a densidade e o tom.

**Dependências: oito em produção, e ponto.** `rueidis` para Redis,
`modernc.org/sqlite` para SQLite (Go puro, sem CGO, binário estático),
`github.com/jackc/pgx/v5` para PostgreSQL (Go puro; usado pela API nativa, nunca
por `database/sql`, porque a tabela de SQLSTATE de `SPEC.md` §8.5.7 precisa do
`pgconn.PgError`), `github.com/go-sql-driver/mysql` para MySQL/MariaDB (Go
puro; este sim pelo `database/sql`, que é como ele foi desenhado, e o
`*mysql.MySQLError` atravessa a camada com o `Number` que §8.6.6 precisa),
`github.com/nats-io/nats.go` com o pacote `jetstream` para NATS (a API atual,
não a `JetStreamContext` legada), `github.com/ClickHouse/clickhouse-go/v2` para
ClickHouse (API nativa e protocolo nativo, escolha do usuário contra o HTTP da
biblioteca padrão; o custo está em `SPEC.md` §14.3: ~4 MB de binário e uma dúzia
de transitivas), `github.com/patrickbrandao/go-loghub-uuidv7`
(UUIDv7 nível 3 por `GenerateAt`) e `github.com/patrickbrandao/go-loghub-ident`
(AgentUUID, Workspace) — estes dois sem dependência transitiva nenhuma.
`github.com/google/uuid` e `github.com/santhosh-tekuri/jsonschema/v6` não têm
`import` em código de produção (a `google/uuid` entra no binário só como
transitiva do `clickhouse-go`). Adicionar dependência de produção exige justificativa na
spec (`SPEC.md` §14.3).

**Identidade: `lhident.Initialize()` só no `main`, uma vez.** A segunda chamada
sai com 112, e os testes montam a app dezenas de vezes: por isso a app recebe
um `ident.Identity` (`app.Main(args, initIdentity)`; nos testes,
`runContext(ctx, args, id, hook)`). `ring.data_dir` é exportado como `DATADIR`,
absoluto, antes do `Initialize()` (`SPEC.md` §10.1).

**Um gerador de uuid por processo** (`envelope/jlh.go`, `uuidv7.NewGenerator()`),
e o `uuid` sempre de `NewUUID(t)` com a MESMA amostra `t` do `event`. Ler o
relógio duas vezes faz o fato parecer posterior ao envelope.

**Configuração: o registry é fonte única.** Um parâmetro novo entra em
`internal/config/registry.go` e daí saem sozinhos o parser de ini, as flags, o
env, o `--help`, o dump e a validação. Nunca adicione um caminho paralelo.

**Métricas: cardinalidade fixa.** Todo label tem conjunto de valores finito e
conhecido em tempo de compilação. Nada de path, método, IP ou user-agent como
label. Fronteiras de histograma são estáticas e idênticas em toda instância.

**Escrita em disco: uma raiz só.** Os três arquivos que o serviço grava
(`ring.db`, `events.db`, `emergency/`) derivam de `ring.data_dir`, e dentro de um
contêiner essa raiz é `/data` — regra de todas as imagens do `loghub-paradise`,
não só desta. Um caminho de escrita que não derive dali quebra a regra: os
compose sobem com `read_only: true`, então ele falha no boot, ou pior, falha no
meio de um incidente se for o `emergency.dir`. Ver `SPEC.md` §13.6.

**Caminhos fixos do contêiner.** `/app` guarda o binário pronto (dono `root`),
`/data` é a única área gravável, `/etc/webhook` é o INI em `:ro` e `/tmp` é
tmpfs volátil. Junto com eles, duas regras: o `WORKDIR` é `/data`, e o binário é
invocado pelo caminho absoluto `/app/<nome>`, nunca pelo nome solto. Vale para
toda imagem do `loghub-paradise`; não invente um caminho novo para um serviço
só.

**Sem CGO.** `CGO_ENABLED=0` é premissa do Dockerfile e do binário estático.

## 9. Armadilhas que já custaram bugs reais

Registradas em `docs/TESTING.md` §9. As que mais reincidem:

- **Posse de buffer.** O item do ring é dono de `Env` (o JLH, corpo incluído). Ele volta ao pool
  exatamente uma vez, só após confirmação. Num retry de lote o item **ainda é o
  dono** e nada é devolvido. O buffer do corpo lido volta ao pool assim que o
  JLH é escrito: nada mais o referencia. Devolver cedo coloca o corpo de um cliente dentro do
  evento de outro — silencioso, invisível sob carga.
- **Comandos do rueidis são reciclados.** Reusar o slice de comandos num retry
  corrompe os dados sem erro. Reconstrua a cada tentativa.
- **Lote retido pelo worker não está no ring.** Um worker que retenta segura os
  itens fora do ring; o shutdown precisa coletá-los de `Workers.Pending()` ou eles
  somem depois de terem recebido `200`.
- **E o `hold` não pode sobrescrever.** Corolário do anterior, que custou um bug
  real: um worker que **abandona** o lote no shutdown o mantém registrado, e se
  o `loop` chamar `Dequeue` outra vez ele pode voltar com um segundo lote e
  gravá-lo por cima do primeiro. O `Dequeue` sorteia entre contexto cancelado e
  ring com backlog, então isso acontece em cerca de metade dos desligamentos com
  fila. Por isso `deliver` devolve se a posse mudou de mãos e o `loop` para
  quando não mudou.
- **Nada é promovido ao lado do JLH.** Os destinos leem `space`, `type` e as
  colunas da `inbox` do próprio envelope (`envelope.ParseHead`, estrito). Um
  item que volta do `ring.db` ou de um dump é indistinguível do que entrou; a
  v1 guardava um `ItemMeta` ao lado e uma reingestão com ele zerado já gravou
  linhas invisíveis.
- **`jsonb` recusa `\u0000`** (`22P05`, classe 22: o lote inteiro descartado).
  O corpo com NUL vai em base64; o `%00` da query vira `\ufffd` só na cópia
  do `sink=postgres` (`sink/inbox.go`, `jsonbSafe`).
- **`INSERT OR IGNORE` engole CHECK no SQLite.** A `inbox` usa
  `ON CONFLICT (uuid) DO NOTHING`, que ignora só a duplicata.
- **Chave primária no `ring=sqlite`.** Perder a identidade do item na releitura já
  descartou 149 de 150 eventos por colisão.
- **Fronteira de histograma como string.** Emitir `0.01` num scrape e `0.010` no
  seguinte cria duas séries e quebra todo quantil, em silêncio. As strings são
  pré-computadas na inicialização.
- **Slices do pool de argumentos do `sink=postgres`.** Os dez arrays do
  `unnest` seguram referências para o `Env` do item (o `payload`). Eles voltam ao pool
  **zerados** (`reset`); um slice devolvido cheio prende buffers que já voltaram
  ao pool de envelope, e o lote seguinte carrega o corpo de outro cliente. É a
  mesma classe de bug da posse de buffer, num lugar novo.
- **Classe `42` do SQLSTATE é fatal, nunca `logic`.** Tabela dropada, coluna
  sumida ou índice único ausente classificados como `logic` descartariam um lote
  a cada 5 ms, com log de erro, para sempre, num processo que parece saudável. A
  tabela inteira está em `SPEC.md` §8.5.7 e no `TestClassifySQLState`; mude os
  dois juntos.
- **O `sink=postgres` nunca faz DDL.** Nem `CREATE TABLE IF NOT EXISTS`: N
  instâncias subindo juntas correm no catálogo, e o papel de produção não tem
  o privilégio. A fila é `docker/postgres/inbox.sql` (DDL literal da skill, um
  teste confere o hash); o servidor valida e sai com 3. Vale igual para
  `sink=mysql` (`docker/mysql/inbox.sql`) e para o stream de `sink=nats`
  (`docker/nats/stream.json`).
- **`ON DUPLICATE KEY UPDATE` dispara em QUALQUER chave única.** Um índice
  único extra na `inbox` do MySQL engolia eventos novos como duplicatas, com o
  `Write` devolvendo sucesso; o boot o recusa (`SPEC.md` §8.6.4). No PostgreSQL
  o mesmo índice derruba o lote com `23505`, e o boot também o recusa (§8.5.5).
- **`ON DUPLICATE KEY UPDATE` exige `UPDATE`, não só `INSERT`.** A sonda de
  boot do `sink=mysql` foi o que descobriu: um usuário só com `INSERT` passa
  pelo catálogo e falha no primeiro lote com `1142`. Por isso a sonda usa o
  mesmo comando do dreno, numa transação revertida, e o esquema documenta
  `GRANT INSERT, UPDATE (uuid)`. E nunca `INSERT IGNORE`: ele trunca dados em
  silêncio (`SPEC.md` §8.6.3).
- **Três opções do `nats.go` que não são as padrões, e o porquê**
  (`SPEC.md` §8.7.2): `WithRetryAttempts(0)`, porque a retentativa interna da
  biblioteca relê `msg.Data` depois de o `Write` ter retornado — a corrida de
  posse de buffer de §12.3 num lugar novo; `ReconnectBufSize(-1)`, porque o
  ring é o único buffer e uma fila escondida no cliente escaparia do `429` e do
  dump; `WithPublishAsyncTimeout`, porque um futuro sem resposta seguraria a
  mensagem e o buffer para sempre. Se você "simplificar" qualquer uma, os
  testes não reclamam e o bug é silencioso.
- **`sink=nats`: entrega parcial, veredito e contrato em toda checagem.**
  Uma recusa `logic` de **uma** mensagem (`ErrMaxPayload`, `10054`, um envelope
  que não mapeia) não encerra o lote: `Error.Delivered` diz o que o stream
  guardou, e os workers contam como perdidas só as recusadas. Permissão negada
  só chega pelo *callback* assíncrono: o sink guarda um veredito que o `Health`
  e todo `Write` devolvem como fatal. O contrato do stream (`no_ack`, plano de
  controle, subjects, tamanhos) vale em toda checagem; não volte a guardar o
  `validate` atrás do `validated`, que só controla o log.
- **`ring.Commit` antes do `Release`**, também na recusa `logic`: o ring
  desconta `len(Env)`, que o `Release` zera.
- **O dump só está salvo no nome final** (temporário oculto e `link`; sem
  *hard link*, `rename` depois de conferir o nome livre). O `fsync` do
  diretório é melhor esforço; só arquivos regulares são dumps.
- **O ring é selado antes do último `Drain` do shutdown** (`Seal`, uma
  barreira contra o `Enqueue`): um upload que terminasse depois recebia `200`
  num ring que ninguém mais lia. Não tire o `RWMutex` do `Enqueue`. O teste
  completa o upload **dentro** do `Drain` (`App.wrapRing`): é o que prova a
  ordem.
- **Parsers de INI e de flags nomeiam a linha ou a flag, nunca o valor**: um
  argumento com forma de flag nunca é valor de outra flag e só é nomeado se for
  uma flag deste servidor, e nada depois de um valor é citado.
- **`drain_timeout` é um prazo para as fases 3 e 4 juntas.**
- **`SIGHUP` é capturado do início ao fim**: o `Main` o ignora antes de tudo, e
  o `run` o observa do boot ao retorno (recarrega só servindo; fora disso,
  `WARN`). A saída do observador é `signal.Ignore`, nunca `signal.Stop`, que
  devolve o sinal à ação padrão do Go — e ela mata o processo no meio do dump.
- **Escrever o dump é do ring de memória; lê-lo é de todo boot.** O
  `emergency.Store` existe em todo `ring.mode`; só `App.dumps` decide a
  escrita. Com `sink=none` nada é reingerido: entregar o dump ao descarte e
  removê-lo destruiria eventos de uma vida com destino. Os gauges `whs_emergency_*` são recontados do disco no fim da
  reingestão (o arquivo ilegível conta) e o dump os soma; nunca `Set(0)` nem
  `Set(1)`.
- **O `ring.db` também é de todo boot.** Com `ring=memory` ou `none`, o boot
  sonda `ring.sqlite.path` sem criar arquivo nem tabela (`ring.ProbeSQLite`,
  nunca `NewSQLite`: o `events.db` pode estar nesse caminho), entrega a
  `jlh_ring` e remove o arquivo, o banco por último — só se ele não tem outras
  tabelas: a primeira versão da correção apagava assim o `events.db` com os
  dois caminhos iguais.
  Com `sink=none` as linhas de uma vida anterior ficam; no `ring=sqlite`, fora
  da conta do ring (`KeepBacklog`, pedido pela `App` — o ring nunca olha o
  sink). O *writer* aplica os `DELETE` enfileirados antes de sair.
- **URL de `sink.nats.urls` nunca ecoada**: nem no erro de validação, nem no log
  de conexão (`connectedURL`), nem no "sink ready" e no dump (parte de usuário
  mascarada).
- **`max_payload` do NATS e `max_allowed_packet` do MySQL são checados no
  boot**, contra o maior envelope (`envelope.MaxEventBytes`, `SPEC.md` §3.2:
  o corpo em base64 mais cabeçalhos e URI escapados), e recusam com 3. Sem a
  checagem, um corpo aceito com `200` seria descartado como `logic` na entrega.

- **O tipo `JSON` do ClickHouse descarta objeto vazio.** `params` é sempre
  `{}` e sumia de toda linha: a `inbox` do ClickHouse tipa `params` e `query`
  como `Map` (`SPEC.md` §3.9), e o boot recusa a tabela sem os dois *hints*. O
  *hint* é do formato do payload webhook (W e F podem dividir a `inbox`);
  por isso a tabela é por formato de payload, e o `go-syslog-server` tem a sua.
- **O tipo `JSON` do ClickHouse infere tipos de strings.** Por padrão uma
  string que parece data vira `Date`/`DateTime64`, e a variante passa a ler as
  strings das OUTRAS linhas do mesmo caminho no bloco (`""` virava
  `1970-01-01`). Todo `INSERT` desliga a inferência
  (`input_format_try_infer_dates/datetimes`,
  `input_format_json_try_infer_numbers_from_strings`); se você "limpar" esses
  ajustes, só o `TestClickHouseStringsAreNeverInferred` reclama.
- **`ORDER BY uuid` no ClickHouse não ordena por tempo**: `UUID` ordena pela
  segunda metade. A chave é `(event_at, uuid)`, e o boot exige `uuid` como
  elemento dela — uma chave sem `uuid` colapsaria eventos diferentes no merge.
- **O `insert_deduplication_token` sai do conteúdo do lote**, nunca de um
  contador: um token repetido para dados diferentes faz o servidor descartar o
  segundo lote em silêncio (§8.8.4). E o ClickHouse não tem transação: a sonda
  de boot é um `INSERT` **vazio**, que confere grant e `readonly` sem deixar
  linha (§8.8.5).
- **O `clickhouse-go` converte no cliente.** Um tipo de coluna diferente do
  esperado (`Int64` para um `[]uint64`) é recusado dentro do `Append`, antes
  de qualquer envio: por isso o boot exige os tipos exatos, e o erro do
  conversor é fatal. E `241` (memória) é fatal: o boot compara
  `max_memory_usage` com o pior lote.

## 10. Estado do projeto

O código está completo e cumpre a spec; a engenharia em volta dele não está
pronta. Antes de qualquer deploy real faltam, em ordem: versionamento (nível
acima), integração contínua com Redis, PostgreSQL, MySQL, NATS **e ClickHouse** de serviço,
calibragem dos fusíveis de saúde do sink, e a aferição dos critérios de aceite
de `SPEC.md` §15.5, que **nunca foram medidos** — e para `sink=postgres`,
`sink=mysql` e `sink=nats` nem sequer existem até a primeira medição
(`SPEC.md` §15.5, `TESTING.md` §8 itens 15 e 16). O limite de memória do Go nos
arquivos de implantação foi fechado em 2026-09-14 (`docker/common.yml`,
`docs/SPEC.md` §12.4). `sink=mysql` e `sink=nats` entraram em 2026-09-15 com
suítes de integração próprias (`TESTING.md` §5.3 e §5.4); `sink=clickhouse`,
em 2026-10-07 (§5.5), também sem meta de vazão até a primeira medição.

Ao propor trabalho novo, prefira fechar esses itens a acrescentar recurso. O
roteiro pós-v1 está em `SPEC.md` §18 e nada dele entra antes de a v1 estar em
produção.
