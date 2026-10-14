# AGENTS.md — go-syslog-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-syslog-server/               <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-syslog-server
    cmd/go-syslog-server/
    internal/
    docs/DECISOES.md              a entrevista de 2026-10-05 que definiu este software
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, 23 compose <ring>-<sink>, conf/syslog.ini (chaves recarregáveis)
  go-syslog-standard-server/      <- o antigo go-syslog-server: REFERÊNCIA, nunca dependência
  go-relplog-server/              <- RELP, só especificação (software futuro)
  go-webhook-server/, go-webhook-fast-server/
```

Este é o servidor syslog do ecossistema **LogHub**, escrito do zero em
2026-10-05. Recebe UDP, TCP, TLS e unix datagram, converte cada mensagem num
envelope **JLH** com `type=syslog` e entrega a um destino configurável (padrão:
NATS JetStream em `loghub.{space}.syslog`). É um Bridge-IN: `step = 0`.

## 2. As fontes normativas mandam, e a SPEC diz onde não

- Skills do servidor MCP `loghub-mcp`: `loghub-json-schema` (envelope),
  `loghub-type-syslog-schema` (payload e algoritmo de conversão),
  `loghub-postgres-table-inbox` (`sink=postgres`), `go-loghub-uuidv7` e
  `go-loghub-ident`. Cópias dos schemas e dos vetores
  dourados ficam em `internal/jlhcheck/loghub/`.
- `docs/SPEC.md` §1.3 lista os **desvios deliberados** (D1 moldura por conexão,
  D2 TAG estrita sem timestamp, D3 schema só em teste, D4 nível de UUID fixo,
  D5 sem importação, D6 espaço em branco nos campos `^\S+$`, D7 avisos de
  encoding de SD com texto colado, D8 anos fora de 0001–9999). Fora deles,
  divergir de uma skill é **bug**.
- `docs/SPEC.md` está congelada: discrepância entre código e spec é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite a spec por seção (`§6.4.3`).
- `docs/TESTING.md` registra as lacunas com honestidade. Fechou uma, atualize;
  achou outra, acrescente.

## 3. A standard é a referência, não a dependência

`../go-syslog-standard-server/` já resolveu, testou e corrigiu quase tudo o que
não é LogHub: moldura, rings, sinks, posse de buffer, saúde, shutdown, registry.
**Leia o código dele antes de escrever o seu**, e copie com adaptação, trazendo os
comentários: eles são o registro de por que cada decisão foi tomada. **Nada é
importado de lá**, e nada é importado de nenhum outro módulo do monorepo.

## 4. Fronteira de responsabilidade

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| Outras pastas do monorepo | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto** | o nível do monorepo, e só com pedido explícito do usuário |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-syslog-server/vX.Y.Z`.

## 5. O modelo mental

### Dois eixos ortogonais

- **RING**: onde a mensagem espera. `memory` | `sqlite` | `redis` | `none`
- **SINK**: para onde o JLH vai. `nats` | `redis` | `sqlite` | `postgres` | `mysql` | `clickhouse` | `none`

Nenhuma implementação de ring consulta o tipo do sink, e vice-versa.

### Cru no ring, conversão no dreno (SPEC §2.2)

A recepção só faz moldura, truncamento, **uma** amostra de relógio, PRI, filtro,
rate-limit e o `uuid`. O item no ring é **cabeçalho + cru**, sem JLH. A conversão
completa e a serialização rodam no worker de dreno (ou na recepção com
`ring=none`).

Consequência que você vai encontrar por todo lado: **o JLH é função pura de
cabeçalho + cru + config de conversão** (SPEC §3.5). Por isso toda chave de
`[syslog]` exige restart, a referência da inferência de ano é a amostra do
cabeçalho (nunca `time.Now()` na conversão), e um retry reenvia os mesmos bytes.

### O que o transporte confirma

| Transporte | Confirmação | Ring cheio |
|---|---|---|
| TCP, TLS | continuar lendo | parar de ler; a janela TCP segura o emissor |
| unix dgram | não existe | descarte, mas o kernel já segurou o emissor |
| UDP | não existe | **descarte contado** |

Não invente confirmação onde o protocolo não tem.

## 6. Mapa do código

```
cmd/go-syslog-server/main.go   flags que encerram, lhident.Initialize(), internal/app
internal/
  app/        montagem, boot, shutdown, SIGHUP, reingestão
  config/     registry único -> INI + flags + env + --help + dump; LOGHUB_EB_URL
  ident/      Identity congelada e gramática de space              [lógica pura]
  jlh/        escritor JSON por append, Event, envelope, UUID       [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH                  [lógica pura, fuzz]
  item/       Item, codec do cabeçalho, pools                       [lógica pura, fuzz]
  rediscli/   cliente e classificação de erros comuns aos dois Redis
  jlhcheck/   oráculo de teste: schemas + regras semânticas da skill [só em teste]
  receiver/   udp (pktinfo), unix, tcp, tls, moldura, proxy, ingest [fuzz]
  filter/, ratelimit/                                               [lógica pura]
  ring/       memory, sqlite, redis, none
  sink/       nats, redis, sqlite, postgres, mysql, clickhouse, none, workers, health
  control/    heartbeat beacon
  emergency/  dump e reingestão                                     [fuzz]
  obs/        métricas Prometheus próprias + slog
```

## 7. Comandos

Todos a partir desta pasta.

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**.

Vetores dourados e contratos dos schemas:

```bash
go test -race -run 'Golden|Schema|Corpus' -v ./internal/convert/
```

Fuzz, obrigatório antes de release (10 min cada; 60 s no dia a dia):

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$     -fuzztime=60s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$          -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$       -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrameOracle$ -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$       -fuzztime=60s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$        -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$        -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$   -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=60s
```

O `^…$` importa: `-fuzz=FuzzFrame` casa também `FuzzFrameOracle`, e o Go recusa
dois alvos.

Destinos reais (as suítes pulam sem a variável):

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &   # sem JetStream
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 SYSLOG_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 600s ./internal/sink/ ./internal/control/ ./internal/app/

docker run --rm -d -p 16379:6379 --name sls-test-redis redis:7-alpine
SYSLOG_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race -timeout 600s ./internal/sink/ ./internal/ring/ ./internal/app/
```

PostgreSQL, MySQL e ClickHouse: `docs/TESTING.md` (§6.2–§6.4a). Um segundo Redis
com `allkeys-lru` (`SYSLOG_TEST_REDIS_LRU_ADDR`, §6.1) e um NATS com usuários de
permissões limitadas (`SYSLOG_TEST_NATS_PERM_URL`, §6.4) provam as recusas.

Executar localmente (um `DATADIR` relativo é resolvido a partir do diretório de
trabalho antes do `Initialize()`, SPEC §13.1 passo 2; o hostname do macOS não
pode ter `_`, ou o `go-loghub-ident` sai com um código entre 100 e 114):

```bash
mkdir -p /tmp/sls-data
DATADIR=/tmp/sls-data AGENT_NAME=go-syslog-server HOSTNAME=dev-mac \
  go run ./cmd/go-syslog-server --sink.type=sqlite --listen.udp=127.0.0.1:1514 \
  --listen.tcp=127.0.0.1:1514 --admin.bind=127.0.0.1:9102
printf '<13>1 - - app - - - teste' | nc -u -w1 127.0.0.1 1514   # o logger do macOS não tem -n
```

## 8. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**. As únicas strings em português no código são os textos
  normativos de `parse.error` (SPEC §6.6), que são dado.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- **Dependências: oito em produção** (SPEC §17.3); a oitava, `clickhouse-go`,
  foi escolha do usuário contra o HTTP da stdlib e traz uma dúzia de
  transitivas. `jsonschema/v6` só em teste.
  Nada de `golang.org/x/net` (PKTINFO é `syscall`) nem `golang.org/x/text`
  (charset é tabela própria). Dependência nova exige justificativa na spec.
- **Config:** o registry é a fonte única. Um parâmetro novo entra só em
  `internal/config/registry.go`.
- **Métricas:** cardinalidade fixa; nenhum label vindo do emissor.
- **Identidade:** `lhident.Initialize()` só no `main`, uma vez. Pacotes recebem
  `ident.Identity`; testes que precisam da biblioteca usam `TestMain`.
- **Escrita em disco:** tudo sob `server.data_dir` (`DATADIR`), inclusive a
  identidade. Exceção única: o socket `listen.unix`.

## 9. Armadilhas que já custaram bugs reais (herdadas da standard e do irmão)

- **Posse de buffer.** O item é dono do cru; volta ao pool **uma vez**, após a
  confirmação. Num retry ainda é o dono. Erro devolvido não significa "ninguém
  mais lê": na dúvida, `Discard`/`Taint` e deixe para o GC.
- **Comandos do rueidis são reciclados.** Retry reconstrói.
- **Lote retido não está no ring.** O shutdown coleta `Workers.Pending()`; o
  `hold` faz `append`, nunca atribuição.
- **Chave primária no `ring=sqlite`.** O `uuid` tem coluna própria; o leitor
  anda por cursor, nunca por conjunto em voo.
- **Fronteira de histograma como string** pré-formatada na inicialização.
- **Slices do pool do `unnest`** voltam zerados.
- **Classe `42` é fatal, nunca `logic`.** Erro desconhecido é `network`.
- **Nunca DDL** em postgres/mysql; nunca criar stream NATS.
- **`sink=postgres` é a fila `loghub.inbox` da skill** (SPEC §3.7.1): DDL
  literal em `docker/postgres/inbox.sql` (um teste confere o hash), chave só
  `uuid`, PostgreSQL 18. **`jsonb` recusa `\u0000`** (`22P05`, classe 22, o
  pedaço inteiro descartado): o payload com NUL vai como cópia reconstruída
  (`raw` em base64, U+FFFD nos outros textos); o JLH do pedaço nunca é tocado.
- **`ON DUPLICATE KEY UPDATE` exige `UPDATE`**; nunca `INSERT IGNORE`.
- **`sink=clickhouse` tem tabela própria, `inbox_syslog`** (SPEC §3.7.2): o tipo
  `JSON` descarta objeto vazio, então `structured-data` é tipado como `Map`, e
  esse *hint* é do `type=syslog` — a `inbox` dos webhooks tem outro. Uma linha
  sem SD volta com `"structured-data":{}`, que este servidor nunca escreve:
  `{}` é ausente. O boot recusa a tabela sem o *hint*, com motor que não seja
  `*ReplacingMergeTree`, sem `uuid` na chave (o ClickHouse ordena `UUID` pela
  segunda metade: a chave é `(event_at, uuid)`), com CHECK, ou com
  `max_memory_usage` abaixo do pior pedaço (`241` é fatal).
- **O tipo `JSON` do ClickHouse infere tipos de strings.** Por padrão uma
  string que parece data vira `Date`/`DateTime64`, e a variante passa a ler as
  strings das OUTRAS linhas do mesmo caminho no bloco (`""` virava
  `1970-01-01`). Todo `INSERT` desliga a inferência
  (`input_format_try_infer_dates/datetimes`,
  `input_format_json_try_infer_numbers_from_strings`); se você "limpar" esses
  ajustes, só o `TestClickHouseStringsAreNeverInferred` reclama.
- **O `insert_deduplication_token` sai do conteúdo do pedaço**, nunca de um
  contador: token repetido para dados diferentes descarta o segundo pedaço em
  silêncio. Sem transação no ClickHouse, a sonda de boot é um `INSERT` vazio.
- **`nats.go`:** `WithRetryAttempts(0)`, `ReconnectBufSize(-1)`,
  `WithPublishAsyncTimeout`. Se você "simplificar", os testes não reclamam.
- **O JLH vai em `Data`, nunca num header NATS** (teto de 64 KiB de headers).
- **Nunca alocar pelo que o par declara** (MSG-LEN, PROXY v2): alocar pelo que
  chegou, com teto.
- **Reavaliar a moldura a cada frame embaralha o fluxo** (D1).
- **O `\n`, o NUL e os `CR` da moldura não são da mensagem.**
- **UDP sem `SO_RCVBUF` grande perde em silêncio**; o efetivo é lido de volta.
- **A hora da mensagem não é tempo.** O tempo é a amostra da recepção (`event`).
- **Duas leituras de relógio** fazem o fato parecer posterior ao envelope: uma
  amostra só, para `uuid` e `event`.
- **`time.RFC3339Nano` remove zeros**; `strings.ToValidUTF8` junta octetos
  inválidos; `time.Date` normaliza 30/fev; `json.Marshal` troca UTF-8 inválido em
  silêncio. A SPEC §6 diz o que usar.
- **SIGHUP** compara chave recarregável com o que está em execução e chave de
  restart com o boot.
- **Durações zero** são recusadas onde zero não significa "sem limite".
- **Testes portados que passam vazios:** um helper que devolve zero quando a
  chave falta faz o teste passar sem provar nada. O mesmo vale para casos de
  recusa: sem um caso de controle válido, uma regra nova pode recusar todos pelo
  motivo errado (foi o que a conferência `uuid` × amostra fez com
  `TestHeadRefusals`).
- **O `\s` do motor de regex não é o do schema.** O `^\S+$` do schema é
  ECMAScript; o RE2 do `jsonschema/v6` deixa passar U+00A0, U+2028, U+FEFF e
  `\v`, e o Python da skill recusa ainda U+001C–U+001F e U+0085. O oráculo
  (`jlhcheck`) confere o conjunto completo na etapa semântica.
- **Escrita no boot é por chunk.** Um retry em volta de `Instrumented.Write`
  reconverte, e um erro `logic` no meio do lote deixava os chunks seguintes sem
  escrita e confirmados. Use `Instrumented.Deliver`.
- **Um "a portar" de um irmão é candidato, não patch.** O R é RELP: o `ctx`
  conferido antes do `Enqueue` evita duplicata lá e **perderia** mensagens TCP
  já lidas aqui; a sonda MySQL do R não tem `unix`, a deste servidor tem; e o
  "sem cabeçalho PROXY e fechando, fecha sem contar" do R descartava aqui as
  linhas que um par `optional` já tinha mandado (só a **espera** pelo cabeçalho
  é do shutdown, §4.6). Confira no S antes de copiar (CHANGELOG, 2026-10-13).
- **Valores aceitos pela config precisam significar algo para quem os usa.** Um
  enum aceito pelo registry e desconhecido no pacote consumidor desliga o recurso
  em silêncio (aliases de facility; `windows-1252`).

## 10. Estado do projeto

Ver `docs/TESTING.md` §8 (lacunas), a ordem de entrega em `docs/DECISOES.md`
§5 e as pendências do código atual em `docs/SPEC.md` §22.1 (corrigida uma, a
regra entra na seção indicada e o item sai de lá).
