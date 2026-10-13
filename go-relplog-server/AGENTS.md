# AGENTS.md — go-relplog-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-relplog-server/              <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-relplog-server
    cmd/go-relplog-server/
    internal/
    docs/DECISOES.md              as decisões de 2026-10-11 que definiram este software
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, compose <ring>-<sink>, conf/relplog.ini, emissor rsyslog de demonstração
  go-syslog-server/               <- o irmão (S): PAI do código, nunca dependência
  go-syslog-standard-server/      <- a standard: o RELP de referência (receiver/relp*.go)
  go-syslog-pg-server/, go-syslog-nats-server/, go-webhook-*/
```

Este é o servidor **RELP** do ecossistema **LogHub**, criado em 2026-10-11 a
partir do `go-syslog-server`. Recebe as mensagens que o `rsyslog` envia com o
`omrelp` (em claro ou sobre TLS), confirma **cada uma** só depois da garantia do
ring, converte cada uma num envelope **JLH** com `type=syslog` (com
`x-transport: "relp"`) e entrega a um destino configurável (padrão: NATS
JetStream em `loghub.{space}.syslog`, o mesmo do S). É um Bridge-IN: `step = 0`.

## 2. As fontes normativas mandam, e a SPEC diz onde não

- Skills do servidor MCP `loghub-mcp`: `loghub-json-schema` (envelope),
  `loghub-type-syslog-schema` (payload e algoritmo de conversão),
  `loghub-postgres-table-inbox` (`sink=postgres`), `go-loghub-uuidv7` e
  `go-loghub-ident`. Cópias dos schemas e dos vetores dourados ficam em
  `internal/jlhcheck/loghub/`.
- O **comportamento do cliente RELP** é o do `librelp` (`rsyslog/librelp`,
  `src/`). SPEC §5.4 diz o que cada resposta significa para ele e cita o arquivo
  de onde isso vem; antes de mudar uma resposta, releia o código do cliente.
- `docs/SPEC.md` §1.3 lista os **desvios deliberados** (D2–D9; D1 não se aplica
  ao RELP). Fora deles, divergir de uma skill é **bug**.
- `docs/SPEC.md` está congelada: discrepância entre código e spec é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite a spec por seção (`§5.7`).
- `docs/TESTING.md` registra as lacunas com honestidade. Fechou uma, atualize;
  achou outra, acrescente.

## 3. O S é o pai, não a dependência

`../go-syslog-server/` é de onde vieram a conversão, os rings, os sinks, o
beacon, o dump, a config e a app; `../go-syslog-standard-server/` é de onde veio
o RELP. **Nada é importado de lá**, e nada é importado de nenhum outro módulo do
monorepo. As pendências do S (SPEC dele, §22.1) foram corrigidas aqui; uma
correção nova num dos dois lados **vale para o outro** — confira o irmão e
registre no `CHANGELOG.md` da raiz o que ficou "a portar".

## 4. Fronteira de responsabilidade

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| Outras pastas do monorepo | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto** | o nível do monorepo, e só com pedido explícito do usuário |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-relplog-server/vX.Y.Z`.

## 5. O modelo mental

### Dois eixos ortogonais

- **RING**: onde a mensagem espera. `memory` | `sqlite` | `redis` | `none`
- **SINK**: para onde o JLH vai. `nats` | `redis` | `sqlite` | `postgres` | `mysql` | `clickhouse` | `none`

Nenhuma implementação de ring consulta o tipo do sink, e vice-versa.

### Cru no ring, conversão no dreno (SPEC §2.2)

A recepção só faz moldura RELP, truncamento, **uma** amostra de relógio, PRI,
filtro, rate-limit e o `uuid`. O item no ring é **cabeçalho + cru**, sem JLH. A
conversão completa e a serialização rodam no worker de dreno (ou na transação
com `ring=none`). **O JLH é função pura de cabeçalho + cru + config de
conversão** (SPEC §3.5): toda chave de `[syslog]` exige restart, e um retry
reenvia os mesmos bytes.

### O que cada resposta RELP significa (SPEC §5.4) — a regra que mais importa

| Resposta | O `librelp` | Use para |
|---|---|---|
| `200 OK` | apaga da fila: entregue | garantia do ring cumprida; filtrada; vazia |
| `500 ...` | **destrói a mensagem e não retransmite**, e quebra a sessão (as vizinhas da janela voltam duplicadas) | só recusa **final**, contada e rara: `logic` do destino com `ring=none`, a guarda de origem não endereçável e os erros de protocolo do cliente (`syslog` antes do `open`, segundo `open`, comando desconhecido) |
| nenhuma + sessão fechada (`serverclose` ou socket) | reconecta e **retransmite** o não confirmado | ring cheio/indisponível, sink DOWN ou rate-limit além de `relp.backpressure_timeout`; shutdown |

Nunca responda `500` ao que vai passar sozinho: é perda na origem. Nunca feche um
socket com respostas decididas no buffer: é duplicata.

### A sessão (SPEC §5.5–§5.8)

Um leitor por conexão; até `relp.window` transações em voo, cada uma numa
goroutine; um escritor que só vê transações decididas; um teto **global** de
bytes das cópias em voo (`relp.inflight_max_bytes`), adquirido antes da cópia.

## 6. Mapa do código

```
cmd/go-relplog-server/main.go   flags que encerram, lhident.Initialize(), internal/app
internal/
  app/        montagem, boot, shutdown, SIGHUP, reingestão; XTransport
  config/     registry único -> INI + flags + env + --help + dump; LOGHUB_EB_URL
  ident/      Identity congelada e gramática de space              [lógica pura]
  jlh/        escritor JSON por append, Event, envelope, UUID       [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH (+ x-transport)  [lógica pura, fuzz]
  item/       Item, codec do cabeçalho, pools                       [lógica pura, fuzz]
  rediscli/   cliente e classificação de erros comuns aos dois Redis
  jlhcheck/   oráculo de teste: schemas + regras semânticas da skill [só em teste]
  receiver/   RELP: relpframe (moldura), relp (sessão, janela,
              respostas), budget (teto em voo), stream (conexões, PROXY,
              TLS, shutdown), tcp, tls, proxy, limits, ingest       [fuzz]
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

Fuzz, obrigatório antes de release (10 min cada; 60 s no dia a dia):

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=60s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=60s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$         -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=60s
```

Destinos reais e o rsyslog de verdade: `docs/TESTING.md` §7. As variáveis são
`RELPLOG_TEST_REDIS_ADDR`, `RELPLOG_TEST_PG_DSN`, `RELPLOG_TEST_MYSQL_DSN`,
`RELPLOG_TEST_NATS_URL`, `RELPLOG_TEST_NATS_NOJS_URL`,
`RELPLOG_TEST_NATS_PERM_URL` (permissões negadas), `RELPLOG_TEST_REDIS_LRU_ADDR`
(Redis com `allkeys-lru`), `RELPLOG_TEST_CLICKHOUSE_DSN` e
`RELPLOG_TEST_EMITTERS=1` (Docker).

Executar localmente (o hostname do macOS não pode ter `_`, ou o
`go-loghub-ident` sai com um código entre 100 e 114):

```bash
mkdir -p /tmp/rls-data
DATADIR=/tmp/rls-data AGENT_NAME=go-relplog-server HOSTNAME=dev-mac \
  go run ./cmd/go-relplog-server --sink.type=sqlite --listen.tcp=127.0.0.1:20514 \
  --admin.bind=127.0.0.1:9103
```

e, num rsyslog: `action(type="omrelp" target="127.0.0.1" port="20514")`.

## 8. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**. As únicas strings em português no código são os textos
  normativos de `parse.error` (SPEC §6.6), que são dado.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- **Dependências: oito em produção** (SPEC §17.3), as mesmas do S. O RELP não
  tem biblioteca: é stdlib. Dependência nova exige justificativa na spec.
- **Config:** o registry é a fonte única. Um parâmetro novo entra só em
  `internal/config/registry.go`. Env `RELPLOG_<SEÇÃO>_<CHAVE>`; `[syslog]` mantém
  `SYSLOG_<CHAVE>` (o nome da skill, igual ao S).
- **Métricas:** prefixo `relplog_`, cardinalidade fixa; nenhum label vindo do
  emissor (o comando RELP desconhecido é `unknown`, nunca a palavra do cliente).
- **Identidade:** `lhident.Initialize()` só no `main`, uma vez. Pacotes recebem
  `ident.Identity`; testes que precisam da biblioteca usam `TestMain`.
- **Escrita em disco:** tudo sob `server.data_dir` (`DATADIR`), sem exceção.

## 9. Armadilhas que já custaram bugs reais

### Do RELP (standard, especificação extraída e a leitura do `librelp`)

- **`500` é perda na origem.** O `librelp` destrói a mensagem que recebe
  qualquer código diferente de `200` (`scrsp.c`). Ring cheio ou indisponível,
  sink DOWN, rate-limit e shutdown **seguram ou fecham a sessão**; nunca `500`.
- **O `close` do socket descarta respostas decididas.** O `Close` do transporte
  só vence o prazo de **leitura**; o escritor esvazia, e só então o socket fecha.
- **`DATALEN = 0` seguido de espaço é frame inválido para o `librelp`.** O
  servidor escreve `0 serverclose 0\n`, nunca `0 serverclose 0 \n`; o leitor
  aceita as duas grafias porque o parque manda as duas.
- **O leitor arma só o prazo de leitura** (`armReadDeadline`); o escritor é o dono
  do prazo de escrita. Mexer no prazo de escrita a partir do leitor corta uma
  escrita de respostas no meio.
- **A cópia da transação vem depois da vaga na janela e no teto em voo.** Copiar
  antes chega a `window + 1` cópias por conexão, fora de todo teto.
- **Nunca alocar pelo `DATALEN` declarado**; o buffer cresce pelos bytes que
  chegam e é solto depois de um frame grande.
- **Uma transação de um par que sumiu é abandonada**, nunca enfileirada depois:
  a retransmissão a traria de novo com outro `uuid`.
- **O `close` do cliente espera as transações em voo** antes da resposta dele.
- **Testes de janela correm**: as transações de uma janela entram no ring em
  qualquer ordem. Libere pelo conteúdo, nunca pela posição.
- **O fim da sessão é conferido até o `Enqueue`.** O ring de memória nunca olha
  o contexto: uma transação montada depois do fim da sessão entraria no ring
  pelas costas da retransmissão do `librelp` (duplicata). Num teste que encerra
  a sessão, espere o ring antes de esperar o `200`.
- **O buffer grande do leitor só é solto com nada no `bufio.Reader`.** Solto
  antes de todo frame, um emissor de linhas de 24 KiB realocava o buffer inteiro
  por frame (`BenchmarkRELPReaderFrames`).

### Herdadas do S

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
- **A sonda de saúde SQL tem conexão própria, fora do pool.** No pool, escritas
  lentas a deixavam sem conexão e um banco sadio ia a FATAL; o pool é dos
  escritores (`pool_size` deriva `sink.workers`). A posse da contagem do aborto
  passa toda por `lossMu` (`enterDumpPhase`, `oweDump`, `settleDump`): um
  `Swap` fora dele reabre a janela do motivo errado.
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
- **Nunca alocar pelo que o par declara** (`DATALEN`, PROXY v2): alocar pelo que
  chegou, com teto.
- **O cabeçalho e o `\n` final do frame RELP não são da mensagem**; um `\n` dentro de `DATA` é.
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
- **Valores aceitos pela config precisam significar algo para quem os usa.** Um
  enum aceito pelo registry e desconhecido no pacote consumidor desliga o recurso
  em silêncio (aliases de facility; `windows-1252`).

## 10. Estado do projeto

Ver `docs/TESTING.md` §10 (lacunas), a ordem de entrega em `docs/DECISOES.md`
§4 e as pendências do código atual em `docs/SPEC.md` §22.1 (corrigida uma, a
regra entra na seção indicada e o item sai de lá).
