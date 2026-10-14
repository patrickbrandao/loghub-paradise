# AGENTS.md — go-relplog-pg-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-relplog-pg-server/           <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server
    cmd/go-relplog-pg-server/
    internal/
    docs/DECISOES.md              as decisões do corte (2026-10-13) e as herdadas
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, docker-compose.yml (RELP -> memory -> PostgreSQL), rsyslog/, conf/relplog.ini
  go-relplog-server/              <- o PAI (R): este módulo é um corte dele. Referência, nunca dependência
  go-syslog-pg-server/            <- o MOLDE do corte (SP). Referência, nunca dependência
  go-relplog-nats-server/         <- o IRMÃO (RN): o mesmo corte, com destino NATS
  go-syslog-server/, go-syslog-nats-server/, go-syslog-standard-server/, go-webhook-*/
```

Este é um servidor syslog por **RELP** do ecossistema **LogHub**, criado em
2026-10-13 como **corte** do `go-relplog-server`: recebe o `omrelp` do rsyslog
em claro, confirma cada mensagem com `200 OK` só depois de ela estar no ring,
converte cada uma num envelope **JLH** com `type=syslog` (e
`"x-transport": "relp"`) e o **enfileira** na `loghub.inbox` do PostgreSQL. Ring
só em memória, emergência em JSON Lines, **sem TLS em lugar nenhum**, três
dependências de produção. É um Bridge-IN: `step = 0`.

## 2. As fontes normativas mandam, e a SPEC diz onde não

- Skills do servidor MCP `loghub-mcp`: `loghub-json-schema` (envelope),
  `loghub-type-syslog-schema` (payload e algoritmo de conversão),
  `loghub-postgres-table-inbox` (a fila), `go-loghub-uuidv7` e
  `go-loghub-ident`. Cópias dos schemas e dos vetores dourados ficam em
  `internal/jlhcheck/loghub/`.
- `docs/SPEC.md` §1.3 lista os **desvios deliberados** (D1 a D9; D9 é o
  `x-transport`). Fora deles, divergir de uma skill é **bug**.
- `docs/SPEC.md` está congelada: discrepância entre código e spec é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite a spec por seção (`§5.7`).
- **A numeração da SPEC é a do `go-relplog-server`.** Seção removida mantém o
  título com a nota "Removido no go-relplog-pg-server"; **nunca renumere**, o
  código cita os números. Seção nova entra no fim da seção-mãe, com número novo.
- `docs/TESTING.md` registra as lacunas com honestidade. Fechou uma, atualize;
  achou outra, acrescente.

## 3. Os pais são a referência, não a dependência

`../go-relplog-server/` tem o RELP de onde este saiu, com a leitura do
`librelp`, o histórico de bugs (TESTING §11 dele) e os comentários que explicam
cada decisão. `../go-syslog-pg-server/` tem o corte (ring, sink, dump JSON
Lines, `ParseLine`, `removed.go`). Para entender por que algo é assim, leia lá.
**Nada é importado de lá**, nem de nenhum outro módulo do monorepo. **Não edite
os pais nem o irmão** a partir daqui.

Os pacotes comuns com o `go-relplog-nats-server` (`item`, `ring`, `receiver`,
`convert`, `emergency`, `sink` workers/instrumented/health, boot e shutdown da
`app`, `obs`) diferem dele só onde o SP e o SN diferem. Uma correção feita aqui
num desses pacotes vale para o irmão e para os pais: registre-a como "a portar".

O que saiu (SPEC §1.2) não volta sem decisão do usuário registrada em
`docs/DECISOES.md`: TLS (listener e cliente), os outros sinks e rings, filtro,
rate-limit, beacon. O objetivo do módulo é ser enxuto.

## 4. Fronteira de responsabilidade

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| Outras pastas do monorepo (pais e irmão inclusive) | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto** | o nível do monorepo, e só com pedido explícito do usuário |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-relplog-pg-server/vX.Y.Z`.

## 5. O modelo mental

### Um ring, um sink

- **RING**: `memory`, e só. Não há chave `ring.mode`.
- **SINK**: `postgres`, a fila `loghub.inbox`, e só. Não há chave `sink.type`.

As interfaces `ring.Ring` e `sink.Sink` ficam, com uma implementação cada: são
a costura dos testes. O log de boot e o `relplog_config_info` dizem
`ring_mode="memory"` e `sink_type="postgres"`, por compatibilidade com os
painéis do pai.

### Cru no ring, conversão no dreno (SPEC §2.2)

A recepção só faz moldura, truncamento, **uma** amostra de relógio, o estado do
sink e do ring e o `uuid`. **Não lê a mensagem.** O item no ring é **cabeçalho +
cru**, sem JLH, e o cabeçalho só existe em memória (não há codec). A conversão
e a serialização rodam no worker de dreno. Consequência: **o JLH é função pura
de cabeçalho + cru + config de conversão** (SPEC §3.5).

### O que cada resposta RELP significa (SPEC §5.4) — a regra que mais importa

| Resposta | O `librelp` | Use para |
|---|---|---|
| `200 OK` | apaga da fila: entregue | a mensagem está no ring; a vazia |
| `500 ...` | **destrói a mensagem e não retransmite**, e quebra a sessão (as vizinhas da janela voltam duplicadas) | só a guarda de origem não endereçável e os erros de protocolo do cliente (`syslog` antes do `open`, segundo `open`, comando desconhecido) |
| nenhuma + sessão fechada (`serverclose` ou socket) | reconecta e **retransmite** o não confirmado | ring cheio ou sink DOWN além de `relp.backpressure_timeout`; shutdown |

Nunca responda `500` ao que vai passar sozinho: é perda na origem. Nunca feche um
socket com respostas decididas no buffer: é duplicata. Uma recusa `logic` da
fila acontece no worker, **depois** do `200`: é descarte contado
(`events_dropped{sink_logic}`), nunca um `500` (decisão RP-6).

### A sessão (SPEC §5.5–§5.8)

Um leitor por conexão; até `relp.window` transações em voo, cada uma numa
goroutine; um escritor que só vê transações decididas; um teto **global** de
bytes das cópias em voo (`relp.inflight_max_bytes`), adquirido antes da cópia.

### O dump é JLH pronto, e volta sem conversão (SPEC §12)

Em FATAL ou dreno estourado, o ring pendente é **convertido** e escrito como
JSON Lines, um JLH por linha, idêntico ao que o worker escreveria. No boot
seguinte cada linha vai **como está** para a fila; as colunas saem do cabeçalho
da própria linha. Essas mensagens já tiveram `200 OK`: o rsyslog não as tem
mais, e o dump é a única cópia.

## 6. Mapa do código

```
cmd/go-relplog-pg-server/main.go  flags que encerram, lhident.Initialize(), internal/app
internal/
  app/        montagem, boot, shutdown, SIGHUP, reingestão, admin; XTransport
  config/     registry único -> INI + flags + env + --help + dump; [relp];
              LOGHUB_EB_URL (só postgres://, regra do sslmode); removed.go (env do R -> WARN)
  ident/      Identity congelada e gramática de space              [lógica pura]
  jlh/        escritor JSON por append, Event, envelope, UUID, Payload [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH (+ x-transport)  [lógica pura, fuzz]
  item/       Item, Head, pools (sem codec)                         [lógica pura]
  jlhcheck/   oráculo de teste: schemas + regras semânticas da skill [só em teste]
  receiver/   RELP (relpframe, relp, budget), stream, tcp, proxy, limites, ingest [fuzz]
  ring/       interface + memory
  sink/       interface + postgres, workers, chunk, health, instrumented [fuzz]
  emergency/  dump JSON Lines, ParseLine, Count, reingestão         [fuzz]
  obs/        métricas Prometheus próprias + slog
```

## 7. Comandos

Todos a partir desta pasta.

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**. Sem `RELPLOG_TEST_PG_DSN` a integração pula: o
PostgreSQL é o único destino, então a prova ponta a ponta precisa dele.

```bash
docker run --rm -d -p 15491:5432 -e POSTGRES_USER=rlpg -e POSTGRES_PASSWORD=rlpg \
  -e POSTGRES_DB=rlpg_test --name rlpg-test-pg18 postgres:18-alpine
RELPLOG_TEST_PG_DSN='postgres://rlpg:rlpg@127.0.0.1:15491/rlpg_test?sslmode=disable' \
  go test -race -timeout 900s ./...
# com o rsyslog real (Docker; constrói rlpg-test-rsyslog:alpine-3.22)
RELPLOG_TEST_EMITTERS=1 RELPLOG_TEST_PG_DSN='...' \
  go test -race -run Emitters -v -timeout 900s ./internal/app/
```

Vetores dourados e contratos dos schemas:

```bash
go test -race -run 'Golden|Schema|Corpus|Semantic' -v ./internal/convert/ ./internal/jlhcheck/
```

Fuzz, obrigatório antes de release (10 min cada; 60 s no dia a dia):

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=60s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=60s
```

Não há `FuzzHead`: o codec do cabeçalho foi apagado.

Executar localmente (o hostname do macOS não pode ter `_`, ou o
`go-loghub-ident` sai com um código entre 100 e 114). A fila precisa existir
(`docker/postgres/inbox.sql`): o servidor nunca faz DDL.

```bash
mkdir -p /tmp/rlpg-data
DATADIR=/tmp/rlpg-data AGENT_NAME=go-relplog-pg-server HOSTNAME=dev-mac \
  LOGHUB_EB_URL='postgres://rlpg:rlpg@127.0.0.1:15491/rlpg_test' \
  go run ./cmd/go-relplog-pg-server --listen.tcp=127.0.0.1:20514 \
  --admin.bind=127.0.0.1:19380
```

## 8. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**. As únicas strings em português no código são os textos
  normativos de `parse.error` (SPEC §6.6), que são dado.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- **Dependências: três em produção** (SPEC §17.3): `pgx/v5`, `go-loghub-ident`,
  `go-loghub-uuidv7`. `jsonschema/v6` só em teste. Dependência nova exige
  justificativa na spec — e vai contra o objetivo do módulo.
- **Config:** o registry é a fonte única. Um parâmetro novo entra só em
  `internal/config/registry.go`. Prefixo `RELPLOG_` e nomes iguais aos do R
  para as chaves comuns (`SYSLOG_` em `[syslog]`); uma chave do R que não existe
  aqui vai para `removed.go` (aviso no env), e `TestRemovedEnvIsTheParentMinusThis`
  confere a lista.
- **Métricas:** prefixo `relplog_`, cardinalidade fixa; nenhum label vindo do
  emissor; série que nunca sobe não é exposta.
- **Identidade:** `lhident.Initialize()` só no `main`, uma vez. Pacotes recebem
  `ident.Identity`; testes que precisam da biblioteca rodam o binário.
- **Escrita em disco:** tudo sob `server.data_dir` (`DATADIR`), inclusive a
  identidade.

## 9. Armadilhas que já custaram bugs reais

### Do RELP (herdadas do R)

- **`500` é perda na origem.** O `librelp` destrói a mensagem que recebe
  qualquer código diferente de `200` (`scrsp.c`). Ring cheio, sink DOWN, um erro
  inesperado do ring e o shutdown **seguram ou fecham a sessão**; nunca `500`.
- **O `close` do socket descarta respostas decididas.** O `Close` do transporte
  só vence o prazo de **leitura**; o escritor esvazia, e só então o socket fecha
  (ou `closeGrace`, 6 s). Os transportes fecham ao mesmo tempo, não em fila.
- **`DATALEN = 0` seguido de espaço é frame inválido para o `librelp`.** O
  servidor escreve `0 serverclose 0\n`, nunca `0 serverclose 0 \n`.
- **O leitor arma só o prazo de leitura**; o escritor é o dono do prazo de
  escrita.
- **A cópia da transação vem depois da vaga na janela e no teto em voo**, e a
  conta é `2 × classe + 8 KiB` (RP-7: a do R, de propósito).
- **A pré-checagem do ring cheio vê os dois tetos** (`ring.FullFor(n)`, com o
  `n` truncado), e a transação confere o fim da sessão de novo logo antes do
  `Enqueue`: só o ring diz "decidida".
- **Nunca alocar pelo `DATALEN` declarado** (nem pelo PROXY v2, nem pela linha
  do dump): alocar pelo que chegou, com teto.
- **Uma transação de um par que sumiu é abandonada**, nunca enfileirada depois;
  e uma transação confere o fim da sessão depois de cada espera.
- **Todo frame `syslog` lido entra na conta**, inclusive o que nunca ganhou
  goroutine: `messages_total = accepted + dropped + relp_unanswered_total`.
- **Testes de janela correm**: libere pelo conteúdo, nunca pela posição.

### Do corte (herdadas do SP)

- **A linha do dump é reingerida SEM conversão.** `emergency.ParseLine` é a
  **única** porta entre um arquivo em disco e as colunas da fila. Uma linha que
  a fila recusasse seria `ClassLogic` do **pedaço inteiro**. Se você
  "simplificar" uma regra de SPEC §12.2, pergunte-se qual linha a fila passaria
  a recusar.
- **Sem TLS, de propósito.** Não "conserte" o `sslmode=disable`, o `TLSConfig`
  nulo nem a recusa de `sslmode=require` na `LOGHUB_EB_URL`: é decisão do
  usuário (RP-3). E não remova os *fallbacks* nulos.
- **O ambiente libpq não chega ao pool.** A config do pool é lida com **toda**
  variável `PG*` escondida (`parseWithoutLibpqEnv`, sob mutex). SPEC §10.5.
- **A validação do boot tem prazo próprio** (`sink.op_timeout`), não o da sonda
  de saúde; toda checagem de saúde carrega o seu (`withLifetime`).
- **Classes `42`, `28`, `3D`, o `23502` e o `0A000` são fatais**, nunca `logic`.
  Erro desconhecido é `network`. Uma regra `ON INSERT`/`ON UPDATE` na fila é
  recusada no boot (o `ON CONFLICT` não roda com ela).
- **`pool_size` ≥ `workers + 1`**: a sonda de saúde tira a conexão do mesmo
  pool.
- **Nunca DDL.** A fila vem de `docker/postgres/inbox.sql`, DDL literal da skill
  (um teste confere o hash: não edite o bloco), chave só `uuid`.
- **`jsonb` recusa `\u0000`** (`22P05`): o payload com NUL vai como cópia
  reconstruída; o JLH do pedaço e o do dump nunca são tocados.
- **Posse de buffer.** O item é dono do cru; volta ao pool **uma vez**, após a
  confirmação. Na dúvida, `Discard`/`Taint` e deixe para o GC.
- **Lote retido não está no ring.** O shutdown coleta `Workers.Pending()`.
- **O dump nasce com nome temporário** (`.emg-…jsonl.tmp`, `fsync`, `link` ou
  `rename` conferido, `fsync` do diretório); um `.tmp` que sobra nunca é listado
  e é informado no boot (`Leftovers`).
- **A reingestão é em fluxo** (`Store.Open` + `Reader.Next`), nunca o arquivo
  inteiro; e os gauges terminam com a contagem do disco (`Store.Count`).
- **`Remove` e `Quarantine` feitos sem `fsync` do diretório** devolvem
  `ErrNotDurable`: é `WARN`, não "será entregue de novo".
- **A contagem do aborto tem um dono só**: `dumpPhase` + `dumping.Swap`, sob
  o `lossMu` (`enterDumpPhase`, `oweDump`: nada rearma o contador depois de um
  aborto; `settleDump`: o dump toma o contador de volta sob ele, ou um dump
  que via `aborted` antes de o aborto tomar o contador contava
  `emergency_failed` no lugar de `abort`). Sem isso o aborto durante o dump contava duas vezes ou nenhuma. E um
  dump que o aborto já contou é **abandonado** (`Store.SetAbandon`): publicado,
  voltaria no boot seguinte embora contado como perdido.
- **O `sink.boot_timeout` da reingestão é sem progresso**: o prazo anda a cada
  pedaço confirmado. Um prazo desde o início gastava-se com sucessos.
- **Shutdown por FATAL tolera o primeiro `SIGTERM`/`SIGINT`, nunca o
  `SIGQUIT`**; e um `SIGQUIT` que chega tarde (já no shutdown) ainda aborta.
- **SIGHUP** compara chave recarregável com o que está em execução e chave de
  restart com o boot. Só `log.level`, `log.access` e `log.access_sample`
  recarregam (o `log.format` exige restart).
- **Chave do pai no env some em silêncio** se não estiver em `removed.go`.
- **Os parsers de INI, de flags e da URL nomeiam a linha ou a flag, nunca o
  valor.** Depois de um valor, uma flag desconhecida fora das seções conhecidas
  (as deste servidor e as que saíram do R) não é nomeada (`knownSectionOf`);
  uma flag que começa com o nome de um parâmetro e continua (o `=` esquecido)
  nunca é; no INI, a chave depois de um segredo também não (a continuação de
  uma senha quebrada de linha).
- **Valores de URL repetidos:** `url.Values.Get` lê o primeiro, a libpq o
  último; a `LOGHUB_EB_URL` recusa parâmetro repetido, fragmento e `@` depois
  de `/`, `?` ou `#`.
- **A hora da mensagem não é tempo**; **uma amostra de relógio só**, para `uuid`
  e `event`; `time.RFC3339Nano` remove zeros; `json.Marshal` troca UTF-8
  inválido em silêncio. A SPEC §6 diz o que usar.
- **Testes portados que passam vazios:** sem um caso de controle válido, uma
  regra nova pode recusar todos pelo motivo errado.
- **Escrita no boot é por pedaço**: use `Instrumented.Deliver`.

## 10. Estado do projeto

Ver `docs/TESTING.md` §8 (lacunas), `docs/SPEC.md` §22.1 (defeitos conhecidos
e ainda não corrigidos) e a ordem de entrega em `docs/DECISOES.md` §6.
