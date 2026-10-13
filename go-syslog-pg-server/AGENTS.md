# AGENTS.md — go-syslog-pg-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-syslog-pg-server/            <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server
    cmd/go-syslog-pg-server/
    internal/
    docs/DECISOES.md              a entrevista de 2026-10-07 que definiu este corte
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, docker-compose.yml (memory -> PostgreSQL), conf/syslog.ini
  go-syslog-server/               <- o PAI: este módulo é um corte dele. Referência, nunca dependência
  go-syslog-standard-server/      <- o antigo, referência do pai
  go-relplog-server/              <- RELP, só especificação
  go-webhook-server/, go-webhook-fast-server/
```

Este é um servidor syslog do ecossistema **LogHub**, criado em 2026-10-07 como
**corte** do `go-syslog-server`: recebe UDP, TCP e unix datagram, converte cada
mensagem num envelope **JLH** com `type=syslog` e o **enfileira** na
`loghub.inbox` do PostgreSQL. Ring só em memória, emergência em JSON Lines,
**sem TLS em lugar nenhum**, três dependências de produção. É um Bridge-IN:
`step = 0`.

## 2. As fontes normativas mandam, e a SPEC diz onde não

- Skills do servidor MCP `loghub-mcp`: `loghub-json-schema` (envelope),
  `loghub-type-syslog-schema` (payload e algoritmo de conversão),
  `loghub-postgres-table-inbox` (a fila), `go-loghub-uuidv7` e
  `go-loghub-ident`. Cópias dos schemas e dos vetores dourados ficam em
  `internal/jlhcheck/loghub/`.
- `docs/SPEC.md` §1.3 lista os **desvios deliberados** (D1 moldura por conexão,
  D2 TAG estrita sem timestamp, D3 schema só em teste, D4 nível de UUID fixo,
  D5 sem importação, D6 espaço em branco nos campos `^\S+$`, D7 avisos de
  encoding de SD com texto colado, D8 anos fora de 0001–9999). Fora deles,
  divergir de uma skill é **bug**.
- `docs/SPEC.md` está congelada: discrepância entre código e spec é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite a spec por seção (`§6.4.3`).
- **A numeração da SPEC é a do pai.** Seção removida mantém o título com a nota
  "Removido no go-syslog-pg-server"; **nunca renumere**, o código cita os
  números. Seção nova entra no fim da seção-mãe, com número novo.
- `docs/TESTING.md` registra as lacunas com honestidade. Fechou uma, atualize;
  achou outra, acrescente.

## 3. O pai é a referência, não a dependência

`../go-syslog-server/` tem o código de onde este saiu, com o histórico de bugs
(TESTING §9 dele) e os comentários que explicam cada decisão. Para entender por
que algo é assim, leia lá. **Nada é importado de lá**, e nada é importado de
nenhum outro módulo do monorepo. **Não edite o pai** a partir daqui.

O que saiu (SPEC §1.2) não volta sem decisão do usuário registrada em
`docs/DECISOES.md`: TLS (listener e cliente), os outros sinks e rings, filtro,
rate-limit, beacon. O objetivo do módulo é ser rápido e ter poucas dependências.

## 4. Fronteira de responsabilidade

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| Outras pastas do monorepo (o pai inclusive) | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto** | o nível do monorepo, e só com pedido explícito do usuário |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-syslog-pg-server/vX.Y.Z`.

## 5. O modelo mental

### Um ring, um sink

- **RING**: `memory`, e só. Não há chave `ring.mode`.
- **SINK**: `postgres`, a fila `loghub.inbox`, e só. Não há chave `sink.type`.

As interfaces `ring.Ring` e `sink.Sink` ficam, com uma implementação cada: são
a costura dos testes. O ring não consulta o sink, e vice-versa. O log de boot e
o `syslog_config_info` dizem `ring_mode="memory"` e `sink_type="postgres"`, por
compatibilidade com os painéis do pai.

### Cru no ring, conversão no dreno (SPEC §2.2)

A recepção só faz moldura, truncamento, **uma** amostra de relógio, o estado do
sink e o `uuid`. **Não lê a mensagem**: nem PRI (não há filtro). O item no ring
é **cabeçalho + cru**, sem JLH, e o cabeçalho só existe em memória (não há
codec). A conversão completa e a serialização rodam no worker de dreno.

Consequência: **o JLH é função pura de cabeçalho + cru + config de conversão**
(SPEC §3.5). Toda chave de `[syslog]` exige restart, a referência da inferência
de ano é a amostra do cabeçalho (nunca `time.Now()` na conversão), e um retry
reenvia os mesmos bytes.

### O dump é JLH pronto, e volta sem conversão (SPEC §12)

Em FATAL ou dreno estourado, o ring pendente é **convertido** e escrito como
JSON Lines, um JLH por linha, idêntico ao que o worker escreveria. No boot
seguinte cada linha vai **como está** para a fila; as colunas saem do cabeçalho
da própria linha. Um dump sobrevive a uma troca de `[syslog]`.

### O que o transporte confirma

| Transporte | Confirmação | Ring cheio |
|---|---|---|
| TCP | continuar lendo | parar de ler; a janela TCP segura o emissor |
| unix dgram | não existe | descarte, mas o kernel já segurou o emissor |
| UDP | não existe | **descarte contado** |

Não invente confirmação onde o protocolo não tem.

## 6. Mapa do código

```
cmd/go-syslog-pg-server/main.go  flags que encerram, lhident.Initialize(), internal/app
internal/
  app/        montagem, boot, shutdown, SIGHUP, reingestão, admin
  config/     registry único -> INI + flags + env + --help + dump; LOGHUB_EB_URL
              (só postgres://, regra do sslmode); removed.go (env do pai -> WARN)
  ident/      Identity congelada e gramática de space              [lógica pura]
  jlh/        escritor JSON por append, Event, envelope, UUID, Payload [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH                  [lógica pura, fuzz]
  item/       Item, Head, pools (sem codec)                         [lógica pura]
  jlhcheck/   oráculo de teste: schemas + regras semânticas da skill [só em teste]
  receiver/   udp (pktinfo), unix, tcp, moldura, proxy, ingest      [fuzz]
  ring/       interface + memory
  sink/       interface + postgres, workers, chunk, health, instrumented [fuzz]
  emergency/  dump JSON Lines, ParseLine, reingestão                [fuzz]
  obs/        métricas Prometheus próprias + slog
```

## 7. Comandos

Todos a partir desta pasta.

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**. Sem `SYSLOG_TEST_PG_DSN` a integração pula: o
PostgreSQL é o único destino, então a prova ponta a ponta precisa dele.

```bash
# contêiner lh-pg18 do host de desenvolvimento, ou docs/TESTING.md §6
SYSLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
  go test -race -timeout 900s ./internal/sink/ ./internal/app/
```

Vetores dourados e contratos dos schemas:

```bash
go test -race -run 'Golden|Schema|Corpus' -v ./internal/convert/
```

Fuzz, obrigatório antes de release (10 min cada; 60 s no dia a dia):

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=60s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrameOracle$  -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=60s
```

O `^…$` importa: `-fuzz=FuzzFrame` casa também `FuzzFrameOracle`, e o Go recusa
dois alvos. Não há `FuzzHead`: o codec do cabeçalho foi apagado.

Executar localmente (um `DATADIR` relativo é resolvido a partir do diretório de
trabalho antes do `Initialize()`, SPEC §13.1 passo 2; o hostname do macOS não
pode ter `_`, ou o `go-loghub-ident` sai com um código entre 100 e 114). A fila
precisa existir (`docker/postgres/inbox.sql`): o servidor nunca faz DDL.

```bash
mkdir -p /tmp/slpg-data
DATADIR=/tmp/slpg-data AGENT_NAME=go-syslog-pg-server HOSTNAME=dev-mac \
  LOGHUB_EB_URL='postgres://sls:sls@127.0.0.1:15432/sls_test' \
  go run ./cmd/go-syslog-pg-server --listen.udp=127.0.0.1:5180 \
  --listen.tcp=127.0.0.1:5180 --admin.bind=127.0.0.1:19180
printf '<13>1 - - app - - - teste' | nc -u -w1 127.0.0.1 5180   # o logger do macOS não tem -n
```

## 8. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**. As únicas strings em português no código são os textos
  normativos de `parse.error` (SPEC §6.6), que são dado.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- **Dependências: três em produção** (SPEC §17.3): `pgx/v5`, `go-loghub-ident`,
  `go-loghub-uuidv7`. `jsonschema/v6` só em teste. Nada de `golang.org/x/net`
  (PKTINFO é `syscall`) nem `golang.org/x/text` (charset é tabela própria).
  Dependência nova exige justificativa na spec — e vai contra o objetivo do
  módulo.
- **Config:** o registry é a fonte única. Um parâmetro novo entra só em
  `internal/config/registry.go`. Prefixo `SYSLOG_` e nomes iguais aos do pai
  para as chaves comuns; uma chave do pai que não existe aqui vai para
  `removed.go` (aviso no env).
- **Métricas:** prefixo `syslog_`, cardinalidade fixa; nenhum label vindo do
  emissor; série que nunca sobe não é exposta.
- **Identidade:** `lhident.Initialize()` só no `main`, uma vez. Pacotes recebem
  `ident.Identity`; testes que precisam da biblioteca usam `TestMain`.
- **Escrita em disco:** tudo sob `server.data_dir` (`DATADIR`), inclusive a
  identidade. Exceção única: o socket `listen.unix`.

## 9. Armadilhas que já custaram bugs reais (herdadas do pai e da standard)

- **A linha do dump é reingerida SEM conversão.** `emergency.ParseLine` é a
  **única** porta entre um arquivo em disco e as colunas da fila. Uma linha que
  a fila recusasse seria `ClassLogic` do **pedaço inteiro** — todo envelope
  daquele `INSERT`, os íntegros junto. Por isso a porta exige o cabeçalho byte a
  byte igual ao de `jlh.AppendHeader`, escapes `\u` só de controle, todo número
  inteiro simples de até 20 dígitos (`1e1000000` é `22003` no `jsonb`), payload
  com `message` string (grafia exata, a última se repetida), o payload cortado
  por posição igual ao do parser, e o resto de SPEC §12.2. Se você
  "simplificar" uma dessas regras, pergunte-se qual linha a fila passaria a
  recusar.
- **Sem TLS, de propósito.** Não "conserte" o `sslmode=disable`, o `TLSConfig`
  nulo nem a recusa de `sslmode=require` na `LOGHUB_EB_URL`: é decisão do
  usuário (DECISOES, decisão 4). E não remova os *fallbacks* nulos: com eles uma
  segunda tentativa poderia negociar outra coisa, ou bater em `localhost`.
- **O ambiente libpq não chega ao pool.** A config do pool é lida com **toda**
  variável `PG*` escondida (`parseWithoutLibpqEnv`, sob mutex), não
  sobrescrita depois: a própria leitura falhava (`PGSSLMODE=bogus`,
  `PGSERVICE` inexistente, `PGPORT=abc`: exit 1) ou deixava passar o que não
  tem campo para sobrescrever (`PGCHANNELBINDING=require`,
  `PGREQUIREAUTH=md5`: toda conexão recusada sem SQLSTATE, retentada até o
  `boot_timeout`). SPEC §10.5.
- **Posse de buffer.** O item é dono do cru; volta ao pool **uma vez**, após a
  confirmação. Num retry ainda é o dono. Erro devolvido não significa "ninguém
  mais lê": na dúvida, `Discard`/`Taint` e deixe para o GC.
- **Lote retido não está no ring.** O shutdown coleta `Workers.Pending()`; o
  `hold` faz `append`, nunca atribuição. E os dois tetos do ring contam o lote
  retido.
- **Contar o que se segura:** o ring conta `cap(cru)` + `ItemOverhead` (256 B),
  não `len(cru)`. `TestItemOverheadCoversTheStruct` falha se a struct crescer.
- **Fronteira de histograma como string** pré-formatada na inicialização.
- **Slices do pool do `unnest`** voltam zerados.
- **Classes `42` e `3D` são fatais**, nunca `logic` nem `network` (uma tabela
  ou um banco que não existe não aparece esperando). Erro desconhecido é
  `network`.
- **Nunca DDL.** A fila vem de `docker/postgres/inbox.sql`, DDL literal da skill
  (um teste confere o hash: não edite o bloco), chave só `uuid`, PostgreSQL 18.
- **`jsonb` recusa `\u0000`** (`22P05`, classe 22, o pedaço inteiro
  descartado): o payload com NUL vai como cópia reconstruída (`raw` em base64,
  U+FFFD nos outros textos); o JLH do pedaço e o do dump nunca são tocados.
- **Nunca alocar pelo que o par declara** (MSG-LEN, PROXY v2, linha do dump):
  alocar pelo que chegou, com teto. `emergency.MaxLine` é teto, não `make`.
- **Reavaliar a moldura a cada frame embaralha o fluxo** (D1).
- **O `\n`, o NUL e os `CR` da moldura não são da mensagem.**
- **UDP sem `SO_RCVBUF` grande perde em silêncio**; o efetivo é lido de volta.
- **A hora da mensagem não é tempo.** O tempo é a amostra da recepção (`event`).
- **Duas leituras de relógio** fazem o fato parecer posterior ao envelope: uma
  amostra só, para `uuid` e `event`.
- **`time.RFC3339Nano` remove zeros**; `strings.ToValidUTF8` junta octetos
  inválidos; `time.Date` normaliza 30/fev; `json.Marshal` (e `json.Unmarshal`)
  troca UTF-8 inválido em silêncio. A SPEC §6 diz o que usar.
- **O dump nasce com nome temporário.** Escreve em `.emg-…jsonl.tmp`, `fsync`,
  `link` para o nome final (nunca um `rename` às cegas, que substituiria; sem
  *hard link* — vfat, exFAT, FUSE — o `rename` vem só depois de conferir o nome
  livre), `fsync` do diretório. Escrito direto no nome final, um dump cortado numa fronteira de
  linha parecia limpo, era entregue em parte e **removido**. Falha em qualquer
  passo remove o temporário e é exit 3; um `.tmp` que sobra nunca é listado.
- **A reingestão é em fluxo.** `Store.Open` + `Reader.Next(batch, max_bytes)`,
  nunca o arquivo inteiro: o dump chega a ~12× o ring, e carregá-lo matava o boot
  por OOM em laço. Um único `boot_timeout` **sem progresso** para a reingestão
  inteira (cada pedaço entregue o empurra).
- **Shutdown por FATAL tolera o primeiro sinal.** Ele começa sem sinal; o
  primeiro `SIGTERM` não pode cortar o dump, só o segundo aborta. E o abort conta
  em `abort` o que já fora entregue ao dump, não só o que ficou no ring.
- **A perda do abort tem um dono só** (SPEC §13.4). Da fase do dump em diante
  o abort conta o contador `dumping`, nunca o ring; as passagens
  (`enterDumpPhase`, `oweDump`) e o abort vão sob `lossMu`, o dump o toma com
  `Swap`, e um dump já contado como perdido desiste (`SetAbandon`). Conte cada
  mensagem num lado só, e teste o abort caindo em **cada** janela (os ganchos
  `beforeCollect`, `beforeDump`): a corrida esperada não aparece. `SIGQUIT`
  nunca é tolerado, e um `SIGQUIT` tardio chega por `quitNow`.
- **SIGHUP** compara chave recarregável com o que está em execução e chave de
  restart com o boot. Só `[log]` recarrega.
- **Chave do pai no env some em silêncio** se não estiver em `removed.go`: o
  registry lê só os próprios nomes.
- **Os parsers de INI e de flags nomeiam a linha ou a flag, nunca o valor**:
  `password: x` e `sink.postgres.password=x` iam inteiros ao stderr. Um
  argumento com forma de flag nunca é valor de outra flag e só é nomeado se for
  uma flag deste servidor, e nada depois de um valor é citado (SPEC §13.2).
- **Valores de URL repetidos:** `url.Values.Get` lê o primeiro, a libpq vale o
  último. A `LOGHUB_EB_URL` recusa parâmetro repetido; sem isso
  `sslmode=disable&sslmode=require` passava pela recusa do TLS.
- **Durações zero** são recusadas onde zero não significa "sem limite".
- **Testes portados que passam vazios:** um helper que devolve zero quando a
  chave falta faz o teste passar sem provar nada. O mesmo vale para casos de
  recusa: sem um caso de controle válido, uma regra nova pode recusar todos pelo
  motivo errado.
- **O `\s` do motor de regex não é o do schema.** O `^\S+$` do schema é
  ECMAScript; o RE2 do `jsonschema/v6` deixa passar U+00A0, U+2028, U+FEFF e
  `\v`, e o Python da skill recusa ainda U+001C–U+001F e U+0085. O oráculo
  (`jlhcheck`) confere o conjunto completo na etapa semântica.
- **Escrita no boot é por pedaço.** Um erro `logic` no meio da reingestão não
  pode deixar os pedaços seguintes sem escrita e confirmados. Use
  `Instrumented.Deliver`.
- **Valores aceitos pela config precisam significar algo para quem os usa.** Um
  enum aceito pelo registry e desconhecido no pacote consumidor desliga o recurso
  em silêncio (`windows-1252` no charset).

## 10. Estado do projeto

Ver `docs/TESTING.md` §8 (lacunas), `docs/SPEC.md` §22.1 (defeitos conhecidos
e ainda não corrigidos) e a ordem de entrega em `docs/DECISOES.md` §5.
