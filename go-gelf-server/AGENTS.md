# AGENTS.md — go-gelf-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-gelf-server/                 <- VOCÊ ESTÁ AQUI (módulo Go independente, "G")
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-gelf-server
    cmd/go-gelf-server/
    internal/
    docs/DECISOES.md              as decisões da família GELF (2026-10-31) e as da implementação
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, 23 compose <ring>-<sink>, compose.demo.yml
                                  (emissor fluent-bit), conf/gelf.ini (chaves recarregáveis)
  go-gelf-pg-server/ (GP), go-gelf-nats-server/ (GN)   <- cortes deste servidor
  go-gelf-injector/ (GI)                              <- cliente de carga e funcional
  go-syslog-server/ (S)           <- PAI do código: referência, nunca dependência
  go-relplog-server/, go-webhook-server/, ...
```

Este é o servidor **GELF** (Graylog Extended Log Format 1.1) do ecossistema
**LogHub**, criado em 2026-10-31 como cópia adaptada de S. Recebe UDP (com
chunks e gzip/zlib), TCP (moldura `\0`), TCP+TLS, HTTP (`POST /gelf`) e HTTPS,
converte cada documento num envelope **JLH** com `type=gelf` e entrega a um
destino configurável (padrão: NATS JetStream em `loghub.{space}.gelf`). É um
Bridge-IN: `step = 0`.

## 2. As fontes normativas mandam, e a SPEC diz onde não

- A entrada comum da família é `tasks/gelf/DESIGN.md` (fora do git); depois da
  SPEC, quem manda é a SPEC.
- Skills do servidor MCP `loghub-mcp`: `loghub-json-schema` (envelope),
  `loghub-postgres-table-inbox` (`sink=postgres`), `go-loghub-uuidv7` e
  `go-loghub-ident`. **Não existe skill `type=gelf`**: o schema do payload é
  deste repositório, `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json`
  (decisão 1), e mudá-lo é mudar o contrato (SPEC §3.2 primeiro).
- A spec GELF 1.1 do Graylog é a fonte do protocolo. `docs/SPEC.md` §1.3 lista
  os **desvios deliberados** (D1 `level` ausente = 6, D2 `host` e
  `short_message` ausentes aceitos, D3 schema só em teste, D4 nível de UUID
  fixo, D5 branco em `host` = `host-invalid`, D6 teto de 128 KiB). Fora deles,
  divergir é **bug**.
- `docs/SPEC.md` está congelada: discrepância entre código e spec é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite a spec por seção (`§5.2`).
- `docs/TESTING.md` registra as lacunas com honestidade. Fechou uma, atualize;
  achou outra, acrescente.

## 3. S é a referência, não a dependência

`../go-syslog-server/` já resolveu, testou e corrigiu tudo o que não é GELF:
rings, sinks, posse de buffer, saúde, shutdown, dump, registry, PROXY, TLS.
**Leia o código dele antes de escrever o seu**, e copie com adaptação, trazendo os
comentários: eles são o registro de por que cada decisão foi tomada. **Nada é
importado de lá**, e nada é importado de nenhum outro módulo do monorepo.

E o sentido inverso: `internal/convert`, os arquivos GELF de
`internal/receiver` (`chunks.go`, `decompress.go`, `framing.go`, `http.go`) e
`internal/item` são **copiados literalmente** por GP e GN (SPEC §17.1). Mudou um
deles aqui, a mudança vai para os dois no mesmo dia, ou fica registrada como
"a portar".

## 4. Fronteira de responsabilidade

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| Outras pastas do monorepo | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto** | o nível do monorepo, e só com pedido explícito do usuário |
| **Imagem no Docker Hub** (`tmsoftbrasil/go-gelf-server`) | só com pedido explícito; `docker push` nunca por conta própria |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-gelf-server/vX.Y.Z`.

## 5. O modelo mental

### Dois eixos ortogonais

- **RING**: onde o documento espera. `memory` | `sqlite` | `redis` | `none`
- **SINK**: para onde o JLH vai. `nats` | `redis` | `sqlite` | `postgres` | `mysql` | `clickhouse` | `none`

Nenhuma implementação de ring consulta o tipo do sink, e vice-versa.

### Cru no ring, conversão no dreno (SPEC §2.2)

A recepção faz o mínimo: remontagem dos chunks UDP, moldura `\0`, corpo HTTP,
**descompressão com teto**, **uma** amostra de relógio, o `level` de topo (só
com filtro ativo), filtro, rate-limit e o `uuid`. O item no ring é **cabeçalho +
documento** — o documento GELF **já remontado e descomprimido**, sem JLH. A
conversão completa (validação JSON, campos, números, escapes) e a serialização
rodam no worker de dreno (ou na recepção com `ring=none`).

Consequência que você vai encontrar por todo lado: **o JLH é função pura de
cabeçalho + documento + config de conversão** (SPEC §3.5). Por isso toda chave
de `[convert]` exige restart, a hora vem da amostra do cabeçalho (nunca
`time.Now()` na conversão), e um retry reenvia os mesmos bytes. O que a
conversão não pode tirar do documento vai no cabeçalho: `compression`,
`chunks`, endereços, sessão TLS.

### O que o transporte confirma

| Transporte | Confirmação | Ring cheio, sink DOWN, rate-limit |
|---|---|---|
| TCP, TLS | continuar lendo | parar de ler; a janela TCP segura o emissor (rate-limit descarta) |
| HTTP, HTTPS | **`202` por documento**, depois da garantia do ring | **`503`** com `Retry-After: 1`; nada é segurado |
| UDP | não existe | **descarte contado** (`gelf_messages_dropped_total`) |

Não invente confirmação onde o protocolo não tem, e não negue com `4xx` o que
pode passar numa nova tentativa.

## 6. Mapa do código

```
cmd/go-gelf-server/main.go     flags que encerram, lhident.Initialize(), internal/app
internal/
  app/        montagem, boot, shutdown, SIGHUP, reingestão
  config/     registry único -> INI + flags + env + --help + dump; LOGHUB_EB_URL
  ident/      Identity congelada e gramática de space                   [lógica pura]
  jlh/        escritor JSON por append, Event, envelope, UUID            [lógica pura]
  convert/    documento GELF -> payload type=gelf -> JLH: json.go (parser por
              varredura validante), number.go (números como texto, timestamp
              decimal), names.go, convert.go                   [lógica pura, fuzz]
  item/       Item, codec do cabeçalho (compression, chunks), pools      [lógica pura, fuzz]
  rediscli/   cliente e classificação de erros comuns aos dois Redis
  jlhcheck/   oráculo de teste: schemas + regras semânticas             [só em teste]
  receiver/   datagram.go (UDP, pktinfo), chunks.go (remontagem), decompress.go,
              framing.go (\0), stream/tcp/tls.go, http.go (POST /gelf),
              proxy.go, limits.go, ingest.go                             [fuzz]
  filter/     level (min_level, levels); ratelimit/                      [lógica pura]
  ring/       memory, sqlite, redis, none
  sink/       nats, redis, sqlite, postgres, mysql, clickhouse, none, workers, health
  control/    heartbeat beacon
  emergency/  dump e reingestão                                          [fuzz]
  obs/        métricas Prometheus próprias (gelf_) + slog
```

## 7. Comandos

Todos a partir desta pasta.

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**.

Corpus dourado, schemas e oráculo:

```bash
go test -race -run 'Corpus|Schema|Semantic|Golden' -v ./internal/convert/ ./internal/jlhcheck/
GELF_UPDATE_GOLDEN=1 go test -run Corpus ./internal/convert/   # regrava os .want.json: revise o diff
```

Fuzz, obrigatório antes de release (10 min cada; 60 s no dia a dia):

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzReassemble$   -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzDecompress$   -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=60s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$         -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzChPayload$    -fuzztime=60s
```

O `^…$` importa: o Go recusa um `-fuzz` que case dois alvos do pacote.

Destinos reais (as suítes pulam sem a variável): `docs/TESTING.md` §6. Com a
infraestrutura do monorepo de pé (`tasks/infra/up.sh`), o `env.sh` exporta as
`GELF_TEST_*`, inclusive `GELF_TEST_NATS_PERM_URL` no `:24224`, que nega
`loghub.*.gelf` ao `nopub` (se o `perm.conf` gerado for anterior à regra,
regenere-o e reinicie só esse `nats-server`, TESTING §6.4):

```bash
source ../tasks/infra/env.sh
go test -race -count=1 -timeout 1200s ./...
```

Emissores reais (fluent-bit em Docker): `GELF_TEST_EMITTERS=1 go test -race
-run Emitters -timeout 600s ./internal/app/`.

Executar localmente (um `DATADIR` relativo é resolvido a partir do diretório de
trabalho antes do `Initialize()`, SPEC §13.1 passo 2; o hostname do macOS não
pode ter `_`, ou o `go-loghub-ident` sai com um código entre 100 e 114):

```bash
mkdir -p /tmp/gls-data
DATADIR=/tmp/gls-data AGENT_NAME=go-gelf-server HOSTNAME=dev-mac \
  go run ./cmd/go-gelf-server --sink.type=sqlite --listen.udp=127.0.0.1:12201 \
  --listen.tcp=127.0.0.1:12201 --listen.http=127.0.0.1:12202 --admin.bind=127.0.0.1:9104
printf '{"version":"1.1","host":"dev","short_message":"teste"}' | nc -u -w1 127.0.0.1 12201
printf '{"version":"1.1","host":"dev","short_message":"teste"}\0' | nc -w1 127.0.0.1 12201
curl -sS -w '%{http_code}\n' --data-binary '{"short_message":"teste"}' http://127.0.0.1:12202/gelf
sqlite3 /tmp/gls-data/events.db 'select env from gelf_events'
```

Imagem (nunca `docker push` sem pedido):

```bash
docker build -f docker/Dockerfile -t tmsoftbrasil/go-gelf-server:dev .
```

## 8. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**. As únicas strings em português no código são os textos
  normativos de `parse.error` (SPEC §6.1), que são dado.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- **Dependências: as oito de S em produção** (SPEC §17.3), nenhuma a mais: gzip,
  zlib, deflate e o servidor HTTP são da stdlib, e o parser JSON é próprio.
  `jsonschema/v6` só em teste. Nada de `golang.org/x/net` (PKTINFO é
  `syscall`). Dependência nova exige justificativa na spec.
- **`encoding/json` fora do caminho quente.** Ele só aparece na cópia NUL (e
  de número fora do `numeric`) da `loghub.inbox`, sempre com `json.Number`. A
  citação do ClickHouse (`chPayload`) é uma reescrita de bytes; a versão com
  `encoding/json` é o oráculo dela nos testes.
- **Config:** o registry é a fonte única. Um parâmetro novo entra só em
  `internal/config/registry.go`. Env `GELF_<SEÇÃO>_<CHAVE>`, sem exceção de
  seção.
- **Métricas:** cardinalidade fixa; nenhum label vindo do emissor (nem `host`,
  nem nome de campo, nem `level`).
- **Identidade:** `lhident.Initialize()` só no `main`, uma vez. Pacotes recebem
  `ident.Identity`; testes que precisam da biblioteca usam `TestMain`.
- **Escrita em disco:** tudo sob `server.data_dir` (`DATADIR`), inclusive a
  identidade, sem exceção (não há socket unix aqui).

## 9. Armadilhas que já custaram bugs reais

### As do GELF (DESIGN §5 e a implementação)

- **Bomba de descompressão:** o teto `L` vale **dentro** do leitor do
  `gzip`/`zlib`/`flate` (`io.LimitReader(L + 1)`; o byte a mais denuncia
  `too_large`), nunca depois de descomprimir. Um gzip de 1 MiB vira 1 GiB.
- **Nunca alocar pelo que o par declara:** nem pelo `count` dos chunks (o
  slice cresce com os chunks que **chegam**), nem pelo `Content-Length` (o
  corpo cresce com o que chega, até `L + 16 KiB`; o declarado só serve para a
  recusa `413` antecipada), nem pelo PROXY v2.
- **A remontagem UDP é estado compartilhado** entre todos os listeners e
  origens, com teto **global** de bytes e de mensagens abertas, e o prazo de 5 s
  é varrido por um **relógio próprio** (a cada 1 s), não pela chegada de chunks:
  senão uma mensagem órfã fica para sempre num servidor ocioso. A chave é
  `(src-ip, src-port, id)`: o id sozinho é do emissor e colide entre emissores.
- **Números JSON nunca passam por `float64`:** `1e400`, `0.1`, `-0`,
  `12345678901234567890` são o **texto** que chegou, validado pela gramática.
  O `timestamp` é aritmética decimal sobre o texto. Um `json.Unmarshal` num
  `any` em qualquer caminho até o JLH é bug.
- **Escape de saída a partir do texto decodificado:** toda string é
  decodificada (escapes, pares de surrogates; surrogate solto → U+FFFD com
  `encoding-replaced`, que força `raw`) e reescrita pelo escritor do `jlh`.
  Copiar o trecho escapado de entrada sem validar deixa passar `\ud800` e
  escapes que o oráculo recusaria.
- **HTTP `4xx` é final para o cliente:** só o que nunca vai passar ganha `4xx`
  (`400`, `404`, `405`, `413`, `415`). Ring cheio, sink DOWN, rate-limit e
  shutdown são **`503` com `Retry-After`**; um `429` ou `400` ali faz o
  emissor jogar o documento fora. Filtrado é `202` (descarte pedido pelo
  operador).
- **ClickHouse: os valores de `fields`/`extra` são citados pelo sink**
  (`chPayload`, SPEC §3.7.2, §10.10.3). Os `input_format_json_read_*_as_strings`
  do 25.8 **não** preservam o texto: `1.50` vira `"1.5"`, e um `1e400` em
  qualquer ponto recusa o `INSERT` com `117` (o pedaço inteiro, `logic`). Se
  você "simplificar" a citação confiando nos ajustes, só o
  `TestClickHouseFieldsAndExtraAreText` reclama. E os dois objetos são
  `Map(String, String)`: como caminhos do `JSON`, `_a.b` contra `_a` objeto
  recusaria o pedaço.
- **PostgreSQL: o NUL chega às chaves.** A cópia reconstruída da
  `loghub.inbox` troca U+0000 por U+FFFD também nas **chaves** de
  `fields`/`extra`, em qualquer profundidade; duas chaves que colidem depois
  da troca seguem a regra fixa (vence a sem NUL; entre duas com NUL, a menor
  original), nunca a ordem do mapa. E o `raw` UTF-8 com NUL real sai em base64
  **só nessa cópia**: os testes da fila reconvertem antes do oráculo (SPEC
  §3.7.1).
- **Documento vazio:** no UDP e no HTTP é `empty` contado (`400`); no TCP um
  quadro vazio ou só de espaço entre `\0` é separador, **sem** contar.
- **Descompressão inválida não é `malformed`:** é descarte `decompress`; o cru
  do ring é o documento descomprimido.
- **A amostra de relógio do UDP chunked é a do último chunk**, o que completa
  a mensagem.

### As herdadas de S (valem igual)

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
- **Nunca DDL** em postgres/mysql/clickhouse; nunca criar stream NATS.
- **`sink=postgres` é a fila `loghub.inbox` da skill** (SPEC §3.7.1): DDL
  literal em `docker/postgres/inbox.sql` (um teste confere o hash), chave só
  `uuid`, PostgreSQL 18. **`jsonb` recusa `\u0000`** (`22P05`, o pedaço
  inteiro): por isso a cópia reconstruída acima; o JLH do pedaço nunca é tocado.
- **`ON DUPLICATE KEY UPDATE` exige `UPDATE`**; nunca `INSERT IGNORE`. O MySQL
  tem 13 colunas: no máximo **5 041 linhas** por comando.
- **`sink=clickhouse` tem tabela própria, `inbox_gelf`**: o tipo `JSON`
  descarta objeto vazio (`{}` é ausente), o boot recusa a tabela sem os dois
  *hints*, com motor que não seja `*ReplacingMergeTree`, sem `uuid` na chave
  (`(event_at, uuid)`), com CHECK, ou com `max_memory_usage` abaixo do pior
  pedaço (`241` é fatal).
- **O tipo `JSON` do ClickHouse infere tipos de strings** (`fields.created` do
  Docker é uma data em texto). Todo `INSERT` desliga a inferência; se você
  "limpar" esses ajustes, só o `TestClickHouseStringsAreNeverInferred` reclama.
- **O `insert_deduplication_token` sai do conteúdo do pedaço**, nunca de um
  contador.
- **`nats.go`:** `WithRetryAttempts(0)`, `ReconnectBufSize(-1)`,
  `WithPublishAsyncTimeout`. Se você "simplificar", os testes não reclamam.
- **O JLH vai em `Data`, nunca num header NATS** (teto de 64 KiB de headers).
- **O `\0` da moldura não é do documento.**
- **UDP sem `SO_RCVBUF` grande perde em silêncio**; o efetivo é lido de volta
  (no Linux o kernel devolve o dobro, e a comparação divide).
- **A hora do documento não é tempo.** O tempo é a amostra da recepção
  (`event`); `payload.timestamp` é alegação do emissor.
- **Duas leituras de relógio** fazem o fato parecer posterior ao envelope: uma
  amostra só, para `uuid` e `event`.
- **`time.RFC3339Nano` remove zeros**; `json.Marshal` troca UTF-8 inválido em
  silêncio. A SPEC §3.3 e §6 dizem o que usar.
- **SIGHUP** compara chave recarregável com o que está em execução e chave de
  restart com o boot.
- **Durações zero** são recusadas onde zero não significa "sem limite"
  (`http.read_header_timeout` e `http.read_timeout` inclusive).
- **Testes portados que passam vazios:** um helper que devolve zero quando a
  chave falta faz o teste passar sem provar nada; um caso de recusa sem um
  caso de controle válido pode recusar tudo pelo motivo errado. Ao portar um
  teste de S, confira que ele ainda exercita algo GELF.
- **O `\s` do motor de regex não é o do schema.** O `^\S+$` de `host.hostname`
  é ECMAScript; o RE2 do `jsonschema/v6` deixa passar U+00A0, U+2028, U+FEFF e
  `\v`. O oráculo (`jlhcheck`) confere o conjunto completo de D5.
- **Escrita no boot é por chunk.** Use `Instrumented.Deliver`, nunca um retry em
  volta de `Instrumented.Write`.
- **Um "a portar" de um irmão é candidato, não patch.** Confira no S (e no
  `CHANGELOG.md` da raiz) antes de copiar: o que vale para RELP ou para syslog
  pode perder documentos aqui.
- **Valores aceitos pela config precisam significar algo para quem os usa.** Um
  enum aceito pelo registry e desconhecido no pacote consumidor desliga o
  recurso em silêncio (a validação do filtro usa o `filter.ParseLevel` do
  próprio filtro por isso).

## 10. Estado do projeto

Ver `docs/TESTING.md` §8 (lacunas), as decisões e os riscos em
`docs/DECISOES.md`, e as pendências do código atual em `docs/SPEC.md` §22.1
(corrigida uma, a regra entra na seção indicada e o item sai de lá). O
andamento da entrega da família está em `tasks/gelf/progress-G.md` (fora do
git).
