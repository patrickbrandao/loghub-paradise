# go-relplog-server — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Os
números são de 2026-10-11/12, num Apple M2 (8 núcleos) com macOS, salvo onde
dito. Comandos a partir da pasta do módulo.

A base de testes veio do `go-syslog-server` (S) e foi adaptada: o que era de UDP,
unix e das molduras da RFC 6587 saiu; o RELP ganhou suíte própria (§3), um
cliente RELP de teste (`internal/receiver/relpclient_test.go`) e a suíte com o
rsyslog real (§7.6), que é a única prova de que a leitura do `librelp` por trás
da SPEC §5.4 vale no fio.

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. Sem variáveis de ambiente, as suítes de destino real e
de emissores pulam (§7) e a suíte inteira leva cerca de 60 s.

Cobertura (`go test -race -coverprofile=c.out ./...`, por pacote):

| Pacote | Cobertura | Observação |
|---|---|---|
| `filter` | 98.2% | |
| `control` | 96.2% | |
| `jlh` | 94.7% | |
| `config` | 93.4% | |
| `receiver` | 93.1% | o RELP inteiro |
| `obs` | 92.3% | |
| `sink` | 92.0% | com os destinos reais de §7; sem eles, bem menos |
| `convert` | 89.5% | |
| `ratelimit` | 89.5% | |
| `emergency` | 86.1% | |
| `ring` | 82.7% | com Redis; sem `RELPLOG_TEST_REDIS_ADDR`, o ring Redis pula |
| `item` | 80.9% | |
| `app` | 76.2% | com destinos e emissores reais |
| `ident` | 71.4% | os caminhos da biblioteca rodam no teste do binário |
| `jlhcheck` | 68.2% | é código de teste (oráculo) |
| `rediscli` | 67.8% | exercitado pelas suítes Redis de `ring` e `sink` |
| **total** | **88.5%** | rodada final de 2026-10-12, com tudo de §7 |

---

## 2. Vetores dourados, contratos e corpus (desvio D3, SPEC §18.2)

```bash
go test -race -run 'Golden|Schema|Corpus|Semantic' -v ./internal/convert/ ./internal/jlhcheck/
```

- **Os 24 vetores** de `loghub-type-syslog-schema`
  (`internal/jlhcheck/loghub/vectors.json`): cada entrada é convertida e
  comparada **byte a byte** com o esperado (o `uuid` só pelo instante), e cada
  envelope esperado passa no oráculo. Os vetores de UDP e unix continuam: o
  conversor é o mesmo do S, e o RELP só usa `tcp`/`tls`.
- **O oráculo** (`internal/jlhcheck`) é o porte de Go do `validate_payload.py`
  da skill: os dois schemas (`santhosh-tekuri/jsonschema/v6`, só em teste) mais
  as regras semânticas (ordem e número de chaves, `level` = severidade, `sid`
  recalculado, instante do `uuid` = `event`, `iso` truncado, `from` UUIDv7 e o
  conjunto completo de espaço em branco nos campos `^\S+$`, D6).
- **`x-transport: "relp"`** (D9) está em todo envelope que a app produz; o
  oráculo o aceita pelo `^x-` do schema, e
  `TestEndToEndEnvelopesAreValidJLH` confere que ele está lá.
- **Corpus de linhas reais** (`internal/convert/testdata/corpus/`, 23 pares):
  rsyslog, syslog-ng, journald, appliances, log4j sem PRI, Cisco com contador,
  BOM, cp1252, SD com escapes e malformadas.

---

## 3. RELP (`internal/receiver`, SPEC §5)

```bash
go test -race -run 'RELP|Review|Budget|TxnCharge' -v ./internal/receiver/
```

Cada teste conversa com o servidor por um cliente RELP de teste
(`relpclient_test.go`) sobre um socket real, com um ring de teste que deixa
segurar e soltar linhas pelo conteúdo (`gateRing`, `releaseRaw`), para que a
ordem das respostas numa janela seja determinística.

| Grupo | Testes | O que prova |
|---|---|---|
| Moldura (§5.1–§5.3) | `TestRELPFrames`, `TestRELPZeroLengthBothSpellings`, `TestRELPHeaderAndTrailerAreNotTheMessage`, `TestRELPMalformed`, `TestRELPFrameCeiling`, `TestRELPTruncationKeepsSync`, `TestRELPFrameGrowsWithTheBytesNotTheCount`, `TestRELPReaderReleasesALargeBuffer`, `TestRELPMidFrameCloseIsUnexpectedEOF`, `TestRELPUnknownAndUppercaseCommandsAreUnknown`, `TestAppendRSPAndServerClose`, `FuzzFrame` | as duas grafias de `DATALEN = 0`; só `DATA` é a mensagem; o fusível antes de ler o corpo; o truncamento que mantém a sincronia; o buffer que cresce com os bytes que chegam e é solto acima de 16 KiB; `0 serverclose 0\n` sem o espaço que o `librelp` recusa |
| Diálogo (§5.4) | `TestRELPSessionHappyPath`, `TestRELPOpenAdvertisesTheSoftware`, `TestOpenResponseOffers`, `TestRELPCommandOrder`, `TestRELPUnknownCommandKeepsTheConnection`, `TestRELPCloseWaitsForTransactionsInFlight`, `TestRELPFilteredAndEmptyAreAcknowledged`, `TestRELPConsumedIsAFinal500`, `TestRingNoneLogicRefusalIsFinal` | as ofertas no formato do `relpOffersToString`; `syslog` antes do `open` e o segundo `open`; o `close` que espera as transações em voo; `200` para filtrada e vazia; `500` só para a recusa `logic` final |
| Garantia | `TestRELPAcknowledgesOnlyAfterTheRing`, `TestEndToEndEnvelopesAreValidJLH` | nenhum `200` antes do ring |
| Janela e teto (§5.5, §5.6) | `TestRELPWindowAllowsConcurrentTransactions`, `TestRELPWindowStopsTheReader`, `TestRELPInflightBudgetIsGlobal`, `TestBudgetAcquireRelease`, `TestTxnChargeCoversTheTransaction` | transações em paralelo numa conexão; a janela cheia para o leitor; um teto para o processo inteiro; a conta `2 × classe + 8 KiB` |
| Backpressure (§5.7, §8) | `TestRELPFullRingIsHeldNotRefused`, `TestRELPBackpressureTimeoutClosesWithoutAnswering`, `TestRELPSinkDownIsHeld`, `TestRELPRateLimitedIsHeldUntilAToken`, `TestRELPHeldMessageIsChargedOnce`, `TestIngestRetryDecidesFilterAndRateLimitOnce`, `TestRELPVanishedPeerAbandonsHeldTransactions`, `TestReviewRetryAfterPeerLeft` | segurar, nunca `500`; o estouro fecha sem resposta; filtro e rate-limit uma vez por mensagem; par que sumiu nunca é enfileirado pelas costas da retransmissão |
| Fim de sessão (§5.8) | `TestRELPShutdownWithTransactionsInFlight`, `TestRELPShutdownReleasesAHeldTransaction`, `TestRELPIdleTimeout`, `TestRELPMalformedFrameClosesAfterTheAnswers`, `TestStreamCloseForceClosesAfterGrace`, `TestArmReadDeadlineLeavesTheWriteSide`, `TestReviewUnansweredReasonOnServerEnd`, `TestReviewReconciliationAfterAbort` | as decididas saem antes do `serverclose`; o `Close` só vence o prazo de leitura; `closeGrace`; os motivos de `relp_unanswered_total` e a reconciliação de §5.4 depois de um aborto |
| Conexões | `TestRELPPerIPLimit`, `TestRELPGlobalLimitParksAndCounts`, `TestConnLimitsAreSharedAcrossListeners`, `TestTCPListenersShareOneLimiter`, `TestBothCeilingsAreReleasedExactlyOnce` | tetos por transporte, não por listener |
| TLS e PROXY (§4.5, §4.6) | `tls_test.go`, `keyupdate_test.go`, `proxy_test.go` (inclusive `TestTLSSpeaksOnlyRELP`, `TestTLSBehindAProxy`, `TestProxyShutdownMidHeaderIsNotInvalid`) | mTLS, `peer-*`, o *flood* de `KeyUpdate` que não prende o `Close`, PROXY v1/v2 só de par confiável |

---

## 4. Fuzz (SPEC §18.3, obrigatório antes de release)

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=600s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=600s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$         -fuzztime=600s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=600s
```

| Alvo | O que prova | Rodada de 2026-10-11 (60 s por alvo) |
|---|---|---|
| `FuzzFrame` | o leitor RELP contra um oráculo independente (`refParse`): os mesmos frames, os mesmos dados cortados, o mesmo `DATALEN`, e o erro exatamente onde o oráculo para | 2,12 M execs, sem falha (outra rodada: 2,64 M) |
| `FuzzConvert` | a conversão nunca falha nem entra em pânico; toda saída passa no oráculo; a mesma entrada dá os mesmos bytes (§3.5) | 312 mil execs, sem falha |
| `FuzzSD` | STRUCTURED-DATA: tetos respeitados, nada perdido em `sd-invalid` | 2,51 M execs, sem falha |
| `FuzzProxy` | PROXY v1/v2: nunca aloca pelo declarado | 790 mil execs, sem falha |
| `FuzzHead` | o codec do cabeçalho do item recusa sem pânico, e o que aceita volta igual | 5,23 M execs, sem falha |
| `FuzzRead` | o leitor do dump nunca confia no comprimento declarado | 5,77 M execs, sem falha |
| `FuzzJSONBSafe` | a troca do `\u0000` para a `loghub.inbox` só troca escapes reais | 187 mil execs, sem falha |
| `FuzzInboxPayload` | o caminho real da `loghub.inbox`: JSON sem escape NUL, `raw` exato em base64 | 271 mil execs, sem falha |

Rodadas com a máquina carregada pelas outras suítes. As de 10 min por alvo não
foram feitas (lacuna 1).

---

## 5. Benchmarks

```bash
go test -run XXX -bench . -benchmem ./internal/convert ./internal/ratelimit ./internal/receiver
```

| Benchmark | Tempo | Alocação | Leitura |
|---|---|---|---|
| `RELPSession` | 2,06 µs/msg (**486 mil msg/s**, 80,7 MB/s) | 815 B, 12 allocs | uma sessão RELP de ponta a ponta sobre loopback: janela cheia, cada `200` só depois do ring em memória, ring drenado por um consumidor; linhas de ~166 B |
| `IngestAccept` | 133 ns/op | 280 B, 2 allocs | recepção até o ring (moldura já feita) |
| `Convert/rfc3164` | 756 ns/op (100 MB/s) | 0 B, 0 allocs | conversão + JLH completo, no buffer do pedaço |
| `Convert/rfc5424` | 1,04 µs/op (166 MB/s) | 0 B, 0 allocs | idem, com SD |
| `SchemaValidate/rfc5424` | 85 µs/op | 82 KiB, 1 922 allocs | o custo que D3 tira do caminho quente: ~80× a conversão |
| `AllowSameKey` | 143 ns/op | 0 | rate-limit, um emissor (contenção no shard) |
| `AllowSpreadKeys` | 44 ns/op | 8 B | rate-limit, emissores espalhados |

O `RELPSession` é a peça de recepção do primeiro critério de §18.8 (uma conexão,
janela 128): cerca de seis vezes o alvo de 80 mil msg/s, **sem** o sink e sem o
rsyslog do outro lado. Não substitui a medição ponta a ponta (lacuna 2). Medido
com a máquina sem outras suítes rodando, `-count=1`.

---

## 6. Integração em processo (`internal/app`)

```bash
go test -race ./internal/app/        # ~60 s sem destinos reais
go test -race -short ./internal/app/ # pula o teste do binário real
```

A app inteira sobe em processo (identidade fixa, `DATADIR` temporário, portas
efêmeras), fala RELP com o cliente de teste, e todo envelope lido de volta passa
pelo oráculo. Cobre:

- **Transportes:** RELP em claro e sobre TLS (com e sem certificado de
  cliente), vários listeners por transporte, PROXY v1/v2 de par confiável.
- **Fidelidade:** `raw` byte a byte com NUL e `\n` embutidos, truncamento acima
  de `syslog.max_message_bytes` com a sessão seguindo, vazias contadas com
  `200` e não escritas.
- **Garantia por ring:** com `ring=sqlite` o `200` vem depois do `COMMIT`; com
  `ring=none`, depois do destino, e a recusa `logic` é o `500` final.
- **Saturação:** ring cheio e sink DOWN seguram sem perder; o estouro de
  `backpressure_timeout` fecha a sessão sem resposta e a retransmissão entrega
  tudo; o teto de itens conta o lote retido.
- **Ciclo de vida:** recusas de boot com exit 1, sink inalcançável mantém os
  transportes fechados, shutdown gracioso entrega as decididas antes do
  `serverclose`, segundo sinal dá 130, `SIGHUP` conforme §16.3.
- **Durabilidade:** dump de emergência exato e reingerido por outra vida;
  `ClassFatal` no `Write` despeja e sai com 3; linha envenenada do `ring.db` vai
  para a quarentena.
- **Abortos** (`review_abort_test.go`): aborto entre o `Drain` e o dump conta
  tudo; depois de um dump salvo, nada; dump desligado conta uma vez;
  `SIGQUIT` tardio ainda aborta (SPEC §13.4).
- **Concorrência:** várias conexões RELP com `ring` memory, sqlite e none:
  aceitas = linhas, sem duplicata.
- **O binário real** (`TestBinaryIdentityExitCodes`,
  `TestBinarySignalsOutsideTheRunWindow`).

---

## 7. Destinos reais e emissores

Todas pulam sem a variável. Cada teste cria e remove o que usa (tabela, stream,
chave); os contêineres são descartáveis. A rodada final usou:

```bash
docker run --rm -d -p 15562:5432 -e POSTGRES_USER=sls -e POSTGRES_PASSWORD=sls -e POSTGRES_DB=sls_test --name rls-full-pg postgres:18-alpine
docker run --rm -d -p 13336:3306 -e MYSQL_ROOT_PASSWORD=sls -e MYSQL_DATABASE=sls_test --name rls-full-mysql mysql:8.4
docker run --rm -d -p 19030:9000 -e CLICKHOUSE_USER=sls -e CLICKHOUSE_PASSWORD=sls -e CLICKHOUSE_DB=sls_test -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 --ulimit nofile=262144:262144 --name rls-full-ch clickhouse/clickhouse-server:25.8
docker run --rm -d -p 16419:6379 --name rls-full-redis redis:7-alpine
docker run --rm -d -p 16420:6379 --name rls-full-redis-lru redis:7-alpine redis-server --maxmemory-policy allkeys-lru
```

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14282 &
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14283 &      # SEM JetStream
go run github.com/nats-io/nats-server/v2@v2.14.6 -c perm.conf &  # 14284, permissões
```

| Variável | Valor da rodada |
|---|---|
| `RELPLOG_TEST_PG_DSN` | `postgres://sls:sls@127.0.0.1:15562/sls_test?sslmode=disable` |
| `RELPLOG_TEST_MYSQL_DSN` | `root:sls@tcp(127.0.0.1:13336)/sls_test` |
| `RELPLOG_TEST_CLICKHOUSE_DSN` | `clickhouse://sls:sls@127.0.0.1:19030/sls_test` |
| `RELPLOG_TEST_REDIS_ADDR` | `127.0.0.1:16419` |
| `RELPLOG_TEST_REDIS_LRU_ADDR` | `127.0.0.1:16420`: um Redis com `allkeys-lru`, que a validação precisa recusar (a evicção apagaria o ring ou o stream em silêncio) |
| `RELPLOG_TEST_NATS_URL` | `nats://127.0.0.1:14282` (JetStream) |
| `RELPLOG_TEST_NATS_NOJS_URL` | `nats://127.0.0.1:14283` (sem JetStream: fatal na hora) |
| `RELPLOG_TEST_NATS_PERM_URL` | `nats://127.0.0.1:14284`, sem usuário na URL: usuários `admin`, `nopub`, `noinbox`, `nobeacon`; o `perm.conf` está no comentário de `natsPermSetup` em `internal/sink/review_nats_test.go` |
| `RELPLOG_TEST_EMITTERS` | `1` (Docker: constrói `rls-test-rsyslog:alpine-3.22`) |

### 7.1 Redis (`ring=redis`, `sink=redis`)

Script de enfileiramento com os dois tetos globais, `Commit` que só decrementa o
que o `XDEL` removeu, `XAUTOCLAIM` de réplica morta, `Touch`, `Quiesce`,
disponibilidade pela escrita, Redis Cluster sem *hash tag* recusado, política de
evicção recusada (`RELPLOG_TEST_REDIS_LRU_ADDR`); na app, duas instâncias num
ring, cada mensagem uma vez. A chave padrão é `relplog:ring` / grupo
`relplog-ring`, para nunca colidir com o S no mesmo Redis.

### 7.2 PostgreSQL (`loghub.inbox`)

PostgreSQL 18. A fila nasce de `docker/postgres/inbox.sql` num esquema próprio
por teste: `unnest` dos arrays, `ON CONFLICT (uuid)`, as CHECKs da skill
aceitando tudo o que o servidor produz, o envelope reconstruído da linha igual
ao JLH, a classificação por SQLSTATE e a validação do boot (tipos, índice único
exatamente sobre `(uuid)`, nenhum outro único, particionada e `LATIN1`
recusadas, `RULE` de `INSERT`/`UPDATE` recusada no boot e `0A000` fatal depois —
`TestPostgresRulesAreRefusedAtBoot`). A checagem de saúde na conexão própria
(`TestPostgresHealthProbeOutsideThePool`, com `pool = workers` e todas as
conexões presas atrás de um `LOCK TABLE`) e o redial depois de um
`pg_terminate_backend` (`TestPostgresHealthProbeRedials`). **Não provado:** o FATAL de um servidor em recuperação exige uma
réplica.

### 7.3 MySQL

MySQL 8.4: `ON DUPLICATE KEY UPDATE`, a divisão em comandos por
`max_allowed_packet`, a sonda revertida (qualquer recusa é fatal), `read_only`
fatal no boot e DOWN depois, entrega parcial, e as formas de tabela que
perderiam em silêncio (`TestMySQLValidationRefusesSilentLossShapes`). Desde a
revisão de 2026-10-11 a sonda usa `tcp` e um `src_ip` presente: `src_ip NOT
NULL`, `transport CHAR(3)` e `CHECK (transport IN ('tcp','tls'))` são aceitos;
`CHAR(2)` é recusado. `1203`/`1226`/`1461` são `network`. Desde 2026-10-13:
`1048` depois do boot é fatal (`TestMySQLNotNullAfterBootIsFatal`); um banco de
nome UTF-8 de 26 caracteres e 66 bytes é aceito e um de 65 caracteres recusado
(`TestMySQLMultibyteDatabaseName`); a sonda na conexão própria
(`TestMySQLHealthProbeOutsideThePool`, atrás de um `LOCK TABLES ... WRITE`); e os
testes de `autocommit` **sem `SET GLOBAL autocommit`**: um usuário do teste e um
`init_connect` condicional a ele, com o controle de que a sessão dele nasce com
`autocommit=0` (`autocommitOffUser`).

### 7.4 NATS

nats-server 2.14.6: o subject `loghub.{space}.syslog` (stream `LOGHUB_SYSLOG`,
o mesmo do S), `Nats-Msg-Id = <uuid>.0`, os `X-LogHub-*`, a validação do stream
(inclusive `discard=new` com `max_bytes` abaixo de um registro, recusado), o
`10077` de stream cheio ou fechando como `network`, JetStream desligado fatal,
entrega parcial, beacon em `loghub._ctl.beacon`, permissões negadas (com
`RELPLOG_TEST_NATS_PERM_URL`), a negação mascarada do `mode=core`
(`TestMaskedDenial`, com o `natsFake` do próprio teste), queda e volta por proxy
sem perda, e FATAL → dump → reingestão. Contra o `natsFake` do teste (que fala TLS com um
certificado gerado na hora): o veredito do TLS numa reconexão a um servidor não
confiável, a recusa no boot, e **um nó não confiável num pool não é veredito**
— por `sink.nats.urls` e por `connect_urls` anunciados —, o `Health` dizendo o
motivo e o cliente voltando ao confiável (`TestNATSOneUntrustedServerOfAPoolIsNoVerdict`,
`TestNATSBootTLSRefusalOfAPoolIsNetwork`); o flush `core` cortado por uma
reconexão é `network` e o que atravessa uma reconexão não confirma; o `10077`
com o texto real do nats-server 2.14.6, "store is closed"
(`TestNATSStoreIsClosedIsNetwork`).

### 7.5 ClickHouse (`inbox_syslog`)

ClickHouse 25.8, protocolo nativo: ida e volta pelo oráculo, token de
idempotência com prefixo `rls-` (`rls-probe` na sonda), as 32 tabelas de
validação, inferência de datas desligada, privilégios e memória (com
`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`), os quatro rings ponta a ponta e o
destino caindo e voltando sem perda. A sonda em cliente próprio
(`TestClickHouseHealthProbeOutsideThePool`, com uma view materializada que dorme
segurando os `INSERT`), a conta de memória pelos escritores
(`TestClickHouseMemoryCountsTheWriters`) e um banco de nome UTF-8 de 26
caracteres (`TestClickHouseMultibyteDatabaseName`). **Achado no caminho:** o
ClickHouse 25.8 cria um banco de 25 caracteres CJK, mas não consegue dropá-lo —
o nome vai percentual-codificado (9 bytes por caractere) num nome de arquivo
de `metadata_dropped` que passa de 255 bytes. Não afeta este servidor, que
nunca cria nem dropa bancos; o teste usa 20 CJK e seis dígitos.

### 7.6 Emissores reais: rsyslog com `omrelp` (SPEC §18.7)

```bash
RELPLOG_TEST_EMITTERS=1 go test -race -run EmittersRELP -timeout 900s ./internal/app/
```

O rsyslog 8.2410 com `librelp` 1.11 do Alpine 3.22, num contêiner, contra o
servidor do processo de teste. Só a facility `local0` vai pela ação RELP (as
mensagens do próprio `rsyslogd` não poluem a contagem), e o emissor continua
mandando *ticks* durante as esperas: o `librelp` só percebe uma sessão fechada
quando envia de novo (SPEC §5.8).

| Teste | Cenário | Resultado |
|---|---|---|
| `TestEmittersRELPInClearAndOverTLS` | 200 linhas em claro e 200 sobre TLS com certificado de cliente | todas, uma vez cada, `payload.tls` com o fingerprint |
| `TestEmittersRELPFiveHundredIsFinal` | `ring=none` com um destino que recusa a 3ª linha como `logic` | a recusada chega uma vez e nunca volta; o `500` suspende a ação e as vizinhas já aceitas voltam como duplicatas (5–6 na janela de 128) — a observação que levou à decisão 8a |
| `TestEmittersRELPBackpressureRetransmits` | ring cheio além de `backpressure_timeout` | a sessão fecha sem resposta, o rsyslog reconecta e retransmite: todas as linhas, 0 duplicatas |
| `TestEmittersRELPSurvivesARestart` | o servidor para com transações em voo e outro sobe | todas as linhas, 0 duplicatas |

---

## 8. Verificação independente por área

Cada área foi entregue e depois revisada por um agente **só leitura**, que
reproduz antes de afirmar; cada achado corrigido ganhou teste de regressão,
conferido por mutação (a correção desfeita faz o teste falhar).

| Área (2026-10-11) | Achados corrigidos | Destaques |
|---|---|---|
| RELP (`receiver`) | 6 | retentativa que acordava depois do aborto e ainda enfileirava (duplicata); teto em voo que contava só a cópia (5× o teto com linhas pequenas); frame lido e não despachado fora da reconciliação; motivo `server_closed` ausente; checagens de contexto depois de cada espera |
| App e shutdown | 4 | aborto entre o `Drain` e o dump não contava nada; dump falho contado duas vezes; `SIGQUIT` tardio perdido; transportes fechados em sequência (um `closeGrace` por transporte) |
| Config e dump | 45 mutantes mortos | cauda de valor partida pelo shell ecoada no erro; `@` depois de `/`, `?`, `#` numa URL; fragmento em URL PostgreSQL; `Remove` do dump sem `fsync` do diretório |
| Sinks | 25 mutantes NATS, 255 testes SQL/Redis | `10077` de stream cheio tratado como fatal; `discard=new` abaixo de um registro; negação mascarada no `mode=core`; sonda MySQL com `unix`/`NULL` recusando tabela legítima; `1203`/`1226`/`1461`; prazo dos health checks |
| Docs e Docker | ~30 | referências de seção, listas de `500`, portas TLS, versões |

---

## 9. Resultado da rodada final (2026-10-12)

| Suíte | Resultado |
|---|---|
| `go test -race -count=1 ./...` com PostgreSQL 18, MySQL 8.4, ClickHouse 25.8, Redis 7 (dois, um com `allkeys-lru`), nats-server 2.14.6 (JetStream, sem JetStream, com permissões) e o rsyslog 8.2410 | 16 pacotes `ok`, 773 testes e subtestes, **1 pulado** (`TestGoldenVectors/arquivo-sem-pri`: `transport=file`, fora por D5) |
| `TestEmittersRELP*` (rsyslog real) | os quatro cenários de §7.6 verdes |
| `gofmt -l .`, `go vet ./...` | limpos |

**Rodada de 2026-10-13** (porte de §9.1), duas vezes seguidas, com PostgreSQL 18
(`max_connections=300`), MySQL 8.4, ClickHouse 25.8, dois Redis 7 (um
`allkeys-lru`), nats-server 2.14.6 (JetStream, sem JetStream, com permissões) e
`RELPLOG_TEST_EMITTERS=1` (rsyslog real): `go test -race -count=1 ./...` com os
16 pacotes `ok`, **699 testes de primeiro nível + 441 subtestes, 1 pulado** (o
mesmo vetor `arquivo-sem-pri`, D5); os quatro `TestEmittersRELP*` verdes. Uma
rodada anterior pegou um teste novo instável
(`TestNATSBootTLSRefusalOfAPoolIsNetwork`: o `nats.go` embaralha o pool a cada
`Connect`, e cinco tentativas não bastavam), corrigido antes das duas. Fuzz de
60 s nos alvos cujo código mudou: `FuzzFrame` 1,64 M execs e `FuzzRead` 7,75 M,
sem falha. `gofmt -l .` e `go vet ./...` limpos.

### 9.1 Porte das correções dos cortes e do S (2026-10-13)

Cada item com teste que falhava antes (rodado contra o código anterior) e
conferido por mutação (a correção desfeita derruba o teste). Os testes estão em
`internal/*/review_port*_test.go`, `internal/emergency/durable_test.go`,
`internal/app/review_{abort,cut,rn}_test.go` (portados do RN, com um `fixture`
mínimo em `review_port_helpers_test.go`) e nos arquivos indicados.

| Item | Teste | Mutantes |
|---|---|---|
| `ErrNotDurable` na abertura do diretório; `SetAbandon` (antes de publicar e a cada 1024) | `TestRemoveWithAnUnopenableDirectoryIsNotDurable`, `TestWriteGivesUpWhenAbandoned`, `TestWriteChecksTheAbandonWhileWriting` | 3 mortos |
| Posse do aborto (`lossMu`, `enterDumpPhase`, `oweDump`, `settleDump` — X2 —, uma vez só), `SetAbandon` ligado, `quitNow` | `TestAbort*`, `TestDumpAfterTheAbortIsAbandoned`, `TestSIGQUITAfterTheEntryCheckStillAborts`, `TestAbortInsideTheDumpKeepsItsReason`, `settle` (nenhum dump abortado chega ao disco) | 6 mortos |
| Bind cancelado = 130 (admin, transportes) e X1 (literal IP) | `TestCancelledBindIsAnAbortNotAConfigError`, `TestSignalBeforeTheBindExits130`, `TestSignalBeforeALiteralBindExits130` | 3 mortos |
| A3: `ring=sqlite` + `sink=none` guarda o backlog; `ring=redis` + `sink=none` avisa | `TestSinkNoneKeepsAPreviousLifesRingRows`, `TestRedisRingWithSinkNoneWarns` | 2 mortos (+1 contado na config) |
| A4: aborto com ring persistente não conta nada (sqlite e redis) | `TestAbortWithAPersistentRingCountsNothing` | 1 morto |
| `MaxPending` 2× e limitado | `TestNATSMaxPendingIsDoubledAndBounded` | 1 morto (o do teto só pela compilação) |
| `FullFor` (dois tetos, ring fechado) e o contexto antes do `Enqueue` | `TestIngest{FullRing,ByteCeiling}IsRefusedBeforeTheCopy`, `TestIngestClosedFullRingIsTheShutdown`, `TestIngestEndedSessionIsNotEnqueued` | 4 mortos |
| E2: buffer do leitor solto só com o emissor parado | `TestRELPReaderKeepsTheBufferWhileFramesFollow`, `TestRELPReaderReleasesALargeBuffer` (ajustado), `BenchmarkRELPReaderFrames` | 2 mortos |
| `Deliver` com `logic` antes do `ctx` | `TestDeliverLogicRefusalWinsOverTheCancellation` | falhava antes |
| PostgreSQL `RULE`/`0A000` | `TestPostgresRulesAreRefusedAtBoot`, `TestClassifySQLState` | 2 mortos |
| D1: sonda fora do pool (PG, MySQL, ClickHouse) | `Test{Postgres,MySQL,ClickHouse}HealthProbeOutsideThePool`, `TestPostgresHealthProbeRedials` | 3 mortos; **1 sobrevivente equivalente**: tirar o `dropProbe` depois de um erro do PG — o pgx fecha sozinho a conexão nos erros que importam, e o `IsClosed` a redisca |
| D2/B3: memória do ClickHouse pelos escritores | `TestClickHouseMemoryCountsTheWriters`, `TestSinkWriters` | 2 mortos (+1 contado na config) |
| X5: MySQL `1048` fatal; nomes de banco por caracteres | `TestMySQLNotNullAfterBootIsFatal`, `TestMySQL/ClickHouseMultibyteDatabaseName`, `TestClassifyMySQLNumber` | 2 mortos |
| D5: `autocommit` sem `SET GLOBAL` | `TestMySQLWritesCommitUnderAGlobalAutocommitOff`, `TestMySQLValidationRefusesASessionWithoutAutocommit` | 1 morto |
| NATS: veredito do TLS, `flushErr`, contagem de reconexões (do S) | `TestNATSReconnectToAnUntrustedTLSServerIsAVerdict`, `TestNATSBootTLSRefusalIsFatal`, `TestNATSCoreFlushCutByAReconnectionIsNetwork`, `TestNATSCoreFlushAcrossAReconnectionIsNotConfirmed` | os quatro falhavam antes |
| C1: TLS de um pool; X3: "store is closed" | `TestNATSOneUntrustedServerOfAPoolIsNoVerdict` (urls e anunciados), `TestNATSBootTLSRefusalOfAPoolIsNetwork`, `TestNATSStoreIsClosedIsNetwork` | 4 mortos |
| Config: valor colado, continuação de segredo, switches, `--config=` vazio, BOM, tetos (B1, B2, C3), pool dos escritores, regras do `addr` e da URL PostgreSQL (B4, B5), vírgula na URL Redis (B6), derivados comentados, sinônimos de charset | `internal/config/port_config_test.go` (18 testes, todos falhavam antes) | 31 mortos |

Total: 67 mutantes mortos (mais um morto só pela compilação), 1 sobrevivente equivalente.

---

## 10. Lacunas conhecidas

Registradas por honestidade, não por esquecimento.

1. **Fuzz de 10 min por alvo ainda não rodou.** As rodadas foram de 60 s.
2. **Os critérios de carga nunca foram medidos** ponta a ponta com o rsyslog: há
   o `BenchmarkRELPSession` (uma sessão, ring em memória), não um gerador de
   carga com várias ações `omrelp`.
3. **`kill -9` de verdade** não é testável em processo. A durabilidade do
   `ring=sqlite` é provada por fechamento e reabertura e pela leitura do
   `ring.db` no instante da confirmação.
4. **Outros clientes RELP** (o `imrelp` do próprio rsyslog como cliente não
   existe; `relp` em Java, Python ou o do NXLog) não foram testados: só o
   `librelp` 1.11 do rsyslog 8.2410.
5. **Retransmissão depois de um `500` com `ring` diferente de `none`** não tem
   cenário real (nenhum outro caminho responde `500` no fluxo normal).
6. **O RELP sobre TLS do `librelp` com `tls.lib="openssl"`** não foi testado; a
   suíte usa o padrão do pacote do Alpine.
7. **`SO_REUSEPORT`** só é testado no que é portável: que os sockets abrem.
8. **Réplicas com config de conversão diferente** no mesmo ring Redis produzem
   bytes diferentes para o mesmo `uuid` (§3.5); nenhum teste prova a exigência.
9. **A regra do `/data`** não tem teste automatizado de contêiner com raiz
   somente leitura.
10. **`sink=clickhouse`**: TLS, `242`/réplicas (exige Keeper) e os perfis de
    `async_insert` não são exercitados.
11. **Envio tardio de um comando cancelado** (rueidis, NATS) só é provado pela
    decisão de quem chamou, não contra um servidor que trave no meio do comando.
12. **A regra do pool no `ReconnectErrHandler`** do NATS (veredito do TLS só com
    um servidor) não tem teste: o `nats.go` 1.x nunca chama esse handler com uma
    recusa de TLS (só com a falha da discagem TCP). Ela fica para uma versão do
    cliente que relate ali; o caminho real, pelo `LastError`, é provado.
13. **O veredito do TLS contra um nats-server de verdade** com TLS não roda na
    suíte (os testes usam o `natsFake` com TLS); a revisão do S o reproduziu com
    o binário 2.14.6, e a regra é a mesma.
14. **A ligação app → `ClickHouseConfig.Writers`** é provada por partes
    (`TestSinkWriters` na config, `TestClickHouseMemoryCountsTheWriters` no
    sink), não por um boot contra um usuário com `max_memory_usage_for_user`
    exato.

---

## 11. Histórico: bugs que os testes e as revisões encontraram

Os bugs herdados e corrigidos no S estão no `docs/TESTING.md` dele. Aqui, só os
desta reescrita:

| Achado por | Bug | Lição |
|---|---|---|
| leitura do `librelp` (`scrsp.c`) | a especificação extraída respondia `500 ring full` e `500 shutdown`: o `librelp` destrói o que recebe `500` | ler o código do cliente antes de escolher o código de resposta |
| leitura do `librelp` (`relpframe.c`) | `0 serverclose 0 \n` era frame inválido para o cliente | o servidor escreve a grafia estrita; o leitor aceita as duas |
| rsyslog real | um `500` quebra a sessão e duplica as vizinhas da janela | rate-limit segura em vez de recusar (decisão 8a) |
| rsyslog real | o `librelp` só reconecta quando envia de novo | o teste mantém o emissor enviando; a SPEC registra o comportamento |
| revisão de RELP | retentativa depois do aborto enfileirava o que o `librelp` retransmitiria | conferir o contexto da sessão depois de toda espera |
| revisão de RELP | o teto em voo contava só a cópia | contar o que a transação segura (`txnCharge`) |
| revisão de app | aborto e dump contavam a mesma perda zero ou duas vezes | um dono só, com `Swap` |
| revisão de config | cauda de senha partida pelo shell ecoada no erro | nada depois de um valor é citado |
| revisão de sinks | `10077` de stream cheio era fatal (dump, exit 3, duplicatas na reingestão) | ler a descrição do erro do store, não só o código |
| revisão de sinks | a sonda do MySQL com `unix` e `src_ip NULL` recusava tabela legítima | sondar com o que este servidor escreve de fato |
