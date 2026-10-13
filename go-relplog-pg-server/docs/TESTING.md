# go-relplog-pg-server — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Os
números são de 2026-10-13, num Apple M2 (8 núcleos) com macOS, salvo onde
dito. Comandos a partir da pasta do módulo.

A base de testes veio dos dois pais e foi fundida: a do RELP (o cliente RELP de
teste, `internal/receiver/relpclient_test.go`, as suítes de janela, teto,
backpressure e fim de sessão, os abortos da app e a suíte com o rsyslog real)
é a do `go-relplog-server` (R), sem o TLS e sem os outros rings e destinos; a do
corte (o harness do PostgreSQL, a fila de `docker/postgres/inbox.sql`, o dump
JSON Lines, `ParseLine`, a reingestão em fluxo, `removed.go`) é a do
`go-syslog-pg-server` (SP), sem UDP, unix e as molduras da RFC 6587.

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. Sem `RELPLOG_TEST_PG_DSN`, tudo o que passa do passo
7 do boot pula (o PostgreSQL é o único destino); com ela e com
`RELPLOG_TEST_EMITTERS=1`, a suíte inteira leva cerca de 52 s (rodada final de
2026-10-13).

Cobertura por pacote (rodada final):

Medida em 2026-10-13, na rodada final, com
`go test -race -count=1 -coverpkg=./... -coverprofile=c.out ./...` e o
PostgreSQL 18 de §7: cada linha é a fração das instruções **do pacote** que
alguma suíte do módulo executou (a da app exercita os outros; um bloco conta
uma vez, coberto se qualquer suíte o cobriu).

| Pacote | Instruções cobertas | Cobertura |
|---|---|---|
| `internal/app` | 530 / 640 | 82,8 % |
| `internal/config` | 858 / 926 | 92,7 % |
| `internal/convert` | 772 / 851 | 90,7 % |
| `internal/emergency` | 324 / 358 | 90,5 % |
| `internal/ident` | 34 / 42 | 81,0 % |
| `internal/item` | 68 / 78 | 87,2 % |
| `internal/jlh` | 90 / 95 | 94,7 % |
| `internal/jlhcheck` (só em teste) | 153 / 195 | 78,5 % |
| `internal/obs` | 319 / 337 | 94,7 % |
| `internal/receiver` | 901 / 948 | 95,0 % |
| `internal/ring` | 73 / 84 | 86,9 % |
| `internal/sink` | 849 / 898 | 94,5 % |
| `cmd/go-relplog-pg-server` | 0 / 6 | 0 % (o `main` roda no `binary_test.go` como processo à parte, fora do perfil) |
| **módulo** | **4 971 / 5 458** | **91,1 %** |

---

## 2. Vetores dourados, contratos e corpus (desvio D3, SPEC §18.2)

```bash
go test -race -run 'Golden|Schema|Corpus|Semantic' -v ./internal/convert/ ./internal/jlhcheck/
```

- **Os 24 vetores** de `loghub-type-syslog-schema`
  (`internal/jlhcheck/loghub/vectors.json`): cada entrada é convertida e
  comparada com o esperado (o `uuid` só pelo instante), e cada envelope
  esperado passa no oráculo. O vetor `transport=tls` roda **reetiquetado como
  TCP**, com `payload.tls` retirado e o `sid` recalculado (molde do SP); o
  `arquivo-sem-pri` pula (`transport=file`, D5).
- **O oráculo** (`internal/jlhcheck`) é o porte de Go do `validate_payload.py`
  da skill: os dois schemas (`santhosh-tekuri/jsonschema/v6`, só em teste) mais
  as regras semânticas.
- **`x-transport: "relp"`** (D9), `transport: "tcp"` e `framing:
  "octet-counting"` estão em todo envelope que a app produz: o `mustJLH` do
  harness da app confere os três em toda linha lida da fila.
- **Corpus de linhas reais** (`internal/convert/testdata/corpus/`, 23 pares).

---

## 3. RELP (`internal/receiver`, SPEC §5)

```bash
go test -race -v ./internal/receiver/
```

O pacote inteiro: a tabela abaixo cita testes de vários prefixos (`RELP`,
`Review`, `Ingest`, `Proxy`, `Stream`...), e um `-run` por prefixo deixava
onze deles de fora.

Os testes conversam com o servidor por um cliente RELP de teste sobre um socket
real, com um ring de teste que deixa segurar e soltar linhas pelo conteúdo.

| Grupo | Testes | O que prova |
|---|---|---|
| Moldura (§5.1–§5.3) | `TestRELPFrames`, `TestRELPZeroLengthBothSpellings`, `TestRELPHeaderAndTrailerAreNotTheMessage`, `TestRELPMalformed`, `TestRELPFrameCeiling`, `TestRELPTruncationKeepsSync`, `TestRELPFrameGrowsWithTheBytesNotTheCount`, `TestRELPReaderReleasesALargeBuffer`, `TestAppendRSPAndServerClose`, `FuzzFrame` | as duas grafias de `DATALEN = 0`; o fusível antes de ler o corpo; o truncamento que mantém a sincronia; `0 serverclose 0\n` |
| Diálogo (§5.4) | `TestRELPSessionHappyPath`, `TestRELPOpenAdvertisesTheSoftware`, `TestOpenResponseOffers`, `TestRELPCommandOrder`, `TestRELPUnknownCommandKeepsTheConnection`, `TestRELPCloseWaitsForTransactionsInFlight`, `TestRELPEmptyIsAcknowledged` | `relp_software=go-relplog-pg-server,...`; `syslog` antes do `open` e o segundo `open`; o `close` que espera as transações em voo; `200` para a vazia |
| Garantia | `TestRELPAcknowledgesOnlyAfterTheRing`, `TestEndToEndEnvelopesAreValidJLH` | nenhum `200` antes do ring |
| Janela e teto (§5.5, §5.6) | `TestRELPWindowAllowsConcurrentTransactions`, `TestRELPWindowStopsTheReader`, `TestRELPInflightBudgetIsGlobal`, `TestBudgetAcquireRelease`, `TestTxnChargeCoversTheTransaction`, `TestCopyComesAfterTheWindowAndTheBudget` | um teto para o processo inteiro; a conta `2 × classe + 8 KiB` (RP-7); a cópia só depois da vaga e do teto (invariante 24) |
| Backpressure (§5.7) | `TestRELPFullRingIsHeldNotRefused`, `TestRELPBackpressureTimeoutClosesWithoutAnswering`, `TestRELPSinkDownIsHeld`, `TestRELPUnexpectedRingErrorIsHeldNotRefused`, `TestRELPVanishedPeerAbandonsHeldTransactions`, `TestReviewRetryAfterPeerLeft`, `TestIngestFullRingIsRefusedBeforeTheCopy`, `TestIngestByteCeilingIsRefusedBeforeTheCopy`, `TestIngestByteCeilingChargesTheTruncatedLine`, `TestIngestSinkDownFailsBeforeTheCopy`, `TestRetryDoesNotRecount`, `TestIngestEndedSessionIsNotEnqueued`, `TestDispatchAfterSessionEndStartsNothing` | segurar, nunca `500` (inclusive o erro inesperado do ring, RP-6); o estouro fecha sem resposta; a pré-conferência dos dois tetos sem `uuid` nem cópia; o fim da sessão vence até o `Enqueue` |
| Fim de sessão (§5.8) | `TestRELPShutdownWithTransactionsInFlight`, `TestRELPShutdownReleasesAHeldTransaction`, `TestRELPIdleTimeout`, `TestRELPMalformedFrameClosesAfterTheAnswers`, `TestStreamCloseForceClosesAfterGrace`, `TestArmReadDeadlineLeavesTheWriteSide`, `TestReviewUnansweredReasonOnServerEnd`, `TestSecondOpenLeavesItsHeldTransactionServerClosed`, `TestReviewReconciliationAfterAbort` | as decididas saem antes do `serverclose`; `closeGrace`; os motivos de `relp_unanswered_total` (o segundo `open` inclusive) e a reconciliação de §5.4 |
| Conexões | `TestRELPPerIPLimit`, `TestRELPGlobalLimitParksAndCounts`, `TestConnLimitsAreSharedAcrossListeners`, `TestTCPListenersShareOneLimiter`, `TestBothCeilingsAreReleasedExactlyOnce` | tetos por transporte, não por listener |
| PROXY (§4.6) | `proxy_test.go`, inclusive `TestProxyShutdownMidHeaderIsNotInvalid` e `TestProxyStalledHeaderIsInvalid` | v1/v2 só de par confiável; o shutdown no meio do cabeçalho não culpa o balanceador |

### 3.1 Sink (`internal/sink`, SPEC §10)

```bash
go test -race -v ./internal/sink/                                  # sem servidor
RELPLOG_TEST_PG_DSN='...' go test -race -v ./internal/sink/         # com o PostgreSQL 18
```

| Grupo | Testes | O que prova |
|---|---|---|
| Workers e pedaços (§10.1, §15.3) | `workers_test.go`, `TestWorkersOversizedJLHTravelsAlone`, `TestTaintedChunkBufferIsNotPooled`, `TestOversizedChunkBufferIsNotPooled`, `TestSealLimitsEachEnvelopeToItsBytes`, `TestResetClearsTheEnvelopeSlots`, `TestWorstCaseJLHIsBounded` | um JLH maior que o teto vai sozinho; buffer manchado ou grande demais nunca volta ao pool; cada JLH com a capacidade do próprio tamanho; o pior JLH de §3.9 (com `x-transport`) dentro de `12 × L + 8 KiB` |
| Entrega no boot (§10.2, §12.2) | `TestDeliverChunksByBatch`, `TestDeliverChunksByBatchMaxBytes`, `TestDeliverLogicRefusalCountsAndMovesOn`, `TestDeliverLogicRefusalWinsOverTheCancellation`, `TestDeliverRetriesNetworkThroughWait`, `TestDeliverFatalReturnsAtOnceAndMarksFatal` | as regras por classe pedaço a pedaço; a recusa `logic` logada com os `uuid` mesmo com o contexto cancelado |
| Classificação (§10.5) | `TestClassifySQLState` | a tabela de SQLSTATE, `0A000` e `23502` fatais |
| Saúde (§13.3) | `health_test.go`, `TestHealthChecksCarryTheirLifetime` | a máquina de estados, FATAL terminal, o tempo de vida das checagens |
| PostgreSQL real (§10.5) | `TestPostgresHealthValidatesTheSchema`, `TestPostgresArbiterIndexRules`, `TestPostgresRulesAreRefusedAtBoot`, `TestPostgresRecoveryIsFatalAtBootAndDownAfter`, `TestPostgresHealthHasAConnectionWhileTheWorkersWrite`, `TestPostgresLogicRefusalSparesTheNeighbours`, e os de §7.1 | a validação do boot (índice árbitro com `indimmediate`, `indisvalid`, `indnkeyatts`; o extra só se pronto; regras), a réplica (FATAL no boot, DOWN depois, com um `pg_is_in_recovery()` falso num banco próprio), a conexão da sonda, a recusa `logic` que só custa o próprio pedaço |

---

## 4. Fuzz (SPEC §18.3, obrigatório antes de release)

Os comandos estão em SPEC §18.3 (10 min por alvo) e em `AGENTS.md` §7 (60 s).

| Alvo | O que prova | Rodada de 2026-10-13 (60 s por alvo; os da revisão, nos pacotes tocados, na última coluna) |
|---|---|---|
| `FuzzConvert` | a conversão nunca falha nem entra em pânico; toda saída passa no oráculo; a mesma entrada dá os mesmos bytes (§3.5) | 359 mil execs, sem falha |
| `FuzzSD` | STRUCTURED-DATA: tetos respeitados, nada perdido em `sd-invalid` | 5,15 M execs, sem falha |
| `FuzzFrame` | o leitor RELP contra um oráculo independente (`refParse`): os mesmos frames, os mesmos dados cortados, o mesmo `DATALEN`, e o erro exatamente onde o oráculo para | 6,98 M execs, sem falha; revisão: 6,12 M, sem falha |
| `FuzzProxy` | PROXY v1/v2: nunca aloca pelo declarado | 2,33 M execs, sem falha; revisão: 6,06 M, sem falha (com `-fuzzminimizetime=1s`, abaixo) |
| `FuzzRead` | o leitor do dump JSON Lines tolera truncamento e lixo; nada acima do teto; o que `ParseLine` aceita a fila aceitaria | 287 mil execs, sem falha; revisão: 238 mil, sem falha |
| `FuzzJSONBSafe` | a troca do `\u0000` para a `loghub.inbox` só troca escapes reais | 271 mil execs, sem falha; revisão: 1,07 M, sem falha |
| `FuzzInboxPayload` | o caminho real da `loghub.inbox`: JSON sem escape NUL, `raw` exato em base64 | 153 mil execs, sem falha; revisão: 331 mil, sem falha |

Rodadas com a máquina carregada pelas revisões independentes. As de 10 min por
alvo não foram feitas (§8, item 1). Na rodada da revisão, o `FuzzProxy` com o
padrão de `-fuzzminimizetime` (60 s) parou em 15–26 mil execs: depois do
primeiro achado "interessante", a minimização ocupou o minuto inteiro com
0 execs/s contados. Não é um laço no alvo (`ReadProxyHeader` é limitado, e com
`-fuzzminimizetime=1s` o mesmo alvo roda a ~100 mil execs/s); a rodada que
conta usou `-fuzzminimizetime=1s`.

---

## 5. Benchmarks

```bash
go test -run XXX -bench . -benchmem ./internal/convert ./internal/receiver
```

| Benchmark | Tempo | Alocação | Leitura |
|---|---|---|---|
| `RELPSession` | 1,69 µs/msg (**591 mil msg/s**, 98 MB/s) | 769 B, 11 allocs | uma sessão RELP de ponta a ponta sobre loopback: janela cheia, cada `200` só depois do ring em memória, ring drenado por um consumidor (R: 486 mil msg/s, 815 B, 12 allocs) |
| `IngestAccept` | 101 ns/op | 224 B, 1 alloc | recepção até o ring (moldura já feita); a caixa do buffer do SP tirou uma alocação (R: 133 ns, 2 allocs). A pré-checagem dos dois tetos e o contexto antes do `Enqueue` (revisão) não mudaram o custo |
| `Convert/rfc3164` | 740 ns/op (103 MB/s) | 0 B, 0 allocs | conversão + JLH completo, no buffer do pedaço |
| `Convert/rfc5424` | 1,04 µs/op (166 MB/s) | 0 B, 0 allocs | idem, com SD |
| `SchemaValidate/rfc5424` | 83 µs/op | 82 KiB, 1 924 allocs | o custo que D3 tira do caminho quente: ~80× a conversão |
| `RELPReaderFrames/200`, `/12288`, `/24576`, `/61440` | 84 ns, 350 ns, 520 ns, 1,14 µs | 0 B, 0 allocs em todos | frames `syslog` seguidos pelo `bufio.Reader` de 4 KiB da sessão. Antes da correção E2 (§9.4) o buffer acima de 16 KiB era solto antes de todo frame: `/24576` 12,3 µs, 55 KB, 5 allocs; `/61440` 37,7 µs, 129 KB, 6 allocs |

Medido em 2026-10-13, na rodada final da revisão independente, com
`-count=1` (a rodada anterior, com as revisões rodando na mesma máquina, deu
532 mil msg/s e 108 ns).

O `RELPSession` é a peça de recepção do primeiro critério de §18.8 (uma conexão,
janela 128), **sem** o sink e sem o rsyslog do outro lado. Não substitui a
medição ponta a ponta (§8, item 2).

---

## 6. Integração em processo (`internal/app`)

```bash
RELPLOG_TEST_PG_DSN='...' go test -race ./internal/app/
go test -race -short ./internal/app/   # pula o teste do binário real
```

A app inteira sobe em processo (identidade fixa, `DATADIR` temporário, portas
efêmeras, uma fila própria por teste num esquema próprio), fala RELP com o
cliente de teste, e todo envelope lido de volta da fila passa pelo oráculo.
Cobre:

- **Transporte:** RELP em claro até a fila, com as colunas e os endereços
  (`TestAllTransportsReachTheQueue`), e PROXY v1 e v2 de par confiável
  (`integration_test.go`); o PROXY v1 com TCP6 também no e2e do `receiver`.
- **Fidelidade:** `raw` byte a byte com NUL e `\n` embutidos, colunas e
  `event_at` da recepção, vazias contadas com `200` e não escritas.
- **Saturação:** ring cheio e sink DOWN seguram sem perder e sem `500`
  (`TestSinkDownHoldsTheTransaction`, os testes de `saturation_test.go` e o
  DOWN de `destinations_test.go`, que confere as respostas RELP); o estouro de
  `backpressure_timeout` fecha a sessão sem resposta.
- **Ciclo de vida:** recusas de boot com exit 1 (chaves do pai na linha de
  comando, nenhum transporte), as do env com `WARN`; banco inalcançável mantém o
  RELP fechado; shutdown gracioso entrega as decididas antes do `serverclose`;
  segundo sinal dá 130; `SIGHUP` de `[log]` e o arquivo inválido
  (`signals_test.go`); o log da recusa fatal no boot nomeia a causa.
- **Durabilidade:** dump de emergência exato e reingerido por outra vida (mesmo
  `uuid`, mesmo JLH, o `from` de quem recebeu); dump danificado em `.corrupt`;
  gêmeo de um dump já ligado removido e o interrompido informado; gauges
  recontados do disco (`TestEmergencyGaugesRecountTheDiskAfterReingestion`);
  `Remove` e `Quarantine` sem `fsync` relatados como feitos
  (`TestQuarantineAndRemovalReportTheirOutcome`).
- **Abortos** (`review_abort_test.go`, `review_shutdown_test.go`,
  `recovery_test.go`): aborto entre o `Drain` e o dump conta tudo; depois de um
  dump salvo, nada; dump desligado e dump falho contam uma vez; aborto antes da
  fase de dump, durante o recolhimento e durante a escrita contam uma vez e não
  publicam dump (o `gracefulShutdown` real, com o gancho `beforeCollect`); com
  um worker só, o dump com itens drenados do ring
  (`TestAbortDuringTheDumpWithItemsDrainedFromTheRing`); `SIGQUIT` tardio ainda
  aborta, inclusive visto depois da conferência; `SIGQUIT` com o servidor no ar
  conta o ring; `SIGQUIT` nunca é tolerado no shutdown por FATAL
  (`TestFatalShutdownSignals`); o *bind* cancelado por sinal é 130.
- **Reingestão** (`review_shutdown_test.go`): o `boot_timeout` sem progresso;
  `relplog_emergency_events_pending` não é o pedaço; o dump do shutdown soma aos
  gauges.
- **Concorrência:** várias conexões RELP com janelas cheias: aceitas = linhas,
  sem duplicata.
- **O binário real** (`binary_test.go`): os códigos 100–114 do
  `go-loghub-ident` e os sinais fora da janela de `run()`.

---

## 7. Destino real e emissores

Pulam sem a variável. Cada teste cria e remove a sua fila. A rodada final usou:

```bash
docker run --rm -d -p 15491:5432 -e POSTGRES_USER=rlpg -e POSTGRES_PASSWORD=rlpg \
  -e POSTGRES_DB=rlpg_test --name rlpg-test-pg18 postgres:18-alpine
export RELPLOG_TEST_PG_DSN='postgres://rlpg:rlpg@127.0.0.1:15491/rlpg_test?sslmode=disable'
```

### 7.1 PostgreSQL (`loghub.inbox`)

PostgreSQL 18. A fila nasce de `docker/postgres/inbox.sql` (um teste confere o
hash do bloco da skill) num esquema próprio por teste: `unnest` dos arrays,
`ON CONFLICT (uuid)`, as CHECKs da skill aceitando tudo o que o servidor
produz, o envelope reconstruído da linha igual ao JLH, o NUL (`jsonb`) por cópia
reconstruída, a classificação por SQLSTATE (`42`, `28`, `3D`, `23502` e `0A000`
fatais: `TestPostgresNotNullViolationIsFatal`, `TestPostgresUnknownDatabaseIsFatal`,
`TestPostgresRulesAreRefusedAtBoot`), a validação do boot (tipos, índice único
exatamente sobre `(uuid)`, coluna extra obrigatória, particionada, `LATIN1` e
regra `ON INSERT`/`ON UPDATE` recusadas) com prazo próprio
(`TestPostgresValidationHasItsOwnDeadline`, por um proxy lento), o ambiente
`PG*` escondido (`TestNewPostgresParsesWithoutTheLibpqEnvironment`) e o
`application_name`. O servidor em recuperação é simulado num banco próprio,
com um `pg_is_in_recovery()` falso à frente do `pg_catalog` no `search_path`
(`TestPostgresRecoveryIsFatalAtBootAndDownAfter`); uma réplica de verdade não
foi testada.

### 7.2 Emissores reais: rsyslog com `omrelp` (SPEC §18.7)

```bash
RELPLOG_TEST_EMITTERS=1 go test -race -run EmittersRELP -v -timeout 900s ./internal/app/
```

O rsyslog 8.2410 com `librelp` 1.11 do Alpine 3.22, num contêiner
(`rlpg-test-rsyslog:alpine-3.22`, construído pelo teste), contra o servidor do
processo de teste e a fila real.

| Teste | Cenário | Resultado (2026-10-13) |
|---|---|---|
| `TestEmittersRELPInClear` | 200 linhas em claro | todas na fila, uma vez cada, com `x-transport: "relp"` |
| `TestEmittersRELPBackpressureRetransmits` | ring cheio além de `backpressure_timeout` | a sessão fecha sem resposta, o rsyslog reconecta e retransmite: todas as linhas, 0 duplicatas, 2 conexões |
| `TestEmittersRELPSurvivesARestart` | o servidor para com transações em voo e o banco sem confirmar (dump, exit 3), e outro sobe no mesmo `data_dir` | o que tinha `200` volta pelo dump; o resto o rsyslog retransmite: enviadas = linhas, 0 duplicatas |

Os cenários do R com TLS e com o `500` final não existem aqui.

### 7.3 Docker

`docker compose --profile demo up -d --build` (2026-10-13): a imagem constrói, o
servidor sobe com o PostgreSQL 18 e a fila de `postgres/inbox.sql`, o rsyslog de
demonstração entrega (132 linhas para 131 `seq`: a duplicata do próprio rsyslog
ao voltar da suspensão inicial, SPEC §19), `/readyz` dá 200, o `SIGHUP` responde
"configuration unchanged" e o `docker compose stop` sai com 0.

---

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento. Os defeitos e arestas do
código atual, com cenário e motivo, estão em SPEC §22.1 (dez em 2026-10-13,
depois da revisão independente); aqui fica o que os testes não provam.

1. **Fuzz de 10 min por alvo ainda não rodou.** As rodadas foram de 60 s.
2. **Os critérios de carga nunca foram medidos** ponta a ponta com o rsyslog: há
   o `BenchmarkRELPSession` (uma sessão, ring em memória), não um gerador de
   carga com várias ações `omrelp` contra a fila.
3. **`kill -9` de verdade** não é testável em processo; a perda do backlog do
   ring (SPEC §19) é por desenho.
4. **Outros clientes RELP** não foram testados: só o `librelp` 1.11 do rsyslog
   8.2410.
5. **`SO_REUSEPORT`** só é testado no que é portável: que os sockets abrem.
6. **A regra do `/data`** não tem teste automatizado de contêiner com raiz
   somente leitura (o compose roda `read_only: true`, e subiu).
7. **Uma réplica de verdade em recuperação** não foi testada: o FATAL no boot e
   o DOWN depois são provados com um `pg_is_in_recovery()` falso
   (`TestPostgresRecoveryIsFatalAtBootAndDownAfter`).
8. **Um terminador TLS na frente com PROXY protocol** não tem cenário real: o
   PROXY é provado pelo cliente de teste.
9. **A ordem `watchAbort` antes de `stopSignals`** (um segundo sinal entre os
   dois teria a ação padrão do Go, 143) só existe no binário: em processo o
   `stopSignals` é vazio, e o mutante que inverte a ordem sobrevive (M24 da
   revisão de 2026-10-13). `binary_test.go` cobre os sinais fora da janela de
   `run()`, não essa.
10. **A brecha da cauda com traço depois de um valor**, na medida exata
    (decidida no N, mantida no SP e no R): depois de uma flag com valor, um
    argumento desconhecido com forma de flag só é nomeado se cair numa seção
    deste servidor ou numa que saiu do R (`--sink.postgres.tabel`,
    `--tls.cert_file`); fora delas (`-hunter2`, `--x=y`) o erro cita só a flag
    anterior e pede aspas. A cauda de um segredo partido pelo shell que tenha a
    forma de uma chave de seção conhecida (`--sink.postgres.abc`) ainda é
    nomeada: escondê-la esconderia os erros de digitação.

---

## 9. Conferência dos dois pais e revisão independente

### 9.1 As correções do R (CHANGELOG R, SPEC §22 e TESTING §8/§11 dele)

Cada linha: a correção do R, e o que a prova aqui. "Não se aplica" diz por quê.

| # | Correção do R | Aqui | Prova |
|---|---|---|---|
| 1 | Retentativa que acordava depois do aborto ainda enfileirava (duplicata) | ok | `TestReviewRetryAfterPeerLeft` |
| 2 | Teto em voo contava só a cópia | ok (RP-7) | `TestTxnChargeCoversTheTransaction`, `TestRELPInflightBudgetIsGlobal` |
| 3 | Frame lido e não despachado fora da reconciliação | ok | `TestReviewReconciliationAfterAbort` |
| 4 | Motivo `server_closed` | ok | `TestReviewUnansweredReasonOnServerEnd` |
| 5 | Aborto entre o `Drain` e o dump não contado; dump falho contado duas vezes (`dumpPhase` + `Swap`) | ok | `TestAbortBetweenTheDrainAndTheDumpCountsEverything`, `TestAbortAfterASavedDumpCountsNothing`, `TestAbortAndFailedDumpCountOnce`, `TestAbortAndDisabledDumpCountOnce` |
| 6 | `SIGQUIT` tardio perdido | ok | `TestLateSIGQUITStillAborts` |
| 7 | Transportes fechados em sequência (um `closeGrace` por transporte) | ok na forma; **sem efeito aqui**, porque há um transporte só (`tcp`, um `stream` para todos os listeners). Mantido igual ao irmão RN | por leitura (`closeTransports`); `TestStreamCloseForceClosesAfterGrace` prova o `closeGrace` |
| 8 | Cauda de senha partida pelo shell ecoada | ok | `TestReviewFlagTailEcho`, `TestConfigErrorsDoNotEchoSecrets`, `TestParserErrorsDoNotEchoValues` |
| 9 | `@` depois de `/`, `?`, `#` na `LOGHUB_EB_URL` | ok | `TestSlashInTheUserinfoIsRefusedWithoutEcho` |
| 10 | Fragmento em URL PostgreSQL | ok | `TestReviewPGFragmentDumpLeaks` |
| 11 | `Remove` do dump sem `fsync` do diretório | ok, com `ErrNotDurable` | `TestRemoveSyncsTheDirectory`, `TestQuarantineAndRemovalReportTheirOutcome` |
| 12 | Health checks sem prazo próprio (`withLifetime`) | ok | `TestHealthChecksCarryTheirLifetime` |
| 13 | NATS: `10077` como `network`, `discard=new` abaixo de um registro, negação mascarada em `core` | não se aplica: sem NATS (§1.2) | — |
| 14 | MySQL: `1203`/`1226`/`1461`, sonda com `unix`/`NULL` | não se aplica: sem MySQL | — |
| 15 | `knownSectionOf`: a cauda depois de um valor nomeada só dentro de uma seção conhecida | ok | `TestConfigErrorsDoNotEchoSecrets` (`--sink.wrkers` nomeado, `--typo.key` não) |
| 16 | `500` só para recusa final; ring cheio e sink DOWN seguram (decisões 4 e 5 do R) | ok (RP-6) | `TestRELPFullRingIsHeldNotRefused`, `TestRELPSinkDownIsHeld`, `TestSinkDownHoldsTheTransaction`, `TestEmittersRELPBackpressureRetransmits` |
| 17 | `0 serverclose 0\n` sem espaço; `serverclose` depois das decididas; `Close` só vence a leitura | ok | `TestAppendRSPAndServerClose`, `TestRELPShutdownWithTransactionsInFlight`, `TestArmReadDeadlineLeavesTheWriteSide` |
| 18 | Rate-limit que segura (decisão 8a) | não se aplica: o rate-limit saiu (RP-5) | — |
| 19 | Pendências 1, 3 do S: buffer de 4 KiB por conexão; shutdown no PROXY não conta | ok | `TestRELPFrameGrowsWithTheBytesNotTheCount`, `TestProxyShutdownMidHeaderIsNotInvalid` |
| 20 | Pendências 6–9 do S: validação com prazo próprio, `23502` e `3D` fatais, ambiente libpq escondido | ok | `TestPostgresValidationHasItsOwnDeadline`, `TestPostgresNotNullViolationIsFatal`, `TestPostgresUnknownDatabaseIsFatal`, `TestNewPostgresParsesWithoutTheLibpqEnvironment` |
| 21 | Pendência 22 do S: porta da URL com socket, parâmetro repetido, um servidor | ok | `TestPostgresURLAndAddr`, `TestEBURLRepeatedParameterIsRefused` |
| 22 | Pendências 23–25 do S: dump com nome temporário, quarentena logada como feita, gauges recontados | ok | `TestWriteOnAFileSystemWithoutHardLinks`, `TestQuarantineWithAFailedDirectoryFsyncIsQuarantined`, `TestEmergencyGaugesRecountTheDiskAfterReingestion` |
| 23 | Pendências 27–29 do S: log da recusa fatal no boot, primeiro sinal do FATAL tolerado, aborto no dump contado | ok | `TestBootRefusalLogNamesItsCause`, `TestFatalShutdownSignals`, `TestSecondSignalCountsTheAbortedLoss` |
| 24 | Pendência 30 do S: erros de configuração sem eco | ok | `TestConfigErrorsDoNotEchoSecrets` |
| 25 | Pendências 5, 10–21, 26 do S (Redis, MySQL, NATS, beacon, `sink=none`) | não se aplicam: peças cortadas | — |
| 26 | O doc de `cmd/go-relplog-server/main.go` ainda descreve o S ("UDP, TCP, TLS and unix datagram") | **achado na criação**, corrigido aqui; a portar para o R | por leitura |

### 9.2 As correções do SP (CHANGELOG SP/SN de 2026-10-07 e os portes de 2026-10-11)

| # | Correção do SP | Aqui | Prova |
|---|---|---|---|
| 1 | O ambiente libpq vazava durante o parse (`PGSSLMODE=bogus`, `PGCHANNELBINDING=require`) | ok | `TestNewPostgresParsesWithoutTheLibpqEnvironment`, `TestNewPostgresIgnoresTheLibpqEnvironment` |
| 2 | Reingestão carregava o dump inteiro (OOM no boot) | ok, em fluxo | `TestNextChunksTheFile`, `TestEmergencyDumpAndReingestionAreExact` |
| 3 | `ParseLine` deixava passar o que a fila recusa (`1e1000000`, surrogate solto, `message` ausente, chave duplicada) | ok | `TestParseLineRefusals`, `TestParseLineAcceptsWhatTheConverterWrites`, `FuzzRead` |
| 4 | Dump escrito no nome final | ok (temporário, `fsync`, `link`, `fsync` do diretório) | `TestDumpRoundTrips`, `TestLeftovers` |
| 5 | Aborto durante o dump contava 0; o primeiro `SIGTERM` do FATAL abortava | ok (e o `SIGQUIT` nunca tolerado, SP §22.1 item 10) | `TestSecondSignalCountsTheAbortedLoss`, `TestFatalShutdownSignals` |
| 6 | `sslmode` repetido passava a recusa do TLS | ok | `TestEBURLRepeatedParameterIsRefused`, `TestEBURLRefusesTLS` |
| 7 | Porta da URL com `host=/socket` sumia; `h1,h2`/porta 0 só falhavam no `boot_timeout` | ok | `TestPostgresURLAndAddr` |
| 8 | `3D` era `network` | ok | `TestPostgresUnknownDatabaseIsFatal` |
| 9 | Histogramas de lote contados por tentativa na reingestão | ok (por pedaço, fora do laço de retry) | `TestDeliverRetriesNetworkThroughWait`, `TestDeliverChunksByBatch` |
| 10 | `rename` sem *hard link* (porte do N) | ok | `TestWriteOnAFileSystemWithoutHardLinks` |
| 11 | Gêmeo de um dump ligado removido; dump interrompido informado (`Leftovers`) | ok | `TestLeftovers`, `TestBootRemovesTheTwinOfADumpAndReportsAnInterruptedOne` |
| 12 | INI e flags sem eco (o `flags.go` do N, o `ini.go` do SN), inclusive `--config= x` | ok | `TestParserErrorsDoNotEchoValues` (com `{"--config=", "hunter2"}`), `TestConfigErrorsDoNotEchoSecrets` |
| 13 | Pré-conferência do ring cheio antes do `uuid` e da cópia (revisão C) | ok | `TestIngestFullRingIsRefusedBeforeTheCopy`, `TestIngestSinkDownFailsBeforeTheCopy` |
| 14 | `ItemOverhead` 256 B com a caixa do buffer | ok | `TestItemOverheadCoversTheStruct` |
| 15 | Chaves do pai: flag/INI exit 1, env `WARN` | ok, com a lista do R | `TestRemovedEnvIsTheParentMinusThis` e os testes de `removed.go` |
| 16 | SP §22.1 itens 1–10 (as pendências que o SP ainda tem) | todos corrigidos aqui, pelo código do R (SPEC §22) | as linhas 19–24 de §9.1 e `TestPostgresValidationHasItsOwnDeadline` |
| 17 | SN: `nats.go` com TLS sozinho, `ErrMaxPayload` no meio do pedaço, permissão negada, `sink.nats.urls` | não se aplicam: sem NATS | — |
| 18 | `Seal` do ring antes do último `Drain` (P, F, W) | não se aplica: é do upload HTTP dos webhooks; aqui uma transação RELP que chega depois do fechamento fica sem resposta e o rsyslog a retransmite | — |

### 9.3 Revisão independente (2026-10-13)

Dez revisores independentes, cinco áreas (app/emergência, config, RELP, sink,
docs) em duas rodadas. Cada correção tem um teste que falhava antes; cada uma
foi conferida por **mutação** (a correção revertida, o teste falha). Itens
corrigidos no **RN** (`go-relplog-nats-server`) na revisão dele, no mesmo
código comum, foram **portados de lá** (marcados "RN"), para os dois só
diferirem onde o SP e o SN diferem. As arestas que ficaram estão em SPEC §22.1.

| # | Achado | Correção | Teste |
|---|---|---|---|
| 1 | Posse do aborto com duas janelas de contagem dupla (aborto entre os dois `Store`; aborto no recolhimento e o shutdown rearmando o contador: 10 contados para 5) | `lossMu` + `aborted`, `enterDumpPhase`, `oweDump` (RN) | `TestAbortDuringTheDrainIsCountedOnce`, `TestAbortBeforeTheDumpPhaseKeepsItClosed`, `TestAbortCountsOnce`, `TestAbortDuringTheCollectionCountsOnceAndDumpsNothing` |
| 2 | Dump publicado depois de o aborto contá-lo como perdido (reingerido no boot seguinte) | `emergency.Store.SetAbandon`, `ErrAbandoned` (RN); `settle` dos testes exige que nada chegue ao disco | `TestDumpAfterTheAbortIsAbandoned`, `TestAbortDuringTheCollectionPublishesNoDump`, `TestFatalShutdownSignals` |
| 3 | Dump que completa depois do aborto ainda contava `malformed` e somava gauges (A7) | `owed == 0` depois do `Write` não conta mais nada | `TestAbortAfterTheLastAbandonCheckCountsNothingMore` |
| 4 | `boot_timeout` único da reingestão gasto por pedaços **bem-sucedidos**: a primeira falha transitória depois dele saía com 3, sem retentativa | prazo sem progresso: anda a cada pedaço confirmado (RN; DECISOES §2) | `TestLongReingestionStillRetriesATransientFailure` |
| 5 | `relplog_emergency_events_pending` mostrava o pedaço em voo durante a reingestão; o mutante sem recontagem (M31) sobrevivia por coincidência | linha removida; o gauge só sai da recontagem (RN) | `TestReingestionDoesNotShowTheChunkAsPending`, `TestPendingGaugeIsNotTheChunkDuringReingestion`, `TestEmergencyGaugesRecountTheDiskAfterReingestion` |
| 6 | Sinal que cancela o *bind* (nome em `listen.tcp`/`admin.bind`) saía com 1, não 130 | `ctx.Err()` no erro do *bind* → 130 (RN) | `TestCancelledBindIsAnAbortNotAConfigError`, `TestSignalBeforeTheBindExits130` |
| 7 | `SIGQUIT` tardio: a conferência única corria com a goroutine observadora | canal `quitNow` no laço do shutdown (RN) | `TestSIGQUITAfterTheEntryCheckStillAborts` |
| 8 | `open` do diretório falho depois do `unlink` logava "cannot remove ... delivered again" (falso) | `ErrNotDurable` também nessa falha (RN) | `TestRemoveWithAnUnopenableDirectoryIsRemoved` |
| 9 | Com 4 workers o teste do aborto no dump nunca exercia o `dumpPhase` (M07 sobrevivia) | teste com `--sink.workers=1` | `TestAbortDuringTheDumpWithItemsDrainedFromTheRing` |
| 10 | Testes do aborto copiavam o `gracefulShutdown` à mão (M06, M28); M16, M18, M30, M32 sobreviviam | gancho `beforeCollect` e o shutdown real; canal `quit` dado em processo | `TestAbortDuringTheCollection*`, `TestSIGQUITWhileServingCountsTheRing` (M16), `TestShutdownDumpAddsToTheGauges` (M18), `TestShutdownReleasesAHeldTransactionAsShutdown` (M30), `TestPendingGaugeIsNotTheChunkDuringReingestion` (M32). M15 deixou de existir (o prazo é outro); M24 só no binário (§8 item 9) |
| 11 | `TestDumpIsPublishedOnlyWhenComplete` do R não portado | portado | `TestDumpIsPublishedOnlyWhenComplete` |
| 12 | Pré-checagem do ring cheio só via itens: com `ring.max_bytes` limitando, cada retentativa cunhava `uuid`, copiava e montava o item (1 alocação a cada 50 ms), contra SPEC §5.7 e DECISOES §2 | `ring.FullFor(n)` com o tamanho truncado; ring fechado nunca é "cheio" | `TestIngestByteCeilingIsRefusedBeforeTheCopy`, `TestIngestByteCeilingChargesTheTruncatedLine`, `TestIngestClosedFullRingIsTheShutdown` |
| 13 | `try()` não conferia o fim da sessão logo antes do `Enqueue` (E9) | `ctx.Err()` → `Shutdown` antes do `Enqueue`; `TestRELPMalformedFrameClosesAfterTheAnswers` espera o ring (só o que está no ring é "decidido") | `TestIngestEndedSessionIsNotEnqueued` |
| 14 | Erro inesperado do ring com o contexto cancelado logava `ERROR` e segurava | `context.Canceled` → `Shutdown` sem log; `DeadlineExceeded` segura (RN) | `TestIngestEnqueueErrorDecidesWhetherTheBufferIsPooled` |
| 15 | Mutantes do RELP sem teste: contexto depois do `Acquire` (E1), cópia antes da vaga (E3, M17), `server_closed` no segundo `open` (E4) | testes | `TestDispatchAfterSessionEndStartsNothing`, `TestCopyComesAfterTheWindowAndTheBudget`, `TestSecondOpenLeavesItsHeldTransactionServerClosed` |
| 16 | `Deliver`: recusa `logic` numa escrita durante a qual veio o `SIGTERM` era contada e não logada | `ClassLogic` tratada antes do contexto | `TestDeliverLogicRefusalWinsOverTheCancellation` |
| 17 | `pool_size = workers` aceito: escritas lentas deixavam a sonda sem conexão → DOWN → FATAL | `pool_size ≥ workers + 1` (SPEC §10.5) | `TestSinkWorkersAndPool`, `TestPostgresHealthHasAConnectionWhileTheWorkersWrite` |
| 18 | Tabela com regra `ON INSERT`/`ON UPDATE` passava no boot; `0A000` era `network` para sempre | recusa no boot via `pg_rewrite`; `0A000` fatal | `TestPostgresRulesAreRefusedAtBoot`, `TestClassifySQLState` |
| 19 | Mutantes do sink sem teste: buffer manchado no pool (M47), `Seal` sem limite de capacidade (M45), `clear(Envelopes)` (M46), buffer grande no pool (M52), réplica no boot/depois (M30/M31), consultas do árbitro (M26/M27/M37/M40) | testes; a réplica com um `pg_is_in_recovery()` falso num banco próprio | `TestTaintedChunkBufferIsNotPooled`, `TestSealLimitsEachEnvelopeToItsBytes`, `TestResetClearsTheEnvelopeSlots`, `TestOversizedChunkBufferIsNotPooled`, `TestPostgresRecoveryIsFatalAtBootAndDownAfter`, `TestPostgresArbiterIndexRules` (M16 do sink é equivalente) |
| 20 | Recusa `logic` com pedaço vizinho no PostgreSQL real sem teste | teste | `TestPostgresLogicRefusalSparesTheNeighbours` |
| 21 | Sem tetos: `batch_max_bytes` ≥ 1 GiB (sem SQLSTATE, `network` para sempre), `pool_size` com volta no `int32`, `workers`, `capacity_items` (pânico do `makechan`), `sink.batch`, `tcp_listeners` | tetos de SPEC §13.2 | `TestConfigCeilings` |
| 22 | `sink.postgres.addr` com tab (regressão do corte), `[h:5432]:5433`, socket com vírgula; `?host=` com `,` ou `:` | `oneHost`, `", \t"`, recusas no socket e no `host=` | `TestPostgresAddrNamesOneServer` |
| 23 | Flag sem `=` com o valor colado ao nome (`--sink.postgres.passwordhunter2`) ecoava o segredo | não citar o que estende um nome do registry | `TestGluedValueIsNotPrinted` |
| 24 | `--help`/`--version`/`--dump-config` aceitavam `=valor` (`--dump-config=false` despejava) | exit 1 sem o valor | `TestSwitchesTakeNoValue` |
| 25 | Flag de seção que saiu do R depois de uma flag com valor culpava a flag anterior | `removedSections` e `removedParams` no `knownSectionOf` e no erro (RN) | `TestRemovedFlagAfterAValuedFlagIsNamed`, `TestRemovedParamsAreTheRemovedEnv` |
| 26 | Continuação de um segredo na linha seguinte do INI era citada como chave desconhecida (achado do RN, herdado de R/SN; vale aqui) | `lookupINI` com `afterSecret` e `iniKeyRE` (RN) | `TestINIUnknownKeyNeverPrintsASecretContinuation` |
| 27 | URL sem usuário/banco caía no padrão do registry; `?sslmode=` vazio aceito; host em porcento e `://` na senha com mensagens enganosas; `LOGHUB_EB_URL` só com espaços ignorada | exit 1 nomeando o que falta; mensagens corrigidas (DECISOES §2) | `TestEBURLMustNameItsParts` |
| 28 | `--x -h` nomeava `--h` (uma senha de dois caracteres) | só nomeia como flag com dois traços | `TestParserErrorsDoNotEchoValues` |
| 29 | `--config=` vazio caía no `RELPLOG_CONFIG`; BOM no INI; dump com derivados como chaves; sinônimo do charset no `SIGHUP` | exit 1; BOM ignorado; derivados comentados; sinônimos normalizados | `TestEmptyConfigFlagIsRefused`, `TestINIByteOrderMarkIsIgnored`, `TestDumpCommentsDerivedValues`, `TestCharsetSynonymsAreOneValue` |
| 30 | Mutantes da config sem teste: `admin.bind` vazio, `@` cru na senha do `redactOneURL`, eco do esquema | testes | `TestReviewMutantGaps` |
| 31 | `--help` dizia "empty disables" em `listen.tcp`, não citava o teto do `max_frame_bytes`; erro "only --name=value" com a forma separada aceita | textos do registry e dos erros | por leitura (`TestParserErrorsDoNotEchoValues` cobre os erros) |
| 32 | Restos do corte: ramo UDP morto no `ingest.go`, `TestIngestUnixHasNoAddresses`, `udpWire`, `FramingNonTransparent` nos testes; comentários velhos (`sockopt_unix.go`, `review_relp_test.go`, `ring.go`, `item.Size`, `admin.go`, tabela do `postgres.go`, `MaxEventBytes`, ordem do `Ingest`) | removidos e corrigidos | a suíte (por leitura nos comentários) |
| 33 | `TestWorstCaseJLHIsBounded` sem o `x-transport` da produção | `XTransport = "relp"` | `TestWorstCaseJLHIsBounded` |
| 34 | `KInt64`/`KList` mortos (D4); `Outcome.Retryable` sem uso em produção (E6) | **recusados**: o `value.go` é comum com o RN, que usa `KList`; `Retryable` documenta a regra e os testes a usam (o RN a mantém) | — |
| 35 | Aborto no `pre_stop_delay` (A9), rótulo `server_closed` com `idle_timeout` < `backpressure_timeout`, `P0001` como `network`, sobrecontagem do dump publicado no aborto, `kind="fatal"` com o boot cancelado (K4), arestas herdadas do SP | **pendências**, SPEC §22.1 | — |
| 36 | Docs (13 + 7 itens): §22.1 dizia "nenhuma" e contradizia TESTING/DECISOES; colisão da 9103 com R **e** RN; tabela dos códigos 100–114 apontava para o README do SP; `stop_grace_period` "+ 6 s de folga"; `@` cru na senha dito exit 1; `-run` do §3 deixava onze testes de fora; textos velhos de filtro, de outro ring e de `ring=sqlite`; `SIGHUP` "recarrega `[log]`"; `.env.example` sem as variáveis do demo e do `log.format`; `item.ItemOverhead` | corrigidos | por leitura |
| 37 | Suspeita de *flake* em `TestPostgresWrongPasswordIsFatal` sob execução paralela | investigado: o PostgreSQL reserva a vaga de conexão **antes** de conferir a senha, e com as vagas esgotadas (`max_connections` 100, várias suítes em paralelo) o login errado recebe `53300 too many clients` (`network`, corretamente), não `28P01`; reproduzido com 100 sessões abertas e `psql` com senha errada. O teste espera passar um `53300`; o código não muda | `TestPostgresWrongPasswordIsFatal` (300 repetições com a app e o sink em paralelo, verdes) |

### 9.4 Correções transversais da revisão independente do `go-syslog-server` (2026-10-13)

Os itens transversais (X1–X4) do plano da revisão independente do porte do
`go-syslog-server` (S), e a conferência de C3, D1, E1, E2 e A2-minor; a mesma
implementação no irmão (regra de sincronia, `diff` conferido). **Conferência
por mutação**, cada correção revertida sozinha: o teste da linha falha
(`exited 0, want 130`; `abort 0, emergency_failed 5 for 5`; `err = <nil>` no
`?host=`; o buffer solto com o frame seguinte já no `bufio.Reader`).

| Item | Achado | Consequência sem a correção | Teste |
|---|---|---|---|
| X1 (A1 do S) | Sinal depois da reingestão e antes do *accept* com IP literal: nenhum *bind* falha | o servidor abria, o `/readyz` dava 200 e o shutdown saía **0**, não 130 (SPEC §13.1, §13.5) | `TestSignalBeforeALiteralBindExits130` (IP literal; o nome de host segue em `TestSignalBeforeTheBindExits130`) |
| X2 (A2 do S) | O aborto publica `aborted` antes de tomar o contador; o dump que via a marca nesse meio (abandonado, ou desligado) o tomava primeiro | `emergency_failed = n`, `abort = 0`: a perda contada uma vez, atribuída ao disco (SPEC §13.4) | `TestAbortInsideTheDumpKeepsItsReason` (dump abandonado e dump desligado; o gancho `inAbort` força a intercalação) |
| X4 (B4 do S) | `?host=` vazio na `LOGHUB_EB_URL` aceito: a libpq e o pgx discam o padrão deles (`pgx.ParseConfig` dá `/private/tmp`), este servidor o host da autoridade | uma URL, dois servidores (SPEC §10.9) | `TestEBURLMustNameItsParts` (dois casos novos) |
| D1 do S | A sonda de saúde sem conexão no pool | **não se aplica**, conferido: ring só memória, os escritores são os `sink.workers`, o `+ 1` basta (SPEC §10.5) | `TestPoolLeavesTheProbeAConnection`: três escritas presas num `LOCK TABLE` (contadas em `pg_stat_activity`), a sonda responde com `pool_size = 4`; o controle `pool_size = 3` a deixa sem conexão |
| E2 do S | O leitor RELP soltava o buffer acima de 16 KiB antes de **todo** frame | um emissor de linhas de 16–64 KiB refazia o crescimento por frame: 24 KiB custavam 12,3 µs, 55 KB e 5 alocações, contra 0,52 µs e 0 (SPEC §5.2) | `TestRELPReaderKeepsTheBufferWhileFramesFollow`, `TestRELPReaderReleasesALargeBuffer` (agora com o emissor parado entre os frames), `BenchmarkRELPReaderFrames` (§5) |
| E1 do S | `closing` com PROXY `optional` | **não aplicado**, por instrução: no RELP as linhas no buffer nunca tiveram `200`, e o `librelp` as retransmite | — |
| A2-minor do S | Comentário de ordem reversa com fechamento paralelo; log do aborto com o ring Redis | **não se aplica**: o comentário diz "AT ONCE", como o código; não há Redis | — |

---

## 10. Resultado da rodada final (2026-10-13)

Rodada de 2026-10-13, depois da revisão independente, num Apple M2 com macOS,
Go 1.27.1, PostgreSQL 18 (`postgres:18-alpine`, contêiner `rlpg-test-pg18`) e o
rsyslog 8.2410 com `librelp` 1.11 (Alpine 3.22).

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `RELPLOG_TEST_PG_DSN=... RELPLOG_TEST_EMITTERS=1 go test -race -count=1 -timeout 900s ./...` | **verde duas vezes seguidas** (53 s e 52 s), sem *flake* |
| Pacotes | 13: 12 com testes, todos verdes; `cmd/go-relplog-pg-server` sem arquivo de teste (o `main` roda no `binary_test.go`) |
| Testes de primeiro nível | **402** passaram (395 `Test` e as sementes dos 7 `Fuzz`), 0 falhas, 0 pulados; eram 349 funções `Test` antes da revisão, 396 depois (47 novas, uma removida: `TestIngestUnixHasNoAddresses`, do UDP/unix que saiu) |
| Subtestes | **281** passaram, **1** pulado: `TestGoldenVectors/arquivo-sem-pri`, o vetor com `transport=file`, que este servidor não produz (desvio D5) |
| Por pacote (primeiro nível / subtestes) | `app` 68/62, `config` 63/0, `convert` 15/70, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 9/0, `receiver` 99/33, `ring` 13/0, `sink` 92/71 |
| Emissores reais | `TestEmittersRELPInClear` (12,1 s), `TestEmittersRELPBackpressureRetransmits` (5,7 s), `TestEmittersRELPSurvivesARestart` (3,6 s): verdes nas duas rodadas |
| Cobertura (`-coverpkg=./...`) | 91,1 % do módulo (§1) |
| Fuzz | 5 alvos dos pacotes tocados, 60 s cada, sem falha (§4); `FuzzConvert` e `FuzzSD` não rodaram nesta rodada (`internal/convert` não mudou) |
| Benchmarks | 4 funções, 6 resultados (§5) |
| Mutação | cada correção da revisão conferida revertendo-a: o teste correspondente falha (§9.3) |
| Binário | 11,9 MB com `-trimpath -ldflags='-s -w'` (o do Dockerfile); 17,3 MB sem |
| Não rodou | fuzz de 10 min (§8 item 1); `docker compose --profile demo` (o Docker do módulo não mudou nesta rodada, só textos); carga ponta a ponta (§8 item 2) |

### Depois das correções transversais de §9.4 (2026-10-13)

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `RELPLOG_TEST_PG_DSN=... RELPLOG_TEST_EMITTERS=1 go test -race -count=1 ./...` | **verde duas vezes seguidas** (55 s e 54 s), sem *flake* |
| Testes | **407** de primeiro nível e **286** subtestes passaram, **1** pulado (`TestGoldenVectors/arquivo-sem-pri`, D5), 0 falhas (402 e 281 antes: os 5 de primeiro nível novos são os de §9.4) |
| Por pacote (primeiro nível / subtestes) | `app` 70/65, `config` 63/0, `convert` 15/70, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 9/0, `receiver` 101/33, `ring` 13/0, `sink` 93/73 |
| Emissores reais | `TestEmittersRELPInClear` (12,3 s), `TestEmittersRELPBackpressureRetransmits` (6,0 s), `TestEmittersRELPSurvivesARestart` (3,7 s), verdes nas duas rodadas |
| Fuzz | `FuzzFrame` 60 s, sem falha (1,66 milhão de execuções); o único alvo cujo código mudou (`relpframe.go`) |
| Mutação | cada correção revertida sozinha: o teste da linha falha (ver acima) |
| Não rodou | fuzz de 10 min; carga ponta a ponta; `docker compose` (o `docker/` não mudou) |
