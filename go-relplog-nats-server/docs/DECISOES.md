# go-relplog-nats-server — Decisões de projeto

Registro das decisões que criaram o `go-relplog-nats-server` (letra **RN** no
`CHANGELOG.md` da raiz) em 2026-10-13. Este arquivo é a **entrada** da
`docs/SPEC.md`; a SPEC é a fonte da verdade, e cada decisão abaixo aparece nela
com o número da seção.

## O pedido

Um **corte** (não uma reescrita) do `go-relplog-server` (**R**) no molde do
`go-syslog-nats-server` (**SN**): ring só em memória, sink só no NATS (JetStream
e core, subject exato, beacon fica, stream `LOGHUB_SYSLOG` compartilhado),
emergência em JSON Lines com reingestão em fluxo, **sem TLS em lugar nenhum**,
sem filtro e sem rate-limit; o RELP do R intacto. Feito **sem entrevista**, pela
regra do usuário de 2026-10-10: o que se aprende do código e dos irmãos é
decidido aqui, e uma decisão anterior do usuário num `DECISOES.md` irmão conta
como descoberta. As decisões são **contestáveis**: estão escritas para o usuário
poder trocá-las sabendo o custo.

Ao mesmo tempo, outra sessão criava o `go-relplog-pg-server` (**RP**), o mesmo
corte no molde do `go-syslog-pg-server` (**SP**). Os pacotes comuns aos dois
(receiver, convert, item, jlh, config fora do sink, emergência, app fora do sink)
só devem diferir onde SN e SP diferem; nenhum dos dois lê o diretório do outro.

## Fontes

| Fonte | Papel |
|---|---|
| `go-relplog-server/` (R), 2026-10-12 | **Pai do código RELP**: moldura, sessão, janela, teto em voo, respostas, fim de sessão, PROXY, conexões; config e app; as correções das revisões dele |
| `go-syslog-nats-server/` (SN), com os portes de 2026-10-11 | **Pai do molde**: ring só memória, sink NATS único, beacon, dump JSON Lines com `ParseLine`, reingestão em fluxo, a forma do corte da config, o `docker/` enxuto |
| `CHANGELOG.md` da raiz (entradas R, SN, N e os portes) | As listas "a portar" conferidas uma a uma (§2.3) |
| skills `loghub-json-schema`, `loghub-type-syslog-schema`, `go-loghub-uuidv7`, `go-loghub-ident` | Fontes normativas, as mesmas dos dois pais |
| `librelp` lido no R (`scrsp.c`, `cserverclose.c`, `copen.c`, `relpframe.c`) | O que cada resposta RELP significa; nada mudou no corte |

## Estado do monorepo

| Pasta | Estado |
|---|---|
| `go-relplog-nats-server/` | **Este módulo**, v1, criado em 2026-10-13, sem commit |
| `go-relplog-server/` (R) | Pai RELP: 4 rings × 7 sinks, TLS, filtro, rate-limit. Nunca importado |
| `go-syslog-nats-server/` (SN) | Pai do molde. Nunca importado |
| `go-relplog-pg-server/` (RP) | Irmão, em criação na mesma hora (prefixo `rlpg-`) |
| `go-syslog-server/` (S), `go-syslog-standard-server/` | Avô e referência |

---

## 1. Decisões

| # | Tema | Decisão | Por quê | SPEC |
|---|---|---|---|---|
| 1 | Forma | **Corte do R**, com os pacotes de ring, sink, beacon, emergência e app do SN; numeração de SPEC **do R** (seções removidas com título e nota); módulo independente | O RELP é o que não pode mudar, e o código cita a SPEC por número: os dois servidores RELP precisam dizer a mesma coisa com o mesmo `§`. O SN já fez o corte equivalente sobre o S, e copiar a forma dele evita reinventar o que lá foi revisado | cabeçalho, §17 |
| 2 | TLS | **Sai em todo lugar**: sem `listen.tls`, sem `[tls]`, sem `payload.tls`; cliente NATS sempre texto claro (`sink.nats.tls`, `tls_ca`, `tls_insecure` saem); só `nats://` em `sink.nats.urls` e na `LOGHUB_EB_URL` (`tls://`, `ws://`, `wss://`, `host:porta` sem esquema: exit 1); `TLSCertCB` que recusa (FATAL, exit 3 no boot); `@` depois de `/ ? #` recusado (regra do R) | Decisão 2 do SN, que o pedido estende a este; o `listen.tls` do R era a única razão de `crypto/tls` no receptor. Quem precisa de RELP cifrado usa o R ou um terminador TLS com PROXY protocol | §1.2, §4.5, §10.7, §10.9 |
| 3 | Subjects | **Só o exato**, `loghub.<space>.syslog`; sem `sink.nats.subjects` | Decisão 4 do SN; a skill do envelope avisa contra tokens extras | §1.2, §3.8 |
| 4 | Filtro e rate-limit | **Saem.** A regra 8a do R ("o rate-limit segura, nunca `500`") **não se aplica** mais: nada na recepção espera por outra coisa que o ring ou o sink | O molde do SN; a recepção deixa de ler o PRI (§2.2) | §7, §8 |
| 5 | Emergência | **JSON Lines**, um JLH por linha, nome temporário + `fsync` + `link` (ou `rename`) + `fsync` do diretório; reingestão em fluxo pela porta estrita `ParseLine`; o `.bin` do R sai | Decisão 3 do SP, herdada pelo SN: legível por qualquer ferramenta, sem codec de cabeçalho | §12 |
| 6 | Onde o R respondia `500` | **Reavaliado**: o `500 refused by the destination` (recusa `logic` com `ring=none`) deixa de existir — com o ring em memória o destino nunca fala com a transação, e a recusa é do worker, depois do `200 OK`, contada em `sink_logic`, como no SN. Ficam só a guarda de origem não endereçável e os erros de protocolo do cliente. Erro inesperado do ring: segura (contexto cancelado: fim da sessão, sem resposta, como no R) | No fluxo normal não há `500`; cada `500` custa uma reconexão e duplicatas (§5.4 do R) | §2.3, §5.4 |
| 7 | Conta do teto em voo | Mantida em `2 × classe + 8 KiB` por transação, embora com o ring em memória a segunda classe (a cópia do item) saia da conta logo depois do `Enqueue` | Uma transação segurada refaz a cópia do item a cada tentativa recusada; e cobrar menos que o R daria um teto diferente para a mesma config | §5.6 |
| 8 | Nomes e portas | Binário `go-relplog-nats-server`; env `RELPLOG_*` e `RELPLOG_CONFIG`; `[syslog]` em `SYSLOG_*`; métricas `relplog_` (as do R que existem aqui, com os mesmos nomes); admin `127.0.0.1:9103` (**o mesmo do R e do RP**, a regra da família, como S, SP e SN na 9102: não rodar dois deles no mesmo host sem trocar; mantida em 2026-10-13); cliente NATS `rlnats/<AgentName>@<Hostname>`; `relp_software=go-relplog-nats-server,<versão>`; Docker com prefixo `rlnats-` e portas de host distintas de R, SN e RP (RELP 20581, admin 19381, NATS 14381, monitor 18381) | A regra do SN com o S: um painel e um monitoramento do R servem aqui, e trocar um pelo outro não muda nada fora. O prefixo do cliente diz no `connz` qual está conectado | §14, §16, §17.1 |
| 9 | Chaves do R que saíram | Na linha de comando e no INI, exit 1 com "go-relplog-server has it, go-relplog-nats-server does not" (também depois de uma flag com valor); no ambiente, **um** `WARN` nomeando as 92 variáveis do `--help` do R que não existem aqui | A regra do SN: um mesmo `.env` pode alimentar mais de um servidor; uma chave do R esquecida não pode sumir em silêncio | §13.2, §16.2 |
| 10 | Dependências | **Três diretas** (`nats.go`, `go-loghub-ident`, `go-loghub-uuidv7`), as do SN; `jsonschema/v6` só em teste | O pedido; binário de 12,6 MB contra 31,9 MB do R | §17.3 |
| 11 | `ParseLine` e dumps de irmãos | Aceita qualquer JLH `type=syslog` válido, inclusive de um dump do SN ou do `go-syslog-server` (sem `x-transport`) | É o mesmo `type` e o mesmo stream; recusar por `x-transport` seria uma regra que o schema não tem | §12.2 |
| 12 | Prazo da reingestão | **Um `boot_timeout` sem progresso** (todo pedaço entregue adia o prazo), não um prazo fixo desde o começo como no SN | Revisão desta criação: uma reingestão saudável de um dump grande saía com 3 no primeiro soluço depois do `boot_timeout`, sem retentativa, e republicava o arquivo a cada boot. Um NATS que não aceita nada continua limitado a um `boot_timeout` | §12.2 |
| 13 | Aborto durante o dump | A contagem passa por um mutex com o aborto; o dump que o aborto contou como perdido é **abandonado** (o temporário é removido) | Revisão desta criação: 10 contadas perdidas para 5, e um dump contado como perdido que voltava no boot seguinte | §13.4, §20.26 |
| 14 | `WithPublishAsyncMaxPending` | `2 × sink.workers × sink.batch` (o SN usa 1×; o R somava o `ring.max_inflight`) | Revisão desta criação: os futuros de um pedaço contaminado contam até o próprio prazo; sem folga, uma retentativa rápida parava na janela cheia | §10.7 |

---

## 2. Padrões assumidos (contestáveis)

### 2.1 Do RELP (iguais ao R)

- Moldura, `0 serverclose 0\n`, `close` que espera as transações, `500` para
  `syslog` antes do `open`, segundo `open` e comando desconhecido; janela 128;
  teto em voo 64 MiB; `relp.backpressure_timeout` 30 s; `closeGrace` 6 s; o
  `Close` que só vence o prazo de leitura; `tcp.max_conns_per_ip` pelo endereço
  exato; `tcp.idle_timeout` 120 s; PROXY v1/v2. Nenhuma semântica mudou.
- **O fechamento paralelo dos transportes fica** (com um só): é a correção do R
  para TCP e TLS em sequência, e mantê-la impede que um segundo transporte traga
  a soma de `closeGrace` de volta.
- **Uma retentativa segurada pelo ring cheio não cunha `uuid` nem copia**: a
  pré-checagem do ring corre antes, e com retentativas a cada 50 ms ela é o
  que importa. Ela confere **os dois tetos** (`ring.Memory.FullFor(n)`, com o
  tamanho truncado); a `Full` do SN via só os itens, e uma transação segurada
  por `ring.max_bytes` ainda cunhava e copiava a cada tentativa (revisão do RP,
  2026-10-13, portada).

### 2.2 Do sink, do beacon e do dump (iguais ao SN, com as correções do R)

- O `nats.go` do R com o corte do SN: uma conexão, `MaxReconnects(-1)`,
  `ReconnectBufSize(-1)`, `WithExpectStream`, `WithRetryAttempts(0)`,
  `WithPublishAsyncTimeout`, validação do stream em toda checagem, veredito de
  permissão, `TLSCertCB` que recusa; as linhas do R que o SN não tinha
  (`10077` cheio/fechando e `10158`/`10167`/429 como `network`, `retention:
  interest` recusado, `max_msgs_per_subject` com `WARN`, `discard: new` abaixo
  de um registro, `_INBOX` como veredito, negação mascarada, `WARN` de `space`
  estrangeiro em `core`).
- **`flushErr`** (achado ao montar o RN, 2026-10-13): um *flush* de `core` cortado
  por uma reconexão devolvia `ErrConnectionClosed`, que a tabela dá como
  `fatal`; agora é `network` enquanto a conexão não está `CLOSED`. Vale para o R,
  o SN e o N (era o "não corrigido" de 2026-10-11).
- **`withLifetime` não se aplica**: o prazo próprio das checagens de saúde do R
  servia a sondas de banco; a sonda NATS valida dentro da própria checagem.
- O log de DOWN diz o que acontece no RELP ("incoming RELP transactions are
  held"); o R ainda tem o texto do S.
- **Tetos de `sink.workers` e `sink.batch_max_bytes`** (2026-10-13, no porte
  da revisão do RP, que pôs tetos nas chaves que dimensionam memória ou uma
  contagem passada a uma biblioteca; SPEC §13.2, §16.5). Os comuns vêm iguais
  (`ring.capacity_items` ≤ 16 Mi, `sink.batch` ≤ 65 536, `listen.tcp_listeners`
  1–256); os dois que dependem do destino foram escolhidos para o NATS:
  - **`sink.workers` ≤ 256** (o RP usa 1024, pelo orçamento de conexões do
    PostgreSQL): aqui todos os workers publicam na **uma** conexão NATS, que
    serializa as escritas num mutex e num socket, e passados algumas dezenas só
    cresce a disputa. E o cliente recebe `2 × workers × batch` como
    `WithPublishAsyncMaxPending` (decisão 14): nos tetos, 2 × 256 × 65 536 =
    33 554 432 futuros, um `int` positivo em qualquer plataforma; sem teto o
    produto podia dar a volta e o boot cair num erro de uma opção que ninguém
    configurou. O padrão (8) fica.
  - **`sink.batch_max_bytes` ≤ 256 MiB**, o número do RP, com outro porquê: o
    NATS não limita o pedaço — cada envelope é uma mensagem, limitada por
    `max_payload` (até 64 MiB no `nats-server`, já conferida contra o pior JLH,
    §3.9) —, então o teto só limita a memória: um pedaço fica inteiro enquanto
    os futuros dele estão pendentes, e um contaminado até eles vencerem.
  Os dois números são folgados de propósito: barram o erro de digitação, não
  um ajuste. Contestável: um teto do produto `workers × batch` (o que de fato
  dimensiona os futuros e os `PubAck` no buffer de saída do servidor, 64 MiB
  por conexão no padrão) seria mais preciso e mais difícil de explicar.
- `docker/` enxuto como o do SN: um `docker-compose.yml` (servidor, `nats`,
  `nats-init` que cria o stream de `nats/stream.json`, e o emissor rsyslog do R
  no profile `demo`), `common.yml`, `.env.example`, `conf/relplog.ini` só com
  `[log]`.

### 2.3 As correções dos dois pais, uma a uma

Cada linha diz onde a regra está e o teste que a prova neste módulo. "N/A" diz
por que não se aplica.

**Do R** (entrada R do `CHANGELOG.md`, SPEC §22 dele e as revisões de
2026-10-11/12):

| Correção | Aqui | Teste |
|---|---|---|
| `500` nunca para o que passa sozinho; segura e fecha sem resposta | ok, §5.4, §5.7 | `TestRELPFullRingIsHeldNotRefused`, `TestRELPSinkDownIsHeld`, `TestRELPBackpressureTimeoutClosesWithoutAnswering`, `TestEmittersRELPBackpressureRetransmits` |
| `0 serverclose 0\n`, depois das respostas, nunca no `close` | ok, §5.1, §5.8 | `TestAppendRSPAndServerClose`, `TestRELPCloseWaitsForTransactionsInFlight`, `TestRELPMalformedFrameClosesAfterTheAnswers` |
| Retentativa que acordava depois do aborto e enfileirava | ok, §5.7 | `TestReviewRetryAfterPeerLeft` |
| Conta `2 × classe + 8 KiB` | ok, §5.6 (decisão 7) | `TestTxnChargeCoversTheTransaction` |
| Frame lido e não despachado na reconciliação; motivo `server_closed` | ok, §5.4 | `TestReviewReconciliationAfterAbort`, `TestReviewUnansweredReasonOnServerEnd` |
| Buffer do frame pelos bytes, não pelo `DATALEN` | ok, §5.2 | `TestRELPFrameGrowsWithTheBytesNotTheCount`, `FuzzFrame` |
| Aborto entre `Drain` e dump; dump falho contado duas vezes (`dumpPhase` + `Swap`) | ok, e **estendido** (decisão 13) | `TestAbortBetweenTheDrainAndTheDumpCountsEverything`, `TestAbortAndDisabledDumpCountOnce`, `TestAbortDuringTheDrainIsCountedOnce` |
| `SIGQUIT` tardio | ok, e **estendido** (o canal `quitNow`) | `TestLateSIGQUITStillAborts`, `TestSIGQUITAfterTheEntryCheckStillAborts` |
| Transportes fechados em paralelo | ok (um transporte; o paralelo fica) | sem teste próprio (um transporte); `TestStreamCloseForceClosesAfterGrace` prova o `closeGrace` de um `Close` |
| Cauda de senha partida pelo shell; `knownSectionOf` | ok, e **estendido** às seções do R | `TestReviewFlagTailEcho`, `TestRemovedFlagAfterAValuedFlagIsNamed` |
| `@` depois de `/`, `?`, `#` numa URL | ok, §10.9, §16.2 | `TestSlashInTheUserinfoIsRefusedWithoutEcho`, `TestURLErrorsQuoteNothingOfTheURL` |
| Fragmento numa URL PostgreSQL | N/A: não há PostgreSQL | — |
| `fsync` do diretório no `Remove` (e `ErrNotDurable`) | ok, §12.2, e **estendido** ao `open` do diretório | `TestRemoveSyncsTheDirectory`, `TestQuarantineWithAFailedDirectoryFsyncIsQuarantined`, `TestRemoveWithAnUnopenableDirectoryIsNotDurable`, `TestQuarantineAndRemovalReportTheirOutcome` |
| Prazo próprio dos health checks (`withLifetime`) | N/A: sonda NATS com prazo próprio (§13.3) | — |
| NATS `10077` cheio/fechando `network` | ok, §10.7 | `TestClassifyNATS` |
| `discard: new` com `max_bytes` abaixo de um registro | ok, §10.7 | `TestNATSValidateDiscardNewBelowARecord` |
| Negação mascarada em `core` | ok, §10.7 | `TestMaskedDenial`, `TestNATSCoreMaskedDenialIsNotConfirmed` |
| MySQL `1203`/`1226`/`1461`; sonda MySQL | N/A: não há MySQL | — |
| Pendências 13–20 do S no NATS (`retention: interest`, `max_msgs_per_subject`, 10158/10167/429, `_INBOX`, `space` estrangeiro, contrato em toda checagem, permissão FATAL) | ok, §10.7 | `TestNATSInterestStreamIsRefused`, `TestNATSValidateRetentionAndPerSubjectCap`, `TestClassifyNATS`, `TestNATSPermissionViolationOnTheDataPathIsFatal`, `TestNATSCoreWarnsOnceAboutAForeignSpace`, `TestNATSForeignSubjectIsNotPartOfTheContract` |
| Pendências 23–26 do S (23 temporário e gêmeo, 24 quarentena durável, 25 gauges, 26 `sink=none`) | ok onde se aplica, §12; 26 N/A | `TestBootRemovesTheTwinOfADumpAndReportsAnInterruptedOne`, `TestQuarantineWithAFailedDirectoryFsyncIsQuarantined`, `TestQuarantineAndRemovalReportTheirOutcome`, `TestEmergencyGaugesRecountTheDiskAfterReingestion` |
| Pendências 28–29 do S (primeiro sinal tolerado no FATAL; aborto conta o dump) | ok, §13.4 | `TestFatalShutdownSignals`, `TestFatalShutdownToleratesTheFirstSignal`, `TestAbortDuringTheDumpCountsTheLeftovers` |
| Pendência 30 do S (erros sem eco) | ok, §16.2, e **estendido** ao INI | `TestConfigErrorsDoNotEchoSecrets`, `TestParserErrorsDoNotEchoValues`, `TestINIUnknownKeyNeverPrintsASecretContinuation` |
| Pendências 1–3 do S (custo por conexão, teto por IP, PROXY no shutdown) | ok, §4.4, §4.6, §5.2 | `TestRELPReaderReleasesALargeBuffer`, `TestConnLimitsPerIPAreSharedAcrossListeners`, `TestProxyShutdownMidHeaderIsNotInvalid`, `TestProxyCloseBreaksAHeaderWait` |
| Pendências 5–12 do S (Redis, PostgreSQL, MySQL) e 22 (libpq) | N/A: destinos removidos | — |
| Pendência 21 do S (piso do beacon) | ok, §11 | `TestBeaconIntervalHasAFloor`, `TestBeaconTinyIntervalIsFloored`, `TestNATSURLAndBeaconReviewFindings` |
| Pendência 27 do S (log da recusa fatal no boot) | ok, §13.1 | `TestBootRefusalLogNamesItsCause` |
| Rate-limit que segura (8a) | N/A: sem rate-limit (decisão 4) | — |
| Filtrada recebe `200` | N/A: sem filtro; a vazia continua `200` | `TestRELPEmptyIsAcknowledged` |

**Do SN** (entrada SP/SN de 2026-10-07, portes de 2026-10-11, §21.1 dele):

| Correção | Aqui | Teste |
|---|---|---|
| `ErrMaxPayload` recusa só aquela mensagem | ok, §10.2 | `TestNATSMaxPayloadMidChunkIsRefusedAlone`, `TestNATSOversizedMessageIsRefusedAlone` |
| Permissão negada FATAL pelo veredito (assíncrono), salvo o subject do beacon | ok, §10.7 | `TestNATSPermissionDeniedIsFatal`, `TestNATSCoreDeniedPublishIsFatalAtOnce`, `TestNATSJetStreamDeniedPublishIsFatalAfterTheTimeout` |
| `%`, `/`, `#`, `?`, `,` sem escape numa senha: exit 1 sem eco | ok, §13.2 | `TestNATSURLAndBeaconReviewFindings`, `TestNATSURLsAreParsedWithoutEcho`, `TestCommaInAPasswordSplitsTheURLListWithAHint` |
| *userinfo* na URL junto de `username`/`token`/`credentials`: exit 1 | ok, §13.2 | `TestNATSURLAndBeaconReviewFindings` |
| Piso de `1s` do `beacon_interval` | ok, §11 | `TestNATSURLAndBeaconReviewFindings`, `TestBeaconIntervalHasAFloor` |
| Token fora do log de conexão | ok, §10.7 | `TestNATSConnectLogsNoCredentials` |
| Contrato do stream em toda checagem; `max_payload` em toda checagem de `core` | ok, §10.7 | `TestNATSStreamEditedAtRuntimeIsFatal`, `TestNATSCoreMaxPayloadIsCheckedOnEveryCheck` |
| Recusas de autenticação no `CONNECT` fatais | ok, §10.7 | `TestNATSRefusedCredentialsAreFatal` |
| JetStream desligado: FATAL sem a sonda `AccountInfo` | ok, §10.7 | `TestNATSJetStreamDisabledIsFatal` (servidor sem JetStream) |
| `TLSCertCB` que recusa; nenhum `ClientHello` | ok, §10.7 | `TestNATSOptionsNeverAskForTLS`, `TestNATSServerRequiringTLSIsNeverDialledWithTLS`, `TestNATSRealServerRequiringTLSIsRefusedWithoutAHandshake` |
| `rename` sem *hard link*; temporário e gêmeo | ok, §12.1 | `TestWriteOnAFileSystemWithoutHardLinks`, `TestBootRemovesTheTwinOfADumpAndReportsAnInterruptedOne` |
| Flags (`knownFlag`, `prev`, `after`) e INI sem eco | ok, §13.2 | `TestReviewFlagTailEcho`, `TestParserErrorsDoNotEchoValues` |
| Pendências 1–3 do SN (custo por conexão, teto por IP, PROXY) | ok pelo R | ver acima |
| Pendência 4 do SN (offset na RFC 3164) | decidida no R (segue a skill), §6.5.1 | vetores e corpus |
| Pendências 5–8 do SN (`interest`, 4xx transitórios, `_INBOX`, `space` estrangeiro) | ok pelo R | ver acima |
| Pendências 9–11 do SN (gauges, log falso de remoção, `SIGQUIT` tolerado) | ok pelo R | `TestEmergencyGaugesRecountTheDiskAfterReingestion`, `TestQuarantineAndRemovalReportTheirOutcome`, `TestFatalShutdownSignals` |
| "Não corrigido" de 2026-10-11: *flush* de `core` interrompido por reconexão vira FATAL | **corrigido aqui** (`flushErr`) | `TestNATSCoreFlushCutByAReconnectionIsNetwork` |
| "Não corrigido" de 2026-10-11: `-ERR` do beacon encobre a negação de um pedaço | ok pelo R (negação mascarada) | `TestNATSCoreMaskedDenialIsNotConfirmed` |

---

## 3. Riscos aceitos

- **`kill -9` e OOM perdem o backlog do ring**, já confirmado ao rsyslog (§19).
  Quem não aceita usa o `ring=sqlite` do R.
- **`core` desfaz a promessa do RELP**: o `200 OK` sai e ninguém guardou. É
  opt-in e o boot avisa.
- **Admin na mesma porta do R e do RP** (`127.0.0.1:9103`, a regra da família,
  como S, SP e SN dividem a 9102; mantida no porte de 2026-10-13): rodar dois
  deles no mesmo host exige trocar o `admin.bind` de um (§14).
- **Um `500` ainda é possível** (guarda de origem, protocolo do cliente), e um
  `500` custa duplicatas na janela (§5.4). Nenhum `omrelp` sadio chega lá.
- **O RELP não deduplica retransmissões** (§19): at-least-once entre vidas e
  sessões; dentro de uma vida, o `Nats-Msg-Id` deduplica retries e dumps.

## 4. Ordem de entrega

1. Cópia do R; corte dos pacotes que saem; `ring`, `sink`, `control`,
   `emergency` e o grosso de `app` trazidos do SN por merge de três vias
   (R, S, SN), conflito a conflito.
2. `receiver` sem TLS, filtro, rate-limit e `ring=none`; `ingest` do SN com os
   desfechos do RELP.
3. `config` cortado (registry, validação, `LOGHUB_EB_URL`, `removed.go`).
4. Testes portados dos dois pais e adaptados; servidores NATS reais (com e sem
   JetStream, com permissões, com `tls_required`) e o rsyslog real.
5. Fuzz de 60 s nos cinco alvos.
6. Revisão independente por área (RELP; app/shutdown/dump; config/URL; sink
   NATS/beacon; docs), cada achado corrigido com um teste que falhava antes.
7. Docs e `docker/`.
8. Porte da revisão independente do RP (2026-10-13), no código comum: 26
   itens, cada correção conferida por mutação (`docs/TESTING.md` §9.1); os
   tetos de `sink.workers` e `sink.batch_max_bytes` escolhidos para o NATS
   (§2.2).
