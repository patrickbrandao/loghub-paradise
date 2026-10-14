# go-relplog-nats-server — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Escrito
em 2026-10-13, na criação do corte a partir do `go-relplog-server` (R) e do
`go-syslog-nats-server` (SN). Comandos a partir da pasta do módulo.

**Estado (2026-10-13, depois do porte da revisão do RP, §9.1):** `gofmt` e
`go vet` limpos; `go test -race -count=1 ./...` verde **duas vezes seguidas**
(49 s e 48 s) nos **13 pacotes** contra quatro `nats-server` 2.14.6 reais (com
JetStream, sem JetStream, com permissões negadas, com `tls {}`) e com
`RELPLOG_TEST_EMITTERS=1` (o rsyslog real de §7): **768 testes e subtestes**
(435 de primeiro nível e 333 subtestes), **767 passam**, **1 pula** — o vetor
`arquivo-sem-pri` (`transport=file`, fora por D5). Cobertura
`-coverpkg=./...`: **91,6%**. Fuzz de 60 s nos três alvos dos pacotes cujo
código mudou no porte, sem falha (§3). A revisão independente por área (§9)
achou e corrigiu 17 defeitos; o porte da revisão do irmão RP (§9.1) trouxe 26
itens do código comum, cada um conferido por mutação (a correção revertida, o
teste falha).

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. **Sem `RELPLOG_TEST_NATS_URL`**, a integração da app
que passa do passo 7 do boot, a do sink real e a do beacon real pulam (§5, §6):
sobram as recusas de boot de antes do passo 7, os testes de contagem do
shutdown, o servidor NATS falso dos testes do sink (`natsFake`) e os testes de
pacote. Como o NATS é o único destino, a prova ponta a ponta exige o servidor:
rode §6 antes de considerar uma mudança pronta.

Cobertura (2026-10-13, depois do porte de §9.1, com os quatro servidores,
`-coverpkg=./...`, unindo os perfis de todos os pacotes):

| Pacote | Cobertura |
|---|---|
| `app` | 83,6% |
| `config` | 93,3% |
| `control` | 96,2% |
| `convert` | 91,0% |
| `emergency` | 90,5% |
| `ident` | 81,0% |
| `item` | 89,7% |
| `jlh` | 97,9% |
| `jlhcheck` (só teste) | 78,5% |
| `obs` | 94,8% |
| `receiver` | 95,0% |
| `ring` | 86,9% |
| `sink` | 94,9% |
| `cmd/go-relplog-nats-server` | 0% (o `main` roda num subprocesso do `binary_test.go`, fora do perfil) |
| **total** | **91,6%** |

```bash
go test -race -coverpkg=./... -coverprofile=cover.out ./... && go tool cover -func=cover.out | tail -1
```

---

## 2. Vetores dourados, contratos e corpus (desvio D3, SPEC §18.2)

```bash
go test -race -run 'Golden|Schema|Corpus|Semantic' -v ./internal/convert/ ./internal/jlhcheck/
```

- **Os vetores** de `loghub-type-syslog-schema`
  (`internal/jlhcheck/loghub/vectors.json`): 23 de 24 (`arquivo-sem-pri` fica
  fora, `transport=file`). O vetor com `transport=tls` roda como TCP, como no
  SN: `payload.tls` retirado do esperado e o `sid` recalculado (SPEC §6.10). Os
  de UDP e unix rodam como estão: provam o conversor, cujo enum de transporte
  continua inteiro (SPEC §3.4).
- **O oráculo** (`internal/jlhcheck`): os dois schemas mais as regras
  semânticas do `validate_payload.py` da skill, o conjunto completo de espaço em
  branco nos campos `^\S+$` (D6) e os exemplos de `beacon`.
- **Corpus de linhas reais** (`internal/convert/testdata/corpus/`): o mesmo do
  R; cada saída passa no oráculo; o teste falha abaixo de vinte entradas.
- **`x-transport: "relp"`**: todo envelope produzido pela app (integração,
  emissores) passa pelo `checkMsg` dos testes, que exige `x-transport`,
  `framing: "octet-counting"` e `transport: "tcp"` (SPEC §20.25).

---

## 3. Fuzz (SPEC §18.3, obrigatório antes de release)

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$ -fuzztime=600s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$      -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$   -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$   -fuzztime=600s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$    -fuzztime=600s
```

São os cinco alvos que existem neste módulo (`grep -rn '^func Fuzz'`). O
`FuzzHead` do R saiu com o codec do cabeçalho; o `FuzzJSONBSafe` e o
`FuzzInboxPayload`, com o `sink=postgres`; o `FuzzFrame` daqui é o do R (a
moldura RELP contra um oráculo), não o par `FuzzFrame`/`FuzzFrameOracle` das
molduras RFC 6587 do SN.

Rodada de 2026-10-13, 60 s por alvo (Apple M2, 8 *workers*), sem falha:

| Alvo | Execuções | Entradas novas |
|---|---|---|
| `FuzzConvert` | 376 447 | 223 |
| `FuzzSD` | 2 626 760 | 114 |
| `FuzzFrame` | 5 762 064 | 64 |
| `FuzzProxy` | 3 517 882 | 18 |
| `FuzzRead` | 206 135 | 5 |

A revisão do RELP rodou `FuzzFrame` e `FuzzProxy` mais 20 s cada, sem achado.
A rodada de 10 min por alvo não foi feita (§8).

Rodada do porte de §9.1 (2026-10-13), 60 s nos alvos dos pacotes cujo código
mudou (`receiver` e `emergency`; `convert` não mudou), sem falha:

| Alvo | Execuções | Entradas novas |
|---|---|---|
| `FuzzFrame` | 5 691 883 | 24 |
| `FuzzProxy` | 3 289 775 | 17 |
| `FuzzRead` | 249 530 | 1 |

O `FuzzProxy` com o corpus do cache do Go desta máquina **parava de executar**
depois de ~30 mil execuções (0/s até o fim dos 60 s, e `PASS`), o mesmo na
cópia de antes do porte; com `-test.fuzzcachedir` vazio, os números acima. É o
motor com uma entrada do cache, não o alvo (que nunca falhou nem estourou o
prazo); fica registrado para quem vir o 0/s.

---

## 4. Benchmarks

```bash
go test -run=XXX -bench='BenchmarkRELPSession|BenchmarkIngestAccept' -benchtime=3s ./internal/receiver/
go test -run=XXX -bench=BenchmarkRELPReaderFrames -benchmem ./internal/receiver/
go test -run=XXX -bench=. -benchmem ./internal/convert/
```

| Benchmark | 2026-10-13 (Apple M2) | Leitura |
|---|---|---|
| `RELPSession` | 1 886 ns/op, **~530 mil msg/s** | uma sessão RELP sobre loopback, janela cheia, `200` depois do ring em memória, sem sink (no R: ~486 mil, a mesma peça com filtro e rate-limit desligados) |
| `IngestAccept` | 102,8 ns/op, 224 B/op, 1 alloc/op | recepção até o ring (moldura já feita); a alocação é o próprio `Item` |
| `RELPReaderFrames/200`, `/12288`, `/24576`, `/61440` | 76 ns, 377 ns, 528 ns, 1,15 µs; 0 B, 0 allocs em todos | frames `syslog` seguidos pelo `bufio.Reader` de 4 KiB da sessão. Antes da correção E2 (§9.2) o buffer acima de 16 KiB era solto antes de todo frame: `/24576` 12,3 µs, 55 KB, 5 allocs; `/61440` 37,7 µs, 129 KB, 6 allocs |

Não há benchmark do sink NATS: o custo de publicação depende do servidor e da
rede, e só uma carga contra um servidor real o mede (§8).

---

## 5. Integração em processo (`internal/app`)

```bash
RELPLOG_TEST_NATS_URL=nats://127.0.0.1:14292 go test -race -timeout 900s ./internal/app/
```

A pilha **real** (a mesma `run()` do binário), por sessões RELP de um cliente de
teste que fala como o `librelp` (`helpers_test.go`: `dialRELP`, `sendRELP`,
`expectServerClose`), contra o `nats-server` real. Cada teste tem um `space`
próprio e um stream próprio (`RLNATS_APP_*`), criado a partir de
`docker/nats/stream.json` e apagado no fim. Falhas do destino entram decorando o
sink real (`faultySink`).

| Área | Testes (exemplos) |
|---|---|
| Transporte, PROXY v1/v2 (IPv6 inclusive), labels | `TestAllTransportsReachTheStream`, `TestProxyOverTCPReplacesTheAddresses`, `TestLabelsAreAClosedSet` |
| `200` só depois do ring; ring cheio e sink DOWN segurados, sem `500` | `TestFullRingHoldsTheTransaction`, `TestSinkDownHoldsTheTransaction` |
| Fidelidade do JLH (NUL no meio, `x-transport`) | `TestNATSPublishStoresTheJLH`, `TestEmergencyDumpAndReingestionAreExact` |
| Dump e reingestão (em fluxo, danificado, gêmeo, desligado, gauges, prazo sem progresso) | `TestDamagedDumpIsQuarantined`, `TestBootRemovesTheTwinOfADumpAndReportsAnInterruptedOne`, `TestDumpIsReingestedEvenWithEmergencyDisabled`, `TestEmergencyGaugesRecountTheDiskAfterReingestion`, `TestLongReingestionStillRetriesATransientFailure` |
| Shutdown, sinais e contagem da perda | `TestSecondSignalAborts`, `TestFatalShutdownSignals`, `TestAbortDuringTheDrainIsCountedOnce`, `TestDumpAfterTheAbortIsAbandoned`, `TestSIGQUITAfterTheEntryCheckStillAborts`, `TestDrainTimeoutDumpsAndExitsThree` |
| Boot (ordem, recusas, bind cancelado) | `TestBootRefusalLogNamesItsCause`, `TestUnreachableSinkKeepsTransportsShut`, `TestCancelledBindIsAnAbortNotAConfigError` |
| `SIGHUP` | `TestSIGHUPReload`, `TestReloadReportsShadowedKeys`, `TestBinarySIGHUPAppliesTheLogLevel` |
| Chaves e variáveis do R | `TestRemovedKeysAreUnknown`, `TestRemovedEnvIsWarned` (em `config`), a recusa no binário |
| Identidade (100–114) | `binary_test.go`, num subprocesso |

---

## 6. NATS

```bash
RELPLOG_TEST_NATS_URL=nats://127.0.0.1:14292 \
RELPLOG_TEST_NATS_NOJS_URL=nats://127.0.0.1:14293 \
RELPLOG_TEST_NATS_PERM_URL=nats://127.0.0.1:14294 \
RELPLOG_TEST_NATS_TLS_URL=nats://127.0.0.1:14295 \
  go test -race -count=1 -timeout 900s ./...
```

Os quatro servidores são o `nats-server` **v2.14.6**, cada um num processo
(`nats-server -c <arquivo>`), com estas configurações:

```
# 14292: JetStream
listen: 127.0.0.1:14292
jetstream { store_dir: "/tmp/rlnats-js" }

# 14293: sem JetStream (o FATAL imediato de SPEC §10.7)
listen: 127.0.0.1:14293

# 14294: permissões (os vereditos de SPEC §10.7)
listen: 127.0.0.1:14294
jetstream { store_dir: "/tmp/rlnats-perm" }
authorization { users = [
  { user: admin, password: admin }
  { user: nopub, password: nopub, permissions: {
      publish: { allow: [">"], deny: ["loghub.*.syslog"] },
      subscribe: { allow: [">"] } } }
  { user: noinbox, password: noinbox, permissions: {
      publish: { allow: [">"] }, subscribe: { deny: ["_INBOX.>"] } } }
  { user: nobeacon, password: nobeacon, permissions: {
      publish: { allow: [">"], deny: ["*.beacon"] }, subscribe: { allow: [">"] } } }
] }

# 14295: tls_required (um certificado RSA autoassinado; o LibreSSL do macOS
# gera um EC que o nats-server recusa com "invalid ECDSA parameters")
listen: 127.0.0.1:14295
tls { cert_file: "cert.pem", key_file: "key.pem" }
```

O que as suítes provam, além do que §5 lista:

- **O contrato do stream**, no boot e em toda checagem: existe, cobre o subject
  do Workspace, nunca o plano de controle, não selado, `no_ack` falso,
  `retention` diferente de `interest`, `max_msgs_per_subject` com `WARN`, a maior
  mensagem cabe em `max_payload`/`max_msg_size`/registro do file store e, com
  `discard: new`, em `max_bytes` (`TestNATSValidate*`, `TestNATSInterestStreamIsRefused`,
  `TestNATSStreamEditedAtRuntimeIsFatal`). O arquivo `docker/nats/stream.json`
  é conferido contra o contrato por um teste sem servidor.
- **JetStream desligado** é FATAL na primeira checagem (`TestNATSJetStreamDisabledIsFatal`).
- **Permissões**: publish negado num subject de dados é FATAL (em `core` na
  hora, em `jetstream` depois do `op_timeout`), assinatura de `_INBOX` negada
  também, e a negação no subject do beacon não (`TestNATSPermissionDeniedIsFatal`
  e vizinhos).
- **TLS**: nenhum `ClientHello` sai para um servidor com `tls_required` — um
  proxy que grava os bytes prova que o primeiro byte do cliente nunca é `0x16`
  —, e o boot sai com 3 nomeando o TLS
  (`TestNATSRealServerRequiringTLSIsRefusedWithoutAHandshake`); depois do boot,
  um servidor que passa a exigir TLS vira veredito na checagem seguinte
  (`TestNATSReconnectToATLSOnlyServerIsAVerdict`, contra o servidor falso).
- **Reconexões em `core`**: um *flush* cortado por uma queda é `network`, não
  FATAL (`TestNATSCoreFlushCutByAReconnectionIsNetwork`), e um que atravessa uma
  reconexão não confirma (`TestNATSCoreFlushAcrossAReconnectionIsNotConfirmed`).
- **Classificação**: a tabela de SPEC §10.7, linha a linha (`TestClassifyNATS`).
- **O beacon** (`internal/control`): envelope válido pelo oráculo, na primeira
  conexão, em toda reconexão e no intervalo, piso de `1s`, falha contada sem
  tocar a saúde.

---

## 7. Emissores reais (SPEC §18.7)

```bash
RELPLOG_TEST_NATS_URL=nats://127.0.0.1:14292 RELPLOG_TEST_EMITTERS=1 \
  go test -race -count=1 -timeout 900s -run TestEmitters -v ./internal/app/
```

Sobe o rsyslog real (Alpine 3.22, `rsyslog` 8.2410 e `rsyslog-relp` com o
`librelp` 1.11; imagem `rlnats-test-rsyslog:alpine-3.22`, construída na primeira
rodada) em contêineres descartáveis `rlnats-emit-*` contra o servidor em
processo. Precisa de Docker; os contêineres são removidos no fim.

| Teste | Resultado (2026-10-13) |
|---|---|
| `TestEmittersRELPInClear` | 25/25 no stream, todas com `x-transport: "relp"`, nenhum `500` (12,9 s) |
| `TestEmittersRELPBackpressureRetransmits` | ring cheio além do `backpressure_timeout`: a sessão fecha sem resposta, o rsyslog reconecta e retransmite; 20/20, **0 duplicatas**, 2 conexões, nenhum `500` nem descarte (4,6 s) |
| `TestEmittersRELPSurvivesARestart` | sink FATAL com transações em voo, dump e exit 3, servidor novo no mesmo `data_dir`: reingestão e o rsyslog retransmitindo o resto; 20/20 (3,6 s) |

O `TestEmittersRELPFiveHundredIsFinal` do R (o `500` da recusa `logic` com
`ring=none`) e o cenário TLS saíram com o que provavam.

---

## 8. Lacunas conhecidas e pendências

Registradas por honestidade, não por esquecimento. Os defeitos do **código**
estão em SPEC §22.1; aqui ficam os do que os testes provam.

### 8.1 Pendências conhecidas

Nenhuma desde a rodada de 2026-10-14 (§9.3): os itens 1–3 e 6 desta lista
eram as pendências SPEC §22.1, hoje regra (SPEC §22.1 diz onde). Ficam as duas
arestas de forma, sem efeito:

1. **`dumpEmergency` devolve um `bool` que ninguém lê** em produção (os testes
   leem); o comentário ainda diz "reports whether they were saved". Mantido
   para a forma ficar igual à do SN.
2. **`validator.setIn` e `validator.notes`** no `config` são resíduo do R (o
   rebaixamento de `sink.workers` dos destinos SQL): preenchidos, mas sem
   efeito nenhum. Mantidos para o diff com o R ficar pequeno; inofensivos. (A
   nota `derived` de `emergency.dir`, que o `SIGHUP` usa desde 2026-10-14,
   SPEC §16.3, vem do `Load`, não do validador.)

### 8.2 O que os testes ainda não provam

1. **Fuzz de 10 min por alvo** não rodou (§3): só 60 s.
2. **Nenhuma medição ponta a ponta** (SPEC §18.8): o `BenchmarkRELPSession` mede
   a recepção sem sink; o RELP contra o JetStream real, com uma conexão do
   rsyslog e a janela de 128, não foi medido, nem a perda zero sob `SIGTERM` com
   carga.
3. **`WithPublishAsyncMaxPending`**: a folga de 2× (SPEC §10.7) não tem teste
   próprio; tirar o `WithPublishAsyncMaxPending` inteiro continua sobrevivendo à
   suíte (o padrão do `nats.go` é 4000, acima do que os testes publicam).
4. **Com várias URLs**, o `LastError` reflete só a última tentativa (herdado do
   SN). Desde 2026-10-14 a recusa de TLS de um nó do *pool* nunca é veredito
   (SPEC §10.7, `soleServer`), então a ordem só muda o **motivo** que o erro
   `network` nomeia, provado com dois servidores falsos
   (`TestNATSOneTLSServerOfAPoolIsNoVerdict`,
   `TestNATSBootTLSRefusalOfAPoolIsNetwork`). O *pool* anunciado por um
   cluster (`connect_urls`) não tem teste próprio aqui: o `nc.Servers()` é o
   mesmo, e o R/F o provam com `nats-server` em cluster.
5. **O ramo `ErrMaxBytesExceeded`** da classificação é inalcançável (só existe
   em consumidores *pull*).
6. **Uma corrida teórica entre `Health` e `Close`** pode deixar uma conexão sem
   fechar no fim do processo (herdado do SN).
7. **O `docker/`** foi subido de ponta a ponta uma vez (2026-10-13, Docker
   Desktop no macOS, `docker compose --profile demo up -d --build`): imagem
   construída, `nats-init` criou o stream, o rsyslog de demonstração começou
   antes do servidor, ficou suspenso e retomou sozinho, 132 de 132 mensagens no
   `LOGHUB_SYSLOG` sem nenhum `500`, dois beacons, `/readyz` 200, `SIGHUP`
   aplicado e `docker compose stop` com exit 0. Não foi provado num Linux, nem com
   o `LOGHUB_EB_URL` no lugar das chaves de conexão.
8. **`RELPLOG_TEST_EMITTERS`** precisa de Docker; o contêiner alcança o
   servidor em processo por `host.docker.internal` (`--add-host
   host.docker.internal:host-gateway`), provado só no Docker Desktop do macOS.
9. **A ordem `watchAbort` antes de `stopSignals`** (um segundo sinal entre os
   dois teria a ação padrão do Go, 143) só existe no binário: em processo o
   `stopSignals` é vazio, e o mutante que inverte a ordem sobrevive (M24 da
   revisão do RP, código comum). `binary_test.go` cobre os sinais fora da
   janela de `run()`, não essa.
10. **A disputa de linha de cache dos contadores da recepção** sob
    `SO_REUSEPORT` (a antiga pendência SPEC §22.1 item 6, registrada em
    2026-10-14) não foi medida. Os contadores não têm *padding*, mas o mesmo
    contador (`messages_total{transport="tcp"}`) é incrementado por **todos**
    os listeners: a disputa principal é *true sharing* da mesma palavra, que
    *padding* não remove. O `BenchmarkRELPSession` com vários listeners é o
    lugar de medir.
11. **`TestNATSOversizedMessageIsRefusedAlone` pula** contra o `nats-server` da
    infraestrutura comum (`tasks/infra`, porta 24222), que tem `max_payload`
    de 8 MiB: o teste precisa de um servidor abaixo de 4 MiB (o de §6 tem).
12. **`TestNATSReconnectToATLSOnlyServerIsAVerdict/jetstream`** leva ora 0,6 s,
    ora os 20 s do prazo do contexto: um `Health` que pega a conexão ainda
    `CONNECTED` logo depois do `drop` espera o `StreamInfo` que o servidor
    falso nunca responde. Passa sempre; igual com o `nats.go` de antes de
    2026-10-14 (conferido: 0,61/20/20/20 s).
13. **A soltura do buffer do leitor RELP vale só entre frames** (A5 e D4 do S,
    2026-10-14; SPEC §5.2): um frame grande seguido de alguns bytes do próximo
    segura o buffer até o idle. Não corrigido (medido no S) e sem teste que
    fixe o número.
14. **`sameStreamSocket` com nome de serviço** (revisão de 2026-10-14) é
    provado com `http` (a tabela embutida do Go) e com um nome inexistente; um
    nome que só o `/etc/services` conhece depende do arquivo da máquina ou da
    imagem.
15. **A transação segurada desde antes do primeiro `Accept`** (A2 do RELP,
    2026-10-14) é provada com um ring falso cujo `Enqueue` espera o teste e
    devolve `ErrFull`, e o idle vence pelo relógio (200 ms contra 700 ms de
    espera); sem teste com o `sqlite`/`redis`/`none` de verdade.
16. **NATS: *pool* só de servidores que recusam e uma URL com vários IPs** (D2
    do syslog, D3 do RELP, 2026-10-14; SPEC §10.7): registrados como limite,
    sem teste; o primeiro precisaria de dois `nats-server` com TLS recusado, o
    segundo de um nome com vários IPs.

---

## 9. Revisão independente (2026-10-13)

Cinco revisões somente-leitura, cada uma numa cópia (RELP; app, shutdown e
dump; config e `LOGHUB_EB_URL`; sink NATS e beacon; docs). Cada achado foi
corrigido com a SPEC primeiro e um teste que **falhava antes** — conferido numa
cópia do código anterior à correção — ou, nas lacunas de teste, que mata o
mutante.

| Área | Achado | Correção | Teste |
|---|---|---|---|
| sink | Servidor que passa a exigir TLS depois do boot: o `ReconnectErrHandler` nunca vê a recusa (é do *handshake*, não da discagem), e o FATAL vinha dos fusíveis com "connection is RECONNECTING" | `noteLastError` e o `escalate` no `Health` com o cliente `RECONNECTING` | `TestNATSReconnectToATLSOnlyServerIsAVerdict` |
| sink | *Flush* de `core` que **começa** durante a reconexão confirmava o pedaço cujos `PUB` foram pelo socket velho | contagem de reconexões antes e depois | `TestNATSCoreFlushAcrossAReconnectionIsNotConfirmed` |
| sink | `MaxPending` sem folga: uma retentativa rápida parava na janela de futuros | `2 × workers × batch` | — (§8.2 item 3) |
| sink | Lacunas: tirar o `tainting()` de duas saídas, o `maskedDenial` do `writeCore`, o `noteLastError` do `Health` em `core` ou o `WithRetryAttempts(0)` não quebrava nada | testes novos, cada mutante morto | `TestNATSPublishErrorAfterTheFirstTaints`, `TestNATSLogicRefusalInAnAckTaints`, `TestNATSCoreMaskedDenialIsNotConfirmed`, `TestNATSCoreHealthReadsTheDenialItsFlushFollows`, `TestNATSLibraryNeverRepublishes` |
| app | Aborto durante `Pending`/`Drain`: o contador era reposto depois e um dump desligado contava de novo (10 para 5); e uma janela entre as duas escritas da fase de dump | `lossMu`, `enterDumpPhase`, `oweDump` | `TestAbortDuringTheDrainIsCountedOnce`, `TestAbortBeforeTheDumpPhaseKeepsItClosed` |
| app | Depois do aborto, o dump podia terminar e ser reingerido, contado como perdido | `Store.SetAbandon` | `TestDumpAfterTheAbortIsAbandoned` (e o `settle` dos testes de sinais agora prova que nada chega ao disco) |
| app | `SIGQUIT` cujo observador marcava a saída logo depois da conferência de entrada | o canal `quitNow` no `select` | `TestSIGQUITAfterTheEntryCheckStillAborts` |
| app | `relplog_emergency_events_pending` mostrava o tamanho do pedaço durante a reingestão | a linha saiu | `TestReingestionDoesNotShowTheChunkAsPending` |
| app | Reingestão longa que passava do `boot_timeout` saía no primeiro soluço, sem retentativa | prazo sem progresso | `TestLongReingestionStillRetriesATransientFailure` |
| emergency | `open` do diretório falho depois do `unlink` dava "cannot remove" | `ErrNotDurable` também ali | `TestRemoveWithAnUnopenableDirectoryIsNotDurable` |
| app | Bind de um nome de host cancelado por um sinal saía com 1 | 130 quando o contexto foi cancelado | `TestCancelledBindIsAnAbortNotAConfigError` |
| config | Chave do R depois de uma flag com valor culpava a flag anterior | `removedSections`/`removedParams`, mensagem que nomeia o R | `TestRemovedFlagAfterAValuedFlagIsNamed` |
| config | Segredo que quebra de linha no INI aparecia no erro (e no log do `SIGHUP`) | `lookupINI` | `TestINIUnknownKeyNeverPrintsASecretContinuation` |
| config | `,` numa senha partia a lista com uma mensagem enganosa; `scheme "none"` | mensagem com a dica | `TestCommaInAPasswordSplitsTheURLListWithAHint` |
| receiver | Contexto cancelado num erro inesperado do ring: `ERROR` a cada shutdown | `Shutdown`, sem log, como no R | `TestIngestEnqueueErrorDecidesWhetherTheBufferIsPooled` |
| receiver, app, config | Comentários herdados de dois transportes, de UDP e unix e do TLS | corrigidos | — |
| docs | Decisões do S creditadas ao R, prefixo do cliente do R (`rls/`), o `500` do R descrito como de rate-limit, referências pendentes (TESTING, §10.3), a frase do `WARN` de `core`, a ordem de fechamento, rótulos das pendências 23–26 do S, citações `§22.1 item N` nos testes que eram do S | corrigidos | — |

**Conferido e correto** pelas revisões (resumo): o mapeamento dos desfechos em
respostas, a semântica de segurar, a ordem janela → teto → cópia, a posse de
buffer, a amostra única de relógio, `0 serverclose 0\n`, o `Close` que espera as
sessões; a ordem do boot, o `ParseLine` idêntico ao do SN, a reingestão por
classe, a tolerância do primeiro sinal; as 92 variáveis de `removed.go` contra o
`--help` do R, as 56 chaves do registry iguais às do R em tipo, padrão, recarga,
redação e nome; nenhuma correção do R ou do SN perdida no sink (lista em
`docs/DECISOES.md` §2.3).

### 9.1 Porte da revisão independente do RP (2026-10-13)

O irmão `go-relplog-pg-server` (RP) passou pela própria revisão independente
no mesmo dia e corrigiu código que os dois servidores têm em comum (receiver,
ring, item, emergência, shutdown, config, `Instrumented`, chunk). Pela regra de
sincronia (RP e RN só diferem onde SP e SN diferem), cada item foi conferido
aqui, portado com o teste adaptado ao NATS e conferido por **mutação**: a
correção revertida numa cópia do módulo, o teste falha.

| # | Achado (no RP) | Aqui | Teste |
|---|---|---|---|
| 1 | Pré-checagem do ring cheio só pelos itens: com `ring.max_bytes` limitando, cada retentativa de 50 ms cunhava `uuid`, copiava e montava o item | portado: `ring.Memory.FullFor(n)` com o tamanho truncado; ring fechado nunca "cheio" (SPEC §2.2, §5.7) | `TestIngestByteCeilingIsRefusedBeforeTheCopy`, `TestIngestByteCeilingChargesTheTruncatedLine`, `TestIngestClosedFullRingIsTheShutdown` |
| 2 | `try()` não conferia o fim da sessão logo antes do `Enqueue` | portado: `ctx.Err()` → `Release` + `Shutdown`; `TestRELPMalformedFrameClosesAfterTheAnswers` espera o item no ring (SPEC §5.7) | `TestIngestEndedSessionIsNotEnqueued` |
| 3 | Ramo UDP morto no `ingest.go`; `udpWire`, `FramingNonTransparent` e `TestIngestUnixHasNoAddresses` nos testes | portado: ramo e teste removidos, `tcpWire` octet-counting, os testes do sink com `FramingOctetCounting` e a porta 20514 | a suíte |
| 4 | Mutantes do RELP sem teste (contexto depois do `Acquire`, cópia antes da vaga, `server_closed` no segundo `open`) | portado (o código já era o mesmo) | `TestDispatchAfterSessionEndStartsNothing`, `TestCopyComesAfterTheWindowAndTheBudget`, `TestSecondOpenLeavesItsHeldTransactionServerClosed` |
| 5 | `TestRELPUnexpectedRingErrorIsHeldNot500` não provava a retentativa | portado o teste do RP (duas tentativas, o item no ring), com o nome dele | `TestRELPUnexpectedRingErrorIsHeldNotRefused` |
| 6 | `Deliver`: recusa `logic` numa escrita durante a qual veio o `SIGTERM` era contada e não logada | portado: `ClassLogic` antes do contexto (SPEC §10.1) | `TestDeliverLogicRefusalWinsOverTheCancellation` |
| 7 | Mutantes do chunk sem teste (buffer manchado ou grande no pool, `Seal` sem limite, `Reset` sem `clear`) | portado (o chunk é comum) | `TestTaintedChunkBufferIsNotPooled`, `TestOversizedChunkBufferIsNotPooled`, `TestSealLimitsEachEnvelopeToItsBytes`, `TestResetClearsTheEnvelopeSlots` |
| 8 | `TestWorstCaseJLHIsBounded` sem o `x-transport` da produção | portado; comentário do `MaxEventBytes` | `TestWorstCaseJLHIsBounded` |
| 9 | Dump que completa depois do aborto ainda contava `malformed` (A7) | portado: `owed == 0` depois do `Write` só loga (SPEC §13.4) | `TestAbortAfterTheLastAbandonCheckCountsNothingMore` |
| 10 | Testes do aborto copiavam o shutdown à mão (M06, M28); com 4 workers o do dump nunca exercia o `dumpPhase` (M07) | portado: gancho `beforeCollect` no shutdown real; teste com `--sink.workers=1` | `TestAbortDuringTheCollectionCountsOnceAndDumpsNothing`, `TestAbortDuringTheCollectionPublishesNoDump`, `TestAbortDuringTheDumpWithItemsDrainedFromTheRing` |
| 11 | Mutantes do shutdown sem teste: `SIGQUIT` servindo (M16), dump que fixa os gauges (M18), transação segurada no shutdown (M30), gauge do pedaço no boot real (M32), dump falho e aborto | portados, contra o NATS real | `TestSIGQUITWhileServingCountsTheRing`, `TestShutdownDumpAddsToTheGauges`, `TestShutdownReleasesAHeldTransactionAsShutdown`, `TestPendingGaugeIsNotTheChunkDuringReingestion`, `TestAbortAndFailedDumpCountOnce` |
| 12 | Sinal antes do *bind* do transporte saía com 1 | o código já era o do RN (130); portado o teste do transporte | `TestSignalBeforeTheBindExits130` |
| 13 | `TestDumpIsPublishedOnlyWhenComplete` do R não portado; `Store.Count` sem teste próprio | portados; `Count` alinhado ao do RP (uma linha por vez com o leitor da reingestão, em vez de pedaços de até 4096 registros e 64 MiB) | `TestDumpIsPublishedOnlyWhenComplete`, `TestCount` |
| 14 | `--help`/`--version`/`--dump-config` com `=valor`; `--config=` vazio | portado (SPEC §13.2) | `TestSwitchesTakeNoValue`, `TestEmptyConfigFlagIsRefused` |
| 15 | `--x -h` nomeava `--h` (uma senha de dois caracteres); "only --name=value" com a forma separada aceita | portado: só nomeia com dois traços; "options are --name=value or --name value" | `TestParserErrorsDoNotEchoValues`, `TestConfigErrorsDoNotEchoSecrets` |
| 16 | Valor colado ao nome (`--sink.nats.passwordhunter2`) ecoava o segredo | portado: `extendsAName` | `TestGluedValueIsNotPrinted` |
| 17 | BOM UTF-8 no INI | portado | `TestINIByteOrderMarkIsIgnored` |
| 18 | Derivado escrito como chave no `--dump-config` (aqui `emergency.dir`) | portado: comentado | `TestDumpCommentsDerivedValues` |
| 19 | Sinônimo do charset relatado como mudança no `SIGHUP` | portado: `canonicalCharset` | `TestCharsetSynonymsAreOneValue` |
| 20 | Sem tetos de configuração | portado, com os tetos do NATS em `sink.workers` (256) e `sink.batch_max_bytes` (256 MiB) (DECISOES §2.2, SPEC §13.2) | `TestConfigCeilings` |
| 21 | `LOGHUB_EB_URL` só com espaços ignorada; host em porcento com mensagem que apontava a senha | portado ao NATS (`nats://u:p@%41h`, também em `sink.nats.urls`); usuário e banco obrigatórios, `sslmode`, `?host=` e `://` duplo **não se aplicam** (PostgreSQL) | `TestEBURLOnlyBlanksIsRefused` |
| 22 | `removedParams` sem teste contra `removedEnv`; `removedEnv` sem teste contra o `--help` do R | portados (92 nomes) | `TestRemovedParamsAreTheRemovedEnv`, `TestRemovedEnvIsTheParentMinusThis` |
| 23 | Mutantes da config sem teste (`admin.bind` vazio, `@` cru no `redactOneURL`, eco do esquema) | portado com URLs NATS | `TestReviewMutantGaps` |
| 24 | Textos: `listen.tcp` "empty disables", teto do `max_frame_bytes`, `ring.max_bytes` "raw lines only", `admin.bind` sem o RP, `pre_stop_delay` | portados; o `admin.bind` cita R e RP | por leitura |
| 25 | Comentários velhos (`ring.go` Stats.Bytes, `item.Size`, `admin.go`, ordem do `Ingest`, `review_relp_test.go`) | portados; e a ajuda de `relplog_ring_bytes` ("Raw-line bytes") corrigida aqui, que o RP ainda tem | por leitura |
| 26 | Docs: §22.1 coerente com TESTING §8 e DECISOES; colisão da 9103 com R **e** RP | portado (SPEC §14, §22.1; DECISOES; README; `docker/README.md`) | por leitura |

**Não se aplicam** (só PostgreSQL): a regra `ON INSERT`/`ON UPDATE` e o `0A000`,
`pool_size ≥ workers + 1`, `sink.postgres.addr` com um servidor só, as regras
de URL do PostgreSQL (usuário, banco, `sslmode`, `?host=`), a réplica em
recuperação, o árbitro do `ON CONFLICT`, o *flake* do `53300`.

**Mantidos do RN**, por serem melhores ou próprios: `TestRELPHeldMessageIsChargedOnce`
(o F1 de 2026-10-05 que o RP removeu), o `conv()` dos testes da emergência com
`x-transport`, o texto do DOWN no `health.go` ("incoming RELP transactions are
held"), a citação de SPEC §13.4 no shutdown (a do RP não existe na SPEC dele) e
`TestAbortDuringTheDumpCountsTheLeftovers`, ao lado do teste com um worker.

### 9.2 Correções transversais da revisão independente do `go-syslog-server` (2026-10-13)

Os itens transversais (X1–X4) do plano da revisão independente do porte do
`go-syslog-server` (S), e a conferência de C3, D1, E1, E2 e A2-minor; a mesma
implementação no irmão (regra de sincronia, `diff` conferido). **Conferência
por mutação**, cada correção revertida sozinha: o teste da linha falha
(`exited 0, want 130`; `abort 0, emergency_failed 5 for 5`; o subteste
`10077_store_is_closed`; o buffer solto com o frame seguinte já no
`bufio.Reader`). **Não provado** contra um servidor real: o `10077` com
"store is closed" vem do texto do `nats-server` 2.14.6 (`server/store.go:43`),
não de um stream fechando durante um publish (difícil de provocar sem
corrida).

| Item | Achado | Consequência sem a correção | Teste |
|---|---|---|---|
| X1 (A1 do S) | Sinal depois da reingestão e antes do *accept* com IP literal: nenhum *bind* falha | o servidor abria, o `/readyz` dava 200 e o shutdown saía **0**, não 130 (SPEC §13.1, §13.5) | `TestSignalBeforeALiteralBindExits130` (IP literal; o nome de host segue em `TestSignalBeforeTheBindExits130`) |
| X2 (A2 do S) | O aborto publica `aborted` antes de tomar o contador; o dump que via a marca nesse meio (abandonado, ou desligado) o tomava primeiro | `emergency_failed = n`, `abort = 0`: a perda contada uma vez, atribuída ao disco (SPEC §13.4) | `TestAbortInsideTheDumpKeepsItsReason` (dump abandonado e dump desligado; o gancho `inAbort` força a intercalação) |
| X3 (C2 do S) | `storeFailedIsTransient` conferia "store closed"; o nats-server 2.14.6 manda "store is closed" (`server/store.go:43`) | o `10077` de um store fechando era `fatal`: dump, exit 3 e duplicatas na reingestão a cada restart do NATS (SPEC §10.7) | `TestClassifyNATS/10077_store_is_closed` |
| C3 do S | Teto do que entra no `MaxPending` | **não se aplica**: sem `ring.max_inflight`; `2 × workers × batch` já limitado por `sink.workers` ≤ 256 e `sink.batch` ≤ 65 536 | `TestConfigCeilings`, `TestMaxPendingHasTwiceTheWorkersBatches` (já existentes) |
| E2 do S | O leitor RELP soltava o buffer acima de 16 KiB antes de **todo** frame | um emissor de linhas de 16–64 KiB refazia o crescimento por frame: 24 KiB custavam 12,3 µs, 55 KB e 5 alocações, contra 0,52 µs e 0 (SPEC §5.2) | `TestRELPReaderKeepsTheBufferWhileFramesFollow`, `TestRELPReaderReleasesALargeBuffer` (agora com o emissor parado entre os frames), `BenchmarkRELPReaderFrames` (§4) |
| E1 do S | `closing` com PROXY `optional` | **não aplicado**, por instrução: no RELP as linhas no buffer nunca tiveram `200`, e o `librelp` as retransmite | — |
| A2-minor do S | Comentário de ordem reversa com fechamento paralelo; log do aborto com o ring Redis | **não se aplica**: o comentário diz "AT ONCE", como o código; não há Redis | — |

### 9.3 Rodada das pendências conhecidas (2026-10-14)

Entrada: `tasks/triage-RP-RN.md` da raiz. Ordem por item: SPEC, teste que
falhava no código de então, correção, e mutação (só a correção revertida, o
teste falha, restaurada). Os testes novos ficam em `known_issues_test.go` de
cada pacote; os do código comum são os mesmos do RP. Registro passo a passo
em `tasks/progress-RP-RN.md` da raiz.

| Item (SPEC §22.1) | Regra | Testes (falhavam antes) | Mutação |
|---|---|---|---|
| 7, aborto lê o ring aberto | §13.4 | `TestAbortClosesTheRingBeforeCounting` (`app`); `TestEnqueueSeesACloseThatRacedTheReservation` (`ring`, gancho `afterReserve`) | sem `ring.Close()` no `abortLoss` e sem a reconferência: os dois falham |
| 1, `ring.max_bytes` sem lugar | §13.2, §15.1 (a direção do RP: classe de `max_message_bytes` + 256 B) | `TestRingMaxBytesFitsTheLargestMessage`, `TestRingMinBytesIsWhatTheRingCharges` | piso de volta a 1: falha |
| 5, `admin.bind` = `listen.tcp` | §13.2, §14 | `TestAdminBindCollidesWithListen` | sem a checagem: falha |
| 3, discagem da reconexão em `ERROR` | §10.7 | `TestNATSReconnectDialFailureIsNotLogged` (`level=ERROR msg="nats async error" error="dial tcp: connection refused"`) | `asyncError` fora do `if` (a forma antiga): falha |
| 10 (novo), veredito de TLS sem olhar o *pool* | §10.7 | `TestNATSOneTLSServerOfAPoolIsNoVerdict` (dois servidores falsos, o bom derrubado e religado: `natsFake.stop`/`restart`), `TestNATSBootTLSRefusalOfAPoolIsNetwork` | sem `soleServer` no `onReconnectErr`, sem `soleServer` no `noteLastError`, sem o ramo do *pool* no `connect`, `Health` sem nomear o TLS: cada um falha |
| 8, rótulo do idle | §5.4, §13.2 | `TestIdleCloseWithAHeldTransactionIsBackpressure`, `TestIdleBelowBackpressureWarns`; controle `TestIdleCloseWithNothingHeldCountsNothing` | rótulo sempre `server_closed`, `held` que nunca sobe, `WARN` desligado: cada um falha |
| 4, derivada no `SIGHUP` | §16.3 | `TestReloadDoesNotReportDerivedKeys` | filtro desligado: falha; `\|\|` no lugar de `&&`: falham os controles |
| 2, 6, 9 | §13.4 e DECISOES nº 15, §8.2 item 10, §14.1 | sem código novo | — |

### 9.4 Revisão independente de 2026-10-14 (S, SP, SN, R, RP, RN)

Entrada: `tasks/review-syslog.md` e `tasks/review-relp.md`; progresso em
`tasks/progress-review-fixes-syslog-relp.md`. **Ordem:** SPEC, teste que falha
no código de antes, correção. **Mutação:** cada correção revertida sozinha,
com o teste da linha falhando e a correção restaurada depois.

| Item | Falha antes / com o mutante | Teste |
|---|---|---|
| `sameStreamSocket` por valor (§13.2; D1 do syslog, D4 do RELP) | 7 casos, p.ex. `sameStreamSocket("[::0]:9102", "127.0.0.1:9102") = false, want true`, `("127.0.0.1:0514", "127.0.0.1:514")`, `("127.0.0.1:http", "127.0.0.1:80")`; mutantes (curinga por grafia; porta comparada como texto): mortos | `TestSameStreamSocketReadsValuesNotSpellings` |
| Transação segurada desde antes do primeiro `Accept` (§5.4; A2 do RELP) | "idle during a slow first Accept: backpressure = 0, server_closed = 1; want 1 and 0"; mutante (o `held` de volta para depois do primeiro `Accept`): a mesma | `TestIdleDuringASlowFirstAcceptIsBackpressure` |

Registrados sem código: comentário do `Memory.Enqueue` (D1); URL única com vários IPs e *pool* só de servidores que exigem TLS (D3; D2 do syslog; §10.7); soltura do buffer do leitor entre frames (A5/D4 do S; §5.2); sobrecontagem pelo recuo de bytes (D3 do syslog; §13.4). Aqui o `Accept` do ring em memória é instantâneo: o A2 entra para o código comum não divergir do R.

Rodadas: `tasks/run2.sh go-relplog-nats-server rev` (`gofmt -l .`, `go vet ./...` e `go test -race -count=1 -timeout 1500s ./...` com `tasks/infra/env.sh`), logs em `tasks/logs/go-relplog-nats-server-rev-r{1,2}.log`: **verde duas vezes** (`exit=0` nas duas; `gofmt` e `vet` limpos), sem *flake*; e o pacote `sink` com `RELPLOG_TEST_NATS_URL=nats://127.0.0.1:24226`: **ok**. Fuzz: nenhum, por instrução da rodada.

---

## 10. Resultado da rodada final (2026-10-13, depois do porte de §9.1)

Apple M2, macOS, Go 1.27.1, quatro `nats-server` 2.14.6 em processos próprios
(§6), o rsyslog 8.2410 com `librelp` 1.11 (Alpine 3.22, §7).

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `RELPLOG_TEST_NATS_*` (os quatro) `RELPLOG_TEST_EMITTERS=1 go test -race -count=1 -timeout 900s ./...` | **verde duas vezes seguidas** (49 s e 48 s), sem *flake* |
| Testes | **435** de primeiro nível e **332** subtestes passaram, **1** pulado (`TestGoldenVectors/arquivo-sem-pri`, D5), 0 falhas |
| Por pacote (primeiro nível / subtestes) | `app` 71/78, `config` 69/0, `control` 9/0, `convert` 15/70, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 10/0, `receiver` 100/33, `ring` 13/0, `sink` 105/106 |
| Emissores reais | `TestEmittersRELPInClear` (11,9 s), `TestEmittersRELPBackpressureRetransmits` (4,6 s), `TestEmittersRELPSurvivesARestart` (3,6 s) |
| Cobertura (`-coverpkg=./...`) | **91,6%** (§1) |
| Fuzz | `FuzzFrame`, `FuzzProxy`, `FuzzRead`, 60 s cada, sem falha (§3); `FuzzConvert` e `FuzzSD` não rodaram (`convert` não mudou) |
| Mutação | cada correção de código portada em §9.1 (itens 1, 2, 6, 9, 10, 13 e 14–21), revertida numa cópia do módulo: o teste correspondente falha (22 mutantes, todos mortos) |
| Não rodou | fuzz de 10 min (§8.2 item 1); `docker compose --profile demo` (o `docker/` só mudou em texto); carga ponta a ponta (§8.2 item 2) |

### Depois das correções transversais de §9.2 (2026-10-13)

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `RELPLOG_TEST_NATS_*` (os quatro: 14622 JetStream, 14623 sem JetStream, 14624 permissões, 14625 `tls {}`) `RELPLOG_TEST_EMITTERS=1 go test -race -count=1 ./...` | **verde duas vezes seguidas** (51 s e 54 s), sem *flake* |
| Testes | **438** de primeiro nível e **336** subtestes passaram, **1** pulado (`TestGoldenVectors/arquivo-sem-pri`, D5), 0 falhas (435 e 332 antes: os 3 de primeiro nível novos são os de §9.2) |
| Por pacote (primeiro nível / subtestes) | `app` 73/81, `config` 69/0, `control` 9/0, `convert` 15/70, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 10/0, `receiver` 101/33, `ring` 13/0, `sink` 105/107 |
| Emissores reais | `TestEmittersRELPInClear` (12,8 s), `TestEmittersRELPBackpressureRetransmits` (5,8 s), `TestEmittersRELPSurvivesARestart` (3,7 s), verdes nas duas rodadas |
| Fuzz | `FuzzFrame` 60 s, sem falha (1,63 milhão de execuções); o único alvo cujo código mudou (`relpframe.go`; `nats.go` não tem alvo) |
| Mutação | cada correção revertida sozinha: o teste da linha falha (ver acima) |
| Não rodou | fuzz de 10 min; carga ponta a ponta; `docker compose` (o `docker/` não mudou) |

### Depois da rodada das pendências conhecidas de §9.3 (2026-10-14)

Os quatro `nats-server` da infraestrutura comum (`tasks/infra/env.sh`: 24222
JetStream, 24223 sem JetStream, 24224 permissões, 24225 `tls {}`) e o rsyslog
8.2410 com `librelp` 1.11.

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `source tasks/infra/env.sh; go test -race -count=1 -timeout 1200s ./...` | **verde duas vezes seguidas** (74 s e 66 s), sem falha |
| Testes | **450** de primeiro nível passaram e **1** pulou (`TestNATSOversizedMessageIsRefusedAlone`, §8.2 item 11: o servidor da infraestrutura comum tem `max_payload` de 8 MiB); **334** subtestes passaram, **1** pulado (`TestGoldenVectors/arquivo-sem-pri`, D5); 0 falhas. Eram 438 de primeiro nível: os 13 novos são os de §9.3 |
| Por pacote (primeiro nível / subtestes, com os pulados) | `app` 74/81, `config` 74/0, `control` 9/0, `convert` 15/71, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 10/0, `receiver` 103/33, `ring` 15/0, `sink` 108/105 |
| Emissores reais | `TestEmittersRELPInClear` (14,9 s e 15,1 s), `TestEmittersRELPBackpressureRetransmits` (6,4 s e 7,7 s), `TestEmittersRELPSurvivesARestart` (4,8 s e 4,5 s), verdes nas duas rodadas |
| Mutação | cada correção revertida sozinha: o teste da linha falha (§9.3) |
| Não rodou | fuzz (por instrução; nenhum alvo de fuzz mudou); carga ponta a ponta; `docker compose` (o `docker/` não mudou) |

