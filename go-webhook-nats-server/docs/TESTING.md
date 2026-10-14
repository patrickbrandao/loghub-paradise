# go-webhook-nats-server — Procedimentos de teste

Companheiro operacional de `SPEC.md` §15. As suítes vieram do
`go-webhook-pg-server` em 2026-10-10, com as do destino trocadas pelas do NATS:
as do `go-syslog-nats-server` (sink, beacon, configuração NATS e
`LOGHUB_EB_URL`) e as do `go-webhook-fast-server` (o `cover` de um subject de
outro `space`, o ciclo de vida da app contra o NATS). O histórico de §9 é o
deles no que vale para o código daqui. Este documento diz **como rodar**, **o
que cada suíte prova** e **o que ainda não é provado**.

---

## 1. Suíte rápida (todo commit)

```bash
cd go-webhook-nats-server
gofmt -l .            # deve ser vazio
go vet ./...
go test -race ./...
```

Tempo: ~45 s sem NATS, ~50 s com ele (2026-10-10, macOS M-series). O `-race`
**não é opcional**: os bugs de perda de dados deste projeto vivem em caminhos
concorrentes, e a liberação dupla de buffer (SPEC §12.3) só é detectável assim.

Sem `WHS_TEST_NATS_URL`, os testes que precisam de um NATS **pulam**; o ciclo de
vida inteiro (boot, dreno, DOWN, FATAL, dump, reingestão, `SIGHUP`) roda mesmo
assim, contra o destino falso de `internal/app/fakesink_test.go` (SPEC §15.3),
e os testes de unidade do sink rodam contra um servidor NATS **falso** de
`internal/sink/nats_test.go` (o que pede TLS).

## 2. Cobertura real

Cobertura por pacote engana: a suíte de `internal/app` exercita `server`,
`ring` e `sink` sem que isso seja atribuído a eles. Use `-coverpkg`:

```bash
WHS_TEST_NATS_URL=nats://127.0.0.1:14222 WHS_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -coverpkg=./internal/... -coverprofile=cov.out ./... && go tool cover -func=cov.out | tail -1
```

Referência (2026-10-10, com os dois NATS, depois da revisão independente):
**91,6 %**. No `sink/nats.go`, o menos coberto é `writeCore` (83,3 %: o erro de
`PublishMsg` que não é `ErrMaxPayload` exige um cliente desconectado no meio do
lote).

## 3. Fuzzing (SPEC §15.2, obrigatório)

Antes de qualquer release, **10 minutos cada** (60 s no dia a dia):

```bash
go test ./internal/clientip/  -run=XXX -fuzz=FuzzResolve      -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$       -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=FuzzGateAdmitsTheWriter -fuzztime=10m
go test ./internal/envelope/  -run=XXX -fuzz=^FuzzEnvelope$   -fuzztime=10m
go test ./internal/server/    -run=XXX -fuzz=FuzzServeHTTP    -fuzztime=10m
```

Não há `FuzzJSONBSafe`: a troca do NUL era da cópia para o `jsonb` do P.

O que cada um prova:

- `FuzzResolve` — o único parser de entrada hostil estruturada; um bug ali é
  spoofing de IP.
- `FuzzRead` — o leitor do dump JSON Lines (SPEC §9.2), em memória: todo item
  recuperado é um JLH **deste servidor** (cabeçalho estrito, `type=webhook`,
  `level=9`, `step=0`, payload aberto por `{"mode":"fast",` — a porta que impede
  o NATS de publicar o que não leu), JSON e UTF-8 válidos, sem `0x0A`, com o
  `uuid` como id, e aprovado por um oráculo que a porta não compartilha
  (`jlhcheck.ValidateEnvelopeSchema`: o schema JLH e um decodificador que vê
  chave repetida em qualquer profundidade); todo byte da entrada é contado
  exatamente uma vez; o que foi recuperado reescreve e relê igual.
- `FuzzGateAdmitsTheWriter` — a outra metade do contrato da porta: todo
  envelope que o `envelope.AppendWebhook` escreve, com path, query, valores de
  cabeçalho, tipo e corpo hostis (nomes de cabeçalho só como *token* HTTP, que
  é o que o `net/http` deixa passar), passa no `checkLine`. Uma porta mais
  estreita que o escritor quarentenaria eventos reais, respondidos com `200`.
  Achou na primeira execução que o próprio alvo passava nomes de cabeçalho que
  o `net/http` recusa — o escritor os emite sem escape, de propósito.
- `FuzzEnvelope` e `FuzzServeHTTP` — todo envelope passa no contrato, o
  `ParseHead` o lê de volta, e **nenhum tem `0x0A` cru** (SPEC §9.1).

Rodados por 60 s cada em 2026-10-10, sem achado; o `FuzzRead` de novo, 60 s,
e o `FuzzGateAdmitsTheWriter`, 60 s (2 milhões de execuções), depois da porta
nova da revisão independente.

### 3.1 Contrato LogHub (SPEC §3, §15.1)

O oráculo `internal/jlhcheck` valida todo envelope que um teste lê de volta —
do handler, do destino falso, do dump e do stream — contra as cópias dos
schemas em `internal/jlhcheck/loghub/` e as regras do `validate.py` da skill. A
mensagem NATS é fiel ao byte, então o envelope lido do stream é comparado
também byte a byte com o que foi publicado. O beacon passa por
`jlhcheck.ValidateEnvelopeSchema` (só o envelope: não há schema de payload de
`beacon` aqui). `jlhcheck.ValidateJSONBCopy` ficou do P, sem uso, pela paridade
do `diff`.

## 4. Integração sem NATS (SPEC §15.3)

`internal/app` monta a mesma aplicação do binário (`runContext`) e troca só o
destino, pelo campo `App.sinkOverride`. O destino falso (`memSink`) guarda uma
**cópia** de cada envelope confirmado e pode ser posto DOWN ou FATAL no meio do
teste:

| Cenário | Teste |
|---|---|
| Todo `200` chega ao destino, e nada além | `TestEveryAcceptedEventIsDelivered` |
| Ring cheio é `429`, nunca `200` com descarte | `TestRingFullReturns429` |
| Superfície HTTP, h2c, admin, histogramas | `TestHTTPSurface`, `TestH2CPriorKnowledge`, `TestAdminEndpoints`, `TestHistogramInvariants` |
| Seletores de compatibilidade, URL não `nats://` e chaves retiradas | `TestInvalidConfigRefusesToStart` |
| NATS inalcançável: porta pública fechada, `exit 3` após `boot_timeout` | `TestUnreachableSinkExitsWithoutOpeningPublicPort` (não precisa de servidor: a porta morta é o ponto) |
| Recusa fatal no boot: `exit 3` na hora, e o log diz "refused this server permanently" | `TestFatalBootRefusalIsLoggedAsSuch` |
| A ligação de `[sink.nats]`/`[loghub]` ao sink: prazos, `workers × batch`, maior envelope, nome do cliente | `TestNATSConfigCarriesTheDeadlinesAndCeilings` |
| `SIGHUP` real ignorado, com `WARN` | `TestSIGHUPIsIgnored`, `TestIgnoreSIGHUPWarnsOnEachSignal` |
| `SIGTERM` com ring cheio: dreno completo, `exit 0` | `TestGracefulShutdownDrainsAFullRing` |
| Segundo sinal: `exit 130` | `TestSecondSignalExits130` |
| Destino cai e volta: `503`, nada perdido | `TestSinkDownThenBackLosesNothing` |
| Destino FATAL: dump JSON Lines lido **à mão**, reingestão no boot seguinte | `TestFatalSinkDumpsAndTheNextBootReingests` |
| `emergency.enabled=false`: `exit 3`, nenhum arquivo | `TestFatalSinkWithoutEmergencyExitsThreeAndWritesNothing` |
| Dump com linhas danificadas no meio e no fim: íntegras entregues, `.corrupt` | `TestDamagedEmergencyDumpIsQuarantined` |
| Lote que o destino recusa inteiro como `logic` (sem `Delivered`): refeito item a item, `.corrupt`, boot segue | `TestALineTheDestinationRefusesIsQuarantinedNotFatal` |
| Dump que não se escreve: perda contada uma vez, em `emergency_failed` | `TestFailedDumpIsCountedOnceAsEmergencyFailed` |
| Primeiro sinal durante um shutdown por FATAL: o dump continua | `TestFirstSignalDuringAFatalShutdownKeepsTheDump` |
| `emg-*.bin` do fast: intocado; o `.jsonl` ao lado é reingerido | `TestFastBinaryDumpIsLeftAlone` |
| Reingestão entregue em parte (`Delivered`): as guardadas contam como escritas, as recusadas ficam no `.corrupt` | `TestReingestPartialDelivery` |
| `SIGTERM` no meio da reingestão: `130`, dump mantido | `TestSIGTERMDuringReingestKeepsTheDump` |
| O temporário gêmeo de um dump é removido; o de um dump interrompido fica, informado num `WARN` que não diz "contados como perda" e ensina a reingerir | `TestBootRemovesTheTwinOfADumpAndKeepsAnInterruptedOne` |
| *Gauges* de emergência depois do dump: a soma, sem reler o diretório | `TestEmergencyGaugesCountTheDumpWithoutReReading` |
| Upload lento e destino que não responde no shutdown: o dump começa em um `drain_timeout`, não em dois | `TestShutdownDrainTimeoutBoundsConnectionsAndRingTogether` |
| Upload que termina depois do último `Drain` do shutdown: `503`, nunca `200` (ring selado). O ring do teste (`App.wrapRing`) completa o upload **dentro** do `Drain`, então um `Seal` depois do `Drain` também falha | `TestUploadFinishingAfterTheDrainIsRefused` |

No pacote `sink`, sem servidor: a tabela de classificação, a forma da mensagem
(cabeçalhos com o nome literal, `Data` como o próprio slice do item), o teto do
bloco de headers (exatamente 257 B), os subjects por `space` com cache, o
`subjectCovers`, o contrato de `docker/nats/stream.json`, as recusas da
validação (inclusive os três tetos de tamanho), as opções que nunca pedem TLS
(e `ReconnectBufSize(-1)`, `MaxReconnects(-1)`), um servidor falso com
`tls_required` que nunca recebe um byte, o veredito de permissão negada, a
amostragem dos erros assíncronos, os **workers com entrega parcial**
(`TestWorkersPartialDeliveryDropsOnlyTheRefused`, que também confere que o ring
vazio não conta byte nenhum), os logs de conexão e reconexão sem token nem senha
(`TestNATSConnectLogsNoCredentials`), a publicação negada em `core` fatal no
próprio lote e nada publicado depois (`TestNATSCoreDeniedPublishIsFatalAtOnce`),
o `WARN` único por subject de outro `space` em `core`
(`TestNATSCoreWarnsOnceAboutAForeignSubject`), e "sem respondedores" na
primeira checagem fatal sem perguntar `$JS.API.INFO` nem guardar veredito,
contra o servidor falso que responde `503` a todo pedido e nega
`$JS.API.INFO` (`TestNATSNoRespondersNeverProbesTheAccount`). No pacote `ring`,
o `Seal` como barreira contra o `Enqueue` (`TestMemorySealIsABarrierForEnqueue`).

## 5. NATS real — suíte automatizada

Os testes pulam sem um servidor, e o servidor não cria o stream: a suíte cria
os dela, cada um com um nome e um `space` únicos, a partir do stream canônico
(`natsCanonicalStream` no sink, `docker/nats/stream.json` na app). Um segundo
servidor, **sem** JetStream, prova o FATAL imediato de SPEC §8.7.5.

```bash
mkdir -p /tmp/whs-nats
cat > /tmp/whs-nats.conf <<'EOF'
port: 14222
max_payload: 8MB
jetstream { store_dir: "/tmp/whs-nats" }
EOF
go run github.com/nats-io/nats-server/v2@v2.14.6 -c /tmp/whs-nats.conf &
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &       # sem JetStream
WHS_TEST_NATS_URL=nats://127.0.0.1:14222 WHS_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 900s ./internal/sink/ ./internal/control/ ./internal/app/
```

O `max_payload` de 8 MB não é exigido: a suíte da app baixa os tetos de corpo e
cabeçalho para caber no 1 MiB padrão, e a do sink lê o `max_payload` do
servidor.

Desde o porte da família syslog/RELP (2026-10-13), três variáveis a mais, cada
uma pulando os seus casos sem ela (`internal/sink/review_port_nats_test.go`):
`WHS_TEST_NATS_PERM_URL` (JetStream com os usuários `admin`, `nopub` — publish
negado em `loghub.*.webhook` —, `noinbox` — subscribe negado em `_INBOX.>` — e
`nobeacon` — publish negado em `*.beacon`; a configuração está no comentário de
`natsPermSetup`), `WHS_TEST_NATS_TLS_URL` (um listener com `tls {}`, que este
servidor deve recusar) e `WHS_TEST_NATS_SERVER` (o binário do `nats-server`,
para o nó em texto claro que o teste derruba no meio de um *pool*).

O que prova:

- **sink** (`internal/sink/nats_integration_test.go`): o JLH no stream byte a
  byte com os cinco cabeçalhos e o subject do `space`; redelivery deduplicada e
  contada; oito escritores numa conexão; a maior mensagem que o boot admite
  cabe, e um byte a mais é recusado; uma mensagem acima do `max_payload`
  recusada **sozinha** (jetstream e core), e uma acima do `max_msg_size`
  também, com `Delivered`; um envelope que não é deste servidor recusado
  sozinho; cada recusa da validação (stream ausente, de outro `space`, só
  `webhook.>`, capturando o plano de controle, selado, `no_ack`, os tetos)
  fatal, com controles aceitos que escrevem; `10060` fatal e com o lote
  contaminado; subject de outro `space` fatal até o stream cobri-lo, e então
  publicado e lembrado; stream apagado depois do boot fatal; `core` entregando a
  um assinante; os ganchos de conexão (inclusive numa reconexão forçada), o
  `PublishCore` e o `ServerAddr` do beacon; workers de ponta a ponta;
  JetStream desligado fatal em segundos; uma recusa `logic` que não deixa bytes
  contados no ring, nos dois modos (`TestNATSLogicRefusalFreesTheRingBytes`); o
  stream editado depois do boot — subjects estreitados, `no_ack` ligado — fatal
  na checagem seguinte (`TestNATSStreamEditedAtRuntimeIsFatal`); um subject de
  outro `space` retirado depois do dreno esquecido, sink UP, e a publicação
  seguinte nele fatal nomeando-o
  (`TestNATSForeignSubjectDroppedAfterTheDrainIsForgotten`).
- **control** (`beacon_integration_test.go`): o beacon da primeira conexão,
  pedido antes do `Start`, chega em `<ctl>.beacon` pelo NATS real.
- **app** (`internal/app/nats_test.go`): todo `uuid` aceito chega ao stream e
  nada além; corpo binário, `%00` e UTF-8 inválido no path ida e volta; cada
  recusa de stream (e o servidor sem JetStream) ⇒ `exit 3` **imediato**, porta
  pública fechada; destino cai (proxy TCP) ⇒ `503`, volta ⇒ nada perdido, uma
  reconexão contada; FATAL ⇒ dump ⇒ reingestão; dump de outro Workspace ⇒
  `exit 3` imediato até o stream cobrir o subject, e então entregue — e o
  stream estreitado de volta com a instância rodando não a derruba; o beacon na
  conexão e no periódico, com o `meta`, fora do stream; `core` entregando a um
  assinante.

Se você mexeu em `internal/sink/`, `internal/control/`, na validação de
`config` ou em `docker/nats/`, **rode esta suíte**.

### 5.1 Teste de fumaça do compose (feito em 2026-10-10)

```bash
cd docker && docker compose up -d --build
for i in $(seq 1 20); do curl -s -o /dev/null -X POST "localhost:8300/hook?i=$i" -d "{\"n\":$i}"; done
docker run --rm --network whn-memory-nats_default natsio/nats-box:0.20.0 \
  nats --server nats://nats:4222 stream info LOGHUB_WEBHOOK
docker run --rm --network whn-memory-nats_default natsio/nats-box:0.20.0 \
  nats --server nats://nats:4222 sub 'loghub._ctl.beacon' --count=1
docker compose down -v
```

Exercitado: boot e validação do stream (a maior mensagem dos padrões,
8 022 995 B, contra o `max_payload` de 8 MiB), entrega de 20 eventos com os
cabeçalhos de SPEC §3.8, beacon em `loghub._ctl.beacon` com o `meta`, `SIGHUP`
ignorado com `WARN`, `docker pause` do NATS ⇒ DOWN ⇒ FATAL após 10 checagens ⇒
dump de 15 eventos ⇒ `exit 3`, e no boot seguinte a reingestão dos 15. O NATS
congelado gravou, ao voltar, 8 publicações que estavam no buffer TCP; a
reingestão as viu como duplicatas (`whs_sink_nats_duplicates_total 8`) e o
stream terminou com os 35 únicos.

## 6. Carga (SPEC §15.4)

```bash
# keep-alive: mede o pipeline
bombardier -c 256 -d 60s -m POST -b '{"k":"v"}' http://127.0.0.1:8300/hook
# conexão nova por request: mede accept + handshake
bombardier -c 256 -d 60s -m POST -b '{"k":"v"}' --disableKeepAlives http://127.0.0.1:8300/hook
# h2c
h2load -n 1000000 -c 64 -m 32 -d body.json http://127.0.0.1:8300/hook
```

## 7. Critérios de aceite (SPEC §15.5) — **não aferidos**

Nenhuma medição de vazão ou latência foi feita neste servidor. A meta numérica
de `ring=memory` + `sink=nats` só existe depois da primeira medição válida.

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento. As lacunas de prova estão
aqui; os defeitos que o código ainda tem estão em SPEC §18.1.

1. **Um `ErrPermissionViolation` real** não é provocado contra um servidor com
   contas e permissões; o veredito é provado pela unidade
   (`TestNATSPermissionViolationOnTheDataPathIsFatal`), com o texto que o
   `nats.go` v1.53.1 produz.
2. **Um servidor que passa a exigir TLS depois do boot** (o
   `ReconnectErrHandler`) só é coberto pela leitura; o boot contra um servidor
   que exige TLS é provado pelo servidor falso.
3. **Várias URLs**: o veredito do boot (`fatal` ou `network`) pode depender da
   ordem aleatória em que o cliente tenta os servidores (herdado do
   `go-syslog-nats-server`).
4. **`writeCore` com o cliente desconectado no meio do lote** não tem teste: o
   `PublishMsg` falha com `network`, o lote volta inteiro, e uma parte pode ter
   saído antes (best effort, duplicata possível).
5. **A contaminação depois de um erro de futuro** é provada para o `10060`
   (`TestNATSExpectedStreamMismatchIsFatalOnWrite`); a de um prazo estourado,
   pela regra dos workers.
6. **`kill -9`** perde o ring por desenho (SPEC §16.1); não há o que testar
   além de documentar.
7. **A regra do `/data` (SPEC §13.6) não tem teste automatizado**; quem a
   sustenta é o `read_only: true` do compose:

   ```bash
   docker compose exec webhook grep ' / ' /proc/mounts   # deve conter "ro,"
   ```

8. **Um envelope com `0x0A` cru** nunca foi produzido pelo serializador; a
   defesa do dump é provada com um envelope forjado
   (`TestWriteLeavesOutAnEnvelopeWithARawNewline`).
9. **Linha do dump acima do teto da configuração atual** é pulada e o arquivo
   vira `.corrupt`; provado pela unidade, não por um ciclo de dois boots.
10. **Um sistema de arquivos sem *hard link* de verdade** não é usado: o
    fallback é provado trocando o `link` por um que falha
    (`TestWriteOnAFileSystemWithoutHardLinks`). O revisor reproduziu a falha
    original numa imagem FAT (`hdiutil -fs MS-DOS`).
11. **`server.listeners > 1`** só é testado no que é portável.
12. **A corrida do `Dequeue` contra o contexto cancelado** é provada por
    repetição (`TestWorkersNeverLoseAnAbandonedBatch`), não por construção.
13. **A forma de cada objeto aninhado do payload** (as chaves de `network`, de
    `content`) não é conferida pela porta do dump: só as de topo. Ela não
    envenena leitor nenhum, e conferi-la seria reescrever o schema em Go.
14. **Código morto herdado**, mantido pela paridade do `diff` com o P e o fast:
    `envelope.Item.RowID`, `Item.Done/Ack`, `obs.Logger.SetLevel`,
    `jlhcheck.ValidateJSONBCopy`.
15. **O dump é lido inteiro por arquivo** na reingestão (SPEC §9.2, "Memória"):
    um dump de uma vida com `ring.max_bytes` muito maior que o atual pede a
    memória da antiga.
16. **Fuzz de 10 minutos e medição de carga** pendentes (60 s por alvo feitos).
17. **Recusas de autenticação no `CONNECT`** ("User Authentication Expired" e
    afins) são provadas pela tabela de classificação, com o texto que o
    `nats.go` v1.53.1 devolve; um servidor com JWT expirado não foi montado (um
    real responde "Authorization Violation", que já era fatal).
18. ~~A janela da contabilidade do aborto~~ — *fechada em 2026-10-13* (§9, "Porte das correções da família syslog/RELP").
19. **Mutante equivalente conhecido**: dar à fase 3 do shutdown o seu próprio
    `drain_timeout` não muda nada, porque o prazo único é calculado no mesmo
    instante; o teste do prazo único mata a mutação da fase 4.
20. ~~Brecha no parser de flags (cauda de um valor partido pelo shell)~~ — *fechada em 2026-10-13* (`knownSectionOf`, `TestFamilyPortTailAfterAValueIsNotNamed`).
21. **A retentativa do `accept`** (SPEC §10.3) é provada com um listener falso
    que devolve `ENOBUFS` (`TestAcceptRetriesNonTemporaryErrors`), e o
    `Serve` com erro, por um `Serve` embrulhado (`App.wrapServe`,
    `TestServeErrorGoesThroughTheShutdown`): nenhum erro real do kernel foi
    provocado.
22. **O ring cobrando o buffer retido** (SPEC §7.2) é provado pela
    contabilidade (`TestMemoryChargesTheRetainedBuffer`,
    `TestItemOverheadCoversTheStruct`, `unsafe.Sizeof` = 80); o RSS de um ring
    cheio com o destino fora não foi medido (§7).
23. **`max_conns_per_ip` por endereço exato** é decisão contestável
    (`DECISOES.md` §3b); `TestPerIPLimitCountsTheExactAddress` é teste de
    **trava**: passa por desenho.
24. **`sink.batch_max_bytes` contra o NATS real**: o corte é provado pelo
    destino falso (`TestWorkersSplitABatchByBytes`,
    `TestBootWritesInPiecesUnderBatchMaxBytes`); pedaços perto do teto contra
    um JetStream real não foram publicados.
25. **O compose** é conferido como texto (`internal/config/compose_test.go`);
    o `.creds` montado em `/etc/webhook` (o volume comentado) não subiu de
    ponta a ponta.

26. **`Expect: 100-continue` com o corpo enviado sem esperar** (SPEC §4.3;
    revisão independente de 2026-10-14) não tem teste: o `net/http` fecha
    sem `closeWriteAndWait`, e um RST pode levar o `429`/`503`. Reproduzi-lo
    pede um cliente que mande o corpo antes do `100` e um *timing* de RST
    que o loopback não dá de forma estável; aceito e documentado.

27. **A cópia do `held` e a fatia do `Dequeue` fora do `ItemOverhead`** (SPEC
    §7.2; revisão independente de 2026-10-14): até `workers × batch × 16`
    bytes fora de `ring.max_bytes`, sem teste, por ser um limite documentado
    e não uma regra.

28. **O recusado no `.corrupt` fica fora dos *gauges*** (SPEC §9.2; revisão
    independente de 2026-10-14): só o `ERROR` com o `uuid` o registra;
    documentado, sem teste de métrica.

## 9. Histórico: bugs que os testes encontraram

O histórico do P e do fast está em `../go-webhook-pg-server/docs/TESTING.md`
§9 e `../go-webhook-fast-server/docs/TESTING.md` §9; o do sink NATS e do beacon,
em `../go-syslog-nats-server/docs/TESTING.md` §9. Os que valem para o código
que ficou aqui, e já vieram corrigidos:

| Bug | De onde | Consequência se não corrigido |
|---|---|---|
| Lote retido evaporava no shutdown, ou era sobrescrito no `held[id]` | fast | Eventos com `200` sumiam sem dump |
| Liberação dupla de buffer não era atômica | fast | Corpo de um cliente dentro do evento de outro |
| No shutdown por FATAL, o primeiro `SIGTERM` jogava o dump fora | P | Eventos do ring perdidos justamente no caminho que os salva |
| Linha do dump que passa em `json.Valid` mas não é do serializador (`0xFF`, `\ud800`) | P | Lá, todo boot saía com 3; aqui, o NATS a publicaria |
| O `nats.go` se promove a TLS sozinho diante de `tls_required` | SN | Um `ClientHello` com as raízes do sistema num servidor "sem TLS" |
| Permissão negada só chega assíncrona, e nunca virava FATAL | SN (herdado do S) | Publish negado retentado para sempre com o sink UP |
| Uma mensagem acima do `max_payload` encerrava o lote | SN (o fast tinha o mesmo até 2026-10-11) | As seguintes nunca publicadas e confirmadas; as anteriores contadas como perdidas |
| Senha com `%`, `/` ou `#` sem escape na URL | SN | A senha em cada log, até o `boot_timeout` |
| Dump escrito sob o nome final | SN (o P tinha o mesmo até 2026-10-11) | Cortado numa fronteira de linha, lia limpo e era removido entregue em parte |
| Subject de um backlog de outro Workspace publicado às cegas | fast | "no responders" até o `boot_timeout`, em laço, sem nomear a causa |

### Rodada de 2026-10-10 (criação deste módulo)

| Achado | Como | Consequência sem a correção |
|---|---|---|
| Numa recusa fatal no boot, o log dizia "sink did not become reachable within sink.boot_timeout" (herdado do P) | O log do `TestNATSStreamMismatchExitsThreeImmediately` | O operador ia olhar a rede quando o stream ou a credencial estavam errados. Corrigido aqui; portado ao P, ao fast e ao W em 2026-10-11 |
| A validação lia `nc.MaxPayload()` de dentro, e o teste de unidade do caminho aceito derrubava com ponteiro nulo | `TestNATSValidateRefusals` | Só teste; a validação passou a receber o `max_payload` como valor, testável sem conexão |

### Revisão independente de 2026-10-10

Três revisores somente-leitura, um por área, cada um com cópia de rascunho e
testes descartáveis; nada foi editado por eles. Cada achado real virou SPEC,
código e teste, e cada correção foi revertida numa cópia (mutação) para provar
que o teste a pega — todas morreram, menos o mutante equivalente de §8 item 19.

| Achado | Área | Consequência sem a correção | Teste |
|---|---|---|---|
| Recusa `logic`: `Release` antes do `ring.Commit` (o SN já corrigira; P e fast tinham até 2026-10-11) | sink | Em `core`, o ring contava os bytes do lote para sempre e acabava recusando tudo como cheio | `TestNATSLogicRefusalFreesTheRingBytes`, `TestWorkersPartialDeliveryDropsOnlyTheRefused` |
| Em `core`, o `Write` não lia o veredito de permissão | sink | Lotes negados confirmados, contados como escritos e perdidos até a próxima checagem | `TestNATSCoreDeniedPublishIsFatalAtOnce` |
| O contrato do stream só era conferido no boot | sink | Stream editado por baixo: `network` para sempre, sink UP, ring enchendo, `429` eterno | `TestNATSStreamEditedAtRuntimeIsFatal` |
| `max_bytes` com `discard: new` menor que uma mensagem passava no boot | sink | `10077` fatal no primeiro evento grande e em toda reingestão, em laço | `TestNATSValidateRefusals` |
| "User Authentication Expired/Revoked" no `CONNECT` era `network` | sink | O boot esperava o `boot_timeout` e saía com o motivo errado | `TestClassifyNATS` |
| `sizedItemIn` tirava uma amostra do relógio por passada | teste | 1 em 20 execuções falhava sem bug nenhum | `-count=20` limpo |
| A porta do dump aceitava o que o escritor nunca escreve (chave repetida, `1e999999`, 9000 níveis, `\u0041`, `uuid` de outro instante) | emergência | Publicado e confirmado; veneno para `jsonb`, decodificadores de `float` e leitores recursivos | `TestGateRefusesWhatTheWriterNeverWrites`, `TestGateAdmitsEverythingTheWriterWrites`, `FuzzGateAdmitsTheWriter` |
| Sem *hard link* (vfat, exFAT, FUSE), todo dump falhava | emergência | O ring inteiro perdido onde a escrita no lugar funcionava | `TestWriteOnAFileSystemWithoutHardLinks` |
| Diretório que não abria para o `fsync` apagava o dump pronto | emergência | Dump completo removido, erro com o nome temporário | `TestWriteKeepsTheDumpWhenTheDirectoryCannotBeOpened` |
| FIFO ou *symlink* com nome de dump era listado | emergência | Boot travado sem atender `SIGTERM`; leitura sem fim de `/dev/zero` | `TestListSkipsWhatIsNotARegularFile` |
| Temporários de dumps cortados nunca eram informados | emergência | Um dump interrompido invisível; o gêmeo de um entregue, confundível com ele | `TestLeftovers`, `TestBootRemovesTheTwinOfADumpAndKeepsAnInterruptedOne` |
| Um `drain_timeout` por fase no shutdown (herdado do P) | ciclo de vida | Dump só em 2× `drain_timeout`, depois do `stop_grace_period`: `SIGKILL` levava o ring | `TestShutdownDrainTimeoutBoundsConnectionsAndRingTogether` |
| As métricas de emergência reliam o diretório inteiro depois do dump (herdado do P) | ciclo de vida | Pico de memória do tamanho do dump sob um `GOMEMLIMIT` de 2× o ring | leitura na revisão; desde o porte de volta (2026-10-11), `TestEmergencyGaugesCountTheDumpWithoutReReading`, com o diretório sem permissão de listagem: uma releitura deixaria os *gauges* em zero |
| Em `core`, dump de outro Workspace publicado às cegas e em silêncio | sink | Perda provável sem aviso | `TestNATSCoreWarnsOnceAboutAForeignSubject` |
| Token da URL no log "nats connected"/"nats reconnected" | config | O token no log INFO de todo boot e no WARN de toda reconexão | `TestNATSConnectLogsNoCredentials` |
| O erro de URL que não parseia citava o trecho que o `net/url` cita | config | A senha até a primeira `/`, `#`, `?` ou `,` no stderr do container | `TestConfigReviewFindings` |
| O procedimento documentado do `LOGHUB_EB_URL` no Docker não funcionava | docker | Exit 1 por conflito com as variáveis vazias do compose; `LOGHUB_EB_URL`, token e `.creds` nunca chegavam ao container | `TestConfigReviewFindings`, `docker compose config` |
| `NaN` passava em `high_water` e `access_sample` (o SN recusa) | config | O portão de saturação do `/readyz` desligado em silêncio | `TestConfigReviewFindings` |
| O dump de config não relia valores com espaço nas pontas ou entre aspas (o SN tem `quoteINI`) | config | `stream = 'LOGHUB_X'` relido apontava para outro stream | `TestConfigReviewFindings` |
| `sink.nats.urls` aceitava caminho, consulta e fragmento | config | `nats://u:1234/5678@h` discava `u:1234` sem credencial até o `boot_timeout` | `TestConfigReviewFindings` |
| Parsers de INI e de flags ecoavam a linha e o argumento | config | `password: x` ou `sink.nats.password=x` no stderr | `TestConfigReviewFindings` |
| `admin.bind` vazio era só aviso (o SN recusa) | config | `/metrics` em toda interface, numa porta aleatória | `TestConfigReviewFindings` |

### Porte de volta das correções dos irmãos (2026-10-11)

A revisão independente do porte do N para o P, o fast, o W, o SN e o SP achou
defeitos que o N também tinha; foram corrigidos lá primeiro e trazidos para cá
depois (SPEC primeiro, código, teste, mutação). Cada correção foi revertida
numa cópia de rascunho do módulo, com o teste verde sem a reversão como
controle: 20 mutantes, 20 mortos, contando os da revisão abaixo (SPEC §10.3,
§8.7.5, §13.2, §9.1).

| Achado | De onde | Consequência sem a correção | Teste |
|---|---|---|---|
| Um upload que terminava depois do último `Drain` do shutdown entrava no ring | P, fast, W (o N tinha o mesmo) | `200` para um evento fora do stream e do dump, sem contagem, e exit 0. O ring agora é selado (`Seal`, `RWMutex` contra o `Enqueue`) antes do `Drain` | `TestUploadFinishingAfterTheDrainIsRefused`, `TestMemorySealIsABarrierForEnqueue` |
| A sonda `AccountInfo` depois de "sem respondedores" na primeira checagem | fast, W, SN | Não distinguia nada (o `jetstream` traduz "sem respondedores" para "não habilitado"); o pedido a `$JS.API.INFO`, negado a um papel mínimo, virava veredito de permissão permanente com a causa errada, depois de esperar o `health_timeout` | `TestNATSNoRespondersNeverProbesTheAccount` (servidor falso que responde `503` a todo pedido e nega `$JS.API.INFO`), `TestNATSValidateRefusals`, `TestNATSJetStreamDisabledIsFatal` |
| Subject de outro `space` retirado do stream depois do dreno era FATAL | fast, W (veio do N) | Estreitar o stream, como o erro do `cover` manda, derrubava um processo saudável: FATAL, dump do ring, exit 3. Agora é esquecido, e a próxima publicação nele pergunta de novo | `TestNATSForeignSubjectDroppedAfterTheDrainIsForgotten`, `TestNATSForeignWorkspaceDumpIsFatalUntilCovered`, `TestNATSValidateRefusals` |
| Flag sem valor engolia a flag seguinte; valor depois de um espaço passado o `=` virava argumento citado | SN | `--sink.workers --sink.nats.password=x` e `--sink.nats.password= x` imprimiam a senha no stderr | `TestConfigReviewFindings` |
| O `WARN` do dump interrompido dizia "contados como perda" em todo caso | P, SN, SP | Só um aborto por segundo sinal os conta; depois de um `SIGKILL` ninguém os contou, e o operador apagava um dump que podia estar inteiro. O `WARN` e a SPEC dizem como reingeri-lo | `TestBootRemovesTheTwinOfADumpAndKeepsAnInterruptedOne` (agora confere o `WARN`) |

Vieram junto, pela mesma forma: o teste dos *gauges* de emergência somados sem
reler o diretório (`TestEmergencyGaugesCountTheDumpWithoutReReading`, que a
correção do N já cumpria) e o `WARN` "drain deadline reached" com o
`drain_timeout` configurado em vez do resto do prazo da fase 4, como nos
irmãos — este sem teste (é a chave de um log).

**Revisão independente do porte de volta** (um revisor somente-leitura, numa
cópia): a suíte e os testes novos passaram, e cinco achados, todos corrigidos:

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| O teste do `Seal` não provava a **ordem**: o gancho `afterDrain` rodava depois do `Drain`, e um `Seal` movido para entre os dois sobrevivia (o do P tem o mesmo) | A ordem, que é o ponto da correção, sem guarda | `TestUploadFinishingAfterTheDrainIsRefused` com o ring do teste agindo dentro do `Drain`; o mutante agora morre |
| Três caminhos de flag ainda imprimiam o segredo (iguais no P, no fast e no SN): `--sink.nats.password --x` nomeava "a flag `--x`" (regressão do porte); `--sink.nats.password= -x` saía como parâmetro desconhecido; a cauda de um valor partido pelo shell (`--sink.nats.password=$P`) saía como argumento solto | O segredo no stderr do contêiner | `TestConfigReviewFindings` (casos novos, e controles: um erro de digitação depois de um valor continua nomeado) |
| SPEC §8.7.7 dizia que a validação roda "uma vez" | A SPEC contradizia o código e §8.7.5 | — (texto) |
| O comentário do `Leftovers` ainda dizia "contados como perda" | Comentário contra a correção 5 | — (texto) |
| A linha da revisão de 2026-10-10 dizia que a soma dos *gauges* não tinha teste | Documento desatualizado | — (texto) |

E um achado da última passada de mutação, depois da revisão: o
`TestMemorySealIsABarrierForEnqueue` herdado do P (300 rodadas) matava o
`Enqueue` sem trava só em 11 de 20 execuções sem `-race` (20 de 20 com ele). Com
3000 rodadas, 20 de 20 sem `-race`, em ~1 s.

### Porte das correções da família syslog/RELP (2026-10-13)

**A (ciclo de vida, dump, shutdown, boot).** Cada item com SPEC primeiro, o
teste abaixo e a correção revertida numa cópia do módulo (mutação): 32 de 32
mutantes mortos. Os testes são os do `go-webhook-fast-server`
(`port_lifecycle_test.go`, `durable_test.go`), adaptados ao dump JSON Lines e
ao destino falso (`memSink`); o código comum é igual entre o P e o N.

| Item | Teste |
|---|---|
| `fsync` do diretório no `Remove` e na quarentena; `ErrNotDurable` (inclusive na abertura do diretório) vira `WARN`, não o "cannot remove" falso; a escrita do dump continua melhor esforço (SPEC §9.1, §9.2) | `TestRemoveSyncsTheDirectory`, `TestRemoveWithAnUnopenableDirectoryIsNotDurable`, `TestQuarantineWithAFailedDirectoryFsyncIsQuarantined`, `TestWriteKeepsTheDumpWhenTheDirectoryFsyncFails`, `TestReingestedDumpRemovedWithoutDurabilityIsAWarning`, `TestQuarantineWithoutDurabilityIsStillReported` |
| Posse única da perda do aborto (`lossMu`, `enterDumpPhase`, `oweDump`, `settleDump`), no lugar do par `atRisk`/`collected`: o aborto entre o `Drain` e o registro do recolhimento contava a menos (2 de 5) (SPEC §10.3; fecha §8 item 18) | `TestAbortBetweenTheDrainAndTheDumpCountsEverything`, `TestAbortAfterASavedDumpCountsNothing`, `TestAbortAndDisabledDumpCountOnce`, `TestAbortAndFailedDumpCountOnce`, `TestAbortDuringTheDrainIsCountedOnce`, `TestAbortBeforeTheDumpPhaseKeepsItClosed`, `TestNewlineEnvelopeLeftOutOfTheDumpIsCountedOnce` (o envelope com `0x0A`, agora contado pelo dump) |
| O dump que o aborto contou desiste (`Store.SetAbandon`), e o que publica depois da última conferência não é contado de novo | `TestWriteGivesUpWhenAbandoned` (antes da primeira linha, a cada 1024, antes do `link`), `TestDumpAfterTheAbortIsAbandoned`, `TestAbortAfterTheLastAbandonCheckCountsNothingMore`, `TestAbortDuringTheDumpCountsItOnceAndLeavesNoDump` (ponta a ponta: `beforeDump` segura o dump até o aborto contar) |
| X2: o dump toma o contador sob `lossMu`, nos dois ramos (com dump e `emergency.enabled=false`) | `TestDumpThatSawTheAbortLeavesTheCountToIt` (`inAbort`: o dump roda com a trava do aborto na mão) |
| `SIGQUIT` nunca tolerado, e o primeiro `SIGTERM`/`SIGINT` do shutdown por FATAL tolerado (já valia; agora com teste de unidade em `shutdownWith`) | `TestFatalShutdownToleratesTheFirstSignal` |
| O canal do aborto registrado antes de o contexto soltar `SIGTERM`/`SIGINT` (um segundo sinal no meio saía 143 sem contar) | sem teste: a janela é de instruções, entre o `signal.Stop` do contexto e o `signal.Notify`; provado por leitura |
| X1: `bind` cancelado por sinal = 130, na porta admin e na pública, com nome de host e com IP literal; o `Server.Close` fecha também os listeners que o `Serve` nunca recebeu | `TestSignalBeforeTheBindExits130` (`127.0.0.1:0` e `localhost:0`, a porta solta depois), `TestCancelledAdminBindIsAnAbortNotAConfigError` |
| Um sink FATAL durante o dreno do `SIGTERM` encerra a espera, e a saída é 3 sempre que a saúde termina FATAL (SPEC §10.3; fecha SPEC §18.1 item 6, já registrado) | `TestFatalDuringTheDrainEndsTheWaitAndExits3` (prazo de 10 min; o dump sai na hora), `TestFatalDuringThePreStopDelayExits3` (ring vazio: saía 0) |
| Leitura dos dumps em todo boot, com `emergency.enabled` desligado também, e o diretório que não se lista só para uma vida que escreve dumps (fecha SPEC §18.1 item 4) | `TestDumpsAreReingestedWithEmergencyDisabled`, `TestUnlistableEmergencyDirStopsOnlyALifeThatDumps` |
| *Gauges* de emergência do arquivo inteiro e do disco: `whs_emergency_files` posto no início da reingestão, o `Count` contando o arquivo ilegível, falha da recontagem com `WARN` (fecha SPEC §18.1 item 5) | `TestEmergencyFilesGaugeDuringReingestion`, `TestCountKeepsAnUnreadableFile`, `TestEmergencyGaugesCountAnUnreadableDump` (o `WARN` sem teste: só log) |
| `withLifetime` e `validationContext` (SP2/RP): não se aplicam — o sink NATS valida o stream dentro da checagem, sobre o `StreamInfo` que acabou de buscar, e nenhuma validação passa do `health_timeout` (como no SN); só o comentário do RN entrou no `health.go` | — |

Não se aplicam (DECISOES, "Porte das correções da família syslog/RELP"): o
`quitNow` (sem observador de `SIGQUIT` fora do shutdown), o prazo da
reingestão sem progresso (sem retentativa nem prazo único; a leitura é do
arquivo inteiro, sem "pedaço"), o fechamento em paralelo (o `Server.Shutdown`
já fecha os listeners juntos) e o A4 do plano (não há `ring=sqlite`).

**B (configuração, `internal/config/`).** Testes em
`internal/config/family_port_test.go`; cada um falhou contra o código de antes
(conferido no módulo intocado; `TestFamilyPortRetiredKeyAfterAValueIsNamed` é
guarda da regra nova) e cada correção revertida sozinha derruba ao menos um
(32 mutantes, 31 mortos; o que sobra é equivalente aqui: tratar `0` como "não
derivado" não muda nada sem um derivado inteiro).

| Item | Teste |
|---|---|
| Cauda depois de um valor nomeada só dentro de uma seção conhecida (`knownSectionOf`; fecha §8 item 20), contando as seções do fast que este servidor não tem (`retiredSections`) | `TestFamilyPortTailAfterAValueIsNotNamed`, `TestFamilyPortRetiredKeyAfterAValueIsNamed`; controles de `TestConfigReviewFindings` ajustados |
| `-h`/`-v` depois de uma flag que espera valor não é nomeado como *switch* | `TestFamilyPortShortDashValueIsNotNamedAsAFlag` |
| Valor colado ao nome, `:` no lugar do `=`, na flag e no INI; chave antes de seção | `TestFamilyPortGluedValueIsNotPrinted` |
| Continuação de segredo numa linha do INI (senha, token, URL com credencial) | `TestFamilyPortINISecretContinuationIsNotPrinted` |
| *Switches* com valor; `--config=` vazio; BOM na linha 1 | `TestFamilyPortSwitchesTakeNoValue`, `TestFamilyPortEmptyConfigFlagIsRefused`, `TestFamilyPortINIByteOrderMarkIsIgnored` |
| `emergency.dir` derivado comentado no `--dump-config` | `TestFamilyPortDumpCommentsDerivedValues` |
| Tetos (`capacity_items`, `batch`, `workers` ≤ 256, `listeners`) | `TestFamilyPortConfigCeilings` |
| `LOGHUB_EB_URL`: só brancos recusada; `@` depois de `/`, `?` ou `#` com a dica do escape; host codificado; `#` ou `?` vazio; nada da URL no erro | `TestFamilyPortEBURL`; `TestEBURLUnsupportedOrMalformed` e `TestConfigReviewFindings` ajustados (só brancos não é mais "não definida"; a mensagem do `@` mudou) |

Já estava feito: fragmento e caminho/consulta recusados (`TestEBURLUnsupportedOrMalformed`),
chave própria vazia que não conflita (`TestConfigReviewFindings`). Não se
aplica: `sink.batch_max_bytes` (*revisto em 2026-10-14: existe, por decisão do
usuário, ver a rodada abaixo*), `pool_size`, `ring.max_inflight` (não existem)
e as regras de URL PostgreSQL (a `LOGHUB_EB_URL` daqui é só `nats://`).

**C (sink NATS).** O sink do `go-syslog-nats-server` depois do porte de
2026-10-13 (commit `fb92edd`, que o trouxe do `go-relplog-nats-server`),
conferido item a item contra este módulo, mais o texto "store is closed" da
revisão do `go-syslog-server` (X3) e as URLs NATS do SN. O servidor falso
(`natsFake`) e os seus auxiliares vieram com os testes. Cada teste falhou no
código de antes, e cada correção foi revertida sozinha numa cópia (mutação):
24 mutantes, todos mortos — 20 do sink, 4 da configuração e da app. A regra do
*pool* do `go-syslog-server` (C1) **não** veio: SPEC §8.7.6 já decidia que um
nó que exige TLS é veredito, e um teste agora guarda essa decisão
(`docs/DECISOES.md` §3).

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| `10077` de stream `discard: new` cheio ("maximum messages exceeded", texto real do `nats-server` 2.14.6) ou fechando ("store is closed", o `ErrStoreClosed` de `server/store.go`; a família casava só "store closed") era fatal | Dump e exit 3 por contrapressão ou por um reinício do NATS, e a parte gravada do lote duplicada na reingestão | `TestNATSFullDiscardNewStreamIsNetwork` (servidor real), `TestClassifyNATS` |
| `10158`, `10167` e todo 429 caíam na regra do 4xx desconhecido (SPEC §18.1 item 8) | FATAL em toda instância de um stream em cluster degradado ou num pico | `TestClassifyNATS` |
| `retention: interest` aceito; `max_msgs_per_subject` sem aviso (SPEC §18.1 item 7) | `PubAck` sem gravar: o item fora do ring e perdido sem log | `TestNATSValidateRetentionAndPerSubjectCap`, `TestNATSInterestStreamIsRefused` (servidor real) |
| A assinatura negada em `_INBOX.` não era veredito (SPEC §18.1 item 9); e a publicação negada só virava FATAL na checagem **seguinte** | O boot esperava o `sink.boot_timeout` culpando a rede; um lote retentado com o sink UP | `TestNATSVerdictIsReturnedByHealthAndWrite`, `TestNATSJetStreamDeniedPublishIsFatalAfterTheTimeout` (o `ErrorHandler` atrasado: só a leitura do `LastError` dá o veredito), `TestNATSPermissionDeniedIsFatal` (servidor real com permissões: `nopub` em `jetstream` pelos dois prazos e em `core`, `noinbox`, `nobeacon` de controle), `TestNATSPermissionViolationOnTheDataPathIsFatal` |
| O servidor que passa a exigir TLS depois do boot: o `nats.go` só o relata pelo `LastError`, nunca pelo `ReconnectErrCB` | `connection is RECONNECTING`, `network`, até os fusíveis — ou para sempre | `TestNATSReconnectToATLSOnlyServerIsAVerdict` (os dois modos), `TestNATSATLSServerOfAPoolIsAVerdict` (nó em texto claro derrubado, o outro com `tls {}`: a decisão de SPEC §8.7.6) |
| *Flush* `core` que atravessa uma reconexão; *flush* cortado por uma queda; negação do beacon mascarando a do caminho de dados | Um lote confirmado sem o servidor tê-lo recebido; FATAL, dump e exit 3 por um soluço; um lote negado confirmado | `TestNATSCoreFlushAcrossAReconnectionIsNotConfirmed`, `TestNATSCoreFlushCutByAReconnectionIsNetwork`, `TestNATSCoreMaskedDenialIsNotConfirmed`, `TestMaskedDenial`, `TestNATSCoreHealthReadsTheDenialItsFlushFollows` |
| `MaxPending` sem folga | Retentativa rápida de um lote contaminado parada na janela cheia (`ErrTooManyStalledMsgs`) | `TestMaxPendingHasTwiceTheWorkersBatches`, `TestNATSConfigCarriesTheDeadlinesAndCeilings` (app; este fixava a fórmula antiga) |
| URLs NATS do SN: `@` depois de `/`, `?` ou `#` só dizia "path"; um pedaço da lista sem esquema não dizia que uma `,` parte a lista; o *escape* no host culpava a senha | O operador procurando o erro no lugar errado | `TestConfigReviewFindings`, `TestNATSURLSchemes` |

Não se aplicam: o teto de `ring.max_inflight` (C3) — este servidor não tem
`ring=none` nem a chave; o aviso de um dump de outro `space` em `core` (SPEC
§21.1 item 8 do SN), que este módulo já tinha por subject.

**D (o dreno).** O `Deliver` do `go-relplog-pg-server` olhava o contexto antes
da classe da resposta, e um `SIGTERM` durante uma recusa `logic` a contava sem
o `ERROR` com os uuids. Os workers daqui (o `sink/workers.go` comum a P, F e W)
já tratavam a classe primeiro: não se aplica, e ficou a guarda
`TestWorkersLogicRefusalDuringShutdownIsLoggedAndCounted` (2 de 2 mutantes
mortos: o contexto conferido antes da classe, e o `ERROR` rebaixado). Os itens
SQL da rodada não se aplicam: este módulo não tem destino SQL.

**Integração (as quatro áreas fundidas).** O `TestWorkersTaintABatchAfterATimedOutAttempt`
falhou duas vezes durante a rodada (`DATA RACE` entre o `Release` de
`sink/workers.go` e a leitura de `it.Env` no teste; 1× no W, 1× no N), com o
`workers.go` e o teste iguais ao `base`. Não era defeito de posse: o teste
iniciava o worker **antes** de enfileirar os quatro itens, e o worker podia
pegar só os primeiros, estourar o prazo neles (marcados, nunca voltam ao pool)
e entregar os demais numa tentativa que não estourou — esses voltam ao pool,
como manda a SPEC §12.3, e o teste lia o `Env` que o `Release` zerava (com
`-race -count=200`, 3 falhas; com a guarda nova, 6 de 300 diziam "the stalled
write carried 1|2|3 of the 4 items"). Correção no teste: enfileirar antes do
`Start` (como o `TestWorkersMarkFatal`) e conferir que a escrita parada levou
os quatro (`stallOnceSink.stalled`); `-race -count=500` verde. O
`go-syslog-standard-server` tem o mesmo teste com a mesma ordem.

### Porte das correções de W e F (2026-10-14)

As correções que fecharam a SPEC §19.1 do `go-webhook-server` (W) e do
`go-webhook-fast-server` (F), portadas do código de F por `diff`. Cada item:
SPEC primeiro, o teste abaixo falhando contra o código de antes (com só o
mínimo para compilar: o campo ou a função vazia), a correção, o teste verde, e
a correção revertida sozinha (mutação) derrubando o teste. Progresso e saídas
em `tasks/progress-P-N.md` da raiz.

| Item (W) | Regra | Teste | Falha antes | Mutantes mortos |
|---|---|---|---|---|
| W 1 `Content-Length` pré-aloca | §12.2, §4.3 passo 11 | `TestReadBodyAllocatesOnlyWhatArrives` | buffer cap 5242880 | sem o teto de 64 KiB |
| W 2 checagem rápida por bytes | §4.3 passo 9 | 3 casos em `TestErrorAnswersBeforeTheUUIDArePlainText` + controle | `status = 200, want 429` ×3 | sem a cláusula de bytes |
| W 3 dreno vs h2/`Expect` | §4.3 | `TestRejectDrainFollowsTheProtocol`, `TestRejectWithExpectSendsNo100Continue` | 65537 bytes lidos; `100 Continue` como primeira linha | sem o h2; sem o `Expect` |
| W 4 `write_timeout` da resposta | §13.5 | `TestSlowUploadStillGetsItsAnswer`, `...OverH2C`, `TestWriteTimeoutIsArmedForEveryAnswer` | `unexpected EOF`; `INTERNAL_ERROR`; prazo não armado ×3 | sem limpar (h2c); sem rearmar |
| W 5 `accept` e `Serve` com erro | §10.3, §10.5 | `TestAcceptRetriesNonTemporaryErrors`, `TestServeReturnsWhenOneListenerStops`, `TestServeErrorGoesThroughTheShutdown` (app: `memSink` com `hang` no lugar do `heldSink`/`sink=sqlite` de F) | conexão recusada; `Serve` esperando; `exit 1, want 3` e 5 de 5 sem entrega | sem `retryListener`; `Serve` esperando todos; `return ExitConfig` antes do shutdown |
| W 6 endereço exato (contestável) | §13.5 | `TestPerIPLimitCountsTheExactAddress` (trava) | passa por desenho | chave por /64 |
| W 8 ring cobra o buffer | §7.2, §12.2, §12.4 | `TestMemoryChargesTheRetainedBuffer`, `TestMemoryGivesBackWhatItCharged`, `TestItemOverheadCoversTheStruct`, `TestMemoryByteCeiling` ajustado | `bytes = 100, want 16640`; `Bytes:300, want 33280` | `it.Size()` no `Enqueue`; `it.Size()` no `Commit` |
| W 30 retidos no teto de itens | §7.2 | `TestMemoryHeldItemsCountAgainstTheCeiling` | `err = <nil>, want ErrFull` | `capItems+1000` |
| W 10 worker sobrevive ao `Dequeue` | §8.1 passo 1 | `TestWorkersSurviveTransientDequeueErrors`, `TestWorkersStopOnTheEndsOfTheRing` (sem `ErrNoBacklog`) | `delivered 0 of 5` | `dequeueEnds` sempre verdadeiro |
| W 11 `sink.batch_max_bytes` | §8.1, §9.2, §13.5 | `TestSplitByBytes`, `TestWorkersSplitABatchByBytes`, `TestWorkersSendAnEnvelopeAboveTheCeilingAlone`, `TestWorkersHoldTheRestOfTheBatchWhileAPieceRetries`, `TestSinkBatchMaxBytes`, `TestBootWritesInPiecesUnderBatchMaxBytes` (dump e workers; novo, `internal/app/batch_bytes_test.go`) | `writes = [[4 4197748]]`; `[[3 68127]]`; lote dado como resolvido; `default = 0`; `a write of 5 items` | `n := len(rest)`; `unhold` que apaga tudo; sem o teto de 256 MiB; app sem `BatchMaxBytes`; boot sem o corte |
| W 29 guardas em `t.Fatalf` | §15.1 | `describe_test.go` (3) | com o bug de volta: `SKIP`, `ok` | `lastElem` no JA3; `hostPort` não zerado; `DisableGeneralOptionsHandler: false` |
| W 20 (§18.1 item 10) U+2028/U+2029 crus | §9.2 | +4 casos em `TestOurShapeEscapes`, +2 *probes* em `TestGateRefusesWhatTheWriterNeverWrites` ("raw U+2028", "same key in two spellings") | `str(...) = true` ×2; `the gate let it through` ×2 | sem a recusa em `shape.go` |
| W 24 defesa dupla no parser | §13.5 | `TestNonFiniteNumbersAreRefused` (`pending_config_test.go`, o mesmo do P, com `TestEmptyAdminBindIsRefused`, `TestDumpINIRoundTripsQuotedAndPaddedValues` e `TestQuoteINI`, que já passavam aqui) | a recusa vinha da validação, sem "must be finite" nem a linha do INI | sem `IsNaN`/`IsInf` |
| Compose (W 27/28 por regra; o volume do `.creds`) | §13.6 | `TestComposeForwardsEverySinkVariable`, `TestComposeImagesArePinned` (`compose_test.go`, novo) | passava (o volume não é testável sem Docker) | `nats-box:latest` |

A armadilha de 2026-10-14 (registrada pelo grupo B de W): o texto `\u2028`
digitado na ferramenta de edição vira o caractere cru. Os testes novos usam
`\xe2\x80\xa8` no Go, e a contagem de U+2028/U+2029 crus por arquivo foi
conferida igual à de antes.

**Rodadas.** `gofmt -l .` vazio e `go vet ./...` limpo. `go test -race
-count=1 -timeout 1500s ./...` com `tasks/infra/env.sh` (JetStream com 8 MiB em
24222): duas rodadas completas verdes (56 s e 57 s); e mais duas com
`WHS_TEST_NATS_URL=nats://127.0.0.1:24226` (JetStream com o `max_payload`
padrão de 1 MiB, para os testes que medem o teto do servidor): verdes (56 s e
55 s). Nenhuma refeita.

### Revisão independente da família webhook (2026-10-14)

Os achados de `tasks/review-web-{http,ring,sinks}.md` que pediam código (e as
três notas do coordenador sobre a sonda do `sink=sqlite`, vindas das revisões
das famílias syslog e RELP). Ordem por item: SPEC, teste falhando contra o
código de antes, correção, mutação (só a correção revertida). Os só
documentados estão em §8 e na seção do assunto da SPEC.

| Item (W) | Teste | Falha antes | Mutação |
|---|---|---|---|
| W 31 — o shutdown por erro do `Serve` tolera o primeiro sinal (§10.3) | `TestServeErrorGoesThroughTheShutdown/held sink, then one signal: still dumped, exit 3` | `exit 130, want 3` | o ramo `serveErr` com `tolerateFirst=false` |
| W 32 — prazo de escrita limpo só com `read_timeout > 0` (§13.5) | `TestWriteDeadlineIsKeptWithoutReadTimeout`, `TestSlowH2CUploadIsBoundedWithoutReadTimeout` (h2c real) | `deadline cleared on entry = true`; `status 200 … outlived write_timeout` | a condição antiga |
| W 35 — piso de `ring.max_bytes` com `ring=memory` (§7.2) | `TestRingMaxBytesCoversTheLargestEvent` | `--ring.max_bytes=65791 … was accepted` | a condição com `&& false` |
| W 39 — `WaitReady` dorme `min(backoff, restante)` (§10.1) | `TestWaitReadyDoesNotSleepPastItsDeadline` | `took 2.00s with a 300ms budget` | `time.After(backoff)` |
| W 40 — `admin.bind` com porta 1–65535 (§11) | `TestEmptyAdminBindIsRefused` (+ `127.0.0.1:`, `:`, `[::1]:`, `localhost:abc`, `:0`, `:65536`, `:+80`, `:080`) | `--admin.bind=127.0.0.1: … was accepted` | sem o `validPort` |
| W 41 — byte de controle num valor de texto é exit 1 (§13.2) | `TestControlBytesInTextValuesAreRefused` | as 4 entradas `was accepted` | a checagem desligada |
| T2 — o resto do lote retido com falha no pedaço do meio, e com fatal (§8.1 passo 3) | `TestWorkersHoldTheRestOfTheBatchWhileAPieceRetries` (6 itens, 3 pedaços) | (lacuna de teste: passa no código) | `hold` por pedaço |

**Rodadas.** `gofmt -l .` vazio e `go vet ./...` limpo. Duas rodadas completas `go test -race -count=1 -timeout 1500s ./...` (`tasks/run2.sh go-webhook-nats-server rev`, NATS reais do `env.sh`): as duas verdes; e o pacote `internal/sink` com `WHS_TEST_NATS_URL=nats://127.0.0.1:24226`, verde, nenhum teste pulado.
