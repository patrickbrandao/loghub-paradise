# go-webhook-pg-server — Procedimentos de teste

Companheiro operacional de `SPEC.md` §15. As suítes vieram do
`go-webhook-fast-server` em 2026-10-07 e foram reduzidas ao ring de memória e
ao destino PostgreSQL; o histórico de §9 é o dele no que vale para o código que
ficou. Este documento diz **como rodar**, **o que cada suíte prova** e **o que
ainda não é provado**.

---

## 1. Suíte rápida (todo commit)

```bash
cd go-webhook-pg-server
gofmt -l .            # deve ser vazio
go vet ./...
go test -race ./...
```

Tempo: ~45 s sem PostgreSQL, ~50 s com ele (2026-10-07, macOS). O `-race`
**não é opcional**: os bugs de perda de dados deste projeto vivem em caminhos
concorrentes, e a liberação dupla de buffer (SPEC §12.3) só é detectável assim.

Sem `WHS_TEST_PG_DSN`, os testes de PostgreSQL **pulam**; o ciclo de vida
inteiro (boot, dreno, DOWN, FATAL, dump, reingestão, `SIGHUP`) roda mesmo
assim, contra o destino falso de `internal/app/fakesink_test.go` (SPEC §15.3).

## 2. Cobertura real

Cobertura por pacote engana: a suíte de `internal/app` exercita `server`,
`ring` e `sink` sem que isso seja atribuído a eles. Use `-coverpkg`:

```bash
go test -race -coverpkg=./internal/... -coverprofile=cov.out ./... && go tool cover -func=cov.out | tail -1
```

Medida de referência em §8 (item 1); refaça ao mexer em qualquer pacote.

## 3. Fuzzing (SPEC §15.2, obrigatório)

Antes de qualquer release, **10 minutos cada**:

```bash
go test ./internal/clientip/  -run=XXX -fuzz=FuzzResolve      -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=FuzzRead         -fuzztime=10m
go test ./internal/envelope/  -run=XXX -fuzz=^FuzzEnvelope$   -fuzztime=10m
go test ./internal/server/    -run=XXX -fuzz=FuzzServeHTTP    -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$  -fuzztime=10m
```

O que cada um prova:

- `FuzzResolve` — o único parser de entrada hostil estruturada; um bug ali é
  spoofing de IP.
- `FuzzRead` — o leitor do dump JSON Lines (SPEC §9.2), em memória (com um
  arquivo e `fsync` por entrada eram ~3 execuções/s no macOS; em memória, ~3 mil):
  todo item recuperado é um JLH deste servidor, JSON e UTF-8 válidos, sem
  `0x0A`, com o `uuid` como id; todo byte da entrada é contado exatamente uma
  vez (item e seu `\n`, dano ou linha vazia); o que foi recuperado reescreve e
  relê igual, sem dano.
- `FuzzEnvelope` e `FuzzServeHTTP` — todo envelope passa no contrato, o
  `ParseHead` o lê de volta, e **nenhum tem `0x0A` cru** (SPEC §9.1, a premissa
  do enquadramento por linha).
- `FuzzJSONBSafe` — a troca do `\u0000` na cópia para o `jsonb` só troca escapes
  reais.

### 3.1 Contrato LogHub (SPEC §3, §15.1)

O oráculo `internal/jlhcheck` valida todo envelope que um teste lê de volta —
do handler, do destino falso, do dump e da fila — contra as cópias dos schemas
em `internal/jlhcheck/loghub/` e as regras do `validate.py` da skill. Um
envelope lido da `loghub.inbox` passa por `jlhcheck.ValidateJSONBCopy`, que
admite exatamente a troca de NUL por U+FFFD na query (invariante 11).

## 4. Integração sem PostgreSQL (SPEC §15.3)

`internal/app` monta a mesma aplicação do binário (`runContext`) e troca só o
destino, pelo campo `App.sinkOverride`. O destino falso (`memSink`) guarda uma
**cópia** de cada envelope confirmado e pode ser posto DOWN ou FATAL no meio do
teste:

| Cenário | Teste |
|---|---|
| Todo `200` chega ao destino, e nada além | `TestEveryAcceptedEventIsDelivered` |
| Ring cheio é `429`, nunca `200` com descarte | `TestRingFullReturns429` |
| Superfície HTTP, h2c, admin, histogramas | `TestHTTPSurface`, `TestH2CPriorKnowledge`, `TestAdminEndpoints`, `TestHistogramInvariants` |
| Seletores de compatibilidade e chaves retiradas | `TestInvalidConfigRefusesToStart` |
| PostgreSQL inalcançável: porta pública fechada, `exit 3` após `boot_timeout` | `TestUnreachableSinkExitsWithoutOpeningPublicPort` (não precisa de servidor: a porta morta é o ponto) |
| `SIGHUP` real ignorado, com `WARN` | `TestSIGHUPIsIgnored`, `TestIgnoreSIGHUPWarnsOnEachSignal` |
| `SIGTERM` com ring cheio: dreno completo, `exit 0` | `TestGracefulShutdownDrainsAFullRing` |
| Segundo sinal: `exit 130` | `TestSecondSignalExits130` |
| Destino cai e volta: `503`, nada perdido | `TestSinkDownThenBackLosesNothing` |
| Destino FATAL: dump JSON Lines lido **à mão**, reingestão no boot seguinte | `TestFatalSinkDumpsAndTheNextBootReingests` |
| `emergency.enabled=false`: `exit 3`, nenhum arquivo | `TestFatalSinkWithoutEmergencyExitsThreeAndWritesNothing` |
| Dump com linhas danificadas no meio e no fim: íntegras entregues, `.corrupt` | `TestDamagedEmergencyDumpIsQuarantined` |
| Linha que o destino recusa como `logic`: o resto entregue, `.corrupt`, boot segue | `TestALineTheDestinationRefusesIsQuarantinedNotFatal` |
| Dump que não se escreve: perda contada uma vez, em `emergency_failed` | `TestFailedDumpIsCountedOnceAsEmergencyFailed` |
| Primeiro sinal durante um shutdown por FATAL: o dump continua | `TestFirstSignalDuringAFatalShutdownKeepsTheDump` |
| `emg-*.bin` do fast: intocado; o `.jsonl` ao lado é reingerido | `TestFastBinaryDumpIsLeftAlone` |

## 5. `sink=postgres` — suíte automatizada (`loghub.inbox`)

Os testes pulam sem um servidor, e o servidor não cria a fila: a suíte cria as
dela, cada uma num esquema próprio, a partir de `docker/postgres/inbox.sql` (a
DDL literal da skill; um teste confere o hash). PostgreSQL **18**.

```bash
docker run --rm -d -p 15432:5432 -e POSTGRES_USER=whs -e POSTGRES_PASSWORD=whs \
  -e POSTGRES_DB=whs_test --name whs-test-pg postgres:18-alpine
WHS_TEST_PG_DSN='postgres://whs:whs@127.0.0.1:15432/whs_test?sslmode=disable' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f whs-test-pg
```

O papel do DSN precisa de `CREATE SCHEMA` e, para os testes de privilégio,
`CREATE ROLE`. Sem Docker, um `initdb` + `pg_ctl` efêmero serve igual:

```bash
initdb -D /tmp/whs-pg -U whs --auth=trust && pg_ctl -D /tmp/whs-pg -o "-p 15432" -l /tmp/whs-pg.log start
createdb -h 127.0.0.1 -p 15432 -U whs whs_test
```

O que prova, além do sink isolado (`internal/sink/postgres_integration_test.go`):

- todo `uuid` aceito chega à fila e cada linha reconstrói um envelope que passa
  no oráculo; corpo binário e `%00` na query ida e volta; UTF-8 inválido no path
  não derruba o lote;
- tabela ausente, tipo errado, índice único ausente, sobre `(uuid, step)` ou
  extra, coluna obrigatória extra, `timestamptz(3)` e papel sem
  `SELECT (uuid)` ⇒ `exit 3` **imediato**, porta pública fechada;
- destino cai (proxy TCP) ⇒ `503`; volta ⇒ nada perdido;
- FATAL ⇒ dump JSON Lines ⇒ reingestão com as colunas consultáveis.

Se você mexeu em `internal/sink/postgres.go`, `inbox.go`, `health.go`,
`workers.go`, na validação de `config` ou em `docker/postgres/inbox.sql`,
**rode esta suíte**.

### 5.1 Verificação manual com o compose

```bash
cd docker && docker compose up -d --build
curl -fsS -X POST localhost:8200/hook?a=1 -d '{"ping":1}'
docker compose exec postgres psql -U webhook -d webhook -c 'SELECT uuid, space, type FROM loghub.inbox'
docker compose down -v
```

## 6. Carga (SPEC §15.4)

```bash
# keep-alive: mede o pipeline
bombardier -c 256 -d 60s -m POST -b '{"k":"v"}' http://127.0.0.1:8200/hook
# conexão nova por request: mede accept + handshake
bombardier -c 256 -d 60s -m POST -b '{"k":"v"}' --disableKeepAlives http://127.0.0.1:8200/hook
# h2c
h2load -n 1000000 -c 64 -m 32 -d body.json http://127.0.0.1:8200/hook
```

## 7. Critérios de aceite (SPEC §15.5) — **não aferidos**

Nenhuma medição de vazão ou latência foi feita neste servidor. A meta numérica
de `ring=memory` + `sink=postgres` só existe depois da primeira medição válida.

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento:

1. **Cobertura não medida** com `-coverpkg` nesta primeira rodada.
2. **A validação de boot no prazo de `sink.op_timeout`** (SPEC §8.5.5) é provada
   pela unidade (`TestValidationContext*`) e pela leitura do `Health`, não contra
   um PostgreSQL com RTT alto: exigiria um proxy que atrase pacotes.
3. **TLS do PostgreSQL**: a montagem do `tls.Config` é exercitada só pelo
   caminho de configuração e pelos casos de CA ausente ou inválida.
4. **`25006` e `pg_is_in_recovery()` em tempo de execução** (réplica promovida
   no meio da operação) não têm teste de ponta a ponta; a classificação é
   unitária (`TestClassifySQLState`) e o efeito de uma escrita fatal é o do
   destino falso (`TestFatalSinkDumpsAndTheNextBootReingests`).
5. **A conta de conexões não foi aferida.** 4 workers e `pool_size = workers + 1`
   são raciocínio (SPEC §8.5.3), não medição.
6. **O teto de 1 GiB de um parâmetro** só é coberto pelo aviso de boot
   (`TestPostgresByteCeilingWarning`).
7. **`kill -9`** perde o ring por desenho (SPEC §16.1); não há o que testar
   além de documentar.
8. **A regra do `/data` (SPEC §13.6) não tem teste automatizado**; quem a
   sustenta é o `read_only: true` do compose. Verificação manual, com a stack de
   pé:

   ```bash
   docker compose exec webhook grep ' / ' /proc/mounts   # deve conter "ro,"
   ```

9. **Um envelope com `0x0A` cru** nunca foi produzido pelo serializador; a
   defesa do dump (deixá-lo fora e contar) é provada com um envelope forjado
   (`TestWriteLeavesOutAnEnvelopeWithARawNewline`).
10. **Linha do dump acima do teto da configuração atual** é pulada e o arquivo
    vira `.corrupt`; reingeri-la exige subir com o `server.max_body_bytes`
    antigo. Provado pela unidade (`TestOverlongLineIsSkippedAndResynchronised`),
    não por um ciclo de dois boots.
11. **`server.listeners > 1`** só é testado no que é portável (os listeners
    sobem e todo evento é servido).
12. **A corrida do `Dequeue` contra o contexto cancelado** é provada por
    repetição (`TestWorkersNeverLoseAnAbandonedBatch`), não por construção.
13. **`SIGTERM` durante a reingestão** sai com 130 e mantém o arquivo; não há
    teste que acerte essa janela.
14. **Código morto herdado do fast**, mantido pela paridade do `diff`:
    `envelope.Item.RowID`, `Item.Done/Ack` (do `ring=sqlite`) e
    `obs.Logger.SetLevel` (da recarga).
15. **`envelope.appendKey` não é usado** (`golangci-lint unused`); veio do fast,
    onde também não é, e fica até a correção ser portada nos dois.
16. ~~**Segundo sinal no meio do dump**~~ — **fechada em 2026-10-13** (§9,
    "Porte das correções da família syslog/RELP": o dump que o aborto contou
    desiste, e o contador passa de mão sob uma trava antes do recolhimento).
    O registro de então: (revisão de 2026-10-11): o aborto conta
    os itens como perda, mas a escrita continua e, se o `link` acontecer antes
    do `os.Exit`, o dump é reingerido no boot seguinte — perda contada a mais,
    nenhum evento perdido. E entre o `Drain` (que já desconta o ring) e o
    `collected.Store`, um aborto lê o ring sem os itens drenados e conta a
    menos. Janela de milissegundos; correção proposta: o `link` como ponto de
    compromisso (o caminho gracioso reivindica o `atRisk` antes dele) e
    `atRisk`/`collected` num só atômico.
17. ~~**`Count` pula um dump ilegível**~~ — **fechada em 2026-10-13** (§9,
    "Porte das correções da família syslog/RELP"). O registro de então:
    (revisão de 2026-10-11, anterior ao
    porte): um arquivo sem permissão de leitura fica no disco e não entra em
    `whs_emergency_files`, ao contrário do que diz a linha de 2026-10-07 abaixo.
    É defeito, com a direção da correção em SPEC §19.1.
18. ~~**Uma brecha deixada no parser de flags** (SPEC §13.2, como no N): a cauda
    de um valor que o shell partiu num espaço, quando começa com traço e vem
    depois de um valor não vazio (`--sink.postgres.password=a -b`), sai como
    parâmetro desconhecido, com o nome. Escondê-la esconderia todo erro de
    digitação de uma flag depois de outra.~~ — **fechada em
    2026-10-13** (porte da família syslog/RELP, `knownSectionOf`): depois de um
    valor, só se nomeia uma flag desconhecida dentro de uma seção deste
    servidor; `TestFamilyPortTailAfterAValueIsNotNamed`.

## 9. Histórico: bugs que os testes encontraram

O histórico completo, com os bugs dos destinos e rings que saíram, está em
`../go-webhook-fast-server/docs/TESTING.md` §9. Os que valem para o código que
ficou aqui:

| Bug | Encontrado por | Consequência se não encontrado |
|---|---|---|
| Zona IPv6 no peer anulava `proxy.trusted` | `FuzzResolve` | Cliente link-local nunca casava com proxy confiável |
| Lote retido evaporava no shutdown | Teste manual do caminho FATAL | Eventos com `200` sumiam sem dump de emergência |
| Liberação dupla de buffer não era atômica | `-race` em `internal/sink` | Corpo de um cliente dentro do evento de outro |
| Lote retido era **sobrescrito** no `held[id]` | `sink=postgres` em Docker: 157 eventos com 200, 156 no dump | Perda silenciosa de eventos já respondidos |
| Reingestão zerava o `ItemMeta` | `sink=postgres` em Docker | Evento reingerido inconsultável por SQL |
| Papel só com `INSERT` passava no boot | `TestPostgresHealthValidatesTheRole` | Primeiro lote falhava com `42501` |
| Só a primeira linha de `X-Forwarded-For` era lida | Auditoria contra §5.2 | Origem forjada com um único header atrás de HAProxy |
| `max_conns` e `max_conns_per_ip` eram por listener | Auditoria contra §13.5 | Tetos multiplicados por `server.listeners` |
| Durações em `0` passavam pela validação | Auditoria | `op_timeout = 0` retentava para sempre; `drain_timeout = 0` saía com 3 |
| `timestamptz(6)` recusado; índice único ou exclusão extra aceitos | Tabelas reais (revisão de 2026-10-06) | Boot recusado sem motivo; ou `23505` derrubando o lote |

### Rodada de 2026-10-07 (criação do pg)

| Achado | Como | Consequência sem a correção |
|---|---|---|
| A validação de boot do PostgreSQL rodava no `sink.health_timeout` (1 s), embora a SPEC do fast dissesse `sink.op_timeout` — e o parágrafo da SPEC trazia texto do ClickHouse | Leitura do `Health` contra §8.5.5 ao montar a SPEC | Com RTT alto (60 ms × uma dúzia de idas e voltas) o boot nunca validava e saía com 3 após `boot_timeout`. Corrigido aqui (`validationContext`); fica para portar ao fast e ao W |
| `--help` e o cabeçalho do `--dump-config` do fast se apresentavam como `go-webhook-server` | Leitura do `render.go` e revisão | Cosmético; corrigido aqui, fica para portar |
| Uma linha do dump que passa em `ParseHead` + `json.Valid` mas o `jsonb` recusa (byte `0xFF`, `\ud800`) fazia **todo** boot sair com 3 | Revisão independente, contra o PostgreSQL 18 real | Nenhum outro evento do arquivo nem dos seguintes era entregue, e a porta pública nunca abria. O leitor passou a exigir UTF-8 e só os escapes do serializador, e a reingestão refaz item a item um lote recusado como `logic` |
| O cancelamento do boot não chegava à validação depois do prazo da checagem (`AfterFunc` dispara uma vez) | Revisão independente, reprodução isolada | `SIGTERM` durante a validação esperava até `sink.op_timeout`. A checagem passou a levar o contexto de vida (`withLifetime`) |
| No shutdown por FATAL, o **primeiro** `SIGTERM` contava como segundo e jogava o dump fora (herdado do fast) | Revisão independente, leitura | Os eventos do ring perdidos com `exit 130` justamente no caminho que existe para salvá-los. Só o segundo sinal aborta agora |
| Um aborto durante o dump não contava os eventos já recolhidos do ring | Revisão independente, leitura | `dropped{abort}` subcontava; agora a contagem é única entre aborto e dump (`atRisk`) |
| `SIGTERM` durante a reingestão saía com 3 | Revisão independente | Agora 130, como no `WaitReady` |
| `whs_emergency_files` zerado no fim da reingestão mesmo com arquivo ilegível no disco | Revisão independente | Os gauges passaram a ser recontados do disco |
| `SIGHUP` antes do `run` (config, identidade) e depois dele, até o `os.Exit`, ainda matava o processo | Revisão independente | O `main` ignora o sinal antes de tudo e o `run` o devolve ignorado |
| Variáveis `WHS_*` de recurso retirado eram ignoradas, mas a doc dizia `exit 1` | Revisão independente, com o binário | Decidido manter o comportamento (o ambiente do fast precisa subir) e documentado na SPEC §0 |

### Porte das correções do `go-webhook-nats-server` (2026-10-11)

A revisão independente do N (que nasceu deste módulo) achou seis defeitos que
vieram daqui. Cada correção tem o teste abaixo e foi revertida numa cópia
(mutação) para provar que o teste a pega — todas morreram. A revisão
independente deste porte (somente leitura, mesmo dia) confirmou as seis e
achou dois buracos de teste (os gauges e o `WARN` do dump interrompido, agora
com teste), dois defeitos com perda ou segredo (o upload que termina depois do
`Drain` e os parsers que ecoavam valores; nas duas últimas linhas da tabela) e
uma frase errada que veio do N: o `WARN` e a SPEC diziam que os
eventos de um dump interrompido "foram contados como perda", o que só vale para
o aborto por segundo sinal — depois de um `SIGKILL` ninguém os contou, e o
arquivo pode estar inteiro (SPEC §9.1 diz agora como reingeri-lo).

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Recusa `logic` nos workers: `Release` antes do `ring.Commit` | O ring descontava `len(Env)` já zerado: cada lote recusado deixava os bytes contados para sempre, até o ring recusar tudo com `429` sem nada dentro | `TestWorkersDropOnLogicError` (bytes do ring) |
| Recusa fatal no boot logada como "did not become reachable within sink.boot_timeout" | O operador ia olhar a rede quando o esquema, o grant ou a credencial estavam errados | `TestFatalBootRefusalIsLoggedAsSuch` |
| Um `drain_timeout` por fase no shutdown (fases 3 e 4) | Upload lento + sink DOWN: dump só em 2× `drain_timeout`, depois do `stop_grace_period`; o `SIGKILL` levava o ring | `TestShutdownDrainTimeoutBoundsConnectionsAndRingTogether` |
| FIFO ou *symlink* com nome de dump era listado | Boot travado sem atender `SIGTERM`; leitura sem fim de `/dev/zero` | `TestListSkipsWhatIsNotARegularFile` |
| Dump escrito direto no nome final | Cortado numa fronteira de linha (`kill -9`, OOM), lia limpo e era removido depois de entregue em parte | `TestWriteGoesThroughATemporaryName` (o `link` só vê o temporário completo) |
| Sem *hard link* (vfat, exFAT, FUSE), o `link` falharia sempre | Todo dump falhando, o ring inteiro perdido | `TestWriteOnAFileSystemWithoutHardLinks` |
| Diretório que não abria para o `fsync` removia o dump pronto | Dump completo apagado por um `fsync` de diretório que é só garantia extra | `TestWriteKeepsTheDumpWhenTheDirectoryCannotBeOpened` |
| Temporário de dump cortado nunca era informado; o gêmeo de um dump entregue ficava | Um dump interrompido invisível ao operador | `TestLeftovers`, `TestBootRemovesTheTwinOfADumpAndKeepsAnInterruptedOne` (o `WARN` com o caminho) |
| O dump relia o diretório inteiro para os gauges `whs_emergency_*` | Pico de memória do tamanho do dump, com o ring ainda vivo, sob um `GOMEMLIMIT` de 2× o ring | `TestEmergencyGaugesCountTheDumpWithoutReReading` (diretório `0300`: uma releitura não acha nada; o boot ainda reconta do disco) |
| Um upload que terminava depois do prazo da fase 3 recebia `200` num ring já drenado (achado pela revisão do porte; o prazo único das fases 3 e 4 tirou a folga que antes o pegava) | O evento fora do destino e do dump, sem contagem, e o processo saindo com 0 | `TestUploadFinishingAfterTheDrainIsRefused` (o ring é selado antes do último `Drain`), `TestMemorySealIsABarrierForEnqueue` |
| Os parsers de INI e de flags ecoavam a linha e o argumento (o N já corrigira; achado de novo pela revisão do porte, com mais dois caminhos: a flag sem valor que engole a seguinte e o valor depois de um espaço) | `password: x` ou `--sink.workers --sink.postgres.password=x` no stderr | `TestParserErrorsDoNotEchoValues` |

Mutante equivalente, como no N: trocar o `context.WithDeadline` da fase 3 por
`WithTimeout(drain_timeout)` não muda nada, porque o prazo é calculado no mesmo
instante; o teste do prazo único mata a mutação da fase 4.

### Porte da revisão do porte de volta ao N (2026-10-11)

A revisão independente do porte de volta ao `go-webhook-nats-server` achou no N
três defeitos que vieram daqui; portados com o N como referência, cada um com
teste e mutação numa cópia do módulo:

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| O teste do `Seal` não provava a **ordem**: o gancho `afterDrain` rodava depois do `Drain`, e um `Seal` movido para entre os dois sobrevivia | A ordem, que é o ponto da correção, sem guarda | `TestUploadFinishingAfterTheDrainIsRefused`, agora com o ring do teste (`App.wrapRing`, `drainHookRing`) completando o upload **dentro** do `Drain` |
| O `TestMemorySealIsABarrierForEnqueue` com 300 rodadas pegava o `Enqueue` sem trava só cerca de metade das vezes sem `-race` | Uma regressão do `RWMutex` passando na suíte rápida | O mesmo teste com 3000 rodadas |
| Três caminhos de flag ainda imprimiam o segredo: `--sink.postgres.password --x` nomeava "a flag `--x`"; `--sink.postgres.password= -x` saía como parâmetro desconhecido; a cauda de um valor partido pelo shell (`--sink.postgres.password=$P`) saía como argumento solto | O segredo no stderr do contêiner | `TestParserErrorsDoNotEchoValues` (casos novos, e controles: `-1` é valor, `-h` é flag, um erro de digitação depois de um valor continua nomeado, e cada recusa diz o que houve) |

E um texto: o comentário do `Leftovers` ainda dizia que os eventos do dump
interrompido "foram contados como perda".

A mutação deste porte achou um caminho do `flags.go` sem teste, também no N:
tirar o `assigned(v)` do `--config` não derrubava nada. Caso novo,
`--config= x` (o argumento depois do `=` vazio não é citado).

### Porte das correções da família syslog/RELP (2026-10-13)

**A (ciclo de vida, dump, shutdown, boot).** Cada item com SPEC primeiro, o
teste abaixo e a correção revertida numa cópia do módulo (mutação): 34 de 34
mutantes mortos. Os testes são os do `go-webhook-fast-server`
(`port_lifecycle_test.go`, `durable_test.go`), adaptados ao dump JSON Lines e
ao destino falso (`memSink`); o código comum é igual entre o P e o N.

| Item | Teste |
|---|---|
| `fsync` do diretório no `Remove` e na quarentena; `ErrNotDurable` (inclusive na abertura do diretório) vira `WARN`, não o "cannot remove" falso; a escrita do dump continua melhor esforço (SPEC §9.1, §9.2) | `TestRemoveSyncsTheDirectory`, `TestRemoveWithAnUnopenableDirectoryIsNotDurable`, `TestQuarantineWithAFailedDirectoryFsyncIsQuarantined`, `TestWriteKeepsTheDumpWhenTheDirectoryFsyncFails`, `TestReingestedDumpRemovedWithoutDurabilityIsAWarning`, `TestQuarantineWithoutDurabilityIsStillReported` |
| Posse única da perda do aborto (`lossMu`, `enterDumpPhase`, `oweDump`, `settleDump`), no lugar do par `atRisk`/`collected`: o aborto entre o `Drain` e o registro do recolhimento contava a menos (2 de 5) (SPEC §10.3; fecha §8 item 16, a segunda metade) | `TestAbortBetweenTheDrainAndTheDumpCountsEverything`, `TestAbortAfterASavedDumpCountsNothing`, `TestAbortAndDisabledDumpCountOnce`, `TestAbortAndFailedDumpCountOnce`, `TestAbortDuringTheDrainIsCountedOnce`, `TestAbortBeforeTheDumpPhaseKeepsItClosed`, `TestNewlineEnvelopeLeftOutOfTheDumpIsCountedOnce` (o envelope com `0x0A`, agora contado pelo dump) |
| O dump que o aborto contou desiste (`Store.SetAbandon`), e o que publica depois da última conferência não é contado de novo (fecha §8 item 16, a primeira metade) | `TestWriteGivesUpWhenAbandoned` (antes da primeira linha, a cada 1024, antes do `link`), `TestDumpAfterTheAbortIsAbandoned`, `TestAbortAfterTheLastAbandonCheckCountsNothingMore`, `TestAbortDuringTheDumpCountsItOnceAndLeavesNoDump` (ponta a ponta: `beforeDump` segura o dump até o aborto contar) |
| X2: o dump toma o contador sob `lossMu`, nos dois ramos (com dump e `emergency.enabled=false`) | `TestDumpThatSawTheAbortLeavesTheCountToIt` (`inAbort`: o dump roda com a trava do aborto na mão) |
| `SIGQUIT` nunca tolerado, e o primeiro `SIGTERM`/`SIGINT` do shutdown por FATAL tolerado (já valia; agora com teste de unidade em `shutdownWith`) | `TestFatalShutdownToleratesTheFirstSignal` |
| O canal do aborto registrado antes de o contexto soltar `SIGTERM`/`SIGINT` (um segundo sinal no meio saía 143 sem contar) | sem teste: a janela é de instruções, entre o `signal.Stop` do contexto e o `signal.Notify`; provado por leitura |
| X1: `bind` cancelado por sinal = 130, na porta admin e na pública, com nome de host e com IP literal; o `Server.Close` fecha também os listeners que o `Serve` nunca recebeu | `TestSignalBeforeTheBindExits130` (`127.0.0.1:0` e `localhost:0`, a porta solta depois), `TestCancelledAdminBindIsAnAbortNotAConfigError` |
| Um sink FATAL durante o dreno do `SIGTERM` encerra a espera, e a saída é 3 sempre que a saúde termina FATAL (SPEC §10.3; fecha SPEC §19.1 item 10, já registrado) | `TestFatalDuringTheDrainEndsTheWaitAndExits3` (prazo de 10 min; o dump sai na hora), `TestFatalDuringThePreStopDelayExits3` (ring vazio: saía 0) |
| Leitura dos dumps em todo boot, com `emergency.enabled` desligado também, e o diretório que não se lista só para uma vida que escreve dumps (fecha SPEC §19.1 item 8) | `TestDumpsAreReingestedWithEmergencyDisabled`, `TestUnlistableEmergencyDirStopsOnlyALifeThatDumps` |
| *Gauges* de emergência do arquivo inteiro e do disco: `whs_emergency_files` posto no início da reingestão, o `Count` contando o arquivo ilegível, falha da recontagem com `WARN` (fecha SPEC §19.1 item 12 e TESTING §8 item 17) | `TestEmergencyFilesGaugeDuringReingestion`, `TestCountKeepsAnUnreadableFile`, `TestEmergencyGaugesCountAnUnreadableDump` (o `WARN` sem teste: só log) |
| `withLifetime` e `validationContext` com `sink.op_timeout` (SP2/RP): já estavam aqui; o guarda do fast veio junto | `TestHealthChecksCarryTheirLifetime` (`internal/sink`) |

Não se aplicam (DECISOES, "Porte das correções da família syslog/RELP"): o
`quitNow` (sem observador de `SIGQUIT` fora do shutdown), o prazo da
reingestão sem progresso (sem retentativa nem prazo único; a leitura é do
arquivo inteiro, sem "pedaço"), o fechamento em paralelo (o `Server.Shutdown`
já fecha os listeners juntos) e o A4 do plano (não há `ring=sqlite`).

**B (configuração, `internal/config/`).** Testes em
`internal/config/family_port_test.go`; cada um falhou contra o código de antes
(conferido no módulo intocado; `TestFamilyPortRetiredKeyAfterAValueIsNamed` é
guarda da regra nova) e cada correção revertida sozinha derruba ao menos um
(31 mutantes, 31 mortos).

| Item | Teste |
|---|---|
| Cauda depois de um valor nomeada só dentro de uma seção conhecida (`knownSectionOf`; fecha §8 item 18), contando as seções do fast que este servidor não tem (`retiredSections`) | `TestFamilyPortTailAfterAValueIsNotNamed`, `TestFamilyPortRetiredKeyAfterAValueIsNamed`; controles de `TestParserErrorsDoNotEchoValues` ajustados |
| `-h`/`-v` depois de uma flag que espera valor não é nomeado como *switch* | `TestFamilyPortShortDashValueIsNotNamedAsAFlag` |
| Valor colado ao nome, `:` no lugar do `=`, na flag e no INI; chave antes de seção | `TestFamilyPortGluedValueIsNotPrinted` |
| Continuação de segredo numa linha do INI | `TestFamilyPortINISecretContinuationIsNotPrinted` |
| *Switches* com valor; `--config=` vazio; BOM na linha 1 | `TestFamilyPortSwitchesTakeNoValue`, `TestFamilyPortEmptyConfigFlagIsRefused`, `TestFamilyPortINIByteOrderMarkIsIgnored` |
| Derivados (`emergency.dir`, `sink.postgres.pool_size`) comentados no `--dump-config` | `TestFamilyPortDumpCommentsDerivedValues` |
| Tetos (`capacity_items`, `batch`, `workers`, `pool_size`, `listeners`) | `TestFamilyPortConfigCeilings` |
| `sink.postgres.addr` um servidor só | `TestFamilyPortPostgresAddrNamesOneServer` |

Não se aplica (`DECISOES.md`): `sink.batch_max_bytes` (a chave não existe),
`ring.max_inflight` e `ring.sqlite.commit_batch` (não existem), as regras de
URL PostgreSQL da `LOGHUB_EB_URL` (não há `LOGHUB_EB_URL` nem outro leitor de
URL/DSN), e o socket com `tls=true`, que já era recusado.

**C (sink NATS).** Não se aplica: este servidor não tem NATS (SPEC §8.7,
retirado; nenhuma dependência do `nats.go` no `go.mod`). Os achados da área —
`10077` de stream cheio ou fechando, `discard=new` abaixo de um registro,
negação mascarada em `core`, veredito do TLS na reconexão, *flush* `core`
através de uma reconexão, folga do `MaxPending` e teto de
`ring.max_inflight`, `retention: interest`, `10158`/`10167`/429, `_INBOX`
negado, `space` estrangeiro e as URLs NATS — ficam para o F, o W e o N.

**D (o destino PostgreSQL e o dreno).** Cada correção tem o teste abaixo,
contra o PostgreSQL 18 real, e foi revertida numa cópia (mutação): 13 de 13
mutantes morreram. `23502` e `0A000` fatais, `RULE` de `INSERT`/`UPDATE`
recusada no boot: `TestClassifySQLState`, `TestPostgresNotNullViolationIsFatal`,
`TestPostgresRulesAreRefusedAtBoot`. O check de saúde com conexão própria:
`TestPostgresHealthHasItsOwnConnection` (`pool = escritas` em 1 e 3, presas
num lock) e `TestPostgresHealthRedialsADeadConnection`. A regra do pool:
`TestPostgresWorkersAndPool` (abaixo de `sink.workers` avisa, igual não). O
prazo próprio da validação já estava no código e nos testes de unidade; veio
do `go-webhook-fast-server` a prova contra o servidor real,
`TestPostgresValidationHasItsOwnDeadline` (por um *proxy* que atrasa cada
pedaço; o aquecimento abre também a conexão do check, ou o *handshake* dela
consumia o prazo da checagem), com `TestHealthChecksCarryTheirLifetime`. O
dreno já tratava a recusa `logic` antes do contexto;
`TestWorkersLogicRefusalDuringShutdownIsLoggedAndCounted` a guarda. E o *flake*
de `TestPostgresWrongPasswordIsFatal` (`53300` antes de conferir a senha, com as
vagas do servidor esgotadas por outras suítes): o teste repete enquanto essa é
a resposta, como no RP.

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
