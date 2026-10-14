# go-webhook-fast-server — Procedimentos de teste

Companheiro operacional de `SPEC.md` §15. As suítes vieram do
`go-webhook-server` em 2026-10-06 e foram adaptadas a `type=webhook`,
`mode=fast` e ao `stage` da resposta; o histórico de §9 é o dele. Este documento diz **como rodar**,
**o que cada suíte prova** e **o que ainda não é provado**.

---

## 1. Suíte rápida (todo commit)

```bash
cd go-webhook-fast-server
gofmt -l .            # deve ser vazio
go vet ./...
go test -race ./...
```

Tempo: ~45 s sem destinos, ~70 s com os cinco (2026-10-07, macOS). O `-race` **não é opcional**: três dos quatro bugs de perda de
dados encontrados até hoje vivem em caminhos concorrentes, e um deles
(liberação dupla de buffer, §12.3) só é detectável assim.

## 2. Cobertura real

Cobertura por pacote engana neste projeto: a suíte de integração em
`internal/app` exercita `server`, `ring` e `sink` sem que isso seja atribuído a
eles. Use `-coverpkg`:

```bash
go test -race -coverpkg=./internal/... -coverprofile=cov.out ./... && go tool cover -func=cov.out | tail -1
```

Estado atual (2026-09-15): **85,4%** sem servidor externo nenhum; **89,4%**
com `WHS_TEST_MYSQL_DSN` e `WHS_TEST_NATS_URL` (§5.3, §5.4); **92,0%** com os
quatro destinos reais de uma vez (`WHS_TEST_REDIS_ADDR`, `WHS_TEST_PG_DSN`,
`WHS_TEST_MYSQL_DSN`, `WHS_TEST_NATS_URL`). O número sem servidor caiu em
relação à medição anterior (87,9%) porque dois destinos novos entraram, e o
que se exercita deles sem servidor é só a lógica pura — classificação de erro,
divisão de lotes, forma da mensagem, parser de id, pool de argumentos; a
validação de esquema ou de stream, a escrita e o `Health()` ficam sem prova
até o servidor estar lá.

Por pacote, com os quatro destinos (cobertura do próprio pacote, sem
`-coverpkg`, para comparar com as medições anteriores):

| Pacote | Cobertura |
|---|---|
| `envelope` | 99,8% |
| `obs` | 98,1% |
| `clientip` | 94,0% |
| `config` | 90,0% |
| `ratelimit` | 88,5% |
| `sink` | 86,4% |
| `emergency` | 80,7% |
| `ring` | 79,7% |
| `app` | 78,0% |
| `server` | 64,0% |


## 3. Fuzzing (SPEC §15.2, obrigatório)

Cinco alvos, todos sobre entrada hostil:

```bash
go test ./internal/clientip/  -run=XXX -fuzz=FuzzResolve      -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=FuzzRead         -fuzztime=60s
go test ./internal/envelope/  -run=XXX -fuzz=^FuzzEnvelope$   -fuzztime=60s
go test ./internal/server/    -run=XXX -fuzz=FuzzServeHTTP    -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$  -fuzztime=60s
```

- **`FuzzResolve`** — o parser de `X-Forwarded-For`, o único código que processa
  entrada estruturada vinda da internet. Um bug ali é spoofing de IP. Já
  encontrou um bug real: zona IPv6 no peer fazendo `netip.Prefix.Contains`
  retornar false para todo `proxy.trusted`.
- **`FuzzRead`** — o decoder do arquivo de emergência, que lê disco **logo após
  um crash**. Um parser frouxo transforma arquivo corrompido em pânico no boot,
  exatamente quando o operador mais precisa do processo de volta. Desde
  2026-10-06 as sementes são dumps reais de JLH, e todo item lido precisa
  passar no `ParseHead`, ter `id = uuid` e voltar byte a byte.
- **`FuzzEnvelope`** — método, alvo (pelo `url.ParseRequestURI`, como o
  `net/http` entrega, `#` inclusive), cabeçalho, query e corpo arbitrários:
  todo envelope passa no contrato completo (dois schemas + regras semânticas,
  §3.1 abaixo) e o `ParseHead` o lê de volta. Rodadas de 2026-10-06: 90 s, 862
  mil execuções; depois da revisão independente, 60 s sem falha.
- **`FuzzServeHTTP`** — bytes arbitrários num socket real até o handler, com e
  sem proxy confiável: o servidor recusa, ou o envelope passa no contrato. É o
  caminho do parser do `net/http` e do `describe` (alvo absoluto, `#`,
  cabeçalhos de proxy) que o `FuzzEnvelope` não percorre. Fecha a conexão com
  RST (`SetLinger(0)`): com `TIME_WAIT`, milhares de execuções por segundo
  esgotam as portas efêmeras do macOS (`can't assign requested address`).
  2026-10-06: 60 s sem falha; a sabotagem da `url` (query crua de novo) cai na
  primeira semente.
- **`FuzzJSONBSafe`** — a troca do `\u0000` para a `loghub.inbox`.

Rode 10 minutos antes de qualquer release. O corpus fica em
`testdata/fuzz/` e cada falha encontrada vira semente permanente.

Última execução de 10 minutos por alvo: 2026-09-14 (`-parallel=3`, macOS),
depois das correções dessa data: `FuzzResolve` 64,6 milhões de execuções e
`FuzzRead` 1,2 milhão, sem falha e sem semente nova.

### 3.1 Contrato LogHub (SPEC §3, §15.1)

O oráculo é `internal/jlhcheck` (só teste): `jlh-envelope.schema.json` e
`jlh-webhook-fast.schema.json`, cópias das skills em `internal/jlhcheck/loghub/`,
validados com `santhosh-tekuri/jsonschema/v6` e `AssertFormat`, mais as regras
que o `validate.py` da skill confere: `event.iso` = `uts`+`sub` truncado, o
instante nível 3 do `uuid` = `event`, `content.length` = corpo decodificado,
`content.type` = o primeiro `content-type` listado, `url` no RFC 3986 sem
fragmento e, lida pelo `urlsplit`/`parse_qsl` do Python (portados em
`jlhcheck/url.go`, sem chamar o pacote testado), igual a `schema`, `host`,
`path` e `query`; nenhuma chave JSON repetida (um varredor de tokens: todo
decodificador guarda a última e esconde a perda); `src`/`dst` da família de
`version`; cabeçalhos ordenados e as nove chaves na ordem canônica. O próprio
oráculo tem testes com valores tirados do CPython (`jlhcheck_test.go`). **Todo** envelope que
um teste lê de volta — do handler, do ring, do dump e dos seis destinos — passa
por ele. `internal/envelope` prova ainda o leitor estrito do cabeçalho
(`ParseHead`, que recusa o envelope v1), a query de `parse_qsl` e a sua
linearidade, a pré-passada do corpo e o pior envelope contra `MaxEventBytes`.

## 4. Integração — as 21 combinações (SPEC §15.3)

```bash
go test -race -timeout 300s ./internal/app/
```

A suíte roda **o mesmo wiring do binário** (`app.RunContext`), não uma
aproximação montada pelo teste. Um teste que remonta a pilha não prova nada
sobre a pilha que sobe em produção.

| Teste | O que prova |
|---|---|
| `TestAllLocalCombinations` | Os seis pares `ring`×`sink` sem destino externo (os três com Redis estão em §5, com PostgreSQL em §5.2, com MySQL em §5.3, com NATS em §5.4 e com ClickHouse em §5.5); cada um aceita tráfego, reporta o modo em `whs_config_info` e encerra com 0; com `sink=sqlite`, todo evento aceito está no destino |
| `TestSQLiteRingSurvivesHardKill` | Zero perda através de restart: o backlog do `ring.db` é drenado antes da porta pública abrir |
| `TestRingFullReturns429` | Ring cheio é 429, **nunca** 200 seguido de descarte (invariante §17.2) |
| `TestRateLimitReturns429WithRetryAfter` | 429 sempre acompanhado de `Retry-After` |
| `TestHTTPSurface` | Todo método de `[A-Z]+` vira evento; `CONNECT` e método fora de `[A-Z]+` são 405; `OPTIONS *` é 400 (com controles); 413 e 414 nos tetos; corpo vazio aceito |
| `TestH2CPriorKnowledge` | HTTP/2 sem TLS na mesma porta, só com a stdlib |
| `TestAdminEndpoints` | `/healthz`, `/readyz`; e `/metrics` na porta **pública** é um evento comum, não um scrape |
| `TestHistogramInvariants` | `+Inf == _count` em toda série — se divergir, todo quantil fica errado em silêncio |
| `TestSameSQLiteFileRefusesToStart` | `ring.sqlite.path == sink.sqlite.path` recusa subir (§10.1) |
| `TestUnreachableSinkExitsWithoutOpeningPublicPort` | Sink inalcançável ⇒ a porta admin responde `/readyz` com o motivo durante a espera, `sink.boot_timeout` é esperado inteiro, `exit 3`, e a porta pública **nunca** abre (§10.1) |
| `TestReloadTracksRunningAndBootValues` | `SIGHUP`: chave recarregável revertida ao valor de boot é aplicada; chave de reinício é avisada em todo `SIGHUP` (§13.3) |
| `TestReloadReachesTheLiveLimiter` | O `SIGHUP` chega ao componente vivo: o limitador responde `429` depois de ligado pelo arquivo e volta a deixar passar depois de desligado (§13.3) |
| `TestSIGHUPNeverEndsTheProcess` | Um `SIGHUP` real, cada passo num processo filho (uma regressão mata o filho, não a suíte; o pai captura o sinal enquanto os cria, para que comecem com a ação padrão mesmo depois de outro teste ter deixado o sinal ignorado, e cada filho confere isso): durante o `Main` antes do `run`, durante o boot esperando o sink (`WARN`), servindo (recarrega), dentro do último `Drain` do shutdown com o dump a escrever (`WARN`, dump inteiro, saída 3) e depois do retorno do `run` — nenhum encerra o processo (§13.3) |
| `TestDumpsAreReingestedWhateverTheRingMode` | Um dump de uma vida anterior é reingerido e removido, e o temporário interrompido informado, com `ring=sqlite`, `ring=none` e `emergency.enabled=false` (§9) |
| `TestSinkNoneLeavesDumpsInPlace` | Com `sink=none`, em cada `ring.mode`, o dump fica intacto no disco, contado nos gauges, com um `WARN` dizendo por quê (§9.2) |
| `TestUnlistableEmergencyDirStopsOnlyALifeThatDumps` | Um `emergency.dir` que não se lista: `ERROR` e o boot segue com `ring=sqlite`; com `ring=memory` e a emergência ligada, saída 1 (§9.2) |
| `TestEmergencyGaugesCountWhatIsLeftAndAddTheDump` | Depois do boot, `whs_emergency_*` contam do disco o arquivo ilegível e o que não pôde ser removido; o dump do shutdown soma um arquivo e os seus eventos (§11.1) |
| `TestGracefulShutdownDrainsAFullRing` | `SIGTERM` com o ring carregado: dreno completo, `exit 0`, zero perdas |
| `TestSecondSignalExits130` | Segundo sinal corta o dreno e sai com 130, contando o abandonado em `dropped{abort}` (§10.3) |
| `TestAbortStopsTheGracefulPath` | Depois do aborto o caminho gracioso para: nada é despejado ao fim do `drain_timeout`, e cada evento perdido é contado uma vez (§10.3) |
| `TestMultipleListenersServeTheSameAddress` | `server.listeners=4` sobe e serve; o que é portável do `SO_REUSEPORT` |
| `TestTruncatedEmergencyDumpIsQuarantined` | Dump truncado: prefixo íntegro entregue, resto renomeado para `.corrupt` (§9.2) |
| `TestLegacyEmergencyDumpIsSetAside` | Dump de dois comprimentos (v1) vira `.legacy` byte a byte e não é reingerido; um dump atual no mesmo diretório é (§9.2) |
| `TestLegacyRingTableIsLeftUntouched` | Tabela `ring` da v1 com linhas fica intocada e nada dela é entregue; vazia, é removida (§7.4) |
| `TestNodeIDInINIRefusesToStart`, `TestInvalidIdentityRefusesToStart` | `server.node_id` num INI e uma identidade inválida saem com 1, com controles (§3.5) |

Em toda resposta a suíte confere §4.4: o `200` com `X-LogHub-UUID` UUIDv7 e o
corpo JSON com o mesmo `uuid` (`fast`/`accepted` e `stage` `ring` ou `sink` conforme o ring), as
recusas depois do `uuid` em JSON com `"status":"error"`, as de antes em
`text/plain`, e nenhum `X-Event-Id`. Todo envelope lido de um destino passa no
oráculo de §3.1, e o conjunto de `uuid` é o dos `X-LogHub-UUID` devolvidos,
nos dois sentidos (nada perdido, nada a mais).

Os três pares com `sink=redis` e os cenários que precisam derrubar o destino
estão em §5, e **pulam** quando não há Redis. Os três pares com `sink=postgres`
estão em §5.2, os três com `sink=mysql` em §5.3, os três com `sink=nats` em
§5.4 e os três com `sink=clickhouse` em §5.5; cada suíte pula quando o seu
servidor não está definido.

## 5. `sink=redis` — suíte automatizada

`sink=redis` é o destino **padrão** e exige um Redis de verdade. Os testes
pulam quando `WHS_TEST_REDIS_ADDR` não está definido, para que `go test ./...`
continue rodando num checkout limpo.

```bash
docker run --rm -d -p 16379:6379 --name whs-test-redis redis:7-alpine
WHS_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race -timeout 600s ./internal/app/
docker rm -f whs-test-redis
```

Um Redis real em vez de um fake é o ponto. As três propriedades que importam
aqui — o auto-pipelining do rueidis, o RESP carregando corpo binário cru sem
escaping, e o `OOM` chegando como resposta de erro de um servidor que ainda
responde `PING` — são exatamente as que um fake teria de inventar.

Cada teste usa um nome de stream próprio e o apaga no fim: a suíte nunca depende
de um servidor vazio e nunca precisa dar `FLUSH` num que talvez não seja dela.

A suíte confere a entrada de §3.3: **um** campo, `e`, com o JLH byte a byte
(`TestRedisEntryShape…`), o stream padrão `loghub.{space}.{type}` expandido
por envelope — um envelope reingerido de outro Workspace vai para o stream
dele —, o nome de conexão `whs/<agent>@<host>` no `CLIENT LIST`, a sonda de
saúde no stream do Workspace sem criar chave, `MAXLEN`, a reconstrução dos
comandos a cada lote (§8.2.3), `WRONGTYPE` e réplica somente leitura fatais,
senha errada fatal na discagem, destino caindo e voltando sem perda, o ciclo
FATAL ⇒ dump ⇒ reingestão, o Redis que sobe tarde, e as sobras do
`ring=sqlite` no shutdown. `TestRedisNULQueryAndBinaryBodyRoundTrip`: corpo
binário em base64 e `%00` na query voltam exatos.

Pelo mesmo proxy, o shutdown por FATAL e o boot interrompido (§9.2, §10.3):
`TestFirstSignalDuringAFatalShutdownKeepsTheDump` (o primeiro sinal num
shutdown por FATAL não aborta: o dump é escrito e a saída é 3),
`TestFailedDumpIsCountedOnceAsEmergencyFailed` e
`TestFatalSinkWithoutEmergencyCountsTheLossAsAbort` (cada evento perdido numa
razão só), `TestSignalDuringReingestionExits130` e
`TestSignalDuringTheBacklogDrainExits130` (o proxy segura os `XADD` do stream e
deixa passar a sonda, o boot é cancelado no meio da escrita: 130, e o dump ou
as linhas do `ring.db` ficam). `TestEmergencyFilesGaugeDuringReingestion`: com a escrita
segura do mesmo jeito, `whs_emergency_files` é o número de dumps listados
durante a reingestão (§11.1). `TestSQLiteRingLeftoversAreNotCountedAsLost`
confere também o `WARN` do `ring=sqlite`: as linhas que ficam, contadas uma vez
cada, com o lote retido pelo worker entre elas (§10.3).

A validação de boot dos destinos SQL tem prazo próprio (SPEC §8.5.5):
`TestPostgresValidationHasItsOwnDeadline`, `TestMySQLValidationHasItsOwnDeadline`
e `TestClickHouseValidationHasItsOwnDeadline` põem um proxy lento (30 ms por
pedaço) entre o sink e o servidor real e conferem que a validação passa do
prazo da checagem e termina, e que um boot cancelado depois desse prazo ainda
a interrompe; `TestValidationContext*` e `TestHealthChecksCarryTheirLifetime`
provam as duas metades sem servidor, e `TestPostgresConfigCarriesTheValidationDeadline`
a ligação com `sink.op_timeout`.

A política de despejo (SPEC §8.2.5) pede um **segundo** Redis, com
`--maxmemory-policy allkeys-lru`, em `WHS_TEST_REDIS_LRU_ADDR` (os testes que o
usam pulam sem ele; o Redis normal precisa ser `noeviction` ou `volatile-*`):

```bash
docker run --rm -d -p 16380:6379 --name whs-test-redis-lru redis:7-alpine \
  redis-server --maxmemory-policy allkeys-lru
WHS_TEST_REDIS_ADDR=127.0.0.1:16379 WHS_TEST_REDIS_LRU_ADDR=127.0.0.1:16380 \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

`TestRedisRefusesAnEvictingServer` (o `NewRedis` do boot é fatal com
`ErrEvictionPolicy`, e um sink que subiu num servidor `noeviction` e passa a
falar com o `allkeys-lru` é recusado na checagem seguinte, e na outra),
`TestRedisEvictingServerEndsTheBoot` (exit 3 na hora, com a linha de recusa
permanente), `TestRedisWarnsWhenThePolicyIsUnreadable` (um usuário ACL sem
`INFO` nem `CONFIG` sobe, com **um** `WARN` em três checagens),
`TestRedisAcceptsANonEvictingServer` e, sem servidor, `TestRedisEvictionVerdict`.
O prazo único do passo 3 (SPEC §10.1): `TestRedisBootHasOneDeadline` (o Redis
aparece aos 1,5 s de um `boot_timeout` de 2 s e a checagem nunca passa: o boot
termina perto dos 2 s, não 1,5 s + 2 s) e
`TestRedisBootClientLoopKeepsTheDeadline` (o backoff não dorme além do prazo).

Os cenários que precisam do destino caindo e voltando passam por um **proxy TCP
dentro do teste**, não por parar o Redis. Isso mantém a falha sob controle do
teste e deixa um Redis compartilhado utilizável por tudo que estiver rodando
contra ele.

### 5.1 Verificação manual

Ainda útil para inspecionar os bytes com o olho:

```bash
redis-server --port 16379 --save '' --appendonly no --daemonize yes
D=$(mktemp -d)
go build -o /tmp/gws ./cmd/go-webhook-fast-server
/tmp/gws --ring.mode=memory --sink.type=redis --sink.redis.addrs=127.0.0.1:16379 \
  --ring.data_dir=$D --server.bind=127.0.0.1:8080 --admin.bind=127.0.0.1:9101 &

printf 'A\x00B\nC\xffD' | curl -s -X POST http://127.0.0.1:8080/hook --data-binary @-
redis-cli -p 16379 --no-raw XRANGE loghub.default.webhook - + COUNT 1 | tail -1
```

Esperado: um campo `e` com o envelope JLH; como o corpo tem NUL e `0xff`, ele
está em `payload.content` com `"encoding":"base64"` (`QQBCCkP/RA==`).

## 5.2 `sink=postgres` — suíte automatizada (`loghub.inbox`)

```bash
docker run --rm -d -p 15432:5432 -e POSTGRES_USER=whs -e POSTGRES_PASSWORD=whs \
  -e POSTGRES_DB=whs_test --name whs-test-pg postgres:18-alpine
WHS_TEST_PG_DSN='postgres://whs:whs@127.0.0.1:15432/whs_test?sslmode=disable' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f whs-test-pg
```

PostgreSQL **18** (a DDL da skill usa `uuid_extract_version` e
`COMPRESSION lz4`). Cada teste cria a fila a partir de
`docker/postgres/inbox.sql` num esquema próprio e o apaga no fim. Prova:

- o `unnest` de dez arrays e o alinhamento comando × contrato
  (`TestPostgresInsertStatementMatchesTheContract`), o hash da DDL da skill
  (`TestPostgresInboxFileIsTheSkillDDL`), o vetor de `inboxEvent`;
- a linha reconstruída com o SQL da skill igual ao JLH como valor JSON (só
  `\u0000` → U+FFFD), válida no oráculo; o consumo `FOR UPDATE SKIP LOCKED`;
- `ON CONFLICT (uuid)` com reenvio e `uuid` repetido no mesmo lote, e o
  contador de duplicatas; escritores concorrentes e as métricas do pool;
- a validação do boot, 27 casos: tabela, tipos (`jsonb`, `timestamptz`,
  `uuid`, `sid` só `bigint`, `varchar` curto recusado), índice único exatamente
  sobre `(uuid)`, imediato e válido (`DEFERRABLE` recusado), coluna extra
  obrigatória, `received_at` sem default; mais tabela particionada e banco
  `LATIN1` recusados (`TestPostgresPartitionedTableIsRefused`,
  `TestPostgresNonUTF8DatabaseIsRefused`);
- o papel: sem `INSERT`, só com `INSERT` (o `ON CONFLICT (uuid)` exige
  `SELECT (uuid)`; o boot e a escrita concordam), sessão somente leitura, e
  `INSERT, SELECT (uuid)` como controle;
- tabela dropada = classe `42` fatal, senha errada fatal, envelope que o
  `ParseHead` recusa = `logic` antes de enviar, o pool livre do ambiente libpq,
  e um `23514` sem constraint (partição ausente) fatal.

## 5.3 `sink=mysql` — suíte automatizada (tabela `inbox`)

```bash
docker run --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=whs -e MYSQL_DATABASE=whs_test \
  --name whs-test-mysql mysql:8.4
WHS_TEST_MYSQL_DSN='root:whs@tcp(127.0.0.1:13306)/whs_test' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f whs-test-mysql
```

Cada teste cria a tabela a partir de `docker/mysql/inbox.sql` com nome
próprio. Prova: a linha reconstrói um envelope válido (`DATETIME(6)` UTC mais
`event_nsec` dá o `event.sub` exato; `received_at` em UTC); o `%00` da query
sobrevive na coluna `JSON`; reenvio e duplicatas; a divisão em comandos sob o
`max_allowed_packet`, conferido contra `MaxEventBytes`; o mapeamento de todo o
lote antes do primeiro comando (um envelope ruim não deixa pedaços gravados
contados como perdidos); a sonda de boot recusando falta de `UPDATE` ou de
`INSERT`, coluna extra `NOT NULL` sem default e uma CHECK que a linha da sonda
violaria; `3819`/`3140`/`3144` como `logic`. Desde a revisão de 2026-10-06,
`TestMySQLHealthValidatesTheSchema` recusa larguras menores que a DDL,
`DATETIME` sem `(6)` e índice único extra (aceitando `UNSIGNED`, tipos mais
largos e uma chave `AUTO_INCREMENT`); a sonda vai com cada valor no limite do
JLH (`TestMySQLProbeRowIsAtTheLimits`); `TestMySQLChunkedBatchIsAllOrNothing`
prova o lote de vários pedaços como uma transação; e um número desconhecido cai
na classe do SQLSTATE (`TestClassifyMySQLNumber`: `3151`, o `4025` do MariaDB).

## 5.4 `sink=nats` — suíte automatizada

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &       # sem JetStream
WHS_TEST_NATS_URL=nats://127.0.0.1:14222 WHS_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

O segundo servidor, **sem** JetStream, prova o FATAL imediato de SPEC §8.7.5;
sem `WHS_TEST_NATS_NOJS_URL` esses casos pulam. Em 2026-10-11 a suíte rodou
contra servidores com `max_payload: 8MB`; o 1 MiB padrão também serve (a suíte
da app baixa os tetos de corpo e cabeçalho, e a do sink lê o `max_payload`).

Desde o porte da família syslog/RELP (2026-10-13), quatro variáveis a mais,
cada uma pulando os seus casos sem ela (`review_port_nats_test.go`):
`WHS_TEST_NATS_PERM_URL` (JetStream com os usuários `admin`, `nopub` — publish
negado em `loghub.*.webhook` — e `noinbox` — subscribe negado em `_INBOX.>`; a
configuração está no comentário de `natsPermSetup`), `WHS_TEST_NATS_TLS_URL`
com `WHS_TEST_NATS_TLS_CA` (um listener com `tls {}` e o PEM do seu
certificado autoassinado, CN/SAN `127.0.0.1`) e `WHS_TEST_NATS_SERVER` (o
binário do `nats-server`, para os servidores que o próprio teste sobe, derruba
e sobe de novo na mesma porta — com outro certificado, em cluster).

Cada teste usa um Workspace e um stream próprios. Prova: subject
`loghub.<space>.webhook` expandido por envelope; headers
`Nats-Msg-Id = <uuid>.0`, `X-LogHub-*`, `Content-Type`, e nenhum `Whs-*`;
`Data` é o JLH byte a byte; a deduplicação pela janela; a validação de boot
(subject do Workspace no stream, selo, `max_payload` e `max_msg_size` contra
`MaxEventBytes`); stream esperado diferente e stream dropado fatais; todas as
mensagens do lote montadas antes da primeira publicação. Desde 2026-10-06: o
subject de um backlog de outro Workspace que o stream não cobre é fatal, com o
subject na mensagem (`TestNATSUncoveredBacklogSubjectIsFatal`), e o
`Nats-Expected-Stream` continua provado sobre o subject da instância
(`TestNATSExpectedStreamHeaderGuardsTheOwnSubject`). Desde 2026-10-11 (porte
das correções do `go-webhook-nats-server`, §9): a entrega parcial — um envelope
que não mapeia, uma mensagem acima do `max_payload` e uma acima do
`max_msg_size` recusados **sozinhos**, com `Delivered`; o contrato do stream em
toda checagem (subjects estreitados, `no_ack` ligado, um subject de outro
`space` retirado); `no_ack`, plano de controle e JetStream desligado fatais no
boot; a recusa `logic` que não deixa bytes contados no ring; e, na app, o log
"refused this server permanently" sem credencial nenhuma e um prazo só para as
fases 3 e 4 do shutdown.

## 5.5 `sink=clickhouse` — suíte automatizada (tabela `inbox`)

```bash
docker run --rm -d -p 19000:9000 -e CLICKHOUSE_USER=whs -e CLICKHOUSE_PASSWORD=whs \
  -e CLICKHOUSE_DB=whs_test -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --ulimit nofile=262144:262144 --name whs-test-clickhouse clickhouse/clickhouse-server:25.8
WHS_TEST_CLICKHOUSE_DSN='clickhouse://whs:whs@127.0.0.1:19000/whs_test' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f whs-test-clickhouse
```

Protocolo nativo (9000), ClickHouse **25.8** ou mais novo. O
`CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` deixa o usuário de teste criar
usuários: sem ele os subtestes de privilégio e de memória pulam. Cada teste
cria a tabela a partir de `docker/clickhouse/inbox.sql` com nome próprio.
Prova:

- a linha reconstrói um envelope válido no oráculo e igual ao JLH como valor
  JSON (lendo com `json_type_escape_dots_in_keys = 1` e
  `output_format_json_quote_64bit_integers = 0`): `params` e `query` vazios
  voltam como `{}` (o *hint* `Map` de §3.9), chaves com ponto, `%2E` literal,
  chave vazia e repetida voltam literais, o NUL do `%00` sobrevive, o corpo
  binário volta exato, `received_at` em UTC
  (`TestClickHouseWriteStoresTheEnvelope`); o maior envelope (corpo de 5 MiB em
  base64) entra num `INSERT` e volta byte a byte
  (`TestClickHouseLargestEnvelope`);
- o retry do mesmo lote é descartado pelo token e contado em
  `whs_sink_clickhouse_dedup_blocks_total`; um reagrupamento é gravado e o
  `FINAL`/merge o colapsa (`TestClickHouseRedeliveryIsDeduplicated`);
- envelope que o `ParseHead` recusa = `logic` antes de enviar; payload que não
  é JSON = `117`, `logic`; tabela dropada = `60`, fatal; servidor
  inalcançável = `network`;
- a validação do boot, 31 casos: 8 tabelas aceitas que escrevem depois
  (`String` em `space`/`type`, `DateTime64(9)` noutro fuso, `max_dynamic_paths`,
  `toStartOfHour` na chave, partição por hora com coluna de versão, sem
  `received_at`, coluna extra com default) e 23 recusadas, uma por regra —
  tabela ausente, `MergeTree`, chave sem `uuid`, cada tipo de coluna errado,
  `payload` sem os *hints*, com `SKIP` ou `String`, coluna faltando,
  `received_at` sem default, `CHECK` (nome nu e entre crases), `payload`
  `EPHEMERAL`, `sid` `MATERIALIZED`, `is_deleted`, partição por `received_at` e
  por `uuid`;
- nenhuma string é lida como outra coisa (`TestClickHouseStringsAreNeverInferred`:
  um cliente com datas e uma vítima com `""`, `"12345"`, `"1531420618"` no mesmo
  `INSERT`, conferido por mutação);
- o usuário: só `SELECT` ⇒ `497` nomeado; perfil `readonly` ⇒ fatal pelo
  `Health`; só `INSERT` escreve e a sonda vazia não deixa linha; senha errada
  fatal; `max_memory_usage` abaixo do pior lote ⇒ fatal com os números
  (`TestClickHouseMemoryLimitIsCheckedAtBoot`), e `max_memory_usage_for_user`
  abaixo de `pool_size` lotes (`TestClickHouseUserMemoryLimitCountsThePool`); um
  perfil que proíbe mudar `max_execution_time` ⇒ `452` fatal na sonda, sob o
  prazo do dreno (`TestClickHouseProbeCarriesTheDrainsDeadline`, por mutação);
- a tabela de §8.8.7 **lida da própria SPEC** (`TestClassifyClickHouseCodeMatchesTheSpec`);
- em `internal/app`: as três combinações, `ring=none` instrumentado, ida e
  volta de corpo binário e `%00`, chaves de query de cliente que não derrubam o
  lote, caminho com UTF-8 inválido, cinco tabelas recusadas com `exit 3`
  imediato e porta pública fechada, servidor inalcançável até
  `boot_timeout`, destino caindo e voltando sem perda (proxy TCP), e o ciclo
  FATAL ⇒ dump ⇒ reingestão com as colunas consultáveis.

## 6. Carga (SPEC §15.4)

**Dois perfis, sempre** — bots reais fazem os dois:

```bash
# keep-alive: mede o pipeline
bombardier -c 64 -n 200000 -m POST -f payload.bin http://host:8080/hook
# conexão nova por request: mede accept + handshake
bombardier -c 64 -n 50000 --no-keepalive -m POST -f payload.bin http://host:8080/hook
# h2c
h2load -n 100000 -c 64 -m 32 -d payload.bin http://host:8080/hook
```

O SLO sai de **dois contadores, sem interpolação**, porque as fronteiras estão
exatamente em 10 ms e 25 ms:

```promql
whs_request_duration_seconds_bucket{outcome="accepted",le="0.01"}
  / ignoring(le) whs_request_duration_seconds_count{outcome="accepted"}
```

Localize regressão por **subtração**, não por profiler:

| Configuração | Isola |
|---|---|
| `ring=none` + `sink=none` | teto de HTTP puro |
| `ring=memory` + `sink=none` | menos o anterior = custo do ring |
| `ring=memory` + `sink=redis` | menos o anterior = custo da entrega |

## 7. Critérios de aceite (SPEC §15.5) — **não aferidos**

Nenhum número de §15.5 foi medido em condições válidas. As medições feitas até
aqui são **loopback num MacBook Air M2 com `ab`**, que é single-threaded e
provavelmente é ele o gargalo. Servem como ordem de grandeza, não como aferição.

| Critério | Meta | Medido (inválido) |
|---|---|---|
| `ring=none`+`sink=none`, keep-alive | ≥ 120.000 req/s | ~145.000 (loopback, `ab`) |
| `ring=memory`+`sink=redis` | ≥ 50.000 req/s | não medido |
| p99 do 200, `ring=memory` | ≤ 10 ms | 100% < 2,5 ms (loopback) |
| `ring=memory`+`sink=sqlite` | ≥ 25.000 req/s | não medido |
| `ring=sqlite`, p99 (`synchronous=FULL`) | ≤ 25 ms | não medido |
| RSS estável por 30 min | < 512 MiB | não medido |

**Aferição válida exige**: nó Linux de 4 vCPU dedicado, carga de outra máquina,
`bombardier`/`h2load`, os dois perfis de conexão, e 30 minutos contínuos para o
RSS.

Teste de fumaça dos limites de memória dos compose (2026-09-14, Docker Desktop
no macOS, **não é a aferição**): `compose.memory-redis.yml` com o Redis pausado
e corpos de 64 KiB até o ring bater no teto de 256 MiB (`whs_ring_bytes`); RSS
máximo de 317 MiB contra `GOMEMLIMIT=512MiB` e limite de 768M no contêiner,
sem OOM, dreno em 3 s ao despausar. Os 12 compose sobem sem o aviso de
`GOMEMLIMIT` (§12.4). **Não vale para o código atual:** é anterior ao JLH com o
corpo dentro, e hoje um corpo de 64 KiB cai na classe de 256 KiB do pool, que o
ring conta pelo `len` e não pelo que segura (SPEC §19.1 item 1). Refazer depois
dessa correção.

Teste de fumaça dos compose novos (2026-09-15, Docker Desktop no macOS):
`compose.memory-nats.yml` e `compose.memory-mysql.yml` sobem com `--build`, o
`nats-init` cria o stream `WEBHOOKS` a partir de `nats/stream.json`, a imagem
do MySQL aplica `mysql/schema.sql` (hoje `mysql/inbox.sql`), `/readyz` responde `ready`, três webhooks
recebem `200` e aparecem no destino (3 mensagens no stream; 3 linhas na
tabela), `whs_events_dropped_total` zerado, e o único log de aviso do boot é
o par esperado sobre `ratelimit` e `admin.bind`. Os outros quatro compose
(`sqlite-*`, `none-*`) foram validados só com `docker compose config`.

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento:

1. **Sentinel e TLS do Redis** sem cobertura de nenhum tipo. A construção do
   `tls.Config` e a montagem da opção de Sentinel são exercitadas apenas pelo
   caminho de configuração; nenhum teste conecta a um Sentinel ou a um Redis
   com TLS.
2. **`kill -9` de verdade** não é testável em processo.
   `TestSQLiteRingSurvivesHardKill` usa parada graciosa e verifica a propriedade
   que importa (o `ring.db` é a fonte da verdade e o dreno de boot o consome). O
   `kill -9` real foi verificado à mão: 150 aceitos, 1 pendente no `ring.db`,
   150 no `events.db` após o restart.
3. **`SQLITE_FULL` (disco cheio)** não tem teste de ponta a ponta: simular disco
   cheio de forma portável entre Linux e macOS não é possível sem privilégios.
   A composição está coberta em duas metades — a classificação
   (`TestClassifySQLite`) e o efeito de uma escrita fatal
   (`TestInstrumentedEscalatesFatal`, `TestFatalSinkDumpsAndTheNextBootReingests`).
4. **`server.listeners > 1`** só é testado no que é portável: que os listeners
   sobem e que todo evento é servido. A semântica do `SO_REUSEPORT` no BSD
   difere da do Linux, então a distribuição de accepts que um teste no macOS
   mediria não é a de produção.
5. **`whs_conns_active{proto}`** não tem asserção própria; o gauge é mantido
   pelos hooks de `ConnState` e só é observado indiretamente.
6. **`server.max_header_bytes` tem folga deliberada.** O `net/http` recebe
   `max_header_bytes + 16 KiB` e o limite configurado é aplicado no handler
   (§4.3 passo 2), porque a stdlib responde o próprio limite escrevendo `431`
   direto no socket, antes de qualquer handler — invisível para
   `whs_requests_total` e `whs_rejected_total`. O custo é 16 KiB a mais de
   buffer de headers por conexão; a stdlib segue como guarda externa para o
   caso absurdo.
7. **A regra do `/data` (SPEC §13.6) não tem teste automatizado.** Nada na suíte
   prova que a imagem escreve só sob `/data`: o binário não sabe se está num
   contêiner e os testes rodam no host, onde o caminho certo é `./data`. Quem
   sustenta a regra em execução é o `read_only: true` dos compose. Verificação
   manual, com a stack de pé:

   ```bash
   S="-f compose.memory-sqlite.yml"
   docker compose $S exec webhook grep ' / ' /proc/mounts   # deve conter "ro,"
   docker compose $S exec webhook ls -l /proc/1/fd          # todo arquivo sob /data
   ```

   Se o rootfs aparecer `rw`, a regra voltou a ser convenção: uma escrita fora
   do volume passa a funcionar e a sumir no próximo `docker rm`, em silêncio.

8. **A ordem entre instâncias do UUIDv7 nível 3 não foi medida em produção.**
   A suíte prova que `uuid` e `event` são a mesma amostra (o oráculo confere em
   todo envelope), mas a janela de empate **entre instâncias** depende do
   sincronismo de relógio dos nós. Aferir junto com §15.5.

9. ~~O `id` e o `uts` vêm de leituras de relógio diferentes~~ — **fechada em
   2026-10-06**: `GenerateAt(Level3, t)` usa a mesma amostra do `event`.

10. **Nada verifica a resolução do relógio do host em produção.** No macOS os
   nanossegundos são zero (a skill avisa); um host de granularidade grossa
   gravaria microssegundos zerados também, sem sintoma.

11. **Empates dentro do mesmo nanossegundo não têm desempate** — propriedade
   do desenho (sem contador monotônico), não lacuna.

12. **TLS do PostgreSQL** está na mesma situação do Redis (item 1): a montagem
   do `tls.Config` com `ServerName` e CA é exercitada só pelo caminho de
   configuração e pelos casos de CA ausente ou inválida. Nenhum teste conecta a
   um PostgreSQL com TLS.

13. **`25006` em tempo de execução** (réplica promovida no meio da operação)
   não tem teste de ponta a ponta: exigiria promover uma réplica sob o teste. A
   composição está coberta em duas metades — a classificação
   (`TestClassifySQLState`) e o efeito de uma escrita fatal
   (`TestInstrumentedEscalatesFatal`, `TestWorkersMarkFatal`) — e a sessão
   somente leitura **no boot** é provada de verdade em
   `TestPostgresHealthValidatesTheRole`.

14. **`pg_is_in_recovery()` respondendo `true`** também não é provado contra um
   servidor real, pelo mesmo motivo. O que se prova é o caminho de recusa de
   conexão (`TestPostgresDownThenBackLosesNothing`), que percorre a mesma
   máquina de estados.

15. **A conta de conexões não foi aferida.** O padrão de 4 workers e o
   `pool_size = workers + 1` são um raciocínio (SPEC §8.5.3), não uma medição.
   O que a suíte prova é que oito escritores sobre cinco conexões entregam
   tudo e que o contador de esperas sobe; quantas instâncias cabem em `100`
   conexões com que vazão é assunto de §7, junto com §15.5.

16. **`sink=postgres` não tem meta de vazão** (SPEC §15.5) e a decisão de manter
   `INSERT ... unnest` em vez de `COPY` com *staging* depende dessa medição.
   Até lá, o caminho de escrita está provado como correto, não como rápido.

17. **O teto de um parâmetro do protocolo (1 GiB)** só é coberto pelo aviso de
   boot (`TestPostgresByteCeilingWarning`). Nenhum teste monta um lote que
   estoure de fato — seriam gigabytes de corpo num teste unitário.
   Desde 2026-10-14 o `sink.batch_max_bytes` (padrão 16 MiB, teto 256 MiB)
   corta o lote antes; o aviso só sobra para um envelope acima de 1 GiB.

18. **O `Host` com bytes fora de ASCII** nunca chega ao sink pela porta
   pública: a stdlib responde `400` antes de qualquer handler. O saneamento da
   coluna `host` é provado só no nível do sink; pela porta, só `path` e `query`.

19. **A corrida do `Dequeue` contra o contexto cancelado** é provada por
   repetição, não por construção. `TestWorkersNeverLoseAnAbandonedBatch` roda
   trinta rodadas porque o `select` do Go sorteia entre casos prontos: uma
   rodada só passaria por sorte cerca de metade das vezes. Antes da correção
   ele falhava na primeira rodada; ainda assim, um teste que depende de
   probabilidade não é uma prova de ausência. O que **é** estrutural é o
   `deliver` devolver se a posse mudou de mãos, e o `loop` parar quando não
   mudou.

20. **O envio tardio do rueidis após um cancelamento só foi reproduzido à mão.** A
   regra de `Discard`/`Taint` de §12.3 é provada pela decisão do handler e dos
   workers (`TestEnqueueErrorDecidesWhetherBuffersArePooled`,
   `TestWorkersTaintABatchAfterATimedOutAttempt`), não contra um Redis que
   trave no meio de um `XADD`: isso exigiria um servidor que pare de ler o
   socket durante o teste. O comportamento do cliente vem da documentação do
   rueidis ("the command is likely sent already") e da leitura do caminho de
   abort em `pipe.go`.

   Reprodução manual em 2026-09-14, `compose.none-redis.yml` com o Redis pausado
   (`docker pause`): 400 corpos únicos de 32 KiB, 96 `XADD` estourando
   `op_timeout` sob rotação do pool, todos respondidos com `503`; ao despausar,
   o Redis executou 2 desses comandos tardios e as 2 entradas chegaram byte a
   byte iguais ao corpo enviado (nenhum byte alheio). Continua sem teste
   automatizado.
21. **A chave de prova `<stream>:healthprobe` precisa estar na ACL.** Desde a
    sonda de escrita (2026-09-14), um usuário Redis restrito a `~<stream>` sem
    o sufixo `:healthprobe` recebe `NOPERM` na sonda e o processo sai com 3 no
    boot. A suíte com Redis real usa o usuário padrão e não cobre a ACL
    restrita.

22. ~~O heartbeat do `sink=sqlite` espera o mutex do escritor sem contexto~~ —
    **fechada em 2026-10-14** (SPEC §8.3.5, §19.1 (W 23)): a sonda espera a trava só
    no próprio prazo, e escritora ocupada com o último `Write` bom conta UP
    (`TestSQLiteProbeDoesNotWaitForTheWriterPastItsDeadline`).

23. **TLS do MySQL e do NATS** estão na situação dos itens 1 e 12: a montagem
    do `tls.Config` é exercitada pelo caminho de configuração e pelos casos de
    CA ausente ou inválida; nenhum teste conecta a um servidor com TLS. As
    formas de autenticação do NATS (`token`, `.creds`) tampouco: a suíte usa um
    servidor sem credenciais.

24. **`@@read_only = 1` e `1290` em tempo de execução** não têm teste contra um
    servidor real: `SET GLOBAL read_only` afetaria o servidor compartilhado da
    suíte. A composição está coberta em duas metades — a classificação
    (`TestClassifyMySQLNumber`) e o efeito de uma escrita fatal
    (`TestInstrumentedEscalatesFatal`, `TestWorkersMarkFatal`).

25. **O estado `CLOSED` do cliente NATS** (recusa de autenticação repetida) não
    é provado ponta a ponta, pelo mesmo motivo do item 23; a classificação
    fatal de `ErrConnectionClosed` e `ErrAuthorization` é unitária. A
    reconexão infinita e o buffer desligado são provados só pelo caminho feliz
    de `TestNATSDownThenBackLosesNothing`.

26. **A durabilidade do JetStream não é medida**: `sync_always`, réplicas e
    `sync_interval` são configuração do servidor (SPEC §8.7.4) e os compose
    sobem um nó só com file store. A suíte prova o `PubAck`, não o `fsync`.

27. **MariaDB não é suportado** desde 2026-10-06 (SPEC §8.6): o `JSON` dele é
    `LONGTEXT`, o boot recusa nomeando o servidor. A recusa é provada só por
    teste de unidade da mensagem (`TestMySQLMariaDBRefusal`); nenhum servidor
    MariaDB foi exercitado.

28. **A conta de conexões do MySQL** está na situação do item 15: o padrão de 4
    workers repete o raciocínio do postgres sem medição própria.

29. **TLS do ClickHouse** está na situação dos itens 1, 12 e 23: só o caminho
    de configuração e a CA inválida.

30. **`242` e `system.replicas.is_readonly`** não têm teste contra um
    `ReplicatedReplacingMergeTree` real: exigiria um Keeper e derrubá-lo no meio
    do teste. A classificação é unitária e o efeito de uma escrita fatal é o de
    sempre (`TestInstrumentedEscalatesFatal`).

31. **Os perfis de `async_insert`** (SPEC §8.8.6) não são exercitados: a suíte
    roda com o padrão do servidor (`async_insert = 0`). O significado do `200`
    com `wait_for_async_insert = 0` está documentado, não provado.

32. **O fator de memória do pior lote** (`128 MiB + 8 ×`, SPEC §8.8.5) vem de
    três medições no 25.8 (6,0× a 6,4× mais ~130 MiB). Outra versão do
    ClickHouse, outro formato de `payload` ou `max_dynamic_paths` diferente
    mudam a conta; a medição precisa ser refeita junto com §15.5.

33. **O merge do `ReplacingMergeTree` é eventual.** A suíte o força com
    `OPTIMIZE TABLE … FINAL`; em produção, até o merge, uma reentrega fora da
    janela do token aparece duas vezes para quem não lê com `FINAL`.

34. **`241` e `242` na escrita são fatais por decisão do usuário** (o
    precedente do `OOM` e do `25006`), mas a revisão independente mostrou que
    um `241` vem também do limite global do servidor (`OvercommitTracker`, uma
    consulta analítica pesada no mesmo ClickHouse) e um `242` de uma perda
    breve do Keeper: os dois derrubam a instância com 3. O boot só confere os
    limites da sessão (§8.8.5).

35. **Uma porta HTTP ou uma porta sem TLS** no lugar da nativa caem em
    `network` (falha de handshake) e esperam `sink.boot_timeout` em vez de sair
    com 3 na hora.

36. ~~**O aborto no meio de um dump** (SPEC §10.3) não tem teste determinístico~~
    — **fechada em 2026-10-13** (§9, "Porte das correções da família
    syslog/RELP"): os ganchos `beforeCollect`, `beforeDump` e `inAbort`
    seguram o shutdown em cada janela, e a contagem única é provada em cada uma
    (`TestAbortDuringTheDumpCountsItOnceAndLeavesNoDump` e os testes unitários
    do livro-caixa do aborto).

37. **`sink=redis` num *pipe* travado ignora o cancelamento.** Achado em
    2026-10-11 ao provar o prazo único das fases 3 e 4: com o Redis atrás de
    um proxy que segura o `XADD`, o `Write` do worker só voltou ~10 s depois
    de começar (o `ConnWriteTimeout` do `rueidis`), não no cancelamento do
    shutdown. O pior caso até o dump (SPEC §10.3) passa a ser
    `pre_stop_delay + drain_timeout + max(sink.op_timeout, 10 s)` com Redis.
    Não corrigido: é do cliente, e o prazo único foi provado com `sink=nats`.

38. ~~Uma recusa `logic` na reingestão ainda encerra o boot com 3~~ —
    **fechada em 2026-10-14** (SPEC §9.2, §10.1 passo 5): refeita item a item;
    no dump o recusado fica e o arquivo vai para `.corrupt`, no `ring.db` a
    linha é descartada e contada (`TestBootLogicRefusalInADumpIsQuarantinedNotFatal`,
    `TestBootLogicRefusalInRingDBIsDroppedAndCounted`).

39. **Permissão negada não tem teste contra um servidor real com
    permissões.** O veredito é provado injetando o erro assíncrono
    (`TestNATSPermissionViolationIsFatal`) e com um servidor falso que nunca
    publica nada depois dele; que o `nats.go` entregue o `Permissions
    Violation` ao `ErrorHandler` é do cliente, provado no N e no SN.

40. **Achados da revisão independente do porte (2026-10-11), anteriores a ele
    e não corrigidos** (fora da lista do N; cada um para uma rodada própria).
    Quatro dos nove foram corrigidos no mesmo dia, numa rodada própria (§9,
    "Achados anteriores ao porte"): o `SIGHUP` durante o dump, o dump parado
    quando a vida seguinte troca o ring, os gauges de emergência e o `WARN` do
    `ring=sqlite`. Seguem abertos:
    - ~~um segundo sinal no meio do dump conta os eventos como perda e o dump
      ainda pode ser ligado ao nome final antes do `os.Exit` (reingerido
      depois: perda contada a mais)~~ — corrigido em 2026-10-13: o dump que o
      aborto contou desiste (`emergency.Store.SetAbandon`); só o que publica
      entre a última conferência e o aborto fica contado e guardado (§9,
      "Porte das correções da família syslog/RELP");
    - com `sink=redis`, o boot pode levar até 2× `sink.boot_timeout` (o
      `buildSink` tem o seu e o `WaitReady` começa outro);
    - em NFS, um `link` que deu certo no servidor e voltou erro faz o dump
      contar como `emergency_failed` embora o arquivo exista (e seja
      reingerido); uma colisão real de nanossegundo no nome perde o dump em
      vez de tentar outro carimbo;
    - um `emergency.dir` dividido entre instâncias faz o boot de uma avisar o
      temporário em escrita de outra (só o `WARN`; nada é removido);
    - ~~no `ring=sqlite`, o *writer* que para não esvazia o canal de `deletes`
      (64): linhas confirmadas logo antes do `Close` voltam no boot seguinte,
      duplicatas reais com `sink=redis`~~ — corrigido em 2026-10-11 na rodada
      do item 42 (§9), onde o dreno de um `ring.db` mantido o fazia aparecer
      em metade dos boots com uma CPU.
41. ~~**Uma brecha deixada no parser de flags** (SPEC §13.2, como no N): a cauda
    de um valor que o shell partiu num espaço, quando começa com traço e vem
    depois de um valor não vazio (`--sink.nats.password=a -b`), sai como
    parâmetro desconhecido, com o nome. Escondê-la esconderia todo erro de
    digitação de uma flag depois de outra.~~ — **fechada em
    2026-10-13** (porte da família syslog/RELP, `knownSectionOf`): depois de um
    valor, só se nomeia uma flag desconhecida dentro de uma seção deste
    servidor; `TestFamilyPortTailAfterAValueIsNotNamed`.
42. ~~Um backlog do `ring.db` fica parado quando a vida seguinte sobe com
    `ring=memory` ou `ring=none`, e com `ring=sqlite` + `sink=none` o dreno do
    boot entrega ao descarte as linhas de uma vida com destino~~ — **fechada em
    2026-10-11** (§9, "O `ring.db` de uma vida anterior"): um boot com outro
    ring entrega o `ring.db` e o remove; com `sink=none` as linhas ficam, em
    qualquer ring (SPEC §7.4).

43. **As linhas que o `sink=none` deixa no `ring.db` não têm gauge.** Os dumps
    que ele deixa contam em `whs_emergency_*` (SPEC §11.1); as linhas de uma
    vida anterior que ele não entrega (SPEC §7.4) aparecem só no `WARN` do
    boot e no do shutdown (`of_a_previous_life`). Uma métrica nova seria uma
    decisão de cardinalidade e de nome; com `sink=none`, um modo de medição, o
    `WARN` foi julgado suficiente.

44. **Nada impede duas instâncias no mesmo `ring.data_dir`.** A SPEC §7.4 o
    declara fora do suportado (o diretório guarda também a identidade), mas
    não há trava: numa troca `sqlite` → `memory` com as duas vidas de pé no
    mesmo volume, a nova drena o `ring.db` vivo da antiga (entrega dupla) e o
    remove de baixo dela.

45. **O desconto do `Sweep` do rate-limit sob a trava do shard** (SPEC §6.3,
    2026-10-14) não tem teste determinístico próprio: o `Reset` novo é provado
    segurando a trava de um shard, e o atraso antigo do `Sweep` só dava excesso
    passageiro na contagem, que a guarda por carga não distingue.

46. **`Expect: 100-continue` com o corpo enviado sem esperar** (SPEC §4.3;
    revisão independente de 2026-10-14) não tem teste: o `net/http` fecha
    sem `closeWriteAndWait`, e um RST pode levar o `429`/`503`. Reproduzi-lo
    pede um cliente que mande o corpo antes do `100` e um *timing* de RST
    que o loopback não dá de forma estável; aceito e documentado.

47. **A cópia do `held` e a fatia do `Dequeue` fora do `ItemOverhead`** (SPEC
    §7.2; revisão independente de 2026-10-14): até `workers × batch × 16`
    bytes fora de `ring.max_bytes`, sem teste, por ser um limite documentado
    e não uma regra.

48. **O recusado no `.corrupt` fica fora dos *gauges*** (SPEC §9.2; revisão
    independente de 2026-10-14): só o `ERROR` com o `uuid` o registra;
    documentado, sem teste de métrica.

49. ~~`ring=sqlite` sem piso de `ring.max_bytes`~~ — **fechada em 2026-10-14**
    (SPEC §7.2, W §19.1 item 43): abaixo do maior evento é exit 1
    (`TestSQLiteRingMaxBytesCoversTheLargestEvent`).

50. **Uma linha do `ring.db` que falha sempre no `Scan` trava o dreno** (SPEC
    §7.4; revisão independente de 2026-10-14): documentado como aceito; o
    `TestSQLiteDequeueServesWhatItReadBeforeAFailure` monta o caso, mas nada
    prova o comportamento de longo prazo (workers em backoff, `429`, boot
    com 3).

51. **O dreno do `ring.db` no boot sai com 3 num erro transitório do `Dequeue`**
    (SPEC §10.1 passo 5; revisão independente de 2026-10-14): aceito (o boot
    seguinte retenta), sem teste.

52. **Redis Cluster**: a sonda confere a `maxmemory-policy` de um nó por
    checagem (SPEC §8.2.5); não há teste de cluster, e um nó isolado com
    `allkeys-*` só é pego quando sorteado.

53. **Proxy Redis que responde o `INFO` com tipo inesperado** vira erro de
    transporte e `exit 3` "not reachable" no boot, em vez do `WARN` de
    ilegível (SPEC §8.2.5); sem teste.

54. **A recusa `logic` conta como `Write` com erro na sonda do `sink=sqlite`**
    (SPEC §8.3.5, intencional): um ciclo DOWN/`503` é possível numa
    reingestão com registros recusados e o banco são; o comportamento está
    nos testes da sonda (`busy, last Write failed`), a decisão não.

55. **O *keep-alive* do `dialPostgres` é o padrão do Go (15 s)**, não os 5 min
    do pgx, e o ambiente `PG*` some do processo durante o parse (SPEC
    §8.5.2): documentados, sem teste próprio além do `libpq_env_test`.

## 9. Histórico: bugs que os testes encontraram

Registro do que cada tipo de teste pagou, para calibrar onde investir:

| Bug | Encontrado por | Consequência se não encontrado |
|---|---|---|
| Zona IPv6 no peer anulava `proxy.trusted` | `FuzzResolve`, em 3 segundos | Cliente link-local nunca casava com proxy confiável nem com isenções |
| Identidade perdida no `ring=sqlite` | Critério de aceite `kill -9` | **149 de 150 eventos descartados em silêncio** por colisão de chave primária |
| Lote retido evaporava no shutdown | Teste manual do caminho FATAL | 8 eventos com 200 sumiam sem dump de emergência |
| Deadlock do dreno em `workers=8 × batch=256` | Teste unitário de `ring` | Dreno travava na **configuração padrão**, com linhas por entregar |
| Shutdown abandonava trabalho entregável | `TestAllLocalCombinations` (então `TestAllNineCombinations`) | Regressão que eu mesmo introduzi ao corrigir o bug #3 |
| Liberação dupla de buffer não era atômica | `-race` em `internal/sink` | Corpo de um cliente dentro do evento de outro |
| Dump de `DumpINI` não era reparseável | Teste de round-trip em `config` | O dump prometia ser um arquivo de config válido e não era |
| `ring=none` não instrumentava nem escalava para FATAL | Auditoria contra §10.2 e §11.1 | Redis em `OOM` responde `PING`: os três modos `ring=none` ficariam em loop de `503` para sempre, com toda série `whs_sink_*` em zero |
| `Sampler` deixava passar duas linhas no início | `TestSampler`, intermitente (~3,5%) | Sentinela `last != 0` confundia "nunca emitiu" com "emitiu em t=0"; `time.Since` retorna 0 logo após `time.Now` com frequência mensurável |
| `sink.sqlite.keep_files` mentia no SIGHUP | Auditoria contra §13.3 | Declarada recarregável no registry, nunca aplicada ao sink: o log dizia "applied" e a retenção não mudava |
| `limitConn` não repassava `CloseWrite` | Investigação do 431 | Com `max_conns` configurado, o `net/http` não conseguia half-close e o cliente perdia o `431`/`400` por RST — a mesma falha que o dreno do §4.3 existe para evitar |
| `431` nunca era contabilizado | Auditoria contra §4.4 | A série ficava em zero para sempre: um flood de headers grandes era invisível nas métricas |
| Lote retido era **sobrescrito** no `held[id]` | `sink=postgres` em Docker: 157 eventos com 200, 156 no dump | **Perda silenciosa de eventos já respondidos com 200.** Um worker que abandona o lote no shutdown o mantém registrado; o `loop` chamava `Dequeue` de novo, e o primeiro `select` do `Dequeue` sorteia entre o contexto cancelado e um ring com backlog. Metade das vezes o canal ganha: o worker volta com um segundo lote e o `hold` o grava **por cima** do primeiro. Mesma família do bug de 2 linhas acima — o registro consertou aquele e deixou este buraco |
| Reingestão zerava o `ItemMeta` | `sink=postgres` em Docker: 156 linhas com `path` vazio |
| `ON DUPLICATE KEY UPDATE` exige `UPDATE`, não só `INSERT` | Sonda de boot do `sink=mysql` contra servidor real (`TestMySQLHealthValidatesThePrivilege`), na primeira execução | Um usuário criado "com INSERT e nada mais", como o desenho do postgres manda, passaria pelo catálogo e falharia no **primeiro lote** com `1142` — fatal em produção, depois de o boot ter dito que estava tudo certo |
| A retentativa interna do `PublishMsgAsync` relê `msg.Data` depois do retorno | Leitura do fonte da biblioteca (`jetstream/publish.go`) ao escolher as opções, não por teste | Com a opção padrão, um lote que recebesse "sem respondedores" seria republicado 250 ms depois pela biblioteca, lendo um buffer que o worker já podia ter devolvido ao pool — a classe de bug de §12.3 num lugar novo, sem teste que a pegue | Todo evento que voltasse por um dump de emergência ficava **inconsultável por SQL** — `path`, `host`, `method` vazios e `ts = 0` — com o `env` perfeito ao lado, o que torna a perda invisível. Valia igual para `sink=sqlite` desde sempre; ninguém tinha olhado as colunas depois de uma reingestão |
| `Enqueue` cancelado devolvia o buffer ao pool com a escritora do `ring=sqlite` e o pipeline do rueidis ainda lendo | Auditoria estática contra §12.3 (2026-09-14) | Corpo de um cliente dentro do evento de outro, ou um grupo inteiro de até 256 eventos recusado com `503`, sempre que um cliente desligasse durante o group commit; com `ring=none`, `XADD` com bytes alheios depois de um `op_timeout`. Corrigido em três partes: o `Enqueue` do SQLite espera o `COMMIT` mesmo com contexto cancelado, o `ring=none` escreve num contexto desligado do cliente e limitado por `op_timeout`, e os caminhos de erro descartam sem devolver (`Discard`/`Taint`, §12.3). Testes: `TestSQLiteEnqueueWaitsOutACancelledContext`, `TestEnqueueErrorDecidesWhetherBuffersArePooled`, `TestWorkersTaintABatchAfterATimedOutAttempt` |
| Só a primeira linha de `X-Forwarded-For` era lida | Auditoria estática contra §5.2 (2026-09-14) | Atrás de um proxy que **insere** uma linha nova (HAProxy `option forwardfor`, regras *add-header*), o cliente forjava `src-ip-addr` com um único header: bypass de `ratelimit.exempt`, envenenamento da chave de rate-limit e origem falsa na auditoria. As linhas passam a ser concatenadas na ordem de chegada antes da caminhada, e de `X-Real-IP` vale a última; `TestRepeatedForwardingHeaderLinesAreJoined`. Confirmado em 2026-09-14 contra um HAProxy 3.0 real com `option forwardfor`: chegam as duas linhas e o IP observado pelo proxy vence, com e sem cabeçalho forjado |
| `Health()` do `sink=sqlite` varria o arquivo inteiro a cada 2 s | Auditoria estática contra §8.3.5 (2026-09-14) | `PRAGMA quick_check(1)` lê todas as páginas sob a trava da escritora, e ainda era executado **duas vezes** por ciclo (o primeiro `Scan` num `int` falhava sempre). Com `rotate = none` o `events.db` cresce sem limite: um dia a sonda passa de `health_timeout`, o sink vai a DOWN, os fusíveis a FATAL e o boot seguinte falha igual, em laço. A sonda virou um UPSERT de uma linha na tabela `health` (custo constante, prova escrita) e o `quick_check` roda só no boot; `TestSQLiteSinkHealth`, `TestSQLiteSinkRefusesADamagedFileAtBoot` |
| `WRONGTYPE`, `NOPERM`, `NOAUTH` e `WRONGPASS` do Redis eram classe `logic` | Auditoria estática contra §8.2.4 e contra a spec do irmão (2026-09-14) | Uma chave de outro tipo ou uma ACL sem `XADD` fazia o processo descartar um lote a cada 5 ms, **depois do `200`**, para sempre, com o `PING` dizendo que estava tudo bem. Agora são fatais, como no syslog e na classe `42` do PostgreSQL: dump de emergência e `exit 3`; `TestClassifyRedisReply`, `TestRedisWrongTypeIsFatal` |
| Redis fora no boot encerrava o processo na primeira discagem | Auditoria estática contra §10.1 (2026-09-14) | `rueidis.NewClient` disca na construção e o `build()` rodava antes da porta admin: `exit 3` imediato, sem retentativa dentro de `sink.boot_timeout` e sem `/readyz` para diagnosticar — o oposto do que a spec e o comentário do código prometiam, e um crashloop rápido em todo compose em que o Redis sobe junto. A porta admin passou a abrir antes do sink e o `buildSink` retenta com backoff até `boot_timeout`, como o irmão já fazia; `TestUnreachableSinkExitsWithoutOpeningPublicPort`, `TestRedisStartingLateIsWaitedFor` |
| `max_conns` e `max_conns_per_ip` eram por listener | Auditoria estática contra §13.5 (2026-09-14) | Com `server.listeners = N` os dois tetos valiam N vezes e um IP ocupava sua cota em cada listener. Um `connLimiter` por servidor passou a envolver todos os listeners; `TestConnLimitsAreSharedAcrossListeners` |
| `SIGHUP` adotava o snapshot inteiro e esquecia as chaves de reinício | Auditoria estática contra §13.3 (2026-09-14) | O segundo `SIGHUP` dizia "unchanged" sobre `ring.capacity_items` alterado no arquivo, e uma senha trocada nunca aparecia no diff porque a comparação usava valores redigidos. A recarga passou a usar duas referências (recarregáveis contra o que roda, as demais contra o boot) e valores sem redação; `TestReloadTracksRunningAndBootValues` |
| Durações em `0` passavam pela validação | Auditoria estática (2026-09-14) | `sink.op_timeout = 0` fazia toda escrita falhar no ato e ser retentada para sempre; `shutdown.drain_timeout = 0` fazia todo desligamento sair com 3; outras eram trocadas em silêncio por um padrão que o `--dump-config` não mostrava. A validação passou a recusar zero onde ele não é "sem limite" (§13.5); `TestValidationRejects`, `TestLegitimateZeroDurationsAreAccepted` |
| `kind="timeout"` nunca subia e sobras do `ring=sqlite` contavam como perdidas | Auditoria estática contra §11.1 e §10.3 (2026-09-14) | Um `op_timeout` estourado era contado como `network`, e um desligamento com lotes retidos e `ring=sqlite` somava linhas ainda em disco a `whs_events_dropped_total{reason="abort"}` e saía com 3. `DeadlineExceeded` passou a contar em `timeout`, e com `ring=sqlite` as sobras ficam para o próximo boot sem contagem, sem dump e com saída 0; `TestWorkersTaintABatchAfterATimedOutAttempt`, `TestSQLiteRingLeftoversAreNotCountedAsLost` |
| A sonda do Redis era um `PING` | Revisão independente das correções (2026-09-14) | Uma réplica promovida por engano responde `PING` e recusa todo `XADD` com `READONLY`: o ring enchia até o `429` com `whs_sink_up` em 1 e nunca havia `exit 3`. A sonda passou a ser a do irmão — `XLEN` mais `XADD NOMKSTREAM` numa chave de prova — e uma resposta de classe fatal à sonda é FATAL na primeira checagem; `TestHealthFatalReplyIsFatalAtOnce`, `TestRedisReadOnlyReplicaIsFatalViaTheWriteProbe` |
| Senha errada do Redis esperava `sink.boot_timeout` inteiro | Revisão independente das correções (2026-09-14) | O rueidis autentica dentro do `NewClient` e o erro voltava embrulhado sem classificação, como falha de rede: 300 s de "retrying" para um `WRONGPASS`. O erro da discagem passa pela mesma classificação de uma resposta; `TestNewRedisClassifiesTheDialReply`, `TestRedisWrongPasswordIsFatalAtBoot` |
| `ring=none` nunca devolvia o buffer ao pool | Revisão independente das correções (2026-09-14) | A tabela de §12.3 prometia a devolução após o `Write` síncrono e o código deixava tudo para o GC. `None.Enqueue` devolve no sucesso e continua deixando o erro para o handler; `TestNoneReleasesTheItemAfterASuccessfulWrite` |
| Dump falho contava a perda duas vezes; cancelamento do shutdown contava como erro de rede | Revisão independente das correções (2026-09-14) | `emergency_failed` e `abort` somavam os mesmos N; o `Canceled` do próprio worker no shutdown subia `kind="network"`, e uma sonda que estourava o prazo também. O `abort` só conta sem armazenamento de emergência, uma escrita cortada pelo contexto do worker não é falha do sink, e a sonda conta `timeout` como uma escrita; `TestHealthProbeTimeoutIsCountedApart` |

### Rodada LogHub de 2026-10-06

Encontrados na migração das suítes e na revisão independente do sink
equivalente do `go-syslog-server`; todos corrigidos, com teste que antes
pulava com `BUG:` ou falhava:

| Bug | Achado por | Consequência |
|---|---|---|
| `X-TLS-JA3`/`X-HTTP2-Fingerprint` cortados na última vírgula | `TestFingerprintsKeepTheirCommas` | `fp3 = "0"`: JA3 e a fingerprint HTTP/2 têm vírgulas próprias |
| `OPTIONS *` respondido pelo `net/http` (200 vazio) | `TestDescribeOnARealSocket` | O `400` de §4.2 nunca saía num socket real; agora `DisableGeneralOptionsHandler` |
| Porta de um host recusado chegava à `url` | `TestFallbackHostPortDoesNotReachTheURL` | `X-Forwarded-Host: a*b:9443` dava `dport` 9443 com o host trocado pelo `dst` |
| Qualquer cabeçalho de dois campos era "legado" | `TestTwoNonLengthsAreNotLegacy` | Dano renomeado `.legacy` com um ERROR mandando reingerir na versão anterior |
| Papel só com `INSERT` passava no boot | `TestPostgresHealthValidatesTheRole` | `ON CONFLICT (uuid)` exige `SELECT (uuid)`: o primeiro lote falhava com `42501` |
| NATS e MySQL publicavam parte do lote antes de recusar um envelope | `Test{NATS,MySQL}RefusesAnEnvelopeItDidNotWrite` | Eventos entregues contados como perdidos em `sink_logic` |
| `sid` estreito, `varchar` curto, índice `DEFERRABLE`, tabela particionada, banco `LATIN1` aceitos no boot | Revisão do sink do `go-syslog-server` | Toda escrita recusada (ou dupla codificação silenciosa) com o sink UP |
| `AppendQuery` quadrático | Auto-revisão | Milhares de pares distintos numa URI de 8 KiB: CPU por requisição amplificada |

### Revisão independente dos webhooks (2026-10-06)

Dois revisores só leitura (envelope/HTTP; destinos, montagem e docs), com o
`validate.py` da skill reproduzido e servidores reais. Tudo corrigido em W e
portado ao F, com teste que falhava antes:

| Bug | Achado por | Consequência |
|---|---|---|
| Chaves da query com bytes inválidos diferentes viravam a mesma chave JSON | `validate.py` com detector de duplicata | `?%FF=1&%FE=2`: `jsonb`, MySQL e todo parser guardavam só o último valor, sem erro |
| `url` × `query` divergentes (`#`, UTF-8 truncado, byte cru ao lado de escape) e `url` fora do RFC 3986 | `urlsplit`/`parse_qsl` contra a `url` | Envelope aceito com `200` que a skill recusa; `#` virava fragmento |
| `jlhcheck` mais fraco que o `validate.py` | Revisão | Passava em todos os casos acima; agora porta o `urlsplit`/`parse_qsl`, acha chave duplicada e tem testes com valores do CPython |
| Proxy confiável sem `X-Forwarded-For`: porta do socket na `url` | Requisição real com `X-Forwarded-Proto: https` | `https://pub.example:8080/` para um cliente que usou a 443 |
| `strings.ToLower` Unicode no host | Alvo absoluto com U+212A | `\u212Aexample.com` virava `kexample.com` |
| `ring=sqlite` nunca devolvia o JLH ao pool | Leitura contra §12.3 | Toda requisição alocava um buffer novo |
| `ParseHead` aceitava `uuid` maiúsculo e `from` de qualquer versão; dump com payload `{]` | Fuzz ad hoc e leitura | Registro danificado reingerido como evento |
| `readBody` dobrava o buffer no EOF do HTTP/2 | Leitura | 10 MiB a mais por corpo de 5 MiB |
| MySQL: `varchar` curto, inteiro estreito, `DATETIME` sem `(6)` e índice único extra aceitos no boot | Tabelas reais com um `ALTER` cada | `1406`/`1264` em todo lote com o sink UP; microssegundo arredondado; com `UNIQUE (from_uuid, sid)`, 1 de 3 eventos gravado e o `Write` devolvendo sucesso |
| MySQL: número desconhecido era `network` | `3151` (chave JSON > 64 KiB) | Lote retentado para sempre com o sink UP |
| MySQL: lote de vários pedaços commitado pedaço a pedaço | Leitura | Lote contado como perdido com parte gravada |
| MariaDB "suportado", mas o boot exigia `json` | Documentação do MariaDB | Toda subida no MariaDB saía com 3 e uma mensagem enganosa |
| NATS: envelope de outro Workspace num subject sem stream | Publicação real | `no response from stream` como `network`; boot em laço de reinício sem nomear a causa |
| PostgreSQL: `timestamptz(6)` recusado com a mensagem errada; índice único ou exclusão extra aceitos | Tabelas reais | Boot recusado sem motivo; ou `23505` derrubando o lote a cada colisão |
| Compose lia `WORKSPACE` do shell | `docker compose config` com o `WORKSPACE` do Jenkins | O caminho do job virava o `space` e o ident saía com 111 |
| Testes de worker davam `Start` antes de enfileirar | Execuções repetidas (500 × `-cpu 1,2,8`) | Lote parcial, falha intermitente; agora enfileiram antes |

### `sink=clickhouse` (2026-10-07)

Achados por experimento contra o servidor real **antes** do código, cada um
virou regra da SPEC e teste:

| Achado | Como | Consequência sem a regra |
|---|---|---|
| O tipo `JSON` descarta objeto vazio | `toString('{"params":{}}'::JSON)` | `params` (sempre `{}`) e `query` vazia sumiam de toda linha: nenhuma passava no oráculo. Virou o *hint* `Map` e, por decisão do usuário, uma tabela por tipo |
| Chave com ponto vira caminho | `?a=2&a.b=1` lido de volta | `{"a":"2","a":{"b":"1"}}`, JSON com chave repetida; com dois caminhos iguais, `117` e o lote inteiro como `logic` |
| O ClickHouse ordena `UUID` pela segunda metade | Documentação do tipo | `ORDER BY uuid` sem poda por tempo; a chave virou `(event_at, uuid)` |
| O resumo não revela a deduplicação | `X-ClickHouse-Summary` de um `INSERT` repetido | `written_rows` igual nas duas vezes; a métrica vem do evento `DuplicatedInsertedBlocks` |
| Um lote de `payload` JSON custa ~6× em memória no servidor | `system.query_log.memory_usage` | `241` é fatal: um lote legítimo grande derrubaria a frota; o boot confere o limite |
| O `.sql` do `initdb.d` roda no banco `default` | Entrypoint da imagem oficial | Tabela criada no banco errado e o webhook saindo com 3; o compose usa um `init.sh` com `--database` |

Revisão independente da mesma rodada (quatro revisores só leitura: núcleo,
fidelidade do `JSON`, config/docs/porte, adaptação do S), tudo corrigido com
teste que falhava antes:

| Achado | Consequência |
|---|---|
| A inferência de datas do tipo `JSON` lia strings como `Date`/`DateTime64` | `""` e `"1531420618"` de **outro** cliente do mesmo lote voltavam como datas; `"2020-02-30"` como `2020-03-01`. Perda silenciosa, sem erro |
| Coluna `EPHEMERAL` com o nome de uma das dez era aceita | o `payload` inteiro sumia com o `Write` devolvendo sucesso |
| `system.replicas` exige grant na imagem oficial | um usuário só com `INSERT` numa tabela `Replicated*` levava `497` fatal logo depois do boot |
| `max_memory_usage_for_user` comparado com um lote só | quatro lotes simultâneos legítimos davam `241` fatal |
| A sonda rodava sob o prazo do health, sem o `max_execution_time` do dreno | um perfil `CONST` passava no boot e recusava todo lote com `452`, retentado para sempre |
| A validação inteira cabia no `health_timeout` de 1 s | com 60 ms de RTT o boot nunca terminava |
| `ReplacingMergeTree(ver, is_deleted)` aceito | linhas marcadas sumiam no merge, ou todo lote recusado com `117` |
| Partição sem limite de tempo | `252` em lote de mais de 100 partições, retentado para sempre; partição por `received_at` nunca colapsava a reentrega |
| CHECK com nome entre crases escapava ao regex | todo lote descartado como `logic` |
| `MATERIALIZED` (`44`) caía em `network` | boot esperando 300 s em vez de sair com 3 |

### Porte das correções do `go-webhook-nats-server` (2026-10-11)

A revisão independente do N (2026-10-10) achou defeitos que este módulo também
tem: alguns herdados por ele via P, outros do sink NATS daqui, que nunca
recebeu as correções da revisão do `go-syslog-nats-server`. Cada correção tem
o teste abaixo, contra NATS 2.14.6 real (com e sem JetStream) e os demais
destinos reais, e foi revertida numa cópia (mutação): 30 de 32 mutantes
morreram; os dois que sobreviveram estão no fim.

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Recusa `logic` nos workers: `Release` antes do `ring.Commit` | O ring descontava `len(Env)` já zerado: cada lote recusado deixava os bytes contados para sempre, até o `429` com o ring vazio | `TestWorkersDropOnLogicError` (bytes do ring) |
| Workers sem entrega parcial | Um lote com uma mensagem recusada contava todas como perdidas, inclusive as que estavam no stream | `TestWorkersPartialDeliveryDropsOnlyTheRefused`, `TestInstrumentedCountsAPartialDelivery` |
| `sink=nats`: um envelope que não mapeia, ou uma mensagem que o cliente recusa (`ErrMaxPayload`) ou o stream recusa (`10054`), encerrava o lote | As anteriores no stream contadas como perdidas; as seguintes nunca publicadas e confirmadas — o `BUG` com que o `TestNATSRefusesAnEnvelopeItDidNotWrite` pulava | `TestNATSRefusesAnEnvelopeItDidNotWrite`, `TestNATSOversizedMessageIsRefusedAlone`, `TestNATSLogicRefusalReportsTheAckedMessages`, `TestNATSLogicRefusalFreesTheRingBytes` |
| Permissão negada só chegava pelo *callback* assíncrono e nunca virava FATAL | Publicação negada retentada para sempre com o sink UP; um log por mensagem | `TestNATSPermissionViolationIsFatal`, `TestNATSAsyncErrorsAreSampled` |
| O contrato do stream só era conferido no boot | Stream editado por baixo (subjects estreitados, `no_ack`): `network` para sempre com o sink UP | `TestNATSStreamEditedAtRuntimeIsFatal`, `TestNATSValidateRefusals` |
| `no_ack`, um stream que captura o plano de controle, o teto de 32 MiB do *file store* e `max_bytes` com `discard: new` não eram conferidos | `no_ack`: nada confirmado, nunca; `loghub.>`: todo beacon do ecossistema guardado; os tetos: `10077` fatal no primeiro corpo grande | `TestNATSValidateRefusals`, `TestNATSHealthValidatesTheStream` |
| JetStream desligado era "sem respondedores", `network` | O boot esperava o `sink.boot_timeout` e saía com o motivo errado | `TestNATSJetStreamDisabledIsFatal`, `TestNATSFatalBootRefusalIsLoggedAsSuch`, `TestNATSValidateRefusals` (`streamErr`: fatal só antes da primeira validação) |
| "User Authentication Expired/Revoked" no `CONNECT` era `network` | O boot esperava o `sink.boot_timeout` | `TestClassifyNATS` |
| `ConnectedUrl()` nos logs de conexão e a URL crua no "sink ready" | Senha e token no `INFO` de todo boot e no `WARN` de toda reconexão | `TestNATSConnectLogsNoCredentials`, `TestNATSFatalBootRefusalIsLoggedAsSuch` |
| A URL não era conferida na validação, e o dump a mostrava inteira | Senha com `%`, `/` ou `#` em cada log até o `boot_timeout`; o erro do `net/url` cita um pedaço dela; *userinfo* junto de `username` ignorada em silêncio | `TestNATSURLsAreCheckedWithoutEcho`, `TestDumpRedactsTheUserinfoOfNATSURLs` |
| Recusa fatal no boot logada como "did not become reachable within sink.boot_timeout" | O operador ia olhar a rede quando o stream ou a credencial estavam errados | `TestNATSFatalBootRefusalIsLoggedAsSuch`, `TestRedisFatalBootRefusalIsLoggedAsSuch` (o Redis tinha uma frase só dele) |
| Um `drain_timeout` por fase no shutdown | Upload lento + sink sem resposta: dump só em 2× `drain_timeout`, depois do `stop_grace_period` | `TestNATSShutdownDrainTimeoutBoundsConnectionsAndRingTogether` |
| Dump escrito direto no nome final; sem *hard link* não haveria saída; `fsync` do diretório apagava o dump pronto | Cortado numa fronteira de registro, lia limpo e era removido entregue em parte | `TestWriteGoesThroughATemporaryName`, `TestWriteOnAFileSystemWithoutHardLinks`, `TestWriteKeepsTheDumpWhenTheDirectoryCannotBeOpened` |
| FIFO ou *symlink* com nome de dump era listado; temporário de dump cortado nunca informado | Boot travado sem atender `SIGTERM`; dump interrompido invisível | `TestListSkipsWhatIsNotARegularFile`, `TestLeftovers`, `TestBootRemovesTheTwinOfADumpAndKeepsAnInterruptedOne` (o `WARN` com o caminho) |

O `WARN` do dump interrompido e a SPEC herdaram do N uma frase errada: "os
eventos foram contados como perda" só vale para o aborto por segundo sinal;
depois de um `SIGKILL`, de um OOM ou de uma queda de energia ninguém os contou,
e o arquivo pode estar inteiro. A frase foi corrigida e a SPEC §9.1 diz como
reingerir o temporário (achado pela revisão independente do porte no P).

Mutantes que sobrevivem, de propósito: tirar `ErrAuthRevoked` e
`ErrAccountAuthExpired` da classificação é equivalente (o texto deles já casa
com o `isAuthRefusal`); tirar a contaminação de um lote recusado não tem efeito
observável (todos os futuros já resolveram; é defesa). E o prazo único das
fases 3 e 4 é provado com `sink=nats`, não com Redis: num *pipe* travado o
`rueidis` só devolve o `Write` cancelado depois do próprio `ConnWriteTimeout`
(10 s), e o shutdown esperou 9,8 s com `drain_timeout=2s` mesmo com a correção
(§8, item novo).

Revisão independente do porte (dois revisores somente-leitura, mesmo dia: sink
NATS e config; ciclo de vida e emergência). Corrigido com teste e mutação (cada
correção revertida derruba o teste):

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Um upload que terminava depois do prazo da fase 3 recebia `200` num ring já drenado; o prazo único das fases 3 e 4 tirou a folga que antes o pegava (o P e o N têm o mesmo) | O evento fora do destino e do dump, sem contagem, e o processo saindo com 0 | `TestUploadFinishingAfterTheDrainIsRefused` (o ring de memória é selado antes do último `Drain`), `TestMemorySealIsABarrierForEnqueue` |
| A sonda `AccountInfo` do JetStream desligado não distinguia nada (o `jetstream` traduz "sem respondedores" para "não habilitado" sozinho), e o pedido a `$JS.API.INFO`, negado a um papel mínimo, virava veredito de permissão permanente | Boot com o JetStream de volta saía com 3 culpando permissão | `TestNATSValidateRefusals` (`streamErr`), `TestNATSJetStreamDisabledIsFatal` |
| Um subject de outro `space` coberto no boot e retirado do stream depois de o backlog drenar era FATAL em toda checagem | O operador seguia o que o erro do `cover` mandava e derrubava instâncias saudáveis (dump, exit 3) | `TestNATSValidateRefusals`, `TestNATSStreamEditedAtRuntimeIsFatal` (agora: esquecido; uma publicação posterior nele é recusada nomeando-o) |
| Os parsers de INI e de flags ecoavam a linha e o argumento (o N e o SN já corrigiam; mais a flag sem valor que engole a seguinte e o valor depois de um espaço) | Senha no stderr no boot e a cada `SIGHUP` | `TestParserErrorsDoNotEchoValues` |
| O teto do bloco de headers não contava o nome do stream | Com um nome acima de ~230 caracteres, a maior mensagem passava no boot e era recusada (`logic`) na entrega | `TestNATSValidateRefusals` |
| O caso do plano de controle de `TestNATSHealthValidatesTheStream` criava um stream em `loghub._ctl.beacon` | Falso negativo num servidor dividido com outra suíte que tenha um (`10065`) | o caso pula quando o stream não pode ser criado; `TestNATSValidateRefusals` cobre a regra |

### Porte da revisão do porte de volta ao N (2026-10-11)

A revisão independente do porte de volta ao `go-webhook-nats-server` achou no N
três defeitos que vieram daqui (o N recebeu o teste do `Seal` e o parser de
flags deste porte); portados com o N como referência, cada um com teste e
mutação numa cópia do módulo:

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| O teste do `Seal` não provava a **ordem**: o gancho `afterDrain` rodava depois do `Drain`, e um `Seal` movido para entre os dois sobrevivia | A ordem, que é o ponto da correção, sem guarda | `TestUploadFinishingAfterTheDrainIsRefused`, agora com o ring do teste (`App.wrapRing`, só o de memória; `drainHookRing`) completando o upload **dentro** do `Drain` |
| O `TestMemorySealIsABarrierForEnqueue` com 300 rodadas pegava o `Enqueue` sem trava só cerca de metade das vezes sem `-race` | Uma regressão do `RWMutex` passando na suíte rápida | O mesmo teste com 3000 rodadas |
| Três caminhos de flag ainda imprimiam o segredo: `--sink.nats.password --x` nomeava "a flag `--x`"; `--sink.nats.password= -x` saía como parâmetro desconhecido; a cauda de um valor partido pelo shell (`--sink.nats.password=$P`) saía como argumento solto | O segredo no stderr do contêiner, no boot e a cada `SIGHUP` | `TestParserErrorsDoNotEchoValues` (casos novos, e controles: `-1` é valor, `-h` é flag, um erro de digitação depois de um valor continua nomeado, e cada recusa diz o que houve) |

E um texto: o comentário do `Leftovers` ainda dizia que os eventos do dump
interrompido "foram contados como perda".

A mutação deste porte achou um caminho do `flags.go` sem teste, também no N:
tirar o `assigned(v)` do `--config` não derrubava nada. Caso novo,
`--config= x` (o argumento depois do `=` vazio não é citado).

### Achados anteriores ao porte, corrigidos (2026-10-11)

Quatro dos achados que a revisão independente do porte registrou no item 40 de
§8, corrigidos numa rodada própria, iguais no `go-webhook-server` e no
`go-webhook-fast-server`. SPEC primeiro, cada correção com o teste abaixo e
revertida numa cópia do módulo: 16 de 16 mutantes morreram (os do `SIGHUP`
rodando depois de um teste que montou uma `App` no mesmo binário, a ordem da
suíte inteira).

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| O observador de `SIGHUP` só existia enquanto servia e saía com `signal.Stop`, que devolve o sinal à ação padrão do Go; no boot não havia nenhum | Um `SIGHUP` no meio do dump de emergência matava o processo: o ring inteiro, já respondido com `200`, perdido e contado em lugar nenhum; no boot, a instância morria antes de servir. Agora o `main` o ignora antes de tudo, o `run` o captura do início ao fim (recarrega só servindo, `WARN` fora disso) e sai com `signal.Ignore` | `TestSIGHUPNeverEndsTheProcess` |
| O leitor de dumps só existia junto do escritor (`ring=memory` com a emergência ligada) | Uma vida seguinte com `ring=sqlite`, `ring=none` ou `emergency.enabled=false` deixava o dump parado no disco, sem log, os eventos respondidos com `200` nunca entregues | `TestDumpsAreReingestedWhateverTheRingMode` (e `TestFatalSinkWithoutEmergencyCountsTheLossAsAbort` guarda que só a escrita continua desligada) |
| A reingestão zerava `whs_emergency_*` no fim, e o `Count` pulava o arquivo ilegível; o dump os punha em 1 e no tamanho dele | Um dump ilegível deixado no lugar sumia de todo alerta; um dump de shutdown escondia o que o boot tinha deixado | `TestEmergencyGaugesCountWhatIsLeftAndAddTheDump`, `TestCountKeepsAnUnreadableFile`, `TestEmergencyFilesGaugeDuringReingestion` |
| Com `ring=sqlite`, o `WARN` do shutdown somava o lote retido pelo worker às linhas que o ring ainda contava, e ele já estava entre elas (só sai da conta no `Commit`) | O operador lia o dobro do lote retido como linhas deixadas no `ring.db` | `TestSQLiteRingLeftoversAreNotCountedAsLost` |

A revisão independente da rodada (um revisor somente-leitura, com experimentos
numa cópia) achou, todos corrigidos com SPEC, teste e mutação salvo o último:

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| O teste do `SIGHUP` passava com o `signal.Ignore` do `Main` removido sempre que outro teste montara uma `App` antes (na suíte inteira, o `clickhouse_test.go`): a saída do `run` deixa o sinal ignorado, e um sinal ignorado continua ignorado depois do `exec`; o passo `main` também aceitava um `exit 1` da configuração, antes de qualquer sinal | A correção sem guarda na suíte que de fato roda | `TestSIGHUPNeverEndsTheProcess`: o pai captura o sinal ao criar os filhos (um sinal capturado volta à ação padrão no `exec`), cada filho confere `signal.Ignored` e o passo `main` confere que a identidade foi pedida |
| Ler em todo modo estendeu ao `ring=sqlite` e ao `ring=none` o que o `ring=memory` + `sink=none` já fazia: o dump "entregue" ao descarte e removido | Um teste de carga no mesmo volume depois de um incidente destruía eventos respondidos com `200` | `TestSinkNoneLeavesDumpsInPlace` (com `sink=none` nada é reingerido; SPEC §8.4, §9.2) |
| Um `emergency.dir` que não se lista passou a encerrar com 1 o boot de modos que nunca o olhavam | Uma vida sem dumps parada por um diretório que não é dela | `TestUnlistableEmergencyDirStopsOnlyALifeThatDumps` (`ERROR` e segue; 1 só para quem escreve dumps) |
| A SPEC §9.3 punha a perda com `emergency.enabled=false` em `emergency_failed`; o código e o teste contam `abort` | O operador procurando a perda na razão errada | texto (o `TestFatalSinkWithoutEmergencyCountsTheLossAsAbort` já provava o código) |
| Falha da recontagem dos gauges sem log; o `whs_emergency_files` do início da reingestão posto depois do estado de boot que a anuncia | Gauges velhos em silêncio; uma leitura logo após o estado podia ver 0 | `WARN` e a ordem trocada; sem teste próprio (só log, e uma janela de instruções) |

E um achado da mesma classe, corrigido na rodada seguinte (abaixo): §8 item 42
(o backlog do `ring.db` parado quando a vida seguinte troca o ring).

### O `ring.db` de uma vida anterior (2026-10-11)

O §8 item 42, numa rodada própria, igual no `go-webhook-server` e no
`go-webhook-fast-server`. Duas escolhas do usuário: um boot com outro ring
**entrega e remove** o `ring.db` (contra recusar o boot, só avisar, ou
entregar e deixar o arquivo); com `sink=none` as linhas de uma vida anterior
**ficam**, em qualquer ring — o espelho da regra do dump (SPEC §9.2). SPEC §7.4
primeiro, cada correção com o teste abaixo e revertida numa cópia do módulo
(mutação): 22 de 22 mutantes morreram em cada módulo, 16 da correção e 6 da
revisão abaixo. Um deles sobreviveu na primeira passada e mostrou um
comentário falso: sem a checagem de arquivo regular, o SQLite não bloqueia num
FIFO (responde na hora com "disk I/O error"); a checagem existe para o `ERROR`
dizer o motivo, e o teste agora o exige.

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Um boot com `ring=memory` ou `ring=none` nunca abria o `ring.db`; o `WARN` do shutdown da vida `sqlite` prometia as linhas "for the next boot" | Eventos respondidos com `200` parados no disco, sem uma linha de log, até uma vida voltar ao `ring=sqlite` | `TestLeftRingDBIsDeliveredAndRemoved` (entregues antes da porta pública abrir, os três arquivos removidos; o `ring.db` vazio também) |
| Com `ring=sqlite` + `sink=none`, o dreno do boot entregava ao descarte as linhas de uma vida com destino | O teste de carga no mesmo volume depois de um incidente destruía eventos respondidos com `200` | `TestSinkNoneLeavesTheRingDBRows` (nos três rings: as linhas ficam; no `sqlite`, fora da conta — um teto abaixo delas aceita —, somadas no `WARN` do shutdown; a vida com destino entrega exatamente elas), `TestSQLiteKeepBacklogSetsThePreviousRunAside` |
| Abrir o arquivo com o `NewSQLite` criaria um `ring.db` para descobrir que não há nenhum, e uma `jlh_ring` no `events.db` quando os dois caminhos coincidem sob outro ring | Um arquivo novo em todo boot; uma tabela estranha num destino | `TestProbeSQLiteCreatesNothing` (inclusive um FIFO, recusado com o motivo), `TestNotARingDBIsLeftAlone` (inclusive um arquivo vazio) |
| A remoção precisa deixar o banco por último | Um crash entre as remoções deixaria um WAL velho ao lado de um nome sem banco, que o SQLite poderia reaplicar no próximo | `TestLeftRingDBIsRemovedLast` (um `-wal` que não se remove para tudo antes do `ring.db`) |
| A tabela `ring` da v1 com linhas | O arquivo removido com ela, ou o `ERROR` dito duas vezes | `TestLeftRingDBWithLegacyRowsStays` |
| O dreno que não termina | — (o mesmo laço do backlog do `ring=sqlite`) | `TestLeftRingDBDrainThatDoesNotFinishKeepsTheRows` (130 no `SIGTERM`, 3 com o destino fora, as linhas ficam) |

A revisão independente da rodada (um revisor somente-leitura, com experimentos
numa cópia) achou, todos corrigidos com SPEC, teste e mutação:

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| A sonda chamava de `ring.db` todo banco com uma `jlh_ring`, e a remoção apagava o arquivo inteiro: o `events.db` com os dois caminhos iguais sob `ring=memory`, depois de uma vida `ring=sqlite` ter posto uma `jlh_ring` nele | O destino inteiro apagado de baixo do sink que o tinha aberto — os eventos da vida também, gravados num inode sem nome —, com saída 0 e só um `INFO` | `TestSharedRingFileIsDrainedNeverRemoved` (com e sem linhas: entregues, o arquivo fica), `TestProbeSQLiteCreatesNothing` (`Shared`) |
| O *writer* do `ring=sqlite` saía no `stop` sem aplicar os `DELETE` enfileirados (item 40); o dreno de boot faz `Commit` e `Close` em seguida | Com uma CPU, metade das vezes o último lote ficava no `ring.db` mantido (tabela v1) e voltava no boot seguinte, duplicata real com `sink=redis`; o mesmo no shutdown de toda vida `ring=sqlite` | `TestSQLiteCloseAppliesTheQueuedDeletes` (`GOMAXPROCS=1`, 30 rodadas) |
| O teste do `KeepBacklog` plantava `rowid_` 1..N: um cursor pela contagem em vez do máximo passava | Com linhas mais antigas já entregues, os workers liam e descartavam as guardadas | `TestSQLiteKeepBacklogSetsThePreviousRunAside` (as duas primeiras entregues antes: guardadas 3..5) |
| O teto 2 com 3 linhas guardadas deixava escapar um `429` (2 em 45 rodadas) | Teste intermitente | 10 guardadas, teto 5 |
| A SPEC dizia que com `sink=none` nada é removido, e o `ring.db` vazio é; "só um `kill -9` as deixa" esquecia o segundo sinal; o `WARN` do `sink=none` estava no passo 4 e sai no 5 | O operador lendo uma promessa que o código não cumpre | texto |
| O ramo do `ring.db` que a sonda lê e o `NewSQLite` não abre não tinha teste | Uma regressão no código de saída passaria | `TestLeftRingDBThatCannotBeOpenedIsReported` (arquivo protegido contra escrita, no *rollback journal*) |

E dois registros sem correção: §8 item 44 (nada impede duas instâncias no
mesmo `ring.data_dir`) e o comentário de `removeLeftRing`, que dizia que os
destinos absorvem uma reentrega pelo `uuid` — falso com `sink=redis`, e agora
moot: os `DELETE` chegam ao disco antes do `Close`.

### Porte das correções da família syslog/RELP (2026-10-13)

**A (ciclo de vida, dump, shutdown, boot).** Cada item com SPEC primeiro, o
teste abaixo e a correção revertida numa cópia do módulo (mutação): 28 de 28
mutantes mortos. O mesmo no `go-webhook-server`.

| Item | Teste |
|---|---|
| `fsync` do diretório no `Remove`, na quarentena e no `.legacy`; `ErrNotDurable` (inclusive na abertura do diretório) vira `WARN`, não o "cannot remove" falso; a escrita do dump continua melhor esforço (SPEC §9.1, §9.2) | `TestRemoveSyncsTheDirectory`, `TestRemoveWithAnUnopenableDirectoryIsNotDurable`, `TestRenameAsideWithAFailedDirectoryFsyncHappened`, `TestWriteKeepsTheDumpWhenTheDirectoryFsyncFails`, `TestReingestedDumpRemovedWithoutDurabilityIsAWarning`, `TestQuarantineWithoutDurabilityIsStillReported` |
| Posse única da perda do aborto (`lossMu`, `enterDumpPhase`, `oweDump`, `settleDump`), no lugar do par `atRisk`/`collected`: o aborto entre o `Drain` e o registro do recolhimento contava a menos (2 de 5) (SPEC §10.3; fecha §8 item 36) | `TestAbortBetweenTheDrainAndTheDumpCountsEverything`, `TestAbortAfterASavedDumpCountsNothing`, `TestAbortAndDisabledDumpCountOnce`, `TestAbortAndFailedDumpCountOnce`, `TestAbortDuringTheDrainIsCountedOnce`, `TestAbortBeforeTheDumpPhaseKeepsItClosed` |
| O dump que o aborto contou desiste (`Store.SetAbandon`), e o que publica depois da última conferência não é contado de novo (fecha o primeiro ponto do §8 item 40) | `TestWriteGivesUpWhenAbandoned` (antes do primeiro registro, a cada 1024, antes do `link`), `TestDumpAfterTheAbortIsAbandoned`, `TestAbortAfterTheLastAbandonCheckCountsNothingMore`, `TestAbortDuringTheDumpCountsItOnceAndLeavesNoDump` (ponta a ponta: `beforeDump` segura o dump até o aborto contar) |
| X2: o dump toma o contador sob `lossMu`, nos dois ramos (com dump e `emergency.enabled=false`) | `TestDumpThatSawTheAbortLeavesTheCountToIt` (`inAbort`: o dump roda com a trava do aborto na mão) |
| A4: `ring=sqlite`, sink que não aceita nada, segundo sinal na fase 4: `abort=0`, as linhas no `ring.db` e entregues no boot seguinte | `TestAbortWithTheSQLiteRingLosesNothing` |
| `SIGQUIT` nunca tolerado, e o primeiro `SIGTERM`/`SIGINT` do shutdown por FATAL tolerado (já valia; agora com teste de unidade em `shutdownWith`) | `TestFatalShutdownToleratesTheFirstSignal` |
| O canal do aborto registrado antes de o contexto soltar `SIGTERM`/`SIGINT` (um segundo sinal no meio saía 143 sem contar) | sem teste: a janela é de instruções, entre o `signal.Stop` do contexto e o `signal.Notify`; provado por leitura |
| Um sink FATAL durante o dreno do `SIGTERM` encerra a espera e sai 3 em qualquer ring (SPEC §10.3; fecha SPEC §19.1 item 4, já registrado, na mesma rodada por ser da fase 4) | `TestFatalDuringTheDrainEndsTheWaitAndExits3` (`memory`: o dump sai na hora; `sqlite`: 3, não 0) |
| X1: `bind` cancelado por sinal = 130, na porta admin e na pública, com nome de host (o *lookup* cancelado saía 1) e com IP literal (abria, `/readyz` 200, saía 0); o `Server.Close` fecha também os listeners que o `Serve` nunca recebeu | `TestSignalBeforeTheBindExits130` (`127.0.0.1:0` e `localhost:0`, a porta solta depois), `TestCancelledAdminBindIsAnAbortNotAConfigError` |

Não se aplicam (DECISOES §4b): o `quitNow` (sem observador de `SIGQUIT` fora do
shutdown), o prazo da reingestão sem progresso (sem retentativa nem prazo
único), o fechamento em paralelo (o `Server.Shutdown` já fecha os listeners
juntos). O `withLifetime` e o `validationContext` com `sink.op_timeout` já
estavam aqui, com `TestHealthChecksCarryTheirLifetime`.

**B (configuração, `internal/config/`).** Testes em
`internal/config/family_port_test.go`; cada um falhou contra o código de antes
(conferido no módulo intocado) e cada correção revertida sozinha derruba ao
menos um (40 mutantes, 40 mortos).

| Item | Teste |
|---|---|
| Cauda depois de um valor nomeada só dentro de uma seção conhecida (`knownSectionOf`; fecha §8 item 41) | `TestFamilyPortTailAfterAValueIsNotNamed`; controles de `TestParserErrorsDoNotEchoValues` ajustados (`--typo.key` depois de um valor culpa a flag anterior; `--sink.wrkers` é nomeada) |
| `-h`/`-v` depois de uma flag que espera valor não é nomeado como *switch* | `TestFamilyPortShortDashValueIsNotNamedAsAFlag` |
| Valor colado ao nome, `:` no lugar do `=`, na flag e no INI; chave antes de seção | `TestFamilyPortGluedValueIsNotPrinted` |
| Continuação de segredo numa linha do INI (senha, token, URL com credencial) | `TestFamilyPortINISecretContinuationIsNotPrinted` |
| *Switches* com valor; `--config=` vazio; BOM na linha 1 | `TestFamilyPortSwitchesTakeNoValue`, `TestFamilyPortEmptyConfigFlagIsRefused`, `TestFamilyPortINIByteOrderMarkIsIgnored` |
| Derivados (`ring.sqlite.path`, `sink.sqlite.path`, `emergency.dir`, `pool_size`) comentados no `--dump-config` | `TestFamilyPortDumpCommentsDerivedValues` (relido com outro `data_dir` e outros `workers`, deriva de novo) |
| Tetos, sem recusar config legítima (ClickHouse `batch=100000`; `ring=sqlite` com 50 milhões de itens e `max_bytes=16GiB`; `ring=none`) | `TestFamilyPortConfigCeilings` |
| `sink.postgres.addr` um servidor só; `sink.redis.addrs` `host:porta` | `TestFamilyPortPostgresAddrNamesOneServer`, `TestFamilyPortRedisAddrsAreHostPort` |

Não se aplica (SPEC §10.1, `DECISOES.md` 4b): `sink.batch_max_bytes` (a chave
não existe aqui), as regras de URL PostgreSQL da `LOGHUB_EB_URL` (não há
`LOGHUB_EB_URL` nem outro leitor de URL/DSN SQL), a `,` no host de uma URL
Redis (não há URL Redis; a vírgula é o separador de `sink.redis.addrs`), e o
socket PostgreSQL com `tls=true`, que já era recusado.

**C (sink NATS).** Os achados do `go-relplog-server`, do
`go-relplog-nats-server` e do `go-syslog-server` no sink NATS, conferidos um a
um contra este módulo (SPEC §8.7). Cada teste falhou antes da correção, e cada
correção foi revertida sozinha numa cópia (mutação): 23 mutantes, todos mortos
— 17 do sink, 6 da configuração e da app. Não se aplicam aqui, por não haver
`mode=core` neste servidor (só JetStream): a negação mascarada pelo `-ERR` do
beacon, o *flush* que atravessa uma reconexão e o aviso de outro `space` em
`core` (o `cover` do JetStream já recusa o subject descoberto).

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| `10077` de stream `discard: new` cheio ("maximum messages exceeded", texto real do `nats-server` 2.14.6) ou fechando ("store is closed", o `ErrStoreClosed` de `server/store.go`) era fatal; a família casava só "store closed", que o servidor não manda | Dump e `exit 3` por contrapressão ou por um reinício do NATS, e a parte já gravada do lote duplicada na reingestão | `TestNATSFullDiscardNewStreamIsNetwork` (servidor real), `TestClassifyNATS` |
| `10158`, `10167` e todo 429 caíam na regra do 4xx desconhecido (SPEC §19.1 item 7) | FATAL em todas as instâncias de um stream em cluster degradado ou num pico | `TestClassifyNATS` |
| `retention: interest` aceito; `max_msgs_per_subject` sem aviso (SPEC §19.1 item 6) | `PubAck` sem gravar: o `200` de `ring=none` e a liberação do ring como perda silenciosa | `TestNATSValidateRetentionAndPerSubjectCap`, `TestNATSInterestStreamIsRefused` (servidor real: o `PubAck` vem e nada fica) |
| A publicação negada e o `_INBOX` negado só viravam FATAL na checagem **seguinte**: a ida e volta que esbarrava na negação expirava como `network` | Um lote retentado e o sink UP até a próxima checagem; no boot, o `_INBOX` negado só no `Health` seguinte | `TestNATSPermissionDeniedIsFatal` (servidor real com permissões: `nopub` pelo prazo da confirmação e pelo do worker, `noinbox`, `admin` de controle) |
| Um certificado que não verifica e `sink.nats.tls` contra um servidor sem TLS eram `network` | O boot esperava o `sink.boot_timeout` inteiro para sair com a mensagem da rede; depois do boot, o sink DOWN até os fusíveis — ou para sempre, sem eles | `TestNATSBootTLSRefusalIsFatal` (o listener `tls {}` compartilhado; controles com a CA e com `tls_insecure`), `TestNATSReconnectToAnUntrustedTLSServerIsAVerdict` (servidor próprio que volta exigindo TLS com certificado não confiável; controle com um confiável), `TestClassifyNATS` |
| A regra do *pool* (C1 da revisão do `go-syslog-server`): a recusa de TLS de um nó só vale como veredito sem outro servidor no *pool* | Sem ela, o nó não confiável de um *pool* — nas URLs ou anunciado pelo cluster — virava FATAL, dump e `exit 3` enquanto o confiável voltava; no boot, um *pool* com o confiável fora saía na hora | `TestNATSOneUntrustedServerOfAPoolIsNoVerdict` (`urls` e `announced`, com servidores próprios: nenhum FATAL, o TLS nomeado como motivo, de volta UP), `TestNATSBootTLSRefusalOfAPoolIsNetwork` |
| `MaxPending` sem folga e sem teto em `ring.max_inflight` (C3) | Retentativa rápida de um lote contaminado parada na janela cheia (`ErrTooManyStalledMsgs`); um `max_inflight` grande dava a volta e caía em 4096 em silêncio | `TestNATSMaxPendingHasTwiceTheInFlight` (app), `TestRingMaxInflightHasACeiling` (config) |
| `sink.nats.urls` aceitava qualquer esquema, recusava o caminho de um `ws://`, e o `@` depois de `/` só dizia "path" | Um esquema que o `nats.go` não disca falhava só na conexão, até o `sink.boot_timeout`; um *endpoint* websocket legítimo recusado | `TestNATSURLsFollowTheFamilyRules`, `TestNATSURLsAreCheckedWithoutEcho` (o texto esperado do `@` depois de `/` passou a ser o `%2F`) |

O `ReconnectErrHandler` novo (veredito de TLS se um dia o `nats.go` o relatar
ali) não tem teste: o cliente atual só o chama na discagem TCP, como no
`go-syslog-server`.

**D (destinos SQL e o dreno).** Cada correção tem o teste abaixo, contra
PostgreSQL 18, MySQL 8.4 e ClickHouse 25.8 reais, e foi revertida numa cópia
(mutação): 46 de 46 mutantes morreram (7 PostgreSQL, 24 MySQL/ClickHouse/dreno,
2 da montagem, 13 da configuração). `23502` e `0A000` fatais, `RULE` de
`INSERT`/`UPDATE` recusada no boot: `TestClassifySQLState`,
`TestPostgresNotNullViolationIsFatal`, `TestPostgresRulesAreRefusedAtBoot`. O
check de saúde com conexão própria: `TestPostgresHealthHasItsOwnConnection`,
`TestMySQLHealthHasItsOwnConnection`, `TestClickHouseHealthHasItsOwnConnection`
(`pool = escritores` em 1 e 3, escritores presos num lock ou numa *materialized
view* que dorme), e a reconexão: `TestPostgresHealthRedialsADeadConnection`.
MySQL: `1048` fatal (`TestClassifyMySQLNumber`,
`TestMySQLNotNullAfterBootIsFatal`, um *trigger* que anula), `1203`/`1226`/
`1461` como `network` (`TestClassifyMySQLNumber`,
`TestMySQLUserConnectionLimitIsNetwork`, com um usuário próprio de
`MAX_USER_CONNECTIONS 1`), `autocommit` (`TestMySQLSessionPinsAutocommit`,
`TestMySQLAutocommitIsReadBackAtBoot`, sem `SET GLOBAL`). Nome do banco por
caracteres: `TestSQLDatabaseNameBetweenBackticks`,
`TestDatabaseNamesBetweenBackticks`. A conta de memória do ClickHouse por
escritores: `TestClickHouseMemoryCountsTheWritersNotThePool`,
`TestClickHouseUserMemoryLimitCountsTheWriters`, `TestSinkWritersFollowTheRing`,
`TestClickHouseBootCountsTheWorkersNotThePool` (a montagem passa os
escritores). A regra do pool: `TestSQLPoolOfExactlyTheWorkersIsLegitimate`,
`TestPostgresPoolSizeWarnings`. O dreno já tratava a recusa `logic` antes do
contexto; `TestWorkersLogicRefusalDuringShutdownIsLoggedAndCounted` a guarda
(o mutante que olha o contexto primeiro morre). E o *flake* de
`TestPostgresWrongPasswordIsFatal` (`53300` antes de conferir a senha, com as
vagas do servidor esgotadas por outras suítes): o teste repete enquanto essa é
a resposta, como no RP. A conexão própria do check mudou um teste antigo: o
aquecimento de `TestPostgresValidationHasItsOwnDeadline` (e dos irmãos MySQL
e ClickHouse) abre também a conexão do check, ou o *handshake* dela, pelo
*proxy* que atrasa cada pedaço, consumia o prazo da checagem (1 falha em 8
rodadas antes; 45 de 45 depois).

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

### Superfície HTTP e conexões (2026-10-14)

Os itens 1 a 7 de "Superfície HTTP e conexões" das pendências do
`go-webhook-server` (SPEC §19.1 de lá), iguais nos dois módulos. Cada teste
abaixo falhou contra o código de antes (o 6 é teste de trava de uma regra que
o código já cumpria), e cada correção revertida sozinha numa cópia derrubou ao
menos um (mutação). Sem serviço externo: tudo em `internal/server`,
`internal/ratelimit` e `internal/app`.

| Item | Regra | Teste |
|---|---|---|
| 1. Alocação pelo `Content-Length` | §12.2, §4.3 passo 11 | `TestReadBodyAllocatesOnlyWhatArrives` (5 MiB declarados e 10 bytes enviados: buffer e maior leitura oferecida ≤ 64 KiB; corpos honestos de 100 KiB, 1 MiB e 5 MiB terminam no tamanho declarado) |
| 2. Checagem rápida por bytes | §4.3 passo 9, §4.4 | `TestErrorAnswersBeforeTheUUIDArePlainText`, casos "429 ring full by bytes, fast check", "... no declared length" e "429 declared body does not fit", com o controle de 100 bytes com folga |
| 3. Dreno só em HTTP/1.x sem `Expect` | §4.3 | `TestRejectDrainFollowsTheProtocol` (h2: nem dreno nem `Connection: close`; `Expect: 100-continue` em qualquer caixa: sem dreno; HTTP/1.1: dreno e `close`, o controle), `TestRejectWithExpectSendsNo100Continue` (socket: a primeira linha é o `503`, nunca o `100 Continue`, e a conexão fecha) |
| 4. `write_timeout` desde a resposta | §13.5 | `TestSlowUploadStillGetsItsAnswer` (HTTP/1.1, corpo 600 ms depois dos cabeçalhos com `write_timeout` de 200 ms), `TestSlowUploadStillGetsItsAnswerOverH2C`, `TestWriteTimeoutIsArmedForEveryAnswer` (200, recusa antes e depois do UUID) |
| 5. Erro no `accept` e listener parado | §10.3, §10.5 | `TestAcceptRetriesNonTemporaryErrors` (três `ENOBUFS` e depois a conexão, servida), `TestServeReturnsWhenOneListenerStops` (`listeners = 2`), `TestServeErrorGoesThroughTheShutdown` (`internal/app`: com o destino preso, o dump e `exit 3`; com ele bom, tudo entregue e `exit 1`) |
| 6. `max_conns_per_ip` pelo endereço exato | §13.5 (decisão contestável, `DECISOES.md` 4c do fast) | `TestPerIPLimitCountsTheExactAddress` (dois endereços do mesmo /64 são duas cotas; IPv4 mapeado = IPv4) |
| 7. Contador de buckets | §6.3 | `TestResetDoesNotLoseConcurrentInserts` (determinístico: o teste segura a trava do shard 1, o `Reset` para nele, uma inserção entra no shard 0 já limpo; depois o teto e o sweep), `TestMaxBucketsHoldsUnderConcurrentInserts` (guarda por carga da reserva no teto), e `TestConcurrentAllowSweepAndReload` passou a exigir o contador exato em repouso |

### Pendências 10–13 da SPEC §19.1 e as do `go-webhook-server` (2026-10-14)

Cada item com a SPEC primeiro, o teste abaixo falhando contra o código de antes
e a correção revertida sozinha (mutação), contra os serviços reais de teste
(Redis normal e `allkeys-lru`, PostgreSQL, MySQL). O mesmo no
`go-webhook-server`; os números da tabela são os da §19.1 de lá (aqui: 12 = 10, 13 = 11, 28 = 12, 29 = 13; os demais não estavam listados).

| Item | Teste |
|---|---|
| 12 — `maxmemory-policy` `allkeys-*` fatal no boot e em toda checagem; ilegível, um `WARN` (§8.2.5) | `TestRedisEvictionVerdict`, `TestRedisRefusesAnEvictingServer`, `TestRedisWarnsWhenThePolicyIsUnreadable`, `TestRedisAcceptsANonEvictingServer` (`internal/sink/redis_eviction_test.go`), `TestRedisEvictingServerEndsTheBoot` (`internal/app/redis_boot_test.go`) |
| 13 — `sink.redis.tls_ca` sem `tls` é exit 1 (§8.2.2) | `TestRedisTLSCAWithoutTLSIsRefused` (`internal/config/pending_config_test.go`) |
| 15 — o pool parseado com o ambiente `PG*` escondido; o discador sem o prazo de `PGCONNECT_TIMEOUT` (§8.5.2) | `TestNewPostgresParsesWithoutTheLibpqEnvironment` (`internal/sink/libpq_env_test.go`; sem servidor) |
| 22 — um `sink.boot_timeout` só no passo 3 (§10.1) | `TestRedisBootHasOneDeadline`, `TestRedisBootClientLoopKeepsTheDeadline` |
| 23 — a sonda do `sink=sqlite` espera a trava só no próprio prazo; escritora ocupada com o último `Write` bom é UP (§8.3.5) | `TestSQLiteProbeDoesNotWaitForTheWriterPastItsDeadline` (o teste segura a trava; sem corrida) |
| 24 — `NaN`/`±Inf` recusados no parser (§13.5) | `TestNonFiniteNumbersAreRefused` |
| 25 — `admin.bind` vazio ou sem porta com `admin.enabled` é exit 1 (§11) | `TestEmptyAdminBindIsRefused` |
| 26 — o dump põe aspas no valor com branco nas pontas ou entre aspas iguais (§13.2) | `TestDumpINIRoundTripsQuotedAndPaddedValues`, `TestQuoteINI` |
| 27, 28 — os compose repassam todo `WHS_SINK_<X>_*` do `.env.example`; imagens fixadas (§13.6) | `TestComposeForwardsEverySinkVariable`, `TestComposeImagesArePinned` (`internal/config/compose_test.go`, sem Docker); `docker compose -f <arquivo> config -q` nos 22 |
| 29 — os seis guardas de regressão falham em vez de pular (§15.1) | os próprios: `TestMySQLRefusesAnEnvelopeItDidNotWrite`, `TestPostgresHealthValidatesTheRole`, `TestTwoNonLengthsAreNotLegacy`, `TestFingerprintsKeepTheirCommas`, `TestFallbackHostPortDoesNotReachTheURL`, `TestDescribeOnARealSocket/OPTIONS *`; cada defeito reintroduzido dava `SKIP` (saída 0) e agora dá `FAIL` |

### Ring, lote e recusa no boot (2026-10-14)

Itens 8, 9, 10, 11 e 20 da SPEC §19.1, e o achado da triagem sobre os lotes
retidos. Cada teste abaixo falhou contra o código de antes (o 8 e o 11 com só
a constante ou o campo novos acrescentados, para compilar), e cada correção
revertida sozinha (mutação) derruba ao menos um deles. O mesmo no
`go-webhook-fast-server`.

| Item | Teste |
|---|---|
| Ring de memória cobra `cap(Env)` + `ItemOverhead` e desconta o `Item.Charged` gravado (SPEC §7.2, §12.2; decisão do usuário) | `TestMemoryChargesTheRetainedBuffer` (100 bytes num buffer de 16 KiB: o segundo não cabe em 20 KiB), `TestMemoryGivesBackWhatItCharged` (o `Commit` depois de um `Env` zerado devolve o cobrado), `TestItemOverheadCoversTheStruct`; `TestMemoryByteCeiling` passou a contar pelo cobrado |
| Lotes retidos contam no teto de itens do ring de memória (SPEC §7.2; achado da triagem) | `TestMemoryHeldItemsCountAgainstTheCeiling` (porte do S) |
| Reserva no `Enqueue` do `ring=sqlite` (SPEC §7.4) | `TestSQLiteConcurrentEnqueueRespectsTheCeilings` (porte do S: 200 *goroutines* num só grupo, `CommitMaxDelay` de 500 ms, itens e bytes; a recontagem conferida fechando e reabrindo o ring), `TestSQLiteReservationIsReleasedWhenTheRingCloses` |
| Leitura parcial do `ring=sqlite` entrega o que leu (SPEC §7.4) | `TestSQLiteDequeueServesWhatItReadBeforeAFailure` (`jlh_ring` criada antes sem `STRICT`/`NOT NULL`, linha 2 com `uuid NULL`) |
| Worker só termina com o contexto, `ErrClosed` ou `ErrNoBacklog`; o resto é log amostrado e backoff (SPEC §8.1 passo 1) | `TestWorkersSurviveTransientDequeueErrors`, `TestWorkersStopOnTheEndsOfTheRing` (porte do S, mais o contexto cancelado com o ring falhando) |
| `sink.batch_max_bytes`: pedaços nos workers, na reingestão e no dreno do `ring.db`; o resto do lote retido durante a retentativa (SPEC §8.1 passo 2, §8.5.4; decisão do usuário) | `TestSplitByBytes`, `TestWorkersSplitABatchByBytes`, `TestWorkersSendAnEnvelopeAboveTheCeilingAlone`, `TestWorkersHoldTheRestOfTheBatchWhileAPieceRetries`, `TestBootWritesInPiecesUnderBatchMaxBytes` (dump, workers montados pela app, `ring.db`), `TestSinkBatchMaxBytes` (padrão, INI, flag, env, dump, recusas), `TestWorstBatchBytes` e `TestPostgresByteCeilingWarning` ajustados |
| Recusa `logic` no boot: dump item a item e `.corrupt`; linha do `ring.db` descartada e contada (SPEC §9.2, §10.1 passo 5) | `TestBootLogicRefusalInADumpIsQuarantinedNotFatal`, `TestBootLogicRefusalInRingDBIsDroppedAndCounted` (`ring=sqlite` e `ring=memory`); `TestQuarantineWithoutDurabilityIsStillReported` com a assinatura nova |
| Porta do dump: UTF-8, só os escapes do serializador, sem U+2028/U+2029 crus (SPEC §9.2) | `TestReadStopsAtInvalidUTF8AndForeignEscapes` (cada registro passa no `ParseHead` e no `json.Valid`; controle com tudo o que o serializador escapa), `TestOnlyOurEscapes` (do P, caso a caso) |

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
| W 33 — a escrita direta do boot respeita `Error.Delivered` (§9.2) | `TestBootWriteHonoursPartialDelivery` | `item … written 2 times, want once` (3 itens) | o ramo `DeliveredOf` desligado |
| W 34 — a porta do dump aceita só os escapes do serializador (§9.2) | `TestOnlyOurEscapes` (reescrito segundo a SPEC), controle do escritor com `\r`, `\x1f`, `\x7f` | 10 casos `= true, want false` | a função antiga |
| W 35 — piso de `ring.max_bytes` com `ring=memory` (§7.2) | `TestRingMaxBytesCoversTheLargestEvent` | `--ring.max_bytes=65791 … was accepted` | a condição com `&& false` |
| W 36 — a escritora ocupada só é prova de vida até `op_timeout + health_timeout`, no relógio monotônico (§8.3.5) | `TestSQLiteProbeSeesAWriterStuckPastItsLimit`, `TestSQLiteProbeMeasuresTheBatchOnTheMonotonicClock` | `<nil> …, want … DOWN`; `Write recorded 0s, want … 10h` | sem limite; relógio de parede |
| W 37 — o heartbeat é escrita para a sonda (§8.3.5) | `TestSQLiteFailedHeartbeatCountsAsAFailedWrite`, `TestSQLiteGoodHeartbeatClearsAFailedOne`, `TestSQLiteHeartbeatCutByItsDeadlineIsNotAFailedWrite` | `UP, want DOWN`; `… cut by its own deadline … want UP` | sem gravar a falha; sem gravar o sucesso; sem a exceção do contexto |
| W 38 — `rueidis.NewClient` dentro do prazo do boot (§10.1) | `TestRedisBootDeadlineHoldsAgainstASilentServer` (listener que aceita e não fala; sem Redis) | `the boot took 5.00s with sink.boot_timeout=1s` | `rueidis.NewClient` direto |
| W 39 — `WaitReady` dorme `min(backoff, restante)` (§10.1) | `TestWaitReadyDoesNotSleepPastItsDeadline` | `took 2.00s with a 300ms budget` | `time.After(backoff)` |
| W 40 — `admin.bind` com porta 1–65535 (§11) | `TestEmptyAdminBindIsRefused` (+ `127.0.0.1:`, `:`, `[::1]:`, `localhost:abc`, `:0`, `:65536`, `:+80`, `:080`) | `--admin.bind=127.0.0.1: … was accepted` | sem o `validPort` |
| W 41 — byte de controle num valor de texto é exit 1 (§13.2) | `TestControlBytesInTextValuesAreRefused` | as 4 entradas `was accepted` | a checagem desligada |
| W 42 — a sonda do Redis em uma ida e volta (§8.2.5) | `TestRedisPolicyProbeStaysOneRoundTrip` (Redis real, ACL `-info` e `-info -config`) | `2 pipelines and 2 commands alone, want 2 and 0` | sem gravar o modo |
| T1 — reserva devolvida com o writer já fora (§7.4) | `TestSQLiteReservationIsReleasedWhenTheWriterIsGone` | (lacuna de teste: passa no código) | sem o `unreserve` do ramo `writerDone` |
| T2 — o resto do lote retido com falha no pedaço do meio, e com fatal (§8.1 passo 3) | `TestWorkersHoldTheRestOfTheBatchWhileAPieceRetries` (6 itens, 3 pedaços) | (lacuna de teste: passa no código) | `hold` por pedaço |

**Rodadas.** `gofmt -l .` vazio e `go vet ./...` limpo. Duas rodadas completas `go test -race -count=1 -timeout 1500s ./...` (`tasks/run2.sh go-webhook-fast-server rev`, serviços reais como no W): as duas verdes, nenhuma refeita.

### `TestRedisBootHasOneDeadline` instável (2026-10-14)

Na rodada final o teste caiu uma vez com a máquina quieta: o proxy voltava aos
1,5 s, o mesmo instante da quinta tentativa do cliente (0; 0,1; 0,3; 0,7;
1,5 s), e às vezes a tentativa chegava antes e o boot esgotava o prazo sem
construir o cliente ("control: the Redis client was never built"). A retomada
passou para 1,2 s; `-race -count=8` verde em W e F. O teste continua pegando o
prazo duplo: com o cliente construído aos 1,5 s, um segundo prazo de 2 s
levaria o boot a ~3,5 s, acima do teto de 3 s.
