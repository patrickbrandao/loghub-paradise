# go-syslog-server — Procedimentos de teste

Como provar que o código cumpre `docs/SPEC.md`, e o que **ainda não** está
provado. A §8 é a parte mais importante deste arquivo: ela é mantida por
honestidade, e fechar um item dela vale mais que acrescentar recurso.

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**. Os bugs de perda de dados deste desenho vivem em
caminhos concorrentes: posse de buffer, lote retido por worker, janela do RELP,
prazo de leitura no shutdown. Todos passam invisíveis sem ele.

Leva cerca de 45 segundos; a maior parte é o `ring=sqlite`, que faz commits
reais.

## 2. Cobertura real

Cobertura por pacote engana: a suíte de `app` exercita `receiver`, `ring` e
`sink` sem que isso lhes seja atribuído.

```bash
go test -coverpkg=./internal/... -coverprofile=cov.out ./... && go tool cover -func=cov.out | tail -1
```

## 3. Fuzzing (SPEC §17.2, obrigatório antes de release)

| Alvo | Pacote | O que protege |
|---|---|---|
| `FuzzParse` | `syslog` | O parser completo; propriedade: nunca entra em pânico e `msg-offset + msg-size == len(raw)` |
| `FuzzSD` | `syslog` | Structured-data, com escaping próprio e buffer de desescape compartilhado |
| `FuzzFrame` | `receiver` | As duas molduras da RFC 6587 e a moldura RELP |
| `FuzzRead` | `emergency` | O decoder do dump, que precisa tolerar arquivo truncado |

```bash
go test ./internal/syslog/    -run=XXX -fuzz=FuzzParse -fuzztime=10m
go test ./internal/syslog/    -run=XXX -fuzz=FuzzSD    -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=FuzzFrame -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=FuzzRead  -fuzztime=10m
```

Rodado durante o desenvolvimento, por períodos curtos: `FuzzParse` passou de 17
milhões de execuções somadas, `FuzzSD` de 5 milhões, `FuzzFrame` de 1 milhão e
`FuzzRead` de 3 milhões, sem falha. Os dez minutos por alvo foram cumpridos em
2026-09-14 (`-parallel=3`, macOS), depois das correções dessa data: `FuzzParse`
74,8 milhões de execuções, `FuzzSD` 69,5 milhões, `FuzzFrame` 7,2 milhões e
`FuzzRead` 30,1 milhões, sem falha e sem semente nova.

## 4. Corpus de mensagens reais (SPEC §17.3)

`internal/syslog/testdata/corpus/` guarda um `.raw` com os bytes exatos e um
`.want` com os campos esperados, um par por emissor real:

```bash
go test -run TestCorpus -v ./internal/syslog/
```

O corpus não é decoração. **Três bugs do parser foram achados só por ele**, e
nenhum teste inventado os pegaria:

| Emissor | Linha | O bug |
|---|---|---|
| glibc `syslog(3)` e journald | `<30>Sep 13 10:00:00 systemd[1]: Started` | `systemd[1]:` gravado como hostname |
| rsyslog `RSYSLOG_ForwardFormat` | `<30>2026-09-13T10:00:00.1+00:00 web-01 app[1]: x` | `2026-09-13T10` virava o app-name |
| Cisco IOS | `<189>1234: *Sep 13 10:00:00.123: %SYS-5-...` | a sequência `1234` virava o app-name |

A regra para o corpus: **ele nunca encolhe**. O teste falha abaixo de vinte
entradas. Um caso novo de campo entra aqui antes da correção do parser.

## 5. Integração em processo

`internal/app/integration_test.go` monta a pilha **real** — a mesma `run()` que o
binário chama — com `sink=sqlite` e `sink=none`, sem servidor externo.

| Teste | O que prova |
|---|---|
| `TestAllTransportsReachTheDestination` | UDP, TCP LF, TCP octet e RELP chegam ao destino, cada linha com seu transporte |
| `TestTheDestinationRowIsFaithful` | a linha crua volta byte a byte, com `\0` no meio; as colunas batem; `msg-offset` fatia a mensagem exata; structured-data sai desescapado |
| `TestFilterAndReload` | o filtro descarta antes do ring, e o SIGHUP o afrouxa em tempo de execução |
| `TestReloadRevertsAReloadableKeyToItsBootValue` | a reversão de uma chave recarregável ao valor de boot é aplicada e observada no filtro; a chave de reinício avisa em todo SIGHUP e para de avisar ao voltar ao boot (§15.3) |
| `TestRELPConfirmationSurvivesARestart` | com `ring=sqlite`, o `200 OK` do RELP só sai com a linha no disco |
| `TestEmergencyDumpAndReingestionAreExact` | **o cenário mais importante**: um sink que para de gravar com o health check passando; o worker retém o lote fora do ring; o shutdown o coleta de `Pending()`, despeja, sai com 3; a vida seguinte reingere as três mensagens com as colunas consultáveis intactas e remove o dump |
| `TestAdminSurface` | `/healthz`, `/readyz`, e as séries `syslog_` com labels de conjunto fechado |
| `TestGracefulShutdownDeliversEverything` | 200 mensagens aceitas, SIGTERM imediato, 200 linhas no destino |
| `TestNoTransportExitsOne` | nenhum transporte configurado é `exit 1` |
| `TestSecondSignalAborts` | segundo sinal durante o shutdown sai com 130 |

O teste do dump usa o ponto de injeção `App.wrapSink`, nulo no binário. Ele
existe porque o cenário não é reproduzível de fora: um diretório somente leitura
não para um SQLite já aberto, e um Redis parado falha o boot em vez de falhar as
escritas.

## 6. Destinos externos

### 6.1 Redis

Verificado **à mão** com a combinação de referência, contra um Redis 7.4 real:

```bash
cd docker
SYSLOG_PORT=15514 SYSLOG_RELP_PORT=25514 SYSLOG_ADMIN_PORT=29103 \
  docker compose -p sls-smoke -f compose.memory-redis.yml up -d --build
```

| Verificação | Resultado |
|---|---|
| UDP, TCP e RELP chegam à stream `syslog:in` com `v`, `e`, `b` | 3 de 3; RELP respondeu `200 OK` |
| A sonda de escrita não cria chave | `EXISTS syslog:in:healthprobe` = `0` |
| Redis rebaixado a réplica: `XLEN` responde, `/readyz` falha | `/readyz` 503 `sink is DOWN`, `syslog_sink_up 0` |
| Réplica promovida de volta | `/readyz` 200, log `sink recovered` |
| Mensagem TCP enviada **durante** a falha | segurada por backpressure, entregue na recuperação; `XLEN` foi de 3 para 4 |
| Contêiner | uid 65532, raiz somente leitura, `cap_drop: ALL`, parada graciosa com exit 0 |

Teste de fumaça dos limites de memória (2026-09-14, Docker Desktop no macOS,
**não é a aferição de §17.7**): `compose.memory-redis.yml` com o Redis pausado e
mensagens de 8 KiB pelo TCP até o ring bater no teto de 256 MiB; 27 s de
backpressure contabilizados, RSS máximo de 329 MiB contra `GOMEMLIMIT=512MiB`
e limite de 768M no contêiner, sem OOM, dreno em 3 s ao despausar. Os 12
compose sobem sem o aviso de `GOMEMLIMIT` (§14.4).

**Ainda não há suíte automatizada contra Redis** como a do irmão. É a §8, item 1.

### 6.2 PostgreSQL

Os testes de unidade cobrem a classificação de SQLSTATE, o saneamento de UTF-8,
o `reset` do pool de argumentos e o parse de UUID. **Nada roda contra um
PostgreSQL real.** É a §8, item 2.

### 6.3 MySQL

Suíte escrita no molde da do irmão, que rodou contra MySQL 8.4 real: cada teste
cria a própria tabela a partir de `docker/mysql/schema.sql` e a apaga no fim.

```bash
docker run --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=sls -e MYSQL_DATABASE=sls_test \
  --name sls-test-mysql mysql:8.4
SYSLOG_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13306)/sls_test' \
  go test -race -timeout 600s -run MySQL ./internal/sink/ ./internal/app/
docker rm -f sls-test-mysql
```

| Pacote | Teste | O que prova |
|---|---|---|
| `sink` | `TestMySQLWriteStoresQueryableColumns` | as dezessete colunas de §3.7, tipadas; `env` e `raw` byte a byte |
| `sink` | `TestMySQLPreservesBinaryAndEmptyRaw` | `NUL`, `\n`, `\r` e `0xff` intactos; linha vazia é blob vazio, nunca `NULL` |
| `sink` | `TestMySQLToleratesRedeliveryAndCountsIt` | redelivery não duplica e é contada pela aritmética das linhas afetadas |
| `sink` | `TestMySQLInvalidUTF8HostnameDoesNotDropTheBatch` | hostname inválido sob `sql_mode` estrito vira `U+FFFD` na coluna, sem derrubar o lote |
| `sink` | `TestMySQLLongHostnameFitsTheCanonicalSchema` | hostname RFC 3164 de 100 KiB cabe no `LONGTEXT`; num `TEXT` vira `logic` |
| `sink` | `TestMySQLSearchesTheRawLine` | `raw LIKE` sem `CAST` e `SUBSTRING(raw, msg_offset + 1, msg_size)` como §3.7 promete |
| `sink` | `TestMySQLSplitsLargeBatches` | lote acima de 3 855 linhas e acima do pacote dividido, todas as linhas gravadas |
| `sink` | `TestMySQLHealthValidatesTheSchema` | tabela ausente, tipo errado, coluna ausente, sem índice único, MyISAM, `id` de largura errada: fatal; extras e tipos mais estreitos tolerados |
| `sink` | `TestMySQLHealthValidatesThePrivilege` | só `INSERT` é recusado no boot; `INSERT` + `UPDATE (id)` basta; a sonda não deixa linha |
| `sink` | `TestMySQLDroppedTableIsFatalNotLogic`, `TestMySQLPacketCeilingIsCheckedAtBoot` | `1146` na escrita é fatal; pacote menor que a maior linha é fatal no boot |
| `app` | `TestMySQLAllRingModes` | os três pares novos da matriz, reconciliando confirmações RELP com linhas |
| `app` | `TestMySQLTheRowIsFaithful`, `TestMySQLRingNoneIsInstrumented` | a linha de ponta a ponta; as métricas com `ring=none` |
| `app` | `TestMySQLSchemaMismatchExitsThreeImmediately`, `TestMySQLUnreachableKeepsTransportsShut` | `exit 3` imediato com esquema errado ou pacote pequeno; transportes nunca abrem |
| `app` | `TestMySQLDownThenBackLosesNothing`, `TestMySQLFatalDumpsAndTheNextBootReingests` | queda e volta por proxy sem perda; FATAL, dump, reingestão com as colunas consultáveis |

**Ainda não executada contra um MySQL real** nesta pasta: é a §8, item 18. Sem
servidor, `TestMySQLSchemaFileMatchesTheContract` confere o arquivo de esquema
contra o contrato de colunas do código, e os testes de unidade cobrem a tabela
de `Number`, o texto do comando, a divisão do lote, o pool de argumentos e a
conta da maior linha.

### 6.4 NATS

O `nats-server` é um módulo Go, e a suíte não precisa de Docker:

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 \
  go test -race -timeout 600s -run NATS ./internal/sink/ ./internal/app/
```

| Pacote | Teste | O que prova |
|---|---|---|
| `sink` | `TestNATSPublishStoresEnvelopeAndRaw` | `Nats-Msg-Id`, `Sls-Version`, e `Data` fatiado por `Sls-Envelope-Size` em envelope e linha crua |
| `sink` | `TestNATSPreservesBinaryAndEmptyRaw` | linha binária intacta; linha vazia deixa `Data` só com o envelope |
| `sink` | `TestNATSEnvelopeSurvivesHostileBytes` | envelope com bytes de controle, `DEL`, UTF-8 inválido e aspas, e um de mais de 100 KiB, voltam byte a byte |
| `sink` | `TestNATSRedeliveryIsDeduplicatedAndCounted`, `TestNATSConcurrentWriters` | desduplicação por `Nats-Msg-Id` contada; oito escritores numa conexão |
| `sink` | `TestNATSLargestAcceptedMessageFitsTheServer` | a maior linha que a checagem de boot admite, no formato que mais infla o envelope, é aceita pelo `max_payload` real; um byte a mais é recusado no boot |
| `sink` | `TestNATSHealthValidatesTheStream` | stream ausente, subject de outro stream, subject sem stream, stream selado, `max_payload` e `max_msg_size`: fatal |
| `sink` | `TestNATSExpectedStreamMismatchIsFatalOnWrite`, `TestNATSDroppedStreamIsFatal` | subject de outro stream recusado na escrita; stream apagado é fatal na sonda |
| `app` | `TestNATSAllRingModes` | os três pares novos, reconciliando confirmações RELP com mensagens no stream |
| `app` | `TestNATSTheStreamMessageIsFaithful`, `TestNATSRingNoneIsInstrumented` | a mensagem de ponta a ponta, com `NUL` e structured-data; as métricas com `ring=none` |
| `app` | `TestNATSStreamMismatchExitsThreeImmediately`, `TestNATSUnreachableKeepsTransportsShut` | `exit 3` imediato com stream errado ou frames padrão acima do `max_payload`; transportes nunca abrem |
| `app` | `TestNATSDownThenBackLosesNothing` | proxy pausado: `/readyz` 503; retomado: recupera, uma reconexão contada, nenhuma confirmação perdida |
| `app` | `TestNATSFatalDumpsAndTheNextBootReingests` | FATAL com 8 mensagens confirmadas no ring, dump, `exit 3`, reingestão completa no boot seguinte |

Primeira execução em 2026-09-15, `nats-server` v2.14.6 compilado do cache de
módulos, macOS: os 16 testes de integração (9 em `sink`, 7 em `app`, com seus
subtestes) e os 4 de unidade passaram com `-race`. Foi ela que achou o limite de
header do JetStream (§9).

## 7. Emissores vivos (SPEC §17.4)

A prova que o corpus não dá: a moldura no fio, com os programas reais.

```bash
SYSLOG_TEST_EMITTERS=1 go test -race -run Emitters -timeout 600s ./internal/app/
```

Sobe o servidor em processo e roda, em contêineres Alpine descartáveis:

| Teste | Emissor | Molduras |
|---|---|---|
| `TestEmittersRsyslog` | rsyslog com `omfwd` e `omrelp` | UDP, TCP LF com `RSYSLOG_ForwardFormat`, TCP octet com `RSYSLOG_SyslogProtocol23Format`, RELP |
| `TestEmittersSyslogNG` | syslog-ng | `network()` TCP LF, `syslog()` TCP octet RFC 5424, `network()` UDP |

Cada teste manda 25 mensagens pelo `logger` de dentro do contêiner, que as passa
ao emissor pelo `/dev/log`, e confere a contagem de linhas por transporte e
moldura. O do rsyslog confere também app-name, facility e severidade de toda
linha, e o número de confirmações RELP.

Os testes **pulam** sem a variável e sem Docker, e baixam pacotes na primeira
execução.

Resultado da primeira execução, em 2026-09-13, rsyslog 8 e syslog-ng 4.8.3 sobre
Alpine 3.22, Docker Desktop no macOS:

| Emissor | Moldura | Linhas |
|---|---|---|
| rsyslog | UDP | 25 de 25 |
| rsyslog | TCP LF (`RSYSLOG_ForwardFormat`, timestamp RFC 3339 em linha 3164) | 25 de 25 |
| rsyslog | TCP octet (`RSYSLOG_SyslogProtocol23Format`) | 25 de 25 |
| rsyslog | RELP, com 25 confirmações `200 OK` | 25 de 25 |
| syslog-ng | `network()` TCP LF | 25 de 25 |
| syslog-ng | `syslog()` TCP octet RFC 5424 | 25 de 25 |
| syslog-ng | `network()` UDP | 25 de 25 |

Todas as 100 linhas do rsyslog saíram com `app-name`, facility e severidade
corretos — incluindo as do formato de encaminhamento, que o parser lia errado
antes do corpus (§4).

Segunda execução em 2026-09-14, depois das mudanças desta rodada nos leitores
de moldura e nos transportes TCP e RELP (`framing.go`, `tcp.go`, `relp.go`,
`limits.go`): os dois testes passaram, 25 de 25 em toda moldura, rsyslog em
12 s e syslog-ng em 11 s.

---

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento.

1. **Sem suíte automatizada contra Redis.** A §6.1 foi verificada à mão. Falta o
   equivalente do `SYSLOG_TEST_REDIS_ADDR` do irmão: escrita em lote, retry que
   reconstrói comandos, `OOM` fatal ponta a ponta, Sentinel e TLS.
2. **Nada contra um PostgreSQL real.** A validação de esquema, o `unnest` com
   dezessete arrays, o `ON CONFLICT` e a detecção de `pg_is_in_recovery()` estão
   escritos mas nunca executaram contra um servidor. Cópia direta do irmão, onde
   rodam, mas a tabela aqui é outra.
3. **RELP com corte do servidor no meio do fluxo.** O cenário 4 de §17.4 — matar
   o servidor com transações em voo e provar que o rsyslog retransmite tudo que
   não foi confirmado — **não existe**. É o único teste que prova a promessa
   central num transporte capaz de prová-la, e é o primeiro a escrever.
4. **Duplicata no RELP após confirmação perdida.** Se o `200 OK` de uma mensagem
   já no ring não chegar ao cliente (conexão cortada na hora), o rsyslog
   retransmite, e como o RELP não carrega id de mensagem, o destino recebe um
   segundo evento com outro `id`. O shutdown gracioso evita isso ao esperar as
   confirmações decididas antes de fechar o socket; um corte abrupto não. É
   at-least-once, inerente ao protocolo, e não tem teste.
5. **`SO_RCVBUF` do UDP não é verificado.** O servidor pede
   `listen.udp_read_buffer` e avisa se o `setsockopt` falhar, mas o Linux reduz
   o pedido **em silêncio** ao `net.core.rmem_max` sem devolver erro. O valor
   efetivo não é lido de volta nem exposto. Descartes do kernel só aparecem em
   `/proc/net/udp`.
6. **`SO_REUSEPORT` com mais de um listener** só é testado no que é portável: que
   os sockets abrem. A distribuição entre núcleos que importaria em produção é a
   do Linux, e os testes rodam no macOS.
7. **`kill -9` de verdade** não é testável em processo. A durabilidade do
   `ring=sqlite` é provada por fechamento e reabertura, e a do RELP por leitura
   direta do `ring.db` no instante da confirmação.
8. **A regra do `/data` não tem teste automatizado.** Verificada à mão na §6.1:
   o contêiner sobe com raiz somente leitura e funciona. Nada na suíte impede um
   caminho de escrita novo fora de `ring.data_dir`.
9. **Os critérios de §17.7 nunca foram medidos.** Não há gerador de carga. Em
   particular, **a decisão de tirar o parse do caminho quente (§2.2) nunca foi
   medida contra a alternativa**. A métrica `syslog_parse_duration_seconds`
   existe para responder isso em produção.
10. **O transporte `unix` com `/dev/log` de verdade** não foi testado no Linux; os
    testes usam um socket em diretório temporário no macOS.
11. **A rotação do `sink=sqlite` com `keep_files > 0`** tem o caminho de remoção
    escrito e não testado; só o padrão `0`, que nunca apaga, tem teste.

12. **O envio tardio do rueidis após um cancelamento só foi reproduzido à mão.** A
    regra de `Discard`/`Taint` de §14.2 é provada pela decisão da recepção, do
    adaptador e dos workers (`TestIngestEnqueueErrorDecidesWhetherTheBufferIsPooled`,
    `TestInstrumentedAbandonsEnvelopesOnAContextError`,
    `TestWorkersTaintABatchAfterATimedOutAttempt`), não contra um Redis que
    trave no meio de um `XADD`. O comportamento do cliente vem da documentação
    do rueidis ("the command is likely sent already") e do caminho de abort em
    `pipe.go`.

    Reprodução manual em 2026-09-14, `compose.memory-redis.yml` com o Redis
    pausado (`docker pause`) e o ring cheio pelo TCP: o lote em voo estourou
    `op_timeout` oito vezes e, ao despausar, o Redis executou também os `XADD`
    antigos — 34480 entradas na stream para 34462 mensagens aceitas, 18 a mais
    e nenhuma perda. É o custo at-least-once que a spec assume para o pipeline;
    a integridade dos bytes do envio tardio foi conferida no irmão, não aqui.
    Continua sem teste automatizado.
13. **O guarda de não-progresso do dreno do boot não tem teste próprio.** Ele
    só dispara quando a contabilidade e a tabela divergem sem que uma linha
    tenha sido quarentenada — um estado que nenhum caminho do código produz
    hoje. O que está provado é o caminho principal (quarentena e dreno que
    termina); o guarda é defesa em profundidade, lido, não exercitado.

14. **`tcp.max_conns` e `relp.max_conns` em `0` deixam o custo fixo por conexão
    sem teto.** Cada conexão ociosa custa cerca de 128 KiB (o primeiro bloco de
    64 KiB da moldura mais o `bufio` de 64 KiB) — 130 vezes menos que antes da
    correção de 2026-09-14, mas ainda sem limite com 65535 descritores; os
    padrões ficaram em `0` por decisão. Um primeiro bloco menor é o
    endurecimento pendente.

15. **A recusa por IP é invisível.** Nenhuma métrica ou log conta uma conexão
    recusada por `max_conns_per_ip` (o irmão tem um contador); o operador não
    distingue "ninguém conecta" de "um IP é recusado o tempo todo". Exige mudar
    a spec antes do código.

16. **O heartbeat do `sink=sqlite` espera o mutex do escritor sem contexto.**
    Um lote que dure mais que `sink.health_timeout` (1 s) faz a sonda falhar
    por prazo com o banco íntegro — um ciclo DOWN por lentidão. A sonda em si
    custa milissegundos; a janela é a do lote, e não há teste que a meça.

17. **A quarentena do `ring.db` não tem teto nem limpeza automática** (§9.4 da
    spec diz como inspecionar), e o `FuzzFrame` usa um teto de 4096 bytes que
    nunca alcança o caminho multibloco de `readCounted` — coberto só pelos dois
    round-trips de 300 KiB.

18. **A suíte de MySQL nunca rodou contra um servidor real nesta pasta.** Ela é
    a cópia adaptada da do irmão, que rodou contra MySQL 8.4, mas a tabela aqui
    é outra (dezessete colunas, `LONGTEXT`, `TINYINT`), e as consultas de
    catálogo, a sonda de privilégio e a divisão do lote por pacote só se provam
    no servidor. Na primeira sessão o Docker Desktop da máquina de
    desenvolvimento respondia 500 a toda chamada. Os comandos estão em §6.3.

19. **Os compose de MySQL e NATS não subiram.** `docker compose config` valida
    os seis; a imagem, o `nats-init` com `nats-box`, o `max_allowed_packet` de
    256 MiB e o `max_payload` de 8 MB lido de `nats/nats-server.conf` não foram
    exercitados em contêiner. O `max_payload` foi conferido no binário do
    `nats-server` com o mesmo arquivo (8 388 608 bytes).

20. **A conta do maior evento (§3.9 da spec) é pessimista e nunca foi medida
    contra tráfego real.** O fator 6 do envelope é provado como teto por
    `TestMaxBytesBoundsTheEnvelope`; quanto ele sobra na prática, e se o
    `max_payload` de 8 MB dos compose é folgado ou justo para quem sobe os
    frames, não tem número.

## 9. Histórico: bugs que os testes encontraram

Cada um destes foi escrito, falhou num teste, e foi corrigido antes de existir
fora da máquina de desenvolvimento.

- **Envelope num header NATS acima de 64 KiB.** O formato herdado do irmão
  publicava o envelope no header e a linha crua em `Data`. O JetStream grava o
  comprimento do bloco de headers em dezesseis bits e recusa qualquer um acima
  de 65 535 bytes com `10097`, um `400` — que a tabela classificava como fatal
  por ser 4xx desconhecido. Um hostname RFC 3164 longo ou alguns quilobytes de
  structured-data encerrariam o processo, e a reingestão do dump o encerraria
  de novo no boot seguinte. Achado por `TestNATSLargestAcceptedMessageFitsTheServer`
  contra um `nats-server` real (2026-09-15), antes de o destino existir fora
  desta máquina. O envelope passou para `Data`, à frente da linha crua, com o
  tamanho em `Sls-Envelope-Size` (§3.8 da spec); `10097` entrou na tabela como
  `logic`; e a checagem de boot passou a conferir também o teto de 32 MiB por
  registro do file store, cujo `10077` é fatal. O irmão tem o mesmo formato, e
  o mesmo limite latente para requisições com muitos headers.

- **Contagem dupla no backpressure.** A primeira versão do laço de retenção do
  TCP chamava a mesma função que conta a chegada. Uma mensagem segurada por
  trinta segundos virava seiscentas chegadas e seiscentos descartes. Separado em
  `Accept`, `Retry` e `RecordDrop`; `TestRetryDoesNotRecount`.
- **Confirmação RELP descartada no shutdown.** O `Close` fechava o socket direto
  e perdia `200 OK` de mensagens já no ring, que viravam duplicatas na
  retransmissão. Agora o `Close` só expira o prazo de leitura e deixa o escritor
  esvaziar as confirmações decididas.
- **Corrida no prazo de leitura.** O laço renovava o prazo de ociosidade antes de
  cada frame e podia sobrescrever o "agora" do `Close`, segurando o shutdown por
  até 120 segundos. Resolvido pela ordem das escritas em `armDeadline`.
- **Recarga que esquece.** Adotar o snapshot novo no SIGHUP deixava o segundo
  SIGHUP calado sobre uma chave que ainda exigia reinício. A correção de então
  comparava tudo contra o boot, o que criou o bug da reversão; a regra vigente,
  de duas referências, está na entrada de 2026-09-14 abaixo.
- **Caminho de socket longo demais.** Acima de `sun_path` o kernel responde só
  `bind: invalid argument`. Validado no boot com mensagem que diz o limite.
- **Três bugs do parser RFC 3164**, achados pelo corpus; ver §4.
- **syslog-ng recusando a própria configuração.** Dois destinos TCP no mesmo
  `host:porta` — o que o teste precisa, para exercitar a autodetecção de moldura
  numa porta só — recebem o mesmo nome de persistência, e o syslog-ng aborta a
  configuração depois de abrir as conexões. O servidor via as conexões e nenhuma
  mensagem. Era o teste, não o servidor; `persist-name()` explícito resolve, e o
  log do emissor agora sai junto quando o teste falha.
- **Dois erros de redação da spec**, achados ao implementar: o health check do
  Redis por `XLEN` (leitura, que uma réplica responde) e o `LIKE` sobre `raw`
  sem `CAST` (que no SQLite não casa nada, sem erro). Ambos corrigidos na spec
  antes do código.
- **`Enqueue` cancelado devolvia o buffer com a escritora ainda lendo.** A fase
  3 do shutdown cancela o contexto de recepção; todo `Enqueue` em voo no
  `ring=sqlite` voltava com `Canceled`, a recepção devolvia a linha ao pool
  enquanto a escritora ainda ia lê-la no `INSERT`, e o RELP respondia
  `500 shutdown` a uma transação cuja linha seria commitada de qualquer forma
  — duplicata garantida na retransmissão. Achado por auditoria estática contra
  §14.2 (2026-09-14). Agora o `Enqueue` espera o group commit
  independentemente do contexto (§9.4), o `ring=none` escreve num contexto
  desligado da recepção e limitado por `op_timeout` (§9.5), e os caminhos de
  erro descartam sem devolver (`Discard`/`Taint`, §14.2).
  `TestSQLiteEnqueueWaitsOutACancelledContext`,
  `TestIngestEnqueueErrorDecidesWhetherTheBufferIsPooled`,
  `TestWorkersTaintABatchAfterATimedOutAttempt`,
  `TestInstrumentedAbandonsEnvelopesOnAContextError`.
- **O frame declarado era alocado antes de chegar.** Os leitores de
  octet-counting e de RELP faziam `make([]byte, n)` com o `DATALEN` declarado,
  até 16 MiB, antes de ler um byte do payload: mil conexões que declarassem o
  teto e ficassem caladas custariam 16 GiB por nove bytes cada, com `max_conns`
  ilimitado por padrão. A spec dizia o contrário (§5.5 apresentava essa
  moldura como a segura). Achado por auditoria estática (2026-09-14). Os dois
  leitores passaram a crescer o buffer em blocos de 64 KiB conforme os bytes
  chegam; `TestFramerOctetGrowsWithTheBytesNotTheCount`,
  `TestRELPFrameGrowsWithTheBytesNotTheCount` e os dois round-trips de 300 KiB.
- **`Health()` do `sink=sqlite` varria o arquivo inteiro a cada 2 s.**
  `PRAGMA quick_check(1)` lê todas as páginas sob a trava da escritora; com
  `rotate = none` o `events.db` cresce sem limite e um dia a sonda passa de
  `health_timeout`: DOWN, FATAL e um boot que falha igual, em laço. A spec
  prometia amostragem e teste de escrita, e o código não fazia nenhum dos dois.
  Achado por auditoria estática (2026-09-14). A sonda virou um UPSERT de uma
  linha na tabela `health` (custo constante, prova escrita) e o `quick_check`
  roda só no boot; `TestSQLiteSinkHealthOnAFreshFile`,
  `TestSQLiteSinkRefusesADamagedFileAtBoot`.
- **Uma linha indecodificável no `ring.db` travava o boot para sempre.** A
  leitora pulava a linha e avançava o cursor, mas não a descontava da
  contabilidade; o dreno do boot esperava `Items == 0`, que nunca chegava, com
  todos os transportes fechados e sem código de saída (só `SIGTERM` → 130).
  Achado por auditoria estática (2026-09-14). A linha passou a ser descontada
  no ato e movida pela escritora para a tabela `quarantine` do mesmo arquivo,
  contada em `syslog_ring_quarantined_total`; o `Dequeue` devolve "progresso
  sem itens" depois de uma quarentena, e o dreno do boot ganhou um guarda de
  não-progresso (5 s → `Recount()` → `exit 3` se a divergência persistir).
  `TestSQLiteUndecodableHeadIsQuarantined`, `TestBootDrainQuarantinesAPoisonedRow`.
- **A recusa por IP chamava `Accept` recursivamente.** Um emissor reconectando
  acima de `max_conns_per_ip` em laço apertado — o caso para o qual o teto
  existe — crescia a pilha da goroutine de accept um quadro por recusa até o
  runtime abortar o processo. Achado por auditoria estática (2026-09-14).
  Virou laço, como no irmão; `TestLimitListenerRefusesWithoutRecursing` (vinte
  mil recusas com a pilha máxima reduzida a 256 KiB).
- **Os tetos de conexão eram por listener, não por transporte.** Com
  `tcp_listeners = N`, `max_conns` e `max_conns_per_ip` valiam N vezes, e um
  mesmo IP ocupava sua cota em cada listener. Um `connLimiter` por transporte
  passou a envolver todos os listeners; de quebra, um `Close` do listener solta
  um `accept` parado no teto global, que antes segurava o shutdown para sempre
  em `max_conns`. `TestConnLimitsAreSharedAcrossListeners`,
  `TestLimitListenerCloseUnblocksAccept`.
- **A recarga ignorava a reversão ao valor de boot.** Comparar só contra o
  boot — a correção do bug "recarga que esquece", acima — fez o caso oposto
  falhar: um filtro afrouxado por `SIGHUP` e revertido no arquivo voltava com
  "configuration unchanged" e continuava afrouxado, de forma intermitente
  (dependia de outra chave também diferir do boot). Achado por auditoria
  estática (2026-09-14). A recarga passou a usar duas referências: chave
  recarregável contra o que está em execução, chave de reinício contra o
  boot; `TestReloadRevertsAReloadableKeyToItsBootValue`.
- **Durações em `0` passavam pela validação.** `sink.op_timeout = 0` fazia
  toda escrita falhar no ato e ser retentada para sempre;
  `tcp.backpressure_timeout = 0` desligava o backpressure sem aviso;
  `shutdown.drain_timeout = 0` fazia todo desligamento sair com 3; outras eram
  trocadas em silêncio por um padrão que o `--dump-config` não mostrava.
  Achado por auditoria estática (2026-09-14). A validação recusa zero onde ele
  não é "sem limite" (§15.5); `TestZeroDurationsAreRejectedWhereDangerous`.
- **Métricas declaradas que nada alimentava, e razões cruzadas.**
  `kind="timeout"` nunca subia (o estouro de `op_timeout` contava como
  `network`), `syslog_confirm_duration_seconds` nunca era observado, e um
  enumerado único de `reason` expunha dez séries que nunca podiam subir entre
  `messages_dropped` e `events_dropped`; um desligamento com `ring=sqlite` e
  lotes retidos ainda contava linhas em disco como `abort` e saía com 3.
  Achado por auditoria estática (2026-09-14). `TestDropReasonsAreNotCrossed`,
  `TestConfirmDurationIsObservedOnTCPAndRELP`,
  `TestInstrumentedAbandonsEnvelopesOnAContextError`,
  `TestSQLiteRingLeftoversAreNotCountedAsLost`.
- **Senha errada do Redis esperava `sink.boot_timeout` inteiro.** O rueidis
  autentica dentro do `NewClient` e o erro voltava embrulhado sem classificação,
  como falha de rede. O erro da discagem passa pela mesma classificação de uma
  resposta e `WRONGPASS` é fatal na hora; `TestNewRedisClassifiesTheDialReply`.
- **Resposta fatal à sonda passava pelos fusíveis.** `WRONGTYPE` ou `NOAUTH`
  na sonda de escrita esperavam `max_retries` ou `fail_timeout` para virar
  FATAL; agora é na primeira checagem, e uma sonda que estoura o prazo conta
  `timeout`, não `network`; `TestHealthFatalReplyIsFatalAtOnce`,
  `TestHealthProbeTimeoutIsCountedApart`.
- **Séries mortas por transporte.** `syslog_conns_active`, `syslog_conns_total`
  e `syslog_backpressure_seconds_total` eram expostas para `udp` e `unix`, que
  não têm conexão, quando §13.1 prometia só `tcp` e `relp`.
- **`ring=none` nunca devolvia o buffer ao pool.** A tabela de §14.2 prometia a
  devolução após o `Write` síncrono e o código deixava tudo para o GC;
  `TestNoneReleasesTheItemAfterASuccessfulWrite`.
- **Dump falho contava a perda duas vezes; cancelamento do shutdown contava
  como rede.** `emergency_failed` e `abort` somavam os mesmos N, e o `Canceled`
  do próprio worker subia `kind="network"`.
- **Keep-alive não chegava ao socket sob o limitador.** A asserção
  `c.(*net.TCPConn)` falhava no `*limitConn`, então o período de 30 s nunca era
  aplicado com `max_conns` configurado; `tcpConnOf` desembrulha;
  `TestKeepAliveReachesTheSocketUnderTheLimiter`.
- **Os dois tetos juntos não tinham prova.** `TestBothCeilingsAreReleasedExactlyOnce`
  prova que o teto global e o por IP são devolvidos uma vez cada, e
  `TestConnLimitsPerIPAreSharedAcrossListeners` que o teto por IP vale entre
  listeners; `TestTCPListenersShareOneLimiter` prova a fiação.
