# go-gelf-pg-server — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Escrito
em 2026-10-31, no corte a partir do `go-gelf-server` (G) pelo molde do
`go-syslog-pg-server` (SP). Comandos a partir da pasta do módulo.

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. **Sem `GELF_TEST_PG_DSN`**, a suíte de integração da
app e a do sink real pulam (§5, §6): só sobram as recusas de boot de antes do
passo 7 e os testes de pacote. Como o PostgreSQL é o único destino, a prova
ponta a ponta exige o banco; rode §6 antes de considerar uma mudança pronta.

416 funções `Test*` e 9 alvos de fuzz em 2026-10-31.

---

## 2. Corpus dourado, contratos e oráculo (desvio D3, SPEC §18.2)

```bash
go test -race -run 'Golden|Schema|Corpus|Semantic' -v ./internal/convert/ ./internal/jlhcheck/
```

`internal/convert` e `internal/jlhcheck` são **cópias literais** de G (§17.2 da
SPEC): o corpus (`internal/convert/testdata/corpus/*.gelf` + `*.want.json`, 21
pares: driver do Docker, fluent-bit, `graypy`, `logstash-gelf`, Logstash, Go
`gelf`, GELF 1.0, sem `level`, `_id`/`null`, `level` duplicado, Latin-1,
truncado, array...), o schema `jlh-gelf-payload.schema.json` e o oráculo
semântico são os de G e passam aqui sem mudança. Uma correção neles se faz em G
e se copia.

O oráculo é usado em toda saída de teste desta pasta: dumps (`dumpLines`),
linhas da fila (`checkRow`, que tolera **só** a exceção de SPEC §3.7.1: `raw`
UTF-8 com NUL em base64 na cópia da fila), e os envelopes dos testes de sink.

---

## 3. Fuzz (SPEC §18.3, obrigatório antes de release)

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=60s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$         -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzReassemble$   -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzDecompress$   -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=60s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=60s
```

10 minutos cada antes de release; 60 s no dia a dia.

**Rodada de 2026-10-31 (60 s cada, macOS arm64, Go 1.27.2):** todos verdes.
Execuções: `FuzzConvert` 1,37 M; `FuzzHead` 5,38 M; `FuzzFrame` 1,10 M;
`FuzzReassemble` 4,02 M; `FuzzDecompress` 40 k; `FuzzProxy` 2,46 M; `FuzzRead`
51 k; `FuzzJSONBSafe` 114 k; `FuzzInboxPayload` 64 k (os de `emergency` e
`sink` rodaram junto com o build da imagem). O `FuzzInboxPayload` achou, aos
45 s, um documento vazio no arnês (o conversor nunca recebe um, §5.6 da SPEC:
a recepção o descarta como `empty`); o arnês passou a pular documento só de
espaço, a entrada ficou em `internal/sink/testdata/fuzz/FuzzInboxPayload/` e a
rodada de 60 s seguinte passou. O arnês de G tem a mesma lacuna (§9).

---

## 4. Benchmarks

Os de G vêm com os pacotes copiados (`BenchmarkConvert*`, `BenchmarkLevelOf`,
os de `receiver`), e o `BenchmarkIngestAccept` de SP adaptado. **Nenhum número
foi registrado nesta rodada**; os alvos de SPEC §18.8 nunca foram medidos (§8).

---

## 5. Integração em processo (`internal/app`)

A pilha real (`runContext`, o mesmo `run()` do binário) com uma fila
`loghub.inbox` por teste, criada de `docker/postgres/inbox.sql` num esquema
próprio. Pula sem `GELF_TEST_PG_DSN`, exceto as recusas de boot (exit 1).

| Teste | Prova |
|---|---|
| `TestAllTransportsReachTheQueue` | UDP (wildcard, `dst-ip` por PKTINFO), UDP gzip em 3 chunks, TCP `\0`, HTTP identidade e gzip: colunas `from`/`space`/`type=gelf`/`step`/`level`, `event_at` = recepção, `sid` ≠ 0, `compression`, `chunks`, `size` descomprimido |
| `TestProxyProtocolFromATrustedPeer` | PROXY v1, v2 e ausente com `optional`; `x-proxy`; contadores |
| `TestTheQueueRowIsFaithful` | NUL em `short-message` e numa chave de `fields` vira U+FFFD na fila; `1.50` e `1e400` com o valor exato (o `jsonb` guarda `1.50` e o `1e400` por extenso); `extra`; omissão; `level-default`; malformado com NUL cru em base64 e malformado simples em texto |
| `TestSIGHUPReload` e os de `signals_test.go` | `[log]` ida e volta, chave de restart relatada, arquivo inválido, `SIGHUP` fora da janela, o binário real |
| `TestEmergencyDumpAndReingestionAreExact` e `TestDumpIsReingestedEvenWithEmergencyDisabled` | o dump JSON Lines com cinco documentos (`1e400`, GELF 1.0, malformado com NUL cru por HTTP, `\u0000`, Latin-1), exit 3, reingestão por outra identidade sem conversão, remoção |
| `TestDrainTimeoutDumpsAndExitsThree`, `TestMalformedIsLeftOutOfTheDump` | dreno estourado, `accept_malformed=false` no dump |
| `TestSinkDownRefusesPerTransport`, `TestFullRingPerTransport` | UDP descarta e conta; HTTP `503` + `Retry-After` e conta; TCP segura sem aceitar nem descartar e entrega depois |
| `TestKeepRawDecidesWhereRawGoes` | `keep_raw` `malformed` e `always`; UTF-8 inválido é malformado com `raw` em base64 |
| `TestTruncationKeepsTheFramingInSync` | (o `TestTooLargeKeepsTheFramingInSync` de G) acima de `max_message_bytes` é `too_large`, nunca truncado; a conexão segue |
| `TestEmptyMessagesAreCountedNotWritten` | datagrama vazio e `POST` só de espaço contam `empty` (`400` no HTTP); quadro vazio no TCP é separador |
| `TestAdminSurface` | `/healthz`, `/readyz`, os conjuntos fechados de label de `/metrics` (transportes `udp,tcp,http`, os 11 motivos de descarte, os 25 avisos) |
| `TestPostgresEndToEnd` | o tráfego do `sqlLines` de G na fila: NUL no host e na mensagem, número de 20 dígitos exato, chave duplicada com `raw`, dois malformados |
| `TestBootRefusalsExitOne`, `TestMoreBootRefusalsExitOne`, `TestBinaryVersionAndConfigErrors` | as chaves de G que saíram (TLS, HTTPS, `[tls]`, filtro, rate-limit, `sink.type`, `ring.mode`, beacon, `sink.postgres.tls`), `listen.tcp` = `listen.http`, `convert.*` fora de faixa, `LOGHUB_EB_URL` com TLS |
| `port_*`, `review_s_test.go`, `recovery_test.go`, `lifecycle_test.go` | os portes de SP (aborto, `SIGQUIT`, sinal antes do *bind*, reingestão em fluxo, dumps danificados, FATAL), com documentos GELF |

---

## 6. PostgreSQL (`loghub.inbox`) e emissores reais

A infraestrutura compartilhada do monorepo (`tasks/infra/`, fora do git):

```bash
bash ../tasks/infra/up.sh        # se não estiver de pé (PostgreSQL 18 em 127.0.0.1:25432, entre outros)
source ../tasks/infra/env.sh     # GELF_TEST_PG_DSN, GELF_TEST_EMITTERS=1
go test -race -count=1 -timeout 1200s ./...
```

Nunca rode `down.sh` enquanto outro módulo pode estar testando contra ela.

**Sink real (`internal/sink`)**: classificação de SQLSTATE contra o servidor,
validação de tabela/tipos/índice/permissão, `pg_is_in_recovery`, `PGSSLMODE`
ignorado (sessão sem SSL em `pg_stat_ssl`), a sonda com uma conexão sobrando,
reingestão por `Deliver` igual à escrita dos workers, e
`TestPostgresNumbersJSONBCannotHoldDoNotDropTheChunk`: um documento com
`_huge: 1e1000000` no meio de um pedaço não derruba os vizinhos (SPEC §3.7.1).
As bordas do `numeric` (`1e131071` sim, `1e131072` não; `1e-16383` sim,
`1e-16384` e `0.5e-16383` não) foram conferidas à mão contra o PostgreSQL 18
real antes da regra.

**Emissores reais (SPEC §18.7)**: `GELF_TEST_EMITTERS=1` roda
`internal/app/emitters_test.go` contra a pilha em processo e a fila real:

- `TestEmittersFluentBit`: `fluent/fluent-bit:4.0.3`, saída `gelf` UDP com
  `Compress On` e `Packet_Size 1420` (registro de 12 000 caracteres aleatórios:
  vários chunks gzip) e TCP; cinco de cada, cada um uma vez, inteiro, pelo
  transporte certo, `level` e `fields` do emissor, o `blob` com 12 000
  caracteres.
- `TestEmittersDockerLogDriver`: `docker run --log-driver=gelf` (`alpine:3.22`)
  com `gelf-compression-type` `gzip`, `zlib` e `none` por UDP e
  `gelf-address=tcp://`; três linhas por caso, `container_name` e `tag` em
  `fields`, `compression` correta.

**Rodada de 2026-10-31** (macOS arm64, Docker Desktop): as duas passaram, e a
suíte inteira com `-race` e os serviços reais ficou verde.

**Imagem e compose** (rodada manual de 2026-10-31): `docker build -f
docker/Dockerfile -t tmsoftbrasil/go-gelf-pg-server:dev .` (28,5 MB);
`docker compose -f docker-compose.yml -f compose.demo.yml --profile demo up -d`
subiu servidor, PostgreSQL 18 com a fila e o fluent-bit 4.1.1; na fila,
`udp|gzip|7` chunks e `tcp` do fluent-bit, o driver do Docker por UDP gzip e
TCP nas portas publicadas (12301), um `POST /gelf` com `202` (12302); `down -v`
no fim.

---

## 7. Verificação independente

Não houve revisão independente desta rodada (o corte foi feito e verificado
pelo mesmo agente). Os pacotes copiados de G carregam a revisão de G; o código
de corte carrega as de SP.

---

## 8. Lacunas conhecidas

1. **Desempenho nunca medido** (SPEC §18.8), como em G e SP.
2. **A saída `http` do fluent-bit ao vivo** não é testada: o `http.bulk`
   (SPEC §4.5.1) é provado pelos testes de `receiver` (`TestHTTPBulk`), não por
   um fluent-bit rodando contra este servidor.
3. **Logstash, `graypy`, `logstash-gelf` e a biblioteca Go `gelf` ao vivo**:
   só pelo corpus (bytes capturados ou montados), não por um emissor rodando.
4. **Mutação**: nenhuma rodada de mutação foi feita sobre o código de corte
   desta pasta (as de SP valem para o código que veio de lá).
5. **Contadores da recepção sem *padding*** (herdado de SP): lacuna de medição.
6. **Rede real entre hosts**: tudo rodou em loopback e na rede do Docker
   Desktop; PKTINFO foi provado no macOS.

---

## 9. Histórico

### Corte de 2026-10-31

- Merge de três vias por arquivo (base S, SP, G, normalizados para os nomes
  GELF), cópia literal dos pacotes GELF de G, conflitos à mão. Os testes de G
  que dependiam do que saiu (TLS, HTTPS, filtro, rate-limit, `ring=none`,
  `Consumed`, outros sinks) foram removidos; os de SP que dependiam do syslog
  (moldura LF/octet, unix, truncamento, charset) também; os portes de SP foram
  traduzidos para documentos GELF.
- **Achado e corrigido aqui, a portar para G:** um número de `fields`/`extra`
  fora do `numeric` derrubava o pedaço inteiro na `loghub.inbox` (`22003`). A
  cópia da fila o leva como string (SPEC §3.7.1); o `ParseLine` do dump aceita
  qualquer número da gramática JSON, porque a cópia resolve o resto.
- **Achado pelo fuzz e corrigido no arnês, a portar para G:** o
  `FuzzInboxPayload` alimentava o conversor com documento vazio (§3).

### Porte das correções de G (2026-10-31)

- Copiados de G (só o module path muda): `internal/convert` (corpus com o
  `fluent-bit-gelf.gelf` real e o antigo como `nested-field-object.gelf`,
  golden por bytes), `internal/jlhcheck`, `receiver/{chunks,decompress}.go` e
  `chunks_test.go`; `receiver/http.go` é o de G sem TLS (keep-alive, prazo de
  escrita contado da resposta, `http.bulk`). Testes novos em
  `internal/receiver/fix_test.go` (os de G, todos aplicáveis ao corte),
  `TestHTTPAccepts` com keep-alive, `TestHTTPSection` (config),
  `TestEmptyDocumentIsRefusedBeforeTheConverter` (`sink/workers_test.go`: o
  `Instrumented` daqui não tem o `Write` de G), as linhas de mantissa zero em
  `TestInboxPayloadCarriesNumbersJSONBCannotHoldAsText` e
  `TestJSONBHoldsAgreesWithPostgres` (PostgreSQL 18 real).
- `context.Canceled` do `Enqueue` passou a ser `Shutdown` (como o GN); o
  `blockRing` de `http_test.go` devolve `ctx.Err()` e prova o ramo.
- **Mutação** (cada uma falha e foi restaurada): lápide da mensagem aberta
  (`kill`→`remove`) e da nova; keep-alive desligado; o `Connection: close` de
  `max_requests_per_conn`; o ramo `context.Canceled`; `ItemOverhead` 256; a
  guarda do documento vazio; o `zero` de `jsonbHolds`. O desarme do prazo de
  escrita e o `armWrite` **juntos** fazem `TestHTTPSlowBodyStillGetsItsAnswer`
  falhar; cada metade sozinha sobrevive (sem desarme, o `armWrite` rearma
  antes da resposta; sem `armWrite`, não há prazo) — são redundantes para esse
  teste, e o `armWrite` sozinho não é observável sem um cliente que não lê.
- Suíte `-race` completa verde com a infra (`tasks/infra/env.sh`); fuzz 60 s
  verde: `FuzzConvert` 475 k, `FuzzReassemble` 2,7 M, `FuzzDecompress` 842 k,
  `FuzzRead` 15 k, `FuzzInboxPayload` e `FuzzJSONBSafe` 121 k.
