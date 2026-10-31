# go-gelf-server — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Os
comandos são a partir da pasta do módulo. Datas e máquina de cada número
estão junto dele; um número sem data não existe.

Este servidor é uma cópia do `go-syslog-server` (S) adaptada ao GELF: as suítes
de ring, sink, beacon, dump, admin e configuração vieram de lá com os nomes
trocados (`SYSLOG_TEST_*` → `GELF_TEST_*`, `syslog_` → `gelf_`) e os casos de
syslog reescritos para documentos GELF. O que lá estava provado sobre essas
peças vale como ponto de partida, não como prova daqui: só conta o que rodou
neste módulo (§6.6, §6.7).

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional: a remontagem UDP (§5.2 da SPEC) é estado
compartilhado entre listeners, e os bugs de perda de dados vivem em caminhos
concorrentes. Sem variáveis de ambiente, as suítes de destino real pulam (§6).

Cobertura: `go test -cover ./internal/...` (sem destinos reais) ou
`-coverpkg=./internal/...` com eles. A tabela por pacote ainda não foi medida
neste módulo (lacuna 1).

---

## 2. Corpus dourado, contratos e oráculo (desvio D3, SPEC §18.2)

```bash
go test -race -run 'Corpus|Schema|Semantic|Golden' -v ./internal/convert/ ./internal/jlhcheck/
```

- **O schema do payload é deste repositório**:
  `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json` (não existe skill
  `type=gelf`, decisão 1). O do envelope e os exemplos `valid/`/`invalid/` são
  cópias da skill `loghub-json-schema`; os `valid/` passam e os `invalid/`
  falham, para provar que o validador está ligado.
- **O oráculo** (`internal/jlhcheck`, `Validate`) é os dois schemas
  (`santhosh-tekuri/jsonschema/v6`, só em teste, `AssertFormat` ligado) mais as
  regras que o schema não expressa (SPEC §18.2): chaves duplicadas, ordem e
  número de chaves, `level` = `payload.level.code` e o nome RFC 5424, avisos
  na ordem canônica e sem repetição (a ordem é duplicada no oráculo de
  propósito), `raw` base64 que decodifica e `raw` UTF-8 nunca em base64,
  `size` = octetos de `raw`, `host.hostname` sem nenhum branco do conjunto de
  D5, `sid` recalculado, `event.iso`, instante do `uuid` = `event`.
- **Corpus** (`internal/convert/testdata/corpus/*.gelf` + `*.want.json`, 15
  pares, o mínimo que o teste aceita): driver GELF do Docker, fluent-bit,
  exemplo da spec do Graylog, `graypy` com traceback, `logstash-gelf`, saída
  `gelf` do Logstash, biblioteca Go `gelf`, GELF 1.0 com `file`/`line`, sem
  `level`, `message` no lugar de `short_message`, `_id` e `null`, `level`
  duplicado, bytes Latin-1, documento truncado e documento array. Cada saída é
  comparada byte a byte e passa no oráculo.

Para regravar o esperado depois de uma mudança **deliberada** da conversão:

```bash
GELF_UPDATE_GOLDEN=1 go test -run Corpus ./internal/convert/
```

e revise o diff dos `.want.json` à mão antes de aceitá-lo. Um `.want.json`
regravado sem revisão transforma o corpus em espelho do bug.

Outros testes de `convert` que valem citar: `TestNumbersAreText` (`1e400`,
`0.1`, `-0`, inteiros acima de 2⁵³ passam como texto), `TestLevelOfMatchesConvert`
(o `level` filtrado na recepção é o emitido), `TestPayloadKeyOrder`,
`TestDeterministic` (mesmo cabeçalho + documento = mesmos bytes) e
`TestWorstCaseJLHIsBounded` (o teto `6 × L + 8 KiB` de SPEC §3.9 contra
documentos adversários: malformado de controles, coerção aninhada, `raw`
forçado).

---

## 3. Fuzz (SPEC §18.3, obrigatório antes de release)

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzReassemble$   -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzDecompress$   -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=600s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$         -fuzztime=600s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzChPayload$    -fuzztime=600s
```

O `^…$` importa: o Go recusa um `-fuzz` que case dois alvos do mesmo pacote.
No dia a dia, `-fuzztime=60s`.

| Alvo | O que prova |
|---|---|
| `FuzzConvert` | a conversão nunca falha nem entra em pânico; toda saída passa no oráculo; o `LevelOf` da recepção é o `level` do envelope; `size` é o documento; o pior JLH de §3.9 vale |
| `FuzzFrame` | a moldura `\0` com qualquer fluxo, qualquer partição das leituras e qualquer teto: nunca um `\0` num documento, nunca um documento acima do teto, e a sequência de documentos e recusas é a de um oráculo independente |
| `FuzzReassemble` | a remontagem de chunks com várias origens, ids, datagramas sem chunk e passos de relógio: nunca passa dos tetos, toda mensagem completa é a concatenação na ordem de `seq`, e uma varredura no futuro devolve todo byte |
| `FuzzDecompress` | qualquer byte como gzip, zlib, deflate ou cru: nunca mais que `L` bytes de saída (a bomba de descompressão), só os erros `too_large` e `decompress`, e o que a stdlib comprimiu volta igual |
| `FuzzProxy` | PROXY v1/v2: nunca consome além do cabeçalho declarado, nunca aloca pelo declarado |
| `FuzzHead` | o codec do cabeçalho do item (com `compression` e `chunks`) recusa sem pânico, e o que aceita volta igual |
| `FuzzRead` | o leitor do dump nunca confia no comprimento declarado |
| `FuzzJSONBSafe` | a troca do `\u0000` para a `loghub.inbox` só troca escapes reais, e o JSON decodifica igual com U+FFFD |
| `FuzzChPayload` | a citação de `fields`/`extra` do ClickHouse por reescrita de bytes contra a versão com `encoding/json` (mesmos valores, mesma ordem de chaves) |
| `FuzzInboxPayload` | o caminho real da `loghub.inbox`: JSON sem escape NUL, `raw` exato em base64, os demais textos (chaves e valores de `fields`/`extra` inclusive) com U+FFFD, a regra de colisão de chaves determinística |

Nenhuma rodada de fuzz deste módulo está registrada aqui ainda: a rodada de
60 s por alvo é da verificação final da entrega (lacuna 2), e a de 10 min por
alvo, de antes de um release.

---

## 4. Benchmarks

```bash
go test -run XXX -bench . -benchmem ./internal/convert ./internal/ratelimit ./internal/receiver
```

| Benchmark | Mede |
|---|---|
| `BenchmarkConvertDocker` | conversão + JLH completo de um documento do driver do Docker, no buffer do pedaço |
| `BenchmarkLevelOf` | a varredura do `level` na recepção, que só roda com o filtro ativo (o custo de validar o documento duas vezes, DECISOES §3) |
| `BenchmarkIngestAccept` | recepção até o ring, sem moldura |
| `BenchmarkTCPReceive` | socket real + moldura `\0` + pipeline até o ring de memória |
| `BenchmarkFramer12K` | quadros de 12 KiB pelo leitor de 4 KiB (as classes do buffer de quadro, SPEC §5.3) |
| `BenchmarkAllowSameKey`, `BenchmarkAllowSpreadKeys` | rate-limit, um emissor e emissores espalhados |

Os números ainda não foram registrados neste módulo (lacuna 3). Não há
benchmark da remontagem de chunks nem do servidor HTTP (lacuna 4).

---

## 5. Integração em processo (`internal/app`)

```bash
go test -race ./internal/app/        # a pilha inteira, sinks locais
go test -race -short ./internal/app/ # pula o teste do binário real
```

A app inteira sobe em processo (identidade fixa, `DATADIR` temporário, portas
efêmeras), e todo envelope lido de volta passa pelo oráculo; as colunas de
`sink=sqlite` são conferidas contra o payload (`NULL` exatamente quando o campo
falta). Cobre os cinco transportes (UDP inteiro e em chunks, comprimido ou não;
TCP `\0`; TLS com e sem certificado de cliente; HTTP e HTTPS com `202`, `4xx` e
`503`), PROXY v1 e v2, fidelidade do `raw`, filtro e `SIGHUP`, dump de
emergência e reingestão exatos, shutdown gracioso sem perda, segundo sinal
(130), as recusas de boot, saturação por transporte, `/readyz` e `/metrics`, e
o binário real (`TestBinaryIdentityExitCodes`, códigos 100–114).

Os testes de `internal/app` são o porte dos de S, adaptado em 2026-10-31; o
resultado da rodada completa entra em §6.7 quando ela rodar.

---

## 6. Destinos externos

Todas as suítes pulam sem a variável. Cada teste cria e remove o que usa
(tabela, stream, chave); os contêineres são descartáveis.

### 6.1 A infraestrutura compartilhada (`tasks/infra`)

Os serviços da rodada são os do monorepo (`tasks/infra/up.sh`, nomes `lhp-*`,
portas 2xxxx; `tasks/infra/down.sh` remove). O `tasks/infra/env.sh` exporta as
`GELF_TEST_*` (desde 2026-10-31), inclusive `GELF_TEST_NATS_PERM_URL` no
servidor de permissões compartilhado `:24224`, cujo `perm.conf` (gerado pelo
`up.sh`) nega `loghub.*.gelf` ao usuário `nopub`:

```bash
source ../tasks/infra/env.sh
go test -race -count=1 -timeout 1200s ./...
```

Se o `perm.conf` de `tasks/infra/nats` for anterior à regra (sem
`loghub.*.gelf` no `deny` do `nopub`), `TestNATSPermissionDeniedIsFatal` falha
com `Write of a denied publish = <nil>`: regenere-o como o `up.sh` faz e
reinicie só o `nats-server` de permissões (§6.4).

| Variável | Para quê |
|---|---|
| `GELF_TEST_REDIS_ADDR` | `ring=redis`, `sink=redis`, `rediscli` |
| `GELF_TEST_REDIS_LRU_ADDR` | um segundo Redis com `allkeys-lru`: a recusa FATAL de §9.5 e §10.3, sem criar chave. À parte porque o primeiro é compartilhado por todos os pacotes em paralelo |
| `GELF_TEST_PG_DSN` | `sink=postgres` (`loghub.inbox`, PostgreSQL 18) |
| `GELF_TEST_MYSQL_DSN` | `sink=mysql` (`gelf_events`, MySQL 8.4) |
| `GELF_TEST_CLICKHOUSE_DSN` | `sink=clickhouse` (`inbox_gelf`, ClickHouse 25.8, protocolo nativo; com `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1` os subtestes de privilégio e memória criam usuários) |
| `GELF_TEST_NATS_URL` | `sink=nats` com JetStream, beacon |
| `GELF_TEST_NATS_NOJS_URL` | um `nats-server` **sem** JetStream (FATAL na hora) |
| `GELF_TEST_NATS_PERM_URL` | um `nats-server` com usuários de permissões limitadas (§6.4) |
| `GELF_TEST_NATS_SERVER_BIN` | o binário `nats-server` 2.14.6, para a suíte subir os próprios servidores com `tls {}` |
| `GELF_TEST_EMITTERS` | `1` liga a suíte de emissores reais (§6.5) |

Sem a infraestrutura do monorepo, cada serviço sobe sozinho, por exemplo:

```bash
docker run --rm -d -p 16379:6379 --name gls-test-redis redis:7-alpine
docker run --rm -d -p 15432:5432 -e POSTGRES_USER=gls -e POSTGRES_PASSWORD=gls \
  -e POSTGRES_DB=gls_test --name gls-sink-pg postgres:18-alpine
docker run --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=gls -e MYSQL_DATABASE=gls_test \
  --name gls-sink-mysql mysql:8.4
docker run --rm -d -p 19000:9000 -e CLICKHOUSE_USER=gls -e CLICKHOUSE_PASSWORD=gls \
  -e CLICKHOUSE_DB=gls_test -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --ulimit nofile=262144:262144 --name gls-sink-clickhouse clickhouse/clickhouse-server:25.8
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &   # SEM JetStream
GELF_TEST_REDIS_ADDR=127.0.0.1:16379 \
GELF_TEST_PG_DSN='postgres://gls:gls@127.0.0.1:15432/gls_test?sslmode=disable' \
GELF_TEST_MYSQL_DSN='root:gls@tcp(127.0.0.1:13306)/gls_test' \
GELF_TEST_CLICKHOUSE_DSN='clickhouse://gls:gls@127.0.0.1:19000/gls_test' \
GELF_TEST_NATS_URL=nats://127.0.0.1:14222 GELF_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 1200s ./...
```

### 6.2 O que cada destino prova

- **Redis:** o script de enfileiramento com os dois tetos globais, o `Commit`
  que só decrementa o que o `XDEL` removeu, a releitura dos próprios
  pendentes, `XAUTOCLAIM` de réplica morta, `Touch`, `Quiesce`,
  disponibilidade pela escrita, Redis Cluster sem *hash tag* recusado, e na
  app duas instâncias num ring (cada documento uma vez, contador em zero).
- **PostgreSQL (`loghub.inbox`):** a DDL de `docker/postgres/inbox.sql`
  (hash da skill conferido sem servidor), o `unnest` de dez arrays,
  `ON CONFLICT (uuid)`, o envelope reconstruído da linha igual ao JLH e válido
  no oráculo, a cópia NUL (`raw` em base64, U+FFFD em chaves e valores de
  `fields`/`extra`, a regra de colisão; o `raw` reconvertido antes do oráculo,
  SPEC §3.7.1), a classificação por SQLSTATE e toda a validação do boot.
- **MySQL (`gelf_events`, 13 colunas):** `ON DUPLICATE KEY UPDATE`, a divisão
  em comandos (no máximo **5 041 linhas**, 13 marcadores por linha, e 7/8 do
  `max_allowed_packet`, com a linha estimada em `env` + `hostname` + 512 B), a
  sonda revertida de duas linhas (a `udp` com `hostname` `NULL` e a de
  extremos, `transport` `https`), `read_only` fatal no boot, entrega parcial.
- **NATS:** o subject exato `loghub.<space>.gelf` (não há granular),
  `Nats-Msg-Id = <uuid>.0` e os `X-LogHub-*`, o JLH só em `Data`, a validação
  do stream (cobre o subject, não captura o plano de controle, não selado,
  `no_ack` recusado, maior evento cabe, `retention: interest` recusado),
  JetStream desligado fatal, entrega parcial (`10054`), beacon, queda e volta
  sem perda, FATAL → dump → reingestão.
- **ClickHouse (`inbox_gelf`):** a ida e volta pelo oráculo com `{}` =
  ausente, os valores de `fields`/`extra` **como texto exato** (`1.50`,
  `1e400`, `12345678901234567890`, objetos e arrays de `extra`; o sink os cita,
  `chPayload`, porque os `input_format_json_read_*_as_strings` do 25.8 reescrevem
  `1.50` como `1.5` e recusam `1e400` com `117`), nenhuma string lida como data,
  um JLH grande, o token descartando o retry, uma tabela recusada no boot por
  regra (a sem os *hints* `Map` inclusive), privilégio, `readonly`, memória.

### 6.3 ClickHouse e PostgreSQL: o que não é provado

O FATAL no boot de um PostgreSQL em recuperação (`pg_is_in_recovery()`) exige
uma réplica de verdade; o `242`/`system.replicas.is_readonly` do ClickHouse
exige um Keeper; TLS do ClickHouse e os perfis de `async_insert` não são
exercitados (como em S).

### 6.4 NATS com permissões

Um terceiro `nats-server`, com JetStream e usuários: o `:24224` de
`tasks/infra`, cujo `perm.conf` nega `loghub.*.webhook`, `loghub.*.syslog` e
`loghub.*.gelf` ao `nopub` (o `env.sh` já aponta `GELF_TEST_NATS_PERM_URL` para
ele). Sem a infraestrutura do monorepo, um servidor próprio com a mesma regra (a
conf está também no comentário de `natsPermSetup`,
`internal/sink/review_nats_test.go`):

```bash
mkdir -p /tmp/gls-nats-perm
cat > /tmp/gls-nats-perm.conf <<'EOF'
listen: 127.0.0.1:24234
jetstream { store_dir: "/tmp/gls-nats-perm" }
authorization { users = [
  { user: admin, password: admin }
  { user: nopub, password: nopub, permissions: {
      publish: { allow: [">"], deny: ["loghub.*.gelf"] },
      subscribe: { allow: [">"] } } }
  { user: noinbox, password: noinbox, permissions: {
      publish: { allow: [">"] }, subscribe: { deny: ["_INBOX.>"] } } }
  { user: nobeacon, password: nobeacon, permissions: {
      publish: { allow: [">"], deny: ["*.beacon"] }, subscribe: { allow: [">"] } } }
] }
EOF
nats-server -c /tmp/gls-nats-perm.conf &
GELF_TEST_NATS_PERM_URL=nats://127.0.0.1:24234 go test -race ./internal/sink/ -run 'Permission|RefusedCredentials'
```

A URL vai **sem** usuário; a suíte usa `admin`, `nopub`, `noinbox` e
`nobeacon` (senha = usuário). Prova a negação do subject de dados como veredito
FATAL (`jetstream`: depois do `sink.op_timeout`; `core`: na hora), a da
assinatura `_INBOX`, a do beacon **sem** veredito, e senha errada no primeiro
`CONNECT` como FATAL sem esperar o `boot_timeout`.

**TLS de verdade, num pool:** com `GELF_TEST_NATS_SERVER_BIN` a suíte sobe os
próprios `nats-server` com `tls {}` (portas 14700–14799, duas CAs geradas no
teste): `go test -race -run 'TLSPool' ./internal/sink/`.

### 6.5 Emissores reais (SPEC §18.7)

**Automática:**

```bash
GELF_TEST_EMITTERS=1 go test -race -run Emitters -timeout 600s ./internal/app/
```

Precisa de Docker. Sobe o fluent-bit (`fluent/fluent-bit:4.0.3`) contra a pilha
em processo, com as saídas `gelf` (UDP com chunks e gzip, TCP, TLS) e `http`.

**Manual, com a imagem** (o driver de log do Docker não roda dentro de um
teste nem de um compose: é o daemon que manda o log do próprio contêiner). Com
a imagem construída (`docker build -f docker/Dockerfile -t
tmsoftbrasil/go-gelf-server:dev .`):

```bash
D=$(mktemp -d); chmod 777 "$D"
docker network create gelf-e2e
docker run -d --name gelf-e2e-server --network gelf-e2e --network-alias gelf --hostname dev-mac \
  -e GELF_SINK_TYPE=sqlite -e GELF_LISTEN_UDP=0.0.0.0:12201 -e GELF_LISTEN_TCP=0.0.0.0:12201 \
  -e GELF_LISTEN_HTTP=0.0.0.0:12202 -e GELF_ADMIN_BIND=0.0.0.0:9104 \
  -v "$D":/data -p 12201:12201/udp -p 12201:12201/tcp -p 12202:12202 -p 127.0.0.1:9104:9104 \
  tmsoftbrasil/go-gelf-server:dev

# fluent-bit: a configuração do compose de demonstração (docker/fluent-bit/)
docker run -d --name gelf-e2e-fluentbit --network gelf-e2e -e GELF_DEMO_HOST=gelf -e GELF_DEMO_INTERVAL=2 \
  -v "$PWD/docker/fluent-bit/fluent-bit.conf:/fluent-bit/etc/gelf-demo.conf:ro" \
  -v "$PWD/docker/fluent-bit/gelf-demo.lua:/fluent-bit/etc/gelf-demo.lua:ro" \
  fluent/fluent-bit:4.1.1 /fluent-bit/bin/fluent-bit -c /fluent-bit/etc/gelf-demo.conf
sleep 13; docker stop gelf-e2e-fluentbit

# o driver de log do Docker: o daemon manda para a porta publicada
docker run --rm --log-driver=gelf --log-opt gelf-address=udp://127.0.0.1:12201 alpine:3.22 echo hello-gelf
docker run --rm --log-driver=gelf --log-opt gelf-address=udp://127.0.0.1:12201 \
  --log-opt gelf-compression-type=zlib alpine:3.22 echo hello-gelf-zlib
docker run --rm --log-driver=gelf --log-opt gelf-address=tcp://127.0.0.1:12201 alpine:3.22 echo hello-gelf-tcp

sqlite3 -readonly "$D/events.db" "select transport, json_extract(env,'$.payload.compression'),
  json_extract(env,'$.payload.chunks'), json_extract(env,'$.payload.size') from gelf_events"
docker rm -f gelf-e2e-fluentbit gelf-e2e-server; docker network rm gelf-e2e
```

Para passar os envelopes pelo oráculo, exporte a coluna `env` de cada linha
para um arquivo (`select writefile('<dir>/'||uuid||'.json', env) from
gelf_events`) e rode `jlhcheck.Validate` em cada um, por um teste descartável
dentro do módulo (o oráculo é `internal`, e não se importa de fora).

#### Rodada de 2026-10-31 (macOS, Docker Desktop 29.9.2, imagem `dev`)

Imagem `tmsoftbrasil/go-gelf-server:dev` construída do `docker/Dockerfile`
(43,8 MB), `ring=memory`, `sink=sqlite`, `AGENT_NAME=go-gelf-server`,
`HOSTNAME=dev-mac`:

| Emissor | Transporte | Linhas | O que chegou |
|---|---|---|---|
| fluent-bit 4.1.1, saída `gelf` `Mode udp`, `Compress On`, `Packet_Size 1420`, registro com `blob` de 12 000 caracteres aleatórios | UDP | 6 | `transport: "udp"`, `compression: "gzip"`, **`chunks: 7`**, `size: 12163` (cada uma) |
| fluent-bit 4.1.1, saída `gelf` `Mode tcp` | TCP | 6 | `transport: "tcp"`, sem `compression`/`chunks`, `size: 138` |
| driver `gelf` do Docker, padrão (gzip) | UDP | 2 (`127.0.0.1` e `host.docker.internal`) | `compression: "gzip"`, sem `chunks` (um datagrama), `host.hostname: "docker-desktop"`, `fields` com `command`, `container_id`, `container_name`, `created`, `image_id`, `image_name`, `tag` |
| driver `gelf`, `gelf-compression-type=gzip` | UDP | 1 | `compression: "gzip"` |
| driver `gelf`, `gelf-compression-type=zlib` | UDP | 1 | `compression: "zlib"` |
| driver `gelf`, `gelf-compression-type=none` | UDP | 1 | sem `compression` |
| driver `gelf`, `gelf-address=tcp://` | TCP | 1 | `transport: "tcp"` |
| `curl` com `Content-Encoding: gzip` (`POST /gelf`) | HTTP | 1 | `202`; `transport: "http"`, `compression: "gzip"`, `fields.n: 1.50` (o texto do número preservado); `GET /gelf` deu `405` |

**Oráculo:** as 18 linhas de emissor real (fluent-bit e driver do Docker)
exportadas e passadas por `jlhcheck.Validate` por um teste descartável
(`internal/jlhcheck/zz_e2e_tmp_test.go`, apagado depois): **18/18 válidas**.
`/metrics` no fim: `gelf_messages_total{transport="udp"} 11`,
`{transport="tcp"} 7`, `gelf_events_written_total 17` antes do `curl`,
nenhum `gelf_messages_dropped_total` acima de zero.

Payload de amostra (fluent-bit UDP, `blob` cortado aqui):

```json
{"host":{"hostname":"fluent-bit-demo","src-ip":"172.20.0.3","src-port":54250,
 "dst-ip":"172.20.0.2","dst-port":12201},"transport":"udp","compression":"gzip",
 "chunks":7,"format":"gelf","gelf-version":"1.1","level":{"code":6,"name":"info"},
 "timestamp":"2026-10-31T17:03:08.718Z","short-message":"fluent-bit udp chunked gzip demo",
 "fields":{"blob":"Ugp1xrAUkZlku9+Sy7RJ…","origin":"udp"},"size":12163}
```

E a do driver do Docker:

```json
{"host":{"hostname":"docker-desktop","src-ip":"192.168.65.1","src-port":26536,
 "dst-ip":"172.20.0.2","dst-port":12201},"transport":"udp","compression":"gzip",
 "format":"gelf","gelf-version":"1.1","level":{"code":6,"name":"info"},
 "timestamp":"2026-10-31T17:03:35.847Z","short-message":"hello-gelf-hdi",
 "fields":{"command":"echo hello-gelf-hdi","container_id":"77b3a371…",
 "container_name":"jolly_ptolemy","created":"2026-10-31T17:03:35.741204212Z",
 "image_id":"sha256:2e0b81d6…","image_name":"alpine:3.22","tag":"77b3a371aa4e"},"size":441}
```

**O compose de demonstração**, na mesma rodada:
`docker compose -f compose.memory-sqlite.yml -f compose.demo.yml --profile demo
up -d --no-build` com `GELF_IMAGE=tmsoftbrasil/go-gelf-server GELF_IMAGE_TAG=dev`
subiu `healthy` (raiz somente leitura, `conf/gelf.ini` montado); em 25 s,
`gelf_messages_total{transport="udp"} 12`, `{transport="tcp"} 12`,
`gelf_events_written_total 24`; `docker kill -s HUP` respondeu
`SIGHUP: configuration unchanged`; os dois `WARN` esperados do boot (rate-limit
desligado fora do loopback, `SO_RCVBUF` do Docker Desktop) e nenhum outro.
Os 23 `compose.<ring>-<sink>.yml`, o `docker-compose.yml` e as combinações com
`compose.demo.yml` passam em `docker compose config -q`.

O que esta rodada **não** cobriu: TLS e HTTPS com emissor real (só a suíte
automática), o driver do Docker com mensagem grande o bastante para chunks
(ele fatia em 1420 bytes, como o fluent-bit), e Logstash/graypy no fio (só no
corpus, como documentos).

### 6.6 Resultado da rodada completa

Ainda não registrada: a verificação final da entrega (todas as variáveis de
§6.1, `-race -count=1`, fuzz de 60 s por alvo) escreve aqui o número de testes,
pulados e falhas.

---

## 7. Verificação independente por área

Pendente. Como em S, cada área (conversão, recepção GELF, sinks, app e Docker)
deve ser revisada por um agente **só leitura**, que reproduz antes de afirmar;
cada achado corrigido ganha teste de regressão conferido por mutação.

---

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento.

1. **Cobertura por pacote não medida** neste módulo.
2. **Fuzz sem rodada registrada.** Os nove alvos existem e rodam; nenhuma
   rodada de 60 s nem de 10 min está anotada em §3.
3. **Benchmarks sem números** deste módulo (§4).
4. **Sem benchmark da remontagem de chunks nem do HTTP.** O mutex único da
   remontagem (SPEC §22.1, item 1) é o candidato a gargalo com
   `udp_listeners > 1`; nada mediu.
5. **Os critérios de carga (SPEC §18.8) nunca foram medidos**, e a decisão de
   converter no dreno não foi medida contra a alternativa ponta a ponta. O
   `go-gelf-injector` existe para isso; `gelf_convert_duration_seconds` mede
   em produção.
6. **Falhas de handshake do HTTPS não são contadas** (SPEC §22.1, item 2) e não
   há teste de um cliente HTTPS com certificado errado no nível da app.
7. **A bomba de descompressão** é provada pelo `FuzzDecompress` e por testes do
   leitor com teto; não há teste de memória de pico (RSS) com um gzip de
   razão 1000:1 chegando em paralelo por vários listeners.
8. **Prazo de 5 s da remontagem** é provado com relógio injetado
   (`ChunkTimeout` só em teste); o relógio real de varredura a cada segundo só
   roda nos testes de integração, sem medida da precisão.
9. **`kill -9` de verdade** não é testável em processo. A durabilidade do
   `ring=sqlite` é provada por fechamento e reabertura (herdado de S).
10. **O caminho `IP_PKTINFO` do Linux** só roda onde a suíte roda no Linux; a
    rodada de 2026-10-31 foi no macOS (o contêiner da rodada manual é Linux, e
    o `dst-ip` dele saiu certo: `172.20.0.2`).
11. **`SO_REUSEPORT`** só é testado no que é portável: que os sockets abrem.
12. **Réplicas com `[convert]` diferente** no mesmo ring Redis produzem bytes
    diferentes para o mesmo `uuid` (SPEC §3.5); o boot não tem como verificar.
13. **A regra do `/data`** tem só a prova manual do compose de demonstração
    (`read_only: true` subiu e gravou); nenhum teste automatizado de contêiner
    com raiz somente leitura.
14. **ClickHouse:** a citação de `fields`/`extra` (`chPayload`) foi medida
    contra o 25.8; outra versão que passe a respeitar os
    `input_format_json_read_*_as_strings` não muda o resultado (o valor já
    chega string), mas uma que mude a leitura de strings pediria nova medida.
15. **Emissores no fio:** só fluent-bit e o driver do Docker (§6.5). Logstash,
    `graypy`, `logstash-gelf` e a biblioteca Go `gelf` estão no corpus como
    documentos, não no fio (compressão e chunking deles não foram exercitados).
16. **Herdadas de S, valem aqui igual** (TESTING de S §8): envio tardio de um
    comando cancelado (rueidis, NATS) provado só pela decisão de
    `Taint`/`Discard`; dump de uma vida com outro Workspace sem teste de app;
    contadores da recepção sem *padding*, nunca medidos como gargalo; o "até
    8 KiB quando ociosa" do buffer de quadro vale só entre quadros; a sonda do
    `sink=sqlite` com o passo do relógio simulado; *pool* NATS só de servidores
    que recusam TLS e URL com vários IPs, sem teste.
