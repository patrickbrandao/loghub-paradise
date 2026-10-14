# go-syslog-server — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Os
números são de 2026-10-05, num Apple M2 (8 núcleos) com macOS, salvo onde dito.
Comandos a partir da pasta do módulo.

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. Sem variáveis de ambiente, as suítes de destino real
pulam (§6) e a suíte inteira leva cerca de 40 s.

Cobertura **sem** destinos reais (`go test -cover ./internal/...`):

| Pacote | Cobertura | Observação |
|---|---|---|
| `filter` | 98.2% | |
| `control` | 96.0% | |
| `jlh` | 94.3% | |
| `obs` | 92.0% | |
| `config` | 91.2% | |
| `receiver` | 90.8% | |
| `convert` | 89.7% | |
| `ratelimit` | 89.5% | |
| `emergency` | 84.8% | |
| `item` | 81.9% | |
| `sink` | 73.3% | PostgreSQL, MySQL, NATS e Redis pulam sem as variáveis de §6 |
| `app` | 72.8% | idem, mais os emissores reais |
| `ident` | 71.4% | os caminhos da biblioteca rodam no teste do binário |
| `jlhcheck` | 68.2% | é código de teste (oráculo) |
| `ring` | 44.5% | o ring Redis é quase metade do pacote e pula sem `SYSLOG_TEST_REDIS_ADDR` |
| `rediscli` | 37.5% | exercitado pelas suítes Redis de `ring` e `sink` |

---

## 2. Vetores dourados, contratos e corpus (desvio D3, SPEC §18.2)

```bash
go test -race -run 'Golden|Schema|Corpus|Semantic' -v ./internal/convert/ ./internal/jlhcheck/
```

- **Os 24 vetores** de `loghub-type-syslog-schema` (`internal/jlhcheck/loghub/vectors.json`):
  cada entrada é convertida e comparada **byte a byte** com o esperado (o `uuid`
  só pelo instante), e cada envelope esperado passa no oráculo.
- **O oráculo** (`internal/jlhcheck`) é o porte de Go do `validate_payload.py` da
  skill: os dois schemas (`santhosh-tekuri/jsonschema/v6`, só em teste) mais as
  regras semânticas (ordem e número de chaves, chaves duplicadas, `level` =
  severidade, `sid` recalculado, instante do `uuid` = `event`, `iso` truncado,
  `from` UUIDv7 e o conjunto completo de espaço em branco nos campos `^\S+$`).
  O `\s` do RE2 por trás do schema deixa passar U+00A0, U+2028, U+FEFF, `\v`,
  U+001C–U+001F, U+0085 e U+3000; a etapa semântica não (D6).
- **Corpus de linhas reais** (`internal/convert/testdata/corpus/*.raw` +
  `*.want.json`, 23 pares): rsyslog, syslog-ng, journald, appliances de rede,
  log4j sem PRI, Cisco com contador de sequência, mensagens com BOM, cp1252,
  SD com escapes e malformadas. Cada saída também passa no oráculo.
- Os desvios D1–D8 (SPEC §1.3) têm teste próprio e são a única divergência
  permitida em relação ao conversor de referência.

---

## 3. Fuzz (SPEC §18.3, obrigatório antes de release)

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=600s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrameOracle$  -fuzztime=600s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=600s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$         -fuzztime=600s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=600s
```

O `^…$` importa: `-fuzz=FuzzFrame` casa também `FuzzFrameOracle` e o Go recusa
dois alvos.

| Alvo | O que prova | Rodada de 2026-10-05 |
|---|---|---|
| `FuzzConvert` | a conversão nunca falha nem entra em pânico; toda saída passa no oráculo; a mesma entrada dá os mesmos bytes (§3.5) | 5 min, 2,27 M execs, sem falha |
| `FuzzSD` | STRUCTURED-DATA: tetos respeitados, nada perdido em `sd-invalid` | 5 min, 12,3 M execs, sem falha |
| `FuzzHead` | o codec do cabeçalho recusa sem pânico, e o que aceita volta igual | 5 min, 15,6 M execs, sem falha |
| `FuzzRead` | o leitor do dump nunca confia no comprimento declarado | 5 min, 22,2 M execs, sem falha |
| `FuzzFrame` | as molduras não perdem nem inventam bytes | 5 min, 1,48 M execs, sem falha |
| `FuzzFrameOracle` | a moldura contra um oráculo independente, escrito pelo revisor | 5 min, 3,67 M execs, sem falha |
| `FuzzProxy` | PROXY v1/v2: nunca aloca pelo declarado | 5 min, 2,42 M execs, sem falha; antes, em 30 s, achou um bug real (§9) |
| `FuzzJSONBSafe` | a troca do `\u0000` para a `loghub.inbox` só troca escapes reais, e o JSON decodifica igual com U+FFFD | 2026-10-06: 20 s, sem falha (rodada longa pendente, lacuna 1) |
| `FuzzInboxPayload` | o caminho real da `loghub.inbox`: JSON sem escape NUL, `raw` exato em base64, os demais textos com U+FFFD | 2026-10-06: 40 s, sem falha (rodada longa pendente, lacuna 1) |

Todas as rodadas com `-parallel=4`, na máquina carregada pelas outras suítes.
Rodadas de 10 min por alvo ainda não foram feitas (lacuna 1).

---

## 4. Benchmarks

```bash
go test -run XXX -bench . -benchmem ./internal/convert ./internal/ratelimit ./internal/receiver
```

| Benchmark | Tempo | Alocação | Leitura |
|---|---|---|---|
| `Convert/rfc3164` | 764 ns/op (99 MB/s) | 0 B, 0 allocs | conversão + JLH completo, no buffer do pedaço |
| `Convert/rfc5424` | 1,09 µs/op (158 MB/s) | 0 B, 0 allocs | idem, com SD |
| `SchemaValidate/rfc5424` | 97 µs/op | 82 KiB, 1 922 allocs | o custo que D3 tira do caminho quente: ~90× a conversão |
| `IngestAccept` | 137 ns/op | 280 B, 2 allocs | recepção até o ring (moldura já feita) |
| `TCPReceive/octet` | 450–560 ns/op (~2 M msg/s) | ~335 B, 2 allocs | uma conexão, linhas de ~200 bytes, socket real + moldura + pipeline até o ring de memória; igual com o leitor de 64 KiB (460–530 ns) e com o de 4 KiB (SPEC §5.4) |
| `TCPReceive/lf` | 450–570 ns/op (~2 M msg/s) | ~330 B, 2 allocs | idem em LF; com 64 KiB era 860–1 020 ns, porque a busca do NUL varria o buffer inteiro a cada frame (corrigido junto: o NUL só é procurado antes do LF) |
| `Framer12K` (2026-10-13) | 61–71 µs/op (~12 GB/s) | 78 KB, 5 allocs | 64 frames de 12 KiB pelo leitor de 4 KiB (SPEC §5.4): entre o limiar do buffer guardado (16 KiB) e a classe de 64 KiB. Soltando o buffer depois de todo frame acima de 16 KiB eram 587–929 µs/op, 4,7 MB e 131 allocs (a classe de 64 KiB realocada a cada frame); agora só antes de uma espera (revisão independente do porte) |
| `AllowSameKey` | 152 ns/op | 0 | rate-limit, um emissor (contenção no shard) |
| `AllowSpreadKeys` | 39 ns/op | 3 B | rate-limit, emissores espalhados |

Um núcleo converte de 900 mil a 1,3 milhão de mensagens por segundo. O pior SD
dentro dos tetos padrão (32 elementos × 64 parâmetros, ~14 KB) custava 1,6 ms
antes da correção do custo quadrático; agora cada elemento só percorre os
próprios parâmetros.

---

## 5. Integração em processo (`internal/app`)

```bash
go test -race ./internal/app/        # ~20 s
go test -race -short ./internal/app/ # pula o teste do binário real
```

A app inteira sobe em processo (identidade fixa, `DATADIR` temporário, portas
efêmeras), e todo envelope lido de volta passa pelo oráculo; as colunas de
`sink=sqlite` são conferidas contra o payload (NULL exatamente quando o campo
falta). Cobre:

- **Transportes:** UDP em `0.0.0.0` com `dst-ip` por PKTINFO, TCP LF e
  octet-counting, TLS com e sem certificado de cliente (`peer-*`), unix (sem
  `host`, `sid = 0`, socket removido no shutdown), vários listeners por
  transporte (`SO_REUSEPORT`), PROXY v1 e v2 de par confiável (`x-proxy`, `sid`
  recalculado).
- **Fidelidade:** `raw` byte a byte com NUL embutido, NUL vira U+FFFD só na
  coluna, `payload_raw=false` mantém o `raw` obrigatório, truncamento mantém a
  moldura sincronizada, mensagens vazias contadas e não escritas, malformada
  recusada com `accept_malformed=false`.
- **Ciclo de vida:** recusas de boot com exit 1 (cada tabela com um controle
  válido que sobe), sink inalcançável mantém os transportes fechados e sai com 3
  no `boot_timeout`, shutdown gracioso entrega tudo, segundo sinal dá 130 e
  conta `abort`, `SIGHUP` aplica/ignora/recusa conforme §16.3, arquivo inválido
  no reload não aplica nada, socket unix vivo não é roubado.
- **Durabilidade:** dump de emergência exato e reingerido por outra vida (mesmos
  `uuid`, mesmos bytes, `from` da primeira vida), qualquer que seja o
  `ring.mode` seguinte; `emergency.enabled=false` conta `emergency_failed`;
  `ClassFatal` no `Write` despeja e sai com 3; dump danificado vai para
  `.corrupt`; linha envenenada do `ring.db` vai para a quarentena; backlog do
  `ring=sqlite` drenado antes de qualquer transporte; a confirmação do
  `ring=sqlite` vem depois do `COMMIT`; a entrega do boot segue as classes chunk
  a chunk.
- **Saturação:** ring cheio por transporte (UDP conta `ring_full`, TCP segura e
  não perde), sink DOWN por transporte, o teto de itens conta o lote retido
  (`TestRingItemsNeverExceedTheCapacity`), `/readyz` com o motivo.
- **Admin:** `/healthz`, `/readyz` com transições, as três métricas `_info`
  exatas, toda série de §14.1 presente, labels só de conjunto fechado, `le`
  estável entre leituras.
- **Concorrência:** 12 conexões TCP + 4 emissores UDP com `ring` memory, sqlite e
  none: aceitas = linhas, sem duplicata.
- **O binário real** (`TestBinaryIdentityExitCodes`): `agent_uuid` como symlink
  dá 100, `WORKSPACE=bad_space` dá 111, as cinco variáveis de identidade
  dispensam o disco, `DATADIR` relativo é resolvido antes do `Initialize()`.
  `TestBinarySignalsOutsideTheRunWindow`: `SIGHUP` no boot é logado e ignorado
  (o boot termina pelo `boot_timeout`, não pelo sinal), `SIGQUIT` no boot e com
  o servidor no ar dá 130, `SIGHUP` no ar recarrega.

---

## 6. Destinos externos

Todas pulam sem a variável. Cada teste cria e remove o que usa (tabela, stream,
chave); os contêineres são descartáveis.

### 6.1 Redis (`ring=redis`, `sink=redis`)

```bash
docker run --rm -d -p 16379:6379 --name sls-test-redis redis:7-alpine
docker run --rm -d -p 16381:6379 --name sls-test-redis-lru redis:7-alpine \
  redis-server --maxmemory-policy allkeys-lru
SYSLOG_TEST_REDIS_ADDR=127.0.0.1:16379 SYSLOG_TEST_REDIS_LRU_ADDR=127.0.0.1:16381 \
  go test -race -timeout 600s ./internal/rediscli/ ./internal/ring/ ./internal/sink/ ./internal/app/
docker rm -f sls-test-redis sls-test-redis-lru
```

O segundo Redis (`SYSLOG_TEST_REDIS_LRU_ADDR`, `allkeys-lru`) é só para a
política de despejo (SPEC §9.5, §10.3; pendência 5 de 2026-10-11): o ring e o
sink recusam o boot como FATAL, sem criar chave. É um servidor à parte porque o
primeiro é compartilhado por todos os pacotes da rodada, em paralelo, e trocar a
política dele quebraria os outros. Sem a variável, esses dois testes pulam; os
de ACL sem `INFO`/`CONFIG` (`WARN` e o boot segue) usam o primeiro.

Prova, entre outros: o script de enfileiramento com os dois tetos globais, o
`Commit` que só decrementa o que o `XDEL` removeu, a releitura dos próprios
pendentes, `XAUTOCLAIM` de réplica morta, renovação (`Touch`) e renovação na
entrega de reivindicadas, `Quiesce`, disponibilidade pela escrita (ACL sem
`@write`), Redis Cluster sem *hash tag* recusado, e na app: duas instâncias num
ring (cada mensagem uma vez, contador em zero), réplica viva não perde o lote
para a outra, réplica parada perde.

### 6.2 PostgreSQL (`loghub.inbox`)

```bash
docker run --rm -d -p 15432:5432 -e POSTGRES_USER=sls -e POSTGRES_PASSWORD=sls \
  -e POSTGRES_DB=sls_test --name sls-sink-pg postgres:18-alpine
SYSLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
  go test -race ./internal/sink/ ./internal/app/
docker rm -f sls-sink-pg
```

PostgreSQL 18: a DDL da skill usa `uuid_extract_version` e `COMPRESSION lz4`.
Cada teste cria a fila a partir de `docker/postgres/inbox.sql` num esquema
próprio. Prova o `unnest` de dez arrays (`jsonb[]` e `timestamptz[]` inclusive),
`ON CONFLICT (uuid)` com reenvio e uuid repetido no mesmo pedaço, as CHECKs da
skill aceitando tudo o que o servidor produz (NUL, unix, outro `space`), o
envelope reconstruído da linha — com o SQL da skill — igual ao JLH e válido nos
schemas e nas regras semânticas (inclusive o `raw` exato em base64 de uma linha
com NUL), o consumo `FOR UPDATE SKIP LOCKED` em ordem de `uuid` sobre o que foi
enfileirado, a classificação por SQLSTATE (classe `42` fatal; `23514` sem
constraint, a partição ausente, fatal), e a validação do boot: tabela, tipos
(`jsonb`, `timestamptz`, `uuid`, `sid` só `bigint`, `varchar(n)` curto
recusado; `timestamptz(6)` aceito, `timestamptz(3)` e `(0)` recusados), índice
único exatamente sobre `(uuid)` e imediato (`DEFERRABLE` recusado), nenhum
outro índice único nem exclusão (`(from_uuid, sid)`, `UNIQUE (sid)`,
`(uuid, step)`, um único `DEFERRABLE` ao lado da PK e uma `EXCLUDE` recusados;
um índice comum e um segundo único sobre `(uuid)` tolerados; desde a revisão
dos webhooks de 2026-10-06), tabela particionada recusada, banco `LATIN1` recusado, privilégio,
sessão somente leitura (`default_transaction_read_only`) e coluna extra
obrigatória (`received_at` sem default recusada; sem `received_at`, com
default, identidade, `bigserial` e gerada toleradas). Sem servidor:
`TestPostgresInboxFileIsTheSkillDDL` confere o hash da DDL da skill,
`TestNewPostgresIgnoresTheLibpqEnvironment` o pool livre de `PGOPTIONS`,
`PGTARGETSESSIONATTRS`, `PGAPPNAME` e de todo outro `PG*` (parse com o ambiente libpq escondido, pendência 9), e
`FuzzInboxPayload` (o caminho real) e `FuzzJSONBSafe` (o recurso de reserva) a
troca do NUL. **Não provado:** o FATAL no boot de um servidor em recuperação
(`pg_is_in_recovery()`) exige uma réplica de verdade.

### 6.3 MySQL

```bash
docker run --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=sls -e MYSQL_DATABASE=sls_test \
  --name sls-sink-mysql mysql:8.4
SYSLOG_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13306)/sls_test' go test -race ./internal/sink/ ./internal/app/
docker rm -f sls-sink-mysql
```

Prova o `ON DUPLICATE KEY UPDATE`, a divisão em comandos (3 640 linhas,
7/8 do `max_allowed_packet`), a sonda revertida (qualquer recusa do servidor é
fatal, inclusive `3819` de um `CHECK`), `read_only` fatal no boot e DOWN depois,
e a entrega parcial (`Delivered` = prefixo gravado). `TestMySQLChunking` leva
~12 s sob `-race`.

### 6.4 NATS

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &   # SEM JetStream
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 SYSLOG_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 600s ./internal/sink/ ./internal/control/ ./internal/app/
```

Prova o subject exato e o granular, `Nats-Msg-Id = <uuid>.0` e os `X-LogHub-*`,
o JLH só em `Data`, a validação do stream (cobre o subject, não captura o plano
de controle, não selado, `no_ack` recusado, maior evento cabe), JetStream
desligado fatal na hora, entrega parcial (`10054`), beacon em
`loghub._ctl.beacon` (schema do envelope, `type=beacon`, `level=9`,
`sid`=PID, periódico e na reconexão), queda e volta por proxy sem perda, e FATAL
pelos fusíveis → dump → reingestão no stream.

Com permissões (pendências 14 e 19 de 2026-10-11, portadas do R), um terceiro
servidor, com JetStream e usuários:

```bash
cat > /tmp/nats-perm.conf <<'EOF'
listen: 127.0.0.1:14224
jetstream { store_dir: "/tmp/nats-perm" }
authorization { users = [
  { user: admin, password: admin }
  { user: nopub, password: nopub, permissions: {
      publish: { allow: [">"], deny: ["loghub.*.syslog", "loghub.*.syslog.>"] },
      subscribe: { allow: [">"] } } }
  { user: noinbox, password: noinbox, permissions: {
      publish: { allow: [">"] }, subscribe: { deny: ["_INBOX.>"] } } }
  { user: nobeacon, password: nobeacon, permissions: {
      publish: { allow: [">"], deny: ["*.beacon"] }, subscribe: { allow: [">"] } } }
] }
EOF
go run github.com/nats-io/nats-server/v2@v2.14.6 -c /tmp/nats-perm.conf &
SYSLOG_TEST_NATS_PERM_URL=nats://127.0.0.1:14224 go test -race ./internal/sink/ -run 'Permission|RefusedCredentials'
```

| Variável | Valor |
|---|---|
| `SYSLOG_TEST_NATS_PERM_URL` | sem usuário na URL; usuários `admin`, `nopub`, `noinbox`, `nobeacon` (senha = usuário); a mesma conf está no comentário de `natsPermSetup` em `internal/sink/review_nats_test.go` |

**TLS de verdade, num pool** (revisão independente do porte, 2026-10-13): a
suíte sobe os próprios `nats-server` com `tls {}`, nas portas 14700–14799, com
duas CAs geradas em Go no teste (uma que o cliente confia, outra de um nó
"estranho"), a partir do binário apontado por `SYSLOG_TEST_NATS_SERVER_BIN`
(2.14.6; pula sem ele):

```bash
go build -o /tmp/nats-server github.com/nats-io/nats-server/v2@v2.14.6
SYSLOG_TEST_NATS_SERVER_BIN=/tmp/nats-server go test -race -run 'TLSPool' ./internal/sink/
```

Prova que um nó de certificado de outra CA num pool de dois não vira veredito
quando o nó confiável reinicia (era FATAL, 5 de 5), que o primeiro `CONNECT`
com o confiável fora e o estranho no ar é `network` (dez conexões, contra a
ordem embaralhada do `nats.go`) e sobe quando o confiável volta, que o estranho
sozinho continua o veredito imediato, e que um servidor confiável em
`jetstream` reinicia sem veredito e volta a escrever.

Prova a negação do subject de dados (`jetstream`: fatal depois do
`sink.op_timeout`; `core`: fatal na hora) e da assinatura `_INBOX` como veredito
FATAL, a negação do subject do beacon **sem** veredito, e senha errada no
primeiro `CONNECT` como FATAL sem esperar o `boot_timeout`. O resto das
pendências 13–20 (stream editado em tempo de execução, `retention: interest`,
mensagem acima do `max_payload` recusada sozinha) roda contra o
`SYSLOG_TEST_NATS_URL`; o TLS que nunca verifica, o *flush* que atravessa uma
reconexão e a negação mascarada rodam contra um servidor falso em processo
(`natsFake`, sem variável).

### 6.4a ClickHouse (`inbox_syslog`, 2026-10-07)

```bash
docker run --rm -d -p 19000:9000 -e CLICKHOUSE_USER=sls -e CLICKHOUSE_PASSWORD=sls \
  -e CLICKHOUSE_DB=sls_test -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  --ulimit nofile=262144:262144 --name sls-sink-clickhouse clickhouse/clickhouse-server:25.8
SYSLOG_TEST_CLICKHOUSE_DSN='clickhouse://sls:sls@127.0.0.1:19000/sls_test' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f sls-sink-clickhouse
```

Protocolo nativo, 25.8 ou mais novo; o `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`
deixa os subtestes de privilégio e de memória criarem usuários (sem ele, pulam).
Prova a ida e volta pelo oráculo — o JLH igual à linha como valor JSON depois
da regra de leitura `"structured-data":{}` = ausente (SPEC §3.7.2), com SD sem
parâmetros, nomes com ponto, `PARAM-NAME` repetido e NUL —, um JLH grande, o
token descartando o retry do mesmo pedaço e o merge colapsando o
reagrupamento, `logic` sem nada gravado (`from` que não é UUIDv7, payload que
não é JSON), tabela dropada fatal, 32 tabelas no boot (8 aceitas que escrevem,
24 recusadas, uma por regra, a tabela dos webhooks inclusive, `EPHEMERAL`,
`MATERIALIZED`, `is_deleted`, partição fora de `event_at`, CHECK entre crases),
nenhuma string lida como data (`TestClickHouseStringsAreNeverInferred`), usuário
só com `SELECT`, `readonly`, senha errada, `max_memory_usage` contra o pior
pedaço e o limite por usuário contra `pool_size` pedaços, o `452` de um perfil
`CONST` na sonda, a
tabela de §10.10.7 lida da própria SPEC, os quatro rings ponta a ponta
(`ring=redis` com `SYSLOG_TEST_REDIS_ADDR`) e o destino caindo e voltando por
proxy sem perda.

### 6.5 Emissores reais (SPEC §18.7)

```bash
SYSLOG_TEST_EMITTERS=1 go test -race -run Emitters -timeout 600s ./internal/app/
```

Precisa de Docker (baixa pacotes). rsyslog por UDP, TCP LF (`ForwardFormat`), TCP
octet-counting (`Protocol23`) e TLS `gtls` com `x509/name` e certificado de
cliente (o fingerprint aparece em `payload.tls`); syslog-ng `network()` TCP e
UDP e `syslog()` octet-counting. 25 mensagens de cada, com `appname`, facility e
severidade conferidos.

### 6.6 Resultado da rodada de 2026-10-05

| Suíte | Resultado |
|---|---|
| `sink` + `control` com redis:7, postgres:17, mysql:8.4, nats-server 2.14.6 com e sem JetStream | 140 testes, 0 pulados, 0 falhas |
| `ring` + `emergency` com Redis | passa, `-count=2` |
| `app` com cada variável sozinha e com as cinco juntas | passa (~52 s com todas) |
| `receiver` no Linux (Docker, Go 1.27.1), inclusive PKTINFO IPv4/IPv6 e o *flood* de `KeyUpdate` | passa, `-count=2` |

### 6.7 Resultado da rodada de 2026-10-13 (porte do R, RP, RN)

`go vet ./...` limpo e `go test -race -count=1 ./...` verde, os 16 pacotes, com
PostgreSQL 18, MySQL 8.4, ClickHouse 25.8, Redis 7 (dois: um com
`allkeys-lru`, `SYSLOG_TEST_REDIS_LRU_ADDR`) e nats-server 2.14.6 (JetStream,
sem JetStream e com permissões, `SYSLOG_TEST_NATS_PERM_URL`) e os emissores reais (`SYSLOG_TEST_EMITTERS=1`) — todas as variáveis de §6:
681 testes de primeiro nível e 458 subtestes passam, **1 pulado**
(`TestGoldenVectors/arquivo-sem-pri`: `transport=file`, fora por D5).

### 6.8 Resultado da rodada de 2026-10-13 (revisão independente do porte)

`gofmt -l .` vazio, `go vet ./...` limpo e `go test -race -count=1 -v ./...`
verde **duas vezes seguidas**, os 16 pacotes, com todas as variáveis de §6
(PostgreSQL 18, MySQL 8.4, ClickHouse 25.8, os dois Redis 7, nats-server 2.14.6
com JetStream, sem JetStream e com permissões), os emissores reais
(`SYSLOG_TEST_EMITTERS=1`) e `SYSLOG_TEST_NATS_SERVER_BIN` (os `nats-server`
com `tls {}` da suíte): **708 testes de primeiro nível e 471 subtestes passam,
1 pulado** (`TestGoldenVectors/arquivo-sem-pri`, D5), nas duas rodadas. Fuzz de
60 s nos alvos cujo código mudou, sem falha: `FuzzFrame` 638 mil execuções,
`FuzzFrameOracle` 882 mil, `FuzzProxy` 49 mil (máquina carregada por outras
suítes em paralelo; soma-se à lacuna 1).

---

## 7. Verificação independente por área

Cada área foi entregue e depois revisada por um agente **só leitura**, que
reproduz antes de afirmar; cada achado corrigido ganhou teste de regressão, e os
testes foram **conferidos por mutação** (a correção desfeita faz o teste falhar).

| Área | Achados | Destaques |
|---|---|---|
| Conversão | 8 (3 corrigidos no código, 4 registrados como desvio/documentação, 1 no oráculo) | aviso de encoding perdido antes do descarte por espaço; referência da inferência de ano sem os nanossegundos; SD quadrático; `uuid` × amostra no codec |
| Rings e dump | 9 | boot em crashloop num erro `logic`; dump ignorado após troca de `ring.mode`; `/readyz` oscilando; contabilidade por `len` e não por `cap`; teto do SQLite furado sob concorrência; `fsync` do diretório ignorado |
| Recepção | 7 | retentativa cobrando o rate-limit de novo; leitor TLS preso numa escrita por `KeyUpdate`; dígitos perdidos num erro durante a detecção; `NewUnix` apagando o que estivesse no caminho |
| Sinks e beacon | 10 | worker morrendo em silêncio num erro de `Dequeue`; PostgreSQL aceitando `NOT NULL` que depois recusa tudo com o sink UP; `no_ack` e JetStream desligado não fatais; FATAL sobrescrito |
| App, config, docs, Docker | 4 médios, 11 baixos, ~20 informativos | SIGHUP matava o processo (129) no boot; sink FATAL no dreno saía com 0; certificado TLS inválido escondido atrás de um exit 3; nenhuma chave recarregável recarregava no Docker (env vence o INI); `NaN` passava pela validação; `admin.bind` vazio publicava `/metrics` em todas as interfaces |

---

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento.

1. **Fuzz de 10 min por alvo ainda não rodou.** As rodadas foram de 5 min por
   alvo, os sete.
2. **Os critérios de carga nunca foram medidos.** Não há gerador de carga. Os
   benchmarks de §4 medem peças; em particular, **a decisão de converter no
   dreno (§2.2) não foi medida contra a alternativa** ponta a ponta. A métrica
   `syslog_convert_duration_seconds` existe para responder isso em produção.
3. **`kill -9` de verdade** não é testável em processo. A durabilidade do
   `ring=sqlite` é provada por fechamento e reabertura, e pela leitura direta do
   `ring.db` no instante da confirmação.
4. **O caminho `IP_PKTINFO` do Linux** só rodou na suíte da recepção (Docker);
   a suíte da app rodou só o do macOS.
5. **`SO_REUSEPORT`** só é testado no que é portável: que os sockets abrem. A
   distribuição entre núcleos é a do Linux, e a app roda os testes no macOS.
6. **`/dev/log` de verdade** não foi testado; os testes usam um socket em
   diretório temporário.
7. **Não cobertos no nível da app:** a correção do `address` do beacon, a
   amostragem de `log.access`, TLS `require` com certificado estranho,
   `proxy.protocol=required`, CIDRs isentos do rate-limit, rotação do
   `sink=sqlite` com `keep_files` e seu reload. Os pacotes cobrem parte disso
   isoladamente.
8. **Envio tardio de um comando cancelado** (rueidis, NATS) só é provado pela
   decisão de `Taint`/`Discard` de quem chamou, não contra um servidor que trave
   no meio do comando. É o custo at-least-once que a SPEC assume; o consumidor
   deduplica por `(uuid, step)`.
9. **Réplicas com config de conversão diferente** no mesmo ring Redis produzem
   bytes diferentes para o mesmo `uuid` (§3.5). O boot não tem como verificar;
   nenhum teste prova a exigência, só a documentação.
10. **Em processo, depois do segundo sinal**, `runContext` devolve 130 enquanto
    a goroutine do shutdown gracioso continua dormindo no `pre_stop_delay` com
    os transportes abertos. No binário o processo sai e não há efeito; os testes
    usam `pre_stop_delay=1h` para que ela nunca acorde.
11. **Dump de uma vida com outro Workspace** vai para o subject/stream do `space`
    antigo (§12.2); o caso em que nenhum stream o cobre (boot sai com 3) não tem
    teste de app.
12. **A regra do `/data`** (nada escrito fora de `server.data_dir`, exceto o
    socket unix) não tem teste automatizado de contêiner com raiz somente
    leitura.
13. **`sink=clickhouse`**: TLS, `242`/`system.replicas.is_readonly` (exige um
    Keeper) e os perfis de `async_insert` (SPEC §10.10.6) não são exercitados;
    o fator de memória do pior pedaço (128 MiB + 8 ×) vem de três medições no
    25.8 feitas no `go-webhook-server`, com payload de webhook; o merge do
    `ReplacingMergeTree` é forçado com `OPTIMIZE … FINAL` nos testes.
    `241` e `242` na escrita são fatais por decisão do usuário, mas vêm também
    do limite global do servidor e de uma perda breve do Keeper. Uma
    *materialized view* sobre a tabela faz uma recusa dela chegar depois das
    linhas gravadas (o boot avisa); nenhum teste a cobre.
14. **Porte de 2026-10-13: o que ficou sem prova.**
    - Custo por conexão medido com 100–200 conexões em processo (as duas pontas
      no mesmo heap); nada com milhares de conexões reais nem sob `GOMEMLIMIT`.
      `BenchmarkTCPReceive` e o fuzz de 60 s de `FuzzFrame`, `FuzzFrameOracle` e
      `FuzzProxy` rodaram com a máquina sob carga alta (poucas execuções):
      repetir com ela livre (soma-se à lacuna 1).
    - Política de despejo do Redis lida de um nó só e só no boot; um
      `CONFIG SET` com o servidor no ar não é visto.
    - O `withLifetime` e o `validationContext` estão provados cada um no seu
      pacote; não há teste de app de um `SIGTERM` durante uma validação longa.
    - Sonda MySQL: linhas `unix` de um ring persistente drenadas numa vida sem
      listener `unix` não têm teste; `parseWithoutLibpqEnv` mexe no ambiente do
      processo sob mutex, sem teste de concorrência com outra leitura dele.
    - ~~`pool_size ≥ workers + 1` sem teste de integração~~: fechado pela
      revisão independente do porte (2026-10-13). A sonda de saúde tem conexão
      própria nos três sinks, e `Test{Postgres,MySQL,ClickHouse}ProbeIsNotStarvedByTheWriters`
      seguram tantas escritas lentas quanto conexões no pool (2 e 3) e provam
      a checagem dentro de 1 s. O teto de 4096 desses pools é escolha, não
      limite de biblioteca.
    - O `knownSectionOf` ainda cita uma cauda de senha com cara de
      `sink.nats.xyz`: risco reduzido, não fechado (como no R e no RN).
    - NATS: a folga 2× do `MaxPending` não tem teste (tirá-la não mata nenhum);
      o `ErrSecureConnWanted` só contra o `natsFake`. O certificado que não
      verifica roda contra `nats-server` com `tls {}` (§6.4,
      `SYSLOG_TEST_NATS_SERVER_BIN`). Até a revisão independente do porte
      (2026-10-13) esta linha dizia que, num cluster com um nó de certificado
      ruim, a reconexão que caísse nele virar FATAL era "semântica do RN": não
      era (o RN não tem TLS), e o FATAL permanente de um restart do nó confiável
      era bug; a regra agora é a de SPEC §10.7 ("só é veredito quando nenhum
      outro servidor pode servir"). Não provado: um nó **descoberto** por
      *gossip* de cluster (o teste usa duas URLs configuradas; a regra lê
      `nc.Servers()`, que conta os dois); o `wss://` pelo
      `ReconnectErrHandler`.
    - Dump: o abandono a cada 1024 registros é provado
      (`TestWriteGivesUpInTheMiddleOfALargeDump`, 2026-10-13); o fechamento
      paralelo dos quatro transportes reais com conexões presas não tem teste de
      tempo (`TestTransportsCloseTogether` usa transportes falsos); bind
      cancelado provado em `tcp`, `udp`, `tls` e admin por nome de host e em
      `tcp`, `udp`, `tls` e `unix` por literal/caminho
      (`TestSignalBeforeALiteralBindExits130`); o fluxo "operador renomeia o
      temporário interrompido e o boot seguinte o reingere" não tem teste.
    - Revisão independente do porte (2026-10-13), o que ficou sem prova: o
      `?host=` vazio e o TLS sobre socket do PostgreSQL são recusados na config,
      sem servidor por socket nos testes; o `1048` fatal do MySQL vem de uma
      coluna alterada depois do boot, não de um trigger; a regra
      `ring=redis` + `sink=none` é só um `WARN`; o teto de `ring.capacity_items`
      com `sqlite`/`redis` (2⁴⁰) é de sanidade, sem teste de um ring com 50
      milhões de linhas de verdade; o `store is closed` do `10077` só pela
      tabela de classes, com o texto do servidor (fechar o store no meio de uma
      publicação exige um restart cronometrado).

---

## 9. Histórico: bugs que os testes e as revisões encontraram

Cada linha é um bug que existiu no código desta reescrita e foi corrigido, com o
que o encontrou. A lição vai para `AGENTS.md` §9 quando generaliza.

| Achado por | Bug | Lição |
|---|---|---|
| `FuzzProxy` | o leitor PROXY v1 consumia um 108º byte (o primeiro depois do cabeçalho) antes de conferir o teto de 107 | conferir o teto **antes** de consumir; a entrada que falhou ficou como semente |
| raciocínio sobre o schema (D6) | um valor com TAB, U+00A0 ou U+2028 em `hostname`/`appname`/`procid`/`msgid` produzia envelope fora do `^\S+$`, como o conversor de referência | o motor de regex do teste não é o do schema; o oráculo precisa do conjunto completo |
| revisão de conversão | o aviso `encoding-invalid` de um campo do cabeçalho se perdia quando o campo era descartado por espaço em branco | ordem: decodificar, avisar, depois decidir |
| revisão de conversão | a inferência de ano comparava só segundos com a referência | a referência é a amostra inteira (`uts`, `sub`) |
| revisão de conversão | montar o `structured-data` era quadrático no número de parâmetros (1,6 ms por mensagem no pior caso dentro dos tetos) | índices por elemento |
| revisão de conversão | o codec aceitava cabeçalho com `uuid` de outro instante | `uuid` e amostra são uma amostra só; e casos de recusa precisam de um controle válido |
| teste de config | a config aceitava `alert`, `clock` e `security` como facility e o filtro não | um valor aceito pela config precisa significar algo para quem o usa (também `windows-1252`) |
| revisão de rings | um erro `logic` na reingestão ou no dreno do boot saía com 3 e deixava o mesmo dado para o boot seguinte: crashloop | no boot vale a regra de classes dos workers |
| revisão de rings | dumps só eram reingeridos com o mesmo `ring.mode` | a leitura de dumps independe do modo atual |
| revisão de rings | o ring em memória contava `len(cru)`, e uma linha de 8 193 bytes segurava 64 KiB | contar o que se segura (`cap` + struct) |
| revisão de rings | `Enqueue`s concorrentes no `ring=sqlite` passavam todos pelo teto numa janela de commit | reservar antes de entregar à escritora |
| revisão de rings | `/readyz` oscilava com um Redis que lê e não escreve | disponibilidade pela escrita |
| revisão de rings | erro do `fsync` do diretório do dump era ignorado | o arquivo não está salvo até o diretório estar |
| revisão de sinks | um worker saía para sempre, sem log, num erro de `Dequeue` que não fosse de contexto | só o fim do contexto ou do ring encerra um worker |
| revisão de sinks | a entrega do boot reconvertia a cada retry, e um `logic` no meio do lote deixava os chunks seguintes sem escrita e confirmados | `Instrumented.Deliver`, chunk a chunk, com o chunk selado |
| revisão de sinks | PostgreSQL com coluna anulável virada `NOT NULL` passava na validação e recusava toda escrita como `logic` com o sink UP | validar nulidade no catálogo |
| revisão de sinks | sonda do MySQL e `read_only` no boot, `no_ack` e JetStream desligado no NATS esperavam 300 s em vez de sair com 3 | recusa definitiva no boot é fatal na hora |
| revisão de sinks | FATAL podia ser sobrescrito por uma checagem concorrente | FATAL é terminal, sob o mesmo lock |
| revisão de sinks | um chunk parcialmente gravado (MySQL, NATS) era contado inteiro como perdido | `Error.Delivered` |
| revisão de recepção | a retentativa do laço de backpressure cobrava o rate-limit de novo, e a mensagem retida acabava descartada como `ratelimit` | filtro e rate-limit uma vez por mensagem |
| revisão de recepção | um `KeyUpdate` do TLS 1.3 prendia o leitor numa escrita e o `Close` travava | prazo de escrita junto com o de leitura |
| revisão de recepção | um erro durante a detecção da moldura descartava os dígitos já lidos | o que chegou é entregue |
| revisão de recepção | `NewUnix` removia qualquer coisa no caminho de `listen.unix` | só um socket datagrama órfão |
| revisão de recepção | com `ring=none`, uma recusa `logic` era retentada até o `backpressure_timeout` e contada a cada volta | `logic` é final |
| suíte de integração da app | com um lote retido, o ring em memória de capacidade 4 aceitava a quinta mensagem e dizia `5/4` | os dois tetos contam a mesma população |
| revisão de app/config | `SIGHUP` fora da janela de recarga tinha a ação padrão do Go: exit 129, sem log, no meio de um boot de até 300 s; `SIGQUIT` dava dump de goroutines e exit 2 | sinal com ação padrão fatal é capturado desde a primeira linha |
| revisão de app/config | sink FATAL durante o dreno do `SIGTERM` saía com 0 (`sqlite`/`redis`) e esperava o `drain_timeout` inteiro | o estado do sink decide o código de saída até o fim |
| revisão de app/config | certificado TLS inválido só aparecia no passo 12; com sink inalcançável, o boot saía com 3 sem nunca logar o erro do TLS | conferir conteúdo, não só permissão, antes de qualquer espera |
| revisão de app/config | nos Compose, toda chave recarregável vinha por env, que vence o INI: o `docker kill -s HUP` documentado nunca mudava nada, e o log dizia só "unchanged" | os Compose não definem chave recarregável por env: elas vêm de `docker/conf/syslog.ini`, montado em `/etc/syslog`; e chave sombreada é logada em todo `SIGHUP` |
| revisão de app/config | `NaN` passava por toda validação de faixa; `admin.bind` vazio publicava o admin em todas as interfaces; `localhost` e `127.0.0.1` na mesma porta deixavam TCP e TLS no mesmo socket | validar o que o parser aceita, não o que se espera receber |

### `sink=clickhouse` (2026-10-07)

| Achado | Como | Consequência sem a regra |
|---|---|---|
| O tipo `JSON` descarta objeto vazio | experimento contra o 25.8, antes do código | um SD sem parâmetros (`[origin]`) sumia da linha; virou o *hint* `Map` em `structured-data` e uma tabela própria (`inbox_syslog`), porque o *hint* dos webhooks injetaria `params`/`query` aqui e vice-versa |
| Nomes do SD com ponto viram caminhos | `[a b.c="1"][a.b c="2"]` | dois caminhos `a.b.c` e `117`: o pedaço inteiro como `logic` |
| Fixture de teste que passava vazio | `[a.b ... ="no"]` (PARAM-NAME vazio) | o SD inválido ia para `message` e a ida e volta passava sem exercitar o `Map`; o teste agora confere que o JLH tem o `structured-data` que diz ter |
| Revisão independente: inferência de datas no tipo `JSON` | um `procid` 12345 voltava `"1970-01-01 03:25:45"`, valores de SD de outro emissor como datas | desligar a inferência em todo `INSERT`; teste com atacante e vítimas no mesmo pedaço, conferido por mutação |
| Revisão independente: `EPHEMERAL`, `is_deleted`, partição, CHECK entre crases, `system.replicas`, limite por usuário, `452`, `44` | perda silenciosa, lotes descartados ou boot preso/fatal sem motivo | as regras de §10.10.5, uma tabela recusada por regra |


### Porte das correções do R, RP e RN (2026-10-13)

As 30 pendências de 2026-10-11 e os "a portar" das entradas R e RP, RN do
`CHANGELOG.md` da raiz, por área (ciclo de vida e dump, configuração, NATS,
SQL, transportes/Redis/conversão). Cada correção tem teste que falhava antes ou,
quando o teste usa API nova, que falha com a correção desfeita (mutação: mais de 120
mutantes, todos mortos).

| Achado por | Bug | Lição |
|---|---|---|
| pendência 1 | cada conexão TCP/TLS segurava um `bufio.Reader` de 64 KiB e o buffer de frame de uma mensagem grande: ~67 KB de heap por conexão TCP calada (81 KB TLS), fora de `ring.max_bytes` | memória por conexão é medida (`TestIdle*ConnectionFixedCost`); o maior `Peek` dita o tamanho do leitor |
| `BenchmarkTCPReceive` | `indexTerminator` procurava o NUL no buffer inteiro antes do LF: a moldura LF custava o dobro da octet | benchmark de ponta a ponta pega custo proporcional ao buffer que o micro-benchmark esconde |
| pendência 3 | o `Close` contava quem esperava o cabeçalho PROXY como `absent`/`invalid` e `messages_dropped{proxy}`, culpando o balanceador | todo caminho que o shutdown interrompe confere `closing` antes de contar |
| pendência 5 | com `allkeys-*` o Redis cheio despeja o stream inteiro com o serviço UP | premissa do destino que decide perda é conferida no boot (FATAL), com `WARN` se a ACL não deixar ler |
| pendência 6 | validação do boot do PG/MySQL no `health_timeout` (1 s): a ~55 ms nunca cabia; a do ClickHouse ignorava o `SIGTERM` | prazo da validação é do trabalho, não da checagem que a dispara |
| pendências 7, 8 | `23502` era `logic` e `3D000` `network` | erro que o servidor nunca causa é da tabela: fatal |
| pendência 9 | `PG*` do ambiente derrubavam o parse ou recusavam toda conexão | esconder o ambiente durante o parse, não sobrescrever depois |
| pendência 10 | MySQL com `autocommit=0` global: `Write` confirmava e as linhas sumiam | parâmetro de sessão de que o sink depende é fixado e relido |
| pendência 11 | outro índice único, prefixo `uuid(4)`, coluna estreita e número sem SQLSTATE no MySQL | `ON DUPLICATE KEY` dispara em qualquer único: só o árbitro pode existir |
| pendência 12 | nome do banco cru entre crases | o que entra cru no SQL é validado na config e no construtor |
| revisão do R | MySQL `1203`/`1226`/`1461` seriam fatais pelo recurso ao SQLSTATE; a sonda punha `src_ip NULL` com `udp`, linha que nenhum listener faz | o recurso por classe precisa das exceções; a sonda usa só linhas reais **deste** servidor (aqui `unix` existe) |
| revisão do RP | `RULE` de INSERT/UPDATE passava no boot e cada pedaço dava `0A000` `network`; `Deliver` conferia o `ctx` antes da recusa `logic`; `53300` antes da senha | conferir no catálogo o que o dreno não aceita; contagem e log da perda andam juntos |
| pendências 13–20 | token no log de conexão e URL não validada; negação de permissão retentada para sempre (`jetstream`) ou confirmada e perdida (`core`); `ErrMaxPayload` perdia o pedaço; contrato do stream só no boot; `interest` confirmava sem guardar; `10158`/`10167` fatais; credencial expirada `network`; dump de outro `space` perdido em silêncio | negação assíncrona vira veredito; o contrato vale a vida inteira; todo 429 é backpressure |
| revisão do R (NATS) | `10077` cheio/fechando fatal; `discard=new` abaixo de um registro aceito; negação do beacon mascarava a dos dados no `core` | o `nats.go` guarda só o último `-ERR` |
| revisão do RN (adaptada ao TLS do S) | TLS que nunca verifica depois do boot ficava `network` até os fusíveis; flush `core` que atravessava uma reconexão confirmava o pedaço | ler o `LastError` em `RECONNECTING`; `flushErr` + contagem de reconexões |
| pendências 21, 22 | `beacon_interval` de `1ns`; `?host=/socket` perdia a porta; `sslmode` repetido valia o primeiro; `addr` com dois hosts | chave dona da URL não perde informação; dois leitores leem a mesma coisa |
| pendência 30, revisões do R/RP/RN | erros de URL, consulta PG, INI e flags citavam a entrada (senhas); `@` depois de `/ ? #`; cauda de senha partida, valor colado ao nome, continuação no INI | mensagem de config cita posição, nome com forma de nome e dica, nunca o texto |
| revisão do RP/RN (config) | `--dump-config=false` despejava; `--config=` vazio caía no env; BOM; sem tetos (`makechan` em pânico, `pool_size` dando a volta no `int32`); `pool_size = workers` sem conexão para a sonda; derivados no dump; sinônimos de charset no `SIGHUP` | toda chave que dimensiona memória tem teto; a sonda é mais um cliente do pool; comparar valores canônicos |
| pendências 23–25 | dump escrito no nome final (parcial com cara de pronto); quarentena feita logada como não feita; gauges zerados com dump no disco | publicar só o completo (temporário, `link`, `fsync` do diretório); recontar do diretório |
| pendências 27–29, revisões do R/RN | recusa fatal logada como `boot_timeout`; primeiro `SIGTERM` abortava o shutdown por FATAL; aborto durante o dump subcontado, depois contado duas vezes, e dump já contado como perdido publicado | um dono só da contagem (`lossMu`, `enterDumpPhase`/`oweDump`, `SetAbandon`) |
| revisão do RN | `SIGQUIT` depois da leitura da flag ignorado; sinal antes do bind de um nome de host saía 1 | sinal tardio chega por canal; bind cancelado é 130 |
| revisão do R | `Remove` do dump sem `fsync` do diretório; health checks sem o contexto de vida; transportes fechados em sequência (um `closeGrace` cada) | `ErrNotDurable` também ao abrir o diretório; `withLifetime`; fechar todos em paralelo |

### Revisão independente do porte (2026-10-13)

Cinco revisores só de leitura sobre o porte (ciclo de vida e dump; config;
NATS; sinks SQL e Redis; recepção, conversão, ring e docs). Cada correção tem
teste que falhava antes, conferido por mutação (a correção desfeita mata o
teste).

| Achado por | Bug | Lição |
|---|---|---|
| revisão A (X1) | sinal nos passos 9–12 com bind de literal IP ou caminho `unix`: nenhum bind falha, o servidor abria, `/readyz` 200 e saía com 0 | o contexto dos sinais é conferido depois de **todos** os binds, não só quando um falha |
| revisão A (X2) | o aborto publica a marca antes de tomar o contador; o dump que via a marca no intervalo contava tudo como `emergency_failed` | quem toma o contador toma sob o mesmo mutex (`settleDump`) |
| revisão A | `ring=sqlite` + `sink=none` drenava o `ring.db` de uma vida com destino para o descarte | a regra dos dumps (decisão 26) vale para o backlog: fica, fora da conta |
| revisão B | teto de 16 Mi itens (do `makechan`) aplicado a `sqlite`/`redis`; `sink.batch` ≤ 65 536 recusava ClickHouse legítimo; `max_inflight` sem teto dava a volta no `MaxPending` | teto vale para quem aloca por ele; o ring **efetivo**, depois da degradação |
| revisão B | `?host=` vazio, TLS sobre socket do PostgreSQL e `,` no host Redis: a URL dizia uma coisa a cada leitor | toda forma que dois leitores leem diferente é recusada |
| revisão C | um nó de certificado ruim num pool virava veredito permanente quando o confiável reiniciava (o `LastError` é do último servidor tentado) | recusa de um servidor só é veredito quando não há outro |
| revisão C (X3) | `10077` "store is closed" (texto do nats-server) era fatal; o teste usava o texto inventado "store closed" | caso de teste com o texto do servidor, citado pelo arquivo e linha |
| revisão D | a sonda de saúde tirava a conexão do pool dos escritores; com `ring=none` o `+1` não protegia nada | a sonda tem conexão própria; o pool é dos escritores |
| revisão D | conta de memória por usuário do ClickHouse multiplicava pelo pool (contando a sonda); `1048` do MySQL era `logic`; nome de banco contado em bytes | multiplicar pelo que escreve; erro que o servidor nunca causa é da tabela; limites do servidor nas unidades dele |
| revisão D | testes de `autocommit` com `SET GLOBAL autocommit` no MySQL compartilhado | um teste não muda estado global do servidor compartilhado: `init_connect` num usuário próprio, ou a sessão pedida pelo próprio conector |
| revisão E | `proxy.protocol=optional`: linhas já lidas de um par sem cabeçalho descartadas sem contagem no `Close` (regra copiada do R) | só a **espera** pelo cabeçalho é do shutdown |
| revisão E | o buffer de frame acima de 16 KiB era solto depois de todo frame: 12 KiB de 62 µs para 583 µs | soltar antes de uma espera, não depois de um frame |
