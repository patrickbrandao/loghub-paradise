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
SYSLOG_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race -timeout 600s ./internal/ring/ ./internal/sink/ ./internal/app/
docker rm -f sls-test-redis
```

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
`PGTARGETSESSIONATTRS` e `PGAPPNAME` (os demais `PG*` ainda chegam: SPEC §22.1), e
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

