# go-syslog-pg-server — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Escrito
em 2026-10-07, no corte a partir do `go-syslog-server`; os números herdados do
pai estão marcados como tais. Comandos a partir da pasta do módulo.

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. **Sem `SYSLOG_TEST_PG_DSN`**, a suíte de integração da
app e a do sink real pulam (§5, §6): só sobram as recusas de boot de antes do
passo 7 e os testes de pacote. Como o PostgreSQL é o único destino, a prova
ponta a ponta exige o banco; rode §6 antes de considerar uma mudança pronta.

Cobertura: `go test -cover -coverpkg=./internal/... ./...`, com e sem o banco.
Os números desta reescrita ainda não foram registrados (§8).

---

## 2. Vetores dourados, contratos e corpus (desvio D3, SPEC §18.2)

```bash
go test -race -run 'Golden|Schema|Corpus|Semantic' -v ./internal/convert/ ./internal/jlhcheck/
```

- **Os vetores** de `loghub-type-syslog-schema`
  (`internal/jlhcheck/loghub/vectors.json`): 23 de 24 (`arquivo-sem-pri` fica
  fora, `transport=file`). Cada entrada é convertida e comparada com o esperado
  (o `uuid` só pelo instante), e cada envelope esperado passa no oráculo.
- **O vetor com `transport=tls` roda como TCP** (não há TLS aqui): os mesmos
  bytes recebidos por TCP simples, `payload.tls` retirado do esperado e o `sid`
  recalculado pela fórmula de SPEC §6.10. A cobertura de conversão dele fica.
- **O oráculo** (`internal/jlhcheck`) é o porte em Go do `validate_payload.py`
  da skill: os dois schemas (`santhosh-tekuri/jsonschema/v6`, só em teste) mais
  as regras semânticas (ordem e número de chaves, chaves duplicadas, `level` =
  severidade, `sid` recalculado, instante do `uuid` = `event`, `iso` truncado,
  `from` UUIDv7 e o conjunto completo de espaço em branco nos campos `^\S+$`,
  D6).
- **Corpus de linhas reais** (`internal/convert/testdata/corpus/*.raw` +
  `*.want.json`, 23 pares): rsyslog, syslog-ng, journald, appliances de rede,
  log4j sem PRI, Cisco com contador de sequência, BOM, cp1252, SD com escapes e
  malformadas. Cada saída também passa no oráculo. O teste falha abaixo de
  vinte entradas.
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
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=600s
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=600s
```

O `^…$` importa: `-fuzz=FuzzFrame` casa também `FuzzFrameOracle` e o Go recusa
dois alvos.

| Alvo | O que prova | Situação |
|---|---|---|
| `FuzzConvert` | a conversão nunca falha nem entra em pânico; toda saída passa no oráculo; a mesma entrada dá os mesmos bytes (SPEC §3.5) | herdado do pai sem mudança de conversor (o corte só tirou as colunas promovidas); rodada do pai: 5 min, 2,27 M execs, sem falha |
| `FuzzSD` | STRUCTURED-DATA: tetos respeitados, nada perdido em `sd-invalid` | herdado; rodada do pai: 5 min, 12,3 M execs |
| `FuzzFrame` | as molduras não perdem nem inventam bytes | herdado; rodada do pai: 5 min, 1,48 M execs |
| `FuzzFrameOracle` | a moldura contra um oráculo independente | herdado; rodada do pai: 5 min, 3,67 M execs |
| `FuzzProxy` | PROXY v1/v2: nunca aloca pelo declarado | herdado (agora só TCP); rodada do pai: 5 min, 2,42 M execs |
| `FuzzRead` | **novo nesta reescrita**: o leitor JSON Lines tolera truncamento e lixo sem pânico, nunca guarda uma linha acima do teto, e tudo o que `ParseLine` aceita a fila aceitaria | sem rodada longa (§8) |
| `FuzzJSONBSafe` | a troca do `\u0000` para a `loghub.inbox` só troca escapes reais | herdado; rodada do pai: 20 s |
| `FuzzInboxPayload` | o caminho real da `loghub.inbox`: JSON sem escape NUL, `raw` exato em base64, os demais textos com U+FFFD | herdado; rodada do pai: 40 s |

**`FuzzHead` saiu** com o codec do cabeçalho do item (SPEC §3.4): nada mais
persiste um cabeçalho.

`FuzzRead` é o alvo que mais importa aqui: a reingestão **não converte**, e
`emergency.ParseLine` é a única porta entre um arquivo em disco e as colunas da
fila (SPEC §12.2). Uma linha que passasse por ela e a fila recusasse derrubaria
como `logic` o pedaço inteiro, com os envelopes íntegros junto.

---

## 4. Benchmarks

```bash
go test -run XXX -bench . -benchmem ./internal/convert ./internal/receiver
```

| Benchmark | Número do pai (M2, 2026-10-05) | Leitura |
|---|---|---|
| `Convert/rfc3164` | 764 ns/op, 0 allocs | conversão + JLH completo, no buffer do pedaço |
| `Convert/rfc5424` | 1,09 µs/op, 0 allocs | idem, com SD |
| `SchemaValidate/rfc5424` | 97 µs/op, 1 922 allocs | o custo que D3 tira do caminho quente |
| `IngestAccept` | 137 ns/op, 2 allocs | recepção até o ring (moldura já feita) |

**Medido nesta reescrita** (2026-10-07, Apple M2, máquina compartilhada e
carregada por outras suítes; leia como ordem de grandeza):

| Benchmark | Aqui | Pai na mesma máquina, rodada quieta |
|---|---|---|
| `IngestAccept` (mensagem aceita) | ~110–134 ns/op, 224 B/op, **1** alloc/op | ~134 ns/op, 280 B/op, 2 allocs/op |
| recusa com o ring cheio (pré-checagem, SPEC §2.2) | ~25 ns, 0 alloc | ~115 ns, 2 allocs (sem pré-checagem) |

A alocação a menos é a caixa `*[]byte` do buffer do pool, que agora volta com o
buffer (SPEC §15.1); a que sobra é o próprio `Item`. Os benchmarks de conversão
não foram remedidos (o conversor não mudou além de perder as colunas
promovidas). Os de rate-limit saíram com ele.

---

## 5. Integração em processo (`internal/app`)

```bash
SYSLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
  go test -race -timeout 900s ./internal/app/
go test -race -short ./internal/app/      # pula o teste do binário real
```

A app inteira sobe em processo (identidade fixa, `DATADIR` temporário, portas
efêmeras) contra o PostgreSQL de §6; cada teste cria a própria fila a partir de
`docker/postgres/inbox.sql` num esquema próprio e o remove no fim. Falhas do
destino são injetadas decorando o sink real (`App.wrapSink`). Todo envelope
lido de volta da fila ou de um dump passa pelo oráculo (`mustJLH`).
Cobre:

- **Transportes:** UDP com `dst-ip` por PKTINFO, TCP LF e octet-counting, unix
  (sem `host`, `sid = 0`, socket removido no shutdown e nunca roubado de um
  processo vivo), vários listeners por transporte, PROXY v1 e v2 de par
  confiável (`x-proxy`, `sid` recalculado).
- **Fidelidade:** a linha da fila igual ao JLH como valor JSON (com NUL: `raw`
  em base64, U+FFFD nos demais textos), `payload_raw=false` mantém o `raw`
  obrigatório, truncamento mantém a moldura sincronizada, mensagens vazias
  contadas e não escritas, malformada recusada com `accept_malformed=false`.
- **Ciclo de vida:** recusas de boot com exit 1 (as chaves do pai na linha de
  comando, `--sink.type`, `--ring.mode`, `--listen.tls`..., no teste do
  binário), banco inalcançável mantém os transportes fechados e sai
  com 3 no `boot_timeout`, validação da fila recusada sai com 3, banco caindo e
  voltando sem perda, shutdown gracioso entrega tudo, segundo sinal dá 130 e
  conta `abort`, `SIGHUP` aplica/ignora/recusa conforme SPEC §16.3, arquivo
  inválido no reload não aplica nada.
- **Emergência:** `ClassFatal` no `Write` e `drain_timeout` estourado despejam
  e saem com 3; o dump é JSON Lines com o JLH exato, e a vida seguinte
  (outro `from`) o reingere com os mesmos `uuid` e o `from` da primeira; as
  malformadas ficam de fora com `accept_malformed=false`; a reingestão acontece
  mesmo com `emergency.enabled=false`; `emergency.enabled=false` no incidente
  conta `emergency_failed`; um dump danificado entrega as linhas íntegras e vai
  para `.corrupt`; a entrega do boot segue as classes pedaço a pedaço; sink
  FATAL no dreno sai com 3 na hora.
- **Saturação:** ring cheio por transporte (UDP conta `ring_full`, TCP segura e
  não perde), sink DOWN por transporte, o teto de itens conta o lote retido
  (`TestRingItemsNeverExceedTheCapacity`), `/readyz` com o motivo.
- **Admin:** `/healthz`, `/readyz` com transições, as três métricas `_info`
  (`ring_mode="memory"`, `sink_type="postgres"`), toda série de SPEC §14.1
  presente, labels só de conjunto fechado, `le` estável entre leituras.
- **Concorrência:** conexões TCP e emissores UDP simultâneos: aceitas = linhas
  na fila, sem duplicata.
- **O binário real** (`TestBinaryIdentityExitCodes`,
  `TestBinaryVersionAndConfigErrors`, `TestBinarySignalsOutsideTheRunWindow`,
  `TestBinarySIGHUPAppliesTheLogLevel`): códigos 100–114 do `go-loghub-ident`,
  `DATADIR` relativo resolvido antes do `Initialize()`, `--version`, `SIGHUP`
  no boot logado e ignorado, `SIGQUIT` dá 130, `SIGHUP` no ar troca o
  `log.level`.

Sem banco, os testes de pacote ainda provam a configuração (registry, chaves do
pai recusadas na linha de comando e avisadas no env, `LOGHUB_EB_URL` com a regra
do `sslmode`), o ring, os workers e a saúde com sinks falsos, a recepção e o
leitor do dump.

---

## 6. PostgreSQL (`loghub.inbox`)

No host de desenvolvimento já existe o contêiner `lh-pg18` com esse DSN. Sem
ele, um descartável:

```bash
docker run --rm -d -p 15432:5432 -e POSTGRES_USER=sls -e POSTGRES_PASSWORD=sls \
  -e POSTGRES_DB=sls_test --name slpg-test-pg postgres:18-alpine
SYSLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
  go test -race -timeout 900s ./internal/sink/ ./internal/app/
docker rm -f slpg-test-pg
```

O DSN é sem TLS, como tudo aqui. PostgreSQL 18: a DDL da skill usa
`uuid_extract_version` e `COMPRESSION lz4`. Cada teste cria a fila a partir de
`docker/postgres/inbox.sql` num esquema próprio.

A suíte do sink prova o `unnest` de dez arrays (`jsonb[]` e `timestamptz[]`
inclusive), `ON CONFLICT (uuid)` com reenvio e uuid repetido no mesmo pedaço
(contados em `syslog_sink_postgres_duplicates_total`), as CHECKs da skill
aceitando tudo o que o servidor produz (NUL, unix, outro `space`), o envelope
reconstruído da linha — com o SQL da skill — igual ao JLH e válido no oráculo,
o consumo `FOR UPDATE SKIP LOCKED` em ordem de `uuid`, a escrita de envelopes
**reingeridos** (colunas tiradas do cabeçalho da linha,
`TestPostgresWritesReingestedEnvelopes`), o `PGSSLMODE=require` do ambiente
ignorado (`TestPostgresIgnoresPGSSLMODE`), a classificação por SQLSTATE (classe
`42` fatal, senha errada fatal, sessão somente leitura fatal) e a validação do
boot: tabela, tipos, índice único exatamente sobre `(uuid)`, outros índices
únicos e exclusão recusados, tabela particionada recusada, banco `LATIN1`
recusado, privilégio, coluna extra obrigatória.

Sem servidor: `TestPostgresInboxFileIsTheSkillDDL` confere o hash da DDL da
skill em `docker/postgres/inbox.sql` (não edite o bloco da DDL),
`TestNewPostgresIgnoresTheLibpqEnvironment` o pool livre do ambiente libpq e
sem TLS, `TestClassifySQLState` a tabela de SPEC §10.5, e `FuzzInboxPayload` e
`FuzzJSONBSafe` a troca do NUL. Em `internal/config`, `TestEBURLRefusesTLS`
prova que `sslmode=require`, `verify-ca` e `verify-full` são exit 1 e que
`disable`, `allow`, `prefer` e a ausência passam.

---

## 7. Emissores reais (SPEC §18.7)

```bash
SYSLOG_TEST_EMITTERS=1 SYSLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
  go test -race -run Emitters -timeout 600s ./internal/app/
```

Precisa de Docker (baixa pacotes). rsyslog por UDP, TCP LF (`ForwardFormat`) e
TCP octet-counting (`Protocol23`); syslog-ng `network()` TCP e UDP e `syslog()`
octet-counting. Mensagens de cada, com `appname`, facility e severidade
conferidos na fila. O cenário TLS `gtls` do pai saiu com o TLS.

---

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento.

1. **Fuzz de 10 min por alvo não rodou** nesta reescrita. Os números de §3 são
   do pai (5 min por alvo, menos para os dois do NUL); o `FuzzRead` do leitor
   JSON Lines é novo e não teve rodada longa nenhuma.
2. **Carga nunca medida.** Não há gerador de carga; os critérios de SPEC §18.8
   são alvos. De §4, só a recepção foi remedida depois do corte, numa máquina
   carregada.
3. **Réplica em recuperação não testada.** O FATAL no boot de um servidor em
   recuperação (`pg_is_in_recovery()`) e o DOWN depois dele exigem uma réplica
   de verdade; só o primário foi exercitado.
4. **A revisão independente** de 2026-10-07 achou e corrigiu os itens de §9
   ("Revisão independente"); os que ficaram estão no item 12. A conferência por
   mutação de cada correção, como no pai (§7 dele), não está registrada aqui.
5. **Cobertura** desta reescrita não registrada (§1).
6. **`kill -9` de verdade** não é testável em processo; a perda do backlog em
   memória é a documentada em SPEC §19.
7. **Dump grande.** O dump de um ring cheio (até 512 MiB de cru, JLH até 12×)
   não foi medido em tempo nem em disco; os testes usam rings pequenos.
8. **O caminho `IP_PKTINFO` do Linux** só rodou na suíte da recepção do pai
   (Docker); aqui, no macOS.
9. **`/dev/log` de verdade** não foi testado; os testes usam um socket em
   diretório temporário.
10. **A regra do `/data`** (nada escrito fora de `server.data_dir`, exceto o
    socket unix) não tem teste automatizado de contêiner com raiz somente
    leitura.
11. **`pg_hba.conf` só com `hostssl`** (o login recusado com classe `28`, fatal no
    boot) é dedução do código e da documentação do PostgreSQL, não teste.
12. **Conhecidos e não corrigidos** (achados da revisão independente de
    2026-10-07, deixados para depois):
    - o `SIGHUP` relata como "requires a restart" chaves **derivadas**
      (`sink.postgres.pool_size` quando muda `sink.workers`, `emergency.dir`
      quando muda `server.data_dir`) e os sinônimos do charset legado
      (`windows-1252` × `cp1252`);
    - o `--dump-config` imprime o `pool_size` derivado sem o comentário de
      origem;
    - `proxy.*` e `tcp.*` são aceitos com `listen.tcp` vazio, sem aviso;
    - `listen.tcp` e `admin.bind` na mesma porta só falham ao abrir o
      transporte, não na validação;
    - uma falha ao remover um dump já entregue só é logada: o arquivo volta no
      boot seguinte e é entregue de novo, deduplicado só enquanto o `uuid` ainda
      estiver na fila;
    - `ring.max_bytes` aceita valores em que nenhuma mensagem cabe (cada item
      custa ao menos 2 KiB + 256 B, SPEC §15.1): um emissor TCP é segurado,
      desconectado no `backpressure_timeout` e reconecta para sempre. Herdado
      do pai;
    - os contadores da recepção não têm *padding*: com vários listeners
      `SO_REUSEPORT` eles disputam a mesma linha de cache (revisão C da
      recepção, 2026-10-07, que não achou defeito de correção).
13. **Uma brecha deixada no parser de flags** (SPEC §13.2, como no N e no
    `go-syslog-nats-server`): a cauda de um valor que o shell partiu num espaço,
    quando começa com traço e vem depois de um valor não vazio
    (`--sink.postgres.password=a -b`), sai como parâmetro desconhecido, com o
    nome. Escondê-la esconderia todo erro de digitação de uma flag depois de
    outra.
14. **Defeitos conhecidos do código**, cada um com a consequência e a direção da
    correção, ficam em SPEC §22.1; a correção de um deles vem com o teste que o
    reproduz.

---

## 9. Histórico

Os bugs que os testes e as revisões acharam no código herdado estão em
`../go-syslog-server/docs/TESTING.md` §9; as lições que generalizam estão em
`AGENTS.md` §9.

Endurecimentos feitos no corte, todos na porta do dump (`emergency.ParseLine`,
SPEC §12.2), porque a reingestão deixou de converter e uma linha que a fila
recusasse derrubaria o pedaço inteiro:

| Regra | O que ela fecha |
|---|---|
| cabeçalho byte a byte igual ao de `jlh.AppendHeader` | chave duplicada, espaço, ordem ou grafia de número que o JSON aceita e este servidor nunca escreve |
| escapes `\u` só em `\u0000`–`\u001f`, minúsculos | *surrogate* solto, que o `jsonb` recusa com `22P05` |
| profundidade ≤ 8 | aninhamento que nenhum payload `syslog` tem |
| `payload` objeto com `message` string | a CHECK `inbox_syslog_ck` |
| payload cortado por posição igual ao payload do parser | uma linha que poria na fila um objeto diferente do que o cabeçalho descreve |

### Revisão independente (2026-10-07)

| Achado | Consequência sem a correção | Correção |
|---|---|---|
| a config do pool lia o ambiente libpq antes de sobrescrever os campos | `PGSSLMODE=bogus`, `PGSSLROOTCERT` ilegível, `PGSERVICE` inexistente ou `PGPORT=abc`: exit 1; `PGCHANNELBINDING=require` ou `PGREQUIREAUTH=md5`: toda conexão recusada sem SQLSTATE, retentada como `network` até o `boot_timeout` | `parseWithoutLibpqEnv`: todo `PG*` escondido durante a leitura (SPEC §10.5) |
| SQLSTATE `3D` (banco inexistente) era `network` | o boot esperava o `boot_timeout` inteiro | `3D` fatal |
| `LOGHUB_EB_URL` com parâmetro repetido | `sslmode=disable&sslmode=require` passava pela recusa do TLS (`Get` lê o primeiro, a libpq o último) | parâmetro repetido é exit 1; `addr` validado como um servidor só; a porta da URL fica com `?host=/dir` |
| dump escrito direto no nome final | um dump cortado numa fronteira de linha parecia limpo, era entregue em parte e removido; outra instância podia pegar um arquivo em escrita | temporário oculto + `link` + `fsync` do diretório (SPEC §12.1) |
| reingestão carregava o arquivo inteiro | um dump de ~12× o ring matava o boot por OOM, em laço | leitura em fluxo, um `boot_timeout` só (SPEC §12.2) |
| `ParseLine` decodificava o payload com `encoding/json` | `"Message"` casava o campo `message` (o `?` do `jsonb` não casa); `1e1000000` passava e o `jsonb` recusava com `22003`, o pedaço inteiro | só o cabeçalho é decodificado; o payload é percorrido token a token; números só inteiros de até 20 dígitos |
| shutdown por FATAL abortava no primeiro `SIGTERM` | o orquestrador cortava o dump que salvava o ring | o primeiro sinal é tolerado; e o abort conta em `abort` o que já fora entregue ao dump |

### Porte do `go-webhook-nats-server` (2026-10-11)

A revisão independente do `go-webhook-nats-server` (o N, `../go-webhook-nats-server/docs/TESTING.md`
§9) achou no dump por temporário e `link` um defeito que este módulo também
tinha; a correção foi portada aqui e no `go-syslog-nats-server` (que copia esta
emergência), com o mesmo teste e mutação (a correção revertida derruba o teste).

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Sem *hard link* (vfat, exFAT, FUSE de *object store*), o `link` falhava sempre: agora, num erro que não seja `EEXIST`, o nome final é conferido livre (`lstat`) e o temporário é renomeado (SPEC §12.1) | todo dump falhava, e o ring inteiro se perdia onde a escrita no lugar funcionava | `TestWriteOnAFileSystemWithoutHardLinks` |
| Temporário de dump interrompido nunca era informado; o gêmeo de um dump ligado ficava (o `Leftovers` do N, trazido pela revisão independente deste porte) | um dump interrompido invisível ao operador, cujas mensagens, depois de um `SIGKILL`, ninguém contou | `TestLeftovers`, `TestBootRemovesTheTwinOfADumpAndReportsAnInterruptedOne` |

O N também tornou o `fsync` do diretório melhor esforço; aqui ele **continua
decidindo o dump** (SPEC §12.3, o comentário de `syncDir`), também no caminho
do `rename`: o mesmo teste prova que um dump renomeado cujo `fsync` do diretório
falha é removido e é falha.

### Parsers sem eco (2026-10-11)

O porte das correções do N trouxe ao `go-syslog-nats-server` os parsers de INI e
de flags sem eco, e as revisões do dia acharam mais cinco caminhos no parser de
flags; nada disso tinha vindo para cá (lista "A portar" da entrada do porte de
volta ao N, no `CHANGELOG.md` da raiz). O `flags.go` agora é o do N, e o
`ini.go` o do `go-syslog-nats-server`; a mesma forma no INI veio junto, porque
o segredo saía igual. Cada caminho tem teste, e cada correção foi revertida
numa cópia do módulo (mutação) e derrubou o teste.

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Linha de INI sem `=` (`password: x`) ou cabeçalho sem `]` citados no erro; `sink.postgres.password=x` sem os traços citado inteiro | O segredo no stderr do contêiner, no boot e a cada `SIGHUP` | `TestParserErrorsDoNotEchoValues` |
| Uma flag sem valor engolia a seguinte (`--sink.workers --sink.postgres.password=x`, um `$WORKERS` vazio num script), e o valor digitado depois de um espaço (`--sink.postgres.password= x`) virava argumento solto citado | A senha no erro de tipo ou no "unexpected argument" | `TestParserErrorsDoNotEchoValues` |
| `--sink.postgres.password --x` "nomeava" a flag `--x`; `--sink.postgres.password= -x` saía como parâmetro desconhecido; a cauda de um valor partido pelo shell (`--sink.postgres.password=$P`) saía como argumento solto | O segredo no stderr do contêiner | `TestParserErrorsDoNotEchoValues` (e controles: `-1` é valor, `-h` é flag, um erro de digitação depois de um valor continua nomeado, e cada recusa diz o que houve) |

A mutação deste porte achou um caminho do `flags.go` sem teste, também no N:
tirar o `assigned(v)` do `--config` não derrubava nada. Caso novo,
`--config= x` (o argumento depois do `=` vazio não é citado).
