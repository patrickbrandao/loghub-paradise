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

**Leitor de 4 KiB e `frameKeepCap`** (2026-10-14, SPEC §5.4; Apple M2,
máquina compartilhada, três rodadas cada, leia como ordem de grandeza):

| Benchmark | 4 KiB (atual) | 64 KiB (antes) |
|---|---|---|
| `TCPReceive/octet` (~200 B, socket real até o ring) | 1,2–3,2 µs/op, 1 alloc/op | 1,2–1,7 µs/op, 1 alloc/op |
| `TCPReceive/lf` | 2,0–3,6 µs/op, 1 alloc/op | 1,6–2,3 µs/op, 1 alloc/op |
| `Framer12K` (64 frames de 12 KiB pelo leitor de 4 KiB) | 194–235 µs/op, 78 KB/op, 5 allocs/op | — |

A dispersão entre rodadas é maior que a diferença entre os dois tamanhos: sem
perda mensurável nesta máquina, como no `go-syslog-server`. O `Framer12K` fica
nos 78 KB e 5 alocações que a forma "solta só com o leitor vazio" promete (a
forma que soltava a cada frame chegava a 4,7 MB no S). Custo de uma conexão
TCP ociosa (`TestIdleTCPConnectionFixedCost`, 200 conexões): ~5,6–5,8 KiB de
heap, contra ~66 KiB antes.

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
`42` fatal, senha errada fatal, sessão somente leitura fatal, `23502` de uma
coluna obrigatória acrescentada depois do boot fatal,
`TestPostgresNotNullViolationIsFatal`), a validação do boot com prazo próprio
atrás de um proxy que atrasa cada pacote (`TestPostgresValidationHasItsOwnDeadline`:
passa de um prazo de checagem curto, e um boot cancelado no meio a interrompe) e
a validação do boot: tabela, tipos, índice único exatamente sobre `(uuid)`, outros índices
únicos e exclusão recusados, tabela particionada recusada, banco `LATIN1`
recusado, privilégio, coluna extra obrigatória.

Sem servidor: `TestPostgresInboxFileIsTheSkillDDL` confere o hash da DDL da
skill em `docker/postgres/inbox.sql` (não edite o bloco da DDL),
`TestNewPostgresIgnoresTheLibpqEnvironment` o pool livre do ambiente libpq e
sem TLS, `TestClassifySQLState` a tabela de SPEC §10.5 (`23502` fatal inclusive),
`TestValidationContext*` e `TestHealthChecksCarryTheirLifetime` o prazo próprio
da validação e o cancelamento que ainda chega a ela, e `FuzzInboxPayload` e
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
    - `proxy.*` e `tcp.*` são aceitos com `listen.tcp` vazio, sem aviso.

    Fechados no porte das pendências do RP/RN (2026-10-14, §9): o `SIGHUP` não
    relata mais chave **derivada** (SPEC §16.3); `listen.tcp` e `admin.bind` na
    mesma porta são exit 1 na validação (§13.2); `ring.max_bytes` abaixo do
    custo de uma mensagem de `syslog.max_message_bytes` é exit 1 (§13.2,
    §15.1); a falha ao remover um dump já entregue continua só `ERROR`, agora
    como regra (§12.2, `TestRemovalFailureIsOnlyAnError`). Os contadores sem
    *padding* viraram o item 17.
13. **Uma brecha deixada no parser de flags** (SPEC §13.2, como no N e no
    `go-syslog-nats-server`), estreitada em 2026-10-13: a cauda de um valor que o
    shell partiu num espaço, quando começa com traço, vem depois de um valor não
    vazio **e** tem a forma `<seção>.<chave>` de uma seção deste servidor ou do
    pai (`--sink.postgres.password=a --sink.postgres.b`), sai como parâmetro
    desconhecido, com o nome. Escondê-la esconderia os erros de digitação dentro
    de uma seção conhecida.
14. **Defeitos conhecidos do código**, cada um com a consequência e a direção da
    correção, ficam em SPEC §22.1; a correção de um deles vem com o teste que o
    reproduz. Em 2026-10-14 os quatro de lá foram fechados (§9, "Pendências
    conhecidas fechadas") e, no mesmo dia, o porte das pendências do RP/RN
    fechou mais seis, registrados lá como corrigidos ou decididos (§9, "Porte
    das pendências do RP/RN"): nenhum aberto.
15. **O custo por conexão é medido no heap, não no RSS.**
    `TestIdleTCPConnectionFixedCost` mede `HeapAlloc` depois de dois GC, com
    as duas pontas no mesmo processo; a pilha da goroutine de cada conexão
    (SPEC §5.4) e a memória do kernel por socket ficam fora. Nenhuma prova com
    milhares de conexões reais nem sob `GOMEMLIMIT`.
16. **O prazo vencido no meio da assinatura PROXY** (SPEC §4.6, aceito em
    2026-10-14) não tem teste próprio: o comportamento é o do parser
    (`TestProxyAbsentConsumesNothing` cobre o "não é cabeçalho"), não o de um
    `header_timeout` vencendo depois de `PRO`.
17. **Os contadores da recepção não têm *padding*** (revisão C da recepção,
    2026-10-07; registrado como lacuna de medição no porte do RP/RN,
    2026-10-14): com vários listeners `SO_REUSEPORT` eles disputam a mesma
    linha de cache (*true sharing* da mesma palavra, não *false sharing*: são
    os mesmos contadores). Sem defeito de correção observável; nunca medido como
    gargalo. O benchmark de recepção de §4 com `tcp_listeners` > 1 é o lugar de
    medir antes de mudar.
18. **O "até 8 KiB quando ociosa" vale só entre frames** (revisão independente
    de 2026-10-14, A5 e D4; SPEC §5.4/§15.1): um frame de 17–64 KiB seguido de
    alguns bytes do próximo segura a classe de 64 KiB até o idle. Não
    corrigido (a soltura no ponto de espera custou 10× em fluxo contínuo de
    frames de 24 KiB) e sem teste que fixe o número; o teto é `max_conns` e o
    `GOMEMLIMIT`.
19. **`sameStreamSocket` com nome de serviço** (revisão de 2026-10-14) é
    provado com `http` (a tabela embutida do Go) e com um nome inexistente; um
    nome que só o `/etc/services` conhece (`syslog`) depende do arquivo da
    máquina ou da imagem, e na imagem sem ele é recusado na validação, como o
    bind o recusaria.
20. **O PROXY `required` com cabeçalho válido no shutdown** (A1 de 2026-10-14)
    é provado no `readProxy` com o leitor cheio e `closing` ligado à mão, não
    com um `Close` de verdade correndo contra uma conexão do balanceador.

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

### Porte das correções do RP e do R (2026-10-13)

As correções das revisões independentes do `go-relplog-pg-server` (RP) e do
`go-relplog-server` (R) que valiam aqui (CHANGELOG da raiz, "A portar" das
entradas "2026-10-13 — RP, RN" e "2026-10-12 — R"), portadas com os testes do
RP. **Conferência por mutação:** os testes novos de `internal/config` rodaram
contra o código de config de antes (numa cópia do módulo) e cada um falhou; no
resto, cada correção foi revertida sozinha numa cópia do módulo, e os 20
mutantes (`A1`–`A13`, `E1`–`E2`, `R1`–`R2`, `K1`–`K3`) derrubaram os testes da
tabela. `-race` verde com PostgreSQL 18.

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Abort durante a coleta das sobras ou entre as passagens do contador contava o ring e deixava o contador para um dump falho ou desligado contar de novo; abort com itens drenados contava só o lote retido (SPEC §13.4) | `abort` + `emergency_failed` somando 2N, ou contando 1 de 9 | `TestAbortDuringTheDrainIsCountedOnce`, `TestAbortBeforeTheDumpPhaseKeepsItClosed`, `TestAbortDuringTheCollectionCountsOnceAndDumpsNothing`, `TestAbortDuringTheDumpWithItemsDrainedFromTheRing`, `TestAbortBetweenTheDrainAndTheDumpCountsEverything`, `TestAbortAndFailedDumpCountOnce`, `TestAbortAndDisabledDumpCountOnce`, `TestAbortAfterASavedDumpCountsNothing` |
| Dump publicado depois de o abort o contar como perdido (SPEC §12.3) | o boot seguinte reingeria mensagens contadas como perdidas | `TestDumpAfterTheAbortIsAbandoned`, `TestAbortDuringTheCollectionPublishesNoDump`, `TestAbortAfterTheLastAbandonCheckCountsNothingMore`, `TestFatalShutdownSignals` |
| `SIGQUIT` tolerado no shutdown por FATAL; `SIGQUIT` entre o início do shutdown e o observador do segundo sinal perdido (antes e depois da leitura de `quitting`) | dump até o fim contra o "aborta sempre"; o `SIGQUIT` não abortava | `TestFatalShutdownSignals`, `TestLateSIGQUITStillAborts`, `TestSIGQUITAfterTheEntryCheckStillAborts`, `TestSIGQUITWhileServingCountsTheRing` |
| Sinal durante o `bind` de um nome de host (admin ou transporte) | exit 1 culpando o endereço, em vez de 130 | `TestCancelledBindIsAnAbortNotAConfigError`, `TestSignalBeforeTheBindExits130` |
| `Remove`/`Quarantine` com o `fsync` (ou a abertura) do diretório falho depois do `unlink`/`rename` | log falso de "cannot remove"/"cannot quarantine", sem as contagens (SPEC §22.1 item 8 de antes) | `TestRemoveWithAnUnopenableDirectoryIsRemoved`, `TestRemoveSyncsTheDirectory`, `TestQuarantineWithAFailedDirectoryFsyncIsQuarantined` |
| Prazo único da reingestão desde o início | reingestão saudável e longa saía com 3 no primeiro soluço | `TestLongReingestionStillRetriesATransientFailure` |
| `syslog_emergency_events_pending` posto no tamanho do pedaço; gauges zerados no fim; dump do shutdown os substituía | a última leitura mostrava um pedaço por um dump; alerta silenciado com dump no disco (item 9 de antes) | `TestPendingGaugeIsNotTheChunkDuringReingestion`, `TestReingestionDoesNotShowTheChunkAsPending`, `TestShutdownDumpAddsToTheGauges`, `TestCount` |
| Pré-checagem do ring cheio só pelo teto de itens | com `ring.max_bytes` limitando, cada retentativa e cada datagrama cunhava `uuid`, copiava e montava o item | `TestIngestByteCeilingIsRefusedBeforeTheCopy`, `TestIngestByteCeilingChargesTheTruncatedLine`, `TestIngestClosedFullRingIsTheShutdown`; e a regra que **não** veio (`TestIngestCancelledContextStillEnqueues`) |
| `Deliver` conferia o contexto antes da recusa `logic` | um SIGTERM na reingestão contava a perda sem o log dos uuids | `TestDeliverLogicRefusalWinsOverTheCancellation` |
| `RULE` de INSERT/UPDATE na fila passava no boot; `0A000` era `network` | todo pedaço falhava para sempre com o sink UP | `TestPostgresRulesAreRefusedAtBoot`, `TestClassifySQLState` |
| `pool_size = workers` aceito | sonda sem conexão: DOWN e FATAL com o banco sadio | `TestSinkWorkersAndPool`, `TestPostgresHealthHasAConnectionWhileTheWorkersWrite` (a propriedade, no banco real) |
| Sem tetos de config | pânico no boot, gigabyte por `Dequeue`, `int32` dando a volta, `Bind` acima de 1 GiB sem SQLSTATE | `TestConfigCeilings` |
| Valor colado ao nome, *switch* com valor, `--config=` vazio, BOM, continuação de segredo no INI, cauda de valor nomeada fora de seção conhecida, flag removida depois de uma com valor, `-h` depois de flag que quer valor | segredo no stderr; `--dump-config=false` despejava; `--config=` caía no env; linha 1 inválida sem motivo visível | `TestGluedValueIsNotPrinted`, `TestSwitchesTakeNoValue`, `TestEmptyConfigFlagIsRefused`, `TestINIByteOrderMarkIsIgnored`, `TestINIUnknownKeyNeverPrintsASecretContinuation`, `TestRemovedFlagAfterAValuedFlagIsNamed`, `TestRemovedParamsAreTheRemovedEnv`, `TestParserErrorsDoNotEchoValues`, `TestReviewFlagTailEcho` |
| `LOGHUB_EB_URL`: sem usuário/banco, `sslmode=` vazio, `host=` com `,`/`:`, `@` depois de `/?#`, fragmento, host com escape, só espaços, texto do parser e nomes de chave no erro | segundo banco por adivinhação; senha no stderr; `?sslmode=require` escondido no fragmento (item 7 de antes) | `TestEBURLMustNameItsParts`, `TestSlashInTheUserinfoIsRefusedWithoutEcho`, `TestReviewPGFragmentDumpLeaks`, `TestConfigErrorsDoNotEchoSecrets`, `TestEBURLMapping`, `TestEBURLRefusesTLS` |
| `sink.postgres.addr` com tab, `[h:5432]:5433` ou socket com `,` | um nome DNS que ninguém resolve, retentado o `boot_timeout` | `TestPostgresAddrNamesOneServer` |
| Derivado escrito como chave no `--dump-config`; sinônimos do charset como mudança no `SIGHUP` | o dump relido congelava o derivado; restart pedido para nada | `TestDumpCommentsDerivedValues`, `TestCharsetSynonymsAreOneValue` |

### `23502` e o prazo da validação (2026-10-13)

Os itens 5 e 6 de SPEC §22.1, portados do `go-relplog-pg-server` com os testes
dele (`review_port_rp_test.go`, `review_port_rp_pg_test.go`). **Conferência por
mutação**, cada uma sozinha e revertida: sem o `case "23502"` caem
`TestClassifySQLState` e `TestPostgresNotNullViolationIsFatal`; com
`p.validate(ctx)` no lugar do contexto próprio cai
`TestPostgresValidationHasItsOwnDeadline`; sem `withLifetime` no `Health` cai
`TestHealthChecksCarryTheirLifetime`; com a vida ignorada em
`validationContext` cai `TestValidationContextFollowsABootCancelledAfterTheCheckDeadline`;
com um `WithoutCancel` puro caem os dois `FollowsA…` e o subteste do boot
cancelado no banco real. `-race` verde com PostgreSQL 18. Não provado por
teste: o `ValidateTimeout: a.cfg.Sink.OpTimeout` do app, que hoje coincide com o
padrão (`ConnectTimeout`, também `op_timeout`).

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| `23502` em execução era `logic` | coluna `NOT NULL` acrescentada depois do boot: todo pedaço descartado com o sink UP, para sempre | `TestClassifySQLState`, `TestPostgresNotNullViolationIsFatal` |
| Validação do boot no `sink.health_timeout` (1 s) | a ~55 ms de RTT nenhuma tentativa terminava; exit 3 no `boot_timeout` culpando a rede | `TestPostgresValidationHasItsOwnDeadline`, `TestValidationContext*`, `TestHealthChecksCarryTheirLifetime` |

### Correções transversais da revisão independente do `go-syslog-server` (2026-10-13)

Os itens transversais (X1–X4) do plano da revisão independente do porte do
`go-syslog-server` (S), e a conferência de C3, D1, E1, E2 e A2-minor; a mesma
implementação no irmão (regra de sincronia, `diff` conferido). **Conferência
por mutação**, cada correção revertida sozinha: o teste da linha falha
(`exited 0, want 130` nos dois subtestes; `abort 0, emergency_failed 5 for
5`; `err = <nil>` no `?host=`).

| Item | Achado | Consequência sem a correção | Teste |
|---|---|---|---|
| X1 (A1 do S) | Sinal depois da reingestão e antes do *accept* com IP literal ou socket unix: nenhum *bind* falha | o servidor abria, o `/readyz` dava 200 e o shutdown saía **0**, não 130 (SPEC §13.1, §13.5) | `TestSignalBeforeALiteralBindExits130` (IPs literais e socket unix) |
| X2 (A2 do S) | O aborto publica `aborted` antes de tomar o contador; o dump que via a marca nesse meio (abandonado, ou desligado) o tomava primeiro | `emergency_failed = n`, `abort = 0`: a perda contada uma vez, atribuída ao disco (SPEC §13.4) | `TestAbortInsideTheDumpKeepsItsReason` (dump abandonado e dump desligado; o gancho `inAbort` força a intercalação) |
| X4 (B4 do S) | `?host=` vazio na `LOGHUB_EB_URL` aceito: a libpq e o pgx discam o padrão deles (`pgx.ParseConfig` dá `/private/tmp`), este servidor o host da autoridade | uma URL, dois servidores (SPEC §10.9) | `TestEBURLMustNameItsParts` (dois casos novos) |
| D1 do S | A sonda de saúde sem conexão no pool | **não se aplica**, conferido: ring só memória, os escritores são os `sink.workers`, o `+ 1` basta (SPEC §10.5) | `TestPoolLeavesTheProbeAConnection`: três escritas presas num `LOCK TABLE` (contadas em `pg_stat_activity`), a sonda responde com `pool_size = 4`; o controle `pool_size = 3` a deixa sem conexão |
| E2 do S | `frameKeepCap` contra as classes 2/8/64 KiB | **não se aplica**: aqui a moldura cresce pelas classes e nunca solta o buffer (não há `frameKeepCap`), sem realocação por frame | — |
| E1 do S | `closing` com PROXY `optional` perdendo as linhas em buffer | **não se aplica**: a leitura do cabeçalho não confere `closing` (a pendência do shutdown contado como falha do PROXY segue, com a forma certa escrita nela) | — |
| A2-minor do S | Comentário de ordem reversa com fechamento paralelo; log do aborto com o ring Redis | **não se aplica**: o fechamento é em ordem reversa, como o comentário diz; não há Redis | — |

Rodada final, PostgreSQL 18 (`rlpg-test-pg18`), Apple M2, Go 1.27.1:

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `SYSLOG_TEST_PG_DSN=... SYSLOG_TEST_EMITTERS=1 go test -race -count=1 ./...` | **verde duas vezes seguidas** (62 s e 75 s), sem *flake* |
| Testes | **404** de primeiro nível e **287** subtestes passaram, **1** pulado (`TestGoldenVectors/arquivo-sem-pri`, D5), 0 falhas |
| Por pacote (primeiro nível / subtestes) | `app` 66/57, `config` 63/0, `convert` 15/70, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 9/0, `receiver` 110/50, `ring` 13/0, `sink` 85/65 |
| Emissores reais | `TestEmittersRsyslog` (13,7 s), `TestEmittersSyslogNG` (30,2 s), verdes nas duas rodadas |
| Fuzz | nenhum: nenhum alvo de fuzz cobre o código que mudou (`app`, `config/eburl.go`) |
| Mutação | cada correção revertida sozinha: o teste da linha falha (ver acima) |
| Não rodou | fuzz de 10 min; carga ponta a ponta; `docker compose` (o `docker/` não mudou) |

### Pendências conhecidas fechadas (2026-10-14)

As quatro pendências de SPEC §22.1, triadas contra o `go-syslog-server` (S),
que já as resolvera (DECISOES §9). Duas correções portadas do S, com os testes
dele, e duas decisões do R portadas como contestáveis, com o teste que as trava.
A mesma implementação no irmão (`diff -r` de `internal/receiver` e
`internal/convert`: só os caminhos de import). **Ordem:** SPEC, teste que falha
no código de antes, correção. **Mutação:** cada correção revertida sozinha (e o
código de antes, via `git show HEAD:`, também no irmão), com o teste da linha
falhando e a correção restaurada depois.

| Item | Falha antes / com o mutante | Teste |
|---|---|---|
| Shutdown contado como falha do PROXY (SPEC §4.6) | sem o bloco: `proxy_headers_total{absent} = 1` (`{invalid} = 1` com meio cabeçalho), `messages_dropped_total{proxy} = 1` e o `WARN` "closing a connection without a PROXY header" nos quatro subcasos; com a forma do R: "optional: two lines already read off the socket (20 bytes in the reader) were dropped"; sem `\|\| mode == required`: "required: proxy_headers_total{absent} = 1" | `TestProxyShutdownIsNotAProxyFailure`, `TestProxyOptionalBufferedLinesSurviveClose` |
| Leitor de 64 KiB por conexão (SPEC §5.4, §15.1) | "an idle TCP connection costs 67353 bytes of heap" (plain; 66902 com PROXY `optional`); depois ~5,8 KiB | `TestIdleTCPConnectionFixedCost` |
| Buffer de frame nunca solto | "buffer of 65536 kept before a wait" (octet e lf) | `TestFramerReleasesALargeBuffer` |
| Liberação a cada frame (sem `Buffered() == 0`) | "frame 1: buffer reallocated with the frame already buffered" | `TestFramerKeepsTheBufferBetweenBufferedFrames` |
| Teto por IP, endereço exato (pin, SPEC §4.4) | chave `PrefixFrom(a, 64).Masked()`: "accepted [2001:db8:: 192.0.2.7], want [2001:db8::1 2001:db8::2 192.0.2.7]" | `TestPerIPLimitCountsTheExactAddress` |
| Offset 3164 sem faixa (pin, SPEC §6.5.1) | offset fora de faixa zerado: os três casos dão "timestamp 2026-10-03T08:00:00.000Z, want …" | `TestRFC3164ISOOffsetIsNotRangeChecked` |

A mutação achou uma lacuna nos testes do S: sem `|| mode == required` nenhum
dos dois testes dele falhava (sem cabeçalho, o `Peek` já devolve o *timeout*, e
cai em `err != nil`; o `required` só pesa quando a conexão mandou linhas e não
cabeçalho). O ramo `required` de `TestProxyOptionalBufferedLinesSurviveClose`
confere agora que nada é contado; o S não foi tocado.

Rodada final, Apple M2, Go 1.27.1, serviços de `tasks/infra/env.sh`:

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `SYSLOG_TEST_PG_DSN` (PostgreSQL de `env.sh`, porta 25432) `SYSLOG_TEST_EMITTERS=1 go test -race -count=1 -timeout 1200s ./...` | **verde duas vezes seguidas** (88 s e 76 s), sem *flake* |
| Testes | **411** de primeiro nível e **295** subtestes passaram, **1** pulado (`TestGoldenVectors/arquivo-sem-pri`, D5), 0 falhas |
| Por pacote (primeiro nível / subtestes) | `app` 66/57, `config` 63/0, `convert` 16/70, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 9/0, `receiver` 116/58, `ring` 13/0, `sink` 85/65 |
| Emissores reais (Docker, `rls-test-rsyslog` e syslog-ng) | `TestEmittersRsyslog` (15,7 s e 14,4 s), `TestEmittersSyslogNG` (18,5 s e 18,6 s), verdes nas duas rodadas |
| Benchmarks | §4 (`TCPReceive`, `Framer12K`) |
| Fuzz | nenhum, por instrução da rodada; `FuzzFrame`, `FuzzFrameOracle` e `FuzzProxy` cobrem código que mudou (moldura e leitura do PROXY) e devem rodar antes do release (§3) |
| Não rodou | fuzz; carga ponta a ponta; `docker compose` (o `docker/` não mudou) |

### Porte das pendências do RP/RN (2026-10-14)

Receita: `tasks/port-to-R-S-SP-SN.md` (o que a triagem do RP/RN achou igual
aqui). **Ordem:** SPEC, teste que falha no código de antes, correção.
**Mutação:** cada correção revertida sozinha, com o teste da linha falhando e a
correção restaurada depois. Testes novos em `internal/*/known_issues_test.go`.

| Item | Falha antes / com o mutante | Teste |
|---|---|---|
| O aborto fecha o ring antes de contar (SPEC §13.4) | "Enqueue racing a Close = <nil>, want ErrClosed"; "Enqueue after the abort counted = <nil>, want ring.ErrClosed" | `TestEnqueueSeesACloseThatRacedTheReservation`, `TestEnqueueWithoutACloseIsUnaffectedByTheHook`, `TestAbortClosesTheRingBeforeCounting` |
| Piso de `ring.max_bytes` (§13.2, §15.1) | "Load([--ring.max_bytes=65791]): err = <nil>, want one mentioning "ring.max_bytes"" (piso 65 792 = 64 KiB + 256); mutante com o piso desligado: a mesma | `TestRingMaxBytesFitsTheLargestMessage`, `TestRingMinBytesIsWhatTheRingCharges` |
| `admin.bind` × `listen.tcp` (§13.2, §14) | "Load([--listen.tcp=127.0.0.1:20514 --admin.bind=127.0.0.1:20514]): err = <nil>, want one mentioning "admin.bind""; mutante com a conferência desligada: a mesma | `TestAdminBindCollidesWithListen` |
| `SIGHUP` não relata derivada (§16.3) | "ignored = [server.data_dir emergency.dir]"; "ignored = [sink.workers sink.postgres.pool_size]" (e `TestReloadSeesSecrets`, ajustado: listava `sink.postgres.pool_size`, o comportamento antigo); com `\|\|` no lugar de `&&`, o controle explícito→derivado: "ignored = [], want emergency.dir" | `TestReloadDoesNotReportDerivedKeys`, `TestReloadDoesNotReportTheDerivedPoolSize` |
| Remoção falha de um dump entregue é só `ERROR` (§12.2, regra) | sem teste antes; trava nova, mutante `Error`→`Warn`: "a failed removal is not an ERROR" | `TestRemovalFailureIsOnlyAnError` |

Registrados sem código (regra ou lacuna): SQLSTATE fora da tabela é `network` (SPEC §10.5); o dump publicado no instante do aborto e o aborto que não espera o disco (§12.3); `kind="fatal"` com o boot cancelado (§14.1); o *padding* dos contadores (§8 item 17).

Rodada final, Apple M2, Go 1.27.1, serviços de `tasks/infra/env.sh` (outros
agentes os usavam ao mesmo tempo; nenhuma falha por carga):

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `source tasks/infra/env.sh; go test -race -count=1 -timeout 1200s ./...` | **verde duas vezes seguidas** (110 s e 65 s), sem *flake* |
| Testes (`-v`) | **420** PASS de primeiro nível e **295** subtestes por rodada; pulado `TestGoldenVectors/arquivo-sem-pri` (D5); 0 falhas |
| Emissores reais (Docker, rsyslog e syslog-ng) | `TestEmittersRsyslog` e `TestEmittersSyslogNG` verdes nas duas rodadas |
| Fuzz | nenhum, por instrução da rodada |
| Não rodou | fuzz; carga; `docker compose` (o `docker/` não mudou) |

### Revisão independente de 2026-10-14 (S, SP, SN, R, RP, RN)

Entrada: `tasks/review-syslog.md` e `tasks/review-relp.md`; progresso em
`tasks/progress-review-fixes-syslog-relp.md`. **Ordem:** SPEC, teste que falha
no código de antes, correção. **Mutação:** cada correção revertida sozinha,
com o teste da linha falhando e a correção restaurada depois.

| Item | Falha antes / com o mutante | Teste |
|---|---|---|
| PROXY `required` com cabeçalho **válido** no shutdown (§4.6; A1) | "required: a valid header and a line already read off the socket (6 bytes in the reader) were dropped uncounted"; mutante (volta a `err != nil \|\| required`): a mesma | `TestProxyRequiredValidHeaderBufferedLinesSurviveClose` |
| `sameStreamSocket` por valor (§13.2; D1 do syslog, D4 do RELP) | 7 casos, p.ex. `sameStreamSocket("[::0]:9102", "127.0.0.1:9102") = false, want true`, `("127.0.0.1:0514", "127.0.0.1:514")`, `("127.0.0.1:http", "127.0.0.1:80")`; mutantes (curinga por grafia; porta comparada como texto): mortos | `TestSameStreamSocketReadsValuesNotSpellings` |

Registrados sem código: o "até 8 KiB" vale entre frames (A5, D4; medido no S e revertido; SPEC §5.4/§15.1); sobrecontagem do aborto pelo recuo de bytes (D3; §13.4); comentário do `Memory.Enqueue` (D1 do RELP). O resto da revisão (sonda sqlite, `ring=none`, TLS do NATS) não se aplica ao corte.

Rodadas: `tasks/run2.sh go-syslog-pg-server rev` (`gofmt -l .`, `go vet ./...` e `go test -race -count=1 -timeout 1500s ./...` com `tasks/infra/env.sh` e banco próprio `whs_sp`), logs em `tasks/logs/go-syslog-pg-server-rev-r{1,2}.log`: **verde duas vezes** (`exit=0` nas duas; `gofmt` e `vet` limpos), sem *flake*. Fuzz: nenhum, por instrução da rodada.
