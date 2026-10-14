# go-syslog-nats-server — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Escrito
em 2026-10-07, na criação a partir do `go-syslog-pg-server` (o molde) e do sink
NATS do `go-syslog-server`; os números herdados estão marcados como tais.
Comandos a partir da pasta do módulo.

**Estado:** o porte dos testes herdados do `go-syslog-pg-server` (que falavam
com a fila `loghub.inbox`) para o NATS está feito: nenhum teste usa o
PostgreSQL, o `pgx` saiu do `go.mod`, e a suíte inteira passa com `-race` contra
um `nats-server` com JetStream (`nats://127.0.0.1:14222`) e um sem JetStream
(`SYSLOG_TEST_NATS_NOJS_URL`). A revisão independente do código NATS foi feita
(2026-10-07): corrigiu quatro achados, cada um com um teste que falhava antes
(§9), e deixou os conhecidos de §8.

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. **Sem `SYSLOG_TEST_NATS_URL`**, a suíte de integração
da app, a do sink real e a do beacon real pulam (§5, §6): só sobram as recusas
de boot de antes do passo 7 e os testes de pacote. Como o NATS é o único
destino, a prova ponta a ponta exige o servidor; rode §6 antes de considerar uma
mudança pronta.

Cobertura: `go test -cover -coverpkg=./internal/... ./...`, com e sem o NATS.
Os números deste módulo ainda não foram registrados (§8).

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
  recalculado pela fórmula de SPEC §6.10.
- **O oráculo** (`internal/jlhcheck`) é o porte em Go do `validate_payload.py`
  da skill: os dois schemas (`santhosh-tekuri/jsonschema/v6`, só em teste) mais
  as regras semânticas (ordem e número de chaves, chaves duplicadas, `level` =
  severidade, `sid` recalculado, instante do `uuid` = `event`, `iso` truncado,
  `from` UUIDv7 e o conjunto completo de espaço em branco nos campos `^\S+$`,
  D6). Os exemplos de envelope incluem os de `beacon`
  (`examples/valid/beacon-*.json`), contra os quais o beacon é conferido.
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
```

São os seis alvos que existem neste módulo (`grep -rn '^func Fuzz'`). O `^…$`
importa: `-fuzz=FuzzFrame` casa também `FuzzFrameOracle` e o Go recusa dois
alvos.

| Alvo | O que prova | Situação |
|---|---|---|
| `FuzzConvert` | a conversão nunca falha nem entra em pânico; toda saída passa no oráculo; a mesma entrada dá os mesmos bytes (SPEC §3.5) | herdado sem mudança de conversor; rodada do pai: 5 min, 2,27 M execs, sem falha |
| `FuzzSD` | STRUCTURED-DATA: tetos respeitados, nada perdido em `sd-invalid` | herdado; rodada do pai: 5 min, 12,3 M execs |
| `FuzzFrame` | as molduras não perdem nem inventam bytes | herdado; rodada do pai: 5 min, 1,48 M execs |
| `FuzzFrameOracle` | a moldura contra um oráculo independente | herdado; rodada do pai: 5 min, 3,67 M execs |
| `FuzzProxy` | PROXY v1/v2: nunca aloca pelo declarado | herdado (só TCP); rodada do pai: 5 min, 2,42 M execs |
| `FuzzRead` | o leitor JSON Lines tolera truncamento e lixo sem pânico, contabiliza todo byte, nunca guarda uma linha acima do teto, e todo registro devolvido passa de novo, igual, por `ParseLine` | do `go-syslog-pg-server`; sem rodada longa (§8) |

**Saíram:** o `FuzzHead` (com o codec do cabeçalho, SPEC §3.4) e, do molde, o
`FuzzJSONBSafe` e o `FuzzInboxPayload` (eram do `jsonb` da fila).

`FuzzRead` é o alvo que mais importa aqui: a reingestão **não converte**, e
`emergency.ParseLine` é a única porta entre um arquivo em disco e o stream (SPEC
§12.2). O NATS não lê o `Data`: uma linha que passasse por ela sem ser um JLH
deste servidor seria publicada, confirmada e entregue a todo consumidor.

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

**Medido no `go-syslog-pg-server`** (2026-10-07, Apple M2, máquina
compartilhada; o caminho de recepção é o mesmo código aqui; leia como ordem de
grandeza):

| Benchmark | Lá | Pai na mesma máquina |
|---|---|---|
| `IngestAccept` (mensagem aceita) | ~110–134 ns/op, 224 B/op, **1** alloc/op | ~134 ns/op, 280 B/op, 2 allocs/op |
| recusa com o ring cheio (pré-checagem, SPEC §2.2) | ~25 ns, 0 alloc | ~115 ns, 2 allocs (sem pré-checagem) |

A alocação a menos é a caixa `*[]byte` do buffer do pool, que volta com o
buffer (SPEC §15.1); a que sobra é o próprio `Item` (224 B). Fora o bloco abaixo,
nada foi remedido neste módulo (§8). Não há benchmark do sink NATS: o custo de
publicação depende do servidor e da rede, e só uma carga contra um servidor real
o mede.

**Leitor de 4 KiB e `frameKeepCap`** (2026-10-14, SPEC §5.4; medido no
`go-syslog-pg-server`, cujo `internal/receiver` é idêntico a este; Apple M2,
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
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 SYSLOG_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 900s ./internal/app/
go test -race -short ./internal/app/      # pula o teste do binário real
```

A app inteira sobe em processo (identidade fixa, `DATADIR` temporário, portas
efêmeras) contra o `nats-server` de §6; cada teste trabalha num `space` próprio,
com um stream próprio criado a partir de `docker/nats/stream.json` e apagado no
fim. Falhas do destino são injetadas decorando o sink real (`App.wrapSink`).
Todo envelope lido de volta do stream ou de um dump passa pelo oráculo.
Cobre:

- **Transportes:** UDP com `dst-ip` por PKTINFO, TCP LF e octet-counting, unix
  (sem `host`, `sid = 0`, socket removido no shutdown e nunca roubado de um
  processo vivo), vários listeners por transporte, PROXY v1 e v2 de par
  confiável (`x-proxy`, `sid` recalculado).
- **Fidelidade:** o `Data` da mensagem igual ao JLH byte a byte (com NUL no
  meio), o subject `loghub.<space>.syslog`, `Nats-Msg-Id = <uuid>.0` e os
  `X-LogHub-*`; `payload_raw=false` mantém o `raw` obrigatório, truncamento
  mantém a moldura sincronizada, mensagens vazias contadas e não publicadas,
  malformada recusada com `accept_malformed=false`.
- **Ciclo de vida:** recusas de boot com exit 1 (as chaves do pai na linha de
  comando, `--sink.type`, `--ring.mode`, `--listen.tls`, `--sink.nats.subjects`,
  `--sink.nats.tls`, uma URL `tls://`, no teste do binário), NATS inalcançável
  mantém os transportes fechados e sai com 3 no `boot_timeout`, validação do
  stream recusada sai com 3 na hora, NATS caindo e voltando sem perda, shutdown
  gracioso entrega tudo, segundo sinal dá 130 e conta `abort`, `SIGHUP`
  aplica/ignora/recusa conforme SPEC §16.3, arquivo inválido no reload não
  aplica nada.
- **Emergência:** `ClassFatal` no `Write` e `drain_timeout` estourado despejam e
  saem com 3; o dump é JSON Lines com o JLH exato, e a vida seguinte (outro
  `from`) o reingere no stream com os mesmos `uuid` e o `from` da primeira; as
  malformadas ficam de fora com `accept_malformed=false`; a reingestão acontece
  mesmo com `emergency.enabled=false`; `emergency.enabled=false` no incidente
  conta `emergency_failed`; um dump danificado entrega as linhas íntegras e vai
  para `.corrupt`; a entrega do boot segue as classes pedaço a pedaço; sink
  FATAL no dreno sai com 3 na hora.
- **Saturação:** ring cheio por transporte (UDP conta `ring_full`, TCP segura e
  não perde), sink DOWN por transporte, o teto de itens conta o lote retido
  (`TestRingItemsNeverExceedTheCapacity`), `/readyz` com o motivo.
- **Admin:** `/healthz`, `/readyz` com transições, as três métricas `_info`
  (`ring_mode="memory"`, `sink_type="nats"`), toda série de SPEC §14.1 presente,
  labels só de conjunto fechado, `le` estável entre leituras.
- **Concorrência:** conexões TCP e emissores UDP simultâneos: aceitas =
  mensagens no stream, sem duplicata.
- **O binário real** (`TestBinaryIdentityExitCodes`,
  `TestBinaryVersionAndConfigErrors`, `TestBinarySignalsOutsideTheRunWindow`,
  `TestBinarySIGHUPAppliesTheLogLevel`): códigos 100–114 do `go-loghub-ident`,
  `DATADIR` relativo resolvido antes do `Initialize()`, `--version`, `SIGHUP`
  no boot logado e ignorado, `SIGQUIT` dá 130, `SIGHUP` no ar troca o
  `log.level`.

Sem servidor, os testes de pacote ainda provam a configuração (registry, chaves
do pai recusadas na linha de comando e avisadas no env, `LOGHUB_EB_URL` só
`nats://`, `tls://` recusada nomeando o TLS, toda entrada de `sink.nats.urls`
`nats://`), o ring, os workers e a saúde com sinks falsos, a recepção, o
leitor do dump e o beacon com um publicador falso.

---

## 6. NATS

No host de desenvolvimento já existe o contêiner `lh-nats` (JetStream ligado) em
`nats://127.0.0.1:14222`. Sem ele, um descartável, e um segundo servidor **sem**
JetStream para o FATAL imediato de SPEC §10.7:

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &   # sem o lh-nats
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &                         # SEM JetStream
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 SYSLOG_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
SYSLOG_TEST_NATS_PERM_URL=nats://127.0.0.1:14224 SYSLOG_TEST_NATS_TLS_URL=nats://127.0.0.1:14225 \
  go test -race -timeout 900s ./internal/sink/ ./internal/control/ ./internal/app/
```

Os dois últimos (desde 2026-10-13, pulam sem eles) são um servidor com
JetStream e permissões — os usuários de `natsPermSetup` em
`internal/sink/review_port_test.go`: `admin`/`admin`; `nopub` negando publish em
`loghub.*.syslog`; `noinbox` negando sub em `_INBOX.>`; `nobeacon` negando pub
em `*.beacon`, todos com a senha igual ao nome — e um com `tls {}` (certificado
RSA autoassinado; o LibreSSL do macOS gera um EC que o `nats-server` recusa).
Os mesmos arquivos de configuração estão em `../go-relplog-nats-server/docs/TESTING.md`
§6. O servidor falso `natsFake` (mesmo arquivo) responde `INFO`/`PONG`, manda
`-ERR` por publish ou por subject, `PubAck` com erro de API no n-ésimo, 503
"sem respondedores" e `PING` sem resposta.

Os dois servidores são sem TLS, como tudo aqui. Cada teste trabalha num `space`
próprio, com um stream próprio apagado no fim: na app, criado a partir de
`docker/nats/stream.json` com os subjects trocados para o `space` do teste; no
sink, a partir do stream canônico em código (`natsCanonicalStream`), que
`TestNATSCanonicalStreamMatchesTheContract` confere, junto com o arquivo,
contra o contrato de SPEC §10.7.

A suíte do sink (`internal/sink/nats_integration_test.go`) prova o JLH guardado
só em `Data` e os headers de SPEC §3.8, um envelope com bytes hostis (NUL,
controles, UTF-8 inválido em `raw`), a redelivery deduplicada pelo
`Nats-Msg-Id` e contada em `syslog_sink_nats_duplicates_total`, escritores
concorrentes, a maior mensagem que o boot admite cabendo de fato no servidor, a
validação do stream (cobre o subject, não captura o plano de controle, não
selado, `no_ack` recusado), o contrato conferido de novo em toda checagem
(`TestNATSStreamEditedAtRuntimeIsFatal`: subjects estreitados, `no_ack` ligado,
`max_msg_size` reduzido, `max_bytes` com `discard: new` menor que um registro),
o stream esperado de outro dono fatal na escrita
(`10060`), o stream apagado fatal, o modo `core`, os ganchos de conexão e o
`PublishCore` do beacon, os workers ponta a ponta, o JetStream desligado fatal
na hora (`SYSLOG_TEST_NATS_NOJS_URL`) e a entrega parcial numa recusa `10054`
(`Delivered` = os confirmados). A suíte do beacon
(`internal/control/beacon_integration_test.go`) prova o beacon contra um
servidor real: `type=beacon`, `level=9`, `sid` = PID, schema do envelope, e o
subject `loghub._ctl.beacon`.

Sem servidor: `TestClassifyNATS` confere a tabela de SPEC §10.7,
`TestNATSHeaderBlockIsBounded` o teto de 256 B do bloco de headers,
`TestSubjectCovers` as regras de token do NATS, `TestNATSValidateRefusals`
cada recusa do contrato com o seu controle (inclusive os tamanhos e o
`max_bytes` com `discard: new`), `TestNewNATSIsLazy` e
`TestNewNATSRejectsBadConfig` a montagem, e
`TestNATSCanonicalStreamMatchesTheContract` lê `docker/nats/stream.json`: nome
`LOGHUB_SYSLOG`, `storage: file`, `duplicate_window` de 10 min, cobrindo
`loghub.default.syslog` e nunca `loghub._ctl.beacon` (nem `loghub.>`), `no_ack`
falso, não selado. Contra um servidor NATS falso, em processo (`fakeNATS`, que
responde `INFO`, `PONG` e, se pedido, um `-ERR` a cada publicação, e derruba as
conexões para forçar a reconexão): `TestNATSServerRequiringTLSIsNeverDialledWithTLS`,
`TestNATSConnectLogsNoCredentials` (nem token nem senha nos logs de conexão e
reconexão), `TestNATSCoreDeniedPublishIsFatalAtOnce` (o `-ERR` de permissão
lido no próprio `Write` em `core`) e `TestNATSCoreMaxPayloadIsCheckedOnEveryCheck`
(uma reconexão a um `max_payload` menor). Em `internal/config`,
`TestEBURLRefusesTLS` prova que `tls://` é exit 1 nomeando o TLS.

---

## 7. Emissores reais (SPEC §18.7)

```bash
SYSLOG_TEST_EMITTERS=1 SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 \
  go test -race -run Emitters -timeout 600s ./internal/app/
```

Precisa de Docker (baixa pacotes). rsyslog por UDP, TCP LF (`ForwardFormat`) e
TCP octet-counting (`Protocol23`); syslog-ng `network()` TCP e UDP e `syslog()`
octet-counting. Mensagens de cada, com `appname`, facility e severidade
conferidos no stream. O cenário TLS `gtls` do pai saiu com o TLS.

---

## 8. Lacunas conhecidas

Registradas por honestidade, não por esquecimento.

1. **Revisão independente do código NATS** (2026-10-07; o sink
   `internal/sink/nats.go`, o beacon `internal/control/`, a `LOGHUB_EB_URL` e a
   validação do stream): os quatro achados corrigidos estão em §9. Conferidos e
   corretos: nenhuma discagem TLS ou websocket é possível (os `connect_urls` que
   o servidor anuncia são reprefixados `nats://`), a posse de buffer, o
   `Nats-Msg-Id`, a validação do stream, a ligação do beacon e o
   `MaxPending` contra a reingestão. **Conhecidos e não corrigidos:**
   - com várias URLs, o veredito do boot pode depender da ordem aleatória em
     que o cliente tenta os servidores; a recusa de TLS, o caso que mais
     pesava, é `network` com mais de uma URL desde 2026-10-14 (SPEC §10.7,
     `TestNATSBootTLSRefusalOfAPoolIsNetwork`), e as outras recusas fatais
     (credenciais) valem para o *pool* inteiro;
   - o ramo `ErrMaxBytesExceeded` da classificação é inalcançável (só existe em
     consumidores *pull*);
   - uma corrida teórica entre `Health` e `Close` pode deixar uma conexão sem
     fechar, no fim do processo;
   - o *flush* interrompido por uma desconexão (`ErrConnectionClosed` com o
     cliente `RECONNECTING`) e o `LastError()` mascarado em `core`, listados
     aqui até 2026-10-13, foram corrigidos no porte do RN (§9).
   O código herdado do `go-syslog-pg-server` passou pela revisão de lá.
2. **Fuzz de 10 min por alvo não rodou** neste módulo. Os números de §3 são do
   pai (5 min por alvo); o `FuzzRead` do leitor JSON Lines não teve rodada longa
   nenhuma.
3. **Carga nunca medida.** Não há gerador de carga; os critérios de SPEC §18.8
   são alvos. Os benchmarks de §4 não foram remedidos.
4. **Servidor NATS que exige TLS**: fechado em 2026-10-13 por
   `TestNATSRealServerRequiringTLSIsRefusedWithoutAHandshake` (servidor real com
   `tls {}`, atrás de um proxy que grava os bytes: nenhum registro TLS sai).
5. **Cluster NATS** (várias URLs, troca de servidor no meio da carga) não foi
   testado com servidores reais. O *pool* de dois servidores falsos (um que
   serve, outro que exige TLS; o que serve cai e volta) é o
   `TestNATSOneTLSServerOfAPoolIsNoVerdict` (2026-10-14); um cluster real com
   rotas e servidores descobertos, não.
6. **Dump de outro Workspace** (conhecido, não corrigido, por desenho): se
   nenhum stream cobre o subject dele, o boot fica preso até o
   `boot_timeout` (ou sai na hora com `10060`, se outro stream o cobre) e sai
   com 3 a **cada** restart, com o arquivo intacto: o operador decide se cria o
   stream ou move o arquivo (SPEC §12.2). Sem teste ponta a ponta.
7. **O `address` do beacon** (o IP local da rota até o servidor) não é conferido
   no nível da app.
8. **Cobertura** deste módulo não registrada (§1).
9. **`kill -9` de verdade** não é testável em processo; a perda do backlog em
    memória é a documentada em SPEC §19.
10. **Dump grande.** O dump de um ring cheio (até 512 MiB de cru, JLH até 12×)
    não foi medido em tempo nem em disco; os testes usam rings pequenos. A
    leitura em fluxo limita a memória da reingestão, mas o tempo de republicar
    gigabytes antes de abrir os transportes não foi medido.
11. **O caminho `IP_PKTINFO` do Linux** só rodou na suíte da recepção do pai
    (Docker); aqui, no macOS.
12. **`/dev/log` de verdade** não foi testado; os testes usam um socket em
    diretório temporário.
13. **A regra do `/data`** (nada escrito fora de `server.data_dir`, exceto o
    socket unix) não tem teste automatizado de contêiner com raiz somente
    leitura.
14. **Conhecidos e não corrigidos**, herdados do `go-syslog-pg-server` (achados
    da revisão independente de lá, 2026-10-07):
    - `proxy.*` e `tcp.*` são aceitos com `listen.tcp` vazio, sem aviso.

    Fechados no porte das pendências do RP/RN (2026-10-14, §9): o `SIGHUP` não
    relata mais `emergency.dir` derivado (SPEC §16.3); `listen.tcp` e
    `admin.bind` na mesma porta são exit 1 na validação (§13.2);
    `ring.max_bytes` abaixo do custo de uma mensagem de
    `syslog.max_message_bytes` é exit 1 (§13.2, §15.1); a falha ao remover um
    dump já entregue continua só `ERROR`, agora como regra (§12.2,
    `TestRemovalFailureIsOnlyAnError`). Os contadores sem *padding* viraram o
    item 20.
15. **O `docker/docker-compose.yml`** foi validado com
    `docker compose config -q`; subir o conjunto (servidor, NATS e o `nats-init`
    que cria o stream) não foi exercitado nesta escrita. Em 2026-10-11 o
    procedimento da `LOGHUB_EB_URL` do `docker/README.md` (apagar as quatro
    chaves donas e descomentar a URL) foi conferido com `docker compose config`
    numa cópia: só a URL chega ao contêiner. As chaves donas são escritas sem
    valor (`KEY:`), e o compose só as repassa quando o shell ou o `.env` as
    define; por isso o defeito do N (`KEY: ${KEY:-}` vazio contando como segunda
    fonte e travando a URL) não existe aqui. O compose não repassa
    `SYSLOG_SINK_NATS_CREDENTIALS` (nem monta um `.creds`): autenticação por
    `.creds` no Docker não é suportada por este compose.
16. **Uma brecha deixada no parser de flags** (SPEC §13.2, como no N),
    estreitada em 2026-10-13: a cauda de um valor que o shell partiu num espaço,
    quando começa com traço, vem depois de um valor não vazio **e** tem a forma
    `<seção>.<chave>` de uma seção deste servidor ou do pai
    (`--sink.nats.password=a --sink.nats.b`), sai como parâmetro desconhecido,
    com o nome. Escondê-la esconderia os erros de digitação dentro de uma seção
    conhecida.
17. **Defeitos conhecidos do código, ainda não corrigidos**, ficam em SPEC
    §21.1, cada um com a consequência e a direção da correção. Quem corrige um
    o tira de lá, com um teste que falha sem a correção. Em 2026-10-14 os quatro
    de lá foram fechados (§9, "Pendências conhecidas fechadas") e, no mesmo dia,
    o porte das pendências do RP/RN fechou mais sete, registrados lá como
    corrigidos ou decididos (§9, "Porte das pendências do RP/RN"): nenhum
    aberto.
18. **O custo por conexão é medido no heap, não no RSS.**
    `TestIdleTCPConnectionFixedCost` mede `HeapAlloc` depois de dois GC, com
    as duas pontas no mesmo processo; a pilha da goroutine de cada conexão
    (SPEC §5.4) e a memória do kernel por socket ficam fora. Nenhuma prova com
    milhares de conexões reais nem sob `GOMEMLIMIT`.
19. **O prazo vencido no meio da assinatura PROXY** (SPEC §4.6, aceito em
    2026-10-14) não tem teste próprio: o comportamento é o do parser
    (`TestProxyAbsentConsumesNothing` cobre o "não é cabeçalho"), não o de um
    `header_timeout` vencendo depois de `PRO`.
20. **Os contadores da recepção não têm *padding*** (herdado do
    `go-syslog-pg-server`; registrado como lacuna de medição no porte do RP/RN,
    2026-10-14): com vários listeners `SO_REUSEPORT` eles disputam a mesma
    linha de cache (*true sharing* da mesma palavra, não *false sharing*: são
    os mesmos contadores). Sem defeito de correção observável; nunca medido como
    gargalo. O benchmark de recepção de §4 com `tcp_listeners` > 1 é o lugar de
    medir antes de mudar.
21. **O "até 8 KiB quando ociosa" vale só entre frames** (revisão independente
    de 2026-10-14, A5 e D4; SPEC §5.4/§15.1): um frame de 17–64 KiB seguido de
    alguns bytes do próximo segura a classe de 64 KiB até o idle. Não
    corrigido (a soltura no ponto de espera custou 10× em fluxo contínuo de
    frames de 24 KiB) e sem teste que fixe o número; o teto é `max_conns` e o
    `GOMEMLIMIT`.
22. **`sameStreamSocket` com nome de serviço** (revisão de 2026-10-14) é
    provado com `http` (a tabela embutida do Go) e com um nome inexistente; um
    nome que só o `/etc/services` conhece (`syslog`) depende do arquivo da
    máquina ou da imagem, e na imagem sem ele é recusado na validação, como o
    bind o recusaria.
23. **O PROXY `required` com cabeçalho válido no shutdown** (A1 de 2026-10-14)
    é provado no `readProxy` com o leitor cheio e `closing` ligado à mão, não
    com um `Close` de verdade correndo contra uma conexão do balanceador.
24. **NATS: *pool* só de servidores que recusam e uma URL com vários IPs** (D2
    do syslog, D3 do RELP, 2026-10-14; SPEC §10.7): registrados como limite,
    sem teste; o primeiro precisaria de dois `nats-server` com TLS recusado, o
    segundo de um nome com vários IPs.

---

## 9. Histórico

Os bugs que os testes e as revisões acharam no código herdado estão em
`../go-syslog-server/docs/TESTING.md` §9 (o sink NATS inclusive: `no_ack` e
JetStream desligado não fatais, entrega parcial contada inteira como perdida) e
em `../go-syslog-pg-server/docs/TESTING.md` §9 (o endurecimento da porta do
dump); as lições que generalizam estão em `AGENTS.md` §9.

Feitos na criação deste módulo:

| Mudança | O que ela fecha |
|---|---|
| **revisão (alta)**: uma mensagem recusada pelo cliente antes de sair (`ErrMaxPayload`) é recusada sozinha, como `logic`, e as demais do pedaço continuam; `Delivered` lista o que foi confirmado/publicado | a recusa encerrava o pedaço: as seguintes nunca saíam e eram confirmadas, as anteriores (guardadas) contavam como perdidas, e o dump era removido |
| **revisão (média)**: `sink.nats.urls` é parseada na validação, sem ecoar a entrada (exit 1, "percent-encode %, / and # in a password"), exige host, e recusa *userinfo* junto com `username`/`password`/`token`/`credentials` | um `%`, `/` ou `#` numa senha falhava dentro do `nats.Connect`, como `network`, até o `boot_timeout`, com a senha em cada linha de log; e o `nats.go` preferia em silêncio as credenciais da URL |
| **revisão (média, herdado do pai)**: veredito de permissão negada (`ErrPermissionViolation` numa publicação de dados ou na `$JS.API`) devolvido pelo `Health` como `fatal`; o do beacon não conta; reconexão recusada por TLS exigido vira o mesmo veredito | uma publicação negada estourava o prazo e era retentada para sempre com o sink UP (`jetstream`) ou sumia contada como escrita (`core`); um `STREAM.INFO` negado segurava o boot; o FATAL por TLS dizia "connection is RECONNECTING" |
| **revisão (baixa)**: `loghub.beacon_interval` acima de zero e abaixo de `1s` é exit 1 | `1ns` girava o laço do beacon |
| `TLSCertCB` que recusa, e `errNATSServerRequiresTLS` fatal | o `nats.go` se promovia a TLS sozinho diante de um `tls_required`, com as raízes do sistema, num servidor que "não tem TLS" |
| o sink recusa toda URL que não seja `nats://`, além da configuração | `tls://`, `ws://`, `wss://` ou um `host:porta` que chegassem ao cliente por outro caminho |
| dump sob `.emg-…jsonl.tmp`, `link` para o nome final | um dump cortado numa fronteira de linha parecia limpo e era removido depois de entregue em parte |
| reingestão em fluxo, pedaço a pedaço | um dump de gigabytes (12× o ring) derrubava o boot por OOM no passo 9, a cada restart |
| `ParseLine` só aceita inteiros simples de até 20 dígitos | números que o conversor nunca escreve, lidos de jeitos diferentes por cada consumidor |
| shutdown por FATAL tolera o primeiro `SIGTERM`/`SIGINT` (herdado da revisão do molde) | o orquestrador cortava o dump que salvava o ring; e o abort conta em `abort` o que já fora entregue ao dump |
| log de boot distingue "the sink refused this server permanently" de "did not become usable within sink.boot_timeout" | um FATAL imediato (stream ausente, TLS exigido, credenciais) parecia um prazo estourado |
| pré-checagem de ring cheio antes do `uuid` e da cópia; buffer cru devolvido na própria caixa do pool | uma enxurrada de UDP com o ring cheio alocava duas vezes por mensagem; uma aceita custa agora uma alocação (o `Item`) |

### Porte das correções do N (2026-10-11)

A revisão independente do `go-webhook-nats-server` (o N, criado em 2026-10-10 a
partir do sink NATS deste módulo; `../go-webhook-nats-server/docs/TESTING.md`
§9) achou defeitos que este módulo também tinha. Cada um foi conferido aqui por
leitura antes de mudar, e portado com SPEC, código e um teste que falha sem a
correção; cada correção foi revertida numa cópia (mutação) e derrubou o teste.
O único mutante que sobrevive é equivalente: tirar `ErrAuthRevoked` e
`ErrAccountAuthExpired` da classificação não muda nada, porque o texto deles já
casa com a recusa por texto do `CONNECT`.

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Token da URL no log "nats connected"/"nats reconnected": o `ConnectedUrlRedacted` do `nats.go` esconde só a senha | o token no `INFO` de todo boot e no `WARN` de toda reconexão | `TestNATSConnectLogsNoCredentials` |
| O erro de URL que não parseia citava o trecho que o `net/url` cita (`invalid port ":Sup3r" after host`) | a senha até a primeira `/`, `#`, `?` ou `,` no stderr, pelas duas portas (`sink.nats.urls` e `LOGHUB_EB_URL`) | `TestURLErrorsQuoteNothingOfTheURL` |
| `sink.nats.urls` aceitava caminho, consulta e fragmento (só a `LOGHUB_EB_URL` recusava; achado aqui, no porte) | `nats://tok/en@host` discava o host `tok`, sem credencial, e o erro de discagem citava o token até a barra em cada log até o `boot_timeout` | `TestURLErrorsQuoteNothingOfTheURL` |
| Em `core`, o `Write` não lia o veredito de permissão (o `-ERR` chega antes do `PONG` do *flush*); nenhum `Write` devolvia o veredito já guardado | pedaços negados confirmados, contados como escritos e perdidos até a próxima checagem | `TestNATSCoreDeniedPublishIsFatalAtOnce`, `TestNATSWriteReturnsTheVerdict` |
| O contrato do stream (e, em `core`, o `max_payload`) só era conferido na primeira checagem | stream editado por baixo: `network` para sempre, sink UP, ring enchendo; reconexão a um `max_payload` menor: recusas `logic` pedaço a pedaço | `TestNATSStreamEditedAtRuntimeIsFatal`, `TestNATSCoreMaxPayloadIsCheckedOnEveryCheck` |
| `max_bytes` com `discard: new` menor que um registro passava na validação | `10077` fatal no primeiro evento grande e em toda reingestão, em laço | `TestNATSValidateRefusals`, `TestNATSStreamEditedAtRuntimeIsFatal` |
| "User Authentication Expired/Revoked" no `CONNECT` era `network` (o `errors.Is` do `nats.go` compara o texto inteiro); `ErrAuthRevoked` e `ErrAccountAuthExpired` fora da tabela | o boot esperava o `boot_timeout` e saía com o motivo errado | `TestClassifyNATS` |
| Sem *hard link* (vfat, exFAT, FUSE de *object store*), o `link` do dump falhava sempre | o ring inteiro perdido onde a escrita no lugar funcionava | `TestWriteOnAFileSystemWithoutHardLinks` (o mesmo no `go-syslog-pg-server`) |
| Parsers de INI e de flags ecoavam a linha e o argumento | `password: x` ou `sink.nats.password=x` no stderr | `TestParserErrorsDoNotEchoValues` |
| Uma flag sem valor engolia a seguinte (`--sink.workers --sink.nats.password=x`, um `$WORKERS` vazio num script), e o valor digitado depois de um espaço (`--sink.nats.password= x`) virava argumento solto citado (achados pela revisão independente deste porte) | a senha no erro de tipo ou no "unexpected argument" | `TestParserErrorsDoNotEchoValues` |
| A sonda `AccountInfo` do JetStream desligado não distinguia nada (o `jetstream` traduz "sem respondedores" para "não habilitado" sozinho), e o pedido a `$JS.API.INFO`, negado a um papel mínimo, virava veredito de permissão permanente (achado na revisão do porte para o fast; o N tem o mesmo) | um boot com o JetStream de volta saía com 3 culpando permissão | `TestNATSValidateRefusals` (`streamErr`), `TestNATSJetStreamDisabledIsFatal` |
| Temporário de dump interrompido nunca era informado; o gêmeo de um dump ligado ficava (porte do `Leftovers` do N, achado pela revisão deste porte) | um dump interrompido invisível ao operador, cujas mensagens, depois de um `SIGKILL`, ninguém contou | `TestLeftovers`, `TestBootRemovesTheTwinOfADumpAndReportsAnInterruptedOne` (o mesmo no `go-syslog-pg-server`) |

Ficaram de fora, conferidos: a nova checagem dos subjects de outro `space` (o
N guarda os que o `cover` aprovou; este módulo não tem `cover`, e um dump de
outro Workspace continua sendo §8 item 6) e o defeito do `LOGHUB_EB_URL` no
compose do N (não existe aqui, §8 item 15). O N tornou o `fsync` do diretório
do dump melhor esforço; aqui ele **continua decidindo o dump** (SPEC §12.3),
também no caminho do `rename`, e o mesmo teste o prova.

A revisão independente deste porte (somente leitura, mesmo dia) confirmou os
nove itens e achou, além dos dois buracos da tabela acima: em `core`, um
`-ERR` de outro subject (o beacon negado) que chegue entre a negação de um
pedaço e o `PONG` do *flush* substitui o `LastError()`, e o pedaço negado é
confirmado — janela de uma ida e volta, num papel que nega os dois subjects (§8
item 1); e o `flush` interrompido por uma reconexão, já em §8 item 1. Os dois
existem igual no N e ficam para uma rodada própria.

### Porte da revisão do porte de volta ao N (2026-10-11)

A revisão independente do porte de volta ao `go-webhook-nats-server` achou no N
três caminhos do parser de flags que ainda imprimiam o segredo — o mesmo
`flags.go` que este módulo tinha. Portados com o N como referência, com teste e
mutação numa cópia do módulo:

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| `--sink.nats.password --x` nomeava "a flag `--x`" (o valor que começa com `--` na forma separada); `--sink.nats.password= -x` saía como parâmetro desconhecido; a cauda de um valor partido pelo shell (`--sink.nats.password=$P`) saía como argumento solto | O segredo no stderr do contêiner | `TestParserErrorsDoNotEchoValues` (casos novos, e controles: `-1` é valor, `-h` é flag, um erro de digitação depois de um valor continua nomeado, e cada recusa diz o que houve) |

A mutação deste porte achou um caminho do `flags.go` sem teste, também no N:
tirar o `assigned(v)` do `--config` não derrubava nada. Caso novo,
`--config= x` (o argumento depois do `=` vazio não é citado).

### Porte das correções do RN, do RP e do R (2026-10-13)

As correções das revisões independentes do `go-relplog-nats-server` (RN), do
`go-relplog-pg-server` (RP) e do `go-relplog-server` (R) que valiam aqui (CHANGELOG
da raiz, "A portar" das entradas "2026-10-13 — RP, RN" e "2026-10-12 — R"). O
código comum veio do `go-syslog-pg-server`, portado primeiro (TESTING §9 de lá,
mesma tabela para o que é comum: posse do aborto, `quitNow`, bind cancelado,
`ErrNotDurable`, prazo da reingestão sem progresso, gauges, `FullFor`, `Deliver`,
parsers, dump, `SIGHUP`), por fusão de três vias e com a redação do NATS; o sink
NATS veio do RN. **Conferência por mutação:** os testes novos de
`internal/config` rodaram contra o código de config de antes e cada um falhou;
o `nats.go` de antes, com um calço só para compilar, derrubou 18 testes de topo;
no resto, 20 mutantes do código comum (`A1`–`A13`, `E1`–`E2`, `R1`–`R2`, `K1`,
`N1`–`N2`) e 15 do sink NATS, um por correção, derrubaram os testes. `-race`
verde com nats-server 2.14.6 (JetStream, sem JetStream, permissões, `tls {}`).

| Achado | Consequência sem a correção | Teste |
|---|---|---|
| Servidor que passa a exigir TLS depois do boot: o `ReconnectErrHandler` nunca vê a recusa (é do *handshake*), só o `LastError` | FATAL só pelos fusíveis, com "connection is RECONNECTING" — ou nunca | `TestNATSReconnectToATLSOnlyServerIsAVerdict` |
| *Flush* de `core` que **começa** durante a reconexão confirmava o pedaço cujos `PUB` foram pelo socket velho | pedaço confirmado sem prova de que o servidor o recebeu | `TestNATSCoreFlushAcrossAReconnectionIsNotConfirmed` |
| *Flush* cortado por uma queda voltava `ErrConnectionClosed`, `fatal` | soluço do NATS = FATAL, dump e exit 3 | `TestNATSCoreFlushCutByAReconnectionIsNetwork` |
| Negação do beacon entre os `PUB` e o `PONG` mascarava a do caminho de dados | pedaço negado confirmado e perdido | `TestNATSCoreMaskedDenialIsNotConfirmed`, `TestMaskedDenial` |
| `jetstream`: o prazo do futuro de um publish negado era `network`; `core`: o `Health` não lia o `LastError` depois do *flush* | retentado para sempre com o sink UP | `TestNATSJetStreamDeniedPublishIsFatalAfterTheTimeout`, `TestNATSCoreHealthReadsTheDenialItsFlushFollows`, `TestNATSPermissionDeniedIsFatal` |
| `10077` de stream cheio (`discard: new`) ou fechando era `fatal` | dump, exit 3 e duplicatas na reingestão a cada pico ou restart do NATS | `TestClassifyNATS` |
| `10158`, `10167` e todo 429 eram `fatal` (SPEC §21.1 item 6 de antes) | FATAL na frota inteira num soluço do cluster | `TestClassifyNATS` |
| `retention: interest` aceito (item 5 de antes); `max_msgs_per_subject` sem aviso | eventos confirmados e não guardados | `TestNATSValidateRetentionAndPerSubjectCap`, `TestNATSInterestStreamIsRefused` |
| Assinatura negada em `_INBOX` não era veredito (item 7 de antes) | boot esperava o `boot_timeout` culpando a rede | `TestNATSDeniesTheDataPath`, `TestNATSVerdictIsReturnedByHealthAndWrite`, `TestNATSPermissionDeniedIsFatal/inbox_subscription_denied` |
| Dump de outro Workspace em `core` sem aviso (item 8 de antes) | perda silenciosa do dump | `TestNATSCoreWarnsOnceAboutAForeignSpace`, `TestNATSOwnSubjectDoesNotAllocate`, `TestNATSSubjects` |
| `MaxPending` = `workers × batch`, sem folga | retentativa rápida parava na janela de futuros (`ErrTooManyStalledMsgs`) | `TestMaxPendingHasTwiceTheWorkersBatches` |
| Sem tetos de config (`sink.workers` ≤ 256 aqui) | pânico no boot; `2 × workers × batch` dando a volta no `int` | `TestConfigCeilings` |
| `sink.nats.urls`/`LOGHUB_EB_URL`: `@` depois de `/?#`, host com escape, `,` na senha partindo a lista, só espaços, esquema citado sem `://` | senha encurtando a autoridade; mensagem que mandava procurar outra coisa | `TestSlashInTheUserinfoIsRefusedWithoutEcho`, `TestNATSURLsAreParsedWithoutEcho`, `TestCommaInAPasswordSplitsTheURLListWithAHint`, `TestEBURLOnlyBlanksIsRefused`, `TestNATSURLSchemes`, `TestURLErrorsQuoteNothingOfTheURL` |
| Parsers, dump e `SIGHUP` (os mesmos do `go-syslog-pg-server`) | — | `TestGluedValueIsNotPrinted`, `TestSwitchesTakeNoValue`, `TestEmptyConfigFlagIsRefused`, `TestINIByteOrderMarkIsIgnored`, `TestINIUnknownKeyNeverPrintsASecretContinuation`, `TestRemovedFlagAfterAValuedFlagIsNamed`, `TestRemovedParamsAreTheRemovedEnv`, `TestDumpCommentsDerivedValues`, `TestCharsetSynonymsAreOneValue`, `TestParserErrorsDoNotEchoValues` |
| Posse do aborto, `SIGQUIT`, bind, emergência, reingestão, ring, `Deliver` (os mesmos do `go-syslog-pg-server`) | — | os de §9 do `go-syslog-pg-server`, com os mesmos nomes (`port_review_*_test.go`, `port_lifecycle_test.go`, `publish_test.go`, `review_port_test.go`) |
| Lacunas (mutantes que sobreviviam no RN) | — | `TestNATSPublishErrorAfterTheFirstTaints`, `TestNATSLogicRefusalInAnAckTaints`, `TestNATSLibraryNeverRepublishes`, `TestNATSRefusedCredentialsAreFatal`, `TestNATSRealServerRequiringTLSIsRefusedWithoutAHandshake` |

### Correções transversais da revisão independente do `go-syslog-server` (2026-10-13)

Os itens transversais (X1–X4) do plano da revisão independente do porte do
`go-syslog-server` (S), e a conferência de C3, D1, E1, E2 e A2-minor; a mesma
implementação no irmão (regra de sincronia, `diff` conferido). **Conferência
por mutação**, cada correção revertida sozinha: o teste da linha falha
(`exited 0, want 130` nos dois subtestes; `abort 0, emergency_failed 5 for
5`; o subteste `10077_store_is_closed`). **Não provado** contra um servidor
real: o `10077` com "store is closed" vem do texto do `nats-server` 2.14.6
(`server/store.go:43`), não de um stream fechando durante um publish.

| Item | Achado | Consequência sem a correção | Teste |
|---|---|---|---|
| X1 (A1 do S) | Sinal depois da reingestão e antes do *accept* com IP literal ou socket unix: nenhum *bind* falha | o servidor abria, o `/readyz` dava 200 e o shutdown saía **0**, não 130 (SPEC §13.1, §13.5) | `TestSignalBeforeALiteralBindExits130` (IPs literais e socket unix) |
| X2 (A2 do S) | O aborto publica `aborted` antes de tomar o contador; o dump que via a marca nesse meio (abandonado, ou desligado) o tomava primeiro | `emergency_failed = n`, `abort = 0`: a perda contada uma vez, atribuída ao disco (SPEC §13.4) | `TestAbortInsideTheDumpKeepsItsReason` (dump abandonado e dump desligado; o gancho `inAbort` força a intercalação) |
| X3 (C2 do S) | `storeFailedIsTransient` conferia "store closed"; o nats-server 2.14.6 manda "store is closed" (`server/store.go:43`) | o `10077` de um store fechando era `fatal`: dump, exit 3 e duplicatas na reingestão a cada restart do NATS (SPEC §10.7) | `TestClassifyNATS/10077_store_is_closed` |
| C3 do S | Teto do que entra no `MaxPending` | **não se aplica**: sem `ring.max_inflight`; `2 × workers × batch` já limitado por `sink.workers` ≤ 256 e `sink.batch` ≤ 65 536 | `TestConfigCeilings`, `TestMaxPendingHasTwiceTheWorkersBatches` (já existentes) |
| E2 do S | `frameKeepCap` contra as classes 2/8/64 KiB | **não se aplica**: aqui a moldura cresce pelas classes e nunca solta o buffer (não há `frameKeepCap`), sem realocação por frame | — |
| E1 do S | `closing` com PROXY `optional` perdendo as linhas em buffer | **não se aplica**: a leitura do cabeçalho não confere `closing` (a pendência do shutdown contado como falha do PROXY segue, com a forma certa escrita nela) | — |
| A2-minor do S | Comentário de ordem reversa com fechamento paralelo; log do aborto com o ring Redis | **não se aplica**: o fechamento é em ordem reversa, como o comentário diz; não há Redis | — |

Rodada final, nats-server 2.14.6, Apple M2, Go 1.27.1:

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `SYSLOG_TEST_NATS_*` (os quatro: 14622 JetStream, 14623 sem JetStream, 14624 permissões, 14625 `tls {}`) `SYSLOG_TEST_EMITTERS=1 go test -race -count=1 ./...` | **verde duas vezes seguidas** (59 s e 61 s), sem *flake* |
| Testes | **431** de primeiro nível e **344** subtestes passaram, **1** pulado (`TestGoldenVectors/arquivo-sem-pri`, D5), 0 falhas |
| Por pacote (primeiro nível / subtestes) | `app` 69/74, `config` 64/0, `control` 8/0, `convert` 15/70, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 10/0, `receiver` 110/50, `ring` 13/0, `sink` 99/105 |
| Emissores reais | `TestEmittersRsyslog` (15,8 s), `TestEmittersSyslogNG` (21,0 s), verdes nas duas rodadas |
| Fuzz | nenhum: nenhum alvo de fuzz cobre o código que mudou (`app`, `sink/nats.go`) |
| Mutação | cada correção revertida sozinha: o teste da linha falha (ver acima) |
| Não rodou | fuzz de 10 min; carga ponta a ponta; `docker compose` (o `docker/` não mudou) |

### Pendências conhecidas fechadas (2026-10-14)

As quatro pendências de SPEC §21.1, triadas contra o `go-syslog-server` (S),
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
| `SYSLOG_TEST_NATS_*` (os quatro de `env.sh`: 24222 JetStream, 24223 sem JetStream, 24224 permissões, 24225 `tls {}`) `SYSLOG_TEST_EMITTERS=1 go test -race -count=1 -timeout 1200s ./...` | **verde duas vezes seguidas** (120 s e 87 s), sem *flake* |
| Testes | **437** de primeiro nível e **350** subtestes passaram, **2** pulados (`TestGoldenVectors/arquivo-sem-pri`, D5; `TestNATSOversizedMessageIsRefusedAlone`, porque o servidor de `env.sh` tem `max_payload` de 8 MiB e o teste precisa de um abaixo de 4 MiB — ambiente, não código), 0 falhas |
| Por pacote (primeiro nível / subtestes) | `app` 69/74, `config` 64/0, `control` 8/0, `convert` 16/70, `emergency` 28/45, `ident` 2/0, `item` 5/0, `jlh` 4/0, `jlhcheck` 4/0, `obs` 10/0, `receiver` 116/58, `ring` 13/0, `sink` 98/103 |
| Emissores reais (Docker, `rls-test-rsyslog` e syslog-ng) | `TestEmittersRsyslog` (20,8 s e 14,0 s), `TestEmittersSyslogNG` (24,2 s e 18,6 s), verdes nas duas rodadas |
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
| `SIGHUP` não relata derivada (§16.3) | "ignored = [server.data_dir emergency.dir]"; com `\|\|` no lugar de `&&`, o controle explícito→derivado: "ignored = [], want emergency.dir" | `TestReloadDoesNotReportDerivedKeys` |
| Discagem falha da reconexão NATS não logada (§10.7; antes, refatoração pura: `onReconnectErr`, `natsFake` com `stop`/`restart`) | "a failed reconnection dial was logged as an ERROR"; mutante `asyncError` fora do `if`: a mesma | `TestNATSReconnectDialFailureIsNotLogged` |
| Veredito TLS só com servidor único (§10.7, `soleServer`) | "a TLS refusal reported with a pool of 2 set a verdict"; "boot Health of a pool with one TLS-only server = connect: the NATS server requires TLS …, want network"; mutantes: sem `soleServer` no handler (verdict), no `noteLastError` ("Health while the good server is down = … refused this server permanently"), sem o ramo de *pool* no `connect` (Boot), `Health` sem nomear ("never named the TLS refusal") | `TestNATSOneTLSServerOfAPoolIsNoVerdict`, `TestNATSBootTLSRefusalOfAPoolIsNetwork` |
| Remoção falha de um dump entregue é só `ERROR` (§12.2, regra) | sem teste antes; trava nova (a mesma do SP) | `TestRemovalFailureIsOnlyAnError` |

Registrados sem código (regra ou lacuna): o dump publicado no instante do aborto e o aborto que não espera o disco (SPEC §12.3); `kind="fatal"` com o boot cancelado (§14.1); o *padding* dos contadores (§8 item 20).

Rodada final, Apple M2, Go 1.27.1, serviços de `tasks/infra/env.sh` (outros
agentes os usavam ao mesmo tempo; nenhuma falha por carga):

| Item | Resultado |
|---|---|
| `gofmt -l .`, `go vet ./...` | limpos |
| `source tasks/infra/env.sh; go test -race -count=1 -timeout 1200s ./...` | **verde duas vezes seguidas** (64 s e 57 s), sem *flake* |
| Testes (`-v`) | **448** PASS de primeiro nível e **350** subtestes por rodada; pulados: `TestNATSOversizedMessageIsRefusedAlone` (o NATS de `env.sh` tem `max_payload` de 8 MiB; rodado à parte com `SYSLOG_TEST_NATS_URL=nats://127.0.0.1:24226`: **PASS**, `jetstream` e `core`) e `TestGoldenVectors/arquivo-sem-pri` (D5); 0 falhas |
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

Registrados sem código: o "até 8 KiB" vale entre frames (A5, D4; medido no S e revertido; SPEC §5.4/§15.1); *pool* NATS só de servidores que exigem TLS e URL única com vários IPs (D2; D3 do RELP; §10.7); sobrecontagem do aborto pelo recuo de bytes (D3; §13.4); comentário do `Memory.Enqueue` (D1 do RELP). Sonda sqlite e `ring=none` não existem no corte.

Rodadas: `tasks/run2.sh go-syslog-nats-server rev` (`gofmt -l .`, `go vet ./...` e `go test -race -count=1 -timeout 1500s ./...` com `tasks/infra/env.sh`), logs em `tasks/logs/go-syslog-nats-server-rev-r{1,2}.log`: **verde duas vezes** (`exit=0` nas duas; `gofmt` e `vet` limpos), sem *flake*; e o pacote `sink` com `SYSLOG_TEST_NATS_URL=nats://127.0.0.1:24226` (`max_payload` 1 MiB): **ok**. Fuzz: nenhum, por instrução da rodada.
