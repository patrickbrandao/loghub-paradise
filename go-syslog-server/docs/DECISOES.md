# go-syslog-server — Decisões de projeto

Registro da entrevista de 2026-10-05 que definiu o novo `go-syslog-server`: um
servidor syslog **escrito do zero** no paradigma LogHub, que publica cada
mensagem como envelope JLH `type=syslog`. Este arquivo é a **entrada** da
`docs/SPEC.md`. Quando a SPEC existir, ela passa a ser a fonte da verdade, e cada
decisão abaixo aparece nela com o número da seção.

## Estado do monorepo

| Pasta | Estado |
|---|---|
| `go-syslog-server/` | **Novo, do zero**, v1. Mesmo module path de antes (`github.com/patrickbrandao/loghub-paradise/go-syslog-server`), agora livre. |
| `go-syslog-standard-server/` | O antigo `go-syslog-server`, renomeado e intacto (só o module path mudou para `.../go-syslog-standard-server`). Nunca entrou em produção. É **referência**: dele se porta código e lição, nunca se importa. |
| `go-relplog-server/` | Só especificação: o RELP extraído para virar software próprio no futuro. |
| `go-webhook-fast-server/` | Origem das decisões LogHub já tomadas (`docs/DECISOES.md` dele), sem código LogHub ainda. |

O passado do software não vale como restrição: nada em produção depende do
envelope v1, do stream `syslog:in` nem da tabela `events`.

## Fontes normativas

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH: nove campos, regras, `jlh-envelope.schema.json` |
| skill `loghub-type-syslog-schema` | consultada em 2026-10-05 | `type=syslog`: payload, schema, algoritmo de conversão, 24 vetores dourados |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 (a mais recente) | UUIDv7 nível 3, `GenerateAt` |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 (a mais recente) | `from` (AgentUUID), `space` (Workspace), `DATADIR` |
| `go-webhook-fast-server/docs/DECISOES.md` | 2026-10-05 | Decisões LogHub reaproveitadas (identidade, layout nos sinks, NATS, beacon, ring Redis) |

Onde a skill do syslog e a do envelope divergem, vale a mais restritiva (a
mesma regra do fast). Onde esta pasta diverge de uma skill, a divergência é uma
decisão abaixo e fica escrita na SPEC com o motivo.

---

## 1. Decisões do usuário

| # | Tema | Decisão |
|---|---|---|
| 1 | Pastas | O `go-syslog-server` atual vira `go-syslog-standard-server`; a pasta `go-syslog-server` recomeça vazia e o software é escrito **do zero**, atendendo todos os recursos dentro do paradigma LogHub. Continua sendo a **v1**. |
| 2 | RELP | **Sai** do syslog. Vira software separado, com especificação própria, no futuro; as especificações foram extraídas para `go-relplog-server/`. |
| 3 | Moldura TCP | Decidida **uma vez por conexão, com confirmação**: octet-counting só se o início casar `[1-9][0-9]{0,9} SP`; senão, non-transparent (LF). `tcp.framing = auto \| octet \| lf`. **Desvio documentado** da skill, que manda decidir por frame: uma linha sem PRI começando com dígito (log4j `2026-09-13 10:00:00,123 ...`) derrubaria a conexão, e reavaliar a cada frame embaralha um fluxo LF. |
| 4 | `message` e `raw` | **Como a skill:** `message` sempre; `raw` por padrão (`SYSLOG_PAYLOAD_RAW=true`, desligável), em base64 com `raw-encoding` quando não é UTF-8. O JLH é o **único** dado nos sinks: sem campo `b`, sem coluna `raw` à parte, sem cru depois do JLH no NATS. |
| 5 | Transportes | UDP (RFC 5426), TCP (RFC 6587), **TLS (RFC 5425)** e unix datagram. Fora: importação de arquivo (`transport=file`), DTLS e BEEP. |
| 6 | Recursos | **Todos** os da standard, reescritos sobre o JLH: rings, os seis sinks (`redis`, `sqlite`, `postgres`, `mysql`, `nats`, `none`), filtro por facility/severity recarregável, rate-limit por prefixo, dump de emergência, admin (`/healthz`, `/readyz`, `/metrics`), `SIGHUP` e registry único de config. |
| 7 | Arquitetura | **Cru no ring, parse no dreno** (o desenho da standard). O ring guarda cabeçalho + linha crua; o worker parseia e serializa o JLH. Consequência: tudo o que muda os bytes do JLH (fuso da RFC 3164, PRI padrão, `raw`, charset legado, aceite de `malformed`) **exige restart**, para que retry e reingestão produzam os mesmos bytes com o mesmo `uuid`. |
| 8 | Beacon | Heartbeat `type=beacon` em `loghub._ctl.beacon`, só com `sink=nats`, a cada `LOGHUB_BEACON_INTERVAL` (15 s; `0` desliga). Sem sonar e sem ping/pong (decisão 21 do fast). |
| 9 | NATS `core` | `sink.nats.mode = jetstream` (padrão) \| `core` (best effort, opt-in), como a decisão 8 do fast. |
| 10 | `LOGHUB_EB_URL` | Aceito: o esquema da URL escolhe `sink.type`; conflito com `sink.*` explícito dá exit 1. |
| 11 | Ring Redis | `ring.mode = redis`: **stream compartilhado** entre instâncias, consumer group, consumidor = AgentUUID, teto global por Lua, `XAUTOCLAIM` para pendentes de réplica morta (decisão 4 do fast). Aqui ele guarda **cabeçalho + cru**, não JLH (decisão 7). |
| 12 | TAG sem timestamp | Mensagem com PRI válido e sem timestamp (`<14>myapp[123]: algo`): extrai `appname`/`procid` com **TAG estrita** (começa com letra, `_` ou `/`, seguida de `:`), com o aviso `timestamp-missing`. **Desvio documentado** da skill (§7.5 dela); o schema aceita, e o vetor `rfc3164-cisco-sem-timestamp` continua passando. |
| 13 | Tamanho | **Truncar como a skill**: `SYSLOG_MAX_MESSAGE_BYTES` = 64 KiB, aviso `truncated`, `size` original; o excedente do frame é **consumido sem alocar** (octet-counting pula N bytes; LF descarta até o `\n`) e a conexão não fecha por tamanho. Pior JLH ≈ 12 × 64 KiB + fixo ≈ 770 KiB: cabe no `max_payload` padrão do NATS. |
| 14 | Validação por schema | **Só testes e CI** (`santhosh-tekuri/jsonschema/v6` só em teste), como a decisão 17 do fast. Desvio da regra "validar antes de publicar" escrito na SPEC com o número de um benchmark. |
| 15 | `dst-ip` no UDP | **PKTINFO pela stdlib**: `IP_PKTINFO`/`IPV6_RECVPKTINFO` no Linux, `IP_RECVDSTADDR`/`IPV6_RECVPKTINFO` no BSD/macOS, via `syscall` e `ReadMsgUDPAddrPort`. Sem `golang.org/x/net`. |
| 16 | Sink padrão | **`nats` em `jetstream`**: o LogHub é o eventbus. |
| 17 | mTLS | `tls.client_auth = optional` (padrão) \| `require` \| `none`. Em `optional` o servidor pede o certificado; com `tls.client_ca` verifica quando ele vem; sem CA só registra `peer-subject` e `peer-fingerprint` (para pinning a jusante). |
| 18 | Opcionais | Entram: **charset legado** (`SYSLOG_LEGACY_CHARSET = cp1252 \| latin1`, tabela própria, sem `golang.org/x/text`); **PROXY protocol v1/v2** no TCP e no TLS (`off \| optional \| required`, só de pares confiáveis, parser próprio com fuzz); **subjects por facility/severity** (opt-in, só NATS: `loghub.{space}.syslog.<facility>.<severity>`; o padrão continua o subject exato `loghub.{space}.syslog`). Fica fora: `space` por listener/rede. |

---

## 2. Padrões assumidos (contestáveis)

Pontos em que as skills, o fast ou a standard já indicam a resposta. Entram na
SPEC assim, a menos que o usuário diga o contrário.

### Módulo e identidade

- Binário `cmd/go-syslog-server`; `/app/go-syslog-server` na imagem; tags
  `go-syslog-server/vX.Y.Z`; letra **S** no `CHANGELOG.md` da raiz (as entradas
  S antigas passam a se referir à standard, com uma nota).
- `lhident.Initialize()` roda **uma vez**, no `main`, depois de `--help`,
  `--version` e `--dump-config`. Um struct `Identity` é injetado na app (os
  testes chamam a montagem dezenas de vezes e uma segunda chamada sai com 112);
  pacotes cujos testes leem identidade ganham `TestMain` com as cinco variáveis.
- `from` = AgentUUID; `space` = Workspace, validado no boot pela gramática da
  interseção (`^[a-z0-9-]+(\.[a-z0-9-]+)*$`, até 64), recusando segmentos vazios
  (exit 1). `type = "syslog"`, `step = 0`, `level = severity.code`, `sid` pela
  fórmula FNV-1a da skill (`0` no `unix`).
- `node.id` sai. Nomes de cliente dos drivers usam AgentName e Hostname; o dump
  passa a `emg-<agent_uuid>-<unixnano>.bin`. Os códigos 100–114 do ident entram
  na tabela de códigos de saída.
- **Raiz de escrita única:** `data_dir` (env `DATADIR`), padrão `./data`
  convertido em absoluto e exportado como `DATADIR` antes do `Initialize()`.
  Identidade, `ring.db`, `events.db` e `emergency/` ficam sob ela; o contêiner
  usa `/data`. Única exceção: o socket `listen.unix`.

### Tempo e `uuid`

- **Uma** amostra de relógio por mensagem, no fim da leitura do frame:
  `uuid = gen.GenerateAt(Level3, t)` e `event{uts, sub, iso}` da mesma `t`, com
  `iso` truncado em ms. Fecha a dívida da standard, que lia o relógio duas vezes.
- O `uuid` nasce **na recepção** e viaja no cabeçalho do item (decisão 7): o
  `200` implícito do TCP e o descarte contado do UDP já têm identidade.
- Gerador `uuidv7.NewGenerator()` (ChaCha8, a preocupação com PCG era da lib
  antiga); o `init()` que confere `crypto/rand` continua. Nível 3 fixo:
  `LOGHUB_UUID_LEVEL` **não** é exposto (desvio documentado: o JLH exige nível 3).
- No macOS o nanossegundo do `uuid` sai `000` (relógio de µs); nenhum teste
  espera ns reais.

### Cabeçalho do item (o que viaja ao lado do cru)

`uuid`, a amostra de relógio (s + ns), `transport`, `framing`, `src`/`dst`
(IP canônico e porta), PROXY usado ou não (e o par TCP real), sessão TLS
(`version`, `cipher`, `peer-subject`, `peer-fingerprint`), o PRI efetivo e a
classe do PRI (`ok`/`missing`/`invalid`), `size` original, `truncated`, **`from`
e `space`** de quem recebeu (o ring Redis deixa outra réplica drenar). Tudo o
mais do JLH é derivado do cabeçalho + cru, de forma determinística, com a config
de parse congelada.

### Parser

- O **algoritmo de conversão da skill** inteiro, com só os desvios das decisões
  3 e 12. Em relação à standard, isso muda: PRI inválido vira `malformed` (o
  texto inteiro em `message`); um PRI padrão único (`SYSLOG_DEFAULT_PRIORITY`,
  13) substitui `default_facility`/`default_severity`; NILVALUE e ausentes são
  **omitidos**; limites de campo da RFC 5424 mantêm o valor com
  `header-invalid` (sem truncar); timestamps em UTC com 3–9 casas; fuso da RFC
  3164 configurável (`SYSLOG_RFC3164_TIMEZONE`, padrão `UTC`) e ano por Y+1/Y/Y−1
  com teto de 24 h; regra de HOSTNAME e gramática de TAG (1–48, `": "` ou fim)
  da skill quando há timestamp; parâmetro SD repetido vira array; SD-ID
  repetido é mesclado com `sd-duplicate-id`; SD inválido é omitido e o texto a
  partir dele vai para `message`; BOM sai de `message`; um U+FFFD por octeto
  inválido; CR/LF/NUL finais saem de `message`; `parse.prefer` sai.
- **Tetos de SD** (`parse.sd_max_elements` 32, `parse.sd_max_params` 64)
  continuam, mas exceder vira `sd-invalid` (SD omitido, texto em `message`), e
  nada se perde em silêncio.
- `malformed` entra por padrão; `SYSLOG_ACCEPT_MALFORMED=false` descarta com
  contador e log amostrado. Mensagem vazia não gera envelope: só contador.
- O filtro continua na recepção, sobre o PRI efetivo (13 em `pri-missing` e
  `pri-invalid`). A leitura do PRI na recepção segue as mesmas regras do parser.
- O corpus de mensagens reais da standard é portado, com os `.want` reescritos
  para o payload novo; o teste falha abaixo de vinte entradas.

### Envelope e serialização

- JLH com os nove campos na ordem canônica e o payload na ordem de chaves da
  skill (§11). Serializador próprio por *append*, sem escapar `<`, `>` e `&`.
  `raw` em base64 padrão com `=`.
- Extensão `x-proxy` (`{"peer-ip", "peer-port"}`) só quando o PROXY protocol foi
  usado: `host.src-*` e `host.dst-*` vêm do cabeçalho PROXY, e o par TCP real (o
  proxy) fica registrado, como o `peer-addr` sempre registrou.
- Nenhuma outra extensão `x-*` é emitida.

### Rings

- `memory` \| `sqlite` \| `redis` \| `none`. A capacidade conta os bytes do cru
  (decisão 7), não o JLH.
- `ring=sqlite` + `sink=sqlite`: arquivos distintos permitidos com `WARN` (dois
  `fsync`), mesmo arquivo é exit 1 (regra da standard).
- `ring=redis` + `sink=redis` **no mesmo servidor**: `WARN` e o ring efetivo
  vira `memory` (regra do fast). Servidores distintos: permitido.
- **Ring Redis:** stream `ring.redis.stream` (padrão `syslog:ring`), grupo
  `syslog-ring`, campos `h` (cabeçalho) e `b` (cru), contador de bytes
  `<stream>:bytes`, `XAUTOCLAIM` com idle mínimo de 30 s. Cheio ou fora do ar:
  UDP descarta, TCP para de ler. Ordem FIFO por instância, não global.
- `ring=none` + `sink=none` continua válido (benchmark de transporte).

### Sinks

- **Lote com teto de bytes** (`sink.batch_max_bytes`, proposta 16 MiB), além de
  `sink.batch`: com o JLH até cerca de 12× o cru, `workers × batch × pior JLH`
  passaria de 1 GiB.
- **Redis:** `XADD <sink.redis.stream> [MAXLEN ~ N] * e <jlh>`, com
  `sink.redis.stream` padrão `loghub.{space}.syslog` (o `{space}` é expandido).
- **NATS:** subject `loghub.{space}.syslog` (ou o granular opt-in), `Data` =
  JLH, headers `Nats-Msg-Id: <uuid>.<step>`, `Content-Type: application/json`,
  `X-LogHub-Type`, `X-LogHub-UUID`, `X-LogHub-Space`. Stream padrão
  `LOGHUB_SYSLOG`, que o boot valida e nunca cria, e que **nunca** usa
  `loghub.>` (capturaria `_ctl`). `WithRetryAttempts(0)` e
  `ReconnectBufSize(-1)` continuam.
- **SQL** (`sqlite`, `postgres`, `mysql`): tabela nova `syslog_events` com
  `uuid` (nativo no PG, `BINARY(16)` no MySQL, `TEXT` no SQLite), `step`,
  `space`, `level`, `sid`, `ev_uts`, `ev_sub`, `facility`, `severity`,
  `hostname`, `appname`, `procid`, `msgid`, `src_ip`, `transport`, `format`,
  `env` (o JLH) e `nbytes`, com chave **`(uuid, step)`**. Campo ausente no
  payload é **`NULL`** na coluna ("não veio" ≠ "veio vazio"). As colunas de texto
  são cópias saneadas (NUL → U+FFFD); `env` é íntegro. Postgres e MySQL nunca
  fazem DDL.
- O boot calcula o pior JLH e recusa com exit 3, como a standard, quando ele não
  cabe no `max_payload` do NATS ou no `max_allowed_packet` do MySQL.

### Configuração e operação

- Env `SYSLOG_<SEÇÃO>_<CHAVE>`, arquivo por `SYSLOG_CONFIG`. Os seis nomes da
  skill são literais (`SYSLOG_DEFAULT_PRIORITY`, `SYSLOG_RFC3164_TIMEZONE`,
  `SYSLOG_MAX_MESSAGE_BYTES`, `SYSLOG_PAYLOAD_RAW`, `SYSLOG_LEGACY_CHARSET`,
  `SYSLOG_ACCEPT_MALFORMED`), mais `LOGHUB_EB_URL`, `LOGHUB_BEACON_INTERVAL` e
  `LOGHUB_CTL_NAMESPACE`. As variáveis de identidade são lidas pelo `lhident`.
- Portas: UDP e TCP `:1514`, TLS `:6514` (desligado até haver certificado),
  unix desligado; admin `127.0.0.1:9102`. Métricas com prefixo `syslog_`.
- TLS: `listen.tls`, `[tls] cert, key, client_auth, client_ca, min_version`
  (`1.2`), `handshake_timeout`. Certificado lido só no boot na v1.
- PROXY: `proxy.protocol = off \| optional \| required`, `proxy.trusted`
  (padrão `127.0.0.1/32,::1/128`), `proxy.header_timeout`.
- `SIGHUP` recarrega `log.*`, `filter.*`, `ratelimit.*` e
  `sink.sqlite.keep_files`. A config de parse exige restart (decisão 7).
- Dependências de produção (7): `rueidis`, `modernc.org/sqlite`, `pgx/v5`,
  `go-sql-driver/mysql`, `nats.go`, `go-loghub-uuidv7`, `go-loghub-ident`. Só
  em teste: `jsonschema/v6` e, se ainda servir de oráculo, `google/uuid`.
- Docker: os 18 compose de ring × sink da standard, mais dois com `ring=redis`
  (nats e postgres); `AGENT_NAME=go-syslog-server`, `DATADIR=/data`,
  `hostname:` em cada serviço, um volume por instância.

### Testes

- Os 24 vetores dourados da skill no nível do conversor, **menos**
  `arquivo-sem-pri` (`transport=file` está fora); o de TLS com os metadados de
  sessão injetados. Contratos com os schemas JLH e syslog, mais uma porta em Go
  das regras do `validate_payload.py`.
- Fuzz: `FuzzParse` (toda saída é JLH válido pelo schema), `FuzzSD`,
  `FuzzFrame`, `FuzzProxy`, `FuzzHead` e `FuzzRead` do dump.
- Suítes de integração por variável (`SYSLOG_TEST_REDIS_ADDR`,
  `SYSLOG_TEST_PG_DSN`, `SYSLOG_TEST_MYSQL_DSN`, `SYSLOG_TEST_NATS_URL`) e
  emissores vivos (`SYSLOG_TEST_EMITTERS=1`), agora com um cenário TLS do
  rsyslog. `-race` sempre.

---

## 3. Pontos a fechar na SPEC (minha proposta)

| Ponto | Proposta |
|---|---|
| `x-proxy` | O nome e a forma da extensão acima. |
| Colunas `NULL` | `NULL` para ausente nas três tabelas SQL, em vez do `''` da standard. |
| Nomes do ring Redis | `syslog:ring`, `syslog-ring`, `<stream>:bytes`, idle de 30 s. |
| Stream NATS | `LOGHUB_SYSLOG`, com subjects `loghub.<space>.syslog` e, no modo granular, `loghub.<space>.syslog.>`. |
| `sink.batch_max_bytes` | 16 MiB. |
| Compose | Os 18 da standard mais `redis-nats` e `redis-postgres`. |

---

## 4. Riscos conhecidos

- **Tamanho.** O JLH ocupa de 2 a 3× o cru (`message` + `raw` + escapes), e 12×
  no pior caso; um cru não UTF-8 ainda vai em base64. O ring conta o cru, então
  a memória do dreno precisa do teto de bytes por lote.
- **Determinismo entre réplicas.** Com o ring Redis, uma réplica drena o que
  outra recebeu: se as configs de parse divergirem, o mesmo `uuid` sai com bytes
  diferentes. A SPEC exige config de parse idêntica entre réplicas; uma
  impressão digital dela no cabeçalho é melhoria futura.
- **Duplicatas.** At-least-once: `XADD *` e NATS core não deduplicam; o
  JetStream deduplica pela janela e o SQL pela chave `(uuid, step)`. Os
  consumidores precisam ser idempotentes por `(uuid, step)`.
- **PKTINFO por plataforma.** O caminho Linux (`IP_PKTINFO`) não roda nos testes
  do macOS (`IP_RECVDSTADDR`); precisa de CI Linux.
- **TLS.** Handshake custa CPU por conexão; o `handshake_timeout` protege contra
  slowloris. A troca de certificado exige restart na v1.
- **PROXY protocol.** Um `proxy.trusted` largo demais permite forjar `src-ip`.
- **Desenvolvimento no macOS.** Sem `DATADIR` absoluto o boot sai com 100; um
  hostname com `_` sai com 109.
- **Réplicas com o mesmo volume** compartilham o AgentUUID: `from` duplicado no
  eventbus, sem aviso.
- **Subjects granulares.** Quebram assinaturas exatas e terminadas em `*`; os
  consumidores assinam `loghub.<space>.syslog.>`. Por isso são opt-in.
- **Testes que passam vazios.** Ao portar testes da standard, um helper que
  devolve zero quando a chave falta faz o teste passar sem provar nada.

---

## 5. Ordem de entrega

1. `docs/SPEC.md` autocontida, mais `AGENTS.md`, `CLAUDE.md` e `TESTING.md`
   novos, herdando o histórico de bugs da standard.
2. Código em fatias:
   1. identidade, `uuid` e serializador JLH;
   2. conversor syslog → payload, com vetores, corpus e fuzz;
   3. recepção: UDP com PKTINFO, TCP com moldura, TLS, unix, PROXY;
   4. rings (`memory`, `sqlite`, `redis`, `none`) e dump de emergência;
   5. sinks e beacon;
   6. app, config, admin, métricas e Docker.
3. Testes com `-race` por pacote e suítes de integração.
4. Verificação independente: um sub-agente só de leitura por área.
5. Registro breve no `CHANGELOG.md` da raiz (letra S).
6. Commit único, só com o "sim" do usuário.
