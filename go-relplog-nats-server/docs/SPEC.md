# go-relplog-nats-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-13
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server`
Binário: `go-relplog-nats-server`
Entrada: `docs/DECISOES.md` (2026-10-13, sem entrevista: cada decisão foi
tomada pelo agente e está registrada lá, com o número da seção daqui), que
aponta para as decisões herdadas do `go-relplog-server`
(`../go-relplog-server/docs/DECISOES.md`) e do `go-syslog-nats-server`
(`../go-syslog-nats-server/docs/DECISOES.md`).

**Origem.** Esta especificação é um **corte** da SPEC do `go-relplog-server`
(R, 2026-10-11, com as revisões de 2026-10-12), feito no molde do
`go-syslog-nats-server` (SN): o que é RELP — moldura, sessão, janela, respostas,
teto em voo, fim de sessão — é o do R, sem mudança de semântica; o que é ring,
sink, beacon, dump e configuração é o do SN, que já fez o mesmo corte sobre o
`go-syslog-server`. O pedido: um servidor RELP com **ring só na memória**,
**sink só no NATS** (JetStream por padrão, ou core), emergência em arquivo
**JSON Lines**, **sem TLS em lugar nenhum**, sem filtro e sem rate-limit (§1.2).
O heartbeat beacon fica (§11).

**Dois pais.** Onde R e SN dizem a mesma coisa, o texto é o de um deles; onde
divergem, vale o que serve ao RELP (do R) e o que serve a um sink NATS único
(do SN), e a seção diz qual. Uma correção feita depois num dos pais **vale para
este**, e uma feita aqui vale para eles: é a lista "a portar" da entrada **RN**
do `CHANGELOG.md` da raiz.

**A numeração das seções é a do `go-relplog-server`, de propósito.** O código
cita a spec por número (`§5.7`, `§10.7`, `§12.2`), e os dois servidores RELP são
da mesma família: um número que mudasse de sentido entre eles enganaria quem lê
os dois. Uma seção removida **mantém o título**, seguido de uma nota de uma
linha; nada é renumerado. Onde a SN tem uma seção com outro número para o mesmo
assunto (a moldura é §5 nos dois, o ring §9, o sink §10), o assunto segue a
numeração do R.

Servidor de ingestão de syslog por **RELP** do ecossistema **LogHub**. Recebe as
mensagens que o `rsyslog` envia com o `omrelp`, **em claro**, confirma **cada
uma** só depois de ela estar no ring em memória, converte cada uma num envelope
**JLH** com `type=syslog` (com `x-transport: "relp"`) e o **publica** no NATS, no
subject `loghub.<space>.syslog`: por padrão num stream JetStream, com
confirmação (`PubAck`) por mensagem. É um **Bridge-IN**: cria a primeira
mensagem de cada fato (`step = 0`).

---

## 1. Escopo

### 1.1 Objetivos

- **Nunca confirmar** uma mensagem antes de ela estar no ring, e **nunca
  recusar** com `500` o que vai passar sozinho: o RELP é o único transporte em
  que as duas promessas podem ser cumpridas ao pé da letra (§2.3, §5).
- Cada mensagem aceita vira **exatamente um** envelope JLH válido pelos schemas
  das skills `loghub-json-schema` e `loghub-type-syslog-schema` (§3).
- Máximo de mensagens por segundo com mínimo de CPU e memória, inclusive com a
  janela de transações em voo de cada conexão (§5.5, §5.6).
- **Dependências mínimas:** três diretas em produção (§17.3), binário pequeno.
- **Sem perda e sem invenção** no payload: o que chegou está nos campos ou em
  `raw`; o que não chegou é omitido, nunca adivinhado (§6).
- Um destino só: o subject `loghub.<space>.syslog` de um servidor (ou cluster)
  NATS, gravado por padrão num stream JetStream que o operador cria (§3.8,
  §10.7) — o mesmo subject e o mesmo stream (`LOGHUB_SYSLOG`) do
  `go-syslog-server` e do `go-syslog-nats-server`.

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| **RELP sobre TLS** (`listen.tls`, `[tls]`, mTLS, `payload.tls`) | Decisão 2 (`docs/DECISOES.md`), no molde do SN: **sem TLS em lugar nenhum**. Quem precisa de RELP cifrado usa o `go-relplog-server`, ou um terminador TLS na frente (com PROXY protocol, §4.6). O payload nunca traz `transport=tls` nem o objeto `tls` |
| **TLS até o NATS** | A mesma decisão. O cliente é **sempre texto claro**: sem `nats.Secure`, sem CA própria, sem `tls_insecure` (as chaves `sink.nats.tls`, `tls_ca` e `tls_insecure` do R saíram). Toda entrada de `sink.nats.urls` precisa ser `nats://`: `tls://`, `ws://`, `wss://` e um `host:porta` sem esquema são exit 1 (§13.2). A `LOGHUB_EB_URL` só aceita `nats://`; `tls://` é exit 1 com uma mensagem que nomeia o TLS (§10.9). E como o `nats.go` **se promove sozinho** a TLS quando o `INFO` do servidor diz `tls_required`, o sink instala um `TLSCertCB` que recusa: nenhum `ClientHello` é enviado, e um servidor que exige TLS é FATAL na hora (exit 3 no boot, nomeando o TLS; §10.7). O NATS é alcançado por rede confiável |
| Outros sinks: `postgres`, `redis`, `sqlite`, `mysql`, `clickhouse`, `none` | Pedido: sink **só** no NATS. Cada um trazia um cliente e a sua árvore de transitivas. Quem precisa deles usa o `go-relplog-server` (todos) ou o `go-relplog-pg-server` (a fila `loghub.inbox`) |
| Outros rings: `sqlite`, `redis`, `none` | Pedido: ring **só** na memória. A perda sob `kill -9` é a do ring em memória (§19); FATAL e `SIGTERM` são cobertos pelo dump (§12). Com o `ring=none` saiu também o único `500` do fluxo normal do R (a recusa `logic` do destino na transação, §5.4) |
| Subjects por facility/severidade (`sink.nats.subjects = facility-severity`) | Decisão 3, a do SN: saem. O subject é sempre `loghub.<space>.syslog`, a regra `loghub.{space}.{type}` do JLH; a chave `sink.nats.subjects` não existe |
| Filtro por facility/severidade | Decisão 4, a do SN: sai. O descarte, se houver, é do consumidor; a recepção nem lê o PRI (§2.2) |
| Rate-limit por IP de origem | Decisão 4: sai. No R ele **segurava** a transação (nenhum `500`, §8 de lá); aqui a proteção contra emissor ruidoso fica na rede e na própria janela RELP (§5.5) |
| `sink.type` e `ring.mode` como chaves | Um valor só cada um. O log de boot e o `relplog_config_info` continuam dizendo `ring_mode="memory"` e `sink_type="nats"`, para que um painel do R sirva aqui (§14.1) |
| UDP, unix datagram, syslog TCP da RFC 6587, TLS da RFC 5425 | Não confirmam por mensagem; são do `go-syslog-server` e dos cortes dele |
| `relp` no enum de `transport` | A skill não tem; o RELP vai como `x-transport` (§3.2) |
| Deduplicação de retransmissão | O RELP não carrega id de mensagem (§19) |
| Resposta a sonar, `ping` e `pong` | Como nos pais: o servidor só **emite** o heartbeat (§11), não assina nada |
| DTLS, BEEP, importação de arquivo (`transport=file`) | Como nos pais (D5) |
| `space` por listener ou rede | Um Workspace por instância na v1 |
| Validação por JSON Schema em execução | Só em teste e CI (§18.2; D3) |
| Autenticação de emissor | Sem TLS, não existe no protocolo: firewall ou rede privada |
| Comandos RELP além de `open`, `syslog` e `close` | O `librelp` só usa esses (§5.4) |

O porquê comum: **o pedido** (um servidor RELP rápido e com dependências
mínimas, no molde do SN), **as dependências** (do R, com oito em produção, para
três diretas; o binário tem 12,6 MB contra os 31,9 MB do R, mesmo `go build`
simples, `darwin/arm64`, Go 1.27.1, §17.3) e **a velocidade** (a recepção não lê
a mensagem: nem PRI, nem filtro, nem balde de rate-limit; §2.2).

### 1.3 Fontes normativas e desvios

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, `jlh-envelope.schema.json`, subject `loghub.{space}.{type}`, `Nats-Msg-Id = <uuid>.<step>`, plano de controle (o heartbeat de §11) |
| skill `loghub-type-syslog-schema` | consultada em 2026-10-11 (sem `relp` em `transport`) | Payload `type=syslog`, schema, algoritmo de conversão, 24 vetores dourados |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 | AgentUUID, Workspace, AgentName, Hostname, MachineID, `DATADIR` |
| `librelp` (`rsyslog/librelp`, `src/`) e `rsyslog/plugins/omrelp` | lidos em 2026-10-11 (no R) | O comportamento do cliente: o que cada resposta significa (§5.4) |

Cópias dos dois schemas, dos exemplos de envelope (inclusive os de `beacon`) e
dos vetores ficam em `internal/jlhcheck/loghub/` (§18.2). Onde as skills
divergem, vale a mais restritiva.

**Desvios deliberados das skills**: os mesmos do R, com os mesmos motivos:

| # | Desvio | Skill diz | Aqui | Seção |
|---|---|---|---|---|
| D1 | Moldura | TCP detecta por frame | **não se aplica**: o RELP delimita por `DATALEN`, que é contagem de octetos; `framing` é sempre `octet-counting` | §5.1 |
| D2 | TAG sem timestamp | sem timestamp, nenhuma TAG | TAG **estrita** ainda extrai `appname`/`procid` | §6.5.5 |
| D3 | Validação | todo envelope passa no schema antes de ser publicado | schema só em teste e CI; o serializador é provado por fuzz | §18.2 |
| D4 | `LOGHUB_UUID_LEVEL` | variável sugerida | **não existe**: nível 3 fixo | §3.1 |
| D5 | Opções de importação | `reference`, `transport=file` | fora | §1.2 |
| D6 | Espaço em branco nos campos `^\S+$` | o conversor de referência emite o valor | o valor é omitido; o conjunto é o `\s` do ECMAScript mais U+001C–U+001F e U+0085 | §6.4.1 |
| D7 | Avisos de encoding de um SD seguido de texto colado | a referência funde os avisos | descartados, como em qualquer SD que falhou | §6.4.3 |
| D8 | Timestamp fora dos anos 0001–9999 | a referência emite fora do schema | `timestamp-invalid` e `timestamp` omitido | §6.4.2 |
| D9 | Extensão | — | `x-transport: "relp"` em todo envelope (permitido pelo `^x-` do schema) | §3.2 |

Não é desvio, mas fica registrado: o único vetor dourado com
`transport=tls` roda **como TCP** (§18.2), porque não há TLS aqui.

---

## 2. Arquitetura

### 2.1 Dois eixos ortogonais

No `go-relplog-server` são dois eixos, ring × sink, com vinte e oito
combinações. Aqui cada eixo tem **um valor só**, como no SN:

- **RING**: `memory` (§9.3).
- **SINK**: `nats`, `jetstream` por padrão ou `core` (§10.7).

As chaves `ring.mode` e `sink.type` não existem (§1.2). A separação continua no
código: **o ring não consulta o sink, e vice-versa** (invariante §20.15), e as
interfaces `ring.Ring` e `sink.Sink` ficam, porque são a costura pela qual
recepção, workers e saúde são testados.

```
   rsyslog (omrelp), qualquer cliente librelp
      |
      |  RELP em claro :20514
      v
 +-----------------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                          |
 |   [PROXY] -> sessão RELP: frame -> truncamento -> amostra de relógio  |
 |   -> janela (relp.window) + teto em voo (relp.inflight_max_bytes)     |
 |   -> por transação: vazia? -> sink DOWN? -> ring cheio? -> uuid       |
 |   -> Item(cabeçalho + cru) -> ring -> 200 OK (escritor da conexão)     |
 +----------------------------------+------------------------------------+
                                    |
                     +--------------v---------------+
                     | RING memory: canal + 2 tetos |
                     +--------------+---------------+
                                    | sink.workers (8)
                     +--------------v---------------+
                     | CONVERSÃO -> JLH type=syslog |  <- fora do caminho quente
                     +--------------+---------------+
                                    |
                     +--------------v---------------+
                     | SINK nats (uma conexão)      |
                     |  PublishMsgAsync -> PubAck   |
                     |  Nats-Msg-Id <uuid>.0        |
                     +--------------+---------------+
                                    |
          subject loghub.<space>.syslog -> stream LOGHUB_SYSLOG (JetStream)

   plano de controle: <ctl_namespace>.beacon (loghub._ctl.beacon), NATS core,
                      na mesma conexão, nunca num stream (§11)

   emergência: ring (ou lote retido) não vazio + sink FATAL ou dreno estourado
               => <data_dir>/emergency/emg-<agent_uuid>-<unixnano>.jsonl (§12)

 +-----------------------------------------------------------------------+
 | porta admin (admin.bind, padrão 127.0.0.1:9103)                        |
 |   /healthz   /readyz   /metrics                                        |
 +-----------------------------------------------------------------------+
```

O ring é **único e global**, compartilhado por todas as conexões: uma métrica
única de profundidade, backpressure justo (uma transação só é segurada quando o
sistema inteiro está saturado) e código simples. O teto de cópias em voo
(§5.6) é igualmente único.

### 2.2 Onde a conversão acontece, e por quê

A recepção faz o mínimo que permite decidir se a mensagem entra; a conversão
completa roda no **worker de dreno**.

| Etapa | Onde roda | Custo |
|---|---|---|
| PROXY, `open` | recepção, uma vez por conexão | do transporte |
| Moldura RELP, truncamento | recepção (leitor da conexão) | inevitável |
| Amostra de relógio | recepção (leitor), no fim do frame | uma leitura de relógio |
| Vaga na janela e no teto, cópia | recepção (leitor) | uma cópia do pool |
| Vazia, sink DOWN, ring cheio, `uuid` | recepção (goroutine da transação) | dois testes, um UUID |
| PRI, RFC 3164 / RFC 5424, SD, UTF-8 | **worker de dreno** | proporcional à linha |
| Serialização do JLH | **worker de dreno** | proporcional aos campos |

A ordem da transação é estrita, e a primeira condição que falha encerra a
tentativa: vazia (§5.4) → truncamento (§5.3) → a guarda de origem não
endereçável (§4.2) → sink DOWN (§13.3) → **ring cheio** → `uuid`, cabeçalho e
cópia para um buffer do pool → fim da sessão → `ring.Enqueue`. O "ring cheio" é
uma pré-checagem barata e sem trava, feita **antes** do `uuid`, da cópia e do
item: uma transação segurada pelo ring cheio retenta a cada 50 ms (§5.7), e
cada retentativa custa ~25 ns e nenhuma alocação em vez de ~115 ns e duas. Ela
confere **os dois tetos** (`ring.Memory.FullFor(n)`: itens ≥ `capacity_items`,
ou bytes + a classe do pool de `n` + `ItemOverhead` > `max_bytes`), com `n` o
tamanho **já truncado**, o que a cópia ocuparia: só os itens, a pré-checagem
herdada do SN, deixava uma transação segurada por `ring.max_bytes` cunhar
`uuid`, pegar buffer, copiar e montar o item a cada 50 ms, para o `Enqueue`
recusar (revisão independente do RP, 2026-10-13, portada). Um ring fechado
nunca é "cheio": o `Enqueue` diz `ErrClosed`, que é o shutdown. Ela pode errar
nos dois sentidos numa corrida; **o `Enqueue` continua sendo a autoridade** (os
dois tetos, §9.2).

No R o PRI era lido na recepção, porque o filtro dependia dele. Sem filtro
(§1.2), **a recepção não lê a mensagem**: nenhum byte do conteúdo é
interpretado antes do ring. A CPU da conversão é paga pelos `sink.workers`, que
é onde a vazão é elástica, e o `200 OK` não espera por ela.

Consequências que o resto da spec assume:

1. **O item no ring não tem envelope.** Ele carrega a linha crua e o que a
   conversão **não pode** recuperar dela: `uuid`, amostra de relógio,
   transporte, endereços, tamanho original, `from` e `space`. É o **cabeçalho do
   item** (§3.4), que só existe em memória.
2. **A contabilidade do ring conta a linha crua** (e a struct, §9.2). O JLH é
   construído e consumido dentro do lote do worker, com teto próprio
   (`sink.batch_max_bytes`, §10.1, §15.3).
3. **O dump de emergência guarda o JLH pronto**, não cabeçalho + cru (§12.1): a
   conversão do que estava pendente é paga na hora do dump, uma vez, por um
   processo que está saindo de qualquer forma.
4. **O JLH é função determinística** de cabeçalho + cru + config de conversão
   (§3.5). Por isso **toda chave que muda os bytes do JLH exige restart**
   (§16.3): um retry precisa produzir os mesmos bytes com o mesmo `uuid`, que é
   a regra do outbox do JLH.

### 2.3 Regra de aceitação

> A confirmação sai no instante em que o item entrou no ring — nunca antes. E
> uma recusa só é respondida quando é **final**: o que vai passar sozinho é
> segurado, e se não passar a tempo, devolvido ao emissor (§5.4, §5.7).

O `200 OK` significa: **o item entrou no canal do ring em memória** (§9.3). A
durabilidade dele está em §19.

| Situação | Resposta | O emissor |
|---|---|---|
| aceita | `200 OK` | apaga a mensagem da fila dele |
| vazia | `200 OK` (não há o que guardar; contada) | apaga |
| ring cheio, sink DOWN | nenhuma enquanto dura; estourado o prazo, a sessão fecha | segura; retransmite na sessão seguinte |
| origem não endereçável (guarda, §4.2) | `500 source address unusable` | **descarta e loga**, suspende a ação e retransmite a janela |
| erro de protocolo do cliente (§5.4) | `500 ...` | idem |

**No fluxo normal não há `500`.** No R, o `ring=none` fazia o `200 OK` esperar o
destino, e a recusa `logic` final dele era respondida com `500 refused by the
destination`. Com o ring em memória, o destino nunca fala com a transação: uma
recusa do NATS é do worker de dreno, muito depois do `200 OK`, e é contada lá
(`relplog_events_dropped_total{reason="sink_logic"}`, §10.2), como no SN. Os
únicos `500` que sobram são a guarda de origem e os erros de protocolo do
cliente, nenhum dos dois produzível por um `omrelp` sadio sobre TCP.

**Como no R, não há transporte que perde por desenho**: nada é descartado sem
que o emissor seja avisado (`500`) ou recupere a mensagem (retransmissão). A
perda que sobra é a do ring em memória sob `kill -9` ou OOM (§19).

### 2.4 Papel no LogHub

| Aspecto | Valor |
|---|---|
| Papel | **Bridge-IN**: traz dados de fora e publica como envelopes |
| Operação | **Produce** (primeira inserção): `uuid` novo, `step = 0`, `from` = AgentUUID |
| Subject | `loghub.{space}.syslog`, e só ele (§3.8). O mesmo `type` e o mesmo subject do `go-syslog-server`, do SN e do R: um consumidor de syslog não distingue a origem, a não ser por `payload.x-transport` |
| Plano de controle | heartbeat beacon em `<ctl_namespace>.beacon` (§11); nunca emite `ping`, `pong` ou sonar |
| Consumo | nenhum: este servidor não assina subject algum |

---

## 3. Modelo de dados

### 3.1 Envelope JLH

Os nove campos, nesta ordem, e nenhum outro:

```json
{
  "uuid": "01a101a2-aa7b-71c8-b15b-db8fdb816d6a",
  "from": "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
  "space": "default",
  "type": "syslog",
  "level": 6,
  "sid": 593636897809901,
  "step": 0,
  "event": { "uts": 1791028800, "sub": 123456789, "iso": "2026-10-03T12:00:00.123Z" },
  "payload": { ... §3.2 ... }
}
```

| Campo | Regra para este servidor |
|---|---|
| `uuid` | UUIDv7 **nível 3** (`go-loghub-uuidv7`), em minúsculas, gerado **na recepção** com `GenerateAt(Level3, t)`, onde `t` é a amostra de relógio da mensagem. É cunhado depois de conferidos o estado do sink e a pré-checagem do ring cheio (§2.2; uma transação segurada por isso não gasta `uuid` a cada retentativa), mas sempre da amostra `t` do fim do frame. Gravado no cabeçalho do item e nunca regerado: a retentativa do laço de backpressure da transação RELP (§5.7), o retry do worker, o dump e a reingestão reusam o mesmo; se uma tentativa de destino incerto (prazo estourado) tiver sido aceita, as duas cópias deduplicam pelo `Nats-Msg-Id` (`<uuid>.0`) dentro da `duplicate_window` do stream (§3.8, §10.2). O nível 3 é fixo; `LOGHUB_UUID_LEVEL` não existe (D4). |
| `from` | AgentUUID de **quem recebeu** a mensagem (`lhident.AgentUUID()`), copiado para o cabeçalho do item. Nunca um identificador do emissor remoto: a origem externa está em `payload.host`. |
| `space` | Workspace de quem recebeu (`lhident.Workspace()`), copiado para o cabeçalho. Validado no boot (§13.2). |
| `type` | `"syslog"`. |
| `level` | `payload.severity.code` (0–7), inclusive quando o PRI veio do padrão. |
| `sid` | FNV-1a 64 da quíntupla de rede, reduzido a 53 bits (§6.10), com `transport` = `tcp`: agrupa os envelopes de uma conexão, e qualquer consumidor o recalcula pelo payload. |
| `step` | `0`: Bridge-IN. |
| `event` | A **mesma** amostra `t` do `uuid`: `uts` = segundos, `sub` = nanossegundos, `iso` = `t` **truncado** em ms, `yyyy-MM-ddTHH:mm:ss.SSSZ`. Tempo real: `event` = instante do `uuid`, até o nanossegundo. |

**Uma amostra de relógio por mensagem**, tirada pelo leitor da conexão no fim
da leitura do frame RELP (o último octeto do trailer), antes de qualquer parse e
antes de a transação ganhar goroutine. O `uuid` e o `event` saem dela; ler o relógio duas vezes faria o
fato parecer posterior ao envelope (erro 3 da lista da skill).

No macOS o relógio tem resolução de microssegundo: o nanossegundo do `uuid` e de
`event.sub` termina em `000`. Nenhum teste espera nanossegundos reais.

A hora declarada pelo emissor vai em `payload.timestamp`, e **nunca** é usada
para ordenar, indexar ou correlacionar: na RFC 3164 ela não tem ano nem fuso, e na
RFC 5424 vem de um relógio que ninguém auditou. A diferença entre as duas mede a
deriva do emissor.

### 3.2 Payload `type=syslog`

Contrato: `jlh-syslog-payload.schema.json` da skill. Chaves em kebab-case,
nesta ordem (recomendada pela skill, §11 dela, e **fixa** neste serializador):

| Campo | Tipo | Presença |
|---|---|---|
| `host` | objeto | se algum campo existir |
| `host.hostname` | string | se declarado na mensagem (nunca DNS, nunca o IP) |
| `host.src-ip`, `host.src-port` | IP canônico, int | rede: sempre |
| `host.dst-ip`, `host.dst-port` | IP canônico, int | sempre (o `LocalAddr` do socket, ou o PROXY) |
| `transport` | `tcp` (RELP em claro, o único) | sempre |
| `framing` | `octet-counting` (o `DATALEN` do RELP é contagem de octetos) | sempre |
| `format` | `rfc5424` \| `rfc3164` \| `malformed` | sempre |
| `version` | int | só `rfc5424` |
| `priority` | int 0–191 | sempre |
| `facility`, `severity` | `{code, name}` (RFC 5427) | sempre |
| `timestamp` | `AAAA-MM-DDThh:mm:ss.fffZ`, 3–9 casas, UTC | se válido |
| `appname`, `procid` | string | se houver |
| `msgid`, `structured-data` | string, objeto | só `rfc5424` |
| `message` | string | **sempre** (pode ser `""`) |
| `size` | int | sempre: octetos originais da SYSLOG-MSG, antes do truncamento |
| `raw` | string | padrão; obrigatório em `malformed` e com aviso de encoding |
| `raw-encoding` | `base64` | quando `raw` não é UTF-8 |
| `parse` | `{warnings, error}` | só se houve desvio |
| `x-proxy` | `{peer-ip, peer-port}` | só quando o PROXY protocol foi usado (§4.6) |
| `x-transport` | `"relp"` | **sempre**, por último (D9) |

O schema da skill aceita `transport = "tls"` e o objeto `tls`; **este servidor
nunca os emite** (§1.2). `transport` também nunca é `udp`, `unix`, `dtls`, `file`
nem `beep`, e `format` nunca é `rfc3195`. As extensões emitidas são `x-proxy` e
`x-transport`.

**Por que `x-transport` e não `transport: "relp"`** (decisão 2 do R): o enum de
`transport` da skill não tem `relp`, e o schema tem `additionalProperties: false`
com `^x-[a-z0-9][a-z0-9.-]*$` para extensões locais. `transport` diz o que o
socket é (aqui sempre `tcp`, o que mantém o `sid` recalculável pela fórmula da
skill);
`x-transport` diz que os bytes vieram em frames RELP, confirmados um a um. O
valor vem do conversor (`convert.Config.XTransport`, fixado pela app como a
constante `app.XTransport`), não do cabeçalho: é constante na vida do processo e
mantém o JLH função pura de cabeçalho + cru + config (§3.5). Se a skill um dia
ganhar `relp` no enum, a mudança é desta seção.

**Omissão:** campo sem valor na origem (NILVALUE, inexistente no formato,
inválido) é **omitido**. Nunca `null`, nunca `""`, exceto `message`. Objetos
sem conteúdo (`host`, `parse`, `structured-data`) também são omitidos.

Exemplo (RELP de `10.0.0.3:59922` para `10.0.0.10:20514`):

```json
{
  "host": { "hostname": "srv101.intranet.br", "src-ip": "10.0.0.3", "src-port": 59922,
            "dst-ip": "10.0.0.10", "dst-port": 20514 },
  "transport": "tcp", "framing": "octet-counting", "format": "rfc5424", "version": 1,
  "priority": 86, "facility": { "code": 10, "name": "authpriv" },
  "severity": { "code": 6, "name": "info" }, "timestamp": "2024-03-15T14:22:05.123Z",
  "appname": "sshd", "procid": "12345", "msgid": "ID47",
  "message": "Accepted publickey for alice from 10.0.0.2 port 54321 ssh2",
  "size": 126,
  "raw": "<86>1 2024-03-15T14:22:05.123Z srv101.intranet.br sshd 12345 ID47 - Accepted publickey for alice from 10.0.0.2 port 54321 ssh2",
  "x-transport": "relp"
}
```

### 3.3 Serialização

Um serializador próprio por *append*, sem `encoding/json` no caminho quente, sem
reflexão e sem mapas:

- JSON compacto, UTF-8, sem espaços e sem `\n` final.
- Os nove campos na ordem de §3.1; o payload na ordem de §3.2; dentro de
  `structured-data`, os SD-IDs e os PARAM-NAMEs **na ordem de chegada** (a ordem
  não tem significado no JSON, mas a determinística torna o byte a byte
  reproduzível).
- Strings já são UTF-8 válido quando chegam ao serializador (a conversão
  decodifica antes, §6.7). O escape é o mínimo do JSON: `"` → `\"`, `\` → `\\`,
  `\n`, `\r`, `\t`, `\b`, `\f`, e todo outro byte abaixo de `0x20` como `\u00XX`
  (o NUL vira `\u0000`). **`<`, `>` e `&` não são escapados**, nem U+2028/U+2029:
  log com HTML ou XML continua legível.
- Inteiros como inteiros: `level`, `sid`, `step`, `uts`, `sub`, `version`,
  `priority`, `code`, `size`, portas. Nunca `6.0`.
- `raw` não UTF-8 vai em base64 padrão **com** `=` (`raw-encoding: "base64"`).

O mesmo serializador produz o beacon (§11).

Como todo controle é escapado, **um JLH nunca contém um `\n` cru**: é o que faz
de `\n` um separador seguro nas linhas do dump (§12.1).

### 3.4 Cabeçalho do item

O que a conversão **não consegue** recuperar da linha crua e que, portanto,
viaja ao lado dela pelo ring (a struct `item.Head`):

| Campo | Conteúdo |
|---|---|
| `UUID` | §3.1 |
| `From`, `Space` | identidade de quem recebeu |
| `UTS`, `Sub` | a amostra de relógio |
| `Transport`, `Framing` | §3.2: a recepção só produz `tcp` com `octet-counting`; o enum do item ainda tem `udp` e `unix`, porque o conversor e os vetores dourados os usam (§18.2) |
| `SrcIP`, `SrcPort`, `DstIP`, `DstPort` | endereços **canônicos** (§4.2), já resolvidos pelo PROXY quando houver |
| `PeerIP`, `PeerPort` | o par TCP real, **só** quando o PROXY protocol foi usado |
| `Size` | o `DATALEN` original; `Size > len(cru)` significa truncamento (§5.3) |

O PRI **não** está no cabeçalho: ele é derivável do cru, e a conversão o lê com
`convert.ScanPRI` (§6.2). Guardá-lo seria criar duas fontes da mesma verdade.

**O cabeçalho só existe em memória.** No R ele tinha um codec JSON
(`item/codec.go`, alvo do `FuzzHead`), com três consumidores: a coluna `head`
do `ring.db`, o campo `h` do stream do ring Redis e o dump `.bin`. Os três
saíram — o ring é só memória e o dump guarda o JLH pronto (§12.1) —, e o codec
foi apagado com eles: nenhum dado persiste um cabeçalho. A regra que o codec
conferia (o instante do `uuid` é o de `UTS`/`Sub`) passou para a leitura do dump
(`emergency.ParseLine`, §12.2).

### 3.5 Determinismo

> **Invariante:** o JLH é função pura de `(cabeçalho, cru, config de conversão)`.
> Dois processos com a mesma config que leiam o mesmo par produzem o mesmo JLH
> byte a byte.

É o que torna exato o dump de emergência (escrito com a config congelada, cada
linha é byte a byte o JLH que o worker teria escrito, §12.1) e o que faz um
retry reenviar os mesmos bytes com o mesmo `uuid`, como o outbox do JLH exige.

A **config de conversão** é o conjunto `[syslog]` (§16.5): `default_priority`,
`rfc3164_timezone`, `payload_raw`, `legacy_charset`, `accept_malformed`,
`sd_max_elements` e `sd_max_params`. Ela é **congelada no boot** (§16.3).
`max_message_bytes` atua na recepção, mas também exige restart.

**A reingestão não converte** (§12.2): a linha do dump já é o envelope. Por isso
um dump sobrevive a uma troca de chaves `[syslog]` ou de versão do binário entre
duas execuções sem mudar um byte: a regra que no R exigia a mesma config entre
quem grava e quem drena aqui não tem a quem se aplicar.

### 3.6 Entrada do stream Redis (`sink=redis`)

Removido no go-relplog-nats-server (§1.2): não há `sink=redis`.

### 3.7 Tabela de eventos (`sink=sqlite`, `sink=mysql`) e fila `loghub.inbox` (`sink=postgres`)

Removido no go-relplog-nats-server (§1.2): não há sink SQL; o único destino é o NATS (§3.8, §10.7).

#### 3.7.1 Fila `loghub.inbox` (`sink=postgres`)

Removido no go-relplog-nats-server (§1.2): não há `sink=postgres`; a fila `loghub.inbox` é o destino do `go-relplog-pg-server`.

#### 3.7.2 Tabela `inbox_syslog` (`sink=clickhouse`)

Removido no go-relplog-nats-server (§1.2): não há `sink=clickhouse`.

### 3.8 Mensagem NATS (`sink=nats`)

```
Subject:  loghub.<space>.syslog
Headers:  Nats-Msg-Id:     <uuid>.<step>          (aqui sempre <uuid>.0)
          Content-Type:    application/json
          X-LogHub-Type:   syslog
          X-LogHub-UUID:   <uuid>
          X-LogHub-Space:  <space>
Data:     <jlh>
```

- O subject exato é a regra do JLH: publicar no subject exato, nunca num
  curinga, e nunca ler `space` ou `type` de volta do subject. É **derivado, não
  configurável**: não há chave de subject, e os subjects granulares
  `loghub.<space>.syslog.<facility>.<severity>` do R saíram (§1.2, decisão 3).
- O `space` do subject e dos headers é o do **cabeçalho do item** (§3.4), que é
  o Workspace de quem recebeu; numa linha reingerida de um dump, o `space` da
  própria linha, conferido por `emergency.ParseLine` (§12.2). Nunca vem do
  emissor. O sink guarda o subject de cada `space` visto num mapa; as chaves
  possíveis são o Workspace desta instância e os dos dumps que o operador pôs no
  diretório.
- `Nats-Msg-Id` é `<uuid>.<step>`, **nunca** o `uuid` puro: um processador que
  encaminhe para o mesmo stream (mesmo `uuid`, `step + 1`) teria o salto legítimo
  descartado como duplicata pela janela do JetStream.
- Os headers `X-LogHub-*` ajudam o roteamento; o envelope continua a fonte da
  verdade.
- **O JLH vai em `Data`, sozinho, e nunca num header.** O bloco de headers tem
  tamanho **fixo e pequeno**: no máximo 256 bytes (`NATS/1.0` 10, `Nats-Msg-Id`
  54, `Content-Type` 32, `X-LogHub-Type` 23, `X-LogHub-UUID` 53,
  `X-LogHub-Space` com um `space` de 64 caracteres 82, e o CRLF final 2), muito
  abaixo do teto de 64 KiB de headers do JetStream (`10097`). A standard
  aprendeu isso contra um servidor real: o envelope num header estoura o teto
  com um hostname longo, e um emissor viraria o fim do processo.
- Em `jetstream`, o `WithExpectStream` acrescenta `Nats-Expected-Stream:
  <stream>` (24 bytes mais o nome do stream), que entra na conta de §3.9.

### 3.9 O maior evento, e por que ele é calculado e não configurado

O NATS tem teto por mensagem: o `max_payload` do servidor (1 MiB por padrão,
headers e dados somados) e, num stream, o `max_msg_size`, o teto de registro do
*file store* (32 MiB, o `rlBadThresh` do `nats-server`) e, com `discard: new`, o
`max_bytes` do stream inteiro. Uma mensagem aceita na
recepção e depois recusada por tamanho seria `logic` (§10.2), descartada e
contada. Por isso o boot compara o teto com a **maior mensagem possível** e não
abre os transportes quando ela não cabe (exit 3, com os números no log, §10.7);
e toda checagem de saúde depois dele compara de novo, porque o servidor e o
stream podem mudar por baixo (FATAL, §10.7).

Com `L = syslog.max_message_bytes` (o cru nunca passa disso, §5.3):

```
pior JLH         = 12 × L + 8 KiB
maior mensagem   = pior JLH + 256 B de headers (§3.8)
                   [+ 24 B + nome do stream, em jetstream]
registro no file store = maior mensagem + subject + 64 B
                         (também o mínimo de max_bytes, com discard: new)
```

- **Texto convertido ≤ 6 × L.** `message`, `hostname`, `appname`, `procid`,
  `msgid` e cada SD-ID, nome e valor são fatias **disjuntas** do cru (um SD
  inválido vai para `message` e sai de `structured-data`, nunca os dois). No
  pior caso todo byte é de controle e vira `\u00XX` (6 bytes). Um octeto
  inválido vira U+FFFD (3 bytes, sem escape), e o charset legado também produz no
  máximo 3 bytes por octeto.
- **`raw` ≤ 6 × L**: texto com escape, ou base64 (4/3 × L) quando não é UTF-8.
- **8 KiB fixos**: chaves, números, `host`, `x-proxy`, `parse` e o cabeçalho do
  envelope.

Com o padrão de 64 KiB, o pior JLH é **776 KiB**: cabe no `max_payload` padrão
do NATS (1 MiB) com os headers. `TestWorstCaseJLHIsBounded` prova o teto com
linhas adversárias. Quem sobe `max_message_bytes` acima de **84 KiB** (com o
stream padrão `LOGHUB_SYSLOG`) precisa subir o `max_payload` do NATS junto, e o
boot diz isso. Com `storage: file`, acima de cerca de **2,6 MiB** nenhum
`max_payload` resolve: o registro passaria dos 32 MiB do file store, cuja
recusa (`10077`) seria fatal, e o boot recusa antes.

O número serve ainda ao pedaço de §10.1 (um JLH maior que
`sink.batch_max_bytes` forma um pedaço sozinho), ao limite de linha do leitor do
dump (`emergency.MaxLine`, §12.2, calculado com o **maior** `max_message_bytes`
que a config aceita: 12 × 16 MiB + 8 KiB) e ao `max_event_bytes` do log de boot.

O número é pessimista de propósito: um fator "realista" deixaria um único
emissor quebrado produzir uma mensagem que o servidor NATS recusa, depois de
ela ter sido confirmada ao emissor.

---

## 4. Transportes

Um transporte, RELP **em claro**, ligado por `listen.tcp`, que, vazio, o
desliga. **Desligado é erro de configuração** (exit 1): um servidor que não
escuta nada parece saudável em toda sonda e não serve para nada.

| Transporte | Parâmetro | Padrão | O que o emissor configura | `payload.transport` / `framing` |
|---|---|---|---|---|
| RELP em claro | `listen.tcp` | `:20514` | `omrelp` sem `tls` | `tcp` / `octet-counting` |

O RELP sobre TLS do R (`listen.tls`) **não existe** aqui (§1.2, §4.5), e UDP,
unix datagram e as molduras da RFC 6587 também não: o RELP só existe sobre
fluxo, e quem precisa daqueles usa o `go-syslog-server` ou os cortes dele.

### 4.1 As portas padrão

A imagem roda como uid 65532 e não abre porta abaixo de 1024. `:20514` é o
costume dos exemplos de RELP do rsyslog e já é alta, e é a mesma do R: um
`omrelp` aponta para um ou para o outro sem mudar o `port=`. O log de boot
imprime o endereço efetivo de cada listener.

### 4.2 Endereços

O par de endereços do payload é `RemoteAddr` (origem) e `LocalAddr` (destino) do
socket, **canônicos**, como a skill exige: IPv4 em decimal pontuado; IPv6 na
forma da RFC 5952 (minúsculas, zeros comprimidos); IPv4 mapeado
(`::ffff:a.b.c.d`, comum em socket dual-stack) vira IPv4; a zona (`%eth0`) sai;
`0.0.0.0` e `::` nunca são gravados (o campo é omitido). Com um PROXY header
confiável, os dois vêm do cabeçalho (§4.6). Uma origem não endereçável não tem
envelope válido possível (o schema exige `host.src-ip`); num par TCP isso não
acontece, e a recusa existe só como guarda (`500 source address unusable`,
`reason="unaddressable"`).

### 4.3 Sem transporte local

Não há `listen.unix`: o RELP não existe sobre socket datagrama. Nenhuma escrita
deste servidor fica fora de `server.data_dir` (§16.6).

### 4.4 RELP em claro

Uma conexão é **uma sessão RELP** (§5). É o transporte padrão.

- `listen.tcp_listeners > 1` habilita `SO_REUSEPORT`.
- `tcp.max_conns` e `tcp.max_conns_per_ip` (`0` = ilimitado) valem **por
  transporte**, somando os listeners: um limitador por listener multiplicaria os
  tetos. O teto global **bloqueia no accept** (a conexão espera no backlog do
  kernel, e o `connect` do emissor decide); o por IP **fecha na hora**. O IP é o
  endereço **exato** do par TCP (um /128 no IPv6), **antes** do PROXY protocol:
  com um balanceador na frente, o teto por IP vê o balanceador (§4.6). Contar
  por prefixo IPv6 foi recusado (decisão 16 do R): os emissores RELP são servidores
  de endereço fixo, e um /64 pode ser uma rede inteira deles.
- `tcp.idle_timeout` (120 s) encerra uma sessão sem frame por esse tempo, com
  `serverclose` (§5.8); `0` desliga. O `omrelp` reconecta na próxima mensagem.
- `SO_KEEPALIVE` ligado, com período de 30 s, inclusive sob o limitador de
  conexões (o *wrapper* é desembrulhado para chegar ao socket).

### 4.5 RELP sobre TLS

Removido no go-relplog-nats-server (§1.2): não há listener TLS, `[tls]` nem `payload.tls`. Quem precisa de RELP cifrado usa o `go-relplog-server`, ou um terminador TLS na frente, com PROXY protocol (§4.6) para que o `src-ip` continue o do emissor. O leitor RELP não escreve fora do escritor da sessão (no R, o `KeyUpdate` do TLS 1.3 fazia o `Read` escrever, e o prazo de escrita precisava cobrir isso; aqui a regra de armar os dois prazos antes da sessão fica, §5.9, sem esse motivo).

### 4.6 PROXY protocol

Opt-in, para quando um balanceador (HAProxy, NLB, Traefik)
está na frente e o `RemoteAddr` seria o dele.

- `proxy.protocol`:

  | Valor | Comportamento |
  |---|---|
  | `off` (padrão) | nada é lido; um cabeçalho PROXY é um frame RELP malformado e fecha a conexão |
  | `optional` | de par **confiável**, lê o cabeçalho se ele vier e segue sem ele se não vier; de par não confiável, nada é lido |
  | `required` | toda conexão precisa vir de par confiável **com** cabeçalho válido; senão fecha (`relplog_conns_refused_total{reason="proxy"}`) |

- Par confiável: `proxy.trusted` (CIDRs, padrão `127.0.0.1/32,::1/128`), testado
  contra o par TCP real.
- v1 (texto, `PROXY TCP4|TCP6|UNKNOWN ...\r\n`, até 107 bytes) e v2 (binário,
  assinatura de 12 bytes). O cabeçalho chega **antes** do `open` do RELP. Prazo
  próprio: `proxy.header_timeout` (5 s).
- v2 com comando `LOCAL`, família `UNSPEC`/`UNIX`/`DGRAM`, ou v1 `UNKNOWN`: usa
  o par real, como se não houvesse cabeçalho. TLVs são lidos e ignorados; um
  cabeçalho v2 acima de 4 KiB fecha a conexão.
- Parser próprio, só com a stdlib, alvo de fuzz (`FuzzProxy`, §18.3).
- Detalhes: em `optional`, um cabeçalho **inválido** de par confiável fecha a
  conexão (`proxy_headers_total{result="invalid"}` e
  `conns_refused_total{reason="proxy"}`); um cabeçalho que declara origem não
  especificada é inválido; de par não confiável nada é lido nem contado; uma
  conexão que fecha antes do primeiro byte (sonda de saúde do balanceador) não
  conta nada; e uma espera de cabeçalho vencida pelo **shutdown** (com ou sem
  cabeçalho pela metade) não conta nada nem loga, porque é um deploy e não uma
  falha do balanceador (pendência 3 do S, corrigida). `LOCAL`, `UNSPEC`, `UNIX`,
  `DGRAM` e `UNKNOWN` contam como `local`.
- **Efeito no payload:** `host.src-*` e `host.dst-*` vêm do cabeçalho (o
  cliente e o endereço que ele discou), o `sid` é calculado com eles, e o par TCP
  real vai em **`x-proxy: {"peer-ip", "peer-port"}`**, para que a ponta real do
  transporte continue registrada (§20.8).
- **Ressalva:** `proxy.trusted` largo demais permite forjar `src-ip`.
- **Com um terminador TLS na frente** (stunnel, HAProxy com `ssl`), o PROXY
  protocol é o que mantém o `src-ip` do emissor no payload; sem ele, todo
  envelope teria o IP do terminador.

---

## 5. RELP

O RELP é o protocolo do `rsyslog` (o `librelp`, usado pelo `omrelp` e pelo
`imrelp`). Não é RFC, mas é a **única** forma difundida de fazer syslog com
confirmação por mensagem, e por isso é o único transporte em que a regra de
aceitação deste projeto pode ser cumprida ao pé da letra (§2.3). Esta seção é a
do R, que consolidou a especificação extraída da standard em 2026-10-05 com as
correções que a leitura do código do `librelp` (2026-10-11) impôs; onde o
comportamento do cliente decide, o arquivo do `librelp` está citado. **O
protocolo não mudou no corte**: o que mudou foi só o que pode acontecer com
uma transação depois que ela é lida (sem filtro, sem rate-limit, sem
`ring=none`, sem TLS), e §5.4 e §5.7 dizem o efeito.

### 5.1 Moldura

```
TXNR SP COMMAND SP DATALEN [SP DATA] TRAILER
```

- `TXNR`: número de transação em decimal, escolhido pelo cliente, até 9
  dígitos (o `librelp` volta a 1 depois de 999 999 999). Vale `0` só no
  `serverclose`.
- `COMMAND`: 1 a 32 letras ASCII. `open`, `syslog` e `close` vêm do cliente;
  `rsp` e `serverclose`, do servidor. Comandos são sensíveis a maiúsculas: `SYSLOG`
  é um comando desconhecido.
- `DATALEN`: comprimento de `DATA` em decimal, até 9 dígitos.
- `TRAILER`: um `\n`.

**O cabeçalho do frame não pertence à mensagem.** `TXNR`, `COMMAND`, `DATALEN` e
o `\n` final nunca entram na linha crua gravada; só `DATA` é a SYSLOG-MSG. Um
`\n` ou um NUL **dentro** de `DATA` é dado (o RELP delimita por comprimento), e a
conversão só tira CR/LF/NUL do **fim** de `message` (§6.7).

**`DATALEN = 0` aparece de duas formas** no parque instalado: `3 close 0\n` e
`3 close 0 \n`. O leitor aceita as duas. Recusar uma seria recusar metade dos
emissores por causa de um byte. O servidor, ao contrário, **escreve só a
primeira**: o leitor do `librelp` trata o byte seguinte a um `DATALEN` zero como
o trailer (`relpframe.c`, `do_trailer`) e recusa o espaço. O
`0 serverclose 0 \n` do código de referência era, para o cliente, um frame
inválido (§5.9).

**Uma moldura que não dá para ressincronizar fecha a conexão**: `TXNR` ou
`DATALEN` não numérico ou longo demais, comando com byte que não é letra, falta
do `\n` onde o comprimento termina, `DATALEN = 0` seguido de dado, `DATA` depois
de `\n` em vez de espaço. Procurar "o próximo frame" gravaria fragmentos como
mensagens. Conta em `relplog_relp_frame_errors_total{reason="malformed"}`, com
`WARN` amostrado; o que já foi decidido antes do frame ruim recebe a resposta, e
a conexão termina com `serverclose` (§5.8). Uma conexão que termina **no meio**
de um frame não entrega nada dele: o frame nunca foi respondido, e o `librelp` o
retransmite.

O leitor é alvo de fuzz contra um oráculo independente (`FuzzFrame`, §18.3): os
mesmos frames, os mesmos dados cortados, o mesmo `DATALEN`, e o erro exatamente
onde o oráculo para.

### 5.2 Fusível e custo por conexão

- **`relp.max_frame_bytes`** (16 MiB) é o fusível: um `DATALEN` acima dele é
  recusado **antes de ler um byte do corpo**, a conexão fecha e conta em
  `relplog_relp_frame_errors_total{reason="too_large"}`. Precisa ser ≥
  `syslog.max_message_bytes` e ≤ 999 999 999 (exit 1 fora disso).
- **O buffer cresce com os bytes, nunca com o `DATALEN` declarado.** O buffer do
  frame começa em 2 KiB (ou no tamanho do frame, se menor) e dobra conforme os
  bytes **chegam**, até `syslog.max_message_bytes`. Quem declara 16 MiB e fica
  calado custa um bloco. Sem essa regra, mil conexões custariam 16 GiB residentes
  por nove bytes enviados cada (bug real de 2026-09-14; §5.9).
- **Custo fixo por conexão** (decisão 11 do R, pendência 1 do S): o `bufio.Reader`
  tem 4 KiB (o leitor só usa `ReadByte` e leituras contadas; o maior `Peek` é o
  do PROXY, de 12 bytes) e o escritor de respostas, outros 4 KiB; depois de um
  frame grande, o leitor descarta o buffer acima de 16 KiB em vez de segurá-lo
  enquanto a conexão viver ociosa — mas só quando nada do frame seguinte está
  no `bufio.Reader` (o emissor parou e a leitura seguinte pode esperar o
  `idle_timeout`). Descartado antes de **todo** frame, um emissor de linhas de
  24 KiB refazia o crescimento inteiro por frame (2+4+8+16+24 KiB e as cópias:
  55 KB e 5 alocações por frame, 12 µs contra 0,5 µs) para um buffer que voltaria a
  crescer (revisão independente do `go-syslog-server`, 2026-10-13, E2;
  `BenchmarkRELPReaderFrames`, TESTING §4). O buffer cresce dobrando a partir
  de 2 KiB até o tamanho do frame, não pelas classes do pool: um frame de até
  16 KiB nunca passa do teto. Com `max_conns = 0`, o custo de N conexões
  caladas é cerca de N × 10 KiB mais as goroutines, fora de `ring.max_bytes`.

### 5.3 Tamanho e truncamento (decisão 13 do S)

`syslog.max_message_bytes` (env `SYSLOG_MAX_MESSAGE_BYTES`, padrão 64 KiB; a RFC
5424 §6.1 exige aceitar 480):

- Um `DATA` maior que o limite é **truncado no fim**: o cru guarda os primeiros
  `max_message_bytes` octetos, e `size` guarda o `DATALEN` original.
- O **excedente é consumido sem alocar**, para manter a moldura sincronizada, e
  a sessão continua: **a conexão não é fechada por tamanho** abaixo do fusível.
- `size > len(cru)` é o que a conversão lê como truncamento: aviso `truncated`,
  e uma sequência UTF-8 cortada no fim sai de `message` sem aviso de encoding
  (§6.7). `raw` mantém os octetos recebidos.
- `relplog_messages_truncated_total{transport}` conta.

O limite de 64 KiB é o que mantém o pior JLH abaixo de 1 MiB (§3.9) **e** o que
limita a cópia de cada transação em voo (§5.6).

### 5.4 Diálogo e o que cada resposta significa

```
cliente  1 open 86 relp_version=0\nrelp_software=librelp,1.11.0,http://librelp.adiscon.com\ncommands=syslog
servidor 1 rsp <n> 200 OK\nrelp_version=0\nrelp_software=go-relplog-nats-server,<versão>\ncommands=syslog

cliente  2 syslog 34 <13>Sep 12 14:33:01 web-01 app: oi
servidor 2 rsp 6 200 OK

cliente  3 close 0
servidor 3 rsp 0
         (o servidor fecha a conexão)
```

- **`open`**: a resposta é `200 OK` seguida das ofertas no formato do
  `relpOffersToString` do `librelp` (uma por linha, sem `\n` final):
  `relp_version=0`, `relp_software=go-relplog-nats-server,<versão>` (para um operador
  lendo o log do rsyslog saber que build está do outro lado; a versão é saneada
  para não quebrar a linha) e `commands=syslog`. A oferta é respondida, não
  negociada: versão 0 e o comando `syslog` são tudo o que o `librelp` precisa
  (`copen.c`, `relpSessCBrspOpen`).
- **`syslog` antes do `open`** recebe `500 command disabled` e a sessão continua
  (o que o servidor do `librelp` faz, `scsyslog.c`). **Um segundo `open`** recebe
  `500 protocol error: connection already open` e a sessão termina com
  `serverclose` (`copen.c`).
- **`close`**: o servidor espera as transações em voo serem decididas e
  enfileiradas, responde `rsp` com corpo vazio e encerra a sessão **sem**
  `serverclose`, como o `librelp` (`cclose.c`). Assim o cliente vê toda resposta
  da sessão antes da do `close` (decisão 7 do R; o código de referência podia mandar
  o `serverclose` antes de confirmações ainda em voo).
- **Comando desconhecido** recebe `500 command not supported` e a sessão continua:
  um comando novo é extensão do cliente, não fluxo quebrado. A métrica conta
  `command="unknown"`; a palavra do cliente nunca vira label.

**O que o `librelp` faz com cada resposta** (`scrsp.c`) decide tudo o que vem a
seguir:

| Resposta | O cliente | Logo, este servidor manda essa resposta para |
|---|---|---|
| `200 ...` | remove a mensagem da lista de não confirmadas: entregue | o que entrou no ring, e a mensagem vazia |
| qualquer outro código (`500 ...`) | **destrói** a mensagem (`relpSendbufDestruct`), loga "peer sent error response" pelo `onErr` e **nunca a retransmite**; e mais: o erro devolvido pelo tratamento da resposta (`librelp error 10020`) **suspende a ação e quebra a sessão**, e as transações da janela já enviadas e ainda sem resposta são **retransmitidas** na sessão seguinte | só o que este servidor recusou de propósito, para sempre, e raramente |
| nenhuma (sessão quebrada: conexão fechada ou `serverclose`) | reconecta e **retransmite** tudo o que não teve `200` (`relpSessTryReestablish`) | o que precisa voltar ao emissor |

A especificação extraída dizia "`500 ring full`: o rsyslog retém e retenta". O
código do cliente diz o contrário: um `500` por ring cheio era uma perda **na
origem** a cada ring cheio. E o rsyslog real mostrou o resto no R
(`TestEmittersRELPFiveHundredIsFinal` de lá, rsyslog 8.2410, 2026-10-11): a
mensagem recusada chega ao destino uma vez e nunca volta, mas o `500` suspende
a ação, e as vizinhas da janela que o servidor já tinha aceitado voltam como
**duplicatas** com outro `uuid` (5 a 6 por `500` na janela de 128). **Um `500`
custa uma reconexão e duplicatas**: por isso nada que aconteça em rajada recebe
`500`.

**Onde o R respondia `500`, reavaliado no corte** (decisão 6):

| Caso do R | Lá | Aqui |
|---|---|---|
| recusa `logic` final do destino com `ring=none` (`500 refused by the destination`) | o único `500` do fluxo normal | **não existe**: sem `ring=none`, o destino nunca fala com a transação. Uma recusa `logic` do NATS é do worker, depois do `200 OK`, contada em `events_dropped_total{reason="sink_logic"}` (§10.2) |
| rate-limit | já não era `500` (segurava) | não existe (§1.2) |
| filtrada | `200 OK` | não existe: nada é filtrado |
| origem não endereçável | `500 source address unusable` (guarda) | **igual**: guarda, inalcançável por um par TCP (§4.2), mantida porque o schema exige `host.src-ip` e um envelope sem ele não pode existir |
| `syslog` antes do `open`, segundo `open`, comando desconhecido | `500 ...` | **igual**: é o que o servidor do `librelp` responde, e só um cliente quebrado chega lá |
| erro inesperado do ring | segurava (um contexto cancelado era shutdown), ou `500` com `ring=none` | **segura**, como ring cheio; um contexto cancelado é o fim da sessão, sem resposta, como no R. O ring em memória não tem esse erro; a regra fica para o caso de aparecer (`TestRELPUnexpectedRingErrorIsHeldNotRefused`, `TestIngestEnqueueErrorDecidesWhetherTheBufferIsPooled`) |

A tabela completa:

| Desfecho (`receiver.Outcome`) | Resposta | Contagem |
|---|---|---|
| aceita | `200 OK` | `messages_accepted_total`, `confirm_duration_seconds` |
| vazia (`DATALEN = 0`) | `200 OK` | `messages_dropped_total{reason="empty"}` |
| origem não endereçável (guarda) | `500 source address unusable` | `messages_dropped_total{reason="unaddressable"}` |
| ring cheio, sink DOWN | **nenhuma enquanto dura**: segurada (§5.7) | `backpressure_seconds_total`; se estourar, `relp_unanswered_total{reason="backpressure"}` |
| shutdown | nenhuma | `relp_unanswered_total{reason="shutdown"}` |
| par sumiu / respostas não saem mais | nenhuma | `relp_unanswered_total{reason="conn_lost"}` |
| o servidor encerrou a sessão de um par presente (frame ruim, idle timeout, segundo `open`) | nenhuma para as que ficaram em voo | `relp_unanswered_total{reason="server_closed"}` |

A vazia recebe `200` porque não há o que guardar: um `500` mentiria que houve
erro, e o rsyslog logaria um erro por mensagem e quebraria a sessão a cada uma.

**Uma transação devolvida não é descarte.** `relplog_messages_dropped_total`
conta só o que foi lido e descartado de propósito. A reconciliação de uma
sessão é:

```
relplog_messages_total = accepted + dropped{empty,unaddressable}
                         + relp_unanswered_total
```

e a do lado do rsyslog: enviadas = `200` + `500` (perdidas, logadas lá) +
retransmitidas (as da sessão fechada sem resposta e as vizinhas de um `500`).
Uma recusa `logic` do NATS depois do `200 OK` está do lado do servidor, em
`relplog_events_dropped_total{reason="sink_logic"}`, fora desta conta.

**Todo frame `syslog` lido entra na conta**, inclusive o que nunca ganhou
goroutine: a sessão pode terminar entre a leitura do frame e a vaga na janela ou
no teto de §5.6 (shutdown, aborto por backpressure de outra transação). Esse
frame conta em `relplog_messages_total` e em `relp_unanswered_total` com o motivo
da sessão; sem isso a igualdade acima furava a cada aborto (revisão
independente do R, 2026-10-11, `TestReviewReconciliationAfterAbort`). O motivo é
o **primeiro** registrado pela sessão: um aborto por backpressure seguido do
shutdown conta `backpressure`; uma sessão sem motivo próprio conta `shutdown` se
o processo está parando, e `conn_lost` caso contrário.

### 5.5 Janela

O cliente mantém uma **janela** de transações não confirmadas (128 no `librelp`,
`windowSize` no `omrelp`). Ao esgotá-la, ele para de enviar sozinho: é o
backpressure do RELP, explícito no protocolo.

- **As respostas podem sair fora de ordem.** O `TXNR` está na resposta, e o
  cliente casa cada uma com a sua (`relpSessGetUnacked`).
- `relp.window` (128) é a janela **do servidor**: até tantas transações por
  conexão ficam em voo, cada uma numa goroutine, esperando a sua garantia; com
  a janela cheia, o leitor **para de ler** e o TCP segura o emissor. Com o ring
  em memória o `Enqueue` não espera nada, mas uma transação segurada (ring
  cheio, sink DOWN) não pode segurar as outras da mesma conexão: a janela é a
  resposta do próprio protocolo (`TestRELPWindowAllowsConcurrentTransactions`).
- As respostas são escritas por **uma goroutine separada** por conexão, que só
  enxerga transações já decididas e é a única que escreve no socket. Um atalho
  "responde já, enfileira depois" transformaria o único transporte confiável no
  menos confiável. O escritor junta as respostas que estiverem prontas num
  `Flush` só e descarrega quando nada mais está decidido, para uma resposta
  solitária não esperar companhia.
- A resposta de uma transação decidida **nunca é descartada** (o canal tem a
  janela de folga, e o escritor esvazia o canal até o fim mesmo depois de uma
  escrita falhar). Um escritor que falha (um par que parou de ler, além de
  `relpWriteTimeout` = 5 s por escrita) encerra a sessão: o resto fica sem
  resposta (`conn_lost`) e volta pelo `librelp`.

### 5.6 Cópias em voo e o teto global

O buffer do frame é reaproveitado pela leitura seguinte, então cada transação em
voo segura **a própria cópia** da linha crua, desde a leitura até a decisão.
**Essas cópias não estão no ring**: nada disso conta em `ring.max_bytes`, e é
justamente quando o ring está cheio que elas se acumulam. Por isso (decisão 9 do R):

- a cópia é feita **depois** de obter a vaga na janela **e** os bytes no teto
  (o código de referência copiava antes e chegava a `window + 1` cópias por
  conexão; `TestCopyComesAfterTheWindowAndTheBudget` fixa a ordem);
- **`relp.inflight_max_bytes`** (64 MiB) é um teto **global ao processo**,
  somando todas as conexões de todos os listeners. A conta de cada transação
  é **`2 × classe + 8 KiB`**: a classe do pool de §15.1 (2, 8 ou 64 KiB) da cópia
  da transação, outra para a cópia do item que cada tentativa faz antes do `Enqueue`, e 8 KiB
  pela goroutine, a pilha, o `Wire`, o timer e o fechamento (medidos em cerca de
  9,6 KiB no todo para uma linha pequena). Contar só a cópia deixava linhas
  pequenas porem até cinco vezes o teto na memória (revisão independente do
  R, 2026-10-11, `TestTxnChargeCoversTheTransaction`). **Com o ring em memória
  a segunda classe é pessimista**: a cópia do item passa ao ring no `Enqueue` e
  sai da conta da transação logo depois; ficou cobrada de propósito (decisão
  7), porque uma transação segurada refaz a cópia a cada tentativa recusada, e
  cobrar menos que o R aqui daria um teto diferente para a mesma config. A conta é adquirida antes da
  cópia e devolvida quando a transação é decidida. Esgotado, o leitor **para de ler**
  (`relplog_relp_inflight_waits_total` conta cada parada) e a janela TCP segura
  o emissor; nada é recusado por isso;
- o teto precisa ser ≥ `syslog.max_message_bytes` (exit 1), para uma transação
  sempre caber; uma aquisição maior que o teto é reduzida a ele, para nunca
  esperar para sempre;
- `relplog_relp_inflight_bytes` e `relplog_relp_inflight_transactions` mostram o
  que está em voo. O termo soma na conta de memória do processo (§15).

Com os padrões, o pior caso é 64 MiB em voo, qualquer que seja o número de
conexões; sem o teto seriam `128 × 64 KiB = 8 MiB` **por conexão**, sem limite
com `max_conns = 0`.

### 5.7 Backpressure: segurar, nunca recusar

Ring cheio ou sink DOWN: a transação **não recebe resposta enquanto a condição
dura**. Ela é retentada a
cada 50 ms; quando a janela de 128 enche, o leitor para de ler, o socket enche,
e o rsyslog segura as mensagens na fila dele, que é onde o dado deve ficar.

```
ring cheio detectado
  -> a transação espera, retentando a cada 50 ms
  -> a janela enche: o leitor para de ler e o cliente segura as mensagens
  -> ring escoou?   200 OK; o leitor volta a ler; nada se perdeu
  -> estourou relp.backpressure_timeout (30 s)?
                    a sessão é ABORTADA: o leitor para, as transações ainda
                    não decididas ficam sem resposta, as decididas recebem a
                    sua, sai 0 serverclose 0 e a conexão fecha;
                    o librelp reconecta e retransmite o que não teve 200
```

- **Nunca `500` para o que vai passar sozinho** (decisão 5 do R): o `librelp` perde o
  que recebe `500` (§5.4). Fechar a sessão é a única forma de devolver a
  mensagem ao emissor.
- `relp.backpressure_timeout` (30 s; zero é recusado) fica **abaixo** do
  `timeout` padrão do `omrelp` (90 s): o servidor decide antes de o cliente
  desistir sozinho. Fechar libera o descritor, a vaga de `max_conns`, a janela e
  as cópias em voo, e deixa o rsyslog reconectar (talvez numa réplica menos
  carregada, atrás de um balanceador).
- O tempo segurado entra em `relplog_backpressure_seconds_total{transport}`;
  cada transação devolvida, em `relplog_relp_unanswered_total{reason="backpressure"}`,
  com um `WARN` amostrado por sessão abortada. Nada disso é descarte.
- Uma retentativa **não conta de novo** a chegada: `Accept`, `Retry` e
  `RecordDrop` são caminhos separados (bug real da standard). Uma transação
  segurada conta uma chegada e um desfecho, por mais retentativas que faça
  (`TestRELPHeldMessageIsChargedOnce`).
- A retentativa é **barata**: o sink DOWN e o ring cheio são testados antes do
  `uuid`, da cópia e do item (§2.2), nos **dois** tetos do ring, e uma
  transação segurada a cada 50 ms não aloca nem cunha `uuid`
  (`TestIngestFullRingIsRefusedBeforeTheCopy`, e
  `TestIngestByteCeilingIsRefusedBeforeTheCopy` para `ring.max_bytes`).
- O `uuid` cunhado na primeira tentativa que chega a virar item é reaproveitado
  em todas (§3.1): a transação segurada é um fato só.
- **O fim da sessão vence a decisão.** Uma transação confere o contexto da
  sessão depois de cada espera (a vaga no teto, os 50 ms entre tentativas, o
  rearme do prazo de leitura) antes de tentar o ring. Sem isso, uma transação
  que acordava depois do aborto ainda podia ser enfileirada, e o `librelp`, que
  a retransmite porque não teve resposta, a entregaria duas vezes
  (`TestReviewRetryAfterPeerLeft`). Uma aquisição do teto que volta depois do
  fim da sessão é devolvida na hora (`TestDispatchAfterSessionEndStartsNothing`).
  E a transação confere o contexto **de novo logo antes do `Enqueue`**, depois
  da cópia (revisão independente do RP, 2026-10-13, portada): o fim da sessão
  que chega enquanto o item é montado deixa a transação sem resposta, e o
  `librelp` a retransmite. Vale para todo fim de sessão, inclusive os que o
  servidor decide com o par presente (frame ruim, idle, segundo `open`):
  "decidida" é "no ring", e só essas recebem a resposta antes do `serverclose`
  (`TestIngestEndedSessionIsNotEnqueued`). Um erro inesperado do ring que seja
  `context.Canceled` é o fim da sessão ou do processo: sem resposta e sem
  `ERROR` (o mapeamento do R); `DeadlineExceeded` segura como ring cheio.
- Um erro do `Enqueue` que não seja `ErrFull` nem `ErrClosed` — o ring em
  memória não tem nenhum — é segurado como ring cheio, com o buffer descartado
  sem voltar ao pool (§15.2) e um `ERROR` amostrado: nunca um `500`
  (`TestRELPUnexpectedRingErrorIsHeldNotRefused`).
- **Um par que sumiu** (EOF, reset) com transações seguradas: elas são
  abandonadas (`conn_lost`), **nunca** enfileiradas depois pelas costas da
  retransmissão, o que duplicaria o evento.
- **Sink FATAL** não é segurado à toa: o processo vai para o dump e sai com 3
  (§13.3), e o shutdown solta as transações seguradas sem resposta (§5.8); o
  `librelp` as retransmite à próxima instância.

### 5.8 Fim de sessão e shutdown

Toda sessão termina pelo mesmo caminho: o leitor para; as transações em voo
terminam (decididas ou abandonadas); o escritor esvazia as respostas decididas;
se foi o **servidor** quem encerrou uma sessão de um par ainda presente (frame
ruim, idle timeout, segundo `open`, backpressure, shutdown), sai
**`0 serverclose 0`** depois de todas as respostas; e só então o socket fecha.

**O `librelp` só percebe uma sessão fechada quando envia de novo**
(observado com o rsyslog 8.2410): as transações sem resposta ficam na lista de
não confirmadas dele, e a reconexão com a retransmissão acontece no próximo envio
da ação. Num host vivo isso é a próxima linha de log; num emissor que parou de
logar, elas esperam no rsyslog (sem perda) até a próxima. Por isso a suíte de
§18.7 mantém o emissor enviando.

O `serverclose` (decisão 6 do R; era item "a decidir pelo usuário" no S) é a dica do
protocolo: o `librelp` marca a sessão como quebrada ao recebê-lo
(`cserverclose.c`) e retransmite o não confirmado na próxima, a mesma
recuperação de um fechamento, mas explícita no log do rsyslog. Ele não sai no
`close` do cliente (o `librelp` também não manda) nem quando o par já foi
embora.

No shutdown (§13.4, fase 3):

- O contexto de recepção é cancelado **antes** de os transportes fecharem. É esse
  cancelamento que solta as transações seguradas no backpressure: elas ficam sem
  resposta (`reason="shutdown"`), e o `librelp` as retransmite. **Não há
  `500 shutdown`** (o da especificação extraída faria o rsyslog descartá-las).
- **O `Close` não fecha o socket direto.** Ele fecha os listeners e só vence o
  prazo de **leitura** de cada conexão, deixando o escritor esvaziar as
  confirmações já decididas. Fechar o socket perderia o `200 OK` de mensagens que
  já estão no ring, e elas virariam duplicatas na retransmissão (bug real; §5.9).
- Uma transação que está **dentro** de um `Enqueue` quando o shutdown chega
  termina o `Enqueue` e recebe a resposta: o ring em memória não consulta o
  contexto, e o item que entrou é drenado na fase 4.
- Frames ainda não lidos ficam sem resposta, e o rsyslog, ao ver o fim da sessão,
  retransmite o que não foi confirmado: nada se perde.
- Uma sessão que não volta em `closeGrace` (6 s), um par que parou de ler, tem
  o socket fechado sob ela: o shutdown não espera um par.
- **Todos os listeners fecham ao mesmo tempo**, dentro de um `Close`, e o
  shutdown espera **um** `closeGrace`. O fechamento em paralelo do R (onde TCP e
  TLS, fechados em sequência, somavam dois `closeGrace`) fica no código, para que
  um segundo transporte nunca traga a sequência de volta.
- O `stop_grace_period` do orquestrador precisa ser **maior** que
  `pre_stop_delay + drain_timeout + closeGrace`. Menor significa ser morto no
  meio do dreno, perdendo mensagens que já foram confirmadas por RELP.

### 5.9 Bugs que já custaram caro (não os repita)

- **Confirmação descartada no shutdown.** O `Close` fechava o socket direto e
  perdia o `200 OK` de mensagens já no ring, que viravam duplicatas na
  retransmissão. Agora o `Close` só expira o prazo de leitura e deixa o escritor
  esvaziar as confirmações decididas.
- **Corrida no prazo de leitura.** O laço renovava o prazo de ociosidade antes de
  cada frame e podia sobrescrever o "agora" do `Close`, segurando o shutdown por
  até 120 s. A ordem das escritas em `armReadDeadline` (o leitor arma e depois
  confere `closing`; o `Close` liga `closing` e depois arma) resolve.
- **Frame alocado pelo `DATALEN` declarado.** `make([]byte, n)` antes de chegar
  um byte; resolvido com crescimento pelos bytes que chegam (§5.2).
- **`Enqueue` cancelado no meio do group commit respondia como recusado** uma
  linha que seria commitada: duplicata garantida; resolvido esperando o commit.
- **Tetos de conexão por listener.** Com `tcp_listeners = N`, os tetos valiam N
  vezes; um limitador por transporte resolve.
- **`500` para ring cheio e para shutdown** (especificação extraída): o
  `librelp` descarta o que recebe `500`. Corrigido por §5.4 e §5.7.
- **`0 serverclose 0 \n`** (código de referência): frame inválido para o leitor
  do `librelp`, que exige o trailer logo depois de um `DATALEN` zero.
- **`serverclose` antes das confirmações em voo** no `close` (código de
  referência): o cliente podia ver o fim da sessão antes de respostas decididas.

### 5.10 Ordem

As transações de uma janela entram no ring **em paralelo**: a ordem entre elas
no ring segue a corrida das goroutines, não a ordem de chegada. A ordem do fato
está no `uuid` e no `event` (a amostra de relógio é tirada pelo leitor, em
ordem, antes de a transação ganhar goroutine), que é por onde todo consumidor
JLH ordena.

---

## 6. Conversão

O conversor é o **único** código deste projeto que processa entrada adversária
estruturada (com o leitor do PROXY e o leitor do dump, §12.2). Implementa o **algoritmo de conversão da
skill** `loghub-type-syslog-schema`, com os desvios D1 e D2, e o critério de
conformidade são os vetores dourados (§18.2). Os textos de `parse.error` são os
da skill, literais.

### 6.1 Regra absoluta: a conversão nunca falha

Não existe caminho em que uma mensagem seja recusada por não ser entendida. O
pior resultado é `format: "malformed"`, com `message` = a mensagem inteira, `raw`
obrigatório e o motivo em `parse.error`. Uma mensagem ilegível é exatamente a que
o operador mais precisa ver.

```
cru (já sem moldura, já truncado) + cabeçalho
  -> PRI .................. ok | missing | invalid               (§6.2)
  -> formato .............. rfc5424 | rfc3164 | malformed        (§6.3)
  -> campos + texto ....... UTF-8, avisos                        (§6.4–§6.7)
  -> payload .............. omissões, raw, parse                 (§6.8)
  -> envelope ............. uuid, from, space, level, sid, event (§3.1)
```

A propriedade de fuzz: **para todo cru e todo cabeçalho válido, a saída é um
JLH que passa nos dois schemas e nas regras semânticas** (§18.3), sem pânico.

### 6.2 PRI

`PRI = "<" PRIVAL ">"`, `PRIVAL` com 1 a 3 dígitos, 0–191, sem zero à esquerda
exceto o próprio `<0>` (RFC 3164 §4.1.1, RFC 5424 §6.2.1). Uma função só
(`convert.ScanPRI`), usada só pela conversão: a recepção não lê o PRI (§2.2):

| Situação | Estado | Efeito |
|---|---|---|
| PRI válido | `ok` | `priority` = PRIVAL; o resto da mensagem segue para §6.3 |
| Não começa com `<` | `missing` | aviso `pri-missing`; `priority` = `syslog.default_priority` (13); a mensagem inteira segue para a tentativa RFC 3164 |
| Começa com `<` mas o PRI é inválido (`<00>`, `<013>`, `<192>`, `<abc>`, sem `>`) | `invalid` | aviso `pri-invalid`; `priority` = padrão; `format = "malformed"`; **a mensagem inteira, inclusive o `<...>`, vira `message`** (RFC 3164 §4.3.3) |

Sempre: `facility.code = priority / 8`, `severity.code = priority % 8`, com os
nomes da RFC 5427:

| code | facility | code | facility | code | facility |
|---:|---|---:|---|---:|---|
| 0 | `kern` | 8 | `uucp` | 16 | `local0` |
| 1 | `user` | 9 | `cron` | 17 | `local1` |
| 2 | `mail` | 10 | `authpriv` | 18 | `local2` |
| 3 | `daemon` | 11 | `ftp` | 19 | `local3` |
| 4 | `auth` | 12 | `ntp` | 20 | `local4` |
| 5 | `syslog` | 13 | `audit` | 21 | `local5` |
| 6 | `lpr` | 14 | `console` | 22 | `local6` |
| 7 | `news` | 15 | `cron2` | 23 | `local7` |

| code = `level` | severity |
|---:|---|
| 0 | `emerg` |
| 1 | `alert` |
| 2 | `crit` |
| 3 | `err` |
| 4 | `warning` |
| 5 | `notice` |
| 6 | `info` |
| 7 | `debug` |

Os nomes de facility mudam em relação à standard (14 era `alert` e 15 era
`clock`): a tabela normativa é a da RFC 5427, que a skill adota.

### 6.3 Detecção do formato

Em ordem; a primeira linha que casar decide:

| # | Condição | `format` |
|---|---|---|
| 1 | PRI `invalid` | `malformed` |
| 2 | PRI `ok` e logo depois `VERSION SP` (`[1-9][0-9]{0,2}`) seguido de `-` (com espaço ou fim depois) ou de `AAAA-MM-DD` + `T`/`t` | `rfc5424` |
| 3 | timestamp BSD ou RFC 3339 reconhecido no início (§6.5.1), com PRI `ok` ou `missing` | `rfc3164` |
| 4 | PRI `ok`, sem timestamp reconhecível | `rfc3164` + `timestamp-missing` (§6.5.5) |
| 5 | PRI `missing` e sem timestamp | `malformed` |

Com PRI `missing` a RFC 5424 não é tentada (não há `VERSION` sem PRI). A exigência
do timestamp na linha 2 evita ler `<13>1 coisa` como cabeçalho IETF. Não existe
`parse.prefer`: a detecção é regra do tipo.

### 6.4 RFC 5424

```
SYSLOG-MSG = PRI VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID SP STRUCTURED-DATA [SP MSG]
```

#### 6.4.1 Cabeçalho

- `VERSION ≠ 1`: aviso `version-unsupported`; o resto é lido com a gramática da
  versão 1.
- Os cinco campos seguintes são separados por **um** espaço. NILVALUE (`-`) →
  campo omitido.
- Limites da RFC: HOSTNAME 255, APP-NAME 48, PROCID 128, MSGID 32, todos
  PRINTUSASCII (33–126). Campo acima do limite ou com octeto fora da faixa →
  aviso `header-invalid` e o valor é **mantido** (a standard truncava; a skill
  não perde dado). Token vazio (dois espaços) → `header-invalid` e omitido.
- O valor de um campo é decodificado como UTF-8 (§6.7): um octeto inválido vira
  U+FFFD com `encoding-invalid`.
- **Espaço em branco que o schema proíbe.** O schema declara `hostname`,
  `appname`, `procid` e `msgid` como `^\S+$`, no sentido do ECMAScript: um valor
  com TAB, U+00A0, U+2028, U+FEFF e afins tornaria o envelope inválido. Um campo
  assim é **omitido**, com `header-invalid`, e `raw` passa a ser obrigatório para
  que o valor continue no envelope (§6.8). O conversor de referência da skill
  emitiria o valor e produziria um envelope fora do schema; o fuzz de §18.3
  achou o caso. O conjunto inclui ainda U+001C–U+001F e U+0085, que o `\s` do
  ECMAScript não casa mas o do Python casa: sem eles, o `validate_payload.py` da
  própria skill recusaria um envelope que este servidor considera válido (D6).
  A ordem é: primeiro a decodificação (§6.7, que pode dar `encoding-invalid`),
  depois o teste de espaço em branco sobre o texto decodificado.
- Mensagem que termina antes do STRUCTURED-DATA (menos de cinco campos, ou nada
  depois do MSGID) → `header-invalid`; os campos faltantes são omitidos e
  `message` = `""`.

#### 6.4.2 TIMESTAMP

`AAAA-MM-DDThh:mm:ss[.1*9DIGIT](Z|±hh:mm)`:

- Tolerados com `header-invalid`: `t`/`z` minúsculos, e fração de 7 a 9 dígitos
  (a RFC permite até 6).
- Data/hora inexistente (31/04, 25:00, segundo 60), offset fora de faixa ou
  forma irreconhecível → `timestamp-invalid` e `timestamp` omitido.
- Instante que, convertido para UTC, cai fora dos anos 0001–9999
  (`9999-12-31T23:59:59-01:00`, `0001-01-01T00:30:00+01:00`) →
  `timestamp-invalid` e `timestamp` omitido: o schema exige quatro dígitos de ano
  (D8). Vale também para os timestamps RFC 3339 de §6.5.1 e para o ano inferido
  de §6.5.2.
- Saída: UTC com sufixo `Z`, **preservando a precisão** da fração, com no mínimo
  3 e no máximo 9 dígitos, por truncamento (`.5` → `.500`, `.999999` →
  `.999999`, sem fração → `.000`). Nunca `time.RFC3339Nano`, que remove zeros. O
  offset original continua visível em `raw`.

#### 6.4.3 STRUCTURED-DATA

```
STRUCTURED-DATA = NILVALUE / 1*SD-ELEMENT
SD-ELEMENT      = "[" SD-ID *(SP PARAM-NAME "=" DQUOTE PARAM-VALUE DQUOTE) "]"
SD-NAME         = 1*32 PRINTUSASCII exceto "=", SP, "]" e DQUOTE
```

- Vira um objeto `SD-ID → {PARAM-NAME → valor}`; elemento sem parâmetros → `{}`.
- PARAM-VALUE sem escape: `\"` → `"`, `\\` → `\`, `\]` → `]`; barra seguida de
  qualquer outro caractere fica literal, com os dois caracteres (`a\b`).
  Decodificado como UTF-8 (§6.7).
- PARAM-NAME repetido no mesmo elemento → **array** de strings na ordem
  recebida; uma ocorrência só é string, nunca array de um item.
- SD-ID repetido na mensagem (a RFC proíbe) → parâmetros mesclados no primeiro
  objeto (repetidos viram array) e aviso `sd-duplicate-id`. A standard fazia "o
  último vence", que perdia dado.
- `]` sem escape **dentro** de um valor entre aspas é caractere comum.
- Elementos contíguos (`][`); um espaço depois de `]` encerra o SD e começa a
  MSG; fim da mensagem depois de `]` → `message` = `""`.
- **Qualquer violação** (SD-NAME vazio ou acima de 32, falta de `="`, aspas sem
  fechamento, falta de `]`, texto colado depois do último `]`, um primeiro
  caractere que não seja `[` nem `-`) → aviso `sd-invalid`, `structured-data`
  **omitido** e todo o texto **a partir do início do STRUCTURED-DATA** vira
  `message`. Nada se perde. Os avisos de encoding dos valores de um SD que
  falhou são descartados (a decodificação de `message` produz os dela).
- **Tetos** (§6.11): exceder `syslog.sd_max_elements` ou
  `syslog.sd_max_params` também é `sd-invalid`, com o mesmo efeito.
- SD-IDs registrados na IANA (`timeQuality`, `origin`, `meta`, `ssign`,
  `ssign-cert`, `alarm`, `snmp`) não têm tratamento especial: são preservados
  como os demais. Assinaturas RFC 5848 só podem ser verificadas depois porque
  `raw` guarda os bytes exatos.

#### 6.4.4 MSG

- Começa depois do espaço que segue o STRUCTURED-DATA; sem MSG → `""`.
- CR, LF e NUL finais saem (§6.7); depois disso, um BOM UTF-8 (`EF BB BF`) no
  início sai de `message` (continua em `raw`).

### 6.5 RFC 3164 (leitura tolerante)

```
[<PRI>]TIMESTAMP SP HOSTNAME SP TAG[pid]: CONTENT
```

#### 6.5.1 Timestamps aceitos

Logo depois do PRI (ou no início, sem PRI):

| Forma | Exemplo | Observação |
|---|---|---|
| BSD | `Oct 11 22:14:15`, `Oct  1 09:00:00` | mês em inglês, caixa indiferente; dia com 1–2 dígitos depois de um ou mais espaços |
| BSD com fração | `Oct 11 22:14:15.123456` | 1–9 dígitos |
| BSD com ano | `Mar  1 2024 00:00:01` | Cisco `service timestamps ... year`; sem inferência de ano |
| RFC 3339 | `2026-10-03T08:59:58.123456-03:00` | rsyslog `ForwardFormat`, syslog-ng; offset `±hh:mm`, `±hhmm` ou `Z`; sem offset → fuso inferido |

Depois do timestamp: um `:` opcional (Cisco, ASA) e então espaço ou fim. Sem isso
não é timestamp.

**O offset ISO na RFC 3164 não tem conferência de faixa**: `+25:00` e `+05:75`
são aplicados como vierem, deslocando `payload.timestamp`, como faz o conversor de
referência da skill (na RFC 5424 o mesmo offset dá `timestamp-invalid`, §6.4.2).
Era a pendência 4 do S, "a decidir pelo usuário"; decidido no R (decisão 16 de
lá, herdada) seguir a skill, sem desvio novo: um desvio de conversão quebraria a igualdade com o
`go-syslog-server`, que recebe o mesmo `type` e publica no mesmo subject. Se a
skill passar a conferir, todos mudam juntos.

#### 6.5.2 Fuso e ano

- **Sem fuso:** `syslog.rfc3164_timezone` (IANA, padrão `UTC`) e aviso
  `timestamp-zone-inferred`. Horário ambíguo (fim do horário de verão): a
  **primeira** ocorrência. Horário inexistente (início do horário de verão): o
  deslocamento em vigor **antes** da transição. As duas regras reproduzem o
  `fold=0` do conversor de referência.
- **Sem ano:** aviso `timestamp-year-inferred`. Seja Y o ano da **amostra de
  recepção** (do cabeçalho) no fuso configurado. Testa Y+1, Y e Y−1, nessa ordem,
  e escolhe o primeiro cuja data exista e cujo instante não fique mais de 24 h
  no futuro da amostra. `Dec 31 23:59:59` recebido em 1º/jan vai para o ano
  anterior; `Jan  1 00:00:05` recebido em 31/dez 23:59:50 vai para o seguinte;
  março recebido em outubro fica no mesmo ano.
- Data inexistente em todos os candidatos (`Feb 29` fora de ano bissexto,
  `Apr 31`, `25:00:00`) → `timestamp-invalid`, `timestamp` omitido, e o resto do
  cabeçalho **continua** sendo lido.
- A referência é a amostra do cabeçalho, não o relógio da conversão: é o que
  mantém o determinismo (§3.5) num retry ou num dump feito muito depois da
  recepção.

#### 6.5.3 HOSTNAME

O token depois do timestamp é o HOSTNAME **se e somente se** for seguido de
espaço, não terminar com `:`, não contiver `[` e não contiver espaço em branco
que o schema proíba (§6.4.1; um TAB entre dois espaços, por exemplo). Senão: aviso `hostname-missing`
e o token pertence à TAG/CONTENT (o `syslog(3)` local não manda hostname:
`<30>Sep 13 10:00:00 systemd[1]: Started`). Ambiguidades inerentes ao formato
(mensagem sem hostname e sem TAG tem a primeira palavra lida como hostname; um
IPv6 terminado em `::` é lido como TAG) são aceitas.

#### 6.5.4 TAG, PID e CONTENT

```
TAG = 1*48 octetos exceto espaço em branco, "[", "]" e ":"
      [ "[" 1*128 octetos exceto espaço em branco e "]" "]" ]
      ":" seguido de SP ou do fim da mensagem
```

- Casou → `appname` = TAG sem `[pid]`; `procid` = o pid (string); `message` = o
  que vem depois de `": "` (só esse espaço sai). Uma TAG ou um pid cujo texto
  decodificado tenha espaço em branco proibido pelo schema (§6.4.1) não casa, e
  tudo fica em `message`.
- A prática usa `-`, `.`, `/`, `_`, `%` (`systemd-logind`, `postfix/smtpd`,
  `%ASA-6-302013`); o limite 48 é o do APP-NAME da RFC 5424.
- Exigir `:` + espaço evita ler `http://...` como TAG `http`.
- Não casou → sem `appname`/`procid`; todo o resto vira `message`.

#### 6.5.5 Sem timestamp (desvio D2)

PRI `ok` e nenhum timestamp reconhecível (§6.3, linha 4): aviso
`timestamp-missing`, **nenhum** HOSTNAME. A skill (§7.5 dela) para aqui e manda
tudo para `message`. Este servidor ainda tenta uma **TAG estrita** no início do
resto:

```
TAG estrita = [A-Za-z_/] *47 octetos exceto espaço em branco, "[", "]" e ":"
              [ "[" 1*128 octetos exceto espaço em branco e "]" "]" ]
              ":" seguido de SP ou do fim
```

Casou → `appname`/`procid` como em §6.5.4; não casou → tudo vira `message`.

O motivo é o formato mais comum de hoje: `<14>myapp[123]: algo aconteceu`, o que
um contêiner emite. Sem a extração, `appname` e `procid` somem de uma mensagem
em que os dois estão à vista. O primeiro caractere obrigatório (letra, `_` ou
`/`) é o que mantém a regra segura: o número de sequência do Cisco IOS
(`<189>123: *Mar  1 ...`, vetor `rfc3164-cisco-sem-timestamp`) e
`firewall,info input:` do MikroTik (sem `:` logo depois do token) continuam em
`message`. O falso positivo que sobra, `<14>error: algo falhou` → `appname =
"error"`, tem a forma que a convenção do syslog tem. O schema aceita `appname`
em `rfc3164`; nenhum vetor dourado muda.

### 6.6 `malformed`

`format = "malformed"` quando o PRI é inválido, ou quando não há PRI nem
timestamp reconhecível (o `DATA` de um frame RELP com requisição HTTP ou lixo
binário):

- `message` = a SYSLOG-MSG inteira, decodificada (§6.7);
- `raw` **obrigatório**, mesmo com `payload_raw = false`;
- `parse.warnings` com `pri-invalid` ou `pri-missing` (mais o que a decodificação
  achar), e `parse.error` **obrigatório**, com o texto literal da skill:

  | Causa | `parse.error` |
  |---|---|
  | PRI inválido | `PRI inválido: a mensagem inteira foi tratada como CONTENT (RFC 3164 §4.3.3)` |
  | sem PRI e sem cabeçalho | `sem PRI e sem cabeçalho syslog reconhecível` |

- `priority`, `facility` e `severity` com o PRI padrão; `level` acompanha;
- sem `timestamp`, `host.hostname`, `appname`, `procid`, `version`, `msgid` e
  `structured-data`.

A malformada **entra** por padrão: é o log do appender mal configurado,
precisamente quando alguém investiga por que ele está mal configurado.
`syslog.accept_malformed = false` a descarta no worker, depois da conversão, com
`relplog_events_dropped_total{reason="malformed"}` e log amostrado; o dump de
emergência a deixa de fora pela mesma regra (§12.1).

### 6.7 Texto e encoding

- **UTF-8 estrito** em todo texto (campos do cabeçalho, valores de SD,
  `message`): forma não mínima e surrogates são inválidos. **Cada octeto** que
  não inicia uma sequência válida vira **um** U+FFFD, com aviso
  `encoding-invalid`: o resultado de `utf8.DecodeRune` em laço. Nunca
  `strings.ToValidUTF8`, que junta uma sequência inválida num U+FFFD só e diverge
  da referência.
- **Charset legado** (decisão 18 do S, herdada pelo R e pelo SN: fica): `syslog.legacy_charset = cp1252 | latin1`,
  aceitando também `windows-1252` e `iso-8859-1` como sinônimos (são nomes de
  codec do Python, e `windows-1252` é o valor de exemplo da skill).
  Se `message` (só ela) não for UTF-8 válido, é decodificada com o charset, com
  aviso `encoding-legacy` em vez de `encoding-invalid`. Tabelas próprias, sem
  `golang.org/x/text`: `latin1` é identidade em U+0000–U+00FF; `cp1252` troca
  0x80–0x9F pelos pontos da Windows-1252, e os cinco octetos indefinidos (0x81,
  0x8D, 0x8F, 0x90, 0x9D) viram U+FFFD, como o `errors="replace"` da referência.
  Não se aplica a PARAM-VALUE nem ao cabeçalho (a RFC 5424 exige UTF-8 ali).
- **Truncamento** (`size > len(cru)`): uma sequência UTF-8 válida porém cortada
  no fim de `message` é descartada sem aviso de encoding (`truncated` já
  explica). `raw` mantém os octetos recebidos.
- **Bordas:** todos os CR, LF e NUL do **fim** de `message` saem. Nada mais é
  aparado além do separador do formato (o espaço antes da MSG na RFC 5424; o
  espaço depois de `TAG:` na RFC 3164).
- **Controles internos** (inclusive NUL, RFC 5424 §8.2) são preservados e saem
  com escape JSON (§3.3). Escapes de terceiros (`#012` do rsyslog) ficam como
  vieram.

### 6.8 Montagem do payload

- **Omissão** (§3.2) e **sem invenção**: `host.hostname` ausente não vira IP nem
  DNS reverso; `appname` ausente não é deduzido do texto; `timestamp` ausente não
  vira a hora de recepção (ela já está em `event`).
- **`host`**: `hostname` (se declarado) mais os campos de rede do cabeçalho, só
  nos transportes de rede. Endereço desconhecido é omitido.
- **`raw`**: o cru (já truncado), byte a byte. UTF-8 válido → texto; senão base64
  padrão com `=` e `raw-encoding: "base64"`. Obrigatório em `malformed`, com
  `encoding-invalid`/`encoding-legacy` e quando um campo do cabeçalho foi omitido
  por espaço em branco (§6.4.1); nos demais casos controlado por
  `syslog.payload_raw` (env `SYSLOG_PAYLOAD_RAW`, padrão `true`).
- **`parse`**: presente só se houve desvio; `warnings` sem repetição, na ordem
  canônica de §6.9; `error` só em `malformed`.
- **`x-proxy`**: §4.6.

### 6.9 Avisos de `parse.warnings`

Ordem canônica (a do schema):

| Código | Quando |
|---|---|
| `pri-missing` | não começa com `<` |
| `pri-invalid` | `<...>` inválido |
| `version-unsupported` | RFC 5424 com VERSION ≠ 1 |
| `header-invalid` | campo do cabeçalho RFC 5424 fora da ABNF, cabeçalho incompleto, timestamp com `t`/`z` minúsculo ou fração > 6 |
| `timestamp-missing` | RFC 3164 sem timestamp reconhecível |
| `timestamp-invalid` | timestamp com forma reconhecida e data, hora ou offset inválidos |
| `timestamp-year-inferred` | timestamp BSD sem ano |
| `timestamp-zone-inferred` | timestamp sem fuso |
| `hostname-missing` | RFC 3164 com timestamp e sem HOSTNAME |
| `sd-invalid` | STRUCTURED-DATA fora da gramática ou acima dos tetos |
| `sd-duplicate-id` | SD-ID repetido |
| `encoding-invalid` | octetos não UTF-8 no texto |
| `encoding-legacy` | `message` decodificada com o charset legado |
| `truncated` | mensagem acima do limite ou frame incompleto |

NILVALUE é válido e não gera aviso. Cada aviso emitido conta em
`relplog_convert_warnings_total{code}` (conjunto fechado de 14 valores).

### 6.10 `sid`

```
chave = transport "|" src-ip "|" src-port "|" dst-ip "|" dst-port
        (campos ausentes = texto vazio; IPs canônicos; portas em decimal)
sid   = FNV-1a-64(chave em ASCII) AND (2^53 − 1)          (unix: 0)

"udp|10.0.0.3|59922|10.0.0.10|514" → 593636897809901   (o vetor da skill)
"tcp|10.0.0.3|59922|10.0.0.10|20514"                      (uma sessão RELP)
```

Calculado com os endereços **do payload** (os do PROXY, quando houver), para que
qualquer consumidor o recalcule a partir de `payload.host` e
`payload.transport`. Agrupa os envelopes de uma conexão RELP (uma sessão do
`omrelp`), não é aleatório e cabe nos 53 bits que o JSON representa sem
perda.

### 6.11 Tetos de STRUCTURED-DATA

`syslog.sd_max_elements` (32) e `syslog.sd_max_params` (64 por elemento). A skill
não tem teto; sem ele, um emissor com dez mil parâmetros num elemento vira um
objeto de dez mil chaves no worker. Exceder um teto é tratado como SD inválido
(`sd-invalid`, SD omitido, texto em `message`): a memória fica limitada e nada
se perde, ao contrário da standard, que ignorava o excedente em silêncio.

---

## 7. Filtro

Removido no go-relplog-nats-server (§1.2): não há filtro por facility nem por severidade, e a recepção nem lê o PRI (§2.2). Um `omrelp` que mandava tudo e contava com o `[filter]` do R manda tudo e tudo é publicado; o descarte, se houver, é do consumidor (ou do próprio rsyslog, com um `if` antes da ação). Toda chave `filter.*` é exit 1 na linha de comando e no INI, e `RELPLOG_FILTER_*` no ambiente é um `WARN` (§13.2).

---

## 8. Rate-limit

Removido no go-relplog-nats-server (§1.2): não há rate-limit por IP de origem. No R ele **segurava** a transação até haver ficha, nunca `500` (a regra 8a de lá, "o rate-limit modela, não descarta"), e esta regra **não tem mais a que se aplicar**: nada na recepção espera por outra coisa que o ring ou o sink. A proteção contra um emissor ruidoso é a da rede (firewall, QoS), a de `tcp.max_conns`/`tcp.max_conns_per_ip` (§4.4) e a do próprio RELP: a janela de 128 transações por conexão (§5.5) e o teto global em voo (§5.6) limitam o que um emissor tem pendente aqui. Toda chave `ratelimit.*` é exit 1; `RELPLOG_RATELIMIT_*` no ambiente é um `WARN`.

---

## 9. Ring

### 9.1 Interface

```go
type Ring interface {
    // Enqueue aceita o item. Não bloqueia por capacidade: cheio devolve
    // ErrFull na hora; fechado devolve ErrClosed.
    Enqueue(ctx context.Context, it *item.Item) error
    // Dequeue entrega até n itens aos workers de dreno; um lote vazio com
    // erro nil quer dizer "pergunte de novo".
    Dequeue(ctx context.Context, n int) ([]*item.Item, error)
    // Commit informa que os itens foram confirmados no sink (ou descartados
    // de propósito, como uma malformada recusada).
    Commit(items []*item.Item) error
    // Drain devolve os itens pendentes (dump de emergência).
    Drain() []*item.Item
    Stats() Stats
    Mode() string
    Close() error
}
```

`Enqueue` só devolve `nil` quando o item está no ring. A confirmação (o `200 OK`
da transação RELP) acontece depois desse retorno, nunca antes. O `Item`
carrega o cabeçalho (§3.4) e o cru; **não carrega envelope**. O `Touch` do R
(renovação de posse no ring Redis) saiu com aquele ring. O ring em memória tem
ainda `FullFor(n)`, a pré-checagem sem trava dos dois tetos de §2.2.

A interface fica, com uma implementação só, porque é a costura pela qual a
recepção e os workers são testados (§2.1).

### 9.2 Saturação

Dois tetos, avaliados juntos:

- `ring.capacity_items` (padrão 65 536);
- `ring.max_bytes` (padrão 512 MiB), sobre a memória que o item **segura**:
  `cap(cru)` mais `ItemOverhead` = **256 B**. A struct `item.Item` tem 224 B
  (`unsafe.Sizeof`, medido em 2026-10-07, já com a caixa `*[]byte` do buffer
  do pool, §15.1), na classe de alocação de 224 B, mais os 8 B do slot no canal
  do ring: 232, arredondados para 256. `TestItemOverheadCoversTheStruct`
  arredonda a struct para a classe de alocação antes de comparar. No R eram
  384 B: o item carregava ainda o id de linha do `ring=sqlite`, o id de entrada
  do `ring=redis` e um canal `done`, que saíram com esses rings.
  O teste falha se a struct crescer além do número.
  Contar `len(cru)` deixava uma linha de 8 193 bytes, copiada para o buffer de
  64 KiB da sua classe de pool (§15.1), custar oito vezes o que a contabilidade
  dizia. `Enqueue`, `Commit` e `Drain` usam a mesma medida.

  O JLH é memória do worker, com teto próprio (§15.3), e somá-lo aqui seria
  contar memória que ninguém alocou.

Os dois tetos contam a **mesma população**: o aceito e ainda não confirmado
(`Commit`), **inclusive** os lotes que os workers retêm em retentativa. O teto
de itens é um contador próprio, não o buffer do canal, que só limitaria o que
ainda está na fila: com um lote retido, um ring de capacidade 4 aceitava a
quinta mensagem e o `/readyz` dizia `5/4` (achado pela suíte de integração do
`go-syslog-server`). "Um lote retido não está no ring" (§10.2) diz respeito ao que o `Drain`
do shutdown devolve, não à contabilidade.

Qualquer um estourando satura o ring: a transação é segurada (§5.7).
`ring.high_water` (0.80) é avaliado nos dois e derruba o `/readyz`.

### 9.3 `ring.mode = memory`

O único ring (não há chave `ring.mode`, §1.2). Canal com buffer de
`capacity_items`, com contadores atômicos de itens e bytes. `Enqueue` retorna
assim que o item entra no canal. **Garantia:** RAM deste nó. Perde sob
`kill -9`, OOM ou pane; sob `SIGTERM` a perda é evitada pelo dreno do shutdown
(§13.4), e sob FATAL do sink ou dreno estourado, pelo dump (§12).

`Dequeue` espera até `sink.batch_max_delay` (5 ms) para completar um lote depois
do primeiro item: lotes maiores são menos idas ao servidor NATS e
amortizam a conversão, que roda por item no worker.

### 9.4 `ring.mode = sqlite`

Removido no go-relplog-nats-server (§1.2): o ring é só memória. Quem precisa de `kill -9` sem perda usa o `ring=sqlite` do `go-relplog-server`.

### 9.5 `ring.mode = redis`

Removido no go-relplog-nats-server (§1.2): o ring é só memória.

### 9.6 `ring.mode = none`

Removido no go-relplog-nats-server (§1.2): o ring é só memória, a conversão nunca roda na transação, e o `200 OK` nunca espera o destino (§2.3, §5.4).

### 9.7 Combinações que degradam ou recusam

Removido no go-relplog-nats-server (§1.2): há uma combinação só, `memory` → `nats`.

---

## 10. Sink

### 10.1 Interface e workers

```go
type Sink interface {
    // Write entrega um pedaço de lote já convertido. Devolve erro classificado
    // (ClassNetwork, ClassLogic, ClassFatal) para a máquina de §13.3 decidir.
    Write(ctx context.Context, batch *Batch) error
    // Health é chamada periodicamente; define UP/DOWN.
    Health(ctx context.Context) error
    Close() error
    Type() string
}
```

Um `Batch` carrega, por envelope (`sink.Envelope`): o JLH (fatia de um buffer do
pedaço, com capacidade limitada ao próprio tamanho) e o item de origem, de cujo
cabeçalho saem o `uuid` (o `Nats-Msg-Id` e o `X-LogHub-UUID`) e o `space` (o
subject e o `X-LogHub-Space`, §3.8). A struct ainda carrega o `level` e o `sid`
do envelope, herança das colunas SQL do R; o sink NATS não os lê.

Os workers de dreno (`sink.workers`, padrão **8**, o do R) dividem a **única**
conexão NATS do processo (§10.7):

1. `Dequeue` até `sink.batch` (256) itens, esperando no máximo
   `sink.batch_max_delay` (5 ms) para completar. O worker só termina quando o
   contexto acaba ou o ring diz `ErrClosed`; qualquer outro erro é logado com
   amostragem e retentado com backoff (`retry_backoff_min` → `max`). O ring em
   memória não tem outro erro, mas sair nele reduziria o dreno a zero em
   silêncio, com `/healthz` 200 e o ring enchendo.
2. **Converte** cada item (§6) e serializa o JLH num buffer de pedaço. O pedaço
   fecha quando a soma dos JLH passa de `sink.batch_max_bytes` (16 MiB); o item
   que passou abre o pedaço seguinte, e um JLH maior que o teto forma um pedaço
   sozinho. Malformada recusada (§6.6) não entra em pedaço nenhum: é contada e
   confirmada junto.
3. `sink.Write(ctx, pedaço)`, com retentativa (§10.2).
4. Sucesso: `ring.Commit(itens do pedaço)`, devolve o cru e o buffer do pedaço
   aos pools, `relplog_events_written_total += n`.
5. Próximo pedaço.

O teto de bytes existe porque o JLH chega a 12× o cru: `workers × batch × pior
JLH` passaria de 1 GiB.

A entrega fora dos workers — a reingestão do boot (§12.2) — passa por
`sink.Instrumented.Deliver`, que corta os envelopes em pedaços pelas mesmas
duas regras e aplica as classes de §10.2 pedaço a pedaço. Como nos workers,
`relplog_sink_batch_size` e `relplog_sink_batch_bytes` são observados uma vez por
pedaço, não por tentativa (um pedaço retentado puxaria os histogramas para o
que o destino estava recusando), e uma escrita cortada pelo contexto de quem
chama (um `SIGTERM` no boot) não conta como erro do sink (§14.1). Uma recusa
`logic` devolvida por uma escrita durante a qual esse contexto acabou é
**resolvida antes** do cancelamento — contada **e** logada com os `uuid`, e a
chamada segue: conferido o contexto primeiro, a recusa ficava contada pela
escrita e nunca logada (revisão independente do RP, 2026-10-13, portada;
`TestDeliverLogicRefusalWinsOverTheCancellation`).

### 10.2 Falha de pedaço, e o outbox

O worker **segura o pedaço** e retenta com backoff (`sink.retry_backoff_min`
100 ms → `retry_backoff_max` 2 s), **sem puxar mais nada do ring**. O ring para de
escoar e enche, até o backpressure de §5.7 aparecer: as transações RELP são
seguradas.

- **O retry reenvia os mesmos bytes**, com o mesmo `uuid`: o JLH já serializado é
  reaproveitado, nunca refeito. É a regra do outbox do JLH, e poupa CPU no
  momento em que o sistema está em dificuldade.
- **Um lote retido não está no ring.** O shutdown o coleta de
  `Workers.Pending()`, ou ele some depois de confirmado. E o `hold` faz `append`,
  nunca atribuição: um worker que abandona o lote no shutdown o mantém registrado,
  e um `Dequeue` posterior não pode sobrescrevê-lo (bug real do `go-webhook-server`).
- **At-least-once.** Uma escrita que estourou o prazo pode ter sido aceita, e o
  retry a reenvia. Em `jetstream` a deduplicação é do stream: o `Nats-Msg-Id`
  (`<uuid>.0`) repetido dentro da `duplicate_window` é confirmado como
  duplicata e guardado uma vez só, e a confirmação com `Duplicate` conta em
  `relplog_sink_nats_duplicates_total`. Fora da janela, uma redelivery duplica.
  Em `core` não há deduplicação: o consumidor deduplica por `(uuid, step)`, como
  o JLH manda.
- **Contaminação** (§15.2): uma tentativa que termina por prazo ou cancelamento,
  ou qualquer erro depois da primeira publicação assíncrona, marca o pedaço, e
  nem o cru nem o buffer do pedaço voltam ao pool, nem quando a retentativa
  seguinte tem sucesso.

| Classe | Efeito |
|---|---|
| `ClassNetwork` | retenta; não muda o estado de saúde (§13.3) |
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`relplog_events_dropped_total{reason="sink_logic"}`), confirmado no ring |
| `ClassFatal` | FATAL imediato |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`relplog_events_written_total`, só os demais em `sink_logic`, e o log traz só os
`uuid` perdidos mais a contagem gravada. O pedaço é confirmado inteiro. É exato
no **JetStream**: numa recusa `logic` de um futuro (`10054`, mensagem maior que
o `max_msg_size` do stream), o sink **espera os demais futuros** e informa os
confirmados; um erro `network` ou `fatal` de um futuro continua retornando na
hora, contaminado. **Uma mensagem recusada pelo próprio cliente antes de
sair** (`ErrMaxPayload`: uma linha de dump de uma execução com
`max_message_bytes` maior, ou uma reconexão a um nó com `max_payload` menor) é
recusada **sozinha**, como `logic`, e as demais do pedaço continuam sendo
publicadas: o `Delivered` lista exatamente o que foi confirmado (em
`jetstream`) ou publicado (em `core`). Antes da revisão independente, essa
recusa encerrava o pedaço: as mensagens seguintes nunca saíam e eram
confirmadas no ring, as anteriores (guardadas pelo stream) contavam como
perdidas, e um dump nessa situação era removido.

**Uma recusa `logic` não volta ao emissor.** O `200 OK` já saiu quando o worker
a encontra: o rsyslog apagou a mensagem, e ela é perda contada e logada do lado
do servidor, como no SN. No R, só com `ring=none` a recusa chegava à transação
(o `500` de §5.4); com o ring em memória isso não existe.

### 10.3 `sink.type = redis`

Removido no go-relplog-nats-server (§1.2). A regra que estava aqui — uma resposta **fatal** à sonda de saúde é FATAL na primeira checagem, sem passar pelos fusíveis — vale para o NATS e está em §13.3.

### 10.4 `sink.type = sqlite`

Removido no go-relplog-nats-server (§1.2).

### 10.5 `sink.type = postgres`

Removido no go-relplog-nats-server (§1.2): a fila `loghub.inbox` é o destino do `go-relplog-pg-server`.

### 10.6 `sink.type = mysql`

Removido no go-relplog-nats-server (§1.2).

### 10.7 `sink.type = nats` (padrão)

O único destino (não há chave `sink.type`, §1.2). `github.com/nats-io/nats.go`
com o pacote `jetstream` (a API atual). **Uma conexão por processo**,
multiplexada pelos workers e pelo beacon (§11), aberta preguiçosamente pela
primeira checagem de saúde; o `WaitReady` do boot é o laço de retentativa
(§13.1 passo 7). O nome do cliente é `rlnats/<AgentName>@<Hostname>`, visível
no `connz` do monitor do `nats-server` (o R usa `rls/`; o prefixo distinto
diz no `connz` qual dos dois está conectado). A discagem tem o prazo de
`sink.op_timeout`.

**Sempre texto claro** (§1.2): sem `nats.Secure`, sem *handshake* TLS. Uma URL
que não seja `nats://` é recusada pela configuração (§13.2) e, de novo, pelo
próprio sink ao ser montado (defesa em profundidade: o `nats.go` discaria
`tls://`, `ws://`, `wss://` e um `host:porta` sem esquema). Isso não basta: o
`nats.go` **se promove a TLS sozinho** quando o `INFO` do servidor diz
`tls_required`, com um `tls.Config` padrão e as raízes do sistema. O único
gancho que ele consulta antes do *handshake* é o `TLSCertCB`, e o sink o
instala — e nada mais de TLS, que ligaria o `Secure` — como uma recusa: a
conexão termina antes de qualquer `ClientHello`, com o erro
`errNATSServerRequiresTLS`, que é **`fatal`**. Um servidor que exige TLS nunca
aceitará o texto claro deste, então o boot sai com 3 na hora, nomeando o TLS,
em vez de retentar até `sink.boot_timeout`.

**Depois do boot também.** Um servidor que passa a exigir TLS numa reconexão
nunca chega ao `ReconnectErrHandler` (o `nats.go` só o chama quando a discagem
TCP falha; a recusa do `TLSCertCB` acontece depois, no *handshake* de conexão, e
fica só no `LastError`, com o estado `RECONNECTING`). Por isso o `Health`, ao ver
o cliente `RECONNECTING`, lê o `LastError` (o `escalate`) e transforma esse
erro em veredito, FATAL nomeando o TLS. Antes, a checagem dizia "connection is
RECONNECTING", `network`, e o FATAL vinha dos fusíveis uns 20 s depois, com o
motivo errado — ou nunca, com os dois fusíveis desligados (revisão
independente, 2026-10-13, `TestNATSReconnectToATLSOnlyServerIsAVerdict`; a
portar para o SN e o `go-webhook-nats-server`).

**Conexão:** `MaxReconnects(-1)` (os fusíveis de §13.3 decidem quando desistir;
um cliente que se fechasse sozinho depois de N tentativas transformaria uma
queda longa num estado irrecuperável, sem dump), `ReconnectWait(500ms)` e
**`ReconnectBufSize(-1)`**: durante uma desconexão, publicar falha na hora
(`network`) em vez de guardar mensagens dentro do cliente. O ring é o único
buffer; uma fila escondida no cliente escaparia da contabilidade, do
backpressure e do dump (§20.19). A desconexão é logada em `WARN` (a do próprio
`Close` não); a reconexão conta em `relplog_sink_nats_reconnects_total`, é
logada em `WARN` e dispara um beacon (§11); um fechamento decidido pelo cliente
(credenciais recusadas duas vezes) é `ERROR` e o estado `CLOSED` é fatal. Os
erros assíncronos do cliente (uma permissão de publicação negada gera um por
mensagem) são logados com amostragem, um a cada 30 s com a contagem suprimida
(§14.2). Os logs `nats connected` e `nats reconnected` mostram do servidor só
esquema e host (`nats://host:porta`): o `ConnectedUrlRedacted` do `nats.go`
esconde a senha e nada mais, e uma URL cujo usuário é um token
(`nats://token@host`, §10.9) saía inteira no `INFO` de todo boot e no `WARN` de
toda reconexão.

**`sink.nats.mode = jetstream`** (padrão): um `PublishMsgAsync` por envelope, e só
depois de os N futuros estarem no ar o worker espera as N confirmações, sob o
`sink.op_timeout` do pedaço. Três opções que não são as padrões, e são as que
importam:

- `WithExpectStream(stream)`: o servidor recusa a mensagem se o subject pertencer
  a outro stream; erro de configuração vira recusa fatal (`10060`), não
  mensagens no lugar errado.
- **`WithRetryAttempts(0)`**: a retentativa interna da biblioteca relê
  `msg.Data` depois de o `Write` ter retornado, a corrida de posse de buffer de
  §15.2. Quem retenta é o worker, ainda dono do buffer (§10.2).
- `WithPublishAsyncTimeout(sink.op_timeout)`: sem ele, um futuro sem resposta
  segura a mensagem e o buffer do pedaço para sempre.

`WithPublishAsyncMaxPending` = `2 × sink.workers × sink.batch`: o dobro do
que os workers podem ter no ar ao mesmo tempo (a reingestão roda antes de eles
começarem). A folga existe porque os futuros de um pedaço contaminado continuam
contando até o próprio prazo assíncrono, que começa na publicação, um pouco
depois do prazo do worker: sem ela, uma retentativa rápida parava 200 ms na
janela cheia e falhava com `ErrTooManyStalledMsgs` (revisão independente,
2026-10-13). O `+ ring.max_inflight` do R saiu com o `ring=none`. Com os tetos
de §13.2 (`sink.workers` ≤ 256, `sink.batch` ≤ 65 536) o número fica em no máximo
33 554 432: um `int` positivo em qualquer plataforma, que o
`WithPublishAsyncMaxPending` aceita (ele recusa `< 1`, e chaves sem teto podiam
dar a volta). `Data` aponta para o buffer do pedaço, que só volta ao pool depois
de todos os futuros resolverem; qualquer erro depois da primeira publicação
deixa o buffer para o GC (§15.2).

Cada confirmação com `PubAck.Duplicate` conta em
`relplog_sink_nats_duplicates_total`. Um futuro com erro `network` ou `fatal`
encerra a espera na hora (o pedaço volta inteiro, contaminado); um com erro
`logic` não: os demais dizem o que o stream guardou (§10.2, entrega parcial).

**`sink.nats.mode = core`** (opt-in, *best effort*): `PublishMsg` por envelope e
um *flush* por pedaço, sob o prazo do worker (ou `FlushTimeout(sink.op_timeout)`
sem prazo). O cliente copia `Data` para o buffer de escrita dentro do
`PublishMsg`, então nada lê o buffer do pedaço depois do `Write`. O sucesso
significa que o servidor NATS **recebeu** as mensagens, não que alguém as
guardou: sem stream, uma mensagem sem assinante some, e não há deduplicação.
**Com RELP isso desfaz a promessa do protocolo** (o `200 OK` sai e ninguém
guardou). O boot registra um `WARN` ("best effort: success means the NATS
server received the messages, not that anyone kept them"), e a primeira
checagem um `INFO` com a maior mensagem e o `max_payload`.

**Um *flush* que atravessa uma reconexão não confirma** (`mode=core`). O `nats.go`
falha um *flush* pendente com `ErrConnectionClosed` quando a conexão **cai**,
embora o cliente só esteja reconectando: a tabela classifica esse erro como
`fatal` (um cliente fechado de vez), e um soluço do NATS no meio do *flush* era
FATAL, dump e exit 3. O `flushErr` o reclassifica como `network` enquanto a
conexão não está `CLOSED` (no `Write` e no `Health`). E um *flush* que
**começa** durante a reconexão é resolvido pelo `PONG` do servidor novo,
embora os `PUB` do pedaço tenham ido pelo socket velho: o `writeCore` compara o
número de reconexões antes da primeira publicação e depois do *flush*, e se ele
mudou o pedaço é `network`, incerto, não confirmado (o `flushErr` foi achado ao
montar este módulo, a contagem de reconexões na revisão independente; os dois
em 2026-10-13; a portar para o R, o SN e o `go-webhook-nats-server`).

**Subject** (§3.8): sempre `loghub.<space>.syslog`, derivado do `space` do
item. Não há `sink.nats.subjects` (§1.2, decisão 3).

**Validação do stream, no boot e em toda checagem** (`jetstream`): na primeira
checagem que alcança o stream e de novo em **toda** checagem depois dela,
contra o `StreamInfo` que a própria checagem já buscou (nenhuma ida e volta a
mais). Cada recusa é **FATAL imediato**: no boot, exit 3 sem esperar
`boot_timeout`; depois, FATAL, dump e exit 3 (§13.3). Conferido só no boot, um
stream editado por baixo — subjects estreitados, `no_ack` ligado, `max_msg_size`
reduzido — respondia toda publicação com um prazo estourado ou uma recusa,
`network` retentado para sempre com o sink UP e o ring enchendo:

| Verificação | Como |
|---|---|
| O stream existe | `js.Stream(sink.nats.stream)` (padrão `LOGHUB_SYSLOG`); `10059` é fatal |
| O stream **cobre** o subject | casamento local de padrões (as regras de token do NATS: `*` é um token, `>` um ou mais, só no fim): algum subject do stream cobre `loghub.<space>.syslog`, com o `space` desta instância |
| Nenhum subject do stream captura o plano de controle | um padrão do stream que case `<ctl_namespace>.beacon` (como `loghub.>`) é recusado: o stream guardaria todo beacon e os reproduziria, velhos, para todo consumidor novo (§20.21) |
| Não está selado | `Sealed = false` |
| Confirma publicações | `NoAck = false`: um stream com `no_ack` transformaria todo futuro em `ErrAsyncPublishTimeout` (`network`), retentado para sempre, com o ring enchendo e o sink UP |
| A maior mensagem cabe | `nc.MaxPayload()` e o `max_msg_size` do stream (quando > 0) contra a maior mensagem de §3.9; com `storage: file`, o registro contra os 32 MiB do file store (`10077`) |
| O stream cabe uma mensagem | com `discard: new` e `max_bytes` > 0, o registro (maior mensagem + subject + 64 B, §3.9) contra o `max_bytes`: abaixo de um registro, o maior evento seria recusado para sempre, e o `10077` de um stream cheio, que é `network` (abaixo), seguraria o ring sem fim. Com `discard: old` o stream abre espaço, e passa |
| Retenção guarda o que confirma | `retention: interest` é FATAL: sem consumidor interessado, o servidor responde com `PubAck` normal e não guarda nada, e o pedaço seria confirmado e perdido — com RELP, depois de um `200 OK` (decisão 16 do R, herdada). `limits` e `workqueue` são aceitos |
| Teto por subject | `max_msgs_per_subject > 0` só gera `WARN`, uma vez por valor: todo evento de um `space` divide um subject, e os mais antigos são descartados conforme os novos são confirmados |

Com tudo aceito, o log traz `stream validated` com o stream, os subjects, o
`storage`, as réplicas, a `duplicate_window`, a maior mensagem e o
`max_payload`, uma vez (a primeira validação; as seguintes só falam quando
recusam). Só o subject do **próprio** Workspace é exigido: o de outro `space`, que um
dump reingerido trouxe, nunca entra no contrato, e estreitar o stream depois do
dreno não é FATAL. Com `core`, só o `max_payload` é conferido, também em toda checagem:
uma reconexão pode cair num servidor com `max_payload` menor. O stream é criado por
quem administra o NATS, a partir de `docker/nats/stream.json`; **o servidor
nunca cria nem altera um stream** (§20.17).

**JetStream desligado no servidor:** o `StreamInfo` recebe `ErrNoResponders`. Na
primeira checagem isso é FATAL nomeando o JetStream ("ninguém responde à API do
JetStream: desligado neste servidor, ou nenhum servidor com JetStream ao
alcance"), em vez de esperar o `boot_timeout`; depois de uma checagem validada,
é `network` (um soluço). Uma conta sem JetStream num servidor com ele responde
`10039`, fatal por si. Até 2026-10-11 o sink perguntava `AccountInfo` nesse caso
para "distinguir" os dois; não distinguia (o `jetstream.AccountInfo` traduz
"sem respondedores" para "não habilitado" ele mesmo), e o pedido a
`$JS.API.INFO`, negado a um papel de privilégio mínimo, virava um veredito de
permissão permanente (revisão do porte para o `go-webhook-fast-server`).

**`Health()`:** o estado da conexão (`CONNECTED` é saúde, `CLOSED` é fatal,
`RECONNECTING`/`DISCONNECTED` é falha de rede) e, em `jetstream`, um
`StreamInfo` (um `PING` responderia feliz com o JetStream desligado, e o estado
nunca sairia de UP); em `core`, um *flush*. Nos dois modos, depois, a validação
acima, em toda checagem.

| Erro | Classe |
|---|---|
| prazo ou cancelamento, `ErrTimeout`, `ErrNoResponders`, `ErrNoServers`, `ErrDisconnected`, `ErrConnectionReconnecting`, `ErrReconnectBufExceeded`, `ErrNoStreamResponse`, `ErrAsyncPublishTimeout`, `ErrTooManyStalledMsgs` | `network` |
| `10077` cuja descrição é de um stream **cheio** com `discard: new` ("maximum messages exceeded", "maximum bytes exceeded", "maximum messages per subject exceeded") ou **fechando** ("store is closed", o `ErrStoreClosed` de `server/store.go` do nats-server 2.14.6, num restart do NATS; só "store closed", texto que nenhum servidor manda, era conferido, e o 10077 de um store fechando era `fatal` — revisão independente do `go-syslog-server`, 2026-10-13, C2) | `network`: backpressure, passa quando um consumidor confirma ou o servidor volta (revisão independente do R, 2026-10-11; como `fatal` eram dump, exit 3 e duplicatas do pedaço já gravado na reingestão) |
| `10158` (409: o mesmo `Nats-Msg-Id` ainda numa proposta Raft, na retentativa depois de um prazo num stream em cluster), `10167` (429: a fila de entrada do stream cheia) e todo outro erro da API com código 429 | `network`: passam sozinhos (pendência 18 do S, corrigida no R; antes, "outro 4xx" os fazia FATAL, dump e exit 3, talvez na frota inteira) |
| `ErrAuthorization`, `ErrAuthExpired`, `ErrAuthRevoked`, `ErrAccountAuthExpired`, e o texto do servidor no `CONNECT` com "authorization violation", "authentication expired" ou "authentication revoked" (o `errors.Is` do `nats.go` compara o texto inteiro, e "User Authentication Expired" nunca casava com o sentinela: o boot esperava o `boot_timeout` e saía com o motivo errado; **nunca** "permissions violation", que é de um subject e vai pelo veredito), `ErrConnectionClosed` com a conexão `CLOSED` (com ela só reconectando, `network`: o `flushErr` acima), servidor que exige TLS (`errNATSServerRequiresTLS`), `ErrStreamNotFound` (`10059`), JetStream desligado (`10076`, `10039`), stream esperado não confere (`10060`), selado (`10109`), recursos (`10023`), outro armazenamento (`10077`: um registro que o store não aceita) | **`fatal`** |
| `ErrMaxPayload` (recusada no cliente: só aquela mensagem, §10.2), `ErrMaxBytesExceeded`, `10054`, `10097` | `logic` |
| violação de permissão (`ErrPermissionViolation`) numa publicação em `loghub.<space>.syslog` (de qualquer `space`), na API `$JS.API` ou na assinatura de `_INBOX.`, vista pelo `ErrorHandler` | **`fatal`**, pelo veredito (abaixo) |
| outro erro da API com código 4xx (um 409 desconhecido inclusive) | `fatal` |
| outro erro da API com código 5xx, qualquer outro | `network` |

**Veredito.** O cliente só informa algumas recusas definitivas de forma
**assíncrona** (pendência 14 do S, corrigida no SN e no R): uma publicação negada por permissão não volta como erro dela (em
`jetstream` o futuro estoura o prazo e seria retentado para sempre com o sink
UP; em `core` a mensagem some e contava como escrita), e um `$JS.API.STREAM.INFO`
negado segurava o boot até o `boot_timeout`. Por isso o sink guarda um
**veredito** quando o `ErrorHandler` vê `ErrPermissionViolation` numa
publicação em `loghub.<space>.syslog` (de qualquer `space`: um dump reingerido
publica no do seu), na `$JS.API` ou na assinatura de `_INBOX.` por onde voltam os
`PubAck` e as respostas da API (sem ela nenhuma confirmação chega, e o
`StreamInfo` estourava o prazo como `network` até o `boot_timeout`; pendência 7
do SN, corrigida no R), e o `Health()` e **todo
`Write`**, nos dois modos, o devolvem como **`fatal`**: dump e exit 3 (§13.3).
Em `core` o `Write` não espera o `ErrorHandler`: o servidor manda o `-ERR`
antes do `PONG` do *flush*, então o `LastError()` da conexão já o tem quando o
*flush* volta, sem corrida, e o pedaço que encontrou a negação volta `fatal` e
fica retido para o dump. Antes, esse pedaço era confirmado, contado como escrito
e perdido, e assim todos os seguintes até a próxima checagem de saúde. Uma
negação no subject do **beacon não é veredito**: o plano de controle não pode derrubar o plano de dados (§11). Em
`jetstream`, o prazo do futuro de um publish negado vira o veredito, com o
pedaço contaminado: o primeiro pedaço negado é fatal só depois do
`sink.op_timeout`; em `core`, na hora.

**Negação mascarada (`mode=core`).** O `nats.go` guarda só o **último** `-ERR`.
Uma negação de permissão do subject do beacon que chega entre os `PUB` de um
pedaço e o `PING` do *flush* esconde uma negação do caminho de dados que veio
antes dela, e o pedaço seria confirmado embora o servidor o tenha recusado. Uma
negação **nova** durante o pedaço que não é do caminho de dados torna o pedaço
incerto: `network`, não confirmado; a retentativa encontra o veredito que o
`ErrorHandler` já gravou, ou passa (revisão independente do R, 2026-10-11;
`TestNATSCoreMaskedDenialIsNotConfirmed`).

**Dump de outro `space` em `core`:** o envelope é publicado e gera um `WARN` por
`space` estrangeiro, uma vez (pendência 8 do SN, corrigida no R): sem stream,
ele se perde se ninguém assinar o subject, e o dump é removido como entregue. A
comparação é entre o `space` do item e o Workspace, sem montar o subject no
caminho quente.

O `ErrMaxBytesExceeded` só existe em consumidores *pull*: o ramo dele na
classificação é inalcançável aqui e fica por simetria com o `go-syslog-server`.

O `10097` (headers acima de 64 KiB) é inalcançável daqui, porque o bloco de
headers é fixo e pequeno (§3.8); é `logic` e não o `fatal` de um 4xx
desconhecido porque, se acontecesse, seria sobre aquela mensagem, não sobre o
stream. Erro desconhecido é `network`, nunca `logic` (§20.18): um ring que enche
é visível, um pedaço descartado não. Esta tabela e `TestClassifyNATS` mudam
juntas.

**Autenticação:** uma de `username`/`password`, `token` ou `credentials`
(arquivo `.creds`, JWT + semente NKey), e só uma: mais de uma é exit 1, assim
como `password` sem `username` e um *userinfo* na URL junto de qualquer uma
delas (o `nats.go` preferiria o da URL em silêncio). Um `credentials` que não existe é exit 1 ao
montar o sink (§13.1 passo 7): a biblioteca só leria o arquivo ao conectar, e o
caminho errado viraria um laço de retentativa do boot. **Sem TLS**: não há
`tls`, `tls_ca` nem `tls_insecure`.

**Fechamento:** `Close`, não `Drain`: os workers param antes de o sink fechar
(§13.4), então não há nada a esvaziar.

### 10.8 `sink.type = none`

Removido no go-relplog-nats-server (§1.2): todo pedaço vai ao NATS; não há destino de benchmark (o `BenchmarkRELPSession` mede a recepção sem sink, §18.8).

### 10.9 `LOGHUB_EB_URL`

Decisão 10 do S (herdada pelo R), cortada como no SN. `loghub.eb_url` (env `LOGHUB_EB_URL`) é a
URL do eventbus da convenção LogHub. Aqui o único eventbus aceito é o NATS
**sem TLS**: a URL preenche `sink.nats.urls`.

| Esquema | Efeito |
|---|---|
| `nats://` (uma lista separada por vírgula é o cluster do `nats.go`) | preenche `sink.nats.urls` com a URL inteira, usuário e senha (ou token) inclusive, que o cliente lê dali |
| `tls://` | **exit 1**, com uma mensagem que nomeia o TLS: este servidor não fala TLS com o NATS (§1.2); use `nats://` numa rede confiável |
| outro (`postgres://`, `redis://`, `rediss://`, `amqp://`...) | exit 1: não suportado. Uma URL de outro eventbus esquecida de uma implantação irmã (o `go-relplog-pg-server`, o R com `sink=redis`) derruba o boot em vez de ser ignorada |

Numa lista, **toda** entrada precisa ser `nats://`, com host e sem caminho,
consulta ou fragmento (as demais opções são as chaves de `[sink.nats]`): uma
entrada `tls://` no meio do cluster seria discada com um *handshake* que este
servidor não faz.

**Uma fonte só da verdade:** com `LOGHUB_EB_URL` preenchida, qualquer chave que a
URL **pode** expressar definida explicitamente (flag, env ou INI) é exit 1 com as
duas fontes no log, mesmo que esta URL não traga aquela parte (uma URL sem senha
quer dizer "sem senha", não "procure em outro lugar"). As chaves donas:
`sink.nats.urls`, `username`, `password` e `token`.

As demais (`credentials`, `mode`, `stream`) convivem com a URL. Consequência: a
senha ou o token vão **dentro** da URL. Nenhuma mensagem de erro cita a URL (ela
pode ter senha): no máximo o esquema, e só quando ele tem forma de esquema;
nem o trecho dela que o erro do `net/url` citaria (`invalid port ":Sup3r"` é a
senha até a primeira `/`, `#`, `?` ou `,`). **Um `@` depois do primeiro `/`, `?`
ou `#` da autoridade é recusado** (a regra do R): nenhuma URL `nats://` tem `@`
no caminho, na consulta ou no fragmento, então é um usuário ou senha com `/`,
`?` ou `#` sem codificar, que encurta a autoridade
(`nats://app:39182#Zsecret9@nats-1` discaria `app:39182` sem senha); a mensagem
pede `%2F`, `%3F` e `%23`. O
`--dump-config` mostra os valores derivados como comentário, com o *userinfo*
redigido (`nats://usuario:***@host`; um usuário sem senha pode ser um token e é
redigido inteiro), para que o dump relido não dispare o conflito.

### 10.10 `sink.type = clickhouse`

Removido no go-relplog-nats-server (§1.2).

#### 10.10.1 Cliente

Removido no go-relplog-nats-server (§10.10).

#### 10.10.2 Concorrência e conexões

Removido no go-relplog-nats-server (§10.10).

#### 10.10.3 Escrita

Removido no go-relplog-nats-server (§10.10).

#### 10.10.4 Desduplicação e at-least-once

Removido no go-relplog-nats-server (§10.10).

#### 10.10.5 Validação no boot

Removido no go-relplog-nats-server (§10.10).

#### 10.10.6 Durabilidade e `async_insert`: decisão do administrador do ClickHouse

Removido no go-relplog-nats-server (§10.10).

#### 10.10.7 Classificação de erro

Removido no go-relplog-nats-server (§10.10).

#### 10.10.8 `Health()`

Removido no go-relplog-nats-server (§10.10).

#### 10.10.9 Retenção

Removido no go-relplog-nats-server (§10.10).

#### 10.10.10 Métricas

Removido no go-relplog-nats-server (§10.10).

#### 10.10.11 Autenticação e TLS

Removido no go-relplog-nats-server (§10.10).

---

## 11. Plano de controle: heartbeat beacon

Decisão 8 do S (herdada pelo R) e 1 do SN: fica. O servidor é um driver de
si mesmo (não existe `go-loghub-driver`), então emite o heartbeat que a skill
exige de todo worker, e **só** ele. Como o único sink é o NATS, o beacon sai
**sempre**, nos dois modos.

- **Quando:** na primeira conexão, em toda reconexão (o `ReconnectHandler` do
  `nats.go`) e a cada `loghub.beacon_interval` (env `LOGHUB_BEACON_INTERVAL`;
  inteiro puro em segundos, como a skill o define, ou duração do Go; padrão
  `15`; `0` desliga o periódico, não o de conexão; acima de zero, no mínimo
  `1s`: um `1ns` girava o laço do beacon, e é exit 1, pendência 21 do S). O
  beacon impõe o piso de `1s` sozinho também, com `WARN` (`control.MinInterval`),
  para que nenhum caminho o transforme num laço.
- **A primeira conexão não perde o beacon.** O gancho é registrado no sink
  **antes** de o `WaitReady` conectar (§13.1 passo 7); o pedido fica guardado
  (um só: pedidos que chegam enquanto um beacon está saindo se fundem no
  próximo) e o beacon sai quando a goroutine começa, no passo 11 (§13.1). O gancho nunca
  bloqueia: roda na goroutine de *callback* do cliente.
- **Onde:** `<loghub.ctl_namespace>.beacon` (env `LOGHUB_CTL_NAMESPACE`, padrão
  `loghub._ctl`), publicado por NATS **core** (`nc.Publish`) na conexão do sink,
  em fanout, **nunca no JetStream**: o stream de dados não pode cobrir o plano de
  controle, e o boot recusa um que cubra (§10.7). O `ctl_namespace` precisa ser
  publicável: tokens separados por ponto, sem espaço, `*`, `>`, `/` ou `\`
  (exit 1, §13.2).
- **Envelope**, com o mesmo escritor dos envelopes de dados (§3.3): `uuid` novo
  de nível 3 e `event` da mesma amostra, `from` = AgentUUID, `space` =
  Workspace, `type = "beacon"`, `level = 9`, `sid` = PID, `step = 0`, e o
  payload, nesta ordem:

  | Chave | Valor |
  |---|---|
  | `name` | AgentName |
  | `hostname` | Hostname |
  | `machine` | MachineID (32 hex) |
  | `address` | o IP local da rota até o servidor NATS conectado (um `net.Dial("udp", servidor)` sem tráfego, lendo o `LocalAddr`); `127.0.0.1` se não der |
  | `uptime` | milissegundos de vida do **processo** |
  | `meta` | `[{"key":"version","value":<versão>},{"key":"ring","value":"memory"},{"key":"sink","value":"nats"},{"key":"transports","value":["tcp"]}]` |

- Falha de publicação (inclusive "não conectado") é contada
  (`relplog_beacons_total{result="error"}`) e logada em `WARN` com amostragem, um
  a cada 30 s; **nunca** afeta a saúde do sink. O beacon é uma cortesia ao plano
  de controle, e perder um não pode parar o plano de dados.
- A goroutine para na fase 5 do shutdown (§13.4), antes de o sink fechar.
- Não responde a sonar, não emite nem responde `ping`/`pong`, não assina nada:
  lacuna documentada até existir um driver (§21).

---

## 12. Arquivo de emergência

**Propriedade do ring, não do sink.** O ring é memória: sem o dump, o que ele
segura morre com o processo. Dispara quando o processo sai com o ring (ou um
lote retido pelos workers) não vazio:

- sink FATAL (§13.3), ou durante o dreno do shutdown (§13.4);
- `shutdown.drain_timeout` estourado.

### 12.1 Formato

`<emergency.dir>/emg-<agent_uuid>-<unixnano>.jsonl`, com `emergency.dir` padrão
`<data_dir>/emergency`. O carimbo de nanossegundos tem sempre dezenove dígitos
(de 2001 a 2286), então, para um mesmo agente, a ordem lexicográfica dos nomes
**é** a ordem temporal.

**JSON Lines: um envelope JLH pronto por linha** (decisão 5; a do SN, que a
herdou do `go-syslog-pg-server`; no R era o `.bin` de cabeçalho + cru):

```
{"uuid":"019a...","from":"0196...","space":"default","type":"syslog","level":6,"sid":59363...,"step":0,"event":{...},"payload":{...}}\n
{"uuid":"019a...", ...}\n
```

- **Cada linha é o JLH exatamente como o worker o teria escrito**: a conversão
  do dump é a do worker, com a config `[syslog]` congelada no boot (§3.5), então
  os bytes são idênticos. Um JLH nunca contém `\n` cru (§3.3), e por isso `\n` é
  um separador seguro.
- **Legível por qualquer ferramenta**: `jq`, um script, qualquer software LogHub,
  não só este servidor. É o ganho da escolha; o custo é converter **todo** o ring
  pendente na hora do dump, pago uma vez por um processo que está saindo.
- Malformadas ficam de fora quando `syslog.accept_malformed = false`: o worker
  as recusaria também (§6.6), e o dump não as ressuscita. Contam em
  `relplog_events_dropped_total{reason="malformed"}`. Se todas forem recusadas,
  nenhum arquivo é criado.
- **Escrito sob um nome temporário oculto** e só depois publicado:
  `.emg-<agent_uuid>-<unixnano>.jsonl.tmp` com `O_CREATE|O_EXCL`, escrita,
  `fsync` do arquivo, `link` para o nome final `emg-…jsonl` (o `link` recusa
  substituir um arquivo existente, onde um `rename` substituiria em silêncio),
  remoção do temporário, `fsync` do diretório. Escrito direto no nome final, um
  dump cortado numa fronteira de linha (um *flush* do buffer terminando em
  `\n`, e então `SIGKILL`, OOM ou queda de energia antes do `fsync`) seria lido
  de volta como **arquivo limpo**, entregue em parte e **removido**, com a cauda
  perdida sem contagem; e outra instância que dividisse o diretório poderia
  pegar um arquivo ainda sendo escrito.
- **Sem *hard link*, `rename`.** Um sistema de arquivos sem *hard link* (vfat,
  exFAT, as montagens FUSE de *object store*) recusa o `link` qualquer que seja
  o nome, e ali todo dump falhava: o ring inteiro perdido onde a escrita no
  lugar funcionava. Quando o `link` falha por outro motivo que não `EEXIST`, o
  nome final é conferido livre (`lstat`) e o temporário é **renomeado** para
  ele; o carimbo de nanossegundos no nome é o que torna inofensiva a janela
  entre os dois. Um `link` recusado porque o nome existe continua sendo falha,
  e o `rename` nunca acontece por cima de um nome que existe. O `fsync` do
  diretório vem depois, nos dois caminhos, com a mesma regra (abaixo): o
  `go-webhook-nats-server` o tornou melhor esforço; aqui ele continua decidindo
  o dump, de propósito (§12.3), como no SN.
- Qualquer falha — escrita, `fsync` do arquivo, `close`, `link` (e o `rename`
  que o substitui), `fsync` do diretório — remove o temporário (e o nome final, se este dump já o
  tinha criado) e é falha do dump (§12.3): sem o `fsync` do diretório a entrada pode não sobreviver a uma
  queda de energia, e o processo sairia dizendo que salvou. Um `.tmp` que sobra
  (o processo morreu no meio) **nunca é listado** nem reingerido: é evidência de
  um dump que não terminou, para o operador. No boot (§13.1 passo 9), um `.tmp`
  que é o **mesmo arquivo** que o dump do nome final é o gêmeo de um crash entre
  o `link` e a remoção, e é removido; qualquer outro é informado num `WARN` com
  os caminhos (porte do `go-webhook-nats-server`, 2026-10-11: antes nada dizia
  ao operador que ele existia). As mensagens dele só foram contadas como perda
  se um segundo sinal abortou o dump; depois de um `SIGKILL`, de um OOM ou de
  uma queda de energia, ninguém as contou, e o arquivo pode estar inteiro.
  Renomeado para o nome final (sem o ponto inicial e sem o `.tmp`), as linhas
  íntegras dele são reingeridas no boot seguinte, e uma última linha cortada
  vai para a quarentena (§12.2).

### 12.2 Reingestão

No boot, todo `emg-*.jsonl` de `emergency.dir` é publicado no NATS **antes** de
qualquer transporte abrir (§13.1 passo 9), do mais antigo para o mais novo
(ordem lexicográfica de nome), **qualquer que seja** o `emergency.enabled`
atual: só a **escrita** de dumps depende dele. Arquivos `.corrupt`, `.tmp` (um
dump que não terminou, §12.1) e qualquer outro nome são ignorados.

**Sem conversão.** A linha é publicada como está, em `Data`; o subject
(`loghub.<space>.syslog`), o `Nats-Msg-Id` (`<uuid>.0`) e os `X-LogHub-*` saem
do **cabeçalho da própria linha** (§3.8). `type` precisa ser `"syslog"` e
`step`, `0`. Consequência: um dump sobrevive a uma troca de chaves `[syslog]`
(ou de versão do binário) entre duas execuções sem mudar um byte (§3.5). Um
envelope já guardado pelo stream antes da queda volta com o mesmo
`Nats-Msg-Id` e, dentro da `duplicate_window`, é confirmado como duplicata em
vez de duplicar.

**`emergency.ParseLine` é a única porta entre um arquivo em disco e o stream**,
e recusa tudo o que este servidor não poderia ter escrito. O NATS não lê o
`Data`: uma linha que passasse sem ser um JLH deste servidor seria publicada,
confirmada pelo stream e entregue a todo consumidor como um envelope legítimo,
com subject e headers derivados de um cabeçalho que ninguém conferiu. A régua
é, então, **o que o escritor do JLH produz**, byte a byte:

- UTF-8 válido (o `encoding/json` trocaria um inválido por U+FFFD em silêncio, e
  os campos lidos descreveriam outra coisa que os bytes publicados);
- todo escape `\u` da linha em `\u0000`–`\u001f`, em minúsculas: os únicos que
  o escritor emite (§3.3). Isso exclui um *surrogate* solto, que cada
  decodificador JSON trata de um jeito (troca, recusa ou repassa);
- todo número um inteiro simples de no máximo 20 dígitos (sem `.`, `e` nem
  `+`): o único que o conversor escreve (prioridade, códigos, portas, tamanhos,
  `uts`, `sid`); um `1e1000000` seria lido de jeitos diferentes por cada
  consumidor;
- profundidade de aninhamento até 8 (o payload `syslog` tem quatro níveis);
- o cabeçalho (tudo antes de `"payload":`) **byte a byte igual** ao que
  `jlh.AppendHeader` produz para os valores lidos: sem chave duplicada (o
  `encoding/json` fica com a última; um consumidor em outra linguagem pode ficar
  com a primeira, e os dois leriam envelopes diferentes), sem espaço, ordem e
  grafia de números canônicas;
- exatamente as nove chaves do JLH, com chave desconhecida recusada e nenhuma
  faltando; nada depois do objeto. Só o **cabeçalho** é decodificado; o
  `payload` é cortado por posição e percorrido token a token, sem montar a
  árvore;
- `uuid` UUIDv7 em minúsculas, canônico: ele vira o `Nats-Msg-Id` e o
  `X-LogHub-UUID`, e uma grafia diferente da original escaparia da deduplicação
  do stream; `from` UUIDv7 em minúsculas (o AgentUUID do `go-loghub-ident`);
- `space` na gramática de §13.2: ele vira tokens do subject, e um `space` livre
  poderia trazer espaço, curinga ou token vazio para um subject que não é um;
- `type = "syslog"` e `step = 0`, que o sink fixa no `X-LogHub-Type` e no `.0`
  do `Nats-Msg-Id`; `level` 0–7 (uma severidade); `sid` ≤ 2^53 − 1;
- `event.uts` em 0..253402300799 e `event.sub` em 0..999999999; `event.iso`
  igual ao derivado de `uts`/`sub`;
- o instante do `uuid` (ms) igual ao de `event`: `uuid` e `event` são uma
  amostra só (§3.1);
- o `payload` cortado por posição (`jlh.Payload`, depois de `},"payload":`)
  igual ao `payload` que o parser JSON viu, e a linha terminando logo depois
  dele: quem corta o payload por posição vê o mesmo objeto que quem decodifica;
- `payload` um objeto cuja chave `message` de primeiro nível (a última, se
  repetida; comparação exata, sem caixa trocada) é uma string, que o contrato
  do `type=syslog` exige e o conversor sempre escreve (§3.2).

O que a porta **não** confere é o tamanho contra os tetos do NATS:
`emergency.MaxLine` é o pior JLH do **maior** `max_message_bytes` aceito, e uma
linha de uma execução com limite maior pode passar do `max_payload` (ou do
`max_msg_size` do stream) que o boot conferiu para a config atual. A publicação
dela é recusada **sozinha** como `logic` (`ErrMaxPayload` no cliente, `10054`
no stream; §10.2): as demais linhas do pedaço são publicadas, só ela conta em
`sink_logic`, e o arquivo segue a regra abaixo.

**Linhas e arquivos danificados:**

| Situação | Efeito |
|---|---|
| a última linha não termina em `\n` | cauda **truncada** (o dump foi cortado); as linhas íntegras antes dela são entregues |
| linha completa que `ParseLine` recusa | **inválida**: pulada; o `\n` localiza a próxima |
| linha maior que `emergency.MaxLine` | **inválida**: descartada durante a leitura, nunca guardada |
| arquivo com cauda truncada ou linha inválida | **danificado**: as linhas íntegras são entregues e o arquivo é renomeado para `.corrupt` (com `fsync` do diretório), com as contagens de bytes em `ERROR`; destruir a evidência de uma queda não é decisão deste programa |
| arquivo limpo | removido (com `fsync` do diretório) **só depois** de todos os seus envelopes serem confirmados. Uma falha da remoção é `ERROR`: o arquivo volta no boot seguinte e é republicado, e o stream só o absorve como duplicata dentro da `duplicate_window` (10 min no `stream.json`); depois dela, ou em `core`, os envelopes duplicam |

`emergency.MaxLine` = 12 × 16 MiB + 8 KiB: o pior JLH de §3.9 com o **maior**
`max_message_bytes` que a config aceita, e não o da execução atual, para que um
dump de uma execução com limite maior ainda volte depois de o operador baixá-lo.
**Nunca é alocado de antemão**: a linha cresce com o que é lido, e uma maior que
o teto é consumida sem ser guardada. O leitor é alvo de fuzz (`FuzzRead`, §18.3).

**O arquivo nunca é carregado inteiro.** As linhas são envelopes prontos, até
12× o cru que o ring segurava (§3.9): um ring cheio de 512 MiB de linhas cheias
de controle vira gigabytes de dump, e um boot que o carregasse morreria por OOM
no passo 9, com todos os transportes fechados, a cada restart (o arquivo só sai
depois de entregue). A reingestão **lê em fluxo**, um pedaço por vez
(`emergency.Store.Open` + `Reader.Next`, até `sink.batch` envelopes e
`sink.batch_max_bytes` de JLH, um JLH maior vindo sozinho): a memória fica em
um pedaço mais uma linha.

A entrega segue as classes dos workers pedaço a pedaço (§13.1 passo 9): `logic`
conta, loga e segue para o pedaço seguinte, e o arquivo é tratado como
entregue (sair deixaria o mesmo arquivo para o boot seguinte, num *crashloop*
com todos os transportes fechados); `network` reenvia o mesmo pedaço com backoff
dentro de **um** `sink.boot_timeout` **sem progresso** para a reingestão inteira
(todo pedaço entregue adia o prazo), e estourado sai com 3 **sem remover o
arquivo**; `fatal` sai com 3. Um prazo único desde o começo, o do SN, fazia uma
reingestão saudável de um dump grande, que passasse do `boot_timeout`, sair com
3 no primeiro soluço da rede, sem retentativa, e todo boot seguinte republicar o
mesmo arquivo desde o início (revisão independente, 2026-10-13,
`TestLongReingestionStillRetriesATransientFailure`; a portar para o SN). Um NATS
que não aceita nada continua limitado a um `boot_timeout`.

**Um dump de uma vida com outro Workspace** é publicado em
`loghub.<space da linha>.syslog`, que a validação do boot não conferiu (ela
prova só o subject do próprio `space`). Sem stream que o cubra, o JetStream não
responde e a entrega falha como `network` até `sink.boot_timeout`: o boot sai
com 3 e o arquivo fica. Coberto por outro stream, o `WithExpectStream` o recusa
(`10060`), fatal. O operador decide se cria o stream ou move o dump. Em `core`,
a mensagem vai para quem assinar aquele subject, ou para ninguém.

**Remoção e quarentena duráveis** (do R; pendências 24 e 10 do S e do SN).
Depois de entregue, o dump é removido e o diretório recebe `fsync`: sem ele, uma
queda de energia logo depois devolveria o nome, e o boot seguinte entregaria o
dump inteiro de novo. Um `fsync` que falha **depois** do `unlink` (ou um
diretório que nem abre para ele, EACCES, EMFILE) é `WARN`: o arquivo já saiu, e
`Store.Remove` o diz com `emergency.ErrNotDurable`, em vez do falso "cannot
remove ... it will be delivered again". Na quarentena, se o `rename` aconteceu e
só o `fsync` do diretório falhou, o arquivo **está** quarentenado: o `fsync` vai
em `WARN` e o `ERROR` com os bytes descartados sai igual, com o nome novo; só sem
nome devolvido é "cannot quarantine".

**Gauges recontados** (pendência 25 do S, pendência 9 do SN, corrigidas no R):
no começo da reingestão `relplog_emergency_files` recebe o número de arquivos
listados; ao fim, os dois gauges são recontados do diretório
(`emergency.Store.Count`, que lê cada arquivo em fluxo, uma linha por vez, e
conta as linhas que `ParseLine` aceita), nunca zerados: um dump que ficou (ilegível,
remoção ou quarentena que falharam) continua contado — o ilegível como arquivo,
sem mensagens —, e o alerta sobre eles continua disparando. Durante a
reingestão, `relplog_emergency_events_pending` não muda (no SN ele mostrava o
tamanho do pedaço em entrega, revisão independente de 2026-10-13). O dump escrito
no shutdown **soma** aos gauges em vez de fixá-los.

**A porta aceita qualquer `type=syslog`.** `ParseLine` confere o envelope, não
quem o escreveu: uma linha de um dump do SN (sem `x-transport`) ou do
`go-syslog-server` passa e é publicada como está, no subject do `space` dela. É
o mesmo JLH, do mesmo `type`, para o mesmo stream; recusar por `x-transport`
inventaria uma regra que o schema não tem (decisão 11).

### 12.3 Falha do próprio dump

Disco cheio, sem permissão, `fsync` recusado, `emergency.enabled = false`:
`ERROR` com a contagem exata, `relplog_events_dropped_total{reason="emergency_failed"} += n`,
exit 3. `emergency.dir` é criado **no boot**, com uma escrita de prova: um
diretório criado durante o incidente falha durante o incidente, e um problema
de permissão achado no dump é uma mensagem perdida em vez de um boot recusado.

---

## 13. Ciclo de vida

### 13.1 Boot

```
 1. config (§16): flags > env > INI > padrão. --help, --version e
    --dump-config saem aqui, sem tocar identidade nem disco (a seção
    informativa [identity] do dump mostra o que o AMBIENTE diz, sem
    resolver nada). LOGHUB_EB_URL é expandida e conferida aqui (§10.9);
    toda URL que não seja nats:// é recusada (§13.2).
    Erro ou combinação inválida => exit 1
 2. data_dir: resolvido para absoluto, criado (0750), exportado como DATADIR
 3. lhident.Initialize() — falha => o processo sai com 100–114, pela biblioteca
 4. Identity congelada; space e from validados (§13.2) => exit 1 se inválidos
 5. porta ADMIN aberta: /healthz 200, /readyz 503 com o motivo, /metrics
 6. emergency.dir criado e provado gravável (com emergency.enabled; a
    reingestão do passo 9 lê o diretório de qualquer forma)
 7. SINK: cliente NATS preguiçoso (configuração inválida, como um .creds
    inexistente => exit 1); o gancho do beacon registrado ANTES da primeira
    conexão; WaitReady com retentativa até sink.boot_timeout (300 s)
    => exit 3; em jetstream, validação do stream (cobre
    loghub.<space>.syslog, nunca cobre <ctl_namespace>.beacon, não selado,
    no_ack false) e da maior mensagem contra max_payload, max_msg_size e o
    teto de registro do file store (§3.9, §10.7); em core, só o
    max_payload: recusa => exit 3 IMEDIATO, com o log "the sink refused
    this server permanently" (e não "did not become usable within
    sink.boot_timeout"); o mesmo para um servidor que exige TLS ou que
    recusa as credenciais
 8. RING memory, workers (ainda parados) e o pipeline de recepção montados
 9. antes, o temporário gêmeo de um dump é removido e o de um dump
    interrompido vira um WARN com o caminho, intocado (§12.1);
    reingere emg-*.jsonl (§12.2), sem conversão, EM FLUXO (nunca o
    arquivo inteiro na memória) e PEDAÇO A PEDAÇO, com um único
    sink.boot_timeout SEM PROGRESSO para tudo (sink.Instrumented.Deliver): `logic` descarta, conta e loga aquele
    pedaço e segue para o próximo; `network` reenvia o MESMO pedaço com
    backoff dentro desse prazo; `fatal` sai com 3; um sinal sai
    com 130. Depois: inicia a saúde e os workers
10. (não existe aqui: o dreno do backlog do ring=sqlite do R; o ring em
    memória começa vazio. A numeração fica a do R, para o código e a SPEC
    citarem o mesmo passo)
11. beacon: a goroutine começa, e o beacon da primeira conexão (pedido no
    passo 7) sai agora (§11)
12. abre o transporte: tcp (RELP)
13. /readyz 200
```

**Identidade antes de tudo que escreve.** `lhident.Initialize()` roda **uma
vez**, no `main`, depois das flags que encerram o programa e antes de abrir
qualquer recurso com `defer` ou criar goroutine (regra 2 da skill). A app recebe
um struct `Identity` (`From`, `Space`, `AgentName`, `Hostname`, `MachineID`)
preenchido dos getters: os testes montam a app dezenas de vezes no mesmo
processo, e uma segunda chamada a `Initialize()` sai com 112. `AgentName` e
`Hostname` compõem o nome do cliente NATS, `rlnats/<AgentName>@<Hostname>`
(§10.7), e o payload do beacon (§11).

**`DATADIR` é exportado antes do `Initialize()`** para que a biblioteca grave
`agent_uuid` e `machine_id` na mesma raiz em que o servidor grava `emergency/`
(§16.6). Sem `DATADIR` absoluto, o padrão `./data` vira absoluto a partir do
diretório de trabalho; no contêiner é `/data`.

**A porta admin abre antes do sink:** num crashloop por NATS indisponível ou
stream ausente, o operador tem `/readyz` com o motivo e `/metrics`.

**O transporte abre por último.** Abrir o RELP antes de o sink estar pronto e de
os dumps terem voltado seguraria toda transação até o `backpressure_timeout` e
devolveria todas; um transporte que não está escutando faz o `omrelp` esperar do
lado dele, que é o certo. Um sinal que cancela o boot enquanto a porta admin ou
o transporte resolvem um nome de host (`localhost:9103`) sai com 130, não com o
1 de "endereço inválido" (revisão independente, 2026-10-13).

### 13.2 Validação de configuração

| Combinação | Efeito |
|---|---|
| Nenhum `listen.*` | exit 1 |
| `listen.tcp` vazio (o único transporte) ou fora de `host:port` (`:20514` serve); `tcp_listeners` fora de 1–256 | exit 1 |
| `relp.window` fora de 1–65536 | exit 1 |
| `relp.max_frame_bytes` < `syslog.max_message_bytes` ou > 999 999 999 | exit 1 (o fusível fecharia sessões em linhas que o truncamento guardaria; nove dígitos de `DATALEN`) |
| `relp.inflight_max_bytes` < `syslog.max_message_bytes` | exit 1 (uma transação precisa sempre caber) |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `proxy.protocol ≠ off` sem `proxy.trusted` válido | exit 1 |
| `space` fora de `^[a-z0-9-]+(\.[a-z0-9-]+)*$` | exit 1 (a lib aceita `a..b`, `.x` e `x.`; segmento vazio não serve à convenção LogHub, e viraria um token vazio no subject). Acima de 64 caracteres a própria lib já sai com 111 antes |
| `from` fora do UUID canônico em minúsculas | exit 1 |
| `syslog.default_priority` fora de 0–191 | exit 1 |
| `syslog.rfc3164_timezone` desconhecido para o `time.LoadLocation`, ou `Local` | exit 1 (a imagem embute `time/tzdata`; `Local` dependeria do host) |
| `syslog.legacy_charset` fora de `""`, `cp1252`, `latin1`, `windows-1252`, `iso-8859-1` | exit 1 |
| `syslog.max_message_bytes` < 480 ou > 16 MiB; `sd_max_elements` ou `sd_max_params` < 1 | exit 1 (480 é o mínimo da RFC 5424) |
| `ring.capacity_items`, `ring.max_bytes`, `sink.workers`, `sink.batch`, `sink.batch_max_bytes`, `sink.max_retries` abaixo de 1; `ring.high_water` fora de (0, 1]; `log.access_sample` fora de [0, 1]; `tcp.max_conns*` negativo; `retry_backoff_max < retry_backoff_min` | exit 1 |
| **tetos** (revisão independente do RP, 2026-10-13, portada): `ring.capacity_items` > 16 777 216; `sink.batch` > 65 536; `sink.batch_max_bytes` > 256 MiB; `sink.workers` > 256; `listen.tcp_listeners` > 256 | exit 1, nomeando a chave. Sem eles um erro de digitação de poucos dígitos derrubava o boot com pânico (`makechan` de `capacity_items`) ou alocava um gigabyte por `Dequeue` (`sink.batch`). Os dois do NATS são deste servidor (`docs/DECISOES.md` §2.2): todos os workers publicam na **uma** conexão NATS (um mutex, um socket), e o cliente recebe `2 × workers × batch` como `PublishAsyncMaxPending` (§10.7) — nos tetos, 33 554 432 futuros, um `int` em qualquer plataforma; e o NATS não tem limite por pedaço (cada envelope é uma mensagem, sob `max_payload`, que o `nats-server` aceita até 64 MiB), então `batch_max_bytes` só limita a memória de um pedaço segurado inteiro enquanto os futuros dele estão pendentes. 256 MiB é o número da família (o RP o tira do limite de 1 GiB da libpq) |
| `sink.nats.urls` vazia, ou com uma entrada que não seja `nats://` (`tls://`, `ws://`, `wss://`, `host:porta` sem esquema) | exit 1, citando só o esquema, nunca a URL (§1.2). Uma entrada **seguinte** sem esquema é quase sempre uma `,` sem escape numa senha que partiu a lista (`nats://u:12,cret@h`), e a mensagem diz isso, com a dica de codificar (revisão independente, 2026-10-13) |
| uma entrada de `sink.nats.urls` que não é URL válida (um `%`, `/`, `#`, `?` ou `,` sem escape num usuário, senha ou token) ou sem host | exit 1, sem repetir **nada** da entrada — nem o trecho que o erro do `net/url` cita: `invalid port ":Sup3r" after host` é a senha até a primeira `/`, e ia para o stderr do contêiner — com a dica "percent-encode %, /, #, ? and , in a user, password or token". Uma entrada com caminho, consulta ou fragmento também é exit 1 (a regra da `LOGHUB_EB_URL`, §10.9): é o que pega o segredo que parseia — `nats://tok/en@host` é o host `tok` e um caminho, sem credencial nenhuma, e o erro de discagem do cliente citava esse host, o token até a barra, em cada log até o `boot_timeout`. Antes da revisão, a falha acontecia dentro do `nats.Connect`, como `network`, retentada até o `boot_timeout` e com a URL (e a senha) citada em cada linha de log |
| *userinfo* numa entrada de `sink.nats.urls` junto com `sink.nats.username`, `password`, `token` ou `credentials` | exit 1: o `nats.go` prefere em silêncio o da URL (a `LOGHUB_EB_URL` já recusava esse conflito, §10.9) |
| `loghub.beacon_interval` acima de zero e abaixo de `1s` | exit 1 (§11) |
| `sink.nats.mode = jetstream` com `sink.nats.stream` que não é nome de stream (vazio, espaço, ponto, curinga, barra) | exit 1 |
| mais de um de `credentials`, `token` e `username`; `password` sem `username` | exit 1 |
| `sink.nats.credentials` que não existe | exit 1 (ao montar o sink, §13.1 passo 7) |
| `loghub.ctl_namespace` vazio ou com um token não publicável | exit 1 (§11) |
| `sink.nats.mode = core` | `WARN`: *best effort*, sucesso é "o servidor recebeu", não "alguém guardou" (§10.7) |
| `LOGHUB_EB_URL` em conflito, com esquema que não é `nats://` (`tls://` com a mensagem do TLS), com entrada sem host, com caminho, consulta ou fragmento (§10.9) | exit 1 |
| `LOGHUB_EB_URL` só com brancos; host codificado em porcento (`nats://u:p@%41h:4222`, também em `sink.nats.urls`) | exit 1 (revisão independente do RP, 2026-10-13, portada): só brancos é "definida, com nada visível", e quem quis desligá-la não saberia que são as chaves `[sink.nats]` que valem; o host em porcento caía em "invalid percent-escape", que mandava o operador procurar na senha. Vazia continua sendo "sem URL" |
| `--help`, `--version` ou `--dump-config` com `=valor`; `--config=` vazio | exit 1, sem o valor (revisão independente do RP, 2026-10-13, portada): `--dump-config=false` despejava e saía com 0, e um `--config=` vazio caía no `RELPLOG_CONFIG`, o contrário do que diz |
| flag com o valor colado ao nome, sem o `=` (`--sink.nats.passwordhunter2`, também com `-`, `_` ou `.` no meio, e o mesmo no INI) | exit 1 **sem citar o nome**: ele começa com o nome de um parâmetro e segue, e o resto é o segredo (revisão independente do RP, 2026-10-13, portada) |
| durações zero onde zero não é "sem limite" (§16.5) | exit 1 |
| `GOMEMLIMIT` ausente, ou `< ring.max_bytes × 2` | `WARN` (§15.4) |
| parâmetro desconhecido na linha de comando ou no INI | exit 1. Isto inclui toda chave do R que saiu (`ring.mode`, `sink.type`, `listen.tls`, `[tls]`, `[filter]`, `[ratelimit]`, `sink.nats.subjects`, `sink.nats.tls`, `tls_ca`, `tls_insecure`, `[sink.postgres]`...): um INI do `go-relplog-server` não sobe aqui em silêncio. Uma chave do R é nomeada com "go-relplog-server has it, go-relplog-nats-server does not", **também** quando vem depois de uma flag com valor (antes, a regra da cauda partida abaixo a tomava pela cauda do valor anterior e culpava a flag errada; revisão independente, 2026-10-13). No INI, uma chave desconhecida só é nomeada quando a seção é deste servidor (ou do R), a chave tem forma de chave (`^[a-z0-9_]+$`) e a linha anterior não era um segredo: um token ou uma senha que quebrou para a linha seguinte (`token = eyJhbGciOiJI` e então `UzI1NiJ9abcDEF==`) vira uma "chave", e o erro, que no `SIGHUP` vai para o log, imprimia o resto do segredo (herdado do R e do SN; a portar). Uma marca de ordem de bytes UTF-8 no começo do INI é ignorada (antes, a primeira linha virava "expected 'key = value'" sem nada visível que dissesse por quê) |
| linha do INI que não é `[seção]` nem `chave = valor` (um cabeçalho sem `]` inclusive); argumento da linha de comando que não começa com `-` | exit 1, nomeando o número da linha ou só o que vem antes do `=` do argumento, **nunca o conteúdo**: `password: x` (sem `=`) ou `sink.nats.password=x` (sem `--`) iam inteiros para o stderr do contêiner |
| flag sem valor seguida de outra flag (`--sink.workers --sink.nats.password=x`, um `$WORKERS` vazio num script); valor depois de um espaço (`--sink.nats.password= x`, `--sink.nats.password = x`, `--sink.nats.password= -x`); valor que o shell partiu num espaço (`--sink.nats.password=$P`, com espaço em `$P`); valor que começa com `--` na forma separada (`--sink.nats.password --x`) | exit 1, nunca com o valor: um argumento que começa com `-` nunca é tomado como valor de outra flag, e só é nomeado se for uma flag deste servidor escrita com **dois** traços (`-h` depois de `--sink.nats.password` é o que uma senha de dois caracteres parece, e não é nomeado; revisão independente do RP, 2026-10-13, portada); o argumento depois de um valor vazio ou de um `=` sozinho não é citado, nem como nome de flag desconhecida; uma palavra solta depois de qualquer valor não é citada. Um valor que começa com `-` vai depois do `=`. Fica uma brecha, de propósito: a cauda partida que começa com traço depois de um valor **não** vazio (`--sink.nats.password=a -b`) sai como parâmetro desconhecido, porque escondê-la esconderia todo erro de digitação de uma flag (`--sink.workers=4 --tipo.errado=1`) (revisão independente do porte, 2026-10-11; os três últimos caminhos, da revisão do porte de volta ao `go-webhook-nats-server`, no mesmo dia) |
| variável de ambiente do R que este servidor não tem (`RELPLOG_LISTEN_TLS`, `RELPLOG_TLS_*`, `RELPLOG_FILTER_*`, `RELPLOG_RATELIMIT_*`, `RELPLOG_SINK_TYPE`, `RELPLOG_RING_MODE`, `RELPLOG_SINK_NATS_SUBJECTS`, `RELPLOG_SINK_NATS_TLS*`, as dos outros sinks e rings: as 92 do `--help` do R que não estão no daqui) | **um** `WARN` que nomeia todas e diz que são ignoradas (`internal/config/removed.go`, §16.2). Não é erro porque um mesmo `.env` pode alimentar mais de um servidor da família; sem o aviso, o env é lido só pelos nomes do registry e elas sumiriam em silêncio, e quem trocou um servidor pelo outro acreditaria ter um listener TLS, um filtro ou subjects granulares |

### 13.3 Saúde do sink

Máquina de estados dirigida **só pelo `Health()`**:

```
UP ---- Health() falha (prazo sink.health_timeout) ----> DOWN
DOWN --- Health() sucesso -----------------------------> UP
DOWN --- persiste por sink.fail_timeout (30 s)
         OU sink.max_retries (10) checagens falhas ----> FATAL
qualquer --- Write() devolve ClassFatal ----------------> FATAL
qualquer --- Health() devolve erro fatal --------------> FATAL (sem fusível)
```

- `Health()` a cada `sink.health_interval` (2 s): a detecção leva até um
  intervalo.
- **Resposta fatal à sonda** (credenciais recusadas, servidor que exige TLS,
  conexão `CLOSED`, stream apagado, JetStream desligado, o veredito de uma
  permissão negada, um stream editado que já não cumpre o contrato ou um
  `max_payload` que já não cabe a maior mensagem, §10.7): FATAL na primeira
  checagem, sem passar
  pelos fusíveis. Esperar não conserta nenhuma delas.
- **FATAL é terminal.** Toda transição e o gauge `relplog_sink_up` são escritos
  sob o mesmo lock, e nada sai de FATAL: uma checagem concorrente não pode
  devolver o estado a UP/DOWN depois de um `ClassFatal` do `Write()`.
- `ClassNetwork` no `Write()` **não** muda o estado; o pedaço é retentado.
- Em **DOWN**, as transações RELP são seguradas mesmo com o ring vazio
  (fail-fast, §5.7), e o `/readyz` falha. O log de DOWN diz isso: "incoming RELP
  transactions are held until it recovers" (o R ainda tem o texto do S, "refused
  or dropped per transport", que não vale para o RELP; a portar).
- Em **FATAL**: §12 e exit 3.
- O beacon (§11) não participa: uma falha dele nunca muda o estado.
- **Sem prazo de vida da checagem** (`withLifetime` do R): lá ele limitava uma
  sonda de banco que podia ficar presa além do `health_timeout`; aqui a sonda
  NATS valida dentro da própria checagem, com o prazo dela, e o `Health` nunca
  bloqueia além dele. Não se aplica.

### 13.4 Shutdown gracioso

`SIGTERM` ou `SIGINT`:

```
fase 1  /readyz passa a falhar
fase 2  espera shutdown.pre_stop_delay (0 s), ainda aceitando
fase 3  cancela o contexto de recepção e fecha o transporte (todos os
        listeners de uma vez, um closeGrace no total): as transações
        decididas recebem a resposta, as seguradas ficam sem ela, sai
        serverclose e cada sessão fecha (§5.8)
fase 4  drena o ring para o sink, com shutdown.drain_timeout (30 s); depois
        para os workers e recolhe Workers.Pending() e o Drain() do ring
fase 5  o que sobrou vai para o dump (§12); para o beacon, fecha o ring e o
        sink; exit 0 (ou 3, abaixo)
```

- O contexto de recepção é cancelado **antes** de o transporte fechar: é o que
  solta as transações presas no laço de backpressure, sem resposta (§5.8). Um
  item já entregue ao ring não é afetado.
- **Nenhum `Enqueue` depois do `Drain`.** O `Close` do transporte só volta quando
  as sessões voltaram (com todas as transações decididas ou abandonadas e as
  respostas entregues ao socket; depois de `closeGrace`, 6 s, fecha o socket de
  quem não voltou). Por isso o `Drain` da fase 4 vê tudo o que entrou no ring: um
  item que entrasse depois dele ficaria num ring que ninguém mais lê, sem
  entrega, sem dump e sem contador.
- `drain_timeout` estourado com itens: §12 e exit 3.
- **Sink FATAL durante a fase 4** (o `SIGTERM` chegou com o sink UP e ele virou
  FATAL no dreno): a espera termina na hora, em vez de gastar o `drain_timeout`
  num dreno que não acontece mais; o que sobrou vai para o dump, e a saída é
  **3**.
- **Shutdown por FATAL** (§13.3) começa sem sinal: o **primeiro**
  `SIGTERM`/`SIGINT` que chega nele (um `docker stop`, um deploy) é logado e
  **tolerado**, e o shutdown segue até o dump e o exit 3; o segundo aborta com
  130; `SIGQUIT` nunca é tolerado (pendências 28 do S e 11 do SN).
- **Segundo sinal** em qualquer fase: saída imediata, `exit 130`, aceitando a
  perda, contada em `reason="abort"`. `SIGQUIT` aborta sempre, inclusive fora do
  shutdown: no boot e com o servidor no ar, saída imediata com 130.
- **A posse da contagem é de um lado só.** A fase 4 grava a ocupação do ring num
  contador e liga a "fase de dump" **antes** do `Drain`: dali em diante a
  ocupação do ring não mede mais nada (os lotes retidos ainda contam lá embora
  vão para o dump; os drenados já não contam). O aborto e o `dumpEmergency`
  (dump falho, desligado ou sem diretório) tomam o contador com uma troca
  atômica (`Swap(0)`): exatamente um deles conta aquelas mensagens (revisão
  independente do R, 2026-10-11). **As passagens do contador são ordenadas contra
  o aborto** por um mutex (`enterDumpPhase`, `oweDump`, `abortLoss`): um aborto
  que caía entre as duas escritas da entrada na fase de dump contava o ring e
  deixava o contador para um dump falho contar de novo, e um que caía durante o
  `Pending`/`Drain` era desfeito pela escrita seguinte, `Store(len(leftover))`:
  10 contadas perdidas para 5 (revisão independente, 2026-10-13,
  `TestAbortDuringTheDrainIsCountedOnce`; o R tem o mesmo código, a portar).
- **O dump toma o contador de volta sob o mesmo mutex** (`settleDump`,
  revisão independente do `go-syslog-server`, 2026-10-13, A2): o aborto
  publica `aborted` antes de tomar o contador, e um dump que via a marca
  nesse meio — desistia (`ErrAbandoned`) ou, desligado, nada tinha a
  escrever — tomava o contador primeiro: `emergency_failed = n` e
  `abort = 0`, a perda contada uma vez mas atribuída ao disco. Sob o
  `lossMu`, ou o aborto terminou (o dump acha 0) ou nem começou (o dump
  conta, o aborto acha 0) (`TestAbortInsideTheDumpKeepsItsReason`).
- **Um dump contado como perdido não volta.** O aborto marca o processo como
  abortado, e o `Store.Write` em andamento confere essa marca no começo, a cada
  1024 linhas e antes de publicar o arquivo: desiste e remove o temporário. Antes, o
  `shutdownWith` devolvia 130 enquanto a goroutine do shutdown seguia
  escrevendo; com os `defer` do `run` correndo antes do `os.Exit`, um dump
  pequeno chegava ao nome final e era reingerido no boot seguinte, depois de
  contado e logado como perdido (revisão independente, 2026-10-13). Fica uma
  janela entre a última conferência e o `link`: um dump publicado nela é contado
  **e** guardado, uma contagem a mais, nunca uma perda; nesse caso o
  `dumpEmergency` não conta mais nada — nem as malformadas que o dump deixou de
  fora, que o aborto já contou, nem os gauges —, só um `WARN` (revisão
  independente do RP, 2026-10-13, portada,
  `TestAbortAfterTheLastAbandonCheckCountsNothingMore`). O aborto durante o
  recolhimento das sobras é provado com o shutdown real, por um gancho de
  teste entre a entrada na fase de dump e o `Pending`/`Drain`
  (`TestAbortDuringTheCollectionCountsOnceAndDumpsNothing`,
  `TestAbortDuringTheCollectionPublishesNoDump`).
- **`SIGQUIT` tardio**: um `SIGQUIT` que chega depois de o shutdown começar e
  antes de ele registrar o canal de aborto só alcança o observador do boot, que
  marca a saída imediata e fecha um canal. O shutdown confere a marca na entrada
  **e** espera esse canal junto com os sinais: a conferência sozinha (a do R)
  perdia o `SIGQUIT` cujo observador gravava a marca microssegundos depois dela
  (revisão independente, 2026-10-13, `TestSIGQUITAfterTheEntryCheckStillAborts`;
  a portar para o R).
- **`SIGHUP` vale o processo inteiro.** É capturado desde a primeira linha; fora
  da janela entre o fim do boot (passo 13) e o início do shutdown, é logado em
  `WARN` e ignorado. A ação padrão do Go encerraria o processo com 129, sem log,
  no meio de um boot que pode durar o `boot_timeout`.
- O observador do segundo sinal é registrado **antes** de o contexto de sinais
  soltar `SIGTERM`/`SIGINT`: no intervalo, um segundo sinal teria a ação padrão
  e mataria o processo com 143.
- `shutdown.pre_stop_delay` existe para Kubernetes (5–10 s sugeridos): dá tempo
  ao balanceador de tirar a instância antes de as sessões fecharem. O
  `stop_grace_period` do orquestrador precisa ser **maior** que
  `pre_stop_delay + drain_timeout + closeGrace`: menor significa ser morto no
  meio do dreno, perdendo mensagens que já foram confirmadas por RELP.

### 13.5 Códigos de saída

| Código | Significado |
|---|---|
| `0` | shutdown completo, ring vazio |
| `1` | erro de configuração ou de inicialização não relacionada ao NATS (inclui toda URL que não seja `nats://`, a `LOGHUB_EB_URL` com `tls://` e um `.creds` inexistente) |
| `3` | NATS indisponível ou inutilizável: sink FATAL (inclusive um servidor que exige TLS), `boot_timeout` ou `drain_timeout` estourados, validação de §10.7 recusada, dump que falhou |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` (`DATADIR`, `MACHINE_ID`, `MACHINE_ID_FILE`, `AGENT_NAME`, `AGENT_UUID`, `HOSTNAME`, `WORKSPACE`; 112 = `Initialize()` duas vezes); a tabela completa está na skill `go-loghub-ident` e em `docker/README.md` |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 5–12 do boot. Um sinal que cancela o *bind* da porta admin (passo 5) ou do transporte (passo 12) é 130, não 1: com um nome em `admin.bind` ou `listen.tcp` a resolução falha com "operation was canceled", que não é endereço ruim (revisão independente de 2026-10-13; `TestCancelledBindIsAnAbortNotAConfigError`, e `TestSignalBeforeTheBindExits130`, portado do RP, para o transporte). Com IP literal em `listen.tcp` nenhuma resolução falha e o *bind* passa: o sinal é conferido depois do *bind* e antes do primeiro *accept*, e também é 130 (antes o servidor abria, o `/readyz` dava 200 e o shutdown saía 0; revisão independente do `go-syslog-server`, 2026-10-13, A1; `TestSignalBeforeALiteralBindExits130`) |

---

## 14. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9103`, **o mesmo do `go-relplog-server` e do
`go-relplog-pg-server`**: os três são da mesma família e um operador pode trocar
um pelo outro sem mexer no monitoramento — a regra da família, como o
`go-syslog-server`, o `go-syslog-pg-server` e o `go-syslog-nats-server`
dividem a `9102`. Decisão mantida (2026-10-13, no porte da revisão do RP). Por
isso **não rode dois deles no mesmo host sem trocar o `admin.bind`** (o
`go-webhook-server` usa `9101`). HTTP/1.1.

| Path | Semântica |
|---|---|
| `/healthz` | `200` enquanto o processo vive. Liveness. |
| `/readyz` | `200` só se: sink `UP`, ring abaixo de `ring.high_water`, boot concluído (inclusive a reingestão), transporte aberto e não em shutdown. Senão `503` com o motivo em `text/plain`. |
| `/metrics` | exposição Prometheus em texto. |

### 14.1 Métricas

Prefixo `relplog_`, **o mesmo do R**: um painel do `go-relplog-server` lê este
servidor. **Nenhum label deriva de conteúdo controlado pelo emissor** (nada de
hostname, appname, IP); todo label tem conjunto finito conhecido em compilação.
`facility` e `severity` **não** são labels: a distribuição por severidade é
pergunta para quem consome o stream.

| Métrica | Tipo | Labels |
|---|---|---|
| `relplog_messages_total` | counter | `transport` (tcp): frames `syslog` lidos depois do `open` (um `syslog` antes dele recebe `500 command disabled` e não conta) |
| `relplog_messages_accepted_total` | counter | `transport` |
| `relplog_messages_dropped_total` | counter | `reason` (empty, unaddressable): só descartes **finais**; o que o RELP devolve ao emissor não está aqui (§5.4) |
| `relplog_messages_truncated_total` | counter | `transport` |
| `relplog_message_bytes` | histogram | — (tamanho original) |
| `relplog_convert_total` | counter | `format` (rfc5424, rfc3164, malformed) |
| `relplog_convert_warnings_total` | counter | `code` (os 14 de §6.9) |
| `relplog_convert_duration_seconds` | histogram | — |
| `relplog_conns_active`, `relplog_conns_total` | gauge, counter | `transport` (tcp) |
| `relplog_conns_refused_total` | counter | `transport`, `reason` (global, per_ip, proxy); `global` conta cada vez que o accept estaciona no teto, um limite inferior, porque o teto global bloqueia em vez de recusar; `proxy` conta as conexões fechadas pelas regras de §4.6 |
| `relplog_proxy_headers_total` | counter | `result` (v1, v2, local, absent, invalid) |
| `relplog_backpressure_seconds_total` | counter | `transport` (tcp) |
| `relplog_confirm_duration_seconds` | histogram | — (do frame completo ao `200 OK` decidido; só aceitas) |
| `relplog_relp_frames_total` | counter | `command` (open, syslog, close, unknown) |
| `relplog_relp_responses_total` | counter | `code` (200, 500); `500` é final: o `librelp` descarta a mensagem |
| `relplog_relp_unanswered_total` | counter | `reason` (backpressure, shutdown, conn_lost, server_closed): transações lidas e devolvidas ao emissor, que as retransmite (§5.4) |
| `relplog_relp_frame_errors_total` | counter | `reason` (malformed, too_large) |
| `relplog_relp_inflight_bytes`, `relplog_relp_inflight_transactions` | gauge | — (o teto de §5.6 e a ocupação das janelas) |
| `relplog_relp_inflight_waits_total` | counter | — (paradas de leitura por teto em voo esgotado) |
| `relplog_ring_items`, `relplog_ring_bytes` | gauge | — |
| `relplog_ring_capacity_items`, `relplog_ring_capacity_bytes` | gauge | — |
| `relplog_sink_batch_size`, `relplog_sink_batch_bytes` | histogram | — (por pedaço) |
| `relplog_sink_duration_seconds` | histogram | — |
| `relplog_sink_health_duration_seconds` | histogram | — |
| `relplog_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); o cancelamento do shutdown não conta |
| `relplog_sink_up` | gauge | — |
| `relplog_sink_nats_duplicates_total` | counter | — (confirmações com `PubAck.Duplicate`: uma redelivery dentro da `duplicate_window`, §10.2) |
| `relplog_sink_nats_reconnects_total` | counter | — |
| `relplog_beacons_total` | counter | `result` (ok, error) (§11) |
| `relplog_events_written_total` | counter | — |
| `relplog_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero, como todo label de conjunto fechado (um `rate()` precisa da série antes do primeiro evento) |
| `relplog_emergency_files`, `relplog_emergency_events_pending` | gauge | — (o que está no disco, §12.2) |
| `relplog_build_info` | gauge | `version`, `commit`, `go` |
| `relplog_config_info` | gauge | `ring_mode` (sempre `memory`), `sink_type` (sempre `nats`), `transports` (`tcp`) |
| `relplog_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

Saíram, com o que as produzia: `reason` `filter` (de `messages_dropped`); o
label `tls` em `transport`; `relplog_tls_handshake_errors_total`;
`relplog_ring_quarantined_total` e `relplog_ring_redis_claimed_total`; as
séries de `postgres`, `sqlite`, `mysql`, `redis` e `clickhouse`;
`relplog_ratelimit_buckets` e `relplog_ratelimit_held_total`. Uma série que
nunca pode subir não é exposta: diria ao operador que existe uma medição onde
não existe nenhuma (a regra do SN; o R expunha toda série em zero para um painel
servir a todo destino, e aqui há um destino só).

**Fronteiras de histograma** são estáticas, idênticas em toda instância e
**pré-formatadas como string** na inicialização: emitir `0.01` num scrape e
`0.010` no seguinte cria duas séries e quebra todo quantil.

### 14.2 Logs

`slog`, `json` por padrão (`text` disponível). **Nenhum log síncrono por mensagem
no caminho padrão.** `log.access = true` liga log por mensagem com
`log.access_sample` (0.01); em syslog isso é logar log. Avisos de anomalia
(conversão, PROXY, moldura RELP, falha de aceite, beacon) são **amostrados**, um
a cada 30 s por tipo, com o contador exato em `/metrics`: um emissor quebrado não
inunda o log do coletor. Nenhum log em `INFO` ou acima carrega o payload de uma
mensagem.

---

## 15. Memória e runtime

### 15.1 Linha crua

Buffers de pools por classe: 2 KiB, 8 KiB e 64 KiB. Acima de 64 KiB (só quando o
operador sobe `max_message_bytes`) o buffer é alocado direto e **nunca** volta a
um pool: pooling de pior caso faz o pool segurar o pior caso para sempre. A
mediana de uma mensagem syslog fica em 150–250 bytes e o p99 raramente passa de
2 KiB.

O buffer volta ao pool **na mesma caixa `*[]byte`** de onde saiu
(`item.CopyRawBox`, `item.NewBoxed`): o item guarda a caixa, e devolvê-la não
aloca uma nova. Uma mensagem aceita custa assim **uma** alocação (o próprio
`Item`, 224 B) em vez de duas (248 B).

**Teto pequeno demais.** Cada item custa no mínimo 2 KiB (a menor classe) +
256 B na contabilidade de §9.2. Um `ring.max_bytes` abaixo disso é aceito pela
validação e nenhuma mensagem cabe: toda transação é segurada, a sessão fecha no
`backpressure_timeout`, e o rsyslog reconecta para sempre. Herdado do R e do SN,
conhecido e não corrigido (§22.1).

### 15.2 Regra de propriedade única

**O `Item` é dono do seu cru. Ele volta ao pool exatamente uma vez, e só depois da
confirmação do sink.** Num retry o item **ainda é o dono** e nada é devolvido.
Devolver cedo põe a linha de um emissor no evento de outro: silencioso, invisível
sob teste leve, o pior bug que este programa pode ter. A marca de "já devolvido"
é atômica.

**Caminhos de erro.** Um erro não significa que ninguém mais lê o buffer:

| Erro do `Enqueue` | Quem ainda pode ler | A recepção |
|---|---|---|
| `ErrFull`, `ErrClosed` | ninguém: o ring recusou antes de receber | devolve (`Release`) |
| qualquer outro (o ring em memória não tem; a regra fica para o caso de aparecer) | não há como provar que ninguém | **descarta sem devolver** (`Discard`) |

Nos workers: uma tentativa que termina por prazo ou cancelamento **contamina** o
pedaço (`Taint`), e nem o cru dos itens nem o buffer do pedaço voltam ao pool,
nem quando a retentativa seguinte tem sucesso: um futuro do JetStream ainda
pendente aponta para o buffer do pedaço (o `Data` da mensagem). Em `jetstream`,
qualquer erro depois da primeira publicação assíncrona também contamina, pela
mesma razão. O custo é uma alocação por
item no caminho de falha.
### 15.3 Memória do JLH, que não é do ring

O JLH vive entre o passo 2 e o passo 4 de §10.1, num buffer de pedaço. O teto é
`sink.workers × sink.batch_max_bytes` (8 × 16 MiB = 128 MiB no pior caso), mais
o buffer de escrita do cliente NATS. Não é somado a `ring.max_bytes`: memórias
com donos e ciclos de vida diferentes, e somá-las esconderia o estouro de uma
atrás da folga da outra. Os buffers de pedaço têm pool próprio, de uma classe (o
teto), e um buffer que cresceu acima dela não volta. O dump (§12.1) converte
item a item num buffer de linha só, reaproveitado, e a reingestão lê o dump em
fluxo, um pedaço por vez (§12.2).
### 15.3a Memória da recepção RELP, que também não é do ring

Duas parcelas, as duas com teto:

- **Cópias das transações em voo** (§5.6): no máximo `relp.inflight_max_bytes`
  (64 MiB) para o processo inteiro, contadas pela capacidade real do buffer.
- **Custo fixo por conexão** (§5.2): cerca de 10 KiB (leitor de 4 KiB, escritor
  de 4 KiB, o primeiro bloco do frame) mais as goroutines do leitor e do
  escritor; um frame grande não deixa um buffer grande para trás. Com
  `tcp.max_conns` em zero (padrão), esse termo cresce com o número de conexões; quem espera milhares de emissores fixa o teto.

A conta de memória do processo é `ring.max_bytes + sink.workers ×
sink.batch_max_bytes + relp.inflight_max_bytes + conexões × ~10 KiB`.
### 15.4 Runtime

- `GOMEMLIMIT` deve ser configurado: `WARN` se ausente ou abaixo de
  `ring.max_bytes × 2`. O compose define `GOMEMLIMIT` **e** o limite do
  contêiner, o segundo acima.
- `GOGC` e `GOMAXPROCS` no padrão (o Go 1.25 respeita o limite de CPU do cgroup).
- `time/tzdata` é embutido no binário: o fuso de §6.5.2 não depende da imagem.

---

## 16. Configuração

### 16.1 Precedência

```
argv  >  env  >  arquivo INI  >  padrão
```

- Flag: `--<seção>.<chave>` (`--ring.capacity_items=131072`).
- Env: `RELPLOG_<SEÇÃO>_<CHAVE>` em maiúsculas, `.` vira `_`
  (`RELPLOG_SINK_NATS_URLS`), com as exceções de §16.2.
- INI: seção `[ring]`, chave `capacity_items`.
- `--config=/caminho.ini` (env `RELPLOG_CONFIG`) indica o arquivo.

**Os mesmos nomes do R.** O prefixo `RELPLOG_` e cada chave que ficou têm o
nome que tinham no `go-relplog-server` (tipo, padrão, recarga, redação e nome de
env conferidos um a um na revisão): os dois são da mesma família, e um operador
pode trocar um pelo outro mantendo o env das chaves comuns.

### 16.2 Registry único, e os nomes de env fixados pelas skills

Uma **tabela** (`internal/config/registry.go`) declara cada parâmetro (seção,
chave, tipo, padrão, descrição, se é recarregável, nome de env). Dela saem o
INI, as flags, o env, o `--help`, o `--dump-config` e a validação. Um parâmetro
novo entra só ali. Sem `gopkg.in/ini.v1` e sem `spf13/pflag`.

Nomes de env que **não** seguem a regra, porque uma skill os fixou:

| Seção | Env | Origem |
|---|---|---|
| `[syslog]` | `SYSLOG_<CHAVE>` (sem repetir a seção, e **com** o prefixo da skill, não o deste servidor: um `.env` comum garante config de conversão idêntica entre este, o R e o `go-syslog-server`): `SYSLOG_DEFAULT_PRIORITY`, `SYSLOG_RFC3164_TIMEZONE`, `SYSLOG_MAX_MESSAGE_BYTES`, `SYSLOG_PAYLOAD_RAW`, `SYSLOG_LEGACY_CHARSET`, `SYSLOG_ACCEPT_MALFORMED`, `SYSLOG_SD_MAX_ELEMENTS`, `SYSLOG_SD_MAX_PARAMS` | skill `loghub-type-syslog-schema` §15 |
| `[loghub]` | `LOGHUB_EB_URL`, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE` | skill `loghub-json-schema` |
| `server.data_dir` | `DATADIR` | `go-loghub-ident` |

As variáveis de identidade (`AGENT_UUID`, `AGENT_NAME`, `WORKSPACE`, `HOSTNAME`,
`MACHINE_ID`, `MACHINE_ID_FILE`, `LOGHUB_IDENT_DEBUG`) **não** passam pelo
registry: são lidas pelo `go-loghub-ident`, e o `--dump-config` mostra o que o
**ambiente** diz numa seção informativa `[identity]`, sem resolver nada
(§13.1 passo 1: o dump não toca identidade nem disco).

**Chaves do R que saíram.** Na linha de comando ou no INI são parâmetro
desconhecido, exit 1, e a mensagem diz que o `go-relplog-server` as tem. No
ambiente, onde o registry lê só os próprios nomes e nunca varre o resto, elas
sumiriam em silêncio; por isso o boot confere a lista de
`internal/config/removed.go` (as 92 variáveis do `--help` do R que não estão no
daqui: TLS, filtro, rate-limit, `sink.type`, `ring.mode`, `ring.max_inflight`,
`sink.nats.subjects`, `sink.nats.tls*` e as dos outros sinks e rings) e emite
**um** `WARN` que nomeia todas as definidas e diz que são ignoradas (§13.2).

**Erros de configuração não ecoam a entrada** (pendência 30 do S, corrigida no
R e no SN): nenhum erro cita texto que possa ser segredo — URL que não parseia
(só o tipo do erro e a dica de codificar `%`, `/`, `#`, `?` e `,`), linha INI
sem `=` ou cabeçalho malformado (só o número da linha), chave INI desconhecida
só com a forma de chave e fora da linha seguinte a um segredo (§13.2), nome de
parâmetro desconhecido citado só se tiver forma de nome, argumento solto citado
só até o `=`. Nas flags, um argumento com forma de flag nunca é valor de outra
flag e só é nomeado se for flag deste servidor (ou do R); nada depois de um
valor (cauda partida pelo shell, valor depois de `= `) é citado. Os erros vão
para o stderr e, num `SIGHUP`, para o log.

**Um `@` depois do primeiro `/`, `?` ou `#` da autoridade de uma URL é
recusado** (a regra do R), em `sink.nats.urls` e na `LOGHUB_EB_URL`, sem citar a
URL (§10.9).

Durações aceitam a sintaxe do Go (`30s`, `5ms`). Um inteiro puro, em segundos,
só em `loghub.beacon_interval`, porque é assim que a skill define
`LOGHUB_BEACON_INTERVAL`. Tamanhos aceitam
**só** `KiB`, `MiB`, `GiB` (sem diferença de caixa) e inteiros puros em bytes;
`KB`, `MB`, `K` e `M`, que a standard aceitava, são recusados.
`syslog.rfc3164_timezone = Local` é recusado: faria hosts converterem de formas
diferentes (§3.5); vazio vale `UTC`.

### 16.3 Recarga por `SIGHUP`

Recarrega **só** o que é seguro trocar a quente e não muda os bytes do JLH:

- `log.level`, `log.access`, `log.access_sample`

(No R entravam também `filter.*`, `ratelimit.*` e `sink.sqlite.keep_files`,
que saíram, §1.2.)

Toda chave de `[syslog]` **exige restart**: ela muda a conversão, e o JLH de um
item não pode depender de **quando** ele foi drenado (§3.5). A identidade fica
congelada.

Duas referências: chave recarregável comparada com o que está **em execução**
(para que reverter ao valor de boot seja aplicado); chave de restart comparada
com o **boot** (para que todo `SIGHUP` avise sobre ela até o restart). Cada uma
das duas sozinha falha num dos casos (bugs reais 009 e 010 da standard).
**Sinônimos são um valor só**: `windows-1252` e `cp1252`, `iso-8859-1` e
`latin1` em `syslog.legacy_charset` não são mudança (a aresta herdada do SN,
corrigida na revisão independente do RP de 2026-10-13 e portada;
`TestCharsetSynonymsAreOneValue`).

**Chave sombreada.** Como env e flag vencem o arquivo (§16.1) e não mudam num
processo em execução, uma chave que o arquivo define mas um env ou uma flag
decide **nunca** muda pelo arquivo: a recarregável não recarrega, e a de restart
não muda nem com o restart. Todo `SIGHUP` loga cada uma em `WARN`, com a origem
que manda (`env RELPLOG_LOG_LEVEL`), mesmo quando o resultado é "configuration
unchanged": é exatamente o caso em que o operador editou o arquivo e não entende
por que nada mudou. Por isso o compose não define chaves recarregáveis por env:
elas vêm do INI montado (`docker/conf/relplog.ini`).

Um arquivo inválido no `SIGHUP` não aplica nada: `ERROR`, e o servidor segue com
a config anterior. Fora da janela entre o fim do boot e o início do shutdown, o
`SIGHUP` é logado e ignorado (§13.4).

### 16.4 Exemplo

```ini
[server]
data_dir = /data

[listen]
tcp = :20514

[relp]
window               = 128
inflight_max_bytes   = 64MiB
backpressure_timeout = 30s

[syslog]
rfc3164_timezone  = America/Sao_Paulo
max_message_bytes = 64KiB

[ring]
capacity_items = 65536
max_bytes      = 256MiB

[sink]
workers = 8

[sink.nats]
urls        = nats://nats-1.intranet:4222,nats://nats-2.intranet:4222
credentials = /etc/relplog/relplog.creds
mode        = jetstream
stream      = LOGHUB_SYSLOG

[loghub]
beacon_interval = 15

[admin]
bind = 0.0.0.0:9103

[log]
level = info
```

Usuário e senha (ou o token), no lugar do `.creds`, vão por env
(`RELPLOG_SINK_NATS_PASSWORD`, `RELPLOG_SINK_NATS_TOKEN`) ou dentro da
`LOGHUB_EB_URL` — nesse caso sem `urls`, `username`, `password` e `token` no INI
(§10.9).

### 16.5 Tabela de parâmetros

A fonte é `internal/config/registry.go`; o `--help` imprime a mesma tabela.

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `shutdown.drain_timeout`,
`relp.backpressure_timeout` e `proxy.header_timeout` — a validação recusa `0` com
exit 1. Zero é legítimo onde diz o que faz: `shutdown.pre_stop_delay` (sem
espera), `sink.batch_max_delay` (sem espera), `sink.fail_timeout` (sem fusível
por tempo), `tcp.idle_timeout` (sem limite) e `loghub.beacon_interval` (sem
beacon periódico; o de conexão continua, §11).

#### `[server]`

| Chave | Padrão | Descrição |
|---|---|---|
| `data_dir` | `./data` (env `DATADIR`) | Raiz de tudo o que o serviço grava, inclusive a identidade; convertida em absoluto; `/data` no contêiner |

#### `[listen]` — vazio desliga o transporte

| Chave | Padrão | Descrição |
|---|---|---|
| `tcp` | `:20514` | RELP em claro (§4.4); vazio desliga o único transporte, e é exit 1 |
| `tcp_listeners` | `1` | 1–256; `>1` habilita `SO_REUSEPORT` |

Não há `tls` nem `tls_listeners` (§1.2, §4.5).

#### `[relp]` — o protocolo

| Chave | Padrão | Descrição |
|---|---|---|
| `window` | `128` | transações em voo por conexão antes de o leitor parar de ler (§5.5); 1–65536 |
| `max_frame_bytes` | `16MiB` | fusível do `DATALEN`; acima dele a conexão fecha antes de ler o corpo (§5.2); ≥ `syslog.max_message_bytes` e ≤ 999 999 999 (os nove dígitos do `DATALEN`) |
| `inflight_max_bytes` | `64MiB` | teto **do processo** para as cópias das transações em voo, fora de `ring.max_bytes` (§5.6); ≥ `syslog.max_message_bytes` |
| `backpressure_timeout` | `30s` | quanto uma transação é segurada com ring cheio ou sink DOWN antes de a sessão fechar **sem** resposta (§5.7); zero é recusado; mantenha abaixo do `timeout` do `omrelp` (90 s) |

#### `[tcp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `idle_timeout` | `120s` | sessão sem frame por esse tempo fecha com `serverclose`; `0` = nunca |
| `max_conns` | `0` | teto do transporte, somando os listeners; bloqueia no accept; `0` = ilimitado |
| `max_conns_per_ip` | `0` | pelo endereço exato do par TCP (antes do PROXY); fecha na hora; `0` = ilimitado |

#### `[tls]`

Removido no go-relplog-nats-server (§1.2).

#### `[proxy]`

| Chave | Padrão | Descrição |
|---|---|---|
| `protocol` | `off` | `off` \| `optional` \| `required` (§4.6) |
| `trusted` | `127.0.0.1/32,::1/128` | CIDRs dos balanceadores; largo demais permite forjar `src-ip` |
| `header_timeout` | `5s` | prazo do cabeçalho PROXY |

#### `[syslog]` — exige restart (§16.3)

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `default_priority` | `SYSLOG_DEFAULT_PRIORITY` | `13` | PRI de `pri-missing`/`pri-invalid`, 0–191 |
| `rfc3164_timezone` | `SYSLOG_RFC3164_TIMEZONE` | `UTC` | fuso IANA dos timestamps sem fuso; `Local` é recusado |
| `max_message_bytes` | `SYSLOG_MAX_MESSAGE_BYTES` | `64KiB` | truncamento (§5.3); 480 a 16 MiB; pior JLH = 12 × isto + 8 KiB; também o tamanho máximo da cópia de uma transação em voo |
| `payload_raw` | `SYSLOG_PAYLOAD_RAW` | `true` | `raw` quando não é obrigatório |
| `legacy_charset` | `SYSLOG_LEGACY_CHARSET` | `""` | `cp1252` \| `latin1`, sinônimos `windows-1252` \| `iso-8859-1` (§6.7) |
| `accept_malformed` | `SYSLOG_ACCEPT_MALFORMED` | `true` | `false` descarta `malformed` no worker e no dump |
| `sd_max_elements` | `SYSLOG_SD_MAX_ELEMENTS` | `32` | §6.11 |
| `sd_max_params` | `SYSLOG_SD_MAX_PARAMS` | `64` | §6.11 |

#### `[filter]` — recarregável

Removido no go-relplog-nats-server (§1.2).

#### `[ratelimit]` — recarregável

Removido no go-relplog-nats-server (§1.2).

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `capacity_items` | `65536` | teto de itens, 1–16 777 216; com mensagens de 200 bytes é o que pega primeiro |
| `max_bytes` | `512MiB` | teto de bytes: o `cap` de cada buffer cru + 256 B por item (§9.2); o JLH não conta |
| `high_water` | `0.80` | fração de qualquer teto acima da qual o `/readyz` falha |

Não há `mode` (sempre `memory`) nem `max_inflight` (era do `ring=none`).

#### `[ring.sqlite]`

Removido no go-relplog-nats-server (§1.2).

#### `[ring.redis]`

Removido no go-relplog-nats-server (§1.2).

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `workers` | `8` | goroutines de dreno, que também convertem; dividem a única conexão NATS; 1–256 (teto deste servidor, §13.2) |
| `batch` | `256` | itens por `Dequeue`; as publicações assíncronas de um pedaço ficam no ar juntas; 1–65 536 |
| `batch_max_bytes` | `16MiB` | teto de JLH por pedaço; até 256 MiB (o NATS não limita o pedaço, só cada mensagem, por `max_payload`; o teto só limita a memória, §13.2) |
| `batch_max_delay` | `5ms` | espera máxima para completar um lote; `0` = sem espera |
| `op_timeout` | `5s` | prazo de uma escrita (as confirmações do pedaço), da discagem e de um futuro assíncrono |
| `health_interval` | `2s` | período da checagem; também a latência de UP→DOWN |
| `health_timeout` | `1s` | prazo de uma checagem |
| `fail_timeout` | `30s` | DOWN antes de FATAL; `0` = sem fusível por tempo |
| `max_retries` | `10` | checagens falhas antes de FATAL |
| `boot_timeout` | `300s` | quanto o boot espera o NATS (e o stream) antes de sair com 3; também o prazo sem progresso da reingestão (§12.2) |
| `retry_backoff_min`, `retry_backoff_max` | `100ms`, `2s` | backoff de um pedaço que falhou |

Não há `type` (sempre `nats`).

#### `[sink.nats]`

Não há `subjects` (o subject é sempre `loghub.<space>.syslog`, §3.8) nem chaves de
TLS (`tls`, `tls_ca`, `tls_insecure`): o cliente é sempre texto claro (§1.2).

| Chave | Padrão | Descrição |
|---|---|---|
| `urls` | `nats://127.0.0.1:4222` | lista separada por vírgula (um cluster); só `nats://`, com host e sem caminho, consulta ou fragmento; `%`, `/`, `#`, `?` e `,` de um usuário, senha ou token escapados; *userinfo* na URL exclui as chaves de autenticação abaixo; redigido no `--dump-config` |
| `username`, `password` | `""` | autenticação por senha; a senha é redigida |
| `token` | `""` | autenticação por token; redigido |
| `credentials` | `""` | caminho de um `.creds` (JWT + semente NKey); um método de autenticação só |
| `mode` | `jetstream` | `jetstream` (`PubAck` por mensagem) \| `core` (*best effort*, `WARN` no boot) |
| `stream` | `LOGHUB_SYSLOG` | o stream que precisa cobrir o subject; validado no boot, nunca criado; ignorado em `core` |

#### `[sink.redis]`

Removido no go-relplog-nats-server (§1.2).

#### `[sink.sqlite]`

Removido no go-relplog-nats-server (§1.2).

#### `[sink.postgres]` e `[sink.mysql]`

Removido no go-relplog-nats-server (§1.2).

#### `[sink.clickhouse]`

Removido no go-relplog-nats-server (§1.2).

#### `[loghub]`

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `eb_url` | `LOGHUB_EB_URL` | `""` | só `nats://` (lista com vírgula = cluster); `tls://` é exit 1; preenche `sink.nats.urls`; redigida no dump (§10.9) |
| `beacon_interval` | `LOGHUB_BEACON_INTERVAL` | `15` | período do beacon; inteiro puro em segundos, ou duração do Go; `0` desliga o periódico, não o de conexão; fora o `0`, no mínimo `1s` (§11) |
| `ctl_namespace` | `LOGHUB_CTL_NAMESPACE` | `loghub._ctl` | prefixo do plano de controle; o beacon vai para `<ctl_namespace>.beacon`, por NATS core |

#### `[emergency]`, `[admin]`, `[log]`, `[shutdown]`

| Chave | Padrão | Descrição |
|---|---|---|
| `emergency.enabled` | `true` | escreve o dump JSON Lines (§12); a reingestão independe dele |
| `emergency.dir` | `<data_dir>/emergency` | criado e provado gravável no boot; derivado do `data_dir`, o `--dump-config` o escreve comentado, para o dump relido derivá-lo de novo |
| `admin.enabled` | `true` | serve `/healthz`, `/readyz` e `/metrics` |
| `admin.bind` | `127.0.0.1:9103` | a mesma porta do `go-relplog-server` e do `go-relplog-pg-server` (a regra da família, como S, SP e SN na 9102): dois deles no mesmo host precisam de valores distintos (§14) |
| `log.level` | `info` | `debug` \| `info` \| `warn` \| `error` (recarregável) |
| `log.format` | `json` | `json` \| `text` |
| `log.access`, `log.access_sample` | `false`, `0.01` | log por mensagem, amostrado (recarregáveis) |
| `shutdown.pre_stop_delay` | `0s` | continua recebendo depois do `SIGTERM`; `0` = sem espera |
| `shutdown.drain_timeout` | `30s` | prazo do dreno do ring no shutdown |

### 16.6 Escrita em disco: uma raiz só

Tudo o que o serviço grava — a identidade (`agent_uuid`, `machine_id`, gravados
pelo `go-loghub-ident`) e `emergency/` — fica sob `server.data_dir`, e no
contêiner essa raiz é `/data` (o compose sobe com `read_only: true`). Não há
exceção: o servidor não tem socket local (§4.3).

Caminhos fixos do contêiner: `/app` guarda o binário (dono `root`), `/data` é a
única área gravável, `/etc/relplog` é o INI em `:ro` (o compose monta
`docker/conf/` ali e define `RELPLOG_CONFIG=/etc/relplog/relplog.ini`, que traz as
chaves recarregáveis, §16.3), `/tmp` é tmpfs. `WORKDIR /data`; o binário é
invocado por `/app/go-relplog-nats-server`. Não há `/etc/relplog/tls`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

---

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-relplog-nats-server/         ESTE módulo (RN)
  go-relplog-server/              o pai RELP (R): dois eixos, TLS, filtro, rate-limit
  go-syslog-nats-server/          o pai do molde (SN): o corte "só NATS" do go-syslog-server
  go-relplog-pg-server/           o irmão (RP): o mesmo corte do R, no molde do go-syslog-pg-server
  go-syslog-server/               o avô (S): de onde vieram conversão, rings, sinks, beacon, dump
  go-syslog-standard-server/      a standard: o RELP de referência
  go-syslog-pg-server/, go-relplog-*, go-webhook-*/   irmãos
```

Módulo independente. Nada é importado de outro módulo do monorepo, nem dos pais
nem do irmão: compartilhar pacotes faria os módulos evoluírem amarrados. Tags
`go-relplog-nats-server/vX.Y.Z`.

Os nomes `go-relplog-nats-server` (binário, imagem Docker, `--version`, tags) e
o prefixo `rlnats-` dos projetos, contêineres e volumes Compose são **deste**
módulo. O R usa `rls-`, o SN `slnats-` e o RP `rlpg-`: com a mesma imagem e o
mesmo nome de projeto, um `docker compose up` feito na pasta de outro
retaguearia a imagem daqui e assumiria os contêineres e os volumes desta pilha,
sem aviso.

**O que é cópia de quem.** `receiver/` é do R (o RELP, sem o TLS e sem o filtro
e o rate-limit da transação); `ingest.go` dentro dele é o do SN com os
desfechos do RELP. `ring/`, `sink/` (`nats`, `workers`, `chunk`, `health`,
`instrumented`), `control/`, `emergency/` e o grosso de `app/` são do SN, com as
correções do R portadas (§22); `config/` é o do R cortado como o SN cortou o do
S; `convert/`, `item/`, `jlh/`, `jlhcheck/`, `ident/` e `obs/` são comuns aos
dois, com o `x-transport` do R. Uma correção feita num dos lados vale para o
outro: confira os pais.

### 17.2 Layout interno

```
cmd/go-relplog-nats-server/main.go  flags que encerram, lhident.Initialize(), chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão, admin;
              XTransport
  config/     registry único -> INI + flags + env + --help + dump; validação;
              LOGHUB_EB_URL (só nats://); as variáveis e chaves do R que saíram
              (removed.go)
  control/    o heartbeat beacon (§11), e só ele
  ident/      Identity (congelada no boot) e a gramática de space          [lógica pura]
  jlh/        escritor JSON por append, Event, cabeçalho do envelope, UUID,
              Payload (o corte por posição)                                [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH (+ x-transport): pri,
              formato, rfc5424, rfc3164, sd, texto, charset, nomes, sid     [lógica pura]
  item/       Item, Head, pools de buffer (sem codec)                       [lógica pura]
  jlhcheck/   SÓ TESTE: os schemas e os vetores copiados das skills e o
              oráculo de conformidade (§18.2); nenhum arquivo de produção o importa
  receiver/   RELP: moldura (relpframe), sessão, janela, teto em voo e
              escritor de respostas (relp, budget), conexões e shutdown
              (stream), tcp, proxy, limites, ingest
  ring/       interface Ring + memory
  sink/       interface Sink + nats, workers, chunk, health, instrumented
  emergency/  dump JSON Lines, ParseLine e o leitor em fluxo da reingestão
  obs/        métricas Prometheus próprias + slog
```

`convert/`, o leitor de frames RELP e o leitor do PROXY em `receiver/` e
`emergency.ParseLine` são os parsers de entrada adversária. Os alvos de fuzz
estão em §18.3.

### 17.3 Dependências

**Três diretas em produção, e ponto.** As mesmas do SN.

| Dependência | Para quê |
|---|---|
| `github.com/nats-io/nats.go` v1.53.1 | o sink, com o pacote `jetstream`, e o beacon por NATS core |
| `github.com/patrickbrandao/go-loghub-uuidv7` v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` v0.5.2 | identidade (sem dependência nova: usa a `go-loghub-uuidv7`) |

Transitivas que o `nats.go` põe no binário (`go list -deps
./cmd/go-relplog-nats-server`): `github.com/nats-io/nkeys` v0.4.15 (NKey e
`.creds`), `github.com/nats-io/nuid` v1.0.1, `github.com/klauspost/compress`
v1.18.5 (a compressão do transporte websocket do cliente, compilada mesmo com
`ws://` recusado) e `golang.org/x/crypto` v0.49.0 (ed25519/nacl do `nkeys`).
`golang.org/x/sys` e `golang.org/x/text` aparecem no `go.mod` como indiretas do
grafo de módulos e **não** entram no binário.

Só em teste: `github.com/santhosh-tekuri/jsonschema/v6` (contratos, §18.2).

O R tinha oito; saíram `rueidis`, `modernc.org/sqlite`, `pgx`,
`go-sql-driver/mysql` e `clickhouse-go/v2`, com as transitivas de cada um. O RELP
não tem biblioteca: é stdlib, e o leitor é fuzzado contra um oráculo. O binário
tem **12,6 MB** (12 570 818 bytes), contra 31,9 MB do R e 12,6 MB do SN (o mesmo
`go build` simples, `darwin/arm64`, Go 1.27.1, 2026-10-13): o RELP custa menos
que o UDP, o unix e as duas molduras que o SN tem e este não.

**Dependência nova exige justificativa nesta seção.** O objetivo do módulo é ter
poucas (§1.1).

**Gerador de UUID:** um `uuidv7.NewGenerator()` por processo, criado no boot e
compartilhado (é seguro para concorrência e usa ChaCha8 com semente de
`crypto/rand`). Um `init()` falha alto se `crypto/rand` não responder. O `uuid`
não é segredo (expõe o instante), e nada deriva capacidade dele.

**O conversor é próprio**, como na standard: as bibliotecas de syslog falham em
mensagem fora da gramática (a regra aqui é nunca falhar), alocam string por campo
e trariam uma superfície de entrada adversária que não fuzzamos.

**Sem CGO.** `CGO_ENABLED=0` é premissa do Dockerfile.

### 17.4 Convenções

- Código, identificadores e comentários em **inglês**; documentação e conversa
  em **português**. Não misture dentro de um arquivo. As únicas strings em
  português no código são os textos normativos de `parse.error` (§6.6), que são
  dado, não mensagem.
- Comentários explicam o **porquê** e o que daria errado com a alternativa
  rejeitada. Código copiado dos pais vem **com** os comentários.
- Citações da spec por número de seção (`§6.4.3`), com a numeração do R
  (cabeçalho deste arquivo).
- Config: o registry é a fonte única. Métricas: cardinalidade fixa.

---

## 18. Testes

### 18.1 Unitários

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**: os bugs de perda de dados vivem em caminhos
concorrentes. Cobertura real com `-coverpkg=./internal/...`. Os pacotes cujos
testes precisam de identidade recebem `ident.Identity` montada à mão; nenhum
pacote chama `lhident.Initialize()` em teste. O caminho real da biblioteca (os
códigos 100–114, `DATADIR`) é provado só pelo binário, rodado como subprocesso
(`internal/app/binary_test.go`), porque o `Initialize()` chama `os.Exit` e só
pode rodar uma vez por processo.

### 18.2 Vetores dourados e contratos (desvio D3)

`internal/jlhcheck/loghub/` (o pacote `jlhcheck`, importado **só** por arquivos
`_test.go`, para que o `jsonschema/v6` nunca entre no binário) guarda cópias de:
`jlh-envelope.schema.json`, `jlh-syslog-payload.schema.json`, os exemplos
`valid/` e `invalid/` do envelope e `vectors.json`. Cada cópia registra no topo
de um `README.md` a skill e a data de origem.

- **Vetores:** para cada vetor, o conversor recebe o cru (com a moldura aplicada
  quando `stream`), os metadados de recepção e a config do vetor, e produz
  envelopes **iguais** aos esperados: todo campo igual, e o `uuid` igual pelo
  **instante** (ms, µs e ns do nível 3). **23 de 24** entram; `arquivo-sem-pri`
  fica fora porque `transport=file` está fora da v1 (§1.2), e o teste diz isso
  pelo nome. **O vetor com `transport=tls` roda reetiquetado como TCP**, como no
  SN: os mesmos bytes, com `payload.tls` retirado do esperado, `transport =
  "tcp"` e o `sid` recalculado pela fórmula de §6.10. Os vetores de UDP e unix
  rodam como estão: provam o conversor, que o enum do item ainda serve (§3.4).
- **Ordem de chaves:** o JLH serializado tem os nove campos e o payload na ordem
  de §3.1 e §3.2.
- **Schemas:** todo envelope produzido nos testes (vetores, corpus, fuzz,
  integração, linhas do dump) passa nos dois schemas, com
  `santhosh-tekuri/jsonschema/v6` e `AssertFormat` ligado (os formatos
  `ipv4`/`ipv6`). Os exemplos `valid/` passam e os `invalid/` falham, para
  provar que o validador está ligado.
- **Regras semânticas** (a porta em Go do `validate_payload.py`):
  `priority = facility.code × 8 + severity.code`; nomes RFC 5427; `level =
  severity.code`; `sid` recalculado de `payload.host`; `event.iso` = `uts` + `sub`
  truncado; instante do `uuid` = `event` (tempo real); `size ≥` octetos de `raw`
  (maior só com `truncated`); `raw-encoding` coerente com `raw`.

**Por que não validar em execução:** o serializador é um só, é determinístico e
é exercido pelo fuzz com validação completa em cada saída; um validador de
schema no worker custaria dezenas de microssegundos por mensagem
(`BenchmarkSchemaValidate`) para pegar um bug que o fuzz e os vetores já pegam
antes do release. É o desvio da regra "validar antes de publicar" do JLH (D3).

### 18.3 Fuzz — obrigatório antes de release

| Alvo | Pacote | Propriedade |
|---|---|---|
| `FuzzConvert` | `convert` | nunca entra em pânico; **toda saída passa nos dois schemas e nas regras semânticas**; `message` é UTF-8 válido |
| `FuzzSD` | `convert` | o parser de STRUCTURED-DATA nunca entra em pânico e respeita os tetos |
| `FuzzFrame` | `receiver` | a moldura RELP contra um oráculo independente: os mesmos frames, os mesmos dados cortados em `max_message_bytes`, o mesmo `DATALEN`, o erro onde o oráculo para, sem pânico e sem guardar além do limite |
| `FuzzProxy` | `receiver` | v1 e v2: sem pânico, nunca lê além do cabeçalho declarado |
| `FuzzRead` | `emergency` | o leitor do dump tolera truncamento e lixo sem pânico, contabiliza todo byte (linha guardada, inválida ou cauda cortada), nunca guarda uma linha acima do teto, e todo registro devolvido passa de novo, igual, por `ParseLine` e tem o payload cortável por posição |

O `FuzzHead` do R saiu com o codec do cabeçalho (§3.4); o `FuzzJSONBSafe` e o
`FuzzInboxPayload` do R não se aplicam (não há `jsonb`).

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$ -fuzztime=10m
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$      -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$   -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$   -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$    -fuzztime=10m
```

### 18.4 Corpus de mensagens reais

`internal/convert/testdata/corpus/` guarda mensagens **capturadas de emissores
reais** (`rsyslog`, `syslog-ng`, `journald`, `logger(1)`, glibc, Cisco IOS,
MikroTik, pfSense, HAProxy, nginx, Kubernetes, Windows via NXLog, log4j): um
`.raw` com os bytes exatos e um `.want` com os campos esperados do payload.
**O corpus nunca encolhe**: o teste falha abaixo de vinte entradas. Um caso novo
de campo entra aqui antes da correção do conversor.

### 18.5 Integração em processo

A pilha **real** (a mesma `run()` do binário), por sessões RELP de um cliente de
teste que fala como o `librelp`, contra um `nats-server` real com JetStream:
como o único destino é o NATS, todo teste que passa do passo 7 do boot precisa
de `RELPLOG_TEST_NATS_URL` (§18.6) e pula sem ela; as recusas de antes do passo
7 (exit 1) e os testes de contagem do shutdown rodam sem servidor. Cada teste
trabalha num `space` próprio, com um stream próprio criado a partir de
`docker/nats/stream.json` e apagado no fim. Falhas do destino são injetadas
decorando o sink real (`App.wrapSink`). Cobre: RELP em claro chega ao stream,
PROXY v1 e v2 (IPv6 inclusive), `200 OK` só depois do ring, ring cheio e sink
DOWN segurados e devolvidos sem `500`, fidelidade do JLH (o `Data` da mensagem
é o JLH byte a byte, com NUL no meio, e `x-transport: "relp"`), `SIGHUP` de
`[log]` (ida e volta, chave sombreada, arquivo inválido), dump de emergência e
reingestão exatos (mesmo `uuid`, mesmo JLH, mesmo depois de trocar uma chave
`[syslog]`), dump danificado em `.corrupt`, shutdown gracioso sem perda,
segundo sinal (130), `SIGQUIT`, nenhum transporte (exit 1), chaves do R na linha
de comando (exit 1) e no env (`WARN`), códigos 100–114 do `go-loghub-ident` num
subprocesso, e a ordem do boot.

### 18.6 Destino real

As suítes do NATS pulam sem a variável. A da app cria cada stream **a partir do
arquivo que o operador aplica** (`docker/nats/stream.json`), com os subjects do
`space` do teste; a do sink, de um stream canônico em código que um teste sem
servidor confere, junto com o arquivo, contra o contrato de §10.7:

```bash
RELPLOG_TEST_NATS_URL=nats://127.0.0.1:14292 \
RELPLOG_TEST_NATS_NOJS_URL=nats://127.0.0.1:14293 \
RELPLOG_TEST_NATS_PERM_URL=nats://127.0.0.1:14294 \
RELPLOG_TEST_NATS_TLS_URL=nats://127.0.0.1:14295 \
  go test -race -timeout 900s ./...
```

`RELPLOG_TEST_NATS_NOJS_URL` aponta para um servidor **sem** JetStream (o FATAL
imediato de §10.7); `RELPLOG_TEST_NATS_PERM_URL`, para um com permissões
negadas (a configuração está em `docs/TESTING.md` §6); `RELPLOG_TEST_NATS_TLS_URL`,
para um com `tls {}` (o `tls_required` do `INFO`): o teste prova, por um proxy
que grava os bytes, que nenhum `ClientHello` sai e que o boot sai com 3
nomeando o TLS. O `nats-server` dos testes é o v2.14.6; o Compose usa
`nats:2.11-alpine` e `natsio/nats-box:0.20.0`, nunca `latest`.

### 18.7 Emissores vivos

`RELPLOG_TEST_EMITTERS=1` (com `RELPLOG_TEST_NATS_URL`) sobe o **rsyslog real
com `omrelp`** (Alpine 3.22, pacote `rsyslog-relp`) em contêineres descartáveis
(`rlnats-emit-*`, imagem `rlnats-test-rsyslog:alpine-3.22`) contra o servidor em
processo:

| Cenário | Prova |
|---|---|
| RELP em claro, N mensagens | N confirmações, N mensagens no stream, todas com `x-transport: "relp"` |
| ring cheio além do `backpressure_timeout` | a sessão fecha sem `500`, o rsyslog reconecta e retransmite: **todas** as mensagens chegam |
| sink FATAL com transações em voo, dump, e um servidor novo no mesmo `data_dir` | reconciliação: enviadas = mensagens no stream, sem perda |

O cenário TLS e o do `500` da recusa `logic` com `ring=none` do R saíram com o
que provavam.
Resultados em `docs/TESTING.md` §7.

### 18.8 Critérios de aceite v1

**Nunca medidos ponta a ponta.** Alvo, não fato; a primeira medição substitui
esta tabela. A peça de recepção é a do R (`BenchmarkRELPSession`: uma sessão
sobre loopback, janela cheia, `200` depois do ring em memória, sem sink;
`docs/TESTING.md` §4).

| Cenário | Alvo |
|---|---|
| RELP, uma conexão do rsyslog, janela 128, mensagens de 200 B, `jetstream` | ≥ 80k msg/s (a confirmação custa) |
| Perda com `SIGTERM` durante carga | **zero**, reconciliada contra o stream (§18.7) |
| Perda com FATAL do sink | **zero**, via dump e reingestão |

---

## 19. Matriz de garantias

Leia junto com §2.3 e §5.4: o `200 OK` de cada transação RELP significa **aceito
na RAM deste nó**.

| Modo | Perda em `kill -9` / OOM | Perda em `SIGTERM` | Perda em FATAL do sink |
|---|---|---|---|
| `jetstream` | o backlog do ring, já confirmado ao rsyslog | nada, se o dreno couber em `drain_timeout`; o resto vai para o dump | nada: o dump (§12) |
| `core` | idem | o que nenhum assinante pegou, mais o que não coube no dreno (para o dump) | idem, mais o dump |

Depois do `PubAck`, a durabilidade é a do stream (`storage`, `num_replicas`,
`sync_interval`/`sync_always` do `nats-server`). Em `core` não há durabilidade
nenhuma depois do servidor: o sucesso é "o servidor recebeu". Um dump que falha
(§12.3) é perda, contada em `reason="emergency_failed"`; um aborto (segundo
sinal, `SIGQUIT`) é perda contada em `reason="abort"`, e o dump em andamento é
abandonado (§13.4). Quem precisa de `kill -9` sem perda usa o `ring=sqlite` do
`go-relplog-server`.

**Duplicatas** (decisão 3 do R). O RELP não carrega id de mensagem: uma
retransmissão do rsyslog chega como mensagem nova, com `uuid` novo, e o
`Nats-Msg-Id` não a deduplica. Ela acontece quando uma mensagem **já aceita**
não teve o `200 OK` processado pelo cliente: corte abrupto (`kill -9`, rede
caindo), escrita de respostas que falhou, uma transação decidida depois de a
sessão fechar, ou a sessão que o próprio `librelp` quebra depois de um `500`
(§5.4). O shutdown gracioso e o fechamento por backpressure esvaziam as
respostas decididas antes de fechar. É at-least-once; os consumidores que
precisam de exatidão deduplicam por conteúdo + janela de tempo, sabendo o custo.
E uma duplicata é do próprio rsyslog (observada no R com tcpdump, rsyslog 8.2410
e librelp 1.11): quando a ação `omrelp` volta de uma suspensão, o rsyslog
reenvia a mensagem que causou a suspensão, inclusive quando ela já tinha
recebido `200 OK`. Uma por suspensão; nada se perde.

Do outro lado, **dentro de uma vida**, a deduplicação é do stream: um retry do
worker e a reingestão de um dump reenviam o mesmo `Nats-Msg-Id` (`<uuid>.0`), e
dentro da `duplicate_window` ele é guardado uma vez só (§10.2).

| Situação | O emissor (rsyslog) | Este servidor |
|---|---|---|
| `200 OK` entregue | apaga | é responsável pela entrega, pela matriz acima |
| `500` (guarda, erro de protocolo) | **descarta e loga**; suspende a ação e retransmite a janela | contou em `messages_dropped_total{reason="unaddressable"}` ou `relp_responses_total{code="500"}` |
| sem resposta (sessão fechada) | retransmite na sessão seguinte | contou em `relp_unanswered_total` |
| recusa `logic` do NATS depois do `200 OK` | nada: já apagou | contou em `events_dropped_total{reason="sink_logic"}` e logou os `uuid` |

---

## 20. Invariantes

Se uma alteração conflita com um destes, ela está errada. A numeração é a do R;
os que perderam o objeto ficam com a nota.

1. O `200 OK` nunca sai antes de o item estar no ring.
2. Ring cheio e sink DOWN nunca viram `500` nem descarte: a transação é segurada
   e, se não couber a tempo, devolvida ao emissor pelo fim da sessão. Um `500` só
   vai para uma recusa **final** e contada, e no fluxo normal não há nenhuma
   (§5.4).
3. Um buffer volta ao pool **uma vez**, e só após a confirmação do sink. Num
   retry o item ainda é o dono; uma tentativa por prazo, ou um erro depois da
   primeira publicação assíncrona, contamina o pedaço.
4. Item aceito só sai do ring após a confirmação do sink (ou um descarte
   deliberado e contado: `logic`, malformada recusada); nunca para liberar
   espaço.
5. **Toda mensagem aceita vira exatamente um JLH válido** pelos dois schemas, ou
   um descarte contado. A conversão nunca falha; o pior resultado é `malformed`.
6. **Sem perda e sem invenção:** o cru recebido (já sem moldura, já truncado) está
   em `raw` quando ele é obrigatório ou ligado; campo ausente é omitido, nunca
   adivinhado nem `null`.
7. Nenhum sink altera o JLH: o `Data` da mensagem NATS é o JLH byte a byte, e o
   JLH nunca vai num header.
8. Os endereços do payload são a ponta real do transporte, ou o cliente
   declarado por um PROXY **confiável**, e então o par real fica em `x-proxy`. O
   `hostname` é sempre alegação do emissor e nunca vira chave de decisão.
9. Nenhum label de métrica deriva de conteúdo controlado pelo emissor.
10. Nenhum log síncrono por mensagem no caminho padrão; nenhum payload em log de
    `INFO` ou acima.
11. Removido no go-relplog-nats-server (era do rueidis, §1.2).
12. O JLH é idêntico no worker e no dump: a mesma conversão, com a mesma config
    congelada.
13. **O JLH é função de cabeçalho + cru + config de conversão congelada** (§3.5):
    mesmo `uuid`, mesmos bytes, em retry e dump. A reingestão não converte.
14. **Uma amostra de relógio por mensagem**, e dela saem o `uuid` e o `event`.
15. `ring` e `sink` são ortogonais: nenhum consulta o outro.
16. Toda escrita em disco fica sob `server.data_dir` (`/data` no contêiner),
    inclusive a identidade, **por padrão**, sem exceção fixa. Um
    `emergency.dir` absoluto fora dele é decisão do operador, aceita com `WARN`
    no boot (§13.2).
17. O servidor nunca cria o que valida: o sink nunca cria nem altera um stream.
18. Configuração e permissão do destino são `fatal`, nunca `logic` (credenciais
    recusadas, permissão negada, stream ausente ou de outro dono, JetStream
    desligado, servidor que exige TLS, 4xx desconhecido da API); erro
    desconhecido é `network`, nunca `logic`.
19. Nenhuma biblioteca cliente guarda nem retenta o que o ring guarda e o worker
    retenta: `WithRetryAttempts(0)` e `ReconnectBufSize(-1)` no `nats.go`.
20. Nenhuma mensagem que o teto do NATS recusaria sai deste servidor: o boot, e
    depois toda checagem de saúde, compara `max_payload`, `max_msg_size`, o
    registro do file store e, com `discard: new`, o `max_bytes` com a maior
    mensagem (§3.9, §10.7), e nenhuma linha de dump chega ao stream sem passar por
    `emergency.ParseLine` (§12.2).
21. O servidor nunca emite `ping`, `pong` nem sonar, e o stream de dados nunca
    cobre o plano de controle.
22. `lhident.Initialize()` roda uma vez por processo, no `main`; nenhum pacote lê
    identidade em `init()`.
23. **A resposta de uma transação decidida nunca é descartada**, e o socket de
    uma sessão só fecha depois de o escritor entregar todas elas (ou de
    `closeGrace`).
24. **As cópias das transações em voo têm teto global** (`relp.inflight_max_bytes`)
    e são feitas só depois da vaga na janela e no teto.
25. Todo envelope tem `x-transport: "relp"`, `framing: "octet-counting"` e
    `transport: "tcp"`.
26. **Uma perda é contada uma vez, e o que foi contado como perdido não
    volta**: o aborto e o dump falho nunca contam as mesmas mensagens, e um dump
    abandonado pelo aborto não chega ao disco (§13.4).

---

## 21. Roteiro pós-v1

Nada disto entra antes de a v1 estar em produção e §18.8 ter sido medido.

| Item | Motivo de ter ficado fora |
|---|---|
| TLS (listener RELP ou cliente NATS) | Fora por decisão (§1.2); voltar exige decisão nova, não só código. Hoje: o `go-relplog-server` |
| `relp` no enum de `transport` da skill | Pedir à skill; até lá, `x-transport` (§3.2) |
| Comandos RELP de extensão | Nenhum cliente conhecido usa |
| `space` por listener ou rede | Multi-tenant numa instância; fora da v1 |
| Resposta a sonar, `ping` e `pong` | Até existir um `go-loghub-driver` (§11) |
| Ferramenta de reingestão manual de um `.jsonl` (ou de um `.corrupt` reparado) | Hoje basta copiar o arquivo para `emergency.dir` com o nome `emg-*.jsonl` e reiniciar |

---

## 22. Lacunas e riscos

Os riscos do desenho estão em `docs/DECISOES.md` §3, e o que os testes ainda não
provam, em `docs/TESTING.md` §8. Esta seção guarda o que o **código atual** ainda
faz diferente do que deveria. Cada item diz o problema, a consequência e a
direção da correção, que segue a regra de sempre: a spec primeiro (a regra entra
na seção indicada), depois o código e o teste. Corrigido um item, ele sai daqui.

**O que veio corrigido dos pais.** As 30 pendências do `go-syslog-server`
corrigidas no R, as 11 do SN (§21.1 da SPEC dele, todas corrigidas no R ou no
porte de 2026-10-11) e os achados das revisões independentes do R
(2026-10-11/12) estão neste código, com a regra na seção onde vale; a lista,
item a item, com o teste de cada um, está em `docs/DECISOES.md` §2.3.

**O que a revisão independente desta criação achou** (2026-10-13, cinco áreas:
RELP, app/shutdown/dump, config/`LOGHUB_EB_URL`, sink NATS e beacon, docs) e foi
corrigido, cada um com um teste que falhava antes: §5.4 (contexto cancelado),
§10.7 (TLS na reconexão, *flush* através de uma reconexão, folga do
`MaxPending`), §12.2 (prazo sem progresso, gauge do pedaço, `ErrNotDurable` no
`open` do diretório), §13.1 (bind cancelado), §13.2 (flag do R depois de uma
flag com valor, segredo que quebra de linha no INI, `,` numa senha), §13.4
(contagem do aborto, dump abandonado, `SIGQUIT` tardio). Os que valem para os
pais são a lista "a portar" da entrada **RN** do `CHANGELOG.md` da raiz.

**O que veio da revisão independente do irmão RP** (2026-10-13, no código
comum, portado no mesmo dia; lista em `docs/TESTING.md` §9.1): §2.2 e §5.7
(pré-checagem dos dois tetos do ring, fim da sessão antes do `Enqueue`), §10.1
(recusa `logic` vence o cancelamento no `Deliver`), §12.2 (`Count` linha a
linha), §13.2 (tetos, *switches* com valor, `--config=` vazio, valor colado ao
nome, BOM no INI, `-h` não nomeado, `LOGHUB_EB_URL` só com brancos, host em
porcento), §13.4 (dump publicado depois do aborto não conta mais nada), §16.3
(sinônimos do charset), §16.5 (`emergency.dir` derivado comentado no `--dump-config`).

### 22.1 Pendências conhecidas

Atualizado em 2026-10-13, no porte da revisão independente do RP. O que ficou
fora por decisão (o TLS, os outros rings e destinos, o filtro, o rate-limit)
está em §1.2 e em `docs/DECISOES.md`; o que os testes não provam, em
`docs/TESTING.md` §8. As arestas abaixo são conhecidas e **não** corrigidas;
cada uma diz o cenário e por que ficou. Os itens 4 a 9 são do código comum com
o RP e estão também no §22.1 dele.

1. **Teto de bytes do ring pequeno demais (§15.1).** Herdado do R e do SN: um
   `ring.max_bytes` abaixo de 2 KiB + 256 B é aceito e nenhuma mensagem cabe;
   toda transação é segurada, a sessão fecha no `backpressure_timeout`, e o
   rsyslog reconecta para sempre. Direção: recusar na validação um
   `ring.max_bytes` abaixo do custo de um item da menor classe (exit 1), nos três
   servidores.
2. **Um dump publicado entre a última conferência do abandono e o `link` (§13.4)
   é contado como perdido e reingerido**: uma contagem a mais, nunca uma perda.
   Direção, a decidir: aceitar (a janela é de microssegundos) ou segurar o
   `lossMu` durante o `link` e o `fsync` do diretório, fazendo o aborto esperar
   um disco que pode estar preso.
3. **Erros de discagem da reconexão logados em `ERROR`** (§10.7). O
   `ReconnectErrHandler` manda toda discagem falha ao log de erros assíncronos
   (amostrado, um a cada 30 s), além do `WARN` de desconexão: numa queda longa
   do NATS, um `ERROR` a cada 30 s por um estado que o `/readyz` e o
   `relplog_sink_up` já mostram. Direção, a decidir: `WARN`, ou só o primeiro.
4. **`SIGHUP` relata a chave derivada junto com a que a deriva** (herdada do
   SN). Trocar `server.data_dir` no INI e mandar `HUP` loga "requires a
   restart" para ele **e** para `emergency.dir` derivado. É verdade (as duas
   mudam no restart), só redundante; separar derivadas de explícitas na
   comparação de §16.3 custa mais que o ruído. (Os sinônimos do charset foram
   corrigidos: §16.3.)
5. **`listen.tcp` e `admin.bind` na mesma porta** só falham ao abrir o
   transporte (passo 12), com exit 1 e a porta admin já tendo servido o boot
   inteiro. Comparar os dois endereços na validação exigiria resolver nomes
   antes do boot.
6. **Os contadores da recepção não têm *padding*** e disputam a linha de cache
   entre listeners `SO_REUSEPORT`. Nunca medido como gargalo.
7. **Aborto durante o `pre_stop_delay`** (`SIGQUIT`, ou o segundo sinal, na fase
   2 do shutdown): o `abortLoss` lê a ocupação do ring enquanto o transporte
   ainda aceita, e uma transação que entra no ring entre a leitura e o
   `os.Exit` fica sem contagem (herdada do R; revisão do RP, 2026-10-13, A9). A
   janela é de microssegundos e o processo está saindo; fechá-la pediria parar
   a recepção antes de contar, o que mudaria a semântica do `pre_stop_delay`.
8. **`tcp.idle_timeout` menor que `relp.backpressure_timeout`**: uma sessão com
   uma transação segurada e nenhum frame novo fecha pelo idle, e a transação
   conta em `relp_unanswered_total{reason="server_closed"}`, não
   `backpressure` (herdada do R; revisão do RP, 2026-10-13). O efeito no emissor
   é o mesmo (retransmite); só o rótulo erra.
9. **`relplog_sink_errors_total{kind="fatal"}`** conta uma escrita fatal mesmo
   quando o boot já foi cancelado (`Instrumented.write`); cosmético (revisão do
   RP, 2026-10-13, K4).
