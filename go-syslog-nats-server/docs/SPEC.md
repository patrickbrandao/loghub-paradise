# go-syslog-nats-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-07
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server`
Binário: `go-syslog-nats-server`
Entrada: `docs/DECISOES.md` (entrevista de 2026-10-07), que aponta para as
decisões herdadas do `go-syslog-pg-server` (`../go-syslog-pg-server/docs/DECISOES.md`)
e do `go-syslog-server` (`../go-syslog-server/docs/DECISOES.md`).

**Origem.** Esta especificação foi derivada em 2026-10-07 da SPEC do
`go-syslog-pg-server` (o corte "só PostgreSQL" do `go-syslog-server`, do mesmo
dia), com o sink trocado pelo **NATS** do pai (§3.8, §10.7, §10.9 e §11 da SPEC
do `go-syslog-server`, revisão de 2026-10-07). O pedido do usuário: "Nesse mesmo
molde, crie o software go-syslog-nats-server" — um servidor de syslog rápido e
com dependências mínimas, com **sink só no NATS** (JetStream por padrão), **ring
só na memória**, emergência em arquivo **JSON Lines**, **sem TLS** e sem os
demais recursos do pai (§1.2). O heartbeat beacon, que o corte PostgreSQL tirou
por depender do NATS, **fica** aqui (§11).

**A numeração das seções é a do `go-syslog-server`, de propósito.** O código
cita a spec por número (`§10.7`, `§12.2`, `§20.19`), e os três servidores são da
mesma família: um número que mudasse de sentido entre eles enganaria quem lê
mais de um. Uma seção removida **mantém o título**, seguido de uma nota de uma
linha; nada é renumerado.

Servidor de ingestão de syslog do ecossistema **LogHub**. Recebe mensagens por
UDP, TCP e socket unix, converte cada uma num envelope **JLH** com
`type=syslog` e o **publica** no NATS, no subject `loghub.<space>.syslog`: por
padrão num stream JetStream, com confirmação (`PubAck`) por mensagem. É um
**Bridge-IN**: cria a primeira mensagem de cada fato (`step = 0`).

---

## 1. Escopo

### 1.1 Objetivos

- Máximo de mensagens por segundo com mínimo de CPU e memória.
- **Dependências mínimas:** três diretas em produção (§17.3), binário pequeno.
- Cada mensagem aceita vira **exatamente um** envelope JLH válido pelos schemas
  das skills `loghub-json-schema` e `loghub-type-syslog-schema` (§3).
- **Nunca confirmar** uma mensagem antes de ela estar no ring, nos transportes
  que têm como confirmar (§2.3).
- Backpressure explícito onde o transporte permite; perda **contada e visível**
  onde ele não permite.
- **Sem perda e sem invenção** no payload: o que chegou está nos campos ou em
  `raw`; o que não chegou é omitido, nunca adivinhado (§6).
- Um destino só: o subject `loghub.<space>.syslog` de um servidor (ou cluster)
  NATS, gravado por padrão num stream JetStream que o operador cria (§3.8,
  §10.7).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| **TLS no listener** (RFC 5425) e **mTLS** | Decisão do usuário (2026-10-07, decisão 2 da entrevista, a opção recomendada; a mesma do `go-syslog-pg-server`): **sem TLS em lugar nenhum**. Quem precisa de syslog cifrado usa o `go-syslog-server` ou um terminador TLS na frente (com PROXY protocol, §4.6). O payload nunca traz `transport=tls` nem o objeto `tls` |
| **TLS até o NATS** | A mesma decisão. O cliente é **sempre texto claro**: sem `nats.Secure`, sem CA própria, sem `tls_insecure` (as chaves `sink.nats.tls`, `tls_ca` e `tls_insecure` do pai saíram). Toda entrada de `sink.nats.urls` precisa ser `nats://`: `tls://`, `ws://`, `wss://` e um `host:porta` sem esquema são exit 1 (§13.2). A `LOGHUB_EB_URL` só aceita `nats://`; `tls://` é exit 1 com uma mensagem que nomeia o TLS (§10.9). E como o `nats.go` **se promove sozinho** a TLS quando o `INFO` do servidor diz `tls_required`, o sink instala um `TLSCertCB` que recusa: nenhum `ClientHello` é enviado, e um servidor que exige TLS é FATAL na hora (exit 3 no boot, nomeando o TLS; §10.7). O NATS é alcançado por rede confiável |
| Outros sinks: `postgres`, `redis`, `sqlite`, `mysql`, `clickhouse`, `none` | Pedido do usuário: sink **só** no NATS. Cada um trazia um cliente (pgx, rueidis, SQLite, MySQL, ClickHouse) e a sua árvore de transitivas. Quem precisa deles usa o `go-syslog-server` (todos) ou o `go-syslog-pg-server` (a fila `loghub.inbox`) |
| Outros rings: `sqlite`, `redis`, `none` | O molde do `go-syslog-pg-server`: ring **só** na memória. A perda sob `kill -9` é a do ring em memória (§19); FATAL e `SIGTERM` são cobertos pelo dump (§12) |
| Subjects por facility/severidade (`sink.nats.subjects = facility-severity`) | Decisão 4 da entrevista: saem. O subject é sempre `loghub.<space>.syslog`, a regra `loghub.{space}.{type}` do JLH; a chave `sink.nats.subjects` não existe. Tokens extras quebravam assinaturas exatas, e a skill do envelope avisa contra eles |
| Filtro por facility/severidade | Herdado do corte PostgreSQL: sai. O descarte, se houver, é do consumidor; a recepção nem lê o PRI (§2.2) |
| Rate-limit por prefixo de rede | Herdado: sai. Proteção contra emissor ruidoso fica na rede (firewall, QoS) |
| `sink.type` e `ring.mode` como chaves | Um valor só cada um. O log de boot e o `syslog_config_info` continuam dizendo `ring_mode="memory"` e `sink_type="nats"`, para que um painel do pai sirva aqui (§14.1) |
| Resposta a sonar, `ping` e `pong` | Como no pai: o servidor só **emite** o heartbeat (§11), não assina nada. Lacuna até existir um `go-loghub-driver` (§21) |
| RELP | Vira software próprio (`go-relplog-server/`); o enum de `transport` do `type=syslog` não tem `relp` |
| DTLS (RFC 6012), BEEP (RFC 3195) | Como no pai: sem implementação na stdlib, sem emissores vivos |
| Importação de arquivo (`transport=file`) | Fora da v1 (D5) |
| `space` por listener ou rede | Um Workspace por instância na v1 |
| Validação por JSON Schema em execução | Só em teste e CI (§18.2; D3) |
| Autenticação de emissor | Não existe no protocolo sem TLS: firewall ou rede privada |
| Roteamento por facility, deduplicação de conteúdo, syslog **de saída** | Responsabilidade de outro software |

O porquê comum: **o pedido do usuário** (o molde do `go-syslog-pg-server`: um
servidor rápido e com dependências mínimas), **as dependências** (do pai, com
oito em produção, para três diretas; o binário tem 12,5 MB contra os 17,2 MB do
`go-syslog-pg-server`, mesmo `go build` simples, `darwin/arm64`, Go 1.27.1,
§17.3) e **a velocidade** (a recepção não lê a mensagem: nem PRI, nem filtro,
nem balde de rate-limit; §2.2).

### 1.3 Fontes normativas e desvios

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, `jlh-envelope.schema.json`, subject `loghub.{space}.{type}`, `Nats-Msg-Id = <uuid>.<step>`, plano de controle (o heartbeat de §11) |
| skill `loghub-type-syslog-schema` | consultada em 2026-10-05 | Payload `type=syslog`, `jlh-syslog-payload.schema.json`, algoritmo de conversão, 24 vetores dourados |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 | AgentUUID, Workspace, AgentName, Hostname, MachineID, `DATADIR` |

Cópias dos dois schemas, dos exemplos de envelope (inclusive os de `beacon`) e
dos vetores ficam em `internal/jlhcheck/loghub/` (§18.2). Onde as skills
divergem, vale a mais restritiva.

**Desvios deliberados das skills**, os mesmos do pai, cada um com o motivo na
seção citada:

| # | Desvio | Skill diz | Aqui | Seção |
|---|---|---|---|---|
| D1 | Moldura TCP | detectar **por frame** | **por conexão**, com confirmação `[1-9][0-9]{0,9} SP` | §5.2 |
| D2 | TAG sem timestamp | sem timestamp, nenhuma TAG | TAG **estrita** ainda extrai `appname`/`procid` | §6.5.5 |
| D3 | Validação | todo envelope passa no schema antes de ser publicado | schema só em teste e CI; o serializador é provado por fuzz | §18.2 |
| D4 | `LOGHUB_UUID_LEVEL` | variável sugerida | **não existe**: nível 3 fixo, que é o que o JLH exige | §3.1 |
| D5 | Opções de importação | `reference`, `transport=file` | fora da v1 | §1.2 |
| D6 | Espaço em branco nos campos `^\S+$` | o conversor de referência emite o valor | o valor é omitido (cabeçalho 5424: `header-invalid` e `raw` obrigatório; 3164: não é hostname/TAG). O conjunto é o `\s` do ECMAScript **mais** U+001C–U+001F e U+0085, que o `\s` do Python (o `validate_payload.py` da skill) também casa | §6.4.1 |
| D7 | Avisos de encoding de um SD seguido de texto colado | a referência funde os avisos dos valores do SD antes de recusar o texto colado | descartados, como em qualquer SD que falhou: só `message` decodifica e avisa | §6.4.3 |
| D8 | Timestamp cujo instante UTC cai fora dos anos 0001–9999 | a referência emite `10000-01-01T…` ou `0000-12-31T…` | `timestamp-invalid` e `timestamp` omitido | §6.4.2 |

Não é desvio, mas fica registrado: o único vetor dourado com
`transport=tls` roda **como TCP** (§18.2), porque não há TLS aqui.

---

## 2. Arquitetura

### 2.1 Dois eixos ortogonais

No `go-syslog-server` são dois eixos, ring × sink, com vinte e oito combinações.
Aqui cada eixo tem **um valor só**:

- **RING**: `memory` (§9.3).
- **SINK**: `nats`, `jetstream` por padrão ou `core` (§10.7).

As chaves `ring.mode` e `sink.type` não existem (§1.2). A separação continua no
código: **o ring não consulta o sink, e vice-versa** (invariante §20.15), e as
interfaces `ring.Ring` e `sink.Sink` ficam, porque são a costura pela qual
recepção, workers e saúde são testados.

```
   emissores (rsyslog, syslog-ng, journald, logger, appliances, k8s)
      |
      |  UDP :1514   TCP :1514   unix dgram
      v
 +----------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                   |
 |   [PROXY] -> moldura -> truncamento -> amostra de relógio       |
 |   -> estado do sink -> uuid -> Item(cabeçalho + cru)            |
 +-------------------------------+--------------------------------+
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

 +----------------------------------------------------------------+
 | porta admin (admin.bind, padrão 127.0.0.1:9102)                 |
 |   /healthz   /readyz   /metrics                                 |
 +----------------------------------------------------------------+
```

O ring é **único e global**, compartilhado por todos os transportes: uma métrica
única de profundidade, backpressure justo (o TCP só para de ler quando o sistema
inteiro está saturado) e código simples.

### 2.2 Onde a conversão acontece, e por quê

A recepção faz o mínimo que permite decidir se a mensagem entra; a conversão
completa roda no **worker de dreno**.

| Etapa | Onde roda | Custo |
|---|---|---|
| PROXY | recepção, uma vez por conexão TCP | do transporte |
| Moldura, truncamento | recepção | inevitável |
| Amostra de relógio, sink DOWN, ring cheio, `uuid` | recepção | uma leitura de relógio, dois testes, um UUID |
| PRI, RFC 3164 / RFC 5424, SD, UTF-8 | **worker de dreno** | proporcional à linha |
| Serialização do JLH | **worker de dreno** | proporcional aos campos |

A ordem da recepção é estrita, e a primeira condição que falha encerra a
mensagem: vazia (§5.6) → truncamento (§5.5) → sink DOWN (§13.3) → **ring
cheio** → `uuid`, cabeçalho e cópia para um buffer do pool → `ring.Enqueue`. O
"ring cheio" é uma pré-checagem barata e sem trava (`ring.Memory.FullFor(n)`:
itens ≥ `capacity_items`, **ou** os bytes do ring mais a classe do pool de `n`
— o tamanho já truncado — e o `ItemOverhead` acima de `max_bytes`), feita
**antes** do `uuid`, da cópia e do item: uma mensagem recusada com o ring cheio
(uma enxurrada de UDP, a retentativa do TCP a cada 50 ms) custa ~25 ns e nenhuma
alocação, em vez de ~115 ns e duas. Conferir só o teto de itens deixava o ring
cheio por `max_bytes` cunhar `uuid`, copiar e montar o item a cada retentativa,
para o `Enqueue` recusar (revisão independente do `go-relplog-pg-server`,
2026-10-13, portada). Um ring fechado nunca está "cheio": o `Enqueue` responde
`ErrClosed`, que é o shutdown. Ela pode errar nos dois sentidos numa corrida;
**o `Enqueue` continua sendo a autoridade** (os dois tetos, §9.2).

No pai o PRI era lido na recepção, porque o filtro dependia dele. Sem filtro
(§1.2), **a recepção não lê a mensagem**: nenhum byte do conteúdo é
interpretado antes do ring. Num pico de UDP, toda a CPU da conversão é paga
pelos `sink.workers`, que é onde a vazão é elástica.

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

### 2.3 Regra de aceitação, por transporte

> A confirmação sai no instante em que o item entrou no ring — nunca antes.
> **Onde não existe confirmação, existe um contador.**

| Transporte | A confirmação é | Ring cheio ou sink DOWN significa |
|---|---|---|
| **UDP** | não existe | datagrama **descartado**, `syslog_messages_dropped_total{reason="ring_full"\|"sink_down"}` |
| **unix dgram** | não existe | idem; mas o kernel bloqueia o emissor quando o buffer do socket enche, e o backpressure existe **antes** de chegar a nós |
| **TCP** | continuar lendo do socket | **parar de ler** (§5.7); a janela TCP segura o emissor e nada se perde |

A garantia é a do único ring: **o item entrou no canal do ring em memória**
(§9.3). A durabilidade dele está em §19.

**O UDP perde por desenho do protocolo, não deste servidor.** O que o kernel
descarta antes de chegarmos aparece em `syslog_udp_receive_errors_total` e nos
contadores da interface. Quem não pode perder usa TCP.

### 2.4 Papel no LogHub

| Aspecto | Valor |
|---|---|
| Papel | **Bridge-IN**: traz dados de fora e publica como envelopes |
| Operação | **Produce** (primeira inserção): `uuid` novo, `step = 0`, `from` = AgentUUID |
| Subject | `loghub.{space}.syslog`, e só ele (§3.8) |
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
| `uuid` | UUIDv7 **nível 3** (`go-loghub-uuidv7`), em minúsculas, gerado **na recepção** com `GenerateAt(Level3, t)`, onde `t` é a amostra de relógio da mensagem. É cunhado depois de conferidos o estado do sink e a pré-checagem do ring cheio (§2.2; uma mensagem recusada por isso não gasta `uuid`), mas sempre da amostra `t` do fim do frame. Gravado no cabeçalho do item e nunca regerado: a retentativa do laço de backpressure do TCP, o retry do worker e o dump reusam o mesmo; se uma tentativa de destino incerto (prazo estourado) tiver sido aceita, as duas cópias deduplicam pelo `Nats-Msg-Id` (`<uuid>.0`) dentro da `duplicate_window` do stream (§3.8, §10.2). O nível 3 é fixo; `LOGHUB_UUID_LEVEL` não existe (D4). |
| `from` | AgentUUID de **quem recebeu** a mensagem (`lhident.AgentUUID()`), copiado para o cabeçalho do item. Nunca um identificador do emissor remoto: a origem externa está em `payload.host`. |
| `space` | Workspace de quem recebeu (`lhident.Workspace()`), copiado para o cabeçalho. Validado no boot (§13.2). |
| `type` | `"syslog"`. |
| `level` | `payload.severity.code` (0–7), inclusive quando o PRI veio do padrão. |
| `sid` | FNV-1a 64 da quíntupla de rede, reduzido a 53 bits (§6.10); `0` no `unix`. |
| `step` | `0`: Bridge-IN. |
| `event` | A **mesma** amostra `t` do `uuid`: `uts` = segundos, `sub` = nanossegundos, `iso` = `t` **truncado** em ms, `yyyy-MM-ddTHH:mm:ss.SSSZ`. Tempo real: `event` = instante do `uuid`, até o nanossegundo. |

**Uma amostra de relógio por mensagem**, tirada no fim da leitura do frame (o
último octeto do frame, ou o retorno do `recvmsg` do datagrama), antes de
qualquer parse. O `uuid` e o `event` saem dela; ler o relógio duas vezes faria o
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
| `host.dst-ip`, `host.dst-port` | IP canônico, int | se conhecidos (§4.2) |
| `transport` | `udp` \| `tcp` \| `unix` | sempre |
| `framing` | `datagram` \| `octet-counting` \| `non-transparent` | sempre |
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

O schema da skill aceita `transport = "tls"` e o objeto `tls`; **este servidor
nunca os emite** (§1.2). `transport` também nunca é `dtls`, `file` nem `beep`, e
`format` nunca é `rfc3195`. A única extensão emitida é `x-proxy`.

**Omissão:** campo sem valor na origem (NILVALUE, inexistente no formato,
inválido) é **omitido**. Nunca `null`, nunca `""`, exceto `message`. Objetos
sem conteúdo (`host`, `parse`, `structured-data`) também são omitidos.

Exemplo (UDP de `10.0.0.3:59922` para `10.0.0.10:514`):

```json
{
  "host": { "hostname": "srv101.intranet.br", "src-ip": "10.0.0.3", "src-port": 59922,
            "dst-ip": "10.0.0.10", "dst-port": 514 },
  "transport": "udp", "framing": "datagram", "format": "rfc5424", "version": 1,
  "priority": 86, "facility": { "code": 10, "name": "authpriv" },
  "severity": { "code": 6, "name": "info" }, "timestamp": "2024-03-15T14:22:05.123Z",
  "appname": "sshd", "procid": "12345", "msgid": "ID47",
  "message": "Accepted publickey for alice from 10.0.0.2 port 54321 ssh2",
  "size": 126,
  "raw": "<86>1 2024-03-15T14:22:05.123Z srv101.intranet.br sshd 12345 ID47 - Accepted publickey for alice from 10.0.0.2 port 54321 ssh2"
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
| `Transport`, `Framing` | §3.2 |
| `SrcIP`, `SrcPort`, `DstIP`, `DstPort` | endereços **canônicos** (§4.2), já resolvidos pelo PROXY quando houver; zero quando desconhecidos ou no `unix` |
| `PeerIP`, `PeerPort` | o par TCP real, **só** quando o PROXY protocol foi usado |
| `Size` | octetos originais da SYSLOG-MSG; `Size > len(cru)` significa truncamento (§5.5) |

O PRI **não** está no cabeçalho: ele é derivável do cru, e a conversão o lê com
`convert.ScanPRI` (§6.2). Guardá-lo seria criar duas fontes da mesma verdade.

**O cabeçalho só existe em memória.** No pai ele tinha um codec JSON
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
duas execuções sem mudar um byte: a regra que no pai exigia a mesma config entre
quem grava e quem drena aqui não tem a quem se aplicar.

### 3.6 Entrada do stream Redis (`sink=redis`)

Removido no go-syslog-nats-server (§1.2): não há `sink=redis`.

### 3.7 Tabela de eventos (`sink=sqlite`, `sink=mysql`) e fila `loghub.inbox` (`sink=postgres`)

Removido no go-syslog-nats-server (§1.2): não há sink SQL; o único destino é o NATS (§3.8, §10.7).

#### 3.7.1 Fila `loghub.inbox` (`sink=postgres`)

Removido no go-syslog-nats-server (§1.2): não há `sink=postgres`; a fila `loghub.inbox` é o destino do `go-syslog-pg-server`.

#### 3.7.2 Tabela `inbox_syslog` (`sink=clickhouse`)

Removido no go-syslog-nats-server (§1.2): não há `sink=clickhouse`.

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
  `loghub.<space>.syslog.<facility>.<severity>` do pai saíram (§1.2, decisão 4).
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

Com `L = syslog.max_message_bytes` (o cru nunca passa disso, §5.5):

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

Três transportes independentes, cada um ligado por um parâmetro de `[listen]`
que, vazio, o desliga. **Todos desligados é erro de configuração** (exit 1).

| Transporte | Parâmetro | Padrão | RFC | `framing` |
|---|---|---|---|---|
| UDP | `listen.udp` | `:1514` | RFC 5426 | `datagram` |
| TCP | `listen.tcp` | `:1514` | RFC 6587 | `octet-counting` ou `non-transparent` (§5.2) |
| unix datagram | `listen.unix` | `""` (desligado) | — | `datagram` |

O TLS (RFC 5425) do pai foi removido (§1.2, §4.5).

### 4.1 As portas padrão não são 514

A imagem roda como uid 65532 e não abre porta abaixo de 1024. Os padrões são
`:1514` (UDP e TCP). O `docker/docker-compose.yml` publica a porta `5181` do
host (`SYSLOG_PORT`) na 1514 do contêiner, fora das portas que os compose do
`go-syslog-server` (514 e 5141–5162) e do `go-syslog-pg-server` (5180) usam,
para que os três convivam no mesmo host. Quem roda no host e quer 514 usa
`setcap cap_net_bind_service=+ep /app/go-syslog-nats-server` ou
`sysctl net.ipv4.ip_unprivileged_port_start=0`. O log de boot imprime o endereço
efetivo de cada listener.

### 4.2 UDP (RFC 5426)

Um datagrama é **uma** mensagem; nunca é dividido por LF.

- Leitura com `ReadMsgUDPAddrPort` num buffer de 65 536 bytes, maior que
  qualquer datagrama (65 507), para que o kernel nunca corte em silêncio.
- **`dst-ip` por PKTINFO**, só com a stdlib (`syscall`):
  - Linux: `IP_PKTINFO` e `IPV6_RECVPKTINFO`; o IP local vem do `in_pktinfo` ou do
    `in6_pktinfo` da mensagem de controle;
  - BSD e macOS: `IP_RECVDSTADDR` (IPv4) e `IPV6_RECVPKTINFO` (IPv6);
  - outra plataforma, ou falha do `setsockopt`: `WARN` no boot e `dst-ip`
    omitido quando o socket está num endereço curinga.

  Com o listener num IP concreto, `dst-ip` é esse IP mesmo sem PKTINFO.
  `dst-port` é sempre a porta local do socket.
- **IPs canônicos**, como a skill exige: IPv4 em decimal pontuado; IPv6 na forma
  da RFC 5952 (minúsculas, zeros comprimidos); IPv4 mapeado (`::ffff:a.b.c.d`,
  comum em socket dual-stack) vira IPv4; a zona (`%eth0`) sai; `0.0.0.0` e `::`
  nunca são gravados (o campo é omitido).
- `listen.udp_read_buffer` (padrão 8 MiB) aplicado com `SO_RCVBUF`. **É o
  parâmetro mais importante do transporte**: o que não couber o kernel descarta
  antes de existirmos. O Linux reduz o pedido ao `net.core.rmem_max` sem
  devolver erro; o valor efetivo é lido de volta (`getsockopt`) e exposto em
  `syslog_udp_read_buffer_bytes`, com `WARN` quando for menor que o pedido.
- `listen.udp_listeners > 1` abre N sockets com `SO_REUSEPORT`.
- **Sem backpressure.** Ring cheio ou sink DOWN descartam o datagrama e
  incrementam o contador com a razão.
- Um datagrama de origem não endereçável (`0.0.0.0` ou `::`) não tem envelope
  válido possível (o schema exige `host.src-ip`): é descartado como
  `reason="malformed_frame"`.
- `syslog_udp_read_buffer_bytes` mostra o valor cru do `getsockopt`; no Linux,
  que reporta o dobro do concedido, o `WARN` de falta compara com a metade.

### 4.3 unix datagram

Socket `SOCK_DGRAM` em `listen.unix` (tipicamente `/dev/log`): `logger(1)`, a
`syslog(3)` da libc, o `journald` com `ForwardToSyslog=yes`.

- O servidor **cria** o socket no boot com `listen.unix_mode` (padrão `0666`). Um
  socket órfão é removido antes do `bind` só depois de um `connect` falhar; se
  alguém responde, o boot sai com exit 1 em vez de roubar o `/dev/log` de outro
  processo. Só um **socket datagrama** é removido (conferido sem seguir links):
  um arquivo comum, um diretório ou um socket de outro tipo no caminho é exit 1
  com a mensagem certa, e fica intocado. No shutdown, o arquivo é removido.
- Caminho relativo, ou maior que `sun_path` (107 bytes no Linux, 103 nos BSD e
  no macOS), é exit 1 com a mensagem certa.
- Buffer de leitura de `max(65536, syslog.max_message_bytes + 1)`. Um datagrama
  maior volta com `MSG_TRUNC`, e `size` registra os bytes lidos (um limite
  inferior); o aviso `truncated` sai porque `size > len(cru)`.
- Sem campos de rede no payload (o schema proíbe) e `sid = 0`.
- O backpressure é do kernel: com o buffer do socket cheio, o `send` do emissor
  bloqueia antes de a mensagem chegar até nós.

### 4.4 TCP (RFC 6587)

Uma conexão carrega N mensagens, separadas por uma das molduras de §5. É o
transporte com backpressure real.

- `listen.tcp_listeners > 1` habilita `SO_REUSEPORT`.
- `tcp.max_conns` e `tcp.max_conns_per_ip` (`0` = ilimitado) valem para o
  transporte inteiro, somando os listeners: um limitador por listener
  multiplicaria os tetos. O teto global **bloqueia no accept**; o por IP
  **fecha na hora**. O IP é o endereço **exato** do par TCP (um /128 no IPv6;
  um IPv4 mapeado em IPv6 é a mesma chave que o IPv4, §4.2), **antes** do PROXY
  protocol: com um balanceador na frente, o teto por IP vê o balanceador (§4.6).
  Quem tem um /64 pode abrir cada conexão de um endereço novo e passar do teto;
  contar por prefixo IPv6 foi recusado (decisão do `go-relplog-server`, portada
  pelo `go-syslog-server` e daqui em 2026-10-14, DECISOES §9): um /64 pode ser
  uma LAN inteira de emissores, e uma cota por prefixo deixaria um deles calar
  os outros. Contra um /64 hostil, o teto global e o firewall.
- `tcp.idle_timeout` (120 s) fecha conexão sem tráfego; `0` desliga.
- `SO_KEEPALIVE` ligado, com período de 30 s, inclusive sob o limitador de
  conexões (o *wrapper* é desembrulhado para chegar ao socket).
- O par de endereços do payload é `RemoteAddr` (origem) e `LocalAddr` (destino),
  canônicos.
- **O leitor nunca escreve**, então só o prazo de leitura importa (no pai, o
  `KeyUpdate` do TLS 1.3 fazia o leitor escrever e exigia o prazo de escrita).
  O `Close` do transporte vence o prazo de toda conexão e, se um leitor não
  voltar em 1 s, fecha o socket dele.

### 4.5 TLS (RFC 5425)

Removido no go-syslog-nats-server (§1.2, decisão 2): não há listener TLS nem mTLS.

### 4.6 PROXY protocol

Opt-in no TCP, para quando um balanceador (HAProxy, NLB, Traefik) está na
frente e o `RemoteAddr` seria o dele. É também o caminho para quem termina TLS
num proxy à frente deste servidor (§1.2).

- `proxy.protocol`:

  | Valor | Comportamento |
  |---|---|
  | `off` (padrão) | nada é lido; um cabeçalho PROXY vira o começo de uma mensagem `malformed` |
  | `optional` | de par **confiável**, lê o cabeçalho se ele vier e segue sem ele se não vier; de par não confiável, nada é lido |
  | `required` | toda conexão precisa vir de par confiável **com** cabeçalho válido; senão fecha (`reason="proxy"`) |

- Par confiável: `proxy.trusted` (CIDRs, padrão `127.0.0.1/32,::1/128`), testado
  contra o par TCP real.
- v1 (texto, `PROXY TCP4|TCP6|UNKNOWN ...\r\n`, até 107 bytes) e v2 (binário,
  assinatura de 12 bytes). O cabeçalho chega antes de qualquer byte syslog.
  Prazo próprio: `proxy.header_timeout` (5 s).
- v2 com comando `LOCAL`, família `UNSPEC`/`UNIX`/`DGRAM`, ou v1 `UNKNOWN`: usa
  o par real, como se não houvesse cabeçalho. TLVs são lidos e ignorados; um
  cabeçalho v2 acima de 4 KiB fecha a conexão.
- Parser próprio, só com a stdlib, alvo de fuzz (`FuzzProxy`, §18.3).
- Detalhes: em `optional`, um cabeçalho **inválido** de par confiável fecha a
  conexão (`invalid` e `reason="proxy"`); um cabeçalho que declara origem não
  especificada é inválido; de par não confiável nada é lido nem contado; uma
  conexão que fecha antes do primeiro byte (sonda de saúde do balanceador) não
  conta nada; e uma espera de cabeçalho vencida pelo **shutdown** (com ou sem
  cabeçalho pela metade) não conta nada (nem `absent`, nem `invalid`, nem
  `reason="proxy"`) nem loga: é um deploy, não uma falha do balanceador
  (corrigido em 2026-10-14, na forma do `go-syslog-server`). Só a **espera**:
  em `optional`, um par confiável que não mandou cabeçalho e cujas linhas
  syslog já estão no leitor quando o shutdown chega (o parser viu que não é
  cabeçalho: sem erro, sem cabeçalho) segue para a moldura, e as linhas entram
  no ring como qualquer mensagem já lida (§13.4); em `required`, a mesma conexão
  fecha sem contar. Com cabeçalho **válido** já lido, `required` também segue: o balanceador manda o cabeçalho e as primeiras linhas no mesmo segmento, o `Close` chega antes de o parser olhar, e o cabeçalho e as linhas já estão no leitor; fechar ali perdia as linhas sem contagem (revisão independente de 2026-10-14, A1). A regra é "fechando, e com erro ou, em `required`, sem cabeçalho". A regra do `go-relplog-server` — "sem cabeçalho ou com
  erro, e fechando" — descartaria até 4 KiB (o leitor, §5.4) dessas linhas sem
  contagem a cada `Close`: no RELP nada é confirmado antes do cabeçalho, aqui a
  janela TCP já as tinha aceitado. `LOCAL`, `UNSPEC`, `UNIX`, `DGRAM` e
  `UNKNOWN` contam como `local`.
- **Prazo vencido no meio da assinatura** (comportamento conhecido, sem perda):
  se `proxy.header_timeout` vence quando só o começo da assinatura chegou (de 1
  a 5 bytes do `PROXY ` do v1, de 1 a 11 dos 12 do v2, por exemplo `PRO`), o
  parser não tem como saber se é um cabeçalho e devolve "sem cabeçalho": conta
  `absent`; em `required` fecha (`reason="proxy"`), como para qualquer conexão
  sem cabeçalho; em `optional` os bytes ficam no leitor e viram o começo da
  primeira linha, registrada como qualquer linha que começa com `P` (em geral
  `malformed`). Nada do que chegou se perde, e só acontece com um balanceador
  **confiável** que mandou o começo da assinatura e parou por `header_timeout`.
  Igual no `go-syslog-server`; aceito como está (2026-10-14).
- **Efeito no payload:** `host.src-*` e `host.dst-*` vêm do cabeçalho (o
  cliente e o endereço que ele discou), o `sid` é calculado com eles, e o par TCP
  real vai em **`x-proxy: {"peer-ip", "peer-port"}`**, para que a ponta real do
  transporte continue registrada (§20.8).
- **Ressalva:** `proxy.trusted` largo demais permite forjar `src-ip`.

---

## 5. Moldura

### 5.1 As duas molduras da RFC 6587

**Octet-counting** (§3.4.1 da RFC): `MSG-LEN SP SYSLOG-MSG`, com
`MSG-LEN = NONZERO-DIGIT *DIGIT`.

```
37 <34>Oct 11 22:14:15 server su: failed53 <13>1 2026-10-03T11:59:59.9Z ...
```

**Non-transparent** (§3.4.2): `SYSLOG-MSG TRAILER`, com o TRAILER sendo LF, CRLF
ou NUL. É a que a RFC chama de "not recommended" e a que todo mundo usa.

### 5.2 Autodetecção: por conexão, com confirmação (desvio D1)

`tcp.framing = auto` (padrão) decide **uma vez por conexão**, no primeiro frame:

| Os primeiros bytes | Decisão |
|---|---|
| casam `[1-9][0-9]{0,9} SP` (até 11 bytes de *peek*) | octet-counting |
| qualquer outra coisa (inclusive `<`, `0`, um dígito sem espaço) | non-transparent |

A skill manda decidir **por frame** (RFC 6587 §3.4.3: um emissor *pode* trocar
de moldura). Duas razões para o desvio, as duas reais:

1. **Reavaliar a cada frame embaralha um fluxo non-transparent.** Uma linha sem
   PRI no formato `123 texto` viraria um frame octet-counting de 123 octetos e
   engoliria as linhas seguintes (a armadilha registrada na standard).
2. **O dígito sozinho derruba a conexão.** O appender log4j do corpus manda
   `2026-09-13 10:00:00,123 ERROR ...` sem PRI; a regra da skill leria `2026` como
   MSG-LEN sem o espaço e fecharia a conexão a cada linha. A confirmação pelo
   espaço resolve o caso também no primeiro frame.

Um erro de leitura (prazo de ociosidade, reset, EOF) que chega **durante** a
decisão, com ao menos um byte lido, decide non-transparent: o que chegou é
entregue como o frame parcial que é (`123` se comporta como `abc`). Descartar os
dígitos lidos só porque ainda não deu para confirmar a moldura perderia dado.

O preço é não acompanhar um emissor que troque de moldura no meio da conexão, o
que nenhum emissor real conhecido faz. `tcp.framing = octet | lf` fixa a moldura
para quem conhece o emissor.

Num fluxo já decidido como octet-counting, um MSG-LEN que não confirma (zero à
esquerda, sem espaço em até 10 dígitos, não numérico) **fecha a conexão** com
`reason="malformed_frame"`: não há como ressincronizar, e procurar o próximo `<`
gravaria fragmentos como mensagens.

### 5.3 Non-transparent

- O frame termina no primeiro **LF** ou **NUL** (RFC 6587 §3.4.2). Todos os `CR`
  imediatamente antes do terminador são removidos (`CRLF`, emissores Windows).
- O terminador e os `CR` finais **não** fazem parte da mensagem: não entram em
  `raw` nem em `size`.
- Frame vazio (terminadores consecutivos, `\r\n` sozinho, `msg\0\n`, que é
  `msg` seguido de um frame vazio) é ignorado: não gera envelope e conta em
  `syslog_messages_dropped_total{reason="empty"}`.
- Conexão encerrada sem terminador: o último frame é entregue **completo**, sem
  `truncated`.

### 5.4 Octet-counting

- Os `MSG-LEN` octetos depois do espaço são a SYSLOG-MSG, exatamente; nada é
  removido (se o emissor contou um `\n`, ele é dado, e a conversão o tira do fim
  de `message`, §6.7).
- O buffer **cresce com os bytes que chegam**, em blocos das classes do pool
  (§15.1), nunca pelo MSG-LEN declarado: quem declara 1 GiB e fica calado custa
  um bloco. Nunca alocar pelo que o par declara.
- Conexão encerrada no meio de um frame: entrega o que chegou, com `size` = o
  MSG-LEN declarado (portanto `truncated`, §5.5).
- **Custo fixo por conexão** (corrigido em 2026-10-14, como no
  `go-syslog-server`; vale para as duas molduras): o `bufio.Reader` de cada
  conexão TCP, alocado no accept, tem **4 KiB**, não 64. O maior `Peek` do
  código é a assinatura PROXY v2, de 12 bytes (a detecção de §5.2 espia onze; o
  v1 é lido byte a byte), e o frame atravessa o leitor uma recarga por vez,
  então um leitor maior só economiza `read`s (`BenchmarkTCPReceive`, TESTING
  §4). Antes de esperar o próximo frame (o leitor vazio), a moldura solta o
  buffer de frame que passou de 16 KiB — na prática o da classe de 64 KiB: as
  classes são 2, 8 e 64 KiB, então uma conexão ociosa segura **até 8 KiB** de
  buffer de frame — em vez de segurá-lo enquanto a conexão viver ociosa. Com o
  próximo frame já no leitor o buffer fica: soltá-lo depois de todo frame acima
  de 16 KiB realocaria a classe de 64 KiB a cada um numa sequência de frames de
  8–64 KiB (`BenchmarkFramer12K`; revisão independente do porte do
  `go-syslog-server`, 2026-10-13). Com `tcp.max_conns = 0` (o padrão), N
  conexões caladas custam cerca de N × 6 KiB de heap (leitor, estruturas da
  conexão), mais a pilha da goroutine de cada uma, **fora** de `ring.max_bytes`
  (§15.1): em produção, ligue `tcp.max_conns`.
- **Limite do "até 8 KiB": vale entre frames, com o leitor vazio** (revisão
  independente de 2026-10-14, A5 e D4; registrado, não corrigido). A soltura
  acontece na entrada de `Next`, só com `Buffered() == 0`. Um par que manda um
  frame de 17–64 KiB (o buffer vai à classe de 64 KiB) seguido de **um** byte
  do próximo e se cala passa por `Next` com `Buffered() == 1`: o buffer fica, a
  moldura copia o byte nele e estaciona no `Peek(1)` até o `idle_timeout`. 17
  KiB enviados seguram 68 KiB (64 + o leitor de 4), 4×, contra os 16× que a
  pendência 1 de 2026-10-11 fechou; não é perda de dado. Soltar no ponto de
  espera **dentro** do frame (o `Peek(1)` de `nextLF`/`readCounted`, copiando
  o pendente para uma classe pequena) foi medido e recusado: o leitor de 4 KiB
  esvazia no meio de todo frame acima de 4 KiB, a moldura não distingue "vai
  esperar" de "o próximo `read` já tem dados", e um fluxo contínuo de frames
  de 24 KiB foi de 111 µs para 1,12 ms e de 78 KB para 9,97 MB alocados por 64
  frames — a mesma regressão que a revisão do porte (2026-10-13) desfez. O
  contrário (D4) também é custo e não defeito: com um emissor que escreve um
  frame por `write()` e um leitor mais rápido, o leitor esvazia entre frames e
  frames de 17–64 KiB realocam a classe de 64 KiB a cada um — a taxa, nesse
  caso, é baixa por definição (o leitor espera o emissor). O teto do caso
  hostil é o mesmo das conexões caladas: `tcp.max_conns` e o
  `GOMEMLIMIT` (§15.4).

### 5.5 Tamanho e truncamento

`syslog.max_message_bytes` (env `SYSLOG_MAX_MESSAGE_BYTES`, padrão 64 KiB; a RFC
5424 §6.1 exige aceitar 480 e recomenda 2048):

- Uma SYSLOG-MSG maior que o limite é **truncada no fim** (RFC 5424 §6.1): o cru
  guarda os primeiros `max_message_bytes` octetos, e `size` guarda o tamanho
  original.
- O **excedente é consumido sem alocar**, para manter a moldura sincronizada:
  no octet-counting, `MSG-LEN − max` bytes são pulados; no non-transparent, tudo
  até o terminador é descartado, contando os bytes para `size` (sem os `CR`
  finais). **A conexão não é fechada por tamanho.**
- `size > len(cru)` é o que a conversão lê como truncamento: aviso `truncated`,
  e uma sequência UTF-8 cortada no fim sai de `message` sem aviso de encoding
  (§6.7). `raw` mantém os octetos recebidos.
- No UDP o datagrama inteiro é lido (65 536 bytes) e cortado em
  `max_message_bytes`; `size` = o tamanho do datagrama.
- `syslog_messages_truncated_total{transport}` conta.

O limite de 64 KiB é o que mantém o pior JLH abaixo de 1 MiB (§3.9).

### 5.6 Mensagem vazia

Um datagrama de 0 octetos ou um frame vazio **não gera envelope** (a skill: só
uma mensagem com ao menos um octeto gera envelope). Conta como chegada em
`syslog_messages_total` e como descarte em
`syslog_messages_dropped_total{reason="empty"}`, sem log. Uma conexão que fecha
depois de `N ` sem nenhum octeto entregue também termina como `empty`.

### 5.7 Backpressure: parar de ler

Ring cheio ou sink DOWN: o leitor do TCP **para de chamar `Read`**. Não fecha,
não descarta, não responde nada. O buffer de recepção do socket enche, a janela
anunciada vai a zero e o emissor para de transmitir; o `rsyslog` acumula na fila
dele, que é onde o dado deve ficar.

```
ring cheio detectado
  -> para de ler
  -> espera por tcp.backpressure_timeout (30 s), conferindo a cada 50 ms
  -> o ring escoou?  volta a ler; nada se perdeu
  -> estourou?       fecha a conexão (reason="ring_full" | "sink_down")
```

O fechamento existe porque um emissor bloqueado para sempre segura um
descritor, um buffer de kernel e uma vaga de `max_conns`. A mensagem que estava
na mão na hora do fechamento é contada como descartada; as que o emissor ainda
não enviou ficam com ele.

Uma retentativa dentro do laço **não conta de novo** a chegada: `Accept`,
`Retry` e `RecordDrop` são caminhos separados (bug real da standard). A
retentativa reavalia só o que pode mudar sozinho — o estado do sink e o espaço
no ring, primeiro pela pré-checagem sem alocação (§2.2) — e reusa o `uuid` já
cunhado (§3.1). (No pai, filtro e rate-limit
também decidiam uma vez por mensagem; os dois saíram, §1.2.)

**O fim do contexto não impede o `Enqueue`.** O `go-relplog-pg-server` deixa de
enfileirar uma transação cuja sessão acabou enquanto ela era montada, porque o
`librelp` retransmite o que nunca teve resposta e a cópia no ring seria
duplicata. Aqui não há resposta a reter: uma linha que este servidor **leu** já
saiu do emissor, e descartá-la na entrega ao ring seria perda, não
desduplicação. Por isso, com o contexto cancelado (o shutdown da fase 3 de
§13.4), o que foi lido termina de entrar no ring
(`TestIngestCancelledContextStillEnqueues`). Decisão do porte de 2026-10-13.

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
são aplicados como vierem, deslocando `payload.timestamp`, sem aviso, como faz o
conversor de referência da skill (na RFC 5424 o mesmo offset dá
`timestamp-invalid`, §6.4.2). Decidido seguir a skill, sem desvio novo (decisão
do `go-relplog-server`, portada pelo `go-syslog-server` e daqui em 2026-10-14,
DECISOES §9): um desvio de conversão quebraria a igualdade com o irmão RELP e
com o pai, que publicam o mesmo `type` no mesmo destino. Se a skill passar a
conferir, todos mudam juntos. `TestRFC3164ISOOffsetIsNotRangeChecked` trava as
duas metades.

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
timestamp reconhecível (um handshake TLS de um emissor configurado para RFC 5425,
requisição HTTP, lixo binário):

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
`syslog_events_dropped_total{reason="malformed"}` e log amostrado; o dump de
emergência a deixa de fora pela mesma regra (§12.1).

### 6.7 Texto e encoding

- **UTF-8 estrito** em todo texto (campos do cabeçalho, valores de SD,
  `message`): forma não mínima e surrogates são inválidos. **Cada octeto** que
  não inicia uma sequência válida vira **um** U+FFFD, com aviso
  `encoding-invalid`: o resultado de `utf8.DecodeRune` em laço. Nunca
  `strings.ToValidUTF8`, que junta uma sequência inválida num U+FFFD só e diverge
  da referência.
- **Charset legado** (decisão 2 do `go-syslog-pg-server`, herdada: fica): `syslog.legacy_charset = cp1252 | latin1`,
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
| `timestamp-invalid` | timestamp com forma reconhecida e data, hora ou offset inválidos (na RFC 3164 o offset não é conferido, §6.5.1) |
| `timestamp-year-inferred` | timestamp BSD sem ano |
| `timestamp-zone-inferred` | timestamp sem fuso |
| `hostname-missing` | RFC 3164 com timestamp e sem HOSTNAME |
| `sd-invalid` | STRUCTURED-DATA fora da gramática ou acima dos tetos |
| `sd-duplicate-id` | SD-ID repetido |
| `encoding-invalid` | octetos não UTF-8 no texto |
| `encoding-legacy` | `message` decodificada com o charset legado |
| `truncated` | mensagem acima do limite ou frame incompleto |

NILVALUE é válido e não gera aviso. Cada aviso emitido conta em
`syslog_convert_warnings_total{code}` (conjunto fechado de 14 valores).

### 6.10 `sid`

```
chave = transport "|" src-ip "|" src-port "|" dst-ip "|" dst-port
        (campos ausentes = texto vazio; IPs canônicos; portas em decimal)
sid   = FNV-1a-64(chave em ASCII) AND (2^53 − 1)          (unix: 0)

"udp|10.0.0.3|59922|10.0.0.10|514" → 593636897809901
```

Calculado com os endereços **do payload** (os do PROXY, quando houver), para que
qualquer consumidor o recalcule a partir de `payload.host` e
`payload.transport`. Agrupa os envelopes de uma conexão TCP (ou de um socket
UDP de origem), não é aleatório e cabe nos 53 bits que o JSON representa sem
perda.

### 6.11 Tetos de STRUCTURED-DATA

`syslog.sd_max_elements` (32) e `syslog.sd_max_params` (64 por elemento). A skill
não tem teto; sem ele, um emissor com dez mil parâmetros num elemento vira um
objeto de dez mil chaves no worker. Exceder um teto é tratado como SD inválido
(`sd-invalid`, SD omitido, texto em `message`): a memória fica limitada e nada
se perde, ao contrário da standard, que ignorava o excedente em silêncio.

---

## 7. Filtro

Removido no go-syslog-nats-server (§1.2; decisão 1 do `go-syslog-pg-server`, herdada): não há filtro por facility nem por severidade.

---

## 8. Rate-limit

Removido no go-syslog-nats-server (§1.2; decisão 1 do `go-syslog-pg-server`, herdada): não há rate-limit por prefixo de rede.

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

`Enqueue` só devolve `nil` quando o item está no ring. A confirmação (a
retomada da leitura no TCP) acontece depois desse retorno, nunca antes. O `Item`
carrega o cabeçalho (§3.4) e o cru; **não carrega envelope**. O `Touch` do pai
(renovação de posse no ring Redis) saiu com aquele ring.

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
  arredonda a struct para a classe de alocação antes de comparar. No pai eram
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
pai). "Um lote retido não está no ring" (§10.2) diz respeito ao que o `Drain`
do shutdown devolve, não à contabilidade.

Qualquer um estourando satura o ring, com o efeito por transporte de §2.3.
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

Removido no go-syslog-nats-server (§1.2): o ring é só memória.

### 9.5 `ring.mode = redis`

Removido no go-syslog-nats-server (§1.2): o ring é só memória.

### 9.6 `ring.mode = none`

Removido no go-syslog-nats-server (§1.2): o ring é só memória, e a conversão nunca roda na recepção.

### 9.7 Combinações que degradam ou recusam

Removido no go-syslog-nats-server (§1.2): há uma combinação só, `memory` → `nats`.

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
do envelope, herança das colunas da fila do `go-syslog-pg-server`; o sink NATS
não os lê.

Os workers de dreno (`sink.workers`, padrão **8**, o do pai) dividem a **única**
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
   aos pools, `syslog_events_written_total += n`.
5. Próximo pedaço.

O teto de bytes existe porque o JLH chega a 12× o cru: `workers × batch × pior
JLH` passaria de 1 GiB.

A entrega fora dos workers — a reingestão do boot (§12.2) — passa por
`sink.Instrumented.Deliver`, que corta os envelopes em pedaços pelas mesmas
duas regras e aplica as classes de §10.2 pedaço a pedaço. Como nos workers,
`syslog_sink_batch_size` e `syslog_sink_batch_bytes` são observados uma vez por
pedaço, não por tentativa (um pedaço retentado puxaria os histogramas para o
que o destino estava recusando), e uma escrita cortada pelo contexto de quem
chama (um `SIGTERM` no boot) não conta como erro do sink (§14.1).

### 10.2 Falha de pedaço, e o outbox

O worker **segura o pedaço** e retenta com backoff (`sink.retry_backoff_min`
100 ms → `retry_backoff_max` 2 s), **sem puxar mais nada do ring**. O ring para de
escoar e enche, até o backpressure de §2.3 aparecer.

- **O retry reenvia os mesmos bytes**, com o mesmo `uuid`: o JLH já serializado é
  reaproveitado, nunca refeito. É a regra do outbox do JLH, e poupa CPU no
  momento em que o sistema está em dificuldade.
- **Um lote retido não está no ring.** O shutdown o coleta de
  `Workers.Pending()`, ou ele some depois de confirmado. E o `hold` faz `append`,
  nunca atribuição: um worker que abandona o lote no shutdown o mantém registrado,
  e um `Dequeue` posterior não pode sobrescrevê-lo (bug real do irmão).
- **At-least-once.** Uma escrita que estourou o prazo pode ter sido aceita, e o
  retry a reenvia. Em `jetstream` a deduplicação é do stream: o `Nats-Msg-Id`
  (`<uuid>.0`) repetido dentro da `duplicate_window` é confirmado como
  duplicata e guardado uma vez só, e a confirmação com `Duplicate` conta em
  `syslog_sink_nats_duplicates_total`. Fora da janela, uma redelivery duplica.
  Em `core` não há deduplicação: o consumidor deduplica por `(uuid, step)`, como
  o JLH manda.
- **Contaminação** (§15.2): uma tentativa que termina por prazo ou cancelamento,
  ou qualquer erro depois da primeira publicação assíncrona, marca o pedaço, e
  nem o cru nem o buffer do pedaço voltam ao pool, nem quando a retentativa
  seguinte tem sucesso.

| Classe | Efeito |
|---|---|
| `ClassNetwork` | retenta; não muda o estado de saúde (§13.3) |
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`syslog_events_dropped_total{reason="sink_logic"}`), confirmado no ring |
| `ClassFatal` | FATAL imediato |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`syslog_events_written_total`, só os demais em `sink_logic`, e o log traz só os
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

### 10.3 `sink.type = redis`

Removido no go-syslog-nats-server (§1.2). A regra que estava aqui e que o código ainda cita — uma resposta **fatal** à sonda de saúde é FATAL na primeira checagem, sem passar pelos fusíveis — vale para o NATS e está em §13.3.

### 10.4 `sink.type = sqlite`

Removido no go-syslog-nats-server (§1.2).

### 10.5 `sink.type = postgres`

Removido no go-syslog-nats-server (§1.2): a fila `loghub.inbox` é o destino do `go-syslog-pg-server`.

### 10.6 `sink.type = mysql`

Removido no go-syslog-nats-server (§1.2).

### 10.7 `sink.type = nats` (padrão)

O único destino (não há chave `sink.type`, §1.2). `github.com/nats-io/nats.go`
com o pacote `jetstream` (a API atual). **Uma conexão por processo**,
multiplexada pelos workers e pelo beacon (§11), aberta preguiçosamente pela
primeira checagem de saúde; o `WaitReady` do boot é o laço de retentativa
(§13.1 passo 7). O nome do cliente é `slnats/<AgentName>@<Hostname>`, visível
no `connz` do monitor do `nats-server`. A discagem tem o prazo de
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
TCP falha; a recusa do `TLSCertCB` acontece depois, no *handshake*, e fica só no
`LastError`, com o estado `RECONNECTING`). Por isso o `Health`, ao ver o cliente
`RECONNECTING`, lê o `LastError` (`noteLastError`, `escalate`) e faz dele o
veredito, FATAL nomeando o TLS. Antes, a checagem dizia "connection is
RECONNECTING", `network`, e o FATAL vinha dos fusíveis, com o motivo errado — ou
nunca, com os dois fusíveis desligados (revisão independente do
`go-relplog-nats-server`, 2026-10-13, portada;
`TestNATSReconnectToATLSOnlyServerIsAVerdict`).

**Servidor que exige TLS é veredito — se não houver outro servidor** (porte do
RP/RN, 2026-10-14, a regra do `soleServer` do `go-syslog-server` e do
`go-relplog-server`). O `LastError` é o do **último servidor tentado**: com um
*pool* — várias `sink.nats.urls`, ou servidores que o cluster anunciou
(`nc.Servers()` inclui os descobertos) — o cliente alterna entre eles enquanto o
que serve está fora, e a recusa de TLS de um nó virava FATAL, dump e exit 3,
embora o outro voltasse logo depois. Por isso `errNATSServerRequiresTLS` só
vira veredito quando o *pool* tem **um** servidor (no `noteLastError` e no
`ReconnectErrHandler`); com mais, é `network` com o motivo no erro do `Health`
("a server of the pool requires TLS"), e se o *pool* nunca se recupera os
fusíveis de §13.3 dão o FATAL nomeando o TLS. No boot, a mesma regra: o
`nats.Connect` tenta todas as URLs e devolve o erro da última, e com mais de
uma URL a recusa é `network`, esperada até o `boot_timeout`; com uma só, é
exit 3 na hora (`TestNATSOneTLSServerOfAPoolIsNoVerdict`,
`TestNATSBootTLSRefusalOfAPoolIsNetwork`).

**Pool só de servidores que recusam** (registrado em 2026-10-14, D2 da revisão do syslog): com duas ou mais URLs e **todas** exigindo TLS, nunca há veredito — cada checagem é `network`, com o motivo no erro do `Health`, e o FATAL só vem dos fusíveis de §13.3; com os dois fusíveis desligados, nunca vem. É consequência da regra acima: o `LastError` é o do último servidor tentado, e saber que **nenhum** nó vai servir exigiria acompanhar a volta inteira do pool. O sinal é `syslog_sink_up` em 0 e o `/readyz`.

**Uma URL só cujo nome resolve a vários IPs** (registrado em 2026-10-14, D3 da revisão do RELP): o `nats.go` resolve o nome e tenta um IP, mas `nc.Servers()` tem uma entrada só, então a recusa de TLS do IP sorteado é veredito FATAL, embora outro IP do mesmo nome pudesse servir (um serviço *headless* do Kubernetes com nós configurados de jeitos diferentes). Não há como distinguir sem `SkipHostLookup`, que tiraria o rodízio entre os IPs: é limite aceito — os nós atrás de um nome devem ter a mesma configuração de TLS, ou entram como URLs separadas, e a regra do *pool* vale.

**Uma discagem que falha na reconexão não é logada** (porte do RP/RN,
2026-10-14; a forma do `go-syslog-server`): o `WARN` de desconexão, o
`/readyz` e `syslog_sink_up` já dizem o estado, e um `ERROR` amostrado a cada
30 s durante uma queda longa só repetia isso. O `ReconnectErrHandler`
(`onReconnectErr`) só põe e loga (`ERROR`) o veredito acima — o servidor único
que exige TLS, num cliente que o relate ali
(`TestNATSReconnectDialFailureIsNotLogged`).

**Conexão:** `MaxReconnects(-1)` (os fusíveis de §13.3 decidem quando desistir;
um cliente que se fechasse sozinho depois de N tentativas transformaria uma
queda longa num estado irrecuperável, sem dump), `ReconnectWait(500ms)` e
**`ReconnectBufSize(-1)`**: durante uma desconexão, publicar falha na hora
(`network`) em vez de guardar mensagens dentro do cliente. O ring é o único
buffer; uma fila escondida no cliente escaparia da contabilidade, do
backpressure e do dump (§20.19). A desconexão é logada em `WARN` (a do próprio
`Close` não); a reconexão conta em `syslog_sink_nats_reconnects_total`, é
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
começarem). A folga existe porque os futuros de um pedaço contaminado
continuam contando até o próprio prazo assíncrono, que começa na publicação,
um pouco depois do prazo do worker: sem ela, uma retentativa rápida parava na
janela cheia e falhava com `ErrTooManyStalledMsgs` (revisão independente do
`go-relplog-nats-server`, 2026-10-13, portada). Com os tetos de §13.2
(`sink.workers` ≤ 256, `sink.batch` ≤ 65 536) o número fica em no máximo
33 554 432, um `int` positivo em qualquer plataforma. `Data` aponta para o buffer do pedaço, que só volta ao pool depois
de todos os futuros resolverem; qualquer erro depois da primeira publicação
deixa o buffer para o GC (§15.2).

Cada confirmação com `PubAck.Duplicate` conta em
`syslog_sink_nats_duplicates_total`. Um futuro com erro `network` ou `fatal`
encerra a espera na hora (o pedaço volta inteiro, contaminado); um com erro
`logic` não: os demais dizem o que o stream guardou (§10.2, entrega parcial).

**`sink.nats.mode = core`** (opt-in, *best effort*): `PublishMsg` por envelope e
um *flush* por pedaço, sob o prazo do worker (ou `FlushTimeout(sink.op_timeout)`
sem prazo). O cliente copia `Data` para o buffer de escrita dentro do
`PublishMsg`, então nada lê o buffer do pedaço depois do `Write`. O sucesso
significa que o servidor NATS **recebeu** as mensagens, não que alguém as
guardou: sem stream, uma mensagem sem assinante some, e não há deduplicação.
Combina com o UDP, que já é lossy. O boot registra `WARN` com essa frase, e a
primeira checagem um `INFO` com a maior mensagem e o `max_payload`.

**Um *flush* que atravessa uma reconexão não confirma** (`core`). O `nats.go`
falha um *flush* pendente com `ErrConnectionClosed` quando a conexão **cai**,
embora o cliente só esteja reconectando: o `flushErr` o classifica como
`network` enquanto a conexão não está `CLOSED` (no `Write` e no `Health`); antes
era `fatal`, e um soluço do NATS no meio do *flush* era FATAL, dump e exit 3. E
um *flush* que **começa** durante a reconexão é resolvido pelo `PONG` do servidor
novo, embora os `PUB` do pedaço tenham ido pelo socket velho: o `writeCore`
compara o número de reconexões antes da primeira publicação e depois do
*flush*, e se ele mudou o pedaço é `network`, não confirmado (portados do
`go-relplog-nats-server`, 2026-10-13).

**Em `core`, um envelope de outro `space`** (um dump de outra vida) gera um
`WARN` por `space` estrangeiro, uma vez: sem stream, ele se perde se ninguém
assinar o subject. A comparação é entre o `space` do item e o Workspace; o
subject próprio é montado uma vez, sem nada no caminho quente.

**Subject** (§3.8): sempre `loghub.<space>.syslog`, derivado do `space` do
item. Não há `sink.nats.subjects` (§1.2, decisão 4).

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
| A retenção guarda o que confirma | `retention: interest` é FATAL: sem consumidor interessado, o servidor responde com `PubAck` normal e não guarda nada, e o pedaço seria confirmado e perdido. `limits` e `workqueue` são aceitos (portado do `go-relplog-nats-server`, 2026-10-13) |
| Teto por subject | `max_msgs_per_subject > 0` só gera `WARN`, uma vez por valor: todo evento de um `space` divide um subject, e os mais antigos são descartados conforme os novos são confirmados |

Com tudo aceito, o log traz `stream validated` com o stream, os subjects, o
`storage`, as réplicas, a `duplicate_window`, a maior mensagem e o
`max_payload`, uma vez (a primeira validação; as seguintes só falam quando
recusam). Com `core`, só o `max_payload` é conferido, também em toda checagem:
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
| `10077` cuja descrição é de um stream **cheio** com `discard: new` ("maximum messages exceeded", "maximum bytes exceeded", "maximum messages per subject exceeded") ou **fechando** ("store is closed", o `ErrStoreClosed` de `server/store.go` do nats-server 2.14.6, num restart do NATS; só "store closed", texto que nenhum servidor manda, era conferido, e o 10077 de um store fechando era `fatal` — revisão independente do `go-syslog-server`, 2026-10-13, C2) | `network`: backpressure, passa quando um consumidor confirma ou o servidor volta (revisão independente do `go-relplog-server`, portada; como `fatal` eram dump, exit 3 e duplicatas na reingestão) |
| `10158` (409: o mesmo `Nats-Msg-Id` ainda numa proposta Raft, na retentativa depois de um prazo num stream em cluster), `10167` (429: a fila de entrada do stream cheia) e todo outro erro da API com código 429 | `network`: passam sozinhos (antes, "outro 4xx" os fazia FATAL, dump e exit 3, talvez na frota inteira) |
| `ErrAuthorization`, `ErrAuthExpired`, `ErrAuthRevoked`, `ErrAccountAuthExpired`, e o texto do servidor no `CONNECT` com "authorization violation", "authentication expired" ou "authentication revoked" (o `errors.Is` do `nats.go` compara o texto inteiro, e "User Authentication Expired" nunca casava com o sentinela: o boot esperava o `boot_timeout` e saía com o motivo errado), `ErrConnectionClosed` com a conexão `CLOSED` (só reconectando, `network`: o `flushErr` acima), servidor que exige TLS (`errNATSServerRequiresTLS`), `ErrStreamNotFound` (`10059`), JetStream desligado (`10076`, `10039`), stream esperado não confere (`10060`), selado (`10109`), recursos (`10023`), outro armazenamento (`10077`: um registro que o store não aceita) | **`fatal`** |
| `ErrMaxPayload` (recusada no cliente: só aquela mensagem, §10.2), `ErrMaxBytesExceeded`, `10054`, `10097` | `logic` |
| violação de permissão (`ErrPermissionViolation`) numa publicação em `loghub.<space>.syslog` (de qualquer `space`), na API `$JS.API` ou na assinatura de `_INBOX.`, vista pelo `ErrorHandler` | **`fatal`**, pelo veredito (abaixo) |
| outro erro da API com código 4xx | `fatal` |
| outro erro da API com código 5xx, qualquer outro | `network` |

**Veredito.** O cliente só informa algumas recusas definitivas de forma
**assíncrona**: uma publicação negada por permissão não volta como erro dela (em
`jetstream` o futuro estoura o prazo e seria retentado para sempre com o sink
UP; em `core` a mensagem some e contava como escrita), e um `$JS.API.STREAM.INFO`
negado segurava o boot até o `boot_timeout`. Por isso o sink guarda um
**veredito** quando o `ErrorHandler` vê `ErrPermissionViolation` numa
publicação em `loghub.<space>.syslog` ou na `$JS.API`, e o `Health()` e **todo
`Write`**, nos dois modos, o devolvem como **`fatal`**: dump e exit 3 (§13.3).
Em `core` o `Write` não espera o `ErrorHandler`: o servidor manda o `-ERR`
antes do `PONG` do *flush*, então o `LastError()` da conexão já o tem quando o
*flush* volta, sem corrida, e o pedaço que encontrou a negação volta `fatal` e
fica retido para o dump. Antes, esse pedaço era confirmado, contado como escrito
e perdido, e assim todos os seguintes até a próxima checagem de saúde. Uma
negação no subject do **beacon não é veredito**: o plano de controle não pode derrubar o plano de dados (§11). O
`ReconnectErrHandler` não transforma em veredito uma discagem recusada (é
`network`, e nem é logada); a recusa do TLS, que vem depois, no *handshake*, é
lida do `LastError`, e só é veredito com um servidor no *pool* (acima).

A negação de **assinatura em `_INBOX.`**, por onde voltam os `PubAck` e as
respostas da API, também é veredito: sem ela nenhuma confirmação chega, e o
`StreamInfo` estourava o prazo como `network` até o `boot_timeout`. Em
`jetstream`, o prazo do futuro de um publish negado vira o veredito, com o
pedaço contaminado: o primeiro pedaço negado é fatal só depois do
`sink.op_timeout`; em `core`, na hora.

**Negação mascarada (`core`).** O `nats.go` guarda só o **último** `-ERR`. Uma
negação do subject do beacon que chega entre os `PUB` de um pedaço e o `PING`
do *flush* esconde uma negação do caminho de dados que veio antes dela, e o
pedaço seria confirmado embora o servidor o tivesse recusado. Uma negação
**nova** durante o pedaço que não é do caminho de dados (`maskedDenial`) torna o
pedaço incerto: `network`, não confirmado; a retentativa encontra o veredito
que o `ErrorHandler` já gravou, ou passa (revisão independente do
`go-relplog-server`, portada do `go-relplog-nats-server`).

O `ErrMaxBytesExceeded` só existe em consumidores *pull*: o ramo dele na
classificação é inalcançável aqui e fica por simetria com o pai.

O `10097` (headers acima de 64 KiB) é inalcançável daqui, porque o bloco de
headers é fixo e pequeno (§3.8); é `logic` e não o `fatal` de um 4xx
desconhecido porque, se acontecesse, seria sobre aquela mensagem, não sobre o
stream. Erro desconhecido é `network`, nunca `logic` (§20.18): um ring que enche
é visível, um pedaço descartado não. Esta tabela e `TestClassifyNATS` mudam
juntas.

**Autenticação:** uma de `username`/`password`, `token` ou `credentials`
(arquivo `.creds`, JWT + semente NKey), e só uma: mais de uma é exit 1, assim
como `password` sem `username`. Um `credentials` que não existe é exit 1 ao
montar o sink (§13.1 passo 7): a biblioteca só leria o arquivo ao conectar, e o
caminho errado viraria um laço de retentativa do boot. **Sem TLS**: não há
`tls`, `tls_ca` nem `tls_insecure`.

**Fechamento:** `Close`, não `Drain`: os workers param antes de o sink fechar
(§13.4), então não há nada a esvaziar.

### 10.8 `sink.type = none`

Removido no go-syslog-nats-server (§1.2): todo pedaço vai ao NATS; não há destino de benchmark.

### 10.9 `LOGHUB_EB_URL`

`loghub.eb_url` (env `LOGHUB_EB_URL`) é a URL do eventbus da convenção LogHub.
Aqui o único eventbus aceito é o NATS **sem TLS**: a URL preenche
`sink.nats.urls`.

| Esquema | Efeito |
|---|---|
| `nats://` (uma lista separada por vírgula é o cluster do `nats.go`) | preenche `sink.nats.urls` com a URL inteira, usuário e senha (ou token) inclusive, que o cliente lê dali |
| `tls://` | **exit 1**, com uma mensagem que nomeia o TLS: este servidor não fala TLS com o NATS (§1.2); use `nats://` numa rede confiável |
| outro (`postgres://`, `redis://`, `amqp://`...) | exit 1: não suportado. Uma URL de outro eventbus esquecida de uma implantação irmã derruba o boot em vez de ser ignorada |

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
senha até a primeira `/`, `#`, `?` ou `,`). O
`--dump-config` mostra os valores derivados como comentário, com o *userinfo*
redigido (`nats://usuario:***@host`; um usuário sem senha pode ser um token e é
redigido inteiro), para que o dump relido não dispare o conflito.

**Recusas portadas do `go-relplog-server` e do `go-relplog-nats-server`**
(2026-10-13), exit 1 sem citar a URL, aqui e em `sink.nats.urls`: um `@` depois
do primeiro `/`, `?` ou `#` da autoridade (um usuário ou senha com um desses
caracteres sem codificar encurta a autoridade: `nats://app:39182#Zs3@n1` discava
`app:39182` sem senha), com a dica de `%2F`, `%3F` e `%23`; um host com escape
(`%`), com a mensagem de que um host NATS é um nome ou um IP literal, em vez de
"invalid percent-escape"; uma entrada sem esquema depois da primeira, com a
dica de que uma `,` no usuário, na senha ou no token parte a lista; e a
`LOGHUB_EB_URL` só com espaços (definida e vazia de conteúdo: o operador que
quis desligá-la não saberia que as chaves `[sink.nats]` é que valem; vazia
continua "não definida"). O esquema recusado é citado com o `://`
(`"tls://"`), e a falta dele como "no scheme".

### 10.10 `sink.type = clickhouse`

Removido no go-syslog-nats-server (§1.2).

#### 10.10.1 Cliente

Removido no go-syslog-nats-server (§10.10).

#### 10.10.2 Concorrência e conexões

Removido no go-syslog-nats-server (§10.10).

#### 10.10.3 Escrita

Removido no go-syslog-nats-server (§10.10).

#### 10.10.4 Desduplicação e at-least-once

Removido no go-syslog-nats-server (§10.10).

#### 10.10.5 Validação no boot

Removido no go-syslog-nats-server (§10.10).

#### 10.10.6 Durabilidade e `async_insert`: decisão do administrador do ClickHouse

Removido no go-syslog-nats-server (§10.10).

#### 10.10.7 Classificação de erro

Removido no go-syslog-nats-server (§10.10).

#### 10.10.8 `Health()`

Removido no go-syslog-nats-server (§10.10).

#### 10.10.9 Retenção

Removido no go-syslog-nats-server (§10.10).

#### 10.10.10 Métricas

Removido no go-syslog-nats-server (§10.10).

#### 10.10.11 Autenticação e TLS

Removido no go-syslog-nats-server (§10.10).

---

## 11. Plano de controle: heartbeat beacon

Decisão 1 da entrevista (fica; a opção recomendada). O servidor é um driver de
si mesmo (não existe `go-loghub-driver`), então emite o heartbeat que a skill
exige de todo worker, e **só** ele. Como o único sink é o NATS, o beacon sai
**sempre**, nos dois modos.

- **Quando:** na primeira conexão, em toda reconexão (o `ReconnectHandler` do
  `nats.go`) e a cada `loghub.beacon_interval` (env `LOGHUB_BEACON_INTERVAL`;
  inteiro puro em segundos, como a skill o define, ou duração do Go; padrão
  `15`; `0` desliga o periódico, não o de conexão; acima de zero, no mínimo
  `1s`: um `1ns` girava o laço do beacon, e é exit 1).
- **A primeira conexão não perde o beacon.** O gancho é registrado no sink
  **antes** de o `WaitReady` conectar (§13.1 passo 7); o pedido fica guardado
  (um só: pedidos que chegam enquanto um beacon está saindo se fundem no
  próximo) e o beacon sai quando a goroutine começa, no passo 10. O gancho nunca
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
  | `meta` | `[{"key":"version","value":<versão>},{"key":"ring","value":"memory"},{"key":"sink","value":"nats"},{"key":"transports","value":[...]}]`, os transportes ligados na ordem `unix`, `udp`, `tcp` |

- Falha de publicação (inclusive "não conectado") é contada
  (`syslog_beacons_total{result="error"}`) e logada em `WARN` com amostragem, um
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

**JSON Lines: um envelope JLH pronto por linha** (decisão 3 do `go-syslog-pg-server`,
herdada; lá, a opção recomendada):

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
  `syslog_events_dropped_total{reason="malformed"}`. Se todas forem recusadas,
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
  o dump, de propósito (§12.3).
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
| arquivo limpo | removido (com `fsync` do diretório) **só depois** de todos os seus envelopes serem confirmados. Uma falha da remoção é só `ERROR`: o arquivo volta no boot seguinte e é republicado, e o stream só o absorve como duplicata dentro da `duplicate_window` (10 min no `stream.json`); depois dela, ou em `core`, os envelopes duplicam (TESTING §8). É regra, não pendência (porte do RP/RN, decidido em 2026-10-14): sair com erro deixaria o boot em laço por um problema (permissão do diretório) que não impede a entrega, e os gauges de §14.1, recontados do disco no fim da reingestão, continuam contando o arquivo, então o alerta segue disparando (`TestRemovalFailureIsOnlyAnError`). |

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
dentro de **um** `sink.boot_timeout` **sem progresso** para a reingestão
inteira (todo pedaço entregue empurra o prazo; um prazo único desde o início se
gastava com sucessos, e uma reingestão saudável de um dump grande que passasse
do `boot_timeout` saía com 3 no primeiro soluço, revisão independente do
`go-relplog-pg-server`, 2026-10-13, portada), e estourado sai com 3 **sem
remover o arquivo**; `fatal` sai com 3.

**Um dump de uma vida com outro Workspace** é publicado em
`loghub.<space da linha>.syslog`, que a validação do boot não conferiu (ela
prova só o subject do próprio `space`). Sem stream que o cubra, o JetStream não
responde e a entrega falha como `network` até `sink.boot_timeout`: o boot sai
com 3 e o arquivo fica. Coberto por outro stream, o `WithExpectStream` o recusa
(`10060`), fatal. O operador decide se cria o stream ou move o dump. Em `core`,
a mensagem vai para quem assinar aquele subject, ou para ninguém, com um `WARN`
por `space` estrangeiro, uma vez (§10.7).

**Os gauges dizem o que está no disco.** `syslog_emergency_files` é o número de
arquivos listados, posto antes do estado de boot que anuncia a reingestão;
`syslog_emergency_events_pending` não é tocado durante ela (posto no tamanho do
pedaço, a última leitura antes de um exit 3 mostrava um pedaço por um dump
inteiro). No fim, os dois são **recontados do diretório** (`Store.Count`, que
lê cada arquivo com o leitor da reingestão), nunca zerados: um arquivo ilegível,
ou cuja remoção ou quarentena falhou, continua pendente, contado como um arquivo
sem envelopes, e um alerta neles continua disparando. O dump do shutdown
**soma** aos gauges (pode haver um arquivo de antes no disco).

**Remoção e quarentena com o `fsync` do diretório falho.** `Remove` e
`Quarantine` devolvem `emergency.ErrNotDurable` quando o `unlink` ou o `rename`
aconteceu e só o `fsync` do diretório depois dele falhou (inclusive ao abrir o
diretório): o boot loga `WARN` (o nome pode voltar depois de uma queda de
energia, e o dump seria publicado de novo, deduplicado pelo stream só dentro da
`duplicate_window`) e, na quarentena, ainda o `ERROR` com
as contagens de bytes sob o nome novo. Antes, "cannot remove … it will be
delivered again" para um arquivo que já não existia, ou "cannot quarantine"
com o caminho antigo e sem as contagens.

### 12.3 Falha do próprio dump

Disco cheio, sem permissão, `fsync` recusado, `emergency.enabled = false`:
`ERROR` com a contagem exata, `syslog_events_dropped_total{reason="emergency_failed"} += n`,
exit 3. `emergency.dir` é criado **no boot**, com uma escrita de prova: um
diretório criado durante o incidente falha durante o incidente, e um problema
de permissão achado no dump é uma mensagem perdida em vez de um boot recusado.

**Um dump que o abort já contou como perdido desiste** (`Store.SetAbandon`): a
escrita confere o abort entre linhas (a cada 1024) e antes de publicar, e
remove o temporário em vez de deixar no disco um arquivo que o boot seguinte
reingeriria. Um dump publicado entre a última conferência e o abort fica contado
como perdido **e** guardado: sobra, nunca perda, e nada mais é contado (§13.4).
O boot seguinte o entrega, e `reason="abort"` sobreconta. É regra, não
pendência (porte do RP/RN, decidido em 2026-10-14): o aborto **não espera o
disco** — segurar o `lossMu` durante o `link` e o `fsync` do diretório faria um
`SIGQUIT`, cujo contrato é "agora", esperar um disco que pode estar preso; a
janela é inerente a qualquer `os.Exit` assíncrono a uma escrita.

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
    sink.boot_timeout para tudo (sink.Instrumented.Deliver): `logic` descarta, conta e loga aquele
    pedaço e segue para o próximo; `network` reenvia o MESMO pedaço com
    backoff dentro de sink.boot_timeout; `fatal` sai com 3; um sinal sai
    com 130. Depois: inicia a saúde e os workers
10. beacon: a goroutine começa, e o beacon da primeira conexão (pedido no
    passo 7) sai agora (§11)
11. abre os transportes: unix, udp, tcp
12. /readyz 200
```

Um sinal que cancela o `bind` da porta admin (passo 5) ou de um transporte
(passo 11) — a resolução de um nome de host falha com "operation was
canceled" — é um boot interrompido, **130**, não um endereço ruim (1) (revisão
independente do `go-relplog-pg-server`, 2026-10-13, portada).
Com IP literal ou socket unix nenhuma resolução falha e os `bind` passam: o
sinal é conferido depois de todos os `bind` do passo 11 e antes do primeiro
`Start`, e também é **130** (antes o servidor abria, o `/readyz` dava 200 e o
shutdown que seguia saía 0; revisão independente do `go-syslog-server`,
2026-10-13, A1; `TestSignalBeforeALiteralBindExits130`).

**Identidade antes de tudo que escreve.** `lhident.Initialize()` roda **uma
vez**, no `main`, depois das flags que encerram o programa e antes de abrir
qualquer recurso com `defer` ou criar goroutine (regra 2 da skill). A app recebe
um struct `Identity` (`From`, `Space`, `AgentName`, `Hostname`, `MachineID`)
preenchido dos getters: os testes montam a app dezenas de vezes no mesmo
processo, e uma segunda chamada a `Initialize()` sai com 112. `AgentName` e
`Hostname` compõem o nome do cliente NATS, `slnats/<AgentName>@<Hostname>`
(§10.7), e o payload do beacon (§11).

**`DATADIR` é exportado antes do `Initialize()`** para que a biblioteca grave
`agent_uuid` e `machine_id` na mesma raiz em que o servidor grava `emergency/`
(§16.6). Sem `DATADIR` absoluto, o padrão `./data` vira absoluto a partir do
diretório de trabalho; no contêiner é `/data`.

**A porta admin abre antes do sink:** num crashloop por NATS indisponível ou
stream ausente, o operador tem `/readyz` com o motivo e `/metrics`.

**Os transportes abrem por último e juntos.** Abrir o UDP antes de o sink estar
pronto e de os dumps terem voltado descartaria datagramas sem ninguém saber; a
única defesa do UDP é não estar escutando.

### 13.2 Validação de configuração

| Combinação | Efeito |
|---|---|
| Nenhum `listen.*` | exit 1 |
| `listen.udp` ou `listen.tcp` fora de `host:port` (`:1514` serve); `*_listeners` fora de 1–256 | exit 1. UDP e TCP podem dividir a porta (são protocolos distintos; 1514 nos dois é o padrão) |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| `admin.enabled = true` com `admin.bind` e `listen.tcp` no mesmo endereço TCP: a mesma porta com o mesmo literal IP (o IPv4 mapeado em IPv6 conta como o IPv4), um curinga (`""` ou um literal não especificado: `0.0.0.0`, `::`, `::0`) de um lado, ou um **nome** de host de qualquer lado | exit 1, nomeando os dois endereços. Nenhum nome é resolvido na validação: um nome na mesma porta pode resolver ao outro endereço (`localhost` e `127.0.0.1`) e conta como colisão. Para dividir a porta, dois literais IP distintos; a porta `0` nunca colide; `listen.udp` não colide (outro protocolo). Antes a colisão só aparecia ao abrir o transporte, com a porta admin já tendo servido o boot inteiro, e no macOS/BSD nem isso: o `SO_REUSEADDR` deixa `127.0.0.1:P` e `*:P` coexistirem, e a porta admin tomava em silêncio as conexões TCP de `localhost`. A regra é a do `sameStreamSocket` do `go-syslog-server` (porte do RP/RN, 2026-10-14; `TestAdminBindCollidesWithListen`). O curinga é reconhecido pelo **valor**, não pela grafia: `""` ou qualquer literal com `IsUnspecified()` (`0.0.0.0`, `::`, `::0`, `0:0::0`); e a **porta é comparada como número**, lida como o `net.Listen` a lê (`net.LookupPort`): `0514` é `514`, e um nome de serviço (`syslog`, `http`) vale o número do banco de serviços; uma porta que não é número nem serviço conhecido é exit 1 aqui, nomeando as duas chaves, em vez de falhar no bind (revisão independente de 2026-10-14, D1 do syslog e D4 do RELP: `[::0]:P` contra `127.0.0.1:P` e `0514` contra `514` passavam como endereços distintos; `TestSameStreamSocketReadsValuesNotSpellings`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `listen.unix` relativo ou maior que `sun_path`; `listen.unix_mode = 0` | exit 1 |
| `proxy.protocol ≠ off` sem `proxy.trusted` | exit 1 |
| `space` fora de `^[a-z0-9-]+(\.[a-z0-9-]+)*$` | exit 1 (a lib aceita `a..b`, `.x` e `x.`; segmento vazio não serve à convenção LogHub, e viraria um token vazio no subject). Acima de 64 caracteres a própria lib já sai com 111 antes |
| `from` fora do UUID canônico em minúsculas | exit 1 |
| `syslog.default_priority` fora de 0–191 | exit 1 |
| `syslog.rfc3164_timezone` desconhecido para o `time.LoadLocation`, ou `Local` | exit 1 (a imagem embute `time/tzdata`; `Local` dependeria do host) |
| `syslog.legacy_charset` fora de `""`, `cp1252`, `latin1`, `windows-1252`, `iso-8859-1` | exit 1 |
| `syslog.max_message_bytes` < 480 ou > 16 MiB; `sd_max_elements` ou `sd_max_params` < 1 | exit 1 (480 é o mínimo da RFC 5424) |
| `ring.max_bytes` abaixo do custo de uma mensagem do tamanho máximo: a classe do pool de `syslog.max_message_bytes` + 256 B (`item.PooledCap(n) + ring.ItemOverhead`, §9.2, §15.1; 65 792 B com o padrão de 64 KiB) | exit 1, nomeando a chave e o piso: essa mensagem nunca caberia num ring vazio; no TCP a conexão ficaria parada até o `tcp.backpressure_timeout` e a mensagem seria descartada, a cada uma, e no UDP descartada sempre como `ring_full`, com o ring vazio. Porte do RP/RN, 2026-10-14 (`TestRingMaxBytesFitsTheLargestMessage`) |
| **Tetos** (revisões independentes do `go-relplog-pg-server` e do `go-relplog-nats-server`, 2026-10-13, portadas): `ring.capacity_items` > 16 777 216, `sink.workers` > 256 (todos dividem a única conexão NATS), `sink.batch` > 65 536, `sink.batch_max_bytes` > 256 MiB | exit 1. Sem eles, um erro de digitação de poucos dígitos derrubava o boot em pânico (`makechan`), alocava um gigabyte por `Dequeue` ou dava a volta no `int` do `PublishAsyncMaxPending` (`2 × workers × batch`, §10.7). Os números são folgados: só barram o erro de digitação |
| `ring.capacity_items`, `ring.max_bytes`, `sink.workers`, `sink.batch`, `sink.batch_max_bytes`, `sink.max_retries` abaixo de 1; `ring.high_water` fora de (0, 1]; `log.access_sample` fora de [0, 1]; `tcp.max_conns*` negativo; `retry_backoff_max < retry_backoff_min` | exit 1 |
| `sink.nats.urls` vazia, ou com uma entrada que não seja `nats://` (`tls://`, `ws://`, `wss://`, `host:porta` sem esquema) | exit 1, citando só o esquema, nunca a URL (§1.2) |
| uma entrada de `sink.nats.urls` que não é URL válida (um `%`, `/`, `#`, `?` ou `,` sem escape num usuário, senha ou token) ou sem host | exit 1, sem repetir **nada** da entrada — nem o trecho que o erro do `net/url` cita: `invalid port ":Sup3r" after host` é a senha até a primeira `/`, e ia para o stderr do contêiner — com a dica "percent-encode %, /, #, ? and , in a user, password or token". Uma entrada com caminho, consulta ou fragmento também é exit 1 (a regra da `LOGHUB_EB_URL`, §10.9): é o que pega o segredo que parseia — `nats://tok/en@host` é o host `tok` e um caminho, sem credencial nenhuma, e o erro de discagem do cliente citava esse host, o token até a barra, em cada log até o `boot_timeout`. Antes da revisão, a falha acontecia dentro do `nats.Connect`, como `network`, retentada até o `boot_timeout` e com a URL (e a senha) citada em cada linha de log |
| *userinfo* numa entrada de `sink.nats.urls` junto com `sink.nats.username`, `password`, `token` ou `credentials` | exit 1: o `nats.go` prefere em silêncio o da URL (a `LOGHUB_EB_URL` já recusava esse conflito, §10.9) |
| `loghub.beacon_interval` acima de zero e abaixo de `1s` | exit 1 (§11) |
| `sink.nats.mode = jetstream` com `sink.nats.stream` que não é nome de stream (vazio, espaço, ponto, curinga, barra) | exit 1 |
| mais de um de `credentials`, `token` e `username`; `password` sem `username` | exit 1 |
| `sink.nats.credentials` que não existe | exit 1 (ao montar o sink, §13.1 passo 7) |
| `loghub.ctl_namespace` vazio ou com um token não publicável | exit 1 (§11) |
| `sink.nats.mode = core` | `WARN`: *best effort*, sucesso é "o servidor recebeu", não "alguém guardou" (§10.7) |
| `LOGHUB_EB_URL` em conflito, com esquema que não é `nats://` (`tls://` com a mensagem do TLS), com entrada sem host, com caminho, consulta ou fragmento (§10.9) | exit 1 |
| durações zero onde zero não é "sem limite" (§16.5) | exit 1 |
| `GOMEMLIMIT` ausente, ou `< ring.max_bytes × 2` | `WARN` (§15.4) |
| parâmetro desconhecido na linha de comando ou no INI | exit 1. Isto inclui toda chave do pai que saiu (`ring.mode`, `sink.type`, `[tls]`, `[filter]`, `[ratelimit]`, `sink.nats.subjects`, `sink.nats.tls`, `tls_ca`, `tls_insecure`, `[sink.postgres]`...): um INI do `go-syslog-server` não sobe aqui em silêncio |
| linha do INI que não é `[seção]` nem `chave = valor` (um cabeçalho sem `]` inclusive); argumento da linha de comando que não começa com `-` | exit 1, nomeando o número da linha ou só o que vem antes do `=` do argumento, **nunca o conteúdo**: `password: x` (sem `=`) ou `sink.nats.password=x` (sem `--`) iam inteiros para o stderr do contêiner |
| flag sem valor seguida de outra flag (`--sink.workers --sink.nats.password=x`, um `$WORKERS` vazio num script); valor depois de um espaço (`--sink.nats.password= x`, `--sink.nats.password = x`, `--sink.nats.password= -x`); valor que o shell partiu num espaço (`--sink.nats.password=$P`, com espaço em `$P`); valor que começa com `--` na forma separada (`--sink.nats.password --x`) | exit 1, nunca com o valor: um argumento que começa com `--` (ou `-` com o nome de um parâmetro ou de um *switch*) nunca é tomado como valor de outra flag, e só é nomeado se for uma flag deste servidor; o argumento depois de um valor vazio ou de um `=` sozinho não é citado, nem como nome de flag desconhecida; uma palavra solta depois de qualquer valor não é citada. Um valor que começa com `-` vai depois do `=`. A cauda partida que começa com traço depois de um valor **não** vazio (`--sink.nats.password=a -b`) só é nomeada quando tem a forma `<seção>.<chave>` de uma seção que este servidor tem, ou que saiu do pai (`--sink.nats.subjet`, `--filter.min_severity`): um erro de digitação dentro de uma seção conhecida continua nomeado, e `-hunter2` ou `--x=y` não (`knownSectionOf`, do `go-relplog-server`, portado em 2026-10-13). Um `-h`/`-v` de um traço depois de uma flag que quer valor não é nomeado (é como um valor de dois caracteres começando com traço se parece); `--help` é (o parser do `go-webhook-nats-server`, portado em 2026-10-11) |
| `--help`, `--version` ou `--dump-config` com valor (`--dump-config=false`) | exit 1, sem citar o valor: um *switch* não tem valor, e um valor ignorado pode dizer o contrário (despejava e saía 0) |
| `--config=` vazio | exit 1: caía no `SYSLOG_CONFIG`, o contrário do que diz |
| nome de parâmetro com algo colado depois (`--sink.nats.passwordhunter2`, `…password-hunter2`), ou com caracteres que nome nenhum tem (`password: hunter2 = x`, `--sink.nats.password:hunter2`) | exit 1 como parâmetro desconhecido, **sem** citar o nome: o `=` esquecido põe o valor dentro dele |
| no INI, chave desconhecida logo depois de um segredo (`password = abc` e, na linha seguinte, `ffd3a9c1b2=`), ou cuja seção não existe aqui nem no pai, ou que não tem a forma de uma chave (`^[a-z0-9_]{1,64}$`) | exit 1 nomeando só a linha: é o resto de um valor que quebrou de linha, e o erro vai ao stderr e, num `SIGHUP`, ao log |
| BOM UTF-8 no começo do INI | ignorado (alguns editores do Windows o gravam; mantido, a linha 1 era "expected 'key = value'" sem nada visível) |
| variável de ambiente do pai que este servidor não tem (`SYSLOG_LISTEN_TLS`, `SYSLOG_TLS_*`, `SYSLOG_FILTER_*`, `SYSLOG_RATELIMIT_*`, `SYSLOG_SINK_TYPE`, `SYSLOG_RING_MODE`, `SYSLOG_SINK_NATS_SUBJECTS`, `SYSLOG_SINK_NATS_TLS*`, as dos outros sinks e rings...) | **um** `WARN` que nomeia todas e diz que são ignoradas (`internal/config/removed.go`, §16.2). Não é erro porque um mesmo `.env` pode alimentar mais de um servidor da família; sem o aviso, o env é lido só pelos nomes do registry e elas sumiriam em silêncio, e quem trocou um servidor pelo outro acreditaria ter um listener TLS, um filtro ou subjects granulares |

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
- **FATAL é terminal.** Toda transição e o gauge `syslog_sink_up` são escritos
  sob o mesmo lock, e nada sai de FATAL: uma checagem concorrente não pode
  devolver o estado a UP/DOWN depois de um `ClassFatal` do `Write()`.
- `ClassNetwork` no `Write()` **não** muda o estado; o pedaço é retentado.
- Em **DOWN**, vale o backpressure de §2.3 mesmo com o ring vazio (fail-fast), e o
  `/readyz` falha.
- Em **FATAL**: §12 e exit 3.
- O beacon (§11) não participa: uma falha dele nunca muda o estado.

### 13.4 Shutdown gracioso

`SIGTERM` ou `SIGINT`:

```
fase 1  /readyz passa a falhar
fase 2  espera shutdown.pre_stop_delay (0 s), ainda aceitando
fase 3  cancela o contexto de recepção e fecha os transportes na ordem inversa
        do boot (tcp, udp, unix); as mensagens já lidas terminam de entrar
        no ring
fase 4  drena o ring para o sink, com shutdown.drain_timeout (30 s); depois
        para os workers e recolhe Workers.Pending() e o Drain() do ring
fase 5  o que sobrou vai para o dump (§12); para o beacon, fecha o sink e o
        ring; exit 0
        (ou 3, abaixo)
```

- O contexto de recepção é cancelado **antes** de os transportes fecharem: é o
  que solta um leitor preso no laço de backpressure. Um item já entregue ao ring
  não é afetado.
- `drain_timeout` estourado com itens: §12 e exit 3.
- **Segundo sinal** em qualquer fase: saída imediata, `exit 130`, aceitando a
  perda. Conta em `reason="abort"` o que o ring ainda tinha **e** o que já fora
  entregue ao dump em andamento (o `Drain` já os tirou da contabilidade do
  ring; antes da revisão independente do `go-syslog-pg-server`, um abort durante
  o dump contava 0).
- **Um dono só para a contagem da perda** (revisões independentes do
  `go-relplog-server` e do `go-relplog-pg-server`, portadas em 2026-10-13).
  Antes de recolher as sobras (`Workers.Pending`, `Drain`), o shutdown entra na
  **fase do dump** (`enterDumpPhase`): daí em diante o abort conta o contador
  `dumping`, nunca o do ring, que deixa de dizer a verdade (os lotes retidos
  ainda estão nele, os drenados já não). O contador recebe exatamente o que vai
  ao dump (`oweDump`), e o dump o toma de volta com `Swap` (salvo, nada mais se
  perde; falho ou desligado, ele conta `emergency_failed`). As passagens e o
  abort são ordenados por um mutex (`lossMu`), e o abort é registrado uma vez
  (`aborted`): um abort entre duas passagens contava o ring e deixava o
  contador para um dump falho contar de novo (10 perdidas para 5). Depois do
  abort nenhuma passagem acontece e o dump desiste (§12.3).
- **O dump toma o contador de volta sob o mesmo mutex** (`settleDump`,
  revisão independente do `go-syslog-server`, 2026-10-13, A2): o aborto
  publica `aborted` antes de tomar o contador, e um dump que via a marca
  nesse meio — desistia (`ErrAbandoned`) ou, desligado, nada tinha a
  escrever — tomava o contador primeiro: `emergency_failed = n` e
  `abort = 0`, a perda contada uma vez mas atribuída ao disco. Sob o
  `lossMu`, ou o aborto terminou (o dump acha 0) ou nem começou (o dump
  conta, o aborto acha 0) (`TestAbortInsideTheDumpKeepsItsReason`).
- **O aborto fecha o ring antes de contar** (porte do RP/RN, 2026-10-14).
  Fora da fase do dump, o `abortLoss` chama `ring.Close()` **antes** de ler a
  ocupação: um `Enqueue` depois disso é recusado com `ring.ErrClosed`, que a
  recepção trata como shutdown (TCP fecha a conexão sem ler adiante; UDP e unix
  descartam), contado em `syslog_messages_dropped_total{reason="shutdown"}`, e
  nenhuma mensagem é aceita depois da contagem sem ser contada. O `Enqueue`
  reconfere o fechamento **depois** de reservar a vaga (`items.Add(1)`): ou a
  reserva é vista pela leitura do aborto (contada), ou o `Enqueue` vê o
  fechamento e desiste — no máximo uma sobrecontagem transitória, nunca uma
  mensagem aceita sem contagem. A sobrecontagem tem uma segunda fonte (registrado em 2026-10-14, D3 da revisão do syslog): um `Enqueue` que reservou a vaga (`items.Add(1)`) e vai desistir por falta de bytes (`ErrFull`) pode ser lido pelo aborto entre a reserva e o recuo, e conta em `abort` uma mensagem que a recepção trata como ring cheio. É transitória, de no máximo uma por recepção concorrente, e nunca esconde uma perda. A janela não era só o `pre_stop_delay`: um
  `SIGQUIT` em operação normal tinha a mesma, com todos os transportes abertos.
  O `Close` do ring em memória é uma marca, e o aborto continua "agora"
  (`TestAbortClosesTheRingBeforeCounting`,
  `TestEnqueueSeesACloseThatRacedTheReservation`).
- **Shutdown por FATAL** (que começa sem sinal nenhum): o **primeiro**
  `SIGTERM`/`SIGINT` é tolerado — logado, e o dump continua —, e só um segundo
  aborta. Um orquestrador que manda `SIGTERM` ao ver o processo sair do ar não
  pode cortar o dump que salva o ring. `SIGQUIT` aborta
  sempre, **nunca é tolerado**, inclusive fora do shutdown: no boot e com o
  servidor no ar, saída imediata com 130. Um `SIGQUIT` que chega depois de o
  shutdown começar e antes de o observador do segundo sinal existir chega só ao
  observador do `run`: o shutdown confere `quitting` na entrada e, depois, espera
  também o canal `quitNow`, fechado junto com a flag, para o `SIGQUIT` gravado
  logo depois dessa leitura (portado do `go-relplog-server` e do
  `go-relplog-pg-server`).
- **Sink FATAL durante a fase 4** (o `SIGTERM` chegou com o sink UP e ele virou
  FATAL no dreno): a espera termina na hora, em vez de gastar o `drain_timeout`
  num dreno que não acontece mais; o que sobrou vai para o dump, e a saída é
  **3**.
- **O socket unix** é removido quando o transporte fecha, na fase 3.
- **`SIGHUP` vale o processo inteiro.** É capturado desde a primeira linha; fora
  da janela entre o fim do boot (passo 12) e o início do shutdown, é logado em
  `WARN` e ignorado. A ação padrão do Go encerraria o processo com 129, sem log,
  no meio de um boot que pode durar o `boot_timeout`.
- O observador do segundo sinal é registrado **antes** de o contexto de sinais
  soltar `SIGTERM`/`SIGINT`: no intervalo, um segundo sinal teria a ação padrão
  e mataria o processo com 143.
- `shutdown.pre_stop_delay` existe para Kubernetes (5–10 s sugeridos): um emissor
  UDP não recebe erro quando o socket some. O `stop_grace_period` do orquestrador
  precisa ser **maior** que `pre_stop_delay + drain_timeout`.

### 13.5 Códigos de saída

| Código | Significado |
|---|---|
| `0` | shutdown completo, ring vazio |
| `1` | erro de configuração ou de inicialização não relacionada ao NATS (inclui toda URL que não seja `nats://`, a `LOGHUB_EB_URL` com `tls://` e um `.creds` inexistente) |
| `3` | NATS indisponível ou inutilizável: sink FATAL (inclusive um servidor que exige TLS), `boot_timeout` ou `drain_timeout` estourados, validação de §10.7 recusada, dump que falhou |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` (`DATADIR`, `MACHINE_ID`, `MACHINE_ID_FILE`, `AGENT_NAME`, `AGENT_UUID`, `HOSTNAME`, `WORKSPACE`; 112 = `Initialize()` duas vezes); a tabela completa está na skill `go-loghub-ident` e em `docker/README.md` |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 7–9 do boot, e o que cancela os *bind* da porta admin (passo 5) ou dos transportes (passo 11, §13.1) |

---

## 14. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9102`, **o mesmo do `go-syslog-server`**: os
dois são da mesma família e um operador pode trocar um pelo outro sem mexer no
monitoramento. Por isso **não rode os dois no mesmo host sem trocar o
`admin.bind` de um deles** (o `go-webhook-server` usa `9101`). HTTP/1.1.

O `admin.bind` nunca divide o endereço TCP do `listen.tcp` (§13.2): mesma
porta com o mesmo literal, um curinga ou um nome de qualquer lado é exit 1 na
validação.

| Path | Semântica |
|---|---|
| `/healthz` | `200` enquanto o processo vive. Liveness. |
| `/readyz` | `200` só se: sink `UP`, ring abaixo de `ring.high_water`, boot concluído (inclusive a reingestão), transportes abertos e não em shutdown. Senão `503` com o motivo em `text/plain`. |
| `/metrics` | exposição Prometheus em texto. |

### 14.1 Métricas

Prefixo `syslog_`, **o mesmo do pai**: um painel do `go-syslog-server` lê este
servidor. **Nenhum label deriva de conteúdo controlado pelo emissor** (nada de
hostname, appname, IP); todo label tem conjunto finito conhecido em compilação.
`facility` e `severity` **não** são labels: o produto com `transport` multiplicaria
as séries, e a distribuição por severidade é pergunta para quem consome o
stream.

| Métrica | Tipo | Labels |
|---|---|---|
| `syslog_messages_total` | counter | `transport` (udp, tcp, unix) |
| `syslog_messages_accepted_total` | counter | `transport` |
| `syslog_messages_dropped_total` | counter | `reason` (ring_full, sink_down, malformed_frame, proxy, empty, shutdown) |
| `syslog_messages_truncated_total` | counter | `transport` |
| `syslog_message_bytes` | histogram | — (tamanho original) |
| `syslog_convert_total` | counter | `format` (rfc5424, rfc3164, malformed) |
| `syslog_convert_warnings_total` | counter | `code` (os 14 de §6.9) |
| `syslog_convert_duration_seconds` | histogram | — |
| `syslog_conns_active`, `syslog_conns_total` | gauge, counter | `transport` (tcp) |
| `syslog_conns_refused_total` | counter | `transport` (tcp), `limit` (global, per_ip); `global` conta cada vez que o accept estaciona no teto, um limite inferior, porque o teto global bloqueia em vez de recusar |
| `syslog_proxy_headers_total` | counter | `result` (v1, v2, local, absent, invalid) |
| `syslog_backpressure_seconds_total` | counter | `transport` (tcp) |
| `syslog_confirm_duration_seconds` | histogram | — (do frame completo à entrada no ring; TCP, só aceitas) |
| `syslog_udp_receive_errors_total` | counter | — |
| `syslog_udp_read_buffer_bytes` | gauge | — (o `SO_RCVBUF` efetivo, lido de volta) |
| `syslog_ring_items`, `syslog_ring_bytes` | gauge | — |
| `syslog_ring_capacity_items`, `syslog_ring_capacity_bytes` | gauge | — |
| `syslog_sink_batch_size`, `syslog_sink_batch_bytes` | histogram | — (por pedaço) |
| `syslog_sink_duration_seconds` | histogram | — |
| `syslog_sink_health_duration_seconds` | histogram | — |
| `syslog_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); o cancelamento do shutdown não conta. `fatal` conta **toda** recusa fatal do destino, mesmo durante um cancelamento (boot cancelado, shutdown): ela não nasce dele, é um veredito real, e o processo sai logo depois. Só o `network` cortado pelo cancelamento deixa de contar, porque o cancelamento **causa** erros de rede (prazo, conexão fechada). Regra, não pendência (porte do RP/RN, decidido em 2026-10-14). |
| `syslog_sink_up` | gauge | — |
| `syslog_sink_nats_duplicates_total` | counter | — (confirmações com `PubAck.Duplicate`: uma redelivery dentro da `duplicate_window`, §10.2) |
| `syslog_sink_nats_reconnects_total` | counter | — |
| `syslog_beacons_total` | counter | `result` (ok, error) (§11) |
| `syslog_events_written_total` | counter | — |
| `syslog_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero, como todo label de conjunto fechado (um `rate()` precisa da série antes do primeiro evento) |
| `syslog_emergency_files`, `syslog_emergency_events_pending` | gauge | — (arquivos e envelopes pendentes no disco, recontados do diretório no fim da reingestão e somados a cada dump; durante a reingestão, `files` é o número de arquivos listados e `events_pending` não muda, §12.2) |
| `syslog_build_info` | gauge | `version`, `commit`, `go` |
| `syslog_config_info` | gauge | `ring_mode` (sempre `memory`), `sink_type` (sempre `nats`), `transports` |
| `syslog_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

Saíram, com o que as produzia: `reason` `ring_unavailable`, `filter` e
`ratelimit`; o label `tls` em `transport`; `syslog_tls_handshake_errors_total`;
`syslog_ring_quarantined_total` e `syslog_ring_redis_claimed_total`; as séries
de `postgres`, `sqlite`, `mysql`, `redis` e `clickhouse`;
`syslog_ratelimit_buckets`. Uma série que nunca pode subir não é exposta: diria
ao operador que existe uma medição onde não existe nenhuma.

**Fronteiras de histograma** são estáticas, idênticas em toda instância e
**pré-formatadas como string** na inicialização: emitir `0.01` num scrape e
`0.010` no seguinte cria duas séries e quebra todo quantil.

### 14.2 Logs

`slog`, `json` por padrão (`text` disponível). **Nenhum log síncrono por mensagem
no caminho padrão.** `log.access = true` liga log por mensagem com
`log.access_sample` (0.01); em syslog isso é logar log. Avisos de anomalia
(conversão, PROXY, falha de aceite) são **amostrados**, um a cada 30 s por tipo,
com o contador exato em `/metrics`: um emissor quebrado não inunda o log do
coletor. Nenhum log em `INFO` ou acima carrega o payload de uma mensagem.

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

**Memória por conexão, fora do ring.** Uma conexão TCP viva segura o leitor de
4 KiB e o buffer de frame (até 8 KiB quando ociosa; o de um frame maior enquanto
os frames seguintes já estão no leitor, ou com alguns bytes do próximo frame no leitor e o par calado), ociosa ou não; os números e a regra
estão em §5.4. Não entram em `ring.max_bytes`: o teto delas é `tcp.max_conns`,
e com `0` é o `GOMEMLIMIT` (§15.4) que segura um enxame de conexões caladas.

**Teto pequeno demais.** Uma mensagem de `n` bytes (já truncada) custa ao ring
a classe do pool de `n` + 256 B (§9.2): 2 KiB, 8 KiB ou 64 KiB, e acima da
maior classe o próprio `n`. O piso de `ring.max_bytes` é o custo de uma
mensagem do tamanho máximo, `item.PooledCap(syslog.max_message_bytes) +
ring.ItemOverhead` (65 792 B com o padrão), e um valor menor é recusado na
validação (§13.2, exit 1). O piso da **menor** classe não basta: um ring de
4 KiB aceitaria as linhas curtas e recusaria **sempre** qualquer linha de
3 KiB, mesmo vazio — no TCP a conexão daquele emissor parava até o
`backpressure_timeout` a cada uma. Com o piso, toda mensagem que o truncamento
guarda cabe num ring vazio (porte do RP/RN, 2026-10-14; antes aceito e listado
em TESTING §8).

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
- Env: `SYSLOG_<SEÇÃO>_<CHAVE>` em maiúsculas, `.` vira `_`
  (`SYSLOG_SINK_NATS_URLS`), com as exceções de §16.2.
- INI: seção `[ring]`, chave `capacity_items`.
- `--config=/caminho.ini` (env `SYSLOG_CONFIG`) indica o arquivo.

**Os mesmos nomes do pai.** O prefixo `SYSLOG_` e cada chave que ficou têm o
nome que tinham no `go-syslog-server`: os dois são da mesma família, e um
operador pode trocar um pelo outro mantendo o env das chaves comuns.

### 16.2 Registry único, e os nomes de env fixados pelas skills

Uma **tabela** (`internal/config/registry.go`) declara cada parâmetro (seção,
chave, tipo, padrão, descrição, se é recarregável, nome de env). Dela saem o
INI, as flags, o env, o `--help`, o `--dump-config` e a validação. Um parâmetro
novo entra só ali. Sem `gopkg.in/ini.v1` e sem `spf13/pflag`.

Nomes de env que **não** seguem a regra, porque uma skill os fixou:

| Seção | Env | Origem |
|---|---|---|
| `[syslog]` | `SYSLOG_<CHAVE>` (sem repetir a seção): `SYSLOG_DEFAULT_PRIORITY`, `SYSLOG_RFC3164_TIMEZONE`, `SYSLOG_MAX_MESSAGE_BYTES`, `SYSLOG_PAYLOAD_RAW`, `SYSLOG_LEGACY_CHARSET`, `SYSLOG_ACCEPT_MALFORMED`, `SYSLOG_SD_MAX_ELEMENTS`, `SYSLOG_SD_MAX_PARAMS` | skill `loghub-type-syslog-schema` §15 |
| `[loghub]` | `LOGHUB_EB_URL`, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE` | skill `loghub-json-schema` |
| `server.data_dir` | `DATADIR` | `go-loghub-ident` |

As variáveis de identidade (`AGENT_UUID`, `AGENT_NAME`, `WORKSPACE`, `HOSTNAME`,
`MACHINE_ID`, `MACHINE_ID_FILE`, `LOGHUB_IDENT_DEBUG`) **não** passam pelo
registry: são lidas pelo `go-loghub-ident`, e o `--dump-config` mostra o que o
**ambiente** diz numa seção informativa `[identity]`, sem resolver nada
(§13.1 passo 1: o dump não toca identidade nem disco).

**O `--dump-config` relido reproduz a config**: um valor **derivado** que
ficou no padrão (`emergency.dir` de `server.data_dir`) sai comentado, com a nota; escrito como chave, viraria
explícito na releitura e deixaria de acompanhar o que o deriva (revisão
independente do `go-relplog-pg-server`, 2026-10-13, portada).

**Chaves do pai que saíram.** Na linha de comando ou no INI são parâmetro
desconhecido, exit 1, e o erro diz de onde a chave vem ("go-syslog-server has
it", `removedParams`, a mesma lista do env em grafia pontuada). No ambiente, onde o registry lê só os próprios nomes e
nunca varre o resto, elas sumiriam em silêncio; por isso o boot confere a lista
de `internal/config/removed.go` (as de TLS, filtro, rate-limit, `sink.type`,
`ring.mode`, `sink.nats.subjects`, `sink.nats.tls*` e as dos outros sinks e
rings) e emite **um** `WARN` que nomeia todas as definidas e diz
que são ignoradas (§13.2).

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

(No pai entravam também `filter.*`, `ratelimit.*` e `sink.sqlite.keep_files`,
que saíram, §1.2.)

Toda chave de `[syslog]` **exige restart**: ela muda a conversão, e o JLH de um
item não pode depender de **quando** ele foi drenado (§3.5). A identidade fica
congelada.

Sinônimos são um valor só: `syslog.legacy_charset = windows-1252` num INI cujo
boot disse `cp1252` (ou `iso-8859-1` e `latin1`) não mudou nada, e o `SIGHUP`
não o relata como "requires a restart" (revisão independente do
`go-relplog-pg-server`, 2026-10-13, portada; `TestCharsetSynonymsAreOneValue`).

Duas referências: chave recarregável comparada com o que está **em execução**
(para que reverter ao valor de boot seja aplicado); chave de restart comparada
com o **boot** (para que todo `SIGHUP` avise sobre ela até o restart). Cada uma
das duas sozinha falha num dos casos (bugs reais 009 e 010 da standard).

**Chave derivada não é relatada.** Uma chave **derivada** no boot e no arquivo
novo (§16.2, a nota `derived` do `--dump-config`: `emergency.dir` de `server.data_dir`) não entra na lista
"requires a restart": muda só porque a chave que a deriva mudou, e essa é
relatada. Uma que passa de explícita a derivada, ou o contrário, é relatada;
notas que não começam com `derived` (`lowered to 4`, `forced to 1`, `resolved to
an absolute path`) dizem como um valor dado foi ajustado e continuam relatadas.
A comparação usa as notas que o `Load` já grava (`Loaded.Notes`), as mesmas do
`--dump-config` (porte do RP/RN, 2026-10-14; `TestReloadDoesNotReportDerivedKeys`).

**Chave sombreada.** Como env e flag vencem o arquivo (§16.1) e não mudam num
processo em execução, uma chave que o arquivo define mas um env ou uma flag
decide **nunca** muda pelo arquivo: a recarregável não recarrega, e a de restart
não muda nem com o restart. Todo `SIGHUP` loga cada uma em `WARN`, com a origem
que manda (`env SYSLOG_LOG_LEVEL`), mesmo quando o resultado é "configuration
unchanged": é exatamente o caso em que o operador editou o arquivo e não entende
por que nada mudou. Por isso o compose não define chaves recarregáveis por env:
elas vêm do INI montado (`docker/conf/syslog.ini`).

Um arquivo inválido no `SIGHUP` não aplica nada: `ERROR`, e o servidor segue com
a config anterior. Fora da janela entre o fim do boot e o início do shutdown, o
`SIGHUP` é logado e ignorado (§13.4).

### 16.4 Exemplo

```ini
[server]
data_dir = /data

[listen]
udp  = :1514
tcp  = :1514
unix =

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
credentials = /etc/syslog/syslog.creds
mode        = jetstream
stream      = LOGHUB_SYSLOG

[loghub]
beacon_interval = 15

[admin]
bind = 0.0.0.0:9102

[log]
level = info
```

Usuário e senha (ou o token), no lugar do `.creds`, vão por env
(`SYSLOG_SINK_NATS_PASSWORD`, `SYSLOG_SINK_NATS_TOKEN`) ou dentro da
`LOGHUB_EB_URL` — nesse caso sem `urls`, `username`, `password` e `token` no INI
(§10.9).

### 16.5 Tabela de parâmetros

A fonte é `internal/config/registry.go`; o `--help` imprime a mesma tabela.

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `shutdown.drain_timeout`,
`tcp.backpressure_timeout` e `proxy.header_timeout` — a validação recusa `0` com
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
| `udp` | `:1514` | RFC 5426 |
| `udp_listeners` | `1` | 1–256; `>1` habilita `SO_REUSEPORT`; o único jeito de passar de um núcleo |
| `udp_read_buffer` | `8MiB` | `SO_RCVBUF`; o efetivo é lido de volta e exposto |
| `tcp` | `:1514` | RFC 6587 |
| `tcp_listeners` | `1` | 1–256; `>1` habilita `SO_REUSEPORT` |
| `unix` | `""` | socket datagram, caminho absoluto; `/dev/log` é o costume. A única escrita fora de `server.data_dir` |
| `unix_mode` | `0666` | octal; todo processo precisa poder logar |
| `unix_read_buffer` | `8MiB` | `SO_RCVBUF` do socket unix |

#### `[tcp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `framing` | `auto` | `auto` \| `octet` \| `lf`; decidido uma vez por conexão (§5.2) |
| `idle_timeout` | `120s` | `0` = nunca |
| `max_conns` | `0` | teto do transporte, somando os listeners; bloqueia no accept; `0` = ilimitado |
| `max_conns_per_ip` | `0` | por IP do par TCP (antes do PROXY); fecha na hora; `0` = ilimitado |
| `backpressure_timeout` | `30s` | quanto tempo parar de ler antes de fechar (§5.7) |

#### `[tls]`

Removido no go-syslog-nats-server (§1.2).

#### `[proxy]`

| Chave | Padrão | Descrição |
|---|---|---|
| `protocol` | `off` | `off` \| `optional` \| `required`, só no TCP (§4.6) |
| `trusted` | `127.0.0.1/32,::1/128` | CIDRs dos balanceadores; largo demais permite forjar `src-ip` |
| `header_timeout` | `5s` | prazo do cabeçalho PROXY |

#### `[syslog]` — exige restart (§16.3)

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `default_priority` | `SYSLOG_DEFAULT_PRIORITY` | `13` | PRI de `pri-missing`/`pri-invalid`, 0–191 |
| `rfc3164_timezone` | `SYSLOG_RFC3164_TIMEZONE` | `UTC` | fuso IANA dos timestamps sem fuso; `Local` é recusado |
| `max_message_bytes` | `SYSLOG_MAX_MESSAGE_BYTES` | `64KiB` | truncamento (§5.5); 480 a 16 MiB; pior JLH = 12 × isto + 8 KiB |
| `payload_raw` | `SYSLOG_PAYLOAD_RAW` | `true` | `raw` quando não é obrigatório |
| `legacy_charset` | `SYSLOG_LEGACY_CHARSET` | `""` | `cp1252` \| `latin1`, sinônimos `windows-1252` \| `iso-8859-1` (§6.7) |
| `accept_malformed` | `SYSLOG_ACCEPT_MALFORMED` | `true` | `false` descarta `malformed` no worker e no dump |
| `sd_max_elements` | `SYSLOG_SD_MAX_ELEMENTS` | `32` | §6.11 |
| `sd_max_params` | `SYSLOG_SD_MAX_PARAMS` | `64` | §6.11 |

#### `[filter]` — recarregável

Removido no go-syslog-nats-server (§1.2).

#### `[ratelimit]` — recarregável

Removido no go-syslog-nats-server (§1.2).

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `capacity_items` | `65536` | teto de itens, 1–16 777 216; com mensagens de 200 bytes é o que pega primeiro |
| `max_bytes` | `512MiB` | teto de bytes: o `cap` de cada buffer cru + 256 B por item (§9.2); o JLH não conta. No mínimo o custo de uma mensagem de `syslog.max_message_bytes` (a classe do pool + 256 B, §13.2) |
| `high_water` | `0.80` | fração de qualquer teto acima da qual o `/readyz` falha |

Não há `mode` (sempre `memory`) nem `max_inflight` (era do `ring=none`).

#### `[ring.sqlite]`

Removido no go-syslog-nats-server (§1.2).

#### `[ring.redis]`

Removido no go-syslog-nats-server (§1.2).

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `workers` | `8` | goroutines de dreno, que também convertem; dividem a única conexão NATS; 1–256 |
| `batch` | `256` | itens por `Dequeue`; as publicações assíncronas de um pedaço ficam no ar juntas; 1–65 536 |
| `batch_max_bytes` | `16MiB` | teto de JLH por pedaço; no máximo 256 MiB (o pedaço fica inteiro na memória enquanto os futuros pendem) |
| `batch_max_delay` | `5ms` | espera máxima para completar um lote; `0` = sem espera |
| `op_timeout` | `5s` | prazo de uma escrita (as confirmações do pedaço), da discagem e de um futuro assíncrono |
| `health_interval` | `2s` | período da checagem; também a latência de UP→DOWN |
| `health_timeout` | `1s` | prazo de uma checagem |
| `fail_timeout` | `30s` | DOWN antes de FATAL; `0` = sem fusível por tempo |
| `max_retries` | `10` | checagens falhas antes de FATAL |
| `boot_timeout` | `300s` | quanto o boot espera o NATS (e o stream) antes de sair com 3; também o prazo da reingestão inteira |
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

Removido no go-syslog-nats-server (§1.2).

#### `[sink.sqlite]`

Removido no go-syslog-nats-server (§1.2).

#### `[sink.postgres]` e `[sink.mysql]`

Removido no go-syslog-nats-server (§1.2).

#### `[sink.clickhouse]`

Removido no go-syslog-nats-server (§1.2).

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
| `emergency.dir` | `<data_dir>/emergency` | criado e provado gravável no boot |
| `admin.enabled` | `true` | serve `/healthz`, `/readyz` e `/metrics` |
| `admin.bind` | `127.0.0.1:9102` | a mesma porta do servidor pai (`go-syslog-server`): não rode os dois no mesmo host sem trocar (§14) |
| `log.level` | `info` | `debug` \| `info` \| `warn` \| `error` (recarregável) |
| `log.format` | `json` | `json` \| `text` |
| `log.access`, `log.access_sample` | `false`, `0.01` | log por mensagem, amostrado (recarregáveis) |
| `shutdown.pre_stop_delay` | `0s` | continua recebendo depois do `SIGTERM`; `0` = sem espera |
| `shutdown.drain_timeout` | `30s` | prazo do dreno do ring no shutdown |

### 16.6 Escrita em disco: uma raiz só

Tudo o que o serviço grava — a identidade (`agent_uuid`, `machine_id`, gravados
pelo `go-loghub-ident`) e `emergency/` — fica sob `server.data_dir`, e no
contêiner essa raiz é `/data` (o compose sobe com `read_only: true`). **A única
exceção é `listen.unix`**, um socket que existe para ser encontrado onde a libc
procura.

Caminhos fixos do contêiner: `/app` guarda o binário (dono `root`), `/data` é a
única área gravável, `/etc/syslog` é o INI em `:ro` (o compose monta
`docker/conf/` ali e define `SYSLOG_CONFIG=/etc/syslog/syslog.ini`, que traz as
chaves recarregáveis, §16.3), `/tmp` é tmpfs. `WORKDIR /data`; o binário é
invocado por `/app/go-syslog-nats-server`. Não há `/etc/syslog/tls`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

---

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-syslog-nats-server/          ESTE módulo
  go-syslog-pg-server/            o irmão: o mesmo molde, com a fila loghub.inbox do PostgreSQL
  go-syslog-server/               o pai: dois eixos (rings × sinks), TLS, filtro, rate-limit, beacon
  go-syslog-standard-server/      o antigo, referência
  go-relplog-server/              só especificação, futuro
  go-webhook-server/, go-webhook-fast-server/
```

Módulo independente. Nada é importado de outro módulo do monorepo, nem do pai
nem do irmão: compartilhar pacotes faria os módulos evoluírem amarrados. Tags
`go-syslog-nats-server/vX.Y.Z`.

### 17.2 Layout interno

```
cmd/go-syslog-nats-server/main.go  flags que encerram, lhident.Initialize(), chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão, admin
  config/     registry único -> INI + flags + env + --help + dump; validação;
              LOGHUB_EB_URL (só nats://); o aviso das variáveis do pai (removed.go)
  control/    o heartbeat beacon (§11), e só ele
  ident/      Identity (congelada no boot) e a gramática de space          [lógica pura]
  jlh/        escritor JSON por append, Event, cabeçalho do envelope, UUID,
              Payload (o corte por posição)                                [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH: pri, formato, rfc5424,
              rfc3164, sd, texto, charset, nomes, sid                       [lógica pura]
  item/       Item, Head, pools de buffer (sem codec)                       [lógica pura]
  jlhcheck/   SÓ TESTE: os schemas e os vetores copiados das skills e o
              oráculo de conformidade (§18.2); nenhum arquivo de produção o importa
  receiver/   udp (+ pktinfo por plataforma), unix, tcp, moldura, proxy,
              limites, ingest
  ring/       interface Ring + memory
  sink/       interface Sink + nats, workers, chunk, health, instrumented
  emergency/  dump JSON Lines, ParseLine e o leitor em fluxo da reingestão
  obs/        métricas Prometheus próprias + slog
```

`convert/`, o leitor do PROXY em `receiver/` e `emergency.ParseLine` são os
parsers de entrada adversária. Os alvos de fuzz estão em §18.3.

### 17.3 Dependências

**Três diretas em produção, e ponto.**

| Dependência | Para quê |
|---|---|
| `github.com/nats-io/nats.go` v1.53.1 | o sink, com o pacote `jetstream`, e o beacon por NATS core |
| `github.com/patrickbrandao/go-loghub-uuidv7` v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` v0.5.2 | identidade (sem dependência nova: usa a `go-loghub-uuidv7`) |

Transitivas que o `nats.go` põe no binário (`go list -deps ./cmd/go-syslog-nats-server`):
`github.com/nats-io/nkeys` v0.4.15 (NKey e `.creds`), `github.com/nats-io/nuid`
v1.0.1, `github.com/klauspost/compress` v1.18.5 (a compressão do transporte
websocket do cliente, compilada mesmo com `ws://` recusado) e
`golang.org/x/crypto` v0.49.0 (ed25519/nacl do `nkeys`).

Só em teste: `github.com/santhosh-tekuri/jsonschema/v6` (contratos, §18.2). O
`pgx` do `go-syslog-pg-server` não está no `go.mod`.

O pai tinha oito; saíram `rueidis`, `modernc.org/sqlite`, `pgx`,
`go-sql-driver/mysql` e `clickhouse-go/v2`, com as transitivas de cada um. O
binário tem **12,5 MB**, contra 17,2 MB do `go-syslog-pg-server` e 31,8 MB do
`go-syslog-server` (o mesmo build `darwin/arm64`, flags padrão, 2026-10-07). `golang.org/x/net` e
`golang.org/x/text` **não** são importados: PKTINFO e o charset legado são feitos
com a stdlib (`syscall`) e com tabela própria, e os dois só aparecem no binário
como as cópias internas da própria stdlib.

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
  rejeitada. Código copiado do pai ou da standard vem **com** os comentários.
- Citações da spec por número de seção (`§6.4.3`), com a numeração do pai
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
  pelo nome. **O vetor com `transport=tls` roda reetiquetado como TCP:** os
  mesmos bytes recebidos por TCP simples, com `payload.tls` retirado do
  esperado, `transport = "tcp"` e o `sid` recalculado pela fórmula de §6.10.
  Assim a cobertura de conversão daquele vetor continua, sem TLS.
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
| `FuzzFrame` | `receiver` | as duas molduras: sem pânico, sem alocação pelo MSG-LEN declarado, `size ≥ len(cru)` |
| `FuzzFrameOracle` | `receiver` | a moldura contra um oráculo independente |
| `FuzzProxy` | `receiver` | v1 e v2: sem pânico, nunca lê além do cabeçalho declarado |
| `FuzzRead` | `emergency` | o leitor do dump tolera truncamento e lixo sem pânico, contabiliza todo byte (linha guardada, inválida ou cauda cortada), nunca guarda uma linha acima do teto, e todo registro devolvido passa de novo, igual, por `ParseLine` e tem o payload cortável por posição |

O `FuzzHead` do pai saiu com o codec do cabeçalho (§3.4); o `FuzzJSONBSafe` e o
`FuzzInboxPayload` do `go-syslog-pg-server` não se aplicam (não há `jsonb`).

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=10m
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrameOracle$  -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=10m
```

### 18.4 Corpus de mensagens reais

`internal/convert/testdata/corpus/` guarda mensagens **capturadas de emissores
reais** (`rsyslog`, `syslog-ng`, `journald`, `logger(1)`, glibc, Cisco IOS,
MikroTik, pfSense, HAProxy, nginx, Kubernetes, Windows via NXLog, log4j): um
`.raw` com os bytes exatos e um `.want` com os campos esperados do payload.
**O corpus nunca encolhe**: o teste falha abaixo de vinte entradas. Um caso novo
de campo entra aqui antes da correção do conversor.

### 18.5 Integração em processo

A pilha **real** (a mesma `run()` do binário), contra um `nats-server` real com
JetStream: como o único destino é o NATS, todo teste que passa do passo 7 do
boot precisa de `SYSLOG_TEST_NATS_URL` (§18.6) e pula sem ela; só as recusas de
antes do passo 7 (exit 1) rodam sem servidor. Cada teste trabalha num `space`
próprio, com um stream próprio criado a partir de `docker/nats/stream.json` e
apagado no fim. Falhas do destino são injetadas decorando o sink real
(`App.wrapSink`). Cobre: todos os transportes chegam ao stream (UDP com
`dst-ip`, TCP nas duas molduras, unix), PROXY v1 e v2, fidelidade do JLH (o
`Data` da mensagem é o JLH byte a byte, com NUL no meio), `SIGHUP` de `[log]`
(ida e volta, chave sombreada, arquivo inválido), dump de emergência e
reingestão exatos (mesmo `uuid`, mesmo JLH, mesmo depois de trocar uma chave
`[syslog]`), dump danificado em `.corrupt`, shutdown gracioso sem perda,
segundo sinal (130), nenhum transporte (exit 1), chaves do pai na linha de
comando (exit 1) e no env (`WARN`), códigos 100–114 do `go-loghub-ident` num
subprocesso, e a ordem do boot.

### 18.6 Destino real

As suítes do NATS pulam sem a variável. A da app cria cada stream **a partir do
arquivo que o operador aplica** (`docker/nats/stream.json`), com os subjects do
`space` do teste; a do sink, de um stream canônico em código que um teste sem
servidor confere, junto com o arquivo, contra o contrato de §10.7:

```bash
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 SYSLOG_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 600s ./internal/sink/ ./internal/control/ ./internal/app/
```

`SYSLOG_TEST_NATS_NOJS_URL` aponta para um servidor **sem** JetStream (o FATAL
imediato de §10.7). Os dois servidores são sem TLS, como tudo aqui. O que as
suítes provam está em `docs/TESTING.md` §6.

### 18.7 Emissores vivos

`SYSLOG_TEST_EMITTERS=1` (com `SYSLOG_TEST_NATS_URL`) sobe rsyslog e syslog-ng em contêineres descartáveis e
confere, por transporte e moldura: rsyslog `omfwd` UDP, TCP LF
(`RSYSLOG_ForwardFormat`) e TCP octet (`RSYSLOG_SyslogProtocol23Format`);
syslog-ng `network()` UDP e TCP LF, `syslog()` TCP octet. É a única prova de que
a moldura funciona no fio. (O cenário TLS `gtls` do pai saiu.)

### 18.8 Critérios de aceite v1

**Nunca medidos.** Alvo, não fato; a primeira medição substitui esta tabela.

| Cenário | Alvo |
|---|---|
| UDP, mensagens de 200 B | ≥ 150k msg/s por núcleo de recepção |
| TCP LF | ≥ 120k msg/s |
| Perda com `SIGTERM` durante carga, TCP | **zero**, reconciliada contra o stream |
| Perda com FATAL do sink | **zero**, via dump e reingestão |

---

## 19. Matriz de garantias

Leia junto com §2.3: a garantia só existe no transporte que confirma (TCP).

| Combinação | A confirmação significa | Perda em `kill -9` | Perda em `SIGTERM` | Perda em FATAL do sink |
|---|---|---|---|---|
| `memory` → `nats` (`jetstream`) | aceito na RAM deste nó | o backlog do ring | nada, se o dreno couber em `drain_timeout`; o resto vai para o dump | nada: o dump (§12) |
| `memory` → `nats` (`core`) | aceito na RAM deste nó | o backlog do ring | o que nenhum assinante pegou, mais o que não coube no dreno (para o dump) | idem, mais o dump |

Depois do `PubAck`, a durabilidade é a do stream (`storage`, `num_replicas`,
`sync_interval`/`sync_always` do `nats-server`). Em `core` não há durabilidade
nenhuma depois do servidor: o sucesso é "o servidor recebeu". Um dump que falha
(§12.3) é perda, contada em `reason="emergency_failed"`.

| Transporte | A confirmação existe? | O que a substitui |
|---|---|---|
| TCP | em parte: a retomada da leitura | nada se perde, mas o emissor não sabe *qual* mensagem foi aceita |
| unix dgram | não | o bloqueio do `send` pelo kernel |
| UDP | não | `syslog_messages_dropped_total` e os contadores da interface |

Nenhum transporte deste servidor confirma **por mensagem**: era o papel do RELP,
que virou software próprio (§1.2). Quem precisa de `kill -9` sem perda usa o
`ring=sqlite` do `go-syslog-server`.

---

## 20. Invariantes

Se uma alteração conflita com um destes, ela está errada. A numeração é a do pai;
os que perderam o objeto ficam com a nota.

1. A confirmação nunca sai antes de o item estar no ring; onde não há
   confirmação, há contador.
2. Ring cheio nunca vira aceitação seguida de descarte. UDP e unix descartam
   antes de aceitar; TCP segura o emissor.
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
11. Removido no go-syslog-nats-server (era do rueidis, §1.2).
12. O JLH é idêntico no worker e no dump: a mesma conversão, com a mesma config
    congelada.
13. **O JLH é função de cabeçalho + cru + config de conversão congelada** (§3.5):
    mesmo `uuid`, mesmos bytes, em retry e dump. A reingestão não converte.
14. **Uma amostra de relógio por mensagem**, e dela saem o `uuid` e o `event`.
15. `ring` e `sink` são ortogonais: nenhum consulta o outro.
16. Toda escrita em disco fica sob `server.data_dir` (`/data` no contêiner),
    inclusive a identidade, **por padrão**: a única exceção fixa é o socket
    `listen.unix`. Um `emergency.dir` absoluto fora dele é decisão do operador
    (um disco mais rápido), aceita com `WARN` no boot (§13.2).
17. O servidor nunca cria o que valida: o sink nunca cria nem altera um stream.
18. Configuração e permissão do destino são `fatal`, nunca `logic` (credenciais
    recusadas, permissão negada, stream ausente ou de outro dono, JetStream
    desligado, servidor que exige TLS, 4xx desconhecido da API); erro desconhecido é `network`,
    nunca `logic`.
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

---

## 21. Roteiro pós-v1

Nada disto entra antes de a v1 estar em produção e §18.8 ter sido medido.

| Item | Motivo de ter ficado fora |
|---|---|
| RELP | Software próprio, `go-relplog-server/` |
| Importação de arquivo (`transport=file`, Caso 2 do JLH) | Fora da v1 |
| `space` por listener ou rede | Multi-tenant numa instância; fora da v1 |
| TLS (listener ou cliente NATS) | Fora por decisão do usuário (§1.2); voltar exige decisão nova, não só código |
| Resposta a sonar, `ping` e `pong` | Até existir um `go-loghub-driver` (§11) |
| Ferramenta de reingestão manual de um `.jsonl` (ou de um `.corrupt` reparado) | Hoje basta copiar o arquivo para `emergency.dir` com o nome `emg-*.jsonl` e reiniciar |

### 21.1 Pendências conhecidas

Defeitos reais do código atual, conhecidos e ainda não corrigidos. Não são
roteiro: valem já. Onde esta SPEC descreve o comportamento certo, a divergência
é bug do código; onde o item diz "a decidir", a correção espera uma decisão do
usuário, registrada em `docs/DECISOES.md`. Quem corrige um item o tira daqui e
deixa a regra na seção citada, com teste.

1. *Corrigido em 2026-10-14:* custo fixo por conexão TCP — leitor de 4 KiB em vez de 64 e buffer de frame acima de 16 KiB solto antes de uma espera (`frameKeepCap`, só com o leitor vazio); regra em §5.4 e §15.1.
2. *Decidido em 2026-10-14 (DECISOES §9, contestável):* teto por IP contornável com IPv6 — `tcp.max_conns_per_ip` conta o endereço exato (um /128), sem prefixo, como no `go-syslog-server` e no `go-relplog-server`; regra em §4.4, trava em `TestPerIPLimitCountsTheExactAddress`.
3. *Corrigido em 2026-10-14:* o shutdown deixou de ser contado como falha do PROXY (`closing && (err != nil || mode == required)`, a forma do `go-syslog-server`); regra em §4.6, com o prazo vencido no meio da assinatura documentado ali como comportamento conhecido sem perda.
4. *Decidido em 2026-10-14 (DECISOES §9, contestável):* offset ISO fora de faixa na RFC 3164 segue a skill (aplicado como veio, sem aviso, sem desvio D9), como no `go-syslog-server` e no `go-relplog-server`; regra em §6.5.1 (e a ressalva em §6.9), trava em `TestRFC3164ISOOffsetIsNotRangeChecked`.
5. *Corrigido em 2026-10-14 (porte do RP/RN):* o aborto fecha o ring antes de contar, e o `Enqueue` reconfere o fechamento depois da reserva; regra em §13.4.
6. *Corrigido em 2026-10-14 (porte do RP/RN):* `ring.max_bytes` abaixo da classe do pool de `syslog.max_message_bytes` + 256 B é exit 1; regra em §13.2 e §15.1.
7. *Corrigido em 2026-10-14 (porte do RP/RN):* `admin.bind` e `listen.tcp` no mesmo endereço TCP são exit 1 na validação (`sameStreamSocket` do `go-syslog-server`); regra em §13.2 e §14.
8. *Corrigido em 2026-10-14 (porte do RP/RN):* `SIGHUP` não relata mais a chave derivada (`emergency.dir`) junto com a que a deriva; regra em §16.3.
9. *Corrigido em 2026-10-14 (porte do RP/RN):* a discagem falha da reconexão NATS não é logada; só o veredito vai a `ERROR`; regra em §10.7.
10. *Corrigido em 2026-10-14 (porte do RP/RN, achado da triagem):* servidor que exige TLS só é veredito com um servidor no *pool* (`soleServer` do `go-syslog-server`), no boot e depois; regra em §10.7.
11. *Decidido/registrado em 2026-10-14 (porte do RP/RN):* a falha ao remover um dump já entregue continua só `ERROR`; regra em §12.2 / DECISOES §10.
12. *Decidido/registrado em 2026-10-14 (porte do RP/RN):* dump publicado no instante do aborto fica contado e no disco, e o aborto não espera o disco; regra em §12.3 / DECISOES §10.
13. *Decidido/registrado em 2026-10-14 (porte do RP/RN):* `syslog_sink_errors_total{kind="fatal"}` conta a recusa fatal mesmo com o boot cancelado; regra em §14.1 / DECISOES §10.
14. *Decidido/registrado em 2026-10-14 (porte do RP/RN):* contadores da recepção sem *padding*, sem defeito observável; lacuna de medição em TESTING §8 / DECISOES §10.
15. *Corrigido em 2026-10-14 (revisão independente):* com `proxy.protocol=required`, um cabeçalho PROXY **válido** já no leitor quando o shutdown chega segue para a moldura, e as linhas que vieram com ele entram no ring; só a espera (sem cabeçalho, ou com erro) fecha sem contar (A1); regra em §4.6.
16. *Corrigido em 2026-10-14 (revisão independente):* `sameStreamSocket` compara valores: curinga por `IsUnspecified()` (`[::0]`, `0:0::0`) e porta como número (`0514` = `514`, nome de serviço resolvido, porta irresolúvel recusada) (D1 do syslog, D4 do RELP); regra em §13.2.
17. *Registrado em 2026-10-14 (revisão independente):* o "até 8 KiB quando ociosa" vale entre frames, com o leitor vazio: um frame grande seguido de alguns bytes do próximo segura o buffer grande até o idle; soltar no ponto de espera dentro do frame foi medido (24 KiB: 10× mais lento, 128× mais alocação) e recusado (A5, D4); regra em §5.4 e §15.1.
18. *Registrado em 2026-10-14 (revisão independente):* *pool* NATS só de servidores que recusam o TLS nunca dá veredito (FATAL só pelos fusíveis; D2 do syslog), e uma URL única cujo nome resolve a vários IPs dá veredito pela recusa do IP sorteado (D3 do RELP); regra em §10.7.
19. *Registrado em 2026-10-14 (revisão independente):* sobrecontagem transitória do aborto também por um `Enqueue` que reservou a vaga e recua por falta de bytes (D3 do syslog); e o comentário do `Memory.Enqueue` corrigido ("pelo menos um dos dois", D1 do RELP); regra em §13.4.

Os itens 5 a 11 da lista de 2026-10-13 (`retention: interest` aceito, 4xx transitórios da API
como `fatal`, assinatura negada em `_INBOX` sem veredito, dump de outro
Workspace em `core` sem aviso, gauges de emergência zerados com dump no disco,
log falso de remoção e de quarentena, `SIGQUIT` tolerado no shutdown por FATAL)
foram corrigidos no porte de 2026-10-13 e vivem em §10.7, §12.2 e §13.4.
