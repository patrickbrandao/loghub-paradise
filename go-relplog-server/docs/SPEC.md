# go-relplog-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-11
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-relplog-server`
Binário: `go-relplog-server`
Entrada: `docs/DECISOES.md` (decisões de 2026-10-11). Cada decisão de lá está
aqui, com o número da seção.
Origem: a especificação do `go-syslog-server` (S) de 2026-10-11, da qual este
documento é uma cópia adaptada — conversão, rings, sinks, beacon, dump, ciclo de
vida, configuração e invariantes são os dele, com as pendências dele corrigidas
—, e a especificação do RELP extraída da standard em 2026-10-05
(`go-relplog-server/docs/SPEC.md` daquela data), corrigida pela leitura do código
do `librelp` (§5).

Servidor de ingestão de syslog por **RELP** do ecossistema **LogHub**. Recebe as
mensagens que o `rsyslog` envia com o `omrelp`, em claro ou sobre TLS, confirma
**cada uma** só depois da garantia do ring, converte cada uma num envelope
**JLH** com `type=syslog` e o entrega a um destino configurável — por padrão, o
eventbus NATS JetStream em `loghub.{space}.syslog`, o mesmo do `go-syslog-server`.
É um **Bridge-IN**: cria a primeira mensagem de cada fato no eventbus
(`step = 0`).

Onde esta spec repete a do S é de propósito; onde diverge, a divergência é
justificada no lugar. As seções 4 e 5 são as que mudam de verdade.

---

## 1. Escopo

### 1.1 Objetivos

- **Nunca confirmar** uma mensagem que não foi aceita segundo a garantia da
  combinação `ring` × `sink`, e **nunca recusar** com `500` o que vai passar
  sozinho: o RELP é o único transporte em que as duas promessas podem ser
  cumpridas ao pé da letra (§2.3, §5).
- Cada mensagem aceita vira **exatamente um** envelope JLH válido pelos schemas
  das skills `loghub-json-schema` e `loghub-type-syslog-schema` (§3).
- Máximo de mensagens por segundo com mínimo de CPU e memória, inclusive com a
  janela de transações em voo de cada conexão (§5.5, §5.6).
- **Sem perda e sem invenção** no payload: o que chegou está nos campos ou em
  `raw`; o que não chegou é omitido, nunca adivinhado (§6).
- Operar acoplado ao eventbus (`sink=nats`, `sink=redis`), de forma autônoma
  (`sink=sqlite`) ou contra um banco compartilhado (`sink=postgres`,
  `sink=mysql`, `sink=clickhouse`).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| UDP, unix datagram, syslog TCP da RFC 6587, TLS da RFC 5425 | Não confirmam por mensagem; são do `go-syslog-server` (decisão 12) |
| `relp` no enum de `transport` | A skill não tem; o RELP vai como `x-transport` (decisão 2, §3.2) |
| Deduplicação de retransmissão | O RELP não carrega id de mensagem (decisão 3, §19) |
| DTLS, BEEP, importação de arquivo | Como no S |
| `space` por listener ou rede | Um Workspace por instância na v1 |
| Sonar, ping e pong | Só o heartbeat beacon (§11) |
| Validação por JSON Schema em execução | Só em teste e CI (§18.2) |
| Autenticação de emissor fora do TLS | Não existe no protocolo; firewall, rede privada ou mTLS (§4.5) |
| Roteamento por facility para destinos distintos | Um destino por instância |
| Comandos RELP além de `open`, `syslog` e `close` | O `librelp` só usa esses (§5.4) |

### 1.3 Fontes normativas e desvios

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, `jlh-envelope.schema.json`, ciclo de vida, plano de controle |
| skill `loghub-type-syslog-schema` | consultada em 2026-10-11 (sem `relp` em `transport`) | Payload `type=syslog`, schema, algoritmo de conversão, 24 vetores dourados |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 | AgentUUID, Workspace, AgentName, Hostname, MachineID, `DATADIR` |
| skill `loghub-postgres-table-inbox` | consultada em 2026-10-06 | `sink=postgres` (§3.7.1) |
| `librelp` (`rsyslog/librelp`, `src/`) e `rsyslog/plugins/omrelp` | lidos em 2026-10-11 | O comportamento do cliente: o que cada resposta significa (§5.4) |

Cópias dos dois schemas, dos exemplos de envelope e dos vetores ficam em
`internal/jlhcheck/loghub/` (§18.2). Onde as duas skills divergem, vale a mais
restritiva.

**Desvios deliberados das skills**: os mesmos do S, com os mesmos motivos (o
D1 deixa de existir, porque não há moldura a detectar), mais o D9:

| # | Desvio | Skill diz | Aqui | Seção |
|---|---|---|---|---|
| D1 | Moldura | TCP detecta por frame | **não se aplica**: o RELP delimita por `DATALEN`, que é contagem de octetos; `framing` é sempre `octet-counting` | §5.1 |
| D2 | TAG sem timestamp | sem timestamp, nenhuma TAG | TAG **estrita** ainda extrai `appname`/`procid` | §6.5.5 |
| D3 | Validação | todo envelope passa no schema antes da fila | schema só em teste e CI; o serializador é provado por fuzz | §18.2 |
| D4 | `LOGHUB_UUID_LEVEL` | variável sugerida | **não existe**: nível 3 fixo | §3.1 |
| D5 | Opções de importação | `reference`, `transport=file` | fora | §1.2 |
| D6 | Espaço em branco nos campos `^\S+$` | o conversor de referência emite o valor | o valor é omitido; o conjunto é o `\s` do ECMAScript mais U+001C–U+001F e U+0085 | §6.4.1 |
| D7 | Avisos de encoding de um SD seguido de texto colado | a referência funde os avisos | descartados, como em qualquer SD que falhou | §6.4.3 |
| D8 | Timestamp fora dos anos 0001–9999 | a referência emite fora do schema | `timestamp-invalid` e `timestamp` omitido | §6.4.2 |
| D9 | Extensão | — | `x-transport: "relp"` em todo envelope (permitido pelo `^x-` do schema) | §3.2 |

---

## 2. Arquitetura

### 2.1 Dois eixos ortogonais

- **RING**: *onde a mensagem espera* entre ser aceita e ser entregue.
  `memory` | `sqlite` | `redis` | `none`
- **SINK**: *para onde o envelope vai*, o destino final.
  `nats` | `redis` | `sqlite` | `postgres` | `mysql` | `clickhouse` | `none`

As vinte e oito combinações são válidas, com as degradações de §9.7.
**Nenhuma implementação de ring consulta o tipo do sink, e vice-versa.**

```
   rsyslog (omrelp), qualquer cliente librelp
      |
      |  RELP :20514        RELP sobre TLS (listen.tls)
      v
 +-----------------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                          |
 |   [PROXY] -> [TLS] -> sessão RELP: frame -> truncamento -> amostra     |
 |   -> janela (relp.window) + teto em voo (relp.inflight_max_bytes)     |
 |   -> por transação: PRI -> filtro -> rate-limit -> uuid -> Item        |
 |   -> ring -> 200 OK (escritor de respostas da conexão)                 |
 +----------------------------------+------------------------------------+
                                    |
                     +--------------v---------------+
                     | RING                         |
                     |  memory  canal + tetos       |
                     |  sqlite  ring.db, fsync      |
                     |  redis   stream compartilhado|
                     |  none    sem buffer          |
                     +--------------+---------------+
                                    | sink.workers
                     +--------------v---------------+
                     | CONVERSÃO -> JLH type=syslog |  <- fora do caminho quente
                     +--------------+---------------+
                                    |
                     +--------------v---------------+
                     | SINK                         |
                     |  nats      PubAck (ou core)  |
                     |  redis     XADD em pipeline  |
                     |  sqlite    syslog_events     |
                     |  postgres  loghub.inbox      |
                     |  mysql     INSERT em lote    |
                     |  clickhouse INSERT nativo    |
                     |  none      descarta          |
                     +--------------+---------------+
                                    |
   loghub.{space}.syslog (JetStream) | stream Redis | syslog_events | loghub.inbox |
   inbox_syslog (ClickHouse) | /dev/null

   beacon (só com sink=nats): <ctl_namespace>.beacon a cada 15 s (§11)
   emergência: ring=memory não vazio + sink FATAL => dump em
               <data_dir>/emergency/emg-<agent_uuid>-<unixnano>.bin (§12)

 +-----------------------------------------------------------------------+
 | porta admin (admin.bind, padrão 127.0.0.1:9103)                        |
 |   /healthz   /readyz   /metrics                                        |
 +-----------------------------------------------------------------------+
```

O ring é **único e global**, compartilhado pelos dois transportes: uma métrica
única de profundidade, backpressure justo (uma transação só é segurada quando o
sistema inteiro está saturado) e código simples. O teto de cópias em voo
(§5.6) é igualmente único.

### 2.2 Onde a conversão acontece, e por quê

Decisão 7 do S. A recepção faz o mínimo que permite decidir se a mensagem entra;
a conversão completa roda no **worker de dreno**.

| Etapa | Onde roda | Custo |
|---|---|---|
| PROXY, handshake TLS, `open` | recepção, uma vez por conexão | do transporte |
| Moldura RELP, truncamento | recepção (leitor da conexão) | inevitável |
| Amostra de relógio | recepção (leitor), no fim do frame | uma leitura de relógio |
| Vaga na janela e no teto, cópia | recepção (leitor) | uma cópia do pool |
| **PRI** (`<134>`), filtro, rate-limit, `uuid` | **recepção** (goroutine da transação) | 5 bytes, inteiros, um UUID |
| RFC 3164 / RFC 5424, SD, UTF-8 | **worker de dreno** | proporcional à linha |
| Serialização do JLH | **worker de dreno** | proporcional aos campos |

O PRI fica na recepção porque o filtro (§7) depende dele, e descartar depois do
ring seria aceitar para jogar fora. O resto sai do caminho quente: a CPU da
conversão é paga pelos `sink.workers`, que é onde a vazão é elástica, e a
confirmação RELP não espera por ela.

Consequências que o resto da spec assume:

1. **O item no ring não tem envelope.** Ele carrega a linha crua e o que a
   conversão **não pode** recuperar dela: `uuid`, amostra de relógio,
   transporte, endereços, sessão TLS, tamanho original, `from` e `space`. É o
   **cabeçalho do item** (§3.4).
2. **A contabilidade do ring conta só a linha crua.** O JLH é construído e
   consumido dentro do lote do worker, com teto próprio (`sink.batch_max_bytes`,
   §10.1, §15).
3. **`ring=sqlite`, `ring=redis` e o dump de emergência guardam cabeçalho + cru**,
   não JLH.
4. **O JLH é função determinística** de cabeçalho + cru + config de conversão
   (§3.5). Por isso **toda chave que muda os bytes do JLH exige restart** (§16.3).

Com `ring=none` não há worker: a conversão roda na goroutine da transação, que é
quem escreve no sink, e o `200 OK` espera a confirmação do destino. É o modo
explicitamente lento, aliviado pela janela.

### 2.3 Regra de aceitação

> A confirmação sai no instante em que a garantia da combinação configurada é
> cumprida — nunca antes. E uma recusa só é respondida quando é **final**: o
> que vai passar sozinho é segurado, e se não passar a tempo, devolvido ao
> emissor (§5.4, §5.7).

| `ring` | O `200 OK` significa |
|---|---|
| `memory` | o item entrou no canal do ring |
| `sqlite` | o `COMMIT` do grupo no `ring.db` terminou (com `fsync` se `synchronous=FULL`) |
| `redis` | o script de enfileiramento retornou (a durabilidade é a do `appendfsync` do Redis) |
| `none` | o **sink** confirmou a escrita (ou na hora, com `sink=none`) |

| Situação | Resposta | O emissor |
|---|---|---|
| aceita | `200 OK` | apaga a mensagem da fila dele |
| ring cheio, ring fora do ar, sink DOWN | nenhuma enquanto dura; estourado o prazo, a sessão fecha | segura; retransmite na sessão seguinte |
| filtrada, vazia | `200 OK` (descarte pedido pelo operador, contado) | apaga |
| recusa final do destino (`ring=none`) | `500 <motivo>` | **descarta e loga o erro**, suspende a ação e retransmite a janela |
| acima do rate-limit | nenhuma até haver ficha: segurada | desacelera para o limite |

**Diferente do S, aqui não há transporte que perde por desenho**: nada é
descartado sem que o emissor seja avisado (`500`) ou recupere a mensagem
(retransmissão).

### 2.4 Papel no LogHub

| Aspecto | Valor |
|---|---|
| Papel | **Bridge-IN**: traz dados de fora e publica como envelopes |
| Operação | **Produce** (primeira inserção): `uuid` novo, `step = 0`, `from` = AgentUUID |
| Subject | `loghub.{space}.syslog`; com `sink.nats.subjects = facility-severity`, `loghub.{space}.syslog.<facility>.<severity>` (§3.8). O mesmo `type` e os mesmos subjects do `go-syslog-server`: um consumidor de syslog não distingue a origem, a não ser por `payload.x-transport` |
| Plano de controle | heartbeat beacon (§11); nunca emite `ping`, `pong` ou sonar |
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
| `uuid` | UUIDv7 **nível 3** (`go-loghub-uuidv7`), em minúsculas, gerado **na recepção** com `GenerateAt(Level3, t)`, onde `t` é a amostra de relógio da mensagem. É cunhado depois de filtro, rate-limit e estado do sink (uma mensagem descartada não gasta `uuid`), mas sempre da amostra `t` do fim do frame. Gravado no cabeçalho do item e nunca regerado: a retentativa do laço de backpressure da transação RELP (§5.7), o retry do worker, o dump e a reingestão reusam o mesmo; se uma tentativa de destino incerto (`ErrUnavailable`, prazo do `ring=none`) tiver sido aceita, as duas cópias deduplicam por `(uuid, step)`. O nível 3 é fixo; `LOGHUB_UUID_LEVEL` não existe (D4). |
| `from` | AgentUUID de **quem recebeu** a mensagem (`lhident.AgentUUID()`), copiado para o cabeçalho do item. Com `ring=redis`, outra réplica pode drenar o item, e o `from` continua o de quem recebeu (§3.5). Nunca um identificador do emissor remoto: a origem externa está em `payload.host`. |
| `space` | Workspace de quem recebeu (`lhident.Workspace()`), copiado para o cabeçalho. Validado no boot (§13.2). |
| `type` | `"syslog"`. |
| `level` | `payload.severity.code` (0–7), inclusive quando o PRI veio do padrão. |
| `sid` | FNV-1a 64 da quíntupla de rede, reduzido a 53 bits (§6.10), com `transport` = `tcp` ou `tls`: agrupa os envelopes de uma conexão, e qualquer consumidor o recalcula pelo payload. |
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
| `transport` | `tcp` (RELP em claro) \| `tls` (RELP sobre TLS) | sempre |
| `framing` | `octet-counting` (o `DATALEN` do RELP é contagem de octetos) | sempre |
| `tls` | `{version, cipher, peer-subject, peer-fingerprint}` | só `tls` (§4.5) |
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

`transport` nunca é `udp`, `unix`, `dtls`, `file` nem `beep`, e `format` nunca é
`rfc3195` (§1.2). As extensões emitidas são `x-proxy` e `x-transport`.

**Por que `x-transport` e não `transport: "relp"`** (decisão 2): o enum de
`transport` da skill não tem `relp`, e o schema tem `additionalProperties: false`
com `^x-[a-z0-9][a-z0-9.-]*$` para extensões locais. `transport` diz o que o
socket é (`tcp` ou `tls`, o que mantém o `sid` recalculável pela fórmula da
skill e as combinações do schema: `tls` ⇒ `octet-counting` e `payload.tls`);
`x-transport` diz que os bytes vieram em frames RELP, confirmados um a um. O
valor vem do conversor (`convert.Config.XTransport`, fixado pela app como a
constante `app.XTransport`), não do cabeçalho: é constante na vida do processo e
mantém o JLH função pura de cabeçalho + cru + config (§3.5). Se a skill um dia
ganhar `relp` no enum, a mudança é desta seção.

**Omissão:** campo sem valor na origem (NILVALUE, inexistente no formato,
inválido) é **omitido**. Nunca `null`, nunca `""`, exceto `message`. Objetos
sem conteúdo (`host`, `tls`, `parse`, `structured-data`) também são omitidos.

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

### 3.4 Cabeçalho do item

O que a conversão **não consegue** recuperar da linha crua e que, portanto,
viaja ao lado dela pelo ring, pelo `ring.db`, pelo stream do ring Redis e pelo
dump:

```json
{"v":1,"uuid":"019a...","from":"0196...","space":"default","uts":1791028800,"sub":123456789,
 "transport":"tcp","framing":"octet-counting","src-ip":"10.0.0.3","src-port":40112,
 "dst-ip":"10.0.0.10","dst-port":20514,"size":175}
```

| Chave | Conteúdo |
|---|---|
| `v` | versão do codec, `1`. Outra versão é linha indecodificável (§9.4). |
| `uuid` | §3.1 |
| `from`, `space` | identidade de quem recebeu |
| `uts`, `sub` | a amostra de relógio |
| `transport`, `framing` | §3.2 |
| `src-ip`, `src-port`, `dst-ip`, `dst-port` | endereços **canônicos** (§4.2), já resolvidos pelo PROXY quando houver |
| `peer-ip`, `peer-port` | o par TCP real, **só** quando o PROXY protocol foi usado |
| `tls` | objeto de §4.5, só no `tls` |
| `size` | o `DATALEN` original; `size > len(cru)` significa truncamento (§5.3) |

O PRI **não** está no cabeçalho: ele é derivável do cru, e a conversão o relê
com a mesma função que a recepção usou (§6.2). Guardá-lo seria criar duas
fontes da mesma verdade.

Um codificador, três consumidores: a coluna `head` do `ring.db`, o campo `h` do
stream do ring Redis e o dump de emergência. É JSON porque roda uma vez por
mensagem fora do caminho quente (com `ring=memory` nem é serializado), e um
formato binário próprio seria um parser novo de entrada que um disco corrompido
torna arbitrária. O codec é o do S e aceita todo transporte e moldura da skill
(o conversor também: os vetores dourados de UDP e unix continuam rodando,
§18.2); a recepção deste servidor só produz `tcp`/`tls` com `octet-counting`.
O decodificador é alvo de fuzz (`FuzzHead`, §18.3): recusa
`v` desconhecido, chave obrigatória ausente, tipo errado, IP não canônico,
porta fora de 0–65535 e um `uuid` cujo instante (em ms) não é o de `uts`/`sub`,
sem pânico. Esta última regra existe porque `uuid` e amostra são **uma** amostra
(§3.1): um cabeçalho em que discordam não foi escrito por este programa, e o
envelope que ele produziria quebraria a regra do JLH de que, no tempo real, o
instante do `uuid` é o `event`.

### 3.5 Determinismo

> **Invariante:** o JLH é função pura de `(cabeçalho, cru, config de conversão)`.
> Dois processos com a mesma config que leiam o mesmo par produzem o mesmo JLH
> byte a byte.

É o que torna exatos a reingestão (§12), o dreno de `ring=sqlite` no boot e o
`XAUTOCLAIM` do ring Redis, e é o que faz um retry reenviar os mesmos bytes com o
mesmo `uuid`, como o outbox do JLH exige.

A **config de conversão** é o conjunto `[syslog]` (§16.5): `default_priority`,
`rfc3164_timezone`, `payload_raw`, `legacy_charset`, `accept_malformed`,
`sd_max_elements` e `sd_max_params`. Ela é **congelada no boot** (§16.3).
`max_message_bytes` atua na recepção, mas também exige restart.

Duas consequências registradas:

- **Réplicas com ring Redis precisam da mesma config de conversão.** Uma réplica
  que drena o item de outra produziria bytes diferentes para o mesmo `uuid`. O
  boot não tem como verificar isso; a SPEC exige, e o `docker/README.md` repete.
- **Atualizar o binário com backlog no ring** pode mudar os bytes de um `uuid`
  se a conversão mudou entre versões. O consumidor deduplica por `(uuid, step)`,
  então o efeito é no máximo uma segunda versão do mesmo fato, nunca uma perda.

### 3.6 Entrada do stream Redis (`sink=redis`)

```
XADD <sink.redis.stream> [MAXLEN ~ N] * e <jlh>
```

Um campo só, `e`, com o JLH de §3.1 byte a byte. Não há `v` (a versão do
contrato está no próprio JLH, que é autodescritivo pelo `type`) nem `b` (o cru
está em `payload.raw`, decisão 4).

`sink.redis.stream` tem padrão `loghub.{space}.syslog`, com `{space}` expandido
**por envelope**, com o `space` do próprio envelope: a mesma regra do subject NATS
(`loghub.{space}.{type}` é do JLH), para que o consumidor de um Redis eventbus use
a mesma convenção. Na prática é sempre o Workspace desta instância; a exceção é a
reingestão de um dump deixado por uma vida com outro Workspace (§12.2), cujos
itens vão, com o `space` com que foram recebidos, para o stream e o subject
desse `space`. A sonda de saúde mira o stream do Workspace atual. Outro nome
literal (sem `{space}`) é aceito e usado como está.

### 3.7 Tabela de eventos (`sink=sqlite`, `sink=mysql`) e fila `loghub.inbox` (`sink=postgres`)

O PostgreSQL não recebe esta tabela: ele recebe a fila comum do LogHub (§3.7.1).
O que segue vale para `sink=sqlite` e `sink=mysql`.

Destino **final**, append-only. Uma linha por envelope: o JLH inteiro em `env` e
cópias de campos para consulta. A chave é **`(uuid, step)`**, a mesma da
deduplicação do JLH: uma redelivery não duplica a linha.

| Coluna | Origem | Ausente |
|---|---|---|
| `uuid` | `uuid` | — |
| `step` | `step` (sempre 0 aqui) | — |
| `space` | `space` | — |
| `level` | `level` | — |
| `sid` | `sid` | — |
| `ev_uts`, `ev_sub` | `event.uts`, `event.sub` | — |
| `facility`, `severity` | `payload.facility.code`, `payload.severity.code` | — |
| `format`, `transport` | `payload.format`, `payload.transport` | — |
| `src_ip` | `payload.host.src-ip` | — (sempre presente: tcp e tls têm par, e a origem não endereçável é recusada) |
| `hostname`, `appname`, `procid`, `msgid` | `payload.host.hostname`, `appname`, `procid`, `msgid` | `NULL` |
| `env` | o JLH, byte a byte | — |
| `nbytes` | `len(env)` | — |

**`NULL` é "não veio"**, e é diferente de `''` ("veio vazio", que o payload nunca
produz, salvo `message`). É a regra de omissão de §3.2 levada à coluna.

**As colunas de texto são cópias para consulta, não o dado.** Os valores do
payload já são UTF-8 válido (§6.7), mas podem conter NUL. O NUL vira U+FFFD **na
coluna**, nos dois bancos: o MySQL o aceitaria, e a troca é mantida para que um
mesmo `WHERE` responda igual no SQLite, no MySQL e na `loghub.inbox` (que faz a
mesma troca, §3.7.1). `env` é íntegro (§20.7).

**Não existe coluna `message`.** A busca textual é sobre `env`, que contém
`message` e `raw` com o escape JSON:

```sql
-- sqlite
SELECT uuid FROM syslog_events WHERE CAST(env AS TEXT) LIKE '%erro%' AND ev_uts > ?;
-- mysql (LONGBLOB: comparação byte a byte, sensível a maiúsculas)
SELECT uuid FROM syslog_events WHERE env LIKE '%erro%' AND ev_uts > ?;
```

O `CAST` é obrigatório no SQLite: `LIKE` sobre um `BLOB` não casa nada e devolve
zero linhas sem erro. `env` é `BLOB`/`LONGBLOB` e não um tipo JSON: guardar o
documento como JSON do banco reordenaria chaves e perderia a fidelidade byte a
byte que estes dois destinos prometem.

```sql
-- MySQL 8.0+ / MariaDB 10.6+ (docker/mysql/schema.sql)
CREATE TABLE IF NOT EXISTS syslog_events (
  uuid      BINARY(16)   NOT NULL,
  step      TINYINT      NOT NULL,
  space     VARCHAR(64)  NOT NULL,
  level     TINYINT      NOT NULL,
  sid       BIGINT       NOT NULL,
  ev_uts    BIGINT       NOT NULL,
  ev_sub    INT          NOT NULL,
  facility  TINYINT      NOT NULL,
  severity  TINYINT      NOT NULL,
  format    VARCHAR(16)  NOT NULL,
  transport VARCHAR(16)  NOT NULL,
  src_ip    VARCHAR(64),
  hostname  LONGTEXT,
  appname   LONGTEXT,
  procid    LONGTEXT,
  msgid     LONGTEXT,
  env       LONGBLOB     NOT NULL,
  nbytes    INT          NOT NULL,
  PRIMARY KEY (uuid, step),
  INDEX sev_uts  (ev_uts),
  INDEX sev_host (hostname(255), ev_uts),
  INDEX sev_sev  (severity, ev_uts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

```sql
-- SQLite (criado pelo próprio servidor no events.db, §10.4)
CREATE TABLE IF NOT EXISTS syslog_events (
  uuid TEXT NOT NULL, step INTEGER NOT NULL, space TEXT NOT NULL,
  level INTEGER NOT NULL, sid INTEGER NOT NULL,
  ev_uts INTEGER NOT NULL, ev_sub INTEGER NOT NULL,
  facility INTEGER NOT NULL, severity INTEGER NOT NULL,
  format TEXT NOT NULL, transport TEXT NOT NULL,
  src_ip TEXT, hostname TEXT, appname TEXT, procid TEXT, msgid TEXT,
  env BLOB NOT NULL, nbytes INTEGER NOT NULL,
  PRIMARY KEY (uuid, step)
) STRICT;
CREATE INDEX IF NOT EXISTS sev_uts  ON syslog_events(ev_uts);
CREATE INDEX IF NOT EXISTS sev_host ON syslog_events(hostname, ev_uts);
CREATE INDEX IF NOT EXISTS sev_sev  ON syslog_events(severity, ev_uts);
CREATE TABLE IF NOT EXISTS health (id INTEGER PRIMARY KEY CHECK (id = 1), ts INTEGER NOT NULL) STRICT;
```

Por que estes tipos:

- **`BINARY(16)` no MySQL**: dezesseis bytes, comparação binária e ordem
  cronológica (o UUIDv7 nível 3 ordena até o nanossegundo); a chave
  clusterizada do InnoDB recebe os inserts sempre no fim do índice. No SQLite,
  `TEXT` canônico em minúsculas.
- **`hostname`, `appname`, `procid` e `msgid` são `LONGTEXT` no MySQL.** A RFC
  3164 não tem teto para hostname nem para o PID, e a RFC 5424 mantém o valor
  acima do limite com `header-invalid` (§6.4.1). Com o modo estrito, um valor
  acima da coluna é `1406`, classe `logic`, e descarta o lote inteiro. O índice
  usa prefixo de 255.
- `utf8mb4_bin`: comparação byte a byte em `WHERE hostname = ?`.
- **InnoDB é exigido** (§10.6): a sonda de boot faz um `INSERT` numa transação
  revertida.

O que o servidor **exige** do MySQL, verificado no boot (§10.6): as dezoito
colunas com tipos compatíveis (extras toleradas; as colunas que o servidor
preenche precisam caber no maior valor dele, e só as de texto do emissor —
`hostname`, `appname`, `procid`, `msgid` — ficam com a largura por conta do
operador); um índice **único** exatamente sobre `(uuid, step)`, e nenhum outro;
permissão de escrita. MySQL e PostgreSQL **nunca** fazem DDL; o SQLite cria o
próprio arquivo, porque é local e é dele.

#### 3.7.1 Fila `loghub.inbox` (`sink=postgres`)

No PostgreSQL o destino é a **fila de espera `loghub.inbox`** da skill
`loghub-postgres-table-inbox`, comum a todo produtor LogHub, e não uma tabela
deste servidor. Ela guarda os envelopes que chegaram e ainda não foram
processados; um worker de outro software os retira em ordem de `uuid`
(`DELETE … FOR UPDATE SKIP LOCKED`, na mesma transação em que grava o resultado)
e os move para o armazenamento definitivo. Este servidor só **enfileira**.

| Coluna | Envelope | Conversão |
|---|---|---|
| `uuid` | `uuid` | `uuid` nativo; é a ordem da fila |
| `from_uuid` | `from` | `uuid` nativo (`from` é palavra reservada no SQL) |
| `event_at` | `event.uts` + `event.sub` | `time.Unix(uts, sub)` em UTC truncado no microssegundo, `timestamptz` |
| `received_at` | — | `DEFAULT now()` da tabela; o servidor não envia (mede o atraso da fila) |
| `sid` | `sid` | `bigint` |
| `step` | `step` | sempre 0 aqui |
| `level` | `level` | a severidade (§3.1) |
| `event_nsec` | `event.sub` | `sub % 1000`, o resto que o microssegundo não guarda |
| `space` | `space` | — |
| `type` | `type` | `syslog` |
| `payload` | `payload` | `jsonb`; com NUL, `raw` em base64 e U+FFFD nos demais textos (abaixo) |

`event.iso` não é gravado: é derivado de `event_at`. Nunca
`to_timestamp(uts + sub / 1e9)`: o `float8` erra 1 µs. A reconstrução sem perda
de `event` está na skill:

```sql
floor(extract(epoch FROM event_at))::bigint                                  AS uts,
(extract(microseconds FROM event_at)::bigint % 1000000) * 1000 + event_nsec  AS sub,
to_char(event_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')        AS iso
```

```sql
-- PostgreSQL 18+ (docker/postgres/inbox.sql é a cópia literal da skill)
CREATE SCHEMA IF NOT EXISTS loghub;

CREATE TABLE loghub.inbox (
  uuid        uuid        NOT NULL,                 -- envelope.uuid (UUIDv7) = ordem da fila
  from_uuid   uuid        NOT NULL,                 -- envelope.from
  event_at    timestamptz NOT NULL,                 -- event.uts + event.sub truncado em µs
  received_at timestamptz NOT NULL DEFAULT now(),   -- chegada na fila (mede o atraso)
  sid         bigint      NOT NULL DEFAULT 0,
  step        integer     NOT NULL DEFAULT 0,
  level       smallint    NOT NULL,
  event_nsec  smallint    NOT NULL DEFAULT 0,       -- resto de event.sub (0-999 ns)
  space       text        NOT NULL DEFAULT 'default',
  type        text        NOT NULL,
  payload     jsonb       COMPRESSION lz4 NOT NULL,

  CONSTRAINT inbox_pk PRIMARY KEY (uuid),

  CONSTRAINT inbox_uuid_v7_ck  CHECK (uuid_extract_version(uuid) = 7),
  CONSTRAINT inbox_from_v7_ck  CHECK (uuid_extract_version(from_uuid) = 7),
  CONSTRAINT inbox_space_ck    CHECK (space ~ '^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$'),
  CONSTRAINT inbox_type_ck     CHECK (type <> ''),
  CONSTRAINT inbox_level_ck    CHECK (level BETWEEN 0 AND 15),
  CONSTRAINT inbox_sid_ck      CHECK (sid >= 0),
  CONSTRAINT inbox_step_ck     CHECK (step BETWEEN 0 AND 65535),
  CONSTRAINT inbox_nsec_ck     CHECK (event_nsec BETWEEN 0 AND 999),
  CONSTRAINT inbox_event_ck    CHECK (event_at >= 'epoch'),
  CONSTRAINT inbox_payload_ck  CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT inbox_syslog_ck   CHECK (type <> 'syslog' OR (level <= 7 AND payload ? 'message')),
  CONSTRAINT inbox_signal_ck   CHECK (type NOT IN ('beacon','ping','pong') OR level IN (8, 9))
) WITH (
  autovacuum_vacuum_scale_factor        = 0,
  autovacuum_vacuum_threshold           = 5000,
  autovacuum_vacuum_insert_scale_factor = 0,
  autovacuum_vacuum_insert_threshold    = 5000,
  autovacuum_analyze_scale_factor       = 0,
  autovacuum_analyze_threshold          = 5000
);
```

Regras, e por que cada uma:

- **Enfileirar é idempotente:** `INSERT … ON CONFLICT (uuid) DO NOTHING`. A chave
  da fila é só `uuid` (a skill), e não o `(uuid, step)` da deduplicação do JLH.
  Aqui as duas coincidem: este servidor é Bridge-IN e só emite `step = 0`
  (§3.1). Um reenvio do mesmo envelope (retry do outbox, reingestão de um dump)
  não duplica a linha nem falha.
- **A fila não é fiel ao byte.** O `jsonb` reordena chaves e normaliza o
  documento: o envelope sai reconstruível **sem perda de informação**, não de
  bytes. Quem precisa dos bytes do JLH usa NATS, Redis, SQLite ou MySQL.
- **NUL.** O `jsonb` recusa o escape `\u0000` (`22P05`), e a classe `22`
  descartaria o pedaço inteiro (§10.2); um `message` ou um `raw` com NUL produz
  esse escape (§3.3). Só nesse caso raro a coluna recebe uma **cópia
  reconstruída** do payload: `raw` vai em base64 com `raw-encoding=base64` (a
  forma que o schema do payload já tem para texto que não viaja como está), então
  continua exato e `size` continua contando os octetos dele; em todos os outros
  textos (`message`, `hostname`, valores de SD...) o NUL vira U+FFFD, a mesma
  troca das colunas de texto de §3.7. A reconstrução passa por um mapa, o que não
  perde nada que o `jsonb` guardaria (ele não guarda ordem de chaves). Sem NUL —
  quase sempre — a coluna recebe o sub-slice do JLH, sem cópia. O JLH do pedaço e
  o dos outros destinos continuam íntegros (§20.7). O envelope reconstruído da
  linha passa nos dois schemas e nas regras semânticas da skill de syslog; perde
  só o NUL dos textos decodificados.
- **As CHECKs da tabela são contrato, não opção.** Todo envelope `type=syslog`
  daqui tem `level` ≤ 7, `payload.message` (§3.2) e `from` UUIDv7
  (`go-loghub-ident`), e nenhum é `beacon`: nenhuma CHECK recusa o que este
  servidor produz. Uma recusa (`23514`) seria `logic`, descartaria o pedaço, e é
  bug. O operador **não** remove CHECKs (regra 6 da skill).
- **Só o índice da PK** (regra 7 da skill). Consultas por host, severidade ou
  aplicação vão para o armazenamento definitivo, não para a fila.
- O consumo (`DELETE … USING job … RETURNING`, quarentena, monitoramento do
  atraso) é de outro software e está na skill; nada dele roda aqui.

O que o servidor **exige** da tabela, verificado no boot (§10.5): as **dez**
colunas que ele escreve, com tipos compatíveis; um índice **único** exatamente
sobre `(uuid)`; `INSERT` e `SELECT (uuid)` para o papel (o `ON CONFLICT (uuid)`
lê a coluna árbitra: só com `INSERT` o primeiro pedaço falha com `42501`).
`received_at` e as CHECKs não são
conferidas: o servidor não depende delas.

#### 3.7.2 Tabela `inbox_syslog` (`sink=clickhouse`)

No ClickHouse a tabela também **espelha** a `loghub.inbox` (§3.7.1): as mesmas
onze colunas, o mesmo mapeamento do envelope, com os tipos do ClickHouse. Não é
uma fila — o ClickHouse não tem `DELETE … FOR UPDATE SKIP LOCKED` —, e sim o
armazém analítico dos envelopes, lido por tempo e por `space`. O servidor
**nunca a cria** (§10.10): `docker/clickhouse/inbox_syslog.sql`.

| Coluna | Envelope | Tipo | Conversão |
|---|---|---|---|
| `uuid` | `uuid` | `UUID` | — |
| `from_uuid` | `from` | `UUID` | — |
| `event_at` | `event.uts` + `event.sub` | `DateTime64(6, 'UTC')` | truncado no µs, como em §3.7.1 |
| `received_at` | — | `DateTime64(6, 'UTC') DEFAULT now64(6)` | não é enviado |
| `sid` | `sid` | `UInt64` | — |
| `step` | `step` | `UInt16` | sempre `0` |
| `level` | `level` | `UInt8` | a severidade |
| `event_nsec` | `event.sub` | `UInt16` | `sub % 1000` |
| `space`, `type` | `space`, `type` | `LowCardinality(String)` | `type` é `syslog` |
| `payload` | `payload` | `JSON(`structured-data` Map(String, Map(String, Dynamic)))` | o texto do JLH a partir de `"payload":`, decomposto em subcolunas pelo servidor |

```sql
-- ClickHouse 25.8+ (docker/clickhouse/inbox_syslog.sql; o servidor nunca a cria)
CREATE TABLE IF NOT EXISTS inbox_syslog (
  uuid        UUID,
  from_uuid   UUID,
  event_at    DateTime64(6, 'UTC'),
  received_at DateTime64(6, 'UTC') DEFAULT now64(6),
  sid         UInt64,
  step        UInt16,
  level       UInt8,
  event_nsec  UInt16,
  space       LowCardinality(String),
  type        LowCardinality(String),
  payload     JSON(`structured-data` Map(String, Map(String, Dynamic)))
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_at)
ORDER BY (event_at, uuid)
SETTINGS non_replicated_deduplication_window = 1000;
```

- **Não é a `inbox` dos servidores de webhook.** A estrutura é a mesma, mas o
  *hint* do `payload` é do `type=syslog`; numa tabela compartilhada ele
  apareceria como `"structured-data":{}` em toda linha de webhook, e o deles
  (`params`, `query`) em toda linha de syslog. Decisão do usuário
  (2026-10-07): uma tabela por formato de payload.
- **`ORDER BY (event_at, uuid)`, não `uuid`.** O ClickHouse ordena `UUID` pela
  **segunda** metade (documentação do tipo): num UUIDv7 é a fração sub-µs e a parte aleatória, e o
  índice esparso de uma chave só com `uuid` não poda nada por tempo. Um `uuid`
  repetido tem sempre o mesmo `event_at` (a mesma amostra, §3.1), então a chave
  colapsa exatamente as reentregas.
- **`ReplacingMergeTree`:** duas linhas com a mesma chave viram uma no merge —
  a idempotência **eventual** do at-least-once. O token de §10.10.4 é a camada
  imediata; até o merge, a leitura exata usa `FINAL`.
- **`non_replicated_deduplication_window = 1000`**: num `MergeTree` sem réplica o
  padrão é `0` e o `insert_deduplication_token` é ignorado; o `Replicated*` já
  guarda os últimos blocos.
- **`PARTITION BY toYYYYMM(event_at)`:** retenção por `DROP PARTITION`, e um
  `uuid` cai sempre na mesma partição (a deduplicação é por partição).
- **Sem `CONSTRAINT … CHECK`.** Uma violação (`469`) seria `logic`, o pedaço
  inteiro, e a sonda de boot é um `INSERT` vazio que não a exercita: o boot
  recusa uma tabela com CHECK (§10.10.5).
- **O `payload` não é fiel ao byte**, como o `jsonb` (§3.7.1): o tipo `JSON`
  achata o documento em caminhos e reordena chaves. Strings continuam
  strings: a inferência de data e de número a partir de texto é desligada em
  todo `INSERT` (§10.10.3), porque com ela uma string escolhida por um cliente
  reescrevia o dado de outras linhas do lote (revisão independente de
  2026-10-07). `raw`
  continua exato (é uma string). NUL é aceito: um `\u0000` vai como está, sem a
  reconstrução que a `loghub.inbox` exige (medido).
- **`structured-data` como `Map`.** O tipo `JSON` **descarta objeto vazio**
  (medido no 25.8, sem ajuste que o evite): sem o *hint*, um elemento SD sem
  parâmetros (`[origin]` → `"origin":{}`, §6.4.3) sumia da linha lida de volta.
  E os SD-IDs e PARAM-NAMEs são as **únicas chaves de objeto que o emissor
  controla**: como chaves de `Map` ficam literais — com ponto inclusive —, sem
  virar caminho. Sem o *hint*, `[a b.c="1"][a.b c="2"]` dava dois caminhos
  `a.b.c` e o `INSERT` era recusado com `117`, o pedaço inteiro. O valor é
  `Dynamic` porque um PARAM-NAME repetido vira array (§6.4.3).
- **Regra de leitura:** uma linha sem `structured-data` volta com
  `"structured-data":{}` (um caminho tipado sai sempre). Este servidor **nunca**
  escreve um `structured-data` vazio (§3.2, omissão), então `{}` significa
  ausente, sem ambiguidade; o consumidor o remove. É a única diferença entre a
  linha e o JLH como valor JSON, e os testes a aplicam antes do oráculo.
- **Os dois ajustes de `JSON`** (§10.10.3) continuam em todo `INSERT` como
  defesa: fora do *hint* o payload `syslog` não tem chave de emissor.

O que o servidor **exige** da tabela (§10.10.5): motor `*ReplacingMergeTree` sem coluna `is_deleted`; partição, se houver, só
por `event_at`;
`uuid` como elemento da chave de ordenação; as dez colunas que ele escreve com
os tipos da DDL, colunas comuns (`DateTime64` de 6 a 9 dígitos e qualquer fuso;
`LowCardinality(String)` ou `String` em `space` e `type`; `JSON` com o *hint* e,
além dele, só `max_dynamic_paths`/`max_dynamic_types`); `received_at`, se
existir, `DateTime64` de ao menos 6 dígitos com `DEFAULT` ou `MATERIALIZED`;
nenhuma CHECK;
ClickHouse 25.8 ou mais novo; `INSERT` nas dez colunas e uma sessão gravável;
um `max_memory_usage` que comporte o pior pedaço.

### 3.8 Mensagem NATS (`sink=nats`)

```
Subject:  loghub.<space>.syslog                          (subjects = exact, padrão)
          loghub.<space>.syslog.<facility>.<severity>    (subjects = facility-severity)
Headers:  Nats-Msg-Id:     <uuid>.<step>
          Content-Type:    application/json
          X-LogHub-Type:   syslog
          X-LogHub-UUID:   <uuid>
          X-LogHub-Space:  <space>
Data:     <jlh>
```

- O subject exato é a regra do JLH: publicar no subject exato, nunca num
  curinga, e nunca ler `space` ou `type` de volta do subject.
- **Subjects granulares são opt-in** (decisão 18): `<facility>` e `<severity>`
  são os **nomes** RFC 5427 (`loghub.default.syslog.authpriv.info`). Tokens extras
  quebram assinaturas exatas (`loghub.default.syslog`) e terminadas em `*`; o
  consumidor assina `loghub.<space>.syslog.>`. A skill do envelope avisa contra
  isso; a do syslog permite. Fica desligado por padrão por isso.
- `Nats-Msg-Id` é `<uuid>.<step>`, **nunca** o `uuid` puro: um processador que
  encaminhe para o mesmo stream (mesmo `uuid`, `step + 1`) teria o salto legítimo
  descartado como duplicata pela janela do JetStream.
- Os headers `X-LogHub-*` ajudam o roteamento; o envelope continua a fonte da
  verdade.
- O bloco de headers tem tamanho **fixo e pequeno** (no máximo 256 bytes, com o
  `space` de 64), muito abaixo do teto de 64 KiB do JetStream (`10097`), porque o
  JLH vai em `Data`. A standard aprendeu isso contra um servidor real: o envelope
  num header estoura o teto com um hostname longo.

### 3.9 O maior evento, e por que ele é calculado e não configurado

Dois destinos têm teto por mensagem: `max_payload` no NATS (1 MiB por padrão) e
`max_allowed_packet` no MySQL. Uma mensagem aceita e depois recusada por tamanho
seria `logic`, e `logic` descarta o **lote inteiro** (com mensagens de outros
emissores). Por isso o boot compara o teto do destino com o pior JLH possível e
não abre os transportes quando ele não cabe (exit 3, com os dois números no log).

Com `L = syslog.max_message_bytes` (o cru nunca passa disso, §5.3):

```
pior JLH = 12 × L + 8 KiB
```

- **Texto convertido ≤ 6 × L.** `message`, `hostname`, `appname`, `procid`,
  `msgid` e cada SD-ID, nome e valor são fatias **disjuntas** do cru (um SD
  inválido vai para `message` e sai de `structured-data`, nunca os dois). No
  pior caso todo byte é de controle e vira `\u00XX` (6 bytes). Um octeto
  inválido vira U+FFFD (3 bytes, sem escape), e o charset legado também produz no
  máximo 3 bytes por octeto.
- **`raw` ≤ 6 × L**: texto com escape, ou base64 (4/3 × L) quando não é UTF-8.
- **8 KiB fixos**: chaves, números, `host`, `tls` (com `peer-subject` limitado a
  1 KiB, §4.5), `x-proxy`, `parse` e o cabeçalho do envelope.

Com o padrão de 64 KiB, o pior JLH é **776 KiB**: cabe no `max_payload` padrão
do NATS (1 MiB) com os headers de §3.8, e numa linha MySQL de
`776 KiB + 3 × L + 512 B` (as colunas de texto saneadas), muito abaixo dos
64 MiB padrão do MySQL 8. `TestWorstCaseJLHIsBounded` prova o teto com linhas
adversárias.

O número é pessimista de propósito: um fator "realista" deixaria um único
emissor quebrado derrubar lotes de mensagens já confirmadas de todos os outros.
Quem sobe `max_message_bytes` acima de ~85 KiB precisa subir o `max_payload` do
NATS junto, e o boot diz isso.

O ClickHouse não tem teto por mensagem no protocolo nativo (medido:
`format_binary_max_string_size` não se aplica). O teto dele é a **memória** do
`INSERT` — cerca de 6× os bytes do pedaço mais ~130 MiB —, e `241` é fatal: o
boot compara `max_memory_usage` com o pior pedaço, `max(sink.batch_max_bytes,
pior JLH)` (§10.10.5).

---

## 4. Transportes

Dois transportes, os dois RELP, cada um ligado por um parâmetro de `[listen]`
que, vazio, o desliga. **Os dois desligados é erro de configuração** (exit 1).

| Transporte | Parâmetro | Padrão | O que o emissor configura | `payload.transport` / `framing` |
|---|---|---|---|---|
| RELP em claro | `listen.tcp` | `:20514` | `omrelp` sem `tls` | `tcp` / `octet-counting` |
| RELP sobre TLS | `listen.tls` | `""` (desligado) | `omrelp` com `tls="on"` | `tls` / `octet-counting` |

UDP, unix datagram e as molduras da RFC 6587 **não existem** aqui (decisão 12):
o RELP só existe sobre fluxo, e quem precisa daqueles usa o `go-syslog-server`.
Os dois transportes podem ficar ligados ao mesmo tempo, em portas (ou IPs)
distintas; a mesma porta nos dois é exit 1 (§13.2).

### 4.1 As portas padrão

A imagem roda como uid 65532 e não abre porta abaixo de 1024. `:20514` é o
costume dos exemplos de RELP do rsyslog e já é alta; o TLS fica desligado até
haver certificado, com `:20515` como sugestão da documentação e do Docker. O log
de boot imprime o endereço efetivo de cada listener.

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
  por prefixo IPv6 foi recusado (decisão 16): os emissores RELP são servidores
  de endereço fixo, e um /64 pode ser uma rede inteira deles.
- `tcp.idle_timeout` (120 s) encerra uma sessão sem frame por esse tempo, com
  `serverclose` (§5.8); `0` desliga. O `omrelp` reconecta na próxima mensagem.
- `SO_KEEPALIVE` ligado, com período de 30 s, inclusive sob o limitador de
  conexões (o *wrapper* é desembrulhado para chegar ao socket).

### 4.5 RELP sobre TLS

Decisões 12 e 17 do S. Um listener `crypto/tls` (stdlib, sem dependência nova):
o handshake logo depois do `connect`, e a mesma sessão RELP do §4.4 dentro do
túnel, que é o que o `omrelp` fala com `tls="on"` (GnuTLS ou OpenSSL do lado do
librelp).

- Dentro do túnel só se fala RELP: uma linha syslog solta é frame malformado e
  fecha a conexão (`relplog_relp_frame_errors_total{reason="malformed"}`).
- `tls.cert_file` e `tls.key_file` obrigatórios com `listen.tls`; lidos **no
  boot** (a troca de certificado exige restart; §21).
- `tls.min_version` = `1.2` (padrão) ou `1.3`. As suítes são as padrão do Go.
- `tls.handshake_timeout` (10 s) protege contra *slowloris* no handshake. A
  falha de handshake conta em `relplog_tls_handshake_errors_total` e fecha; uma
  falha causada pelo shutdown não conta.
- **O leitor também escreve.** No TLS 1.3, um `KeyUpdate(update_requested)` do
  par faz o `Read` responder com uma escrita, fora do escritor da sessão. Essa
  escrita precisa de prazo: entre duas escritas de respostas, e antes da
  primeira, o prazo de escrita do socket é o `idle_timeout` (o escritor o arma
  depois de cada lote); o `Close` vence o prazo de leitura de toda sessão e, se
  uma não voltar em `closeGrace` (6 s: um prazo de escrita de respostas mais
  folga), fecha o socket dela.
- **Certificado do cliente** (`tls.client_auth`):

  | Valor | Comportamento | `tls.Config.ClientAuth` |
  |---|---|---|
  | `optional` (padrão) | pede o certificado; com `tls.client_ca`, verifica quando ele vem; sem CA, aceita sem verificar e registra `peer-*` (para pinning a jusante) | `VerifyClientCertIfGiven` com CA; `RequestClientCert` sem |
  | `require` | exige e verifica; `tls.client_ca` é obrigatório (exit 1 sem ele) | `RequireAndVerifyClientCert` |
  | `none` | não pede | `NoClientCert` |

  O `omrelp` com `tls.authMode="certvalid"` ou `"name"` e `tls.myCert` manda o
  certificado; `optional` com CA é o par natural disso.
- **`payload.tls`**, preenchido depois do handshake, uma vez por conexão, e
  gravado no cabeçalho do item:

  | Chave | Valor |
  |---|---|
  | `version` | `TLS1.2` ou `TLS1.3` (o `tls.VersionName` do Go sem o espaço) |
  | `cipher` | nome IANA da suíte (`tls.CipherSuiteName`) |
  | `peer-subject` | DN do certificado do cliente (RFC 4514); **omitido acima de 1 KiB** |
  | `peer-fingerprint` | `sha-256:` + os 32 octetos do SHA-256 do DER em hexadecimal **maiúsculo** separados por `:` (RFC 5425 §4.2.2) |

  Sem certificado de cliente, `peer-*` são omitidos. Um certificado apresentado
  e não verificado (`optional` sem CA) é registrado do mesmo jeito: o payload diz
  o que foi apresentado, não o que foi confiado.
- `[tls]` tem `idle_timeout`, `max_conns` e `max_conns_per_ip` próprios, com os
  padrões do `[tcp]`: são transportes distintos, com limitadores distintos. Os
  parâmetros do protocolo (`[relp]`) são os mesmos para os dois.

### 4.6 PROXY protocol

Opt-in nos dois transportes, para quando um balanceador (HAProxy, NLB, Traefik)
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
  assinatura de 12 bytes). O cabeçalho chega **antes** do handshake TLS. Prazo
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
  transporte continue registrada (§20.8). O rate-limit usa o `src-ip` resolvido.
- **Ressalva:** `proxy.trusted` largo demais permite forjar `src-ip`.

---

## 5. RELP

O RELP é o protocolo do `rsyslog` (o `librelp`, usado pelo `omrelp` e pelo
`imrelp`). Não é RFC, mas é a **única** forma difundida de fazer syslog com
confirmação por mensagem, e por isso é o único transporte em que a regra de
aceitação deste projeto pode ser cumprida ao pé da letra (§2.3). Esta seção
consolida a especificação extraída da standard em 2026-10-05, com as correções
que a leitura do código do `librelp` (2026-10-11) impôs; onde o comportamento do
cliente decide, o arquivo do `librelp` está citado.

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
- **Custo fixo por conexão** (decisão 11, pendência 1 do S): o `bufio.Reader`
  tem 4 KiB (o leitor só usa `ReadByte` e leituras contadas; o maior `Peek` é o
  do PROXY, de 12 bytes) e o escritor de respostas, outros 4 KiB; depois de um
  frame grande, o leitor descarta o buffer acima de 16 KiB em vez de segurá-lo
  enquanto a conexão viver ociosa. Com `max_conns = 0`, o custo de N conexões
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
servidor 1 rsp <n> 200 OK\nrelp_version=0\nrelp_software=go-relplog-server,<versão>\ncommands=syslog

cliente  2 syslog 34 <13>Sep 12 14:33:01 web-01 app: oi
servidor 2 rsp 6 200 OK

cliente  3 close 0
servidor 3 rsp 0
         (o servidor fecha a conexão)
```

- **`open`**: a resposta é `200 OK` seguida das ofertas no formato do
  `relpOffersToString` do `librelp` (uma por linha, sem `\n` final):
  `relp_version=0`, `relp_software=go-relplog-server,<versão>` (para um operador
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
  da sessão antes da do `close` (decisão 7; o código de referência podia mandar
  o `serverclose` antes de confirmações ainda em voo).
- **Comando desconhecido** recebe `500 command not supported` e a sessão continua:
  um comando novo é extensão do cliente, não fluxo quebrado. A métrica conta
  `command="unknown"`; a palavra do cliente nunca vira label.

**O que o `librelp` faz com cada resposta** (`scrsp.c`) decide tudo o que vem a
seguir:

| Resposta | O cliente | Logo, este servidor manda essa resposta para |
|---|---|---|
| `200 ...` | remove a mensagem da lista de não confirmadas: entregue | o que a garantia do ring cumpriu, e o que o operador mandou descartar |
| qualquer outro código (`500 ...`) | **destrói** a mensagem (`relpSendbufDestruct`), loga "peer sent error response" pelo `onErr` e **nunca a retransmite**; e mais: o erro devolvido pelo tratamento da resposta (`librelp error 10020`) **suspende a ação e quebra a sessão**, e as transações da janela já enviadas e ainda sem resposta são **retransmitidas** na sessão seguinte | só o que este servidor recusou de propósito, para sempre, e raramente |
| nenhuma (sessão quebrada: conexão fechada ou `serverclose`) | reconecta e **retransmite** tudo o que não teve `200` (`relpSessTryReestablish`) | o que precisa voltar ao emissor |

A especificação extraída dizia "`500 ring full`: o rsyslog retém e retenta". O
código do cliente diz o contrário: um `500` por ring cheio era uma perda **na
origem** a cada ring cheio (decisão 4). E o rsyslog real mostrou o resto
(`TestEmittersRELPFiveHundredIsFinal`, rsyslog 8.2410, 2026-10-11): a mensagem
recusada chega ao destino uma vez e nunca volta, mas o `500` suspende a ação, e
as vizinhas da janela que este servidor já tinha aceitado voltam como
**duplicatas** com outro `uuid` (5 a 6 por `500` na janela de 128). **Um `500`
custa uma reconexão e duplicatas**: por isso nada que aconteça em rajada recebe
`500` — nem o rate-limit (§8). A tabela completa:

| Desfecho (`receiver.Outcome`) | Resposta | Contagem |
|---|---|---|
| aceita | `200 OK` | `messages_accepted_total`, `confirm_duration_seconds` |
| filtrada (§7) | `200 OK` | `messages_dropped_total{reason="filter"}` |
| vazia (`DATALEN = 0`) | `200 OK` | `messages_dropped_total{reason="empty"}` |
| recusa `logic` final do destino (`ring=none`) | `500 refused by the destination` | `events_dropped_total{reason="sink_logic"}` (pelo sink, uma vez) |
| origem não endereçável (guarda) | `500 source address unusable` | `messages_dropped_total{reason="unaddressable"}` |
| ring cheio, ring indisponível, sink DOWN, **rate-limit** | **nenhuma enquanto dura**: segurada (§5.7, §8) | `backpressure_seconds_total` (e `ratelimit_held_total`); se estourar, `relp_unanswered_total{reason="backpressure"}` |
| shutdown | nenhuma | `relp_unanswered_total{reason="shutdown"}` |
| par sumiu / respostas não saem mais | nenhuma | `relp_unanswered_total{reason="conn_lost"}` |
| o servidor encerrou a sessão de um par presente (frame ruim, idle timeout, segundo `open`) | nenhuma para as que ficaram em voo | `relp_unanswered_total{reason="server_closed"}` |

A filtrada e a vazia recebem `200` porque o operador pediu o descarte (e uma
vazia não tem o que guardar): um `500` mentiria que houve erro, o rsyslog
logaria um erro por mensagem e quebraria a sessão a cada uma. A recusa `logic`
é final porque uma retransmissão seria recusada de novo; o `500` dela é raro, e
as duplicatas das vizinhas são o custo registrado.

**Uma transação devolvida não é descarte.** `relplog_messages_dropped_total`
conta só o que foi lido e descartado de propósito. A reconciliação de uma
sessão é:

```
relplog_messages_total = accepted + dropped{filter,empty,unaddressable}
                         + consumidas (sink_logic) + relp_unanswered_total
```

e a do lado do rsyslog: enviadas = `200` + `500` (perdidas, logadas lá) +
retransmitidas (as da sessão fechada sem resposta e as vizinhas de um `500`).

**Todo frame `syslog` lido entra na conta**, inclusive o que nunca ganhou
goroutine: a sessão pode terminar entre a leitura do frame e a vaga na janela ou
no teto de §5.6 (shutdown, aborto por backpressure de outra transação). Esse
frame conta em `relplog_messages_total` e em `relp_unanswered_total` com o motivo
da sessão; sem isso a igualdade acima furava a cada aborto (revisão
independente, 2026-10-11, `TestReviewReconciliationAfterAbort`). O motivo é o
**primeiro** registrado pela sessão: um aborto por backpressure seguido do
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
  a janela cheia, o leitor **para de ler** e o TCP segura o emissor. Com
  `ring=sqlite`, um `Enqueue` espera o group commit, e uma conexão só (o
  rsyslog abre exatamente uma por ação) seria serializada por ele: a janela é a
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
justamente quando o ring está cheio que elas se acumulam. Por isso (decisão 9):

- a cópia é feita **depois** de obter a vaga na janela **e** os bytes no teto
  (o código de referência copiava antes e chegava a `window + 1` cópias por
  conexão);
- **`relp.inflight_max_bytes`** (64 MiB) é um teto **global ao processo**,
  somando todas as conexões dos dois transportes. A conta de cada transação
  é **`2 × classe + 8 KiB`**: a classe do pool de §15.1 (2, 8 ou 64 KiB) da cópia
  da transação, outra para a cópia do item dentro do `Enqueue` (que com
  `ring=none` ou um group commit lento vive tanto quanto a transação), e 8 KiB
  pela goroutine, a pilha, o `Wire`, o timer e o fechamento (medidos em cerca de
  9,6 KiB no todo para uma linha pequena). Contar só a cópia deixava linhas
  pequenas porem até cinco vezes o teto na memória (revisão independente,
  2026-10-11, `TestTxnChargeCoversTheTransaction`). A conta é adquirida antes da
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

Ring cheio, ring indisponível (`ring=redis` fora do ar), sink DOWN ou emissor
acima do rate-limit (§8): a transação **não recebe resposta enquanto a condição
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

- **Nunca `500` para o que vai passar sozinho** (decisão 5): o `librelp` perde o
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
  `RecordDrop` são caminhos separados (bug real da standard). Pelo mesmo motivo,
  **filtro e rate-limit decidem uma vez por mensagem**: a retentativa não gasta
  outra ficha do balde nem reavalia um filtro trocado por `SIGHUP` durante a
  espera (`TestRELPHeldMessageIsChargedOnce`).
- O `uuid` cunhado na primeira tentativa é reaproveitado em todas: se uma
  tentativa de destino incerto (`ring=redis` indisponível, prazo do `ring=none`)
  tiver sido aceita, a segunda é o mesmo fato (§3.1).
- **O fim da sessão vence a decisão.** Uma transação confere o contexto da
  sessão depois de cada espera (a vaga no teto, os 50 ms entre tentativas, o
  rearme do prazo de leitura) antes de tentar o ring. Sem isso, uma transação
  que acordava depois do aborto ainda podia ser enfileirada, e o `librelp`, que
  a retransmite porque não teve resposta, a entregaria duas vezes
  (`TestReviewRetryAfterPeerLeft`). Uma aquisição do teto que volta depois do
  fim da sessão é devolvida na hora.
- **Um par que sumiu** (EOF, reset) com transações seguradas: elas são
  abandonadas (`conn_lost`), **nunca** enfileiradas depois pelas costas da
  retransmissão, o que duplicaria o evento.
- Com `ring=none`, uma recusa `logic` do sink é **final** para aquela mensagem
  (`500 refused by the destination`, §5.4), sem retentativa e sem contá-la de
  novo; `network`/indisponível é segurada como ring cheio; `fatal` segue §13.3.

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

O `serverclose` (decisão 6; era item "a decidir pelo usuário" no S) é a dica do
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
  termina o `Enqueue` e recebe a resposta: com `ring=sqlite`, o `Enqueue` espera o
  `COMMIT` mesmo com o contexto cancelado, porque voltar antes produziria uma
  resposta para uma linha que será commitada de qualquer forma.
- Frames ainda não lidos ficam sem resposta, e o rsyslog, ao ver o fim da sessão,
  retransmite o que não foi confirmado: nada se perde.
- Uma sessão que não volta em `closeGrace` (6 s), um par que parou de ler ou um
  leitor preso numa escrita de `KeyUpdate`, tem o socket fechado sob ela: o
  shutdown não espera um par.
- **Os dois transportes fecham ao mesmo tempo**: o `Close` de cada um corre em
  paralelo, e o shutdown espera **um** `closeGrace`, não um por transporte.
  Fechados em sequência, um par preso no TCP atrasava o fechamento do TLS em 6 s,
  e as confirmações decididas do TLS esperavam por nada.
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
estruturada (com o leitor do PROXY). Implementa o **algoritmo de conversão da
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
(`convert.ScanPRI`), usada pela recepção (para o filtro) e pela conversão:

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
`clock`): a tabela normativa é a da RFC 5427, que a skill adota. O filtro (§7)
aceita os nomes desta tabela.

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
Era a pendência 4 do S, "a decidir pelo usuário"; decidido aqui (decisão 16)
seguir a skill, sem desvio novo: um desvio de conversão quebraria a igualdade com o
`go-syslog-server`, que recebe o mesmo `type` e publica no mesmo subject. Se a
skill passar a conferir, os dois mudam juntos.

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
  mantém o determinismo (§3.5) numa reingestão feita meses depois.

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
timestamp reconhecível (handshake TLS numa porta texto, requisição HTTP, lixo
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
`relplog_events_dropped_total{reason="malformed"}` e log amostrado.

### 6.7 Texto e encoding

- **UTF-8 estrito** em todo texto (campos do cabeçalho, valores de SD,
  `message`): forma não mínima e surrogates são inválidos. **Cada octeto** que
  não inicia uma sequência válida vira **um** U+FFFD, com aviso
  `encoding-invalid`: o resultado de `utf8.DecodeRune` em laço. Nunca
  `strings.ToValidUTF8`, que junta uma sequência inválida num U+FFFD só e diverge
  da referência.
- **Charset legado** (decisão 18): `syslog.legacy_charset = cp1252 | latin1`,
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

Descarte por facility e severidade, **antes do ring**, logo depois do PRI. Quem
recebe syslog recebe o que o parque manda, e o parque é dominado por `debug` e
`info` que ninguém pediu.

```
aceita  <=>  severity <= filter.min_severity
         E   (filter.facilities vazio  OU  facility ∈ filter.facilities)
```

- Sobre o PRI **efetivo**: `missing` e `invalid` usam `syslog.default_priority`.
- `filter.min_severity` (padrão `7`, aceita tudo); nome ou número (`warning` =
  `4`). Severidade menor é mais grave.
- `filter.facilities` (padrão vazio): lista de nomes RFC 5427 ou números.
- **Sem sinônimos.** Só os nomes da tabela de §6.2 e números. `warn`, `error`,
  `security` e os `alert` (14) e `clock` (15) da standard são recusados no boot:
  `alert` também é severidade, e um filtro que significasse `console` em
  silêncio seria uma armadilha.
- Descarte: `relplog_messages_dropped_total{reason="filter"}`.
- **Recarregável por `SIGHUP`** (§16.3): é o botão das três da manhã.

Sem expressão regular no texto: custo imprevisível no caminho quente.

---

## 8. Rate-limit

Token bucket por prefixo de rede, com shards e evicção exata por sweep, como a
standard: `ratelimit.rate` tokens/s, teto `ratelimit.burst`, chave `/24` (IPv4)
e `/48` (IPv6), `ratelimit.max_buckets` como rede de segurança,
`ratelimit.exempt` para CIDRs isentos.

- **A chave é o `src-ip` resolvido**: o par do transporte, ou o cliente do
  PROXY de um par confiável. O `hostname` dentro da mensagem é alegação do
  emissor e **nunca** vira chave.
- Estourar o limite **segura** a transação até o balde do prefixo ter uma ficha
  (retentada a cada 50 ms, como um ring cheio, §5.7): o emissor é **desacelerado
  para o limite**, sem nada recusado nem descartado. No RELP, o emissor guarda o
  que o servidor não pode receber ainda, que é exatamente o que um limite deveria
  produzir. A recusa (`500`), que o S faz por descarte no TCP, aqui seria pior em
  tudo: perda na origem e, como o `500` quebra a sessão (§5.4), uma reconexão e
  duplicatas a cada mensagem acima do limite, isto é, uma tempestade de
  reconexões numa rajada. Com o limite muito abaixo da taxa do emissor por mais
  de `relp.backpressure_timeout`, a sessão fecha sem resposta e o rsyslog
  retransmite. Cada transação segurada conta em `relplog_ratelimit_held_total`;
  `messages_dropped_total` não tem mais o motivo `ratelimit`.
- Uma ficha recusada não é gasta, e a retentativa pergunta ao limitador de novo,
  mas **não** ao filtro, que já deixou a mensagem passar; uma ficha concedida é
  cobrada uma vez só (§5.7). Um `SIGHUP` que desliga o limitador solta as
  seguradas na retentativa seguinte.
- `ratelimit.enabled = false` por padrão; com ele desligado e algum listener fora
  do loopback, `WARN` no boot. Recarregável por `SIGHUP`.
- **O contador de baldes muda junto dos mapas**, sob o lock de cada shard: o
  `Allow` soma ao inserir, e o `Reset` (troca de máscara por `SIGHUP`) e o sweep
  descontam o que aquele shard perdeu. Um `Store(0)` no fim do `Reset` apagaria
  inserções concorrentes em shards já limpos, e um desconto do sweep feito
  depois do laço levaria o contador abaixo de zero: `relplog_ratelimit_buckets`
  mentiria e `max_buckets` passaria a valer mais que o configurado.

---

## 9. Ring

### 9.1 Interface

```go
type Ring interface {
    // Enqueue aceita o item. Não bloqueia por capacidade: cheio devolve
    // ErrFull na hora; indisponível (ring Redis fora do ar) devolve
    // ErrUnavailable. Bloqueia apenas pela garantia do modo (§2.3).
    Enqueue(ctx context.Context, it *item.Item) error
    // Dequeue entrega até n itens aos workers de dreno.
    Dequeue(ctx context.Context, n int) ([]*item.Item, error)
    // Commit informa que os itens foram confirmados no sink (ou descartados
    // de propósito, como uma malformada recusada).
    Commit(items []*item.Item) error
    // Touch renova a posse de itens retidos por um worker em retentativa
    // (só o ring Redis faz algo; §9.5).
    Touch(items []*item.Item) error
    // Drain devolve os itens pendentes em memória (dump de emergência).
    Drain() []*item.Item
    Stats() Stats
    Mode() string
    Close() error
}
```

`Enqueue` só devolve `nil` quando a garantia do modo foi cumprida. A
confirmação (o `200 OK` da transação RELP) acontece depois desse retorno, nunca
antes. O `Item` carrega o cabeçalho (§3.4) e o cru; **não carrega envelope**.

### 9.2 Saturação

Dois tetos, avaliados juntos:

- `ring.capacity_items` (padrão 65 536);
- `ring.max_bytes` (padrão 512 MiB), sobre o **cru**, com a medida de cada modo:
  - `memory`: a memória que o item **segura**, `cap(cru)` mais
    `ItemOverhead` = 384 B (a struct `item.Item` de 256 B, o slot do canal e um
    canal `done` eventual, arredondados). Contar `len(cru)` deixava uma linha de
    8 193 bytes, copiada para o buffer de 64 KiB da sua classe de pool (§15.1),
    custar oito vezes o que a contabilidade dizia. `Enqueue`, `Commit` e `Drain`
    usam a mesma medida.
  - `sqlite` e `redis`: `len(cru)`, o que de fato fica no disco ou no Redis, e o
    número que o recount do boot (`SUM(nbytes)`) e o contador `<stream>:bytes`
    reproduzem.

  O JLH é memória do worker, com teto próprio (§15.3), e somá-lo aqui seria
  contar memória que ninguém alocou.

Os dois tetos contam a **mesma população** em todos os modos: o aceito e ainda
não confirmado (`Commit`), **inclusive** os lotes que os workers retêm em
retentativa. No `sqlite` e no `redis` isso é natural (a linha e a entrada só
somem no `Commit`); no `memory`, o teto de itens é um contador próprio, não o
buffer do canal, que só limitaria o que ainda está na fila: com um lote retido,
um ring de capacidade 4 aceitava a quinta mensagem e o `/readyz` dizia `5/4`
(achado pela suíte de integração). "Um lote retido não está no ring" (§10.2) diz
respeito ao que o `Drain` do shutdown devolve, não à contabilidade.

Qualquer um estourando satura o ring: as transações são seguradas (§5.7).
`ring.high_water` (0.80) é avaliado nos dois e derruba o `/readyz`.

### 9.3 `ring.mode = memory`

Canal com buffer de `capacity_items`, com contador atômico de bytes. `Enqueue`
retorna assim que o item entra no canal. **Garantia:** RAM deste nó. Perde sob
`kill -9`, OOM ou pane; sob `SIGTERM` a perda é evitada pelo dreno do shutdown
(§13.4), e sob FATAL do sink, pelo dump (§12). É o modo de maior vazão e o
padrão.

### 9.4 `ring.mode = sqlite`

Ring persistente, para retenção máxima ao custo de latência; com RELP é a
combinação que dá ponta a ponta a promessa "confirmado é durável".
**Garantia:** `COMMIT` concluído antes da confirmação;
zero perda sob crash do processo, e sob queda de energia com
`ring.sqlite.synchronous = FULL`.

```sql
CREATE TABLE IF NOT EXISTS ring (
  seq    INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid   TEXT    NOT NULL,   -- o uuid do item, verificável sem decodificar o head
  head   BLOB    NOT NULL,   -- cabeçalho de §3.4
  raw    BLOB    NOT NULL,   -- o cru
  nbytes INTEGER NOT NULL    -- len(raw): o número da contabilidade deste modo (§9.2)
) STRICT;
CREATE TABLE IF NOT EXISTS quarantine (
  seq INTEGER PRIMARY KEY, uuid TEXT NOT NULL, head BLOB NOT NULL, raw BLOB NOT NULL,
  nbytes INTEGER NOT NULL, reason TEXT NOT NULL, ts INTEGER NOT NULL
) STRICT;
```

Arquivo `<data_dir>/ring.db` (ou `ring.sqlite.path`), DSN com `journal_mode(WAL)`,
`synchronous`, `busy_timeout`, `wal_autocheckpoint` e `_txlock=immediate`. Tudo o
que a standard aprendeu continua:

- **Duas goroutines tocam o banco.** A escritora agrupa `INSERT`s e faz `COMMIT` a
  cada `ring.sqlite.commit_batch` (256) itens ou `commit_max_delay` (5 ms); só
  então libera o grupo para confirmar; também executa os `DELETE` dos itens
  confirmados. A leitora avança por **cursor** sobre `seq`, nunca por conjunto
  em voo (que trava quando `workers × batch` excede a janela).
- **`uuid` em coluna própria:** perder a identidade na releitura já descartou,
  no irmão, 149 de 150 eventos por colisão de chave.
- **`nbytes` = `len(raw)`**, nunca `len(head) + len(raw)`: o recount do boot
  (`SUM(nbytes)`) precisa dar o mesmo número que a contabilidade do processo.
- **Reserva antes da entrega à escritora.** O teto é conferido e reservado
  (contadores atômicos de reserva, separados dos commitados) **antes** de o item
  ir para a escritora, e a reserva é desfeita se a fila da escritora estiver
  cheia, se o item não for apanhado no `Close` ou se o lote falhar. No `COMMIT`,
  primeiro sobem os commitados, depois cai a reserva: o teto pode contar a mais
  por um instante, nunca a menos. Sem isso, `Enqueue`s concorrentes dentro de uma
  janela de commit passavam todos pelo teto. `Stats` reporta commitados mais
  reservados; o recount e o backlog só os commitados.
- **Falha de armazenamento** (`BEGIN`, `INSERT` ou `COMMIT` recusados): a reserva
  do lote é desfeita, um `ROLLBACK` explícito garante a conexão limpa, cada
  `Enqueue` à espera recebe `ErrUnavailable` (a transação é segurada, §5.7)
  e o ring passa a **indisponível** (`/readyz` falha, `WARN` uma vez por
  transição) até o próximo `COMMIT` bem-sucedido. Nunca é `sink_down`: o sink
  pode estar perfeito.
- **Cancelamento durante o group commit:** uma vez entregue à escritora, o
  `Enqueue` espera o `COMMIT` mesmo com o contexto cancelado. Voltar antes
  descartaria uma mensagem que será commitada de qualquer forma e devolveria ao
  pool um buffer que a escritora ainda lê.
- **Linha indecodificável** (corrupção, codec de outra versão): movida para
  `quarantine` numa transação pela escritora e descontada da contabilidade no
  ato; `ERROR` no log e `relplog_ring_quarantined_total`. Sem teto e sem limpeza
  automática; o operador inspeciona e esvazia com o processo parado.
- **Leitura parcial:** um `Scan` ou um `rows.Err()` que falha depois de linhas já
  lidas entrega essas linhas, sem erro; o cursor não passou da linha que falhou,
  e ela é relida na chamada seguinte. Descartá-las as deixaria atrás do cursor,
  contadas e sem entrega até o próximo boot, e o dreno do shutdown esperaria por
  elas o `drain_timeout` inteiro.
- **`Close`:** antes de sair, a escritora executa os `DELETE` e as quarentenas já
  enfileirados, além dos `INSERT` pendentes. O `Commit` já descontou a
  contabilidade; uma linha confirmada que ficasse no arquivo seria reentregue
  pelo boot seguinte, em duplicata nos destinos que não deduplicam.
- **Boot:** o backlog é drenado **integralmente** antes de qualquer transporte
  abrir (§13.1), com guarda de não-progresso (5 s sem linhas com contabilidade
  acima de zero → recount; divergência persistente → exit 3).

### 9.5 `ring.mode = redis`

Decisão 11. Um **stream compartilhado** entre as instâncias, num Redis próprio
do ring (`[ring.redis]`), com consumer group. É o único ring que sobrevive à
perda do nó: o que uma instância aceitou outra pode entregar.

| Elemento | Valor |
|---|---|
| Stream | `ring.redis.stream` (padrão `relplog:ring`: o `go-syslog-server` usa `syslog:ring`, e os dois nunca dividem um ring, porque os cabeçalhos de um não são do outro) |
| Entrada | `XADD <stream> * h <cabeçalho> b <cru>` |
| Contador de bytes | `<stream>:bytes` (string com inteiro) |
| Grupo | `ring.redis.group` (padrão `relplog-ring`), criado no boot com `XGROUP CREATE <stream> <group> 0 MKSTREAM` (`BUSYGROUP` é sucesso) |
| Consumidor | o AgentUUID da instância |

**Capacidade global** por stream, com os mesmos `ring.capacity_items` e
`ring.max_bytes`. O `Enqueue` é um script Lua atômico (`EVALSHA`):

```lua
-- KEYS[1]=stream KEYS[2]=bytes ARGV: cap_items cap_bytes nbytes head raw
if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then return 0 end
local b = tonumber(redis.call('GET', KEYS[2]) or '0')
if b + tonumber(ARGV[3]) > tonumber(ARGV[2]) then return 0 end
redis.call('XADD', KEYS[1], '*', 'h', ARGV[4], 'b', ARGV[5])
redis.call('INCRBY', KEYS[2], ARGV[3])
return 1
```

`0` é `ErrFull`. O `Enqueue` retorna quando o script retorna: a durabilidade é a
do `appendfsync` do Redis do ring, não nossa.

**Dreno:** `XREADGROUP GROUP <group> <agent_uuid> COUNT n BLOCK 1000 STREAMS
<stream> >`. **Commit** (Lua, atômico): `XACK` e `XDEL` de cada id, e `DECRBY`
só da soma dos ids que o `XDEL` de fato removeu (outra instância pode ter
entregado e apagado uma entrada reivindicada; decrementar por ela de novo
quebraria o contador).

**Recuperação:**

- No boot, a instância relê os **próprios** pendentes (`XREADGROUP ... STREAMS
  <stream> 0`) antes de pedir entradas novas: o que a vida anterior leu e não
  confirmou.
- A cada `ring.redis.claim_idle / 2`, `XAUTOCLAIM <stream> <group> <agent_uuid>
  <claim_idle> 0-0 COUNT n` reivindica pendentes de **qualquer** consumidor
  ociosos há mais de `ring.redis.claim_idle` (padrão 30 s): os de uma réplica
  morta.
- Um worker que **retém** um lote em retentativa (sink fora do ar) renova a posse
  a cada `claim_idle / 3` com `XCLAIM ... 0 <ids> JUSTID` (`Ring.Touch`), para
  que outra réplica não reivindique um lote vivo. Sem isso, uma queda do sink
  maior que `claim_idle` faria todas as réplicas entregarem o mesmo lote.
- Mesmo assim a entrega é **at-least-once**: uma réplica que congela além de
  `claim_idle` volta e entrega o que outra já entregou. O consumidor deduplica
  por `(uuid, step)` (§10.2).
- A renovação nunca acontece no meio de uma tentativa, que dura até
  `sink.op_timeout`; por isso a validação exige `claim_idle > 2 × op_timeout`
  (§13.2). Entradas tiradas da fila de reivindicadas são renovadas
  (`XCLAIM ... 0 <ids> JUSTID`, fora do lock) no instante em que são entregues ao
  worker; as que o Redis já não tem como pendentes são liberadas em vez de
  entregues, e uma falha do próprio `XCLAIM` não impede a entrega.

**Ordem:** FIFO por instância, não global.

**Detalhes de implementação:**

- **Redis Cluster** exige um stream com *hash tag* (`{relplog:ring}`), porque os
  scripts tocam o stream e o contador numa operação só; sem ela, a construção
  recusa com erro fatal (exit 3).
- O `Commit` decrementa a soma dos `len(cru)` que o chamador passa (o mesmo
  número que o `Enqueue` somou), sem reler a entrada dentro do Lua; por isso o
  `Commit` vem **antes** do `Release`, como §10.1 já ordena.
- Numa queda do Redis do ring, o `Dequeue` não devolve erro: espera um backoff
  (100 ms → 2 s) e devolve um lote vazio, para que os workers não parem.
- Os pendentes reivindicados ficam numa fila limitada a `sink.batch` e são
  renovados a cada rodada; um `NOGROUP` recria o grupo.
- Consumidores mortos não são removidos do grupo (sem `XGROUP DELCONSUMER`).

**Contador de bytes:** só é tocado dentro dos dois scripts, então não deriva por
crash. No boot, `XLEN = 0` com contador diferente de zero é zerado com `WARN`. Uma
reconciliação completa (`XRANGE` somando `len(b)`) é procedimento manual,
documentado em `docker/README.md`, com o tráfego parado.

**Política de despejo:** a mesma regra do sink Redis (§10.3): `allkeys-*` é FATAL
no boot (o ring inteiro e o contador dele sumiriam num Redis cheio, com o serviço
UP), checado antes de qualquer chave ser criada; ilegível, `WARN`.

**Indisponibilidade do Redis do ring:** `Enqueue` devolve `ErrUnavailable`: a
transação RELP é segurada como num ring cheio (§5.7), o `/readyz` falha. O
dreno retenta com backoff. Não há dump de emergência: o conteúdo já está fora do
processo.

A disponibilidade segue a **escrita**, não a leitura. A atualização de estatísticas
(1 s) é um pipeline de `XLEN`, `GET` do contador e uma sonda
`XADD <stream>:probe NOMKSTREAM * p 1` (resposta nula é sucesso: a chave nunca é
criada). As estatísticas são publicadas sempre que as duas leituras respondem; o
ring fica indisponível quando qualquer um dos três comandos falha e volta só com
uma sonda ou um `Enqueue` bem-sucedidos. Falhas de `Dequeue`, `Commit`, `XCLAIM`
e `XAUTOCLAIM` não mudam o estado. Sem isso, um Redis que responde a leituras e
recusa escritas (OOM, ACL sem `@write`) fazia o `/readyz` oscilar. A mesma sonda
roda na construção: OOM é fatal (exit 3), `READONLY` é retentado.

**`from` e `space`:** vêm do cabeçalho, de quem recebeu (§3.1); o consumidor no
stream é quem drenou. Por isso todas as réplicas precisam da mesma config de
conversão (§3.5).

`/readyz` e as métricas usam a profundidade **global** (`XLEN` e o contador), lida
em cache e atualizada pelo dreno a cada segundo.

### 9.6 `ring.mode = none`

Sem buffer. A goroutine da recepção converte, monta o JLH e escreve no sink **de
forma síncrona**, confirmando só depois. É o único modo em que a conversão roda
na recepção, e é aceitável porque este modo já é o lento.

- **Backpressure:** semáforo de `ring.max_inflight` (2048) escritas
  simultâneas; cheio vale a regra de ring cheio de §2.3. Sem ele, um sink lento
  faria o número de goroutines crescer sem teto, cada uma segurando o cru.
- A escrita é limitada por `sink.op_timeout`, num contexto **desligado do
  cancelamento** da recepção (`context.WithoutCancel`): o cliente do destino pode
  já ter enfileirado o comando, e abortar só deixaria a recepção devolver ao pool
  um buffer que o pipeline ainda vai serializar. O estouro conta como ring cheio
  (retentável).
- Com `sink=postgres`/`mysql` não há coalescência: a vazão é
  `pool_size / latência de commit`, e um `max_inflight` acima do pool só cria fila
  escondida (`WARN` no boot).
- Com `sink=none` o semáforo é dispensado.
- `Stats()` reporta `items` = em voo e `capItems` = `max_inflight`.

### 9.7 Combinações que degradam ou recusam

| Combinação | Efeito |
|---|---|
| `ring=sqlite` + `sink=sqlite` com o **mesmo arquivo** | exit 1 |
| `ring=sqlite` + `sink=sqlite` com arquivos distintos | permitido, `WARN` (dois `fsync`) |
| `ring=redis` + `sink=redis` no **mesmo servidor** (mesmos endereços e `db`) | `WARN`, e o ring efetivo vira `memory`: o ring não protegeria nada que o destino já não perca junto |
| `ring=redis` + `sink=redis` em servidores distintos | permitido |
| `ring=none` + `sink=none` | permitido (benchmark de transporte) |

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

Um `Batch` carrega, por envelope: o JLH (fatia de um buffer do pedaço), as
colunas promovidas de §3.7, o subject (§3.8) e o item de origem.

Os workers de dreno (`sink.workers`, padrão 8) são **idênticos** para todos os
sinks:

1. `Dequeue` até `sink.batch` (256) itens, esperando no máximo
   `sink.batch_max_delay` (5 ms) para completar. O worker só termina quando o
   contexto acaba ou o ring diz `ErrClosed`/`ErrNoBacklog`; qualquer outro erro
   (um `SQLITE_BUSY` depois do `busy_timeout`, um `IOERR`) é logado com
   amostragem e retentado com backoff (`retry_backoff_min` → `max`). Sair nele
   reduzia o dreno a zero em silêncio, com `/healthz` 200 e o ring enchendo.
2. **Converte** cada item (§6) e serializa o JLH num buffer de pedaço. O pedaço
   fecha quando a soma dos JLH passa de `sink.batch_max_bytes` (16 MiB); o item
   que passou abre o pedaço seguinte, e um JLH maior que o teto forma um pedaço
   sozinho. Malformada recusada (§6.6) não entra em pedaço nenhum: é contada e
   confirmada junto.
3. `sink.Write(ctx, pedaço)`, com retentativa (§10.2).
4. Sucesso: `ring.Commit(itens do pedaço)`, devolve o cru e o buffer do pedaço
   aos pools, `relplog_events_written_total += n`.
5. Próximo pedaço.

O passo 2 roda **sempre**, inclusive com `sink=none`: sem ele o benchmark mediria
um pipeline que não existe. O teto de bytes existe porque o JLH chega a 12× o cru:
`workers × batch × pior JLH` passaria de 1 GiB.

### 10.2 Falha de pedaço, e o outbox

O worker **segura o pedaço** e retenta com backoff (`sink.retry_backoff_min`
100 ms → `retry_backoff_max` 2 s), **sem puxar mais nada do ring**. O ring para de
escoar e enche, até o backpressure de §2.3 aparecer.

- **O retry reenvia os mesmos bytes**, com o mesmo `uuid`: o JLH já serializado é
  reaproveitado, nunca refeito. É a regra do outbox do JLH, e poupa CPU no
  momento em que o sistema está em dificuldade.
- Com `ring=redis`, o worker renova a posse (`Touch`) enquanto retém (§9.5).
- **Um lote retido não está no ring.** O shutdown o coleta de
  `Workers.Pending()`, ou ele some depois de confirmado. E o `hold` faz `append`,
  nunca atribuição: um worker que abandona o lote no shutdown o mantém registrado,
  e um `Dequeue` posterior não pode sobrescrevê-lo (bug real do irmão).
- **At-least-once.** Uma escrita que estourou o prazo pode ter sido aceita, e o
  retry a reenvia. A deduplicação é do destino: `ON CONFLICT (uuid, step)` no
  SQLite e no MySQL, `ON CONFLICT (uuid)` na `loghub.inbox`, `Nats-Msg-Id` na
  janela do JetStream, `insert_deduplication_token` e o `ReplacingMergeTree`
  no ClickHouse (§10.10.4). O `XADD *` do Redis e o NATS `core`
  não deduplicam: o consumidor deduplica por `(uuid, step)`, como o JLH manda.
- **Contaminação** (§15.2): uma tentativa que termina por prazo ou cancelamento
  marca o pedaço, e nem o cru nem o buffer do pedaço voltam ao pool, nem quando a
  retentativa seguinte tem sucesso.

| Classe | Efeito |
|---|---|
| `ClassNetwork` | retenta; não muda o estado de saúde (§13.3) |
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`relplog_events_dropped_total{reason="sink_logic"}`), confirmado no ring |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`relplog_events_written_total`, só os demais em `sink_logic`, e o log traz só os
`uuid` perdidos mais a contagem gravada. O pedaço é confirmado inteiro, como
antes. É exato onde o destino sabe: **MySQL** (os comandos do pedaço dividido
são autocommit e em ordem: os anteriores ao recusado estão gravados; os
posteriores não são tentados e contam como perdidos) e **NATS JetStream** (numa
recusa `10054` o sink espera os demais futures e informa os confirmados; erro
`network` ou `fatal` de um future continua retornando na hora, contaminado).
**PostgreSQL** e **ClickHouse** (um comando por pedaço) e **SQLite** (uma
transação revertida) não gravam nada na falha, e o **Redis** não produz `logic`.
| `ClassFatal` | FATAL imediato |

### 10.3 `sink.type = redis`

Cliente `github.com/redis/rueidis` com auto-pipelining. Um `XADD` por envelope
(§3.6), enviados como um pipeline por pedaço. O JLH vai com
`rueidis.BinaryString`, sem cópia.

- **Comandos do rueidis são reciclados após executar:** um retry **reconstrói**
  os comandos; reusar o slice corrompe dados sem erro (§20.11).
- **Health check de escrita:** um pipeline com `XLEN <stream>` e
  `XADD <stream>:healthprobe NOMKSTREAM * p 1`. `PING` e `XLEN` são leitura, e uma
  réplica promovida por engano responderia feliz recusando todo `XADD` com
  `READONLY`. O `XADD NOMKSTREAM` não cria a chave, mas passa pelas verificações
  de escrita (`READONLY`, `OOM`, `NOPERM`). O `XLEN` pega um `WRONGTYPE` no boot.
- `sink.redis.maxlen` (`0` = desligado) aplica `MAXLEN ~ N`. Desligado por
  padrão: trimming descarta eventos em silêncio.
- Knobs do rueidis expostos: `ring_scale_each_conn`, `pipeline_multiplex`,
  `read_buffer_each_conn`, `write_buffer_each_conn`.
- **Preguiçoso**, como os demais destinos: a discagem acontece no primeiro uso,
  e o `WaitReady` do boot retenta uma porta recusada; um `WRONGPASS` na discagem
  continua fatal na hora.

| Resposta | Classe |
|---|---|
| `OOM`, `WRONGTYPE`, `NOPERM`, `NOAUTH`, `WRONGPASS` | **`fatal`** (não muda sozinho; como `logic` descartaria um lote a cada poucos ms, para sempre) |
| `LOADING`, `BUSY`, `MASTERDOWN`, `READONLY`, `CLUSTERDOWN`, `TRYAGAIN` | `network` |
| timeout, dial, pipe rompido, qualquer outra | `network` |

Uma resposta fatal à sonda **ou à discagem** (o rueidis autentica dentro do
`NewClient`) é FATAL na primeira checagem, sem passar pelos fusíveis: senha
errada encerra o boot na hora. A mesma tabela classifica o cliente do ring Redis.

**Política de despejo** (pendência 5 do S, também para o ring Redis, §9.5): o Redis
do sink e o do ring precisam de `maxmemory-policy` `noeviction` (ou `volatile-*`:
estas chaves nunca têm TTL). Sob `allkeys-*` um Redis cheio não responde `OOM`:
despeja chaves, e o stream, que é uma chave só, leva todas as entradas com o
serviço UP. O boot lê a política (`INFO memory`, senão `CONFIG GET`) e recusa
`allkeys-*` (qualquer política com esse prefixo) como FATAL, exit 3, **mesmo com
`maxmemory 0`**: a política diz o que acontece quando um limite existir, e um
`CONFIG SET maxmemory` com o Redis no ar a ativaria sem este servidor ver (a
revisão independente de 2026-10-11 levantou o caso, e a regra ficou). Se a ACL
ou um proxy não deixar ler, `WARN` com a premissa e o boot segue. Só no boot: a
política não muda sem um operador.

### 10.4 `sink.type = sqlite`

Destino final local, `<data_dir>/events.db` (ou `sink.sqlite.path`), esquema de
§3.7 criado pelo próprio servidor.

- `sink.workers` forçado a 1, com `WARN` se pedido mais: o SQLite serializa
  escritores.
- `INSERT ... ON CONFLICT (uuid, step) DO NOTHING`; um pedaço é uma transação.
- `SQLITE_FULL`, `SQLITE_CORRUPT` → fatal; `SQLITE_BUSY` → network; constraint →
  logic.
- `sink.sqlite.rotate` (`none` | `hourly` | `daily`, em UTC) e
  `sink.sqlite.keep_files` (`0` nunca apaga; recarregável).
- `Health()` é um **heartbeat de escrita** (`health(id = 1, ts)`, `INSERT ... ON
  CONFLICT DO UPDATE`) sob a trava da escritora: custo constante, prova o que
  importa. `PRAGMA quick_check` só no boot; periódico, ele leria o arquivo inteiro
  a cada sonda (bug real).

### 10.5 `sink.type = postgres`

Fila **compartilhada entre instâncias** e entre produtores LogHub: a
`loghub.inbox` de §3.7.1. `github.com/jackc/pgx/v5` pela API nativa (a
classificação precisa do `pgconn.PgError`).

- **Nunca faz DDL**, nem `CREATE TABLE IF NOT EXISTS`: N instâncias subindo
  juntas correm no catálogo, e o papel de produção não tem o privilégio.
  `docker/postgres/inbox.sql` é a cópia literal da DDL da skill (§3.7.1).
  `sink.postgres.table` tem padrão `loghub.inbox`; outro nome só serve para
  testes ou para uma segunda fila com a **mesma** DDL.
- **Escrita:** um `INSERT` por pedaço com `unnest` sobre dez arrays paralelos
  (`uuid`, `from_uuid`, `space`, `type`, `level`, `sid`, `step`, `event_at`,
  `event_nsec`, `payload`) e `ON CONFLICT (uuid) DO NOTHING`; um uuid repetido
  dentro do mesmo pedaço também é ignorado. É o "INSERT com várias linhas" que a
  skill pede para volume alto; `COPY` direto na fila é proibido por ela (um uuid
  repetido derruba o `COPY` inteiro). O `payload` é um sub-slice do JLH do
  pedaço, sem cópia, exceto quando há um escape `\u0000` (§3.7.1): aí vai uma
  cópia reconstruída, e o JLH do pedaço não é tocado. Os arrays vêm de um pool
  e voltam **zerados**: um slice devolvido cheio prende referências a buffers que
  já voltaram ao pool, e o pedaço seguinte carregaria o envelope de outro
  emissor. As duplicatas (`len − linhas afetadas`) contam em
  `relplog_sink_postgres_duplicates_total`.
- `sink.workers` no padrão é rebaixado para 4 com `WARN`; `WARN` acima de 16;
  `pool_size = 0` deriva `workers + 1`.
- **Validação no boot**, na primeira checagem válida, cada recusa FATAL imediato
  (exit 3, sem esperar `boot_timeout`): a tabela existe; as dez colunas com tipos
  compatíveis (`uuid` em `uuid` e `from_uuid`; `smallint`/`integer`/`bigint` em
  `level`, `sid`, `step` e `event_nsec`; `text` ou `character varying` em `space`
  e `type`; `timestamp with time zone` em `event_at`; `jsonb` em `payload`); um
  índice **único** exatamente sobre `(uuid)`, sem predicado nem expressão; o
  papel tem `INSERT` e `SELECT (uuid)` (`GRANT INSERT, SELECT (uuid)`: o
  `ON CONFLICT (uuid)` lê a coluna árbitra, e só com `INSERT` o primeiro pedaço
  falharia com `42501`); o servidor não está em recuperação
  (`pg_is_in_recovery()`). Em recuperação é FATAL no boot e DOWN depois dele (uma
  réplica que um failover pode promover).
  **Revisão independente de 2026-10-06** — também FATAL no boot, porque cada um
  passava na validação e depois recusava todo pedaço com o sink UP (§20.18):
  `sid` que não seja `bigint` (o `sid` de rede é um hash de 53 bits: `22003`);
  `character varying(n)` em `space` ou `type` com `n` < 64 (`22001`); o índice
  único `DEFERRABLE` (o `ON CONFLICT` não o aceita como árbitro: `55000`,
  retentado para sempre) ou inválido (um `CREATE UNIQUE INDEX CONCURRENTLY` que
  falhou); a tabela **particionada** (uma linha sem partição é `23514`); e um
  banco com `server_encoding` diferente de `UTF8` (um `LATIN1` guardava o texto
  em dupla codificação, sem erro). A sessão fixa `client_encoding = UTF8`, e os
  parâmetros de execução são refeitos do zero, e **o pool é parseado com todo
  `PG*` escondido** (sob mutex, só no boot) e `sslmode=disable`, com o TLS vindo
  só de `[sink.postgres]` (pendência 9 do S, como no SP): nenhuma variável libpq
  (`PGSSLMODE`, `PGPORT`, `PGSERVICE`, `PGCHANNELBINDING`, `PGREQUIREAUTH`,
  `PGSSLNEGOTIATION`, `PG{MIN,MAX}PROTOCOLVERSION`, `PGOPTIONS`,
  `PGTARGETSESSIONATTRS`, `PGAPPNAME`…) chega ao pool nem derruba o parse. Em tempo
  de execução, um `23514` que não nomeia constraint (a partição que sumiu) é
  `fatal`, não `logic`; um `from` que não é UUIDv7 (só dado de outro ring ou
  dump) é recusado antes do `INSERT`, como `logic`.
  **E da revisão dos webhooks (2026-10-06)**, também FATAL no boot: um
  `timestamptz(p)` em `event_at` com p < 6 (arredonda o microssegundo em
  silêncio; `timestamptz(6)` é aceito — o tipo passa a ser comparado por
  `format_type(tipo, NULL)`, porque cortar o texto no `(` fazia dele um
  `timestamp` recusado com a mensagem errada); e **qualquer outro índice
  único** que não o árbitro, ou uma constraint de exclusão: o `ON CONFLICT
  (uuid)` absorve colisão só no árbitro, e um evento novo que colidisse em
  `UNIQUE (from_uuid, sid)` derrubava o pedaço inteiro com `23505` (`23P01` na
  exclusão), um descarte `logic` a cada colisão com o sink UP. Um índice ainda
  não pronto (`indisready`) é ignorado; inválido mas pronto conta, porque ainda
  impõe unicidade.
  **Coluna extra obrigatória:** é FATAL se uma coluna fora das dez for `NOT
  NULL` sem default, não identidade e não gerada. Sem isso, a validação passava e
  toda escrita virava `23502`, `logic`: lotes descartados para sempre com o sink
  UP (§20.18). `received_at` passa porque tem `DEFAULT now()`. Pelo catálogo, não
  por sonda como no MySQL: uma sonda dispararia triggers do operador e
  consumiria sequências, e a recusa dela não nomeia a coluna.
- Durabilidade: a do servidor (`synchronous_commit`). A skill admite a fila
  `UNLOGGED`, que o servidor não confere: ela é apagada numa queda do PostgreSQL
  e só serve se a origem puder reenviar, o que o syslog não faz.

| SQLSTATE | Classe |
|---|---|
| `08`, `53`, `57`, `40` | `network` |
| `22`, `23` (fora do `ON CONFLICT`) | `logic` |
| **`42`** (esquema), `28`, `2F`, **`3D`**, `42501`, `25006`, **`23502`**, `23514` sem constraint nomeada | **`fatal`** |
| qualquer outra | `network` |

`23502` (`not_null_violation`) é `fatal` (pendência 7 do S): o servidor nunca
manda `NULL` nas dez colunas, então ele vem da tabela (coluna obrigatória
acrescentada, `DROP DEFAULT`, trigger) e, como `logic`, descartaria todo pedaço
com o sink UP. A classe `3D` (`sink.postgres.database` inexistente, `3D000` no
login) é `fatal`, como o `1049` do MySQL (pendência 8): retentar até o
`boot_timeout` só adiava o exit 3 com a mensagem de rede.

**A validação do boot tem prazo próprio** (pendência 6 do S), aqui, no MySQL e no
ClickHouse: roda com `sink.op_timeout`, não no `sink.health_timeout` (1 s) da
checagem que a dispara — são uma dúzia de idas e voltas, e a ~55 ms nenhuma
tentativa cabia em 1 s —, e um cancelamento do boot (`SIGTERM`) ainda a
interrompe: as checagens de saúde carregam o contexto de quem as chamou como
"tempo de vida" (`withLifetime`), que a validação vigia.

**A classe `42` é fatal e nunca `logic`**: uma tabela dropada classificada como
`logic` descartaria um lote a cada 5 ms, para sempre, num processo que parece
saudável. Esta tabela e `TestClassifySQLState` mudam juntas.

### 10.6 `sink.type = mysql`

MySQL 8.0+ ou MariaDB 10.6+, compartilhado. `github.com/go-sql-driver/mysql` por
`database/sql` (é para onde ele foi desenhado; o `*mysql.MySQLError` atravessa
com o `Number`). Tudo de §10.5 que não é repetido aqui vale igual.

- Sem `interpolateParams`: protocolo binário, e `env` viaja cru.
- `sql_mode` **da sessão** fixado em `STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION`:
  fora do modo estrito, um valor longo é truncado com aviso em vez de recusado.
- **`autocommit=1` da sessão** (pendência 10 do S): com `autocommit=0` global
  (`my.cnf`, `init_connect`), o `INSERT` ficava numa transação que ninguém
  commitava, o `Write` devolvia sucesso e as linhas sumiam com a conexão (a
  entrega parcial de §10.2 também depende disso). A validação confere
  `@@session.autocommit = 1` (FATAL se não).
- **Escrita:** `INSERT INTO ... VALUES (...), (...) ON DUPLICATE KEY UPDATE uuid
  = uuid`. **Nunca `INSERT IGNORE`**, que rebaixa `1406` e `1366` a aviso e trunca
  em silêncio. Duplicatas = `len − afetadas`.
- O pedaço é **dividido** de novo antes de ir ao servidor: no máximo 65 535
  placeholders por comando (dezoito por linha → **3 640 linhas**; `sink.batch`
  acima disso é exit 1), e no máximo 7/8 do `max_allowed_packet` lido no boot
  (custo estimado por linha: `env` + 3 × colunas de texto + 512 bytes).
- **Validação no boot** (cada recusa é exit 3): tabela existe e é InnoDB; colunas
  com as famílias de tipo de §3.7, e as que o servidor preenche com largura
  para o maior valor dele (`space` 64, `format` 9, `transport` 3 — só `tcp` e
  `tls` chegam aqui —, `src_ip` 39, o IPv6 canônico mais longo);
  o índice único exatamente sobre `(uuid, step)`, colunas inteiras (sem
  `SUB_PART`), e **o único** (pendência 11 do S): o `ON DUPLICATE KEY` dispara em
  qualquer chave única, e um evento novo que colidisse em outro índice (ou num
  prefixo de `uuid`) era confirmado como duplicata e perdido; tolera-se só uma
  coluna `AUTO_INCREMENT` sozinha (chave substituta);
  `max_allowed_packet` ≥ a maior linha (§3.9); e uma **sonda** de **duas linhas**,
  uma com todos os `NULL` possíveis e outra com cada coluna no extremo, ambas com
  `transport = 'tcp'` e `src_ip` presente, porque nenhuma linha deste servidor
  tem outro transporte nem `src_ip` `NULL` (sondar com `unix` e `src_ip` `NULL`,
  como no S, recusava uma tabela legítima com `src_ip NOT NULL`, `CHAR(3)` ou um
  `CHECK (transport IN ('tcp','tls'))`; revisão independente, 2026-10-11) (`sid`
  2^53−1, `ev_uts` 253402300799, `nbytes` = o maior JLH, os textos do servidor
  na largura máxima): o mesmo
  `INSERT ... ON DUPLICATE KEY UPDATE` do dreno, com `uuid` de dezesseis zeros,
  numa transação **revertida** (privilégio dado por papel não aparece em
  `information_schema`; e o `ON DUPLICATE KEY` exige `UPDATE` além de `INSERT`,
  que pode ficar restrito à coluna `uuid`). **Qualquer** recusa do servidor à
  sonda (`*mysql.MySQLError`, inclusive `1048`, `1366`, `1406` e números que a
  tabela de execução não conhece, como `3819` de um `CHECK`) é FATAL: a tabela de
  classes abaixo é de execução, e aplicada à sonda fazia o boot esperar 300 s.
  Um erro sem resposta do servidor (conexão caída, prazo) segue a tabela, porque
  não provou nada.
- `@@read_only = 1` na **primeira** validação é FATAL (a réplica de §10.5);
  depois dela é DOWN.
- `Health()`: `SELECT @@read_only` (`1` é DOWN) e os gauges do pool.

| `Number` | Classe |
|---|---|
| sem número (EOF, `ErrBadConn`, prazo), `1040` `1053` `1077` `1927` `1158`–`1161` `1205` `1213` `1317` `3024` `2006` `2013`, e os limites do servidor que passam sozinhos e vêm com SQLSTATE `42000` — `1203` (`max_user_connections`), `1226` (limite por usuário, inclusive os `*_PER_HOUR`), `1461` (`max_prepared_stmt_count`) | `network` (os três últimos listados porque o recurso ao SQLSTATE os faria `fatal`: um deploy que sobrepõe o orçamento de conexões viraria dump e exit 3 na frota inteira; revisão independente de 2026-10-11) |
| `1290` `1836` (somente leitura), `1021` `1030` `1041` `1114`, `1044` `1045` `1049` `1130` `1142` `1143`, `1054` `1146` `1364` | **`fatal`** |
| `1048` `1062` `1153` `1264` `1366` `1406` `1690` `3819`, `ErrPktTooLarge` | `logic` |
| qualquer outro | pelo **SQLSTATE** do erro: `22`/`23` `logic`, `42`/`28` `fatal`, `08` e `HY000` `network` |

O `1062` não vem mais do `INSERT` (outro índice único é recusado no boot) e fica
`logic` só para o que outro comando gerar (um trigger). `3819` (um `CHECK`, com
SQLSTATE `HY000`) é `logic` por número. `sink.mysql.database` e
`sink.clickhouse.database` entram entre crases no `INSERT`: são recusados
(exit 1) com crase, barra invertida (o escape do ClickHouse dentro de crases),
byte de controle ou mais de 64 bytes, na config e no construtor (pendência 12).

### 10.7 `sink.type = nats` (padrão)

Decisões 9 e 16. `github.com/nats-io/nats.go` com o pacote `jetstream` (a API
atual). Uma conexão por processo, multiplexada pelos workers e pelo beacon (§11),
aberta preguiçosamente na primeira checagem; o `WaitReady` do boot retenta.

**Conexão:** `MaxReconnects(-1)` (os fusíveis de §13.3 decidem quando desistir) e
**`ReconnectBufSize(-1)`**: durante uma desconexão, publicar falha na hora
(`network`) em vez de guardar mensagens dentro do cliente. O ring é o único
buffer; uma fila escondida no cliente escaparia da contabilidade, do
backpressure e do dump (§20.18). `CLOSED` é fatal.

**`sink.nats.mode = jetstream`** (padrão): um `PublishMsgAsync` por envelope, e só
depois de os N futuros estarem no ar o worker espera as N confirmações, sob o
`sink.op_timeout` do pedaço. Três opções que não são as padrões, e são as que
importam:

- `WithExpectStream(stream)`: o servidor recusa a mensagem se o subject pertencer
  a outro stream; erro de configuração vira recusa fatal.
- **`WithRetryAttempts(0)`**: a retentativa interna da biblioteca relê
  `msg.Data` depois de o `Write` ter retornado, a corrida de posse de buffer de
  §15.2. Quem retenta é o worker, ainda dono do buffer.
- `WithPublishAsyncTimeout(sink.op_timeout)`: sem ele, um futuro sem resposta
  segura a mensagem e o buffer para sempre.

`WithPublishAsyncMaxPending` = `sink.workers × sink.batch + ring.max_inflight`.
`Data` aponta para o buffer do pedaço, que só volta ao pool depois de todos os
futuros resolverem; em qualquer caminho de erro fica para o GC.

`PubAck.Duplicate` conta em `relplog_sink_nats_duplicates_total`. Fora da
`duplicate_window` do stream, uma redelivery duplica (at-least-once).

**`sink.nats.mode = core`** (opt-in, *best effort*): `PublishMsg` por envelope e um
`FlushTimeout(sink.op_timeout)` por pedaço. O sucesso significa que o servidor
NATS **recebeu** as mensagens, não que alguém as guardou: sem stream, uma
mensagem sem assinante some. Com RELP isso desfaz a promessa do protocolo (o
`200 OK` sai e ninguém guardou): o boot registra `WARN` com essa frase.

**Subjects** (§3.8): `sink.nats.subjects = exact` (padrão) ou
`facility-severity`. O subject é derivado, não configurável: a regra
`loghub.{space}.{type}` é do JLH.

**Validação no boot** (`jetstream`; cada recusa é exit 3):

| Verificação | Como |
|---|---|
| O stream existe | `js.Stream(sink.nats.stream)` (padrão `LOGHUB_SYSLOG`) |
| O stream **cobre** os subjects | casamento local de padrões: algum subject do stream cobre `loghub.<space>.syslog` (`exact`) ou `loghub.<space>.syslog.*.*` (`facility-severity`) |
| Nenhum subject do stream captura o plano de controle | um padrão do stream que case `<ctl_namespace>.beacon` (como `loghub.>`) é recusado: reproduziria beacons antigos para todo consumidor |
| Não está selado | `Sealed = false` |
| Confirma publicações | `NoAck = false`: um stream com `no_ack` deixaria toda escrita esperar o prazo como `network`, para sempre, com o sink UP |
| O maior evento cabe | `nc.MaxPayload()` e `max_msg_size` do stream contra §3.9 + headers; com `storage: file`, também contra os 32 MiB por registro do file store (`10077`); com `discard=new`, também contra `max_bytes` do stream: um `max_bytes` abaixo de um registro recusaria o maior evento para sempre, e o `10077` dele, que agora é `network`, seguraria o ring sem fim (exit 1 no boot) |
| Retenção guarda o que confirma | `retention: interest` é FATAL: sem consumidor interessado o servidor responde com `PubAck` normal e não guarda nada, e o pedaço seria confirmado e perdido (decisão 16; com RELP, um `200 OK` mentiroso). `limits` e `workqueue` são aceitos |
| Teto por subject | `max_msgs_per_subject > 0` só gera `WARN`, uma vez por valor: todo evento de um `space` divide um subject, e os mais antigos são descartados conforme os novos são confirmados |

Com `core`, só o `max_payload`. O stream é criado por quem administra o NATS, a
partir de `docker/nats/stream.json`; o servidor nunca cria nem altera um stream.

**O contrato vale a vida inteira** (pendência 16 do S): ele é conferido em
**toda** checagem de saúde, sobre o `CachedInfo` já buscado, sem ida e volta a
mais; em `core`, o `max_payload` também (uma reconexão a outro nó pode trazer um
menor). Um stream editado com o servidor no ar (subjects estreitados, `no_ack`,
`max_msg_size` menor, `loghub.>`) vira FATAL em vez de deixar toda escrita
`network` para sempre com o sink UP. Só o subject do **próprio** Workspace é
exigido: o de outro `space`, que um dump reingerido trouxe, nunca entra no
contrato, e estreitar o stream depois do dreno não é FATAL.

**JetStream desligado no servidor:** "sem respondedores" (`ErrNoResponders`) na
**primeira** checagem é FATAL nomeando o JetStream, sem perguntar `AccountInfo`:
a sonda era inútil, e uma negação de `$JS.API.INFO` a um papel de privilégio
mínimo virava um veredito permanente. Depois da primeira checagem bem-sucedida,
é `network`.

**Permissão negada é FATAL** (pendência 14 do S, como no SN/N). O NATS recusa um
publish sem permissão só com um `-ERR` assíncrono, e a negação vira um
**veredito permanente** quando atinge o caminho de dados: publish num subject de
dados (`loghub.<space>.syslog` ou `…syslog.<facility>.<severity>`, de qualquer
`space`), qualquer coisa em `$JS.API`, ou a assinatura em `_INBOX` por onde voltam
as confirmações. **Nunca** a negação no subject do beacon. O `ErrorHandler` grava
o veredito; o `Health` e todo `Write` o devolvem como `fatal`; o `writeCore` lê o
`LastError` depois do flush (o `-ERR` chega antes do `PONG`); em `jetstream`, o
prazo do futuro de um publish negado vira o veredito, com o pedaço contaminado
(o primeiro pedaço negado é fatal só depois do `sink.op_timeout`; em `core`, na
hora). Antes, em `jetstream` o worker retentava para sempre com o sink UP, e em
`core` o pedaço era confirmado e perdido.

**Uma mensagem recusada pelo cliente antes de sair** (`ErrMaxPayload`: uma
reconexão a um nó com `max_payload` menor, ou uma linha de dump de outra vida)
é recusada **sozinha**, como um `10054`, nos dois modos: as demais do pedaço são
publicadas e `Error.Delivered` diz quais saíram (§10.2). Encerrar o pedaço nela
confirmava sem publicar as seguintes e contava como perdidas as anteriores.

**Dump de outro `space` em `core`:** o envelope é publicado e gera um `WARN` por
`space` estrangeiro (nunca um por mensagem, nem um por subject granular): sem
stream, ele se perde se ninguém assinar o subject, e o dump é removido como
entregue. A comparação é entre o `space` do item e o Workspace, uma comparação de
string, sem alocar no caminho quente.

**Negação mascarada (`mode=core`).** O `nats.go` guarda só o **último** `-ERR`.
Uma negação de permissão do subject do beacon que chega entre os `PUB` de um
pedaço e o `PING` do flush esconde uma negação do caminho de dados que veio
antes dela, e o pedaço seria confirmado embora o servidor o tenha recusado. Uma
negação **nova** durante o pedaço que não é do caminho de dados torna o pedaço
incerto: `network`, não confirmado; a retentativa encontra o veredito que o
`ErrorHandler` já gravou, ou passa (revisão independente, 2026-10-11).

**Logs de conexão** (`nats connected`, `nats reconnected`) registram só esquema e
host do servidor alcançado: o `ConnectedUrlRedacted` do `nats.go` esconde a
senha, não um token (`nats://<token>@host`, que a `LOGHUB_EB_URL` aceita). Uma URL
que não parseia nunca é ecoada; no lugar dela vai o `host:porta`.

**`Health()`:** o estado da conexão (`CONNECTED` é saúde, `CLOSED` é fatal,
`RECONNECTING`/`DISCONNECTED` é falha de rede) e, em `jetstream`, um
`StreamInfo` (um `PING` responderia feliz com o JetStream desligado); em `core`,
um `FlushTimeout`.

| Erro | Classe |
|---|---|
| prazo, `ErrTimeout`, `ErrNoResponders`, `ErrNoServers`, `ErrDisconnected`, `ErrConnectionReconnecting`, `ErrReconnectBufExceeded`, `ErrNoStreamResponse`, `ErrAsyncPublishTimeout`, `ErrTooManyStalledMsgs` | `network` |
| `10077` cuja descrição é de um stream **cheio** com `discard=new` ("maximum messages exceeded", "maximum bytes exceeded", "maximum messages per subject exceeded") ou **fechando** ("store closed", um restart do NATS) | `network`: backpressure, passa quando um consumidor confirma ou o servidor volta (revisão independente, 2026-10-11; como `fatal` eram dump, exit 3 e duplicatas do pedaço já gravado na reingestão) |
| `10158` (409: o mesmo `Nats-Msg-Id` ainda numa proposta Raft, na retentativa depois de um prazo num stream em cluster), `10167` (429: a fila de entrada do stream cheia) e todo outro erro da API com código 429 | `network`: passam sozinhos (pendência 18 do S; antes, "outro 4xx" os fazia FATAL, dump e exit 3, talvez na frota inteira) |
| `ErrAuthorization`, `ErrAuthExpired`, `ErrAuthRevoked`, `ErrAccountAuthExpired`, as frases do servidor no primeiro `CONNECT` ("authorization violation", "authentication expired", "authentication revoked"; **nunca** "permissions violation", que é de um subject e vai pelo veredito), o veredito de permissão, `ErrConnectionClosed`, `ErrStreamNotFound` (`10059`), JetStream desligado (`10076`, `10039`), stream esperado não confere (`10060`), selado (`10109`), recursos (`10023`), outro armazenamento (`10077`: um registro que o store não aceita) | **`fatal`** |
| `ErrMaxPayload`, `ErrMaxBytesExceeded`, `10054`, `10097` | `logic` (só daquela mensagem) |
| outro erro da API com código 4xx (um 409 desconhecido inclusive) | `fatal` |
| outro erro da API com código 5xx, qualquer outro | `network` |

**Autenticação:** uma de `username`/`password`, `token` ou `credentials` (arquivo
`.creds`), e só uma (exit 1 com mais de uma). TLS verificado por padrão,
`tls_ca` própria, `tls_insecure` desliga.

### 10.8 `sink.type = none`

Descarta. Mede o custo de recepção, moldura, conversão e serialização sem I/O de
destino. **Nunca sai de UP.**

### 10.9 `LOGHUB_EB_URL`

Decisão 10. `loghub.eb_url` (env `LOGHUB_EB_URL`) é a URL do eventbus da
convenção LogHub. Quando preenchida, o **esquema** escolhe o sink e a URL
preenche a conexão dele:

| Esquema | `sink.type` | Preenche |
|---|---|---|
| `nats://`, `tls://` | `nats` | `sink.nats.urls` (a URL inteira, com usuário e senha se houver); `tls://` liga `sink.nats.tls` |
| `redis://`, `rediss://` | `redis` | `addrs`, `username`, `password`, `db` (do caminho); `rediss://` liga `tls` |
| `postgres://`, `postgresql://` | `postgres` | `addr`, `username`, `password`, `database`; `sslmode=require` ou `verify-full` liga `tls` |
| outro (inclusive `amqp://`) | — | exit 1: não suportado |

**Uma fonte só da verdade:** com `LOGHUB_EB_URL` preenchida, um `sink.type`
explícito diferente do esquema, ou qualquer chave que a URL **pode** expressar
definida explicitamente (flag, env ou INI), é exit 1 com as duas fontes no log,
mesmo que esta URL não traga aquela parte. As chaves "donas" da URL:

| Esquema | Chaves donas |
|---|---|
| `nats`, `tls` | `urls`, `tls`, `username`, `password`, `token` |
| `redis`, `rediss` | `addrs`, `username`, `password`, `db`, `tls` |
| `postgres`, `postgresql` | `addr`, `username`, `password`, `database`, `tls` |

As demais (`credentials`, `tls_ca`, `tls_insecure`, `sentinel_master`, `table`,
`stream`, `mode`, `pool_size`) convivem com a URL. Consequência: com Redis ou
PostgreSQL, a senha vai **dentro** da URL. `nats://` e `tls://` aceitam uma lista
separada por vírgula (o cluster do `nats.go`); Redis e PostgreSQL, um servidor
só. `sslmode=verify-ca` também liga o TLS; o PostgreSQL aceita `host=/dir` do
socket, e então **a porta da URL vale** e `addr` fica `/dir:porta` (a libpq disca
`/dir/.s.PGSQL.<porta>`; antes a porta era descartada e o cliente discava 5432,
talvez outro cluster). Qualquer outro parâmetro de consulta é recusado, não
ignorado, e um parâmetro **repetido** (`?sslmode=disable&sslmode=require`) é
exit 1: o leitor pegava o primeiro e a libpq pega o último (pendência 22 do S).
Os erros da URL nunca citam o texto dela (§16.2). Um `@` depois da primeira `/`
da autoridade (`postgres://u:12/ss@h/db`) é recusado: é uma `/` sem escape na
senha, que fazia o `net/url` ler `12` como porta e mandar o resto da senha para
o caminho; a mensagem pede `%2F`. O
`--dump-config` mostra os valores derivados como comentário, com a senha
redigida, para que o dump relido não dispare o conflito.

### 10.10 `sink.type = clickhouse`

Destino final num ClickHouse **compartilhado por todas as instâncias**. Tabela
em §3.7.2. É o terceiro destino remoto com esquema, e tudo de §10.5 que não é
citado aqui vale igual: o servidor valida e nunca cria (§10.5), a conta de
conexões (§10.5) e a retenção externa (§10.5). Ele muda duas coisas: o
destino é um armazém analítico, que quer **poucos `INSERT` grandes** (cada um
vira uma parte, e partes demais travam o merge); e o ClickHouse **não tem
transação**, o que muda a sonda de boot (§10.10.5) e a idempotência (§10.10.4).

#### 10.10.1 Cliente

`github.com/ClickHouse/clickhouse-go/v2`, pela API nativa (`clickhouse.Open`,
`driver.Conn`), no **protocolo nativo** (porta 9000; 9440 com TLS). Go puro,
sem CGO; a justificativa da dependência, escolhida pelo usuário contra o HTTP
da biblioteca padrão, está em §17.3. O protocolo nativo manda o pedaço em
colunas, comprimido com **LZ4** — o objetivo deste destino é vazão, e o JLH
comprime bem —, e o erro do servidor chega como `*clickhouse.Exception`, com o
`Code` que §10.10.7 exige.

O pool nasce **preguiçoso**: `clickhouse.Open` não disca, e o `WaitReady` do
boot (§13.1) é quem retenta. `sink.op_timeout` é o prazo de discagem e de cada
`INSERT` (pelo contexto). As conexões levam o nome da instância,
`sls/<AgentName>@<Hostname>` (a identidade de §13.1), no `client_name`,
visível no `system.query_log`.

#### 10.10.2 Concorrência e conexões

Igual a §10.5: `sink.workers` padrão vira **4** quando o operador não
configurou, `sink.clickhouse.pool_size` padrão é `sink.workers + 1` (abaixo de
`sink.workers` é exit 1), e com `ring=none` um `ring.max_inflight` acima do pool
cria fila escondida dentro dele, com `WARN` no boot. Aqui há um motivo a mais para poucos workers: cada
`INSERT` é uma parte nova no disco do servidor, e o ClickHouse prefere poucas
partes grandes (com muitas, `252` `TOO_MANY_PARTS`, §10.10.7). Com `ring=none`
cada mensagem é um `INSERT` de uma linha; quem quiser vazão ali liga o
`async_insert` no perfil do usuário (§10.10.6).

#### 10.10.3 Escrita

Um `INSERT` por pedaço (§10.1), pelo `PrepareBatch` do driver, com os valores em colunas:

```sql
INSERT INTO `banco`.`inbox_syslog`
  (uuid, from_uuid, event_at, sid, step, level, event_nsec, space, type, payload)
SETTINGS insert_deduplication_token = 'rls-<sha256 dos uuids do pedaço>',
         json_type_escape_dots_in_keys = 1,
         type_json_skip_duplicated_paths = 1,
         input_format_try_infer_dates = 0,
         input_format_try_infer_datetimes = 0,
         input_format_json_try_infer_numbers_from_strings = 0,
         throw_on_max_partitions_per_insert_block = 0
```

(Os ajustes viajam no pacote do comando, não no texto.) Os valores vêm do
cabeçalho do item e da conversão, como na `loghub.inbox` (§3.7.1): `level` é a
severidade, `sid` o da conversão (§6.10), `step` 0, `type` `syslog`. **Todas as
linhas são mapeadas antes de o comando sair**: um envelope que não mapeia (um
`from` que não é UUIDv7, só de outro ring ou dump) é `logic`, o pedaço inteiro,
sem nada enviado. O `payload` é o sub-slice do JLH do pedaço a partir de
`"payload":`, no modo string da coluna `JSON` do driver; o servidor o decompõe.
Numa recusa nada do pedaço fica gravado — o servidor lê o bloco inteiro antes
de escrever a parte —, então o `Error.Delivered` de §10.2 fica vazio. A exceção
é uma *materialized view* sobre a tabela: a recusa dela vem depois das linhas
gravadas, e o boot avisa (§10.10.5).

**Posse de buffer (§15.2).** O `Append` do driver **copia** cada valor para o
buffer da própria coluna (`ColStr.AppendBytes` do `ch-go`): quando o `Send`
começa, nada do comando referencia o buffer do pedaço. Não há releitura tardia,
como a do `nats.go` (§10.7). O custo é de duas a três cópias de cada `payload`
por tentativa (a coluna, o bloco serializado e o comprimido), o preço do protocolo colunar; os slices de colunas vêm de um pool e
voltam **zerados**, pela razão de §10.5.

**Por que a inferência desligada.** Por padrão o tipo `JSON` tenta ler toda
string como data, e um caminho que ganha a variante `Date`/`DateTime`/`DateTime64`
num bloco passa a ler por ela as strings das **outras** linhas do mesmo
caminho: na revisão independente de 2026-10-07, um cliente com
`Content-Type: 2020-01-01 10:00:00.000` fez o `content.type` e o `content.body`
vazios de outra requisição do mesmo lote voltarem como
`"1970-01-01 00:00:00.000000000"`, e um `X-Slack-Request-Timestamp: 1531420618`
voltar como data — a assinatura dele deixava de ser verificável a partir do
ClickHouse. O dado também mudava sozinho (`"2020-02-30"` lia `"2020-03-01"`), e
com perfis comuns do usuário (`date_time_input_format = best_effort`,
`input_format_json_try_infer_numbers_from_strings = 1`) todo timestamp e todo
número em texto. Nenhum `INSERT` falhava: era perda silenciosa. Os três ajustes
fazem toda string do JLH ficar string.

**Por que `throw_on_max_partitions_per_insert_block = 0`.** Um pedaço que cai em
mais partições que `max_partitions_per_insert_block` (100) — a reingestão de um
backlog de meses — era recusado com `252`, o código de `TOO_MANY_PARTS`, que é
`network`: o worker retentava para sempre. As partes são escritas assim mesmo e
o merge as junta; o boot já recusa uma chave de partição que não seja função de
`event_at` só (§10.10.5).

**O que o comando não leva**, de propósito: `async_insert`,
`wait_for_async_insert`, `insert_quorum`, `fsync_after_insert`. São decisões do
administrador do ClickHouse (§10.10.6).

**Por que os dois ajustes de `JSON`.** §3.7.2: as chaves que o emissor controla
(os SD-IDs e PARAM-NAMEs) são chaves de um `Map` e nunca viram caminho; os
ajustes defendem o resto do documento. Sem o escape, uma chave com ponto fora de um *hint* grava
`"a"` como folha e como prefixo, e a leitura devolve um JSON com chave
repetida; sem o segundo, duas chaves que dão o mesmo caminho recusam o `INSERT`
com `117` — `logic`, o pedaço inteiro. Os dois exigem o ClickHouse 25.8 (§10.10.5).

**O pedaço não é dividido de novo**, como no MySQL: o protocolo nativo não tem
teto de parâmetro nem de pacote (medido: `format_binary_max_string_size` não se
aplica, e um `payload` com 20 000 chaves entra em ~70 ms). O teto real é a
memória do servidor, que o boot confere (§10.10.5), e o `sink.batch_max_bytes`
já limita o pedaço.

#### 10.10.4 Desduplicação e at-least-once

Duas camadas, porque o ClickHouse não tem `ON CONFLICT`:

1. **Imediata: `insert_deduplication_token`.** O mesmo token num segundo
   `INSERT` faz o servidor descartar o bloco inteiro como duplicata. O token é
   `rls-` + os 32 primeiros dígitos hexadecimais do SHA-256 dos `uuid` do
   pedaço, em ordem: o retry de um pedaço (§10.2) reenvia o mesmo pedaço
   selado — a regra do outbox — e repete o token, e um `INSERT` que deu certo
   mas cuja resposta se perdeu (o prazo estourou depois do commit) não duplica.
   Pedaços diferentes nunca têm o mesmo token — os `uuid` são únicos —, e é por
   isso que ele sai do conteúdo, nunca de um contador: um token repetido para
   dados diferentes faria o servidor descartar o segundo pedaço **em
   silêncio**. O servidor guarda o token por partição: um pedaço que cruza a
   virada do mês são dois blocos, e um retry depois de uma falha parcial
   reescreve só o que faltou.
2. **Eventual: `ReplacingMergeTree`.** Uma reentrega fora da janela de tokens
   (1000 blocos na DDL de exemplo), ou agrupada de outro jeito — a reingestão
   de um dump (§12.2), um `ring.db` ou um ring Redis relidos — ganha outro token e é gravada; o
   merge a colapsa pela chave `(event_at, uuid)`. Até lá, a leitura exata usa
   `FINAL`.

O ClickHouse não diz quantas **linhas** foram descartadas: o
`X-ClickHouse-Summary` e o progresso contam as linhas enviadas também num
bloco descartado (medido). O que ele diz é o evento de perfil
`DuplicatedInsertedBlocks`, que o driver entrega por `WithProfileEvents`;
`relplog_sink_clickhouse_dedup_blocks_total` o acumula (§10.10.10).

#### 10.10.5 Validação no boot

Na primeira checagem de saúde bem-sucedida, com a regra de §10.5: cada recusa
é `ErrFatal`, o `WaitReady` desiste na hora e o processo sai com 3.

| Verificação | Como | Se falhar |
|---|---|---|
| A tabela existe e o usuário a enxerga | `system.tables` | exit 3 |
| ClickHouse 25.8 ou mais novo | `version()` | exit 3 — os dois ajustes de `JSON` de §10.10.3 não existem antes; recusar pela versão dá uma mensagem melhor que o `115` da sonda |
| Motor `ReplacingMergeTree`, `ReplicatedReplacingMergeTree` ou `SharedReplacingMergeTree` | `system.tables.engine` | exit 3 — um `MergeTree` guardaria para sempre cada reentrega fora da janela do token; um `Distributed` entrega aos shards de forma assíncrona e o `200` não significaria nada (§21) |
| O motor não tem coluna `is_deleted` (no máximo a de versão) | `system.tables.engine_full`, os argumentos que não são literais | exit 3 — o merge apaga as linhas que ela marca, e um valor fora de 0/1 (`level` 9) recusa o `INSERT` com `117` |
| A chave de partição, se houver, lê só `event_at` | `system.tables.partition_key` | exit 3 — partição por `received_at` deixava a reentrega noutra partição, que o merge nunca colapsa; uma por `uuid` espalhava um pedaço por centenas de partições |
| `uuid` é, sozinho, um elemento da chave de ordenação | `system.tables.sorting_key`, separado nas vírgulas de primeiro nível | exit 3 — uma chave sem `uuid` (só `event_at`) colapsaria no merge dois eventos **diferentes** do mesmo microssegundo: perda silenciosa com o sink UP |
| As dez colunas que o servidor escreve, com os tipos de §3.7.2: `UUID`; `DateTime64` de 6 a 9 dígitos, qualquer fuso; `UInt64` em `sid`, `UInt16` em `step` e `event_nsec`, `UInt8` em `level`; `String` ou `LowCardinality(String)` em `space` e `type`; extras toleradas | `system.columns` | exit 3 — o driver converte os valores no `Append`, no cliente, e recusa um tipo que não é o esperado (um `Int64` para um `[]uint64`, um `Nullable`) antes de mandar qualquer coisa: seria fatal no primeiro pedaço. Uma precisão menor que 6 trunca o microssegundo em silêncio |
| `payload` é `JSON` com `` `structured-data` Map(String, Map(String, Dynamic)) ``, e além dele só `max_dynamic_paths`/`max_dynamic_types` | `system.columns.type`, separado nas vírgulas de primeiro nível | exit 3 — sem o *hint*, um SD sem parâmetros some da linha e os nomes do SD viram caminhos (§3.7.2); a tabela dos webhooks (`params`, `query`) é recusada por isso; um `SKIP` descartaria parte do envelope em silêncio, e um caminho tipado a mais (`size UInt8`) recusaria o pedaço que não coubesse nele |
| As dez colunas são colunas comuns (sem `default_kind`, ou `DEFAULT`) | `system.columns.default_kind` | exit 3 — uma `EPHEMERAL` aceita o `INSERT` e não guarda nada (o `payload` inteiro sumia com o `Write` devolvendo sucesso); `MATERIALIZED` e `ALIAS` recusam o `INSERT` |
| `received_at`, se existir, `DateTime64` de 6 a 9 dígitos com `DEFAULT` ou `MATERIALIZED` | `system.columns` (`default_kind`) | exit 3 — sem expressão, o ClickHouse a preencheria com o zero do tipo, 1970, e o atraso medido por ela seria mentira |
| Nenhuma `CONSTRAINT … CHECK`, com o nome nu ou entre crases | `system.tables.create_table_query` | exit 3 — §3.7.2: a sonda vazia não a exercita, e uma violação seria `logic` em todo pedaço |
| `max_memory_usage` (quando não é `0`) comporta o pior pedaço: 128 MiB + 8 × `max(sink.batch_max_bytes, pior JLH)` | `getSetting(...)` na sessão do servidor de syslog | exit 3, com os números na mensagem — medido no 25.8 (no `go-webhook-server`): o `INSERT` de um bloco de `payload` JSON consome ~6× os bytes dele mais ~130 MiB fixos, e `241` é fatal (§10.10.7); sem a conta, um pedaço legítimo derrubaria a frota |
| `max_memory_usage_for_user` (quando não é `0`) comporta `pool_size` vezes isso | `getSetting(...)` | exit 3 — o limite do usuário soma todas as consultas dele: quatro pedaços legítimos e simultâneos estouravam um limite que a comparação com um só aceitava, e `241` é fatal. Outras instâncias no mesmo usuário somam por cima, e nenhuma as enxerga: dê a cada uma o seu usuário |
| `INSERT` concedido nas dez colunas, sessão gravável, ajustes aceitos | um `INSERT` **vazio**: `PrepareBatch` com o mesmo comando e os mesmos ajustes do dreno, e `Send` sem linhas, sob o mesmo prazo do dreno (`sink.op_timeout`) | exit 3 — `497` é o grant (a mensagem nomeia as colunas), `164` o modo `readonly` (com `readonly=1` o servidor recusa já a mudança de ajuste); zero linhas não criam parte (medido) |

O MySQL prova a escrita com uma linha de sonda numa transação revertida
(§10.6). O ClickHouse não tem transação, e uma linha de verdade ficaria na
tabela de produção para sempre. O `INSERT` vazio é a resposta: o servidor
confere o grant coluna a coluna e o modo `readonly` **ao receber o comando**,
antes de ler dado, com o mesmo comando que o dreno usa. O que ele não exercita
— uma CHECK, um tipo que só falharia com um valor — o catálogo cobre.

O pior pedaço é o teto de bytes dele, `sink.batch_max_bytes` (16 MiB), ou o
pior JLH (§3.9) quando ele sozinho passa do teto e forma um pedaço só
(`config.WorstChunkBytes`). Com os padrões dá 256 MiB de exigência. Um
`max_memory_usage = 0` (o padrão do ClickHouse) passa: aí o teto é o do
servidor inteiro (`max_server_memory_usage`), que esta sessão não enxerga.

A validação inteira roda com prazo próprio, `sink.op_timeout`, e não o
`sink.health_timeout` (1 s) da checagem que a dispara: são uma dúzia de idas e
voltas, e com 60 ms de RTT nenhuma tentativa cabia em um segundo. E é o prazo do
dreno de propósito: o driver o converte em `max_execution_time`, e um perfil que
proíbe mudar esse ajuste recusava o dreno com `452` depois de uma sonda que,
com outro prazo, passava. Uma tabela que alimenta *materialized views* gera um
`WARN`: uma recusa da view falha o `INSERT` depois de as linhas estarem gravadas,
e elas são contadas como descartadas.

A validação **não se repete**. Uma tabela dropada depois do boot aparece como
`60` na escrita, que é fatal (§10.10.7).

#### 10.10.6 Durabilidade e `async_insert`: decisão do administrador do ClickHouse

O servidor de syslog **não envia nem lê** `async_insert`,
`wait_for_async_insert`, `insert_quorum` ou `fsync_after_insert`: valem os do
perfil do usuário no ClickHouse. A decisão (do usuário, 2026-10-07) é que este
destino existe para **vazão**, e que segurança e commit são do administrador do
ClickHouse — a mesma linha da durabilidade do PostgreSQL (§10.5). O que a
confirmação de `ring=none` (o `200 OK` do RELP, §2.3) significa depende do
perfil:

| Perfil do usuário | O `INSERT` responde quando | Perda numa queda do **ClickHouse** |
|---|---|---|
| `async_insert = 0` (padrão) | a parte foi escrita | o que o `fsync_after_insert = 0` (padrão) deixou no cache do SO, numa queda da máquina |
| `async_insert = 1`, `wait_for_async_insert = 1` (padrão do par) | o buffer do servidor foi descarregado numa parte — agrupa os `INSERT` pequenos de `ring=none`, ao custo de até `async_insert_busy_timeout_ms` de latência | como a linha acima |
| `async_insert = 1`, `wait_for_async_insert = 0` | o servidor **recebeu** o dado no buffer em memória | o buffer inteiro, já confirmado ao emissor |

Com `async_insert` a deduplicação por token segue o
`async_insert_deduplicate` do perfil, que também é dele. A linha de §19 leva
a marca `†`.

#### 10.10.7 Classificação de erro

O servidor entrega o código numérico da exceção (`clickhouse.Exception.Code`),
e a tabela abaixo **é** a função `classifyClickHouseCode`;
`TestClassifyClickHouseCode` a lê de volta. Os nomes foram conferidos no
servidor com `errorCodeToName`. A regra de §10.5 vale igual: esquema e
permissão são `fatal`, nunca `logic`; um código desconhecido é `network`.

| Código | Situação | Classe |
|---|---|---|
| *(sem exceção)* | recusa de conexão, EOF, prazo, cancelamento, `ErrAcquireConnTimeout` | `ErrNetwork` |
| `3` `32` `33` `209` `210` | fim inesperado do fluxo, socket, rede | `ErrNetwork` |
| `159` `160` `394` | prazo do servidor, consulta lenta ou cancelada | `ErrNetwork` |
| `202` `203` `439` `473` | servidor ocupado: consultas simultâneas, sem conexão, sem thread, deadlock evitado | `ErrNetwork` |
| `252` | `TOO_MANY_PARTS`: o merge não acompanha — é o pedido do servidor para desacelerar | `ErrNetwork` |
| `225` `285` `319` `999` | Keeper fora, réplicas de menos, estado incerto do `INSERT`, exceção do Keeper | `ErrNetwork` |
| `236` `425` `1000` `1001` `1002` | abortado, erro de sistema, exceções genéricas | `ErrNetwork` |
| **`164` `242`** | **somente leitura**: o perfil do usuário (`READONLY`), ou a réplica sem Keeper (`TABLE_IS_READ_ONLY`) | **`ErrFatal`** — a decisão de §10.5 sobre `25006`, pelos mesmos motivos |
| `241` `173` `243` | sem memória (`MEMORY_LIMIT_EXCEEDED`, `CANNOT_ALLOCATE_MEMORY`), sem disco | `ErrFatal` — coerente com o `OOM` do Redis; o boot confere o limite de memória contra o pior pedaço (§10.10.5) |
| `192` `193` `194` `195` `497` `516` | usuário, senha, IP proibido, acesso negado, autenticação | `ErrFatal` |
| `16` `47` `53` `60` `81` | coluna, identificador, tipo, tabela ou banco que sumiram: contrato de esquema quebrado | `ErrFatal` |
| `36` `44` `62` `115` `452` | argumento, coluna que não aceita `INSERT`, sintaxe, ajuste desconhecido ou proibido pelo perfil | `ErrFatal` — repetiria em toda tentativa |
| `6` `26` `27` `41` `69` `70` `72` `117` `131` `469` | dado recusado: texto ou número que não converte, JSON inválido, string grande demais, CHECK violada | `ErrLogic` |
| outro código | desconhecido | `ErrNetwork` — encher o ring é visível, descartar não |
| erro do driver ao converter um valor (`ColumnConverterError`, `ErrBatchInvalid`) | o tipo de uma coluna mudou depois do boot | `ErrFatal` — repetiria em todo pedaço |

Uma coluna mudada para `String` depois do boot escapa: o driver converte o
`[]uint8` de `level` em bytes crus e o `Write` devolve sucesso. A validação não
se repete (`ALTER TABLE` em produção é do operador).

`117` (`INCORRECT_DATA`) é o JSON inválido. Um JLH deste servidor é sempre
JSON válido, e com os dois ajustes de §10.10.3 nenhuma chave de emissor o
provoca: na prática ele só vem de dano.

#### 10.10.8 `Health()`

`SELECT getSetting('readonly')`, que também prova a conexão. Um `readonly`
diferente de zero é
**fatal**: no ClickHouse ele é do perfil do usuário, não de um failover, e não
muda esperando — a regra de §13.1 sobre recusa definitiva no boot. Com motor
`Replicated*` (sabido na validação), também
`SELECT is_readonly FROM system.replicas` da tabela: `1` é falha de rede e o
estado vai para DOWN, como o `pg_is_in_recovery()` depois do boot (§10.5): a
réplica perdeu o Keeper e volta sozinha; um `242` chegando pela escrita
continua fatal (§10.10.7). Ler `system.replicas` exige
`GRANT SELECT(database, table, is_readonly) ON system.replicas` na imagem
oficial (`select_from_system_db_requires_grant`): sem ele (`497`) a checagem
da réplica é desligada com um `WARN`, em vez de derrubar o processo. O check também atualiza os gauges do pool
(§10.10.10).

#### 10.10.9 Retenção

O servidor não apaga nada. A poda é do administrador: `ALTER TABLE … DROP
PARTITION` por mês (a DDL particiona por `toYYYYMM(event_at)`), ou um `TTL` na
tabela. O `ReplacingMergeTree` não é uma fila: não há consumidor que apague.

#### 10.10.10 Métricas

```
relplog_sink_clickhouse_dedup_blocks_total      counter  blocos descartados pelo insert_deduplication_token
relplog_sink_clickhouse_pool_conns{state}       gauge    state: acquired|idle|max
```

Cardinalidade fixa (§14.1). `Stats()` do driver não conta esperas por conexão,
então não há `pool_waits_total`.

#### 10.10.11 Autenticação e TLS

`sink.clickhouse.username`/`password`. TLS como §10.5, com
`sink.clickhouse.tls`, `tls_ca` e `tls_insecure`: verificado contra o pool do
sistema ou a CA configurada, `ServerName` vazio para que o `tls.DialWithDialer`
o tire de cada endereço discado (há vários em `addrs`, como no NATS), e
`tls_ca` sem `tls` é erro de configuração. Com TLS, o endereço costuma ser a
porta 9440.

---

## 11. Plano de controle: heartbeat beacon

Decisão 8. O servidor é um driver de si mesmo (não existe `go-loghub-driver`),
então emite o heartbeat que a skill exige de todo worker, e **só** ele.

- O beacon impõe o piso de `1s` sozinho também, com `WARN` (`control.MinInterval`),
  para que nenhum caminho o transforme num laço.
- **Só com `sink=nats`** (nos dois modos). Com outro sink, nada é emitido e o boot
  registra `INFO`.
- **Quando:** na primeira conexão, em toda reconexão (o `ReconnectHandler` do
  `nats.go`) e a cada `loghub.beacon_interval` (env `LOGHUB_BEACON_INTERVAL`, em
  segundos; padrão 15; `0` desliga o periódico, não o de conexão). Fora o `0`, o
  mínimo é `1s`: `1ns` ou `15ms` são exit 1, qualquer que seja o sink, porque
  inundariam `<ctl_namespace>.beacon` e disputariam a conexão dos dados
  (pendência 21 do S).
- **Onde:** `<loghub.ctl_namespace>.beacon` (env `LOGHUB_CTL_NAMESPACE`, padrão
  `loghub._ctl`), publicado por NATS **core** (`nc.Publish`), em fanout, nunca no
  JetStream: o stream de dados não pode cobrir o plano de controle (§10.7).
- **Envelope:** `uuid` novo de nível 3 e `event` da mesma amostra, `from` =
  AgentUUID, `space` = Workspace, `type = "beacon"`, `level = 9`, `sid` = PID,
  `step = 0`, e o payload:

  | Chave | Valor |
  |---|---|
  | `name` | AgentName |
  | `hostname` | Hostname |
  | `machine` | MachineID (32 hex) |
  | `address` | o IP local da rota até o servidor NATS (um `net.Dial("udp", servidor)` sem tráfego, lendo o `LocalAddr`); `127.0.0.1` se não der |
  | `uptime` | milissegundos de vida do **processo** |
  | `meta` | `[{"key":"version","value":<versão>},{"key":"ring","value":<modo>},{"key":"sink","value":"nats"},{"key":"transports","value":[...]}]` |

- Falha de publicação é contada (`relplog_beacons_total{result="error"}`) e logada
  com amostragem; **nunca** afeta a saúde do sink.
- Não responde a sonar, não emite nem responde `ping`/`pong`, não assina nada:
  lacuna documentada até existir um driver (§21).

---

## 12. Arquivo de emergência

**Propriedade do ring, não do sink.** Dispara em FATAL ou com
`shutdown.drain_timeout` estourado, só com `ring.mode = memory` e ring (ou lote
retido pelos workers) não vazio.

| `ring.mode` | Ação |
|---|---|
| `memory` | dump para arquivo de emergência |
| `sqlite`, `redis` | nada: o conteúdo já está fora do processo e volta no próximo boot (ou noutra réplica) |
| `none` | nada: não há ring |

### 12.1 Formato

`<emergency.dir>/emg-<agent_uuid>-<unixnano>.bin`, com `emergency.dir` padrão
`<data_dir>/emergency`:

```
<len_head> <len_raw>\n
<cabeçalho de §3.4><cru>
<len_head> <len_raw>\n
...
```

Os comprimentos em decimal ASCII, seguidos dos dois blobs crus: **não é JSON**,
para não escapar o cru. O dump é exatamente o par cabeçalho + cru, com o
codificador de §3.4.

**Publicação atômica** (pendência 23 do S, corrigida como no SP/SN): o dump é
escrito sob o nome oculto `.emg-<agent_uuid>-<unixnano>.bin.tmp`
(`O_CREATE|O_EXCL`), com `fsync`, publicado por **`link`** no nome final (com
`rename`, depois de conferir o nome livre, quando o sistema de arquivos não tem
hard link — senão todo dump falharia ali) e com `fsync` do diretório. Falha em
qualquer passo, inclusive no `fsync` do **diretório**, remove o que foi escrito e
é falha do dump (§12.3): sem ela a entrada do diretório pode não sobreviver a uma
queda de energia e o processo sairia dizendo que salvou. Escrever direto no nome
final deixava, num processo morto no meio (`SIGKILL`, OOM), um arquivo parcial
com cara de pronto, que o boot entregava pela metade e removia sem contar o
resto; num diretório compartilhado, outra instância o pegava enquanto era
escrito.

### 12.2 Reingestão

No boot, todo `emg-*.bin` de `emergency.dir` é entregue ao sink **antes** da
drenagem do backlog e de qualquer transporte, em ordem lexicográfica de nome
(temporal pelo sufixo), e **removido só depois de todos os seus itens serem
confirmados**. Isso vale **qualquer que seja** o `ring.mode` e o
`emergency.enabled` atuais: um dump deixado por uma execução com `ring=memory` é
reingerido mesmo depois de o operador trocar para `ring=sqlite` em reação ao
incidente. Só a **escrita** de dumps depende do modo. Não há metadado a
reconstruir: o cabeçalho **é** o metadado, e o item reingerido é indistinguível
do original (mesmo `uuid`, mesma amostra, mesmos endereços, mesmo `space`); a
conversão produz o mesmo JLH (§3.5). Um dump de uma vida com outro Workspace vai
para o subject/stream do `space` do item (§3.6, §3.8); se nenhum stream o
cobrir, a entrega falha como `network` até `sink.boot_timeout` e o boot sai com 3,
sem remover o arquivo: o operador decide se cria o stream ou move o dump.

**Temporários interrompidos.** Antes da reingestão, o temporário gêmeo de um
dump completo (o mesmo arquivo, um corte entre o `link` e a remoção) é removido,
e todo outro temporário é listado num `WARN`: contado como perda só se um segundo
sinal abortou o dump; depois de `SIGKILL` ninguém o contou. Nunca é lido nem
apagado; renomeado pelo operador para o nome final, os registros íntegros entram
no boot seguinte.

**`sink=none` não reingere** (pendência 26 do S, "a decidir pelo usuário",
decidida aqui): o descarte por configuração destruiria mensagens que uma vida com
destino real confirmou com `200 OK`. Os arquivos ficam no disco, nomeados num
`WARN` e contados nos gauges, à espera de um boot com destino.

**Gauges recontados** (pendência 25 do S): ao fim da reingestão,
`relplog_emergency_files` e `relplog_emergency_events_pending` são recontados do
diretório, nunca zerados: um dump que ficou (ilegível, remoção ou quarentena que
falharam) continua contado — o ilegível como arquivo, sem mensagens —, e o
alerta sobre eles continua disparando. O dump escrito no shutdown soma aos
gauges em vez de fixá-los.

**Remoção durável.** Depois de entregue, o dump é removido e o diretório recebe
`fsync`: sem ele, uma queda de energia logo depois devolveria o nome, e o boot
seguinte entregaria o dump inteiro de novo (com os mesmos `uuid`, deduplicados só
onde o destino deduplica). Um `fsync` que falha depois do `unlink` é `WARN`: o
arquivo já saiu.

Arquivo truncado ou corrompido: processado até o último registro íntegro, o resto
logado em `ERROR` com a contagem de bytes, e o arquivo renomeado para
`.corrupt`. Se o `rename` aconteceu e só o `fsync` do diretório falhou, o arquivo
**está** quarentenado (pendência 24 do S): o `fsync` vai em `WARN` e o `ERROR`
com os bytes descartados sai igual, com o nome novo; só sem nome devolvido é
"cannot quarantine". Um registro de cabeçalho indecodificável é pulado e também torna o
arquivo danificado (quarentena depois da entrega, nunca remoção). A linha de
comprimentos tem no máximo 32 bytes. O decodificador **nunca confia no comprimento declarado** e tolera
truncamento sem pânico (`FuzzRead`).

### 12.3 Falha do próprio dump

Disco cheio, sem permissão, `emergency.enabled = false`: `ERROR` com a contagem
exata, `relplog_events_dropped_total{reason="emergency_failed"} += n`, exit 3.
`emergency.dir` é criado **no boot**: um diretório criado durante o incidente
falha durante o incidente.

---

## 13. Ciclo de vida

### 13.1 Boot

```
 1. config (§16): flags > env > INI > padrão. --help, --version e
    --dump-config saem aqui, sem tocar identidade nem disco (a seção
    informativa [identity] do dump mostra o que o AMBIENTE diz, sem
    resolver nada). Os arquivos do TLS são conferidos aqui, leitura E
    conteúdo: certificado e chave que fazem parse e combinam, CA com ao
    menos um certificado (exit 1).
    Erro ou combinação inválida => exit 1
 2. data_dir: resolvido para absoluto, criado (0750), exportado como DATADIR
 3. lhident.Initialize() — falha => o processo sai com 100–114, pela biblioteca
 4. Identity congelada; space validado (§13.2) => exit 1 se inválido
 5. porta ADMIN aberta: /healthz 200, /readyz 503 com o motivo, /metrics
 6. emergency.dir criado (com ring=memory e emergency.enabled: o único modo
    que escreve dumps; a reingestão do passo 9 lê o diretório em qualquer modo)
 7. SINK: WaitReady com retentativa até sink.boot_timeout (300 s) => exit 3;
    validação de esquema/stream/permissão e do maior evento (§3.9):
    recusa => exit 3 IMEDIATO, com a própria linha de log ("the sink refused
    this server permanently"), sem citar sink.boot_timeout, que não chegou a
    correr; a linha do boot_timeout fica para o destino inalcançável
 8. RING: memory/none: nada; sqlite: abre ring.db e conta o backlog;
    redis: conecta (até sink.boot_timeout, senão exit 3), cria o grupo,
    zera o contador órfão, relê os próprios pendentes
 9. reingere emg-*.bin (§12.2), qualquer que seja o ring.mode atual
10. DRENA o backlog local integralmente (ring=sqlite), com /readyz
    "draining backlog: N pending" e NENHUM transporte aberto. Com ring=redis
    não espera o backlog global, que outras réplicas alimentam
    Nos passos 9 e 10 a entrega segue as classes dos workers (§10.2), CHUNK
    A CHUNK (sink.Instrumented.Deliver): `logic` descarta, conta e loga
    aquele chunk e segue para o próximo, e o lote é confirmado (sair deixaria
    as mesmas linhas para o boot seguinte, num crashloop com todos os
    transportes fechados; parar no chunk recusado deixaria os seguintes sem
    escrita e confirmados, perdidos sem contagem); `network` reenvia o MESMO
    chunk selado, sem reconverter, com backoff dentro de sink.boot_timeout;
    `fatal` sai com 3; um sinal sai com 130
11. beacon (só sink=nats)
12. abre os transportes: tcp, tls
13. /readyz 200
```

**Identidade antes de tudo que escreve.** `lhident.Initialize()` roda **uma
vez**, no `main`, depois das flags que encerram o programa e antes de abrir
qualquer recurso com `defer` ou criar goroutine (regra 2 da skill). A app recebe
um struct `Identity` (`From`, `Space`, `AgentName`, `Hostname`, `MachineID`)
preenchido dos getters: os testes montam a app dezenas de vezes no mesmo
processo, e uma segunda chamada a `Initialize()` sai com 112.

**`DATADIR` é exportado antes do `Initialize()`** para que a biblioteca grave
`agent_uuid` e `machine_id` na mesma raiz em que o servidor grava `ring.db`,
`events.db` e `emergency/` (§16.6). Sem `DATADIR` absoluto, o padrão `./data`
vira absoluto a partir do diretório de trabalho; no contêiner é `/data`.

**A porta admin abre antes do sink:** num crashloop por destino indisponível, o
operador tem `/readyz` com o motivo e `/metrics` com o backlog.

**Os transportes abrem por último e juntos.** Abrir o RELP antes de o sink estar
pronto seguraria toda transação até o `backpressure_timeout` e devolveria todas;
um transporte que não está escutando faz o `omrelp` esperar do lado dele, que é
o certo.

### 13.2 Validação de configuração

| Combinação | Efeito |
|---|---|
| Nenhum `listen.*` | exit 1 |
| Dois `listen.*` no mesmo endereço e protocolo (TCP e TLS na mesma porta inclusive) | exit 1; com a mesma porta, um **nome** de host de qualquer lado conta como o mesmo endereço (`localhost` e `127.0.0.1`): para dividir a porta, dois literais IP distintos |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `ring.sqlite.path`, `sink.sqlite.path` ou `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `relp.window` fora de 1–65536 | exit 1 |
| `relp.max_frame_bytes` < `syslog.max_message_bytes` ou > 999 999 999 | exit 1 (o fusível fecharia sessões em linhas que o truncamento guardaria; nove dígitos de `DATALEN`) |
| `relp.inflight_max_bytes` < `syslog.max_message_bytes` | exit 1 (uma transação precisa sempre caber) |
| `listen.tls` sem `tls.cert_file`/`key_file`, com arquivos ilegíveis, certificado ou chave que não fazem parse, chave de outro certificado, ou `tls.client_ca` sem nenhum certificado PEM | exit 1, no passo 1 (antes da espera pelo sink: com um sink inalcançável o erro do TLS ficaria escondido atrás de um exit 3) |
| `tls.client_auth = require` sem `tls.client_ca` | exit 1 |
| `proxy.protocol ≠ off` sem `proxy.trusted` válido | exit 1 |
| `space` fora de `^[a-z0-9-]+(\.[a-z0-9-]+)*$` | exit 1 (a lib aceita `a..b`, `.x` e `x.`; o subject não). Acima de 64 caracteres a própria lib já sai com 111 antes |
| `space` igual a um prefixo do plano de controle (o primeiro token começa com `_`) | impossível: `_` não é permitido em `space` |
| `syslog.default_priority` fora de 0–191 | exit 1 |
| `syslog.rfc3164_timezone` desconhecido para o `time.LoadLocation` | exit 1 (a imagem embute `time/tzdata`) |
| `syslog.legacy_charset` fora de `""`, `cp1252`, `latin1`, `windows-1252`, `iso-8859-1` | exit 1 |
| `syslog.max_message_bytes` < 480 ou > 16 MiB | exit 1 (480 é o mínimo da RFC 5424) |
| `ring=sqlite` + `sink=sqlite` no mesmo arquivo | exit 1 |
| combinações de §9.7 que degradam | `WARN` |
| `sink=sqlite` com `workers > 1` | `WARN`, forçado a 1 |
| `sink=postgres`/`mysql`/`clickhouse` com `workers` no padrão | rebaixado para 4, `WARN`; `WARN` acima de 16 |
| `pool_size` abaixo de `workers` | exit 1 |
| `sink.mysql.database` ou `sink.clickhouse.database` com crase, barra invertida, byte de controle (< 0x20 ou 0x7F) ou mais de 64 bytes | exit 1: o nome entra cru entre crases no `INSERT` e na sonda; uma crase quebraria o comando e, no MySQL, o `1064` seria `network` até o `boot_timeout` (pendência 12 do S) |
| `sink.postgres.addr` que não é um `host:porta` (porta 1–65535, um servidor, sem vírgula) nem um diretório de socket começando com `/`, com `:porta` opcional | exit 1 (`h1,h2:5432` falhava no DNS até o `boot_timeout`) |
| uma entrada de `sink.nats.urls` que não parseia, sem host, de esquema fora de `nats`/`tls`/`ws`/`wss`, ou com caminho, consulta ou fragmento em `nats://`/`tls://` (`host:porta` sem esquema vale como `nats://`) | exit 1, sem citar a entrada (§16.2) |
| credencial na URL do NATS junto de `username`/`password`/`token`/`credentials` | exit 1: "uma forma só"; o `nats.go` usaria a da URL e ignoraria a chave em silêncio |
| `loghub.beacon_interval` entre 0 e 1 s (exclusive) | exit 1 (§11) |
| `sink=mysql` com `sink.batch > 3640` | exit 1 |
| `sink=clickhouse` sem `addrs`, com endereço que não é `host:porta`, `table` que não é identificador simples ou `pool_size` negativo | exit 1 |
| `ring=redis` com `ring.redis.claim_idle ≤ 2 × sink.op_timeout` | exit 1: uma escrita mais longa que a janela deixaria outra réplica reivindicar um lote vivo (§9.5) |
| `sink=nats` sem `urls`; mais de um método de autenticação; `password` sem `username` | exit 1 |
| `sink.nats.mode = core` | `WARN`: best effort |
| `LOGHUB_EB_URL` em conflito (§10.9) ou com esquema não suportado | exit 1 |
| `tls_ca` sem `tls` em qualquer cliente | exit 1 |
| `filter.min_severity` fora de 0–7 ou nome desconhecido | exit 1 |
| `ratelimit.enabled = false` com listener fora do loopback | `WARN` |
| durações zero onde zero não é "sem limite" (§16.5) | exit 1 |
| `GOMEMLIMIT` ausente, ou `< ring.max_bytes × 2` com `ring=memory` | `WARN` (§15.4) |

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
- **FATAL é terminal.** Toda transição e o gauge `relplog_sink_up` são escritos
  sob o mesmo lock, e nada sai de FATAL: uma checagem concorrente não pode
  devolver o estado a UP/DOWN depois de um `ClassFatal` do `Write()`.
- `ClassNetwork` no `Write()` **não** muda o estado; o pedaço é retentado.
- Em **DOWN**, vale o backpressure de §2.3 mesmo com o ring vazio (fail-fast), e o
  `/readyz` falha.
- Em **FATAL**: §12 e exit 3.
- `sink=none` nunca sai de UP.

### 13.4 Shutdown gracioso

`SIGTERM` ou `SIGINT`:

```
fase 1  /readyz passa a falhar
fase 2  espera shutdown.pre_stop_delay (0 s), ainda aceitando
fase 3  cancela o contexto de recepção e fecha os dois transportes ao mesmo
        tempo (um closeGrace no total): as transações decididas recebem a
        resposta, as seguradas ficam sem ela, sai serverclose e cada sessão
        fecha (§5.8)
fase 4  drena o ring para o sink, com shutdown.drain_timeout (30 s), incluindo
        Workers.Pending()
fase 5  para o beacon, fecha o sink, o ring e o SQLite, exit 0
```

- O contexto de recepção é cancelado **antes** de os transportes fecharem: é o
  que solta as transações presas no laço de backpressure, sem resposta (§5.8). Um item já entregue ao ring
  não é afetado: o `Enqueue` do sqlite espera o group commit, e o `ring=none`
  escreve num contexto desligado.
- **Nenhum `Enqueue` depois do `Drain`.** O `Close` de cada transporte só volta
  quando as sessões dele voltaram (com todas as transações decididas ou
  abandonadas e as respostas entregues ao socket; depois de `closeGrace`, 6 s,
  fecha o socket de quem não voltou).
  Por isso o `Drain` da fase 4 vê tudo o que entrou no ring: um item que
  entrasse depois dele ficaria num ring que ninguém mais lê, sem entrega, sem
  dump e sem contador.
- `drain_timeout` estourado com itens: §12 e exit 3, com `ring=memory`/`none`.
  Com `ring=sqlite` e `ring=redis` nada é despejado nem contado como perdido: as
  linhas e as entradas continuam lá, o processo registra `WARN` e sai com 0 (3 se
  o sink estiver FATAL).
- **Fase 4 com `ring=redis`:** a ocupação é global e nunca zera; o que esta
  instância deve são os lotes que os workers já seguram. O ring para de entregar
  entradas novas (`Quiesce`) e os workers terminam o que têm, até
  `drain_timeout`: uma escrita em voo não vira duplicata cortada, e nenhuma
  entrada nova entra na lista de pendentes desta instância na saída. O que
  sobrar fica pendente no stream e é reivindicado. `Quiesce` é idempotente: libera a fila de reivindicadas (que continuam
  pendentes no Redis), desliga a rodada de `XAUTOCLAIM`, e a partir dele o
  `Dequeue` espera `min(BlockTimeout, 100 ms)` e devolve lote vazio, sem
  releitura, sem entregar reivindicadas e sem `XREADGROUP` novo; `Commit` e
  `Touch` continuam funcionando para os lotes retidos. A app considera os workers
  ociosos quando duas leituras de `Workers.Held()` separadas por 50 ms dão zero.
- **Shutdown por FATAL** (§13.3) começa sem sinal: o **primeiro** `SIGTERM`/`SIGINT`
  que chega nele (um `docker stop`, um deploy) é logado e **tolerado**, e o
  shutdown segue até o dump e o exit 3; o segundo aborta com 130; `SIGQUIT` nunca
  é tolerado. Antes, o primeiro sinal era tratado como o segundo e abortava no
  meio do dump, com o ring perdido (pendência 28 do S).
- **Aborto durante o dump** conta em `reason="abort"` **todo** o conteúdo do dump:
  o `Drain` já tirou esses itens da contabilidade do ring, e o `os.Exit` corta a
  escrita. O contador do que está sendo despejado é zerado assim que a escrita
  termina, antes de contar `emergency_failed`, para a mesma perda não ser contada
  duas vezes (pendência 29 do S).
- **A posse da contagem é de um lado só** (revisão independente, 2026-10-11). Com
  `ring=memory`/`none`, a fase 4 grava a ocupação do ring nesse contador e liga a
  "fase de dump" **antes** do `Drain`: dali em diante a ocupação do ring não mede
  mais nada (os lotes retidos ainda contam lá embora vão para o dump; os drenados
  já não contam). O aborto e o `dumpEmergency` (dump falho, desligado ou sem
  diretório) tomam o contador com uma troca atômica (`Swap(0)`): exatamente um
  deles conta aquelas mensagens. Antes, um aborto entre o `Drain` e o dump não
  contava nada, e um aborto durante um dump que falhava contava duas vezes.
- **`SIGQUIT` tardio**: um `SIGQUIT` que chega depois de o shutdown começar e
  antes de ele registrar o canal de aborto só alcança o observador do boot, que
  marca a saída imediata; o shutdown confere essa marca antes de qualquer fase e
  sai com 130. Antes, esse `SIGQUIT` era perdido e o shutdown seguia gracioso.
- **Segundo sinal** em qualquer fase: saída imediata, `exit 130`, aceitando a
  perda (contada em `reason="abort"` com `ring=memory` e `none`; com `sqlite` e
  `redis` o que sobrou continua no ring). `SIGQUIT` aborta sempre, inclusive
  fora do shutdown: no boot e com o servidor no ar, saída imediata com 130.
- **Sink FATAL durante a fase 4** (o `SIGTERM` chegou com o sink UP e ele virou
  FATAL no dreno): a espera termina na hora, em vez de gastar o `drain_timeout`
  num dreno que não acontece mais, e a saída é **3** em qualquer ring; com
  `memory`, o que sobrou vai para o dump.
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
  `pre_stop_delay + drain_timeout + closeGrace`.

### 13.5 Códigos de saída

| Código | Significado |
|---|---|
| `0` | shutdown completo: ring vazio, ou `sqlite`/`redis` com o resto guardado |
| `1` | erro de configuração ou de inicialização não relacionada a destino |
| `3` | destino indisponível: sink FATAL, `boot_timeout` ou `drain_timeout` estourados, ring Redis indisponível no boot, maior evento que não cabe, guarda de não-progresso do dreno |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` (`DATADIR`, `MACHINE_ID`, `MACHINE_ID_FILE`, `AGENT_NAME`, `AGENT_UUID`, `HOSTNAME`, `WORKSPACE`; 112 = `Initialize()` duas vezes); a tabela completa está na skill `go-loghub-ident` e em `docker/README.md` |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 7–10 do boot |

---

## 14. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9103` (o `go-webhook-server` usa `9101` e o
`go-syslog-server`, `9102`; os três rodam lado a lado). HTTP/1.1.

| Path | Semântica |
|---|---|
| `/healthz` | `200` enquanto o processo vive. Liveness. |
| `/readyz` | `200` só se: sink `UP`, ring abaixo de `ring.high_water`, backlog de boot zerado, ring Redis alcançável (com `ring=redis`), porta pública aberta e não em shutdown. Senão `503` com o motivo em `text/plain`. |
| `/metrics` | exposição Prometheus em texto. |

### 14.1 Métricas

Prefixo `relplog_`. **Nenhum label deriva de conteúdo controlado pelo emissor**
(nada de hostname, appname, IP); todo label tem conjunto finito conhecido em
compilação. `facility` e `severity` **não** são labels: o produto com
`transport` daria 768 séries, e a distribuição por severidade é pergunta para o
destino, onde é coluna indexada.

| Métrica | Tipo | Labels |
|---|---|---|
| `relplog_messages_total` | counter | `transport` (tcp, tls): frames `syslog` lidos |
| `relplog_messages_accepted_total` | counter | `transport` |
| `relplog_messages_dropped_total` | counter | `reason` (filter, empty, unaddressable): só descartes **finais**; o que o RELP devolve ao emissor e o que o rate-limit segura não estão aqui (§5.4, §8) |
| `relplog_messages_truncated_total` | counter | `transport` |
| `relplog_message_bytes` | histogram | — (tamanho original) |
| `relplog_convert_total` | counter | `format` (rfc5424, rfc3164, malformed) |
| `relplog_convert_warnings_total` | counter | `code` (os 14 de §6.9) |
| `relplog_convert_duration_seconds` | histogram | — |
| `relplog_conns_active`, `relplog_conns_total` | gauge, counter | `transport` (tcp, tls) |
| `relplog_conns_refused_total` | counter | `transport`, `reason` (global, per_ip, proxy); `global` conta cada vez que o accept estaciona no teto, um limite inferior, porque o teto global bloqueia em vez de recusar; `proxy` conta as conexões fechadas pelas regras de §4.6 |
| `relplog_tls_handshake_errors_total` | counter | — |
| `relplog_proxy_headers_total` | counter | `result` (v1, v2, local, absent, invalid) |
| `relplog_backpressure_seconds_total` | counter | `transport` (tcp, tls) |
| `relplog_confirm_duration_seconds` | histogram | — (do frame completo ao `200 OK` decidido; só aceitas) |
| `relplog_relp_frames_total` | counter | `command` (open, syslog, close, unknown) |
| `relplog_relp_responses_total` | counter | `code` (200, 500); `500` é final: o `librelp` descarta a mensagem |
| `relplog_relp_unanswered_total` | counter | `reason` (backpressure, shutdown, conn_lost, server_closed): transações lidas e devolvidas ao emissor, que as retransmite (§5.4) |
| `relplog_relp_frame_errors_total` | counter | `reason` (malformed, too_large) |
| `relplog_relp_inflight_bytes`, `relplog_relp_inflight_transactions` | gauge | — (o teto de §5.6 e a ocupação das janelas) |
| `relplog_relp_inflight_waits_total` | counter | — (paradas de leitura por teto em voo esgotado) |
| `relplog_ring_items`, `relplog_ring_bytes` | gauge | — (global com `ring=redis`) |
| `relplog_ring_capacity_items`, `relplog_ring_capacity_bytes` | gauge | — |
| `relplog_ring_quarantined_total` | counter | — |
| `relplog_ring_redis_claimed_total` | counter | — (entradas reivindicadas por `XAUTOCLAIM`) |
| `relplog_sink_batch_size`, `relplog_sink_batch_bytes` | histogram | — (por pedaço) |
| `relplog_sink_duration_seconds` | histogram | — |
| `relplog_sink_health_duration_seconds` | histogram | — |
| `relplog_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); o cancelamento do shutdown não conta |
| `relplog_sink_up` | gauge | — |
| `relplog_sink_sqlite_file_bytes`, `_rotations_total`, `_files_removed_total` | gauge, counter | — |
| `relplog_sink_postgres_duplicates_total`, `relplog_sink_mysql_duplicates_total`, `relplog_sink_nats_duplicates_total` | counter | — |
| `relplog_sink_postgres_pool_conns`, `relplog_sink_mysql_pool_conns` | gauge | `state` (acquired, idle, max) |
| `relplog_sink_postgres_pool_waits_total`, `relplog_sink_mysql_pool_waits_total` | counter | — |
| `relplog_sink_nats_reconnects_total` | counter | — |
| `relplog_sink_clickhouse_dedup_blocks_total` | counter | — (blocos descartados pelo token, §10.10.4) |
| `relplog_sink_clickhouse_pool_conns` | gauge | `state` (acquired, idle, max) |
| `relplog_events_written_total` | counter | — |
| `relplog_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero, como todo label de conjunto fechado (um `rate()` precisa da série antes do primeiro evento) |
| `relplog_emergency_files`, `relplog_emergency_events_pending` | gauge | — |
| `relplog_beacons_total` | counter | `result` (ok, error) |
| `relplog_ratelimit_buckets` | gauge | — |
| `relplog_ratelimit_held_total` | counter | — (transações seguradas à espera de uma ficha: o emissor desacelerado, nada descartado) |
| `relplog_build_info` | gauge | `version`, `commit`, `go` |
| `relplog_config_info` | gauge | `ring_mode`, `sink_type`, `transports` |
| `relplog_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

As séries de todo destino são expostas qualquer que seja o `sink.type`, em zero
quando o destino é outro: um painel serve a toda instância. Cada label tem teste
de exposição.

**Fronteiras de histograma** são estáticas, idênticas em toda instância e
**pré-formatadas como string** na inicialização: emitir `0.01` num scrape e
`0.010` no seguinte cria duas séries e quebra todo quantil.

### 14.2 Logs

`slog`, `json` por padrão (`text` disponível). **Nenhum log síncrono por mensagem
no caminho padrão.** `log.access = true` liga log por mensagem com
`log.access_sample` (0.01); em syslog isso é logar log. Avisos de anomalia
(conversão, PROXY, TLS, beacon) são **amostrados**, um a cada 30 s por tipo, com
o contador exato em `/metrics`: um emissor quebrado não inunda o log do coletor.
Nenhum log em `INFO` ou acima carrega o payload de uma mensagem.

---

## 15. Memória e runtime

### 15.1 Linha crua

Buffers de pools por classe: 2 KiB, 8 KiB e 64 KiB. Acima de 64 KiB (só quando o
operador sobe `max_message_bytes`) o buffer é alocado direto e **nunca** volta a
um pool: pooling de pior caso faz o pool segurar o pior caso para sempre. A
mediana de uma mensagem syslog fica em 150–250 bytes e o p99 raramente passa de
2 KiB.

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
| `ErrUnavailable` do ring Redis | o pipeline do rueidis pode ter o comando na fila | **descarta sem devolver** (`Discard`) |
| qualquer outro (prazo, cancelamento, falha do destino com `ring=none`) | o pipeline do cliente do destino | **descarta sem devolver** |

Nos workers: uma tentativa que termina por prazo ou cancelamento **contamina** o
pedaço (`Taint`), e nem o cru dos itens nem o buffer do pedaço voltam ao pool,
nem quando a retentativa seguinte tem sucesso. O custo é uma alocação por item no
caminho de falha.

### 15.3 Memória do JLH, que não é do ring

O JLH vive entre o passo 2 e o passo 4 de §10.1, num buffer de pedaço. O teto é
`sink.workers × sink.batch_max_bytes` (8 × 16 MiB = 128 MiB no pior caso), mais
as colunas promovidas. Não é somado a `ring.max_bytes`: memórias com donos e
ciclos de vida diferentes, e somá-las esconderia o estouro de uma atrás da folga
da outra. Os buffers de pedaço têm pool próprio, de uma classe (o teto), e um
buffer que cresceu acima dela não volta.

### 15.3a Memória da recepção RELP, que também não é do ring

Duas parcelas, as duas com teto:

- **Cópias das transações em voo** (§5.6): no máximo `relp.inflight_max_bytes`
  (64 MiB) para o processo inteiro, contadas pela capacidade real do buffer.
- **Custo fixo por conexão** (§5.2): cerca de 10 KiB (leitor de 4 KiB, escritor
  de 4 KiB, o primeiro bloco do frame) mais as goroutines do leitor e do
  escritor; um frame grande não deixa um buffer grande para trás. Com
  `tcp.max_conns`/`tls.max_conns` em zero (padrão), esse termo cresce com o
  número de conexões; quem espera milhares de emissores fixa o teto.

A conta de memória do processo é `ring.max_bytes + sink.workers ×
sink.batch_max_bytes + relp.inflight_max_bytes + conexões × ~10 KiB`.

### 15.4 Runtime

- `GOMEMLIMIT` deve ser configurado: `WARN` se ausente ou, com `ring=memory`,
  abaixo de `ring.max_bytes × 2`. Os compose definem `GOMEMLIMIT` **e** o limite
  do contêiner, o segundo acima.
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

### 16.2 Registry único, e os nomes de env fixados pelas skills

Uma **tabela** declara cada parâmetro (seção, chave, tipo, padrão, descrição, se
é recarregável, nome de env). Dela saem o INI, as flags, o env, o `--help`, o
`--dump-config` e a validação. Um parâmetro novo entra só ali. Sem
`gopkg.in/ini.v1` e sem `spf13/pflag`.

Nomes de env que **não** seguem a regra, porque uma skill os fixou:

| Seção | Env | Origem |
|---|---|---|
| `[syslog]` | `SYSLOG_<CHAVE>` (sem repetir a seção, e **com** o prefixo da skill, não o deste servidor: a mesma conversão do `go-syslog-server`, e um `.env` comum garante config de conversão idêntica entre os dois): `SYSLOG_DEFAULT_PRIORITY`, `SYSLOG_RFC3164_TIMEZONE`, `SYSLOG_MAX_MESSAGE_BYTES`, `SYSLOG_PAYLOAD_RAW`, `SYSLOG_LEGACY_CHARSET`, `SYSLOG_ACCEPT_MALFORMED`, `SYSLOG_SD_MAX_ELEMENTS`, `SYSLOG_SD_MAX_PARAMS` | skill `loghub-type-syslog-schema` §15 |
| `[loghub]` | `LOGHUB_<CHAVE>`: `LOGHUB_EB_URL`, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE` | skill `loghub-json-schema` |
| `server.data_dir` | `DATADIR` | `go-loghub-ident` |

As variáveis de identidade (`AGENT_UUID`, `AGENT_NAME`, `WORKSPACE`, `HOSTNAME`,
`MACHINE_ID`, `MACHINE_ID_FILE`, `LOGHUB_IDENT_DEBUG`) **não** passam pelo
registry: são lidas pelo `go-loghub-ident`, e o `--dump-config` mostra os valores
o que o **ambiente** diz numa seção informativa `[identity]`, sem resolver nada
(§13.1 passo 1: o dump não toca identidade nem disco).

**Erros de configuração não ecoam a entrada** (pendência 30 do S, corrigida
como no N): nenhum erro cita texto que possa ser segredo — URL que não parseia
(só o tipo do erro e a dica de codificar `%`, `/`, `#`, `?` e `,`), chave de
consulta PostgreSQL citada só se estiver na lista fixa da libpq, `sslmode`
inválido sem o valor, linha INI sem `=` ou cabeçalho malformado (só o número da
linha), nome de parâmetro desconhecido citado só se tiver forma de nome,
argumento solto citado só até o `=`. Nas flags, um argumento com forma de flag
nunca é valor de outra flag (`--sink.nats.password --sink.redis.password=x` é
recusado, não engolido) e só é nomeado se for flag deste servidor; nada depois
de um valor (cauda partida pelo shell, valor depois de `= `) é citado. Um
argumento desconhecido com forma de flag logo depois de um valor
(`--sink.nats.password=abc -hunter2`, `... abc --x=y`) pode ser a cauda desse
valor partida num espaço: é nomeado só quando cai numa seção que este servidor
tem (`--sink.nats.subjet`, um erro de digitação); fora disso o erro cita só a
flag anterior e pede aspas (revisão independente, 2026-10-11: antes, a cauda
`-hunter2` saía no stderr). Os erros vão para o stderr e, num `SIGHUP`, para o
log.

**Um `@` depois do primeiro `/`, `?` ou `#` da autoridade de uma URL é
recusado**, sem citar a URL: nenhuma URL destes esquemas tem `@` literal no
caminho, na consulta ou no fragmento, então é um usuário ou senha com `/`, `?`
ou `#` sem codificar, que encurta a autoridade (`postgres://app:39182#Zs3@db/x`
discava `app:39182` sem senha, com o `sslmode` jogado fora no fragmento). Uma
URL PostgreSQL com fragmento também é recusada.

Durações aceitam a sintaxe do Go (`30s`, `5ms`). `loghub.beacon_interval` aceita
também um **inteiro puro em segundos** (a skill define a variável em segundos).
Tamanhos aceitam **só** `KiB`, `MiB`, `GiB` (sem diferença de caixa) e inteiros
puros em bytes; `KB`, `MB`, `K` e `M`, que a standard aceitava, são recusados.
`syslog.rfc3164_timezone = Local` é recusado: faria réplicas converterem de
formas diferentes (§3.5); vazio vale `UTC`.

### 16.3 Recarga por `SIGHUP`

Recarrega **só** o que é seguro trocar a quente e não muda os bytes do JLH:

- `log.level`, `log.access`, `log.access_sample`
- `filter.min_severity`, `filter.facilities`
- `ratelimit.*` (inclusive `enabled`)
- `sink.sqlite.keep_files`

Toda chave de `[syslog]` **exige restart**: ela muda a conversão, e a conversão
precisa ser determinística entre o item gravado e o item drenado (§3.5). A
identidade fica congelada.

Duas referências: chave recarregável comparada com o que está **em execução**
(para que reverter ao valor de boot seja aplicado); chave de restart comparada
com o **boot** (para que todo `SIGHUP` avise sobre ela até o restart). Cada uma
das duas sozinha falha num dos casos, como a standard aprendeu: comparada só
com o boot, a recarregável revertida ao valor de boot parece "inalterada" e não
é aplicada; comparada só com o que está em execução, a de restart é avisada no
primeiro `SIGHUP` e nunca mais.

**Chave sombreada.** Como env e flag vencem o arquivo (§16.1) e não mudam num
processo em execução, uma chave que o arquivo define mas um env ou uma flag
decide **nunca** muda pelo arquivo: a recarregável não recarrega, e a de restart
não muda nem com o restart. Todo `SIGHUP` loga cada uma em
`WARN`, com a origem que manda (`env RELPLOG_FILTER_MIN_SEVERITY`), mesmo quando o
resultado é "configuration unchanged": é exatamente o caso em que o operador
editou o arquivo e não entende por que nada mudou. Por isso os arquivos Compose
não definem chaves recarregáveis por env: elas vêm do INI montado.

Fora da janela entre o fim do boot e o início do shutdown, o `SIGHUP` é logado e
ignorado (§13.4).

### 16.4 Exemplo

```ini
[server]
data_dir = /data

[listen]
tcp = :20514
tls = :20515

[relp]
window               = 128
inflight_max_bytes   = 64MiB
backpressure_timeout = 30s

[tls]
cert_file   = /etc/relplog/tls/server.crt
key_file    = /etc/relplog/tls/server.key
client_auth = optional
client_ca   = /etc/relplog/tls/clients-ca.crt

[syslog]
rfc3164_timezone  = America/Sao_Paulo
max_message_bytes = 64KiB

[filter]
min_severity = info

[ring]
mode           = memory
capacity_items = 65536
max_bytes      = 256MiB

[sink]
type = nats

[sink.nats]
urls   = nats://nats-1:4222,nats://nats-2:4222
stream = LOGHUB_SYSLOG

[admin]
bind = 0.0.0.0:9103
```

### 16.5 Tabela de parâmetros

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `ring.sqlite.commit_max_delay`,
`ring.sqlite.busy_timeout`, `ring.redis.claim_idle`, `sink.sqlite.busy_timeout`,
`ratelimit.sweep_interval`, `shutdown.drain_timeout`, `relp.backpressure_timeout`,
`tls.handshake_timeout` e `proxy.header_timeout` —
a validação recusa `0` com exit 1. Zero é legítimo onde diz o que faz:
`shutdown.pre_stop_delay` (sem espera), `sink.batch_max_delay` (sem espera),
`sink.fail_timeout` (sem fusível por tempo), `tcp.idle_timeout` e
`tls.idle_timeout` (sem limite), `loghub.beacon_interval` (sem beacon periódico).

#### `[server]`

| Chave | Padrão | Descrição |
|---|---|---|
| `data_dir` | `./data` (env `DATADIR`) | Raiz de tudo o que o serviço grava, inclusive a identidade; convertida em absoluto; `/data` no contêiner |

#### `[listen]` — vazio desliga o transporte

| Chave | Padrão | Descrição |
|---|---|---|
| `tcp` | `:20514` | RELP em claro (§4.4) |
| `tcp_listeners` | `1` | `>1` habilita `SO_REUSEPORT` |
| `tls` | `""` | RELP sobre TLS (§4.5); `:20515` é a sugestão |
| `tls_listeners` | `1` | |

#### `[relp]` — o protocolo, para os dois transportes

| Chave | Padrão | Descrição |
|---|---|---|
| `window` | `128` | transações em voo por conexão antes de o leitor parar de ler (§5.5); 1–65536 |
| `max_frame_bytes` | `16MiB` | fusível do `DATALEN`; acima dele a conexão fecha antes de ler o corpo (§5.2); ≥ `syslog.max_message_bytes` |
| `inflight_max_bytes` | `64MiB` | teto **do processo** para as cópias das transações em voo, fora de `ring.max_bytes` (§5.6); ≥ `syslog.max_message_bytes` |
| `backpressure_timeout` | `30s` | quanto uma transação é segurada com ring cheio ou sink DOWN antes de a sessão fechar **sem** resposta (§5.7); zero é recusado; mantenha abaixo do `timeout` do `omrelp` (90 s) |

#### `[tcp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `idle_timeout` | `120s` | sessão sem frame por esse tempo fecha com `serverclose`; `0` = nunca |
| `max_conns` | `0` | por transporte; bloqueia no accept |
| `max_conns_per_ip` | `0` | por transporte, pelo endereço exato do par; fecha na hora |

#### `[tls]`

| Chave | Padrão | Descrição |
|---|---|---|
| `cert_file`, `key_file` | `""` | obrigatórios com `listen.tls` |
| `client_auth` | `optional` | `optional` \| `require` \| `none` (§4.5) |
| `client_ca` | `""` | CA dos clientes; obrigatória com `require` |
| `min_version` | `1.2` | `1.2` \| `1.3` |
| `handshake_timeout` | `10s` | |
| `idle_timeout`, `max_conns`, `max_conns_per_ip` | como `[tcp]` | limitador próprio |

#### `[proxy]`

| Chave | Padrão | Descrição |
|---|---|---|
| `protocol` | `off` | `off` \| `optional` \| `required` (§4.6) |
| `trusted` | `127.0.0.1/32,::1/128` | CIDRs dos balanceadores |
| `header_timeout` | `5s` | |

#### `[syslog]` — exige restart (§16.3)

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `default_priority` | `SYSLOG_DEFAULT_PRIORITY` | `13` | PRI de `pri-missing`/`pri-invalid` |
| `rfc3164_timezone` | `SYSLOG_RFC3164_TIMEZONE` | `UTC` | fuso IANA dos timestamps sem fuso |
| `max_message_bytes` | `SYSLOG_MAX_MESSAGE_BYTES` | `64KiB` | truncamento (§5.3); 480 a 16 MiB; também o tamanho máximo da cópia de uma transação em voo |
| `payload_raw` | `SYSLOG_PAYLOAD_RAW` | `true` | `raw` quando não é obrigatório |
| `legacy_charset` | `SYSLOG_LEGACY_CHARSET` | `""` | `cp1252` \| `latin1`, sinônimos `windows-1252` \| `iso-8859-1` (§6.7) |
| `accept_malformed` | `SYSLOG_ACCEPT_MALFORMED` | `true` | `false` descarta `malformed` no worker |
| `sd_max_elements` | `SYSLOG_SD_MAX_ELEMENTS` | `32` | §6.11 |
| `sd_max_params` | `SYSLOG_SD_MAX_PARAMS` | `64` | §6.11 |

#### `[filter]` — recarregável

| Chave | Padrão | Descrição |
|---|---|---|
| `min_severity` | `7` | aceita `severity <= isto`; nome ou número |
| `facilities` | `""` | nomes RFC 5427 ou números; vazio aceita todas |

#### `[ratelimit]` — recarregável

| Chave | Padrão |
|---|---|
| `enabled` | `false` |
| `rate` | `1000` |
| `burst` | `2000` |
| `v4_mask` | `24` |
| `v6_mask` | `48` |
| `exempt` | `""` |
| `max_buckets` | `1000000` |
| `sweep_interval` | `30s` |

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `mode` | `memory` | `memory` \| `sqlite` \| `redis` \| `none` |
| `capacity_items` | `65536` | global com `redis` |
| `max_bytes` | `512MiB` | bytes do cru; global com `redis` |
| `high_water` | `0.80` | `/readyz` falha acima |
| `max_inflight` | `2048` | escritas síncronas com `none` |

#### `[ring.sqlite]`

| Chave | Padrão |
|---|---|
| `path` | `<data_dir>/ring.db` |
| `synchronous` | `FULL` (`FULL` \| `NORMAL` \| `OFF`) |
| `commit_batch` | `256` |
| `commit_max_delay` | `5ms` |
| `busy_timeout` | `5s` |
| `wal_autocheckpoint` | `1000` |

#### `[ring.redis]`

| Chave | Padrão | Descrição |
|---|---|---|
| `addrs` | `127.0.0.1:6379` | ou do Sentinel com `sentinel_master` |
| `username`, `password`, `db`, `sentinel_master` | `""`, `""`, `0`, `""` | senha redigida no dump |
| `tls`, `tls_ca`, `tls_insecure` | `false`, `""`, `false` | |
| `stream` | `relplog:ring` | |
| `group` | `relplog-ring` | |
| `claim_idle` | `30s` | ociosidade para `XAUTOCLAIM` |

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `type` | `nats` | `nats` \| `redis` \| `sqlite` \| `postgres` \| `mysql` \| `clickhouse` \| `none` |
| `workers` | `8` | 1 com `sqlite`; 4 com `postgres`/`mysql`/`clickhouse` no padrão |
| `batch` | `256` | itens por `Dequeue` |
| `batch_max_bytes` | `16MiB` | teto de JLH por pedaço |
| `batch_max_delay` | `5ms` | |
| `op_timeout` | `5s` | prazo de uma escrita |
| `health_interval` | `2s` | |
| `health_timeout` | `1s` | |
| `fail_timeout` | `30s` | DOWN antes de FATAL |
| `max_retries` | `10` | checagens falhas antes de FATAL |
| `boot_timeout` | `300s` | |
| `retry_backoff_min`, `retry_backoff_max` | `100ms`, `2s` | |

#### `[sink.nats]`

| Chave | Padrão | Descrição |
|---|---|---|
| `urls` | `nats://127.0.0.1:4222` | lista separada por vírgula |
| `username`, `password`, `token`, `credentials` | `""` | um método só; segredos redigidos |
| `tls`, `tls_ca`, `tls_insecure` | `false`, `""`, `false` | |
| `mode` | `jetstream` | `jetstream` \| `core` |
| `stream` | `LOGHUB_SYSLOG` | validado, nunca criado; ignorado em `core` |
| `subjects` | `exact` | `exact` \| `facility-severity` |

#### `[sink.redis]`

| Chave | Padrão |
|---|---|
| `addrs`, `username`, `password`, `db`, `sentinel_master`, `tls`, `tls_ca`, `tls_insecure` | como `[ring.redis]` |
| `stream` | `loghub.{space}.syslog` |
| `maxlen` | `0` |
| `ring_scale_each_conn`, `pipeline_multiplex`, `read_buffer_each_conn`, `write_buffer_each_conn` | `10`, `0`, `512KiB`, `512KiB` |

#### `[sink.sqlite]`

| Chave | Padrão |
|---|---|
| `path` | `<data_dir>/events.db` |
| `rotate` | `none` (`none` \| `hourly` \| `daily`) |
| `keep_files` | `0` (recarregável) |
| `synchronous`, `busy_timeout`, `wal_autocheckpoint` | `FULL`, `5s`, `1000` |

#### `[sink.postgres]` e `[sink.mysql]`

| Chave | Padrão |
|---|---|
| `addr` | `127.0.0.1:5432` / `127.0.0.1:3306` (ou socket unix começando com `/`) |
| `username`, `password`, `database` | `syslog`, `""`, `syslog` |
| `table` | `loghub.inbox` no PostgreSQL (§3.7.1), `syslog_events` no MySQL (`nome` ou `esquema.nome`; nunca criada) |
| `pool_size` | `0` (deriva `workers + 1`) |
| `tls`, `tls_ca`, `tls_insecure` | `false`, `""`, `false` |

#### `[sink.clickhouse]`

| Chave | Padrão |
|---|---|
| `addrs` | `127.0.0.1:9000` (lista, protocolo nativo; 9440 com TLS) |
| `username`, `password`, `database` | `syslog`, `""`, `syslog` |
| `table` | `inbox_syslog` (§3.7.2; `nome` ou `banco.nome`, caixa preservada; nunca criada) |
| `pool_size` | `0` (deriva `workers + 1`) |
| `tls`, `tls_ca`, `tls_insecure` | `false`, `""`, `false` |

#### `[loghub]`

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `eb_url` | `LOGHUB_EB_URL` | `""` | §10.9 |
| `beacon_interval` | `LOGHUB_BEACON_INTERVAL` | `15` (s) | `0` desliga o periódico |
| `ctl_namespace` | `LOGHUB_CTL_NAMESPACE` | `loghub._ctl` | |

#### `[emergency]`, `[admin]`, `[log]`, `[shutdown]`

| Chave | Padrão |
|---|---|
| `emergency.enabled` | `true` |
| `emergency.dir` | `<data_dir>/emergency` |
| `admin.enabled` | `true` |
| `admin.bind` | `127.0.0.1:9103` |
| `log.level` | `info` (recarregável) |
| `log.format` | `json` |
| `log.access`, `log.access_sample` | `false`, `0.01` (recarregáveis) |
| `shutdown.pre_stop_delay` | `0s` |
| `shutdown.drain_timeout` | `30s` |

### 16.6 Escrita em disco: uma raiz só

Tudo o que o serviço grava — a identidade (`agent_uuid`, `machine_id`, gravados
pelo `go-loghub-ident`), `ring.db`, `events.db` e `emergency/` — fica sob
`server.data_dir`, e no contêiner essa raiz é `/data` (os compose sobem com
`read_only: true`). Não há exceção: o servidor não tem socket local (§4.3).

Caminhos fixos do contêiner: `/app` guarda o binário (dono `root`), `/data` é a
única área gravável, `/etc/relplog` é o INI em `:ro` (os compose montam
`docker/conf/` ali e definem `RELPLOG_CONFIG=/etc/relplog/relplog.ini`, que traz as
chaves recarregáveis, §16.3), `/etc/relplog/tls` são os certificados
(`docker/tls/`, também `:ro`), `/tmp` é tmpfs. `WORKDIR
/data`; o binário é invocado por `/app/go-relplog-server`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

---

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-relplog-server/              ESTE módulo
  go-syslog-server/               o irmão (S): pai do código, nunca importado
  go-syslog-standard-server/      a standard: o RELP de referência (receiver/relp*.go)
  go-syslog-pg-server/, go-syslog-nats-server/, go-webhook-*/   irmãos
```

Módulo independente. Nada é importado de outro módulo do monorepo: compartilhar
pacotes faria os módulos evoluírem amarrados.

Os nomes `go-relplog-server` (binário, imagem Docker, `--version`, tags
`go-relplog-server/vX.Y.Z`) e o prefixo `rls-` dos projetos, contêineres e
volumes Compose são **deste** módulo. O S usa `sls-`: com a mesma imagem e o
mesmo nome de projeto, um `docker compose up` feito na pasta dele retaguearia a
imagem daqui e assumiria os contêineres e os volumes desta pilha, sem aviso.

**O que é cópia do S e o que é deste módulo.** `convert/`, `item/`, `jlh/`,
`jlhcheck/`, `ident/`, `filter/`, `ratelimit/`, `ring/`, `sink/`, `control/`,
`emergency/`, `rediscli/` e o grosso de `config/`, `obs/` e `app/` são do S, com
as pendências dele corrigidas (§22); `receiver/` é deste módulo (o RELP) sobre o
esqueleto de conexões do S (limitador, PROXY, TLS, ingest). Uma correção feita
num dos lados vale para o outro: confira o irmão.

### 17.2 Layout interno

```
cmd/go-relplog-server/main.go   flags que encerram, lhident.Initialize(), chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão
  config/     registry único -> INI + flags + env + --help + dump; validação; LOGHUB_EB_URL
  ident/      Identity (congelada no boot) e a gramática de space          [lógica pura]
  jlh/        escritor JSON por append, Event, cabeçalho do envelope, UUID [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH: pri, formato, rfc5424,
              rfc3164, sd, texto, charset, nomes, sid                       [lógica pura]
  item/       Item, codec do cabeçalho, pools de buffer                    [lógica pura]
  rediscli/   cliente rueidis e a tabela de classificação de erro do Redis,
              compartilhados por sink=redis e ring=redis (§10.3, §9.5)
  jlhcheck/   SÓ TESTE: os schemas e os vetores copiados das skills e o
              oráculo de conformidade (§18.2); nenhum arquivo de produção o importa
  receiver/   RELP: moldura (relpframe), sessão, janela, teto em voo e
              escritor de respostas (relp, budget), conexões e shutdown
              (stream), tcp, tls, proxy, limites, ingest
  filter/     facility e severidade                                         [lógica pura]
  ratelimit/  token bucket com shards e sweep                               [lógica pura]
  ring/       interface Ring + memory, sqlite, redis, none
  sink/       interface Sink + nats, redis, sqlite, postgres, mysql, none,
              workers, health, tls
  control/    heartbeat beacon
  emergency/  dump e reingestão
  obs/        métricas Prometheus próprias + slog
```

`convert/`, o leitor de frames RELP e o leitor do PROXY em `receiver/` são os
únicos parsers de entrada adversária. Os alvos de fuzz obrigatórios são esses e os decodificadores do que
volta do disco ou da rede interna (moldura, cabeçalho do item, dump): a lista
completa está em §18.3.

### 17.3 Dependências

**Oito em produção, e ponto.**

| Dependência | Para quê |
|---|---|
| `github.com/redis/rueidis` | `sink=redis`, `ring=redis` |
| `modernc.org/sqlite` | `ring=sqlite`, `sink=sqlite` (Go puro, sem CGO) |
| `github.com/jackc/pgx/v5` | `sink=postgres`, pela API nativa |
| `github.com/go-sql-driver/mysql` | `sink=mysql`, por `database/sql` |
| `github.com/nats-io/nats.go` | `sink=nats` e beacon, com o pacote `jetstream` |
| `github.com/ClickHouse/clickhouse-go/v2` | `sink=clickhouse`, pela API nativa e no protocolo nativo |
| `github.com/patrickbrandao/go-loghub-uuidv7` v0.0.3 | UUIDv7 nível 3 (substitui a `go-loghub-uuid` da standard) |
| `github.com/patrickbrandao/go-loghub-ident` v0.5.2 | identidade (sem dependência nova: usa a `go-loghub-uuidv7`) |

Só em teste: `github.com/santhosh-tekuri/jsonschema/v6` (contratos, §18.2).
`golang.org/x/net`, `golang.org/x/sys` e `golang.org/x/text` **não** são
importados por este código: `SO_REUSEPORT` e o charset legado são feitos com a
stdlib (`syscall`) e com tabela própria. O RELP não tem biblioteca: o protocolo
é pequeno, e o leitor é fuzzado contra um oráculo. O `golang.org/x/text` ainda aparece no binário, **transitivamente**,
pelo SASLprep (`secure/precis`) do `pgx`; nenhum pacote daqui o usa.

**`clickhouse-go`, a oitava** (2026-10-07): escolha do usuário contra a
recomendação da entrevista (HTTP da biblioteca padrão com `RowBinary`, sem
dependência nova); o registro do custo é o do `go-webhook-server` (§14.3 de lá):
uma dúzia de transitivas no binário (`ch-go`, `lz4`, `klauspost/compress`,
`brotli`, `orb`, `decimal`, `go-faster`, `segmentio/asm`, `xxhash`,
`google/uuid`, a API do `otel`), nenhuma abrindo socket, escrevendo em disco ou
lendo `TMPDIR` por conta própria, e ~4 MB a mais de binário. A posse de buffer
foi auditada no driver v2.48.0: o `Append` copia cada valor para a coluna
(§10.10.3); uma versão nova do driver pede a mesma auditoria.

**Gerador de UUID:** um `uuidv7.NewGenerator()` por processo, criado no boot e
compartilhado (é seguro para concorrência e usa ChaCha8 com semente de
`crypto/rand`; a preocupação com PCG da standard era da biblioteca antiga). Um
`init()` falha alto se `crypto/rand` não responder. O `uuid` não é segredo
(expõe o instante), e nada deriva capacidade dele.

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
  rejeitada. Código copiado da standard vem **com** os comentários.
- Citações da spec por número de seção (`§6.4.3`).
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
  quando `stream`), os metadados de recepção (inclusive o TLS do vetor, injetado
  no cabeçalho) e a config do vetor, e produz envelopes **iguais** aos esperados:
  todo campo igual, e o `uuid` igual pelo **instante** (ms, µs e ns do nível 3).
  **23 de 24** entram; `arquivo-sem-pri` fica fora porque `transport=file` está
  fora da v1 (§1.2), e o teste diz isso pelo nome.
- **Ordem de chaves:** o JLH serializado tem os nove campos e o payload na ordem
  de §3.1 e §3.2.
- **Schemas:** todo envelope produzido nos testes (vetores, corpus, fuzz,
  integração) passa nos dois schemas, com `santhosh-tekuri/jsonschema/v6` e
  `AssertFormat` ligado (os formatos `ipv4`/`ipv6`). Os exemplos `valid/` passam
  e os `invalid/` falham, para provar que o validador está ligado.
- **Regras semânticas** (a porta em Go do `validate_payload.py`):
  `priority = facility.code × 8 + severity.code`; nomes RFC 5427; `level =
  severity.code`; `sid` recalculado de `payload.host`; `event.iso` = `uts` + `sub`
  truncado; instante do `uuid` = `event` (tempo real); `size ≥` octetos de `raw`
  (maior só com `truncated`); `raw-encoding` coerente com `raw`.

**Por que não validar em execução** (decisão 14): o serializador é um só, é
determinístico e é exercido pelo fuzz com validação completa em cada saída; um
validador de schema no worker custaria dezenas de microssegundos por mensagem
(`BenchmarkSchemaValidate` registra o número em `docs/TESTING.md`) para pegar um
bug que o fuzz e os vetores já pegam antes do release. É o desvio da regra
"validar antes de publicar" do JLH, e fica registrado aqui.

### 18.3 Fuzz — obrigatório antes de release

| Alvo | Pacote | Propriedade |
|---|---|---|
| `FuzzConvert` | `convert` | nunca entra em pânico; **toda saída passa nos dois schemas e nas regras semânticas**; `message` é UTF-8 válido |
| `FuzzSD` | `convert` | o parser de STRUCTURED-DATA nunca entra em pânico e respeita os tetos |
| `FuzzFrame` | `receiver` | a moldura RELP contra um oráculo independente: os mesmos frames, os mesmos dados cortados em `max_message_bytes`, o mesmo `DATALEN`, o erro onde o oráculo para, sem pânico e sem guardar além do limite |
| `FuzzProxy` | `receiver` | v1 e v2: sem pânico, nunca lê além do cabeçalho declarado |
| `FuzzHead` | `item` | o decodificador do cabeçalho recusa sem pânico; ida e volta é identidade |
| `FuzzRead` | `emergency` | o dump tolera truncamento sem pânico |
| `FuzzJSONBSafe` | `sink` | a troca do `\u0000` para a `loghub.inbox` (§3.7.1) devolve JSON válido igual ao original com U+0000 → U+FFFD, e nada mais |
| `FuzzInboxPayload` | `sink` | o payload da coluna é JSON sem escape NUL, `raw` volta exato e os demais textos só trocam U+0000 por U+FFFD |

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$ -fuzztime=10m
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$    -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$ -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$ -fuzztime=10m
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$  -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$  -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$ -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=10m
```

### 18.4 Corpus de mensagens reais

`internal/convert/testdata/corpus/` guarda mensagens **capturadas de emissores
reais** (`rsyslog`, `syslog-ng`, `journald`, `logger(1)`, glibc, Cisco IOS,
MikroTik, pfSense, HAProxy, nginx, Kubernetes, Windows via NXLog, log4j): um
`.raw` com os bytes exatos e um `.want` com os campos esperados do payload. Vem da
standard, com os `.want` reescritos para o payload novo. **O corpus nunca
encolhe**: o teste falha abaixo de vinte entradas. Um caso novo de campo entra
aqui antes da correção do conversor.

### 18.5 Integração em processo

A pilha **real** (a mesma `run()` do binário) com `sink=sqlite` e `sink=none`, sem
servidor externo, por sessões RELP de um cliente de teste que fala como o
`librelp`: os dois transportes chegam ao destino (RELP em claro e sobre TLS, com
e sem certificado de cliente), PROXY v1 e v2, `200 OK` só depois da garantia,
ring cheio segurado e devolvido sem `500`,
fidelidade do JLH (o `raw` volta byte a byte, com NUL no meio), filtro e
`SIGHUP` (ida e volta), dump de emergência e reingestão exatos (mesmo `uuid`, mesmo
JLH), shutdown gracioso sem perda, segundo sinal (130), nenhum transporte
(exit 1), códigos 100–114 do `go-loghub-ident` num subprocesso, e a ordem do boot.

### 18.6 Destinos reais

Suítes que pulam sem a variável e criam o que precisam **a partir do arquivo que
o operador aplica** (`docker/postgres/inbox.sql`, `docker/mysql/schema.sql`,
`docker/nats/stream.json`, `docker/clickhouse/inbox_syslog.sql`):

```bash
RELPLOG_TEST_REDIS_ADDR=127.0.0.1:16379      go test -race -timeout 600s ./internal/sink/ ./internal/ring/ ./internal/app/
RELPLOG_TEST_PG_DSN='postgres://...'         go test -race -timeout 600s ./internal/sink/ ./internal/app/
RELPLOG_TEST_MYSQL_DSN='root:...@tcp(...)/x' go test -race -timeout 600s ./internal/sink/ ./internal/app/
RELPLOG_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race -timeout 600s ./internal/sink/ ./internal/control/ ./internal/app/
RELPLOG_TEST_CLICKHOUSE_DSN='clickhouse://...:19000/x' go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

O ClickHouse (25.8+, protocolo nativo) cria cada tabela de
`docker/clickhouse/inbox_syslog.sql`. A suíte prova a ida e volta pelo oráculo
(SD sem parâmetros, nomes com ponto, `PARAM-NAME` repetido, NUL, um JLH grande),
a regra de leitura `"structured-data":{}` = ausente, o token descartando o
retry e o merge colapsando o reagrupamento, uma tabela recusada no boot para
cada regra de §10.10.5 (a tabela dos webhooks inclusive), privilégio,
`readonly`, limite de memória, a tabela de §10.10.7 lida da própria SPEC, os
quatro rings ponta a ponta e o destino caindo e voltando sem perda.

O ring Redis tem suíte própria contra `RELPLOG_TEST_REDIS_ADDR`: capacidade
global entre duas instâncias, `XAUTOCLAIM` de uma instância morta, `Touch` que
impede o roubo de um lote vivo, contador de bytes exato depois de duplicatas.

Nenhuma ferramenta dos testes ou do Compose usa `latest`: o `nats-server` dos
testes é o v2.14.6, o `natsio/nats-box` do serviço `nats-init` (que cria o
stream a partir de `docker/nats/stream.json`) é o 0.20.0, e as imagens dos
destinos fixam a versão (`nats:2.11`, `redis:7.4`, `postgres:18`, `mysql:8.4`,
`clickhouse-server:25.8`). Com `latest`, o stream criado ou o
resultado de um teste mudariam sem mudança no repositório.

### 18.7 Emissores vivos

`RELPLOG_TEST_EMITTERS=1` sobe o **rsyslog real com `omrelp`** (Alpine, pacote
`rsyslog-relp`) em contêineres descartáveis contra o servidor em processo. É a
única prova de que a leitura do `librelp` que fundamenta §5.4 vale no fio:

| Cenário | Prova |
|---|---|
| RELP em claro, N mensagens | N confirmações, N linhas no destino, todas com `x-transport: "relp"` |
| RELP sobre TLS (`tls="on"`, com certificado de cliente) | idem, com `payload.tls` e o fingerprint do cliente |
| `500` para uma mensagem (rate-limit) | o rsyslog **não** a retransmite e loga "peer sent error response" |
| ring cheio além do `backpressure_timeout` | a sessão fecha sem `500`, o rsyslog reconecta e retransmite: **todas** as mensagens chegam |
| shutdown do servidor com transações em voo, e um servidor novo no mesmo `data_dir` (`ring=sqlite`) | reconciliação: enviadas = linhas no destino, sem perda e sem duplicata |

Resultados de 2026-10-11 (rsyslog 8.2410.0, librelp 1.11.0, Alpine 3.22): os
quatro cenários passam; `500`: a mensagem recusada chega ao destino uma vez e
nunca volta, 4–6 vizinhas duplicadas por `500`; backpressure: 20/20, 0
duplicatas, uma reconexão; reinício: 20/20, 0 duplicatas.

### 18.8 Critérios de aceite v1

**Nunca medidos ponta a ponta.** Alvo, não fato; a primeira medição substitui
esta tabela. A peça de recepção do primeiro cenário foi medida em 2026-10-12
(`BenchmarkRELPSession`: uma sessão sobre loopback, janela cheia, `200` depois
do ring em memória, sem sink): cerca de 486 mil msg/s num Apple M2
(`docs/TESTING.md` §5).

| Cenário | Alvo |
|---|---|
| RELP, `memory`+`nats`, uma conexão do rsyslog, janela 128, mensagens de 200 B | ≥ 80k msg/s (a confirmação custa) |
| RELP sobre TLS, `memory`+`nats` | ≥ 50k msg/s |
| `sqlite`+`nats`, RELP | ≥ 20k msg/s, p99 de confirmação < 20 ms (a janela amortiza o group commit) |
| Perda com `SIGTERM` durante carga | **zero**, reconciliada (§18.7) |
| Perda com FATAL do sink, `memory` | **zero**, via dump e reingestão |

---

## 19. Matriz de garantias

Leia junto com §2.3 e §5.4: o `200 OK` de cada transação RELP significa:

| `ring` | A confirmação significa | Perda em `kill -9` |
|---|---|---|
| `memory` | aceito na RAM deste nó | o backlog do ring (o dump cobre FATAL e `SIGTERM`, não `kill -9`) |
| `sqlite` | gravado com `fsync` no `ring.db` | nada |
| `redis` | `XADD` executado no Redis do ring | nada deste nó; o que o Redis perder pela política de `appendfsync` dele † |
| `none` + `nats` (`jetstream`) | `PubAck` do stream † | nada † |
| `none` + `nats` (`core`) | o servidor NATS recebeu | o que nenhum assinante pegou |
| `none` + `redis` | `XADD` confirmado | nada † |
| `none` + `sqlite` | `COMMIT` no `events.db` | nada |
| `none` + `postgres`/`mysql` | `INSERT` confirmado † | nada † |
| `none` + `clickhouse` | `INSERT` respondido † | nada † (com `wait_for_async_insert = 0`, o buffer do servidor) |
| qualquer + `none` | **descartado** | tudo: benchmark |

† A durabilidade depende de configuração fora do processo: `appendfsync` do
Redis, `synchronous_commit` do PostgreSQL, `innodb_flush_log_at_trx_commit` do
MySQL, `sync_interval`/`sync_always` e réplicas do stream NATS, `async_insert` e
`wait_for_async_insert` do perfil do usuário no ClickHouse (§10.10.6).

**Duplicatas (decisão 3).** O RELP não carrega id de mensagem: uma
retransmissão do rsyslog chega como mensagem nova, com `uuid` novo, e o JLH não
a deduplica por `(uuid, step)`. Ela acontece quando uma mensagem **já aceita**
não teve o `200 OK` processado pelo cliente: corte abrupto (`kill -9`, rede
caindo), escrita de respostas que falhou, uma transação decidida depois de a
sessão fechar, ou a sessão que o próprio `librelp` quebra depois de um `500`
(§5.4).
O shutdown gracioso e o fechamento por backpressure esvaziam as respostas
decididas antes de fechar, então nesses caminhos só duplica o que foi decidido
depois do fechamento. Uma chave derivada do conteúdo foi recusada: fundiria
mensagens legítimas idênticas (um `link down` repetido). É at-least-once; os
consumidores que precisam de exatidão deduplicam por conteúdo + janela de tempo,
sabendo o custo.

**Uma duplicata é do próprio rsyslog** (observada com tcpdump, rsyslog 8.2410 e
librelp 1.11, 2026-10-11): quando a ação `omrelp` volta de uma suspensão (o
servidor ainda não estava no ar no boot do emissor, por exemplo), o rsyslog
reenvia a mensagem que causou a suspensão, inclusive quando ela já tinha recebido
`200 OK` na mesma conexão. Uma por suspensão; nada se perde, e o servidor não tem
como distingui-la.

| Situação | O emissor (rsyslog) | Este servidor |
|---|---|---|
| `200 OK` entregue | apaga | é responsável pela entrega, pela matriz acima |
| `500` | **descarta e loga**; suspende a ação e retransmite a janela (vizinhas já aceitas viram duplicatas) | contou em `events_dropped_total{sink_logic}` (ou `unaddressable`) |
| sem resposta (sessão fechada) | retransmite na sessão seguinte | contou em `relp_unanswered_total` |

---

## 20. Invariantes

Se uma alteração conflita com um destes, ela está errada.

1. O `200 OK` nunca sai antes da garantia da combinação `ring` × `sink`.
2. Ring cheio, ring indisponível e sink DOWN nunca viram `500` nem descarte: a
   transação é segurada e, se não couber a tempo, devolvida ao emissor pelo fim
   da sessão. Um `500` só vai para uma recusa **final** e contada.
3. Um buffer volta ao pool **uma vez**, e só após a confirmação do sink. Num
   retry o item ainda é o dono; uma tentativa por prazo contamina o pedaço.
4. Item aceito só sai do ring após a confirmação do sink (ou um descarte
   deliberado e contado: `logic`, malformada recusada); nunca para liberar
   espaço.
5. **Toda mensagem aceita vira exatamente um JLH válido** pelos dois schemas, ou
   um descarte contado. A conversão nunca falha; o pior resultado é `malformed`.
6. **Sem perda e sem invenção:** o cru recebido (já sem moldura, já truncado) está
   em `raw` quando ele é obrigatório ou ligado; campo ausente é omitido, nunca
   adivinhado nem `null`.
7. Nenhum sink altera o JLH. As colunas promovidas são cópias saneadas; `env` é o
   JLH byte a byte.
8. Os endereços do payload são a ponta real do transporte, ou o cliente
   declarado por um PROXY **confiável**, e então o par real fica em `x-proxy`. O
   `hostname` é sempre alegação do emissor e nunca vira chave de rate-limit,
   particionamento ou decisão.
9. Nenhum label de métrica deriva de conteúdo controlado pelo emissor.
10. Nenhum log síncrono por mensagem no caminho padrão; nenhum payload em log de
    `INFO` ou acima.
11. Um comando do rueidis nunca é reusado após executar; um retry reconstrói.
12. O JLH é idêntico em todos os sinks.
13. **O JLH é função de cabeçalho + cru + config de conversão congelada** (§3.5):
    mesmo `uuid`, mesmos bytes, em retry, dump, reingestão e reivindicação.
14. **Uma amostra de relógio por mensagem**, e dela saem o `uuid` e o `event`.
15. `ring` e `sink` são ortogonais.
16. Toda escrita em disco fica sob `server.data_dir` (`/data` no contêiner),
    inclusive a identidade, **por padrão**: os caminhos derivados ficam lá, sem
    exceção fixa. Um `ring.sqlite.path`,
    `sink.sqlite.path` ou `emergency.dir` absoluto fora dele é decisão do
    operador (um disco mais rápido), aceita com `WARN` no boot (§13.2).
17. Nenhum destino compartilhado cria o que valida: `sink=postgres`,
    `sink=mysql` e `sink=clickhouse` nunca fazem DDL, e `sink=nats` nunca cria
    nem altera um stream.
18. Esquema e permissão são `fatal`, nunca `logic` (classe `42` do SQLSTATE,
    `1054`/`1146`/`1142` do MySQL, `WRONGTYPE`/`NOPERM` do Redis); erro
    desconhecido é `network`, nunca `logic`.
19. Nenhuma biblioteca cliente guarda nem retenta o que o ring guarda e o worker
    retenta: `WithRetryAttempts(0)` e `ReconnectBufSize(-1)` no `nats.go`.
20. Nenhum destino recebe uma mensagem que o teto dele recusaria: o boot compara
    `max_payload` e `max_allowed_packet` com o pior JLH, e o `max_memory_usage`
    do ClickHouse com o pior pedaço (§3.9, §10.10.5).
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
    `transport` `tcp` ou `tls`.

---

## 21. Roteiro pós-v1

Nada disto entra antes de a v1 estar em produção e §18.8 ter sido medido.

| Item | Motivo de ter ficado fora |
|---|---|
| `relp` no enum de `transport` da skill | Pedir à skill; até lá, `x-transport` (§3.2) |
| Comandos RELP de extensão | Nenhum cliente conhecido usa |
| `space` por listener ou rede | Multi-tenant numa instância; fora da v1 |
| Resposta a sonar, ping e pong | Até existir um `go-loghub-driver` |
| Troca de certificado TLS sem restart | Recarga de `tls.cert_file` por `SIGHUP` |
| Impressão digital da config de conversão no cabeçalho | Detectaria réplicas com config divergente no ring Redis (§3.5) |
| `sink=syslog` (encaminhar) | Política de retry própria e decisão sobre o `hostname` |
| Particionamento por tempo no PostgreSQL e no MySQL | Precisa de decisão sobre quem cria a partição |
| `sink=clickhouse` numa tabela `Distributed` | O `INSERT` vai aos shards de forma assíncrona; a confirmação exigiria decidir o que é "entregue" num cluster (§10.10.5) |
| Esquema `clickhouse://` na `LOGHUB_EB_URL` | Hoje o `sink=clickhouse` só se configura pelas chaves de `[sink.clickhouse]` (§10.9) |

---

## 22. Lacunas e riscos

Os riscos do desenho estão em `docs/DECISOES.md` §3, e o que os testes ainda não
provam, em `docs/TESTING.md` §10. Esta seção guarda o que o **código atual** ainda
faz diferente do que deveria. Cada item diz o problema, a consequência e a
direção da correção, que segue a regra de sempre: a spec primeiro (a regra entra
na seção indicada), depois o código e o teste. Corrigido um item, ele sai daqui.

**As 30 pendências do `go-syslog-server`** (§22.1 da SPEC dele, 2026-10-11) foram
corrigidas neste módulo na criação, e a regra de cada uma está na seção onde
vale, citando o número dela: 1 (§5.2), 2 (§4.4, decisão), 3 (§4.6), 4 (§6.5.1,
decisão), 5 (§9.5, §10.3), 6–9 (§10.5), 10–12 (§10.6, §13.2), 13–20 (§10.7),
21 (§11, §13.2), 22 (§10.9, §13.2), 23–26 (§12.1, §12.2), 27 (§13.1), 28–29
(§13.4), 30 (§16.2). O S continua com elas: é a lista "a portar" da entrada **R**
do `CHANGELOG.md` da raiz.

### 22.1 Pendências conhecidas

Nenhuma conhecida em 2026-10-12. A última encontrada na criação (uma senha com
`/`, `?` ou `#` sem escape lida como porta na `LOGHUB_EB_URL`) foi corrigida: um
`@` depois do primeiro `/`, `?` ou `#` da autoridade é recusado, sem eco, com a
dica de `%2F`, `%3F` e `%23` (§10.9, §16.2). Os achados da revisão independente
de 2026-10-11 estão corrigidos, e a regra de cada um entrou na seção onde vale
(§5.4, §5.6, §5.7, §5.8, §10.6, §10.7, §13.4, §16.2).
