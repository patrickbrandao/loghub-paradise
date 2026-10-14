# go-relplog-pg-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-13
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-relplog-pg-server`
Binário: `go-relplog-pg-server`
Entrada: `docs/DECISOES.md` (decisões de 2026-10-13, sem entrevista), que
aponta para as decisões herdadas do `go-relplog-server`
(`../go-relplog-server/docs/DECISOES.md`) e do `go-syslog-pg-server`
(`../go-syslog-pg-server/docs/DECISOES.md`).

**Origem.** Esta especificação foi derivada em 2026-10-13, **por corte**, da SPEC
do `go-relplog-server` (R, v1.0.0, 2026-10-12), com o molde do corte que o
`go-syslog-pg-server` (SP) fez no `go-syslog-server`. O pedido do usuário: um
servidor RELP simplificado, que faz ingestão **só no PostgreSQL**, com ring
**só na memória**, emergência em arquivo **JSON Lines**, **sem TLS** e sem os
demais recursos do pai (§1.2).

**A numeração das seções é a do `go-relplog-server`, de propósito.** O código
cita a spec por número (`§5.7`, `§12.2`, `§20.13`), e os dois servidores são da
mesma família: um número que mudasse de sentido entre eles enganaria quem lê os
dois. Uma seção removida **mantém o título**, seguido de uma nota de uma linha;
nada é renumerado. Onde o texto cita "o S" (o `go-syslog-server`) ou as
"pendências do S", é a herança que o R registrou e que vale aqui.

Servidor de ingestão de syslog por **RELP** do ecossistema **LogHub**. Recebe as
mensagens que o `rsyslog` envia com o `omrelp`, em claro, confirma **cada uma**
só depois de ela estar no ring, converte cada uma num envelope **JLH** com
`type=syslog` e o **enfileira** na `loghub.inbox` do PostgreSQL, a fila comum de
envelopes do LogHub. É um **Bridge-IN**: cria a primeira mensagem de cada fato
(`step = 0`).

Onde esta spec repete a do R é de propósito; onde diverge, a divergência é
justificada no lugar.

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
- **Dependências mínimas:** três em produção (§17.3), binário pequeno.
- **Sem perda e sem invenção** no payload: o que chegou está nos campos ou em
  `raw`; o que não chegou é omitido, nunca adivinhado (§6).
- Um destino só: a fila `loghub.inbox` de um PostgreSQL compartilhado entre
  instâncias e produtores (§3.7.1, §10.5).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| **RELP sobre TLS** (`listen.tls`), **mTLS** e o objeto `payload.tls` | Decisão do usuário no SP (2026-10-07, decisão 4), que vale aqui: o TLS sai **em todo lugar**. Quem precisa de RELP cifrado usa o `go-relplog-server` ou um terminador TLS na frente (com PROXY protocol, §4.6). O payload nunca traz `transport=tls` |
| **TLS até o PostgreSQL** | A mesma decisão. A conexão é sempre `sslmode=disable`: o `TLSConfig` do pgx é nulo, os *fallbacks* saem e o ambiente `PG*` é escondido durante o parse (§10.5). Uma `LOGHUB_EB_URL` com `sslmode=require`, `verify-ca` ou `verify-full` é recusada com exit 1 (§10.9). O banco é alcançado por rede confiável ou por socket unix |
| Outros sinks: `nats`, `redis`, `sqlite`, `mysql`, `clickhouse`, `none` | Pedido do usuário: ingestão **só** no PostgreSQL. Cada um trazia um cliente e a sua árvore de transitivas. Quem precisa deles usa o `go-relplog-server`, que é o mesmo servidor com os dois eixos abertos |
| Outros rings: `sqlite`, `redis`, `none` | Ring **só** na memória (molde do SP). A perda sob `kill -9` é a do ring em memória (§19); FATAL e `SIGTERM` são cobertos pelo dump (§12). Com o `ring=none` saiu o único caminho em que um destino recusava uma mensagem **dentro** da transação RELP, e com ele o `500 refused by the destination` (§5.4) |
| Filtro por facility/severidade | Molde do SP: sai. O descarte, se houver, é do consumidor da fila; a recepção nem lê o PRI (§2.2) |
| Rate-limit por prefixo de rede | Molde do SP: sai. A decisão 8a do R (o rate-limit que **segura** em vez de recusar) deixa de se aplicar. Proteção contra emissor ruidoso fica na rede, ou nos tetos `tcp.max_conns*`, na janela e em `relp.inflight_max_bytes` |
| Heartbeat beacon | Dependia do `sink=nats`, que saiu. Sonar, ping e pong também ficam de fora |
| `sink.type` e `ring.mode` como chaves | Um valor só cada um. O log de boot e o `relplog_config_info` continuam dizendo `ring_mode="memory"` e `sink_type="postgres"`, para que um painel do pai sirva aqui (§14.1) |
| UDP, unix datagram, syslog TCP da RFC 6587, TLS da RFC 5425 | Não confirmam por mensagem; são do `go-syslog-server` e do `go-syslog-pg-server` (decisão 12 do R) |
| `relp` no enum de `transport` | A skill não tem; o RELP vai como `x-transport` (decisão 2 do R, §3.2) |
| Deduplicação de retransmissão | O RELP não carrega id de mensagem (decisão 3 do R, §19) |
| DTLS, BEEP, importação de arquivo | Como no pai |
| `space` por listener ou rede | Um Workspace por instância na v1 |
| Validação por JSON Schema em execução | Só em teste e CI (§18.2) |
| Autenticação de emissor | Não existe no protocolo sem TLS: firewall ou rede privada |
| Roteamento por facility para destinos distintos | Um destino por instância |
| Comandos RELP além de `open`, `syslog` e `close` | O `librelp` só usa esses (§5.4) |

O porquê comum: **o pedido do usuário** (um RELP simplificado, só PostgreSQL),
**as dependências** (de oito em produção para três; o binário cai de 31,9 MB
para 17,2 MB, mesmo build `darwin/arm64` com as mesmas flags, §17.3) e **a
velocidade** (a recepção não lê a mensagem: nem PRI, nem filtro, nem balde de
rate-limit; §2.2).

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

Não é desvio, mas fica registrado: o único vetor dourado com `transport=tls`
roda **como TCP** (§18.2), porque não há TLS aqui (o molde do SP).

## 2. Arquitetura

### 2.1 Dois eixos ortogonais

No `go-relplog-server` são dois eixos, ring × sink, com vinte e oito
combinações. Aqui cada eixo tem **um valor só**:

- **RING**: `memory` (§9.3).
- **SINK**: `postgres`, a fila `loghub.inbox` (§10.5).

As chaves `ring.mode` e `sink.type` não existem (§1.2). A separação continua no
código: **o ring não consulta o sink, e vice-versa** (invariante §20.15), e as
interfaces `ring.Ring` e `sink.Sink` ficam, porque são a costura pela qual
recepção, workers e saúde são testados.

```
   rsyslog (omrelp), qualquer cliente librelp
      |
      |  RELP :20514 (em claro)
      v
 +-----------------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                          |
 |   [PROXY] -> sessão RELP: frame -> truncamento -> amostra de relógio   |
 |   -> janela (relp.window) + teto em voo (relp.inflight_max_bytes)     |
 |   -> por transação: sink DOWN? -> ring cheio? -> uuid -> Item          |
 |   -> ring -> 200 OK (escritor de respostas da conexão)                 |
 +----------------------------------+------------------------------------+
                                    |
                     +--------------v---------------+
                     | RING memory: canal + 2 tetos |
                     +--------------+---------------+
                                    | sink.workers (4)
                     +--------------v---------------+
                     | CONVERSÃO -> JLH type=syslog |  <- fora do caminho quente
                     +--------------+---------------+
                                    |
                     +--------------v---------------+
                     | SINK postgres                |
                     |  INSERT ... unnest ...       |
                     |  ON CONFLICT (uuid)          |
                     +--------------+---------------+
                                    |
                         loghub.inbox (PostgreSQL 18)

   emergência: ring (ou lote retido) não vazio + sink FATAL ou dreno estourado
               => <data_dir>/emergency/emg-<agent_uuid>-<unixnano>.jsonl (§12)

 +-----------------------------------------------------------------------+
 | porta admin (admin.bind, padrão 127.0.0.1:9103)                        |
 |   /healthz   /readyz   /metrics                                        |
 +-----------------------------------------------------------------------+
```

O ring é **único e global**: uma métrica única de profundidade, backpressure
justo (uma transação só é segurada quando o sistema inteiro está saturado) e
código simples. O teto de cópias em voo (§5.6) é igualmente único.

### 2.2 Onde a conversão acontece, e por quê

Decisão 7 do S. A recepção faz o mínimo que permite decidir se a mensagem entra;
a conversão completa roda no **worker de dreno**.

| Etapa | Onde roda | Custo |
|---|---|---|
| PROXY, `open` | recepção, uma vez por conexão | do transporte |
| Moldura RELP, truncamento | recepção (leitor da conexão) | inevitável |
| Amostra de relógio | recepção (leitor), no fim do frame | uma leitura de relógio |
| Vaga na janela e no teto, cópia | recepção (leitor) | uma cópia do pool |
| sink DOWN, ring cheio, `uuid` | recepção (goroutine da transação) | dois testes, um UUID |
| PRI, RFC 3164 / RFC 5424, SD, UTF-8 | **worker de dreno** | proporcional à linha |
| Serialização do JLH | **worker de dreno** | proporcional aos campos |

A ordem da transação é estrita, e a primeira condição que falha decide:
vazia (§5.4) → truncamento (§5.3) → origem não endereçável (guarda, §4.2) →
sink DOWN (§13.3) → **ring cheio** → `uuid`, cabeçalho e cópia para um buffer
do pool → fim da sessão → `ring.Enqueue`. O "ring cheio" é uma pré-checagem
barata e sem trava, feita **antes** do `uuid`, da cópia e do item, como no SP
(revisão C dele): uma transação segurada volta a cada 50 ms (§5.7), e a recusa
custaria tanto quanto uma aceitação. Ela confere **os dois tetos**
(`ring.Memory.FullFor(n)`: itens ≥ `capacity_items`, ou bytes + a classe do
pool de `n` + `ItemOverhead` > `max_bytes`), com `n` o tamanho **já truncado**,
o que a cópia ocuparia: só os itens, a pré-checagem herdada do SP, deixava uma
transação segurada por `ring.max_bytes` cunhar `uuid`, pegar buffer, copiar e
montar o item a cada 50 ms, para o `Enqueue` recusar (revisão independente,
2026-10-13). Um ring fechado nunca é "cheio": o `Enqueue` diz `ErrClosed`, que
é o shutdown. Ela pode errar nos dois sentidos numa corrida; **o `Enqueue`
continua sendo a autoridade** (os dois tetos, §9.2).

No R o PRI era lido na recepção, porque o filtro dependia dele. Sem filtro
(§1.2), **a recepção não lê a mensagem**: nenhum byte do conteúdo é
interpretado antes do ring. A CPU da conversão é paga pelos `sink.workers`, que
é onde a vazão é elástica, e a confirmação RELP não espera por ela.

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
| ring cheio, sink DOWN | nenhuma enquanto dura; estourado o prazo, a sessão fecha | segura; retransmite na sessão seguinte |
| vazia | `200 OK` (nada a guardar, contada) | apaga |
| origem não endereçável (guarda, §4.2) | `500 source address unusable` | **descarta e loga o erro** |

**Diferente do R, nenhuma mensagem de um emissor real recebe `500` aqui**: a
recusa final do destino só existia com `ring=none` (que saiu, §1.2), e a guarda
de origem não dispara num par TCP. Com o ring em memória, uma recusa `logic` da
fila acontece no worker, **depois** do `200 OK`, e é contada como
`events_dropped_total{reason="sink_logic"}` (§10.2). Nada é descartado sem que
o emissor seja avisado ou recupere a mensagem, salvo esse descarte contado de
uma mensagem já confirmada.

### 2.4 Papel no LogHub

| Aspecto | Valor |
|---|---|
| Papel | **Bridge-IN**: traz dados de fora e publica como envelopes |
| Operação | **Produce** (primeira inserção): `uuid` novo, `step = 0`, `from` = AgentUUID |
| Destino | a fila `loghub.inbox` (§3.7.1); nenhum subject de eventbus. Um consumidor da fila não distingue a origem de um `go-syslog-pg-server`, a não ser por `payload.x-transport` |
| Plano de controle | nenhum: o beacon saiu (§11); nunca emite `ping`, `pong` ou sonar |
| Consumo | nenhum: quem retira da fila é outro software |

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
| `uuid` | UUIDv7 **nível 3** (`go-loghub-uuidv7`), em minúsculas, gerado **na recepção** com `GenerateAt(Level3, t)`, onde `t` é a amostra de relógio da mensagem. É cunhado depois da conferência do ring cheio e do estado do sink (uma mensagem segurada ou descartada não gasta `uuid`; §5.7), mas sempre da amostra `t` do fim do frame. Gravado no cabeçalho do item e nunca regerado: a retentativa do laço de backpressure da transação RELP (§5.7), o retry do worker e o dump reusam o mesmo, e a reingestão entrega a linha como está; uma tentativa de destino incerto que tiver sido aceita deduplica na fila por `ON CONFLICT (uuid)`. O nível 3 é fixo; `LOGHUB_UUID_LEVEL` não existe (D4). |
| `from` | AgentUUID de **quem recebeu** a mensagem (`lhident.AgentUUID()`), copiado para o cabeçalho do item. Um dump reingerido por outra instância mantém o `from` de quem recebeu (§12.2). Nunca um identificador do emissor remoto: a origem externa está em `payload.host`. |
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
| `transport` | `tcp` (RELP em claro; não há TLS, §1.2) | sempre |
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

`transport` nunca é `udp`, `unix`, `tls`, `dtls`, `file` nem `beep`, e o objeto
`tls` nunca aparece; `format` nunca é `rfc3195` (§1.2). As extensões emitidas são `x-proxy` e `x-transport`.

**Por que `x-transport` e não `transport: "relp"`** (decisão 2 do R): o enum de
`transport` da skill não tem `relp`, e o schema tem `additionalProperties: false`
com `^x-[a-z0-9][a-z0-9.-]*$` para extensões locais. `transport` diz o que o
socket é (`tcp` aqui, `tcp` ou `tls` no pai, o que mantém o `sid` recalculável
pela fórmula da skill e as combinações do schema);
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

O mesmo serializador escreve cada linha do dump de emergência (§12.1).

### 3.4 Cabeçalho do item

O que a conversão **não consegue** recuperar da linha crua e que, portanto,
viaja ao lado dela pelo ring:

| Campo | Conteúdo |
|---|---|
| `UUID` | §3.1 |
| `From`, `Space` | identidade de quem recebeu |
| `UTS`, `Sub` | a amostra de relógio |
| `Transport`, `Framing` | §3.2 (`tcp`, `octet-counting`) |
| `SrcIP`, `SrcPort`, `DstIP`, `DstPort` | endereços **canônicos** (§4.2), já resolvidos pelo PROXY quando houver |
| `PeerIP`, `PeerPort` | o par TCP real, **só** quando o PROXY protocol foi usado |
| `Size` | o `DATALEN` original; `Size > len(cru)` significa truncamento (§5.3) |

O PRI **não** está no cabeçalho: ele é derivável do cru, e a conversão o lê
(§6.2). Guardá-lo seria criar duas fontes da mesma verdade.

**O cabeçalho só existe em memória** (molde do SP). O codec JSON do R
(`item/codec.go`) servia à coluna `head` do `ring.db`, ao campo `h` do stream do
ring Redis e ao dump `.bin`; os três saíram (o dump guarda JLH pronto, §12.1), e
o codec saiu com eles, junto do alvo de fuzz `FuzzHead` (§18.3). A struct
`item.Item` perdeu os campos dos rings que saíram, e o `ItemOverhead` caiu de
384 para 256 B (§9.2). O tipo `item.TLSInfo` e o campo `TLS` do cabeçalho
saíram com o TLS. O conversor continua aceitando todo transporte e moldura da
skill (os vetores dourados de UDP e unix rodam, §18.2); a recepção deste
servidor só produz `tcp` com `octet-counting`.

### 3.5 Determinismo

> **Invariante:** o JLH é função pura de `(cabeçalho, cru, config de conversão)`.
> Dois processos com a mesma config que leiam o mesmo par produzem o mesmo JLH
> byte a byte.

É o que faz um retry reenviar os mesmos bytes com o mesmo `uuid`, como o outbox
do JLH exige, e o que torna o dump de emergência (§12.1) idêntico ao que o
worker teria escrito.

A **config de conversão** é o conjunto `[syslog]` (§16.5): `default_priority`,
`rfc3164_timezone`, `payload_raw`, `legacy_charset`, `accept_malformed`,
`sd_max_elements` e `sd_max_params`, mais a extensão `x-transport` (constante,
`app.XTransport`). Ela é **congelada no boot** (§16.3). `max_message_bytes` atua
na recepção, mas também exige restart.

A reingestão **não converte** (§12.2): a linha do dump vai como está, e por isso
um dump sobrevive a uma troca de chaves `[syslog]` (ou de versão do binário)
entre duas execuções. A consequência do R sobre réplicas com ring Redis não
existe aqui (não há ring compartilhado).

### 3.6 Entrada do stream Redis (`sink=redis`)

Removido no go-relplog-pg-server (§1.2).

### 3.7 Destino: a fila `loghub.inbox`

O único destino é a fila de §3.7.1. A tabela de eventos `syslog_events` do
SQLite e do MySQL saiu com esses sinks (§1.2), e com ela as colunas promovidas
do conversor (`hostname`, `appname`, `procid`, `msgid`, `src_ip`): só elas as
usavam.

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
  bytes. Quem precisa dos bytes do JLH usa o `go-relplog-server` com NATS,
  Redis, SQLite ou MySQL, ou o dump de emergência (§12.1).
- **NUL.** O `jsonb` recusa o escape `\u0000` (`22P05`), e a classe `22`
  descartaria o pedaço inteiro (§10.2); um `message` ou um `raw` com NUL produz
  esse escape (§3.3). Só nesse caso raro a coluna recebe uma **cópia
  reconstruída** do payload: `raw` vai em base64 com `raw-encoding=base64` (a
  forma que o schema do payload já tem para texto que não viaja como está), então
  continua exato e `size` continua contando os octetos dele; em todos os outros
  textos (`message`, `hostname`, valores de SD...) o NUL vira U+FFFD, a mesma
  troca que o R faz nas colunas de texto do SQLite e do MySQL. A reconstrução passa por um mapa, o que não
  perde nada que o `jsonb` guardaria (ele não guarda ordem de chaves). Sem NUL —
  quase sempre — a coluna recebe o sub-slice do JLH, sem cópia. O JLH do pedaço e
  o do dump continuam íntegros (§20.7). O envelope reconstruído da
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

Removido no go-relplog-pg-server (§1.2).

### 3.8 Mensagem NATS (`sink=nats`)

Removido no go-relplog-pg-server (§1.2).

### 3.9 O maior evento, e por que ele é calculado e não configurado

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
- **8 KiB fixos**: chaves, números, `host`, `x-proxy`, `x-transport`, `parse` e
  o cabeçalho do envelope.

Com o padrão de 64 KiB, o pior JLH é **776 KiB**. O R compara esse número com o
`max_payload` do NATS e o `max_allowed_packet` do MySQL; aqui o único teto por
valor é o do `jsonb` (255 MiB), e o teto de `syslog.max_message_bytes` (16 MiB,
§13.2) existe para o pior JLH, ~192 MiB, ainda caber nele. O número também é o
de `emergency.MaxLine` (12 × 16 MiB + 8 KiB, §12.2) e o de
`config.MaxEventBytes`, que o boot loga ("sink configured").

## 4. Transportes

Um transporte, RELP, ligado por `listen.tcp`, que, vazio, o desliga. **Sem ele
é erro de configuração** (exit 1): não há outro.

| Transporte | Parâmetro | Padrão | O que o emissor configura | `payload.transport` / `framing` |
|---|---|---|---|---|
| RELP em claro | `listen.tcp` | `:20514` | `omrelp` sem `tls` | `tcp` / `octet-counting` |

UDP, unix datagram e as molduras da RFC 6587 **não existem** aqui (decisão 12
do R): o RELP só existe sobre fluxo, e quem precisa daqueles usa o
`go-syslog-pg-server`. O RELP sobre TLS do R (`listen.tls`) saiu com o TLS
(§1.2).

### 4.1 As portas padrão

A imagem roda como uid 65532 e não abre porta abaixo de 1024. `:20514` é o
costume dos exemplos de RELP do rsyslog e já é alta. O log de boot imprime o
endereço efetivo do listener.

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

Uma conexão é **uma sessão RELP** (§5). É o único transporte.

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

Removido no go-relplog-pg-server (§1.2). Sem `listen.tls`, sem `[tls]`, sem mTLS e sem `payload.tls`.
O RELP cifrado do `omrelp` (`tls="on"`) não conversa com este servidor; um
terminador TLS na frente (com PROXY protocol, §4.6) ou o `go-relplog-server`.

### 4.6 PROXY protocol

Opt-in, para quando um balanceador (HAProxy, NLB, Traefik) ou um terminador
TLS está na frente e o `RemoteAddr` seria o dele.

- `proxy.protocol`:

  | Valor | Comportamento |
  |---|---|
  | `off` (padrão) | nada é lido; um cabeçalho PROXY é um frame RELP malformado e fecha a conexão |
  | `optional` | de par **confiável**, lê o cabeçalho se ele vier e segue sem ele se não vier; de par não confiável, nada é lido |
  | `required` | toda conexão precisa vir de par confiável **com** cabeçalho válido; senão fecha (`relplog_conns_refused_total{reason="proxy"}`) |

- Par confiável: `proxy.trusted` (CIDRs, padrão `127.0.0.1/32,::1/128`), testado
  contra o par TCP real.
- v1 (texto, `PROXY TCP4|TCP6|UNKNOWN ...\r\n`, até 107 bytes) e v2 (binário,
  assinatura de 12 bytes). O cabeçalho chega **antes** da sessão RELP. Prazo
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
  falha do balanceador (pendência 3 do S, corrigida no R; no SP ainda aberta). `LOCAL`, `UNSPEC`, `UNIX`,
  `DGRAM` e `UNKNOWN` contam como `local`.
- **Efeito no payload:** `host.src-*` e `host.dst-*` vêm do cabeçalho (o
  cliente e o endereço que ele discou), o `sid` é calculado com eles, e o par TCP
  real vai em **`x-proxy: {"peer-ip", "peer-port"}`**, para que a ponta real do
  transporte continue registrada (§20.8).
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
- **Custo fixo por conexão** (decisão 11 do R, pendência 1 do S): o
  `bufio.Reader` tem 4 KiB (o leitor só usa `ReadByte` e leituras contadas; o
  maior `Peek` é o do PROXY, de 12 bytes) e o escritor de respostas, outros
  4 KiB; depois de um
  frame grande, o leitor descarta o buffer acima de 16 KiB em vez de segurá-lo
  enquanto a conexão viver ociosa — mas só quando nada do frame seguinte está
  no `bufio.Reader` (o emissor parou e a leitura seguinte pode esperar o
  `idle_timeout`). Descartado antes de **todo** frame, um emissor de linhas de
  24 KiB refazia o crescimento inteiro por frame (2+4+8+16+24 KiB e as cópias:
  55 KB e 5 alocações por frame, 12 µs contra 0,5 µs) para um buffer que voltaria a
  crescer (revisão independente do `go-syslog-server`, 2026-10-13, E2;
  `BenchmarkRELPReaderFrames`, TESTING §5). O buffer cresce dobrando a partir
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
servidor 1 rsp <n> 200 OK\nrelp_version=0\nrelp_software=go-relplog-pg-server,<versão>\ncommands=syslog

cliente  2 syslog 34 <13>Sep 12 14:33:01 web-01 app: oi
servidor 2 rsp 6 200 OK

cliente  3 close 0
servidor 3 rsp 0
         (o servidor fecha a conexão)
```

- **`open`**: a resposta é `200 OK` seguida das ofertas no formato do
  `relpOffersToString` do `librelp` (uma por linha, sem `\n` final):
  `relp_version=0`, `relp_software=go-relplog-pg-server,<versão>` (para um operador
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
| `200 ...` | remove a mensagem da lista de não confirmadas: entregue | o que a garantia do ring cumpriu, e a mensagem vazia (que não tem nada a guardar) |
| qualquer outro código (`500 ...`) | **destrói** a mensagem (`relpSendbufDestruct`), loga "peer sent error response" pelo `onErr` e **nunca a retransmite**; e mais: o erro devolvido pelo tratamento da resposta (`librelp error 10020`) **suspende a ação e quebra a sessão**, e as transações da janela já enviadas e ainda sem resposta são **retransmitidas** na sessão seguinte | só o que este servidor recusou de propósito, para sempre, e raramente |
| nenhuma (sessão quebrada: conexão fechada ou `serverclose`) | reconecta e **retransmite** tudo o que não teve `200` (`relpSessTryReestablish`) | o que precisa voltar ao emissor |

A especificação extraída dizia "`500 ring full`: o rsyslog retém e retenta". O
código do cliente diz o contrário: um `500` por ring cheio era uma perda **na
origem** a cada ring cheio (decisão 4 do R). E o rsyslog real mostrou o resto
(`TestEmittersRELPFiveHundredIsFinal` do R, rsyslog 8.2410, 2026-10-11; o
cenário não existe aqui, sem o `500` final): a mensagem
recusada chega ao destino uma vez e nunca volta, mas o `500` suspende a ação, e
as vizinhas da janela que este servidor já tinha aceitado voltam como
**duplicatas** com outro `uuid` (5 a 6 por `500` na janela de 128). **Um `500`
custa uma reconexão e duplicatas**: por isso nada que aconteça em rajada recebe
`500`. A tabela completa, com o que saiu do R marcado:

| Desfecho (`receiver.Outcome`) | Resposta | Contagem |
|---|---|---|
| aceita | `200 OK` | `messages_accepted_total`, `confirm_duration_seconds` |
| ~~filtrada (§7)~~ | — | saiu com o filtro (§1.2) |
| vazia (`DATALEN = 0`) | `200 OK` | `messages_dropped_total{reason="empty"}` |
| ~~recusa `logic` final do destino (`ring=none`)~~ | — | saiu com o `ring=none` (§1.2). Com o ring em memória a recusa `logic` acontece no worker, depois do `200` (§2.3, §10.2) |
| origem não endereçável (guarda) | `500 source address unusable` | `messages_dropped_total{reason="unaddressable"}` |
| ring cheio, sink DOWN (e um erro do `Enqueue` que o ring em memória não tem, por segurança) | **nenhuma enquanto dura**: segurada (§5.7) | `backpressure_seconds_total`; se estourar, `relp_unanswered_total{reason="backpressure"}` |
| shutdown | nenhuma | `relp_unanswered_total{reason="shutdown"}` |
| par sumiu / respostas não saem mais | nenhuma | `relp_unanswered_total{reason="conn_lost"}` |
| o servidor encerrou a sessão de um par presente (frame ruim, idle timeout, segundo `open`) | nenhuma para as que ficaram em voo | `relp_unanswered_total{reason="server_closed"}` |

A vazia recebe `200` porque não tem o que guardar: um `500` mentiria que houve
erro, o rsyslog logaria um erro por mensagem e quebraria a sessão a cada uma.
**Reavaliação dos `500` do R** (decisão RP-6): dos três `500` de mensagem do R,
o de rate-limit já tinha saído (decisão 8a do R), e o da recusa `logic` final
saiu com o `ring=none`; fica só o da guarda de origem, que não dispara num par
TCP. Os `500` de protocolo (`syslog` antes do `open`, segundo `open`, comando
desconhecido) ficam iguais: o `omrelp` nunca os provoca.

**Uma transação devolvida não é descarte.** `relplog_messages_dropped_total`
conta só o que foi lido e descartado de propósito. A reconciliação de uma
sessão é:

```
relplog_messages_total = accepted + dropped{empty,unaddressable}
                         + relp_unanswered_total
```

e a do lado do rsyslog: enviadas = `200` + retransmitidas (as da sessão fechada
sem resposta); um `500` só poderia vir da guarda de origem.

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
  a janela cheia, o leitor **para de ler** e o TCP segura o emissor. Com o ring
  em memória o `Enqueue` não espera, mas uma transação segurada (§5.7) espera, e
  uma conexão só (o rsyslog abre exatamente uma por ação) seria serializada por
  ela; no R o argumento era o group commit do `ring=sqlite`. A janela é a
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
  da transação, outra para a cópia do item dentro do `Enqueue` (que no R, com
  `ring=none` ou um group commit lento, vive tanto quanto a transação), e 8 KiB
  pela goroutine, a pilha, o `Wire`, o timer e o fechamento (medidos em cerca de
  9,6 KiB no todo para uma linha pequena). Contar só a cópia deixava linhas
  pequenas porem até cinco vezes o teto na memória (revisão independente,
  2026-10-11, `TestTxnChargeCoversTheTransaction`). Com o ring em
  memória a segunda cópia só existe entre a cópia e a entrega ao canal, e uma
  transação segurada nem a faz (o ring cheio é recusado antes da cópia, §2.2):
  a conta é conservadora aqui, **de propósito a do pai**, para que
  `relp.inflight_max_bytes` meça a mesma coisa nos dois servidores (decisão
  RP-7). A conta é adquirida antes da
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
  `RecordDrop` são caminhos separados (bug real da standard). (No R, pelo mesmo
  motivo, filtro e rate-limit decidiam uma vez por mensagem; os dois saíram.)
- A retentativa é **barata**: o sink DOWN e o ring cheio são testados antes do
  `uuid`, da cópia e do item (§2.2), nos **dois** tetos do ring, e uma
  transação segurada a cada 50 ms não aloca nem cunha `uuid`
  (`TestIngestFullRingIsRefusedBeforeTheCopy`, e
  `TestIngestByteCeilingIsRefusedBeforeTheCopy` para `ring.max_bytes`).
- O `uuid` cunhado na primeira tentativa que chega a virar item é
  reaproveitado em todas: a mensagem segurada é um fato só (§3.1).
- **O fim da sessão vence a decisão.** Uma transação confere o contexto da
  sessão depois de cada espera (a vaga no teto, os 50 ms entre tentativas, o
  rearme do prazo de leitura) antes de tentar o ring. Sem isso, uma transação
  que acordava depois do aborto ainda podia ser enfileirada, e o `librelp`, que
  a retransmite porque não teve resposta, a entregaria duas vezes
  (`TestReviewRetryAfterPeerLeft`). Uma aquisição do teto que volta depois do
  fim da sessão é devolvida na hora (`TestDispatchAfterSessionEndStartsNothing`).
  E a transação confere o contexto **de novo logo antes do `Enqueue`**, depois
  da cópia (revisão independente, 2026-10-13): o fim da sessão que chega
  enquanto o item é montado deixa a transação sem resposta, e o `librelp` a
  retransmite. Vale para todo fim de sessão, inclusive os que o servidor
  decide com o par presente (frame ruim, idle, segundo `open`): "decidida" é
  "no ring", e só essas recebem a resposta antes do `serverclose`
  (`TestIngestEndedSessionIsNotEnqueued`). Um erro inesperado do ring que seja
  `context.Canceled` é o fim da sessão ou do processo: sem resposta e sem
  `ERROR` (o mapeamento do R, porte do RN); `DeadlineExceeded` segura como ring
  cheio.
- **Um par que sumiu** (EOF, reset) com transações seguradas: elas são
  abandonadas (`conn_lost`), **nunca** enfileiradas depois pelas costas da
  retransmissão, o que duplicaria o evento.
- Um erro do `Enqueue` que não seja `ErrFull` nem `ErrClosed` — o ring em
  memória não tem nenhum — é segurado como ring cheio, com o buffer descartado
  sem voltar ao pool (§15.2) e um `ERROR` amostrado: nunca um `500`.

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
  termina o `Enqueue` e recebe a resposta (com o ring em memória ele não
  espera; no R, com `ring=sqlite`, esperava o `COMMIT`).
- Frames ainda não lidos ficam sem resposta, e o rsyslog, ao ver o fim da sessão,
  retransmite o que não foi confirmado: nada se perde.
- Uma sessão que não volta em `closeGrace` (6 s), um par que parou de ler, tem
  o socket fechado sob ela: o shutdown não espera um par.
- **Os transportes fecham ao mesmo tempo** (correção da revisão do R): o
  `Close` de cada um corre em paralelo, e o shutdown espera **um**
  `closeGrace`, não um por transporte. Aqui há um transporte só, e a forma fica
  para que um segundo nunca volte a somar `closeGrace`s.
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
  linha que seria commitada: duplicata garantida; resolvido esperando o commit
  (do R, com `ring=sqlite`; o ring em memória não tem group commit).
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
(`convert.ScanPRI`), usada só pela conversão (sem filtro, a recepção não lê o
PRI, §2.2):

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
Era a pendência 4 do S, "a decidir pelo usuário"; decidido no R (decisão 16 do R)
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
- **Charset legado** (decisão 18 do R): `syslog.legacy_charset = cp1252 | latin1`,
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

Removido no go-relplog-pg-server (§1.2). Não há filtro por facility/severidade: o descarte, se houver, é do consumidor da fila. A recepção nem lê o PRI (§2.2).

## 8. Rate-limit

Removido no go-relplog-pg-server (§1.2). Não há rate-limit; a decisão 8a do R (segurar em vez de recusar) deixa de se aplicar. A proteção contra um emissor ruidoso fica na rede e nos tetos de conexão (§4.4), na janela (§5.5) e em `relp.inflight_max_bytes` (§5.6).

## 9. Ring

### 9.1 Interface

```go
type Ring interface {
    // Enqueue aceita o item. Não bloqueia por capacidade: cheio devolve
    // ErrFull na hora; fechando, ErrClosed.
    Enqueue(ctx context.Context, it *item.Item) error
    // Dequeue entrega até n itens aos workers de dreno.
    Dequeue(ctx context.Context, n int) ([]*item.Item, error)
    // Commit informa que os itens foram confirmados no sink (ou descartados
    // de propósito, como uma malformada recusada).
    Commit(items []*item.Item) error
    // Drain devolve os itens pendentes em memória (dump de emergência).
    Drain() []*item.Item
    Stats() Stats
    Mode() string
    Close() error
}
```

`Enqueue` só devolve `nil` quando o item entrou no canal. A confirmação (o
`200 OK` da transação RELP) acontece depois desse retorno, nunca antes. O
`Item` carrega o cabeçalho (§3.4) e o cru; **não carrega envelope**.
`ErrUnavailable`, `ErrNoBacklog`, `Touch` e `ring.Writer` saíram com os rings
Redis, SQLite e `none` (molde do SP). O ring em memória tem ainda
`FullFor(n)`, a pré-checagem sem trava dos dois tetos de §2.2.

### 9.2 Saturação

Dois tetos, avaliados juntos:

- `ring.capacity_items` (padrão 65 536);
- `ring.max_bytes` (padrão 512 MiB), sobre o **cru**, com a medida de cada modo:
  - a memória que o item **segura**, `cap(cru)` mais `ItemOverhead` = 256 B (a
    struct `item.Item` de 224 B, com a caixa `*[]byte` do buffer do pool, e o
    slot do canal, arredondados; no R, 384, com os campos dos rings que
    saíram). Contar `len(cru)` deixava uma linha de
    8 193 bytes, copiada para o buffer de 64 KiB da sua classe de pool (§15.1),
    custar oito vezes o que a contabilidade dizia. `Enqueue`, `Commit` e `Drain`
    usam a mesma medida.

  O JLH é memória do worker, com teto próprio (§15.3), e somá-lo aqui seria
  contar memória que ninguém alocou.

Os dois tetos contam a **mesma população**: o aceito e ainda não confirmado
(`Commit`), **inclusive** os lotes que os workers retêm em retentativa. O teto
de itens é um contador próprio, não o buffer do canal, que só limitaria o que ainda está na fila: com um lote retido,
um ring de capacidade 4 aceitava a quinta mensagem e o `/readyz` dizia `5/4`
(achado pela suíte de integração). "Um lote retido não está no ring" (§10.2) diz
respeito ao que o `Drain` do shutdown devolve, não à contabilidade.

Qualquer um estourando satura o ring: as transações são seguradas (§5.7).
`ring.high_water` (0.80) é avaliado nos dois e derruba o `/readyz`.

### 9.3 `ring.mode = memory`

Canal com buffer de `capacity_items`, com contador atômico de bytes. `Enqueue`
retorna assim que o item entra no canal. **Garantia:** RAM deste nó. Perde sob
`kill -9`, OOM ou pane; sob `SIGTERM` a perda é evitada pelo dreno do shutdown
(§13.4), e sob FATAL do sink, pelo dump (§12). É o único modo (não há chave
`ring.mode`, §1.2).

### 9.4 `ring.mode = sqlite`

Removido no go-relplog-pg-server (§1.2).

### 9.5 `ring.mode = redis`

Removido no go-relplog-pg-server (§1.2).

### 9.6 `ring.mode = none`

Removido no go-relplog-pg-server (§1.2).

### 9.7 Combinações que degradam ou recusam

Removido no go-relplog-pg-server (§1.2).

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
pedaço, com capacidade limitada ao próprio tamanho), o `level` e o `sid` (as duas
colunas da fila que o cabeçalho do item não tem) e o item de origem, de onde
saem `uuid`, `from`, `space` e `event` (§3.7.1). As colunas promovidas e o
subject NATS do R saíram com os destinos que os usavam.

Os workers de dreno (`sink.workers`, padrão **4**, §10.5):

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
pedaço, não por tentativa, e uma escrita cortada pelo contexto de quem chama (um
`SIGTERM` no boot) não conta como erro do sink (§14.1).

### 10.2 Falha de pedaço, e o outbox

O worker **segura o pedaço** e retenta com backoff (`sink.retry_backoff_min`
100 ms → `retry_backoff_max` 2 s), **sem puxar mais nada do ring**. O ring para de
escoar e enche, até o backpressure de §2.3 aparecer: as transações RELP são
seguradas (§5.7).

- **O retry reenvia os mesmos bytes**, com o mesmo `uuid`: o JLH já serializado é
  reaproveitado, nunca refeito. É a regra do outbox do JLH, e poupa CPU no
  momento em que o sistema está em dificuldade.
- **Um lote retido não está no ring.** O shutdown o coleta de
  `Workers.Pending()`, ou ele some depois de confirmado. E o `hold` faz `append`,
  nunca atribuição: um worker que abandona o lote no shutdown o mantém registrado,
  e um `Dequeue` posterior não pode sobrescrevê-lo (bug real do irmão).
- **At-least-once.** Uma escrita que estourou o prazo pode ter sido aceita, e o
  retry a reenvia. A deduplicação é da fila: `ON CONFLICT (uuid) DO NOTHING`
  (§3.7.1), e a segunda cópia conta em `relplog_sink_postgres_duplicates_total`.
- **Contaminação** (§15.2): uma tentativa que termina por prazo ou cancelamento
  marca o pedaço, e nem o cru nem o buffer do pedaço voltam ao pool, nem quando a
  retentativa seguinte tem sucesso.

| Classe | Efeito |
|---|---|
| `ClassNetwork` | retenta; não muda o estado de saúde (§13.3) |
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`relplog_events_dropped_total{reason="sink_logic"}`), confirmado no ring. Essas mensagens já tinham recebido `200 OK`: é o descarte contado de §2.3 |
| `ClassFatal` | FATAL imediato |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`relplog_events_written_total`, só os demais em `sink_logic`. No PostgreSQL o
pedaço é **um** comando: na falha nada foi gravado, e o pedaço inteiro conta
como perdido. O mecanismo fica porque é o contrato da interface, que os testes
dos workers exercitam.

### 10.3 `sink.type = redis`

Removido no go-relplog-pg-server (§1.2).

### 10.4 `sink.type = sqlite`

Removido no go-relplog-pg-server (§1.2).

### 10.5 `sink.type = postgres`

O único destino (não há chave `sink.type`, §1.2): fila **compartilhada entre
instâncias** e entre produtores LogHub, a `loghub.inbox` de §3.7.1. `github.com/jackc/pgx/v5` pela API nativa (a
classificação precisa do `pgconn.PgError`).

- **Nunca faz DDL**, nem `CREATE TABLE IF NOT EXISTS`: N instâncias subindo
  juntas correm no catálogo, e o papel de produção não tem o privilégio.
  `docker/postgres/inbox.sql` é a cópia literal da DDL da skill (§3.7.1).
  `sink.postgres.table` tem padrão `loghub.inbox`; outro nome (identificador em
  minúsculas, com esquema opcional) só serve para testes ou para uma segunda
  fila com a **mesma** DDL.
- **Sem TLS, sempre** (§1.2, molde do SP). A conexão é `sslmode=disable`: o
  `TLSConfig` do pgx é nulo e os *fallbacks* são descartados, então não há
  segunda tentativa que pudesse negociar TLS. Um servidor cujo `pg_hba.conf` só
  tem linhas `hostssl` recusa o login com a classe `28`, fatal no boot com as
  palavras do próprio servidor. As chaves `sink.postgres.tls`, `tls_ca` e
  `tls_insecure` do R não existem.
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
- `sink.workers` tem padrão **4** direto (no R, 8 rebaixado para 4 com `WARN`
  num destino SQL; molde do SP); `WARN` acima de 16; `pool_size = 0` deriva
  `workers + 1`, e um `pool_size` abaixo de **`workers + 1`** é exit 1
  (revisão independente, 2026-10-13): a sonda de saúde tira a conexão dela do
  mesmo pool, e com `pool_size = workers` escritas lentas mas funcionando
  seguravam todas, a sonda estourava `sink.health_timeout` esperando uma, o sink
  ia a DOWN e, passado `fail_timeout`, a FATAL: exit 3 sem nada errado no banco
  (`TestPostgresHealthHasAConnectionWhileTheWorkersWrite`). O `+ 1` basta aqui, e só aqui (conferido na revisão independente do
  `go-syslog-server`, 2026-10-13, D1, onde o `ring=none` faz dos receptores,
  até `ring.max_inflight`, os escritores e o `+ 1` não protege nada): o ring é
  só memória, quem escreve são os `sink.workers`, um `Write` (uma conexão) por
  vez cada, a reingestão roda antes deles e da sonda, e a sonda é uma só
  (`TestPoolLeavesTheProbeAConnection`, contra o PostgreSQL real: três
  escritas presas num `LOCK TABLE`, conferidas em `pg_stat_activity`, e a
  sonda responde com `pool_size = 4`; com `pool_size = 3`, não). Tetos de §13.2:
  `workers` 1..1024, `pool_size` 0..4096.
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
  `PG*` escondido** (sob mutex, só no boot) e `sslmode=disable` (pendência 9 do
  S, como no SP): nenhuma variável libpq
  (`PGSSLMODE`, `PGPORT`, `PGSERVICE`, `PGCHANNELBINDING`, `PGREQUIREAUTH`,
  `PGSSLNEGOTIATION`, `PG{MIN,MAX}PROTOCOLVERSION`, `PGOPTIONS`,
  `PGTARGETSESSIONATTRS`, `PGAPPNAME`…) chega ao pool nem derruba o parse. Em tempo
  de execução, um `23514` que não nomeia constraint (a partição que sumiu) é
  `fatal`, não `logic`; um `from` que não é UUIDv7 (só dado de um dump) é
  recusado antes do `INSERT`, como `logic`.
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
  toda escrita virava `23502`, que só derruba o processo depois de o boot
  ter dado a fila por boa (§20.18). `received_at` passa porque tem `DEFAULT now()`. Pelo catálogo, não
  por sonda (como no MySQL do R): uma sonda dispararia triggers do operador e
  consumiria sequências, e a recusa dela não nomeia a coluna.
  **Revisão independente de 2026-10-13** — também FATAL no boot: uma **regra**
  `ON INSERT` ou `ON UPDATE` na tabela (`pg_rewrite`, `ev_type` 3 ou 2). O
  PostgreSQL recusa `INSERT ... ON CONFLICT` numa tabela com uma delas
  (`0A000`, *feature not supported*), e a tabela passava no boot para recusar
  todo pedaço como `network`, para sempre, com o sink UP e o `/readyz` verde.
  Regras `ON DELETE` e `ON SELECT` não tocam o `INSERT` e passam. Uma regra
  criada **depois** do boot dá `0A000` na escrita, que é `fatal`
  (`TestPostgresRulesAreRefusedAtBoot`).
- Durabilidade: a do servidor (`synchronous_commit`). A skill admite a fila
  `UNLOGGED`, que o servidor não confere: ela é apagada numa queda do PostgreSQL
  e só serve se a origem puder reenviar, o que o syslog não faz.

| SQLSTATE | Classe |
|---|---|
| `08`, `53`, `57`, `40` | `network` |
| `22`, `23` (fora do `ON CONFLICT`) | `logic` |
| **`42`** (esquema), `28`, `2F`, **`3D`**, `42501`, `25006`, **`23502`**, **`0A000`**, `23514` sem constraint nomeada | **`fatal`** |
| qualquer outra | `network` |

`23502` (`not_null_violation`) é `fatal` (pendência 7 do S, corrigida no R; no
SP ainda aberta, §22.1 item 5 de lá): o servidor nunca manda `NULL` nas dez
colunas, então ele vem da tabela (coluna obrigatória acrescentada,
`DROP DEFAULT`, trigger) e, como `logic`, descartaria todo pedaço com o sink UP.
A classe `3D` (`sink.postgres.database` inexistente, `3D000` no login) é `fatal`
(pendência 8): retentar até o `boot_timeout` só adiava o exit 3 com a mensagem de
rede.

**A validação do boot tem prazo próprio** (pendência 6 do S, corrigida no R; no
SP ainda aberta): roda com `sink.op_timeout`, não no `sink.health_timeout` (1 s) da
checagem que a dispara — são uma dúzia de idas e voltas, e a ~55 ms nenhuma
tentativa cabia em 1 s —, e um cancelamento do boot (`SIGTERM`) ainda a
interrompe: as checagens de saúde carregam o contexto de quem as chamou como
"tempo de vida" (`withLifetime`), que a validação vigia.

**A classe `42` é fatal e nunca `logic`**: uma tabela dropada classificada como
`logic` descartaria um lote a cada 5 ms, para sempre, num processo que parece
saudável. Esta tabela e `TestClassifySQLState` mudam juntas.

### 10.6 `sink.type = mysql`

Removido no go-relplog-pg-server (§1.2).

### 10.7 `sink.type = nats` (padrão)

Removido no go-relplog-pg-server (§1.2).

### 10.8 `sink.type = none`

Removido no go-relplog-pg-server (§1.2).

### 10.9 `LOGHUB_EB_URL`

`loghub.eb_url` (env `LOGHUB_EB_URL`) é a URL do eventbus da convenção LogHub.
Aqui o único eventbus aceito é o PostgreSQL: a URL preenche a conexão de
`[sink.postgres]` (molde do SP).

| Esquema | Preenche |
|---|---|
| `postgres://`, `postgresql://` | `addr` (`host:porta`, porta 5432 se ausente; ou o diretório do socket de `?host=/dir`), `username`, `password`, `database` (do caminho). **Usuário e banco são obrigatórios** (abaixo) |
| outro (`nats://`, `tls://`, `redis://`, `rediss://`, `amqp://`...) | exit 1: não suportado. Uma URL de outro eventbus esquecida de uma implantação irmã derruba o boot em vez de ser ignorada |

**`sslmode`, a regra do TLS.** Este servidor nunca usa TLS (§1.2):

| `sslmode` na URL | Efeito |
|---|---|
| ausente, `disable`, `allow`, `prefer` | aceito: todos admitem conexão sem TLS (`prefer` é o padrão da libpq que muita URL gerada escreve por extenso) |
| vazio (`?sslmode=`) | **exit 1**: a libpq o recusa, e lido aqui como "ausente" a URL diria duas coisas a dois leitores (revisão independente, 2026-10-13) |
| `require`, `verify-ca`, `verify-full` | **exit 1**: pedem TLS, e aceitá-los mandaria a senha e todo evento em claro a quem pediu o contrário |
| qualquer outro valor | exit 1: não é um `sslmode` da libpq (o valor não é citado) |

**Uma fonte só da verdade:** com `LOGHUB_EB_URL` preenchida, qualquer chave que a
URL **pode** expressar definida explicitamente (flag, env ou INI) é exit 1 com as
duas fontes no log, mesmo que esta URL não traga aquela parte (uma URL sem senha
quer dizer "sem senha", não "procure em outro lugar"). As chaves donas:
`sink.postgres.addr`, `username`, `password`, `database`. As demais (`table`,
`pool_size`) convivem com a URL. Consequência: a senha vai **dentro** da URL.

Um servidor só (a URL não é lista; uma senha pode conter vírgula). Fora
`sslmode` e `host=/dir` (o socket unix, sem host antes do caminho), qualquer
parâmetro de consulta é recusado, não ignorado; o nome dele é citado só se for
uma palavra-chave da libpq (uma senha colada depois de um `?` também é "chave").
**Um parâmetro repetido também é recusado**: em `sslmode=disable&sslmode=require`
o `url.Values.Get` lê o primeiro e a libpq vale o último, e a URL passaria pela
recusa do TLS dizendo o contrário. Com socket, a porta da URL é mantida:
`postgres://u:p@:5433/db?host=/run/pg` dá `addr = /run/pg:5433`.
**`?host=` vazio é exit 1** (revisão independente do `go-syslog-server`,
2026-10-13, B4): a libpq e o pgx o leem como "ausente" e discam o padrão deles
(o socket do diretório compilado, ou `localhost`), e este servidor discava o host
da autoridade: duas leituras da mesma URL, dois servidores. Recusado como o
`sslmode` vazio (`TestEBURLMustNameItsParts`).

**As recusas da revisão independente de 2026-10-13** (exit 1, sem citar a URL):

- **Sem usuário ou sem banco.** `postgres://h/db` e `postgres://u:p@h` são
  recusadas nomeando a parte que falta. A libpq usaria o usuário do sistema
  operacional e um banco de mesmo nome; este servidor usava o padrão do
  registry (`syslog`): dois leitores da mesma `LOGHUB_EB_URL` chegavam a dois
  bancos, e a URL é dona das duas chaves, então o operador não podia
  corrigi-las em outro lugar. Explícito em vez de adivinhado (decisão,
  `docs/DECISOES.md` §2).
- **`host=` com `,`, `:` ou espaço.** A libpq lê o `host=` literal:
  `/run/pg,/x` são dois servidores, e `/run/pg:5433` é um diretório com `:` no
  nome, onde este servidor leria diretório e porta. Nenhum dos dois é um
  servidor só nas duas leituras; a porta vai na URL
  (`postgres://u@:5433/db?host=/run/pg`).
- **Host com `:` que não é IPv6.** `postgres://u@h:5432:5433/db` virava
  `addr = [h:5432]:5433`, um nome DNS que ninguém resolve, retentado até o
  `boot_timeout`.
- **Host codificado em porcento** (`postgres://u@%2Frun%2Fpg/db`, a forma da
  libpq para o socket): o parser do Go o recusa, e a mensagem diz agora para
  usar `?host=/dir` em vez de "invalid percent-escape", que mandava o operador
  atrás da senha. Um `://` repetido diz que um `://` na senha precisa ser
  codificado (`%3A%2F%2F`).
- **`LOGHUB_EB_URL` só com espaços** é exit 1: definida e vazia de conteúdo, o
  operador que quis desligá-la não saberia que as chaves `[sink.postgres]` é
  que valem, e o dump mostrava os espaços como se fossem uma URL. Vazia
  (`LOGHUB_EB_URL=`) continua sendo "não definida".

**As recusas do R, além das do SP** (revisão independente do R, 2026-10-11):

- **Um `@` depois do primeiro `/`, `?` ou `#` da autoridade é recusado**, sem
  citar a URL, com a dica de `%2F`, `%3F` e `%23`: nenhuma URL PostgreSQL tem
  `@` literal no caminho, na consulta ou no fragmento, então é um usuário ou
  senha com `/`, `?` ou `#` sem codificar, que encurta a autoridade
  (`postgres://app:39182#Zs3@db/x` discava `app:39182` sem senha).
- **Uma URL PostgreSQL com fragmento é recusada**: a libpq não tem fragmento, e
  um `#` ali é uma senha cortada ou uma consulta jogada fora — aqui, um
  `?sslmode=require` depois do `#` nunca chegaria à recusa do TLS.

Nenhuma mensagem de erro cita a URL (ela pode ter senha; §16.2). O
`--dump-config` mostra os valores derivados como comentário, com a senha
redigida, para que o dump relido não dispare o conflito.

**`sink.postgres.addr`, venha da URL ou não, é um servidor só:** `host:porta`
com host não vazio, sem vírgula, espaço ou tab (o tab voltou à lista do R:
o corte o tinha perdido), sem `:` a não ser num IPv6 literal, e porta 1–65535
sem zero à esquerda; ou um diretório de socket começando com `/`, sem vírgula
nem branco, com um `:porta` opcional na mesma faixa.

### 10.10 `sink.type = clickhouse`

Removido no go-relplog-pg-server (§1.2).

## 11. Plano de controle: heartbeat beacon

Removido no go-relplog-pg-server (§1.2). Não há heartbeat beacon (dependia do `sink=nats`); `loghub.beacon_interval` e `loghub.ctl_namespace` saíram. O servidor nunca emite `beacon`, `ping`, `pong` nem sonar (§20.21).

## 12. Arquivo de emergência

**Propriedade do ring, não do sink.** O ring é memória: sem o dump, o que ele
segura — mensagens que já receberam `200 OK` — morre com o processo. Dispara quando o processo sai com o ring (ou um
lote retido pelos workers) não vazio:

- sink FATAL (§13.3), ou durante o dreno do shutdown (§13.4);
- `shutdown.drain_timeout` estourado.

### 12.1 Formato

`<emergency.dir>/emg-<agent_uuid>-<unixnano>.jsonl`, com `emergency.dir` padrão
`<data_dir>/emergency`. O carimbo de nanossegundos tem sempre dezenove dígitos
(de 2001 a 2286), então, para um mesmo agente, a ordem lexicográfica dos nomes
**é** a ordem temporal.

**JSON Lines: um envelope JLH pronto por linha** (decisão 3 da entrevista do
SP, herdada; o `.bin` de cabeçalho + cru do R saiu com o codec, §3.4):

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
  `go-webhook-nats-server` o tornou melhor esforço; aqui (como no SP e no R) ele
  continua decidindo o dump, de propósito (§12.3).
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

No boot, todo `emg-*.jsonl` de `emergency.dir` é entregue à fila **antes** de
qualquer transporte abrir (§13.1 passo 9), do mais antigo para o mais novo
(ordem lexicográfica de nome), **qualquer que seja** o `emergency.enabled` atual:
só a **escrita** de dumps depende dele. Arquivos `.corrupt`, `.tmp` e qualquer
outro nome são ignorados.

**Em fluxo, nunca o arquivo inteiro.** Cada arquivo é aberto
(`emergency.Store.Open`) e lido em pedaços (`Reader.Next(sink.batch,
sink.batch_max_bytes)`), cada pedaço entregue antes de o próximo ser lido. Um
dump de JLH chega a ~12× o ring que o produziu (§3.9); carregado inteiro, ele
matava o boot por OOM, e o boot seguinte tentava de novo, em laço. Um único
`sink.boot_timeout` **sem progresso** cobre a reingestão inteira, não um por
arquivo: o prazo anda a cada pedaço confirmado (é `boot_timeout` desde o último
pedaço entregue). Com um prazo só a partir do início — a regra herdada do SP —
uma reingestão saudável de um dump grande que passasse do `boot_timeout` saía
com 3 na primeira falha transitória, sem uma retentativa, e todo boot seguinte
recomeçava o mesmo arquivo da primeira linha (duplicatas que o `ON CONFLICT`
só absorve enquanto a fila guarda os `uuid`, e talvez um *crashloop*).
Revisão independente de 2026-10-13, a mesma escolha do RN
(`TestLongReingestionStillRetriesATransientFailure`; `docs/DECISOES.md` §2).

**Sem conversão.** A linha é entregue como está; as colunas da `loghub.inbox`
(`uuid`, `from_uuid`, `space`, `level`, `sid`, `event_at`/`event_nsec`, §3.7.1)
saem do **cabeçalho da própria linha**. `type` precisa ser `"syslog"` e `step`,
`0`. Consequência: um dump sobrevive a uma troca de chaves `[syslog]` (ou de
versão do binário) entre duas execuções sem mudar um byte (§3.5).

**`emergency.ParseLine` é a única porta entre um arquivo em disco e as colunas
da fila**, e recusa tudo o que este servidor não poderia ter escrito. A régua é
a fila, não o schema: a linha volta **sem conversão**, e uma linha que a fila
recusasse seria uma recusa `ClassLogic` do pedaço **inteiro** — todo envelope
daquele `INSERT`, inclusive os íntegros (§10.2). Então a porta recusa, antes,
tudo o que a fila poderia recusar:

- UTF-8 válido (o `encoding/json` trocaria um inválido por U+FFFD em silêncio, e
  a fila guardaria outra coisa);
- todo escape `\u` da linha em `\u0000`–`\u001f`, em minúsculas: os únicos que
  o escritor do JLH emite (§3.3). Isso exclui um *surrogate* solto, que o
  `jsonb` recusa com `22P05`;
- profundidade de aninhamento até 8;
- o cabeçalho (tudo antes de `"payload":`) **byte a byte igual** ao que
  `jlh.AppendHeader` produz para os valores lidos: sem chave duplicada, sem
  espaço, ordem e grafia de números canônicas;
- exatamente as nove chaves do JLH, com chave desconhecida recusada e nenhuma
  faltando; nada depois do objeto. Só o **cabeçalho** é decodificado; o
  `payload` é cortado por posição e percorrido token a token, sem montar a
  árvore;
- `uuid` UUIDv7 em minúsculas, canônico; `from` UUIDv7 em minúsculas (a CHECK
  `inbox_from_v7_ck`);
- `space` na gramática de §13.2;
- `type = "syslog"`, `step = 0`, `level` 0–7, `sid` ≤ 2^53 − 1;
- `event.uts` em 0..253402300799 e `event.sub` em 0..999999999; `event.iso`
  igual ao derivado de `uts`/`sub`;
- o instante do `uuid` (ms) igual ao de `event`: `uuid` e `event` são uma
  amostra só (§3.1);
- o `payload` que o sink corta por posição (`jlh.Payload`, depois de
  `},"payload":`) igual ao `payload` que o parser JSON viu. Uma linha em que os
  dois discordam poria um objeto na fila enquanto o cabeçalho diz outro;
- `payload` um objeto com a chave de primeiro nível `message`, exatamente com
  essa grafia, e uma string — e, se a chave se repetir, a **última** ocorrência,
  que é a que o `jsonb` guarda (a CHECK `inbox_syslog_ck`: o `?` do `jsonb`
  diferencia maiúsculas, e o casamento de struct do `encoding/json` não);
- todo número da linha é um inteiro simples de no máximo 20 dígitos, sem `.`,
  `e` nem `E`: o `jsonb` recusa `1e1000000` ou `1e-16384` com `22003`, e o
  pedaço inteiro se perderia.

O que passa não pode ser recusado pelas CHECKs da fila nem pelo `jsonb`.

**Linhas e arquivos danificados:**

| Situação | Efeito |
|---|---|
| a última linha não termina em `\n` | cauda **truncada** (o dump foi cortado); as linhas íntegras antes dela são entregues |
| linha completa que `ParseLine` recusa | **inválida**: pulada; o `\n` localiza a próxima |
| linha maior que `emergency.MaxLine` | **inválida**: descartada durante a leitura, nunca guardada |
| arquivo com cauda truncada ou linha inválida | **danificado**: as linhas íntegras são entregues e o arquivo é renomeado para `.corrupt` (com `fsync` do diretório), com as contagens de bytes em `ERROR`; destruir a evidência de uma queda não é decisão deste programa. Se o `rename` aconteceu e só o `fsync` do diretório falhou, o arquivo **está** quarentenado (pendência 24 do S, corrigida no R, `emergency.ErrNotDurable`): o `fsync` vai em `WARN` e o `ERROR` com os bytes sai igual, com o nome novo; só sem nome devolvido é "cannot quarantine" |
| arquivo limpo | removido **só depois** de todos os seus envelopes serem confirmados, com `fsync` do diretório depois do `unlink` (a "remoção durável" do R): sem ele, uma queda de energia logo depois devolveria o nome, e o boot seguinte entregaria o dump inteiro de novo, absorvido pela fila só enquanto ela ainda guardar aqueles `uuid`. Um `fsync` que falha depois do `unlink` é `WARN` (o arquivo já saiu); uma falha da própria remoção é `ERROR`, e o arquivo volta no boot seguinte (§22.1 item 3). Um `open` do diretório que falha depois do `unlink` também é `ErrNotDurable` (revisão independente de 2026-10-13, o código do RN): o arquivo saiu, e "cannot remove ... delivered again" seria falso |

`emergency.MaxLine` = 12 × 16 MiB + 8 KiB: o pior JLH de §3.9 com o **maior**
`max_message_bytes` que a config aceita, e não o da execução atual, para que um
dump de uma execução com limite maior ainda volte depois de o operador baixá-lo.
**Nunca é alocado de antemão**: a linha cresce com o que é lido, e uma maior que
o teto é consumida sem ser guardada. O leitor é alvo de fuzz (`FuzzRead`, §18.3).

A entrega segue as classes dos workers pedaço a pedaço (§13.1 passo 9): `logic`
conta, loga e segue para o pedaço seguinte, e o arquivo é tratado como
entregue (sair deixaria o mesmo arquivo para o boot seguinte, num *crashloop*
com todos os transportes fechados); `network` reenvia o mesmo pedaço com backoff
dentro de `sink.boot_timeout`, e estourado sai com 3 **sem remover o arquivo**;
`fatal` sai com 3. Um envelope de uma vida com outro Workspace vai para a fila
com o `space` da linha.

**Gauges recontados do disco** (pendência 25 do S, corrigida no R; no SP ainda
aberta, §22.1 item 9 de lá): ao fim da reingestão, `relplog_emergency_files` e
`relplog_emergency_events_pending` são recontados do diretório
(`emergency.Store.Count`, que lê cada arquivo em fluxo e conta as linhas que
`ParseLine` aceita), **nunca zerados**. **Durante** a reingestão,
`relplog_emergency_files` diz quantos arquivos esperam (fixado antes do estado
de boot que anuncia a reingestão) e `relplog_emergency_events_pending` **não é
tocado**: o que está no disco só se sabe pela recontagem do fim. Ele mostrava o
tamanho do pedaço em voo, que não é nem uma coisa nem outra (o último *scrape*
antes de um exit 3 dizia um pedaço por um dump inteiro; revisão independente,
2026-10-13). Ao fim: um dump que ficou (ilegível, remoção ou
quarentena que falharam) continua contado — o ilegível como arquivo, sem
mensagens —, e o alerta sobre eles continua disparando. O dump escrito no
shutdown **soma** aos gauges em vez de fixá-los.

**Não há a exceção `sink=none` do R** (que mantinha os dumps no disco): todo
dump vai para a fila.

### 12.3 Falha do próprio dump

Disco cheio, sem permissão, `fsync` recusado, `emergency.enabled = false`:
`ERROR` com a contagem exata, `relplog_events_dropped_total{reason="emergency_failed"} += n`,
exit 3. `emergency.dir` é criado **no boot**, com uma escrita de prova: um
diretório criado durante o incidente falha durante o incidente, e um problema
de permissão achado no dump é uma mensagem perdida em vez de um boot recusado.

## 13. Ciclo de vida

### 13.1 Boot

```
 1. config (§16): flags > env > INI > padrão. --help, --version e
    --dump-config saem aqui, sem tocar identidade nem disco (a seção
    informativa [identity] do dump mostra o que o AMBIENTE diz, sem
    resolver nada). LOGHUB_EB_URL é expandida e conferida aqui (§10.9).
    Erro ou combinação inválida => exit 1. (No R, os arquivos do TLS eram
    conferidos aqui; não há TLS.)
 2. data_dir: resolvido para absoluto, criado (0750), exportado como DATADIR
 3. lhident.Initialize() — falha => o processo sai com 100–114, pela biblioteca
 4. Identity congelada; space validado (§13.2) => exit 1 se inválido
 5. porta ADMIN aberta: /healthz 200, /readyz 503 com o motivo, /metrics
 6. emergency.dir criado e provado gravável (com emergency.enabled; a
    reingestão do passo 9 lê o diretório de qualquer forma)
 7. SINK: pool preguiçoso; WaitReady com retentativa até sink.boot_timeout
    (300 s) => exit 3; validação de tabela, tipos, índices, permissão e
    recuperação (§10.5), com prazo próprio de sink.op_timeout: recusa =>
    exit 3 IMEDIATO, com o log "the sink refused this server permanently"
    (e não "within sink.boot_timeout"); a linha do boot_timeout fica para o
    destino inalcançável
 8. RING memory, workers (ainda parados) e o pipeline de recepção montados
 9. antes, o temporário gêmeo de um dump é removido e o de um dump
    interrompido vira um WARN com o caminho, intocado (§12.1);
    reingere emg-*.jsonl (§12.2), sem conversão, EM FLUXO (nunca o
    arquivo inteiro na memória) e PEDAÇO A PEDAÇO, com um único
    sink.boot_timeout para tudo (sink.Instrumented.Deliver): `logic` descarta,
    conta e loga aquele pedaço e segue para o próximo; `network` reenvia o
    MESMO pedaço com backoff dentro de sink.boot_timeout; `fatal` sai com 3;
    um sinal sai com 130. No fim, os gauges de emergência são recontados do
    disco (§12.2)
10. (do R: dreno do backlog do ring=sqlite; não existe aqui, o ring em memória
    começa vazio)
11. (do R: beacon; não existe aqui, §11)
12. inicia a saúde e os workers; abre o transporte: tcp
13. /readyz 200
```

**Identidade antes de tudo que escreve.** `lhident.Initialize()` roda **uma
vez**, no `main`, depois das flags que encerram o programa e antes de abrir
qualquer recurso com `defer` ou criar goroutine (regra 2 da skill). A app recebe
um struct `Identity` (`From`, `Space`, `AgentName`, `Hostname`, `MachineID`)
preenchido dos getters: os testes montam a app dezenas de vezes no mesmo
processo, e uma segunda chamada a `Initialize()` sai com 112. `AgentName` e
`Hostname` compõem o `application_name` da conexão,
`rlpg/<AgentName>@<Hostname>` (§10.5).

**`DATADIR` é exportado antes do `Initialize()`** para que a biblioteca grave
`agent_uuid` e `machine_id` na mesma raiz em que o servidor grava `emergency/`
(§16.6). Sem `DATADIR` absoluto, o padrão `./data` vira absoluto a partir do
diretório de trabalho; no contêiner é `/data`.

**A porta admin abre antes do sink:** num crashloop por banco indisponível, o
operador tem `/readyz` com o motivo e `/metrics`.

**O transporte abre por último.** Abrir o RELP antes de o sink estar pronto e de
os dumps terem voltado seguraria toda transação até o `backpressure_timeout` e
devolveria todas; um transporte que não está escutando faz o `omrelp` esperar do
lado dele, que é o certo.

### 13.2 Validação de configuração

| Combinação | Efeito |
|---|---|
| `listen.tcp` vazio (não há outro transporte) | exit 1 |
| `listen.tcp` fora de `host:port` (`:20514` serve); `listen.tcp_listeners` < 1 | exit 1 |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `relp.window` fora de 1–65536 | exit 1 |
| `relp.max_frame_bytes` < `syslog.max_message_bytes` ou > 999 999 999 | exit 1 (o fusível fecharia sessões em linhas que o truncamento guardaria; nove dígitos de `DATALEN`) |
| `relp.inflight_max_bytes` < `syslog.max_message_bytes` | exit 1 (uma transação precisa sempre caber) |
| `proxy.protocol ≠ off` sem `proxy.trusted` válido | exit 1 |
| `space` fora de `^[a-z0-9-]+(\.[a-z0-9-]+)*$` | exit 1 (a lib aceita `a..b`, `.x` e `x.`; segmento vazio não serve à convenção LogHub). Acima de 64 caracteres a própria lib já sai com 111 antes |
| `syslog.default_priority` fora de 0–191 | exit 1 |
| `syslog.rfc3164_timezone` desconhecido para o `time.LoadLocation`, ou `Local` | exit 1 (a imagem embute `time/tzdata`; `Local` dependeria do host) |
| `syslog.legacy_charset` fora de `""`, `cp1252`, `latin1`, `windows-1252`, `iso-8859-1` | exit 1 |
| `syslog.max_message_bytes` < 480 ou > 16 MiB; `sd_max_elements` ou `sd_max_params` < 1 | exit 1 (480 é o mínimo da RFC 5424) |
| `ring.capacity_items`, `ring.max_bytes`, `sink.workers`, `sink.batch`, `sink.batch_max_bytes`, `sink.max_retries` abaixo de 1; `ring.high_water` fora de (0, 1]; `log.access_sample` fora de [0, 1]; `tcp.max_conns*` negativo; `retry_backoff_max < retry_backoff_min` | exit 1 |
| **tetos** (revisão independente, 2026-10-13): `ring.capacity_items` > 16 777 216; `sink.batch` > 65 536; `sink.batch_max_bytes` > 256 MiB; `sink.workers` > 1024; `sink.postgres.pool_size` > 4096; `listen.tcp_listeners` fora de 1–256 | exit 1, nomeando a chave. Sem eles um erro de digitação de poucos dígitos derrubava o boot com pânico (`makechan` de `capacity_items`), alocava um gigabyte por `Dequeue` (`sink.batch`), dava a volta no `int32` do pool (`pool_size = 4294967300` virava 4) ou montava um pedaço cujo `Bind` passa do limite de 1 GiB da mensagem da libpq e falha sem SQLSTATE: `network`, retentado para sempre (`batch_max_bytes`) |
| `sink.workers > 16` | `WARN` (§10.5) |
| `sink.postgres.pool_size` negativo, ou abaixo de `sink.workers + 1` | exit 1 (`0` deriva `workers + 1`; a sonda de saúde precisa de uma conexão a mais, §10.5) |
| `sink.postgres.addr`, `username` ou `database` vazios; `addr` que não é um servidor só (§10.9); `table` que não é identificador em minúsculas (com esquema opcional) | exit 1 |
| `LOGHUB_EB_URL` em conflito, com esquema que não é `postgres://`/`postgresql://`, com `sslmode` que pede TLS ou vazio, com parâmetro desconhecido ou repetido, com fragmento, com `@` depois de `/`, `?` ou `#`, sem usuário ou sem banco, com `host=` vazio ou que não é um diretório só, ou só com espaços (§10.9) | exit 1, sem citar a URL |
| durações zero onde zero não é "sem limite" (§16.5) | exit 1 |
| `GOMEMLIMIT` ausente, ou `< ring.max_bytes × 2` | `WARN` (§15.4) |
| parâmetro desconhecido na linha de comando ou no INI | exit 1. Isto inclui toda chave do R que saiu (`ring.mode`, `sink.type`, `listen.tls`, `[tls]`, `[filter]`, `[ratelimit]`, `[sink.nats]`, `sink.postgres.tls`...) e as do SP que não existem aqui (`listen.udp`, `listen.unix`, `tcp.framing`, `tcp.backpressure_timeout`): um INI do pai não sobe aqui em silêncio. Uma chave do R que saiu é nomeada com "go-relplog-server has it, go-relplog-pg-server does not (SPEC §1.2)" (`removedParams`, porte do RN, 2026-10-13), inclusive logo depois de uma flag com valor |
| `--help`, `--version` ou `--dump-config` com `=valor`; `--config=` vazio | exit 1, sem o valor (revisão independente, 2026-10-13): `--dump-config=false` despejava e saía com 0, e um `--config=` vazio caía no `RELPLOG_CONFIG`, o contrário do que diz |
| linha do INI que não é `[seção]` nem `chave = valor`; argumento da linha de comando que não começa com `-`; nome de parâmetro desconhecido sem forma de nome | exit 1, nomeando o número da linha ou só o que vem antes do `=` do argumento, **nunca o conteúdo** (pendência 30 do S, corrigida no R e no SP) |
| flag sem valor seguida de outra flag; valor depois de um espaço; valor que o shell partiu num espaço; valor que começa com `--` na forma separada | exit 1, nunca com o valor: um argumento com forma de flag nunca é valor de outra flag e só é nomeado se for uma flag deste servidor; nada depois de um valor é citado. Um argumento desconhecido com forma de flag logo depois de um valor (`--sink.postgres.password=abc -hunter2`) só é nomeado se cair numa seção que este servidor tem (`--sink.postgres.tabel`, um erro de digitação; o `knownSectionOf` do R); fora disso o erro cita só a flag anterior e pede aspas. As seções que **saíram** do R contam como conhecidas (`removedSections`, porte do RN): um `--tls.cert_file` copiado de uma linha de comando do R depois de `--sink.workers=4` é nomeado, em vez de culpar o valor de antes. Uma flag que **começa com o nome de um parâmetro e continua** (`--sink.postgres.passwordhunter2`, ou com `-`, `_`, `.` no meio: o `=` esquecido) nunca é citada. Depois de uma flag que pede valor, o argumento seguinte só é nomeado como flag se tiver dois traços (`-h` é também uma senha de dois caracteres). Isso fecha, aqui, a brecha que o SP deixou aberta (TESTING §8 item 13 de lá), na medida descrita em TESTING §8 item 10 |
| chave desconhecida no INI | nomeada só se a seção for deste servidor ou do R, a chave tiver forma de chave (`[a-z0-9_]`) e a linha anterior **não** for um segredo: a continuação de uma senha que quebrou de linha lê como chave (porte do RN, 2026-10-13). Uma marca de ordem de bytes UTF-8 no começo do arquivo é ignorada |
| variável de ambiente do R que este servidor não tem (`RELPLOG_LISTEN_TLS`, `RELPLOG_TLS_*`, `RELPLOG_FILTER_*`, `RELPLOG_RATELIMIT_*`, `RELPLOG_SINK_TYPE`, `RELPLOG_RING_MODE`, as dos outros sinks e rings, `RELPLOG_SINK_POSTGRES_TLS*`, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE`) | **um** `WARN` que nomeia todas e diz que são ignoradas (`internal/config/removed.go`, §16.2). Não é erro porque um mesmo `.env` pode alimentar os dois servidores; sem o aviso, o env é lido só pelos nomes do registry e elas sumiriam em silêncio |

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
- **Resposta fatal à sonda** (senha errada, tabela dropada, privilégio retirado,
  §10.5): FATAL na primeira checagem, sem passar pelos fusíveis.
- **FATAL é terminal.** Toda transição e o gauge `relplog_sink_up` são escritos
  sob o mesmo lock, e nada sai de FATAL: uma checagem concorrente não pode
  devolver o estado a UP/DOWN depois de um `ClassFatal` do `Write()`.
- `ClassNetwork` no `Write()` **não** muda o estado; o pedaço é retentado.
- Em **DOWN**, vale o backpressure de §2.3 mesmo com o ring vazio (fail-fast): a
  transação RELP é segurada (§5.7). E o `/readyz` falha.
- Em **FATAL**: §12 e exit 3.
- **Os health checks carregam o tempo de vida de quem os chama**
  (`withLifetime`, correção da revisão do R): a validação do boot, que tem
  prazo próprio (§10.5), ainda vê um `SIGTERM` que chega depois do prazo da
  checagem.

### 13.4 Shutdown gracioso

`SIGTERM` ou `SIGINT`:

```
fase 1  /readyz passa a falhar
fase 2  espera shutdown.pre_stop_delay (0 s), ainda aceitando
fase 3  cancela o contexto de recepção e fecha os transportes ao mesmo
        tempo (um closeGrace no total): as transações decididas recebem a
        resposta, as seguradas ficam sem ela, sai serverclose e cada sessão
        fecha (§5.8)
fase 4  drena o ring para o sink, com shutdown.drain_timeout (30 s); depois
        para os workers e recolhe Workers.Pending() e o Drain() do ring
fase 5  o que sobrou vai para o dump (§12); fecha o sink e o ring; exit 0
        (ou 3, abaixo)
```

- O contexto de recepção é cancelado **antes** de os transportes fecharem: é o
  que solta as transações presas no laço de backpressure, sem resposta (§5.8).
  Um item já entregue ao ring não é afetado.
- **Nenhum `Enqueue` depois do `Drain`.** O `Close` de cada transporte só volta
  quando as sessões dele voltaram (com todas as transações decididas ou
  abandonadas e as respostas entregues ao socket; depois de `closeGrace`, 6 s,
  fecha o socket de quem não voltou).
  Por isso o `Drain` da fase 4 vê tudo o que entrou no ring: um item que
  entrasse depois dele ficaria num ring que ninguém mais lê, sem entrega, sem
  dump e sem contador.
- `drain_timeout` estourado com itens: §12 e exit 3.
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
- **A posse da contagem é de um lado só** (revisão independente do R,
  2026-10-11; no SP, o `abortLoss` ainda lê o ring até o dump começar). A
  fase 4 grava a ocupação do ring nesse contador e liga a
  "fase de dump" (`dumpPhase`) **antes** do `Drain`: dali em diante a ocupação do ring não mede
  mais nada (os lotes retidos ainda contam lá embora vão para o dump; os drenados
  já não contam). O aborto e o `dumpEmergency` (dump falho, desligado ou sem
  diretório) tomam o contador com uma troca atômica (`Swap(0)`): exatamente um
  deles conta aquelas mensagens. Antes, um aborto entre o `Drain` e o dump não
  contava nada, e um aborto durante um dump que falhava contava duas vezes.
- **E a troca é ordenada por um mutex** (`lossMu`, revisão independente de
  2026-10-13, o código do RN): o aborto marca `aborted` sob ele, e a entrada na
  fase de dump (`enterDumpPhase`, contador e fase juntos) e o acerto do
  contador com o que foi recolhido (`oweDump`) também, recusando-se depois de
  um aborto. Com dois atômicos havia duas janelas que contavam um dump falho
  duas vezes (10 para 5): um aborto entre os dois `Store`, e um aborto durante
  o recolhimento seguido do shutdown rearmando o contador. Um aborto que
  contou conta uma vez só (um segundo aborto não soma).
- **O dump toma o contador de volta sob o mesmo mutex** (`settleDump`,
  revisão independente do `go-syslog-server`, 2026-10-13, A2): o aborto
  publica `aborted` antes de tomar o contador, e um dump que via a marca
  nesse meio — desistia (`ErrAbandoned`) ou, desligado, nada tinha a
  escrever — tomava o contador primeiro: `emergency_failed = n` e
  `abort = 0`, a perda contada uma vez mas atribuída ao disco. Sob o
  `lossMu`, ou o aborto terminou (o dump acha 0) ou nem começou (o dump
  conta, o aborto acha 0) (`TestAbortInsideTheDumpKeepsItsReason`).
- **Dump abandonado depois do aborto** (`emergency.Store.SetAbandon`): o dump
  confere `aborted` antes de começar, a cada 1024 linhas e antes de publicar o
  nome final, e desiste removendo o temporário (`ErrAbandoned`). Publicado
  depois do aborto, ele seria reingerido no boot seguinte embora contado como
  perdido. Um dump que publica entre a última conferência e o aborto fica
  contado **e** no disco: uma sobrecontagem que o boot seguinte corrige
  entregando-o, nunca uma perda; nesse caso o `dumpEmergency` não conta mais
  nada (nem as malformadas que o dump deixou de fora: o aborto já as contou).
- **`SIGQUIT` tardio**: um `SIGQUIT` que chega depois de o shutdown começar e
  antes de ele registrar o canal de aborto só alcança o observador do boot, que
  marca a saída imediata; o shutdown confere essa marca antes de qualquer fase e
  sai com 130. Antes, esse `SIGQUIT` era perdido e o shutdown seguia gracioso.
  A conferência única corria com o observador (uma goroutine que podia marcar
  logo depois dela): o observador também fecha um canal (`quitNow`) que o laço
  do shutdown espera, e o `SIGQUIT` visto tarde ainda aborta (revisão
  independente de 2026-10-13, o código do RN).
- **Segundo sinal** em qualquer fase: saída imediata, `exit 130`, aceitando a
  perda (contada em `reason="abort"`). `SIGQUIT` aborta sempre, inclusive
  fora do shutdown: no boot e com o servidor no ar, saída imediata com 130.
- **Sink FATAL durante a fase 4** (o `SIGTERM` chegou com o sink UP e ele virou
  FATAL no dreno): a espera termina na hora, em vez de gastar o `drain_timeout`
  num dreno que não acontece mais, e a saída é **3**; o que sobrou vai para o dump.
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
| `0` | shutdown completo, ring vazio |
| `1` | erro de configuração ou de inicialização não relacionada ao banco (inclui a `LOGHUB_EB_URL` com `sslmode` que pede TLS) |
| `3` | banco indisponível ou inutilizável: sink FATAL, `boot_timeout` ou `drain_timeout` estourados, validação de §10.5 recusada, dump que falhou |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` (`DATADIR`, `MACHINE_ID`, `MACHINE_ID_FILE`, `AGENT_NAME`, `AGENT_UUID`, `HOSTNAME`, `WORKSPACE`; 112 = `Initialize()` duas vezes); a tabela completa está na skill `go-loghub-ident` e em `docker/README.md` |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 5–12 do boot. Um sinal que cancela o *bind* da porta admin (passo 5) ou do transporte (passo 12) é 130, não 1: com um nome em `admin.bind` ou `listen.tcp` a resolução falha com "operation was canceled", que não é endereço ruim (revisão independente de 2026-10-13, o código do RN). Com IP literal em `listen.tcp` nenhuma resolução falha e o *bind* passa: o sinal é conferido depois do *bind* e antes do primeiro *accept*, e também é 130 (antes o servidor abria, o `/readyz` dava 200 e o shutdown saía 0; revisão independente do `go-syslog-server`, 2026-10-13, A1; `TestSignalBeforeALiteralBindExits130`) |

---

## 14. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9103`, **o mesmo do `go-relplog-server` e do
`go-relplog-nats-server`**: os três são da mesma família e um operador pode
trocar um pelo outro sem mexer no monitoramento. Por isso **não rode dois
deles no mesmo host sem trocar o `admin.bind`** (o `go-webhook-server` usa
`9101`, o `go-syslog-server` e o `go-syslog-pg-server`, `9102`). HTTP/1.1.

| Path | Semântica |
|---|---|
| `/healthz` | `200` enquanto o processo vive. Liveness. |
| `/readyz` | `200` só se: sink `UP`, ring abaixo de `ring.high_water`, boot concluído (inclusive a reingestão), transporte aberto e não em shutdown. Senão `503` com o motivo em `text/plain`. |
| `/metrics` | exposição Prometheus em texto. |

### 14.1 Métricas

Prefixo `relplog_`, **o mesmo do pai**: um painel do `go-relplog-server` lê
este servidor. **Nenhum label deriva de conteúdo controlado pelo emissor** (nada
de hostname, appname, IP); todo label tem conjunto finito conhecido em
compilação. `facility` e `severity` **não** são labels.

| Métrica | Tipo | Labels |
|---|---|---|
| `relplog_messages_total` | counter | `transport` (tcp): frames `syslog` lidos |
| `relplog_messages_accepted_total` | counter | `transport` |
| `relplog_messages_dropped_total` | counter | `reason` (empty, unaddressable): só descartes **finais**; o que o RELP devolve ao emissor não está aqui (§5.4) |
| `relplog_messages_truncated_total` | counter | `transport` |
| `relplog_message_bytes` | histogram | — (tamanho original) |
| `relplog_convert_total` | counter | `format` (rfc5424, rfc3164, malformed) |
| `relplog_convert_warnings_total` | counter | `code` (os 14 de §6.9) |
| `relplog_convert_duration_seconds` | histogram | — |
| `relplog_conns_active`, `relplog_conns_total` | gauge, counter | `transport` (tcp) |
| `relplog_conns_refused_total` | counter | `transport` (tcp), `reason` (global, per_ip, proxy); `global` conta cada vez que o accept estaciona no teto, um limite inferior, porque o teto global bloqueia em vez de recusar |
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
| `relplog_sink_postgres_duplicates_total` | counter | — (linhas ignoradas pelo `ON CONFLICT (uuid)`) |
| `relplog_sink_postgres_pool_conns` | gauge | `state` (acquired, idle, max) |
| `relplog_sink_postgres_pool_waits_total` | counter | — |
| `relplog_events_written_total` | counter | — |
| `relplog_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero |
| `relplog_emergency_files`, `relplog_emergency_events_pending` | gauge | — (recontados do disco no fim da reingestão; somados por um dump, §12.2) |
| `relplog_build_info` | gauge | `version`, `commit`, `go` |
| `relplog_config_info` | gauge | `ring_mode` (sempre `memory`), `sink_type` (sempre `postgres`), `transports` (`tcp`) |
| `relplog_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

Saíram, com o que as produzia: `reason="filter"` e o label `tls` em
`transport`; `relplog_tls_handshake_errors_total`; `relplog_ring_quarantined_total`
e `relplog_ring_redis_claimed_total`; as séries de `sqlite`, `mysql`, `nats` e
`clickhouse`; `relplog_beacons_total`; `relplog_ratelimit_buckets` e
`relplog_ratelimit_held_total`. Uma série que nunca pode subir não é exposta:
diria ao operador que existe uma medição onde não existe nenhuma. Cada label tem
teste de exposição.

**Fronteiras de histograma** são estáticas, idênticas em toda instância e
**pré-formatadas como string** na inicialização: emitir `0.01` num scrape e
`0.010` no seguinte cria duas séries e quebra todo quantil.

### 14.2 Logs

`slog`, `json` por padrão (`text` disponível). **Nenhum log síncrono por mensagem
no caminho padrão.** `log.access = true` liga log por mensagem com
`log.access_sample` (0.01); em syslog isso é logar log. Avisos de anomalia
(conversão, PROXY, falha de aceite, backpressure) são **amostrados**, um a cada 30 s por tipo, com
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

O buffer do item volta ao pool **na mesma caixa `*[]byte`** de onde saiu
(`item.CopyRawBox`, `item.NewBoxed`, molde do SP): o item guarda a caixa, e
devolvê-la não aloca uma nova. A cópia da transação RELP (§5.6) é a outra
classe de buffer da recepção.

**Teto pequeno demais.** Cada item custa no mínimo 2 KiB (a menor classe) +
256 B na contabilidade de §9.2. Um `ring.max_bytes` abaixo disso é aceito pela
validação e nenhuma mensagem cabe: a transação é segurada, a sessão fecha no
`backpressure_timeout`, e o rsyslog reconecta para sempre. Herdado do SP,
conhecido e não corrigido (§22.1 item 4).

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
| qualquer outro (o ring em memória não tem; a regra fica para o caso de aparecer) | não há como provar que ninguém | **descarta sem devolver** (`Discard`), e a transação é segurada (§5.7) |

Nos workers: uma tentativa que termina por prazo ou cancelamento **contamina** o
pedaço (`Taint`), e nem o cru dos itens nem o buffer do pedaço voltam ao pool,
nem quando a retentativa seguinte tem sucesso: o pgx pode ainda estar
serializando os argumentos da tentativa anterior. O custo é uma alocação por
item no caminho de falha.

### 15.3 Memória do JLH, que não é do ring

O JLH vive entre o passo 2 e o passo 4 de §10.1, num buffer de pedaço. O teto é
`sink.workers × sink.batch_max_bytes` (4 × 16 MiB = 64 MiB no pior caso), mais
os arrays do `unnest`. Não é somado a `ring.max_bytes`: memórias com donos e
ciclos de vida diferentes, e somá-las esconderia o estouro de uma atrás da folga
da outra. Os buffers de pedaço têm pool próprio, de uma classe (o teto), e um
buffer que cresceu acima dela não volta. O dump (§12.1) converte item a item
num buffer de linha só, reaproveitado.

### 15.3a Memória da recepção RELP, que também não é do ring

Duas parcelas, as duas com teto:

- **Cópias das transações em voo** (§5.6): no máximo `relp.inflight_max_bytes`
  (64 MiB) para o processo inteiro, contadas pela capacidade real do buffer.
- **Custo fixo por conexão** (§5.2): cerca de 10 KiB (leitor de 4 KiB, escritor
  de 4 KiB, o primeiro bloco do frame) mais as goroutines do leitor e do
  escritor; um frame grande não deixa um buffer grande para trás. Com
  `tcp.max_conns` em zero (padrão), esse termo cresce com o número de conexões;
  quem espera milhares de emissores fixa o teto.

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
  (`RELPLOG_SINK_POSTGRES_ADDR`), com as exceções de §16.2.
- INI: seção `[ring]`, chave `capacity_items`.
- `--config=/caminho.ini` (env `RELPLOG_CONFIG`) indica o arquivo.

**Os mesmos nomes do pai.** O prefixo `RELPLOG_` e cada chave que ficou têm o
nome que tinham no `go-relplog-server`: os dois são da mesma família, e um
operador pode trocar um pelo outro mantendo o env das chaves comuns
(`TestRemovedEnvIsTheParentMinusThis` prova que todo nome lido aqui é lido lá).

### 16.2 Registry único, e os nomes de env fixados pelas skills

Uma **tabela** declara cada parâmetro (seção, chave, tipo, padrão, descrição, se
é recarregável, nome de env). Dela saem o INI, as flags, o env, o `--help`, o
`--dump-config` e a validação. Um parâmetro novo entra só ali. Sem
`gopkg.in/ini.v1` e sem `spf13/pflag`.

Nomes de env que **não** seguem a regra, porque uma skill os fixou:

| Seção | Env | Origem |
|---|---|---|
| `[syslog]` | `SYSLOG_<CHAVE>` (sem repetir a seção, e **com** o prefixo da skill, não o deste servidor: a mesma conversão do `go-syslog-server`, e um `.env` comum garante config de conversão idêntica entre os dois): `SYSLOG_DEFAULT_PRIORITY`, `SYSLOG_RFC3164_TIMEZONE`, `SYSLOG_MAX_MESSAGE_BYTES`, `SYSLOG_PAYLOAD_RAW`, `SYSLOG_LEGACY_CHARSET`, `SYSLOG_ACCEPT_MALFORMED`, `SYSLOG_SD_MAX_ELEMENTS`, `SYSLOG_SD_MAX_PARAMS` | skill `loghub-type-syslog-schema` §15 |
| `[loghub]` | `LOGHUB_EB_URL` (as outras duas do pai saíram com o beacon) | skill `loghub-json-schema` |
| `server.data_dir` | `DATADIR` | `go-loghub-ident` |

As variáveis de identidade (`AGENT_UUID`, `AGENT_NAME`, `WORKSPACE`, `HOSTNAME`,
`MACHINE_ID`, `MACHINE_ID_FILE`, `LOGHUB_IDENT_DEBUG`) **não** passam pelo
registry: são lidas pelo `go-loghub-ident`, e o `--dump-config` mostra o que o
**ambiente** diz numa seção informativa `[identity]`, sem resolver nada
(§13.1 passo 1: o dump não toca identidade nem disco).

**Erros de configuração não ecoam a entrada** (pendência 30 do S, corrigida
como no N): nenhum erro cita texto que possa ser segredo — URL que não parseia
(só o tipo do erro e a dica de codificar `%`, `/`, `#`, `?` e `,`), chave de
consulta PostgreSQL citada só se estiver na lista fixa da libpq, `sslmode`
inválido sem o valor, linha INI sem `=` ou cabeçalho malformado (só o número da
linha), nome de parâmetro desconhecido citado só se tiver forma de nome,
argumento solto citado só até o `=`. Nas flags, um argumento com forma de flag
nunca é valor de outra flag (`--sink.workers --sink.postgres.password=x` é
recusado, não engolido) e só é nomeado se for flag deste servidor; nada depois
de um valor (cauda partida pelo shell, valor depois de `= `) é citado. Um
argumento desconhecido com forma de flag logo depois de um valor
(`--sink.postgres.password=abc -hunter2`, `... abc --x=y`) pode ser a cauda desse
valor partida num espaço: é nomeado só quando cai numa seção que este servidor
tem (`--sink.postgres.tabel`, um erro de digitação); fora disso o erro cita só a
flag anterior e pede aspas (revisão independente, 2026-10-11: antes, a cauda
`-hunter2` saía no stderr). Os erros vão para o stderr e, num `SIGHUP`, para o
log.

**Chaves do pai que saíram.** Na linha de comando ou no INI são parâmetro
desconhecido, exit 1. No ambiente, onde o registry lê só os próprios nomes e
nunca varre o resto, elas sumiriam em silêncio; por isso o boot confere a lista
de `internal/config/removed.go` — exatamente as variáveis do registry do
`go-relplog-server` v1.0.0 que este servidor não lê (as de TLS, filtro,
rate-limit, `sink.type`, `ring.mode`, dos outros sinks e rings, as de TLS do
PostgreSQL, `LOGHUB_BEACON_INTERVAL` e `LOGHUB_CTL_NAMESPACE`) — e emite **um**
`WARN` que nomeia todas as definidas e diz que são ignoradas (§13.2, molde do
SP).

**Um `@` depois do primeiro `/`, `?` ou `#` da autoridade de uma URL é
recusado**, sem citar a URL: nenhuma URL destes esquemas tem `@` literal no
caminho, na consulta ou no fragmento, então é um usuário ou senha com `/`, `?`
ou `#` sem codificar, que encurta a autoridade (`postgres://app:39182#Zs3@db/x`
discava `app:39182` sem senha, com o `sslmode` jogado fora no fragmento). Uma
URL PostgreSQL com fragmento também é recusada.

Durações aceitam a sintaxe do Go (`30s`, `5ms`). O inteiro puro em segundos do
pai existia só para `loghub.beacon_interval` e saiu com ele.
Tamanhos aceitam **só** `KiB`, `MiB`, `GiB` (sem diferença de caixa) e inteiros
puros em bytes; `KB`, `MB`, `K` e `M`, que a standard aceitava, são recusados.
`syslog.rfc3164_timezone = Local` é recusado: faria hosts converterem de formas
diferentes (§3.5); vazio vale `UTC`.

### 16.3 Recarga por `SIGHUP`

Recarrega **só** o que é seguro trocar a quente e não muda os bytes do JLH:

- `log.level`, `log.access`, `log.access_sample`

(No pai entravam também `filter.*`, `ratelimit.*` e `sink.sqlite.keep_files`,
que saíram, §1.2; molde do SP.)

Toda chave de `[syslog]` **exige restart**: ela muda a conversão, e o JLH de um
item não pode depender de **quando** ele foi drenado (§3.5). A identidade fica
congelada.

Duas referências: chave recarregável comparada com o que está **em execução**
(para que reverter ao valor de boot seja aplicado); chave de restart comparada
com o **boot** (para que todo `SIGHUP` avise sobre ela até o restart). Cada uma
das duas sozinha falha num dos casos, como a standard aprendeu. **Sinônimos são
um valor só**: `windows-1252` e `cp1252`, `iso-8859-1` e `latin1` em
`syslog.legacy_charset` não são mudança (a aresta herdada do SP, corrigida na
revisão independente de 2026-10-13; `TestCharsetSynonymsAreOneValue`).

**Chave sombreada.** Como env e flag vencem o arquivo (§16.1) e não mudam num
processo em execução, uma chave que o arquivo define mas um env ou uma flag
decide **nunca** muda pelo arquivo: a recarregável não recarrega, e a de restart
não muda nem com o restart. Todo `SIGHUP` loga cada uma em `WARN`, com a origem
que manda (`env RELPLOG_LOG_LEVEL`), mesmo quando o resultado é "configuration
unchanged". Por isso o compose não define chaves recarregáveis por env: elas vêm
do INI montado (`docker/conf/relplog.ini`).

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
workers = 4

[sink.postgres]
addr     = db-1.intranet:5432
username = syslog
database = loghub
table    = loghub.inbox

[admin]
bind = 0.0.0.0:9103

[log]
level = info
```

A senha vai por env (`RELPLOG_SINK_POSTGRES_PASSWORD`) ou dentro da
`LOGHUB_EB_URL` — nesse caso sem `addr`, `username` e `database` no INI (§10.9).

### 16.5 Tabela de parâmetros

A fonte é `internal/config/registry.go`; o `--help` imprime a mesma tabela.

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `shutdown.drain_timeout`,
`relp.backpressure_timeout` e `proxy.header_timeout` — a validação recusa `0`
com exit 1. Zero é legítimo onde diz o que faz: `shutdown.pre_stop_delay` (sem
espera), `sink.batch_max_delay` (sem espera), `sink.fail_timeout` (sem fusível
por tempo) e `tcp.idle_timeout` (sem limite).

#### `[server]`

| Chave | Padrão | Descrição |
|---|---|---|
| `data_dir` | `./data` (env `DATADIR`) | Raiz de tudo o que o serviço grava, inclusive a identidade; convertida em absoluto; `/data` no contêiner |

#### `[listen]` — o único transporte, que não pode ficar vazio

| Chave | Padrão | Descrição |
|---|---|---|
| `tcp` | `:20514` | RELP em claro (§4.4); vazio é exit 1, não há outro transporte |
| `tcp_listeners` | `1` | 1–256; `>1` habilita `SO_REUSEPORT` |

Não há `tls` nem `tls_listeners` (§1.2).

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
| `max_conns` | `0` | por transporte; bloqueia no accept |
| `max_conns_per_ip` | `0` | por transporte, pelo endereço exato do par; fecha na hora |

#### `[tls]`

Removido no go-relplog-pg-server (§1.2).

#### `[proxy]`

| Chave | Padrão | Descrição |
|---|---|---|
| `protocol` | `off` | `off` \| `optional` \| `required` (§4.6) |
| `trusted` | `127.0.0.1/32,::1/128` | CIDRs dos balanceadores; largo demais permite forjar `src-ip` |
| `header_timeout` | `5s` | |

#### `[syslog]` — exige restart (§16.3)

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `default_priority` | `SYSLOG_DEFAULT_PRIORITY` | `13` | PRI de `pri-missing`/`pri-invalid` |
| `rfc3164_timezone` | `SYSLOG_RFC3164_TIMEZONE` | `UTC` | fuso IANA dos timestamps sem fuso; `Local` é recusado |
| `max_message_bytes` | `SYSLOG_MAX_MESSAGE_BYTES` | `64KiB` | truncamento (§5.3); 480 a 16 MiB; também o tamanho máximo da cópia de uma transação em voo; pior JLH = 12 × isto + 8 KiB |
| `payload_raw` | `SYSLOG_PAYLOAD_RAW` | `true` | `raw` quando não é obrigatório |
| `legacy_charset` | `SYSLOG_LEGACY_CHARSET` | `""` | `cp1252` \| `latin1`, sinônimos `windows-1252` \| `iso-8859-1` (§6.7) |
| `accept_malformed` | `SYSLOG_ACCEPT_MALFORMED` | `true` | `false` descarta `malformed` no worker e no dump |
| `sd_max_elements` | `SYSLOG_SD_MAX_ELEMENTS` | `32` | §6.11 |
| `sd_max_params` | `SYSLOG_SD_MAX_PARAMS` | `64` | §6.11 |

#### `[filter]` — recarregável

Removido no go-relplog-pg-server (§1.2).

#### `[ratelimit]` — recarregável

Removido no go-relplog-pg-server (§1.2).

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `capacity_items` | `65536` | teto de itens, 1–16 777 216; com mensagens de 200 bytes é o que pega primeiro |
| `max_bytes` | `512MiB` | teto de bytes: o `cap` de cada buffer cru + 256 B por item (§9.2); o JLH não conta |
| `high_water` | `0.80` | fração de qualquer teto acima da qual o `/readyz` falha |

Não há `mode` (sempre `memory`) nem `max_inflight` (era do `ring=none`).

#### `[ring.sqlite]`

Removido no go-relplog-pg-server (§1.2).

#### `[ring.redis]`

Removido no go-relplog-pg-server (§1.2).

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `workers` | `4` | goroutines de dreno, que também convertem; cada uma prende uma conexão; 1–1024, `WARN` acima de 16 |
| `batch` | `256` | itens por `Dequeue` e linhas por `INSERT`; 1–65 536 |
| `batch_max_bytes` | `16MiB` | teto de JLH por pedaço; no máximo 256 MiB (o `Bind` de um pedaço maior passaria do 1 GiB da mensagem da libpq) |
| `batch_max_delay` | `5ms` | espera máxima para completar um lote; `0` = sem espera |
| `op_timeout` | `5s` | prazo de uma escrita, da discagem e da validação do boot |
| `health_interval` | `2s` | período da checagem; também a latência de UP→DOWN |
| `health_timeout` | `1s` | prazo de uma checagem |
| `fail_timeout` | `30s` | DOWN antes de FATAL; `0` = sem fusível por tempo |
| `max_retries` | `10` | checagens falhas antes de FATAL |
| `boot_timeout` | `300s` | quanto o boot espera o PostgreSQL antes de sair com 3 |
| `retry_backoff_min`, `retry_backoff_max` | `100ms`, `2s` | backoff de um pedaço que falhou |

Não há `type` (sempre `postgres`).

#### `[sink.nats]`

Removido no go-relplog-pg-server (§1.2).

#### `[sink.redis]`

Removido no go-relplog-pg-server (§1.2).

#### `[sink.sqlite]`

Removido no go-relplog-pg-server (§1.2).

#### `[sink.postgres]` e `[sink.mysql]`

Só `[sink.postgres]`; `[sink.mysql]` foi removido (§1.2). Não há chaves de TLS
(`tls`, `tls_ca`, `tls_insecure`): a conexão é sempre `sslmode=disable` (§10.5).

| Chave | Padrão | Descrição |
|---|---|---|
| `addr` | `127.0.0.1:5432` | `host:port`, ou o diretório de um socket unix começando com `/` (com `:porta` opcional) |
| `username` | `syslog` | o papel; precisa de `INSERT` na tabela e `SELECT` na coluna `uuid`, nada mais |
| `password` | `""` | redigida no `--dump-config` |
| `database` | `syslog` | |
| `table` | `loghub.inbox` | a fila (§3.7.1), `nome` ou `esquema.nome` em minúsculas; nunca criada, só validada |
| `pool_size` | `0` | teto do pool, de `sink.workers + 1` (a sonda de saúde precisa de uma conexão a mais) a 4096; `0` deriva `sink.workers + 1`, e o `--dump-config` o escreve comentado |

#### `[sink.clickhouse]`

Removido no go-relplog-pg-server (§1.2).

#### `[loghub]`

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `eb_url` | `LOGHUB_EB_URL` | `""` | só `postgres://` e `postgresql://`, sem `sslmode` que peça TLS; redigida no dump (§10.9) |

`beacon_interval` e `ctl_namespace` saíram com o beacon (§11).

#### `[emergency]`, `[admin]`, `[log]`, `[shutdown]`

| Chave | Padrão | Descrição |
|---|---|---|
| `emergency.enabled` | `true` | escreve o dump JSON Lines (§12); a reingestão independe dele |
| `emergency.dir` | `<data_dir>/emergency` | criado e provado gravável no boot |
| `admin.enabled` | `true` | serve `/healthz`, `/readyz` e `/metrics` |
| `admin.bind` | `127.0.0.1:9103` | a mesma porta do pai (`go-relplog-server`) e do irmão (`go-relplog-nats-server`): não rode dois deles no mesmo host sem trocar (§14) |
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
`docker/conf/` ali e define `RELPLOG_CONFIG=/etc/relplog/relplog.ini`, que traz
as chaves recarregáveis, §16.3), `/tmp` é tmpfs. `WORKDIR /data`; o binário é
invocado por `/app/go-relplog-pg-server`. Não há `/etc/relplog/tls`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-relplog-pg-server/           ESTE módulo
  go-relplog-server/              o pai (R): RELP em claro e sobre TLS, rings × sinks, filtro, rate-limit, beacon
  go-syslog-pg-server/            o molde do corte (SP): o go-syslog-server cortado para memória -> PostgreSQL
  go-relplog-nats-server/         o irmão (RN): o mesmo corte do R, com o destino NATS
  go-syslog-server/, go-syslog-standard-server/, go-syslog-nats-server/, go-webhook-*/
```

Módulo independente. Nada é importado de outro módulo do monorepo, nem do pai:
compartilhar pacotes faria os módulos evoluírem amarrados. Tags
`go-relplog-pg-server/vX.Y.Z`. O prefixo `rlpg-` dos projetos, contêineres e
volumes Compose é **deste** módulo (o R usa `rls-`, o SP `slpg`).

**O que é cópia de quem.** `receiver/` (o RELP, sem TLS), `obs/`, `config/`
(registry e validação do `[relp]`, `eburl.go`, `flags.go`), `app/` (shutdown,
contagem do aborto, `XTransport`) e `sink/postgres.go` (prazo próprio da
validação, `23502` fatal) são do R; o corte — `ring/` só memória, `item/` sem
codec e sem TLS, `emergency/` JSON Lines com `ParseLine`, `sink/` só
PostgreSQL sem TLS com o ambiente libpq escondido, `config/removed.go`, a
reingestão em fluxo — é o do SP. Os pacotes compartilhados com o RN (`item`,
`ring`, `receiver`, `convert`, `emergency`, `sink` workers/instrumented/health,
o boot e o shutdown da `app`, `obs`) diferem dele só onde o SP e o SN diferem.
Uma correção feita num dos lados vale para os outros: confira os irmãos.

### 17.2 Layout interno

```
cmd/go-relplog-pg-server/main.go  flags que encerram, lhident.Initialize(), chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão, admin; XTransport
  config/     registry único -> INI + flags + env + --help + dump; validação;
              LOGHUB_EB_URL (só postgres://); o aviso das variáveis do pai (removed.go)
  ident/      Identity (congelada no boot) e a gramática de space          [lógica pura]
  jlh/        escritor JSON por append, Event, cabeçalho do envelope, UUID,
              Payload (o corte por posição)                                [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH (+ x-transport)          [lógica pura]
  item/       Item, Head, pools de buffer (sem codec)                       [lógica pura]
  jlhcheck/   SÓ TESTE: os schemas e os vetores copiados das skills e o
              oráculo de conformidade (§18.2); nenhum arquivo de produção o importa
  receiver/   RELP: moldura (relpframe), sessão, janela, teto em voo e
              escritor de respostas (relp, budget), conexões e shutdown
              (stream), tcp, proxy, limites, ingest
  ring/       interface Ring + memory
  sink/       interface Sink + postgres, workers, chunk, health, instrumented
  emergency/  dump JSON Lines, ParseLine, Count e reingestão
  obs/        métricas Prometheus próprias + slog
```

`convert/`, o leitor de frames RELP e o leitor do PROXY em `receiver/` e
`emergency.ParseLine` são os parsers de entrada adversária. Os alvos de fuzz
estão em §18.3.

### 17.3 Dependências

**Três em produção, e ponto.**

| Dependência | Para quê |
|---|---|
| `github.com/jackc/pgx/v5` v5.11.0 | o sink, pela API nativa (`pgxpool`, `pgconn.PgError`) |
| `github.com/patrickbrandao/go-loghub-uuidv7` v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` v0.5.2 | identidade (sem dependência nova: usa a `go-loghub-uuidv7`) |

Só em teste: `github.com/santhosh-tekuri/jsonschema/v6` (contratos, §18.2).

O pai tinha oito; saíram `rueidis`, `modernc.org/sqlite`, `go-sql-driver/mysql`,
`nats.go` e `clickhouse-go/v2`, com as transitivas de cada um. O binário caiu de
31,9 MB para 17,2 MB (build `darwin/arm64`, Go 1.27.1, as mesmas flags nos
dois). O RELP não tem biblioteca: é stdlib. `golang.org/x/net`, `golang.org/x/sys`
e `golang.org/x/text` **não** são importados por este código: `SO_REUSEPORT` e o
charset legado são feitos com a stdlib (`syscall`) e com tabela própria. O
`golang.org/x/text` ainda aparece no binário, **transitivamente**, pelo
SASLprep (`secure/precis`) do `pgx`; nenhum pacote daqui o usa.

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
  pelo nome. **O vetor com `transport=tls` roda reetiquetado como TCP** (molde
  do SP): os mesmos bytes recebidos por TCP simples, com `payload.tls` retirado
  do esperado, `transport = "tcp"` e o `sid` recalculado pela fórmula de §6.10.
- **Ordem de chaves:** o JLH serializado tem os nove campos e o payload na ordem
  de §3.1 e §3.2.
- **Schemas:** todo envelope produzido nos testes (vetores, corpus, fuzz,
  integração, linhas do dump, linhas da fila) passa nos dois schemas, com `santhosh-tekuri/jsonschema/v6` e
  `AssertFormat` ligado (os formatos `ipv4`/`ipv6`). Os exemplos `valid/` passam
  e os `invalid/` falham, para provar que o validador está ligado.
- **Regras semânticas** (a porta em Go do `validate_payload.py`):
  `priority = facility.code × 8 + severity.code`; nomes RFC 5427; `level =
  severity.code`; `sid` recalculado de `payload.host`; `event.iso` = `uts` + `sub`
  truncado; instante do `uuid` = `event` (tempo real); `size ≥` octetos de `raw`
  (maior só com `truncated`); `raw-encoding` coerente com `raw`.

**Por que não validar em execução** (decisão 14 do S): o serializador é um só, é
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
| `FuzzRead` | `emergency` | o leitor do dump JSON Lines tolera truncamento e lixo sem pânico, nunca guarda uma linha acima do teto, e tudo o que `ParseLine` aceita a fila aceitaria |
| `FuzzJSONBSafe` | `sink` | a troca do `\u0000` para a `loghub.inbox` (§3.7.1) devolve JSON válido igual ao original com U+0000 → U+FFFD, e nada mais |
| `FuzzInboxPayload` | `sink` | o payload da coluna é JSON sem escape NUL, `raw` volta exato e os demais textos só trocam U+0000 por U+FFFD |

O `FuzzHead` do R saiu com o codec do cabeçalho (§3.4).

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=10m
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=10m
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

A pilha **real** (a mesma `run()` do binário), contra um PostgreSQL real, por
sessões RELP de um cliente de teste que fala como o `librelp`: como o único
destino é a fila, todo teste que passa do passo 7 do boot precisa de
`RELPLOG_TEST_PG_DSN` (§18.6) e pula sem ela; só as recusas de antes do passo 7
(exit 1) rodam sem banco. Cada teste cria a própria fila a partir de
`docker/postgres/inbox.sql`, num esquema próprio. Falhas do destino são
injetadas decorando o sink real (`App.wrapSink`). Todo envelope lido de volta
passa pelo oráculo e tem `x-transport: "relp"`, `transport: "tcp"` e
`framing: "octet-counting"`. Cobre: o transporte chega à fila (com as colunas e
o `event_at` da recepção), PROXY v1 e v2, `200 OK` só depois do ring, ring cheio
e sink DOWN segurados e devolvidos sem `500`, fidelidade do JLH (o `raw` volta
byte a byte, com NUL no meio), `SIGHUP` de `[log]` (ida e volta, arquivo
inválido), dump de emergência e reingestão exatos (mesmo `uuid`, mesmo JLH, o
`from` de quem recebeu), dump danificado em `.corrupt`, gauges recontados,
contagem do aborto com um dono só, shutdown gracioso sem perda, segundo sinal
(130), nenhum transporte (exit 1), chaves do pai na linha de comando (exit 1) e
no env (`WARN`), códigos 100–114 do `go-loghub-ident` num subprocesso, e a
ordem do boot.

### 18.6 Destino real

A suíte do PostgreSQL pula sem a variável e cria a fila **a partir do arquivo que
o operador aplica** (`docker/postgres/inbox.sql`), num esquema próprio por teste:

```bash
RELPLOG_TEST_PG_DSN='postgres://rlpg:rlpg@127.0.0.1:15491/rlpg_test?sslmode=disable' \
  go test -race -timeout 900s ./internal/sink/ ./internal/app/
```

O DSN de teste também é sem TLS (`sslmode=disable`). O que ela prova está em
`docs/TESTING.md` §7.

Nenhuma ferramenta dos testes ou do Compose usa `latest`: a imagem do destino
fixa a versão (`postgres:18-alpine`).

### 18.7 Emissores vivos

`RELPLOG_TEST_EMITTERS=1` (com `RELPLOG_TEST_PG_DSN`) sobe o **rsyslog real com
`omrelp`** (Alpine, pacote `rsyslog-relp`) em contêineres descartáveis contra o
servidor em processo. É a única prova de que a leitura do `librelp` que
fundamenta §5.4 vale no fio:

| Cenário | Prova |
|---|---|
| RELP em claro, N mensagens | N linhas na fila, uma vez cada, todas com `x-transport: "relp"`, nenhum `500` |
| ring cheio além do `backpressure_timeout` | a sessão fecha sem `500`, o rsyslog reconecta e retransmite: **todas** as mensagens chegam |
| shutdown do servidor com transações em voo e o destino sem confirmar, e um servidor novo no mesmo `data_dir` | o que tinha `200` vai para o dump (exit 3) e é reingerido; o resto o rsyslog retransmite: enviadas = linhas na fila, sem perda e sem duplicata |

Os cenários do R com TLS e com o `500` final não existem aqui (§1.2, §5.4).

### 18.8 Critérios de aceite v1

**Nunca medidos ponta a ponta.** Alvo, não fato; a primeira medição substitui
esta tabela. A peça de recepção é a do R (`BenchmarkRELPSession`: uma sessão
sobre loopback, janela cheia, `200` depois do ring em memória, sem sink), medida
de novo aqui (`docs/TESTING.md` §5).

| Cenário | Alvo |
|---|---|
| RELP, uma conexão do rsyslog, janela 128, mensagens de 200 B | ≥ 80k msg/s (a confirmação custa) |
| Perda com `SIGTERM` durante carga | **zero**, reconciliada contra a fila (§18.7) |
| Perda com FATAL do sink | **zero**, via dump e reingestão |

## 19. Matriz de garantias

Leia junto com §2.3 e §5.4: o `200 OK` de cada transação RELP significa:

| Combinação | A confirmação significa | Perda em `kill -9` | Perda em `SIGTERM` | Perda em FATAL do sink |
|---|---|---|---|---|
| `memory` → `postgres` | aceito na RAM deste nó | o backlog do ring | nada, se o dreno couber em `drain_timeout`; o resto vai para o dump | nada: o dump (§12) |

Depois do `INSERT` confirmado, a durabilidade é a do PostgreSQL
(`synchronous_commit`; uma fila `UNLOGGED` perde tudo numa queda dele, §10.5). Um dump que falha
(§12.3) é perda, contada em `reason="emergency_failed"`; uma recusa `logic` da
fila depois do `200` também, em `reason="sink_logic"` (§2.3). Quem precisa de
`kill -9` sem perda usa o `ring=sqlite` do `go-relplog-server`.

**Duplicatas (decisão 3 do R).** O RELP não carrega id de mensagem: uma
retransmissão do rsyslog chega como mensagem nova, com `uuid` novo, e o JLH não
a deduplica por `(uuid, step)`. Ela acontece quando uma mensagem **já aceita**
não teve o `200 OK` processado pelo cliente: corte abrupto (`kill -9`, rede
caindo), escrita de respostas que falhou, ou uma transação decidida depois de a sessão
fechar. Um reenvio que chega à fila com o **mesmo** `uuid` (retry do worker,
reingestão) é absorvido por `ON CONFLICT (uuid)`; uma retransmissão do rsyslog,
com `uuid` novo, não.
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
| `500` (só a guarda de origem, que não dispara num par TCP) | **descarta e loga**; suspende a ação e retransmite a janela | contou em `messages_dropped_total{unaddressable}` |
| sem resposta (sessão fechada) | retransmite na sessão seguinte | contou em `relp_unanswered_total` |

**Um erro permanente que a tabela de §10.5 não conhece é `network`, para
sempre** (revisão independente de 2026-10-13, por desenho): uma trigger que
levanta exceção (`P0001`) ou qualquer outro código fora da tabela é retentado
com o sink UP, e o ring enche e segura as transações. É a escolha de §20.18 —
um ring que enche é visível, um pedaço descartado não —, e o operador vê
`relplog_sink_errors_total{kind="network"}` subir, com o SQLSTATE no `WARN` "sink write failed, retrying" (na primeira tentativa e a cada dez).

---

## 20. Invariantes

Se uma alteração conflita com um destes, ela está errada.

1. O `200 OK` nunca sai antes de o item estar no ring.
2. Ring cheio e sink DOWN nunca viram `500` nem descarte: a transação é
   segurada e, se não couber a tempo, devolvida ao emissor pelo fim da sessão.
   Um `500` só vai para uma recusa **final** e contada.
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
7. Nenhum sink altera o JLH. A coluna `payload` é o sub-slice do JLH ou, com NUL,
   uma cópia reconstruída; o JLH do pedaço e o do dump são íntegros.
8. Os endereços do payload são a ponta real do transporte, ou o cliente
   declarado por um PROXY **confiável**, e então o par real fica em `x-proxy`. O
   `hostname` é sempre alegação do emissor e nunca vira chave de decisão.
9. Nenhum label de métrica deriva de conteúdo controlado pelo emissor.
10. Nenhum log síncrono por mensagem no caminho padrão; nenhum payload em log de
    `INFO` ou acima.
11. Removido no go-relplog-pg-server (era do rueidis, §1.2).
12. O JLH é idêntico no worker e no dump: a mesma conversão, com a mesma config
    congelada.
13. **O JLH é função de cabeçalho + cru + config de conversão congelada** (§3.5):
    mesmo `uuid`, mesmos bytes, em retry e dump. A reingestão não converte.
14. **Uma amostra de relógio por mensagem**, e dela saem o `uuid` e o `event`.
15. `ring` e `sink` são ortogonais: nenhum consulta o outro.
16. Toda escrita em disco fica sob `server.data_dir` (`/data` no contêiner),
    inclusive a identidade, **por padrão**: os caminhos derivados ficam lá, sem
    exceção fixa. Um `emergency.dir` absoluto fora dele é decisão do
    operador (um disco mais rápido), aceita com `WARN` no boot (§13.2).
17. O servidor nunca cria o que valida: o sink nunca faz DDL na fila.
18. Esquema e permissão são `fatal`, nunca `logic` (classe `42` do SQLSTATE,
    `42501`, `28`, `3D`, `23502`); erro desconhecido é `network`, nunca `logic`.
19. Removido no go-relplog-pg-server (era do `nats.go`, §1.2). O princípio fica:
    nenhuma biblioteca cliente guarda nem retenta o que o ring guarda e o worker
    retenta.
20. Nenhuma linha de dump que a fila poderia recusar chega ao `INSERT`:
    `emergency.ParseLine` a recusa antes (§12.2). (No R: nenhum destino recebia
    uma mensagem que o teto dele recusaria.)
21. O servidor nunca emite `beacon`, `ping`, `pong` nem sonar.
22. `lhident.Initialize()` roda uma vez por processo, no `main`; nenhum pacote lê
    identidade em `init()`.
23. **A resposta de uma transação decidida nunca é descartada**, e o socket de
    uma sessão só fecha depois de o escritor entregar todas elas (ou de
    `closeGrace`).
24. **As cópias das transações em voo têm teto global** (`relp.inflight_max_bytes`)
    e são feitas só depois da vaga na janela e no teto.
25. Todo envelope tem `x-transport: "relp"`, `framing: "octet-counting"` e
    `transport` `tcp`.

---

## 21. Roteiro pós-v1

Nada disto entra antes de a v1 estar em produção e §18.8 ter sido medido.

| Item | Motivo de ter ficado fora |
|---|---|
| `relp` no enum de `transport` da skill | Pedir à skill; até lá, `x-transport` (§3.2) |
| Comandos RELP de extensão | Nenhum cliente conhecido usa |
| `space` por listener ou rede | Multi-tenant numa instância; fora da v1 |
| Resposta a sonar, ping e pong | Até existir um `go-loghub-driver` |
| TLS (listener ou cliente) | Fora por decisão do usuário (§1.2); voltar exige decisão nova, não só código |
| Particionamento por tempo na fila | Precisa de decisão sobre quem cria a partição |
| Ferramenta de reingestão manual de um `.jsonl` (ou de um `.corrupt` reparado) | Hoje basta copiar o arquivo para `emergency.dir` com o nome `emg-*.jsonl` e reiniciar |

---

## 22. Lacunas e riscos

Os riscos do desenho estão em `docs/DECISOES.md` §5, e o que os testes ainda não
provam, em `docs/TESTING.md` §8. Esta seção guarda o que o **código atual** ainda
faz diferente do que deveria. Cada item diz o problema, a consequência e a
direção da correção, que segue a regra de sempre: a spec primeiro (a regra entra
na seção indicada), depois o código e o teste. Corrigido um item, ele sai daqui.

**As 30 pendências do `go-syslog-server`** (§22.1 da SPEC dele, 2026-10-11)
chegam aqui pelos dois pais. As que tocam o que este módulo tem foram corrigidas
no R e vieram com o código dele: 1 (§5.2), 2 (§4.4, decisão 16 do R), 3 (§4.6),
4 (§6.5.1, decisão 16 do R), 6–9 (§10.5), 22 (§10.9, §13.2), 23–25 (§12.1,
§12.2), 27 (§13.1), 28–29 (§13.4), 30 (§16.2). As outras eram de peças cortadas
e **não se aplicam**: 5 (o Redis, §9.5, §10.3), 10–12 (o MySQL, §10.6), 13–20
(o NATS, §10.7), 21 (o beacon, §11) e 26 (o `sink=none`, §1.2).

**As 10 pendências do `go-syslog-pg-server`** (§22.1 da SPEC dele, 2026-10-12)
estão todas corrigidas aqui, porque o R já as tinha corrigido no código que veio
para este módulo: 1, o teto por IP no IPv6, decidido como está (§4.4, decisão 16
do R); 2, o shutdown contado como falha do PROXY (§4.6); 3, o custo fixo de
64 KiB por conexão, hoje 4 KiB de leitura e 4 KiB de escrita (§5.2); 4, o offset
da RFC 3164, decidido seguir a skill (§6.5.1); 5, o `23502` fatal (§10.5); 6, a
validação do boot com prazo próprio (§10.5, §13.1); 7, os erros da
`LOGHUB_EB_URL` sem eco (§10.9, §16.2); 8, os logs depois de um `fsync` de
diretório que falhou (§12.1, §12.2); 9, os gauges recontados do disco (§12.2,
§14.1); 10, o `SIGQUIT` nunca tolerado no shutdown por FATAL (§13.4). O SP
continua com elas: é material da lista "a portar".

### 22.1 Pendências conhecidas

Atualizado na revisão independente de 2026-10-13. O que ficou fora por decisão
(o TLS, os outros rings e destinos, o filtro, o rate-limit e o beacon) está em
§1.2 e em `docs/DECISOES.md`; o que não foi medido (carga, fuzz de 10 minutos),
em `docs/TESTING.md` §8. As arestas abaixo são conhecidas e **não** corrigidas;
cada uma diz o cenário e por que ficou.

1. **`SIGHUP` relata a chave derivada junto com a que a deriva** (herdada do
   SP). Trocar `sink.workers` no INI e mandar `HUP` loga "requires a restart"
   para `sink.workers` **e** para `sink.postgres.pool_size` derivado; trocar
   `server.data_dir`, para ele e para `emergency.dir`. É verdade (as duas mudam
   no restart), só redundante; separar derivadas de explícitas na comparação
   de §16.3 custa mais que o ruído. (Os sinônimos do charset, a outra metade da
   aresta do SP, foram corrigidos: §16.3.)
2. **`listen.tcp` e `admin.bind` na mesma porta** só falham ao abrir o
   transporte (passo 12), com exit 1 e a porta admin já tendo servido o boot
   inteiro (herdada do SP). Comparar os dois endereços na validação exigiria
   resolver nomes antes do boot.
3. **Uma falha ao remover um dump já entregue** é só `ERROR`, e o arquivo é
   entregue de novo no boot seguinte, deduplicado enquanto a fila guardar os
   `uuid` (herdada do SP; §12.2). Sair com erro deixaria o boot em laço por um
   problema de permissão que não impede a entrega.
4. **`ring.max_bytes` aceita valores em que nenhuma mensagem cabe** (menor que
   a classe do pool de `syslog.max_message_bytes` mais `ItemOverhead`): toda
   transação fica segurada até o `backpressure_timeout` (herdada do SP). A
   recusa precisaria da tabela de classes do pool na config; fica para quando
   alguém pedir.
5. **Os contadores da recepção não têm *padding*** e disputam a linha de cache
   entre listeners `SO_REUSEPORT` (herdada do SP). Nunca medido como gargalo.
6. **Aborto durante o `pre_stop_delay`** (`SIGQUIT`, ou o segundo sinal, na fase
   2 do shutdown): o `abortLoss` lê a ocupação do ring enquanto o transporte
   ainda aceita, e uma transação que entra no ring entre a leitura e o
   `os.Exit` fica sem contagem (herdada do R; revisão de 2026-10-13, A9). A
   janela é de microssegundos e o processo está saindo; fechá-la pediria parar
   a recepção antes de contar, o que mudaria a semântica do `pre_stop_delay`.
7. **`tcp.idle_timeout` menor que `relp.backpressure_timeout`**: uma sessão com
   uma transação segurada e nenhum frame novo fecha pelo idle, e a transação
   conta em `relp_unanswered_total{reason="server_closed"}`, não
   `backpressure` (herdada do R; revisão de 2026-10-13). O efeito no emissor é o
   mesmo (retransmite); só o rótulo erra.
8. **Erro permanente fora da tabela de SQLSTATE** (`P0001` de uma trigger, por
   exemplo) é `network` para sempre, com o sink UP (§19). Por desenho
   (§20.18).
9. **Um dump publicado no instante do aborto** (entre a última conferência do
   `SetAbandon` e o `os.Exit`, §13.4) fica contado como perdido **e** no disco:
   o boot seguinte o entrega, e `reason="abort"` sobreconta. Nunca perda.
10. **`relplog_sink_errors_total{kind="fatal"}`** conta uma escrita fatal mesmo
    quando o boot já foi cancelado (`Instrumented.write`); cosmético (revisão
    de 2026-10-13, K4).
