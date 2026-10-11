# go-syslog-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-05
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-syslog-server`
Binário: `go-syslog-server`
Entrada: `docs/DECISOES.md` (entrevista de 2026-10-05). Cada decisão de lá está
aqui, com o número da seção.
Revisão: 2026-10-05 — decisões tomadas na implementação, registradas nas seções:
`uuid` cunhado depois de filtro e rate-limit e reaproveitado na retentativa
(§3.1); NUL e quadros vazios (§5.3, §5.6); origem não endereçável (§4.2);
detalhes do PROXY protocol (§4.6); nomes de facility estritos (§7); ring Redis
em Redis Cluster, contador e leitura em queda (§9.5); sinks preguiçosos e réplica
do PostgreSQL (§10.3, §10.5); chaves "donas" da `LOGHUB_EB_URL` (§10.9); dump
danificado (§12.2); `--dump-config` sem tocar a identidade (§13.1); gramática de
tamanhos e fuso `Local` (§16.2). Depois da revisão independente: desvios D6
ampliado, D7 e D8 (§1.3); `uuid` conferido contra a amostra no codec (§3.4);
`{space}` do stream Redis por envelope (§3.6); contabilidade do ring por modo
(§9.2); disponibilidade dos rings `sqlite` e `redis` pela escrita (§9.4, §9.5);
entrega do boot por chunk (§13.1); aliases do charset legado (§6.7); erro
durante a autodetecção (§5.2); filtro e rate-limit uma vez por mensagem e recusa
`logic` final no `ring=none` (§5.7); prazos de escrita do TLS (§4.5); remoção
só de socket datagrama órfão (§4.3); erro de `Dequeue` não encerra o worker
(§10.1); entrega parcial (§10.2); nulidade no PostgreSQL, sonda e `read_only` no
MySQL (§10.5, §10.6); `no_ack` e JetStream desligado (§10.7); FATAL terminal
(§13.3); teto do ring pela mesma população (§9.2); sinais e FATAL no dreno
(§13.4, §13.5); conteúdo do TLS, `admin.bind`, `NaN` e nomes de host no passo 1
(§13.1, §13.2); chaves sombreadas no `SIGHUP` (§16.3); caminhos fora de
`data_dir` (§20.16).
Revisão: 2026-10-06 — `sink=postgres` passa a enfileirar na **`loghub.inbox`** da
skill `loghub-postgres-table-inbox` em vez de uma tabela `syslog_events` própria
(§1.3, §3.7, §3.7.1, §10.5, §16.5, §18.6): `ON CONFLICT (uuid)`, payload em
`jsonb` (com NUL: `raw` em base64 e U+FFFD nos demais textos), validação do catálogo pelas dez
colunas que o servidor escreve, PostgreSQL 18 no Docker. SQLite e MySQL não
mudam. Na revisão independente dos webhooks do mesmo dia: `timestamptz(6)`
aceito e precisão menor recusada; outro índice único ou constraint de exclusão
recusados no boot (§10.5).
Revisão: 2026-10-07 — `sink=clickhouse`, o sétimo destino (§3.7.2, §10.10,
§17.3), na mesma rodada dos servidores de webhook: a tabela `inbox_syslog`
espelha a `loghub.inbox` num `ReplacingMergeTree` ordenado por
`(event_at, uuid)`, com o `payload` no tipo `JSON` e `structured-data` tipado
como `Map` (o tipo `JSON` descarta objeto vazio), uma tabela por formato de
payload;
protocolo nativo do `clickhouse-go` v2 (a oitava dependência);
`insert_deduplication_token` por pedaço; `async_insert` e a durabilidade ficam
com o administrador do ClickHouse; as combinações passam a 28 (§2.1, §19).
Revisão: 2026-10-11 — regras de correções já presentes no código registradas
onde valem (§4.3, §8, §9.4, §13.4, §17.1, §18.6), e o que o código atual ainda
faz de errado numa seção própria, com a direção de cada correção (§22).

Servidor de ingestão de syslog do ecossistema **LogHub**. Recebe mensagens por
UDP, TCP, TLS e socket unix, converte cada uma num envelope **JLH** com
`type=syslog` e o entrega a um destino configurável — por padrão, o eventbus
NATS JetStream em `loghub.{space}.syslog`. É um **Bridge-IN**: cria a primeira
mensagem de cada fato no eventbus (`step = 0`).

Esta especificação foi escrita do zero. O `go-syslog-standard-server` (o antigo
`go-syslog-server`, com envelope próprio e RELP) é a referência de onde vêm
desenho, código e lições; onde esta spec repete a dele é de propósito, e onde
diverge, a divergência é justificada no lugar.

---

## 1. Escopo

### 1.1 Objetivos

- Máximo de mensagens por segundo com mínimo de CPU e memória.
- Cada mensagem aceita vira **exatamente um** envelope JLH válido pelos schemas
  das skills `loghub-json-schema` e `loghub-type-syslog-schema` (§3).
- **Nunca confirmar** uma mensagem que não foi aceita segundo a garantia da
  combinação `ring` × `sink`, nos transportes que têm como confirmar (§2.3).
- Backpressure explícito onde o transporte permite; perda **contada e visível**
  onde ele não permite.
- **Sem perda e sem invenção** no payload: o que chegou está nos campos ou em
  `raw`; o que não chegou é omitido, nunca adivinhado (§6).
- Operar acoplado ao eventbus (`sink=nats`, `sink=redis`), de forma autônoma
  (`sink=sqlite`) ou contra um banco compartilhado (`sink=postgres`,
  `sink=mysql`).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| RELP | Vira software próprio (`go-relplog-server/`); o enum de `transport` do `type=syslog` não tem `relp` (decisão 2) |
| DTLS (RFC 6012) | Sem implementação na stdlib e com pouca adoção pelos emissores (decisão 5) |
| BEEP (RFC 3195) | Histórico, sem emissores vivos (decisão 5) |
| Importação de arquivo (`transport=file`) | Fora da v1 (decisão 5) |
| `space` por listener ou rede | Um Workspace por instância na v1 (decisão 18) |
| Sonar, ping e pong | Só o heartbeat beacon (§11; decisão 8) |
| Validação por JSON Schema em execução | Só em teste e CI (§18.2; decisão 14) |
| Autenticação de emissor fora do TLS | Não existe no protocolo; firewall, rede privada ou mTLS (§4.5) |
| Roteamento por facility para destinos distintos | Um destino por instância |
| Deduplicação de conteúdo, `last message repeated N times` | Responsabilidade do consumidor |
| Syslog **de saída** | §21 |

### 1.3 Fontes normativas e desvios

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, `jlh-envelope.schema.json`, ciclo de vida, plano de controle |
| skill `loghub-type-syslog-schema` | consultada em 2026-10-05 | Payload `type=syslog`, `jlh-syslog-payload.schema.json`, algoritmo de conversão, 24 vetores dourados |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 | AgentUUID, Workspace, AgentName, Hostname, MachineID, `DATADIR` |
| skill `loghub-postgres-table-inbox` | consultada em 2026-10-06 | `sink=postgres`: DDL da `loghub.inbox`, mapeamento envelope → colunas, enfileirar com `ON CONFLICT (uuid) DO NOTHING` (§3.7.1) |

Cópias dos dois schemas, dos exemplos de envelope e dos vetores ficam em
`internal/jlhcheck/loghub/` (§18.2). Onde as duas skills divergem, vale
a mais restritiva.

**Desvios deliberados das skills**, cada um com o motivo na seção citada:

| # | Desvio | Skill diz | Aqui | Seção |
|---|---|---|---|---|
| D1 | Moldura TCP | detectar **por frame** | **por conexão**, com confirmação `[1-9][0-9]{0,9} SP` | §5.2 |
| D2 | TAG sem timestamp | sem timestamp, nenhuma TAG | TAG **estrita** ainda extrai `appname`/`procid` | §6.5.5 |
| D3 | Validação | todo envelope passa no schema antes da fila | schema só em teste e CI; o serializador é provado por fuzz | §18.2 |
| D4 | `LOGHUB_UUID_LEVEL` | variável sugerida | **não existe**: nível 3 fixo, que é o que o JLH exige | §3.1 |
| D5 | Opções de importação | `reference`, `transport=file` | fora da v1 | §1.2 |
| D6 | Espaço em branco nos campos `^\S+$` | o conversor de referência emite o valor | o valor é omitido (cabeçalho 5424: `header-invalid` e `raw` obrigatório; 3164: não é hostname/TAG). O conjunto é o `\s` do ECMAScript **mais** U+001C–U+001F e U+0085, que o `\s` do Python (o `validate_payload.py` da skill) também casa | §6.4.1 |
| D7 | Avisos de encoding de um SD seguido de texto colado | a referência funde os avisos dos valores do SD antes de recusar o texto colado (`sd-invalid`, `encoding-invalid`, `encoding-legacy`) | descartados, como em qualquer SD que falhou: só `message` decodifica e avisa (`sd-invalid`, `encoding-legacy`). A própria referência faz assim num SD sem fechamento | §6.4.3 |
| D8 | Timestamp cujo instante UTC cai fora dos anos 0001–9999 | a referência emite `10000-01-01T…` ou `0000-12-31T…`, fora do schema ou recusado pelo próprio validador | `timestamp-invalid` e `timestamp` omitido | §6.4.2 |

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
   emissores (rsyslog, syslog-ng, journald, logger, appliances, k8s)
      |
      |  UDP :1514   TCP :1514   TLS :6514   unix dgram
      v
 +-----------------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                          |
 |   [PROXY] -> [TLS] -> moldura -> truncamento -> amostra de relógio     |
 |   -> PRI -> filtro -> rate-limit -> uuid -> Item(cabeçalho + cru)      |
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
 | porta admin (admin.bind, padrão 127.0.0.1:9102)                        |
 |   /healthz   /readyz   /metrics                                        |
 +-----------------------------------------------------------------------+
```

O ring é **único e global**, compartilhado por todos os transportes: uma métrica
única de profundidade, backpressure justo (o TCP só para de ler quando o sistema
inteiro está saturado) e código simples.

### 2.2 Onde a conversão acontece, e por quê

Decisão 7. A recepção faz o mínimo que permite decidir se a mensagem entra; a
conversão completa roda no **worker de dreno**.

| Etapa | Onde roda | Custo |
|---|---|---|
| PROXY, handshake TLS | recepção, uma vez por conexão | do transporte |
| Moldura, truncamento | recepção | inevitável |
| Amostra de relógio, `uuid` | recepção | uma leitura de relógio, um UUID |
| **PRI** (`<134>`) | **recepção** | uma varredura de no máximo 5 bytes |
| Filtro, rate-limit | recepção | comparações de inteiro, um mapa em shards |
| RFC 3164 / RFC 5424, SD, UTF-8 | **worker de dreno** | proporcional à linha |
| Serialização do JLH | **worker de dreno** | proporcional aos campos |

O PRI fica na recepção porque o filtro (§7) depende dele, e descartar depois do
ring seria aceitar para jogar fora. O resto sai do caminho quente: num pico de
UDP, a CPU da conversão é paga pelos `sink.workers`, que é onde a vazão é
elástica.

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
   (§3.5). Por isso **toda chave que muda os bytes do JLH exige restart** (§16.3):
   uma reingestão ou um retry precisam produzir os mesmos bytes com o mesmo
   `uuid`, que é a regra do outbox do JLH.

Com `ring=none` não há worker: a conversão roda na goroutine da recepção, que é
quem escreve no sink. É o modo explicitamente lento.

### 2.3 Regra de aceitação, por transporte

> A confirmação sai no instante em que a garantia da combinação configurada é
> cumprida — nunca antes. **Onde não existe confirmação, existe um contador.**

| Transporte | A confirmação é | Ring cheio ou sink DOWN significa |
|---|---|---|
| **UDP** | não existe | datagrama **descartado**, `syslog_messages_dropped_total{reason="ring_full"\|"sink_down"}` |
| **unix dgram** | não existe | idem; mas o kernel bloqueia o emissor quando o buffer do socket enche, e o backpressure existe **antes** de chegar a nós |
| **TCP** | continuar lendo do socket | **parar de ler** (§5.7); a janela TCP segura o emissor e nada se perde |
| **TLS** | continuar lendo do socket | idem ao TCP |

Dentro de cada transporte que confirma, a garantia é a do ring:

| `ring` | A garantia é |
|---|---|
| `memory` | o item entrou no canal do ring |
| `sqlite` | o `COMMIT` do grupo no `ring.db` terminou (com `fsync` se `synchronous=FULL`) |
| `redis` | o script de enfileiramento retornou (a durabilidade é a do `appendfsync` do Redis) |
| `none` | o **sink** confirmou a escrita (ou na hora, com `sink=none`) |

**O UDP perde por desenho do protocolo, não deste servidor.** O que o kernel
descarta antes de chegarmos aparece em `syslog_udp_receive_errors_total` e nos
contadores da interface. Quem não pode perder usa TCP ou TLS.

### 2.4 Papel no LogHub

| Aspecto | Valor |
|---|---|
| Papel | **Bridge-IN**: traz dados de fora e publica como envelopes |
| Operação | **Produce** (primeira inserção): `uuid` novo, `step = 0`, `from` = AgentUUID |
| Subject | `loghub.{space}.syslog`; com `sink.nats.subjects = facility-severity`, `loghub.{space}.syslog.<facility>.<severity>` (§3.8) |
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
| `uuid` | UUIDv7 **nível 3** (`go-loghub-uuidv7`), em minúsculas, gerado **na recepção** com `GenerateAt(Level3, t)`, onde `t` é a amostra de relógio da mensagem. É cunhado depois de filtro, rate-limit e estado do sink (uma mensagem descartada não gasta `uuid`), mas sempre da amostra `t` do fim do frame. Gravado no cabeçalho do item e nunca regerado: a retentativa do laço de backpressure do TCP, o retry do worker, o dump e a reingestão reusam o mesmo; se uma tentativa de destino incerto (`ErrUnavailable`, prazo do `ring=none`) tiver sido aceita, as duas cópias deduplicam por `(uuid, step)`. O nível 3 é fixo; `LOGHUB_UUID_LEVEL` não existe (D4). |
| `from` | AgentUUID de **quem recebeu** a mensagem (`lhident.AgentUUID()`), copiado para o cabeçalho do item. Com `ring=redis`, outra réplica pode drenar o item, e o `from` continua o de quem recebeu (§3.5). Nunca um identificador do emissor remoto: a origem externa está em `payload.host`. |
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
| `transport` | `udp` \| `tcp` \| `tls` \| `unix` | sempre |
| `framing` | `datagram` \| `octet-counting` \| `non-transparent` | sempre |
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

`transport` nunca é `dtls`, `file` nem `beep`, e `format` nunca é `rfc3195`
(§1.2). A única extensão emitida é `x-proxy`.

**Omissão:** campo sem valor na origem (NILVALUE, inexistente no formato,
inválido) é **omitido**. Nunca `null`, nunca `""`, exceto `message`. Objetos
sem conteúdo (`host`, `tls`, `parse`, `structured-data`) também são omitidos.

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

O mesmo serializador produz o beacon (§11).

### 3.4 Cabeçalho do item

O que a conversão **não consegue** recuperar da linha crua e que, portanto,
viaja ao lado dela pelo ring, pelo `ring.db`, pelo stream do ring Redis e pelo
dump:

```json
{"v":1,"uuid":"019a...","from":"0196...","space":"default","uts":1791028800,"sub":123456789,
 "transport":"tcp","framing":"octet-counting","src-ip":"10.0.0.3","src-port":40112,
 "dst-ip":"10.0.0.10","dst-port":1514,"size":175}
```

| Chave | Conteúdo |
|---|---|
| `v` | versão do codec, `1`. Outra versão é linha indecodificável (§9.4). |
| `uuid` | §3.1 |
| `from`, `space` | identidade de quem recebeu |
| `uts`, `sub` | a amostra de relógio |
| `transport`, `framing` | §3.2 |
| `src-ip`, `src-port`, `dst-ip`, `dst-port` | endereços **canônicos** (§4.2), já resolvidos pelo PROXY quando houver; omitidos quando desconhecidos ou no `unix` |
| `peer-ip`, `peer-port` | o par TCP real, **só** quando o PROXY protocol foi usado |
| `tls` | objeto de §4.5, só no `tls` |
| `size` | octetos originais da SYSLOG-MSG; `size > len(cru)` significa truncamento (§5.5) |

O PRI **não** está no cabeçalho: ele é derivável do cru, e a conversão o relê
com a mesma função que a recepção usou (§6.2). Guardá-lo seria criar duas
fontes da mesma verdade.

Um codificador, três consumidores: a coluna `head` do `ring.db`, o campo `h` do
stream do ring Redis e o dump de emergência. É JSON porque roda uma vez por
mensagem fora do caminho quente (com `ring=memory` nem é serializado), e um
formato binário próprio seria um parser novo de entrada que um disco corrompido
torna arbitrária. O decodificador é alvo de fuzz (`FuzzHead`, §18.3): recusa
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
| `src_ip` | `payload.host.src-ip` | `NULL` (unix) |
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
colunas com tipos compatíveis (extras toleradas, tipos mais estreitos também, por
conta do operador); um índice **único** exatamente sobre `(uuid, step)`;
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

Com `L = syslog.max_message_bytes` (o cru nunca passa disso, §5.5):

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

Quatro transportes independentes, cada um ligado por um parâmetro de `[listen]`
que, vazio, o desliga. **Todos desligados é erro de configuração** (exit 1).

| Transporte | Parâmetro | Padrão | RFC | `framing` |
|---|---|---|---|---|
| UDP | `listen.udp` | `:1514` | RFC 5426 | `datagram` |
| TCP | `listen.tcp` | `:1514` | RFC 6587 | `octet-counting` ou `non-transparent` (§5.2) |
| TLS | `listen.tls` | `""` (desligado) | RFC 5425 | `octet-counting` |
| unix datagram | `listen.unix` | `""` (desligado) | — | `datagram` |

### 4.1 As portas padrão não são 514

A imagem roda como uid 65532 e não abre porta abaixo de 1024. Os padrões são
`:1514` (UDP e TCP) e `:6514` como costume do TLS (a porta da RFC 5425 já é alta).
O `docker-compose.yml` (memory-nats) publica `514:1514`; os demais compose usam
portas altas distintas por padrão (`SYSLOG_PORT`), para conviverem no mesmo host.
Quem roda no host e quer 514 usa
`setcap cap_net_bind_service=+ep /app/go-syslog-server` ou
`sysctl net.ipv4.ip_unprivileged_port_start=0`. O log de boot imprime o endereço
efetivo de cada listener.

### 4.2 UDP (RFC 5426)

Um datagrama é **uma** mensagem; nunca é dividido por LF.

- Leitura com `ReadMsgUDPAddrPort` num buffer de 65 536 bytes, maior que
  qualquer datagrama (65 507), para que o kernel nunca corte em silêncio.
- **`dst-ip` por PKTINFO** (decisão 15), só com a stdlib (`syscall`):
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
- **Sem backpressure.** Ring cheio, sink DOWN, rate-limit ou filtro descartam o
  datagrama e incrementam o contador com a razão.
- Um datagrama de origem não endereçável (`0.0.0.0` ou `::`) não tem envelope
  válido possível (o schema exige `host.src-ip`): é descartado como
  `reason="malformed_frame"`.
- `syslog_udp_read_buffer_bytes` mostra o valor cru do `getsockopt`; no Linux,
  que reporta o dobro do concedido, o `WARN` de falta compara com a metade.

### 4.3 unix datagram

Socket `SOCK_DGRAM` em `listen.unix` (tipicamente `/dev/log`): `logger(1)`, a
`syslog(3)` da libc, o `journald` com `ForwardToSyslog=yes`.

- O servidor **cria** o socket no boot com `listen.unix_mode` (padrão `0666`). Um
  socket órfão é removido antes do `bind` só quando o `connect` datagrama a ele é
  **recusado** (`ECONNREFUSED`: ninguém escuta ali); se alguém responde, o boot
  sai com exit 1 em vez de roubar o `/dev/log` de outro processo. Só um **socket
  datagrama** é removido (conferido sem seguir links): um arquivo comum, um
  diretório ou um socket de outro tipo no caminho (um `/dev/log` em modo stream
  responde `EPROTOTYPE`, não recusa) é exit 1 com a mensagem certa, e fica
  intocado. A regra "o `connect` falhou, então remove" apagaria o arquivo de um
  caminho mal configurado ou o socket vivo de outro dono. No shutdown, o arquivo
  é removido.
- Caminho relativo, ou maior que `sun_path` (107 bytes no Linux, 103 nos BSD e
  no macOS), é exit 1 com a mensagem certa.
- Buffer de leitura de `max(65536, syslog.max_message_bytes + 1)`. Um datagrama
  maior volta com `MSG_TRUNC`, e `size` registra os bytes lidos (um limite
  inferior); o aviso `truncated` sai porque `size > len(cru)`.
- Sem campos de rede no payload (o schema proíbe), `sid = 0`, sem rate-limit
  (não há prefixo de rede, e limitar o `/dev/log` calaria o próprio host).
- O backpressure é do kernel: com o buffer do socket cheio, o `send` do emissor
  bloqueia antes de a mensagem chegar até nós.

### 4.4 TCP (RFC 6587)

Uma conexão carrega N mensagens, separadas por uma das molduras de §5. É o
transporte com backpressure real.

- `listen.tcp_listeners > 1` habilita `SO_REUSEPORT`.
- `tcp.max_conns` e `tcp.max_conns_per_ip` (`0` = ilimitado) valem **por
  transporte**, somando os listeners: um limitador por listener multiplicaria os
  tetos. O teto global **bloqueia no accept**; o por IP **fecha na hora**. O IP
  é o do par TCP, **antes** do PROXY protocol: com um balanceador na frente, o
  teto por IP vê o balanceador (§4.6).
- `tcp.idle_timeout` (120 s) fecha conexão sem tráfego; `0` desliga.
- `SO_KEEPALIVE` ligado, com período de 30 s, inclusive sob o limitador de
  conexões (o *wrapper* é desembrulhado para chegar ao socket).
- O par de endereços do payload é `RemoteAddr` (origem) e `LocalAddr` (destino),
  canônicos.

### 4.5 TLS (RFC 5425)

Decisões 5 e 17. Um listener `crypto/tls` (stdlib, sem dependência nova) com o
mesmo pipeline do TCP depois do handshake.

- **Moldura:** só octet-counting (RFC 5425 §4.3). Um frame que não começa com
  dígito, ou cujo MSG-LEN não confirma, **fecha a conexão**
  (`reason="malformed_frame"`). Não há autodetecção.
- `tls.cert_file` e `tls.key_file` obrigatórios com `listen.tls`; lidos **no
  boot** (a troca de certificado exige restart na v1; §21).
- `tls.min_version` = `1.2` (padrão) ou `1.3`. As suítes são as padrão do Go, que
  cobrem as da RFC 9662.
- `tls.handshake_timeout` (10 s) protege contra *slowloris* no handshake. A
  falha de handshake conta em `syslog_tls_handshake_errors_total` e fecha.
- **O leitor também escreve.** No TLS 1.3, um `KeyUpdate(update_requested)` do
  par faz o `Read` responder com uma escrita; um par que não lê enche a janela e
  prende o leitor nessa escrita. Por isso o prazo de ociosidade arma **leitura e
  escrita** juntas, o handshake não limpa o prazo de escrita, e o `Close` do
  transporte vence os dois prazos de toda conexão e, se um leitor não voltar em
  1 s, fecha o socket dele.
- **Certificado do cliente** (`tls.client_auth`):

  | Valor | Comportamento | `tls.Config.ClientAuth` |
  |---|---|---|
  | `optional` (padrão) | pede o certificado; com `tls.client_ca`, verifica quando ele vem; sem CA, aceita sem verificar e registra `peer-*` (para pinning a jusante) | `VerifyClientCertIfGiven` com CA; `RequestClientCert` sem |
  | `require` | exige e verifica; `tls.client_ca` é obrigatório (exit 1 sem ele) | `RequireAndVerifyClientCert` |
  | `none` | não pede | `NoClientCert` |

- **`payload.tls`**, preenchido depois do handshake e gravado no cabeçalho do
  item:

  | Chave | Valor |
  |---|---|
  | `version` | `TLS1.2` ou `TLS1.3` (o `tls.VersionName` do Go sem o espaço) |
  | `cipher` | nome IANA da suíte (`tls.CipherSuiteName`) |
  | `peer-subject` | DN do certificado do cliente (RFC 4514); **omitido acima de 1 KiB** (o fingerprint identifica o certificado; o teto mantém §3.9 fechado) |
  | `peer-fingerprint` | `sha-256:` + os 32 octetos do SHA-256 do DER em hexadecimal **maiúsculo** separados por `:` (RFC 5425 §4.2.2) |

  Sem certificado de cliente, `peer-*` são omitidos. Um certificado apresentado
  e não verificado (`optional` sem CA) é registrado do mesmo jeito: o payload diz
  o que foi apresentado, não o que foi confiado.
- `[tls]` tem `idle_timeout`, `max_conns`, `max_conns_per_ip` e
  `backpressure_timeout` próprios, com os padrões do `[tcp]`: são transportes
  distintos, com limitadores distintos.

### 4.6 PROXY protocol

Decisão 18. Opt-in no TCP e no TLS, para quando um balanceador (HAProxy, NLB,
Traefik) está na frente e o `RemoteAddr` seria o dele.

- `proxy.protocol`:

  | Valor | Comportamento |
  |---|---|
  | `off` (padrão) | nada é lido; um cabeçalho PROXY vira o começo de uma mensagem `malformed` |
  | `optional` | de par **confiável**, lê o cabeçalho se ele vier e segue sem ele se não vier; de par não confiável, nada é lido |
  | `required` | toda conexão precisa vir de par confiável **com** cabeçalho válido; senão fecha (`reason="proxy"`) |

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
  conexão (`invalid` e `reason="proxy"`); um cabeçalho que declara origem não
  especificada é inválido; de par não confiável nada é lido nem contado; uma
  conexão que fecha antes do primeiro byte (sonda de saúde do balanceador) não
  conta nada. `LOCAL`, `UNSPEC`, `UNIX`, `DGRAM` e `UNKNOWN` contam como
  `local`.
- **Efeito no payload:** `host.src-*` e `host.dst-*` vêm do cabeçalho (o
  cliente e o endereço que ele discou), o `sid` é calculado com eles, e o par TCP
  real vai em **`x-proxy: {"peer-ip", "peer-port"}`**, para que a ponta real do
  transporte continue registrada (§20.8). O rate-limit usa o `src-ip` resolvido.
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
gravaria fragmentos como mensagens. No TLS a moldura é sempre octet-counting
(§4.5).

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

### 5.5 Tamanho e truncamento (decisão 13)

`syslog.max_message_bytes` (env `SYSLOG_MAX_MESSAGE_BYTES`, padrão 64 KiB; a RFC
5424 §6.1 exige aceitar 480 e recomenda 2048, a RFC 5425 recomenda 8192):

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

O limite de 64 KiB é o que mantém o pior JLH abaixo de 1 MiB (§3.9). A standard
não limitava (fusível de 16 MiB que fechava a conexão) e por isso precisava de
`max_payload` e `max_allowed_packet` sob medida em todo compose.

### 5.6 Mensagem vazia

Um datagrama de 0 octetos ou um frame vazio **não gera envelope** (a skill: só
uma mensagem com ao menos um octeto gera envelope). Conta como chegada em
`syslog_messages_total` e como descarte em
`syslog_messages_dropped_total{reason="empty"}`, sem log. Uma conexão que fecha
depois de `N ` sem nenhum octeto entregue também termina como `empty`.

### 5.7 Backpressure: parar de ler

Ring cheio ou sink DOWN: o leitor do TCP (e do TLS) **para de chamar `Read`**.
Não fecha, não descarta, não responde nada. O buffer de recepção do socket
enche, a janela anunciada vai a zero e o emissor para de transmitir; o `rsyslog`
acumula na fila dele, que é onde o dado deve ficar.

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
`Retry` e `RecordDrop` são caminhos separados (bug real da standard). Pelo mesmo
motivo, **filtro e rate-limit decidem uma vez por mensagem**: a retentativa não
gasta outra ficha do balde nem reavalia um filtro trocado por `SIGHUP` durante a
espera. Cobrar o rate-limit a cada volta de 50 ms fazia uma mensagem retida por
backpressure acabar descartada como `ratelimit` depois que o ring escoava.

Com `ring=none`, uma recusa `logic` do sink é **final** para aquela mensagem:
o escritor instrumentado já a contou em
`syslog_events_dropped_total{reason="sink_logic"}`, e a recepção a trata como
consumida (o buffer vai para o GC, a leitura segue para o próximo frame), sem
retentativa e sem contá-la de novo com outro motivo. Retentá-la seguraria o
leitor por todo o `backpressure_timeout` e contaria a mesma perda a cada volta.
`network`/indisponível continua retentável; `fatal` segue §13.3.

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
`syslog_events_dropped_total{reason="malformed"}` e log amostrado.

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
`payload.transport`. Agrupa os envelopes de uma conexão TCP/TLS (ou de um socket
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
- Descarte: `syslog_messages_dropped_total{reason="filter"}`.
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
- Não se aplica ao `unix`.
- Estourar o limite **descarta** (`reason="ratelimit"`), também no TCP: ao
  contrário do ring cheio, um emissor acima do limite não escoa sozinho.
- `ratelimit.enabled = false` por padrão; com ele desligado e algum listener fora
  do loopback, `WARN` no boot. Recarregável por `SIGHUP`.
- **O contador de baldes muda junto dos mapas**, sob o lock de cada shard: o
  `Allow` soma ao inserir, e o `Reset` (troca de máscara por `SIGHUP`) e o sweep
  descontam o que aquele shard perdeu. Um `Store(0)` no fim do `Reset` apagaria
  inserções concorrentes em shards já limpos, e um desconto do sweep feito
  depois do laço levaria o contador abaixo de zero: `syslog_ratelimit_buckets`
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
confirmação (a retomada da leitura no TCP) acontece depois desse retorno, nunca
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

Qualquer um estourando satura o ring, com o efeito por transporte de §2.3.
`ring.high_water` (0.80) é avaliado nos dois e derruba o `/readyz`.

### 9.3 `ring.mode = memory`

Canal com buffer de `capacity_items`, com contador atômico de bytes. `Enqueue`
retorna assim que o item entra no canal. **Garantia:** RAM deste nó. Perde sob
`kill -9`, OOM ou pane; sob `SIGTERM` a perda é evitada pelo dreno do shutdown
(§13.4), e sob FATAL do sink, pelo dump (§12). É o modo de maior vazão e o
padrão.

### 9.4 `ring.mode = sqlite`

Ring persistente, para retenção máxima ao custo de latência. Só faz sentido com
TCP ou TLS (com só UDP, `WARN`: um `fsync` por mensagem que o kernel do emissor
já pode ter descartado). **Garantia:** `COMMIT` concluído antes da confirmação;
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
  `Enqueue` à espera recebe `ErrUnavailable` (`reason="ring_unavailable"` no UDP)
  e o ring passa a **indisponível** (`/readyz` falha, `WARN` uma vez por
  transição) até o próximo `COMMIT` bem-sucedido. Nunca é `sink_down`: o sink
  pode estar perfeito.
- **Cancelamento durante o group commit:** uma vez entregue à escritora, o
  `Enqueue` espera o `COMMIT` mesmo com o contexto cancelado. Voltar antes
  descartaria uma mensagem que será commitada de qualquer forma e devolveria ao
  pool um buffer que a escritora ainda lê.
- **Linha indecodificável** (corrupção, codec de outra versão): movida para
  `quarantine` numa transação pela escritora e descontada da contabilidade no
  ato; `ERROR` no log e `syslog_ring_quarantined_total`. Sem teto e sem limpeza
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
| Stream | `ring.redis.stream` (padrão `syslog:ring`) |
| Entrada | `XADD <stream> * h <cabeçalho> b <cru>` |
| Contador de bytes | `<stream>:bytes` (string com inteiro) |
| Grupo | `ring.redis.group` (padrão `syslog-ring`), criado no boot com `XGROUP CREATE <stream> <group> 0 MKSTREAM` (`BUSYGROUP` é sucesso) |
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

- **Redis Cluster** exige um stream com *hash tag* (`{syslog:ring}`), porque os
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

**Indisponibilidade do Redis do ring:** `Enqueue` devolve `ErrUnavailable`: o UDP
descarta (`reason="ring_unavailable"`), o TCP para de ler, o `/readyz` falha. O
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
| `ring=sqlite` com só UDP | permitido, `WARN` |
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
   aos pools, `syslog_events_written_total += n`.
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
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`syslog_events_dropped_total{reason="sink_logic"}`), confirmado no ring |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`syslog_events_written_total`, só os demais em `sink_logic`, e o log traz só os
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
  `syslog_sink_postgres_duplicates_total`.
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
  parâmetros de execução são refeitos do zero: o ambiente libpq do processo
  (`PGOPTIONS`, `PGTARGETSESSIONATTRS`, `PGAPPNAME`) não chega ao pool (os
  demais `PG*` ainda chegam: §22.1). Em tempo
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
| **`42`** (esquema), `28`, `2F`, `42501`, `25006`, `23514` sem constraint nomeada | **`fatal`** |
| qualquer outra | `network` |

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
- **Escrita:** `INSERT INTO ... VALUES (...), (...) ON DUPLICATE KEY UPDATE uuid
  = uuid`. **Nunca `INSERT IGNORE`**, que rebaixa `1406` e `1366` a aviso e trunca
  em silêncio. Duplicatas = `len − afetadas`.
- O pedaço é **dividido** de novo antes de ir ao servidor: no máximo 65 535
  placeholders por comando (dezoito por linha → **3 640 linhas**; `sink.batch`
  acima disso é exit 1), e no máximo 7/8 do `max_allowed_packet` lido no boot
  (custo estimado por linha: `env` + 3 × colunas de texto + 512 bytes).
- **Validação no boot** (cada recusa é exit 3): tabela existe e é InnoDB; colunas
  com as famílias de tipo de §3.7; índice único exatamente sobre `(uuid, step)`;
  `max_allowed_packet` ≥ a maior linha (§3.9); e uma **sonda**: o mesmo
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
| sem número (EOF, `ErrBadConn`, prazo), `1040` `1053` `1077` `1927` `1158`–`1161` `1205` `1213` `1317` `3024` `2006` `2013` | `network` |
| `1290` `1836` (somente leitura), `1021` `1030` `1041` `1114`, `1044` `1045` `1049` `1130` `1142` `1143`, `1054` `1146` `1364` | **`fatal`** |
| `1048` `1062` `1153` `1264` `1366` `1406` `1690`, `ErrPktTooLarge` | `logic` |
| qualquer outro | `network` |

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

`PubAck.Duplicate` conta em `syslog_sink_nats_duplicates_total`. Fora da
`duplicate_window` do stream, uma redelivery duplica (at-least-once).

**`sink.nats.mode = core`** (opt-in, *best effort*): `PublishMsg` por envelope e um
`FlushTimeout(sink.op_timeout)` por pedaço. O sucesso significa que o servidor
NATS **recebeu** as mensagens, não que alguém as guardou: sem stream, uma
mensagem sem assinante some. Combina com o UDP, que já é lossy; o boot registra
`WARN` com essa frase.

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
| O maior evento cabe | `nc.MaxPayload()` e `max_msg_size` do stream contra §3.9 + headers; com `storage: file`, também contra os 32 MiB por registro do file store (`10077`) |

Com `core`, só o `max_payload`. O stream é criado por quem administra o NATS, a
partir de `docker/nats/stream.json`; o servidor nunca cria nem altera um stream.

**JetStream desligado no servidor:** o `StreamInfo` recebe `ErrNoResponders`, que
é `network`. Só nesse caso, na primeira validação, o sink pergunta
`AccountInfo`; se ele disser que o JetStream não está habilitado (no servidor ou
na conta), é FATAL nomeando o JetStream, em vez de esperar o `boot_timeout`. Não
se pergunta sempre porque um papel de privilégio mínimo pode ter a API do stream
sem `$JS.API.INFO`.

**`Health()`:** o estado da conexão (`CONNECTED` é saúde, `CLOSED` é fatal,
`RECONNECTING`/`DISCONNECTED` é falha de rede) e, em `jetstream`, um
`StreamInfo` (um `PING` responderia feliz com o JetStream desligado); em `core`,
um `FlushTimeout`.

| Erro | Classe |
|---|---|
| prazo, `ErrTimeout`, `ErrNoResponders`, `ErrNoServers`, `ErrDisconnected`, `ErrConnectionReconnecting`, `ErrReconnectBufExceeded`, `ErrNoStreamResponse`, `ErrAsyncPublishTimeout`, `ErrTooManyStalledMsgs` | `network` |
| `ErrAuthorization`, `ErrAuthExpired`, `ErrConnectionClosed`, `ErrStreamNotFound` (`10059`), JetStream desligado (`10076`, `10039`), stream esperado não confere (`10060`), selado (`10109`), recursos (`10023`), armazenamento (`10077`) | **`fatal`** |
| `ErrMaxPayload`, `ErrMaxBytesExceeded`, `10054`, `10097` | `logic` |
| outro erro da API com código 4xx | `fatal` |
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
socket; qualquer outro parâmetro de consulta é recusado, não ignorado. O
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
SETTINGS insert_deduplication_token = 'sls-<sha256 dos uuids do pedaço>',
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
   `sls-` + os 32 primeiros dígitos hexadecimais do SHA-256 dos `uuid` do
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
`syslog_sink_clickhouse_dedup_blocks_total` o acumula (§10.10.10).

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
confirmação de `ring=none` (o TCP voltar a ler, §2.3) significa depende do
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
syslog_sink_clickhouse_dedup_blocks_total      counter  blocos descartados pelo insert_deduplication_token
syslog_sink_clickhouse_pool_conns{state}       gauge    state: acquired|idle|max
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

- **Só com `sink=nats`** (nos dois modos). Com outro sink, nada é emitido e o boot
  registra `INFO`.
- **Quando:** na primeira conexão, em toda reconexão (o `ReconnectHandler` do
  `nats.go`) e a cada `loghub.beacon_interval` (env `LOGHUB_BEACON_INTERVAL`, em
  segundos; padrão 15; `0` desliga o periódico, não o de conexão).
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

- Falha de publicação é contada (`syslog_beacons_total{result="error"}`) e logada
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
para não escapar o cru. Escrito com `O_CREATE|O_EXCL` e `fsync` no arquivo e no
diretório; uma falha do `fsync` do **diretório** também é falha do dump (§12.3):
o arquivo parcial é removido, porque sem ela a entrada do diretório pode não
sobreviver a uma queda de energia e o processo sairia dizendo que salvou. O dump
é exatamente o par cabeçalho + cru, com o codificador de §3.4.

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

Arquivo truncado ou corrompido: processado até o último registro íntegro, o resto
logado em `ERROR` com a contagem de bytes, e o arquivo renomeado para
`.corrupt`. Um registro de cabeçalho indecodificável é pulado e também torna o
arquivo danificado (quarentena depois da entrega, nunca remoção). A linha de
comprimentos tem no máximo 32 bytes. O decodificador **nunca confia no comprimento declarado** e tolera
truncamento sem pânico (`FuzzRead`).

### 12.3 Falha do próprio dump

Disco cheio, sem permissão, `emergency.enabled = false`: `ERROR` com a contagem
exata, `syslog_events_dropped_total{reason="emergency_failed"} += n`, exit 3.
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
    recusa => exit 3 IMEDIATO
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
12. abre os transportes: unix, udp, tcp, tls
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

**Os transportes abrem por último e juntos.** Abrir o UDP antes de o sink estar
pronto descartaria datagramas sem ninguém saber; a única defesa do UDP é não
estar escutando.

### 13.2 Validação de configuração

| Combinação | Efeito |
|---|---|
| Nenhum `listen.*` | exit 1 |
| Dois `listen.*` no mesmo endereço e protocolo (TCP e TLS na mesma porta inclusive) | exit 1; com a mesma porta, um **nome** de host de qualquer lado conta como o mesmo endereço (`localhost` e `127.0.0.1`): para dividir a porta, dois literais IP distintos |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `ring.sqlite.path`, `sink.sqlite.path` ou `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `listen.unix` relativo ou maior que `sun_path` | exit 1 |
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
- **FATAL é terminal.** Toda transição e o gauge `syslog_sink_up` são escritos
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
fase 3  cancela o contexto de recepção e fecha os transportes na ordem inversa
        do boot (tls, tcp, udp, unix); as mensagens já lidas terminam de entrar
        no ring
fase 4  drena o ring para o sink, com shutdown.drain_timeout (30 s), incluindo
        Workers.Pending()
fase 5  para o beacon, fecha o sink, o ring e o SQLite, exit 0
```

- O contexto de recepção é cancelado **antes** de os transportes fecharem: é o
  que solta um leitor preso no laço de backpressure. Um item já entregue ao ring
  não é afetado: o `Enqueue` do sqlite espera o group commit, e o `ring=none`
  escreve num contexto desligado.
- **Nenhum `Enqueue` depois do `Drain`.** O `Close` de cada transporte só volta
  quando os leitores dele voltaram (UDP e unix esperam as goroutines; TCP e TLS
  esperam as conexões e, depois de 1 s, fecham o socket de quem não voltou).
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
- **Segundo sinal** em qualquer fase: saída imediata, `exit 130`, aceitando a
  perda (contada em `reason="abort"` com `ring=memory` e `none`; com `sqlite` e
  `redis` o que sobrou continua no ring). `SIGQUIT` aborta sempre, inclusive
  fora do shutdown: no boot e com o servidor no ar, saída imediata com 130.
- **Sink FATAL durante a fase 4** (o `SIGTERM` chegou com o sink UP e ele virou
  FATAL no dreno): a espera termina na hora, em vez de gastar o `drain_timeout`
  num dreno que não acontece mais, e a saída é **3** em qualquer ring; com
  `memory`, o que sobrou vai para o dump.
- **O socket unix** é removido quando o transporte fecha, na fase 3.
- **`SIGHUP` vale o processo inteiro.** É capturado desde a primeira linha; fora
  da janela entre o fim do boot (passo 13) e o início do shutdown, é logado em
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
| `0` | shutdown completo: ring vazio, ou `sqlite`/`redis` com o resto guardado |
| `1` | erro de configuração ou de inicialização não relacionada a destino |
| `3` | destino indisponível: sink FATAL, `boot_timeout` ou `drain_timeout` estourados, ring Redis indisponível no boot, maior evento que não cabe, guarda de não-progresso do dreno |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` (`DATADIR`, `MACHINE_ID`, `MACHINE_ID_FILE`, `AGENT_NAME`, `AGENT_UUID`, `HOSTNAME`, `WORKSPACE`; 112 = `Initialize()` duas vezes); a tabela completa está na skill `go-loghub-ident` e em `docker/README.md` |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 7–10 do boot |

---

## 14. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9102` (o `go-webhook-server` usa `9101`; os dois
rodam lado a lado). HTTP/1.1.

| Path | Semântica |
|---|---|
| `/healthz` | `200` enquanto o processo vive. Liveness. |
| `/readyz` | `200` só se: sink `UP`, ring abaixo de `ring.high_water`, backlog de boot zerado, ring Redis alcançável (com `ring=redis`), porta pública aberta e não em shutdown. Senão `503` com o motivo em `text/plain`. |
| `/metrics` | exposição Prometheus em texto. |

### 14.1 Métricas

Prefixo `syslog_`. **Nenhum label deriva de conteúdo controlado pelo emissor**
(nada de hostname, appname, IP); todo label tem conjunto finito conhecido em
compilação. `facility` e `severity` **não** são labels: o produto com
`transport` daria 768 séries, e a distribuição por severidade é pergunta para o
destino, onde é coluna indexada.

| Métrica | Tipo | Labels |
|---|---|---|
| `syslog_messages_total` | counter | `transport` (udp, tcp, tls, unix) |
| `syslog_messages_accepted_total` | counter | `transport` |
| `syslog_messages_dropped_total` | counter | `reason` (ring_full, ring_unavailable, sink_down, filter, ratelimit, malformed_frame, proxy, empty, shutdown) |
| `syslog_messages_truncated_total` | counter | `transport` |
| `syslog_message_bytes` | histogram | — (tamanho original) |
| `syslog_convert_total` | counter | `format` (rfc5424, rfc3164, malformed) |
| `syslog_convert_warnings_total` | counter | `code` (os 14 de §6.9) |
| `syslog_convert_duration_seconds` | histogram | — |
| `syslog_conns_active`, `syslog_conns_total` | gauge, counter | `transport` (tcp, tls) |
| `syslog_conns_refused_total` | counter | `transport`, `limit` (global, per_ip); `global` conta cada vez que o accept estaciona no teto, um limite inferior, porque o teto global bloqueia em vez de recusar |
| `syslog_tls_handshake_errors_total` | counter | — |
| `syslog_proxy_headers_total` | counter | `result` (v1, v2, local, absent, invalid) |
| `syslog_backpressure_seconds_total` | counter | `transport` (tcp, tls) |
| `syslog_confirm_duration_seconds` | histogram | — (do frame completo à garantia do ring; TCP e TLS, só aceitas) |
| `syslog_udp_receive_errors_total` | counter | — |
| `syslog_udp_read_buffer_bytes` | gauge | — (o `SO_RCVBUF` efetivo, lido de volta) |
| `syslog_ring_items`, `syslog_ring_bytes` | gauge | — (global com `ring=redis`) |
| `syslog_ring_capacity_items`, `syslog_ring_capacity_bytes` | gauge | — |
| `syslog_ring_quarantined_total` | counter | — |
| `syslog_ring_redis_claimed_total` | counter | — (entradas reivindicadas por `XAUTOCLAIM`) |
| `syslog_sink_batch_size`, `syslog_sink_batch_bytes` | histogram | — (por pedaço) |
| `syslog_sink_duration_seconds` | histogram | — |
| `syslog_sink_health_duration_seconds` | histogram | — |
| `syslog_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); o cancelamento do shutdown não conta |
| `syslog_sink_up` | gauge | — |
| `syslog_sink_sqlite_file_bytes`, `_rotations_total`, `_files_removed_total` | gauge, counter | — |
| `syslog_sink_postgres_duplicates_total`, `syslog_sink_mysql_duplicates_total`, `syslog_sink_nats_duplicates_total` | counter | — |
| `syslog_sink_postgres_pool_conns`, `syslog_sink_mysql_pool_conns` | gauge | `state` (acquired, idle, max) |
| `syslog_sink_postgres_pool_waits_total`, `syslog_sink_mysql_pool_waits_total` | counter | — |
| `syslog_sink_nats_reconnects_total` | counter | — |
| `syslog_sink_clickhouse_dedup_blocks_total` | counter | — (blocos descartados pelo token, §10.10.4) |
| `syslog_sink_clickhouse_pool_conns` | gauge | `state` (acquired, idle, max) |
| `syslog_events_written_total` | counter | — |
| `syslog_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero, como todo label de conjunto fechado (um `rate()` precisa da série antes do primeiro evento) |
| `syslog_emergency_files`, `syslog_emergency_events_pending` | gauge | — |
| `syslog_beacons_total` | counter | `result` (ok, error) |
| `syslog_ratelimit_buckets` | gauge | — |
| `syslog_build_info` | gauge | `version`, `commit`, `go` |
| `syslog_config_info` | gauge | `ring_mode`, `sink_type`, `transports` |
| `syslog_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

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
- Env: `SYSLOG_<SEÇÃO>_<CHAVE>` em maiúsculas, `.` vira `_`
  (`SYSLOG_SINK_NATS_URLS`), com as exceções de §16.2.
- INI: seção `[ring]`, chave `capacity_items`.
- `--config=/caminho.ini` (env `SYSLOG_CONFIG`) indica o arquivo.

### 16.2 Registry único, e os nomes de env fixados pelas skills

Uma **tabela** declara cada parâmetro (seção, chave, tipo, padrão, descrição, se
é recarregável, nome de env). Dela saem o INI, as flags, o env, o `--help`, o
`--dump-config` e a validação. Um parâmetro novo entra só ali. Sem
`gopkg.in/ini.v1` e sem `spf13/pflag`.

Nomes de env que **não** seguem a regra, porque uma skill os fixou:

| Seção | Env | Origem |
|---|---|---|
| `[syslog]` | `SYSLOG_<CHAVE>` (sem repetir a seção): `SYSLOG_DEFAULT_PRIORITY`, `SYSLOG_RFC3164_TIMEZONE`, `SYSLOG_MAX_MESSAGE_BYTES`, `SYSLOG_PAYLOAD_RAW`, `SYSLOG_LEGACY_CHARSET`, `SYSLOG_ACCEPT_MALFORMED`, `SYSLOG_SD_MAX_ELEMENTS`, `SYSLOG_SD_MAX_PARAMS` | skill `loghub-type-syslog-schema` §15 |
| `[loghub]` | `LOGHUB_<CHAVE>`: `LOGHUB_EB_URL`, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE` | skill `loghub-json-schema` |
| `server.data_dir` | `DATADIR` | `go-loghub-ident` |

As variáveis de identidade (`AGENT_UUID`, `AGENT_NAME`, `WORKSPACE`, `HOSTNAME`,
`MACHINE_ID`, `MACHINE_ID_FILE`, `LOGHUB_IDENT_DEBUG`) **não** passam pelo
registry: são lidas pelo `go-loghub-ident`, e o `--dump-config` mostra os valores
o que o **ambiente** diz numa seção informativa `[identity]`, sem resolver nada
(§13.1 passo 1: o dump não toca identidade nem disco).

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
`WARN`, com a origem que manda (`env SYSLOG_FILTER_MIN_SEVERITY`), mesmo quando o
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
udp  = :1514
tcp  = :1514
tls  = :6514
unix =

[tls]
cert_file   = /etc/syslog/tls/server.crt
key_file    = /etc/syslog/tls/server.key
client_auth = optional
client_ca   = /etc/syslog/tls/clients-ca.crt

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
bind = 0.0.0.0:9102
```

### 16.5 Tabela de parâmetros

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `ring.sqlite.commit_max_delay`,
`ring.sqlite.busy_timeout`, `ring.redis.claim_idle`, `sink.sqlite.busy_timeout`,
`ratelimit.sweep_interval`, `shutdown.drain_timeout`, `tcp.backpressure_timeout`,
`tls.backpressure_timeout`, `tls.handshake_timeout` e `proxy.header_timeout` —
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
| `udp` | `:1514` | RFC 5426 |
| `udp_listeners` | `1` | `>1` habilita `SO_REUSEPORT` |
| `udp_read_buffer` | `8MiB` | `SO_RCVBUF`; o efetivo é exposto |
| `tcp` | `:1514` | RFC 6587 |
| `tcp_listeners` | `1` | |
| `tls` | `""` | RFC 5425; `:6514` é o costume |
| `tls_listeners` | `1` | |
| `unix` | `""` | socket datagram; `/dev/log` é o costume |
| `unix_mode` | `0666` | |
| `unix_read_buffer` | `8MiB` | |

#### `[tcp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `framing` | `auto` | `auto` \| `octet` \| `lf`; decidido uma vez por conexão (§5.2) |
| `idle_timeout` | `120s` | `0` = nunca |
| `max_conns` | `0` | por transporte; bloqueia no accept |
| `max_conns_per_ip` | `0` | por transporte; fecha na hora |
| `backpressure_timeout` | `30s` | |

#### `[tls]`

| Chave | Padrão | Descrição |
|---|---|---|
| `cert_file`, `key_file` | `""` | obrigatórios com `listen.tls` |
| `client_auth` | `optional` | `optional` \| `require` \| `none` (§4.5) |
| `client_ca` | `""` | CA dos clientes; obrigatória com `require` |
| `min_version` | `1.2` | `1.2` \| `1.3` |
| `handshake_timeout` | `10s` | |
| `idle_timeout`, `max_conns`, `max_conns_per_ip`, `backpressure_timeout` | como `[tcp]` | |

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
| `max_message_bytes` | `SYSLOG_MAX_MESSAGE_BYTES` | `64KiB` | truncamento (§5.5); 480 a 16 MiB |
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
| `stream` | `syslog:ring` | |
| `group` | `syslog-ring` | |
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
| `admin.bind` | `127.0.0.1:9102` |
| `log.level` | `info` (recarregável) |
| `log.format` | `json` |
| `log.access`, `log.access_sample` | `false`, `0.01` (recarregáveis) |
| `shutdown.pre_stop_delay` | `0s` |
| `shutdown.drain_timeout` | `30s` |

### 16.6 Escrita em disco: uma raiz só

Tudo o que o serviço grava — a identidade (`agent_uuid`, `machine_id`, gravados
pelo `go-loghub-ident`), `ring.db`, `events.db` e `emergency/` — fica sob
`server.data_dir`, e no contêiner essa raiz é `/data` (os compose sobem com
`read_only: true`). **A única exceção é `listen.unix`**, um socket que existe para
ser encontrado onde a libc procura.

Caminhos fixos do contêiner: `/app` guarda o binário (dono `root`), `/data` é a
única área gravável, `/etc/syslog` é o INI em `:ro` (os compose montam
`docker/conf/` ali e definem `SYSLOG_CONFIG=/etc/syslog/syslog.ini`, que traz as
chaves recarregáveis, §16.3), `/etc/syslog/tls` são os certificados
(`docker/tls/`, também `:ro`), `/tmp` é tmpfs. `WORKDIR
/data`; o binário é invocado por `/app/go-syslog-server`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

---

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-syslog-server/               ESTE módulo
  go-syslog-standard-server/      o antigo, referência (copiar com adaptação, nunca importar)
  go-relplog-server/              só especificação, futuro
  go-webhook-server/, go-webhook-fast-server/
```

Módulo independente. Nada é importado de outro módulo do monorepo: compartilhar
pacotes faria os módulos evoluírem amarrados.

Os nomes `go-syslog-server` (binário, imagem Docker, `--version`, tags
`go-syslog-server/vX.Y.Z`) e o prefixo `sls-` dos projetos, contêineres e
volumes Compose são **deste** módulo. A standard já os usou e precisa de nomes
próprios: com a mesma imagem e o mesmo nome de projeto, um `docker compose up`
feito na pasta dela retaguearia a imagem daqui e assumiria os contêineres e os
volumes desta pilha, sem aviso.

### 17.2 Layout interno

```
cmd/go-syslog-server/main.go   flags que encerram, lhident.Initialize(), chama internal/app
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
  receiver/   udp (+ pktinfo por plataforma), unix, tcp, tls, moldura,
              proxy, limites, ingest
  filter/     facility e severidade                                         [lógica pura]
  ratelimit/  token bucket com shards e sweep                               [lógica pura]
  ring/       interface Ring + memory, sqlite, redis, none
  sink/       interface Sink + nats, redis, sqlite, postgres, mysql, none,
              workers, health, tls
  control/    heartbeat beacon
  emergency/  dump e reingestão
  obs/        métricas Prometheus próprias + slog
```

`convert/` e o leitor do PROXY em `receiver/` são os únicos parsers de entrada
adversária. Os alvos de fuzz obrigatórios são esses e os decodificadores do que
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
`golang.org/x/net` e `golang.org/x/text` **não** são importados por este código:
PKTINFO e o charset legado são feitos com a stdlib (`syscall`) e com tabela
própria. O `golang.org/x/text` ainda aparece no binário, **transitivamente**,
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
| `FuzzFrame` | `receiver` | as duas molduras: sem pânico, sem alocação pelo MSG-LEN declarado, `size ≥ len(cru)` |
| `FuzzProxy` | `receiver` | v1 e v2: sem pânico, nunca lê além do cabeçalho declarado |
| `FuzzHead` | `item` | o decodificador do cabeçalho recusa sem pânico; ida e volta é identidade |
| `FuzzRead` | `emergency` | o dump tolera truncamento sem pânico |
| `FuzzJSONBSafe` | `sink` | a troca do `\u0000` para a `loghub.inbox` (§3.7.1) devolve JSON válido igual ao original com U+0000 → U+FFFD, e nada mais |
| `FuzzInboxPayload` | `sink` | o payload da coluna é JSON sem escape NUL, `raw` volta exato e os demais textos só trocam U+0000 por U+FFFD |

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$ -fuzztime=10m
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$    -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$ -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrameOracle$ -fuzztime=10m
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
servidor externo: todos os transportes chegam ao destino (UDP com `dst-ip`, TCP
nas duas molduras, TLS com e sem certificado de cliente, unix), PROXY v1 e v2,
fidelidade do JLH (o `raw` volta byte a byte, com NUL no meio), filtro e
`SIGHUP` (ida e volta), dump de emergência e reingestão exatos (mesmo `uuid`, mesmo
JLH), shutdown gracioso sem perda, segundo sinal (130), nenhum transporte
(exit 1), códigos 100–114 do `go-loghub-ident` num subprocesso, e a ordem do boot.

### 18.6 Destinos reais

Suítes que pulam sem a variável e criam o que precisam **a partir do arquivo que
o operador aplica** (`docker/postgres/inbox.sql`, `docker/mysql/schema.sql`,
`docker/nats/stream.json`, `docker/clickhouse/inbox_syslog.sql`):

```bash
SYSLOG_TEST_REDIS_ADDR=127.0.0.1:16379      go test -race -timeout 600s ./internal/sink/ ./internal/ring/ ./internal/app/
SYSLOG_TEST_PG_DSN='postgres://...'         go test -race -timeout 600s ./internal/sink/ ./internal/app/
SYSLOG_TEST_MYSQL_DSN='root:...@tcp(...)/x' go test -race -timeout 600s ./internal/sink/ ./internal/app/
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 go test -race -timeout 600s ./internal/sink/ ./internal/control/ ./internal/app/
SYSLOG_TEST_CLICKHOUSE_DSN='clickhouse://...:19000/x' go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

O ClickHouse (25.8+, protocolo nativo) cria cada tabela de
`docker/clickhouse/inbox_syslog.sql`. A suíte prova a ida e volta pelo oráculo
(SD sem parâmetros, nomes com ponto, `PARAM-NAME` repetido, NUL, um JLH grande),
a regra de leitura `"structured-data":{}` = ausente, o token descartando o
retry e o merge colapsando o reagrupamento, uma tabela recusada no boot para
cada regra de §10.10.5 (a tabela dos webhooks inclusive), privilégio,
`readonly`, limite de memória, a tabela de §10.10.7 lida da própria SPEC, os
quatro rings ponta a ponta e o destino caindo e voltando sem perda.

O ring Redis tem suíte própria contra `SYSLOG_TEST_REDIS_ADDR`: capacidade
global entre duas instâncias, `XAUTOCLAIM` de uma instância morta, `Touch` que
impede o roubo de um lote vivo, contador de bytes exato depois de duplicatas.

Nenhuma ferramenta dos testes ou do Compose usa `latest`: o `nats-server` dos
testes é o v2.14.6, o `natsio/nats-box` do serviço `nats-init` (que cria o
stream a partir de `docker/nats/stream.json`) é o 0.20.0, e as imagens dos
destinos fixam a versão (`nats:2.11`, `redis:7.4`, `postgres:18`, `mysql:8.4`,
`clickhouse-server:25.8`). Com `latest`, o stream criado ou o
resultado de um teste mudariam sem mudança no repositório.

### 18.7 Emissores vivos

`SYSLOG_TEST_EMITTERS=1` sobe rsyslog e syslog-ng em contêineres descartáveis e
confere, por transporte e moldura: rsyslog `omfwd` UDP, TCP LF
(`RSYSLOG_ForwardFormat`), TCP octet (`RSYSLOG_SyslogProtocol23Format`), **TLS
(`gtls`)**; syslog-ng `network()` UDP e TCP LF, `syslog()` TCP octet. É a única
prova de que a moldura e o TLS funcionam no fio.

### 18.8 Critérios de aceite v1

**Nunca medidos.** Alvo, não fato; a primeira medição substitui esta tabela.

| Cenário | Alvo |
|---|---|
| UDP, `memory`+`nats`, mensagens de 200 B | ≥ 150k msg/s por núcleo de recepção |
| TCP LF, `memory`+`nats` | ≥ 120k msg/s |
| TLS, `memory`+`nats` | ≥ 80k msg/s |
| `sqlite`+`nats`, TCP | ≥ 20k msg/s, p99 de confirmação < 20 ms |
| Perda com `SIGTERM` durante carga, TCP | **zero**, reconciliada |
| Perda com FATAL do sink, `memory` | **zero**, via dump e reingestão |

---

## 19. Matriz de garantias

Leia junto com §2.3: a garantia só existe nos transportes que confirmam (TCP e
TLS).

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

| Transporte | A confirmação existe? | O que a substitui |
|---|---|---|
| TCP, TLS | em parte: a retomada da leitura | nada se perde, mas o emissor não sabe *qual* mensagem foi aceita |
| unix dgram | não | o bloqueio do `send` pelo kernel |
| UDP | não | `syslog_messages_dropped_total` e os contadores da interface |

Nenhum transporte deste servidor confirma **por mensagem**: era o papel do RELP,
que virou software próprio (§1.2).

---

## 20. Invariantes

Se uma alteração conflita com um destes, ela está errada.

1. A confirmação nunca sai antes da garantia da combinação `ring` × `sink`; onde
   não há confirmação, há contador.
2. Ring cheio nunca vira aceitação seguida de descarte, exceto com `sink=none`.
   UDP e unix descartam antes de aceitar; TCP e TLS seguram o emissor.
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
    inclusive a identidade, **por padrão**: os caminhos derivados ficam lá, e a
    única exceção fixa é o socket `listen.unix`. Um `ring.sqlite.path`,
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

---

## 21. Roteiro pós-v1

Nada disto entra antes de a v1 estar em produção e §18.8 ter sido medido.

| Item | Motivo de ter ficado fora |
|---|---|
| RELP | Software próprio, `go-relplog-server/` |
| Importação de arquivo (`transport=file`, Caso 2 do JLH) | Fora da v1 (decisão 5) |
| `space` por listener ou rede | Multi-tenant numa instância; fora da v1 (decisão 18) |
| Resposta a sonar, ping e pong | Até existir um `go-loghub-driver` |
| Troca de certificado TLS sem restart | Recarga de `tls.cert_file` por `SIGHUP` |
| DTLS (RFC 6012) | Exige dependência nova |
| Impressão digital da config de conversão no cabeçalho | Detectaria réplicas com config divergente no ring Redis (§3.5) |
| `sink=syslog` (encaminhar) | Política de retry própria e decisão sobre o `hostname` |
| Particionamento por tempo no PostgreSQL e no MySQL | Precisa de decisão sobre quem cria a partição |
| `sink=clickhouse` numa tabela `Distributed` | O `INSERT` vai aos shards de forma assíncrona; a confirmação exigiria decidir o que é "entregue" num cluster (§10.10.5) |
| Esquema `clickhouse://` na `LOGHUB_EB_URL` | Hoje o `sink=clickhouse` só se configura pelas chaves de `[sink.clickhouse]` (§10.9) |

---

## 22. Lacunas e riscos

Os riscos do desenho estão em `docs/DECISOES.md` §4, e o que os testes ainda não
provam, em `docs/TESTING.md` §8. Esta seção guarda o que o **código atual** ainda
faz diferente do que deveria. Cada item diz o problema, a consequência e a
direção da correção, que segue a regra de sempre: a spec primeiro (a regra entra
na seção indicada), depois o código e o teste. Corrigido um item, ele sai daqui.

### 22.1 Pendências conhecidas

**Transportes (§4, §5)**

1. **Buffer de leitura fixo por conexão.** Toda conexão TCP (já no accept) e TLS
   (depois do handshake) segura um `bufio.Reader` de 64 KiB enquanto vive,
   ociosa ou não, fora da conta de `ring.max_bytes`; com `max_conns = 0` (o
   padrão), alguns milhares de conexões caladas levam o heap ao `GOMEMLIMIT`.
   Direção: 4 KiB (o maior `Peek` é de 12 bytes, a assinatura PROXY v2), medido
   com o benchmark de recepção, e o custo fixo por conexão escrito em §5.4/§15.
2. **Teto por IP contornável com IPv6.** `max_conns_per_ip` conta o endereço
   exato (um /128): quem tem um /64 abre cada conexão de um endereço novo.
   Direção, a decidir pelo usuário: documentar em §4.4, ou contar por prefixo
   IPv6 configurável, como `ratelimit.v6_mask`, sabendo que um /64 pode ser uma
   LAN inteira de emissores.
3. **Shutdown contado como falha de PROXY.** O `Close` vence o prazo de quem
   ainda espera o cabeçalho PROXY, e isso conta `proxy_headers_total{absent}`
   (ou `invalid`, com o cabeçalho pela metade) e, em `required`,
   `messages_dropped_total{reason="proxy"}`, com um `WARN` que culpa o
   balanceador: um deploy parece falha dele. Direção: erro com `closing` ligado
   sai sem contar nada, como o handshake TLS já faz.

**Conversão (§6)**

4. **Offset fora de faixa no timestamp ISO da RFC 3164.** `+25:00` e `+05:75`
   são aceitos e deslocam `payload.timestamp` sem aviso, enquanto na RFC 5424
   dão `timestamp-invalid` (§6.4.2, §6.9). O conversor de referência da skill
   faz igual. Direção, a decidir pelo usuário: conferir a faixa
   (`timestamp-invalid`, o resto do cabeçalho segue lido) como desvio D9,
   avisando a skill; ou registrar em §6.5.1 que qualquer `hh:mm` é aceito.

**Redis (§9.5, §10.3)**

5. **Eviction `allkeys-*` apaga o stream.** Com `maxmemory-policy` `allkeys-*`,
   um Redis cheio não responde `OOM` (que é fatal): despeja chaves, e o stream,
   que é uma chave só, leva todas as entradas, inclusive o ring Redis inteiro e
   o contador dele, com o serviço UP. Direção: ler `maxmemory_policy`
   (`INFO memory`) no boot e recusar `allkeys-*` (exit 3), com `WARN` se a ACL
   não deixar ler; escrever a premissa (`noeviction`, ou `volatile-*` sem TTL
   nestas chaves) em §9.5 e §10.3.

**PostgreSQL, MySQL e ClickHouse (§10.5, §10.6, §10.10)**

6. **Validação do boot no prazo da checagem de saúde.** No PostgreSQL e no
   MySQL, a dúzia de idas e voltas roda sob `sink.health_timeout` (1 s): com o
   banco a mais de ~55 ms, nenhuma tentativa cabe e o boot sai com 3 depois do
   `boot_timeout`, apontando a rede. A do ClickHouse já tem `sink.op_timeout`,
   mas ignora o cancelamento do boot (um `SIGTERM` espera até esse prazo).
   Direção: prazo próprio de `sink.op_timeout` nas três, cancelável pelo boot.
7. **SQLSTATE `23502` é `logic`.** O servidor nunca manda `NULL` nas dez
   colunas, então um `not_null_violation` depois do boot vem da tabela (coluna
   obrigatória acrescentada, `DROP DEFAULT`, trigger) e descarta todo pedaço
   com o sink UP; o `1364` do MySQL já é `fatal` por isso. Direção: `23502`
   `fatal`, na tabela de §10.5 e em `TestClassifySQLState`.
8. **SQLSTATE `3D` é `network`.** Um `sink.postgres.database` inexistente
   (`3D000`) é retentado até o `boot_timeout` e sai com a mensagem de rede,
   enquanto o `1049` do MySQL é `fatal`. Direção: classe `3D` `fatal` (§10.5).
9. **O ambiente libpq ainda chega ao pool.** `pgxpool.ParseConfig("")` lê todo
   `PG*` e só os campos do registry são sobrescritos: `PGSSLMODE` ou `PGPORT`
   inválidos e `PGSERVICE` inexistente derrubam o boot como erro de
   configuração, e `PGCHANNELBINDING`, `PGREQUIREAUTH`, `PGSSLNEGOTIATION` e
   `PGMIN/MAXPROTOCOLVERSION` sobrevivem e podem recusar toda conexão.
   Direção: parsear com todo `PG*` escondido (sob mutex, só no boot) e ampliar
   `TestNewPostgresIgnoresTheLibpqEnvironment`.
10. **MySQL sem `autocommit` fixado.** A sessão fixa só `sql_mode`; com
    `autocommit=0` global (`my.cnf`, `init_connect`), o `INSERT` fica numa
    transação implícita que ninguém commita, o `Write` devolve sucesso e as
    linhas somem quando a conexão fecha (a entrega parcial de §10.2 também
    depende do autocommit). Direção: `autocommit=1` nos parâmetros da sessão e,
    opcionalmente, `@@session.autocommit` conferido na validação.
11. **MySQL: outros índices únicos, prefixo e números desconhecidos.** O
    `ON DUPLICATE KEY` dispara em **qualquer** índice único, e a validação
    aceita outro único ao lado de `(uuid, step)` e um `UNIQUE (uuid(4), step)`
    com prefixo: um evento novo que colida neles é contado como duplicata e
    confirmado, perdido em silêncio. Um número fora da tabela de §10.6 (`3819`,
    o `4025` do MariaDB) é `network` para sempre, sem recorrer ao SQLSTATE do
    erro; o `1062` listado como `logic` é inalcançável; e a sonda não pega uma
    coluna de texto estreita (`space VARCHAR(16)`), que depois recusa linhas com
    `1406`. Direção: recusar outro índice único e `SUB_PART`, recorrer ao
    SQLSTATE (`22`/`23` `logic`, `42`/`28` `fatal`), corrigir o `1062` em §10.6
    e conferir larguras.
12. **Nome do banco cru no SQL.** `sink.mysql.database` e
    `sink.clickhouse.database` entram entre crases no `INSERT` sem validação,
    embora a conferência da tabela exista justamente contra injeção; um nome com
    crase quebra o comando (no MySQL, o `1064` vira `network` e a sonda espera o
    `boot_timeout`). Direção: recusar crase, barra invertida, bytes de controle
    e mais de 64 bytes, na config e no construtor.

**NATS (§10.7, §11)**

13. **Credenciais na URL do NATS.** O log de conexão usa
    `ConnectedUrlRedacted`, que esconde a senha mas não um token
    (`nats://<token>@host`, que a `LOGHUB_EB_URL` aceita); `sink.nats.urls` não
    é parseada na validação, e uma senha com `/`, `#` ou `?` sem escape vira um
    host errado ou um erro do cliente que ecoa a URL inteira a cada retentativa;
    e credencial na URL junto de `username`/`password`/`token`/`credentials` é
    aceita, com as chaves ignoradas pelo cliente, contra "uma forma só" de §10.7.
    Direção: logar só esquema e host; parsear cada entrada sem eco (host
    obrigatório; sem caminho, consulta nem fragmento em `nats://`/`tls://`;
    `host:porta` sem esquema continua aceito); recusar credencial nos dois
    lugares.
14. **Permissão negada nunca vira FATAL.** O NATS recusa um publish sem
    permissão só com um `-ERR` assíncrono, que o sink apenas loga: em
    `jetstream`, cada futuro expira como `network` e o worker retenta para
    sempre com o sink UP; em `core`, o flush volta sem erro e o pedaço é contado
    como escrito e confirmado, perda total e silenciosa, contra §20.18.
    Direção: um veredito permanente, gravado pelo `ErrorHandler` quando a
    negação atinge o caminho de dados (`$JS.API`, o subject exato e o de
    `facility-severity`, a assinatura de `_INBOX` das confirmações; nunca o
    subject do beacon) e devolvido como `fatal` pelo `Write`, pelo `writeCore`
    (que lê o `LastError` depois do flush) e pelo `Health`.
15. **`ErrMaxPayload` no meio do pedaço descarta o pedaço.** A recusa síncrona
    do cliente (um `max_payload` menor depois do boot, numa reconexão a outro
    nó) devolve `logic` sem `Error.Delivered`, em `jetstream` e em `core`: as
    mensagens anteriores, já publicadas, contam como perdidas, e as seguintes,
    nunca enviadas, são confirmadas. Direção: tratá-la como um `10054`, só
    daquela mensagem, publicar as demais e preencher `Delivered` (§10.2).
16. **Contrato do stream conferido só no boot.** Depois da primeira validação,
    o `Health` só faz `js.Stream`: um stream editado com o servidor no ar
    (subjects estreitados, `no_ack`, `max_msg_size` menor, `loghub.>`) deixa
    toda escrita `network` para sempre com o sink UP, ou descarta como `logic`;
    em `core`, um `max_payload` menor numa reconexão passa sem conferência.
    Direção: `validate`/`validateSize` em toda checagem, sobre o `CachedInfo`
    já buscado (sem ida e volta a mais), sem passar a exigir, depois da
    reingestão, a cobertura dos subjects de outro `space` que um dump trouxe.
17. **`retention: interest` aceito.** Sem consumidor interessado, o NATS
    responde ao publish com um `PubAck` normal e não guarda nada: pedaço
    confirmado e perdido, sem erro nem contador. Direção, a decidir pelo
    usuário: recusar na validação (`fatal`) ou só avisar; avisar também
    `max_msgs_per_subject > 0`, porque todo evento de um `space` divide um
    subject.
18. **4xx transitórios viram FATAL.** A regra "outro 4xx é `fatal`" pega o
    `10158` (409: o mesmo `Nats-Msg-Id` ainda numa proposta Raft, na
    retentativa depois de um prazo num stream em cluster) e o `10167` (429: a
    fila de entrada do stream cheia), que passam sozinhos: dump e exit 3, talvez
    na frota inteira de uma vez. Direção: os dois `network` (talvez todo 429),
    com linha na tabela de §10.7 e caso em `TestClassifyNATS`.
19. **Recusa de credencial no primeiro `CONNECT` vira `network`.** Ali o
    cliente devolve o texto do servidor, e "User Authentication Expired" ou
    "Revoked" não casam por `errors.Is` com `ErrAuthExpired`; `ErrAuthRevoked` e
    `ErrAccountAuthExpired` nem estão na tabela. O boot espera o `boot_timeout`
    e sai com a mensagem de rede. Direção: as sentinelas e um casamento textual
    das frases de credencial (nunca "permissions violation") no ramo `fatal`.
20. **`core` publica dump de outro `space` sem aviso.** Sem stream, o dump de
    uma vida com outro Workspace vai para um subject que provavelmente ninguém
    assina, é perdido e o arquivo é removido como entregue (§12.2 só cobre o
    `jetstream`). Direção: um `WARN` por subject estrangeiro, comparando o
    `space` do item com o Workspace, sem alocar no caminho quente.
21. **`loghub.beacon_interval` sem piso.** `1ns` ou `15ms` (no lugar de `15s`)
    são aceitos, e o beacon vira um laço que inunda `<ctl_namespace>.beacon` e
    disputa a conexão dos dados. Direção: fora o `0`, mínimo de `1s`, exit 1.

**`LOGHUB_EB_URL` (§10.9)**

22. **URL PostgreSQL.** Com `host=/socket`, a porta da URL é descartada e o
    cliente disca `.s.PGSQL.5432`, talvez outro cluster, sem como corrigir,
    porque `addr` é chave dona da URL; um `sslmode` repetido vale o primeiro
    (a libpq vale o último), e `?sslmode=disable&sslmode=require` desliga o TLS;
    e `sink.postgres.addr` com dois hosts (`h1,h2:5432`) passa na validação e
    falha no DNS até o `boot_timeout`. Direção: `addr = socket:porta`, recusar
    parâmetro repetido e validar `addr` como um `host:porta` ou um diretório de
    socket.

**Dump de emergência (§12)**

23. **Dump escrito direto no nome final.** O arquivo nasce com o nome
    `emg-*.bin` que o boot reingere: um processo morto no meio da escrita
    (`SIGKILL`, OOM, aborto) deixa um arquivo parcial com cara de pronto, e com
    o corte numa fronteira de registro o boot entrega a metade, remove o arquivo
    e ninguém conta o resto; num diretório compartilhado, outra instância o pega
    enquanto é escrito. Direção (§12.1): temporário oculto, `fsync`, publicação
    por `link` (com `rename` quando o sistema de arquivos não tem hard link,
    senão todo dump falharia ali) e `fsync` do diretório; no boot, um `WARN`
    com os temporários interrompidos, nunca lidos nem apagados.
24. **Quarentena logada como não feita.** Quando o `rename` para `.corrupt` já
    aconteceu e só o `fsync` do diretório falhou, a reingestão loga "cannot
    quarantine" com o caminho antigo e omite o `ERROR` com os bytes
    descartados. Direção: com o nome devolvido, tratar como quarentenado e logar
    o `fsync` em `WARN`.
25. **Gauges de emergência zerados com dump no disco.** No fim da reingestão,
    `syslog_emergency_files` e `syslog_emergency_events_pending` vão a zero
    mesmo com um arquivo que ficou (ilegível, ou a remoção ou a quarentena
    falharam), e o alerta sobre eles não dispara; o `Store.Count` feito para eles
    não tem chamador. Direção: recontar a partir do diretório no fim.
26. **Reingestão com `sink=none` destrói o dump.** A reingestão vale em
    qualquer modo (§12.2), também com `sink=none`: o dump é "entregue" ao
    descarte e removido. Direção, a decidir pelo usuário: com `sink=none`,
    manter os arquivos e logar `WARN`.

**Ciclo de vida (§13)**

27. **Log da recusa fatal no boot cita o `boot_timeout`.** Quando o `WaitReady`
    desiste na hora por um erro `fatal` (§13.1 passo 7), a última linha ainda
    diz "sink did not become usable within sink.boot_timeout" e manda o operador
    olhar a rede. Direção: separar as mensagens com `sink.IsFatal`, como o ring
    Redis já faz.
28. **O primeiro sinal aborta o shutdown por FATAL.** Esse shutdown começa sem
    sinal, mas o primeiro `SIGTERM`/`SIGINT` que chega (um `docker stop`, um
    deploy) é tratado como o segundo de §13.4: exit 130 no meio do
    `workers.Wait` ou do dump, com o ring perdido, contra §13.3 (FATAL é §12 e
    exit 3). Direção: no shutdown por FATAL, tolerar o primeiro
    `SIGTERM`/`SIGINT` (logado; o segundo aborta) e nunca o `SIGQUIT`, que
    aborta sempre; a regra entra em §13.4.
29. **Aborto durante o dump subconta a perda.** O `Drain` tira os itens da
    contabilidade do ring antes do dump, e o `abortLoss` conta
    `Stats().Items`, que então é só o lote retido pelos workers:
    `syslog_events_dropped_total{reason="abort"}` registra uma fração da perda.
    Direção: um contador atômico com o tamanho do que está sendo despejado,
    usado pelo `abortLoss` enquanto não for zero e zerado depois do dump (antes
    de contar `emergency_failed`); enquanto o item 23 não for corrigido, a conta
    é conservadora.

**Configuração (§16)**

30. **Erros de configuração ecoam trechos de segredo.** O erro da
    `LOGHUB_EB_URL` repassa o texto do `url.Parse` (`invalid port ":<x>"` é a
    senha até a primeira `/`), o de parâmetro de consulta desconhecido do
    PostgreSQL cita a "chave" (o resto de uma senha com `?`, o host e o banco),
    e o INI (`got %q`, a linha inteira, inclusive `password: x` sem `=`) e as
    flags (`unexpected argument %q`) citam a entrada; vão para o stderr e, num
    `SIGHUP`, para o log. Direção: mensagens sem texto da entrada (número da
    linha, posição do argumento, dica de escape), e uma chave de consulta citada
    só se estiver numa lista fixa da libpq.
