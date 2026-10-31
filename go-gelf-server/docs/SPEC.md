# go-gelf-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-31
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-gelf-server`
Binário: `go-gelf-server`
Entrada: `docs/DECISOES.md` (decisões 1–19 da família GELF, 2026-10-31, e as
tomadas na implementação). Cada decisão de lá está aqui, com o número da seção.
Pai do código: `go-syslog-server` (S), versão de 2026-10-14, com as correções
das rodadas de porte (CHANGELOG da raiz, 2026-10-13 e 2026-10-14). Tudo o que
não é GELF — rings, sinks, beacon, dump, admin, `SIGHUP`, registry, PROXY, TLS —
é o de S, e as seções correspondentes desta SPEC repetem as de S com os nomes
trocados (`syslog_` → `gelf_`, `SYSLOG_` → `GELF_`, `syslog_events` →
`gelf_events`, `inbox_syslog` → `inbox_gelf`). Onde a regra muda, a seção diz.

Servidor de ingestão **GELF** (Graylog Extended Log Format 1.1) do ecossistema
**LogHub**. Recebe documentos GELF por UDP (com chunks e compressão), TCP
(moldura `\0`), TCP+TLS, HTTP (`POST /gelf`) e HTTPS, converte cada um num
envelope **JLH** com `type=gelf` e o entrega a um destino configurável — por
padrão, o eventbus NATS JetStream em `loghub.{space}.gelf`. É um
**Bridge-IN**: cria a primeira mensagem de cada fato no eventbus (`step = 0`).

---

## 1. Escopo

### 1.1 Objetivos

- Máximo de mensagens por segundo com mínimo de CPU e memória.
- Cada documento aceito vira **exatamente um** envelope JLH válido pelo schema
  do envelope (`loghub-json-schema`) e pelo `jlh-gelf-payload.schema.json`
  deste repositório (§3.2).
- **Nunca confirmar** um documento que não foi aceito segundo a garantia da
  combinação `ring` × `sink`, nos transportes que têm como confirmar (§2.3).
- Backpressure explícito onde o transporte permite (TCP/TLS: parar de ler;
  HTTP: `503`); perda **contada e visível** onde ele não permite (UDP).
- **Sem perda e sem invenção** no payload: o que chegou está nos campos ou em
  `raw`; o que não chegou é omitido, nunca adivinhado (§6) — com uma exceção
  deliberada, o `level` padrão (D1).
- Operar acoplado ao eventbus (`sink=nats`, `sink=redis`), de forma autônoma
  (`sink=sqlite`) ou contra um banco compartilhado (`sink=postgres`,
  `sink=mysql`, `sink=clickhouse`).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| GELF por AMQP/Kafka | Transportes do Graylog sem uso nos emissores alvo; um consumidor de fila é outro software |
| Confirmação por mensagem no TCP | O GELF não tem; a confirmação é continuar lendo (§2.3) |
| HTTP/2 (h2c e h2) | HTTP/1.1 com keep-alive basta para os emissores GELF; o backpressure é o `503` por requisição (§4.5) |
| `X-Forwarded-For` | A origem é o socket ou o PROXY protocol de um par confiável (§4.6) |
| Compressão no TCP | A spec GELF não tem; um documento comprimido no TCP é `malformed` |
| `space` por listener ou rede | Um Workspace por instância |
| Validação por JSON Schema em execução | Só em teste e CI (D3) |
| Unix datagram, RFC 3164/5424, charset legado, subjects por facility | São do syslog; não existem aqui |

### 1.3 Fontes normativas e desvios

| Fonte | Versão | Papel |
|---|---|---|
| GELF Payload Specification (Graylog) | 1.1 | Documento, campos, `_` adicionais, chunks, compressão, `\0`, HTTP |
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, `jlh-envelope.schema.json` |
| `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json` | este repositório | Payload `type=gelf` (não há skill `type=gelf`, decisão 1) |
| skill `loghub-postgres-table-inbox` | 2026-10-06 | `sink=postgres` (§3.7.1) |
| `go-loghub-uuidv7` | v0.0.3 | UUIDv7 nível 3 |
| `go-loghub-ident` | v0.5.2 | AgentUUID, Workspace, `DATADIR` |

**Desvios deliberados**, cada um com o motivo na seção citada:

| # | Desvio | A fonte diz | Aqui | Seção |
|---|---|---|---|---|
| D1 | `level` ausente | GELF 1.1: `1` (alert) | **6** (info) com aviso `level-default` (decisão 3): muitas bibliotecas omitem `level`, e tudo viraria alerta no LogHub | §6.5 |
| D2 | `host` e `short_message` ausentes | GELF 1.1: obrigatórios (o Graylog recusa o documento) | aceito, com `host-missing` / `short-message-missing` e `short-message: ""` (regra absoluta de §6.1) | §6.5 |
| D3 | Validação | a skill do envelope: todo envelope passa no schema antes da fila | schema só em teste e CI; o serializador é provado por fuzz | §18.2 |
| D4 | `LOGHUB_UUID_LEVEL` | variável sugerida | não existe: nível 3 fixo | §3.1 |
| D5 | Espaço em branco em `host` | GELF: qualquer string | `^\S+$` com o conjunto de S D6 (`\s` do ECMAScript mais U+001C–U+001F e U+0085); outro valor é omitido com `host-invalid` | §6.5 |
| D6 | Tamanho | Graylog aceita ~1 MiB em UDP (128 × 8 KiB) | **128 KiB** do documento descomprimido (§3.9); acima disso, recusa contada | §5.5 |

---

## 2. Arquitetura

### 2.1 Dois eixos ortogonais

- **RING**: `memory` | `sqlite` | `redis` | `none`
- **SINK**: `nats` | `redis` | `sqlite` | `postgres` | `mysql` | `clickhouse` | `none`

As vinte e oito combinações são válidas, com as degradações de §9.7.
**Nenhuma implementação de ring consulta o tipo do sink, e vice-versa.**

```
   emissores (Docker gelf driver, fluent-bit, Logstash, graypy, logstash-gelf, gelf-cli)
      |
      |  UDP :12201 (chunks, gzip/zlib)   TCP :12201 (\0)   TLS (\0)
      |  HTTP :12202 POST /gelf           HTTPS POST /gelf
      v
 +-----------------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                          |
 |   [PROXY] -> [TLS] -> chunks UDP | moldura \0 | corpo HTTP             |
 |   -> descompressão com teto -> amostra de relógio -> [level de topo]   |
 |   -> filtro -> rate-limit -> uuid -> Item(cabeçalho + documento)       |
 +----------------------------------+------------------------------------+
                                    |
                     +--------------v---------------+
                     | RING  memory|sqlite|redis|none|
                     +--------------+---------------+
                                    | sink.workers
                     +--------------v---------------+
                     | CONVERSÃO -> JLH type=gelf   |  <- fora do caminho quente
                     +--------------+---------------+
                                    |
                     +--------------v---------------+
                     | SINK nats|redis|sqlite|postgres|mysql|clickhouse|none
                     +------------------------------+
   loghub.{space}.gelf | stream Redis | gelf_events | loghub.inbox | inbox_gelf

   beacon (só com sink=nats): <ctl_namespace>.beacon a cada 15 s (§11)
   emergência: ring=memory não vazio + sink FATAL => dump (§12)
   porta admin 127.0.0.1:9104: /healthz /readyz /metrics (§14)
```

O ring é **único e global**, compartilhado por todos os transportes.

### 2.2 Onde a conversão acontece, e por quê

Decisão 8. A recepção faz o mínimo que permite decidir se o documento entra; a
conversão completa roda no **worker de dreno**.

| Etapa | Onde roda | Custo |
|---|---|---|
| PROXY, handshake TLS | recepção, uma vez por conexão | do transporte |
| Remontagem de chunks, moldura `\0`, corpo HTTP | recepção | inevitável |
| Descompressão com teto | recepção | proporcional ao documento |
| Amostra de relógio, `uuid` | recepção | uma leitura de relógio, um UUID |
| **`level` de topo** | **recepção, só com filtro ativo** (§7) | uma validação do documento |
| Filtro, rate-limit | recepção | comparações de inteiro, um mapa em shards |
| Validação JSON, campos, números, escapes | **worker de dreno** | proporcional ao documento |
| Serialização do JLH | **worker de dreno** | proporcional aos campos |

A descompressão fica na recepção porque o cru do ring é o **documento
descomprimido** (decisão 8): o ring, o `ring.db`, o stream do ring Redis e o
dump guardam JSON legível, e o teto de §5.5 vale sobre o que o converter vai
ler. O `level` fica na recepção só quando o filtro pode recusar algo: descartar
depois do ring seria aceitar para jogar fora.

Consequências que o resto da spec assume (as de S):

1. **O item no ring não tem envelope.** Ele carrega o documento e o que a
   conversão **não pode** recuperar dele: `uuid`, amostra de relógio,
   transporte, endereços, sessão TLS, compressão, número de chunks, `from` e
   `space` (§3.4).
2. **A contabilidade do ring conta só o documento.**
3. **`ring=sqlite`, `ring=redis` e o dump guardam cabeçalho + documento.**
4. **O JLH é função determinística** de cabeçalho + documento + config de
   conversão (§3.5). Toda chave de `[convert]` exige restart.

Com `ring=none`, a conversão roda na goroutine da recepção.

### 2.3 Regra de aceitação, por transporte

> A confirmação sai no instante em que a garantia da combinação configurada é
> cumprida — nunca antes. **Onde não existe confirmação, existe um contador.**

| Transporte | A confirmação é | Ring cheio, sink DOWN, ring indisponível |
|---|---|---|
| **UDP** | não existe | documento **descartado**, `gelf_messages_dropped_total{reason=...}` |
| **TCP**, **TLS** | continuar lendo do socket | **parar de ler** (§5.7); a janela TCP segura o emissor |
| **HTTP**, **HTTPS** | `202 Accepted` | `503` com `Retry-After: 1` (§4.5); nada é segurado |

Dentro de cada transporte que confirma, a garantia é a do ring (S §2.3):
`memory` = entrou no canal; `sqlite` = `COMMIT` do grupo; `redis` = o script
retornou; `none` = o sink confirmou.

**O UDP perde por desenho do protocolo.** Um chunk perdido pela rede deixa a
mensagem incompleta, descartada no prazo (§5.2). Quem não pode perder usa TCP,
TLS ou HTTP.

### 2.4 Papel no LogHub

| Aspecto | Valor |
|---|---|
| Papel | **Bridge-IN**: `uuid` novo, `step = 0`, `from` = AgentUUID |
| Subject | `loghub.{space}.gelf` (só o exato, decisão 16) |
| Plano de controle | heartbeat beacon (§11) |
| Consumo | nenhum |

---

## 3. Modelo de dados

### 3.1 Envelope JLH

Os nove campos, nesta ordem, e nenhum outro (S §3.1):

```json
{
  "uuid": "01a101a2-aa7b-71c8-b15b-db8fdb816d6a",
  "from": "01962a3f-07bc-70b4-ae05-c57e7541d8a8",
  "space": "default",
  "type": "gelf",
  "level": 6,
  "sid": 593636897809901,
  "step": 0,
  "event": { "uts": 1791028800, "sub": 123456789, "iso": "2026-10-03T12:00:00.123Z" },
  "payload": { ... §3.2 ... }
}
```

| Campo | Regra |
|---|---|
| `uuid` | UUIDv7 **nível 3**, gerado na recepção da amostra de relógio do documento, depois de filtro, rate-limit e estado do sink; reusado em toda retentativa, no dump e na reingestão. |
| `from`, `space` | identidade de quem **recebeu** (cabeçalho do item). |
| `type` | `"gelf"`. |
| `level` | `payload.level.code` (0–7); `6` quando ausente (D1) ou inválido. |
| `sid` | FNV-1a 64 de `transport|src-ip|src-port|dst-ip|dst-port` reduzido a 53 bits (§6.10), com `transport` ∈ `udp`, `tcp`, `tls`, `http`, `https`. Nunca 0 aqui: todo transporte é de rede. |
| `step` | `0`. |
| `event` | a **mesma** amostra do `uuid`. |

**Uma amostra de relógio por documento**: no UDP, o retorno do `recvmsg` do
datagrama que **completa** a mensagem (o último chunk, decisão 11); no TCP/TLS,
a leitura do `\0` (ou do EOF) que fecha o quadro; no HTTP, o fim da leitura do
corpo. Antes de qualquer descompressão ou parse.

`payload.timestamp` (a hora declarada pelo emissor) nunca é usado para
ordenar, indexar ou correlacionar.

### 3.2 Payload `type=gelf`

Contrato: `jlh-gelf-payload.schema.json` (draft 2020-12, `additionalProperties:
false`, propriedades `^x-` livres). Chaves em kebab-case, **nesta ordem fixa**:

| Campo | Tipo | Presença |
|---|---|---|
| `host` | objeto | sempre (a rede existe sempre) |
| `host.hostname` | string `^\S+$` (D5) | se o GELF declarou `host` válido |
| `host.src-ip`, `host.src-port` | IP canônico, int | sempre |
| `host.dst-ip` | IP canônico | se conhecido (PKTINFO no UDP; `LocalAddr` nos demais) |
| `host.dst-port` | int | sempre |
| `transport` | `udp` \| `tcp` \| `tls` \| `http` \| `https` | sempre |
| `compression` | `gzip` \| `zlib` \| `deflate` | só se houve (UDP, HTTP) |
| `chunks` | int 1..128 | só UDP com chunk (um documento num chunk só conta: `1`) |
| `tls` | `{version, cipher, peer-subject, peer-fingerprint}` | só `tls`/`https`, se algum campo existir |
| `format` | `gelf` \| `malformed` | sempre |
| `gelf-version` | string | se o GELF declarou `version` string não vazia |
| `level` | `{code, name}`, 0–7, nomes `emerg` `alert` `crit` `err` `warning` `notice` `info` `debug` | sempre |
| `facility` | string | se presente (GELF 1.0) |
| `timestamp` | `AAAA-MM-DDThh:mm:ss.fffZ`, 3–9 casas, UTC | se válido |
| `short-message` | string | **sempre** (`""` com aviso) |
| `full-message` | string | se presente e não vazio |
| `file` | string | se presente e não vazio (GELF 1.0) |
| `line` | int ≥ 0 | se presente (GELF 1.0) |
| `fields` | objeto: nome → string \| número | campos `_nome`, sem o `_` (§6.6) |
| `extra` | objeto: chave → valor JSON não nulo | chaves de topo desconhecidas sem `_` (§6.7) |
| `size` | int | sempre: octetos do documento descomprimido |
| `raw` | string | `malformed`, perda (§6.9) ou `keep_raw=always` |
| `raw-encoding` | `base64` | quando `raw` não é UTF-8 |
| `parse` | `{warnings[], error}` | só se houve desvio |
| `x-proxy` | `{peer-ip, peer-port}` | só com PROXY protocol |

**Omissão:** nunca `null`, nunca `""`, exceto `short-message`. Objetos sem
conteúdo (`tls`, `fields`, `extra`, `parse`) são omitidos.

Exemplo (Docker `--log-driver=gelf` por UDP, gzip, de `172.17.0.1:41377`):

```json
{
  "host": { "hostname": "docker-desktop", "src-ip": "172.17.0.1", "src-port": 41377,
            "dst-ip": "172.17.0.2", "dst-port": 12201 },
  "transport": "udp", "compression": "gzip", "format": "gelf", "gelf-version": "1.1",
  "level": { "code": 6, "name": "info" }, "timestamp": "2026-10-31T12:00:00.123Z",
  "short-message": "hello",
  "fields": { "command": "echo hello", "container_id": "0f3c…", "container_name": "x",
              "created": "2026-10-31T11:59:59.9Z", "image_id": "sha256:…",
              "image_name": "alpine", "tag": "0f3c…" },
  "size": 384
}
```

### 3.3 Serialização

O serializador por *append* de S (§3.3 de S), sem `encoding/json` no caminho
quente:

- JSON compacto, UTF-8, sem espaços; os nove campos na ordem de §3.1; o
  payload na ordem de §3.2; `fields` e `extra` **na ordem de chegada** da
  última ocorrência de cada chave (§6.6).
- Strings são **decodificadas** do JSON de entrada (escapes resolvidos,
  surrogates combinados, surrogate solto → U+FFFD) e **reescritas** com o
  escape mínimo do `jlh`: `"`, `\`, `\n`, `\r`, `\t`, `\b`, `\f`, e todo byte
  abaixo de `0x20` como `\u00XX`. O trecho escapado de entrada **nunca** é
  copiado sem passar por isso (DESIGN §5).
- **Números são o texto que chegou**, byte a byte, validado pela gramática do
  JSON (RFC 8259 §6): `1e400`, `0.1`, `-0`, `12345678901234567890` passam como
  estão. Nunca `float64`.
- Valores compostos reescritos (§6.6, §6.7) são o JSON **compacto** do valor,
  com as strings internas decodificadas e reescritas pela mesma regra.
- `raw` não UTF-8 vai em base64 padrão com `=` (`raw-encoding: "base64"`).

### 3.4 Cabeçalho do item

```json
{"v":1,"uuid":"019a...","from":"0196...","space":"default","uts":1791028800,"sub":123456789,
 "transport":"udp","compression":"gzip","chunks":3,"src-ip":"10.0.0.3","src-port":40112,
 "dst-ip":"10.0.0.10","dst-port":12201}
```

| Chave | Conteúdo |
|---|---|
| `v` | `1` |
| `uuid`, `from`, `space`, `uts`, `sub` | como S |
| `transport` | `udp` \| `tcp` \| `tls` \| `http` \| `https` |
| `compression` | `gzip` \| `zlib` \| `deflate`; ausente = nenhuma. Só `udp` (gzip, zlib) e `http`/`https` (os três) |
| `chunks` | 1..128, só `udp`; ausente = sem chunk |
| `src-ip`, `src-port`, `dst-port` | obrigatórios (todo transporte é de rede) |
| `dst-ip` | omitido quando desconhecido |
| `peer-ip`, `peer-port` | o par real, só com PROXY (nunca em `udp`) |
| `tls` | só em `tls`/`https` |

Não há `framing` (só existe uma moldura por transporte) nem `size`: o GELF
nunca trunca, então `size = len(documento)`; guardá-lo seria duas fontes da
mesma verdade. O decodificador (`FuzzHead`) recusa `v` desconhecido, chave
obrigatória ausente, chave desconhecida, tipo errado, IP não canônico, porta
fora de 0–65535, `compression`/`chunks` num transporte que não os tem, `chunks`
fora de 1..128, `tls` fora de `tls`/`https`, `peer-*` em `udp`, e o `uuid` cujo
instante não é o da amostra.

### 3.5 Determinismo

> **Invariante:** o JLH é função pura de `(cabeçalho, documento, config de
> conversão)`.

A config de conversão é `[convert]` (§16.5): `keep_raw`, `max_message_bytes`
(atua também na recepção) e `accept_malformed`, congelada no boot. Réplicas com
ring Redis precisam da mesma `[convert]`.

### 3.6 Entrada do stream Redis (`sink=redis`)

`XADD <sink.redis.stream> [MAXLEN ~ N] * e <jlh>`, com `sink.redis.stream =
loghub.{space}.gelf` e `{space}` expandido por envelope (S §3.6).

### 3.7 Tabela de eventos (`sink=sqlite`, `sink=mysql`) e fila `loghub.inbox` (`sink=postgres`)

O que segue vale para `sink=sqlite` e `sink=mysql`. Destino **final**,
append-only, chave **`(uuid, step)`**.

| Coluna | Origem | Ausente |
|---|---|---|
| `uuid`, `step`, `space`, `level`, `sid` | envelope | — |
| `ev_uts`, `ev_sub` | `event.uts`, `event.sub` | — |
| `format`, `transport` | `payload.format`, `payload.transport` | — |
| `src_ip` | `payload.host.src-ip` | — |
| `hostname` | `payload.host.hostname` | `NULL` |
| `env` | o JLH, byte a byte | — |
| `nbytes` | `len(env)` | — |

`NULL` é "não veio". O NUL na coluna `hostname` vira U+FFFD (S §3.7). Não há
coluna de mensagem: a busca textual é sobre `env` (`CAST(env AS TEXT) LIKE` no
SQLite).

```sql
-- MySQL 8.0+ / MariaDB 10.6+ (docker/mysql/schema.sql)
CREATE TABLE IF NOT EXISTS gelf_events (
  uuid      BINARY(16)   NOT NULL,
  step      TINYINT      NOT NULL,
  space     VARCHAR(64)  NOT NULL,
  level     TINYINT      NOT NULL,
  sid       BIGINT       NOT NULL,
  ev_uts    BIGINT       NOT NULL,
  ev_sub    INT          NOT NULL,
  format    VARCHAR(16)  NOT NULL,
  transport VARCHAR(16)  NOT NULL,
  src_ip    VARCHAR(64),
  hostname  LONGTEXT,
  env       LONGBLOB     NOT NULL,
  nbytes    INT          NOT NULL,
  PRIMARY KEY (uuid, step),
  INDEX gev_uts   (ev_uts),
  INDEX gev_host  (hostname(255), ev_uts),
  INDEX gev_level (level, ev_uts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

```sql
-- SQLite (criado pelo próprio servidor no events.db, §10.4)
CREATE TABLE IF NOT EXISTS gelf_events (
  uuid TEXT NOT NULL, step INTEGER NOT NULL, space TEXT NOT NULL,
  level INTEGER NOT NULL, sid INTEGER NOT NULL,
  ev_uts INTEGER NOT NULL, ev_sub INTEGER NOT NULL,
  format TEXT NOT NULL, transport TEXT NOT NULL,
  src_ip TEXT, hostname TEXT,
  env BLOB NOT NULL, nbytes INTEGER NOT NULL,
  PRIMARY KEY (uuid, step)
) STRICT;
CREATE INDEX IF NOT EXISTS gev_uts   ON gelf_events(ev_uts);
CREATE INDEX IF NOT EXISTS gev_host  ON gelf_events(hostname, ev_uts);
CREATE INDEX IF NOT EXISTS gev_level ON gelf_events(level, ev_uts);
CREATE TABLE IF NOT EXISTS health (id INTEGER PRIMARY KEY CHECK (id = 1), ts INTEGER NOT NULL) STRICT;
```

O boot do MySQL exige as **treze** colunas com tipos compatíveis, o índice
único exato sobre `(uuid, step)`, InnoDB e permissão de escrita (S §10.6).

**Aviso para quem consulta o MySQL** (revisão de 2026-10-31): o `env` guarda o
JLH byte a byte, e um campo GELF pode trazer um número fora do `double`
(`1e400`, §3.3). `JSON_EXTRACT(CAST(env AS CHAR), …)` ou `CAST(env AS JSON)`
numa linha dessas **aborta a consulta inteira** com `3141` ("Number too big to
be stored in double"): basta uma linha de um emissor qualquer. A gravação está
certa e o dado não muda; para ler o `env` como JSON, use `LIKE`/`REGEXP` sobre
`CAST(env AS CHAR)`, ou os destinos SQLite (cujo `json_extract` aceita) e
ClickHouse. O aviso está também no cabeçalho de `docker/mysql/schema.sql`.

#### 3.7.1 Fila `loghub.inbox` (`sink=postgres`)

Idêntica a S §3.7.1 (a DDL de `docker/postgres/inbox.sql` é cópia literal da
skill, conferida por hash), com `type = 'gelf'`. A CHECK `inbox_syslog_ck` não se
aplica a `type=gelf`; `inbox_level_ck` (0–15) é cumprida (`level` ≤ 7). **NUL:**
`jsonb` recusa `\u0000` (`22P05`, o pedaço inteiro); o payload com um escape
`\u0000` vai como **cópia reconstruída** (o JLH do pedaço e o dos outros
destinos ficam íntegros):

- `raw` com NUL, ainda não em base64, vai em base64 com `raw-encoding:
  "base64"`: fica exato, e `size` continua contando os octetos dele;
- todo outro texto troca U+0000 por U+FFFD, **inclusive as chaves e os valores
  de `fields` e `extra` em qualquer profundidade** (`extra` guarda qualquer
  valor JSON do emissor), `short-message`, `full-message`, `host.hostname`,
  `facility` e `file`;
- duas chaves do mesmo objeto que viram a mesma depois da troca (`"a\u0000"` e
  `"a\ufffd"` em `fields`): **vence a que já não tinha NUL**; entre duas que
  tinham, **vence a menor chave original**. Nunca a ordem de iteração do mapa:
  a cópia precisa sair igual em todo retry e em toda réplica;
- a reconstrução passa por um mapa (o `jsonb` não guarda ordem de chave, então
  nada que a coluna guardaria se perde) e preserva números como texto
  (`json.Number`): `1e400` e `12345678901234567890` nunca passam por `float64`.

**Número que o `jsonb` não guarda** (portado do `go-gelf-pg-server`,
2026-10-31). Os números de `fields` e `extra` são o texto que chegou (§3.3), e
o tipo `numeric` em que o `jsonb` os guarda pára em 131 072 dígitos antes da
vírgula e 16 383 depois: `1e1000000` ou `1e-16384` num campo GELF era `22003`,
classe `22`, e descartava como `logic` o pedaço inteiro — os vizinhos do
documento hostil junto. A cópia reconstruída (a mesma da regra do NUL) leva esse
número como a **string do seu texto**: nada dele se perde, e o JLH do pedaço e o
do dump continuam com o número como chegou. A conta é sobre o texto, sem
`float64` (`jsonbHolds`): dígitos do inteiro sem zeros à esquerda mais o
expoente ≤ 131 072, e dígitos da fração menos o expoente ≤ 16 383; uma
**mantissa toda zero** é 0 para o PostgreSQL, então só a segunda conta vale
(`0e999999` e `0.0e131072` cabem e ficam número; `0e-20000` não cabe). Cada
borda é conferida contra o PostgreSQL 18 real
(`TestJSONBHoldsAgreesWithPostgres`), e o pedaço com o número hostil é gravado
inteiro (`TestPostgresNumbersJSONBCannotHoldDoNotDropTheChunk`). Sem NUL e sem
número fora da faixa — quase sempre — a coluna recebe o sub-slice do JLH, sem
cópia. O MySQL (`env` é `LONGBLOB`), o ClickHouse (`chPayload` cita o texto) e o
SQLite gravam o mesmo número sem cópia (conferido na revisão de 2026-10-31).

**Exceção do oráculo, só nesta cópia.** O `raw` de um documento UTF-8 **válido**
que contém um NUL de verdade (o byte `00` dentro de uma string JSON é controle
cru, então o documento é `malformed` e o `raw` vai como string com `\u0000`)
sai na cópia da fila em base64. A regra do oráculo "`raw` UTF-8 nunca em
base64" (§18.2) vale para o JLH, não para esta cópia: os testes da fila
reconvertem o `raw` antes de passar a linha pelo oráculo, e um consumidor da
`loghub.inbox` decodifica o `raw` pelo `raw-encoding`, sempre (§22.1).

#### 3.7.2 Tabela `inbox_gelf` (`sink=clickhouse`)

Espelha a `loghub.inbox` como S §3.7.2, com o *hint* do `type=gelf`:

```sql
-- ClickHouse 25.8+ (docker/clickhouse/inbox_gelf.sql; o servidor nunca a cria)
CREATE TABLE IF NOT EXISTS inbox_gelf (
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
  payload     JSON(fields Map(String, String), extra Map(String, String))
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_at)
ORDER BY (event_at, uuid)
SETTINGS non_replicated_deduplication_window = 1000;
```

- **`fields` e `extra` como `Map(String, String)`.** São as únicas chaves de
  objeto que o emissor controla: como caminhos do tipo `JSON`, `_a.b` e um
  `_a` objeto colidiriam (`117`, o pedaço inteiro), e o número de caminhos
  dinâmicos seria do emissor. Como `Map`, as chaves ficam literais.
- **Os valores viram texto, e quem os cita é o sink** (`chPayload`, §10.10.3):
  todo valor de `fields`/`extra` que não é string — número, objeto, array,
  `true`, `false` — vai como **string JSON com o seu texto**, byte a byte como
  o JLH o tem: `1.50` chega `"1.50"`, `1e400` chega `"1e400"`, um objeto de
  `extra` chega como o seu JSON compacto. Os ajustes
  `input_format_json_read_numbers_as_strings`,
  `input_format_json_read_objects_as_strings` e
  `input_format_json_read_arrays_as_strings` existiam para fazer isso no
  servidor e **não fazem** no 25.8 (medido em 2026-10-31): o número é lido
  como `Float64` e reescrito (`1.50` vira `"1.5"`, `-0` vira `"0"`), e um
  número fora da faixa do `double` (`1e400`) em qualquer ponto do documento
  recusa o `INSERT` com `117` — um descarte `logic` do pedaço inteiro por um
  campo de um emissor. Os ajustes continuam ligados em todo `INSERT`, como
  defesa. O tipo do número fica no `env` dos outros destinos; este não tem
  `env`: quem precisa do tipo lê o JLH de outro destino, ou converte
  (`toFloat64OrNull`). A cópia só existe quando o payload tem `fields` ou
  `extra`; sem eles, o `payload` é o sub-slice do JLH, sem cópia.
- O tipo `JSON` descarta objeto vazio: uma linha sem `fields` volta com
  `"fields":{}`, que este servidor nunca escreve (omissão). `{}` é ausente.
- A inferência de datas e números a partir de strings é desligada em todo
  `INSERT` (S §10.10.3), e importa ainda mais aqui: `fields.created` do Docker
  é uma data em string.
- O boot recusa a tabela sem os dois *hints*, além do que S §10.10.5 já recusa.

### 3.8 Mensagem NATS (`sink=nats`)

```
Subject:  loghub.<space>.gelf
Headers:  Nats-Msg-Id:     <uuid>.<step>
          Content-Type:    application/json
          X-LogHub-Type:   gelf
          X-LogHub-UUID:   <uuid>
          X-LogHub-Space:  <space>
Data:     <jlh>
```

Só o subject exato (decisão 16). O stream sugerido é `LOGHUB_GELF` com subject
`loghub.*.gelf` (`docker/nats/stream.json`); o servidor nunca o cria. O JLH vai
em `Data`, nunca num header.

### 3.9 O maior evento, e por que ele é calculado e não configurado

O boot compara o teto do destino (`max_payload` do NATS, `max_allowed_packet`
do MySQL, `max_memory_usage` do ClickHouse) com o pior JLH possível e não abre
os transportes quando ele não cabe (exit 3), como S.

Com `L = convert.max_message_bytes` (o documento descomprimido nunca passa
disso, §5.5):

```
pior JLH = 6 × L + 8 KiB
```

A conta, caso a caso:

- **Documento malformado** (§6.1): `short-message` é `""` e o único texto é
  `raw`. UTF-8 válido com todo byte de controle (fora de string JSON, um byte
  de controle torna o documento malformado) vira `\u00XX`: **6 × L**. Não UTF-8
  vai em base64: 4/3 × L.
- **Documento válido**: toda string do payload é uma fatia **disjunta** do
  documento (uma chave duplicada some, não se repete), reescrita com escape
  mínimo, o que nunca a alonga: um escape de entrada (`\u0001`, 6 bytes) sai
  com o mesmo tamanho, um caractere literal sai igual, e um surrogate solto
  (6 bytes) sai como U+FFFD (3). Números saem como chegaram. O único
  crescimento é a **coerção** (§6.6): um valor composto vira string do seu JSON
  compacto, e cada `"` e `\` dele ganha um `\` — no máximo **2 ×** os bytes do
  valor (mais as duas aspas, pagas pela vírgula ou pelos dois pontos que o
  valor tinha). Texto convertido ≤ 2 × L. Com `raw` (perda ou
  `keep_raw=always`): o documento como string JSON, em que só `"`, `\` e os
  espaços `\t` `\n` `\r` crescem, cada um para 2 bytes: ≤ 2 × L. Total ≤ **4 × L**.
- **8 KiB fixos**: as chaves do payload, `host` (dois IPs, duas portas),
  `tls` (`peer-subject` ≤ 1 KiB), `x-proxy`, `parse` (25 avisos ≈ 500 B e o maior
  `parse.error`), `level`, `timestamp` e o cabeçalho do envelope (≈ 250 B).

O pior caso é o malformado: **6 × L + 8 KiB**.

| `L` | pior JLH | cabe em 1 MiB (NATS, com os ≤ 256 B de headers de §3.8)? |
|---|---|---|
| 256 KiB (o pedido da família, DESIGN decisão 10) | 1 544 KiB | **não** |
| 160 KiB | 968 KiB | sim, com 56 KiB de folga |
| **128 KiB (padrão)** | **776 KiB** | sim, com 248 KiB de folga |

O padrão é **128 KiB** (D6): o maior valor redondo que cabe com folga, e o
mesmo pior JLH de S (776 KiB), já provado contra MySQL e ClickHouse reais. Uma
linha MySQL é `pior JLH + L (hostname saneado) + 512 B`. Quem sobe
`max_message_bytes` acima de ~170 KiB precisa subir o `max_payload` do NATS
junto, e o boot diz isso. `TestWorstCaseJLHIsBounded` prova o teto com
documentos adversários (malformado de controles, coerção aninhada, `raw`
forçado).

O fio pode trazer mais que `L`: o teto do **corpo comprimido** (HTTP) e da soma
dos chunks de uma mensagem (UDP) é `L + 16 KiB` (§5.5), e o documento
descomprimido continua limitado a `L` dentro do leitor.

---

## 4. Transportes

Cinco transportes independentes, cada um ligado por um parâmetro de `[listen]`
que, vazio, o desliga. **Todos desligados é erro de configuração** (exit 1).

| Transporte | Parâmetro | Padrão | Moldura |
|---|---|---|---|
| UDP | `listen.udp` | `:12201` | datagrama ou chunks GELF (§5.2) |
| TCP | `listen.tcp` | `:12201` | `\0` (§5.3) |
| TLS | `listen.tls` | `""` (desligado; sugestão `:12203`) | `\0` sobre TLS |
| HTTP | `listen.http` | `:12202` | um documento por `POST /gelf` |
| HTTPS | `listen.https` | `""` (desligado; sugestão `:12204`) | idem, sobre TLS |

### 4.1 Portas

12201 é a porta GELF do Graylog e do Docker para UDP e TCP (decisão 5); o TCP e
o HTTP não dividem porta, então o HTTP fica em 12202. A imagem roda como uid
65532: todas são altas. O log de boot imprime o endereço efetivo de cada
listener. Dois transportes no mesmo endereço TCP (`tcp`, `tls`, `http`,
`https`, `admin.bind`) são exit 1; UDP e TCP na mesma porta são permitidos.

### 4.2 UDP

- Leitura com `ReadMsgUDPAddrPort` num buffer de 65 536 bytes; `dst-ip` por
  PKTINFO; IPs canônicos; `SO_RCVBUF` lido de volta; `SO_REUSEPORT` com
  `listen.udp_listeners > 1`; origem não endereçável é `malformed_frame` — tudo
  como S §4.2.
- Um datagrama que começa com `1e 0f` é um **chunk** (§5.2); outro é o
  documento inteiro, possivelmente comprimido (§5.4).
- **Sem backpressure.** Toda recusa é descarte contado com a razão.

### 4.3 (reservado)

O unix datagram de S não existe aqui.

### 4.4 TCP

Uma conexão carrega N documentos separados por `\0` (§5.3). É o transporte com
backpressure real. `tcp.max_conns`, `tcp.max_conns_per_ip`, `tcp.idle_timeout`
(120 s), `SO_KEEPALIVE` (30 s), endereços canônicos — tudo como S §4.4.

### 4.5 HTTP e HTTPS

Decisão 13. Um `net/http.Server` da stdlib por listener, endurecido como os
webhooks (CHANGELOG 2026-10-14):

| Requisição | Resposta |
|---|---|
| `POST /gelf` aceito pelo ring (ou filtrado, ou consumido de vez pelo sink com `ring=none`); com `http.bulk`, todas as linhas (§4.5.1) | **`202 Accepted`**, corpo vazio |
| caminho diferente de `/gelf` (inclusive `/gelf/`) | `404` |
| `/gelf` com método diferente de `POST` | `405` com `Allow: POST` |
| `Content-Encoding` fora de `gzip`, `x-gzip`, `deflate`, `identity` | `415` (contado como `reason="decompress"`) |
| `Content-Length` acima de `L + 16 KiB`, corpo lido acima disso, ou documento descomprimido acima de `L` (com `http.bulk`: `B + 16 KiB`, `B`, ou uma linha acima de `L`, §4.5.1) | `413` |
| corpo vazio ou só espaço | `400` |
| descompressão inválida | `400` |
| ring cheio, ring indisponível, sink DOWN, rate-limit, shutdown | **`503`** com `Retry-After: 1` |

- **`4xx` é final para o cliente**: só o que nunca vai passar o recebe. Tudo o
  que pode passar numa nova tentativa é `503`.
- **Keep-alive** (HTTP/1.1 persistente, com o *pipelining* sequencial da
  stdlib: as requisições de uma conexão são respondidas na ordem, uma de cada
  vez). Entre duas requisições a conexão espera até `http.idle_timeout` (60
  s); `http.max_requests_per_conn` (0 = sem teto) põe `Connection: close` na
  resposta da N-ésima. A conexão por documento custava um *handshake* e um
  `TIME_WAIT` por evento (e2e de 2026-10-31: 20 000 requisições, 20 000
  conexões). O `503` de backpressure (ring cheio, sink DOWN, rate-limit)
  **não** fecha a conexão — o cliente espera um segundo, não paga outro
  *handshake* —; só o shutdown fecha: a resposta do `503` de `shutdown` leva
  `Connection: close`, e desde o início do shutdown **toda** resposta leva
  (`SetKeepAlivesEnabled(false)`), de modo que uma conexão ociosa é fechada e
  uma em uso termina depois da requisição em voo, nunca no meio dela.
  **Sem HTTP/2**, nem h2c nem h2 no HTTPS (`TLSNextProto` vazio, ALPN só
  `http/1.1`).
- **Corpo:** lido de um `io.LimitedReader(L + 16 KiB + 1)`, num buffer que
  cresce com o que **chega**, por classes (2, 8, 64 KiB e dobrando) e **nunca
  acima do teto + 1**, nunca pelo `Content-Length` declarado (só a recusa
  antecipada `413` o usa). O documento descomprimido cresce da mesma forma até
  `L + 1` (§5.4). `Content-Encoding: gzip` → gzip; `deflate` → zlib se os dois
  primeiros bytes forem um cabeçalho zlib válido, senão deflate cru
  (`compression: "deflate"`); ausente ou `identity` → detecção pelos bytes
  como no UDP (§5.4). A descompressão tem o teto `L` dentro do leitor.
- **Uma recusa lê o resto do corpo?** O handler não: o que sobrou é do
  `net/http`, que descarta até 256 KiB para reaproveitar a conexão e, acima
  disso (ou com um `Expect: 100-continue` não atendido), fecha a conexão
  depois da resposta.
- **Prazos:** `http.read_header_timeout` (10 s), `http.read_timeout` (30 s, o
  corpo inteiro), `http.idle_timeout` (60 s, entre requisições), escrita de 10
  s **contados da resposta**, cabeçalhos até 16 KiB. O `net/http` arma o prazo
  de escrita no fim dos cabeçalhos; o handler o desarma ao começar e o rearma
  imediatamente antes de cada resposta. Sem isso, um corpo lento (legítimo,
  dentro do `read_timeout`) somado à espera do ring (`ring=none`, o `fsync`
  do `sqlite`) passava de 10 s: o documento entrava no ring e o `202` se
  perdia, e o cliente reenviava o mesmo evento com outro `uuid` (a correção
  do `go-webhook-server` de 2026-10-14; revisão de 2026-10-31). No HTTPS o
  handshake usa o menor dos prazos de leitura (comportamento da stdlib) e o
  `tls.handshake_timeout` não se aplica.
- **Limites de conexão:** `http.max_conns` e `http.max_conns_per_ip`, por
  transporte (um limitador para `http`, outro para `https`), como §4.4.
- **Origem:** o socket, ou o PROXY protocol de um par confiável (§4.6), lido
  antes do TLS e antes do HTTP. **`X-Forwarded-For` e `X-Real-IP` são
  ignorados**: um cabeçalho HTTP é alegação do cliente, e o rate-limit por
  prefixo dependeria dele.
- **TLS (HTTPS):** os certificados e o `client_auth` de `[tls]`; `payload.tls`
  como §4.7.
- O tempo entre o corpo completo e a garantia do ring vai para
  `gelf_confirm_duration_seconds`, como no TCP.

#### 4.5.1 `http.bulk`: um documento por linha

Padrão `false`, como o `enable_bulk_receiving` do input GELF HTTP do Graylog:
o corpo é **um** documento. Com `true` (decidido sem entrevista em 2026-10-31,
contestável; DECISOES §2) — o que o output `http` do fluent-bit com `Format
gelf` manda, todos os registros de um *flush* num corpo:

- O corpo é descomprimido inteiro (como sempre, pelo `Content-Encoding` ou
  pelos bytes) com o teto `B = http.bulk_max_bytes` (4 MiB, até 64 MiB;
  abaixo de `L`, vale `L`) no lugar de `L`; no fio o teto é `B + 16 KiB`. Fora disso, `413`.
- O corpo descomprimido é dividido em `\n`; um `\r` antes do `\n` é cortado
  junto (CRLF dá os mesmos documentos que LF). Uma linha vazia ou só de espaço
  JSON é ignorada sem contar.
- **Tamanho é tudo ou nada:** antes de enfileirar qualquer coisa, toda linha é
  medida; uma acima de `L` recusa o corpo inteiro com `413`, nada enfileirado,
  contado como **uma** chegada e **um** `too_large`. Corpo sem nenhuma linha
  não vazia: `400`, uma chegada e um `empty`.
- Cada linha é um documento com o **próprio `uuid`**, todos da **mesma
  amostra de relógio** (a do corpo completo, §3.1); o gerador de UUIDv7
  ordena os empates. `compression` é a do corpo. Filtro e rate-limit valem
  por linha; filtrado não interrompe.
- O `202` só sai depois de **todas** as linhas no ring. A primeira linha
  recusada por um motivo que pode passar depois (ring cheio, sink DOWN,
  rate-limit, shutdown) interrompe o corpo: **as linhas anteriores ficam no
  ring** e a resposta é `503` com `Retry-After`. O cliente que reenvia o corpo
  **duplica** essas linhas (com outros `uuid`s: o destino não as deduplica).
  Retirar do ring o que ele já garantiu não existe na interface do ring
  (§9.1); a alternativa seria reservar espaço para o corpo inteiro antes, o
  que nenhum ring sabe fazer.
- As linhas que não chegaram a ser tentadas não são contadas.
- Recarregável? **Não**: `http.bulk` e `http.bulk_max_bytes` exigem restart,
  como toda chave de recepção (§16.3).

### 4.6 PROXY protocol

Como S §4.6, nos quatro transportes de conexão (`tcp`, `tls`, `http`,
`https`), com as mesmas regras de par confiável, `optional`/`required`, prazo
próprio, v1/v2, `LOCAL`, shutdown durante a espera e `x-proxy`. No HTTP o
cabeçalho é lido pela conexão no primeiro `Read` (dentro da goroutine da
conexão, nunca no laço de `Accept`), e o handler lê dela os endereços
resolvidos. Um cabeçalho inválido ou ausente em `required` fecha a conexão sem
resposta HTTP.

### 4.7 TLS (TCP+TLS e HTTPS)

Como S §4.5: `tls.cert_file`/`tls.key_file` obrigatórios quando `listen.tls` ou
`listen.https` estão ligados, `tls.min_version`, `tls.client_auth`
(`optional`/`require`/`none`) com `tls.client_ca`, `payload.tls` com `version`,
`cipher`, `peer-subject` (≤ 1 KiB) e `peer-fingerprint`. `[tls]` tem
`idle_timeout`, `max_conns`, `max_conns_per_ip`, `backpressure_timeout` e
`handshake_timeout` do transporte `tls`; o `https` usa os de `[http]`.

---

## 5. Moldura, remontagem e descompressão

### 5.1 O documento

A unidade é um **documento GELF**: um objeto JSON em UTF-8. O transporte
entrega bytes; esta seção diz como eles viram exatamente um documento, ou um
descarte contado.

### 5.2 Chunks UDP (decisão 11)

```
 0      2                10    11      12
 +------+-----------------+-----+-------+------------------------
 | 1e 0f| message id (8)  | seq | count | dados (até o fim do datagrama)
 +------+-----------------+-----+-------+------------------------
```

- `count` 1..128 e `seq < count`; fora disso, ou datagrama de menos de 12
  bytes com o magic, é descarte `reason="chunk_invalid"`.
- A mensagem é identificada por `(src-ip, src-port, id)` (a origem é o par UDP:
  o id sozinho é escolhido pelo emissor e colidiria entre emissores).
- **Prazo de 5 s** contado do primeiro chunk (spec GELF). Uma mensagem que não
  completa no prazo é descartada inteira: `reason="chunk_timeout"`, uma vez por
  mensagem. O prazo é **varrido por um relógio próprio** (a cada 1 s), não pela
  chegada de chunks: num servidor ocioso uma mensagem órfã ficaria para sempre.
  Um chunk que chega para uma mensagem vencida e ainda não varrida a descarta e
  começa uma nova.
- **Chunk repetido** (mesmo `seq`) é ignorado sem descarte
  (`gelf_udp_chunk_duplicates_total`). Um chunk com `count` diferente do
  primeiro da mensagem é `chunk_invalid` e descarta a mensagem inteira.
- **Nunca alocar pelo `count` declarado.** Cada chunk é copiado num buffer do
  tamanho dele; a mensagem guarda os chunks que chegaram, num slice que cresce
  com eles, e um mapa de bits de 128 posições.
- **Teto global** `listen.udp_reassembly_max_bytes` (64 MiB, contando os dados
  de cada chunk mais 64 bytes de estrutura) e
  `listen.udp_reassembly_max_messages` (16 384 mensagens abertas). Um chunk que
  estouraria qualquer dos dois é descartado e a mensagem dele também:
  `reason="chunk_overflow"`, **uma vez por mensagem** (as que já estavam
  abertas seguem). A mensagem recusada vira **lápide** (0 byte, uma vaga de
  mensagem) até o prazo, e os chunks restantes dela são engolidos sem nova
  contagem — tanto a mensagem nova recusada pelo teto de bytes quanto a
  aberta que o estoura no meio (antes da revisão de 2026-10-31, cada chunk de
  uma mensagem nova contava um descarte, e a aberta que estourava era contada
  de novo como `chunk_timeout`). **Com `max_messages` cheio não há onde
  lembrar** a mensagem, nem como lápide: aí cada chunk recusado conta como
  uma chegada e um `chunk_overflow` (até `count` por mensagem). Expulsar uma
  mensagem aberta para dar lugar à lápide deixaria uma enxurrada de primeiros
  chunks destruir toda a remontagem em curso. O gauge
  `gelf_udp_reassembly_bytes` mostra o uso.
- **Teto por mensagem:** a soma dos dados de uma mensagem acima de `L + 16 KiB`
  é `too_large`: a mensagem é descartada, e a entrada fica como **lápide** até
  o prazo, para que os chunks restantes sejam engolidos sem novo descarte.
- Completa a mensagem, os dados são concatenados na ordem de `seq` e seguem
  para a descompressão (§5.4) com a amostra de relógio do **último** chunk e
  `chunks = count`. Um documento que chega num chunk só (`count = 1`) também
  tem `chunks: 1`.
- O estado é um mapa sob um mutex compartilhado pelos `udp_listeners`.

### 5.3 Moldura `\0` (TCP e TLS)

- Um documento termina no primeiro `\0`. O `\0` não faz parte do documento.
- Um quadro **vazio ou só de espaço JSON** (` `, `\t`, `\n`, `\r`) é separador:
  ignorado **sem** contar (decisão 12). Emissores que mandam `\n\0` ou `\0\0`
  não geram descarte.
- **EOF sem `\0`:** o que chegou é um documento, como o Graylog faz (o Docker e
  alguns clientes não terminam o último). Um erro de leitura (reset, prazo)
  com bytes pendentes entrega o mesmo quadro — o que foi lido não se perde; se
  estiver incompleto, a conversão o marca `malformed`.
- **Acima de `L`:** o quadro é descartado até o próximo `\0` **sem alocar além
  de `L`** (até `L` bytes ficam no buffer antes de se saber; o resto é
  consumido e esquecido), contado como `too_large`; a conexão
  continua.
- **Sem compressão no TCP** (spec GELF): um quadro comprimido é um documento
  `malformed` (não é JSON), com `raw` em base64.
- O buffer de quadro cresce com o que chega em classes de 2, 8 e 64 KiB (e
  dobrando acima, até `L`), e um buffer acima de 16 KiB é solto antes de
  esperar o próximo quadro com o leitor vazio — as regras de custo fixo por
  conexão de S §5.4 valem como estão.

### 5.4 Compressão

Detecção pelos bytes, no UDP (o documento inteiro ou remontado) e no HTTP sem
`Content-Encoding`:

| Primeiros bytes | Contêiner |
|---|---|
| `1f 8b` | gzip |
| `78 xx` com `(0x78 << 8 \| xx) % 31 == 0` | zlib |
| outro | JSON cru |

- **O teto vale dentro do leitor** (bomba de descompressão): o leitor do
  `gzip`/`zlib`/`flate` é lido por um `io.LimitReader(L + 1)`; o byte a mais
  denuncia `too_large`. Nunca descomprimir para depois medir.
- O gzip com vários membros é lido inteiro (o `gzip.Reader` da stdlib), então
  bytes depois de um membro são lidos como o cabeçalho do próximo, e lixo ali é
  `decompress`. No zlib e no deflate cru, bytes depois do fim do fluxo são
  ignorados (o leitor para no fim do fluxo).
- **Descompressão inválida** (cabeçalho, checksum, fluxo truncado) é descarte
  `reason="decompress"` (HTTP `400`). Não vira `malformed`: o cru do ring é o
  documento descomprimido, e bytes comprimidos corrompidos não têm conteúdo a
  mostrar.
- `compression` no cabeçalho e no payload diz o contêiner real.

### 5.5 Tamanho (decisão 10)

`convert.max_message_bytes` (env `GELF_CONVERT_MAX_MESSAGE_BYTES`, padrão
**128 KiB**, mínimo 1 KiB) é o teto do documento **descomprimido**:

- Acima dele **não há truncamento**: cortar um JSON o torna inválido. O
  documento é **recusado e contado** (`reason="too_large"`); o HTTP responde
  `413`; o TCP descarta até o próximo `\0`.
- O teto do que vem **no fio** antes de descomprimir é `L + 16 KiB`: o corpo
  HTTP e a soma dos chunks UDP. Um documento JSON cru no fio também é
  limitado por `L`.
- O UDP lê o datagrama inteiro (até 65 507 bytes).
- `size` = `len(documento)`, sempre; `gelf_message_bytes` é o histograma.

### 5.6 Documento vazio

Um documento de zero bytes, ou só de espaço JSON, depois de descomprimido, não
gera envelope. No UDP e no HTTP conta como chegada e como
`reason="empty"` (HTTP `400`); no TCP/TLS é separador (§5.3).

### 5.7 Backpressure: parar de ler (TCP, TLS)

Como S §5.7: ring cheio, ring indisponível ou sink DOWN fazem o leitor parar de
chamar `Read`, conferindo a cada 50 ms, por até `tcp.backpressure_timeout` (30
s); estourado, fecha a conexão contando o documento em mãos. Filtro e
rate-limit decidem **uma vez** por documento; `Accept`, `Retry` e `RecordDrop`
são caminhos separados; uma recusa `logic` final do sink com `ring=none` é
`Consumed`, sem retentativa.

O HTTP não segura: uma tentativa, e `503` com `Retry-After: 1` (§4.5). O UDP
descarta.

---

## 6. Conversão

### 6.1 Regra absoluta: a conversão nunca falha

Qualquer documento **de pelo menos um byte** vira um envelope válido (um
documento vazio não tem envelope possível — `size` ≥ 1 e `raw` obrigatório no
`malformed` —, os receptores nunca o enfileiram, §5.6, e o montador de pedaços
recusa e conta como `malformed` um item vazio vindo de um ring ou dump
danificado, antes de chamar o conversor). O pior resultado é
`format: "malformed"`, com `short-message: ""`, `level` 6, `raw` obrigatório
(base64 com `raw-encoding` quando não é UTF-8), `size` e `parse.error`:

| Causa | `parse.error` |
|---|---|
| UTF-8 inválido em qualquer ponto | `UTF-8 inválido no documento` |
| sintaxe JSON inválida (inclusive BOM, número fora da gramática, escape inválido, controle cru em string, documento truncado) | `documento não é JSON válido` |
| conteúdo depois do fim do valor de topo | `conteúdo depois do fim do documento JSON` |
| o valor de topo não é objeto | `o documento JSON não é um objeto` |
| aninhamento acima de 64 níveis | `aninhamento JSON acima de 64 níveis` |

Os cinco textos são dado normativo (em português num código em inglês, como os
de S §6.6), e o schema os fecha num `enum`. Um documento `malformed` não tem
nenhum outro campo de conteúdo: nada do que veio é interpretado.

`convert.accept_malformed = false` descarta o envelope `malformed` no dreno e
conta em `gelf_events_dropped_total{reason="malformed"}` (S §6.6).

### 6.2 Validação (o parser)

Próprio, por varredura (decisão de implementação, DECISOES §2), em
`internal/convert`:

- Uma passada **valida o documento inteiro** — gramática RFC 8259, UTF-8 de
  todo byte, escapes de string (`\" \\ \/ \b \f \n \r \t \uXXXX`), controle
  cru em string proibido, profundidade ≤ 64 (o valor de topo é o nível 1, seja
  objeto ou não: 64 arrays aninhados são "não é um objeto", 65 são
  "aninhamento") — e registra os **membros de topo**
  como fatias do documento (chave crua, valor cru, tipo do valor). Nada é
  decodificado nessa passada.
- A mesma passada serve o filtro na recepção (§7) e a conversão no dreno: o
  `level` filtrado é o `level` emitido por construção.
- Espaço permitido é o do JSON: ` `, `\t`, `\n`, `\r`. Um BOM é sintaxe
  inválida.

### 6.3 Strings

Decodificadas na emissão: escapes resolvidos; um par de surrogates `😀`
vira o caractere; um surrogate **solto** (alto sem baixo, baixo sem alto) vira
U+FFFD com o aviso `encoding-replaced`, que força `raw`. `\u0000` é um
caractere legítimo (sai como `\u0000`; o PostgreSQL o reconstrói, §3.7.1). As
chaves passam pela mesma decodificação **antes** de serem comparadas: `"level"`
é `level`.

### 6.4 Chaves duplicadas

No topo e em `fields`, **a última ocorrência vence** (o Jackson do Graylog faz
o mesmo), com o aviso `duplicate-key`, que força `raw` (o valor anterior se
perdeu). A posição em `fields`/`extra` é a da última ocorrência. Um valor de
`extra` (objeto ou array) com uma chave repetida **dentro** dele não pode ir
como JSON: o parser de cada consumidor ficaria com uma das duas em silêncio (o
`jsonb`, o ClickHouse, o oráculo). Ele vai como **string** do seu JSON compacto,
que preserva as duas, com `duplicate-key`. O mesmo vale, por construção, para um
valor coagido de `fields` (que já é string).

### 6.5 Campos padronizados

| Chave GELF | Valor | Payload | Aviso |
|---|---|---|---|
| `version` | string não vazia `1.1` ou `1.0` | `gelf-version` | — |
| | outra string não vazia | `gelf-version` (como veio) | `version-unknown` |
| | ausente | — | `version-missing` |
| | `""`, outro tipo, `null` | — | `version-invalid` |
| `host` | string `^\S+$` (D5) | `host.hostname` | — |
| | ausente | — | `host-missing` (D2) |
| | `""`, com espaço, outro tipo, `null` | — | `host-invalid` |
| `short_message` | string | `short-message` (pode ser `""`, com aviso) | `""` → `short-message-missing` |
| | número, `true`/`false`, objeto, array | `short-message` = o texto do número ou o JSON compacto | `short-message-coerced` |
| | ausente ou `null` | `short-message: ""` | `short-message-missing` (D2) |
| `full_message` | string não vazia | `full-message` | — |
| | `""` | — | — |
| | outro tipo, `null` | — | `full-message-invalid` |
| `level` | número inteiro literal (`^-?[0-9]+$`) 0..7 | `level` | — |
| | número 0..7 em outra grafia (`3.0`, `3e0`, `30e-1`) | `level` | `level-coerced` |
| | string de um dígito `"0"`..`"7"` | `level` | `level-coerced` |
| | ausente | `level` 6 | `level-default` (D1) |
| | qualquer outra coisa (`8`, `-1`, `3.5`, `"info"`, `null`, objeto) | `level` 6 | `level-invalid` |
| `timestamp` | número ≥ 0 | `timestamp` (abaixo) | — |
| | string na gramática de número JSON | `timestamp` | `timestamp-coerced` |
| | número negativo (exceto `-0`, que é a época), depois de 9999-12-31T23:59:59Z, outro tipo, `null`, outra string | — | `timestamp-invalid` |
| `facility` | string não vazia | `facility` | — |
| | número | `facility` = o texto do número | `facility-coerced` |
| | `""`, outro tipo, `null` | — | `facility-invalid` |
| `file` | string não vazia | `file` | — |
| | `""`, outro tipo, `null` | — | `file-invalid` |
| `line` | número inteiro literal 0..2 147 483 647 | `line` | — |
| | número inteiro nessa faixa em outra grafia, ou string só de dígitos nessa faixa, zeros à esquerda aceitos (`"0042"` é 42) | `line` | `line-coerced` |
| | outra coisa | — | `line-invalid` |

O `level` sai como `{code, name}` com os nomes de S (`emerg` … `debug`).

**`timestamp`**: segundos desde a época, com fração. A conversão é **aritmética
decimal sobre o texto** (sem `float64`): mantissa e expoente são combinados num
inteiro de segundos e numa fração de até 9 dígitos; dígitos além do nono são
**truncados** (não arredondados), sem aviso. A saída tem `max(3, n)` casas,
onde `n` é o número de dígitos da fração **como veio** (até 9), zeros à direita
incluídos: `1700000000.5` sai `.500Z`, `1700000000.123456` sai `.123456Z`,
`1700000000.1234567891` sai `.123456789Z`, `1700000000` sai `.000Z`,
`1.7e9` sai `2023-11-14T22:13:20.000Z`. Um expoente que desloca a vírgula conta
os dígitos que sobram à direita dela. Um expoente com mais de 4 dígitos, ou um
número cuja parte inteira tenha mais de 12 dígitos, é `timestamp-invalid` sem
fazer a conta (o ano já passaria de 9999).

### 6.6 Campos adicionais (`fields`)

Toda chave de topo que começa com `_` vira `fields[nome]`, com `nome` = a chave
sem o primeiro `_`:

| Valor | `fields[nome]` | Aviso |
|---|---|---|
| string | a string decodificada (inclusive `""`) | — |
| número | o **texto literal** do número | — |
| `true`, `false`, objeto, array | string com o JSON compacto | `field-coerced` |
| `null` | omitido | `field-null` |

- `_id` entra como `fields.id` com `field-id-reserved` (a spec GELF o proíbe; o
  Graylog o descarta; aqui ele é preservado e marcado).
- Um `nome` fora de `^[\w\.\-]+$` (ASCII: letra, dígito, `_`, `.`, `-`; vazio
  incluído) entra como veio com `field-name-invalid`.
- Ordem de chegada, com a regra de duplicatas de §6.4 (sobre o nome sem `_`).

### 6.7 Chaves desconhecidas (`extra`)

Uma chave de topo que não é padronizada e não começa com `_` (`message`,
`@timestamp`, `log`, `HOST`…) vai para `extra[chave]` com o valor JSON
**reescrito compacto** (§3.3: strings decodificadas e reescritas, números
literais, objetos e arrays compactos), e o aviso `key-unknown`. `null` é
omitido com `field-null`. Duplicatas como §6.4.

### 6.8 Avisos de `parse.warnings`

Lista **fechada** (o `enum` do schema), escrita na ordem desta tabela, sem
repetição:

| # | Código | Força `raw` |
|---|---|---|
| 1 | `version-missing` | |
| 2 | `version-unknown` | |
| 3 | `version-invalid` | sim |
| 4 | `host-missing` | |
| 5 | `host-invalid` | sim |
| 6 | `short-message-missing` | |
| 7 | `short-message-coerced` | |
| 8 | `full-message-invalid` | sim |
| 9 | `level-default` | |
| 10 | `level-coerced` | |
| 11 | `level-invalid` | sim |
| 12 | `timestamp-coerced` | |
| 13 | `timestamp-invalid` | sim |
| 14 | `facility-coerced` | |
| 15 | `facility-invalid` | sim |
| 16 | `file-invalid` | sim |
| 17 | `line-coerced` | |
| 18 | `line-invalid` | sim |
| 19 | `field-coerced` | |
| 20 | `field-null` | |
| 21 | `field-id-reserved` | |
| 22 | `field-name-invalid` | |
| 23 | `key-unknown` | |
| 24 | `duplicate-key` | sim |
| 25 | `encoding-replaced` | sim |

`format=malformed` não tem aviso: tem `parse.error`.

### 6.9 `raw`

`raw` = o documento descomprimido inteiro, quando:

- `format = malformed`; ou
- algum aviso da coluna "força `raw`" (houve perda: um valor omitido, uma
  duplicata vencida, um surrogate trocado); ou
- `convert.keep_raw = always` (decisão 4; padrão `malformed`).

UTF-8 válido vai como string JSON; senão base64 com `raw-encoding: "base64"`
(só acontece em `malformed`).

### 6.10 `sid`

FNV-1a 64 de `"<transport>|<src-ip>|<src-port>|<dst-ip>|<dst-port>"` (com
`dst-ip` vazio quando desconhecido), reduzido a 53 bits — a fórmula de S §6.10.
Qualquer consumidor o recalcula de `payload.host` e `payload.transport`.

---

## 7. Filtro

Descarte por `level`, **antes do ring** (decisão 15):

```
aceita  <=>  level <= filter.min_level
         E   (filter.levels vazio  OU  level ∈ filter.levels)
```

- `level` é o **efetivo** (§6.5), lido pela mesma validação da conversão:
  ausente → 6, inválido → 6, documento `malformed` → 6.
- `filter.min_level` (padrão `7`, aceita tudo); nome ou número (`warning` = 4).
- `filter.levels` (padrão vazio): lista de nomes ou números 0–7. Sem sinônimos
  (`warn`, `error` são recusados no boot).
- A varredura só roda quando o filtro pode recusar algo (`min_level < 7` ou
  `levels` não vazio); sem isso, a recepção não lê o documento.
- Descarte: `gelf_messages_dropped_total{reason="filter"}`; no HTTP, `202`.
- **Recarregável por `SIGHUP`**.

---

## 8. Rate-limit

Token bucket por prefixo de rede, com shards e evicção exata por sweep, como a
standard: `ratelimit.rate` tokens/s, teto `ratelimit.burst`, chave `/24` (IPv4)
e `/48` (IPv6), `ratelimit.max_buckets` como rede de segurança,
`ratelimit.exempt` para CIDRs isentos.

- **A chave é o `src-ip` resolvido**: o par do transporte, ou o cliente do
  PROXY de um par confiável. O `host` dentro do documento GELF é alegação do
  emissor e **nunca** vira chave; `X-Forwarded-For` também não (§4.5).
- Vale para os cinco transportes, decidido **uma vez** por documento, depois do
  filtro e antes do ring.
- Estourar o limite **descarta** (`reason="ratelimit"`), também no TCP: ao
  contrário do ring cheio, um emissor acima do limite não escoa sozinho. No
  HTTP e no HTTPS a resposta é `503` com `Retry-After: 1` (§4.5), nunca `429`:
  o limite pode passar numa nova tentativa, e os clientes GELF HTTP tratam um
  `4xx` como final.
- `ratelimit.enabled = false` por padrão; com ele desligado e algum listener fora
  do loopback, `WARN` no boot. Recarregável por `SIGHUP`.
- **O contador de baldes muda junto dos mapas**, sob o lock de cada shard: o
  `Allow` soma ao inserir, e o `Reset` (troca de máscara por `SIGHUP`) e o sweep
  descontam o que aquele shard perdeu. Um `Store(0)` no fim do `Reset` apagaria
  inserções concorrentes em shards já limpos, e um desconto do sweep feito
  depois do laço levaria o contador abaixo de zero: `gelf_ratelimit_buckets`
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
    canal `done` eventual, arredondados). Contar `len(cru)` deixava um documento de
    8 193 bytes, copiado para o buffer de 64 KiB da sua classe de pool (§15.1),
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
TCP, TLS, HTTP ou HTTPS (com só UDP, `WARN`: um `fsync` por documento que o
kernel do emissor já pode ter descartado, e que um chunk perdido já pode ter
tornado incompleto). **Garantia:** `COMMIT` concluído antes da confirmação;
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
  ato; `ERROR` no log e `gelf_ring_quarantined_total`. Sem teto e sem limpeza
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
- **Com `sink.type = none` o backlog de uma vida anterior fica** (revisão
  independente do porte, 2026-10-13; a regra do F, F SPEC §8.4, e o motivo da
  decisão 26 dos dumps, §12.2). Drená-lo para um sink que descarta por
  configuração destruiria mensagens que uma vida com destino confirmou — o
  teste de carga no mesmo volume depois de um incidente. As linhas que o boot
  encontra ficam no `ring.db`, intocadas: o cursor começa depois do maior
  `seq` visto no boot (a `AUTOINCREMENT` põe toda linha nova depois dele), e
  elas ficam fora da contabilidade desta vida — `Stats()`, teto, `/readyz`,
  gauges, recount —, porque esta vida nunca as entrega. O boot as informa num
  `WARN` com a contagem e o caminho, e o `WARN` do shutdown e o do aborto as
  somam às que ficam (`of_a_previous_life`). As mensagens desta vida passam
  pelo ring e são descartadas normalmente. Uma vida com destino drena todas. O
  ring não sabe por quê: quem pede é a app, o único lugar que conhece os dois
  eixos (§20.15).

### 9.5 `ring.mode = redis`

Como S §9.5 (decisão 11 de S). Um **stream compartilhado** entre as instâncias, num Redis próprio
do ring (`[ring.redis]`), com consumer group. É o único ring que sobrevive à
perda do nó: o que uma instância aceitou outra pode entregar.

| Elemento | Valor |
|---|---|
| Stream | `ring.redis.stream` (padrão `gelf:ring`) |
| Entrada | `XADD <stream> * h <cabeçalho> b <cru>` |
| Contador de bytes | `<stream>:bytes` (string com inteiro) |
| Grupo | `ring.redis.group` (padrão `gelf-ring`), criado no boot com `XGROUP CREATE <stream> <group> 0 MKSTREAM` (`BUSYGROUP` é sucesso) |
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

- **Redis Cluster** exige um stream com *hash tag* (`{gelf:ring}`), porque os
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

**Política de despejo** (pendência 5 de 2026-10-11): a mesma regra do sink Redis
(§10.3). O Redis do ring precisa de `maxmemory-policy` `noeviction` (ou
`volatile-*`: as chaves do ring nunca têm TTL); sob `allkeys-*` um Redis cheio
não responde o `OOM` em que o ring se apoia para parar de aceitar, despeja
chaves, e o stream, que é uma chave só, leva todas as entradas de uma vez, com o
contador junto ou separado, e o ring UP. `allkeys-*` é FATAL no boot (exit 3),
checado **antes** de qualquer chave ser criada; ilegível (ACL sem `INFO` e
`CONFIG`, proxy), `WARN` com a premissa e o boot segue.

**Indisponibilidade do Redis do ring:** `Enqueue` devolve `ErrUnavailable`: o UDP
descarta (`reason="ring_unavailable"`), o TCP e o TLS param de ler, o HTTP e o
HTTPS respondem `503`, o `/readyz` falha. O
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
| `ring=sqlite` + `sink=none` | permitido; o backlog de uma vida anterior fica no `ring.db`, fora da conta (§9.4) |
| `ring=redis` + `sink=none` | permitido, `WARN`: o stream é compartilhado, e esta instância consome e descarta entradas que outras réplicas (e vidas anteriores) enfileiraram para um destino. Ao contrário do `ring.db`, não há como separar as de outra vida: só o operador sabe se o stream é de teste |

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
   sozinho. Um documento malformado recusado (`convert.accept_malformed =
   false`, §6.1) não entra em pedaço nenhum: é contado e confirmado junto.
3. `sink.Write(ctx, pedaço)`, com retentativa (§10.2).
4. Sucesso: `ring.Commit(itens do pedaço)`, devolve o cru e o buffer do pedaço
   aos pools, `gelf_events_written_total += n`.
5. Próximo pedaço.

O passo 2 roda **sempre**, inclusive com `sink=none`: sem ele o benchmark mediria
um pipeline que não existe. O teto de bytes existe porque o JLH chega a 6× o
documento (§3.9):
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
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`gelf_events_dropped_total{reason="sink_logic"}`), confirmado no ring |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`gelf_events_written_total`, só os demais em `sink_logic`, e o log traz só os
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

**Política de despejo** (pendência 5 de 2026-10-11, também para o ring Redis,
§9.5): o Redis do sink e o do ring precisam de `maxmemory-policy` `noeviction`
(ou `volatile-*`: estas chaves nunca têm TTL). Sob `allkeys-*` um Redis cheio
não responde `OOM`: despeja chaves, e o stream, que é uma chave só, leva todos
os eventos já confirmados ao emissor com o serviço UP; a sonda de escrita não
vê, porque sob `allkeys-*` um `XADD` no `maxmemory` dá certo, despejando. A
primeira checagem que alcança o servidor (a do boot, depois da sonda) lê a
política (`INFO memory`, senão `CONFIG GET maxmemory-policy`) e recusa
`allkeys-*` (qualquer política com esse prefixo) como FATAL, exit 3, **mesmo com
`maxmemory 0`**: a política diz o que acontece quando um limite existir, e um
`CONFIG SET maxmemory` com o Redis no ar a ativaria sem este servidor ver. Se a
ACL ou um proxy não deixar ler (resposta de erro às duas), `WARN` com a premissa
e o boot segue: recusar travaria toda implantação de ACL mínima. Só no boot: a
política não muda sem um operador. Um Redis Cluster é checado em um nó.

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
- **A sonda espera a trava da escritora só dentro do próprio prazo** (decisão do
  usuário em 2026-10-14, a mesma do item 23 do `go-webhook-server`). A
  escritora segura a trava durante o pedaço inteiro, até `sink.op_timeout`; a
  sonda esperava por ela sem prazo, e um pedaço mais longo que
  `sink.health_timeout` a fazia falhar com o banco íntegro — um ciclo DOWN (§13.3),
  com o backpressure de §2.3, por lentidão. Agora a sonda tenta tomar a trava
  dentro de `sink.health_timeout` (a trava é um semáforo de uma vaga, não um
  `sync.Mutex`, justamente para poder desistir):
  - obtida, faz o heartbeat como sempre;
  - **não obtida no prazo** (a escritora está ocupada) e o **último `Write`
    terminou sem erro**: a sonda conta **UP sem tocar o banco**. A escrita em
    curso é a prova de vida, e o que a sonda procura (disco cheio, arquivo
    somente leitura, volume desmontado) o próprio `Write` devolve como `fatal`
    — e passa a contar como último `Write` com erro;
  - não obtida no prazo e o último `Write` **falhou** (qualquer erro, de
    qualquer classe), ou nunca houve `Write` nem heartbeat bom neste processo: DOWN, como antes
    (`network`, alimentando os fusíveis de §13.3).
  - **a escritora ocupada só conta UP dentro de um prazo** (revisão
    independente de 2026-10-14, A1): o `Write` grava o instante em que tomou a
    trava, e a sonda só conta UP se o pedaço em curso a segura há no máximo
    `sink.op_timeout + sink.health_timeout`. Passado isso, DOWN (`network`),
    mesmo com o último `Write` sem erro. O `COMMIT` e a rotação não recebem
    contexto: um `fsync` preso (NFS sem resposta, disco morrendo) segurava a
    trava para sempre, e o "último `Write` sem erro" — o pedaço **anterior** —
    mantinha o sink em UP sem prazo, sem fusível, sem FATAL nem dump. O que
    conta é há quanto tempo o pedaço em curso segura a trava, não a idade do
    último que terminou.
  - **a falha do próprio heartbeat conta como último `Write` com erro**
    (revisão, A2): sem isso, uma sonda que tomou a trava e falhou (arquivo
    somente leitura, disco cheio) dava DOWN, e a seguinte, com a escritora
    ocupada e o `writeOK` de antes, dava UP; o UP intercalado zerava a
    sequência que os fusíveis de §13.3 contam.

  - **a idade do pedaço em curso é medida no relógio monotônico** (revisão
    independente de 2026-10-14, A3): o `Write` grava `time.Since(base)`, com
    `base` tomado no `NewSQLite`, e a sonda compara com `time.Since(base)`.
    Com o relógio de parede (`time.Now().UnixNano()` e
    `time.Since(time.Unix(0, …))`, sem leitura monotônica), um passo do NTP
    para trás (ou ajuste manual) durante um pedaço preso dava idade negativa
    ou pequena e UP com a escritora presa, pelo tamanho do passo; um passo
    para frente dava um DOWN espúrio com escrita normal.
  - **um heartbeat bem-sucedido grava "último `Write` sem erro"** (revisão,
    A4): ele tomou a trava e escreveu no arquivo, a mesma prova de um pedaço
    bom. Sem isso, depois de um heartbeat falho (disco cheio, liberado
    depois) o `writeFailed` ficava até o próximo `Write` bom, e o primeiro
    pedaço mais longo que `sink.health_timeout` dava DOWN com tudo íntegro —
    o ciclo DOWN que esta regra existe para eliminar. Consequência: o
    heartbeat do `WaitReady` do boot (§13.1) já deixa a marca, e o primeiro
    pedaço lento depois do boot também conta como escritora ocupada (D5 da
    revisão do `go-syslog-server`: antes, um *backlog* grande logo após o boot, com o
    primeiro pedaço acima de `sink.health_timeout`, dava um DOWN — "nunca houve
    `Write`").
  - **o heartbeat que falha pelo próprio contexto não marca o `Write` como
    falho** (revisão, A1 da família RELP): a sonda pode tomar a trava pouco
    antes do prazo, porque o `Write` a soltou em `prazo − ε`, e o
    `ExecContext` roda com ε e falha com `context deadline exceeded`; o
    cancelamento do shutdown dá `context canceled`. A sonda devolve o erro
    (DOWN dessa vez, `network`), mas só grava `writeFailed` quando
    `ctx.Err()` é nulo e o erro não é `DeadlineExceeded`/`Canceled`: um
    prazo da sonda não diz nada do arquivo, e marcá-lo fazia as sondas
    seguintes, com a escritora ocupada, darem DOWN até o próximo `Write` —
    o mesmo ciclo, com probabilidade ε/duração do pedaço a cada sonda.
  - "qualquer classe" inclui `logic` (registrado em 2026-10-14, D5 da
    revisão do RELP): com `ON CONFLICT DO NOTHING`, um `logic` (`TOOBIG`,
    `MISMATCH`) é improvável e dá no máximo um DOWN pontual com o banco são
    até o próximo `Write` ou heartbeat bom; separar as classes não compensa a
    regra a mais.
  - só o `Write` grava o instante da trava (registrado em 2026-10-14, D6 da
    revisão do RELP): quando quem segura a trava é o `Close`, a troca de
    `keep_files` ou a rotação de teste, a idade lida é a do último `Write`,
    possivelmente antiga, e a sonda dá DOWN. Na prática só o `Close` do
    shutdown segura a trava por tempo relevante, e então o resultado da sonda
    já é ignorado.

  Travas: `TestSQLiteProbeDoesNotWaitForTheWriterPastItsDeadline` (segura a
  trava no próprio teste, sem corrida; inclui a escritora presa além do prazo),
  `TestSQLiteFailedHeartbeatCountsAsAFailedWrite`,
  `TestSQLiteProbeAgeIgnoresWallClockSteps` (A3),
  `TestSQLiteGoodHeartbeatClearsAFailedWrite` (A4) e
  `TestSQLiteHeartbeatCutByItsOwnContextIsNotAFailedWrite` (A1 do RELP).

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
  `gelf_sink_postgres_duplicates_total`.
- `sink.workers` no padrão é rebaixado para 4 com `WARN`; `WARN` acima de 16;
  `pool_size = 0` deriva `workers`, e um `pool_size` abaixo de `workers` é
  exit 1: cada worker segura uma conexão enquanto escreve.
- **A sonda de saúde tem conexão própria, fora do pool** (revisão independente
  do porte, 2026-10-13): um segundo pool de **uma** conexão, cópia da
  configuração do primeiro (mesmo servidor, credenciais, TLS e parâmetros de
  sessão, então o que a validação lê de uma sessão é o que toda sessão de
  escrita tem); o `Health` e a validação do boot inteira rodam nela. Antes a
  sonda tirava a conexão do pool dos workers, e a regra era `pool_size ≥
  workers + 1` (revisão independente do RP, 2026-10-13): com `ring=none` quem
  escreve são os receptores, até `ring.max_inflight`, e a conexão de folga não
  protegia nada — escritas lentas mas funcionando seguravam todas, a sonda
  estourava `sink.health_timeout` esperando uma, o sink ia a DOWN e, passado
  `fail_timeout`, a FATAL: exit 3 sem nada errado no banco. Vale igual para
  `sink=mysql` (um segundo `sql.DB` do mesmo conector, `MaxOpenConns(1)`) e
  `sink=clickhouse` (uma segunda conexão do `clickhouse-go`, §10.10.2). A
  instância continua abrindo `workers + 1` conexões no padrão. Tetos de §13.2:
  `workers` 1..1024, `pool_size` 0..4096.
- `sink.postgres.addr` é **um servidor só**: `host:porta` (porta 1–65535 escrita
  na forma canônica, host sem `,`, branco nem `:` fora de um literal IPv6) ou um
  diretório de socket começando com `/`, com `:porta` opcional, que nomeia o
  arquivo do socket (`/run/pg:5433` é `/run/pg/.s.PGSQL.5433`), sem `,` nem
  branco. `h1,h2:5432`, `[h:5432]:5433` ou um tab passavam na validação e
  falhavam no DNS ou no dial até o `boot_timeout` (pendência 22 de 2026-10-11;
  revisão independente do RP, 2026-10-13).
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
  só de `[sink.postgres]` (pendência 9 de 2026-10-11): nenhuma variável libpq
  (`PGSSLMODE`, `PGPORT`, `PGSERVICE`, `PGCHANNELBINDING`, `PGREQUIREAUTH`,
  `PGSSLNEGOTIATION`, `PG{MIN,MAX}PROTOCOLVERSION`, `PGOPTIONS`,
  `PGTARGETSESSIONATTRS`, `PGAPPNAME`…) chega ao pool nem derruba o parse
  (antes, um `PGSSLMODE` inválido era exit 1 e um `PGCHANNELBINDING=require`
  recusava toda conexão sem SQLSTATE, retentado como rede). Em tempo
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
  **Nenhuma `RULE` de `INSERT` ou `UPDATE`** na fila (`pg_rewrite`, `ev_type`
  `2`/`3`): o PostgreSQL recusa `INSERT ... ON CONFLICT` numa tabela com uma
  (`0A000`), e cada pedaço falhava como `network`, para sempre, com o sink UP
  (achado da revisão do `go-relplog-pg-server`, 2026-10-13). `RULE` de `DELETE`
  ou `SELECT` não toca o `INSERT` e é aceita; a mensagem nomeia as regras.
- Durabilidade: a do servidor (`synchronous_commit`). A skill admite a fila
  `UNLOGGED`, que o servidor não confere: ela é apagada numa queda do PostgreSQL
  e só serve se a origem puder reenviar, o que nenhum emissor GELF faz (o GELF
  não tem confirmação por mensagem: o que o servidor aceitou o emissor esqueceu).

| SQLSTATE | Classe |
|---|---|
| `08`, `53`, `57`, `40` | `network` |
| `22`, `23` (fora do `ON CONFLICT`) | `logic` |
| **`42`** (esquema), `28`, `2F`, **`3D`**, `42501`, `25006`, **`23502`**, **`0A000`**, `23514` sem constraint nomeada | **`fatal`** |
| qualquer outra | `network` |

`23502` (`not_null_violation`) é `fatal` (pendência 7 de 2026-10-11): o
servidor nunca manda `NULL` nas dez colunas, então ele vem da tabela (coluna
obrigatória acrescentada, `DROP DEFAULT`, trigger) e, como `logic`, descartaria
todo pedaço com o sink UP; o `1364` do MySQL já era `fatal` por isso. A classe
`3D` (`sink.postgres.database` inexistente, `3D000` no login) é `fatal`, como o
`1049` do MySQL (pendência 8 de 2026-10-11): retentar até o `boot_timeout` só
adiava o exit 3 com a mensagem de rede. `0A000` (`feature_not_supported`) é o
que o `INSERT ... ON CONFLICT` recebe de uma `RULE` criada depois do boot:
`fatal`, não `network` retentado para sempre (2026-10-13).

**SQLSTATE fora da tabela é `network`** (`P0001` de uma trigger que levanta
exceção, ou qualquer outro código não listado). É regra, não pendência (porte do
RP/RN, decidido em 2026-10-14): nunca descartado, nunca FATAL. O pedaço é
retentado com o sink UP, o ring enche e segura a recepção (§20.18: um ring que
enche é visível, um pedaço descartado não), e o operador vê
`gelf_sink_errors_total{kind="network"}` subir, com o SQLSTATE no `WARN`
"sink write failed, retrying". As alternativas são piores: `logic` descartaria
pedaços em silêncio a cada poucos milissegundos se a trigger recusa tudo;
`fatal` derrubaria o servidor (dump, exit 3) por uma trigger que talvez recuse
uma linha só. Tratar a classe `P0` como `fatal` seria uma decisão de contrato
separada, sem caso real que a peça.

**A validação do boot tem prazo próprio** (pendência 6 de 2026-10-11), aqui,
no MySQL (§10.6) e no ClickHouse (§10.10.5): roda com `sink.op_timeout`, não no
`sink.health_timeout` (1 s) da checagem que a dispara — são uma dúzia de idas e
voltas, e a ~55 ms nenhuma tentativa cabia em 1 s, o boot saindo com 3 depois do
`boot_timeout` e culpando a rede —, e um cancelamento do boot (`SIGTERM`) ainda
a interrompe (no ClickHouse, um `WithoutCancel` puro fazia o `SIGTERM` esperar o
`op_timeout` inteiro). O sink vigia o cancelamento no "tempo de vida" que a
checagem carrega no contexto (a chave que `Health.WaitReady` e `Health.check`
põem, §13.3); sem ele, vigia o contexto da checagem menos o prazo, o que pega um
cancelamento dentro da janela da checagem mas não um posterior.

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
- **`autocommit=1` da sessão** (pendência 10 de 2026-10-11): com
  `autocommit=0` global (`my.cnf`, `init_connect`), o `INSERT` ficava numa
  transação que ninguém commitava, o `Write` devolvia sucesso e as linhas sumiam
  com a conexão (a entrega parcial de §10.2 também depende disso). O driver o
  manda como `SET` depois do handshake, por cima do `init_connect`. A validação
  confere `@@session.autocommit = 1` (FATAL se não).
- **Escrita:** `INSERT INTO ... VALUES (...), (...) ON DUPLICATE KEY UPDATE uuid
  = uuid`. **Nunca `INSERT IGNORE`**, que rebaixa `1406` e `1366` a aviso e trunca
  em silêncio. Duplicatas = `len − afetadas`.
- O pedaço é **dividido** de novo antes de ir ao servidor: no máximo 65 535
  placeholders por comando (treze por linha → **5 041 linhas**; `sink.batch`
  acima disso é exit 1), e no máximo 7/8 do `max_allowed_packet` lido no boot
  (custo estimado por linha: `env` + o `hostname` saneado + 512 bytes, que
  cobrem o `uuid`, os seis inteiros, os prefixos de comprimento, o mapa de
  nulos e as colunas curtas `space`, `src_ip`, `format`, `transport`). A maior
  linha possível é `pior JLH + L + 512` (§3.9): o `hostname` é um trecho do
  documento, e o NUL que chegou como o escape de seis bytes `\u0000` sai como
  U+FFFD, de três.
- **Validação no boot** (cada recusa é exit 3), com o prazo próprio de §10.5:
  tabela existe e é InnoDB; colunas com as famílias de tipo de §3.7, e as que o
  servidor preenche com largura para o maior valor dele (`space` 64, `format` 9
  — `malformed` —, `transport` 5 — `https` —, `src_ip` 39, o IPv6 canônico
  mais longo): uma coluna
  estreita (`space VARCHAR(16)`) recusava depois as linhas com `1406`, `logic`,
  com o sink UP; o índice único exatamente sobre `(uuid, step)`, colunas
  inteiras (sem `SUB_PART`), e **o único** (pendência 11 de 2026-10-11): o
  `ON DUPLICATE KEY` dispara em qualquer chave única, e um evento novo que
  colidisse em outro índice (ou num prefixo `UNIQUE (uuid(4), step)`, que dois
  eventos do mesmo milissegundo compartilham) era confirmado como duplicata e
  perdido, sem erro; tolera-se só uma coluna `AUTO_INCREMENT` sozinha (chave
  substituta, que nunca colide); `@@session.autocommit = 1`;
  `max_allowed_packet` ≥ a maior linha (§3.9); e uma **sonda** de **duas
  linhas**, ambas linhas que um listener produz: uma `udp` do endereço mais
  curto, com o único `NULL` possível (`hostname`: um documento sem `host`
  válido não tem), e uma com cada coluna no extremo (`space` de 64 caracteres,
  o `format` mais longo, o `transport` mais longo, `https`, o `src_ip` mais
  largo, `level` 7, `sid` 2^53−1, `ev_uts` 253402300799, `ev_sub` 999999999,
  `nbytes` = o maior JLH). Todo transporte deste servidor é de rede (§3.1),
  então toda linha tem `src_ip`: ao contrário do `go-syslog-server`, que sonda
  um `src_ip` `NULL` com o transporte `unix`, uma tabela com `src_ip NOT NULL`
  é legítima aqui. Uma tabela que recusa uma das duas linhas (`hostname NOT
  NULL`, `transport CHAR(3)`) continua recusada: o dreno grava essas linhas.
  A sonda é o mesmo `INSERT ... ON DUPLICATE KEY UPDATE` do
  dreno, com `uuid` de versão 0 (`00…00` e `00…01`, que o gerador nunca cunha),
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
| sem número (EOF, `ErrBadConn`, prazo), `1040` `1053` `1077` `1927` `1158`–`1161` `1205` `1213` `1317` `3024` `2006` `2013`, e os limites do servidor que passam sozinhos e vêm com SQLSTATE `42000` — `1203` (`max_user_connections`), `1226` (limite por usuário, inclusive os `*_PER_HOUR`), `1461` (`max_prepared_stmt_count`) | `network` (os três últimos listados porque o recurso ao SQLSTATE os faria `fatal`: um deploy que sobrepõe o orçamento de conexões viraria dump e exit 3 na frota inteira; revisão do `go-relplog-server`, 2026-10-11) |
| `1290` `1836` (somente leitura), `1021` `1030` `1041` `1114`, `1044` `1045` `1049` `1130` `1142` `1143`, `1054` `1146` `1364`, `1048` | **`fatal`** |
| `1062` `1153` `1264` `1366` `1406` `1690` `3819`, `ErrPktTooLarge` | `logic` |
| qualquer outro | pelo **SQLSTATE** do erro: `22`/`23` `logic`, `42`/`28` `fatal`, `08`, `HY000` e sem SQLSTATE `network` |

O recurso ao SQLSTATE (pendência 11 de 2026-10-11): "desconhecido é
`network`" sozinho retentava para sempre, com o sink UP, uma recusa do dado que
a tabela não lista (o `4025` de `CHECK` do MariaDB, `23000`; um `3156`,
`22018`), e fazia um `1064` (`42000`, o erro de sintaxe que um nome mal citado
daria) esperar o `boot_timeout` como rede. O `1062` não vem mais do `INSERT`
(outro índice único é recusado no boot, e o `ON DUPLICATE KEY` absorve colisão
em qualquer um) e fica `logic` só para o que outro comando gerar (um trigger).
`3819` (um `CHECK`, com SQLSTATE `HY000`, que o recurso não lê) é `logic` por
número. O `1048` (coluna que não aceita `NULL`) é **fatal**, gêmeo do `23502`
do PostgreSQL (§10.5): a sonda do boot provou que toda coluna anulável aceita
`NULL`, e toda coluna `NOT NULL` do contrato sempre recebe valor; um `1048`
depois do boot é uma coluna que virou `NOT NULL` ou um trigger, e como `logic`
descartava toda linha com `NULL` ali com o sink UP (revisão independente do
porte, 2026-10-13).

`sink.mysql.database` e `sink.clickhouse.database` entram crus entre crases no
`INSERT` e na sonda: são recusados (exit 1) com crase, barra invertida (o
escape do ClickHouse dentro de crases), byte de controle (< 0x20 ou 0x7F) ou
mais de 64 **caracteres** (não bytes: o limite do MySQL é de caracteres, e um
nome de 64 `ç` foi recusado até a revisão independente do porte, 2026-10-13),
na validação da config (§13.2) e no construtor do sink
(pendência 12 de 2026-10-11). Antes, uma crase quebrava o comando e, no MySQL,
o `1064` era `network`, e a sonda esperava o `boot_timeout`.

### 10.7 `sink.type = nats` (padrão)

Como S §10.7, com o subject único da decisão 16. `github.com/nats-io/nats.go` com o pacote `jetstream` (a API
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

`WithPublishAsyncMaxPending` = `2 × (sink.workers × sink.batch + ring.max_inflight)`:
o dobro do que os workers e as escritas síncronas de `ring=none` podem ter no ar
ao mesmo tempo. A folga existe porque os futuros de um pedaço contaminado
continuam contando até o próprio prazo assíncrono, que começa na publicação, um
pouco depois do prazo do worker: sem ela, uma retentativa rápida parava na janela
cheia e falhava com `ErrTooManyStalledMsgs` (revisão independente do
`go-relplog-nats-server`, 2026-10-13). `Data` aponta para o buffer do pedaço, que só volta ao pool depois de todos os
futuros resolverem; em qualquer caminho de erro fica para o GC.

`PubAck.Duplicate` conta em `gelf_sink_nats_duplicates_total`. Fora da
`duplicate_window` do stream, uma redelivery duplica (at-least-once).

**`sink.nats.mode = core`** (opt-in, *best effort*): `PublishMsg` por envelope e um
`FlushTimeout(sink.op_timeout)` por pedaço. O sucesso significa que o servidor
NATS **recebeu** as mensagens, não que alguém as guardou: sem stream, uma
mensagem sem assinante some. Combina com o UDP, que já é lossy; o boot registra
`WARN` com essa frase.

**Subject** (§3.8): só o exato, `loghub.{space}.gelf` (decisão 16). Não há
chave `sink.nats.subjects`, nem subjects granulares: o GELF não tem um par
facility/severidade fechado que os sustente. O subject é derivado, não
configurável: a regra `loghub.{space}.{type}` é do JLH.

**Validação no boot** (`jetstream`; cada recusa é exit 3):

| Verificação | Como |
|---|---|
| O stream existe | `js.Stream(sink.nats.stream)` (padrão `LOGHUB_GELF`) |
| O stream **cobre** o subject | casamento local de padrões: algum subject do stream cobre `loghub.<space>.gelf` |
| Nenhum subject do stream captura o plano de controle | um padrão do stream que case `<ctl_namespace>.beacon` (como `loghub.>`) é recusado: reproduziria beacons antigos para todo consumidor |
| Não está selado | `Sealed = false` |
| Confirma publicações | `NoAck = false`: um stream com `no_ack` deixaria toda escrita esperar o prazo como `network`, para sempre, com o sink UP |
| O maior evento cabe | `nc.MaxPayload()` e `max_msg_size` do stream contra §3.9 + headers; com `storage: file`, também contra os 32 MiB por registro do file store (`10077`); com `discard=new`, também contra `max_bytes` do stream: um `max_bytes` abaixo de um registro recusaria o maior evento para sempre, e o `10077` dele, que agora é `network`, seguraria o ring sem fim |
| Retenção guarda o que confirma | `retention: interest` é FATAL: sem consumidor interessado o servidor responde com `PubAck` normal e não guarda nada, e o pedaço seria confirmado e perdido, sem erro nem contador (pendência 17 de 2026-10-11; decisão portada do R, `docs/DECISOES.md` §2). `limits` e `workqueue` são aceitos |
| Teto por subject | `max_msgs_per_subject > 0` só gera `WARN`, uma vez por valor: todo evento de um `space` divide um subject, e os mais antigos são descartados conforme os novos são confirmados |

Com `core`, só o `max_payload`. O stream é criado por quem administra o NATS, a
partir de `docker/nats/stream.json`; o servidor nunca cria nem altera um stream.

**O contrato vale a vida inteira** (pendência 16 de 2026-10-11): ele é conferido em
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

**Permissão negada é FATAL** (pendência 14 de 2026-10-11, como em S, SN e R). O NATS recusa um
publish sem permissão só com um `-ERR` assíncrono, e a negação vira um
**veredito permanente** quando atinge o caminho de dados: publish num subject de
dados (`loghub.<space>.gelf`, de qualquer `space`), qualquer coisa em `$JS.API`, ou a assinatura em `_INBOX` por onde voltam
as confirmações. **Nunca** a negação no subject do beacon. O `ErrorHandler` grava
o veredito; o `Health` e todo `Write` o devolvem como `fatal`; o `writeCore` lê o
`LastError` depois do flush (o `-ERR` chega antes do `PONG`); em `jetstream`, o
prazo do futuro de um publish negado vira o veredito, com o pedaço contaminado
(o primeiro pedaço negado é fatal só depois do `sink.op_timeout`; em `core`, na
hora). Antes, em `jetstream` o worker retentava para sempre com o sink UP, e em
`core` o pedaço era confirmado e perdido.

**Uma mensagem recusada pelo cliente antes de sair** (`ErrMaxPayload`: uma
reconexão a um nó com `max_payload` menor, ou uma linha de dump de outra vida)
é recusada **sozinha**, como um `10054`, nos dois modos (pendência 15 de 2026-10-11): as demais do pedaço são
publicadas e `Error.Delivered` diz quais saíram (§10.2). Encerrar o pedaço nela
confirmava sem publicar as seguintes e contava como perdidas as anteriores.

**Dump de outro `space` em `core`** (pendência 20 de 2026-10-11): o envelope é publicado e gera um `WARN` por
`space` estrangeiro (nunca um por mensagem): sem
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
host do servidor alcançado (pendência 13 de 2026-10-11): o `ConnectedUrlRedacted` do `nats.go` esconde a
senha, não um token (`nats://<token>@host`, que a `LOGHUB_EB_URL` aceita). Uma URL
que não parseia nunca é ecoada; no lugar dela vai o `host:porta`. Cada entrada
de `sink.nats.urls` é parseada na validação, sem eco, e credencial na URL junto
de `username`/`password`/`token`/`credentials` é exit 1 (§13.2).

**TLS que nunca vai dar certo é FATAL, no boot e depois dele.** Duas recusas de
TLS não passam com retentativa: o certificado do servidor que não verifica
(autoridade desconhecida, nome que não confere, vencido; o `nats.go` embrulha o
`tls.CertificateVerificationError` do *handshake* em `ErrTLS`) e
`sink.nats.tls` ligado contra um servidor que não oferece TLS
(`ErrSecureConnWanted`). As duas pedem o operador (uma CA, um certificado, a
configuração) e são **`fatal`**; outra falha do *handshake* (um reset, um prazo)
continua `network`. Um servidor que **passa a exigir** TLS é promovido pelo
`nats.go` sozinho (com as raízes do sistema, se `sink.nats.tls` estiver
desligado) e só cai aqui se o certificado não verificar. No primeiro `CONNECT`
o erro volta síncrono e o boot sai com 3 na hora, em vez de esperar o
`boot_timeout` com a mensagem da rede. **Depois do boot**, numa reconexão, o
`nats.go` nunca o entrega ao `ReconnectErrHandler` (que ele só chama quando a
discagem TCP falha): o erro fica no `LastError`, com o estado `RECONNECTING`.
Por isso o `Health`, ao ver o cliente `RECONNECTING`, lê o `LastError` (o
`escalate`) e transforma esse erro em veredito, FATAL nomeando o certificado.
Antes, a checagem dizia "connection is RECONNECTING", `network`, e o FATAL vinha
dos fusíveis uns 20 s depois, com o motivo errado, ou nunca, com os dois
fusíveis desligados (revisão independente do `go-relplog-nats-server`,
2026-10-13, adaptada: lá não há TLS e todo servidor que o exige é recusado).
**Uma discagem que falha na reconexão não é logada**: o `WARN` de desconexão,
o `/readyz` e `gelf_sink_up` já dizem o estado, e um `ERROR` amostrado a cada
30 s durante uma queda longa só repetia isso; o `ReconnectErrHandler`
(`onReconnectErr`) só põe e loga (`ERROR`) o veredito — a recusa de TLS do
servidor único, abaixo. Era regra só do código; escrita e travada no porte do
RP/RN, 2026-10-14 (`TestNATSReconnectDialFailureIsNotLogged`).

**Só é veredito quando nenhum outro servidor pode servir** (revisão
independente do porte, 2026-10-13). Com um *pool* — mais de uma URL em
`sink.nats.urls`, ou os servidores que um cluster anuncia (`nc.Servers()` conta
os dois) — o `LastError` guarda a recusa do **último** servidor tentado
enquanto o cliente percorre o pool: um nó com certificado de outra CA, ou um nó
descoberto cujo nome não está no certificado, transformava um restart do nó
confiável em FATAL permanente, dump e exit 3, embora ele voltasse em segundos.
Por isso a recusa de TLS vira veredito só com `len(nc.Servers()) == 1`, no
`Health` (`escalate`), no `ReconnectErrHandler` (que recebe a de um `wss://`) e
no primeiro `CONNECT`; com mais de um servidor é `network`, e o erro da
checagem diz "the last server tried refused TLS" com o erro do certificado: se
nenhum servidor do pool voltar, os fusíveis de §13.3 dão o FATAL nomeando o
TLS. No primeiro `CONNECT` com mais de uma URL o `nats.go` tenta todas uma vez e
devolve o erro da última: a recusa de TLS vira `network` e o boot espera o
`boot_timeout` por um servidor que sirva (a linha do exit 3 nomeia o TLS). Com
um servidor só, continua o veredito imediato. Travado com `nats-server` real e TLS de
verdade (`TestNATSTLSPoolOneUntrustedNodeIsNotAVerdict`,
`TestNATSTLSPoolBootWaitsForATrustedNode`,
`TestNATSTLSPoolTrustedServerSurvivesARestart`, com
`GELF_TEST_NATS_SERVER_BIN`).

**Pool só de servidores que recusam** (registrado em 2026-10-14, D2 da revisão do `go-syslog-server`): com duas ou mais URLs e **todas** recusando o TLS (certificado de outra CA, nome fora do certificado, servidor que exige TLS sem `tls://`), nunca há veredito — cada checagem é `network`, com o motivo no erro do `Health`, e o FATAL só vem dos fusíveis de §13.3; com os dois fusíveis desligados, nunca vem. É consequência da regra acima: o `LastError` é o do último servidor tentado, e saber que **nenhum** nó vai servir exigiria acompanhar a volta inteira do pool. O sinal é `gelf_sink_up` em 0 e o `/readyz`.

**Uma URL só cujo nome resolve a vários IPs** (registrado em 2026-10-14, D3 da revisão do RELP): o `nats.go` resolve o nome e tenta um IP, mas `nc.Servers()` tem uma entrada só, então a recusa de TLS do IP sorteado é veredito FATAL, embora outro IP do mesmo nome pudesse servir (um serviço *headless* do Kubernetes com nós configurados de jeitos diferentes). Não há como distinguir sem `SkipHostLookup`, que tiraria o rodízio entre os IPs: é limite aceito — os nós atrás de um nome devem ter a mesma configuração de TLS, ou entram como URLs separadas, e a regra do *pool* vale.

**Um *flush* que atravessa uma reconexão não confirma** (`mode=core`). O `nats.go`
falha um *flush* pendente com `ErrConnectionClosed` quando a conexão **cai**,
embora o cliente só esteja reconectando: a tabela classifica esse erro como
`fatal` (um cliente fechado de vez), e um soluço do NATS no meio do *flush* era
FATAL, dump e exit 3. O `flushErr` o reclassifica como `network` enquanto a
conexão não está `CLOSED` (no `Write` e no `Health`). E um *flush* que
**começa** durante a reconexão é resolvido pelo `PONG` do servidor novo,
embora os `PUB` do pedaço tenham ido pelo socket velho: o `writeCore` compara o
número de reconexões antes da primeira publicação e depois do *flush*, e se ele
mudou o pedaço é `network`, incerto, não confirmado (`go-syslog-nats-server`
TESTING §8 item 1 e revisão independente do `go-relplog-nats-server`,
2026-10-13).

**`Health()`:** o estado da conexão (`CONNECTED` é saúde, `CLOSED` é fatal,
`RECONNECTING`/`DISCONNECTED` é falha de rede) e, em `jetstream`, um
`StreamInfo` (um `PING` responderia feliz com o JetStream desligado); em `core`,
um `FlushTimeout`.

| Erro | Classe |
|---|---|
| prazo, `ErrTimeout`, `ErrNoResponders`, `ErrNoServers`, `ErrDisconnected`, `ErrConnectionReconnecting`, `ErrReconnectBufExceeded`, `ErrNoStreamResponse`, `ErrAsyncPublishTimeout`, `ErrTooManyStalledMsgs` | `network` |
| `10077` cuja descrição é de um stream **cheio** com `discard=new` ("maximum messages exceeded", "maximum bytes exceeded", "maximum messages per subject exceeded") ou **fechando** ("store is closed", o `ErrStoreClosed` do nats-server, `server/store.go:43`; um restart do NATS) | `network`: backpressure, passa quando um consumidor confirma ou o servidor volta (revisão independente, 2026-10-11; como `fatal` eram dump, exit 3 e duplicatas do pedaço já gravado na reingestão). Até a revisão independente do porte (2026-10-13) a regra casava "store closed", texto que nenhum nats-server manda, e o teste usava o mesmo texto inventado: o stream fechando continuava `fatal` |
| `10158` (409: o mesmo `Nats-Msg-Id` ainda numa proposta Raft, na retentativa depois de um prazo num stream em cluster), `10167` (429: a fila de entrada do stream cheia) e todo outro erro da API com código 429 | `network`: passam sozinhos (pendência 18 de 2026-10-11; antes, "outro 4xx" os fazia FATAL, dump e exit 3, talvez na frota inteira) |
| `ErrAuthorization`, `ErrAuthExpired`, `ErrAuthRevoked`, `ErrAccountAuthExpired`, as frases do servidor no primeiro `CONNECT` ("authorization violation", "authentication expired", "authentication revoked"; **nunca** "permissions violation", que é de um subject e vai pelo veredito; pendência 19 de 2026-10-11: o `errors.Is` do `nats.go` compara o texto inteiro, e o boot esperava o `boot_timeout` com o motivo errado), o veredito de permissão, o TLS que nunca vai dar certo (certificado que não verifica, `ErrSecureConnWanted`), `ErrConnectionClosed` com a conexão `CLOSED` (só reconectando, `network`: o `flushErr` acima), `ErrStreamNotFound` (`10059`), JetStream desligado (`10076`, `10039`), stream esperado não confere (`10060`), selado (`10109`), recursos (`10023`), outro armazenamento (`10077`: um registro que o store não aceita) | **`fatal`** |
| `ErrMaxPayload`, `ErrMaxBytesExceeded`, `10054`, `10097` | `logic` (só daquela mensagem) |
| outro erro da API com código 4xx (um 409 desconhecido inclusive) | `fatal` |
| outro erro da API com código 5xx, qualquer outro | `network` |

**Autenticação:** uma de `username`/`password`, `token` ou `credentials` (arquivo
`.creds`), ou a credencial na URL, e só uma (exit 1 com mais de uma). TLS verificado por padrão,
`tls_ca` própria, `tls_insecure` desliga.

### 10.8 `sink.type = none`

Descarta. Mede o custo de recepção, moldura, conversão e serialização sem I/O de
destino. **Nunca sai de UP.**

### 10.9 `LOGHUB_EB_URL`

Como S §10.9 (decisão 10 de S). `loghub.eb_url` (env `LOGHUB_EB_URL`) é a URL do eventbus da
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
só (um segundo `://` é recusado com a dica de codificar `%3A%2F%2F`, porque
pode estar dentro da senha). `sslmode=verify-ca` também liga o TLS; o
PostgreSQL aceita `host=/dir` do socket, e então **a porta da URL vale** e
`addr` fica `/dir:porta` (a libpq disca `/dir/.s.PGSQL.<porta>`; antes a porta
era descartada e o cliente discava 5432, talvez outro cluster, sem como
corrigir, porque `addr` é chave dona). O `host=` nomeia **um** diretório: com
`,`, `:` ou branco é exit 1 (a libpq lê `/run/pg,/x` como dois servidores e
`/run/pg:5433` como um diretório com `:` no nome); a porta vai na URL
(`postgres://u@:5433/db?host=/run/pg`). Um host codificado
(`postgres://%2Frun%2Fpg/db`, que a libpq lê como socket) é recusado com a dica
de `?host=/dir`. Qualquer outro parâmetro de consulta é recusado, não ignorado,
e o nome dele só é citado se for uma palavra-chave da libpq (uma senha colada
depois de um `?` também é "chave"). Um parâmetro **repetido**
(`?sslmode=disable&sslmode=require`) é exit 1: o leitor pegava o primeiro e a
libpq pega o último. Um `sslmode=` **vazio** é exit 1: a libpq o recusa, e lido
aqui como "ausente" a URL diria duas coisas a dois leitores (pendência 22 de
2026-10-11; revisão independente do RP, 2026-10-13). Pelo mesmo motivo, um
`host=` **vazio** é exit 1: a libpq usa o diretório de socket padrão dela, e
este servidor discaria o host da autoridade. TLS sobre socket (`host=/dir` com
`sslmode=require`, `verify-ca` ou `verify-full`, ou `sink.postgres.tls=true`
com `addr` que começa com `/`) é exit 1, como no MySQL: o sink mandava um
`SSLRequest` pelo socket, o servidor recusava, e o erro `network` era
retentado até o `boot_timeout` (a libpq ignora o `sslmode` num socket). Uma
`,` no host de uma URL Redis é exit 1: `sink.redis.addrs` é uma lista, e
`redis://a,b:6379` virava duas sementes onde qualquer outro leitor vê um host
só; a lista vai em `sink.redis.addrs` (revisão independente do porte,
2026-10-13).

**Usuário e banco são obrigatórios na URL PostgreSQL** (decisão portada do RP,
2026-10-13; `docs/DECISOES.md`). `postgres://h/db` e `postgres://u:p@h` são
exit 1, nomeando a parte que falta. Antes, sem eles, a libpq de qualquer outro
leitor da mesma URL usaria o usuário do sistema operacional e um banco de mesmo
nome, e este servidor usava o padrão do registry (`gelf`): dois leitores de
uma `LOGHUB_EB_URL` chegavam a dois bancos, e a URL é dona das duas chaves, então
o operador não podia corrigi-las em outro lugar. Consequência para o dump: uma
URL PostgreSQL sempre tem usuário, e um usuário sem senha é redigido inteiro
(pode ser um token colado no lugar errado), então esse dump traz o aviso de
segredos e não é relido como está.

**A URL nunca aparece num erro** (pendência 30 de 2026-10-11): do `url.Parse`
fica só o tipo do erro, com a dica de codificar `%`, `/`, `#`, `?` e `,` (o
`invalid port ":<x>"` do parser era a senha até a primeira `/`); o `sslmode`
inválido não é citado. Um `@` depois do primeiro `/`, `?` ou `#` da autoridade
é recusado com a dica de `%2F`, `%3F`, `%23`: nenhuma URL destes esquemas tem
`@` literal no caminho, na consulta ou no fragmento, então é uma senha com um
deles sem codificar, que encurta a autoridade (`postgres://app:39182#Zs3@db/x`
discava `app:39182` sem senha, com o `sslmode` jogado fora no fragmento). Uma
URL PostgreSQL com **fragmento** (`#`, mesmo vazio) é recusada: a libpq não tem
fragmento. Uma `LOGHUB_EB_URL` definida **só com brancos** é exit 1 (antes valia
"não definida", e o dump mostrava os brancos como se fossem uma URL); vazia
continua sendo "não definida". O `--dump-config` mostra os valores derivados
como comentário, com a senha redigida, para que o dump relido não dispare o
conflito.

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
`gls/<AgentName>@<Hostname>` (a identidade de §13.1), no `client_name`,
visível no `system.query_log`.

#### 10.10.2 Concorrência e conexões

Igual a §10.5: `sink.workers` padrão vira **4** quando o operador não
configurou, `sink.clickhouse.pool_size` padrão é `sink.workers` (abaixo de
`sink.workers` é exit 1; a sonda de saúde e a validação do boot, com o `INSERT`
vazio de sonda, têm conexão própria, fora do pool), e com `ring=none` um `ring.max_inflight` acima do pool
cria fila escondida dentro dele, com `WARN` no boot. Aqui há um motivo a mais para poucos workers: cada
`INSERT` é uma parte nova no disco do servidor, e o ClickHouse prefere poucas
partes grandes (com muitas, `252` `TOO_MANY_PARTS`, §10.10.7). Com `ring=none`
cada mensagem é um `INSERT` de uma linha; quem quiser vazão ali liga o
`async_insert` no perfil do usuário (§10.10.6).

#### 10.10.3 Escrita

Um `INSERT` por pedaço (§10.1), pelo `PrepareBatch` do driver, com os valores em colunas:

```sql
INSERT INTO `banco`.`inbox_gelf`
  (uuid, from_uuid, event_at, sid, step, level, event_nsec, space, type, payload)
SETTINGS insert_deduplication_token = 'gelf-<sha256 dos uuids do pedaço>',
         json_type_escape_dots_in_keys = 1,
         type_json_skip_duplicated_paths = 1,
         input_format_try_infer_dates = 0,
         input_format_try_infer_datetimes = 0,
         input_format_json_try_infer_numbers_from_strings = 0,
         input_format_json_read_numbers_as_strings = 1,
         input_format_json_read_objects_as_strings = 1,
         input_format_json_read_arrays_as_strings = 1,
         throw_on_max_partitions_per_insert_block = 0
```

(Os ajustes viajam no pacote do comando, não no texto.) Os valores vêm do
cabeçalho do item e da conversão, como na `loghub.inbox` (§3.7.1): `level` é a
o `level` do envelope, `sid` o da conversão (§6.10), `step` 0, `type` `gelf`. **Todas as
linhas são mapeadas antes de o comando sair**: um envelope que não mapeia (um
`from` que não é UUIDv7, só de outro ring ou dump) é `logic`, o pedaço inteiro,
sem nada enviado. O `payload` é o sub-slice do JLH do pedaço a partir de
`"payload":`, no modo string da coluna `JSON` do driver; o servidor o decompõe.
Quando o payload tem `fields` ou `extra`, vai uma **cópia** em que todo valor
desses dois objetos que não é string (número, objeto, array, `true`, `false`)
virou uma string JSON com o seu texto, byte a byte como o JLH o tem
(`chPayload`, §3.7.2); o JLH do pedaço não é tocado. A cópia é uma **reescrita
dos bytes**, não uma decodificação: o payload é o JSON compacto do próprio
conversor (§3.3), então uma varredura que pula valores basta, preserva a ordem
das chaves e custa uma alocação (~1 µs por envelope do Docker). A primeira
versão passava pelo `encoding/json` (um mapa por objeto e um novo *marshal*:
~19 µs e 84 alocações, revisão de 2026-10-31); ela ficou como oráculo de teste
(`TestChPayloadMatchesTheReference`, `FuzzChPayload`). Determinística: o mesmo
JLH dá os mesmos bytes em todo retry.
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
(as de `fields` e `extra`) são chaves de um `Map` e nunca viram caminho; os
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
   `gelf-` + os 32 primeiros dígitos hexadecimais do SHA-256 dos `uuid` do
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
`gelf_sink_clickhouse_dedup_blocks_total` o acumula (§10.10.10).

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
| `payload` é `JSON` com os dois *hints* `fields Map(String, String)` e `extra Map(String, String)`, e além deles só `max_dynamic_paths`/`max_dynamic_types` | `system.columns.type`, separado nas vírgulas de primeiro nível | exit 3 — sem os *hints*, as chaves de `fields`/`extra` viram caminhos e `_a.b` contra um `_a` objeto recusa o pedaço com `117` (§3.7.2); a tabela dos webhooks (`params`, `query`) e a `inbox_syslog` do `go-syslog-server` (`structured-data`) são recusadas por isso; um `SKIP` descartaria parte do envelope em silêncio, e um caminho tipado a mais (`size UInt8`) recusaria o pedaço que não coubesse nele |
| As dez colunas são colunas comuns (sem `default_kind`, ou `DEFAULT`) | `system.columns.default_kind` | exit 3 — uma `EPHEMERAL` aceita o `INSERT` e não guarda nada (o `payload` inteiro sumia com o `Write` devolvendo sucesso); `MATERIALIZED` e `ALIAS` recusam o `INSERT` |
| `received_at`, se existir, `DateTime64` de 6 a 9 dígitos com `DEFAULT` ou `MATERIALIZED` | `system.columns` (`default_kind`) | exit 3 — sem expressão, o ClickHouse a preencheria com o zero do tipo, 1970, e o atraso medido por ela seria mentira |
| Nenhuma `CONSTRAINT … CHECK`, com o nome nu ou entre crases | `system.tables.create_table_query` | exit 3 — §3.7.2: a sonda vazia não a exercita, e uma violação seria `logic` em todo pedaço |
| `max_memory_usage` (quando não é `0`) comporta o pior pedaço: 128 MiB + 8 × `max(sink.batch_max_bytes, pior JLH)` | `getSetting(...)` na sessão deste servidor | exit 3, com os números na mensagem — medido no 25.8 (no `go-webhook-server`): o `INSERT` de um bloco de `payload` JSON consome ~6× os bytes dele mais ~130 MiB fixos, e `241` é fatal (§10.10.7); sem a conta, um pedaço legítimo derrubaria a frota |
| `max_memory_usage_for_user` (quando não é `0`) comporta **escritores** vezes isso: `sink.workers` com ring, `min(pool_size, ring.max_inflight)` com `ring=none` | `getSetting(...)` | exit 3 — o limite do usuário soma todas as consultas dele: quatro pedaços legítimos e simultâneos estouravam um limite que a comparação com um só aceitava, e `241` é fatal. Outras instâncias no mesmo usuário somam por cima, e nenhuma as enxerga: dê a cada uma o seu usuário. O multiplicador era `pool_size`, que contava a conexão da sonda — um `SELECT`, nunca um pedaço — como mais um `INSERT`, e a mensagem mandava "baixar o pool" quando o pool não pode ficar abaixo dos workers; a conta é dos escritores, passados pela app, e a mensagem manda baixar `sink.workers` (revisão independente do porte, 2026-10-13) |
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
com outro prazo, passava. Um cancelamento do boot (`SIGTERM`) ainda a
interrompe, como em §10.5 (pendência 6 de 2026-10-11): o prazo era um
`WithoutCancel` puro, e um `SIGTERM` esperava o `op_timeout` inteiro. Uma
tabela que alimenta *materialized views* gera um
`WARN`: uma recusa da view falha o `INSERT` depois de as linhas estarem gravadas,
e elas são contadas como descartadas.

A validação **não se repete**. Uma tabela dropada depois do boot aparece como
`60` na escrita, que é fatal (§10.10.7).

#### 10.10.6 Durabilidade e `async_insert`: decisão do administrador do ClickHouse

Este servidor **não envia nem lê** `async_insert`,
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
JSON válido, e com os dois ajustes de `JSON` de §10.10.3, os *hints* `Map` e a
citação dos valores de `fields`/`extra` (`chPayload`) nenhuma chave ou número
de emissor o provoca: na prática ele só vem de dano.

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
gelf_sink_clickhouse_dedup_blocks_total      counter  blocos descartados pelo insert_deduplication_token
gelf_sink_clickhouse_pool_conns{state}       gauge    state: acquired|idle|max
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

Como S §11 (decisão 8 de S). O servidor é um driver de si mesmo (não existe `go-loghub-driver`),
então emite o heartbeat que a skill exige de todo worker, e **só** ele.

- **Só com `sink=nats`** (nos dois modos). Com outro sink, nada é emitido e o boot
  registra `INFO`.
- **Quando:** na primeira conexão, em toda reconexão (o `ReconnectHandler` do
  `nats.go`) e a cada `loghub.beacon_interval` (env `LOGHUB_BEACON_INTERVAL`, em
  segundos; padrão 15; `0` desliga o periódico, não o de conexão). Fora o `0`, o
  mínimo é `1s`: `1ns` ou `15ms` são exit 1, qualquer que seja o sink, porque
  inundariam `<ctl_namespace>.beacon` e disputariam a conexão dos dados
  (pendência 21 de 2026-10-11). O beacon impõe o mesmo piso sozinho, com
  `WARN` (`control.MinInterval`), para quem o monte sem passar pela config.
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

- Falha de publicação é contada (`gelf_beacons_total{result="error"}`) e logada
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

**Publicação atômica** (pendência 23 de 2026-10-11, corrigida como no R): o
dump é escrito sob o nome oculto `.emg-<agent_uuid>-<unixnano>.bin.tmp`
(`O_CREATE|O_EXCL`), com `fsync`, publicado por **`link`** no nome final (com
`rename`, depois de conferir o nome livre, quando o sistema de arquivos não tem
hard link — senão todo dump falharia ali; um `link` recusado porque o nome
existe nunca cai no `rename`) e com `fsync` do diretório. Falha em qualquer
passo, inclusive no `fsync` do **diretório** (ou na abertura do diretório para
ele), remove o que foi escrito e é falha do dump (§12.3): sem ela a entrada do
diretório pode não sobreviver a uma queda de energia e o processo sairia dizendo
que salvou. Escrever direto no nome final deixava, num processo morto no meio
(`SIGKILL`, OOM, aborto), um arquivo parcial com cara de pronto, que o boot
entregava pela metade e removia sem contar o resto; num diretório compartilhado,
outra instância o pegava enquanto era escrito.

**Dump abandonado.** Um dump cujas mensagens um aborto já contou como perdidas
(§13.4) desiste: o `Write` confere o aborto no início, a cada 1024 registros e
antes da publicação, e remove o temporário em vez de publicar um arquivo que o
boot seguinte reingeriria. Um aborto que chega depois da última conferência
deixa o dump publicado e contado — sobreconta, nunca perda: o boot seguinte o
entrega, e `reason="abort"` sobreconta. É regra, não pendência (porte do RP/RN,
decidido em 2026-10-14): o aborto **não espera o disco** — segurar o mutex da
contagem durante o `link` e o `fsync` do diretório faria um `SIGQUIT`, cujo
contrato é "agora", esperar um disco que pode estar preso; a janela é inerente a
qualquer `os.Exit` assíncrono a uma escrita.

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
O prazo é por pedaço entregue: cada lote de `sink.batch` tem seu próprio
`sink.boot_timeout` **sem progresso**, e uma reingestão longa e sadia ainda
retenta uma falha transitória (regra do SP/RN, que aqui já valia por
construção).

**Temporários interrompidos** (pendência 23 de 2026-10-11). Antes da
reingestão, o temporário gêmeo de um dump completo (o mesmo arquivo, um corte
entre o `link` e a remoção) é removido, e todo outro temporário é listado num
`WARN`, qualquer que seja o sink: contado como perda só se um segundo sinal
abortou o dump; depois de `SIGKILL`, OOM ou queda de energia ninguém o contou, e
ele pode estar completo. Nunca é lido nem apagado; renomeado pelo operador para
o nome final, os registros íntegros entram no boot seguinte e o resto vai para
quarentena.

**`sink=none` não reingere** (pendência 26 de 2026-10-11, "a decidir pelo
usuário"; decisão portada do R, DECISOES): o descarte por configuração
destruiria mensagens que uma vida com destino real aceitou. Os arquivos ficam no
disco, nomeados num `WARN` e contados nos gauges, à espera de um boot com
destino.

**Gauges recontados** (pendência 25 de 2026-10-11): `gelf_emergency_files`
recebe o número de arquivos listados antes da primeira entrega;
`gelf_emergency_events_pending` não é tocado durante a reingestão (o tamanho
de um arquivo não diz nem o que está no disco nem o que falta). Ao fim, os dois
são recontados do diretório, nunca zerados: um dump que ficou (ilegível, remoção
ou quarentena que falharam) continua contado — o ilegível como arquivo, sem
mensagens —, e o alerta sobre eles continua disparando. O dump escrito no
shutdown **soma** aos gauges em vez de fixá-los.

**Remoção durável.** Depois de entregue, o dump é removido e o diretório recebe
`fsync`: sem ele, uma queda de energia logo depois devolveria o nome, e o boot
seguinte entregaria o dump inteiro de novo (com os mesmos `uuid`, deduplicados só
onde o destino deduplica). Um `fsync` (ou a abertura do diretório para ele) que
falha depois do `unlink` é `WARN` ("may reappear"): o arquivo já saiu, e "cannot
remove" seria falso. Uma falha da própria remoção é `ERROR` ("cannot remove a
reingested dump"), e o arquivo volta no boot seguinte. É regra, não pendência (porte do RP/RN, decidido em 2026-10-14): sair com erro deixaria o boot em laço por um problema (permissão do diretório) que não impede a entrega, e os gauges de §14.1, recontados do disco no fim da reingestão, continuam contando o arquivo, então o alerta segue disparando (`TestQuarantineAndRemovalReportTheirOutcome`).

Arquivo truncado ou corrompido: processado até o último registro íntegro, o resto
logado em `ERROR` com a contagem de bytes, e o arquivo renomeado para
`.corrupt`. Se o `rename` aconteceu e só o `fsync` do diretório falhou, o arquivo
**está** quarentenado (pendência 24 de 2026-10-11): o `fsync` vai em `WARN` e o
`ERROR` com os bytes descartados sai igual, com o nome novo; só sem nome
devolvido é "cannot quarantine". Um registro de cabeçalho indecodificável é pulado e também torna o
arquivo danificado (quarentena depois da entrega, nunca remoção). A linha de
comprimentos tem no máximo 32 bytes. O decodificador **nunca confia no comprimento declarado** e tolera
truncamento sem pânico (`FuzzRead`).

### 12.3 Falha do próprio dump

Disco cheio, sem permissão, `emergency.enabled = false`: `ERROR` com a contagem
exata, `gelf_events_dropped_total{reason="emergency_failed"} += n`, exit 3.
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
    recusa => exit 3 IMEDIATO, com a linha "the sink refused this server
    permanently" (classe `fatal`, sink.IsFatal), nunca a do boot_timeout,
    que mandaria o operador olhar a rede (pendência 27 de 2026-10-11).
    Cada checagem leva o contexto do boot como "tempo de vida"
    (withLifetime): uma validação que passa do sink.health_timeout ainda vê
    o SIGTERM
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
12. abre os transportes: udp, tcp, tls, http, https
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

**Bind cancelado é 130.** O bind da porta admin (passo 5) e o dos transportes
(passo 12) usam o contexto dos sinais: com um nome de host num `listen.*` ou no
`admin.bind`, um sinal no meio faz a resolução falhar com "operation was
canceled". Isso é um boot interrompido, exit 130, não o exit 1 de um endereço
inválido (revisão independente do RN, 2026-10-13). Um literal IP não consulta
o contexto e o bind dá certo depois do sinal: por isso,
depois de **todos** os binds e antes de qualquer transporte começar a ler, o
contexto dos sinais é conferido de novo, e um sinal dos passos 9–12 sai com
130 e fecha o que já foi aberto. Sem essa conferência o servidor abria,
respondia `/readyz` 200 e saía com 0 pelo shutdown gracioso (revisão
independente do porte, 2026-10-13).

### 13.2 Validação de configuração

| Combinação | Efeito |
|---|---|
| Nenhum dos cinco transportes (`listen.udp`, `listen.tcp`, `listen.tls`, `listen.http`, `listen.https` todos vazios) | exit 1 |
| Dois dos quatro listeners TCP (`listen.tcp`, `listen.tls`, `listen.http`, `listen.https`) no mesmo endereço, comparados dois a dois | exit 1, nomeando as duas chaves; com a mesma porta, um **nome** de host de qualquer lado conta como o mesmo endereço (`localhost` e `127.0.0.1`): para dividir a porta, dois literais IP distintos. O `SO_REUSEPORT` (`*_listeners > 1`) aceitaria o bind duplo e entregaria conexões em claro ao leitor TLS, ou quadros GELF ao servidor HTTP. `listen.udp` pode dividir a porta com qualquer um deles (outro protocolo; `12201` no UDP e no TCP é o padrão) |
| `listen.udp`, `listen.tcp`, `listen.tls`, `listen.http` ou `listen.https` fora de `host:port` | exit 1 |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| `admin.enabled = true` com `admin.bind` no mesmo endereço TCP de `listen.tcp`, `listen.tls`, `listen.http` ou `listen.https`: a mesma porta com o mesmo literal IP (o IPv4 mapeado em IPv6 conta como o IPv4), um curinga (`""` ou um literal não especificado: `0.0.0.0`, `::`, `::0`) de um lado, ou um **nome** de host de qualquer lado | exit 1, nomeando os dois endereços. A regra é a da linha anterior (`sameStreamSocket`), estendida à porta admin: nenhum nome é resolvido na validação; para dividir a porta, dois literais IP distintos; a porta `0` nunca colide. `listen.udp` não colide (outro protocolo). Antes a colisão só aparecia ao abrir o transporte (passo 12), com a porta admin já tendo servido o boot inteiro, e no macOS/BSD nem isso: o `SO_REUSEADDR` deixa `127.0.0.1:P` e `*:P` coexistirem, e a porta admin tomava em silêncio as conexões de `localhost` (porte do RP/RN, 2026-10-14; `TestAdminBindCollidesWithListen`). O curinga é reconhecido pelo **valor**, não pela grafia: `""` ou qualquer literal com `IsUnspecified()` (`0.0.0.0`, `::`, `::0`, `0:0::0`); e a **porta é comparada como número**, lida como o `net.Listen` a lê (`net.LookupPort`): `0514` é `514`, e um nome de serviço (`http`, `https`) vale o número do banco de serviços; uma porta que não é número nem serviço conhecido é exit 1 aqui, nomeando as duas chaves, em vez de falhar no bind (revisão independente de 2026-10-14, D1 do `go-syslog-server` e D4 do RELP: `[::0]:P` contra `127.0.0.1:P` e `0514` contra `514` passavam como endereços distintos; `TestSameStreamSocketReadsValuesNotSpellings`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `ring.sqlite.path`, `sink.sqlite.path` ou `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `listen.udp_reassembly_max_bytes` fora de 1 MiB–4 GiB; `listen.udp_reassembly_max_messages` fora de 1–1 048 576 | exit 1 (§4.2: abaixo, nem um punhado de documentos fragmentados máximos cabe; acima, não é teto) |
| `listen.tls` **ou** `listen.https` (basta um deles) sem `tls.cert_file`/`key_file`, com arquivos ilegíveis, certificado ou chave que não fazem parse, chave de outro certificado, ou `tls.client_ca` sem nenhum certificado PEM | exit 1, no passo 1 (antes da espera pelo sink: com um sink inalcançável o erro do TLS ficaria escondido atrás de um exit 3) |
| `tls.client_auth = require` sem `tls.client_ca` | exit 1 |
| `proxy.protocol ≠ off` sem `proxy.trusted` válido | exit 1 |
| `space` fora de `^[a-z0-9-]+(\.[a-z0-9-]+)*$` | exit 1 (a lib aceita `a..b`, `.x` e `x.`; o subject não). Acima de 64 caracteres a própria lib já sai com 111 antes |
| `space` igual a um prefixo do plano de controle (o primeiro token começa com `_`) | impossível: `_` não é permitido em `space` |
| `convert.keep_raw` fora de `malformed`, `always` | exit 1 |
| `convert.max_message_bytes` < 1 KiB ou > 16 MiB | exit 1 (o pior JLH, 6 × L + 8 KiB, §3.9, é conferido contra o `max_payload` do NATS e o `max_allowed_packet` do MySQL no boot) |
| `http.read_header_timeout`, `http.read_timeout` ou `http.idle_timeout` zero; `http.max_conns`, `http.max_conns_per_ip` ou `http.max_requests_per_conn` negativo; `http.bulk_max_bytes` fora de 1 KiB..64 MiB (conferido mesmo com `http.bulk=false`) | exit 1 (os prazos são a defesa contra slowloris; `0` nas conexões e no teto de requisições é "sem limite") |
| `ring.max_bytes` abaixo do custo de uma mensagem do tamanho máximo, na medida do ring **efetivo** (§9.2, o pedido ou o que `ring=redis` vira ao degradar, §9.7): com `memory`, a classe do pool de `convert.max_message_bytes` + 384 B (`item.PooledCap(n) + ring.ItemOverhead`, §15.1; 131 456 B com o padrão de 128 KiB, acima da maior classe, em que o buffer é o próprio documento); com `sqlite` e `redis`, o próprio `convert.max_message_bytes` (eles contam `len(cru)`). Com `none` a chave não é usada e só precisa ser positiva | exit 1, nomeando a chave e o piso: essa mensagem nunca caberia num ring vazio; no TCP/TLS a conexão ficaria parada até o `tcp.backpressure_timeout` e a mensagem seria descartada, no HTTP respondida sempre com `503`, e no UDP descartada sempre como `ring_full`, com o ring vazio. Porte do RP/RN, 2026-10-14 (`TestRingMaxBytesFitsTheLargestMessage`) |
| `ring=sqlite` + `sink=sqlite` no mesmo arquivo | exit 1 |
| combinações de §9.7 que degradam | `WARN` |
| `sink=sqlite` com `workers > 1` | `WARN`, forçado a 1 |
| `sink=postgres`/`mysql`/`clickhouse` com `workers` no padrão | rebaixado para 4, `WARN`; `WARN` acima de 16 |
| `pool_size` abaixo de `workers` | exit 1 (cada worker segura uma conexão enquanto escreve; a sonda de saúde tem a sua, fora do pool, §10.5) |
| `sink.mysql.database` ou `sink.clickhouse.database` com crase, barra invertida, byte de controle (< 0x20 ou 0x7F) ou mais de 64 **caracteres** (o limite do MySQL é de caracteres: 64 `ç` são um nome válido de 128 bytes, e eram recusados; revisão independente do porte, 2026-10-13. O do ClickHouse é o nome codificado em `%XX` como nome de arquivo, 255 bytes: 64 caracteres nunca recusam um nome que ele aceite, salvo ASCII entre 65 e ~240) | exit 1: o nome entra cru entre crases no `INSERT` e na sonda; uma crase quebraria o comando e, no MySQL, o `1064` seria `network` até o `boot_timeout` (pendência 12 de 2026-10-11, §10.6) |
| **tetos** (revisão independente do RP/RN, 2026-10-13, portada; ajustados pela revisão independente do porte, 2026-10-13): `ring.capacity_items` > 16 777 216 com o ring **efetivo** `memory` (o pedido e o que `ring=redis` vira ao degradar, §9.7), > 2⁴⁰ com `sqlite` e `redis` (teto de sanidade: o `ring.db` conta linhas num `int64` e o script do Redis compara `double`s; 50 milhões de linhas são configuração legítima e eram recusadas); `ring.max_inflight` > 1 048 576 (é termo do `MaxPending` do NATS, `2 × (workers × batch + max_inflight)`, que dava a volta para negativo e o `NewNATS` trocava por 4096 em silêncio); `ring.sqlite.commit_batch` > 65 536; `sink.batch` > 1 048 576 (um lote de 100 000 linhas no ClickHouse é legítimo; o MySQL mantém o seu teto de 5 041); `sink.batch_max_bytes` > 256 MiB; `sink.workers` > 1024; `sink.postgres`/`mysql`/`clickhouse.pool_size` > 4096; `listen.udp_listeners`, `tcp_listeners`, `tls_listeners` fora de 1–256 | exit 1, nomeando a chave. Sem eles um erro de digitação de poucos dígitos derrubava o boot com pânico (`makechan` de `capacity_items` e de `commit_batch`), alocava um gigabyte por `Dequeue` (`sink.batch`), dava a volta no `int32` do pool do PostgreSQL (`pool_size = 4294967300` virava 4) ou montava um pedaço cujo `Bind` passa do limite de 1 GiB da mensagem da libpq e falha sem SQLSTATE: `network`, retentado para sempre (`batch_max_bytes`). MySQL e ClickHouse levam o teto do PostgreSQL: milhares de conexões por instância são erro de digitação, nunca orçamento |
| `sink=postgres` com `addr` que não é um servidor só (§10.5) | exit 1 |
| `sink.postgres.tls` ou `sink.mysql.tls` = `true` com `addr` de socket unix (começa com `/`) | exit 1: TLS é recurso do TCP (§10.9) |
| `loghub.beacon_interval` entre `0` e `1s` (exclusive) | exit 1, qualquer que seja o sink (§11) |
| `sink=mysql` com `sink.batch > 5041` | exit 1 (65 535 marcadores por comando, 13 por linha de `gelf_events`) |
| `sink=clickhouse` sem `addrs`, com endereço que não é `host:porta`, `table` que não é identificador simples ou `pool_size` negativo | exit 1 |
| `ring=redis` com `ring.redis.claim_idle ≤ 2 × sink.op_timeout` | exit 1: uma escrita mais longa que a janela deixaria outra réplica reivindicar um lote vivo (§9.5) |
| uma entrada de `sink.nats.urls` que não parseia, sem host, de esquema fora de `nats`/`tls`/`ws`/`wss`, ou com caminho, consulta ou fragmento em `nats://`/`tls://` (`host:porta` sem esquema vale como `nats://`) | exit 1, sem citar a entrada (§16.2): uma senha com `/`, `#` ou `?` sem escape virava um host errado ou um erro do cliente que ecoava a URL a cada retentativa (pendência 13 de 2026-10-11) |
| credencial na URL do NATS junto de `username`/`password`/`token`/`credentials` | exit 1: "uma forma só"; o `nats.go` usaria a da URL e ignoraria a chave em silêncio (pendência 13) |
| `sink=nats` sem `urls`; mais de um método de autenticação; `password` sem `username` | exit 1 |
| `sink.nats.mode = core` | `WARN`: best effort |
| `LOGHUB_EB_URL` em conflito (§10.9) ou com esquema não suportado; só com brancos; PostgreSQL sem usuário ou sem banco, com parâmetro desconhecido, repetido, `sslmode` vazio ou `host=` vazio, com fragmento, com `host=` que não é um diretório só, com host codificado, com TLS sobre o socket; Redis com `,` no host; `@` depois de `/`, `?` ou `#` em qualquer esquema | exit 1, **sem citar a URL** (§10.9) |
| `--help`, `--version` ou `--dump-config` com `=valor`; `--config=` vazio | exit 1, sem o valor (revisão independente do RP/RN, 2026-10-13, portada): `--dump-config=false` despejava e saía com 0, e um `--config=` vazio caía no `GELF_CONFIG`, o contrário do que diz |
| linha do INI que não é `[seção]` nem `chave = valor`; argumento da linha de comando que não começa com `-`; nome de parâmetro desconhecido sem forma de nome, ou que começa com o nome de um parâmetro e continua; chave desconhecida do INI fora de uma seção deste servidor, sem forma de chave ou logo depois de um segredo | exit 1, nomeando o número da linha ou só o que vem antes do `=` do argumento, **nunca o conteúdo** (§16.2; pendência 30 de 2026-10-11) |
| `tls_ca` sem `tls` em qualquer cliente | exit 1 |
| `filter.min_level` ou uma entrada de `filter.levels` fora de 0–7 ou nome desconhecido (só `emerg` `alert` `crit` `err` `warning` `notice` `info` `debug`, sem diferença de caixa; `warn` e `error` são recusados) | exit 1 no boot; num `SIGHUP`, recarga recusada e o filtro em execução mantido. A validação usa o `filter.ParseLevel` do próprio filtro: o que ela aceita, ele compila |
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
- **FATAL é terminal.** Toda transição e o gauge `gelf_sink_up` são escritos
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
fase 3  cancela o contexto de recepção e fecha os transportes TODOS DE UMA VEZ
        (udp, tcp, tls, http, https em paralelo); os documentos já lidos
        terminam de entrar no ring
fase 4  drena o ring para o sink, com shutdown.drain_timeout (30 s), incluindo
        Workers.Pending()
fase 5  para o beacon, fecha o sink, o ring e o SQLite, exit 0
```

- O contexto de recepção é cancelado **antes** de os transportes fecharem: é o
  que solta um leitor preso no laço de backpressure. Um item já entregue ao ring
  não é afetado: o `Enqueue` do sqlite espera o group commit, e o `ring=none`
  escreve num contexto desligado.
- **Nenhum `Enqueue` depois do `Drain`.** O `Close` de cada transporte só volta
  quando os leitores dele voltaram (UDP espera as goroutines; TCP e TLS
  esperam as conexões e, depois de 1 s (`closeGrace`), fecham o socket de quem
  não voltou; HTTP e HTTPS chamam `http.Server.Shutdown` com o mesmo prazo de
  1 s e, estourado, `Close`). Uma requisição HTTP em voo durante a fase 3 vê o
  contexto de recepção cancelado e recebe **`503`** com `Retry-After: 1`, nunca
  `202`: o cliente reenvia a outra instância ou depois do restart. O que já
  recebeu `202` está no ring.
- **A remontagem UDP é abandonada.** Mensagens chunked ainda incompletas na
  fase 3 são descartadas com o estado de remontagem (o emissor não tem como
  completá-las numa instância que fechou o socket); o relógio de varredura
  (§5.2) para junto com o transporte.
  Por isso o `Drain` da fase 4 vê tudo o que entrou no ring: um item que
  entrasse depois dele ficaria num ring que ninguém mais lê, sem entrega, sem
  dump e sem contador.
- **Os transportes fecham juntos.** Um depois do outro, as conexões TCP
  continuavam lendo — e aceitando — enquanto as do TLS fechavam, e a fase 3
  podia durar um prazo de fechamento (1 s) por transporte de conexão em vez do
  único para o qual o `stop_grace_period` é dimensionado (revisão independente
  do R, 2026-10-11).
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
  fora do shutdown: no boot e com o servidor no ar, saída imediata com 130. Um
  `SIGQUIT` que chega depois de o shutdown começar chega por um canal
  (`quitNow`), não por uma flag lida uma vez na entrada.
- **Shutdown por FATAL tolera o primeiro sinal** (pendência 28 de 2026-10-11).
  Esse shutdown começa sem sinal, então o primeiro `SIGTERM`/`SIGINT` que chega
  nele (um `docker stop`, um deploy, no meio do `workers.Wait` ou do dump) não é
  o "segundo sinal": é logado em `WARN` e o shutdown segue até o dump e o exit 3
  de §13.3; o seguinte aborta. `SIGQUIT` nunca é tolerado.
- **Um dono só para a contagem do aborto** (pendência 29 de 2026-10-11; posse
  final do RN, revisão independente de 2026-10-13). Com `ring=memory`/`none`,
  depois do `workers.Wait` e **antes** do `Drain`, a perda passa da contagem do
  ring para um contador do que vai ao dump (fase do dump): o `Drain` tira os
  itens da contabilidade do ring, e um aborto durante o dump contaria só o lote
  retido pelos workers. A passagem (`enterDumpPhase`), o ajuste ao tamanho exato
  do que vai ao dump (`oweDump`) e o aborto são ordenados por um mutex: o aborto
  cai inteiro antes (conta o ring, e a fase do dump não começa — o shutdown sai
  sem despejar) ou inteiro depois (toma o contador). O contador é tomado com
  `Swap` por um lado só — o aborto (`reason="abort"`) ou o dump que falhou ou
  está desligado (`reason="emergency_failed"`) —, e nada é contado depois do
  dono (contador já zerado): nunca 2N. O aborto conta uma vez só, e um dump
  ainda sendo escrito desiste (§12.1, "Dump abandonado"). O dump também toma o
  contador **sob o mesmo mutex**, nos dois ramos (dump que falhou ou desistiu,
  e dump desligado): o aborto publica a marca de abortado antes de tomar o
  contador, e um dump que via a marca nesse intervalo desistia e contava tudo
  como `emergency_failed`, com o aborto logando que nada se perdeu — uma
  contagem só, com o motivo errado (revisão independente do porte,
  2026-10-13).
- **O aborto fecha o ring antes de contar** (porte do RP/RN, 2026-10-14). Com
  `ring=memory`/`none`, fora da fase de dump, o `abortLoss` chama
  `ring.Close()` **antes** de ler a ocupação: um `Enqueue` depois disso é
  recusado com `ring.ErrClosed`, que a recepção trata como shutdown (TCP/TLS
  fecham a conexão sem ler adiante; HTTP/HTTPS respondem `503`; UDP
  descarta), contado em
  `gelf_messages_dropped_total{reason="shutdown"}`, e nenhuma mensagem é
  aceita depois da contagem sem ser contada. O `Enqueue` do ring em memória
  reconfere o fechamento **depois** de reservar a vaga (`items.Add(1)`): ou a
  reserva é vista pela leitura do aborto (contada), ou o `Enqueue` vê o
  fechamento e desiste — no máximo uma sobrecontagem transitória, nunca uma
  mensagem aceita sem contagem. A sobrecontagem tem uma segunda fonte (registrado em 2026-10-14, D3 da revisão do `go-syslog-server`): um `Enqueue` que reservou a vaga (`items.Add(1)`) e vai desistir por falta de bytes (`ErrFull`) pode ser lido pelo aborto entre a reserva e o recuo, e conta em `abort` uma mensagem que a recepção trata como ring cheio. É transitória, de no máximo uma por recepção concorrente, e nunca esconde uma perda. A janela não era só o `pre_stop_delay`: um
  `SIGQUIT` em operação normal tinha a mesma, com todos os transportes abertos.
  O `ring=none` faz o mesmo depois de `cur.Add(1)`, que é a ocupação que o
  aborto lê: fechado, desfaz a reserva e o semáforo e devolve `ErrClosed`
  (revisão independente de 2026-10-14, A2: só o `memory` reconferia, e uma
  linha TCP já lida que passava do primeiro `closed` antes do `Close` era
  escrita depois da contagem, e o `os.Exit` a perdia sem conta;
  `TestNoneEnqueueSeesACloseThatRacedTheReservation`). Uma escrita síncrona já
  contada pode ainda terminar no destino antes do `os.Exit`: entregue e contada
  em `abort`, uma sobrecontagem, nunca uma perda.
  Com `ring=sqlite`/`redis` o aborto não conta o ring (o que sobrou continua
  lá) e não o fecha: o `Close` deles espera o *group commit* e o cliente, e o
  aborto é "agora" (`TestAbortClosesTheRingBeforeCounting`,
  `TestEnqueueSeesACloseThatRacedTheReservation`).
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
- `shutdown.pre_stop_delay` existe para Kubernetes (5–10 s sugeridos): um emissor
  UDP não recebe erro quando o socket some, e um emissor HTTP que pegou a
  instância saindo recebe `503`. O `stop_grace_period` do orquestrador
  precisa ser **maior** que `pre_stop_delay + drain_timeout`.

### 13.5 Códigos de saída

| Código | Significado |
|---|---|
| `0` | shutdown completo: ring vazio, ou `sqlite`/`redis` com o resto guardado |
| `1` | erro de configuração ou de inicialização não relacionada a destino |
| `3` | destino indisponível: sink FATAL, `boot_timeout` ou `drain_timeout` estourados, ring Redis indisponível no boot, maior evento que não cabe, guarda de não-progresso do dreno |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` (`DATADIR`, `MACHINE_ID`, `MACHINE_ID_FILE`, `AGENT_NAME`, `AGENT_UUID`, `HOSTNAME`, `WORKSPACE`; 112 = `Initialize()` duas vezes); a tabela completa está na skill `go-loghub-ident` e em `docker/README.md` |
| `130` | abortado: segundo sinal no shutdown (o primeiro, num shutdown por FATAL), `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 5–12 do boot (um bind cancelado inclusive, §13.1) |

---

## 14. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9104` (decisão 6: o `go-webhook-server` usa
`9101`, o `go-syslog-server` `9102` e o `go-relplog-server` `9103`; todos rodam
lado a lado). HTTP/1.1.

O `admin.bind` nunca divide o endereço TCP do `listen.tcp`, do `listen.tls`, do
`listen.http` nem do `listen.https` (§13.2): mesma porta com o mesmo literal, um
curinga ou um nome de qualquer lado é exit 1 na validação.

| Path | Semântica |
|---|---|
| `/healthz` | `200` enquanto o processo vive. Liveness. |
| `/readyz` | `200` só se: sink `UP`, ring abaixo de `ring.high_water`, backlog de boot zerado, ring Redis alcançável (com `ring=redis`), porta pública aberta e não em shutdown. Senão `503` com o motivo em `text/plain`. |
| `/metrics` | exposição Prometheus em texto. |

### 14.1 Métricas

Prefixo `gelf_`. **Nenhum label deriva de conteúdo controlado pelo emissor**
(nada de `host`, nome de campo, IP); todo label tem conjunto finito conhecido em
compilação (`internal/obs/metrics.go`). O `level` **não** é label: a
distribuição por nível é pergunta para o destino, onde é coluna indexada.

| Métrica | Tipo | Labels |
|---|---|---|
| `gelf_messages_total` | counter | `transport` (udp, tcp, tls, http, https): documentos recebidos do fio, antes de qualquer filtro. No UDP com chunks, um por mensagem remontada (ou descartada como mensagem), não por chunk |
| `gelf_messages_accepted_total` | counter | `transport` |
| `gelf_messages_dropped_total` | counter | `reason` (ring_full, ring_unavailable, sink_down, filter, ratelimit, malformed_frame, proxy, empty, shutdown, too_large, decompress, chunk_invalid, chunk_timeout, chunk_overflow). No UDP é o **único** registro de que algo se perdeu |
| `gelf_message_bytes` | histogram | — (o documento **descomprimido**, `size`) |
| `gelf_confirm_duration_seconds` | histogram | — (do documento completo à garantia do ring; tcp, tls, http e https, só aceitos: o UDP não tem o que confirmar) |
| `gelf_convert_total` | counter | `format` (gelf, malformed) |
| `gelf_convert_warnings_total` | counter | `code` (os 25 de §6.8) |
| `gelf_convert_duration_seconds` | histogram | — (a conversão de um documento no worker de dreno) |
| `gelf_conns_active`, `gelf_conns_total` | gauge, counter | `transport` (tcp, tls, http, https) |
| `gelf_conns_refused_total` | counter | `transport` (tcp, tls, http, https), `limit` (global, per_ip); `global` conta cada vez que o accept estaciona no teto, um limite inferior, porque o teto global bloqueia em vez de recusar |
| `gelf_tls_handshake_errors_total` | counter | — (do transporte `tls`; as falhas de handshake do `https` não são contadas, §22.1) |
| `gelf_proxy_headers_total` | counter | `result` (v1, v2, local, absent, invalid) |
| `gelf_backpressure_seconds_total` | counter | `transport` (tcp, tls): o HTTP não segura, responde `503` |
| `gelf_udp_receive_errors_total` | counter | — (erros do próprio socket; o descarte do kernel por `SO_RCVBUF` pequeno não aparece aqui) |
| `gelf_udp_read_buffer_bytes` | gauge | — (o `SO_RCVBUF` efetivo, lido de volta) |
| `gelf_udp_reassembly_bytes` | gauge | — (bytes segurados por mensagens chunked incompletas, contra `listen.udp_reassembly_max_bytes`, §5.2) |
| `gelf_udp_chunk_duplicates_total` | counter | — (chunks ignorados porque o `seq` já tinha chegado) |
| `gelf_ring_items`, `gelf_ring_bytes` | gauge | — (global com `ring=redis`) |
| `gelf_ring_capacity_items`, `gelf_ring_capacity_bytes` | gauge | — |
| `gelf_ring_quarantined_total` | counter | — |
| `gelf_ring_redis_claimed_total` | counter | — (entradas reivindicadas por `XAUTOCLAIM`) |
| `gelf_sink_batch_size`, `gelf_sink_batch_bytes` | histogram | — (por pedaço) |
| `gelf_sink_duration_seconds` | histogram | — |
| `gelf_sink_health_duration_seconds` | histogram | — |
| `gelf_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); o cancelamento do shutdown não conta. `fatal` conta **toda** recusa fatal do destino, mesmo durante um cancelamento (boot cancelado, shutdown): ela não nasce dele, é um veredito real, e o processo sai logo depois. Só o `network` cortado pelo cancelamento deixa de contar, porque o cancelamento **causa** erros de rede (prazo, conexão fechada). Regra, não pendência (porte do RP/RN, decidido em 2026-10-14). Vale também para o cancelamento do boot na reingestão e no backlog (`Instrumented.Deliver`): o `network` cortado por ele não conta, como nos workers e no SP/SN (achado do porte do RP/RN, 2026-10-14: o `writeChunk` contava antes de olhar o contexto; `TestDeliverCancellationIsNotANetworkError`). |
| `gelf_sink_up` | gauge | — |
| `gelf_sink_sqlite_file_bytes`, `_rotations_total`, `_files_removed_total` | gauge, counter | — |
| `gelf_sink_postgres_duplicates_total`, `gelf_sink_mysql_duplicates_total`, `gelf_sink_nats_duplicates_total` | counter | — |
| `gelf_sink_postgres_pool_conns`, `gelf_sink_mysql_pool_conns` | gauge | `state` (acquired, idle, max) |
| `gelf_sink_postgres_pool_waits_total`, `gelf_sink_mysql_pool_waits_total` | counter | — |
| `gelf_sink_nats_reconnects_total` | counter | — |
| `gelf_sink_clickhouse_dedup_blocks_total` | counter | — (blocos descartados pelo token, §10.10.4) |
| `gelf_sink_clickhouse_pool_conns` | gauge | `state` (acquired, idle, max) |
| `gelf_events_written_total` | counter | — |
| `gelf_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero, como todo label de conjunto fechado (um `rate()` precisa da série antes do primeiro evento) |
| `gelf_emergency_files`, `gelf_emergency_events_pending` | gauge | — |
| `gelf_beacons_total` | counter | `result` (ok, error) |
| `gelf_ratelimit_buckets` | gauge | — |
| `gelf_build_info` | gauge | `version`, `commit`, `go` |
| `gelf_config_info` | gauge | `ring_mode`, `sink_type`, `transports` |
| `gelf_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

As séries de todo destino são expostas qualquer que seja o `sink.type`, em zero
quando o destino é outro: um painel serve a toda instância. Cada label tem teste
de exposição.

**Fronteiras de histograma** são estáticas, idênticas em toda instância e
**pré-formatadas como string** na inicialização: emitir `0.01` num scrape e
`0.010` no seguinte cria duas séries e quebra todo quantil.

### 14.2 Logs

`slog`, `json` por padrão (`text` disponível). **Nenhum log síncrono por mensagem
no caminho padrão.** `log.access = true` liga log por mensagem com
`log.access_sample` (0.01); num coletor de log isso é logar log. Avisos de anomalia
(conversão, PROXY, TLS, beacon) são **amostrados**, um a cada 30 s por tipo, com
o contador exato em `/metrics`: um emissor quebrado não inunda o log do coletor.
Nenhum log em `INFO` ou acima carrega o payload de uma mensagem.

---

## 15. Memória e runtime

### 15.1 O documento cru

Buffers de pools por classe: 2 KiB, 8 KiB e 64 KiB. Acima de 64 KiB o buffer é
alocado direto e **nunca** volta a um pool: pooling de pior caso faz o pool
segurar o pior caso para sempre. Com o padrão de 128 KiB de
`convert.max_message_bytes` (§5.5) um documento maior que 64 KiB existe (um
stack trace em `full_message`), mas é raro: o documento GELF típico de uma
biblioteca ou do driver do Docker tem de 300 bytes a 2 KiB.

**Memória por conexão, fora do ring.** Uma conexão TCP ou TLS viva segura o
leitor de 4 KiB e o buffer de quadro (até 8 KiB quando ociosa; o de um quadro
maior enquanto os quadros seguintes já estão no leitor, ou com alguns bytes do
próximo quadro no leitor e o par calado), ociosa ou não; as classes do buffer
de quadro estão em §5.3. Não entram em `ring.max_bytes`: o teto delas é
`tcp.max_conns`/`tls.max_conns`, e com `0` é o `GOMEMLIMIT` (§15.4) que segura
um enxame de conexões caladas.

**Memória por requisição HTTP.** Cada requisição em voo segura o corpo como
chegou do fio (um buffer que cresce por classes com o que **chega**, até
`L + 16 KiB + 1`, nunca pelo `Content-Length`, §4.5) e o documento
descomprimido (até `L + 1`); com `http.bulk`, `B + 16 KiB` e `B` (§4.5.1). Os
dois vêm de pools; um buffer acima de 64 KiB não volta ao pool (seguraria `L`
por P ocioso). Uma conexão keep-alive serve uma requisição de cada vez, então
o teto é `http.max_conns` por transporte e, com `0`, o `GOMEMLIMIT`; entre
requisições ela não segura buffer.

**Estado de remontagem UDP.** Os chunks de mensagens ainda incompletas ficam em
memória, cada um num buffer do tamanho dele, até completar ou vencer o prazo de
5 s (§5.2). O teto é **global**, `listen.udp_reassembly_max_bytes` (64 MiB,
contando os dados de cada chunk mais 64 bytes de estrutura) e
`listen.udp_reassembly_max_messages` (16 384); o uso aparece em
`gelf_udp_reassembly_bytes`. Não entra em `ring.max_bytes`: é memória da
recepção, de documentos que ainda não existem. Uma mensagem completa é
concatenada e descomprimida num documento novo, e só ele entra no ring.

**Teto pequeno demais.** Um documento de `n` bytes custa ao ring em memória a
classe do pool de `n` + 384 B (§9.2): 2 KiB, 8 KiB ou 64 KiB, e acima da maior
classe o próprio `n`. O piso de `ring.max_bytes` é o custo de um documento do
tamanho máximo, `item.PooledCap(convert.max_message_bytes) + ring.ItemOverhead`
(131 456 B com o padrão de 128 KiB), e um valor menor é recusado na validação
(§13.2, exit 1); com `sqlite` e `redis` o piso é o próprio
`max_message_bytes`, a medida deles. O piso da **menor** classe não basta: um
ring de 4 KiB aceitaria os documentos curtos e recusaria **sempre** qualquer
documento de 3 KiB, mesmo vazio — no TCP a conexão daquele emissor parava até
o `backpressure_timeout` a cada um, e no HTTP todo `POST` dele levaria `503`.
Com o piso, todo documento que o teto de §5.5 deixa passar cabe num ring vazio
(porte do RP/RN, 2026-10-14).

### 15.2 Regra de propriedade única

**O `Item` é dono do seu cru. Ele volta ao pool exatamente uma vez, e só depois da
confirmação do sink.** Num retry o item **ainda é o dono** e nada é devolvido.
Devolver cedo põe o documento de um emissor no evento de outro: silencioso, invisível
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
- `time/tzdata` é embutido no binário (herdado do `go-syslog-server`): nenhuma
  conversão daqui usa fuso (o `timestamp` GELF é UTC, §6.5), mas nada que um
  dia carregue um fuso deve depender da imagem, que não traz `zoneinfo`.

---

## 16. Configuração

### 16.1 Precedência

```
argv  >  env  >  arquivo INI  >  padrão
```

- Flag: `--<seção>.<chave>` (`--ring.capacity_items=131072`).
- Env: `GELF_<SEÇÃO>_<CHAVE>` em maiúsculas, `.` vira `_`
  (`GELF_SINK_NATS_URLS`, `GELF_CONVERT_KEEP_RAW`), com as exceções de §16.2.
- INI: seção `[ring]`, chave `capacity_items`.
- `--config=/caminho.ini` (env `GELF_CONFIG`) indica o arquivo.

### 16.2 Registry único, e os nomes de env fixados pelas skills

Uma **tabela** declara cada parâmetro (seção, chave, tipo, padrão, descrição, se
é recarregável, nome de env). Dela saem o INI, as flags, o env, o `--help`, o
`--dump-config` e a validação. Um parâmetro novo entra só ali. Sem
`gopkg.in/ini.v1` e sem `spf13/pflag`.

Nomes de env que **não** seguem a regra, porque uma skill os fixou:

| Seção | Env | Origem |
|---|---|---|
| `[loghub]` | `LOGHUB_<CHAVE>`: `LOGHUB_EB_URL`, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE` | skill `loghub-json-schema` |
| `server.data_dir` | `DATADIR` | `go-loghub-ident` |

Nenhuma seção deste servidor tem nome especial: `[convert]` segue a regra como
as outras (`GELF_CONVERT_KEEP_RAW`, `GELF_CONVERT_MAX_MESSAGE_BYTES`,
`GELF_CONVERT_ACCEPT_MALFORMED`). Não existe skill `type=gelf` que fixe nomes
sem a seção, ao contrário do `SYSLOG_<CHAVE>` do `[syslog]` do `go-syslog-server`;
os nomes do `go-syslog-server` (`SYSLOG_CONFIG`, `SYSLOG_*`) não são lidos.

As variáveis de identidade (`AGENT_UUID`, `AGENT_NAME`, `WORKSPACE`, `HOSTNAME`,
`MACHINE_ID`, `MACHINE_ID_FILE`, `LOGHUB_IDENT_DEBUG`) **não** passam pelo
registry: são lidas pelo `go-loghub-ident`, e o `--dump-config` mostra os valores
o que o **ambiente** diz numa seção informativa `[identity]`, sem resolver nada
(§13.1 passo 1: o dump não toca identidade nem disco).

**Erros de configuração não ecoam a entrada** (pendência 30 de 2026-10-11,
corrigida como no R; porte dos achados do RP e do RN, 2026-10-13). Os erros vão
para o stderr e, num `SIGHUP`, para o log; nenhum cita texto que possa ser
segredo:

- **URL** (`LOGHUB_EB_URL`): só o tipo do erro e a dica de codificar; chave de
  consulta PostgreSQL citada só se estiver na lista fixa da libpq; `sslmode`
  inválido sem o valor (§10.9).
- **INI**: linha sem `=` ou cabeçalho sem `]` cita só o número da linha (antes,
  `got %q` citava a linha inteira, inclusive `password: x`). Uma chave
  desconhecida é nomeada só se a seção for deste servidor, a chave tiver forma
  de chave (`[a-z0-9_]`, até 64) e a linha anterior **não** for um segredo: a
  continuação de uma senha que quebrou de linha (`password = eyJhbGciOiJI` e,
  na linha seguinte, `UzI1NiJ9abc==`) lê como chave. Uma chave antes de qualquer
  seção é nomeada só com forma de chave. Uma marca de ordem de bytes UTF-8 no
  começo do arquivo é ignorada (antes, a linha 1 virava "expected 'key =
  value'" sem nada visível que dissesse por quê).
- **Flags**: um argumento solto é citado só até o `=` (antes, `unexpected
  argument %q`). Um nome desconhecido é citado só se tiver forma de nome
  (`[A-Za-z0-9_.-]`) e **não** começar com o nome de um parâmetro e continuar
  (`--sink.postgres.passwordhunter2`, com ou sem `-`, `_`, `.` no meio: o `=`
  esquecido). Um argumento com forma de flag nunca é valor de outra flag
  (`--sink.workers --sink.nats.password=x` é recusado, não engolido) e só é
  nomeado se for flag deste servidor escrita com dois traços (`-h` é também uma
  senha de dois caracteres). Nada depois de um valor é citado: o valor depois
  de `= `, ou a cauda de um valor que o shell partiu num espaço. Um argumento
  desconhecido com forma de flag logo depois de um valor
  (`--sink.nats.password=abc -hunter2`, `... abc --x=y`) pode ser essa cauda: é
  nomeado só quando cai numa seção que este servidor tem (`--sink.nats.strem`,
  um erro de digitação: o `knownSectionOf` do R); fora disso o erro cita só a
  flag anterior e pede aspas. Depois de um valor **vazio**, nem isso: o
  argumento seguinte é o valor.
- **Switches** (`--help`, `-h`, `--version`, `-v`, `--dump-config`) não levam
  valor: `--dump-config=false` despejava e saía com 0; agora é exit 1, sem
  citar o valor. Um `--config=` vazio é exit 1 em vez de cair no
  `GELF_CONFIG`.

**Valores derivados saem comentados no `--dump-config`** (revisão independente
do RP, 2026-10-13, portada): um valor que o `Load` derivou (`pool_size =
workers`, os caminhos debaixo de `server.data_dir`) aparece como
`# chave = valor` depois da nota "derived", para que o dump relido o derive de
novo. Escrito como chave, ele viraria explícito: o `pool_size` deixaria de
seguir `sink.workers` e o `emergency.dir` deixaria de seguir o `data_dir`. Os
valores ajustados que não são derivação (`sink.workers` rebaixado, ring
degradado) continuam escritos: relidos, dão o mesmo efeito.

Durações aceitam a sintaxe do Go (`30s`, `5ms`). `loghub.beacon_interval` aceita
também um **inteiro puro em segundos** (a skill define a variável em segundos).
Tamanhos aceitam **só** `KiB`, `MiB`, `GiB` (sem diferença de caixa) e inteiros
puros em bytes; `KB`, `MB`, `K` e `M`, que a standard aceitava, são recusados.

### 16.3 Recarga por `SIGHUP`

Recarrega **só** o que é seguro trocar a quente e não muda os bytes do JLH:

- `log.level`, `log.access`, `log.access_sample`
- `filter.min_level`, `filter.levels`
- `ratelimit.*` (inclusive `enabled`)
- `sink.sqlite.keep_files`

Toda chave de `[convert]` (`keep_raw`, `max_message_bytes`, `accept_malformed`)
**exige restart**: ela muda a conversão, e a conversão precisa ser
determinística entre o item gravado e o item drenado (§3.5). Os listeners e os
tetos de `[listen]`, `[tcp]`, `[tls]` e `[http]` também exigem restart. A
identidade fica congelada.

Duas referências: chave recarregável comparada com o que está **em execução**
(para que reverter ao valor de boot seja aplicado); chave de restart comparada
com o **boot** (para que todo `SIGHUP` avise sobre ela até o restart). Cada uma
das duas sozinha falha num dos casos, como a standard aprendeu: comparada só
com o boot, a recarregável revertida ao valor de boot parece "inalterada" e não
é aplicada; comparada só com o que está em execução, a de restart é avisada no
primeiro `SIGHUP` e nunca mais. Nenhuma chave deste servidor aceita sinônimos
(os enums guardam a grafia canônica), então a comparação é a dos valores.

**Chave derivada não é relatada.** Uma chave **derivada** no boot e no arquivo
novo (§16.2, a nota `derived` do `--dump-config`: `emergency.dir`, `ring.sqlite.path` e `sink.sqlite.path` de `server.data_dir`, e `sink.<tipo>.pool_size` de `sink.workers` com `postgres`, `mysql` e `clickhouse`) não entra na lista
"requires a restart": muda só porque a chave que a deriva mudou, e essa é
relatada. Uma que passa de explícita a derivada, ou o contrário, é relatada;
notas que não começam com `derived` (`lowered to 4`, `forced to 1`, `resolved to
an absolute path`) dizem como um valor dado foi ajustado e continuam relatadas.
A comparação usa as notas que o `Load` já grava (`Loaded.Notes`), as mesmas do
`--dump-config` (porte do RP/RN, 2026-10-14; `TestReloadDoesNotReportDerivedKeys`, `TestReloadDoesNotReportTheDerivedPoolSize`).

**Chave sombreada.** Como env e flag vencem o arquivo (§16.1) e não mudam num
processo em execução, uma chave que o arquivo define mas um env ou uma flag
decide **nunca** muda pelo arquivo: a recarregável não recarrega, e a de restart
não muda nem com o restart. Todo `SIGHUP` loga cada uma em
`WARN`, com a origem que manda (`env GELF_FILTER_MIN_LEVEL`), mesmo quando o
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
udp   = :12201
tcp   = :12201
tls   = :12203
http  = :12202
https = :12204

[tls]
cert_file   = /etc/gelf/tls/server.crt
key_file    = /etc/gelf/tls/server.key
client_auth = optional
client_ca   = /etc/gelf/tls/clients-ca.crt

[http]
read_header_timeout = 10s
read_timeout        = 30s

[convert]
keep_raw          = malformed
max_message_bytes = 128KiB

[filter]
min_level = info

[ring]
mode           = memory
capacity_items = 65536
max_bytes      = 256MiB

[sink]
type = nats

[sink.nats]
urls   = nats://nats-1:4222,nats://nats-2:4222
stream = LOGHUB_GELF

[admin]
bind = 0.0.0.0:9104
```

Com `--config=/etc/gelf/gelf.ini` (ou `GELF_CONFIG=/etc/gelf/gelf.ini`).

### 16.5 Tabela de parâmetros

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `ring.sqlite.commit_max_delay`,
`ring.sqlite.busy_timeout`, `ring.redis.claim_idle`, `sink.sqlite.busy_timeout`,
`ratelimit.sweep_interval`, `shutdown.drain_timeout`, `tcp.backpressure_timeout`,
`tls.backpressure_timeout`, `tls.handshake_timeout`, `http.read_header_timeout`,
`http.read_timeout` e `proxy.header_timeout` —
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
| `udp` | `:12201` | GELF UDP, com chunks (§4.2, §5.2) |
| `udp_listeners` | `1` | `>1` habilita `SO_REUSEPORT` |
| `udp_read_buffer` | `8MiB` | `SO_RCVBUF`; o efetivo é exposto |
| `udp_reassembly_max_bytes` | `64MiB` | teto global dos chunks em remontagem (§5.2); 1 MiB a 4 GiB |
| `udp_reassembly_max_messages` | `16384` | teto de mensagens abertas; 1 a 1 048 576 |
| `tcp` | `:12201` | GELF TCP, moldura `\0` |
| `tcp_listeners` | `1` | |
| `tls` | `""` | GELF TCP sobre TLS; `:12203` é a sugestão |
| `tls_listeners` | `1` | |
| `http` | `:12202` | `POST /gelf` (§4.5) |
| `https` | `""` | idem sobre TLS; `:12204` é a sugestão |

#### `[tcp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `idle_timeout` | `120s` | `0` = nunca |
| `max_conns` | `0` | por transporte; bloqueia no accept |
| `max_conns_per_ip` | `0` | por transporte; fecha na hora |
| `backpressure_timeout` | `30s` | |

#### `[tls]`

| Chave | Padrão | Descrição |
|---|---|---|
| `cert_file`, `key_file` | `""` | obrigatórios com `listen.tls` ou `listen.https` |
| `client_auth` | `optional` | `optional` \| `require` \| `none` (§4.7) |
| `client_ca` | `""` | CA dos clientes; obrigatória com `require` |
| `min_version` | `1.2` | `1.2` \| `1.3` |
| `handshake_timeout` | `10s` | só o transporte `tls` |
| `idle_timeout`, `max_conns`, `max_conns_per_ip`, `backpressure_timeout` | como `[tcp]` | só o transporte `tls` |

#### `[http]` — `http` e `https`

| Chave | Padrão | Descrição |
|---|---|---|
| `read_header_timeout` | `10s` | prazo dos cabeçalhos (e do handshake no `https`) |
| `read_timeout` | `30s` | prazo da requisição inteira, corpo incluído |
| `idle_timeout` | `60s` | espera de uma conexão keep-alive pela próxima requisição; nunca 0 |
| `max_requests_per_conn` | `0` | a resposta da N-ésima requisição leva `Connection: close`; 0 = sem teto |
| `bulk` | `false` | um documento por linha do corpo (§4.5.1) |
| `bulk_max_bytes` | `4MiB` | com `bulk`, teto do corpo descomprimido; 1 KiB..64 MiB, e abaixo de `convert.max_message_bytes` vale ele |
| `max_conns` | `0` | por transporte; bloqueia no accept |
| `max_conns_per_ip` | `0` | por transporte; fecha na hora |

#### `[proxy]`

| Chave | Padrão | Descrição |
|---|---|---|
| `protocol` | `off` | `off` \| `optional` \| `required` (§4.6); vale para `tcp`, `tls`, `http`, `https` |
| `trusted` | `127.0.0.1/32,::1/128` | CIDRs dos balanceadores |
| `header_timeout` | `5s` | |

#### `[convert]` — exige restart (§16.3)

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `keep_raw` | `GELF_CONVERT_KEEP_RAW` | `malformed` | `malformed` \| `always` (§6.9) |
| `max_message_bytes` | `GELF_CONVERT_MAX_MESSAGE_BYTES` | `128KiB` | teto do documento descomprimido (§5.5); 1 KiB a 16 MiB |
| `accept_malformed` | `GELF_CONVERT_ACCEPT_MALFORMED` | `true` | `false` descarta `malformed` no worker |

#### `[filter]` — recarregável

| Chave | Padrão | Descrição |
|---|---|---|
| `min_level` | `7` | aceita `level <= isto`; nome ou número |
| `levels` | `""` | nomes ou números 0–7; vazio aceita todos |

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
| `max_bytes` | `512MiB` | bytes do documento cru; global com `redis`. No mínimo o custo de um documento de `convert.max_message_bytes` na medida do modo (com `memory`, a classe do pool + 384 B: 131 456 B com o padrão; §13.2, §15.1) |
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
| `stream` | `gelf:ring` | |
| `group` | `gelf-ring` | |
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
| `stream` | `LOGHUB_GELF` | validado, nunca criado; ignorado em `core` |

Não há `subjects`: o subject é só `loghub.{space}.gelf` (§3.8, decisão 16).

#### `[sink.redis]`

| Chave | Padrão |
|---|---|
| `addrs`, `username`, `password`, `db`, `sentinel_master`, `tls`, `tls_ca`, `tls_insecure` | como `[ring.redis]` |
| `stream` | `loghub.{space}.gelf` (`{space}` expandido por envelope, §3.6) |
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
| `username`, `password`, `database` | `gelf`, `""`, `gelf` |
| `table` | `loghub.inbox` no PostgreSQL (§3.7.1), `gelf_events` no MySQL (`nome` ou `esquema.nome`; nunca criada) |
| `pool_size` | `0` (deriva `workers`; a sonda tem conexão própria) |
| `tls`, `tls_ca`, `tls_insecure` | `false`, `""`, `false` |

#### `[sink.clickhouse]`

| Chave | Padrão |
|---|---|
| `addrs` | `127.0.0.1:9000` (lista, protocolo nativo; 9440 com TLS) |
| `username`, `password`, `database` | `gelf`, `""`, `gelf` |
| `table` | `inbox_gelf` (§3.7.2; `nome` ou `banco.nome`, caixa preservada; nunca criada) |
| `pool_size` | `0` (deriva `workers`; a sonda tem conexão própria) |
| `tls`, `tls_ca`, `tls_insecure` | `false`, `""`, `false` |

#### `[loghub]`

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `eb_url` | `LOGHUB_EB_URL` | `""` | §10.9 |
| `beacon_interval` | `LOGHUB_BEACON_INTERVAL` | `15` (s) | `0` desliga o periódico; fora o `0`, mínimo `1s` (§11) |
| `ctl_namespace` | `LOGHUB_CTL_NAMESPACE` | `loghub._ctl` | |

#### `[emergency]`, `[admin]`, `[log]`, `[shutdown]`

| Chave | Padrão |
|---|---|
| `emergency.enabled` | `true` |
| `emergency.dir` | `<data_dir>/emergency` |
| `admin.enabled` | `true` |
| `admin.bind` | `127.0.0.1:9104` |
| `log.level` | `info` (recarregável) |
| `log.format` | `json` |
| `log.access`, `log.access_sample` | `false`, `0.01` (recarregáveis) |
| `shutdown.pre_stop_delay` | `0s` |
| `shutdown.drain_timeout` | `30s` |

### 16.6 Escrita em disco: uma raiz só

Tudo o que o serviço grava — a identidade (`agent_uuid`, `machine_id`, gravados
pelo `go-loghub-ident`), `ring.db`, `events.db` e `emergency/` — fica sob
`server.data_dir`, e no contêiner essa raiz é `/data` (os compose sobem com
`read_only: true`). Sem exceção: este servidor não tem socket unix (o
`listen.unix` do `go-syslog-server` não existe aqui).

Caminhos fixos do contêiner: `/app` guarda o binário (dono `root`), `/data` é a
única área gravável, `/etc/gelf` é o INI em `:ro` (os compose montam
`docker/conf/` ali e definem `GELF_CONFIG=/etc/gelf/gelf.ini`, que traz as
chaves recarregáveis, §16.3), `/etc/gelf/tls` são os certificados
(`docker/tls/`, também `:ro`), `/tmp` é tmpfs. `WORKDIR
/data`; o binário é invocado por `/app/go-gelf-server`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

---

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-gelf-server/                 ESTE módulo (G)
  go-gelf-pg-server/              GP: corte de G (ring memory + sink postgres, sem TLS)
  go-gelf-nats-server/            GN: corte de G (ring memory + sink nats, sem TLS)
  go-gelf-injector/               GI: cliente de carga e funcional
  go-syslog-server/               S: pai do código (copiar com adaptação, nunca importar)
  go-relplog-server/, go-webhook-server/, ...
```

Módulo independente. Nada é importado de outro módulo do monorepo: compartilhar
pacotes faria os módulos evoluírem amarrados (DESIGN da família, §1).

Os nomes `go-gelf-server` (binário, imagem Docker `tmsoftbrasil/go-gelf-server`,
`--version`, tags `go-gelf-server/vX.Y.Z`) e o prefixo `gls-` dos projetos,
contêineres e volumes Compose são **deste** módulo. O `go-syslog-server` usa
`sls-`: com o mesmo nome de projeto, um `docker compose up` feito na pasta de um
assumiria os contêineres e os volumes da pilha do outro, sem aviso.

**O que GP e GN copiam literalmente** (a menos do module path): `internal/convert`
inteiro, os arquivos GELF de `internal/receiver` (`chunks.go`, `decompress.go`,
`framing.go`, `http.go`) e `internal/item`. Uma correção num deles vale para os
três servidores, e é portada no mesmo dia; uma divergência entre as cópias é
bug.

### 17.2 Layout interno

```
cmd/go-gelf-server/main.go     flags que encerram, lhident.Initialize(), chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão
  config/     registry único -> INI + flags + env + --help + dump; validação; LOGHUB_EB_URL
  ident/      Identity (congelada no boot) e a gramática de space          [lógica pura]
  jlh/        escritor JSON por append, Event, cabeçalho do envelope, UUID [lógica pura]
  convert/    documento GELF -> payload type=gelf -> JLH: json.go (parser
              por varredura validante), number.go (números como texto,
              timestamp decimal), names.go, convert.go          [lógica pura, fuzz]
  item/       Item, codec do cabeçalho (compression, chunks), pools       [lógica pura, fuzz]
  rediscli/   cliente rueidis e a tabela de classificação de erro do Redis,
              compartilhados por sink=redis e ring=redis (§10.3, §9.5)
  jlhcheck/   SÓ TESTE: os schemas (envelope da skill, jlh-gelf-payload deste
              repositório), os exemplos do envelope e o oráculo de
              conformidade (§18.2); nenhum arquivo de produção o importa
  receiver/   datagram.go (UDP + pktinfo por plataforma, sockopt_*.go),
              chunks.go (remontagem GELF), decompress.go (gzip/zlib/deflate
              com teto), framing.go (moldura \0), stream.go/tcp.go/tls.go
              (conexões), http.go (POST /gelf), proxy.go, limits.go, ingest.go  [fuzz]
  filter/     level (min_level, levels)                                     [lógica pura]
  ratelimit/  token bucket com shards e sweep                               [lógica pura]
  ring/       interface Ring + memory, sqlite, redis, none
  sink/       interface Sink + nats, redis, sqlite, postgres, mysql, clickhouse,
              none, workers, health, tls
  control/    heartbeat beacon
  emergency/  dump e reingestão                                             [fuzz]
  obs/        métricas Prometheus próprias + slog
```

`convert/` (o parser JSON), `receiver/` (chunks, descompressão, moldura `\0`,
corpo HTTP e o leitor do PROXY) são os parsers de entrada adversária. Os alvos
de fuzz obrigatórios são esses e os decodificadores do que volta do disco ou da
rede interna (cabeçalho do item, dump, cópias da `loghub.inbox`): a lista
completa está em §18.3.

### 17.3 Dependências

**Oito em produção, e ponto** — as mesmas oito do `go-syslog-server`. O GELF
não trouxe nenhuma: gzip, zlib, deflate e o servidor HTTP são da biblioteca
padrão, e o parser JSON é próprio.

| Dependência | Para quê |
|---|---|
| `github.com/redis/rueidis` | `sink=redis`, `ring=redis` |
| `modernc.org/sqlite` | `ring=sqlite`, `sink=sqlite` (Go puro, sem CGO) |
| `github.com/jackc/pgx/v5` | `sink=postgres`, pela API nativa |
| `github.com/go-sql-driver/mysql` | `sink=mysql`, por `database/sql` |
| `github.com/nats-io/nats.go` | `sink=nats` e beacon, com o pacote `jetstream` |
| `github.com/ClickHouse/clickhouse-go/v2` | `sink=clickhouse`, pela API nativa e no protocolo nativo |
| `github.com/patrickbrandao/go-loghub-uuidv7` v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` v0.5.2 | identidade (sem dependência nova: usa a `go-loghub-uuidv7`) |

Só em teste: `github.com/santhosh-tekuri/jsonschema/v6` (contratos, §18.2).
`golang.org/x/net` **não** é importado: o PKTINFO é feito com a stdlib
(`syscall`). O `golang.org/x/text` aparece no binário só **transitivamente**,
pelo SASLprep (`secure/precis`) do `pgx`; nenhum pacote daqui o usa.

**`clickhouse-go`, a oitava**: escolha do usuário na família do `go-syslog-server` contra a
recomendação da entrevista (HTTP da biblioteca padrão com `RowBinary`); o
registro do custo é o do `go-webhook-server` (§14.3 de lá): uma dúzia de
transitivas no binário, nenhuma abrindo socket, escrevendo em disco ou lendo
`TMPDIR` por conta própria, e ~4 MB a mais de binário. A posse de buffer foi
auditada no driver v2.48.0: o `Append` copia cada valor para a coluna
(§10.10.3); uma versão nova do driver pede a mesma auditoria.

**Gerador de UUID:** um `uuidv7.NewGenerator()` por processo, criado no boot e
compartilhado (seguro para concorrência, ChaCha8 com semente de `crypto/rand`).
Um `init()` falha alto se `crypto/rand` não responder. O `uuid` não é segredo
(expõe o instante), e nada deriva capacidade dele.

**O parser JSON é próprio** (DECISOES §2): o `encoding/json` passa números por
`float64` (ou exige `UseNumber` e aloca um mapa por documento), troca UTF-8
inválido por U+FFFD em silêncio, aceita chaves duplicadas sem dizer e falha no
primeiro erro — e a regra aqui é nunca falhar (§6.1), sem perder o texto dos
números (§3.3). O `encoding/json` só aparece fora do caminho quente: na cópia
reconstruída da `loghub.inbox` (§3.7.1), com `json.Number`, e só para o payload
com NUL ou número fora do `numeric`. A citação de `fields`/`extra` do ClickHouse
(§10.10.3) é uma reescrita de bytes, sem ele.

**Sem CGO.** `CGO_ENABLED=0` é premissa do Dockerfile.

### 17.4 Convenções

- Código, identificadores e comentários em **inglês**; documentação e conversa
  em **português**. Não misture dentro de um arquivo. As únicas strings em
  português no código são os textos normativos de `parse.error` (§6.1), que são
  dado, não mensagem.
- Comentários explicam o **porquê** e o que daria errado com a alternativa
  rejeitada. Código copiado do `go-syslog-server` vem **com** os comentários.
- Citações da spec por número de seção (`§5.2`).
- Config: o registry é a fonte única. Métricas: cardinalidade fixa.

---

## 18. Testes

### 18.1 Unitários

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**: os bugs de perda de dados vivem em caminhos
concorrentes (a remontagem UDP é estado compartilhado entre listeners). Cobertura
real com `-coverpkg=./internal/...`. Os pacotes cujos testes precisam de
identidade recebem `ident.Identity` montada à mão; nenhum pacote chama
`lhident.Initialize()` em teste. O caminho real da biblioteca (os códigos
100–114, `DATADIR`) é provado só pelo binário, rodado como subprocesso
(`internal/app/binary_test.go`), porque o `Initialize()` chama `os.Exit` e só
pode rodar uma vez por processo.

### 18.2 Vetores dourados e contratos (desvio D3)

`internal/jlhcheck/loghub/` (o pacote `jlhcheck`, importado **só** por arquivos
`_test.go`, para que o `jsonschema/v6` nunca entre no binário) guarda:
`jlh-envelope.schema.json` e os exemplos `valid/` e `invalid/` do envelope
(cópias da skill `loghub-json-schema`, com a origem no `README.md`), e
`jlh-gelf-payload.schema.json`, **escrito neste repositório** (decisão 1: não
existe skill `type=gelf`), com o rigor do `jlh-syslog-payload.schema.json`
(`additionalProperties: false`, `^x-` livres, `enum` fechado dos 25 avisos e dos
cinco textos de `parse.error`).

- **Schemas:** todo envelope produzido nos testes (corpus, fuzz, integração,
  destinos reais) passa nos dois schemas, com `santhosh-tekuri/jsonschema/v6` e
  `AssertFormat` ligado (os formatos `ipv4`/`ipv6`). Os exemplos `valid/` passam
  e os `invalid/` falham, para provar que o validador está ligado.
- **Regras semânticas** (o oráculo, `jlhcheck.Validate`, sem confiar no código
  que confere: a ordem dos avisos de §6.8 é duplicada nele de propósito):
  sem chave duplicada em nenhum nível; ordem e número das chaves do envelope e
  do payload (§3.1, §3.2); `type = "gelf"`; `level` do envelope =
  `payload.level.code`, e o nome é o rótulo RFC 5424 do código; `timestamp`
  com data possível; `parse.warnings` sem repetição e na ordem canônica; `raw`
  marcado base64 decodifica, e `raw` UTF-8 **nunca** vai em base64; `size` =
  octetos de `raw` quando há `raw` (o GELF nunca trunca, §5.5); `host.hostname`
  sem nenhum branco do conjunto completo de D5; `from` UUIDv7 canônico
  minúsculo; `sid` recalculado de `payload.host` e `payload.transport` (§6.10);
  `event.iso` = `uts` + `sub` truncado em ms; `uuid` UUIDv7 minúsculo cujo
  instante é o `event`.
- **Corpus dourado:** `internal/convert/testdata/corpus/*.gelf` (os bytes
  exatos do documento descomprimido) + `*.want.json` (o envelope esperado),
  **15 pares** no mínimo (o teste falha abaixo disso): driver GELF do Docker,
  fluent-bit, Graylog (exemplo da spec), `graypy` com traceback, `logstash-gelf`
  (Java), saída `gelf` do Logstash, a biblioteca Go `gelf`, GELF 1.0 com
  `file`/`line`, sem `level` (D1), `message` no lugar de `short_message`,
  `_id` e `null`, `level` duplicado, bytes Latin-1, documento truncado e
  documento array. Cada saída é comparada byte a byte (o `uuid` pelo instante)
  e passa no oráculo. `GELF_UPDATE_GOLDEN=1 go test -run Corpus
  ./internal/convert/` reescreve os `.want.json`; o diff é revisado à mão
  antes de entrar. **O corpus nunca encolhe**; um caso novo de campo entra
  aqui antes da correção do conversor.

**Por que não validar em execução** (D3): o serializador é um só, é
determinístico e é exercido pelo fuzz com validação completa em cada saída; um
validador de schema no worker custaria dezenas de microssegundos por documento
para pegar um bug que o fuzz e o corpus já pegam antes do release. É o desvio
da regra "validar antes de publicar" do JLH, e fica registrado aqui.

### 18.3 Fuzz — obrigatório antes de release

| Alvo | Pacote | Propriedade |
|---|---|---|
| `FuzzConvert` | `convert` | nunca entra em pânico; **toda saída passa nos dois schemas e nas regras semânticas**; o `LevelOf` da recepção é o `level` do envelope (§7); `size` é o documento; o pior JLH de §3.9 vale |
| `FuzzFrame` | `receiver` | a moldura `\0` com qualquer fluxo, qualquer partição das leituras e qualquer teto: sem pânico, nunca um `\0` num documento, nunca um documento acima do teto, e a sequência de documentos e recusas é exatamente a de um oráculo independente |
| `FuzzReassemble` | `receiver` | a remontagem de chunks com duas origens, alguns ids, datagramas sem chunk de uma terceira e passos de relógio com varreduras: sem pânico, nunca passa dos tetos, toda mensagem completa é a concatenação dos chunks na ordem de `seq`, e uma varredura no futuro devolve todo byte |
| `FuzzDecompress` | `receiver` | qualquer byte como qualquer contêiner: sem pânico, nunca mais que `L` bytes de saída, só os dois erros (`too_large`, `decompress`), e o que a stdlib comprimiu volta igual |
| `FuzzProxy` | `receiver` | v1 e v2: sem pânico, nunca consome além do cabeçalho declarado; cabeçalho válido consome exatamente os bytes dele, nenhum não consome nada |
| `FuzzHead` | `item` | o decodificador do cabeçalho (com `compression` e `chunks`) recusa sem pânico; ida e volta é identidade |
| `FuzzRead` | `emergency` | o dump tolera truncamento sem pânico e nunca confia no comprimento declarado |
| `FuzzJSONBSafe` | `sink` | a troca do `\u0000` para a `loghub.inbox` (§3.7.1) devolve JSON válido igual ao original com U+0000 → U+FFFD, e nada mais |
| `FuzzChPayload` | `sink` | a reescrita de bytes do payload do ClickHouse é igual, decodificada, à versão com `encoding/json`, e preserva a ordem das chaves (§10.10.3) |
| `FuzzInboxPayload` | `sink` | o payload da coluna é JSON sem escape NUL, `raw` volta exato e os demais textos (chaves e valores de `fields`/`extra` inclusive) só trocam U+0000 por U+FFFD |

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzReassemble$   -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzDecompress$   -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=10m
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$         -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzChPayload$    -fuzztime=10m
```

O `^…$` importa: o Go recusa um `-fuzz` que case dois alvos do pacote.

### 18.4 Corpus de documentos reais

O corpus dourado de §18.2 é o corpus de emissores reais: cada `.gelf` foi
capturado do emissor que o nome diz (ou montado byte a byte a partir do código
dele, quando a captura exigiria um ambiente que este repositório não tem; o
nome do par e o comentário do teste dizem qual). O que vem de compressão e de
chunks não está no corpus (ele guarda o documento já descomprimido): esses
caminhos são provados pelos testes de `receiver` e pelos emissores vivos
(§18.7).

O `fluent-bit-gelf.gelf` é captura real do `out_gelf` do fluent-bit 4.0.3
(2026-10-31): mapas aninhados chegam **achatados** (`_kubernetes_pod_name`) e
arrays como texto (`"1, a"`); o vetor anterior, com `_kubernetes` como objeto,
não era o que o fluent-bit manda e ficou como `nested-field-object.gelf`
(prova o `field-coerced`). O corpus nunca encolhe. Os vetores dourados são
comparados **byte a byte** com o `.want.json` compactado (`json.Compact`
preserva a ordem): a ordem fixa do payload (§3.2) e a ordem de chegada de
`fields`/`extra` (§3.3, §6.6) fazem parte do contrato.

### 18.5 Integração em processo

A pilha **real** (a mesma `run()` do binário) com `sink=sqlite` e `sink=none`,
sem servidor externo: os cinco transportes chegam ao destino (UDP com `dst-ip`
por PKTINFO, inteiro e em chunks, comprimido ou não; TCP com `\0`; TLS com e
sem certificado de cliente; HTTP e HTTPS com `202`/`4xx`/`503`), PROXY v1 e v2,
fidelidade do JLH (o `raw` volta byte a byte), filtro e `SIGHUP` (ida e volta),
dump de emergência e reingestão exatos (mesmo `uuid`, mesmo JLH), shutdown
gracioso sem perda, segundo sinal (130), nenhum transporte (exit 1), códigos
100–114 do `go-loghub-ident` num subprocesso, e a ordem do boot.

### 18.6 Destinos reais

Suítes que pulam sem a variável e criam o que precisam **a partir do arquivo que
o operador aplica** (`docker/postgres/inbox.sql`, `docker/mysql/schema.sql`,
`docker/nats/stream.json`, `docker/clickhouse/inbox_gelf.sql`):

```bash
GELF_TEST_REDIS_ADDR=127.0.0.1:26379      go test -race -timeout 600s ./internal/sink/ ./internal/ring/ ./internal/app/
GELF_TEST_PG_DSN='postgres://...'         go test -race -timeout 600s ./internal/sink/ ./internal/app/
GELF_TEST_MYSQL_DSN='root:...@tcp(...)/x' go test -race -timeout 600s ./internal/sink/ ./internal/app/
GELF_TEST_NATS_URL=nats://127.0.0.1:24222 go test -race -timeout 600s ./internal/sink/ ./internal/control/ ./internal/app/
GELF_TEST_CLICKHOUSE_DSN='clickhouse://...:29000/x' go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

Mais `GELF_TEST_REDIS_LRU_ADDR` (um Redis com `allkeys-lru`, a recusa de §9.5 e
§10.3), `GELF_TEST_NATS_NOJS_URL` (sem JetStream), `GELF_TEST_NATS_PERM_URL`
(usuários com `loghub.*.gelf` negado, `_INBOX` negado e o beacon negado,
§10.7) e `GELF_TEST_NATS_SERVER_BIN` (`nats-server` com `tls {}` subido pela
suíte). `docs/TESTING.md` §6 diz como subir cada um.

O ClickHouse (25.8+, protocolo nativo) cria cada tabela de
`docker/clickhouse/inbox_gelf.sql`. A suíte prova a ida e volta pelo oráculo
(com a regra de leitura `{}` = ausente e os valores de `fields`/`extra` como
texto: `1.50`, `1e400`, `12345678901234567890`, objetos e arrays de `extra`),
nenhuma string lida como data (`fields.created` do Docker), um JLH grande, o
token descartando o retry, uma tabela recusada no boot para cada regra de
§10.10.5 (a sem os *hints* inclusive), privilégio, `readonly`, limite de
memória e a tabela de §10.10.7 lida da própria SPEC.

O ring Redis tem suíte própria contra `GELF_TEST_REDIS_ADDR`: capacidade
global entre duas instâncias, `XAUTOCLAIM` de uma instância morta, `Touch` que
impede o roubo de um lote vivo, contador de bytes exato depois de duplicatas.

Nenhuma ferramenta dos testes ou do Compose usa `latest`: o `nats-server` dos
testes é o v2.14.6, o `natsio/nats-box` do serviço `nats-init` é o 0.20.0, o
`fluent/fluent-bit` do emissor de demonstração é o 4.1.1, e as imagens dos
destinos fixam a versão (`nats:2.11`, `redis:7.4`, `postgres:18`, `mysql:8.4`,
`clickhouse-server:25.8`). Com `latest`, o stream criado ou o resultado de um
teste mudariam sem mudança no repositório.

### 18.7 Emissores vivos

A única prova de que chunks, compressão e moldura funcionam no fio é um emissor
GELF de verdade. Dois, os mais leves que cobrem os caminhos (DESIGN §6):

- **fluent-bit** (`fluent/fluent-bit`, saída `gelf`): `Mode udp` com
  `Compress On` e `Packet_Size 1420`, com um registro de ~12 KiB aleatório que,
  comprimido, ainda passa de um chunk (UDP + gzip + chunks), e `Mode tcp`
  (moldura `\0`). É o emissor do `docker/compose.demo.yml`.
- **driver de log `gelf` do Docker** (`docker run --log-driver=gelf`), UDP com
  `gelf-compression-type` `gzip` (padrão), `zlib` e `none`, e TCP
  (`gelf-address=tcp://`). Não roda dentro de um compose (o daemon manda o log
  do próprio contêiner), então é um passo manual.

A suíte automática é `GELF_TEST_EMITTERS=1 go test -race -run Emitters
./internal/app/` (`internal/app/emitters_test.go`): sobe o fluent-bit
(`fluent/fluent-bit:4.0.3`) contra a pilha em processo, com as saídas `gelf`
(UDP com chunks e gzip, TCP, TLS) e `http` (um documento por `POST`). O driver
do Docker e a imagem com o compose de demonstração são a rodada manual. O
procedimento e o resultado de cada rodada estão em `docs/TESTING.md` §6.5.
Cada envelope gravado passa no oráculo de §18.2.

### 18.8 Critérios de aceite v1

**Nunca medidos.** Alvo, não fato; a primeira medição substitui esta tabela.

| Cenário | Alvo |
|---|---|
| UDP sem chunk, `memory`+`nats`, documentos de 400 B | ≥ 100k msg/s por núcleo de recepção |
| UDP gzip em 8 chunks, `memory`+`nats` | ≥ 10k msg/s |
| TCP `\0`, `memory`+`nats` | ≥ 100k msg/s |
| HTTP, um documento por conexão, `memory`+`nats` | ≥ 5k req/s |
| `sqlite`+`nats`, TCP | ≥ 20k msg/s, p99 de confirmação < 20 ms |
| Perda com `SIGTERM` durante carga, TCP e HTTP (`202`) | **zero**, reconciliada |
| Perda com FATAL do sink, `memory` | **zero**, via dump e reingestão |

---

## 19. Matriz de garantias

Leia junto com §2.3: a garantia só existe nos transportes que confirmam (TCP,
TLS, HTTP e HTTPS).

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
| HTTP, HTTPS | **sim, por documento**: o `202` sai depois da garantia do ring; `503` + `Retry-After` quando não dá agora | — |
| TCP, TLS | em parte: a retomada da leitura | nada se perde, mas o emissor não sabe *qual* documento foi aceito |
| UDP | não | `gelf_messages_dropped_total` (inclusive `chunk_timeout`, `chunk_overflow`) e os contadores da interface |

O GELF não tem confirmação por mensagem no TCP: quem precisa saber o que cada
documento virou usa HTTP, um documento por requisição.

---

## 20. Invariantes

Se uma alteração conflita com um destes, ela está errada.

1. A confirmação nunca sai antes da garantia da combinação `ring` × `sink`; onde
   não há confirmação, há contador.
2. Ring cheio nunca vira aceitação seguida de descarte, exceto com `sink=none`.
   UDP descarta antes de aceitar; TCP e TLS seguram o emissor; HTTP e HTTPS
   respondem `503` sem aceitar.
3. Um buffer volta ao pool **uma vez**, e só após a confirmação do sink. Num
   retry o item ainda é o dono; uma tentativa por prazo contamina o pedaço.
4. Item aceito só sai do ring após a confirmação do sink (ou um descarte
   deliberado e contado: `logic`, malformado recusado); nunca para liberar
   espaço.
5. **Todo documento aceito vira exatamente um JLH válido** pelos dois schemas, ou
   um descarte contado. A conversão nunca falha; o pior resultado é `malformed`.
6. **Sem perda e sem invenção:** o documento recebido (já remontado, já
   descomprimido, sem a moldura) está em `raw` quando ele é obrigatório ou
   ligado; campo ausente é omitido, nunca adivinhado nem `null` (a exceção
   deliberada é o `level` 6 de D1). Nada é truncado: acima do teto, o documento
   é recusado e contado.
7. Nenhum sink altera o JLH. As colunas promovidas e as cópias da
   `loghub.inbox` (NUL) e do ClickHouse (valores citados) são cópias; `env` é o
   JLH byte a byte.
8. Os endereços do payload são a ponta real do transporte, ou o cliente
   declarado por um PROXY **confiável**, e então o par real fica em `x-proxy`. O
   `host` do documento é sempre alegação do emissor e nunca vira chave de
   rate-limit, particionamento ou decisão; `X-Forwarded-For` é ignorado.
9. Nenhum label de métrica deriva de conteúdo controlado pelo emissor.
10. Nenhum log síncrono por documento no caminho padrão; nenhum payload em log de
    `INFO` ou acima.
11. Um comando do rueidis nunca é reusado após executar; um retry reconstrói.
12. O JLH é idêntico em todos os sinks.
13. **O JLH é função de cabeçalho + documento + config de conversão congelada**
    (§3.5): mesmo `uuid`, mesmos bytes, em retry, dump, reingestão e
    reivindicação.
14. **Uma amostra de relógio por documento** (no UDP, a do último chunk), e dela
    saem o `uuid` e o `event`.
15. `ring` e `sink` são ortogonais.
16. Toda escrita em disco fica sob `server.data_dir` (`/data` no contêiner),
    inclusive a identidade, **por padrão**: os caminhos derivados ficam lá, sem
    exceção fixa. Um `ring.sqlite.path`, `sink.sqlite.path` ou `emergency.dir`
    absoluto fora dele é decisão do operador (um disco mais rápido), aceita com
    `WARN` no boot (§13.2).
17. Nenhum destino compartilhado cria o que valida: `sink=postgres`,
    `sink=mysql` e `sink=clickhouse` nunca fazem DDL, e `sink=nats` nunca cria
    nem altera um stream.
18. Esquema e permissão são `fatal`, nunca `logic` (classe `42` do SQLSTATE,
    `1054`/`1146`/`1142` do MySQL, `WRONGTYPE`/`NOPERM` do Redis); erro
    desconhecido é `network`, nunca `logic`.
19. Nenhuma biblioteca cliente guarda nem retenta o que o ring guarda e o worker
    retenta: `WithRetryAttempts(0)` e `ReconnectBufSize(-1)` no `nats.go`.
20. Nenhum destino recebe um envelope que o teto dele recusaria: o boot compara
    `max_payload` e `max_allowed_packet` com o pior JLH, e o `max_memory_usage`
    do ClickHouse com o pior pedaço (§3.9, §10.10.5).
21. O servidor nunca emite `ping`, `pong` nem sonar, e o stream de dados nunca
    cobre o plano de controle.
22. `lhident.Initialize()` roda uma vez por processo, no `main`; nenhum pacote lê
    identidade em `init()`.
23. **Nunca alocar pelo que o par declara**: nem pelo `count` dos chunks, nem
    pelo `Content-Length`, nem pelo comprimento do PROXY v2; o teto da
    descompressão vale **dentro** do leitor (§5.2, §5.4, §4.5).
24. **Um número JSON nunca passa por `float64`** em nenhum caminho até o JLH, e
    nenhum trecho escapado da entrada é copiado sem ser decodificado e
    reescrito (§3.3).
25. **`4xx` no HTTP só para o que nunca vai passar**; tudo o que pode passar numa
    nova tentativa é `503` (§4.5).

---

## 21. Roteiro pós-v1

Nada disto entra antes de a v1 estar em produção e §18.8 ter sido medido.

| Item | Motivo de ter ficado fora |
|---|---|
| GELF por AMQP ou Kafka | Transportes do Graylog sem uso nos emissores alvo; um consumidor de fila é outro software (§1.2) |
| HTTP/2 | HTTP/1.1 com keep-alive (§4.5) basta; só com um emissor que precise dele |
| Remontagem UDP em shards | Hoje é um mapa sob um mutex (§5.2, §22.1); só com medida de contenção |
| Importação de arquivo GELF (`transport=file`, um documento por linha) | Fora da v1 |
| `space` por listener ou rede | Multi-tenant numa instância; fora da v1 |
| Resposta a sonar, ping e pong | Até existir um `go-loghub-driver` |
| Troca de certificado TLS sem restart | Recarga de `tls.cert_file` por `SIGHUP` |
| Impressão digital da config de conversão no cabeçalho | Detectaria réplicas com config divergente no ring Redis (§3.5) |
| `sink=gelf` (encaminhar a um Graylog) | Política de retry própria e decisão sobre reconstruir o documento a partir do JLH |
| Particionamento por tempo no PostgreSQL e no MySQL | Precisa de decisão sobre quem cria a partição |
| `sink=clickhouse` numa tabela `Distributed` | O `INSERT` vai aos shards de forma assíncrona; a confirmação exigiria decidir o que é "entregue" num cluster (§10.10.5) |
| Esquema `clickhouse://` na `LOGHUB_EB_URL` | Hoje o `sink=clickhouse` só se configura pelas chaves de `[sink.clickhouse]` (§10.9) |

---

## 22. Lacunas e riscos

Os riscos do desenho estão em `docs/DECISOES.md` §3, e o que os testes ainda não
provam, em `docs/TESTING.md` §8. Esta seção guarda o que o **código atual** ainda
faz diferente do que deveria, ou o que se sabe limitado. Cada item diz o
problema, a consequência e a direção da correção, que segue a regra de sempre:
a spec primeiro (a regra entra na seção indicada), depois o código e o teste.
Corrigido um item, ele sai daqui.

**Herança.** Este servidor é uma cópia do `go-syslog-server` de 2026-10-14, e
traz todas as correções que estavam lá: as 30 pendências de 2026-10-11, o porte
das correções do `go-relplog-server` e das revisões do RP e do RN
(2026-10-13), e o porte das pendências do RP/RN com a revisão independente de
2026-10-14 (entradas S do `CHANGELOG.md` da raiz). A regra de cada uma está na
seção onde vale, com a data e a origem no texto (§9.4, §10.4–§10.7, §12,
§13.2–§13.4, §16.2–§16.3). As que eram do `go-syslog-server` e não têm análogo aqui (a
ressalva da RFC 3164 no `timestamp`, o socket `unix`, a sonda MySQL com `unix`,
os subjects por facility) não vieram.

### 22.1 Pendências conhecidas

Abertas em 2026-10-31:

1. **A remontagem UDP é um mapa sob um mutex só**, compartilhado pelos
   `listen.udp_listeners` (§5.2). Com vários listeners e chunks em massa ele é o
   ponto de contenção; nada foi medido. Direção: shards por hash de
   `(src-ip, src-port, id)` com o teto global num contador atômico, quando
   houver medida (§21; DECISOES §3).
2. **Falhas de handshake do HTTPS não são contadas.** O `net/http` faz o
   handshake dentro do servidor e o reporta só no `ErrorLog`, que aqui é
   descartado (senão um scanner inundaria o log); `gelf_tls_handshake_errors_total`
   conta só o transporte `tls` (§14.1). Consequência: um cliente HTTPS com
   certificado errado só aparece como conexão aceita sem requisição. Direção:
   um `ErrorLog` próprio que conte as linhas de handshake (sem logá-las), ou o
   handshake feito pelo próprio listener antes de entregar a conexão ao
   `http.Server`.
3. **Com o filtro ativo o documento é validado duas vezes** (recepção e
   dreno, §2.2, §7). É o preço de o `level` filtrado ser o emitido por
   construção; `BenchmarkLevelOf` mede a varredura, mas o efeito ponta a ponta
   não foi medido (DECISOES §3).
4. **`fields`/`extra` no ClickHouse perdem o tipo do número** (viram o texto,
   §3.7.2): é decisão, não defeito, mas um consumidor que precise do tipo não o
   tem nesse destino. Direção, se aparecer o caso: um segundo *hint* tipado
   para nomes conhecidos, nunca a inferência.
5. **A cópia da `loghub.inbox` pode levar `raw` em base64 de um UTF-8 válido**
   (§3.7.1): é a exceção, só na cópia da fila, à regra do oráculo "`raw` UTF-8
   nunca em base64". Os testes reconvertem antes de passar a linha pelo
   oráculo; um consumidor da fila precisa decodificar o `raw` pelo
   `raw-encoding`, sempre.

O que os testes ainda não provam está em `docs/TESTING.md` §8.

Para que a história fique registrada, o que o porte das pendências do RP/RN
achou e corrigiu no pai, e que veio com a cópia (a regra na seção do assunto):

1. O aborto fecha o ring (`memory`/`none`) antes de contar, e o `Enqueue` do ring em memória reconfere o fechamento depois da reserva; regra em §13.4.
2. `ring.max_bytes` abaixo do custo de um documento de `convert.max_message_bytes` na medida do ring efetivo (com `memory`, a classe do pool + 384 B) é exit 1; regra em §13.2 e §15.1.
3. `admin.bind` no mesmo endereço TCP de `listen.tcp`, `listen.tls`, `listen.http` ou `listen.https` é exit 1 na validação (`sameStreamSocket`); regra em §13.2 e §14.
4. `SIGHUP` não relata a chave derivada (`emergency.dir`, `ring.sqlite.path`, `sink.sqlite.path`, `sink.<tipo>.pool_size`) junto com a que a deriva; regra em §16.3.
5. A discagem falha da reconexão NATS não é logada, só o veredito; o veredito TLS só com servidor único; regra em §10.7.
6. O `Instrumented.Deliver` não conta em `gelf_sink_errors_total{kind="network"}` a escrita cortada pelo cancelamento do boot; regra em §14.1.
7. Com `proxy.protocol=required`, o shutdown não conta `absent` nem `reason="proxy"`, e um cabeçalho PROXY **válido** já no leitor quando o shutdown chega segue para a moldura; só a espera fecha sem contar; regra em §4.6.
8. A falha ao remover um dump já entregue continua só `ERROR`; regra em §12.2.
9. SQLSTATE fora da tabela (`P0001`) é `network`, nunca descarte nem FATAL; regra em §10.5 e §20.18.
10. Dump publicado no instante do aborto fica contado e no disco, e o aborto não espera o disco; regra em §12.1.
11. `gelf_sink_errors_total{kind="fatal"}` conta a recusa fatal mesmo com o boot cancelado; regra em §14.1.
12. Contadores da recepção sem *padding*, sem defeito observável; lacuna de medição em TESTING §8.
13. A sonda do `sink=sqlite` espera a trava da escritora só no próprio prazo, com idade monotônica, heartbeat falho contando como `Write` falho, heartbeat bom limpando a falha e o corte pelo próprio contexto não marcando nada; regra em §10.4.
14. O `ring=none` reconfere o fechamento depois de `cur.Add(1)` e desfaz a reserva e o semáforo, como o `memory`; regra em §13.4.
15. `sameStreamSocket` compara valores: curinga por `IsUnspecified()` e porta como número (D1 da revisão do `go-syslog-server`, D4 do RELP); regra em §13.2.
16. O "até 8 KiB quando ociosa" do buffer de quadro TCP vale entre quadros, com o leitor vazio: um quadro grande seguido de alguns bytes do próximo segura o buffer grande até o idle (medido no pai: soltar antes custava 10× em fluxo contínuo); regra em §5.3 e §15.1.
17. *Pool* NATS só de servidores que recusam o TLS nunca dá veredito (FATAL só pelos fusíveis), e uma URL única cujo nome resolve a vários IPs dá veredito pela recusa do IP sorteado; regra em §10.7.
18. Sobrecontagem transitória do aborto também por um `Enqueue` que reservou a vaga e recua por falta de bytes; regra em §13.4.
