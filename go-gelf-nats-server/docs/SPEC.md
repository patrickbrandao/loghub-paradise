# go-gelf-nats-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-31
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server`
Binário: `go-gelf-nats-server`
Entrada: `docs/DECISOES.md` (decisões 1–19 da família GELF, 2026-10-31, as que
valem para o corte NATS, e as tomadas no corte). Cada decisão de lá está aqui,
com o número da seção.

**Origem.** Esta especificação é a do `go-gelf-server` (G, 2026-10-31) cortada
exatamente como a do `go-syslog-nats-server` (SN) corta a do `go-syslog-server`
(S): ring **só na memória**, sink **só no NATS** (JetStream por padrão, `core`
opcional), emergência em arquivo **JSON Lines**, heartbeat beacon mantido,
**sem TLS em lugar nenhum** (nem listener TCP+TLS/HTTPS, nem cliente NATS), sem
filtro, sem rate-limit, sem subjects granulares. Tudo o que é GELF — conversão,
remontagem de chunks, descompressão, moldura `\0`, recepção HTTP — é o de G,
copiado literalmente (§17.1); tudo o que é do corte — ring, sink NATS, beacon,
dump, `removed.go` — é o de SN, com os nomes trocados (`syslog_` → `gelf_`,
`SYSLOG_` → `GELF_`, `[syslog]` → `[convert]`, `LOGHUB_SYSLOG` → `LOGHUB_GELF`).
Onde a regra muda, a seção diz.

**A numeração das seções é a do `go-gelf-server`, de propósito.** O código
cita a spec por número (`§10.7`, `§12.2`, `§20.19`), e os servidores são da
mesma família: um número que mudasse de sentido entre eles enganaria quem lê
mais de um. Uma seção removida **mantém o título**, seguido de uma nota de uma
linha; nada é renumerado.

Servidor de ingestão **GELF** (Graylog Extended Log Format 1.1) do ecossistema
**LogHub**. Recebe documentos GELF por UDP (com chunks e compressão), TCP
(moldura `\0`) e HTTP (`POST /gelf`), converte cada um num envelope **JLH** com
`type=gelf` e o **publica** no NATS, no subject `loghub.<space>.gelf`: por padrão
num stream JetStream, com confirmação (`PubAck`) por mensagem. É um
**Bridge-IN**: cria a primeira mensagem de cada fato (`step = 0`).

---

## 1. Escopo

### 1.1 Objetivos

- Máximo de documentos por segundo com mínimo de CPU e memória.
- **Dependências mínimas:** três diretas em produção (§17.3), binário pequeno.
- Cada documento aceito vira **exatamente um** envelope JLH válido pelo schema
  do envelope da skill `loghub-json-schema` e pelo
  `jlh-gelf-payload.schema.json` deste repositório (§3).
- **Nunca confirmar** um documento antes de ele estar no ring, nos transportes
  que têm como confirmar (§2.3): TCP continua lendo, HTTP responde `202`.
- Backpressure explícito onde o transporte permite; perda **contada e visível**
  onde ele não permite (UDP).
- **A conversão nunca falha** (§6.1): qualquer documento vira envelope válido;
  o que não coube nos campos fica em `raw`.
- Um destino só: o subject `loghub.<space>.gelf` de um servidor (ou cluster)
  NATS, gravado por padrão num stream JetStream que o operador cria (§3.8,
  §10.7).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| **TCP+TLS** (`listen.tls`), **HTTPS** (`listen.https`) e **mTLS** | Decisão 2 da família: GN **sem TLS**, como SN. Quem precisa de GELF cifrado usa o `go-gelf-server` ou um terminador TLS na frente (com PROXY protocol, §4.6). O payload nunca traz `transport=tls`/`https` nem o objeto `tls` |
| **TLS até o NATS** | A mesma decisão. O cliente é **sempre texto claro**: sem `nats.Secure`, sem CA própria, sem `tls_insecure` (as chaves `sink.nats.tls`, `tls_ca` e `tls_insecure` de G saíram). Toda entrada de `sink.nats.urls` precisa ser `nats://`: `tls://`, `ws://`, `wss://` e um `host:porta` sem esquema são exit 1 (§13.2). A `LOGHUB_EB_URL` só aceita `nats://`; `tls://` é exit 1 com uma mensagem que nomeia o TLS (§10.9). E como o `nats.go` **se promove sozinho** a TLS quando o `INFO` do servidor diz `tls_required`, o sink instala um `TLSCertCB` que recusa: nenhum `ClientHello` é enviado, e um servidor que exige TLS é FATAL na hora (exit 3 no boot, nomeando o TLS; §10.7) |
| Outros sinks: `postgres`, `redis`, `sqlite`, `mysql`, `clickhouse`, `none` | Corte NATS: sink **só** no NATS. Quem precisa deles usa o `go-gelf-server` (todos) ou o `go-gelf-pg-server` (a fila `loghub.inbox`) |
| Outros rings: `sqlite`, `redis`, `none` | Ring **só** na memória. A perda sob `kill -9` é a do ring em memória (§19); FATAL e `SIGTERM` são cobertos pelo dump (§12) |
| Filtro por `level` (`[filter]`) | Como SN: sai. A recepção nem lê o `level` (§2.2); o descarte, se houver, é do consumidor |
| Rate-limit por prefixo de rede (`[ratelimit]`) | Como SN: sai. Proteção contra emissor ruidoso fica na rede (firewall, QoS) |
| Subjects granulares | Não existem nem em G (decisão 16): o subject é sempre `loghub.<space>.gelf` |
| `sink.type` e `ring.mode` como chaves | Um valor só cada um. O log de boot e o `gelf_config_info` continuam dizendo `ring_mode="memory"` e `sink_type="nats"`, para que um painel de G sirva aqui (§14.1) |
| Resposta a sonar, `ping` e `pong` | Como em G: o servidor só **emite** o heartbeat (§11) |
| GELF de saída, roteamento, deduplicação de conteúdo | Responsabilidade de outro software |
| `space` por listener ou rede | Um Workspace por instância na v1 |
| Validação por JSON Schema em execução | Só em teste e CI (§18.2; D3) |
| Autenticação de emissor | Não existe no protocolo sem TLS: firewall ou rede privada |

O porquê comum: **o corte** (o mesmo de SN↔S, pedido pela família), **as
dependências** (de oito diretas em G para três; o binário tem 12,2 MB contra os
30,5 MB de G, mesmo `go build` simples, `darwin/arm64`, Go 1.27.2; 8,4 MB contra
21,0 MB com `-s -w`; imagem de 24,5 MB contra 43,8 MB; §17.3) e **a
velocidade** (a recepção não lê o documento: nem `level`, nem filtro, nem balde
de rate-limit; §2.2).

### 1.3 Fontes normativas e desvios

| Fonte | Versão | Papel |
|---|---|---|
| GELF Payload Specification (Graylog) | 1.1 | Documento, campos, `_` adicionais, chunks, compressão, `\0`, HTTP |
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, `jlh-envelope.schema.json` |
| `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json` | este repositório | Payload `type=gelf` (não há skill `type=gelf`, decisão 1) |
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

No `go-gelf-server` são dois eixos, ring × sink. Aqui cada eixo tem **um valor
só**:

- **RING**: `memory` (§9.3).
- **SINK**: `nats`, `jetstream` por padrão ou `core` (§10.7).

As chaves `ring.mode` e `sink.type` não existem (§1.2). A separação continua no
código: **o ring não consulta o sink, e vice-versa** (invariante §20.15), e as
interfaces `ring.Ring` e `sink.Sink` ficam, porque são a costura pela qual
recepção, workers e saúde são testados.

```
   emissores (Docker gelf driver, fluent-bit, Graylog libs, Logstash, graypy...)
      |
      |  UDP :12201 (chunks, gzip/zlib)   TCP :12201 (\0)   HTTP :12202 (POST /gelf)
      v
 +----------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                   |
 |   [PROXY] -> remontagem / moldura \0 / corpo HTTP              |
 |   -> descompressão com teto -> amostra de relógio               |
 |   -> estado do sink -> uuid -> Item(cabeçalho + documento)      |
 +-------------------------------+--------------------------------+
                                 |
                  +--------------v---------------+
                  | RING memory: canal + 2 tetos |
                  +--------------+---------------+
                                 | sink.workers (8)
                  +--------------v---------------+
                  | CONVERSÃO -> JLH type=gelf   |  <- fora do caminho quente
                  +--------------+---------------+
                                 |
                  +--------------v---------------+
                  | SINK nats (uma conexão)      |
                  |  PublishMsgAsync -> PubAck   |
                  |  Nats-Msg-Id <uuid>.0        |
                  +--------------+---------------+
                                 |
          subject loghub.<space>.gelf -> stream LOGHUB_GELF (JetStream)

   plano de controle: <ctl_namespace>.beacon (loghub._ctl.beacon), NATS core,
                      na mesma conexão, nunca num stream (§11)

   emergência: ring (ou lote retido) não vazio + sink FATAL ou dreno estourado
               => <data_dir>/emergency/emg-<agent_uuid>-<unixnano>.jsonl (§12)

 +----------------------------------------------------------------+
 | porta admin (admin.bind, padrão 127.0.0.1:9104)                 |
 |   /healthz   /readyz   /metrics                                 |
 +----------------------------------------------------------------+
```

O ring é **único e global**, compartilhado por todos os transportes: uma métrica
única de profundidade, backpressure justo (o TCP só para de ler quando o sistema
inteiro está saturado) e código simples.

### 2.2 Onde a conversão acontece, e por quê

Decisão 8. A recepção faz o mínimo que permite decidir se o documento entra; a
conversão completa roda no **worker de dreno**.

| Etapa | Onde roda | Custo |
|---|---|---|
| PROXY | recepção, uma vez por conexão | do transporte |
| Remontagem de chunks, moldura `\0`, corpo HTTP | recepção | inevitável |
| Descompressão com teto | recepção | proporcional ao documento |
| Amostra de relógio, `uuid` | recepção | uma leitura de relógio, um UUID |
| Estado do sink, ring cheio | recepção | duas leituras atômicas |
| Validação JSON, campos, números, escapes | **worker de dreno** | proporcional ao documento |
| Serialização do JLH | **worker de dreno** | proporcional aos campos |

A descompressão fica na recepção porque o cru do ring é o **documento
descomprimido** (decisão 8): o ring guarda JSON legível, e o teto de §5.5 vale
sobre o que o converter vai ler. **A recepção não lê o documento**: não há
filtro nem rate-limit (§1.2), então nem o `level` de topo é varrido (em G ele é,
com filtro ativo).

Consequências que o resto da spec assume (as de S):

1. **O item no ring não tem envelope.** Ele carrega o documento e o que a
   conversão **não pode** recuperar dele: `uuid`, amostra de relógio,
   transporte, endereços, compressão, número de chunks, `from` e `space` (§3.4).
2. **A contabilidade do ring conta só o documento** (mais o `ItemOverhead`, §9.2).
3. **O dump guarda o JLH pronto** (JSON Lines, §12), e volta sem conversão.
4. **O JLH é função determinística** de cabeçalho + documento + config de
   conversão (§3.5). Toda chave de `[convert]` exige restart.


### 2.3 Regra de aceitação, por transporte

> A confirmação sai no instante em que a garantia do ring é cumprida — nunca
> antes. **Onde não existe confirmação, existe um contador.**

| Transporte | A confirmação é | Ring cheio, sink DOWN |
|---|---|---|
| **UDP** | não existe | documento **descartado**, `gelf_messages_dropped_total{reason=...}` |
| **TCP** | continuar lendo do socket | **parar de ler** (§5.7); a janela TCP segura o emissor |
| **HTTP** | `202 Accepted` | `503` com `Retry-After: 1` (§4.5); nada é segurado |

A garantia do ring `memory` é "entrou no canal" (§9.3); depois do `PubAck`, a
do stream (§19).

**O UDP perde por desenho do protocolo.** Um chunk perdido pela rede deixa a
mensagem incompleta, descartada no prazo (§5.2). Quem não pode perder usa TCP
ou HTTP.

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
| `uuid` | UUIDv7 **nível 3**, gerado na recepção da amostra de relógio do documento, depois do estado do sink e da pré-checagem do ring; reusado em toda retentativa, no dump e na reingestão. |
| `from`, `space` | identidade de quem **recebeu** (cabeçalho do item). |
| `type` | `"gelf"`. |
| `level` | `payload.level.code` (0–7); `6` quando ausente (D1) ou inválido. |
| `sid` | FNV-1a 64 de `transport|src-ip|src-port|dst-ip|dst-port` reduzido a 53 bits (§6.10), com `transport` ∈ `udp`, `tcp`, `http` (o schema também admite `tls` e `https`, que só G produz). Nunca 0 aqui: todo transporte é de rede. |
| `step` | `0`. |
| `event` | a **mesma** amostra do `uuid`. |

**Uma amostra de relógio por documento**: no UDP, o retorno do `recvmsg` do
datagrama que **completa** a mensagem (o último chunk, decisão 11); no TCP,
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
| `tls` | `{version, cipher, peer-subject, peer-fingerprint}` | só `tls`/`https`, se algum campo existir — **nunca aqui** (sem TLS, §1.2); o schema é o da família |
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

**Aqui o cabeçalho só existe em memória** (o ring é `memory`, e o dump guarda o
JLH pronto): o codec acima vem com o pacote `internal/item`, cópia literal de G
(§17.1), e só o `FuzzHead` e os testes o exercitam. Os transportes `tls`/`https`
e o campo `tls` existem no tipo e nunca são preenchidos (§22.1 item 3).

### 3.5 Determinismo

> **Invariante:** o JLH é função pura de `(cabeçalho, documento, config de
> conversão)`.

A config de conversão é `[convert]` (§16.5): `keep_raw`, `max_message_bytes`
(atua também na recepção) e `accept_malformed`, congelada no boot. Um dump já
guarda o JLH pronto e não depende da `[convert]` de quem o reingere (§12.2).

### 3.6 Entrada do stream Redis (`sink=redis`)

Removido no go-gelf-nats-server (§1.2): não há `sink=redis`.

### 3.7 Tabela de eventos (`sink=sqlite`, `sink=mysql`) e fila `loghub.inbox` (`sink=postgres`)

Removido no go-gelf-nats-server (§1.2): não há sink SQL; o único destino é o NATS (§3.8, §10.7).

#### 3.7.1 Fila `loghub.inbox` (`sink=postgres`)

Removido no go-gelf-nats-server (§1.2): não há `sink=postgres`; a fila `loghub.inbox` é o destino do `go-gelf-pg-server`.

#### 3.7.2 Tabela `inbox_gelf` (`sink=clickhouse`)

Removido no go-gelf-nats-server (§1.2): não há `sink=clickhouse`.

### 3.8 Mensagem NATS (`sink=nats`)

```
Subject:  loghub.<space>.gelf
Headers:  Nats-Msg-Id:     <uuid>.<step>          (aqui sempre <uuid>.0)
          Content-Type:    application/json
          X-LogHub-Type:   gelf
          X-LogHub-UUID:   <uuid>
          X-LogHub-Space:  <space>
Data:     <jlh>
```

- O subject exato é a regra do JLH: publicar no subject exato, nunca num
  curinga, e nunca ler `space` ou `type` de volta do subject. É **derivado, não
  configurável**: não há chave de subject, nem subjects granulares (decisão 16;
  também não existem em G).
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

O boot compara o teto do destino (`max_payload` do NATS, `max_msg_size` do
stream e o registro do file store, §10.7) com o pior JLH possível e não abre os
transportes quando ele não cabe (exit 3), como SN.

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
  `x-proxy` (o `tls` de G, ≤ 1 KiB, nunca aparece aqui), `parse` (25 avisos ≈ 500 B e o maior
  `parse.error`), `level`, `timestamp` e o cabeçalho do envelope (≈ 250 B).

O pior caso é o malformado: **6 × L + 8 KiB**.

| `L` | pior JLH | cabe em 1 MiB (NATS, com os ≤ 256 B de headers de §3.8)? |
|---|---|---|
| 256 KiB (o pedido da família, DESIGN decisão 10) | 1 544 KiB | **não** |
| 160 KiB | 968 KiB | sim, com 56 KiB de folga |
| **128 KiB (padrão)** | **776 KiB** | sim, com 248 KiB de folga |

O padrão é **128 KiB** (D6): o maior valor redondo que cabe com folga, e o
mesmo pior JLH de S e SN (776 KiB). Quem sobe
`max_message_bytes` acima de ~170 KiB precisa subir o `max_payload` do NATS
junto, e o boot diz isso. `TestWorstCaseJLHIsBounded` prova o teto com
documentos adversários (malformado de controles, coerção aninhada, `raw`
forçado).

O fio pode trazer mais que `L`: o teto do **corpo comprimido** (HTTP) e da soma
dos chunks de uma mensagem (UDP) é `L + 16 KiB` (§5.5), e o documento
descomprimido continua limitado a `L` dentro do leitor.

---

## 4. Transportes

Três transportes independentes, cada um ligado por um parâmetro de `[listen]`
que, vazio, o desliga. **Todos desligados é erro de configuração** (exit 1).
Não há TLS (§1.2): as chaves `listen.tls` e `listen.https` de G são parâmetros
desconhecidos (exit 1 na linha de comando e no INI; `WARN` no ambiente, §13.2).

| Transporte | Parâmetro | Padrão | Moldura |
|---|---|---|---|
| UDP | `listen.udp` | `:12201` | datagrama ou chunks GELF (§5.2) |
| TCP | `listen.tcp` | `:12201` | `\0` (§5.3) |
| HTTP | `listen.http` | `:12202` | um documento por `POST /gelf` |

### 4.1 Portas

12201 é a porta GELF do Graylog e do Docker para UDP e TCP (decisão 5); o TCP e
o HTTP não dividem porta, então o HTTP fica em 12202. A imagem roda como uid
65532: todas são altas. O log de boot imprime o endereço efetivo de cada
listener. Dois transportes no mesmo endereço TCP (`tcp`, `http`, `admin.bind`)
são exit 1; UDP e TCP na mesma porta são permitidos.

### 4.2 UDP

- Leitura com `ReadMsgUDPAddrPort` num buffer de 65 536 bytes; `dst-ip` por
  PKTINFO; IPs canônicos; `SO_RCVBUF` lido de volta; `SO_REUSEPORT` com
  `listen.udp_listeners > 1`; origem não endereçável é `malformed_frame` — tudo
  como S §4.2.
- Um datagrama que começa com `1e 0f` é um **chunk** (§5.2); outro é o
  documento inteiro, possivelmente comprimido (§5.4).
- **Sem backpressure.** Toda recusa é descarte contado com a razão.

### 4.3 (reservado)

O unix datagram de S não existe na família GELF.

### 4.4 TCP

Uma conexão carrega N documentos separados por `\0` (§5.3). É o transporte com
backpressure real. `tcp.max_conns`, `tcp.max_conns_per_ip`, `tcp.idle_timeout`
(120 s), `SO_KEEPALIVE` (30 s), endereços canônicos — tudo como S §4.4.

### 4.5 HTTP e HTTPS

Decisão 13, **só HTTP** (o HTTPS saiu, §1.2; o título é o de G). Um
`net/http.Server` da stdlib por listener, endurecido como os webhooks
(CHANGELOG 2026-10-14):

| Requisição | Resposta |
|---|---|
| `POST /gelf` aceito pelo ring; com `http.bulk`, todas as linhas (§4.5.1) | **`202 Accepted`**, corpo vazio |
| caminho diferente de `/gelf` (inclusive `/gelf/`) | `404` |
| `/gelf` com método diferente de `POST` | `405` com `Allow: POST` |
| `Content-Encoding` fora de `gzip`, `x-gzip`, `deflate`, `identity` | `415` (contado como `reason="decompress"`) |
| `Content-Length` acima de `L + 16 KiB`, corpo lido acima disso, ou documento descomprimido acima de `L` (com `http.bulk`: `B + 16 KiB`, `B`, ou uma linha acima de `L`, §4.5.1) | `413` |
| corpo vazio ou só espaço | `400` |
| descompressão inválida | `400` |
| ring cheio, sink DOWN, shutdown | **`503`** com `Retry-After: 1` |

- **`4xx` é final para o cliente**: só o que nunca vai passar o recebe. Tudo o
  que pode passar numa nova tentativa é `503`.
- **Keep-alive** (HTTP/1.1 persistente, com o *pipelining* sequencial da
  stdlib: as requisições de uma conexão são respondidas na ordem, uma de cada
  vez). Entre duas requisições a conexão espera até `http.idle_timeout` (60
  s); `http.max_requests_per_conn` (0 = sem teto) põe `Connection: close` na
  resposta da N-ésima. A conexão por documento custava um *handshake* e um
  `TIME_WAIT` por evento (e2e de G em 2026-10-31: 20 000 requisições, 20 000
  conexões; porte de G no mesmo dia). O `503` de backpressure (ring cheio,
  sink DOWN) **não** fecha a conexão — o cliente espera um segundo, não paga
  outro *handshake* —; só o shutdown fecha: a resposta do `503` de `shutdown`
  leva `Connection: close`, e desde o início do shutdown **toda** resposta leva
  (`SetKeepAlivesEnabled(false)`), de modo que uma conexão ociosa é fechada e
  uma em uso termina depois da requisição em voo, nunca no meio dela.
  **Sem HTTP/2** (sem TLS não há `h2`; o h2c não é ligado: `TLSNextProto`
  vazio).
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
  dentro do `read_timeout`) passava de 10 s: o documento entrava no ring e o
  `202` se perdia, e o cliente reenviava o mesmo evento com outro `uuid` (a
  correção do `go-webhook-server` de 2026-10-14; revisão de G de 2026-10-31,
  portada no mesmo dia).
- **Limites de conexão:** `http.max_conns` e `http.max_conns_per_ip`, um
  limitador próprio, como §4.4.
- **Origem:** o socket, ou o PROXY protocol de um par confiável (§4.6), lido
  antes do HTTP. **`X-Forwarded-For` e `X-Real-IP` são ignorados**: um
  cabeçalho HTTP é alegação do cliente.
- **Shutdown:** o contexto de recepção cancelado transforma toda requisição que
  espera o ring em `503` (`reason="shutdown"`, §13.4).
- O tempo entre o corpo completo e a garantia do ring vai para
  `gelf_confirm_duration_seconds`, como no TCP.

#### 4.5.1 `http.bulk`: um documento por linha

Padrão `false`, como o `enable_bulk_receiving` do input GELF HTTP do Graylog:
o corpo é **um** documento. Com `true` (decidido sem entrevista em G em
2026-10-31, contestável; portado no mesmo dia; DECISOES §2) — o que o output
`http` do fluent-bit com `Format gelf` manda, todos os registros de um *flush*
num corpo:

- O corpo é descomprimido inteiro (como sempre, pelo `Content-Encoding` ou
  pelos bytes) com o teto `B = http.bulk_max_bytes` (4 MiB, até 64 MiB;
  abaixo de `L`, vale `L`) no lugar de `L`; no fio o teto é `B + 16 KiB`. Fora
  disso, `413`.
- O corpo descomprimido é dividido em `\n`; um `\r` antes do `\n` é cortado
  junto (CRLF dá os mesmos documentos que LF). Uma linha vazia ou só de espaço
  JSON é ignorada sem contar.
- **Tamanho é tudo ou nada:** antes de enfileirar qualquer coisa, toda linha é
  medida; uma acima de `L` recusa o corpo inteiro com `413`, nada enfileirado,
  contado como **uma** chegada e **um** `too_large`. Corpo sem nenhuma linha
  não vazia: `400`, uma chegada e um `empty`.
- Cada linha é um documento com o **próprio `uuid`**, todos da **mesma
  amostra de relógio** (a do corpo completo, §3.1); o gerador de UUIDv7
  ordena os empates. `compression` é a do corpo. Não há filtro nem rate-limit
  neste corte (§1.2), então nenhuma linha é descartada no meio por eles.
- O `202` só sai depois de **todas** as linhas no ring. A primeira linha
  recusada por um motivo que pode passar depois (ring cheio, sink DOWN,
  shutdown) interrompe o corpo: **as linhas anteriores ficam no ring** e a
  resposta é `503` com `Retry-After`. O cliente que reenvia o corpo
  **duplica** essas linhas (com outros `uuid`s: o `Nats-Msg-Id` não as
  deduplica). Retirar do ring o que ele já garantiu não existe na interface do
  ring (§9.1); a alternativa seria reservar espaço para o corpo inteiro antes,
  o que o ring não sabe fazer.
- As linhas que não chegaram a ser tentadas não são contadas.
- Recarregável? **Não**: `http.bulk` e `http.bulk_max_bytes` exigem restart,
  como toda chave de recepção (§16.3).

### 4.6 PROXY protocol

Como S §4.6, nos dois transportes de conexão (`tcp`, `http`), com as mesmas
regras de par confiável, `optional`/`required`, prazo próprio, v1/v2, `LOCAL`,
shutdown durante a espera e `x-proxy`. No HTTP o cabeçalho é lido pela conexão
no primeiro `Read` (dentro da goroutine da conexão, nunca no laço de `Accept`),
e o handler lê dela os endereços resolvidos. Um cabeçalho inválido ou ausente
em `required` fecha a conexão sem resposta HTTP.

### 4.7 TLS (TCP+TLS e HTTPS)

**Removido no go-gelf-nats-server** (decisão 2: sem TLS em lugar nenhum, §1.2).

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

### 5.3 Moldura `\0` (TCP)

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
`reason="empty"` (HTTP `400`); no TCP é separador (§5.3).

### 5.7 Backpressure: parar de ler (TCP)

Como S §5.7: ring cheio ou sink DOWN fazem o leitor parar de chamar `Read`,
conferindo a cada 50 ms, por até `tcp.backpressure_timeout` (30 s); estourado,
fecha a conexão contando o documento em mãos. `Accept`, `Retry` e `RecordDrop`
são caminhos separados (um documento segurado conta uma chegada e, se for o
caso, um descarte), e o `uuid` cunhado na primeira tentativa é reusado. A
pré-checagem do ring cheio (com o teto de bytes, `FullFor`) vem antes do
`uuid` e da cópia: uma retentativa não aloca.

O HTTP não segura: uma tentativa, e `503` com `Retry-After: 1` (§4.5). O UDP
descarta.

---

## 6. Conversão

### 6.1 Regra absoluta: a conversão nunca falha

Qualquer documento **de pelo menos um byte** vira um envelope válido (um
documento vazio não tem envelope possível — `size` ≥ 1 e `raw` obrigatório no
`malformed` —, os receptores nunca o enfileiram, §5.6, e o montador de pedaços
recusa e conta como `malformed` um item vazio, antes de chamar o conversor:
o ring daqui só guarda o que os receptores enfileiraram, então a guarda é a
pré-condição do conversor escrita no código, porte de G de 2026-10-31,
`TestEmptyDocumentIsRefusedBeforeTheConverter`). O pior resultado é
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
- Em G a mesma passada serve também o filtro na recepção; aqui só a conversão
  no dreno a usa (não há filtro, §7).
- Espaço permitido é o do JSON: ` `, `\t`, `\n`, `\r`. Um BOM é sintaxe
  inválida.

### 6.3 Strings

Decodificadas na emissão: escapes resolvidos; um par de surrogates `😀`
vira o caractere; um surrogate **solto** (alto sem baixo, baixo sem alto) vira
U+FFFD com o aviso `encoding-replaced`, que força `raw`. `\u0000` é um
caractere legítimo (sai como `\u0000`; no NATS o JLH viaja intacto, §3.8). As
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

**Removido no go-gelf-nats-server** (§1.2): sem filtro; a recepção não lê o `level`. As chaves `filter.*` de G são parâmetros desconhecidos (§13.2).

## 8. Rate-limit

**Removido no go-gelf-nats-server** (§1.2): sem rate-limit. As chaves `ratelimit.*` de G são parâmetros desconhecidos (§13.2).

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
  `cap(cru)` mais `ItemOverhead` = **288 B**: a struct `item.Item` (256 B,
  `unsafe.Sizeof`, já uma classe de alocação) e os 8 B do slot no canal do
  ring, 264 B arredondados para 288. Nem o número de SN (256 B) nem o de G (384
  B): o pacote `internal/item` é cópia literal do de G (§17.1), e a struct ainda
  traz o id de linha do `ring=sqlite`, o id de entrada do `ring=redis`, o
  ponteiro `done` e o `TLSInfo`, que este servidor nunca preenche; mas o canal
  `done` (112 B) que G cobra sempre só os outros rings de G criam, e nenhum
  ring daqui o cria (revisão dos cortes de 2026-10-31: com 384 a mesma
  `ring.max_bytes` guardava ~11 % menos itens que no GP, que já cobrava 288).
  `TestItemOverheadCoversTheStruct` arredonda a struct para a classe de
  alocação antes de comparar: falha se a struct crescer além do número, e
  também se o número passar da necessidade arredondada a 32 B (o que pegaria
  a volta do canal `done` cobrado à toa).
  Contar `len(cru)` deixava um documento de 8 193 bytes, copiado para o buffer de
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

Removido no go-gelf-nats-server (§1.2): o ring é só memória.

### 9.5 `ring.mode = redis`

Removido no go-gelf-nats-server (§1.2): o ring é só memória.

### 9.6 `ring.mode = none`

Removido no go-gelf-nats-server (§1.2): o ring é só memória, e a conversão nunca roda na recepção.

### 9.7 Combinações que degradam ou recusam

Removido no go-gelf-nats-server (§1.2): há uma combinação só, `memory` → `nats`.

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
   aos pools, `gelf_events_written_total += n`.
5. Próximo pedaço.

O teto de bytes existe porque o JLH chega a 12× o cru: `workers × batch × pior
JLH` passaria de 1 GiB.

A entrega fora dos workers — a reingestão do boot (§12.2) — passa por
`sink.Instrumented.Deliver`, que corta os envelopes em pedaços pelas mesmas
duas regras e aplica as classes de §10.2 pedaço a pedaço. Como nos workers,
`gelf_sink_batch_size` e `gelf_sink_batch_bytes` são observados uma vez por
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
  `gelf_sink_nats_duplicates_total`. Fora da janela, uma redelivery duplica.
  Em `core` não há deduplicação: o consumidor deduplica por `(uuid, step)`, como
  o JLH manda.
- **Contaminação** (§15.2): uma tentativa que termina por prazo ou cancelamento,
  ou qualquer erro depois da primeira publicação assíncrona, marca o pedaço, e
  nem o cru nem o buffer do pedaço voltam ao pool, nem quando a retentativa
  seguinte tem sucesso.

| Classe | Efeito |
|---|---|
| `ClassNetwork` | retenta; não muda o estado de saúde (§13.3) |
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`gelf_events_dropped_total{reason="sink_logic"}`), confirmado no ring |
| `ClassFatal` | FATAL imediato |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`gelf_events_written_total`, só os demais em `sink_logic`, e o log traz só os
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

Removido no go-gelf-nats-server (§1.2). A regra que estava aqui e que o código ainda cita — uma resposta **fatal** à sonda de saúde é FATAL na primeira checagem, sem passar pelos fusíveis — vale para o NATS e está em §13.3.

### 10.4 `sink.type = sqlite`

Removido no go-gelf-nats-server (§1.2).

### 10.5 `sink.type = postgres`

Removido no go-gelf-nats-server (§1.2): a fila `loghub.inbox` é o destino do `go-gelf-pg-server`.

### 10.6 `sink.type = mysql`

Removido no go-gelf-nats-server (§1.2).

### 10.7 `sink.type = nats` (padrão)

O único destino (não há chave `sink.type`, §1.2). `github.com/nats-io/nats.go`
com o pacote `jetstream` (a API atual). **Uma conexão por processo**,
multiplexada pelos workers e pelo beacon (§11), aberta preguiçosamente pela
primeira checagem de saúde; o `WaitReady` do boot é o laço de retentativa
(§13.1 passo 7). O nome do cliente é `glnats/<AgentName>@<Hostname>`, visível
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
RP/RN, 2026-10-14, a regra do `soleServer` do `go-gelf-server` e do
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

**Pool só de servidores que recusam** (registrado em 2026-10-14, D2 da revisão do syslog): com duas ou mais URLs e **todas** exigindo TLS, nunca há veredito — cada checagem é `network`, com o motivo no erro do `Health`, e o FATAL só vem dos fusíveis de §13.3; com os dois fusíveis desligados, nunca vem. É consequência da regra acima: o `LastError` é o do último servidor tentado, e saber que **nenhum** nó vai servir exigiria acompanhar a volta inteira do pool. O sinal é `gelf_sink_up` em 0 e o `/readyz`.

**Uma URL só cujo nome resolve a vários IPs** (registrado em 2026-10-14, D3 da revisão do RELP): o `nats.go` resolve o nome e tenta um IP, mas `nc.Servers()` tem uma entrada só, então a recusa de TLS do IP sorteado é veredito FATAL, embora outro IP do mesmo nome pudesse servir (um serviço *headless* do Kubernetes com nós configurados de jeitos diferentes). Não há como distinguir sem `SkipHostLookup`, que tiraria o rodízio entre os IPs: é limite aceito — os nós atrás de um nome devem ter a mesma configuração de TLS, ou entram como URLs separadas, e a regra do *pool* vale.

**Uma discagem que falha na reconexão não é logada** (porte do RP/RN,
2026-10-14; a forma do `go-gelf-server`): o `WARN` de desconexão, o
`/readyz` e `gelf_sink_up` já dizem o estado, e um `ERROR` amostrado a cada
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
`Close` não); a reconexão conta em `gelf_sink_nats_reconnects_total`, é
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
`gelf_sink_nats_duplicates_total`. Um futuro com erro `network` ou `fatal`
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

**Subject** (§3.8): sempre `loghub.<space>.gelf`, derivado do `space` do
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
| O stream existe | `js.Stream(sink.nats.stream)` (padrão `LOGHUB_GELF`); `10059` é fatal |
| O stream **cobre** o subject | casamento local de padrões (as regras de token do NATS: `*` é um token, `>` um ou mais, só no fim): algum subject do stream cobre `loghub.<space>.gelf`, com o `space` desta instância |
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
| `10077` cuja descrição é de um stream **cheio** com `discard: new` ("maximum messages exceeded", "maximum bytes exceeded", "maximum messages per subject exceeded") ou **fechando** ("store is closed", o `ErrStoreClosed` de `server/store.go` do nats-server 2.14.6, num restart do NATS; só "store closed", texto que nenhum servidor manda, era conferido, e o 10077 de um store fechando era `fatal` — revisão independente do `go-gelf-server`, 2026-10-13, C2) | `network`: backpressure, passa quando um consumidor confirma ou o servidor volta (revisão independente do `go-relplog-server`, portada; como `fatal` eram dump, exit 3 e duplicatas na reingestão) |
| `10158` (409: o mesmo `Nats-Msg-Id` ainda numa proposta Raft, na retentativa depois de um prazo num stream em cluster), `10167` (429: a fila de entrada do stream cheia) e todo outro erro da API com código 429 | `network`: passam sozinhos (antes, "outro 4xx" os fazia FATAL, dump e exit 3, talvez na frota inteira) |
| `ErrAuthorization`, `ErrAuthExpired`, `ErrAuthRevoked`, `ErrAccountAuthExpired`, e o texto do servidor no `CONNECT` com "authorization violation", "authentication expired" ou "authentication revoked" (o `errors.Is` do `nats.go` compara o texto inteiro, e "User Authentication Expired" nunca casava com o sentinela: o boot esperava o `boot_timeout` e saía com o motivo errado), `ErrConnectionClosed` com a conexão `CLOSED` (só reconectando, `network`: o `flushErr` acima), servidor que exige TLS (`errNATSServerRequiresTLS`), `ErrStreamNotFound` (`10059`), JetStream desligado (`10076`, `10039`), stream esperado não confere (`10060`), selado (`10109`), recursos (`10023`), outro armazenamento (`10077`: um registro que o store não aceita) | **`fatal`** |
| `ErrMaxPayload` (recusada no cliente: só aquela mensagem, §10.2), `ErrMaxBytesExceeded`, `10054`, `10097` | `logic` |
| violação de permissão (`ErrPermissionViolation`) numa publicação em `loghub.<space>.gelf` (de qualquer `space`), na API `$JS.API` ou na assinatura de `_INBOX.`, vista pelo `ErrorHandler` | **`fatal`**, pelo veredito (abaixo) |
| outro erro da API com código 4xx | `fatal` |
| outro erro da API com código 5xx, qualquer outro | `network` |

**Veredito.** O cliente só informa algumas recusas definitivas de forma
**assíncrona**: uma publicação negada por permissão não volta como erro dela (em
`jetstream` o futuro estoura o prazo e seria retentado para sempre com o sink
UP; em `core` a mensagem some e contava como escrita), e um `$JS.API.STREAM.INFO`
negado segurava o boot até o `boot_timeout`. Por isso o sink guarda um
**veredito** quando o `ErrorHandler` vê `ErrPermissionViolation` numa
publicação em `loghub.<space>.gelf` ou na `$JS.API`, e o `Health()` e **todo
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

Removido no go-gelf-nats-server (§1.2): todo pedaço vai ao NATS; não há destino de benchmark.

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

Removido no go-gelf-nats-server (§1.2).

#### 10.10.1 Cliente

Removido no go-gelf-nats-server (§10.10).

#### 10.10.2 Concorrência e conexões

Removido no go-gelf-nats-server (§10.10).

#### 10.10.3 Escrita

Removido no go-gelf-nats-server (§10.10).

#### 10.10.4 Desduplicação e at-least-once

Removido no go-gelf-nats-server (§10.10).

#### 10.10.5 Validação no boot

Removido no go-gelf-nats-server (§10.10).

#### 10.10.6 Durabilidade e `async_insert`: decisão do administrador do ClickHouse

Removido no go-gelf-nats-server (§10.10).

#### 10.10.7 Classificação de erro

Removido no go-gelf-nats-server (§10.10).

#### 10.10.8 `Health()`

Removido no go-gelf-nats-server (§10.10).

#### 10.10.9 Retenção

Removido no go-gelf-nats-server (§10.10).

#### 10.10.10 Métricas

Removido no go-gelf-nats-server (§10.10).

#### 10.10.11 Autenticação e TLS

Removido no go-gelf-nats-server (§10.10).

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
  (`gelf_beacons_total{result="error"}`) e logada em `WARN` com amostragem, um
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

**JSON Lines: um envelope JLH pronto por linha** (decisão 3 do `go-syslog-pg-server`, herdada por SN e daqui por GN,
herdada; lá, a opção recomendada):

```
{"uuid":"019a...","from":"0196...","space":"default","type":"gelf","level":6,"sid":59363...,"step":0,"event":{...},"payload":{...}}\n
{"uuid":"019a...", ...}\n
```

- **Cada linha é o JLH exatamente como o worker o teria escrito**: a conversão
  do dump é a do worker, com a config `[convert]` congelada no boot (§3.5), então
  os bytes são idênticos. Um JLH nunca contém `\n` cru (§3.3), e por isso `\n` é
  um separador seguro.
- **Legível por qualquer ferramenta**: `jq`, um script, qualquer software LogHub,
  não só este servidor. É o ganho da escolha; o custo é converter **todo** o ring
  pendente na hora do dump, pago uma vez por um processo que está saindo.
- Malformadas ficam de fora quando `convert.accept_malformed = false`: o worker
  as recusaria também (§6.6), e o dump não as ressuscita. Contam em
  `gelf_events_dropped_total{reason="malformed"}`. Se todas forem recusadas,
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
(`loghub.<space>.gelf`), o `Nats-Msg-Id` (`<uuid>.0`) e os `X-LogHub-*` saem
do **cabeçalho da própria linha** (§3.8). `type` precisa ser `"gelf"` e
`step`, `0`. Consequência: um dump sobrevive a uma troca de chaves `[convert]`
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
- todo número na gramática JSON da RFC 8259 (sem `+` inicial, sem zero à
  esquerda, dígitos depois do `.` e do expoente): os números do cabeçalho são
  inteiros simples, mas `payload.fields` e `payload.extra` guardam o **texto**
  dos números do emissor como chegaram (`1.50`, `1e400`, até `1e1000000`,
  §3.3), e uma porta que só aceitasse inteiros poria em quarentena um dump
  legítimo. **Desvio do SN** (inteiros de até 20 dígitos). Desde 2026-10-31 a
  varredura é o `numberLen` do GP, a mesma nos dois cortes;
- profundidade de aninhamento até 66: envelope, payload e `extra` (três níveis) mais os 63 abaixo da raiz de um documento GELF de 64 níveis (§6.2; a raiz é o próprio `extra`), porque os valores de `payload.extra` são copiados como vieram. Era 67 (`3 + 64`) até a unificação com o leitor do GP em 2026-10-31: o mesmo código nos dois cortes, e o nível a mais nunca aparecia numa linha que o conversor escreve (`TestParseLineRefusals/one_level_too_deep`). **Desvio do SN** (que tem 8: o payload `syslog` tem quatro níveis);
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
- `type = "gelf"` e `step = 0`, que o sink fixa no `X-LogHub-Type` e no `.0`
  do `Nats-Msg-Id`; `level` 0–7 (um nível GELF); `sid` ≤ 2^53 − 1;
- `event.uts` em 0..253402300799 e `event.sub` em 0..999999999; `event.iso`
  igual ao derivado de `uts`/`sub`;
- o instante do `uuid` (ms) igual ao de `event`: `uuid` e `event` são uma
  amostra só (§3.1);
- o `payload` cortado por posição (`jlh.Payload`, depois de `},"payload":`)
  igual ao `payload` que o parser JSON viu, e a linha terminando logo depois
  dele: quem corta o payload por posição vê o mesmo objeto que quem decodifica;
- `payload` um objeto cuja chave `short-message` de primeiro nível (a última,
  se repetida; comparação exata, sem caixa trocada) é uma string, que o
  contrato do `type=gelf` exige e o conversor sempre escreve (§3.2).

O motivo da recusa vai no log da quarentena e nomeia o tipo desta família:
`not type=gelf at step 0`, `level is not a GELF level (0..7)` (até
2026-10-31 o corte ainda dizia `syslog`; `TestParseLineRefusals`).

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
`loghub.<space da linha>.gelf`, que a validação do boot não conferiu (ela
prova só o subject do próprio `space`). Sem stream que o cubra, o JetStream não
responde e a entrega falha como `network` até `sink.boot_timeout`: o boot sai
com 3 e o arquivo fica. Coberto por outro stream, o `WithExpectStream` o recusa
(`10060`), fatal. O operador decide se cria o stream ou move o dump. Em `core`,
a mensagem vai para quem assinar aquele subject, ou para ninguém, com um `WARN`
por `space` estrangeiro, uma vez (§10.7).

**Os gauges dizem o que está no disco.** `gelf_emergency_files` é o número de
arquivos listados, posto antes do estado de boot que anuncia a reingestão;
`gelf_emergency_events_pending` não é tocado durante ela (posto no tamanho do
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
`ERROR` com a contagem exata, `gelf_events_dropped_total{reason="emergency_failed"} += n`,
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
    loghub.<space>.gelf, nunca cobre <ctl_namespace>.beacon, não selado,
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
11. abre os transportes: udp, tcp, http
12. /readyz 200
```

Um sinal que cancela o `bind` da porta admin (passo 5) ou de um transporte
(passo 11) — a resolução de um nome de host falha com "operation was
canceled" — é um boot interrompido, **130**, não um endereço ruim (1) (revisão
independente do `go-relplog-pg-server`, 2026-10-13, portada).
Com IP literal nenhuma resolução falha e os `bind` passam: o
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
`Hostname` compõem o nome do cliente NATS, `glnats/<AgentName>@<Hostname>`
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
| `listen.udp`, `listen.tcp` ou `listen.http` fora de `host:port` (`:12201` serve); `*_listeners` fora de 1–256 | exit 1. UDP e TCP podem dividir a porta (são protocolos distintos; 12201 nos dois é o padrão) |
| `listen.tcp` e `listen.http` no mesmo endereço TCP (mesma regra de `sameStreamSocket` da linha de `admin.bind`) | exit 1, nomeando as duas chaves: com `SO_REUSEPORT` o kernel aceitaria o duplo `bind` e entregaria molduras `\0` ao servidor HTTP |
| `listen.udp_reassembly_max_bytes` fora de 1 MiB–4 GiB; `listen.udp_reassembly_max_messages` fora de 1–1 048 576 | exit 1 (§5.2) |
| `http.read_header_timeout`, `http.read_timeout` ou `http.idle_timeout` zero; `http.max_conns`, `http.max_conns_per_ip` ou `http.max_requests_per_conn` negativo; `http.bulk_max_bytes` fora de 1 KiB..64 MiB (conferido mesmo com `http.bulk=false`) | exit 1 (§4.5; os prazos são a defesa contra slowloris; `0` nas conexões e no teto de requisições é "sem limite") |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| `admin.enabled = true` com `admin.bind` e `listen.tcp` ou `listen.http` no mesmo endereço TCP: a mesma porta com o mesmo literal IP (o IPv4 mapeado em IPv6 conta como o IPv4), um curinga (`""` ou um literal não especificado: `0.0.0.0`, `::`, `::0`) de um lado, ou um **nome** de host de qualquer lado | exit 1, nomeando os dois endereços. Nenhum nome é resolvido na validação: um nome na mesma porta pode resolver ao outro endereço (`localhost` e `127.0.0.1`) e conta como colisão. Para dividir a porta, dois literais IP distintos; a porta `0` nunca colide; `listen.udp` não colide (outro protocolo). Antes a colisão só aparecia ao abrir o transporte, com a porta admin já tendo servido o boot inteiro, e no macOS/BSD nem isso: o `SO_REUSEADDR` deixa `127.0.0.1:P` e `*:P` coexistirem, e a porta admin tomava em silêncio as conexões TCP de `localhost`. A regra é a do `sameStreamSocket` do `go-syslog-server`, a mesma de G (porte do RP/RN, 2026-10-14; `TestAdminBindCollidesWithListen`). O curinga é reconhecido pelo **valor**, não pela grafia: `""` ou qualquer literal com `IsUnspecified()` (`0.0.0.0`, `::`, `::0`, `0:0::0`); e a **porta é comparada como número**, lida como o `net.Listen` a lê (`net.LookupPort`): `0514` é `514`, e um nome de serviço (`syslog`, `http`) vale o número do banco de serviços; uma porta que não é número nem serviço conhecido é exit 1 aqui, nomeando as duas chaves, em vez de falhar no bind (revisão independente de 2026-10-14, D1 do syslog e D4 do RELP: `[::0]:P` contra `127.0.0.1:P` e `0514` contra `514` passavam como endereços distintos; `TestSameStreamSocketReadsValuesNotSpellings`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `proxy.protocol ≠ off` sem `proxy.trusted` | exit 1 |
| `space` fora de `^[a-z0-9-]+(\.[a-z0-9-]+)*$` | exit 1 (a lib aceita `a..b`, `.x` e `x.`; segmento vazio não serve à convenção LogHub, e viraria um token vazio no subject). Acima de 64 caracteres a própria lib já sai com 111 antes |
| `from` fora do UUID canônico em minúsculas | exit 1 |
| `convert.max_message_bytes` < 1 KiB ou > 16 MiB; `convert.keep_raw` fora de `malformed`, `always` | exit 1 (§5.5, §6.9) |
| `ring.max_bytes` abaixo do custo de uma mensagem do tamanho máximo: a classe do pool de `convert.max_message_bytes` + 288 B (`item.PooledCap(n) + ring.ItemOverhead`, §9.2, §15.1; 131 360 B com o padrão de 128 KiB) | exit 1, nomeando a chave e o piso: esse documento nunca caberia num ring vazio; no TCP a conexão ficaria parada até o `tcp.backpressure_timeout` e a mensagem seria descartada, a cada uma, e no UDP descartada sempre como `ring_full`, com o ring vazio. Porte do RP/RN, 2026-10-14 (`TestRingMaxBytesFitsTheLargestMessage`) |
| **Tetos** (revisões independentes do `go-relplog-pg-server` e do `go-relplog-nats-server`, 2026-10-13, portadas): `ring.capacity_items` > 16 777 216, `sink.workers` > 256 (todos dividem a única conexão NATS), `sink.batch` > 65 536, `sink.batch_max_bytes` > 256 MiB | exit 1. Sem eles, um erro de digitação de poucos dígitos derrubava o boot em pânico (`makechan`), alocava um gigabyte por `Dequeue` ou dava a volta no `int` do `PublishAsyncMaxPending` (`2 × workers × batch`, §10.7). Os números são folgados: só barram o erro de digitação |
| `ring.capacity_items`, `ring.max_bytes`, `sink.workers`, `sink.batch`, `sink.batch_max_bytes`, `sink.max_retries` abaixo de 1; `ring.high_water` fora de (0, 1]; `log.access_sample` fora de [0, 1]; `tcp.max_conns*` ou `http.max_conns*` negativo; `retry_backoff_max < retry_backoff_min` | exit 1 |
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
| parâmetro desconhecido na linha de comando ou no INI | exit 1. Isto inclui toda chave de G que saiu (`ring.mode`, `sink.type`, `listen.tls`, `listen.https`, `[tls]`, `[filter]`, `[ratelimit]`, `sink.nats.tls`, `tls_ca`, `tls_insecure`, `[sink.postgres]`...): um INI do `go-gelf-server` não sobe aqui em silêncio; o erro diz que a chave é do `go-gelf-server` |
| linha do INI que não é `[seção]` nem `chave = valor` (um cabeçalho sem `]` inclusive); argumento da linha de comando que não começa com `-` | exit 1, nomeando o número da linha ou só o que vem antes do `=` do argumento, **nunca o conteúdo**: `password: x` (sem `=`) ou `sink.nats.password=x` (sem `--`) iam inteiros para o stderr do contêiner |
| flag sem valor seguida de outra flag (`--sink.workers --sink.nats.password=x`, um `$WORKERS` vazio num script); valor depois de um espaço (`--sink.nats.password= x`, `--sink.nats.password = x`, `--sink.nats.password= -x`); valor que o shell partiu num espaço (`--sink.nats.password=$P`, com espaço em `$P`); valor que começa com `--` na forma separada (`--sink.nats.password --x`) | exit 1, nunca com o valor: um argumento que começa com `--` (ou `-` com o nome de um parâmetro ou de um *switch*) nunca é tomado como valor de outra flag, e só é nomeado se for uma flag deste servidor; o argumento depois de um valor vazio ou de um `=` sozinho não é citado, nem como nome de flag desconhecida; uma palavra solta depois de qualquer valor não é citada. Um valor que começa com `-` vai depois do `=`. A cauda partida que começa com traço depois de um valor **não** vazio (`--sink.nats.password=a -b`) só é nomeada quando tem a forma `<seção>.<chave>` de uma seção que este servidor tem, ou que saiu de G (`--sink.nats.subjet`, `--filter.min_level`): um erro de digitação dentro de uma seção conhecida continua nomeado, e `-hunter2` ou `--x=y` não (`knownSectionOf`, do `go-relplog-server`, portado em 2026-10-13). Um `-h`/`-v` de um traço depois de uma flag que quer valor não é nomeado (é como um valor de dois caracteres começando com traço se parece); `--help` é (o parser do `go-webhook-nats-server`, portado em 2026-10-11) |
| `--help`, `--version` ou `--dump-config` com valor (`--dump-config=false`) | exit 1, sem citar o valor: um *switch* não tem valor, e um valor ignorado pode dizer o contrário (despejava e saía 0) |
| `--config=` vazio | exit 1: caía no `GELF_CONFIG`, o contrário do que diz |
| nome de parâmetro com algo colado depois (`--sink.nats.passwordhunter2`, `…password-hunter2`), ou com caracteres que nome nenhum tem (`password: hunter2 = x`, `--sink.nats.password:hunter2`) | exit 1 como parâmetro desconhecido, **sem** citar o nome: o `=` esquecido põe o valor dentro dele |
| no INI, chave desconhecida logo depois de um segredo (`password = abc` e, na linha seguinte, `ffd3a9c1b2=`), ou cuja seção não existe aqui nem no pai, ou que não tem a forma de uma chave (`^[a-z0-9_]{1,64}$`) | exit 1 nomeando só a linha: é o resto de um valor que quebrou de linha, e o erro vai ao stderr e, num `SIGHUP`, ao log |
| BOM UTF-8 no começo do INI | ignorado (alguns editores do Windows o gravam; mantido, a linha 1 era "expected 'key = value'" sem nada visível) |
| variável de ambiente de G que este servidor não tem (`GELF_LISTEN_TLS`, `GELF_LISTEN_HTTPS`, `GELF_TLS_*`, `GELF_FILTER_*`, `GELF_RATELIMIT_*`, `GELF_SINK_TYPE`, `GELF_RING_MODE`, `GELF_SINK_NATS_TLS*`, as dos outros sinks e rings: 93 nomes) | **um** `WARN` que nomeia todas e diz que são ignoradas (`internal/config/removed.go`, §16.2). Não é erro porque um mesmo `.env` pode alimentar mais de um servidor da família; sem o aviso, o env é lido só pelos nomes do registry e elas sumiriam em silêncio, e quem trocou um servidor pelo outro acreditaria ter um listener TLS, um filtro ou um rate-limit |

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
- **FATAL é terminal.** Toda transição e o gauge `gelf_sink_up` são escritos
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
        do boot (http, tcp, udp); os documentos já lidos terminam de entrar
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
  recepção trata como shutdown (TCP fecha a conexão sem ler adiante; UDP
  descarta; HTTP responde `503`), contado em `gelf_messages_dropped_total{reason="shutdown"}`, e
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
- **HTTP no shutdown:** o contexto de recepção cancelado transforma toda requisição que espera o ring em `503` com `Retry-After` (`reason="shutdown"`), e a conexão fecha.
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

`admin.bind`, padrão `127.0.0.1:9104`, **o mesmo do `go-gelf-server`**: os
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

Prefixo `gelf_`, **o mesmo de G**: um painel do `go-gelf-server` lê este
servidor. **Nenhum label deriva de conteúdo controlado pelo emissor** (nada de
`host`, nome de campo, IP); todo label tem conjunto finito conhecido em
compilação (`internal/obs/metrics.go`). O `level` **não** é label: a
distribuição por nível é pergunta para quem consome o stream.

| Métrica | Tipo | Labels |
|---|---|---|
| `gelf_messages_total` | counter | `transport` (udp, tcp, http): documentos recebidos do fio. No UDP com chunks, um por mensagem remontada (ou descartada como mensagem), não por chunk |
| `gelf_messages_accepted_total` | counter | `transport` |
| `gelf_messages_dropped_total` | counter | `reason` (ring_full, sink_down, malformed_frame, proxy, empty, shutdown, too_large, decompress, chunk_invalid, chunk_timeout, chunk_overflow). No UDP é o **único** registro de que algo se perdeu |
| `gelf_message_bytes` | histogram | — (o documento **descomprimido**, `size`) |
| `gelf_confirm_duration_seconds` | histogram | — (do documento completo à garantia do ring; tcp e http, só aceitos: o UDP não tem o que confirmar) |
| `gelf_convert_total` | counter | `format` (gelf, malformed) |
| `gelf_convert_warnings_total` | counter | `code` (os 25 de §6.8) |
| `gelf_convert_duration_seconds` | histogram | — (a conversão de um documento no worker de dreno) |
| `gelf_conns_active`, `gelf_conns_total` | gauge, counter | `transport` (tcp, http) |
| `gelf_conns_refused_total` | counter | `transport` (tcp, http), `limit` (global, per_ip); `global` conta cada vez que o accept estaciona no teto, um limite inferior, porque o teto global bloqueia em vez de recusar |
| `gelf_proxy_headers_total` | counter | `result` (v1, v2, local, absent, invalid) |
| `gelf_backpressure_seconds_total` | counter | `transport` (tcp): o HTTP não segura, responde `503` |
| `gelf_udp_receive_errors_total` | counter | — (erros do próprio socket; o descarte do kernel por `SO_RCVBUF` pequeno não aparece aqui) |
| `gelf_udp_read_buffer_bytes` | gauge | — (o `SO_RCVBUF` efetivo, lido de volta) |
| `gelf_udp_reassembly_bytes` | gauge | — (bytes segurados por mensagens chunked incompletas, contra `listen.udp_reassembly_max_bytes`, §5.2) |
| `gelf_udp_chunk_duplicates_total` | counter | — (chunks ignorados porque o `seq` já tinha chegado) |
| `gelf_ring_items`, `gelf_ring_bytes` | gauge | — |
| `gelf_ring_capacity_items`, `gelf_ring_capacity_bytes` | gauge | — |
| `gelf_sink_batch_size`, `gelf_sink_batch_bytes` | histogram | — (por pedaço) |
| `gelf_sink_duration_seconds` | histogram | — |
| `gelf_sink_health_duration_seconds` | histogram | — |
| `gelf_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); o cancelamento do shutdown não conta. `fatal` conta **toda** recusa fatal do destino, mesmo durante um cancelamento (boot cancelado, shutdown): ela não nasce dele, é um veredito real, e o processo sai logo depois. Só o `network` cortado pelo cancelamento deixa de contar, porque o cancelamento **causa** erros de rede (prazo, conexão fechada). Regra, não pendência (porte do RP/RN, decidido em 2026-10-14). |
| `gelf_sink_up` | gauge | — |
| `gelf_sink_nats_duplicates_total` | counter | — (confirmações com `PubAck.Duplicate`: uma redelivery dentro da `duplicate_window`, §10.2) |
| `gelf_sink_nats_reconnects_total` | counter | — |
| `gelf_beacons_total` | counter | `result` (ok, error) (§11) |
| `gelf_events_written_total` | counter | — |
| `gelf_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero, como todo label de conjunto fechado (um `rate()` precisa da série antes do primeiro evento) |
| `gelf_emergency_files`, `gelf_emergency_events_pending` | gauge | — (arquivos e envelopes pendentes no disco, recontados do diretório no fim da reingestão e somados a cada dump; durante a reingestão, `files` é o número de arquivos listados e `events_pending` não muda, §12.2) |
| `gelf_build_info` | gauge | `version`, `commit`, `go` |
| `gelf_config_info` | gauge | `ring_mode` (sempre `memory`), `sink_type` (sempre `nats`), `transports` |
| `gelf_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

Saíram, com o que as produzia (o corte de SN sobre S, aplicado a G):
`reason` `ring_unavailable`, `filter` e `ratelimit`; os labels `tls` e `https`
em `transport`; `gelf_tls_handshake_errors_total`; `gelf_ring_quarantined_total`
e `gelf_ring_redis_claimed_total`; as séries de `postgres`, `sqlite`, `mysql`,
`redis` e `clickhouse`; `gelf_ratelimit_buckets`. Uma série que nunca pode subir
não é exposta: diria ao operador que existe uma medição onde não existe nenhuma
(`TestAdminSurface` confere o conjunto fechado de labels).

**Fronteiras de histograma** são estáticas, idênticas em toda instância e
**pré-formatadas como string** na inicialização: emitir `0.01` num scrape e
`0.010` no seguinte cria duas séries e quebra todo quantil.

### 14.2 Logs

`slog`, `json` por padrão (`text` disponível). **Nenhum log síncrono por mensagem
no caminho padrão.** `log.access = true` liga log por mensagem com
`log.access_sample` (0.01); num coletor de logs isso é logar log. Avisos de anomalia
(conversão, PROXY, falha de aceite) são **amostrados**, um a cada 30 s por tipo,
com o contador exato em `/metrics`: um emissor quebrado não inunda o log do
coletor. Nenhum log em `INFO` ou acima carrega o payload de uma mensagem.

---

## 15. Memória e runtime

### 15.1 O documento cru

Buffers de pools por classe: 2 KiB, 8 KiB e 64 KiB. Acima de 64 KiB o buffer é
alocado direto e **nunca** volta a um pool: pooling de pior caso faz o pool
segurar o pior caso para sempre. Com o padrão de 128 KiB de
`convert.max_message_bytes` (§5.5) um documento maior que 64 KiB existe (um
stack trace em `full_message`), mas é raro: o documento GELF típico de uma
biblioteca ou do driver do Docker tem de 300 bytes a 2 KiB.

**Memória por conexão, fora do ring.** Uma conexão TCP viva segura o
leitor de 4 KiB e o buffer de quadro (até 8 KiB quando ociosa; o de um quadro
maior enquanto os quadros seguintes já estão no leitor, ou com alguns bytes do
próximo quadro no leitor e o par calado), ociosa ou não; as classes do buffer
de quadro estão em §5.3. Não entram em `ring.max_bytes`: o teto delas é
`tcp.max_conns`, e com `0` é o `GOMEMLIMIT` (§15.4) que segura
um enxame de conexões caladas.

**Memória por requisição HTTP.** Cada requisição em voo segura o corpo como
chegou do fio (um buffer que cresce por classes com o que **chega**, até
`L + 16 KiB + 1`, nunca pelo `Content-Length`, §4.5) e o documento
descomprimido (até `L + 1`); com `http.bulk`, `B + 16 KiB` e `B` (§4.5.1). Os
dois vêm de pools; um buffer acima de 64 KiB não volta ao pool (seguraria `L`
por P ocioso). Uma conexão keep-alive serve uma requisição de cada vez, então
o teto é `http.max_conns` e, com `0`, o `GOMEMLIMIT`; entre requisições ela
não segura buffer.

**Estado de remontagem UDP.** Os chunks de mensagens ainda incompletas ficam em
memória, cada um num buffer do tamanho dele, até completar ou vencer o prazo de
5 s (§5.2). O teto é **global**, `listen.udp_reassembly_max_bytes` (64 MiB,
contando os dados de cada chunk mais 64 bytes de estrutura) e
`listen.udp_reassembly_max_messages` (16 384); o uso aparece em
`gelf_udp_reassembly_bytes`. Não entra em `ring.max_bytes`: é memória da
recepção, de documentos que ainda não existem. Uma mensagem completa é
concatenada e descomprimida num documento novo, e só ele entra no ring.

**Teto pequeno demais.** Um documento de `n` bytes custa ao ring em memória a
classe do pool de `n` + 288 B (§9.2): 2 KiB, 8 KiB ou 64 KiB, e acima da maior
classe o próprio `n`. O piso de `ring.max_bytes` é o custo de um documento do
tamanho máximo, `item.PooledCap(convert.max_message_bytes) + ring.ItemOverhead`
(131 360 B com o padrão de 128 KiB), e um valor menor é recusado na validação
(§13.2, exit 1). O piso da **menor** classe não basta: um
ring de 4 KiB aceitaria os documentos curtos e recusaria **sempre** qualquer
documento de 3 KiB, mesmo vazio — no TCP a conexão daquele emissor parava até
o `backpressure_timeout` a cada um, e no HTTP todo `POST` dele levaria `503`.
Com o piso, todo documento que o teto de §5.5 deixa passar cabe num ring vazio
(porte do RP/RN, 2026-10-14).

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
- Env: `GELF_<SEÇÃO>_<CHAVE>` em maiúsculas, `.` vira `_`
  (`GELF_SINK_NATS_URLS`), com as exceções de §16.2.
- INI: seção `[ring]`, chave `capacity_items`.
- `--config=/caminho.ini` (env `GELF_CONFIG`) indica o arquivo.

**Os mesmos nomes de G.** O prefixo `GELF_` e cada chave que ficou têm o
nome que tinham no `go-gelf-server`: os dois são da mesma família, e um
operador pode trocar um pelo outro mantendo o env das chaves comuns.

### 16.2 Registry único, e os nomes de env fixados pelas skills

Uma **tabela** (`internal/config/registry.go`) declara cada parâmetro (seção,
chave, tipo, padrão, descrição, se é recarregável, nome de env). Dela saem o
INI, as flags, o env, o `--help`, o `--dump-config` e a validação. Um parâmetro
novo entra só ali. Sem `gopkg.in/ini.v1` e sem `spf13/pflag`.

Nomes de env que **não** seguem a regra, porque uma skill os fixou (não há
exceção de seção: `[convert]` é `GELF_CONVERT_<CHAVE>`, como em G, decisão 7):

| Seção | Env | Origem |
|---|---|---|
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

**Chaves de G que saíram.** Na linha de comando ou no INI são parâmetro
desconhecido, exit 1, e o erro diz de onde a chave vem ("go-gelf-server has
it", `removedParams`, a mesma lista do env em grafia pontuada). No ambiente, onde o registry lê só os próprios nomes e
nunca varre o resto, elas sumiriam em silêncio; por isso o boot confere a lista
de `internal/config/removed.go` (as 93 de G que não existem aqui: TLS e HTTPS,
filtro, rate-limit, `sink.type`, `ring.mode`, `ring.max_inflight`,
`sink.nats.tls*` e as dos outros sinks e rings) e emite **um** `WARN` que nomeia todas as definidas e diz
que são ignoradas (§13.2).

Durações aceitam a sintaxe do Go (`30s`, `5ms`). Um inteiro puro, em segundos,
só em `loghub.beacon_interval`, porque é assim que a skill define
`LOGHUB_BEACON_INTERVAL`. Tamanhos aceitam
**só** `KiB`, `MiB`, `GiB` (sem diferença de caixa) e inteiros puros em bytes;
`KB`, `MB`, `K` e `M`, que a standard aceitava, são recusados.

### 16.3 Recarga por `SIGHUP`

Recarrega **só** o que é seguro trocar a quente e não muda os bytes do JLH:

- `log.level`, `log.access`, `log.access_sample`

(Em G entravam também `filter.*`, `ratelimit.*` e `sink.sqlite.keep_files`,
que saíram, §1.2.)

Toda chave de `[convert]` **exige restart**: ela muda a conversão, e o JLH de um
item não pode depender de **quando** ele foi drenado (§3.5). A identidade fica
congelada.

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
que manda (`env GELF_LOG_LEVEL`), mesmo quando o resultado é "configuration
unchanged": é exatamente o caso em que o operador editou o arquivo e não entende
por que nada mudou. Por isso o compose não define chaves recarregáveis por env:
elas vêm do INI montado (`docker/conf/gelf.ini`).

Um arquivo inválido no `SIGHUP` não aplica nada: `ERROR`, e o servidor segue com
a config anterior. Fora da janela entre o fim do boot e o início do shutdown, o
`SIGHUP` é logado e ignorado (§13.4).

### 16.4 Exemplo

```ini
[server]
data_dir = /data

[listen]
udp  = :12201
tcp  = :12201
http = :12202

[convert]
max_message_bytes = 128KiB
keep_raw          = malformed

[ring]
capacity_items = 65536
max_bytes      = 256MiB

[sink]
workers = 8

[sink.nats]
urls        = nats://nats-1.intranet:4222,nats://nats-2.intranet:4222
credentials = /etc/gelf/gelf.creds
mode        = jetstream
stream      = LOGHUB_GELF

[loghub]
beacon_interval = 15

[admin]
bind = 0.0.0.0:9104

[log]
level = info
```

Usuário e senha (ou o token), no lugar do `.creds`, vão por env
(`GELF_SINK_NATS_PASSWORD`, `GELF_SINK_NATS_TOKEN`) ou dentro da
`LOGHUB_EB_URL` — nesse caso sem `urls`, `username`, `password` e `token` no INI
(§10.9).

### 16.5 Tabela de parâmetros

A fonte é `internal/config/registry.go`; o `--help` imprime a mesma tabela.

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `shutdown.drain_timeout`,
`tcp.backpressure_timeout`, `proxy.header_timeout`, `http.read_header_timeout`
e `http.read_timeout` — a validação recusa `0` com
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
| `udp` | `:12201` | GELF UDP: datagramas e chunks (§4.2, §5.2) |
| `udp_listeners` | `1` | 1–256; `>1` habilita `SO_REUSEPORT`; o único jeito de passar de um núcleo |
| `udp_read_buffer` | `8MiB` | `SO_RCVBUF`; o efetivo é lido de volta e exposto |
| `udp_reassembly_max_bytes` | `64MiB` | teto global de bytes em remontagem, 1 MiB–4 GiB (§5.2) |
| `udp_reassembly_max_messages` | `16384` | teto global de mensagens chunked abertas, 1–1 048 576 (§5.2) |
| `tcp` | `:12201` | GELF TCP, moldura `\0` (§5.3) |
| `tcp_listeners` | `1` | 1–256; `>1` habilita `SO_REUSEPORT` |
| `http` | `:12202` | `POST /gelf`: um documento por requisição, ou um por linha com `http.bulk` (§4.5, §4.5.1) |

#### `[tcp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `idle_timeout` | `120s` | `0` = nunca |
| `max_conns` | `0` | teto do transporte, somando os listeners; bloqueia no accept; `0` = ilimitado |
| `max_conns_per_ip` | `0` | por IP do par TCP (antes do PROXY); fecha na hora; `0` = ilimitado |
| `backpressure_timeout` | `30s` | quanto tempo parar de ler antes de fechar (§5.7) |

#### `[tls]`

Removido no go-gelf-nats-server (§1.2).

#### `[http]`

| Chave | Padrão | Descrição |
|---|---|---|
| `read_header_timeout` | `10s` | prazo da linha de requisição e dos cabeçalhos; nunca `0` |
| `read_timeout` | `30s` | prazo da requisição inteira, corpo incluído; nunca `0` |
| `idle_timeout` | `60s` | espera de uma conexão keep-alive pela próxima requisição; nunca `0` |
| `max_requests_per_conn` | `0` | a resposta da N-ésima requisição leva `Connection: close`; `0` = sem teto; negativo é exit 1 |
| `bulk` | `false` | um documento por linha do corpo (§4.5.1) |
| `bulk_max_bytes` | `4MiB` | com `bulk`, teto do corpo descomprimido; 1 KiB..64 MiB, e abaixo de `convert.max_message_bytes` vale ele |
| `max_conns` | `0` | teto do listener http; bloqueia no accept; `0` = ilimitado |
| `max_conns_per_ip` | `0` | por IP do par TCP (antes do PROXY); fecha na hora; `0` = ilimitado |

#### `[proxy]`

| Chave | Padrão | Descrição |
|---|---|---|
| `protocol` | `off` | `off` \| `optional` \| `required`, no TCP e no HTTP (§4.6) |
| `trusted` | `127.0.0.1/32,::1/128` | CIDRs dos balanceadores; largo demais permite forjar `src-ip` |
| `header_timeout` | `5s` | prazo do cabeçalho PROXY |

#### `[convert]` — exige restart (§16.3)

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `keep_raw` | `GELF_CONVERT_KEEP_RAW` | `malformed` | `malformed` \| `always` (§6.9, decisão 4) |
| `max_message_bytes` | `GELF_CONVERT_MAX_MESSAGE_BYTES` | `128KiB` | teto do documento descomprimido; acima, recusa contada, nunca truncamento (§5.5); 1 KiB a 16 MiB; pior JLH = 6 × isto + 8 KiB |
| `accept_malformed` | `GELF_CONVERT_ACCEPT_MALFORMED` | `true` | `false` descarta `malformed` no worker e no dump |

#### `[filter]` — recarregável

Removido no go-gelf-nats-server (§1.2).

#### `[ratelimit]` — recarregável

Removido no go-gelf-nats-server (§1.2).

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `capacity_items` | `65536` | teto de itens, 1–16 777 216; com documentos pequenos é o que pega primeiro |
| `max_bytes` | `512MiB` | teto de bytes: o `cap` de cada buffer do documento + 288 B por item (§9.2); o JLH não conta. No mínimo o custo de um documento de `convert.max_message_bytes` (a classe do pool + 288 B, §13.2) |
| `high_water` | `0.80` | fração de qualquer teto acima da qual o `/readyz` falha |

Não há `mode` (sempre `memory`) nem `max_inflight` (era do `ring=none`).

#### `[ring.sqlite]`

Removido no go-gelf-nats-server (§1.2).

#### `[ring.redis]`

Removido no go-gelf-nats-server (§1.2).

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

Não há `subjects` (o subject é sempre `loghub.<space>.gelf`, §3.8) nem chaves de
TLS (`tls`, `tls_ca`, `tls_insecure`): o cliente é sempre texto claro (§1.2).

| Chave | Padrão | Descrição |
|---|---|---|
| `urls` | `nats://127.0.0.1:4222` | lista separada por vírgula (um cluster); só `nats://`, com host e sem caminho, consulta ou fragmento; `%`, `/`, `#`, `?` e `,` de um usuário, senha ou token escapados; *userinfo* na URL exclui as chaves de autenticação abaixo; redigido no `--dump-config` |
| `username`, `password` | `""` | autenticação por senha; a senha é redigida |
| `token` | `""` | autenticação por token; redigido |
| `credentials` | `""` | caminho de um `.creds` (JWT + semente NKey); um método de autenticação só |
| `mode` | `jetstream` | `jetstream` (`PubAck` por mensagem) \| `core` (*best effort*, `WARN` no boot) |
| `stream` | `LOGHUB_GELF` | o stream que precisa cobrir o subject; validado no boot, nunca criado; ignorado em `core` |

#### `[sink.redis]`

Removido no go-gelf-nats-server (§1.2).

#### `[sink.sqlite]`

Removido no go-gelf-nats-server (§1.2).

#### `[sink.postgres]` e `[sink.mysql]`

Removido no go-gelf-nats-server (§1.2).

#### `[sink.clickhouse]`

Removido no go-gelf-nats-server (§1.2).

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
| `admin.bind` | `127.0.0.1:9104` | a mesma porta do servidor pai (`go-gelf-server`): não rode os dois no mesmo host sem trocar (§14) |
| `log.level` | `info` | `debug` \| `info` \| `warn` \| `error` (recarregável) |
| `log.format` | `json` | `json` \| `text` |
| `log.access`, `log.access_sample` | `false`, `0.01` | log por mensagem, amostrado (recarregáveis) |
| `shutdown.pre_stop_delay` | `0s` | continua recebendo depois do `SIGTERM`; `0` = sem espera |
| `shutdown.drain_timeout` | `30s` | prazo do dreno do ring no shutdown |

### 16.6 Escrita em disco: uma raiz só

Tudo o que o serviço grava — a identidade (`agent_uuid`, `machine_id`, gravados
pelo `go-loghub-ident`) e `emergency/` — fica sob `server.data_dir`, e no
contêiner essa raiz é `/data` (o compose sobe com `read_only: true`), sem
exceção (não há socket unix na família GELF).

Caminhos fixos do contêiner: `/app` guarda o binário (dono `root`), `/data` é a
única área gravável, `/etc/gelf` é o INI em `:ro` (o compose monta
`docker/conf/` ali e define `GELF_CONFIG=/etc/gelf/gelf.ini`, que traz as
chaves recarregáveis, §16.3), `/tmp` é tmpfs. `WORKDIR /data`; o binário é
invocado por `/app/go-gelf-nats-server`. Não há `/etc/gelf/tls`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

---

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-gelf-nats-server/            ESTE módulo (GN)
  go-gelf-server/                 G, o pai do comportamento GELF: dois eixos, TLS, filtro, rate-limit
  go-gelf-pg-server/              GP, o irmão: o mesmo corte, com a fila loghub.inbox do PostgreSQL
  go-gelf-injector/               GI, cliente de carga e funcional
  go-syslog-nats-server/          SN, o pai estrutural: o corte que este repete
  go-syslog-server/               S, o pai de G e de SN
```

Módulo independente. Nada é importado de outro módulo do monorepo:
compartilhar pacotes faria os módulos evoluírem amarrados. **Cópia literal de
G**, só com o module path trocado: `internal/convert`, `internal/item`,
`internal/jlhcheck` e `internal/receiver/{chunks,decompress,framing}.go`, com os
testes, o corpus e o `testdata/fuzz`. O `internal/receiver/http.go` é o de G
sem o HTTPS (`NewHTTPS`, o `tls.Config`, a sessão), o mesmo corte do GP. Mudou
um deles em G, a mudança vem para cá no mesmo dia, ou fica registrada como "a
portar" (§22.1). Tags `go-gelf-nats-server/vX.Y.Z`.

### 17.2 Layout interno

```
cmd/go-gelf-nats-server/main.go  flags que encerram, lhident.Initialize(), chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão, admin
  config/     registry único -> INI + flags + env + --help + dump; validação;
              LOGHUB_EB_URL (só nats://); o aviso das variáveis de G (removed.go)
  control/    o heartbeat beacon (§11), e só ele
  ident/      Identity (congelada no boot) e a gramática de space          [lógica pura]
  jlh/        escritor JSON por append, Event, cabeçalho do envelope, UUID,
              Payload (o corte por posição)                                [lógica pura]
  convert/    documento GELF -> payload type=gelf -> JLH (cópia de G)    [lógica pura]
  item/       Item, Head, codec do cabeçalho, pools (cópia de G; o codec
              só serve ao FuzzHead e aos testes aqui)                     [lógica pura]
  jlhcheck/   SÓ TESTE: schemas (jlh-gelf-payload.schema.json) e oráculo
  receiver/   datagram.go (UDP, pktinfo), chunks.go, decompress.go,
              framing.go (\0), stream/tcp.go, http.go (POST /gelf),
              proxy.go, limits.go, ingest.go
  ring/       interface Ring + memory
  sink/       interface Sink + nats, workers, chunk, health, instrumented
  emergency/  dump JSON Lines, ParseLine e o leitor em fluxo da reingestão
  obs/        métricas Prometheus próprias + slog
```

`convert/`, a remontagem, a descompressão, a moldura, o leitor do PROXY em
`receiver/` e `emergency.ParseLine` são os parsers de entrada adversária. Os
alvos de fuzz estão em §18.3.

### 17.3 Dependências

**Três diretas em produção, e ponto.**

| Dependência | Para quê |
|---|---|
| `github.com/nats-io/nats.go` v1.53.1 | o sink, com o pacote `jetstream`, e o beacon por NATS core |
| `github.com/patrickbrandao/go-loghub-uuidv7` v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` v0.5.2 | identidade (sem dependência nova: usa a `go-loghub-uuidv7`) |

Transitivas que o `nats.go` põe no binário (`go list -deps ./cmd/go-gelf-nats-server`):
`github.com/nats-io/nkeys` v0.4.15 (NKey e `.creds`), `github.com/nats-io/nuid`
v1.0.1, `github.com/klauspost/compress` v1.18.5 (a compressão do transporte
websocket do cliente, compilada mesmo com `ws://` recusado) e
`golang.org/x/crypto` v0.49.0 (ed25519/nacl do `nkeys`).

Só em teste: `github.com/santhosh-tekuri/jsonschema/v6` (contratos, §18.2).
O `go.mod` é o de SN, sem mudança (`go mod tidy` não tira nem põe nada): gzip,
zlib, deflate e o servidor HTTP são da stdlib, e o parser JSON é próprio (§6.2).

G tem oito; saíram `rueidis`, `modernc.org/sqlite`, `pgx`,
`go-sql-driver/mysql` e `clickhouse-go/v2`, com as transitivas de cada um. O
binário tem **12,2 MB**, contra **30,5 MB** de G e 12,1 MB de SN (o mesmo
`go build` simples, `darwin/arm64`, Go 1.27.2, 2026-10-31); com `-s -w`, 8,4 MB
contra 21,0 MB. A imagem (`docker/Dockerfile`, `-s -w`, alpine) tem 24,5 MB,
contra 43,8 MB da de G. `golang.org/x/net` não é importado: PKTINFO é
`syscall`.

**Dependência nova exige justificativa nesta seção.** O objetivo do módulo é ter
poucas (§1.1).

**Gerador de UUID:** um `uuidv7.NewGenerator()` por processo, criado no boot e
compartilhado (é seguro para concorrência e usa ChaCha8 com semente de
`crypto/rand`). Um `init()` falha alto se `crypto/rand` não responder. O `uuid`
não é segredo (expõe o instante), e nada deriva capacidade dele.

**O conversor é próprio** (o de G, §6.2): `encoding/json` perderia o texto dos
números e trocaria UTF-8 inválido em silêncio, e a regra aqui é nunca falhar.

**Sem CGO.** `CGO_ENABLED=0` é premissa do Dockerfile.

### 17.4 Convenções

- Código, identificadores e comentários em **inglês**; documentação e conversa
  em **português**. Não misture dentro de um arquivo. As únicas strings em
  português no código são os textos normativos de `parse.error` (§6.1), que são
  dado, não mensagem.
- Comentários explicam o **porquê** e o que daria errado com a alternativa
  rejeitada. Código copiado de G ou de SN vem **com** os comentários.
- Citações da spec por número de seção (`§5.2`), com a numeração de G
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

Os de G, sem mudança (o pacote `convert` e o `jlhcheck` são cópias literais):
o corpus `internal/convert/testdata/corpus/*.gelf` com os `.want.json`, o
`jlh-gelf-payload.schema.json` e o `jlh-envelope.schema.json` com os exemplos
`valid/`/`invalid/`, e as regras semânticas do oráculo (`level` do envelope =
`payload.level.code`, `sid` recalculado de `payload.host`, `event.iso`, instante
do `uuid`, `size` = octetos do documento, `raw-encoding` coerente, o conjunto de
D5 no `host.hostname`). Todo envelope produzido nos testes (corpus, fuzz,
integração, linhas do dump, emissores vivos) passa no oráculo.

**Por que não validar em execução:** o serializador é um só, é determinístico e
é exercido pelo fuzz com validação completa em cada saída. É o desvio da regra
"validar antes de publicar" do JLH (D3).

### 18.3 Fuzz — obrigatório antes de release

| Alvo | Pacote | Propriedade |
|---|---|---|
| `FuzzConvert` | `convert` | nunca entra em pânico; **toda saída passa nos dois schemas e nas regras semânticas**; `size` é o documento; o pior JLH de §3.9 vale |
| `FuzzFrame` | `receiver` | a moldura `\0` com qualquer fluxo, qualquer partição das leituras e qualquer teto: sem pânico, nunca um `\0` num documento, nunca um documento acima do teto, e a sequência de documentos e recusas é exatamente a de um oráculo independente |
| `FuzzReassemble` | `receiver` | a remontagem de chunks com duas origens, alguns ids, datagramas sem chunk de uma terceira e passos de relógio com varreduras: sem pânico, nunca passa dos tetos, toda mensagem completa é a concatenação dos chunks na ordem de `seq`, e uma varredura no futuro devolve todo byte |
| `FuzzDecompress` | `receiver` | qualquer byte como qualquer contêiner: sem pânico, nunca mais que `L` bytes de saída, só os dois erros (`too_large`, `decompress`), e o que a stdlib comprimiu volta igual |
| `FuzzProxy` | `receiver` | v1 e v2: sem pânico, nunca consome além do cabeçalho declarado; cabeçalho válido consome exatamente os bytes dele, nenhum não consome nada |
| `FuzzHead` | `item` | o codec do cabeçalho (cópia de G) recusa sem pânico; ida e volta é identidade |
| `FuzzRead` | `emergency` | o leitor do dump tolera qualquer byte sem pânico, contabiliza todo byte, e só devolve linhas que a porta `ParseLine` aceita de novo, iguais |

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$    -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$      -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzReassemble$ -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzDecompress$ -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$      -fuzztime=10m
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$       -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$       -fuzztime=10m
```

O `^…$` importa: o Go recusa um `-fuzz` que case dois alvos do pacote. Os
alvos `FuzzJSONBSafe` e `FuzzInboxPayload` de G são do sink PostgreSQL e não
existem aqui.

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
(prova o `field-coerced`). O corpus nunca encolhe (porte de G de 2026-10-31, corpus inteiro). Os vetores dourados são
comparados **byte a byte** com o `.want.json` compactado (`json.Compact`
preserva a ordem): a ordem fixa do payload (§3.2) e a ordem de chegada de
`fields`/`extra` (§3.3, §6.6) fazem parte do contrato.

### 18.5 Integração em processo

A pilha **real** (a mesma `run()` do binário), contra um `nats-server` real com
JetStream: como o único destino é o NATS, todo teste que passa do passo 7 do
boot precisa de `GELF_TEST_NATS_URL` (§18.6) e pula sem ela; só as recusas de
antes do passo 7 (exit 1) rodam sem servidor. Cada teste trabalha num `space`
próprio, com um stream próprio criado a partir de `docker/nats/stream.json` e
apagado no fim. Falhas do destino são injetadas decorando o sink real
(`App.wrapSink`). Cobre: todos os transportes chegam ao stream (UDP plano e em
chunks gzip com `dst-ip`, TCP `\0`, HTTP plano e gzip), PROXY v1 e v2, POST
`/gelf` com cada `Content-Encoding` e cada recusa `4xx`, `503` com o sink DOWN,
documento acima do teto sem perder a moldura, documentos vazios, fidelidade do
JLH (o `Data` da mensagem é o JLH byte a byte, com NUL no meio), `SIGHUP` de
`[log]` (ida e volta, chave sombreada, arquivo inválido), dump de emergência e
reingestão exatos (mesmo `uuid`, mesmo JLH, mesmo depois de trocar uma chave
`[convert]`), dump danificado em `.corrupt`, shutdown gracioso sem perda,
segundo sinal (130), nenhum transporte (exit 1), chaves de G na linha de
comando (exit 1) e no env (`WARN`), códigos 100–114 do `go-loghub-ident` num
subprocesso, e a ordem do boot.

### 18.6 Destino real

As suítes do NATS pulam sem a variável. A da app cria cada stream **a partir do
arquivo que o operador aplica** (`docker/nats/stream.json`), com os subjects do
`space` do teste; a do sink, de um stream canônico em código que um teste sem
servidor confere, junto com o arquivo, contra o contrato de §10.7:

```bash
GELF_TEST_NATS_URL=nats://127.0.0.1:24222 GELF_TEST_NATS_NOJS_URL=nats://127.0.0.1:24223 \
GELF_TEST_NATS_PERM_URL=nats://127.0.0.1:24224 GELF_TEST_NATS_TLS_URL=nats://127.0.0.1:24225 \
  go test -race -timeout 1200s ./internal/sink/ ./internal/control/ ./internal/app/
```

`GELF_TEST_NATS_NOJS_URL` aponta para um servidor **sem** JetStream (o FATAL
imediato de §10.7); `GELF_TEST_NATS_PERM_URL`, para um com os usuários de
permissão restrita (`loghub.*.gelf` negado a `nopub`); `GELF_TEST_NATS_TLS_URL`,
para um com `tls {}`, que prova a guarda contra a promoção automática a TLS do
`nats.go` (nenhum `ClientHello` sai). O que as
suítes provam está em `docs/TESTING.md` §6.

### 18.7 Emissores vivos

A única prova de que chunks, compressão e moldura funcionam no fio é um emissor
GELF de verdade, até o stream JetStream, validado pelo oráculo:

- **fluent-bit** (`fluent/fluent-bit:4.0.3`, saída `gelf`): `Mode udp` com
  `Compress On` e `Packet_Size 1420`, com um registro de ~12 KiB aleatório que,
  comprimido, ainda passa de um chunk (UDP + gzip + chunks), e `Mode tcp`
  (moldura `\0`). É também o emissor do perfil `demo` do `docker-compose.yml`.
- **driver de log `gelf` do Docker** (`docker run --log-driver=gelf`), UDP com
  `gelf-compression-type=gzip` e TCP (`gelf-address=tcp://`).

A suíte é `GELF_TEST_EMITTERS=1 go test -race -run Emitters ./internal/app/`
(`internal/app/emitters_test.go`, `TestEmittersFluentBit` e
`TestEmittersDockerLogDriver`), com `GELF_TEST_NATS_URL`: sobe os contêineres
contra a pilha em processo, espera cada documento no stream e confere
transporte, compressão, chunks, `level`, `host` e os campos adicionais. Sem TLS
nem HTTPS, que G também testa com o fluent-bit. O resultado de cada rodada está
em `docs/TESTING.md` §6.5.

### 18.8 Critérios de aceite v1

**Nunca medidos.** Alvo, não fato; a primeira medição substitui esta tabela.

| Cenário | Alvo |
|---|---|
| UDP sem chunk, documentos de 400 B | ≥ 100k msg/s por núcleo de recepção |
| UDP gzip em 8 chunks | ≥ 10k msg/s |
| TCP `\0`, JetStream | ≥ 100k msg/s |
| HTTP, um documento por conexão | ≥ 5k req/s |
| Perda com `SIGTERM` durante carga, TCP e HTTP (`202`) | **zero**, reconciliada |
| Perda com FATAL do sink | **zero**, via dump e reingestão |

---

## 19. Matriz de garantias

Leia junto com §2.3: a garantia só existe nos transportes que confirmam (TCP e HTTP).

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
| TCP | em parte: a retomada da leitura | nada se perde, mas o emissor não sabe *qual* documento foi aceito |
| HTTP | **sim, por documento**: `202` depois da garantia do ring; `503` + `Retry-After` quando não | o `503` diz ao cliente que repita |
| UDP | não | `gelf_messages_dropped_total` e os contadores da interface |

Quem precisa de `kill -9` sem perda usa o `ring=sqlite` do `go-gelf-server`.

---

## 20. Invariantes

Se uma alteração conflita com um destes, ela está errada. A numeração é a de G;
os que perderam o objeto ficam com a nota.

1. A confirmação nunca sai antes de o item estar no ring; onde não há
   confirmação, há contador.
2. Ring cheio nunca vira aceitação seguida de descarte. UDP descarta antes de
   aceitar; TCP segura o emissor; HTTP responde `503`.
3. Um buffer volta ao pool **uma vez**, e só após a confirmação do sink. Num
   retry o item ainda é o dono; uma tentativa por prazo, ou um erro depois da
   primeira publicação assíncrona, contamina o pedaço.
4. Item aceito só sai do ring após a confirmação do sink (ou um descarte
   deliberado e contado: `logic`, malformada recusada); nunca para liberar
   espaço.
5. **Todo documento aceito vira exatamente um JLH válido** pelos dois schemas, ou
   um descarte contado. A conversão nunca falha; o pior resultado é `malformed`.
6. **Sem perda e sem invenção:** o documento recebido (já sem moldura, já
   remontado e descomprimido; nunca truncado) está em `raw` quando ele é
   obrigatório (§6.9) ou `keep_raw=always`; campo ausente é omitido, nunca
   adivinhado nem `null`.
7. Nenhum sink altera o JLH: o `Data` da mensagem NATS é o JLH byte a byte, e o
   JLH nunca vai num header.
8. Os endereços do payload são a ponta real do transporte, ou o cliente
   declarado por um PROXY **confiável**, e então o par real fica em `x-proxy`. O
   `host` do documento é sempre alegação do emissor e nunca vira chave de
   decisão; `X-Forwarded-For` é ignorado (§4.5).
9. Nenhum label de métrica deriva de conteúdo controlado pelo emissor.
10. Nenhum log síncrono por mensagem no caminho padrão; nenhum payload em log de
    `INFO` ou acima.
11. Removido no go-gelf-nats-server (era do rueidis, §1.2).
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
| GELF por AMQP ou Kafka | Transportes do Graylog sem uso nos emissores alvo; um consumidor de fila é outro software (§1.2) |
| HTTP/2 | HTTP/1.1 com keep-alive (§4.5) basta; só com um emissor que precise dele. O keep-alive saiu do roteiro em 2026-10-31 (porte de G) |
| Remontagem UDP em shards | Hoje é um mapa sob um mutex (§5.2, §22.1); só com medida de contenção |
| Importação de arquivo GELF (`transport=file`, um documento por linha) | Fora da v1 |
| `space` por listener ou rede | Multi-tenant numa instância; fora da v1 |
| Resposta a sonar, ping e pong | Até existir um `go-loghub-driver` |
| TLS (listener ou NATS) | Só com decisão do usuário registrada em `docs/DECISOES.md` (decisão 2): hoje é o `go-gelf-server` |

---

## 22. Lacunas e riscos

Os riscos do desenho estão em `docs/DECISOES.md` §4, e o que os testes ainda não
provam, em `docs/TESTING.md` §8. Esta seção guarda o que o **código atual** ainda
faz diferente do que deveria, ou o que se sabe limitado. Cada item diz o
problema, a consequência e a direção da correção, que segue a regra de sempre:
a spec primeiro (a regra entra na seção indicada), depois o código e o teste.
Corrigido um item, ele sai daqui.

**Herança.** Este servidor é G cortado como SN corta S, e traz as correções de
ambos: as de S até 2026-10-14 (por G e por SN), as rodadas de porte de SN (N,
RP/RN, pendências de 2026-10-14) e as de G. A regra de cada uma está na seção
onde vale, com a data e a origem no texto.

### 22.1 Pendências conhecidas

Abertas em 2026-10-31:

1. **A remontagem UDP é um mapa sob um mutex só**, compartilhado pelos
   `listen.udp_listeners` (§5.2). Com vários listeners e chunks em massa ele é o
   ponto de contenção; nada foi medido. Direção: shards por hash de
   `(src-ip, src-port, id)` com o teto global num contador atômico, quando
   houver medida (§21). Herdado de G (G §22.1 item 1).
2. ~~Um `POST /gelf` com vários documentos separados por `\n`~~ — **fechado
   em 2026-10-31** pelo porte de `http.bulk` de G (§4.5.1; desligado por
   padrão, como o Graylog).
3. **O pacote `internal/item` traz o que GN não usa.** É cópia literal de G
   (§17.1): o codec do cabeçalho (`EncodeHead`/`DecodeHead`, só exercido pelo
   `FuzzHead` e pelos testes), os campos `RowID`, `StreamID`, `done` e `TLS`, e
   os transportes `tls`/`https`. Consequência: `ItemOverhead` = 288 B em vez dos
   256 B de SN (§9.2), um oitavo a mais de *overhead* por item no teto
   `ring.max_bytes`; e o `obsTransport` da recepção mapeia por nome, não por
   posição, `tls`→`tcp` e `https`→`http` só para ser total, a mesma função do
   GP (`TestTransportEnumsAgree`). Direção, se a medida pedir: decidir na
   família se `item` deixa de ser cópia literal em GP/GN (o corte de SN apagou o
   codec), mudando §17.1 nos três.
4. **`receiver/http.go` não é cópia literal de G**: é o de G sem o HTTPS, o
   mesmo corte de GP (§17.1). Uma mudança em G precisa ser portada à mão nos
   dois cortes; a comparação (`diff` com o module path normalizado) fica no
   roteiro de porte.

O que os testes ainda não provam está em `docs/TESTING.md` §8.

Para que a história fique registrada, o que o corte mudou em relação ao espelho
literal de SN↔S (cada um com a regra na seção indicada):

1. `emergency.ParseLine` aceita qualquer número da gramática JSON (não só
   inteiros de até 20 dígitos) e aninhamento até 66 (não 8): o payload GELF
   guarda o texto dos números de `fields`/`extra` e os valores de `extra` como
   vieram (§12.2). Exige `short-message` string, não `message`.
2. Na recepção, `context.Canceled` vindo do `Enqueue` é `Shutdown` (`503` no
   HTTP), como em G; em SN era `RingFull` (§13.4).
3. `ItemOverhead` = 288 B (§9.2; item 3 acima): era 384 B, o de G, até a
   revisão dos cortes de 2026-10-31, que o igualou ao do GP.
4. As chaves removidas (`removed.go`) são as 93 de G que GN não tem, não as de S
   (§13.2, §16.2).
