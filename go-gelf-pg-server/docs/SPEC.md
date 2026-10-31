# go-gelf-pg-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-31
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server`
Binário: `go-gelf-pg-server`
Entrada: `docs/DECISOES.md`, que copia as decisões da família GELF
(`tasks/gelf/DESIGN.md`, 2026-10-31) que valem aqui e registra o corte.

**Origem.** Esta especificação foi derivada em 2026-10-31, por corte, da SPEC do
`go-gelf-server` (G, a mesma data), exatamente como a do `go-syslog-pg-server`
(SP) foi derivada da do `go-syslog-server` (S): **sink só no PostgreSQL** (a
fila `loghub.inbox`), **ring só na memória**, emergência em arquivo **JSON
Lines**, **sem TLS** (nem listener TCP+TLS, nem HTTPS, nem cliente do
PostgreSQL), sem filtro, sem rate-limit e sem beacon (§1.2). Tudo o que é GELF
— recepção UDP/TCP/HTTP, remontagem de chunks, descompressão, moldura `\0`,
conversão, schema do payload — é o de G, com o código copiado literalmente
(§17.2). Tudo o que é do corte — ring, sink, dump, registry, validação, admin,
`SIGHUP` — é o de SP, com os nomes da família GELF.

**A numeração das seções é a do `go-gelf-server`, de propósito.** O código
cita a spec por número (`§5.2`, `§10.5`, `§12.2`), e os servidores da família
dividem código: um número que mudasse de sentido entre eles enganaria quem lê
os dois. Uma seção removida **mantém o título**, seguido de uma nota de uma
linha; nada é renumerado. Da §9 em diante a numeração de G é a de S, e o texto
é o de SP (que já é o corte de S) com o GELF no lugar do syslog.

Servidor de ingestão **GELF** (Graylog Extended Log Format 1.1) do ecossistema
**LogHub**. Recebe documentos GELF por UDP (com chunks e compressão), TCP
(moldura `\0`) e HTTP (`POST /gelf`), converte cada um num envelope **JLH**
com `type=gelf` e o **enfileira** na `loghub.inbox` do PostgreSQL, a fila comum
de envelopes do LogHub. É um **Bridge-IN**: cria a primeira mensagem de cada
fato (`step = 0`).

---

## 1. Escopo

### 1.1 Objetivos

- Máximo de mensagens por segundo com mínimo de CPU e memória.
- **Dependências mínimas:** três em produção (§17.3), binário pequeno.
- Cada documento aceito vira **exatamente um** envelope JLH válido pelo schema
  do envelope (`loghub-json-schema`) e pelo `jlh-gelf-payload.schema.json`
  deste repositório (§3.2).
- **Nunca confirmar** um documento antes de ele estar no ring, nos transportes
  que têm como confirmar (§2.3).
- Backpressure explícito onde o transporte permite (TCP: parar de ler; HTTP:
  `503`); perda **contada e visível** onde ele não permite (UDP).
- **Sem perda e sem invenção** no payload: o que chegou está nos campos ou em
  `raw`; o que não chegou é omitido, nunca adivinhado (§6) — com uma exceção
  deliberada, o `level` padrão (D1).
- Um destino só: a fila `loghub.inbox` de um PostgreSQL compartilhado entre
  instâncias e produtores (§3.7.1, §10.5).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| **TCP+TLS** e **HTTPS** no listener, **mTLS** | Corte (DESIGN §1, GP): o mesmo que SP fez com o RFC 5425 de S, por decisão do usuário daquele corte (SP DECISOES, decisão 4) — o TLS sai **em todo lugar**. Quem precisa de GELF cifrado usa o `go-gelf-server` ou um terminador TLS na frente (com PROXY protocol, §4.6). O payload nunca traz `transport=tls`/`https` nem o objeto `tls` |
| **TLS até o PostgreSQL** | A mesma regra. A conexão é sempre `sslmode=disable`: o `TLSConfig` do pgx é nulo, os *fallbacks* saem e o `PGSSLMODE` é ignorado (§10.5). Uma `LOGHUB_EB_URL` com `sslmode=require`, `verify-ca` ou `verify-full` é recusada com exit 1 (§10.9). O banco é alcançado por rede confiável ou por socket unix |
| Outros sinks: `nats`, `redis`, `sqlite`, `mysql`, `clickhouse`, `none` | Sink **só** no PostgreSQL. Cada um trazia um cliente e a sua árvore de transitivas. Quem precisa deles usa o `go-gelf-server`, o mesmo servidor com os dois eixos abertos |
| Outros rings: `sqlite`, `redis`, `none` | Ring **só** na memória. A perda sob `kill -9` é a do ring em memória (§19); FATAL e `SIGTERM` são cobertos pelo dump (§12) |
| Filtro por `level` | Sai, como o filtro de SP. O descarte, se houver, é do consumidor da fila; a recepção nem lê o documento (§2.2) |
| Rate-limit por prefixo de rede | Sai, como em SP. Proteção contra emissor ruidoso fica na rede (firewall, QoS) |
| Heartbeat beacon | Dependia do `sink=nats`, que saiu. Sonar, ping e pong também ficam de fora |
| `sink.type` e `ring.mode` como chaves | Um valor só cada um. O log de boot e o `gelf_config_info` continuam dizendo `ring_mode="memory"` e `sink_type="postgres"`, para que um painel de G sirva aqui (§14.1) |
| GELF por AMQP/Kafka | Transportes do Graylog sem uso nos emissores alvo; um consumidor de fila é outro software |
| Confirmação por mensagem no TCP | O GELF não tem; a confirmação é continuar lendo (§2.3) |
| HTTP/2 (h2c) | HTTP/1.1 com keep-alive basta para os emissores GELF; o backpressure é o `503` por requisição (§4.5) |
| `X-Forwarded-For` | A origem é o socket ou o PROXY protocol de um par confiável (§4.6) |
| Compressão no TCP | A spec GELF não tem; um documento comprimido no TCP é `malformed` |
| `space` por listener ou rede | Um Workspace por instância |
| Validação por JSON Schema em execução | Só em teste e CI (D3) |
| Autenticação de emissor | Não existe no protocolo sem TLS: firewall ou rede privada |
| Unix datagram, RFC 3164/5424, charset legado, subjects por facility | São do syslog; não existem nem em G |

O porquê comum: **o objetivo do corte** (um servidor rápido e com dependências
mínimas, como SP), **as dependências** (as de SP, três em produção; o binário
caiu de 32,0 MB para 17,5 MB, mesmo build `darwin/arm64` com as mesmas flags nos
dois, §17.3) e **a velocidade** (a recepção não lê o documento: nem `level`,
nem filtro, nem balde de rate-limit; §2.2).

### 1.3 Fontes normativas e desvios

| Fonte | Versão | Papel |
|---|---|---|
| GELF Payload Specification (Graylog) | 1.1 | Documento, campos, `_` adicionais, chunks, compressão, `\0`, HTTP |
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, `jlh-envelope.schema.json` |
| `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json` | este repositório | Payload `type=gelf` (não há skill `type=gelf`, decisão 1) |
| skill `loghub-postgres-table-inbox` | 2026-10-06 | DDL da `loghub.inbox`, mapeamento envelope → colunas, `ON CONFLICT (uuid) DO NOTHING` (§3.7.1) |
| `go-loghub-uuidv7` | v0.0.3 | UUIDv7 nível 3 |
| `go-loghub-ident` | v0.5.2 | AgentUUID, Workspace, `DATADIR` |

**Desvios deliberados**, os mesmos de G, cada um com o motivo na seção citada:

| # | Desvio | A fonte diz | Aqui | Seção |
|---|---|---|---|---|
| D1 | `level` ausente | GELF 1.1: `1` (alert) | **6** (info) com aviso `level-default` (decisão 3): muitas bibliotecas omitem `level`, e tudo viraria alerta no LogHub | §6.5 |
| D2 | `host` e `short_message` ausentes | GELF 1.1: obrigatórios (o Graylog recusa o documento) | aceito, com `host-missing` / `short-message-missing` e `short-message: ""` (regra absoluta de §6.1) | §6.5 |
| D3 | Validação | a skill do envelope: todo envelope passa no schema antes da fila | schema só em teste e CI; o serializador é provado por fuzz | §18.2 |
| D4 | `LOGHUB_UUID_LEVEL` | variável sugerida | não existe: nível 3 fixo | §3.1 |
| D5 | Espaço em branco em `host` | GELF: qualquer string | `^\S+$` com o conjunto de S D6 (`\s` do ECMAScript mais U+001C–U+001F e U+0085); outro valor é omitido com `host-invalid` | §6.5 |
| D6 | Tamanho | Graylog aceita ~1 MiB em UDP (128 × 8 KiB) | **128 KiB** do documento descomprimido (§3.9); acima disso, recusa contada | §5.5 |

---

Não é desvio, mas fica registrado: o enum `transport` do schema do payload
continua `udp | tcp | tls | http | https` (o schema é o da família, copiado
literalmente, §18.2); `tls` e `https` só nunca aparecem aqui. A cópia da fila
leva como **string** o número que o `jsonb` não guarda (§3.7.1): é regra do
destino, como o NUL, não da conversão.

## 2. Arquitetura

### 2.1 Dois eixos ortogonais

No `go-gelf-server` são dois eixos, ring × sink, com vinte e oito combinações.
Aqui cada eixo tem **um valor só**:

- **RING**: `memory` (§9.3).
- **SINK**: `postgres`, a fila `loghub.inbox` (§10.5).

As chaves `ring.mode` e `sink.type` não existem (§1.2). A separação continua no
código: **o ring não consulta o sink, e vice-versa** (invariante §20.15), e as
interfaces `ring.Ring` e `sink.Sink` ficam, porque são a costura pela qual
recepção, workers e saúde são testados.

```
   emissores (Docker gelf driver, fluent-bit, Logstash, graypy, logstash-gelf, gelf-cli)
      |
      |  UDP :12201 (chunks, gzip/zlib)   TCP :12201 (\0)   HTTP :12202 POST /gelf
      v
 +-----------------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                          |
 |   [PROXY] -> chunks UDP | moldura \0 | corpo HTTP                      |
 |   -> descompressão com teto -> amostra de relógio                     |
 |   -> sink DOWN? -> ring cheio? -> uuid -> Item(cabeçalho + documento)  |
 +----------------------------------+------------------------------------+
                                    |
                     +--------------v---------------+
                     | RING memory: canal + 2 tetos |
                     +--------------+---------------+
                                    | sink.workers (4)
                     +--------------v---------------+
                     | CONVERSÃO -> JLH type=gelf   |  <- fora do caminho quente
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
   porta admin 127.0.0.1:9104: /healthz /readyz /metrics (§14)
```

O ring é **único e global**, compartilhado por todos os transportes: uma métrica
única de profundidade, backpressure justo e código simples.

### 2.2 Onde a conversão acontece, e por quê

Decisão 8. A recepção faz o mínimo que permite decidir se o documento entra; a
conversão completa roda no **worker de dreno**.

| Etapa | Onde roda | Custo |
|---|---|---|
| PROXY | recepção, uma vez por conexão | do transporte |
| Remontagem de chunks, moldura `\0`, corpo HTTP | recepção | inevitável |
| Descompressão com teto | recepção | proporcional ao documento |
| Amostra de relógio, sink DOWN, ring cheio, `uuid` | recepção | uma leitura de relógio, dois testes, um UUID |
| Validação JSON, campos, números, escapes | **worker de dreno** | proporcional ao documento |
| Serialização do JLH | **worker de dreno** | proporcional aos campos |

A descompressão fica na recepção porque o cru do ring é o **documento
descomprimido** (decisão 8), e o teto de §5.5 vale sobre o que o converter vai
ler.

A ordem da recepção é estrita, e a primeira condição que falha encerra o
documento: vazio (§5.6) → acima do teto (§5.5) → sink DOWN (§13.3) → **ring
cheio** → `uuid`, cabeçalho e cópia para um buffer do pool → `ring.Enqueue`. O
"ring cheio" é uma pré-checagem barata e sem trava (`ring.Memory.FullFor(n)`:
itens ≥ `capacity_items`, **ou** os bytes do ring mais a classe do pool de `n`
e o `ItemOverhead` acima de `max_bytes`), feita **antes** do `uuid`, da cópia e
do item: um documento recusado com o ring cheio (uma enxurrada de UDP, a
retentativa do TCP a cada 50 ms) não custa alocação nenhuma
(`TestIngestFullRingIsRefusedBeforeTheCopy`), como em SP. Um ring fechado nunca
está "cheio": o `Enqueue` responde `ErrClosed`, que é o shutdown. **O `Enqueue`
continua sendo a autoridade** (os dois tetos, §9.2).

Em G o `level` de topo é lido na recepção quando o filtro pode recusar algo.
Sem filtro (§1.2), **a recepção não lê o documento**: nenhum byte do JSON é
interpretado antes do ring. Num pico de UDP, toda a CPU da conversão é paga
pelos `sink.workers`, que é onde a vazão é elástica.

Consequências que o resto da spec assume (as de S):

1. **O item no ring não tem envelope.** Ele carrega o documento e o que a
   conversão **não pode** recuperar dele: `uuid`, amostra de relógio,
   transporte, endereços, compressão, número de chunks, `from` e `space`
   (§3.4).
2. **A contabilidade do ring conta só o documento.**
3. **O dump guarda o JLH pronto** (§12), não cabeçalho + documento.
4. **O JLH é função determinística** de cabeçalho + documento + config de
   conversão (§3.5). Toda chave de `[convert]` exige restart.

### 2.3 Regra de aceitação, por transporte

> A confirmação sai no instante em que o documento está no ring — nunca antes.
> **Onde não existe confirmação, existe um contador.**

| Transporte | A confirmação é | Ring cheio, sink DOWN |
|---|---|---|
| **UDP** | não existe | documento **descartado**, `gelf_messages_dropped_total{reason=...}` |
| **TCP** | continuar lendo do socket | **parar de ler** (§5.7); a janela TCP segura o emissor |
| **HTTP** | `202 Accepted` | `503` com `Retry-After: 1` (§4.5); nada é segurado |

A garantia é a do ring em memória: o documento entrou no canal. `SIGTERM`
drena; FATAL e dreno estourado viram dump (§12); `kill -9` e OOM perdem o
backlog (§19).

**O UDP perde por desenho do protocolo.** Um chunk perdido pela rede deixa a
mensagem incompleta, descartada no prazo (§5.2). Quem não pode perder usa TCP
ou HTTP.

### 2.4 Papel no LogHub

| Aspecto | Valor |
|---|---|
| Papel | **Bridge-IN**: `uuid` novo, `step = 0`, `from` = AgentUUID |
| Destino | a fila `loghub.inbox` (§3.7.1), coluna `type = 'gelf'` |
| Plano de controle | nenhum (o beacon saiu com o `sink=nats`, §1.2) |
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
| `uuid` | UUIDv7 **nível 3**, gerado na recepção da amostra de relógio do documento, depois do estado do sink e do ring cheio; reusado em toda retentativa, no dump e na reingestão. |
| `from`, `space` | identidade de quem **recebeu** (cabeçalho do item). |
| `type` | `"gelf"`. |
| `level` | `payload.level.code` (0–7); `6` quando ausente (D1) ou inválido. |
| `sid` | FNV-1a 64 de `transport|src-ip|src-port|dst-ip|dst-port` reduzido a 53 bits (§6.10), com `transport` ∈ `udp`, `tcp`, `http`. Nunca 0 aqui: todo transporte é de rede. |
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
| `transport` | `udp` \| `tcp` \| `http` (o schema da família também tem `tls` e `https`, que nunca saem daqui) | sempre |
| `compression` | `gzip` \| `zlib` \| `deflate` | só se houve (UDP, HTTP) |
| `chunks` | int 1..128 | só UDP com chunk (um documento num chunk só conta: `1`) |
| `tls` | — | nunca (não há TLS, §1.2) |
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
conteúdo (`fields`, `extra`, `parse`) são omitidos.

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
| `transport` | `udp` \| `tcp` \| `http` |
| `compression` | `gzip` \| `zlib` \| `deflate`; ausente = nenhuma. Só `udp` (gzip, zlib) e `http` (os três) |
| `chunks` | 1..128, só `udp`; ausente = sem chunk |
| `src-ip`, `src-port`, `dst-port` | obrigatórios (todo transporte é de rede) |
| `dst-ip` | omitido quando desconhecido |
| `peer-ip`, `peer-port` | o par real, só com PROXY (nunca em `udp`) |
| `tls` | nunca aqui |

Não há `framing` (só existe uma moldura por transporte) nem `size`: o GELF
nunca trunca, então `size = len(documento)`; guardá-lo seria duas fontes da
mesma verdade. O decodificador (`FuzzHead`) recusa `v` desconhecido, chave
obrigatória ausente, chave desconhecida, tipo errado, IP não canônico, porta
fora de 0–65535, `compression`/`chunks` num transporte que não os tem, `chunks`
fora de 1..128, `tls` fora de `tls`/`https`, `peer-*` em `udp`, e o `uuid` cujo
instante não é o da amostra.

**O cabeçalho só existe em memória.** Em G ele é serializado pelo `ring.db`,
pelo stream do ring Redis e pelo dump `.bin`; aqui nada persiste um cabeçalho
(o ring é memória, o dump guarda JLH, §12). Em SP o codec foi apagado; aqui o
pacote `internal/item` é o de G **copiado literalmente** (decisão da família:
os pacotes GELF são idênticos em G, GP e GN), então `EncodeHead`/`DecodeHead`
existem, não são chamados por código de produção e continuam provados pelo
`FuzzHead`. É o único código morto deliberado do módulo, e o preço de não
manter duas cópias divergentes do mesmo pacote (DECISOES §2).

### 3.5 Determinismo

> **Invariante:** o JLH é função pura de `(cabeçalho, documento, config de
> conversão)`.

A config de conversão é `[convert]` (§16.5): `keep_raw`, `max_message_bytes`
(atua também na recepção) e `accept_malformed`, congelada no boot. Um dump já
guarda o JLH pronto e volta sem conversão (§12.2): trocar `[convert]` entre duas
subidas não muda um dump pendente.

### 3.6 Entrada do stream Redis (`sink=redis`)

Removido no go-gelf-pg-server (§1.2): não há `sink=redis`.

### 3.7 Destino: a fila `loghub.inbox`

O único destino é a fila comum do LogHub (§3.7.1). A tabela `gelf_events` de G
(`sink=sqlite` e `sink=mysql`, com colunas promovidas e o JLH em `env`) e a
`inbox_gelf` do ClickHouse foram removidas com esses sinks (§1.2). Da regra
delas fica uma só, aplicada à fila: o NUL dos textos vira U+FFFD na cópia da
coluna (§3.7.1).

#### 3.7.1 Fila `loghub.inbox` (`sink=postgres`)

O destino é a **fila de espera `loghub.inbox`** da skill
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
| `level` | `level` | `payload.level.code` (§3.1) |
| `event_nsec` | `event.sub` | `sub % 1000`, o resto que o microssegundo não guarda |
| `space` | `space` | — |
| `type` | `type` | `gelf` |
| `payload` | `payload` | `jsonb`; com NUL ou com número que o `jsonb` não guarda, uma cópia reconstruída (abaixo) |

Num envelope que vem do worker, `uuid`, `from_uuid`, `space` e `event_at`/
`event_nsec` saem do cabeçalho do item, e `level`/`sid` da conversão. Num
envelope **reingerido** de um dump (§12.2) as dez colunas saem do **próprio
cabeçalho da linha**, conferido por `emergency.ParseLine`: não há item nem cru
de onde tirá-las.

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
  bytes. O dump de emergência (§12.1) é o único lugar onde o JLH deste servidor
  existe byte a byte.
- **NUL.** O `jsonb` recusa o escape `\u0000` (`22P05`), e a classe `22`
  descartaria o pedaço inteiro (§10.2). Só nesse caso raro a coluna recebe uma
  **cópia reconstruída** do payload (as regras de G, §3.7.1 dele):
  - `raw` com NUL, ainda não em base64, vai em base64 com `raw-encoding:
    "base64"`: fica exato, e `size` continua contando os octetos dele;
  - todo outro texto troca U+0000 por U+FFFD, **inclusive as chaves e os
    valores de `fields` e `extra` em qualquer profundidade** (`extra` guarda
    qualquer valor JSON do emissor), `short-message`, `full-message`,
    `host.hostname`, `facility` e `file`;
  - duas chaves do mesmo objeto que viram a mesma depois da troca (`"a\u0000"`
    e `"a\ufffd"` em `fields`): **vence a que já não tinha NUL**; entre duas
    que tinham, **vence a menor chave original**. Nunca a ordem de iteração do
    mapa: a cópia precisa sair igual em todo retry;
  - a reconstrução passa por um mapa (o `jsonb` não guarda ordem de chave) e
    preserva números como texto (`json.Number`): `1e400` e
    `12345678901234567890` nunca passam por `float64`.
- **Número que o `jsonb` não guarda** (decisão deste corte, DECISOES §2;
  portado para G em 2026-10-31). Os números de `fields` e `extra` são o texto que chegou
  (§3.3), e o tipo `numeric` em que o `jsonb` os guarda pára em 131 072
  dígitos antes da vírgula e 16 383 depois: `1e1000000` ou `1e-16384` num
  campo GELF é `22003`, classe `22`, e descartaria como `logic` o pedaço
  inteiro — os vizinhos do documento hostil junto. A cópia reconstruída (a
  mesma da regra do NUL) leva esse número como a **string do seu texto**: nada
  dele se perde, e o JLH do pedaço e o do dump continuam com o número como
  chegou. A conta é sobre o texto, sem `float64` (`jsonbHolds`: dígitos do
  inteiro sem zeros à esquerda mais o expoente ≤ 131 072; dígitos da fração
  menos o expoente ≤ 16 383); uma **mantissa toda zero** é 0 para o
  PostgreSQL, então só a segunda conta vale (`0e999999` e `0.0e131072` cabem e
  ficam número; `0e-20000` não cabe — antes da revisão de G de 2026-10-31 o
  zero ia como string sem precisar). Cada borda está conferida contra o
  PostgreSQL 18 real (`TestJSONBHoldsAgreesWithPostgres`,
  `TestInboxPayloadCarriesNumbersJSONBCannotHoldAsText`,
  `TestPostgresNumbersJSONBCannotHoldDoNotDropTheChunk`). Sem NUL e sem número
  fora da faixa — quase sempre — a coluna recebe o sub-slice do JLH, sem cópia.
- **Exceção do oráculo, só nesta cópia.** O `raw` de um documento UTF-8
  **válido** que contém um NUL de verdade (o byte `00` dentro de uma string
  JSON é controle cru, então o documento é `malformed` e o `raw` vai como
  string com `\u0000`) sai na cópia da fila em base64. A regra do oráculo
  "`raw` UTF-8 nunca em base64" (§18.2) vale para o JLH, não para esta cópia:
  os testes da fila a toleram só nesse caso (`queueJLH`), e um consumidor da
  `loghub.inbox` decodifica o `raw` pelo `raw-encoding`, sempre (§22.1).
- **As CHECKs da tabela são contrato, não opção.** Todo envelope `type=gelf`
  daqui tem `level` ≤ 7 e `from` UUIDv7 (`go-loghub-ident`), e nenhum é
  `beacon`; a CHECK `inbox_syslog_ck` só olha `type=syslog`: nenhuma CHECK
  recusa o que este servidor produz. Uma recusa (`23514`) seria `logic`, descartaria o pedaço, e é
  bug. O operador **não** remove CHECKs (regra 6 da skill). A leitura do dump
  (§12.2) recusa antes do `INSERT` toda linha que uma CHECK recusaria.
- **Só o índice da PK** (regra 7 da skill). Consultas por host, `level` ou
  campo vão para o armazenamento definitivo, não para a fila.
- O consumo (`DELETE … USING job … RETURNING`, quarentena, monitoramento do
  atraso) é de outro software e está na skill; nada dele roda aqui.

O que o servidor **exige** da tabela, verificado no boot (§10.5): as **dez**
colunas que ele escreve, com tipos compatíveis; um índice **único** exatamente
sobre `(uuid)`; `INSERT` e `SELECT (uuid)` para o papel (o `ON CONFLICT (uuid)`
lê a coluna árbitra: só com `INSERT` o primeiro pedaço falha com `42501`).
`received_at` e as CHECKs não são conferidas: o servidor não depende delas.

#### 3.7.2 Tabela `inbox_gelf` (`sink=clickhouse`)

Removido no go-gelf-pg-server (§1.2): não há `sink=clickhouse`.

### 3.8 Mensagem NATS (`sink=nats`)

Removido no go-gelf-pg-server (§1.2): não há `sink=nats`.

### 3.9 O maior evento, e por que ele é calculado e não configurado

Com `L = convert.max_message_bytes` (o documento descomprimido nunca passa
disso, §5.5), a conta de G, que é a do conversor copiado:

```
pior JLH = 6 × L + 8 KiB
```

- **Documento malformado** (§6.1): `short-message` é `""` e o único texto é
  `raw`. UTF-8 válido com todo byte de controle vira `\u00XX`: **6 × L**. Não
  UTF-8 vai em base64: 4/3 × L.
- **Documento válido**: toda string do payload é uma fatia **disjunta** do
  documento, reescrita com escape mínimo, o que nunca a alonga; o único
  crescimento é a coerção de um valor composto (≤ 2 × os bytes dele), e o `raw`
  de perda ou de `keep_raw=always` (≤ 2 × L). Total ≤ **4 × L**.
- **8 KiB fixos**: as chaves do payload, `host`, `x-proxy`, `parse`, `level`,
  `timestamp` e o cabeçalho do envelope.

Com o padrão de 128 KiB, o pior JLH é **776 KiB**, o mesmo de SP com o padrão
dele. `TestWorstCaseJLHIsBounded` prova o teto com documentos adversários.

Em G o número existe para recusar no boot um destino com teto por mensagem
menor que ele (`max_payload` do NATS, `max_allowed_packet` do MySQL). O
PostgreSQL não tem um teto desses para conferir, então aqui ele serve a três
coisas, como em SP: o pedaço de §10.1 (um JLH maior que `sink.batch_max_bytes`
forma um pedaço sozinho), o limite de linha do leitor do dump
(`emergency.MaxLine`, §12.2, calculado com o **maior** `max_message_bytes` que a
config aceita: 6 × 16 MiB + 8 KiB) e o `max_event_bytes` do log de boot. O teto
de 128 KiB, que em G vem do `max_payload` de 1 MiB do NATS, fica por paridade
com a família: o mesmo documento é aceito pelos três servidores.

O fio pode trazer mais que `L`: o teto do **corpo comprimido** (HTTP) e da soma
dos chunks de uma mensagem (UDP) é `L + 16 KiB` (§5.5), e o documento
descomprimido continua limitado a `L` dentro do leitor.

## 4. Transportes

Três transportes independentes, cada um ligado por um parâmetro de `[listen]`
que, vazio, o desliga. **Todos desligados é erro de configuração** (exit 1).

| Transporte | Parâmetro | Padrão | Moldura |
|---|---|---|---|
| UDP | `listen.udp` | `:12201` | datagrama ou chunks GELF (§5.2) |
| TCP | `listen.tcp` | `:12201` | `\0` (§5.3) |
| HTTP | `listen.http` | `:12202` | um documento por `POST /gelf` |

### 4.1 Portas

12201 é a porta GELF do Graylog e do Docker para UDP e TCP (decisão 5); o TCP e
o HTTP não dividem porta, então o HTTP fica em 12202. A imagem roda como uid
65532: todas são altas. O log de boot imprime o endereço efetivo de cada
listener. Dois transportes no mesmo endereço TCP (`tcp`, `http`,
`admin.bind`) são exit 1; UDP e TCP na mesma porta são permitidos. Não há
12203 (TCP+TLS) nem 12204 (HTTPS) aqui (§1.2).

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

### 4.5 HTTP

Decisão 13. Um `net/http.Server` da stdlib por listener, endurecido como os
webhooks (CHANGELOG 2026-10-14):

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
  conexões). O `503` de backpressure (ring cheio, sink DOWN) **não** fecha a
  conexão — o cliente espera um segundo, não paga outro *handshake* —; só o
  shutdown fecha: a resposta do `503` de `shutdown` leva `Connection: close`,
  e desde o início do shutdown **toda** resposta leva
  (`SetKeepAlivesEnabled(false)`), de modo que uma conexão ociosa é fechada e
  uma em uso termina depois da requisição em voo, nunca no meio dela.
  **Sem HTTP/2**: não há TLS que negocie `h2`, e o h2c fica desligado
  (`TLSNextProto` vazio; o `http.Server` não o oferece sem `Protocols`).
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
  correção do `go-webhook-server` de 2026-10-14; revisão de G de 2026-10-31).
- **Limites de conexão:** `http.max_conns` e `http.max_conns_per_ip`, como §4.4.
- **Origem:** o socket, ou o PROXY protocol de um par confiável (§4.6), lido
  antes do HTTP. **`X-Forwarded-For` e `X-Real-IP` são ignorados**: um
  cabeçalho HTTP é alegação do cliente.
- **Sem HTTPS** (§1.2): `NewHTTPS` e o ramo TLS do `http.go` de G saíram
  (§17.2).
- O tempo entre o corpo completo e a garantia do ring vai para
  `gelf_confirm_duration_seconds`, como no TCP.

#### 4.5.1 `http.bulk`: um documento por linha

Padrão `false`, como o `enable_bulk_receiving` do input GELF HTTP do Graylog:
o corpo é **um** documento. Com `true` (decidido sem entrevista em G em
2026-10-31, contestável; DECISOES §2) — o que o output `http` do fluent-bit
com `Format gelf` manda, todos os registros de um *flush* num corpo:

- O corpo é descomprimido inteiro (como sempre, pelo `Content-Encoding` ou
  pelos bytes) com o teto `B = http.bulk_max_bytes` (4 MiB, até 64 MiB;
  abaixo de `L`, vale `L`) no lugar de `L`; no fio o teto é `B + 16 KiB`.
  Fora disso, `413`.
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
  neste corte (§1.2): nada é avaliado por linha na recepção.
- O `202` só sai depois de **todas** as linhas no ring. A primeira linha
  recusada por um motivo que pode passar depois (ring cheio, sink DOWN,
  shutdown) interrompe o corpo: **as linhas anteriores ficam no ring** e a
  resposta é `503` com `Retry-After`. O cliente que reenvia o corpo
  **duplica** essas linhas (com outros `uuid`s: a fila não as deduplica).
  Retirar do ring o que ele já garantiu não existe na interface do ring
  (§9.1); a alternativa seria reservar espaço para o corpo inteiro antes, o
  que o ring não sabe fazer.
- As linhas que não chegaram a ser tentadas não são contadas.
- Recarregável? **Não**: `http.bulk` e `http.bulk_max_bytes` exigem restart,
  como toda chave de recepção (§16.3).

### 4.6 PROXY protocol

Como S §4.6, nos dois transportes de conexão (`tcp`, `http`), com as mesmas regras de par confiável, `optional`/`required`, prazo
próprio, v1/v2, `LOCAL`, shutdown durante a espera e `x-proxy`. No HTTP o
cabeçalho é lido pela conexão no primeiro `Read` (dentro da goroutine da
conexão, nunca no laço de `Accept`), e o handler lê dela os endereços
resolvidos. Um cabeçalho inválido ou ausente em `required` fecha a conexão sem
resposta HTTP.

### 4.7 TLS (TCP+TLS e HTTPS)

Removido no go-gelf-pg-server (§1.2): não há TLS nem HTTPS.

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
  aberta que o estoura no meio (antes da revisão de G de 2026-10-31, cada
  chunk de uma mensagem nova contava um descarte, e a aberta que estourava era
  contada de novo como `chunk_timeout`). **Com `max_messages` cheio não há
  onde lembrar** a mensagem, nem como lápide: aí cada chunk recusado conta
  como uma chegada e um `chunk_overflow` (até `count` por mensagem). Expulsar
  uma mensagem aberta para dar lugar à lápide deixaria uma enxurrada de
  primeiros chunks destruir toda a remontagem em curso. O gauge
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

Como SP §5.7: ring cheio ou sink DOWN fazem o leitor parar de chamar `Read`,
conferindo a cada 50 ms, por até `tcp.backpressure_timeout` (30 s); estourado,
fecha a conexão contando o documento em mãos. `Accept`, `Retry` e `RecordDrop`
são caminhos separados: um documento segurado por trinta segundos conta uma
chegada e no máximo um descarte. O `uuid` é cunhado uma vez e reusado em toda
retentativa.

O HTTP não segura: uma tentativa, e `503` com `Retry-After: 1` (§4.5). O UDP
descarta.

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
- A mesma passada serve, em G, o filtro na recepção (§7 de G) e a conversão no dreno; aqui só a conversão: o
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

Removido no go-gelf-pg-server (§1.2): não há filtro; a recepção não lê o `level`.

## 8. Rate-limit

Removido no go-gelf-pg-server (§1.2).

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
  `cap(cru)` mais `ItemOverhead` = **288 B**. A struct `item.Item` tem 256 B
  (`unsafe.Sizeof`, medido em 2026-10-31), na classe de alocação de 256 B,
  mais os 8 B do slot no canal do ring: 264, arredondados para 288. O pacote
  `item` é o de G copiado literalmente (§3.4), então a struct ainda tem o
  ponteiro `done`, o id de linha do `ring=sqlite` e o id de entrada do
  `ring=redis`; nada aqui cria o canal `done`, que G conta à parte (G: 384 B;
  SP, que apagou esses campos, 256 B). `TestItemOverheadCoversTheStruct`
  arredonda a struct para a classe de alocação antes de comparar e falha se ela
  crescer além do número.
  Contar `len(cru)` deixava um documento de 8 193 bytes, copiada para o buffer de
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
do primeiro item: lotes maiores são menos idas ao banco e amortizam a
conversão, que roda por item no worker.

### 9.4 `ring.mode = sqlite`

Removido no go-gelf-pg-server (§1.2): o ring é só memória.

### 9.5 `ring.mode = redis`

Removido no go-gelf-pg-server (§1.2): o ring é só memória.

### 9.6 `ring.mode = none`

Removido no go-gelf-pg-server (§1.2): o ring é só memória, e a conversão nunca roda na recepção.

### 9.7 Combinações que degradam ou recusam

Removido no go-gelf-pg-server (§1.2): há uma combinação só, `memory` → `postgres`.

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
pedaço, com capacidade limitada ao próprio tamanho), o `level` e o `sid` (as duas
colunas da fila que o cabeçalho do item não tem) e o item de origem, de onde
saem `uuid`, `from`, `space` e `event` (§3.7.1).

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
   sozinho. Um documento malformado recusado (`convert.accept_malformed =
   false`, §6.1) não entra em pedaço nenhum: é contado e confirmado junto.
3. `sink.Write(ctx, pedaço)`, com retentativa (§10.2).
4. Sucesso: `ring.Commit(itens do pedaço)`, devolve o cru e o buffer do pedaço
   aos pools, `gelf_events_written_total += n`.
5. Próximo pedaço.

O teto de bytes existe porque o JLH chega a 6× o documento (§3.9): `workers × batch × pior
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
  retry a reenvia. A deduplicação é da fila: `ON CONFLICT (uuid) DO NOTHING`
  (§3.7.1), e a segunda cópia conta em `gelf_sink_postgres_duplicates_total`.
- **Contaminação** (§15.2): uma tentativa que termina por prazo ou cancelamento
  marca o pedaço, e nem o cru nem o buffer do pedaço voltam ao pool, nem quando a
  retentativa seguinte tem sucesso.

| Classe | Efeito |
|---|---|
| `ClassNetwork` | retenta; não muda o estado de saúde (§13.3) |
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`gelf_events_dropped_total{reason="sink_logic"}`), confirmado no ring |
| `ClassFatal` | FATAL imediato |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`gelf_events_written_total`, só os demais em `sink_logic`. No PostgreSQL o
pedaço é **um** comando: na falha nada foi gravado, e o pedaço inteiro conta
como perdido. O mecanismo fica porque é o contrato da interface, que os testes
dos workers exercitam.

### 10.3 `sink.type = redis`

Removido no go-gelf-pg-server (§1.2). A regra que estava aqui e que o código ainda cita — uma resposta **fatal** à sonda de saúde é FATAL na primeira checagem, sem passar pelos fusíveis — vale para o PostgreSQL e está em §13.3.

### 10.4 `sink.type = sqlite`

Removido no go-gelf-pg-server (§1.2).

### 10.5 `sink.type = postgres`

O único destino (não há chave `sink.type`, §1.2): fila **compartilhada entre
instâncias** e entre produtores LogHub, a `loghub.inbox` de §3.7.1.
`github.com/jackc/pgx/v5` pela API nativa (a classificação precisa do
`pgconn.PgError`).

- **Nunca faz DDL**, nem `CREATE TABLE IF NOT EXISTS`: N instâncias subindo
  juntas correm no catálogo, e o papel de produção não tem o privilégio.
  `docker/postgres/inbox.sql` é a cópia literal da DDL da skill (§3.7.1).
  `sink.postgres.table` tem padrão `loghub.inbox`; outro nome (identificador em
  minúsculas, com esquema opcional) só serve para testes ou para uma segunda
  fila com a **mesma** DDL.
- **Sem TLS, sempre** (§1.2). A conexão é `sslmode=disable`: o `TLSConfig` do
  pgx é nulo e os *fallbacks* são descartados, então não há segunda tentativa
  que pudesse negociar TLS. Um servidor cujo `pg_hba.conf` só tem linhas
  `hostssl` recusa o login com classe `28`, que é fatal no boot com as palavras
  do próprio servidor. O banco é alcançado por rede confiável ou por socket unix
  (`sink.postgres.addr` começando com `/`).
- **O ambiente libpq não chega ao pool.** A configuração do pool é lida com
  **toda** variável `PG*` escondida (`parseWithoutLibpqEnv`: retiradas do
  ambiente, `pgxpool.ParseConfig("sslmode=disable")`, restauradas, sob um
  mutex; a leitura acontece uma vez, no boot, e nada mais no processo lê
  `PG*`). Sobrescrever campo a campo depois da leitura não bastava: a própria
  leitura falhava — `PGSSLMODE=bogus`, um `PGSSLROOTCERT` ilegível, um
  `PGSERVICE` que não existe ou `PGPORT=abc` derrubavam o boot com exit 1 — e o
  que ela aceitava sem campo para sobrescrever chegava ao pool:
  `PGCHANNELBINDING=require` ou `PGREQUIREAUTH=md5` recusavam toda conexão sem
  SQLSTATE, retentada como `network` até o `boot_timeout`. Depois da leitura,
  todo campo que o registry possui é preenchido; os *fallbacks* saem (senão uma
  recusa no host configurado seria retentada contra `localhost`); os parâmetros
  de execução são refeitos do zero; o `ValidateConnect` sai. A sessão fixa
  `client_encoding = UTF8` e `application_name = gelf-pg/<AgentName>@<Hostname>`,
  visível em `pg_stat_activity`.
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
- **Conexões:** `sink.workers` tem padrão **4** (no pai era 8, rebaixado para 4
  com `WARN` num destino SQL; aqui o padrão já é o valor certo). Acima de 16,
  `WARN`: cada worker prende uma conexão de um orçamento (100 por padrão no
  PostgreSQL) que a frota inteira divide. `pool_size = 0` deriva `workers + 1`;
  um `pool_size` abaixo de `workers + 1` é exit 1: a sonda de saúde tira a
  conexão dela do mesmo pool, e com `pool_size = workers` escritas lentas mas
  boas prendiam todas, a sonda estourava o prazo esperando uma, o sink ia a
  DOWN e, passado o `fail_timeout`, a FATAL com o banco saudável (revisão
  independente do `go-relplog-pg-server`, 2026-10-13, portada). O `+ 1` basta aqui, e só aqui (conferido na revisão independente do
  `go-syslog-server`, 2026-10-13, D1, onde o `ring=none` faz dos receptores,
  até `ring.max_inflight`, os escritores e o `+ 1` não protege nada): o ring é
  só memória, quem escreve são os `sink.workers`, um `Write` (uma conexão) por
  vez cada, a reingestão roda antes deles e da sonda, e a sonda é uma só
  (`TestPoolLeavesTheProbeAConnection`, contra o PostgreSQL real: três
  escritas presas num `LOCK TABLE`, conferidas em `pg_stat_activity`, e a
  sonda responde com `pool_size = 4`; com `pool_size = 3`, não).
- **Sem `RULE` de INSERT ou UPDATE na fila.** O PostgreSQL recusa `INSERT … ON
  CONFLICT` numa tabela com uma (`0A000`, *feature_not_supported*): a validação
  do boot confere `pg_rewrite` (`ev_type` 2 e 3) e recusa nomeando as regras,
  FATAL; uma regra `ON DELETE` ou `ON SELECT` não toca o `INSERT` e passa. Uma
  regra criada depois do boot falha todo pedaço com `0A000`, que é `fatal` na
  tabela abaixo (como `network`, era retentada para sempre com o sink UP).
- **Validação no boot**, na primeira checagem válida, cada recusa FATAL imediato
  (exit 3, sem esperar `boot_timeout`): a tabela existe; as dez colunas com tipos
  compatíveis (`uuid` em `uuid` e `from_uuid`; `smallint`/`integer`/`bigint` em
  `level`, `step` e `event_nsec`; **só `bigint`** em `sid`; `text` ou
  `character varying` em `space` e `type`; `timestamp with time zone` em
  `event_at`; `jsonb` em `payload`); um índice **único** exatamente sobre
  `(uuid)`, sem predicado nem expressão; o papel tem `INSERT` e `SELECT (uuid)`
  (`GRANT INSERT, SELECT (uuid)`: o `ON CONFLICT (uuid)` lê a coluna árbitra, e
  só com `INSERT` o primeiro pedaço falharia com `42501`); o servidor não está
  em recuperação (`pg_is_in_recovery()`). Em recuperação é FATAL no boot e DOWN
  depois dele (uma réplica que um failover pode promover).
  **Herdado da revisão independente do pai (2026-10-06)** — também FATAL no
  boot, porque cada um passava na validação e depois recusava todo pedaço com o
  sink UP (§20.18): `sid` que não seja `bigint` (o `sid` de rede é um hash de 53
  bits: `22003`); `character varying(n)` em `space` ou `type` com `n` < 64
  (`22001`); o índice único `DEFERRABLE` (o `ON CONFLICT` não o aceita como
  árbitro: `55000`, retentado para sempre) ou inválido (um
  `CREATE UNIQUE INDEX CONCURRENTLY` que falhou); a tabela **particionada** (uma
  linha sem partição é `23514`); um banco com `server_encoding` diferente de
  `UTF8` (um `LATIN1` guardava o texto em dupla codificação, sem erro); um
  `timestamptz(p)` em `event_at` com p < 6 (arredonda o microssegundo em
  silêncio; `timestamptz(6)` é aceito — o tipo é comparado por
  `format_type(tipo, NULL)`); e **qualquer outro índice único** que não o
  árbitro, ou uma constraint de exclusão: o `ON CONFLICT (uuid)` absorve colisão
  só no árbitro, e um evento novo que colidisse em `UNIQUE (from_uuid, sid)`
  derrubava o pedaço inteiro com `23505` (`23P01` na exclusão). Um índice ainda
  não pronto (`indisready`) é ignorado; inválido mas pronto conta, porque ainda
  impõe unicidade.
  **Coluna extra obrigatória:** é FATAL se uma coluna fora das dez for `NOT
  NULL` sem default, não identidade e não gerada. Sem isso, a validação passava e
  toda escrita virava `23502`, que só derruba o processo depois de o boot ter
  dado a fila por boa (§20.18). `received_at` passa porque tem `DEFAULT now()`. Pelo catálogo, não
  por sonda: uma sonda dispararia triggers do operador e consumiria sequências,
  e a recusa dela não nomeia a coluna.
- Em tempo de execução, um `23514` que não nomeia constraint (a partição que
  sumiu) é `fatal`, não `logic`; um `from` que não é UUIDv7 é recusado antes do
  `INSERT`, como `logic` (não acontece com o que este processo produz nem com o
  que `ParseLine` aceita, §12.2).
- Durabilidade: a do servidor (`synchronous_commit`). A skill admite a fila
  `UNLOGGED`, que o servidor não confere: ela é apagada numa queda do PostgreSQL
  e só serve se a origem puder reenviar, o que nenhum emissor GELF faz (o GELF
  não tem confirmação por mensagem: o que o servidor aceitou o emissor esqueceu).

| SQLSTATE | Classe |
|---|---|
| `08`, `53`, `57`, `40` | `network` |
| `22`, `23` (fora do `ON CONFLICT`) | `logic` |
| **`42`** (esquema), **`3D`** (o banco não existe), `28`, `2F`, `42501`, `25006`, **`23502`**, `0A000`, `23514` sem constraint nomeada | **`fatal`** |
| qualquer outra | `network` |

`23502` (`not_null_violation`) é `fatal` (porte do `go-relplog-pg-server`,
2026-10-13; antes §22.1 item 5): o servidor nunca manda `NULL` nas dez colunas,
então ele vem da **tabela** — uma coluna `NOT NULL` acrescentada depois do boot,
o `DEFAULT` de `received_at` retirado, um trigger que escreve `NULL` — e, como
`logic` (classe `23`), descartaria todo pedaço com o sink UP, para sempre. Como
`fatal` é dump e exit 3, e o boot seguinte já recusa a coluna extra obrigatória
pelo nome. O custo, aceito: um trigger que anula uma coluna só em certas linhas
passa a derrubar o processo na primeira delas, em vez de descartar só aqueles
pedaços.

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

**A validação do boot tem prazo próprio** (porte do `go-relplog-pg-server`,
2026-10-13; antes §22.1 item 6): roda com `sink.op_timeout`, não no
`sink.health_timeout` (1 s) da checagem que a dispara. São ~18 idas e voltas
(conexão, SCRAM, as consultas de catálogo, cada uma preparada na primeira vez):
a ~55 ms de RTT nenhuma tentativa cabia em 1 s, e o boot saía com 3 no
`boot_timeout` culpando a rede. O prazo da checagem é trocado, não o
cancelamento: um `SIGTERM` durante o boot ainda interrompe a validação, porque
as checagens de saúde carregam o contexto de quem as chamou como "tempo de
vida" (`withLifetime`), que a validação vigia (`validationContext`). A checagem
periódica depois do boot não valida nada e segue no `sink.health_timeout`.

**A classe `42` é fatal e nunca `logic`**: uma tabela dropada classificada como
`logic` descartaria um lote a cada 5 ms, para sempre, num processo que parece
saudável. **A `3D` também é fatal** (revisão independente de 2026-10-07): um
banco que não existe não passa a existir esperando, e como `network` o boot
gastava o `boot_timeout` inteiro antes de sair. Esta tabela e
`TestClassifySQLState` mudam juntas.

### 10.6 `sink.type = mysql`

Removido no go-gelf-pg-server (§1.2).

### 10.7 `sink.type = nats` (padrão)

Removido no go-gelf-pg-server (§1.2).

### 10.8 `sink.type = none`

Removido no go-gelf-pg-server (§1.2): todo pedaço vai ao PostgreSQL; não há destino de benchmark.

### 10.9 `LOGHUB_EB_URL`

`loghub.eb_url` (env `LOGHUB_EB_URL`) é a URL do eventbus da convenção LogHub.
Aqui o único eventbus aceito é o PostgreSQL: a URL preenche a conexão de
`[sink.postgres]`.

| Esquema | Preenche |
|---|---|
| `postgres://`, `postgresql://` | `addr` (`host:porta`, porta 5432 se ausente; ou o diretório do socket de `?host=/dir`), `username`, `password`, `database` (do caminho) |
| outro (`nats://`, `tls://`, `redis://`, `rediss://`, `amqp://`...) | exit 1: não suportado. Uma URL de outro eventbus esquecida de uma implantação irmã derruba o boot em vez de ser ignorada |

**`sslmode`, a regra do TLS.** Este servidor nunca usa TLS (§1.2):

| `sslmode` na URL | Efeito |
|---|---|
| ausente, `disable`, `allow`, `prefer` | aceito: todos admitem conexão sem TLS (`prefer` é o padrão da libpq que muita URL gerada escreve por extenso) |
| `require`, `verify-ca`, `verify-full` | **exit 1**: pedem TLS, e aceitá-los mandaria a senha e todo evento em claro a quem pediu o contrário |
| qualquer outro valor | exit 1: não é um `sslmode` da libpq |

**Uma fonte só da verdade:** com `LOGHUB_EB_URL` preenchida, qualquer chave que a
URL **pode** expressar definida explicitamente (flag, env ou INI) é exit 1 com as
duas fontes no log, mesmo que esta URL não traga aquela parte (uma URL sem senha
quer dizer "sem senha", não "procure em outro lugar"). As chaves donas:
`sink.postgres.addr`, `username`, `password`, `database`.

As demais (`table`, `pool_size`) convivem com a URL. Consequência: a senha vai
**dentro** da URL. Um servidor só (a URL não é lista; uma senha pode conter
vírgula). Fora `sslmode` e `host=/dir` (o socket unix, sem host antes do
caminho), qualquer parâmetro de consulta é recusado, não ignorado. **Um
parâmetro repetido também é recusado**: em `sslmode=disable&sslmode=require` o
`url.Values.Get` lê o primeiro e a libpq vale o último, e a URL passaria pela
recusa do TLS dizendo o contrário. Com socket, a porta da URL é mantida:
`postgres://u:p@:5433/db?host=/run/pg` dá `addr = /run/pg:5433`.

**Recusas portadas do `go-relplog-server` e do `go-relplog-pg-server`**
(revisões independentes de 2026-10-11 e 2026-10-13, portadas em 2026-10-13),
todas exit 1 sem citar a URL:

- **Usuário e banco são obrigatórios** (DECISOES §7). Sem eles, a libpq tomaria
  o usuário do sistema e um banco de mesmo nome, e este servidor o padrão do
  registry (`gelf`): duas leituras da mesma URL, dois bancos. A URL é dona
  das duas chaves, então o operador não podia corrigir em outro lugar.
- **`?sslmode=` vazio**: a libpq o recusa, e lido como "ausente" a URL dizia
  duas coisas a dois leitores.
- **`?host=` vazio** (revisão independente do `go-syslog-server`, 2026-10-13,
  B4): a libpq e o pgx o leem como "ausente" e discam o padrão deles (o socket
  do diretório compilado, ou `localhost`), e este servidor discava o host da
  autoridade: duas leituras da mesma URL, dois servidores.
- **`host=` com `,`, `:` ou branco**: a libpq lê `host=` literalmente
  (`/run/pg,/x` são dois servidores, `/run/pg:5433` um diretório com `:` no
  nome).
- **Um `@` depois do primeiro `/`, `?` ou `#` da autoridade**, com a dica de
  `%2F`, `%3F` e `%23`: é um usuário ou senha com um desses caracteres sem
  codificar, que encurta a autoridade (`postgres://app:39182#Zs3@db/x` discava
  `app:39182` sem senha).
- **Fragmento**: a libpq não tem; um `#` é uma senha cortada ou uma consulta
  jogada fora (um `?sslmode=require` depois dele nunca chegaria à recusa do
  TLS).
- **Host com escape** (`postgres://%2Frun%2Fpg/db`): o parser do Go o recusa, e
  "invalid percent-escape" mandava o operador atrás da senha; a mensagem manda
  usar `?host=/dir`. Um `://` repetido diz que o da senha precisa ser
  codificado (`%3A%2F%2F`).
- **Só com espaços**: definida e vazia de conteúdo, o operador que quis
  desligá-la não saberia que as chaves `[sink.postgres]` é que valem. Vazia
  (`LOGHUB_EB_URL=`) continua "não definida".
- Nenhum texto do parser de URL vai ao erro (o `url.Error` cita a entrada, e o
  erro interno dele cita a senha até a barra), e um parâmetro de consulta
  recusado só é nomeado se for um nome da libpq (lista fixa): uma senha colada
  depois de um `?` ou `&` também é uma "chave". O valor de um `sslmode`
  desconhecido não é citado.

**`sink.postgres.addr`, venha da URL ou não, é um servidor só:** `host:porta`
com host não vazio, sem vírgula, espaço ou tab, sem `:` a não ser num IPv6
literal (`[h:5432]:5433`, o que `postgres://u@h:5432:5433/db` vira, era um nome
DNS que ninguém resolve, retentado o `boot_timeout` inteiro), e porta 1–65535
sem zero à esquerda; ou um diretório de socket começando com `/`, sem vírgula
nem branco, com um `:porta` opcional na mesma faixa. Nenhuma
mensagem de erro cita a URL (ela pode ter senha). O `--dump-config` mostra os
valores derivados como comentário, com a senha redigida, para que o dump relido
não dispare o conflito.

### 10.10 `sink.type = clickhouse`

Removido no go-gelf-pg-server (§1.2).

#### 10.10.1 Cliente

Removido no go-gelf-pg-server (§10.10).

#### 10.10.2 Concorrência e conexões

Removido no go-gelf-pg-server (§10.10).

#### 10.10.3 Escrita

Removido no go-gelf-pg-server (§10.10).

#### 10.10.4 Desduplicação e at-least-once

Removido no go-gelf-pg-server (§10.10).

#### 10.10.5 Validação no boot

Removido no go-gelf-pg-server (§10.10).

#### 10.10.6 Durabilidade e `async_insert`: decisão do administrador do ClickHouse

Removido no go-gelf-pg-server (§10.10).

#### 10.10.7 Classificação de erro

Removido no go-gelf-pg-server (§10.10).

#### 10.10.8 `Health()`

Removido no go-gelf-pg-server (§10.10).

#### 10.10.9 Retenção

Removido no go-gelf-pg-server (§10.10).

#### 10.10.10 Métricas

Removido no go-gelf-pg-server (§10.10).

#### 10.10.11 Autenticação e TLS

Removido no go-gelf-pg-server (§10.10).

---

## 11. Plano de controle: heartbeat beacon

Removido no go-gelf-pg-server (§1.2): o beacon só existia com `sink=nats`. O servidor não emite `beacon`, `ping`, `pong` nem sonar.

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

**JSON Lines: um envelope JLH pronto por linha** (a decisão 3 da entrevista de
SP, que vale para todo corte da família):

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
- Malformados ficam de fora quando `convert.accept_malformed = false`: o worker
  os recusaria também (§6.1), e o dump não os ressuscita. Contam em
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

No boot, todo `emg-*.jsonl` de `emergency.dir` é entregue à fila **antes** de
qualquer transporte abrir (§13.1 passo 9), do mais antigo para o mais novo
(ordem lexicográfica de nome), **qualquer que seja** o `emergency.enabled` atual:
só a **escrita** de dumps depende dele. Arquivos `.corrupt`, `.tmp` e qualquer
outro nome são ignorados.

**Em fluxo, nunca o arquivo inteiro.** Cada arquivo é aberto
(`emergency.Store.Open`) e lido em pedaços (`Reader.Next(sink.batch,
sink.batch_max_bytes)`), cada pedaço entregue antes de o próximo ser lido. Um
dump de JLH chega a ~6× o ring que o produziu (§3.9); carregado inteiro, ele
matava o boot por OOM, e o boot seguinte tentava de novo, em laço. Um único
`sink.boot_timeout` **sem progresso** cobre a reingestão inteira, não um por
arquivo: todo pedaço entregue empurra o prazo. Um prazo único desde o início se
gastava com sucessos, e uma reingestão saudável de um dump grande que passasse
do `boot_timeout` saía com 3 no primeiro soluço, sem retentativa, e todo boot
seguinte recomeçava o mesmo arquivo (revisão independente do
`go-relplog-pg-server`, 2026-10-13, portada).

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
energia, e o dump seria entregue de novo) e, na quarentena, ainda o `ERROR` com
as contagens de bytes sob o nome novo. Antes, "cannot remove … it will be
delivered again" para um arquivo que já não existia, ou "cannot quarantine"
com o caminho antigo e sem as contagens.

**Sem conversão.** A linha é entregue como está; as colunas da `loghub.inbox`
(`uuid`, `from_uuid`, `space`, `level`, `sid`, `event_at`/`event_nsec`, §3.7.1)
saem do **cabeçalho da própria linha**. `type` precisa ser `"gelf"` e `step`,
`0`. Consequência: um dump sobrevive a uma troca de chaves `[convert]` (ou de
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
- profundidade de aninhamento até 66: o documento GELF aceita 64 níveis
  (§6.2), e o valor mais fundo dele cai em `payload.extra`, três níveis abaixo
  da raiz do envelope;
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
- `type = "gelf"`, `step = 0`, `level` 0–7, `sid` ≤ 2^53 − 1;
- `event.uts` em 0..253402300799 e `event.sub` em 0..999999999; `event.iso`
  igual ao derivado de `uts`/`sub`;
- o instante do `uuid` (ms) igual ao de `event`: `uuid` e `event` são uma
  amostra só (§3.1);
- o `payload` que o sink corta por posição (`jlh.Payload`, depois de
  `},"payload":`) igual ao `payload` que o parser JSON viu. Uma linha em que os
  dois discordam poria um objeto na fila enquanto o cabeçalho diz outro;
- `payload` um objeto com a chave de primeiro nível `short-message`,
  exatamente com essa grafia, e uma string — e, se a chave se repetir, a
  **última** ocorrência, que é a que o `jsonb` guarda. Não é uma CHECK da fila
  (a `inbox_syslog_ck` de SP, que exigia `message`, não olha `type=gelf`): é a
  chave que o conversor sempre escreve (§3.2), e uma linha sem ela não foi
  escrita por este programa;
- todo número da linha segue a gramática do JSON (RFC 8259 §6). Ao contrário
  de SP, cujo payload só tinha inteiros, o payload GELF carrega o texto dos
  números do emissor (`1.50`, `1e400`, até `1e1000000`, §3.3). Os que o `jsonb`
  não guarda (`22003`) **não** são recusados aqui: a cópia da fila os leva como
  string (§3.7.1), exatamente como faz com a escrita do worker, e um dump volta
  como o tráfego vivo teria voltado.

O que passa não pode ser recusado pelas CHECKs da fila nem pelo `jsonb`.

**Linhas e arquivos danificados:**

| Situação | Efeito |
|---|---|
| a última linha não termina em `\n` | cauda **truncada** (o dump foi cortado); as linhas íntegras antes dela são entregues |
| linha completa que `ParseLine` recusa | **inválida**: pulada; o `\n` localiza a próxima |
| linha maior que `emergency.MaxLine` | **inválida**: descartada durante a leitura, nunca guardada |
| arquivo com cauda truncada ou linha inválida | **danificado**: as linhas íntegras são entregues e o arquivo é renomeado para `.corrupt` (com `fsync` do diretório), com as contagens de bytes em `ERROR`; destruir a evidência de uma queda não é decisão deste programa |
| arquivo limpo | removido (com `fsync` do diretório) **só depois** de todos os seus envelopes serem confirmados. Uma falha da remoção é só `ERROR`: o arquivo volta no boot seguinte, e a fila só deduplica enquanto o `uuid` ainda estiver nela (TESTING §8). É regra, não pendência (porte do RP/RN, decidido em 2026-10-14): sair com erro deixaria o boot em laço por um problema (permissão do diretório) que não impede a entrega, e os gauges de §14.1, recontados do disco no fim da reingestão, continuam contando o arquivo, então o alerta segue disparando (`TestRemovalFailureIsOnlyAnError`). |

`emergency.MaxLine` = 6 × 16 MiB + 8 KiB: o pior JLH de §3.9 com o **maior**
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
    resolver nada). LOGHUB_EB_URL é expandida e conferida aqui (§10.9).
    Erro ou combinação inválida => exit 1
 2. data_dir: resolvido para absoluto, criado (0750), exportado como DATADIR
 3. lhident.Initialize() — falha => o processo sai com 100–114, pela biblioteca
 4. Identity congelada; space e from validados (§13.2) => exit 1 se inválidos
 5. porta ADMIN aberta: /healthz 200, /readyz 503 com o motivo, /metrics
 6. emergency.dir criado e provado gravável (com emergency.enabled; a
    reingestão do passo 9 lê o diretório de qualquer forma)
 7. SINK: pool preguiçoso; WaitReady com retentativa até sink.boot_timeout
    (300 s) => exit 3; validação de tabela, tipos, índices, permissão e
    recuperação (§10.5), com prazo próprio de sink.op_timeout: recusa =>
    exit 3 IMEDIATO, com o log "the sink refused this server permanently"
    (e não "within sink.boot_timeout")
 8. RING memory, workers (ainda parados) e o pipeline de recepção montados
 9. antes, o temporário gêmeo de um dump é removido e o de um dump
    interrompido vira um WARN com o caminho, intocado (§12.1);
    reingere emg-*.jsonl (§12.2), sem conversão, EM FLUXO (nunca o
    arquivo inteiro na memória) e PEDAÇO A PEDAÇO, com um único
    sink.boot_timeout para tudo (sink.Instrumented.Deliver): `logic` descarta, conta e loga aquele
    pedaço e segue para o próximo; `network` reenvia o MESMO pedaço com
    backoff dentro de sink.boot_timeout; `fatal` sai com 3; um sinal sai
    com 130
10. inicia a saúde e os workers; abre os transportes: udp, tcp, http
11. /readyz 200
```

Um sinal que cancela o `bind` da porta admin (passo 5) ou de um transporte
(passo 10) — a resolução de um nome de host falha com "operation was
canceled" — é um boot interrompido, **130**, não um endereço ruim (1) (revisão
independente do `go-relplog-pg-server`, 2026-10-13, portada).
Com IP literal nenhuma resolução falha e os `bind` passam: o
sinal é conferido depois de todos os `bind` do passo 10 e antes do primeiro
`Start`, e também é **130** (antes o servidor abria, o `/readyz` dava 200 e o
shutdown que seguia saía 0; revisão independente do `go-syslog-server`,
2026-10-13, A1; `TestSignalBeforeALiteralBindExits130`).

**Identidade antes de tudo que escreve.** `lhident.Initialize()` roda **uma
vez**, no `main`, depois das flags que encerram o programa e antes de abrir
qualquer recurso com `defer` ou criar goroutine (regra 2 da skill). A app recebe
um struct `Identity` (`From`, `Space`, `AgentName`, `Hostname`, `MachineID`)
preenchido dos getters: os testes montam a app dezenas de vezes no mesmo
processo, e uma segunda chamada a `Initialize()` sai com 112. `AgentName` e
`Hostname` compõem o `application_name` da conexão,
`gelf-pg/<AgentName>@<Hostname>` (§10.5).

**`DATADIR` é exportado antes do `Initialize()`** para que a biblioteca grave
`agent_uuid` e `machine_id` na mesma raiz em que o servidor grava `emergency/`
(§16.6). Sem `DATADIR` absoluto, o padrão `./data` vira absoluto a partir do
diretório de trabalho; no contêiner é `/data`.

**A porta admin abre antes do sink:** num crashloop por banco indisponível, o
operador tem `/readyz` com o motivo e `/metrics`.

**Os transportes abrem por último e juntos.** Abrir o UDP antes de o sink estar
pronto e de os dumps terem voltado descartaria datagramas sem ninguém saber; a
única defesa do UDP é não estar escutando.

### 13.2 Validação de configuração

| Combinação | Efeito |
|---|---|
| Nenhum dos três transportes (`listen.udp`, `listen.tcp`, `listen.http` todos vazios) | exit 1 |
| `listen.udp`, `listen.tcp` ou `listen.http` fora de `host:port` (`:12201` serve); `*_listeners` fora de 1–256 | exit 1. UDP e TCP podem dividir a porta (são protocolos distintos; 12201 nos dois é o padrão) |
| `listen.tcp` e `listen.http` no mesmo endereço | exit 1, nomeando as duas chaves; a regra de endereço é a da linha seguinte (o `SO_REUSEPORT` aceitaria o bind duplo e entregaria documentos GELF ao servidor HTTP) |
| `listen.udp_reassembly_max_bytes` fora de 1 MiB–4 GiB; `listen.udp_reassembly_max_messages` fora de 1–1 048 576 | exit 1 (§5.2) |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| `admin.enabled = true` com `admin.bind` e `listen.tcp` ou `listen.http` no mesmo endereço TCP: a mesma porta com o mesmo literal IP (o IPv4 mapeado em IPv6 conta como o IPv4), um curinga (`""` ou um literal não especificado: `0.0.0.0`, `::`, `::0`) de um lado, ou um **nome** de host de qualquer lado | exit 1, nomeando os dois endereços. Nenhum nome é resolvido na validação: um nome na mesma porta pode resolver ao outro endereço (`localhost` e `127.0.0.1`) e conta como colisão. Para dividir a porta, dois literais IP distintos; a porta `0` nunca colide; `listen.udp` não colide (outro protocolo). Antes a colisão só aparecia ao abrir o transporte, com a porta admin já tendo servido o boot inteiro, e no macOS/BSD nem isso: o `SO_REUSEADDR` deixa `127.0.0.1:P` e `*:P` coexistirem, e a porta admin tomava em silêncio as conexões TCP de `localhost`. A regra é a do `sameStreamSocket` do `go-syslog-server` (porte do RP/RN, 2026-10-14; `TestAdminBindCollidesWithListen`). O curinga é reconhecido pelo **valor**, não pela grafia: `""` ou qualquer literal com `IsUnspecified()` (`0.0.0.0`, `::`, `::0`, `0:0::0`); e a **porta é comparada como número**, lida como o `net.Listen` a lê (`net.LookupPort`): `0514` é `514`, e um nome de serviço (`syslog`, `http`) vale o número do banco de serviços; uma porta que não é número nem serviço conhecido é exit 1 aqui, nomeando as duas chaves, em vez de falhar no bind (revisão independente de 2026-10-14, D1 do syslog e D4 do RELP: `[::0]:P` contra `127.0.0.1:P` e `0514` contra `514` passavam como endereços distintos; `TestSameStreamSocketReadsValuesNotSpellings`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `proxy.protocol ≠ off` sem `proxy.trusted` | exit 1 |
| `space` fora de `^[a-z0-9-]+(\.[a-z0-9-]+)*$` | exit 1 (a lib aceita `a..b`, `.x` e `x.`; segmento vazio não serve à convenção LogHub). Acima de 64 caracteres a própria lib já sai com 111 antes |
| `from` fora do UUID canônico em minúsculas | exit 1 |
| `convert.keep_raw` fora de `malformed`, `always` | exit 1 |
| `convert.max_message_bytes` < 1 KiB ou > 16 MiB | exit 1 (16 MiB mantém o pior JLH, §3.9, abaixo dos 255 MiB de um `jsonb`) |
| `http.read_header_timeout`, `http.read_timeout` ou `http.idle_timeout` zero; `http.max_conns`, `http.max_conns_per_ip` ou `http.max_requests_per_conn` negativo; `http.bulk_max_bytes` fora de 1 KiB..64 MiB (conferido mesmo com `http.bulk=false`) | exit 1 (os prazos são a defesa contra slowloris; `0` nas conexões e no teto de requisições é "sem limite") |
| `ring.max_bytes` abaixo do custo de uma mensagem do tamanho máximo: a classe do pool de `convert.max_message_bytes` + 288 B (`item.PooledCap(n) + ring.ItemOverhead`, §9.2, §15.1; 131 360 B com o padrão de 128 KiB, acima da maior classe, em que o buffer é o próprio documento) | exit 1, nomeando a chave e o piso: esse documento nunca caberia num ring vazio; no TCP a conexão ficaria parada até o `tcp.backpressure_timeout` e o documento seria descartado, a cada um, no HTTP seria `503` para sempre, e no UDP descartado sempre como `ring_full`, com o ring vazio. Porte do RP/RN, 2026-10-14 (`TestRingMaxBytesFitsTheLargestMessage`) |
| **Tetos** (revisão independente do `go-relplog-pg-server`, 2026-10-13, portada): `ring.capacity_items` > 16 777 216, `sink.workers` > 1024, `sink.batch` > 65 536, `sink.batch_max_bytes` > 256 MiB, `sink.postgres.pool_size` > 4096 | exit 1. Sem eles, um erro de digitação de poucos dígitos derrubava o boot em pânico (`makechan`), alocava um gigabyte por `Dequeue`, dava a volta no `int32` do pool (4294967300 virava 4) ou montava um pedaço cujo `Bind` passa do limite de 1 GiB da libpq e falha sem SQLSTATE, `network` para sempre. Os números são folgados: só barram o erro de digitação |
| `ring.capacity_items`, `ring.max_bytes`, `sink.workers`, `sink.batch`, `sink.batch_max_bytes`, `sink.max_retries` abaixo de 1; `ring.high_water` fora de (0, 1]; `log.access_sample` fora de [0, 1]; `tcp.max_conns*` ou `http.max_conns*` negativo; `retry_backoff_max < retry_backoff_min` | exit 1 |
| `sink.workers > 16` | `WARN` (§10.5) |
| `sink.postgres.pool_size` negativo, ou abaixo de `sink.workers + 1` | exit 1 (`0` deriva `workers + 1`; a sonda de saúde precisa de uma conexão, §10.5) |
| `sink.postgres.addr`, `username` ou `database` vazios; `addr` que não é um servidor só (`host:porta` com host não vazio, sem vírgula, porta 1–65535 sem zero à esquerda, ou diretório começando com `/` com `:porta` opcional, §10.9); `table` que não é identificador em minúsculas (com esquema opcional) | exit 1 |
| `LOGHUB_EB_URL` em conflito, com esquema que não é `postgres://`/`postgresql://`, com `sslmode` que pede TLS ou vazio, com parâmetro desconhecido ou repetido, com fragmento, com `@` depois de `/`, `?` ou `#`, sem usuário ou sem banco, com `host=` vazio ou que não é um diretório só, ou só com espaços (§10.9) | exit 1, sem citar a URL |
| durações zero onde zero não é "sem limite" (§16.5) | exit 1 |
| `GOMEMLIMIT` ausente, ou `< ring.max_bytes × 2` | `WARN` (§15.4) |
| parâmetro desconhecido na linha de comando ou no INI | exit 1. Isto inclui toda chave do pai que saiu (`ring.mode`, `sink.type`, `[tls]`, `[filter]`, `[ratelimit]`, `[sink.nats]`...): um INI do `go-gelf-server` não sobe aqui em silêncio |
| linha do INI que não é `[seção]` nem `chave = valor` (um cabeçalho sem `]` inclusive); argumento da linha de comando que não começa com `-` | exit 1, nomeando o número da linha ou só o que vem antes do `=` do argumento, **nunca o conteúdo**: `password: x` (sem `=`) ou `sink.postgres.password=x` (sem `--`) iam inteiros para o stderr do contêiner, no boot e a cada `SIGHUP` (porte do `go-syslog-nats-server`, 2026-10-11) |
| flag sem valor seguida de outra flag (`--sink.workers --sink.postgres.password=x`, um `$WORKERS` vazio num script); valor depois de um espaço (`--sink.postgres.password= x`, `--sink.postgres.password = x`, `--sink.postgres.password= -x`); valor que o shell partiu num espaço (`--sink.postgres.password=$P`, com espaço em `$P`); valor que começa com `--` na forma separada (`--sink.postgres.password --x`) | exit 1, nunca com o valor: um argumento que começa com `--` (ou `-` com o nome de um parâmetro ou de um *switch*) nunca é tomado como valor de outra flag, e só é nomeado se for uma flag deste servidor; o argumento depois de um valor vazio ou de um `=` sozinho não é citado, nem como nome de flag desconhecida; uma palavra solta depois de qualquer valor não é citada. Um valor que começa com `-` vai depois do `=`. A cauda partida que começa com traço depois de um valor **não** vazio (`--sink.postgres.password=a -b`) só é nomeada quando tem a forma `<seção>.<chave>` de uma seção que este servidor tem, ou que saiu do pai (`--sink.postgres.tabel`, `--filter.min_severity`): um erro de digitação dentro de uma seção conhecida continua nomeado, e `-hunter2` ou `--x=y` não (`knownSectionOf`, do `go-relplog-server`, portado em 2026-10-13). Um `-h`/`-v` de um traço depois de uma flag que quer valor não é nomeado (é como um valor de dois caracteres começando com traço se parece); `--help` é (o parser do `go-webhook-nats-server`, portado em 2026-10-11) |
| `--help`, `--version` ou `--dump-config` com valor (`--dump-config=false`) | exit 1, sem citar o valor: um *switch* não tem valor, e um valor ignorado pode dizer o contrário (despejava e saía 0) |
| `--config=` vazio | exit 1: caía no `GELF_CONFIG`, o contrário do que diz |
| nome de parâmetro com algo colado depois (`--sink.postgres.passwordhunter2`, `…password-hunter2`), ou com caracteres que nome nenhum tem (`password: hunter2 = x`, `--sink.postgres.password:hunter2`) | exit 1 como parâmetro desconhecido, **sem** citar o nome: o `=` esquecido põe o valor dentro dele |
| no INI, chave desconhecida logo depois de um segredo (`password = abc` e, na linha seguinte, `ffd3a9c1b2=`), ou cuja seção não existe aqui nem no pai, ou que não tem a forma de uma chave (`^[a-z0-9_]{1,64}$`) | exit 1 nomeando só a linha: é o resto de um valor que quebrou de linha, e o erro vai ao stderr e, num `SIGHUP`, ao log |
| BOM UTF-8 no começo do INI | ignorado (alguns editores do Windows o gravam; mantido, a linha 1 era "expected 'key = value'" sem nada visível) |
| variável de ambiente do pai que este servidor não tem (`GELF_LISTEN_TLS`, `GELF_LISTEN_HTTPS`, `GELF_TLS_*`, `GELF_FILTER_*`, `GELF_RATELIMIT_*`, `GELF_SINK_TYPE`, `GELF_RING_MODE`, as dos outros sinks e rings, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE`...) | **um** `WARN` que nomeia todas e diz que são ignoradas (`internal/config/removed.go`, §16.2). Não é erro porque um mesmo `.env` pode alimentar os dois servidores; sem o aviso, o env é lido só pelos nomes do registry e elas sumiriam em silêncio, e quem trocou um servidor pelo outro acreditaria ter um listener TLS ou um filtro |

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
  §10.5): FATAL na primeira checagem, sem passar pelos fusíveis. Esperar não
  conserta nenhuma delas.
- **FATAL é terminal.** Toda transição e o gauge `gelf_sink_up` são escritos
  sob o mesmo lock, e nada sai de FATAL: uma checagem concorrente não pode
  devolver o estado a UP/DOWN depois de um `ClassFatal` do `Write()`.
- `ClassNetwork` no `Write()` **não** muda o estado; o pedaço é retentado.
- Em **DOWN**, vale o backpressure de §2.3 mesmo com o ring vazio (fail-fast), e o
  `/readyz` falha.
- Em **FATAL**: §12 e exit 3.

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
fase 5  o que sobrou vai para o dump (§12); fecha o sink e o ring; exit 0
        (ou 3, abaixo)
```

- O contexto de recepção é cancelado **antes** de os transportes fecharem: é o
  que solta um leitor preso no laço de backpressure. Um item já entregue ao ring
  não é afetado.
- `drain_timeout` estourado com itens: §12 e exit 3.
- **Segundo sinal** em qualquer fase: saída imediata, `exit 130`, aceitando a
  perda. Conta em `reason="abort"` o que o ring ainda tinha **e** o que já fora
  entregue ao dump em andamento (o `Drain` já os tirou da contabilidade do
  ring; antes da revisão, um abort durante o dump contava 0).
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
  recepção trata como shutdown (TCP fecha a conexão sem ler adiante; HTTP
  responde `503`; UDP descarta), contado em `gelf_messages_dropped_total{reason="shutdown"}`, e
  nenhuma mensagem é aceita depois da contagem sem ser contada. O `Enqueue`
  reconfere o fechamento **depois** de reservar a vaga (`items.Add(1)`): ou a
  reserva é vista pela leitura do aborto (contada), ou o `Enqueue` vê o
  fechamento e desiste — no máximo uma sobrecontagem transitória, nunca uma
  mensagem aceita sem contagem. A sobrecontagem tem uma segunda fonte (registrado em 2026-10-14, D3 da revisão do `go-syslog-server`): um `Enqueue` que reservou a vaga (`items.Add(1)`) e vai desistir por falta de bytes (`ErrFull`) pode ser lido pelo aborto entre a reserva e o recuo, e conta em `abort` uma mensagem que a recepção trata como ring cheio. É transitória, de no máximo uma por recepção concorrente, e nunca esconde uma perda. A janela não era só o `pre_stop_delay`: um
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
- **Uma requisição HTTP em andamento** na fase 3 recebe `503` com
  `Retry-After` (o contexto de recepção cancelado), e o `http.Server` espera
  até `closeGrace` pelas que já estavam no handler.
- **`SIGHUP` vale o processo inteiro.** É capturado desde a primeira linha; fora
  da janela entre o fim do boot (passo 11) e o início do shutdown, é logado em
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
| `0` | shutdown completo, ring vazio |
| `1` | erro de configuração ou de inicialização não relacionada ao banco (inclui a `LOGHUB_EB_URL` com `sslmode` que pede TLS) |
| `3` | banco indisponível ou inutilizável: sink FATAL, `boot_timeout` ou `drain_timeout` estourados, validação de §10.5 recusada, dump que falhou |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` (`DATADIR`, `MACHINE_ID`, `MACHINE_ID_FILE`, `AGENT_NAME`, `AGENT_UUID`, `HOSTNAME`, `WORKSPACE`; 112 = `Initialize()` duas vezes); a tabela completa está na skill `go-loghub-ident` e em `docker/README.md` |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 7–9 do boot, e o que cancela os *bind* da porta admin (passo 5) ou dos transportes (passo 10, §13.1) |

---

## 14. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9104`, **o mesmo do `go-gelf-server`** (a porta
da família GELF; S e SP usam 9102, R 9103, W 9101): um operador pode trocar um
pelo outro sem mexer no monitoramento. Por isso **não rode dois servidores GELF
no mesmo host sem trocar o `admin.bind` de um deles**. HTTP/1.1.

O `admin.bind` nunca divide o endereço TCP do `listen.tcp` nem do
`listen.http` (§13.2): mesma
porta com o mesmo literal, um curinga ou um nome de qualquer lado é exit 1 na
validação.

| Path | Semântica |
|---|---|
| `/healthz` | `200` enquanto o processo vive. Liveness. |
| `/readyz` | `200` só se: sink `UP`, ring abaixo de `ring.high_water`, boot concluído (inclusive a reingestão), transportes abertos e não em shutdown. Senão `503` com o motivo em `text/plain`. |
| `/metrics` | exposição Prometheus em texto. |

### 14.1 Métricas

Prefixo `gelf_`, **o mesmo do pai**: um painel do `go-gelf-server` lê este
servidor. **Nenhum label deriva de conteúdo controlado pelo emissor** (nada de
host, campo, IP); todo label tem conjunto finito conhecido em compilação.
`level` **não** é label: o produto com `transport` multiplicaria as séries, e a
distribuição por `level` é pergunta para o armazenamento definitivo.

| Métrica | Tipo | Labels |
|---|---|---|
| `gelf_messages_total` | counter | `transport` (udp, tcp, http) |
| `gelf_messages_accepted_total` | counter | `transport` |
| `gelf_messages_dropped_total` | counter | `reason` (ring_full, sink_down, malformed_frame, proxy, empty, shutdown, too_large, decompress, chunk_invalid, chunk_timeout, chunk_overflow) |
| `gelf_message_bytes` | histogram | — (tamanho do documento descomprimido) |
| `gelf_convert_total` | counter | `format` (gelf, malformed) |
| `gelf_convert_warnings_total` | counter | `code` (os 25 de §6.8) |
| `gelf_convert_duration_seconds` | histogram | — |
| `gelf_conns_active`, `gelf_conns_total` | gauge, counter | `transport` (tcp, http) |
| `gelf_conns_refused_total` | counter | `transport` (tcp, http), `limit` (global, per_ip); `global` conta cada vez que o accept estaciona no teto, um limite inferior, porque o teto global bloqueia em vez de recusar |
| `gelf_proxy_headers_total` | counter | `result` (v1, v2, local, absent, invalid) |
| `gelf_backpressure_seconds_total` | counter | `transport` (tcp) |
| `gelf_confirm_duration_seconds` | histogram | — (do documento completo à entrada no ring; TCP e HTTP, só aceitos) |
| `gelf_udp_receive_errors_total` | counter | — |
| `gelf_udp_read_buffer_bytes` | gauge | — (o `SO_RCVBUF` efetivo, lido de volta) |
| `gelf_udp_reassembly_bytes` | gauge | — (bytes presos em mensagens UDP incompletas, §5.2) |
| `gelf_udp_chunk_duplicates_total` | counter | — (chunks repetidos, ignorados) |
| `gelf_ring_items`, `gelf_ring_bytes` | gauge | — |
| `gelf_ring_capacity_items`, `gelf_ring_capacity_bytes` | gauge | — |
| `gelf_sink_batch_size`, `gelf_sink_batch_bytes` | histogram | — (por pedaço) |
| `gelf_sink_duration_seconds` | histogram | — |
| `gelf_sink_health_duration_seconds` | histogram | — |
| `gelf_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); o cancelamento do shutdown não conta. `fatal` conta **toda** recusa fatal do destino, mesmo durante um cancelamento (boot cancelado, shutdown): ela não nasce dele, é um veredito real, e o processo sai logo depois. Só o `network` cortado pelo cancelamento deixa de contar, porque o cancelamento **causa** erros de rede (prazo, conexão fechada). Regra, não pendência (porte do RP/RN, decidido em 2026-10-14). |
| `gelf_sink_up` | gauge | — |
| `gelf_sink_postgres_duplicates_total` | counter | — |
| `gelf_sink_postgres_pool_conns` | gauge | `state` (acquired, idle, max) |
| `gelf_sink_postgres_pool_waits_total` | counter | — |
| `gelf_events_written_total` | counter | — |
| `gelf_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero, como todo label de conjunto fechado (um `rate()` precisa da série antes do primeiro evento) |
| `gelf_emergency_files`, `gelf_emergency_events_pending` | gauge | — (arquivos e envelopes pendentes no disco, recontados do diretório no fim da reingestão e somados a cada dump; durante a reingestão, `files` é o número de arquivos listados e `events_pending` não muda, §12.2) |
| `gelf_build_info` | gauge | `version`, `commit`, `go` |
| `gelf_config_info` | gauge | `ring_mode` (sempre `memory`), `sink_type` (sempre `postgres`), `transports` |
| `gelf_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

Saíram, com o que as produzia: `reason` `ring_unavailable`, `filter` e
`ratelimit`; os labels `tls` e `https` em `transport`; `gelf_tls_handshake_errors_total`;
`gelf_ring_quarantined_total` e `gelf_ring_redis_claimed_total`; as séries
de `sqlite`, `mysql`, `nats` e `clickhouse`; `gelf_beacons_total`;
`gelf_ratelimit_buckets`. Uma série que nunca pode subir não é exposta: diria
ao operador que existe uma medição onde não existe nenhuma.

**Fronteiras de histograma** são estáticas, idênticas em toda instância e
**pré-formatadas como string** na inicialização: emitir `0.01` num scrape e
`0.010` no seguinte cria duas séries e quebra todo quantil.

### 14.2 Logs

`slog`, `json` por padrão (`text` disponível). **Nenhum log síncrono por mensagem
no caminho padrão.** `log.access = true` liga log por mensagem com
`log.access_sample` (0.01); num coletor de log isso é logar log. Avisos de anomalia
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

O buffer volta ao pool pelo `item.PutRaw` de G (o pacote é copiado
literalmente, §3.4): a otimização de SP que guardava a caixa `*[]byte` no item
(`CopyRawBox`/`NewBoxed`, uma alocação a menos por mensagem) **não** veio, para
não divergir do pacote da família. Custo medido em SP: 24 B e uma alocação por
documento aceito (DECISOES §2).

**Memória por conexão, fora do ring.** Uma conexão TCP viva segura o leitor de
4 KiB e o buffer de quadro (2, 8 ou 64 KiB, solto acima de 16 KiB com o leitor
vazio), ociosa ou não; os números e a regra estão em §5.3. Não entram em
`ring.max_bytes`: o teto delas é `tcp.max_conns`, e com `0` é o `GOMEMLIMIT`
(§15.4) que segura um enxame de conexões caladas.

**Memória por requisição HTTP.** Cada requisição em voo segura o corpo como
chegou do fio (um buffer que cresce por classes com o que **chega**, até
`L + 16 KiB + 1`, nunca pelo `Content-Length`, §4.5) e o documento
descomprimido (até `L + 1`); com `http.bulk`, `B + 16 KiB` e `B` (§4.5.1). Os
dois vêm de pools; um buffer acima de 64 KiB não volta ao pool (seguraria `L`
por P ocioso). Uma conexão keep-alive serve uma requisição de cada vez, então
o teto é `http.max_conns` e, com `0`, o `GOMEMLIMIT`; entre requisições ela
não segura buffer.

**Teto pequeno demais.** Um documento de `n` bytes custa ao ring a classe do
pool de `n` + 288 B (§9.2): 2 KiB, 8 KiB ou 64 KiB, e acima da maior classe o
próprio `n`. O piso de `ring.max_bytes` é o custo de um documento do tamanho
máximo, `item.PooledCap(convert.max_message_bytes) + ring.ItemOverhead`
(131 360 B com o padrão de 128 KiB), e um valor menor é recusado na validação
(§13.2, exit 1). O piso da **menor** classe não basta: um ring de 4 KiB
aceitaria os documentos curtos e recusaria **sempre** qualquer um de 3 KiB,
mesmo vazio — no TCP a conexão daquele emissor parava até o
`backpressure_timeout` a cada um. Com o piso, todo documento aceito pelo teto
de §5.5 cabe num ring vazio (porte do RP/RN, 2026-10-14; antes aceito e listado
em TESTING §8).

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
| qualquer outro (o ring em memória não tem; a regra fica para o caso de aparecer) | não há como provar que ninguém | **descarta sem devolver** (`Discard`) |

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

### 15.4 Runtime

- `GOMEMLIMIT` deve ser configurado: `WARN` se ausente ou abaixo de
  `ring.max_bytes × 2`. O compose define `GOMEMLIMIT` **e** o limite do
  contêiner, o segundo acima.
- `GOGC` e `GOMAXPROCS` no padrão (o Go 1.25 respeita o limite de CPU do cgroup).
- `time/tzdata` é embutido no binário (herdado do `go-gelf-server`): nenhuma
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
  (`GELF_SINK_POSTGRES_ADDR`), com as exceções de §16.2.
- INI: seção `[ring]`, chave `capacity_items`.
- `--config=/caminho.ini` (env `GELF_CONFIG`) indica o arquivo.

**Os mesmos nomes do pai.** O prefixo `GELF_` e cada chave que ficou têm o
nome que tinham no `go-gelf-server`: os dois são da mesma família, e um
operador pode trocar um pelo outro mantendo o env das chaves comuns.

### 16.2 Registry único, e os nomes de env fixados pelas skills

Uma **tabela** (`internal/config/registry.go`) declara cada parâmetro (seção,
chave, tipo, padrão, descrição, se é recarregável, nome de env). Dela saem o
INI, as flags, o env, o `--help`, o `--dump-config` e a validação. Um parâmetro
novo entra só ali. Sem `gopkg.in/ini.v1` e sem `spf13/pflag`.

Nomes de env que **não** seguem a regra, porque uma skill os fixou:

| Seção | Env | Origem |
|---|---|---|
| `[loghub]` | `LOGHUB_EB_URL` | skill `loghub-json-schema` |
| `server.data_dir` | `DATADIR` | `go-loghub-ident` |

Nenhuma seção deste servidor tem nome especial: `[convert]` segue a regra como
as outras (`GELF_CONVERT_KEEP_RAW`, `GELF_CONVERT_MAX_MESSAGE_BYTES`,
`GELF_CONVERT_ACCEPT_MALFORMED`). Não existe skill `type=gelf` que fixe nomes
sem a seção, ao contrário do `SYSLOG_<CHAVE>` do `[syslog]` do `go-syslog-server`;
os nomes do `go-syslog-server` (`SYSLOG_CONFIG`, `SYSLOG_*`) não são lidos.

As variáveis de identidade (`AGENT_UUID`, `AGENT_NAME`, `WORKSPACE`, `HOSTNAME`,
`MACHINE_ID`, `MACHINE_ID_FILE`, `LOGHUB_IDENT_DEBUG`) **não** passam pelo
registry: são lidas pelo `go-loghub-ident`, e o `--dump-config` mostra o que o
**ambiente** diz numa seção informativa `[identity]`, sem resolver nada
(§13.1 passo 1: o dump não toca identidade nem disco).

**O `--dump-config` relido reproduz a config**: um valor **derivado** que
ficou no padrão (`sink.postgres.pool_size` de `sink.workers`, `emergency.dir`
de `server.data_dir`) sai comentado, com a nota; escrito como chave, viraria
explícito na releitura e deixaria de acompanhar o que o deriva (revisão
independente do `go-relplog-pg-server`, 2026-10-13, portada).

**Chaves do pai que saíram.** Na linha de comando ou no INI são parâmetro
desconhecido, exit 1, e o erro diz de onde a chave vem ("go-gelf-server has
it", `removedParams`, a mesma lista do env em grafia pontuada). No ambiente, onde o registry lê só os próprios nomes e
nunca varre o resto, elas sumiriam em silêncio; por isso o boot confere a lista
de `internal/config/removed.go` (as 96 de G que não existem aqui: TLS, HTTPS, filtro, rate-limit, `sink.type`,
`ring.mode`, dos outros sinks e rings, `LOGHUB_BEACON_INTERVAL` e
`LOGHUB_CTL_NAMESPACE`) e emite **um** `WARN` que nomeia todas as definidas e diz
que são ignoradas (§13.2).

Durações aceitam a sintaxe do Go (`30s`, `5ms`). O inteiro puro em segundos
do pai existia só para `LOGHUB_BEACON_INTERVAL` e saiu com ele. Tamanhos aceitam
**só** `KiB`, `MiB`, `GiB` (sem diferença de caixa) e inteiros puros em bytes;
`KB`, `MB`, `K` e `M`, que a standard aceitava, são recusados.

### 16.3 Recarga por `SIGHUP`

Recarrega **só** o que é seguro trocar a quente e não muda os bytes do JLH:

- `log.level`, `log.access`, `log.access_sample`

(No pai entravam também `filter.*`, `ratelimit.*` e `sink.sqlite.keep_files`,
que saíram, §1.2.)

Toda chave de `[convert]` **exige restart**: ela muda a conversão, e o JLH de um
item não pode depender de **quando** ele foi drenado (§3.5). A identidade fica
congelada.

Duas referências: chave recarregável comparada com o que está **em execução**
(para que reverter ao valor de boot seja aplicado); chave de restart comparada
com o **boot** (para que todo `SIGHUP` avise sobre ela até o restart). Cada uma
das duas sozinha falha num dos casos (bugs reais 009 e 010 da standard).

**Chave derivada não é relatada.** Uma chave **derivada** no boot e no arquivo
novo (§16.2, a nota `derived` do `--dump-config`: `emergency.dir` de `server.data_dir` e `sink.postgres.pool_size` de `sink.workers`) não entra na lista
"requires a restart": muda só porque a chave que a deriva mudou, e essa é
relatada. Uma que passa de explícita a derivada, ou o contrário, é relatada;
notas que não começam com `derived` (`lowered to 4`, `forced to 1`, `resolved to
an absolute path`) dizem como um valor dado foi ajustado e continuam relatadas.
A comparação usa as notas que o `Load` já grava (`Loaded.Notes`), as mesmas do
`--dump-config` (porte do RP/RN, 2026-10-14; `TestReloadDoesNotReportDerivedKeys`, `TestReloadDoesNotReportTheDerivedPoolSize`).

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
keep_raw          = malformed
max_message_bytes = 128KiB

[ring]
capacity_items = 65536
max_bytes      = 256MiB

[sink]
workers = 4

[sink.postgres]
addr     = db-1.intranet:5432
username = gelf
database = loghub
table    = loghub.inbox

[admin]
bind = 0.0.0.0:9104

[log]
level = info
```

A senha vai por env (`GELF_SINK_POSTGRES_PASSWORD`) ou dentro da
`LOGHUB_EB_URL` — nesse caso sem `addr`, `username` e `database` no INI (§10.9).

### 16.5 Tabela de parâmetros

A fonte é `internal/config/registry.go`; o `--help` imprime a mesma tabela.

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `shutdown.drain_timeout`,
`tcp.backpressure_timeout`, `proxy.header_timeout`, `http.read_header_timeout`
e `http.read_timeout` — a validação recusa `0` com exit 1. Zero é legítimo onde diz o que faz: `shutdown.pre_stop_delay` (sem
espera), `sink.batch_max_delay` (sem espera), `sink.fail_timeout` (sem fusível
por tempo) e `tcp.idle_timeout` (sem limite).

#### `[server]`

| Chave | Padrão | Descrição |
|---|---|---|
| `data_dir` | `./data` (env `DATADIR`) | Raiz de tudo o que o serviço grava, inclusive a identidade; convertida em absoluto; `/data` no contêiner |

#### `[listen]` — vazio desliga o transporte

| Chave | Padrão | Descrição |
|---|---|---|
| `udp` | `:12201` | GELF UDP: datagramas inteiros ou chunks, gzip/zlib (§4.2, §5.2) |
| `udp_listeners` | `1` | 1–256; `>1` habilita `SO_REUSEPORT`; o único jeito de passar de um núcleo |
| `udp_read_buffer` | `8MiB` | `SO_RCVBUF`; o efetivo é lido de volta e exposto |
| `udp_reassembly_max_bytes` | `64MiB` | teto dos bytes presos em mensagens UDP incompletas, todas as origens juntas; 1 MiB–4 GiB (§5.2) |
| `udp_reassembly_max_messages` | `16384` | teto de mensagens UDP abertas; 1–1 048 576 |
| `tcp` | `:12201` | GELF TCP, moldura `\0` (§5.3) |
| `tcp_listeners` | `1` | 1–256; `>1` habilita `SO_REUSEPORT` |
| `http` | `:12202` | GELF HTTP, `POST /gelf` (§4.5) |

Não há `tls`, `tls_listeners` nem `https` (§1.2).

#### `[tcp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `idle_timeout` | `120s` | `0` = nunca |
| `max_conns` | `0` | teto do transporte, somando os listeners; bloqueia no accept; `0` = ilimitado |
| `max_conns_per_ip` | `0` | por IP do par TCP (antes do PROXY); fecha na hora; `0` = ilimitado |
| `backpressure_timeout` | `30s` | quanto tempo parar de ler antes de fechar (§5.7) |

#### `[tls]`

Removido no go-gelf-pg-server (§1.2).

#### `[http]`

| Chave | Padrão | Descrição |
|---|---|---|
| `read_header_timeout` | `10s` | prazo da linha de requisição e dos cabeçalhos; nunca `0` |
| `read_timeout` | `30s` | prazo da requisição inteira, corpo incluído; nunca `0` |
| `idle_timeout` | `60s` | espera de uma conexão keep-alive pela próxima requisição; nunca `0` |
| `max_requests_per_conn` | `0` | a resposta da N-ésima requisição leva `Connection: close`; `0` = sem teto |
| `bulk` | `false` | um documento por linha do corpo (§4.5.1) |
| `bulk_max_bytes` | `4MiB` | com `bulk`, teto do corpo descomprimido; 1 KiB..64 MiB, e abaixo de `convert.max_message_bytes` vale ele |
| `max_conns` | `0` | teto de conexões do transporte HTTP; bloqueia no accept; `0` = ilimitado |
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
| `keep_raw` | `GELF_CONVERT_KEEP_RAW` | `malformed` | `malformed`: `raw` só no malformado, com aviso de encoding ou com perda (§6.9); `always`: em todo documento |
| `max_message_bytes` | `GELF_CONVERT_MAX_MESSAGE_BYTES` | `128KiB` | teto do documento descomprimido; acima, recusa contada, nunca truncamento (§5.5); 1 KiB a 16 MiB; pior JLH = 6 × isto + 8 KiB |
| `accept_malformed` | `GELF_CONVERT_ACCEPT_MALFORMED` | `true` | `false` descarta `malformed` no worker e no dump |

#### `[filter]` — recarregável

Removido no go-gelf-pg-server (§1.2).

#### `[ratelimit]` — recarregável

Removido no go-gelf-pg-server (§1.2).

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `capacity_items` | `65536` | teto de itens, 1–16 777 216; com documentos de 200 bytes é o que pega primeiro |
| `max_bytes` | `512MiB` | teto de bytes: o `cap` de cada buffer cru + 288 B por item (§9.2); o JLH não conta. No mínimo o custo de um documento de `convert.max_message_bytes` (a classe do pool + 288 B, §13.2) |
| `high_water` | `0.80` | fração de qualquer teto acima da qual o `/readyz` falha |

Não há `mode` (sempre `memory`) nem `max_inflight` (era do `ring=none`).

#### `[ring.sqlite]`

Removido no go-gelf-pg-server (§1.2).

#### `[ring.redis]`

Removido no go-gelf-pg-server (§1.2).

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `workers` | `4` | goroutines de dreno, que também convertem; cada uma prende uma conexão; `WARN` acima de 16; 1–1024 |
| `batch` | `256` | itens por `Dequeue` e linhas por `INSERT`; 1–65 536 |
| `batch_max_bytes` | `16MiB` | teto de JLH por pedaço; no máximo 256 MiB, abaixo da mensagem de 1 GiB da libpq |
| `batch_max_delay` | `5ms` | espera máxima para completar um lote; `0` = sem espera |
| `op_timeout` | `5s` | prazo de uma escrita, da discagem e da validação do boot (§10.5) |
| `health_interval` | `2s` | período da checagem; também a latência de UP→DOWN |
| `health_timeout` | `1s` | prazo de uma checagem |
| `fail_timeout` | `30s` | DOWN antes de FATAL; `0` = sem fusível por tempo |
| `max_retries` | `10` | checagens falhas antes de FATAL |
| `boot_timeout` | `300s` | quanto o boot espera o PostgreSQL antes de sair com 3 |
| `retry_backoff_min`, `retry_backoff_max` | `100ms`, `2s` | backoff de um pedaço que falhou |

Não há `type` (sempre `postgres`).

#### `[sink.nats]`

Removido no go-gelf-pg-server (§1.2).

#### `[sink.redis]`

Removido no go-gelf-pg-server (§1.2).

#### `[sink.sqlite]`

Removido no go-gelf-pg-server (§1.2).

#### `[sink.postgres]` e `[sink.mysql]`

Só `[sink.postgres]`; `[sink.mysql]` foi removido (§1.2). Não há chaves de TLS
(`tls`, `tls_ca`, `tls_insecure`): a conexão é sempre `sslmode=disable` (§10.5).

| Chave | Padrão | Descrição |
|---|---|---|
| `addr` | `127.0.0.1:5432` | `host:port`, ou o diretório de um socket unix começando com `/` |
| `username` | `gelf` | o papel; precisa de `INSERT` na tabela e `SELECT` na coluna `uuid`, nada mais |
| `password` | `""` | redigida no `--dump-config` |
| `database` | `gelf` | |
| `table` | `loghub.inbox` | a fila (§3.7.1), `nome` ou `esquema.nome` em minúsculas; nunca criada, só validada |
| `pool_size` | `0` | teto do pool, ao menos `sink.workers + 1` (a sonda de saúde precisa de uma), no máximo 4096; `0` deriva `sink.workers + 1` |

#### `[sink.clickhouse]`

Removido no go-gelf-pg-server (§1.2).

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
| `admin.bind` | `127.0.0.1:9104` | a porta da família GELF, a mesma do `go-gelf-server`: não rode dois deles no mesmo host sem trocar (§14) |
| `log.level` | `info` | `debug` \| `info` \| `warn` \| `error` (recarregável) |
| `log.format` | `json` | `json` \| `text` |
| `log.access`, `log.access_sample` | `false`, `0.01` | log por mensagem, amostrado (recarregáveis) |
| `shutdown.pre_stop_delay` | `0s` | continua recebendo depois do `SIGTERM`; `0` = sem espera |
| `shutdown.drain_timeout` | `30s` | prazo do dreno do ring no shutdown |

### 16.6 Escrita em disco: uma raiz só

Tudo o que o serviço grava — a identidade (`agent_uuid`, `machine_id`, gravados
pelo `go-loghub-ident`) e `emergency/` — fica sob `server.data_dir`, e no
contêiner essa raiz é `/data` (o compose sobe com `read_only: true`). Não há
exceção: o socket unix do syslog não existe aqui.

Caminhos fixos do contêiner: `/app` guarda o binário (dono `root`), `/data` é a
única área gravável, `/etc/gelf` é o INI em `:ro` (o compose monta
`docker/conf/` ali e define `GELF_CONFIG=/etc/gelf/gelf.ini`, que traz as
chaves recarregáveis, §16.3), `/tmp` é tmpfs. `WORKDIR /data`; o binário é
invocado por `/app/go-gelf-pg-server`. Não há `/etc/gelf/tls`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

---

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-gelf-pg-server/              ESTE módulo (GP)
  go-gelf-server/                 o pai (G): dois eixos, TLS/HTTPS, filtro, rate-limit, beacon
  go-gelf-nats-server/            o irmão (GN): ring memory -> NATS
  go-gelf-injector/               cliente de carga e funcional (GI)
  go-syslog-pg-server/            o modelo do corte (SP), irmão syslog
  go-syslog-server/               o pai de G (S)
```

Módulo independente. Nada é importado de outro módulo do monorepo, nem do pai:
compartilhar pacotes faria os módulos evoluírem amarrados. Tags
`go-gelf-pg-server/vX.Y.Z`.

### 17.2 Layout interno

```
cmd/go-gelf-pg-server/main.go  flags que encerram, lhident.Initialize(), chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão, admin
  config/     registry único -> INI + flags + env + --help + dump; validação;
              LOGHUB_EB_URL; as chaves de G que não existem aqui (removed.go)
  ident/      Identity (congelada no boot) e a gramática de space          [lógica pura]
  jlh/        escritor JSON por append, Event, cabeçalho do envelope, UUID,
              Payload (o corte por posição)                                [lógica pura]
  convert/    GELF -> payload type=gelf -> JLH: parser JSON por varredura,
              números como texto, campos, avisos, sid    [CÓPIA LITERAL de G]
  item/       Item, Head, codec do cabeçalho (sem uso aqui), pools
                                                         [CÓPIA LITERAL de G]
  jlhcheck/   SÓ TESTE: schemas, exemplos e o oráculo    [CÓPIA LITERAL de G]
  receiver/   chunks.go, decompress.go, framing.go       [CÓPIA LITERAL de G]
              http.go                  [G menos NewHTTPS e o ramo TLS]
              udp (+ pktinfo por plataforma), stream (tcp), proxy, limites,
              ingest                   [G com os cortes de SP]
  ring/       interface Ring + memory
  sink/       interface Sink + postgres, workers, chunk, health, instrumented
  emergency/  dump JSON Lines, ParseLine e reingestão
  obs/        métricas Prometheus próprias + slog
```

**A regra da cópia** (DESIGN §1, memória do corte): os pacotes GELF são
**idênticos** em G, GP e GN, a menos do module path, e não se editam aqui; uma
correção neles vai para G e é copiada. O `http.go` é a única exceção: o de G
chama `BuildTLSConfig` e `sessionInfo` (de `tls.go`, que não existe aqui), então
o de GP é o de G menos `NewHTTPS`, o `tls.NewListener`, o `TLSNextProto`, o
`payload.tls` e os `Filtered`/`Consumed` de outros rings (DECISOES §2).

`convert/`, o leitor do PROXY em `receiver/` e `emergency.ParseLine` são os
parsers de entrada adversária. Os alvos de fuzz estão em §18.3.

### 17.3 Dependências

**Três em produção, e ponto.**

| Dependência | Para quê |
|---|---|
| `github.com/jackc/pgx/v5` | o sink, pela API nativa (`pgxpool`, `pgconn.PgError`) |
| `github.com/patrickbrandao/go-loghub-uuidv7` v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` v0.5.2 | identidade (sem dependência nova: usa a `go-loghub-uuidv7`) |

Só em teste: `github.com/santhosh-tekuri/jsonschema/v6` (contratos, §18.2).

As mesmas três de SP. O pai tinha oito; saíram `rueidis`,
`modernc.org/sqlite`, `go-sql-driver/mysql`, `nats.go` e `clickhouse-go/v2`, com
as transitivas de cada um. O binário caiu de 32,0 MB para 17,5 MB (22,0 MB para
12,0 MB com `-s -w`; build `darwin/arm64`, Go 1.27.2, as mesmas flags nos dois,
2026-10-31); a imagem, de 43,8 MB para 28,5 MB. `golang.org/x/net` **não** é
importado: PKTINFO é feito com a stdlib (`syscall`). O `golang.org/x/text` ainda aparece no binário, **transitivamente**,
pelo SASLprep (`secure/precis`) do `pgx`; nenhum pacote daqui o usa.

**Dependência nova exige justificativa nesta seção.** O objetivo do módulo é ter
poucas (§1.1).

**Gerador de UUID:** um `uuidv7.NewGenerator()` por processo, criado no boot e
compartilhado (é seguro para concorrência e usa ChaCha8 com semente de
`crypto/rand`). Um `init()` falha alto se `crypto/rand` não responder. O `uuid`
não é segredo (expõe o instante), e nada deriva capacidade dele.

**O parser JSON é próprio** (DECISOES §2): o `encoding/json` passa números por
`float64` (ou exige `UseNumber` e aloca um mapa por documento), troca UTF-8
inválido por U+FFFD em silêncio, aceita chaves duplicadas sem dizer e falha no
primeiro erro — e a regra aqui é nunca falhar (§6.1), sem perder o texto dos
números (§3.3). O `encoding/json` só aparece fora do caminho quente: na cópia
reconstruída da `loghub.inbox` (§3.7.1), com `json.Number`, e na leitura do
cabeçalho de uma linha de dump (§12.2).

**Sem CGO.** `CGO_ENABLED=0` é premissa do Dockerfile.

### 17.4 Convenções

- Código, identificadores e comentários em **inglês**; documentação e conversa
  em **português**. Não misture dentro de um arquivo. As únicas strings em
  português no código são os textos normativos de `parse.error` (§6.1), que são
  dado, não mensagem.
- Comentários explicam o **porquê** e o que daria errado com a alternativa
  rejeitada. Código copiado do pai ou da standard vem **com** os comentários.
- Citações da spec por número de seção (`§5.2`), com a numeração do pai
  (cabeçalho deste arquivo).
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
  linhas do dump, a fila real) passa nos dois schemas, com `santhosh-tekuri/jsonschema/v6` e
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
| `FuzzConvert` | `convert` | nunca entra em pânico; **toda saída passa nos dois schemas e nas regras semânticas**; o `LevelOf` (que em G serve o filtro, e aqui não é chamado na recepção) é o `level` do envelope; `size` é o documento; o pior JLH de §3.9 vale |
| `FuzzFrame` | `receiver` | a moldura `\0` com qualquer fluxo, qualquer partição das leituras e qualquer teto: sem pânico, nunca um `\0` num documento, nunca um documento acima do teto, e a sequência de documentos e recusas é exatamente a de um oráculo independente |
| `FuzzReassemble` | `receiver` | a remontagem de chunks com duas origens, alguns ids, datagramas sem chunk de uma terceira e passos de relógio com varreduras: sem pânico, nunca passa dos tetos, toda mensagem completa é a concatenação dos chunks na ordem de `seq`, e uma varredura no futuro devolve todo byte |
| `FuzzDecompress` | `receiver` | qualquer byte como qualquer contêiner: sem pânico, nunca mais que `L` bytes de saída, só os dois erros (`too_large`, `decompress`), e o que a stdlib comprimiu volta igual |
| `FuzzProxy` | `receiver` | v1 e v2: sem pânico, nunca consome além do cabeçalho declarado; cabeçalho válido consome exatamente os bytes dele, nenhum não consome nada |
| `FuzzHead` | `item` | o decodificador do cabeçalho (com `compression` e `chunks`) recusa sem pânico; ida e volta é identidade. O codec não é usado aqui (§3.4), mas o pacote é o de G e o alvo vai com ele |
| `FuzzRead` | `emergency` | o leitor do dump tolera truncamento e lixo sem pânico, nunca guarda uma linha acima do teto, e tudo o que `ParseLine` aceita a fila aceitaria |
| `FuzzJSONBSafe` | `sink` | a troca do `\u0000` para a `loghub.inbox` (§3.7.1) devolve JSON válido igual ao original com U+0000 → U+FFFD, e nada mais |
| `FuzzInboxPayload` | `sink` | o payload da coluna é JSON sem escape NUL, `raw` volta exato e os demais textos (chaves e valores de `fields`/`extra` inclusive) só trocam U+0000 por U+FFFD (com a regra do número de §3.7.1 a provar à parte, `TestInboxPayloadCarriesNumbersJSONBCannotHoldAsText`) |

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
(2026-10-31, em G): mapas aninhados chegam **achatados**
(`_kubernetes_pod_name`) e arrays como texto (`"1, a"`); o vetor anterior, com
`_kubernetes` como objeto, não era o que o fluent-bit manda e ficou como
`nested-field-object.gelf` (prova o `field-coerced`). O corpus nunca encolhe.
Os vetores dourados são comparados **byte a byte** com o `.want.json`
compactado (`json.Compact` preserva a ordem): a ordem fixa do payload (§3.2) e
a ordem de chegada de `fields`/`extra` (§3.3, §6.6) fazem parte do contrato.

### 18.5 Integração em processo

A pilha **real** (a mesma `run()` do binário), contra um PostgreSQL real: como
o único destino é a fila, todo teste que passa do passo 7 do boot precisa de
`GELF_TEST_PG_DSN` (§18.6) e pula sem ela; só as recusas de antes do passo 7
(exit 1) rodam sem banco. Cada teste cria a própria fila a partir de
`docker/postgres/inbox.sql`, num esquema próprio. Falhas do destino são
injetadas decorando o sink real (`App.wrapSink`). Cobre: os três transportes
chegam ao destino (UDP com `dst-ip` por PKTINFO, inteiro e em chunks gzip; TCP
com `\0`; HTTP com `202`, e `503` com ring cheio ou sink DOWN), PROXY v1 e v2,
fidelidade do JLH (o `raw` de um malformado volta byte a byte, com NUL no meio
em base64; números exatos; chaves com NUL),
`SIGHUP` de `[log]` (ida e volta, chave sombreada, arquivo inválido), dump de
emergência e reingestão exatos (mesmo `uuid`, mesmo JLH, mesmo depois de trocar
uma chave `[convert]`; o documento com NUL chega por HTTP), dump danificado em
`.corrupt`, `too_large` sem perder a moldura, shutdown gracioso sem
perda, segundo sinal (130), nenhum transporte (exit 1), chaves do pai na linha
de comando (exit 1) e no env (`WARN`), códigos 100–114 do `go-loghub-ident` num
subprocesso, e a ordem do boot.

### 18.6 Destino real

A suíte do PostgreSQL pula sem a variável e cria a fila **a partir do arquivo que
o operador aplica** (`docker/postgres/inbox.sql`), num esquema próprio por teste:

```bash
source ../tasks/infra/env.sh   # GELF_TEST_PG_DSN, PostgreSQL 18 em 127.0.0.1:25432
go test -race -timeout 900s ./internal/sink/ ./internal/app/
```

O DSN de teste também é sem TLS (`sslmode=disable`). O que ela prova está em
`docs/TESTING.md` §6.

### 18.7 Emissores vivos

A única prova de que chunks, compressão e moldura funcionam no fio é um emissor
GELF de verdade, ponta a ponta até a `loghub.inbox` e pelo oráculo.
`GELF_TEST_EMITTERS=1` (com `GELF_TEST_PG_DSN`) roda
`internal/app/emitters_test.go`:

- **fluent-bit** (`fluent/fluent-bit:4.0.3`, saída `gelf`): `Mode udp` com
  `Compress On` e `Packet_Size 1420`, com um registro de ~12 KiB aleatório que,
  comprimido, ainda passa de um chunk (UDP + gzip + chunks), e `Mode tcp`
  (moldura `\0`). É também o emissor do `docker/compose.demo.yml`.
- **driver de log `gelf` do Docker** (`docker run --log-driver=gelf`, imagem
  `alpine:3.22`), UDP com `gelf-compression-type` `gzip`, `zlib` e `none`, e
  TCP (`gelf-address=tcp://`). O daemon resolve o endereço
  (`host.docker.internal` no Docker Desktop).

Cada documento chega uma vez só, inteiro, pelo transporte em que foi mandado, e
cada linha da fila passa no oráculo de §18.2. A saída `http` do fluent-bit
(vários documentos por requisição) depende de `http.bulk` (§4.5.1) e não está
entre os emissores vivos (TESTING §8). O resultado de cada rodada está em
`docs/TESTING.md` §6.

### 18.8 Critérios de aceite v1

**Nunca medidos.** Alvo, não fato; a primeira medição substitui esta tabela.

| Cenário | Alvo |
|---|---|
| UDP sem chunk, documentos de 400 B | ≥ 100k msg/s por núcleo de recepção |
| UDP gzip em 8 chunks | ≥ 10k msg/s |
| TCP `\0` | ≥ 100k msg/s |
| HTTP, um documento por conexão | ≥ 5k req/s |
| Perda com `SIGTERM` durante carga, TCP e HTTP (`202`) | **zero**, reconciliada contra a fila |
| Perda com FATAL do sink | **zero**, via dump e reingestão |

---

## 19. Matriz de garantias

Leia junto com §2.3: a garantia só existe nos transportes que confirmam (TCP e
HTTP).

| Combinação | A confirmação significa | Perda em `kill -9` | Perda em `SIGTERM` | Perda em FATAL do sink |
|---|---|---|---|---|
| `memory` → `postgres` | aceito na RAM deste nó | o backlog do ring | nada, se o dreno couber em `drain_timeout`; o resto vai para o dump | nada: o dump (§12) |

Depois do `INSERT` confirmado, a durabilidade é a do PostgreSQL
(`synchronous_commit`, e nunca uma fila `UNLOGGED`, §10.5). Um dump que falha
(§12.3) é perda, contada em `reason="emergency_failed"`.

| Transporte | A confirmação existe? | O que a substitui |
|---|---|---|
| TCP | em parte: a retomada da leitura | nada se perde, mas o emissor não sabe *qual* documento foi aceito |
| HTTP | sim, por documento: `202` | o `503` com `Retry-After` manda tentar de novo |
| UDP | não | `gelf_messages_dropped_total` e os contadores da interface |

Quem precisa de `kill -9` sem perda usa o `ring=sqlite` do `go-gelf-server`.

---

## 20. Invariantes

Se uma alteração conflita com um destes, ela está errada. A numeração é a do pai;
os que perderam o objeto ficam com a nota.

1. A confirmação nunca sai antes de o item estar no ring; onde não há
   confirmação, há contador.
2. Ring cheio nunca vira aceitação seguida de descarte. UDP descarta antes de
   aceitar; TCP segura o emissor; HTTP responde `503`.
3. Um buffer volta ao pool **uma vez**, e só após a confirmação do sink. Num
   retry o item ainda é o dono; uma tentativa por prazo contamina o pedaço.
4. Item aceito só sai do ring após a confirmação do sink (ou um descarte
   deliberado e contado: `logic`, malformado recusado); nunca para liberar
   espaço.
5. **Todo documento aceito vira exatamente um JLH válido** pelos dois schemas, ou
   um descarte contado. A conversão nunca falha; o pior resultado é `malformed`.
6. **Sem perda e sem invenção:** o documento recebido (já sem moldura, já
   descomprimido) está em `raw` quando ele é obrigatório ou ligado; campo ausente é omitido, nunca
   adivinhado nem `null`.
7. Nenhum sink altera o JLH. A coluna `payload` é o sub-slice do JLH ou, com NUL
   ou um número que o `jsonb` não guarda, uma cópia reconstruída (§3.7.1); o JLH do pedaço e o do dump são íntegros.
8. Os endereços do payload são a ponta real do transporte, ou o cliente
   declarado por um PROXY **confiável**, e então o par real fica em `x-proxy`. O
   `hostname` é sempre alegação do emissor e nunca vira chave de decisão.
9. Nenhum label de métrica deriva de conteúdo controlado pelo emissor.
10. Nenhum log síncrono por documento no caminho padrão; nenhum payload em log de
    `INFO` ou acima.
11. Removido no go-gelf-pg-server (era do rueidis, §1.2).
12. O JLH é idêntico no worker e no dump: a mesma conversão, com a mesma config
    congelada.
13. **O JLH é função de cabeçalho + cru + config de conversão congelada** (§3.5):
    mesmo `uuid`, mesmos bytes, em retry e dump. A reingestão não converte.
14. **Uma amostra de relógio por documento**, e dela saem o `uuid` e o `event`.
15. `ring` e `sink` são ortogonais: nenhum consulta o outro.
16. Toda escrita em disco fica sob `server.data_dir` (`/data` no contêiner),
    inclusive a identidade, **por padrão**. Um `emergency.dir` absoluto fora
    dele é decisão do operador
    (um disco mais rápido), aceita com `WARN` no boot (§13.2).
17. O servidor nunca cria o que valida: o sink nunca faz DDL na fila.
18. Esquema e permissão são `fatal`, nunca `logic` (classe `42` do SQLSTATE,
    `42501`, `28`); erro desconhecido é `network`, nunca `logic`.
19. Removido no go-gelf-pg-server (era do `nats.go`, §1.2). O princípio fica:
    nenhuma biblioteca cliente guarda nem retenta o que o ring guarda e o worker
    retenta.
20. Nenhuma linha de dump que a fila poderia recusar chega ao `INSERT`:
    `emergency.ParseLine` a recusa antes (§12.2). (No pai: nenhum destino recebia
    uma mensagem que o teto dele recusaria.)
21. O servidor nunca emite `beacon`, `ping`, `pong` nem sonar.
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
| GELF por AMQP/Kafka | Fora da v1 (§1.2) |
| `space` por listener ou rede | Multi-tenant numa instância; fora da v1 |
| TLS (TCP+TLS, HTTPS, cliente) | Fora pelo corte (§1.2); voltar exige decisão nova, não só código |
| Particionamento por tempo na fila | Precisa de decisão sobre quem cria a partição |
| Ferramenta de reingestão manual de um `.jsonl` (ou de um `.corrupt` reparado) | Hoje basta copiar o arquivo para `emergency.dir` com o nome `emg-*.jsonl` e reiniciar |

---

## 22. Lacunas e riscos

O que ainda não está provado fica em `docs/TESTING.md` §8, e os riscos aceitos
por decisão, em `docs/DECISOES.md` §4. Esta seção guarda os **defeitos** do
código atual.

### 22.1 Pendências conhecidas

Problemas reais, conferidos contra o código atual e ainda não corrigidos. Cada
item diz o problema, a consequência e a direção da correção. Corrigido um, ele
sai daqui e a regra vai para a seção que ele cita, spec primeiro. As pendências
fechadas de SP (2026-10-14) e de G (2026-10-31) que valem aqui já estão no
código copiado e nas seções citadas; as abertas de G que valem aqui seguem.

Abertas em 2026-10-31:

1. **A remontagem UDP é um mapa sob um mutex só** (G, item 1), compartilhado
   pelos `listen.udp_listeners` (§5.2). Com vários listeners e chunks em massa
   ele é o ponto de contenção; nada foi medido. Direção: shards por hash de
   `(src-ip, src-port, id)`, em G primeiro (o pacote é cópia literal).
2. *(fechada em 2026-10-31: `http.bulk`, §4.5.1, porte de G.)*
3. **A cópia da `loghub.inbox` pode levar `raw` em base64 de um UTF-8 válido**
   (G, item 6; §3.7.1): exceção, só na cópia da fila, à regra do oráculo. Um
   consumidor da fila precisa decodificar o `raw` pelo `raw-encoding`, sempre.
4. *(fechada em 2026-10-31: G recebeu `jsonbHolds`, e a correção da mantissa
   zero voltou de G para cá, §3.7.1.)*
5. **O `internal/item` copiado de G carrega código morto** (§3.4, §9.2): o
   codec do cabeçalho, o canal `done`, `RowID`, `StreamID` e `TLS`. Custa 32 B
   de `ItemOverhead` a mais que SP (288 contra 256; o GN também é 288 desde
   2026-10-31) e a alocação da caixa
   `*[]byte` que SP economiza (§15.1). É decisão (DESIGN §1: os pacotes GELF
   são idênticos nos três), não defeito; registrado para que uma futura
   divergência seja escolha, não deriva.

O que os testes ainda não provam está em `docs/TESTING.md` §8.
