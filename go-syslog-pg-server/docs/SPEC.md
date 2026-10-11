# go-syslog-pg-server — Especificação v1 (LogHub)

Status: **congelada para implementação**
Data: 2026-10-07
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server`
Binário: `go-syslog-pg-server`
Entrada: `docs/DECISOES.md` (entrevista de 2026-10-07), que aponta para as
decisões herdadas do `go-syslog-server` (`../go-syslog-server/docs/DECISOES.md`).

**Origem.** Esta especificação foi derivada em 2026-10-07, por corte, da SPEC do
`go-syslog-server` (revisão de 2026-10-07). O pedido do usuário: um servidor de
syslog rápido e com dependências mínimas, com **sink só no PostgreSQL**, **ring
só na memória**, emergência em arquivo **JSON Lines**, **sem TLS** e sem os
demais recursos do pai (§1.2).

**A numeração das seções é a do `go-syslog-server`, de propósito.** O código
cita a spec por número (`§10.5`, `§12.2`, `§20.18`), e os dois servidores são da
mesma família: um número que mudasse de sentido entre eles enganaria quem lê os
dois. Uma seção removida **mantém o título**, seguido de uma nota de uma linha;
nada é renumerado.

Servidor de ingestão de syslog do ecossistema **LogHub**. Recebe mensagens por
UDP, TCP e socket unix, converte cada uma num envelope **JLH** com
`type=syslog` e o **enfileira** na `loghub.inbox` do PostgreSQL, a fila comum de
envelopes do LogHub. É um **Bridge-IN**: cria a primeira mensagem de cada fato
(`step = 0`).

---

## 1. Escopo

### 1.1 Objetivos

- Máximo de mensagens por segundo com mínimo de CPU e memória.
- **Dependências mínimas:** três em produção (§17.3), binário pequeno.
- Cada mensagem aceita vira **exatamente um** envelope JLH válido pelos schemas
  das skills `loghub-json-schema` e `loghub-type-syslog-schema` (§3).
- **Nunca confirmar** uma mensagem antes de ela estar no ring, nos transportes
  que têm como confirmar (§2.3).
- Backpressure explícito onde o transporte permite; perda **contada e visível**
  onde ele não permite.
- **Sem perda e sem invenção** no payload: o que chegou está nos campos ou em
  `raw`; o que não chegou é omitido, nunca adivinhado (§6).
- Um destino só: a fila `loghub.inbox` de um PostgreSQL compartilhado entre
  instâncias e produtores (§3.7.1, §10.5).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| **TLS no listener** (RFC 5425) e **mTLS** | Decisão do usuário (2026-10-07, decisão 4): o TLS sai **em todo lugar**, não só do listener (a recomendação era tirar só o listener). Quem precisa de syslog cifrado usa o `go-syslog-server` ou um terminador TLS na frente (com PROXY protocol, §4.6). O payload nunca traz `transport=tls` nem o objeto `tls` |
| **TLS até o PostgreSQL** | A mesma decisão. A conexão é sempre `sslmode=disable`: o `TLSConfig` do pgx é nulo, os *fallbacks* saem e o `PGSSLMODE` é ignorado (§10.5). Uma `LOGHUB_EB_URL` com `sslmode=require`, `verify-ca` ou `verify-full` é recusada com exit 1 (§10.9). O banco é alcançado por rede confiável ou por socket unix |
| Outros sinks: `nats`, `redis`, `sqlite`, `mysql`, `clickhouse`, `none` | Pedido do usuário: sink **só** no PostgreSQL. Cada um trazia um cliente (NATS, rueidis, SQLite, MySQL, ClickHouse) e a sua árvore de transitivas. Quem precisa deles usa o `go-syslog-server`, que é o mesmo servidor com os dois eixos abertos |
| Outros rings: `sqlite`, `redis`, `none` | Pedido do usuário: ring **só** na memória. A perda sob `kill -9` é a do ring em memória (§19); FATAL e `SIGTERM` são cobertos pelo dump (§12) |
| Filtro por facility/severidade | Decisão 1 da entrevista: sai. O descarte, se houver, é do consumidor da fila; a recepção nem lê o PRI (§2.2) |
| Rate-limit por prefixo de rede | Decisão 1: sai. Proteção contra emissor ruidoso fica na rede (firewall, QoS) |
| Heartbeat beacon | Padrão assumido: dependia do `sink=nats`, que saiu. Sonar, ping e pong também ficam de fora |
| `sink.type` e `ring.mode` como chaves | Padrão assumido: um valor só cada um. O log de boot e o `syslog_config_info` continuam dizendo `ring_mode="memory"` e `sink_type="postgres"`, para que um painel do pai sirva aqui (§14.1) |
| RELP | Vira software próprio (`go-relplog-server/`); o enum de `transport` do `type=syslog` não tem `relp` |
| DTLS (RFC 6012), BEEP (RFC 3195) | Como no pai: sem implementação na stdlib, sem emissores vivos |
| Importação de arquivo (`transport=file`) | Fora da v1 (D5) |
| `space` por listener ou rede | Um Workspace por instância na v1 |
| Validação por JSON Schema em execução | Só em teste e CI (§18.2; D3) |
| Autenticação de emissor | Não existe no protocolo sem TLS: firewall ou rede privada |
| Roteamento por facility, deduplicação de conteúdo, syslog **de saída** | Responsabilidade de outro software |

O porquê comum: **o pedido do usuário** (um servidor rápido e com dependências
mínimas), **as dependências** (de oito em produção para três; o binário caiu de
31,8 MB para 17,2 MB, mesmo build `darwin/arm64` com as mesmas flags nos dois)
e **a velocidade** (a recepção não lê a mensagem: nem PRI, nem filtro, nem balde
de rate-limit; §2.2).

### 1.3 Fontes normativas e desvios

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, `jlh-envelope.schema.json` |
| skill `loghub-type-syslog-schema` | consultada em 2026-10-05 | Payload `type=syslog`, `jlh-syslog-payload.schema.json`, algoritmo de conversão, 24 vetores dourados |
| skill `loghub-postgres-table-inbox` | consultada em 2026-10-06 | DDL da `loghub.inbox`, mapeamento envelope → colunas, enfileirar com `ON CONFLICT (uuid) DO NOTHING` (§3.7.1) |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 | UUIDv7 nível 3 |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 | AgentUUID, Workspace, AgentName, Hostname, MachineID, `DATADIR` |

Cópias dos dois schemas, dos exemplos de envelope e dos vetores ficam em
`internal/jlhcheck/loghub/` (§18.2). Onde as skills divergem, vale a mais
restritiva.

**Desvios deliberados das skills**, os mesmos do pai, cada um com o motivo na
seção citada:

| # | Desvio | Skill diz | Aqui | Seção |
|---|---|---|---|---|
| D1 | Moldura TCP | detectar **por frame** | **por conexão**, com confirmação `[1-9][0-9]{0,9} SP` | §5.2 |
| D2 | TAG sem timestamp | sem timestamp, nenhuma TAG | TAG **estrita** ainda extrai `appname`/`procid` | §6.5.5 |
| D3 | Validação | todo envelope passa no schema antes da fila | schema só em teste e CI; o serializador é provado por fuzz | §18.2 |
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
- **SINK**: `postgres`, a fila `loghub.inbox` (§10.5).

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
 |   -> sink DOWN? -> ring cheio? -> uuid -> Item(cabeçalho + cru) |
 +-------------------------------+--------------------------------+
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
"ring cheio" é uma pré-checagem barata e sem trava (`ring.Memory.Full`: itens ≥
`capacity_items`), feita **antes** do `uuid`, da cópia e do item: uma mensagem
recusada com o ring cheio (uma enxurrada de UDP, a retentativa do TCP a cada
50 ms) custa ~25 ns e nenhuma alocação, em vez de ~115 ns e duas. Ela pode errar
nos dois sentidos numa corrida; **o `Enqueue` continua sendo a autoridade** (os
dois tetos, §9.2).

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
| Destino | a fila `loghub.inbox` (§3.7.1); nenhum subject de eventbus |
| Plano de controle | nenhum: o beacon saiu (§11); nunca emite `ping`, `pong` ou sonar |
| Consumo | nenhum: quem retira da fila é outro software |

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
| `uuid` | UUIDv7 **nível 3** (`go-loghub-uuidv7`), em minúsculas, gerado **na recepção** com `GenerateAt(Level3, t)`, onde `t` é a amostra de relógio da mensagem. É cunhado depois de conferidos o estado do sink e a pré-checagem do ring cheio (§2.2; uma mensagem recusada por isso não gasta `uuid`), mas sempre da amostra `t` do fim do frame. Gravado no cabeçalho do item e nunca regerado: a retentativa do laço de backpressure do TCP, o retry do worker e o dump reusam o mesmo; se uma tentativa de destino incerto (prazo estourado) tiver sido aceita, as duas cópias deduplicam por `uuid` na fila (§3.7.1). O nível 3 é fixo; `LOGHUB_UUID_LEVEL` não existe (D4). |
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

Removido no go-syslog-pg-server (§1.2): não há `sink=redis`.

### 3.7 Destino: a fila `loghub.inbox`

O único destino é a fila comum do LogHub (§3.7.1). A tabela `syslog_events` do
pai (`sink=sqlite` e `sink=mysql`, com colunas promovidas e o JLH em `env`) foi
removida com esses sinks (§1.2). Da regra dela fica uma só, aplicada à fila: o
NUL dos textos vira U+FFFD na coluna (§3.7.1).

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
| `level` | `level` | a severidade (§3.1) |
| `event_nsec` | `event.sub` | `sub % 1000`, o resto que o microssegundo não guarda |
| `space` | `space` | — |
| `type` | `type` | `syslog` |
| `payload` | `payload` | `jsonb`; com NUL, `raw` em base64 e U+FFFD nos demais textos (abaixo) |

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
  descartaria o pedaço inteiro (§10.2); um `message` ou um `raw` com NUL produz
  esse escape (§3.3). Só nesse caso raro a coluna recebe uma **cópia
  reconstruída** do payload: `raw` vai em base64 com `raw-encoding=base64` (a
  forma que o schema do payload já tem para texto que não viaja como está), então
  continua exato e `size` continua contando os octetos dele; em todos os outros
  textos (`message`, `hostname`, valores de SD...) o NUL vira U+FFFD. A
  reconstrução passa por um mapa, o que não perde nada que o `jsonb` guardaria
  (ele não guarda ordem de chaves). Sem NUL — quase sempre — a coluna recebe o
  sub-slice do JLH, sem cópia. O JLH do pedaço e o do dump continuam íntegros
  (§20.7). O envelope reconstruído da linha passa nos dois schemas e nas regras
  semânticas da skill de syslog; perde só o NUL dos textos decodificados.
- **As CHECKs da tabela são contrato, não opção.** Todo envelope `type=syslog`
  daqui tem `level` ≤ 7, `payload.message` (§3.2) e `from` UUIDv7
  (`go-loghub-ident`), e nenhum é `beacon`: nenhuma CHECK recusa o que este
  servidor produz. Uma recusa (`23514`) seria `logic`, descartaria o pedaço, e é
  bug. O operador **não** remove CHECKs (regra 6 da skill). A leitura do dump
  (§12.2) recusa antes do `INSERT` toda linha que uma CHECK recusaria.
- **Só o índice da PK** (regra 7 da skill). Consultas por host, severidade ou
  aplicação vão para o armazenamento definitivo, não para a fila.
- O consumo (`DELETE … USING job … RETURNING`, quarentena, monitoramento do
  atraso) é de outro software e está na skill; nada dele roda aqui.

O que o servidor **exige** da tabela, verificado no boot (§10.5): as **dez**
colunas que ele escreve, com tipos compatíveis; um índice **único** exatamente
sobre `(uuid)`; `INSERT` e `SELECT (uuid)` para o papel (o `ON CONFLICT (uuid)`
lê a coluna árbitra: só com `INSERT` o primeiro pedaço falha com `42501`).
`received_at` e as CHECKs não são conferidas: o servidor não depende delas.

#### 3.7.2 Tabela `inbox_syslog` (`sink=clickhouse`)

Removido no go-syslog-pg-server (§1.2): não há `sink=clickhouse`.

### 3.8 Mensagem NATS (`sink=nats`)

Removido no go-syslog-pg-server (§1.2): não há `sink=nats`.

### 3.9 O maior evento, e por que ele é calculado e não configurado

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
- **8 KiB fixos**: chaves, números, `host`, `x-proxy`, `parse` e o cabeçalho do
  envelope.

Com o padrão de 64 KiB, o pior JLH é **776 KiB**. `TestWorstCaseJLHIsBounded`
prova o teto com linhas adversárias.

No pai o número existia para recusar no boot um destino com teto por mensagem
menor que ele (`max_payload` do NATS, `max_allowed_packet` do MySQL). O
PostgreSQL não tem um teto desses para conferir, então aqui ele serve a três
coisas: o pedaço de §10.1 (um JLH maior que `sink.batch_max_bytes` forma um
pedaço sozinho), o limite de linha do leitor do dump (`emergency.MaxLine`,
§12.2, calculado com o **maior** `max_message_bytes` que a config aceita:
12 × 16 MiB + 8 KiB) e o `max_event_bytes` do log de boot.

O número é pessimista de propósito: um fator "realista" deixaria um único
emissor quebrado produzir um envelope maior que o teto que todo o resto assume.

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
`:1514` (UDP e TCP). O `docker/docker-compose.yml` publica a porta `5180` do
host (`SYSLOG_PORT`) na 1514 do contêiner, fora das portas que os compose do
`go-syslog-server` usam (514 e 5141–5162), para que os dois convivam no mesmo
host. Quem
roda no host e quer 514 usa
`setcap cap_net_bind_service=+ep /app/go-syslog-pg-server` ou
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
  **fecha na hora**. O IP é o do par TCP, **antes** do PROXY protocol: com um
  balanceador na frente, o teto por IP vê o balanceador (§4.6). A chave é o
  endereço exato: em IPv6, um /128 (§22.1).
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

Removido no go-syslog-pg-server (§1.2, decisão 4): não há listener TLS nem mTLS.

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
  conta nada. `LOCAL`, `UNSPEC`, `UNIX`, `DGRAM` e `UNKNOWN` contam como
  `local`.
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
- **Charset legado** (decisão 2 da entrevista: fica): `syslog.legacy_charset = cp1252 | latin1`,
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

Removido no go-syslog-pg-server (§1.2, decisão 1): não há filtro por facility nem por severidade.

---

## 8. Rate-limit

Removido no go-syslog-pg-server (§1.2, decisão 1): não há rate-limit por prefixo de rede.

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
do primeiro item: lotes maiores são menos idas ao banco e amortizam a
conversão, que roda por item no worker.

### 9.4 `ring.mode = sqlite`

Removido no go-syslog-pg-server (§1.2): o ring é só memória.

### 9.5 `ring.mode = redis`

Removido no go-syslog-pg-server (§1.2): o ring é só memória.

### 9.6 `ring.mode = none`

Removido no go-syslog-pg-server (§1.2): o ring é só memória, e a conversão nunca roda na recepção.

### 9.7 Combinações que degradam ou recusam

Removido no go-syslog-pg-server (§1.2): há uma combinação só, `memory` → `postgres`.

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
  retry a reenvia. A deduplicação é da fila: `ON CONFLICT (uuid) DO NOTHING`
  (§3.7.1), e a segunda cópia conta em `syslog_sink_postgres_duplicates_total`.
- **Contaminação** (§15.2): uma tentativa que termina por prazo ou cancelamento
  marca o pedaço, e nem o cru nem o buffer do pedaço voltam ao pool, nem quando a
  retentativa seguinte tem sucesso.

| Classe | Efeito |
|---|---|
| `ClassNetwork` | retenta; não muda o estado de saúde (§13.3) |
| `ClassLogic` | `ERROR` com os `uuid`, pedaço descartado e contado (`syslog_events_dropped_total{reason="sink_logic"}`), confirmado no ring |
| `ClassFatal` | FATAL imediato |

**Entrega parcial.** Um erro `logic` pode trazer os índices do que o destino
**já gravou** do pedaço (`Error.Delivered`); esses contam em
`syslog_events_written_total`, só os demais em `sink_logic`. No PostgreSQL o
pedaço é **um** comando: na falha nada foi gravado, e o pedaço inteiro conta
como perdido. O mecanismo fica porque é o contrato da interface, que os testes
dos workers exercitam.

### 10.3 `sink.type = redis`

Removido no go-syslog-pg-server (§1.2). A regra que estava aqui e que o código ainda cita — uma resposta **fatal** à sonda de saúde é FATAL na primeira checagem, sem passar pelos fusíveis — vale para o PostgreSQL e está em §13.3.

### 10.4 `sink.type = sqlite`

Removido no go-syslog-pg-server (§1.2).

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
  `client_encoding = UTF8` e `application_name = slpg/<AgentName>@<Hostname>`,
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
  `syslog_sink_postgres_duplicates_total`.
- **Conexões:** `sink.workers` tem padrão **4** (no pai era 8, rebaixado para 4
  com `WARN` num destino SQL; aqui o padrão já é o valor certo). Acima de 16,
  `WARN`: cada worker prende uma conexão de um orçamento (100 por padrão no
  PostgreSQL) que a frota inteira divide. `pool_size = 0` deriva `workers + 1`;
  um `pool_size` abaixo de `workers` é exit 1.
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
  toda escrita virava `23502`, `logic`: lotes descartados para sempre com o sink
  UP (§20.18). `received_at` passa porque tem `DEFAULT now()`. Pelo catálogo, não
  por sonda: uma sonda dispararia triggers do operador e consumiria sequências,
  e a recusa dela não nomeia a coluna.
- Em tempo de execução, um `23514` que não nomeia constraint (a partição que
  sumiu) é `fatal`, não `logic`; um `from` que não é UUIDv7 é recusado antes do
  `INSERT`, como `logic` (não acontece com o que este processo produz nem com o
  que `ParseLine` aceita, §12.2).
- Durabilidade: a do servidor (`synchronous_commit`). A skill admite a fila
  `UNLOGGED`, que o servidor não confere: ela é apagada numa queda do PostgreSQL
  e só serve se a origem puder reenviar, o que o syslog não faz.

| SQLSTATE | Classe |
|---|---|
| `08`, `53`, `57`, `40` | `network` |
| `22`, `23` (fora do `ON CONFLICT`) | `logic` |
| **`42`** (esquema), **`3D`** (o banco não existe), `28`, `2F`, `42501`, `25006`, `23514` sem constraint nomeada | **`fatal`** |
| qualquer outra | `network` |

**A classe `42` é fatal e nunca `logic`**: uma tabela dropada classificada como
`logic` descartaria um lote a cada 5 ms, para sempre, num processo que parece
saudável. **A `3D` também é fatal** (revisão independente de 2026-10-07): um
banco que não existe não passa a existir esperando, e como `network` o boot
gastava o `boot_timeout` inteiro antes de sair. Esta tabela e
`TestClassifySQLState` mudam juntas.

### 10.6 `sink.type = mysql`

Removido no go-syslog-pg-server (§1.2).

### 10.7 `sink.type = nats` (padrão)

Removido no go-syslog-pg-server (§1.2).

### 10.8 `sink.type = none`

Removido no go-syslog-pg-server (§1.2): todo pedaço vai ao PostgreSQL; não há destino de benchmark.

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

**`sink.postgres.addr`, venha da URL ou não, é um servidor só:** `host:porta`
com host não vazio, sem vírgula e porta 1–65535 sem zero à esquerda; ou um
diretório de socket começando com `/`, com um `:porta` opcional na mesma faixa. Nenhuma
mensagem de erro cita a URL (ela pode ter senha). O `--dump-config` mostra os
valores derivados como comentário, com a senha redigida, para que o dump relido
não dispare o conflito.

### 10.10 `sink.type = clickhouse`

Removido no go-syslog-pg-server (§1.2).

#### 10.10.1 Cliente

Removido no go-syslog-pg-server (§10.10).

#### 10.10.2 Concorrência e conexões

Removido no go-syslog-pg-server (§10.10).

#### 10.10.3 Escrita

Removido no go-syslog-pg-server (§10.10).

#### 10.10.4 Desduplicação e at-least-once

Removido no go-syslog-pg-server (§10.10).

#### 10.10.5 Validação no boot

Removido no go-syslog-pg-server (§10.10).

#### 10.10.6 Durabilidade e `async_insert`: decisão do administrador do ClickHouse

Removido no go-syslog-pg-server (§10.10).

#### 10.10.7 Classificação de erro

Removido no go-syslog-pg-server (§10.10).

#### 10.10.8 `Health()`

Removido no go-syslog-pg-server (§10.10).

#### 10.10.9 Retenção

Removido no go-syslog-pg-server (§10.10).

#### 10.10.10 Métricas

Removido no go-syslog-pg-server (§10.10).

#### 10.10.11 Autenticação e TLS

Removido no go-syslog-pg-server (§10.10).

---

## 11. Plano de controle: heartbeat beacon

Removido no go-syslog-pg-server (§1.2): o beacon só existia com `sink=nats`. O servidor não emite `beacon`, `ping`, `pong` nem sonar.

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

**JSON Lines: um envelope JLH pronto por linha** (decisão 3 da entrevista, a
opção recomendada):

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
`sink.boot_timeout` cobre a reingestão inteira, não um por arquivo.

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
| arquivo com cauda truncada ou linha inválida | **danificado**: as linhas íntegras são entregues e o arquivo é renomeado para `.corrupt` (com `fsync` do diretório), com as contagens de bytes em `ERROR`; destruir a evidência de uma queda não é decisão deste programa |
| arquivo limpo | removido (com `fsync` do diretório) **só depois** de todos os seus envelopes serem confirmados. Uma falha da remoção só é logada: o arquivo volta no boot seguinte, e a fila só deduplica enquanto o `uuid` ainda estiver nela (TESTING §8) |

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

### 12.3 Falha do próprio dump

Disco cheio, sem permissão, `fsync` recusado, `emergency.enabled = false`:
`ERROR` com a contagem exata, `syslog_events_dropped_total{reason="emergency_failed"} += n`,
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
    recuperação (§10.5): recusa => exit 3 IMEDIATO, com o log "the sink
    refused this server permanently" (e não "within sink.boot_timeout")
 8. RING memory, workers (ainda parados) e o pipeline de recepção montados
 9. antes, o temporário gêmeo de um dump é removido e o de um dump
    interrompido vira um WARN com o caminho, intocado (§12.1);
    reingere emg-*.jsonl (§12.2), sem conversão, EM FLUXO (nunca o
    arquivo inteiro na memória) e PEDAÇO A PEDAÇO, com um único
    sink.boot_timeout para tudo (sink.Instrumented.Deliver): `logic` descarta, conta e loga aquele
    pedaço e segue para o próximo; `network` reenvia o MESMO pedaço com
    backoff dentro de sink.boot_timeout; `fatal` sai com 3; um sinal sai
    com 130
10. inicia a saúde e os workers; abre os transportes: unix, udp, tcp
11. /readyz 200
```

**Identidade antes de tudo que escreve.** `lhident.Initialize()` roda **uma
vez**, no `main`, depois das flags que encerram o programa e antes de abrir
qualquer recurso com `defer` ou criar goroutine (regra 2 da skill). A app recebe
um struct `Identity` (`From`, `Space`, `AgentName`, `Hostname`, `MachineID`)
preenchido dos getters: os testes montam a app dezenas de vezes no mesmo
processo, e uma segunda chamada a `Initialize()` sai com 112. `AgentName` e
`Hostname` compõem o `application_name` da conexão,
`slpg/<AgentName>@<Hostname>` (§10.5).

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
| Nenhum `listen.*` | exit 1 |
| `listen.udp` ou `listen.tcp` fora de `host:port` (`:1514` serve); `*_listeners` < 1 | exit 1. UDP e TCP podem dividir a porta (são protocolos distintos; 1514 nos dois é o padrão) |
| `admin.enabled = true` com `admin.bind` vazio ou fora de `host:port` | exit 1 (vazio publicaria `/metrics` em todas as interfaces numa porta aleatória; desligar é `admin.enabled = false`) |
| Número real `NaN` ou `±Inf` em qualquer parâmetro | exit 1 (o `NaN` passa por toda comparação de faixa) |
| `emergency.dir` fora de `server.data_dir` | `WARN` (§20.16) |
| `listen.unix` relativo ou maior que `sun_path`; `listen.unix_mode = 0` | exit 1 |
| `proxy.protocol ≠ off` sem `proxy.trusted` | exit 1 |
| `space` fora de `^[a-z0-9-]+(\.[a-z0-9-]+)*$` | exit 1 (a lib aceita `a..b`, `.x` e `x.`; segmento vazio não serve à convenção LogHub). Acima de 64 caracteres a própria lib já sai com 111 antes |
| `from` fora do UUID canônico em minúsculas | exit 1 |
| `syslog.default_priority` fora de 0–191 | exit 1 |
| `syslog.rfc3164_timezone` desconhecido para o `time.LoadLocation`, ou `Local` | exit 1 (a imagem embute `time/tzdata`; `Local` dependeria do host) |
| `syslog.legacy_charset` fora de `""`, `cp1252`, `latin1`, `windows-1252`, `iso-8859-1` | exit 1 |
| `syslog.max_message_bytes` < 480 ou > 16 MiB; `sd_max_elements` ou `sd_max_params` < 1 | exit 1 (480 é o mínimo da RFC 5424) |
| `ring.capacity_items`, `ring.max_bytes`, `sink.workers`, `sink.batch`, `sink.batch_max_bytes`, `sink.max_retries` abaixo de 1; `ring.high_water` fora de (0, 1]; `log.access_sample` fora de [0, 1]; `tcp.max_conns*` negativo; `retry_backoff_max < retry_backoff_min` | exit 1 |
| `sink.workers > 16` | `WARN` (§10.5) |
| `sink.postgres.pool_size` negativo, ou abaixo de `sink.workers` | exit 1 (`0` deriva `workers + 1`) |
| `sink.postgres.addr`, `username` ou `database` vazios; `addr` que não é um servidor só (`host:porta` com host não vazio, sem vírgula, porta 1–65535 sem zero à esquerda, ou diretório começando com `/` com `:porta` opcional, §10.9); `table` que não é identificador em minúsculas (com esquema opcional) | exit 1 |
| `LOGHUB_EB_URL` em conflito, com esquema que não é `postgres://`/`postgresql://`, com `sslmode` que pede TLS, com parâmetro desconhecido ou repetido (§10.9) | exit 1 |
| durações zero onde zero não é "sem limite" (§16.5) | exit 1 |
| `GOMEMLIMIT` ausente, ou `< ring.max_bytes × 2` | `WARN` (§15.4) |
| parâmetro desconhecido na linha de comando ou no INI | exit 1. Isto inclui toda chave do pai que saiu (`ring.mode`, `sink.type`, `[tls]`, `[filter]`, `[ratelimit]`, `[sink.nats]`...): um INI do `go-syslog-server` não sobe aqui em silêncio |
| linha do INI que não é `[seção]` nem `chave = valor` (um cabeçalho sem `]` inclusive); argumento da linha de comando que não começa com `-` | exit 1, nomeando o número da linha ou só o que vem antes do `=` do argumento, **nunca o conteúdo**: `password: x` (sem `=`) ou `sink.postgres.password=x` (sem `--`) iam inteiros para o stderr do contêiner, no boot e a cada `SIGHUP` (porte do `go-syslog-nats-server`, 2026-10-11) |
| flag sem valor seguida de outra flag (`--sink.workers --sink.postgres.password=x`, um `$WORKERS` vazio num script); valor depois de um espaço (`--sink.postgres.password= x`, `--sink.postgres.password = x`, `--sink.postgres.password= -x`); valor que o shell partiu num espaço (`--sink.postgres.password=$P`, com espaço em `$P`); valor que começa com `--` na forma separada (`--sink.postgres.password --x`) | exit 1, nunca com o valor: um argumento que começa com `--` (ou `-` com o nome de um parâmetro ou de um *switch*) nunca é tomado como valor de outra flag, e só é nomeado se for uma flag deste servidor; o argumento depois de um valor vazio ou de um `=` sozinho não é citado, nem como nome de flag desconhecida; uma palavra solta depois de qualquer valor não é citada. Um valor que começa com `-` vai depois do `=`. Fica uma brecha, de propósito: a cauda partida que começa com traço depois de um valor **não** vazio (`--sink.postgres.password=a -b`) sai como parâmetro desconhecido, porque escondê-la esconderia todo erro de digitação de uma flag (`--sink.workers=4 --tipo.errado=1`) (o parser do `go-webhook-nats-server`, portado em 2026-10-11) |
| variável de ambiente do pai que este servidor não tem (`SYSLOG_LISTEN_TLS`, `SYSLOG_TLS_*`, `SYSLOG_FILTER_*`, `SYSLOG_RATELIMIT_*`, `SYSLOG_SINK_TYPE`, `SYSLOG_RING_MODE`, as dos outros sinks e rings, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE`...) | **um** `WARN` que nomeia todas e diz que são ignoradas (`internal/config/removed.go`, §16.2). Não é erro porque um mesmo `.env` pode alimentar os dois servidores; sem o aviso, o env é lido só pelos nomes do registry e elas sumiriam em silêncio, e quem trocou um servidor pelo outro acreditaria ter um listener TLS ou um filtro |

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
- **FATAL é terminal.** Toda transição e o gauge `syslog_sink_up` são escritos
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
        do boot (tcp, udp, unix); as mensagens já lidas terminam de entrar
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
- **Shutdown por FATAL** (que começa sem sinal nenhum): o **primeiro**
  `SIGTERM`/`SIGINT` é tolerado — logado, e o dump continua —, e só um segundo
  aborta. Um orquestrador que manda `SIGTERM` ao ver o processo sair do ar não
  pode cortar o dump que salva o ring. `SIGQUIT` aborta
  sempre, inclusive fora do shutdown: no boot e com o servidor no ar, saída
  imediata com 130.
- **Sink FATAL durante a fase 4** (o `SIGTERM` chegou com o sink UP e ele virou
  FATAL no dreno): a espera termina na hora, em vez de gastar o `drain_timeout`
  num dreno que não acontece mais; o que sobrou vai para o dump, e a saída é
  **3**.
- **O socket unix** é removido quando o transporte fecha, na fase 3.
- **`SIGHUP` vale o processo inteiro.** É capturado desde a primeira linha; fora
  da janela entre o fim do boot (passo 11) e o início do shutdown, é logado em
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
| `1` | erro de configuração ou de inicialização não relacionada ao banco (inclui a `LOGHUB_EB_URL` com `sslmode` que pede TLS) |
| `3` | banco indisponível ou inutilizável: sink FATAL, `boot_timeout` ou `drain_timeout` estourados, validação de §10.5 recusada, dump que falhou |
| `100`–`114` | falha de identidade, emitida pelo `go-loghub-ident` (`DATADIR`, `MACHINE_ID`, `MACHINE_ID_FILE`, `AGENT_NAME`, `AGENT_UUID`, `HOSTNAME`, `WORKSPACE`; 112 = `Initialize()` duas vezes); a tabela completa está na skill `go-loghub-ident` e em `docker/README.md` |
| `130` | abortado: segundo sinal no shutdown, `SIGQUIT` a qualquer momento, ou `SIGTERM`/`SIGINT` durante os passos 7–9 do boot |

---

## 14. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9102`, **o mesmo do `go-syslog-server`**: os
dois são da mesma família e um operador pode trocar um pelo outro sem mexer no
monitoramento. Por isso **não rode os dois no mesmo host sem trocar o
`admin.bind` de um deles** (o `go-webhook-server` usa `9101`). HTTP/1.1.

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
as séries, e a distribuição por severidade é pergunta para o armazenamento
definitivo.

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
| `syslog_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); o cancelamento do shutdown não conta |
| `syslog_sink_up` | gauge | — |
| `syslog_sink_postgres_duplicates_total` | counter | — |
| `syslog_sink_postgres_pool_conns` | gauge | `state` (acquired, idle, max) |
| `syslog_sink_postgres_pool_waits_total` | counter | — |
| `syslog_events_written_total` | counter | — |
| `syslog_events_dropped_total` | counter | `reason` (sink_logic, malformed, emergency_failed, abort): conjunto **distinto** de `messages_dropped`; as quatro séries existem desde o boot, em zero, como todo label de conjunto fechado (um `rate()` precisa da série antes do primeiro evento) |
| `syslog_emergency_files`, `syslog_emergency_events_pending` | gauge | — (arquivos a reingerir; envelopes do pedaço em voo durante a reingestão, ou a contagem do dump depois de um dump) |
| `syslog_build_info` | gauge | `version`, `commit`, `go` |
| `syslog_config_info` | gauge | `ring_mode` (sempre `memory`), `sink_type` (sempre `postgres`), `transports` |
| `syslog_identity_info` | gauge | `from`, `space`, `agent_name` (fixos por processo, não vêm do emissor) |

Saíram, com o que as produzia: `reason` `ring_unavailable`, `filter` e
`ratelimit`; o label `tls` em `transport`; `syslog_tls_handshake_errors_total`;
`syslog_ring_quarantined_total` e `syslog_ring_redis_claimed_total`; as séries
de `sqlite`, `mysql`, `nats` e `clickhouse`; `syslog_beacons_total`;
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

**Teto pequeno demais.** Cada item custa no mínimo 2 KiB (a menor classe) +
256 B na contabilidade de §9.2. Um `ring.max_bytes` abaixo disso é aceito pela
validação e nenhuma mensagem cabe: o TCP segura o emissor, fecha no
`backpressure_timeout`, e ele reconecta para sempre. Herdado do pai, conhecido e
não corrigido (TESTING §8).

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
- `time/tzdata` é embutido no binário: o fuso de §6.5.2 não depende da imagem.

---

## 16. Configuração

### 16.1 Precedência

```
argv  >  env  >  arquivo INI  >  padrão
```

- Flag: `--<seção>.<chave>` (`--ring.capacity_items=131072`).
- Env: `SYSLOG_<SEÇÃO>_<CHAVE>` em maiúsculas, `.` vira `_`
  (`SYSLOG_SINK_POSTGRES_ADDR`), com as exceções de §16.2.
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
| `[loghub]` | `LOGHUB_EB_URL` | skill `loghub-json-schema` |
| `server.data_dir` | `DATADIR` | `go-loghub-ident` |

As variáveis de identidade (`AGENT_UUID`, `AGENT_NAME`, `WORKSPACE`, `HOSTNAME`,
`MACHINE_ID`, `MACHINE_ID_FILE`, `LOGHUB_IDENT_DEBUG`) **não** passam pelo
registry: são lidas pelo `go-loghub-ident`, e o `--dump-config` mostra o que o
**ambiente** diz numa seção informativa `[identity]`, sem resolver nada
(§13.1 passo 1: o dump não toca identidade nem disco).

**Chaves do pai que saíram.** Na linha de comando ou no INI são parâmetro
desconhecido, exit 1. No ambiente, onde o registry lê só os próprios nomes e
nunca varre o resto, elas sumiriam em silêncio; por isso o boot confere a lista
de `internal/config/removed.go` (as de TLS, filtro, rate-limit, `sink.type`,
`ring.mode`, dos outros sinks e rings, `LOGHUB_BEACON_INTERVAL` e
`LOGHUB_CTL_NAMESPACE`) e emite **um** `WARN` que nomeia todas as definidas e diz
que são ignoradas (§13.2).

Durações aceitam a sintaxe do Go (`30s`, `5ms`). O inteiro puro em segundos
do pai existia só para `LOGHUB_BEACON_INTERVAL` e saiu com ele. Tamanhos aceitam
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

Duas referências: chave recarregável comparada com o que está **em execução**
(para que reverter ao valor de boot seja aplicado); chave de restart comparada
com o **boot** (para que todo `SIGHUP` avise sobre ela até o restart). Cada uma
das duas sozinha falha num dos casos (bugs reais 009 e 010 da standard).

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
workers = 4

[sink.postgres]
addr     = db-1.intranet:5432
username = syslog
database = loghub
table    = loghub.inbox

[admin]
bind = 0.0.0.0:9102

[log]
level = info
```

A senha vai por env (`SYSLOG_SINK_POSTGRES_PASSWORD`) ou dentro da
`LOGHUB_EB_URL` — nesse caso sem `addr`, `username` e `database` no INI (§10.9).

### 16.5 Tabela de parâmetros

A fonte é `internal/config/registry.go`; o `--help` imprime a mesma tabela.

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `shutdown.drain_timeout`,
`tcp.backpressure_timeout` e `proxy.header_timeout` — a validação recusa `0` com
exit 1. Zero é legítimo onde diz o que faz: `shutdown.pre_stop_delay` (sem
espera), `sink.batch_max_delay` (sem espera), `sink.fail_timeout` (sem fusível
por tempo) e `tcp.idle_timeout` (sem limite).

#### `[server]`

| Chave | Padrão | Descrição |
|---|---|---|
| `data_dir` | `./data` (env `DATADIR`) | Raiz de tudo o que o serviço grava, inclusive a identidade; convertida em absoluto; `/data` no contêiner |

#### `[listen]` — vazio desliga o transporte

| Chave | Padrão | Descrição |
|---|---|---|
| `udp` | `:1514` | RFC 5426 |
| `udp_listeners` | `1` | `>1` habilita `SO_REUSEPORT`; o único jeito de passar de um núcleo |
| `udp_read_buffer` | `8MiB` | `SO_RCVBUF`; o efetivo é lido de volta e exposto |
| `tcp` | `:1514` | RFC 6587 |
| `tcp_listeners` | `1` | `>1` habilita `SO_REUSEPORT` |
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

Removido no go-syslog-pg-server (§1.2).

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

Removido no go-syslog-pg-server (§1.2).

#### `[ratelimit]` — recarregável

Removido no go-syslog-pg-server (§1.2).

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `capacity_items` | `65536` | teto de itens; com mensagens de 200 bytes é o que pega primeiro |
| `max_bytes` | `512MiB` | teto de bytes: o `cap` de cada buffer cru + 256 B por item (§9.2); o JLH não conta |
| `high_water` | `0.80` | fração de qualquer teto acima da qual o `/readyz` falha |

Não há `mode` (sempre `memory`) nem `max_inflight` (era do `ring=none`).

#### `[ring.sqlite]`

Removido no go-syslog-pg-server (§1.2).

#### `[ring.redis]`

Removido no go-syslog-pg-server (§1.2).

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `workers` | `4` | goroutines de dreno, que também convertem; cada uma prende uma conexão; `WARN` acima de 16 |
| `batch` | `256` | itens por `Dequeue` e linhas por `INSERT` |
| `batch_max_bytes` | `16MiB` | teto de JLH por pedaço |
| `batch_max_delay` | `5ms` | espera máxima para completar um lote; `0` = sem espera |
| `op_timeout` | `5s` | prazo de uma escrita e da discagem |
| `health_interval` | `2s` | período da checagem; também a latência de UP→DOWN |
| `health_timeout` | `1s` | prazo de uma checagem |
| `fail_timeout` | `30s` | DOWN antes de FATAL; `0` = sem fusível por tempo |
| `max_retries` | `10` | checagens falhas antes de FATAL |
| `boot_timeout` | `300s` | quanto o boot espera o PostgreSQL antes de sair com 3 |
| `retry_backoff_min`, `retry_backoff_max` | `100ms`, `2s` | backoff de um pedaço que falhou |

Não há `type` (sempre `postgres`).

#### `[sink.nats]`

Removido no go-syslog-pg-server (§1.2).

#### `[sink.redis]`

Removido no go-syslog-pg-server (§1.2).

#### `[sink.sqlite]`

Removido no go-syslog-pg-server (§1.2).

#### `[sink.postgres]` e `[sink.mysql]`

Só `[sink.postgres]`; `[sink.mysql]` foi removido (§1.2). Não há chaves de TLS
(`tls`, `tls_ca`, `tls_insecure`): a conexão é sempre `sslmode=disable` (§10.5).

| Chave | Padrão | Descrição |
|---|---|---|
| `addr` | `127.0.0.1:5432` | `host:port`, ou o diretório de um socket unix começando com `/` |
| `username` | `syslog` | o papel; precisa de `INSERT` na tabela e `SELECT` na coluna `uuid`, nada mais |
| `password` | `""` | redigida no `--dump-config` |
| `database` | `syslog` | |
| `table` | `loghub.inbox` | a fila (§3.7.1), `nome` ou `esquema.nome` em minúsculas; nunca criada, só validada |
| `pool_size` | `0` | teto do pool; `0` deriva `sink.workers + 1` |

#### `[sink.clickhouse]`

Removido no go-syslog-pg-server (§1.2).

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
invocado por `/app/go-syslog-pg-server`. Não há `/etc/syslog/tls`.

**Um volume por instância.** Réplicas que montam o mesmo `/data` gravável
compartilham o AgentUUID e publicam o mesmo `from`, sem aviso.

---

## 17. Código

### 17.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-syslog-pg-server/            ESTE módulo
  go-syslog-server/               o pai: dois eixos (rings × sinks), TLS, filtro, rate-limit, beacon
  go-syslog-standard-server/      o antigo, referência
  go-relplog-server/              só especificação, futuro
  go-webhook-server/, go-webhook-fast-server/
```

Módulo independente. Nada é importado de outro módulo do monorepo, nem do pai:
compartilhar pacotes faria os módulos evoluírem amarrados. Tags
`go-syslog-pg-server/vX.Y.Z`.

### 17.2 Layout interno

```
cmd/go-syslog-pg-server/main.go  flags que encerram, lhident.Initialize(), chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão, admin
  config/     registry único -> INI + flags + env + --help + dump; validação;
              LOGHUB_EB_URL; o aviso das variáveis do pai (removed.go)
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
  sink/       interface Sink + postgres, workers, chunk, health, instrumented
  emergency/  dump JSON Lines, ParseLine e reingestão
  obs/        métricas Prometheus próprias + slog
```

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

O pai tinha oito; saíram `rueidis`, `modernc.org/sqlite`, `go-sql-driver/mysql`,
`nats.go` e `clickhouse-go/v2`, com as transitivas de cada um. O binário caiu de
31,8 MB para 17,2 MB (build `darwin/arm64`, as mesmas flags nos dois).
`golang.org/x/net` e `golang.org/x/text` **não** são importados por este código:
PKTINFO e o charset legado são feitos com a stdlib (`syscall`) e com tabela
própria. O `golang.org/x/text` ainda aparece no binário, **transitivamente**,
pelo SASLprep (`secure/precis`) do `pgx`; nenhum pacote daqui o usa.

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
| `FuzzRead` | `emergency` | o leitor do dump tolera truncamento e lixo sem pânico, nunca guarda uma linha acima do teto, e tudo o que `ParseLine` aceita a fila aceitaria |
| `FuzzJSONBSafe` | `sink` | a troca do `\u0000` para a `loghub.inbox` (§3.7.1) devolve JSON válido igual ao original com U+0000 → U+FFFD, e nada mais |
| `FuzzInboxPayload` | `sink` | o payload da coluna é JSON sem escape NUL, `raw` volta exato e os demais textos só trocam U+0000 por U+FFFD |

O `FuzzHead` do pai saiu com o codec do cabeçalho (§3.4).

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=10m
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrameOracle$  -fuzztime=10m
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$    -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzInboxPayload$ -fuzztime=10m
```

### 18.4 Corpus de mensagens reais

`internal/convert/testdata/corpus/` guarda mensagens **capturadas de emissores
reais** (`rsyslog`, `syslog-ng`, `journald`, `logger(1)`, glibc, Cisco IOS,
MikroTik, pfSense, HAProxy, nginx, Kubernetes, Windows via NXLog, log4j): um
`.raw` com os bytes exatos e um `.want` com os campos esperados do payload.
**O corpus nunca encolhe**: o teste falha abaixo de vinte entradas. Um caso novo
de campo entra aqui antes da correção do conversor.

### 18.5 Integração em processo

A pilha **real** (a mesma `run()` do binário), contra um PostgreSQL real: como
o único destino é a fila, todo teste que passa do passo 7 do boot precisa de
`SYSLOG_TEST_PG_DSN` (§18.6) e pula sem ela; só as recusas de antes do passo 7
(exit 1) rodam sem banco. Cada teste cria a própria fila a partir de
`docker/postgres/inbox.sql`, num esquema próprio. Falhas do destino são
injetadas decorando o sink real (`App.wrapSink`). Cobre: todos os
transportes chegam ao destino (UDP com `dst-ip`, TCP nas duas molduras, unix),
PROXY v1 e v2, fidelidade do JLH (o `raw` volta byte a byte, com NUL no meio),
`SIGHUP` de `[log]` (ida e volta, chave sombreada, arquivo inválido), dump de
emergência e reingestão exatos (mesmo `uuid`, mesmo JLH, mesmo depois de trocar
uma chave `[syslog]`), dump danificado em `.corrupt`, shutdown gracioso sem
perda, segundo sinal (130), nenhum transporte (exit 1), chaves do pai na linha
de comando (exit 1) e no env (`WARN`), códigos 100–114 do `go-loghub-ident` num
subprocesso, e a ordem do boot.

### 18.6 Destino real

A suíte do PostgreSQL pula sem a variável e cria a fila **a partir do arquivo que
o operador aplica** (`docker/postgres/inbox.sql`), num esquema próprio por teste:

```bash
SYSLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

O DSN de teste também é sem TLS (`sslmode=disable`). O que ela prova está em
`docs/TESTING.md` §6.

### 18.7 Emissores vivos

`SYSLOG_TEST_EMITTERS=1` (com `SYSLOG_TEST_PG_DSN`) sobe rsyslog e syslog-ng em contêineres descartáveis e
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
| Perda com `SIGTERM` durante carga, TCP | **zero**, reconciliada contra a fila |
| Perda com FATAL do sink | **zero**, via dump e reingestão |

---

## 19. Matriz de garantias

Leia junto com §2.3: a garantia só existe no transporte que confirma (TCP).

| Combinação | A confirmação significa | Perda em `kill -9` | Perda em `SIGTERM` | Perda em FATAL do sink |
|---|---|---|---|---|
| `memory` → `postgres` | aceito na RAM deste nó | o backlog do ring | nada, se o dreno couber em `drain_timeout`; o resto vai para o dump | nada: o dump (§12) |

Depois do `INSERT` confirmado, a durabilidade é a do PostgreSQL
(`synchronous_commit`, e nunca uma fila `UNLOGGED`, §10.5). Um dump que falha
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
11. Removido no go-syslog-pg-server (era do rueidis, §1.2).
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
17. O servidor nunca cria o que valida: o sink nunca faz DDL na fila.
18. Esquema e permissão são `fatal`, nunca `logic` (classe `42` do SQLSTATE,
    `42501`, `28`); erro desconhecido é `network`, nunca `logic`.
19. Removido no go-syslog-pg-server (era do `nats.go`, §1.2). O princípio fica:
    nenhuma biblioteca cliente guarda nem retenta o que o ring guarda e o worker
    retenta.
20. Nenhuma linha de dump que a fila poderia recusar chega ao `INSERT`:
    `emergency.ParseLine` a recusa antes (§12.2). (No pai: nenhum destino recebia
    uma mensagem que o teto dele recusaria.)
21. O servidor nunca emite `beacon`, `ping`, `pong` nem sonar.
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
| TLS (listener ou cliente) | Fora por decisão do usuário (§1.2); voltar exige decisão nova, não só código |
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
sai daqui e a regra vai para a seção que ele cita, spec primeiro.

1. **Teto por IP contornável com IPv6** (§4.4). `tcp.max_conns_per_ip` conta
   pelo endereço exato, e quem tem um /64 (qualquer host com SLAAC) usa um
   endereço por conexão: o teto não o segura e, com `tcp.max_conns` ligado, ele
   ocupa o teto global e trava os emissores legítimos no accept. Direção, a
   decidir pelo usuário: aceitar como está (já dito em §4.4), ou contar o IPv6
   por um prefixo configurável (uma LAN inteira costuma ser um /64, então o
   padrão é escolha), com isenção por CIDR.
2. **O shutdown é contado como falha do PROXY** (§4.6). Uma conexão parada à
   espera do cabeçalho quando o transporte fecha (o `Close` vence o prazo) conta
   `proxy_headers_total{result="absent"}`, ou `invalid` com um cabeçalho pela
   metade, e em `required` também `messages_dropped_total{reason="proxy"}` com
   um `WARN` que culpa o balanceador: todo deploy parece uma falha dele, sem
   perda de dado. Direção: um erro de `ReadProxyHeader` com `closing` ligado
   encerra a conexão sem contar nem logar nada.
3. **Custo fixo de 64 KiB por conexão TCP** (§4.4, §5.4). O `bufio.Reader` de
   cada conexão (`streamReadBuffer`) é alocado no accept e fica preso enquanto
   ela viver, ociosa ou não; com `tcp.max_conns = 0` por padrão, milhares de
   conexões ociosas somam centenas de MiB que `ring.max_bytes` não vê, até o
   `GOMEMLIMIT` ou o OOM (e o backlog do ring se perde, §19). O "custa um bloco"
   de §5.4 vale para o buffer da moldura, não para a conexão. Direção: um buffer
   de 4–8 KiB (o maior `Peek` é de 12 bytes, a assinatura do PROXY v2), medido
   com o benchmark de recepção, e o custo fixo por conexão documentado com a
   recomendação de um `max_conns` em produção.
4. **Offset fora de faixa aceito no RFC 3339 da RFC 3164** (§6.5.1). Ali
   `+25:00` ou `+05:75` são aceitos e `timestamp` sai deslocado, sem
   `timestamp-invalid`, enquanto a RFC 5424 recusa o mesmo offset (§6.4.2); o
   `raw` guarda o original. O conversor de referência da skill faz o mesmo (só
   confere a faixa no ramo 5424), então corrigir é desvio. Direção, a decidir
   pelo usuário: conferir `hh ≤ 23` e `mm ≤ 59` também ali (`timestamp-invalid`,
   `timestamp` omitido, o resto do cabeçalho segue), registrado como desvio D9 e
   levado ao mantenedor da skill; ou dizer em §6.5.1 que a referência aceita
   qualquer offset de dois dígitos.
5. **`23502` em execução é `logic`** (§10.5). Este servidor nunca manda NULL
   nas dez colunas, então um `not_null_violation` depois do boot é recusa da
   **tabela** (uma coluna `NOT NULL` acrescentada, o `DEFAULT` de `received_at`
   retirado, um trigger): como classe `23`, todo pedaço é descartado em
   `sink_logic` com o sink UP, para sempre. Direção: `23502` fatal (dump e exit
   3; o boot seguinte já recusa a coluna extra obrigatória pelo nome), com a
   tabela de §10.5 e `TestClassifySQLState` mudando juntas; um trigger que anula
   uma coluna só em certas linhas passa a derrubar o processo, e isso fica
   escrito.
6. **A validação do boot corre no prazo da sonda de saúde** (§10.5, §13.1 passo
   7). O `Health` roda a validação com o contexto de `sink.health_timeout` (1 s),
   e ela custa ~18 idas e voltas (conexão, SCRAM e as consultas de catálogo,
   cada uma preparada na primeira vez): com o banco a mais de ~55 ms de RTT
   nenhuma tentativa termina, e o boot sai com 3 no `boot_timeout` culpando a
   rede. Subir `health_timeout` contorna, mas afrouxa a detecção de queda.
   Direção: a validação com prazo próprio de `sink.op_timeout`, ainda seguindo o
   cancelamento do boot.
7. **Erros da `LOGHUB_EB_URL` ainda citam trechos dela** (§10.9). O erro de URL
   inválida repete o texto interno do `url.Error` (`invalid port ":<x>" after
   host`, que numa senha com `/` é a senha até a barra; o erro de escape cita
   três caracteres), e o nome de um parâmetro de consulta desconhecido e o valor
   de um `sslmode` desconhecido são citados, que numa senha com `?` são o resto
   dela, o host e o banco. A senha vai ao stderr a cada reinício. Direção:
   nenhum texto do parser no erro, e um nome de parâmetro citado só se estiver
   numa lista fixa de nomes da libpq, com a dica de codificar `?`, `/` e `#` na
   senha.
8. **Logs falsos depois de um `fsync` de diretório que falhou** (§12.1, §12.2).
   `Remove` e `Quarantine` devolvem o erro do `fsync` depois de o arquivo já ter
   sido apagado ou renomeado, e o boot os lê como falha da operação: loga que o
   dump "será entregue de novo" quando ele já não existe, ou "não pôde ir para a
   quarentena" com o caminho antigo, sem o `ERROR` com as contagens de bytes. E
   o erro do `fsync` do diretório depois da publicação cita o nome temporário,
   que já não existe. Só o log engana: a política (o `fsync` decide o dump,
   §12.3) é deliberada. Direção: distinguir "feito, sem `fsync`" (destino
   devolvido pelo `Quarantine`, `Lstat` sem o arquivo no `Remove`) com um `WARN`,
   e citar o nome final.
9. **`syslog_emergency_files` zera com dump no disco** (§12.2, §14.1). No fim da
   reingestão o gauge vai a 0 mesmo quando um dump ficou para trás (ilegível, ou
   com a remoção ou a quarentena falhando): um alerta em
   `syslog_emergency_files > 0` não dispara, e só um `ERROR` do boot avisa.
   Direção: terminar com a contagem de `List()`, os arquivos que restam,
   legíveis ou não; `syslog_emergency_events_pending` continua zerado, porque é
   o pedaço em voo.
10. **O `SIGQUIT` é tolerado no shutdown por FATAL** (§13.4). A tolerância ao
    primeiro sinal não distingue o sinal, e um `kill -QUIT` durante o dump do
    FATAL só aborta no segundo sinal, contra o "`SIGQUIT` aborta sempre" de
    §13.4 e §13.5; o dump segue até o fim, sem perda. Direção: tolerar só
    `SIGTERM`/`SIGINT`, com um teste em que o `SIGQUIT` é o primeiro sinal de um
    shutdown fatal e sai com 130.
