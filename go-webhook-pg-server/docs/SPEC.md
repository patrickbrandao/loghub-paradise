# go-webhook-pg-server — Especificação v1

Status: **congelada para implementação** (escopo de 2026-10-07, abaixo)
Data: 2026-10-07
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server`
Binário: `go-webhook-pg-server`
Entrada: `docs/DECISOES.md` (entrevista de 2026-10-07, quatro decisões).
Origem: cópia do `go-webhook-fast-server` em 2026-10-07 (o estado dele depois
da rodada do `sink=clickhouse`), reduzida ao que está em §0.

## 0. Escopo desta versão

O `go-webhook-pg-server` é o `go-webhook-fast-server` com **um ring e um
destino**: o ring em memória e a fila `loghub.inbox` do PostgreSQL. O objetivo é
um servidor de webhooks rápido e com dependências mínimas — três em produção
(§14.3), contra oito do fast.

| Fica, igual ao fast | Sai |
|---|---|
| Envelope JLH `type=webhook`, `mode=fast`, `uuid`/`event` de uma amostra de relógio, identidade `go-loghub-ident` (§3) | `ring.mode = sqlite` e `none` (§7.4, §7.5) |
| `ring.mode = memory` (§7.3) | `sink.type = redis`, `sqlite`, `mysql`, `nats`, `clickhouse`, `none` (§8.2–§8.4, §8.6–§8.8) |
| `sink.type = postgres` na `loghub.inbox`, validada e nunca criada (§3.6, §8.5) | Rate-limit (§6): papel do proxy reverso |
| Pipeline HTTP, h2c, `SO_REUSEPORT`, limites, resolução de IP por proxy confiável (§4, §5) | Recarga por `SIGHUP` (§13.3): a configuração é lida só no boot |
| Saúde do sink, `503 sink_down`, FATAL, shutdown em cinco fases (§10) | |
| Porta admin, métricas `whs_*`, logs (§11) | |
| Dump de emergência e reingestão no boot (§9) — **o formato muda para JSON Lines** | O dump binário de comprimento + bytes |

**Numeração.** As seções seguem a numeração do `go-webhook-fast-server`, e uma
seção retirada continua no lugar, com uma linha dizendo para onde foi. O código
cita a spec por número (`§8.5.7`), e uma correção portada de um módulo para o
outro (`diff -ruN`) continua apontando para a mesma regra.

**Compatibilidade de configuração (decisão 3).** Um INI, uma lista de flags ou
um ambiente `WHS_*` escrito para o fast com `ring.mode = memory` e
`sink.type = postgres` sobe aqui sem mudança, e os painéis das métricas `whs_*`
continuam valendo. Três chaves **seletoras** continuam aceitas, cada uma só com
o valor que este servidor implementa: `ring.mode = memory`,
`sink.type = postgres` e `ratelimit.enabled = false` (qualquer outro valor é
`exit 1`, com a mensagem dizendo que o recurso foi retirado). Toda outra chave
de recurso retirado — `[ring.sqlite]`, `ring.max_inflight`, `[sink.redis]`,
`[sink.sqlite]`, `[sink.mysql]`, `[sink.nats]`, `[sink.clickhouse]`, o resto de
`[ratelimit]` — num INI ou numa flag é **chave desconhecida** (`exit 1`), o
mesmo tratamento que o `server.node_id` recebeu na rodada do JLH: uma
configuração que pede um recurso que não existe não pode subir em silêncio. O
padrão de `sink.type` passa a ser `postgres` (era `redis`).

**No ambiente é diferente, e de propósito.** O carregador só lê as variáveis
`WHS_*` que o registry conhece; uma desconhecida nunca foi vista, no fast
inclusive (o ambiente é compartilhado com CI, shell e compose). Assim
`WHS_RATELIMIT_RATE` ou `WHS_SINK_REDIS_ADDRS` esquecidas no ambiente são
**inertes**, e o ambiente do `common.yml` do fast — que exporta
`WHS_RATELIMIT_RATE`/`BURST` com o limitador desligado — sobe aqui. Um
**seletor** no ambiente continua recusando o recurso retirado
(`WHS_RING_MODE=sqlite`, `WHS_RATELIMIT_ENABLED=true`: `exit 1`): é a fronteira
entre ajustar algo que não existe e pedir algo que não existe (revisão
independente de 2026-10-07).

**`SIGHUP`.** Sem recarga, o sinal é **ignorado** com um `WARN` dizendo que a
configuração só muda com reinício. Deixá-lo com a ação padrão do Go encerraria o
processo — um `kill -HUP` de um procedimento antigo do fast derrubaria a
instância sem dreno nem dump.

**O que continua aberto no fast** (`docs/DECISOES.md` dele: `stage`
`auto|ring|sink`, `503` no ring cheio, PROXY protocol, nomes `WEBHOOK_*`) não
entra aqui antes de entrar lá; o `stage` da resposta é sempre `ring`.

---

## 1. Escopo

Servidor HTTP de ingestão de webhooks. Recebe requisições arbitrárias vindas da
internet, encapsula cada uma num envelope JLH do LogHub e o enfileira na
`loghub.inbox` de um PostgreSQL. Escalável horizontalmente: instâncias são
independentes e não compartilham estado entre si; o banco é o único ponto comum.

### 1.1 Objetivos

- Máximo de requisições por segundo com mínimo de CPU e memória.
- Dependências mínimas: só o driver do PostgreSQL e as duas bibliotecas LogHub
  (§14.3).
- Nunca responder `200` para um evento que não está no ring (§2.2).
- Backpressure explícito em vez de perda silenciosa.

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| Outros destinos e rings | Use o `go-webhook-fast-server` (§0) |
| Rate-limit | Papel do proxy reverso, que vê o IP real antes de qualquer header (§6) |
| Recarga a quente | Uma configuração só, a do boot; reinício é o caminho (§13.3) |
| HTTP/3 e QUIC | Nenhum emissor relevante de webhook usa; 2–4× a CPU por byte |
| TLS na porta pública | Responsabilidade do proxy reverso (Traefik/nginx) |
| Roteamento interno de URLs | Critérios de roteamento ficam no proxy reverso |
| Parse do corpo | O corpo é tratado como bytes opacos |
| Verificação de assinatura HMAC | Responsabilidade do consumidor da fila |
| `Upgrade: h2c` (RFC 7540 §3.2) | Removido pela RFC 9113; stdlib não implementa |
| Multi-tenancy / múltiplas tabelas | v2 |

---

## 2. Arquitetura

### 2.1 Um ring, um destino

O fast tem dois eixos ortogonais (ring × sink, 21 combinações). Aqui cada eixo
tem um valor só, mas a separação continua no código: `ring.Ring` e `sink.Sink`
são interfaces com uma implementação cada (decisão 4), e nenhuma consulta o tipo
da outra. É o que mantém o porte de correções do fast por `diff`.

- **RING** — *onde o evento espera* entre ser aceito e ser entregue: `memory`.
- **SINK** — *para onde o evento vai*: `postgres` (`loghub.inbox`).

```
   internet
      |
   [proxy reverso: TLS, roteamento, WAF, limite de taxa]
      |  HTTP/1.0 · HTTP/1.1 · h2c (mesma porta)
      v
 +------------------------------------------------------------------+
 | porta pública  (server.bind, padrão :8080)                        |
 |  accept -> headers -> clientip -> 413 -> envelope JLH             |
 +---------------------------------+--------------------------------+
                                   |
                    +--------------v---------------+
                    | RING memory                  |
                    |  canal + tetos de itens/bytes|
                    +--------------+---------------+
                                   | sink.workers (padrão 4)
                    +--------------v---------------+
                    | SINK postgres                |
                    |  INSERT … unnest, em lote    |
                    +--------------+---------------+
                                   v
                          loghub.inbox (PostgreSQL 18)

 emergência: ring não-vazio + sink FATAL ou drain_timeout
             => <emergency.dir>/emg-<agent_uuid>-<unixnano>.jsonl   (§9)

 +------------------------------------------------------------------+
 | porta admin (admin.bind, padrão 127.0.0.1:9101)                   |
 |   /healthz   /readyz   /metrics                                   |
 +------------------------------------------------------------------+
```

O ring é **único e global** (não particionado por CPU). É o único ponto de
serialização do desenho, e é intencional: dá uma métrica única de profundidade,
backpressure justo (`429` só quando o sistema inteiro está saturado) e código
simples. `server.listeners > 1` (SO_REUSEPORT) existe como knob, mas o ring
continua único.

### 2.2 Regra de aceitação

> O `200` é enviado no instante em que o item entrou no canal do ring — nunca
> antes.

A garantia é a do ring em memória (§7.3, §16.1): perda sob crash não-gracioso;
sob `SIGTERM`, o dreno do shutdown (§10.3); sob falha fatal do PostgreSQL, o
dump de emergência (§9).

---

## 3. Modelo de dados (LogHub)

O evento é um **envelope JLH** (JSON LogHub) do tipo `webhook`: o servidor é um
*bridge-in* HTTP do LogHub.

### 3.0 Fontes normativas

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH: nove campos, regras, `jlh-envelope.schema.json` |
| skill `loghub-type-webhook-fast` | consultada em 2026-10-06 | `type=webhook`, payload (`mode`, `network`, `headers`, `query`, `content`...), respostas HTTP |
| skill `loghub-postgres-table-inbox` | consultada em 2026-10-06 | `sink=postgres`: fila `loghub.inbox` (§3.6) |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 | `uuid` nível 3 de uma amostra de relógio |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 | `from` (AgentUUID), `space` (Workspace), `DATADIR` |

Cópias dos schemas ficam em `internal/jlhcheck/loghub/` e só os testes
as leem (§15.1): a validação por JSON Schema **não** roda no caminho quente (o
serializador é provado por testes de contrato e fuzz). Onde as skills divergem,
vale a mais restritiva: `space` `^[a-z0-9-]+(\.[a-z0-9-]+)*$` com até 64
caracteres, `step` ≤ 32, `from` UUIDv7 minúsculo.

O `go-webhook-server` é o irmão com `type=webhook.request` e `mode` `fast` ou
`bucket` conforme o ring (skill `loghub-type-webhook-schema`). O payload dos
dois é o mesmo, campo a campo; muda o `type`, o `mode` e o corpo da resposta
(§4.4).

### 3.1 Envelope

```json
{
  "uuid": "01a1122b-8b69-73a4-b9d1-074a5fb34be2",
  "from": "01a1122b-6a52-7ee1-a4fe-16a232e7f4ed",
  "space": "default",
  "type": "webhook",
  "level": 9,
  "sid": 1,
  "step": 0,
  "event": { "uts": 1791313406, "sub": 57932925, "iso": "2026-10-06T17:03:26.057Z" },
  "payload": {
    "mode": "fast",
    "network": {
      "ip":  { "version": 4, "src": "203.0.113.9", "dst": "0.0.0.0" },
      "l4":  { "sport": 0, "dport": 443 },
      "sec": { "mode": "proxy" },
      "meta": [
        { "key": "peer-addr", "value": "10.0.0.7" },
        { "key": "peer-port", "value": 51234 },
        { "key": "src-source", "value": "xff" },
        { "key": "http-proto", "value": "HTTP/1.1" }
      ]
    },
    "method": "POST",
    "schema": "https",
    "host": "api.exemplo.com",
    "path": "/hook/telegram",
    "url": "https://api.exemplo.com/hook/telegram?token=abc",
    "headers": [
      { "name": "content-type", "value": "application/json" },
      { "name": "host", "value": "api.exemplo.com" },
      { "name": "x-forwarded-for", "value": "203.0.113.9" },
      { "name": "x-forwarded-proto", "value": "https" }
    ],
    "params": {},
    "query": { "token": "abc" },
    "content": { "length": 15, "type": "application/json", "encoding": "utf-8", "body": "{\"update_id\":1}" }
  }
}
```

Exatamente os nove campos, na ordem canônica, escritos por um serializador
próprio por *append* (§12.1), **uma** vez por requisição: o ring, o dump e todos
os sinks recebem os mesmos bytes.

| Campo | Valor |
|---|---|
| `uuid` | UUIDv7 **nível 3** (`go-loghub-uuidv7`), minúsculo, `GenerateAt(Level3, t)` sobre a **mesma** amostra `t` que preenche `event`. Gerado no passo 10 de §4.3 e nunca regerado: ring, retry, dump e reingestão reusam o mesmo. É o UUID devolvido ao cliente. |
| `from` | AgentUUID desta instância (`go-loghub-ident`), validado no boot. O cliente externo vai em `payload.network`, nunca aqui. |
| `space` | Workspace da identidade (`WORKSPACE`, `$DATADIR/workspace` ou `default`), validado no boot pela gramática da interseção (§3.0), sem segmento vazio. Não há rotas, então não há space por rota. |
| `type` | `webhook`. |
| `level` | `9` (data-channel). |
| `sid` | PID do processo. Nunca aleatório; agrupa por `(from, sid)`. |
| `step` | `0`: o servidor é a origem da mensagem. |
| `event` | `uts` (segundos), `sub` (ns no segundo) e `iso` (`yyyy-MM-ddTHH:mm:ss.SSSZ`, **truncado** no ms) da amostra `t`, tomada no fim da leitura dos headers, antes do corpo. Era a dívida do §3.1 antigo (duas leituras de relógio); com `GenerateAt` ela acabou: `uuid` e `event` concordam até o nanossegundo (no macOS, até o µs). |
| `payload` | §3.2. |

### 3.2 Payload `webhook`

Todas as propriedades são sempre emitidas, na ordem abaixo; vazias continuam
presentes (`[]`, `{}`, `""`). Não há `callback` nem `forward`: a v1 não tem rotas.

| Propriedade | Origem |
|---|---|
| `mode` | Sempre **`fast`** (skill `loghub-type-webhook-fast`): o `uuid` volta ao cliente quando o envelope está no ring, e nenhum worker confirma. O `stage` da resposta é sempre `ring` (§4.4). Não há `callback`. |
| `network.ip` | `src`: o IP resolvido por §5, IPv4 mapeado em IPv6 convertido. `version`: `4` ou `6`. `dst`: o `X-Server-Addr` quando o par TCP é confiável e o manda, com ou sem `X-Forwarded-For`; senão o IP local que aceitou a conexão quando `src` veio do TCP; senão (`src` de cabeçalho sem `X-Server-Addr`) ou quando ele é de outra família, o não especificado da família (`0.0.0.0`/`::`). Sem porta, colchetes ou zona. |
| `network.l4` | `sport`: a porta remota quando `src` veio do TCP; o `X-Client-Port` do proxy confiável ou `0` quando veio de cabeçalho. `dport`: sem par confiável, a porta local. De par confiável, o `X-Forwarded-Port`; sem ele, a porta local só se `src` veio do TCP e o TLS não terminou no proxy; senão a porta do host efetivo (só de um host aceito, abaixo), senão `443`/`80` pelo `schema`. A porta local descreve o salto proxy→servidor: com `src` de cabeçalho ou TLS no proxy ela não é a que o cliente usou — `https://pub.example:8080` para um cliente que foi à 443. `mss` não é emitido. |
| `network.sec` | `mode: "proxy"` quando o par TCP é confiável (`proxy.trusted`) e o último `X-Forwarded-Proto` é `https`; senão `"none"` (não há TLS na porta pública, §1.2). `fp3`/`fp4` de `X-TLS-JA3`/`X-TLS-JA4` e `fp2` de `X-HTTP2-Fingerprint`, só de par confiável, cada um a **última linha inteira** (JA3 e a fingerprint HTTP/2 têm vírgulas próprias; os demais cabeçalhos de proxy valem pelo último elemento da última linha); com `mode: "none"` não há `fp3`/`fp4`. |
| `network.meta` | Campos da v1 sem lugar no JLH, nesta ordem: `peer-addr` (IP da ponta TCP, sempre), `peer-port`, `src-source` (`tcp`, `xff` ou `xri`, §5) e `http-proto` (`HTTP/1.0`, `HTTP/1.1`, `HTTP/2.0`). |
| `method` | O método da linha de requisição. Fora de `^[A-Z]+$` a requisição é recusada com `405` antes do envelope (§4.2). |
| `schema` | `https` quando `sec.mode` é `proxy`, `http` quando é `none`. O nome é `schema`, não `scheme`. |
| `host` | `X-Forwarded-Host` de par confiável (o último elemento), senão `r.Host` (`Host` ou `:authority`), em minúsculas **só ASCII** (o `strings.ToLower` do Unicode faz do sinal Kelvin, U+212A, um `k`: `\u212Aexample.com` virava `kexample.com`), sem porta e sem colchetes. Precisa ser um IP ou um nome de `[a-z0-9._-]`: vazio (HTTP/1.0 sem `Host`) ou com algo que o `net/http` aceita e nenhuma URL carrega (`*`, `$`, `!`...) vira o `dst`, e o valor cru continua em `headers`. |
| `path` | `r.URL.EscapedPath()`, percent-encoding preservado, sem a query; vazio vira `/`. `OPTIONS *` é recusado com `400` (§4.2). |
| `url` | `schema://host[:porta]path[?query]`; a porta é a do host efetivo, senão `dport`, omitida quando é a padrão (443/80); host IPv6 entre colchetes. O `path` vai como no campo `path`. A query vai como chegou, **exceto** os bytes que o RFC 3986 não aceita numa query — `#`, espaço, `` " < > \ ^ ` { \| } ``, `[`, `]`, controles, bytes a partir de `0x80` e um `%` que não inicia escape —, que vão como `%XX` (maiúsculo). |
| `headers` | Array de `{name, value}`, uma entrada por valor, nomes em minúsculas, ordenado de forma estável por nome (valores de um mesmo nome na ordem de chegada; o `http.Header` do Go não guarda a ordem entre nomes). `host` é recolocado a partir de `r.Host`. Saem os hop-by-hop (`connection`, `keep-alive`, `proxy-connection`, `te`, `trailer`, `transfer-encoding`, `upgrade`), os nomes listados em `Connection` e os de `headers.exclude` (§3.4). |
| `params` | Sempre `{}` (sem rotas). |
| `query` | A query decodificada como o `parse_qsl(keep_blank_values=True)` do Python: separador `&`, `+` vira espaço, `%XX` decodificado (escape inválido fica literal; UTF-8 inválido vira U+FFFD, um por subparte máxima), chave sem `=` vale `""`, pedaço vazio ignorado. Uma ocorrência → string; repetida → array na ordem de chegada. Chaves na ordem da primeira ocorrência, **agrupadas pelo texto escrito**: `%FF` e `%FE` viram o mesmo U+FFFD e são uma chave só, com array. `{}` sem query. |
| `content.length` | Bytes do corpo lido (depois de desfeito o chunked), antes de qualquer base64. |
| `content.type` | O primeiro `Content-Type` como veio; `""` se ausente. Vale também quando o cabeçalho sai de `headers` (listado em `Connection` ou em `headers.exclude`): o campo diz o que chegou, como a skill pede ("como recebido"); o `validate.py` só avisa nesse caso, e o `jlhcheck` confere a igualdade só quando o item `content-type` está na lista. |
| `content.encoding` | `utf-8` quando o corpo é UTF-8 válido, **não tem NUL** e o escape JSON não fica maior que o base64; senão `base64`. Uma pré-passada mede os dois tamanhos antes de escrever. |
| `content.body` | O corpo cru, sem parse nem reformatação (assinaturas HMAC continuam verificáveis): texto escapado, ou base64 padrão com padding. `""` quando `length` é 0. `Content-Encoding: gzip` não é desfeito. |

**Por que a `url` reescapa parte da query.** A skill pede a query "crua, sem
reescapar" e também `format: uri`, e o seu `validate.py` confere que o
`parse_qsl(urlsplit(url).query)` é igual a `query`. Cru, um `#` vira fragmento
(`?a=1#b` dava `query` `{"a":"1#b"}` e, lida da `url`, `{"a":"1"}`); um byte
inválido ao lado de um escape (`%C3\xA9`) decodifica junto na `query` e separado
na `url`; e `"<>{}` deixam a `url` fora do RFC 3986. Percent-encodar só esses
bytes mantém cru tudo o que uma URI aceita, e um `%XX` decodifica no próprio
byte: a `query` não muda, e as duas leituras coincidem. O `path` não precisa
disso: o `EscapedPath` do `net/http` já codifica o que o caminho não aceita
(`[` e `]` ele deixa, e eles ficam crus nos dois campos).

**Um U+FFFD por subparte máxima.** Toda string do JLH troca UTF-8 inválido por
U+FFFD, como o `bytes.decode('utf-8', 'replace')` do Python: um por sequência
inválida máxima (Unicode §3.9 — o byte inicial e as continuações que ainda
poderiam completá-lo), não um por byte, como o `encoding/json`. `%E2%80` é um
U+FFFD só, e é o que o `validate.py` lê de volta da `url`.

**Por que sem NUL no texto:** um `\u0000` é JSON válido, mas o `jsonb` da
`loghub.inbox` o recusa (§3.6) e muitos consumidores o tratam mal; o base64 leva
os bytes exatos. NUL ainda pode aparecer em `query` (um `%00`) e é tratado no
destino que não o aceita (§3.6).

**O maior envelope** é calculado, não configurado; é o teto de uma linha do
dump de emergência (§9.2) e entra no aviso de lote de §8.5.4:

```
4 × ⌈B / 3⌉ + 14 × H + 13 × U + 8 KiB
B = server.max_body_bytes, H = server.max_header_bytes, U = server.max_uri_bytes
```

O corpo pesa no máximo o base64 (`4⌈B/3⌉`); cada byte de cabeçalho no máximo
triplica no escape (obs-text vira U+FFFD) e aparece até duas vezes (`headers` e
o campo derivado: `host`, `content.type`, `fp*`, o host da `url`), com 23 bytes
de moldura por linha; a URI aparece em `path`, `url` e `query` decodificada.
`TestWorstCaseEnvelopeIsBounded` prova a fórmula com entradas adversárias. Com
os padrões (5 MiB, 64 KiB, 8 KiB) são ~7,7 MiB.


### 3.3 Entrada do stream Redis — retirada

`sink=redis` não existe neste servidor (§0).

### 3.4 Headers

`headers.exclude` (lista de nomes, vazia por padrão) omite headers do envelope;
comparação sem distinção de maiúsculas. **Nenhuma redação por padrão**:
`Authorization`, `Cookie` e as assinaturas HMAC (`X-Hub-Signature-256`,
`Stripe-Signature`) são capturados integralmente, porque o worker pode precisar
deles para autenticar o chamador. Os cabeçalhos de proxy ficam na lista como
chegaram: são evidência do caminho.


### 3.5 Identidade

`from` é o AgentUUID e `space` o Workspace, do `go-loghub-ident`
(`lhident.Initialize()` no `main`, uma vez, §10.1). `AgentName` e `Hostname`
nomeiam as conexões do driver (`application_name = whs/<agent_name>@<hostname>`),
e o AgentUUID nomeia o dump (§9.1). A identidade é persistida em `ring.data_dir`
(exportado como `DATADIR`) e fica congelada até o fim do processo.
`server.node_id` não existe.

### 3.6 Fila `loghub.inbox` (`sink=postgres`)

No PostgreSQL o destino é a **fila de espera `loghub.inbox`** da skill
`loghub-postgres-table-inbox`, comum a todo produtor LogHub. Ela guarda os
envelopes que chegaram e ainda não foram processados; um worker de outro
software os retira em ordem de `uuid` (`DELETE … FOR UPDATE SKIP LOCKED`, na
mesma transação em que grava o resultado). Este servidor só **enfileira**, e
**nunca cria nem altera** a tabela (§8.5.1): `docker/postgres/inbox.sql` é a
cópia literal da DDL da skill.

| Coluna | Envelope | Conversão |
|---|---|---|
| `uuid` | `uuid` | `uuid` nativo; é a ordem da fila |
| `from_uuid` | `from` | `uuid` nativo |
| `event_at` | `event.uts` + `event.sub` | `time.Unix(uts, sub)` UTC truncado no µs, `timestamptz` |
| `received_at` | — | `DEFAULT now()` da tabela; não é enviado |
| `sid`, `step`, `level` | `sid`, `step`, `level` | — |
| `event_nsec` | `event.sub` | `sub % 1000` |
| `space`, `type` | `space`, `type` | — |
| `payload` | `payload` | `jsonb`; `\u0000` vira `\ufffd` |

```sql
-- PostgreSQL 18+ (docker/postgres/inbox.sql)
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

- **Enfileirar é idempotente:** `INSERT … ON CONFLICT (uuid) DO NOTHING`. A
  chave da fila é só `uuid`; aqui ela coincide com o `(uuid, step)` do JLH,
  porque o servidor só emite `step = 0`.
- **A fila não é fiel ao byte:** o `jsonb` reordena chaves e normaliza o
  documento. O corpo continua exato (`content.body`), porque é uma string; quem
  precisa dos bytes do JLH lê um dump de emergência (§9.1), que os guarda byte a
  byte, ou usa o `go-webhook-fast-server` com outro destino.
- **NUL:** o `jsonb` recusa o escape `\u0000` (`22P05`), e a classe `22`
  descartaria o lote inteiro. O corpo nunca o tem (§3.2), mas um `%00` na query
  o produz. Na cópia que vai para a coluna, cada escape `\u0000` vira `\ufffd`;
  um `\\u0000` (barra literal) fica como está. O JLH do ring e do dump
  continua íntegro. Sem o escape — quase sempre — a coluna recebe um
  sub-slice do JLH, sem cópia.
- **As CHECKs são contrato:** um `webhook` tem `level` 9, `from` UUIDv7 e
  payload objeto; nenhuma CHECK recusa o que este servidor produz.
- **Só o índice da PK** (regra da skill); consultas por host ou tempo vão para o
  armazenamento definitivo, não para a fila.

O que o servidor **exige** da tabela (§8.5.5): as dez colunas que ele escreve
com tipos compatíveis (`event_at` com precisão 6 ou sem modificador); um índice
**único** exatamente sobre `(uuid)` e **nenhum outro** índice único nem
constraint de exclusão (a regra "só o índice da PK" da skill passa a ser
conferida); `INSERT`
para o papel, e `SELECT` na coluna `uuid` (o `ON CONFLICT (uuid)` a lê); banco em
UTF8; tabela não particionada; `space` e `type` com ao menos 64 caracteres e
`sid` `bigint`. `received_at` e as CHECKs não são conferidas.


### 3.7 Tabela `inbox` (`sink=sqlite` e `sink=mysql`) — retirada

### 3.8 Mensagem NATS (`sink=nats`) — retirada

### 3.9 Tabela `inbox` (`sink=clickhouse`) — retirada

Os três destinos não existem neste servidor (§0); o espelho da `loghub.inbox`
neles continua documentado no `go-webhook-fast-server`.

---

## 4. Superfície HTTP pública

### 4.1 Protocolos

`http.Server` do stdlib com:

```go
var p http.Protocols
p.SetHTTP1(true)             // cobre HTTP/1.0 e HTTP/1.1
p.SetUnencryptedHTTP2(true)  // h2c por prior-knowledge
srv.Protocols = &p
srv.HTTP2 = &http.HTTP2Config{ MaxConcurrentStreams: cfg.HTTP2MaxConcurrentStreams }
```

**Sem `golang.org/x/net/http2/h2c`.** O stdlib do Go 1.24+ detecta o preâmbulo
`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` na mesma porta e serve h2c nativamente.

`Upgrade: h2c` **não é suportado** — o mecanismo foi removido pela RFC 9113 e
não é implementado pelo stdlib. Requisições com esse header são servidas como
HTTP/1.1 normal. Traefik (`h2c: true`), nginx e Envoy usam prior-knowledge por
padrão, então o caso real está coberto.

### 4.2 Métodos e paths

Quase toda requisição vira evento: qualquer path e qualquer método de letras
maiúsculas (`POST`, `GET`, `PUT`, `DELETE`, `OPTIONS`, `HEAD`, `TRACE`,
`PROPFIND`, métodos inventados). Não há roteamento nem `404`.

Exceções, recusadas antes do envelope (sem UUID):

- **`CONNECT` → `405`.** Aceitá-lo faria o servidor ser catalogado como proxy
  aberto por scanners, atraindo varredura permanente.
- **Método fora de `^[A-Z]+$` → `405`.** O schema do `webhook` exige
  maiúsculas, e converter (`ToUpper`) mentiria sobre o que chegou.
- **`OPTIONS *` (alvo sem `/`) → `400`.** O schema exige `path` começando com
  `/`, e um `*` não descreve recurso nenhum. O `http.Server` sobe com
  `DisableGeneralOptionsHandler`: sem isso o `net/http` responderia ao
  `OPTIONS *` sozinho, `200` sem corpo, antes do handler.

`HEAD` entra no ring normalmente; a resposta não carrega corpo (o `net/http`
suprime automaticamente).


### 4.3 Pipeline de uma requisição

Ordem estrita. A primeira condição que falha encerra o processamento. A
numeração dos passos é a do fast; o passo 6 era o rate-limit.

```
1.  accept                     -> teto server.max_conns             (se >0)
2.  leitura de headers         -> read_header_timeout, max_header_bytes -> 431
3.  URI                        -> max_uri_bytes                      -> 414
4.  método CONNECT ou fora de ^[A-Z]+$                               -> 405
    alvo sem "/" (OPTIONS *)                                         -> 400
5.  resolve IP de origem       (§5)
6.  (retirado: rate-limit, §6)
7.  Content-Length declarado > max_body_bytes                        -> 413
8.  sink DOWN                                                        -> 503
9.  ring saturado (checagem rápida, otimização)                      -> 429
10. UMA amostra de relógio t: uuid = GenerateAt(Level3, t), event = t
11. lê corpo com http.MaxBytesReader (read_timeout)                  -> 413
12. pré-passada no corpo (utf-8 ou base64, §3.2); serializa o JLH
    UMA vez num buffer do pool; o buffer do corpo volta ao pool
13. entrega ao ring, não-bloqueante                                  -> 429
14. (o ring de memória aceita no próprio passo 13, §2.2)
15. 200 OK, X-LogHub-UUID: <uuid>, corpo JSON (§4.4)
```

**Rejeições nos passos 4–9** (antes de ler o corpo) drenam e descartam até
`server.reject_drain_bytes` (64 KiB) do corpo antes de responder, para que um
cliente HTTP/1.1 que ainda está enviando consiga **ler a resposta** na mesma
conexão. Se o corpo exceder isso, responde com `Connection: close`.

Sem esse dreno, o cliente recebe `ECONNRESET` em vez do `429`, não vê o
`Retry-After` e retenta sem backoff — piorando exatamente a sobrecarga que o
`429` deveria conter.

### 4.4 Respostas

Sucesso (skill `loghub-type-webhook-fast`):

```
HTTP/1.1 200 OK
X-LogHub-UUID: 01a1122b-8b69-73a4-b9d1-074a5fb34be2
Content-Type: application/json

{"uuid":"01a1122b-8b69-73a4-b9d1-074a5fb34be2","mode":"fast","status":"accepted","stage":"ring"}
```

`mode` é sempre `fast`, `status` é `accepted` e `stage` é sempre `ring`: o
envelope estava no ring de memória ao confirmar. As respostas de erro não têm
`stage`: o envelope não está em lugar nenhum. O corpo é montado de um molde
fixo, sem `encoding/json`, e é byte a byte o do `go-webhook-fast-server` com
`ring=memory`.

Depois que o UUID existe (passo 13), as falhas levam o mesmo cabeçalho e um
corpo JSON com `"status":"error"`, para que o cliente correlacione com o log do
servidor; o envelope foi descartado, e um reenvio gera outro UUID. Antes do
passo 10 não há UUID, e o erro é `text/plain; charset=utf-8`, uma linha:

| Código | `reason` da métrica | Quando | `Retry-After` | UUID |
|---|---|---|---|---|
| `400` | `bad_request` | Requisição malformada; alvo sem `/` | — | não |
| `405` | `bad_request` | `CONNECT`; método fora de `^[A-Z]+$` | — | não |
| `413` | `too_large` | Corpo acima de `max_body_bytes` | — | não |
| `414` | `bad_request` | URI acima de `max_uri_bytes` | — | não |
| `429` | `ring_full` | Ring cheio (itens ou bytes), na checagem rápida | `1` | não |
| `429` | `ring_full` | Ring cheio, no `Enqueue` | `1` | sim |
| `503` | `sink_down` | Erro inesperado do `Enqueue` (o ring de memória não o produz; o buffer vai ao GC, §12.3) | `5` | sim |
| `431` | `bad_request` | Headers acima de `max_header_bytes` | — | não |
| `503` | `sink_down` | Sink marcado DOWN (§10.2) | `5` | não |
| `503` | `shutdown` | Servidor em drenagem | `5` | não / sim no `Enqueue` |

O `500` do fast (recusa `logic` com `ring=none`) não existe: com o ring de
memória o cliente já recebeu `200` quando o destino recusa, e a recusa vira
descarte contado (§10.4). O `429` para ring cheio veio da v1 e continua por ora,
como no fast: a skill `loghub-type-webhook-fast` pede `503` com `Retry-After`, e
essa troca entra quando entrar lá (§0).

---

## 5. Resolução do IP de origem

### 5.1 Algoritmo

```
peer := IP da conexão TCP

se peer NÃO está em proxy.trusted:
    src-ip-addr   = peer
    src-ip-source = "tcp"
    se a requisição trouxe X-Forwarded-For ou X-Real-IP:
        whs_clientip_untrusted_forwarded_total++
        WARN amostrado (1 a cada 30s)
    fim

senão:
    se X-Forwarded-For presente e não vazio:
        percorre a lista da DIREITA para a ESQUERDA
        pula toda entrada que também está em proxy.trusted
        primeira entrada não-confiável => src-ip-addr, source = "xff"
        se TODAS forem confiáveis => a mais à esquerda, source = "xff"
    senão se X-Real-IP presente:
        src-ip-addr = X-Real-IP, source = "xri"
    senão:
        src-ip-addr = peer, source = "tcp"
```

`src-ip-version` = `4` se o endereço resolvido for IPv4 (incluindo IPv4-mapped
`::ffff:a.b.c.d`, normalizado para a forma v4), `6` caso contrário.

`peer-addr` e `peer-port` registram **sempre** a ponta TCP real, independente do
que os headers digam. Auditoria sempre tem o dado bruto.

### 5.2 Por que direita→esquerda

É o único algoritmo não-spoofável. O cliente controla o que envia; o proxy
**anexa** o IP observado à direita. Percorrendo da direita, o primeiro valor
fora da lista de confiança é a primeira ponta que o atacante não pôde forjar.

Pegar o `XFF[0]` (leftmost) — o que a maioria dos tutoriais mostra — é
trivialmente spoofável: o cliente manda `X-Forwarded-For: 1.2.3.4`, o proxy
anexa o IP real depois, e o servidor lê o valor forjado. Isso envenena
logs, auditoria e qualquer limite de taxa feito com o dado, de uma vez.

Contagem fixa de hops (`trusted_hops=N`) foi descartada: quebra silenciosamente
quando a topologia muda (adicionar um CDN desloca tudo) e volta a ser spoofável
se `N` estiver alto demais.

**Linhas repetidas.** Nem todo proxy anexa na mesma linha. O `option forwardfor`
do HAProxy e qualquer regra do tipo *add-header* **inserem uma linha nova** de
`X-Forwarded-For` depois da que o cliente mandou. Ler só a primeira linha — o
que `Header.Get` faz — seria caminhar uma lista que o atacante escreveu de
ponta a ponta, com a linha honesta do proxy nunca aberta. Por isso todas as
linhas de `X-Forwarded-For` são **concatenadas com vírgula, na ordem de
chegada**, antes da caminhada (a RFC 9110 §5.3 permite exatamente isso para
campos de lista); com uma linha só o resultado é idêntico. `X-Real-IP` não é
campo de lista: vale a **última** linha, a que o proxy mais próximo acrescentou.

### 5.3 Parsing

- Entradas do XFF são separadas por vírgula, com espaço opcional.
- Linhas repetidas de `X-Forwarded-For` são concatenadas com vírgula na ordem
  de chegada antes de qualquer outra regra; de `X-Real-IP` vale a última linha
  (§5.2).
- Entradas inválidas (não-IP, `unknown`, `_secret` obfuscado da RFC 7239) são
  **puladas** como se não estivessem na lista, nunca causando erro.
- Colchetes de IPv6 (`[2001:db8::1]:443`) e porta anexada são removidos.
- Limite de `proxy.max_xff_entries` (padrão 32) entradas processadas; o excedente
  à esquerda é ignorado. Impede que um XFF com 100k entradas vire CPU.

Este parser é o **único** código que processa entrada hostil estruturada.
Cobertura por fuzz é obrigatória (§15.2).

### 5.4 Configuração padrão

`proxy.trusted = "127.0.0.0/8,::1"` — apenas loopback.

Falha fechada: um deploy exposto direto à internet nunca confia em header
nenhum. A consequência é que **todo deploy real atrás de proxy precisa
configurar** `proxy.trusted`, e enquanto não configurar, o contador
`whs_clientip_untrusted_forwarded_total` sobe e o WARN aponta o IP exato a
adicionar.

Redes RFC1918 **não** entram no padrão: se o serviço um dia for exposto num host
com IP privado alcançável (VPN, rede compartilhada, peering de cloud), o
spoofing voltaria sem aviso.


---

## 6. Rate-limit — retirado

Decisão 1 (2026-10-07): o rate-limit sai junto com os outros recursos. Ele vinha
desligado por padrão no fast (§6.1 de lá: com `proxy.trusted` mal configurado,
todo o tráfego resolve para o IP do proxy e o limitador vira um auto-DoS), e o
lugar natural dele é o proxy reverso, que vê o IP real antes de qualquer header
e já tem limitador. `ratelimit.enabled` continua aceito com `false` (§0); o
`WARN` de "exposto sem proteção de taxa" não existe mais, porque não há proteção
a ligar.

---

## 7. Ring

### 7.1 Interface

```go
type Ring interface {
    // Enqueue aceita o item. Não bloqueia por capacidade: se estiver cheio,
    // retorna ErrFull imediatamente.
    Enqueue(ctx context.Context, it *Item) error
    // Dequeue entrega até n itens aos workers de dreno.
    Dequeue(ctx context.Context, n int) ([]*Item, error)
    // Commit informa que os itens foram confirmados no sink.
    Commit(items []*Item) error
    // Drain devolve todos os itens pendentes (usado pelo dump de emergência).
    Drain() []*Item
    Stats() (items, bytes, capItems, capBytes int64)
    Close() error
}
```

`Enqueue` só retorna `nil` com o item no canal (§2.2). O `200` é enviado depois
desse retorno. A interface fica, com uma implementação, pela decisão 4 (§2.1).

### 7.2 Saturação

Dois tetos, avaliados simultaneamente:

- `ring.capacity_items` (padrão 65536)
- `ring.max_bytes` (padrão 512 MiB), contando o JLH inteiro (corpo incluído)

`429 ring_full` dispara quando **qualquer um** estourar. `ring.high_water`
(padrão 0.80) é avaliado nos dois e controla `/readyz`.

Os dois são necessários: com corpos de 200 bytes o teto de itens é quem manda;
com corpos de 5 MiB, o de bytes. Só o de itens permitiria 65536 × 5 MiB = 320
GiB de exposição teórica; só o de bytes permitiria 2,6 milhões de itens
pequenos, com profundidade e latência de dreno absurdas e um shutdown que nunca
termina dentro do `drain_timeout`.

### 7.3 `ring.mode = memory`

Canal buffered de `capacity_items`, com um contador atômico de bytes.

`Enqueue` retorna assim que o item entra no canal.

**Garantia:** o evento está na RAM **deste nó**. Perda sob crash não-gracioso
(`kill -9`, OOM, pane do kernel). Sob `SIGTERM` a perda é evitada pelo dreno do
shutdown (§10.3); sob falha fatal do sink, pelo arquivo de emergência (§9).

### 7.4 `ring.mode = sqlite` — retirado

### 7.5 `ring.mode = none` — retirado

Os dois não existem neste servidor (§0). Quem precisa de zero perda sob
`kill -9` usa o `go-webhook-fast-server` com `ring=sqlite` ou `ring=none`.

---

## 8. Sink

### 8.1 Interface

```go
type Sink interface {
    // Write entrega o lote. Retorna erro classificado (ErrNetwork, ErrLogic,
    // ErrFatal) para a máquina de estados de §10.2 decidir.
    Write(ctx context.Context, items []*Item) error
    // Health é chamada periodicamente; define UP/DOWN.
    Health(ctx context.Context) error
    Close() error
}
```

Os workers de dreno (`sink.workers`, padrão 4, §8.5.3):

1. `Dequeue` até `sink.batch` (256) itens, esperando no máximo
   `sink.batch_max_delay` (5ms) para completar o lote.
2. `sink.Write(ctx, items)`.
3. Em sucesso: `ring.Commit(items)`, devolve os buffers ao pool,
   `whs_events_written_total += n`.
4. Num `logic`: o lote inteiro é perda, contada em
   `whs_events_dropped_total{reason="sink_logic"}` e num `ERROR` com os `uuid`,
   e sai do ring do mesmo jeito — `ring.Commit(items)` **antes** de devolver os
   buffers (§12.3).

### 8.2 `sink.type = redis` — retirado

### 8.3 `sink.type = sqlite` — retirado

### 8.4 `sink.type = none` — retirado

Não existem neste servidor (§0). O `sink=none` do fast era a ferramenta de
medição do custo do pipeline sem I/O; a medição equivalente aqui é a do fast,
que tem o mesmo pipeline.

### 8.5 `sink.type = postgres`

Destino final num PostgreSQL **compartilhado por todas as instâncias**. Esquema
em §3.6. O destino é **remoto e com esquema**, e o servidor não é dono de nada
dele; quase tudo que segue decorre disso.

#### 8.5.1 O servidor valida o esquema; nunca o cria

Nenhuma DDL sai deste processo. Motivos, em ordem de peso:

1. **Corrida entre instâncias.** N instâncias sobem contra a mesma tabela.
   `CREATE TABLE IF NOT EXISTS` não é atômico sob concorrência no PostgreSQL e
   pode falhar com violação de unicidade no catálogo. Serializar com advisory
   lock resolveria a corrida ao custo de o serviço carregar DDL, um lock e uma
   política de migração.
2. **Privilégio.** Política comum de produção dá ao papel da aplicação `INSERT`
   e nada mais. Um serviço que exige DDL não roda nesse ambiente.
3. **Evolução.** Quem migra o esquema entre versões do servidor, com instâncias
   de versões diferentes escrevendo ao mesmo tempo, não é o próprio servidor.

A fila versionada vive em `docker/postgres/inbox.sql`, a DDL literal da skill. O
boot **verifica** (§8.5.5) e sai com código 3 se algo faltar. O papel precisa de
`INSERT` na tabela, de `SELECT` na coluna `uuid` (o `ON CONFLICT (uuid)` a lê) e
de leitura do catálogo, que é pública.

#### 8.5.2 Cliente

`github.com/jackc/pgx/v5` com `pgxpool`. Go puro, sem CGO; justificativa da
dependência em §14.3. Conexão **direta** ao servidor. Um pooler externo em modo
*transaction* (PgBouncer) não é suportado na v1: o pgx usa *prepared statements*
nomeados por padrão e o modo transaction os quebra; ver §18.

O pool nasce **preguiçoso** — nenhuma conexão é aberta até o primeiro
`Health()`. É o `WaitReady` do boot (§10.1) quem retenta, com a mesma
disciplina de backoff dos outros destinos.

#### 8.5.3 Concorrência e conexões

Cada worker ocupa uma conexão durante a escrita. Conexão é recurso escasso e
**compartilhado entre instâncias**:

| Grandeza | Valor |
|---|---|
| Workers por instância, padrão do `go-webhook-fast-server` para os outros destinos | 8 |
| `max_connections` padrão do PostgreSQL | 100 |
| Instâncias que esgotam isso com 8 workers cada | 12 |

Por isso:

- **`sink.workers` tem padrão 4.** No `go-webhook-fast-server` o padrão é 8, e
  `sink=postgres` o rebaixa para 4 quando não configurado; aqui não há outro
  destino, e o padrão já é o valor certo. Quem escreve o número manda, com
  `WARN` se passar de 16.
- **`sink.postgres.pool_size`**, padrão `0` = `sink.workers + 1`: uma conexão por
  worker mais uma para o `Health()`, que corre em paralelo aos workers e não
  pode esperar por uma conexão sem estourar `sink.health_timeout`.
- A ocupação do pool é exposta em métrica (§8.5.10).

#### 8.5.4 Escrita

Um comando por lote na fila `loghub.inbox` (§3.6), com **arrays e `unnest`**:
dez parâmetros fixos, qualquer que seja o tamanho do lote. É o "INSERT com
várias linhas" que a skill pede para volume alto. A forma `VALUES (...), (...)`
foi rejeitada porque gera um texto de comando por tamanho de lote (um plano por
tamanho no cache) e esbarra no teto de 65 535 parâmetros do protocolo estendido.

```sql
INSERT INTO loghub.inbox
  (uuid, from_uuid, space, type, level, sid, step, event_at, event_nsec, payload)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::text[], $4::text[],
                     $5::smallint[], $6::bigint[], $7::integer[],
                     $8::timestamptz[], $9::smallint[], $10::jsonb[])
ON CONFLICT (uuid) DO NOTHING
```

Os valores vêm do cabeçalho do próprio JLH (`envelope.ParseHead`, §12.1); o
`payload` é um sub-slice do JLH, sem cópia, exceto quando há um escape
`\u0000` a trocar (§3.6). Um JLH que o leitor recusa é `logic`.

`ON CONFLICT (uuid) DO NOTHING` é a idempotência da fila: o pipeline é
at-least-once por desenho, e uma redelivery não pode derrubar o lote; um `uuid`
repetido dentro do mesmo lote também é absorvido (um `DO UPDATE` falharia). O
número de linhas afetadas diz quantas foram ignoradas;
`whs_sink_postgres_duplicates_total` acumula essa diferença.

O protocolo `COPY` é proibido pela skill na fila: um `uuid` repetido derruba o
`COPY` inteiro (só com tabela de *staging*).

**Os dez slices vêm de um pool por worker**, na disciplina de §12. Eles não são
comandos reciclados pela biblioteca: um retry pode reusá-los sem risco. A armadilha é outra, e é prima da de §12.3: o slice de
`payload` **segura referências** para o buffer do JLH do item. Ao voltar ao pool
os slices são **zerados**; um slice devolvido cheio prenderia buffers que já
voltaram ao pool, e o lote seguinte enxergaria o envelope de outro cliente.

**Teto de bytes.** O lote é limitado por contagem, nunca por bytes. Um
parâmetro do protocolo não passa de 1 GiB, e o pior caso teórico (o maior
envelope de §3.2 × `sink.batch`) excede isso com os padrões. O que salva hoje é
`ring.max_bytes`, de outro eixo: um lote nunca carrega mais do que o ring
aceita conter. O servidor **não divide** o lote; avisa no boot quando
`min(maior envelope × batch, ring.max_bytes)` passa de 1 GiB.

#### 8.5.5 Validação no boot

Corre uma vez, na **primeira checagem de saúde bem-sucedida**, antes de a porta
pública abrir. Cada item que falha é `ErrFatal`, e o `WaitReady` de §10.1
**desiste imediatamente** diante de um erro fatal em vez de retentar até
`sink.boot_timeout`: uma tabela ausente ou uma senha errada não se resolvem em
trezentos segundos.

| Verificação | Como | Se falhar |
|---|---|---|
| A tabela existe e o papel a enxerga | `to_regclass(tabela)` | exit 3 |
| As dez colunas que o servidor escreve existem com tipos compatíveis (`uuid` em `uuid` e `from_uuid`; `bigint` em `sid`; inteiros em `level`, `step`, `event_nsec`; `text` ou `varchar` de ao menos 64 em `space` e `type`; `timestamp with time zone` em `event_at`, sem modificador ou `timestamptz(6)`; `jsonb` em `payload`); extras toleradas | `pg_attribute` + `format_type(tipo, NULL)` + `atttypmod` | exit 3 — um `sid` menor ou um `varchar` curto recusariam toda linha (`22003`, `22001`) com o sink UP; um `timestamptz(p)` com p < 6 arredonda o microssegundo em silêncio. O tipo é comparado sem o modificador: cortar o texto no `(` fazia de `timestamptz(6)` um `timestamp`, recusado com a mensagem errada |
| Nenhuma coluna **extra** obrigatória (`NOT NULL` sem default, não identidade, não gerada); `received_at` passa pelo `DEFAULT now()` | `pg_attribute` | exit 3 — sem isso toda escrita seria `23502`, `logic`, um lote descartado a cada poucos ms com o sink UP |
| Índice único exatamente sobre `(uuid)`, sem predicado nem expressão, imediato e válido | `pg_index` (`indimmediate`, `indisvalid`) | exit 3 — sem ele o `ON CONFLICT (uuid)` falha em **toda** escrita; um único sobre `(uuid, step)` não serve; um `DEFERRABLE` dá `55000` retentado para sempre |
| Nenhum outro índice único nem constraint de exclusão além do árbitro (único, exatamente `(uuid)`, sem predicado nem expressão, imediato); índice ainda não `indisready` é ignorado, inválido mas pronto conta | `pg_index` (`indisunique`, `indisready`, `indimmediate`), `pg_constraint` (`contype = 'x'`) | exit 3 — o `ON CONFLICT (uuid)` absorve colisão só no árbitro: um evento novo que colide noutro índice único derruba o `INSERT` multilinha inteiro com `23505` (`23P01` na exclusão), classe 23, um lote descartado a cada colisão com o sink UP; um único `DEFERRABLE` sobre `uuid` ao lado da PK é pior — a inferência o escolhe e toda escrita falha com `55000`, retentada para sempre |
| `INSERT` e `SELECT (uuid)` concedidos | `has_table_privilege`, `has_column_privilege` | exit 3 — o `ON CONFLICT (uuid)` lê a coluna árbitra: só com `INSERT` o primeiro lote falharia com `42501` (`GRANT INSERT, SELECT (uuid)`) |
| Banco `UTF8` | `current_setting('server_encoding')` | exit 3 — um `LATIN1` guardaria o texto em dupla codificação, sem erro; a sessão fixa `client_encoding = UTF8` |
| Tabela comum, não particionada | `pg_class.relkind` | exit 3 — uma linha sem partição seria `23514` em todo lote (e em tempo de execução um `23514` sem constraint nomeada é `fatal`) |
| Sessão gravável | `transaction_read_only = off` | exit 3 — o mesmo estado que em tempo de execução é fatal (§8.5.7) |

As CHECKs da skill não são conferidas: o que o servidor produz satisfaz todas
(§3.6).

A validação inteira roda com prazo próprio, `sink.op_timeout`, e não o
`sink.health_timeout` (1 s) da checagem que a dispara: são uma dúzia de idas e
voltas, e com 60 ms de RTT nenhuma tentativa cabia em um segundo. O prazo parte
de um contexto que **não** herda o prazo da checagem (`context.WithoutCancel`
mais `sink.op_timeout`), ou o segundo de `health_timeout` continuaria mandando;
o cancelamento do boot continua chegando por ele.

No `go-webhook-fast-server` (e no `go-webhook-server`) este parágrafo existia,
mas o código rodava a validação no prazo da checagem, e o texto seguia com
`max_execution_time`, `452` e *materialized views*, que são do ClickHouse
(§8.8.5 de lá). Corrigido aqui em 2026-10-07 e portado ao F e ao W no mesmo
dia (onde a validação do MySQL tinha o mesmo defeito e a do ClickHouse não
seguia o cancelamento do boot).

A validação **não se repete**. Uma tabela dropada depois do boot aparece como
`42P01` na escrita, que é fatal (§8.5.7) — se caísse na classe `logic`, cada lote
seria descartado com log de erro e o serviço seguiria aceitando webhooks para
lugar nenhum, indefinidamente.

#### 8.5.6 Durabilidade: o servidor não toca no `synchronous_commit`

O serviço **não lê nem altera** o modo de commit. Com `synchronous_commit=on`
(padrão do PostgreSQL) o `INSERT` só retorna após o `fsync` do WAL e o `200`
significa o que §16.1 promete. Com `off`, o servidor confirma antes do `fsync` e
um crash **do banco** perde até `3 × wal_writer_delay` de eventos já respondidos
com `200`.

É a **única linha de §16.1 cuja durabilidade depende de configuração fora do
processo**. Fica explícito lá e aqui, e a decisão foi tomada de olhos abertos:
forçar `SET synchronous_commit = on` por sessão tiraria do administrador do banco
uma escolha que ele pode ter feito de propósito, e ler o valor para avisar
criaria um aviso que quase ninguém veria. A linha honesta é: **o `200` significa
commit confirmado pelo servidor, com a durabilidade que o servidor der.**

#### 8.5.7 Classificação de erro

O PostgreSQL entrega `SQLSTATE` estruturado (`pgconn.PgError.Code`), o que
dispensa classificar por substring de mensagem. A tabela abaixo
**é** a função `classifyPostgres`; qualquer mudança passa primeiro por aqui.

| SQLSTATE | Situação | Classe | Efeito |
|---|---|---|---|
| *(sem SQLSTATE)* | recusa de conexão, EOF, deadline, pool fechado | `ErrNetwork` | retry; máquina de estados de §10.2 |
| classe `08` | conexão perdida no meio do comando | `ErrNetwork` | idem |
| `57P01` `57P02` `57P03` | servidor encerrando ou em recuperação | `ErrNetwork` | idem |
| `53300` | `too_many_connections` | `ErrNetwork` | idem |
| `40001` `40P01` `55P03` | serialização, deadlock, lock indisponível | `ErrNetwork` | idem |
| `57014` | `query_canceled` (`statement_timeout`) | `ErrNetwork` | idem |
| **`25006`** | **transação somente leitura**: réplica promovida, ou servidor configurado assim | **`ErrFatal`** | FATAL imediato — ver §8.5.8 |
| `53100` `53200` `53400` | disco cheio, sem memória, limite de configuração | `ErrFatal` | FATAL — o servidor está vivo e não aceita escrita |
| `28000` `28P01` `3D000` `42501` | credencial, banco inexistente, sem permissão | `ErrFatal` | FATAL — não se resolve sozinho |
| `42P01` `42703` `42P10` `42804` e o resto da classe `42` | tabela ou coluna sumiu, índice único ausente, contrato de esquema quebrado | `ErrFatal` | FATAL — `logic` descartaria um lote a cada 5 ms para sempre |
| `57P04` | `database_dropped` | `ErrFatal` | FATAL |
| classe `XX` | corrupção interna | `ErrFatal` | FATAL |
| classe `22` | dado recusado: codificação, tamanho | `ErrLogic` | log `ERROR`, lote descartado e contado em `sink_logic` (o cliente já recebeu `200`) |
| classe `23` | `NOT NULL`, `CHECK`, unicidade em **outro** índice (que o boot recusa, §8.5.5) | `ErrLogic` | idem — `23505` sobre `uuid` é impossível com o `ON CONFLICT` |
| classe `54` | limite do programa: lote grande demais | `ErrLogic` | idem — é o caso que o aviso de §8.5.4 existe para evitar |
| qualquer outro | desconhecido | `ErrNetwork` | retry: encher o ring é visível, descartar não |

A escolha de **fatal** para `25006` é a mais dura das possíveis e foi
deliberada: um failover de poucos segundos derruba todas as instâncias com
código 3, o orquestrador as reinicia, e cada uma **trava na porta de boot** até
um primário responder (§10.1), sem laço de reinicialização. Com `ring=memory`
isso faz o dump de emergência (§9) ser requisito, não opção: o failover é um
evento esperado que percorre o caminho de emergência.

#### 8.5.8 `Health()`

`SELECT pg_is_in_recovery()`. Barato, sem escrita, sem WAL, sem vacuum.
Responder `true` é falha: o destino está vivo e não é gravável, e o estado vai
para DOWN dentro de um `sink.health_interval`. É o que uma consulta trivial
(`SELECT 1`) não veria — ela responde normalmente numa réplica, o estado nunca
viraria DOWN, o fusível de §10.2 nunca dispararia e o serviço ficaria preso em
`429` sem escalar para FATAL nem despejar o ring.

O que o check **não** vê — permissão revogada, servidor posto em somente leitura
por configuração, tabela dropada — chega como erro de escrita, e por isso os
três são fatais em §8.5.7: fatal é a única classe que os workers escalam sem
esperar pelo check.

O check também atualiza as métricas de ocupação do pool (§8.5.10). Fazê-lo em
`Write()` custaria um lock do pool por escrita, no caminho quente; um valor com
até um `health_interval` de atraso é o preço.

#### 8.5.9 Retenção

A `loghub.inbox` é uma fila: quem a esvazia é o consumidor (§3.6). O servidor
**não apaga nada** e não particiona; se o consumidor parar, a fila cresce, e o
alerta vem do atraso `now() - received_at`, lido no próprio banco.

Particionar na v1 foi recusado por um motivo só: alguém precisa criar a partição
do período seguinte antes de ele chegar, e um `INSERT` sem partição falha com um
erro da classe `23`, que é `logic` — o caminho que descarta. Ver §18.

#### 8.5.10 Métricas

```
whs_sink_postgres_duplicates_total          counter  linhas ignoradas pelo ON CONFLICT
whs_sink_postgres_pool_conns{state}         gauge    state: acquired|idle|max
whs_sink_postgres_pool_waits_total          counter  aquisições que esperaram por conexão
```

Cardinalidade fixa (§11.1). `whs_sink_errors_total{kind}` segue com os quatro
valores de §11.1; a distinção por `SQLSTATE` fica no log.

#### 8.5.11 TLS

`sink.postgres.tls = false` por padrão: o cenário base é rede interna. Com `tls = true` o certificado é **verificado** contra o pool do
sistema ou contra `sink.postgres.tls_ca`, com `ServerName` igual ao host
configurado; `tls_insecure` desliga a verificação. Um `tls_ca` sem `tls` é erro de
configuração (exit 1), não aviso: quem aponta uma autoridade acredita que o TLS
está ligado.


### 8.6 `sink.type = mysql` — retirado

### 8.7 `sink.type = nats` — retirado

### 8.8 `sink.type = clickhouse` — retirado

Não existem neste servidor (§0).

---

## 9. Arquivo de emergência

**Propriedade do ring, não do sink.** Dispara em estado **FATAL** do sink ou com
`shutdown.drain_timeout` estourado, com o ring (ou um lote retido por um worker,
§10.4) não-vazio. Com `emergency.enabled = false` não há dump, e o que sobrou é
perda contada (§9.3).

### 9.1 Formato: JSON Lines

`<emergency.dir>/emg-<agent_uuid>-<unixnano>.jsonl`

```
<JLH>\n
<JLH>\n
...
```

Um envelope por linha, cada linha terminada por um `\n` (`0x0A`), sem mais nada:
o arquivo é [JSON Lines](https://jsonlines.org/) e qualquer ferramenta de linha
(`jq -c`, `wc -l`, `grep`, `psql \copy`) o lê. O JLH já é um documento JSON numa
linha só: o serializador (§12.1) não emite espaço entre tokens, e todo byte de
controle dentro de uma string — `\n` inclusive — sai escapado. Os bytes de cada
linha são **exatamente** os que o cliente viu confirmados: a reingestão entrega o
mesmo `uuid`, e a fila deduplica por ele (§3.6).

**Escrito sob um nome temporário** (correção da revisão do
`go-syslog-nats-server`, portada do `go-webhook-nats-server` em 2026-10-11):
`.emg-<agent_uuid>-<unixnano>.jsonl.tmp`, oculto e fora do padrão que a
reingestão lista, com `O_CREATE|O_EXCL`; depois `fsync` do arquivo, `link` para
o nome final (também exclusivo: um nome que já existe é erro, nunca
sobrescrita), remoção do temporário e `fsync` do diretório. Qualquer falha antes
do nome final remove o que foi escrito e é perda contada (§9.3). Escrito direto
no nome final, como era até então, um processo morto no meio da escrita
(`kill -9`, OOM, queda de energia antes do `fsync`) deixava um arquivo que,
cortado numa fronteira de linha, **lia limpo** — e era removido depois de
entregue em parte, o resto perdido sem contagem.
**Sem *hard link*** (vfat, exFAT, montagens FUSE de *object store*), o `link`
falha qualquer que seja o nome; então, quando o `link` falha por outro motivo
que não "o nome existe", o nome final é conferido livre e o temporário é
**renomeado** para ele (o carimbo de nanossegundos no nome é o que torna
inofensiva a janela entre os dois). Um nome final ocupado continua recusado,
nos dois caminhos.
Um temporário que sobrou de um crash nunca é lido pela reingestão. No boot
(§10.1 passo 4), um temporário que é o **mesmo arquivo** que o dump do nome
final é o gêmeo de um crash entre o `link` e a remoção, e é removido (o dump é o
que a reingestão lê); qualquer outro é um dump interrompido: fica no disco,
nunca é lido, e o boot o informa num `WARN` com os caminhos. Os eventos dele só
foram contados como perda se um segundo sinal abortou o dump (§10.3); depois de
um `SIGKILL` (o `stop_grace_period` estourado), de um OOM ou de uma queda de
energia, ninguém os contou, e o arquivo pode estar inteiro. Para reingeri-lo, o
operador o renomeia para o nome final (sem o ponto inicial e sem o `.tmp`): as
linhas íntegras são entregues no boot seguinte, uma última linha cortada vai
para a quarentena (§9.2), e as já entregues são duplicatas que o
`ON CONFLICT (uuid)` absorve. Um temporário cortado numa fronteira de linha lê
limpo — o que faltar não aparece em lugar nenhum —, e por isso a decisão é do
operador, não da reingestão.
`O_EXCL` porque um dump nunca acrescenta a outro: dois desligamentos no mesmo
nanossegundo de outra instância não podem misturar arquivos. O `fsync` do
diretório é melhor esforço: um sistema de arquivos que o recusa, ou um diretório
que se escreve mas não se lê (modo `0300`, que nem abre para o `fsync`), é
tolerado — o arquivo já está completo, sincronizado e no nome final, e
descartá-lo por isso perderia o dump (antes, a falha ao abrir o diretório
removia o dump pronto).

**Defesa do enquadramento.** Um JLH com um `0x0A` cru partiria a linha em duas,
ambas inválidas. O serializador não o produz (`FuzzEnvelope` e `FuzzServeHTTP`
conferem que nenhum envelope tem `0x0A`), mas o escritor do dump confere de
novo, item a item: um envelope com `0x0A` fica **fora** do arquivo e é contado
como perda (`emergency_failed`, §9.3), e os demais são gravados. Gravá-lo
estragaria a linha e a seguinte; recusar o dump inteiro perderia todos.

**Por que mudou** (decisão 2): o formato do fast era um cabeçalho decimal com o
comprimento e os bytes crus em seguida — compacto, mas opaco para qualquer
ferramenta e frágil, porque um comprimento corrompido tira o leitor de
sincronia e tudo que vem depois vira resto não lido. Em JSON Lines o `\n` é o
enquadramento: uma linha danificada não contamina a seguinte (§9.2), e o
operador inspeciona e reenvia um dump com as ferramentas que já usa.

### 9.2 Reingestão

No boot, todo arquivo `emg-*.jsonl` de `emergency.dir` é entregue **direto ao
sink**, em lotes de `sink.batch`, sem passar pelo ring — que ainda não serve e
não deve encher com o backlog que existe para absorver —, **antes** de a porta
pública abrir (§10.1), em ordem do sufixo `unixnano` (temporal), e **removido só
após todos os seus itens serem confirmados no sink**.

Cada linha é validada como um registro **que este servidor escreveria**: o
cabeçalho do JLH conferido pelo mesmo leitor estrito que o destino usa
(`envelope.ParseHead`, §12.1); o JSON inteiro validado (`json.Valid`: o leitor
só varre as chaves do payload, e `{]` passaria); UTF-8 válido; e só os escapes
`\u` que o serializador escreve — `\u00XX` e `\u2028`/`\u2029` (§12.1). As
duas últimas regras vieram da revisão independente de 2026-10-07: o
`json.Valid` não confere UTF-8 nem surrogates, e uma linha com um byte `0xFF`
num texto ou um `\ud800` passava por ele e era recusada pelo `jsonb` (`22021`,
`22P02`, classe 22) — e a reingestão, que devolvia o erro, fazia **todo** boot
sair com 3, sem entregar mais nada. Uma linha é **danificada** quando:

- falha uma dessas validações (uma linha com `\r\n` no fim, de um editor de
  Windows, também: o JLH não termina em `\r`);
- é a última e não termina em `\n` (um dump de antes de 2026-10-11, escrito
  direto no nome final e cortado, ou um temporário renomeado à mão, §9.1);
- passa do teto de uma linha, o maior envelope que a configuração **atual**
  aceita (`envelope.MaxEventBytes`, §3.2) — sem teto, um arquivo corrompido sem
  `\n` viraria uma alocação do tamanho do arquivo. Um dump escrito com um
  `server.max_body_bytes` maior que o atual pode ter linhas acima do teto: elas
  ficam no arquivo (abaixo), e reingeri-las é subir com o limite antigo.

Linha vazia (`\n\n`) é ignorada sem contar como dano. Uma linha danificada é
**pulada**, e a leitura continua na seguinte: o enquadramento por `\n` se
recupera sozinho, e as linhas íntegras depois dela são entregues. Um arquivo com
ao menos uma linha danificada, depois de entregues as íntegras, é **renomeado**
para `.corrupt` em vez de removido, com um `ERROR` contando as linhas e os bytes
pulados: o resto não é legível por este programa, mas apagar evidência de um
crash não é decisão dele. Reingerir o `.corrupt` de novo (renomeando-o de volta)
é seguro: as linhas já entregues são duplicatas que o `ON CONFLICT (uuid)`
absorve (§3.6).

**Linha recusada pelo destino.** Se ainda assim o PostgreSQL recusar um lote da
reingestão como `logic` (§8.5.7), o lote é refeito item a item: os aceitos são
entregues, os recusados ficam no arquivo, contados num `ERROR` com o `uuid`, e o
arquivo vira `.corrupt` como acima. O boot segue. Uma recusa de outra classe
(rede, fatal) mantém o arquivo e encerra o boot com 3, como antes; um `SIGTERM`
no meio da reingestão encerra com 130 e também mantém o arquivo.

**Dumps de outros formatos.** Um `emg-*.bin` — o dump binário do
`go-webhook-fast-server` — **não é lido** (decisão 2): o boot registra um `WARN`
com a contagem, dizendo para reingeri-lo com o fast, e não toca no arquivo.
Nada é perdido nem misturado.

**Só arquivos regulares** são dumps: um FIFO com nome de dump bloqueava o
`open` para sempre, e o boot com ele, sem atender `SIGTERM`; um *symlink* podia
apontar para `/dev/zero`.

### 9.3 Falha do próprio dump

Se o dump falhar (disco cheio, sem permissão, `emergency.enabled = false`): log
`ERROR` com a contagem exata de eventos perdidos,
`whs_events_dropped_total{reason="emergency_failed"} += n` (com
`emergency.enabled = false`, `reason="abort"`), e saída com código 3. Um envelope
deixado fora do arquivo pela defesa do enquadramento (§9.1) conta igual, com o
dump dos demais bem-sucedido.

---

## 10. Ciclo de vida

### 10.1 Boot

```
1. parse da config (§13); erro, chave desconhecida ou valor de seletor
   retirado (§0) => exit 1
   --help, --version e --dump-config encerram aqui, sem tocar o disco
1a. IDENTIDADE (§3.5): cria ring.data_dir, exporta-o como DATADIR e chama
   lhident.Initialize() UMA vez (só no main). Falha => a biblioteca sai
   sozinha com 100..114 (§10.5); from e space são validados (exit 1)
2. abre a porta ADMIN imediatamente
      /healthz -> 200 (processo vivo)
      /readyz  -> 503 com o motivo em texto
      /metrics -> já exportando
3. inicializa o SINK
      pool preguiçoso; a primeira checagem de saúde válida verifica
      tabela, colunas, índice único, INSERT e modo de escrita (§8.5.5).
      Retry com backoff 100ms -> 5s até sink.boot_timeout (300s);
      estourou => exit 3; erro FATAL (esquema, credencial) => exit 3
      IMEDIATO, sem esperar sink.boot_timeout, com o log "the sink
      refused this server permanently" (não "did not become reachable
      within sink.boot_timeout", que mandava o operador olhar a rede)
4. se existem arquivos emg-*.jsonl: entrega direto ao sink (§9.2)
   (um emg-*.bin do fast => WARN, intocado, §9.2; o temporário gêmeo de
   um dump é removido, o de um dump interrompido => WARN, intocado, §9.1)
5. (retirado: o dreno do backlog do ring.db; o ring de memória nasce vazio)
6. abre a porta pública
7. /readyz -> 200
```

A porta admin aberta durante todo o processo é o ponto: mesmo num crashloop por
sink indisponível, o operador tem `/readyz` com o motivo exato e `/metrics` com
o estado do sink. Sem isso, restaria só o log do container.

Com `sink.boot_timeout` estourado, o processo sai com código 3 e o supervisor
reinicia — o crashloop é **intencional** (falha fechada: nada é aceito que não
possa ser persistido) e dispara os alertas de `CrashLoopBackOff` que os times
já têm.

#### Validação de configuração

| Combinação | Efeito |
|---|---|
| `ring.mode` ≠ `memory`, `sink.type` ≠ `postgres`, `ratelimit.enabled = true` | `exit 1`: recurso retirado (§0) |
| Chave de recurso retirado (`[ring.sqlite]`, `ring.max_inflight`, `[sink.redis]`...) num INI ou numa flag | `exit 1`: chave desconhecida (§0); no ambiente, inerte |
| Linha de INI sem `=` ou cabeçalho sem `]`; argumento sem `--` | `exit 1` com o número da linha ou o nome antes do `=`, nunca o valor (`password: x` é exatamente a linha que cai aqui) (§13.2) |
| Flag sem valor seguida de outra flag (`--sink.workers --sink.postgres.password=x`, um `$WORKERS` vazio num script); valor depois de um espaço (`--sink.postgres.password= x`, `--sink.postgres.password = x`, `--sink.postgres.password= -x`); valor que o shell partiu num espaço (`--sink.postgres.password=$P`, com espaço em `$P`); valor que começa com `--` na forma separada (`--sink.postgres.password --x`) | `exit 1`, nunca com o valor: um argumento que começa com `--` (ou `-` com o nome de um parâmetro ou de um *switch*) nunca é tomado como valor de outra flag, e só é nomeado se for uma flag deste servidor; o argumento depois de um valor vazio ou de um `=` sozinho não é citado, nem como nome de flag desconhecida; uma palavra solta depois de qualquer valor não é citada. Um valor que começa com `-` vai depois do `=` (§13.2; revisão do porte de volta ao N, 2026-10-11) |
| `sink.workers` acima de 16 | Permitido, com `WARN` (§8.5.3) |
| `min(maior envelope × sink.batch, ring.max_bytes) > 1 GiB` | Permitido, com `WARN`: um lote pode exceder o teto de um parâmetro do protocolo (§8.5.4) |
| `sink.postgres.tls_ca` sem `tls` | `exit 1` (§8.5.11) |
| `sink.postgres.table` que não é identificador simples (`nome` ou `esquema.nome`, minúsculos) | `exit 1` |
| `GOMEMLIMIT` ausente ou `< ring.max_bytes * 2` | Permitido, com `WARN` (§12.4) |

### 10.2 Saúde do sink

Máquina de estados dirigida **exclusivamente pelo `Health()`** do sink.

```
UP ---- Health() falha (timeout sink.health_timeout) ----> DOWN
DOWN --- Health() sucesso -------------------------------> UP
DOWN --- persiste por sink.fail_timeout (30s)
         OU sink.max_retries (10) checagens falhas ------> FATAL
qualquer --- Write() retorna ErrFatal -------------------> FATAL
qualquer --- Health() retorna ErrFatal ------------------> FATAL
```

- `Health()` a cada `sink.health_interval` (padrão 2s). **Latência de detecção =
  até um intervalo de health check.**
- Uma recusa permanente à própria sonda (`ErrFatal`: senha trocada, banco
  removido) é FATAL na **primeira** checagem, sem esperar os fusíveis: eles
  existem para um destino inalcançável, não para um que respondeu "nunca".
- Erros de `Write()` classificados como `ErrNetwork` **não** mudam o estado: o
  lote continua com o worker e é retentado (§10.4). Ninguém recebeu `200` por um
  evento perdido.
- `ErrLogic` → log `ERROR`, lote descartado e contado
  (`whs_events_dropped_total{reason="sink_logic"}`).
- `ErrFatal` → FATAL imediato.

Em estado **DOWN**, a porta pública responde **`503 sink_down`** a toda
requisição — fail-fast, mesmo com o ring vazio. `/readyz` falha no mesmo
instante.

Em **FATAL**: executa §9 e sai com código 3.

### 10.3 Shutdown gracioso

`SIGTERM` ou `SIGINT`:

```
fase 1  /readyz passa a falhar
fase 2  espera shutdown.pre_stop_delay (padrão 0s)
        AINDA ACEITANDO tráfego normalmente
fase 3  para de aceitar conexões novas
        responde Connection: close nas abertas
        requisições em voo terminam
        novas requisições em conexões abertas -> 503 shutdown
fase 4  drena o ring inteiro para o sink
        shutdown.drain_timeout (padrão 30s) é UM prazo para as fases
        3 e 4 juntas, contado do início da fase 3
fase 5  fecha o sink, exit 0
```

**Segundo sinal** em qualquer fase → pula direto para a saída, aceitando a perda
(contada em `whs_events_dropped_total{reason="abort"}`, inclusive o que já tinha
sido recolhido para um dump ainda não ligado ao nome final), `exit 130`. No shutdown
**por FATAL** nenhum sinal o iniciou: o primeiro que chega — o `SIGTERM` comum de
um orquestrador — é o primeiro, registra um `WARN` e o dump continua; só um
segundo aborta. No fast, esse primeiro sinal jogava o dump fora (revisão
independente de 2026-10-07). Um dump interrompido assim fica no disco só como o
temporário oculto (§9.1), que a reingestão não lê: os eventos dele são a perda
que o aborto contou.
**`SIGQUIT`** durante o shutdown aborta como o segundo sinal; fora dele vale a
ação padrão do Go (despejo das goroutines e saída `2`), que é o que um operador
quer ao mandá-lo para um processo travado. A frase do fast ("sempre aborta
imediatamente") prometia mais do que o código fazia.

`shutdown.pre_stop_delay` existe para Kubernetes: a remoção do endpoint no LB é
assíncrona e o pod continua recebendo tráfego **depois** do `SIGTERM`. Sem a
espera, o emissor recebe conexão recusada — um erro de rede, não um `503`
retentável. Valor sugerido em K8s: 5–10s.

Se o `drain_timeout` estourar com itens ainda no ring, executa §9 e sai com
código 3. **Um prazo, não dois** (correção do `go-webhook-nats-server`, portada
em 2026-10-11): cada fase tinha o seu, e um upload lento (que segura a fase 3
até o prazo) seguido de um sink DOWN (que segura a fase 4) só começava o dump
em duas vezes o `drain_timeout` — além de um `stop_grace_period` dimensionado
para uma, e o `SIGKILL` levava o ring inteiro, respondido com `200`, sem nada
contar. O pior caso até o dump começar é
`pre_stop_delay + drain_timeout + sink.op_timeout` (o lote em voo de um worker),
e o `stop_grace_period` precisa cobri-lo com folga para o próprio dump.

**O ring é selado antes do último `Drain`** (achado pela revisão independente
do porte, 2026-10-11). Um upload que ainda lia o corpo quando a fase 3 estourou
continua depois dela; se o corpo chegasse depois do `Drain`, o item entrava num
ring que ninguém mais lia e o cliente recebia `200` — fora do destino e do
dump, sem contagem, e o processo saía com 0. O prazo único das fases 3 e 4
tirou a folga que antes o pegava. Agora o ring de memória é selado (`Seal`, uma
barreira: o `Enqueue` segura um `RWMutex` compartilhado da checagem ao envio, e
o `Seal` o pega exclusivo) logo antes do `Drain`, e o que chegar depois recebe
`503 shutdown`.

**`SIGHUP`** é ignorado em toda fase, com `WARN` (§0, §13.3).

### 10.4 Falha de lote

O worker **segura o lote no próprio buffer** e retenta com backoff
(`sink.retry_backoff_min` 100ms → `sink.retry_backoff_max` 2s), **sem puxar mais
nada do ring**.

O ring **não é modificado**: ele simplesmente para de escoar e enche, até o `429`
aparecer naturalmente. Consequências:

- Nenhum item é perdido.
- Nenhuma reordenação.
- O ring pode ser um canal simples — não precisa suportar reinserção na cabeça.
- Se o estado virar FATAL, esses itens entram no dump de emergência junto com o
  ring.

Reenfileirar na cauda foi descartado: reordena tudo e, com o sink fora, vira um
loop de CPU girando os mesmos itens.

### 10.5 Códigos de saída

| Código | Significado |
|---|---|
| `0` | Shutdown gracioso completo: ring vazio |
| `1` | Erro de configuração ou falha de inicialização não relacionada ao sink |
| `3` | Sink indisponível (FATAL, `boot_timeout`, ou `drain_timeout` estourado) |
| `100`–`114` | Identidade: o `go-loghub-ident` sai sozinho (`DATADIR` inválido ou sem escrita, `AGENT_UUID` que não é UUIDv7, `HOSTNAME` fora da RFC 1123, `WORKSPACE` com `_`...). A tabela completa está na skill `go-loghub-ident` §8 |
| `130` | Abortado por segundo sinal |

---

## 11. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9101`. Loopback por padrão para não expor à
internet. HTTP/1.1 apenas.

Existe porque a porta pública é **100% opaca** — reservar paths nela criaria um
buraco onde qualquer webhook legítimo que usasse esse path seria engolido pelo
servidor em vez de virar evento.

| Path | Semântica |
|---|---|
| `/healthz` | `200` sempre que o processo está vivo e respondendo. Liveness. |
| `/readyz` | `200` somente se: sink `UP`, ring abaixo de `ring.high_water`, reingestão de boot concluída, e não em shutdown. Caso contrário `503` com o motivo em `text/plain`. |
| `/metrics` | Exposição Prometheus em formato texto. |

### 11.1 Métricas

Regra de cardinalidade: **nenhum label de path, método, IP ou user-agent.** Todo
label tem conjunto de valores finito e conhecido em tempo de compilação. Path vem
de tráfego arbitrário da internet — um scanner com 100k paths distintos
derrubaria o Prometheus, não o servidor.

Os nomes, labels e fronteiras são os do `go-webhook-fast-server` (decisão 3):
um painel feito para o fast lê este servidor sem mudança. O que sai são as
séries dos recursos retirados.

```
whs_requests_total{code}                    counter
whs_rejected_total{reason}                  counter
    reason: ring_full|too_large|sink_down|shutdown|bad_request
whs_request_duration_seconds{outcome}       histogram
    outcome: accepted|rejected
whs_body_bytes                              histogram

whs_ring_items                              gauge
whs_ring_bytes                              gauge
whs_ring_capacity_items                     gauge
whs_ring_capacity_bytes                     gauge

whs_sink_batch_size                         histogram
whs_sink_duration_seconds                   histogram
whs_sink_errors_total{kind}                 counter
    kind: network|logic|fatal|timeout
whs_sink_up                                 gauge (0|1)
whs_sink_health_duration_seconds            histogram
whs_events_written_total                    counter
whs_events_dropped_total{reason}            counter
    reason: sink_logic|emergency_failed|abort

whs_emergency_files                         gauge
whs_emergency_events_pending                gauge

whs_conns_active{proto}                     gauge
    proto: http1|http2
whs_clientip_untrusted_forwarded_total      counter
whs_config_info{ring_mode,sink_type}        gauge (=1)  sempre memory, postgres
whs_build_info{version,commit,go}           gauge (=1)

whs_sink_postgres_duplicates_total          counter     (§8.5.10)
whs_sink_postgres_pool_conns{state}         gauge
    state: acquired|idle|max
whs_sink_postgres_pool_waits_total          counter
```

`kind="timeout"` conta a escrita ou a sonda que estourou o próprio prazo
(`sink.op_timeout`, `sink.health_timeout`), à parte de `network`, que é a
recusa da rede; o tratamento das duas é o mesmo (retry, fusíveis de §10.2).
Uma escrita cortada pelo cancelamento do próprio shutdown não é falha do sink e
não conta. Cada evento perdido conta em **uma** razão de
`whs_events_dropped_total`: o dump que falha conta `emergency_failed`, nunca
também `abort` (§9.3).

Saem `whs_ratelimit_buckets`, `reason="ratelimit"` e as séries de
`sink=sqlite`, `mysql`, `nats` e `clickhouse`. O `code="500"` de
`whs_requests_total` fica, sempre em zero: não há caminho que responda `500`
(§4.4), e a série existe para que um painel do fast continue achando-a.

### 11.2 Fronteiras dos histogramas

#### Regra: fronteiras estáticas

As fronteiras são **idênticas em toda instância**, independentes de
`max_body_bytes`, `sink.batch` ou `sink.op_timeout`. O teto de cada conjunto é
escolhido com folga para cobrir qualquer configuração sã.

Motivo — a armadilha silenciosa:

```promql
histogram_quantile(0.99,
  sum by (le) (rate(whs_request_duration_seconds_bucket[5m])))
```

Se dois nós expõem conjuntos de `le` diferentes, o `sum by (le)` trata cada valor
como uma série independente, o invariante cumulativo do histograma quebra e o
quantil resultante é **lixo, sem nenhum erro visível**. É o pior modo de falha
possível em observabilidade. Fronteiras derivadas da configuração local
introduziriam exatamente isso no dia em que um nó fosse reconfigurado.

Buckets vazios custam um inteiro cada. A portabilidade de dashboards, alertas e
histórico entre reconfigurações vale muito mais.

#### `whs_request_duration_seconds{outcome}` — 16 fronteiras

```
0.00005   50µs
0.0001   100µs
0.00025  250µs
0.0005   500µs
0.001      1ms
0.0025   2.5ms
0.005      5ms
0.010     10ms  ◄ critério de aceite do fast (memory + redis)
0.025     25ms  ◄ critério de aceite do fast (ring=sqlite)
0.050     50ms
0.100    100ms
0.250    250ms
0.500    500ms
1.0        1s
5.0        5s
30.0      30s     teto = server.read_timeout
```

As fronteiras em `0.010` e `0.025` são **exatamente** os critérios de aceite.
Isso torna a verificação do SLO uma divisão de dois contadores, sem interpolação:

```promql
whs_request_duration_seconds_bucket{outcome="accepted",le="0.01"}
  / ignoring(le)
whs_request_duration_seconds_count{outcome="accepted"}
```

Um conjunto exponencial base 2 partindo de 100µs colocaria o alvo de 10ms dentro
do bucket `(6.4ms, 12.8ms]` — um bucket com o dobro da largura do alvo, e a
aferição do critério de aceite passaria a depender de uma estimativa.

O label é `{outcome}` (`accepted` para `200`, `rejected` para todo o resto), não
`{code}`. O `{code}` completo permanece no contador `whs_requests_total`, que é
barato. Separar aceites de rejeições é essencial: sob um ataque que gere 90% de
`429` — que custam microssegundos — uma distribuição única teria a p99 **puxada
para baixo**, escondendo uma degradação real do caminho de aceitação. E a
latência das rejeições é informação própria: ela deve ser de microssegundos, e
se subir, denuncia trabalho sendo feito antes de rejeitar (§4.3).

#### `whs_sink_duration_seconds` — 13 fronteiras

Duração de um `sink.Write()` completo (o lote inteiro).

```
0.0001   100µs
0.00025  250µs
0.0005   500µs
0.001      1ms
0.0025   2.5ms
0.005      5ms
0.010     10ms
0.025     25ms
0.050     50ms
0.100    100ms
0.250    250ms
1.0        1s
5.0        5s     teto = sink.op_timeout padrão
```

Faixa útil do `postgres`: um `INSERT … unnest` de um lote, com o `fsync` do WAL
do servidor, vive em 1–50 ms numa rede local; a região está densamente coberta.

#### `whs_sink_health_duration_seconds` — 8 fronteiras

```
0.0001   100µs
0.00025  250µs
0.0005   500µs
0.001      1ms
0.005      5ms
0.025     25ms
0.100    100ms
1.0        1s     teto = sink.health_timeout padrão
```

#### `whs_body_bytes` — 13 fronteiras

```
0                    corpo vazio (GET, HEAD, DELETE)
64
256
1024        1 KiB    tamanho de referência do benchmark (§15.5)
4096        4 KiB  ◄ classe do sync.Pool (§12.2)
16384      16 KiB  ◄ classe do sync.Pool
65536      64 KiB  ◄ classe do sync.Pool
262144    256 KiB  ◄ classe do sync.Pool / corte de pooling
1048576     1 MiB
2097152     2 MiB
5242880     5 MiB    server.max_body_bytes padrão
16777216   16 MiB    folga para configurações maiores
67108864   64 MiB    folga para configurações maiores
```

Quatro fronteiras coincidem **exatamente** com as classes do pool de buffers
(§12.2). Isso é deliberado e é a razão de o conjunto não ser potências de 2
puras: o histograma passa a informar diretamente a distribuição de acerto do
pool e se o corte de 256 KiB está calibrado para o tráfego real deste deploy —
tuning que nenhum outro conjunto de fronteiras entregaria.

> **Invariante:** `whs_body_bytes_bucket{le="+Inf"}` menos
> `whs_body_bytes_bucket{le="6.7108864e+07"}` deve ser **sempre 0** em qualquer
> configuração com `max_body_bytes ≤ 64 MiB`. Um corpo acima do limite é
> rejeitado com `413` **antes** de ser lido (§4.3), então nunca chega a ser
> observado. Contagem diferente de zero ali é bug no pipeline, não tráfego.

#### `whs_sink_batch_size` — 11 fronteiras

```
1
2
4
8
16
32
64
128
256      sink.batch padrão
1024     folga para configurações maiores
4096     folga para configurações maiores
```

Este é o principal sinal de saturação do sink. Lotes consistentemente **cheios**
significam que o sink é o gargalo; lotes consistentemente em **1** significam
tráfego baixo ou `sink.batch_max_delay` curto demais. A fração de lotes cheios
sai direto de dois contadores:

```promql
1 - (whs_sink_batch_size_bucket{le="128"} / whs_sink_batch_size_count)
```

#### Regras de implementação da exposição

Como a exposição em texto Prometheus é própria (§14.3), estas regras são
obrigatórias e não há biblioteca para garanti-las:

1. **Buckets cumulativos**: `_bucket{le="X"}` conta observações **≤ X**. Emitidos
   em ordem crescente de `le`.
2. **`le="+Inf"` é obrigatório** e seu valor deve ser **idêntico** a `_count`.
   Grafado exatamente `+Inf`.
3. **As strings de `le` são pré-computadas uma vez na inicialização**, a partir
   de uma tabela de literais, e nunca formatadas a cada scrape. O Prometheus
   trata `le` como **string de label**: emitir `0.01` num scrape e `0.010` no
   seguinte cria duas séries distintas e quebra o histograma silenciosamente.
   Esta é a mesma classe de falha da fronteira derivada de config, por um
   caminho diferente.
4. **Observação é O(1)**: busca binária sobre o array de fronteiras e incremento
   atômico em **um** bucket. Os contadores armazenados são **não-cumulativos**; a
   soma cumulativa é feita no momento do scrape. Incrementar todos os buckets
   ≥ valor a cada observação seria O(n) no caminho quente.
5. **Sem float atômico**: `_sum` de duração é acumulado em `atomic.Uint64` de
   **nanossegundos** e dividido por 1e9 na exposição; `_sum` de tamanho acumula
   bytes em `atomic.Uint64` diretamente. Nenhum `CompareAndSwap` sobre float no
   caminho quente.
6. `_sum` e `_count` são obrigatórios em todo histograma.

#### Orçamento de cardinalidade

| Métrica | Séries |
|---|---|
| `whs_request_duration_seconds{outcome}` | 2 × (16 + 1 + 2) = 38 |
| `whs_body_bytes` | 13 + 1 + 2 = 16 |
| `whs_sink_duration_seconds` | 13 + 1 + 2 = 16 |
| `whs_sink_batch_size` | 11 + 1 + 2 = 14 |
| `whs_sink_health_duration_seconds` | 8 + 1 + 2 = 11 |
| Contadores e gauges (todos) | ~39 |
| **Total** | **~134 séries por instância** |

Um `/metrics` de ~134 séries é irrelevante para qualquer Prometheus. A disciplina
de cardinalidade desta spec não existe para economizar essas séries — existe para
que **nenhuma** delas possa crescer com o tráfego (§17, invariante 6).

### 11.3 Logs

`log/slog` do stdlib, saída JSON, nível configurável.

**Por padrão registra apenas**: boot (versão, identidade, arquivo de
configuração e avisos de validação; o dump da configuração efetiva, redigido,
sai só com `log.level = debug`, e `--dump-config` o imprime sob demanda),
mudanças de estado do sink, fases do shutdown, dump e reingestão de emergência,
contagens de descarte, e erros. **Nada por requisição.**

Access log é opt-in: `log.access = true`, com amostragem `log.access_sample`
(padrão 0.01). Rejeições (`429`/`413`/`503`) também são amostradas, nunca logadas
uma a uma.

Log síncrono por requisição destrói qualquer benchmark e, sob ataque, enche o
disco mais rápido que o ring enche a memória.


---

## 12. Memória e runtime

### 12.1 Envelope

O JLH é escrito **uma vez**, por *append*, num buffer do pool, e é o único buffer
que o item carrega (o corpo vai dentro dele, §3.2). O serializador é próprio,
não `encoding/json`: chaves na ordem canônica, sem reflexão, e o escape de
string da v1 com uma diferença — UTF-8 inválido vira **um U+FFFD por subparte
máxima** (§3.2), como no Python, e não um por byte; U+2028/U+2029 escapados.

O destino lê os campos do cabeçalho que vão para as colunas da `inbox` **do
próprio JLH**, com um
leitor estrito do cabeçalho que este servidor escreve (`envelope.ParseHead`),
e não de uma cópia guardada no item: assim um item que volta de um dump é
indistinguível do que entrou. O leitor não aloca além das três
strings curtas (`from`, `space`, `type`) e recusa qualquer JLH que não tenha
exatamente a forma de §3.1 — `uuid` e `from` UUIDv7 em minúsculas inclusive (o
`uuidv7.FromString` aceita maiúsculas, e a regra genérica do `from` do JLH,
qualquer versão de 1 a 8). Do payload ele só confere que é um objeto que fecha o
envelope; a reingestão de um dump (§9.2) valida o resto.

### 12.2 Corpo e buffer do JLH

`sync.Pool` com **classes** de 4 KiB, 16 KiB, 64 KiB e 256 KiB, um conjunto só
de pools para o corpo lido e para o JLH. Acima de 256 KiB o buffer é
alocado fresco com `make` e **nunca entra no pool**: pooling de buffers grandes
faz o pool reter a memória do pior caso para sempre, e o GC do Go já lida bem
com objetos no large object heap.

O corpo vive só entre a leitura e a serialização: a pré-passada (§3.2) diz o
tamanho exato da sua representação, o JLH é pedido ao pool com esse tamanho
mais uma folga para o cabeçalho (`envelope.SizeHint`) e recebe o buffer da
classe que o cobre, e o buffer do corpo volta ao pool assim que o JLH é escrito
— nada mais o referencia. Um JLH que cresceu além da capacidade pedida (só se a
estimativa do cabeçalho falhar) deixa de ser do pool e vai para o GC. O item
segura a classe inteira, e o ring conta só o comprimento do JLH (§19.1).

### 12.3 Regra de propriedade única

> O item do ring é **dono** do buffer do JLH. Ele volta ao pool **apenas**
> depois de o item ser confirmado, e **apenas uma vez**.

Ponto de devolução: o `sink.Write()` do lote retorna sem erro, ou com uma
recusa `logic` (o lote sai do ring como perda contada, §8.1). Nos dois casos o
`ring.Commit` vem **antes** do `Release`: o ring desconta `it.Size()`, que é
`len(Env)`, e o `Release` esvazia o `Env`. Na ordem inversa — a da recusa
`logic` até 2026-10-11, herdada do fast — o ring continuava contando os bytes
de um lote que ninguém mais tinha, até recusar tudo como cheio com nada dentro.

Num retry de lote (§10.4), o buffer **não** é devolvido — o item ainda é o dono.
No dump de emergência (§9) o buffer não volta ao pool: o processo encerra em
seguida.

**Caminhos de erro.** Um `Enqueue` que falha **não** significa que ninguém mais
referencia o item. A regra é por causa do erro:

| Erro do `Enqueue` | Quem ainda pode ler o buffer | O que o handler faz |
|---|---|---|
| `ErrFull`, `ErrClosed` | ninguém: o ring recusou antes de receber o item | devolve ao pool (`Release`) |
| qualquer outro erro (o ring de memória não devolve outro; a regra protege uma implementação futura) | não se sabe | **descarta sem devolver** (`Discard`): o buffer fica para o GC |

Nos workers de dreno vale a regra correspondente: uma tentativa que termina em
erro de contexto (`sink.op_timeout` estourado, shutdown) **contamina** o lote, e
os buffers desses itens não voltam mais ao pool — nem quando a retentativa
seguinte tem sucesso. O pgx devolve o controle no cancelamento enquanto a
conexão ainda é encerrada em segundo plano, e nada prova que ninguém lê mais os
argumentos da tentativa abandonada; devolver o buffer arriscaria corrompê-los. O
custo é uma alocação por item num caminho que já é o de falha; o caminho normal
não muda.

Devolver ao pool um buffer ainda referenciado entrega o corpo do webhook de um
cliente dentro do evento de outro. É o pior bug possível neste sistema, é
silencioso, e não aparece em teste de carga. Todo caminho de erro precisa de
teste explícito com `-race`.

### 12.4 Runtime

- **`GOMAXPROCS`: não mexer.** O Go 1.25+ lê o limite de CPU do cgroup
  automaticamente e reavalia dinamicamente. A dependência `automaxprocs` é
  desnecessária.
- **`GOMEMLIMIT`: não define automaticamente**, mas emite `WARN` no boot se
  estiver ausente ou menor que `ring.max_bytes * 2`. Definir automaticamente
  seria comportamento mágico, e ler cgroups corretamente (v1 vs v2, hierarquias
  aninhadas) é mais sutil do que parece. O compose de `docker/` define `GOMEMLIMIT` **e** o limite de
  memória do contêiner, o segundo acima do primeiro, para que o GC trabalhe
  antes de o kernel matar.
- `TCP_NODELAY` já é o padrão do Go.
- `server.listeners > 1` habilita `SO_REUSEPORT` via `ListenConfig.Control`.
  Padrão `1`: o accept loop do Go raramente é o gargalo, e o ring único já é o
  ponto de contenção real. No macOS/BSD a semântica do `SO_REUSEPORT` difere da
  do Linux, o que atrapalha desenvolvimento local.


---

## 13. Configuração

### 13.1 Precedência

```
argv  >  env  >  arquivo ini  >  padrão
```

- Flag: `--<seção>.<chave>` — ex.: `--ring.capacity_items=131072`
- Env: `WHS_<SEÇÃO>_<CHAVE>` maiúsculo, `.` vira `_` — ex.:
  `WHS_SINK_POSTGRES_ADDR=10.0.0.5:5432`
- INI: seção `[ring]`, chave `capacity_items`
- `--config=/caminho/arquivo.ini` (env `WHS_CONFIG`) indica o arquivo.

O prefixo `WHS_`, as seções e as chaves são os do fast (decisão 3, §0).

### 13.2 Registry único

Uma **única tabela** declara, para cada parâmetro: seção, chave, tipo, valor
padrão, descrição e validação. Dela são derivados:

- o parser de INI,
- o registro de flags,
- a leitura de env,
- o `--help`,
- o dump da configuração efetiva no boot.

É impossível os cinco divergirem. Sem `gopkg.in/ini.v1` e sem `spf13/pflag`:
unir essas bibliotecas numa precedência coerente daria mais código de cola que o
registry inteiro, e sem o `--help` e o dump gerados da mesma fonte. As três
chaves seletoras de §0 estão no registry como qualquer outra; a validação aceita
um valor só.

**Erros sem eco** (porte do `go-webhook-nats-server` e do `go-syslog-nats-server`,
2026-10-11): os parsers de INI e de flags nomeiam a linha ou a flag, nunca o
valor. Uma linha `password: x` (sem `=`), um cabeçalho sem `]`, um
`sink.x.password=y` sem os traços, uma flag sem valor que engoliria a seguinte
(`--sink.workers --sink.x.password=y`, um `$WORKERS` vazio num script: um
argumento que começa com `--` nunca é tomado como valor) e um valor digitado
depois de um espaço (`--sink.x.password= y`) imprimiam o segredo no stderr — no
boot e, onde há recarga, a cada `SIGHUP`.

A revisão do porte de volta ao N (2026-10-11) achou mais três caminhos, iguais
aqui, portados no mesmo dia: um valor que começa com `--` na forma separada
(`--sink.postgres.password --x`) era "nomeado" como a flag `--x`; um valor com
traço depois de um `=` vazio (`--sink.postgres.password= -x`) saía como
parâmetro desconhecido; e a cauda de um valor que o shell partiu num espaço
(`--sink.postgres.password=$P`, com espaço em `$P`) saía como argumento solto.
Agora só se nomeia uma flag que este servidor conhece, nada depois de um valor
vazio é citado, e uma palavra solta depois de qualquer valor também não. Um
valor que começa com `-` vai depois do `=`. Fica uma brecha, de propósito: a
cauda partida que começa com traço depois de um valor **não** vazio
(`--sink.postgres.password=a -b`) sai como parâmetro desconhecido, porque
escondê-la esconderia todo erro de digitação de uma flag
(`--sink.workers=4 --tipo.errado=1`).

### 13.3 Recarga por `SIGHUP` — retirada

Decisão 1 (2026-10-07). A configuração é lida **uma vez**, no boot; mudar
qualquer chave é reiniciar a instância (com `pre_stop_delay` e dreno, §10.3, um
reinício não perde nada). O `SIGHUP` é **capturado e ignorado** com um `WARN`
(`"SIGHUP ignored: this server does not reload its configuration; restart it to
apply changes"`), porque a ação padrão do Go para ele é encerrar o processo sem
dreno. O `main` o ignora antes mesmo de ler a configuração e a identidade; o
`WARN` vale durante todo o `run`, e depois dele o sinal volta a ser só ignorado
até o `os.Exit`. No fast, as chaves recarregáveis eram `log.*`, `ratelimit.*`, `proxy.*`,
`headers.exclude` e `sink.sqlite.keep_files`; aqui todas exigem reinício.

### 13.4 Exemplo completo

```ini
[server]
bind = :8080

[admin]
bind = 127.0.0.1:9101

[proxy]
trusted = 127.0.0.0/8,::1,10.42.0.0/16

[ring]
capacity_items = 65536
max_bytes = 512MiB

[sink]
workers = 4
batch = 256

[sink.postgres]
addr = 10.0.0.5:5432
username = webhook
password = segredo
database = webhook
table = loghub.inbox

[emergency]
enabled = true
dir = ./data/emergency

[log]
level = info
```

Exemplo de **host**: o `./data` relativo é o certo quando o binário roda direto
na máquina. Em contêiner os mesmos caminhos ficam sob `/data` (§13.6).

### 13.5 Tabela de parâmetros

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max` e `shutdown.drain_timeout` —
a validação recusa `0` com `exit 1`. Um `op_timeout = 0` faria toda escrita
falhar no ato e ser retentada para sempre sem nunca virar FATAL; um
`drain_timeout = 0` faria todo desligamento terminar em dump de emergência e
código 3. Zero continua legítimo onde é o que diz: `shutdown.pre_stop_delay` (sem
espera), `sink.batch_max_delay` (sem espera para completar o lote),
`sink.fail_timeout` (sem fusível por tempo; o FATAL vem só de
`sink.max_retries`) e os timeouts do `net/http` em `[server]` (sem limite).

#### `[server]`

| Chave | Padrão | Descrição |
|---|---|---|
| `bind` | `:8080` | Endereço da porta pública |
| `listeners` | `1` | Nº de listeners; `>1` habilita `SO_REUSEPORT` |
| `read_header_timeout` | `5s` | Defesa principal contra Slowloris |
| `read_timeout` | `30s` | Headers + corpo. Generoso: 5 MiB em link ruim demora |
| `write_timeout` | `10s` | |
| `idle_timeout` | `60s` | Keep-alive |
| `max_body_bytes` | `5MiB` | `413` acima disso |
| `max_header_bytes` | `64KiB` | `431` acima disso |
| `max_uri_bytes` | `8KiB` | `414` acima disso |
| `max_conns` | `0` | `0` = ilimitado. Teto do **servidor inteiro**, somando todos os `listeners` |
| `max_conns_per_ip` | `0` | `0` = ilimitado. Também somado entre `listeners`. Inútil atrás de proxy único |
| `reject_drain_bytes` | `64KiB` | Dreno antes de rejeitar (§4.3) |
| `http2` | `true` | h2c por prior-knowledge |
| `http2_max_concurrent_streams` | `250` | |

#### `[admin]`

| Chave | Padrão |
|---|---|
| `enabled` | `true` |
| `bind` | `127.0.0.1:9101` |

#### `[proxy]`

| Chave | Padrão | Descrição |
|---|---|---|
| `trusted` | `127.0.0.0/8,::1` | CIDRs confiáveis. **Todo deploy atrás de proxy precisa alterar** |
| `max_xff_entries` | `32` | Teto de entradas de XFF processadas |

#### `[ratelimit]`

| Chave | Padrão | Descrição |
|---|---|---|
| `enabled` | `false` | Seletor de compatibilidade: só `false` (§0, §6) |

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `mode` | `memory` | Seletor de compatibilidade: só `memory` (§0) |
| `capacity_items` | `65536` | |
| `max_bytes` | `512MiB` | |
| `high_water` | `0.80` | Fração que faz `/readyz` falhar |
| `data_dir` | `./data` | Raiz de **toda** escrita em disco do serviço (§13.6). Em contêiner é sempre `/data` |

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `type` | `postgres` | Seletor de compatibilidade: só `postgres` (§0) |
| `workers` | `4` | `WARN` acima de 16 (§8.5.3) |
| `batch` | `256` | |
| `batch_max_delay` | `5ms` | |
| `op_timeout` | `5s` | Também o prazo da validação de boot (§8.5.5) |
| `health_interval` | `2s` | |
| `health_timeout` | `1s` | |
| `fail_timeout` | `30s` | DOWN → FATAL |
| `max_retries` | `10` | Health checks falhos → FATAL |
| `boot_timeout` | `300s` | |
| `retry_backoff_min` | `100ms` | |
| `retry_backoff_max` | `2s` | |

#### `[sink.postgres]`

| Chave | Padrão | Descrição |
|---|---|---|
| `addr` | `127.0.0.1:5432` | `host:porta`, ou diretório de socket unix (começa com `/`) |
| `username` | `webhook` | |
| `password` | *(vazio)* | Redigida no dump e no log de boot |
| `database` | `webhook` | |
| `table` | `loghub.inbox` | A fila de §3.6; `nome` ou `esquema.nome`, minúsculos. O servidor nunca a cria (§8.5.1) |
| `pool_size` | `0` | `0` = `sink.workers + 1` (§8.5.3) |
| `tls` | `false` | |
| `tls_ca` | *(vazio)* | Exige `tls = true` |
| `tls_insecure` | `false` | |

#### `[emergency]`

| Chave | Padrão | Descrição |
|---|---|---|
| `enabled` | `true` | |
| `dir` | `<ring.data_dir>/emergency` | Criado no primeiro dump, não no boot (§13.6) |

#### `[headers]`

| Chave | Padrão |
|---|---|
| `exclude` | *(vazio)* |

#### `[log]`

| Chave | Padrão |
|---|---|
| `level` | `info` |
| `format` | `json` |
| `access` | `false` |
| `access_sample` | `0.01` |

#### `[shutdown]`

| Chave | Padrão |
|---|---|
| `pre_stop_delay` | `0s` |
| `drain_timeout` | `30s` |

### 13.6 Escrita em disco: uma raiz só, e em contêiner ela é `/data`

O serviço escreve em **dois** lugares, e os dois derivam de `ring.data_dir`:

| O quê | Caminho padrão | Só existe quando |
|---|---|---|
| Identidade (`go-loghub-ident`) | `<ring.data_dir>/` (`DATADIR`, §3.5) | sempre, no primeiro boot |
| Dumps de emergência | `<ring.data_dir>/emergency/emg-*.jsonl` (e `.emg-*.jsonl.tmp` durante a escrita, `.corrupt` em quarentena) | `emergency.enabled=true` e um dump (§9) |

Em mais nenhum. Log vai para stdout/stderr (§11.3), métricas são servidas pela
porta admin (§11.1), o corpo nunca é gravado em arquivo temporário (§12.2) e
nenhuma das três dependências de produção (§14.3) usa `TMPDIR`.

**Regra de ecossistema: dentro de um contêiner essa raiz é `/data`.** Vale para
todo contêiner do `loghub-paradise`, não só para este serviço — um ponto de
montagem só, com o mesmo nome em toda imagem, para que backup, rotação de disco
e política de volume sejam decididos uma vez e não por serviço. A imagem oficial
do Redis já grava em `/data`; a regra alinha o resto do ecossistema com ela.

A imagem entrega isso pronta: `/data` criado com dono `65532:65532`,
`WHS_RING_DATA_DIR=/data` e `WORKDIR /data` — este último para que até um caminho
relativo mal configurado caia dentro do volume. O volume nomeado herda dono e
modo do diretório da imagem na primeira montagem, o que dispensa `chown` externo.

Os dois caminhos (`ring.data_dir` e `emergency.dir`) aceitam valor absoluto e o servidor **não** valida se ele cai
sob `/data`: de dentro do processo não há como saber se ele está num contêiner,
e o mesmo binário roda direto no host, onde o padrão `./data` é o certo. Quem
valida é o sistema de arquivos — o contêiner roda `read_only: true` com `/data`
como único ponto gravável. Uma configuração apontando para fora falha assim:

- `ring.data_dir` fora de `/data` → o `MkdirAll` do boot falha e o processo
  sai com código **1**, antes de servir requisição nenhuma (§10.5).
- `emergency.dir` fora de `/data` → o diretório só é criado no instante do dump.
  A falha aparece **durante o incidente**, não no boot: o ring inteiro vira
  `whs_events_dropped_total{reason="emergency_failed"}` e o processo sai com 3
  (§9.3). É a razão principal de a regra existir.

Sem `read_only`, o custo é pior do que um erro: a escrita fora do volume vai para
a camada de escrita do contêiner, funciona, e some no próximo `docker rm`. Nada
falha, nada avisa, e a perda só é descoberta quando alguém for procurar os dados.

Todo o resto do contêiner é leitura, e o layout é o mesmo em toda imagem do
ecossistema:

| Caminho | Papel | Gravável |
|---|---|---|
| `/app` | binário pronto, dono `root`, modo `0755` | não |
| `/data` | identidade, dumps de emergência, e o `WORKDIR` | **sim**, e só ele |
| `/etc/webhook` | INI montado com `:ro` | não |
| `/tmp` | `tmpfs`, rede de segurança do `read_only` | sim, mas volátil |

Duas convenções fecham o layout, e valem para todo serviço do `loghub-paradise`:

- **`WORKDIR` é `/data`.** O processo nasce dentro da única área gravável, então
  um caminho relativo mal configurado cai no volume em vez da camada efêmera do
  contêiner. A imagem oficial do Redis já faz o mesmo.
- **O binário é invocado pelo caminho absoluto `/app/<nome>`**, nunca pelo nome
  solto. O runtime resolveria o nome pelo `PATH`, mas isso faria o contêiner
  depender de uma variável que qualquer camada ou `docker run -e` pode mudar.
  `/app` está no `PATH` da imagem só por conveniência de `docker exec`.

A regra governa as imagens construídas pelo projeto. Imagens de terceiros que
acompanham um deploy, como a do Redis, mantêm o layout delas — `/data` é
justamente o ponto em que as duas coincidem, porque a imagem oficial do Redis já
grava e trabalha lá.

Sendo de `root`, o binário é executável pelo processo e não reescrevível por ele.
`/tmp` não é área de trabalho: o serviço não escreve nele, e o que alguma
biblioteca porventura escrevesse ali morre com o contêiner.


---

## 14. Código

### 14.1 Posição no monorepo

O repositório `loghub-paradise` abriga os quatro serviços do ecossistema. A
**raiz não tem `go.mod`** — cada serviço Go é um módulo aninhado e independente,
deixando a raiz livre para serviços em outras linguagens.

```
loghub-paradise/                  <- sem go.mod
  go-webhook-pg-server/
    go.mod                        <- module .../go-webhook-pg-server
    cmd/go-webhook-pg-server/
    internal/
    docs/SPEC.md
  webhook-worker/                 <- futuro, linguagem livre
  webhook-admin/
  webhook-shots/
  tmp/                            <- documentos históricos, não usados
```

Consequências:

- Cada serviço Go versiona, testa e libera de forma independente, com tags no
  formato `go-webhook-pg-server/v1.2.0`.
- `go install github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server/cmd/go-webhook-pg-server@latest`
  funciona direto.
- Uma atualização de dependência aqui não obriga a recompilar nem retestar os
  outros serviços.
- **Sem `go.work`.** É ferramenta de desenvolvimento, exigiria `GOWORK=off` no
  CI e, com um único serviço Go, não resolve problema nenhum. Reavaliar apenas se
  o `webhook-worker` for escrito em Go e a duplicação do envelope (§14.2) doer.


### 14.2 Layout interno

```
go-webhook-pg-server/
  cmd/go-webhook-pg-server/main.go
  internal/
    config/     registry, ini, flags, env, defaults, dump, validação
    server/     http.Server, handler, pipeline, admin
    clientip/   XFF/X-Real-IP + proxies confiáveis        [lógica pura]
    ident/      Identity congelada e gramática de space   [lógica pura]
    envelope/   JLH, payload webhook, ParseHead, pools    [lógica pura]
    jlhcheck/   oráculo de teste dos schemas              [só em teste]
    ring/       Ring + memory.go
    emergency/  dump e reingestão em JSON Lines
    sink/       Sink + postgres.go + inbox.go + workers.go + health.go
    obs/        métricas + slog
  docs/SPEC.md
  go.mod
```

`internal/` impede import externo acidental. `clientip` e `envelope` são lógica
pura, testáveis sem rede, sem disco e sem banco.

`ring` e `sink` expõem cada um **uma** interface, com uma implementação (§2.1).
Os pacotes e arquivos que ficaram têm o mesmo nome e o mesmo conteúdo dos do
fast, menos o que saiu: `diff -ruN -x .DS_Store ../go-webhook-fast-server .`
mostra exatamente a redução e é a base do porte de correções nos dois sentidos.

### 14.3 Dependências

| Módulo | Uso | Escopo |
|---|---|---|
| `github.com/jackc/pgx/v5` | Cliente PostgreSQL puro Go — protocolo v3, pool, tipos, `SQLSTATE` estruturado | produção |
| `github.com/patrickbrandao/go-loghub-uuidv7` | UUIDv7 (RFC 9562) nível 3, `GenerateAt` sobre a amostra de relógio do `event` (§3.1) | produção |
| `github.com/patrickbrandao/go-loghub-ident` | Identidade LogHub: AgentUUID (`from`), Workspace (`space`), AgentName, Hostname (§3.5) | produção |
| `github.com/google/uuid` | Oráculo independente nos testes de UUIDv7 | **somente teste** |
| `github.com/santhosh-tekuri/jsonschema/v6` | Contrato dos envelopes com os schemas JLH e `webhook` (§15.1) | **somente teste** |

**Três dependências em produção. Mais nada.** `go-loghub-uuidv7` e
`go-loghub-ident` não têm dependência transitiva alguma — só a biblioteca padrão
do Go. Saem, com os recursos, `rueidis`, `modernc.org/sqlite`,
`go-sql-driver/mysql`, `nats.go` e `clickhouse-go` e todas as transitivas
deles. Acrescentar uma dependência de produção exige justificativa nesta seção.

#### pgx: a primeira dependência

Cada dependência tem uma justificativa medida. A do pgx é curta:

- **Não há alternativa razoável na biblioteca padrão.** O protocolo v3 do
  PostgreSQL — autenticação SCRAM, formato binário de arrays, `COPY`, cancelamento,
  *pipelining* — é da ordem de dezenas de milhares de linhas. Reescrevê-lo aqui
  seria a decisão errada.
- **Puro Go, sem CGO.** `CGO_ENABLED=0` e o binário estático (§12.4, Dockerfile)
  continuam de pé. `lib/pq` também é puro Go, mas está em modo de manutenção,
  não expõe `RowsAffected` de `ON CONFLICT` sem passar pelo `database/sql`, e
  codifica arrays por texto — o `unnest` de §8.5.4 seria mais lento e mais
  frágil.
- **A API nativa, não `database/sql`.** O pgx é usado direto: `pgconn.PgError`
  traz o `SQLSTATE` que a tabela de §8.5.7 exige, arrays vão em binário, e o
  pool é o dele. O `database/sql` acrescentaria uma camada de conversão sem
  devolver nada.
- **Transitivas.** `pgpassfile` e `pgservicefile` (leitores de arquivos de
  configuração do libpq, sem rede), `puddle/v2` (o pool), `golang.org/x/sync`,
  e `golang.org/x/text`.
  Nenhuma abre socket por conta própria, escreve em disco ou lê `TMPDIR`; a
  regra de §13.6 continua verdadeira.


#### UUIDv7 nível 3 e identidade: a segunda e a terceira dependência

Até 2026-10-05 o id era um UUIDv7 `Level2` da `go-loghub-uuid`, gerado numa
leitura de relógio **separada** da que preenchia `ts`/`uts`/`nts`: ~12% dos
eventos tinham o microssegundo do id diferente do `uts` (pior caso medido,
115 µs), e o tempo do id era só chave de ordenação. A `go-loghub-uuidv7`
v0.0.3 tem `GenerateAt(level, t)`, que fecha essa dívida: **uma** amostra `t`
alimenta `uuid` (nível 3: ms + µs + ns) e `event` (§3.1).

- **Gerador:** `uuidv7.NewGenerator()`, um por processo, entropia do ChaCha8
  por thread do runtime (sem lock, sem syscall). As medições do Ato 1 da v1
  continuam valendo: `crypto/rand` por id custaria mais de 10% de um core no
  alvo de §15.5. O `init()` que confere se o `crypto/rand` responde continua.
- **Colisão:** o nível 3 deixa 52 bits aleatórios por nanossegundo de relógio.
  No macOS o relógio tem resolução de µs e os ns gravados são zero (a skill
  avisa); os testes conferem `uuid` × `event` em ms e µs em toda plataforma, e
  em ns só onde o relógio os tem.
- **Nada é segredo:** nenhum token, assinatura ou capacidade deriva do `uuid`.

A `go-loghub-ident` v0.5.2 entrou junto: dá o `from` e o `space` de todo
envelope e persiste o AgentUUID em `DATADIR` (§3.5). As duas não têm
dependência transitiva.


---

## 15. Testes

### 15.1 Unitários

Lógica pura, com tabela de casos:

- `clientip` — cadeias de XFF com N proxies, entradas inválidas, `unknown`, IPv6
  com colchetes e porta, IPv4-mapped, peer confiável e não-confiável,
  precedência XFF/X-Real-IP, todas as entradas confiáveis.
- `envelope` — **contrato** de todo envelope com `jlh-envelope.schema.json` e
  `jlh-webhook-fast.schema.json` (cópias das skills em `internal/jlhcheck/loghub/`,
  validador `santhosh-tekuri/jsonschema/v6` com `AssertFormat`) e com as regras
  que o `validate.py` da skill confere e um schema não expressa (`jlhcheck`);
  `ParseHead` lendo de volta o que foi escrito e recusando qualquer outra forma;
  a query de `parse_qsl`; a pré-passada do corpo; o pior envelope contra
  `MaxEventBytes`; o escape contra o decode do Python; os pools; e nenhum
  envelope com `0x0A` cru (§9.1).
- `config` — precedência argv > env > ini > padrão em cada tipo, a validação de
  §10.1, os três seletores aceitando só o valor implementado e as chaves dos
  recursos retirados recusadas como desconhecidas (§0).
- `emergency` — round-trip do JSON Lines com JLH reais, arquivo terminado sem
  `\n`, linha danificada no meio com as seguintes entregues, linha acima do teto,
  linha cujo JLH o `ParseHead` recusa, JSON inválido, linhas vazias, um envelope
  com `0x0A` deixado fora do arquivo e contado, e `emg-*.bin` ignorado (§9).

### 15.2 Fuzz — obrigatório

- `FuzzResolve` — o único código que processa entrada hostil
  estruturada; um bug ali vira spoofing de IP.
- `FuzzRead` — o dump é lido de disco após um crash; um leitor frouxo
  transforma um arquivo corrompido em pânico no boot, ou entrega uma linha que
  não é envelope.
- `FuzzEnvelope` — método, alvo, cabeçalho, query e corpo arbitrários: todo
  envelope passa no contrato, não tem `0x0A`, e o `ParseHead` o lê de volta.
- `FuzzServeHTTP` — bytes arbitrários num socket real, com e sem proxy
  confiável: o servidor recusa, ou o envelope passa no contrato.
- `FuzzJSONBSafe` — a troca do `\u0000` para a `loghub.inbox` só troca escapes
  reais e decodifica igual com U+FFFD.

### 15.3 Integração

PostgreSQL 18 real, endereçado por `WHS_TEST_PG_DSN` e **pulando** na ausência
dela; cada teste cria a sua fila num esquema próprio a partir de
`docker/postgres/inbox.sql`.

Cenários obrigatórios:

- Todo id aceito chega à fila, e cada linha lida de volta passa no `jlhcheck`;
  corpo binário em base64 e NUL na query ida e volta (§3.6); redelivery do mesmo
  lote não duplica e é contada; os slices do pool de argumentos voltam zerados
  (§8.5.4).
- Tabela ausente, coluna com tipo errado, índice único ausente ou extra, papel
  sem `INSERT`/`SELECT (uuid)`, sessão somente leitura ⇒ `exit 3` **imediato**,
  porta pública fechada (§8.5.5).
- A validação de boot roda no prazo `sink.op_timeout`, não no
  `sink.health_timeout` (§8.5.5).
- Destino cai → `503` em até `health_interval`; volta dentro da janela → retoma
  sem perder nada.
- Destino cai e não volta → FATAL, dump JSON Lines correto, `exit 3`; boot
  seguinte reingere e remove o arquivo.
- Boot com `emg-*.jsonl` com uma linha danificada no meio → as íntegras
  entregues, arquivo renomeado para `.corrupt`.
- Boot com `emg-*.bin` → `WARN`, arquivo intocado, porta pública abre.
- `SIGTERM` com ring cheio → dreno completo, `exit 0`, zero perdas.
- Segundo `SIGTERM` → `exit 130`.
- `SIGHUP` → ignorado, processo segue servindo.
- `-race` em todos os caminhos de erro do pool de buffers (§12.3).

Sem servidor PostgreSQL, a suíte de `app` sobe o ciclo de vida com um sink
falso de teste (no próprio arquivo de teste, nunca em código de produção).

### 15.4 Carga

- `bombardier` para HTTP/1.1, `h2load` para h2c.
- **Dois perfis**, sempre: conexões persistentes (mede o pipeline) e conexão nova
  a cada requisição (mede accept + handshake). Bots reais fazem os dois.
- `perf` + flamegraph para localizar CPU. Quase sempre é alocação ou
  serialização, raramente o parser HTTP.

### 15.5 Critérios de aceite v1

Referência: 1 nó de 4 vCPU, corpo de 1 KiB, NVMe local, PostgreSQL 18 local.

| Combinação | Métrica | Meta |
|---|---|---|
| `ring=memory` + `sink=postgres` | Vazão e p99 do `200` | **sem meta numérica** até a primeira medição válida |
| Qualquer | RSS sob carga por 30 min | estável, < 512 MiB |
| `SIGTERM` com ring cheio | Eventos perdidos | 0 |
| Sink FATAL | Eventos perdidos (via emergência) | 0 |
| Qualquer | Casos de spoofing de XFF rejeitados | 100% |

A vazão depende da latência de commit do servidor e do tamanho do pool; um
número inventado aqui viraria referência. A medição decide também se o `COPY` de
§8.5.4 volta. O fast com `ring=memory` + `sink=none` dá o teto do pipeline que
este servidor compartilha, e a diferença para a medição daqui é o custo exato do
PostgreSQL.

---

## 16. Matriz de garantias

### 16.1 O que o `200` significa

| `ring` | `sink` | O `200` significa | Perda em `kill -9` |
|---|---|---|---|
| `memory` | `postgres` | Está na RAM deste nó | **Sim** (mitigado por §9 e §10.3) |

Depois do `200`, a entrega na fila tem a durabilidade que o `synchronous_commit`
do PostgreSQL der (§8.5.6); o servidor não lê nem altera esse valor.

### 16.2 Custo relativo

Não medido (§15.5). A vazão fica presa a `pool_size / latência de commit` do
servidor, dividida pelo tamanho do lote: com `sink.batch = 256` cada commit leva
até 256 eventos.

---

## 17. Invariantes

Regras que nenhuma otimização futura pode violar. A numeração é a do fast; as
regras dos recursos retirados ficam marcadas.

1. O `200` **nunca** é enviado antes de o item estar no ring (§2.2).
2. Ring cheio → `429`. **Nunca** um `200` seguido de descarte — exceto na recusa
   permanente de **um** lote por um destino vivo (`logic`, §8.5.7), que é logada
   e contada. Uma recusa que atingiria todos os eventos igual nunca é `logic`: é
   fatal (§8.5.7).
3. Um buffer volta ao pool **uma vez**, e só após confirmação (§12.3).
4. Um evento aceito só sai do ring após confirmação do sink — **nunca** é
   descartado para liberar espaço.
5. `network.ip.src` **nunca** vem de header quando o peer TCP não está em
   `proxy.trusted`, e nenhum outro cabeçalho de proxy (`X-Forwarded-Proto`,
   `-Host`, `-Port`, `X-Client-Port`, `X-Server-Addr`, fingerprints) é lido de
   um par que não esteja nela (§3.2).
6. Nenhum label de métrica deriva de conteúdo controlado pelo cliente.
7. O corpo **nunca** é parseado, decodificado, validado ou modificado: vai em
   `payload.content.body` como texto escapado ou base64, e decodificado é o
   mesmo byte a byte (§3.2).
8. `peer-addr` (em `network.meta`) sempre registra a ponta TCP real,
   independente de headers.
9. Nenhum log síncrono por requisição no caminho padrão.
10. *(Retirada: era a regra dos comandos do rueidis, §8.2.3 do fast.)*
11. O envelope JLH é escrito **uma** vez no handler (§12.1), e o ring, o dump e a
    fila recebem os mesmos bytes; as colunas da `inbox` são lidas do próprio JLH,
    e a única alteração — `\u0000` em `�` na cópia do `jsonb` (§3.6) — nunca
    toca o JLH do item.
12. `ring` e `sink` são **ortogonais**: nenhuma implementação de um consulta o
    tipo do outro (§2.1).
13. Toda escrita em disco acontece sob `ring.data_dir` — e dentro de um
    contêiner esse diretório é `/data`, em toda imagem do ecossistema (§13.6).
14. `uuid` e `event` saem de **uma** amostra de relógio, e o `uuid` devolvido em
    `X-LogHub-UUID` é o do envelope na fila; ele nunca é regerado (retry, dump,
    reingestão) (§3.1).
15. *(Retirada: era a checagem de `max_payload`/`max_allowed_packet`/
    `max_memory_usage` dos destinos retirados. O teto do PostgreSQL é por
    parâmetro, e o aviso de §8.5.4 cobre o lote.)*
16. Uma linha do dump de emergência é um JLH inteiro seguido de `\n`, e nada
    mais (§9.1).

---

## 18. Roteiro pós-v1

| Item | Motivo de estar fora da v1 |
|---|---|
| Múltiplas tabelas por Host ou path | Depende do desenho de multi-tenancy do `webhook-admin` |
| Particionamento da tabela | A `loghub.inbox` é uma fila; a skill a define sem partição (§8.5.9) |
| Pooler externo em modo *transaction* | Exige desligar o cache de *statements* do pgx ou migrar para `COPY` com *staging* (§8.5.2) |
| `COPY` com tabela de *staging* | Só se a medição de §15.5 mostrar que o `INSERT` com `unnest` é o gargalo (§8.5.4) |
| `stage`, `503` no ring cheio, PROXY protocol | Entram quando entrarem no `go-webhook-fast-server` (§0) |
| Verificação HMAC por tenant | Precisa de um registro de tenants |
| Rings particionados por CPU | Só se o benchmark mostrar contenção no ring único |
| HTTP/3 | Nenhum emissor relevante usa; 2–4× a CPU por byte |
| TLS na porta pública | Proxy reverso resolve com ACME de graça |

---

## 19. Lacunas e riscos

O que ainda não está provado fica em `docs/TESTING.md` §8, e os riscos aceitos
por decisão, em `docs/DECISOES.md` (Riscos). Esta seção guarda os **defeitos**
do código atual.

### 19.1 Pendências conhecidas

Problemas reais, conferidos contra o código atual e ainda não corrigidos. Cada
item diz o problema, a consequência e a direção da correção. Corrigido um, ele
sai daqui e a regra vai para a seção que ele cita, spec primeiro; os pacotes
são os do fast, então a correção se porta nos dois sentidos (§14.2).

1. **O ring conta `len(Env)`, não o buffer que o item segura** (§7.2, §12.2,
   §12.4). O JLH vive no buffer inteiro da classe do pool, escolhida por uma
   estimativa com folga para o cabeçalho: um JLH de ~2,4 KiB ocupa 16 KiB e
   conta 2,4. Com o destino fora, o ring cheio prende de 4 a 7× o que
   `whs_ring_bytes` mostra, passa do `GOMEMLIMIT` (2× `ring.max_bytes`) e do
   limite do contêiner, e um OOM perde os eventos já respondidos com `200`, sem
   dump. Direção: o ring de memória cobra `cap(Env)` mais um overhead medido do
   item, gravado no item no `Enqueue` e descontado igual no `Commit` e no
   `Drain`; e, depois do `AppendWebhook`, o JLH passa para a classe mais justa
   ao seu tamanho, com o buffer grande devolvido ao pool.
2. **NaN aceito nos números da configuração** (§13.5). O `KFloat` aceita `nan`,
   e toda comparação com NaN é falsa: `ring.high_water = nan` passa pela
   validação e o `/readyz` nunca acusa saturação; `log.access_sample = nan`
   desliga o access log em silêncio. Direção: recusar valor não finito (`NaN`,
   `±Inf`) no parser do `KFloat` (`exit 1`) e escrever as faixas na forma "não
   está dentro" (`!(x > 0 && x <= 1)`).
3. **`admin.bind` vazio escuta em todas as interfaces** (§11, §13.5). Com
   `admin.enabled = true`, um `admin.bind` vazio só gera o `WARN` de
   não-loopback, e o `Listen("")` abre `/metrics`, `/readyz` e `/healthz` em
   toda interface, numa porta sorteada que nenhum firewall planejado cobre.
   Direção: com o admin ligado, recusar o vazio e exigir `host:porta`
   (`net.SplitHostPort`), `exit 1`; o `WARN` de não-loopback só com o admin
   ligado.
4. **O `--dump-config` não volta igual** (§13.2). O dump escreve o texto cru, e
   o parser de INI tira os espaços das pontas e um par de aspas externo:
   `" abc "` volta `abc`, e `"x"` com aspas literais volta `x`, contra a
   promessa de que o dump relido reproduz a configuração. Direção: no dump, pôr
   entre aspas o valor com espaço nas pontas ou que começa e termina com a
   mesma aspa (o `unquote` tira exatamente uma camada).
5. **O compose não repassa `WHS_SINK_POSTGRES_TLS_CA`** (§8.5.11). O
   `.env.example` e o `docker/README.md` mandam defini-la, mas o
   `compose.memory-postgres.yml` só repassa `WHS_SINK_POSTGRES_TLS`: o valor do
   `.env` é ignorado, e o TLS contra uma CA privada falha com "unknown
   authority" com o operador certo de ter configurado. Direção: repassar
   `WHS_SINK_POSTGRES_TLS_CA` com padrão vazio e deixar comentado o volume
   `:ro` de `/etc/webhook`.
6. **Um erro de `accept` encerra sem dreno** (§10.3, §10.5). O `net/http` só
   retenta o erro de `accept` que chama de temporário; ENOBUFS, ENOMEM, EPERM ou
   EPROTO encerram o `Serve`, e o processo sai com 1 sem dreno nem dump: o ring
   e os lotes retidos, já respondidos com `200`, se perdem sem contagem, e o
   código manda procurar um erro de configuração. Direção: um listener que
   retenta o erro não temporário com backoff e log amostrado (com uma cota
   antes de desistir), e um `Serve` que termina com erro passando pelo shutdown
   (dreno ou dump), com um código de saída próprio definido antes em §10.5.
7. **O `Content-Length` declarado aloca o corpo antes de ele chegar** (§4.3
   passo 11, §12.2). O `readBody` pede ao pool um buffer do tamanho declarado,
   até `max_body_bytes`: só com cabeçalhos, cada requisição prende 5 MiB até o
   `read_timeout`, e em h2c uma conexão faz isso em 250 fluxos (~1,2 GiB),
   passando do `GOMEMLIMIT` e do limite do contêiner; um OOM leva o ring.
   Direção: nada é alocado pelo que o par promete, só pelo que chegou — o
   declarado pré-dimensiona até um teto pequeno (64 KiB), e o buffer cresce só
   quando está cheio de bytes recebidos, sem passar do declarado.
8. **`emergency.enabled = false` também desliga a reingestão** (§9.2, §10.1
   passo 4). O `Store` só existe com a chave ligada, e sem ele a reingestão
   retorna em silêncio: os dumps de um boot anterior ficam no disco sem log,
   com `whs_emergency_files` em 0 e `/readyz` 200 — eventos respondidos com
   `200` que nunca chegam à fila, contra o "todo arquivo" de §9.2. Direção: a
   reingestão e os gauges sempre leem `emergency.dir`, com um `Store` próprio;
   a chave decide só a escrita.
9. **O dreno de recusa ignora HTTP/2 e `Expect: 100-continue`** (§4.3). O dreno
   de `reject_drain_bytes` e o `Connection: close` foram pensados para o
   HTTP/1.1: em h2c o `Connection: close` vira `GOAWAY` da conexão inteira,
   compartilhada pelo proxy, a cada recusa com mais de 64 KiB de corpo,
   justamente sob sobrecarga; e com `Expect: 100-continue` a primeira leitura
   do dreno envia o `100 Continue`, convidando o corpo que vai ser recusado.
   Direção: drenar só em HTTP/1.x sem `Expect` pendente, e nunca pôr
   `Connection: close` em HTTP/2 (o `net/http` já encerra o fluxo com
   `RST_STREAM(NO_ERROR)`).
10. **O dreno do `SIGTERM` não vê o FATAL** (§10.2, §10.3). Se o sink vira
    FATAL durante a fase 4 de um shutdown por sinal, o `waitDrained` espera o
    ring esvaziar até o fim do `drain_timeout`, e só então o dump começa —
    talvez depois do `SIGKILL` do orquestrador, que leva o ring; e, se o FATAL
    veio dos fusíveis com o ring já vazio, o processo sai com 0, embora §10.2
    mande 3. Direção: o `waitDrained` retorna com a saúde em FATAL, e o código
    de saída é 3 sempre que ela terminar FATAL.
11. **`23502` em execução é `logic`** (§8.5.5, §8.5.7). O servidor nunca manda
    NULL nas dez colunas, então um `not_null_violation` depois do boot é recusa
    da **tabela** (uma coluna `NOT NULL` acrescentada, o `DEFAULT` de
    `received_at` retirado, um trigger): como classe `23`, todo lote é
    descartado em `sink_logic` com o sink UP, para sempre — o laço que §8.5.5
    evita no boot e que fez da classe `42` fatal. Direção: `23502` fatal (dump
    e exit 3; o boot seguinte já recusa a coluna extra obrigatória), com a
    tabela de §8.5.7 e o `TestClassifySQLState` mudando juntos; um trigger que
    anula uma coluna só em certas linhas passa a derrubar o processo, e isso
    fica escrito.
12. **`whs_emergency_files` não conta um dump ilegível** (§9.2, §11.1). O
    `Count` que reconta os gauges no boot pula o arquivo cujo `Read` falha (sem
    permissão de leitura, por exemplo): ele fica no disco e fora do gauge, e o
    alerta por `whs_emergency_files > 0` não dispara. Direção: contar os
    arquivos pelo `List`, legíveis ou não, e somar eventos só dos legíveis.
