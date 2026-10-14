# go-webhook-server — Especificação v1

Status: **congelada para implementação**
Data: 2026-09-06
Revisão: 2026-10-11 — pendências conhecidas do código atual (§19.1); o proxy
confiável precisa reescrever os cabeçalhos que o servidor lê dele (§5.2);
`kind="timeout"` (§11.1); dump da configuração em `debug` (§11.3); um só
conjunto de pools para o corpo e o JLH (§12.2); o teto por IP fecha a conexão e
o global segura o accept (§13.5).
Revisão: 2026-10-07 — `sink=clickhouse`, o sétimo destino (§3.9, §8.8, §14.3): o
mesmo espelho da `loghub.inbox` num `ReplacingMergeTree` ordenado por
`(event_at, uuid)`, `payload` no tipo `JSON`, protocolo nativo do
`clickhouse-go` v2 (a oitava dependência), `insert_deduplication_token` por
lote; `params` e `query` tipados como `Map`, porque o tipo `JSON` descarta objeto
vazio (§3.9); `async_insert` e a durabilidade ficam com o administrador do ClickHouse
(§8.8.6); a matriz passa a 21 combinações (§16).
Revisão: 2026-10-06 — **LogHub**: o evento passa a ser o envelope JLH
`type=webhook.request` (skill `loghub-type-webhook-schema`), com `uuid` UUIDv7
nível 3 e `event` de uma só amostra de relógio, `from`/`space` do
`go-loghub-ident` (§3); `server.node_id` sai; resposta `X-LogHub-UUID` com corpo
JSON (§4.4); métodos fora de `^[A-Z]+$` e `OPTIONS *` recusados (§4.2); Redis e
NATS em `loghub.{space}.{type}` com o JLH inteiro (§3.3, §3.8); `sink=postgres`
na fila `loghub.inbox` da skill `loghub-postgres-table-inbox` (§3.6); SQLite e
MySQL numa tabela `inbox` que espelha a fila (§3.7); ring SQLite e dump
guardam só o JLH (§7.4, §9.1); o maior envelope é calculado (§3.2).
Revisão: 2026-10-06 — revisão independente do JLH: a `url` percent-encoda só o
que o RFC 3986 não aceita numa query (`#` inclusive), e `url` e `query` voltam a
decodificar igual (§3.2); UTF-8 inválido vira um U+FFFD por subparte máxima,
como no Python (§3.2, §12.1); chaves da query agrupadas pelo texto escrito
(§3.2); proxy confiável sem `X-Forwarded-For` ainda define `dst` e `dport`, e o
`host` passa a minúsculas só em ASCII (§3.2); `content.type` diz o que chegou
mesmo fora de `headers` (§3.2); `ParseHead` exige `uuid` e `from` UUIDv7
minúsculos e o dump confere o JSON inteiro (§9.2, §12.1); o dump vai direto ao
sink (§9.2, §10.1); `ring=sqlite` devolve o JLH ao pool após o `COMMIT` (§12.3).
Nos destinos: o MySQL confere no boot as larguras da DDL, `DATETIME(6)` e a
ausência de outro índice único, cai na classe do SQLSTATE com um número
desconhecido, grava o lote de vários pedaços numa transação e recusa o MariaDB
(§3.7, §8.6); o PostgreSQL aceita `timestamptz(6)`, recusa precisão menor e
índice único ou exclusão extra (§3.6, §8.5.5); o NATS recusa como fatal o
subject de um backlog que o stream não cobre (§3.8, §8.7.5).
Revisão: 2026-09-10 — `sink=postgres`, o quarto destino (§3.6, §8.5, §14.3)
Revisão: 2026-09-14 — posse de buffer nos caminhos de erro do `Enqueue` (§7.4, §7.5, §12.3); linhas repetidas de `X-Forwarded-For` e `X-Real-IP` (§5.2, §5.3); sonda de saúde do `sink=sqlite` por heartbeat (§8.3.5); erros permanentes do Redis são fatais (§8.2.4); tetos de conexão por servidor, não por listener (§13.5); duas referências na recarga por `SIGHUP` (§13.3); durações zero recusadas na validação (§13.5); `kind="timeout"` alimentado e sobras do `ring=sqlite` não contam como perda (§10.3, §11.1); nome do dump com nanossegundos (§9.1); aviso de `GOMEMLIMIT` por modo de ring (§12.4)
Revisão: 2026-09-15 — revisão independente das correções: sonda do Redis por escrita (`XLEN` + `XADD NOMKSTREAM`), recusa permanente na discagem ou na sonda é fatal na hora (§8.2.4, §10.1); `ring=none` devolve os buffers ao pool após o `Write` (§12.3); tabela `health` no esquema (§3.3); `sink.retry_backoff_max` recusa zero e `sink.fail_timeout = 0` documentado (§13.5); aviso de `GOMEMLIMIT` qualificado por modo na tabela de §10.1
Revisão: 2026-09-15 — `sink=mysql` e `sink=nats`, o quinto e o sexto destinos (§3.7, §3.8, §8.6, §8.7, §14.3): a matriz passa a dezoito combinações (§16); `sink.workers` rebaixado para 4 também com `sink=mysql` (§10.1)
Linguagem: Go 1.25+ (desenvolvido e validado em 1.27)
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-webhook-server`
Binário: `go-webhook-server`

---

## 1. Escopo

Servidor HTTP de ingestão de webhooks. Recebe requisições arbitrárias vindas da
internet, encapsula cada uma num envelope e a entrega a um **destino
configurável**. Escalável horizontalmente: instâncias são independentes e não
compartilham estado entre si.

### 1.1 Objetivos

- Máximo de requisições por segundo com mínimo de CPU e memória.
- Nunca responder `200` para um evento que não foi aceito segundo a garantia da
  combinação `ring` × `sink` configurada.
- Backpressure explícito em vez de perda silenciosa.
- Operar tanto acoplado ao ecossistema (`sink=redis`, alimentando o
  `webhook-worker`) quanto de forma autônoma (`sink=sqlite`, sem Redis nenhum),
  contra um banco compartilhado entre instâncias (`sink=postgres`, `sink=mysql`),
  num stream JetStream (`sink=nats`) ou num armazém analítico (`sink=clickhouse`).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| HTTP/3 e QUIC | Nenhum emissor relevante de webhook usa; 2–4× a CPU por byte |
| TLS na porta pública | Responsabilidade do proxy reverso (Traefik/nginx) |
| Roteamento interno de URLs | Critérios de roteamento ficam no proxy reverso |
| Parse do corpo | O corpo é tratado como bytes opacos |
| Verificação de assinatura HMAC | Responsabilidade do `webhook-worker` |
| Redis Cluster | Um stream vive num único slot; cluster não traz vazão |
| `Upgrade: h2c` (RFC 7540 §3.2) | Removido pela RFC 9113; stdlib não implementa |
| Multi-tenancy / múltiplos streams | v2 |

---

## 2. Arquitetura

### 2.1 Dois eixos ortogonais

O desenho tem **dois eixos independentes**, e é essencial não confundi-los:

- **RING** — *onde o evento espera* entre ser aceito e ser entregue.
  `memory` | `sqlite` | `none`
- **SINK** — *para onde o evento vai*, o destino final.
  `redis` | `sqlite` | `postgres` | `mysql` | `nats` | `clickhouse` | `none`

As vinte e uma combinações são todas válidas e têm significado distinto (§16).

```
   internet
      |
   [proxy reverso: TLS, roteamento, WAF]
      |  HTTP/1.0 · HTTP/1.1 · h2c (mesma porta)
      v
 +------------------------------------------------------------------+
 | porta pública  (server.bind, padrão :8080)                        |
 |  accept -> headers -> clientip -> ratelimit -> 413 -> envelope    |
 +---------------------------------+--------------------------------+
                                   |
                    +--------------v---------------+
                    | RING                         |
                    |  memory  canal + tetos       |
                    |  sqlite  ring.db, fsync      |
                    |  none    sem buffer          |
                    +--------------+---------------+
                                   | sink.workers
                    +--------------v---------------+
                    | SINK                         |
                    |  redis     XADD em pipeline  |
                    |  sqlite    events.db         |
                    |  postgres  loghub.inbox      |
                    |  mysql     INSERT em lote    |
                    |  nats      publish JetStream |
                    |  clickhouse INSERT nativo    |
                    |  none      descarta          |
                    +--------------+---------------+
                                   |
     +-----------+-----------+-----+------+-----------+-----------+-----------+
     v           v           v            v           v           v           v
  Redis       events.db   loghub.inbox tabela      stream      tabela      /dev/null
  Stream      (tabela     (PostgreSQL, inbox       JetStream   inbox       (bench)
  loghub.     inbox)      fila)        (MySQL)     loghub.<space>. (ClickHouse)
  <space>.                                         webhook.request
  webhook.request

 todo destino recebe o MESMO envelope JLH type=webhook.request (§3)
 emergência: ring=memory não-vazio + sink FATAL
             => dump em <emergency.dir>/emg-<agent_uuid>-<unixnano>.bin  (§9)

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

> O `200` é enviado no instante em que a garantia da combinação configurada
> é cumprida — nunca antes.

| `ring` | O `200` sai quando |
|---|---|
| `memory` | O item entrou no canal do ring |
| `sqlite` | O `COMMIT` do grupo no `ring.db` terminou (com `fsync` se `synchronous=FULL`) |
| `none` | O **sink** confirmou a escrita (ou imediatamente, se `sink=none`) |

---

## 3. Modelo de dados (LogHub)

Desde 2026-10-06 o evento é um **envelope JLH** (JSON LogHub) do tipo
`webhook.request`: o servidor é o *bridge-in* HTTP do LogHub. O envelope v1
próprio (`id`, `ts`/`uts`/`nts`, corpo à parte) deixou de existir; nenhum destino
o recebe mais.

### 3.0 Fontes normativas

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH: nove campos, regras, `jlh-envelope.schema.json` |
| skill `loghub-type-webhook-schema` | consultada em 2026-10-06 | `type=webhook.request`, payload (`mode`, `network`, `headers`, `query`, `content`...), respostas HTTP |
| skill `loghub-postgres-table-inbox` | consultada em 2026-10-06 | `sink=postgres`: fila `loghub.inbox` (§3.6) |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 | `uuid` nível 3 de uma amostra de relógio |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 | `from` (AgentUUID), `space` (Workspace), `DATADIR` |

Cópias dos schemas ficam em `internal/jlhcheck/loghub/` e só os testes
as leem (§15.1): a validação por JSON Schema **não** roda no caminho quente (o
serializador é provado por testes de contrato e fuzz). Onde as skills divergem,
vale a mais restritiva: `space` `^[a-z0-9-]+(\.[a-z0-9-]+)*$` com até 64
caracteres, `step` ≤ 32, `from` UUIDv7 minúsculo.

O `go-webhook-fast-server` é o irmão com `type=webhook` e `mode=fast` fixo
(skill `loghub-type-webhook-fast`). O payload dos dois é o mesmo, campo a campo;
muda o `type`, o `mode` e o corpo da resposta (§4.4).

### 3.1 Envelope

```json
{
  "uuid": "01a1122b-8b69-73a4-b9d1-074a5fb34be2",
  "from": "01a1122b-6a52-7ee1-a4fe-16a232e7f4ed",
  "space": "default",
  "type": "webhook.request",
  "level": 9,
  "sid": 1,
  "step": 0,
  "event": { "uts": 1791313406, "sub": 57932925, "iso": "2026-10-06T17:03:26.057Z" },
  "payload": {
    "mode": "bucket",
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
| `type` | `webhook.request`. |
| `level` | `9` (data-channel). |
| `sid` | PID do processo. Nunca aleatório; agrupa por `(from, sid)`. |
| `step` | `0`: o servidor é a origem da mensagem. |
| `event` | `uts` (segundos), `sub` (ns no segundo) e `iso` (`yyyy-MM-ddTHH:mm:ss.SSSZ`, **truncado** no ms) da amostra `t`, tomada no fim da leitura dos headers, antes do corpo. Era a dívida do §3.1 antigo (duas leituras de relógio); com `GenerateAt` ela acabou: `uuid` e `event` concordam até o nanossegundo (no macOS, até o µs). |
| `payload` | §3.2. |

### 3.2 Payload `webhook.request`

Todas as propriedades são sempre emitidas, na ordem abaixo; vazias continuam
presentes (`[]`, `{}`, `""`). Não há `callback` nem `forward`: a v1 não tem rotas.

| Propriedade | Origem |
|---|---|
| `mode` | Garantia da combinação ring × sink (§2.2), fixa no boot: **`bucket`** quando o envelope está em disco ou num destino antes do `200` — `ring=sqlite`, ou `ring=none` com sink diferente de `none` —; **`fast`** quando o `200` sai com o envelope só na memória — `ring=memory`, ou `ring=none` com `sink=none`. `commit` e `full` não existem aqui (exigiriam esperar um worker). |
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

**O maior envelope** é calculado, não configurado (§8.7.5, §8.6.4):

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

### 3.3 Entrada do stream Redis (`sink=redis`)

```
XADD <sink.redis.stream> [MAXLEN ~ N] * e <jlh>
```

Um campo só, `e`, com o JLH de §3.1 byte a byte. Não há `v` nem `b`: o JLH é
autodescritivo pelo `type`, e o corpo está em `payload.content`.
`sink.redis.stream` tem padrão `loghub.{space}.{type}`, com `{space}` e `{type}`
expandidos **por envelope** com os valores do próprio envelope (a convenção de
subject do JLH, para que um consumidor de Redis use a mesma). Um nome literal,
sem chaves, é usado como está.

### 3.4 Headers

`headers.exclude` (lista de nomes, vazia por padrão) omite headers do envelope;
comparação sem distinção de maiúsculas. **Nenhuma redação por padrão**:
`Authorization`, `Cookie` e as assinaturas HMAC (`X-Hub-Signature-256`,
`Stripe-Signature`) são capturados integralmente, porque o worker pode precisar
deles para autenticar o chamador. Os cabeçalhos de proxy ficam na lista como
chegaram: são evidência do caminho.

### 3.5 Identidade

`server.node_id` não existe mais: a identidade vem do `go-loghub-ident`
(`lhident.Initialize()` no `main`, uma vez, §10.1). `from` é o AgentUUID e
`space` o Workspace; `AgentName` e `Hostname` nomeiam as conexões dos drivers
(`whs/<agent_name>@<hostname>`), e o AgentUUID nomeia o dump (§9.1). A
identidade é persistida em `ring.data_dir` (exportado como `DATADIR`) e fica
congelada até o fim do processo: o `SIGHUP` não a recarrega.

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
  precisa dos bytes do JLH usa Redis, NATS ou SQLite (o `JSON` do MySQL e o do
  ClickHouse, §3.9, também normalizam).
- **NUL:** o `jsonb` recusa o escape `\u0000` (`22P05`), e a classe `22`
  descartaria o lote inteiro. O corpo nunca o tem (§3.2), mas um `%00` na query
  o produz. Na cópia que vai para a coluna, cada escape `\u0000` vira `\ufffd`;
  um `\\u0000` (barra literal) fica como está. O JLH do ring e dos outros
  destinos continua íntegro. Sem o escape — quase sempre — a coluna recebe um
  sub-slice do JLH, sem cópia.
- **As CHECKs são contrato:** um `webhook.request` tem `level` 9, `from` UUIDv7 e
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

### 3.7 Tabela `inbox` (`sink=sqlite` e `sink=mysql`)

SQLite e MySQL **espelham** a `loghub.inbox`: as mesmas onze colunas, com os
tipos que cada banco tem, a mesma chave (`uuid`) e as CHECKs que cada um
expressa — o MySQL confere a versão 7 de `uuid` e `from_uuid`, mas não a
gramática de `space` nem `event_at >= epoch`; o SQLite, nenhuma das três. Nenhuma
recusa o que este servidor escreve. Um
consumidor escrito para a fila do PostgreSQL lê as três com o mesmo mapeamento
(§3.6). Diferente da fila, o `payload` aqui é o texto do JLH, byte a byte (o
SQLite guarda `TEXT`; o JSON do MySQL normaliza como o `jsonb`).

```sql
-- SQLite (criado pelo próprio servidor no events.db, §8.3)
CREATE TABLE IF NOT EXISTS inbox (
  uuid        TEXT    NOT NULL PRIMARY KEY,   -- canônico, minúsculo
  from_uuid   TEXT    NOT NULL,
  event_at    TEXT    NOT NULL,               -- 'YYYY-MM-DDTHH:MM:SS.ffffffZ' (UTC, µs)
  received_at TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  sid         INTEGER NOT NULL DEFAULT 0  CHECK (sid >= 0),
  step        INTEGER NOT NULL DEFAULT 0  CHECK (step BETWEEN 0 AND 65535),
  level       INTEGER NOT NULL            CHECK (level BETWEEN 0 AND 15),
  event_nsec  INTEGER NOT NULL DEFAULT 0  CHECK (event_nsec BETWEEN 0 AND 999),
  space       TEXT    NOT NULL DEFAULT 'default',
  type        TEXT    NOT NULL            CHECK (type <> ''),
  payload     TEXT    NOT NULL            CHECK (json_type(payload) = 'object')
) STRICT;

-- Batimento da sonda de saúde (§8.3.5): uma linha, reescrita no lugar.
CREATE TABLE IF NOT EXISTS health (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  ts INTEGER NOT NULL
) STRICT;
```

```sql
-- MySQL 8.0.16+ (docker/mysql/inbox.sql; o servidor nunca a cria)
CREATE TABLE IF NOT EXISTS inbox (
  uuid        BINARY(16)   NOT NULL,
  from_uuid   BINARY(16)   NOT NULL,
  event_at    DATETIME(6)  NOT NULL,             -- UTC
  received_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  sid         BIGINT       NOT NULL DEFAULT 0,
  step        INT          NOT NULL DEFAULT 0,
  level       SMALLINT     NOT NULL,
  event_nsec  SMALLINT     NOT NULL DEFAULT 0,
  space       VARCHAR(64)  NOT NULL DEFAULT 'default',
  type        VARCHAR(64)  NOT NULL,
  payload     JSON         NOT NULL,
  CONSTRAINT inbox_pk PRIMARY KEY (uuid),
  CONSTRAINT inbox_uuid_v7_ck CHECK ((ascii(substr(uuid, 7, 1)) >> 4) = 7),
  CONSTRAINT inbox_from_v7_ck CHECK ((ascii(substr(from_uuid, 7, 1)) >> 4) = 7),
  CONSTRAINT inbox_type_ck    CHECK (type <> ''),
  CONSTRAINT inbox_level_ck   CHECK (level BETWEEN 0 AND 15),
  CONSTRAINT inbox_sid_ck     CHECK (sid >= 0),
  CONSTRAINT inbox_step_ck    CHECK (step BETWEEN 0 AND 65535),
  CONSTRAINT inbox_nsec_ck    CHECK (event_nsec BETWEEN 0 AND 999),
  CONSTRAINT inbox_payload_ck CHECK (json_type(payload) = 'OBJECT')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

- **SQLite:** `event_at` é texto ISO-8601 de largura fixa (27 caracteres), que
  ordena como o tempo e é legível; `received_at` vem do `strftime` do próprio
  SQLite (ms). `uuid` em texto canônico minúsculo ordena como os bytes.
- **MySQL:** `BINARY(16)` porque não há tipo `uuid`; a chave clusterizada do
  InnoDB recebe os inserts no fim do índice (UUIDv7). `DATETIME(6)` em UTC: a
  sessão do servidor fixa `time_zone = '+00:00'`, então o `CURRENT_TIMESTAMP(6)`
  de `received_at` também é UTC. `JSON` valida e normaliza o documento.
  `utf8mb4_bin` compara `space` e `type` byte a byte.
- **Idempotência:** `INSERT … ON CONFLICT (uuid) DO NOTHING` no SQLite e
  `INSERT … ON DUPLICATE KEY UPDATE uuid = uuid` no MySQL (§8.6.3).
- A tabela `events` da v1 não é mais escrita nem lida; num `events.db` antigo
  ela fica como está, ao lado da `inbox`.

O que o servidor **exige** do MySQL (§8.6.4): motor InnoDB; as dez colunas que
ele escreve com as larguras da DDL ou maiores — `binary(16)` em `uuid` e
`from_uuid`; `datetime(6)` em `event_at` (e em `received_at`, se a coluna
existir); `bigint` em `sid`, `int` ou maior em `step`, `smallint` ou maior em
`level` e `event_nsec` (`UNSIGNED` é aceito: todo valor escrito é ≥ 0);
`varchar`, `char` ou `text` de ao menos 64 caracteres em `space` e `type`;
`json` em `payload`; um índice **único** exatamente sobre a coluna `uuid`
inteira e **nenhum outro índice único**, salvo o de uma única coluna
`AUTO_INCREMENT`; `INSERT` e `UPDATE`; um `max_allowed_packet` que comporte o
maior envelope (§3.2). MariaDB não é suportado (§8.6).

### 3.8 Mensagem NATS (`sink=nats`)

```
Subject:  loghub.<space>.webhook.request       (sink.nats.subject = loghub.{space}.{type})
Headers:  Nats-Msg-Id:     <uuid>.<step>
          Content-Type:    application/json
          X-LogHub-Type:   webhook.request
          X-LogHub-UUID:   <uuid>
          X-LogHub-Space:  <space>
Data:     <jlh>
```

- `Data` é o JLH byte a byte; o corpo está dentro dele. Nenhum header carrega o
  envelope (teto de 64 KiB de headers do JetStream).
- `Nats-Msg-Id` é `<uuid>.<step>`, **nunca** o `uuid` puro: um processador que
  encaminhe para o mesmo stream (mesmo `uuid`, `step + 1`) teria o salto
  legítimo descartado como duplicata.
- `{space}` e `{type}` são expandidos **por envelope**; os headers `X-LogHub-*`
  ajudam o roteamento, mas o envelope é a fonte da verdade.
- O stream de exemplo (`docker/nats/stream.json`) cobre o subject do Workspace e
  **nunca** usa `loghub.>`, que capturaria o plano de controle (`loghub._ctl`).
  Um backlog de outro Workspace (`ring.db` ou dump) exige que o stream cubra
  também o subject do `space` antigo até drenar (§8.7.5).
- O boot recusa (§8.7.5) um `max_payload` ou `max_msg_size` menor que o maior
  envelope (§3.2) mais 512 bytes de headers.

### 3.9 Tabela `inbox` (`sink=clickhouse`)

O ClickHouse também **espelha** a `loghub.inbox` (§3.6): as mesmas onze colunas,
o mesmo mapeamento do envelope, com os tipos do ClickHouse. Ela não é uma fila —
o ClickHouse não tem `DELETE … FOR UPDATE SKIP LOCKED` —, e sim o armazém
analítico dos envelopes, lido por tempo e por `space`/`type`. Como nas outras,
o servidor **nunca a cria** (§8.8.5): `docker/clickhouse/inbox.sql`.

| Coluna | Envelope | Tipo | Conversão |
|---|---|---|---|
| `uuid` | `uuid` | `UUID` | — |
| `from_uuid` | `from` | `UUID` | — |
| `event_at` | `event.uts` + `event.sub` | `DateTime64(6, 'UTC')` | truncado no µs, como em §3.6 |
| `received_at` | — | `DateTime64(6, 'UTC') DEFAULT now64(6)` | não é enviado |
| `sid` | `sid` | `UInt64` | — |
| `step` | `step` | `UInt16` | — |
| `level` | `level` | `UInt8` | — |
| `event_nsec` | `event.sub` | `UInt16` | `sub % 1000` |
| `space`, `type` | `space`, `type` | `LowCardinality(String)` | — |
| `payload` | `payload` | `JSON(params Map(String, String), query Map(String, Dynamic))` | o texto do JLH a partir de `"payload":`, como no MySQL, decomposto em subcolunas pelo servidor |

```sql
-- ClickHouse 25.8+ (docker/clickhouse/inbox.sql; o servidor nunca a cria)
CREATE TABLE IF NOT EXISTS inbox (
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
  payload     JSON(params Map(String, String), query Map(String, Dynamic))
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_at)
ORDER BY (event_at, uuid)
SETTINGS non_replicated_deduplication_window = 1000;
```

- **`ORDER BY (event_at, uuid)`, não `uuid`.** O ClickHouse ordena `UUID` pela
  **segunda** metade (decisão histórica da documentação do tipo): num UUIDv7 é
  a fração sub-µs e a parte aleatória, e o índice esparso de uma chave só com `uuid` não poda nada por
  tempo — toda consulta lê ao menos uma marca de cada parte. Um `uuid` repetido
  tem sempre o mesmo `event_at` (a mesma amostra de relógio, §3.1), então a
  chave colapsa exatamente as reentregas, e nada além delas, com o tempo na
  frente.
- **`ReplacingMergeTree`:** duas linhas com a mesma chave viram uma no merge.
  É a idempotência **eventual** do at-least-once: uma reentrega fora da janela
  do token (§8.8.4), como um dump reingerido horas depois, fica duplicada até o
  merge; a leitura exata usa `FINAL`. O token de §8.8.4 é a camada imediata.
- **`non_replicated_deduplication_window = 1000`.** Num `MergeTree` sem
  réplica o padrão é `0`, e o `insert_deduplication_token` é ignorado; o
  `Replicated*` já guarda os últimos blocos (`replicated_deduplication_window`).
- **`PARTITION BY toYYYYMM(event_at)`:** a retenção é `DROP PARTITION` (§8.8.9).
  A partição sai de `event_at` para que um `uuid` caia sempre na mesma — a
  deduplicação do ClickHouse, a do token e a do merge, é por partição.
- **Sem `CONSTRAINT … CHECK`.** O ClickHouse avalia a CHECK no `INSERT`, e uma
  violação (`469`) é `logic`: o lote inteiro. A sonda de boot é um `INSERT`
  vazio (§8.8.5), que não a exercita, então o boot **recusa** uma tabela com
  CHECK. As CHECKs de versão do PostgreSQL e do MySQL não fazem falta aqui: o
  `inboxRowOf` (§8.5.4) já recusa um `from` que não seja UUIDv7.
- **O `payload` não é fiel ao byte**, como o `jsonb` (§3.6): o tipo `JSON`
  achata o documento em caminhos e reordena chaves. Strings continuam
  strings: a inferência de data e de número a partir de texto é desligada em
  todo `INSERT` (§8.8.3), porque com ela uma string escolhida por um cliente
  reescrevia o dado de outras linhas do lote (revisão independente de
  2026-10-07). O corpo
  continua exato (`content.body` é uma string). NUL é aceito: um `\u0000` da
  query vai como está, sem a troca de §3.6 (medido).
- **`params` e `query` como `Map`.** O tipo `JSON` **descarta objeto vazio**
  (medido no 25.8, sem ajuste que o evite): sem os dois *hints*, `params` — que
  é sempre `{}` (§3.2) — e `query` de uma requisição sem query sumiam de toda
  linha lida de volta, e a linha deixava de ser um `webhook.request` válido. Um
  caminho tipado como `Map` sai sempre, `{}` inclusive. E são as **únicas
  chaves de objeto que o cliente controla** (`headers` e `network.meta` são
  arrays de `{name, value}`): como chaves de um `Map` elas ficam literais —
  `a.b`, `a%2Eb`, a chave vazia, o NUL de um `%00` —, sem virar caminho. Sem o
  *hint*, `?a=2&a.b=1` era gravada e lida como `{"a":"2","a":{"b":"1"}}` (JSON
  com chave repetida), duas chaves que dessem o mesmo caminho recusavam o
  `INSERT` com `117`, e cada chave inventada por um varredor da internet virava
  um caminho dinâmico. `query` é `Map(String, Dynamic)` porque o valor é string
  ou array (§3.2). Decisão do usuário (2026-10-07): o *hint* é do formato do
  payload, e por isso a tabela é **por formato de payload** — numa tabela
  compartilhada com o `go-syslog-server` ele injetaria `"params":{},"query":{}`
  nas linhas de syslog, que tem a sua (`inbox_syslog`). O
  `go-webhook-fast-server` (`type=webhook`) tem o mesmo payload e o mesmo
  *hint*: os dois podem dividir a `inbox`, e o `type` os distingue.
- **Os dois ajustes de `JSON` continuam** em todo `INSERT` (§8.8.3), como
  defesa: `json_type_escape_dots_in_keys = 1` (um ponto numa chave de caminho
  vira `%2E` e volta na leitura com o mesmo ajuste) e
  `type_json_skip_duplicated_paths = 1`. Fora de `params` e `query` o
  `webhook.request` não tem chave que venha do cliente, então hoje eles não
  mudam nada; um payload futuro com chaves livres fora de um *hint* não
  derrubaria lotes por isso.

O que o servidor **exige** da tabela (§8.8.5): motor `*ReplacingMergeTree` sem coluna `is_deleted`; partição, se houver, só
por `event_at`;
`uuid` como elemento da chave de ordenação; as dez colunas que ele escreve com
os tipos da DDL, colunas comuns (`DateTime64` com precisão de 6 a 9 e qualquer fuso;
`LowCardinality(String)` ou `String` em `space` e `type`; `JSON` com os dois
*hints* em `payload` e, além deles, só `max_dynamic_paths`/`max_dynamic_types`),
`received_at`, se existir, `DateTime64` de ao menos 6
dígitos com `DEFAULT` ou
`MATERIALIZED`; nenhuma CHECK; ClickHouse 25.8 ou mais novo;
`INSERT` nas dez colunas e uma sessão gravável; um `max_memory_usage` que
comporte o pior lote.

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
- **Método fora de `^[A-Z]+$` → `405`.** O schema do `webhook.request` exige
  maiúsculas, e converter (`ToUpper`) mentiria sobre o que chegou.
- **`OPTIONS *` (alvo sem `/`) → `400`.** O schema exige `path` começando com
  `/`, e um `*` não descreve recurso nenhum. O `http.Server` sobe com
  `DisableGeneralOptionsHandler`: sem isso o `net/http` responderia ao
  `OPTIONS *` sozinho, `200` sem corpo, antes do handler.

`HEAD` entra no ring normalmente; a resposta não carrega corpo (o `net/http`
suprime automaticamente).

### 4.3 Pipeline de uma requisição

Ordem estrita. A primeira condição que falha encerra o processamento.

```
1.  accept                     -> teto server.max_conns             (se >0)
2.  leitura de headers         -> read_header_timeout, max_header_bytes -> 431
3.  URI                        -> max_uri_bytes                      -> 414
4.  método CONNECT ou fora de ^[A-Z]+$                               -> 405
    alvo sem "/" (OPTIONS *)                                         -> 400
5.  resolve IP de origem       (§5)
6.  rate-limit                 (§6, desligado por padrão)            -> 429
7.  Content-Length declarado > max_body_bytes                        -> 413
8.  sink DOWN                                                        -> 503
9.  ring saturado (checagem rápida, otimização)                      -> 429
10. UMA amostra de relógio t: uuid = GenerateAt(Level3, t), event = t
11. lê corpo com http.MaxBytesReader (read_timeout)                  -> 413
12. pré-passada no corpo (utf-8 ou base64, §3.2); serializa o JLH
    UMA vez num buffer do pool; o buffer do corpo volta ao pool
13. entrega ao ring, não-bloqueante                                  -> 429
14. aguarda a aceitação conforme §2.2
15. 200 OK, X-LogHub-UUID: <uuid>, corpo JSON (§4.4)
```

**Rejeições nos passos 4–9** (antes de ler o corpo) drenam e descartam até
`server.reject_drain_bytes` (64 KiB) do corpo antes de responder, para que um
cliente HTTP/1.1 que ainda está enviando consiga **ler a resposta** na mesma
conexão. Se o corpo exceder isso, responde com `Connection: close`.

Sem esse dreno, o cliente recebe `ECONNRESET` em vez do `429`, não vê o
`Retry-After` e retenta sem backoff — piorando exatamente a sobrecarga que o
rate-limit deveria conter.

### 4.4 Respostas

Sucesso (skill `loghub-type-webhook-schema`):

```
HTTP/1.1 200 OK
X-LogHub-UUID: 01a1122b-8b69-73a4-b9d1-074a5fb34be2
Content-Type: application/json

{"uuid":"01a1122b-8b69-73a4-b9d1-074a5fb34be2","mode":"bucket","status":"stored"}
```

`mode` é o do payload (§3.2); `status` é `accepted` com `mode=fast` e `stored`
com `mode=bucket`. O `X-Event-Id` da v1 não existe mais. O corpo é montado de um
molde fixo, sem `encoding/json`.

Depois que o UUID existe (passos 13–14), as falhas levam o mesmo cabeçalho e um
corpo JSON com `"status":"error"`, para que o cliente correlacione com o log do
servidor; o envelope foi descartado, e um reenvio gera outro UUID. Antes do
passo 10 não há UUID, e o erro é `text/plain; charset=utf-8`, uma linha:

| Código | `reason` da métrica | Quando | `Retry-After` | UUID |
|---|---|---|---|---|
| `400` | `bad_request` | Requisição malformada; alvo sem `/` | — | não |
| `405` | `bad_request` | `CONNECT`; método fora de `^[A-Z]+$` | — | não |
| `413` | `too_large` | Corpo acima de `max_body_bytes` | — | não |
| `414` | `bad_request` | URI acima de `max_uri_bytes` | — | não |
| `429` | `ratelimit` | Rate-limit excedido | segundos até o bucket ter 1 token | não |
| `429` | `ring_full` | Ring cheio (itens ou bytes), na checagem rápida | `1` | não |
| `429` | `ring_full` | Ring cheio ou `max_inflight` cheio (`ring=none`), no `Enqueue` | `1` | sim |
| `431` | `bad_request` | Headers acima de `max_header_bytes` | — | não |
| `500` | — | Recusa permanente deste evento pelo sink (`logic`: §8.3.4, §8.5.7) | — | sim |
| `503` | `sink_down` | Sink marcado DOWN (§10.2) | `5` | não |
| `503` | `sink_down` | Falha de escrita com `ring=none` | `5` | sim |
| `503` | `shutdown` | Servidor em drenagem | `5` | não / sim no `Enqueue` |

O `429` para ring cheio é da v1 e continua: a skill pede `503`, mas o `429` com
`Retry-After` já é o contrato de backpressure dos emissores desta versão (o
`go-webhook-fast-server` segue o mesmo, até a sua etapa de *stage*).

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
rate-limit, logs e auditoria de uma vez.

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

**O proxy confiável precisa reescrever o que o servidor lê dele.** De um par
confiável, um `X-Forwarded-For` não vazio vence o `X-Real-IP` (§5.1), e os
demais cabeçalhos de proxy de §3.2 (`X-Forwarded-Proto/-Host/-Port`,
`X-Client-Port`, `X-Server-Addr`, fingerprints) valem como chegaram. Um proxy
em `proxy.trusted` que só preenche o `X-Real-IP` e repassa o `X-Forwarded-For`
do cliente (um nginx com `proxy_set_header X-Real-IP $remote_addr` e sem
`$proxy_add_x_forwarded_for`) deixa o cliente escolher `network.ip.src`, a chave
do rate-limit e o `ratelimit.exempt`; o mesmo vale para cada cabeçalho que ele
não sobrescreve. A precedência não se inverte: com o `X-Real-IP` na frente, o
furo seria o simétrico, num proxy que anexa ao `X-Forwarded-For` e repassa o
`X-Real-IP` do cliente (o `option forwardfor` do HAProxy). `peer-addr` registra
o proxy em qualquer caso.

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

## 6. Rate-limit

### 6.1 Padrão: DESLIGADO

`ratelimit.enabled = false`.

Motivo: com `proxy.trusted` no padrão loopback, um operador que esqueça de
configurá-lo faz **todo** o tráfego resolver para o IP do proxy — um único
prefixo `/24`, limitado a `ratelimit.rate`. Isso é um auto-DoS: 100 req/s
globais para o mundo inteiro. Um rate-limiter com resolução de IP mal
configurada é pior que nenhum rate-limiter.

**Obrigatório:** no boot, se `ratelimit.enabled = false` **e** `server.bind` não
for loopback, emitir uma linha `WARN` explícita informando que o serviço está
exposto sem proteção de taxa.

### 6.2 Algoritmo

Token bucket por prefixo de rede.

- Chave: `src-ip-addr` mascarado — `/24` para IPv4, `/48` para IPv6, ambos
  configuráveis (`ratelimit.v4_mask`, `ratelimit.v6_mask`).
- Estado por chave: `tokens float64` + `last_ns int64` (16 bytes + overhead de mapa).
- `ratelimit.rate` tokens/s de reposição, teto `ratelimit.burst`.
- Mapa dividido em shards (`nextPowerOfTwo(GOMAXPROCS * 4)`) com um mutex por
  shard, indexado por hash da chave.

### 6.3 Evicção

Um bucket que reencheu até `burst` é **indistinguível de um bucket
inexistente**. Um sweep periódico (`ratelimit.sweep_interval`, padrão 30s)
percorre os shards e apaga todos os buckets cheios.

Sem LRU, sem heurística de idade, sem perda de precisão — a regra é exata.

`ratelimit.max_buckets` (padrão 1.000.000, ~48 MiB) é rede de segurança: ao
atingir o teto, novas chaves passam sem limite e `whs_ratelimit_buckets` fica no
máximo, tornando a saturação visível.

### 6.4 Isenções

`ratelimit.exempt` — lista de CIDRs isentos, vazia por padrão. Para
health-checkers e emissores conhecidos de alto volume.

---

## 7. Ring

### 7.1 Interface

```go
type Ring interface {
    // Enqueue aceita o item. Não bloqueia por capacidade: se estiver cheio,
    // retorna ErrFull imediatamente. Bloqueia apenas pela garantia do modo.
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

`Enqueue` só retorna `nil` quando a garantia do modo foi cumprida (§2.2). O `200`
é enviado depois desse retorno.

### 7.2 Saturação

Dois tetos, avaliados simultaneamente:

- `ring.capacity_items` (padrão 65536)
- `ring.max_bytes` (padrão 512 MiB), contando envelope + corpo

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

Este é o modo de maior vazão e o padrão.

### 7.4 `ring.mode = sqlite`

Ring persistente, para quem quer **retenção máxima mesmo ao custo de
performance**.

**Garantia:** o evento está em disco com `COMMIT` concluído antes do `200`. Zero
perda sob crash do processo; zero perda sob queda de energia se
`ring.sqlite.synchronous = FULL`.

#### Esquema

```sql
CREATE TABLE IF NOT EXISTS jlh_ring (
  rowid_  INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid    TEXT    NOT NULL,   -- o uuid do envelope, para log e Commit
  jlh     BLOB    NOT NULL,   -- o envelope JLH, byte a byte (§3.1)
  nbytes  INTEGER NOT NULL    -- len(jlh)
) STRICT;
```

Linhas são **removidas** após confirmação no sink — esta tabela é uma fila, não
um arquivo. Não confundir com `inbox` (§3.7), que é destino final.

O ring guarda o JLH pronto, nunca a requisição crua: a descarga entrega
exatamente o envelope cujo UUID o cliente recebeu. Nada é promovido a coluna:
os destinos que precisam de campos do cabeçalho os leem do próprio JLH (§12.1).

**`ring.db` de antes do LogHub.** A tabela `ring` da v1 guardava o envelope
antigo e o corpo à parte, que nenhum destino entende mais. No boot: vazia, ela é
removida; com linhas, ela **fica** (nada é perdido nem misturado), e o boot
registra um `ERROR` com a contagem, dizendo para drená-la com a versão anterior.
O ring novo segue na `jlh_ring`.

#### DSN

```
file:<ring.sqlite.path>
  ?_pragma=journal_mode(WAL)
  &_pragma=synchronous(FULL)
  &_pragma=busy_timeout(5000)
  &_pragma=wal_autocheckpoint(1000)
  &_txlock=immediate
```

#### Concorrência

**Exatamente duas goroutines** tocam o banco:

- **Escritora** — dona da conexão de escrita. Recebe os itens dos handlers,
  agrupa `INSERT`s e faz um `COMMIT` a cada `ring.sqlite.commit_batch` (256)
  itens **ou** `ring.sqlite.commit_max_delay` (5ms), o que vier primeiro. Só
  após o `COMMIT` os handlers do grupo são liberados para responder `200`.
  Também executa os `DELETE` dos lotes já confirmados no sink, enfileirados pela
  leitora — inclusive, ao fechar, os que ainda estão na fila: até 2026-10-11 a
  escritora saía sem eles, e as linhas já entregues voltavam no boot seguinte
  como duplicatas (TESTING §8 item 40).
- **Leitora** — conexão separada (o modo WAL permite leitor concorrente com
  escritor). `SELECT rowid_, uuid, jlh FROM jlh_ring WHERE rowid_ > ? ORDER BY
  rowid_ LIMIT n` e entrega ao canal dos workers de dreno.

Mais de um escritor só geraria `SQLITE_BUSY`: o SQLite serializa escritores por
construção. Uma coluna de `claim` foi descartada por dobrar as escritas (um
`UPDATE` + um `DELETE` por item) num caminho onde cada `COMMIT` custa um `fsync`.

**Cancelamento durante o group commit.** Uma vez entregue à escritora, o
`Enqueue` espera o resultado do `COMMIT` mesmo que o contexto da requisição seja
cancelado no meio — cliente que desligou, ou o `srv.Close()` do segundo sinal.
A espera é limitada pelo próprio group commit (`commit_max_delay` mais um
`fsync`). Devolver antes teria duas consequências: o handler responderia `503`
para uma linha que será commitada de qualquer forma, e devolveria ao pool um
buffer que a escritora ainda vai ler no `INSERT` — a violação de §12.3 que
coloca o corpo de um cliente dentro do evento de outro. A única saída antecipada
é o próprio ring fechar sem ter recebido o item, que devolve `ErrClosed`.

#### Custo de latência do `synchronous=FULL`

`FULL` faz `fsync` a cada `COMMIT`. Em NVMe local, um `fsync` custa ~50–200µs e
o group commit de 5ms domina: p99 do `200` fica em ~5–10ms. Em armazenamento de
rede (EBS gp3, disco de rede) o `fsync` custa 1–10ms e a p99 vai a 10–40ms.

`ring.sqlite.synchronous` é configurável (`FULL` | `NORMAL` | `OFF`). `NORMAL`
sobrevive a crash do processo mas não a queda de energia, com p99 muito menor. O
padrão `FULL` é a escolha deliberada de durabilidade sobre latência.

#### Boot

O backlog da execução anterior é drenado **integralmente** antes de a porta
pública abrir (§10.1).

**Com `sink.type = none` o backlog de uma vida anterior fica.** Drená-lo para
um sink que descarta por configuração (§8.4) destruiria eventos que uma vida
com destino respondeu com `200` — o teste de carga no mesmo volume depois de
um incidente, que fez a regra do dump (§9.2). As linhas que o boot encontra
ficam no `ring.db`, intocadas: o cursor dos workers começa depois da maior
`rowid_` vista no boot (a `AUTOINCREMENT` põe toda linha nova depois dela), e
elas ficam fora do `Stats()` — capacidade, `/readyz`, gauges `whs_ring_*` —,
porque esta vida nunca as entrega. O boot as informa num `WARN` com a contagem
e o caminho, e o `WARN` do shutdown as soma às que ficam (§10.3). Uma vida com
destino drena todas — inclusive as que uma vida com `sink=none` deixou, que
são indistinguíveis (só uma saída que corta o dreno as deixa — `kill -9`,
segundo sinal —: o descarte esvazia o ring), como num dump. O ring não sabe por quê: quem pede é a `App`, o único lugar que
conhece os dois eixos (§17 item 12). Até 2026-10-11 o dreno do boot as
entregava ao descarte (TESTING §8 item 42).

**Um `ring.db` encontrado por uma vida com outro `ring.mode`.** Só
`ring.mode = sqlite` cria o arquivo e o usa como fila, mas a vida anterior pode
ter saído com linhas nele — destino fora, `kill -9`, o `WARN` "events left in
ring.db for the next boot" do shutdown —, e até 2026-10-11 um boot com
`memory` ou `none` nunca as lia nem as informava (TESTING §8 item 42). Agora,
no passo 5 de §10.1, todo boot com outro ring procura o arquivo em
`ring.sqlite.path` (o padrão deriva de `ring.data_dir` em qualquer modo; um
caminho trocado na vida `sqlite` precisa continuar na configuração para ser
achado):

- o arquivo é sondado **sem criar arquivo nem tabela**, e só tem um ring o
  banco que tem a `jlh_ring` ou a tabela `ring` da v1. Qualquer outro não é
  tocado — o `events.db`, quando `ring.sqlite.path` e `sink.sqlite.path`
  coincidem sob outro ring, o que a validação só recusa com `ring=sqlite`
  (§10.1);
- as linhas da `jlh_ring` são entregues ao sink como o backlog do próprio
  `ring=sqlite`: porta pública fechada, `/readyz` em "draining backlog: N
  pending", uma recusa do sink encerra o boot com 3 e um `SIGTERM` com 130, e
  nos dois casos as linhas ficam (§9.2);
- sem nada pendente, o arquivo é **removido** — `-wal` e `-shm` primeiro e o
  `ring.db` por último, para nunca deixar um WAL velho ao lado de um banco que
  não existe mais. Vale também para o `ring.db` vazio de um shutdown limpo,
  com qualquer sink. Uma falha na remoção é um `ERROR`, e o boot segue: tudo
  já foi entregue;
- **só um arquivo que é só do ring é removido.** Um banco que tem, além do
  ring, outras tabelas (fora as internas `sqlite_*`) é de outro também: o
  `events.db` de uma vida que rodou `ring=sqlite` com `ring.sqlite.path`
  apontado para ele, e que depois volta a ser o destino de uma vida com os dois
  caminhos iguais. As linhas da `jlh_ring` são entregues e o arquivo fica,
  com um `WARN`. A primeira versão desta correção o removia — o destino
  inteiro, de baixo do sink que o tinha aberto, com saída 0 (revisão
  independente desta rodada);
- a tabela `ring` da v1 com linhas fica intocada, com o `ERROR` de sempre, e
  com ela o arquivo;
- com `sink.type = none` as linhas não são entregues e o arquivo que as tem
  fica: `WARN`, como acima;
- um arquivo que não se lê ou não se abre (permissão, não é SQLite, não é
  arquivo regular) é um `ERROR` que o nomeia, e o boot segue sem entregá-lo,
  como o `emergency.dir` de uma vida que não escreve dumps (§9.2).

Um `ring.db` é de uma instância só: `ring.data_dir` guarda também a identidade
dela (§3.5), e duas vidas simultâneas no mesmo arquivo entregariam as mesmas
linhas duas vezes — e a remoção o tiraria de baixo da outra.

### 7.5 `ring.mode = none`

Sem buffer. O handler executa a escrita no sink **de forma síncrona** e só
responde `200` após a confirmação.

- Com `sink=redis`: o `200` significa que o `XADD` foi confirmado pelo Redis.
- Com `sink=sqlite`: o `200` significa que o `COMMIT` no `events.db` terminou.
- Com `sink=postgres`: o `200` significa que o servidor confirmou o `INSERT`,
  com a durabilidade que **ele** estiver configurado para dar (§8.5.6).
- Com `sink=none`: o `200` é imediato, nada é escrito.

Throughput continua alto com `sink=redis` porque o **auto-pipelining do rueidis**
agrupa as chamadas concorrentes de várias goroutines num único syscall de
escrita — não há um round-trip por requisição.

Com `sink=postgres` **não há** coalescência: cada handler ocupa uma conexão do
pool pelo tempo do seu `INSERT`, e a vazão fica presa a
`pool_size / latência de commit`. Um `max_inflight` acima do pool não amplia
nada — as goroutines excedentes esperam conexão **dentro do pool**, que é uma
fila sem contabilidade nenhuma, exatamente o que este semáforo existe para
impedir. A validação avisa no boot (§8.5.3, §10.1).

**Backpressure:** semáforo de `ring.max_inflight` (padrão 2048) escritas
simultâneas. Ao encher, `429 ring_full` — mantendo a mesma semântica de "cheio =
429" dos outros modos.

Sem o semáforo, um sink lento faria o número de goroutines bloqueadas crescer
sem teto, cada uma segurando até 5 MiB de corpo — exatamente o estouro de
memória que o ring existe para evitar.

A escrita síncrona é limitada por `sink.op_timeout`, num contexto **desligado
do cancelamento** da requisição (`context.WithoutCancel`). Um cliente que
desliga no meio não aborta a escrita: o cliente Redis pode já ter enfileirado o
comando e o enviaria de qualquer forma, e abortar só deixaria o handler devolver
ao pool um buffer que o pipeline ainda vai serializar (§12.3). A escrita termina
ou estoura o prazo no seu próprio relógio.

Erro de escrita ou estouro de `sink.op_timeout` → `503`.

`Stats()` reporta `items` = in-flight e `capItems` = `max_inflight`, para as
métricas e o `/readyz` funcionarem sem caso especial.

Com `sink=none`, o semáforo é dispensado: não há nada que possa demorar.

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

Os workers de dreno (`sink.workers`, padrão 8) são **idênticos** para todos os
sinks:

1. `Dequeue` até `sink.batch` (256) itens, esperando no máximo
   `sink.batch_max_delay` (5ms) para completar o lote.
2. `sink.Write(ctx, items)`.
3. Em sucesso: `ring.Commit(items)`, devolve os buffers ao pool,
   `whs_events_written_total += n`.
4. Num `logic`, o lote sai do ring do mesmo jeito — `ring.Commit(items)`
   **antes** de devolver os buffers (§12.3) — e é perda contada em
   `whs_events_dropped_total{reason="sink_logic"}` e num `ERROR` com os `uuid`.
   **Entrega parcial:** um destino que recusa uma mensagem do lote e guarda as
   outras (`sink=nats`, §8.7.2) diz quais guardou (`Error.Delivered`); só as
   recusadas contam como perda, e as guardadas contam em
   `whs_events_written_total`. Sem essa lista, o lote inteiro é perda. A escrita
   direta de `ring=none` (§7.5) conta do mesmo jeito.

### 8.2 `sink.type = redis`

Destino padrão. Alimenta o `webhook-worker` do ecossistema.

#### 8.2.1 Cliente

`github.com/redis/rueidis`. Auto-pipelining ligado (padrão).

Corpo cru enviado com `rueidis.BinaryString([]byte)` — converte `[]byte` para
`string` sem cópia, e o RESP transporta bulk strings binary-safe.

Knobs de memória do rueidis expostos: `sink.redis.ring_scale_each_conn`,
`sink.redis.pipeline_multiplex`, `sink.redis.read_buffer_each_conn`,
`sink.redis.write_buffer_each_conn`.

#### 8.2.2 Topologias

- **Standalone** — `sink.redis.addrs` com um endereço (TCP ou socket unix).
- **Sentinel** — `sink.redis.addrs` com os sentinelas + `sink.redis.sentinel_master`.
- Auth ACL (`sink.redis.username` / `sink.redis.password`), `sink.redis.db`.
- TLS opcional (`sink.redis.tls`, `.tls_ca`, `.tls_insecure`). Diferente do HTTP,
  aqui o proxy reverso não resolve, e Redis gerenciado em nuvem exige TLS.

**Cluster fora de escopo na v1**: um stream vive num único slot, então cluster
não traz vazão nenhuma — só complexidade de teste e documentação.

#### 8.2.3 Escrita

```go
cmds := make(rueidis.Commands, 0, len(items))
for _, it := range items {
    h, _ := envelope.ParseHead(it.Env)          // space e type do próprio JLH
    b := c.B().Xadd().Key(streamFor(h)).Id("*"). // loghub.{space}.{type}, §3.3
        FieldValue().
        FieldValue("e", rueidis.BinaryString(it.Env))
    cmds = append(cmds, b.Build())
}
resps := c.DoMulti(ctx, cmds...)
```

Um JLH que o leitor do cabeçalho recusa não vira comando: o lote falha como
`logic` (o dado, não o destino).

> **Armadilha obrigatória de documentar:** o rueidis **recicla os comandos para
> um `sync.Pool`** após a execução. Um lote que falhou tem os comandos já
> reciclados — reusar o slice `cmds` num retry corrompe os dados
> **silenciosamente**. O worker retém os **itens** e **reconstrói** os comandos a
> cada tentativa. (`Build().Pin()` é a alternativa, mas reconstruir é mais
> simples e evita o risco de esquecer um `Pin`.)

#### 8.2.4 Classificação de erro

| Situação | Classe | Efeito |
|---|---|---|
| sonda falha por rede / erro de rede em `DoMulti` | `ErrNetwork` | Alimenta a máquina de estados (§10.2) |
| `OOM command not allowed...` | `ErrFatal` | FATAL imediato — o Redis está vivo mas não aceita escrita |
| `WRONGTYPE` | `ErrFatal` | FATAL imediato — a chave da stream é outro tipo. Não muda sozinho, e **todo** lote falharia igual |
| `NOPERM`, `NOAUTH`, `WRONGPASS` | `ErrFatal` | FATAL imediato — credencial ou ACL. Idem: não muda sozinho |
| `LOADING`, `BUSY`, `MASTERDOWN`, `READONLY`, `CLUSTERDOWN`, `TRYAGAIN` | `ErrNetwork` | Vivo mas temporariamente incapaz; retentar é o certo, e se persistir os fusíveis de §10.2 escalam |
| qualquer outra resposta de erro | `ErrNetwork` | Descartar por palpite é pior que retentar um lote sem esperança: o ring enche, o `429` aparece e o operador vê |

O Redis **não tem classe `logic`**: o `XADD` aceita qualquer sequência de bytes,
então nada que um servidor vivo responda é uma recusa *deste* evento em
particular — é uma recusa de todos, e isso é fatal. A tabela é a mesma do
serviço irmão. Classificar `WRONGTYPE` ou `NOPERM` como `logic` faria o
processo descartar um lote a cada 5 ms, depois do `200`, para sempre, com uma
sonda de leitura dizendo que está tudo bem. Com `ring=none` um erro fatal chega
ao cliente como `503 sink_down` (o processo está encerrando) e não como `500`.

**A sonda pergunta "consigo escrever?", não "você está vivo?".** Duas chamadas
num pipeline, a cada `sink.health_interval`, as mesmas do serviço irmão:

```
XLEN <sink.redis.stream>
XADD <sink.redis.stream>:healthprobe NOMKSTREAM * p 1
```

Um `PING` não serve: é leitura, e uma réplica promovida por engano responde a
ele com alegria enquanto recusa todo `XADD` com `READONLY` — o serviço ficaria
respondendo `429` com o sink eternamente UP. O `NOMKSTREAM` faz o `XADD`
devolver nulo sem criar a chave quando ela não existe, mas o comando passa
pelas mesmas verificações de uma escrita real (`READONLY`, `OOM`, `NOPERM`),
então a ACL do usuário precisa cobrir `<stream>:healthprobe`. Uma resposta de
classe fatal à sonda — `WRONGTYPE`, `NOAUTH` — é FATAL na primeira checagem,
sem passar pelos fusíveis de §10.2; o mesmo vale para a recusa devolvida na
discagem, porque o rueidis autentica dentro do `NewClient`: senha errada
encerra o boot na hora, não depois de `sink.boot_timeout`.

#### 8.2.5 Trimming

`sink.redis.maxlen = 0` — **desligado por padrão**.

Se o worker atrasa, o stream cresce e o alerta vem da métrica de `XLEN`. Se o
Redis atinge `maxmemory`, o `XADD` retorna `OOM`, que é fatal.

`MAXLEN ~ N` foi rejeitado como padrão porque descarta eventos
**silenciosamente** quando o consumidor atrasa — exatamente o cenário contra o
qual todo o resto do desenho (429, dreno, shutdown gracioso) foi construído.

Quando configurado (`> 0`), usa a forma aproximada `MAXLEN ~ N`, muito mais
barata que a exata.

### 8.3 `sink.type = sqlite`

Destino final autônomo. **Não requer Redis nenhum.** Esquema em §3.7 (a tabela
`inbox`, espelho da `loghub.inbox`).

#### 8.3.1 Concorrência

Uma **única** goroutine escritora, dona da conexão. `sink.workers` é **ignorado**
(forçado a 1) com `WARN` no boot se configurado acima disso — o SQLite serializa
escritores e goroutines extras só produziriam `SQLITE_BUSY`.

O lote inteiro vai numa **única transação**, com statement preparado reutilizado:

```
BEGIN IMMEDIATE
  INSERT INTO inbox (uuid, from_uuid, event_at, sid, step, level, event_nsec,
                     space, type, payload)
       VALUES (?,...) ON CONFLICT (uuid) DO NOTHING   -- N vezes
COMMIT
```

#### 8.3.2 DSN

```
file:<arquivo corrente>
  ?_pragma=journal_mode(WAL)
  &_pragma=synchronous(<sink.sqlite.synchronous>)
  &_pragma=busy_timeout(5000)
  &_pragma=wal_autocheckpoint(1000)
  &_txlock=immediate
```

#### 8.3.3 Rotação

`sink.sqlite.rotate = none | hourly | daily` (padrão `none`).

| Valor | Arquivo |
|---|---|
| `none` | `<sink.sqlite.path>` (ex.: `data/events.db`) |
| `daily` | `data/events-2026-09-06.db` |
| `hourly` | `data/events-2026-09-06-14.db` |

A rotação ocorre **entre lotes**, nunca no meio de uma transação: ao detectar a
virada do período, a escritora fecha o banco corrente e abre o próximo antes de
processar o lote seguinte. O período é avaliado em **UTC**.

Arquivar e podar vira mover arquivo — sem `DELETE` em massa (que trava escritas
num banco grande) e sem `VACUUM` completo (que exige o dobro do espaço em disco).

`sink.sqlite.keep_files = 0` (padrão) — **nunca apaga nada**. Quando `> N`, a
escritora remove os arquivos rotacionados mais antigos além dos N mais recentes,
logando cada remoção em `INFO`. O arquivo corrente nunca é removido.

#### 8.3.4 Classificação de erro

| Situação | Classe | Efeito |
|---|---|---|
| `SQLITE_BUSY`, `SQLITE_LOCKED` | `ErrNetwork` | Retry com backoff; alimenta a máquina de estados |
| `SQLITE_IOERR` | `ErrNetwork` | idem |
| `SQLITE_FULL` (disco cheio) | `ErrFatal` | FATAL imediato |
| `SQLITE_CORRUPT`, `SQLITE_READONLY` | `ErrFatal` | FATAL imediato — não se resolve sozinho |
| `SQLITE_CONSTRAINT` (uma CHECK de §3.7) | `ErrLogic` | O dado, não o destino: lote descartado e contado. Duplicata de `uuid` não chega aqui: o `ON CONFLICT (uuid) DO NOTHING` a absorve |

#### 8.3.5 `Health()`

Uma **escrita trivial numa tabela de heartbeat** a cada `sink.health_interval`:
`INSERT ... ON CONFLICT DO UPDATE` de uma linha única (`health(id = 1, ts)`),
sob a mesma trava da escritora. Custo constante, independente do tamanho de
`inbox`, e a pergunta certa — "ainda consigo escrever?" — que é a que um
volume desmontado, um arquivo somente leitura ou um disco cheio respondem.

`PRAGMA quick_check` roda **uma vez, no boot**, e só. Ele lê o arquivo inteiro
sob a trava da escritora; como o `events.db` cresce sem limite com
`rotate = none`, uma checagem periódica um dia passaria de
`sink.health_timeout`, o sink iria a DOWN, os fusíveis de §10.2 escalariam para
FATAL e o boot seguinte falharia do mesmo jeito, em laço. Um arquivo corrompido
no boot é erro fatal (`exit 3`).

### 8.4 `sink.type = none`

Descarta o lote e retorna `nil`. `Health()` sempre retorna `nil` — este sink
**nunca fica DOWN** e **nunca é FATAL**.

Existe para medição:

| Combinação | O que mede |
|---|---|
| `ring=memory` + `sink=none` | Custo **real** do pipeline inteiro — parse HTTP, resolução de IP, envelope, pool de buffers, enfileiramento, dreno — menos a I/O do destino. Isola o gargalo interno. |
| `ring=none` + `sink=none` | Teto absoluto de HTTP puro. Nada é enfileirado, nada é escrito. |
| `ring=sqlite` + `sink=none` | Custo isolado do ring persistente (o `fsync` do group commit). |

Com `ring != none`, o ring **opera normalmente**: o item entra, os workers o
retiram e descartam. Isso é deliberado — fazer bypass do ring produziria um
número bonito e enganoso, medindo um caminho que não existe em produção.

`whs_events_written_total` **conta** os eventos descartados por este sink (foram
"entregues" ao destino configurado). `whs_events_dropped_total` **não** é
incrementado: não houve perda, houve descarte por configuração.

Os dumps de emergência de uma vida anterior **não** são reingeridos com
`sink=none`: ficam no disco para uma vida com destino (§9.2). Nem as linhas
que uma vida anterior deixou no `ring.db`, em qualquer `ring.mode`: ficam no
arquivo, fora da conta do ring (§7.4).

### 8.5 `sink.type = postgres`

Destino final num PostgreSQL **compartilhado por todas as instâncias**. Esquema
em §3.6. É o primeiro destino que é ao mesmo tempo **remoto e com esquema**: o
Redis é remoto sem esquema, o SQLite tem esquema mas o processo é dono do
arquivo. Aqui o servidor não é dono de nada, e quase tudo que segue decorre
disso.

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

Cada worker ocupa uma conexão durante a escrita: é o oposto do SQLite (um
escritor) e do Redis (poucas conexões multiplexadas). Conexão é recurso escasso
e **compartilhado entre instâncias**:

| Grandeza | Valor |
|---|---|
| Workers por instância, padrão dos outros destinos | 8 |
| `max_connections` padrão do PostgreSQL | 100 |
| Instâncias que esgotam isso com 8 workers cada | 12 |

Por isso:

- **`sink.workers` padrão vira 4** quando o destino é postgres **e o operador
  não configurou o valor** (a origem do parâmetro é o default, §13.2). Quem
  escreveu o número manda, com `WARN` se passar de 16. O precedente é o SQLite,
  que força 1.
- **`sink.postgres.pool_size`**, padrão `0` = `sink.workers + 1`: uma conexão por
  worker mais uma de folga. O `Health()` **não** usa o pool: tem uma conexão
  própria, fora dele (§8.5.8), e a conta por instância é `pool_size + 1`. A
  folga ficou porque o pool abre conexões sob demanda (ela não custa nada até
  ser usada) e porque mudar a derivação mudaria o que toda configuração já
  implantada deriva. Um `pool_size` explícito **igual** a `sink.workers` é
  legítimo; **abaixo** dele os workers esperam conexão uns dos outros (vazão,
  não correção), e o boot avisa com `WARN` (padrões assumidos no
  porte da família syslog/RELP, 2026-10-13, contestáveis: com a sonda dentro do
  pool, escritores lentos que seguravam todas as conexões deixavam o `Health()`
  esperando além do `health_timeout` — DOWN e depois FATAL, `exit 3`, com o
  banco sadio; com `ring=none` nenhum `pool_size` evitava isso, porque os
  escritores são os handlers, até `ring.max_inflight`).
- Com `ring=none`, quem escreve são os handlers, limitados por
  `ring.max_inflight` (padrão 2048). Se esse número passar de `pool_size`, o
  excedente espera **dentro do pool**, fora de qualquer contabilidade. O boot
  avisa; a ocupação do pool é exposta em métrica (§8.5.10) para que a fila
  escondida seja visível.

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
comandos reciclados por biblioteca, como no rueidis (§8.2.3): um retry pode
reusá-los sem risco. A armadilha é outra, e é prima da de §12.3: o slice de
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
| Nenhuma `RULE` de `INSERT` ou `UPDATE` na tabela (as de `DELETE` e `SELECT` passam) | `pg_rewrite` (`ev_type` `2`, `3`) | exit 3, nomeando as regras — o `INSERT ... ON CONFLICT` é recusado numa tabela com elas (`0A000`): todo lote falharia com o sink UP (porte da família syslog/RELP, 2026-10-13; revisão do `go-relplog-pg-server`) |
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
o cancelamento do boot (um `SIGTERM` durante o `WaitReady`) continua chegando
por ele, inclusive depois de vencido o prazo da checagem. A mesma regra vale
para a validação do MySQL (§8.6.4) e do ClickHouse (§8.8.5); a do NATS (§8.7.5)
é uma ida e volta, como a própria checagem, e fica no prazo dela.

Até 2026-10-07 este parágrafo existia, mas o código validava o PostgreSQL e o
MySQL no prazo da checagem, o ClickHouse ignorava o cancelamento do boot, e o
texto seguia com o `max_execution_time`, o `452` e as *materialized views* do
ClickHouse, que foram para §8.8.5 (achado ao montar o `go-webhook-pg-server`).

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
dispensa a classificação por substring de mensagem de §8.3.4. A tabela abaixo
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
| `53100` `53200` `53400` | disco cheio, sem memória, limite de configuração | `ErrFatal` | FATAL — coerente com `OOM` do Redis |
| `28000` `28P01` `3D000` `42501` | credencial, banco inexistente, sem permissão | `ErrFatal` | FATAL — não se resolve sozinho |
| `42P01` `42703` `42P10` `42804` e o resto da classe `42` | tabela ou coluna sumiu, índice único ausente, contrato de esquema quebrado | `ErrFatal` | FATAL — `logic` descartaria um lote a cada 5 ms para sempre |
| `57P04` | `database_dropped` | `ErrFatal` | FATAL |
| **`23502`** | `not_null_violation`: o servidor nunca manda `NULL` nas dez colunas, então depois do boot ele é a **tabela** (coluna obrigatória acrescentada, `DEFAULT` retirado, *trigger* que anula) | `ErrFatal` | FATAL — como `logic` todo lote era descartado com o sink UP (porte da família syslog/RELP, 2026-10-13; o `1364` do MySQL já era fatal). Um *trigger* que anula uma coluna só em certas linhas passa a derrubar o processo, e isso fica escrito |
| **`0A000`** | `feature_not_supported`: o `ON CONFLICT` numa tabela com `RULE` de `INSERT`/`UPDATE` criada depois do boot (§8.5.5) | `ErrFatal` | FATAL — como desconhecido era retentado para sempre com o sink UP |
| classe `XX` | corrupção interna | `ErrFatal` | FATAL |
| classe `22` | dado recusado: codificação, tamanho | `ErrLogic` | `500`, log `ERROR`, lote descartado e contado |
| classe `23` (salvo `23502`) | `CHECK`, unicidade em **outro** índice (que o boot recusa, §8.5.5) | `ErrLogic` | idem — `23505` sobre `uuid` é impossível com o `ON CONFLICT`; um `23514` sem constraint nomeada é a partição ausente, `ErrFatal` |
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

**O check tem conexão própria, fora do pool** (porte da família syslog/RELP, 2026-10-13). Dentro do
pool ele disputava conexão com os escritores: `pool_size` escritores lentos —
um lock, um disco cheio de fila, nada falhando — seguravam todas, o check
esperava uma além do `sink.health_timeout` e o estado ia a DOWN e, passado o
`fail_timeout`, a FATAL, com dump e `exit 3` e o banco sadio. Com um ring,
`pool_size ≥ sink.workers + 1` guardava uma conexão; com `ring=none` os
escritores são os handlers, até `ring.max_inflight`, e nenhum `pool_size` que
a configuração exigisse a guardava. A conexão do check (`pgx.Conn`, com a
mesma configuração do pool) é aberta no primeiro check, fechada em qualquer
falha e reaberta no seguinte; uma conexão reaproveitada que falha é refeita
**uma vez** dentro do mesmo prazo, porque uma conexão parada um
`health_interval` inteiro pode ter morrido sob um *proxy* ou um reinício do
servidor (o pool escondia isso com o *ping* antes do reuso), e só a falha numa
conexão nova é a resposta. A validação de §8.5.5 continua no pool: roda no
boot, antes de qualquer escritor.

#### 8.5.9 Retenção

Tabela simples. O servidor **não apaga nada** e não particiona — coerente com o
`keep_files = 0` de §8.3.3. A poda é responsabilidade externa, e a forma barata
dela é particionar por faixa de `ts` e dropar partições, que é o mesmo raciocínio
da rotação de arquivo do SQLite. `DELETE` em massa numa tabela grande prende o
`VACUUM` e não é o que este desenho recomenda.

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

`sink.postgres.tls = false` por padrão, como no Redis (§8.2.2): o cenário base é
rede interna. Com `tls = true` o certificado é **verificado** contra o pool do
sistema ou contra `sink.postgres.tls_ca`, com `ServerName` igual ao host
configurado; `tls_insecure` desliga a verificação. Um `tls_ca` sem `tls` é erro de
configuração (exit 1), não aviso: quem aponta uma autoridade acredita que o TLS
está ligado.

### 8.6 `sink.type = mysql`

Destino final num MySQL 8.0.16+ **compartilhado por todas as
instâncias**. Esquema em §3.7. É o irmão do `sink=postgres`: remoto, com esquema,
de que o servidor não é dono. Tudo de §8.5 que não é citado aqui vale igual —
o servidor valida e nunca cria (§8.5.1), a conta de conexões (§8.5.3), a
durabilidade fora do processo (§8.5.6) e a retenção externa (§8.5.9). Esta
seção registra só o que o MySQL muda.

**MariaDB não é suportado** nesta versão: o `JSON` dele é apelido de `LONGTEXT`
(o `information_schema` responde `longtext` até para a DDL de
`docker/mysql/inbox.sql`), o espelho perderia a coluna JSON que promete ao
consumidor, e nunca foi testado contra ele. O boot recusa com 3, e a mensagem
diz "MariaDB is not supported by sink=mysql" em vez de um "want json" que
mandaria o operador procurar um erro de digitação.

#### 8.6.1 Cliente

`github.com/go-sql-driver/mysql` através de `database/sql`. Go puro, sem CGO;
justificativa em §14.3. Ao contrário do pgx (§8.5.2), o driver do MySQL foi
desenhado **para** `database/sql`: não há API nativa a preferir, o
`*mysql.MySQLError` atravessa a camada intacto (`errors.As` o encontra com o
`Number` que §8.6.6 exige) e o pool de `database/sql` é o que se usa.

Sem `interpolateParams`: cada lote vai pelo protocolo binário (statement
preparado, executado e fechado pelo `database/sql` — três idas ao servidor por
lote, uma para cada 256 eventos), e os blobs viajam crus. Interpolar no
cliente mandaria `env` e `body` escapados dentro do texto do comando, dobrando
o tamanho de cada corpo no pior caso.

O `sql_mode` da sessão é fixado em `STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION`
no `SET` de conexão. Fora do modo estrito, um valor longo demais ou um byte
inválido é **truncado com aviso** em vez de recusado — e o servidor de webhooks
gravaria um corpo cortado sem nenhum erro para contar. O modo é da sessão; a
configuração global do banco não é tocada.

O `autocommit` da sessão é fixado em `1` no mesmo `SET` (porte da família syslog/RELP, 2026-10-13;
era a pendência 16 de §19.1). Ele é do servidor por padrão, e este destino
depende dele: com `autocommit = 0` no `my.cnf` ou num `init_connect`, o lote de
um pedaço só abria uma transação implícita numa conexão do pool que ninguém
commitava — o `Write` voltava sem erro, o `200` saía e as linhas sumiam quando
a conexão fechava; a sonda de boot, numa transação revertida, passava. O driver
manda o `SET` depois do *handshake*, então ele vence o `init_connect`, que o
servidor roda antes; e a validação de §8.6.4 lê `@@session.autocommit` de volta
numa conexão do pool.

O pool nasce **preguiçoso**: `sql.OpenDB` não conecta, e o `WaitReady` do boot
(§10.1) é quem retenta.

#### 8.6.2 Concorrência e conexões

Igual a §8.5.3, com o `max_connections` padrão do MySQL em 151: `sink.workers`
padrão vira 4 quando o operador não configurou, `sink.mysql.pool_size` padrão é
`sink.workers + 1` (abaixo de `sink.workers`, `WARN`), o `Health()` tem um
`sql.DB` próprio de **uma** conexão, do mesmo *connector*, fora do pool
(§8.6.7; a conta por instância é `pool_size + 1`), e com `ring=none` o
`ring.max_inflight` acima do pool cria fila escondida dentro dele, com `WARN` no
boot.

#### 8.6.3 Escrita

Um comando por lote, com uma linha por evento:

```sql
INSERT INTO `inbox` (uuid, from_uuid, event_at, sid, step, level, event_nsec,
                     space, type, payload)
VALUES (?,?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?,?), ...
ON DUPLICATE KEY UPDATE uuid = uuid
```

O banco entra cru entre crases (`` `banco`.`tabela` ``): a tabela passa pela
regra de identificador e `sink.mysql.database` por esta (`exit 1`, tabela de §10.1) — no máximo 64
**caracteres** (não bytes: um nome de 64 letras acentuadas tem 128 bytes e é
legítimo) e nenhuma crase, contrabarra ou byte de controle, que quebrariam a
citação; a configuração recusa com `exit 1` e o construtor do destino confere o
mesmo para quem a contorna (porte da família syslog/RELP, 2026-10-13; era a
pendência 17 de §19.1, que dizia que o `1064` caía em `network`: pelo recuo
de `SQLSTATE` ele já era fatal, e a recusa vinha no boot com `exit 3`).

Os valores vêm do cabeçalho do JLH (`envelope.ParseHead`, §12.1): `uuid` e
`from_uuid` em dezesseis bytes, `event_at` como `DATETIME(6)` UTC (a conexão fixa
`time_zone = '+00:00'` e o driver envia em UTC), `payload` o texto do JLH a
partir de `"payload":`. O JSON do MySQL aceita `\u0000`, e o texto vai como está.

`ON DUPLICATE KEY UPDATE uuid = uuid` é o `ON CONFLICT (uuid) DO NOTHING` de §8.5.4:
a atualização é vazia, então uma redelivery não altera nada e não derruba o
lote. **Não é `INSERT IGNORE`**, de propósito: o `IGNORE` rebaixa a aviso
também `1406` (valor longo demais) e `1366` (string inválida) e **trunca** o
dado em silêncio — o oposto do que §17.7 permite.

O número de linhas afetadas conta as inseridas: o MySQL reporta `1` por linha
nova e `0` por linha existente cujo `UPDATE` não mudou nada (sem
`CLIENT_FOUND_ROWS`), então `len(lote) − afetadas` são as duplicatas, acumuladas
em `whs_sink_mysql_duplicates_total` (§8.6.9).

Dois tetos que o postgres não tem, e por isso o lote é **dividido em pedaços**
antes de ir ao servidor:

- **Placeholders.** O protocolo binário endereça 65 535 parâmetros por
  comando; com dez colunas, 6 553 linhas. A validação de configuração recusa
  `sink.batch` acima de 5 000 (§10.1).
- **`max_allowed_packet`.** Um comando (e cada parâmetro dele) não pode passar
  desse limite do servidor — 64 MiB no MySQL 8, e menos em
  instalações antigas. O servidor de webhooks lê o valor na validação do boot
  (§8.6.4) e corta o lote em pedaços cujo custo (o `payload` + as colunas de
  texto + sobrecarga por linha) fique abaixo de 7/8 dele. Um lote que precisa de
  mais de um pedaço vai inteiro numa **transação** (`BEGIN` … `COMMIT`): uma
  recusa `logic` do pedaço k reverte os anteriores, e "o lote foi descartado" é
  verdade — o tudo-ou-nada do `INSERT` único do postgres (§8.5.4). Commitar
  pedaço a pedaço foi rejeitado: o worker contava e logava como perdido o lote
  inteiro com parte dele gravada. As duplicatas contam depois do `COMMIT`. O
  lote de um pedaço só (todos, salvo os que encostam no teto) segue sem
  transação explícita. Um retry depois de um `COMMIT` cuja resposta se perdeu
  reescreve linhas que já estão lá, o que o `ON DUPLICATE KEY` torna inócuo.

A divisão não isola um evento que sozinho exceda o limite: o boot já recusa
uma configuração em que isso seja possível (§8.6.4), então um `1153` em tempo
de execução só acontece se o banco reduzir o limite depois de o processo subir
— e nesse caso é `logic`, logado com os ids.

O texto do comando é montado por lote (custa microssegundos; um cache por
número de linhas guardaria até 5 000 variantes por nada). Os dez valores por
linha vêm de um pool de slices por worker que volta **zerado**, pela razão de
§8.5.4: o `payload` segura referência ao buffer do JLH do item.

#### 8.6.4 Validação no boot

Corre uma vez, na primeira checagem de saúde bem-sucedida, com a regra de
§8.5.5: cada recusa é `ErrFatal`, o `WaitReady` desiste na hora e o processo
sai com 3.

| Verificação | Como | Se falhar |
|---|---|---|
| A tabela existe, o usuário a enxerga, e o motor é InnoDB | `information_schema.TABLES` | exit 3 |
| As dez colunas que o servidor escreve, com as larguras de §3.7 ou maiores (`binary(16)`; `datetime(6)` em `event_at` e, se existir, em `received_at`; `bigint` em `sid`, ≥ `int` em `step`, ≥ `smallint` em `level` e `event_nsec`, `UNSIGNED` aceito; ≥ 64 caracteres em `space` e `type`; `json`); extras toleradas | `information_schema.COLUMNS` (`DATA_TYPE`, `COLUMN_TYPE`, `CHARACTER_MAXIMUM_LENGTH`, `DATETIME_PRECISION`) | exit 3 — um `varchar` curto ou um inteiro estreito recusariam linhas com `1406`/`1264` (`logic`, o lote inteiro) com o sink UP; um `DATETIME` sem `(6)` arredonda o microssegundo em silêncio, mesmo no modo estrito |
| Índice único exatamente sobre a coluna `uuid` inteira (sem prefixo) | `information_schema.STATISTICS` (`NON_UNIQUE = 0`, uma coluna, `SUB_PART` nulo) | exit 3 — sem ele o `ON DUPLICATE KEY` não tem chave para colidir |
| Nenhum outro índice único, salvo o de uma única coluna `AUTO_INCREMENT` | `STATISTICS` + `COLUMNS.EXTRA` | exit 3 — o `ON DUPLICATE KEY UPDATE` dispara em **qualquer** chave única: um evento novo que colide noutro índice (`UNIQUE (from_uuid, sid)`, `(uuid, step)`, `uuid(8)`) toma o `UPDATE` vazio, o `Write` devolve sucesso e o evento some sem erro (revisão de 2026-10-06: 1 de 3 linhas gravadas). Um `AUTO_INCREMENT` não colide: o InnoDB nunca recua o contador |
| Servidor MariaDB | `SELECT VERSION()` quando `payload` é `longtext` | exit 3, nomeando o MariaDB (§8.6) |
| `autocommit = 1` na sessão | `SELECT @@session.autocommit` numa conexão do pool | exit 3 — um servidor ou *proxy* que ignorasse o `SET` de §8.6.1 deixaria todo `INSERT` numa transação que ninguém commita |
| `max_allowed_packet` comporta o maior envelope | `SELECT @@max_allowed_packet` contra a fórmula de §3.2 mais a sobrecarga do comando | exit 3, com os dois números na mensagem |
| `INSERT` e `UPDATE` concedidos, sessão gravável, CHECKs e colunas extras satisfeitas | um `INSERT ... ON DUPLICATE KEY UPDATE` de sonda com cada valor no limite do JLH — `uuid` e `from` o UUIDv7 fixo de tempo zero (`00000000-0000-7000-8000-000000000000`), `space` de 64 caracteres, `type` `webhook.request`, `level` 15, `sid` 2^53−1, `step` 32, `event_nsec` 999, `payload` `{}` —, dentro de uma transação **revertida** | exit 3 — `1142` é permissão, `1290` é servidor somente leitura, `3819` é uma CHECK, `1364` uma coluna extra sem default, `1264`/`1406` uma coluna estreita que o catálogo não pegou |

A sonda por transação revertida substitui a consulta de privilégios que o
postgres faz com `has_table_privilege`: no MySQL um privilégio concedido por
papel (MySQL 8) ou por `GRANT ... ON db.*` não aparece em
`information_schema.TABLE_PRIVILEGES`, e a única resposta confiável para "posso
escrever?" é tentar, com **o mesmo comando** que o dreno usa. Isso expõe uma
diferença real para o postgres: o `ON DUPLICATE KEY UPDATE` exige o privilégio
`UPDATE` além de `INSERT`, mesmo com a atualização vazia. O usuário precisa dos
dois; o `UPDATE` pode ficar restrito à coluna `uuid`
(`GRANT INSERT, UPDATE (uuid) ON webhook.inbox TO ...`). É por isso que InnoDB é
exigido: num motor sem transação a sonda ficaria gravada.

A validação **não se repete**. Uma tabela dropada depois do boot aparece como
`1146` na escrita, que é fatal (§8.6.6).

#### 8.6.5 Durabilidade

Como em §8.5.6: o servidor não lê nem altera `innodb_flush_log_at_trx_commit`.
Com `1` (padrão) o `INSERT` só retorna depois do `fsync` do redo log, e o `200`
de `ring=none` significa o que §16.1 promete; com `2` ou `0`, um crash **do
banco** perde até um segundo de eventos já respondidos. A linha de §16.1 leva a
mesma marca `†`.

#### 8.6.6 Classificação de erro

O driver entrega o número do erro do servidor (`MySQLError.Number`), e a tabela
abaixo **é** a função `classifyMySQLNumber`; `TestClassifyMySQLNumber` a lê de
volta. O número decide primeiro. Um número que a tabela não lista cai na classe
do `SQLSTATE` (`MySQLError.SQLState`): `22` e `23` são `logic`; `42` e `28` são
`fatal`; `08`, o genérico `HY000`, qualquer outro ou nenhum são `network`. Sem
esse recuo, um `3151` (`22032`, chave JSON acima de 64 KiB) era retentado para
sempre com o sink UP. Esquema e permissão continuam `fatal`, nunca `logic`.

| `Number` | Situação | Classe |
|---|---|---|
| *(sem número)* | recusa de conexão, EOF, `driver.ErrBadConn`, `mysql.ErrInvalidConn`, deadline | `ErrNetwork` |
| `1040` `1053` `1077` `1927` | conexões demais, servidor encerrando, conexão morta | `ErrNetwork` |
| `1158` `1159` `1160` `1161` | erro ou timeout de rede no meio do comando | `ErrNetwork` |
| `1205` `1213` | espera de lock, deadlock | `ErrNetwork` |
| `1317` `3024` | comando interrompido, `max_execution_time` | `ErrNetwork` |
| `2006` `2013` | servidor foi embora, conexão perdida | `ErrNetwork` |
| `1203` `1226` `1461` | `max_user_connections`, limite por usuário (`MAX_USER_CONNECTIONS`, `*_PER_HOUR`), `max_prepared_stmt_count` | `ErrNetwork` — limites que passam sozinhos; o `SQLSTATE` deles é `42000`, e pelo recuo seriam fatais: um *rolling deploy* que encosta no orçamento de conexões derrubava a frota com `exit 3` (porte da família syslog/RELP, 2026-10-13; revisão do `go-relplog-server`) |
| **`1290` `1836`** | **servidor somente leitura**: réplica, ou configurado assim | **`ErrFatal`** — a decisão de §8.5.7 sobre `25006`, pelos mesmos motivos |
| `1021` `1030` `1041` `1114` | disco cheio, erro do motor de armazenamento, sem memória, tabela cheia | `ErrFatal` |
| `1044` `1045` `1049` `1130` `1142` `1143` | acesso negado, banco inexistente, host proibido, comando ou coluna negados | `ErrFatal` |
| `1054` `1146` `1364` | coluna desconhecida, tabela inexistente, coluna sem default: contrato de esquema quebrado | `ErrFatal` |
| **`1048`** | `NULL` recusado: o servidor nunca manda `NULL`, então depois da sonda de boot ele é a **tabela** (um *trigger* que anula, uma coluna mudada) — o gêmeo do `1364` e do `23502` de §8.5.7 | `ErrFatal` — como `logic` todo lote era descartado com o sink UP (porte da família syslog/RELP, 2026-10-13) |
| `1062` `1153` `1264` `1366` `1406` `1690`, `mysql.ErrPktTooLarge` | duplicata (inalcançável, abaixo), pacote ou valor grande demais, fora de faixa, string inválida | `ErrLogic` |
| `3819` `3140` `3144` | CHECK violada, texto que não é JSON, JSON de string binária | `ErrLogic` |
| outro número, `SQLSTATE` de classe `22` ou `23` | dado recusado (o `3151`; o `4025` de CHECK do MariaDB) | `ErrLogic` |
| outro número, classe `42` ou `28` | sintaxe, regra de acesso, autorização | `ErrFatal` |
| outro número, classe `08`, `HY000`, outra ou nenhuma | desconhecido | `ErrNetwork` |

`1062` é inalcançável: o `ON DUPLICATE KEY` absorve a colisão em **qualquer**
chave única, não só na primária. Por isso um índice único extra **não** vira
`1062`/`logic`, como esta seção dizia até a revisão de 2026-10-06: ele engole o
evento em silêncio, e o boot o recusa (§8.6.4). A linha fica pelo que o número
seria.

#### 8.6.7 `Health()`

`SELECT @@read_only`. Um `1` é falha: o servidor está vivo e não é gravável —
uma réplica, que um failover pode promover — e o estado vai para DOWN dentro
de um `sink.health_interval`, pelas razões de §8.5.8. Um `1290` chegando pela
escrita continua fatal (§8.6.6): é o mesmo par de sinais de §8.5.7 e §8.5.8.
O check também atualiza os gauges do pool (§8.6.9).

O check roda num `sql.DB` próprio de **uma** conexão, do mesmo *connector* e
fora do pool dos escritores, pelo motivo de §8.5.8 (porte da família syslog/RELP, 2026-10-13); o
`database/sql` refaz a conexão quebrada sozinho.

#### 8.6.8 Retenção

Igual a §8.5.9: o servidor não apaga nem particiona. A poda barata é
particionar por faixa de `ts` e descartar partições; com InnoDB, um `DELETE` em
massa numa tabela grande segura o histórico de undo e não é o que este desenho
recomenda.

#### 8.6.9 Métricas

```
whs_sink_mysql_duplicates_total             counter  linhas deixadas intactas pelo ON DUPLICATE KEY
whs_sink_mysql_pool_conns{state}            gauge    state: acquired|idle|max
whs_sink_mysql_pool_waits_total             counter  aquisições que esperaram por conexão
```

Cardinalidade fixa (§11.1). Os valores vêm de `sql.DB.Stats()` a cada
`Health()`, como em §8.5.8.

#### 8.6.10 TLS

Como §8.5.11, com `sink.mysql.tls`, `tls_ca` e `tls_insecure`: verificado
contra o pool do sistema ou a CA configurada, `ServerName` igual ao host, e
`tls_ca` sem `tls` é erro de configuração.

### 8.7 `sink.type = nats`

Destino final num stream **JetStream**. Mensagem em §3.8. É o destino que mais
se parece com o Redis (§8.2): remoto, sem esquema, um consumidor do outro lado
— com uma diferença que decide tudo: o JetStream **confirma** cada publicação
com um `PubAck`, e essa confirmação é o que dá ao `200` de `ring=none` o
significado de §16.1. NATS "core", sem JetStream, não confirma nada e não é
aceito: o boot exige que o subject pertença a um stream (§8.7.5).

#### 8.7.1 Cliente e conexão

`github.com/nats-io/nats.go`, com o pacote `jetstream` (a API atual, não a
`nats.JetStreamContext` legada). Go puro, sem CGO; justificativa em §14.3.

Uma conexão por processo, multiplexada por todos os workers — o cliente
serializa as publicações numa única saída em lote, então `sink.workers` fica
no padrão de 8, como no Redis. A conexão é aberta **preguiçosamente** na
primeira checagem de saúde, e é o `WaitReady` do boot (§10.1) quem retenta,
com a disciplina de backoff dos outros destinos. Uma recusa de autenticação
na conexão é fatal na hora.

Depois de conectado, o cliente reconecta sozinho, sem limite de tentativas
(`MaxReconnects(-1)`): a máquina de estados de §10.2 já tem os fusíveis que
decidem quando desistir, e um cliente que se fechasse por conta própria depois
de N tentativas transformaria uma indisponibilidade longa num estado
irrecuperável sem passar pelo dump de emergência. O **buffer de reconexão é
desligado** (`ReconnectBufSize(-1)`): durante uma desconexão, publicar falha
na hora com classe `network`, em vez de guardar mensagens dentro do cliente. O
ring é o único buffer deste desenho, e uma fila escondida no cliente escaparia
da contabilidade, do `429` e do dump.

A conexão só entra no estado `CLOSED` por ação deste processo ou por uma
recusa de autenticação repetida; nesse estado o cliente nunca mais reconecta, e
por isso `CLOSED` é fatal (§8.7.6).

Os logs de conexão e de reconexão mostram só o esquema e o host do servidor:
o `ConnectedUrl()` do `nats.go` devolve a URL com a senha, e o
`ConnectedUrlRedacted()` esconde a senha mas não um *token* na parte de usuário
(`nats://token@host`). Até 2026-10-11 a URL inteira, senha incluída, saía no
`INFO` de todo boot e no `WARN` de toda reconexão (achado no
`go-webhook-nats-server`). O log "sink ready" e o dump de configuração mostram
`sink.nats.urls` com a parte de usuário trocada por `***`.

#### 8.7.2 Escrita

Publicação **assíncrona em lote**: cada item vira um `PublishMsgAsync`, e só
depois de os N futuros estarem no ar o worker espera as N confirmações, sob o
`sink.op_timeout` do lote. Uma publicação síncrona por item custaria um
round-trip por evento por worker — a mesma razão de o Redis usar `DoMulti`.

```go
f, err := js.PublishMsgAsync(msg,
    jetstream.WithExpectStream(stream),   // o servidor recusa se o subject cair noutro stream
    jetstream.WithRetryAttempts(0))       // ver abaixo
...
select {
case ack := <-f.Ok():    if ack.Duplicate { dup++ }
case err := <-f.Err():   // logic: só esta mensagem; outra classe: o lote todo
case <-ctx.Done():       return network(ctx.Err())
}
```

Três decisões que não são as padrões da biblioteca:

- **`WithExpectStream`**: o header `Nats-Expected-Stream` faz o servidor
  rejeitar a mensagem se o subject pertencer a outro stream. Um erro de
  configuração vira recusa no boot, não linhas gravadas no lugar errado.
- **`WithRetryAttempts(0)`**: por padrão a biblioteca **retenta sozinha**, em
  segundo plano, uma publicação que recebe "sem respondedores" — relendo
  `msg.Data` depois de o `Write` ter retornado. `msg.Data` é o buffer do item,
  que §12.3 devolve ao pool depois da confirmação; uma releitura tardia é
  exatamente a corrida que §12.3 proíbe. Com zero tentativas internas, quem
  retenta é o worker (§10.4), com o item ainda dono do buffer.
- **`WithPublishAsyncTimeout(sink.op_timeout)`**: sem ele, um futuro sem
  resposta fica pendente para sempre, segurando a mensagem — e o buffer — na
  tabela interna do cliente. Com ele, um `Write` que estourou o prazo deixa a
  biblioteca soltar a referência dentro do mesmo prazo. Até lá vale a
  contaminação de §12.3: o worker não devolve os buffers de uma tentativa que
  terminou por prazo.

`WithPublishAsyncMaxPending` é dimensionado em `2 × (sink.workers × sink.batch +
ring.max_inflight)`: o dobro do teto de publicações em voo que este processo
consegue gerar. Um teto menor faria `PublishMsgAsync` bloquear e depois falhar
com "stalled" numa carga normal. O dobro porque os futuros de um lote
contaminado continuam contando até o próprio prazo assíncrono, que começa na
publicação, um pouco depois do prazo do worker: uma retentativa rápida
encontrava a janela cheia e falhava com `ErrTooManyStalledMsgs`
(`go-relplog-nats-server`, revisão independente de 2026-10-13; porte da família
syslog/RELP, 2026-10-13). Os tetos de `sink.workers`, `sink.batch` e
`ring.max_inflight` (§13.5) mantêm a conta num `int`: sem o de
`ring.max_inflight`, um valor grande dava a volta, negativo, e o sink trocava em
silêncio por 4096 (revisão do `go-syslog-server`, 2026-10-13).

O JLH vai em `Data`, o slice do item, sem cópia — a biblioteca o serializa
para o socket **dentro** da chamada e, com `WithRetryAttempts(0)`, não o lê de
novo. Os headers (§3.8) são curtos e de tamanho fixo (`uuid`, `space`, `type`),
lidos do cabeçalho do JLH (`envelope.ParseHead`); nada do envelope vai num
header. O subject é `sink.nats.subject` com `{space}` e `{type}` do próprio
envelope.

**Entrega parcial.** Uma recusa `logic` de **uma** mensagem não encerra o lote:
um envelope que não mapeia (cabeçalho ilegível), uma mensagem que o cliente
recusa antes de sair (`nats.ErrMaxPayload`) e uma que o stream recusa no
`PubAck` (`10054`) são recusadas sozinhas, e as demais do lote são publicadas e
confirmadas. O `Write` devolve então um `logic` com `Delivered`, os índices que
o stream guardou (§8.1). Até 2026-10-11 a primeira recusa encerrava o lote: as
anteriores já estavam no stream e eram contadas como perdidas, e as seguintes
nunca eram publicadas — e eram confirmadas e liberadas do ring como perda
(achado na revisão do `go-syslog-nats-server`). Uma falha de outra classe
(rede, fatal) é do lote inteiro, como antes.

**Contaminação.** Todo erro depois da primeira publicação contamina o lote
(§12.3): um futuro ainda pendente aponta para o `Env`, e os buffers vão para o
GC em vez do pool. Uma recusa `logic` também contamina: o caminho é raro.

**Permissão negada.** O servidor avisa uma publicação ou um pedido à API do
JetStream negados só pelo *callback* assíncrono de erro: a publicação negada
expira (`ErrAsyncPublishTimeout`, `network`), retentada para sempre com o sink
UP. O sink guarda o primeiro `Permissions Violation` como **veredito**, e toda
checagem de saúde (§8.7.7) e todo `Write` seguinte o devolvem como `ErrFatal`:
dump e `exit 3`; no boot, `exit 3` na hora. Este servidor não publica nada além
dos dados nesta conexão, então toda violação de permissão é do caminho de
dados — inclusive a assinatura negada em `_INBOX.`, onde chegam os `PubAck` e
as respostas da API. O `-ERR` chega antes de o prazo da confirmação (ou do
`StreamInfo`) estourar; quando a ida e volta falha como `network`, o sink lê o
`LastError` da conexão e, havendo veredito, o **próprio** `Write` ou `Health`
que esbarrou nele já é `ErrFatal` (antes era `network`, retentado, com o sink UP
até a checagem seguinte; porte da família syslog/RELP, 2026-10-13). O log desses erros assíncronos é amostrado (um por intervalo, com a
contagem do que foi suprimido): uma permissão negada produz um por mensagem.

#### 8.7.3 Desduplicação e at-least-once

`Nats-Msg-Id` carrega `<uuid>.<step>` (§3.8). O stream responde como duplicata uma
segunda publicação com o mesmo id dentro de `duplicate_window` (dois minutos
por padrão; `docker/nats/stream.json` usa dez), com `PubAck.Duplicate = true`
— que conta em `whs_sink_nats_duplicates_total` e é o equivalente do
`ON CONFLICT` de §8.5.4. Fora da janela, uma redelivery (um dump de emergência
reingerido horas depois) **duplica** a mensagem: o pipeline é at-least-once por
desenho, e o consumidor do stream deduplica por `(uuid, step)`, como o JLH
manda. A janela é do stream, não deste servidor.

#### 8.7.4 Durabilidade

O `PubAck` significa que o stream **gravou** a mensagem conforme a própria
configuração: `storage: file` e o número de réplicas. Contra a queda do
processo `nats-server`, o file store só sincroniza com o disco a cada
`sync_interval` (dois minutos por padrão) — um crash do servidor NATS perde o
que estava entre dois `fsync`, a menos que `sync_always: true` esteja ligado ou
que o stream tenha réplicas. O servidor de webhooks não lê nem altera nada
disso; a linha de §16.1 leva a marca `†` pela razão de §8.5.6.

#### 8.7.5 Validação do stream (no boot e em toda checagem)

Na primeira checagem de saúde que alcança o stream, e de novo em **toda**
checagem depois dela, contra o `StreamInfo` que a própria checagem já buscou
(nenhuma ida e volta a mais), com a regra de §8.5.5: cada recusa é fatal; no
boot o `WaitReady` desiste na hora (exit 3, sem esperar `sink.boot_timeout`, com
o log "the sink refused this server permanently"); depois, FATAL, dump e exit 3
(§10.2). Até 2026-10-11 o contrato era conferido só no boot: um stream editado
por baixo — subjects estreitados, `no_ack` ligado — respondia toda publicação
com um prazo estourado, `network`, retentado para sempre com o sink UP e o ring
enchendo (achado no `go-webhook-nats-server`). O log "stream validated" sai uma
vez.

| Verificação | Como | Se falhar |
|---|---|---|
| O stream existe | `js.Stream(stream)` | exit 3 |
| O JetStream responde | "sem respondedores" na primeira checagem: o JetStream está desligado neste servidor, ou nenhum servidor com JetStream está ao alcance (uma folha com o *hub* fora); o cliente não distingue os dois. Uma conta sem JetStream num servidor com ele responde `10039`, fatal por si | exit 3 na hora, nomeando o JetStream. Antes era `network`, e o boot esperava o `sink.boot_timeout` inteiro para sair com o motivo errado. Depois de uma checagem validada, o mesmo é `network`. Não se pergunta `AccountInfo`: ele traduz "sem respondedores" para "não habilitado" sozinho, e o pedido a `$JS.API.INFO`, negado a um papel mínimo, virava veredito de permissão permanente (revisão independente do porte) |
| O subject do Workspace (`sink.nats.subject` expandido com o `space` desta instância e `type=webhook.request`) é coberto pelos subjects de **este** stream | casamento local dos padrões do stream (`*` um token, `>` um ou mais, só no fim), sem ida e volta: dois streams de uma conta não se sobrepõem, então um padrão deste stream que cobre o subject prova que nenhum outro é o dono | exit 3 — publicar cairia noutro stream, ou em nenhum |
| O stream não captura o plano de controle | nenhum subject do stream cobre `loghub._ctl.beacon` (`loghub.>` é o jeito clássico de chegar lá, §3.8) | exit 3 — o stream guardaria todo beacon do ecossistema e reentregaria os antigos a todo consumidor novo |
| O stream aceita publicações | `Sealed = false` | exit 3 |
| O stream confirma publicações | `no_ack = false` | exit 3 — com `no_ack` todo futuro expira (`ErrAsyncPublishTimeout`, `network`), retentado para sempre com o sink UP: nada seria confirmado |
| O stream guarda o que confirma | `retention ≠ interest` | exit 3 — com `interest` e nenhum consumidor interessado o servidor responde um `PubAck` normal e descarta a mensagem: o `200` de `ring=none` e a liberação do ring virariam perda silenciosa (a decisão do `go-syslog-server`, 2026-10-11, como padrão assumido do porte de 2026-10-13, contestável: a alternativa seria só avisar; antes, §19.1) |
| Um teto por subject | `max_msgs_per_subject > 0` | só `WARN`, uma vez por valor: todo evento de um `space` divide um subject, e o stream descarta os mais antigos à medida que confirma os novos — legítimo num stream de "só o último" |
| Cada subject **diferente** do da instância — o de um envelope de outro `space`, que só um backlog de outro Workspace (`ring.db` ou dump de emergência) produz — pertence a **este** stream | `js.StreamNameBySubject(subject)` na primeira escrita com esse subject, antes de publicar qualquer item do lote; o subject coberto fica em cache (até 1024); uma checagem que o encontre descoberto o esquece (o operador estreitou o stream depois de o backlog drenar, como o erro manda), e a próxima publicação nele, se houver, pergunta de novo | `ErrFatal`, nomeando o subject: o stream precisa cobri-lo também até o backlog drenar. Publicado às cegas, dava "no response from stream", `network`, e o dreno do boot (§10.1) saía com 3 num laço de reinício sem nomear a causa. Se outro stream é o dono, fatal nomeando-o (o `Nats-Expected-Stream` recusaria com `10060`) |
| O maior envelope cabe | `nc.MaxPayload()` e o `max_msg_size` do stream contra a fórmula de §3.2 mais 512 bytes de headers; com `storage: file`, o registro (mensagem, subject e 64 bytes de moldura) contra o teto de 32 MiB do *file store*; com `discard: new` e `max_bytes`, o registro contra o `max_bytes` | exit 3, com os números na mensagem. O teto do *file store* vale qualquer que seja o `max_payload` (que vai a 64 MB): acima dele a recusa é `10077`, fatal, no primeiro corpo grande. Um stream que recusa mensagens novas quando cheio, menor que uma mensagem, recusaria o primeiro evento grande e toda reingestão — e, com o `10077` de stream cheio agora `network` (§8.7.6), seguraria o ring para sempre |

O stream é criado por quem administra o servidor NATS, a partir de
`docker/nats/stream.json` (`nats stream add LOGHUB_WEBHOOK_REQUEST --config stream.json`).
O servidor de webhooks nunca cria nem altera um stream, pelos motivos de
§8.5.1: N instâncias subindo juntas, uma conta sem permissão de administração
do JetStream, e a evolução do stream entre versões.

#### 8.7.6 Classificação de erro

Erros da API do JetStream trazem um código numérico (`APIError.ErrorCode`, da
tabela de erros do `nats-server`) e um código HTTP (`APIError.Code`); os da
biblioteca são valores sentinela. A tabela **é** a função `classifyNATS`, lida
de volta por `TestClassifyNATS`.

| Erro | Situação | Classe |
|---|---|---|
| deadline, cancelamento, `nats.ErrTimeout`, `ErrNoResponders`, `ErrDisconnected`, `ErrConnectionReconnecting`, `ErrReconnectBufExceeded`, `jetstream.ErrNoStreamResponse`, `ErrAsyncPublishTimeout`, `ErrTooManyStalledMsgs` | sem resposta, ou cliente entre servidores | `ErrNetwork` |
| `nats.ErrAuthorization`, `ErrAuthExpired`, `ErrAuthRevoked`, `ErrAccountAuthExpired`, `ErrConnectionClosed` (conexão fechada em definitivo), e o texto do servidor no `CONNECT` ("Authorization Violation", "User Authentication Expired", "User Authentication Revoked") | credencial recusada, cliente encerrado | `ErrFatal`. O `errors.Is` do `nats.go` compara o texto inteiro, e "User Authentication Expired" nunca casava com `ErrAuthExpired` ("authentication expired"): o boot esperava o `sink.boot_timeout` |
| `nats.ErrSecureConnWanted` (`sink.nats.tls` contra um servidor sem TLS) e um certificado que não verifica (`tls.CertificateVerificationError` dentro de `nats.ErrTLS`) | um TLS que nunca vai dar certo | `ErrFatal` — com a regra do *pool* de §8.7.9. Outra falha do *handshake* (reset, prazo) é `ErrNetwork` |
| `Permissions Violation` no *callback* assíncrono (publicação ou API do JetStream negadas) | permissão da conta | `ErrFatal`, pelo veredito de §8.7.2 |
| `jetstream.ErrStreamNotFound` (`10059`), `ErrJetStreamNotEnabled` (`10076`), `ErrJetStreamNotEnabledForAccount` (`10039`) | stream sumiu, JetStream desligado | `ErrFatal` |
| API `10060` stream esperado não confere, `10109` stream selado | contrato de configuração quebrado | `ErrFatal` |
| API `10023` recursos insuficientes, `10077` armazenamento falhou | disco ou limite da conta | `ErrFatal` — coerente com o `OOM` de §8.2.4 |
| API `10077` com a descrição do *store* "maximum messages exceeded", "maximum bytes exceeded", "maximum messages per subject exceeded" (stream com `discard: new` cheio) ou "store is closed" (stream ou servidor fechando, o `ErrStoreClosed` de `server/store.go` do `nats-server` 2.14.6; "store closed" também, grafia antiga) | contrapressão, reinício do NATS | `ErrNetwork` — passam sozinhos. Como fatal eram dump e `exit 3`, e a parte já gravada do lote duplicava na reingestão (revisão independente do `go-relplog-server`, 2026-10-11). A família casava só "store closed", texto que o servidor não manda (revisão do `go-syslog-server`, 2026-10-13). Um registro que o *store* não comporta continua fatal |
| API `10158` (409, o mesmo `Nats-Msg-Id` ainda numa proposta Raft), `10167` (429, fila de entrada do stream cheia) e todo `Code` 429 | transitórios, embora 4xx | `ErrNetwork` — pela regra do 4xx desconhecido, um cluster degradado ou um pico virava FATAL, dump e `exit 3` em todas as instâncias do stream de uma vez |
| `nats.ErrMaxPayload`, `jetstream.ErrMaxBytesExceeded`, API `10054` tamanho excede o máximo | um evento grande demais | `ErrLogic` **daquela mensagem** (entrega parcial, §8.7.2) — inalcançável depois de §8.7.5, salvo mudança no servidor |
| outro erro da API com `Code` 4xx (fora 429) | pedido recusado por forma: repetiria em toda tentativa | `ErrFatal` |
| outro erro da API com `Code` 5xx | falha do lado do servidor | `ErrNetwork` |
| qualquer outro | desconhecido | `ErrNetwork` |

#### 8.7.7 `Health()`

Dois passos: o estado da conexão (`CONNECTED` é saúde; `CLOSED` é fatal;
qualquer outro — `RECONNECTING`, `DISCONNECTED` — é falha de rede) e uma
consulta `StreamInfo`, que prova que o JetStream responde **e** que o stream
ainda existe. Um `PING` responderia feliz com o JetStream desligado, e o
estado nunca sairia de UP. Com o `StreamInfo` em mãos, o contrato inteiro de
§8.7.5 é conferido de novo. Antes de tudo, um veredito (§8.7.2, §8.7.9) é
devolvido como `ErrFatal`. Fora de `CONNECTED`, e quando o `StreamInfo` falha
como `network`, a checagem lê o `LastError` da conexão: o que mantém o cliente
reconectando pode ser permanente (um TLS que não verifica, §8.7.9), e uma
recusa de TLS entra no erro como motivo — o FATAL dos fusíveis (§10.2), se o
*pool* nunca voltar, guarda o erro da última checagem e diz por quê.

#### 8.7.8 Métricas

```
whs_sink_nats_duplicates_total              counter  PubAck com Duplicate=true
whs_sink_nats_reconnects_total              counter  reconexões do cliente
```

#### 8.7.9 Autenticação e TLS

Uma de três formas, e só uma: `sink.nats.username`/`password`, `sink.nats.token`
ou `sink.nats.credentials` (um arquivo `.creds` com JWT e semente NKey). A
biblioteca aceitaria todas ao mesmo tempo e escolheria uma em silêncio; a
validação recusa a ambiguidade (§10.1). TLS como em §8.5.11: verificado por
padrão, CA própria por `tls_ca`, `tls_insecure` desliga a verificação.

**Um TLS que nunca vai dar certo é veredito** (porte da família syslog/RELP,
2026-10-13: achado no `go-relplog-nats-server`, adaptado pelo `go-syslog-server`
a um cliente com TLS). Só dois casos, os que precisam do operador: um
certificado que não verifica (autoridade desconhecida, nome que não confere,
vencido) e `sink.nats.tls` ligado contra um servidor que não oferece TLS
(`ErrSecureConnWanted`). Um servidor que passa a **exigir** TLS depois do boot é
aceito pelo `nats.go` sozinho, com as raízes do sistema quando `sink.nats.tls`
está desligado, e só cai aqui se o certificado não verificar. No primeiro
`CONNECT` a recusa é `exit 3` na hora, em vez de esperar o `sink.boot_timeout`
com a mensagem da rede. Depois do boot o `nats.go` não a relata pelo
`ReconnectErrCB` (que é da discagem TCP): ela fica no `LastError` com a
conexão em `RECONNECTING`, e a checagem de saúde a lê (§8.7.7). Sem isso, o
FATAL vinha dos fusíveis, ~20 s depois, lendo "connection is RECONNECTING" — ou
nunca, com os dois fusíveis desligados.

**Só quando nenhum outro servidor pode servir** (revisão do `go-syslog-server`,
2026-10-13, C1; a opção recomendada, padrão assumido do porte, contestável). O
`LastError` é o do último servidor **tentado**: num *pool* — mais de uma URL em
`sink.nats.urls`, ou servidores que o cluster anunciou e o `nats.go`
acrescentou sozinho — o cliente alterna entre eles enquanto o confiável está
fora, e a recusa de um nó não confiável virava veredito permanente, dump e
`exit 3`, embora o confiável voltasse logo depois. Por isso a recusa de TLS só
é veredito com um servidor só no *pool* do cliente (`len(nc.Servers()) == 1`,
os descobertos incluídos); senão é `network`, com a recusa nomeada no erro da
checagem, e os fusíveis dão o FATAL, nomeando o TLS, se o *pool* nunca voltar.
No boot, com mais de uma URL, a recusa de TLS também é `network` (o
`nats.Connect` devolve o erro do último servidor tentado), e o `WaitReady`
espera o `sink.boot_timeout`.

Uma URL de `sink.nats.urls` com *userinfo* (`nats://user:senha@host` ou
`nats://token@host`) **junto** de `username`, `password`, `token` ou
`credentials` é `exit 1`: o `nats.go` prefere a credencial da URL, e a outra
ficava sem uso, em silêncio. Cada URL é conferida na validação, **sem eco**:
uma senha com `%`, `/` ou `#` sem *percent-encoding* falhava dentro do
`nats.Connect` como erro de rede, com a senha em cada linha de log até o
`sink.boot_timeout`; e o erro do `net/url` cita um pedaço da entrada
(`invalid port ":Sup3r" after host` é a senha até a barra), então nada do texto
do parser vai para a mensagem. Uma URL sem host, ou com caminho, consulta ou
fragmento, é `exit 1`: `nats://user:1234/5678@host` é o host `user`, a porta
`1234` e um caminho, sem credencial nenhuma, e o cliente a discaria até o
`sink.boot_timeout`.

Desde o porte da família syslog/RELP (2026-10-13; as regras do `natsurls.go`
do `go-syslog-server`, vindas do `go-relplog-server`): o esquema tem de ser um
dos que o `nats.go` disca — `nats://`, `tls://`, `ws://`, `wss://` (um
`host:porta` sem esquema é `nats://`) —, e outro é `exit 1` na hora, nomeado
só quando tem forma de esquema (um valor estropiado pode guardar um segredo
ali); antes ele só falhava na conexão, retentado até o `sink.boot_timeout`. Uma
URL de websocket pode ter caminho (o *endpoint* do servidor); `nats://` e
`tls://`, não. Um `@` depois do primeiro `/`, `?` ou `#` é uma senha com um
deles sem *percent-encoding*, e o erro diz qual escape usar (`%2F`, `%3F`,
`%23`) — vale também para `ws://`, onde o caminho não recusaria a URL. Um
*escape* no host é nomeado como problema do host, não da senha.

### 8.8 `sink.type = clickhouse`

Destino final num ClickHouse **compartilhado por todas as instâncias**. Tabela
em §3.9. É o terceiro destino remoto com esquema, e tudo de §8.5 que não é
citado aqui vale igual: o servidor valida e nunca cria (§8.5.1), a conta de
conexões (§8.5.3) e a retenção externa (§8.5.9). Ele muda duas coisas: o
destino é um armazém analítico, que quer **poucos `INSERT` grandes** (cada um
vira uma parte, e partes demais travam o merge); e o ClickHouse **não tem
transação**, o que muda a sonda de boot (§8.8.5) e a idempotência (§8.8.4).

#### 8.8.1 Cliente

`github.com/ClickHouse/clickhouse-go/v2`, pela API nativa (`clickhouse.Open`,
`driver.Conn`), no **protocolo nativo** (porta 9000; 9440 com TLS). Go puro,
sem CGO; a justificativa da dependência, escolhida pelo usuário contra o HTTP
da biblioteca padrão, está em §14.3. O protocolo nativo manda o lote em
colunas, comprimido com **LZ4** — o objetivo deste destino é vazão, e o JLH
comprime bem —, e o erro do servidor chega como `*clickhouse.Exception`, com o
`Code` que §8.8.7 exige.

O pool nasce **preguiçoso**: `clickhouse.Open` não disca, e o `WaitReady` do
boot (§10.1) é quem retenta. `sink.op_timeout` é o prazo de discagem e de cada
`INSERT` (pelo contexto). As conexões levam o nome da instância (§3.5) no
`client_name`, visível no `system.query_log`.

#### 8.8.2 Concorrência e conexões

Igual a §8.5.3: `sink.workers` padrão vira **4** quando o operador não
configurou, `sink.clickhouse.pool_size` padrão é `sink.workers + 1` (abaixo de
`sink.workers`, `WARN`), o `Health()` tem uma conexão própria fora do pool
(§8.8.8; a conta por instância é `pool_size + 1`), e com `ring=none` um
`ring.max_inflight` acima do pool cria fila escondida dentro dele, com `WARN`
no boot. Aqui há um motivo a mais para poucos workers: cada
`INSERT` é uma parte nova no disco do servidor, e o ClickHouse prefere poucas
partes grandes (com muitas, `252` `TOO_MANY_PARTS`, §8.8.7). Com `ring=none`
cada requisição é um `INSERT` de uma linha; quem quiser vazão ali liga o
`async_insert` no perfil do usuário (§8.8.6).

#### 8.8.3 Escrita

Um `INSERT` por lote, pelo `PrepareBatch` do driver, com os valores em colunas:

```sql
INSERT INTO `banco`.`inbox`
  (uuid, from_uuid, event_at, sid, step, level, event_nsec, space, type, payload)
SETTINGS insert_deduplication_token = 'whs-<sha256 dos uuids do lote>',
         json_type_escape_dots_in_keys = 1,
         type_json_skip_duplicated_paths = 1,
         input_format_try_infer_dates = 0,
         input_format_try_infer_datetimes = 0,
         input_format_json_try_infer_numbers_from_strings = 0,
         throw_on_max_partitions_per_insert_block = 0
```

(Os ajustes viajam no pacote do comando, não no texto.) O banco entra cru entre
crases, com a regra de §8.6.3 para `sink.clickhouse.database` (64 caracteres,
sem crase, contrabarra ou byte de controle): dentro de crases o ClickHouse lê a
contrabarra como escape, e um nome com ela endereçaria outro banco que não o
validado (porte da família syslog/RELP, 2026-10-13). Os valores vêm do
cabeçalho do JLH (`envelope.ParseHead`, §12.1), e **todas as linhas são
mapeadas antes de o comando sair**: um JLH que o leitor recusa é `logic`, e o
lote inteiro, como em §8.6.3. O `payload` é o texto do JLH a partir de
`"payload":`, no modo string da coluna `JSON` do driver; o servidor o decompõe.

**Posse de buffer (§12.3).** O `Append` do driver **copia** cada valor para o
buffer da própria coluna (`ColStr.AppendBytes` do `ch-go`): quando o `Send`
começa, nada do comando referencia o JLH do item. Não há releitura tardia, como
a do `nats.go` (§8.7.2) ou a do pipeline do rueidis (§12.3). O custo é de duas a
três cópias de cada `payload` por tentativa (a coluna, o bloco serializado e
o comprimido), o preço do protocolo colunar; os slices de
colunas vêm de um pool por worker e voltam **zerados**, pela razão de §8.5.4.

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

**Por que `throw_on_max_partitions_per_insert_block = 0`.** Um lote que cai em
mais partições que `max_partitions_per_insert_block` (100) — a reingestão de um
backlog de meses — era recusado com `252`, o código de `TOO_MANY_PARTS`, que é
`network`: o worker retentava para sempre. As partes são escritas assim mesmo e
o merge as junta; o boot já recusa uma chave de partição que não seja função de
`event_at` só (§8.8.5).

**O que o comando não leva**, de propósito: `async_insert`,
`wait_for_async_insert`, `insert_quorum`, `fsync_after_insert`. São decisões do
administrador do ClickHouse (§8.8.6).

**Por que os dois ajustes de `JSON`.** §3.9: as chaves que o cliente controla
(`query`) são chaves de um `Map` e nunca viram caminho; os ajustes defendem o
resto do documento. Sem o escape, uma chave com ponto fora de um *hint* grava
`"a"` como folha e como prefixo, e a leitura devolve um JSON com chave
repetida; sem o segundo, duas chaves que dão o mesmo caminho recusam o `INSERT`
com `117` — `logic`, o lote inteiro. Os dois exigem o ClickHouse 25.8 (§8.8.5).

**O lote não é dividido.** O protocolo nativo não tem teto de parâmetro nem de
pacote (medido: `format_binary_max_string_size` não se aplica, e um `payload`
com 20 000 chaves entra em ~70 ms). O teto real é a memória do servidor, que
o boot confere (§8.8.5).

#### 8.8.4 Desduplicação e at-least-once

Duas camadas, porque o ClickHouse não tem `ON CONFLICT`:

1. **Imediata: `insert_deduplication_token`.** O mesmo token num segundo
   `INSERT` faz o servidor descartar o bloco inteiro como duplicata. O token é
   `whs-` + os 32 primeiros dígitos hexadecimais do SHA-256 dos `uuid` do lote,
   em ordem: o retry de um lote (§10.4), que o worker segura com os mesmos itens
   na mesma ordem, repete o token, e um `INSERT` que deu certo mas cuja resposta
   se perdeu (o prazo estourou depois do commit) não duplica. Lotes diferentes
   nunca têm o mesmo token — os `uuid` são únicos —, e é por isso que ele sai
   do conteúdo, nunca de um contador: um token repetido para dados diferentes
   faria o servidor descartar o segundo lote **em silêncio**. O servidor guarda
   o token por partição: um lote que cruza a virada do mês são dois blocos, e
   um retry depois de uma falha parcial reescreve só o que faltou.
2. **Eventual: `ReplacingMergeTree`.** Uma reentrega fora da janela de tokens
   (1000 blocos na DDL de exemplo), ou agrupada de outro jeito — a reingestão
   de um dump (§9.2), um `ring.db` relido — ganha outro token e é gravada; o
   merge a colapsa pela chave `(event_at, uuid)`. Até lá, a leitura exata usa
   `FINAL`.

O ClickHouse não diz quantas **linhas** foram descartadas: o
`X-ClickHouse-Summary` e o progresso contam as linhas enviadas também num
bloco descartado (medido). O que ele diz é o evento de perfil
`DuplicatedInsertedBlocks`, que o driver entrega por `WithProfileEvents`;
`whs_sink_clickhouse_dedup_blocks_total` o acumula (§8.8.10).

#### 8.8.5 Validação no boot

Na primeira checagem de saúde bem-sucedida, com a regra de §8.5.5: cada recusa
é `ErrFatal`, o `WaitReady` desiste na hora e o processo sai com 3.

| Verificação | Como | Se falhar |
|---|---|---|
| A tabela existe e o usuário a enxerga | `system.tables` | exit 3 |
| ClickHouse 25.8 ou mais novo | `version()` | exit 3 — os dois ajustes de `JSON` de §8.8.3 não existem antes; recusar pela versão dá uma mensagem melhor que o `115` da sonda |
| Motor `ReplacingMergeTree`, `ReplicatedReplacingMergeTree` ou `SharedReplacingMergeTree` | `system.tables.engine` | exit 3 — um `MergeTree` guardaria para sempre cada reentrega fora da janela do token; um `Distributed` entrega aos shards de forma assíncrona e o `200` não significaria nada (§18) |
| O motor não tem coluna `is_deleted` (no máximo a de versão) | `system.tables.engine_full`, os argumentos que não são literais | exit 3 — o merge apaga as linhas que ela marca, e um valor fora de 0/1 (`level` 9) recusa o `INSERT` com `117` |
| A chave de partição, se houver, lê só `event_at` | `system.tables.partition_key` | exit 3 — partição por `received_at` deixava a reentrega noutra partição, que o merge nunca colapsa; uma por `uuid` espalhava um lote por centenas de partições |
| `uuid` é, sozinho, um elemento da chave de ordenação | `system.tables.sorting_key`, separado nas vírgulas de primeiro nível | exit 3 — uma chave sem `uuid` (só `event_at`) colapsaria no merge dois eventos **diferentes** do mesmo microssegundo: perda silenciosa com o sink UP |
| As dez colunas que o servidor escreve, com os tipos de §3.9: `UUID`; `DateTime64` de 6 a 9 dígitos, qualquer fuso; `UInt64` em `sid`, `UInt16` em `step` e `event_nsec`, `UInt8` em `level`; `String` ou `LowCardinality(String)` em `space` e `type`; extras toleradas | `system.columns` | exit 3 — o driver converte os valores no `Append`, no cliente, e recusa um tipo que não é o esperado (um `Int64` para um `[]uint64`, um `Nullable`) antes de mandar qualquer coisa: seria fatal no primeiro lote. Uma precisão menor que 6 trunca o microssegundo em silêncio |
| `payload` é `JSON` com `params Map(String, String)` e `query Map(String, Dynamic)`, e além deles só `max_dynamic_paths`/`max_dynamic_types` | `system.columns.type`, separado nas vírgulas de primeiro nível | exit 3 — sem os *hints*, `params` e `query` vazios somem de toda linha e as chaves da query viram caminhos (§3.9); um `SKIP` descartaria parte do envelope em silêncio, e um caminho tipado a mais (`content.length UInt8`) recusaria o lote que não coubesse nele |
| As dez colunas são colunas comuns (sem `default_kind`, ou `DEFAULT`) | `system.columns.default_kind` | exit 3 — uma `EPHEMERAL` aceita o `INSERT` e não guarda nada (o `payload` inteiro sumia com o `Write` devolvendo sucesso); `MATERIALIZED` e `ALIAS` recusam o `INSERT` |
| `received_at`, se existir, `DateTime64` de 6 a 9 dígitos com `DEFAULT` ou `MATERIALIZED` | `system.columns` (`default_kind`) | exit 3 — sem expressão, o ClickHouse a preencheria com o zero do tipo, 1970, e o atraso medido por ela seria mentira |
| Nenhuma `CONSTRAINT … CHECK`, com o nome nu ou entre crases | `system.tables.create_table_query` | exit 3 — §3.9: a sonda vazia não a exercita, e uma violação seria `logic` em todo lote |
| `max_memory_usage` (quando não é `0`) comporta o pior lote: 128 MiB + 8 × `min(maior envelope × sink.batch, ring.max_bytes)` | `getSetting(...)` na sessão do servidor de webhooks | exit 3, com os números na mensagem — medido no 25.8: o `INSERT` de um lote de `payload` JSON consome ~6× os bytes do lote mais ~130 MiB fixos, e `241` é fatal (§8.8.7); sem a conta, um lote legítimo derrubaria a frota |
| `max_memory_usage_for_user` (quando não é `0`) comporta `min(pool_size, escritores)` vezes isso — escritores são `sink.workers` com ring e `ring.max_inflight` com `ring=none`, passados pela montagem | `getSetting(...)` | exit 3 — o limite do usuário soma todas as consultas dele: quatro lotes legítimos e simultâneos estouravam um limite que a comparação com um só aceitava, e `241` é fatal. Outras instâncias no mesmo usuário somam por cima, e nenhuma as enxerga: dê a cada uma o seu usuário. A conta multiplicava pelo `pool_size`, e a folga do `sink.workers + 1` (que nenhum escritor usa) e a conexão do check (um `SELECT`, fora do pool) não fazem `INSERT`: um limite de exatamente `sink.workers` piores lotes era recusado sem que escrita alguma pudesse estourá-lo; e a mensagem mandava "baixar o pool", que abaixo dos workers só os faz esperar conexão (porte da família syslog/RELP, 2026-10-13) |
| `INSERT` concedido nas dez colunas, sessão gravável, ajustes aceitos | um `INSERT` **vazio**: `PrepareBatch` com o mesmo comando e os mesmos ajustes do dreno, e `Send` sem linhas, sob o mesmo prazo do dreno (`sink.op_timeout`) | exit 3 — `497` é o grant (a mensagem nomeia as colunas), `164` o modo `readonly` (com `readonly=1` o servidor recusa já a mudança de ajuste); zero linhas não criam parte (medido) |

O MySQL prova a escrita com uma linha de sonda numa transação revertida
(§8.6.4). O ClickHouse não tem transação, e uma linha de verdade ficaria na
tabela de produção para sempre. O `INSERT` vazio é a resposta: o servidor
confere o grant coluna a coluna e o modo `readonly` **ao receber o comando**,
antes de ler dado, com o mesmo comando que o dreno usa. O que ele não exercita
— uma CHECK, um tipo que só falharia com um valor — o catálogo cobre.

A validação tem o prazo próprio de §8.5.5, e é o prazo do dreno de propósito: o
driver o converte em `max_execution_time`, e um perfil que proíbe mudar esse
ajuste recusava o dreno com `452` depois de uma sonda que, com outro prazo,
passava. Uma tabela que alimenta *materialized views* gera um `WARN`: uma
recusa da view falha o `INSERT` depois de as linhas estarem gravadas, e elas
são contadas como descartadas.

O pior lote e o número de escritores são calculados pela montagem (`app`), que conhece os dois eixos: com
`ring=none` é o maior envelope (§3.2); com os outros rings,
`min(maior envelope × sink.batch, ring.max_bytes)`. Com os padrões (`5 MiB` de
corpo, lote de 256, `ring.max_bytes` de 512 MiB) dá ~4,1 GiB de exigência. Um
`max_memory_usage = 0` (o padrão do ClickHouse) passa: aí o teto é o do
servidor inteiro (`max_server_memory_usage`), que esta sessão não enxerga.

A validação **não se repete**. Uma tabela dropada depois do boot aparece como
`60` na escrita, que é fatal (§8.8.7).

#### 8.8.6 Durabilidade e `async_insert`: decisão do administrador do ClickHouse

O servidor de webhooks **não envia nem lê** `async_insert`,
`wait_for_async_insert`, `insert_quorum` ou `fsync_after_insert`: valem os do
perfil do usuário no ClickHouse. A decisão (do usuário, 2026-10-07) é que este
destino existe para **vazão**, e que segurança e commit são do administrador do
ClickHouse — a mesma linha de §8.5.6. O que o `200` de `ring=none` significa
depende do perfil:

| Perfil do usuário | O `INSERT` responde quando | Perda numa queda do **ClickHouse** |
|---|---|---|
| `async_insert = 0` (padrão) | a parte foi escrita | o que o `fsync_after_insert = 0` (padrão) deixou no cache do SO, numa queda da máquina |
| `async_insert = 1`, `wait_for_async_insert = 1` (padrão do par) | o buffer do servidor foi descarregado numa parte — agrupa os `INSERT` pequenos de `ring=none`, ao custo de até `async_insert_busy_timeout_ms` de latência | como a linha acima |
| `async_insert = 1`, `wait_for_async_insert = 0` | o servidor **recebeu** o dado no buffer em memória | o buffer inteiro, já respondido com `200` |

Com `async_insert` a deduplicação por token segue o
`async_insert_deduplicate` do perfil, que também é dele. A linha de §16.1 leva
a marca `†`.

#### 8.8.7 Classificação de erro

O servidor entrega o código numérico da exceção (`clickhouse.Exception.Code`),
e a tabela abaixo **é** a função `classifyClickHouseCode`;
`TestClassifyClickHouseCode` a lê de volta. Os nomes foram conferidos no
servidor com `errorCodeToName`. A regra de §8.5.7 vale igual: esquema e
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
| **`164` `242`** | **somente leitura**: o perfil do usuário (`READONLY`), ou a réplica sem Keeper (`TABLE_IS_READ_ONLY`) | **`ErrFatal`** — a decisão de §8.5.7 sobre `25006`, pelos mesmos motivos |
| `241` `173` `243` | sem memória (`MEMORY_LIMIT_EXCEEDED`, `CANNOT_ALLOCATE_MEMORY`), sem disco | `ErrFatal` — coerente com o `OOM` do Redis; o boot confere o limite de memória contra o pior lote (§8.8.5) |
| `192` `193` `194` `195` `497` `516` | usuário, senha, IP proibido, acesso negado, autenticação | `ErrFatal` |
| `16` `47` `53` `60` `81` | coluna, identificador, tipo, tabela ou banco que sumiram: contrato de esquema quebrado | `ErrFatal` |
| `36` `44` `62` `115` `452` | argumento, coluna que não aceita `INSERT`, sintaxe, ajuste desconhecido ou proibido pelo perfil | `ErrFatal` — repetiria em toda tentativa |
| `6` `26` `27` `41` `69` `70` `72` `117` `131` `469` | dado recusado: texto ou número que não converte, JSON inválido, string grande demais, CHECK violada | `ErrLogic` |
| outro código | desconhecido | `ErrNetwork` — encher o ring é visível, descartar não |
| erro do driver ao converter um valor (`ColumnConverterError`, `ErrBatchInvalid`) | o tipo de uma coluna mudou depois do boot | `ErrFatal` — repetiria em todo lote |

Uma coluna mudada para `String` depois do boot escapa: o driver converte o
`[]uint8` de `level` em bytes crus e o `Write` devolve sucesso. A validação não
se repete (`ALTER TABLE` em produção é do operador).

`117` (`INCORRECT_DATA`) é o JSON inválido. Um JLH deste servidor é sempre
JSON válido, e com os dois ajustes de §8.8.3 nenhuma chave de cliente o
provoca: na prática ele só vem de dano.

#### 8.8.8 `Health()`

`SELECT getSetting('readonly')`, que também prova a conexão. Um `readonly`
diferente de zero é
**fatal**: no ClickHouse ele é do perfil do usuário, não de um failover, e não
muda esperando — a lição de §8.2.4 sobre recusa definitiva no boot. Com motor
`Replicated*` (sabido na validação), também
`SELECT is_readonly FROM system.replicas` da tabela: `1` é falha de rede e o
estado vai para DOWN, pelas razões de §8.5.8 (a réplica perdeu o Keeper e
volta sozinha); um `242` chegando pela escrita continua fatal (§8.8.7), o mesmo
par de sinais de §8.5.7 e §8.5.8. Ler `system.replicas` exige
`GRANT SELECT(database, table, is_readonly) ON system.replicas` na imagem
oficial (`select_from_system_db_requires_grant`): sem ele (`497`) a checagem
da réplica é desligada com um `WARN`, em vez de derrubar o processo. O check também atualiza os gauges do pool
(§8.8.10).

As duas consultas do check rodam numa conexão própria (um segundo
`clickhouse.Open` com `MaxOpenConns = 1`), fora do pool dos escritores, pelo
motivo de §8.5.8 (porte da família syslog/RELP, 2026-10-13): dentro dele o check esperava o
*acquire* do driver com todas as conexões em `INSERT`s lentos.

#### 8.8.9 Retenção

O servidor não apaga nada. A poda é do administrador: `ALTER TABLE … DROP
PARTITION` por mês (a DDL particiona por `toYYYYMM(event_at)`), ou um `TTL` na
tabela. O `ReplacingMergeTree` não é uma fila: não há consumidor que apague.

#### 8.8.10 Métricas

```
whs_sink_clickhouse_dedup_blocks_total      counter  blocos descartados pelo insert_deduplication_token
whs_sink_clickhouse_pool_conns{state}       gauge    state: acquired|idle|max
```

Cardinalidade fixa (§11.1). `Stats()` do driver não conta esperas por conexão,
então não há `pool_waits_total`.

#### 8.8.11 Autenticação e TLS

`sink.clickhouse.username`/`password`. TLS como §8.5.11, com
`sink.clickhouse.tls`, `tls_ca` e `tls_insecure`: verificado contra o pool do
sistema ou a CA configurada, `ServerName` vazio para que o `tls.DialWithDialer`
o tire de cada endereço discado (há vários em `addrs`, como no NATS), e
`tls_ca` sem `tls` é erro de configuração. Com TLS, o endereço costuma ser a
porta 9440.

---

## 9. Arquivo de emergência

**Propriedade do ring, não do sink.** Dispara em qualquer estado **FATAL** ou
`drain_timeout` estourado, com `ring.mode = memory` e ring não-vazio.

| `ring.mode` | Ação |
|---|---|
| `memory` | **Dump para arquivo de emergência.** |
| `sqlite` | Nada a fazer — os itens já estão em disco e serão drenados no próximo boot com destino, qualquer que seja o `ring.mode` dele (§7.4). |
| `none` | Nada a fazer — não há ring; as requisições em voo recebem `503`. |

**Escrever é do ring; ler é de todo boot.** Só `ring.mode = memory` com
`emergency.enabled = true` escreve dumps. A reingestão (§9.2) e o informe dos
temporários (§9.1) rodam em **todo** boot, qualquer que seja o `ring.mode` e
com `emergency.enabled` ligado ou não: um dump é de uma vida anterior, não da
configuração desta. Até 2026-10-11 o leitor só existia junto do escritor, e uma
vida seguinte que trocasse o ring para `sqlite` ou `none`, ou desligasse a
emergência, deixava o dump parado no disco, sem uma linha de log, com eventos
respondidos com `200` e nunca entregues (achado pela revisão independente do
porte de 2026-10-11). O mesmo vale para as linhas que um `ring=sqlite` deixa
no `ring.db`: todo boot as procura, e um com outro `ring.mode` as entrega e
remove o arquivo (§7.4; achado pela revisão independente da rodada que
corrigiu o dump, TESTING §8 item 42).

### 9.1 Formato

`<emergency.dir>/emg-<agent_uuid>-<unixnano>.bin`

```
<len_jlh>\n
<bytes do JLH>
<len_jlh>\n
...
```

Cabeçalho de linha em ASCII com **um** comprimento em decimal, seguido do JLH
cru (§3.1). Os bytes são os mesmos que o cliente viu confirmados: a reingestão
entrega o mesmo `uuid`, e os destinos deduplicam por ele.

**Escrito sob um nome temporário** (correção da revisão do
`go-syslog-nats-server`, portada do `go-webhook-nats-server` em 2026-10-11):
`.emg-<agent_uuid>-<unixnano>.bin.tmp`, oculto e fora do padrão que a
reingestão lista, com `O_CREATE|O_EXCL`; depois `fsync` do arquivo, `link` para
o nome final (também exclusivo: um nome que já existe é erro, nunca
sobrescrita), remoção do temporário e `fsync` do diretório. Qualquer falha
antes do nome final remove o que foi escrito e é perda contada (§9.3). Escrito
direto no nome final, como era até então, um processo morto no meio da escrita
(`kill -9`, OOM) deixava um arquivo que, cortado numa fronteira de registro,
**lia limpo** — e era removido depois de entregue em parte, o resto perdido
sem contagem. **Sem *hard link*** (vfat, exFAT, montagens FUSE de *object
store*), o `link` falha qualquer que seja o nome; então, quando ele falha por
outro motivo que não "o nome existe", o nome final é conferido livre e o
temporário é **renomeado** para ele (o carimbo de nanossegundos no nome torna
inofensiva a janela entre os dois). Um nome final ocupado continua recusado,
nos dois caminhos. Um temporário que sobrou de um crash nunca é lido pela
reingestão. No boot (§10.1 passo 4), um temporário que é o **mesmo arquivo** que
o dump do nome final é o gêmeo de um crash entre o `link` e a remoção, e é
removido; qualquer outro é um dump interrompido: fica no disco, nunca é lido,
e o boot o informa num `WARN` com os caminhos. Os eventos dele só foram
contados como perda se um segundo sinal abortou o dump (§10.3); depois de um
`SIGKILL` (o `stop_grace_period` estourado), de um OOM ou de uma queda de
energia, ninguém os contou, e o arquivo pode estar inteiro. Para reingeri-lo, o
operador o renomeia para o nome final (sem o ponto inicial e sem o `.tmp`): os
registros íntegros são entregues no boot seguinte, um último cortado leva o
arquivo à quarentena (§9.2), e os já entregues são duplicatas que os destinos
absorvem pelo `uuid`. Um temporário cortado numa fronteira de registro lê
limpo — o que faltar não aparece em lugar nenhum —, e por isso a decisão é do
operador, não da reingestão.

`O_EXCL` porque um dump nunca acrescenta a outro. O `fsync` do diretório é
melhor esforço: um sistema de arquivos que o recusa, ou um diretório que se
escreve mas não se lê (modo `0300`, que nem abre para o `fsync`), é tolerado —
o arquivo já está completo, sincronizado e no nome final, e descartá-lo por isso
perderia o dump (antes, a falha ao abrir o diretório removia o dump pronto).

### 9.2 Reingestão

No boot — em todo `ring.mode`, com `emergency.enabled` ligado ou não (§9) —,
todo arquivo `emg-*.bin` em `emergency.dir` é entregue **direto ao
sink**, em lotes, sem passar pelo ring — que ainda não serve e não deve encher com
o backlog que existe para absorver —, **antes** da drenagem do passo 5 (§10.1), em ordem lexicográfica de nome (que é
temporal pelo sufixo unix), e **removido só após todos os seus itens serem
confirmados no sink**.

Cada registro tem o cabeçalho do JLH conferido (o mesmo leitor de §12.1 que os
destinos usam) e o JSON inteiro validado (`json.Valid`: o leitor só varre as
chaves do payload, e `{]` passaria): um registro que não é o que este servidor
escreve conta como corrompido.

Um arquivo corrompido ou truncado (comprimento declarado maior que os bytes
restantes: um dump de antes de 2026-10-11, escrito direto no nome final e
cortado, um temporário renomeado à mão, dano em disco) é processado até o último registro íntegro; o resto é logado em
`ERROR` com a contagem de bytes descartados (do início do registro danificado,
a linha de comprimento incluída), e o arquivo é renomeado para
`.corrupt` em vez de removido.

**A remoção e a quarentena fazem `fsync` do diretório** (porte da família
syslog/RELP, 2026-10-13). Sem ele, uma queda de energia logo depois da
reingestão podia trazer o nome de volta, e o boot seguinte entregava o dump
inteiro de novo com os mesmos `uuid` — absorvido só onde o destino deduplica.
O `fsync` vem **depois** do `unlink` ou do `rename`, então a falha dele
(inclusive a de **abrir** o diretório, num modo `0300`) não desfaz nada: o
arquivo já saiu (ou já é `.corrupt`, ou `.legacy`). O boot a registra num
`WARN` que diz o que aconteceu e o risco de uma segunda entrega, em vez do
`ERROR` "cannot remove" — falso, com o arquivo já removido — ou do "cannot
quarantine" com um caminho que não existia mais, que ainda engolia a linha com
os bytes descartados. Só a escrita do dump mantém o `fsync` do diretório como
melhor esforço (§9.1): ali o arquivo está completo e no nome final, e jogá-lo
fora perderia o dump.

**Com `sink.type = none` nada é reingerido.** Entregar o dump de uma vida
anterior a um sink que descarta por configuração (§8.4) e então remover o
arquivo destruiria eventos respondidos com `200` por um destino de verdade —
um teste de carga no mesmo volume depois de um incidente. Os dumps ficam no
disco, contados nos gauges (§11.1), e o boot os informa num `WARN`; uma vida
com destino os reingere. Até a revisão desta rodada (2026-10-11) o par
`ring=memory` + `sink=none` os removia assim, e ler em todo modo estendera isso
a `ring=sqlite` e `ring=none`. As linhas de uma vida anterior no `ring.db`
seguem a mesma regra (§7.4).

**Um `emergency.dir` que não se lista** encerra o boot com 1 só quando esta
vida escreve dumps (`ring=memory` com `emergency.enabled`), porque o dump
precisará dele. Nos outros modos vira um `ERROR` nomeando o diretório, e o boot
segue sem reingerir: uma vida que nunca escreve ali não para por causa dele —
até 2026-10-11 nem o olhava.

Uma recusa do sink durante a reingestão mantém o arquivo e encerra o boot com 3.
Um `SIGTERM` no meio dela — ou da drenagem do backlog do passo 5 — encerra com
130, a saída de um boot interrompido, e também mantém o arquivo (ou as linhas
no `ring.db`); até 2026-10-07 saía com 3, como se o destino tivesse recusado
(achado no `go-webhook-pg-server`).

**Dump de antes do LogHub.** O formato da v1 tinha **dois** comprimentos no
cabeçalho (envelope antigo e corpo). Um arquivo cujo primeiro cabeçalho são dois
inteiros não negativos não é reingerido (dois campos que não são comprimentos
são dano, `.corrupt`): é
renomeado para `.legacy`, com um `ERROR` dizendo para reingeri-lo com a versão
anterior. Nada é perdido nem misturado.

**Só arquivos regulares** são dumps: um FIFO com nome de dump bloqueava o
`open` para sempre, e o boot com ele, sem atender `SIGTERM`; um *symlink* podia
apontar para `/dev/zero`.

### 9.3 Falha do próprio dump

Se o dump falhar (disco cheio, sem permissão): log
`ERROR` com a contagem exata de eventos perdidos,
`whs_events_dropped_total{reason="emergency_failed"} += n`, e saída com código 3.

Com `emergency.enabled = false` não há dump que falhe: os eventos que o
shutdown não entregou são perda do aborto, `whs_events_dropped_total{reason="abort"}
+= n`, contados uma vez, e a saída também é 3. Até 2026-10-11 esta seção os
punha em `emergency_failed`, ao contrário do código e do
`TestFatalSinkWithoutEmergencyCountsTheLossAsAbort`.

---

## 10. Ciclo de vida

### 10.1 Boot

```
1. parse da config (§13); erro ou combinação inválida => exit 1
   --help, --version e --dump-config encerram aqui, sem tocar o disco
1a. IDENTIDADE (§3.5): cria ring.data_dir, exporta-o como DATADIR e chama
   lhident.Initialize() UMA vez (só no main). Falha => a biblioteca sai
   sozinha com 100..114 (§10.5); from e space são validados (exit 1)
2. abre a porta ADMIN imediatamente
      /healthz -> 200 (processo vivo)
      /readyz  -> 503 com o motivo em texto
      /metrics -> já exportando
3. inicializa o SINK
      redis:  conecta e sonda (XLEN + XADD NOMKSTREAM); retry com
              backoff 100ms -> 5s até sink.boot_timeout (300s);
              estourou => exit 3; recusa permanente (WRONGPASS,
              NOPERM, WRONGTYPE) => exit 3 na hora
      sqlite: abre o arquivo corrente, cria esquema e índices
              erro de I/O => exit 3
      postgres: pool preguiçoso; a primeira checagem de saúde válida
              verifica tabela, colunas, índice único, INSERT e modo de
              escrita (§8.5.5). Retry com o mesmo backoff do redis;
              erro FATAL (esquema, credencial) => exit 3 IMEDIATO,
              sem esperar sink.boot_timeout
      mysql:  pool preguiçoso; a primeira checagem de saúde válida
              verifica motor, colunas, índice único, max_allowed_packet
              e um INSERT de sonda revertido (§8.6.4); mesma regra do
              postgres para erro FATAL
      nats:   conecta na primeira checagem de saúde; cada uma verifica
              stream, subject, plano de controle, selo, no_ack e tamanhos
              (§8.7.5); recusa de autenticação, permissão negada ou
              JetStream desligado => exit 3 IMEDIATO
      clickhouse: pool preguiçoso; a primeira checagem de saúde válida
              verifica versão, motor, chave, colunas, CHECKs, limite de
              memória e um INSERT vazio de sonda (§8.8.5); mesma regra
              do postgres para erro FATAL
      none:   no-op
      todo erro FATAL no boot sai com o log "the sink refused this server
      permanently", não "did not become reachable within
      sink.boot_timeout", que mandava o operador olhar a rede quando o
      esquema, o stream ou a credencial estavam errados
4. se ring.mode = sqlite: abre ring.db e conta o backlog
   (uma tabela `ring` da v1 com linhas => ERROR e fica intocada, §7.4;
   com sink=none o backlog encontrado fica no arquivo, fora da conta
   do ring, §7.4)
   se existem arquivos emg-*.bin: entrega direto ao sink (§9.2), em
   qualquer ring.mode e com emergency.enabled ligado ou não (§9)
   (um dump de dois comprimentos, da v1 => renomeado .legacy, §9.2;
   o temporário gêmeo de um dump é removido, o de um dump interrompido
   => WARN, intocado, §9.1)
5. DRENA o backlog integralmente
      com ring.mode != sqlite, o de um ring.db que uma vida ring=sqlite
      deixou em ring.sqlite.path: sondado sem criar arquivo nem tabela,
      entregue e então removido se o arquivo é só do ring (§7.4)
      com sink=none o backlog de uma vida anterior não é entregue => WARN,
      e o arquivo que o tem fica (§7.4)
      a porta pública permanece FECHADA
      /readyz reporta: "draining backlog: 412k pending"
6. abre a porta pública
7. /readyz -> 200
```

A porta admin aberta durante todo o processo é o ponto: mesmo num crashloop por
sink indisponível, o operador tem `/readyz` com o motivo exato e `/metrics` com
a contagem de backlog. Sem isso, restaria só o log do container.

Drenar tudo antes de abrir garante ordem estrita e um estado inicial limpo, ao
custo de indisponibilidade proporcional ao backlog. Com `sink.boot_timeout`
estourado, o processo sai com código 3 e o supervisor reinicia — o crashloop é
**intencional** (falha fechada: nada é aceito que não possa ser persistido) e
dispara os alertas de `CrashLoopBackOff` que os times já têm.

#### Validação de configuração

| Combinação | Efeito |
|---|---|
| `ring=sqlite` + `sink=sqlite` com o **mesmo arquivo** | `exit 1` — `ring.sqlite.path` e `sink.sqlite.path` devem diferir |
| `ring=sqlite` + `sink=sqlite` com arquivos distintos | Permitido, com `WARN`: o evento sofre **dois** `fsync` antes de ser entregue; `ring=memory` + `sink=sqlite` dá a mesma garantia de durabilidade pela metade do custo. Legítimo quando os dois estão em discos diferentes. |
| `sink=sqlite` com `sink.workers > 1` | Permitido, com `WARN`; forçado a 1 (§8.3.1) |
| `sink=postgres` com `sink.workers` **no default** | Rebaixado para 4, com `WARN` (§8.5.3). Um valor configurado é respeitado; `WARN` acima de 16 |
| `sink=postgres` com `ring=none` e `ring.max_inflight > pool_size` | Permitido, com `WARN`: o excedente espera conexão dentro do pool (§7.5) |
| `sink=postgres`, `sink=mysql` ou `sink=clickhouse` com um ring e `pool_size` explícito abaixo de `sink.workers` | Permitido, com `WARN`: os workers esperam conexão uns dos outros. Igual a `sink.workers` é legítimo: o check tem conexão própria (§8.5.3) |
| `sink=postgres` com `min(max_body_bytes × batch, ring.max_bytes) > 1 GiB` | Permitido, com `WARN`: um lote pode exceder o teto de um parâmetro do protocolo (§8.5.4) |
| `sink=postgres` com `tls_ca` sem `tls` | `exit 1` (§8.5.11) |
| `sink=postgres` com `table` que não é identificador simples (`nome` ou `esquema.nome`, minúsculos) | `exit 1` |
| `sink=mysql` com `sink.workers` **no default** | Rebaixado para 4, com `WARN` (§8.6.2) |
| `sink=mysql` com `ring=none` e `ring.max_inflight > pool_size` | Permitido, com `WARN` (§7.5) |
| `sink=mysql` com `sink.batch > 5000` | `exit 1`: 65 535 placeholders por comando (§8.6.3) |
| `sink=mysql` com `tls_ca` sem `tls`, ou `table` que não é identificador simples (`nome` ou `banco.nome`) | `exit 1` |
| `sink=mysql` ou `sink=clickhouse` com `database` acima de 64 **caracteres**, ou com crase, contrabarra ou byte de controle | `exit 1` (§8.6.3) |
| `sink=nats` com mais de um método de autenticação, `password` sem `username`, `stream` ou `subject` inválidos, ou `tls_ca` sem `tls` | `exit 1` (§8.7.9) |
| `sink=nats` com uma URL que não parseia, sem host, com caminho, consulta ou fragmento, ou com *userinfo* junto de `username`/`password`/`token`/`credentials` | `exit 1`, sem citar a URL (§8.7.9) |
| `sink=clickhouse` com `sink.workers` **no default** | Rebaixado para 4, com `WARN` (§8.8.2). Um valor configurado é respeitado; `WARN` acima de 16 |
| `sink=clickhouse` com `ring=none` e `ring.max_inflight > pool_size` | Permitido, com `WARN` (§7.5) |
| `sink=clickhouse` com `addrs` vazio ou fora de `host:porta`, `username` ou `database` vazios, `pool_size` negativo, `tls_ca` sem `tls`, ou `table` que não é identificador simples (`nome` ou `banco.nome`, caixa preservada) | `exit 1` (§8.8.1, §8.8.11) |
| `ratelimit.enabled=false` + `server.bind` não-loopback | Permitido, com `WARN` (§6.1) |
| `GOMEMLIMIT` ausente, ou `< ring.max_bytes * 2` com `ring=memory` | Permitido, com `WARN` (§12.4) |
| Linha de INI sem `=` ou cabeçalho sem `]`; argumento sem `--` | `exit 1` com o número da linha ou o nome antes do `=`, nunca o valor (`password: x` é exatamente a linha que cai aqui) (§13.2) |
| **Tetos** (porte da família syslog/RELP, 2026-10-13): `ring.capacity_items` > 16 777 216 com `ring.mode=memory`, > 2⁴⁰ com `sqlite` ou `none`; `ring.max_inflight` > 1 048 576; `ring.sqlite.commit_batch` > 65 536; `sink.batch` > 1 048 576 (o MySQL mantém os seus 5000); `sink.workers` > 1024; `sink.postgres`/`mysql`/`clickhouse.pool_size` > 4096; `server.listeners` > 256; `sink.redis.ring_scale_each_conn` fora de 0–20; `sink.redis.pipeline_multiplex` fora de −1–8; `sink.redis.read_buffer_each_conn` ou `write_buffer_each_conn` > 256 MiB | `exit 1`, nomeando a chave. Sem eles um erro de digitação de poucos dígitos derrubava o boot com pânico (`makechan` do ring de memória e do `ring.sqlite`; `makeslice` do anel e dos buffers do rueidis), alocava um gigabyte por `Dequeue` (`sink.batch`), dava a volta no `int32` do pool do pgx (`pool_size = 4294967300` virava 4) ou tornava negativo o `MaxPending` do NATS (`workers × batch + max_inflight`), que o sink trocava por 4096 em silêncio; um `pipeline_multiplex` acima de 8 o próprio rueidis recusa, mas no `connect`, retentado até o `boot_timeout`. Só o ring de memória faz um canal de `capacity_items` posições: no `ring.db` 50 milhões de linhas são configuração legítima, e um lote de 100 000 linhas no ClickHouse também. O par `ring.max_inflight` ↔ `MaxPending` é da área do NATS (§8.7) |
| `sink=postgres` com `addr` que não é um servidor só — `host:porta` (porta 1–65535 escrita sem sinal nem zero à esquerda) ou diretório de socket (começa com `/`, com `:porta` opcional, sem vírgula nem branco); recusados `h1,h2:5432`, tab, `[h:5432]:5433`, `pg-1` sem porta | `exit 1` (§8.5.2): passava aqui e falhava no DNS ou no *dial*, erro `network` retentado até o `boot_timeout` (porte do `go-syslog-pg-server`/`go-relplog-pg-server`, 2026-10-13) |
| `sink=redis` com uma entrada de `addrs` que não é `host:porta` (sem porta, com branco, porta fora de 1–65535) | `exit 1`: o rueidis disca toda entrada como TCP `host:porta`, e o erro do *dial* era retentado até o `boot_timeout`. A vírgula é o separador da lista: não há URL Redis neste servidor cujo host pudesse levar uma (porte da família syslog/RELP, 2026-10-13) |
| `--help`, `--version` ou `--dump-config` com `=valor`; `--config=` vazio | `exit 1`, sem o valor (§13.2) |
| Nome de parâmetro desconhecido sem forma de nome, ou que começa com o nome de um parâmetro e continua; chave desconhecida do INI fora de uma seção deste servidor, sem forma de chave ou logo depois de um segredo; flag desconhecida fora das seções deste servidor logo depois de uma flag com valor | `exit 1`, nomeando só a linha ou a flag anterior, **nunca o conteúdo** (§13.2) |
| Flag sem valor seguida de outra flag (`--sink.workers --sink.nats.password=x`, um `$WORKERS` vazio num script); valor depois de um espaço (`--sink.nats.password= x`, `--sink.nats.password = x`, `--sink.nats.password= -x`); valor que o shell partiu num espaço (`--sink.nats.password=$P`, com espaço em `$P`); valor que começa com `--` na forma separada (`--sink.nats.password --x`) | `exit 1`, nunca com o valor: um argumento que começa com `--` (ou `-` com o nome de um parâmetro ou de um *switch*) nunca é tomado como valor de outra flag, e só é nomeado se for uma flag deste servidor; o argumento depois de um valor vazio ou de um `=` sozinho não é citado, nem como nome de flag desconhecida; uma palavra solta depois de qualquer valor não é citada. Um valor que começa com `-` vai depois do `=` (§13.2; revisão do porte de volta ao N, 2026-10-11) |

### 10.2 Saúde do sink

Máquina de estados dirigida **exclusivamente pelo `Health()`** do sink.

```
UP ---- Health() falha (timeout sink.health_timeout) ----> DOWN
DOWN --- Health() sucesso -------------------------------> UP
DOWN --- persiste por sink.fail_timeout (30s)
         OU sink.max_retries (10) checagens falhas ------> FATAL
qualquer --- Write() retorna ErrFatal -------------------> FATAL
```

- `Health()` a cada `sink.health_interval` (padrão 2s). **Latência de detecção =
  até um intervalo de health check.**
- Erros de `Write()` classificados como `ErrNetwork` **não** mudam o estado: o
  item continua no ring e é retentado (§10.4). Ninguém recebeu `200` por um
  evento perdido.
- `ErrLogic` → `500`, log `ERROR`, evento descartado.
- `ErrFatal` → FATAL imediato.
- **`sink=none` nunca sai de UP.**

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
fase 5  fecha o sink, fecha o SQLite, exit 0
```

**Segundo sinal** em qualquer fase → pula direto para a saída, aceitando a perda
(contada em `whs_events_dropped_total{reason="abort"}`, inclusive o que já tinha
sido recolhido para um dump ainda não ligado ao nome final, e uma vez só: o que o aborto
conta o dump não conta de novo), `exit 130`. No shutdown **por FATAL** nenhum
sinal o iniciou: o primeiro `SIGTERM` ou `SIGINT` que chega — o comum de um
orquestrador — é o primeiro, registra um `WARN` e o dump continua; só um
segundo aborta. Até 2026-10-07 esse primeiro sinal jogava o dump fora (achado
no `go-webhook-pg-server`). **`SIGQUIT`** durante o shutdown aborta como o
segundo sinal e nunca é tolerado, nem como primeiro sinal do shutdown por
FATAL; fora dele vale a ação padrão do Go (despejo das goroutines e saída `2`),
que é o que um operador quer ao mandá-lo para um processo travado. A frase
anterior ("sempre aborta imediatamente") prometia mais do que o código fazia.
O observador do segundo sinal é registrado **antes** de o contexto do primeiro
soltar `SIGTERM`/`SIGINT`: no meio, um segundo sinal recebia a ação padrão do
Go e matava o processo com 143, sem contar nada (porte da família
syslog/RELP, 2026-10-13).

**Um dono só para a perda do aborto** (forma final da família syslog/RELP,
portada em 2026-10-13). Até ali, o aborto contava o ring até o recolhimento e,
depois dele, um contador do que fora recolhido: um aborto entre o `Drain` (que
já tira os itens da conta do ring) e o registro do recolhimento contava a
menos, e um aborto no meio do dump contava os eventos enquanto a escrita
seguia e podia ligar o arquivo ao nome final antes do `os.Exit` — o boot
seguinte o reingeria, perda contada a mais. Agora:

- antes de recolher as sobras (`Pending` dos workers e o `Drain`), o shutdown
  passa a conta do ring para um contador próprio, sob a mesma trava do aborto:
  o aborto cai inteiro antes (conta o ring, e o recolhimento nem começa) ou
  inteiro depois (conta o contador);
- o contador passa a ser exatamente o que vai para o dump, a menos que o aborto
  já o tenha tomado; e o dump o toma de volta, também sob a trava, assim que o
  resultado é conhecido e antes de contar qualquer coisa. Fora da trava, um dump
  que via o aborto marcado antes de o aborto tomar o contador contava a perda
  inteira como `emergency_failed`, e o aborto dizia que nada se perdera (X2 da
  revisão do porte do `go-syslog-server`);
- um dump que o aborto já contou **desiste**: a escrita confere a marca do
  aborto antes do primeiro registro, a cada 1024 e antes do `link`, e remove o
  temporário em vez de publicar um arquivo que o boot seguinte reingeriria. Um
  dump que publica entre a última conferência e o aborto fica contado **e**
  guardado — contagem a mais, nunca perda —, e nada mais é contado. Um
  temporário ainda pode sobrar quando o `os.Exit` corta a escrita entre duas
  conferências (§9.1): os eventos dele são a perda que o aborto contou;
- com `emergency.enabled = false`, a perda continua sendo do aborto (§9.3), e
  conta uma vez, por quem tomar o contador.

`shutdown.pre_stop_delay` existe para Kubernetes: a remoção do endpoint no LB é
assíncrona e o pod continua recebendo tráfego **depois** do `SIGTERM`. Sem a
espera, o emissor recebe conexão recusada — um erro de rede, não um `503`
retentável. Valor sugerido em K8s: 5–10s.

Se o `drain_timeout` estourar com itens ainda no ring, executa §9 e sai com
código 3. Um sink que vira **FATAL durante a fase 4** encerra a espera na hora,
com um `ERROR` — nada mais escoa, e esperar o resto do prazo só adiaria o dump,
talvez para depois do `SIGKILL` do orquestrador —, e a saída é 3 sempre que a
saúde termina FATAL, em qualquer ring (§10.2, §10.5; até 2026-10-13, §19.1
item 21: com `ring=sqlite` ou `none` o processo saía com 0 com o destino FATAL). **Um prazo, não dois** (correção do `go-webhook-nats-server`, portada
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

Isso vale para `ring=memory` e `ring=none`. Com `ring=sqlite` não há o que
despejar: as linhas continuam no `ring.db` e o próximo boot as drena, então o
processo registra `WARN`, **não** conta nada em `whs_events_dropped_total` e
sai com `0` — a menos que o sink esteja FATAL, quando o código continua `3`.
Um segundo sinal com `ring=sqlite` também não perde nada: sai com 130 sem
contar descarte. O `WARN` dá as linhas que ficam no `ring.db`, e o lote que um
worker retinha é parte delas: uma linha só sai da conta do ring no `Commit`.
Com `sink=none` ele soma também as de uma vida anterior que esta deixou
intocadas (§7.4), repetidas num campo à parte.
Até 2026-10-11 o lote retido era somado de novo e aparecia duas vezes no número
(achado pela revisão independente do porte).

Um `SIGHUP` em qualquer fase não interrompe nada (§13.3).

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
| `0` | Shutdown gracioso completo: ring vazio, ou `ring=sqlite` com linhas que o próximo boot drena |
| `1` | Erro de configuração ou falha de inicialização não relacionada ao sink |
| `3` | Sink indisponível (FATAL, `boot_timeout`, ou `drain_timeout` estourado) |
| `100`–`114` | Identidade: o `go-loghub-ident` sai sozinho (`DATADIR` inválido ou sem escrita, `AGENT_UUID` que não é UUIDv7, `HOSTNAME` fora da RFC 1123, `WORKSPACE` com `_`...). A tabela completa está na skill `go-loghub-ident` §8 |
| `130` | Abortado por segundo sinal, ou boot interrompido por sinal: na espera do sink, na reingestão, na drenagem do backlog e no `bind` da porta admin ou da pública. Um nome de host no `bind` falha com "operation was canceled" e saía com 1, como um endereço errado; um IP literal nem olha o sinal, e o boot abria a porta, respondia `/readyz` 200 e saía com 0 pelo caminho gracioso. Agora o boot confere o sinal depois do último `bind` e antes de servir (porte da família syslog/RELP, 2026-10-13) |

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
| `/readyz` | `200` somente se: sink `UP`, ring abaixo de `ring.high_water`, backlog de boot zerado, e não em shutdown. Caso contrário `503` com o motivo em `text/plain`. |
| `/metrics` | Exposição Prometheus em formato texto. |

### 11.1 Métricas

Regra de cardinalidade: **nenhum label de path, método, IP ou user-agent.** Todo
label tem conjunto de valores finito e conhecido em tempo de compilação. Path vem
de tráfego arbitrário da internet — um scanner com 100k paths distintos
derrubaria o Prometheus, não o servidor.

```
whs_requests_total{code}                    counter
whs_rejected_total{reason}                  counter
    reason: ratelimit|ring_full|too_large|sink_down|shutdown|bad_request
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
whs_ratelimit_buckets                       gauge
whs_clientip_untrusted_forwarded_total      counter
whs_config_info{ring_mode,sink_type}        gauge (=1)
whs_build_info{version,commit,go}           gauge (=1)
```

Métricas específicas de `sink=sqlite`:

```
whs_sink_sqlite_file_bytes                  gauge
whs_sink_sqlite_rotations_total             counter
whs_sink_sqlite_files_removed_total         counter
```

Métricas específicas de `sink=postgres` (§8.5.10):

```
whs_sink_postgres_duplicates_total          counter
whs_sink_postgres_pool_conns{state}         gauge
    state: acquired|idle|max
whs_sink_postgres_pool_waits_total          counter
```

Métricas específicas de `sink=mysql` (§8.6.9):

```
whs_sink_mysql_duplicates_total             counter
whs_sink_mysql_pool_conns{state}            gauge
    state: acquired|idle|max
whs_sink_mysql_pool_waits_total             counter
```

Métricas específicas de `sink=nats` (§8.7.8):

```
whs_sink_nats_duplicates_total              counter
whs_sink_nats_reconnects_total              counter
```

Métricas específicas de `sink=clickhouse` (§8.8.10):

```
whs_sink_clickhouse_dedup_blocks_total      counter
whs_sink_clickhouse_pool_conns{state}       gauge
    state: acquired|idle|max
```

**`whs_sink_errors_total{kind}`.** `timeout` conta a escrita que estourou
`sink.op_timeout` e a sonda que estourou `sink.health_timeout`: as duas são
retentadas como `network`, mas "lento" e "inalcançável" são incidentes
diferentes. Uma escrita cortada pelo cancelamento do próprio shutdown não é
falha do destino e não conta em série nenhuma.

**`whs_emergency_files` e `whs_emergency_events_pending`** dizem o que está no
disco à espera de reingestão (§9.2). Quando a reingestão do boot começa,
`whs_emergency_files` passa ao número de dumps listados; ao fim dela (com
`sink=none`, que não reingere, no lugar dela) os dois
são **recontados do disco**, a única fonte honesta: um arquivo que o boot não
conseguiu ler fica lá e conta como arquivo (os eventos dele não se sabem e não
entram em `whs_emergency_events_pending`), e um que foi entregue mas não pôde
ser removido conta com os seus eventos, que voltam no boot seguinte. Se a
recontagem falha, um `WARN` diz que os gauges ficaram com os valores de antes.
Um dump **soma** um arquivo e os seus eventos, sem reler o diretório: reler decodifica
cada arquivo inteiro, com o processo saindo e o que sobrou do ring ainda vivo
na memória. Até 2026-10-11 a reingestão zerava os dois no fim mesmo com um
arquivo ilegível deixado no lugar, e o dump os punha em 1 e no tamanho dele em
vez de somar (achado pela revisão independente do porte; a forma é a do
`go-webhook-pg-server`, que aqui conta também o arquivo ilegível).

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
0.00005   50µs     piso do modo ring=none + sink=none
0.0001   100µs
0.00025  250µs
0.0005   500µs
0.001      1ms
0.0025   2.5ms
0.005      5ms
0.010     10ms  ◄ SLO: ring=memory + sink=redis (§15.5)
0.025     25ms  ◄ SLO: ring=sqlite, synchronous=FULL (§15.5)
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

Faixa útil por sink: `redis` com auto-pipelining vive em 0,2–5ms; `sqlite` com
`synchronous=FULL` vive em 1–50ms. As duas regiões estão densamente cobertas.

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

**Por padrão registra apenas**: boot, mudanças de estado do sink, fases do
shutdown, rotações de arquivo, contagens de descarte, e erros. **Nada por
requisição.** O dump da configuração efetiva (§13.2, com os segredos redigidos) sai
no boot em nível `debug`: no padrão `info` ele não aparece.

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

Os destinos que precisam de campos do cabeçalho (`space` e `type` para o
stream e o subject; as colunas da `inbox`) os **leem do próprio JLH** com um
leitor estrito do cabeçalho que este servidor escreve (`envelope.ParseHead`),
e não de uma cópia guardada no item: assim um item que volta do `ring.db` ou de
um dump é indistinguível do que entrou. O leitor não aloca além das três
strings curtas (`from`, `space`, `type`) e recusa qualquer JLH que não tenha
exatamente a forma de §3.1 — `uuid` e `from` UUIDv7 em minúsculas inclusive (o
`uuidv7.FromString` aceita maiúsculas, e a regra genérica do `from` do JLH,
qualquer versão de 1 a 8). Do payload ele só confere que é um objeto que fecha o
envelope; a reingestão de um dump (§9.2) valida o resto.

### 12.2 Corpo e buffer do JLH

`sync.Pool` com **classes por potência de 2**: 4 KiB, 16 KiB, 64 KiB, 256 KiB,
os **mesmos** pools para o corpo lido e para o JLH (que leva o corpo dentro),
não um conjunto para cada. Acima de 256 KiB o buffer é
alocado fresco com `make` e **nunca entra no pool**: pooling de buffers grandes
faz o pool reter a memória do pior caso para sempre, e o GC do Go já lida bem
com objetos no large object heap.

O corpo vive só entre a leitura e a serialização: a pré-passada (§3.2) diz o
tamanho exato da sua representação, o JLH é pedido ao pool já com essa
capacidade, e o buffer do corpo volta ao pool assim que o JLH é escrito — nada
mais o referencia. Um JLH que cresceu além da capacidade pedida (só se a
estimativa do cabeçalho falhar) deixa de ser do pool e vai para o GC.

### 12.3 Regra de propriedade única

> O item do ring é **dono** do buffer do JLH. Ele volta ao pool **apenas**
> depois de o item ser confirmado, e **apenas uma vez**.

Ponto de devolução por combinação:

| `ring` | Devolve o buffer ao pool quando |
|---|---|
| `memory` | O `sink.Write()` do lote retorna sem erro, ou com uma recusa `logic` (o lote sai do ring, entregue em parte ou não, §8.1) |
| `sqlite` | O `COMMIT` do grupo no `ring.db` termina (os bytes já estão no banco) |
| `none` | O `sink.Write()` síncrono retorna |

Nos workers, o `ring.Commit` vem **antes** do `Release`, no sucesso e na
recusa `logic`: o ring desconta `it.Size()`, que é `len(Env)`, e o `Release`
esvazia o `Env`. Na ordem inversa — a da recusa `logic` até 2026-10-11 — o ring
continuava contando os bytes de um lote que ninguém mais tinha, até recusar tudo
como cheio com nada dentro (achado no `go-webhook-nats-server`).

Num retry de lote (§10.4), o buffer **não** é devolvido — o item ainda é o dono.
No dump de emergência (§9), o buffer é devolvido só após o `fsync` do arquivo.

**Caminhos de erro.** Um `Enqueue` que falha **não** significa que ninguém mais
referencia o item. A regra é por causa do erro:

| Erro do `Enqueue` | Quem ainda pode ler o buffer | O que o handler faz |
|---|---|---|
| `ErrFull`, `ErrClosed` | ninguém: o ring recusou antes de receber o item | devolve ao pool (`Release`) |
| recusa permanente do destino (`logic`, só com `ring=none`) | ninguém: o destino respondeu, logo o comando foi consumido inteiro | devolve ao pool |
| qualquer outro erro: estouro de `sink.op_timeout`, cancelamento, falha de conexão | o pipeline do cliente Redis: o rueidis devolve cedo num contexto cancelado, mas deixa o comando na fila e ainda o envia ("the command is likely sent already") | **descarta sem devolver** (`Discard`): o buffer fica para o GC |

Nos workers de dreno vale a regra correspondente: uma tentativa que termina em
erro de contexto (`sink.op_timeout` estourado, shutdown) **contamina** o lote, e
os buffers desses itens não voltam mais ao pool — nem quando a retentativa
seguinte tem sucesso. O comando da tentativa anterior pode ainda estar sendo
serializado por outra conexão do pipeline, e devolver o buffer o corromperia. O
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
  estiver ausente ou, com `ring=memory`, menor que `ring.max_bytes * 2`. Com
  `ring=sqlite` e `ring=none` não há número a sugerir: o que fica residente são
  os corpos em voo (`ring.max_inflight × server.max_body_bytes` com `none`;
  `sink.workers × sink.batch × server.max_body_bytes` com `sqlite`), tetos de
  pior caso grandes demais para virar limite — o aviso só sai se a variável
  faltar, e cita o teto. Definir automaticamente seria comportamento mágico, e
  ler cgroups corretamente (v1 vs v2, hierarquias aninhadas) é mais sutil do
  que parece. Os compose de `docker/` definem `GOMEMLIMIT` **e** o limite de
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
  `WHS_SINK_REDIS_ADDRS=10.0.0.5:6379`
- INI: seção `[ring]`, chave `capacity_items`
- `--config=/caminho/arquivo.ini` (env `WHS_CONFIG`) indica o arquivo.

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
registry inteiro, e sem o `--help` e o dump gerados da mesma fonte.

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
(`--sink.nats.password --x`) era "nomeado" como a flag `--x`; um valor com
traço depois de um `=` vazio (`--sink.nats.password= -x`) saía como parâmetro
desconhecido; e a cauda de um valor que o shell partiu num espaço
(`--sink.nats.password=$P`, com espaço em `$P`) saía como argumento solto.
Agora só se nomeia uma flag que este servidor conhece, nada depois de um valor
vazio é citado, e uma palavra solta depois de qualquer valor também não. Um
valor que começa com `-` vai depois do `=`. A cauda partida que começa com traço
depois de um valor **não** vazio (`--sink.nats.password=a -b`) era uma brecha
deixada de propósito, para não esconder o erro de digitação de uma flag depois
de outra; a regra abaixo a fecha (2026-10-13).

**Porte da família syslog/RELP (2026-10-13)** — as regras de configuração que
a revisão do `go-relplog-server`, do `go-relplog-pg-server` e do
`go-relplog-nats-server` firmou, e que o `go-syslog-server` e os cortes dele já
seguem:

- **A cauda depois de um valor é nomeada só dentro de uma seção conhecida.**
  Depois de uma flag com valor, uma flag desconhecida pode ser a cauda desse
  valor que o shell partiu num espaço (`--sink.nats.password=abc -hunter2`,
  `... abc --x=y`): ela só é nomeada quando o nome, lido como
  `<seção>.<chave>`, cai numa seção deste servidor (`--sink.wrkers=1` depois de
  `--sink.workers=4` é um erro de digitação e aparece); senão o erro culpa a
  flag anterior ("a value with a space goes in quotes"), sem citar nada. Sem
  valor antes, toda flag desconhecida continua nomeada. Era a brecha acima:
  esconder a cauda não esconde mais os erros de digitação que importam.
- **Um `-h` ou `-v` depois de uma flag que espera valor** só é nomeado como o
  *switch* quando escrito com dois traços: `--sink.nats.password -h` é também
  como fica uma senha de dois caracteres que começa com traço.
- **Valor colado ao nome.** Um nome desconhecido que começa com o nome de um
  parâmetro e continua (`--sink.postgres.passwordhunter2`, com ou sem `-`, `_`,
  `.` no meio; o `=` esquecido) não é citado, nem o que tem caracteres que
  nenhum nome tem (`--sink.nats.password:hunter2`, um `:` no lugar do `=`). O
  mesmo vale para a chave do INI (`password: hunter2 = x`) e para a chave antes
  de qualquer seção.
- **Continuação de segredo no INI.** Uma chave desconhecida do INI só é
  nomeada quando a seção é deste servidor, a chave tem forma de chave
  (`[a-z0-9_]{1,64}`) **e** a linha anterior não era um segredo (senha, token
  ou URL com credencial): `password = eyJhbGciOiJI` seguido de
  `UzI1NiJ9abcDEF==` é um valor que quebrou de linha, e o resto dele virava o
  nome de uma chave desconhecida no stderr — e, a cada `SIGHUP`, no log.
- ***Switches* não levam valor.** `--help`, `-h`, `--version`, `-v` e
  `--dump-config` com `=valor` são exit 1, sem o valor: `--dump-config=false`
  despejava a configuração e saía com 0, o contrário do que diz.
- **`--config=` vazio é exit 1**, em vez de cair no `WHS_CONFIG` (ou no padrão),
  que é o contrário do que a flag diz.
- **BOM UTF-8 na linha 1 do INI** é ignorado: alguns editores o escrevem, e a
  linha virava "expected 'key = value'" sem nada visível que explicasse.
- **Valores derivados saem comentados no `--dump-config`**:
  `ring.sqlite.path`, `sink.sqlite.path`, `emergency.dir` (derivados de
  `ring.data_dir`) e `sink.postgres`/`mysql`/`clickhouse.pool_size` (derivado
  de `sink.workers`), quando é o servidor quem os deriva, aparecem como
  `# chave = valor` sob "`# derived`". Escritos como chave, o dump relido os
  fixava: o pool deixava de seguir `sink.workers`, e os caminhos deixavam de
  seguir `ring.data_dir` — para fora da única área gravável (§13.6). Um valor
  explícito continua chave.

**Padrões assumidos nesse porte** (este módulo não tem `DECISOES.md`; os mesmos
do `go-webhook-fast-server`, `DECISOES.md` 4b de lá; contestáveis): os tetos da
tabela de §10.1 são os do `go-syslog-server`, que tem os mesmos rings e sinks
(16 Mi itens só para o ring **efetivo** de memória, 2⁴⁰ de sanidade com `sqlite`
e `none`; `sink.batch` 1 Mi, porque um lote de 100 000 linhas no ClickHouse é
legítimo; o par `ring.max_inflight` ↔ `MaxPending` é da parte do NATS); os
tetos dos botões do rueidis (`ring_scale_each_conn` 0–20, `pipeline_multiplex`
−1–8, buffers até 256 MiB) são próprios do webhook e são sanidade, não
orçamento — um valor legítimo acima deles é motivo para subir o teto;
`sink.redis.addrs` exige `host:porta` em cada entrada (a `,` no host de uma URL
Redis dos irmãos não se aplica: aqui não há URL, e a vírgula é o separador).
Não se aplicam `sink.batch_max_bytes` (a chave não existe; o teto de 1 GiB de um
parâmetro já é `WARN` de §8.5.4) nem as regras de URL PostgreSQL da
`LOGHUB_EB_URL`: não há `LOGHUB_EB_URL` nem outro leitor de URL ou DSN
PostgreSQL/MySQL na configuração.

### 13.3 Recarga por `SIGHUP`

Recarrega **apenas** o que é seguro trocar a quente:

- `log.level`, `log.access`, `log.access_sample`
- `ratelimit.*` (inclusive `enabled`)
- `proxy.trusted`, `proxy.max_xff_entries`
- `headers.exclude`
- `sink.sqlite.keep_files`

Qualquer outra chave alterada no arquivo é **ignorada** e registrada em `WARN`
nominalmente (`"SIGHUP: this key requires a restart and was ignored" key=ring.capacity_items`).

A comparação usa **duas referências**. Uma chave recarregável é comparada com
o que está **em execução** — o último snapshot aplicado —, para que revertê-la
ao valor de boot seja uma mudança como outra qualquer e seja aplicada. Uma
chave que exige reinício é comparada com a configuração **de boot**, com a qual
o processo ainda roda, para que **todo** `SIGHUP` avise sobre ela até o
reinício acontecer, e não só o primeiro. Uma referência única falha num dos
dois casos. Os segredos entram na comparação sem redação (uma senha trocada é
uma mudança); só os nomes das chaves vão para o log.

**O sinal é capturado do início ao fim do processo** (achado pela revisão
independente do porte de 2026-10-11; o `go-webhook-pg-server` e o
`go-webhook-nats-server` já o faziam). A ação padrão do Go para `SIGHUP`
encerra o processo, sem dreno e sem dump. O `main` o ignora antes de ler a
configuração; o `run` o captura do primeiro passo do boot até retornar, e só
**recarrega** entre a abertura da porta pública (§10.1 passo 6) e a parada dos
workers na fase 4 (§10.3). Fora dessa janela — esperando o sink, reingerindo
dumps, drenando o backlog, despejando o ring — o sinal vira um `WARN`
(`"SIGHUP ignored outside of serving"`) e nada é recarregado. Ao retornar, o
`run` deixa o sinal **ignorado** (`signal.Ignore`, não `signal.Stop`, que o
devolveria à ação padrão) até o `os.Exit`. Até então só havia o observador
dessa janela, e ele saía com `signal.Stop`: um `SIGHUP` no meio do dump de
emergência — uma ferramenta de configuração recarregando enquanto o
orquestrador para a instância — matava o processo, e o ring inteiro, já
respondido com `200`, se perdia sem contagem; no boot, matava a instância
antes de servir.

### 13.4 Exemplo completo

```ini
[server]
bind = :8080

[admin]
bind = 127.0.0.1:9101

[proxy]
trusted = 127.0.0.0/8,::1,10.42.0.0/16

[ratelimit]
enabled = true
rate = 500
burst = 1000

[ring]
mode = memory
capacity_items = 65536
max_bytes = 512MiB

[sink]
type = redis
workers = 8
batch = 256

[sink.redis]
addrs = 10.0.0.5:6379
stream = loghub.{space}.{type}

[emergency]
enabled = true
dir = ./data/emergency

[log]
level = info
```

Exemplo de **host**: o `./data` relativo é o certo quando o binário roda direto
na máquina. Em contêiner os mesmos três caminhos ficam sob `/data` (§13.6).

### 13.5 Tabela de parâmetros

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `ring.sqlite.commit_max_delay`,
`ring.sqlite.busy_timeout`, `sink.sqlite.busy_timeout`,
`ratelimit.sweep_interval` e `shutdown.drain_timeout` — a validação recusa `0`
com `exit 1`. Um `op_timeout = 0` faria toda escrita falhar no ato e ser
retentada para sempre sem nunca virar FATAL; um `drain_timeout = 0` faria todo
desligamento terminar em dump de emergência e código 3; os demais eram trocados
em silêncio por um padrão que o `--dump-config` não mostrava. Zero continua
legítimo onde é o que diz: `shutdown.pre_stop_delay` (sem espera),
`sink.batch_max_delay` (sem espera para completar o lote), `sink.fail_timeout`
(sem fusível por tempo; o FATAL vem só de `sink.max_retries`) e os timeouts do
`net/http` em `[server]` (sem limite).

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
| `max_conns` | `0` | `0` = ilimitado. Teto do **servidor inteiro**, somando todos os `listeners`; cheio, o accept espera uma vaga |
| `max_conns_per_ip` | `0` | `0` = ilimitado. Também somado entre `listeners`. Conta o par TCP (nenhum cabeçalho existe no accept); a conexão excedente é fechada na hora, sem segurar os outros endereços, e conta em `whs_rejected_total{reason="bad_request"}`. Inútil atrás de proxy único |
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
| `enabled` | `false` | Ver §6.1 |
| `rate` | `100` | Tokens/s por prefixo |
| `burst` | `200` | |
| `v4_mask` | `24` | |
| `v6_mask` | `48` | |
| `exempt` | *(vazio)* | CIDRs isentos |
| `max_buckets` | `1000000` | |
| `sweep_interval` | `30s` | |

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `mode` | `memory` | `memory` \| `sqlite` \| `none` |
| `capacity_items` | `65536` | |
| `max_bytes` | `512MiB` | |
| `high_water` | `0.80` | Fração que faz `/readyz` falhar |
| `max_inflight` | `2048` | Só em `mode=none`. De 1 a 1 Mi (`1048576`): é um termo do `MaxPending` do NATS (§8.7.2), que um valor sem teto fazia dar a volta |
| `data_dir` | `./data` | Raiz de **toda** escrita em disco do serviço (§13.6). Em contêiner é sempre `/data` |

#### `[ring.sqlite]`

| Chave | Padrão |
|---|---|
| `path` | `<ring.data_dir>/ring.db` |
| `synchronous` | `FULL` |
| `commit_batch` | `256` |
| `commit_max_delay` | `5ms` |
| `busy_timeout` | `5s` |
| `wal_autocheckpoint` | `1000` |

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `type` | `redis` | `redis` \| `sqlite` \| `postgres` \| `mysql` \| `nats` \| `clickhouse` \| `none` |
| `workers` | `8` | Forçado a `1` quando `type=sqlite`; `4` quando `type=postgres`, `type=mysql` ou `type=clickhouse` e não configurado (§8.5.3, §8.6.2, §8.8.2) |
| `batch` | `256` | |
| `batch_max_delay` | `5ms` | |
| `op_timeout` | `5s` | |
| `health_interval` | `2s` | |
| `health_timeout` | `1s` | |
| `fail_timeout` | `30s` | DOWN → FATAL |
| `max_retries` | `10` | Health checks falhos → FATAL |
| `boot_timeout` | `300s` | |
| `retry_backoff_min` | `100ms` | |
| `retry_backoff_max` | `2s` | |

#### `[sink.redis]`

| Chave | Padrão | Descrição |
|---|---|---|
| `addrs` | `127.0.0.1:6379` | Lista separada por vírgula |
| `username` | *(vazio)* | ACL |
| `password` | *(vazio)* | |
| `db` | `0` | |
| `sentinel_master` | *(vazio)* | Ativa modo Sentinel |
| `tls` | `false` | |
| `tls_ca` | *(vazio)* | |
| `tls_insecure` | `false` | |
| `stream` | `loghub.{space}.{type}` | `{space}` e `{type}` expandidos por envelope (§3.3); um nome sem chaves é usado como está |
| `maxlen` | `0` | `0` = trim desligado. Ver §8.2.5 |
| `ring_scale_each_conn` | `10` | rueidis, `2^n` |
| `pipeline_multiplex` | `0` | rueidis; `-1` = uma conexão por nó |
| `read_buffer_each_conn` | `512KiB` | rueidis |
| `write_buffer_each_conn` | `512KiB` | rueidis |

#### `[sink.sqlite]`

| Chave | Padrão | Descrição |
|---|---|---|
| `path` | `<ring.data_dir>/events.db` | Deve diferir de `ring.sqlite.path` |
| `rotate` | `none` | `none` \| `hourly` \| `daily` (UTC) |
| `keep_files` | `0` | `0` = **nunca apaga**. `>0` mantém os N mais recentes |
| `synchronous` | `FULL` | |
| `busy_timeout` | `5s` | |
| `wal_autocheckpoint` | `1000` | |

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

#### `[sink.mysql]`

| Chave | Padrão | Descrição |
|---|---|---|
| `addr` | `127.0.0.1:3306` | `host:porta`, ou caminho de socket unix (começa com `/`) |
| `username` | `webhook` | |
| `password` | *(vazio)* | Redigida no dump e no log de boot |
| `database` | `webhook` | |
| `table` | `inbox` | A tabela de §3.7; `nome` ou `banco.nome`, caixa preservada. O servidor nunca a cria (§8.6) |
| `pool_size` | `0` | `0` = `sink.workers + 1` (§8.6.2) |
| `tls` | `false` | |
| `tls_ca` | *(vazio)* | Exige `tls = true` |
| `tls_insecure` | `false` | |

#### `[sink.nats]`

| Chave | Padrão | Descrição |
|---|---|---|
| `urls` | `nats://127.0.0.1:4222` | Lista separada por vírgula |
| `username` | *(vazio)* | Com `password` |
| `password` | *(vazio)* | Redigida no dump e no log de boot |
| `token` | *(vazio)* | Redigido no dump e no log de boot |
| `credentials` | *(vazio)* | Caminho de um arquivo `.creds` |
| `tls` | `false` | |
| `tls_ca` | *(vazio)* | Exige `tls = true` |
| `tls_insecure` | `false` | |
| `stream` | `LOGHUB_WEBHOOK_REQUEST` | O servidor nunca o cria (§8.7.5) |
| `subject` | `loghub.{space}.{type}` | `{space}` e `{type}` expandidos por envelope (§3.8); o subject do Workspace atual precisa pertencer ao stream |

#### `[sink.clickhouse]`

| Chave | Padrão | Descrição |
|---|---|---|
| `addrs` | `127.0.0.1:9000` | Lista separada por vírgula, `host:porta` do protocolo nativo (§8.8.1) |
| `username` | `webhook` | |
| `password` | *(vazio)* | Redigida no dump e no log de boot |
| `database` | `webhook` | |
| `table` | `inbox` | A tabela de §3.9; `nome` ou `banco.nome`, caixa preservada. O servidor nunca a cria (§8.8.5) |
| `pool_size` | `0` | `0` = `sink.workers + 1` (§8.8.2) |
| `tls` | `false` | |
| `tls_ca` | *(vazio)* | Exige `tls = true` |
| `tls_insecure` | `false` | |

#### `[emergency]`

| Chave | Padrão | Descrição |
|---|---|---|
| `enabled` | `true` | Liga a escrita do dump; só tem efeito com `ring.mode=memory`. A reingestão de dumps já no disco roda em todo boot, ligada ou não (§9) |
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

O serviço escreve em **três** lugares, e os três derivam de `ring.data_dir`:

| O quê | Caminho padrão | Só existe quando |
|---|---|---|
| Fila durável do ring | `<ring.data_dir>/ring.db` + `-wal` + `-shm` | `ring.mode=sqlite`; um boot com outro modo a drena e a remove, salvo com `sink=none` ou num arquivo com outras tabelas (§7.4) |
| Destino append-only | `<ring.data_dir>/events.db` + rotações + `-wal` + `-shm` | `sink.type=sqlite` |
| Dumps de emergência | `<ring.data_dir>/emergency/emg-*.bin` (e `.emg-*.bin.tmp` durante a escrita, `.corrupt` e `.legacy` à parte) | `ring.mode=memory` com `emergency.enabled=true` |

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

Os três caminhos aceitam valor absoluto e o servidor **não** valida se ele cai
sob `/data`: de dentro do processo não há como saber se ele está num contêiner,
e o mesmo binário roda direto no host, onde o padrão `./data` é o certo. Quem
valida é o sistema de arquivos — o contêiner roda `read_only: true` com `/data`
como único ponto gravável. Uma configuração apontando para fora falha assim:

- `ring.sqlite.path` fora de `/data` → o `MkdirAll` do boot falha e o processo
  sai com código **1**, antes de servir requisição nenhuma (§10.5).
- `sink.sqlite.path` fora de `/data` → mesma coisa no boot, código **3**.
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
| `/data` | ring, events, dumps de emergência, e o `WORKDIR` | **sim**, e só ele |
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
  go-webhook-server/
    go.mod                        <- module .../go-webhook-server
    cmd/go-webhook-server/
    internal/
    docs/SPEC.md
  webhook-worker/                 <- futuro, linguagem livre
  webhook-admin/
  webhook-shots/
  tmp/                            <- documentos históricos, não usados
```

Consequências:

- Cada serviço Go versiona, testa e libera de forma independente, com tags no
  formato `go-webhook-server/v1.2.0`.
- `go install github.com/patrickbrandao/loghub-paradise/go-webhook-server/cmd/go-webhook-server@latest`
  funciona direto.
- Uma atualização de dependência aqui não obriga a recompilar nem retestar os
  outros serviços.
- **Sem `go.work`.** É ferramenta de desenvolvimento, exigiria `GOWORK=off` no
  CI e, com um único serviço Go, não resolve problema nenhum. Reavaliar apenas se
  o `webhook-worker` for escrito em Go e a duplicação do envelope (§14.2) doer.

### 14.2 Layout interno

```
go-webhook-server/
  cmd/go-webhook-server/main.go
  internal/
    config/     registry, ini, flags, env, defaults, dump, validação
    server/     http.Server, handler, pipeline, shutdown
    clientip/   XFF/X-Real-IP + proxies confiáveis        [lógica pura]
    ratelimit/  token bucket + shards + sweep             [lógica pura]
    envelope/   struct, serialização, pools               [lógica pura]
    ring/       Ring + memory.go + sqlite.go + none.go
    emergency/  dump e reingestão do arquivo binário
    sink/       Sink + redis.go + sqlite.go + postgres.go + mysql.go + nats.go + clickhouse.go + none.go + workers.go + health.go
    obs/        métricas + slog
  docs/SPEC.md
  go.mod
```

`internal/` impede import externo acidental. `clientip`, `ratelimit` e `envelope`
são lógica pura, testáveis sem rede, sem disco e sem Redis.

`ring` e `sink` expõem cada um **uma** interface — três implementações de ring,
sete de sink — e a ortogonalidade dos dois eixos (§2.1) é uma propriedade do
código, não só da documentação. Os workers de dreno (`sink/workers.go`) são
idênticos para todos os sinks.

### 14.3 Dependências

| Módulo | Uso | Escopo |
|---|---|---|
| `github.com/redis/rueidis` | Cliente Redis com auto-pipelining | produção |
| `modernc.org/sqlite` | SQLite puro Go — **sem CGO**, binário estático, cross-compile trivial | produção |
| `github.com/patrickbrandao/go-loghub-uuidv7` | UUIDv7 (RFC 9562) nível 3, `GenerateAt` sobre a amostra de relógio do `event` (§3.1) | produção |
| `github.com/patrickbrandao/go-loghub-ident` | Identidade LogHub: AgentUUID (`from`), Workspace (`space`), AgentName, Hostname (§3.5) | produção |
| `github.com/jackc/pgx/v5` | Cliente PostgreSQL puro Go — protocolo v3, pool, tipos, `SQLSTATE` estruturado | produção |
| `github.com/go-sql-driver/mysql` | Cliente MySQL/MariaDB puro Go, pelo `database/sql`, com o número de erro do servidor | produção |
| `github.com/nats-io/nats.go` | Cliente NATS puro Go, com o pacote `jetstream` | produção |
| `github.com/ClickHouse/clickhouse-go/v2` | Cliente ClickHouse puro Go, protocolo nativo, API nativa (§8.8.1) | produção |
| `github.com/google/uuid` | Oráculo independente nos testes de UUIDv7 | **somente teste** |
| `github.com/santhosh-tekuri/jsonschema/v6` | Contrato dos envelopes com os schemas JLH e `webhook.request` (§15.1) | **somente teste** |

**Oito dependências em produção. Mais nada.** `go-loghub-uuidv7` e
`go-loghub-ident` não têm dependência transitiva alguma — só a biblioteca padrão
do Go. A `google/uuid` continua sem nenhum `import` no código de produção, mas
entra no binário como transitiva do `clickhouse-go` (abaixo).

#### pgx: por que existe a quarta dependência

A regra das três dependências existe para que cada uma tenha uma justificativa
medida. A do pgx é curta:

- **Não há alternativa razoável na biblioteca padrão.** O protocolo v3 do
  PostgreSQL — autenticação SCRAM, formato binário de arrays, `COPY`, cancelamento,
  *pipelining* — é da ordem de dezenas de milhares de linhas. Reescrevê-lo aqui
  seria a decisão errada pelo mesmo motivo que reescrever o SQLite seria.
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
  `golang.org/x/text` e `golang.org/x/sys` (esta já presente pelo SQLite).
  Nenhuma abre socket por conta própria, escreve em disco ou lê `TMPDIR`; a
  regra de §13.6 continua verdadeira.

#### mysql e nats.go: a quinta e a sexta dependência

As duas entraram juntas, em 2026-09-15, com o critério da quarta: não há
alternativa razoável na biblioteca padrão, e as duas são Go puro.

- **`go-sql-driver/mysql`** é o driver de referência do MySQL em Go, e o único
  com a autenticação `caching_sha2_password` (padrão do MySQL 8) sem CGO.
  Usa-se pelo `database/sql` porque é assim que ele foi desenhado (§8.6.1), e
  o `*mysql.MySQLError` atravessa a camada com o `Number` que §8.6.6 precisa.
  Transitiva: `filippo.io/edwards25519` (a curva da troca de chave da
  autenticação; sem rede, sem disco).
- **`nats-io/nats.go`** é o cliente oficial. O protocolo NATS é simples, mas o
  JetStream em cima dele — publicação assíncrona com futuros, cabeçalhos,
  desduplicação, stream esperado, reconexão com estado — não é, e reescrevê-lo
  seria a decisão errada pelo motivo de sempre. Transitivas: `nkeys` e `nuid`
  (do próprio projeto NATS), `klauspost/compress` (compressão do transporte
  websocket, que este servidor não usa) e `golang.org/x/crypto`. Nenhuma abre
  socket por conta própria, escreve em disco ou lê `TMPDIR`; a regra de §13.6
  continua verdadeira.

#### clickhouse-go: a oitava dependência

Entrou em 2026-10-07 com `sink=clickhouse`, por **escolha do usuário contra a
recomendação**: a entrevista recomendou o HTTP da biblioteca padrão com
`RowBinary`, sem dependência nova, e o usuário escolheu o protocolo nativo. O
critério das outras vale em parte — o protocolo nativo (blocos colunares,
compressão, eventos de perfil, exceção com código) não é razoável de
reescrever —, mas aqui havia alternativa na biblioteca padrão, e o registro
honesto do custo é este:

- **Transitivas no binário** (`go list -deps ./cmd/go-webhook-server`):
  `ClickHouse/ch-go` (o codec do protocolo), `go-faster/city` e
  `go-faster/errors`, `pierrec/lz4/v4`, `klauspost/compress` (já presente pelo
  NATS; agora com `zstd`), `andybalholm/brotli` (compressão do HTTP do driver,
  que este servidor não usa), `paulmach/orb` (tipos geográficos),
  `shopspring/decimal`, `segmentio/asm`, `cespare/xxhash/v2`, `google/uuid`
  (o tipo `UUID` do driver) e `go.opentelemetry.io/otel` + `otel/trace` (só a
  API; sem *exporter* configurado, nada sai do processo). Nenhuma abre socket
  por conta própria, escreve em disco ou lê `TMPDIR`; a regra de §13.6
  continua verdadeira.
- **Tamanho:** o binário (`CGO_ENABLED=0`, `-trimpath -s -w`) passa de ~17,1
  MB para ~21,1 MB (+24%), medido em 2026-10-07 contra o
  `go-webhook-fast-server` ainda sem o destino.
- **Posse de buffer:** auditada no código do driver (v2.48.0): o `Append`
  copia cada valor para o buffer da coluna, e nada do comando referencia o
  item depois dele (§8.8.3). Uma versão nova do driver pede a mesma auditoria.
- **Só a API nativa** (`clickhouse.Open`), nunca o `database/sql`: é ela que
  dá os eventos de perfil da deduplicação (§8.8.4) e o `PrepareBatch` colunar.

#### UUIDv7 nível 3 e identidade: a terceira e a sétima dependência

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
- `ratelimit` — reposição, burst, máscaras v4/v6, sweep apagando só os cheios,
  teto de buckets, isenções.
- `envelope` — **contrato** de todo envelope com `jlh-envelope.schema.json` e
  `jlh-webhook.schema.json` (cópias das skills em `internal/jlhcheck/loghub/`,
  validador `santhosh-tekuri/jsonschema/v6` com `AssertFormat`) e com as regras
  que o `validate.py` da skill confere e um schema não expressa: `event.iso` =
  `uts`+`sub` truncado, o instante nível 3 do `uuid` = `event`,
  `content.length` = corpo decodificado, `content.type` = o primeiro
  `content-type` listado, `url` no RFC 3986 sem fragmento e, lida pelo
  `urlsplit`/`parse_qsl` do Python (portados, sem usar o pacote testado), igual
  a `schema`, `host`, `path` e `query`; nenhuma chave JSON repetida (todo
  decodificador guarda a última, então só um varredor de tokens a vê);
  `src`/`dst` da família de `version`; cabeçalhos ordenados, chaves na ordem
  canônica (`jlhcheck`). Também:
  `ParseHead` lendo de volta o que foi escrito e recusando qualquer outra forma
  (o envelope v1 inclusive), a query de `parse_qsl`, a pré-passada do corpo
  (tamanho exato, utf-8 × base64), o pior envelope contra `MaxEventBytes` com
  entradas adversárias, o escape contra o decode do Python (um U+FFFD por
  subparte máxima, valores tirados do CPython), e os pools.
- `config` — precedência argv > env > ini > padrão em cada tipo, validação da
  matriz de §10.1, `SIGHUP` recarregando só o permitido.
- `emergency` — round-trip do formato de um comprimento com JLH reais, arquivo
  truncado, comprimento declarado maior que o disponível, registro cujo JLH o
  `ParseHead` recusa, e o dump de dois comprimentos (v1) como `ErrLegacy`.

### 15.2 Fuzz — obrigatório

- `FuzzResolve` — o único código que processa entrada hostil
  estruturada; um bug ali vira spoofing de IP. Corpus semente com as cadeias
  malformadas conhecidas.
- `FuzzRead` — o formato binário é lido de disco após um crash; um
  decoder frouxo transforma um arquivo corrompido em pânico no boot.
- `FuzzEnvelope` — método, alvo (pelo `url.ParseRequestURI`, como o `net/http`
  entrega, `#` inclusive), cabeçalho, query e corpo arbitrários: todo envelope
  passa no contrato e o `ParseHead` o lê de volta.
- `FuzzServeHTTP` — bytes arbitrários num socket real, com e sem proxy
  confiável: o servidor recusa, ou o envelope passa no contrato. É o caminho do
  parser do `net/http` e do `describe` (alvo absoluto, `#`, cabeçalhos de
  proxy), que o `FuzzEnvelope` não percorre.
- `FuzzJSONBSafe` — a troca do `\u0000` para a `loghub.inbox` só troca escapes
  reais e decodifica igual com U+FFFD.

### 15.3 Integração

Redis, PostgreSQL, MySQL, NATS e ClickHouse reais, endereçados por variável de
ambiente (`WHS_TEST_REDIS_ADDR`, `WHS_TEST_PG_DSN`, `WHS_TEST_MYSQL_DSN`,
`WHS_TEST_NATS_URL`, `WHS_TEST_CLICKHOUSE_DSN`) e **pulando** na ausência dela;
SQLite em diretório temporário.

**Matriz completa:** as **21** combinações `ring` × `sink` ponta a ponta, cada uma
verificando que o `200` só sai após a garantia de §2.2.

Cenários adicionais obrigatórios:

- Sink cai → `503` em até `health_interval`; volta dentro da janela → retoma sem
  perder nada. (para `redis` e `sqlite`)
- Sink cai e não volta → FATAL, arquivo de emergência correto, `exit 3`.
- Boot com `emg-*.bin` presente → reingestão completa e remoção do arquivo.
- Boot com `emg-*.bin` **truncado** → processa até o último registro íntegro,
  renomeia para `.corrupt`.
- Boot com backlog em `ring.db` e sink fora → porta admin aberta, pública
  fechada, `/readyz` com o motivo.
- Boot com `ring.mode` ≠ `sqlite` e backlog num `ring.db` → backlog entregue e
  arquivo removido; com `sink=none`, `WARN` e arquivo intocado (§7.4).
- `SIGTERM` com ring cheio → dreno completo, `exit 0`, zero perdas.
- Segundo `SIGTERM` → `exit 130`.
- `kill -9` com `ring=sqlite` → zero perdas após restart.
- `sink=sqlite` com `rotate=hourly` → virada de período entre lotes, nenhum
  evento perdido ou duplicado na transição; `keep_files` removendo só os
  antigos e nunca o corrente.
- `sink=sqlite` com disco cheio simulado → `SQLITE_FULL` → FATAL imediato.
- Retry de lote no `sink=redis`: `DoMulti` falha e o lote é **reconstruído**
  (regressão da armadilha de reciclagem do rueidis, §8.2.3).
- Validação: `ring=sqlite` + `sink=sqlite` no mesmo arquivo → `exit 1`.
- `-race` em todos os caminhos de erro do pool de buffers (§12.3).
- `sink=postgres`: os três rings entregam todo id aceito à tabela; corpo com
  NUL, `\n` e `0xff` byte a byte em `bytea`; corpo vazio é `bytea` vazio, nunca
  `NULL`; redelivery do mesmo lote não duplica e é contada; tabela ausente,
  coluna com tipo errado, índice único ausente ⇒ `exit 3` **imediato**, porta
  pública fechada; destino cai ⇒ `503`, volta ⇒ nada perdido; path com UTF-8
  inválido não derruba o lote (§3.6); os slices do pool de argumentos voltam
  zerados (§8.5.4).
- `sink=mysql`: os três rings entregam todo id aceito à tabela; corpo com NUL,
  `\n` e `0xff` byte a byte em `LONGBLOB`; corpo vazio é blob vazio, nunca
  `NULL`; redelivery do mesmo lote não duplica e é contada; um lote acima de
  5 000 linhas e um lote acima de `max_allowed_packet` são divididos; tabela
  ausente, tipo errado, índice único ausente, motor MyISAM e usuário sem
  `INSERT` ⇒ `exit 3` **imediato**; `max_allowed_packet` menor que o maior
  evento ⇒ `exit 3`; destino cai ⇒ `503`, volta ⇒ nada perdido; path com
  UTF-8 inválido não derruba o lote (§3.7); os slices do pool de argumentos
  voltam zerados (§8.6.3).
- `sink=nats`: os três rings entregam todo id aceito ao stream; headers e corpo
  byte a byte (§3.8); corpo vazio é publicado; redelivery dentro da janela
  volta como `Duplicate` e é contada; stream ausente, subject de outro stream,
  stream selado e `max_payload` menor que o maior evento ⇒ `exit 3`
  **imediato**; destino cai ⇒ `503`, volta ⇒ nada perdido; FATAL ⇒ dump ⇒
  reingestão.
- `sink=clickhouse`: os três rings entregam todo id aceito à tabela, e cada
  linha lida de volta passa no `jlhcheck` (as colunas iguais ao cabeçalho, o
  `payload` igual ao do JLH como JSON); corpo binário em base64 e NUL na query
  ida e volta; query com chaves com ponto, vazias e `a%2Eb` junto de `a.b`
  não derruba o lote; o maior envelope entra; redelivery do mesmo lote é
  descartada pelo token e contada; uma tabela recusada para cada regra de
  §8.8.5 (motor, chave sem `uuid`, cada tipo de coluna, `received_at` sem
  default, CHECK, usuário sem `INSERT`, usuário `readonly`, limite de memória)
  como fatal no `Health()` — e cinco delas ponta a ponta, com `exit 3`
  **imediato** e a porta pública fechada; destino cai ⇒ `503`, volta ⇒ nada
  perdido.

### 15.4 Carga

- `bombardier` para HTTP/1.1, `h2load` para h2c.
- **Dois perfis**, sempre: conexões persistentes (mede o pipeline) e conexão nova
  a cada requisição (mede accept + handshake). Bots reais fazem os dois.
- `perf` + flamegraph para localizar CPU. Quase sempre é alocação ou
  serialização, raramente o parser HTTP.

### 15.5 Critérios de aceite v1

Referência: 1 nó de 4 vCPU, corpo de 1 KiB, NVMe local, Redis local.

| Combinação | Métrica | Meta |
|---|---|---|
| `ring=none` + `sink=none` | Vazão (teto de HTTP puro) | ≥ 120.000 req/s |
| `ring=memory` + `sink=none` | Vazão (teto do pipeline) | ≥ 90.000 req/s |
| `ring=memory` + `sink=redis` | Vazão | ≥ 50.000 req/s |
| `ring=memory` + `sink=redis` | p99 do `200` | ≤ 10 ms |
| `ring=memory` + `sink=sqlite` | Vazão | ≥ 25.000 req/s |
| `ring=sqlite` + `sink=redis` | p99 do `200` (`synchronous=FULL`) | ≤ 25 ms |
| `ring=none` + `sink=redis` | Vazão | ≥ 15.000 req/s |
| Qualquer | RSS sob carga por 30 min | estável, < 512 MiB |
| `ring=memory`, `SIGTERM` com ring cheio | Eventos perdidos | 0 |
| `ring=sqlite`, `kill -9` | Eventos perdidos | 0 |
| `ring=memory`, sink FATAL | Eventos perdidos (via emergência) | 0 |
| Qualquer | Casos de spoofing de XFF rejeitados | 100% |

As metas dos modos `sqlite` são medidas separadamente e com alvo próprio: o
`synchronous=FULL` as prende ao disco por decisão de projeto (§7.4).

`sink=postgres`, `sink=mysql`, `sink=nats` e `sink=clickhouse` **não têm meta
numérica** até a primeira medição válida: o custo depende da latência de commit
(ou do `PubAck`, ou da escrita da parte) do servidor e do tamanho do pool, e um
número inventado aqui viraria referência. A medição decide também se o `COPY` de §8.5.4 volta.

Os dois primeiros números têm papel de **diagnóstico**: a diferença entre
`ring=none/sink=none` e `ring=memory/sink=none` é o custo exato do ring; entre
esta e `sink=redis`, o custo exato da entrega. Uma regressão de performance é
localizada por subtração, sem profiler.

---

## 16. Matriz de garantias

### 16.1 O que o `200` significa

| `ring` | `sink` | O `200` significa | Perda em `kill -9` | Uso típico |
|---|---|---|---|---|
| `memory` | `redis` | Está na RAM deste nó | **Sim** (mitigado por §9 e §10.3) | **Padrão de produção** |
| `memory` | `sqlite` | Está na RAM deste nó | **Sim** (idem) | Autônomo, sem Redis |
| `memory` | `none` | Está na RAM deste nó | — (descarte por configuração) | Benchmark do pipeline |
| `sqlite` | `redis` | `COMMIT` no `ring.db` | Não | Retenção máxima + ecossistema |
| `sqlite` | `sqlite` | `COMMIT` no `ring.db` | Não | Retenção máxima autônoma (**2 fsync**, `WARN`) |
| `sqlite` | `none` | `COMMIT` no `ring.db` | Não | Benchmark do custo do ring persistente |
| `none` | `redis` | `XADD` confirmado pelo Redis | Não | Entrega garantida ponta a ponta |
| `none` | `sqlite` | `COMMIT` no `events.db` | Não | Entrega garantida autônoma |
| `none` | `none` | Nada foi escrito | — | Teto absoluto de HTTP puro |
| `memory` | `postgres` | Está na RAM deste nó | **Sim** (mitigado por §9 e §10.3) | Banco compartilhado, vazão máxima |
| `sqlite` | `postgres` | `COMMIT` no `ring.db` | Não | Retenção local + banco compartilhado |
| `none` | `postgres` | `INSERT` confirmado pelo servidor † | Não † | Entrega garantida ponta a ponta, sem fila local |
| `memory` | `mysql` | Está na RAM deste nó | **Sim** (mitigado por §9 e §10.3) | Banco MySQL compartilhado, vazão máxima |
| `sqlite` | `mysql` | `COMMIT` no `ring.db` | Não | Retenção local + MySQL compartilhado |
| `none` | `mysql` | `INSERT` confirmado pelo servidor † | Não † | Entrega garantida ponta a ponta, sem fila local |
| `memory` | `nats` | Está na RAM deste nó | **Sim** (mitigado por §9 e §10.3) | JetStream como barramento, vazão máxima |
| `sqlite` | `nats` | `COMMIT` no `ring.db` | Não | Retenção local + JetStream |
| `none` | `nats` | `PubAck` do stream † | Não † | Entrega garantida ponta a ponta, sem fila local |
| `memory` | `clickhouse` | Está na RAM deste nó | **Sim** (mitigado por §9 e §10.3) | Armazém analítico, vazão máxima |
| `sqlite` | `clickhouse` | `COMMIT` no `ring.db` | Não | Retenção local + armazém analítico |
| `none` | `clickhouse` | `INSERT` respondido pelo ClickHouse † | Não † | Entrega ponta a ponta, sem fila local (uma parte por requisição, §8.8.2) |

† **Linhas cuja durabilidade depende de configuração fora do processo.**
No PostgreSQL, "confirmado" significa o que o `synchronous_commit` do servidor
disser: com `on` (padrão) o WAL foi sincronizado antes da resposta; com `off`,
um crash **do banco** perde os últimos `3 × wal_writer_delay` de eventos já
respondidos com `200` (§8.5.6). No MySQL, o que `innodb_flush_log_at_trx_commit`
disser (§8.6.5). No JetStream, o que `storage`, o número de réplicas e o
`sync_interval` do servidor disserem (§8.7.4). No ClickHouse, o que o perfil do
usuário disser sobre `async_insert` e `wait_for_async_insert`: com
`wait_for_async_insert = 0`, o `INSERT` responde com o dado só no buffer em
memória do servidor (§8.8.6). O servidor de webhooks não lê nem altera nenhum
desses valores.

### 16.2 Custo relativo

| `ring` | Vazão | p99 |
|---|---|---|
| `none` | maior | menor (com `sink=none`) |
| `memory` | alta | baixa |
| `sqlite` | ~40–70% | presa ao disco |

| `sink` | Vazão | p99 |
|---|---|---|
| `none` | máxima | mínima |
| `redis` | alta (auto-pipelining) | presa ao RTT com `ring=none` |
| `sqlite` | ~50% do Redis (escritor único) | presa ao disco |
| `postgres` | não medida; presa a `pool_size / latência de commit` | presa ao `fsync` do WAL do servidor |
| `mysql` | não medida; presa a `pool_size / latência de commit` | presa ao `fsync` do redo log do servidor |
| `nats` | não medida; publicação assíncrona em lote | presa ao RTT do `PubAck` com `ring=none` |
| `clickhouse` | não medida; um `INSERT` colunar por lote | presa à escrita da parte (ou ao `async_insert` do perfil) com `ring=none` |

---

## 17. Invariantes

Regras que nenhuma otimização futura pode violar:

1. O `200` **nunca** é enviado antes de a garantia da combinação `ring` × `sink`
   ser cumprida (§2.2).
2. Ring cheio → `429`. **Nunca** um `200` seguido de descarte — exceto com
   `sink=none`, onde o descarte é a configuração explícita do operador, e na
   recusa permanente de **um** evento por um destino vivo (`logic`, §8.3.4,
   §8.5.7, §8.6.6, §8.7.6 e §8.8.7), que é logada e contada. Uma recusa que
   atingiria todos os eventos igual nunca é `logic`: é fatal (§8.2.4, §8.5.7,
   §8.6.6, §8.7.6, §8.8.7).
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
10. Um comando rueidis **nunca** é reusado após execução (§8.2.3).
11. O envelope JLH é **idêntico** em todos os sinks, escrito **uma** vez no
    handler (§12.1). Trocar de destino nunca muda o dado; as colunas da `inbox`
    são lidas do próprio JLH, e a única alteração — `\u0000` em `\ufffd` na
    cópia do `jsonb` da `loghub.inbox` (§3.6) — nunca toca o JLH do item.
12. `ring` e `sink` são **ortogonais**: nenhuma implementação de um consulta o
    tipo do outro.
13. Toda escrita em disco acontece sob `ring.data_dir` — e dentro de um
    contêiner esse diretório é `/data`, em toda imagem do ecossistema (§13.6).
    Nenhum caminho de escrita novo pode nascer fora dessa raiz — nem a
    identidade do `go-loghub-ident`, que vai em `DATADIR = ring.data_dir`.
14. `uuid` e `event` saem de **uma** amostra de relógio, e o `uuid` devolvido em
    `X-LogHub-UUID` é o do envelope em todo destino; ele nunca é regerado
    (retry, ring, dump, reingestão) (§3.1).
15. Nenhum destino recebe um envelope que o teto dele recusaria: o boot compara
    `max_payload` e `max_allowed_packet` com o maior envelope, e o
    `max_memory_usage` do ClickHouse com o pior lote (§3.2, §8.8.5).

---

## 18. Roteiro pós-v1

| Item | Motivo de estar fora da v1 |
|---|---|
| Múltiplos streams/tabelas por Host ou path | Depende do desenho de multi-tenancy do `webhook-admin` |
| Redis Cluster + sharding de stream | Só faz sentido depois de múltiplos streams |
| `sink=clickhouse` numa tabela `Distributed` | O `INSERT` num `Distributed` vai aos shards de forma assíncrona por padrão (`distributed_foreground_insert = 0`): o `200` exigiria decidir o que significa "entregue" num cluster (§8.8.5) |
| `sink=clickhouse` dividindo o lote por bytes | O ClickHouse não tem transação: um lote dividido gravaria parte e descartaria parte, o problema que §8.6.3 resolveu com uma transação. Hoje o boot recusa o limite de memória que não comporta o pior lote (§8.8.5) |
| Particionamento da tabela de `sink=postgres` | Exige decidir quem cria a partição seguinte; a v1 usa tabela simples (§8.5.9) |
| Particionamento da tabela de `sink=mysql` | A mesma razão (§8.6.8) |
| Cache de statements preparados por tamanho de lote em `sink=mysql` | Só se a medição de §15.5 mostrar que as três idas por lote pesam (§8.6.1) |
| `sink=nats` com um subject por Host ou path | Depende do desenho de multi-tenancy do `webhook-admin`, como os streams múltiplos |
| Pooler externo em modo *transaction* para `sink=postgres` | Exige desligar o cache de *statements* do pgx ou migrar para `COPY` com *staging* (§8.5.2) |
| `COPY` com tabela de *staging* em `sink=postgres` | Só se a medição de §15.5 mostrar que o `INSERT` com `unnest` é o gargalo (§8.5.4) |
| Sink duplo (Redis **e** SQLite simultâneos) | Exige decidir a semântica do `200` com entrega parcial |
| Verificação HMAC por tenant | Precisa de um registro de tenants |
| Rings particionados por CPU | Só se o benchmark mostrar contenção no ring único |
| HTTP/3 | Nenhum emissor relevante usa; 2–4× a CPU por byte |
| TLS na porta pública | Proxy reverso resolve com ACME de graça |

---

## 19. Lacunas e riscos

As lacunas de **prova** (o que a suíte não cobre) ficam em `docs/TESTING.md` §8.
Esta seção guarda as do **código**.

### 19.1 Pendências conhecidas

Problemas reais que o código atual ainda tem, contra esta especificação ou
contra a intenção dela. Cada item diz o problema, a consequência e a direção
da correção. Corrigir um é mudar primeiro a seção normativa citada, depois o
código e o teste, e então trocar o item por uma linha só, com o número (que
o `docs/TESTING.md` e os comentários do código citam) e a seção da regra.

**Superfície HTTP e conexões**

1. **Alocação pelo `Content-Length` declarado** (§4.3, §12.2). O corpo é
   pré-alocado com o tamanho que o cabeçalho declara (até
   `server.max_body_bytes`), antes de chegar um byte: requisições que declaram
   5 MiB e não mandam nada seguram 5 MiB cada até o `read_timeout` (em h2c, 250
   por conexão), e o OOM leva o ring, já respondido com `200`. Direção:
   pré-dimensionar até um teto pequeno (64 KiB) e crescer só com bytes
   recebidos, limitado ao declarado.
2. **A checagem rápida do passo 9 olha só os itens** (§4.3, §4.4). Com o ring
   cheio por bytes, o corpo é lido e serializado antes do `429`, que sai com
   UUID, e não sem ele, como diz a tabela de §4.4. Direção: incluir os bytes na
   checagem, com uma estimativa por baixo (cabeçalho mais `Content-Length`) que
   só serve para recusar.
3. **O dreno de recusa ignora o protocolo** (§4.3). Em h2c, o
   `Connection: close` de uma recusa com corpo acima de `reject_drain_bytes`
   vira GOAWAY e derruba a conexão que o proxy divide entre muitos clientes; com
   `Expect: 100-continue`, o dreno envia o `100 Continue` e convida o corpo que
   vai recusar. Direção: drenar só em HTTP/1.x sem `Expect` pendente e nunca
   pôr `Connection: close` em h2.
4. **`server.write_timeout` conta desde os cabeçalhos** (§13.5). Com o padrão
   de 10 s contra 30 s de `read_timeout`, um upload que termina aos 15 s entra
   no ring e o `200` não chega: o cliente reenvia, e o evento duplica com outro
   `uuid`; em h2c o stream é cortado aos 10 s. Direção: rearmar o prazo de
   escrita na hora da resposta, ou exigir `write_timeout` acima de
   `read_timeout + sink.op_timeout`.
5. **Erro não temporário no accept encerra sem dreno** (§10.3, §10.5).
   `ENOBUFS`, `ENOMEM`, `EPERM` ou `EPROTO` terminam o `Serve`, e o processo sai
   com 1 sem passar pelo shutdown: com `ring=memory`, o ring e os lotes retidos,
   já respondidos com `200`, somem sem dump nem contagem. Direção: retentar
   esses erros no listener, com backoff e log amostrado, e levar uma falha
   definitiva ao shutdown gracioso, com um código de saída próprio.
6. **`max_conns_per_ip` conta o endereço exato** (§13.5). Em IPv6 a chave é o
   /128: quem tem um /64 abre conexões sem limite por IP e, com `max_conns`,
   ocupa o teto global. Direção: decidir entre documentar o limite e contar por
   prefixo IPv6 configurável, com a chave do rate-limit.
7. **O contador de buckets do rate-limit deriva** (§6.3). O `Reset` de um
   `SIGHUP` que troca a máscara limpa shard a shard e zera o contador no fim,
   fora das travas; `Allow` e `Sweep` concorrentes deixam
   `whs_ratelimit_buckets` errado (até negativo) e `max_buckets` frouxo até o
   próximo `Reset`. Direção: cada incremento e decremento sob a trava do shard
   que muda.

**Ring**

8. **O ring de memória conta `len(Env)`, não o buffer retido** (§7.2, §12.2,
   §12.4). O JLH fica no buffer inteiro da classe do pool: um evento de ~2,4 KiB
   numa classe de 16 KiB conta 2,4 KiB. Com o destino fora, o ring retém de 4 a
   7× `ring.max_bytes`, além de `GOMEMLIMIT` e do limite do contêiner. Direção:
   cobrar `cap(Env)` mais um custo fixo por item, gravar no item o valor cobrado
   para descontar o mesmo, e mover o JLH para a classe justa depois de escrito.
9. **O teto do `ring=sqlite` ignora o que vai para o `COMMIT`** (§7.4). O
   `Enqueue` compara com contadores que só sobem depois do `COMMIT`, sem
   reservar: centenas de itens passam juntos, o `ring.db` excede
   `ring.max_bytes` em gigabytes, e o pior lote de §8.5.4 e §8.8.5 deixa de ser
   teto. Direção: reservar no `Enqueue`, como o ring de memória.
10. **Um erro de leitura do `ring=sqlite` encerra o worker em silêncio** (§8.1).
    Qualquer erro do `Dequeue` (I/O, `SQLITE_BUSY` além do `busy_timeout`)
    termina o worker sem log; com `sink=sqlite`, que tem um só, o dreno para, o
    ring enche e tudo é `429` com `/healthz` em 200. Uma leitura parcial
    descarta os itens já lidos. Direção: sair só com o fim do contexto,
    `ErrClosed` ou `ErrNoBacklog`; nos outros erros, log amostrado, backoff e
    nova tentativa, entregando o que já foi lido.

**Destinos**

11. **O lote não tem teto de bytes** (§8.5.4, §10.4), hipótese não medida. Com
    corpos grandes um lote chega a centenas de MiB; se não couber em
    `sink.op_timeout`, ele é retentado para sempre como `network`, com o sink
    UP, e a reingestão repete o mesmo lote a cada boot. Direção: um teto de
    bytes por lote, no `Dequeue` e na reingestão.
12. **A política de despejo do Redis não é conferida** (§8.2.5). "`maxmemory`
    dá `OOM`, que é fatal" só vale com `noeviction` (ou `volatile-*` sem TTL);
    com `allkeys-*` o Redis apaga o stream inteiro, já respondido com `200`, sem
    erro. Direção: ler `maxmemory_policy` no boot e na checagem, recusar
    `allkeys-*` como fatal, e só avisar quando o `INFO` for negado.
13. **`sink.redis.tls_ca` sem `tls` passa calado** (§8.2.2, §10.1). Os outros
    destinos recusam com exit 1 (§8.5.11); no Redis a CA é descartada e a
    conexão, com a senha, sai em claro. Direção: a mesma recusa.
14. *Corrigido no porte da família syslog/RELP (2026-10-13):* `23502` do
    PostgreSQL é fatal; a regra está em §8.5.7.
15. **O ambiente libpq ainda chega ao pool** (§8.5.2). Os campos que o
    servidor não sobrescreve depois do `ParseConfig("")` (`PGCHANNELBINDING`,
    `PGREQUIREAUTH`, `PGSSLNEGOTIATION`, `PGMIN/MAXPROTOCOLVERSION`, o prazo de
    `PGCONNECT_TIMEOUT`) sobrevivem, e um `PGSSLMODE` ou `PGSERVICE` inválido
    derruba o boot com exit 1. Direção: parsear com o ambiente `PG*` escondido.
16. *Corrigido no porte da família syslog/RELP (2026-10-13):* a sessão do MySQL
    fixa `autocommit=1` no `SET` de conexão e o boot confere
    `@@session.autocommit`; a regra está em §8.6.1.
17. *Corrigido no porte da família syslog/RELP (2026-10-13):* o nome do banco
    do MySQL e do ClickHouse é validado na configuração e no destino (sem
    crase, barra invertida nem byte de controle; até 64 caracteres); a regra
    está em §8.6.3 e §8.8.3.
18. *Corrigido no porte da família syslog/RELP (2026-10-13):* `retention:
    interest` é recusado na validação de toda checagem, e
    `max_msgs_per_subject > 0` dá um `WARN` por valor; a regra está em §8.7.5.
19. *Corrigido no porte da família syslog/RELP (2026-10-13):* `10158`, `10167`
    e todo `Code` 429 são `ErrNetwork`; a regra está em §8.7.6.

**Boot, shutdown e emergência**

20. **Uma recusa `logic` no boot vira laço de reinício** (§9.2, §10.1). A
    reingestão e o dreno do `ring.db` saem com 3 em qualquer erro, `logic`
    inclusive, e o boot seguinte encontra o mesmo registro com a porta fechada.
    A porta do dump não confere UTF-8 nem os escapes do serializador, e deixa
    passar dano que o PostgreSQL e o MySQL recusam. Direção: refazer item a item
    o lote recusado, descartar e contar o item recusado (o dump vai para
    `.corrupt`), e exigir na porta UTF-8 válido e só os escapes que o
    serializador escreve.
21. *Corrigido no porte da família syslog/RELP (2026-10-13):* um FATAL durante
    o dreno do `SIGTERM` encerra a espera e o processo sai com 3, em qualquer
    ring; a regra está em §10.3.
22. **Com `sink=redis` o boot espera até 2× `sink.boot_timeout`** (§10.1): a
    construção do cliente tem o seu prazo, e a espera pela primeira checagem
    começa outro. Direção: um prazo só, contado do início do passo 3.
23. **A sonda do `sink=sqlite` espera a trava da escritora sem prazo**
    (§8.3.5). Um lote mais longo que `sink.health_timeout` faz a sonda falhar
    com o banco íntegro, um ciclo DOWN por lentidão. Direção: tomar a trava
    dentro do prazo da sonda.

**Configuração e implantação**

24. **`NaN` passa nos números da configuração** (§13.5). Toda comparação de
    faixa com `NaN` é falsa: `ratelimit.rate` ou `burst` em `NaN` dão `429` para
    sempre (o estado sobrevive ao `SIGHUP` que corrige o valor),
    `ring.high_water` desliga a saturação do `/readyz`, e `log.access_sample`
    desliga o access log. Direção: recusar valor não finito no parser.
25. **`admin.bind` vazio escuta em todas as interfaces** (§11, §13.5), numa
    porta efêmera, com um `WARN` só. Direção: com `admin.enabled`, recusar vazio
    e exigir `host:porta` (exit 1).
26. **O `--dump-config` não volta igual** (§13.2). O valor de texto sai sem
    aspas, e o leitor de INI tira os espaços das pontas e um par de aspas
    externo. Direção: pôr aspas no valor que tem espaço nas pontas ou começa e
    termina com a mesma aspa.
27. **Os compose não repassam a CA nem as credenciais do NATS** (§13.6).
    `WHS_SINK_{POSTGRES,MYSQL,CLICKHOUSE,NATS}_TLS_CA`,
    `WHS_SINK_NATS_CREDENTIALS` e `WHS_SINK_NATS_TOKEN` estão no `.env.example`
    e no README do Docker, mas nenhum compose os passa ao contêiner: a
    configuração é ignorada. Direção: repassá-los com padrão vazio.
28. **O `nats-init` dos compose usa `natsio/nats-box:latest`**: a criação do
    stream pode mudar sem mudança no projeto. Direção: fixar a versão.

**Testes**

29. **Testes de regressão pulam em vez de falhar** (§15). Seis guardas
    `t.Skip("BUG ...")` (lote do MySQL em pedaços, cabeçalho de dois campos do
    dump, papel do PostgreSQL só com `INSERT`, JA3 com vírgulas, porta do host
    recusado, `OPTIONS *`) só disparam quando o defeito volta, e o `go test`
    conta o pulo como aprovado. Direção: `t.Fatalf` com "regressão".
