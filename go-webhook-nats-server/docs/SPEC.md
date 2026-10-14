# go-webhook-nats-server — Especificação v1

Status: **congelada para implementação** (escopo de 2026-10-10, abaixo)
Data: 2026-10-10
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server`
Binário: `go-webhook-nats-server`
Entrada: `docs/DECISOES.md` (sem entrevista: cada ponto tinha resposta numa
decisão já tomada ou num padrão dos módulos de referência).
Origem: cópia do `go-webhook-pg-server` em 2026-10-10 (o estado dele depois do
porte das correções de 2026-10-07), com o PostgreSQL trocado pelo NATS "como foi
feito no `go-syslog-nats-server`" (§0).

## 0. Escopo desta versão

O `go-webhook-nats-server` é o `go-webhook-pg-server` com o destino trocado:
**um ring e um destino**, o ring em memória e o **NATS** (JetStream por padrão).
O objetivo continua o do P — um servidor de webhooks rápido e com dependências
mínimas, três em produção (§14.3) —, e a troca segue o
`go-syslog-nats-server`: sem TLS até o NATS, `jetstream` e `core`, subject
derivado, heartbeat beacon e `LOGHUB_EB_URL`.

| Fica, igual ao P | Muda | Sai |
|---|---|---|
| Envelope JLH `type=webhook`, `mode=fast`, `uuid`/`event` de uma amostra de relógio, identidade `go-loghub-ident` (§3) | O destino: `sink.type = nats` (§3.8, §8.7), no lugar da `loghub.inbox` do PostgreSQL | `sink.type = postgres` (§3.6, §8.5) |
| `ring.mode = memory` (§7.3) | Entrega parcial: uma recusa `logic` de uma mensagem não descarta o lote (§8.1, §8.7.2) | TLS até o destino: o cliente NATS é sempre texto claro (§8.7.9) |
| Pipeline HTTP, h2c, `SO_REUSEPORT`, limites, resolução de IP por proxy confiável (§4, §5) | A porta da reingestão: só volta um JLH deste servidor (§9.2) | |
| Saúde do sink, `503 sink_down`, FATAL, shutdown em cinco fases (§10) | O dump sob nome temporário e `link` (§9.1) | |
| Porta admin, métricas `whs_*`, logs (§11) | | |
| Dump JSON Lines e reingestão no boot, no mesmo formato (§9) | | |
| Sem rate-limit (§6) e sem recarga por `SIGHUP` (§13.3) | | |

| Volta, do fast e do `go-syslog-nats-server` | Decisão |
|---|---|
| `sink.nats.mode = jetstream` (padrão) e `core` (opt-in, *best effort*) | decisão 8 do fast; decisão 3 do SN (§8.7.2) |
| Heartbeat beacon em `loghub._ctl.beacon`, por NATS core | decisão 21 do fast; decisão 1 do SN (§8.7.10) |
| `LOGHUB_EB_URL`, só `nats://` | padrão assumido do fast; SN §10.9 (§8.7.11) |

**Numeração.** As seções seguem a numeração do `go-webhook-fast-server` (a mesma
do P): uma seção retirada continua no lugar, com uma linha dizendo para onde
foi, e uma seção nova entra no fim da seção-mãe (§8.7.10, §8.7.11). O código cita
a spec por número, e uma correção portada entre os três módulos (`diff -ruN`)
continua apontando para a mesma regra.

**Compatibilidade de configuração (decisão 3 do P).** Um INI, uma lista de
flags ou um ambiente `WHS_*` escrito para o fast com `ring.mode = memory` e
`sink.type = nats` sobe aqui sem mudança, e os painéis das métricas `whs_*`
continuam valendo. Cinco chaves **seletoras** continuam aceitas, cada uma só com
o valor que este servidor implementa: `ring.mode = memory`, `sink.type = nats`,
`ratelimit.enabled = false`, `sink.nats.tls = false` e
`sink.nats.subject = loghub.{space}.{type}` (qualquer outro valor é `exit 1`,
com a mensagem dizendo que o recurso não existe aqui). Toda outra chave de
recurso retirado — `[ring.sqlite]`, `ring.max_inflight`, `[sink.redis]`,
`[sink.sqlite]`, `[sink.postgres]`, `[sink.mysql]`, `[sink.clickhouse]`,
`sink.nats.tls_ca`, `sink.nats.tls_insecure`, o resto de `[ratelimit]` — num INI
ou numa flag é **chave desconhecida** (`exit 1`): uma configuração que pede um
recurso que não existe não pode subir em silêncio. O padrão de `sink.type` passa
a ser `nats` (no fast era `redis`; no P, `postgres`).

**No ambiente é diferente, e de propósito** (regra do P). O carregador só lê as
variáveis que o registry conhece; uma desconhecida nunca foi vista. Assim
`WHS_SINK_POSTGRES_ADDR` ou `WHS_RATELIMIT_RATE` esquecidas no ambiente são
**inertes**. Um **seletor** no ambiente continua recusando o recurso retirado
(`WHS_SINK_TYPE=postgres`, `WHS_SINK_NATS_TLS=true`: `exit 1`): é a fronteira
entre ajustar algo que não existe e pedir algo que não existe.

**`SIGHUP`.** Sem recarga, o sinal é **ignorado** com um `WARN` dizendo que a
configuração só muda com reinício. Deixá-lo com a ação padrão do Go encerraria o
processo — um `kill -HUP` de um procedimento antigo do fast derrubaria a
instância sem dreno nem dump.

**O que continua aberto no fast** (`docs/DECISOES.md` dele: `stage`
`auto|ring|sink`, `503` no ring cheio, PROXY protocol, nomes `WEBHOOK_*`) não
entra aqui antes de entrar lá; o `stage` da resposta é sempre `ring`. O beacon,
o `core` e a `LOGHUB_EB_URL` estavam na mesma lista do fast; entram aqui porque o
pedido manda trocar o destino "como foi feito no `go-syslog-nats-server`", e lá
eles fazem parte da troca.

---

## 1. Escopo

Servidor HTTP de ingestão de webhooks. Recebe requisições arbitrárias vindas da
internet, encapsula cada uma num envelope JLH do LogHub e o **publica** no NATS,
no subject `loghub.<space>.webhook`: por padrão num stream JetStream, com
confirmação (`PubAck`) por mensagem. É um **Bridge-IN** (`step = 0`). Escalável
horizontalmente: instâncias são independentes e não compartilham estado entre
si; o NATS é o único ponto comum.

### 1.1 Objetivos

- Máximo de requisições por segundo com mínimo de CPU e memória.
- Dependências mínimas: só o cliente NATS e as duas bibliotecas LogHub (§14.3).
- Nunca responder `200` para um evento que não está no ring (§2.2).
- Backpressure explícito em vez de perda silenciosa.

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| Outros destinos e rings | Use o `go-webhook-fast-server` ou o `go-webhook-pg-server` (§0) |
| TLS até o NATS | Decisão do `go-syslog-nats-server`, seguida aqui: o NATS fica numa rede confiável (§8.7.9). Um servidor que exige TLS é recusado no boot |
| Rate-limit | Papel do proxy reverso, que vê o IP real antes de qualquer header (§6) |
| Recarga a quente | Uma configuração só, a do boot; reinício é o caminho (§13.3) |
| Sonar, `ping`/`pong` do plano de controle | Lacuna até existir um `go-loghub-driver`; só o beacon (§8.7.10) |
| Criar ou alterar o stream | Papel de quem administra o NATS (§8.7.5) |
| HTTP/3 e QUIC | Nenhum emissor relevante de webhook usa; 2–4× a CPU por byte |
| TLS na porta pública | Responsabilidade do proxy reverso (Traefik/nginx) |
| Roteamento interno de URLs | Critérios de roteamento ficam no proxy reverso |
| Parse do corpo | O corpo é tratado como bytes opacos |
| Verificação de assinatura HMAC | Responsabilidade do consumidor do stream |
| `Upgrade: h2c` (RFC 7540 §3.2) | Removido pela RFC 9113; stdlib não implementa |
| Multi-tenancy / múltiplos streams | v2 |

---

## 2. Arquitetura

### 2.1 Um ring, um destino

O fast tem dois eixos ortogonais (ring × sink, 21 combinações). Aqui cada eixo
tem um valor só, mas a separação continua no código: `ring.Ring` e `sink.Sink`
são interfaces com uma implementação cada (decisão 4 do P), e nenhuma consulta
o tipo da outra.

- **RING** — *onde o evento espera* entre ser aceito e ser entregue: `memory`.
- **SINK** — *para onde o evento vai*: `nats` (`loghub.<space>.webhook`).

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
                                   | sink.workers (padrão 8)
                    +--------------v---------------+       beacon (core)
                    | SINK nats  (uma conexão)     |-----> loghub._ctl.beacon
                    |  PublishMsgAsync em lote     |
                    +--------------+---------------+
                                   v
                 stream LOGHUB_WEBHOOK  <-  loghub.<space>.webhook

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
sob `SIGTERM`, o dreno do shutdown (§10.3); sob falha fatal do NATS, o dump de
emergência (§9).

---

## 3. Modelo de dados (LogHub)

O evento é um **envelope JLH** (JSON LogHub) do tipo `webhook`: o servidor é um
*bridge-in* HTTP do LogHub.

### 3.0 Fontes normativas

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH: nove campos, regras, `jlh-envelope.schema.json`; subject `loghub.{space}.{type}`, `Nats-Msg-Id`, plano de controle e beacon (§3.8, §8.7.10) |
| skill `loghub-type-webhook-fast` | consultada em 2026-10-06 | `type=webhook`, payload (`mode`, `network`, `headers`, `query`, `content`...), respostas HTTP |
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

**Por que sem NUL no texto:** um `\u0000` é JSON válido, mas muitos
consumidores o tratam mal (o `jsonb` do PostgreSQL o recusa, e um consumidor do
stream que copie o envelope para a `loghub.inbox` herdaria o problema); o base64
leva os bytes exatos. NUL ainda pode aparecer em `query` (um `%00`): a mensagem
NATS é fiel ao byte (§3.8), e o escape `\u0000` chega ao consumidor como está.

**O maior envelope** é calculado, não configurado; é o teto de uma linha do
dump de emergência (§9.2) e entra na conta da maior mensagem que o boot compara
com o `max_payload` do NATS e o `max_msg_size` do stream (§8.7.5):

```
4 × ⌈B / 3⌉ + 14 × H + 13 × U + 8 KiB
B = server.max_body_bytes, H = server.max_header_bytes, U = server.max_uri_bytes
```

O corpo pesa no máximo o base64 (`4⌈B/3⌉`); cada byte de cabeçalho no máximo
triplica no escape (obs-text vira U+FFFD) e aparece até duas vezes (`headers` e
o campo derivado: `host`, `content.type`, `fp*`, o host da `url`), com 23 bytes
de moldura por linha; a URI aparece em `path`, `url` e `query` decodificada.
`TestWorstCaseEnvelopeIsBounded` prova a fórmula com entradas adversárias. Com
os padrões (5 MiB, 64 KiB, 8 KiB) são 8 022 700 bytes (~7,7 MiB): acima do
`max_payload` padrão do NATS (1 MiB), e o boot recusa até um dos dois mudar
(§8.7.5).


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
nomeiam a conexão NATS (`whs/<agent_name>@<hostname>`, o nome do cliente visto no
`connz`, §8.7.1) e entram no beacon com o `MachineID` (§8.7.10); o AgentUUID
nomeia o dump (§9.1). A identidade é persistida em `ring.data_dir`
(exportado como `DATADIR`) e fica congelada até o fim do processo.
`server.node_id` não existe.

### 3.6 Fila `loghub.inbox` (`sink=postgres`) — retirada

O destino PostgreSQL é o do `go-webhook-pg-server` (§0, §8.5).

### 3.7 Tabela `inbox` (`sink=sqlite` e `sink=mysql`) — retirada

Os dois destinos não existem neste servidor (§0).

### 3.8 Mensagem NATS (`sink=nats`)

```
Subject:  loghub.<space>.webhook
Headers:  Nats-Msg-Id:     <uuid>.<step>          (aqui sempre <uuid>.0)
          Content-Type:    application/json
          X-LogHub-Type:   webhook
          X-LogHub-UUID:   <uuid>
          X-LogHub-Space:  <space>
Data:     <jlh>
```

- `Data` é o JLH byte a byte, o mesmo do ring e do dump; o corpo está dentro
  dele. A mensagem é **fiel ao byte**: o consumidor recebe exatamente o que o
  cliente viu confirmado.
- O subject exato é a regra do JLH (`loghub.{space}.{type}`): publicar no subject
  exato, nunca num curinga, e nunca ler `space` ou `type` de volta do subject.
  É **derivado, não configurável** (decisão 4 do `go-syslog-nats-server`):
  `sink.nats.subject` só existe como seletor de compatibilidade com o fast,
  aceito com o valor padrão de lá, `loghub.{space}.{type}` (§0).
- `space`, `type`, `uuid` e `step` vêm do **cabeçalho do próprio JLH**
  (`envelope.ParseHead`, §12.1), nunca de uma cópia guardada ao lado: o `space`
  é o Workspace de quem recebeu; numa linha reingerida de um dump, o da própria
  linha, conferido pela porta de §9.2. Nunca vem do cliente HTTP.
- `Nats-Msg-Id` é `<uuid>.<step>`, **nunca** o `uuid` puro: um processador que
  encaminhe para o mesmo stream (mesmo `uuid`, `step + 1`) teria o salto
  legítimo descartado como duplicata pela janela do JetStream.
- Os headers `X-LogHub-*` ajudam o roteamento; o envelope continua a fonte da
  verdade.
- **O JLH vai em `Data`, sozinho, e nunca num header.** O bloco de headers tem
  tamanho **fixo e pequeno**: no máximo 257 bytes (`NATS/1.0` 10, `Nats-Msg-Id`
  com um `step` de até dois dígitos 54, `Content-Type` 32, `X-LogHub-Type` 24,
  `X-LogHub-UUID` 53, `X-LogHub-Space` com um `space` de 64 caracteres 82, e o
  CRLF final 2), muito abaixo do teto de 64 KiB de headers do JetStream
  (`10097`).
- Em `jetstream`, o `WithExpectStream` acrescenta `Nats-Expected-Stream:
  <stream>` (24 bytes mais o nome do stream), que entra na conta da maior
  mensagem (§8.7.5).
- O stream de exemplo (`docker/nats/stream.json`) cobre `loghub.default.webhook`
  e **nunca** usa `loghub.>`, que capturaria o plano de controle
  (`loghub._ctl`); o boot recusa um stream que o capture (§8.7.5).

### 3.9 Tabela `inbox` (`sink=clickhouse`) — retirada

Não existe neste servidor (§0); o espelho da `loghub.inbox` continua
documentado no `go-webhook-fast-server`.

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
9.  ring saturado em itens, ou em bytes pelo Content-Length          -> 429
    (checagem rápida, só para recusar; o Enqueue decide no passo 13)
10. UMA amostra de relógio t: uuid = GenerateAt(Level3, t), event = t
11. lê corpo com http.MaxBytesReader (read_timeout)                  -> 413
    o buffer cresce com o que chegou, não com o Content-Length (§12.2)
12. pré-passada no corpo (utf-8 ou base64, §3.2); serializa o JLH
    UMA vez num buffer do pool; o buffer do corpo volta ao pool
13. entrega ao ring, não-bloqueante                                  -> 429
14. (o ring de memória aceita no próprio passo 13, §2.2)
15. 200 OK, X-LogHub-UUID: <uuid>, corpo JSON (§4.4)
```

**Checagem rápida do passo 9.** Recusa sem ler o corpo quando o ring já está
cheio em itens, ou quando os bytes ocupados mais uma **estimativa por baixo**
do evento já chegam ao teto de bytes. A estimativa é só o `Content-Length`
declarado (0 sem ele): o corpo entra inteiro no JLH, em utf-8 escapado ou em
base64, ambos maiores ou iguais ao tamanho bruto, e o JLH tem sempre mais
que o corpo, então um evento que a checagem recusa o `Enqueue` também
recusaria. O cabeçalho não entra na estimativa: `headers.exclude` tira
cabeçalhos do envelope, e o tamanho do cabeçalho recebido não é limite
inferior do JLH. A checagem nunca aceita nada: um `Content-Length` mentiroso
para menos só passa para o `Enqueue`, que decide (o `429` de lá leva UUID,
§4.4). O `429` da checagem rápida, de itens ou de bytes, sai **sem** UUID.
Até 2026-10-14 ela olhava só os itens: com o ring cheio em bytes, todo
pedido lia o corpo inteiro e serializava o JLH para ouvir o `429` do
`Enqueue` (porte do `go-webhook-server`, W 2).

**Rejeições nos passos 4–9** (antes de ler o corpo) drenam e descartam até
`server.reject_drain_bytes` (64 KiB) do corpo antes de responder, para que um
cliente HTTP/1.1 que ainda está enviando consiga **ler a resposta** na mesma
conexão. Se o corpo exceder isso, responde com `Connection: close`.

Sem esse dreno, o cliente recebe `ECONNRESET` em vez do `429`, não vê o
`Retry-After` e retenta sem backoff — piorando exatamente a sobrecarga que o
`429` deveria conter.

O dreno só acontece em **HTTP/1.x** e **sem `Expect: 100-continue`
pendente**; nos outros casos a recusa responde sem ler o corpo:

- Em HTTP/2 não há dreno nem `Connection: close`. O `net/http` encerra só
  aquele fluxo depois da resposta (`RST_STREAM`) e a conexão segue; um
  `Connection: close` ali vira GOAWAY e derruba a conexão inteira, que um
  proxy divide entre muitos clientes.
- Com `Expect: 100-continue` ainda sem resposta, ler o corpo faria o
  `net/http` enviar o `100 Continue`, convidando o corpo que vai ser recusado.
  Sem a leitura, o `100` nunca sai: o cliente recebe a recusa no lugar dele e o
  `net/http` fecha a conexão depois da resposta (o corpo não enviado não pode
  ficar no meio do fluxo).
- Um cliente que manda `Expect: 100-continue` e **envia o corpo sem esperar**
  o `100` (vários fazem isso depois de ~1 s) também não é drenado: o
  `net/http` fecha a conexão depois da resposta sem o `closeWriteAndWait`
  (o corpo é um `expectContinueReader`, não o `body` que ele confere), e com
  bytes ainda chegando o fechamento pode virar RST e o cliente perder o
  `429`/`503`. Aceito e só documentado (revisão independente de 2026-10-14): a
  resposta sai em milissegundos, antes de o cliente desistir de esperar, e
  corpos acima de `reject_drain_bytes` já fechavam assim antes.

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

Decisão 1 do P (2026-10-07): o rate-limit sai junto com os outros recursos. Ele vinha
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
desse retorno. A interface fica, com uma implementação, pela decisão 4 do P
(§2.1).

### 7.2 Saturação

Dois tetos, avaliados simultaneamente:

- `ring.capacity_items` (padrão 65536)
- `ring.max_bytes` (padrão 512 MiB), contando a memória que o item **retém**
  — `cap(Env)`, o buffer inteiro da classe do pool em que o JLH foi escrito
  (§12.2), mais um custo fixo por item (`ring.ItemOverhead`, 256 bytes: a
  `struct` do item, o `uuid` em texto, o canal de confirmação e a vaga no
  canal do ring). Um JLH de ~2,4 KiB numa classe de 16 KiB conta ~16,25 KiB,
  não 2,4 KiB. O valor cobrado é gravado no item no `Enqueue`
  (`Item.Charged`) e é **esse** valor que o `Commit` e o `Drain` descontam,
  qualquer que seja o estado do buffer depois.

`429 ring_full` dispara quando **qualquer um** estourar. `ring.high_water`
(padrão 0.80) é avaliado nos dois e controla `/readyz`. Os gauges
`whs_ring_items` e `whs_ring_bytes` (§11.1) mostram as mesmas grandezas que os
tetos comparam.

**Os dois tetos contam a mesma população: aceitos e ainda não confirmados**,
o que inclui os lotes que os workers retiraram do ring e seguram enquanto
retentam (§10.4). O teto de itens era só o tamanho do canal: um item retirado
por `Dequeue` saía do canal sem sair da conta, e o ring aceitava até
`sink.workers × sink.batch` itens além de `capacity_items`, enquanto o teto de
bytes, contado até o `Commit`, dizia o contrário (achado da triagem de
2026-10-14; a família syslog já contava assim; porte do `go-webhook-server`,
W 30). Agora o `Enqueue` reserva o item no contador antes de pôr no canal, e o
`Commit` o devolve.

**Decisão do usuário (2026-10-14): cobrar o buffer retido, sem copiar o JLH
para uma classe menor** e sem mudar os padrões (porte do `go-webhook-server`,
W 8). Cobrar `len(Env)` deixava o ring reter de 4 a 7× `ring.max_bytes` com
eventos pequenos (classes espaçadas 4×), além de `GOMEMLIMIT` e do limite do
contêiner (§12.4); copiar para a classe justa custaria uma cópia por
requisição e não eliminaria o fator. A consequência aceita: com eventos
pequenos o teto de bytes passa a mandar antes — 512 MiB / ~16 KiB ≈ 32 000
eventos, abaixo de `capacity_items` —, e uma instância que vivia perto do teto
vê `429` antes. `Item.Size()` continua `len(Env)`: é o tamanho do **dado**.

Os dois são necessários: com corpos de 200 bytes o teto de itens é quem manda;
com corpos de 5 MiB, o de bytes. Só o de itens permitiria 65536 × 5 MiB = 320
GiB de exposição teórica; só o de bytes permitiria 2,6 milhões de itens
pequenos, com profundidade e latência de dreno absurdas e um shutdown que nunca
termina dentro do `drain_timeout`.

**O que o `ItemOverhead` não conta** (revisão independente de 2026-10-14, só
documentado). A cópia que os workers guardam dos lotes retidos (`held`, 8
bytes por ponteiro) e a fatia que o `Dequeue` devolve ficam fora da cobrança:
no máximo `sink.workers × sink.batch × 16` bytes (64 KiB com os padrões), fora
da conta de `ring.max_bytes`. O "pior caso" do número é o do item no canal;
contar também essas fatias pediria cobrar na saída o que já saiu do ring.

**Piso de `ring.max_bytes`** (revisão independente de
2026-10-14, W 35; o piso que o `go-relplog-pg-server` e o
`go-relplog-nats-server` já tinham). O maior evento que a configuração aceita,
`envelope.MaxEventBytes` de `server.max_body_bytes`, `server.max_header_bytes`
e `server.max_uri_bytes` (§3.2), custa ao ring de memória a capacidade da
classe do pool que o comporta (o próprio valor acima de 256 KiB, §12.2) mais
`ring.ItemOverhead`. Um `ring.max_bytes` menor que essa cobrança é `exit 1` na
validação, com os dois valores e o piso na mensagem: abaixo dele um evento que
a configuração aceita receberia `429` sempre, mesmo com o ring vazio, e o
emissor o retentaria para sempre. Com os padrões o piso é ~7,7 MiB, muito
abaixo dos 512 MiB.

### 7.3 `ring.mode = memory`

Canal buffered de `capacity_items`, com dois contadores atômicos (itens e
bytes retidos, §7.2) que sobem no `Enqueue` antes do canal e descem no
`Commit`.

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

type Error struct {
    Class Class      // network | logic | fatal
    Op    string
    Err   error
    // Delivered, num erro logic, lista os índices do lote que o destino
    // GUARDOU apesar da recusa de outros (entrega parcial, §8.7.2). nil
    // quer dizer "nenhum": o lote inteiro foi recusado.
    Delivered []int
}
```

Os workers de dreno (`sink.workers`, padrão 8, §8.7.1):

1. `Dequeue` até `sink.batch` (256) itens, esperando no máximo
   `sink.batch_max_delay` (5ms) para completar o lote. O worker só termina
   quando o contexto acaba ou o ring diz `ErrClosed`; qualquer outro erro é
   logado com amostragem (um `WARN` a cada 30 s, com a contagem dos
   suprimidos) e retentado com backoff (`retry_backoff_min` →
   `retry_backoff_max`). O ring de memória não devolve outro erro: a regra é
   por simetria com o fast, onde o `ring=sqlite` devolve `SQLITE_BUSY` e erros
   de I/O, e qualquer erro encerrava o worker em silêncio (porte do
   `go-webhook-server`, W 10).
2. **Corta o lote em pedaços por bytes.** O pedaço fecha quando a soma dos JLH
   (`len(Env)`) passaria de `sink.batch_max_bytes` (padrão 16 MiB); o item que
   passaria abre o pedaço seguinte, e um JLH maior que o teto forma um pedaço
   sozinho. Nenhum JLH é dividido nem recusado por tamanho.
3. `sink.Write(ctx, pedaço)`, com a retentativa de §10.4: o worker segura o
   pedaço **e o resto do lote** (os dois ficam registrados para o dump, §9), e
   não puxa mais nada do ring.
4. Em sucesso: `ring.Commit(pedaço)`, devolve os buffers ao pool,
   `whs_events_written_total += n`.
5. Num `logic` com `Delivered`: os entregues contam em
   `whs_events_written_total`, só os recusados em
   `whs_events_dropped_total{reason="sink_logic"}` e no `ERROR` com os `uuid`;
   o pedaço inteiro sai do ring (§10.2). Sem `Delivered`, o pedaço inteiro é
   perda.
6. Próximo pedaço; com o lote esgotado, volta ao passo 1.

**Por que o teto de bytes** (decisão do usuário, 2026-10-14; porte do
`go-webhook-server`, W 11). O lote era limitado só por contagem: com corpos
grandes, 256 envelopes chegam a centenas de MiB, que não cabem em
`sink.op_timeout` (5 s); a escrita estoura o prazo, é `network`, e o mesmo lote
é retentado para sempre com o sink UP — e a reingestão repetia o mesmo lote a
cada boot. `ring.max_bytes` não servia de teto: 512 MiB num lote é o mesmo
problema. O teto vale em todo caminho que escreve um lote: os workers e a
reingestão do dump (§9.2). O teto é por pedaço, não por `Dequeue`: o ring
entrega até `sink.batch` itens como antes, e o worker os entrega em um ou mais
pedaços — o ring de memória não pode devolver um item à cabeça do canal.
Aqui cada mensagem é uma publicação, e um pedaço é o que fica em voo de uma
vez por worker: 256 envelopes de 5 MiB eram 1,25 GiB pendentes de `PubAck`
dentro do mesmo `sink.op_timeout`.

### 8.2 `sink.type = redis` — retirado

### 8.3 `sink.type = sqlite` — retirado

### 8.4 `sink.type = none` — retirado

Não existem neste servidor (§0). O `sink=none` do fast era a ferramenta de
medição do custo do pipeline sem I/O; a medição equivalente aqui é a do fast,
que tem o mesmo pipeline.

### 8.5 `sink.type = postgres` — retirado

A fila `loghub.inbox` do PostgreSQL é o destino do `go-webhook-pg-server`, de
onde este servidor veio (§0); a seção continua lá.

### 8.6 `sink.type = mysql` — retirado

Não existe neste servidor (§0).

### 8.7 `sink.type = nats`

O único destino. Mensagem em §3.8. O NATS é **remoto e sem esquema**, com um
consumidor do outro lado, e o servidor não é dono de nada dele: nunca cria nem
altera um stream (§8.7.5). Por padrão (`sink.nats.mode = jetstream`) cada
publicação é **confirmada** pelo stream com um `PubAck`; em `core` (opt-in), o
sucesso só diz que o servidor NATS recebeu a mensagem (§8.7.2).

Origem do código (decisões em `docs/DECISOES.md` §2 e §3): o sink NATS do
`go-syslog-nats-server`, com as correções da revisão independente dele,
adaptado ao item deste servidor, mais duas peças do sink NATS do
`go-webhook-fast-server` — os cabeçalhos lidos do próprio JLH e a conferência
de subject de outro `space` (`cover`, §8.7.5).

#### 8.7.1 Cliente e conexão

`github.com/nats-io/nats.go`, com o pacote `jetstream` (a API atual, não a
`nats.JetStreamContext` legada). Go puro, sem CGO; justificativa em §14.3.

**Uma conexão por processo**, multiplexada pelos workers e pelo beacon
(§8.7.10). O cliente serializa as publicações numa única saída, então
`sink.workers` fica no padrão de 8: não há orçamento de conexões a proteger. A
conexão é aberta **preguiçosamente** na primeira checagem de saúde, e é o
`WaitReady` do boot (§10.1) quem retenta, com a disciplina de backoff de
sempre. O nome do cliente é `whs/<AgentName>@<Hostname>` (§3.5), visível no
`connz` do monitor do `nats-server`. A discagem tem o prazo de
`sink.op_timeout`.

**Sempre texto claro** (§1.2): sem `nats.Secure`, sem *handshake* TLS. Toda
entrada de `sink.nats.urls` precisa ser `nats://` com host; `tls://`, `ws://`,
`wss://` e um `host:porta` sem esquema são recusados pela configuração (exit 1,
§10.1) e, de novo, pelo próprio sink ao ser montado (defesa em profundidade: o
`nats.go` discaria qualquer um deles). Isso não basta: o `nats.go` **se promove
a TLS sozinho** quando o `INFO` do servidor diz `tls_required`, com um
`tls.Config` padrão e as raízes do sistema. O único gancho que ele consulta
antes do *handshake* é o `TLSCertCB`, e o sink o instala — e nada mais de TLS,
que ligaria o `Secure` — como uma recusa: a conexão termina antes de qualquer
`ClientHello`, com `errNATSServerRequiresTLS`, que é **fatal**. Um servidor que
exige TLS nunca aceitará o texto claro deste, então o boot sai com 3 na hora,
nomeando o TLS, em vez de retentar até `sink.boot_timeout`. "Sem TLS" se prova
com um servidor que pede TLS, não lendo as opções (lição da revisão do
`go-syslog-nats-server`).

Depois de conectado, o cliente reconecta sozinho, sem limite de tentativas
(`MaxReconnects(-1)`, `ReconnectWait(500ms)`): a máquina de estados de §10.2 já
tem os fusíveis que decidem quando desistir, e um cliente que se fechasse por
conta própria depois de N tentativas transformaria uma indisponibilidade longa
num estado irrecuperável sem passar pelo dump de emergência. O **buffer de
reconexão é desligado** (`ReconnectBufSize(-1)`): durante uma desconexão,
publicar falha na hora com classe `network`, em vez de guardar mensagens dentro
do cliente. O ring é o único buffer deste desenho, e uma fila escondida no
cliente escaparia da contabilidade, do `429` e do dump.

A desconexão é logada em `WARN` (a do próprio `Close` não: um `WARN` em todo
desligamento limpo ensinaria o operador a ignorar o que significa uma queda); a
reconexão conta em `whs_sink_nats_reconnects_total`, é logada em `WARN` e
dispara um beacon (§8.7.10); um fechamento decidido pelo cliente (credenciais
recusadas duas vezes) é `ERROR`, e o estado `CLOSED` é fatal (§8.7.6), porque
dali o cliente nunca mais reconecta. Os erros assíncronos do cliente (uma
permissão de publicação negada gera um por mensagem) são logados com
amostragem, um a cada 30 s com a contagem suprimida: §11.3 não admite log por
mensagem no caminho padrão.

#### 8.7.2 Escrita

**`sink.nats.mode = jetstream`** (padrão). Publicação **assíncrona em lote**:
cada item vira um `PublishMsgAsync`, e só depois de os N futuros estarem no ar
o worker espera as N confirmações, sob o `sink.op_timeout` do lote. Uma
publicação síncrona por item custaria um *round-trip* por evento por worker.

```go
f, err := js.PublishMsgAsync(msg,
    jetstream.WithExpectStream(stream),   // o servidor recusa se o subject cair noutro stream
    jetstream.WithRetryAttempts(0))       // ver abaixo
...
select {
case ack := <-f.Ok():    if ack.Duplicate { dup++ }
case err := <-f.Err():   // logic: só esta mensagem; network/fatal: o lote inteiro
case <-ctx.Done():       return network(ctx.Err())
}
```

Três opções que não são as padrões da biblioteca, e são as que importam:

- **`WithExpectStream`**: o header `Nats-Expected-Stream` faz o servidor
  rejeitar a mensagem se o subject pertencer a outro stream (`10060`, fatal). Um
  erro de configuração vira recusa, não mensagens no lugar errado.
- **`WithRetryAttempts(0)`**: por padrão a biblioteca **retenta sozinha**, em
  segundo plano, uma publicação que recebe "sem respondedores" — relendo
  `msg.Data` depois de o `Write` ter retornado. `msg.Data` é o buffer do item,
  que §12.3 devolve ao pool depois da confirmação; uma releitura tardia é
  exatamente a corrida que §12.3 proíbe. Com zero tentativas internas, quem
  retenta é o worker (§10.4), com o item ainda dono do buffer.
- **`WithPublishAsyncTimeout(sink.op_timeout)`**: sem ele, um futuro sem
  resposta fica pendente para sempre, segurando a mensagem — e o buffer — na
  tabela interna do cliente.

`WithPublishAsyncMaxPending` = `2 × sink.workers × sink.batch`: o dobro do
que os workers podem ter no ar ao mesmo tempo (a reingestão roda antes de eles
começarem). Um teto menor faria `PublishMsgAsync` bloquear e depois falhar com
"stalled" numa carga normal. O dobro porque os futuros de um lote contaminado
continuam contando até o próprio prazo assíncrono, que começa na publicação, um
pouco depois do prazo do worker: uma retentativa rápida encontrava a janela
cheia e falhava com `ErrTooManyStalledMsgs` (`go-relplog-nats-server`, revisão
independente de 2026-10-13; porte da família syslog/RELP, 2026-10-13). Os
tetos de `sink.workers` e `sink.batch` (§13.5) mantêm a conta num `int`.

O JLH vai em `Data`, o slice do item, **sem cópia**: a biblioteca o serializa
para o socket dentro da chamada e, sem retentativa interna, não o lê de novo.
Mesmo assim, **todo erro depois da primeira publicação contamina** os itens do
lote (`Taint`, §12.3): um futuro ainda pendente aponta para o `Env` deles, e o
buffer fica para o GC em vez de voltar ao pool. É o caminho raro, e o custo é
uma alocação por item numa falha.

**Entrega parcial.** Uma recusa `logic` de **uma** mensagem — grande demais,
recusada pelo cliente antes de sair (`ErrMaxPayload`) ou pelo stream (`10054`),
ou um item cujo cabeçalho não é de um JLH deste servidor — **não encerra o
lote**: as demais mensagens continuam publicadas, e os futuros dizem quais o
stream guardou. O erro devolvido é `logic` com `Delivered`, a lista dos índices
entregues, e o worker conta como perdidas só as recusadas (§8.1, §10.2).
Encerrar o lote na primeira recusa deixava as mensagens seguintes **nunca
publicadas e confirmadas**, e contava as anteriores como perdidas apesar de
estarem no stream — o defeito que a revisão do `go-syslog-nats-server` achou,
e que o sink do fast tem. Um futuro com erro `network` ou `fatal`, ao
contrário, encerra a espera na hora: o lote volta inteiro, contaminado.

**`sink.nats.mode = core`** (opt-in, *best effort*): um `PublishMsg` por item e
um *flush* por lote, sob o prazo do worker (ou `FlushTimeout(sink.op_timeout)`
sem prazo). O cliente copia `Data` para o buffer de escrita dentro do
`PublishMsg`, então nada lê o `Env` depois do `Write` e não há contaminação. O
sucesso significa que o servidor NATS **recebeu** as mensagens, não que alguém
as guardou: sem stream, uma mensagem sem assinante some, e não há
deduplicação. O boot registra um `WARN` com essa frase, e a primeira checagem
um `INFO` com a maior mensagem e o `max_payload`. A entrega parcial vale aqui
também (`ErrMaxPayload` recusa só aquela mensagem). O `200` continua
significando "está no ring" (§2.2); `core` muda só o que a entrega ao destino
garante (§16.1).

Três casos em que o *flush* volta sem erro, ou com o erro errado, e o lote
**não** é confirmado (porte da família syslog/RELP, 2026-10-13, do
`go-syslog-nats-server`, que os trouxe do `go-relplog-nats-server`):

- **A conexão foi restabelecida durante o lote** (a contagem de reconexões do
  cliente mudou entre o primeiro `PublishMsg` e o fim do *flush*). Um *flush*
  que **começou** com o cliente reconectando põe o seu `PING` na fila do
  servidor novo, cujo `PONG` o resolve — mas os `PUB` foram pelo socket velho.
  É `network`: a retentativa publica de novo.
- **A conexão caiu no meio do *flush***: o `nats.go` falha o *flush* pendente
  com `ErrConnectionClosed`, embora o cliente esteja só `RECONNECTING`. A
  tabela de §8.7.6 classifica esse erro como fatal (cliente fechado de vez), e
  um soluço do NATS virava FATAL, dump e exit 3. Com a conexão não fechada, é
  `network` (também no *flush* da checagem de saúde); fechada de vez, segue
  fatal.
- **Uma negação nova que não é do caminho de dados chegou durante o lote**: o
  `nats.go` guarda só o **último** `-ERR`, e a negação do subject do beacon
  entre os `PUB` do lote e o `PING` do *flush* esconderia uma negação do caminho
  de dados anterior a ela. Incerto, não é confirmado (`network`): a retentativa
  encontra o veredito que o `ErrorHandler` já guardou, ou passa.

**Subject e cabeçalhos** (§3.8) saem do próprio JLH, por `envelope.ParseHead`
(§12.1): `Nats-Msg-Id` = `<uuid>.<step>`, `X-LogHub-Type`, `X-LogHub-UUID`,
`X-LogHub-Space`, e o subject `loghub.<space>.webhook` (o subject de cada
`space` visto fica num mapa: as chaves possíveis são o Workspace desta
instância e os dos dumps que o operador pôs no diretório). Um item que volta de
um dump é indistinguível do que entrou.

#### 8.7.3 Desduplicação e at-least-once

`Nats-Msg-Id` carrega `<uuid>.<step>` (§3.8). O stream responde como duplicata
uma segunda publicação com o mesmo id dentro da `duplicate_window` (dois
minutos por padrão; `docker/nats/stream.json` usa dez), com
`PubAck.Duplicate = true`, que conta em `whs_sink_nats_duplicates_total`. Fora
da janela, uma redelivery (um dump de emergência reingerido horas depois)
**duplica** a mensagem: o pipeline é at-least-once por desenho, e o consumidor
do stream deduplica por `(uuid, step)`, como o JLH manda. A janela é do stream,
não deste servidor. Em `core` não há janela nenhuma.

#### 8.7.4 Durabilidade

O `PubAck` significa que o stream **gravou** a mensagem conforme a própria
configuração: `storage: file` e o número de réplicas. Contra a queda do
processo `nats-server`, o file store só sincroniza com o disco a cada
`sync_interval` (dois minutos por padrão) — um crash do servidor NATS perde o
que estava entre dois `fsync`, a menos que `sync_always: true` esteja ligado ou
que o stream tenha réplicas. O servidor de webhooks não lê nem altera nada
disso; a linha de §16.1 leva a marca `†` por essa razão.

#### 8.7.5 Validação do stream (no boot e em toda checagem)

Na primeira checagem de saúde que alcança o stream (`jetstream`), e de novo em
**toda** checagem depois dela, contra o `StreamInfo` que a própria checagem já
buscou (nenhuma ida e volta a mais). Cada recusa é **fatal**: no boot o
`WaitReady` desiste na hora (exit 3, sem esperar `sink.boot_timeout`, com o log
"the sink refused this server permanently"); depois, FATAL, dump e exit 3
(§10.2). Conferido só no boot, um stream editado por baixo — subjects
estreitados, `no_ack` ligado — respondia toda publicação com um prazo estourado,
`network`, retentado para sempre com o sink UP e o ring enchendo. O log "stream
validated" sai uma vez.

| Verificação | Como | Se falhar |
|---|---|---|
| O stream existe | `js.Stream(sink.nats.stream)` (padrão `LOGHUB_WEBHOOK`) | exit 3 (`10059`) |
| O JetStream responde | "sem respondedores" (`ErrNoResponders`) no `StreamInfo` da **primeira** checagem: o JetStream está desligado neste servidor, ou nenhum servidor com JetStream está ao alcance (uma folha com o *hub* fora); o cliente não distingue os dois. Uma conta sem JetStream num servidor com ele responde `10039`, fatal por si | exit 3 na hora, nomeando o JetStream — como `network`, o boot esperaria `sink.boot_timeout` e sairia com o motivo errado. Depois de uma checagem validada, o mesmo é `network` (um soluço). **Não se pergunta `AccountInfo`** (perguntado aqui até 2026-10-11): ele não distinguia nada — o `jetstream` traduz "sem respondedores" para "não habilitado" sozinho —, e o pedido a `$JS.API.INFO`, negado a um papel mínimo, virava veredito de permissão permanente (§8.7.2), FATAL mesmo depois de o JetStream voltar (revisão do porte para o fast) |
| O stream **cobre** o subject do Workspace | casamento local de padrões, com as regras de token do NATS (`*` é um token, `>` um ou mais, só no fim): algum subject do stream cobre `loghub.<space>.webhook`, com o `space` desta instância | exit 3 — publicar cairia noutro stream, ou em nenhum |
| Nenhum subject do stream captura o plano de controle | um padrão que case `<loghub.ctl_namespace>.beacon` (como `loghub.>`) | exit 3 — o stream guardaria todo beacon e os reproduziria, velhos, para todo consumidor novo (§17.21) |
| Aceita publicações | `Sealed = false` | exit 3 |
| Confirma publicações | `NoAck = false` | exit 3 — um stream com `no_ack` transformaria todo futuro em `ErrAsyncPublishTimeout` (`network`), retentado para sempre com o ring enchendo e o sink UP |
| A maior mensagem cabe | `nc.MaxPayload()` e o `max_msg_size` do stream (quando > 0) contra a maior mensagem (abaixo); com `storage: file`, o registro (maior mensagem + subject + 64 B) contra os 32 MiB do *file store* | exit 3, com os números — uma mensagem aceita com `200` e recusada por tamanho na entrega seria `logic`, perdida. Acima de 32 MiB a recusa do *file store* é `10077`, fatal, e só o boot a evita |
| O stream cabe uma mensagem | com `discard: new` e `max_bytes` > 0, o registro contra o `max_bytes` | exit 3 — o primeiro evento grande seria recusado, e todo boot depois do dump validaria e cairia na reingestão do mesmo jeito; com o `10077` de stream cheio agora `network` (§8.7.6), seguraria o ring para sempre. Com `discard: old` o stream abre espaço |
| O stream guarda o que confirma | `retention ≠ interest` | exit 3 — com `interest` e nenhum consumidor interessado o servidor responde um `PubAck` normal e descarta a mensagem: o item sai do ring contado como escrito e se perde sem log (§17.18). Decisão do `go-syslog-server` (2026-10-11), padrão assumido aqui (`docs/DECISOES.md` §3); fecha §18.1 item 7 |
| Um teto por subject | `max_msgs_per_subject > 0` | só `WARN`, uma vez por valor: todo evento de um `space` divide um subject, e o stream descarta os mais antigos à medida que confirma os novos — legítimo num stream de "só o último" |
| Os subjects de outro `space` já publicados continuam cobertos | casamento local de cada subject que o `cover` (abaixo) aprovou contra os subjects do stream | **nenhuma recusa**: o subject que o stream deixou de cobrir é **esquecido** do cache, e a próxima publicação nele, se houver, pergunta de novo e é recusada então, nomeando-o. Só a reingestão de um dump de outro Workspace publica ali, e o operador que estreita o stream depois de o dump drenar — o que o próprio erro do `cover` diz que ele pode fazer — não pode derrubar um processo saudável (até 2026-10-11 era FATAL, dump do ring e exit 3; achado da revisão do porte para o fast) |

```
maior mensagem = maior envelope (§3.2) + 257 B de headers (§3.8)
                 [+ 24 B + nome do stream, em jetstream: o Nats-Expected-Stream]
```

Com os padrões (5 MiB de corpo, ~7,7 MiB de envelope), a maior mensagem passa
do `max_payload` padrão do NATS (1 MiB): o boot recusa e diz para subir o
`max_payload` (o `docker/nats/nats-server.conf` usa 8 MiB, o teto do NATS que
não é só aviso) ou baixar `server.max_body_bytes`. É de propósito: o número é
pessimista, e um fator "realista" deixaria um único emissor produzir uma
mensagem que o NATS recusa depois de ela ter sido confirmada ao emissor.

Em `core` só o `max_payload` é conferido, também em toda checagem: uma
reconexão pode cair num servidor com `max_payload` menor.

**Subject de outro `space`.** Em `core` não há stream a perguntar: um dump de
outro Workspace é publicado às cegas e só um assinante daquele subject o
recebe; o sink registra um `WARN` por subject (nunca por mensagem), para que a
perda provável ao menos não seja silenciosa.

Em `jetstream`, o boot confere só o subject desta instância, e o
subject é calculado por envelope (§3.8): um dump de outra vida, com outro
Workspace, cai num subject que talvez nenhum stream cubra. Antes de publicar
**qualquer** item de um lote, cada subject diferente do da
instância é conferido uma vez com `js.StreamNameBySubject` (o coberto fica em
cache, até 1024): nenhum stream, ou outro stream que não o configurado, é
**fatal** nomeando o subject e o `space` — o stream precisa cobri-lo também até o
dump drenar. Publicado às cegas, dava "no responders", `network`, e o boot
esperava `sink.boot_timeout` a cada restart sem nomear a causa. Do sink do fast.
Uma checagem de saúde que encontre um subject do cache descoberto o esquece
(tabela acima): estreitar o stream depois do dump drenado é o uso previsto.

O stream é criado por quem administra o NATS, a partir de
`docker/nats/stream.json` (`nats stream add LOGHUB_WEBHOOK --config
stream.json`). **O servidor nunca cria nem altera um stream** (§17.17): N
instâncias subindo juntas, uma conta sem permissão de administração do
JetStream, e a evolução do stream entre versões.

#### 8.7.6 Classificação de erro

Erros da API do JetStream trazem um código numérico (`APIError.ErrorCode`, da
tabela de erros do `nats-server`) e um código HTTP (`APIError.Code`); os da
biblioteca são valores sentinela. A tabela **é** a função `classifyNATS`, lida
de volta por `TestClassifyNATS`; as duas mudam juntas.

| Erro | Situação | Classe |
|---|---|---|
| prazo, cancelamento, `nats.ErrTimeout`, `ErrNoResponders`, `ErrNoServers`, `ErrDisconnected`, `ErrConnectionReconnecting`, `ErrReconnectBufExceeded`, `jetstream.ErrNoStreamResponse`, `ErrAsyncPublishTimeout`, `ErrTooManyStalledMsgs` | sem resposta, ou cliente entre servidores | `network` |
| `nats.ErrAuthorization`, `ErrAuthExpired`, `ErrAuthRevoked`, `ErrAccountAuthExpired`, e o texto do servidor no `CONNECT` com "authorization violation", "authentication expired" ou "authentication revoked" (o `errors.Is` do `nats.go` compara o texto inteiro, e "User Authentication Expired" nunca casava com o sentinela); `ErrConnectionClosed`; servidor que exige TLS (`errNATSServerRequiresTLS`) | credencial recusada, cliente encerrado, TLS que este servidor não tem | **`fatal`** |
| `jetstream.ErrStreamNotFound` (`10059`), JetStream desligado (`10076`, `10039`) | stream sumiu, JetStream desligado | **`fatal`** |
| API `10060` stream esperado não confere, `10109` selado | contrato de configuração quebrado | **`fatal`** |
| API `10023` recursos insuficientes, `10077` armazenamento falhou | disco ou limite da conta | **`fatal`** |
| API `10077` com a descrição do *store* "maximum messages exceeded", "maximum bytes exceeded", "maximum messages per subject exceeded" (stream com `discard: new` cheio) ou "store is closed" (stream ou servidor fechando: o `ErrStoreClosed` de `server/store.go` do `nats-server` 2.14.6; "store closed" também, grafia antiga) | contrapressão, reinício do NATS | `network` — passam sozinhos. Como fatal eram dump e exit 3, e a parte já gravada do lote duplicava na reingestão (revisão independente do `go-relplog-server`, 2026-10-11). A família casava só "store closed", texto que o servidor não manda (revisão do `go-syslog-server`, 2026-10-13). Um registro que o *store* não comporta continua fatal |
| API `10158` (409, o mesmo `Nats-Msg-Id` ainda numa proposta Raft), `10167` (429, fila de entrada do stream cheia) e todo `Code` 429 | transitórios, embora 4xx | `network` — pela regra do 4xx desconhecido, um soluço do NATS virava FATAL, dump e exit 3 em toda instância de uma vez (fecha §18.1 item 8) |
| `nats.ErrMaxPayload` (recusada no cliente: só aquela mensagem, §8.7.2), `jetstream.ErrMaxBytesExceeded`, API `10054`, `10097` | uma mensagem grande demais | `logic` — inalcançável depois de §8.7.5, salvo mudança no servidor |
| violação de permissão (`ErrPermissionViolation`) numa publicação em `loghub.<space>.webhook`, na API `$JS.API` ou numa assinatura em `_INBOX.`, vista pelo `ErrorHandler` ou lida do `LastError` | papel sem permissão | **`fatal`**, pelo veredito (abaixo) |
| outro erro da API com `Code` 4xx (fora 429) | pedido recusado por forma: repetiria em toda tentativa | **`fatal`** |
| outro erro da API com `Code` 5xx, qualquer outro | falha do servidor, desconhecido | `network` |

**Veredito.** O cliente só informa algumas recusas definitivas de forma
**assíncrona**: uma publicação negada por permissão não volta como erro dela
(em `jetstream` o futuro estoura o prazo e seria retentado para sempre com o
sink UP; em `core` a mensagem some e contava como escrita), e um
`$JS.API.STREAM.INFO` negado segurava o boot até o `boot_timeout`. Por isso o
sink guarda um **veredito** quando o `ErrorHandler` vê `ErrPermissionViolation`
numa publicação em `loghub.<space>.webhook` (de qualquer `space`: um dump
reingerido publica no seu), na `$JS.API` ou numa **assinatura em `_INBOX.`** —
onde chegam as confirmações e as respostas da API: negada ali, todo lote expira,
e o boot esperava o `sink.boot_timeout` culpando a rede (fecha §18.1 item 9; o
beacon só publica, então uma negação em `_INBOX.` é sempre do caminho de
dados) —, e o `Health()` e todo `Write` o devolvem como **fatal**: dump e exit 3
(§10.2). O `-ERR` chega antes de a confirmação (ou o `StreamInfo`) estourar o
prazo; quando a ida e volta falha como `network`, o sink lê o `LastError()` da
conexão, e havendo veredito o **próprio** `Write` ou `Health` que esbarrou nele
já é fatal (antes era `network`, retentado, até a checagem seguinte; porte da
família syslog/RELP, 2026-10-13). Em `core` o `Write` não espera o
`ErrorHandler`: o servidor manda o `-ERR` antes do `PONG` do *flush*, então o
`LastError()` da conexão já o tem quando o *flush* volta, e o lote que
encontrou a negação volta fatal e fica retido para o dump; a checagem `core`
lê o mesmo `LastError()` depois do seu *flush*. Uma negação no subject do
**beacon não é veredito**: o plano de controle não pode derrubar o plano de
dados (§8.7.10). Uma reconexão recusada porque o servidor passou a exigir TLS
também é veredito, mas o `nats.go` **não** a relata pelo `ReconnectErrCB` (que
é da discagem TCP): a recusa do `TLSCertCB` vem depois, no *handshake*, e fica
no `LastError()` com a conexão em `RECONNECTING`; a checagem de saúde a lê ali
(antes o FATAL vinha dos fusíveis, ~20 s depois, dizendo "connection is
RECONNECTING" — ou nunca, com os dois fusíveis desligados; achado do
`go-relplog-nats-server`, porte da família syslog/RELP, 2026-10-13). Num
cluster misto — uma migração para TLS em andamento — isso vale mesmo que outro
nó do *pool* ainda aceitasse texto claro: o primeiro nó com TLS que a reconexão
sortear encerra o processo. É consistente com a decisão de não ter TLS: um
cluster que está passando a exigi-lo não é mais um destino deste servidor. O
`go-syslog-server`, que tem TLS, fez da recusa de um nó de um *pool* `network`
(revisão de 2026-10-13, C1); aqui a regra de cima fica (`docs/DECISOES.md` §3).

O `ErrMaxBytesExceeded` só existe em consumidores *pull*: o ramo é inalcançável
aqui e fica por simetria. O `10097` (headers acima de 64 KiB) é inalcançável,
porque o bloco de headers é fixo e pequeno (§3.8); é `logic` e não o `fatal` de
um 4xx desconhecido porque, se acontecesse, seria sobre aquela mensagem, não
sobre o stream. Erro desconhecido é `network`, nunca `logic` (§17.18): um ring
que enche é visível, um lote descartado não.

#### 8.7.7 `Health()`

O veredito (§8.7.6), se houver, é fatal. Depois, o estado da conexão
(`CONNECTED` é saúde; `CLOSED` é fatal; `RECONNECTING`/`DISCONNECTED` é falha
de rede) e, em `jetstream`, um `StreamInfo`, que prova que o JetStream responde
**e** que o stream ainda existe — um `PING` responderia feliz com o JetStream
desligado, e o estado nunca sairia de UP; em `core`, um *flush*. **Toda**
checagem roda também a validação de §8.7.5 contra o que acabou de buscar (até
a revisão de 2026-10-10 rodava uma vez; esta frase continuou dizendo "uma vez"
até a revisão do porte de volta), e só o log "stream validated" sai uma vez. Um
`StreamInfo` que recebe "sem respondedores" é FATAL na primeira checagem e
`network` depois de uma validada (§8.7.5). Fora de `CONNECTED`, quando o
`StreamInfo` falha como `network` e quando o *flush* `core` volta, a checagem
lê o `LastError()` da conexão (§8.7.6): uma recusa permanente — um servidor que
passou a exigir TLS, uma negação do caminho de dados — vira o veredito ali
mesmo. Um *flush* `core` cortado por uma queda é `network`, não o fatal do
`ErrConnectionClosed` (§8.7.2). A validação cabe numa ida e volta (o `StreamInfo` já traz a configuração; a cobertura é
conferida localmente), então roda no prazo da checagem, `sink.health_timeout`,
sem o prazo próprio que a do PostgreSQL precisava no P.

#### 8.7.8 Métricas

```
whs_sink_nats_duplicates_total              counter  PubAck com Duplicate=true
whs_sink_nats_reconnects_total              counter  reconexões do cliente
whs_beacons_total{result}                   counter  result: ok|error (§8.7.10)
```

Os nomes das duas primeiras são os do `go-webhook-fast-server`.

#### 8.7.9 Autenticação, e nada de TLS

Uma de três formas, e só uma: `sink.nats.username`/`password`,
`sink.nats.token` ou `sink.nats.credentials` (um arquivo `.creds` com JWT e
semente NKey). A biblioteca aceitaria todas ao mesmo tempo e escolheria uma em
silêncio; a validação recusa a ambiguidade (exit 1, §10.1), assim como
`password` sem `username`. Um `credentials` que não existe é exit 1 ao montar o
sink: a biblioteca só leria o arquivo ao conectar, e o caminho errado viraria um
laço de retentativa do boot.

Uma URL de `sink.nats.urls` pode trazer credenciais (`nats://u:senha@host`,
`nats://token@host`). Ela é **parseada na validação, sem eco**: um `%`, `/`,
`#`, `?` ou `,` sem escape na senha falhava dentro do `nats.Connect` como
`network`, com a senha em cada log, até o `boot_timeout`; agora é exit 1 com uma
mensagem que não cita **nada** da URL — nem o trecho que o erro do `net/url`
cita (`invalid port ":Sup3r"` é a senha até a barra). Uma URL com caminho,
consulta ou fragmento é exit 1 como em `LOGHUB_EB_URL`: é também o que pega a
senha que parseia (`nats://u:1234/5678@host` é o host `u`, a porta 1234 e um
caminho, sem credencial nenhuma). Desde o porte da família syslog/RELP
(2026-10-13, as URLs NATS do `go-syslog-nats-server`): um `@` depois do
primeiro `/`, `?` ou `#` é uma senha com um deles sem escape, e o erro diz qual
usar (`%2F`, `%3F`, `%23`); um pedaço da lista sem esquema depois do primeiro
diz que uma `,` sem escape numa senha parte a lista; e um *escape* no host é
nomeado como problema do host, não da senha. Os logs de conexão e reconexão mostram só
esquema e host: o `ConnectedUrlRedacted` do `nats.go` esconde a senha, não o
token. *Userinfo* na URL junto com `username`, `token` ou
`credentials` é exit 1: o `nats.go` prefere o da URL em silêncio. O
`--dump-config` redige o *userinfo* (`nats://usuario:***@host`; um usuário sem
senha pode ser um token e é redigido inteiro).

**Sem TLS** (decisão do `go-syslog-nats-server`, `docs/DECISOES.md` §2): não há
`sink.nats.tls_ca` nem `tls_insecure`, e `sink.nats.tls` existe só como seletor
de compatibilidade com o fast, aceito com `false` (§0). Credenciais (senha,
token, o JWT do `.creds`) e eventos trafegam em claro: o NATS precisa estar
numa rede confiável.

**Fechamento:** `Close`, não `Drain`: os workers param antes de o sink fechar
(§10.3), então não há nada a esvaziar.

#### 8.7.10 Plano de controle: heartbeat beacon

Decisão 21 do fast e decisão 1 do `go-syslog-nats-server`. O servidor é um
driver de si mesmo (não existe `go-loghub-driver`), então emite o heartbeat que a
skill `loghub-json-schema` exige de todo worker, e **só** ele. Como o único sink
é o NATS, o beacon sai **sempre**, nos dois modos.

- **Quando:** na primeira conexão, em toda reconexão (o `ReconnectHandler` do
  `nats.go`) e a cada `loghub.beacon_interval` (env `LOGHUB_BEACON_INTERVAL`;
  inteiro puro em segundos, como a skill o define, ou duração do Go; padrão
  `15`; `0` desliga o periódico, não o de conexão; acima de zero, no mínimo
  `1s`, ou exit 1: um `1ns` girava o laço do beacon).
- **A primeira conexão não perde o beacon.** O gancho é registrado no sink
  **antes** de o `WaitReady` conectar (§10.1 passo 3); o pedido fica guardado (um
  só: pedidos que chegam enquanto um beacon está saindo se fundem no próximo) e
  o beacon sai quando a goroutine começa, no passo 4a. O gancho nunca bloqueia:
  roda na goroutine de *callback* do cliente.
- **Onde:** `<loghub.ctl_namespace>.beacon` (env `LOGHUB_CTL_NAMESPACE`, padrão
  `loghub._ctl`), publicado por NATS **core** (`nc.Publish`) na conexão do sink,
  em fanout, **nunca no JetStream**: o stream de dados não pode cobrir o plano de
  controle, e o boot recusa um que cubra (§8.7.5). O `ctl_namespace` precisa ser
  publicável: tokens separados por ponto, sem espaço, `*`, `>`, `/` ou `\`
  (exit 1).
- **Envelope**, com o mesmo escritor dos envelopes de dados (§12.1): `uuid` novo
  de nível 3 e `event` da mesma amostra, `from` = AgentUUID, `space` =
  Workspace, `type = "beacon"`, `level = 9`, `sid` = PID, `step = 0`, e o
  payload, nesta ordem:

  | Chave | Valor |
  |---|---|
  | `name` | AgentName |
  | `hostname` | Hostname |
  | `machine` | MachineID |
  | `address` | o IP local da rota até o servidor NATS conectado (um `net.Dial("udp", servidor)` sem tráfego, lendo o `LocalAddr`); `127.0.0.1` se não der |
  | `uptime` | milissegundos de vida do **processo** |
  | `meta` | `[{"key":"version","value":<versão>},{"key":"ring","value":"memory"},{"key":"sink","value":"nats"},{"key":"mode","value":"jetstream"\|"core"},{"key":"transports","value":["http1"(,"h2c")]}]` — `h2c` só com `server.http2` |

- Falha de publicação (inclusive "não conectado") é contada
  (`whs_beacons_total{result="error"}`) e logada em `WARN` com amostragem, um a
  cada 30 s; **nunca** afeta a saúde do sink. O beacon é uma cortesia ao plano
  de controle, e perder um não pode parar o plano de dados.
- A goroutine para na fase 5 do shutdown (§10.3), antes de o sink fechar.
- Não responde a sonar, não emite nem responde `ping`/`pong`, não assina nada:
  lacuna documentada até existir um driver (§18).

#### 8.7.11 `LOGHUB_EB_URL`

`loghub.eb_url` (env `LOGHUB_EB_URL`) é a URL do eventbus da convenção LogHub.
Aqui o único eventbus aceito é o NATS **sem TLS**: a URL preenche
`sink.nats.urls`.

| Esquema | Efeito |
|---|---|
| `nats://` (uma lista separada por vírgula é o cluster do `nats.go`) | preenche `sink.nats.urls` com a URL inteira, usuário e senha (ou token) inclusive, que o cliente lê dali |
| `tls://` | **exit 1**, com uma mensagem que nomeia o TLS: este servidor não fala TLS com o NATS; use `nats://` numa rede confiável |
| outro (`postgres://`, `redis://`, `amqp://`...) | exit 1: não suportado. Uma URL de outro eventbus esquecida de uma implantação irmã derruba o boot em vez de ser ignorada |

Numa lista, **toda** entrada precisa ser `nats://`, com host e sem caminho,
consulta ou fragmento (as demais opções são as chaves de `[sink.nats]`); um `#`
ou um `?` **vazio** também conta — a URL NATS não tem nenhum dos dois, e o
caractere é o resto de um valor cortado. Um `@` depois do primeiro `/`, `?` ou
`#` é exit 1 com a dica do escape (`%2F`, `%3F`, `%23`): é uma senha que um
desses caracteres sem escape cortou (`nats://app:39182#Zsecret9@nats-1` discaria
`app:39182` sem senha); um host codificado (`%41`) é exit 1 dizendo isso, em vez
de "escape inválido", que mandava o operador olhar a senha. Uma
`LOGHUB_EB_URL` **só com brancos** é exit 1: definida, mas sem nada visível, o
operador que quis desligá-la não saberia que as chaves de `[sink.nats]` é que
valem (porte da família syslog/RELP, 2026-10-13; antes, só brancos valia como
"não definida"). Vazia continua "não definida".

**Uma fonte só da verdade:** com `LOGHUB_EB_URL` preenchida, qualquer chave que a
URL **pode** expressar definida explicitamente **com valor** (flag, env ou INI) é
exit 1 com as duas fontes na mensagem, mesmo que esta URL não traga aquela parte (uma URL sem
senha quer dizer "sem senha", não "procure em outro lugar"). As chaves donas:
`sink.nats.urls`, `username`, `password` e `token`. As demais (`credentials`,
`mode`, `stream`) convivem com a URL. Uma chave dona definida **vazia** não
compete: não diz nada que a URL possa contradizer, e é o que um compose produz
para uma variável que repassa e o operador não preencheu (`KEY: ${KEY:-}`). Nenhuma mensagem de erro cita a URL (ela
pode ter senha): no máximo o esquema, e só quando ele tem forma de esquema. O
`--dump-config` mostra os valores derivados como comentário, com o *userinfo*
redigido, para que o dump relido não dispare o conflito.

### 8.8 `sink.type = clickhouse` — retirado

Não existe neste servidor (§0).

---

## 9. Arquivo de emergência

**Propriedade do ring, não do sink.** Dispara em estado **FATAL** do sink ou com
`shutdown.drain_timeout` estourado, com o ring (ou um lote retido por um worker,
§10.4) não-vazio. Com `emergency.enabled = false` não há dump, e o que sobrou é
perda contada (§9.3).

**Escrever depende da chave; ler é de todo boot.** A reingestão (§9.2) e o
informe dos temporários (§9.1) rodam com `emergency.enabled` ligado ou não: um
dump é de uma vida anterior, não da configuração desta. Até 2026-10-13 o leitor
só existia junto do escritor, e uma vida com a chave desligada deixava o dump
parado no disco, sem uma linha de log, com `whs_emergency_files` em 0 e
`/readyz` 200 — eventos respondidos com `200` que nunca chegavam ao stream
(§18.1 item 4; a regra do `go-webhook-fast-server`, portada na rodada da família
syslog/RELP).

### 9.1 Formato: JSON Lines

`<emergency.dir>/emg-<agent_uuid>-<unixnano>.jsonl`

```
<JLH>\n
<JLH>\n
...
```

Um envelope por linha, cada linha terminada por um `\n` (`0x0A`), sem mais nada:
o arquivo é [JSON Lines](https://jsonlines.org/) e qualquer ferramenta de linha
(`jq -c`, `wc -l`, `grep`, `nats pub`) o lê. O JLH já é um documento JSON numa
linha só: o serializador (§12.1) não emite espaço entre tokens, e todo byte de
controle dentro de uma string — `\n` inclusive — sai escapado. Os bytes de cada
linha são **exatamente** os que o cliente viu confirmados: a reingestão publica o
mesmo `uuid`, com o mesmo `Nats-Msg-Id`, e o stream deduplica por ele dentro da
`duplicate_window` (§8.7.3).

**O formato é o do `go-webhook-pg-server`, byte a byte**: um dump dele, movido
para o `emergency.dir` desta instância, é reingerido aqui sem conversão, e
vice-versa.

**Escrito sob um nome temporário** (correção da revisão do
`go-syslog-nats-server`): `.emg-<agent_uuid>-<unixnano>.jsonl.tmp`, oculto e
fora do padrão que a reingestão lista, com `O_CREATE|O_EXCL`; depois `fsync` do
arquivo, `link` para o nome final (também exclusivo: um nome que já existe é
erro, nunca sobrescrita), remoção do temporário e `fsync` do diretório. Qualquer
falha antes do nome final remove o que foi escrito e é perda contada (§9.3).
**Sem *hard link*** (vfat, exFAT, montagens FUSE de *object store*), o `link`
falha qualquer que seja o nome, e todo dump falhava ali — o ring inteiro
perdido onde a escrita no lugar funcionava; então, quando o `link` falha por
outro motivo que não "o nome existe", o nome final é conferido livre e o
temporário é **renomeado** para ele (o carimbo de nanossegundos no nome é o que
torna inofensiva a janela entre os dois). Um nome final ocupado continua
recusado, nos dois caminhos. No P o dump era escrito
direto no nome final: um processo morto no meio da escrita (`kill -9`, OOM)
deixava um arquivo que, cortado numa fronteira de linha, **lia limpo** — e era
removido depois de entregue em parte. Um temporário que sobrou de um crash nunca
é lido pela reingestão. No boot (§10.1 passo 4), um temporário que é o
**mesmo arquivo** que o dump do nome final é o gêmeo de um crash entre o `link`
e a remoção, e é removido (o dump é o que a reingestão lê); qualquer outro é um
dump interrompido: fica no disco, nunca é lido, e o boot o informa num `WARN`
com os caminhos — antes, nada dizia ao operador que ele existia. Os eventos
dele só foram contados como perda se um **segundo sinal** abortou o dump
(§10.3); depois de um `SIGKILL` (o `stop_grace_period` estourado), de um OOM ou
de uma queda de energia, ninguém os contou, e o arquivo pode estar inteiro (até
2026-10-11 o `WARN` e esta seção diziam "contados como perda" em todo caso, o
que só valia para o aborto). Para reingeri-lo, o operador o renomeia para o nome
final (sem o ponto inicial e sem o `.tmp`): as linhas íntegras são publicadas no
boot seguinte, uma última linha cortada é pulada e o arquivo vira `.corrupt`
(§9.2), e as já entregues são duplicatas que o stream absorve dentro da
`duplicate_window` — fora dela, o consumidor deduplica por `(uuid, step)`
(§8.7.3). Um temporário cortado numa fronteira de linha lê limpo — o que faltar
não aparece em lugar nenhum —, e por isso a decisão é do operador, não da
reingestão.
`O_EXCL` porque um dump nunca acrescenta a outro: dois desligamentos no mesmo
nanossegundo de outra instância não podem misturar arquivos. O `fsync` do
diretório é melhor esforço: um sistema de arquivos que o recusa, ou um diretório
que se escreve mas não se lê (modo `0300`, que nem abre para o `fsync`), é
tolerado — o arquivo já está completo, sincronizado e no nome final, e
descartá-lo por isso perderia o dump (antes, a falha ao abrir o diretório
removia o dump pronto e relatava o nome temporário).

**Defesa do enquadramento.** Um JLH com um `0x0A` cru partiria a linha em duas,
ambas inválidas. O serializador não o produz (`FuzzEnvelope` e `FuzzServeHTTP`
conferem que nenhum envelope tem `0x0A`), mas o escritor do dump confere de
novo, item a item: um envelope com `0x0A` fica **fora** do arquivo e é contado
como perda (`emergency_failed`, §9.3), e os demais são gravados. Gravá-lo
estragaria a linha e a seguinte; recusar o dump inteiro perderia todos.

**Por que JSON Lines** (decisão 2 do P): o formato do fast era um cabeçalho
decimal com o comprimento e os bytes crus em seguida — compacto, mas opaco para
qualquer ferramenta e frágil, porque um comprimento corrompido tira o leitor de
sincronia e tudo que vem depois vira resto não lido. Em JSON Lines o `\n` é o
enquadramento: uma linha danificada não contamina a seguinte (§9.2), e o
operador inspeciona e reenvia um dump com as ferramentas que já usa.

### 9.2 Reingestão

No boot, todo arquivo `emg-*.jsonl` de `emergency.dir` é publicado **direto pelo
sink**, em lotes de `sink.batch` e no máximo `sink.batch_max_bytes` de JLH (um
envelope maior que o teto sozinho, §8.1 passo 2), sem passar pelo ring — que
ainda não serve e
não deve encher com o backlog que existe para absorver —, **antes** de a porta
pública abrir (§10.1), em ordem do sufixo `unixnano` (temporal), e **removido só
após todos os seus itens serem confirmados** (em `jetstream`, pelo `PubAck`).

**A porta.** Uma linha é reingerida **sem conversão**: o subject, o
`Nats-Msg-Id` e os `X-LogHub-*` saem do cabeçalho da própria linha (§3.8), e o
NATS não lê o `Data`. Uma linha que passasse sem ser um JLH deste servidor seria
publicada e **confirmada como legítima** — no P, a fila recusava parte disso pelas
CHECKs e pelo `jsonb`; aqui não há quem recuse. Por isso cada linha precisa ser
um registro **que este servidor escreveria**:

- o cabeçalho conferido pelo leitor estrito (`envelope.ParseHead`, §12.1): as
  nove chaves na ordem e na forma do escritor, `uuid` e `from` UUIDv7
  minúsculos, `space` e `type` nas gramáticas (o `space` vira tokens do
  subject), números nos intervalos, `event.iso` igual a `uts` + `sub`;
- `type = webhook`, `level = 9` e `step = 0`, os valores fixos deste servidor
  (§3.1), e o payload começando por `{"mode":"fast",`, como o escritor o abre
  (§3.2). Um dump do `go-syslog-*` posto no diretório por engano tem o mesmo
  nome de arquivo e é dano, não evento;
- o JSON inteiro validado (`json.Valid`: o leitor só varre as chaves do payload,
  e `{]` passaria) e UTF-8 válido (o `json.Valid` não confere);
- o payload na **forma** que o escritor lhe dá (revisão independente deste
  módulo): as chaves de topo exatamente `mode`, `network`, `method`, `schema`,
  `host`, `path`, `url`, `headers`, `params`, `query`, `content`, nessa ordem;
  nenhuma chave repetida em objeto nenhum; aninhamento até 6 níveis abaixo do
  payload; números só inteiros não negativos de até 20 dígitos; nada de `true`,
  `false` ou `null`; e só os escapes que o serializador escreve — `\"`, `\\`,
  `\n`, `\r`, `\t`, `\u00XX` minúsculo para os demais bytes de controle,
  `\u2028` e `\u2029` (§12.1), que por isso nunca aparecem crus: um
  U+2028/U+2029 cru numa string é linha **danificada** (até 2026-10-14 o leitor
  de string o aceitava, e a mesma chave escrita nas duas grafias passava pela
  conferência de chave repetida, que é por bytes; §18.1 item 10) —, o que
  também torna o escape canônico: duas chaves são a mesma exatamente quando os
  bytes são. O `json.Valid` aceitava um `"mode"` repetido (que um leitor
  *last-wins* lê como outro valor), um `1e999999` (veneno para `jsonb` e
  decodificadores de `float`), 9000 níveis de colchetes (que derrubam um leitor
  recursivo) e `\u0041`. A forma de cada objeto aninhado (as chaves de
  `network` ou de `content`) não é conferida: ela não envenena leitor nenhum;
- o `uuid` com o instante do `event`, ao nanossegundo: o escritor tira os dois
  da mesma amostra do relógio (§3.1, invariante 14).

Uma linha é **danificada** quando:

- falha uma dessas regras (uma linha com `\r\n` no fim, de um editor de
  Windows, também: o JLH não termina em `\r`);
- é a última e não termina em `\n` (o arquivo foi cortado no meio da escrita);
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
é seguro dentro da `duplicate_window` (as linhas já entregues são duplicatas que
o stream absorve, §8.7.3); fora dela, o consumidor deduplica por `(uuid, step)`.

**A remoção e a quarentena fazem `fsync` do diretório** (porte da família
syslog/RELP, 2026-10-13). Sem ele, uma queda de energia logo depois da
reingestão podia trazer o nome de volta, e o boot seguinte entregava o dump
inteiro de novo — absorvido pelo stream só dentro da `duplicate_window` (em
`core`, nunca). O `fsync` vem **depois** do `unlink` ou do `rename`,
então a falha dele (inclusive a de **abrir** o diretório, num modo `0300`) não
desfaz nada: o arquivo já saiu, ou já é `.corrupt`. O boot a registra num `WARN`
que diz o que aconteceu e o risco de uma segunda entrega, em vez do `ERROR`
"cannot remove" — falso, com o arquivo já removido — ou do "cannot quarantine"
com um caminho que não existia mais, que ainda engolia o `ERROR` com as linhas
puladas. Só a escrita do dump mantém o `fsync` do diretório como melhor esforço
(§9.1).

**Um `emergency.dir` que não se lista** encerra o boot com 1 só quando esta
vida escreve dumps (`emergency.enabled = true`), porque o dump precisará dele;
com a chave desligada vira um `ERROR` nomeando o diretório, e o boot segue sem
reingerir (a regra do fast).

**Os *gauges*** `whs_emergency_files` e `whs_emergency_events_pending` dizem o
que está no disco (§11.1): durante a reingestão, `whs_emergency_files` é o
número de dumps listados (todos esperam a reingestão), posto antes do estado de
boot que a anuncia; no fim, os dois são recontados do diretório — um arquivo que
não se lê conta como arquivo, sem eventos (até 2026-10-13 ele ficava fora,
§18.1 item 5) — e o dump do shutdown soma o seu, sem reler o diretório.

**Linha recusada pelo destino.** Uma recusa `logic` de uma mensagem na
reingestão (tamanho, §8.7.6) recusa só aquela mensagem (entrega parcial,
§8.7.2): as demais do lote são entregues, a recusada fica no arquivo, contada
num `ERROR` com o `uuid`, e o arquivo vira `.corrupt` como acima. O boot segue.
Uma recusa de outra classe (rede, fatal — um subject de outro Workspace que
nenhum stream cobre, §8.7.5, inclusive) mantém o arquivo e encerra o boot com 3
(em `core` não há stream a perguntar: o dump de outro Workspace é publicado às
cegas, com um `WARN` por subject, §8.7.5);
um `SIGTERM` no meio da reingestão encerra com 130 e também mantém o arquivo.

**O recusado fica no `.corrupt`, fora dos *gauges*** (revisão independente de
2026-10-14, só documentado). A linha recusada como `logic` na reingestão fica
no arquivo, que vai para `.corrupt` junto com as já entregues; ela não entra
em `whs_events_dropped_total` (não se perdeu) nem em
`whs_emergency_events_pending` (o `.corrupt` não é listado como dump), e o
único rastro é o `ERROR` com o `uuid`. Reprocessar o arquivo à mão
(renomeá-lo para o nome de dump) entrega de novo todas as outras linhas, que um
destino que deduplica por `uuid` absorve; para reaproveitar só a recusada, o
operador a extrai pelo `uuid` do log.

**Memória.** Um arquivo é lido inteiro antes de publicado. O JLH é o que o ring
guarda, então um dump de uma vida cabe no que o ring dela ocupava
(`ring.max_bytes`); quem sobe com um `ring.max_bytes` muito menor que o da vida
que escreveu o dump precisa da memória da antiga no boot. A leitura em fluxo do
`go-syslog-nats-server` resolve um problema de lá (o JLH chega a 12× o cru que o
ring guardava) que não existe aqui.

**Só arquivos regulares** são dumps: um FIFO com nome de dump bloqueava o
`open` para sempre, e o boot com ele, sem atender `SIGTERM`; um *symlink* podia
apontar para `/dev/zero`.

**Dumps de outros formatos.** Um `emg-*.bin` — o dump binário do
`go-webhook-fast-server` — **não é lido**: o boot registra um `WARN` com a
contagem, dizendo para reingeri-lo com o fast, e não toca no arquivo.

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
1. parse da config (§13); LOGHUB_EB_URL expandida e conferida aqui
   (§8.7.11); toda URL que não seja nats:// é recusada; erro, chave
   desconhecida ou valor de seletor retirado (§0) => exit 1
   --help, --version e --dump-config encerram aqui, sem tocar o disco
1a. IDENTIDADE (§3.5): cria ring.data_dir, exporta-o como DATADIR e chama
   lhident.Initialize() UMA vez (só no main). Falha => a biblioteca sai
   sozinha com 100..114 (§10.5); from e space são validados (exit 1)
2. abre a porta ADMIN imediatamente
      /healthz -> 200 (processo vivo)
      /readyz  -> 503 com o motivo em texto
      /metrics -> já exportando
3. inicializa o SINK
      cliente NATS preguiçoso (configuração inválida, como um .creds
      inexistente => exit 1); o gancho do beacon é registrado ANTES da
      primeira conexão (§8.7.10); a primeira checagem de saúde conecta e,
      em jetstream, valida o stream e a maior mensagem (§8.7.5); em core,
      só o max_payload.
      Retry com backoff 100ms -> 5s até sink.boot_timeout (300s); o
      backoff dorme no máximo o que falta do prazo (W 39, revisão
      independente de 2026-10-14: dormia o backoff inteiro e ainda fazia
      mais uma checagem depois do prazo);
      estourou => exit 3; erro FATAL (stream, credencial, servidor que
      exige TLS, permissão negada) => exit 3 IMEDIATO, sem esperar
      sink.boot_timeout
4. se existem arquivos emg-*.jsonl: publica direto pelo sink (§9.2), com
   emergency.enabled ligado ou não (§9)
   (um emg-*.bin do fast => WARN, intocado, §9.2; o temporário gêmeo de
   um dump é removido, o de um dump interrompido => WARN, intocado, §9.1)
4a. o beacon começa, e o da primeira conexão (pedido no passo 3) sai
   agora (§8.7.10); a saúde e os workers começam
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
| `ring.mode` ≠ `memory`, `sink.type` ≠ `nats`, `ratelimit.enabled = true`, `sink.nats.tls = true`, `sink.nats.subject` ≠ `loghub.{space}.{type}` | `exit 1`: recurso que não existe aqui (§0) |
| Chave de recurso retirado (`[ring.sqlite]`, `[sink.postgres]`, `sink.nats.tls_ca`...) num INI ou numa flag | `exit 1`: chave desconhecida (§0); no ambiente, inerte |
| `sink.nats.urls` vazia, ou uma entrada que não seja `nats://` com host (`tls://`, `ws://`, `wss://`, `host:porta` sem esquema) | `exit 1` (§8.7.1) |
| Uma entrada de `sink.nats.urls` que não parseia (um `%`, `/`, `#`, `?` ou `,` sem escape na senha) ou que traz caminho, consulta ou fragmento | `exit 1`, sem citar nada da URL (§8.7.9) |
| *Userinfo* numa URL junto com `username`, `token` ou `credentials`; mais de um método de autenticação; `password` sem `username` | `exit 1` (§8.7.9) |
| `sink.nats.mode` fora de `jetstream`, `core` | `exit 1` |
| `sink.nats.mode = core` | Permitido, com `WARN`: *best effort* (§8.7.2) |
| `sink.nats.stream` que não é um nome de stream (espaço, `.`, `*`, `>`, `/`, `\`) em `jetstream` | `exit 1` |
| `loghub.eb_url` com esquema que não seja `nats://`, ou junto de `sink.nats.urls`/`username`/`password`/`token` explícitos e não vazios | `exit 1` (§8.7.11) |
| `loghub.beacon_interval` entre 0 e 1 s (exclusive) | `exit 1`: `0` desliga o periódico (§8.7.10) |
| `loghub.ctl_namespace` vazio ou com token não publicável | `exit 1` (§8.7.10) |
| `admin.enabled = true` com `admin.bind` vazio (o `net.Listen` ouviria em toda interface, numa porta aleatória) ou que não seja `host:porta` com porta numérica de 1 a 65535 (`127.0.0.1:`, `:` e `localhost:abc` incluídos: o `SplitHostPort` aceita porta vazia, e o `Listen` sortearia uma; W 40, revisão independente de 2026-10-14) | `exit 1` |
| `ring.high_water` fora de (0, 1] ou `log.access_sample` fora de [0, 1], `NaN` inclusive (um `NaN` nunca satura o ring e desligava o portão do `/readyz`) | `exit 1` |
| Linha de INI sem `=` ou cabeçalho sem `]`; argumento sem `--` | `exit 1` com o número da linha ou o nome antes do `=`, nunca o valor (`password: x` é exatamente a linha que cai aqui) |
| **Tetos** (porte da família syslog/RELP, 2026-10-13): `ring.capacity_items` > 16 777 216; `sink.batch` > 1 048 576; `sink.workers` > 256; `server.listeners` > 256 | `exit 1`, nomeando a chave. Sem eles um erro de digitação de poucos dígitos derrubava o boot com pânico (`makechan` do ring de memória) ou alocava um gigabyte por `Dequeue` (`sink.batch`). O teto de `sink.workers` é o do `go-syslog-nats-server` (DECISOES §2.2 de lá): todo worker publica na **uma** conexão NATS, e `workers × batch` é o `PublishAsyncMaxPending` do cliente |
| `loghub.eb_url` só com brancos; com `@` depois do primeiro `/`, `?` ou `#`; com host codificado (`%41`); com `#` ou `?` mesmo vazios | `exit 1`, sem citar a URL (§8.7.11) |
| `--help`, `--version` ou `--dump-config` com `=valor`; `--config=` vazio | `exit 1`, sem o valor (§13.2) |
| Nome de parâmetro desconhecido sem forma de nome, ou que começa com o nome de um parâmetro e continua; chave desconhecida do INI fora de uma seção conhecida, sem forma de chave ou logo depois de um segredo; flag desconhecida fora das seções conhecidas logo depois de uma flag com valor | `exit 1`, nomeando só a linha ou a flag anterior, **nunca o conteúdo** (§13.2) |
| Flag sem valor seguida de outra flag (`--sink.workers --sink.nats.password=x`, um `$WORKERS` vazio num script); valor depois de um espaço (`--sink.nats.password= x`, `--sink.nats.password = x`, `--sink.nats.password= -x`); valor que o shell partiu num espaço (`--sink.nats.password=$P`, com espaço em `$P`); valor que começa com `--` na forma separada (`--sink.nats.password --x`) | `exit 1`, nunca com o valor: um argumento que começa com `--` (ou `-` com o nome de um parâmetro ou de um *switch*) nunca é tomado como valor de outra flag, e só é nomeado se for uma flag deste servidor; o argumento depois de um valor vazio ou de um `=` sozinho não é citado, nem como nome de flag desconhecida; uma palavra solta depois de qualquer valor não é citada. Um valor que começa com `-` vai depois do `=` (§13.2; porte de volta e sua revisão, 2026-10-11) |
| `sink.batch_max_bytes` abaixo de 1 ou acima de 256 MiB | `exit 1`, nomeando a chave (§8.1 passo 2). 1 é coerente com a regra (cada evento vai sozinho) e por isso é o mínimo; acima de 256 MiB nenhum destino escreve o pedaço dentro de `sink.op_timeout` (o teto é o da família webhook) |
| `GOMEMLIMIT` ausente ou `< ring.max_bytes * 2` | Permitido, com `WARN` (§12.4) |

### 10.2 Saúde do sink

Máquina de estados dirigida **exclusivamente pelo `Health()`** do sink.

```
UP ---- Health() falha (timeout sink.health_timeout) ----> DOWN
DOWN --- Health() sucesso -------------------------------> UP
DOWN --- persiste por sink.fail_timeout (30s)
         OU sink.max_retries (10) checagens falhas ------> FATAL
qualquer --- Health() ou Write() retorna ErrFatal -------> FATAL
```

- `Health()` a cada `sink.health_interval` (padrão 2s). **Latência de detecção =
  até um intervalo de health check.**
- Um `Health()` que devolve `ErrFatal` (o destino respondeu, e a resposta é
  permanente: §8.7.5, §8.7.6) é FATAL **na mesma checagem**, sem passar pelos
  fusíveis: eles existem para um destino inalcançável, não para um que
  respondeu "nunca".
- Erros de `Write()` classificados como `ErrNetwork` **não** mudam o estado: o
  lote continua com o worker e é retentado (§10.4). Ninguém recebeu `200` por um
  evento perdido.
- `ErrLogic` → log `ERROR`, e as mensagens recusadas descartadas e contadas
  (`whs_events_dropped_total{reason="sink_logic"}`); com `Delivered` (§8.1), as
  entregues contam como escritas.
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
fase 5  para o beacon, fecha o sink, exit 0
```

**Erro no `accept` e listener parado.** Um erro do `accept` que não seja o
fechamento do próprio listener (`ENOBUFS`, `ENOMEM`, `EPERM` de uma regra de
firewall, `EPROTO`, e também os temporários `EMFILE`/`ENFILE`) é **retentado
no listener**, com pausa crescente de 5 ms a 1 s e um `WARN` amostrado (um a
cada 10 s, com a contagem dos suprimidos); o listener nunca desiste, como o
laço de `accept` da família syslog/RELP. Só o fechamento pelo próprio servidor
(`net.ErrClosed`) encerra o `accept`. Se ainda assim o `Serve` de **um**
listener terminar com erro, o processo não espera os outros (com
`server.listeners > 1`, um listener morto ficava invisível até todos
morrerem): sai pelo shutdown gracioso, como num `SIGTERM` (as cinco fases
e o dump se o destino não der conta), e termina com `exit 1` se o shutdown
foi completo, ou com o código que o shutdown der (`3`, `130`). Até 2026-10-14
esse caminho saía com `1` na hora, sem dreno nem dump: os eventos já
respondidos com `200` sumiam sem contagem. O `1` do shutdown completo é padrão
assumido, contestável (`docs/DECISOES.md`, porte de W e F de 2026-10-14): o
código de saída próprio que a pendência sugeria não foi criado, porque o
evento já foi entregue e o que falhou foi a configuração do host (porte do
`go-webhook-server`, W 5).
Como no shutdown por FATAL, **nenhum sinal o iniciou**: o primeiro `SIGTERM`
ou `SIGINT` que chega durante ele — o do orquestrador que viu o `/readyz`
falhar — é tolerado com um `WARN`, e só um segundo aborta. Na primeira versão
da correção ele contava como segundo sinal, e o shutdown que existia para
salvar o ring abortava ali: com o destino retido, os eventos respondidos com
`200` iam para `reason="abort"` em vez do dump (revisão independente de
2026-10-14, W 31). O dreno e o `pre_stop_delay` continuam os de um `SIGTERM`:
tolerar o primeiro sinal é um parâmetro à parte do modo FATAL.

**Segundo sinal** em qualquer fase → pula direto para a saída, aceitando a perda
(contada em `whs_events_dropped_total{reason="abort"}`, inclusive o que já tinha
sido recolhido para um dump ainda não ligado ao nome final, e uma vez só: o que
o aborto conta o dump não conta de novo), `exit 130`. No shutdown **por FATAL**
nenhum sinal o iniciou: o primeiro `SIGTERM` ou `SIGINT` que chega — o comum de
um orquestrador — é o primeiro, registra um `WARN` e o dump continua; só um
segundo aborta. O mesmo vale para o shutdown que começa porque o listener
público parou com erro (acima). No fast, esse primeiro sinal jogava o dump fora (revisão
independente de 2026-10-07).
**`SIGQUIT`** durante o shutdown aborta como o segundo sinal e nunca é
tolerado, nem como primeiro sinal do shutdown por FATAL; fora dele vale a ação
padrão do Go (despejo das goroutines e saída `2`), que é o que um operador quer
ao mandá-lo para um processo travado. A frase do fast ("sempre aborta
imediatamente") prometia mais do que o código fazia. O observador do segundo
sinal é registrado **antes** de o contexto do primeiro soltar
`SIGTERM`/`SIGINT`: no meio, um segundo sinal recebia a ação padrão do Go e
matava o processo com 143, sem contar nada (porte da família syslog/RELP,
2026-10-13).

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
  aborto antes da primeira linha, a cada 1024 e antes do `link`, e remove o
  temporário em vez de publicar um arquivo que o boot seguinte reingeriria. Um
  dump que publica entre a última conferência e o aborto fica contado **e**
  guardado — contagem a mais, nunca perda —, e nada mais é contado. Um
  temporário ainda pode sobrar quando o `os.Exit` corta a escrita entre duas
  conferências (§9.1): os eventos dele são a perda que o aborto contou;
- com `emergency.enabled = false`, a perda continua sendo do aborto (§9.3), e
  conta uma vez, por quem tomar o contador; o envelope deixado fora pela defesa
  do enquadramento (§9.1) é contado pelo próprio dump, em `emergency_failed`.

`shutdown.pre_stop_delay` existe para Kubernetes: a remoção do endpoint no LB é
assíncrona e o pod continua recebendo tráfego **depois** do `SIGTERM`. Sem a
espera, o emissor recebe conexão recusada — um erro de rede, não um `503`
retentável. Valor sugerido em K8s: 5–10s.

Se o `drain_timeout` estourar com itens ainda no ring, executa §9 e sai com
código 3. Um sink que vira **FATAL durante a fase 4** encerra a espera na hora,
com um `ERROR` — nada mais escoa, e esperar o resto do prazo só adiaria o dump,
talvez para depois do `SIGKILL` do orquestrador —, e a saída é 3 sempre que a
saúde termina FATAL, mesmo com o ring vazio (§10.2, §10.5; até 2026-10-13,
§18.1 item 6). **Um prazo, não dois:** cada fase tinha o seu, e um upload lento (que
segura a fase 3 até o prazo) seguido de um sink DOWN (que segura a fase 4) só
começava o dump em duas vezes o `drain_timeout` — além de um
`stop_grace_period` dimensionado para uma, e o `SIGKILL` levava o ring inteiro,
respondido com `200`, sem nada contar. O pior caso até o dump começar é
`pre_stop_delay + drain_timeout + sink.op_timeout` (o lote em voo de um worker),
e o `stop_grace_period` precisa cobri-lo com folga para o próprio dump.

**O ring é selado antes do último `Drain`** (achado pela revisão independente
do porte para o P, o fast e o W, 2026-10-11; aqui desde o porte de volta do
mesmo dia). Um upload que ainda lia o corpo quando a fase 3 estourou continua
depois dela; se o corpo chegasse depois do `Drain`, o item entrava num ring que
ninguém mais lia e o cliente recebia `200` — fora do stream e do dump, sem
contagem, e o processo saía com 0. O prazo único das fases 3 e 4 tirou a folga
que antes o pegava. Agora o ring de memória é selado (`Seal`, uma barreira: o
`Enqueue` segura um `RWMutex` compartilhado da checagem ao envio, e o `Seal` o
pega exclusivo) logo antes do `Drain`, e o que chegar depois recebe
`503 shutdown`.

**`SIGHUP`** é ignorado em toda fase, com `WARN` (§0, §13.3).

### 10.4 Falha de lote

O worker **segura o lote no próprio buffer** — o pedaço em retentativa e os
que ainda não foram escritos (§8.1 passo 3) — e retenta com backoff
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
| `1` | Erro de configuração ou falha de inicialização não relacionada ao sink; ou o listener público parou com erro e o shutdown que se seguiu foi completo (§10.3) |
| `3` | Sink indisponível (FATAL, `boot_timeout`, ou `drain_timeout` estourado) |
| `100`–`114` | Identidade: o `go-loghub-ident` sai sozinho (`DATADIR` inválido ou sem escrita, `AGENT_UUID` que não é UUIDv7, `HOSTNAME` fora da RFC 1123, `WORKSPACE` com `_`...). A tabela completa está na skill `go-loghub-ident` §8 |
| `130` | Abortado por segundo sinal, ou boot interrompido por sinal: na espera do sink, na reingestão e no `bind` da porta admin ou da pública. Um nome de host no `bind` falha com "operation was canceled" e saía com 1, como um endereço errado; um IP literal nem olha o sinal, e o boot abria a porta, respondia `/readyz` 200 e saía com 0 pelo caminho gracioso. Agora o boot confere o sinal depois do último `bind` e antes de servir (porte da família syslog/RELP, 2026-10-13) |

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

Os nomes, labels e fronteiras são os do `go-webhook-fast-server` (decisão 3 do
P): um painel feito para o fast lê este servidor sem mudança. O que sai são as
séries dos recursos retirados; o que entra é a do beacon.

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
whs_config_info{ring_mode,sink_type}        gauge (=1)  sempre memory, nats
whs_build_info{version,commit,go}           gauge (=1)

whs_sink_nats_duplicates_total              counter     (§8.7.8)
whs_sink_nats_reconnects_total              counter
whs_beacons_total{result}                   counter     (§8.7.10)
    result: ok|error
```

Saem `whs_ratelimit_buckets`, `reason="ratelimit"` e as séries de
`sink=sqlite`, `postgres`, `mysql` e `clickhouse`. O `code="500"` de
`whs_requests_total` fica, sempre em zero: não há caminho que responda `500`
(§4.4), e a série existe para que um painel do fast continue achando-a.

`whs_sink_errors_total{kind="timeout"}` conta a escrita que passou de
`sink.op_timeout` e a checagem que passou de `sink.health_timeout`: as duas
valem como `network` (a escrita é retentada, a checagem conta para os fusíveis
de §10.2), mas são contadas à parte, porque lento e inalcançável são
incidentes diferentes. Uma escrita cortada pelo cancelamento do próprio
shutdown não é falha do sink e não conta.

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

Faixa útil do `nats`: um lote de publicações assíncronas com as N confirmações
do JetStream (o `fsync` do *file store* não está no caminho do `PubAck`, §8.7.4)
vive em 0,5–25 ms numa rede local; a região está densamente coberta. As
fronteiras são as do fast, sem mudança.

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

**Por padrão registra apenas**: boot (o dump da configuração efetiva, redigido,
só sai com `log.level = debug`), mudanças de estado do sink, fases do shutdown,
dump e reingestão de emergência, contagens de descarte, e erros. **Nada por
requisição.**

Access log é opt-in: `log.access = true`, com amostragem `log.access_sample`
(padrão 0.01). Rejeições (`429`/`413`/`503`) também são amostradas, nunca logadas
uma a uma. Os erros assíncronos do cliente NATS (uma permissão negada gera um
por mensagem) e as falhas do beacon também: um registro a cada 30 s, com a
contagem do que foi suprimido (§8.7.1, §8.7.10).

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

O destino lê os campos do cabeçalho que vão para o subject e para os headers da
mensagem NATS (§3.8) **do próprio JLH**, com um
leitor estrito do cabeçalho que este servidor escreve (`envelope.ParseHead`),
e não de uma cópia guardada no item: assim um item que volta de um dump é
indistinguível do que entrou. O leitor não aloca além das três
strings curtas (`from`, `space`, `type`) e recusa qualquer JLH que não tenha
exatamente a forma de §3.1 — `uuid` e `from` UUIDv7 em minúsculas inclusive (o
`uuidv7.FromString` aceita maiúsculas, e a regra genérica do `from` do JLH,
qualquer versão de 1 a 8). Do payload ele só confere que é um objeto que fecha o
envelope; a reingestão de um dump (§9.2) valida o resto.

### 12.2 Corpo e buffer do JLH

`sync.Pool` com **classes por potência de 2**: 4 KiB, 16 KiB, 64 KiB, 256 KiB,
os mesmos pools para o corpo lido e para o JLH (o JLH carrega o corpo, §3.2:
não há um conjunto separado para ele). Acima de 256 KiB o buffer é
alocado fresco com `make` e **nunca entra no pool**: pooling de buffers grandes
faz o pool reter a memória do pior caso para sempre, e o GC do Go já lida bem
com objetos no large object heap.

O corpo vive só entre a leitura e a serialização: a pré-passada (§3.2) diz o
tamanho exato da sua representação, o JLH é pedido ao pool já com essa
capacidade, e o buffer do corpo volta ao pool assim que o JLH é escrito — nada
mais o referencia. Um JLH que cresceu além da capacidade pedida (só se a
estimativa do cabeçalho falhar) deixa de ser do pool e vai para o GC.

O `Content-Length` declarado só **pré-dimensiona** o buffer do corpo até
64 KiB (a classe de 64 KiB, ou a menor que comporte o declarado). Acima disso
o buffer cresce só quando está cheio de bytes **recebidos**, dobrando, e
nunca além do declarado, salvo o arredondamento à classe do pool: até 256 KiB
o buffer pedido vem da classe que o comporta (um corpo de 200 KiB cresce de
64 KiB para a classe de 256 KiB); acima dela o último passo é exatamente o
declarado, então um corpo honesto de 1 MiB termina num buffer de 1 MiB. Nada
é alocado pelo que o par promete: uma requisição que declara 5 MiB e não manda
nada segura no máximo 64 KiB até o `read_timeout`, e não 5 MiB (em h2c, vezes
`http2_max_concurrent_streams` por conexão; com `read_timeout = 0`, até o
`write_timeout` do fluxo, §13.5). O custo é uma cópia por
dobra num corpo grande e honesto, que já é o caso raro (porte do
`go-webhook-server`, W 1).

**O ring de memória cobra o buffer, não o JLH** (§7.2): o item retém o buffer
inteiro da classe, e é `cap(Env)` mais `ring.ItemOverhead` que entra em
`ring.max_bytes` e em `whs_ring_bytes`. As classes são espaçadas 4×, então um
JLH logo acima de uma fronteira custa até 4× o seu tamanho; o JLH **não** é
copiado para uma classe menor depois de escrito (decisão do usuário,
2026-10-14: uma cópia por requisição no caminho quente, e o fator não some).

### 12.3 Regra de propriedade única

> O item do ring é **dono** do buffer do JLH. Ele volta ao pool **apenas**
> depois de o item ser confirmado, e **apenas uma vez**.

Ponto de devolução: o `sink.Write()` do lote retorna sem erro, ou com uma
recusa `logic` (o lote sai do ring, entregue em parte ou não, §8.1). Nos dois
casos o `ring.Commit` vem **antes** do `Release`. Desde 2026-10-14 o ring
desconta o valor que gravou no item no `Enqueue` (`Item.Charged`, §7.2) e não
relê o buffer, então a ordem já não muda a conta; ela continua a regra por ser
a única que serve a todo ring (o `ring=sqlite` do fast desconta `len(Env)`,
que o `Release` esvazia). Na ordem inversa, quando o ring descontava
`it.Size()`, ele continuava contando os bytes de um lote que ninguém mais
tinha, até recusar tudo como cheio com nada dentro.

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
seguinte tem sucesso. O sink NATS contamina ele mesmo, antes de devolver o erro,
o lote de **toda** falha em `jetstream` depois da primeira publicação (§8.7.2):
um `PubAckFuture` ainda pendente aponta para o `Env` do item, e nada prova que o
cliente não o lerá; devolver o buffer arriscaria entregá-lo, já reusado, a quem o
lê. O custo é uma alocação por item num caminho que já é o de falha; o caminho
normal não muda.

Devolver ao pool um buffer ainda referenciado entrega o corpo do webhook de um
cliente dentro do evento de outro. É o pior bug possível neste sistema, é
silencioso, e não aparece em teste de carga. Todo caminho de erro precisa de
teste explícito com `-race`.

### 12.4 Runtime

- **`GOMAXPROCS`: não mexer.** O Go 1.25+ lê o limite de CPU do cgroup
  automaticamente e reavalia dinamicamente. A dependência `automaxprocs` é
  desnecessária.
- **`GOMEMLIMIT`: não define automaticamente**, mas emite `WARN` no boot se
  estiver ausente ou menor que `ring.max_bytes * 2`. O fator 2 só é teto
  porque `ring.max_bytes` mede a memória **retida** pelo ring — buffer inteiro
  da classe mais o custo fixo por item (§7.2, §12.2) —; enquanto ele contava
  `len(Env)`, o ring retinha de 4 a 7× o número com eventos pequenos, e o
  `GOMEMLIMIT` sugerido não o cobria. Definir automaticamente
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
  `WHS_SINK_NATS_URLS=nats://10.0.0.5:4222` —, **exceto** a seção `[loghub]`,
  cujas variáveis são os nomes fixados pela skill: `LOGHUB_EB_URL`,
  `LOGHUB_BEACON_INTERVAL` e `LOGHUB_CTL_NAMESPACE` (não há
  `WHS_LOGHUB_*`). São da convenção LogHub, e o mesmo `.env` alimenta todo
  produtor da família.
- INI: seção `[ring]`, chave `capacity_items`
- `--config=/caminho/arquivo.ini` (env `WHS_CONFIG`) indica o arquivo.

O prefixo `WHS_`, as seções e as chaves são os do fast (decisão 3 do P, §0).

**No compose**, o `.env` só preenche os `${...}` que o compose escreve: o de
`docker/` repassa, com padrão vazio, toda variável que o `docker/.env.example`
ou o `docker/README.md` mandam definir — `LOGHUB_EB_URL`,
`WHS_SINK_NATS_TOKEN` e `WHS_SINK_NATS_CREDENTIALS` inclusive. Uma variável
documentada e não repassada seria configuração ignorada em silêncio; vazia, ela
não compete com nada (§8.7.11).

### 13.2 Registry único

Uma **única tabela** declara, para cada parâmetro: seção, chave, tipo, valor
padrão, descrição e validação. Dela são derivados:

- o parser de INI,
- o registro de flags,
- a leitura de env,
- o `--help`,
- o dump da configuração efetiva no boot (em `debug`, §11.3).

É impossível os cinco divergirem. Sem `gopkg.in/ini.v1` e sem `spf13/pflag`:
unir essas bibliotecas numa precedência coerente daria mais código de cola que o
registry inteiro, e sem o `--help` e o dump gerados da mesma fonte. As cinco
chaves seletoras de §0 estão no registry como qualquer outra; a validação aceita
um valor só. Uma entrada pode fixar o nome da sua variável de ambiente (os
`LOGHUB_*`), e outra pode ser redigida como URL (só o *userinfo*:
`sink.nats.urls`, `loghub.eb_url`) ou inteira (`sink.nats.password`,
`sink.nats.token`).

O dump sem segredos **relê para a mesma configuração**: um valor com espaço nas
pontas ou já entre aspas sai entre aspas duplas, e o INI tira exatamente uma
camada (sem isso, `stream = 'LOGHUB_X'` relido apontava para outro stream).

**Nenhum valor de texto carrega byte de controle** (W 41, revisão
independente de 2026-10-14). Um valor de string ou de lista vindo do ambiente
ou de uma flag com um byte abaixo de `0x20` (fora o tab) ou `0x7f` —
`WHS_SINK_SQLITE_PATH=$'a\nb'` — é `exit 1`, nomeando a chave (a variável ou
a flag) e o byte, **nunca o valor**. O INI não tem como representá-lo: no
`--dump-config` o valor saía em duas linhas, e relido dava "expected 'key =
value'" ou, com o resto na forma `k = v`, injetava uma chave ou uma seção; na
linha comentada de um valor derivado, a segunda linha deixava de ser
comentário. O tab fica, porque o INI o aceita e o dump o relê (entre aspas).

**Erros sem eco.** Os parsers de INI e de flags nomeiam a linha ou a flag, nunca
o valor. Uma linha `password: x` (sem `=`), um cabeçalho sem `]` e um
`sink.nats.password=x` sem os traços já não ecoavam (revisão de 2026-10-10).
Faltavam dois caminhos, achados pela revisão do porte para o SN e portados de
volta em 2026-10-11: uma flag sem valor **engolia a seguinte**
(`--sink.workers --sink.nats.password=x` tomava `--sink.nats.password=x` como o
número de workers e o imprimia no erro de tipo) e um valor digitado **depois de
um espaço** (`--sink.nats.password= x`) virava um argumento solto, citado no
erro. Agora um argumento com forma de flag nunca é valor de outra, e o
argumento solto depois de um valor vazio é recusado sem citação. A revisão do
porte de volta achou mais três caminhos, iguais no P, no fast e no SN: um valor
que começa com `--` na forma separada (`--sink.nats.password --x`) era
"nomeado" como a flag `--x`; um valor com traço depois de um `=` vazio
(`--sink.nats.password= -x`) saía como parâmetro desconhecido; e a cauda de um
valor que o shell partiu num espaço (`--sink.nats.password=$P`) saía como
argumento solto. Agora só se nomeia uma flag que este servidor conhece, nada
depois de um valor vazio é citado, e uma palavra solta depois de qualquer valor
também não. A cauda partida que começa com traço depois de um valor **não**
vazio (`--sink.nats.password=a -b`) era uma brecha deixada de propósito, para
não esconder o erro de digitação de uma flag depois de outra; a regra abaixo a
fecha (2026-10-13).

**Porte da família syslog/RELP (2026-10-13)** — as regras de configuração que
a revisão do `go-relplog-server`, do `go-relplog-pg-server` e do
`go-relplog-nats-server` firmou, e que o `go-syslog-pg-server` e o
`go-syslog-nats-server` já seguem:

- **A cauda depois de um valor é nomeada só dentro de uma seção conhecida.**
  Depois de uma flag com valor, uma flag desconhecida pode ser a cauda desse
  valor que o shell partiu num espaço (`--sink.nats.password=abc -hunter2`,
  `... abc --x=y`): ela só é nomeada quando o nome, lido como
  `<seção>.<chave>`, cai numa seção deste servidor **ou numa das seções do
  `go-webhook-fast-server` que este não tem** (`[ring.sqlite]`, `[sink.redis]`...:
  uma configuração trazida do fast é exatamente de onde ela vem, e o erro a
  nomeia); senão o erro culpa a flag anterior ("a value with a space goes in
  quotes"), sem citar nada. Sem valor antes, toda flag desconhecida continua
  nomeada. Era a brecha acima, fechada sem esconder os erros de digitação que
  importam (`--sink.wrkers=1` depois de `--sink.workers=4` aparece).
- **Um `-h` ou `-v` depois de uma flag que espera valor** só é nomeado como o
  *switch* quando escrito com dois traços: `--sink.nats.password -h` é também como
  fica uma senha de dois caracteres que começa com traço.
- **Valor colado ao nome.** Um nome desconhecido que começa com o nome de um
  parâmetro e continua (`--sink.nats.passwordhunter2`, com ou sem `-`, `_`, `.` no
  meio; o `=` esquecido) não é citado, nem o que tem caracteres que nenhum nome
  tem (`--sink.nats.password:hunter2`). O mesmo vale para a chave do INI
  (`password: hunter2 = x`) e para a chave antes de qualquer seção.
- **Continuação de segredo no INI.** Uma chave desconhecida do INI só é
  nomeada quando a seção é conhecida (como acima), a chave tem forma de chave
  (`[a-z0-9_]{1,64}`) **e** a linha anterior não era um segredo (senha, token ou URL com credencial):
  `password = eyJhbGciOiJI` seguido de `UzI1NiJ9abcDEF==` é um valor que quebrou
  de linha, e o resto dele virava o nome de uma chave desconhecida no stderr.
  Um erro de digitação (`[sink.nats]` `strem = x`) continua nomeado, com a linha.
- ***Switches* não levam valor.** `--help`, `-h`, `--version`, `-v` e
  `--dump-config` com `=valor` são exit 1, sem o valor: `--dump-config=false`
  despejava a configuração e saía com 0, o contrário do que diz.
- **`--config=` vazio é exit 1**, em vez de cair no `WHS_CONFIG` (ou no padrão),
  que é o contrário do que a flag diz.
- **BOM UTF-8 na linha 1 do INI** é ignorado: alguns editores o escrevem, e a
  linha virava "expected 'key = value'" sem nada visível que explicasse.
- **Valores derivados saem comentados no `--dump-config`**: `emergency.dir` (derivado de `ring.data_dir`), quando é o servidor quem o deriva, aparece como
  `# chave = valor` sob "`# derived`". Escritos como chave, o dump relido os
  fixava: o diretório de emergência deixava de seguir `ring.data_dir` — para fora da única área gravável
  (§13.6). Um valor explícito continua chave.

### 13.3 Recarga por `SIGHUP` — retirada

Decisão 1 do P (2026-10-07). A configuração é lida **uma vez**, no boot; mudar
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
workers = 8
batch = 256

[sink.nats]
urls = nats://10.0.0.5:4222,nats://10.0.0.6:4222
username = webhook
password = segredo
mode = jetstream
stream = LOGHUB_WEBHOOK

[loghub]
beacon_interval = 15

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
`sink.max_retries`), `loghub.beacon_interval` (sem beacon periódico) e os
timeouts do `net/http` em `[server]` (sem limite).

**Números não finitos.** `NaN`, `Inf` e `-Inf` (em qualquer caixa) são recusados
pelo parser de todo número com fração (`ring.high_water`, `log.access_sample`),
com `exit 1` no boot e o erro nomeando a linha do INI ou a flag; as faixas
continuam na forma "não está dentro" (`!(x > 0 && x <= 1)`), que um `NaN`
também não passa (defesa dupla, 2026-10-14: o parser do `go-webhook-server`,
W 24, que veio daqui na forma da validação).

#### `[server]`

| Chave | Padrão | Descrição |
|---|---|---|
| `bind` | `:8080` | Endereço da porta pública |
| `listeners` | `1` | Nº de listeners; `>1` habilita `SO_REUSEPORT` |
| `read_header_timeout` | `5s` | Defesa principal contra Slowloris |
| `read_timeout` | `30s` | Headers + corpo. Generoso: 5 MiB em link ruim demora |
| `write_timeout` | `10s` | Prazo da escrita da **resposta**, contado de quando ela começa: o handler limpa o prazo que o `net/http` arma ao fim dos cabeçalhos (no HTTP/1, na conexão; no h2c, no fluxo) e o rearma logo antes de responder. A leitura do corpo é limitada por `read_timeout`, e a espera do destino por `sink.op_timeout`; contado desde os cabeçalhos, um upload que terminasse depois de `write_timeout` entrava no ring e o `200` não chegava (o cliente reenviava, e o evento duplicava com outro `uuid`; porte do `go-webhook-server`, W 4). O handler **só limpa o prazo quando `read_timeout > 0`**: com `read_timeout = 0` (sem prazo de leitura, aceito) fica o comportamento do `net/http`, e o `write_timeout` limita também o upload — no h2c ele é o único prazo de um fluxo que lê o corpo, e sem ele um fluxo lento seguraria até 64 KiB, uma goroutine e uma vaga de `http2_max_concurrent_streams` para sempre (revisão independente de 2026-10-14, W 32) |
| `idle_timeout` | `60s` | Keep-alive |
| `max_body_bytes` | `5MiB` | `413` acima disso |
| `max_header_bytes` | `64KiB` | `431` acima disso |
| `max_uri_bytes` | `8KiB` | `414` acima disso |
| `max_conns` | `0` | `0` = ilimitado. Teto do **servidor inteiro**, somando todos os `listeners` |
| `max_conns_per_ip` | `0` | `0` = ilimitado. Também somado entre `listeners`. Conta o endereço **exato** do par TCP (nenhum cabeçalho existe no accept; regra abaixo); a conexão excedente é fechada na hora, sem segurar os outros endereços, e conta em `whs_rejected_total{reason="bad_request"}`. Inútil atrás de proxy único |
| `reject_drain_bytes` | `64KiB` | Dreno antes de rejeitar (§4.3) |
| `http2` | `true` | h2c por prior-knowledge |
| `http2_max_concurrent_streams` | `250` | |

**`max_conns_per_ip` conta o endereço exato.** A chave é o endereço do par TCP,
um /128 no IPv6; a zona é descartada e um IPv4 mapeado em IPv6
(`::ffff:192.0.2.7`) é a mesma chave que o IPv4 (a forma única de endereço que o
servidor usa em toda comparação, a mesma de `proxy.trusted`), então
o mapeamento não contorna o teto. Quem tem um /64 pode abrir cada conexão de
um endereço novo e passar do teto por IP; contra um /64 hostil valem
`max_conns` (o teto global, que espera no accept) e o firewall. Contar
por prefixo IPv6 foi recusado (decisão contestável, `docs/DECISOES.md`, porte
de W e F de 2026-10-14; a mesma do `go-webhook-fast-server`, `DECISOES.md` 4c,
do `go-relplog-server`, decisão 16, e do `go-syslog-server`): um /64 pode ser
uma LAN inteira de emissores, e os emissores de webhook são em geral
provedores que saem de *pools* de endereços IPv6 vizinhos; uma cota por
prefixo juntaria as entregas legítimas de um provedor numa só e as recusaria
com um fechamento de conexão, que o emissor vê como falha de entrega. Teste de
trava: `TestPerIPLimitCountsTheExactAddress` (W 6).

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
| `max_bytes` | `512MiB` | A memória retida: `cap` do buffer do JLH mais 256 bytes por item (§7.2). Abaixo da cobrança do maior evento (`MaxEventBytes` pela classe do pool, mais 256) é `exit 1` (§7.2) |
| `high_water` | `0.80` | Fração que faz `/readyz` falhar |
| `data_dir` | `./data` | Raiz de **toda** escrita em disco do serviço (§13.6). Em contêiner é sempre `/data` |

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `type` | `nats` | Seletor de compatibilidade: só `nats` (§0) |
| `workers` | `8` | Dividem **uma** conexão NATS (§8.7.1) |
| `batch` | `256` | `sink.workers × sink.batch` é o teto de publicações em voo (§8.7.2) |
| `batch_max_bytes` | `16MiB` | Teto de JLH por escrita: o lote é escrito em pedaços que não passam dele; um envelope maior vai sozinho. De 1 a 256 MiB (§8.1 passo 2) |
| `batch_max_delay` | `5ms` | |
| `op_timeout` | `5s` | Prazo de um lote, de um futuro pendente e da discagem (§8.7.1, §8.7.2) |
| `health_interval` | `2s` | |
| `health_timeout` | `1s` | |
| `fail_timeout` | `30s` | DOWN → FATAL |
| `max_retries` | `10` | Health checks falhos → FATAL |
| `boot_timeout` | `300s` | |
| `retry_backoff_min` | `100ms` | |
| `retry_backoff_max` | `2s` | |

#### `[sink.nats]`

| Chave | Padrão | Descrição |
|---|---|---|
| `urls` | `nats://127.0.0.1:4222` | Só `nats://`, lista com vírgula para um cluster; o *userinfo* é redigido no dump (§8.7.1, §8.7.9) |
| `username` | *(vazio)* | |
| `password` | *(vazio)* | Redigida no dump e no log de boot |
| `token` | *(vazio)* | Redigido |
| `credentials` | *(vazio)* | Caminho de um `.creds` (JWT + semente NKey) |
| `mode` | `jetstream` | `jetstream` (`PubAck` por mensagem) ou `core` (*best effort*, §8.7.2) |
| `stream` | `LOGHUB_WEBHOOK` | Validado no boot e nunca criado (§8.7.5); ignorado em `core` |
| `subject` | `loghub.{space}.{type}` | Seletor de compatibilidade: só esse valor; o subject é derivado (§3.8) |
| `tls` | `false` | Seletor de compatibilidade: só `false` (§8.7.9) |

#### `[loghub]`

| Chave | Env | Padrão | Descrição |
|---|---|---|---|
| `eb_url` | `LOGHUB_EB_URL` | *(vazio)* | Só `nats://`; preenche `sink.nats.urls` (§8.7.11) |
| `beacon_interval` | `LOGHUB_BEACON_INTERVAL` | `15` | Inteiro puro em segundos, ou duração do Go; `0` desliga o beacon periódico, não o de conexão; acima de zero, no mínimo `1s` (§8.7.10) |
| `ctl_namespace` | `LOGHUB_CTL_NAMESPACE` | `loghub._ctl` | O beacon vai para `<ctl_namespace>.beacon` (§8.7.10) |

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
| Dumps de emergência | `<ring.data_dir>/emergency/emg-*.jsonl` | `emergency.enabled=true` e um dump (§9) |

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

**Os compose repassam o que o `.env.example` oferece** (2026-10-14; a regra do
`go-webhook-server`, W 27 e 28). O compose lista o ambiente do serviço
explicitamente, então uma variável fora da lista é ignorada em silêncio. Todo
`WHS_SINK_*` do `docker/.env.example` aparece como `${NOME…}` no compose (ou no
`common.yml` que ele estende), e as imagens auxiliares são fixadas por versão,
nunca `latest`. O `TestComposeForwardsEverySinkVariable` e o
`TestComposeImagesArePinned` (`internal/config/compose_test.go`) conferem os
dois, sem Docker. `WHS_SINK_BATCH_MAX_BYTES` (§8.1) também é repassada, com o
padrão do binário.

O `.creds` de `WHS_SINK_NATS_CREDENTIALS` (§8.7.9) é um arquivo **lido**, nunca
escrito: ele entra por `/etc/webhook`, montado `:ro`. O compose traz o volume
comentado (`./certs:/etc/webhook:ro`, com o `.creds` dentro); o serviço é
`read_only` e só tem `/data`, então sem o volume o caminho que o
`.env.example` sugere (`/etc/webhook/whs.creds`) não existe e o boot falha,
ruidoso e não silencioso (achado da triagem de 2026-10-14).


---

## 14. Código

### 14.1 Posição no monorepo

O repositório `loghub-paradise` abriga os quatro serviços do ecossistema. A
**raiz não tem `go.mod`** — cada serviço Go é um módulo aninhado e independente,
deixando a raiz livre para serviços em outras linguagens.

```
loghub-paradise/                  <- sem go.mod
  go-webhook-nats-server/
    go.mod                        <- module .../go-webhook-nats-server
    cmd/go-webhook-nats-server/
    internal/
    docs/SPEC.md
  webhook-worker/                 <- futuro, linguagem livre
  webhook-admin/
  webhook-shots/
  tmp/                            <- documentos históricos, não usados
```

Consequências:

- Cada serviço Go versiona, testa e libera de forma independente, com tags no
  formato `go-webhook-nats-server/v1.2.0`.
- `go install github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server/cmd/go-webhook-nats-server@latest`
  funciona direto.
- Uma atualização de dependência aqui não obriga a recompilar nem retestar os
  outros serviços.
- **Sem `go.work`.** É ferramenta de desenvolvimento, exigiria `GOWORK=off` no
  CI e, com um único serviço Go, não resolve problema nenhum. Reavaliar apenas se
  o `webhook-worker` for escrito em Go e a duplicação do envelope (§14.2) doer.


### 14.2 Layout interno

```
go-webhook-nats-server/
  cmd/go-webhook-nats-server/main.go
  internal/
    config/     registry, ini, flags, env, defaults, dump, validação
    server/     http.Server, handler, pipeline, admin
    clientip/   XFF/X-Real-IP + proxies confiáveis        [lógica pura]
    ident/      Identity congelada e gramática de space   [lógica pura]
    envelope/   JLH, payload webhook, ParseHead, pools    [lógica pura]
    jlhcheck/   oráculo de teste dos schemas              [só em teste]
    ring/       Ring + memory.go
    emergency/  dump e reingestão em JSON Lines
    sink/       Sink + nats.go + workers.go + health.go
    control/    heartbeat beacon (loghub._ctl.beacon, NATS core)
    obs/        métricas + slog
  docs/SPEC.md
  go.mod
```

`internal/` impede import externo acidental. `clientip` e `envelope` são lógica
pura, testáveis sem rede, sem disco e sem NATS.

`ring` e `sink` expõem cada um **uma** interface, com uma implementação (§2.1).
Fora do destino, os pacotes e arquivos têm o mesmo nome e o mesmo conteúdo dos
do P: `diff -ruN -x .DS_Store ../go-webhook-pg-server .` mostra a troca e é a
base do porte de correções nos dois sentidos. O `sink/nats.go` tem a forma do
`go-syslog-nats-server` (o item é outro) e as peças do fast de §8.7;
`control/` é o do `go-syslog-nats-server`, com o escritor JLH deste servidor.

### 14.3 Dependências

| Módulo | Uso | Escopo |
|---|---|---|
| `github.com/nats-io/nats.go` | Cliente NATS puro Go, com o pacote `jetstream` | produção |
| `github.com/patrickbrandao/go-loghub-uuidv7` | UUIDv7 (RFC 9562) nível 3, `GenerateAt` sobre a amostra de relógio do `event` (§3.1) | produção |
| `github.com/patrickbrandao/go-loghub-ident` | Identidade LogHub: AgentUUID (`from`), Workspace (`space`), AgentName, Hostname (§3.5) | produção |
| `github.com/google/uuid` | Oráculo independente nos testes de UUIDv7 | **somente teste** |
| `github.com/santhosh-tekuri/jsonschema/v6` | Contrato dos envelopes com os schemas JLH e `webhook` (§15.1) | **somente teste** |

**Três dependências em produção. Mais nada.** `go-loghub-uuidv7` e
`go-loghub-ident` não têm dependência transitiva alguma — só a biblioteca padrão
do Go. Saem, com os recursos, `rueidis`, `modernc.org/sqlite`, `pgx`,
`go-sql-driver/mysql` e `clickhouse-go` e todas as transitivas deles.
Acrescentar uma dependência de produção exige justificativa nesta seção.

#### nats.go: a primeira dependência

- **Não há alternativa razoável na biblioteca padrão.** O protocolo NATS é
  simples, mas o JetStream em cima dele — publicação assíncrona com futuros,
  cabeçalhos, desduplicação, stream esperado, reconexão com estado — não é, e
  reescrevê-lo seria a decisão errada pelo motivo de sempre. É o cliente
  oficial, Go puro, e o pacote `jetstream` é a API atual.
- **As opções que não são as padrões** (§8.7.1, §8.7.2) são parte da
  justificativa: `WithRetryAttempts(0)`, `ReconnectBufSize(-1)`,
  `WithPublishAsyncTimeout` e o `TLSCertCB` que recusa. Uma versão nova do
  cliente pede a mesma auditoria de posse de buffer (a biblioteca não pode ler
  `msg.Data` depois de a publicação voltar) e de TLS automático.
- **Transitivas.** `nkeys` e `nuid` (do próprio projeto NATS),
  `klauspost/compress` (compressão do transporte websocket, que este servidor
  recusa) e `golang.org/x/crypto` (as chaves NKey). Nenhuma abre socket por
  conta própria, escreve em disco ou lê `TMPDIR`; a regra de §13.6 continua
  verdadeira.
- **Tamanho:** o binário tem 11,9 MB, contra 16,5 MB do `go-webhook-pg-server`
  (o mesmo build `darwin/arm64`, `CGO_ENABLED=0`, flags padrão, medido em
  2026-10-10); a imagem do compose, 26,9 MB.


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
  §10.1, os cinco seletores aceitando só o valor implementado, as chaves dos
  recursos retirados recusadas como desconhecidas (§0), os `LOGHUB_*`, a
  `LOGHUB_EB_URL` (§8.7.11), a redação de URL no dump e os erros dos parsers
  sem eco, inclusive a flag sem valor diante de outra flag e o valor depois
  de um espaço (§13.2).
- `sink` — a tabela de `classifyNATS` (§8.7.6), a forma da mensagem e o teto do
  bloco de headers (§3.8), o casamento de subjects (`subjectCovers`), o contrato
  de `docker/nats/stream.json`, as opções do cliente que nunca pedem TLS, um
  servidor NATS falso que exige TLS e nunca recebe um `ClientHello`, o veredito
  de permissão negada, a amostragem dos erros assíncronos, "sem
  respondedores" fatal só na primeira checagem (§8.7.5), e os workers com
  entrega parcial (§8.1).
- `ring` — a contabilidade de bytes e itens, e o `Seal` como barreira: nenhum
  `Enqueue` aceito depois dele, nenhum perdido pelo `Drain` que o segue
  (§10.3).
- `control` — o envelope do beacon (ordem das chaves, contrato do JLH), o
  `address`, o pedido antes do `Start`, o periódico, a falha contada (§8.7.10).
- `emergency` — round-trip do JSON Lines com JLH reais, arquivo terminado sem
  `\n`, linha danificada no meio com as seguintes entregues, linha acima do teto,
  linha cujo JLH o `ParseHead` recusa, linha de outro `type`, `level`, `step` ou
  payload (a porta de §9.2), JSON inválido, linhas vazias, um envelope com `0x0A`
  deixado fora do arquivo e contado, o temporário que não sobra nem é lido, e
  `emg-*.bin` ignorado (§9).

**Um teste de regressão falha, nunca pula** (porte do `go-webhook-server`,
W 29, 2026-10-14). O guarda de um defeito já corrigido termina em `t.Fatalf`
(ou `t.Errorf`) com "regressão de §…"; `t.Skip` fica só para ambiente ausente
(o servidor externo sem a variável `WHS_TEST_*`, o privilégio que o DSN não
tem). Um `t.Skip("BUG …")` disparava exatamente quando o defeito voltava, e o
`go test` contava o pulo como aprovado.

### 15.2 Fuzz — obrigatório

- `FuzzResolve` — o único código que processa entrada hostil
  estruturada; um bug ali vira spoofing de IP.
- `FuzzRead` — o dump é lido de disco após um crash; um leitor frouxo
  transforma um arquivo corrompido em pânico no boot, ou entrega uma linha que
  não é envelope (e que o NATS publicaria sem ler).
- `FuzzEnvelope` — método, alvo, cabeçalho, query e corpo arbitrários: todo
  envelope passa no contrato, não tem `0x0A`, e o `ParseHead` o lê de volta.
- `FuzzServeHTTP` — bytes arbitrários num socket real, com e sem proxy
  confiável: o servidor recusa, ou o envelope passa no contrato.

### 15.3 Integração

NATS real com JetStream, endereçado por `WHS_TEST_NATS_URL` e **pulando** na
ausência dela; cada teste cria o seu stream (o servidor nunca cria). Um segundo
servidor **sem** JetStream (`WHS_TEST_NATS_NOJS_URL`) prova o FATAL imediato de
§8.7.5.

As versões das ferramentas NATS são **fixadas**, nunca `latest`: `nats-server`
`v2.14.6` nos comandos da suíte e `natsio/nats-box:0.20.0` no `nats-init` do
compose, que cria o stream a partir de `docker/nats/stream.json`. Com `latest`,
a mesma receita instalaria outra ferramenta noutro dia, e uma mudança nas flags
ou na leitura do `stream.json` derrubaria a subida ou os testes sem mudança
nenhuma no projeto.

Cenários obrigatórios:

- Todo id aceito chega ao stream, com subject, `Nats-Msg-Id` e headers de §3.8,
  e cada mensagem lida de volta passa no `jlhcheck` e é igual ao JLH byte a byte;
  corpo binário em base64 e NUL na query ida e volta; redelivery do mesmo lote
  deduplicada e contada (§8.7.3).
- Stream ausente, que não cobre o subject, que captura o plano de controle,
  selado, com `no_ack`, `max_msg_size` pequeno, JetStream desligado ⇒ `exit 3`
  **imediato**, porta pública fechada (§8.7.5).
- Uma mensagem acima do `max_payload` recusada sozinha, as demais do lote
  entregues (§8.7.2).
- Subject de outro `space` sem stream ⇒ fatal nomeando o subject (§8.7.5).
- `core`: entrega a um assinante, sem stream (§8.7.2); um subject de outro
  `space` sai com um `WARN` por subject (§8.7.5).
- Beacon na conexão e no periódico, em `<ctl_namespace>.beacon`, fora do stream
  (§8.7.10).
- Destino cai → `503` em até `health_interval`; volta dentro da janela → retoma
  sem perder nada.
- Destino cai e não volta → FATAL, dump JSON Lines correto, `exit 3`; boot
  seguinte reingere e remove o arquivo.
- Boot com `emg-*.jsonl` com uma linha danificada no meio → as íntegras
  entregues, arquivo renomeado para `.corrupt`.
- Boot com `emg-*.bin` → `WARN`, arquivo intocado, porta pública abre.
- `SIGTERM` com ring cheio → dreno completo, `exit 0`, zero perdas.
- Segundo `SIGTERM` → `exit 130`.
- Stream editado depois do boot (subjects estreitados, `no_ack` ligado) ⇒ FATAL
  na checagem seguinte; um subject de outro `space` já publicado e retirado ⇒
  esquecido, sink UP, e a próxima publicação nele é recusada nomeando-o
  (§8.7.5); publicação negada em `core` ⇒ o próprio lote volta fatal
  (§8.7.6); recusa `logic` não deixa bytes contados no ring (§12.3).
- Reingestão entregue em parte; `SIGTERM` no meio da reingestão ⇒ `130`, dump
  mantido; gêmeo de um dump removido, dump interrompido intocado e informado
  num `WARN` (§9.1, §9.2).
- Upload lento e sink que não responde no shutdown ⇒ o dump começa em um
  `drain_timeout`, não em dois; um upload que termina depois do último `Drain`
  ⇒ `503`, nunca `200` (§10.3).
- `SIGHUP` → ignorado, processo segue servindo.
- `-race` em todos os caminhos de erro do pool de buffers (§12.3).

Sem servidor NATS, a suíte de `app` sobe o ciclo de vida com um sink falso de
teste (no próprio arquivo de teste, nunca em código de produção).

### 15.4 Carga

- `bombardier` para HTTP/1.1, `h2load` para h2c.
- **Dois perfis**, sempre: conexões persistentes (mede o pipeline) e conexão nova
  a cada requisição (mede accept + handshake). Bots reais fazem os dois.
- `perf` + flamegraph para localizar CPU. Quase sempre é alocação ou
  serialização, raramente o parser HTTP.

### 15.5 Critérios de aceite v1

Referência: 1 nó de 4 vCPU, corpo de 1 KiB, NVMe local, NATS 2.11 local com
`storage: file`.

| Combinação | Métrica | Meta |
|---|---|---|
| `ring=memory` + `sink=nats` (`jetstream`) | Vazão e p99 do `200` | **sem meta numérica** até a primeira medição válida |
| Qualquer | RSS sob carga por 30 min | estável, < 512 MiB |
| `SIGTERM` com ring cheio | Eventos perdidos | 0 |
| Sink FATAL | Eventos perdidos (via emergência) | 0 |
| Qualquer | Casos de spoofing de XFF rejeitados | 100% |

A vazão depende da latência do `PubAck` e do tamanho do lote; um número
inventado aqui viraria referência. O fast com `ring=memory` + `sink=none` dá o
teto do pipeline que este servidor compartilha, e a diferença para a medição
daqui é o custo exato do NATS.

---

## 16. Matriz de garantias

### 16.1 O que o `200` significa

| `ring` | `sink` | O `200` significa | Depois do `200` | Perda em `kill -9` |
|---|---|---|---|---|
| `memory` | `nats` `jetstream` | Está na RAM deste nó | Entregue quando o stream confirma (`PubAck`) † | **Sim** (mitigado por §9 e §10.3) |
| `memory` | `nats` `core` | Está na RAM deste nó | Entregue quando o servidor NATS recebe; sem assinante, some | **Sim** (idem) |

† A durabilidade depois do `PubAck` é a do stream (`storage`, réplicas,
`sync_interval`, §8.7.4); o servidor não lê nem altera esses valores.

### 16.2 Custo relativo

Não medido (§15.5). Com `sink.batch = 256`, cada lote espera as confirmações de
até 256 publicações em voo, e os oito workers dividem uma conexão.

---

## 17. Invariantes

Regras que nenhuma otimização futura pode violar. A numeração é a do fast; as
regras dos recursos retirados ficam marcadas, e as do NATS começam em 17.

1. O `200` **nunca** é enviado antes de o item estar no ring (§2.2).
2. Ring cheio → `429`. **Nunca** um `200` seguido de descarte — exceto na recusa
   permanente de **uma** mensagem por um destino vivo (`logic`, §8.7.6), que é
   logada e contada. Uma recusa que atingiria todos os eventos igual nunca é
   `logic`: é fatal (§8.7.6).
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
    mensagem NATS levam os mesmos bytes; subject e headers são lidos do próprio
    JLH (§3.8).
12. `ring` e `sink` são **ortogonais**: nenhuma implementação de um consulta o
    tipo do outro (§2.1).
13. Toda escrita em disco acontece sob `ring.data_dir` — e dentro de um
    contêiner esse diretório é `/data`, em toda imagem do ecossistema (§13.6).
14. `uuid` e `event` saem de **uma** amostra de relógio, e o `uuid` devolvido em
    `X-LogHub-UUID` é o do envelope publicado; ele nunca é regerado (retry, dump,
    reingestão) (§3.1).
15. *(Retirada no P e devolvida aqui como 20: a maior mensagem é conferida no
    boot contra os tetos do destino.)*
16. Uma linha do dump de emergência é um JLH inteiro seguido de `\n`, e nada
    mais (§9.1).
17. O servidor **nunca** cria nem altera um stream (§8.7.5).
18. Erro desconhecido é `network`, **nunca** `logic` (§8.7.6).
19. Não há fila fora do ring: `ReconnectBufSize(-1)` e `WithRetryAttempts(0)`
    (§8.7.1, §8.7.2).
20. A maior mensagem que o servidor pode publicar cabe no `max_payload`, no
    `max_msg_size` e no teto do *file store*, conferidos no boot (§8.7.5).
21. O stream de dados **nunca** cobre o plano de controle; o beacon vai por NATS
    core e **nunca** toca a saúde do sink (§8.7.5, §8.7.10).
22. O cliente NATS **nunca** faz *handshake* TLS (§8.7.1).

---

## 18. Roteiro pós-v1

| Item | Motivo de estar fora da v1 |
|---|---|
| Múltiplos streams por Host ou path | Depende do desenho de multi-tenancy do `webhook-admin` |
| TLS até o NATS | Fora por decisão (§1.2, §8.7.9); volta só com decisão registrada em `docs/DECISOES.md` |
| Sonar, `ping`/`pong` | Quando existir um `go-loghub-driver` (§8.7.10) |
| `stage`, `503` no ring cheio, PROXY protocol | Entram quando entrarem no `go-webhook-fast-server` (§0) |
| Verificação HMAC por tenant | Precisa de um registro de tenants |
| Rings particionados por CPU | Só se o benchmark mostrar contenção no ring único |
| HTTP/3 | Nenhum emissor relevante usa; 2–4× a CPU por byte |
| TLS na porta pública | Proxy reverso resolve com ACME de graça |

### 18.1 Pendências conhecidas

Defeitos reais que o código ainda tem, cada um contra uma regra desta spec. A
correção segue a ordem de sempre (spec, código, teste) e troca o item por uma
linha só, com o número (que o `docs/TESTING.md` e os comentários do código
citam) e a seção da regra.

Em 2026-10-14 nenhum item ficou aberto. O número de W (`go-webhook-server`,
§19.1), de onde veio cada correção, vai entre parênteses.

1. *Corrigido em 2026-10-14 (decisão do usuário):* o ring de memória cobra `cap(Env)` mais `ring.ItemOverhead` (256 bytes) e desconta o `Item.Charged` gravado; sem cópia para classe menor, padrões mantidos; regra em §7.2 e §12.2. (W 8)
2. *Corrigido em 2026-10-14:* o `Content-Length` só pré-dimensiona o corpo até 64 KiB; o buffer cresce com o que chegou, sem passar do declarado; regra em §12.2 e §4.3 passo 11. (W 1)
3. *Corrigido em 2026-10-14:* o listener retenta todo erro do `accept` salvo o próprio fechamento, e um `Serve` que ainda termine com erro passa pelo shutdown gracioso e sai com 1 se ele foi completo (3/130 senão; o código próprio que a direção sugeria não foi criado, `docs/DECISOES.md`); regra em §10.3 e §10.5. (W 5)
4. *Corrigido em 2026-10-13:* a reingestão e os *gauges* leem `emergency.dir` em todo boot; regra em §9 e §9.2.
5. *Corrigido em 2026-10-13:* os *gauges* de emergência contam todo arquivo listado, legível ou não; regra em §9.2.
6. *Corrigido em 2026-10-13:* um FATAL durante o dreno do `SIGTERM` encerra a espera e sai com 3; regra em §10.3. (W 21)
7. *Corrigido em 2026-10-13:* `retention: interest` é recusado em toda checagem e `max_msgs_per_subject > 0` dá `WARN`; regra em §8.7.5. (W 18)
8. *Corrigido em 2026-10-13:* `10158`, `10167` e todo `Code` 429 são `network`; regra em §8.7.6. (W 19)
9. *Corrigido em 2026-10-13:* a assinatura negada em `_INBOX.` é veredito; regra em §8.7.6.
10. *Corrigido em 2026-10-14:* a porta do dump recusa U+2028/U+2029 crus (o leitor de string de `emergency/shape.go`), e a mesma chave não passa mais em duas grafias; regra em §9.2. (W 20)
11. *Corrigido em 2026-10-14:* os três guardas de regressão de `internal/server/describe_test.go` terminam em `t.Fatalf("regressão de §…")`; regra em §15.1. (W 29)

Corrigidos aqui na mesma rodada, sem item próprio nesta lista (a linha de cada
um está na §19.1 do `go-webhook-server`): W 2 (a checagem rápida do passo 9
olha também os bytes, §4.3), W 3 (o dreno de recusa só em HTTP/1.x sem
`Expect` pendente; este servidor também fala h2c, §4.3), W 4 (`write_timeout`
conta da resposta, §13.5), W 6 (decidido, contestável: `max_conns_per_ip`
conta o endereço exato, §13.5 e `docs/DECISOES.md`), W 10 (o worker sobrevive a
um erro de `Dequeue`, por simetria, §8.1), W 11 (decisão do usuário:
`sink.batch_max_bytes`, 16 MiB, de 1 byte a 256 MiB, §8.1), W 30 (os lotes
retidos contam no teto de itens, §7.2), e no Docker o volume
`./certs:/etc/webhook:ro` comentado para o `.creds` de
`WHS_SINK_NATS_CREDENTIALS` (§13.6). W 25, W 26, W 27 e W 28 já estavam certos
aqui; de W 24 (já certo na validação) veio a defesa dupla no parser (§13.5).

Corrigidos aqui na revisão independente de 2026-10-14, com teste e mutação (a
linha de cada um está na §19.1 do `go-webhook-server`): W 31 (o shutdown por
erro do `Serve` tolera o primeiro sinal, §10.3), W 32 (o prazo de escrita só é
limpo com `read_timeout > 0`, §13.5 e §12.2), W 35 (piso de `ring.max_bytes`,
§7.2), W 39 (o `WaitReady` não dorme além do prazo, §10.1), W 40
(`admin.bind` com porta numérica, §11 e §13.5) e W 41 (byte de controle num
valor de texto é exit 1, §13.2; o `LOGHUB_EB_URL` com `\n` passa a ser recusado
por isso, antes da regra de "só brancos"). W 33 e W 34 já estavam certos aqui
(o `writeDirect` usa `DeliveredOf`; a porta de `shape.go` é a estrita); W 36,
W 37, W 38 e W 42 não se aplicam (sem `sink=sqlite`, sem Redis).
