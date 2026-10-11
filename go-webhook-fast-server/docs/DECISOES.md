# go-webhook-fast-server — Decisões de projeto

Registro da entrevista de 2026-10-04/05 que definiu como o `go-webhook-fast-server`
nasce do `go-webhook-server`. Este arquivo é a **entrada** da reescrita de
`docs/SPEC.md`. Quando a SPEC nova existir, ela é a fonte da verdade, e cada
decisão abaixo aparece nela com o número da seção.

> **Rodada de 2026-10-06 — fatia "JLH + identidade + sinks" entregue.** O
> usuário escolheu, nesta rodada, só essa fatia sobre a mecânica ring × sink da
> v1: envelope JLH `type=webhook`/`mode=fast` (§2 "Envelope"), identidade
> LogHub (§2 "Módulo e identidade"), resposta `X-LogHub-UUID` com `stage`, e os
> seis sinks com o JLH. A **decisão 10 foi substituída**: `sink=postgres` grava
> na fila `loghub.inbox` da skill `loghub-postgres-table-inbox`, e MySQL e
> SQLite numa tabela `inbox` que espelha as colunas dela. O que entrou e o que
> continua em aberto está em `docs/SPEC.md` §0; o resto deste documento segue
> valendo como plano das próximas etapas. O `go-webhook-server` também virou
> LogHub nesta rodada (`type=webhook.request`), então a frase abaixo sobre ele
> continuar "inalterado" é histórica.

Estado da pasta em 2026-10-05: **cópia pura** do `go-webhook-server` (110
arquivos, sem `.DS_Store`), sem nenhuma adaptação ainda. O `go-webhook-server` continua vivo e
não é alterado. Até o commit único no fim (decisão 20), a base de revisão é:

```bash
diff -ruN -x .DS_Store go-webhook-server go-webhook-fast-server
```

## Fontes normativas

| Fonte | Versão | Papel |
|---|---|---|
| skill `loghub-json-schema` | 1.0.0 (2026-10-03) | Envelope JLH: nove campos, regras, `jlh-envelope.schema.json` |
| skill `loghub-type-webhook-fast` | 2026-10-03 | `type=webhook`, `payload.mode=fast`, ring/sink, respostas, mapeamento de campos |
| `github.com/patrickbrandao/go-loghub-uuidv7` | v0.0.3 | UUIDv7 nível 3, `GenerateAt` |
| `github.com/patrickbrandao/go-loghub-ident` | v0.5.2 | `from` (AgentUUID), `space` (Workspace), `DATADIR` |

Quando as duas skills divergem, vale a mais restritiva: a interseção dos dois
schemas (`space` `^[a-z0-9-]+(\.[a-z0-9-]+)*$` com até 64 caracteres; `step`
≤ 32; `from` UUIDv7 minúsculo). Onde o exemplo `go-bridge` contradiz
`mapeamento-campos.md`, vale o mapeamento.

---

## 1. Decisões do usuário

| # | Tema | Decisão |
|---|---|---|
| 1 | Ring | `memory` \| `sqlite` \| `redis` \| desativado (`none`, que equivale a `WEBHOOK_FAST_STAGE=sink`). O fast **sempre** grava a requisição num meio (ring ou, com o ring desativado, o sink) antes de devolver o UUID. |
| 2 | SQLite duplo | `ring=sqlite` com `sink=sqlite` **não** derruba o boot: emite um alerta (`WARN`) e o ring efetivo vira `memory`, com o SQLite só como destino final. |
| 3 | Redis duplo | `ring=redis` e `sink=redis` podem coexistir, cada um com a sua URL (`ring.redis.*`, `sink.redis.*`). Com o **mesmo** servidor: alerta, e o ring efetivo vira `memory`, a mesma regra do SQLite. |
| 4 | Forma do ring Redis | **Stream compartilhado** entre as instâncias, com consumer group. O nome do consumidor é o AgentUUID, e os pendentes de uma réplica morta são reivindicados com `XAUTOCLAIM`. |
| 5 | Sobras no desligamento | **Dump de emergência adaptado** (ring `memory`): tópico mais bytes JLH em `$DATADIR/emergency` (`O_EXCL` + `fsync`), reingestão no boot antes de abrir a porta, exit 3. Mesmo `uuid`, então a dedup por `(uuid, step)` continua valendo. |
| 6 | Sinks | Os seis da v1, adaptados ao JLH: `redis`, `sqlite`, `postgres`, `mysql`, `nats`, `none`. Nenhum destino local novo (spool, socket, stdout). |
| 7 | `sink=none` | Só com ring ativo (`memory`/`sqlite`/`redis`). Ring desativado com `sink=none` é recusado no boot (exit 1): o `200` sairia sem nada gravado. |
| 8 | NATS | `sink.nats.mode = jetstream` (padrão) \| `core`. `jetstream` valida o stream no boot, espera o `PubAck` e usa `Nats-Msg-Id = <uuid>.<step>`. `core` é opt-in e documentado como *best effort*. `ReconnectBufSize(-1)` continua. |
| 9 | Rotas | **Nenhuma na v1.** `params` é sempre `{}`, `space` é o Workspace, o payload nunca tem `forward`, e `CONNECT` responde 405. |
| 10 | Layout nos sinks | **Só JLH mais colunas promovidas**: um blob com os mesmos bytes do ring. Redis: `XADD loghub.{space}.webhook` com o campo `e`. NATS: subject `loghub.{space}.webhook`, `Data` = JLH, headers `X-LogHub-*`. SQL: tabela nova com `uuid` (nativo no PG, `BINARY(16)` no MySQL), `step`, `space`, `level`, `sid`, `ev_uts`, `ev_sub`, `method`, `host`, `path`, `src_ip`, `ip_ver`, `content_length` e `env`, mais `UNIQUE (uuid, step)`. |
| 11 | Recusa permanente (`logic`) | Na tentativa direta (`auto`/`sink`): `500` com `X-LogHub-UUID`, sem entrar no ring. Na descarga: descarte com o UUID no log e contado em `dropped{sink_logic}`. As checagens de tamanho no boot tornam o caso raro. |
| 12 | Saúde do sink | **Atalho, não fusível.** `Health()` periódico para métricas e log. Em DOWN, o `auto` pula a tentativa direta e grava no ring, sem `503 sink_down`. `/readyz` falha no boot, no shutdown e com o ring ≥ `high_water` (0.80); com `stage=sink`, DOWN também o derruba. FATAL só por erro fatal explícito (credencial, stream ou tabela inexistente). O boot continua *fail-closed*. |
| 13 | `content.body` | `utf-8` quando o corpo é UTF-8 válido, não tem NUL e o escape JSON não fica maior que o base64; senão base64 padrão com padding. Uma pré-passada mede o tamanho exato. Pior caso ~1,33× (512 KiB → ~683 KiB). |
| 14 | IP do cliente | Cadeia fixa `X-Forwarded-For` → `Forwarded for=` → `X-Real-IP`, lida só de par TCP confiável. **Ressalva a documentar:** o proxy confiável precisa sobrescrever os três cabeçalhos; um que ele não toca pode ser forjado e vencer. |
| 15 | Borda | PROXY protocol v1/v2 **opt-in** (`proxy.protocol = off\|optional\|required`), parser próprio só com stdlib e com fuzz, aceito só de pares confiáveis e com deadline próprio. Sem TLS na porta pública: `sec.mode ∈ {proxy, none}`, e os `fp*` vêm só de cabeçalho do proxy. |
| 16 | Buffers | Pool em classes de 4, 16, 64 e 256 KiB (`make` acima disso), escolhidas pela pré-passada, mantendo a posse única `Release`/`Taint`/`Discard` (§12.3 da v1). Uma tentativa direta que estoura o prazo marca `Taint` antes de ir ao ring. |
| 17 | Validação por JSON Schema | **Só testes e CI** (`santhosh-tekuri/jsonschema/v6` só em teste). No boot, código próprio valida `from` e `space`. O desvio da regra JLH ("validar antes de publicar") fica escrito na SPEC com o número de um benchmark. |
| 18 | Nomes | Env `WEBHOOK_*`: os 7 nomes da skill são literais (`WEBHOOK_FAST_STAGE`, `WEBHOOK_RING_SLOTS`, `WEBHOOK_RING_BYTES`, `WEBHOOK_SINK_TIMEOUT_MS`, `WEBHOOK_DRAIN_TIMEOUT`, `WEBHOOK_MAX_BODY`, `WEBHOOK_TRUSTED_PROXIES`), o resto é `WEBHOOK_<SEÇÃO>_<CHAVE>` e o arquivo vem de `WEBHOOK_CONFIG`, sem aliases `WHS_`. Métricas com prefixo `whf_`. |
| 19 | Documentação | `docs/SPEC.md` **autocontida e reescrita** ("go-webhook-fast-server — Especificação v1"), com o esqueleto §1–§18, as fontes normativas com versão fixada e uma seção de divergências em relação à v1 e às skills. `AGENTS.md`, `CLAUDE.md` e `TESTING.md` novos, herdando o histórico de bugs. |
| 20 | Git | **Commit único no fim**, só com o seu "sim". Nada de git de dentro da pasta do módulo. |
| 21 | Plano de controle | Só **heartbeat beacon** (`type=beacon`, level 9, payload `name`/`hostname`/`machine`/`address`/`uptime`/`meta`) em `loghub._ctl.beacon`, e só com `sink=nats`: na conexão, em cada reconexão e a cada `LOGHUB_BEACON_INTERVAL` (15 s; 0 desliga). Sem sonar e sem ping/pong (lacuna documentada até existir um `go-loghub-driver`). |

---

## 2. Padrões assumidos (contestáveis)

Pontos em que as specs ou a v1 já indicam a resposta. Entram na SPEC assim, a
menos que você diga o contrário.

### Módulo e identidade

- Módulo `github.com/patrickbrandao/loghub-paradise/go-webhook-fast-server`;
  binário `cmd/go-webhook-fast-server`; `/app/go-webhook-fast-server` na imagem;
  tags `go-webhook-fast-server/vX.Y.Z`; letra **F** no `CHANGELOG.md` da raiz.
  São dois módulos independentes, sem código compartilhado: correções nos
  pacotes de mesma origem são portadas à mão.
- `lhident.Initialize()` roda **uma vez**, no `main`, depois de `--help`,
  `--version` e `--dump-config`. Um struct `Identity` é injetado na app, porque os
  testes chamam `runContext` dezenas de vezes e uma segunda chamada sai com 112.
  Os pacotes cujos testes leem a identidade ganham `TestMain` com as cinco
  variáveis.
- `from` = AgentUUID; `space` = Workspace, validado no boot pela gramática da
  interseção (exit 1), recusando segmentos vazios (`a..b`, `.x`, `x.`);
  `sid` = PID; `step` = 0.
- `server.node_id` sai. Os nomes de cliente dos drivers usam AgentName e
  Hostname; o dump passa a `emg-<agent_uuid>-<unixnano>.bin`. Os códigos de saída
  100–114 do ident entram na tabela de códigos de saída.
- **Raiz de escrita única:** um parâmetro só (`data_dir`, env `DATADIR`), com
  padrão `./data` convertido em caminho absoluto. O `main` o exporta como
  `DATADIR` antes do `Initialize()`. Identidade, ring SQLite, `events.db` e dump
  ficam todos sob ela; o contêiner usa `/data`.

### Envelope

- JLH com os nove campos na ordem canônica: `type=webhook`, `level=9`, `step=0`,
  `payload.mode="fast"`. Serializador próprio por *append*, chamado **uma** vez:
  ring e sink recebem os mesmos bytes. `content.type` e `content.encoding` são
  sempre emitidos (`""` quando o tipo está ausente).
- `uuid` = `uuidv7.GenerateAt(Level3, now)` sobre **uma** amostra de relógio, a
  mesma que preenche `event{uts, sub, iso}` (com o `iso` truncado em ms). Isso
  fecha a dívida do §3.1 da v1. Gerador `uuidv7.NewGenerator()` (ChaCha8): a
  preocupação com PCG era da lib antiga. O `init()` que verifica `crypto/rand`
  continua. Streams e tabelas são novos, sem mistura com ids Level2.
- Campos da v1 sem lugar no JLH vão para `network.meta` com chaves fixas:
  `peer-addr`, `peer-port`, `src-source` (`tcp|xff|forwarded|xri|proxy`) e
  `http-proto`. `uri` e `node-id` saem, substituídos por `url` e `from`. O
  invariante 8 ("`peer-addr` sempre registra a ponta TCP") passa a apontar para
  `network.meta`.
- `headers` vira um array `{name, value}` com nomes em minúsculas, ordenado de
  forma estável por nome (valores repetidos na ordem de chegada), com `host`
  recolocado a partir de `r.Host`. Saem os hop-by-hop e os nomes listados em
  `Connection`. `headers.exclude` continua e entra `headers.redact` (o valor vira
  `"***"` e o nome fica); os dois vêm vazios por padrão.
- Método fora de `^[A-Z]+$` responde 405 sem envelope (sem `ToUpper`), e
  `OPTIONS *` responde 400. `path` = `EscapedPath` (`""` vira `/`). `host` vai em
  minúsculas, sem porta nem colchetes. `url` é reconstruída sem a porta padrão.
  A `query` passa por um parser próprio equivalente a
  `parse_qsl(keep_blank_values)`, com fuzz.
- Os cabeçalhos de proxy lidos de par confiável são `X-Forwarded-Proto`, `Host`
  e `Port`, `X-Client-Port`, `X-Server-Addr`, `X-TLS-JA3`/`JA4` e
  `X-HTTP2-Fingerprint`. Quando faltam: `sport=0` e `dst=0.0.0.0` ou `::`, como
  manda o mapeamento (o `go-bridge` faz diferente). `mss` vem de `TCP_MAXSEG`,
  só quando a origem é a própria conexão. Padrão de confiáveis:
  `127.0.0.1/32,::1/128`.
- Os exemplos `examples/*.json` da skill fast **não** servem de vetor dourado: o
  `uuid` `01a103ed-a1fe-73e9-…` tem `rand_a = 1001`, então seus bits abaixo do ms
  não são tempo. Os testes afirmam a igualdade completa entre `uuid` e `event` em
  ms, µs e ns (ns = 000 no macOS).

### Gravação, ring e sinks

- `WEBHOOK_FAST_STAGE = auto|ring|sink` (padrão `auto`), orquestrado na camada
  do handler, nunca dentro do ring ou do sink: o invariante 12 (ortogonalidade)
  continua valendo. Ring desativado força `sink`. `stage=sink` com ring ativo é
  recusado no boot (exit 1), por ser contraditório.
- O caminho direto é limitado por um semáforo `max_inflight`. Cheio: vai ao ring
  em `auto` e responde 503 em `sink`. `WEBHOOK_SINK_TIMEOUT_MS` (50 ms) vale para
  a escrita direta; a descarga usa `sink.op_timeout`.
- **Descarga:** uma rotina por instância. Lê até `sink.batch` (256) envelopes com
  teto de bytes por lote, faz um `Write` do lote e só remove depois do aceite.
  Backoff de 100 ms a 2 s, sempre com os mesmos bytes. `sink.workers` sai.
- **Ring SQLite:** igual à v1 (group commit, `fsync` conforme
  `ring.sqlite.synchronous`). `stage=ring` significa `200` após o `COMMIT`, com
  teto de slots e bytes.
- **Ring Redis (decisão 4):**
  - Estrutura: um stream compartilhado (`ring.redis.stream`), um consumer group
    e um consumidor por AgentUUID.
  - Capacidade: o teto de slots e bytes é **global por stream**. Um script Lua
    confere `XLEN` e um contador de bytes e faz o `XADD` de forma atômica; o
    `200` sai quando o script retorna (a durabilidade é a do `appendfsync` do
    Redis). A remoção é `XACK` + `XDEL` + decremento do contador, também em Lua.
  - Recuperação e ordem: `XAUTOCLAIM` com idle mínimo configurável. A ordem FIFO
    vale **por instância**, não globalmente; a dedup por `(uuid, step)` cobre as
    reentregas.
  - Detecção de fila no `auto`: o "ring vazio?" usa a profundidade global em
    cache, atualizada pela descarga.
  - Falha do Redis do ring: 503 (`auto` tenta o sink antes).
- O dump de emergência só existe com ring `memory`: os rings `sqlite` e `redis`
  já guardam o conteúdo fora do processo.
- **Respostas:** `200` com `X-LogHub-UUID` e o corpo
  `{uuid, mode:"fast", status:"accepted", stage}` montado de um template.
  `503` com `Retry-After: 1` para ring cheio, semáforo cheio e falha em
  `stage=sink` (corpo JSON com `uuid` quando ele já existe). As rejeições antes do
  envelope continuam em `text/plain`. Sem alias `X-Event-Id`. `429` só para o
  rate limit, que continua desligado por padrão. A checagem de ring cheio antes
  do corpo, pelo `Content-Length`, responde 503 sem UUID e com dreno.
- **Limites:** `WEBHOOK_MAX_BODY` 512 KiB (413 sem envelope), 65536 slots,
  64 MiB, drain de 5 s, `high_water` 0.80. Os parâmetros novos aceitam inteiro
  puro em ms ou s. O boot calcula o pior envelope e recusa (exit 1) se ele não
  couber no ring, no `max_payload` do NATS ou no `max_allowed_packet` do MySQL.
- **NATS:** subject `loghub.{space}.webhook`. O stream de exemplo cobre o
  subject do Workspace e **nunca** usa `loghub.>`, que capturaria `_ctl`. O boot
  valida o subject contra o stream.
- `LOGHUB_EB_URL` é aceito: o esquema da URL escolhe `sink.type`, e um conflito
  com `sink.*` explícito dá exit 1.

### Operação

- Descartes (503 e sobras) registram o UUID por um canal de capacidade fixa com
  goroutine própria. Quando o canal enche, a linha não sai e é contada em
  `log_dropped_total`. O invariante 9 (nenhum log síncrono por requisição)
  continua.
- **SIGHUP** passa a recarregar também `WEBHOOK_SINK_TIMEOUT_MS`, `high_water`,
  `WEBHOOK_DRAIN_TIMEOUT` e `headers.redact`. `stage`, modo e tamanho do ring e
  `max body` exigem restart. A identidade fica congelada.
- **HTTP:** HTTP/1.x e h2c por *prior knowledge*, como hoje. Timeouts,
  `SO_REUSEPORT` e os limitadores de conexão continuam.
- **Métricas `whf_`:**
  - ocupação do ring em slots e bytes;
  - `accepted_total{stage}`;
  - 503 por motivo;
  - erros e latência da escrita (`path=inline|drain`);
  - fallback por motivo;
  - `dropped_total{reason}`.
- **Docker:**
  - Um compose por combinação válida, com o `stage` vindo do `.env`.
  - `AGENT_NAME=go-webhook-fast-server` e `DATADIR=/data`; `hostname:` em cada
    serviço; `WORKSPACE` é um tenant, não um ambiente.
  - Um volume por instância, nunca compartilhado entre réplicas.
  - Continuam: `/app`, `WORKDIR /data`, usuário 65532, `read_only`.
- **Dependências de produção (7):** `rueidis`, `modernc.org/sqlite`, `pgx/v5`,
  `go-sql-driver/mysql`, `nats.go`, `go-loghub-uuidv7` (substitui a
  `go-loghub-uuid`) e `go-loghub-ident` (nenhuma traz transitiva nova). Só para
  teste: `google/uuid` e `jsonschema/v6`.
- **Testes:**
  - Contratos com os schemas JLH e webhook-fast (com `AssertFormat`), mais uma
    porta em Go das checagens semânticas do `validate.py`.
  - Fuzz: `FuzzResolve` estendido ao `Forwarded` e ao PROXY protocol,
    `FuzzEnvelope`, `FuzzQuery`/`FuzzRequestURL` e `FuzzRead` do dump.
  - Matriz `stage` × sink enxuta, mais um teste em subprocesso para a ordem de
    boot e os códigos 100–114.
  - `-race` sempre.

---

## 3. Pontos a fechar na SPEC (minha proposta)

| Ponto | Proposta |
|---|---|
| Sink padrão | `nats` em modo `jetstream`, porque o LogHub é o eventbus. A v1 tinha `redis` como padrão de produção. |
| Escrita direta em sinks SQL | O pgx e o driver MySQL fecham a conexão quando o contexto é cancelado no meio do comando, e um prazo de 50 ms geraria rotatividade de conexões. Proposta: com `sink=postgres`/`mysql`/`sqlite`, o `auto` se comporta como `ring` (com `WARN`), e o `stage=sink` usa `sink.op_timeout` em vez de `WEBHOOK_SINK_TIMEOUT_MS`. |
| Ring Redis | Nome padrão do stream e do grupo, idle mínimo do `XAUTOCLAIM` (proposta: 30 s) e a chave do contador de bytes. |
| Compose | Quais combinações ring × sink ganham arquivo: são 4 × 6, menos `none`+`none` (recusada) e as duas que degradam (`sqlite`+`sqlite`, `redis`+`redis` no mesmo servidor). |

---

## 4. Riscos conhecidos

- **Duplicatas.** Uma escrita direta que estoura os 50 ms pode ter sido aceita,
  e a descarga reenvia. Redis `XADD *` e NATS core não deduplicam, então os
  consumidores precisam ser idempotentes por `(uuid, step)`.
- **`auto` preso no ring.** Se a descarga única for mais lenta que a entrada, o
  ring nunca esvazia e tudo termina em 503. A descarga precisa de lote e de
  métrica de vazão.
- **Posse de buffer.** No `auto`, os mesmos bytes podem estar ao mesmo tempo com
  o cliente do sink (escrita expirada) e no ring. Faltar o `Taint` em qualquer
  caminho de erro põe o corpo de um cliente no evento de outro.
- **Contenção.** O mutex do ring, somado ao "ring vazio?" a cada requisição do
  `auto`, pode virar gargalo; o canal da v1 contabilizava sem lock.
- **Ring Redis compartilhado.**
  - A capacidade global depende do contador de bytes. Um processo que morre
    entre o `XADD` e o `XACK` é coberto pelo `XAUTOCLAIM`, mas o contador precisa
    ser reconciliável (por exemplo, recalculado por `XRANGE` sob comando
    administrativo).
  - A ordem global não é garantida.
- **IP do cliente.** Com a cadeia fixa (decisão 14), um proxy que sanitiza só um
  dos três cabeçalhos deixa os outros forjáveis. Dito em destaque na SPEC §5.2
  e no README do Docker. A precedência `X-Forwarded-For` antes de `X-Real-IP`
  fica: invertê-la abriria o furo simétrico no proxy que anexa ao
  `X-Forwarded-For` e repassa o `X-Real-IP` do cliente.
- **Desenvolvimento no macOS.** Sem `DATADIR` absoluto o boot sai com 100, e um
  hostname com `_` sai com 109. Os comandos do AGENTS e do TESTING mudam.
- **Réplicas com o mesmo volume** compartilham o AgentUUID: `from` duplicado no
  eventbus, sem aviso.
- **Testes que passam vazios.** Cerca de 26 asserções fixam `X-Event-Id` ou 429,
  e os helpers de teste devolvem zero quando a chave falta: um teste portado sem
  cuidado pode passar sem provar nada.
- **SIGHUP sem efeito em deploy só por env**, que é o caso de todos os compose:
  `headers.redact` só recarrega com um INI montado.
- **Driver futuro.** Não existe um `go-loghub-driver`: o layout escolhido agora
  para Redis e Postgres pode divergir de um driver que venha depois.

---

## 4a. Porte das correções do `go-webhook-nats-server` (2026-10-11)

A revisão independente do N (2026-10-10) achou defeitos que valem aqui; a lista
"A portar" da entrada N do `CHANGELOG.md` da raiz foi portada sem decisão nova
do usuário. Contestáveis, como tudo neste arquivo:

- **Ciclo de vida e emergência** (vindos do P, que nasceu daqui): log próprio
  da recusa fatal no boot; `ring.Commit` antes do `Release` na recusa `logic`;
  um `drain_timeout` só para as fases 3 e 4; dump sob `.emg-…bin.tmp` e `link`,
  com o *fallback* de `rename` sem *hard link* e o `fsync` do diretório como
  melhor esforço; o boot remove o temporário gêmeo de um dump e informa o de um
  dump interrompido; só arquivos regulares são dumps. O formato `.bin` não
  mudou.
- **Entrega parcial** (`Error.Delivered`): os workers e a escrita direta de
  `ring=none` contam como perdidas só as mensagens recusadas.
- **`sink=nats`** (as correções da revisão do `go-syslog-nats-server` que este
  sink não tinha): recusa de uma mensagem sem encerrar o lote; veredito de
  permissão negada como FATAL — aqui **toda** violação de permissão conta,
  porque esta conexão só carrega dados (o N e o SN excluem o subject do
  beacon, que este módulo não tem); `no_ack`, JetStream desligado, plano de
  controle (`loghub._ctl.beacon`), teto de 32 MiB do *file store* e
  `max_bytes` com `discard: new` conferidos; o contrato do stream em toda
  checagem, por casamento local de padrões (o servidor só é perguntado quem é o
  dono do subject quando o boot falha, para nomeá-lo); recusas de
  autenticação no `CONNECT` fatais; logs de conexão só com esquema e host.
- **Config:** cada URL de `sink.nats.urls` conferida na validação, sem eco;
  *userinfo* na URL junto de `username`/`password`/`token`/`credentials` é
  `exit 1`; o dump e o log "sink ready" mostram a URL com a parte de usuário
  mascarada. Uma URL sem esquema (`host:4222`) continua aceita, como o `nats.go`
  a aceita.

Decididas na revisão independente do porte (mesmo dia), contestáveis:

- **"Sem respondedores" na primeira checagem é FATAL**, sem perguntar
  `AccountInfo`: o cliente não distingue o JetStream desligado de um JetStream
  fora de alcance, e a sonda a `$JS.API.INFO`, negada a um papel mínimo,
  virava veredito permanente. Uma folha com o *hub* fora no boot sai com 3 na
  hora em vez de depois do `boot_timeout` — o mesmo laço de reinício, mais
  curto. O N e o SN perguntavam; o SN foi corrigido junto, o N fica para o
  porte de volta.
- **Um subject de outro `space` retirado do stream depois de o backlog drenar é
  esquecido, não FATAL** (o N o trata como FATAL): só o backlog do boot publica
  ali, e o erro do `cover` manda o operador fazer exatamente isso.
- **O ring de memória é selado antes do último `Drain`** do shutdown, e os
  parsers de INI e de flags não ecoam valores — os dois também no P e no W.

Da revisão do porte de volta ao N (mesmo dia), portado com o N como
referência, também no W: o teste do `Seal` age **dentro** do `Drain`
(`App.wrapRing`, só no ring de memória, nulo no binário), não depois dele, que
não distinguia um `Seal` antes do `Drain` de um entre os dois; o teste da
barreira roda 3000 rodadas; e o parser de flags só nomeia uma flag que este
servidor conhece e não cita nada depois de um valor. Fica, decidida como no N,
a brecha da cauda com traço depois de um valor não vazio (`--x=a -b`):
escondê-la esconderia os erros de digitação (`TESTING.md` §8 item 41).

Fica de fora, com o porquê: o TLS do `sink=nats` continua (este módulo tem TLS;
o `TLSCertCB` que recusa é do N e do SN, que não têm), e a reingestão que refaz
item a item uma recusa `logic` (`TESTING.md` §8 item 38) não estava na lista.

## 5. Ordem de entrega

1. `docs/SPEC.md` reescrita, mais `AGENTS.md`, `CLAUDE.md` e `TESTING.md` novos.
2. Código em fatias:
   1. envelope e identidade;
   2. HTTP e borda;
   3. ring, stage e descarga;
   4. sinks;
   5. config, Docker e métricas.
3. Testes com `-race` por pacote e suítes de integração.
4. Verificação independente: um sub-agente só de leitura por área.
5. Registro breve no `CHANGELOG.md` da raiz (letra F).
6. Commit único, só com o seu "sim".
