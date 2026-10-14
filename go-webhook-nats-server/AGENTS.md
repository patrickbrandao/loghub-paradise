# AGENTS.md — go-webhook-nats-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes de
editar qualquer arquivo.

---

## 1. Onde você está

Você está em `go-webhook-nats-server/`, **uma subpasta** do monorepo
`loghub-paradise/`. Não é um projeto isolado.

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-webhook-nats-server/         <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server
    cmd/go-webhook-nats-server/
    internal/
    docs/DECISOES.md              como o módulo nasceu (2026-10-10) e por quê
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, compose memory -> NATS JetStream, nats/
  go-webhook-pg-server/           <- a BASE: o mesmo corte, com a fila loghub.inbox. Referência
  go-syslog-nats-server/          <- o MOLDE da troca: sink NATS, beacon, LOGHUB_EB_URL. Referência
  go-webhook-fast-server/         <- origem do P e do sink NATS da família webhook. Referência
  tmp/                            <- rascunhos históricos, NÃO use como referência
```

O servidor recebe webhooks arbitrários da internet, encapsula cada requisição num
envelope **JLH do LogHub** (`type=webhook`, `docs/SPEC.md` §3), guarda-o num ring
em memória e o **publica** no NATS, em `loghub.<space>.webhook` — num stream
JetStream com `PubAck` por padrão, `core` opcional. Emite o heartbeat beacon em
`loghub._ctl.beacon`. É um *bridge-in* HTTP do LogHub (`step = 0`), rápido e com
dependências mínimas (três em produção), **sem TLS até o NATS**. Escala
horizontalmente; instâncias não compartilham estado.

Fontes normativas (skills do servidor MCP `loghub-mcp`): `loghub-json-schema`
(envelope, subject `loghub.{space}.{type}`, `Nats-Msg-Id`, plano de controle),
`loghub-type-webhook-fast` (payload `webhook`, respostas), `go-loghub-uuidv7` e
`go-loghub-ident`. Cópias dos schemas em `internal/jlhcheck/loghub/`.

**Este módulo nasceu em 2026-10-10 como cópia do `../go-webhook-pg-server/`**,
com o PostgreSQL trocado pelo NATS "como foi feito no `go-syslog-nats-server`"
(`docs/SPEC.md` §0, `docs/DECISOES.md`). Fora do destino, os pacotes e arquivos
têm o mesmo nome e o mesmo conteúdo dos do P, e as seções da SPEC a mesma
numeração (a do fast; as retiradas ficam no lugar, marcadas). São módulos
independentes, **sem código compartilhado**: uma correção num pacote de mesma
origem é portada à mão; `diff -ruN -x .DS_Store ../go-webhook-pg-server .`
mostra as diferenças.

## 2. Fronteira de responsabilidade

**Seu escopo é esta pasta e nada acima dela.**

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| `docs/SPEC.md`, `docs/TESTING.md` | você |
| Outras pastas do monorepo (a base, o molde e o fast inclusive) | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto, `.gitignore`** | o nível do monorepo, e só com pedido explícito do usuário |
| Integração contínua, publicação de imagem, releases | agente do monorepo |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-webhook-nats-server/vX.Y.Z`.
**Não edite o P, o SN nem o fast** a partir daqui: correções a portar vão para a
lista da entrada **N** do `CHANGELOG.md` da raiz.

## 3. A especificação manda

`docs/SPEC.md` está marcada como **congelada para implementação**. Ela não é
documentação descritiva do código: é o contrato que o código cumpre.

- Discrepância entre código e spec é **bug no código**, salvo justificativa
  explícita registrada na própria spec.
- Para mudar comportamento: **altere a spec primeiro**, com o porquê, depois o
  código, depois o teste.
- Ao citar a spec em comentário, commit ou resposta, use o número da seção
  (`§8.7.2`, `§9.2`). O código já faz isso e a convenção deve continuar.
- **Nunca renumere.** Seção nova entra no fim da seção-mãe (`§8.7.10` beacon,
  `§8.7.11` `LOGHUB_EB_URL`, invariantes 17–22).
- `docs/TESTING.md` §8 registra as lacunas conhecidas com honestidade. Se você
  fechar uma, atualize a lista. Se descobrir outra, acrescente.
- `docs/SPEC.md` §18.1 lista os defeitos que o código ainda tem (pendências
  conhecidas). Ao corrigir um, tire-o de lá; ao achar outro que não vai
  corrigir agora, registre-o ali.

O que saiu (SPEC §1.2) não volta sem decisão do usuário registrada em
`docs/DECISOES.md`: TLS até o NATS, os outros sinks e rings, rate-limit, recarga
por `SIGHUP`, subject configurável.

## 4. O modelo mental: um ring, um destino, uma conexão, dois planos

- **RING** — *onde o evento espera* entre ser aceito e ser entregue: `memory`.
  O `200` sai quando o item entrou no canal (`SPEC.md` §2.2).
- **SINK** — *para onde o evento vai*: o NATS (§3.8, §8.7).
  `sink.nats.mode = jetstream` (padrão: `PubAck`, deduplicação por
  `Nats-Msg-Id` na `duplicate_window`) ou `core` (*best effort*: "o servidor
  recebeu"). O servidor **nunca cria nem altera o stream**, só o valida no boot
  (§8.7.5): cobre `loghub.<space>.webhook`, não captura o plano de controle,
  não selado, sem `no_ack`, a maior mensagem cabe.
- **UMA conexão NATS**, aberta preguiçosamente pela primeira checagem de saúde,
  dividida pelos workers (padrão 8) e pelo beacon. Os dados vão por JetStream
  (ou core) em `loghub.<space>.webhook`; o beacon por NATS **core** em
  `<ctl_namespace>.beacon`, nunca num stream (§8.7.10).
- **EMERGÊNCIA** — com o sink FATAL ou o `drain_timeout` estourado, o que o ring
  tem vai para `emg-<agent_uuid>-<unixnano>.jsonl`, um JLH por linha (escrito
  como temporário e ligado ao nome final), e volta no boot seguinte publicado
  **sem conversão**, depois de passar pela porta de §9.2 (§9).

As interfaces `ring.Ring` e `sink.Sink` ficam, com uma implementação cada
(decisão 4 do P). **Nenhuma implementação de ring consulta o sink, e
vice-versa.**

**Compatibilidade com o fast (decisão 3 do P, §0).** Chaves, env `WHS_*` e
métricas `whs_*` são as do fast. Cinco seletores aceitos só com um valor:
`ring.mode = memory`, `sink.type = nats`, `ratelimit.enabled = false`,
`sink.nats.tls = false`, `sink.nats.subject = loghub.{space}.{type}`. Toda outra
chave de recurso retirado é chave desconhecida (`exit 1`) num INI ou numa flag,
e inerte no ambiente. A seção `[loghub]` lê os nomes fixados pela skill:
`LOGHUB_EB_URL`, `LOGHUB_BEACON_INTERVAL`, `LOGHUB_CTL_NAMESPACE`.

## 5. Mapa do código

```
cmd/go-webhook-nats-server/main.go   versão/commit, lhident.Initialize() (UMA vez), internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP ignorado, reingestão, beacon
  config/     registry único -> ini + flags + env + --help + dump; validação; eburl.go
  server/     http.Server, pipeline da requisição, admin, listeners, limites
  clientip/   X-Forwarded-For / X-Real-IP + proxies confiáveis   [lógica pura]
  ident/      Identity congelada e gramática de space             [lógica pura]
  envelope/   escritor JLH, payload webhook, ParseHead, query, pools [lógica pura, fuzz]
  jlhcheck/   oráculo de teste: schemas + regras semânticas       [só em teste]
  ring/       interface Ring + memory.go
  sink/       interface Sink + nats.go, workers.go, health.go
  control/    heartbeat beacon (loghub._ctl.beacon, NATS core)
  emergency/  dump JSON Lines, a porta da reingestão              [fuzz]
  obs/        métricas Prometheus próprias + slog
```

| Pacote | Onde mexer com mais cuidado |
|---|---|
| `app` | ordem do boot (o gancho do beacon antes do `WaitReady`), as cinco fases do shutdown, o `ignoreSIGHUP` logo no início do `run` |
| `sink` | `nats.go`: as opções que não são as padrões, o `TLSCertCB` que recusa, o veredito, a entrega parcial e a contaminação; `workers.go`: posse de buffer e `refusedOf` |
| `config` | o registry é a fonte única; os cinco seletores; `EnvName` dos `LOGHUB_*`; a URL nunca ecoada |
| `server` | ordem do pipeline em `handler.go` |
| `ring` | contabilidade de bytes e itens |
| `envelope` | pools: liberação dupla é o pior bug possível aqui; e nenhum `0x0A` cru no JLH (§9.1) |
| `obs` | fronteiras de histograma são estáticas, nunca derivadas da config |
| `emergency` | `checkLine` é a **única** porta entre o disco e o stream; o leitor tolera arquivo cortado e linha danificada sem pânico |
| `clientip` | único parser de entrada hostil; cobertura por fuzz é obrigatória |

`internal/` impede import externo acidental. `clientip` e `envelope` são lógica
pura: testáveis sem rede, sem disco e sem NATS.

## 6. Invariantes que nenhuma mudança pode violar

Resumo de `SPEC.md` §17. Se sua alteração conflita com um destes, ela está errada.

1. O `200` nunca sai antes de o item estar no ring.
2. Ring cheio responde `429`. Nunca `200` seguido de descarte, exceto a recusa
   `logic` de uma mensagem pelo destino vivo, logada e contada.
3. Um buffer volta ao pool **uma vez**, e só após confirmação.
4. Evento aceito só sai do ring após confirmação do sink; nunca é descartado para
   liberar espaço.
5. `network.ip.src` (e todo cabeçalho de proxy) nunca vem de header quando o
   peer TCP não está em `proxy.trusted`.
6. Nenhum label de métrica deriva de conteúdo controlado pelo cliente.
7. O corpo nunca é parseado, decodificado, validado ou modificado.
8. `network.meta` `peer-addr` sempre registra a ponta TCP real.
9. Nenhum log síncrono por requisição no caminho padrão.
10. *(retirada: era a regra dos comandos do rueidis)*
11. O envelope JLH é escrito UMA vez no handler; ring, dump e mensagem NATS
    levam os mesmos bytes; subject e headers saem do próprio JLH.
12. `ring` e `sink` são ortogonais.
13. Toda escrita em disco fica sob `ring.data_dir` — `/data` num contêiner.
14. `uuid` e `event` saem de UMA amostra de relógio; o `uuid` de
    `X-LogHub-UUID` é o do envelope publicado, nunca regerado.
15. *(devolvida como 20)*
16. Uma linha do dump é um JLH inteiro seguido de `\n`, e nada mais.
17. O servidor nunca cria nem altera um stream.
18. Erro desconhecido é `network`, nunca `logic`.
19. Não há fila fora do ring: `ReconnectBufSize(-1)`, `WithRetryAttempts(0)`.
20. A maior mensagem cabe no `max_payload`, no `max_msg_size` e no *file store*,
    conferidos no boot.
21. O stream de dados nunca cobre o plano de controle; o beacon vai por core e
    nunca toca a saúde do sink.
22. O cliente NATS nunca faz *handshake* TLS.

## 7. Comandos

Todos rodam a partir desta pasta.

Suíte rápida, a cada mudança:

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**. Sem NATS a suíte roda o ciclo de vida inteiro
contra o destino falso de `internal/app/fakesink_test.go`, e o sink contra um
servidor NATS falso; os testes que precisam de um NATS real pulam.

NATS real — **a prova ponta a ponta precisa dele**; o servidor nunca cria um
stream (a suíte cria os dela). O segundo servidor, **sem** JetStream, prova o
FATAL imediato de SPEC §8.7.5:

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &
WHS_TEST_NATS_URL=nats://127.0.0.1:14222 WHS_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 900s ./internal/sink/ ./internal/control/ ./internal/app/
```

Os casos de permissões, TLS e *pool* pedem ainda `WHS_TEST_NATS_PERM_URL`,
`WHS_TEST_NATS_TLS_URL` e `WHS_TEST_NATS_SERVER` (`TESTING.md` §5).

Se você mexeu em `internal/sink/`, `internal/control/`, na validação de `config`
ou em `docker/nats/`, **rode esta suíte**.

Fuzzing, obrigatório antes de qualquer release, 10 minutos cada (60 s no dia a
dia):

```bash
go test ./internal/clientip/  -run=XXX -fuzz=FuzzResolve      -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=FuzzRead         -fuzztime=10m
go test ./internal/envelope/  -run=XXX -fuzz=^FuzzEnvelope$   -fuzztime=10m
go test ./internal/server/    -run=XXX -fuzz=FuzzServeHTTP    -fuzztime=10m
```

Executar e inspecionar (o stream precisa existir; o servidor nunca cria um):

```bash
go run ./cmd/go-webhook-nats-server --help
go run ./cmd/go-webhook-nats-server --dump-config
nats --server nats://127.0.0.1:14222 stream add LOGHUB_WEBHOOK --config docker/nats/stream.json
# o hostname do macOS não pode ter `_`, ou o go-loghub-ident sai com 109; o
# maior envelope dos padrões (~7,7 MiB) não cabe no max_payload de 1 MiB de um
# nats-server padrão, então o corpo é baixado aqui
HOSTNAME=dev-mac LOGHUB_EB_URL=nats://127.0.0.1:14222 go run ./cmd/go-webhook-nats-server \
  --server.max_body_bytes=256KiB --server.max_header_bytes=16KiB \
  --ring.data_dir=./data --server.bind=127.0.0.1:8080 --admin.bind=127.0.0.1:9101
nats --server nats://127.0.0.1:14222 sub 'loghub._ctl.beacon'     # o heartbeat
```

Docker, a partir de `docker/`:

```bash
docker compose up -d --build      # memory -> NATS JetStream, com o nats-server e o nats-init ao lado
```

## 8. Convenções

**Idioma.** Código, identificadores e comentários em **inglês**. Documentação
(`docs/`, `docker/README.md`, este arquivo) e conversa com o usuário em
**português**. Não misture dentro de um mesmo arquivo.

**Comentários explicam o porquê, não o quê.** O padrão deste projeto é um
comentário que justifica a decisão e diz o que aconteceria com a alternativa
rejeitada. Leia `internal/sink/nats.go` ou `internal/sink/workers.go` antes de
escrever o seu; imite a densidade e o tom.

**Dependências: três em produção, e ponto.** `github.com/nats-io/nats.go` com o
pacote `jetstream` (a API atual; transitivas `nkeys`, `nuid`,
`klauspost/compress`, `golang.org/x/crypto`),
`github.com/patrickbrandao/go-loghub-uuidv7` (UUIDv7 nível 3 por `GenerateAt`)
e `github.com/patrickbrandao/go-loghub-ident` (AgentUUID, Workspace) — estes
dois sem dependência transitiva nenhuma. `github.com/google/uuid` e
`github.com/santhosh-tekuri/jsonschema/v6` são só de teste. Acrescentar uma de
produção exige justificativa na spec (`SPEC.md` §14.3).

**Identidade: `lhident.Initialize()` só no `main`, uma vez.** A segunda chamada
sai com 112, e os testes montam a app dezenas de vezes: por isso a app recebe
um `ident.Identity` (`app.Main(args, initIdentity)`; nos testes,
`runContext(ctx, args, id, hook)`). `ring.data_dir` é exportado como `DATADIR`,
absoluto, antes do `Initialize()` (`SPEC.md` §10.1).

**Um gerador de uuid por processo** (`envelope/jlh.go`, `uuidv7.NewGenerator()`),
e o `uuid` sempre de `NewUUID(t)` com a MESMA amostra `t` do `event` — também
no beacon.

**Configuração: o registry é fonte única.** Um parâmetro novo entra em
`internal/config/registry.go` e daí saem sozinhos o parser de ini, as flags, o
env, o `--help`, o dump e a validação. Nunca adicione um caminho paralelo. Um
nome de ambiente fixado por skill vai em `Param.EnvName`; um segredo, em
`Param.Redact`.

**Métricas: cardinalidade fixa.** Todo label tem conjunto de valores finito e
conhecido em tempo de compilação. Fronteiras de histograma são estáticas e
idênticas em toda instância.

**Escrita em disco: uma raiz só.** O que o serviço grava (a identidade e
`emergency/`) deriva de `ring.data_dir`, e dentro de um contêiner essa raiz é
`/data`. Ver `SPEC.md` §13.6.

**Caminhos fixos do contêiner.** `/app` guarda o binário (dono `root`), `/data`
é a única área gravável, `/etc/webhook` é o INI (ou o `.creds`) em `:ro` e
`/tmp` é tmpfs volátil. O `WORKDIR` é `/data`, e o binário é invocado por
`/app/<nome>`.

**Sem CGO.** `CGO_ENABLED=0` é premissa do Dockerfile e do binário estático.

## 9. Armadilhas que já custaram bugs reais

Registradas em `docs/TESTING.md` §9. As que mais reincidem:

- **`nats.go`: `WithRetryAttempts(0)`, `ReconnectBufSize(-1)`,
  `WithPublishAsyncTimeout(sink.op_timeout)`.** A retentativa interna relê
  `msg.Data` depois de o `Write` voltar (corrida de posse de buffer); o buffer
  de reconexão é uma fila escondida fora do ring, do `429` e do dump; um futuro
  sem prazo segura o buffer para sempre. Se você "simplificar", os testes de
  comportamento não reclamam (só o `TestNATSOptionsNeverAskForTLS` confere o
  `ReconnectBufSize`).
- **Sem TLS, de propósito — e o `nats.go` tenta TLS sozinho.** Diante de um
  `tls_required` no `INFO`, o cliente se promove a TLS com as raízes do sistema.
  O `TLSCertCB` que recusa (e `errNATSServerRequiresTLS` fatal) é o que impede:
  não o remova, e não ligue nada de TLS no cliente. A recusa de `tls://`,
  `ws://` e `wss://` existe na config **e** no sink. Depois do boot o `nats.go`
  **não** relata essa recusa pelo `ReconnectErrCB` (só a falha da discagem
  TCP): ela fica no `LastError`, com a conexão em `RECONNECTING`, e é o
  `Health` que a lê (`noteLastError`).
- **O JLH vai em `Data`, nunca num header NATS** (teto de 64 KiB, `10097`). O
  bloco de headers é fixo: exatamente 257 B no pior caso.
- **`Nats-Msg-Id` é `<uuid>.<step>`, nunca o `uuid` puro**: um processador que
  republique no mesmo stream (`step + 1`) teria o salto descartado.
- **Entrega parcial.** Uma recusa `logic` de **uma** mensagem (`ErrMaxPayload`
  no cliente, `10054` no stream, um item que não mapeia) não encerra o lote:
  `Error.Delivered` diz o que o stream guardou, e os workers e a reingestão
  contam como perdidas só as recusadas. Encerrar o lote na primeira recusa
  deixava as seguintes nunca publicadas e confirmadas.
- **Contaminação.** Todo erro em `jetstream` depois da primeira publicação
  contamina o lote (`Taint`): um futuro pendente aponta para o `Env`. Um erro
  devolvido não quer dizer "ninguém mais lê".
- **Permissão negada só chega assíncrona.** O `ErrorHandler` guarda um veredito
  (`loghub.<space>.webhook`, `$JS.API` ou a assinatura em `_INBOX.`) que o
  `Health` e todo `Write` devolvem como `fatal`; no subject do beacon, não. Em
  `core` o `Write` lê o `LastError()` depois do *flush* (o `-ERR` vem antes do
  `PONG`): sem isso o lote negado era confirmado e perdido. Uma ida e volta que
  falha como `network` também o lê (`escalate`). E o *flush* `core` não
  confirma um lote que atravessou uma reconexão, nem um em que chegou uma
  negação nova de outro subject (`maskedDenial`): o `nats.go` guarda só o
  último `-ERR`.
- **O `10077` tem dois sentidos pela descrição do *store***
  (`storeFailedIsTransient`): cheio ou fechando é `network`, o resto fatal.
  Confira o texto no `server/store.go` do `nats-server` ("store is closed"),
  não na memória: a família casou "store closed" por meses.
- **O contrato do stream vale em toda checagem, não só no boot.** Um stream
  editado por baixo respondia toda publicação com timeout, `network`, para
  sempre, com o sink UP. Não volte a guardar o `validate` atrás do
  `validated`: ele só controla o log e o "sem respondedores" fatal da primeira
  checagem.
- **`ring.Commit` antes do `Release`**, também na recusa `logic`: o ring
  desconta `len(Env)`, que o `Release` zera.
- **`no_ack` e JetStream desligado são fatais no boot, não `network`.** "Sem
  respondedores" na primeira checagem é FATAL nomeando o JetStream; depois de
  uma checagem validada, `network`. Não pergunte `AccountInfo`: ele não
  distingue nada, e o `$JS.API.INFO` negado a um papel mínimo virava veredito
  permanente.
- **Subject de outro `space`** (um dump de outra vida) é conferido com
  `StreamNameBySubject` antes de publicar o lote: fatal nomeando-o. Publicado às
  cegas, era "no responders" até o `boot_timeout`, em laço. Retirado do stream
  depois do dreno, é **esquecido**, não FATAL: estreitar o stream é o que o
  próprio erro manda fazer depois.
- **A linha do dump é publicada SEM conversão, e o NATS não lê o `Data`.**
  `emergency.checkLine` é a **única** porta: cabeçalho estrito, `type=webhook`,
  `level=9`, `step=0`, payload aberto por `{"mode":"fast",`, UTF-8, JSON válido,
  a forma do escritor (`shape.go`: chaves de topo na ordem, nenhuma repetida,
  inteiros curtos, sem literais, só os escapes do serializador, aninhamento
  limitado) e o `uuid` com o instante do `event`. No P a fila recusava parte
  disso; aqui ninguém recusa. Se você "simplificar", pergunte-se o que um
  consumidor receberia. Mudou o escritor (`envelope.AppendWebhook`)? Mude a
  porta junto: `TestGateAdmitsEverythingTheWriterWrites` é quem avisa.
- **O dump só está salvo no nome final.** Escrito sob ele, um dump cortado numa
  fronteira de linha pareceria limpo. Sem *hard link* o `link` falha sempre: o
  fallback renomeia depois de conferir o nome livre. O `fsync` do diretório é
  melhor esforço — nunca apague um dump pronto porque ele falhou.
- **`drain_timeout` é um prazo para as fases 3 e 4 juntas.** Um prazo por fase
  dobrava o pior caso e passava do `stop_grace_period`.
- **O ring é selado antes do último `Drain` do shutdown** (`Seal`, uma
  barreira contra o `Enqueue`): um upload que terminasse depois recebia `200`
  num ring que ninguém mais lia. Não tire o `RWMutex` do `Enqueue`. O teste
  completa o upload **dentro** do `Drain` (`App.wrapRing`): é o que prova a
  ordem.
- **URL com senha é parseada na validação, sem eco** — nem o trecho que o erro
  do `net/url` cita (`invalid port ":Sup3r"` é a senha até a barra); *userinfo*
  na URL junto de `username`/`token`/`credentials` é exit 1 (o `nats.go`
  prefere o da URL). O `ConnectedUrlRedacted` do `nats.go` esconde senha, não
  token: os logs de conexão usam `connectedURL` (esquema e host). Os parsers de
  INI e de flags nomeiam a linha ou a flag, nunca o valor: um argumento com
  forma de flag nunca é valor de outra flag e só é nomeado se for uma flag
  deste servidor, e nada depois de um valor é citado.
- **Recusa fatal no boot tem log próprio** ("the sink refused this server
  permanently"), não "did not become reachable within sink.boot_timeout".
- **Posse de buffer.** O item do ring é dono de `Env`; volta ao pool exatamente
  uma vez, só após confirmação. Num retry o item **ainda é o dono**.
- **Lote retido pelo worker não está no ring**, e o `hold` faz `append`, nunca
  atribuição; o shutdown coleta `Workers.Pending()`.
- **O beacon nunca toca a saúde do sink**, e o gancho de conexão nunca bloqueia
  (roda na goroutine de *callback* do cliente). O gancho é registrado **antes**
  da primeira conexão, ou o beacon dela se perde.
- **Um envelope é uma linha.** Um `0x0A` cru num JLH partiria a linha do dump.
- **`SIGHUP` precisa ser capturado**; `ignoreSIGHUP` é a primeira coisa do `run`.
- **Fronteira de histograma como string**, pré-computada, igual à do fast.

## 10. Estado do projeto

Criado em 2026-10-10 a partir do `go-webhook-pg-server` (SPEC §0). O código
cumpre a spec, salvo as pendências conhecidas de SPEC §18.1, e a suíte passa
com `-race` contra um NATS 2.14 real (com e sem JetStream); o compose foi
exercitado de ponta a ponta (entrega, beacon, `SIGHUP`, FATAL, dump JSON Lines,
reingestão com duplicatas absorvidas). Uma
revisão independente por área, no mesmo dia, achou 22 problemas, todos
corrigidos com teste e mutação (`TESTING.md` §9, `DECISOES.md` §3). Antes de
qualquer deploy real faltam: versionamento (nível acima), integração contínua
com NATS de serviço, fuzz de 10 minutos, calibragem dos fusíveis de saúde e a
aferição dos critérios de aceite de `SPEC.md` §15.5, que **nunca foram medidos**
(`TESTING.md` §7, §8).

Ao portar uma correção do P para cá (ou daqui para lá), comece pelo `diff -ruN`
dos pacotes de mesma origem: `envelope`, `clientip`, `server`, `obs`, `ring`,
`emergency`, `sink/workers.go`, `sink/health.go`. As do sink NATS e do beacon
vêm do `go-syslog-nats-server` (`sink/nats.go`, `control/`). O `emergency` já
não é igual ao do P: tem a porta estreita (`shape.go`); a escrita por
temporário e `link` e os `Leftovers` o P recebeu em 2026-10-11.
