# AGENTS.md — go-relplog-nats-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-relplog-nats-server/         <- VOCÊ ESTÁ AQUI (módulo Go independente, letra RN)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-relplog-nats-server
    cmd/go-relplog-nats-server/
    internal/
    docs/DECISOES.md              as decisões de 2026-10-13 que definiram este corte
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, docker-compose.yml (servidor + NATS + emissor rsyslog), conf/relplog.ini
  go-relplog-server/              <- o pai RELP (R): PAI do RELP, nunca dependência
  go-syslog-nats-server/          <- o pai do molde (SN): ring, sink NATS, beacon, dump
  go-relplog-pg-server/           <- o irmão (RP): o mesmo corte, no molde do go-syslog-pg-server
  go-syslog-server/, go-syslog-*, go-webhook-*/
```

Este é o servidor **RELP** do ecossistema **LogHub** com destino único no
**NATS**, criado em 2026-10-13 como corte do `go-relplog-server` no molde do
`go-syslog-nats-server`. Recebe as mensagens que o `rsyslog` envia com o
`omrelp`, **em claro**, confirma **cada uma** só depois de ela estar no ring em
memória, converte cada uma num envelope **JLH** `type=syslog` (com
`x-transport: "relp"`) e o publica em `loghub.{space}.syslog` (JetStream por
padrão, ou core). É um Bridge-IN: `step = 0`.

## 2. As fontes normativas mandam, e a SPEC diz onde não

- Skills do servidor MCP `loghub-mcp`: `loghub-json-schema` (envelope, subject,
  plano de controle), `loghub-type-syslog-schema` (payload e algoritmo de
  conversão), `go-loghub-uuidv7` e `go-loghub-ident`. Cópias dos schemas e dos
  vetores dourados ficam em `internal/jlhcheck/loghub/`.
- O **comportamento do cliente RELP** é o do `librelp` (`rsyslog/librelp`,
  `src/`). SPEC §5.4 diz o que cada resposta significa para ele e cita o arquivo
  de onde isso vem; antes de mudar uma resposta, releia o código do cliente.
- `docs/SPEC.md` §1.3 lista os **desvios deliberados** (D2–D9; D1 não se aplica
  ao RELP). Fora deles, divergir de uma skill é **bug**.
- `docs/SPEC.md` está congelada: discrepância entre código e spec é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite a spec por seção (`§5.7`). **A numeração é a do
  `go-relplog-server`**: uma seção removida mantém o título com uma nota.
- `docs/TESTING.md` registra as lacunas com honestidade. Fechou uma, atualize;
  achou outra, acrescente.

## 3. Os pais são referência, não dependência

`../go-relplog-server/` (R) é de onde vieram o RELP, a config e a app;
`../go-syslog-nats-server/` (SN) é de onde vieram o ring, o sink NATS, o
beacon, o dump JSON Lines e a forma do corte. **Nada é importado de lá**, nem do
irmão `go-relplog-pg-server`, nem de nenhum outro módulo do monorepo. Uma
correção feita aqui ou num dos pais **vale para os outros** — confira-os e
registre no `CHANGELOG.md` da raiz o que ficou "a portar". A conferência das
correções dos dois pais, item a item, está em `docs/DECISOES.md` §2.3.

## 4. Fronteira de responsabilidade

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| Outras pastas do monorepo | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto** | o nível do monorepo, e só com pedido explícito do usuário |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-relplog-nats-server/vX.Y.Z`.

## 5. O modelo mental

### Um ring, um sink

- **RING**: só `memory`. **SINK**: só `nats` (`jetstream` ou `core`).
- Não há chaves `ring.mode` nem `sink.type`; o log de boot e o
  `relplog_config_info` ainda dizem `memory`/`nats`. As interfaces `ring.Ring` e
  `sink.Sink` ficam: são a costura dos testes.

### Uma conexão NATS, dois planos

Os workers publicam os dados (JetStream, `PubAck`) e o beacon publica o
heartbeat em `loghub._ctl.beacon` (NATS core) **na mesma conexão**. O stream
nunca cobre o plano de controle, e uma falha do beacon nunca toca a saúde do
sink.

### Cru no ring, conversão no dreno (SPEC §2.2)

A recepção só faz moldura RELP, truncamento, **uma** amostra de relógio, os
testes de vazia, sink DOWN e ring cheio, e o `uuid`. **Ela não lê a mensagem**
(nem o PRI: não há filtro). O item no ring é **cabeçalho + cru**, sem JLH; a
conversão roda no worker de dreno. **O JLH é função pura de cabeçalho + cru +
config de conversão** (SPEC §3.5): toda chave de `[syslog]` exige restart, e um
retry reenvia os mesmos bytes.

### O dump é JLH pronto, e volta sem conversão (SPEC §12)

Um JLH por linha (`emg-*.jsonl`); a reingestão lê em fluxo e publica cada linha
como está, depois de `emergency.ParseLine`, a única porta entre o disco e o
stream.

### O que cada resposta RELP significa (SPEC §5.4) — a regra que mais importa

| Resposta | O `librelp` | Use para |
|---|---|---|
| `200 OK` | apaga da fila: entregue | entrou no ring; vazia |
| `500 ...` | **destrói a mensagem e não retransmite**, e quebra a sessão (as vizinhas da janela voltam duplicadas) | só a guarda de origem não endereçável e os erros de protocolo do cliente (`syslog` antes do `open`, segundo `open`, comando desconhecido). **No fluxo normal, nenhum** |
| nenhuma + sessão fechada (`serverclose` ou socket) | reconecta e **retransmite** o não confirmado | ring cheio ou sink DOWN além de `relp.backpressure_timeout`; shutdown |

Nunca responda `500` ao que vai passar sozinho: é perda na origem. Nunca feche um
socket com respostas decididas no buffer: é duplicata. Uma recusa `logic` do
NATS chega **depois** do `200 OK` (é do worker, contada em `sink_logic`): não
tente devolvê-la ao emissor.

### A sessão (SPEC §5.5–§5.8)

Um leitor por conexão; até `relp.window` transações em voo, cada uma numa
goroutine; um escritor que só vê transações decididas; um teto **global** de
bytes das cópias em voo (`relp.inflight_max_bytes`), adquirido antes da cópia.

## 6. Mapa do código

```
cmd/go-relplog-nats-server/main.go  flags que encerram, lhident.Initialize(), internal/app
internal/
  app/        montagem, boot, shutdown, SIGHUP, reingestão, admin; XTransport
  config/     registry único -> INI + flags + env + --help + dump; LOGHUB_EB_URL
              (só nats://); removed.go (as chaves e variáveis do R que saíram)
  control/    heartbeat beacon, e só ele
  ident/      Identity congelada e gramática de space              [lógica pura]
  jlh/        escritor JSON por append, Event, envelope, UUID, Payload [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH (+ x-transport)  [lógica pura, fuzz]
  item/       Item, Head, pools (sem codec)                         [lógica pura]
  jlhcheck/   oráculo de teste: schemas + regras semânticas da skill [só em teste]
  receiver/   RELP: relpframe (moldura), relp (sessão, janela,
              respostas), budget (teto em voo), stream (conexões, PROXY,
              shutdown), tcp, proxy, limits, ingest                 [fuzz]
  ring/       interface + memory
  sink/       interface + nats, workers, chunk, health, instrumented
  emergency/  dump JSON Lines, ParseLine, leitor em fluxo           [fuzz]
  obs/        métricas Prometheus próprias + slog
```

## 7. Comandos

Todos a partir desta pasta.

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**. Sem `RELPLOG_TEST_NATS_URL`, a integração da app,
a do sink real e a do beacon real pulam: rode `docs/TESTING.md` §6 antes de
considerar uma mudança pronta.

Fuzz, obrigatório antes de release (10 min cada; 60 s no dia a dia):

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$ -fuzztime=60s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$      -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$   -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$   -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$    -fuzztime=60s
```

NATS real e o rsyslog de verdade: `docs/TESTING.md` §6 e §7. As variáveis são
`RELPLOG_TEST_NATS_URL` (com JetStream), `RELPLOG_TEST_NATS_NOJS_URL` (sem),
`RELPLOG_TEST_NATS_PERM_URL` (permissões negadas), `RELPLOG_TEST_NATS_TLS_URL`
(um servidor com `tls {}`) e `RELPLOG_TEST_EMITTERS=1` (Docker).

Executar localmente (o hostname do macOS não pode ter `_`, ou o
`go-loghub-ident` sai com um código entre 100 e 114; o stream precisa existir):

```bash
mkdir -p /tmp/rlnats-data
DATADIR=/tmp/rlnats-data AGENT_NAME=go-relplog-nats-server HOSTNAME=dev-mac \
  go run ./cmd/go-relplog-nats-server --listen.tcp=127.0.0.1:20514 \
  --sink.nats.urls=nats://127.0.0.1:4222 --admin.bind=127.0.0.1:9103
```

e, num rsyslog: `action(type="omrelp" target="127.0.0.1" port="20514")`.

## 8. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**. As únicas strings em português no código são os textos
  normativos de `parse.error` (SPEC §6.6), que são dado.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- **Dependências: três diretas em produção** (SPEC §17.3), as do SN. O RELP não
  tem biblioteca: é stdlib. Dependência nova exige justificativa na spec.
- **Config:** o registry é a fonte única. Um parâmetro novo entra só em
  `internal/config/registry.go`. Env `RELPLOG_<SEÇÃO>_<CHAVE>`; `[syslog]` mantém
  `SYSLOG_<CHAVE>` (o nome da skill). Uma chave do R que saiu entra em
  `removed.go`, ou some em silêncio no env.
- **Métricas:** prefixo `relplog_`, os nomes do R; cardinalidade fixa; nenhum
  label vindo do emissor. Uma série que nunca pode subir não é exposta.
- **Identidade:** `lhident.Initialize()` só no `main`, uma vez. Pacotes recebem
  `ident.Identity`; testes que precisam da biblioteca usam o binário.
- **Escrita em disco:** tudo sob `server.data_dir` (`DATADIR`), sem exceção.
- **Docker:** prefixo `rlnats-` em projeto, contêineres e volumes; portas de host
  distintas das do R (`rls-`), do SN (`slnats-`) e do RP (`rlpg-`).

## 9. Armadilhas que já custaram bugs reais

### Do RELP (do R: standard, especificação extraída e a leitura do `librelp`)

- **`500` é perda na origem.** O `librelp` destrói a mensagem que recebe
  qualquer código diferente de `200` (`scrsp.c`). Ring cheio, sink DOWN e
  shutdown **seguram ou fecham a sessão**; nunca `500`.
- **O `close` do socket descarta respostas decididas.** O `Close` do transporte
  só vence o prazo de **leitura**; o escritor esvazia, e só então o socket fecha.
- **`DATALEN = 0` seguido de espaço é frame inválido para o `librelp`.** O
  servidor escreve `0 serverclose 0\n`, nunca `0 serverclose 0 \n`; o leitor
  aceita as duas grafias porque o parque manda as duas.
- **O leitor arma só o prazo de leitura** (`armReadDeadline`); o escritor é o dono
  do prazo de escrita.
- **A cópia da transação vem depois da vaga na janela e no teto em voo.** Copiar
  antes chega a `window + 1` cópias por conexão, fora de todo teto.
- **Nunca alocar pelo `DATALEN` declarado**; o buffer cresce pelos bytes que
  chegam e é solto depois de um frame grande.
- **Uma transação de um par que sumiu é abandonada**, nunca enfileirada depois:
  a retransmissão a traria de novo com outro `uuid`. E o fim da sessão vence
  toda decisão: confira o contexto depois de cada espera e logo antes do
  `Enqueue` (só o que está no ring é "decidido").
- **O `close` do cliente espera as transações em voo** antes da resposta dele.
- **Todo frame `syslog` lido entra na reconciliação**, inclusive o que nunca
  ganhou goroutine.
- **Testes de janela correm**: as transações de uma janela entram no ring em
  qualquer ordem. Libere pelo conteúdo, nunca pela posição.

### Do NATS e do dump (do SN, com as correções do R e desta criação)

- **`nats.go`: `WithRetryAttempts(0)`, `ReconnectBufSize(-1)`,
  `WithPublishAsyncTimeout(sink.op_timeout)`.** A retentativa interna relê
  `msg.Data` depois de o `Write` voltar; o buffer de reconexão é uma fila
  escondida fora do ring; um futuro sem prazo segura o buffer do pedaço para
  sempre. `TestNATSLibraryNeverRepublishes` pega a primeira.
- **O JLH vai em `Data`, nunca num header NATS** (teto de 64 KiB de headers).
- **`Nats-Msg-Id` é `<uuid>.<step>`**, nunca o `uuid` puro.
- **Nunca crie nem altere um stream.** Ele vem de `docker/nats/stream.json`; o
  servidor só o valida, no boot **e em toda checagem**.
- **Sem TLS, de propósito — e o `nats.go` tenta TLS sozinho.** O `TLSCertCB`
  que recusa (e `errNATSServerRequiresTLS` fatal) é o que impede. Depois do
  boot, a recusa só aparece no `LastError` com o cliente `RECONNECTING`: o
  `Health` passa pelo `escalate` também nesse estado. Não ligue nada de TLS no
  cliente.
- **`ErrConnectionClosed` de um *flush* não é sempre fatal**: com a conexão só
  reconectando, é `network` (`flushErr`). E um *flush* que atravessa uma
  reconexão não confirma o pedaço (o `writeCore` compara as reconexões).
- **Permissão negada só chega assíncrona.** O veredito (`ErrPermissionViolation`
  num subject de dados, na `$JS.API` ou no `_INBOX.`) volta em todo `Write` e
  `Health`; em `core`, o `LastError()` lido depois do *flush*, e uma negação
  **nova** do beacon durante o pedaço o torna incerto (negação mascarada). A
  negação no subject do beacon **não** é veredito.
- **`10077` não é sempre fatal**: stream cheio com `discard: new` ou fechando é
  `network`. `10158`, `10167` e todo 429 também.
- **A linha do dump é reingerida SEM conversão**, e `ParseLine` é a única porta:
  se você "simplificar" uma regra dela, pergunte-se o que um consumidor passaria
  a receber.
- **O dump é lido em fluxo, nunca inteiro**, e o prazo da reingestão é um
  `boot_timeout` **sem progresso**.
- **O dump só está salvo depois do `link` e do `fsync` do diretório**; sem *hard
  link*, `rename` depois de conferir o nome livre. O `fsync` do diretório decide
  o dump. Depois do `unlink` de uma remoção, a falha do `fsync` (ou do `open` do
  diretório) é `ErrNotDurable`, não "cannot remove".
- **Uma perda é contada uma vez.** O aborto e o dump falho trocam o contador por
  `Swap` e as passagens dele (a do dump inclusive, `settleDump`) são ordenadas
  por `lossMu`: fora dele, um dump que via `aborted` antes de o aborto tomar o
  contador contava `emergency_failed` no lugar de `abort`; um dump contado como
  perdido pelo aborto é abandonado (`Store.SetAbandon`), ou voltaria no boot.
- **Shutdown por FATAL tolera o primeiro `SIGTERM`/`SIGINT`**, nunca `SIGQUIT`;
  um `SIGQUIT` tardio chega pelo `quitNow`.
- **Ring cheio é conferido antes do `uuid` e da cópia**, nos **dois** tetos
  (`ring.Memory.FullFor(n)`, com o tamanho truncado); o `Enqueue` continua
  sendo a autoridade.
- **Posse de buffer.** O item é dono do cru; volta ao pool **uma vez**, após a
  confirmação. Num retry ainda é o dono; um futuro JetStream pendente aponta para
  o buffer do pedaço: na dúvida, `Discard`/`Taint`.
- **Lote retido não está no ring.** O shutdown coleta `Workers.Pending()`; o
  `hold` faz `append`, nunca atribuição.
- **Erro desconhecido é `network`, nunca `logic`**; configuração e permissão do
  destino são `fatal`.
- **O beacon nunca toca a saúde do sink**, e o gancho de conexão nunca bloqueia.

### Da conversão e da config (comuns a toda a família)

- **A hora da mensagem não é tempo.** O tempo é a amostra da recepção (`event`).
  Duas leituras de relógio fazem o fato parecer posterior ao envelope.
- **`time.RFC3339Nano` remove zeros**; `strings.ToValidUTF8` junta octetos
  inválidos; `time.Date` normaliza 30/fev; `json.Marshal` troca UTF-8 inválido em
  silêncio. A SPEC §6 diz o que usar.
- **O `\s` do motor de regex não é o do schema** (D6): o oráculo confere o
  conjunto completo.
- **SIGHUP** compara chave recarregável com o que está em execução e chave de
  restart com o boot. Só `[log]` recarrega.
- **Erros de configuração não ecoam a entrada**: nem a URL, nem o trecho que o
  `net/url` cita, nem a cauda de um valor partido pelo shell, nem a linha de um
  segredo que quebrou no INI.
- **Durações zero** são recusadas onde zero não significa "sem limite".
- **Testes portados que passam vazios:** sem um caso de controle válido, uma
  regra nova pode recusar todos pelo motivo errado.
- **Escrita no boot é por pedaço** (`Instrumented.Deliver`).
- **Valores aceitos pela config precisam significar algo para quem os usa.**

## 10. Estado do projeto

Ver `docs/TESTING.md` §8 (lacunas), a ordem de entrega em `docs/DECISOES.md`
§4 e as pendências do código atual em `docs/SPEC.md` §22.1 (corrigida uma, a
regra entra na seção indicada e o item sai de lá).
