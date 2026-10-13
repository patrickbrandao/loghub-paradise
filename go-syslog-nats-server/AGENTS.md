# AGENTS.md — go-syslog-nats-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-syslog-nats-server/          <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server
    cmd/go-syslog-nats-server/
    internal/
    docs/DECISOES.md              a entrevista de 2026-10-07 que definiu este módulo
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, docker-compose.yml (memory -> NATS JetStream),
                                  nats/ (servidor e stream), conf/syslog.ini
  go-syslog-pg-server/            <- o MOLDE: o mesmo corte, com a fila loghub.inbox. Referência
  go-syslog-server/               <- o PAI: de onde vêm o sink NATS e o beacon. Referência
  go-syslog-standard-server/      <- o antigo, referência do pai
  go-relplog-server/              <- RELP, só especificação
  go-webhook-server/, go-webhook-fast-server/
```

Este é um servidor syslog do ecossistema **LogHub**, criado em 2026-10-07 "no
molde" do `go-syslog-pg-server` (um corte do `go-syslog-server`): recebe UDP,
TCP e unix datagram, converte cada mensagem num envelope **JLH** com
`type=syslog` e o **publica** no NATS em `loghub.<space>.syslog` (JetStream com
`PubAck` por padrão, `core` opcional). Emite o heartbeat beacon em
`loghub._ctl.beacon`. Ring só em memória, emergência em JSON Lines, **sem TLS em
lugar nenhum**, três dependências diretas de produção. É um Bridge-IN:
`step = 0`.

## 2. As fontes normativas mandam, e a SPEC diz onde não

- Skills do servidor MCP `loghub-mcp`: `loghub-json-schema` (envelope, subject
  `loghub.{space}.{type}`, `Nats-Msg-Id`, plano de controle),
  `loghub-type-syslog-schema` (payload e algoritmo de conversão),
  `go-loghub-uuidv7` e `go-loghub-ident`. Cópias dos schemas, dos exemplos
  (inclusive os de `beacon`) e dos vetores dourados ficam em
  `internal/jlhcheck/loghub/`.
- `docs/SPEC.md` §1.3 lista os **desvios deliberados** (D1 moldura por conexão,
  D2 TAG estrita sem timestamp, D3 schema só em teste, D4 nível de UUID fixo,
  D5 sem importação, D6 espaço em branco nos campos `^\S+$`, D7 avisos de
  encoding de SD com texto colado, D8 anos fora de 0001–9999). Fora deles,
  divergir de uma skill é **bug**.
- `docs/SPEC.md` está congelada: discrepância entre código e spec é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite a spec por seção (`§10.7`).
- **A numeração da SPEC é a do pai.** Seção removida mantém o título com a nota
  "Removido no go-syslog-nats-server"; **nunca renumere**, o código cita os
  números. Seção nova entra no fim da seção-mãe, com número novo.
- `docs/TESTING.md` registra as lacunas com honestidade. Fechou uma, atualize;
  achou outra, acrescente.

## 3. O pai e o molde são referência, não dependência

`../go-syslog-server/` tem o sink NATS e o beacon de onde estes saíram, com o
histórico de bugs (TESTING §9 dele). `../go-syslog-pg-server/` tem o corte
(ring só memória, dump JSON Lines, sem TLS, a porta `ParseLine`). Para entender
por que algo é assim, leia lá. **Nada é importado de lá**, e nada é importado de
nenhum outro módulo do monorepo. **Não edite o pai nem o molde** a partir
daqui.

O que saiu (SPEC §1.2) não volta sem decisão do usuário registrada em
`docs/DECISOES.md`: TLS (listener e cliente NATS), os outros sinks e rings, os
subjects por facility/severidade, filtro, rate-limit. O objetivo do módulo é
ser rápido e ter poucas dependências.

## 4. Fronteira de responsabilidade

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| Outras pastas do monorepo (o pai e o molde inclusive) | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto** | o nível do monorepo, e só com pedido explícito do usuário |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-syslog-nats-server/vX.Y.Z`.

## 5. O modelo mental

### Um ring, um sink

- **RING**: `memory`, e só. Não há chave `ring.mode`.
- **SINK**: `nats`, e só. Não há chave `sink.type`. `sink.nats.mode` escolhe
  `jetstream` (padrão: `PubAck`, deduplicação por `Nats-Msg-Id` na
  `duplicate_window`) ou `core` (*best effort*: "o servidor recebeu").

As interfaces `ring.Ring` e `sink.Sink` ficam, com uma implementação cada: são
a costura dos testes. O ring não consulta o sink, e vice-versa. O log de boot e
o `syslog_config_info` dizem `ring_mode="memory"` e `sink_type="nats"`, por
compatibilidade com os painéis do pai.

### Uma conexão NATS, dois planos

Os workers de dreno e o beacon dividem **uma** conexão, aberta preguiçosamente
pela primeira checagem de saúde. Os dados vão por JetStream (ou core) em
`loghub.<space>.syslog`; o beacon vai por NATS **core** em
`<ctl_namespace>.beacon`, nunca num stream. O stream de dados **nunca** cobre o
plano de controle: o boot recusa um que cubra (`loghub.>` é o jeito clássico de
errar).

### Cru no ring, conversão no dreno (SPEC §2.2)

A recepção só faz moldura, truncamento, **uma** amostra de relógio, o estado do
sink e o `uuid`. **Não lê a mensagem**: nem PRI (não há filtro). O item no ring
é **cabeçalho + cru**, sem JLH, e o cabeçalho só existe em memória (não há
codec). A conversão completa e a serialização rodam no worker de dreno.

Consequência: **o JLH é função pura de cabeçalho + cru + config de conversão**
(SPEC §3.5). Toda chave de `[syslog]` exige restart, a referência da inferência
de ano é a amostra do cabeçalho (nunca `time.Now()` na conversão), e um retry
reenvia os mesmos bytes com o mesmo `Nats-Msg-Id`.

### O dump é JLH pronto, e volta sem conversão (SPEC §12)

Em FATAL ou dreno estourado, o ring pendente é **convertido** e escrito como
JSON Lines, um JLH por linha, idêntico ao que o worker publicaria. No boot
seguinte cada linha é **publicada como está**, lida em fluxo; o subject, o
`Nats-Msg-Id` e os `X-LogHub-*` saem do cabeçalho da própria linha. Um dump
sobrevive a uma troca de `[syslog]`.

### O que o transporte confirma

| Transporte | Confirmação | Ring cheio |
|---|---|---|
| TCP | continuar lendo | parar de ler; a janela TCP segura o emissor |
| unix dgram | não existe | descarte, mas o kernel já segurou o emissor |
| UDP | não existe | **descarte contado** |

Não invente confirmação onde o protocolo não tem.

## 6. Mapa do código

```
cmd/go-syslog-nats-server/main.go  flags que encerram, lhident.Initialize(), internal/app
internal/
  app/        montagem, boot, shutdown, SIGHUP, reingestão, admin
  config/     registry único -> INI + flags + env + --help + dump; LOGHUB_EB_URL
              (só nats://, tls:// recusado); removed.go (env do pai -> WARN)
  control/    heartbeat beacon (loghub._ctl.beacon, NATS core)
  ident/      Identity congelada e gramática de space              [lógica pura]
  jlh/        escritor JSON por append, Event, envelope, UUID, Payload [lógica pura]
  convert/    syslog -> payload type=syslog -> JLH                  [lógica pura, fuzz]
  item/       Item, Head, pools (sem codec)                         [lógica pura]
  jlhcheck/   oráculo de teste: schemas + regras semânticas da skill [só em teste]
  receiver/   udp (pktinfo), unix, tcp, moldura, proxy, ingest      [fuzz]
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

O `-race` **não é opcional**. Sem `SYSLOG_TEST_NATS_URL` a integração pula: o
NATS é o único destino, então a prova ponta a ponta precisa dele.

```bash
# contêiner lh-nats do host de desenvolvimento (JetStream), ou docs/TESTING.md §6;
# o segundo servidor, SEM JetStream, prova o FATAL imediato de SPEC §10.7
go run github.com/nats-io/nats-server/v2@v2.14.6 -p 14223 &
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 SYSLOG_TEST_NATS_NOJS_URL=nats://127.0.0.1:14223 \
  go test -race -timeout 900s ./internal/sink/ ./internal/control/ ./internal/app/
```

Vetores dourados e contratos dos schemas:

```bash
go test -race -run 'Golden|Schema|Corpus' -v ./internal/convert/
```

Fuzz, obrigatório antes de release (10 min cada; 60 s no dia a dia):

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$      -fuzztime=60s
go test ./internal/convert/   -run=XXX -fuzz=^FuzzSD$           -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$        -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrameOracle$  -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$        -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$         -fuzztime=60s
```

O `^…$` importa: `-fuzz=FuzzFrame` casa também `FuzzFrameOracle`, e o Go recusa
dois alvos. Não há `FuzzHead` (o codec do cabeçalho foi apagado) nem os alvos do
`jsonb` do molde.

Executar localmente (um `DATADIR` relativo é resolvido a partir do diretório de
trabalho antes do `Initialize()`, SPEC §13.1 passo 2; o hostname do macOS não
pode ter `_`, ou o `go-loghub-ident` sai com um código entre 100 e 114). O
stream precisa existir: o servidor nunca cria um.

```bash
nats --server nats://127.0.0.1:14222 stream add LOGHUB_SYSLOG --config docker/nats/stream.json
mkdir -p /tmp/slnats-data
DATADIR=/tmp/slnats-data AGENT_NAME=go-syslog-nats-server HOSTNAME=dev-mac \
  LOGHUB_EB_URL='nats://127.0.0.1:14222' \
  go run ./cmd/go-syslog-nats-server --listen.udp=127.0.0.1:5181 \
  --listen.tcp=127.0.0.1:5181 --admin.bind=127.0.0.1:19181
printf '<13>1 - - app - - - teste' | nc -u -w1 127.0.0.1 5181   # o logger do macOS não tem -n
nats --server nats://127.0.0.1:14222 sub 'loghub._ctl.beacon'     # o heartbeat
```

## 8. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**. As únicas strings em português no código são os textos
  normativos de `parse.error` (SPEC §6.6), que são dado.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- **Dependências: três diretas em produção** (SPEC §17.3): `nats.go` (com as
  transitivas `nkeys`, `nuid`, `klauspost/compress`, `golang.org/x/crypto`),
  `go-loghub-ident`, `go-loghub-uuidv7`. `jsonschema/v6` só em teste; nada do
  PostgreSQL (o `pgx` do molde saiu). Nada de `golang.org/x/net` (PKTINFO é
  `syscall`) nem `golang.org/x/text` (charset é tabela própria). Dependência
  nova exige justificativa na spec — e vai contra o objetivo do módulo.
- **Config:** o registry é a fonte única. Um parâmetro novo entra só em
  `internal/config/registry.go`. Prefixo `SYSLOG_` e nomes iguais aos do pai
  para as chaves comuns; uma chave do pai que não existe aqui vai para
  `removed.go` (aviso no env).
- **Métricas:** prefixo `syslog_`, cardinalidade fixa; nenhum label vindo do
  emissor; série que nunca sobe não é exposta.
- **Identidade:** `lhident.Initialize()` só no `main`, uma vez. Pacotes recebem
  `ident.Identity`; testes que precisam da biblioteca usam `TestMain`.
- **Escrita em disco:** tudo sob `server.data_dir` (`DATADIR`), inclusive a
  identidade. Exceção única: o socket `listen.unix`.

## 9. Armadilhas que já custaram bugs reais (herdadas do pai, do molde e da standard)

- **`nats.go`: `WithRetryAttempts(0)`, `ReconnectBufSize(-1)`,
  `WithPublishAsyncTimeout(sink.op_timeout)`.** A retentativa interna relê
  `msg.Data` depois de o `Write` voltar (corrida de posse de buffer); o buffer
  de reconexão é uma fila escondida fora do ring, do backpressure e do dump; um
  futuro sem prazo segura o buffer do pedaço para sempre. Se você "simplificar",
  os testes não reclamam.
- **O JLH vai em `Data`, nunca num header NATS.** O JetStream recusa headers
  acima de 64 KiB (`10097`): um hostname longo num header viraria o fim do
  processo. O bloco de headers é fixo, ≤ 256 B.
- **`Nats-Msg-Id` é `<uuid>.<step>`, nunca o `uuid` puro**: um processador que
  republique no mesmo stream (`step + 1`) teria o salto descartado como
  duplicata.
- **Nunca crie nem altere um stream.** O stream vem de `docker/nats/stream.json`
  (um teste confere o contrato: só `loghub.<space>.syslog`, nunca cobrindo
  `loghub._ctl.>`, `no_ack` falso, não selado); o servidor só o valida — no
  boot **e em toda checagem de saúde** (um stream editado por baixo ficava
  `network` para sempre com o sink UP). Não volte a guardar o `validate` atrás
  do `validated`: ele só controla o log. Em `core`, o `max_payload` idem.
- **Sem TLS, de propósito — e o `nats.go` tenta TLS sozinho.** Diante de um
  `tls_required` no `INFO`, o cliente se promove a TLS com as raízes do sistema.
  O `TLSCertCB` que recusa (e `errNATSServerRequiresTLS` fatal) é o que impede:
  não o remova, e não ligue nada de TLS no cliente (`Secure`, `RootCAs`), que
  é decisão do usuário (DECISOES, decisão 2). A recusa de `tls://`, `ws://` e
  `wss://` existe na config **e** no sink.
- **`no_ack` e JetStream desligado são fatais no boot, não `network`.** Como
  `network`, o boot esperaria `sink.boot_timeout` e o sink ficaria UP com o ring
  enchendo. "Sem respondedores" na primeira checagem é FATAL nomeando o
  JetStream; depois de uma checagem validada, `network`. Não pergunte
  `AccountInfo`: ele não distingue nada, e o `$JS.API.INFO` negado a um papel
  mínimo virava veredito permanente.
- **`logic` num futuro não encerra a espera**: os outros futuros dizem o que o
  stream guardou (`Error.Delivered`). `network`/`fatal` encerram, contaminados.
  E uma mensagem recusada **no cliente** (`ErrMaxPayload`) é recusada sozinha:
  encerrar o pedaço nela deixava as seguintes confirmadas sem nunca sair.
- **Permissão negada só chega assíncrona.** O `ErrorHandler` guarda um veredito
  (`ErrPermissionViolation` em `loghub.<space>.syslog` ou na `$JS.API`) que o
  `Health` e **todo `Write`** devolvem como `fatal`; sem ele, um publish negado
  era retentado para sempre com o sink UP. Em `core` o `Write` lê o
  `LastError()` depois do *flush* (o `-ERR` vem antes do `PONG`): sem isso o
  pedaço negado era confirmado e perdido. A negação no subject do beacon
  **não** é veredito.
- **URL com senha é parseada na validação, sem ecoar** — nem o trecho que o
  erro do `net/url` cita (`invalid port ":Sup3r"` é a senha até a barra). Um
  `%`, `/` ou `#` sem escape falhava dentro do `nats.Connect` como `network`,
  com a senha em cada log; caminho, consulta ou fragmento é exit 1 (um token com
  `/` virava o host discado, citado em cada log); e *userinfo* na URL junto com
  `username`/`token`/`credentials` é exit 1, porque o `nats.go` prefere o da
  URL em silêncio. Os logs de conexão usam `connectedURL` (esquema e host): o
  `ConnectedUrlRedacted` do `nats.go` esconde senha, não token. E os parsers de
  INI e de flags nomeiam a linha ou a flag, nunca o valor: um argumento com
  forma de flag nunca é valor de outra flag e só é nomeado se for uma flag
  deste servidor, e nada depois de um valor é citado.
- **A linha do dump é reingerida SEM conversão.** `emergency.ParseLine` é a
  **única** porta entre um arquivo em disco e o stream, e o NATS não lê o
  `Data`: uma linha que passasse sem ser um JLH deste servidor seria publicada e
  confirmada como legítima, com subject e headers tirados de um cabeçalho que
  ninguém conferiu. Por isso a porta exige o cabeçalho byte a byte igual ao de
  `jlh.AppendHeader`, escapes `\u` só de controle, só inteiros simples de até 20
  dígitos, `space` na gramática (vira token do subject), `type=syslog`/`step=0`
  (fixos nos headers), payload com `message` string, o payload cortado por
  posição igual ao do parser, e o resto de SPEC §12.2. Se você "simplificar"
  uma dessas regras, pergunte-se o que um consumidor passaria a receber.
- **O dump é lido em fluxo, nunca inteiro**: ele chega a 12× o ring, e um boot
  que o carregasse morreria por OOM no passo 9, a cada restart.
- **O dump só está salvo depois do `link` e do `fsync` do diretório.** Escrito
  sob o nome final, um dump cortado numa fronteira de linha pareceria limpo e
  seria removido depois de entregue em parte. Sem *hard link* (vfat, exFAT,
  FUSE) o `link` falha sempre: o fallback renomeia depois de conferir o nome
  livre, nunca por cima. Falha em qualquer passo — o `fsync` do diretório
  inclusive, também depois do `rename` (o N o tornou melhor esforço; aqui não,
  de propósito) — remove o que foi escrito e é exit 3.
- **Shutdown por FATAL tolera o primeiro sinal.** Ele começa sem sinal; o
  primeiro `SIGTERM` (o orquestrador vendo o processo cair) não pode cortar o
  dump, só o segundo aborta. E o abort conta em `abort` o que já fora entregue
  ao dump, não só o que ficou no ring.
- **A perda do abort tem um dono só** (SPEC §13.4). Da fase do dump em diante
  o abort conta o contador `dumping`, nunca o ring; as passagens
  (`enterDumpPhase`, `oweDump`) e o abort vão sob `lossMu`, o dump o toma com
  `Swap`, e um dump já contado como perdido desiste (`SetAbandon`). Conte cada
  mensagem num lado só, e teste o abort caindo em **cada** janela (os ganchos
  `beforeCollect`, `beforeDump`): a corrida esperada não aparece. `SIGQUIT`
  nunca é tolerado, e um `SIGQUIT` tardio chega por `quitNow`.
- **Ring cheio é conferido antes do `uuid` e da cópia** (pré-checagem sem trava,
  zero alocação); o `Enqueue` continua sendo a autoridade. O buffer cru volta ao
  pool na própria caixa `*[]byte`: uma mensagem aceita custa uma alocação.
- **Posse de buffer.** O item é dono do cru; volta ao pool **uma vez**, após a
  confirmação. Num retry ainda é o dono. Erro devolvido não significa "ninguém
  mais lê" (um futuro JetStream pendente aponta para o buffer do pedaço): na
  dúvida, `Discard`/`Taint` e deixe para o GC.
- **Lote retido não está no ring.** O shutdown coleta `Workers.Pending()`; o
  `hold` faz `append`, nunca atribuição. E os dois tetos do ring contam o lote
  retido.
- **Contar o que se segura:** o ring conta `cap(cru)` + `ItemOverhead` (256 B),
  não `len(cru)`. `TestItemOverheadCoversTheStruct` falha se a struct crescer.
- **Fronteira de histograma como string** pré-formatada na inicialização.
- **Erro desconhecido é `network`, nunca `logic`**; configuração e permissão do
  destino (credenciais, stream ausente ou de outro dono, 4xx da API) são
  `fatal`. Um `logic` de um erro que se repete descartaria um pedaço a cada 5 ms
  num processo que parece saudável.
- **O beacon nunca toca a saúde do sink**, e o gancho de conexão nunca bloqueia
  (roda na goroutine de *callback* do cliente). O gancho é registrado **antes**
  da primeira conexão, ou o beacon dela se perde.
- **Nunca alocar pelo que o par declara** (MSG-LEN, PROXY v2, linha do dump):
  alocar pelo que chegou, com teto. `emergency.MaxLine` é teto, não `make`.
- **Reavaliar a moldura a cada frame embaralha o fluxo** (D1).
- **O `\n`, o NUL e os `CR` da moldura não são da mensagem.**
- **UDP sem `SO_RCVBUF` grande perde em silêncio**; o efetivo é lido de volta.
- **A hora da mensagem não é tempo.** O tempo é a amostra da recepção (`event`).
- **Duas leituras de relógio** fazem o fato parecer posterior ao envelope: uma
  amostra só, para `uuid` e `event`.
- **`time.RFC3339Nano` remove zeros**; `strings.ToValidUTF8` junta octetos
  inválidos; `time.Date` normaliza 30/fev; `json.Marshal` (e `json.Unmarshal`)
  troca UTF-8 inválido em silêncio. A SPEC §6 diz o que usar.
- **SIGHUP** compara chave recarregável com o que está em execução e chave de
  restart com o boot. Só `[log]` recarrega.
- **Chave do pai no env some em silêncio** se não estiver em `removed.go`: o
  registry lê só os próprios nomes.
- **Durações zero** são recusadas onde zero não significa "sem limite"
  (`loghub.beacon_interval = 0` é legítimo: desliga só o beacon periódico).
- **Testes portados que passam vazios:** um helper que devolve zero quando a
  chave falta faz o teste passar sem provar nada. O mesmo vale para casos de
  recusa: sem um caso de controle válido, uma regra nova pode recusar todos pelo
  motivo errado.
- **O `\s` do motor de regex não é o do schema.** O `^\S+$` do schema é
  ECMAScript; o RE2 do `jsonschema/v6` deixa passar U+00A0, U+2028, U+FEFF e
  `\v`, e o Python da skill recusa ainda U+001C–U+001F e U+0085. O oráculo
  (`jlhcheck`) confere o conjunto completo na etapa semântica.
- **Escrita no boot é por pedaço.** Um erro `logic` no meio da reingestão não
  pode deixar os pedaços seguintes sem escrita e confirmados. Use
  `Instrumented.Deliver`.
- **Valores aceitos pela config precisam significar algo para quem os usa.** Um
  enum aceito pelo registry e desconhecido no pacote consumidor desliga o recurso
  em silêncio (`windows-1252` no charset).

## 10. Estado do projeto

Ver `docs/TESTING.md` §8 (lacunas, inclusive os conhecidos da revisão
independente do código NATS) e §9 (o que ela corrigiu), os defeitos conhecidos
e ainda não corrigidos em `docs/SPEC.md` §21.1, e a ordem de entrega em
`docs/DECISOES.md` §5.
