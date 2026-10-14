# AGENTS.md — go-webhook-pg-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes de
editar qualquer arquivo.

---

## 1. Onde você está

Você está em `go-webhook-pg-server/`, **uma subpasta** do monorepo
`loghub-paradise/`. Não é um projeto isolado.

```
loghub-paradise/                  <- monorepo, SEM go.mod, mantido por outro agente
  go-webhook-pg-server/              <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-webhook-pg-server
    cmd/go-webhook-pg-server/
    internal/
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/
  webhook-worker/                 <- futuro, ainda não existe
  webhook-admin/                  <- futuro, ainda não existe
  webhook-shots/                  <- futuro, ainda não existe
  tmp/                            <- rascunhos históricos, NÃO use como referência
```

O servidor recebe webhooks arbitrários da internet, encapsula cada requisição num
envelope **JLH do LogHub** (`type=webhook`, `docs/SPEC.md` §3), guarda-o num ring
em memória e o enfileira na `loghub.inbox` de um PostgreSQL. É um *bridge-in*
HTTP do LogHub, rápido e com dependências mínimas (três em produção). Escala
horizontalmente; instâncias não compartilham estado.

Fontes normativas (skills do servidor MCP `loghub-mcp`): `loghub-json-schema`
(envelope), `loghub-type-webhook-fast` (payload `webhook`, respostas),
`loghub-postgres-table-inbox` (`sink=postgres`), `go-loghub-uuidv7` e
`go-loghub-ident`. Cópias dos schemas em `internal/jlhcheck/loghub/`.

**Este módulo nasceu em 2026-10-07 como cópia do `../go-webhook-fast-server/`**,
reduzida a um ring (`memory`) e um destino (`postgres`), sem rate-limit e sem
recarga por `SIGHUP`, com o dump de emergência em JSON Lines (`docs/SPEC.md`
§0, `docs/DECISOES.md`). Os pacotes e arquivos que ficaram têm o mesmo nome e o
mesmo conteúdo dos do fast, e as seções da SPEC a mesma numeração (as retiradas
ficam no lugar, marcadas). São módulos independentes, **sem código
compartilhado**: uma correção num pacote de mesma origem é portada à mão, nos
dois; `diff -ruN -x .DS_Store ../go-webhook-fast-server .` mostra as diferenças.

## 2. Fronteira de responsabilidade

**Seu escopo é esta pasta e nada acima dela.**

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| `docs/SPEC.md`, `docs/TESTING.md` | você |
| Diretório raiz `loghub-paradise/` e seus arquivos | agente do monorepo |
| **Git: init, commit, branch, tag, remoto, `.gitignore`** | agente do monorepo |
| Integração contínua, publicação de imagem, releases | agente do monorepo |
| Os outros serviços do ecossistema | agente do monorepo |

### Git não é seu

O versionamento vive no nível de `loghub-paradise/`, não aqui. Regras:

- **Nunca** rode `git init`, `git add`, `git commit`, `git tag`, `git push` ou
  `git remote` a partir desta pasta.
- Se precisar que algo seja versionado, **descreva o que mudou** e deixe o commit
  para o agente do monorepo.
- Um `.gitignore` desta pasta é aceitável apenas para padrões estritamente locais
  (`data/`); o `.gitignore` principal é do nível
  acima.
- As tags de release seguem o formato `go-webhook-pg-server/vX.Y.Z`, criadas do nível
  acima.

> O `.git` que existia **dentro** desta pasta (zero commits) não existe mais:
> desde 2026-09-20 o versionamento vive na raiz `loghub-paradise/`.

## 3. A especificação manda

`docs/SPEC.md` está marcada como **congelada para implementação**. Ela não é
documentação descritiva do código: é o contrato que o código cumpre.

- Discrepância entre código e spec é **bug no código**, salvo justificativa
  explícita registrada na própria spec (há precedentes, como a decisão sobre o
  gerador de identificadores em §14.3).
- Para mudar comportamento: **altere a spec primeiro**, com o porquê, depois o
  código, depois o teste.
- Ao citar a spec em comentário, commit ou resposta, use o número da seção
  (`§4.3`, `§10.2`). O código já faz isso e a convenção deve continuar.
- `docs/TESTING.md` §8 registra as lacunas conhecidas com honestidade. Se você
  fechar uma, atualize a lista. Se descobrir outra, acrescente.
- `docs/SPEC.md` §19.1 registra os **defeitos** conhecidos e ainda não
  corrigidos, cada um com a direção da correção. Corrigiu um, tire-o de lá e
  escreva a regra na seção que ele cita.

## 4. O modelo mental: um ring, um destino

- **RING** — *onde o evento espera* entre ser aceito e ser entregue: `memory`.
  O `200` sai quando o item entrou no canal (`SPEC.md` §2.2).
- **SINK** — *para onde o evento vai*: a fila `loghub.inbox` do PostgreSQL
  (§3.6, §8.5), compartilhada entre instâncias. O servidor **nunca cria a
  tabela**, só a valida no boot (§8.5.1, §8.5.5).
- **EMERGÊNCIA** — com o sink FATAL ou o `drain_timeout` estourado, o que o ring
  tem vai para `emg-<agent_uuid>-<unixnano>.jsonl`, um JLH por linha, e volta
  no boot seguinte (§9).

As interfaces `ring.Ring` e `sink.Sink` ficam, com uma implementação cada
(decisão 4): é o que mantém o porte de correções do fast por `diff`.
**Nenhuma implementação de ring consulta o sink, e vice-versa.**

**Compatibilidade com o fast (decisão 3, §0).** Chaves, env `WHS_*` e métricas
`whs_*` são as do fast. `ring.mode`, `sink.type` e `ratelimit.enabled` são
*seletores*: aceitos só com `memory`, `postgres` e `false`. Toda outra chave de
recurso retirado é chave desconhecida (`exit 1`). Não "simplifique" tirando os
seletores: um INI do fast quebraria.

## 5. Mapa do código

```
cmd/go-webhook-pg-server/main.go   versão/commit, lhident.Initialize() (UMA vez), internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP ignorado, reingestão
  config/     registry único -> ini + flags + env + --help + dump; validação
  server/     http.Server, pipeline da requisição, admin, listeners, limites
  clientip/   X-Forwarded-For / X-Real-IP + proxies confiáveis   [lógica pura]
  ident/      Identity congelada e gramática de space             [lógica pura]
  envelope/   escritor JLH, payload webhook, ParseHead, query, pools [lógica pura, fuzz]
  jlhcheck/   oráculo de teste: schemas + regras semânticas       [só em teste]
  ring/       interface Ring + memory.go
  sink/       interface Sink + postgres.go, inbox.go, workers.go, health.go
  emergency/  dump JSON Lines e reingestão pós-crash
  obs/        métricas Prometheus próprias + slog
```

| Pacote | Onde mexer com mais cuidado |
|---|---|
| `app` | ordem do boot, as cinco fases do shutdown, e o `ignoreSIGHUP` logo no início do `run` |
| `sink` | posse de buffer nos caminhos de erro dos workers; em `postgres.go`, a tabela de SQLSTATE, o reset dos slices do pool e o prazo próprio da validação (`validationContext`) |
| `config` | o registry é a fonte única; os três seletores de compatibilidade |
| `server` | ordem do pipeline em `handler.go` |
| `ring` | contabilidade de bytes e itens |
| `envelope` | pools: liberação dupla é o pior bug possível aqui; e nenhum `0x0A` cru no JLH (§9.1) |
| `obs` | fronteiras de histograma são estáticas, nunca derivadas da config |
| `emergency` | o leitor tolera arquivo cortado e linha danificada sem pânico, e nunca guarda uma linha acima do teto |
| `clientip` | único parser de entrada hostil; cobertura por fuzz é obrigatória |

`internal/` impede import externo acidental. `clientip` e `envelope` são lógica
pura: testáveis sem rede, sem disco e sem banco.

## 6. Invariantes que nenhuma mudança pode violar

Resumo de `SPEC.md` §17. Se sua alteração conflita com um destes, ela está errada.

1. O `200` nunca sai antes de o item estar no ring.
2. Ring cheio responde `429`. Nunca `200` seguido de descarte, exceto a recusa
   `logic` de um lote pelo destino vivo, logada e contada.
3. Um buffer volta ao pool **uma vez**, e só após confirmação.
4. Evento aceito só sai do ring após confirmação do sink; nunca é descartado para
   liberar espaço.
5. `network.ip.src` (e todo cabeçalho de proxy: `X-Forwarded-Proto/-Host/-Port`,
   `X-Client-Port`, `X-Server-Addr`, fingerprints) nunca vem de header quando o
   peer TCP não está em `proxy.trusted`.
6. Nenhum label de métrica deriva de conteúdo controlado pelo cliente.
7. O corpo nunca é parseado, decodificado, validado ou modificado.
8. `network.meta` `peer-addr` sempre registra a ponta TCP real, independente de headers.
9. Nenhum log síncrono por requisição no caminho padrão.
10. *(retirada: era a regra dos comandos do rueidis)*
11. O envelope JLH é escrito UMA vez no handler; ring, dump e fila recebem os
    mesmos bytes. A única alteração é a da cópia que vai para o `jsonb` da
    `loghub.inbox` (`\u0000` → `\ufffd`), nunca no JLH do item.
12. `ring` e `sink` são ortogonais.
13. Toda escrita em disco fica sob `ring.data_dir` — e dentro de um contêiner
    esse diretório é `/data`, em toda imagem do ecossistema.
14. `uuid` e `event` saem de UMA amostra de relógio (`GenerateAt`); o `uuid`
    devolvido em `X-LogHub-UUID` é o do envelope na fila, e nunca é
    regerado (retry, ring, dump).
15. *(retirada: era a checagem de teto de payload dos destinos que saíram)*
16. Uma linha do dump de emergência é um JLH inteiro seguido de `\n`, e nada
    mais.

## 7. Comandos

Todos rodam a partir desta pasta.

Suíte rápida, a cada mudança:

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**: os bugs de perda de dados deste projeto vivem em
caminhos concorrentes. Sem PostgreSQL a suíte roda o ciclo de vida inteiro
contra o destino falso de `internal/app/fakesink_test.go`; os testes do
PostgreSQL pulam.

Fuzzing, obrigatório antes de qualquer release, 10 minutos cada:

```bash
go test ./internal/clientip/  -run=XXX -fuzz=FuzzResolve      -fuzztime=10m
go test ./internal/emergency/ -run=XXX -fuzz=FuzzRead         -fuzztime=10m
go test ./internal/envelope/  -run=XXX -fuzz=^FuzzEnvelope$   -fuzztime=10m
go test ./internal/server/    -run=XXX -fuzz=FuzzServeHTTP    -fuzztime=10m
go test ./internal/sink/      -run=XXX -fuzz=^FuzzJSONBSafe$  -fuzztime=10m
```

PostgreSQL — **os testes pulam sem um servidor**, e o servidor não cria a
fila (a suíte cria as dela, cada uma num esquema próprio, a partir de
`docker/postgres/inbox.sql`, a DDL literal da skill; PostgreSQL **18**):

```bash
docker run --rm -d -p 15432:5432 -e POSTGRES_USER=whs -e POSTGRES_PASSWORD=whs \
  -e POSTGRES_DB=whs_test --name whs-test-pg postgres:18-alpine
WHS_TEST_PG_DSN='postgres://whs:whs@127.0.0.1:15432/whs_test?sslmode=disable' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f whs-test-pg
```

Se você mexeu em `internal/sink/`, na validação de `config` ou em
`docker/postgres/inbox.sql`, **rode esta suíte**.

Executar e inspecionar:

```bash
go run ./cmd/go-webhook-pg-server --help
go run ./cmd/go-webhook-pg-server --dump-config
# o hostname do macOS não pode ter `_`, ou o go-loghub-ident sai com 109
HOSTNAME=dev-mac go run ./cmd/go-webhook-pg-server --sink.postgres.addr=127.0.0.1:15432 \
  --sink.postgres.username=whs --sink.postgres.password=whs --sink.postgres.database=whs_test \
  --ring.data_dir=./data --server.bind=127.0.0.1:8080 --admin.bind=127.0.0.1:9101
```

Docker, a partir de `docker/`:

```bash
docker compose up -d --build      # memory -> postgres, com o PostgreSQL 18 ao lado
```

## 8. Convenções

**Idioma.** Código, identificadores e comentários em **inglês**. Documentação
(`docs/`, `docker/README.md`, este arquivo) e conversa com o usuário em
**português**. Não misture dentro de um mesmo arquivo.

**Comentários explicam o porquê, não o quê.** O padrão deste projeto é um
comentário que justifica a decisão e diz o que aconteceria com a alternativa
rejeitada. Leia `internal/server/handler.go` ou `internal/sink/workers.go` antes
de escrever o seu; imite a densidade e o tom.

**Dependências: três em produção, e ponto.** `github.com/jackc/pgx/v5` para
PostgreSQL (Go puro; usado pela API nativa, nunca por `database/sql`, porque a
tabela de SQLSTATE de `SPEC.md` §8.5.7 precisa do `pgconn.PgError`),
`github.com/patrickbrandao/go-loghub-uuidv7` (UUIDv7 nível 3 por `GenerateAt`)
e `github.com/patrickbrandao/go-loghub-ident` (AgentUUID, Workspace) — estes
dois sem dependência transitiva nenhuma. `github.com/google/uuid` e
`github.com/santhosh-tekuri/jsonschema/v6` são só de teste. O objetivo do
módulo é dependência mínima: acrescentar uma de produção exige justificativa
na spec (`SPEC.md` §14.3).

**Identidade: `lhident.Initialize()` só no `main`, uma vez.** A segunda chamada
sai com 112, e os testes montam a app dezenas de vezes: por isso a app recebe
um `ident.Identity` (`app.Main(args, initIdentity)`; nos testes,
`runContext(ctx, args, id, hook)`). `ring.data_dir` é exportado como `DATADIR`,
absoluto, antes do `Initialize()` (`SPEC.md` §10.1).

**Um gerador de uuid por processo** (`envelope/jlh.go`, `uuidv7.NewGenerator()`),
e o `uuid` sempre de `NewUUID(t)` com a MESMA amostra `t` do `event`. Ler o
relógio duas vezes faz o fato parecer posterior ao envelope.

**Configuração: o registry é fonte única.** Um parâmetro novo entra em
`internal/config/registry.go` e daí saem sozinhos o parser de ini, as flags, o
env, o `--help`, o dump e a validação. Nunca adicione um caminho paralelo.

**Métricas: cardinalidade fixa.** Todo label tem conjunto de valores finito e
conhecido em tempo de compilação. Nada de path, método, IP ou user-agent como
label. Fronteiras de histograma são estáticas e idênticas em toda instância.

**Escrita em disco: uma raiz só.** O que o serviço grava (a identidade e
`emergency/`) deriva de `ring.data_dir`, e dentro de um
contêiner essa raiz é `/data` — regra de todas as imagens do `loghub-paradise`,
não só desta. Um caminho de escrita que não derive dali quebra a regra: os
compose sobem com `read_only: true`, então ele falha no boot, ou pior, falha no
meio de um incidente se for o `emergency.dir`. Ver `SPEC.md` §13.6.

**Caminhos fixos do contêiner.** `/app` guarda o binário pronto (dono `root`),
`/data` é a única área gravável, `/etc/webhook` é o INI em `:ro` e `/tmp` é
tmpfs volátil. Junto com eles, duas regras: o `WORKDIR` é `/data`, e o binário é
invocado pelo caminho absoluto `/app/<nome>`, nunca pelo nome solto. Vale para
toda imagem do `loghub-paradise`; não invente um caminho novo para um serviço
só.

**Sem CGO.** `CGO_ENABLED=0` é premissa do Dockerfile e do binário estático.

## 9. Armadilhas que já custaram bugs reais

Registradas em `docs/TESTING.md` §9. As que mais reincidem:

- **Posse de buffer.** O item do ring é dono de `Env` (o JLH, corpo incluído). Ele volta ao pool
  exatamente uma vez, só após confirmação. Num retry de lote o item **ainda é o
  dono** e nada é devolvido. O buffer do corpo lido volta ao pool assim que o
  JLH é escrito: nada mais o referencia. Devolver cedo coloca o corpo de um cliente dentro do
  evento de outro — silencioso, invisível sob carga.
- **Lote retido pelo worker não está no ring.** Um worker que retenta segura os
  itens fora do ring; o shutdown precisa coletá-los de `Workers.Pending()` ou eles
  somem depois de terem recebido `200`.
- **E o `hold` não pode sobrescrever.** Um worker que **abandona** o lote no
  shutdown o mantém registrado, e se o `loop` chamar `Dequeue` outra vez ele pode
  voltar com um segundo lote e gravá-lo por cima do primeiro. Por isso `deliver`
  devolve se a posse mudou de mãos e o `loop` para quando não mudou.
- **Nada é promovido ao lado do JLH.** As colunas da `inbox` são lidas do próprio
  envelope (`envelope.ParseHead`, estrito). Um item que volta de um dump é
  indistinguível do que entrou.
- **`jsonb` recusa `\u0000`** (`22P05`, classe 22: o lote inteiro descartado).
  O corpo com NUL vai em base64; o `%00` da query vira `\ufffd` só na cópia
  do `jsonb` (`sink/inbox.go`, `jsonbSafe`).
- **Slices do pool de argumentos do `sink=postgres`.** Os dez arrays do
  `unnest` seguram referências para o `Env` do item. Eles voltam ao pool
  **zerados** (`reset`); um slice devolvido cheio prende buffers que já voltaram
  ao pool de envelope, e o lote seguinte carrega o corpo de outro cliente.
- **Classe `42` do SQLSTATE é fatal, nunca `logic`.** Tabela dropada, coluna
  sumida ou índice único ausente classificados como `logic` descartariam um lote
  a cada 5 ms, para sempre, num processo que parece saudável. A tabela inteira
  está em `SPEC.md` §8.5.7 e no `TestClassifySQLState`; mude os dois juntos.
- **O servidor nunca faz DDL.** Nem `CREATE TABLE IF NOT EXISTS`: N instâncias
  subindo juntas correm no catálogo, e o papel de produção não tem o
  privilégio. A fila é `docker/postgres/inbox.sql` (DDL literal da skill, um
  teste confere o hash); o servidor valida e sai com 3.
- **`ON CONFLICT (uuid)` exige `SELECT (uuid)`**, não só `INSERT`: o boot
  confere o grant (`GRANT INSERT, SELECT (uuid)`).
- **A validação de boot tem prazo próprio** (`sink.op_timeout`, por
  `validationContext`): no prazo da checagem de saúde (1 s) ela não cabia com
  RTT alto. O fast tinha o texto na SPEC e não no código (§8.5.5).
- **Um envelope é uma linha.** O dump de emergência é JSON Lines; um `0x0A` cru
  num JLH partiria a linha. O serializador escapa todo controle, os fuzz
  conferem, e o escritor do dump deixa de fora (e conta) o que tiver um.
- **O dump só está salvo no nome final.** Escrito sob ele, um dump cortado numa
  fronteira de linha pareceria limpo: o escritor usa um temporário oculto e
  `link` (sem *hard link*, renomeia depois de conferir o nome livre). O `fsync`
  do diretório é melhor esforço — nunca apague um dump pronto porque ele
  falhou. Só arquivos regulares são dumps.
- **`ring.Commit` antes do `Release`**, também na recusa `logic`. Desde
  2026-10-14 o ring cobra `cap(Env)` mais `ring.ItemOverhead` e desconta o
  `Item.Charged` gravado no `Enqueue` (SPEC §7.2), sem reler o buffer; a ordem
  continua a regra (é a única que serve também ao `ring=sqlite` do fast, que
  desconta `len(Env)`, que o `Release` zera).
- **O lote vai em pedaços de `sink.batch_max_bytes`** (SPEC §8.1): o worker
  segura o pedaço em retentativa **e o resto do lote**, e o `unhold` tira só o
  pedaço resolvido (`unhold(id, piece)`); apagar o registro inteiro perderia o
  resto num dump.
- **O ring é selado antes do último `Drain` do shutdown** (`Seal`, uma
  barreira contra o `Enqueue`): um upload que terminasse depois recebia `200`
  num ring que ninguém mais lia. Não tire o `RWMutex` do `Enqueue`. O teste
  completa o upload **dentro** do `Drain` (`App.wrapRing`): é o que prova a
  ordem.
- **Parsers de INI e de flags nomeiam a linha ou a flag, nunca o valor**: um
  argumento com forma de flag nunca é valor de outra flag e só é nomeado se for
  uma flag deste servidor, e nada depois de um valor é citado.
- **`drain_timeout` é um prazo para as fases 3 e 4 juntas.** Um prazo por fase
  dobrava o pior caso e passava do `stop_grace_period`.
- **Recusa fatal no boot tem log próprio** ("the sink refused this server
  permanently"), não "did not become reachable within sink.boot_timeout".
- **`SIGHUP` precisa ser capturado.** Sem recarga, deixá-lo com a ação padrão
  do Go mataria o processo sem dreno nem dump; `ignoreSIGHUP` é a primeira coisa
  do `run`.
- **Fronteira de histograma como string.** Emitir `0.01` num scrape e `0.010` no
  seguinte cria duas séries e quebra todo quantil, em silêncio. As strings são
  pré-computadas na inicialização, e são as mesmas do fast.

## 10. Estado do projeto

Criado em 2026-10-07 a partir do `go-webhook-fast-server` (SPEC §0). O código
cumpre a spec e a suíte passa com `-race` contra um PostgreSQL 18 real; o
compose foi exercitado de ponta a ponta (entrega, `SIGHUP`, FATAL, dump JSON
Lines, reingestão). Antes de qualquer deploy real faltam: versionamento (nível
acima), integração contínua com PostgreSQL de serviço, calibragem dos fusíveis
de saúde do sink e a aferição dos critérios de aceite de `SPEC.md` §15.5, que
**nunca foram medidos** (`TESTING.md` §7, §8). Os defeitos conhecidos e ainda
não corrigidos estão em `SPEC.md` §19.1.

Ao portar uma correção do fast para cá (ou daqui para lá), comece pelo `diff
-ruN` dos pacotes de mesma origem: `envelope`, `clientip`, `server`, `obs`,
`sink/postgres.go`, `sink/inbox.go`, `sink/workers.go`, `sink/health.go`.
