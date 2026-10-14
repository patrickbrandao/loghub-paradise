# AGENTS.md — go-syslog-standard-server

> **Renomeado em 2026-10-05.** Esta pasta era o `go-syslog-server` (envelope
> próprio, RELP, `node-id`) e nunca entrou em produção. Foi renomeada para
> `go-syslog-standard-server`, com o module path trocado para
> `.../go-syslog-standard-server`, e fica **só como referência**: código, testes e
> lições de onde o novo `../go-syslog-server/` (LogHub, envelope JLH,
> `type=syslog`) porta o que servir. A especificação do RELP foi extraída para
> `../go-relplog-server/`. O texto abaixo descreve o estado anterior ao
> renomeio; onde ele diz `go-syslog-server`, leia esta pasta.
>
> **Antes de portar um trecho, leia `docs/SPEC.md` §21.1.** Ela lista os
> defeitos conhecidos que esta referência ainda tem, com a direção da correção:
> o trecho vai para o módulo novo já corrigido.

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

Você está em `go-syslog-server/`, **uma subpasta** do monorepo
`loghub-paradise/`. Não é um projeto isolado.

```
loghub-paradise/                  <- monorepo, SEM go.mod, mantido por outro agente
  go-webhook-server/              <- serviço IRMÃO, módulo Go independente
  go-syslog-server/               <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server
    cmd/go-syslog-server/
    internal/
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/
  tmp/                            <- rascunhos históricos, NÃO use como referência
```

O servidor recebe syslog de quatro transportes, interpreta o que consegue sem
nunca falhar, e entrega a um destino configurável. Escala horizontalmente;
instâncias não compartilham estado.

## 2. O serviço irmão é a referência, não a dependência

`../go-webhook-server/` resolve o mesmo problema para HTTP e já está em estado
maduro. **Leia o código dele antes de escrever o seu.** Os dois compartilham
desenho, invariantes, o registry de configuração, a máquina de saúde, os cinco
estágios do shutdown e o formato de três campos no Redis.

| Arquivo do irmão | O que ele ensina |
|---|---|
| `internal/sink/workers.go` | lote retido, `hold`/`Pending`, por que o laço para |
| `internal/sink/health.go` | os fusíveis e o `WaitReady` que para em fatal |
| `internal/ring/sqlite.go` | group commit, cursor, identidade em coluna própria |
| `internal/sink/postgres.go` | `unnest`, validação de esquema, SQLSTATE, pool zerado |
| `internal/emergency/emergency.go` | decoder que nunca confia no comprimento |
| `internal/config/registry.go` | o registry como fonte única |
| `internal/app/app.go`, `boot.go` | o boot em passos e as cinco fases |
| `internal/envelope/pool.go` | a regra de posse única de buffer |

**Mas nada é importado de lá.** São dois módulos Go independentes e continuam
assim. Compartilhar um pacote faria os dois evoluírem amarrados, e a primeira
divergência necessária — que já existe, no envelope e no parse — viraria um
parâmetro condicional. Copiar com adaptação é o certo aqui; importar não é.

Quando copiar um arquivo do irmão, **traga os comentários**. Eles são o registro
de por que cada decisão foi tomada, e sem eles a próxima pessoa reintroduz o bug
que aquele comentário evita.

## 3. Fronteira de responsabilidade

**Seu escopo é esta pasta e nada acima dela.**

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| `docs/SPEC.md`, `docs/TESTING.md` | você |
| `../go-webhook-server/` | o agente daquela pasta |
| Diretório raiz `loghub-paradise/` | agente do monorepo |
| **Git: init, commit, branch, tag, remoto, `.gitignore`** | agente do monorepo |
| Integração contínua, publicação de imagem, releases | agente do monorepo |

### Git não é seu

O versionamento vive no nível de `loghub-paradise/`, não aqui.

- **Nunca** rode `git init`, `git add`, `git commit`, `git tag`, `git push` ou
  `git remote` a partir desta pasta.
- Se precisar que algo seja versionado, **descreva o que mudou** e deixe o
  commit para o agente do monorepo.
- Um `.gitignore` desta pasta é aceitável apenas para padrões estritamente
  locais (`data/`, `*.db`, `*.db-wal`, `*.db-shm`).
- As tags de release, se houver, seguem o formato
  `go-syslog-standard-server/vX.Y.Z`, criadas do nível acima. O prefixo
  `go-syslog-server/` é do servidor LogHub que hoje ocupa aquela pasta.

## 4. A especificação manda

`docs/SPEC.md` está marcada como **congelada para implementação**. Ela não é
documentação descritiva do código: é o contrato que o código cumpre.

- Discrepância entre código e spec é **bug no código**, salvo justificativa
  explícita registrada na própria spec.
- Para mudar comportamento: **altere a spec primeiro**, com o porquê, depois o
  código, depois o teste.
- Ao citar a spec em comentário ou resposta, use o número da seção (`§6.3`,
  `§10.5.7`). O código do irmão já faz isso e a convenção continua aqui.
- `docs/TESTING.md` registra as lacunas conhecidas com honestidade. Se você
  fechar uma, atualize a lista. Se descobrir outra, acrescente.

## 5. O modelo mental

### Dois eixos ortogonais

- **RING** — *onde a mensagem espera* entre ser aceita e ser entregue.
  `memory` | `sqlite` | `none`
- **SINK** — *para onde ela vai*, o destino final.
  `redis` | `sqlite` | `postgres` | `mysql` | `nats` | `none`

As dezoito combinações são válidas e distintas. `ring=sqlite` é uma **fila**
(linhas apagadas após entrega); `sink=sqlite` é um **destino final**
append-only. Bancos diferentes, arquivos diferentes, e o servidor recusa subir
se apontarem para o mesmo arquivo.

**Nenhuma implementação de ring consulta o tipo do sink, e vice-versa.** Um
`if sinkType == ...` dentro de um ring quebra o desenho. O `ring=none` escreve
através de uma interface `Writer` declarada no **próprio pacote do ring**, para
que ele não consiga nem importar o pacote do sink.

### Um terceiro eixo que o irmão não tem: onde o parse roda

`SPEC.md` §2.2. Só o **PRI** é lido na recepção, porque o filtro depende dele.
Todo o resto — RFC 3164, RFC 5424, structured-data, envelope — roda no **worker
de dreno**.

Consequências que você vai encontrar espalhadas pelo código:

- O item no ring **não tem envelope**. Ele tem cabeçalho (§3.2) + linha crua.
- A contabilidade de bytes do ring conta **só a linha crua**. A memória do
  envelope é do worker e tem teto próprio (§14.3). Não some as duas.
- O `ring=sqlite` e o arquivo de emergência guardam **cabeçalho + linha crua**.
  Não existe reconstrução de metadado a partir de JSON escapado — a armadilha
  que o irmão tem em `MetaFromJSON` simplesmente não existe aqui.
- Com `ring=none` o parse roda na goroutine de recepção, porque é ela quem
  escreve no sink.

### A confirmação não existe em todo transporte

`SPEC.md` §2.3 é a tabela mais importante desta spec, e a maior divergência em
relação ao irmão:

| Transporte | A confirmação é | Ring cheio significa |
|---|---|---|
| RELP | `rsp 200 OK` | `500 ring full`; o emissor retém |
| TCP | continuar lendo | **parar de ler**; a janela TCP segura o emissor |
| unix dgram | não existe | descarte — mas o kernel já segurou o emissor antes |
| UDP | não existe | **descarte contado** |

Não invente semântica de confirmação onde o protocolo não tem. Um `syslog_`
métrica com a razão do descarte é a resposta honesta no UDP; um log por
datagrama descartado, não.

## 6. Mapa do código

```
cmd/go-syslog-server/main.go   só injeta versão/commit e chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão
  config/     registry único -> ini + flags + env + --help + dump; validação
  receiver/   udp.go, unix.go, tcp.go, relp.go, framing.go, limits.go
  syslog/     pri.go, rfc3164.go, rfc5424.go, sd.go, names.go   [lógica pura]
  filter/     facility e severidade                             [lógica pura]
  ratelimit/  token bucket com shards e sweep                   [lógica pura]
  envelope/   Item, codec do cabeçalho, envelope JSON, pools    [lógica pura]
  ring/       interface Ring + memory.go, sqlite.go, none.go
  sink/       interface Sink + redis.go, sqlite.go, postgres.go, mysql.go,
              nats.go, none.go, tls.go, workers.go, health.go
  emergency/  dump binário e reingestão pós-crash
  obs/        métricas Prometheus próprias + slog
```

Em relação ao irmão: `server/` virou `receiver/`, `clientip/` **não existe**
(syslog não tem header de encaminhamento), e `syslog/` e `filter/` são novos.

`internal/syslog/` herda o papel que o `clientip/` tinha lá: é o **único** parser
de entrada adversária, e é alvo de fuzz obrigatório.

## 7. Comandos

Todos rodam a partir desta pasta.

Suíte rápida, a cada mudança:

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**: os bugs de perda de dados deste projeto vivem em
caminhos concorrentes.

Cobertura real (por pacote engana; a suíte de `app` exercita `receiver`, `ring`
e `sink` sem que isso lhes seja atribuído):

```bash
go test -coverpkg=./internal/... -coverprofile=cov.out ./... && go tool cover -func=cov.out | tail -1
```

Fuzzing, obrigatório antes de qualquer release:

```bash
go test ./internal/syslog/    -run=XXX -fuzz=FuzzParse -fuzztime=60s
go test ./internal/syslog/    -run=XXX -fuzz=FuzzSD    -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=FuzzFrame -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=FuzzRead  -fuzztime=60s
```

Destino Redis — **é o padrão de produção**. A suíte que lê esta variável **ainda
não existe** (`docs/TESTING.md` §8, item 1); o comando abaixo é o formato
combinado com o irmão, para quando ela for escrita:

```bash
docker run --rm -d -p 16379:6379 --name sls-test-redis redis:7-alpine
SYSLOG_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race -timeout 600s ./internal/app/
docker rm -f sls-test-redis
```

Destino PostgreSQL — a suíte que lê esta variável **ainda não existe**
(`docs/TESTING.md` §8, item 2). O servidor não cria a tabela; quando a suíte for
escrita, ela cria as dela a partir de `docker/postgres/schema.sql`:

```bash
docker run --rm -d -p 15432:5432 -e POSTGRES_USER=sls -e POSTGRES_PASSWORD=sls \
  -e POSTGRES_DB=sls_test --name sls-test-pg postgres:17-alpine
SYSLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
docker rm -f sls-test-pg
```

Destino MySQL — a suíte rodou pela primeira vez contra MySQL 8.4 real em
2026-10-14; a expectativa de teste errada e o teste instável que ela mostrou
foram corrigidos no mesmo dia (`docs/TESTING.md` §8, itens 18, 21 e 22). Cria as tabelas dela a partir de
`docker/mysql/schema.sql`:

```bash
docker run --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=sls -e MYSQL_DATABASE=sls_test \
  --name sls-test-mysql mysql:8.4
SYSLOG_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13306)/sls_test' \
  go test -race -timeout 600s -run MySQL ./internal/sink/ ./internal/app/
docker rm -f sls-test-mysql
```

Destino NATS — o `nats-server` é um módulo Go, sem Docker. A suíte cria os
streams dela a partir de `docker/nats/stream.json`:

```bash
go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 \
  go test -race -timeout 600s -run NATS ./internal/sink/ ./internal/app/
```

Interoperabilidade com emissores reais (`SPEC.md` §17.4) — **a única prova de
que a moldura funciona no fio**. Se você mexeu em `receiver/framing.go`,
`receiver/tcp.go`, `receiver/relp.go` ou no parser, **rode**.

Executar e inspecionar:

```bash
go run ./cmd/go-syslog-server --help
go run ./cmd/go-syslog-server --dump-config
go run ./cmd/go-syslog-server --listen.udp=127.0.0.1:1514 --listen.tcp=127.0.0.1:1514 \
  --sink.type=sqlite --ring.data_dir=./data --admin.bind=127.0.0.1:9102
logger -n 127.0.0.1 -P 1514 -d "teste"
```

## 8. Convenções

**Idioma.** Código, identificadores e comentários em **inglês**. Documentação
(`docs/`, `docker/README.md`, este arquivo) e conversa com o usuário em
**português**. Não misture dentro de um mesmo arquivo.

**Comentários explicam o porquê, não o quê.** O padrão é um comentário que
justifica a decisão e diz o que aconteceria com a alternativa rejeitada. Leia
`../go-webhook-server/internal/sink/workers.go` antes de escrever o seu; imite a
densidade e o tom.

**Dependências: seis em produção, e ponto.** `rueidis` (Redis),
`modernc.org/sqlite` (Go puro, sem CGO), `jackc/pgx/v5` (API nativa, nunca
`database/sql`, porque a tabela de SQLSTATE precisa do `pgconn.PgError`),
`go-sql-driver/mysql` (por `database/sql`, que é para onde ele foi desenhado; o
`*mysql.MySQLError` atravessa com o `Number`), `nats-io/nats.go` (com o pacote
`jetstream`) e `go-loghub-uuid` (UUIDv7 `Level2`, sem dependência transitiva).
As duas do meio entraram em 2026-09-15, nas mesmas versões do irmão
(`SPEC.md` §16.3).
`github.com/google/uuid` é importado por este código **só em teste**, como oráculo
independente — mas entra no binário de forma transitiva pelo `modernc.org/libc`.
Não afirme que ele está fora do binário; `go version -m` mostra que está.

**O parser de syslog é próprio e continua próprio.** Não troque por biblioteca:
as bibliotecas falham numa mensagem que não casa com a gramática, e a regra de
§6 é que o parser **nunca falha**. Elas também alocam string por campo, onde
este código fatia bytes e registra deslocamentos. A justificativa completa está
em `SPEC.md` §16.3.

**Ids: a entropia não vem do gerador padrão da lib.** `internal/envelope/id.go`
monta o gerador com `NewGeneratorWith(rand.Uint64)` de propósito. O
`NewGenerator()` da biblioteca usa PCG (previsível) e, se `crypto/rand` falhar,
degrada em silêncio para semente de relógio.

**Configuração: o registry é fonte única.** Um parâmetro novo entra em
`internal/config/registry.go` e daí saem sozinhos o INI, as flags, o env, o
`--help`, o dump e a validação. Nunca um caminho paralelo.

**Métricas: cardinalidade fixa.** Nenhum label deriva de conteúdo do emissor —
nada de hostname, tag, app-name ou IP. `facility` e `severity` também **não**
são labels, mesmo sendo conjuntos finitos: o produto com `transport` daria 768
séries, e a distribuição por severidade é pergunta para o destino, onde é uma
coluna indexada. `SPEC.md` §13.1.

**Escrita em disco: uma raiz só.** Os três arquivos que o serviço grava
(`ring.db`, `events.db`, `emergency/`) derivam de `ring.data_dir`, e em
contêiner essa raiz é `/data`. A **única** exceção é `listen.unix`, que é um
socket de leitura e por definição mora fora (`/dev/log`).

**Caminhos fixos do contêiner.** `/app` guarda o binário (dono `root`), `/data`
é a única área gravável, `/etc/syslog` é o INI em `:ro`, `/tmp` é tmpfs. O
`WORKDIR` é `/data` e o binário é invocado por `/app/go-syslog-server`, nunca
pelo nome solto.

**As portas padrão não são 514.** São `:1514` (UDP e TCP) e `:20514` (RELP),
porque a imagem roda como uid 65532 e não pode abrir porta privilegiada. Os
compose publicam como `514:1514`. Não "conserte" isso trocando o padrão para
514: o contêiner padrão deixaria de subir.

## 9. Armadilhas que já custaram bugs reais no irmão

Todas se aplicam aqui, porque o desenho é o mesmo.

- **Posse de buffer.** O item é dono da linha crua. Ela volta ao pool
  exatamente uma vez, só após confirmação. Num retry de lote o item **ainda é o
  dono** e nada é devolvido. Devolver cedo coloca a linha de um emissor dentro
  do evento de outro — silencioso, invisível sob carga.
- **Comandos do rueidis são reciclados.** Reusar o slice de comandos num retry
  corrompe os dados sem erro. Reconstrua a cada tentativa.
- **Lote retido pelo worker não está no ring.** O shutdown precisa coletá-lo de
  `Workers.Pending()` ou ele some depois de ter sido confirmado por RELP.
- **E o `hold` não pode sobrescrever.** Um worker que abandona o lote no
  shutdown o mantém registrado; se o laço chamar `Dequeue` outra vez ele pode
  voltar com um segundo lote e gravá-lo por cima. `hold` faz `append`, nunca
  atribuição, e `deliver` devolve se a posse mudou de mãos.
- **Chave primária no `ring=sqlite`.** Perder a identidade do item na releitura
  já descartou 149 de 150 eventos por colisão. A coluna `id` é separada do
  `head` por isso.
- **Cursor, não conjunto em voo.** Um ring persistente lido por janela filtrada
  contra o conjunto em voo trava quando `workers × batch` excede a janela.
- **Fronteira de histograma como string.** Emitir `0.01` num scrape e `0.010`
  no seguinte cria duas séries e quebra todo quantil, em silêncio. As strings
  são pré-computadas na inicialização.
- **Slices do pool de argumentos do `sink=postgres`.** Os arrays do `unnest`
  seguram referências à linha crua. Eles voltam ao pool **zerados**; um slice
  devolvido cheio prende buffers que já voltaram ao pool.
- **Classe `42` do SQLSTATE é fatal, nunca `logic`.** Tabela dropada
  classificada como `logic` descartaria um lote a cada 5 ms, para sempre, num
  processo que parece saudável.
- **O `sink=postgres` nunca faz DDL.** Nem `CREATE TABLE IF NOT EXISTS`. O
  `sink=mysql` também não, e o `sink=nats` nunca cria nem altera um stream.
- **`ON DUPLICATE KEY UPDATE` exige `UPDATE`, não só `INSERT`.** A sonda de boot
  do `sink=mysql` usa o mesmo comando do dreno numa transação revertida, porque
  privilégio dado por papel não aparece em `information_schema`. E nunca
  `INSERT IGNORE`: ele rebaixa `1406` e `1366` a aviso e trunca em silêncio.
- **A retentativa interna do `nats.go` relê o buffer.** `PublishMsgAsync` retenta
  sozinho em segundo plano, relendo `msg.Data` depois do retorno.
  `WithRetryAttempts(0)`, sempre; quem retenta é o worker. E
  `ReconnectBufSize(-1)`: o ring é o único buffer.

### E as que são desta pasta

- **O envelope não cabe num header NATS.** O JetStream recusa bloco de headers
  acima de 64 KiB (`10097`), e um hostname RFC 3164 longo passa disso. Aqui o
  envelope vai em `Data`, à frente da linha crua, com o tamanho em
  `Sls-Envelope-Size` (`SPEC.md` §3.8). É uma divergência deliberada do irmão;
  não "alinhe" de volta.
- **O maior evento é sete vezes o frame, e é calculado, não configurado.** O
  boot compara `max_allowed_packet` e `max_payload` com esse pior caso
  (`SPEC.md` §3.9) e sai com 3 se não couber. Um fator "realista" deixaria um
  emissor quebrado derrubar lotes confirmados de todos os outros.
- **Hostname e procid da RFC 3164 não têm teto.** Por isso as colunas de texto do
  MySQL são `LONGTEXT`: um `TEXT` recusaria um token acima de 64 KiB com `1406`,
  que é `logic` e descarta o lote inteiro.

- **O parser não pode falhar, nem entrar em pânico.** A propriedade de fuzz é
  `msg-offset + msg-size == len(raw)` **sempre**, inclusive em entrada
  truncada, com UTF-8 inválido, com `<` sem `>`, com structured-data que não
  fecha. Um `slice out of range` aqui derruba a goroutine de recepção inteira.
- **Dessincronizar a moldura octet-counting é irrecuperável.** Um `DATALEN`
  não numérico ou acima do teto **fecha a conexão**. Tentar "procurar o próximo
  `<`" grava fragmentos como se fossem mensagens.
- **Reavaliar a moldura a cada mensagem embaralha o fluxo.** A autodetecção é
  **uma vez por conexão**. Uma mensagem legítima sem PRI, começando com dígito,
  trocaria a moldura no meio e corromperia tudo o que vem depois.
- **O `\n` da moldura LF não é da mensagem.** Nem o `\0` final, nem o
  comprimento da moldura octet, nem o cabeçalho do frame RELP. Um byte de
  moldura que vaze para `raw` quebra o invariante de fidelidade e aparece no
  destino de todo mundo.
- **O `rsp 200 OK` do RELP é o `200` do irmão.** Ele não pode sair antes da
  garantia do ring, e um atalho "responde já, enfileira depois" transforma o
  único transporte confiável deste servidor no menos confiável.
- **UDP sem `SO_RCVBUF` grande perde em silêncio.** O descarte acontece no
  kernel, antes de existirmos: não aparece em métrica nossa, só em
  `/proc/net/udp`. `listen.udp_read_buffer` é o parâmetro mais importante do
  transporte.
- **`msg-ts` não é tempo.** Nunca use o timestamp da mensagem para ordenar,
  indexar ou correlacionar. Ele não tem ano nem fuso na RFC 3164, e vem de um
  relógio que ninguém auditou na RFC 5424. O tempo deste servidor é `ts`.

## 10. Estado do projeto

A spec está congelada e o código a cumpre. Todo pacote tem teste, a suíte passa com
`-race`, os quatro alvos de fuzz rodam limpos, e o corpus de mensagens reais tem 23
casos. `docs/TESTING.md` diz o que foi provado de ponta a ponta e o que ainda não.

O que já foi verificado fora dos testes unitários, numa máquina real:

- o binário com os três transportes de rede e `sink=sqlite`, contra `nc` e
  mensagens reais;
- a imagem de contêiner (10 MB, uid 65532, raiz somente leitura, sem capabilities)
  com `compose.memory-redis.yml` contra um Redis real;
- a sonda de escrita do `sink=redis` pegando um Redis rebaixado a réplica, e a
  mensagem TCP enviada durante a falha sendo segurada e entregue na recuperação;
- rsyslog e syslog-ng reais, em contêiner, pelas quatro molduras (§17.4);
- o `sink=nats` contra um `nats-server` v2.14.6 real, pela suíte de §6.4 de
  `docs/TESTING.md`, de ponta a ponta nos três modos de ring.

O `sink=mysql` rodou a suíte contra MySQL 8.4 real só em 2026-10-14
(`docs/TESTING.md` §8.1), e os seis compose de MySQL e NATS nunca subiram
(`docs/TESTING.md` §8, item 19).

Antes de qualquer deploy real faltam, em ordem: versionamento (nível acima),
integração contínua com Redis, PostgreSQL **e** os emissores vivos, o teste do
RELP com corte do servidor no meio do fluxo, e a aferição dos critérios de aceite
de `SPEC.md` §17.7, que **nunca foram medidos**.

Ao propor trabalho novo, prefira fechar esses itens a acrescentar recurso. O roteiro
pós-v1 está em `SPEC.md` §20 e nada dele entra antes de a v1 estar em produção.
