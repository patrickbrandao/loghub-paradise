# AGENTS.md — go-gelf-nats-server

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

---

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-gelf-nats-server/            <- VOCÊ ESTÁ AQUI (módulo Go independente, "GN")
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-gelf-nats-server
    cmd/go-gelf-nats-server/
    internal/
    docs/DECISOES.md              as decisões da família GELF que valem aqui e as do corte
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/                       Dockerfile, docker-compose.yml (memory -> NATS JetStream,
                                  perfil "demo" com fluent-bit), nats/, conf/gelf.ini
  go-gelf-server/ (G)             <- o pai do comportamento GELF. Referência
  go-gelf-pg-server/ (GP)         <- o irmão: o mesmo corte, com a fila loghub.inbox
  go-syslog-nats-server/ (SN)     <- o pai estrutural: o corte que este repete. Referência
  go-syslog-server/ (S)           <- pai de G e de SN
```

Servidor **GELF** do ecossistema **LogHub**, criado em 2026-10-31: é para G
exatamente o que SN é para S. Recebe GELF por UDP (chunks, gzip/zlib), TCP
(moldura `\0`) e HTTP (`POST /gelf`), converte cada documento num envelope
**JLH** `type=gelf` e o **publica** no NATS em `loghub.<space>.gelf`
(JetStream com `PubAck` por padrão, `core` opcional). Emite o heartbeat beacon
em `loghub._ctl.beacon`. Ring só em memória, emergência em JSON Lines, **sem
TLS em lugar nenhum**, sem filtro, sem rate-limit, três dependências diretas de
produção. Bridge-IN: `step = 0`.

## 2. As fontes normativas mandam, e a SPEC diz onde não

- A entrada comum da família é `tasks/gelf/DESIGN.md` (fora do git); depois da
  SPEC, quem manda é a SPEC.
- Skill `loghub-json-schema` (envelope, subject, `Nats-Msg-Id`, plano de
  controle), `go-loghub-uuidv7`, `go-loghub-ident`. **Não existe skill
  `type=gelf`**: o schema do payload é o da família,
  `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json`.
- `docs/SPEC.md` §1.3 lista os desvios deliberados (os de G: D1 `level`
  ausente = 6, D2 `host`/`short_message` ausentes aceitos, D3 schema só em
  teste, D4 nível de UUID fixo, D5 branco em `host`, D6 teto de 128 KiB). Fora
  deles, divergir é **bug**.
- `docs/SPEC.md` está congelada: discrepância entre código e spec é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite a spec por seção (`§10.7`).
- **A numeração da SPEC é a de G.** Seção removida mantém o título com a nota
  "Removido no go-gelf-nats-server"; **nunca renumere**.
- `docs/TESTING.md` registra as lacunas com honestidade.

## 3. G e SN são referência, não dependência

**Nada é importado** de outro módulo do monorepo. **Não edite G, GP nem SN** a
partir daqui.

- **Cópia literal de G** (só o module path muda): `internal/convert`,
  `internal/item`, `internal/jlhcheck`, `internal/receiver/{chunks,decompress,framing}.go`,
  com testes, corpus e `testdata/fuzz`. `internal/receiver/http.go` é o de G
  sem o HTTPS, igual ao de GP. Mudou um deles em G: a mudança vem para cá (e
  para GP) no mesmo dia, ou fica registrada como "a portar" (SPEC §22.1).
- **O corte é o de SN**: ring memory, sink NATS com a guarda de TLS, beacon,
  dump JSON Lines, `removed.go`. Uma correção de SN que valha aqui vem com teste.
- Regra de sincronia com GP: onde SP e SN não diferem, GP e GN não diferem.

O que saiu (SPEC §1.2) não volta sem decisão do usuário registrada em
`docs/DECISOES.md`: TLS (listener, HTTPS e cliente NATS), os outros sinks e
rings, filtro, rate-limit.

## 4. Fronteira de responsabilidade

| Assunto | Quem responde |
|---|---|
| Código, testes, docs e Docker **desta** pasta | você |
| Outras pastas do monorepo | o agente de cada uma |
| **Git: init, commit, branch, tag, remoto** | o nível do monorepo, e só com pedido explícito do usuário |
| **Imagem no Docker Hub** (`tmsoftbrasil/go-gelf-nats-server`) | só com pedido explícito; `docker push` nunca por conta própria |

Nunca rode `git` de dentro desta pasta. Tags seguem `go-gelf-nats-server/vX.Y.Z`.

## 5. O modelo mental

### Um ring, um sink

- **RING**: `memory`, e só. Não há chave `ring.mode`.
- **SINK**: `nats`, e só. Não há chave `sink.type`. `sink.nats.mode` escolhe
  `jetstream` (padrão) ou `core`.

O log de boot e o `gelf_config_info` dizem `ring_mode="memory"` e
`sink_type="nats"`, para que os painéis de G sirvam.

### Uma conexão NATS, dois planos

Workers e beacon dividem **uma** conexão. Os dados vão em
`loghub.<space>.gelf`; o beacon por NATS **core** em `<ctl_namespace>.beacon`,
nunca num stream. O stream nunca cobre o plano de controle (`loghub.>` é
recusado).

### Cru no ring, conversão no dreno (SPEC §2.2)

A recepção faz remontagem, moldura, corpo HTTP, descompressão com teto, **uma**
amostra de relógio, o estado do sink e o `uuid`. **Não lê o documento** (não há
filtro). O item é cabeçalho + documento descomprimido; a conversão roda no
worker. O JLH é função pura de cabeçalho + documento + `[convert]` (§3.5).

### O dump é JLH pronto, e volta sem conversão (SPEC §12)

Em FATAL ou dreno estourado o ring é convertido e escrito em JSON Lines;
`emergency.ParseLine` é a **única** porta do disco para o stream.

### O que o transporte confirma

| Transporte | Confirmação | Ring cheio, sink DOWN |
|---|---|---|
| TCP | continuar lendo | parar de ler; a janela TCP segura o emissor |
| HTTP | `202` por documento | `503` com `Retry-After: 1`; nada é segurado |
| UDP | não existe | **descarte contado** |

## 6. Mapa do código

```
cmd/go-gelf-nats-server/main.go  flags que encerram, lhident.Initialize(), internal/app
internal/
  app/        montagem, boot, shutdown, SIGHUP, reingestão, admin
  config/     registry único -> INI + flags + env + --help + dump; LOGHUB_EB_URL
              (só nats://); removed.go (as 93 chaves de G -> exit 1 / WARN)
  control/    heartbeat beacon (loghub._ctl.beacon, NATS core)
  ident/      Identity congelada e gramática de space              [lógica pura]
  jlh/        escritor JSON por append, Event, envelope, UUID, Payload [lógica pura]
  convert/    documento GELF -> payload type=gelf -> JLH (cópia de G) [lógica pura, fuzz]
  item/       Item, Head, codec, pools (cópia de G)                 [lógica pura, fuzz]
  jlhcheck/   oráculo de teste: schemas + regras semânticas         [só em teste]
  receiver/   datagram (udp, pktinfo), chunks, decompress, framing (\0), stream/tcp,
              http (POST /gelf), proxy, limits, ingest             [fuzz]
  ring/       interface + memory
  sink/       interface + nats, workers, chunk, health, instrumented
  emergency/  dump JSON Lines, ParseLine, leitor em fluxo           [fuzz]
  obs/        métricas Prometheus próprias (gelf_) + slog
```

## 7. Comandos

Todos a partir desta pasta.

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**. Sem `GELF_TEST_NATS_URL` a integração pula.

```bash
source ../tasks/infra/env.sh                 # NATS com e sem JetStream, permissões (24224), TLS exigido
go test -race -count=1 -timeout 1500s ./internal/sink/ ./internal/control/ ./internal/app/
GELF_TEST_EMITTERS=1 go test -race -count=1 -run Emitters -timeout 900s ./internal/app/
```

Fuzz (10 min cada antes de release; 60 s no dia a dia):

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$    -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$      -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzReassemble$ -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzDecompress$ -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$      -fuzztime=60s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$       -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$       -fuzztime=60s
```

Executar localmente (o stream precisa existir: o servidor nunca cria um):

```bash
nats --server nats://127.0.0.1:24222 stream add LOGHUB_GELF --config docker/nats/stream.json
mkdir -p /tmp/glnats-data
DATADIR=/tmp/glnats-data AGENT_NAME=go-gelf-nats-server HOSTNAME=dev-mac \
  LOGHUB_EB_URL='nats://127.0.0.1:24222' \
  go run ./cmd/go-gelf-nats-server --listen.udp=127.0.0.1:12201 \
  --listen.tcp=127.0.0.1:12201 --listen.http=127.0.0.1:12202 --admin.bind=127.0.0.1:19104
printf '{"version":"1.1","host":"dev","short_message":"oi"}' | nc -u -w1 127.0.0.1 12201
curl -sS -w '%{http_code}\n' --data-binary '{"short_message":"oi"}' http://127.0.0.1:12202/gelf
```

Imagem (nunca `docker push` sem pedido):

```bash
docker build -f docker/Dockerfile -t tmsoftbrasil/go-gelf-nats-server:dev .
```

## 8. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**. As únicas strings em português no código são os textos
  normativos de `parse.error`.
- Comentários explicam o **porquê**.
- **Dependências: três diretas em produção** (SPEC §17.3): `nats.go`,
  `go-loghub-ident`, `go-loghub-uuidv7`. `jsonschema/v6` só em teste. Gzip,
  zlib, deflate e HTTP são da stdlib; o parser JSON é próprio.
- **Config:** o registry é a fonte única. Env `GELF_<SEÇÃO>_<CHAVE>`. Uma chave
  de G que não existe aqui vai para `removed.go`.
- **Métricas:** prefixo `gelf_`, cardinalidade fixa; nenhum label vindo do
  emissor.
- **Identidade:** `lhident.Initialize()` só no `main`, uma vez.
- **Escrita em disco:** tudo sob `server.data_dir`, sem exceção.

## 9. Armadilhas que já custaram bugs reais

As de G §9 (GELF) e as de SN §9 (corte) valem inteiras. As mais caras:

- **Bomba de descompressão:** o teto vale **dentro** do leitor
  (`io.LimitReader(L + 1)`), nunca depois.
- **Nunca alocar pelo que o par declara** (`count` de chunks, `Content-Length`,
  PROXY v2, linha do dump).
- **A remontagem UDP é estado compartilhado**, com teto global e prazo varrido
  por relógio próprio.
- **Números JSON nunca passam por `float64`**; o dump guarda o texto deles, e
  por isso a porta `ParseLine` aceita a gramática JSON inteira, não só inteiros
  (desvio de SN, SPEC §12.2). Não "simplifique" de volta: um dump legítimo
  ficaria em quarentena.
- **HTTP `4xx` é final para o cliente**: ring cheio, sink DOWN e shutdown são
  `503` com `Retry-After`.
- **`nats.go`:** `WithRetryAttempts(0)`, `ReconnectBufSize(-1)`,
  `WithPublishAsyncTimeout`. O JLH vai em `Data`, nunca num header.
  `Nats-Msg-Id` é `<uuid>.<step>`.
- **Sem TLS, de propósito — e o `nats.go` tenta TLS sozinho** diante de
  `tls_required`. O `TLSCertCB` que recusa é o que impede: não o remova.
- **Nunca crie nem altere um stream**; valide-o no boot e em toda checagem.
- **O transporte do item não é o do `obs`**: o item (cópia de G) tem
  `tls`/`https`; `obsTransport` mapeia por nome. Uma conversão por posição
  (`obs.Transport(t)`) indexaria fora do vetor no HTTP (achado no corte).
- **`ItemOverhead` = 288**, não 256 (SN) nem 384 (G): a struct é a de G, mas
  nenhum ring daqui cria o canal `done` (112 B) que G cobra.
- **Posse de buffer**, **lote retido não está no ring**, **perda do abort com
  um dono só**, **dump só salvo depois do `link` e do `fsync`**, **uma amostra
  de relógio**: como em SN.
- **Testes portados que passam vazios:** um teste de SN adaptado precisa ainda
  exercitar algo GELF; confira que não pula (`-v`).

## 10. Estado do projeto

Ver `docs/TESTING.md` §8 (lacunas), `docs/SPEC.md` §22.1 (pendências) e
`docs/DECISOES.md` §5 (ordem de entrega). Andamento da família em
`tasks/gelf/progress-GN.md` (fora do git).
