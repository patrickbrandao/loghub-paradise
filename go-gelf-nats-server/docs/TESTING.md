# go-gelf-nats-server — Testes

Como testar, o que cada suíte prova e o que ainda **não** está provado. As
regras de teste estão na SPEC §18; este arquivo registra o estado. Fechou uma
lacuna, atualize §8; achou outra, acrescente.

Toda a família (G, GP, GN) roda contra a infraestrutura compartilhada de
`tasks/infra/` (fora do git), quando os serviços reais são pedidos.

---

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. Sem as variáveis `GELF_TEST_*`, as suítes do NATS e
os testes da app que passam do passo 7 do boot **pulam** (o NATS é o único
destino); rodam os unitários, os de recepção com sockets reais, o corpus, o
oráculo, a config, o dump e as recusas de boot (exit 1). Tempo: ~40 s.

## 2. Corpus dourado, contratos e oráculo (SPEC §18.2)

```bash
go test -race -run 'Corpus|Schema|Semantic|Golden' -v ./internal/convert/ ./internal/jlhcheck/
```

`internal/convert` e `internal/jlhcheck` são cópias literais de G: o corpus
`testdata/corpus/*.gelf` + `.want.json` (Docker gelf driver, graypy,
logstash-gelf, Logstash, Go `gelf`, malformados), o `jlh-gelf-payload.schema.json`
e as regras semânticas. Nada mudou; um diff com o G (module path normalizado)
deve ser vazio.

## 3. Fuzz (SPEC §18.3)

```bash
go test ./internal/convert/   -run=XXX -fuzz=^FuzzConvert$    -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzFrame$      -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzReassemble$ -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzDecompress$ -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=^FuzzProxy$      -fuzztime=60s
go test ./internal/item/      -run=XXX -fuzz=^FuzzHead$       -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=^FuzzRead$       -fuzztime=60s
```

Rodada de 2026-10-31, 60 s cada (`darwin/arm64`, Go 1.27.2), **sem achado**:

| Alvo | Execuções | Novos interessantes |
|---|---|---|
| `FuzzConvert` | 47 682 | 23 |
| `FuzzFrame` | 259 069 | 38 |
| `FuzzReassemble` | 38 312 | 40 |
| `FuzzDecompress` | 134 646 | 116 |
| `FuzzProxy` | 93 509 | 13 |
| `FuzzHead` | 1 032 059 | 82 |
| `FuzzRead` | 2 751 | 3 |

O `FuzzRead` é lento por desenho (escreve e relê um dump por execução). Antes de
release: 10 min cada.

## 4. Benchmarks

Os de G (`BenchmarkConvert`, `BenchmarkTCPReceive`, `BenchmarkIngestAccept`) e
os de SN que vieram com os pacotes. Nenhum número de GN foi registrado: os
alvos de SPEC §18.8 continuam **não medidos** (§8).

## 5. Integração em processo (`internal/app`)

A pilha real (`run()`), contra um `nats-server` real, cada teste com um `space`
e um stream próprios criados a partir de `docker/nats/stream.json` (o
`loghub.*.gelf` estreitado ao `space` do teste). Falhas do destino são
injetadas decorando o sink real (`App.wrapSink`). O que é provado (lista
completa em SPEC §18.5): todos os transportes até o stream (UDP plano e em 3
chunks gzip, TCP `\0`, HTTP plano e gzip; `dst-ip` por PKTINFO), PROXY v1/v2,
`POST /gelf` com cada `Content-Encoding` e cada recusa `4xx`, `503` com o sink
DOWN, documento acima do teto sem perder a moldura, documentos vazios e
separadores, fidelidade do JLH, `SIGHUP`, dump e reingestão exatos (com
`keep_raw=always` para casar cada linha ao documento), dump danificado,
shutdown gracioso, segundo sinal, abortos em cada janela, `SIGQUIT`, beacon,
`core`, `LOGHUB_EB_URL`, binário como subprocesso (códigos 100–114 do
`go-loghub-ident`, `--version`, recusas de config).

## 6. NATS

### 6.1 A infraestrutura (`tasks/infra`)

```bash
tasks/infra/up.sh            # se não estiver de pé (compartilhada: nunca down.sh com outro agente testando)
source tasks/infra/env.sh    # exporta GELF_TEST_NATS_URL (24222, max_payload 8MB), _NOJS_URL (24223), _PERM_URL (24224), _TLS_URL (24225)
```

`env.sh` também exporta `GELF_TEST_NATS_PERM_URL` (`127.0.0.1:24224`): desde
2026-10-31 o `perm.conf` de `tasks/infra` nega `loghub.*.gelf` ao usuário
`nopub` (além de `loghub.*.syslog` e `loghub.*.webhook`), com os usuários de
`natsPermSetup` (`internal/sink/review_port_test.go`). Antes disso a rodada de
criação usou um `nats-server` próprio em `127.0.0.1:24254`; não é mais preciso.

```bash
go test -race -count=1 -timeout 1500s ./internal/sink/ ./internal/control/ ./internal/app/
```

### 6.2 O que cada suíte prova

- `internal/sink`: o contrato do stream (`docker/nats/stream.json` cobre
  `loghub.<space>.gelf` de todo `space` de um segmento e nunca o plano de
  controle), a validação no boot e em toda checagem, as classes de erro, a
  permissão negada (publicação, `$JS.API`, `_INBOX`), as credenciais recusadas,
  o servidor sem JetStream (FATAL imediato), o servidor que exige TLS
  (`GELF_TEST_NATS_TLS_URL`: FATAL nomeando o TLS, **nenhum `ClientHello`
  enviado**, provado por um proxy que grava os bytes), `core`, deduplicação por
  `Nats-Msg-Id`, futuros contaminados, a mensagem recusada sozinha no cliente.
- `TestNATSOversizedMessageIsRefusedAlone` **pula** contra o servidor de 8 MB:
  rode-o com `GELF_TEST_NATS_URL=nats://127.0.0.1:24226` (o `js1m`, 1 MB).
- `internal/control`: o beacon (`loghub._ctl.beacon`, NATS core).
- `internal/app`: §5.

### 6.3 Rodada de 2026-10-31

Tudo verde: `internal/sink` 40 s, `internal/control` 1,5 s, `internal/app` 76 s
(146 testes, 2 pulados: os emissores, §7); o oversized contra o `js1m`, verde
nos dois modos.

## 7. Emissores reais (SPEC §18.7)

```bash
GELF_TEST_EMITTERS=1 go test -race -count=1 -run Emitters -timeout 900s ./internal/app/
```

(com `GELF_TEST_NATS_URL`). Rodada de 2026-10-31, Docker Desktop, **verde**:

| Teste | Emissor | O que chegou ao stream |
|---|---|---|
| `TestEmittersFluentBit` | `fluent/fluent-bit:4.0.3`, saída `gelf` | 5 documentos por UDP com `Compress On` e `Packet_Size 1420` (gzip, `chunks ≥ 2`, campo `blob` de 12 000 caracteres inteiro) e 5 por TCP `\0`; `level`, `host` e `_origin` certos; nenhum `chunk_*`, `decompress` ou `too_large` |
| `TestEmittersDockerLogDriver` | `docker run --log-driver=gelf` (`alpine:3.22`) | 3 linhas por UDP com `gelf-compression-type=gzip` e 3 por TCP; `short-message` certo, `fields.container_name` do driver presente |

Cada mensagem passou no oráculo e na conferência de headers NATS (`checkMsg`).
Os contêineres são `--rm` ou removidos pelo `t.Cleanup`.

O compose de demonstração (`docker compose --profile demo up`) foi validado
com `docker compose config`; não foi subido na rodada.

## 8. Lacunas conhecidas

1. **Desempenho nunca medido** (SPEC §18.8).
2. **Sem TLS e sem HTTPS nos emissores**, por desenho; G os cobre.
3. **O HTTP com vários documentos por corpo** (o output `http` do fluent-bit
   com `Format gelf`) não é testado com emissor real: é a pendência SPEC §22.1
   item 2, herdada de G.
4. **O compose não foi subido** na rodada (só `config`), nem a imagem foi
   provada contra um NATS de verdade fora do processo de teste (`--version` sim).
5. **Várias URLs NATS**: o veredito do boot pode depender da ordem em que o
   cliente tenta os servidores (herdado de SN).
6. **Os contadores da recepção sem *padding*** (herdado de SN): lacuna de
   medição, sem defeito observável.
7. **`ItemOverhead` = 288 B** (era 384 até 2026-10-31) é conferido contra a
   struct nos dois sentidos (cobre a struct e o slot; não passa da necessidade
   arredondada a 32 B), mas o custo real por item num ring cheio não foi medido.

## 9. Histórico

- **2026-10-31** — criação por corte de G como SN corta S (DECISOES §5);
  suíte rápida, NATS real, emissores reais, fuzz 60 s e imagem local verdes.
- **2026-10-31** — porte das correções de G e da revisão dos cortes
  (keep-alive, `http.bulk`, prazo de escrita, lápide do `chunk_overflow`,
  buffers do corpo, conversor/oráculo, documento vazio no dreno,
  `ItemOverhead` 288, `obsTransport` e leitor do dump iguais aos do GP).
  `go test -race ./...` verde com `tasks/infra/env.sh`, inclusive
  `TestNATSPermissionDeniedIsFatal` contra o `:24224`; fuzz 60 s de
  `FuzzConvert`, `FuzzReassemble`, `FuzzDecompress` e `FuzzRead` verde; cada
  teste novo conferido por mutação (falha sem a correção).
