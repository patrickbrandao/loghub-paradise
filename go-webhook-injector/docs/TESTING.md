# go-webhook-injector — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Números
de 2026-10-31, num Apple M2 com macOS, injetor e servidor na mesma máquina.
Comandos a partir da pasta do módulo.

Os pacotes `report` e `pcap` são cópias do `go-syslog-injector` (o pai), com as
mesmas suítes e o mesmo `FuzzReader` (TESTING do SI §1–§2). `tmpl` também, com
a regra de chaves de JSON (SPEC §5.2) e os testes dela.

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional (achou a corrida do `tls.Config` do HTTP/2, SPEC §4.2).
Leva cerca de 5 s; não precisa de nada externo. As linhas
`http: TLS handshake error ... bad certificate` no log são do teste que confere
a recusa de um certificado sem a CA.

`internal/app` roda contra servidores falsos: `httptest` (HTTP/1.1, h2c, TLS com
h1 e h2 por ALPN) e servidores TCP crus para os comportamentos que o `net/http`
não produz.

| Teste | O que prova |
|---|---|
| `TestGeneratorConcurrent` | 3000 requisições em 4 conexões: `seq` 1..3000 exatas, JSON válido, marcadores no caminho, na query e num `--header`, `Content-Type`, `--size` |
| `TestPipelining` | 5000 requisições, pipeline 32, 2 conexões, `seq` exatas |
| `TestH2C` | 2000 requisições por h2c (*prior knowledge*), 2 conexões, `HTTP/2.0` no servidor |
| `TestTLSBothProtocols` | HTTPS com CA própria em HTTP/1.1 e em HTTP/2 (ALPN); sem a CA, saída 3 |
| `TestRetryAfter429` | 50 requisições recusadas uma vez com `429`: com `--retries`, todas `202` e o servidor vê 250; sem, 50 `429` e saída 1 |
| `TestRetryGivesUpWithLastStatus` | `503` sempre: 3 tentativas cada, desfecho `503`, em HTTP/1.1 e HTTP/2 |
| `TestRequestsPerConn` | 1000 requisições, 100 por conexão: 10 conexões, nenhuma contada como quebrada |
| `TestServerClosesConnection` | servidor que responde 3 e fecha com `Connection: close`: com pipeline 1, 8 e 64, as 300 chegam exatamente uma vez (as de trás do fechamento são devolvidas e reenviadas, SPEC §4.4) |
| `TestHTTPServerClosingConnections` | servidor falso com teto por conexão (`fake.maxPerConn`: a N-ésima requisição de cada conexão sai com `Connection: close`), teto/pipeline 1/1, 1/32, 7/1, 7/8 e 50/32, 1000 requisições em 4 conexões: todas exatamente uma vez, `connections.broken` 0, e com pipeline 1 nenhum `resent on a new connection` (nada escrito atrás do fechamento). Mutação conferida em 2026-10-31: liberar a vaga antes de marcar `closing` dá eventos de reenvio com pipeline 1; `writeErr` devolvendo o erro cru dá `broken pipe`/`reset` como conexões quebradas; sem `Unwritten`, as devolvidas viram `resent` (SPEC §4.4, achado 3 da revisão do GI) |
| `TestServerHangsUp` | servidor que lê e desliga sem responder: `no_response`/`conn_error`, saída 1, nada reenviado, a execução termina |
| `TestResponseTimeout` | servidor que nunca responde: `timeout` nas 4 do pipeline; idem em HTTP/2 |
| `TestUnreachable` | porta fechada: saída 3, em HTTP/1.1 e HTTP/2 |
| `TestUUIDOutAndDuration` | `--duration` sem `--count` roda o tempo todo; `--uuid-out` com uma linha por resposta |
| `TestTemplateFileAndDirectory` | modelo JSON com objetos aninhados e `PUT`; arquivo de linhas (vazias puladas, `CR` tirado); pasta (um arquivo por corpo, em ordem de nome, com quebra de linha) |
| `TestPcapReplay` | captura sintética: `POST` com `Content-Length`, `PUT` chunked, `GET` em forma absoluta, em pipelining e em segmentos de 40 bytes, mais a direção da resposta e outra porta; `--pcap-port`, `--pcap-xff` (acrescentado ao XFF capturado), *hop-by-hop* retirados; `--pcap-path url` e `--pcap-host url` |
| `TestBadPcap`, `TestUsageErrors` | arquivo que não é captura; 22 combinações recusadas |

`internal/transport` (regressões da revisão independente de 2026-10-31):

| Teste | O que prova |
|---|---|
| `TestDeadConnGivesBackRetryAndNewRequest` | reenvio pendente e requisição nova numa conexão que o servidor desligou: as duas voltam ao worker, nenhuma vira `conn_error` (falha sem a correção) |
| `TestSpentByRetryTakesNothingMore` | com 2 por conexão, o reenvio que gasta a conexão faz o `Flush` devolver `ErrSpent`, e a requisição seguinte volta ao worker em vez de sair atrás do `Connection: close` |
| `TestIdleCloseIsPlanned` | servidor que fecha a conexão ociosa (sem `Connection: close`): o `Send` seguinte devolve `ErrSpent` (não quebra) e a requisição volta com `Unwritten`, sem desfecho; a anterior fica `200`. Mutação conferida: sem marcar `idle`, o `Send` devolve `EOF` |

`internal/httpx` (o extrator de requisições, SPEC §6.6):

| Teste | O que prova |
|---|---|
| `TestPipelinedKinds` | três requisições coladas (Content-Length, sem corpo, chunked) com segmentos de 1, 3, 7 e 1000 bytes |
| `TestResponsesAndH2CIgnored` | direção de resposta, h2c e TLS são descartados e contados |
| `TestMidstreamResync` | captura que começou no meio de um corpo acha a próxima requisição colada a ele |
| `TestGapLosesOnlyTheCutRequest` | uma lacuna perde só a requisição atingida |
| `TestIncompleteAtEndIsLost`, `TestGarbageBetweenRequestsResyncs` | fim cortado; lixo entre requisições |
| `TestHugeContentLengthRefusedEarly` | `Content-Length` de 1 GB recusado no cabeçalho, sem guardar o corpo, e a requisição seguinte achada |

`internal/source`:

| Teste | O que prova |
|---|---|
| `TestLoopBlankFileEnds`, `TestLoopFileEmptiedBetweenPassesEnds` | `--loop` numa passada sem corpo (arquivo só de linhas em branco, ou esvaziado entre passadas) termina; antes de 2026-10-31 girava a 100 % de CPU para sempre (achado da revisão do GI) |

### 1.1 Rodada de 2026-11-07 (pendências 1–8)

| Teste | O que prova |
|---|---|
| `httpx.TestFieldsKeepOrderAndCase` | campos na ordem capturada, nome como escrito, repetidos um por linha, *obs-fold* juntado; segmentos de 1, 5 e 1000 bytes (SPEC §5.5) |
| `httpx.TestLargeCandidateSplitInvariant` | linha de requisição de 20 KiB achada inteira **e** aos pedaços (antes, só inteira), no início e em ressincronização; cabeçalho acima de 1 MiB recusado dos dois jeitos; linha de chunked acima de 4 KiB igual dos dois jeitos (SPEC §6.6) |
| `httpx.TestChunkedIncremental` | upload chunked de 4 MiB em 4000 pedaços, segmentos de 1460 bytes: no máximo 3 leituras de linha por pedaço (com a reanálise antiga, 5,6 milhões) |
| `httpx.TestH2CPriorKnowledge` | h2c com `HEADERS` + 2 `CONTINUATION`, `DATA` com *padding*, `PING`, *streams* intercalados, `RST_STREAM`, *trailers*, `cookie` repartido, tabela dinâmica do HPACK; segmentos de 1, 7, 100 e inteiro dão o mesmo (SPEC §6.8) |
| `httpx.TestH2CUpgrade`, `TestH2CGapEndsDirection` | *upgrade* h2c (a requisição HTTP/1.1 e depois o HTTP/2); lacuna encerra a direção (`http2_streams_lost`), quadro inválido conta `http2_errors` |
| `app.TestReplayKeepsHeaderOrder` | servidor cru vê os campos na ordem e caixa capturadas, `X-Forwarded-For` juntado no lugar do primeiro com o IP no fim, `--header` depois |
| `app.TestReplayH2C` | captura h2c reproduzida em HTTP/1.1 (servidor cru) e em h2c (`httptest`) |
| `app.TestPcapConnFlow` | `--pcap-flows`: 3 fluxos → 3 conexões, cada uma com as requisições do seu fluxo, em ordem; `--concurrency 1` divide (`flow split`); sem a flag, uma conexão só (SPEC §6.9) |
| `app.TestRetryAfterAcrossConnections` | `429` com `Retry-After: 1` e `--requests-per-conn 1`: o reenvio sai ≥ 0,9 s depois (antes, em ~3 ms); `503` sempre com `--retries 2`: exatamente 3 tentativas por requisição (antes, ~700 por requisição até o dreno) (SPEC §4.5) |
| `app.TestNoKeepAliveAdapts`, `TestNoKeepAliveAdaptsBack` | servidor que fecha toda conexão, pipeline 32: evento `adapted`, 2000 exatamente uma vez, < 200 escritas atrás de um fechamento (sem a adaptação, 62 944); servidor que passa a manter a conexão: a sonda acha e a adaptação acaba (SPEC §4.6) |
| `app.TestVerifySQLite` | servidor falso que grava numa `inbox` SQLite: tudo achado (saída 0, `complete`); 10 perdidas + 1 processada duas vezes (saída 1, `lost_seqs` com as 10); gravada mas respondida `500` (`unexpected` 3, saída 1 pelo `500`); arquivo sem `inbox`: saída 4 e **nada enviado** (SPEC §8.6) |
| `app.TestVerifyUsageErrors` | 16 combinações recusadas com 2, sem ecoar a senha: as de antes, mais URL e elemento de lista malformados, `--verify-tls-ca` sem destino, com PostgreSQL e com arquivo inexistente |
| `verify.TestAccounting`, `TestWaitsForTheSink`, `TestNoUUID`, `TestFinalReadCatchesLateDuplicate`, `TestRanges`, `TestMarker`, `TestUUIDFloor`, `TestNoSecretsEchoed` | a contagem de SPEC §8.6.4 com tudo errado de uma vez; a espera; a leitura extra depois da completa; o marcador (`"seq":"12"` entre aspas não conta); a janela por UUIDv7; nenhuma credencial no texto |
| `verify.TestMalformedURLNeverEchoed`, `TestReadErrorRetried`, `TestNothingEverAppears` (revisão de 2026-11-07) | URL malformada e lista NATS com o 2º elemento malformado (espaço na senha, `[` sem `]`, esquema `http`, sem host) recusadas sem citar a senha, por `Kind` e por `Open`; erro de driver com a senha de qualquer elemento (ou o token) trocada, `*url.Error` genérico; leitura que falha 2 vezes e depois passa = completa, falha até o fim = `error` sem senha e impossível; destino em que nada aparece = `lost`, saída 1 (SPEC §8.6.1, §8.6.4) |
| `transport.TestIdleCloseHandsOnRetry` | `429` com `Retry-After: 3` e o servidor fechando a conexão ociosa: o reenvio é passado adiante com `Tries` 1, `Last` `429` e o prazo restante, sem desfecho (antes terminava como `429`; SPEC §4.5) |
| `httpx.TestH2ContinuationChainLinear`, `TestH2HeaderBlockCapped`, `TestH2StreamsCapped`, `TestH2BufferedCapped` | 40 000 `CONTINUATION` de 1 byte, um quadro por segmento: ~2 cabeçalhos de quadro olhados por quadro (sem a retomada, 613 milhões para 35 000 quadros); bloco de 1,2 MB e `CONTINUATION` declarando 2 MiB encerram a direção; 1001 *streams* abertos = 1000 reproduzidos e 1 perdido, sem os *trailers* do perdido virarem requisição; teto do corpo somado derruba só o *stream* que passaria (SPEC §6.8) |
| `httpx.TestResyncLinear` | 1,5 MiB de `A ` (um candidato a cada 2 bytes), com e sem LF antes da requisição verdadeira, em segmentos de 4 KiB e inteiro: a requisição é achada, ≤ 8 bytes procurados por byte e ≤ 4 cabeçalhos entregues ao `net/http` (antes, quadrático: 7,7 s por MiB; SPEC §6.6) |
| `sched.TestFlowsCapAndOrder`, `TestFlowsEvictOnlyEmptyQueue` | `--pcap-flows` com 12 fluxos intercalados em 3 vagas e servidor lento: nunca mais de 3 conexões abertas, nenhuma requisição fora de ordem no fluxo; no teto, o fluxo mais ocioso **com fila** fica, o sem fila é fechado (SPEC §6.9) |
| `pcap.TestTCPPendingBoundCountsEverySegment`, `TestTCPDrainManySegments` (os mesmos do SI e do RI) | 100 000 segmentos esvaziados pelo *snaplen* atrás de um buraco declaram a lacuna (antes, sem teto); 30 000 segmentos em ordem inversa e o buraco: saem inteiros, em ordem, em O(n log n) (SPEC §6.4) |
| `verify.TestSQLiteReader` | sempre roda: janela pelo `uuid`, leitura seguinte só depois do último `rowid`, tabela ausente recusada |
| `verify.TestPGReader`, `TestRedisReader`, `TestValkeyReader`, `TestNATSReader` | opt-in, contra os servidores reais (abaixo): janela (PostgreSQL); nos streams, o que já estava lá na checagem não é lido e o stream que nasce depois é lido inteiro (SPEC §8.6.3, mutação: partir do início faz `TestNATSReader` ler 2503 e `TestRedisReader` achar a entrada velha); cursor, o mesmo envelope adicionado duas vezes (Redis, NATS), chave que não é stream, stream ausente |

Integração opt-in (2026-11-07: PostgreSQL 18, Redis 7.4, Valkey 8.1, NATS
2.12, em contêineres `vkg-*`):

```bash
WI_TEST_NATS_URL=nats://127.0.0.1:38720 WI_TEST_PG_URL=postgres://webhook:webhook@127.0.0.1:38721/webhook \
WI_TEST_REDIS_URL=redis://127.0.0.1:38722 WI_TEST_VALKEY_URL=valkey://127.0.0.1:38723 \
  go test -race ./internal/verify/
```

**Mutação** (2026-11-07, cada uma derruba o teste que trava a regra): limite
da linha de volta a 16 KiB, cabeçalho inteiro acima de 1 MiB aceito, chunked
sem retomar de onde parou, campos fora de ordem, `cookie` sem juntar, lacuna
h2 sem encerrar a direção, `Adopt` desligado, `Due` ou `Tries` perdidos na
passagem, adaptação que nunca liga ou nunca volta, `--pcap-flows` ignorado ou
sem fechar o fluxo mais ocioso, `X-Forwarded-For` no fim, conferência sem a
leitura extra, sem ignorar a chave já vista, destino inalcançável com 2, perda
com 4. Um sobreviveu, e é esperado: o teto de 4 KiB da linha de chunked
inteira já era garantido pelo `httputil.NewChunkedReader` (a guarda nova é
redundante; o comportamento era coerente antes).

Mutação da revisão de 2026-11-07 (todas derrubam o seu teste): `Close` sem
passar adiante o reenvio na conexão ociosa; `wholeFrames` recomeçando do
início do bloco; teto do bloco, de *streams* e do corpo somado desligados;
*trailer* de *stream* perdido aceito como novo; ressincronização sem dividir
o LF, sem retomar o candidato do início e sem exigir alvo de uma palavra;
vaga do `--pcap-flows` devolvida no fechamento (passa de 3 conexões); fluxo
com fila escolhido no teto; segmento segurado sem o custo fixo. Uma sobreviveu, e é esperado: a espera pelo worker
antigo de um fluxo que reaparece fechando (`closing`) é defensiva — com a
vaga devolvida só no retorno do worker, um fluxo dividido só reabre depois
de o antigo terminar, e a fonte nunca repete o id de um fluxo encerrado.

## 2. Fuzz

```bash
go test ./internal/httpx/ -run=XXX -fuzz=^FuzzStream$ -fuzztime=60s
go test ./internal/pcap/  -run=XXX -fuzz=^FuzzReader$ -fuzztime=60s
```

`FuzzStream` confere que a divisão do fluxo em segmentos não muda as
requisições extraídas. Achou o corte da janela de ressincronização no meio de
uma palavra maiúscula (SPEC §6.6; a entrada fica no corpus). Depois da correção,
2 min e 10,7 milhões de execuções limpas (2026-10-31). Em 2026-11-07, com as
sementes h2c (prefácio e *upgrade*) e a procura incremental: 60 s, 2 milhões de
execuções limpas. Na revisão de 2026-11-07, com as sementes novas (cadeia de
`CONTINUATION`, *trailers* de um *stream* cancelado, linha longa de
candidatos): `FuzzStream` 45 s, 1,38 milhão de execuções limpas;
`FuzzReader` (o `tcp.go` mudou) 30 s, 2,1 milhões.

Contra destino real na revisão (2026-11-07, contêineres `fxf-*`): NATS 2.12
com JetStream e usuário/senha — `TestNATSReader` com
`WI_TEST_NATS_URL` = lista de dois servidores (o 2º fechado) passa; o
binário com senha errada (saída 4, `Authorization Violation`), lista com o 1º
fechado e senha errada no 2º (4), elemento com espaço na senha (2, `server 2
of the list is not a URL`), `[` sem `]` no 1º (2): a senha não aparece em
nenhuma saída. PostgreSQL 18: `TestPGReader` passa com a folga de 10 min.
Redis/Valkey e `--verify-tls-ca` contra um servidor TLS real não rodaram.

## 3. Prova contra os servidores reais (2026-10-31)

Binários dos servidores das tags atuais, `HOSTNAME=dev-mac`, portas altas.

### 3.1 `go-webhook-server` (W)

`ring=memory`, `sink=sqlite`. Cinco cargas de 5 s seguidas:

| Modo | 2xx | req/s | p99 |
|---|---|---|---|
| 1 conexão | 143 849 | 28 732 | 0,13 ms |
| 16 conexões | 374 753 (+142 460 `429`) | 74 845 | 1,0 ms |
| 8 conexões, pipeline 64 | 230 431 (+957 127 `429`) | 46 030 | 17 ms |
| 16 conexões h2c, 128 streams | 210 909 (+119 176 `429`) | 42 070 | 77 ms |
| 8 conexões, 50 por conexão | 318 097 (+58 209 `429`) | 63 542 | 0,5 ms |

Os `429` são o ring cheio (o `sink=sqlite` grava mais devagar do que a carga
chega): o backpressure do servidor funcionando. A soma dos 2xx, **1 278 039**,
é exatamente o `whs_requests_total{code="200"}` do servidor, e o
`whs_events_written_total` chegou a 1 278 039 com o ring vazio.

Depois, 300 000 requisições com pipeline 64 e `--retries 1000
--retry-max-wait 50ms`: **300 000 `200`**, saída 0, apesar de ~1 milhão de
`429` no caminho; o `events.db` terminou com 1 578 039 linhas, igual ao total
de `200`.

Depois das correções da revisão, contra um W novo (`sink=sqlite`), 200 000
requisições em 8 conexões em cada modo — pipeline 64; 7 por conexão com
pipeline 4; HTTP/2 com 64 streams — sempre com `--retries`: **600 000 `200`, 0
falhas, 0 conexões quebradas**, com 697 777 `429` absorvidos no caminho; o
`events.db` ficou com 600 000 linhas e 600 000 `uuid` distintos.

Com `sink=none` (só o custo do HTTP), antes: até ~165 mil req/s com pipeline
16–32 em 8–64 conexões; 2 918 051 requisições em sete cargas, contadas iguais
dos dois lados.

### 3.2 `go-webhook-fast-server` (F)

`sink=none`, 8 conexões com pipeline 32 por 5 s: 844 219 `200` (168 632 req/s),
igual ao contador do servidor; a resposta traz `stage`.

### 3.3 `go-webhook-pg-server` (P) e PostgreSQL 18

PostgreSQL 18 num contêiner próprio, `loghub.inbox` de
`docker/postgres/inbox.sql`. 200 000 requisições, 8 conexões, pipeline 16,
`--retries`, `--uuid-out`: 200 000 `200` (68 124 req/s); a fila tem 200 000
linhas e 200 000 `uuid` distintos, e a lista do `--uuid-out` e a da tabela são
**idênticas** (`comm -3` vazio).

### 3.4 `go-webhook-nats-server` (N) e NATS JetStream

`nats-server` 2.14.6, stream `LOGHUB_WEBHOOK` de `docker/nats/stream.json`.
Primeiro com o `max_payload` padrão (1 MiB): o N recusou o boot, com razão
(precisa de 8 MiB, SPEC do N §8.7.5), e o injetor saiu com 3 (alvo
inalcançável). Com `max_payload: 8MB`: 300 000 requisições por h2c, 16
conexões × 64 streams, `--retries`: 300 000 `200` (57 481 req/s), e o stream
ficou com **300 000** mensagens.

### 3.5 tcpdump real, num container Linux

No macOS a captura exige root; a prova roda num `alpine:3.22` com `tcpdump`,
com os binários linux/arm64 do W e do injetor:

```sh
whs --ring.mode=memory --sink.type=none --server.bind=127.0.0.1:8080 ... &
tcpdump -i lo -B 65536 -U -s 0 -w lo.pcap "tcp port 8080" &
wi --url 'http://127.0.0.1:8080/cap/{seq}?r=x' --template '{"seq":{seq},"pad":"{rand:200}"}' \
   --header 'X-Seq: {seq}' --count 3000 --concurrency 3 --pipeline 16
wi --url http://127.0.0.1:8080/big --count 200 --size 20000
sleep 3; kill -INT %tcpdump
```

5400 pacotes, 0 descartados pelo kernel, 3200 `200` no servidor. Replay no
host, contra um W com `sink=sqlite`, em 4 conexões com pipeline 8: **3200
`200`**, `http_requests` 3200, 4 direções de resposta descartadas, nenhuma
lacuna nem ressincronização. No `events.db`: as 3000 `/cap/N` com o `seq` do
corpo igual ao N do caminho, o `X-Seq` igual, o `Content-Length` igual ao corpo;
os 200 corpos de 20 000 bytes (vários segmentos TCP cada) inteiros e com `seq`
distintos.

### 3.6 Conferência no destino (2026-11-07)

Binários de W, N, P e F compilados do worktree; NATS 2.12 (`max_payload 8MB`,
streams `LOGHUB_WEBHOOK_REQUEST` e `LOGHUB_WEBHOOK` dos `docker/nats/stream.json`),
PostgreSQL 18 (`docker/postgres/inbox.sql`), Redis 7.4 e Valkey 8.1. 100 000
requisições cada, 8 conexões, pipeline 16 (ou h2c com 64 streams),
`--retries 1000 --retry-max-wait 50ms`:

| Servidor e destino | Conferência |
|---|---|
| W `sink=nats` (HTTP/1.1 e h2c) | 100 000 / 100 000, `complete`, saída 0 |
| W `sink=postgres` (`ring=sqlite`) | 100 000 / 100 000, saída 0 |
| W `sink=redis`, Redis 7.4 (`redis://`) | 100 000 / 100 000, saída 0 |
| W `sink=redis`, Valkey 8.1 (`valkey://`) | 100 000 / 100 000, saída 0 |
| W `sink=sqlite` | 100 000 / 100 000, saída 0 |
| N (NATS), P (`postgresql://`), F `sink=sqlite` | 100 000 / 100 000 cada, saída 0 |
| modelo com o mesmo `seq` em 3 requisições (PostgreSQL) | `duplicated` 2, saída 1 |
| cópia congelada do `events.db`; Redis errado | `lost` 500 / 200, `lost_seqs` `1-500` / `1-200`, saída 1 |
| NATS sem `--verify-stream` com os dois streams padrão | recusado antes de enviar, saída 4 |
| PostgreSQL inalcançável | nada enviado, saída 4, senha não ecoada |

Achado da prova: a coluna `payload` da `inbox` SQLite é o `payload` do JLH, não
o envelope inteiro (o texto da W SPEC §3.7 sugere o contrário); a primeira
versão lia `$.payload.content` e não achava nenhum marcador (só o `uuid`
salvava a contagem). Corrigido antes do commit.

## 4. O que ainda não está provado

- O W/F atrás de um proxy confiável recebendo o `X-Forwarded-For` de
  `--pcap-xff` (provado só contra o servidor falso).
- HTTPS contra os servidores LogHub: eles servem HTTP em claro (o TLS fica no
  proxy reverso); TLS provado só contra o `httptest`.
- Carga com injetor e servidor em máquinas separadas: todos os números acima
  dividem a mesma CPU, e o teto medido é o da máquina, não o do servidor.
- Captura midstream real (o tcpdump iniciado com conexões já abertas): coberta
  pelos testes e pelo fuzz, não por uma captura real.
- A adaptação a servidor sem keep-alive (SPEC §4.6) e o `--pcap-flows` foram
  provados só contra servidores falsos; o replay h2c, só com capturas
  sintéticas (feitas com o `Framer` do próprio x/net), não com um tcpdump de
  h2c real.
- A conferência contra Valkey 7.2/9.0, Redis Cluster ou Sentinel, e contra
  NATS com TLS (`tls://`) ou credenciais: não rodou.
- As correções do fechamento pelo servidor de 2026-10-31 (SPEC §4.4: vaga
  liberada depois de `closing`, escrita falha depois do fechamento como
  `ErrSpent`, fechamento ocioso planejado, evento
  `requeued unwritten (connection closing)`) foram provadas só contra o
  servidor falso (`net/http` com teto por conexão), não contra W/F/P/N reais
  com `max_requests_per_conn` ou `idle_timeout`.
