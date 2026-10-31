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

## 2. Fuzz

```bash
go test ./internal/httpx/ -run=XXX -fuzz=^FuzzStream$ -fuzztime=60s
go test ./internal/pcap/  -run=XXX -fuzz=^FuzzReader$ -fuzztime=60s
```

`FuzzStream` confere que a divisão do fluxo em segmentos não muda as
requisições extraídas. Achou o corte da janela de ressincronização no meio de
uma palavra maiúscula (SPEC §6.6; a entrada fica no corpus). Depois da correção,
2 min e 10,7 milhões de execuções limpas (2026-10-31).

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

## 4. O que ainda não está provado

- O W/F atrás de um proxy confiável recebendo o `X-Forwarded-For` de
  `--pcap-xff` (provado só contra o servidor falso).
- HTTPS contra os servidores LogHub: eles servem HTTP em claro (o TLS fica no
  proxy reverso); TLS provado só contra o `httptest`.
- Carga com injetor e servidor em máquinas separadas: todos os números acima
  dividem a mesma CPU, e o teto medido é o da máquina, não o do servidor.
- Captura midstream real (o tcpdump iniciado com conexões já abertas): coberta
  pelos testes e pelo fuzz, não por uma captura real.
- As correções do fechamento pelo servidor de 2026-10-31 (SPEC §4.4: vaga
  liberada depois de `closing`, escrita falha depois do fechamento como
  `ErrSpent`, fechamento ocioso planejado, evento
  `requeued unwritten (connection closing)`) foram provadas só contra o
  servidor falso (`net/http` com teto por conexão), não contra W/F/P/N reais
  com `max_requests_per_conn` ou `idle_timeout`.
