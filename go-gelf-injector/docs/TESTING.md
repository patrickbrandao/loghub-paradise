# go-gelf-injector — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Números
de 2026-10-31, num Apple M2 com macOS (e containers Linux arm64 do Docker
Desktop para o tcpdump). Comandos a partir da pasta do módulo.

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. Leva cerca de 15 s; não precisa de nada externo (a
conferência no SQLite roda sempre; NATS, PostgreSQL e Redis/Valkey são
opt-in, §1.1).

Os testes ponta a ponta falam com os receptores de `internal/gelftest`, escritos
**à parte** dos pacotes `gelf` e `extract` (um teste que lesse com o código do
emissor não pegaria uma regra de fio que os dois erram igual). Eles aplicam as
regras estritas dos servidores: chunks por (origem, id) com `count` 1..128,
`seq < count`, prazo de 5 s; gzip/zlib pelos primeiros bytes; teto de 256 KiB
dentro do leitor; `\0` no TCP com o último documento aceito no EOF; HTTP `POST
/gelf` com `Content-Encoding`, `202`, `503` + `Retry-After`, `413`, `405`,
`404`, `400`; PROXY v1/v2 lido byte a byte.

| Pacote | O que prova |
|---|---|
| `gelf` | compressão e detecção nos três formatos; teto dentro do leitor (bomba de gzip de 64 MiB recusada a 256 KiB); `NumChunks` nas bordas (128 × 8180 e +1); remontagem em ordem aleatória, duplicata tardia de mensagem completa, mesma id de outra origem, prazo, cabeçalhos inválidos, `Flush`; prazo de 5 s exato no chunk que completaria (`TestReassemblyDeadlineExact`) e id reusado da mesma origem dentro de 5 s remontado, com a cópia idêntica ainda duplicata (`TestReassemblyIDReuse`) |
| `extract` | moldura `\0` em quatro divisões; quadro não-objeto no estrito e no não estrito; último quadro cortado × inteiro; lacuna no meio de um quadro; midstream GELF e midstream HTTP com corpo gzip (e linha terminada só em LF); midstream de GELF de logs de acesso com ` HTTP/1.1` dentro do JSON fica GELF (5 documentos, em divisões de 1, 7 e inteira); caminho do `POST` no estrito (`/gelf`, `.../gelf`, forma absoluta, query) × outros caminhos pulados, e qualquer caminho com `--pcap-port` (`TestHTTPPathStrict`); detecção (GELF, HTTP, resposta, SSH, TLS, vazio); datagramas planos, zlib, DNS e gzip quebrado |
| `transport` | os chunks no fio byte a byte (magic, id único, `seq`, `count`, tamanho ≤ `--chunk-size`, gzip do documento); mensagem pequena num datagrama só; `\0` e `nul_byte`; compressão recusada no TCP; os dois testes de revisão do cliente HTTP do WI (conexão morta devolve reenvio e requisição nova; reenvio que gasta a conexão); fechamento ocioso do servidor é planejado (`TestIdleCloseIsPlanned`: `ErrSpent`, requisição devolvida sem ir ao fio); escrita TCP parcial: `sent` para os quadros inteiros, `write_error` para o cortado e os seguintes (`TestStreamPartialWrite`) |
| `source` | gerador com `host` escapado (aspas, chaves), `level`, `_seq`, `_run`; `--payload-size` exato e aleatório, e cada N de `len+10` em diante exato (`TestPadSizes`); `--loop` termina numa passada sem documento: arquivo só de linhas em branco, arquivo e captura esvaziados entre passadas (`TestLoop*`; antes girava a 100 % de CPU) |
| `app` | TCP 5000 em 4 conexões com `seq` 1..5000 exatas; UDP chunked+gzip (300 × 30 000 bytes, 3 chunks), zlib e none com `--chunk-size 1420`; `too_large` sem nenhum chunk no fio; TLS com CA própria e PROXY v2 com origem anunciada; TLS sem CA → saída 3; HTTP com pipelining 16 e gzip; `503` reenviado com `--retries` (zlib/`deflate`) e `503` final sem eles; HTTPS + PROXY v1 + `--requests-per-conn`; `404` de outro caminho; servidor que responde `Connection: close` na 1ª, 7ª ou 50ª requisição de cada conexão, com pipeline 1, 8 e 32: 1000 de 1000 exatas, nenhuma conexão contada como quebrada, e com pipeline 1 nada escrito atrás do fechamento (`TestHTTPServerClosingConnections`); `--duration` sem `--count` (`TestDurationWithoutCount`); alvo fechado → saída 3; servidor que fecha a cada 100 (um desfecho por mensagem); modelo com JSON aninhado e marcadores; arquivo (CR, linhas vazias, não-JSON, NUL) e `--loop`; 26 combinações recusadas (inclusive `--progress 5x` e `--run` com aspas); captura sintética com todos os caminhos (UDP plano/gzip/zlib, chunks invertidos e repetidos, TCP midstream, HTTP pipelined gzip + GET, DNS, resposta HTTP) reproduzida em TCP, em UDP chunked zlib no ritmo original, e em HTTP com `--pcap-port` e `--loop`; **três capturas reais** em `testdata/captures/` (§3.3) |

Rodada de 2026-11-07 (pendências 1–7, e o alinhamento com o SI e o RI; cada
teste falha sem a correção, conferido por mutação, §3.8):

| Pacote | O que prova |
|---|---|
| `verify` | `Parse` dos oito esquemas com `--verify-space`/`--verify-key` (stream Redis `loghub.<space>.gelf`, subject NATS, `nats://` sem parâmetros, `--verify-key` recusado no PostgreSQL), `valkey(s)://` → `redis(s)://`, `destination` só com esquema e host, nenhuma URL ou senha em erro, troca da senha no texto do driver (`TestParse`); só JLH `type=gelf` do `run` contam, `seq` número ou string de dígitos, `0`/fração/ausente = `unparsable` (`TestAddJLH`); as oito classes de §10.1 e o que é erro (`TestClassify`); `lost_seqs` em faixas, no máximo 20 (`TestLostSeqs`); o piso UUIDv7 (`01a101a2-aa7b-0000-…` para o ms do exemplo da SPEC do G) e a ordem (`TestUUIDFloor`); a espera: lê até achar tudo e mais uma vez 1 s depois, `lost` no prazo, nenhuma leitura boa = falha (`TestRunWaits`); o `gelf_events` do SQLite com a DDL do G, linha fora da janela ignorada, filtro de espaço, arquivo ausente nunca criado (`TestSQLiteDest`); e, opt-in, `loghub.inbox` (`TestPostgresDest`), stream Redis 7.4 **e** Valkey 8.1 com chave de outro tipo recusada (`TestRedisDest`), JetStream `LOGHUB_GELF` com stream ausente recusado (`TestNATSDest`) — cada destino com 1, 2, 3, 3, 5 e um registro de outra execução: `found` 4, `lost_seqs` ["4"], `duplicated` 1 |
| `extract` | cópias do `-i any` com a chave do SI (porta de origem, identificação IP, tamanho, FNV-1a): saída+entrada = 1, enviado 2 vezes com a **mesma** identificação = 2 (também com as cópias atrasadas), uma cópia perdida, um ponto só nunca deduplica em `auto`, outra identificação IP, outro documento, fora da janela de 50 ms, `on` (toda cópia na janela), `off`, outra porta de origem (`TestDatagramCopies`); último quadro de direção fechada × cortada, estrito e não estrito, em 1, 3 e 100 bytes (`TestTailClosedVersusCut`); `FuzzStream` ganhou o `closed` |
| `pcap` | `FIN` = fechada, fim de captura = não, `RST` logo depois do último byte = fechada, `RST` adiante de um buraco = não (`TestTCPEndClosed`); ponto de captura do SLL2 (interface, tipo), SLL e pcapng (`TestVantage`); do RI: PKTAP lê o tipo de enlace de `pth_dlt` e cada `pth_ifname` é um ponto (`TestPKTAPInnerLinkType`), fragmentos esperando no fim da passada contam perdidos (`TestFragmentsWaitingAtEndAreLost`) |
| `httpx` | achado do `FuzzStream` desta rodada (código do WI): um método maior que a janela de ressincronização, fatiado byte a byte, era cortado e a cauda virava uma requisição que o fluxo inteiro não tinha; a entrada fica em `testdata/fuzz/FuzzStream/982beb276083681b` |
| `transport` | `chunked` byte a byte e relido pelo `net/http` (`TestAppendRequestChunked`); adaptação: 1 fechamento não adapta, uma resposta 2 zera, 2 seguidos adaptam, restaura, pipeline 1 nunca adapta (`TestAdaptive`); `FlowDialer` (`TestFlowDialer`) |
| `app` | `--http-chunked 7` com gzip e pipeline 4: 500 de 500 e 500 requisições `chunked` no receptor (`TestHTTPChunked`); servidor que fecha toda conexão com pipeline 32: adapta uma vez e no máximo 62 reenvios (sem a adaptação, ~8800) e um keep-alive nunca adapta (`TestHTTPAdaptsToClosingServer`); 3 conexões TCP (uma IPv6) e 2 pares UDP intercalados → 5 conexões, o PROXY de cada uma com a origem capturada, a ordem de cada fluxo; sem a flag 1 conexão (`TestPcapFlows`); duas conexões no mesmo par (porta reusada) → 2 conexões (`TestPcapFlowsEnd`); 260 pares UDP duas vezes cada com `--concurrency 1`: passa de 256, o mais antigo fecha e reabre, 520 de 520 (`TestPcapFlowsEviction`); UDP: um socket por par (`TestPcapFlowsUDP`); 20 recusas novas, inclusive senha nunca ecoada (`TestUsageRound`); destino ausente = saída 4 **sem enviar nada**, destino vazio = 1, destino completo = 0 (`TestVerifyExitCodes`); duas capturas reais novas em `testdata/captures/` (§3.7) |

### 1.1 Conferência contra destinos reais (opt-in)

```bash
docker run -d --name vkh-gi-nats   -p 127.0.0.1:38821:4222 nats:2.12-alpine -js
docker run -d --name vkh-gi-pg     -p 127.0.0.1:38822:5432 -e POSTGRES_PASSWORD=pw \
  -v $PWD/../go-gelf-server/docker/postgres/inbox.sql:/docker-entrypoint-initdb.d/inbox.sql:ro postgres:18-alpine
docker run -d --name vkh-gi-redis  -p 127.0.0.1:38823:6379 redis:7.4-alpine
docker run -d --name vkh-gi-valkey -p 127.0.0.1:38824:6379 valkey/valkey:8.1-alpine
GELF_TEST_VERIFY_NATS=nats://127.0.0.1:38821 \
GELF_TEST_VERIFY_PG='postgres://postgres:pw@127.0.0.1:38822/postgres?sslmode=disable' \
GELF_TEST_VERIFY_REDIS=redis://127.0.0.1:38823,valkey://127.0.0.1:38824 \
  go test -race -v ./internal/verify/
docker rm -f vkh-gi-nats vkh-gi-pg vkh-gi-redis vkh-gi-valkey
```

O teste NATS cria o stream `LOGHUB_GELF` (`loghub.*.gelf`) se faltar, como o
`docker/nats/stream.json` do G. Valkey 7.2 e 9.0 não foram rodados aqui: o
conferidor usa só `TYPE` e `XRANGE`, que não mudam entre versões.

## 2. Fuzz

Antes de release, 10 min cada; 60 s no dia a dia:

```bash
go test ./internal/extract/ -run=XXX -fuzz=^FuzzStream$      -fuzztime=60s
go test ./internal/extract/ -run=XXX -fuzz=^FuzzDatagrams$   -fuzztime=60s
go test ./internal/gelf/    -run=XXX -fuzz=^FuzzReassemble$  -fuzztime=60s
go test ./internal/gelf/    -run=XXX -fuzz=^FuzzDecompress$  -fuzztime=60s
go test ./internal/pcap/    -run=XXX -fuzz=^FuzzReader$      -fuzztime=60s
go test ./internal/httpx/   -run=XXX -fuzz=^FuzzStream$      -fuzztime=60s
```

- `FuzzStream` (extract): nada derruba o extrator de fluxo, e a mesma entrada
  inteira ou fatiada (1 a 32 bytes), estrita ou não, midstream ou não, dá **os
  mesmos documentos** — inclusive a decisão GELF × HTTP no meio de um fluxo.
- `FuzzDatagrams`: nada derruba o caminho UDP; o estrito só entrega objetos.
- `FuzzReassemble`: bytes arbitrários como chunks não derrubam a remontagem, e
  uma mensagem fatiada, embaralhada e com chunks repetidos volta inteira
  **exatamente uma vez**.
- `FuzzDecompress`: nenhuma entrada passa do teto.

2026-10-31: os seis passaram 60 s cada (4,7 M execuções do `FuzzStream`, 2,8 M
do `FuzzReassemble`), sem achado. Depois das correções da revisão (marca
midstream com fim de linha, prazo e id reusado na remontagem), `FuzzStream`,
`FuzzReassemble` e `FuzzDatagrams` de novo 60 s cada: ver §3.5.
2026-11-07: na primeira passada da rodada o `FuzzStream` do `httpx` achou um
defeito do código herdado do WI (método maior que a janela de
ressincronização, fatiado byte a byte, virava requisição; corrigido, entrada
em `internal/httpx/testdata/fuzz/FuzzStream/`). Depois de todas as mudanças,
os seis 60 s cada, sem achado: `FuzzStream` (extract, agora também com
`closed`) 2,55 M execuções, `FuzzDatagrams` 1,43 M, `FuzzReassemble` 0,36 M,
`FuzzDecompress` 3,10 M, `FuzzReader` 6,28 M, `FuzzStream` (httpx) 1,31 M
(máquina com carga média 20–30 de outros agentes).

## 3. Prova com tráfego real (2026-10-31)

### 3.1 Carga: 100 000 por transporte

`GELF_TEST_LOAD=1 go test ./internal/app -run TestLoad -v` manda 100 000
mensagens por transporte aos receptores estritos e confere cada `seq`
exatamente uma vez (`GELF_TEST_LOAD_N` muda a contagem,
`GELF_TEST_LOAD_UDP_RATE` a taxa do UDP). Injetor e receptor no mesmo M2:

| Transporte | Forma | Vazão | Latência p50 / p99 | Recebidas |
|---|---|---|---|---|
| UDP chunked + gzip | `--payload-size 20000` (2 chunks de 8192), 2 sockets, `--rate 20000` | 17 200 msg/s (344 MB/s de documento) | 93 / 215 µs | 100 000 |
| UDP chunked + gzip | idem, `--rate 10000` | 10 000 msg/s | 87 / 174 µs | 100 000 |
| TCP | 4 conexões | 1,32 M msg/s | 7 / 65 µs | 100 000 |
| TLS | 4 conexões | 1,04 M msg/s | 9 / 81 µs | 100 000 |
| HTTP | 4 conexões, `--pipeline 32` | 279 000 req/s | 373 / 1229 µs | 100 000 |
| HTTP + gzip | idem, `--compress gzip` | 134 000 req/s | 827 / 1917 µs | 100 000 |

No UDP o limite é o gzip de 20 KB aleatórios por mensagem no injetor; a
primeira tentativa, com o receptor descomprimindo numa goroutine só, perdeu
70% no buffer do kernel sem o injetor saber (é a natureza do UDP: `sent` é "o
kernel aceitou") — o receptor de teste passou a ler numa goroutine e
descomprimir em quatro.

### 3.2 tcpdump real num container Linux

No macOS a captura exige root; a prova roda em `alpine:3.22` com `tcpdump`, os
binários Linux do injetor e do receptor estrito (`go test -c ./internal/gelftest`,
o teste `TestSink` servindo UDP/TCP/HTTP até um arquivo de parada):

1. o receptor sobe; dois tcpdump capturam as portas, `-i any` (SLL2) e `-i lo`
   (Ethernet), com `-B 65536 -U`;
2. o injetor manda 3000 UDP chunked+gzip (`--payload-size 20000`), 3000 UDP
   zlib, 2000 UDP sem compressão em chunks de 1420, 6000 TCP em 3 conexões e
   3000 HTTP gzip com pipelining 8; o receptor conta 8000 UDP (5000 chunked, 0
   incompletos), 6000 TCP, 3000 HTTP;
3. cada captura (52 MB) é reproduzida no Mac em TCP, em UDP (gzip, 8192) e em
   HTTP (gzip, pipelining 8) aos receptores estritos
   (`GELF_TEST_PCAP=... GELF_TEST_PCAP_RUNS=udpc:3000,udpz:3000,udpn:2000,tcpa:6000,httpa:3000 go test ./internal/app -run TestReplayCapture`):
   **17 000 de 17 000** nas duas capturas, cada `seq` uma vez, 12 000 chunks
   remontados, 0 lacunas, 0 bytes pulados, 0 chunks inválidos ou duplicados;
4. **midstream**: dois injetores TCP a 500/s por 6 s, o tcpdump começando 2 s
   depois; o replay ressincroniza as duas direções (2139 bytes pulados, os
   quadros parciais do começo) e entrega 1994 + 1994 documentos, cada execução
   uma sequência contígua sem repetidos, até a última (`run:0` no
   `GELF_TEST_PCAP_RUNS`).

O roteiro (refeito a cada release, fora da suíte por exigir Docker):

```sh
GOOS=linux CGO_ENABLED=0 go build -o w/go-gelf-injector ./cmd/go-gelf-injector
GOOS=linux CGO_ENABLED=0 go test -c -o w/sink.test ./internal/gelftest
# no container alpine:3.22 com tcpdump, binários em /w
GELF_SINK=udp=127.0.0.1:12201,tcp=127.0.0.1:12201,http=127.0.0.1:12202 \
  GELF_SINK_STOP=/w/stop GELF_SINK_OUT=/w/sink.jsonl ./sink.test -test.run '^TestSink$' &
tcpdump -i any -B 65536 -U -s 0 -w any.pcap 'port 12201 or port 12202' &
./go-gelf-injector --transport udp --count 3000 --payload-size 20000 --rate 3000 --run udpc
./go-gelf-injector --transport tcp --count 6000 --concurrency 3 --run tcpa
./go-gelf-injector --transport http --count 3000 --pipeline 8 --compress gzip --run httpa
sleep 4; kill -INT %2; touch /w/stop
# no Mac
GELF_TEST_PCAP=w/any.pcap GELF_TEST_PCAP_RUNS=udpc:3000,tcpa:6000,httpa:3000 \
  go test ./internal/app -run TestReplayCapture -v
```

### 3.3 Emissores GELF reais

Capturados com tcpdump num container que divide a rede com o emissor, com o
receptor estrito gravando o que aceitou (`GELF_TEST_PCAP_EXPECT=<sink.jsonl>`
no `TestReplayCapture` compara o replay com isso, documento a documento):

- **Driver `gelf` do Docker** (`docker run --log-driver gelf`, o daemon do
  Docker Desktop, alcançado por um container `--net=host`): UDP gzip com
  `gelf-address=udp://127.0.0.1:12221` (200 linhas + uma de 40 000 bytes que o
  Docker parte em 3 documentos, chunked) e TCP. 403 de 403 idênticos.
- **fluent-bit 4.0.3**, saída `gelf` (`Mode udp` com `Compress`, `Mode udp`
  com `Packet_Size 64` para chunking, `Mode tcp`), no namespace de rede do
  container do tcpdump: 620 de 620 idênticos (40 chunks).

As três capturas pequenas (injetor: 40 UDP chunked+gzip em 4096, 60 UDP zlib,
150 TCP, 80 HTTP gzip; Docker; fluent-bit) e os documentos que o receptor vivo
aceitou ficam em `internal/app/testdata/captures/` e são reproduzidas em TCP,
UDP e HTTP pela suíte rápida (`TestRealCaptures`).

### 3.4 Contra o `go-gelf-server` (revisão independente, 2026-10-31)

Feito pelo revisor (`tasks/gelf/review-gi.md`) com o binário do código de
então, `--sink.type=sqlite`, portas UDP/TCP 14201, HTTP 14202, TLS 14203,
HTTPS 14204, certificado RSA autoassinado. Cada `_seq` conferido exatamente uma
vez em `gelf_events` (`payload.fields.seq`/`run`):

| Execução | Transporte | Mensagens | No sqlite (distintos) |
|---|---|---|---|
| udpgz | udp gzip | 2000 | 2000 |
| udpbig | udp gzip, 20 KB (2 chunks) | 300 | 300 |
| udpzl / udpnone | udp zlib / none, chunk 1400 | 300 + 300 | 300 + 300 |
| udp5k | udp 12 KB, `--rate 5000`, 2 conexões | 20 000 | 20 000 |
| tcp / tcpbig | tcp 4 conexões / 60 KB | 5000 / 500 | 5000 / 500 |
| tcpdur | tcp `--duration 2s --rate 500` sem `--count` | 1000 | 1000 |
| tls | tls `--tls-ca` | 5000 | 5000 |
| http / httpgz / httpzl | pipeline 8 / gzip `--requests-per-conn 7` / zlib pipeline 4 | 5000 / 2000 / 2000 | iguais |
| https / rpc | gzip pipeline 4 / pipeline 16, per-conn 13, 6 conexões | 3000 / 20 000 | iguais |
| PROXY v1/v2 | tcp, tls, https, http (`proxy.protocol=optional`) | 4 × 1000 | 4 × 1000, `src_ip` = a origem anunciada (inclusive `2001:db8::1` com destino IPv4) |
| sigint | http `--duration 20s`, SIGINT em 2 s | 4004 (saída 130) | 4004 |

Mais: as três capturas reais (`testdata/captures/`) reproduzidas por UDP, TCP e
HTTP (403/403, 620/620, 330/330, nenhuma malformada); alvo fechado → saída 3
com 5 `not_sent`.

Na época o servidor fechava **toda** conexão HTTP (sem keep-alive): com
`--count 5000 --concurrency 4 --compress gzip`, `--pipeline 1` levou 0,49 s e
`--pipeline 32` 0,77 s, com 157 920 reenvios e 18 `conn_broken: broken pipe` —
nada perdido, mas ~30× o tráfego. Corrigido no cliente (a vaga do pipeline só
é liberada depois de marcar a conexão fechando; escrita que falha depois de um
`Connection: close` não conta quebra) e documentado (SPEC §4.3). Roteiro
(não refeito nesta rodada: os servidores estão sendo alterados para ter
keep-alive):

```sh
go-gelf-server --sink.type=sqlite ... &      # portas 12201 udp/tcp, 12202 http
./go-gelf-injector --transport udp  --count 100000 --payload-size 20000 --rate 10000 --run e2eudp
./go-gelf-injector --transport tcp  --count 100000 --concurrency 4 --run e2etcp
# com keep-alive no servidor:
./go-gelf-injector --transport http --count 100000 --concurrency 4 --pipeline 32 --retries 5 --run e2ehttp
# servidor que fecha toda conexão: só --concurrency (pipeline > 1 desperdiça)
./go-gelf-injector --transport http --count 100000 --concurrency 32 --retries 5 --run e2ehttp1
# no sqlite: contar _seq distintos por _run (payload.fields.seq / fields.run)
```

### 3.5 Correções da revisão (2026-10-31)

Cada achado de `tasks/gelf/review-gi.md` tem um teste que falha sem a correção
(conferido por mutação): `TestLoop*` (#1), `TestMidstream` com logs de acesso
(#2), `TestHTTPServerClosingConnections` e `TestIdleCloseIsPlanned` (#3),
`TestUsage` com `--run`/`--progress` (#4, #10), `TestPadSizes` (#5),
`TestStreamPartialWrite` (#6), `TestReassemblyDeadlineExact` (#7),
`TestReassemblyIDReuse` (#8), `TestHTTPPathStrict` (#9).

Fuzz depois das correções, 60 s cada, sem achado: `FuzzStream` 5,1 M execuções,
`FuzzReassemble` 2,4 M, `FuzzDatagrams` 1,3 M.

### 3.6 Conferência no destino contra o `go-gelf-server` (2026-11-07)

O `go-gelf-server` compilado deste worktree, `--ring.mode=memory`, um processo
por destino (portas UDP/TCP e HTTP próprias em 388xx), contra os containers de
§1.1 e um `events.db`: `sink=sqlite`, `sink=nats` (JetStream `LOGHUB_GELF`),
`sink=postgres` (`loghub.inbox`), `sink=redis` contra Redis 7.4 e contra Valkey
8.1. Por destino, quatro execuções com `--verify-*`:

| Execução | Forma | Por destino |
|---|---|---|
| udp | 3000, `--payload-size 12000` (2 chunks gzip), `--rate 3000` | 3000 de 3000, 0 perdidas, 0 duplicadas, saída 0 |
| tcp | 20 000 em 4 conexões | 20 000 de 20 000, saída 0 |
| http | 10 000, 4 conexões, `--pipeline 16`, gzip, `--retries 5` | 10 000 de 10 000, saída 0 |
| httpch | 3000, `--http-chunked 100`, pipeline 4 | 3000 de 3000, saída 0 |

Os cortes `go-gelf-pg-server` (`postgresql://`) e `go-gelf-nats-server`
(`nats://`), compilados deste worktree, com as mesmas quatro execuções: iguais
(gravam o mesmo `loghub.inbox` e o mesmo stream: DDL e `stream.json`
conferidos por diff). Sete destinos ao todo.

Duas passadas completas: a primeira com a conferência em 0,7–2,5 s; a segunda
(depois do alinhamento com o SI, binário final) com a máquina a carga média 32
(outros agentes rodando, conferência ainda com a janela de tempo nos streams): 26 de 28 iguais, e as duas UDP do `sink=sqlite` e
do GN com perda **real** no buffer do kernel do servidor — 231 e 40 de 3000
`lost`, `complete: false` em 60 s, saída 1 —; repetidas a `--rate 1000`,
3000 de 3000 nas duas. A conferência NATS levou 6–13 s nessa passada: a
janela de 10 min tinha ~250 000 mensagens das execuções anteriores, todas
lidas para achar as da `--run` (e o `TestNATSDest` passou do prazo). Daí a
regra de §10.3 da SPEC: os streams partem da última entrada na checagem
(`checkStartsAfterLast` nos testes Redis/Valkey e NATS). Terceira passada,
com ela e o binário final (carga média ~25): **28 de 28** — os sete destinos,
quatro execuções cada, todas completas, 0 perdidas, 0 duplicadas, saída 0;
conferência em 1,0–1,7 s nos streams (NATS 1,3–1,7 s, antes 6–13 s) e
1,5–3,7 s no PostgreSQL e no SQLite (que releem a janela).

E o que ela acusa:

- **perda real**: UDP sem `--rate`, 30 000 documentos de 30 KB em 4 sockets
  contra o `sink=nats`: o injetor diz `sent` 30 000 (o kernel aceitou); o
  JetStream tem 7612 — `lost` 22 388, `lost_seqs` `534,569-570,…`,
  `complete: false`, **saída 1**. É a cegueira do UDP (AGENTS §6) medida;
- **duplicação**: a mesma `--run` duas vezes contra o `sink=sqlite`:
  `duplicated` 50, saída 1; 100 HTTP com a `--run` de uma execução TCP de
  5000: `duplicated` 100, `unexpected` 4900;
- **destino impossível** (saída 4, **nada enviado**): NATS fechado, senha
  errada do PostgreSQL (a mensagem do `pgx` não tem a senha), chave Redis de
  outro tipo (`--verify-key`), stream NATS inexistente; e as recusas de uso
  (saída 2): `--file` com conferência, esquema `amqp://`, modelo sem
  marcadores;
- **modelo** com `"_seq":"{seq}"` (string) contra Valkey: 200 de 200.

### 3.7 Capturas reais novas (2026-11-07)

- `ip-fragments.pcap`: `tcpdump -i eth0` no container receptor de uma ponte
  Docker (MTU 1500), o injetor em outro container: 20 documentos de 20 KB
  (chunks gzip de 8192, 6 fragmentos cada) e 10 de 5000 bytes sem compressão
  e sem chunk (4 fragmentos): 240 fragmentos, 50 datagramas remontados, 30 de
  30 idênticos ao receptor. A mesma forma com 800 documentos (6500
  fragmentos, 1600 datagramas) deu 800 de 800 em TCP, UDP e HTTP
  (`GELF_TEST_PCAP_EXPECT`). Fecha a pendência 6.
- `any-bridge.pcap`: `tcpdump -i any` no host da VM do Docker (`--net=host`),
  entre os dois containers: cada pacote duas vezes (as duas `veth`). 50 do
  gerador sem compressão, a mesma linha 5 vezes (`--file`) e 10 de 20 KB:
  310 pacotes, 55 `udp_datagrams_duplicate`, 20 `udp_chunks_duplicate`, 65 de
  65 — as 5 repetições de propósito inclusive.

```sh
docker network create vkh-gi-frag
docker run -d --name vkh-gi-recv --network vkh-gi-frag -v $PWD/w:/w alpine:3 sleep 3600
docker exec vkh-gi-recv apk add --no-cache tcpdump
docker exec -d -e GELF_SINK=udp=0.0.0.0:12201 -e GELF_SINK_STOP=/w/stop -e GELF_SINK_OUT=/w/sink.jsonl \
  vkh-gi-recv /w/sink.test -test.run '^TestSink$'
docker exec -d vkh-gi-recv tcpdump -i eth0 -U -s 0 -w /w/frag.pcap 'ip proto 17'   # ou, num container --net=host: -i any
docker run --rm --network vkh-gi-frag -v $PWD/w:/w alpine:3 /w/go-gelf-injector --transport udp \
  --target vkh-gi-recv:12201 --count 20 --payload-size 20000 --rate 100 --run fragc
# pare o tcpdump, toque /w/stop; no Mac:
GELF_TEST_PCAP=w/frag.pcap GELF_TEST_PCAP_RUNS=fragc:20 GELF_TEST_PCAP_EXPECT=w/sink.jsonl \
  go test ./internal/app -run TestReplayCapture -v
```

Nota: no loopback de um container (Docker Desktop, kernel 6.x) o `-i any`
gravou **uma** cópia de cada datagrama; a duplicação aparece entre
interfaces (ponte, `veth`).

### 3.8 Mutação (2026-11-07)

Cada regra nova foi quebrada no código e o teste que a trava conferido
vermelho (29 mutações, 27 mortas): dedup desligada (`TestDatagramCopies`, e a
captura real `any-bridge` passa a ter cada documento duas vezes), dedup sem
olhar o ponto, sem a identificação IP, `on` igual a `auto`, `off` ignorado,
ponto não decodificado (`TestVantage`); PKTAP lendo `pth_type_next`
(`TestPKTAPInnerLinkType`), fragmentos do fim da passada sem contar
(`TestFragmentsWaitingAtEndAreLost`); o `midWord` do `httpx` (`FuzzStream`,
pela entrada guardada); `closed` ignorado no último quadro
(`TestTailClosedVersusCut`), `RST` adiante de buraco tido como fechado
(`TestTCPEndClosed`); `chunked` desligado (`TestHTTPChunked`); adaptação
desligada e vagas que ignoram a adaptação (`TestHTTPAdaptsToClosingServer`);
PROXY do fluxo com a origem real (`TestPcapFlows`), fim de fluxo ignorado
(`TestPcapFlowsEnd`), todos os fluxos numa chave (`TestPcapFlows`), teto
diferente de 256 por worker (`TestPcapFlowsEviction`); duplicata não contada
(`TestClassify`), perda que não é erro e conferência sem checagem prévia
(`TestVerifyExitCodes`), filtro de `run` (`TestAddJLH`), filtro de espaço
(`TestSQLiteDest`), leitura de assentamento (`TestRunWaits`), streams que não partem da última entrada na checagem (`TestNATSDest`, `TestRedisDest`).

Duas sobrevivem, as duas defesas que o ambiente de teste não aciona:

- tirar o `CAST(env AS TEXT)` do `json_extract`: o SQLite do
  `modernc.org/sqlite` 1.58 aceita JSON texto dentro de um BLOB; o `CAST`
  fica, porque um SQLite que leia o BLOB como JSONB recusaria toda linha
  (go-gelf-server SPEC §3.7);
- tirar o "sequência do stream já contada não conta de novo" do NATS: o
  `nats.go` 1.53.1 com `Fetch` não reentregou nada nos testes (0 duplicatas
  falsas em ~250 000 mensagens); a defesa é contra o reinício do consumidor
  ordenado que o SI viu com `FetchNoWait`.

## 4. O que ainda não está provado

- Um Graylog; o `go-gelf-server` depois da mudança para keep-alive (§3.4: o
  E2E da revisão é anterior a ela) — a não ser pelo E2E da conferência
  (§3.6), que usa o G atual com keep-alive.
- IPv6 fragmentado numa captura real (só sintética); PKTAP, pcapng com várias
  interfaces (o ponto de captura por interface pcapng é provado só com
  pacotes sintéticos).
- Carga longa (horas) e replay de capturas com muitas direções abertas;
  `--pcap-flows` com milhares de fluxos.
- A conferência contra Valkey 7.2/9.0, com
  `tls://`/`rediss://` e `--verify-tls-ca`, e contra um Redis Cluster ou
  Sentinel (o `rueidis.ParseURL` aceita `addr=` e `master_set=`, não
  conferido).
