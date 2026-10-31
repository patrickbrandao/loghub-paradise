# go-gelf-injector — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Números
de 2026-10-31, num Apple M2 com macOS (e containers Linux arm64 do Docker
Desktop para o tcpdump). Comandos a partir da pasta do módulo.

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. Leva cerca de 10 s; não precisa de nada externo.

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

## 4. O que ainda não está provado

- Um Graylog; o `go-gelf-server` depois da mudança para keep-alive (§3.4: o
  E2E da revisão é anterior a ela).
- Fragmentos IP e chunks numa captura de rede real (o loopback não fragmenta);
  PKTAP, pcapng com várias interfaces.
- Carga longa (horas) e replay de capturas com muitas direções abertas.
