# go-syslog-injector — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Números
de 2026-10-30, num Apple M2 com macOS (e um container Linux arm64 para o
tcpdump). Comandos a partir da pasta do módulo.

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. Leva cerca de 5 s; não precisa de nada externo.

| Pacote | O que prova |
|---|---|
| `tmpl` | cada marcador, chaves literais, erros de compilação, UUID v4 |
| `report` | quantis do histograma dentro de 4%, cobertura dos baldes, mescla |
| `pcap` | pcap e pcapng em Ethernet, SLL, SLL2 e NULL, IPv4 e IPv6, carimbos de tempo; arquivo cortado; snaplen; fragmentos fora de ordem e repetidos; fragmento vencido; TCP em ordem com volta de 2³², reordenado, retransmitido e sobreposto; buraco virando lacuna; midstream com snaplen; reuso de porta |
| `extract` | as três molduras em quatro divisões de segmentos; invariância da divisão (200 mensagens, 50 divisões aleatórias, três molduras); midstream; lacuna; tamanho inválido; quadro incompleto no fim; seleção de datagramas |
| `app` | ponta a ponta contra servidores falsos: TCP octet com 4 conexões e `seq` 1..5000 exatas; LF e 3164 com PRI calculado; UDP e `too_large`; unix; TLS com CA própria e PROXY v2 byte a byte; PROXY v1; TLS sem CA → saída 3; alvo fechado → saída 3 dentro do `--drain-timeout`; servidor que fecha no meio (reconexão); `--rate` e `--duration`; `--duration` sem `--count` (enviava uma mensagem só antes de 2026-10-31, `TestDurationWithoutCount`); modelo com todos os marcadores; arquivo com `--loop`; 12 combinações recusadas; replay de captura com UDP, octet, LF midstream, RELP (e a direção `rsp`), DNS ignorado, pcapng com `--pcap-port`, `original` a 10x e `--loop`, `--format 5424` em volta da mensagem capturada; arquivo que não é captura |
| `source` | `--loop` numa passada sem mensagem termina (`TestLoopBlankFileEnds`: arquivo só de linhas em branco; `TestLoopFileEmptiedBetweenPassesEnds`: arquivo esvaziado entre passadas). Antes de 2026-10-31 girava a 100 % de CPU para sempre (achado da revisão do GI) |

## 2. Fuzz

Antes de release, 10 min cada; 60 s no dia a dia:

```bash
go test ./internal/pcap/    -run=XXX -fuzz=^FuzzReader$ -fuzztime=60s
go test ./internal/extract/ -run=XXX -fuzz=^FuzzStream$ -fuzztime=60s
```

`FuzzReader`: nenhuma entrada derruba o leitor, o decodificador ou o
remontador. `FuzzStream`: além de não derrubar, a mesma entrada entregue
inteira ou fatiada (1 a 32 bytes por vez), midstream ou não, dá **as mesmas
mensagens**. Achou três defeitos reais de ressincronização em 2026-10-30
(DECISOES §3); as três entradas ficam em `testdata/fuzz/FuzzStream/`.

## 3. Prova contra servidores reais (2026-10-30)

### 3.1 `go-syslog-server` local

Servidor com `--sink.type=none --ring.mode=memory`, UDP, TCP e unix em portas
altas; os contadores `syslog_messages_total` e `_accepted_total` do `/metrics`
bateram exatamente com o injetor: 100 000 TCP octet em 4 conexões (~1 M msg/s),
20 000 UDP a 20 000/s, 5 000 unix de 2 000 bytes, 1 000 TCP LF de modelo — 0
descartes.

### 3.2 tcpdump real, num container Linux

No macOS a captura exige root (`/dev/bpf*`); a prova roda em
`alpine:3.22` com `tcpdump` e `socat`, com os binários Linux do injetor e do
`go-syslog-server`:

1. o servidor sobe; dois tcpdump capturam a porta, `-i any` (SLL2) e `-i lo`
   (Ethernet), com `-B 65536 -U`;
2. 3000 linhas distintas (de 60 a 760 bytes) vão por TCP octet em 3 conexões,
   TCP LF e UDP; o servidor conta 6000 TCP e 3000 UDP;
3. cada captura é reproduzida, com `--framing lf`, num `socat` que grava o que
   recebe; cada linha original aparece **exatamente 3 vezes**, nenhuma linha
   estranha: 9000 de 9000 nas duas capturas, 0 lacunas, 0 bytes pulados.
4. **midstream**: dois injetores a 500/s (octet e LF) por 6 s, o tcpdump
   começando 2 s depois; o replay detecta as duas molduras depois de
   ressincronizar (334 bytes pulados, os quadros parciais do começo), entrega
   3995 mensagens — todas linhas originais, nenhuma duplicada.

Na primeira tentativa, com o tcpdump interrompido 1 s depois do tráfego, 783
mensagens não estavam na captura e nenhuma lacuna apareceu (DECISOES §3): daí o
`tcp_unterminated`. Pare o tcpdump com folga.

O roteiro (refeito a cada release, fora da suíte por exigir Docker):

```sh
# no container alpine:3.22 com tcpdump e socat, binários em /w
./go-syslog-server --server.data_dir=/tmp/d --sink.type=none --ring.mode=memory \
  --listen.udp=127.0.0.1:1514 --listen.tcp=127.0.0.1:1514 &
tcpdump -i any -B 65536 -U -s 0 -w any.pcap 'port 1514' &
./go-syslog-injector --file lines.txt --concurrency 3
./go-syslog-injector --file lines.txt --framing lf
./go-syslog-injector --transport udp --file lines.txt --rate 20000
sleep 3; kill -INT %2
socat -u TCP-LISTEN:7000,reuseaddr,fork OPEN:out.txt,creat,append &
./go-syslog-injector --target 127.0.0.1:7000 --framing lf --pcap any.pcap --report json
sort out.txt | uniq -c | awk '$1 != 3'     # vazio
```

## 4. O que ainda não está provado

- TLS e PROXY contra o `go-syslog-server` real (só contra o servidor falso dos
  testes, que confere o cabeçalho byte a byte).
- PKTAP (o `-i any` do macOS), pcap big-endian e pcapng com várias interfaces e
  seções: o código existe, mas só capturas sintéticas de Ethernet/SLL/SLL2/NULL
  passaram pelos testes, e as reais foram SLL2 e Ethernet.
- Fragmentos IP numa captura real (no loopback não há fragmentação: MTU 64 KiB).
- Carga longa (horas): o injetor guarda tudo em memória por mensagem só até o
  envio, mas o replay de uma captura enorme com muitas direções abertas não foi
  medido.
