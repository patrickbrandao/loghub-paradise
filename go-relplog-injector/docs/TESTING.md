# go-relplog-injector — Procedimentos de teste

Como testar, o que cada suíte prova e o que ainda **não** está provado. Números
de 2026-10-30, num Apple M2 com macOS. Comandos a partir da pasta do módulo.

Os pacotes `tmpl`, `report`, `pcap`, `extract` e `source` são cópias do
`go-syslog-injector` (o pai), com as mesmas suítes e o mesmo fuzz (TESTING do
SI §1–§2). Uma correção lá é "a portar" para cá.

## 1. Suíte rápida (toda mudança)

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` não é opcional. Leva cerca de 7 s; não precisa de nada externo.

`internal/app` roda contra um **servidor RELP falso** (`relpserver_test.go`) cujas
respostas o teste escolhe (confirmar, recusar algumas, fechar sem responder na
N-ésima, mandar `serverclose`, segurar tudo):

| Teste | O que prova |
|---|---|
| `TestAckedConcurrent` | 5000 mensagens em 4 sessões, janela 16: todas `acked`, `seq` 1..5000 exatas no servidor; as ofertas do `open` |
| `TestRejectContinueAndBreak` | um `500`: com `continue`, 1 `rejected` e 49 `acked` sem quebra; com `break`, a sessão quebra, reabre, as vizinhas são reenviadas e a recusada **nunca** volta |
| `TestDropAndServercloseRetransmit` | sessão que cai sem resposta e sessão com `serverclose`: retransmissão, toda `seq` confirmada ao menos uma vez, duplicatas no servidor (at-least-once) |
| `TestRetriesZero` | sem retransmissão: `unanswered`, saída 1 |
| `TestAckTimeoutAndWindow` | janela 2, servidor que nunca responde: 2 transações por sessão, 2 sessões quebradas por `--ack-timeout`, mensagem que esperava vaga não gasta tentativa |
| `TestTLSAndProxyV2` | RELP sobre TLS com CA própria e PROXY v2 byte a byte; sem a CA, saída 3 |
| `TestUnreachable`, `TestOpenRefusedIsDialError` | alvo fechado e `open` recusado: saída 3 |
| `TestDurationWithoutCount` | `--duration` sem `--count` envia durante o tempo todo, a `--rate` (falhava antes de 2026-10-31: uma mensagem só) |
| `TestUsageErrors` | 11 combinações recusadas |
| `TestPcapReplay` | a captura sintética do SI (UDP, octet, LF midstream, RELP e a direção `rsp`, DNS) reenviada por RELP, em ordem; pcapng, `--pcap-port`, `original` a 10x, `--loop`, `--format 5424` |
| `source` | `--loop` numa passada sem mensagem termina (`TestLoopBlankFileEnds`: arquivo só de linhas em branco; `TestLoopFileEmptiedBetweenPassesEnds`: arquivo esvaziado entre passadas). Antes de 2026-10-31 girava a 100 % de CPU para sempre (achado da revisão do GI) |

## 2. Fuzz

Os mesmos do SI (`FuzzReader`, `FuzzStream`), com o mesmo corpus.

## 3. Prova contra o `go-relplog-server` real (2026-10-30)

Servidor com `--sink.type=none --ring.mode=memory`, admin em porta alta:

- **Carga**: 100 000 mensagens em 4 sessões, ~510 mil msg/s confirmadas uma a
  uma (p50 0,7 ms, p99 2,8 ms com janela 128); o servidor contou 100 000
  aceitas e 100 004 respostas `200` (as 100 000 e os 4 `open`), 0 sem resposta.
- **Reinício no meio**: 12 000 mensagens a 2000/s em 2 sessões; o servidor
  recebe SIGTERM aos 2 s e volta 1,5 s depois. O injetor recebeu o `serverclose`
  do shutdown nas 2 sessões, teve 10 discagens recusadas, retransmitiu 3
  transações e terminou com **12 000 `acked`**, saída 0.
- **TLS + PROXY v2**: 20 000 mensagens em 2 sessões com CA própria e origem
  anunciada `198.51.100.9:5555`; o servidor contou 20 000 aceitas no TLS e 2
  cabeçalhos v2.

## 4. O que ainda não está provado

- Um `500` vindo do servidor real (ele só o envia em recusa final rara:
  `logic` do destino com `ring=none` e origem não endereçável). Provado só
  contra o servidor falso.
- O backpressure real (ring cheio, sink DOWN) por mais que alguns segundos, e o
  `relp.backpressure_timeout` do servidor vencendo antes do `--ack-timeout`.
- Comparação lado a lado com o `rsyslog`/`omrelp` sob as mesmas falhas
  (duplicatas por quebra): o comportamento foi escrito pela leitura da SPEC do
  servidor, que cita o `librelp`.
- As lacunas de captura do SI (TESTING do SI §4) valem aqui.
