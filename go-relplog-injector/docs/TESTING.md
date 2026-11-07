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

Rodada de 2026-11-07 (pendências 1–6, SPEC §9). Cada teste abaixo trava uma
regra: a mutação indicada (a correção removida) o derruba — conferido.

| Teste | O que prova | Mutação que o derruba |
|---|---|---|
| `TestVerifySQLite` (app) | conferência contra a tabela `syslog_events` do `sink=sqlite`, escrita pelo servidor falso com o JLH de cada `200`: tudo lá (0); uma confirmada ausente (1, `lost_seqs ["77"]`); uma gravada duas vezes (1; 0 com `--verify-allow-duplicates`); modelo com `{seq}`/`{run}`; modelo sem `{run}` (2); destino ilegível (4, **nada enviado**) | `OK` sem `Lost`; sem a sonda antes do envio |
| `TestVerifyUsage` (app) | as combinações recusadas de `--verify-*`, `--pcap-flows`, `--pcap-dedup`; nenhum erro ecoa a senha da URL | — |
| `verify` (`TestParseURL`, `TestMatcher`, `TestTally`) | esquemas, `valkey://` → `redis://`, parâmetros do injetor retirados, destino sem segredos; padrão do modelo (gerador, `{uuid}`, `{ts}`, `{rand:N}` acima do limite do RE2, `raw` em base64); a conta (perdida, ausente não confirmada, encontrada não confirmada, duplicata com e sem o mesmo `uuid`, fora da faixa) | — |
| `TestRejectTextInEvents`, `TestRejectTextCardinality` (app) | `rejected: <texto>` nos eventos; 32 textos no máximo, 120 bytes, controle → `?` | `OnReject` não chamado |
| `TestPcapDedupAnyCopies` (app) | pcapng cooked v2 com cada pacote na veth e, com NAT, na eth0 (UDP e uma conexão TCP): cada mensagem uma vez; a mensagem que o emissor mandou duas vezes sai duas; `--pcap-dedup off` reproduz as cópias | dedup desligado na fonte |
| `TestDedupRule`, `TestDedupKey` (pcap) | um original absorve uma cópia por interface; mesma interface nunca é cópia; fora da janela não é cópia; a chave ignora endereços, porta de destino e interface, e muda com IP ID, porta de origem, conteúdo, protocolo e sequência | sem a exigência de outra interface |
| `TestPcapPerFlow` (app) | 3 conexões TCP e um fluxo UDP, cada um na sua sessão, em ordem, com o PROXY v1 da origem capturada; também com `--concurrency 1` (vaga cedida pela sessão ociosa, sem impasse) | `FlowDialer` sem os endereços do fluxo |
| `TestPKTAPInnerLinkType` (pcap) | o enlace interno é `pth_dlt`: loopback (`NULL`) do `-i any` do macOS decodifica; `pth_ifname` distingue as interfaces; registro que não é pacote é pulado | `pth_type_next` como enlace (o código antigo) |
| `TestSnaplenFragmentCondemnsDatagram`, `TestFragmentsWaitingAtEndAreLost` (pcap) | fragmento cortado condena o datagrama na hora (3 fragmentos perdidos, 1 datagrama em `pcap_ip_datagrams_snaplen`), o seguinte remonta; o que esperava no fim da passada conta | sem `poison` |
| `TestResyncLargeCandidateHolds`, `TestResyncLargeRELPCandidate` (extract) | candidato octet e RELP acima de 64 KiB segura a decisão: o mesmo resultado inteiro e em segmentos de 3, 7, 1460 e 65 536 bytes | o teto de 64 KiB de volta (octet e RELP, cada um) |
| `TestResyncLinearCost` (extract) | um quadro de 6 MiB em segmentos de 1460 bytes com no máximo 10 buscas (eram ~4300, 12 s); um fluxo midstream sem candidato guarda ≤ 64 bytes | sem o limite `need`; sem o corte do buffer sem candidato |
| `TestTCPEndClosed` (pcap, do GI, 2026-11-07) | §6.4: FIN = fechada; fim de captura = cortada; RST logo depois do último byte = fechada; RST adiante de um buraco = cortada | trocar cada um dos três `closed` |
| `TestTailClosedVersusCut` (extract), `TestPcapClosedTail` (source) | §6.6: LF entrega a cauda nas duas; octet fechada entrega o parcial (`tcp_closed_partial_frames`), cortada perde; RELP sem o LF final perde nas duas; resto de ressincronização perde | entregar o parcial também na cortada; nunca entregar; o `endHandler` não repassar o `closed`. Sobrevive (equivalente) tirar o `!resync` da regra octet: no fim, uma ressincronização sem candidato já esvaziou o buffer |
| `TestVantage` (pcap) | §6.8: SLL2 com o tipo do pacote (o loopback, enviado e recebido na mesma interface), SLL, pcapng | tirar o tipo do SLL2 |
| `TestNATSStartsAfterLast`, `TestRedisStartsAfterLast` (verify; opt-in `RI_TEST_VERIFY_NATS`, `RI_TEST_VERIFY_REDIS` com Redis e Valkey) | §8.5: o que já estava no stream na sonda não é lido; o stream que nasce depois é lido inteiro; 1500 mensagens em mais de um `Fetch` | partir da sequência 1 ou do `-`; a sonda não anotar o ponto |

A regra de pular as sequências do stream NATS já contadas (consumidor
ordenado que se recria) não tem teste que a trave: exige um NATS real e a
recriação do consumidor no meio da leitura. Está provada só pela ausência de
duplicatas falsas nas provas de §5 (e pelo achado do SI que a motivou).

## 2. Fuzz

Os mesmos do SI (`FuzzReader`, `FuzzStream`), com o mesmo corpus. Em
2026-11-07, depois da mudança da ressincronização (SPEC §6.6), `FuzzStream`
rodou 90 s (3,5 milhões de execuções, 261 entradas novas) sem falha.

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

## 4. Conferência no destino e `-i any` contra servidores reais (2026-11-07)

`go-relplog-server` compilado do mesmo commit, `ring.mode=memory`, contêineres
`vkf-ri-*` (NATS 2.12 com o stream de `docker/nats/stream.json`, PostgreSQL 18
com `docker/postgres/inbox.sql`, Redis 7.4, Valkey 8.1), portas do host
38641–38670. 50 000 mensagens em 4 sessões, cada sink:

| Sink | `--verify-*` | Resultado | Espera |
|---|---|---|---|
| `nats` (JetStream) | `nats://127.0.0.1:38641` | 50 000 de 50 000, 0 perdidas, 0 duplicadas (52 100 lidas: o stream tinha execuções anteriores na janela de 10 min), saída 0 | 5,5 s |
| `postgres` | `postgres://loghub:…@127.0.0.1:38642/loghub?sslmode=disable` | 50 000 de 50 000, saída 0 | 2,9 s |
| `redis` (Redis 7.4) | `redis://127.0.0.1:38643/0` | 50 000 de 50 000, saída 0 | 1,8 s |
| `redis` (Valkey 8.1) | `valkey://127.0.0.1:38644` | 50 000 de 50 000, saída 0 | 1,8 s |
| `sqlite` | `--verify-sqlite <data>/events.db` | 50 000 de 50 000, saída 0 | 2,1 s |
| — | `redis://127.0.0.1:38699` (nada ouvindo) | sonda falha antes do envio: 0 mensagens, saída 4 | — |

**Perda real detectada**: `sink=redis`, `ring.mode=memory`, 2000 msg/s por 6 s;
aos 1 s o Redis é pausado (`docker pause`: o ring segura o que já confirmou),
aos 2,5 s o servidor leva SIGKILL e volta 1 s depois. O injetor: 12 000
`acked`, saída **1**, conferência `lost 3019`, `lost_seqs 2184-5202` — as
mensagens confirmadas que estavam no ring em memória do processo morto. É o
que um `200` com `ring=memory` promete, e é o que só a conferência mostra.

**`-i any` real**: `tcpdump -i any` num contêiner com `--network host` no host
Docker (cooked v2), dois contêineres numa ponte própria trocando 300
datagramas UDP e 3 conexões TCP de 100 linhas: 1804 pacotes, cada um capturado
duas vezes (as duas veth). Sem a regra (`--pcap-dedup off`): 900 mensagens
(os UDP em dobro; o TCP já caía como retransmissão). Com ela: **600**, 902
cópias em `pcap_duplicates`.

**Uma sessão por fluxo real**: a mesma captura com `--pcap-flows
--proxy-protocol v2` contra o servidor com `proxy.protocol=required` e
`sink=redis` (Valkey): 4 sessões (3 TCP e o fluxo UDP), e o servidor gravou
`src-ip 172.21.0.2` com as portas de origem capturadas: 300 envelopes de uma
porta (o UDP) e 100 de cada uma das 3 conexões.

## 5. O que ainda não está provado

- Um `500` vindo do servidor real (ele só o envia em recusa final rara:
  `logic` do destino com `ring=none` e origem não endereçável). Provado só
  contra o servidor falso.
- O backpressure real (ring cheio, sink DOWN) por mais que alguns segundos, e o
  `relp.backpressure_timeout` do servidor vencendo antes do `--ack-timeout`.
- Comparação lado a lado com o `rsyslog`/`omrelp` sob as mesmas falhas
  (duplicatas por quebra): o comportamento foi escrito pela leitura da SPEC do
  servidor, que cita o `librelp`.
- As lacunas de captura do SI (TESTING do SI §4) valem aqui.
- `-i any` **com NAT** numa captura real (só sintética: no Docker Desktop a
  porta publicada fica no 127.0.0.1 do Mac, e o tráfego entre contêineres não
  passa pelo DNAT) e o PKTAP corrigido numa captura real do macOS (o tcpdump
  precisa de root no host de teste): SPEC §9, pendência 7.
- Conferência com `tls://`/`rediss://` e o parâmetro `ca` (sem servidor TLS
  nesta rodada); `space` diferente de `default`.
