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
| `verify` | marcadores `seq=`/`run=` como palavras; esquemas aceitos (`valkey://` → `redis://`, `valkeys://` → `rediss://`), recusas sem ecoar a URL, destino sem credenciais; só envelopes `syslog` do `space` e da execução contam; perdidas, duplicadas, inesperadas e faixas (`TestMarkers`, `TestParse`, `TestCountJLH`) |

Testes da rodada de 2026-11-07 (SPEC §9), todos com checagem de mutação
(remover a correção derruba o teste):

| Teste | Regra | Mutação que ele pega |
|---|---|---|
| `extract` `TestResyncLargeCandidateHolds` | §6.6: candidato > 64 KiB segura a decisão | voltar o teto de 64 KiB |
| `pcap` `TestFragmentCutBySnaplen` | §6.3: fragmento cortado descarta o datagrama na hora, conta uma vez | não descartar os fragmentos que chegam depois |
| `app` `TestPcapDedupAnyCopies` | §6.9: captura SLL2 com cada pacote duas vezes (a cópia com DNAT) sai uma vez; repetição real (outro IP ID) fica; `off` manda as cópias | desligar a deduplicação |
| `app` `TestPcapFlowsProxy` | §6.8: uma conexão por fluxo, PROXY v1 com a origem capturada, reuso da quádrupla = conexão nova, a conexão fecha no FIN (não no fim) | ignorar `FlowDial`; não trocar a origem; não emitir o fim do fluxo |
| `app` `TestVerifySQLite` | §8.5: `events.db` com uma perdida, uma duplicada, outra execução, outro `space` e uma `seq` nunca enviada → código 1 e as contas; tudo uma vez → 0 sem esperar o prazo | não contar duplicadas; não dar código 1 por perda |
| `app` `TestVerifyUnreachable` | §8.5: destino inalcançável (Redis, Valkey, PostgreSQL, NATS, SQLite inexistente) → 4, nada enviado, senha nunca impressa | seguir sem o destino |
| `app` `TestPendingUsageErrors` | §3.2: as 12 recusas novas, sem ecoar a URL | — |

Porte entre injetores (2026-11-07; SPEC §9 item 6), também com mutação:

| Teste | Regra | Mutação que ele pega |
|---|---|---|
| `pcap` `TestPKTAPInnerLinkType` (do RI) | §6.2: o enlace de dentro do PKTAP é `pth_dlt`; `pth_type_next` ≠ 1 não é pacote; cada `pth_ifname` é um ponto | ler `pth_type_next`; aceitar `next` ≠ 1 |
| `pcap` `TestFragmentsWaitingAtEndAreLost` (do RI), `source` `TestPcapFragmentsWaitingAtEndAreLost` | §6.3: o fim da passada conta os fragmentos à espera | `flush` sem contar; não chamar `Decoder.Flush` no fim da passada |
| `pcap` `TestVantage`, `TestDedupRule` | §6.9: ponto de captura (pcapng, SLL2 índice+tipo, SLL); cópia só de outro ponto, uma por ponto, na janela; endereços fora da chave | tirar o tipo do SLL2; ignorar a interface pcapng; aceitar cópia do mesmo ponto; absorver mais de uma por ponto |
| `app` `TestPcapDedupAnyCopies` (revisto) | §6.9: a repetição do emissor com o **mesmo** IP ID sai duas vezes | a regra antiga (só a chave) |
| `app` `TestPcapDedupAfterPortFilter` | §6.9: com DNAT e `--pcap-port`, a cópia recusada pelo filtro não esconde a aceita (UDP e TCP) | perguntar a cópia antes do filtro |
| `pcap` `TestTCPEndClosed` (do GI) | §6.4: FIN = fechada; fim de captura = cortada; RST logo depois do último byte = fechada; RST adiante de um buraco = cortada | trocar cada um dos três `closed` |
| `extract` `TestTailClosedVersusCut`, `source` `TestPcapClosedTail` | §6.6: LF entrega a cauda nas duas; octet fechada entrega o parcial (`tcp_closed_partial_frames`), cortada perde; RELP sem LF perde nas duas; resto de ressincronização perde; com e sem `--pcap-flows` | entregar o parcial também na cortada; nunca entregar; entregar sem byte de mensagem. Sobrevive (equivalente) tirar o `!resync` da regra octet: no fim, uma ressincronização sem candidato já esvaziou o buffer |
| `extract` `TestResyncLinearCost`, `TestResyncLargeRELPCandidate`, `TestResyncLargeOctetCandidateHolds` (do RI) | §6.6: sem candidato o buffer fica em 64 bytes e a busca não recomeça; um quadro de 6 MiB em 4300 segmentos pede no máximo 10 buscas | não aparar sem candidato; buscar a cada segmento; esquecer `from`/`need` |
| `verify` `TestNATSStartsAfterLast`, `TestRedisStartsAfterLast` (opt-in: `SI_TEST_VERIFY_NATS`, `SI_TEST_VERIFY_REDIS` com Redis e Valkey) | §8.5: o que já estava no stream na abertura não é lido; o stream que nasce depois é lido inteiro; 1500 mensagens em mais de um `Fetch` | partir do início (sequência 1, `-`) |

Revisão independente dos injetores (2026-11-07; SPEC §9 item 7). Cada teste
foi conferido por mutação (a correção tirada, o teste falha); os opt-in rodaram
contra `nats:latest -js`, `postgres:18-alpine`, `redis:7.4-alpine` e
`valkey/valkey:8.1-alpine` em Docker (`SI_TEST_VERIFY_PG` é novo):

| Teste | Regra | Mutação que ele pega |
|---|---|---|
| `verify` `TestNATSFacilitySeverity` (opt-in) | §8.5: stream com os dois subjects, publicação abaixo de `.syslog.>`; o filtro é sempre os dois | filtrar no subject que achou o stream (o defeito: 1 de 10) |
| `verify` `TestNATSForeignTraffic` (opt-in) | §8.5: outra carga no mesmo subject sem parar; a leitura termina (`NumPending`, fatia de 2 s) e a espera completa | voltar só com o lote vazio (prazo vencido, `error`, 25 s) |
| `verify` `TestRedisStartsAfterLast` (revisto) | §8.5: stream que nunca aparece = tudo perdido, sem `error` | — (a regra antiga foi apagada) |
| `verify` `TestPostgresRunPresent` (opt-in), `app` `TestVerifyRunPresent` | §8.5: linhas da execução já na janela → código 2, nada enviado; outro `--run` (mesmo um prefixo) passa | não recusar |
| `verify` `TestParseLists`, `TestClean`, `app` `TestPendingUsageErrors`, `TestVerifyUnreachable` | §8.5: cada elemento da lista NATS analisado antes, mensagem genérica; `*url.Error` nunca repassado; usuário/senha/token apagados; destino com todos os hosts | analisar só o 1º elemento; repassar o `*url.Error` (senha codificada de outro jeito escapa do apagamento) |
| `verify` `TestWaitExtraRead` | §8.5: a leitura a mais depois de completo vê a cópia da rajada | tirar a leitura a mais |
| `verify` `TestWaitRetriesErrors` | §8.5: erro repetido até o prazo; leitura inteira cortada não troca a conta; última leitura com erro → `error` | parar no 1º erro; trocar a conta por uma leitura cortada |
| `verify` `TestTallyUnexpectedRecords` | §8.5: `unexpected` em registros; `duplicated` = cópias a mais | contar `seq` distintas |
| `app` `TestPcapFlows` (estendido) | §6.8: `--proxy-src`/`--proxy-dst` com `--pcap-flows` → 2 | aceitar |
| `pcap` `TestFragmentLateCopy` | §6.3: cópias intercaladas sem NAT não abrem órfão; passada a janela de 50 ms, é datagrama novo | não lembrar o datagrama completado |
| `pcap` `TestSnaplenUDPDatagramIsLostFrame` | §6.3: UDP condenado pelo snaplen contado para `frames_lost` | não contar |
| `pcap` `TestTCPPendingBoundCountsEverySegment` | §6.4: 100 000 segmentos sem bytes atrás de um buraco batem no teto | custo 0 por segmento |
| `pcap` `TestTCPDrainManySegments` | §6.4: 30 000 segmentos em ordem inversa drenam em < 5 s (0,4 s) | reordenar a cada retirada (59 s) |

Não coberto por teste: a soma de `DatagramsSnaplenUDP` em `frames_lost` no
`source` (uma linha; o contador do decodificador é testado) e a folga de 10 min
(constante).

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

### 3.3 Conferência no destino contra servidores reais (2026-11-07)

`go-syslog-server` compilado deste worktree, um processo por destino
(`--ring.mode=memory`, TCP e UDP em `127.0.0.1:38546`), e os cortes
`go-syslog-pg-server` e `go-syslog-nats-server`; destinos em Docker:
`nats:2.11-alpine -js` (stream `LOGHUB_SYSLOG` de `docker/nats/stream.json`),
`postgres:18-alpine` (com `docker/postgres/inbox.sql`), `redis:7.4-alpine` e
`valkey/valkey:8.1-alpine`, e o `events.db` do `sink=sqlite`. Quatro cenários
por destino:

1. TCP, 5000 mensagens em 4 conexões: `found 5000, lost 0, duplicated 0,
   complete`, saída 0 (1,3 a 1,9 s de espera, o 1 s final incluso);
2. a mesma execução de novo (mesmo `--run`): `found 5000, duplicated 5000`,
   saída 1;
3. UDP para uma porta sem ninguém: `expected 45` (5 `refused`), `lost 45`,
   `lost_seqs 1,3-11,...`, saída 1;
4. UDP, 20 000 na taxa máxima para o servidor: `found 20000, lost 0`, saída 0.

Os sete conjuntos (S com nats, postgres, redis, valkey, sqlite; SP; SN) deram
exatamente isso. O roteiro é a receita abaixo.

Duas falhas que só o destino real mostrou: o `FetchNoWait` do consumidor
ordenado do `nats.go` recomeçava do início a cada chamada (cada mensagem
contada dezenas de vezes, `duplicated 536000`), e sem a leitura final de 1 s
a segunda execução completava com as linhas da primeira e via 0 duplicadas
no PostgreSQL (SPEC §8.5).

Receita (portas da faixa da rodada; remova os contêineres pelo nome no fim):

```sh
docker run -d --name vke-si-nats -p 38540:4222 nats:2.11-alpine -js
docker run --rm --network host -v "$PWD/../go-syslog-server/docker/nats/stream.json:/s.json:ro" \
  natsio/nats-box:0.20.0 nats --server nats://host.docker.internal:38540 stream add LOGHUB_SYSLOG --config /s.json
docker run -d --name vke-si-pg -p 38541:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_USER=syslog -e POSTGRES_DB=syslog postgres:18-alpine
docker exec -i vke-si-pg psql -U syslog -d syslog -q < ../go-syslog-server/docker/postgres/inbox.sql
docker run -d --name vke-si-redis  -p 38542:6379 redis:7.4-alpine
docker run -d --name vke-si-valkey -p 38543:6379 valkey/valkey:8.1-alpine
# um servidor por destino, p.ex.:
LOGHUB_EB_URL=redis://127.0.0.1:38543/0 DATADIR=/tmp/d AGENT_NAME=go-syslog-server HOSTNAME=dev-mac \
  ./go-syslog-server --listen.tcp=127.0.0.1:38546 --listen.udp=127.0.0.1:38546 --admin.bind=127.0.0.1:38545 &
./go-syslog-injector --target 127.0.0.1:38546 --count 5000 --concurrency 4 --verify-url valkey://127.0.0.1:38543/0
docker rm -f vke-si-nats vke-si-pg vke-si-redis vke-si-valkey
```

## 4. O que ainda não está provado

- TLS e PROXY contra o `go-syslog-server` real (só contra o servidor falso dos
  testes, que confere o cabeçalho byte a byte).
- PKTAP (o `-i any` do macOS), pcap big-endian e pcapng com várias interfaces e
  seções: o código existe, mas só capturas sintéticas de Ethernet/SLL/SLL2/NULL
  passaram pelos testes, e as reais foram SLL2 e Ethernet.
- Fragmentos IP numa captura real (no loopback não há fragmentação: MTU 64 KiB).
- `--pcap-dedup` e `--pcap-flows` numa captura real: provados com capturas
  sintéticas (SLL2 com cópias de DNAT; fluxos com PROXY v1 num servidor falso),
  não com um `tcpdump -i any` de containers nem com o PROXY do
  `go-syslog-server` real.
- Conferência com Redis Sentinel/Cluster, NATS com TLS/credenciais e
  PostgreSQL com TLS: os clientes aceitam pela URL, mas só o caminho sem TLS e
  sem senha (NATS, Redis) ou com senha (PostgreSQL) foi exercitado; o
  `--verify-tls-ca` (2026-11-07) só nas recusas de uso, sem um destino TLS
  real.
- Um candidato de ressincronização de vários MiB: o custo (memória e tempo)
  foi medido só até 100 KiB (`TestResyncLargeCandidateHolds`).
- Carga longa (horas): o injetor guarda tudo em memória por mensagem só até o
  envio, mas o replay de uma captura enorme com muitas direções abertas não foi
  medido.
