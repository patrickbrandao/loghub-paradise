# go-relplog-injector — Especificação v1

Ferramenta cliente que **injeta mensagens syslog por RELP** num servidor — em
especial os do ecossistema LogHub (`go-relplog-server`, `go-relplog-pg-server`,
`go-relplog-nats-server`) — falando como o `omrelp` do rsyslog (`librelp`),
para testar o comportamento dele ou colocá-lo sob carga. Criada em 2026-10-30; as decisões que
a definiram estão em `docs/DECISOES.md`, citadas aqui como "decisão N".

Esta especificação é a fonte da verdade: discrepância entre código e spec é bug
no código. Para mudar comportamento, primeiro a spec (com o porquê), depois o
código, depois o teste.

---

## 1. Escopo

### 1.1 Objetivos

- Uma sessão RELP por conexão, em **TCP** ou **TLS**, com PROXY protocol
  opcional, e o diálogo do `librelp`: `open` com ofertas, `syslog` numa
  **janela** de transações não confirmadas, `close` no fim.
- **Uma confirmação por mensagem**: o relatório diz quantas o servidor
  confirmou (`200`), recusou (`500`) ou deixou sem resposta — o que o RELP, ao
  contrário do syslog/TCP, permite saber.
- Reagir a cada resposta como o `librelp` (SPEC do `go-relplog-server` §5.4):
  `500` destrói a mensagem e quebra a sessão; sessão quebrada (fechamento,
  `serverclose`, prazo sem resposta) reconecta e **retransmite** o que não teve
  `200`. É o que permite testar, com um cliente controlado, o backpressure, o
  shutdown e as duplicatas do servidor.
- Teste funcional e de carga, com as mesmas quatro fontes do
  `go-syslog-injector`: gerador com `seq=`, modelo, arquivo/stdin e **replay de
  captura do tcpdump** (decisão 3) — inclusive de capturas de syslog UDP/TCP,
  que este injetor reenvia por RELP.

### 1.2 Não-objetivos (v1)

- **Conferir a entrega no destino** (decisão 4; pendência 1). Um `200` do
  servidor significa a garantia do ring dele, não o destino final.
- Comandos RELP além de `open`, `syslog` e `close` (não há outros em uso).
- Captura ao vivo; reproduzir o IP de origem da captura; decifrar TLS capturado;
  arquivo de configuração (como no SI).

### 1.3 Família

O módulo é **filho** do `go-syslog-injector` (SI, o pai) e irmão do
`go-webhook-injector` (HTTP). Como os servidores do monorepo, cada um é um
módulo Go independente e **nada é importado entre eles** (decisão 1): o código
comum (gerador, modelos, leitor de pcap, escalonador, relatório) é copiado e
adaptado. Uma correção num deles vale para os outros: registre no
`CHANGELOG.md` da raiz o que ficou "a portar".

---

## 2. Arquitetura

```
fonte ──> produtor ──> fila (4 × concorrência) ──> N workers ──> N sessões RELP ──> servidor
 gen        ritmo          canal                     cada um       TCP/TLS, janela W
 template   (--rate,                                 com a sua
 file       --pcap-timing,                           conexão
 pcap       --count,
            --duration)                    desfechos ──> relatório (shards por worker)
```

- **Uma** goroutine produtora pede à fonte a mensagem `seq` (de 1 em diante) e a
  solta na fila no instante planejado (§7).
- **N workers** (`--concurrency`), cada um dono de **uma** conexão (ou socket),
  tiram da fila, enviam e registram o desfecho no seu shard do relatório. Uma
  conexão quebrada é fechada, e o worker disca de novo com espera exponencial
  (100 ms dobrando até 2 s).
- O contrato de toda sessão (`sched.Conn`): para cada mensagem (`sched.Msg`)
  entregue a `Send`, **ou** o callback de desfecho é chamado exatamente uma vez
  (chegou a resposta), **ou** a mensagem volta em `Unacked` depois do `Close`
  — nunca as duas, nunca nenhuma. O worker decide o destino das devolvidas
  (retransmitir ou `unanswered`, §4.4). É o que garante
  `mensagens = ok + falhas` no relatório.

### 2.1 Mapa do código

```
cmd/go-relplog-injector/main.go  só chama internal/app
internal/
  app/        flags, validação, montagem, sinais, relatório, código de saída
  tmpl/       modelos com marcadores (compilados uma vez)          [lógica pura]
  source/     gerador, modelo, arquivo/stdin, pcap; cabeçalho 5424/3164
  pcap/       leitor pcap/pcapng, enlaces, IPv4/IPv6, fragmentos,
              remontagem TCP                                       [fuzz]
  pcap/pcaptest/  escritor de capturas para os testes
  extract/    mensagens syslog dentro do tráfego: datagrama, octet,
              LF, RELP; detecção e ressincronização                [fuzz]
  sched/      produtor, workers, ritmo, retransmissão, interrupção
  transport/  cliente RELP (sessão, janela, leitor de respostas), TCP, TLS,
              PROXY protocol
  report/     contadores, histograma, texto/JSON, progresso
```

Dependências: **nenhuma** fora da biblioteca padrão (decisão 6).

---

## 3. Linha de comando

```
go-relplog-injector [flags]
```

### 3.1 Flags

| Flag | Padrão | Significado |
|---|---|---|
| `--target` | `127.0.0.1:20514` | `host:porta` do servidor RELP |
| `--transport` | `tcp` | `tcp` ou `tls` (RELP sobre TLS, o `tls="on"` do `omrelp`) |
| `--timeout` | `10s` | prazo de discagem, handshake, escrita e resposta ao `open` |
| `--window` | `128` | transações sem resposta por sessão (a janela do `librelp`), 1–65536 |
| `--ack-timeout` | `90s` | sessão cuja transação mais antiga espera resposta por mais que isto é quebrada (o `timeout` do `omrelp`) |
| `--retries` | `10` | em quantas sessões **a mais** uma mensagem sem resposta é reenviada (`0` = nunca; o `librelp` não tem limite) |
| `--on-reject` | `break` | depois de uma resposta não `200`: `break` quebra a sessão como o `librelp`; `continue` segue |
| `--tls-ca` / `--tls-cert` / `--tls-key` | — | CAs que verificam o servidor (padrão: as do sistema); certificado e chave do cliente (mTLS) |
| `--tls-server-name` | host do alvo | nome conferido no certificado |
| `--tls-min-version` | `1.2` | `1.2` ou `1.3` |
| `--tls-insecure` | `false` | não verifica o certificado |
| `--proxy-protocol` | `off` | cabeçalho PROXY no TCP/TLS: `off`, `v1`, `v2` (§4.1) |
| `--proxy-src` / `--proxy-dst` | os reais | `ip:porta` anunciados no cabeçalho |
| `--count` | 1; a fonte inteira com `--file`/`--pcap` | mensagens a enviar; `0` = sem limite |
| `--duration` | `0` | para depois deste tempo |
| `--rate` | `0` | mensagens/s somando todas as conexões; `0` = o máximo |
| `--concurrency` | `1` | sessões RELP em paralelo |
| `--drain-timeout` | `10s` | no fim, quanto esperar pelo que ainda está na fila |
| `--template` | — | corpo com marcadores (§5.2) |
| `--file` | — | uma mensagem por linha (`-` = stdin) (§5.3) |
| `--pcap` | — | replay de captura (§6) |
| `--format` | `5424`; `raw` com `--file`/`--pcap` | cabeçalho em volta do corpo: `5424`, `3164` ou `raw` (§5.1) |
| `--facility` / `--severity` | `user` / `notice` | nome ou número; o PRI é `facility × 8 + severity` |
| `--hostname` | o do host, sem domínio | HOSTNAME do cabeçalho |
| `--app` | `go-relplog-injector` | APP-NAME (5424) ou TAG (3164) |
| `--msgid` | `-` | MSGID do 5424 |
| `--size` | `0` | completa as mensagens geradas até este tamanho (§5.1) |
| `--run` | 8 hex aleatórios | identificador da execução (`{run}`) |
| `--loop` | `false` | recomeça `--file`/`--pcap` no fim |
| `--pcap-port` | — | portas de destino a reproduzir; dada, desliga a checagem de conteúdo (§6.1) |
| `--pcap-proto` | `udp,tcp` | protocolos da captura a reproduzir |
| `--pcap-timing` | `asap` | `asap` (ritmo de `--rate`) ou `original` (os intervalos da captura) |
| `--pcap-speed` | `1` | fator de `original` (`2` = duas vezes mais rápido) |
| `--report` | `text` | relatório final no stdout: `text` ou `json` |
| `--progress` | `auto` | linha de progresso no stderr a cada intervalo; `0` desliga; `auto` = 1 s num terminal |
| `--version` | | imprime a versão e sai |

Flags aceitam `-x` e `--x` (pacote `flag`). Argumento posicional é erro.

### 3.2 Combinações recusadas (exit 2)

`--template`, `--file` e `--pcap` juntos; `--format raw` sem fonte de texto (o
gerador não tem cabeçalho próprio); `--size` com `--file`/`--pcap` (o replay
não altera o que foi capturado); `--loop` sem `--file`/`--pcap`, ou com stdin;
`--pcap-timing original` sem `--pcap` ou com `--rate`; `--transport` que não
seja `tcp`/`tls`; `--window` fora de 1–65536; `--retries` negativo;
`--ack-timeout` zero; `--on-reject` desconhecido; `--tls-*` sem
`--transport tls`; marcador desconhecido; captura que não é pcap
nem pcapng. Todas são conferidas **antes** do primeiro envio.

### 3.3 Códigos de saída

| Código | Quando |
|---|---|
| `0` | toda mensagem foi confirmada (`200`) |
| `1` | ao menos uma falhou, ou a fonte deu erro no meio da execução |
| `2` | uso: flag inválida, combinação recusada, arquivo que não abre |
| `3` | alvo inalcançável: houve mensagens e **nenhuma** conexão abriu |
| `130` | interrompido (SIGINT/SIGTERM) |

---

## 4. Sessão RELP

### 4.1 Transporte

TCP, ou TLS (`crypto/tls`, SNI e verificação pelo host do alvo ou
`--tls-server-name`, mTLS opcional). Com `--proxy-protocol v1|v2`, o cabeçalho
do haproxy vai no início de cada conexão, **antes** do handshake TLS, como um
balanceador faz; por padrão anuncia o par real, e `--proxy-src`/`--proxy-dst`
trocam qualquer um dos dois (endereços de famílias diferentes saem como IPv6).

### 4.2 Diálogo

```
injetor  1 open 98 relp_version=0\nrelp_software=go-relplog-injector,<versão>,<url>\ncommands=syslog
servidor 1 rsp <n> 200 OK\nrelp_version=0\n...\ncommands=syslog
injetor  2 syslog <len> <mensagem>
servidor 2 rsp 6 200 OK
...
injetor  N close 0
servidor N rsp 0
```

- O `open` é síncrono e faz parte da discagem: resposta que não seja `200`, ou
  sem `commands=syslog`, é erro de discagem (o worker tenta de novo com espera
  exponencial), e um servidor que nunca aceita dá saída 3.
- `TXNR` começa em 2 depois do `open` e volta a 1 depois de 999 999 999, como no
  `librelp`. Uma mensagem vazia sai como `N syslog 0\n`, sem espaço (o leitor do
  `librelp` recusa o espaço depois de `DATALEN` zero).
- O leitor de respostas aceita `DATALEN 0` com e sem o espaço, casa cada `rsp`
  pelo `TXNR` (podem vir fora de ordem), e trata como fim de sessão: `serverclose`,
  qualquer comando que não seja `rsp`, resposta a uma transação desconhecida e
  moldura inválida. Dados acima de 1 MiB num frame do servidor são inválidos.
- Os frames são escritos num buffer descarregado acima de 64 KiB, quando o
  worker não tem a próxima mensagem, e **antes de esperar vaga na janela** —
  sem isso uma janela cheia esperaria respostas a frames que nem saíram.

### 4.3 Janela e backpressure

Até `--window` transações sem resposta por sessão. Com a janela cheia, o `Send`
espera — é o backpressure explícito do RELP: um servidor que segura as
transações (ring cheio, sink DOWN, rate-limit; SPEC do servidor §5.7) aparece
como **latência** de confirmação, não como erro. A latência é do `Send` até a
resposta (ou até a resposta na sessão em que a mensagem foi enfim confirmada,
somando as retransmissões).

Uma sessão cuja transação mais antiga fica sem resposta por `--ack-timeout` é
quebrada (`no answer within the ack timeout`): o padrão de 90 s é o do `omrelp`
e fica acima dos 30 s de `relp.backpressure_timeout` do servidor, para que o
servidor decida antes de o cliente desistir.

### 4.4 Respostas, quebra e retransmissão

| O que chega | Desfecho da mensagem | A sessão |
|---|---|---|
| `rsp` com `200` | `acked` (sucesso) | segue |
| `rsp` com outro código | `rejected` (falha; nunca retransmitida, como o `librelp` faz com a mensagem destruída) | com `--on-reject break` (padrão) quebra, como o `librelp` (`scrsp.c`); com `continue`, segue |
| nada, e a sessão acaba (`serverclose`, fechamento, erro de escrita, `--ack-timeout`, frame inválido) | volta ao worker | quebrada |

As mensagens de uma sessão quebrada que não tiveram resposta voltam, **em ordem
de envio**, e são reenviadas primeiro na sessão seguinte (`retransmitted` nos
eventos, uma vez por reenvio), até `--retries` sessões a mais; esgotadas, o
desfecho é `unanswered`. Uma mensagem que esperava vaga na janela quando a
sessão morreu não chegou ao fio: volta junto, sem gastar tentativa e sem contar
como retransmissão (achado do teste de janela, 2026-10-30).

**Duplicatas são o comportamento correto.** Uma sessão que morre pode perder
respostas `200` já a caminho — um servidor que fecha o socket com dados não
lidos gera RST, e o cliente descarta o que estava no buffer de recepção. Essas
mensagens são reenviadas e chegam duas vezes ao destino: o RELP não tem id de
mensagem (SPEC do servidor, decisão 3). O injetor as conta como uma mensagem
`acked`; quem conta as duplicatas é o destino, pelo `seq=` do gerador.

### 4.5 Fim

O produtor para; cada worker descarrega o buffer, espera as respostas até o fim
de `--drain-timeout` (um prazo só para toda a fase), manda `close` e espera o
`rsp` vazio. Uma sessão que quebra durante essa espera conta como quebrada, e
o que ela deixou é retransmitido enquanto o prazo durar; o que sobrar é
`unanswered`.

## 5. Fontes

### 5.1 Gerador e cabeçalho

O gerador é o modelo fixo `seq={seq} run={run} injector=go-relplog-injector`.
Toda mensagem leva o número de sequência e a execução: no destino, um buraco na
sequência é **perda** e um repetido é **retransmissão**, o mesmo método do
`demo.sh` do `go-relplog-server`.

O corpo (do gerador, do modelo, de uma linha ou de uma mensagem capturada)
recebe o cabeçalho de `--format`:

| Formato | Mensagem |
|---|---|
| `5424` | `<PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID - CORPO`, `TIMESTAMP` local com microssegundos e zona; `PROCID` = PID do injetor; campo vazio = `-` |
| `3164` | `<PRI>Mmm dd hh:mm:ss HOSTNAME TAG[PID]: CORPO` (dia com espaço à esquerda, sem ano nem zona) |
| `raw` | o corpo, sem nada |

`--size N` completa a mensagem gerada até `N` bytes com um espaço e `x`; uma
mensagem já maior fica como está. Serve para testar o truncamento
(`syslog.max_message_bytes`) e os tetos de bytes do ring.

### 5.2 Modelo

`--template` é compilado uma vez no início: marcador desconhecido é erro de uso.

| Marcador | Valor |
|---|---|
| `{seq}` | número da mensagem, de 1 |
| `{run}` | `--run` |
| `{uuid}` | UUID v4 aleatório, novo a cada mensagem |
| `{ts}` | hora local, RFC 3339 com microssegundos |
| `{unix}` / `{unixms}` | segundos / milissegundos desde a época |
| `{host}` | `--hostname` |
| `{pid}` | PID do injetor |
| `{rand:N}` | `N` caracteres de `[a-z0-9]`, `1 ≤ N ≤ 65536` |

`{{` e `}}` são chaves literais; `}` sozinho também. Com `--format raw`, o
modelo é a mensagem inteira (o usuário escreve o PRI e o cabeçalho, inclusive
inválidos de propósito, para testar o parser do servidor).

### 5.3 Arquivo e stdin

`--file` envia uma mensagem por linha, na ordem: o LF termina a linha, os `CR`
finais saem, linhas vazias são puladas (contadas), a última linha sem LF conta.
Linha acima de 16 MiB é cortada ali (contada). O padrão é `--format raw` (as
linhas já são mensagens syslog); com `5424`/`3164`, cada linha vira o corpo.
`--loop` relê o arquivo do início, limitado por `--count`/`--duration`; um
arquivo sem nenhuma linha termina em vez de girar no vazio. O stdin (`-`) não
pode ser relido: `--loop` com ele é erro de uso.

---

## 6. Replay de captura (`--pcap`)

### 6.1 O que é reproduzido

As mensagens syslog de uma captura do tcpdump (`tcpdump -w`), **em ordem de
captura**, cada uma como um `syslog` RELP: a moldura original é retirada (§6.6)
— uma captura de RELP, de syslog TCP ou de syslog UDP serve igualmente.

- **Datagramas UDP**: um datagrama, uma mensagem.
- **Fluxos TCP** (cliente → servidor, remontados, §6.4) nas três molduras do
  ecossistema: RFC 6587 octet-counting, RFC 6587 LF e **RELP** — dos comandos
  `syslog` (os de `open`, `close` e a direção `rsp` do servidor são
  descartados, contados).
- **Seleção**: sem `--pcap-port`, a checagem de conteúdo é **estrita** — um
  datagrama precisa começar com `<` (o PRI), e um fluxo precisa abrir com uma
  moldura syslog reconhecida seguida de `<` ou com um comando RELP; assim DNS,
  NTP e HTTP da mesma captura não viram syslog. Com `--pcap-port`, só o tráfego
  para essas portas de destino, e sem checagem de conteúdo (mensagens sem PRI
  passam, como o servidor as aceita). `--pcap-proto` restringe a `udp` ou `tcp`.
- O padrão é `--format raw`: a mensagem sai como foi capturada.

### 6.2 Formatos e enlaces

pcap clássico (microssegundos ou nanossegundos, as duas ordens de bytes) e
pcapng (seções, várias interfaces, `if_tsresol`, blocos EPB, SPB e o obsoleto
PB; os demais blocos são pulados). Enlaces: Ethernet (com 802.1Q/802.1ad),
Linux cooked v1 (`-i any` antigo) e **v2** (`-i any` atual), loopback BSD
(`NULL`, `LOOP`), raw IPv4/IPv6 (101, 12, 14, 228, 229) e **PKTAP** do macOS
(`-i any` no Mac). Outro enlace é contado (`pcap_not_ip`) e pulado. Um arquivo
cortado no meio de um registro (tcpdump ainda escrevendo, ou morto) reproduz
tudo até ali e conta `pcap_cut_at_end`.

### 6.3 IP

IPv4 (com o enchimento do Ethernet descartado) e IPv6 (com os cabeçalhos de
extensão hop-by-hop, routing e destination). **Fragmentos** IPv4 e IPv6 são
remontados — uma mensagem UDP acima do MTU só existe inteira depois disso —
com limites: 64 KiB e 128 fragmentos por datagrama, 1024 datagramas abertos, 30
s de janela em tempo de captura; o fragmento repetido idêntico (o `-i any` vê o
quadro em duas interfaces) é ignorado; sobreposição é recusada (RFC 5722). O
que se perde é contado (`pcap_ip_fragments_lost`). Pacote cortado pelo snaplen
é marcado: um datagrama cortado **não** é reproduzido (`frames_lost`).

### 6.4 Remontagem TCP

Um fluxo de bytes por **direção** de conexão, em ordem de sequência (com a
volta de 2³²), sem os bytes retransmitidos (contados), com os segmentos fora de
ordem segurados até 4 MiB por direção. Um buraco que não se fecha — perda na
captura — é entregue ao extrator como **lacuna** (`tcp_gaps`, `tcp_gap_bytes`),
que descarta o quadro atingido e ressincroniza. O snaplen que corta um segmento
vira lacuna do tamanho que falta. FIN/RST encerram a direção; um SYN novo na
mesma quádrupla é conexão nova; uma direção ociosa por 10 min de captura é
encerrada; no máximo 131 072 direções abertas (as excedentes são ignoradas,
contadas). Uma direção cuja captura começou depois do SYN é **midstream**
(`tcp_midstream`): seu primeiro byte não é início de quadro.

Uma direção que termina com a captura sem FIN nem RST conta em
`tcp_unterminated`: o fim dela pode faltar **sem lacuna visível** (o tcpdump
parou antes de ler o buffer do kernel). Foi o que a primeira prova real
mostrou (TESTING §3): pare o tcpdump alguns segundos depois do tráfego, com
`-B` generoso.

### 6.5 Ritmo

`--pcap-timing asap` (padrão) envia no ritmo de `--rate` (ou o máximo).
`original` agenda cada mensagem no seu deslocamento desde o **primeiro pacote**
da captura, dividido por `--pcap-speed`: o primeiro pacote não syslog também
conta, para que o silêncio do começo seja reproduzido. Uma mensagem extraída de
TCP leva o instante do segmento que a completou. Com `--loop`, cada passada
começa 1 ms depois da última mensagem da anterior. Um relógio que volta na
captura não volta no envio. Atraso de mais de 100 ms em relação ao plano (o
alvo ou os workers não acompanham) é contado no relatório
(`behind_schedule`).

### 6.6 Molduras dentro de um fluxo

Decididas **uma vez por fluxo**, nos primeiros bytes, como o servidor decide
(SPEC do `go-syslog-server` §5.2):

| Os primeiros bytes | Moldura |
|---|---|
| `TXNR SP (open\|syslog\|close) SP` | RELP |
| `TXNR SP (rsp\|serverclose) SP` | direção do servidor RELP: ignorada |
| `[1-9][0-9]{0,9} SP` (estrito: seguido de `<`) | octet-counting |
| `<` (não estrito: qualquer coisa) | LF |
| outra coisa (estrito) | ignorado (`tcp_streams_not_syslog`) |

- **Octet**: exatamente `MSG-LEN` octetos, nada removido. **LF**: termina no LF
  ou NUL, `CR` finais saem, quadros vazios pulados; a última linha sem
  terminador no fim do fluxo é entregue (como o servidor). **RELP**:
  `TXNR SP COMANDO SP DATALEN [SP DADOS] LF`; os dados de `syslog` são a
  mensagem.
- **Ressincronização** — depois de uma lacuna, de um quadro inválido, ou desde
  o início de um fluxo midstream — descarta bytes até a primeira posição que
  **comprovadamente** começa um quadro:
  - LF: o byte depois de um terminador (estrito: que seja `<`);
  - RELP: depois de um LF, um comando RELP conhecido cujo quadro inteiro
    analisa até o LF final;
  - octet: `LEN SP` (estrito: seguido de `<`) cujo quadro seguinte também
    começa com um cabeçalho válido (estrito: seguido de `<`).

  Num fluxo midstream as três são buscadas, e vence a posição mais cedo. Um
  candidato que ainda espera bytes **segura** a decisão enquanto declara até 64
  KiB, mesmo que um candidato posterior já esteja comprovado: assim a escolha
  não depende de como a captura dividiu o fluxo em segmentos (achado do fuzz,
  `FuzzStream`, três entradas no corpus). Sem candidato, guarda só os últimos 64
  bytes. Tudo o que foi pulado é contado (`tcp_skipped_bytes`, `tcp_resyncs`), e
  o quadro perdido também (`frames_lost`).
- Quadro acima de 16 MiB não é quadro (ressincroniza); linha LF acima disso é
  cortada (`tcp_long_lines`).

### 6.7 O que não é reproduzido

- TLS capturado (porta 6514) — o conteúdo é cifrado. Fica em
  `tcp_streams_not_syslog`.
- O IP de origem (§1.2); com `--proxy-protocol` dá para anunciar **um** endereço
  por execução, não um por mensagem (pendência 3).
- **Duplicatas do `-i any`**: no Linux, um pacote que atravessa duas interfaces
  (eth0 e a veth de um container) é capturado duas vezes, às vezes com o
  destino reescrito pelo NAT; o datagrama UDP repetido é reproduzido duas vezes.
  Capture numa interface só (`-i eth0`) (pendência 2).
- Direções de servidor e mensagens de outros protocolos na mesma porta.

---

## 7. Escalonamento

- `--count` e `--duration` limitam a execução; o que vier primeiro. Uma fonte
  finita (arquivo, captura) termina antes se acabar.
- `--rate R` agenda a mensagem `seq` em `início + (seq−1)/R`: a taxa é **global**
  (somando as conexões), e o produtor que se atrasa não compensa com rajada além
  do plano — ele envia tão logo possa, e o atraso fica no relatório. Sem
  `--rate`, o máximo que os workers aguentam.
- A fila tem `4 × --concurrency` posições; o produtor bloqueia quando ela está
  cheia (o backpressure do servidor chega até ele). `--duration` interrompe
  também essa espera: a mensagem que não entrou na fila não foi enviada e não
  conta.
- **Fim**: o produtor para, a fila esvazia, cada worker escreve o buffer e
  fecha. Quem ainda não conseguiu conexão tem `--drain-timeout` para conseguir;
  depois disso, as mensagens dele são `not_sent`.
- **Interrupção**: o primeiro SIGINT/SIGTERM para o produtor e desiste das
  discagens pendentes (`not_sent`), mas o que já está na fila é enviado e o
  relatório sai; o segundo imprime o relatório parcial e sai na hora. Saída 130.

---

## 8. Relatório

### 8.1 Formas

`--report text` (padrão) ou `json`, no **stdout** e só no fim; o progresso e os
avisos vão para o **stderr**. O JSON (`report.Summary`): `info` (injetor,
execução, alvo, moldura, formato, fonte, plano), `start`, `elapsed_sec`,
`messages`, `ok`, `failed`, `outcomes`, `bytes`, `rate_msgs_per_sec`,
`rate_bytes_per_sec`, `latency` (µs: min, mean, p50, p90, p99, p99.9, max — só
dos sucessos), `connections` (`opened`, `dial_errors`, `broken`), `events`
(erros de discagem e quebras agrupados pelo texto final do erro) e `source`
(contadores da fonte, §8.4).

### 8.2 Desfechos

| Desfecho | Sucesso | Significado |
|---|---|---|
| `acked` | sim | `rsp` com `200` |
| `rejected` | não | `rsp` com outro código (o texto não é guardado; o servidor conta) |
| `unanswered` | não | nenhuma resposta em `1 + --retries` sessões, ou o prazo do fim acabou |
| `not_sent` | não | nenhuma sessão abriu até o fim (`--drain-timeout`) ou interrompido |

A latência é um histograma log-linear (32 sub-baldes por potência de dois, erro
de ~3%), dos sucessos, mesclado dos shards no fim. Os eventos trazem
`retransmitted` (reenvios), `conn_broken: <motivo>` (inclusive
`server sent serverclose`, `no answer within the ack timeout`,
`session ended after a rejected message`) e `dial_error: <motivo>` (inclusive
`relp open refused: ...`).

### 8.3 Progresso

Uma linha por intervalo no stderr: tempo, sucessos, falhas, taxa do intervalo,
conexões abertas. `auto` liga só quando o stderr é um terminal, para não sujar
um log de CI.

### 8.4 Contadores da fonte

Arquivo: `file_lines`, `file_empty_lines`, `file_long_lines`, `file_passes`.
Captura: `pcap_*` (pacotes, fora de IP, malformados, outro protocolo, cortados
pelo snaplen, fragmentos vistos/remontados/perdidos, passadas, corte no fim),
`udp_datagrams`, `udp_not_syslog`, `tcp_*` (direções, midstream, filtradas,
excedentes, bytes retransmitidos, fora de ordem, lacunas e seus bytes,
`tcp_unterminated`, fluxos por moldura, comandos RELP sem mensagem,
ressincronizações, bytes pulados, linhas longas, mensagens), `frames_lost` e
`messages_replayed`. São a prova de que o replay entregou tudo o que a captura
tinha — ou do que faltou nela.

---

## 9. Pendências conhecidas

1. **Conferência no destino** (decisão 4): ler o NATS/PostgreSQL e contar as
   `seq` da execução (perdidas, duplicadas). Hoje o relatório para no `200`.
2. **Deduplicação das duplicatas do `-i any`** (§6.7), como no SI.
3. **Uma sessão por fluxo original** no replay, cada uma com o PROXY da origem
   capturada.
4. **Fragmentos com captura cortada pelo snaplen**, como no SI.
5. **Candidato de ressincronização acima de 64 KiB** (§6.6), como no SI.
6. **O texto das recusas** (`500 ...`) não vai ao relatório: só a contagem. Com
   cardinalidade aberta, ele iria para os eventos agrupado pelo texto.
