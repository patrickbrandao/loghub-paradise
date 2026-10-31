# go-syslog-injector — Especificação v1

Ferramenta cliente que **injeta mensagens syslog** num servidor — em especial
os do ecossistema LogHub (`go-syslog-server`, `go-syslog-pg-server`,
`go-syslog-nats-server`, `go-syslog-standard-server`) — para testar o
comportamento dele ou colocá-lo sob carga. Criada em 2026-10-30; as decisões que
a definiram estão em `docs/DECISOES.md`, citadas aqui como "decisão N".

Esta especificação é a fonte da verdade: discrepância entre código e spec é bug
no código. Para mudar comportamento, primeiro a spec (com o porquê), depois o
código, depois o teste.

---

## 1. Escopo

### 1.1 Objetivos

- Enviar mensagens syslog por **UDP** (RFC 5426), **TCP** (RFC 6587, as duas
  molduras), **TLS** (RFC 5425) e **socket unix datagrama** (`/dev/log`).
- Servir para o teste **funcional** (uma mensagem, ou poucas, e o que aconteceu
  com cada uma) e para a **carga** (taxa alvo, concorrência, duração, vazão e
  latência medidas).
- Quatro fontes de conteúdo (decisão 3): o **gerador** embutido com `seq=`, um
  **modelo com marcadores**, as linhas de um **arquivo ou do stdin** e o
  **replay de uma captura do tcpdump** (pcap ou pcapng), que é a forma de levar
  ao servidor o tráfego real de um servidor de produção.
- Relatório final honesto: cada mensagem tem **um** desfecho (§8.2), e o código
  de saída diz se alguma falhou.

### 1.2 Não-objetivos (v1)

- **Conferir a entrega no destino** (NATS, PostgreSQL...). O relatório vem do
  que o transporte informa; no syslog isso é "o kernel aceitou", não "o
  servidor gravou" (decisão 4; pendência 1).
- Captura ao vivo de uma interface: a entrada do replay é sempre um arquivo.
- Reproduzir o **IP de origem** da captura (exigiria socket cru e permissão de
  root). O endereço original pode ser anunciado no PROXY protocol (§4.4).
- Decifrar TLS capturado (porta 6514): não há como recuperar as mensagens.
- Arquivo de configuração: a ferramenta é de linha de comando, só flags.

### 1.3 Família

O módulo é o **pai** dos injetores `go-relplog-injector` (RELP) e
`go-webhook-injector` (HTTP). Como os servidores do monorepo, cada um é um
módulo Go independente e **nada é importado entre eles** (decisão 1): o código
comum (gerador, modelos, leitor de pcap, escalonador, relatório) é copiado e
adaptado. Uma correção num deles vale para os outros: registre no
`CHANGELOG.md` da raiz o que ficou "a portar".

---

## 2. Arquitetura

```
fonte ──> produtor ──> fila (4 × concorrência) ──> N workers ──> N conexões ──> servidor
 gen        ritmo          canal                     cada um       UDP/TCP/TLS/unix
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
- O contrato de toda conexão (`sched.Conn`): o callback de desfecho é chamado
  **exatamente uma vez** por mensagem entregue a `Send`, aconteça o que
  acontecer — na hora, no próximo `Flush`, quando o servidor responde ou no
  `Close`. É o que garante `mensagens = ok + falhas` no relatório.

### 2.1 Mapa do código

```
cmd/go-syslog-injector/main.go   só chama internal/app
internal/
  app/        flags, validação, montagem, sinais, relatório, código de saída
  tmpl/       modelos com marcadores (compilados uma vez)          [lógica pura]
  source/     gerador, modelo, arquivo/stdin, pcap; cabeçalho 5424/3164
  pcap/       leitor pcap/pcapng, enlaces, IPv4/IPv6, fragmentos,
              remontagem TCP                                       [fuzz]
  pcap/pcaptest/  escritor de capturas para os testes
  extract/    mensagens syslog dentro do tráfego: datagrama, octet,
              LF, RELP; detecção e ressincronização                [fuzz]
  sched/      produtor, workers, ritmo, interrupção
  transport/  UDP, unix, TCP, TLS, PROXY protocol
  report/     contadores, histograma, texto/JSON, progresso
```

Dependências: **nenhuma** fora da biblioteca padrão (decisão 6).

---

## 3. Linha de comando

```
go-syslog-injector [flags]
```

### 3.1 Flags

| Flag | Padrão | Significado |
|---|---|---|
| `--target` | `127.0.0.1:1514` | `host:porta` do servidor; com `--transport unix`, o caminho do socket |
| `--transport` | `tcp` | `udp`, `tcp`, `tls` ou `unix` (datagrama) |
| `--framing` | `octet` | moldura do TCP: `octet` (RFC 6587 §3.4.1) ou `lf` (§3.4.2); no TLS é sempre `octet` |
| `--timeout` | `10s` | prazo de discagem, handshake e escrita |
| `--tls-ca` / `--tls-cert` / `--tls-key` | — | CAs que verificam o servidor (padrão: as do sistema); certificado e chave do cliente (mTLS) |
| `--tls-server-name` | host do alvo | nome conferido no certificado |
| `--tls-min-version` | `1.2` | `1.2` ou `1.3` |
| `--tls-insecure` | `false` | não verifica o certificado |
| `--proxy-protocol` | `off` | cabeçalho PROXY no TCP/TLS: `off`, `v1`, `v2` (§4.4) |
| `--proxy-src` / `--proxy-dst` | os reais | `ip:porta` anunciados no cabeçalho |
| `--count` | 1; a fonte inteira com `--file`/`--pcap` | mensagens a enviar; `0` = sem limite |
| `--duration` | `0` | para depois deste tempo |
| `--rate` | `0` | mensagens/s somando todas as conexões; `0` = o máximo |
| `--concurrency` | `1` | conexões (sockets) em paralelo |
| `--drain-timeout` | `10s` | no fim, quanto esperar pelo que ainda está na fila |
| `--template` | — | corpo com marcadores (§5.2) |
| `--file` | — | uma mensagem por linha (`-` = stdin) (§5.3) |
| `--pcap` | — | replay de captura (§6) |
| `--format` | `5424`; `raw` com `--file`/`--pcap` | cabeçalho em volta do corpo: `5424`, `3164` ou `raw` (§5.1) |
| `--facility` / `--severity` | `user` / `notice` | nome ou número; o PRI é `facility × 8 + severity` |
| `--hostname` | o do host, sem domínio | HOSTNAME do cabeçalho |
| `--app` | `go-syslog-injector` | APP-NAME (5424) ou TAG (3164) |
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
`--pcap-timing original` sem `--pcap` ou com `--rate`; `--framing lf` com TLS;
`--proxy-protocol` fora de TCP/TLS; `--tls-*` sem `--transport tls`;
`--transport unix` sem caminho; marcador desconhecido; captura que não é pcap
nem pcapng. Todas são conferidas **antes** do primeiro envio.

### 3.3 Códigos de saída

| Código | Quando |
|---|---|
| `0` | toda mensagem teve desfecho de sucesso |
| `1` | ao menos uma falhou, ou a fonte deu erro no meio da execução |
| `2` | uso: flag inválida, combinação recusada, arquivo que não abre |
| `3` | alvo inalcançável: houve mensagens e **nenhuma** conexão abriu |
| `130` | interrompido (SIGINT/SIGTERM) |

---

## 4. Transportes

### 4.1 UDP

Socket UDP **conectado**, um datagrama por mensagem, desfecho na hora. Não há
backpressure nem confirmação: `sent` é "o kernel aceitou". Desfechos de falha:
`too_large` (acima de 65 507 bytes, recusado antes de enviar, ou `EMSGSIZE`),
`nobufs` (`ENOBUFS`/`EAGAIN`: o kernel local descartou; comum no macOS a taxas
altas) e `refused` (`ECONNREFUSED`: um ICMP *port unreachable* de um datagrama
**anterior**; o desfecho fica nesta mensagem, que pode ter chegado).

### 4.2 unix datagrama

Socket `unixgram` conectado ao caminho de `--target`. O backpressure do servidor
num socket unix é o do kernel (SPEC do `go-syslog-server` §4.3): com o buffer
do receptor cheio, o Linux bloqueia o `send` e o macOS devolve `ENOBUFS`. Aqui
`ENOBUFS`/`EAGAIN` **esperam** (1 ms entre tentativas, até `--timeout`), como a
`syslog(3)` da libc; vencido o prazo, `nobufs`. `ECONNREFUSED` (ninguém escuta)
quebra a conexão e o worker disca de novo.

### 4.3 TCP e TLS

- Moldura `octet` (`MSG-LEN SP MSG`) ou `lf` (`MSG LF`). Com `lf`, um LF dentro
  da mensagem a divide em duas no servidor: é a natureza da moldura, e o
  injetor não a altera (use `octet`).
- As mensagens são enquadradas num buffer escrito quando passa de **64 KiB** ou
  quando o worker não tem a próxima mensagem pronta. O desfecho (`sent` ou
  `write_error`) é registrado na escrita do buffer, para **todas** as mensagens
  dele; a latência é do `Send` até essa escrita.
- **Não há confirmação no syslog sobre fluxo.** Uma goroutine por conexão só
  espera o EOF: se o servidor fechar (prazo de ociosidade, shutdown), a próxima
  mensagem falha com `write_error`, a conexão conta como quebrada e o worker
  reconecta. O que foi escrito entre o fechamento do servidor e a percepção dele
  aparece como `sent` e pode ter se perdido — é a mesma cegueira de qualquer
  emissor syslog/TCP, documentada e não escondida.
- O backpressure do servidor (ele para de ler, SPEC do servidor §5.7) aparece
  como latência de escrita crescente e, além de `--timeout`, `write_error`.
- TLS: `crypto/tls`, SNI e verificação pelo host do alvo (ou
  `--tls-server-name`), mTLS opcional. Só `octet` (RFC 5425 §4.3).

### 4.4 PROXY protocol

`--proxy-protocol v1|v2` escreve o cabeçalho do haproxy (`proxy-protocol.txt`)
no início de cada conexão TCP/TLS, **antes** do handshake TLS, como um
balanceador faz. Por padrão anuncia o par real (local como origem, o endereço
discado como destino); `--proxy-src`/`--proxy-dst` trocam qualquer um dos dois.
Endereços de famílias diferentes saem como IPv6 (IPv4 mapeado). Serve para
testar `proxy.protocol` e `proxy.trusted` do servidor, e para atribuir uma
origem diferente às mensagens.

---

## 5. Fontes

### 5.1 Gerador e cabeçalho

O gerador é o modelo fixo `seq={seq} run={run} injector=go-syslog-injector`.
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
captura**, pelo transporte escolhido: uma captura de UDP pode ser reproduzida
em TCP, e vice-versa, porque a moldura original é retirada (§6.6) e a do
transporte é aplicada no envio.

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
| `sent` | sim | o kernel aceitou (UDP, unix) ou o buffer foi escrito (TCP, TLS) |
| `write_error` | não | a conexão quebrou antes de a mensagem ser escrita |
| `too_large` | não | não cabe num datagrama |
| `nobufs` | não | o kernel local descartou o datagrama |
| `refused` | não | ICMP port unreachable (UDP) |
| `not_sent` | não | nenhuma conexão até o fim (`--drain-timeout`) ou interrompido |

A latência é um histograma log-linear (32 sub-baldes por potência de dois, erro
de ~3%), mesclado dos shards no fim.

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
   `seq` da execução (perdidas, duplicadas). Hoje o relatório para no transporte.
2. **Deduplicação das duplicatas do `-i any`** (§6.7): exigiria uma janela por
   (origem, IP ID, hash do conteúdo), e o NAT entre as cópias atrapalha a chave.
3. **Uma conexão por fluxo original** no replay, cada uma com o PROXY da origem
   capturada, para que o servidor veja os emissores como eram.
4. **Fragmentos com captura cortada pelo snaplen**: o datagrama é descartado
   inteiro; não há como reconstruir o que faltou.
5. **Candidato de ressincronização acima de 64 KiB** (§6.6): enquanto o quadro
   não chegou, ele não segura a decisão, e a escolha pode depender da divisão
   dos segmentos. Quadros syslog desse tamanho no meio de uma ressincronização
   são raros; o fuzz não usa entradas tão grandes.
