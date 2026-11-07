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
- **Conferir a entrega no destino** (decisão 20, que reviu a 4): com
  `--verify-url` ou `--verify-sqlite`, depois do envio o injetor lê o destino
  em que o servidor grava (NATS JetStream, PostgreSQL `loghub.inbox`, stream
  Redis/Valkey, SQLite) e conta, pelo `seq=` da execução, as mensagens
  confirmadas que não chegaram (perdidas) e as que chegaram mais de uma vez
  (duplicadas) — no relatório e no código de saída (§8.5). Um `200` do servidor
  significa a garantia do ring dele; a conferência é o que prova o destino.

### 1.2 Não-objetivos (v1)

- Comandos RELP além de `open`, `syslog` e `close` (não há outros em uso).
- Captura ao vivo; reproduzir o IP de origem da captura **no socket** (só pelo
  PROXY protocol, §6.9); decifrar TLS capturado; arquivo de configuração (como
  no SI).
- Conferir destinos que o servidor grava e que não são o eventbus/fila do
  LogHub (`sink=mysql`, `sink=clickhouse`), nem mensagens sem marcador de
  execução (`--file`, `--pcap`, modelo sem `{seq}` e `{run}`): sem `seq` não há
  o que contar (§8.5).

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
  verify/     conferência no destino: URL, leitores NATS/PostgreSQL/
              Redis/SQLite, casamento do marcador, contagem      [§8.5]
```

Dependências: **nenhuma** fora da biblioteca padrão (decisão 6) no caminho de
envio. A conferência no destino (`internal/verify`, §8.5) usa os clientes que os
servidores já usam, nas **mesmas versões** do `go-relplog-server/go.mod`
(decisão 20): `nats.go`, `pgx/v5`, `rueidis` e `modernc.org/sqlite` (puro Go,
o binário continua estático com `CGO_ENABLED=0`).

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
| `--count` | 1; sem limite com `--duration`; a fonte inteira com `--file`/`--pcap` | mensagens a enviar; `0` = sem limite |
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
| `--pcap-dedup` | `auto` | descarta as cópias do `-i any` (§6.8): `auto` (= `on`), `on`, `off` |
| `--pcap-flows` | `false` | uma sessão RELP por fluxo original, com o PROXY da origem capturada (§6.9) |
| `--verify-url` | — | confere no destino depois do envio: `nats://`, `tls://`, `postgres://`, `postgresql://`, `redis://`, `rediss://`, `valkey://`, `valkeys://` (§8.5) |
| `--verify-sqlite` | — | confere no arquivo SQLite do `sink=sqlite` (§8.5) |
| `--verify-timeout` | `30s` | quanto esperar, depois do envio, que o destino tenha toda mensagem confirmada |
| `--verify-space` | `default` | `space` LogHub do destino (subject NATS, stream Redis; filtro da coluna `space` no PostgreSQL) |
| `--verify-key` | o do servidor | stream NATS, chave do stream Redis ou tabela PostgreSQL a ler |
| `--verify-allow-duplicates` | `false` | duplicatas no destino não mudam o código de saída (continuam contadas); só no RI |
| `--verify-tls-ca` | — | CAs (PEM) que verificam o destino em `tls://`, `rediss://` e `valkeys://` (padrão: as do sistema); o mesmo que o parâmetro `ca=` da URL, que continua valendo (§8.5) |
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
nem pcapng; `--pcap-flows` sem `--pcap`; `--pcap-dedup` fora de `auto|on|off`;
`--verify-url` com `--verify-sqlite`;
`--verify-*` com `--file`/`--pcap` ou com um modelo sem `{seq}` e `{run}`;
`--verify-url` com esquema ou parâmetro desconhecido (a mensagem nomeia o
parâmetro, nunca o valor nem a URL: pode haver senha nela), ou com um elemento
da lista NATS que não é URL (mensagem genérica, §8.5); `--verify-timeout`,
`--verify-space`, `--verify-key`, `--verify-tls-ca` ou
`--verify-allow-duplicates` sem `--verify-*` (e os dois primeiros e
`--verify-tls-ca` com `--verify-sqlite`); `--verify-tls-ca` sem esquema TLS,
junto do `ca=` da URL, ou com arquivo sem certificado PEM; `--verify-timeout`
zero ou negativo; `--proxy-src`/`--proxy-dst` com `--pcap-flows` (§6.9).
Todas são conferidas **antes** do primeiro envio. Também é código 2, conferido
na sonda do destino e ainda antes do envio, um `--run` que **já está** no
PostgreSQL (§8.5).

### 3.3 Códigos de saída

| Código | Quando |
|---|---|
| `0` | toda mensagem foi confirmada (`200`) |
| `1` | ao menos uma falhou, ou a fonte deu erro no meio da execução |
| `2` | uso: flag inválida, combinação recusada, arquivo que não abre; `--run` já presente no PostgreSQL (§8.5) |
| `3` | alvo inalcançável: houve mensagens e **nenhuma** conexão abriu |
| `4` | conferência no destino **impossível**: o destino não respondeu antes do envio (nada é enviado), ou a última leitura depois do envio falhou (§8.5) |
| `130` | interrompido (SIGINT/SIGTERM) |

Com `--verify-*`, uma mensagem **confirmada** que não chegou ao destino, ou uma
cópia a mais (sem `--verify-allow-duplicates`), é falha: `1`. `unexpected`
não muda o código.
Precedência, do mais grave: `130`, `3`, `4`, `1`. Sem `--verify-*`, `4` nunca
sai. Interrompido ou inalcançável, não há conferência. (A mesma tabela do SI:
decisão 21, alinhada com ele em 2026-11-07.)

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
| `rsp` com outro código | `rejected` (falha; nunca retransmitida, como o `librelp` faz com a mensagem destruída); o texto da resposta vai aos eventos (§8.2) | com `--on-reject break` (padrão) quebra, como o `librelp` (`scrsp.c`); com `continue`, segue |
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
`--loop` relê o arquivo do início, limitado por `--count`/`--duration`; uma
passada sem nenhuma mensagem (arquivo vazio, só de linhas em branco, ou
esvaziado entre passadas) termina a fonte em vez de girar no vazio; o mesmo
vale para uma passada de `--pcap` sem mensagem (correção de 2026-10-31,
achada na revisão do go-gelf-injector). O stdin (`-`) não
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

O cabeçalho PKTAP é o `struct pktap_header` do xnu: `pth_length`,
`pth_type_next` (`1` = segue um pacote; outro valor não é pacote e conta em
`pcap_not_ip`), **`pth_dlt`** (o enlace do que segue) e `pth_ifname` (24 bytes).
Até 2026-11-07 o leitor tomava `pth_type_next` por enlace: como ele vale 1, que
é também o número do Ethernet, só pacotes de interfaces Ethernet decodificavam,
e o loopback (`lo0`, `NULL`) de uma captura `-i any` local virava lixo (achado
ao implementar §6.8, que precisa do `pth_ifname`; **a portar** ao SI e ao GI).

Cada pacote leva a **identidade da interface** em que foi capturado, para §6.8:
o número da interface do pcapng (blocos EPB), mais o índice da interface do
Linux cooked v2, o tipo de pacote e o endereço de enlace do cooked v1, ou o
`pth_ifname` do PKTAP. Num pcap clássico Ethernet, todos os pacotes têm a mesma.

### 6.3 IP

IPv4 (com o enchimento do Ethernet descartado) e IPv6 (com os cabeçalhos de
extensão hop-by-hop, routing e destination). **Fragmentos** IPv4 e IPv6 são
remontados — uma mensagem UDP acima do MTU só existe inteira depois disso —
com limites: 64 KiB e 128 fragmentos por datagrama, 1024 datagramas abertos, 30
s de janela em tempo de captura; o fragmento repetido idêntico (o `-i any` vê o
quadro em duas interfaces) é ignorado; sobreposição é recusada (RFC 5722). O
que se perde é contado (`pcap_ip_fragments_lost`), inclusive os fragmentos que
ainda esperavam a remontagem quando a passada da captura acabou (até 2026-11-07
eles sumiam sem contagem). Pacote cortado pelo snaplen é marcado: um datagrama
cortado **não** é reproduzido (`frames_lost`).

**Fragmento cortado pelo snaplen** (pendência 4): os bytes que o tcpdump não
gravou não estão no arquivo, e nenhum outro fragmento os repõe — o datagrama
não tem conserto. O que dá para fazer, e é feito: o primeiro fragmento cortado
**condena** o datagrama inteiro (a chave fica marcada até a janela de 30 s
acabar): os fragmentos que já esperavam e os que chegarem depois são
descartados na hora, contados em `pcap_ip_fragments_lost`, em vez de esperar
30 s e expirar; e o datagrama conta **uma** vez em `pcap_ip_datagrams_snaplen`,
o número que diz "estas mensagens existiam e a captura as cortou". Um datagrama
condenado que é UDP (o protocolo está em todo fragmento) conta também em
`frames_lost`, como um datagrama inteiro cortado: é o máximo que a captura
permite saber sem o conteúdo (se era syslog, só o primeiro fragmento diria, e
ele pode ser justamente o cortado). Para não cortar, capture com `-s 0` (o
padrão do tcpdump atual, 262144).

**Cópia tardia de um fragmento** (regra de 2026-11-07, achado 9 da revisão dos
injetores): no `-i any` sem NAT as cópias de um fragmento têm a mesma chave de
remontagem, e intercaladas (`f1` na veth, `f1` na eth0, `f2` na veth — que
completa o datagrama —, `f2` na eth0) a última abria um datagrama órfão que
vencia em 30 s e contava em `pcap_ip_fragments_lost`, sem nada perdido. O
datagrama completado é lembrado por **50 ms** de captura (a janela das cópias,
§6.8), com o deslocamento e o tamanho de cada fragmento: um fragmento dessa
chave igual a um deles, nesse prazo, é cópia e é ignorado (como o fragmento
repetido de um datagrama aberto); qualquer outro abre um datagrama novo. Os
50 ms bastam para as cópias (microssegundos uma da outra) e não confundem a
reutilização do IP ID, que leva 65 536 datagramas do mesmo par.

### 6.4 Remontagem TCP

Um fluxo de bytes por **direção** de conexão, em ordem de sequência (com a
volta de 2³²), sem os bytes retransmitidos (contados), com os segmentos fora de
ordem segurados até 4 MiB por direção — contando, desde 2026-11-07, também um
custo fixo de 64 bytes por segmento segurado, para que os segmentos sem bytes
capturados (o snaplen cortou o payload inteiro) não se acumulem sem teto; os
segmentos segurados ficam num heap mínimo por sequência (só o menor importa),
e a fila não é mais reordenada a cada segmento retirado (era O(n² log n) num
buraco com muitos segmentos atrás; achado 10 da revisão,
`internal/pcap/tcp.go`, igual nos quatro injetores). Um buraco que não se fecha — perda na
captura — é entregue ao extrator como **lacuna** (`tcp_gaps`, `tcp_gap_bytes`),
que descarta o quadro atingido e ressincroniza. O snaplen que corta um segmento
vira lacuna do tamanho que falta. FIN/RST encerram a direção; um SYN novo na
mesma quádrupla é conexão nova; uma direção ociosa por 10 min de captura é
encerrada; no máximo 131 072 direções abertas (as excedentes são ignoradas,
contadas). Uma direção cuja captura começou depois do SYN é **midstream**
(`tcp_midstream`): seu primeiro byte não é início de quadro.

**Como a direção terminou** (regra de 2026-11-07, portada do
`go-gelf-injector`, decisão G12 de lá) decide o último quadro (§6.6): ela é
**fechada** quando a captura prova o fim — um FIN com todos os bytes antes
dele entregues, ou um RST sem buraco antes dele (um RST adiante do que foi
entregue diz que faltam bytes); é **cortada** no fim da captura, quando fica
ociosa ou quando a quádrupla é reusada sem o fim da anterior.

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
  ou NUL, `CR` finais saem, quadros vazios pulados. **RELP**:
  `TXNR SP COMANDO SP DATALEN [SP DADOS] LF`; os dados de `syslog` são a
  mensagem.
- **O último quadro, sem o seu fim** (regra de 2026-11-07, do GI; a mesma do
  SI): numa direção **fechada** (§6.4) ele é lido como o servidor que recebia
  a captura o lê no EOF; numa **cortada**, vale a regra de antes. Por moldura:
  - LF: a última linha sem terminador é mensagem **nas duas** (o
    `go-syslog-server` a entrega inteira no EOF, SPEC dele §5.3; uma linha não
    tem tamanho que prove o corte, e era a regra de sempre);
  - octet: **fechada**, o que chegou do quadro (com o cabeçalho e ao menos um
    byte da mensagem) é mensagem — o `go-syslog-server` entrega o frame
    parcial no EOF (marcado `truncated`, SPEC dele §5.4) — e conta em
    `tcp_closed_partial_frames`; o replay manda esses bytes como mensagem
    inteira, sem a marca. **Cortada**: perdido (`frames_lost`), como antes;
  - RELP: o quadro sem o LF final nunca é lido pelo `librelp` nem pelo
    `go-relplog-server` (não há o que confirmar): perdido nas duas;
  - o que uma ressincronização deixou não é quadro: perdido nas duas.
- **Ressincronização** — depois de uma lacuna, de um quadro inválido, ou desde
  o início de um fluxo midstream — descarta bytes até a primeira posição que
  **comprovadamente** começa um quadro:
  - LF: o byte depois de um terminador (estrito: que seja `<`);
  - RELP: depois de um LF, um comando RELP conhecido cujo quadro inteiro
    analisa até o LF final;
  - octet: `LEN SP` (estrito: seguido de `<`) cujo quadro seguinte também
    começa com um cabeçalho válido (estrito: seguido de `<`).

  Num fluxo midstream as três são buscadas, e vence a posição mais cedo. Um
  candidato que ainda espera bytes **segura** a decisão, mesmo que um candidato
  posterior já esteja comprovado: assim a escolha não depende de como a captura
  dividiu o fluxo em segmentos (achado do fuzz, `FuzzStream`, três entradas no
  corpus). Até 2026-11-07 só segurava o candidato que declarava até 64 KiB, e
  acima disso a escolha voltava a depender da divisão (pendência 5); agora
  segura **todo** candidato válido, isto é, até o teto de quadro (16 MiB, o
  mesmo que define um cabeçalho válido). A memória continua limitada: as
  posições antes do candidato que espera já foram recusadas para sempre e saem
  do buffer (fica só o contexto de 64 bytes antes dele), então um fluxo em
  ressincronização guarda no máximo um quadro (16 MiB) e o cabeçalho do
  seguinte. E o custo continua linear: a busca recomeça de onde parou, e só
  quando chegaram os bytes de que o candidato precisa (sem isso, segurar 16 MiB
  em segmentos de 1460 bytes refaria a varredura milhares de vezes).
  Sem candidato, guarda só os últimos 64 bytes (até 2026-11-07, o buffer crescia
  até 16 MiB num fluxo midstream que não era syslog, revarrido a cada segmento).
  Tudo o que foi pulado é contado (`tcp_skipped_bytes`, `tcp_resyncs`), e o
  quadro perdido também (`frames_lost`).
- Quadro acima de 16 MiB não é quadro (ressincroniza); linha LF acima disso é
  cortada (`tcp_long_lines`).

### 6.7 O que não é reproduzido

- TLS capturado (porta 6514) — o conteúdo é cifrado. Fica em
  `tcp_streams_not_syslog`.
- O IP de origem no socket (§1.2): o servidor vê o endereço do injetor. Com
  `--pcap-flows` e `--proxy-protocol`, cada sessão anuncia a origem e o
  destino capturados do seu fluxo (§6.9).
- Direções de servidor e mensagens de outros protocolos na mesma porta.

### 6.8 Duplicatas do `-i any` (`--pcap-dedup`)

No Linux, `tcpdump -i any` captura um pacote em **cada** interface que ele
atravessa (a veth de um container, a ponte `docker0`, a `eth0`), às vezes com o
destino ou a origem reescritos pelo NAT entre uma cópia e outra; o mesmo vale
para um pcapng gravado em várias interfaces. Sem tratamento, o datagrama UDP
saía duas ou três vezes, e uma conexão TCP com NAT virava duas direções
diferentes (quádruplas diferentes), cada uma reproduzida inteira.

A regra (`--pcap-dedup auto`, o padrão, é `on`; `off` desliga):

- **Por pacote**, depois da remontagem IP e antes da remontagem TCP: a chave é
  protocolo, IP ID, porta de origem, sequência, confirmação e flags TCP,
  tamanho no fio e FNV-1a do conteúdo — a mesma chave do SI. Endereços e
  porta de destino ficam fora, porque o NAT os troca entre as cópias; o IP ID
  e o ISN o NAT não troca. A **interface** de um pacote é a do pcapng e a que
  o enlace nomeia: no SLL2 o índice **e, desde 2026-11-07, o tipo do pacote**
  (enviado, para nós, ...: no loopback o mesmo pacote aparece duas vezes na
  mesma interface, uma por sentido; regra do GI), no SLL o tipo e o endereço
  de enlace, no PKTAP o `pth_ifname`. Um pacote cuja chave é igual à de um pacote
  **original** visto há até **50 ms** de captura numa **outra interface** é
  cópia e é descartado (`pcap_duplicates`). A pergunta é feita depois do filtro
  de `--pcap-port`, para que a cópia que o filtro recusa (o NAT pode trocar a
  porta) não esconda a que ele aceita.
- Cada original absorve **no máximo uma cópia por interface**: o mesmo texto
  enviado duas vezes pelo emissor (duas mensagens legítimas iguais, até com o
  IP ID 0 do Linux com DF) aparece duas vezes em cada interface e sai duas
  vezes. Na mesma interface nunca há cópia: num pcap Ethernet de uma interface
  só a regra não descarta nada — por isso `auto` é `on` (no SI, `auto` decide
  pelo enlace; aqui a exigência de outra interface faz o mesmo papel).
- No TCP com NAT, todos os segmentos da cópia caem, e a direção copiada nem
  abre; sem NAT, as cópias tinham a mesma quádrupla e caíam como retransmissão
  (`tcp_retransmitted_bytes`), agora caem antes.
- A janela de 50 ms cobre a passagem do pacote entre as interfaces do mesmo
  host (microssegundos) com folga para um tcpdump carregado; ela guarda no
  máximo 65 536 originais (os mais antigos saem primeiro).

Prova real (TESTING §4): `tcpdump -i any` no host Docker, 300 datagramas UDP e
3 conexões TCP entre dois containers na mesma ponte: 1804 pacotes, cada um
capturado duas vezes; sem a regra, 900 mensagens (os 300 UDP em dobro); com
ela, 600, e 902 cópias descartadas.

O que **não** é deduplicado: duas mensagens iguais de emissores diferentes que
chegam em interfaces diferentes dentro de 50 ms com a mesma porta de origem e
o mesmo IP ID são tomadas por cópia (raro; desligue com `--pcap-dedup off`),
e um pcap clássico de cooked v1 em que as duas cópias tenham o mesmo tipo de
pacote e o mesmo endereço de enlace não é deduplicado (o cooked v1 não grava a
interface; o v2, padrão do tcpdump desde 4.99, grava).

### 6.9 Uma sessão por fluxo original (`--pcap-flows`)

Por padrão as mensagens da captura são distribuídas pelas `--concurrency`
sessões, sem relação com as conexões originais. Com `--pcap-flows`, cada
**fluxo original** tem a sua sessão RELP, como os emissores eram:

- Fluxo é uma direção TCP cliente → servidor (uma conexão; um SYN novo na mesma
  quádrupla é outro fluxo) ou um par origem → destino de datagramas UDP.
- As mensagens de um fluxo saem **em ordem de captura, na sessão dele**; fluxos
  diferentes correm em paralelo. O ritmo (§6.5) continua global.
- Com `--proxy-protocol v1|v2`, o cabeçalho de cada sessão anuncia a **origem e
  o destino capturados** do fluxo. Assim o servidor registra o `src-ip` de
  cada emissor original (ele precisa confiar no injetor como balanceador, SPEC
  do servidor §4.6). `--proxy-src`/`--proxy-dst` com `--pcap-flows` é
  **recusado** (exit 2; alinhamento 8(a) de 2026-11-07, a regra do
  `go-gelf-injector`, igual nos quatro): até então eles valiam para todas as
  sessões e desfaziam o que a flag existe para fazer.
- Quando a direção TCP termina na captura (FIN ou RST), a sessão do fluxo é
  encerrada depois da última mensagem dele (espera as respostas e manda
  `close`, §4.5); o fluxo UDP e a direção sem fim vão até o fim da execução.
- `--concurrency` passa a ser o **teto de sessões abertas ao mesmo tempo**; sem
  ele, o teto é 1024. Com o teto cheio, um fluxo que precisa de sessão espera, e
  uma sessão **ociosa** (fluxo sem mensagem na fila) que vê alguém esperando é
  encerrada para ceder a vaga; ela reabre quando o fluxo tiver outra mensagem.
  Assim nenhum fluxo espera para sempre por outro que não tem o que enviar.
  A sessão ociosa **espera** a fila dela ou o aviso de que alguém começou a
  esperar uma vaga, o que vier primeiro (achado 1 da revisão de 2026-11-07,
  reproduzido): até então ela só olhava a espera no instante em que a fila
  esvaziava e depois bloqueava na fila segurando a vaga; o fluxo novo esperava
  a vaga, o produtor enchia a fila dele (4 posições) e parava — a execução
  travava para sempre com mais fluxos ativos que o teto.
- Quebra, retransmissão e desfecho (§4.4) são por sessão, como sem a opção: o
  que um fluxo deixou sem resposta é reenviado na sessão seguinte **dele**.
- O relatório traz `flows` em `info`; as conexões abertas contam as sessões.

---

## 7. Escalonamento

- `--count` e `--duration` limitam a execução; o que vier primeiro. Sem
  `--count`, `--duration` sozinho não limita a contagem (até 2026-10-31 o padrão
  de 1 mensagem valia também aqui: defeito dos três injetores). Uma fonte
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
(erros de discagem, quebras e recusas agrupados pelo texto), `source`
(contadores da fonte, §8.4) e, com `--verify-*`, `verify` (§8.5).

### 8.2 Desfechos

| Desfecho | Sucesso | Significado |
|---|---|---|
| `acked` | sim | `rsp` com `200` |
| `rejected` | não | `rsp` com outro código; o texto vai aos eventos (`rejected: <texto>`) |
| `unanswered` | não | nenhuma resposta em `1 + --retries` sessões, ou o prazo do fim acabou |
| `not_sent` | não | nenhuma sessão abriu até o fim (`--drain-timeout`) ou interrompido |

A latência é um histograma log-linear (32 sub-baldes por potência de dois, erro
de ~3%), dos sucessos, mesclado dos shards no fim. Os eventos trazem
`retransmitted` (reenvios), `conn_broken: <motivo>` (inclusive
`server sent serverclose`, `no answer within the ack timeout`,
`session ended after a rejected message`), `dial_error: <motivo>` (inclusive
`relp open refused: ...`) e `rejected: <texto>`.

**O texto das recusas** (pendência 6): `rejected: <texto>` conta cada `rsp` que
não é `200` pelo texto da resposta — a primeira linha dos dados, inclusive o
código (`rejected: 500 message refused by the destination`). É o que o operador
precisa para saber *por que* o servidor recusou. A cardinalidade é limitada,
porque o texto vem do servidor: cortado em 120 bytes (sem partir um caractere
UTF-8), bytes de controle trocados por `?`, e no máximo **32** textos
distintos; o 33º em diante conta em `rejected: (other texts)`. A soma dos
`rejected: …` é igual ao desfecho `rejected`.

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
`messages_replayed`. Desde 2026-11-07: `pcap_ip_datagrams_snaplen` (§6.3),
`pcap_duplicates` (§6.8), `tcp_closed_partial_frames` (§6.6). São a prova de que o
replay entregou tudo o que a captura tinha — ou do que faltou nela.

### 8.5 Conferência no destino (`--verify-url`, `--verify-sqlite`)

Pendência 1, decisão 20. Depois do envio (e do `close` das sessões), o injetor
lê o destino em que o servidor grava e confere as mensagens **desta execução**.

**O marcador.** Só o gerador e um `--template` com `{seq}` **e** `{run}` têm o
que contar: o modelo vira uma expressão regular (os literais escapados, `{seq}`
→ `([0-9]+)`, `{run}` → o `--run` literal, `{host}` → `--hostname`, os demais
marcadores pelo que podem produzir), procurada no `message` do payload e, se
não estiver lá, no `raw` (decodificado se `raw-encoding=base64`). Com `--format
5424`/`3164` o modelo é o `message`; com `raw` ele é a mensagem inteira, e o
`raw` o contém. Um envelope que não casa não é desta execução e é ignorado.
`--file` e `--pcap` não têm marcador: `--verify-*` com eles é erro de uso.
Use um `--run` novo a cada execução (o padrão é aleatório). Nos streams (NATS,
Redis) e no SQLite, lidos depois da última entrada da sonda, uma execução
anterior com o mesmo `--run` não é lida; no PostgreSQL, lido por janela de
tempo, ela é **recusada** antes do envio (abaixo).

**Os destinos** (o formato de cada um é o da SPEC do `go-relplog-server` §3, o
mesmo nos cortes `go-relplog-pg-server` e `go-relplog-nats-server`):

| `--verify-*` | Lê | Parâmetros da URL (do injetor, retirados antes de conectar) |
|---|---|---|
| `nats://`, `tls://` | o stream JetStream (§3.8, §10.7 do servidor) por um consumidor ordenado efêmero, filtrado em `loghub.<space>.syslog` e `loghub.<space>.syslog.>`, a partir da sequência seguinte à última do stream na sonda de antes do envio; o JLH em `Data`. Usuário, senha e token vão na URL, como na `LOGHUB_EB_URL` | `stream` (padrão `LOGHUB_SYSLOG`), `space` (padrão `default`), `ca` (arquivo PEM; `tls://` sem ele usa as CAs do sistema) |
| `redis://`, `rediss://`, `valkey://`, `valkeys://` | o stream (§3.6 do servidor) por `XRANGE`, a partir do id seguinte à última entrada na sonda de antes do envio (inteiro, se o stream ainda não existia), 1000 por vez; o JLH no campo `e`. `valkey://` é `redis://` e `valkeys://` é `rediss://` (o Valkey fala o mesmo protocolo); o banco vem do caminho (`/1`) | `stream` (padrão `loghub.<space>.syslog`), `space` (padrão `default`), `ca`, `db` |
| `postgres://`, `postgresql://` | a fila `loghub.inbox` (§3.7.1 do servidor): `payload->>'message'` e `payload->>'raw'` das linhas `type='syslog'` com `received_at` desde o início da execução menos 10 min e o `--run` no texto | `table` (padrão `loghub.inbox`, o `sink.postgres.table`); os demais (`sslmode`, `sslrootcert`, …) vão ao `pgx` |
| `--verify-sqlite <arquivo>` | a tabela `syslog_events` do `events.db` (§3.7, §10.4 do servidor), só leitura, pelo `rowid`, a partir do maior `rowid` da sonda de antes do envio; o JLH em `env` | — |

**Lista NATS e segredos** (achado 3 da revisão de 2026-11-07): a URL NATS pode
ser uma lista com vírgula (`nats://a:4222,tls://b:4222`), como a
`sink.nats.urls` do servidor; cada elemento é analisado com `url.Parse` antes
de chegar ao `nats.go`, e um que não analisa é erro de uso com mensagem
genérica. Os parâmetros do injetor (`stream`, `space`, `ca`) podem estar em
qualquer elemento, uma vez só, e são retirados de todos. Até então a lista ia
inteira ao `url.Parse`: o destino mostrava `nats://a:4222,nats:` (achado 12)
e um elemento malformado chegava ao `nats.go`, que repetia o texto dele
(senha incluída) no erro — que ia ao relatório. Nenhum texto de `*url.Error`
é repassado, e o usuário, a senha e o token de cada elemento são apagados de
qualquer erro do cliente, no stderr e em `verify.error`. O destino mostra os
hosts de todos os elementos (`nats://a:4222,b:4222`).

**CA privada**: `--verify-tls-ca <arquivo>` (portado do GI em 2026-11-07,
achado 7, igual nos quatro injetores) ou o parâmetro `ca=` da URL, um ou
outro (os dois juntos são erro de uso); vale para `tls://`, `rediss://` e
`valkeys://`. No PostgreSQL a CA vai em `sslrootcert=`, lido pelo `pgx`.

**Leitura NATS** (achado 2, reproduzido): uma leitura termina quando o lote vem
vazio, quando a mensagem lida diz `NumPending = 0`, ou depois de 2 s lendo.
Num stream com tráfego alheio constante no mesmo subject o lote nunca vinha
vazio, a leitura não voltava, e a espera acabava em código 4.

**Ponto de partida dos streams** (regra de 2026-11-07, portada do
`go-gelf-injector`): a sonda que roda **antes do envio** anota a última
sequência do stream NATS (`StreamInfo`) e o último ID do stream Redis
(`XREVRANGE … COUNT 1`); a conferência lê depois deles. Tudo da execução vem
depois, sem relógio e sem ler nada do passado. Antes, NATS e Redis também
usavam a janela de 10 min, e num stream movimentado ela relia tudo desses 10
min a cada conferência (o GI achou ~250 000 mensagens, e a leitura passava do
`--verify-timeout`). Limite: um stream NATS apagado e recriado no meio do
envio recomeça as sequências, e o que for gravado abaixo da anotada não é
lido (conta como perdido). O SQLite também: a sonda anota o maior `rowid` da
tabela, e a conferência lê depois dele (até 2026-11-07 lia desde o `rowid` 0:
um `--run` repetido achava as linhas da execução anterior e passava, achado
4). A janela de 10 min antes do início fica para o PostgreSQL (a fila não tem
ordem de chegada) e cobre relógios desencontrados entre o host do injetor e o
do destino; é a folga dos quatro injetores (achado 8).

**`--run` já presente no PostgreSQL** (decisão do integrador de 2026-11-07,
achado 4): a sonda faz a leitura da conferência uma vez; se a janela já tem
qualquer envelope com o marcador da execução, a execução é recusada com
**código 2** ("`--run` já presente no destino; use outro"), sem relatório, e
nada é enviado. Antes ela passava com as linhas da anterior enquanto a atual
podia perder tudo. O que os leitores **não** veem: uma `loghub.inbox`
que um consumidor já esvaziou (a fila é para ser consumida: confira antes de
ligá-lo, ou num banco de teste), um arquivo SQLite já girado
(`sink.sqlite.rotate`: confira o arquivo do período) e um NATS em
`sink.nats.mode=core` (não há stream para ler). Um `space` diferente de
`default` precisa de `space=`.

**A espera.** O destino é assíncrono: o `200` do servidor sai quando a
mensagem está no ring, e o sink grava depois. O injetor lê o destino a cada
250 ms até que **toda mensagem confirmada** (`acked`) tenha chegado e nada novo
da execução apareça por 1 s, ou até `--verify-timeout` (padrão 30 s). Esse
segundo de calma é lido pelo menos quatro vezes depois de a conta fechar: é a
"leitura a mais" que pega a duplicata atrasada (o SI achou, no PostgreSQL, uma
duplicata da execução que só uma leitura final via). Leitores incrementais
(NATS, Redis, SQLite) continuam de onde pararam; o PostgreSQL relê a janela a
cada vez, num contador **novo** que só substitui a conta anterior se a leitura
terminar (até 2026-11-07 a conta era zerada antes da leitura, e uma leitura que
falhava no meio deixava um falso "perdido", achado 5). O consumidor ordenado
do NATS se recria sozinho (lacuna, heartbeat perdido) e então entrega de novo
desde o começo: o leitor guarda a última sequência do stream contada e pula o
que já viu (o SI mediu 536 000 duplicatas falsas antes dessa regra). Um erro
de leitura é retentado até o prazo; se a **última** leitura antes do prazo
falhou, a conta não é a do destino: `unreadable`, código 4. Um stream Redis
que nunca aparece é tudo perdido (código 1), nos quatro injetores. A leitura a
mais depois de a conta fechar é provada por `TestCheckExtraRead`: sem ela, a
cópia que chega logo depois não é contada.

**Antes do envio**, o injetor abre o destino e lê uma vez (PING no Redis, a
consulta com `LIMIT 0` no PostgreSQL, o consumidor criado no NATS, a tabela no
SQLite), sob `--timeout`. Se falha, **nada é enviado**: o relatório sai com
`verify.error` e o código é `4` — uma conferência que não pode rodar torna a
execução inútil. Se o destino some depois e a última leitura falha, também
`4`.

**A conta**, por `seq` de 1 até a última mensagem que saiu do produtor. Os
nomes e significados são os do SI, nos quatro injetores (decisão do
integrador de 2026-11-07, achado 6); os campos próprios do RI estão no fim:

| Campo | Significado |
|---|---|
| `expected` | os **sucessos**: as mensagens confirmadas com `200` |
| `found` | delas, as encontradas no destino |
| `lost` | **confirmadas e ausentes** (`expected − found`): perda depois do `200` — o que a conferência existe para pegar (código `1`) |
| `duplicated` | **cópias a mais**: somadas sobre todas as `seq` encontradas, cada cópia além da primeira (uma `seq` gravada três vezes conta 2; código `1`, salvo `--verify-allow-duplicates`) |
| `unexpected` | **registros** da execução cuja `seq` não é um sucesso: não confirmada mas presente (a resposta se perdeu na sessão que caiu, ou o servidor gravou e recusou) ou fora de 1..última (outra execução com o mesmo `--run`). Informativo: **não** muda o código |
| `missing_unacked` | (do RI) não confirmadas e ausentes (recusadas, sem resposta, não enviadas): o esperado |
| `duplicated_same_uuid` | (do RI) das cópias a mais, as que repetem o `uuid` da primeira (reentrega do sink do servidor, o mesmo envelope); as de `uuid` diferente são retransmissões RELP (§4.4: o mesmo `seq` recebido duas vezes pelo servidor) |
| `entries_read` | (do RI) envelopes lidos do destino (da execução ou não) |

Até 2026-11-07 `expected` era o que o produtor soltou e havia `acked` (hoje
`expected`), `duplicated` contava `seq` com mais de uma cópia (o número de
cópias a mais era `extra_copies`, removido; `extra_copies_same_uuid` virou
`duplicated_same_uuid`), `found` contava toda `seq` presente, e
`found_unacked` (removido) é a parte de `unexpected` dentro de 1..última. As
cópias são contadas num vetor de 2 bytes por `seq`, saturando em 65 535.
| `lost_seqs` | até 20 faixas de `seq` perdidas (`"2062-2081"`, `"77"`), para procurar no log do servidor |
| `complete` | a conta fechou (toda confirmada encontrada e 1 s sem novidade) antes do prazo |
| `waited`, `error`, `unreadable` | quanto esperou (s); o último erro de leitura, sem a URL; o destino nunca pôde ser lido (código `4`) |

O relatório de texto mostra uma linha `verify:` com o destino (só esquema,
host e porta, ou o arquivo: nunca usuário, senha ou parâmetros) e uma linha por
campo diferente de zero. **Duplicata não é necessariamente defeito** (§4.4):
uma sessão quebrada retransmite o que não teve resposta, e o RELP não tem id de
mensagem; `--verify-allow-duplicates` aceita isso num teste de quebra, e a
conta continua no relatório.

---

## 9. Pendências conhecidas

1. *Corrigido em 2026-11-07:* conferência no destino (`--verify-url`,
   `--verify-sqlite`, código 4); regra em §8.5, decisão 20.
2. *Corrigido em 2026-11-07:* deduplicação das cópias do `-i any`
   (`--pcap-dedup`); regra em §6.8, provada com um `tcpdump -i any` real.
3. *Corrigido em 2026-11-07:* uma sessão por fluxo original com o PROXY da
   origem capturada (`--pcap-flows`); regra em §6.9.
4. *Corrigido em 2026-11-07 (o que é possível):* fragmento cortado pelo snaplen
   condena o datagrama na hora e conta em `pcap_ip_datagrams_snaplen`; os
   bytes cortados não estão no arquivo e não têm conserto; regra em §6.3.
5. *Corrigido em 2026-11-07:* candidato de ressincronização acima de 64 KiB
   segura a decisão até o teto de quadro, com memória e custo limitados; regra
   em §6.6.
6. *Corrigido em 2026-11-07:* o texto das recusas vai aos eventos, agrupado e
   com cardinalidade limitada; regra em §8.2.
7. **Prova real do `-i any` com NAT e do PKTAP** (§6.8, §6.2): o `-i any` foi
   provado com um tcpdump real num host Docker (duas cópias por pacote, sem
   NAT); o caminho **com NAT** (DNAT de uma porta publicada) só com captura
   sintética, porque a porta publicada do Docker Desktop fica no 127.0.0.1 do
   Mac e o tráfego entre containers não passa pelo DNAT; o PKTAP corrigido só
   com cabeçalhos sintéticos (o tcpdump do macOS precisa de root no host de
   teste). Fica aberta até essas duas provas.
8. *Corrigido em 2026-11-07 (porte entre injetores):* último quadro de direção
   fechada lido como no EOF do servidor (§6.4, §6.6, do GI); tipo do pacote do
   SLL2 no ponto de captura do `-i any` (§6.8, do GI); NATS e Redis/Valkey
   conferidos depois da última entrada na sonda (§8.5, do GI).
9. *Corrigido em 2026-11-07 (revisão independente dos injetores):*
   `--pcap-flows` que travava com mais fluxos que o teto (§6.9);
   `--proxy-src`/`--proxy-dst` recusados com `--pcap-flows` (§6.9); cópia
   tardia de fragmento ignorada (§6.3); fila de segmentos TCP num heap e com
   teto que conta todo segmento (§6.4); leitura NATS que termina sob tráfego
   alheio, lista NATS e segredos fora dos erros e do relatório, `--run` já
   presente recusado (PostgreSQL) e SQLite lido depois da sonda, contador novo
   por leitura inteira, campos do relatório alinhados ao SI,
   `--verify-tls-ca`, última leitura com erro = 4 (§3.2, §3.3, §8.5).
