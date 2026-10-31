# go-gelf-injector — Especificação v1

Ferramenta cliente que **injeta mensagens GELF** num servidor — em especial os
da família GELF do ecossistema LogHub (`go-gelf-server`, `go-gelf-pg-server`,
`go-gelf-nats-server`), mas também um Graylog ou qualquer receptor GELF — para
testar o comportamento dele ou colocá-lo sob carga. Criada em 2026-10-31; as
decisões que a definiram estão em `docs/DECISOES.md`, citadas aqui como
"decisão N" (as herdadas dos injetores) e "decisão Gn" (as do GELF).

Esta especificação é a fonte da verdade: discrepância entre código e spec é bug
no código. Para mudar comportamento, primeiro a spec (com o porquê), depois o
código, depois o teste.

---

## 1. Escopo

### 1.1 Objetivos

- Enviar documentos GELF por **UDP** (com chunking e compressão gzip/zlib),
  **TCP** e **TLS** (moldura `\0`) e **HTTP/HTTPS** (`POST /gelf`, keep-alive,
  pipelining, `Content-Encoding`).
- Teste **funcional** (poucas mensagens, o que aconteceu com cada uma) e de
  **carga** (taxa alvo, concorrência, duração, vazão e latência medidas).
- Quatro fontes (decisão 3): o **gerador** com `_seq`, um **modelo** com
  marcadores, as linhas de um **arquivo ou do stdin** e o **replay de captura
  do tcpdump** (pcap ou pcapng) com GELF de UDP (chunks remontados,
  descomprimidos), TCP e HTTP.
- Relatório honesto: cada mensagem tem **um** desfecho (§8.2), e o código de
  saída diz se alguma falhou.

### 1.2 Não-objetivos (v1)

- **Conferir a entrega no destino** (NATS, PostgreSQL...) (decisão 4;
  pendência 1). No UDP/TCP o relatório é "o kernel aceitou"; no HTTP é a
  resposta do servidor (`202` = o ring guardou).
- Captura ao vivo; reproduzir o IP de origem (só anunciável pelo PROXY); TLS
  capturado (cifrado).
- HTTP/2 e socket unix (decisão G1; pendência 4).
- Validar o JSON do usuário: arquivo e modelo saem como estão (decisão G8).

### 1.3 Família

Copiado do `go-syslog-injector` (SI: `pcap`, `report`, `transport/proxy`) e do
`go-webhook-injector` (WI: `sched` com devolução de mensagens, `tmpl` com a
regra de chaves de JSON, `httpx`, o cliente HTTP/1.1). **Nada é importado**
entre módulos (decisão 1). Uma correção num pacote comum vale para os irmãos:
registre no `CHANGELOG.md` da raiz o que ficou "a portar".

---

## 2. Arquitetura

```
fonte ──> produtor ──> fila (4 × concorrência) ──> N workers ──> N conexões ──> servidor
 gen        ritmo          canal                     cada um       UDP/TCP/TLS/HTTP(S)
 template   (--rate,                                 com a sua
 file       --pcap-timing,                           conexão
 pcap       --count, --duration)          desfechos ──> relatório (shards por worker)
```

- Uma goroutine produtora pede à fonte o documento `seq` (de 1 em diante) e o
  solta na fila no instante planejado (§7).
- N workers (`--concurrency`), cada um dono de **uma** conexão. Conexão
  quebrada é fechada e o worker disca de novo (100 ms dobrando até 2 s). Uma
  conexão HTTP pode **devolver** mensagens que o servidor garantidamente não
  processou (§4.4): o worker as manda primeiro na próxima conexão.
- O documento viaja **sem compressão e sem moldura** da fonte até a conexão; o
  transporte comprime, fatia em chunks ou enquadra.
- Contrato de toda conexão (`sched.Conn`): o callback de desfecho é chamado
  **exatamente uma vez** por mensagem dada a `Send` — na hora, no `Flush`, na
  resposta ou no `Close` — ou a mensagem é devolvida (`Unsent`) sem desfecho,
  para outra conexão. É o que garante `mensagens = ok + falhas`.

### 2.1 Mapa do código

```
cmd/go-gelf-injector/main.go   só chama internal/app
internal/
  app/        flags, validação, montagem, sinais, relatório, código de saída
  tmpl/       modelos com marcadores (regra de chaves do WI)        [lógica pura]
  gelf/       compressão e detecção, chunks, remontagem             [fuzz]
  extract/    GELF em datagramas, fluxos \0 e corpos HTTP;
              detecção e ressincronização                           [fuzz]
  httpx/      requisições HTTP/1.x num fluxo (do WI)                 [fuzz]
  pcap/       leitor pcap/pcapng, enlaces, IP, fragmentos, TCP (do SI) [fuzz]
  pcap/pcaptest/  escritor de capturas para os testes
  source/     gerador, modelo, arquivo/stdin, pcap
  sched/      produtor, workers, ritmo, devolução, interrupção (do WI)
  transport/  UDP (chunks), TCP/TLS (\0), HTTP/1.1 (do WI), PROXY (do SI)
  report/     contadores, histograma, texto/JSON, progresso
  gelftest/   receptores GELF estritos e independentes, para os testes
```

---

## 3. Linha de comando

```
go-gelf-injector [flags]
```

### 3.1 Flags

| Flag | Padrão | Significado |
|---|---|---|
| `--transport` | `tcp` | `udp`, `tcp`, `tls`, `http` ou `https` |
| `--target` | `127.0.0.1:` + `12201` (udp, tcp), `12203` (tls), `12202` (http), `12204` (https) | `host:porta` do servidor (decisão 7) |
| `--compress` | `gzip` no udp; `none` no resto | `none`, `gzip`, `zlib` (udp, http, https; recusado em tcp/tls); `deflate` é aceito como sinônimo de `zlib` (é o nome HTTP do formato zlib, `Content-Encoding: deflate`) |
| `--chunk-size` | `8192` | udp: maior datagrama, cabeçalho de chunk incluído, `13..65507` |
| `--timeout` | `10s` | discagem, handshake e escrita |
| `--tls-ca` / `--tls-cert` / `--tls-key` | — | CAs do servidor (padrão: do sistema); certificado e chave do cliente (mTLS) |
| `--tls-server-name` | host do alvo | nome conferido no certificado |
| `--tls-min-version` | `1.2` | `1.2` ou `1.3` |
| `--tls-insecure` | `false` | não verifica o certificado |
| `--proxy-protocol` | `off` | `v1` ou `v2` em tcp, tls, http, https (§4.5) |
| `--proxy-src` / `--proxy-dst` | os reais | `ip:porta` anunciados |
| `--http-path` | `/gelf` | caminho do `POST` |
| `--http-host` | o alvo | cabeçalho `Host` |
| `--pipeline` | `1` | requisições em voo por conexão HTTP |
| `--requests-per-conn` | `0` | fecha e reabre a conexão HTTP depois de N requisições |
| `--retries` | `0` | reenvios de `429`/`503` |
| `--retry-max-wait` | `5s` | maior espera de um reenvio (`>= 0`; `0` reenvia sem esperar) |
| `--response-timeout` | `30s` | espera por cada resposta HTTP |
| `--count` | 1; sem limite com `--duration`; a fonte inteira com `--file`/`--pcap` | mensagens; `0` = sem limite |
| `--duration` | `0` | para depois deste tempo |
| `--rate` | `0` | mensagens/s somando as conexões; `0` = o máximo |
| `--concurrency` | `1` | conexões em paralelo |
| `--drain-timeout` | `10s` | no fim, espera pelo que está na fila, pelas respostas e reenvios |
| `--template` | — | documento com marcadores (§5.2) |
| `--file` | — | um documento por linha (`-` = stdin) (§5.3) |
| `--pcap` | — | replay de captura (§6) |
| `--hostname` | o do host, sem domínio | `host` do gerador e `{host}` |
| `--level` | `6` | `level` do gerador, `0..7` |
| `--payload-size` | `0` | completa os documentos do gerador até N bytes (§5.1) |
| `--run` | 8 hex aleatórios | identificador da execução: 1 a 64 de `[A-Za-z0-9._:-]` (é um rótulo, e o gerador o põe sem escape numa string JSON) |
| `--loop` | `false` | recomeça `--file`/`--pcap` no fim |
| `--pcap-port` | — | portas de destino a reproduzir; dada, desliga a checagem de conteúdo |
| `--pcap-proto` | `udp,tcp` | protocolos da captura (`tcp` inclui o HTTP) |
| `--pcap-timing` | `asap` | `asap` ou `original` |
| `--pcap-speed` | `1` | fator de `original` |
| `--report` | `text` | `text` ou `json` no stdout |
| `--progress` | `auto` | intervalo da linha de progresso no stderr (duração Go); `0`/`off` desliga; outro valor é erro de uso |
| `--version` | | imprime a versão e sai |

### 3.2 Combinações recusadas (exit 2)

`--template`, `--file`, `--pcap` juntos; transporte desconhecido; `--target`
sem `host:porta`; `--compress` fora de `none|gzip|zlib|deflate`, ou diferente de `none`
em tcp/tls; `--chunk-size` fora de udp ou fora de `13..65507`; flags HTTP
(`--http-path`, `--http-host`, `--pipeline`, `--requests-per-conn`,
`--retries`, `--retry-max-wait`, `--response-timeout`) fora de http/https;
`--http-path` sem `/` inicial ou com espaço; `--tls-*` fora de tls/https;
`--proxy-protocol` em udp; `--proxy-src/--proxy-dst` sem `--proxy-protocol`;
`--level` fora de `0..7` ou com fonte que não é o gerador; `--payload-size`
com fonte que não é o gerador; `--loop` sem `--file`/`--pcap` ou com stdin;
`--pcap-port`/`--pcap-proto`/`--pcap-speed`/`--pcap-timing original` sem
`--pcap`; `--rate` com `original`; `--run` fora de `[A-Za-z0-9._:-]{1,64}`; `--progress`
que não é `auto`, `off` nem duração `>= 0`; marcador desconhecido; captura que
não é pcap/pcapng; argumento posicional. Tudo conferido **antes** do primeiro envio.

### 3.3 Códigos de saída

| Código | Quando |
|---|---|
| `0` | todo desfecho foi sucesso (`sent` ou HTTP `2xx`) |
| `1` | alguma falhou, ou a fonte deu erro no meio |
| `2` | uso |
| `3` | alvo inalcançável: houve mensagens e nenhuma conexão abriu |
| `130` | interrompido (SIGINT/SIGTERM) |

---

## 4. Transportes

### 4.1 UDP

Socket UDP **conectado**. A mensagem é comprimida (`--compress`, padrão gzip);
se o resultado cabe em `--chunk-size`, vai num datagrama; senão, em chunks GELF
(decisão G2):

```
0x1e 0x0f | id (8 bytes) | seq (1) | count (1) | pedaço
```

com `count = ceil(n / (chunk-size − 12))`, o mesmo id em todos, `seq` de 0 a
`count−1`, enviados em ordem. O id vem de um gerador pseudoaleatório por
conexão (é o que as bibliotecas GELF fazem; só precisa ser único entre as
mensagens que o receptor remonta da mesma porta de origem). `count > 128`:
desfecho `too_large`, **nada é enviado**.

Desfecho na hora, por mensagem: `sent` quando o kernel aceitou **todos** os
datagramas dela. Um datagrama recusado encerra a mensagem (o resto não é
enviado: o receptor nunca a completaria): `too_large` (`EMSGSIZE`), `nobufs`
(`ENOBUFS`/`EAGAIN`: o kernel local descartou; comum no macOS a taxas altas, e
use `--rate`), `refused` (`ECONNREFUSED`: ICMP de um datagrama **anterior**),
`write_error` (quebra o socket). Não há backpressure nem confirmação no UDP.

### 4.2 TCP e TLS

- Moldura: o documento seguido de **um** `\0` (decisão G5). Sem compressão
  (decisão G3). Um documento que contém `\0` é recusado, desfecho `nul_byte`,
  sem ser enviado.
- Buffer de 64 KiB escrito quando enche ou quando o worker não tem a próxima
  pronta; o desfecho sai na escrita, e a latência é do `Send` até ela. Escrita
  inteira: `sent` para todas as mensagens do buffer. Escrita **parcial** (o
  `--timeout` estourou no meio, sob backpressure): `sent` para as mensagens cujo
  quadro, `\0` incluído, foi inteiro ao kernel (o mesmo critério), e
  `write_error` para as demais. O quadro cortado no meio chega truncado ao
  servidor, que o lê no EOF como documento malformado (DESIGN do G, decisão 12).
- Não há confirmação no GELF/TCP: uma goroutine por conexão só espera o EOF;
  se o servidor fechar, a próxima mensagem falha com `write_error` e o worker
  reconecta. O que foi escrito entre o fechamento e a percepção aparece como
  `sent` e pode ter se perdido — a cegueira de qualquer emissor GELF/TCP.
- Backpressure do servidor (para de ler) aparece como latência de escrita e,
  além de `--timeout`, `write_error`.
- TLS: `crypto/tls`, SNI e verificação pelo host do alvo (ou
  `--tls-server-name`), mTLS opcional.

### 4.3 HTTP e HTTPS

O cliente HTTP/1.1 do WI (sua SPEC §4.1, §4.3–§4.5), sobre `net.Conn` próprio:

- Um `POST --http-path` por documento, `Host` = `--http-host` ou o alvo,
  `Content-Type: application/json`, `Content-Length`. Com `--compress gzip`,
  corpo gzip e `Content-Encoding: gzip`; com `zlib`, corpo zlib e
  `Content-Encoding: deflate` (decisão G4). O corpo é comprimido uma vez; um
  reenvio leva os mesmos bytes.
- Keep-alive, uma conexão por worker, `TCP_NODELAY`; TLS com ALPN `http/1.1`.
- **Pipelining** (`--pipeline N`): até N requisições em voo; o buffer é
  empurrado antes de esperar uma vaga (sem isso, impasse). Uma goroutine
  leitora casa as respostas em ordem; o prazo de leitura só corre com
  requisição pendente.
- **Servidor sem keep-alive ou com teto por conexão.** A família G passa a ter
  keep-alive (HTTP/1.1 persistente, `http.idle_timeout`, teto opcional
  `http.max_requests_per_conn`, `Connection: close` no shutdown). Um servidor
  que fecha **toda** conexão (keep-alive desligado, teto 1, ou a família G
  antes dessa mudança) só tem uma requisição útil por conexão: a carga HTTP
  contra ele se faz com `--concurrency` (conexões em paralelo), e
  `--pipeline > 1` só desperdiça — as requisições escritas atrás da 1ª são
  descartadas pelo servidor e voltam ao worker (com 32, ~30× o tráfego).
  `--pipeline > 1` serve a servidores com keep-alive; com teto N por conexão,
  cada conexão perde no máximo o que estava em voo atrás da N-ésima.
- A vaga do pipeline só é liberada **depois** de a conexão ser marcada como
  fechando, quando a resposta traz `Connection: close`: o worker que esperava
  a vaga devolve a requisição sem escrevê-la (antes de 2026-10-31 ela ia a um
  socket que o servidor não lia mais).
- Desfecho = o código de status (`"202"`, `"503"`, ...); sucesso = `2xx`
  (decisão G6). Sem resposta: `timeout`, `conn_error` (quebrou durante a
  resposta), `no_response` (o servidor fechou com ela pendente: **pode** ter
  sido processada, então não é reenviada).
- `Connection: close` na resposta: o que foi escrito atrás dela não foi
  processado e **volta ao worker** (sem desfecho, sem duplicata, evento
  `resent on a new connection`); o mesmo para o que nunca chegou ao fio
  (evento `requeued unwritten (connection closing)`). Devolvida 5 vezes por
  conexões que não responderam nada, termina como `conn_error`.
- Fechamento **planejado** não é conexão quebrada (`connections.broken`): a
  escrita que falha (`broken pipe`, `reset`) depois de o servidor ter dito
  `Connection: close` — o pipelining escreve adiante das respostas — e o
  fechamento de uma conexão ociosa pelo servidor (o `idle_timeout` do
  keep-alive, ou o shutdown entre requisições) fazem o worker reabrir sem
  contar quebra. Uma requisição pendente quando o servidor fecha sem
  `Connection: close` continua `no_response` (pode ter sido processada).
- `--retries N`: `429` e `503` voltam depois de `Retry-After` (segundos ou
  data; sem ele 100 ms), limitado por `--retry-max-wait`, até N vezes; o
  desfecho final é o último status. `413` e os demais `4xx` são finais.
- `--requests-per-conn N`: a N-ésima sai com `Connection: close` e o worker
  reabre a conexão (sem contá-la como quebrada).

### 4.4 Devolução de mensagens

Só o HTTP devolve (`sched.Resender`): o worker reenvia na próxima conexão as
mensagens que a anterior garantiu não ter processado. UDP e TCP não
devolvem: não há como saber.

### 4.5 PROXY protocol

`--proxy-protocol v1|v2` escreve o cabeçalho do haproxy no início de cada
conexão tcp, tls, http ou https, **antes** do handshake TLS. Anuncia o par real
(local como origem, o discado como destino) ou `--proxy-src`/`--proxy-dst`.
Famílias diferentes saem como IPv6 (IPv4 mapeado). Serve para testar
`proxy.protocol`/`proxy.trusted` do servidor e atribuir outra origem.

---

## 5. Fontes

### 5.1 Gerador

O documento (decisão 9):

```json
{"version":"1.1","host":"<--hostname>","short_message":"seq=N run=R injector=go-gelf-injector","level":6,"_seq":N,"_run":"R"}
```

`level` vem de `--level`; `host` é escapado como string JSON uma vez no
início. `--payload-size N` insere, antes do `}` final, `,"_pad":"…"` com
caracteres `[a-z0-9]` **aleatórios** até o documento ter exatamente N bytes
(decisão G7); um documento já maior fica como está. O menor documento com
`_pad` tem 10 bytes a mais (`,"_pad":""`): N entre o tamanho do documento + 1 e
+ 9 não é alcançável, e o documento também fica como está. Aleatórios porque o gzip
de uma repetição cabe em qualquer datagrama: 20 000 bytes de `_pad` viram ~15
KB de gzip, 2 chunks de 8192.

### 5.2 Modelo

`--template` é o documento inteiro, compilado uma vez (marcador desconhecido é
erro de uso). Marcadores: `{seq}`, `{run}`, `{uuid}` (v4 novo a cada
mensagem), `{ts}` (RFC 3339 local com µs), `{unix}`, `{unixms}`, `{host}`,
`{pid}`, `{rand:N}` (`[a-z0-9]`, `1 ≤ N ≤ 65536`).

Regra de chaves do WI (decisão 19): `{` só abre marcador quando o que segue
até o próximo `}` parece um (`[a-z]+` ou `[a-z]+:[a-z0-9]+`); aí um nome
desconhecido é erro. Todo outro `{`, e todo `}`, é literal; `}}` fica `}}`
(JSON aninhado). `{{` é um `{` literal, para escrever o texto de um marcador.
Os valores não são escapados: o modelo é do usuário.

### 5.3 Arquivo e stdin

`--file`: um documento por linha, na ordem, **como está** (decisão G8): o LF
termina a linha, os `CR` finais saem, linhas só de espaços são puladas
(`file_empty_lines`), a última sem LF conta. Linha acima de 16 MiB é cortada
(`file_long_lines`). `--loop` relê do início (não com stdin); uma passada sem
documentos (arquivo vazio, só de linhas em branco, ou esvaziado entre passadas)
termina a fonte em vez de girar no vazio; o mesmo vale para uma passada de
`--pcap` sem documento.

---

## 6. Replay de captura (`--pcap`)

### 6.1 O que é reproduzido

Os documentos GELF de uma captura (`tcpdump -w`), **em ordem de captura**
(o instante de um documento é o do pacote que o completou), pelo transporte
escolhido: o transporte original é retirado — chunks remontados, compressão
desfeita, moldura `\0` ou HTTP retirada — e o escolhido aplica o seu (decisão
10). Uma captura UDP gzip pode sair em TCP, uma TCP em UDP chunked, um HTTP
em UDP.

- **Seleção estrita** (sem `--pcap-port`): o documento (descomprimido, sem
  espaços nas pontas) precisa começar com `{`; um fluxo TCP precisa começar
  com `{` (GELF) ou com uma linha de requisição HTTP/1.x, e uma requisição
  HTTP precisa ser `POST` para um caminho que é `/gelf` ou termina em `/gelf`
  (§6.5). Assim DNS, NTP, SSH, TLS, webhooks JSON e `_bulk` do Elasticsearch da
  mesma captura não viram GELF.
- **Com `--pcap-port`**: só o tráfego para essas portas de destino, sem a
  checagem de conteúdo (um quadro que não é objeto passa, como o servidor o
  receberia e marcaria malformado).
- `--pcap-proto` restringe a `udp` ou `tcp`.
- Documento acima de 16 MiB (descomprimido) é descartado (`docs_too_large`);
  o teto vale **dentro** do leitor de gzip/zlib (bomba de descompressão).

### 6.2 Formatos, enlaces, IP, remontagem TCP

Iguais ao SI (sua SPEC §6.2–§6.4): pcap e pcapng; Ethernet (802.1Q/ad),
Linux cooked v1/v2, loopback BSD, raw IPv4/IPv6, PKTAP; fragmentos IPv4/IPv6
remontados com limites; datagrama cortado pelo snaplen não é reproduzido
(`frames_lost`); TCP por direção em ordem de sequência, retransmissões e fora
de ordem tratados, buraco = lacuna, `tcp_midstream`, `tcp_unterminated`.

### 6.3 UDP

Decisão G9. Um datagrama que começa com `0x1e 0x0f` é chunk:

- chave (origem `ip:porta`, id); `count` de 1 a 128 e `seq < count`, senão
  `udp_chunks_invalid`; `count` diferente do primeiro chunk do id, inválido;
- chunk repetido ignorado (`udp_chunks_duplicate`), inclusive a cópia
  **idêntica** de um chunk de uma mensagem já completa nos últimos 5 s (o
  `-i any` vê o pacote duas vezes; cada chunk de uma mensagem completa é
  lembrado por um hash do datagrama). Um chunk **diferente** com o id de uma
  mensagem completa há menos de 5 s é de uma mensagem nova de um emissor que
  reusou o id, e é remontado (decisão de 2026-10-31: antes, a 2ª mensagem
  inteira era contada como duplicata);
- prazo de **5 s de tempo de captura** desde o primeiro chunk, conferido
  também no chunk que completaria a mensagem (não só na varredura, que roda uma
  vez por segundo de captura); vencido, a mensagem é descartada
  (`udp_chunked_incomplete`, `udp_chunks_dropped`) e o chunk tardio começa uma
  nova; no fim da passada, as abertas também;
- tetos: 65 536 mensagens abertas e 64 MiB em remontagem
  (`udp_chunks_overflow`); a memória nunca é reservada pelo `count`
  declarado além de 128 ponteiros.

Completo (ou datagrama sem magic), o payload é descomprimido pelos primeiros
bytes: gzip `1f 8b`, zlib `78 xx` com `(b0<<8|b1) % 31 == 0` (método 8), senão
JSON cru. Falha de descompressão: `decompress_errors`.

### 6.4 TCP: GELF com `\0`

Decisão G10. Uma direção decide o protocolo uma vez, nos primeiros bytes
depois de espaços e `\0`:

| Primeiro byte significativo | Direção |
|---|---|
| `{` | GELF, moldura `\0` |
| linha de requisição HTTP/1.x | HTTP (§6.5) |
| resposta HTTP, prefácio h2c | ignorada (direção de servidor) |
| outra coisa | ignorada (estrito) ou GELF (`--pcap-port`) |

- Quadros terminam no `\0`; espaços nas pontas saem; quadro vazio não é
  documento (o servidor ignora `\0` e espaços entre mensagens). Estrito: quadro
  que não começa com `{` é `tcp_frames_not_gelf`.
- Quadro acima de 16 MiB: `docs_too_large`, descartado até o próximo `\0`
  sem acumular.
- **Fim da direção** (decisão G12): o quadro final sem `\0` é documento
  (estrito) só se começar com `{` e terminar com `}`; senão `frames_lost`.
- **Lacuna**: o quadro atingido é perdido (`frames_lost`), e os bytes são
  pulados até o próximo `\0` (`tcp_resyncs`, `tcp_skipped_bytes`).
- **Midstream** (e lacuna antes da decisão): varre byte a byte; a primeira
  prova vence — um `{` cujos bytes anteriores, até um `\0`, são só espaços
  (GELF: o documento começa ali), ou o fim de uma linha de requisição,
  ` HTTP/1.` + dígito + `CR` ou `LF` (HTTP: o extrator do WI ressincroniza na
  linha de requisição dentro do que foi guardado). O fim de linha é exigido
  porque ` HTTP/1.1` sozinho aparece dentro de documentos GELF de logs de
  acesso (`"GET /x HTTP/1.1" 200`) e viraria a direção para HTTP, perdendo
  todos os documentos seguintes; `CR`/`LF` crus nunca aparecem numa string
  JSON (correção da revisão de 2026-10-31). As duas
  são decididas num byte, sem olhar adiante: a escolha não depende da divisão
  em segmentos (`FuzzStream`). O que é guardado para o HTTP é zerado a cada 1
  MiB por posição de byte, pelo mesmo motivo. O primeiro documento de uma
  direção midstream que começa exatamente no primeiro byte capturado é
  perdido: sem o `\0` anterior, não é comprovado.

### 6.5 TCP: HTTP

Decisão G11. O extrator `httpx` do WI (sua SPEC §6.6: pipelining,
`Content-Length` ou `chunked`, ressincronização na próxima linha de
requisição) entrega cada requisição; o corpo de um `POST` não vazio, decodificado
por `Content-Encoding` (`gzip`, `x-gzip`, `deflate` = zlib, e deflate cru se
o zlib falhar) ou pelos primeiros bytes, é o documento (`http_messages`).
Outro método, corpo vazio, e no modo estrito um caminho que não é `/gelf` nem
termina em `/gelf` (sem a query; forma absoluta aceita) ou um corpo que não é
GELF: `http_requests_skipped`. Com `--pcap-port`, qualquer caminho (decisão de
2026-10-31: o caminho era ignorado, e qualquer API JSON da captura virava
GELF). Caminho e cabeçalhos capturados não são reproduzidos:
o documento sai pelo transporte escolhido.

### 6.6 Ritmo

Como no SI (§6.5 dele): `asap` (sob `--rate`) ou `original` a partir do
primeiro pacote da captura, dividido por `--pcap-speed`; `--loop` encadeia as
passadas 1 ms depois da última mensagem; atraso de mais de 100 ms é contado
(`behind_schedule`).

### 6.7 O que não é reproduzido

TLS/HTTPS capturado; o IP de origem (pendência 3); as duplicatas do `-i any`
de datagramas **sem** chunk (pendência 2; os chunks repetidos são ignorados);
HTTP/2; direções de servidor.

---

## 7. Escalonamento

Igual ao SI/WI:

- `--count` e `--duration` limitam a execução, o que vier primeiro. Sem
  `--count`, `--duration` sozinho **não** limita a contagem (o defeito de
  2026-10-31 dos três injetores nasce corrigido aqui). Fonte finita termina
  antes se acabar.
- `--rate R` agenda `seq` em `início + (seq−1)/R`, global; atraso não vira
  rajada.
- Fila de `4 × --concurrency`; o produtor bloqueia quando cheia; `--duration`
  interrompe a espera (a mensagem que não entrou não conta).
- **Fim**: o produtor para, a fila esvazia, cada worker escreve o buffer,
  espera respostas e reenvios até `--drain-timeout` e fecha; quem não conseguiu
  conexão até lá tem as mensagens `not_sent`.
- **Interrupção**: o primeiro SIGINT/SIGTERM para o produtor e desiste das
  discagens pendentes; o segundo imprime o relatório parcial e sai. Saída 130.

---

## 8. Relatório

### 8.1 Formas

`--report text` ou `json`, no stdout, só no fim (`report.Summary`): `info`
(injetor, execução, alvo, compressão, `chunk_size`, protocolo HTTP, PROXY,
fonte, plano), `start`, `elapsed_sec`, `messages`, `ok`, `failed`, `outcomes`,
`bytes` (dos documentos, sem compressão), `rate_msgs_per_sec`,
`rate_bytes_per_sec`, `latency` (µs, só dos sucessos), `connections`, `events`
e `source` (§8.4).

### 8.2 Desfechos

| Desfecho | Sucesso | Significado |
|---|---|---|
| `sent` | sim | UDP: o kernel aceitou todos os datagramas; TCP/TLS: o buffer foi escrito |
| `200`, `202`, `204`, ... (`2xx`) | sim | HTTP: a resposta do servidor |
| `503`, `429`, `413`, `400`, `404`, ... | não | HTTP: resposta final (depois dos reenvios) |
| `write_error` | não | a conexão quebrou antes da escrita |
| `too_large` | não | UDP: mais de 128 chunks, ou `EMSGSIZE` |
| `nobufs` | não | UDP: o kernel local descartou |
| `refused` | não | UDP: ICMP port unreachable |
| `nul_byte` | não | TCP/TLS: documento com `\0` |
| `timeout`, `conn_error`, `no_response`, `bad_response` | não | HTTP sem resposta (§4.3) |
| `not_sent` | não | nenhuma conexão até o fim, ou interrompido |

### 8.3 Progresso

Uma linha por intervalo no stderr; `auto` só num terminal.

### 8.4 Contadores da fonte

Arquivo: `file_lines`, `file_empty_lines`, `file_long_lines`, `file_passes`.

Captura: `pcap_*` (do SI), `udp_datagrams` (documentos de um datagrama),
`udp_not_gelf`, `udp_chunked_messages`, `udp_chunks`, `udp_chunks_invalid`,
`udp_chunks_duplicate`, `udp_chunks_overflow`, `udp_chunked_incomplete`,
`udp_chunks_dropped`, `compressed_gzip`/`_zlib`/`_deflate`,
`decompress_errors`, `docs_too_large`, `tcp_*` do remontador (do SI),
`tcp_streams_gelf`/`_http`/`_ignored`, `tcp_messages`, `tcp_frames_not_gelf`,
`tcp_resyncs`, `tcp_skipped_bytes`, `frames_lost`, `http_messages`,
`http_requests`, `http_requests_skipped`, `http_resyncs`,
`http_skipped_bytes`, `http_requests_lost`, `http_requests_too_large`,
`messages_replayed`. Renomear um é mudança incompatível.

---

## 9. Pendências conhecidas

1. **Conferência no destino** (decisão 4): ler NATS/PostgreSQL/SQLite e contar
   os `_seq` da execução.
2. **Duplicatas do `-i any`** de datagramas sem chunk (§6.7), como no SI.
3. **Uma conexão por fluxo original** no replay, com o PROXY da origem
   capturada.
4. **HTTP/2** (h2 via TLS, se o `go-gelf-server` o tiver) e **HTTP/1.1
   chunked** no envio: hoje sempre `Content-Length`.
5. **Último quadro TCP sem `\0`** que não termina em `}` (§6.4): um documento
   inteiro com espaço final interno ou malformado de propósito no fim de uma
   conexão é contado como perdido no modo estrito.
6. **Fragmentos IP numa captura real**: o código existe (SI), mas o loopback
   não fragmenta; os chunks GELF de 8192 sobre Ethernet (MTU 1500) só serão
   provados numa captura de rede real.
7. **Adaptação a servidor que fecha toda conexão** (sugestão da revisão de
   2026-10-31): o worker poderia notar que as primeiras conexões respondem
   `Connection: close` à 1ª requisição e passar a mandar uma por conexão. Não
   feito: a §4.3 manda usar `--concurrency` contra esse servidor, e o
   desperdício do `--pipeline > 1` fica visível nos eventos do relatório.
