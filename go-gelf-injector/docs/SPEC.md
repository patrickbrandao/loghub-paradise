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
- **Conferência no destino** (§10, decisão 4 revista em 2026-11-07): depois do
  envio, ler o destino do servidor (NATS JetStream, `loghub.inbox` do
  PostgreSQL, stream Redis/Valkey, `gelf_events` do SQLite) e contar, pelos
  `_seq` da execução, o que se perdeu e o que chegou repetido. Sem ela, no
  UDP/TCP o relatório é "o kernel aceitou" e no HTTP é a resposta do servidor
  (`202` = o ring guardou).

### 1.2 Não-objetivos (v1)

- Captura ao vivo; reproduzir o IP de origem (só anunciável pelo PROXY, que o
  replay por fluxo anuncia, §6.8); TLS capturado (cifrado).
- Socket unix (decisão G1).
- **HTTP/2** no envio (decisão G1, revista em 2026-11-07): nenhum servidor da
  família G o aceita — o `go-gelf-server` (sua SPEC §4.5), e os cortes que o
  copiam, anunciam só `http/1.1` no ALPN e deixam o `TLSNextProto` vazio, e
  não há h2c. Um h2 que o servidor recusa não testa nada; o envio é
  HTTP/1.1, com `Content-Length` ou `chunked` (§4.3).
- Validar o JSON do usuário: arquivo e modelo saem como estão (decisão G8).
- Conferir no destino documentos que não trazem os marcadores da execução
  (`--file`, `--pcap`): não há como reconhecê-los (§10.1).

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
- Com `--pcap-flows` (§6.8) a fila única dá lugar a **uma fila e um worker
  por fluxo original** da captura, cada um com a sua conexão; `--concurrency`
  passa a ser o teto de fluxos abertos ao mesmo tempo.
- Com `--verify-url`/`--verify-sqlite` (§10), depois do fim do envio o
  conferidor lê o destino até achar toda mensagem enviada com sucesso ou até
  `--verify-timeout`.
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
  verify/     conferência no destino: NATS, PostgreSQL, Redis/Valkey, SQLite
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
| `--http-chunked` | `0` | corpo em `Transfer-Encoding: chunked`, em pedaços de N bytes (`1..16777216`); `0` = `Content-Length` (§4.3) |
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
| `--pcap-flows` | `false` | uma conexão por fluxo original da captura, com o PROXY anunciando a origem e o destino capturados (§6.8) |
| `--verify-url` | — | conferir no destino (§10): `nats://`, `tls://`, `postgres://`, `postgresql://`, `redis://`, `rediss://`, `valkey://`, `valkeys://` |
| `--verify-sqlite` | — | conferir no `events.db` do `sink=sqlite` (§10) |
| `--verify-timeout` | `30s` | espera máxima da conferência, contada do fim do envio |
| `--verify-space` | `default` | espaço LogHub: subject NATS e stream Redis `loghub.<space>.gelf`, coluna `space` do PostgreSQL e do SQLite |
| `--verify-key` | — | stream Redis/Valkey (padrão `loghub.<space>.gelf`) ou stream NATS (padrão `LOGHUB_GELF`) |
| `--pcap-dedup` | `auto` | cópias de um datagrama na captura: `auto` (pelo ponto de captura), `on` (toda cópia em 50 ms) ou `off` (§6.3.1) |
| `--verify-tls-ca` | — | CAs (PEM) que verificam o destino em `tls://` e `rediss://`/`valkeys://` (padrão: as do sistema) |
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
não é pcap/pcapng; argumento posicional; `--http-chunked` fora de http/https ou
fora de `0..16777216`; `--pcap-flows` sem `--pcap`, ou com
`--proxy-src`/`--proxy-dst` (o fluxo anuncia os seus); `--verify-url` e
`--verify-sqlite` juntos; esquema de `--verify-url` fora da lista, URL sem
host, `nats://` com parâmetros (§10.2); `--verify-space` fora da gramática de
espaço, `--verify-key` com PostgreSQL ou SQLite; `--verify-timeout <= 0`, ou
`--verify-tls-ca`/`--verify-space`/`--verify-key`/`--verify-timeout` sem
conferência; `--pcap-dedup` sem `--pcap` ou fora de `auto|on|off`; conferência com `--file`/`--pcap`, ou com um
`--template` sem `"_seq"`, `{seq}`, `"_run"` e `{run}` (§10.1). Tudo conferido
**antes** do primeiro envio. A mensagem de erro de `--verify-url` nunca repete
a URL (pode ter senha).

### 3.3 Códigos de saída

| Código | Quando |
|---|---|
| `0` | todo desfecho foi sucesso (`sent` ou HTTP `2xx`) |
| `1` | alguma falhou, ou a fonte deu erro no meio |
| `2` | uso |
| `3` | alvo inalcançável: houve mensagens e nenhuma conexão abriu |
| `4` | conferência impossível (§10.3): o destino não abriu antes do envio (e **nada foi enviado**), não tem a tabela/stream, ou nenhuma leitura deu certo |
| `130` | interrompido (SIGINT/SIGTERM) |

Precedência: `130`, `3`, `4`, `1`. Uma mensagem enviada com sucesso que não
está no destino, um `_seq` repetido ou desconhecido no destino (§10.1) é
**`1`**, como uma falha de transporte: o relatório separa as duas. Os códigos
são os do `go-syslog-injector` (alinhamento da rodada de 2026-11-07; a
primeira versão deste módulo tinha `4` para perda e `5` para impossível).

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
- **`--http-chunked N`** (2026-11-07, pendência 4): em vez de
  `Content-Length`, `Transfer-Encoding: chunked` com o corpo (já comprimido)
  em pedaços de N bytes, o último menor, e o pedaço final `0` sem trailers. É
  o que um emissor que transmite sem saber o tamanho manda; o `net/http` do
  servidor o aceita, e o teto do corpo vale do mesmo jeito (o `413` antecipado
  por `Content-Length` não existe, o `413` pelo que foi lido sim). Sem efeito
  no desfecho nem nos reenvios.
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
- **Adaptação automática** (2026-11-07, pendência 7): quando **2 conexões
  seguidas** (de qualquer worker) recebem `Connection: close` já na 1ª
  resposta, as conexões abertas dali em diante usam pipeline **1** — evento
  `adapted: server closes every connection, pipeline 1`. Uma conexão que
  depois receba uma 2ª resposta (o servidor voltou a manter a conexão, ou
  era um shutdown passageiro) restaura `--pipeline` nas seguintes — evento
  `adapted: server keeps connections, pipeline N`. Só existe com
  `--pipeline > 1`; com 1 não há o que adaptar (a vaga só é liberada depois
  de marcar a conexão fechando, então nada é escrito atrás do fechamento).
  Dois e não um: um `Connection: close` isolado é o teto por conexão ou o
  shutdown, e o primeiro fechamento custa no máximo `--pipeline − 1`
  requisições devolvidas. As conexões já abertas mantêm o pipeline com que
  nasceram; o que elas escreverem atrás do fechamento volta ao worker como
  antes (sem perda, sem duplicata).
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
`proxy.protocol`/`proxy.trusted` do servidor e atribuir outra origem. Com
`--pcap-flows`, cada conexão anuncia a origem e o destino capturados do seu
fluxo (§6.8).

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

Portado do `go-relplog-injector` em 2026-11-07: o PKTAP (o `-i any` do
macOS) lê o tipo de enlace interno de `pth_dlt` — lia de `pth_type_next`,
sempre 1 (Ethernet), e o loopback (`DLT_NULL`) nunca decodificava —, e um
registro que não é pacote (`pth_type_next` ≠ 1) é `pcap_not_ip`; fragmentos
ainda esperando o datagrama no fim de uma passada contam em
`pcap_ip_fragments_lost` (sumiam sem contar).

**Fragmentos numa captura real** (pendência 6, fechada em 2026-11-07). O
loopback não fragmenta (MTU 64 KiB); uma ponte Docker sim (MTU 1500): o
injetor num container manda ao receptor estrito em outro, e o `tcpdump -i
eth0` do receptor grava os fragmentos antes de o kernel remontá-los. Datagramas
de 8192 (chunks gzip de documentos de 20 KB) e de 5000 sem chunk viram 6 e 4
fragmentos IPv4. Medido: 800 documentos (500 chunked + 300 sem compressão em
chunks de 3000), 6500 fragmentos, 1600 datagramas remontados, nenhum perdido,
800 de 800 idênticos ao que o receptor vivo aceitou, reproduzidos em TCP, UDP
e HTTP. Uma captura pequena dessa forma (30 documentos, 240 fragmentos) fica
em `internal/app/testdata/captures/ip-fragments.pcap` (`TestRealCaptures`).
Com o `-i any` do host da ponte, cada fragmento aparece duas vezes (em cada
`veth`): a cópia de mesmo deslocamento e tamanho é ignorada enquanto o
datagrama está aberto, e o datagrama remontado de novo pelas cópias tardias é
uma cópia do primeiro (§6.3.1 ou, se for chunk, a regra da remontagem).
IPv6 fragmentado continua provado só com capturas sintéticas.

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

#### 6.3.1 Duplicatas do `-i any` em datagramas sem chunk

Corrigido em 2026-11-07 (pendência 2). O `tcpdump -i any` grava o mesmo
datagrama mais de uma vez: no loopback, uma cópia de saída e uma de entrada;
numa máquina que roteia ou faz ponte (Docker), uma por interface. Um emissor
que repete de propósito o mesmo documento também gera datagramas idênticos, e
esses **não** podem sumir. O que separa os dois é o **ponto de captura**
(`vantage`) de cada cópia, que a captura registra:

| Enlace | Ponto de captura |
|---|---|
| Linux cooked v2 (`-i any` moderno) | índice da interface + tipo do pacote (de saída, para o host, ...) |
| Linux cooked v1 | tipo do pacote |
| pcapng | a interface do bloco (IDB), mais o acima quando o enlace é cooked |
| PKTAP (macOS `-i any`) | o nome da interface (`pth_ifname`), como o `go-relplog-injector` |
| outros (Ethernet de uma interface, loopback BSD, raw) | um só: nunca há cópia |

Chave (a mesma do `go-syslog-injector`): porta de origem, identificação IP
(IPv4, ou o id do fragmento IPv6; as cópias de um pacote a têm igual, o
próximo pacote do emissor não), tamanho e FNV-1a 64 dos bytes. Endereços e
porta de destino ficam **fora**: um NAT entre dois pontos de captura os
reescreve. `--pcap-dedup`:

- `auto` (padrão): cada ponto de captura tem a sua contagem de cópias da
  chave; a cópia que leva a
contagem do **seu** ponto acima da maior contagem dos **outros** pontos é uma
mensagem nova (reproduzida); senão é a cópia de uma já vista
(`udp_datagrams_duplicate`, não reproduzida). Assim saída+entrada de um envio
dão um documento, e o mesmo documento enviado duas vezes (saída, entrada,
saída, entrada) dá dois — mesmo com a identificação IP igual (IPv4 com DF
pode mandar 0 sempre), e inclusive se uma cópia de algum ponto se perdeu no
buffer do tcpdump. Numa captura de um ponto só, nada é descartado;
  É a regra do `go-relplog-injector` dita de outro jeito: cada original
  absorve no máximo uma cópia por **outro** ponto de captura, então uma
  mensagem enviada de fato duas vezes é reproduzida duas vezes;
- `on`: toda repetição da chave dentro da janela é cópia, qualquer que seja o
  ponto — para capturas que não registram o ponto (Ethernet de várias
  interfaces juntadas com `mergecap`), ao risco de uma repetição do emissor
  com a mesma identificação IP;
- `off`: nada é descartado.

A chave esquece **50 ms de tempo de captura** depois da sua última cópia (as
cópias de um mesmo pacote vêm a microssegundos umas das outras; o mesmo prazo
do `go-syslog-injector`), e há no máximo 65 536 chaves (acima disso a mais antiga é esquecida
antes do prazo). Só datagramas **sem** magic de chunk: os chunks já têm a
regra da remontagem (acima), que continua a mesma. Vale com e sem
`--pcap-port`, antes da checagem de conteúdo.

Prova real (2026-11-07, `internal/app/testdata/captures/any-bridge.pcap`):
`tcpdump -i any` no host de uma ponte Docker, entre dois containers, grava
cada datagrama duas vezes (as duas `veth`, índices diferentes). 50 documentos
do gerador, a mesma linha mandada 5 vezes de propósito e 10 documentos de 20
KB em chunks fragmentados: 310 pacotes, 55 cópias de datagramas sem chunk
descartadas, 20 chunks repetidos ignorados, e os 65 documentos que o receptor
vivo aceitou — as 5 repetições inclusive — reproduzidos exatamente uma vez
cada. No loopback de um container o Docker Desktop (kernel 6.x) grava uma
cópia só: o `-i any` não duplica em toda máquina, e a regra não depende disso.

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
- **Fim da direção** (decisão G12, revista em 2026-11-07, pendência 5): o
  quadro final sem `\0` depende de **como** a direção terminou na captura:
  - **fechada** — `FIN` (com todos os bytes até ele, sem lacuna no fim) ou
    `RST`: é o que o servidor leu no EOF (ou no erro de leitura) e entregou
    como documento (go-gelf-server SPEC §5.3). O replay faz o mesmo: no
    estrito, basta começar com `{` (senão `tcp_frames_not_gelf`); um
    documento com espaço interno no fim, ou malformado de propósito, sai como
    está e o servidor o marca como marcaria o original;
  - **sem fim** (`tcp_unterminated`: a captura parou, ou a direção ficou
    ociosa 10 min): o corte pode ter caído em qualquer byte, então só é
    documento (estrito) se começar com `{` e terminar com `}`; senão
    `frames_lost`. Um quadro cortado logo depois de um `}` interno ainda
    passa: é o limite do que se prova sem ler o JSON.
  Com `--pcap-port` (não estrito) o quadro final não vazio é sempre
  documento, fechada a direção ou não, como antes.
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

TLS/HTTPS capturado; o IP de origem como origem do socket (o PROXY o anuncia,
§6.8); HTTP/2; direções de servidor.

### 6.8 Uma conexão por fluxo (`--pcap-flows`)

Corrigido em 2026-11-07 (pendência 3). Sem a flag, os documentos da captura
inteira se dividem entre as `--concurrency` conexões, na ordem em que um
worker fica livre: o servidor vê poucas conexões, nenhuma com a forma das
originais. Com `--pcap-flows`:

- **Fluxo** = o par (origem `ip:porta`, destino `ip:porta`) capturado de onde o
  documento saiu: uma direção TCP (GELF `\0` ou HTTP) ou os datagramas UDP de
  um par. Cada fluxo tem a **sua** fila e o seu worker, com uma conexão (ou
  socket UDP) só dele, aberta no primeiro documento do fluxo. A ordem dentro
  de um fluxo é a da captura; entre fluxos, cada um anda no seu passo.
- **Fim do fluxo**: a conexão é fechada (depois de escrever o que tem e
  esperar as respostas, como no fim da execução) quando a direção TCP
  termina na captura (`FIN`, `RST`, ou o fim da passada para as que ficaram
  abertas), e, no UDP, no fim de cada passada. Um documento do mesmo par
  depois disso (porta reusada, ou a passada seguinte do `--loop`) abre uma
  conexão nova. O fim é entregue em ordem: só depois do último documento do
  fluxo.
- **Teto**: `--concurrency` × **256** fluxos abertos ao mesmo tempo (os 256
  por worker do `go-syslog-injector`; com o padrão 1, 256). No teto, o fluxo
  usado há mais tempo é fechado como se tivesse terminado (evento `flow
  closed at --concurrency`), e um documento dele depois disso reabre a
  conexão.
- **PROXY** (`--proxy-protocol v1|v2`): cada conexão anuncia a origem e o
  destino **capturados** do seu fluxo (inclusive IPv6, ou famílias
  diferentes como IPv6 mapeado, §4.5). `--proxy-src`/`--proxy-dst` são
  recusados junto (exit 2): cada fluxo tem os seus. É o único modo de o
  servidor atribuir a cada documento o emissor original (`src_ip`,
  `x-proxy`); sem PROXY (e sempre no UDP), o fluxo só preserva a separação em
  conexões.
- `--rate` e `--pcap-timing original` continuam globais (o produtor é um só).
  Os desfechos, os reenvios HTTP e o relatório são os de sempre; os workers
  de fluxo dividem os mesmos contadores.

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
`rate_bytes_per_sec`, `latency` (µs, só dos sucessos), `connections`, `events`,
`source` (§8.4) e, com conferência, `verify` (§10.4).

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
`udp_not_gelf`, `udp_datagrams_duplicate` (§6.3.1), `udp_chunked_messages`, `udp_chunks`, `udp_chunks_invalid`,
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

1. *Corrigido em 2026-11-07:* conferência no destino (NATS, PostgreSQL,
   Redis/Valkey, SQLite) pelos `_seq` da execução; regra em §10.
2. *Corrigido em 2026-11-07:* duplicatas do `-i any` de datagramas sem chunk,
   pelo ponto de captura de cada cópia; regra em §6.3.1.
3. *Corrigido em 2026-11-07:* uma conexão por fluxo original no replay, com o
   PROXY da origem capturada (`--pcap-flows`); regra em §6.8.
4. *Corrigido em 2026-11-07:* HTTP/1.1 `chunked` no envio (`--http-chunked`,
   §4.3); HTTP/2 virou decisão (o `go-gelf-server` não o aceita, §1.2).
5. *Corrigido em 2026-11-07:* último quadro TCP sem `\0` de uma direção
   fechada (`FIN`/`RST`) é documento como no servidor; regra em §6.4.
6. *Corrigido em 2026-11-07:* fragmentos IP provados numa captura real
   (`tcpdump -i eth0` numa ponte Docker de MTU 1500, emissor em outro
   container); regra e prova em §6.2.
7. *Corrigido em 2026-11-07:* adaptação a servidor que fecha toda conexão
   (pipeline 1 depois de 2 conexões fechadas na 1ª resposta); regra em §4.3.

---

## 10. Conferência no destino

Decisão 4, revista pelo usuário em 2026-11-07: o relatório do transporte diz
o que saiu ("o kernel aceitou", "o servidor respondeu `202`"); só o destino
diz o que chegou. Com `--verify-url` (ou `--verify-sqlite`), depois do envio o
injetor lê o que o servidor gravou e conta, pelos marcadores da execução, o
que se perdeu e o que chegou repetido. Usa as **mesmas bibliotecas e
versões** dos servidores (`go-gelf-server/go.mod`): `nats.go` (pacote
`jetstream`), `pgx/v5`, `rueidis` e `modernc.org/sqlite` (sem cgo: o binário
continua estático, decisão 6 revista).

### 10.1 O que é conferido

- **Marcadores**: o documento do gerador traz `"_seq":N` e `"_run":"R"`
  (§5.1), que todo servidor da família grava em `payload.fields.seq` e
  `payload.fields.run` do JLH `type=gelf` (go-gelf-server SPEC §3.2, §6.6). O
  conferidor lê os JLH cujo `fields.run` é o `--run` desta execução e conta
  cada `fields.seq` — número inteiro positivo, ou string de dígitos (um modelo
  pode pôr `"_seq":"{seq}"`); outro valor conta em `unparsable`.
- **Fontes**: o gerador; um `--template` que contém `"_seq"`, `{seq}`, `"_run"`
  e `{run}` (o usuário escreve `"_seq":{seq},"_run":"{run}"`; o conferidor
  não analisa o modelo além disso). `--file` e `--pcap` são recusados (exit 2):
  os documentos não trazem os marcadores desta execução.
- **Execução**: o `--run` padrão é aleatório, então duas execuções não se
  misturam; reusar um `--run` dentro da janela de §10.3 mistura as duas, e o
  relatório acusa duplicatas.
- **Classificação**, para os `seq` produzidos (de 1 ao último que entrou na
  fila, §7):

| Contador | Significado | Erro (exit 1)? |
|---|---|---|
| `expected` | desfecho de sucesso (`sent`, `2xx`) | — |
| `found` | dos `expected`, os achados no destino (distintos) | — |
| `lost` | `expected` que não estão no destino | **sim** |
| `duplicated` | `seq` achados mais de uma vez (`extra_copies`: as cópias além da 1ª) | **sim** |
| `failed_found` | desfecho de falha, mas o servidor guardou (um `no_response` ou `timeout` que ele processou) | não: a falha já está no relatório |
| `failed_missing` | desfecho de falha e ausente | não |
| `unexpected` | `seq` desta execução que nunca foi produzido (`0`, ou além do último) | **sim** |
| `unparsable` | JLH desta execução com `fields.seq` que não é inteiro positivo | **sim** |

  O relatório lista as perdidas em até 20 faixas (`lost_seqs`: `"4"`,
  `"10-19"`) e até 10 `seq` repetidos e desconhecidos (`duplicated_sample`,
  `unexpected_sample`).

### 10.2 Destinos

O formato que cada destino grava é o da SPEC do `go-gelf-server` (§3.6–§3.8,
§10.3–§10.7), o mesmo nos cortes `go-gelf-pg-server` (só PostgreSQL) e
`go-gelf-nats-server` (só NATS).

| URL | Lê | Como |
|---|---|---|
| `nats://`, `tls://` (lista separada por vírgula, como o `nats.go`) | JetStream, stream `--verify-key` (padrão `LOGHUB_GELF`, o `sink.nats.stream`; §3.8 do servidor), subject `loghub.<--verify-space>.gelf` | consumidor **ordenado e efêmero** (nada fica no servidor) com `DeliverByStartSequence` (a última sequência na checagem + 1) e o filtro de subject, `Fetch` com `FetchMaxWait` até `NumPending = 0`; uma sequência do stream já contada nunca conta de novo (um consumidor ordenado que se reinicia recomeça do ponto de partida; o `go-syslog-injector` viu o `FetchNoWait` fazer isso a cada chamada); o JLH está em `Data` |
| `postgres://`, `postgresql://` | `loghub.inbox` (§3.7.1) | `SELECT payload->'fields'->>'seq' FROM loghub.inbox WHERE uuid >= $1 AND type = 'gelf' AND space = $3 AND payload->'fields'->>'run' = $2`; o limite inferior do `uuid` (UUIDv7, ordem de tempo) usa a chave primária |
| `redis://`, `rediss://`, `valkey://`, `valkeys://` | stream `--verify-key` (padrão `loghub.<--verify-space>.gelf`, o `sink.redis.stream` com `{space}` expandido; §3.6), campo `e` | `XRANGE` desde depois do último ID na checagem, em páginas de 1000, continuando do último ID lido |
| `--verify-sqlite <arquivo>` | `gelf_events` do `events.db` (§3.7) | aberto **só leitura**; `uuid >= $1` (texto canônico, mesma ordem), `space = $2` e `json_extract(CAST(env AS TEXT), '$.payload.fields.run')` (o `env` é BLOB, e um SQLite que o lesse como JSONB recusaria a linha) |

`--verify-space` (padrão `default`) e `--verify-key` são os nomes do
`go-syslog-injector`. A URL vai inteira ao driver: `nats://` não aceita
parâmetros (exit 2); no `redis://` o banco vem do caminho (`/1`) e os
parâmetros são os do `rueidis.ParseURL` (`dial_timeout`, `protocol`, ...);
`valkey://` e `valkeys://` são `redis://` e `rediss://` (decisão da rodada
Valkey de 2026-11-07: o protocolo é o mesmo); no `postgres://` todos vão ao
`pgx` (`sslmode`, `sslrootcert`, ...), e a tabela é sempre `loghub.inbox`
(`--verify-key` recusado). Usuário e senha (ou token NATS) na URL;
`--verify-tls-ca` dá as CAs de `tls://` e `rediss://`.

Limites de cada destino, que a conferência mostra mas não contorna:

- **`loghub.inbox` é uma fila**: um consumidor que já removeu linhas faz a
  conferência acusar `lost`. Confira com o consumidor parado.
- **NATS `sink.nats.mode = core`** não guarda nada: não há o que ler.
- **Redis com `sink.redis.maxlen`**: o que o trimming descartou é `lost` (e é
  mesmo: o destino não o tem mais). O mesmo para o `max_age`/`max_msgs` do
  stream NATS.
- **SQLite com `sink.sqlite.rotate`**: só o arquivo dado é lido.
- O usuário da conferência precisa de leitura: `SELECT` na `loghub.inbox`
  (o papel do servidor tem só `SELECT (uuid)`), consumidor no stream NATS,
  `XRANGE` no Redis.

### 10.3 Espera e janela

- **Antes do primeiro envio** o destino é aberto e conferido (o stream existe,
  a tabela existe, o arquivo abre): falha é exit 4 sem enviar nada — uma URL
  errada não gasta uma carga.
- **Ponto de partida**: NATS e Redis/Valkey são lidos **depois da última
  entrada do stream no momento da checagem** (a sequência do JetStream, o ID
  do `XREVRANGE … COUNT 1`): tudo da execução vem depois, sem relógio
  nenhum, e nada do passado é lido. A primeira versão partia do início da
  execução menos 10 min e, num stream com ~250 000 mensagens nessa janela,
  uma leitura passava do `--verify-timeout`. PostgreSQL e SQLite não têm essa
  ordem de chegada: **janela** do início da execução menos **10 min** pelo
  UUIDv7 (que vem do relógio do servidor; a folga cobre relógios de máquinas
  diferentes).
- **Espera**: depois do fim do envio (drenagem incluída, §7), lê o destino a
  cada 250 ms — NATS e Redis continuando do ponto lido (stream só cresce);
  PostgreSQL e SQLite relendo a janela inteira (uma linha de `uuid` menor pode
  ser gravada depois, por outro worker do sink). Termina quando todo
  `expected` foi achado, com **uma leitura a mais 1 s depois** (uma
  duplicata do at-least-once chega logo atrás; o `go-syslog-injector` perdia
  duplicatas no PostgreSQL sem ela), ou em `--verify-timeout`
  (padrão 30 s, contado do fim do envio): aí `complete` é `false` e o que
  falta é `lost`.
- Uma leitura que falha é tentada de novo na próxima volta; sem nenhuma
  leitura boa até o prazo, exit 4 com o erro.
- Interrompido (SIGINT) durante a espera: o relatório sai com o que foi
  contado, `complete: false`, exit 130.

### 10.4 Relatório

O objeto `verify` do JSON (e a seção `verify:` do texto), com os nomes do
`go-syslog-injector` onde existem: `destination` (**só esquema e host** da URL,
nunca usuário, senha, caminho ou parâmetros, mais o stream ou a tabela; ou o
arquivo), `run`, `reads`, `waited` (segundos, do fim do envio), `complete`,
`expected`, `found`, `lost`, `duplicated`, `unexpected`, `lost_seqs`, os
demais contadores de §10.1, as amostras e `error` (a última falha de leitura).
Nenhuma mensagem ecoa a URL: o texto de erro dos drivers passa por uma troca
da senha por `xxxxx` antes de sair.