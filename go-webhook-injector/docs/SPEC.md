# go-webhook-injector — Especificação v1

Ferramenta cliente que **injeta requisições HTTP** num servidor de webhooks —
em especial os do ecossistema LogHub (`go-webhook-server`,
`go-webhook-fast-server`, `go-webhook-pg-server`, `go-webhook-nats-server`) —
para testar o comportamento dele ou colocá-lo sob carga extrema. Criada em
2026-10-30/31; as decisões que a definiram estão em `docs/DECISOES.md`, citadas
aqui como "decisão N".

Esta especificação é a fonte da verdade: discrepância entre código e spec é bug
no código. Para mudar comportamento, primeiro a spec (com o porquê), depois o
código, depois o teste.

---

## 1. Escopo

### 1.1 Objetivos

- Enviar requisições por **HTTP/1.1** (keep-alive, com **pipelining**
  opcional) e **HTTP/2** (h2c por *prior knowledge* em `http://`, ALPN `h2` em
  `https://`), em claro ou TLS (mTLS opcional).
- Servir para o teste **funcional** (uma requisição, ou poucas, e a resposta de
  cada uma) e para a **carga extrema** (centenas de milhares de requisições por
  segundo: muitas conexões, pipelining, muitos *streams* HTTP/2, taxa alvo,
  duração, vazão e latência medidas).
- Quatro fontes de conteúdo (decisão 3): o **gerador** JSON com `seq`, um
  **modelo com marcadores**, as linhas de um **arquivo ou do stdin** (ou os
  arquivos de uma pasta) e o **replay de uma captura do tcpdump** (pcap ou
  pcapng), que leva ao servidor as requisições reais de produção — método,
  caminho, cabeçalhos e corpo.
- Relatório final honesto: cada requisição tem **um** desfecho (§8.2) — o código
  de status da resposta, ou o motivo de não haver uma — e o código de saída diz
  se alguma falhou.
- `--uuid-out`: o `X-LogHub-UUID` de cada resposta, para conferir no destino
  (§8.5).

### 1.2 Não-objetivos (v1)

- **Conferir a entrega no destino** pelo próprio injetor (decisão 4). No HTTP a
  resposta `2xx` dos servidores LogHub já significa a garantia da combinação
  ring × sink; `--uuid-out` dá a lista para conferir fora (pendência 1).
- Captura ao vivo de uma interface: a entrada do replay é sempre um arquivo.
- Reproduzir o **IP de origem** da captura (exigiria socket cru). O endereço
  original pode ir no `X-Forwarded-For` (`--pcap-xff`, §6.1), que o servidor só
  aceita de um proxy confiável.
- Replay de HTTP/2 ou de HTTPS capturados (§6.7).
- Seguir redirecionamentos, cookies, compressão: um servidor de webhooks não os
  usa, e o injetor mede a resposta como ela veio.
- Arquivo de configuração: só flags.

### 1.3 Família

Filho do `go-syslog-injector` (SI, o pai do código), irmão do
`go-relplog-injector` (decisão 1). Nada é importado entre eles: gerador de
marcadores, leitor de pcap, remontagem TCP, escalonador e relatório vieram
copiados e adaptados. Uma correção num pacote de mesma origem vale para os
outros: registre no `CHANGELOG.md` da raiz o que ficou "a portar".

---

## 2. Arquitetura

```
fonte ──> produtor ──> fila (4 × concorrência) ──> N workers ──> N conexões ──> servidor
 gen        ritmo          canal                     cada um       HTTP/1.1 (pipeline)
 template   (--rate,                                 com a sua     ou HTTP/2 (streams)
 file       --pcap-timing,                           conexão
 pcap       --count,
            --duration)                  respostas ──> relatório (shards por worker)
```

- **Uma** goroutine produtora pede à fonte a mensagem `seq` (de 1 em diante) e a
  solta na fila no instante planejado (§7).
- **N workers** (`--concurrency`), cada um dono de **uma** conexão, tiram da
  fila, montam a requisição (§5.5), enviam e registram o desfecho. Uma conexão
  quebrada é fechada, e o worker disca de novo com espera exponencial (100 ms
  dobrando até 2 s).
- O contrato de toda conexão (`sched.Conn`): o callback de desfecho é chamado
  **exatamente uma vez** por requisição entregue a `Send` — na resposta, quando
  a conexão morre, no `Close` — **ou** a requisição é devolvida ao worker por
  `Unsent` (§4.4), que a reenvia na próxima conexão. É o que garante
  `requisições = ok + falhas` no relatório.

### 2.1 Mapa do código

```
cmd/go-webhook-injector/main.go   só chama internal/app
internal/
  app/        flags, validação, montagem da requisição, sinais, relatório
  tmpl/       modelos com marcadores, seguros para JSON (§5.2)  [lógica pura]
  source/     gerador JSON, modelo, arquivo/pasta/stdin, pcap
  httpx/      requisições HTTP/1.x dentro de um fluxo TCP capturado:
              pipelining, Content-Length, chunked, ressincronização [fuzz]
  pcap/       leitor pcap/pcapng, enlaces, IPv4/IPv6, fragmentos,
              remontagem TCP                                   [fuzz]
  pcap/pcaptest/  escritor de capturas para os testes
  sched/      produtor, workers, ritmo, reenvio do devolvido, interrupção
  transport/  cliente HTTP/1.1 próprio (pipelining) e HTTP/2 (net/http)
  report/     contadores, histograma, texto/JSON, progresso
```

Dependências: **nenhuma** fora da biblioteca padrão (decisão 6). O HTTP/2 em
claro usa `http.Protocols.SetUnencryptedHTTP2` (Go 1.24+).

---

## 3. Linha de comando

```
go-webhook-injector [flags]
```

### 3.1 Flags

| Flag | Padrão | Significado |
|---|---|---|
| `--url` | `http://127.0.0.1:8080/` | alvo, `http://` ou `https://`; caminho e query aceitam marcadores (§5.2) |
| `--method` | `POST` | método (maiúsculas; `CONNECT` é recusado) |
| `--header` | — | cabeçalho extra `"Nome: valor"`, repetível; o valor aceita marcadores |
| `--host` | o host de `--url` | cabeçalho `Host` |
| `--content-type` | `application/json` | `Content-Type` do gerador, do modelo e do arquivo; vazio = nenhum |
| `--http2` | `false` | HTTP/2: h2c em `http://`, ALPN em `https://` |
| `--pipeline` | `1` | HTTP/1.1: requisições em voo por conexão (pipelining quando > 1) |
| `--streams` | `100` | HTTP/2: requisições em voo por conexão |
| `--requests-per-conn` | `0` | HTTP/1.1: fecha e reabre a conexão depois de N requisições (`0` = mantém) |
| `--retries` | `0` | reenvia uma resposta `429` ou `503` até N vezes (§4.5) |
| `--retry-max-wait` | `5s` | maior espera antes de um reenvio, diga o `Retry-After` o que disser |
| `--timeout` | `10s` | prazo de discagem, handshake TLS e escrita |
| `--response-timeout` | `30s` | prazo de cada resposta |
| `--tls-ca` / `--tls-cert` / `--tls-key` | — | CAs que verificam o servidor; certificado e chave do cliente (mTLS) |
| `--tls-server-name` | o host de `--url` | nome conferido no certificado |
| `--tls-min-version` | `1.2` | `1.2` ou `1.3` |
| `--tls-insecure` | `false` | não verifica o certificado |
| `--uuid-out` | — | grava `seq status X-LogHub-UUID` de cada resposta final (§8.5) |
| `--count` | 1; sem limite com `--duration`; a fonte inteira com `--file`/`--pcap` | requisições; `0` = sem limite |
| `--duration` | `0` | para depois deste tempo |
| `--rate` | `0` | requisições/s somando as conexões; `0` = o máximo |
| `--concurrency` | `1` | conexões em paralelo |
| `--drain-timeout` | `10s` | no fim, quanto esperar por respostas e reenvios |
| `--template` | — | corpo com marcadores (§5.2) |
| `--file` | — | um corpo por linha (`-` = stdin), ou um corpo por arquivo de uma pasta (§5.3) |
| `--pcap` | — | replay de captura (§6) |
| `--size` | `0` | completa o JSON do gerador até N bytes (§5.1) |
| `--run` | 8 hex aleatórios | identificador da execução (`{run}`) |
| `--loop` | `false` | recomeça `--file`/`--pcap` no fim |
| `--pcap-port` | — | portas de destino a reproduzir |
| `--pcap-timing` / `--pcap-speed` | `asap` / `1` | ritmo do replay (§6.5) |
| `--pcap-path` | `keep` | alvo da requisição reproduzida: `keep` (o capturado) ou `url` (o de `--url`) |
| `--pcap-host` | `keep` | `Host` da requisição reproduzida: `keep` ou `url` |
| `--pcap-xff` | `false` | acrescenta o IP do cliente capturado ao `X-Forwarded-For` |
| `--report` | `text` | relatório final: `text` ou `json` |
| `--progress` | `auto` | linha de progresso no stderr; `0` desliga; `auto` = 1 s num terminal |
| `--version` | | imprime a versão e sai |

### 3.2 Combinações recusadas (exit 2)

`--template`, `--file` e `--pcap` juntos; `--size` fora do gerador; `--loop`
sem `--file`/`--pcap`, ou com stdin; `--pcap-*` sem `--pcap`;
`--pcap-timing original` com `--rate`; `--pipeline` ou `--requests-per-conn`
com `--http2`; `--tls-*` sem `https://`; URL sem host, com espaço, quebra de
linha ou fragmento; método fora de `[A-Z]+` ou `CONNECT`; `--header` sem `:`,
com nome inválido, com quebra de linha, ou de enquadramento (`Host`,
`Content-Length`, `Transfer-Encoding`, `Connection`, `Keep-Alive`, `Upgrade`,
`TE`, `Trailer`, `Expect`, `Proxy-Connection`, `HTTP2-Settings`: são do
transporte, e um valor do usuário desalinharia a conexão); marcador
desconhecido; captura que não é pcap nem pcapng. Tudo antes do primeiro envio.

### 3.3 Códigos de saída

| Código | Quando |
|---|---|
| `0` | toda requisição teve resposta `2xx` |
| `1` | ao menos uma falhou (não-2xx ou sem resposta), ou a fonte deu erro no meio |
| `2` | uso: flag inválida, combinação recusada, arquivo que não abre |
| `3` | alvo inalcançável: houve requisições e **nenhuma** conexão abriu |
| `130` | interrompido (SIGINT/SIGTERM) |

---

## 4. Transporte

### 4.1 HTTP/1.1

Cliente próprio sobre `net.Conn` (não o `http.Transport`), porque a carga
extrema pede o que o da biblioteca padrão não faz: **pipelining** e uma conexão
por worker sob controle total.

- A requisição é serializada uma vez num buffer: linha de requisição, `Host`,
  cabeçalhos na ordem, `Content-Length` (sempre que há corpo, e também com corpo
  vazio em métodos que o têm; `GET`, `HEAD`, `OPTIONS` e `DELETE` sem corpo não
  o levam) e o corpo. Nunca `chunked`.
- Escrita por um `bufio.Writer` de 64 KiB. Com `--pipeline 1`, cada requisição é
  empurrada na hora. Com pipelining, o buffer é empurrado quando enche, quando
  o worker não tem a próxima pronta, e **antes de esperar uma vaga** no
  pipeline — sem isso, as respostas que liberariam a vaga nunca viriam
  (impasse).
- Uma goroutine leitora por conexão casa as respostas com as requisições **em
  ordem** (`http.ReadResponse`, o corpo lido e descartado). O prazo de leitura
  (`--response-timeout`) só corre com requisição pendente: ociosa, a conexão
  espera sem prazo, e um fechamento do servidor é percebido ali.
- `TCP_NODELAY` ligado; TLS com ALPN `http/1.1`.

### 4.2 HTTP/2

`net/http` com um `http.Transport` **por worker** (`MaxConnsPerHost = 1`):
uma conexão, até `--streams` requisições em voo, cada uma na sua goroutine. Em
`http://`, só h2c por *prior knowledge* (`SetUnencryptedHTTP2`), como os
servidores LogHub aceitam; em `https://`, só `h2` por ALPN. Cada transporte tem
a sua **cópia** do `tls.Config`: o `net/http` escreve `NextProtos` na
configuração que recebe, e as dos workers rodam ao mesmo tempo (corrida achada
pelo `-race`). Discagens contadas pelo `DialContext`.

### 4.3 Conexão por requisições (`--requests-per-conn`)

Só HTTP/1.1. A N-ésima requisição sai com `Connection: close`; o worker fecha a
conexão (sem contá-la como quebrada) e disca outra. Serve para medir o custo do
*accept* e do handshake TLS do servidor.

### 4.4 Fechamento pelo servidor

- **`Connection: close` na resposta** (RFC 9112 §9.6): o servidor não lê nada
  depois dela. As requisições já escritas atrás dela, em pipelining, **não
  foram processadas**: voltam ao worker (`Unsent`) e saem de novo na próxima
  conexão, sem desfecho e sem duplicata. O mesmo vale para uma requisição que
  nunca chegou ao fio (a escrita falhou antes, ou a conexão já estava morta).
- **Fechamento sem aviso** (EOF, RST) com requisições pendentes: elas **podem**
  ter sido processadas, então não são reenviadas — `no_response` (ou
  `conn_error` para a que estava sendo lida). Um `POST` não é idempotente.
- Uma requisição devolvida por uma conexão que **não respondeu nada** conta uma
  tentativa; depois de 5 assim, ela termina como `conn_error` em vez de girar
  para sempre. Um servidor que responde algumas antes de fechar sempre progride,
  e as devolvidas dele não contam.
- Uma conexão gasta (`--requests-per-conn`) não aceita mais nada, nem um
  reenvio: o que chega depois da requisição com `Connection: close` volta ao
  worker. Um reenvio que gasta a conexão faz o `Flush` avisar (`ErrSpent`), e o
  worker a fecha sem contá-la como quebrada.
- A decisão de reenviar um `429`/`503` é tomada no mesmo trecho protegido que
  tira a requisição da fila de respostas: o `Close` nunca a vê fora das duas
  listas (fecharia cedo e perderia um reenvio que o prazo comportava).
- Uma escrita que falha não fecha o socket: a resposta com `Connection: close`
  pode estar no buffer de leitura, e só a leitora sabe o que o servidor deixou
  de processar.

### 4.5 Reenvio de `429` e `503` (`--retries`)

Os servidores LogHub respondem `429` (ring cheio) e `503` (sem prontidão) com
`Retry-After`: "agora não", nunca "não". Com `--retries N`, a requisição volta
depois de `Retry-After` (segundos ou data HTTP; sem ele, 100 ms), limitado por
`--retry-max-wait`, até N vezes. No HTTP/1.1 o reenvio sai na mesma conexão
quando chega a hora (no próximo `Send` ou `Flush`, ou no `Close`); uma conexão
gasta (`--requests-per-conn`) ou fechada pelo servidor passa os reenvios
pendentes para a próxima. Esgotadas as tentativas, o desfecho é o último status.
A latência registrada inclui as esperas.

---

## 5. Fontes e requisições

### 5.1 Gerador

O corpo do gerador é JSON:

```json
{"seq":1,"run":"0f1e4480","injector":"go-webhook-injector","ts":"2026-10-31T11:52:06.645938-03:00"}
```

No destino, um `seq` faltando é **perda** e um repetido é **retransmissão**.
`--size N` acrescenta `"pad":"xxx…"` antes do `}` final até N bytes, mantendo o
JSON válido; um corpo já maior fica como está. Serve para testar
`server.max_body_bytes` e os tetos de bytes do ring.

### 5.2 Modelo

`--template` (o corpo), o caminho e a query de `--url` e o valor de cada
`--header` são compilados uma vez no início.

| Marcador | Valor |
|---|---|
| `{seq}` | número da requisição, de 1 |
| `{run}` | `--run` |
| `{uuid}` | UUID v4 aleatório, novo a cada requisição |
| `{ts}` | hora local, RFC 3339 com microssegundos |
| `{unix}` / `{unixms}` | segundos / milissegundos desde a época |
| `{host}` | o hostname do injetor, sem domínio |
| `{pid}` | PID do injetor |
| `{rand:N}` | `N` caracteres de `[a-z0-9]`, `1 ≤ N ≤ 65536` |

**Diferença do SI:** os corpos aqui são JSON, cheios de chaves. Um `{` só abre
marcador quando o que vem até o próximo `}` **parece** um (letras minúsculas,
opcionalmente `:` e dígitos/letras); aí um nome desconhecido é erro, para que um
erro de digitação não saia como texto. Qualquer outro `{` e todo `}` são
literais, e `}}` continua `}}` (o JSON fecha objetos aninhados assim). `{{` é
um `{` literal, para escrever o texto de um marcador (`{{seq}` → `{seq}`).

### 5.3 Arquivo, pasta e stdin

`--file` com um arquivo envia um corpo por linha, na ordem: o LF termina a
linha, os `CR` finais saem, linhas vazias são puladas (contadas), a última
linha sem LF conta. Com uma **pasta**, cada arquivo regular é um corpo inteiro
(quebras de linha incluídas), em ordem de nome. Corpo acima de 64 MiB é cortado
(contado). `--loop` relê do início, limitado por `--count`/`--duration`; uma
fonte sem nenhum corpo termina em vez de girar. O stdin (`-`) não pode ser
relido: `--loop` com ele é erro de uso.

### 5.4 Ordem dos cabeçalhos

Gerador, modelo e arquivo: `Host`, `Content-Type` (de `--content-type`, a menos
que um `--header Content-Type` o substitua), os `--header` na ordem dada,
`Content-Length`.

### 5.5 Requisição reproduzida (pcap)

- Método, alvo e cabeçalhos capturados. O alvo em forma absoluta
  (`http://host/x`, de um cliente falando com proxy) vira a forma de origem
  (`/x`).
- **Saem**: os de enquadramento e *hop-by-hop* de §3.2 (o corpo vai com o
  `Content-Length` novo; um corpo `chunked` capturado sai inteiro, já
  decodificado). Os demais saem em ordem alfabética de nome, porque o mapa do
  `net/http` perde a ordem capturada (pendência 3).
- `Host`: o capturado (`--pcap-host keep`) ou o de `--url`. Alvo: o capturado
  (`--pcap-path keep`) ou o de `--url` com marcadores.
- `--pcap-xff`: o IP do cliente capturado é acrescentado ao `X-Forwarded-For`
  (depois dos que já vinham). O servidor só o aceita se o injetor estiver em
  `proxy.trusted`.
- Os `--header` são acrescentados depois dos capturados.

---

## 6. Replay de captura (`--pcap`)

### 6.1 O que é reproduzido

As requisições HTTP/1.x dos fluxos TCP **cliente → servidor** de uma captura do
tcpdump, **em ordem de captura**, pelo transporte escolhido (uma captura de
HTTP/1.1 pode ser reproduzida em HTTP/2). Sem `--pcap-port`, toda direção que
abre com uma linha de requisição HTTP/1.x; com ela, só as direções para essas
portas de destino. As direções de resposta (`HTTP/1.1 200 …`), as de h2c
(`PRI * HTTP/2.0`) e as de outros protocolos são contadas e descartadas.

### 6.2 Formatos e enlaces

pcap clássico (microssegundos ou nanossegundos, as duas ordens de bytes) e
pcapng (seções, várias interfaces, `if_tsresol`, blocos EPB, SPB e PB). Enlaces:
Ethernet (com 802.1Q/802.1ad), Linux cooked v1 e v2 (`-i any`), loopback BSD
(`NULL`, `LOOP`), raw IPv4/IPv6 e PKTAP do macOS. Outro enlace é contado
(`pcap_not_ip`) e pulado. Um arquivo cortado no meio de um registro reproduz
tudo até ali e conta `pcap_cut_at_end`.

### 6.3 IP

IPv4 e IPv6 (com os cabeçalhos de extensão hop-by-hop, routing e destination),
com remontagem de fragmentos (64 KiB e 128 fragmentos por datagrama, 1024
abertos, 30 s de janela em tempo de captura; duplicata idêntica ignorada;
sobreposição recusada, RFC 5722). Código herdado do SI.

### 6.4 Remontagem TCP

Um fluxo de bytes por **direção**, em ordem de sequência (com a volta de 2³²),
sem os bytes retransmitidos (contados), fora de ordem segurado até 4 MiB por
direção. Um buraco que não se fecha é entregue como **lacuna** (`tcp_gaps`,
`tcp_gap_bytes`). FIN/RST encerram a direção; SYN novo na mesma quádrupla é
conexão nova; ociosa por 10 min de captura é encerrada; no máximo 131 072
direções abertas. Uma direção cuja captura começou depois do SYN é
**midstream**. Uma direção sem FIN nem RST no fim da captura conta em
`tcp_unterminated`: o fim dela pode faltar sem lacuna visível (pare o tcpdump
alguns segundos depois do tráfego, com `-B` generoso).

### 6.5 Ritmo

`--pcap-timing asap` (padrão) envia no ritmo de `--rate` (ou o máximo).
`original` agenda cada requisição no seu deslocamento desde o **primeiro
pacote** da captura, dividido por `--pcap-speed`; a requisição leva o instante
do segmento que a completou. Com `--loop`, cada passada começa 1 ms depois da
última requisição da anterior. Atraso de mais de 100 ms é contado
(`behind_schedule`).

### 6.6 Requisições dentro de um fluxo

Decidido **uma vez por direção**, nos primeiros bytes: linha de requisição
`MÉTODO SP alvo SP HTTP/1.x` (método `[A-Z]{1,32}`) → HTTP; `HTTP/` → resposta,
ignorada; `PRI * HTTP/2.0` → h2c, ignorado; outra coisa → ignorado.

- Cada requisição é lida pelo `http.ReadRequest` (o mesmo parser dos
  servidores), mas só depois de o **cabeçalho inteiro** estar no buffer (até a
  linha vazia; teto de 1 MiB): com o cabeçalho cortado no meio de uma linha, o
  parser acusa "cabeçalho malformado" em vez de "faltam bytes", e uma
  requisição só incompleta seria descartada (achado do primeiro teste com
  segmentos de 1 byte).
- Corpo por `Content-Length` ou **chunked**. Um `Content-Length` acima de 64
  MiB é recusado já no cabeçalho (`http_requests_too_large`), sem esperar os
  bytes declarados (achado da revisão: o extrator guardava até o tamanho
  declarado, ou o resto da captura). O fim do chunked é achado por um
  leitor próprio antes do `net/http`, pela mesma razão: cortado numa linha de
  tamanho ou no trailer, o `net/http` acusa malformação.
- Requisições em **pipelining** são lidas uma atrás da outra; uma requisição
  acima de 64 MiB é descartada (`http_requests_too_large`).
- **Ressincronização** — depois de uma lacuna, de uma requisição inválida, ou
  desde o início de um fluxo midstream — descarta bytes até a primeira posição
  que começa uma palavra maiúscula (não precedida de outra maiúscula) e cuja
  linha é uma linha de requisição que analisa (ou ainda pode analisar). Não só
  depois de um LF: uma requisição em pipelining começa colada ao fim do corpo
  da anterior. Um candidato que ainda espera bytes **segura** a decisão. Sem
  candidato, guarda só os últimos 64 bytes, e o corte nunca cai no meio de uma
  palavra maiúscula — o pedaço final dela viraria um candidato que nunca foi, e
  o resultado dependeria de quando o corte aconteceu (achado do `FuzzStream`).
  O pulado é contado (`http_skipped_bytes`, `http_resyncs`); a requisição
  atingida por uma lacuna ou pelo fim do fluxo também (`http_requests_lost`).

### 6.7 O que não é reproduzido

- HTTPS capturado: cifrado.
- HTTP/2 capturado (h2c ou TLS): é binário e multiplexado; contado em
  `http_h2c_streams` (pendência 2).
- O IP de origem (§1.2); `--pcap-xff` o leva no cabeçalho.
- As **respostas** capturadas não são comparadas com as do replay.

---

## 7. Escalonamento

- `--count` e `--duration` limitam a execução; o que vier primeiro. Sem
  `--count`, `--duration` sozinho não tem limite de contagem (até 2026-10-31 o
  padrão de 1 requisição valia também aqui, e `--duration 60s` enviava uma só:
  defeito dos três injetores, corrigido nos três). Uma fonte finita termina
  antes se acabar.
- `--rate R` agenda a requisição `seq` em `início + (seq−1)/R`: a taxa é
  **global**, e o atraso não vira rajada; fica no relatório. Sem `--rate`, o
  máximo que os workers aguentam.
- A fila tem `4 × --concurrency` posições; o produtor bloqueia quando ela está
  cheia (o backpressure do servidor chega até ele).
- Um worker reenvia primeiro o que uma conexão fechada devolveu (§4.4), depois
  tira da fila.
- **Fim**: o produtor para, a fila esvazia, cada worker empurra o buffer e
  fecha, esperando respostas e reenvios até `--drain-timeout`. O que uma conexão
  devolve no fechamento ainda tem uma tentativa, se o prazo deixar. Quem não
  conseguiu conexão até lá: `not_sent`.
- **Interrupção**: o primeiro SIGINT/SIGTERM para o produtor e drena; o segundo
  imprime o relatório parcial e sai na hora. Saída 130.

---

## 8. Relatório

### 8.1 Formas

`--report text` (padrão) ou `json`, no **stdout** e só no fim; o progresso e os
avisos vão para o **stderr**. O JSON (`report.Summary`): `info` (injetor,
execução, alvo, protocolo, fonte, plano), `start`, `elapsed_sec`, `messages`,
`ok`, `failed`, `outcomes`, `bytes` (corpos dos sucessos), `rate_msgs_per_sec`,
`rate_bytes_per_sec`, `latency` (µs: min, mean, p50, p90, p99, p99.9, max — só
dos sucessos, do `Send` à resposta final), `connections` (`opened`,
`dial_errors`, `broken`), `events` e `source` (§8.4).

### 8.2 Desfechos

| Desfecho | Sucesso | Significado |
|---|---|---|
| `200`, `201`, `202`, `204`… | 2xx | a resposta final, pelo código de status |
| `429`, `503`, `413`, `400`… | não | idem; com `--retries`, o status da última tentativa |
| `timeout` | não | sem resposta em `--response-timeout` |
| `conn_error` | não | a conexão quebrou enquanto a resposta era lida, ou a requisição foi devolvida 5 vezes por conexões que não responderam nada |
| `no_response` | não | o servidor fechou sem responder a esta (pode tê-la processado) |
| `bad_response` | não | a requisição não pôde ser montada (HTTP/2) |
| `not_sent` | não | nenhuma conexão até o fim, ou interrompido |

Eventos (`events`): erros de discagem e quebras de conexão agrupados pelo texto
do erro, `resent on a new connection` (§4.4), `behind_schedule`.

### 8.3 Progresso

Uma linha por intervalo no stderr: tempo, sucessos, falhas, taxa do intervalo,
conexões abertas. `auto` liga só num terminal.

### 8.4 Contadores da fonte

Arquivo: `file_bodies`, `file_empty_lines`, `file_long_bodies`, `file_passes`.
Captura: `pcap_*` (como no SI), `tcp_*` (direções, midstream, filtradas,
excedentes, bytes retransmitidos, fora de ordem, lacunas, `tcp_unterminated`),
`http_requests`, `http_streams`, `http_response_streams`, `http_h2c_streams`,
`http_not_http_streams`, `http_resyncs`, `http_skipped_bytes`,
`http_requests_lost`, `http_requests_too_large` e `messages_replayed`.

### 8.5 `--uuid-out`

Uma linha por resposta final: `seq status uuid` (`-` sem `X-LogHub-UUID`), na
ordem em que as respostas chegam (não em ordem de `seq`). Com os servidores
LogHub, o `uuid` é o do envelope gravado: conferir é comparar a lista com o
destino (`select uuid from loghub.inbox`), como a TESTING §3 faz.

---

## 9. Pendências conhecidas

1. **Conferência no destino** pelo próprio injetor (decisão 4): hoje fica no
   `--uuid-out` e numa consulta feita à mão.
2. **Replay de HTTP/2 capturado** (h2c): exigiria um decodificador de quadros e
   de HPACK.
3. **Ordem original dos cabeçalhos** no replay: o `net/http` entrega um mapa.
   Um parser próprio do cabeçalho a preservaria.
4. **Uma conexão por fluxo original** no replay, para que o servidor veja os
   clientes como eram (hoje as requisições se espalham pelos workers).
5. **Reenvio de `429` na fronteira de `--requests-per-conn`** ignora o
   `Retry-After` restante: a requisição devolvida sai logo na conexão nova.
6. **Candidato de ressincronização acima de 64 KiB**: como no SI, enquanto o
   cabeçalho não chegou ele não segura a decisão.
7. **Corpo chunked grande chegando aos poucos**: cada segmento novo reanalisa o
   buffer inteiro (o tamanho final não é conhecido antes do último pedaço), um
   custo quadrático no replay de uploads chunked de muitos megabytes.
