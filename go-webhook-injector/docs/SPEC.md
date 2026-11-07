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
- **Conferência da entrega no destino** pelo próprio injetor (decisão 4,
  revista em 2026-11-07): `--verify-url` (NATS JetStream, PostgreSQL, Redis ou
  Valkey) ou `--verify-sqlite`; depois do envio ele lê o que o servidor gravou
  e conta as requisições aceitas que **faltam** e as **repetidas** (§8.6), no
  relatório e no código de saída.

### 1.2 Não-objetivos (v1)

- Captura ao vivo de uma interface: a entrada do replay é sempre um arquivo.
- Reproduzir o **IP de origem** da captura (exigiria socket cru). O endereço
  original pode ir no `X-Forwarded-For` (`--pcap-xff`, §6.1), que o servidor só
  aceita de um proxy confiável.
- Replay de HTTPS capturado, HTTP/1 ou HTTP/2 (§6.7): cifrado.
- Conferir no destino um ClickHouse ou um MySQL (`sink=clickhouse`/`mysql` de
  W e F): os quatro destinos de §8.6 são os que a decisão de 2026-11-07 pediu.
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
              pipelining, Content-Length, chunked, ressincronização, e
              HTTP/2 h2c (quadros e HPACK do x/net/http2)      [fuzz]
  verify/     conferência no destino: NATS, PostgreSQL, Redis/Valkey,
              SQLite (§8.6)
  pcap/       leitor pcap/pcapng, enlaces, IPv4/IPv6, fragmentos,
              remontagem TCP                                   [fuzz]
  pcap/pcaptest/  escritor de capturas para os testes
  sched/      produtor, workers, ritmo, reenvio do devolvido, interrupção
  transport/  cliente HTTP/1.1 próprio (pipelining) e HTTP/2 (net/http)
  report/     contadores, histograma, texto/JSON, progresso
```

Dependências (decisão 6, revista em 2026-11-07): o envio, o leitor de pcap e a
remontagem continuam só na biblioteca padrão (o HTTP/2 em claro usa
`http.Protocols.SetUnencryptedHTTP2`, Go 1.24+). Entram só as bibliotecas que
os servidores já usam, **nas mesmas versões** do `go-webhook-server/go.mod`,
para ler os destinos da conferência (§8.6) — `nats.go` v1.53.1, `pgx/v5`
v5.11.0, `rueidis` v1.0.77, `modernc.org/sqlite` v1.58.0 (puro Go: o binário
continua `CGO_ENABLED=0`) — e `golang.org/x/net` v0.57.0, cujo `http2.Framer`
e `hpack` decodificam o h2c capturado (§6.8; decisão do usuário).

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
| `--pcap-flows` | `false` | uma conexão por fluxo capturado (§6.9); sem ela, as requisições se espalham pelos workers |
| `--verify-url` | — | confere a entrega no destino depois do envio (§8.6): `nats://`, `tls://`, `postgres://`, `postgresql://`, `redis://`, `rediss://`, `valkey://`, `valkeys://` |
| `--verify-sqlite` | — | idem, lendo o arquivo SQLite de um servidor com `sink=sqlite` |
| `--verify-timeout` | `30s` | depois do envio, quanto esperar até que toda requisição aceita apareça no destino |
| `--verify-stream` | automático (§8.6.2) | NATS: o stream; Redis/Valkey: a chave do stream |
| `--verify-table` | `loghub.inbox` (PostgreSQL), `inbox` (SQLite) | a tabela |
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
desconhecido; captura que não é pcap nem pcapng; `--pcap-flows` sem `--pcap`; `--verify-url` com `--verify-sqlite`;
`--verify-url` com outro esquema; `--verify-timeout`, `--verify-stream` ou
`--verify-table` sem destino de conferência; `--verify-stream` com
PostgreSQL/SQLite, `--verify-table` com NATS/Redis; `--verify-table` fora de
`nome` ou `esquema.nome` (`[a-z_][a-z0-9_]*`); `--verify-timeout` ≤ 0. Tudo
antes do primeiro envio. (Um destino de conferência que não conecta, ou não tem
o stream/tabela, também é recusado antes do envio, mas com `4`: §3.3, §8.6.1.) A mensagem de erro de `--verify-url` nunca ecoa a URL (ela
pode levar senha): diz o esquema e o motivo.

### 3.3 Códigos de saída

| Código | Quando |
|---|---|
| `0` | toda requisição teve resposta `2xx` |
| `1` | ao menos uma falhou (não-2xx ou sem resposta), ou a fonte deu erro no meio, ou a conferência no destino (§8.6) achou requisição aceita que falta ou cópia repetida |
| `2` | uso: flag inválida, combinação recusada, arquivo que não abre |
| `3` | alvo inalcançável: houve requisições e **nenhuma** conexão abriu |
| `4` | conferência no destino **impossível** (§8.6): o destino não conectou ou não tem o stream/tabela (antes de enviar: nada é enviado), não pôde ser lido até o fim, ou há aceitas sem nada que as ache (`unverifiable`) |
| `130` | interrompido (SIGINT/SIGTERM) |

Precedência quando vários valem: `130`, `3`, `4`, `1` — a mesma do
`go-syslog-injector`, para que os injetores digam o mesmo com o mesmo número:
`4` é "não deu para conferir", e perda ou duplicata no destino é falha da
execução (`1`), como uma resposta não-2xx.

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
  conexão, sem desfecho e sem duplicata (evento `resent on a new connection`).
  O mesmo vale para uma requisição que nunca chegou ao fio (a escrita falhou
  antes, ou a conexão já estava fechando ou morta), com o evento próprio
  `requeued unwritten (connection closing)` (`sched.Pending.Unwritten`): ela
  não foi "reenviada", nunca saiu.
- A vaga do pipeline só é liberada **depois** de a conexão ser marcada como
  fechando, quando a resposta traz `Connection: close`: o worker que esperava
  a vaga devolve a requisição sem escrevê-la. Antes de 2026-10-31 a ordem era
  a inversa, e cada requisição depois da 1ª de uma conexão ia a um socket que
  o servidor não lia mais (achado 3 da revisão do GI, que copiou este cliente;
  inofensivo pela RFC 9112 §9.6, mas desperdício).
- Fechamento **planejado** não é conexão quebrada (`connections.broken`): uma
  escrita que falha (`broken pipe`, `reset`) depois de a leitora ter visto o
  `Connection: close` — o pipelining escreve adiante das respostas — e o
  fechamento de uma conexão **ociosa** pelo servidor (o *idle timeout* do
  keep-alive, ou o shutdown entre requisições) viram `ErrSpent`: o worker
  reabre sem contar quebra, e o que não foi escrito volta sem desfecho. Para
  decidir, a escrita que falhou espera a leitora terminar (limitado por
  `--response-timeout`). Um prazo de escrita estourado (contrapressão) segue
  sendo erro, e uma requisição pendente quando o servidor fecha sem
  `Connection: close` continua `no_response`.
- Um servidor que fecha **toda** conexão (sem keep-alive, ou teto 1 por
  conexão) só tem uma requisição útil por conexão: a carga contra ele se faz
  com `--concurrency`, e `--pipeline > 1` só desperdiça — o escrito atrás da
  1ª é descartado pelo servidor e volta ao worker (com 32, ~30× o tráfego).
  Desde 2026-11-07 o injetor percebe esse servidor e se adapta sozinho
  (§4.6). Com teto N por conexão, cada conexão perde no máximo o que estava em
  voo atrás da N-ésima.
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
pendentes para a próxima **com o prazo e as tentativas que tinham**
(`sched.Pending.Due`/`Tries`/`Last`): a conexão nova os adota na sua lista de
reenvios (`sched.Adopter`) e os escreve quando o `Retry-After` restante vencer,
e `--retries` conta as tentativas de todas as conexões. Até 2026-11-07 a
requisição devolvida saía logo na conexão nova, ignorando o `Retry-After` que
restava, e a contagem recomeçava (pendência 5). Esgotadas as tentativas, o
desfecho é o último status. A latência registrada inclui as esperas.

### 4.6 Servidor sem keep-alive (adaptação automática)

Um servidor que responde `Connection: close` à 1ª requisição de toda conexão
(sem keep-alive, ou teto 1 por conexão) recebe um pipeline inteiro para nada
(§4.4). Desde 2026-11-07, só no HTTP/1.1:

- Quando **2 conexões seguidas** (contadas no injetor inteiro, não por worker)
  terminam com `Connection: close` na **1ª resposta**, o injetor passa a abrir
  cada conexão com **uma** requisição só, que já leva o próprio
  `Connection: close` (como `--requests-per-conn 1`): nada é escrito atrás
  dela. Evento `adapted: server without keep-alive (one request per
  connection)`. Uma conexão cuja 1ª resposta mantém a conexão zera a contagem.
  O fechamento de uma conexão ociosa (sem `Connection: close`) não conta.
- A cada **64** conexões adaptadas, uma é **sonda**: sai sem o
  `Connection: close` e com uma requisição em voo só (sem pipelining). Se a
  resposta vier sem `Connection: close`, a adaptação acaba (evento `adapted
  back: server keeps alive`) e as conexões seguintes voltam a `--pipeline` e
  `--requests-per-conn`; senão a sonda é só mais uma conexão de uma requisição.
- Com `--requests-per-conn 1` explícito não há o que adaptar. O que já estava
  em voo nas primeiras conexões segue a regra de §4.4 (volta ao worker sem
  desfecho). HTTP/2 não se adapta: um servidor h2 que fecha cada conexão manda
  `GOAWAY`, e o `net/http` já reabre.

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
passada sem nenhum corpo (arquivo vazio, só de linhas em branco, ou esvaziado
entre passadas) termina a fonte em vez de girar; o mesmo vale para uma passada
de `--pcap` sem mensagem (correção de 2026-10-31, achada na revisão do
go-gelf-injector). O stdin (`-`) não pode ser
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
  decodificado). Os demais saem **na ordem capturada**, com o nome como foi
  escrito (caixa incluída), um campo por linha, como vieram (valores repetidos
  não são juntados). Desde 2026-11-07 o extrator lê o cabeçalho com um parser
  próprio (`httpx.Request.Fields`), depois de o `http.ReadRequest` validá-lo;
  antes, o mapa do `net/http` perdia a ordem e os campos saíam em ordem
  alfabética (pendência 3). Uma linha dobrada (*obs-fold*) é juntada à anterior
  com um espaço, como o `net/http` faz. Uma requisição HTTP/2 capturada (§6.8)
  sai com os nomes em minúsculas, na ordem do bloco de cabeçalho, e os
  pseudo-cabeçalhos viram linha de requisição e `Host`.
- `Host`: o capturado (`--pcap-host keep`) ou o de `--url`. Alvo: o capturado
  (`--pcap-path keep`) ou o de `--url` com marcadores.
- `--pcap-xff`: o IP do cliente capturado é acrescentado ao `X-Forwarded-For`
  (depois dos que já vinham): o primeiro campo `X-Forwarded-For` capturado
  fica no lugar dele, com todos os valores capturados juntados por `, ` e o
  IP no fim; os outros campos `X-Forwarded-For` saem. Sem nenhum capturado, o
  campo vai depois dos capturados. O servidor só o aceita se o injetor estiver
  em `proxy.trusted`.
- Os `--header` são acrescentados depois dos capturados.

---

## 6. Replay de captura (`--pcap`)

### 6.1 O que é reproduzido

As requisições HTTP/1.x e HTTP/2 em claro (h2c, §6.8) dos fluxos TCP
**cliente → servidor** de uma captura do tcpdump, **em ordem de captura**, pelo
transporte escolhido (uma captura de HTTP/1.1 pode ser reproduzida em HTTP/2,
e uma de h2c em HTTP/1.1). Sem `--pcap-port`, toda direção que abre com uma
linha de requisição HTTP/1.x ou com o prefácio h2c (`PRI * HTTP/2.0`); com
ela, só as direções para essas portas de destino. As direções de resposta
(`HTTP/1.1 200 …`) e as de outros protocolos são contadas e descartadas.

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
ignorada; o prefácio `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n` → h2c (§6.8); outra
coisa → ignorado. Uma linha de requisição ainda sem LF segura a decisão até o
teto do cabeçalho (1 MiB), o mesmo de abaixo (até 2026-11-07, 16 KiB: ver a
ressincronização).

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
  tamanho ou no trailer, o `net/http` acusa malformação. Uma linha de tamanho
  ou de trailer acima de 4 KiB é malformação, chegue ela inteira ou aos
  pedaços.
- **Procura incremental** (desde 2026-11-07): o extrator lembra, para a
  requisição no início do buffer, até onde já procurou a linha vazia do
  cabeçalho e até onde o chunked já foi provado (o início do próximo pedaço),
  e o segmento seguinte continua dali; os bytes de dados de um pedaço não são
  relidos — o extrator só volta ao buffer quando ele alcança o fim do pedaço
  em curso. O `http.ReadRequest` roda uma vez no cabeçalho e uma vez no fim.
  Antes, cada segmento novo reanalisava o buffer inteiro (o tamanho final de
  um chunked não é conhecido antes do último pedaço): custo quadrático no
  replay de um upload chunked de muitos megabytes chegando em segmentos de
  1,4 KiB (pendência 7). A lembrança é descartada quando o início do buffer
  muda (requisição extraída ou descartada).
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
  Quando a palavra é maior que a janela (um método de 70 letras aos pedaços
  de 1 byte), o corte cai no fim do buffer, e o primeiro byte que chegar
  depois **continua** a palavra cortada: não é candidato (regra de
  2026-11-07, portada do `go-gelf-injector`, cujo `FuzzStream` achou a cauda
  do método virando uma requisição que o fluxo inteiro não tinha).
  O pulado é contado (`http_skipped_bytes`, `http_resyncs`); a requisição
  atingida por uma lacuna ou pelo fim do fluxo também (`http_requests_lost`).
- **Candidato grande** (desde 2026-11-07, pendência 6): todo limite que
  recusa um candidato vale igual para ele inteiro no buffer e para ele chegando
  aos pedaços, para que a escolha não dependa da divisão em segmentos. A linha
  de requisição sem LF segura a decisão até 1 MiB (antes, 16 KiB: uma linha de
  20 KiB vinda num segmento só era aceita, e aos pedaços era recusada no meio,
  e a próxima requisição verdadeira era perdida — o "64 KiB" do SI); um
  cabeçalho cuja linha vazia passa de 1 MiB é recusado mesmo quando chega
  inteiro (antes só aos pedaços); a linha de chunked acima de 4 KiB, idem.

### 6.7 O que não é reproduzido

- HTTPS capturado (HTTP/1 ou HTTP/2): cifrado.
- HTTP/2 de uma direção cuja captura começou depois do prefácio (midstream):
  sem o começo, a tabela dinâmica do HPACK é desconhecida, e o binário não
  tem onde ressincronizar; a direção conta como `http_not_http_streams`.
- O IP de origem (§1.2); `--pcap-xff` o leva no cabeçalho.
- As **respostas** capturadas não são comparadas com as do replay.

### 6.8 HTTP/2 em claro capturado (h2c)

Desde 2026-11-07 (pendência 2; decisão do usuário: `golang.org/x/net/http2`,
`Framer` e `hpack`, em vez de um decodificador próprio).

- Uma direção que abre com o prefácio h2c (*prior knowledge*), ou uma direção
  HTTP/1.1 que, depois de uma requisição com `Upgrade: h2c`, segue com o
  prefácio (o *upgrade* h2c), passa a ser lida como HTTP/2. A requisição do
  *upgrade* já foi reproduzida como HTTP/1.1 (ela é o *stream* 1).
- Os quadros são lidos um a um pelo `http2.Framer` quando o quadro está
  **inteiro** no buffer (o cabeçalho de 9 bytes diz o tamanho; teto de 16 MiB
  − 1, o máximo do protocolo), com `ReadMetaHeaders` e um `hpack.Decoder` por
  direção: `HEADERS` + `CONTINUATION` viram um bloco decodificado, com a
  tabela dinâmica mantida entre blocos (aceita até 1 MiB, porque o SETTINGS
  do servidor, na outra direção, pode tê-la aumentado). `DATA` de um *stream*
  é juntado (sem o *padding*) até `END_STREAM`.
- Cada *stream* do cliente vira uma requisição quando termina (`END_STREAM`,
  num `HEADERS` sem corpo, no último `DATA` ou num bloco de *trailers*, que
  não é reproduzido), em ordem de término — a ordem em que o servidor as teve
  inteiras. `:method` e `:path` fazem a linha de requisição; `:authority` (ou,
  sem ele, `host`) é o `Host`; `:scheme` sai. Os campos `cookie` repetidos
  são juntados por `; ` (RFC 9113 §8.2.3), os demais vão um por linha.
  `SETTINGS`, `PING`, `WINDOW_UPDATE`, `PRIORITY` e `GOAWAY` são ignorados.
- Um *stream* cancelado pelo cliente (`RST_STREAM`) não é reproduzido
  (`http2_streams_reset`). Um corpo acima de 64 MiB é descartado
  (`http_requests_too_large`).
- Não há ressincronização: uma **lacuna**, um quadro que o `Framer` recusa ou
  um bloco que o HPACK não decodifica encerra a leitura da direção — o estado
  do HPACK se perdeu, e nenhum bloco seguinte pode ser lido com segurança —; os
  *streams* abertos contam em `http2_streams_lost`, o erro em `http2_errors`.
- Contadores: `http_h2c_streams` (direções h2c, nome mantido; agora
  reproduzidas), `http2_requests`, `http2_streams_reset`,
  `http2_streams_lost`, `http2_errors`. `http_requests` continua contando só
  as HTTP/1.x; `messages_replayed` é o total.

### 6.9 Uma conexão por fluxo capturado (`--pcap-flows`)

Desde 2026-11-07 (pendência 4; nome da flag igual ao do `go-syslog-injector`).
Sem a flag (o padrão, para carga) as requisições da captura se espalham pelos
workers, como as de qualquer fonte. Com `--pcap-flows` o servidor vê os
clientes como eram:

- Cada direção cliente → servidor capturada (uma conexão TCP) é reproduzida
  numa conexão **própria**, aberta na 1ª requisição do fluxo e fechada depois
  da última, quando a captura mostrou o fim dele (FIN, RST, ociosidade de 10
  min de captura ou fim do arquivo). As requisições do fluxo vão nela, em
  ordem (com `--pipeline` ou `--streams`, como numa conexão qualquer).
- `--concurrency` é o máximo de fluxos com conexão aberta ao mesmo tempo.
  Quando um fluxo novo chega com todas tomadas, o fluxo que está há mais tempo
  sem requisição perde a sua (fechada depois do que está em voo); a próxima
  requisição dele, se houver, abre uma nova (evento `flow split (concurrency
  cap)`). Um `Connection: close` do servidor também divide o fluxo (§4.4).
- O fim de um fluxo chega ao produtor como um marcador da fonte
  (`source.Message.FlowEnd`), que não consome `seq` nem conta em `--count`.
  Com `--loop`, os fluxos de cada passada são novos.
- O ritmo continua sendo o do produtor (`--rate` ou `--pcap-timing`), e a fila
  de cada fluxo tem 64 posições: um fluxo lento segura o
  produtor, como a fila comum faz.

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
do erro, `resent on a new connection` (escrita atrás de um `Connection: close`,
§4.4), `requeued unwritten (connection closing)` (nunca chegou ao fio, §4.4),
`behind_schedule`, `adapted: …` e `adapted back: …` (§4.6), `flow split
(concurrency cap)` (§6.9).

### 8.3 Progresso

Uma linha por intervalo no stderr: tempo, sucessos, falhas, taxa do intervalo,
conexões abertas. `auto` liga só num terminal.

### 8.4 Contadores da fonte

Arquivo: `file_bodies`, `file_empty_lines`, `file_long_bodies`, `file_passes`.
Captura: `pcap_*` (como no SI), `tcp_*` (direções, midstream, filtradas,
excedentes, bytes retransmitidos, fora de ordem, lacunas, `tcp_unterminated`),
`http_requests`, `http_streams`, `http_response_streams`, `http_h2c_streams`,
`http_not_http_streams`, `http_resyncs`, `http_skipped_bytes`,
`http_requests_lost`, `http_requests_too_large`, `http2_requests`,
`http2_streams_reset`, `http2_streams_lost`, `http2_errors` (§6.8),
`pcap_flows_ended` (§6.9) e `messages_replayed`.

### 8.5 `--uuid-out`

Uma linha por resposta final: `seq status uuid` (`-` sem `X-LogHub-UUID`), na
ordem em que as respostas chegam (não em ordem de `seq`). Com os servidores
LogHub, o `uuid` é o do envelope gravado: conferir é comparar a lista com o
destino (`select uuid from loghub.inbox`), como a TESTING §3 faz.
Desde 2026-11-07 o próprio injetor faz essa conferência (§8.6).

### 8.6 Conferência no destino (`--verify-url`, `--verify-sqlite`)

Decisão 4, revista em 2026-11-07: o injetor confere a entrega lendo o que o
servidor gravou, com as bibliotecas e versões dos servidores (§2.1).

#### 8.6.1 Destinos

| Destino | Servidores | O que é lido |
|---|---|---|
| `nats://`, `tls://` (NATS JetStream; vírgula separa servidores, como no `nats.go`) | W, F, N com `sink=nats` | as mensagens do stream (o `Data` é o JLH, W SPEC §3.8), por um *consumer* ordenado efêmero que começa depois da última sequência do stream na checagem (§8.6.3) |
| `postgres://`, `postgresql://` (o `pgx`; `PGPASSWORD` e as demais variáveis do libpq valem) | W, F, P com `sink=postgres` | `uuid` e `payload->'content'` da fila `loghub.inbox` (W SPEC §3.6) |
| `redis://`, `rediss://`, `valkey://`, `valkeys://` (o `rueidis`; `valkey` é alias de `redis`) | W, F com `sink=redis` | o campo `e` (o JLH) das entradas do stream (W SPEC §3.3), por `XRANGE` |
| `--verify-sqlite <arquivo>` (só leitura) | W, F com `sink=sqlite` | `uuid` e `payload->content` (o `payload` do JLH, em texto) da tabela `inbox` (W SPEC §3.7) |

Antes do primeiro envio o injetor conecta e confere que o stream NATS ou a
tabela existem (`--verify-timeout` limita essa espera); se não, sai com `4` sem
enviar nada — uma conferência impossível descoberta depois de uma carga de uma
hora desperdiçaria a carga. No Redis só a conexão (`PING`) e, com
`--verify-stream`, que a chave é um stream ou ainda não existe: o stream nasce
no primeiro `XADD` do servidor. Nessa checagem o injetor anota também o ponto
de partida dos streams (§8.6.3). Com `sink=sqlite` e `rotate=daily|hourly`,
aponte o arquivo do período da execução.

#### 8.6.2 Nome do stream, da chave e da tabela

- NATS, sem `--verify-stream`: o stream que existir entre
  `LOGHUB_WEBHOOK_REQUEST` (padrão do W) e `LOGHUB_WEBHOOK` (F e N); os dois, ou
  nenhum, é erro que pede `--verify-stream`.
- Redis/Valkey, sem `--verify-stream`: todas as chaves do tipo stream que casam
  `loghub.*.webhook*` (`SCAN … TYPE stream`, refeito a cada leitura): o padrão
  `loghub.{space}.{type}` de W e F, em qualquer Workspace. Nenhuma achada
  deixa as aceitas como `lost`. Num Redis Cluster o `SCAN` vê um nó só: dê
  `--verify-stream`.
- PostgreSQL: `loghub.inbox`; SQLite: `inbox`. O nome é citado como
  identificador (`pgx.Identifier`, aspas no SQLite), nunca colado cru no SQL.

#### 8.6.3 Janela e marcadores

- **Ponto de partida dos streams** (regra de 2026-11-07, portada do
  `go-gelf-injector`): NATS e Redis/Valkey são lidos **depois da última
  entrada que o stream tinha na checagem de antes do envio** — a sequência do
  JetStream (`DeliverByStartSequence`, última + 1) e, em cada stream do Redis
  que já existia, o ID do `XREVRANGE … COUNT 1` (a leitura continua com
  `(<id>`); um stream que nasce depois da checagem é lido inteiro. Tudo da
  execução vem depois, sem relógio nenhum, e nada do passado é lido. Antes era
  a janela de tempo abaixo também para eles, e num stream movimentado ela relia
  tudo do último minuto a cada conferência (o GI achou ~250 000 mensagens numa
  janela de 10 min, e a leitura passava do `--verify-timeout`). Limite: um
  stream NATS apagado e recriado entre a checagem e o fim do envio recomeça as
  sequências, e o que foi gravado nele antes da última sequência anotada não é
  lido (conta `lost`).
- **Janela** (PostgreSQL e SQLite, que não têm ordem de chegada): só registros
  a partir de **1 min antes** do início da execução (folga para o relógio),
  pelo tempo do UUIDv7 (`uuid >= <limite>`). Registros mais antigos não são
  lidos.
- **Aceitas**: cada resposta final `2xx` registra o `seq` e o `X-LogHub-UUID`
  (o uuid do envelope gravado). São as **esperadas**.
- Um registro é **da execução** quando o seu `uuid` é de uma aceita, ou quando
  o corpo (`payload.content.body`, desfeito o base64 se `encoding=base64`) é um
  objeto JSON com `"run"` igual a `--run` e um `"seq"` inteiro — o corpo do
  gerador (§5.1), e todo modelo que leve `"run":"{run}"` e `"seq":{seq}`.
  Registros de outras execuções e de outros clientes são ignorados.

#### 8.6.4 Espera e contagem

Depois do fim do envio (o dreno de §7), o injetor lê o destino; enquanto faltar
alguma aceita, lê de novo (o NATS, o Redis e o SQLite continuam de onde
pararam: *consumer*, ID, `rowid`; o PostgreSQL relê a janela, porque a fila
não tem ordem de inserção, e o que já foi visto é ignorado pelo `uuid`), com
intervalos de 250 ms dobrando até 2 s, até `--verify-timeout`. Um sink que
grava depois do `200` (`ring=memory`, ou `ring=sqlite` com o sink atrasado) tem
esse prazo para alcançar.

| Contador | Significado |
|---|---|
| `expected` | requisições aceitas (`2xx`) |
| `found` | aceitas achadas no destino: pelo `uuid`; uma aceita sem `X-LogHub-UUID`, pelo `seq` do corpo |
| `lost` | `expected − found` |
| `duplicated` | cópias além da primeira: registros da execução agrupados pelo `seq` do corpo (ou, sem marcador, pelo `uuid`), somando `n − 1` de cada grupo. Pega o reenvio que o servidor processou duas vezes (dois `uuid`, o mesmo `seq`) e o mesmo envelope gravado duas vezes (um `XADD` repetido) |
| `unexpected` | `seq` da execução no destino cuja requisição **não** teve `2xx` (um `no_response` que o servidor processou, por exemplo). Informativo: não falha |
| `lost_seqs` | os `seq` das perdidas, em até 20 faixas (`5`, `7-9`; `...` se houver mais) |
| `complete` | `true` quando a espera terminou porque toda aceita foi achada (não pelo prazo nem por erro) |
| `waited` | segundos de espera, leituras incluídas |
| `unverifiable` | aceitas sem `uuid` e sem marcador no corpo (servidor que não é LogHub, com modelo sem `seq`/`run`): não há como achá-las |
| `records` | registros da execução lidos |

Quando a contagem fica completa, o injetor lê **mais uma vez** 1 s depois: uma
cópia repetida gravada depois da última aceita passaria despercebida se a
conferência parasse na primeira leitura completa (achado do SI contra o
PostgreSQL). O NATS lê com `Fetch` + `FetchMaxWait` e ignora as sequências do
stream já contadas (o `FetchNoWait` do *consumer* ordenado recomeça do início a
cada chamada: achado do SI).

`lost` ou `duplicated` maiores que zero são **falha** da execução (saída `1`);
`unverifiable` maior que zero ou um erro de leitura (`error`) tornam a
conferência **impossível** (saída `4`, §3.3). No relatório JSON é o objeto
`verify` (`destination` só com esquema, host e nome, sem credenciais; os
contadores; `error`), com os nomes do relatório do SI onde o sentido é o mesmo;
no texto, um bloco `verify:`. No PostgreSQL e no SQLite o `uuid` é chave primária, então
`duplicated` ali só vem de `seq` repetido.

Interrompido (SIGINT), o injetor não confere: sai com `130`.

**Precisa de um destino parado**: um worker que consome a `loghub.inbox`
(`DELETE … SKIP LOCKED`), o `MAXLEN` do Redis, os limites de um stream NATS ou
o `keep_files` do SQLite apagam o que a conferência procura, e o que sumiu
conta como `lost`. A memória é ~100 bytes por requisição aceita.

---

## 9. Pendências conhecidas

1. *Corrigido em 2026-11-07:* conferência no destino pelo próprio injetor
   (`--verify-url`, `--verify-sqlite`); regra em §8.6.
2. *Corrigido em 2026-11-07:* replay de h2c capturado com o `x/net/http2`
   (`Framer` + `hpack`); regra em §6.8.
3. *Corrigido em 2026-11-07:* ordem original dos cabeçalhos no replay (parser
   próprio); regra em §5.5.
4. *Corrigido em 2026-11-07:* uma conexão por fluxo capturado
   (`--pcap-flows`); regra em §6.9.
5. *Corrigido em 2026-11-07:* reenvio de `429` na fronteira de
   `--requests-per-conn` respeita o `Retry-After` restante e as tentativas;
   regra em §4.5.
6. *Corrigido em 2026-11-07:* candidato de ressincronização grande decide igual
   inteiro ou aos pedaços; regra em §6.6.
7. *Corrigido em 2026-11-07:* chunked grande chegando aos poucos sem a
   reanálise quadrática; regra em §6.6.
8. *Corrigido em 2026-11-07:* adaptação automática a servidor sem keep-alive;
   regra em §4.6.
9. **h2c midstream e h2 depois de uma lacuna** não são reproduzidos (§6.7,
   §6.8): sem o começo do HPACK não há como decodificar com segurança. É
   limite do protocolo, não do extrator.
10. **Conferência em ClickHouse e MySQL** (sinks de W e F) não existe (§1.2):
    fora do pedido de 2026-11-07.
11. *Corrigido em 2026-11-07 (porte entre injetores):* palavra maiúscula maior
    que a janela de ressincronização não vira candidato pela cauda (§6.6, do
    GI); NATS e Redis/Valkey conferidos depois da última entrada na checagem
    (§8.6.3, do GI).
