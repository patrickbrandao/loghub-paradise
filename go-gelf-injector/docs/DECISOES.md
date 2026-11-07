# go-gelf-injector — Decisões de projeto

Registro das decisões que criaram o `go-gelf-injector` (GI) em 2026-10-31,
quarto injetor do monorepo. As decisões 1–19 são as dos injetores de
2026-10-30/31 (`go-syslog-injector`, SI, o pai do código; `go-relplog-injector`,
RI; `go-webhook-injector`, WI), copiadas e adaptadas; as decisões G1–G12 são
as próprias do GELF. A entrada comum da família GELF é `tasks/gelf/DESIGN.md`
(transitório): a decisão 18 de lá é o escopo deste módulo. Cada decisão
aparece na `docs/SPEC.md` com a seção.

O pedido do usuário (2026-10-31): "baseado nos modelos de syslog e relp, crie
os softwares semelhantes para atender o GELF. Todos os softwares de ingestão e
injeção de syslog e relp devem existir em versão semelhante para GELF."

## 1. Decisões herdadas dos injetores (entrevista de 2026-10-30)

| # | Tema | Decisão em GI |
|---|---|---|
| 1 | Empacotamento | Um módulo por família: este é o injetor da família GELF (`go-gelf-server`, `go-gelf-pg-server`, `go-gelf-nats-server`). Código copiado de SI e WI, nada importado |
| 2 | Alcance | Funcional + carga: N mensagens ou por duração, taxa alvo, concorrência; relatório com desfechos, vazão e latência p50/p90/p99 |
| 3 | Conteúdo | Gerador com `_seq`, modelo com marcadores, arquivo ou stdin, **replay de captura do tcpdump** (pcap/pcapng) |
| 4 | Conferência no destino | **Revista pelo usuário em 2026-11-07**: é objetivo. `--verify-url` (`nats://`, `tls://`, `postgres://`, `postgresql://`, `redis://`, `rediss://`, `valkey://`, `valkeys://`) ou `--verify-sqlite`, mais `--verify-timeout`: depois do envio, conta perdidos e duplicados pelos `_seq` da execução, no relatório e no código de saída (4 e 5). Com as bibliotecas e versões dos servidores. SPEC §10 (antes: "não na v1, o relatório vem do transporte") |

## 2. Padrões herdados (contestáveis)

| # | Tema | Decisão em GI | Por quê |
|---|---|---|---|
| 5 | Interface | Só flags, relatório no stdout, progresso no stderr, saída 0/1/2/3/130 | ferramenta de linha de comando para scripts e CI |
| 6 | Dependências | Fora da stdlib, **só** as da conferência no destino (`nats.go`, `pgx/v5`, `rueidis`, `modernc.org/sqlite`, nas versões do `go-gelf-server/go.mod`), só em `internal/verify`; binário estático `CGO_ENABLED=0` (revista em 2026-11-07; antes: nenhuma) | o leitor de pcap, o gzip/zlib e o cliente HTTP da stdlib bastam para enviar; ler o destino exige o cliente de cada um, e o mesmo do servidor lê exatamente o que ele grava |
| 7 | Alvo padrão | `tcp` em `127.0.0.1:12201`; por transporte: udp `12201`, tls `12203`, http `12202` (`/gelf`), https `12204` | as portas dos servidores GELF (DESIGN decisão 5) |
| 8 | Transporte padrão | `tcp` | mostra erro de conexão (o UDP falha em silêncio) |
| 9 | Gerador | Documento GELF 1.1 com `short_message` `seq=N run=R injector=go-gelf-injector` e os campos adicionais `_seq` e `_run` | buraco no destino é perda, repetido é retransmissão; `_seq` numérico permite contar no banco sem analisar texto |
| 10 | Replay | Mensagens em ordem de captura; o transporte original é desembrulhado (remontagem, descompressão, moldura) e o escolhido reembrulha; seleção estrita por conteúdo sem `--pcap-port` | uma captura de servidor real mistura protocolos |
| 11 | Captura no meio | Direção sem SYN ressincroniza na primeira posição comprovada | emissores GELF/TCP mantêm conexões por dias |
| 12 | Ritmo do replay | `asap` ou `original` com `--pcap-speed` | vazão e forma do tráfego são usos diferentes |
| 13 | PROXY protocol | v1/v2 opcional em tcp, tls, http e https | os servidores GELF aceitam PROXY de par confiável nos três listeners de fluxo (DESIGN decisões 12 e 13) |
| 14 | Concorrência | N workers, uma conexão (socket) cada; taxa global | o modelo de qualquer emissor real |
| 15 | Contrato de desfecho | Um desfecho por mensagem, sempre (`sched.Conn`) | sem ele `mensagens = ok + falhas` não fecha |
| 16 | Cobertura | GI é o injetor de G, GP e GN | os três falam o mesmo protocolo |
| 17 | HTTP "envio extremo" | Cliente HTTP/1.1 próprio do WI com **pipelining** e `--requests-per-conn` | o `http.Transport` não faz pipelining |
| 18 | HTTP reenvio | `--retries` só para `429`/`503`; o que o servidor garantidamente não processou volta ao worker; nada que ele possa ter processado é reenviado | `POST` não é idempotente |
| 19 | Chaves no modelo | A regra do WI: `{` só abre marcador quando parece um, `}}` é literal | o documento GELF é JSON |

O defeito de 2026-10-31 dos três injetores (`--duration` sem `--count`
enviava uma mensagem) nasce corrigido aqui, com o mesmo teste
(`TestDurationWithoutCount`).

## 3. Decisões próprias do GELF (2026-10-31, sem entrevista)

Pela regra do usuário de 2026-10-10, o que se aprende dos irmãos e da
especificação GELF é decidido aqui.

| # | Tema | Decisão | Por quê |
|---|---|---|---|
| G1 | Transportes | `udp`, `tcp`, `tls`, `http`, `https`. **Sem** unix datagrama (SI) e **sem** HTTP/2 (WI); HTTP/1.1 com `Content-Length` ou, com `--http-chunked`, `Transfer-Encoding: chunked` (2026-11-07) | os servidores GELF não escutam unix; G/GP/GN são só HTTP/1.1 — conferido em 2026-11-07: ALPN `http/1.1` e `TLSNextProto` vazio no `go-gelf-server` (SPEC §4.5 dele), sem h2c —, e um h2 recusado não testa nada (SPEC §1.2) |
| G2 | UDP: chunking | `--chunk-size` (padrão **8192**, como o Graylog e as bibliotecas em LAN) é o **datagrama inteiro**, cabeçalho de 12 bytes incluído. Mensagem (já comprimida) até esse tamanho vai em um datagrama sem chunk; acima, em chunks `0x1e 0x0f`, id de 8 bytes aleatório, `seq`, `count`. Mais de **128** chunks: **recusada antes de enviar**, desfecho `too_large` | o receptor descarta `count > 128`; mandar 129 chunks gastaria rede para nada |
| G3 | Compressão | `--compress none\|gzip\|zlib`; padrão **gzip no UDP** (o Docker e o Graylog fazem assim), **none** no resto; no TCP/TLS é **recusada** (uso) | a spec GELF não comprime no TCP: o `\0` de moldura pode aparecer no comprimido |
| G4 | HTTP: compressão | `gzip` → `Content-Encoding: gzip`; `zlib` → `Content-Encoding: deflate` (RFC 9110 §8.4.1.2: "deflate" é o formato zlib) | é o que o servidor decodifica (DESIGN decisão 13) |
| G5 | TCP: moldura | Documento + `\0`. Um documento com byte NUL é **recusado** (`nul_byte`), não enviado | o servidor o dividiria em dois documentos inválidos, em silêncio |
| G6 | HTTP: sucesso | Qualquer `2xx` é sucesso (os servidores LogHub respondem `202`); `503` com `Retry-After` é reenviável com `--retries`; `413` e os demais `4xx` são finais | regra do WI e da DESIGN decisão 13 |
| G7 | Tamanho | `--payload-size N` completa o documento do **gerador** com um campo `"_pad"` de `[a-z0-9]` **aleatórios** até `N` bytes | padding constante (`xxxx`) cabe em qualquer datagrama depois do gzip, e o chunking nunca seria exercitado |
| G8 | Arquivo | Um documento por linha, enviado como está (sem validar) | testar o servidor com malformados de propósito |
| G9 | Replay UDP | Datagrama com o magic é chunk: remontagem por (origem, id), prazo de 5 s em tempo de captura (conferido também no chunk que completaria), duplicata ignorada (só a cópia idêntica: um chunk diferente sob o id de uma mensagem completa há menos de 5 s é de uma mensagem nova com id reusado — revisão de 2026-10-31), `count`/`seq` inválidos descartados e contados; depois a descompressão pelos primeiros bytes (gzip `1f 8b`, zlib `78 xx` com checksum, senão JSON) | as mesmas regras que os servidores aplicam (DESIGN decisão 11) |
| G10 | Replay TCP | Uma direção é decidida pelos primeiros bytes: `{` (depois de espaços ou `\0`) é GELF com moldura `\0`; linha de requisição HTTP/1.x vai ao extrator do WI; o resto é ignorado (estrito) ou lido como GELF (com `--pcap-port`). Midstream: o primeiro `{` precedido de `\0` e só espaços, ou o fim de uma linha de requisição (` HTTP/1.` + dígito + `CR`/`LF`: sem o fim de linha, o texto aparece em logs de acesso dentro do JSON — revisão de 2026-10-31), decidido num único byte sem olhar adiante | a decisão não pode depender da divisão em segmentos (lição do `FuzzStream` do SI) |
| G11 | Replay HTTP | O corpo de cada `POST` (decodificado por `Content-Encoding` ou pelos primeiros bytes) é um documento; outro método ou corpo vazio é contado e pulado; no modo estrito, também o caminho que não é `/gelf` nem termina em `/gelf` (revisão de 2026-10-31: webhooks e `_bulk` da mesma captura viravam GELF) | o input HTTP do Graylog é um documento por requisição |
| G12 | Último quadro TCP | No fim de uma direção **fechada** na captura (`FIN` com todos os bytes, ou `RST` sem lacuna), o quadro sem `\0` é documento como o servidor o leu no EOF (estrito: basta começar com `{`). Numa direção **sem fim** (a captura parou, ou ociosa), só se começar com `{` e terminar com `}`; senão `frames_lost` (revista em 2026-11-07; antes a 2ª regra valia para as duas) | o servidor aceita no EOF e no erro de leitura (DESIGN decisão 12; SPEC §5.3 do G); só a captura cortada pode ter cortado o quadro em qualquer ponto |

## 4. Decisões da rodada de 2026-11-07 (pendências conhecidas)

Do usuário (entrevista da rodada Valkey de 2026-11-07): conferência no destino
(decisão 4 revista), uma conexão por fluxo no replay com o PROXY da origem,
dedup do `-i any`, HTTP/2 só se o G aceitar e HTTP/1.1 `chunked`, último quadro
TCP, adaptação a servidor que fecha toda conexão; fragmentos IP numa captura
real deixados abertos se não puderem ser provados localmente. O resto foi
decidido aqui, pelo código e pelos irmãos:

| # | Tema | Decisão | Por quê |
|---|---|---|---|
| G13 | Códigos de saída da conferência | Os do `go-syslog-injector`: `4` = conferência impossível (o destino não abriu antes do primeiro envio, e nada é enviado; ou nenhuma leitura deu certo); perda, duplicata ou `seq` desconhecido no destino = `1`. Precedência `130`, `3`, `4`, `1`. Flags `--verify-space`/`--verify-key` e campos `waited`/`lost_seqs` também alinhados | os injetores falam a mesma língua para scripts; uma URL errada não pode custar uma carga |
| G14 | Janela e espera da conferência | Streams (NATS, Redis) lidos depois da última entrada no momento da checagem, sem relógio; PostgreSQL e SQLite a partir do início da execução **menos 10 min** pelo UUIDv7 (relógios de máquinas diferentes); a cada 250 ms, até achar tudo e mais uma leitura **1 s** depois (como o SI), ou `--verify-timeout` (30 s) | o UUIDv7 vem do relógio do servidor; a janela por tempo num stream movimentado lia ~250 000 mensagens a cada conferência; uma duplicata do at-least-once chega logo atrás |
| G15 | Dedup do `-i any` | Chave do SI (porta de origem, identificação IP, tamanho, FNV-1a; sem endereços, que o NAT reescreve), janela de 50 ms, `--pcap-dedup auto\|on\|off`; em `auto`, pelo ponto de captura (interface pcapng, índice e tipo do pacote do Linux cooked): a cópia que leva a contagem do seu ponto acima da dos outros é mensagem nova | separa a cópia da captura (outro ponto) da repetição do emissor (mesmo ponto), que não pode sumir mesmo com a mesma identificação IP; conferido numa captura real de ponte Docker (SPEC §6.3.1) |
| G16 | Fluxos | `--pcap-flows`: fila, worker e conexão por par (origem, destino) capturado, fechada no fim do fluxo na captura (marcador de fim em ordem, como o `FlowEnd` do SI); teto de `--concurrency` × 256 fluxos abertos (os 256 por worker do SI; o mais antigo fecha) | é o que o servidor veria dos emissores originais; o teto limita descritores |
| G17 | Adaptação | 2 conexões seguidas fechadas na 1ª resposta → pipeline 1 nas novas; uma 2ª resposta restaura | um fechamento isolado é teto ou shutdown |
