# go-gelf-nats-server — Decisões de projeto

Registro das decisões que definiram o `go-gelf-nats-server` (letra **GN**), o
corte NATS do servidor GELF da família LogHub. A entrada comum da família é
`tasks/gelf/DESIGN.md` (2026-10-31, fora do git); este arquivo copia dali o que
vale para GN e acrescenta o que foi decidido no corte. Depois da SPEC, a
`docs/SPEC.md` é a fonte da verdade, e cada decisão abaixo aparece nela com o
número da seção.

## O pedido

> Baseado nos modelos de syslog e relp, crie os softwares semelhantes para
> atender o GELF. Todos os softwares de ingestão e injeção de syslog e relp
> devem existir em versão semelhante para GELF.

O par do `go-syslog-nats-server` (SN) é este módulo. A regra da família (DESIGN
§1): **GN é para o `go-gelf-server` (G) exatamente o que SN é para o
`go-syslog-server` (S)** — ring só em memória, sink só NATS (`jetstream` padrão
e `core`), beacon mantido, **sem TLS em lugar nenhum**, sem filtro, sem
rate-limit, sem subjects granulares; o resto (admin, `LOGHUB_EB_URL`, `SIGHUP`,
PROXY protocol, UDP + TCP + HTTP) fica.

## Estado do monorepo

| Pasta | Papel para GN |
|---|---|
| `go-gelf-server/` (G) | **Pai do comportamento GELF.** Conversão, remontagem de chunks, descompressão, moldura `\0`, recepção HTTP. Os pacotes `internal/convert`, `internal/item`, `internal/jlhcheck` e `internal/receiver/{chunks,decompress,framing}.go` são **cópia literal** (só o module path muda). Nada é importado. |
| `go-syslog-nats-server/` (SN) | **Pai estrutural.** O corte (ring memory, sink NATS, beacon, dump JSON Lines, `removed.go`, a guarda contra a promoção automática a TLS do `nats.go`) vem de lá, com as rodadas de porte de 2026-10-11/13/14. |
| `go-gelf-pg-server/` (GP) | O irmão: o mesmo corte com a fila `loghub.inbox`. Onde SN e SP não diferem, GN e GP não diferem (o `receiver/http.go` sem HTTPS é o mesmo nos dois). |
| `go-gelf-injector/` (GI) | Cliente de carga e funcional. |

## Fontes normativas

| Fonte | Versão | Papel |
|---|---|---|
| GELF 1.1 (Graylog) | 1.1 | Documento JSON, chunks UDP, compressão, `\0` no TCP, `POST /gelf` |
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH, subject `loghub.{space}.{type}`, `Nats-Msg-Id = <uuid>.<step>`, plano de controle |
| `jlh-gelf-payload.schema.json` | deste repositório (cópia de G) | Não existe skill `type=gelf`: o schema é o da família (decisão 1) |
| `go-loghub-uuidv7` v0.0.3, `go-loghub-ident` v0.5.2 | | `uuid` nível 3, identidade |

---

## 1. Decisões do usuário (entrevista da família, 2026-10-31)

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| 1 | Envelope | `type=gelf`, payload estruturado em kebab-case, subject `loghub.{space}.gelf`. Schema próprio (`jlh-gelf-payload.schema.json`, `additionalProperties: false`). | §3.2 |
| 2 | Transportes | UDP + TCP + HTTP. **GN sem TLS**, como SN: nem TCP+TLS, nem HTTPS, nem TLS até o NATS. | §1.2, §4 |
| 3 | `level` ausente | **6 (info)** com aviso `level-default` (D1). | §6.5 |
| 4 | `raw` | Só quando malformado, com aviso de encoding ou com perda; `convert.keep_raw = malformed` (padrão) \| `always`. | §6.9 |

## 2. Padrões assumidos (contestáveis)

### Da família (DESIGN §3), que valem aqui

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| 5 | Portas | `listen.udp = :12201`, `listen.tcp = :12201`, `listen.http = :12202`. Não há `listen.tls`/`listen.https`. | §4.1 |
| 6 | Admin | `127.0.0.1:9104` (o de G: não rode G e GN no mesmo host sem trocar um deles). | §14 |
| 7 | Nomes | env `GELF_<SEÇÃO>_<CHAVE>` (`GELF_CONVERT_KEEP_RAW`), arquivo `GELF_CONFIG`; métricas `gelf_`; stream `LOGHUB_GELF` (`loghub.*.gelf`), nunca criado pelo servidor; Docker `gelf-nats-*`. | §16.2 |
| 8 | Arquitetura | Cru no ring, conversão no dreno; o cru é o documento já remontado e descomprimido. A recepção **não lê** o documento (sem filtro). | §2.2 |
| 9 | Cabeçalho do item | O de G (com `compression`, `chunks`), em memória só. | §3.4 |
| 10 | Tamanho | `convert.max_message_bytes` = 128 KiB do documento descomprimido; acima, recusa contada, nunca truncamento; pior JLH 776 KiB, cabe no `max_payload` de 1 MiB. | §3.9, §5.5 |
| 11 | UDP | Chunks, remontagem por `(src-ip, src-port, id)`, 5 s, tetos globais, compressão pelos bytes. | §5.2 |
| 12 | TCP | Moldura `\0`, backpressure por parar de ler, PROXY opcional. | §5.3 |
| 13 | HTTP | `POST /gelf`, um documento por requisição (ou por linha com `http.bulk`), `202` depois do ring, `503` + `Retry-After` quando pode passar depois, `4xx` só para o que nunca passa. **Keep-alive sim** (HTTP/1.1 persistente, pipelining sequencial da stdlib), **HTTP/2 não** (nem h2c). Corrigido em 2026-10-31 (porte de G): a primeira versão desligava o keep-alive, o que o DESIGN não queria dizer. | §4.5 |
| 14 | Conversão nunca falha | Malformado vira envelope com `raw`. | §6.1 |
| 15 | Filtro e rate-limit | **Não existem aqui** (como SN). | §7, §8 |
| 16 | Subjects NATS | Só `loghub.{space}.gelf`. | §3.8 |
| 19 | Versão | v1.0.0. | — |

### Do corte (as de SN, com os nomes de G)

- **O sink é o NATS de SN** (que é o de S sem TLS e sem subjects granulares):
  uma conexão por processo, preguiçosa, `MaxReconnects(-1)`,
  `ReconnectBufSize(-1)`, `WithExpectStream`, `WithRetryAttempts(0)`,
  `WithPublishAsyncTimeout`, `PublishAsyncMaxPending = 2 × workers × batch`,
  validação do stream no boot e em toda checagem de saúde, tabela de classes de
  erro (§10.7).
- **Sem TLS, de propósito, e com guarda:** o `TLSCertCB` que recusa e
  `errNATSServerRequiresTLS` (fatal) impedem a promoção automática do `nats.go`
  diante de `tls_required` (§10.7); `tls://`, `ws://`, `wss://` são exit 1 em
  `sink.nats.urls` e na `LOGHUB_EB_URL`.
- **Nome do cliente NATS** `glnats/<AgentName>@<Hostname>`.
- **Emergência em JSON Lines** (a decisão 3 do `go-syslog-pg-server`, herdada
  por SN): um JLH pronto por linha, reingerido sem conversão e em fluxo; a porta
  `emergency.ParseLine` exige o cabeçalho byte a byte e `short-message` string
  (§12).
- **Chaves de G que saíram:** na linha de comando ou no INI, parâmetro
  desconhecido (exit 1, dizendo que é do `go-gelf-server`); no ambiente, um
  `WARN` que nomeia todas (`internal/config/removed.go`, 93 nomes).
- **Docker:** portas do host distintas das de G e GP: GELF `12281` (udp/tcp),
  HTTP `12282`, admin `127.0.0.1:19281`, cliente NATS `127.0.0.1:14291`, monitor
  `127.0.0.1:18291`. Emissor de demonstração fluent-bit no perfil `demo`.

### Tomadas no corte (2026-10-31)

| Tema | Decisão | Por quê | SPEC |
|---|---|---|---|
| `internal/item` | Cópia literal de G, codec incluído; `ItemOverhead` = 288 B (o do GP), não 256 B (o de SN) nem 384 B (o de G) | A regra da família (G AGENTS §3) pede o pacote literal em GP/GN; a struct de G ainda tem `RowID`, `StreamID`, `done` e `TLS`, e o teto precisa cobri-la. Era 384 até a revisão dos cortes de 2026-10-31: cobrava o canal `done` (112 B) que nenhum ring daqui cria, e a mesma `ring.max_bytes` guardava ~11 % menos itens que no GP | §9.2, §17.1, §22.1 |
| `receiver/http.go` | O de G sem HTTPS, igual ao de GP | "Sem TLS em lugar nenhum" vence o literal; GP fez o mesmo corte | §4.5, §17.1 |
| `obsTransport` | Mapeia o transporte do item ao label por **nome**, explícito: `tls`→`tcp`, `https`→`http` (nunca produzidos aqui; só para a função ser total). A mesma função do GP desde 2026-10-31 | O item tem `tls`/`https`, o `obs` só `udp`/`tcp`/`http`; antes os dois caíam em `udp` aqui e em `tcp`/`http` no GP | §14.1 |
| `ParseLine` | Números pela gramática JSON (não só inteiros ≤ 20 dígitos) e profundidade 66 (não 8) | O payload GELF guarda o texto dos números de `fields`/`extra` (`1.50`, `1e400`) e os valores de `extra` aninhados como vieram; a regra de SN recusaria um dump legítimo | §12.2 |
| Leitor do dump unificado com o GP (2026-10-31) | O código de `emergency.ParseLine` é o do GP (`maxDepth = 3 + 63`, `numberLen`, checagem de `short-message`); comentários e `MaxLine` continuam os de cada corte. Os motivos de recusa dizem `not type=gelf at step 0` e `level is not a GELF level (0..7)` (diziam `syslog`) | Duas redações do mesmo código: a próxima correção num não chegaria ao outro (revisão dos cortes, §2.4) | §12.2 |
| `context.Canceled` no `Enqueue` | `Shutdown`, como G (em SN, `RingFull`); o GP passou a ter o mesmo ramo em 2026-10-31 | Um `POST` em voo no shutdown recebe `503` na hora | §13.4 |
| Keep-alive HTTP (2026-10-31, porte de G) | `http.idle_timeout` (60 s, nunca 0) entre requisições; `http.max_requests_per_conn` (0 = sem teto, negativo exit 1) põe `Connection: close` na N-ésima resposta; o `503` de backpressure mantém a conexão, o de shutdown fecha, e no shutdown toda resposta leva `Connection: close`. Restart | Uma conexão por documento custava um handshake e um `TIME_WAIT` por evento (20 000 requisições, 20 000 conexões no e2e de G) | §4.5 |
| `http.bulk` (2026-10-31, porte de G, **sem entrevista, contestável**) | Padrão `false`, como o `enable_bulk_receiving` do Graylog. Com `true`: corpo dividido em `\n` (CRLF aceito), linha vazia ignorada, um `uuid` por linha com a mesma amostra de relógio; tamanho tudo ou nada (`413` sem enfileirar nada); ring cheio no meio → as anteriores ficam, `503` (reenviar duplica). Corpo descomprimido até `http.bulk_max_bytes` (4 MiB, 1 KiB..64 MiB; abaixo de `L` vale `L`). Restart | O output `http` do fluent-bit manda um *flush* inteiro num corpo; fecha a pendência §22.1 item 2 | §4.5.1 |
| Prazo de escrita HTTP (2026-10-31, porte de G) | 10 s **contados da resposta**: o handler desarma o prazo que o `net/http` arma no fim dos cabeçalhos e o rearma antes de cada resposta | Corpo lento passava de 10 s: o documento entrava e o `202` se perdia (o cliente reenviava com outro `uuid`); correção do W de 2026-10-14 | §4.5 |
| `chunk_overflow` (2026-10-31, porte de G) | Uma vez por mensagem: a mensagem recusada pelo teto de bytes vira lápide (nova ou aberta); com `max_messages` cheio, cada chunk conta | Antes, até `count` descartes por mensagem e uma dupla contagem com `chunk_timeout`, justamente na saturação | §5.2 |
| Documento vazio no dreno (2026-10-31, porte de G) | O montador de pedaços recusa e conta como `malformed` um item de 0 byte antes de chamar o conversor | Pré-condição do `convert.Append` (`size` ≥ 1); aqui nenhum caminho produz um, então é a pré-condição escrita | §6.1 |

## 3. Decisões herdadas

### De G (`../go-gelf-server/docs/DECISOES.md`)

Valem todas as do GELF (parser próprio por varredura, profundidade 64, BOM é
malformado, surrogate solto vira U+FFFD, 25 avisos, `level` inválido = 6 com
`level-invalid`, `timestamp` decimal, `compression` real, descompressão inválida
é `decompress`, documento vazio, teto do corpo comprimido `L + 16 KiB`, prazo
fixo de 5 s, `Content-Encoding` desconhecido `415`, `convert.accept_malformed`).
Não valem: TLS/HTTPS, filtro, rate-limit, `HTTP Consumed` (era do `ring=none`),
HTTP filtrado `202`, as colunas de `gelf_events` e o ClickHouse.

### De SN (`../go-syslog-nats-server/docs/DECISOES.md`)

Valem as decisões 1 (beacon fica), 2 (sem TLS), 3 (os dois modos de publicação)
e 4 (sem subjects granulares, aqui já ausentes em G), e todas as rodadas de
porte (§6–§10 de lá: segredos fora das mensagens, veredito em todo `Write`,
contrato do stream em toda checagem, `max_bytes` com `discard: new`, recusas de
autenticação fatais, `rename` sem *hard link*, temporário de dump informado,
tetos de `sink.workers` (256) e `sink.batch`, `retention: interest` fatal, prazo
da reingestão sem progresso, X1/X2/X3, pendências de 2026-10-14). O que era
específico do syslog (unix datagram, moldura octet-counting, charset legado,
fuso RFC 3164, `default_priority`, tetos de STRUCTURED-DATA, truncamento) não
existe aqui.

---

## 4. Riscos conhecidos

- **Sem TLS até o NATS.** Credenciais e eventos trafegam em claro: o NATS
  precisa estar numa rede confiável. Um servidor que exige TLS é recusado no
  boot (exit 3).
- **Sem TLS na entrada.** GELF cifrado exige um terminador na frente (com PROXY
  protocol para manter o `src-ip`) ou o `go-gelf-server`.
- **UDP perde por desenho**, e um chunk perdido descarta a mensagem inteira.
- **`kill -9` perde o backlog do ring.** O dump cobre FATAL e `SIGTERM`.
- **`core` é *best effort*.**
- **Dump grande na saída**: até ~6× o ring no pior caso; a reingestão lê em
  fluxo, mas publica tudo antes de abrir os transportes.
- **O item copiado de G** custa 288 B por item em vez de 256 B (§22.1 item 3).
- **G e GN no mesmo host** colidem no `admin.bind` padrão (9104) e nas portas
  GELF padrão.

---

## 5. Ordem de entrega (2026-10-31)

1. Corte do código por fusão de três vias por arquivo: G como base de trabalho,
   a diferença S→SN aplicada (S e SN normalizados para os nomes GELF); pacotes
   GELF copiados de G; conflitos resolvidos à mão.
2. Porte dos testes: os de SN com documentos GELF, mais os GELF de G
   (`gelf_wiring_test.go`, emissores) sobre o NATS.
3. Suíte rápida, integração NATS real (com e sem JetStream, permissões, TLS
   exigido), emissores reais (fluent-bit, `docker --log-driver=gelf`), fuzz 60 s
   de cada alvo, imagem local.
4. Docs e Docker.
5. Registro no `CHANGELOG.md` da raiz e commit: só com o "sim" do usuário.
