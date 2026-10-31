# go-gelf-server — Decisões de projeto

Registro das decisões que definiram o `go-gelf-server` (letra **G**), o servidor
GELF da família LogHub. A entrada comum da família é `tasks/gelf/DESIGN.md`
(2026-10-31, fora do git); este arquivo copia dali o que vale para G e
acrescenta o que foi decidido na implementação. Depois da SPEC, a
`docs/SPEC.md` é a fonte da verdade, e cada decisão abaixo aparece nela com o
número da seção.

## Estado do monorepo

| Pasta | Papel para G |
|---|---|
| `go-syslog-server/` (S) | **Pai do código.** G é uma cópia de S adaptada ao GELF: rings, sinks, beacon, dump, admin, `SIGHUP`, registry, PROXY, TLS/mTLS vêm de lá com as correções de 2026-10-13/14. Nada é importado. |
| `go-gelf-pg-server/` (GP), `go-gelf-nats-server/` (GN) | Cortes de G, como SP/SN são de S. Os pacotes GELF de G (`internal/convert`, `internal/receiver`, `internal/item`) são **copiados literalmente** para eles. |
| `go-gelf-injector/` (GI) | Cliente de carga e funcional. |

## Fontes normativas

| Fonte | Versão | Papel |
|---|---|---|
| GELF 1.1 (Graylog, "GELF Payload Specification") | 1.1 | Documento JSON, campos padronizados, `_` adicionais, chunks UDP, compressão, `\0` no TCP, `POST /gelf` |
| skill `loghub-json-schema` | 1.0.0 | Envelope JLH: nove campos, `jlh-envelope.schema.json` |
| `jlh-gelf-payload.schema.json` | **deste repositório** | Não existe skill `type=gelf`: o schema é escrito aqui (decisão 1), com o rigor do `jlh-syslog-payload.schema.json` |
| skill `loghub-postgres-table-inbox` | 2026-10-06 | `sink=postgres` (`loghub.inbox`) |
| `go-loghub-uuidv7` v0.0.3, `go-loghub-ident` v0.5.2 | | `uuid` nível 3, identidade |

---

## 1. Decisões do usuário (entrevista de 2026-10-31)

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| 1 | Envelope | `type=gelf`, payload estruturado em kebab-case, subject `loghub.{space}.gelf`. Schema próprio em `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json` (`additionalProperties: false`, extensões `^x-`). Recusados: o objeto GELF original no payload; mapear para `type=syslog`. | §3.2 |
| 2 | Transportes | UDP + TCP + HTTP; em G também TCP+TLS e HTTPS. | §4 |
| 3 | `level` ausente | **6 (info)** com aviso `level-default`. Desvio da GELF 1.1 (que manda 1 = alert), D1: muitas bibliotecas omitem `level` e tudo viraria alerta. | §1.3, §6.5 |
| 4 | `raw` | Só quando malformado, com aviso de encoding ou com perda (§6.9); `convert.keep_raw = malformed` (padrão) \| `always`. | §6.9 |

## 2. Padrões assumidos (contestáveis)

### Da família (DESIGN §3, decididos sem entrevista)

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| 5 | Portas | `listen.udp = :12201`, `listen.tcp = :12201`, `listen.http = :12202`; `listen.tls` e `listen.https` vazios (sugestão `:12203` e `:12204`) | §4.1 |
| 6 | Admin | `127.0.0.1:9104` | §14 |
| 7 | Nomes | env `GELF_<SEÇÃO>_<CHAVE>` (sem exceção de seção: `GELF_CONVERT_KEEP_RAW`), arquivo `GELF_CONFIG`; métricas `gelf_`; tabelas `gelf_events` (sqlite/mysql), `inbox_gelf` (clickhouse); stream Redis `loghub.{space}.gelf`; stream NATS sugerido `LOGHUB_GELF` (`loghub.*.gelf`), nunca criado pelo servidor | §16.2 |
| 8 | Arquitetura | Cru no ring, conversão no dreno. O cru é o **documento GELF já remontado e descomprimido**. | §2.2 |
| 9 | Cabeçalho do item | O de S sem `framing` e sem `size` (o GELF não trunca: `size = len(cru)`), com `compression` e `chunks`; transportes `udp`/`tcp`/`tls`/`http`/`https`. `FuzzHead` cobre o codec novo. | §3.4 |
| 10 | Tamanho | `convert.max_message_bytes` do documento descomprimido. Acima: recusa contada (`reason="too_large"`), sem truncar; HTTP `413`; TCP descarta até o próximo `\0` sem alocar. Teto **dentro** do leitor de descompressão. **Padrão 128 KiB, não 256 KiB**: com 256 KiB o pior JLH (6 × L + 8 KiB = 1,5 MiB) não cabe no `max_payload` de 1 MiB do NATS; com 128 KiB ele é 776 KiB (§3.9). | §3.9, §5.5 |
| 11 | UDP | Chunks `1e 0f`, id de 8 bytes, `seq`, `count` 1..128; remontagem por `(src-ip, src-port, id)`, prazo 5 s, teto global de bytes (`listen.udp_reassembly_max_bytes`, 64 MiB) e de mensagens abertas (`listen.udp_reassembly_max_messages`, 16384); duplicado ignorado; incompleto no prazo é descarte contado; compressão pelos bytes (gzip `1f 8b`, zlib `78 xx` com `%31==0`); amostra de relógio do último chunk. | §4.2, §5.2 |
| 12 | TCP / TLS | Moldura `\0`; sem compressão; EOF sem `\0` entrega a mensagem; backpressure por parar de ler; PROXY e mTLS como S. | §4.4, §5.3 |
| 13 | HTTP / HTTPS | `POST /gelf`, um documento por requisição (ou por linha com `http.bulk`), `202` vazio depois da garantia do ring; `405`/`404`/`413`/`400`; ring cheio, sink DOWN, rate-limit, shutdown → `503` + `Retry-After`. **Keep-alive sim** (HTTP/1.1 persistente, pipelining sequencial da stdlib), **HTTP/2 não** (nem h2c, nem h2 no HTTPS). `X-Forwarded-For` ignorado. Corrigido em 2026-10-31: a primeira versão desligava o keep-alive, o que o DESIGN não queria dizer. | §4.5 |
| 14 | Conversão nunca falha | Não JSON, não objeto, UTF-8 inválido → `format=malformed`, `short-message: ""`, `raw` obrigatório. Documento só de espaço: ignorado e contado (`empty`). | §6.1 |
| 15 | Filtro e rate-limit | Filtro por `level` (`filter.min_level`, `filter.levels`), recarregável; malformado passa como 6. Rate-limit por prefixo como S. | §7, §8 |
| 16 | Subjects NATS | Só `loghub.{space}.gelf` (a chave `sink.nats.subjects` de S não existe). | §3.8 |
| 17 | ClickHouse | `inbox_gelf` espelhando a `loghub.inbox`, com *hints* `fields Map(String, String)` e `extra Map(String, String)` (§3.7.2). | §3.7.2 |
| 19 | Versão | v1.0.0. | — |

### Tomadas na implementação (2026-10-31)

| Tema | Decisão | Por quê | SPEC |
|---|---|---|---|
| Parser JSON | Próprio, por varredura validante, sem `encoding/json` no caminho quente. Uma passada valida o documento inteiro (sintaxe, UTF-8, escapes, profundidade ≤ 64) e registra os membros de topo como fatias do cru; a decodificação de strings só acontece na emissão. | A mesma função serve o filtro na recepção e a conversão no dreno, então o `level` filtrado é o `level` emitido por construção | §6.2 |
| `level` na recepção | Só é varrido quando o filtro pode recusar algo (`min_level < 7` ou `levels` não vazio). | Sem filtro, a varredura seria CPU no caminho quente sem efeito | §7 |
| Profundidade | Aninhamento acima de 64 é `malformed` (`parse.error` próprio). | Teto da recursão do validador; o Jackson do Graylog aceita mais, mas nenhum emissor real passa de 10 | §6.2 |
| BOM | Um BOM UTF-8 no começo é `malformed`. | RFC 8259 permite ignorar, mas nenhum emissor GELF o manda e aceitar exigiria dizer onde o `size` conta | §6.2 |
| Strings com surrogate solto | `\ud800` sozinho vira U+FFFD com `encoding-replaced`, e o `raw` é forçado. | DESIGN §5 | §6.3 |
| Avisos | Lista fechada de 25 códigos (§6.8). `*-invalid` e `duplicate-key` forçam `raw` (houve perda); `*-coerced`, `*-missing`, `level-default`, `field-null`, `key-unknown`, `field-id-reserved`, `field-name-invalid`, `version-unknown` não. | Regra de S: sem perda e sem invenção — o que não coube nos campos fica em `raw` | §6.8 |
| `level` inválido | 6 com `level-invalid` (não `level-default`, que é só ausência). | Separa "não veio" de "veio errado" | §6.5 |
| `timestamp` | Número (ou string no formato de número, com `timestamp-coerced`); aritmética decimal sobre o texto, sem `float64`; até 9 casas (o excedente é truncado, sem aviso); mínimo 3 casas; negativo (exceto `-0`, a época) ou depois de 9999-12-31T23:59:59Z é `timestamp-invalid`: como negativo é recusado, o menor ano possível é 1970. | DESIGN §4 | §6.5 |
| `compression` | O contêiner real: `gzip`, `zlib` ou `deflate` (este só pelo HTTP `Content-Encoding: deflate` sem cabeçalho zlib). | Diz o que chegou, não o que o cabeçalho HTTP disse | §3.2, §4.5 |
| Descompressão inválida | Descarte contado `reason="decompress"` (HTTP `400`). Não vira `malformed`. | Os bytes comprimidos corrompidos não têm conteúdo recuperável; o cru do ring é o documento descomprimido | §5.4 |
| Documento vazio | UDP/HTTP: `empty` contado (HTTP `400`). TCP: um quadro vazio ou só de espaço entre `\0` é separador, ignorado **sem** contar. | DESIGN 12 e 14 | §5.6 |
| Teto do corpo comprimido | Leitura do fio limitada a `L + 16 KiB` (o gzip de dado incompressível cresce ~30 B por 128 KiB); a soma dos chunks de uma mensagem UDP tem o mesmo teto. | Nunca alocar pelo que o par declara | §5.5 |
| Prazo da remontagem | Fixo em 5 s (a spec GELF), varrido por um relógio próprio a cada segundo. | Um parâmetro a mais sem caso de uso | §5.2 |
| HTTP `Consumed` | Recusa final do sink com `ring=none` responde `202`, como o TCP segue lendo. | A mesma semântica do ring assíncrono, em que o cliente já teria recebido `202` | §4.5 |
| HTTP `Content-Encoding` desconhecido | `415`. | Nunca vai passar | §4.5 |
| HTTP filtrado | `202`. | Descarte pedido pelo operador, não erro do cliente | §4.5 |
| Colunas de `gelf_events` | `uuid, step, space, level, sid, ev_uts, ev_sub, format, transport, src_ip, hostname, env, nbytes` (13). | As de S sem as do syslog (`facility`, `severity`, `appname`, `procid`, `msgid`) | §3.7 |
| `[http]` | `read_header_timeout` (10 s), `read_timeout` (30 s), `max_conns`, `max_conns_per_ip`, valendo para `http` e `https` (limitadores distintos). O HTTPS usa os certificados de `[tls]`. | Um conjunto de chaves por protocolo de aplicação, como `[tcp]`/`[tls]` em S | §16.5 |
| `convert.accept_malformed` | Mantida (padrão `true`): com `false`, o envelope `malformed` é descartado no dreno e contado, como `syslog.accept_malformed`. | Paridade com S | §6.1 |
| Keep-alive HTTP (2026-10-31) | `http.idle_timeout` (60 s, nunca 0) entre requisições; `http.max_requests_per_conn` (0 = sem teto) põe `Connection: close` na N-ésima resposta; o `503` de backpressure mantém a conexão, o de shutdown fecha, e no shutdown toda resposta leva `Connection: close`. Restart. | Uma conexão por documento custava um handshake e um `TIME_WAIT` por evento (20 000 requisições, 20 000 conexões no e2e); o `503` com conexão viva custa menos ao cliente que espera 1 s | §4.5 |
| `http.bulk` (2026-10-31, **sem entrevista, contestável**) | Padrão `false`, como o `enable_bulk_receiving` do Graylog. Com `true`: corpo dividido em `\n` (CRLF aceito), linha vazia ignorada, um `uuid` por linha com a mesma amostra de relógio; tamanho tudo ou nada (`413` sem enfileirar nada); ring cheio no meio → as anteriores ficam, `503` (reenviar duplica). Corpo descomprimido até `http.bulk_max_bytes` (4 MiB, até 64 MiB; abaixo de `L` vale `L`). Restart. | O output `http` do fluent-bit manda um *flush* inteiro num corpo; com o padrão do Graylog, quem não pediu continua com um documento por requisição. O teto do corpo é parâmetro porque um *flush* passa de `L`, e um `413` faria o fluent-bit jogar o lote fora | §4.5.1 |
| Prazo de escrita HTTP (2026-10-31) | 10 s **contados da resposta**: o handler desarma o prazo que o `net/http` arma no fim dos cabeçalhos e o rearma antes de cada resposta. | Corpo lento + espera do ring passavam de 10 s: o documento entrava e o `202` se perdia (o cliente reenviava com outro `uuid`); correção do W de 2026-10-14 | §4.5 |
| `chunk_overflow` (2026-10-31) | Uma vez por mensagem: a mensagem recusada pelo teto de bytes vira lápide (nova ou aberta); com `max_messages` cheio, cada chunk conta (não há onde lembrar). | Antes, até `count` descartes por mensagem e uma dupla contagem com `chunk_timeout`, justamente na saturação | §5.2 |
| Número fora do `numeric` (2026-10-31, portado do GP) | Na cópia da `loghub.inbox`, o número de `fields`/`extra` que o `jsonb` não guarda vai como string do texto; mantissa toda zero cabe com qualquer expoente positivo. | `1e1000000` num campo era `22003` e derrubava o pedaço inteiro | §3.7.1 |

## 3. Riscos conhecidos

- O `level` de topo é lido na recepção só com filtro ativo: com filtro, o
  documento é validado duas vezes (recepção e dreno). Medido em
  `BenchmarkScanLevel`.
- A remontagem UDP é estado compartilhado sob um mutex: com `udp_listeners > 1`
  e chunks em massa, ele é o ponto de contenção. Fragmentar em shards fica para
  quando houver medida (SPEC §22.1).
- `fields` como `Map(String, String)` no ClickHouse perde o tipo do número (vira
  o texto). O `env` (o JLH íntegro) fica no `payload` de qualquer forma.
