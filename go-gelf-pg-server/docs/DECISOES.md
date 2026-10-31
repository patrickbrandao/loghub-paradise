# go-gelf-pg-server — Decisões de projeto

Registro das decisões que definiram o `go-gelf-pg-server` (letra **GP**). A
entrada comum da família GELF é `tasks/gelf/DESIGN.md` (2026-10-31, fora do
git); este arquivo copia dali o que vale para GP e acrescenta o que o corte
decidiu. A `docs/SPEC.md` é a fonte da verdade, e cada decisão abaixo aparece
nela com o número da seção.

## O pedido

> Baseado nos modelos de syslog e relp, crie os softwares semelhantes para
> atender o GELF. Todos os softwares de ingestão e injeção de syslog e relp
> devem existir em versão semelhante para GELF.

Para GP, a tabela da família (DESIGN §1): **GP é ao `go-gelf-server` (G)
exatamente o que o `go-syslog-pg-server` (SP) é ao `go-syslog-server` (S)** —
só `ring=memory` + `sink=postgres` (`loghub.inbox`), dump JSON Lines, **sem
TLS**, sem filtro e sem rate-limit. O corte em si não teve entrevista própria:
ele repete as decisões que o usuário tomou para SP em 2026-10-07 (SP DECISOES
§1), aplicadas ao GELF.

## Estado do monorepo

| Pasta | Papel para GP |
|---|---|
| `go-gelf-server/` (G) | **Pai do comportamento GELF.** Recepção (chunks, descompressão, `\0`, HTTP), conversão, schema e oráculo vêm de lá; os pacotes GELF são **copiados literalmente**. Nada é importado. |
| `go-syslog-pg-server/` (SP) | **Pai estrutural do corte.** O que SP tirou de S, GP tira de G, com o mesmo código (ring memory, sink postgres, dump JSON Lines, `removed.go`, validação). |
| `go-gelf-nats-server/` (GN) | O irmão (ring memory → NATS). Os pacotes comuns de GP e GN só diferem onde SP e SN diferem. |
| `go-gelf-injector/` (GI) | Cliente de carga e funcional. |

## Fontes normativas

As de G: GELF 1.1 (Graylog), skill `loghub-json-schema` 1.0.0, o
`jlh-gelf-payload.schema.json` deste repositório (não existe skill `type=gelf`),
skill `loghub-postgres-table-inbox` (2026-10-06), `go-loghub-uuidv7` v0.0.3,
`go-loghub-ident` v0.5.2.

---

## 1. Decisões do usuário (entrevista da família, 2026-10-31)

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| 1 | Envelope | `type=gelf`, payload estruturado em kebab-case; schema próprio em `internal/jlhcheck/loghub/jlh-gelf-payload.schema.json`. Aqui o destino é a coluna `type = 'gelf'` da `loghub.inbox` (não há subject). | §3.2, §3.7.1 |
| 2 | Transportes | UDP + TCP + HTTP. **GP sem TLS** (nem TCP+TLS, nem HTTPS), como SP/SN. | §1.2, §4 |
| 3 | `level` ausente | **6 (info)** com aviso `level-default` (D1). | §1.3, §6.5 |
| 4 | `raw` | Só quando malformado, com aviso de encoding ou com perda; `convert.keep_raw = malformed` (padrão) \| `always`. | §6.9 |

## 2. Padrões assumidos (contestáveis)

### Da família (DESIGN §3), como em G

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| 5 | Portas | `listen.udp = :12201`, `listen.tcp = :12201`, `listen.http = :12202`. Sem `listen.tls`/`listen.https`. | §4.1 |
| 6 | Admin | `127.0.0.1:9104`, a porta da família GELF | §14 |
| 7 | Nomes | env `GELF_<SEÇÃO>_<CHAVE>` (`GELF_CONVERT_KEEP_RAW`), arquivo `GELF_CONFIG`; métricas `gelf_`; Docker `gelf-pg*` | §16.2 |
| 8 | Arquitetura | Cru no ring (o documento já remontado e descomprimido), conversão no dreno | §2.2 |
| 9 | Cabeçalho do item | O de G (`compression`, `chunks`), só em memória | §3.4 |
| 10 | Tamanho | `convert.max_message_bytes` = **128 KiB** do documento descomprimido; acima, recusa contada (`too_large`), HTTP `413`, TCP descarta até o `\0`; teto dentro do leitor de descompressão. O 128 KiB de G vem do `max_payload` do NATS; aqui fica por paridade (o mesmo documento é aceito pelos três servidores) | §3.9, §5.5 |
| 11 | UDP | Chunks, prazo de 5 s, tetos globais `listen.udp_reassembly_max_bytes`/`_messages`, compressão pelos bytes | §5.2 |
| 12 | TCP | Moldura `\0`, EOF sem `\0` entrega, backpressure por parar de ler, PROXY opt-in | §5.3 |
| 13 | HTTP | `POST /gelf`, um documento por requisição (ou por linha com `http.bulk`), `202` vazio depois da garantia do ring; `404`/`405`/`413`/`415`/`400`; ring cheio, sink DOWN e shutdown → `503` + `Retry-After`. **Keep-alive sim** (HTTP/1.1 persistente, pipelining sequencial da stdlib), **HTTP/2 não**. `X-Forwarded-For` ignorado. Corrigido em G em 2026-10-31 e portado no mesmo dia: a primeira versão desligava o keep-alive, o que o DESIGN não queria dizer. | §4.5 |
| 14 | Conversão nunca falha | o pior é `format=malformed` com `raw`; documento só de espaço é `empty` | §6.1 |
| 19 | Versão | v1.0.0 | — |

As decisões 15 (filtro e rate-limit), 16 (subjects NATS), 17 (ClickHouse) e 18
(injetor) não se aplicam a GP.

### Do corte (espelho de SP DECISOES §2)

- **Sai o TLS em todo lugar**: listener TCP+TLS, HTTPS, mTLS e o TLS até o
  PostgreSQL (`sslmode=disable` sempre; `LOGHUB_EB_URL` com `sslmode=require`,
  `verify-ca` ou `verify-full` é exit 1). SPEC §1.2, §10.5, §10.9.
- **Saem os outros sinks e rings, o filtro, o rate-limit e o beacon.** As
  chaves `sink.type` e `ring.mode` saem; o log de boot e o `gelf_config_info`
  continuam dizendo `ring_mode="memory"` e `sink_type="postgres"`. SPEC §1.2.
- **A recepção não lê o documento**: sem filtro, o `LevelOf` de G não é chamado
  na recepção (existe no `convert` copiado e só serve ao dreno). A recepção
  testa o ring cheio (`FullFor`) antes de cunhar `uuid` e copiar, como SP.
  SPEC §2.2.
- **`sink.workers` padrão 4**, `WARN` acima de 16; `pool_size = 0` deriva
  `workers + 1`; a sonda de saúde usa o pool (não há sonda separada como em G:
  com o ring só em memória o `+ 1` basta, decisão D1 de SP). SPEC §10.5.
- **`SIGHUP` recarrega só `[log]`.** SPEC §16.3.
- **Dump JSON Lines com um JLH pronto por linha**, reingerido sem conversão
  pela porta `emergency.ParseLine` (SP DECISOES, decisão 3). Adaptado ao GELF:
  `type = "gelf"`, `payload.short-message` string no lugar de
  `payload.message`, aninhamento até 66 (o documento GELF aceita 64) e números
  pela gramática do JSON (o payload GELF carrega o texto dos números do
  emissor). `emergency.MaxLine` = 6 × 16 MiB + 8 KiB. SPEC §12.
- **Chaves de G que saíram** (`internal/config/removed.go`, 96 nomes gerados
  da diferença entre os dois registries): na linha de comando e no INI são
  parâmetro desconhecido, exit 1, com a mensagem "go-gelf-server has it,
  go-gelf-pg-server does not"; no ambiente, **um** `WARN` que as nomeia. SPEC
  §13.2, §16.2.
- **Nomes**: module path `github.com/patrickbrandao/loghub-paradise/go-gelf-pg-server`;
  `application_name` `gelf-pg/<AgentName>@<Hostname>`; Docker `gelf-pg`,
  `gelf-pg-postgres`, volumes `gelf-pg-*`; portas do host no compose 12301
  (UDP/TCP), 12302 (HTTP), `127.0.0.1:19304` (admin), `127.0.0.1:15494`
  (PostgreSQL); usuário e banco padrão `gelf`; imagem
  `tmsoftbrasil/go-gelf-pg-server`.
- **Dependências**: as três de SP (`pgx/v5`, `go-loghub-ident`,
  `go-loghub-uuidv7`; `jsonschema/v6` só em teste). Binário 17,5 MB contra
  32,0 MB de G; imagem 28,5 MB contra 43,8 MB. SPEC §17.3.

### Tomadas na implementação (2026-10-31)

| Tema | Decisão | Por quê | SPEC |
|---|---|---|---|
| Pacotes copiados | `internal/convert`, `internal/item`, `internal/jlhcheck` e `internal/receiver/{chunks,decompress,framing}.go` (com testes, corpora e fuzz) são **literais** de G, a menos do module path | Regra da família: o GELF é idêntico nos três servidores; uma correção vai para G e é copiada | §17.2 |
| `internal/item` com código morto | O pacote vem inteiro, com o codec do cabeçalho, o canal `done`, `RowID`, `StreamID` e `TLS`, que nada aqui usa; `FuzzHead` fica. **Desvio do espelho**: SP apagou o codec e esses campos | A cópia literal pedida; manter dois `item` divergentes custaria mais que 32 B por item | §3.4, §9.2, §22.1 |
| `ItemOverhead` | **288 B** (struct de 256 B + 8 B do slot, arredondado): SP tinha 256, G 384 (com o canal `done`, que só os outros rings de G criam) | Medido; `TestItemOverheadCoversTheStruct` | §9.2 |
| Sem a caixa `*[]byte` | A otimização de SP (`CopyRawBox`/`NewBoxed`, uma alocação a menos por documento) não veio: ela mora em `internal/item`, que é cópia de G | Consequência da linha anterior; custo de 24 B e uma alocação por documento aceito | §15.1 |
| `http.go` | O de G **menos** `NewHTTPS`, o `tls.NewListener`, o `TLSNextProto`, o `payload.tls` e os ramos `Filtered`/`Consumed`. **Desvio da cópia literal** | O de G não compila sem `tls.go` (`BuildTLSConfig`, `sessionInfo`), e o corte tira o TLS | §4.5, §17.2 |
| `obs.Transport` | Só `udp`, `tcp`, `http`; o `obsTransport` mapeia o enum de `item` (de G, com `tls` e `https`) por nome | Série que nunca sobe não é exposta (SP); o enum de `item` não muda (cópia) | §14.1 |
| Número que o `jsonb` não guarda | A cópia da `loghub.inbox` leva como **string** o número de `fields`/`extra` fora do `numeric` (131 072 dígitos antes da vírgula, 16 383 depois); antes, `1e1000000` num campo era `22003`, `logic`, e derrubava o pedaço inteiro. Correção nascida aqui, portada para G em 2026-10-31; de lá voltou o ajuste da **mantissa toda zero** (cabe com qualquer expoente positivo; antes ia como string sem precisar) | O mesmo raciocínio da regra do NUL; conferido contra o PostgreSQL 18 (`TestJSONBHoldsAgreesWithPostgres`) | §3.7.1 |
| Keep-alive HTTP (porte de G, 2026-10-31) | `http.idle_timeout` (60 s, nunca 0) entre requisições; `http.max_requests_per_conn` (0 = sem teto) põe `Connection: close` na N-ésima resposta; o `503` de backpressure mantém a conexão, o de shutdown fecha, e no shutdown toda resposta leva `Connection: close`. Restart. | Uma conexão por documento custava um handshake e um `TIME_WAIT` por evento (20 000 requisições, 20 000 conexões no e2e de G); o `503` com conexão viva custa menos ao cliente que espera 1 s | §4.5 |
| `http.bulk` (porte de G, 2026-10-31, **sem entrevista, contestável**) | Padrão `false`, como o `enable_bulk_receiving` do Graylog. Com `true`: corpo dividido em `\n` (CRLF aceito), linha vazia ignorada, um `uuid` por linha com a mesma amostra de relógio; tamanho tudo ou nada (`413` sem enfileirar nada); ring cheio no meio → as anteriores ficam, `503` (reenviar duplica). Corpo descomprimido até `http.bulk_max_bytes` (4 MiB, até 64 MiB; abaixo de `L` vale `L`). Sem filtro nem rate-limit por linha (não existem no corte). Restart. | O output `http` do fluent-bit manda um *flush* inteiro num corpo; com o padrão do Graylog, quem não pediu continua com um documento por requisição | §4.5.1 |
| Prazo de escrita HTTP (porte de G, 2026-10-31) | 10 s **contados da resposta**: o handler desarma o prazo que o `net/http` arma no fim dos cabeçalhos e o rearma antes de cada resposta. | Corpo lento passava de 10 s: o documento entrava e o `202` se perdia (o cliente reenviava com outro `uuid`); correção do W de 2026-10-14 | §4.5 |
| `chunk_overflow` (porte de G, 2026-10-31) | Uma vez por mensagem: a mensagem recusada pelo teto de bytes vira lápide (nova ou aberta); com `max_messages` cheio, cada chunk conta (não há onde lembrar). | Antes, até `count` descartes por mensagem e uma dupla contagem com `chunk_timeout`, justamente na saturação | §5.2 |
| Shutdown no `Enqueue` (2026-10-31, igual ao GN) | Um `context.Canceled` devolvido pelo ring é `Shutdown` (HTTP `503` com `Connection: close`), não ring cheio. | Um ring que espera no contexto de recepção (o `blockRing` dos testes, um ring futuro) faria a requisição em voo levar um `503` de backpressure e manter a conexão; GP e GN passam a tratar o erro do mesmo jeito | §13.4, §4.5 |
| `ItemOverhead` nos dois cortes (2026-10-31) | GP e GN com **288 B** (o GN tinha 384, o número de G). | A struct é a mesma nos dois; o número de G conta o canal `done`, que nenhum corte cria | §9.2 |
| Nome de cliente no PostgreSQL (2026-10-31) | Mantido `gelf-pg/<AgentName>@<Hostname>` no `application_name`. | Mais legível no `pg_stat_activity` que o nome do módulo; a família não tem regra escrita para esse prefixo | §10.5 |
| `FuzzInboxPayload` | Pula documento só de espaço (a recepção o descarta como `empty`, §5.6) | Achado pelo fuzz desta rodada; o arnês de G tem a mesma lacuna | §18.3 |

## 3. Decisões herdadas

As de G (G DECISOES §1–§2) valem, salvo as que dependem do que saiu: filtro
(15), subjects NATS (16), ClickHouse (17), HTTPS e o TLS da decisão 2. As de SP
(SP DECISOES §1–§10) valem como regras do corte, com os nomes GELF, salvo as
específicas do syslog (unix datagram, charset legado, RFC 3164, framing,
`EffectivePRI`, o vetor dourado com `transport=tls`).

## 4. Riscos conhecidos

- **Sem TLS até o banco nem na entrada.** Senha e eventos trafegam em claro: o
  PostgreSQL precisa estar numa rede confiável ou num socket unix; GELF cifrado
  exige um terminador na frente (com PROXY protocol para manter o `src-ip`) ou o
  `go-gelf-server`.
- **`kill -9` perde o backlog do ring.** O dump cobre FATAL e `SIGTERM`.
- **Dump grande na saída.** Converter todo o ring pendente custa CPU e disco no
  momento do incidente (até ~6× o cru).
- **Servidores GELF no mesmo host** colidem no `admin.bind` padrão (9104).
- **A remontagem UDP** é um mapa sob um mutex só (G §3), compartilhado pelos
  `udp_listeners`.

## 5. Ordem de entrega

1. Corte do código por merge de três vias por arquivo (base S, nossa SP, deles
   G, os três normalizados para os nomes GELF), cópia literal dos pacotes GELF,
   conflitos resolvidos à mão (2026-10-31).
2. Testes com `-race` por pacote, a suíte real do PostgreSQL 18 e os emissores
   reais (fluent-bit, driver `gelf` do Docker); fuzz de 60 s por alvo.
3. `docs/SPEC.md`, `docs/TESTING.md`, `AGENTS.md`, `README.md` e Docker
   (imagem local `tmsoftbrasil/go-gelf-pg-server:dev`, compose com emissor de
   demonstração provado ponta a ponta).
4. Registro no `CHANGELOG.md` da raiz, commit, tag e release: só com o "sim" do
   usuário, pelo nível do monorepo.
