# go-relplog-pg-server — Decisões de projeto

Registro das decisões que criaram o `go-relplog-pg-server` em 2026-10-13. Este
arquivo é a **entrada** da `docs/SPEC.md`; a SPEC é a fonte da verdade, e cada
decisão abaixo aparece nela com o número da seção. As decisões **RP-N** são
deste módulo; "decisão N do R" e "decisão N do SP" são as dos pais, nos
`docs/DECISOES.md` deles.

## O pedido

O pedido (2026-10-13, repassado pelo agente do monorepo, sem entrevista): criar
o `go-relplog-pg-server` como **corte do `go-relplog-server`** (R, v1.0.0) no
molde do corte que o `go-syslog-pg-server` (SP) fez no `go-syslog-server`: ring
**só em memória**, destino **só** a `loghub.inbox` do PostgreSQL, emergência em
**JSON Lines** com um JLH por linha, **sem TLS em lugar nenhum**, dependências
mínimas. Ficam a porta admin, a `LOGHUB_EB_URL`, o PROXY protocol, o `SIGHUP`
de `[log]` e o charset legado; saem o filtro, o rate-limit, o beacon e os
outros rings e destinos. A semântica RELP do R fica intacta. "Faça tudo
automaticamente": não houve entrevista, e pela regra do usuário de 2026-10-10 o
que se aprende do código e dos irmãos é decidido aqui. Uma decisão anterior do
usuário num `DECISOES.md` irmão conta como descoberta: a decisão 4 do SP (o TLS
sai **em todo lugar**, contra a recomendação de tirar só o listener) vale aqui
sem nova pergunta.

O módulo nasceu como cópia do R e foi **cortado**, não reescrito: a recepção
RELP, a conversão, a posse de buffer, o shutdown e o registry são os do R, com
os comentários; o corte (ring, sink, emergência, `removed.go`) é o do SP, por
fusão de três vias (o R como "nosso", o S como base, o SP como "deles"). A
numeração da SPEC é a do R, e uma seção removida mantém o título com uma nota.

## Estado do monorepo

| Pasta | Estado em 2026-10-13 |
|---|---|
| `go-relplog-pg-server/` (**RP**) | **Este módulo**, criado em 2026-10-13, não commitado |
| `go-relplog-server/` (**R**) | O pai: RELP em claro e sobre TLS, rings `memory`/`sqlite`/`redis`/`none` × sete sinks, filtro, rate-limit, beacon. v1.0.0 (commit `f41a0aa`). Fonte do código; **nunca importado** |
| `go-syslog-pg-server/` (**SP**) | O molde do corte: o `go-syslog-server` cortado para memória → PostgreSQL. Fonte do corte |
| `go-relplog-nats-server/` (**RN**) | O irmão, criado no mesmo dia por outro agente: o mesmo corte do R no molde do `go-syslog-nats-server` (SN). Os pacotes comuns diferem dele só onde o SP e o SN diferem |
| `go-syslog-server/` (S), `go-syslog-nats-server/` (SN), `go-syslog-standard-server/`, `go-webhook-*` | Referência; nada importado |

---

## 1. Decisões deste módulo

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| RP-1 | Forma | Módulo independente, cópia cortada do R, numeração da SPEC do R. Nada importado de outro módulo | §17.1 |
| RP-2 | Um ring, um sink | Ring **só** `memory`, destino **só** `loghub.inbox`. As chaves `ring.mode` e `sink.type` saem (cada uma teria um valor só); o log de boot e o `relplog_config_info` continuam dizendo `ring_mode="memory"` e `sink_type="postgres"`, para os painéis do R servirem aqui | §1.2, §2.1, §9.3, §10.5, §14.1 |
| RP-3 | TLS | **Sai em todo lugar** (decisão 4 do SP): sem `listen.tls`, sem `[tls]`, sem mTLS, sem `payload.tls`; até o PostgreSQL sempre `sslmode=disable`, o ambiente `PG*` escondido durante o parse. Na `LOGHUB_EB_URL`, só `postgres://`/`postgresql://`; `sslmode=require`/`verify-*` e parâmetro repetido são exit 1 (do SP); `@` depois do primeiro `/`, `?` ou `#` da autoridade e fragmento são exit 1 (do R); nenhum erro cita a URL | §1.2, §4.5, §10.5, §10.9 |
| RP-4 | Emergência | O dump do SP: JSON Lines, um JLH pronto por linha, `ParseLine` estrito como única porta para a fila, temporário + `fsync` + `link` (ou `rename` depois de conferir o nome livre) + `fsync` do diretório, reingestão em fluxo, `Leftovers`. Mais o que o R trouxe a mais e a entrada R do CHANGELOG pediu para portar: `fsync` do diretório no `Remove` (com `ErrNotDurable`: "feito, sem `fsync`" vira `WARN`, não falha), contagem do aborto com um dono só (`dumpPhase` + `Swap`), gauges recontados do disco no fim da reingestão (`Store.Count`) | §12 |
| RP-5 | Filtro, rate-limit, beacon | Saem (molde do SP). A **decisão 8a do R** (o rate-limit que segura em vez de recusar) deixa de se aplicar; a decisão 8 do R vale só para a vazia (`200 OK`, `messages_dropped{empty}`). Proteção contra emissor ruidoso: os tetos `tcp.max_conns*`, a janela e `relp.inflight_max_bytes` | §1.2, §7, §8, §11 |
| RP-6 | **Onde o R respondia `500`** | Reavaliado caso a caso. **Saíram com o corte**: `500 refused by the destination` (recusa `logic` final com `ring=none`; aqui a recusa `logic` acontece no worker, depois do `200`, e é descarte contado em `events_dropped{sink_logic}`), o `500` do filtro (já era `200` no R) e o do rate-limit (já era segurar no R). **Ficam**: `500 source address unusable` (a guarda de origem, que um par TCP não alcança), `500 command disabled`, `500 protocol error: connection already open`, `500 command not supported` (erros de protocolo do emissor, como o servidor do `librelp`). **Novo caso decidido**: um erro inesperado do `Enqueue` (nem cheio nem fechado; o ring em memória não o produz) é **segurado** como ring cheio, com `ERROR` amostrado, e nunca vira `500`: um `500` destruiria a mensagem na origem por um defeito do servidor | §5.4, §5.7 |
| RP-7 | Conta das cópias em voo | `txnCharge = 2 × PooledCap + 8 KiB`, **a do R**, mesmo sem o caso que a justificava lá (a cópia segurada do `ring=none`). Mais justa seria possível, mas `relp.inflight_max_bytes` precisa medir a mesma coisa nos dois servidores: um operador que copia o valor do R para cá não pode ver o teto mudar de sentido | §5.6 |
| RP-8 | Nomes | Binário e `AGENT_NAME` `go-relplog-pg-server`; env `RELPLOG_<SEÇÃO>_<CHAVE>` e `RELPLOG_CONFIG` (os do R), `[syslog]` com `SYSLOG_<CHAVE>`; métricas `relplog_`; admin `127.0.0.1:9103` (o do R e o do RN: dois deles no mesmo host colidem, documentado); `relp_software=go-relplog-pg-server,<versão>`; `application_name` `rlpg/<AgentName>@<Hostname>`; Docker com prefixo `rlpg`; letra **RP** no CHANGELOG | §5.4, §14, §16.2 |
| RP-9 | Workers e pool | `sink.workers` com padrão **4** direto (no R, 8 rebaixado para 4 com `WARN` num destino SQL), `WARN` acima de 16; `sink.postgres.pool_size = 0` deriva `workers + 1` | §10.5 |
| RP-10 | `SIGHUP` | Recarrega só `log.level`, `log.access`, `log.access_sample`; `filter.*`, `ratelimit.*` e `sink.sqlite.keep_files` saíram | §16.3 |
| RP-11 | Chaves do pai | Na linha de comando ou no INI, parâmetro desconhecido (exit 1). No ambiente, **um** `WARN` que nomeia todas as variáveis do R que este servidor não lê. A lista (`internal/config/removed.go`, 95 nomes) é os 148 nomes do registry do R v1.0.0 menos os deste; `TestRemovedEnvIsTheParentMinusThis` a fixa | §13.2, §16.2 |
| RP-12 | Dependências | Três em produção: `pgx/v5`, `go-loghub-ident`, `go-loghub-uuidv7`; `jsonschema/v6` só em teste. Binário de 31,9 MB (R) para 17,2 MB | §17.3 |

---

## 2. Padrões assumidos (não perguntados, contestáveis)

- **Ordem do boot com a numeração do R.** Os passos 10 (dreno do backlog do
  `ring=sqlite`) e 11 (beacon) não existem; ficam no texto como "não existe
  aqui", e o código cita 12 (transportes) e 13 (pronto) como no R (§13.1).
- **`idleWriteDeadline` mantido** no escritor de respostas: o prazo de escrita de
  uma sessão ociosa é do R e não depende do destino (§5.8).
- **O vetor dourado com `transport=tls` roda reetiquetado como TCP**, com o
  `sid` recalculado, como no SP (§18.2).
- **`ring.ItemOverhead` 256 B, sem codec do cabeçalho**, como no SP; o
  `FuzzHead` do R saiu com o codec (§3.4, §18.3).
- **A recepção confere o ring cheio antes de cunhar o `uuid` e copiar a linha**
  (revisão C do SP), **nos dois tetos** (`ring.FullFor(n)` com o tamanho
  truncado; a do SP via só os itens): uma transação segurada não gasta `uuid`
  nem cópia, nem quando quem segura é `ring.max_bytes` (§2.2, §5.7; revisão
  independente de 2026-10-13).
- **Itens de crash nos testes vêm de uma sessão RELP real** (o R os montava pelo
  TCP do S); nada no código de produção muda com isso.
- **Suíte de emissores vivos com três cenários** (em claro, backpressure,
  restart com dump): os cenários de TLS e do `500` final do R não existem aqui.
  Imagem `rlpg-test-rsyslog:alpine-3.22` (§18.7).
- **Flags: a regra do R para a cauda depois de um valor** (`knownSectionOf`):
  depois de um valor, uma flag desconhecida fora de toda seção conhecida não é
  nomeada (pode ser a cauda do valor); dentro de uma seção conhecida é
  (`--sink.postgres.tabel`). É a mesma regra no R e aqui; o SP cita só o
  "unexpected argument" (§16.2).
- **Docker:** um `docker-compose.yml` com o servidor, um PostgreSQL 18 com a fila
  de `postgres/inbox.sql` e, só com `--profile demo`, o rsyslog de
  demonstração do R (`rsyslog/`, sem a variante TLS). Portas no host 20580
  (RELP), `127.0.0.1:19380` (admin), `127.0.0.1:15380` (PostgreSQL), disjuntas
  das do R, do SP e do RN.
- **Revisão independente de 2026-10-13** (achados de dez revisores, TESTING
  §9.3). Escolhas desta rodada, todas contestáveis:
  - **`sink.boot_timeout` da reingestão sem progresso**: o prazo anda a cada
    pedaço confirmado, em vez de um prazo único desde o início (a regra
    herdada do SP, que se gastava com sucessos). A mesma escolha do RN (§12.2).
  - **A `LOGHUB_EB_URL` exige usuário e banco** (exit 1 nomeando o que falta),
    em vez do padrão do registry (`syslog`) ou do da libpq (o usuário do
    sistema): explícito em vez de adivinhado, porque a URL é dona das duas
    chaves (§10.9). `?sslmode=` vazio, `host=` com `,`/`:`/branco, host com `:`
    que não é IPv6 e uma URL só com espaços também são exit 1.
  - **`sink.postgres.pool_size` ≥ `sink.workers + 1`**, e não `≥ workers`: a
    sonda de saúde precisa de uma conexão (§10.5). Uma conexão dedicada à
    sonda seria a alternativa; a regra é mais simples e o padrão (0 → workers
    + 1) já a cumpria.
  - **Tetos de configuração** (§13.2): `ring.capacity_items` ≤ 16 Mi,
    `sink.batch` ≤ 65 536, `sink.batch_max_bytes` ≤ 256 MiB, `sink.workers`
    ≤ 1024, `pool_size` ≤ 4096, `listen.tcp_listeners` 1–256. Os números são
    folgados de propósito: só barram o erro de digitação, nunca um ajuste.
  - **Regra `ON INSERT`/`ON UPDATE` na fila é FATAL no boot, e `0A000` é
    `fatal`** (§10.5): a alternativa (ignorar) deixava toda escrita em laço de
    `network`.
  - **O fim da sessão vence até o `Enqueue`** (§5.7): a transação confere o
    contexto de novo depois da cópia. Uma transação em montagem quando o
    servidor encerra a sessão (frame ruim, idle) fica sem resposta e é
    retransmitida, em vez de entrar no ring e responder.
  - **Dump abandonado depois do aborto** (`Store.SetAbandon`, o código do RN,
    §13.4): o dump que o aborto já contou não é publicado.
  - **Flags e INI** (§13.2): `--help`/`--version`/`--dump-config` com valor e
    `--config=` vazio são exit 1; uma marca de ordem de bytes UTF-8 no INI é
    ignorada; o `--dump-config` escreve comentados os valores derivados
    (`pool_size`, `emergency.dir`), para o dump relido derivá-los de novo; os
    sinônimos do charset não são mudança no `SIGHUP` (§16.3).
  - **Recusados**, com o porquê em TESTING §9.3: remover `Outcome.Retryable`
    (documenta a regra e os testes a usam; o RN a mantém) e os tipos
    `KInt64`/`KList` do registry (o `value.go` é comum com o RN, que usa
    `KList`).
- **Doc de `main.go` corrigido**: o do R ainda dizia "syslog in over UDP, TCP,
  TLS and unix datagram", herança do S; aqui diz o que o binário faz. É
  material da lista "a portar" para o R.

---

## 3. Decisões herdadas do `go-relplog-server`

As decisões 1 a 16 do R (`../go-relplog-server/docs/DECISOES.md`) **continuam
valendo**, salvo as abaixo.

| # do R | Tema | Aqui |
|---|---|---|
| 1, 3, 5, 6, 7, 9, 10, 11, 13 | forma, at-least-once, segurar, shutdown com `serverclose`, `close` do cliente, janela e teto em voo, tamanho, custo por conexão, ordem dos comandos | **valem** |
| 2 | `x-transport: "relp"` | vale, com `transport` sempre `tcp` (não há `tls`) |
| 4 | O que um `500` significa | vale a leitura do `librelp`; os casos que recebem `500` estão em RP-6 |
| 8 | Filtrada, vazia | vale só para a vazia (não há filtro) |
| 8a | Rate-limit segurando | **não vale**: o rate-limit saiu (RP-5) |
| 12 | Transportes | vale **sem o TLS**: só `listen.tcp`, com PROXY v1/v2 |
| 14 | Todos os recursos do S | **não vale**: um ring, um sink, sem filtro, rate-limit e beacon |
| 15 | Nomes | valem os de env, métricas e admin; o resto é RP-8. Não há ring Redis nem stream NATS |
| 16 | Pendências do S | valem as decididas que tocam o que ficou (teto por IP pelo endereço exato, offset da RFC 3164 seguindo a skill); `retention: interest` e `sink=none` saíram com os destinos (§22) |

## 4. Decisões herdadas do `go-syslog-pg-server`

| # do SP | Tema | Aqui |
|---|---|---|
| 1 | Admin e `LOGHUB_EB_URL` ficam; filtro e rate-limit saem | **vale** (RP-5) |
| 2 | Unix datagram, `SIGHUP`, PROXY (só TCP), charset legado | vale **sem o unix datagram** (o RELP só existe sobre fluxo, decisão 12 do R) |
| 3 | Emergência em JSON Lines com JLH pronto | **vale** (RP-4) |
| 4 | TLS sai em todo lugar | **vale** (RP-3) |
| padrões | `sink.type`/`ring.mode` fora com os rótulos mantidos; `workers` 4; `SIGHUP` só `[log]`; codec do cabeçalho apagado; overhead 256 B; pré-conferência do ring cheio; chaves do pai em `removed.go`; três dependências | **valem** (RP-2, RP-9, RP-10, RP-11, RP-12, §2) |
| porte do N (2026-10-11) | `rename` sem *hard link*, `fsync` do diretório como falha do dump, `Leftovers`, INI e flags sem eco | **valem** (§12.1, §13.2) |

A recepção do SP (UDP, unix, molduras da RFC 6587, `tcp.framing`,
`tcp.backpressure_timeout`) **não** vem: a do R é a que fica.

---

## 5. Riscos conhecidos

- **Sem TLS até o banco nem na entrada.** Senha e eventos trafegam em claro:
  rede confiável, socket unix, ou um terminador TLS na frente do RELP (com PROXY
  protocol). Um `pg_hba.conf` só com `hostssl` recusa o login (classe `28`,
  fatal no boot).
- **`kill -9` perde o backlog do ring, que já teve `200 OK`.** O rsyslog o
  apagou da fila dele; o dump cobre FATAL e `SIGTERM`, não `kill -9`, OOM nem
  pane (§19). Quem precisa disso usa o `ring=sqlite` do R.
- **Recusa `logic` depois do `200`.** Com o ring em memória, uma linha que a fila
  recusa para sempre (classe `22`, `23` exceto `23502`) é descarte contado e
  logado com os `uuid`, não um `500` ao emissor (RP-6, §2.3).
- **Comportamento do cliente.** Como no R: as regras de §5.4 dependem do
  `librelp`, provadas contra o rsyslog 8.2410 real.
- **Duplicatas.** At-least-once (decisão 3 do R); a retransmissão do rsyslog
  chega com `uuid` novo, e `ON CONFLICT (uuid)` só absorve o reenvio do mesmo
  `uuid` (retry, reingestão). O rsyslog reenvia uma mensagem já confirmada ao
  voltar de uma suspensão (observado de novo no compose de demonstração,
  2026-10-13: 132 linhas para 131 `seq`).
- **Dump grande na saída.** Converter todo o ring pendente custa CPU e disco no
  incidente; com o ring cheio o arquivo chega a ~12× o cru.
- **Dois servidores da família no mesmo host** colidem no `admin.bind` padrão
  (9103): este, o `go-relplog-server` e o `go-relplog-nats-server` usam a
  mesma porta.
- **Arestas conhecidas e não corrigidas**: estão em `docs/SPEC.md` §22.1, com
  cenário e motivo (as herdadas do SP que ficaram e as que a revisão de
  2026-10-13 achou e deixou). Das herdadas do SP, os sinônimos do charset no
  `SIGHUP` e o `pool_size` derivado sem comentário no `--dump-config` foram
  corrigidos na revisão.

---

## 6. Ordem de entrega

1. Corte do código: fusão de três vias R/S/SP, `receiver/` sem TLS, `config/`
   (registry `RELPLOG_`, `eburl.go`, `removed.go`), `app/` (boot do SP com o
   shutdown do R).
2. Testes com `-race` por pacote, a suíte real do PostgreSQL 18 e o rsyslog real.
3. `docs/SPEC.md` (a do R cortada, por roteiro), `docs/TESTING.md`,
   `AGENTS.md`, `CLAUDE.md`, `README.md` e Docker.
4. Conferência das correções dos dois pais, uma a uma (TESTING §9).
5. Revisão independente por área; achados corrigidos com teste que falhava antes
   (TESTING §9).
6. Entrada **RP** no `CHANGELOG.md` da raiz (pelo agente do monorepo) e commit,
   só com o "sim" do usuário.
