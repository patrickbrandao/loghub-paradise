# go-syslog-pg-server — Decisões de projeto

Registro da entrevista de 2026-10-07 que definiu o `go-syslog-pg-server`. Este
arquivo é a **entrada** da `docs/SPEC.md`; a SPEC é a fonte da verdade, e cada
decisão abaixo aparece nela com o número da seção.

## O pedido

> Crie o software go-syslog-pg-server baseado no go-syslog-server que fará sink
> somente no PostgreSQL e ring somente na memória, com emergência em arquivo
> JSON Lines. Os demais recursos do go-syslog-server devem ser retirados.
> Retire também suporte TLS. O objetivo é ter um servidor de syslogs rápido e
> com dependências mínimas.

O módulo nasceu como cópia do `../go-syslog-server` (o servidor syslog LogHub,
com dois eixos ring × sink) e foi **cortado**, não reescrito: conversão,
moldura, recepção, posse de buffer, saúde, shutdown e registry são os do pai,
com os comentários. A numeração da SPEC é a do pai (o código cita a spec por
número), e uma seção removida mantém o título com uma nota.

## Estado do monorepo

| Pasta | Estado |
|---|---|
| `go-syslog-pg-server/` | **Este módulo**, v1, criado em 2026-10-07 |
| `go-syslog-server/` | O pai: rings `memory`/`sqlite`/`redis`/`none` × sinks `nats`/`redis`/`sqlite`/`postgres`/`mysql`/`clickhouse`/`none`, TLS, filtro, rate-limit, beacon. Fonte do código e das decisões herdadas; **nunca importado** |
| `go-syslog-standard-server/` | O antigo, referência do pai |

---

## 1. Decisões do usuário (entrevista de 2026-10-07)

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| 1 | Recursos operacionais | **Ficam:** a porta admin (`/healthz`, `/readyz`, `/metrics`) e a `LOGHUB_EB_URL` (só `postgres://`). **Saem:** o filtro por facility/severidade e o rate-limit por prefixo | §1.2, §10.9, §14 |
| 2 | Mais o que fica | O socket unix datagram, a recarga por `SIGHUP`, o PROXY protocol v1/v2 (agora só no TCP) e o charset legado (`cp1252`/`latin1`) | §4.3, §4.6, §6.7, §16.3 |
| 3 | Formato da emergência | **JSON Lines com um envelope JLH pronto por linha** (a opção recomendada): legível com `jq`, utilizável por qualquer ferramenta LogHub. O custo: converter todo o ring pendente na hora do dump | §12 |
| 4 | TLS | **Sai em todo lugar** (o usuário escolheu isto contra a recomendação de tirar só o listener): sem listener RFC 5425, sem mTLS, e **sem TLS até o PostgreSQL** — a conexão é sempre `sslmode=disable` (o `TLSConfig` do pgx nulo, os *fallbacks* descartados, `PGSSLMODE` ignorado). Uma `LOGHUB_EB_URL` com `sslmode=require`, `verify-ca` ou `verify-full` é recusada com exit 1; `disable`, `allow`, `prefer` ou ausente passam. O banco é alcançado por rede confiável ou por socket unix | §1.2, §10.5, §10.9 |

---

## 2. Padrões assumidos (não perguntados, contestáveis)

### Corte

- **O heartbeat beacon sai.** Ele precisava do `sink=nats`. Sonar, ping e pong
  continuam fora (§11).
- **As chaves `sink.type` e `ring.mode` saem**: cada uma teria um valor só. O
  log de boot e o `syslog_config_info` continuam dizendo `ring_mode="memory"` e
  `sink_type="postgres"`, para que um painel do pai sirva aqui (§14.1).
- **`sink.workers` tem padrão 4 direto** (no pai, 8 rebaixado para 4 com `WARN`
  num destino SQL); `WARN` acima de 16; `pool_size = 0` deriva `workers + 1`
  (§10.5).
- **`SIGHUP` recarrega só `log.level`, `log.access` e `log.access_sample`**
  (§16.3); `filter.*`, `ratelimit.*` e `sink.sqlite.keep_files` saíram.
- **O conversor perdeu a extração das colunas promovidas** (`facility`,
  `hostname`... para a tabela `syslog_events`): só `sqlite` e `mysql` as usavam.
- **A recepção não lê mais o PRI**: sem filtro, nenhum byte da mensagem é
  interpretado antes do ring (§2.2); `convert.EffectivePRI`, que só o filtro
  usava, foi apagado. A recepção testa o ring cheio antes de cunhar `uuid` e
  copiar a linha (revisão C, §2.2).
- **O codec do cabeçalho do item (`item/codec.go`) foi apagado**: nada persiste
  um cabeçalho (o ring é memória, o dump guarda JLH), e o `FuzzHead` saiu com
  ele (§3.4, §18.3).
- **`item.ItemOverhead` caiu de 384 para 256 B**: a struct tem 224 B (com a
  caixa `*[]byte` do buffer do pool, que tirou uma alocação por mensagem), sem o
  canal `done`, o `RowID` do `ring=sqlite` e o `StreamID` do `ring=redis`
  (§9.2).
- **O vetor dourado com `transport=tls` roda reetiquetado como TCP**, com o
  `sid` recalculado pela fórmula de §6.10 (§18.2).
- **Chaves do pai que saíram**: na linha de comando ou no INI são parâmetro
  desconhecido (exit 1); no ambiente, um `WARN` único que as nomeia e diz que
  são ignoradas, porque um mesmo `.env` pode alimentar os dois servidores
  (§13.2, §16.2).

### Emergência (detalhes da decisão 3)

- Arquivo `<emergency.dir>/emg-<agent_uuid>-<unixnano>.jsonl`; cada linha é o
  JLH exatamente como o worker o teria escrito (conversão com a config
  `[syslog]` congelada, então byte a byte idêntico).
- Malformadas ficam de fora com `syslog.accept_malformed=false`, contadas em
  `events_dropped{reason="malformed"}`.
- `O_CREATE|O_EXCL`, `fsync` do arquivo e do diretório, arquivo parcial removido
  em qualquer falha (exit 3, `emergency_failed`).
- Reingestão no boot, antes de qualquer transporte, do arquivo mais antigo para
  o mais novo, de todo `emg-*.jsonl` do diretório, qualquer que seja o
  `emergency.enabled` atual. **Sem conversão**: a linha vai como está, e as
  colunas da `loghub.inbox` saem do cabeçalho da própria linha; `type` precisa
  ser `syslog` e `step` 0. Por isso um dump sobrevive a uma troca de chaves
  `[syslog]` entre execuções.
- Cada linha é validada com rigor por `emergency.ParseLine` (SPEC §12.2), a
  única porta entre o disco e a fila: uma linha que a fila recusasse derrubaria
  como `logic` o pedaço inteiro. Linha sem o `\n` final = cauda truncada; linha
  completa inválida, ou maior que `MaxLine` (12 × 16 MiB + 8 KiB, nunca alocado
  de antemão), é pulada; arquivo danificado entrega as linhas íntegras e vira
  `.corrupt`; arquivo limpo é removido (com `fsync` do diretório) só depois de
  todos os envelopes confirmados.

### Nomes e compatibilidade com o pai

- Module path `github.com/patrickbrandao/loghub-paradise/go-syslog-pg-server`;
  binário `cmd/go-syslog-pg-server`; `/app/go-syslog-pg-server` na imagem; tags
  `go-syslog-pg-server/vX.Y.Z`.
- **Prefixo de env `SYSLOG_` e de métrica `syslog_` inalterados**: mesma
  família; um operador pode trocar um servidor pelo outro.
- **`admin.bind` continua `127.0.0.1:9102`**: não rode os dois servidores no
  mesmo host sem trocar o de um deles.
- `application_name` do PostgreSQL: `slpg/<AgentName>@<Hostname>`.
- Docker: prefixo `slpg` nos nomes de projeto, contêiner e volume; portas do
  host distintas das do compose do pai (syslog `5180`, admin
  `127.0.0.1:19180`, PostgreSQL `127.0.0.1:15480`).

### Dependências

- **De oito para três em produção**: `pgx/v5`, `go-loghub-ident`,
  `go-loghub-uuidv7` (`jsonschema/v6` só em teste). O binário caiu de 31,8 MB
  para 17,2 MB (build `darwin/arm64`, as mesmas flags nos dois). SPEC §17.3.

---

## 3. Decisões herdadas do `go-syslog-server`

As decisões 1 a 18 do pai estão em `../go-syslog-server/docs/DECISOES.md` e
**continuam valendo** aqui, salvo as abaixo.

| # do pai | Tema | Aqui |
|---|---|---|
| 1, 2, 3, 4, 7, 12, 13, 14, 15 | pastas, RELP fora, moldura por conexão, `message`/`raw`, cru no ring e parse no dreno, TAG estrita, truncamento, schema só em teste, PKTINFO | **valem** |
| 5 | Transportes | vale **sem o TLS** (RFC 5425): UDP, TCP e unix |
| 6 | Todos os recursos da standard | **não vale**: só um ring, um sink; sem filtro e sem rate-limit |
| 8 | Beacon | **não vale**: saiu com o `sink=nats` |
| 9 | NATS `core` | **não vale** |
| 10 | `LOGHUB_EB_URL` | vale **só com `postgres://`/`postgresql://`** e sem `sslmode` que peça TLS |
| 11 | Ring Redis | **não vale** |
| 16 | Sink padrão `nats` | **não vale**: o único sink é a `loghub.inbox` |
| 17 | mTLS | **não vale** |
| 18 | Opcionais | o charset legado e o PROXY protocol (só TCP) **valem**; os subjects por facility/severidade **não** (eram do NATS) |

Também saem dos padrões assumidos do pai: o dump `.bin` com cabeçalho + cru
(virou JSON Lines com JLH), a tabela `syslog_events` dos sinks SQL, os nomes do
ring Redis e do stream NATS, `LOGHUB_BEACON_INTERVAL` e `LOGHUB_CTL_NAMESPACE`,
e os compose ring × sink (aqui há um).

---

## 4. Riscos conhecidos

- **Sem TLS até o banco.** Senha e eventos trafegam em claro: o PostgreSQL
  precisa estar numa rede confiável ou num socket unix. Um `pg_hba.conf` só
  com `hostssl` recusa o login (classe `28`, fatal no boot).
- **Sem TLS na entrada.** Syslog cifrado exige um terminador na frente (com
  PROXY protocol para manter o `src-ip`) ou o `go-syslog-server`.
- **`kill -9` perde o backlog do ring.** O dump cobre FATAL e `SIGTERM`, não
  `kill -9`, OOM nem pane (SPEC §19).
- **Dump grande na saída.** Converter todo o ring pendente custa CPU e disco no
  momento do incidente; com o ring cheio (512 MiB de cru) o arquivo chega a
  vários GiB no pior caso (12× o cru).
- **Os dois servidores no mesmo host** colidem no `admin.bind` padrão (9102).
- **Arestas conhecidas e não corrigidas** (revisão independente de 2026-10-07,
  TESTING §8 item 12): o `SIGHUP` relata chaves derivadas (`pool_size`,
  `emergency.dir`) e sinônimos do charset como "requires restart"; o
  `--dump-config` imprime o `pool_size` derivado sem comentário; `proxy.*` e
  `tcp.*` são aceitos com `listen.tcp` vazio; `listen.tcp` e `admin.bind` na
  mesma porta só falham ao abrir o transporte; uma falha ao remover um dump já
  entregue só é logada, e o arquivo é entregue de novo no boot seguinte
  (deduplicado só enquanto o `uuid` estiver na fila); `ring.max_bytes` aceita
  valores em que nenhuma mensagem cabe (herdado do pai); os contadores da
  recepção não têm *padding* e disputam a linha de cache entre listeners
  `SO_REUSEPORT`.

---

## 5. Ordem de entrega

1. Corte do código (pacotes, registry, métricas, dump JSON Lines, `ParseLine`).
2. Testes com `-race` por pacote e a suíte real do PostgreSQL.
3. `docs/SPEC.md`, `docs/TESTING.md`, `AGENTS.md` e Docker.
4. Revisão independente (2026-10-07): achados corrigidos no código e
   registrados em TESTING §9; os que ficaram, em TESTING §8 item 12.
5. Registro no `CHANGELOG.md` da raiz e commit, só com o "sim" do usuário.

---

## 6. Porte do `go-webhook-nats-server` (2026-10-11)

- **Dump sem *hard link*: `rename` depois de conferir o nome livre** (SPEC
  §12.1). Achado da revisão independente do `go-webhook-nats-server`; num
  sistema de arquivos sem *hard link* todo dump falhava. Portado igual ao
  `go-syslog-nats-server`, para que a emergência dos dois continue a mesma.
- **Mantido de propósito:** o `fsync` do diretório continua sendo falha do dump
  (o N o tornou melhor esforço). Sem ele, o nome pode não sobreviver a uma queda
  de energia e o processo sairia dizendo que salvou (SPEC §12.3). Contestável.
- **O temporário de um dump interrompido é informado** no boot (um `WARN` com os
  caminhos), e o gêmeo de um dump já ligado é removido: o `Leftovers` do N,
  trazido pela revisão independente deste porte. Igual no
  `go-syslog-nats-server`.
- **Parsers de INI e de flags sem eco** (SPEC §13.2), o mesmo dia: o
  `flags.go` do N inteiro (uma flag sem valor não engole a seguinte, só se nomeia
  uma flag que este servidor conhece, nada depois de um valor é citado) e o
  `ini.go` do `go-syslog-nats-server` (a linha nunca é citada). Fica, decidida
  como no N, a brecha da cauda com traço depois de um valor não vazio
  (`--x=a -b`): escondê-la esconderia os erros de digitação (TESTING §8
  item 13).

## 7. Porte das correções do RP e do R (2026-10-13)

As correções da revisão independente do `go-relplog-pg-server` (RP) e as do
`go-relplog-server` (R) que valiam aqui, conferidas uma a uma (CHANGELOG,
entradas "2026-10-13 — RP, RN" e "2026-10-12 — R"). Sem entrevista: as escolhas
são as que o RP já registrou nas DECISOES dele; contestáveis.

- **A `LOGHUB_EB_URL` exige usuário e banco** (SPEC §10.9), em vez do padrão do
  registry (`syslog`) ou da libpq (o usuário do sistema): a URL é dona das duas
  chaves, e duas leituras dela não podem chegar a dois bancos. Muda o que era
  aceito: uma URL sem usuário ou sem caminho passa a ser exit 1.
- **`sink.postgres.pool_size` ≥ `sink.workers + 1`** (SPEC §10.5), não
  `≥ workers`: a sonda de saúde precisa de uma conexão. Um `pool_size` igual aos
  workers, aceito antes, passa a ser exit 1.
- **Tetos de configuração** (SPEC §13.2): os do RP, com `listen.udp_listeners`
  junto do `tcp_listeners` (1–256). Folgados de propósito.
- **O prazo da reingestão conta sem progresso** (SPEC §12.2): muda a regra deste
  servidor (um prazo único desde o início), como no RP e no RN.
- **Não portado, por não se aplicar:** o RP deixa de enfileirar uma transação
  cuja sessão acabou durante a montagem (o `librelp` a retransmitiria). Aqui o
  que foi lido já saiu do emissor, e o contexto cancelado ainda enfileira
  (SPEC §5.7).
- **Ficaram para depois** (SPEC §22.1 itens 5 e 6), por não estarem na lista
  do porte: `23502` fatal e o prazo próprio da validação do boot, que o RP já
  tem.
