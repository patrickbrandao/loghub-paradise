# go-syslog-nats-server — Decisões de projeto

Registro da entrevista de 2026-10-07 que definiu o `go-syslog-nats-server`.
Este arquivo é a **entrada** da `docs/SPEC.md`; a SPEC é a fonte da verdade, e
cada decisão abaixo aparece nela com o número da seção.

## O pedido

> Nesse mesmo molde, crie o software go-syslog-nats-server.

"Esse molde" é o `go-syslog-pg-server`, criado no mesmo dia como corte do
`../go-syslog-server`: ring só em memória, emergência em JSON Lines, sem TLS,
sem filtro nem rate-limit, dependências mínimas. Este módulo é o código do
`go-syslog-pg-server` com o sink trocado pelo **sink NATS do pai**
(`internal/sink/nats.go`, com os comentários) e com o heartbeat beacon
(`internal/control/`) de volta. A numeração da SPEC é a do pai (o código cita a
spec por número), e uma seção removida mantém o título com uma nota.

## Estado do monorepo

| Pasta | Estado |
|---|---|
| `go-syslog-nats-server/` | **Este módulo**, v1, criado em 2026-10-07 |
| `go-syslog-pg-server/` | O molde: o mesmo corte, com a fila `loghub.inbox` do PostgreSQL. Fonte do código fora do sink e das decisões de corte; **nunca importado** |
| `go-syslog-server/` | O pai: rings `memory`/`sqlite`/`redis`/`none` × sinks `nats`/`redis`/`sqlite`/`postgres`/`mysql`/`clickhouse`/`none`, TLS, filtro, rate-limit, beacon. Fonte do sink NATS e do beacon; **nunca importado** |
| `go-syslog-standard-server/` | O antigo, referência do pai |

---

## 1. Decisões do usuário (entrevista de 2026-10-07)

Nas quatro perguntas o usuário escolheu a opção recomendada.

| # | Tema | Decisão | SPEC |
|---|---|---|---|
| 1 | Heartbeat beacon | **Fica.** Ele precisa do NATS, e aqui o NATS é o único sink: `type=beacon` em `loghub._ctl.beacon` (`LOGHUB_CTL_NAMESPACE`), por NATS **core**, na conexão do sink, na primeira conexão, em toda reconexão e a cada `LOGHUB_BEACON_INTERVAL` (15 s; `0` desliga só o periódico). Sem sonar, `ping` e `pong` | §11, §16.5 |
| 2 | TLS | **Sai em todo lugar**, como no `go-syslog-pg-server`: sem listener RFC 5425 e sem TLS até o NATS. O cliente é sempre texto claro (`sink.nats.tls`, `tls_ca` e `tls_insecure` saíram); toda entrada de `sink.nats.urls` precisa ser `nats://` (`tls://`, `ws://`, `wss://` são exit 1); a `LOGHUB_EB_URL` só aceita `nats://` (lista com vírgula = cluster), e `tls://` é exit 1 com uma mensagem que nomeia o TLS. Um servidor NATS que exige TLS é FATAL no boot (exit 3) sem que nenhum `ClientHello` seja enviado | §1.2, §10.7, §10.9 |
| 3 | Modos de publicação | **Os dois ficam:** `jetstream` (padrão, `PubAck` por mensagem, deduplicação por `Nats-Msg-Id`) e `core` (*best effort*, opt-in, `WARN` no boot) | §10.7, §19 |
| 4 | Subjects por facility/severidade | **Saem.** O subject é sempre `loghub.<space>.syslog`; a chave `sink.nats.subjects` não existe | §1.2, §3.8 |

---

## 2. Padrões assumidos (não perguntados, contestáveis)

### Do sink NATS

- **O sink é o do pai, sem TLS e sem subjects granulares**: uma conexão por
  processo, preguiçosa, `MaxReconnects(-1)`, `ReconnectBufSize(-1)`,
  `WithExpectStream`, `WithRetryAttempts(0)`, `WithPublishAsyncTimeout`, a
  validação do stream no boot (cobre `loghub.<space>.syslog`, nunca cobre
  `<ctl_namespace>.beacon`, não selado, `no_ack` falso, a maior mensagem cabe
  em `max_payload`, `max_msg_size` e no registro do file store) e a tabela de
  classes de erro (§10.7).
- **`WithPublishAsyncMaxPending` = `sink.workers × sink.batch`**: no pai somava
  `ring.max_inflight`, que era do `ring=none` e saiu.
- **O sink recusa ele mesmo toda URL que não seja `nats://`**, atrás da
  validação da configuração (defesa em profundidade), e instala um `TLSCertCB`
  que recusa, porque o `nats.go` se promove a TLS sozinho quando o servidor
  anuncia `tls_required` (§10.7).
- **`sink.workers` volta ao padrão 8 do pai**, sem o `WARN` acima de 16 do
  `go-syslog-pg-server`: aqui os workers dividem **uma** conexão NATS, e não
  prendem uma conexão cada de um orçamento compartilhado.
- **Nome do cliente NATS** `slnats/<AgentName>@<Hostname>`, visível no `connz`
  do `nats-server` (no `go-syslog-pg-server`, o `application_name`
  `slpg/...`).
- **O stream é `LOGHUB_SYSLOG`** e o servidor nunca o cria:
  `docker/nats/stream.json` é o contrato que o operador aplica, e cobre só
  `loghub.default.syslog` (sem o `loghub.default.syslog.>` do pai, que servia
  aos subjects granulares).
- **O `space` de uma linha reingerida decide o subject dela**: um dump de outra
  vida, com outro Workspace, só volta se um stream cobrir aquele subject; senão
  o boot sai com 3 e o arquivo fica (§12.2).

### Herdados do corte `go-syslog-pg-server`

- Ring só `memory`; as chaves `sink.type` e `ring.mode` não existem, e o log de
  boot e o `syslog_config_info` dizem `ring_mode="memory"` e `sink_type="nats"`
  (§14.1).
- `SIGHUP` recarrega só `log.level`, `log.access` e `log.access_sample` (§16.3).
- A recepção não lê o PRI; o codec do cabeçalho do item foi apagado
  (`FuzzHead` saiu); `item.ItemOverhead` = 256 B (§2.2, §3.4, §9.2).
- O vetor dourado com `transport=tls` roda reetiquetado como TCP (§18.2).
- Chaves do pai que saíram: na linha de comando ou no INI, parâmetro
  desconhecido (exit 1); no ambiente, um `WARN` único que as nomeia, porque um
  mesmo `.env` pode alimentar mais de um servidor da família (§13.2, §16.2).
  Aqui a lista inclui ainda `SYSLOG_SINK_NATS_SUBJECTS`, `SYSLOG_SINK_NATS_TLS*`
  e as do PostgreSQL; `LOGHUB_BEACON_INTERVAL` e `LOGHUB_CTL_NAMESPACE` voltaram a
  ser parâmetros.

### Emergência (a decisão 3 do `go-syslog-pg-server`, herdada)

- Arquivo `<emergency.dir>/emg-<agent_uuid>-<unixnano>.jsonl`, um JLH pronto por
  linha, byte a byte o que o worker teria publicado; malformadas de fora com
  `syslog.accept_malformed=false`.
- Escrito sob um nome temporário oculto (`.emg-…jsonl.tmp`), `fsync`, `link`
  para o nome final, `fsync` do diretório; qualquer falha remove o que foi
  escrito e é exit 3 (`emergency_failed`). Um dump cortado numa fronteira de
  linha não pode mais parecer limpo (§12.1).
- Reingestão no boot, antes de qualquer transporte, do mais antigo para o mais
  novo, **sem conversão** e **em fluxo** (pedaço a pedaço, nunca o arquivo
  inteiro na memória). O subject, o `Nats-Msg-Id` e os `X-LogHub-*` saem do
  cabeçalho da própria linha (§12.2).
- `emergency.ParseLine` é a única porta entre o disco e o stream: o NATS não lê
  o `Data`, então uma linha que passasse sem ser um JLH deste servidor seria
  publicada e confirmada como legítima. A porta exige o cabeçalho byte a byte
  igual ao de `jlh.AppendHeader`, só escapes `\u` de controle, só inteiros
  simples de até 20 dígitos, profundidade ≤ 8, `space` na gramática (vira token
  do subject), `type=syslog` e `step=0` (fixos nos headers), o payload por
  posição igual ao do parser, com `message` string (§12.2).

### Nomes e compatibilidade

- Module path `github.com/patrickbrandao/loghub-paradise/go-syslog-nats-server`;
  binário `cmd/go-syslog-nats-server`; `/app/go-syslog-nats-server` na imagem;
  tags `go-syslog-nats-server/vX.Y.Z`.
- **Prefixo de env `SYSLOG_` e de métrica `syslog_` inalterados**: mesma
  família. As métricas são as do `go-syslog-pg-server` sem as do PostgreSQL,
  mais `syslog_beacons_total{result}`, `syslog_sink_nats_duplicates_total` e
  `syslog_sink_nats_reconnects_total`, com os nomes do pai (§14.1).
- **`admin.bind` continua `127.0.0.1:9102`**: não rode dois servidores da
  família no mesmo host sem trocar o de um deles.
- Docker: prefixo `slnats` nos nomes de projeto, contêiner e volume; portas do
  host distintas das do pai (5141–5162, 19101–19122) e das do
  `go-syslog-pg-server` (5180, 19180, 15480): syslog `5181`, admin
  `127.0.0.1:19181`, cliente NATS `127.0.0.1:14281`, monitor do NATS
  `127.0.0.1:18281`.

### Dependências

- **Três diretas em produção**: `nats.go` (com as transitivas `nkeys`, `nuid`,
  `klauspost/compress` e `golang.org/x/crypto`), `go-loghub-ident` e
  `go-loghub-uuidv7`. `jsonschema/v6` só em teste; o `pgx/v5` do molde saiu
  do `go.mod` com o porte dos testes. Binário de 12,5 MB, contra 17,2 MB do
  `go-syslog-pg-server` e 31,8 MB do `go-syslog-server` (o mesmo build
  `darwin/arm64`, flags padrão). SPEC §17.3.

---

## 3. Decisões herdadas

### Do `go-syslog-pg-server` (`../go-syslog-pg-server/docs/DECISOES.md`)

| # de lá | Tema | Aqui |
|---|---|---|
| 1 | Admin e `LOGHUB_EB_URL` ficam; filtro e rate-limit saem | **vale**, com a URL só em `nats://` |
| 2 | Unix datagram, `SIGHUP`, PROXY no TCP, charset legado ficam | **vale** |
| 3 | Emergência em JSON Lines com JLH pronto | **vale** |
| 4 | Sem TLS em lugar nenhum | **vale**, refeita como a decisão 2 desta entrevista, para o NATS |

Dos padrões assumidos de lá, **não** valem aqui: a saída do beacon (voltou,
decisão 1), `sink.workers` 4 com `WARN` acima de 16 e `pool_size` (o NATS usa
uma conexão), o `application_name` `slpg/...` (aqui o nome do cliente NATS) e as
portas `slpg`.

### Do `go-syslog-server` (`../go-syslog-server/docs/DECISOES.md`)

As decisões 1 a 18 do pai continuam valendo, salvo:

| # do pai | Tema | Aqui |
|---|---|---|
| 1, 2, 3, 4, 7, 12, 13, 14, 15 | pastas, RELP fora, moldura por conexão, `message`/`raw`, cru no ring e parse no dreno, TAG estrita, truncamento, schema só em teste, PKTINFO | **valem** |
| 5 | Transportes | vale **sem o TLS** (RFC 5425): UDP, TCP e unix |
| 6 | Todos os recursos da standard | **não vale**: um ring, um sink; sem filtro e sem rate-limit |
| 8 | Beacon | **vale** (decisão 1 desta entrevista): sempre, porque o sink é sempre NATS |
| 9 | NATS `core` | **vale** (decisão 3) |
| 10 | `LOGHUB_EB_URL` | vale **só com `nats://`**; `tls://` é exit 1 |
| 11 | Ring Redis | **não vale** |
| 16 | Sink padrão `nats` em `jetstream` | **vale**, e é o único |
| 17 | mTLS | **não vale** |
| 18 | Opcionais | o charset legado e o PROXY protocol (só TCP) **valem**; os subjects por facility/severidade **não** (decisão 4) |

Também saem dos padrões assumidos do pai: o dump `.bin` (virou JSON Lines), os
sinks SQL, o ring Redis, o `loghub.default.syslog.>` do stream e os compose
ring × sink (aqui há um).

---

## 4. Riscos conhecidos

- **Sem TLS até o NATS.** Credenciais (senha, token, o JWT do `.creds`) e
  eventos trafegam em claro: o NATS precisa estar numa rede confiável. Um
  servidor que exige TLS é recusado no boot (exit 3).
- **Sem TLS na entrada.** Syslog cifrado exige um terminador na frente (com
  PROXY protocol para manter o `src-ip`) ou o `go-syslog-server`.
- **`kill -9` perde o backlog do ring.** O dump cobre FATAL e `SIGTERM`, não
  `kill -9`, OOM nem pane (SPEC §19).
- **`core` é *best effort*.** Sem stream, uma mensagem sem assinante some, e
  nada deduplica.
- **A durabilidade depois do `PubAck` é a do stream** (`storage`, réplicas,
  `sync_interval`), configurada por quem administra o NATS.
- **Dump grande na saída.** Converter todo o ring pendente custa CPU e disco no
  momento do incidente; com o ring cheio (512 MiB de cru) o arquivo chega a
  vários GiB no pior caso (12× o cru). A reingestão lê em fluxo, mas publica
  tudo de novo antes de abrir os transportes.
- **Dump de outro Workspace** trava o boot até o `boot_timeout` (ou na hora com
  `10060`, se outro stream o cobre) a cada restart, com o arquivo intacto, se
  nenhum stream cobrir o subject dele: por desenho, o operador decide se cria o
  stream ou move o arquivo (SPEC §12.2).
- **Várias URLs**: o veredito do boot (`fatal` ou `network`) pode depender da
  ordem aleatória em que o cliente tenta os servidores (TESTING §8).
- **Os servidores da família no mesmo host** colidem no `admin.bind` padrão
  (9102).

---

## 5. Ordem de entrega

1. Corte do código (o sink NATS e o beacon do pai sobre o `go-syslog-pg-server`;
   registry, métricas, `removed.go`).
2. Porte dos testes para o NATS, com `-race`, e a suíte real do NATS (com e
   sem JetStream): feito.
3. `docs/SPEC.md`, `docs/TESTING.md`, `AGENTS.md` e Docker.
4. Revisão independente do código NATS: feita (2026-10-07), quatro achados
   corrigidos e os conhecidos registrados (TESTING §8, §9). O código herdado
   passou pela revisão do molde.
5. Registro no `CHANGELOG.md` da raiz e commit, só com o "sim" do usuário.

---

## 6. Porte das correções do `go-webhook-nats-server` (2026-10-11)

O `go-webhook-nats-server` (o N) nasceu em 2026-10-10 do sink NATS deste módulo,
e a revisão independente dele achou defeitos que valiam aqui também (lista "A
portar" da entrada N do `CHANGELOG.md` da raiz). Portados sem entrevista, cada
um com SPEC, teste e mutação (TESTING §9); todos contestáveis.

- **Segredos fora das mensagens.** Os logs de conexão e reconexão mostram só
  esquema e host (o `ConnectedUrlRedacted` do `nats.go` não esconde token); o
  erro de uma URL que não parseia não cita nem o trecho que o `net/url` cita;
  `sink.nats.urls` recusa caminho, consulta e fragmento, como a `LOGHUB_EB_URL`
  já recusava (achado no porte: um token com `/` virava o host discado, citado
  em cada log); os parsers de INI e de flags não ecoam a linha nem o valor.
- **O veredito em todo `Write`**, e em `core` lido no próprio `Write` (o `-ERR`
  vem antes do `PONG` do *flush*).
- **O contrato do stream em toda checagem de saúde**, e o `max_payload` em
  `core` também; o `validated` só controla o log. A rechecagem dos subjects de
  outro `space` do N não se aplica: aqui não há `cover`.
- **`max_bytes` com `discard: new` menor que um registro** é recusado.
- **Recusas de autenticação no `CONNECT` são fatais**, pelo texto do servidor
  ("User Authentication Expired/Revoked") e pelos sentinelas `ErrAuthRevoked` e
  `ErrAccountAuthExpired`.
- **Dump sem *hard link*: `rename`** depois de conferir o nome livre, igual no
  `go-syslog-pg-server` (de onde esta emergência é copiada).
- **O temporário de um dump interrompido é informado** no boot, e o gêmeo de um
  dump já ligado é removido (o `Leftovers` do N; não estava na lista, e veio
  porque a revisão deste porte notou que nada dizia ao operador que ele
  existia). Igual no `go-syslog-pg-server`.
- **Flags sem eco também quando uma engole a seguinte** ou quando o valor vem
  depois de um espaço (revisão deste porte). O N tem os mesmos dois caminhos.
  A revisão do porte de volta ao N achou mais três, portados do N no mesmo
  dia: só se nomeia uma flag que este servidor conhece, e nada depois de um
  valor é citado. Fica, decidida como no N, a brecha da cauda com traço depois
  de um valor não vazio (`--x=a -b`): escondê-la esconderia os erros de
  digitação (TESTING §8 item 16).
- **"Sem respondedores" na primeira checagem é FATAL sem a sonda
  `AccountInfo`** (achado na revisão do porte para o fast): a sonda não
  distinguia nada e o `$JS.API.INFO` negado a um papel mínimo virava veredito
  permanente. O N mantém a sonda.
- **Mantido de propósito, divergindo do N:** o `fsync` do diretório do dump
  continua sendo falha do dump, também depois do `rename` (o N o tornou melhor
  esforço). Sem ele o nome pode não sobreviver a uma queda de energia e o
  processo sairia dizendo que salvou (SPEC §12.3).
- **Não se aplica:** o defeito do `LOGHUB_EB_URL` no compose do N. Aqui as
  chaves donas são escritas sem valor (`KEY:`) e o README manda apagá-las para
  usar a URL; conferido com `docker compose config` (TESTING §8 item 15).

## 7. Porte das correções do RN, do RP e do R (2026-10-13)

As correções das revisões independentes do `go-relplog-nats-server` (RN), do
`go-relplog-pg-server` e do `go-relplog-server` que valiam aqui, conferidas uma
a uma. Sem entrevista; as escolhas são as que o RN já registrou, contestáveis.

- **Código comum primeiro no `go-syslog-pg-server`**, copiado para cá por fusão
  de três vias e readaptado na redação do NATS (a regra de sincronia SP↔SN).
- **`sink.workers` ≤ 256** (o teto do RN, não o 1024 do SP): todos os workers
  dividem a única conexão NATS, e `2 × workers × batch` (o
  `PublishAsyncMaxPending`, SPEC §10.7) precisa caber num `int`.
- **`retention: interest` é FATAL**, `10158`/`10167`/429 são `network`,
  assinatura negada em `_INBOX` é veredito e um `space` estrangeiro em `core`
  gera um `WARN`: as pendências 5 a 8 da SPEC §21.1, com a direção já escrita lá,
  vieram junto do sink do RN porque o código era o mesmo.
- **O prazo da reingestão conta sem progresso** (SPEC §12.2), como no RN.
- **Não portado, por não se aplicar:** o fim da sessão antes do `Enqueue` (SPEC
  §5.7), como no `go-syslog-pg-server`.

---

## 8. Correções transversais da revisão independente do `go-syslog-server` (2026-10-13)

Itens X1–X4 do plano da revisão do porte do S, mais a conferência de C3, D1,
E1, E2 e A2-minor. A mesma implementação no irmão (regra de sincronia).

- **X1, o sinal entre os *bind* e o primeiro *accept*** (SPEC §13.1, §13.5):
  conferido uma vez, depois de todos os *bind* e antes do primeiro `Start`
  (`bindCtx.Err()` → 130). Com IP literal ou socket unix nenhum *bind* falha,
  e o servidor abria e saía 0 no shutdown que seguia.
- **X2, o motivo do aborto** (SPEC §13.4): das duas opções do plano, o
  `Swap` do dump **sob o `lossMu`** (`settleDump`, nos dois ramos: dump
  desligado e dump escrito), não reordenar o `abortLoss`. É a regra que já
  valia para `enterDumpPhase` e `oweDump` — toda passagem do contador sob o
  mutex — e não depende da ordem das duas escritas dentro do aborto. O
  gancho `inAbort` (nulo no binário, como `beforeDump`) existe só para o teste
  forçar a intercalação.
- **X3, `"store is closed"`**: o texto do `ErrStoreClosed` do nats-server
  2.14.6 (`server/store.go:43`) entra em `storeFailedIsTransient`; o
  `"store closed"`, que nenhum servidor manda, fica (inofensivo).
- **C3, teto do `MaxPending`**: não se aplica. Não há `ring.max_inflight`
  (sem `ring=none`), e `2 × workers × batch` já é limitado pelos tetos de
  `sink.workers` (256) e `sink.batch` (65 536): no máximo 33 554 432.
- **E2**: em 2026-10-13 não se aplicava (a moldura nunca soltava o buffer, não
  havia `frameKeepCap`). **Aplicado em 2026-10-14** (§9): com o `frameKeepCap`
  portado, a liberação só acontece com o leitor vazio, na forma do S.
- **E1**: em 2026-10-13 não se aplicava (a leitura do cabeçalho PROXY não
  conferia `closing`). **Aplicado em 2026-10-14** (§9), na forma do S:
  `closing && (err != nil || mode == required)`.
- **A2-minor**: não se aplica. `closeTransports` fecha em ordem reversa, como o
  comentário diz, e não há ring Redis.

## 9. Pendências conhecidas fechadas (2026-10-14)

As quatro "Pendências conhecidas" de SPEC §21.1, triadas contra o
`go-syslog-server` (S), que já as tinha resolvido no porte de 2026-10-13. Duas
por correção, portada como está; duas por decisão do `go-relplog-server` (R,
decisão 16 dele), que o S trouxe como contestável e que vem para cá do mesmo
jeito: **o usuário não as decidiu para este módulo**. Mantêm a igualdade com o
S, o R e o irmão (`go-syslog-pg-server` ↔ `go-syslog-nats-server`).

### Correções (sem mudança de contrato)

- **Shutdown contado como falha do PROXY** (SPEC §4.6): uma espera de cabeçalho
  vencida pelo `Close` não conta nem loga nada; em `optional`, as linhas já no
  leitor de um par sem cabeçalho seguem para a moldura. Forma do S
  (`closing && (err != nil || mode == required)`), nunca a do R. Testes:
  `TestProxyShutdownIsNotAProxyFailure`, `TestProxyOptionalBufferedLinesSurviveClose`
  (este com a conferência, que o S não tem, de que `required` não conta
  `absent` nem `reason="proxy"`).
- **Custo fixo por conexão TCP** (SPEC §5.4, §15.1): leitor de 4 KiB e
  `frameKeepCap` de 16 KiB, solto só com o leitor vazio. Testes:
  `TestIdleTCPConnectionFixedCost`, `TestFramerReleasesALargeBuffer`,
  `TestFramerKeepsTheBufferBetweenBufferedFrames`. Veio junto a busca do NUL só
  antes do LF (`indexTerminator`, do S).
- **Prazo vencido no meio da assinatura PROXY** (SPEC §4.6): aceito e
  documentado, sem mudança — os bytes viram o começo de uma linha registrada;
  nada se perde. Igual no S.

### Decisões portadas (contestáveis)

- **Teto por IP:** `tcp.max_conns_per_ip` conta o endereço **exato** (um /128 no
  IPv6), documentado em SPEC §4.4. Contar por prefixo IPv6 foi recusado: um /64
  pode ser uma LAN inteira de emissores, e uma cota por prefixo deixaria um
  deles calar os outros. Contra um /64 hostil, `tcp.max_conns` e o firewall.
  Teste: `TestPerIPLimitCountsTheExactAddress`.
- **Offset ISO fora de faixa na RFC 3164:** segue a skill (aplicado como veio,
  sem aviso), registrado em SPEC §6.5.1 e ressalvado em §6.9, **sem desvio
  D9**: um desvio de conversão faria a mesma linha virar JLH diferente conforme
  o transporte (o irmão RELP e o pai publicam o mesmo `type` no mesmo destino).
  Se a skill passar a conferir, todos mudam juntos. Teste:
  `TestRFC3164ISOOffsetIsNotRangeChecked`.

---

## 10. Porte das pendências do RP/RN (2026-10-14)

O usuário aprovou portar e listar o que a triagem achou igual aqui
(`tasks/port-to-R-S-SP-SN.md`); nenhum destes estava em §21.1. Corrigidas, com
teste que falhava antes e mutação, na forma do RP/RN e em sincronia com o
irmão: o aborto fecha o ring antes de contar (SPEC §13.4); piso de
`ring.max_bytes` = classe do pool de `syslog.max_message_bytes` + 256 B (§13.2,
§15.1); `admin.bind` × `listen.tcp` pelo `sameStreamSocket` do
`go-syslog-server` (§13.2, §14); a chave derivada fora do relato do `SIGHUP`
(§16.3); e, só aqui, o veredito de um servidor que exige TLS só com um servidor no *pool* (`soleServer`, no boot e depois) e a discagem falha da reconexão fora do `ERROR` (§10.7). Registradas como regra ou lacuna, sem mudança de código (decididas,
contestáveis, como no RP/RN): a falha ao remover um dump entregue é só `ERROR`
(§12.2, travada agora por `TestRemovalFailureIsOnlyAnError`); o dump publicado
no instante do aborto fica contado e no disco, e o aborto não espera o disco
(§12.3, §13.4); `kind="fatal"` conta a recusa fatal mesmo com o boot cancelado
(§14.1); os contadores da recepção sem *padding* são lacuna de medição
(TESTING §8).
