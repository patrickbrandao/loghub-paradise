# CHANGELOG

Registro breve dos problemas encontrados, das correções, das decisões e da
lição de cada um — o suficiente para não repetir o erro. As regras e decisões
que saíram de cada item vivem nas especificações do módulo (`docs/SPEC.md`,
`docs/DECISOES.md`, `docs/TESTING.md`), que é onde uma refatoração as encontra.
Os relatórios de auditoria que ficavam em `tasks/` foram incorporados a elas e
removidos em 2026-10-11; os da rodada de 2026-09-14 (001 a 015 e o diff
`CORRECOES-2026-09-14`) seguem no commit `d1d3864` do repositório antigo,
`github.com/patrickbrandao/webhook-paradise` (abandonado — veja a entrada de
2026-10-11 sobre a renomeação). Módulos: **W** = `go-webhook-server`,
**S** = `go-syslog-server`, **F** = `go-webhook-fast-server`, **P** = `go-webhook-pg-server`,
**SP** = `go-syslog-pg-server`, **SN** = `go-syslog-nats-server`,
**N** = `go-webhook-nats-server`, **R** = `go-relplog-server`,
**RP** = `go-relplog-pg-server`, **RN** = `go-relplog-nats-server`. Nas entradas anteriores a 2026-10-05, **S** é o
código que hoje vive em `go-syslog-standard-server`; desde então, `go-syslog-server`
é a reescrita LogHub.

## 2026-10-13 — S: porte das correções do R, RP e RN

Pedido: trazer para o `go-syslog-server` os "a portar" das entradas R e RP, RN
abaixo, conferindo cada um no S, com teste que falha antes (conferido por
mutação), regra na SPEC e decisões na DECISOES; sem o rate-limit que segura.
Cinco sub-agentes por área (ciclo de vida e dump, configuração, NATS, SQL,
transportes/Redis/conversão), cada um num worktree isolado, depois integração
e a suíte inteira contra os destinos reais.

- **As 30 pendências** da SPEC §22.1 do S estão corrigidas; §22 diz em que
  seção ficou a regra de cada uma. As quatro "a decidir pelo usuário" seguem a
  decisão do R, registradas como **contestáveis** em `docs/DECISOES.md` §2: teto
  por IP pelo endereço exato; offset ISO fora de faixa como a skill;
  `retention: interest` recusado; reingestão com `sink=none` mantém os dumps.
- **Ciclo de vida e dump:** posse única da contagem do aborto na forma final do
  RN (`lossMu`, `enterDumpPhase`/`oweDump`, `SetAbandon`), `quitNow`, bind
  cancelado = 130 (nos quatro transportes e no admin), `ErrNotDurable` ao abrir
  o diretório e `fsync` dele no `Remove`, `withLifetime` nos health checks,
  transportes fechados em paralelo.
- **Configuração:** `knownSectionOf`, valor colado ao nome, continuação de
  segredo no INI, switches com valor, `--config=` vazio, BOM, tetos (mais os
  que só o S precisa: `ring.sqlite.commit_batch`, pools do MySQL e ClickHouse,
  `*_listeners`), `@` depois de `/ ? #`, fragmento e as regras da URL
  PostgreSQL do RP; `pool_size ≥ workers + 1` estendido a MySQL e ClickHouse
  (as três sondas usam o pool). **Decisão:** a `LOGHUB_EB_URL` PostgreSQL passa
  a exigir usuário e banco, como no RP (o S tinha o mesmo defeito).
- **NATS:** `nats.go` do R (pendências 13–20, `10077`, `discard=new`, negação
  mascarada no `core`) e os achados do RN **adaptados ao TLS do S**: só o TLS
  que nunca vai dar certo (certificado que não verifica, `ErrSecureConnWanted`)
  vira veredito, lido do `LastError` em `RECONNECTING`; `flushErr` + contagem de
  reconexões; `MaxPending` com folga de 2×.
- **SQL:** pendências 6–12, MySQL `1203`/`1226`/`1461` como `network`, `RULE`/
  `0A000` e `Deliver` com `logic` antes do `ctx` do RP. A sonda MySQL do R foi
  **adaptada**, não copiada: o S tem `unix`, então `src_ip NULL` + `unix` é
  linha real; o defeito do S era `NULL` com `udp`.
- **Não se aplica ao S:** o rate-limit que segura (decisão 8a do R); `FullFor`
  e o `ctx` conferido antes do `Enqueue` (o S não pré-checa o ring, e conferir o
  `ctx` ali perderia no shutdown mensagens TCP já lidas, que §13.4 manda para o
  ring); "flag removida culpava a errada" (o S não tem chaves removidas);
  `conns_refused{proxy}` (mudança de contrato de métrica do R). O prazo da
  reingestão sem progresso já valia (cada lote tem o seu); o teste veio como guarda.
- **Achado no caminho:** `indexTerminator` procurava o NUL no buffer inteiro
  antes do LF; a moldura LF custava o dobro da octet.

**Resultado:** `go vet` limpo; `go test -race` verde nos 16 pacotes com
PostgreSQL 18, MySQL 8.4, ClickHouse 25.8, dois Redis 7 (um `allkeys-lru`),
nats-server 2.14.6 (JetStream, sem JetStream, com permissões) e os emissores
reais: 681 testes + 458 subtestes, 1 pulado (D5). Mais de 120 mutantes, todos mortos. O que ficou sem prova está em TESTING §8
item 14.

**Lição.** O "a portar" de um corte é uma lista de candidatos, não de
patches: das correções vindas do RELP, três não valiam para o syslog (uma
**perderia** mensagens se portada), e duas tiveram de ser adaptadas ao que o S
tem e o pai não (`unix` na sonda MySQL, TLS no NATS). Conferir no destino antes
de copiar é o que separa um porte de uma regressão.

## 2026-10-13 — RP, RN: `go-relplog-pg-server` e `go-relplog-nats-server`, cortes do R

Pedido: a partir do R, criar os dois servidores RELP simplificados nos moldes
dos cortes do S — **RP** (memória → PostgreSQL `loghub.inbox`, molde SP) e
**RN** (memória → NATS, molde SN) —, tudo automático, sem entrevista. Um
sub-agente por módulo, em paralelo; depois revisão independente por área (cinco
áreas por módulo), correção com teste conferido por mutação e porte cruzado RP↔RN.
Decisões em `docs/DECISOES.md` de cada um (RP-1..12; RN 1–14).

- **Forma:** cópia do R **cortada**, com fusão de três vias (R como nosso, S
  como base, SP/SN como deles) e a numeração da SPEC do R. Herdado do molde:
  ring só em memória, dump em JSON Lines (um JLH por linha, reingerido em fluxo
  pelo `ParseLine` estrito), **sem TLS em lugar nenhum** (RELP só em
  `listen.tcp` `:20514`; PostgreSQL sempre `sslmode=disable`; NATS só
  `nats://` com o `TLSCertCB` que recusa), sem filtro, rate-limit (a decisão 8a
  do R deixa de valer) nem as chaves `ring.mode`/`sink.type`. RN mantém beacon,
  JetStream + core e o subject exato. Prefixos do R (`RELPLOG_`, `relplog_`);
  admin `127.0.0.1:9103` **compartilhada por R, RP e RN** (regra da família,
  como S/SP/SN na 9102); docker `rlpg-*` e `rlnats-*` em portas próprias.
- **Os `500` do R reavaliados:** sem `ring=none`, a recusa `logic` acontece
  depois do `200` e é descarte contado (`events_dropped{sink_logic}`); `500` só
  sobra para a guarda de origem e erros de protocolo do cliente. Um erro
  inesperado do ring é segurado como ring cheio, nunca `500`.
- **Dois pais conferidos por lista:** as correções da revisão do R (SPEC §22),
  do SP/SN e os portes de 2026-10-11, uma a uma com teste (TESTING §9 de cada
  módulo). Os "a portar" do R (fsync do diretório no `Remove`, `dumpPhase` +
  `Swap`, `SIGQUIT` tardio, `withLifetime`, `knownSectionOf`, `@` depois de
  `/ ? #`) chegaram pelo código do R; o molde não os tinha.

**Achados das revisões** (cada um com teste que falhava antes):
- **Aborto contado duas vezes** (herdado do R): um aborto durante
  `Pending`/`Drain`, ou entre os dois `Store`, contava N como `abort` e de novo
  como `emergency_failed`; e o dump já contado como perdido ainda podia ser
  publicado e reingerido. Agora `lossMu` + `enterDumpPhase`/`oweDump`,
  `Store.SetAbandon`, e nada é contado depois do dono (`owed == 0`). O
  `SIGQUIT` tardio chega por um canal (`quitNow`), não por uma flag lida uma vez.
- **Reingestão morria no primeiro soluço** depois de gastar o `boot_timeout`
  único com entregas bem-sucedidas (regra do SP): o prazo agora conta **sem
  progresso**. O gauge `emergency_events_pending` mostrava o tamanho do pedaço.
- **Pré-checagem do ring cheio só pelo teto de itens:** com `ring.max_bytes`
  limitando, cada retentativa de 50 ms cunhava `uuid`, copiava e montava o
  item. Agora `ring.FullFor(n)` nos dois tetos; o fim da sessão é conferido até
  o `Enqueue`.
- RP: **`pool_size == workers`** deixava a sonda de saúde sem conexão (DOWN e
  depois FATAL com o banco sadio): agora `≥ workers + 1`. **`RULE` de
  INSERT/UPDATE** na fila passava no boot e cada pedaço dava `0A000` como
  `network` para sempre: recusa no boot via `pg_rewrite`, `0A000` fatal.
  `Deliver` com SIGTERM durante uma recusa `logic` a contava sem o log dos uuids.
- RN: **TLS exigido depois do boot** nunca virava veredito — o `nats.go` só o
  relata por `LastError`, nunca pelo `ReconnectErrCB`; um flush core que
  atravessava uma reconexão confirmava o pedaço (contagem de reconexões +
  `flushErr`); `MaxPending` com folga de 2×.
- **Config:** sem tetos (`makechan` em pânico, `pool_size` dando a volta no
  `int32`, `batch_max_bytes` acima de 1 GiB sem SQLSTATE) → tetos com exit 1;
  valor colado ao nome (`--sink.postgres.passwordX`) e continuação de segredo
  numa linha do INI eram ecoados; `--dump-config=false` despejava e saía 0;
  `--config=` vazio caía no env; BOM quebrava a linha 1; flag removida do R
  depois de uma com valor culpava a flag errada; RP: tab, `[h:5432]` e `,`
  no `addr`/`host=`, `sslmode=` vazio, URL só com brancos; **a
  `LOGHUB_EB_URL` do RP passa a exigir usuário e banco** (antes caía no padrão
  do registry, e um servidor podia escrever no banco errado).
- Sinal entre a reingestão e o bind de um nome de host saía 1, não 130.
- **Flake** de `TestPostgresWrongPasswordIsFatal`: com as vagas esgotadas o
  PostgreSQL responde `53300` antes de conferir a senha.

**Resultado:** RP — 3 dependências diretas (pgx, ident, uuidv7), binário
17,3 MB (31,9 no R); `-race` verde com PG 18 e rsyslog real, 402 testes de
primeiro nível + 281 subtestes, 1 pulado (D5), cobertura 91,1 %; ~590 mil
msg/s numa sessão. RN — 3 dependências (nats.go, ident, uuidv7), 12,6 MB;
`-race` verde com nats-server 2.14.6 (JetStream, core, permissões, `tls {}`) e
rsyslog real, 435 + 332, 1 pulado, cobertura 91,6 %; ~530 mil msg/s. Fuzz de
60 s nos alvos tocados sem falha. RP e RN diferem só onde SP e SN diferem.

**A portar** (candidatos; nada conferido nos irmãos):
- R, SP, SN: posse do aborto (`lossMu`, `SetAbandon`, `owed == 0`),
  `quitNow`, bind cancelado = 130, `ErrNotDurable` na abertura do diretório,
  `FullFor` e `ctx` antes do `Enqueue`, `Deliver` com `logic` antes do `ctx`,
  tetos de config, valor colado ao nome, switches com valor, `--config=`
  vazio, BOM, continuação de segredo no INI.
- SP, SN: prazo da reingestão sem progresso; gauge do pedaço; derivados
  comentados no `--dump-config`; sinônimos de charset no SIGHUP.
- SP, R (PostgreSQL): `pool ≥ workers + 1`, `RULE`/`0A000`, `addr` com tab/`,`,
  `sslmode=` vazio, URL sem usuário/banco.
- SN, N, R (NATS): veredito do TLS na reconexão, contagem de reconexões +
  `flushErr`, folga do `MaxPending`.

**Lição.** Um corte de dois pais herda os defeitos dos dois e as correções de
nenhum, a menos que alguém confira por lista. E as janelas de contagem no
shutdown só aparecem quando o aborto cai em cada uma: os revisores as
reproduziram reencenando a sequência exata, não esperando a corrida.

## 2026-10-12 — R: `go-relplog-server` desenvolvido por completo

Pedido: desenvolver o `go-relplog-server` (RELP do rsyslog/`omrelp`) a partir da
especificação extraída, com o S como modelo, sem entrevista. Decisões em
`docs/DECISOES.md` (1–16 e 8a); a SPEC é a do S adaptada, com §5 (RELP) nova.

- **Módulo independente** copiado do S e adaptado: só `listen.tcp` (`:20514`) e
  `listen.tls`, RELP nos dois; UDP, unix e RFC 6587 saíram. Envelope JLH
  `type=syslog` com `transport` `tcp`/`tls`, `framing` `octet-counting` e
  `x-transport: "relp"` (D9). Mesmos rings, sinks, beacon, dump, filtro,
  `LOGHUB_EB_URL`; prefixos `RELPLOG_`, `relplog_`, admin `127.0.0.1:9103`,
  `relplog:ring`; stream NATS `LOGHUB_SYSLOG` compartilhado com o S.
- **O `librelp` decide o protocolo** (lido em `src/`): um `500` **destrói** a
  mensagem no emissor (`scrsp.c`). A especificação extraída respondia
  `500 ring full` e `500 shutdown`: perda na origem. Agora ring cheio ou
  indisponível, sink DOWN e shutdown **seguram** a transação; passado
  `relp.backpressure_timeout` (30 s, abaixo dos 90 s do `omrelp`) a sessão fecha
  sem resposta e o rsyslog retransmite. `500` só para a recusa `logic` final com
  `ring=none`, a guarda de origem e erros de protocolo do cliente.
- **`0 serverclose 0\n`** sem o espaço que o leitor do `librelp` recusa; o
  `serverclose` sai depois de todas as respostas decididas, e nunca no `close`
  do cliente. O `Close` do transporte só vence o prazo de leitura, para não
  perder `200` de mensagens já no ring.
- **Memória limitada:** janela por conexão (`relp.window` 128) e teto global
  das cópias em voo (`relp.inflight_max_bytes` 64 MiB, `2 × classe + 8 KiB` por
  transação); buffer de frame que cresce com os bytes que chegam.
- **Rsyslog real** (8.2410, `librelp` 1.11, suíte `TestEmittersRELP*`): o `500`
  também suspende a ação e **quebra a sessão**, e as vizinhas da janela já
  aceitas voltam duplicadas (5–6 por `500`). Por isso o **rate-limit segura**
  em vez de recusar (decisão 8a; a primeira versão dava `500 rate limited`). O
  `librelp` só percebe uma sessão fechada quando envia de novo.
- **As 30 pendências do S** foram corrigidas na criação (SPEC §22 do R cita a
  seção de cada uma).
- **Revisão independente** (RELP, app, config/dump, sinks, docs), cada achado
  com teste conferido por mutação: retentativa que acordava depois do aborto e
  ainda enfileirava (duplicata); teto em voo que contava só a cópia (5× o teto
  com linhas pequenas); frame lido e não despachado fora da reconciliação;
  motivo `server_closed`; aborto entre `Drain` e dump não contado, e dump falho
  contado duas vezes (agora `Swap` com um dono só); `SIGQUIT` tardio perdido;
  transportes fechados em sequência (um `closeGrace` por transporte); cauda de
  senha partida pelo shell ecoada; `@` depois de `/`, `?`, `#` numa URL e
  fragmento numa URL PostgreSQL; `Remove` do dump sem `fsync` do diretório;
  health checks sem prazo próprio; NATS `10077` de stream cheio ou fechando era
  fatal; `discard=new` com `max_bytes` abaixo de um registro; negação de
  permissão mascarada no `mode=core`; MySQL `1203`/`1226`/`1461` fatais; sonda
  MySQL com `unix`/`src_ip NULL` recusando tabela legítima.

**Resultado:** `go test -race` com PostgreSQL 18, MySQL 8.4, ClickHouse 25.8,
Redis 7 (um com `allkeys-lru`), nats-server 2.14.6 (com e sem JetStream, com
permissões) e o rsyslog real: 16 pacotes verdes, 773 testes e subtestes, um
pulado (vetor `transport=file`, fora por D5); cobertura 88,5%. Fuzz de 60 s
nos oito alvos sem falha (`FuzzFrame` contra um oráculo independente).
`BenchmarkRELPSession`: ~486 mil msg/s numa sessão, ring em memória.

**A portar** (candidatos; o código é o mesmo, mas nada foi conferido nos
irmãos):
- S: as 30 pendências (lista em §22 da SPEC do R).
- S, SN, N, F, W (onde houver NATS): `10077` de stream cheio/fechando como
  `network`, `discard=new` abaixo de um registro recusado no boot, negação
  mascarada no `mode=core`.
- S, F, W (MySQL): `1203`/`1226`/`1461` como `network`.
- Todos com `LOGHUB_EB_URL`: `@` depois de `/`, `?` ou `#` recusado; fragmento
  em URL PostgreSQL recusado.
- Todos com dump: `fsync` do diretório no `Remove`; posse única da contagem do
  aborto (`dumpPhase` + `Swap`) e o `SIGQUIT` tardio.
- S: fechamento simultâneo dos transportes no shutdown; prazo próprio dos
  health checks (`withLifetime`).
- S, N, SN: a cauda depois de um valor nomeada só dentro de uma seção conhecida
  (`knownSectionOf`).
- **Não se aplica** ao S: o rate-limit que segura (no TCP de syslog não há
  resposta por mensagem; descartar continua sendo o que o S pode fazer).

**Lição.** Num protocolo com confirmação, o código de resposta só vale o que o
**cliente** faz com ele: ler o `librelp` virou do avesso a especificação
extraída, e só o rsyslog de verdade mostrou o custo restante (sessão quebrada e
duplicatas por `500`).

## 2026-10-11 — Todos: o projeto passa a se chamar `loghub-paradise`

Pedido: renomear o projeto de `webhook-paradise` para `loghub-paradise`, com o
repositório novo em `github.com/patrickbrandao/loghub-paradise` e o git
recomeçado do zero.

- Os oito `go.mod` e todos os imports passaram para
  `github.com/patrickbrandao/loghub-paradise/<módulo>`; SPECs, AGENTS,
  Dockerfiles, compose e `.env.example` citam o nome novo. Os nomes dos
  módulos, binários e imagens não mudaram.
- O histórico do git local foi descartado e o repositório novo começa num
  commit único. O antigo, `github.com/patrickbrandao/webhook-paradise`, fica
  abandonado: o que só existia no histórico (os relatórios de `tasks/`) está no
  commit `d1d3864` de lá.

**Lição.** O nome do repositório está no caminho de cada import Go; renomear o
repositório é renomear os oito módulos, e só build, vet e testes de todos
confirmam que nenhum ficou para trás.

## 2026-10-11 — Todos: os relatórios de `tasks/` incorporados às especificações

Pedido: remover `tasks/` inteira, levando antes para as especificações as
decisões e correções, para que valham em refatorações futuras. Um agente por
módulo conferiu cada relatório contra o código atual — os 15 da rodada de
2026-09-14 e os 67 da auditoria estática de 2026-10-11 — e classificou:
resolvido (a regra foi para a SPEC, ou já estava lá), não procede, ou aberto.

- **Resolvidos:** a regra e o porquê estão na SPEC (e na DECISOES quando era
  uma escolha). Entraram regras que só os testes guardavam: o FATAL da sonda na
  primeira checagem, o que `kind="timeout"` conta, o `Commit` antes do
  `Release` (o comentário de `Ring.Commit` dizia o contrário em W, F e P), a
  precedência deliberada do `X-Forwarded-For` sobre o `X-Real-IP`.
- **Abertos:** a auditoria não era só sobre o que já fora corrigido. O que
  continua no código virou uma subseção **"Pendências conhecidas"** no fim de
  cada SPEC, com o problema, a consequência e a direção da correção: W 29,
  S 30, standard 20, F 13, P 12, N 11, SN 11, SP 10. O padrão do S (30): as
  correções do SP, do SN e do N nunca chegaram ao pai.
- **A decidir pelo usuário** (marcados assim nas pendências): o teto por IP em
  IPv6 (o /128 deixa um /64 contornar), o offset fora de faixa no timestamp ISO
  da RFC 3164, stream NATS com `retention: interest`, a reingestão com
  `sink=none` no S, e o shutdown do RELP sem `serverclose` (o texto foi
  alinhado ao código de referência).
- Comentários de teste que citavam `tasks/NNN` agora citam a seção da SPEC.
- `tasks/` saiu do repositório; os relatórios de 001 a 015 e o
  `CORRECOES-2026-09-14` seguem no commit `d1d3864` do repositório antigo
  (`github.com/patrickbrandao/webhook-paradise`).

**Lição.** Um relatório só pode sair quando a regra que ele produziu está onde
o próximo leitor a procura; e "foi resolvido" se confere no código, um por um.

## 2026-10-11 — F, W: o `ring.db` de uma vida anterior (TESTING §8 item 42)

Pedido: fechar no F e no W, iguais, o item 42 que a revisão da rodada abaixo
deixou aberto. Entrevista, SPEC §7.4 primeiro, código, testes com `-race`
contra os serviços reais, mutação e uma revisão independente somente-leitura.

- **Bug:** um boot com `ring=memory` ou `ring=none` nunca abria o `ring.db`.
  As linhas que uma vida `ring=sqlite` deixou — e que o `WARN` do shutdown
  prometia "for the next boot" — ficavam paradas sem log. E com
  `ring=sqlite` + `sink=none`, o dreno do boot entregava ao descarte as linhas
  de uma vida com destino.
- **Decisões do usuário:**
  - Um boot com outro ring entrega o `ring.db` no passo 5, como o backlog
    próprio (3 numa recusa, 130 num `SIGTERM`), e **remove** o arquivo: `-wal`
    e `-shm` primeiro, o banco por último. Ele preferiu isso a "entregar e
    deixar o arquivo", que era o recomendado.
  - Com `sink=none` as linhas de uma vida anterior **ficam**, em qualquer
    ring, como o dump (§9.2). No `ring=sqlite` o cursor começa depois da maior
    `rowid_` vista no boot, e elas ficam fora do `Stats` (`KeepBacklog`,
    pedido pela `App`).
- **Correção:**
  - `ring.ProbeSQLite` sonda o arquivo sem criar arquivo nem tabela: o
    `NewSQLite` criaria os dois.
  - Arquivo ilegível ou que não se abre: `ERROR`, e o boot segue.
  - Tabela v1 com linhas: o arquivo fica.
  - O `WARN` do shutdown soma as linhas guardadas.

**Revisão independente**, corrigido:
- **ALTO:** a remoção apagava o `events.db`. Isso acontecia quando
  `ring.sqlite.path` e `sink.sqlite.path` coincidiam sob `ring=memory` depois
  de uma vida `ring=sqlite` ter posto uma `jlh_ring` nele: o destino inteiro
  sumia de baixo do sink aberto, com saída 0. Agora só se remove um arquivo
  sem outras tabelas além das do ring e das `sqlite_*`. Um arquivo dividido é
  drenado e fica, com `WARN`.
- **Item 40, um dos marcadores:** o *writer* do `ring=sqlite` saía sem aplicar
  os `DELETE` enfileirados. Com uma CPU, metade das vezes o último lote voltava
  no boot seguinte. Isso valia para todo shutdown `ring=sqlite` e era
  duplicata real com Redis. Agora a saída do *writer* esvazia o canal.
- **Testes fracos:**
  - Os `rowid_` contíguos deixavam passar um cursor pela contagem.
  - Um teto apertado demais dava `429` intermitente.
  - Um ramo de erro não tinha teste.
- **Texto da SPEC:** o `ring.db` vazio é removido também com `sink=none`, e o
  segundo sinal também deixa linhas.

**Resultado:** suítes `-race` verdes no F e no W contra Redis, PostgreSQL 18,
MySQL 8.4, NATS 2.14.6 (com e sem JetStream) e ClickHouse 25.8, sem teste
pulado. Mutação: 22 de 22 mortos em cada módulo. Um mutante sobreviveu na
primeira passada e desmentiu um comentário: o SQLite não bloqueia num FIFO.

**Aberto:**
- TESTING §8 item 43: as linhas guardadas pelo `sink=none` não têm gauge.
- Item 44: nada impede duas instâncias no mesmo `ring.data_dir`.
- O resto do item 40 e o item 38.

**Lições.**
- "É um banco do ring" e "é só do ring" são perguntas diferentes, e apagar
  exige a segunda.
- Um caminho novo que fecha um recurso logo depois de usá-lo expõe toda
  corrida de fechamento antiga. Rodar com `-cpu 1` é o que a mostra.
- Um teste de cursor precisa de chaves que não comecem em 1.

## 2026-10-11 — F, W: quatro achados da revisão do porte, anteriores a ele

Pedido: corrigir no F e no W, iguais, quatro dos defeitos que a revisão
independente do porte registrou como anteriores a ele (TESTING §8 item 40 de
cada um). SPEC primeiro, código, testes com `-race` contra os serviços reais
(Redis, PostgreSQL 18, MySQL 8.4, NATS 2.14.6 com e sem JetStream, ClickHouse
25.8), mutação em cada correção e uma revisão independente somente-leitura.

- **`SIGHUP` durante o dump matava o processo.** O observador só existia
  enquanto servia e saía com `signal.Stop`, que devolve o sinal à ação padrão
  do Go: o ring inteiro, já respondido com `200`, perdido sem contagem; no boot,
  a instância morria antes de servir. Agora o `main` o ignora antes de tudo, o
  `run` o captura do início ao fim (recarrega só servindo; fora disso, `WARN`)
  e sai com `signal.Ignore` — a forma do P e do N (SPEC §13.3).
- **Dump parado sem log quando a vida seguinte troca o ring.** O leitor só
  existia junto do escritor (`ring=memory` com a emergência ligada); com
  `ring=sqlite`, `ring=none` ou `emergency.enabled=false` o dump ficava no
  disco para sempre. O `Store` agora existe em todo modo e `App.dumps` decide só
  a escrita (SPEC §9).
- **`whs_emergency_*`**: a reingestão os zerava mesmo com um arquivo ilegível
  deixado no lugar, e o dump os punha em 1 e no tamanho dele. Agora o número de
  dumps listados no início da reingestão, a recontagem do disco no fim (a forma
  do P, contando também o arquivo ilegível, que o `Count` do P ainda pula) e o
  dump soma (SPEC §11.1).
- **`ring=sqlite`**: o `WARN` do shutdown somava o lote retido pelo worker às
  linhas que o ring ainda contava; ele já estava entre elas (SPEC §10.3).

**Revisão independente** (um revisor somente-leitura, com experimentos numa
cópia), corrigido:
- **O teste do `SIGHUP` passava vazio na suíte inteira.** A saída do `run`
  deixa o sinal ignorado, um sinal ignorado continua ignorado depois do `exec`,
  e o `clickhouse_test.go` monta uma `App` antes: com o `signal.Ignore` do
  `Main` removido, o teste passava. Agora o pai captura o sinal ao criar os
  filhos (capturado, ele volta à ação padrão no `exec`), cada filho confere
  `signal.Ignored`, e o passo do `Main` confere que a identidade foi pedida
  (antes, um `exit 1` da configuração passava sem sinal algum).
- **Ler em todo modo estendeu um caminho de perda**: com `sink=none` o dump era
  "entregue" ao descarte e removido (o `ring=memory` já fazia isso; agora também
  o `sqlite` e o `none`). Decisão, registrada na SPEC §9.2: com `sink=none` nada
  é reingerido; o dump fica, contado nos gauges, com um `WARN`.
- Um `emergency.dir` que não se lista passou a parar com 1 modos que nunca o
  olhavam: agora `ERROR` e o boot segue, salvo para quem escreve dumps.
- A SPEC §9.3 punha a perda com `emergency.enabled=false` em
  `emergency_failed`; o código conta `abort`. Texto corrigido, e a descrição da
  chave cita também o `drain_timeout`.
- Recontagem dos gauges que falha agora tem `WARN`; o gauge de arquivos sobe
  antes do estado de boot que anuncia a reingestão.

**Resultado:** suítes `-race` verdes no F e no W contra os serviços reais;
mutação 16 de 16 mortos em cada módulo, numa cópia, os do `SIGHUP` rodando
depois de um teste que montou uma `App` (a ordem da suíte inteira).

**Seguem abertos** (TESTING §8): no item 40, o segundo sinal no meio do dump, o
boot do Redis com até 2× `boot_timeout`, o `link` de NFS e a colisão de
nanossegundo, o `emergency.dir` dividido e o canal de `deletes` do
`ring=sqlite`; o item 38 (reingestão e dreno do `ring.db` saem com 3 numa
recusa `logic`); e o item 42, novo, da mesma classe do dump parado: um backlog
do `ring.db` fica parado se a vida seguinte sobe com `ring=memory` ou
`ring=none`, e com `ring=sqlite` + `sink=none` é drenado para o descarte —
escolha de desenho, não feita aqui.

**Lições.**
- Um leitor de estado deixado por uma vida anterior não pode depender da
  configuração desta: o dump é de quem o escreveu, não de quem sobe agora.
- "Parar de observar um sinal" não é "ignorá-lo": `signal.Stop` devolve o
  sinal à ação padrão, e para `SIGHUP` ela mata o processo.
- Um gauge que diz "o que está pendente" se lê de onde o pendente está (o
  disco), ou se soma; nunca se põe num número que o código acha que sabe.
- Um teste de sinal num processo filho herda o que o pai deixou: um sinal
  ignorado sobrevive ao `exec`. O filho confere a própria premissa, e a mutação
  roda na ordem da suíte inteira, não sozinha.
- Ampliar onde um caminho roda amplia também os seus efeitos colaterais: ler o
  dump em todo modo levou junto o "entregar ao descarte e apagar" do
  `sink=none`.

## 2026-10-11 — P, F, W, SN, SP: porte da revisão do porte de volta ao N

Pedido: portar aos irmãos a lista "A portar" da entrada N logo abaixo, com o N
como referência e sem editá-lo. SPEC primeiro, código, testes com `-race`
contra os serviços reais de cada módulo e mutação de cada correção numa cópia.

- **Flags** (P, F, W, SN): o `flags.go` do N (`knownFlag`, `prev`, `after`).
  `--x.password --v` não nomeia mais "a flag `--v`", `--x.password= -v` não sai
  como parâmetro desconhecido, e a cauda que o shell partiu num espaço não sai
  como argumento solto; um erro de digitação depois de um valor real continua
  nomeado. Fica a brecha decidida no N (cauda com traço depois de um valor não
  vazio), no `TESTING.md` §8 de cada um. SPEC §13.2 e a tabela de validação
  (linhas novas no P, F e W; a do SN estendida).
- **SP**: o parser de flags inteiro do N. **Estendido** (mesma forma): o
  `ini.go` do SP ainda citava a linha (`password: x`) e o cabeçalho, no boot e a
  cada `SIGHUP`; agora é o do SN. Duas linhas novas na SPEC §13.2.
- **`Seal`** (P, F, W): o gancho `afterDrain` saiu; `App.wrapRing` (no F e no
  W, só no ring de memória) e o `drainHookRing` do teste completam o upload
  **dentro** do último `Drain`, e o mutante "`Seal` depois do `Drain`" agora
  morre. `TestMemorySealIsABarrierForEnqueue` com 3000 rodadas; o comentário do
  `Leftovers` corrigido. O teste novo do `SIGHUP` durante o dump do F (da sessão
  que corrige o `SIGHUP`, em curso) usava o `afterDrain`: passou ao
  `drainHookRing`, no mesmo instante, e continua verde.
- Caso novo em todos: `--config= x`. Tirar o `assigned(v)` do `--config`
  sobrevivia à suíte — também no N (conferido numa cópia).

**Resultado:** suítes com `-race` verdes nos cinco contra os serviços reais (P e
SP: PostgreSQL 18; SN: NATS 2.14.6 com e sem JetStream; F e W: Redis,
PostgreSQL, MySQL, NATS e ClickHouse, nenhum teste pulado); `gofmt` e `vet`
limpos. Mutação 74 de 74 mortos (P, F e W 16 cada; SN 12; SP 14), cada um com
o controle verde antes; o `Enqueue` sem trava morre em 20 de 20 execuções sem
`-race` no P, no F e no W.

**A portar:** N: o caso `{"--config=", "hunter2"}` no
`TestConfigReviewFindings`.

**Lição.** A mutação de um porte também mede o original: o caso que faltava no
irmão faltava no N, de onde o código veio.

## 2026-10-11 — N: porte de volta das correções da revisão do porte

Pedido: trazer ao N as correções que a revisão do porte (entrada abaixo) fez
nos irmãos e que o N também precisava, com P, F e SN como referência. SPEC
primeiro, código, testes com `-race` contra NATS 2.14.6 com e sem JetStream,
mutação em cada correção e uma revisão independente somente-leitura.

- **Ring selado antes do último `Drain`** (`Seal`, `RWMutex` contra o
  `Enqueue`): o upload que terminava depois do `Drain` recebia `200` e se
  perdia.
- **Sem a sonda `AccountInfo`**: "sem respondedores" na primeira checagem é
  FATAL nomeando o JetStream; depois de validado, `network`. O servidor NATS
  falso do sink agora responde `503` a um pedido e nega um subject, e prova que
  nada pergunta `$JS.API.INFO` nem guarda veredito.
- **Subject de outro `space` retirado do stream é esquecido**, não FATAL. A
  tarefa admitia mantê-lo com justificativa no `DECISOES.md`; não houve: só a
  reingestão publica ali, e estreitar o stream é o que o próprio erro do
  `cover` manda fazer depois. O teste de ponta a ponta estreita o stream com a
  instância rodando.
- **Flags**: uma flag sem valor não engole a seguinte, e o valor depois de um
  espaço não é citado.
- **Dump interrompido**: o `WARN` e a SPEC §9.1 dizem quem contou a perda e
  como reingerir o temporário; o `docker/README.md` traz o comando.
- Comentários e `DECISOES.md` que diziam "o P não tem" corrigidos.
- Estendido pela mesma forma: o `WARN` "drain deadline reached" com o
  `drain_timeout` configurado (o N, sozinho, logava o resto do prazo da fase 4);
  os testes do `WARN` do dump interrompido e dos *gauges* de emergência, que o
  P ganhou na revisão.

**Revisão independente** (um revisor somente-leitura, numa cópia do módulo),
cinco achados, todos corrigidos:
- **O teste do `Seal` não provava a ordem**: o gancho rodava depois do
  `Drain`, e um `Seal` movido para entre os dois sobrevivia. Agora o ring do
  teste completa o upload *dentro* do `Drain`, e esse mutante morre.
- **Três caminhos do parser de flags ainda imprimiam o segredo**: na forma
  separada, um valor que começa com `--` era "nomeado" como flag (regressão
  trazida pelo próprio porte); `--x= -valor` saía como parâmetro desconhecido;
  e a cauda de um valor que o shell partiu num espaço saía como argumento
  solto. Agora só se nomeia uma flag que o servidor conhece, e nada depois de
  um valor é citado. Fica uma brecha, decidida e registrada: a cauda com traço
  depois de um valor não vazio, porque escondê-la esconderia os erros de
  digitação.
- Texto: a SPEC §8.7.7 ainda dizia "valida uma vez"; o comentário do
  `Leftovers` ainda dizia "contados como perda"; uma linha do `TESTING.md`
  dava a soma dos *gauges* como sem teste.

**Resultado:** suíte `-race` verde com e sem JetStream, os testes novos verdes
em `-count=10`, cobertura 91,7 %; mutação 20 de 20 mortos (cada reversão numa
cópia do módulo, com o controle verde antes).

**A portar** (conferido por leitura; nada foi editado fora do N) —
~~feito em 2026-10-11, entrada acima~~:
- P, F, W, SN: os três caminhos de flag acima (o `flags.go` é o mesmo do N
  antes desta revisão).
- SP: o parser de flags inteiro do N — nem a flag que engole a seguinte nem o
  valor depois de um espaço foram portados para lá.
- P, F, W: o teste do `Seal` com o mesmo ponto cego (`afterDrain` depois do
  `Drain`); o `TestMemorySealIsABarrierForEnqueue` com 300 rodadas, que sem
  `-race` pega o `Enqueue` sem trava só metade das vezes (aqui, 3000); e o
  comentário do `Leftovers` que diz "contados como perda".

**Lições.**
- Um erro que diz ao operador "cubra este subject até o dump drenar" promete
  que descobri-lo depois é seguro; uma checagem que trata o descoberto como
  fatal quebra a promessa do próprio erro. Ao escrever uma recusa, conferir o
  que acontece quando o operador faz exatamente o que ela manda.
- Um gancho de teste posto *depois* de um passo não prova a ordem dos passos
  antes dele. Para provar "A antes de B", o teste age dentro de B.
- "Nunca citar o valor" se prova pelos caminhos em que o valor chega com outra
  forma: começando com traço, depois de um espaço, partido pelo shell.

## 2026-10-11 — P, F, W, SN, SP: porte das correções achadas no N

Pedido: portar aos irmãos a lista "A portar" da entrada N (abaixo), com o N como
referência e sem editá-lo. SPEC primeiro, código, testes com `-race` contra os
serviços reais (PostgreSQL 18; NATS 2.14.6 com e sem JetStream, `max_payload`
8 MB; Redis, MySQL 8.4 e ClickHouse 25.8 no F e no W), mutação em cada correção
e uma revisão independente somente-leitura por área (P; SN e SP; F/W sink NATS e
config; F/W ciclo de vida e emergência).

**Portado como listado.**
- P, F, W: log próprio da recusa fatal no boot; `ring.Commit` antes do
  `Release` na recusa `logic`; um `drain_timeout` para as fases 3 e 4; só
  arquivos regulares na listagem do dump.
- P, F: dump sob `.emg-…tmp` e `link`, `rename` sem *hard link*, `fsync` do
  diretório como melhor esforço, gêmeo removido e dump interrompido informado no
  boot. P: gauges de emergência somados no dump.
- SN: token fora do log de conexão; URL inválida sem eco; veredito de permissão
  no `Write` em `core`; contrato do stream em toda checagem; `max_bytes` com
  `discard: new`; recusas de autenticação no `CONNECT` fatais; `rename` sem
  *hard link*; INI e flags sem eco. O compose do SN não tem o defeito do
  `LOGHUB_EB_URL` (as chaves donas são `KEY:`, sem valor).
- F (`sink/nats.go`, config): uma mensagem recusada sem encerrar o lote
  (`Error.Delivered`, e entrega parcial nos workers e no `ring=none`); veredito
  de permissão; `no_ack`, JetStream desligado, plano de controle, 32 MiB e
  `discard: new`; contrato em toda checagem; URL sem eco; *userinfo* junto de
  `username` recusado. O `TestNATSRefusesAnEnvelopeItDidNotWrite`, que pulava
  com `BUG`, agora passa.

**Estendido** (mesma forma em arquivo igual ao do listado — a lição de
2026-10-07): o W recebeu tudo o que o F recebeu (o código é o do F, arquivo por
arquivo); o SP, o `rename` e o informe do dump interrompido (a emergência dele é
a do SN); P, F e W, os parsers sem eco. No F/W, a URL do NATS ia inteira, senha
incluída, ao "sink ready", ao dump de config e ao log de conexão
(`ConnectedUrl()`); agora sai mascarada ou só esquema e host.

**Achados da revisão** (corrigidos com SPEC, teste e mutação):
- **Um upload que terminava depois do prazo da fase 3 recebia `200` e se
  perdia** (P, F, W; o N tem o mesmo): o handler bloqueado no corpo seguia
  depois do `srv.Shutdown`, e o prazo único das fases 3 e 4 tirou a folga que
  antes o pegava. O ring de memória agora é selado antes do último `Drain`
  (`Seal`, barreira por `RWMutex` contra o `Enqueue`).
- **A sonda `AccountInfo` do JetStream desligado** (F, W, SN; o N tem): não
  distinguia nada (o `jetstream` traduz "sem respondedores" para "não
  habilitado" sozinho) e, negada a um papel mínimo, virava veredito de
  permissão permanente. "Sem respondedores" na primeira checagem agora é FATAL
  sem sonda.
- F/W: um subject de outro `space` retirado do stream depois do backlog drenar
  era FATAL para sempre (veio do N); agora é esquecido. O Redis tinha uma frase
  própria para a recusa fatal no boot; o teto de headers não contava o nome do
  stream.
- SN: flag sem valor engolia a seguinte (`--sink.workers
  --sink.nats.password=x`) e o valor depois de um espaço virava argumento
  citado — os dois imprimiam o segredo (o N também). P/SN/SP: gauges e o `WARN`
  do dump interrompido sem teste; e a frase herdada do N "os eventos foram
  contados como perda" só vale para o aborto por segundo sinal — depois de um
  `SIGKILL` ninguém os contou, e a SPEC agora diz como reingerir o temporário.

**Resultado:** suítes com `-race` verdes nos cinco módulos contra os serviços
reais; mutação: P 23 de 23, F e W 45 de 47 cada (dois equivalentes
documentados), SN e SP todas mortas salvo um equivalente (`ErrAuthRevoked` já
casa com o texto). O prazo único das fases 3 e 4 se prova com NATS, não com
Redis: num *pipe* travado o `rueidis` só devolve o `Write` cancelado no próprio
`ConnWriteTimeout` (10 s).

**Não corrigido** (anterior ao porte, no `TESTING.md` §8 de cada módulo):
`SIGHUP` durante o dump mata o F/W (o watcher para com os workers e devolve o
sinal à ação padrão); dump parado sem log quando a vida seguinte troca o
`ring.mode` ou desliga a emergência (F/W) — ~~estes dois corrigidos em
2026-10-11, entrada "F, W: quatro achados" acima~~; segundo sinal no meio do dump conta
perda a mais ou a menos (P, F, W, N); reingestão e dreno do `ring.db` do F/W
encerram o boot numa recusa `logic`; SN/N em `core`: *flush* interrompido por
reconexão vira FATAL, e um `-ERR` do beacon pode encobrir a negação de um
pedaço. O N continuava com as correções desta rodada a portar de volta (sonda,
subject esquecido, `Seal`, flags) — ~~feito em 2026-10-11, entrada acima~~ —,
e a entrada N abaixo atribui ao SN a recusa de caminho em `sink.nats.urls`, que
o SN só tinha para a `LOGHUB_EB_URL` (agora tem).

**Lições.**
- Uma lista de porte nomeia módulos; a busca é por forma. O W tinha cada linha
  da lista do F, e o SP a do SN, porque são cópias sincronizadas.
- Um prazo encurtado precisa de um teste do que chega depois dele: o prazo único
  fechou a folga que escondia o `200` perdido.
- Uma frase sobre uma perda precisa dizer quem a contou: "contados como perda"
  era verdade num caminho e mentira nos que mais acontecem.
- Copiar um arquivo inteiro entre irmãos exige conferir antes que eles eram
  iguais: o `redis_test.go` do W tinha três streams `webhook.request` e um
  modo `bucket`, e só a suíte do Redis avisou.
- Um teste de prazo precisa de um destino que respeite o cancelamento; senão
  mede o cliente, não a correção.

## 2026-10-10 — N: `go-webhook-nats-server`, o P com o destino trocado pelo NATS

Pedido: criar o `go-webhook-nats-server` a partir do P, trocando o PostgreSQL
pelo NATS "como foi feito no SN"; decidir sozinho o que os projetos e o código
respondem, e entrevistar só sobre o que não puder ser descoberto. **Não houve
entrevista**: cada ponto tinha uma decisão do usuário (F, decisões 8 e 21: NATS
`core` e beacon com `sink=nats`) ou um precedente do SN e do P, e tudo ficou
registrado como contestável em `docs/DECISOES.md` do módulo. SPEC antes do
código (numeração do fast, seções novas §8.7.10 beacon e §8.7.11
`LOGHUB_EB_URL`, invariantes 17–22), suíte com `-race` contra NATS 2.14 real
(com e sem JetStream), teste de fumaça do compose, mutação nos comportamentos
novos e revisão independente por área.

**Decisões** (todas derivadas, nenhuma perguntada).
- Do SN: sem TLS até o NATS (o `TLSCertCB` que recusa impede a promoção
  automática do `nats.go`), `jetstream` e `core`, subject derivado
  `loghub.<space>.webhook`, beacon em `loghub._ctl.beacon` por core,
  `LOGHUB_EB_URL` só `nats://`, 8 workers numa conexão.
- Do P: drop-in do fast com cinco seletores (`ring.mode=memory`,
  `sink.type=nats`, `ratelimit.enabled=false`, `sink.nats.tls=false`,
  `sink.nats.subject=loghub.{space}.{type}`); `SIGHUP` ignorado; dump JSON Lines
  no mesmo formato (um dump do P é reingerido aqui).
- Próprias: entrega parcial (`sink.Error.Delivered`) nos workers e na
  reingestão; o sink contamina o lote em toda falha `jetstream` depois da
  primeira publicação; a porta do dump exige `type=webhook`, `level=9`,
  `step=0` e o payload aberto por `{"mode":"fast",` (o NATS publica sem ler);
  dump sob nome temporário e `link`; o `cover` do fast para subject de outro
  `space`.

**Resultado:** 3 dependências de produção (`nats.go`, `uuidv7`, `ident`);
binário de 11,9 MB contra 16,5 MB do P (mesmo build);
compose exercitado de ponta a ponta (entrega, beacon, `SIGHUP`, `docker pause`
do NATS ⇒ FATAL ⇒ dump de 15 ⇒ reingestão, com as 8 publicações que o NATS
congelado gravou ao voltar absorvidas como duplicatas).

**Achado na construção:** numa recusa fatal no boot (stream errado, credencial,
TLS), o log dizia "sink did not become reachable within sink.boot_timeout" —
mandava o operador olhar a rede. Agora "the sink refused this server
permanently". Herdado do W, do F e do P.

**Revisão independente** (três revisores somente-leitura: sink e workers;
emergência e ciclo de vida; config, docs e Docker): 22 achados reais, todos
corrigidos com SPEC, teste e mutação (cada correção revertida derruba um teste;
sobrou só um mutante equivalente). Os que mais pesavam:
- **Sink.** Em `core`, a recusa `logic` liberava o buffer antes do
  `ring.Commit`, e o ring contava os bytes para sempre até recusar tudo como
  cheio; em `core`, o lote com publicação negada era confirmado e perdido; o
  contrato do stream só era conferido no boot, e um stream editado por baixo
  ficava `network` para sempre com o sink UP. Agora vale em toda checagem.
- **Emergência.** A porta do dump aceitava JSON que o escritor nunca escreve
  (chave repetida, `1e999999`, 9000 níveis, `\u0041`, `uuid` de outro
  instante); agora confere a forma do escritor, e um fuzz novo prova que tudo o
  que o escritor escreve passa. Sem *hard link* (vfat, FUSE), todo dump falhava:
  agora renomeia. O `drain_timeout` virou um prazo para as fases 3 e 4 juntas
  (um por fase dobrava o pior caso e passava do `stop_grace_period`).
- **Config e Docker.** O token da URL saía no log de conexão (o
  `ConnectedUrlRedacted` do `nats.go` esconde só senha); o erro de URL
  inválida citava a senha até a primeira `/`; o procedimento do
  `LOGHUB_EB_URL` no compose não funcionava (variáveis vazias contavam como
  segunda fonte, e o compose nem repassava a URL). E quatro correções do SN que
  não tinham vindo: `NaN`, `admin.bind` vazio, `quoteINI`, URL com caminho.

Resultado final: suíte com `-race` verde contra NATS real, cobertura 91,6 %,
fuzz de 60 s por alvo limpo.

**A portar** (nada foi editado fora do N; conferido por leitura em cada um) —
~~feito em 2026-10-11, entrada acima~~:
- W, F, P: o log da recusa fatal no boot (acima); `ring.Commit` antes do
  `Release` na recusa `logic` dos workers; um `drain_timeout` só para as fases
  3 e 4; só arquivos regulares na listagem do dump.
- P, F: o dump sob nome temporário e `link`, já com o fallback de rename sem
  *hard link*. P: as métricas de emergência sem reler o diretório no dump.
- SN: o token no log de conexão; o erro de URL que cita a senha; o veredito de
  permissão lido no `Write` em `core`; o contrato do stream em toda checagem;
  `max_bytes` com `discard: new`; as recusas de autenticação no `CONNECT`; o
  fallback de rename sem *hard link*; parsers de INI e flags sem eco.
- F (`sink/nats.go`), as correções do SN que o sink NATS do fast não tem: uma
  mensagem recusada no cliente (`ErrMaxPayload`) ou um envelope que não mapeia
  encerram o lote (o fast publica as anteriores e conta o lote inteiro como
  perdido — o próprio `TestNATSRefusesAnEnvelopeItDidNotWrite` dele pula com
  esse BUG); permissão negada nunca vira FATAL; `no_ack` e JetStream desligado
  esperam o `boot_timeout`; senha com `%`/`/`/`#` sem escape vai para o log;
  *userinfo* na URL junto de `username` é ignorado em silêncio; o boot não
  recusa um stream que captura o plano de controle (`loghub.>`) nem confere o
  teto de 32 MiB do *file store*; o contrato do stream só no boot. E os workers
  do F não têm entrega parcial.

**Lições.**
- "Aprender com os projetos" inclui as decisões já tomadas nos irmãos: o
  `DECISOES.md` do fast decidia `core` e beacon para a família webhook, e o SN
  os implementava; nada disso precisava voltar ao usuário.
- Um gancho que existe para um evento único (o beacon da primeira conexão)
  precisa de um teste em que nenhum outro caminho produza o mesmo efeito: com o
  periódico ligado, a mutação que tirava o gancho sobrevivia.
- Uma porta de reingestão vale o que o destino deixa de recusar: trocar uma
  fila com CHECKs por um bus que não lê o payload exige estreitar o leitor — e
  uma porta estreita precisa do teste do outro lado (tudo o que o escritor
  escreve passa), ou passa a quarentenar eventos reais.
- "Portar do irmão" não é só copiar o código que se lê: quatro correções da
  revisão do SN não vieram porque viviam em arquivos que o N herdou do P. Na
  próxima criação a partir de dois pais, listar as correções da revisão de cada
  pai e conferir uma a uma.

## 2026-10-07 — F, W: porte das correções achadas no P

Pedido: portar ao F e ao W a lista "A portar" da entrada do P (abaixo). SPEC
primeiro, código, teste com `-race` contra Redis, PostgreSQL, MySQL, NATS e
ClickHouse reais, e mutação em cada correção (o teste falha sem ela).

- **Validação de boot no prazo da checagem** (§8.5.5): o `validationContext` e
  o `withLifetime` do P foram para `sink/health.go` e valem para três sinks,
  não só um: o PostgreSQL (`ValidateTimeout` = `sink.op_timeout`) e o **MySQL**
  validavam no `health_timeout` de 1 s; o **ClickHouse** já tinha prazo
  próprio, mas por `WithoutCancel` puro, e um `SIGTERM` no boot esperava o
  `op_timeout` inteiro. O NATS valida numa ida e volta e ficou como estava. O
  texto do ClickHouse (`max_execution_time`, `452`, *materialized views*) saiu
  de §8.5.5 para §8.8.5. Teste novo de ponta a ponta: um proxy lento (30 ms por
  pedaço) entre cada sink e o servidor real.
- **Primeiro sinal num shutdown por FATAL** não aborta mais: só o segundo, e
  o `SIGQUIT` sempre (§10.3, frase corrigida). O aborto conta cada perda uma
  vez (`atRisk`/`collected`; `ring=sqlite` continua sem contar nada) e para o
  caminho gracioso, que antes acordava no fim do `drain_timeout` e despejava
  o que o aborto já tinha dado por perdido. O teste do segundo sinal passou a
  conferir `dropped{abort}`.
- **`SIGTERM` na reingestão sai com 130** (era 3), e o mesmo na drenagem do
  backlog do `ring.db` (só F/W têm), com o arquivo ou as linhas mantidos
  (§9.2).
- `--help` e `--dump-config` do F dizem `go-webhook-fast-server`;
  `envelope.appendKey`, sem uso, saiu do P, do F e do W.

**Fica de fora** (anotado em `TESTING.md` §8 item 36): o aborto no meio do
dump não tem teste determinístico, porque a janela é de milissegundos.

**Lição.** Um defeito achado num módulo derivado vale uma busca pela mesma
forma nos irmãos: a lista do P falava de um sink, e o mesmo erro estava em três.

## 2026-10-07 — P: `go-webhook-pg-server`, o fast reduzido a memória → PostgreSQL

Pedido: um servidor de webhooks rápido e com dependências mínimas, a partir do
F, com ring só em memória, sink só no PostgreSQL (`loghub.inbox`) e emergência
em JSON Lines. Entrevista de 4 decisões, SPEC antes do código (numeração do F
mantida, seções retiradas marcadas), suíte com `-race` contra PostgreSQL 18
real, teste de fumaça do compose (entrega, `SIGHUP`, FATAL, dump, reingestão),
mutação nos comportamentos novos e revisão independente por área.

**Decisões** (as do usuário contra a recomendação, marcadas).
- Saem os outros rings e sinks **e também** o rate-limit e a recarga por
  `SIGHUP` (**contra** a recomendação de manter os dois). O `SIGHUP` é capturado
  e ignorado com `WARN`: a ação padrão do Go mataria o processo sem dreno.
- Dump `emg-<agent>-<unixnano>.jsonl`, um JLH por linha; uma linha danificada é
  pulada e as outras entregues; o `.bin` do F não é lido (só um `WARN`).
- *Drop-in* do F: mesmas chaves, `WHS_*` e `whs_*`; `ring.mode`, `sink.type` e
  `ratelimit.enabled` aceitos só com `memory`/`postgres`/`false`; outra chave de
  recurso retirado num INI ou flag é desconhecida (`exit 1`); no ambiente, inerte.
- Interfaces `Ring`/`Sink` mantidas com uma implementação cada: o porte de
  correções com o F continua por `diff -ruN`.

**Resultado:** 3 dependências de produção (eram 8), binário de 11,2 MB (o F tem
~21 MB).

**Revisão independente** (três revisores só leitura: emergência; configuração e
documentação; entrega e ciclo de vida). Tudo corrigido com teste que falhava
antes, conferido por mutação:
- **Uma linha do dump que o `jsonb` recusa travava todo boot** (a pior): o
  `json.Valid` não confere UTF-8 nem surrogates, e um `0xFF` ou um `\ud800` numa
  linha passava pelo leitor, era recusado com classe 22, e a reingestão saía
  com 3 a cada boot, sem entregar mais nada. O leitor exige UTF-8 e só os
  escapes do serializador; um lote recusado como `logic` na reingestão é refeito
  item a item, e o recusado fica no arquivo em quarentena.
- **No shutdown por FATAL o primeiro `SIGTERM` jogava o dump fora** (herdado do
  F): era tratado como o segundo sinal. Só o segundo aborta agora.
- O cancelamento do boot não chegava à validação depois do prazo da checagem
  (`AfterFunc` dispara uma vez): a checagem leva o contexto de vida.
- Contagem de perdas única entre aborto e dump; `SIGTERM` na reingestão sai com
  130; gauges de emergência recontados do disco; `SIGHUP` ignorado desde o
  início do `main`.
- `FuzzRead` passou a rodar em memória: de ~3 para ~3 mil execuções por segundo.

**A portar para o F (e o W, onde couber)** — ~~feito em 2026-10-07, entrada
acima~~: a validação de boot do PostgreSQL
roda no `sink.health_timeout` (1 s), embora a SPEC diga `sink.op_timeout`, e o
parágrafo de §8.5.5 traz texto do ClickHouse (`max_execution_time`, `452`,
*materialized views*); o primeiro sinal num shutdown por FATAL; a frase "SIGQUIT
sempre aborta imediatamente" (§10.3), que o código não cumpre; o `--help` e o
cabeçalho do `--dump-config` se apresentam como `go-webhook-server`;
`envelope.appendKey` não é usado.

**Lições.**
- `json.Valid` não é "o banco aceita": quem escreve o formato sabe o que ele
  contém, e o leitor deve exigir exatamente isso.
- Formato de linha convida a edição à mão: o leitor precisa aguentar o que uma
  edição produz (`\r\n`, bytes soltos) sem travar o boot.
- `context.AfterFunc` dispara uma vez: para seguir o avô depois que o pai
  venceu, é preciso levá-lo junto.
- `docker kill -s <qualquer sinal>` marca o contêiner como parado à mão e
  desliga o `restart: unless-stopped` (observado no teste de fumaça).

## 2026-10-07 — SP, SN: `go-syslog-pg-server` e `go-syslog-nats-server`, cortes do S

Pedido: dois servidores syslog rápidos e com dependências mínimas, cortados do S.
**SP**: sink só PostgreSQL (`loghub.inbox`); **SN**: "no mesmo molde", sink só NATS.
Nos dois: ring só em memória, emergência em JSON Lines, sem TLS. Entrevista
(recomendação primeiro), código, porte das suítes com `-race` contra PG 18 e
NATS reais, docs com a numeração da SPEC do S, revisão independente por área.

**Decisões** (as contra a recomendação, marcadas).
- SP: ficam admin HTTP, `LOGHUB_EB_URL` (só `postgres://`), unix datagram,
  SIGHUP (só `log.*`), PROXY no TCP e charset legado; saem filtro, rate-limit,
  beacon, os outros sinks e rings, e as chaves `sink.type` e `ring.mode`.
- **TLS fora de tudo**, inclusive no cliente PostgreSQL (**contra** "só o
  listener"): `sslmode=disable` sempre; `sslmode=require|verify-*` na URL é
  exit 1. SN: cliente NATS sempre em claro, `tls://`/`ws://`/`wss://` exit 1.
- Emergência: **um JLH pronto por linha**, reingerido sem conversão.
- SN: beacon fica; JetStream e core; só o subject exato (sai facility-severity).
- Variável de ambiente do S que o corte não tem → um WARN no boot (na flag e
  no INI já era exit 1).
- Resultado: dependências diretas de produção 8 → 3 (SP: pgx; SN: nats.go; mais
  ident e uuidv7); binário 31,8 → 17,2 MB (SP) e 12,5 MB (SN); a recepção
  ~17 % mais rápida que a do S, depois 1 alocação por mensagem e 0 com o ring
  cheio.

**Achados da revisão independente** (cada um com teste que falhava antes):
- **O `nats.go` faz TLS sozinho** quando o servidor anuncia `tls_required`:
  apesar de nenhum `nats.Secure`, saía um ClientHello. Um `TLSCertCB` que
  recusa corta antes do handshake, e o erro é FATAL nomeando o TLS. Lição:
  "sem TLS" se prova com um servidor que pede TLS, não lendo as opções.
- **O ambiente libpq vazava** apesar de cada campo ser sobrescrito:
  `PGSSLMODE=bogus` dava exit 1 por TLS num servidor sem TLS, e
  `PGCHANNELBINDING=require` recusava toda conexão sem SQLSTATE até o
  `boot_timeout`. O parse agora roda com todo `PG*` escondido. Lição:
  sobrescrever depois do parse não protege do que falha DURANTE o parse.
- **Reingestão carregava o dump inteiro**: o JLH chega a 12× o cru, e um ring
  cheio virava OOM no boot, em laço. Agora em fluxo, pedaço a pedaço.
- **A porta `ParseLine` deixava passar o que a fila recusa** — e uma recusa
  derruba o pedaço inteiro (`logic`): número que o `jsonb` não guarda
  (`1e1000000`, 22003), surrogate solto (22P05), `payload.message` ausente
  (CHECK), chave duplicada no cabeçalho. Agora: cabeçalho byte a byte igual ao
  do escritor, escapes só de controle, números inteiros de até 20 dígitos,
  `message` exato. Lição: uma linha reingerida sem conversão precisa passar por
  tudo o que o destino recusaria, porque a recusa é do lote, não da linha.
- **Dump escrito no nome final**: cortado numa fronteira de linha, lia limpo e
  era removido depois de entregue pela metade. Agora nome temporário, `fsync`,
  link para o nome final, `fsync` do diretório.
- **Aborto durante o dump contava 0** (o `Drain` já tinha baixado a
  contabilidade), e o primeiro SIGTERM de um shutdown FATAL abortava o dump. O
  primeiro sinal do FATAL agora é tolerado; o aborto conta os itens do dump.
- SN: **uma mensagem acima do `max_payload` derrubava o pedaço**: o cliente a
  recusa antes de sair (`ErrMaxPayload`), e o laço parava ali — as seguintes
  nunca publicadas e confirmadas, as anteriores dadas como perdidas, o dump
  removido. Agora só ela é recusada e `Delivered` diz o que saiu. Lição: uma
  recusa síncrona no meio do lote é tão "parcial" quanto uma de futuro.
- SN: **permissão negada nunca era FATAL** (herdado do S): o NATS avisa só no
  callback assíncrono; publish negado expirava (jetstream, retentado para
  sempre com o sink UP) ou sumia (core, contado como escrito). O sink guarda o
  veredito e o `Health` o devolve como FATAL; no subject do beacon, não.
- SN: senha com `%`, `/` ou `#` sem escape em `sink.nats.urls` ia para o log a
  cada tentativa e segurava o boot até o `boot_timeout`; userinfo na URL junto
  de `sink.nats.username` era ignorada em silêncio; `beacon_interval=1ns`
  aceito. Os três agora são exit 1.
- Menores: `sslmode` repetido na URL passava a recusa de TLS (`Get` lê o
  primeiro, a libpq o último); a porta da URL com `host=/socket` sumia; endereço
  `h1,h2`/porta 0 só falhava depois do `boot_timeout`; SQLSTATE `3D` agora é
  fatal; histogramas de lote contados por tentativa na reingestão.

**Conhecido, não corrigido** (TESTING §8 de SP e SN): SIGHUP lista chaves
derivadas como "exige restart"; `--dump-config` fixa o `pool_size` derivado;
`ring.max_bytes` minúsculo aceito; contadores da recepção sem padding entre
listeners `SO_REUSEPORT`; fuzz de 10 min e medição de carga pendentes.

## 2026-10-07 — W, F, S: `sink=clickhouse`

Pedido: `sink=clickhouse` no W, portado ao F; a entrevista trouxe o S para a
mesma rodada. Entrevista de 9 decisões (recomendação primeiro), experimentos
contra um ClickHouse 25.8 real **antes** do código, SPEC antes do código, suítes
com `-race` contra Redis, PostgreSQL, MySQL, NATS e ClickHouse reais, teste de
fumaça do compose e revisão independente por área.

**Decisões** (as do usuário contra a recomendação, marcadas).
- Cliente `clickhouse-go` v2 no protocolo nativo (**contra** o HTTP da
  biblioteca padrão com `RowBinary`): a oitava dependência, ~4 MB a mais de
  binário e uma dúzia de transitivas, registradas na SPEC.
- `ReplacingMergeTree` com `ORDER BY (event_at, uuid)` e
  `insert_deduplication_token` por lote (hash dos `uuid`, nunca contador).
- `payload` no tipo `JSON` (**contra** `String`); depois do achado abaixo,
  `params`/`query` (W/F) e `structured-data` (S) tipados como `Map`, e **uma
  tabela por formato de payload** (`inbox` nos webhooks, que W e F podem
  dividir; `inbox_syslog` no S).
- `async_insert`, `wait_for_async_insert` e a durabilidade ficam com o
  administrador do ClickHouse (resposta livre do usuário: o objetivo é vazão);
  o servidor não envia nenhum desses ajustes, e a linha de `ring=none` leva `†`.
- Classes pelo precedente: `readonly` e réplica sem Keeper fatais, como o
  `25006`; `MEMORY_LIMIT_EXCEEDED` fatal, como o `OOM` do Redis.
- Sonda de boot por `INSERT` vazio (o ClickHouse não tem transação).

**Achados por experimento, antes do código** (cada um virou regra e teste):
- O tipo `JSON` **descarta objeto vazio**: `params` (sempre `{}`) sumia de toda
  linha, e nenhuma passava no oráculo.
- Chave com ponto vira caminho: `?a=2&a.b=1` voltava como JSON com chave
  repetida, e dois caminhos iguais recusavam o lote com `117`.
- O ClickHouse ordena `UUID` pela segunda metade: `ORDER BY uuid` não poda por
  tempo.
- O `X-ClickHouse-Summary` não revela a deduplicação; a métrica vem do evento
  de perfil `DuplicatedInsertedBlocks`.
- Um `INSERT` de `payload` JSON custa ~6× os bytes em memória no servidor: o
  boot compara `max_memory_usage` com o pior lote.
- O `.sql` do `initdb.d` da imagem roda no banco `default`: os compose usam um
  `init.sh` com `--database` e um healthcheck que espera a tabela.

**Revisão independente** (quatro revisores só leitura: núcleo do sink,
fidelidade do `JSON`, config/docs/porte, adaptação do S), tudo corrigido em W,
portado a F e S, com teste que falhava antes (os principais conferidos por
mutação):
- **Inferência de datas no tipo `JSON`** (a pior): um cliente com
  `Content-Type: 2020-01-01 10:00:00.000` fazia o corpo e o `content.type`
  vazios de **outra** requisição do mesmo lote voltarem como
  `1970-01-01 00:00:00`, e um `X-Slack-Request-Timestamp` virar data; no S, um
  `procid` 12345 e valores de SD de outro emissor. Sem erro nenhum. Todo
  `INSERT` desliga a inferência de datas e de números em texto.
- **Perda silenciosa ou lote recusado com a tabela aceita no boot:** coluna
  `EPHEMERAL` com o nome de uma das dez (o `payload` sumia com sucesso),
  `ReplacingMergeTree(ver, is_deleted)`, CHECK com nome entre crases,
  partição fora de `event_at` (`252` retentado para sempre num backlog de
  mais de 100 partições; reentrega que nunca colapsava). Agora recusados, e
  o `INSERT` leva `throw_on_max_partitions_per_insert_block = 0`.
- **Fatal ou preso sem motivo:** `system.replicas` exige grant na imagem
  oficial (`497` fatal logo depois do boot, agora `WARN`);
  `max_memory_usage_for_user` comparado com um lote só (quatro simultâneos
  davam `241`; agora `pool_size` lotes); a sonda rodava sob o prazo de 1 s do
  health — sem o `max_execution_time` do dreno (perfil `CONST` passava e
  recusava todo lote com `452`, retentado) e sem caber em 60 ms de RTT; `44`
  (`MATERIALIZED`) era `network`. A validação ganhou prazo próprio, o do dreno.
- **Docs:** contagens, referências e o `Ping` que o `Health` não faz.
- **Registrado, não mudado:** `241` e `242` fatais vêm também do limite global
  do servidor e de uma perda breve do Keeper (decisão do usuário mantida;
  lacuna no TESTING).

**Lições.**
- Um tipo "inteligente" no destino é uma superfície de ataque: a inferência
  do `JSON` deixava um cliente reescrever o dado de outro, e nenhum teste de
  ida e volta com um envelope por vez a via. Os testes de fidelidade põem
  atacante e vítima no mesmo `INSERT`.
- Toda regra de boot precisa do contexto em que o dreno roda: a sonda sob
  outro prazo mandou outros ajustes e provou outra coisa.
- Medir o tipo de destino antes de escrever a regra: as duas escolhas do
  usuário (`JSON`, tabela compartilhada) só puderam ser reconciliadas porque a
  perda de `{}` apareceu num `SELECT` de dez segundos, e voltou à entrevista.
- Um fixture inválido passa vazio: um SD com `PARAM-NAME` vazio ia para
  `message`, e a ida e volta "provava" o `Map` sem exercitá-lo. Todo fixture
  confere que tem a forma que diz ter.
- Uma afirmação da SPEC sobre o código (`Ping` no `Health`) precisa ser lida
  contra o código: estava errada no primeiro texto.

## 2026-10-06 — S, W, F: envelope LogHub (JLH) e fila `loghub.inbox`

Pedido: garantir o padrão JLH e a tabela `loghub.inbox` (skill
`loghub-postgres-table-inbox`) nos três servidores. Entrevista de 4 decisões:
F só com a fatia "JLH + identidade + sinks"; W deixa de ser congelada e vira
`type=webhook.request`; `sink=postgres` só na `loghub.inbox`; e (escolha do
usuário contra a recomendação) MySQL e SQLite dos webhooks **espelham** as
colunas da fila. SPEC antes do código, suítes com `-race` contra PostgreSQL 18,
MySQL 8.4, Redis 7 e NATS JetStream reais, e revisão independente por área.

**Decisões.**
- **S**: o sink PostgreSQL enfileira na `loghub.inbox` (DDL literal da skill
  em `docker/postgres/inbox.sql`, hash conferido em teste); `syslog_events`
  fica só no SQLite e no MySQL. O servidor nunca cria a fila, só valida.
- **W**: todo evento é um JLH `type=webhook.request` (`uuid` UUIDv7 nível 3 e
  `event` de uma amostra de relógio, `from`/`space` do `go-loghub-ident`,
  `mode` `fast` ou `bucket` pelo ring), escrito uma vez com o corpo dentro
  (utf-8 ou base64). `server.node_id` e `X-Event-Id` saem; a resposta é
  `X-LogHub-UUID` + JSON. Redis e NATS em `loghub.{space}.{type}`.
- **F**: o mesmo código com `type=webhook`, `mode=fast` e `stage` na resposta;
  o resto do `DECISOES.md` (stage, ring Redis, PROXY protocol, nomes `whf_`)
  continua em aberto (SPEC §0).

**Bugs encontrados e corrigidos** (todos com teste de regressão; os de
validação conferidos por mutação):
- `ON CONFLICT (uuid)` exige `SELECT (uuid)`: um papel só com `INSERT` passava
  no boot e o primeiro lote falhava com `42501` (S e W; o teste antigo do S
  dava `SELECT` na tabela inteira e escondia). Documentado
  `GRANT INSERT, SELECT (uuid)`.
- A validação do PostgreSQL aceitava tabelas em que toda escrita falhava com o
  sink UP: `sid` estreito, `varchar` curto, índice único `DEFERRABLE`, tabela
  particionada (e `23514` sem constraint agora é fatal), banco `LATIN1` (texto
  em dupla codificação, sem erro). E o ambiente libpq (`PGOPTIONS`,
  `PGTARGETSESSIONATTRS`) chegava ao pool.
- `jsonb` recusa `\u0000` (`22P05`, o lote inteiro): S reconstrói a cópia da
  coluna com `raw` exato em base64 e U+FFFD nos demais textos; W/F trocam o
  escape na cópia (o corpo com NUL já vai em base64).
- W/F: fingerprints JA3/HTTP2 cortadas na vírgula; `OPTIONS *` respondido pelo
  `net/http` antes do handler; porta de um `Host` recusado chegando à `url`;
  qualquer cabeçalho de dump com dois campos tratado como legado; NATS e MySQL
  gravavam parte de um lote antes de recusar um envelope; agrupamento da query
  quadrático (amplificação de CPU com milhares de pares).

**Lições.**
- Teste de privilégio não pode conceder mais do que o papel de produção terá:
  o `SELECT` "inofensivo" do teste escondeu o `42501` por uma rodada inteira.
- Toda tabela que a validação aceita tem de aceitar toda linha que o servidor
  produz; o que ela não confere (tipo estreito, partição, encoding,
  `DEFERRABLE`) vira descarte silencioso com o processo saudável.
- `jsonb` não é JSON: NUL fora, chaves reordenadas. Uma fila em `jsonb` não é
  fiel ao byte, e isso vai escrito na SPEC.
- Gerar texto com `\ufffd` por ferramenta pode virar o caractere literal: num
  literal Go entre crases isso troca 6 bytes de escape por 3 de UTF-8. Conferir
  com `grep` depois de cada geração.
- O PostgreSQL 18 em Docker muda o diretório de dados (`/var/lib/postgresql`) e
  recusa o volume antigo: atualizar a imagem pede `down -v` ou `pg_upgrade`.

**Revisão independente dos webhooks** (dois revisores só leitura — envelope/HTTP
e destinos/montagem/docs —, com o `validate.py` da skill reproduzido e os
quatro servidores reais). Corrigido em W, portado ao F; o que é PostgreSQL
também no S:
- **Query e `url`:** chaves com bytes inválidos diferentes viravam a mesma
  chave JSON e um valor sumia em todo parser; `#`, UTF-8 truncado e byte cru
  ao lado de escape faziam a `url` e a `query` decodificarem diferente, e a
  `url` saía fora do RFC 3986. **Decisão:** a `url` percent-encoda só o que uma
  query não aceita (o resto vai cru, como a skill pede); UTF-8 inválido vira um
  U+FFFD por subparte máxima, como o Python, não um por byte; chaves agrupadas
  pelo texto escrito.
- **HTTP:** proxy confiável sem `X-Forwarded-For` punha a porta do socket na
  `url`; `strings.ToLower` Unicode fazia do sinal Kelvin um `k` no host;
  `readBody` dobrava o buffer no EOF do HTTP/2. **Decisão:** `content.type`
  diz o que chegou mesmo quando o cabeçalho sai de `headers`.
- **Posse e leitura:** `ring=sqlite` nunca devolvia o JLH ao pool; `ParseHead`
  aceitava `uuid` maiúsculo e `from` de qualquer versão, e um dump com payload
  `{]` era reingerido (agora `json.Valid`).
- **MySQL:** `varchar` curto, inteiro estreito, `DATETIME` sem `(6)` e índice
  único extra passavam no boot — com `UNIQUE (from_uuid, sid)`, 1 de 3 eventos
  gravado e o `Write` devolvendo sucesso, porque o `ON DUPLICATE KEY` dispara
  em qualquer chave única (a SPEC dizia `1062`/`logic`); número desconhecido
  era `network` e um `3151` se retentava para sempre (agora cai na classe do
  SQLSTATE); lote de vários pedaços commitado pedaço a pedaço (agora uma
  transação). **Decisão:** MariaDB deixa de ser "suportado" — o `JSON` dele é
  `LONGTEXT` — e o boot o recusa pelo nome.
- **PostgreSQL (W, F, S):** `timestamptz(6)` recusado com a mensagem errada
  (o tipo era cortado no `(`); precisão menor que 6 e índice único ou exclusão
  extra agora recusados no boot.
- **NATS:** envelope de outro Workspace num subject sem stream dava
  `no response from stream`, `network`, e o boot entrava em laço; agora é
  fatal nomeando o subject.
- **Compose (W, F, S):** `WORKSPACE` vinha do shell — o Jenkins exporta o
  caminho do job — e o ident saía com 111; agora `WHS_WORKSPACE` /
  `SYSLOG_WORKSPACE`.
- **Oráculo:** o `jlhcheck` era mais fraco que o `validate.py` e passava em
  tudo acima. Agora porta `urlsplit`/`parse_qsl`, acha chave JSON duplicada,
  confere o RFC 3986 e tem testes com valores do CPython; `FuzzServeHTTP` novo,
  por socket real. Dois testes de worker intermitentes (davam `Start` antes de
  enfileirar) corrigidos.

**Lições da revisão.**
- Um oráculo que decodifica antes de conferir não vê chave duplicada: todo
  decodificador guarda a última. Duplicata se procura nos tokens.
- Portar o validador da skill para o oráculo vale mais que reler a skill: os
  três defeitos de `url` só apareceram comparando com `urlsplit`/`parse_qsl`.
- Um fuzz por socket real precisa fechar com RST (`SetLinger(0)`): com
  `TIME_WAIT`, milhares de execuções por segundo esgotam as portas efêmeras do
  macOS e o erro parece do servidor.
- Validação de esquema por família de tipo não basta: largura, precisão e
  índices únicos extras mudam o que o banco aceita, e cada lacuna vira descarte
  ou perda silenciosa com o sink UP.

**Pendências.** Rodadas longas de fuzz (10 min por alvo, `FuzzServeHTTP`
inclusive); `pg_is_in_recovery()` verdadeiro sem réplica real; o `sink=mysql`
do S tem as mesmas lacunas que o do W tinha (validação por família, índice
único extra, número desconhecido como `network`, pedaços commitados um a um);
o resto do plano do F (`docs/DECISOES.md`); commit único só com o "sim" do
usuário.

## 2026-10-05 — S: go-syslog-server reescrito do zero no paradigma LogHub

Entrevista (18 decisões, `go-syslog-server/docs/DECISOES.md`), SPEC nova
(`docs/SPEC.md`), código, testes com `-race`, e uma verificação independente
somente leitura por área (conversão, rings, recepção, sinks, app/config/docs).
Detalhe, números e lacunas em `go-syslog-server/docs/TESTING.md`.

**Decisões.** O antigo virou `go-syslog-standard-server` (referência, nunca
dependência); RELP saiu para `go-relplog-server` (só especificação). Cada
mensagem vira um envelope JLH `type=syslog` (`step = 0`, UUIDv7 nível 3, uma
amostra de relógio para `uuid` e `event`), entregue por padrão a NATS JetStream
em `loghub.{space}.syslog`. Cru no ring, conversão no dreno: o JLH é função pura
de cabeçalho + cru + config congelada, e todo retry, dump e reingestão reenvia
os mesmos bytes. Desvios deliberados das skills, D1 a D8, listados na SPEC §1.3
(moldura TCP por conexão com confirmação, TAG estrita sem timestamp, schema só em
teste, espaço em branco nos campos `^\S+$`, avisos de SD com texto colado, anos
fora de 0001–9999).

**Bugs encontrados e corrigidos** (30, todos com teste de regressão conferido
por mutação; lista completa em `TESTING.md` §9). Os que mais ensinam:

- Fuzz: o leitor PROXY v1 consumia o 108º byte antes de conferir o teto de 107.
- Um `logic` na entrega do boot saía com 3 e deixava o mesmo dado para o boot
  seguinte: crashloop com os transportes fechados. E, corrigido isso, um retry em
  volta do `Write` reconvertia e deixava sem escrita os chunks depois do
  recusado, já confirmados: `Instrumented.Deliver`, chunk a chunk.
- Um worker de dreno saía para sempre, sem log, num erro de `Dequeue` do
  `ring=sqlite`: dreno a zero com `/healthz` 200.
- PostgreSQL com coluna anulável virada `NOT NULL` passava na validação e
  recusava toda escrita como `logic`, com o sink UP.
- A retentativa do backpressure TCP cobrava o rate-limit de novo, e a mensagem
  retida acabava descartada como `ratelimit`.
- TLS 1.3: um `KeyUpdate` prendia o leitor numa escrita e o `Close` travava.
- O ring em memória contava `len(cru)` (uma linha de 8 KiB segura 64 KiB), e
  com um lote retido aceitava a quinta mensagem num teto de quatro.
- `SIGHUP` durante o boot matava o processo com 129, sem log; um sink FATAL
  durante o dreno do `SIGTERM` saía com 0; e nos Compose nenhuma chave
  recarregável recarregava, porque vinha por env, que vence o INI.

**Lições.**

- O `\s` do motor de regex do teste não é o do schema: RE2 deixa passar
  U+00A0, U+2028, U+FEFF e `\v`; o Python da skill recusa ainda U+001C–U+001F e
  U+0085. O oráculo confere o conjunto completo.
- No boot vale a mesma regra de classes dos workers; uma recusa definitiva no
  boot é fatal na hora, nunca 300 s de espera.
- Um valor aceito pela config precisa significar algo para quem o consome
  (aliases de facility, `windows-1252`).
- Casos de recusa precisam de um controle válido: uma regra nova pode recusar
  todos pelo motivo errado e o teste continua verde.
- A verificação independente por área achou 49 defeitos (15 deles na última,
  de app/config/docs/Docker) que a suíte própria de cada área não via. Vale o
  custo, área por área.
- Sinal cuja ação padrão encerra o processo é capturado desde a primeira linha
  do `main`, não só na janela em que alguém pensou nele.

## 2026-09-15 — verificação independente e segunda rodada

Um sub-agente por problema, somente leitura, com testes direcionados, revisou
as quinze correções contra o código, a spec e os testes. Nenhum problema
voltou e nenhuma regressão foi encontrada.

| # | Veredito | O que a revisão ainda achou e o que foi feito |
|---|---|---|
| 001 | corrigido (W+S) | `ring=none` nunca devolvia o buffer ao pool no sucesso, contra §12.3/§14.2: agora devolve após o `Write`. Aberto: o ramo `writerDone → ErrClosed` sem cobertura |
| 002 | corrigido | Linha de revisão da spec citava §5.1; §15.2 nomeava alvos de fuzz inexistentes. Corrigidos |
| 003 | corrigido | Literal `64<<10` no `relp.go` virou a constante. Registrado: custo fixo por conexão sem teto com `max_conns = 0` (decisão) |
| 004 | corrigido, cobertura parcial | Teste do heartbeat contra arquivo somente leitura (W+S); tabela `health` nos DDL das specs. Registrado: sonda espera o mutex do escritor sem contexto |
| 005 | corrigido | A sonda do webhook era um `PING` e não via `READONLY`: passou a `XLEN` + `XADD NOMKSTREAM`, como o syslog; resposta fatal à sonda é FATAL na primeira checagem (W+S) |
| 006 | corrigido, **defeito novo** | Senha errada do Redis esperava `sink.boot_timeout` inteiro (o rueidis autentica na discagem e o erro não era classificado). Fatal na hora (W+S). Admin fechado nas saídas antecipadas; `/readyz` limpa só com a porta pública aberta |
| 007 | corrigido | Contrato do `Dequeue`, tabela de códigos de saída e quarentena sem teto documentados; quarentenas pendentes terminam no stop do escritor |
| 008 | corrigido | Keep-alive não chegava ao socket sob o limitador (`*limitConn` não é `*net.TCPConn`); teste dos dois tetos juntos. Registrado: recusa por IP invisível |
| 009 | corrigido (W+S) | Exemplo do `WARN` nas specs alinhado ao código; TESTING do syslog alinhado |
| 010 | corrigido, teste parcial (W) | O teste do webhook provava só a contabilidade: agora liga e desliga o limitador pelo arquivo e observa `429`/`200` |
| 011 | corrigido, cobertura parcial (S) | Teto por IP entre listeners e a fiação (todos os listeners no mesmo limitador) provados nos dois módulos; `--help` diz "por servidor/transporte" |
| 012 | corrigido (W+S) | Casos `0s` que faltavam; `sink.retry_backoff_max` recusa zero; `sink.fail_timeout = 0` documentado |
| 013 | corrigido (W), parcial (S) | Spec do syslog sem `timeout` em `kind`; séries de conexão expostas para `udp`/`unix`; dump falho contava a perda duas vezes; cancelamento do shutdown contava como rede. Todos corrigidos |
| 014 | parcial | Três resíduos fora da lista (comentário "three sinks", invariante `<=` no AGENTS.md do syslog, §17.5 prometendo suítes inexistentes). Corrigidos |
| 015 | corrigido (W+S) | Tabelas de validação qualificadas por modo de ring; gramática distinta de `GOMEMLIMIT` e do limite do Compose documentada |

**Lições da segunda rodada:**

- Uma revisão que lê o código de novo, com outra cabeça, acha o que a
  correção deixou: dois defeitos reais (005, 006) e uma dúzia de incoerências
  de documentação. Vale o custo.
- Clientes que fazem trabalho no construtor (o rueidis disca **e autentica**
  no `NewClient`) devolvem erros de protocolo por um caminho que não é o dos
  comandos; todo erro de construção passa pela mesma classificação.
- Sonda de saúde é escrita, não leitura: `PING` e `XLEN` não distinguem um
  servidor de uma réplica promovida.
- Contadores de perda contam cada evento uma vez; "perdido por X" e "perdido
  por Y" são razões exclusivas.
- Um `type assertion` num wrapper falha em silêncio: ao envolver uma conexão,
  todo caminho que precisa do tipo concreto passa por um desembrulho explícito.

## 2026-09-14 — auditoria estática e correção dos 15 problemas

Decisões transversais: sem git nesta rodada (o `.git` vazio dentro de
`go-webhook-server/` fica até o versionamento subir para a raiz); os 15 em
ordem de severidade, spec antes do código, depois código, depois testes; suíte
de cada módulo com `-race`; Redis e PostgreSQL efêmeros em Docker para a suíte
do webhook.

### Corrigido

**001 · W+S · crítica · Liberação prematura de buffer após cancelamento.**
Bug: um `Enqueue` cancelado devolvia o buffer ao pool enquanto a escritora do
`ring=sqlite` (group commit) ou o pipeline do rueidis (`ring=none`) ainda o
liam — corpo de um cliente dentro do evento de outro, ou um grupo inteiro
recusado. Correção: o `Enqueue` do SQLite espera o commit mesmo com contexto
cancelado; o `ring=none` escreve num contexto desligado do cliente, limitado
por `sink.op_timeout`; nos caminhos de erro de posse indefinida o item é
descartado sem voltar ao pool (`Discard`/`Taint`). Decisão: transferência de
posse + `Discard`; a fase 3 do shutdown do syslog manteve a ordem (inverter
travaria o `Close()` nos leitores em backpressure). Lição: um buffer só volta
ao pool quando nenhuma outra goroutine puder lê-lo; erro devolvido não significa
"ninguém mais usa" — na dúvida, deixa para o GC.

**002 · W · alta · `X-Forwarded-For` em várias linhas permitia spoofing.**
Bug: `Header.Get` devolve só a primeira linha; um proxy que insere linha nova
(HAProxy `option forwardfor`, regras *add-header*) deixava a linha forjada do
cliente vencer — bypass de `ratelimit.exempt`, chave de rate limit envenenada,
origem falsa na auditoria. Correção: todas as linhas concatenadas com vírgula na
ordem de chegada antes da caminhada da direita para a esquerda; de `X-Real-IP`
vale a última linha. Verificado contra um HAProxy 3.0 real. Lição: cabeçalho de
lista é a junção de todas as linhas (RFC 9110 §5.3); toda decisão de confiança
sobre cabeçalho precisa de teste com `Header.Add` repetido.

**003 · S · alta · Alocação pelo tamanho declarado dos frames.** Bug: moldura
por contagem de octetos e `DATALEN` do RELP alocavam o buffer pelo número que o
cliente declarava, antes de receber um byte — 16 MiB por conexão com nove bytes
enviados. Correção: `readCounted` cresce em blocos de 64 KiB conforme os bytes
chegam, com o fusível `n > max` antes de qualquer leitura. Decisão: só o
crescimento em blocos; padrões inalterados. Lição: nunca alocar pelo que o par
declara; alocar pelo que chegou, com teto configurado.

**004 · W+S · alta · `quick_check` varria o banco inteiro na sonda de saúde.**
Bug: a sonda do `sink=sqlite` rodava `PRAGMA quick_check` a cada
`health_interval` sob o mutex do escritor — custo proporcional ao arquivo,
latência de escrita crescendo com os dados. Correção: heartbeat de escrita
(`UPSERT` numa tabela `health`), que prova o que importa (arquivo gravável,
volume montado); `quick_check` só na abertura. Decisão: heartbeat. Lição: sonda
periódica tem de ser O(1) e provar escritabilidade, não integridade; nada
proporcional ao tamanho dos dados roda em laço.

**005 · W · alta · Erros permanentes do Redis tratados como `logic`.** Bug:
`WRONGTYPE`, `NOPERM`, `NOAUTH`, `READONLY`, `OOM` e afins eram classificados
como erro por evento (descarte silencioso), escondendo um destino inteiro fora
do ar. Correção: passam a ser fatais (saída 3, dump de emergência com
`ring=memory`), com a tabela de classificação idêntica à do syslog. Decisão:
fatal imediato. Lição: classificar erro de destino pelo alcance — por evento
descarta, por destino é fatal e barulhento; tabelas de classificação iguais
entre módulos irmãos.

**006 · W · média · Boot com Redis indisponível saía sem retentar.** Bug:
`rueidis.NewClient` disca na construção e o `build()` rodava antes da porta
admin: `exit 3` imediato, sem esperar `sink.boot_timeout` e sem `/readyz` para
diagnosticar, ao contrário da spec. Correção: porta admin aberta antes do sink,
`buildSink` retenta com backoff até `boot_timeout`, `/readyz` responde o motivo
durante a espera, porta pública só depois. Lição: conhecer a semântica de
construção do cliente (discagem ansiosa); diagnóstico sobe antes das
dependências; testar "dependência que sobe tarde".

**007 · S · média · `head` indecodificável travava o boot.** Bug: uma linha do
`ring.db` que não decodificava era pulada sem ajustar a contabilidade; o dreno
do boot nunca via o ring esvaziar e os transportes nunca abriam. Correção:
tabela `quarantine` no próprio `ring.db` (a linha é movida, `items`/`bytes`
ajustados com o mesmo critério do `recount`, contador
`syslog_ring_quarantined_total`) e guarda de não-progresso no dreno (`Recount`
e, se continuar travado, saída 3). Decisão: quarentena no próprio arquivo.
Lição: todo "pular" tem de ser contabilizado como uma entrega; todo laço de
dreno precisa de guarda de não-progresso.

**008 · S · média · `Accept` recursivo por IP.** Bug: o listener limitado por
IP chamava `Accept()` recursivamente a cada conexão recusada — pilha sem limite
sob uma rajada de um IP acima de `max_conns_per_ip`. Correção: `connLimiter`
com `Accept` em laço, semáforo global e mapa por IP liberados exatamente uma vez
em todos os caminhos. Lição: laço de accept/retry nunca recorre; recusa é
`continue`, não chamada.

**009 · W+S · média · `SIGHUP` esquecia chaves que exigem reinício.** Bug: o
"atual" era substituído pelo arquivo novo inteiro, então uma chave de reinício
alterada só avisava no primeiro `SIGHUP`. Correção: duas referências (valor de
boot e valor em execução); chaves de reinício comparam com o boot e avisam em
todo `SIGHUP` enquanto diferirem. Lição: o reload compara cada classe de chave
com a linha de base certa; testar dois `SIGHUP` seguidos.

**010 · W+S · média · `SIGHUP` ignorava a reversão ao valor de boot.** Bug: o
irmão do 009 — chave recarregável revertida ao valor de boot era comparada com
o boot e não com o valor em execução, e não era aplicada. Correção:
recarregáveis comparam com o valor em execução; `current` só muda quando algo
é aplicado. Lição: testar o caminho de ida e o de volta (muda, reverte) de toda
chave recarregável, pelo efeito observável e não pelo log.

**011 · W+S · baixa · `max_conns` era por listener.** Bug: com `listeners = N`
o teto real de conexões (global e por IP) era N vezes o configurado. Correção:
um limitador por servidor (W) / por transporte (S), compartilhado por todos os
listeners; spec e ajuda das chaves dizem "por servidor/transporte". Lição:
limite documentado como global vive em um único objeto compartilhado;
multiplicar listeners não pode multiplicar limites; testar com `listeners > 1`.

**012 · W+S · baixa · Durações zero aceitas na validação.** Bug: chaves de
duração aceitavam `0s` onde zero não significa "sem limite" — laço quente,
timeout imediato, sonda em rajada. Correção: validação recusa zero onde a spec
não o define como desligado. Decisão: recusar na validação. Lição: toda chave
de duração declara o que zero significa, e a validação impõe isso.

**013 · W+S · baixa · Métricas declaradas sem alimentação.** Bug: séries e
rótulos documentados na spec nunca recebiam valor (`kind="timeout"` do sink,
razões de descarte, eventos abandonados), e sobras do `ring=sqlite` no
shutdown eram contadas como perda. Correção: as medições foram implementadas;
com `ring=sqlite` as sobras ficam para o próximo boot, sem contagem, sem dump
e com saída 0. Decisão: implementar, não remover. Lição: métrica declarada é
contrato; só se declara o que é alimentado, e cada rótulo tem teste de
exposição.

**014 · W+S · baixa · Documentação e comentários desatualizados.** Bug: nome do
dump de emergência, referências de seção, identificadores do README, item
pendente do `AGENTS.md` §10 e outros pontos divergiam do código. Correção:
textos alinhados ao comportamento atual; o `.git` vazio em `go-webhook-server/`
ficou por decisão. Adendo: o `AGENTS.md` do syslog descrevia suítes contra Redis
e PostgreSQL que não existem; corrigido. Lição: ao renomear ou mudar
comportamento, `grep` do nome antigo em docs e comentários faz parte da
mudança; spec primeiro, código depois.

**015 · W+S · baixa · Limite de memória ausente na implantação.** Bug: os
compose não definiam `GOMEMLIMIT` nem limite do contêiner, embora a spec os
tratasse como obrigatórios — o OOM killer chegaria antes do GC e levaria o
backlog do `ring=memory`. Correção: `GOMEMLIMIT` 512 MiB e limite 768M no
serviço-base (parametrizáveis por `.env`), `ring.max_bytes` de 256 MiB nos
compose de `ring=memory`, aviso de boot por modo de ring. Decisão: 512 MiB /
768M parametrizáveis. Lição: arquivo de implantação é parte do contrato — o que
a spec chama de obrigatório tem de estar no compose, e é verificado subindo o
compose de verdade.

### Verificação

- `gofmt`, `go vet` e `go test -race ./...` verdes nos dois módulos; webhook
  também com Redis e PostgreSQL reais. O syslog não tem suíte contra destinos
  reais (`docs/TESTING.md` §8) — a cobertura veio das verificações manuais.
- Fuzz de 10 minutos por alvo, seis alvos, sem falha.
- Os 24 compose sobem saudáveis, sem aviso de `GOMEMLIMIT`, com o limite de
  memória honrado; ring cheio (256 MiB) com Redis pausado: RSS máximo de 317 MiB
  (W) e 329 MiB (S), sem OOM.
- HAProxy real confirmou o 002; rsyslog e syslog-ng reais passaram após as
  mudanças de moldura; envio tardio do rueidis reproduzido com Redis pausado e
  chegou íntegro byte a byte (001).

### Regras que saem destas rodadas

1. Posse de buffer é explícita: quem devolve ao pool prova que ninguém mais lê.
2. Nada é alocado ou decidido pelo que o par declara; só pelo que chegou.
3. Sonda de saúde é O(1), é escrita, e prova escritabilidade.
4. Erro de destino é classificado pelo alcance, inclusive o erro da discagem;
   o que derruba o destino é fatal na hora.
5. Todo "pular" conta como entrega; todo dreno tem guarda de não-progresso;
   cada perda conta uma vez.
6. Reload compara cada chave com a linha de base certa e é testado ida e volta.
7. Limite global vive num objeto só; validação diz o que zero significa.
8. Métrica declarada é alimentada e testada; doc, `--help` e compose acompanham
   a spec, e um `grep` do valor antigo fecha toda mudança.

### Pendências

- Teste de RSS de 30 min em nó Linux dedicado e dimensionamento por combinação.
- Suítes automatizadas do syslog contra Redis e PostgreSQL reais; teste
  automatizado do envio tardio do rueidis.
- Lacunas registradas nos `docs/TESTING.md` §8 de cada módulo (custo fixo por
  conexão com `max_conns = 0`, recusa por IP invisível, mutex da sonda do
  SQLite, quarentena sem teto, ACL da chave de prova do Redis).
- Versionamento na raiz do monorepo (o `.git` vazio de `go-webhook-server/`).
