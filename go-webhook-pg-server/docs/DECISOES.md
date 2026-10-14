# go-webhook-pg-server — Decisões de projeto

Registro da entrevista de 2026-10-07 que criou o `go-webhook-pg-server` a partir
do `go-webhook-fast-server`. A fonte da verdade é `docs/SPEC.md`; cada decisão
abaixo aparece lá com o número da seção.

## Pedido

> Crie o software "go-webhook-pg-server" baseado no "go-webhook-fast-server" que
> fará sink somente no PostgreSQL e ring somente na memória, com emergência em
> arquivo JSON Lines. Os demais recursos do "go-webhook-fast-server" devem ser
> retirados. O objetivo é ter um servidor de webhooks rápido e com dependências
> mínimas.

## Decisões do usuário

| # | Tema | Recomendação | Decisão |
|---|---|---|---|
| 1 | Até onde vai o corte | Só os outros rings e sinks | **Também rate-limit e recarga por `SIGHUP`** (override). h2c, `SO_REUSEPORT`, clientip/proxy confiável e a porta admin ficam (§0, §6, §13.3) |
| 2 | Emergência em JSON Lines | Mesma regra de disparo, formato JSONL | Recomendada: uma linha = o JLH + `\n`, `emg-<agent>-<unixnano>.jsonl`, `O_EXCL` + `fsync`, reingestão no boot direto no PostgreSQL; o `.bin` do fast não é lido (§9) |
| 3 | Compatibilidade de configuração e métricas | Drop-in do fast | Recomendada: mesmas chaves, env `WHS_*` e métricas `whs_*`; `ring.mode` e `sink.type` aceitos só com `memory`/`postgres`; padrão de `sink.type` vira `postgres` (§0, §13) |
| 4 | Estrutura do código | Manter as interfaces Ring/Sink | Recomendada: mesmos pacotes e arquivos do fast, menos o que saiu; correções portam por `diff -ruN` (§2.1, §14.2) |

## Interpretações registradas antes de construir

Restituídas ao usuário na mesma rodada, porque as decisões 1 e 3 se tocam:

- **Seletores de compatibilidade.** O "drop-in" da decisão 3 e o corte do
  rate-limit da decisão 1 conflitam numa chave: um INI do fast pode ter
  `[ratelimit] enabled = false`. Regra adotada: as três chaves que **escolhem**
  um recurso — `ring.mode`, `sink.type` e `ratelimit.enabled` — continuam
  aceitas, cada uma só com o valor que este servidor implementa; toda outra chave
  de recurso retirado é chave desconhecida (`exit 1`), como o `server.node_id`
  na rodada do JLH. Um INI que **liga** um recurso retirado não pode subir em
  silêncio.
- **`SIGHUP` ignorado, não fatal.** Sem recarga, o sinal é capturado e ignorado
  com `WARN`: a ação padrão do Go o trataria como término, e um `kill -HUP` de um
  procedimento antigo derrubaria a instância sem dreno nem dump.
- **Linha danificada é pulada, não o resto do arquivo.** Consequência natural do
  JSON Lines: o `\n` é o enquadramento, então uma linha ruim não tira o leitor de
  sincronia. As íntegras são entregues e o arquivo vira `.corrupt` (§9.2).

## Defaults assumidos (sem pergunta)

- `sink.workers` com padrão **4** direto no registry (no fast, 8 rebaixado para
  4 com postgres): o efeito é o mesmo e some o caso especial "valor de origem
  default".
- A validação de boot do PostgreSQL roda no prazo `sink.op_timeout`, como a SPEC
  do fast já dizia e o código do fast não fazia (§8.5.5). O parágrafo da SPEC do
  fast trazia texto do ClickHouse (`max_execution_time`, `452`, *materialized
  views*); registrado no `CHANGELOG.md` da raiz para o porte de volta.
- Linha do dump com teto no maior envelope da configuração **atual**
  (`envelope.MaxEventBytes`): sem teto, um arquivo corrompido sem `\n` vira uma
  alocação do tamanho do arquivo.
- Um envelope com `0x0A` cru (impossível pelo serializador, conferido por fuzz)
  fica fora do dump e é contado como perda, sem derrubar o dump dos demais.
- Testes de `app` sem PostgreSQL usam um sink falso **no arquivo de teste**; o
  `sink=none` de produção saiu.
- `docker/` fica com um compose só (`memory` → `postgres`, com o PostgreSQL 18 e
  a `loghub.inbox` criada pelo `initdb`).

## Porte das correções do `go-webhook-nats-server` (2026-10-11)

O N nasceu deste módulo em 2026-10-10, e a revisão independente dele achou
defeitos herdados daqui. Portados sem decisão nova (cada um com teste e
mutação, `TESTING.md` §9):

- Recusa fatal no boot com log próprio ("the sink refused this server
  permanently"), não "did not become reachable within sink.boot_timeout".
- `ring.Commit` antes do `Release` também na recusa `logic` dos workers.
- `shutdown.drain_timeout` é **um** prazo para as fases 3 e 4 juntas (§10.3).
- Dump escrito sob `.emg-…jsonl.tmp` e ligado ao nome final; sem *hard link*,
  renomeado depois de conferir o nome livre; `fsync` do diretório como melhor
  esforço; no boot, o temporário gêmeo de um dump é removido e o de um dump
  interrompido é informado num `WARN` (§9.1). O formato não mudou: um dump do P
  continua sendo reingerido pelo N, e vice-versa.
- Só arquivos regulares são dumps (§9.2).
- O dump soma aos gauges `whs_emergency_*` em vez de reler o diretório; o boot
  continua recontando do disco depois da reingestão.

Da revisão independente do porte (mesmo dia): o ring de memória é **selado
antes do último `Drain`** do shutdown (um upload que terminasse depois recebia
`200` e se perdia; o prazo único das fases 3 e 4 tirou a folga que antes o
pegava), e os parsers de INI e de flags **não ecoam valores** (o N já não
ecoava). Os dois também no fast e no W.

Da revisão do porte de volta ao N (mesmo dia), portado com o N como
referência: o teste do `Seal` age **dentro** do `Drain` (`App.wrapRing`, nulo no
binário), não depois dele, que não distinguia um `Seal` antes do `Drain` de um
entre os dois; o teste da barreira roda 3000 rodadas; e o parser de flags só
nomeia uma flag que este servidor conhece e não cita nada depois de um valor.
Fica, decidida como no N, a brecha da cauda com traço depois de um valor não
vazio (`--x=a -b`): escondê-la esconderia os erros de digitação (TESTING §8
item 18). Fechada em 2026-10-13 (seção abaixo).

**Não portado**: a porta estreita do dump do N (`shape.go`). Aqui a fila
recusa o que o N precisa recusar sozinho (CHECKs, `jsonb`), e a reingestão já
refaz item a item um lote recusado como `logic`.

## Porte das correções da família syslog/RELP (2026-10-13)

Uma subseção por área da rodada; o que veio igual dos irmãos está na SPEC.

### Ciclo de vida e dump

Padrões assumidos nesta rodada (contestáveis); o que veio igual do S, do SP e
do RN está na SPEC (§9, §9.2, §10.3, §10.5), na mesma forma do fast:

- **O `fsync` do diretório decide a remoção e a quarentena, não o dump.** O SP
  recusa o dump cujo diretório não sincroniza; aqui ele continua melhor esforço
  (§9.1, decisão do porte de 2026-10-11). Na remoção e na quarentena a falha
  vem depois do `unlink`/`rename` e vira `WARN` (`emergency.ErrNotDurable`).
- **Com `emergency.enabled = false` a perda continua sendo `abort`** (§9.3),
  não `emergency_failed` como no SP; o envelope deixado fora do dump pela
  defesa do enquadramento passa a ser contado pelo próprio dump.
- **A leitura dos dumps em todo boot** (a regra do fast, pendência registrada
  na SPEC) entrou nesta rodada por ser do mesmo caminho: com a chave
  desligada, um `emergency.dir` que não se lista é `ERROR` e o boot segue.
- **Não se aplica o `quitNow` do SP:** `SIGQUIT` fora do shutdown é a ação
  padrão do Go (§10.3); o canal do aborto, que inclui `SIGQUIT`, passa a ser
  registrado antes de o contexto soltar `SIGTERM`/`SIGINT`.
- **Não se aplica o prazo da reingestão "sem progresso"** do SP: a reingestão
  daqui não espera nem retenta — cada lote tem o seu `sink.op_timeout` e uma
  recusa de rede ou fatal sai com 3 (§9.2). A leitura é do arquivo inteiro, não
  em fluxo, então não há "pedaço" para o *gauge* mostrar: ele é do arquivo, e
  recontado do disco no fim.
- **Fechamento em paralelo:** já era (`Server.Shutdown`).

### Configuração

As regras de configuração do `go-relplog-server` e dos cortes dele, já no
`go-syslog-pg-server` (SPEC §13.2, tabela de §10.1). Padrões assumidos,
contestáveis:

- **Tetos da família:** `ring.capacity_items` 16 Mi (só há ring de memória);
  `sink.batch` 1 Mi, o do fast (que leva lotes de 100 000 linhas ao
  ClickHouse), e não os 65 536 do SP: o código é comum aos quatro webhooks, e
  um `Dequeue` de 1 Mi ponteiros são 8 MiB; `sink.workers` 1024; `pool_size`
  4096; `server.listeners` 256.
- **As seções do fast que este servidor não tem contam como conhecidas**
  (`retiredSections`) para a regra da cauda: `--sink.redis.addrs=x` depois de
  uma flag com valor é nomeada, como sozinha, em vez de culpar a flag anterior
  (a mesma escolha do SP com as seções do `go-syslog-server`).
- **Não se aplica:** `sink.batch_max_bytes` (*revisto em 2026-10-14: a chave
  existe agora, por decisão do usuário; ver "Porte das correções de W e F"*),
  `ring.max_inflight` e
  `ring.sqlite.commit_batch` (não existem aqui; o teto de 1 GiB de um parâmetro
  já é `WARN` de §8.5.4), as regras de URL PostgreSQL da `LOGHUB_EB_URL` (não
  há `LOGHUB_EB_URL` nem outro leitor de URL ou DSN: `[sink.postgres]` são
  chaves separadas) e o socket com `tls=true`, que já era recusado.

### O destino PostgreSQL

Portado do `go-syslog-pg-server` e do `go-relplog-pg-server`, com o código do
`go-webhook-fast-server` (o `sink/postgres.go` segue igual ao dele, salvo os
comentários que já diferiam). Contestáveis:

- **O check de saúde tem conexão própria (`pgx.Conn`), fora do pool**, em vez
  da recusa de `pool_size < sink.workers + 1` que o RP adotou para o mesmo
  defeito (escritas lentas seguram todas as conexões, o check espera uma, DOWN
  e FATAL com o banco sadio). Aqui só há `ring=memory` e a recusa bastaria, mas
  o código é o do fast, onde com `ring=none` nenhum `pool_size` bastaria, e
  manter os dois iguais vale mais. Custo: `pool_size + 1` conexões por
  instância.
- **A derivação continua `sink.workers + 1`** (a folga só é aberta se usada), e
  um `pool_size` explícito **abaixo** de `sink.workers` é `WARN`; igual a
  `sink.workers` não avisa mais (avisava abaixo de `workers + 1`).
- **Uma conexão de check reaproveitada que falha é refeita uma vez** dentro do
  mesmo prazo: o *ping* do pool escondia uma conexão parada que morreu.
- **Não se aplica:** o `Deliver` com `logic` antes do `ctx` do RP (os workers
  daqui já tratavam a classe antes do contexto; ficou um teste de guarda); o
  prazo próprio da validação (já existia; veio a prova contra o servidor real,
  `validation_deadline_test.go`, do fast).

## Porte das correções de W e F (2026-10-14)

Porte das correções que fecharam a SPEC §19.1 do `go-webhook-server` (W) e do
`go-webhook-fast-server` (F) em 2026-10-14 (relatórios `tasks/W-A-result.md`,
`W-B-result.md`, `W-C-result.md` e a triagem `tasks/triage-FPN.md` da raiz). O
código é o de F, portado por `diff`; as regras estão na seção normativa da
SPEC. Aqui fica o que foi decisão:

- **CONTESTÁVEL -- `max_conns_per_ip` conta o endereço exato** (SPEC §13.5; W
  6). O usuário não decidiu para a família webhook; a decisão segue a do F
  (`go-webhook-fast-server/docs/DECISOES.md` 4c), que segue o precedente da
  família syslog/RELP (`go-relplog-server`, decisão 16). Um /128 no IPv6, IPv4
  mapeado = IPv4. Contar por prefixo IPv6 foi recusado: um /64 pode ser uma LAN
  inteira de emissores, e os emissores de webhook saem de *pools* de endereços
  IPv6 vizinhos. Contra um /64 hostil: `max_conns` e o firewall (aqui não há
  rate-limit). Alternativa registrada no F: uma chave aditiva
  `server.max_conns_v6_prefix` (padrão 128). Teste de trava:
  `TestPerIPLimitCountsTheExactAddress`.
- **Saída de um listener parado** (SPEC §10.3, §10.5; W 5), padrão assumido, o
  do F: o listener retenta todo erro do `accept` salvo o próprio fechamento e
  nunca desiste, então não há código de saída novo (a direção da pendência
  sugeria um); um `Serve` que ainda assim termine com erro passa pelo shutdown
  gracioso e sai com `1` se ele foi completo, ou com o que o shutdown der (`3`,
  `130`).
- **O ring de memória cobra o buffer retido** (decisão do usuário,
  2026-10-14, para a família webhook; SPEC §7.2, §12.2, §12.4; W 8).
  `cap(Env)` mais `ring.ItemOverhead` = 256 bytes (`unsafe.Sizeof` do
  `envelope.Item` medido aqui: 80 bytes com o `Charged`, o mesmo do F), gravado
  em `Item.Charged`. Só cobrar: sem cópia para a classe justa e sem mudar os
  padrões; com eventos pequenos o teto de bytes manda antes e uma instância
  perto do teto vê `429` antes. Os lotes retidos pelos workers passam a contar
  no teto de itens (W 30, achado da triagem, sem decisão nova).
- **`sink.batch_max_bytes`** (decisão do usuário, 2026-10-14; SPEC §8.1; W 11).
  Padrão 16 MiB, de 1 byte a 256 MiB (exit 1 fora), corte no worker depois do
  `Dequeue` e na reingestão do dump, com `sink.SplitByBytes`; um envelope
  maior que o teto vai sozinho. O compose repassa `WHS_SINK_BATCH_MAX_BYTES`
  (padrão `16MiB`).
- **O worker sobrevive a um erro de `Dequeue`** (SPEC §8.1 passo 1; W 10). O
  ring de memória não devolve outro erro que não o fim do contexto; a regra
  veio por simetria com o F e o SP, para o laço dos workers continuar igual ao
  deles.
- **A porta do dump recusa U+2028/U+2029 crus** (SPEC §9.2; W 20). Aqui não
  havia defeito (sem conferência de chave repetida, e o `jsonb` normaliza),
  só a mesma folga; a recusa veio para a porta ser a da família.
- **Também portados:** a checagem rápida do passo 9 por bytes (W 2), o dreno
  de recusa só em HTTP/1.x sem `Expect` (W 3), `write_timeout` contado da
  resposta (W 4), o ambiente libpq escondido no parse do pool (W 15), `NaN`
  recusado no parser, `admin.bind` vazio recusado e o `quoteINI` no dump
  (W 24, 25, 26, as regras do N), a CA no compose e o teste do compose (W 27,
  com o de imagens fixadas de W 28) e os guardas de regressão em `t.Fatalf`
  (W 29). Sem decisão nova.

## Revisão independente da família webhook (2026-10-14)

Correções da revisão independente da família webhook (`tasks/review-web-*.md`),
com teste e mutação; as decisões do coordenador da rodada foram seguidas, e o
que segue é o que ficou a escolher no caminho (padrões assumidos,
contestáveis). A linha de cada item está na §19.1 do `go-webhook-server`.

- **Primeiro sinal no shutdown por erro do `Serve`** (W 31): tolerado como no
  FATAL; o dreno e o `pre_stop_delay` continuam os de um `SIGTERM`
  (`tolerateFirst` é um parâmetro à parte de `fatal`).
- **`read_timeout = 0`** (W 32): continua aceito; nesse caso o handler não limpa
  o prazo de escrita e o `write_timeout` volta a limitar o upload. A
  alternativa, exigir `read_timeout > 0`, foi descartada para não recusar uma
  configuração que já subia.
- **Piso de `ring.max_bytes`** (W 35): `PooledCap(MaxEventBytes) +
  ItemOverhead`, exit 1 (não `WARN`), como o piso dos cortes RELP.
- **`admin.bind`** (W 40): a porta 0 também é recusada (o mesmo sorteio da
  porta vazia); os testes que usavam `:0` passaram a uma porta fixa que não
  chegam a abrir.
- **Byte de controle num valor de texto** (W 41): recusado abaixo de `0x20`
  salvo o tab, e o `0x7f`; o tab fica porque o INI o lê e o dump o relê entre
  aspas. A mensagem dá o byte e o *offset*, nunca o valor.
- **Só documentados**: o `Expect: 100-continue` com corpo enviado sem esperar
  (§4.3), o `ItemOverhead` sem a cópia do `held` (§7.2) e o recusado no
  `.corrupt` sem *gauge* (§9.2).
- **Só documentados também**: o *keep-alive* do `dialPostgres` e o ambiente
  `PG*` escondido do processo no parse (§8.5.2).

## Riscos

- **Divergência com o fast.** Dois módulos sem código compartilhado: uma correção
  num pacote comum (`envelope`, `clientip`, `server`, `sink/postgres.go`,
  `sink/workers.go`) precisa ser portada à mão. `diff -ruN -x .DS_Store
  ../go-webhook-fast-server .` mostra a redução e é a base do porte.
- **Sem meta de vazão** até a primeira medição (§15.5).
