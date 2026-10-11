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
item 18).

**Não portado**: a porta estreita do dump do N (`shape.go`). Aqui a fila
recusa o que o N precisa recusar sozinho (CHECKs, `jsonb`), e a reingestão já
refaz item a item um lote recusado como `logic`.

## Riscos

- **Divergência com o fast.** Dois módulos sem código compartilhado: uma correção
  num pacote comum (`envelope`, `clientip`, `server`, `sink/postgres.go`,
  `sink/workers.go`) precisa ser portada à mão. `diff -ruN -x .DS_Store
  ../go-webhook-fast-server .` mostra a redução e é a base do porte.
- **Sem meta de vazão** até a primeira medição (§15.5).
