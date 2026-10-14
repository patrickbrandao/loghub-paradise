# go-webhook-nats-server — Decisões de projeto

Registro de como o `go-webhook-nats-server` nasceu, em 2026-10-10. A fonte da
verdade é `docs/SPEC.md`; cada decisão abaixo aparece lá com o número da seção.

## Pedido

> Crie o serviço "go-webhook-nats-server" usando o "go-webhook-pg-server" como
> base, troque o postgres pelo NATS como foi feito no serviço
> "go-syslog-nats-server". O que não puder ser aprendido com esses projetos,
> investigue na base de código e tome a decisão; somente o que não puder ser
> descoberto deve ser discutido comigo em entrevista.

**Não houve entrevista.** Cada ponto em aberto tinha resposta numa decisão já
tomada pelo usuário ou num padrão estabelecido por um dos módulos de
referência. As fontes, em ordem de precedência:

1. o pedido ("como foi feito no `go-syslog-nats-server`") para tudo o que é do
   destino NATS;
2. as decisões do usuário no `go-webhook-fast-server` (`../go-webhook-fast-server/docs/DECISOES.md`),
   que já previam NATS `core` (decisão 8) e o heartbeat beacon com `sink=nats`
   (decisão 21) para a família webhook, sem nunca tê-los implementado;
3. as decisões do `go-syslog-nats-server` (`../go-syslog-nats-server/docs/DECISOES.md`),
   o molde da troca;
4. as decisões do `go-webhook-pg-server` (`../go-webhook-pg-server/docs/DECISOES.md`),
   a base do código e de tudo o que não é o destino.

Tudo abaixo é contestável: um "não" do usuário muda a SPEC primeiro.

## Estado do monorepo

| Pasta | Papel aqui |
|---|---|
| `go-webhook-nats-server/` | **Este módulo** (letra **N** no `CHANGELOG.md` da raiz) |
| `go-webhook-pg-server/` | A base: o código, a SPEC e as decisões fora do destino. **Nunca importado** |
| `go-syslog-nats-server/` | O molde da troca: o sink NATS com as correções da revisão dele, o beacon, a `LOGHUB_EB_URL`, "sem TLS". **Nunca importado** |
| `go-webhook-fast-server/` | Origem do P e do sink NATS da família webhook (`cover`, cabeçalhos tirados do `ParseHead`, chaves `[sink.nats]`). **Nunca importado** |

---

## 1. Herdado do `go-webhook-pg-server` (sem mudança)

| # de lá | Tema | Aqui |
|---|---|---|
| 1 | Corte: um ring, um destino; sem rate-limit e sem recarga por `SIGHUP` | **vale**: ring `memory`, destino `nats`. O `SIGHUP` continua capturado e ignorado com `WARN` (§13.3) |
| 2 | Emergência em JSON Lines | **vale**, com o formato idêntico: um dump do P é lido aqui sem conversão (§9) |
| 3 | *Drop-in* do fast: mesmas chaves, `WHS_*`, `whs_*`, seletores | **vale**, com os seletores adaptados (abaixo, §3) |
| 4 | Interfaces `Ring`/`Sink` com uma implementação cada | **vale** |

## 2. Herdado do `go-syslog-nats-server` (a troca do destino)

| # de lá | Tema | Aqui | SPEC |
|---|---|---|---|
| 1 | Heartbeat beacon | **Fica** (também a decisão 21 do fast): `type=beacon`, level 9, em `loghub._ctl.beacon` (`LOGHUB_CTL_NAMESPACE`), por NATS **core**, na conexão do sink, na primeira conexão, em toda reconexão e a cada `LOGHUB_BEACON_INTERVAL` (15 s; `0` desliga só o periódico). Sem sonar, `ping` e `pong` | §8.7.10 |
| 2 | Sem TLS até o NATS | **Vale**: cliente sempre em texto claro; toda URL é `nats://`; o `TLSCertCB` que recusa impede a promoção automática do `nats.go`; servidor com `tls_required` é FATAL no boot (exit 3) | §8.7.1, §8.7.9 |
| 3 | `jetstream` e `core` | **Os dois** (também a decisão 8 do fast): `jetstream` padrão, `core` opt-in, *best effort*, `WARN` no boot | §8.7.2 |
| 4 | Subject derivado | **Vale**: sempre `loghub.<space>.webhook`, com o `space` do próprio envelope; não há subject configurável | §3.8 |

E o resto do sink do SN, com as correções da revisão independente dele:
veredito assíncrono de permissão negada (FATAL), `no_ack` e JetStream desligado
fatais no boot, mensagem recusada no cliente (`ErrMaxPayload`) recusada
sozinha, URL com senha parseada sem eco, *userinfo* na URL junto de
`username`/`token`/`credentials` recusado, teto de registro do *file store*
(32 MiB) no boot, stream que captura o plano de controle recusado.

A `LOGHUB_EB_URL` (SN §10.9, e um padrão assumido do fast, "`LOGHUB_EB_URL` é
aceito") vem junto: só `nats://` (lista com vírgula = cluster), `tls://` é
exit 1 nomeando o TLS, outro esquema é exit 1; com ela preenchida, uma chave
que a URL pode expressar definida explicitamente é exit 1 (§8.7.11).

## 3. Decisões tomadas na construção

### Destino

- **O sink é o do SN adaptado ao item do webhook**, mais duas peças do sink
  NATS do fast: os cabeçalhos (`Nats-Msg-Id` com o `step` do envelope,
  `X-LogHub-Type`, `X-LogHub-Space`) lidos do próprio JLH por
  `envelope.ParseHead`, e o `cover`: um subject de **outro** `space` (só um dump
  de outra vida produz) é conferido com `StreamNameBySubject` antes de qualquer
  publicação do lote, e um que nenhum stream cubra é FATAL nomeando o subject.
  No SN o mesmo caso esperava o `boot_timeout` (risco registrado lá).
- **Entrega parcial.** Do SN: uma recusa `logic` de uma mensagem (tamanho, no
  cliente ou no stream) não encerra o lote; as demais são publicadas e
  `Error.Delivered` diz quais o destino guardou. Os workers e a reingestão
  contam como perdidas só as recusadas. No fast e no P, uma recusa no meio do
  lote descartava o lote inteiro — inclusive mensagens nunca publicadas.
- **Contaminação.** Todo erro depois da primeira publicação contamina os itens
  do lote (`Taint`): um futuro pendente ainda aponta para o `Env` deles, e o
  buffer fica para o GC (§12.3).
- `WithPublishAsyncMaxPending` = `sink.workers × sink.batch` (no fast somava
  `ring.max_inflight`, que saiu com o `ring=none`).
- **`sink.workers` padrão 8** (o do fast para NATS, e o do SN): os workers
  dividem **uma** conexão; não há orçamento de conexões a proteger.
- **Nome do cliente** `whs/<AgentName>@<Hostname>` (o `application_name` do P),
  visível no `connz` do `nats-server`.
- **Stream `LOGHUB_WEBHOOK`** (padrão do fast), nunca criado pelo servidor;
  `docker/nats/stream.json` cobre só `loghub.default.webhook`.

### Compatibilidade de configuração (decisão 3 do P, adaptada)

- Seletores: `ring.mode = memory`, `sink.type = nats` (novo padrão),
  `ratelimit.enabled = false` e, de `[sink.nats]` do fast,
  **`tls = false`** e **`subject = loghub.{space}.{type}`** (o padrão do fast,
  que é a regra do JLH). Outro valor é exit 1 com a mensagem dizendo que o
  recurso não existe aqui.
- `sink.nats.tls_ca`, `sink.nats.tls_insecure`, `[sink.postgres]` e o resto do
  que saiu: chave desconhecida num INI ou numa flag (exit 1); no ambiente,
  inerte (a regra do P).
- Novas: `sink.nats.mode` (o nome da decisão 8 do fast) e a seção `[loghub]`
  (`eb_url`, `beacon_interval`, `ctl_namespace`), cujas variáveis de ambiente
  são os nomes fixados pela skill: `LOGHUB_EB_URL`, `LOGHUB_BEACON_INTERVAL`,
  `LOGHUB_CTL_NAMESPACE` (sem a forma `WHS_LOGHUB_*`).

### Emergência

- **A porta da reingestão fica mais estreita.** No P a fila recusava, pelas
  suas CHECKs e pelo `jsonb`, parte do que não fosse um JLH; o NATS não lê o
  `Data` e publicaria qualquer coisa, confirmada como legítima (lição do SN).
  Uma linha só volta se, além do que o P já exigia (cabeçalho estrito, UTF-8,
  JSON válido, só os escapes do serializador), tiver `type=webhook`,
  `level=9`, `step=0` e o payload começar com `{"mode":"fast",` — um JLH deste
  servidor. Um dump do `go-syslog-*` posto no diretório por engano é dano, não
  evento (§9.2).
- **Dump sob nome temporário e `link`** (correção da revisão do SN): escrito
  como `.emg-…jsonl.tmp`, `fsync`, `link` para o nome final, `fsync` do
  diretório. No P, um dump cortado numa fronteira de linha (um `kill -9` no meio
  da escrita) pareceria limpo e seria removido depois de entregue em parte.

### Beacon

- `payload.meta` com `version`, `ring` (`memory`), `sink` (`nats`), `mode`
  (`jetstream` ou `core`) e `transports` (`http1`, e `h2c` quando
  `server.http2` está ligado): as chaves do SN, com `mode` a mais, que é o que
  muda a garantia de entrega.
- `whs_beacons_total{result="ok|error"}`. Uma falha de beacon nunca toca a
  saúde do sink, e uma permissão negada no subject do beacon não é veredito.

### Nomes e Docker

- Module path `github.com/patrickbrandao/loghub-paradise/go-webhook-nats-server`;
  binário `cmd/go-webhook-nats-server`; `/app/go-webhook-nats-server` na imagem;
  tags `go-webhook-nats-server/vX.Y.Z`.
- Docker: prefixo `whn`; HTTP `8300`, admin `127.0.0.1:9301`, cliente NATS
  `127.0.0.1:14301`, monitor `127.0.0.1:18301` (o P usa 8200/9201/55432; o fast,
  8080–8100/9101–9130/4222–4224).
- `admin.bind` continua `127.0.0.1:9101`, o do fast e do P: não rode dois deles
  no mesmo host sem trocar o de um.
- Ferramentas NATS com versão fixada, nunca `latest`: `nats-server` `v2.14.6`
  na suíte e `natsio/nats-box:0.20.0` no `nats-init` (SPEC §15.3). Com
  `latest`, a mesma receita mudaria de ferramenta de um dia para o outro.

### Dependências

Três diretas em produção: `nats.go` (com as transitivas `nkeys`, `nuid`,
`klauspost/compress` e `golang.org/x/crypto`), `go-loghub-uuidv7` e
`go-loghub-ident`. O `pgx` saiu.

### Decididas na revisão independente (2026-10-10)

Três revisores somente-leitura, um por área (sink e workers; emergência e ciclo
de vida; config, docs e Docker), acharam o que segue; cada correção tem teste e
sobreviveu à mutação. Todas contestáveis.

- **O contrato do stream é conferido em toda checagem de saúde**, não só no
  boot, contra o `StreamInfo` que a checagem já busca: um stream editado por
  baixo (subjects estreitados, `no_ack` ligado) ficava `network` para sempre
  com o sink UP. Agora é FATAL, dump e exit 3 — o mesmo tratamento do boot. Um
  subject de outro `space` retirado também era FATAL; desde 2026-10-11 é
  esquecido (abaixo).
- **Em `core`, o veredito de permissão é lido no próprio `Write`**: o `-ERR`
  chega antes do `PONG` do *flush*, então o `LastError()` já o tem. Antes, o
  lote negado era confirmado, contado como escrito e perdido, e todos os
  seguintes até a próxima checagem. Todo `Write` devolve o veredito, nos dois
  modos.
- **`ring.Commit` antes do `Release` na recusa `logic`** (correção que o SN já
  tinha; o P e o fast a receberam no porte de 2026-10-11): na ordem inversa, em
  `core`, o ring contava os bytes do lote para sempre e acabava recusando tudo
  como cheio.
- **`max_bytes` com `discard: new` menor que uma mensagem** é recusado na
  validação (seria `10077`, fatal, a cada reingestão).
- **A porta do dump confere a forma do payload**: as chaves de topo na ordem do
  escritor, nenhuma chave repetida, inteiros curtos, sem literais, só os
  escapes do serializador (canônicos), aninhamento limitado, e o `uuid` com o
  instante do `event`. A forma de cada objeto aninhado ficou de fora: não
  envenena leitor nenhum, e conferi-la seria reescrever o schema em Go.
- **Sem *hard link*, o dump renomeia** depois de conferir o nome livre; o
  `fsync` do diretório virou melhor esforço (antes, um diretório que não abria
  apagava o dump pronto); só arquivos regulares são dumps; o temporário gêmeo de
  um dump é removido no boot e o de um dump interrompido é informado.
- **`shutdown.drain_timeout` é um prazo só para as fases 3 e 4** (herdado do P
  com um prazo por fase, o que dobrava o pior caso e passava do
  `stop_grace_period`).
- **Uma chave dona do `LOGHUB_EB_URL` definida vazia não compete com ela**: é o
  que o compose produz para `KEY: ${KEY:-}`, e sem isso o procedimento
  documentado do `LOGHUB_EB_URL` no Docker não funcionava. O compose agora
  repassa `LOGHUB_EB_URL`, `WHS_SINK_NATS_TOKEN` e `WHS_SINK_NATS_CREDENTIALS`.
- Segredos fora das mensagens: o log de conexão mostra só esquema e host (o
  `ConnectedUrlRedacted` do `nats.go` não esconde token); o erro de URL que não
  parseia não cita nem o pedaço que o `net/url` cita; o parser de INI e o de
  flags não ecoam a linha nem o valor. E as correções do SN que não tinham
  vindo: `NaN` recusado em `high_water` e `access_sample`, `admin.bind` vazio
  recusado, `quoteINI` no dump, `sink.nats.urls` sem caminho, consulta ou
  fragmento.

### Porte de volta das correções dos irmãos (2026-10-11)

A rodada que portou as correções do N para o P, o fast, o W, o SN e o SP teve
a sua própria revisão independente, que achou defeitos que o N também tinha e
que foram corrigidos só nos irmãos (entrada "2026-10-11 — P, F, W, SN, SP" do
`CHANGELOG.md` da raiz). Vieram para cá com o P, o fast e o SN como referência,
cada um com SPEC, teste e mutação. Todos contestáveis.

- **O ring é selado antes do último `Drain` do shutdown** (`Seal`, barreira por
  `RWMutex` contra o `Enqueue`; SPEC §10.3): um upload que terminava depois do
  `Drain` recebia `200` e se perdia sem contagem.
- **"Sem respondedores" na primeira checagem é FATAL sem a sonda
  `AccountInfo`** (SPEC §8.7.5): a sonda não distinguia nada (o `jetstream` já
  traduz "sem respondedores" para "não habilitado") e o pedido a
  `$JS.API.INFO`, negado a um papel mínimo, virava um veredito de permissão
  permanente que nomeava a causa errada. Depois de uma checagem validada, o
  mesmo é `network`.
- **Um subject de outro `space` que o stream deixou de cobrir é esquecido, não
  FATAL** (SPEC §8.7.5). A tarefa admitia manter o FATAL com justificativa;
  não houve uma: aqui, como no fast, só a reingestão de um dump de outro
  Workspace publica nesses subjects, e ela termina antes de o ring abrir. Depois
  dela, o próprio erro do `cover` manda o operador cobrir o subject "até o dump
  drenar" — e estreitar o stream em seguida derrubava um processo saudável
  (FATAL, dump do ring, exit 3) por um subject que ninguém mais publicaria. A
  próxima publicação nele, se houver, pergunta de novo e é recusada nomeando-o.
- **Parser de flags** (SPEC §13.2): uma flag sem valor não engole a seguinte, e
  um valor depois de um espaço passado o `=` nunca é citado. A revisão deste
  porte achou mais três caminhos que imprimiam o segredo (também no P, no fast
  e no SN, a portar): agora só se nomeia uma flag que este servidor conhece, e
  nada depois de um valor é citado. Fica, decidida, a brecha da cauda com traço
  depois de um valor não vazio (`--x=a -b`): escondê-la esconderia os erros de
  digitação (TESTING §8 item 20). Fechada em 2026-10-13 (abaixo).
- **O teste do `Seal` age dentro do `Drain`** (`App.wrapRing`, nulo no
  binário), não depois dele: um gancho depois do `Drain` não distinguia um
  `Seal` antes do `Drain` de um entre os dois (achado da revisão; o teste do P
  tem o mesmo ponto cego).
- **O `WARN` do dump interrompido** (SPEC §9.1) diz que os eventos só foram
  contados como perda num aborto por segundo sinal, e a SPEC diz como
  reingerir o temporário (renomeá-lo para o nome final).
- Estendido pela mesma forma: o `WARN` "drain deadline reached" leva o
  `drain_timeout` configurado (era o pedaço restante do prazo da fase 4, como
  em nenhum irmão), e os testes que o P ganhou na revisão — o `WARN` do dump
  interrompido e os *gauges* de emergência somados sem reler o diretório —
  vieram também.

---

## 3a. Porte das correções da família syslog/RELP (2026-10-13)

Uma subseção por área da rodada; o que veio igual dos irmãos está na SPEC.

### Ciclo de vida e dump

Padrões assumidos nesta rodada (contestáveis); o que veio igual do S, do SN e
do RN está na SPEC (§9, §9.2, §10.3, §10.5), na mesma forma do fast:

- **O `fsync` do diretório decide a remoção e a quarentena, não o dump.** O SN
  recusa o dump cujo diretório não sincroniza; aqui ele continua melhor esforço
  (§9.1, decisão do porte de 2026-10-11). Na remoção e na quarentena a falha
  vem depois do `unlink`/`rename` e vira `WARN` (`emergency.ErrNotDurable`).
- **Com `emergency.enabled = false` a perda continua sendo `abort`** (§9.3),
  não `emergency_failed` como no SN; o envelope deixado fora do dump pela
  defesa do enquadramento passa a ser contado pelo próprio dump.
- **A leitura dos dumps em todo boot** (a regra do fast, pendência registrada
  na SPEC) entrou nesta rodada por ser do mesmo caminho: com a chave
  desligada, um `emergency.dir` que não se lista é `ERROR` e o boot segue.
- **Não se aplica o `quitNow` do SN:** `SIGQUIT` fora do shutdown é a ação
  padrão do Go (§10.3); o canal do aborto, que inclui `SIGQUIT`, passa a ser
  registrado antes de o contexto soltar `SIGTERM`/`SIGINT`.
- **Não se aplica o prazo da reingestão "sem progresso"** do SN: a reingestão
  daqui não espera nem retenta — cada lote tem o seu `sink.op_timeout` e uma
  recusa de rede ou fatal sai com 3 (§9.2). A leitura é do arquivo inteiro, não
  em fluxo, então não há "pedaço" para o *gauge* mostrar: ele é do arquivo, e
  recontado do disco no fim.
- **Fechamento em paralelo:** já era (`Server.Shutdown`).

### Configuração

As regras de configuração do `go-relplog-server` e dos cortes dele, já no
`go-syslog-nats-server` (SPEC §13.2, tabela de §10.1, §8.7.11). Padrões
assumidos, contestáveis:

- **Tetos:** `ring.capacity_items` 16 Mi; `sink.batch` 1 Mi, o da família
  webhook (o SN tem 65 536; com `workers` ≤ 256, `workers × batch` ainda cabe
  num `int` de 32 bits); `sink.workers` **256**, o do SN (todo worker publica
  na uma conexão NATS); `server.listeners` 256.
- **As seções do fast que este servidor não tem contam como conhecidas**
  (`retiredSections`) para a regra da cauda, como no P.
- **`LOGHUB_EB_URL` só com brancos é exit 1**, não mais "não definida" (a
  regra do SN e do RP); vazia continua "não definida". **`#` ou `?` vazio**
  numa URL NATS também é recusado: a URL não tem nenhum dos dois. As mensagens
  do `@` depois de `/`, `?` ou `#` e do host codificado são as do SN, no
  `parseURL` — que também serve o `sink.nats.urls`.
- **Já estava feito:** fragmento, caminho e consulta recusados; chave própria
  vazia sem conflito. **Não se aplica:** `sink.batch_max_bytes` (*revisto em
  2026-10-14: a chave existe agora, por decisão do usuário; ver §3b*), `pool_size`,
  `ring.max_inflight` (não existem) e as regras de URL PostgreSQL.

### Sink NATS

O sink do `go-syslog-nats-server` depois do porte de 2026-10-13
(e o texto "store is closed" da revisão do `go-syslog-server`), conferido aqui
item a item. Padrões assumidos, contestáveis:

- **`retention: interest` é recusado** (fatal na validação de toda checagem),
  como decidiu o `go-syslog-server` em 2026-10-11 e o SN adotou; SPEC §18.1
  item 7 deixava a escolha (recusar, avisar ou conferir os consumidores) em
  aberto. O `PubAck` é o que deixa o ring esquecer uma mensagem, e um stream
  que descarta no aceite o esvazia. `max_msgs_per_subject > 0` só dá `WARN`.
- **A regra do *pool* do `go-syslog-server` (C1) não vem para cá.** Lá, com
  TLS, a recusa de TLS de um nó de um *pool* virou `network` (outro nó pode
  servir, e um certificado mal posto num nó é erro de operação passageiro).
  Aqui a SPEC §8.7.6 já decidia o contrário, e fica: um nó que **exige** TLS é
  um cluster migrando para TLS, que deixou de ser destino deste servidor, e o
  primeiro nó assim que a reconexão sortear é o veredito — agora lido do
  `LastError`, como no SN e no RN. A contestar junto com o SN e o RN, que têm a
  mesma regra.

## 3b. Porte das correções de W e F (2026-10-14)

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
  veio por simetria com o F e o SN, para o laço dos workers continuar igual ao
  deles.
- **`sink.batch_max_bytes` no NATS:** cada mensagem já é uma publicação, e o
  pedaço é o que fica em voo de uma vez por worker; 256 envelopes de 5 MiB
  eram 1,25 GiB pendentes de `PubAck` no mesmo `sink.op_timeout`. O teto de
  256 MiB é o da família; aqui nada o liga ao 1 GiB de um parâmetro do
  PostgreSQL.
- **A porta do dump recusa U+2028/U+2029 crus** (SPEC §9.2; §18.1 item 10).
  Aqui era defeito: a chave repetida é conferida por bytes, e a mesma chave em
  duas grafias passava. O leitor de string de `shape.go` recusa os bytes
  `E2 80 A8`/`E2 80 A9`.
- **Docker:** o volume `./certs:/etc/webhook:ro` comentado no compose, para o
  `.creds` que o `.env.example` sugere; o serviço é `read_only` e só tinha
  `/data` (achado da triagem).
- **Também portados:** a checagem rápida do passo 9 por bytes (W 2), o dreno
  de recusa só em HTTP/1.x sem `Expect` (W 3; este servidor também fala h2c),
  `write_timeout` contado da resposta (W 4), o teste do compose (W 27/28) e os
  guardas de regressão em `t.Fatalf` (W 29, §18.1 item 11). W 24, 25 e 26 já
  estavam aqui (o P os recebeu daqui); de W 24 veio a recusa de `NaN`/`±Inf`
  também no parser, como defesa dupla, e o `pending_config_test.go` é o mesmo
  do P. Sem decisão nova.

## 3c. Revisão independente da família webhook (2026-10-14)

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
- **`LOGHUB_EB_URL` com `\n`**: passa a ser recusado pela regra do byte de
  controle (W 41), antes da de "só brancos"; o teste mudou de mensagem, não de
  resultado (exit 1).

## 4. Riscos

- **Sem TLS até o NATS**: credenciais e eventos em claro; o NATS precisa estar
  numa rede confiável. A porta pública continua sem TLS também (proxy reverso).
- **`core` é *best effort***: sem stream, uma mensagem sem assinante some, e
  nada deduplica.
- **A durabilidade depois do `PubAck` é a do stream** (`storage`, réplicas,
  `sync_interval`), de quem administra o NATS.
- **Dump de outro Workspace**: o boot sai com 3 nomeando o subject, a cada
  restart, até o operador fazer um stream cobri-lo ou mover o arquivo.
- **Divergência com o fast e o P**: três módulos sem código compartilhado. As
  listas de porte nos dois sentidos foram cumpridas em 2026-10-11 (entradas
  **N** e "2026-10-11 — P, F, W, SN, SP" do `CHANGELOG.md` da raiz); a porta
  estreita do dump (`shape.go`) continua só daqui, porque no P a fila recusa
  parte do que ela recusa. Uma correção nova num pacote de mesma origem
  continua sendo portada à mão.
- **`kill -9` perde o ring**, como no P.
- **Cluster NATS migrando para TLS**: o primeiro nó com TLS que uma reconexão
  sortear encerra o processo, mesmo que outro nó ainda aceitasse texto claro.
