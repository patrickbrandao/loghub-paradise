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
  digitação (TESTING §8 item 20).
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
