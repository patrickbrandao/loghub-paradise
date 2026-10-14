# go-relplog-server — Decisões de projeto

Registro das decisões que transformaram a especificação extraída do RELP
(`docs/SPEC.md` da versão de 2026-10-05, preservada no histórico do git) num
software LogHub completo, em 2026-10-11. Este arquivo é a **entrada** da
`docs/SPEC.md`; cada decisão aparece lá com o número da seção. "A extraída" e
os `§` que a acompanham (`§10.3 da extraída`) são dessa versão antiga, no
commit `4b667d5`; os `§` sem qualificação são da `docs/SPEC.md` atual.

O pedido do usuário (2026-10-11): "faça o desenvolvimento completo do
`go-relplog-server` (protocolo RELP) usando as especificações iniciais; detalhes
de um sistema semelhante estão em `go-syslog-server`; proceda automaticamente e
faça tudo até concluir; em caso de dúvida consulte os demais projetos". Não houve
entrevista: pela regra do usuário de 2026-10-10, o que se aprende do código e dos
projetos irmãos é decidido aqui, e uma decisão anterior do usuário num
`DECISOES.md` irmão conta como descoberta. As decisões abaixo são **contestáveis**:
estão escritas para o usuário poder trocá-las sabendo o custo.

## Fontes

| Fonte | Papel |
|---|---|
| `go-relplog-server/docs/SPEC.md` de 2026-10-05 | RELP extraído da standard: moldura, diálogo, janela, backpressure, shutdown, bugs; §10 com as quatro perguntas em aberto |
| `go-syslog-server/` (S) de 2026-10-11 | O irmão LogHub: conversão `type=syslog`, rings, sinks, beacon, dump, config, admin, Docker. **Pai do código** |
| `go-syslog-standard-server/internal/receiver/relp*.go` | Código RELP de referência, provado contra o `rsyslog` em 2026-09-13/14 |
| `go-syslog-nats-server/` (SN), `go-syslog-pg-server/` (SP), `go-webhook-nats-server/` (N) | Correções que nunca chegaram ao S (S §22.1) |
| `librelp` (`rsyslog/librelp`, `src/scrsp.c`, `cserverclose.c`, `copen.c`, `scsyslog.c`, `relpframe.c`, `relpsess.c`) e `rsyslog/plugins/omrelp/omrelp.c`, lidos em 2026-10-11 | O comportamento real do cliente, que decide o que cada resposta significa |
| skills `loghub-json-schema`, `loghub-type-syslog-schema` (sem `relp` no enum de `transport` em 2026-10-11), `loghub-postgres-table-inbox`, `go-loghub-uuidv7`, `go-loghub-ident` | Fontes normativas, as mesmas do S |

---

## 1. Decisões

| # | Tema | Decisão | Por quê |
|---|---|---|---|
| 1 | Forma do código (§10.3 da extraída) | **Módulo independente** que copia o S com adaptação; nada importado de outro módulo | É a regra do monorepo (AGENTS do S §3), e reverter a decisão de 2026-10-05 ("o RELP sai do syslog") seria contrariar o usuário |
| 2 | Representação no payload (§10.1) | `transport = "tcp"` ou `"tls"`, `framing = "octet-counting"` e a extensão **`"x-transport": "relp"`** em todo envelope | Passa nos dois schemas sem mudar a skill (que não tem `relp` em 2026-10-11); `DATALEN` é contagem de octetos; o `sid` continua recalculável pela fórmula da skill. Um `relp` futuro no enum é uma mudança de SPEC, não de desenho |
| 3 | Duplicata após confirmação perdida (§10.2) | **At-least-once**, sem chave de deduplicação | O RELP não carrega id de mensagem; uma chave derivada do conteúdo fundiria mensagens legítimas idênticas (um `link down` repetido). O shutdown gracioso esvazia as confirmações antes de fechar (§5.2 da SPEC); só o corte abrupto duplica |
| 4 | **O que um `500` significa** | `500` é **recusa final**: o `librelp` remove a mensagem da lista de não confirmadas e não a retransmite (`scrsp.c`: `relpSendbufDestruct` no ramo `rspCode != 200`, com o `onErr` "peer sent error response" que o rsyslog loga). Logo só recebe `500` o que este servidor descartou de propósito, para sempre e raramente: recusa `logic` final do destino (`ring=none`), a guarda de origem não endereçável, comando desconhecido, `syslog` antes do `open`, segundo `open` | A especificação extraída dizia "`500 ring full`: o rsyslog retém e retenta". O código do cliente diz o contrário: era perda na origem a cada ring cheio. O rsyslog real confirmou (a mensagem recusada nunca volta) e mostrou um custo a mais: o `500` suspende a ação e quebra a sessão, e as vizinhas da janela já aceitas voltam como duplicatas (5–6 por `500`) |
| 5 | Ring cheio, ring fora do ar, sink DOWN | **Segurar** a transação (sondando a cada 50 ms) até `relp.backpressure_timeout` (30 s); estourado, **fechar a conexão sem responder**. A sessão quebrada faz o `librelp` reconectar e retransmitir tudo o que não foi confirmado (`relpSessTryReestablish`) | É a única forma de devolver a mensagem ao emissor. 30 s fica abaixo do `timeout` padrão do `omrelp` (90 s), então o servidor decide antes de o cliente desistir |
| 6 | Shutdown | O leitor para; as transações já decididas recebem a resposta; as que ainda esperam vaga **não** recebem nada; depois das confirmações sai `0 serverclose 0` e a conexão fecha | Um `500 shutdown` (o da especificação extraída) descartaria a mensagem no emissor. O `serverclose` era item "a decidir pelo usuário" (CHANGELOG 2026-10-11): ele marca a sessão quebrada no cliente (`cserverclose.c`), a mesma recuperação de um fechamento, mas explícita e logada pelo rsyslog como fechamento do servidor |
| 7 | `close` do cliente | Espera as transações em voo serem decididas e responde `rsp` vazio; a sessão termina **sem** `serverclose`, como o servidor do `librelp` (`cclose.c`) | No código de referência o `serverclose` podia sair antes de confirmações ainda em voo |
| 8 | Filtrada, vazia | `200 OK` (contadas como `filter` e `empty`) | O operador pediu o descarte; um `500` mentiria que houve erro, o rsyslog logaria um erro por mensagem e quebraria a sessão a cada uma |
| 8a | **Rate-limit** | **Segurar** a transação até o balde ter uma ficha (como um ring cheio): o emissor é desacelerado para o limite, nada é recusado nem descartado; `relplog_ratelimit_held_total` conta | Decidido depois do teste com o rsyslog real (2026-10-11). A primeira versão respondia `500 rate limited` (o S descarta no TCP); com o `500` quebrando a sessão, cada mensagem acima do limite custaria uma reconexão e duplicatas — numa rajada, uma tempestade de reconexões. No RELP o emissor guarda o que o servidor não pode receber ainda, que é o que um limite deveria produzir |
| 9 | Janela e cópias (§10.4) | `relp.window` = 128 transações por conexão; teto **global** `relp.inflight_max_bytes` = 64 MiB para as cópias das transações em voo, ≥ `syslog.max_message_bytes`; a cópia é feita **depois** da vaga na janela e no teto; esgotado, o leitor para de ler | Exigência de §4 da extraída; o nome diz o que conta (bytes em voo do processo inteiro) |
| 10 | Tamanho | `DATALEN` acima de `relp.max_frame_bytes` (16 MiB) fecha a conexão antes de ler o corpo; acima de `syslog.max_message_bytes` (64 KiB) **trunca** como o S (aviso `truncated`, `size` original), e o excedente é consumido sem alocar. `relp.max_frame_bytes` ≥ `syslog.max_message_bytes` | Mesma regra de tamanho do S (decisão 13 dele) e o fusível da extraída. Com o truncamento a cópia de uma transação nunca passa de 64 KiB |
| 11 | Custo fixo por conexão | `bufio.Reader` de 4 KiB e buffer de frame que começa em 2 KiB e dobra conforme os bytes chegam | Fecha §8.3 item 3 da extraída e a pendência 1 do S |
| 12 | Transportes | `listen.tcp` (`:20514`, RELP em claro) e `listen.tls` (desligado; `:20515` por costume), RELP sobre TLS como o `omrelp` com `tls="on"`, com o `[tls]` do S (mTLS opcional). PROXY protocol v1/v2 como no S. Fora: UDP, unix, RFC 6587 | O RELP só existe sobre fluxo; o TLS do S custa pouco e o `librelp` o suporta |
| 13 | Ordem dos comandos | `syslog` antes do `open` recebe `500 command disabled` e a conexão continua; um segundo `open` recebe `500 protocol error: connection already open` e a conexão fecha | É o que o servidor do `librelp` faz (`scsyslog.c`, `copen.c`) |
| 14 | Recursos | Tudo o que o S tem, igual: rings `memory`/`sqlite`/`redis`/`none`, os sete sinks, beacon, dump de emergência, filtro, rate-limit, admin, `SIGHUP`, `LOGHUB_EB_URL`, registry único | "Desenvolvimento completo" com o S como modelo |
| 15 | Nomes | Binário `go-relplog-server`; env `RELPLOG_<SEÇÃO>_<CHAVE>` e `RELPLOG_CONFIG`; as chaves de `[syslog]` mantêm `SYSLOG_<CHAVE>` (a mesma conversão do S, e um `.env` comum garante config de conversão idêntica); `LOGHUB_*` iguais; métricas `relplog_`; admin `127.0.0.1:9103`; ring Redis `relplog:ring`/`relplog-ring`; stream NATS `LOGHUB_SYSLOG` e subject `loghub.{space}.syslog` **compartilhados** com o S (mesmo `type`); nome de cliente `rls/`; letra **R** no CHANGELOG | Dois servidores lado a lado não disputam variáveis, portas, métricas nem o ring Redis; o dado é o mesmo e vai ao mesmo lugar |
| 16 | Pendências do S (§22.1 dele) | Corrigidas aqui, portando de SN, SP e N onde já foram corrigidas. As quatro "a decidir pelo usuário" foram decididas: teto por IP conta o endereço exato, documentado (emissores RELP são servidores, com endereço fixo); offset ISO fora de faixa na RFC 3164 segue a skill (sem desvio novo); `retention: interest` é **recusado** (fatal), porque confirma e perde; reingestão com `sink=none` **mantém** os arquivos com `WARN` | Copiar 30 defeitos conhecidos não é "concluir com perfeição" |
| 17 | Sonda do `sink=sqlite` com a escritora ocupada (2026-10-14) | A sonda espera a trava da escritora **só dentro do próprio prazo** (`sink.health_timeout`). Não obtida no prazo e o último `Write` terminou sem erro: **UP sem tocar o banco**; o último `Write` falhou (qualquer erro) ou nunca houve `Write`: DOWN, como antes. SPEC §10.4. Revisão independente de 2026-10-14 (A1, A2): a escritora ocupada só conta UP enquanto o pedaço em curso segura a trava há no máximo `sink.op_timeout + sink.health_timeout` (um `fsync` preso no `COMMIT`, que não recebe contexto, deixava o sink em UP para sempre), e a falha do próprio heartbeat conta como `Write` falho (sem isso a sonda alternava DOWN/UP e zerava os fusíveis). | Decisão do usuário, a mesma do item 23 do `go-webhook-server`: um pedaço mais longo que o prazo da sonda dava DOWN (transações seguradas) com o banco íntegro |

---

## 2. Padrões assumidos

- Portas: `:20514` é o costume do RELP nos exemplos do rsyslog e porta alta (a
  imagem roda como uid 65532); `:20515` para o TLS é só uma sugestão de
  documentação, o padrão é desligado (exige certificado).
- `relp_software=go-relplog-server,<versão>` na resposta ao `open`, para o
  operador lendo o log do rsyslog saber que build está do outro lado.
- Métricas novas: `relplog_relp_frames_total{command}`,
  `relplog_relp_responses_total{code}`, `relplog_relp_unanswered_total{reason}`
  (transações lidas e devolvidas ao emissor sem resposta: `backpressure`,
  `shutdown`, `conn_lost`), `relplog_relp_inflight_bytes`,
  `relplog_relp_inflight_waits_total`. Elas fecham a reconciliação de §8.3
  item 1 da extraída: enviadas = confirmadas + recusadas + devolvidas.
- Uma transação devolvida **não** é descarte: o emissor a retransmite. Por isso
  não entra em `relplog_messages_dropped_total`.

### 2.1 Porte das correções dos cortes e do S (2026-10-13)

Pedido: trazer para cá as correções que o RP, o RN e o S receberam depois deste
módulo (lista "A portar" da entrada RP, RN do `CHANGELOG.md` da raiz) e os
achados da revisão independente do porte do S que valem aqui. As escolhas que o
plano daquela revisão deixou como "recomendadas" foram autorizadas pelo
usuário; as novas, minhas, estão abaixo como **padrões assumidos**, contestáveis.

| # | Tema | Decisão | Por quê |
|---|---|---|---|
| P1 | **Sonda de saúde SQL** | A checagem de saúde de PostgreSQL, MySQL e ClickHouse tem **conexão própria, fora do pool** (`pgx.Conn`; `sql.DB` com uma conexão; um cliente ClickHouse de uma conexão). O pool é dos escritores: `pool_size = 0` deriva **`sink.workers`** (era `workers + 1`), e abaixo dos workers continua exit 1. **Não** porto o `pool_size ≥ workers + 1` do RP | O "+ 1" do RP existia só para a sonda; com `ring=none` os escritores são até `ring.max_inflight` e nenhum "+ 1" a protegia (revisão do S, D1). Com a sonda fora, `pool = workers` é a configuração exata, e recusá-la seria recusar o certo. O total de conexões continua `workers + 1`, como antes. Custo: uma conexão a mais quando o operador fixou o pool |
| P2 | Memória do ClickHouse | A conta de `max_memory_usage_for_user` multiplica o pior pedaço pelos **escritores** (`Config.SinkWriters`: `sink.workers` com ring; `min(pool_size, ring.max_inflight)` com `ring=none`), passados pela app | A sonda não faz `INSERT`; e a mensagem antiga mandava "baixar o pool", que a config recusa abaixo dos workers |
| P3 | TLS de um pool NATS | A recusa do TLS só é veredito com **um** servidor no pool do cliente (`nc.Servers()`, descobertos inclusive); com mais, `network` com o motivo no erro, e os fusíveis dão o FATAL; no boot, mais de uma URL também é `network` | Opção recomendada do plano (C1). O `LastError` é do último servidor tentado |
| P4 | `ring=sqlite` + `sink=none` | O backlog de uma vida anterior fica no `ring.db`, fora da contabilidade (`KeepBacklog`, como o F), com `WARN`; `ring=redis` + `sink=none` é `WARN` na validação | Opção recomendada (A3); a regra é a do dump com `sink=none` (decisão 16) |
| P5 | Tetos | `ring.capacity_items` ≤ 16 Mi só no ring de memória efetivo (degradado inclusive), ≤ 2^40 nos persistentes; `sink.batch` ≤ 1 Mi; `ring.max_inflight` ≤ 1 Mi; `sink.workers` ≤ 1024; `pool_size` ≤ 4096; `batch_max_bytes` ≤ 256 MiB; `commit_batch` ≤ 65536; listeners ≤ 256. O `MaxPending` do NATS é calculado em `int64` e limitado a `MaxInt32` | Os valores do S (mesmos rings e sinks); o limite do `MaxPending` garante um `int` em qualquer plataforma, mesmo nos tetos |
| P6 | URL PostgreSQL | Usuário e banco obrigatórios (decisão do RP); `?host=` vazio, `sslmode=` vazio e TLS num socket (chaves ou URL) são exit 1 | Uma URL, um destino, em toda leitura (libpq e este servidor) |
| P7 | Pré-checagem do ring cheio | Só o ring de **memória** tem `FullFor` (os dois tetos); `sqlite` e `redis` seguem sem pré-checagem | O R não tinha pré-checagem nenhuma; nos persistentes ela seria uma consulta, que custa mais que a cópia que economiza |
| P8 | Frame ruim com transações em voo | O contexto da sessão é conferido logo antes do `Enqueue`: num frame ruim, só recebe `200` o que já estava no ring; o resto fica sem resposta e o `librelp` retransmite | É o "fim da sessão vence" de §5.7 levado até o `Enqueue` (RN). `TestRELPMalformedFrameClosesAfterTheAnswers` passou a esperar o ring, como no RN |
| P9 | Testes de `autocommit` do MySQL | Sem `SET GLOBAL autocommit`: um usuário próprio do teste e um `init_connect` **condicional** a ele (`IF(USER() LIKE 'ac0\_%', 0, @@autocommit)`), com controle de que a sessão desse usuário nasce com `autocommit=0` | Opção recomendada (D5). O `init_connect` ainda é global, mas não muda a sessão de nenhum outro usuário; usuários com `CONNECTION_ADMIN` (o `root` das outras suítes) nem o executam |
| P10 | Não portado | O **E1** da revisão do S (linhas em buffer no `Close` com `proxy.protocol=optional`): no RELP o que não teve `200` o `librelp` retransmite. Nada que contradiga §5.4 | Pedido explícito; a semântica RELP decide |

### 2.2 Porte das pendências do RP/RN (2026-10-14)

O usuário aprovou portar e listar o que a triagem do RP/RN achou igual aqui
(`tasks/port-to-R-S-SP-SN.md` da raiz; registro em `tasks/progress-R-port.md`).
Corrigidas com teste e mutação: o aborto fecha o ring antes de contar (§13.4),
o piso de `ring.max_bytes` (§13.2, §15.1), `admin.bind` contra `listen.tcp` e
`listen.tls` (§13.2, §14), o rótulo do idle com transação segurada e o `WARN`
(§5.4, §13.2), a derivada fora do relato do `SIGHUP` (§16.3) e, achado do porte
(o mesmo do S), o `network` cortado pelo cancelamento do boot no
`Instrumented.Deliver` (§14.1). As escolhas abaixo são padrões assumidos,
contestáveis; P11–P13 são as mesmas do `go-syslog-server` (DECISOES dele,
"Porte das pendências do RP/RN"), para R e S ficarem iguais onde o código é o
mesmo.

| # | Tema | Decisão | Por quê |
|---|---|---|---|
| P11 | Piso de `ring.max_bytes` por modo | Na medida do ring **efetivo**: `memory` (inclusive o `redis` degradado, §9.7) = classe do pool + 384 B (65 920 B com o padrão); `sqlite`/`redis` = o próprio `syslog.max_message_bytes` (eles contam `len(cru)`); `none` = só positivo (a chave não é usada) | A receita aceitava manter `< 1` fora do `memory`; derivar da medida de cada modo fecha o mesmo defeito neles (uma mensagem que nunca cabe num ring vazio) |
| P12 | Aborto e rings persistentes | O `abortLoss` fecha o ring antes de contar só com `memory`/`none`; `sqlite`/`redis` não são fechados ali | O `Close` deles esvazia o escritor e espera disco ou rede, o contrário do "agora" do aborto; e eles não contam perda |
| P13 | `tls.idle_timeout` | O `WARN` do idle abaixo de `relp.backpressure_timeout` vale para cada transporte que escuta: `tcp.idle_timeout` com `listen.tcp`, `tls.idle_timeout` com `listen.tls` | O R tem um idle por transporte; avisar sobre o idle de um transporte desligado seria ruído |
| P14 | Regras de texto (sem código) | Falha ao remover um dump entregue é só `ERROR` (§12.2); SQLSTATE fora da tabela (`P0001`) é `network` (§10.5, §20.18); o dump publicado no instante do aborto fica contado e no disco, e o aborto não espera o disco (§13.4); `kind="fatal"` conta mesmo com o cancelamento (§14.1); contadores da recepção sem *padding* são lacuna de medição (TESTING §10) | As decisões do RP/RN (RN DECISOES nº 15), as mesmas no S |

## 3. Riscos

- **Comportamento do cliente.** As decisões 4–6 dependem do `librelp`. Outro
  cliente RELP que trate `500` como "retente" perderia menos com a regra antiga;
  nenhum é conhecido. A suíte de emissores vivos prova as três contra o rsyslog
  real.
- **Duplicatas.** Fechar a conexão por backpressure faz o rsyslog retransmitir
  também o que já foi aceito e cuja confirmação não saiu a tempo; o servidor
  esvazia as confirmações decididas antes de fechar, então só o que for decidido
  depois do fechamento duplica. Um `500` (só recusa `logic` com `ring=none`)
  quebra a sessão do lado do cliente e duplica as vizinhas da janela.
- **Ordem.** As transações de uma janela entram no ring em paralelo; a ordem
  entre elas segue o `uuid` (a amostra de relógio é tirada pelo leitor, em
  ordem), não a ordem de chegada ao ring.
- Os riscos do S valem aqui (`go-syslog-server/docs/DECISOES.md` §4).

## 4. Ordem de entrega

1. Este arquivo; `docs/SPEC.md` autocontida (a do S adaptada, com o RELP da
   extraída corrigido); `AGENTS.md`, `CLAUDE.md`, `README.md`, `docs/TESTING.md`.
2. Código: recepção RELP (moldura, sessão, janela, teto em voo, escritor,
   shutdown), config, app, métricas, Docker; pendências do S.
3. Testes com `-race`, destinos reais e rsyslog real.
4. Verificação independente por área; correções.
5. Entrada **R** no `CHANGELOG.md` da raiz.
6. Commit único, só com o "sim" do usuário.
