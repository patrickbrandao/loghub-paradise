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
