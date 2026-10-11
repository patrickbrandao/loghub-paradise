# Deploy em Docker

Build e execução do `go-webhook-nats-server` em contêiner. Há **uma** combinação:
ring na RAM, publicação num stream JetStream do NATS (`docs/SPEC.md` §0). O
compose sobe o servidor, um `nats-server` com JetStream e um job (`nats-init`)
que cria o stream uma vez.

```bash
docker compose up -d --build
# o mesmo, pelo nome do arquivo
docker compose -f compose.memory-nats.yml up -d --build
```

Os arquivos têm a forma dos do `go-webhook-fast-server` e do
`go-webhook-pg-server` (um `common.yml` herdado por `extends`), com nomes
`whn-*` e portas próprias, para os três rodarem lado a lado no mesmo host. As
variáveis (`WHS_*`) e as métricas (`whs_*`) são as do fast: um `.env` ou um
painel de lá serve aqui (`docs/SPEC.md` §0). As três chaves do plano de controle
usam os nomes da convenção LogHub, sem prefixo: `LOGHUB_EB_URL`,
`LOGHUB_BEACON_INTERVAL` e `LOGHUB_CTL_NAMESPACE`.

---

## Portas

| serviço | porta do host | sobrescreva com |
|---|---|---|
| HTTP público | `8300` | `WHS_HTTP_PORT` |
| admin (`/healthz`, `/readyz`, `/metrics`) | `127.0.0.1:9301` | `WHS_ADMIN_PORT` |
| cliente NATS | `127.0.0.1:14301` | `WHS_NATS_PORT` |
| monitor do NATS (`/connz`, `/jsz`) | `127.0.0.1:18301` | `WHS_NATS_MONITOR_PORT` |

A porta admin é publicada **somente em `127.0.0.1` do host**. Dentro do
contêiner ela escuta em `0.0.0.0` — não há como publicar de outra forma — e por
isso o servidor emite este warning no boot, que é esperado neste contexto:

```
admin.bind="0.0.0.0:9101" is not loopback: /metrics and /readyz are reachable off-host
```

---

## Memória

O servidor **não define `GOMEMLIMIT` sozinho** e avisa no boot se ele faltar
(`docs/SPEC.md` §12.4). Sem ele o coletor de lixo do Go não sabe do teto do
contêiner e o OOM killer chega antes do GC — levando o ring junto, porque o dump
de emergência não roda num processo morto pelo kernel. Por isso `common.yml`
define os dois valores, parametrizáveis pelo `.env`:

| variável | padrão | papel |
|---|---|---|
| `WHS_GOMEMLIMIT` | `512MiB` | `GOMEMLIMIT` do processo |
| `WHS_MEMORY_LIMIT` | `768M` | limite de memória do contêiner, **acima** do `GOMEMLIMIT` |
| `WHS_RING_MAX_BYTES` | `256MiB` | teto de bytes do ring, todo na RAM |

O boot avisa se `GOMEMLIMIT` for menor que o dobro de `ring.max_bytes`; os
padrões respeitam isso. Quem subir um dos três, suba os outros junto.

As duas variáveis têm gramáticas diferentes: `WHS_MEMORY_LIMIT` é lido pelo
Compose (`768M`, `1g`); `WHS_GOMEMLIMIT` pelo runtime do Go, que aceita só um
inteiro com sufixo `B`, `KiB`, `MiB`, `GiB` ou `TiB`, ou `off`. Um
`GOMEMLIMIT=768M` derruba o processo antes do boot com `malformed GOMEMLIMIT`, e
com `restart: unless-stopped` o contêiner entra em laço.

---

## Arquivos

| arquivo | papel |
|---|---|
| `Dockerfile` | build multi-stage, binário estático (`CGO_ENABLED=0`), imagem final Alpine rodando como uid 65532 |
| `common.yml` | fragmento herdado via `extends`: build, healthcheck, hardening, limites, logging |
| `compose.memory-nats.yml` | a combinação, com o `nats-server` e o `nats-init` ao lado |
| `docker-compose.yml` | `include` do compose acima, para `docker compose up` sem `-f` |
| `nats/nats-server.conf` | JetStream em `/data`, `max_payload` de 8 MB, **sem TLS** |
| `nats/stream.json` | o stream `LOGHUB_WEBHOOK`: o contrato que o servidor valida no boot (`docs/SPEC.md` §8.7.5); o `nats-init` o aplica |
| `../.dockerignore` | mantém o contexto de build mínimo; precisa ficar na raiz do módulo, que é o contexto |

O build usa a raiz do módulo como contexto (`context: ..`), não esta pasta.

---

## Configuração

Todo parâmetro aceita variável de ambiente `WHS_<SEÇÃO>_<CHAVE>` com `.`
virando `_` — `sink.nats.urls` → `WHS_SINK_NATS_URLS` —, menos a seção
`[loghub]`, que lê os `LOGHUB_*` da skill. A precedência é:

```
defaults  <  arquivo INI  <  ambiente  <  linha de comando
```

Os compose usam ambiente. Para ver a configuração efetiva e a lista completa de
parâmetros:

```bash
docker compose run --rm --no-deps webhook --dump-config
docker compose run --rm --no-deps webhook --help
```

**`LOGHUB_EB_URL`** (`docs/SPEC.md` §8.7.11) é a URL do eventbus da convenção
LogHub: só `nats://` (uma lista com vírgula é um cluster), e ela preenche
`sink.nats.urls`. `tls://` e qualquer outro esquema encerram o boot com código
1. Com ela definida, `WHS_SINK_NATS_URLS`, `USERNAME`, `PASSWORD` e `TOKEN` não
podem ter valor também (código 1, nomeando as duas fontes); vazias, não
competem. O compose põe `nats://nats:4222` em `WHS_SINK_NATS_URLS` só quando a
variável não está no `.env`, então no `.env` basta:

```bash
LOGHUB_EB_URL=nats://usuario:senha@nats.exemplo:4222
WHS_SINK_NATS_URLS=
```

**Vindo do fast.** `WHS_RING_MODE=memory`, `WHS_SINK_TYPE=nats`,
`WHS_RATELIMIT_ENABLED=false`, `WHS_SINK_NATS_TLS=false` e
`WHS_SINK_NATS_SUBJECT=loghub.{space}.{type}` continuam aceitos; qualquer outro
valor deles encerra o boot com código 1 dizendo o porquê. Uma chave de recurso
retirado num INI ou numa flag (`sink.nats.tls_ca`, `[sink.postgres]`,
`ratelimit.rate`...) também é código 1; **no ambiente**, uma variável dessas é
ignorada, como toda `WHS_*` que o servidor não conhece (`docs/SPEC.md` §0). Não há
recarga por `SIGHUP`: o sinal é ignorado com um aviso, e mudar a configuração é
recriar o contêiner. Não mande o sinal com `docker kill -s HUP`: o Docker marca o
contêiner como parado à mão por **qualquer** `docker kill`, e a partir daí o
`restart: unless-stopped` deixa de valer — uma saída com código 3 mais tarde não
reinicia sozinha.

Para usar arquivo INI em vez de ambiente, monte-o e aponte o comando:

```yaml
    volumes:
      - ./meu-server.ini:/etc/webhook/server.ini:ro
    command: ["--config=/etc/webhook/server.ini"]
```

---

## Layout do contêiner: binário em `/app`, dados em `/data`

**Toda escrita do serviço acontece em `/data`.** É regra do `loghub-paradise`
inteiro (`docs/SPEC.md` §13.6, invariante 13). Aqui são duas escritas, ambas
derivadas de `WHS_RING_DATA_DIR`, fixado em `/data` na imagem:

| escrita | caminho | quando |
|---|---|---|
| identidade LogHub | `/data/agent_uuid`, `/data/machine_id` | na primeira subida |
| dumps de emergência | `/data/emergency/emg-*.jsonl` (escritos como `.emg-*.jsonl.tmp` e ligados ao nome final) | sink FATAL ou `drain_timeout` estourado com ring não vazio |

O resto do sistema de arquivos é **read-only** (`read_only: true` em
`common.yml`). O caso caro é o `emergency.dir`: o diretório só é criado no
instante do dump, então um caminho errado nele aparece **durante o incidente**,
com o ring inteiro virando `whs_events_dropped_total{reason="emergency_failed"}`.
Nunca o aponte para fora de `/data`.

| caminho | papel | gravável |
|---|---|---|
| `/app` | o binário, dono `root`, modo `0755` | não |
| `/data` | identidade, dumps de emergência, e o `WORKDIR` | **sim**, e só ele |
| `/etc/webhook` | INI ou `.creds` montados com `:ro` | não |
| `/tmp` | `tmpfs`, rede de segurança do `read_only` | sim, mas volátil |

Para mudar onde os dados ficam **no host**, troque o volume — nunca o caminho
dentro do contêiner:

```yaml
    volumes:
      # o diretório precisa existir e pertencer ao uid 65532
      - /mnt/nvme0/whn:/data
```

---

## Identidade LogHub e o envelope

Todo evento é um envelope JLH `type=webhook` (`docs/SPEC.md` §3), com o corpo da
requisição dentro dele (`payload.content`, em texto ou base64). O `200` devolve
o `uuid` do envelope no cabeçalho `X-LogHub-UUID` e num corpo JSON
`{"uuid":…,"mode":"fast","status":"accepted","stage":"ring"}`.

| Variável | De onde vem | Observação |
|---|---|---|
| `AGENT_NAME` | a imagem (`go-webhook-nats-server`) | nomeia a conexão NATS (`whs/<AgentName>@<Hostname>`, no `connz`) e o beacon |
| `WORKSPACE` | `WHS_WORKSPACE` do `.env` (`default`), via `common.yml` | o **tenant**: vira o `space` de todo envelope e o subject `loghub.<space>.webhook`; sem `_` |
| `HOSTNAME` | o `hostname:` do serviço | RFC 1123; sem ele seria o id do contêiner |
| `AGENT_UUID`, `MACHINE_ID` | gerados e persistidos em `/data` na primeira subida | **um volume por instância**: duas réplicas no mesmo volume publicam o mesmo `from` |

Uma identidade inválida encerra o processo com um código entre 100 e 114 antes
de qualquer porta abrir (a tabela está na skill `go-loghub-ident`).

---

## NATS: o stream `LOGHUB_WEBHOOK`

O servidor **publica** cada envelope em `loghub.<space>.webhook` (`docs/SPEC.md`
§3.8): o JLH inteiro em `Data`, byte a byte, e os cabeçalhos `Nats-Msg-Id`
(`<uuid>.0`), `X-LogHub-Type`, `X-LogHub-UUID` e `X-LogHub-Space`.

- **O servidor nunca cria nem altera o stream.** O `nats-init` o cria a partir
  de `nats/stream.json`; contra um NATS de produção esse papel é de quem o
  administra. O boot valida que o stream existe, que **cobre** o subject do
  `WORKSPACE`, que **não** captura o plano de controle (`loghub._ctl.beacon` —
  `loghub.>` é o jeito clássico de errar), que não está selado nem com
  `no_ack`, e que a maior mensagem cabe no `max_payload`, no `max_msg_size` e no
  registro de 32 MiB do *file store*. Um NATS em que ninguém responde à API do
  JetStream (desligado, ou uma folha com o *hub* fora) também falha. Qualquer
  falha é código 3 **na hora**, sem esperar `sink.boot_timeout`.
- **Outro `WORKSPACE` pede o subject dele no stream.** E um dump de emergência
  de uma vida com outro Workspace também: até o stream cobrir aquele subject, o
  boot sai com 3 nomeando-o, e o arquivo fica. Depois de o dump drenar, o
  subject pode sair do stream: a instância em execução o esquece e segue.
- **`max_payload` de 8 MB** em `nats/nats-server.conf`: com os padrões (5 MiB de
  corpo) o maior envelope tem ~7,7 MiB, acima do 1 MiB padrão do NATS. Quem
  subir `WHS_SERVER_MAX_BODY_BYTES` sobe o `max_payload` junto, e o boot diz se
  esqueceu.
- **A desduplicação** é do stream: `duplicate_window` de 10 minutos em
  `nats/stream.json`. Uma redelivery dentro dela é confirmada como duplicata e
  gravada uma vez (`whs_sink_nats_duplicates_total`); fora dela, duplica, e o
  consumidor deduplica por `(uuid, step)`.
- **`WHS_SINK_NATS_MODE=core`** publica sem stream nem confirmação: o sucesso diz
  só que o servidor NATS recebeu, e uma mensagem sem assinante some. O boot avisa.
- **Sem TLS.** O cliente é sempre texto claro; um `nats-server` com `tls {}` é
  recusado no boot (código 3) sem nenhum *handshake*. O NATS precisa estar numa
  rede confiável — aqui, a rede interna do compose.
- **Autenticação**, contra um NATS de fora: usuário e senha
  (`WHS_SINK_NATS_USERNAME`/`PASSWORD`), token (`WHS_SINK_NATS_TOKEN`) ou um
  `.creds` montado em `/etc/webhook` (`WHS_SINK_NATS_CREDENTIALS`), e só uma
  delas.

Para olhar o que chegou:

```bash
nats --server nats://127.0.0.1:14301 stream info LOGHUB_WEBHOOK
nats --server nats://127.0.0.1:14301 stream get LOGHUB_WEBHOOK 1
```

Sem o `nats` CLI no host, o do `nats-box`:

```bash
docker run --rm --network whn-memory-nats_default natsio/nats-box:0.20.0 \
  nats --server nats://nats:4222 stream info LOGHUB_WEBHOOK
```

---

## O heartbeat beacon

O servidor emite o beacon do plano de controle LogHub (`docs/SPEC.md` §8.7.10):
um envelope `type=beacon` em `loghub._ctl.beacon`, por NATS **core** (nunca num
stream), na primeira conexão, em toda reconexão e a cada
`LOGHUB_BEACON_INTERVAL` segundos (15; `0` desliga só o periódico). Uma falha
do beacon conta em `whs_beacons_total{result="error"}` e nunca derruba o
destino.

```bash
nats --server nats://127.0.0.1:14301 sub 'loghub._ctl.beacon'
```

---

## O dump de emergência (JSON Lines)

Quando o NATS fica FATAL, ou o desligamento estoura o `drain_timeout`, o que o
ring ainda tem vai para `/data/emergency/emg-<agent_uuid>-<unixnano>.jsonl`:
**um envelope JLH por linha**, exatamente os bytes cujo `uuid` o cliente
recebeu (`docs/SPEC.md` §9). O boot seguinte publica o arquivo direto no stream,
antes de abrir a porta pública, e o apaga. O formato é o do
`go-webhook-pg-server`: um dump de lá é reingerido aqui.

É JSON Lines comum, então as ferramentas de sempre servem:

```bash
docker compose exec webhook sh -c 'ls -la /data/emergency; wc -l /data/emergency/*.jsonl'
docker compose exec webhook sh -c 'head -1 /data/emergency/*.jsonl' | jq .uuid
```

Uma linha danificada — o arquivo cortado, uma edição à mão, **ou uma linha que
não seja um envelope `webhook` deste servidor** (um dump do `go-syslog-*` posto
no diretório errado, por exemplo) — é pulada; as linhas íntegras antes e depois
dela são entregues, e o arquivo vira `.corrupt` em vez de ser apagado. Ao editar
um dump, mantenha `\n` puro: um editor que grave `\r\n` torna **todas** as
linhas danificadas. Reingeri-lo de novo (renomeando de volta) é seguro dentro da
janela de desduplicação do stream. Um `emg-*.bin` do `go-webhook-fast-server`
**não** é lido: o boot avisa e o deixa intocado.

Um `.emg-*.jsonl.tmp` que sobra é um dump **interrompido** (um `SIGKILL` no
meio da escrita, um OOM, um segundo sinal): o boot o informa num `WARN` e
**não** o lê. Os eventos dele só foram contados como perda se foi o segundo
sinal; depois de um `SIGKILL` o arquivo pode estar inteiro. Para reingeri-lo,
renomeie-o para o nome final, sem o ponto inicial e sem o `.tmp` (uma última
linha cortada vai para o `.corrupt`):

```bash
docker compose exec webhook sh -c 'cd /data/emergency && for f in .emg-*.jsonl.tmp; do g=${f#.}; mv -n "$f" "${g%.tmp}"; done'
```

Um NATS **congelado** (`docker pause`, uma VM suspensa) ainda grava, ao voltar,
as publicações que já estavam no buffer TCP; o dump as tem também, e a
reingestão as vê como duplicatas (`whs_sink_nats_duplicates_total`). É o
at-least-once de `docs/SPEC.md` §8.7.3 em ação, observado no teste de fumaça de
2026-10-10.

---

## Antes de expor à internet

1. **`WHS_PROXY_TRUSTED`.** O default (`127.0.0.0/8,::1`) é fail-closed: nenhum
   header de encaminhamento é acreditado, e o IP do cliente é o peer do socket.
   Atrás de proxy/ingress isso faz o servidor enxergar o proxy como cliente —
   quebra `network.ip.src` no envelope. Declare os CIDRs do proxy.
2. **Limite de taxa no proxy.** Este servidor não tem rate limit
   (`docs/SPEC.md` §6); o proxy reverso, que vê o IP real, é o lugar dele.
3. **`stop_grace_period`.** Está em 45s em `common.yml`, acima do pior caso até
   o dump começar, `pre_stop_delay + drain_timeout + sink.op_timeout` (0s + 30s
   + 5s), com folga para o próprio dump: o `drain_timeout` é um prazo só para
   fechar as conexões e esvaziar o ring (`docs/SPEC.md` §10.3). Os 10s default
   do Docker matariam o processo no meio do dreno — perdendo eventos já
   respondidos com 200. Se aumentar qualquer um dos três, aumente também o
   grace period.
4. **`WHS_SHUTDOWN_PRE_STOP_DELAY`.** Atrás de balanceador, ponha o tempo que
   ele leva para tirar o endpoint da rotação.
5. **A rede até o NATS.** Não há TLS: credenciais e eventos trafegam em claro.
   O NATS fica numa rede privada, nunca exposto; o compose publica as portas
   dele só no loopback do host.

---

## Operação

```bash
docker compose up -d --build     # sobe
docker compose logs -f webhook   # logs (JSON por padrão)
docker compose ps                # estado + health
docker compose down              # derruba, preserva volumes
docker compose down -v           # derruba e APAGA os dados (o stream inclusive)
```

Verificação rápida:

```bash
curl -fsS -X POST localhost:8300/qualquer/caminho -d '{"ping":1}'
curl -fsS localhost:9301/readyz
curl -fsS localhost:9301/metrics | grep -E '^whs_(requests_total|events_written_total|ring_items|sink_up|beacons_total)'
nats --server nats://127.0.0.1:14301 stream info LOGHUB_WEBHOOK
```

As métricas que dizem se a stack está saudável:

| métrica | leitura |
|---|---|
| `whs_sink_up` | 1 = destino alcançável. Zerou, a porta pública responde 503 |
| `whs_ring_items` / `whs_ring_capacity_items` | ocupação do ring; a razão entre elas dispara `/readyz` no `high_water` |
| `whs_events_written_total` | eventos confirmados pelo stream (`PubAck`) |
| `whs_rejected_total` | recusas — inclui os 429 de ring cheio |
| `whs_events_dropped_total` | **deve ficar em zero**; qualquer valor aqui é evento perdido |
| `whs_emergency_files` | dumps de emergência pendentes de reingestão |
| `whs_sink_nats_duplicates_total` | publicações confirmadas como duplicatas: o custo do at-least-once. Cresce depois de uma reingestão; crescendo em regime, o dreno está retentando lotes já gravados |
| `whs_sink_nats_reconnects_total` | reconexões do cliente; cada uma também dispara um beacon |
| `whs_beacons_total{result}` | `error` subindo é o plano de controle falhando — o destino segue |

`/healthz` é *liveness* pura: responde 200 enquanto o processo consegue
responder, e de propósito não olha o NATS — reiniciar não conserta um NATS
inalcançável. Quem decide se a instância deve receber tráfego é `/readyz`, que
falha durante o boot, quando o sink está DOWN ou FATAL, quando o ring passa do
`high_water` e durante o desligamento.

### Exit codes

| código | significado |
|---|---|
| 0 | saída limpa |
| 1 | erro de configuração — inclusive um recurso retirado pedido (`docs/SPEC.md` §0) ou uma URL que não seja `nats://` |
| 3 | o NATS não ficou disponível dentro de `sink.boot_timeout` (300s); ou o stream, as credenciais, a permissão, o tamanho ou um servidor que exige TLS foram recusados no boot — nesse caso a saída é **imediata**, com o log "the sink refused this server permanently"; ou o sink ficou FATAL em operação (o ring foi para o dump) |
| 100–114 | identidade LogHub inválida |
| 130 | interrompido por um segundo sinal |

Um contêiner reiniciando em loop com exit 1 é erro de configuração, não de
infraestrutura: os logs trazem o parâmetro exato.

---

## Segurança da imagem

- Binário estático, sem CGO — o cliente NATS (`nats.go`) é Go puro, então a
  imagem final não tem toolchain nem bibliotecas nativas.
- Roda como uid/gid **65532**, nunca root. O volume nomeado herda dono e modo do
  diretório da imagem na primeira montagem, o que dispensa `chown` externo.
- O binário em `/app` pertence a `root`: o processo o executa e não tem como
  reescrever a si mesmo.
- `read_only: true` com `/tmp` em tmpfs, `cap_drop: [ALL]`,
  `no-new-privileges:true`.

Para uma imagem versionada:

```bash
WHS_IMAGE_TAG=$(git describe --tags --always) \
WHS_VERSION=$(git describe --tags --always) \
WHS_COMMIT=$(git rev-parse --short HEAD) \
docker compose build
```
