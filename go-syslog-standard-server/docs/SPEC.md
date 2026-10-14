# go-syslog-server — Especificação v1

Status: **congelada para implementação**
Data: 2026-09-12
Revisão: 2026-09-14 — posse de buffer nos caminhos de erro do `Enqueue` (§9.4, §9.5, §12.3, §14.2); leitura incremental do frame declarado (§5.5); sonda de saúde do `sink=sqlite` por heartbeat (§10.4); quarentena de linhas indecodificáveis do `ring.db` (§9.4, §13.1); tetos de conexão por transporte, não por listener (§4.4); duas referências na recarga por `SIGHUP` (§15.3); durações zero recusadas na validação (§15.5); métricas mortas alimentadas ou separadas, sobras do `ring=sqlite` não contam como perda (§12.3, §13.1); nome do dump com nanossegundos (§11.1) e invariante do parser como igualdade (§3.2, §17.2); aviso de `GOMEMLIMIT` por modo de ring (§14.4)
Revisão: 2026-09-15 — revisão independente das correções: recusa permanente na discagem ou na sonda é fatal na hora (§10.3); `ring=none` devolve o buffer ao pool após o `Write` (§14.2); tabela `health` no esquema (§3.5); `syslog_sink_errors_total{kind}` com `timeout`, `syslog_config_info` com os rótulos reais e séries de conexão só para tcp e relp (§13.1); quarentena sem teto documentada e guarda do dreno na tabela de códigos de saída (§9.4, §12.4); `sink.retry_backoff_max` recusa zero e `sink.fail_timeout = 0` documentado (§15.5); tetos de conexão descritos na tabela `[relp]` (§15.5); §17.5 sem prometer suítes que não existem
Revisão: 2026-09-15 — `sink=mysql` e `sink=nats` saem do roteiro pós-v1 (§20) e entram na v1, a pedido do dono do projeto e no molde que o `go-webhook-server` validou contra servidores reais no mesmo dia: tabela MySQL (§3.7) e mensagem JetStream (§3.8), os dois destinos (§10.7, §10.8), o teto de evento conferido no boot contra `max_allowed_packet` e `max_payload` (§3.9), o envelope no corpo da mensagem NATS e não num header, porque o JetStream recusa header acima de 64 KiB (§3.8), a matriz com dezoito combinações (§2.1, §18), a configuração (§12.1, §15.5), as métricas (§13.1), seis dependências (§16.3) e dois invariantes (§19)
Revisão: 2026-10-11 — recusa por IP em laço, keep-alive sob o limitador e padrão `0` dos tetos de conexão como decisão (§4.4); fase 3 do RELP sem `serverclose`, como o código faz (§12.3); arquivo inválido no `SIGHUP` (§15.3); dump falho conta só `emergency_failed` (§11.3); pendências conhecidas desta referência (§21)
Linguagem: Go 1.25+
Repositório: `https://github.com/patrickbrandao/loghub-paradise` (monorepo)
Module path: `github.com/patrickbrandao/loghub-paradise/go-syslog-standard-server`
Binário: `go-syslog-server`

Serviço irmão do `go-webhook-server`. O desenho, os dois eixos, a máquina de
saúde, o registry de configuração, o arquivo de emergência e as cinco fases do
shutdown são os **mesmos**, e onde esta spec repete a dele é de propósito: um
operador que conhece um dos dois conhece o outro. Onde ela **diverge**, a
divergência é justificada no lugar, porque syslog não tem como mentir a mesma
mentira que o HTTP tem.

---

## 1. Escopo

Servidor de ingestão de syslog. Recebe mensagens em quatro transportes,
interpreta o que consegue de RFC 3164 e RFC 5424 sem nunca falhar, encapsula
cada mensagem num envelope e a entrega a um **destino configurável**. Escalável
horizontalmente: instâncias são independentes e não compartilham estado.

### 1.1 Objetivos

- Máximo de mensagens por segundo com mínimo de CPU e memória.
- Nunca confirmar uma mensagem que não foi aceita segundo a garantia da
  combinação `ring` × `sink` configurada — **nos transportes que têm como
  confirmar** (§2.3).
- Backpressure explícito onde o transporte permite; perda **contada e visível**
  onde ele não permite.
- Preservar os bytes da linha recebida **intactos**, sempre, independentemente
  do que o parser tenha conseguido entender deles.
- Operar tanto acoplado ao ecossistema (`sink=redis`, `sink=nats`) quanto de
  forma autônoma (`sink=sqlite`), ou contra um banco compartilhado
  (`sink=postgres`, `sink=mysql`).

### 1.2 Não-objetivos (v1)

| Fora de escopo | Motivo |
|---|---|
| TLS na porta de ingestão (RFC 5425) | Decisão do operador; o `stunnel`/`haproxy` na frente resolve, e a v1 não carrega o custo de um handshake por conexão |
| DTLS (RFC 6012) | Mesma razão, com implementação muito menos madura no ecossistema |
| Autenticação de emissor | Não existe no protocolo; quem precisa usa firewall ou rede privada |
| Reescrita ou normalização da mensagem | A linha é preservada byte a byte |
| Roteamento por facility para destinos distintos | Um destino por instância na v1; §20 |
| Filtro por expressão regular no texto | Custo por mensagem imprevisível no caminho quente |
| Deduplicação / `last message repeated N times` | Responsabilidade do consumidor |
| Syslog **de saída** (encaminhar para outro servidor) | §20; o destino do ecossistema é o Redis |

> **TLS na ingestão está fora; TLS de saída não.** `sink.redis.tls`,
> `sink.postgres.tls`, `sink.mysql.tls` e `sink.nats.tls` existem e funcionam.
> São coisas diferentes: o primeiro é um servidor que o operador escolhe não
> expor com TLS, o segundo é um cliente nosso falando com a infraestrutura dele,
> onde nenhum proxy reverso ajuda.

---

## 2. Arquitetura

### 2.1 Dois eixos ortogonais

Idêntico ao `go-webhook-server`, e pela mesma razão:

- **RING** — *onde a mensagem espera* entre ser aceita e ser entregue.
  `memory` | `sqlite` | `none`
- **SINK** — *para onde a mensagem vai*, o destino final.
  `redis` | `sqlite` | `postgres` | `mysql` | `nats` | `none`

As dezoito combinações são válidas e têm significado distinto (§18).

```
   emissores (rsyslog, syslog-ng, journald, logger, appliances, k8s)
      |
      |  UDP/514   TCP/514   unix dgram   RELP/20514
      v
 +------------------------------------------------------------------+
 | RECEPÇÃO  (internal/receiver)                                     |
 |   moldura -> teto de frame -> PRI -> filtro -> ratelimit -> Item  |
 +---------------------------------+--------------------------------+
                                   |
                    +--------------v---------------+
                    | RING                         |
                    |  memory  canal + tetos       |
                    |  sqlite  ring.db, fsync      |
                    |  none    sem buffer          |
                    +--------------+---------------+
                                   | sink.workers
                    +--------------v---------------+
                    | PARSE COMPLETO + ENVELOPE    |  <- fora do caminho quente
                    +--------------+---------------+
                                   |
                    +--------------v---------------+
                    | SINK                         |
                    |  redis     XADD em pipeline  |
                    |  sqlite    events.db         |
                    |  postgres  INSERT em lote    |
                    |  mysql     INSERT em lote    |
                    |  nats      publish + PubAck  |
                    |  none      descarta          |
                    +--------------+---------------+
                                   |
      +-------------+-------------+-+-----------+-------------+-----------+
      v             v             v             v             v           v
 Redis Stream   events.db    tabela events  tabela events  stream       /dev/null
 "syslog:in"    (destino     (PostgreSQL    (MySQL         JetStream    (bench
                 final)       partilhado)    partilhado)   "SYSLOG"      puro)

 emergência: ring=memory não-vazio + sink FATAL
             => dump em <emergency.dir>/emg-<node>-<unixnano>.bin  (§11)

 +------------------------------------------------------------------+
 | porta admin (admin.bind, padrão 127.0.0.1:9102)                   |
 |   /healthz   /readyz   /metrics                                   |
 +------------------------------------------------------------------+
```

O ring é **único e global**, compartilhado pelos quatro transportes. É o único
ponto de serialização do desenho e é intencional: dá uma métrica única de
profundidade, backpressure justo (o TCP só para de ler quando o sistema inteiro
está saturado, não quando "o ring do TCP" encheu) e código simples.

### 2.2 Onde o parse acontece, e por quê

Esta é a **divergência estrutural** em relação ao `go-webhook-server`, onde o
envelope é serializado dentro do handler, antes do ring.

| Etapa | Onde roda | Custo |
|---|---|---|
| Moldura, teto de frame | recepção | inevitável, é o transporte |
| **PRI** (`<134>`) | **recepção** | uma varredura de no máximo 5 bytes |
| Filtro por facility/severity | recepção | duas comparações de inteiro |
| RFC 3164 / RFC 5424 completo, structured-data | **worker de dreno** | proporcional ao tamanho da linha |
| Serialização do envelope | **worker de dreno** | proporcional ao número de campos |

O PRI fica na recepção porque o filtro de §7 precisa dele, e descartar depois
do ring seria aceitar para jogar fora — o oposto do invariante §19.2. Todo o
resto sai do caminho quente: num pico de UDP, a recepção faz o mínimo que
permite decidir se a mensagem entra, e a CPU do parse é paga pelos
`sink.workers`, que já são o lugar onde a vazão é elástica.

Três consequências que o resto da spec assume:

1. **O item no ring não tem envelope.** Ele carrega os bytes crus e os campos
   que o parse *não pode* recuperar deles: identificador, instante de recepção,
   transporte, peer e o PRI efetivo. §3.2 chama isso de **cabeçalho do item**.
2. **A contabilidade de bytes do ring conta só o que existe em RAM** — os bytes
   crus. O envelope é construído e consumido dentro do lote do worker, e seu
   teto é `sink.workers × sink.batch × tamanho máximo de envelope`, contabilizado
   em §14.3 e não no ring. Isso é **mais** exato que somar um envelope que ainda
   não foi alocado, não menos.
3. **O arquivo de emergência e o `ring=sqlite` guardam cabeçalho + bytes crus**,
   não envelope + corpo. O cabeçalho *é* o metadado, então a reconstrução de
   metadado a partir de JSON escapado — que na referência foi uma armadilha real
   — simplesmente não existe aqui.

Com `ring=none` não há worker: o parse completo roda na própria goroutine da
recepção, porque é ela quem escreve no sink. É o modo explicitamente lento.

### 2.3 Regra de aceitação, por transporte

> A confirmação sai no instante em que a garantia da combinação configurada é
> cumprida — nunca antes. **Onde não existe confirmação, existe um contador.**

O `go-webhook-server` pode sempre responder `200` ou `429`. O syslog não. Esta
tabela é o contrato honesto de cada transporte:

| Transporte | A confirmação é | Quando ela sai | Ring cheio significa |
|---|---|---|---|
| **UDP** | não existe | nunca | datagrama **descartado**, `syslog_messages_dropped_total{reason="ring_full"}` |
| **unix dgram** | não existe | nunca | mesma coisa — mas o kernel bloqueia o emissor quando o buffer do socket enche, então o backpressure existe **antes** de chegar a nós |
| **TCP** | continuar lendo do socket | a janela TCP só reabre depois da garantia do ring | **parar de ler** (§5.4); o emissor sente a pressão e nada se perde |
| **RELP** | um frame `rsp` com `200 OK` | depois da garantia do ring, nunca antes | **parar de responder**; o emissor esgota a janela e segura a mensagem |

E dentro de cada transporte que confirma, a garantia é a do ring:

| `ring` | A garantia é |
|---|---|
| `memory` | o item entrou no canal do ring |
| `sqlite` | o `COMMIT` do grupo no `ring.db` terminou (com `fsync` se `synchronous=FULL`) |
| `none` | o **sink** confirmou a escrita (ou imediatamente, se `sink=none`) |

**O UDP é lossy por desenho do protocolo, não por desenho deste servidor.** Um
datagrama perdido no kernel antes de chegarmos (`SO_RCVBUF` estourado) nem
aparece como perda nossa: aparece em `syslog_udp_receive_errors_total` e no
contador de drops da interface. Quem não pode perder usa RELP ou TCP; a spec
não finge o contrário em lugar nenhum.

---

## 3. Modelo de dados

### 3.1 Envelope v1

Estrutura plana, nomes em kebab-case, idêntico em todos os sinks. É o mesmo
contrato do `go-webhook-server` na forma, e completamente diferente no conteúdo.

```json
{
  "id": "019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f",
  "ts": 1757116800,
  "uts": 123456,
  "nts": 123456789,
  "transport": "tcp",
  "framing": "octet",
  "listener": ":1514",
  "peer-addr": "10.0.0.7",
  "peer-port": 51234,
  "peer-ip-version": 4,
  "rfc": "5424",
  "pri": 134,
  "pri-source": "wire",
  "facility": 16,
  "facility-name": "local0",
  "severity": 6,
  "severity-name": "info",
  "version": 1,
  "msg-ts": "2026-09-12T14:33:01.123456-03:00",
  "msg-ts-source": "rfc5424",
  "hostname": "web-01",
  "app-name": "nginx",
  "procid": "1234",
  "msgid": "ID47",
  "structured-data": {
    "exampleSDID@32473": { "iut": "3", "eventSource": "Application" }
  },
  "msg-offset": 98,
  "msg-size": 31,
  "raw-size": 129,
  "truncated": false,
  "node-id": "sls-prod-a1"
}
```

| Campo | Tipo | Definição |
|---|---|---|
| `id` | string | UUIDv7 (RFC 9562) em `Level2` — milissegundo nos 48 bits padrão, microssegundo em `rand_a` (§16.3). Gerado **antes** de entrar no ring. |
| `ts` | int | Unix timestamp em **segundos** da **recepção**. |
| `uts` | int | Fração do segundo em microssegundos, `0..999999`. |
| `nts` | int | Fração do segundo em nanossegundos, `0..999999999`. |
| `transport` | string | `"udp"`, `"tcp"`, `"unix"` ou `"relp"`. |
| `framing` | string | `"datagram"`, `"octet"`, `"lf"` ou `"relp"`. Ver §5. |
| `listener` | string | O `listen.*` que recebeu a mensagem. |
| `peer-addr` | string | IP da ponta que enviou. `""` no transporte `unix`. |
| `peer-port` | int | Porta de origem. `0` no transporte `unix`. |
| `peer-ip-version` | int | `4`, `6`, ou `0` no transporte `unix`. |
| `rfc` | string | `"5424"`, `"3164"` ou `"unknown"`. Ver §6. |
| `pri` | int | Prioridade **efetiva**, `0..191`. |
| `pri-source` | string | `"wire"` se veio da mensagem, `"default"` se veio de `parse.default_*`. |
| `facility` | int | `pri / 8`, `0..23`. |
| `facility-name` | string | `kern`, `user`, …, `local7`. Ver §6.1. |
| `severity` | int | `pri % 8`, `0..7`. |
| `severity-name` | string | `emerg`, `alert`, `crit`, `err`, `warning`, `notice`, `info`, `debug`. |
| `version` | int | Campo `VERSION` da RFC 5424. `0` quando `rfc != "5424"`. |
| `msg-ts` | string | Timestamp **declarado dentro da mensagem**, em RFC 3339. `""` se ausente ou ilegível. |
| `msg-ts-source` | string | `"rfc5424"`, `"rfc3164"` ou `"none"`. Ver §6.4. |
| `hostname` | string | `HOSTNAME` da mensagem. **Alegação do emissor, não verificada.** |
| `app-name` | string | `APP-NAME` (5424) ou o `TAG` sem o pid (3164). |
| `procid` | string | `PROCID` (5424) ou o pid entre colchetes do `TAG` (3164). |
| `msgid` | string | `MSGID` (5424). `""` nos demais. |
| `structured-data` | object | `SD-ID → (nome de parâmetro → valor)`. Objeto vazio quando ausente. Ver §6.5. |
| `msg-offset` | int | Deslocamento em bytes, dentro da linha crua, onde o `MSG` começa. |
| `msg-size` | int | Bytes do `MSG`. |
| `raw-size` | int | Bytes da linha crua. |
| `truncated` | bool | `true` se a linha foi cortada pelo transporte ou pelo teto de frame. |
| `node-id` | string | Identidade da instância. Ver §3.4. |

#### Escape

Todo campo de texto do envelope veio do fio, e o serializador trata cada byte
como hostil: UTF-8 inválido vira `U+FFFD`, e `"`, `\` e todo byte abaixo de
`0x20` saem como sequência de escape (`\n`, `\r`, `\t` ou `\u00XX`). O preço é
de tamanho, e §3.9 o contabiliza: um byte de controle ocupa seis no envelope.

#### Os três campos de tempo, e de quem é o tempo

`ts`/`uts`/`nts` são o instante da **recepção**, decompostos de **uma única**
chamada a `time.Now()`, nunca duas. São pequenos e portanto seguros em
JavaScript, pela mesma aritmética do serviço irmão: um epoch em nanossegundos
(`1.757e18`) excede `Number.MAX_SAFE_INTEGER` (`9.007e15`) e um `JSON.parse`
perderia os últimos dígitos em silêncio.

O instante marcado é o fim da leitura do **frame**, antes de qualquer parse.

> **`ts`/`uts`/`nts` são autoritativos. `msg-ts` não é, e o tempo dentro do
> `id` também não.**
>
> `msg-ts` vem do emissor e é a fonte mais mal-comportada de todo o protocolo:
> na RFC 3164 ele **não tem ano e não tem fuso** (`Oct 11 22:14:15`), o que
> torna impossível ordená-lo sem adivinhar duas coisas; na RFC 5424 ele é
> RFC 3339 correto, mas depende do relógio de uma máquina que ninguém auditou.
> Usá-lo como chave de ordenação quebra na virada do ano, no horário de verão e
> em qualquer host com NTP fora.
>
> Por isso ele é **exportado e nunca usado**: não indexa, não ordena, não entra
> em nenhum `WHERE` que a spec prometa. Quem quiser corrigir o relógio do
> emissor tem o dado; quem quiser correlacionar eventos usa `ts`.
>
> O tempo embutido no `id` tem a mesma ressalva do serviço irmão (§16.3): ele é
> chave de **ordenação**, não resposta para "que horas eram". A leitura de
> relógio acontece dentro da biblioteca, numa chamada separada da que preenche
> `ts`/`uts`/`nts`.

#### Por que o `MSG` não está no envelope

O envelope carrega `msg-offset` e `msg-size`, não o texto. A mensagem está uma
vez só, nos bytes crus, e o consumidor a obtém com uma fatia:

```
msg := raw[env["msg-offset"] : env["msg-offset"]+env["msg-size"]]
```

A alternativa — um campo `"msg"` com o texto — foi recusada por três motivos
medidos no desenho:

1. **Duplicaria a carga.** Numa mensagem típica de 200 bytes, o texto é a maior
   parte dela. Guardá-lo no envelope *e* nos bytes crus quase dobra o
   armazenamento de todo evento, em todo destino.
2. **Custaria escaping dos dois lados.** Pôr o texto em JSON obriga a escapar na
   escrita e a **desescapar na leitura**. Fatiar é mais barato que qualquer uma
   das duas pontas, e não aloca.
3. **Os bytes crus precisam existir de qualquer forma.** O parse é best-effort:
   quando ele classifica `rfc: "unknown"`, a linha original é a única coisa que
   permite descobrir o que o emissor realmente mandou. Ela não é opcional, então
   o texto dentro dela também não precisa ser copiado.

O preço é uma linha de aritmética no consumidor. É o mesmo consumidor que já
lida com o campo binário `b` separado no serviço irmão.

**Invariante:** `raw-size == len(b)` e `msg-offset + msg-size == raw-size` (o
`MSG` vai sempre até o fim da linha; o parser garante a igualdade e o fuzz a
exige).

### 3.2 Cabeçalho do item

O que o parse **não consegue** recuperar dos bytes crus, e que portanto precisa
viajar ao lado deles pelo ring, pelo `ring=sqlite` e pelo arquivo de emergência:

```json
{
  "id": "019823f1-7a2c-7b3d-8e4f-1a2b3c4d5e6f",
  "ts": 1757116800, "uts": 123456, "nts": 123456789,
  "transport": "tcp", "framing": "octet", "listener": ":1514",
  "peer-addr": "10.0.0.7", "peer-port": 51234, "peer-ip-version": 4,
  "pri": 134, "pri-source": "wire",
  "truncated": false
}
```

Tipicamente ~190 bytes. Todo o resto do envelope de §3.1 é **derivado** dos
bytes crus pelo parse, de forma determinística: dois processos que leiam o mesmo
cabeçalho e a mesma linha produzem envelopes idênticos byte a byte.

Essa propriedade é o que faz a reingestão (§11.2) ser trivial aqui e ter sido
uma armadilha no serviço irmão, onde o dump guardava envelope pronto e o
metadado das colunas precisava ser **reconstruído parseando JSON escapado**.
Aqui o cabeçalho *é* o metadado e não há nada a reconstruir.

O cabeçalho tem **um** codificador, usado por três consumidores: o `ring=sqlite`
(coluna `head`), o arquivo de emergência (§11.1) e nada mais. Ele é JSON pelo
mesmo motivo que a reconstrução do irmão é: roda uma vez por evento fora do
caminho quente, e um formato próprio ali seria um parser novo de entrada
adversária sem ganho nenhum.

### 3.3 Entrada do Redis Stream (`sink=redis`)

Cada mensagem é **um** `XADD` com exatamente **três campos**, idênticos em forma
aos do `go-webhook-server`:

```
XADD <sink.redis.stream> * v 1 e <envelope-json> b <linha-crua>
```

| Campo | Conteúdo |
|---|---|
| `v` | Versão do formato. `"1"` na v1. Fica **fora** do envelope para permitir trocar a codificação numa v2 sem o consumidor precisar parsear para descobrir como parsear. |
| `e` | Envelope de §3.1 em JSON (UTF-8, sem indentação). |
| `b` | A linha crua, bytes idênticos aos recebidos, **sem a moldura do transporte**. Sem escaping, sem base64. |

O campo `b` é **sempre escrito**, inclusive para uma mensagem vazia (`b ""`).

**O que `b` contém e o que não contém.** Contém a mensagem syslog do `<` do PRI
ao último byte útil. **Não** contém: o comprimento da moldura octet-counting, o
`\n` terminador da moldura LF, o `\0` de emissores antigos, nem o cabeçalho do
frame RELP. Aquilo é envelope de transporte e vive em `transport` e `framing`.

**Stream separado.** O padrão é `syslog:in`, não `webhooks:in`. Os dois fluxos
têm volume, retenção e consumidores com necessidades diferentes; misturá-los
faria `v=1` significar duas coisas e a retenção de um estrangular o outro.

### 3.4 `node-id`

1. `node.id` se configurado;
2. senão, `os.Hostname()`;
3. senão (hostname vazio ou erro), os 8 primeiros hex de um UUIDv7 do boot.

Registrado no log de boot.

### 3.5 Tabela de eventos (`sink=sqlite`)

Destino **final**, append-only. Nenhum `DELETE` no caminho quente.

```sql
CREATE TABLE IF NOT EXISTS events (
  id         TEXT    PRIMARY KEY,   -- UUIDv7 Level2
  ts         INTEGER NOT NULL,      -- unix seconds da RECEPÇÃO
  nts        INTEGER NOT NULL,      -- fração ns dentro do segundo
  facility   INTEGER NOT NULL,      -- 0..23, efetiva
  severity   INTEGER NOT NULL,      -- 0..7, efetiva
  hostname   TEXT    NOT NULL,      -- alegado pela mensagem
  app_name   TEXT    NOT NULL,
  procid     TEXT    NOT NULL,
  msgid      TEXT    NOT NULL,
  peer_ip    TEXT    NOT NULL,      -- verdade do fio
  transport  TEXT    NOT NULL,      -- udp | tcp | unix | relp
  rfc        TEXT    NOT NULL,      -- 5424 | 3164 | unknown
  msg_offset INTEGER NOT NULL,      -- início do MSG dentro de raw
  msg_size   INTEGER NOT NULL,
  env        BLOB    NOT NULL,      -- envelope JSON completo
  raw        BLOB    NOT NULL,      -- a linha crua
  nbytes     INTEGER NOT NULL       -- len(env) + len(raw)
) STRICT;

CREATE INDEX IF NOT EXISTS ev_ts   ON events(ts);
CREATE INDEX IF NOT EXISTS ev_host ON events(hostname, ts);

-- Batimento da sonda de saúde (§10.4): uma linha, reescrita no lugar.
CREATE TABLE IF NOT EXISTS health (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  ts INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ev_sev  ON events(severity, ts);
```

As colunas expandidas são deliberadas e pela mesma razão do serviço irmão: este
é o destino **final**, e um banco que só guarda blob obriga `json_extract()` em
varredura completa, reduzindo o SQLite a um arquivo de despejo.

**Não existe coluna `msg`.** A busca textual é

```sql
SELECT * FROM events WHERE CAST(raw AS TEXT) LIKE '%erro%' AND ts > ?;
```

que é **melhor** que uma coluna de mensagem: cobre também hostname, tag e
structured-data, que é onde metade das investigações realmente termina. Quem
precisar exatamente do `MSG` usa
`substr(CAST(raw AS TEXT), msg_offset + 1, msg_size)`.

O `CAST` é obrigatório e não é detalhe: `raw` é `BLOB`, e o `LIKE` do SQLite
sobre um blob não casa nada — devolve zero linhas sem erro nenhum, que é a pior
forma possível de falhar numa consulta de investigação. `BLOB` é o tipo certo
mesmo assim, porque é o único que preserva byte a byte uma linha que pode conter
`\0` no meio.

`structured-data` **não** vira coluna: é o campo de cardinalidade e tamanho
imprevisíveis, e já está em `env`.

**Três índices, não quatro.** `(app_name, ts)` é o quarto óbvio e foi deixado de
fora: cada índice é uma escrita a mais por linha num destino cujo gargalo é
escrita, e `app_name` aparece em `raw` para quem busca por texto. Quem consulta
por aplicação o tempo todo cria o índice; o servidor não impõe o custo a quem
não consulta.

### 3.6 Tabela de eventos (`sink=postgres`)

Destino **final**, append-only, **compartilhado por todas as instâncias**. Mesmas
colunas de §3.5 com tipos nativos. O servidor **nunca cria nem altera** esta
tabela (§10.5): o esquema é aplicado por quem administra o banco, a partir de
`docker/postgres/schema.sql`, que é a cópia canônica deste bloco.

```sql
CREATE TABLE IF NOT EXISTS events (
  id         uuid      PRIMARY KEY,   -- UUIDv7 Level2, indexável e ordenável
  ts         bigint    NOT NULL,
  nts        bigint    NOT NULL,
  facility   smallint  NOT NULL,
  severity   smallint  NOT NULL,
  hostname   text      NOT NULL,
  app_name   text      NOT NULL,
  procid     text      NOT NULL,
  msgid      text      NOT NULL,
  peer_ip    text      NOT NULL,
  transport  text      NOT NULL,
  rfc        text      NOT NULL,
  msg_offset bigint    NOT NULL,
  msg_size   bigint    NOT NULL,
  env        bytea     NOT NULL,
  raw        bytea     NOT NULL,
  nbytes     bigint    NOT NULL
);

CREATE INDEX IF NOT EXISTS ev_ts   ON events (ts);
CREATE INDEX IF NOT EXISTS ev_host ON events (hostname, ts);
CREATE INDEX IF NOT EXISTS ev_sev  ON events (severity, ts);
```

O que muda em relação a §3.5, e por quê:

- `id` é `uuid` nativo: dezesseis bytes em vez de trinta e seis, comparação
  binária e a ordenação cronológica que §16.3 promete.
- `env` e `raw` são `bytea`. Acima de ~2 KiB vão para TOAST e são **comprimidos
  pelo servidor**. Fica assim de propósito: texto de log comprime
  excepcionalmente bem — melhor que JSON de webhook — e o disco é o recurso
  caro.
- **As colunas de texto são cópias para consulta, não o dado.** O PostgreSQL
  recusa texto que não seja UTF-8 válido (`22021`), e uma classe `22` derruba o
  lote inteiro (§10.5.7). Isto **importa mais aqui que no serviço irmão**: um
  `hostname` ou um `app-name` vindo de um appliance em latin-1 não é hipótese
  remota, é terça-feira. Por isso `hostname`, `app_name`, `procid`, `msgid`,
  `peer_ip`, `transport` e `rfc` têm sequências inválidas substituídas por
  `U+FFFD` **na coluna**. O dado íntegro continua em `env` e em `raw`, byte a
  byte (§19.7).
- Sem particionamento na v1. Volume de syslog pede particionamento por tempo
  mais cedo que webhook; §20.

O que o servidor **exige** da tabela, verificado no boot (§10.5.5): as dezessete
colunas acima com estes tipos (`text` ou `character varying` nas de texto;
`smallint` ou `integer` em `facility` e `severity`); um índice **único**
exatamente sobre `(id)`; permissão de `INSERT` para o papel configurado. Colunas
extras são toleradas. Nada além disso é lido nem escrito.

### 3.7 Tabela de eventos (`sink=mysql`)

Destino **final**, append-only, **compartilhado por todas as instâncias** — o
mesmo papel de §3.6, num MySQL (8.0+) ou MariaDB (10.6+). O servidor **nunca
cria nem altera** esta tabela (§10.7); a cópia canônica é
`docker/mysql/schema.sql`.

```sql
CREATE TABLE IF NOT EXISTS events (
  id         BINARY(16)   NOT NULL PRIMARY KEY,  -- UUIDv7 Level2 em dezesseis bytes
  ts         BIGINT       NOT NULL,              -- unix seconds da RECEPÇÃO
  nts        BIGINT       NOT NULL,              -- fração ns dentro do segundo
  facility   TINYINT      NOT NULL,              -- 0..23, efetiva
  severity   TINYINT      NOT NULL,              -- 0..7, efetiva
  hostname   LONGTEXT     NOT NULL,              -- alegado pela mensagem; U+FFFD se inválido
  app_name   LONGTEXT     NOT NULL,
  procid     LONGTEXT     NOT NULL,
  msgid      LONGTEXT     NOT NULL,
  peer_ip    VARCHAR(64)  NOT NULL,              -- verdade do fio
  transport  VARCHAR(16)  NOT NULL,              -- udp | tcp | unix | relp
  rfc        VARCHAR(16)  NOT NULL,              -- 5424 | 3164 | unknown
  msg_offset BIGINT       NOT NULL,              -- início do MSG dentro de raw
  msg_size   BIGINT       NOT NULL,
  env        LONGBLOB     NOT NULL,              -- envelope JSON completo
  raw        LONGBLOB     NOT NULL,              -- a linha crua, byte a byte
  nbytes     BIGINT       NOT NULL,              -- len(env) + len(raw)
  INDEX ev_ts   (ts),
  INDEX ev_host (hostname(255), ts),
  INDEX ev_sev  (severity, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

O que muda em relação a §3.6, e por quê:

- `id` é `BINARY(16)`: o MySQL não tem tipo `uuid`. Os dezesseis bytes são
  produzidos no cliente, como no postgres, e a chave primária clusterizada do
  InnoDB ordena as linhas por ele — que é cronológico (§16.3), então os inserts
  caem sempre no fim do índice, sem fragmentar página.
- **`hostname`, `app_name`, `procid` e `msgid` são `LONGTEXT`, não `VARCHAR`
  nem `TEXT`.** Com o `sql_mode` estrito da sessão (§10.7.1), um valor acima da
  capacidade da coluna é recusado com `1406`, cuja classe é `logic` (§10.7.6):
  o lote inteiro é descartado. O parser só trunca na RFC 5424 (§6.5); na RFC
  3164 o hostname é um token até o espaço e o pid é uma sequência de dígitos,
  os dois sem teto além do frame. Um `TEXT` guarda 64 KiB, e uma única linha de
  lixo com um token maior que isso levaria 255 vizinhos junto. `LONGTEXT` custa
  dois bytes de comprimento a mais por valor e tira o caso do mapa. O índice usa
  prefixo de 255 caracteres, que o InnoDB aceita em `utf8mb4`.
- `peer_ip`, `transport` e `rfc` ficam em `VARCHAR` porque têm teto por
  construção: quem os escreve é o servidor, nunca o emissor.
- **As colunas de texto são cópias saneadas**, pela razão de §3.6: com o modo
  estrito, um byte que não seja UTF-8 é `1366` e derruba o lote. UTF-8 inválido
  e `NUL` viram `U+FFFD` na coluna. O `NUL` o MySQL aceitaria; ele é trocado
  mesmo assim para que os dois bancos compartilhados respondam ao mesmo `WHERE`
  do mesmo jeito. `env` e `raw` seguem íntegros (§19.7).
- `utf8mb4_bin`: comparação byte a byte em `WHERE hostname = ?`, sem o folding
  de caixa de uma collation `_ci`.
- **InnoDB é exigido** (§10.7.4): a validação do boot faz um `INSERT` de sonda
  numa transação revertida, o que só é inócuo num motor transacional; e um motor
  sem recuperação de crash não sustenta a garantia de §18.

A busca textual de §3.5 funciona sem `CAST`: `LIKE` sobre `LONGBLOB` compara
bytes no MySQL (e portanto distingue caixa), e
`SUBSTRING(raw, msg_offset + 1, msg_size)` devolve exatamente o `MSG`.

O que o servidor **exige** da tabela, verificado no boot (§10.7.4): motor
InnoDB; as dezessete colunas com estas famílias de tipo (`binary(16)` em `id`;
`bigint` em `ts`, `nts`, `msg_offset`, `msg_size` e `nbytes`; `tinyint`,
`smallint`, `mediumint`, `int` ou `bigint` em `facility` e `severity`; `char`,
`varchar` ou qualquer `text` nas de texto; `varbinary` ou qualquer `blob` em
`env` e `raw`); um índice **único** exatamente sobre `(id)`; `INSERT` e `UPDATE`
para o usuário (o `UPDATE` pode ficar restrito à coluna `id`; §10.7.4); um
`max_allowed_packet` que comporte o maior evento que este servidor aceita
(§3.9). Colunas extras são toleradas. **Tipos mais estreitos que os canônicos
também são** — um `VARCHAR(255)` em `hostname`, um `MEDIUMBLOB` em `raw` — e
trocam espaço por um `1406` quando um valor não couber, com o lote descartado e
contado. É a mesma troca de um `character varying(n)` no PostgreSQL, e a escolha
é do operador.

### 3.8 Mensagem JetStream (`sink=nats`)

Cada mensagem syslog é **uma** mensagem publicada em `sink.nats.subject`, num
stream JetStream que o servidor valida no boot e nunca cria (§10.8.5):

```
Subject:  syslog.in
Headers:  Nats-Msg-Id:        <id>        -- desduplicação do JetStream
          Sls-Version:        1
          Sls-Envelope-Size:  <n>         -- bytes do envelope, em decimal
Data:     <envelope JSON><linha crua>
```

| Parte | Conteúdo |
|---|---|
| `Nats-Msg-Id` | O `id` do envelope. O JetStream responde como duplicata uma segunda mensagem com o mesmo id dentro da janela `duplicate_window` do stream (§10.8.3). |
| `Sls-Version` | Versão do formato, `"1"`, fora do envelope pelo mesmo motivo do campo `v` de §3.3. |
| `Sls-Envelope-Size` | Quantos bytes do início de `Data` são o envelope. |
| `Data` | O envelope de §3.1, byte a byte, seguido **imediatamente** da linha crua, bytes idênticos aos recebidos e sem a moldura do transporte — o conteúdo dos campos `e` e `b` de §3.3, nessa ordem. Sem separador, sem escaping, sem base64. |

O consumidor fatia, como fatia o `MSG`:

```
n   := atoi(msg.Header["Sls-Envelope-Size"])
env := msg.Data[:n]
raw := msg.Data[n:]
```

**Invariante:** `len(Data) == Sls-Envelope-Size + e["raw-size"]`.

**O envelope não vai num header, e aqui este serviço diverge do irmão.** O
`go-webhook-server` publica o envelope no header `Whs-Envelope` e o corpo em
`Data`. O JetStream grava o comprimento do bloco de headers em dezesseis bits e
recusa qualquer um acima de 65 535 bytes (`10097`, "header size exceeds maximum
allowed of 64k"), sem configuração que mude isso. Um envelope de syslog passa
desse tamanho com um hostname RFC 3164 longo ou alguns quilobytes de
structured-data, e a resposta é um `400` — que, desconhecido, seria `fatal`: um
único emissor encerraria o processo, e o dump reingerido no boot seguinte o
encerraria de novo. No corpo, o envelope só conta para o `max_payload`, que §3.9
já confere, e o bloco de headers tem tamanho fixo e pequeno. Foi o teste contra
servidor real que achou o limite (`docs/TESTING.md` §9).

`Sls-` é o prefixo deste serviço, como `Whs-` é o do irmão. Os dois fluxos
vivem em streams separados (`SYSLOG` e `WEBHOOKS`) pela razão de §3.3.

Headers e dados contam juntos para o `max_payload` do servidor NATS (1 MiB por
padrão), para o `max_msg_size` do stream, se houver, e para o teto de registro
do file store (32 MiB). O boot recusa (§10.8.5) uma configuração em que o maior
evento aceitável (§3.9) não caiba nesses limites: confirmar uma mensagem por
RELP e descobrir na publicação que ela não cabe seria descarte de mensagem
confirmada.

### 3.9 O maior evento, e por que ele é calculado e não configurado

Dois destinos têm teto por mensagem bem abaixo do frame de 16 MiB:
`max_allowed_packet` no MySQL e `max_payload` no NATS. Os outros três não têm
teto que o frame alcance (o Redis aceita 512 MiB num campo; o `bytea` e o blob
do SQLite, perto de 1 GiB). Uma mensagem aceita, confirmada e depois recusada
por tamanho seria `logic`, e `logic` descarta o **lote inteiro**. Por isso o
boot compara o teto do destino com o pior caso do que este servidor pode ter
aceitado, e não abre os transportes quando ele não cabe.

O pior caso é calculado da configuração, sem parâmetro próprio:

| Parte | Teto |
|---|---|
| linha crua `L` | o maior teto entre os transportes **ligados**: `tcp.max_frame_bytes`, `relp.max_frame_bytes`, e 65 507 bytes para UDP e unix |
| envelope | `6 × (L + listener + len(node-id)) + 1 KiB`, com `listener` o maior `listen.*` configurado mais 64 bytes: o campo guarda o endereço que o socket reporta (`:1514` vira `[::]:1514`), e 64 bytes cobrem um IPv6 com zona e porta |

O fator 6 não é folga, é o escape: um byte de controle vira `\u00XX` (§3.1), e
todo campo de texto do envelope é uma fatia disjunta da linha crua — hostname,
app-name, procid, msgid e cada id, nome e valor da structured-data, com a
sintaxe de cada elemento paga pelos bytes da linha que a produziram. O `MSG`,
que é a maior parte de uma linha real, nem entra no envelope. O quilobyte cobre
as chaves e os valores de tamanho fixo; `TestMaxBytesBoundsTheEnvelope` o prova
com linhas adversárias.

No NATS, os headers somam um bloco fixo de até 256 bytes mais o nome do stream
(§3.8); no MySQL, a linha soma ainda até `3 × L` de colunas de texto saneadas e
512 bytes de protocolo e colunas curtas (§10.7.4).

O número é pessimista de propósito, e a consequência é concreta: com os frames
padrão de 16 MiB o maior evento passa de 112 MiB. Quem usa `sink=nats` baixa
`tcp.max_frame_bytes` e `relp.max_frame_bytes` — os compose usam 1 MiB contra um
`max_payload` de 8 MB —, o que continua muito acima de qualquer emissor real
(§5.5); quem usa `sink=mysql` sobe `max_allowed_packet` ou faz o mesmo (§10.7.4
tem a conta). Um fator realista seria mais cômodo e deixaria um único emissor
quebrado derrubar lotes de mensagens confirmadas de todos os outros.

---

## 4. Transportes

Quatro transportes independentes, cada um ligado ou desligado por um parâmetro
de `[listen]` que, vazio, desliga o transporte. **Todos desligados é erro de
configuração** (`exit 1`): um servidor que não escuta nada não tem o que fazer.

| Transporte | Parâmetro | Padrão | RFC |
|---|---|---|---|
| UDP | `listen.udp` | `:1514` | RFC 5426 |
| TCP | `listen.tcp` | `:1514` | RFC 6587 |
| RELP | `listen.relp` | `""` (desligado) | librelp, não é RFC |
| unix datagram | `listen.unix` | `""` (desligado) | — |

### 4.1 As portas padrão não são 514

**Padrão alto de propósito.** A porta canônica do syslog é 514, e abrir uma
porta abaixo de 1024 exige privilégio. A imagem de contêiner de todo o
`loghub-paradise` roda como uid 65532 e não vai deixar de rodar por causa
disto.

A resolução é a publicação:

```yaml
ports:
  - "514:1514/udp"
  - "514:1514/tcp"
```

Quem roda direto no host e quer 514 tem dois caminhos, ambos sem root:

```bash
setcap cap_net_bind_service=+ep /app/go-syslog-server
# ou, para o sistema inteiro:
sysctl net.ipv4.ip_unprivileged_port_start=0
```

O custo aceito é que o número na configuração e o número no fio diferem dentro
do contêiner. É explicitado no log de boot, que imprime os dois.

### 4.2 UDP (RFC 5426)

Um datagrama é **uma** mensagem. Não há moldura: o comprimento do datagrama é o
comprimento da mensagem, e `framing` fica `"datagram"`.

- Buffer de leitura de `listen.udp_read_buffer` (padrão 8 MiB) aplicado com
  `SO_RCVBUF`. **É o parâmetro mais importante do transporte**: o que não couber
  ali é descartado pelo kernel antes de existirmos, e aparece só no contador de
  drops da interface.
- `listen.udp_listeners > 1` abre N sockets com `SO_REUSEPORT`; o kernel
  distribui os datagramas entre eles por hash da 4-tupla. É o único jeito de
  escalar recepção de UDP além de um núcleo.
- Um datagrama acima de 65507 bytes não existe; o kernel trunca e
  `truncated` fica `true`.
- **Não há backpressure.** Ring cheio, sink DOWN, rate-limit ou filtro
  descartam o datagrama e incrementam o contador com a razão. Ninguém foi
  enganado: não houve promessa nenhuma.

### 4.3 unix datagram

Socket `SOCK_DGRAM` em `listen.unix` (tipicamente `/dev/log`). É por onde o
`logger(1)`, a `syslog(3)` da libc e o `journald` com `ForwardToSyslog=yes`
falam.

- O servidor **cria** o socket no boot, com o modo de `listen.unix_mode`
  (padrão `0666`, que é o que `/dev/log` tem em todo sistema — qualquer processo
  precisa poder logar).
- Um socket órfão de uma execução anterior é **removido** antes do `bind`, mas
  só quando o `connect` datagrama a ele é **recusado** (`ECONNREFUSED`: ninguém
  escuta ali). Um `connect` que sucede significa que há outro processo vivo, e
  aí o boot falha com `exit 1` em vez de roubar o `/dev/log` de quem estava
  servindo. Só um **socket** é candidato, conferido sem seguir link simbólico
  (`Lstat`): um arquivo comum, um diretório ou um socket de outro tipo no
  caminho (um `/dev/log` em modo stream responde `EPROTOTYPE`, não recusa) é
  `exit 1` com a mensagem certa, e fica intocado. A regra "o `connect` falhou,
  então remove" apagaria o arquivo de um caminho mal configurado ou o socket
  vivo de outro dono.
- No shutdown o arquivo é removido — só o que este processo criou: um boot
  recusado pela regra acima sai sem tocar no que estava no caminho.
- `peer-addr` fica `""`, `peer-port` `0`, `peer-ip-version` `0`. O rate-limit de
  §8 não se aplica: não há prefixo de rede, e limitar o `/dev/log` local seria
  calar o próprio host.
- **Há backpressure**, e ele é do kernel: quando o buffer de recepção do socket
  enche, o `send` do emissor bloqueia (ou devolve `EAGAIN`). Isso acontece
  **antes** de o datagrama chegar até nós, então não aparece como perda nossa.

### 4.4 TCP (RFC 6587)

Uma conexão carrega N mensagens, separadas por uma das duas molduras de §5. É o
transporte com backpressure real e é o que deveria ser o padrão de qualquer
emissor que se importe com o dado.

- `listen.tcp_listeners > 1` habilita `SO_REUSEPORT`.
- `tcp.max_conns` e `tcp.max_conns_per_ip` (ambos `0` = ilimitado) protegem
  contra exaustão de descritores. Os dois tetos valem **por transporte**,
  somando todos os listeners de `listen.tcp_listeners` — um limitador por
  listener multiplicaria os tetos pelo número de listeners. O teto global
  **bloqueia no accept**, deixando a conexão no backlog do kernel para o
  timeout de conexão do cliente resolver; o teto por IP **fecha
  imediatamente**, porque bloquear estagnaria todo mundo atrás de um emissor
  abusivo. Um `Close` do listener solta um `accept` parado no teto global.
- A recusa por IP fecha a conexão, libera o teto global e **volta ao laço** de
  `accept`, nunca por recursão: um emissor reconectando em laço apertado
  cresceria a pilha um quadro por recusa até o runtime abortar o processo.
- O padrão `0` dos dois tetos é decisão, não esquecimento: o número certo
  depende do parque. O preço é que o custo fixo de cada conexão (§21.1, item 1)
  fica sem teto, e quem expõe a porta fora do host define `max_conns`.
- `tcp.idle_timeout` (padrão 120s) fecha conexão sem tráfego. Generoso de
  propósito: um appliance que loga uma linha por hora é caso normal, e
  reconectar custa mais que manter o socket.
- Sem `keep-alive` de aplicação. O `SO_KEEPALIVE` do sistema operacional é
  ligado, porque uma conexão morta sem `FIN` (NAT que expirou, máquina que
  sumiu) de outro modo ficaria pendurada até o `idle_timeout`. Ele precisa
  chegar ao socket também quando a conexão vem embrulhada pelo limitador dos
  tetos acima; o embrulho é desfeito antes de configurá-lo.

### 4.5 RELP

O protocolo do `rsyslog`. Não é RFC, mas é a **única** forma difundida de fazer
syslog com confirmação por mensagem, e portanto o único transporte em que a
regra de aceitação deste projeto pode ser cumprida ao pé da letra.

#### Moldura

```
TXNR SP COMMAND SP DATALEN [SP DATA] TRAILER
```

- `TXNR` — número de transação, decimal, escolhido pelo cliente. `0` só no
  `serverclose`.
- `COMMAND` — `open`, `syslog`, `close` do cliente; `rsp`, `serverclose` do
  servidor.
- `DATALEN` — comprimento de `DATA` em decimal.
- `TRAILER` — um `\n`.

Quando `DATALEN` é `0`, emissores reais divergem: alguns escrevem `3 close 0\n`
e outros `3 close 0 \n`. O parser aceita os dois — o espaço antes de `DATA` é
opcional quando `DATALEN` é zero. Recusar um dos dois seria recusar metade do
parque instalado por causa de um byte.

#### Diálogo

```
cliente  1 open 71 relp_version=0\nrelp_software=librelp,1.11.0\ncommands=syslog\n
servidor 1 rsp 84 200 OK\nrelp_version=0\nrelp_software=go-syslog-server,<versão>\ncommands=syslog\n

cliente  2 syslog 38 <13>Sep 12 14:33:01 web-01 app: oi
servidor 2 rsp 6 200 OK

cliente  3 close 0
servidor 3 rsp 0
servidor 0 serverclose 0
```

- **O `rsp 200 OK` só sai depois da garantia do ring.** É o `200` do serviço
  irmão, com o mesmo significado e a mesma proibição de sair antes.
- Uma mensagem recusada recebe `500` com a razão em texto (`500 ring full`,
  `500 sink down`). O `rsyslog` retém e retenta, que é exatamente o
  comportamento desejado.
- O cliente mantém uma **janela** de transações não confirmadas
  (`relp.window`, padrão 128, negociada implicitamente pelo comportamento). Ao
  esgotá-la ele para de enviar sozinho: é o backpressure do RELP, e é mais
  limpo que o do TCP porque é explícito no protocolo.
- Confirmações podem sair **fora de ordem** — o `TXNR` está na resposta. Isso é
  o que permite confirmar um lote inteiro de uma vez quando o ring confirma.
- `COMMAND` desconhecido recebe `500` e a conexão continua. Um `DATALEN` que não
  couber em `relp.max_frame_bytes`, ou uma moldura que não fecha, **fecha a
  conexão**: o fluxo está dessincronizado e não há como recuperar.

---

## 5. Moldura no TCP

### 5.1 As duas molduras da RFC 6587

**Octet-counting** (§3.4.1 da RFC) — a correta:

```
38 <13>Sep 12 14:33:01 web-01 app: oi123 <165>1 2026-09-12T...
^^ ^                                     ^^^
comprimento em decimal, um espaço, e exatamente aquele número de bytes.
```

**Non-transparent-framing** (§3.4.2) — a legada, delimitada por `\n`:

```
<13>Sep 12 14:33:01 web-01 app: oi\n
<165>1 2026-09-12T14:33:01Z web-01 nginx - - - texto\n
```

A RFC chama a segunda de "not recommended" e ela é a que todo mundo usa.

### 5.2 Autodetecção

`tcp.framing = auto` (padrão) decide **uma vez por conexão**, pelo primeiro byte
útil:

| Primeiro byte | Decisão | Raciocínio |
|---|---|---|
| `0`–`9` | octet-counting | Uma mensagem syslog nunca começa com dígito; começa com `<`. |
| `<` | LF | É o PRI. Não há comprimento na frente dele. |
| qualquer outro | LF | Lixo ou mensagem sem PRI (§6.3). A moldura LF é a que consegue ressincronizar. |

Decidir **por conexão**, e não por mensagem, é deliberado: um emissor não troca
de moldura no meio do fluxo, e reavaliar a cada mensagem faria uma mensagem
legítima começada por dígito (que só existe se o emissor omitir o PRI) trocar a
moldura e embaralhar o resto do fluxo.

`tcp.framing` pode ser fixado em `octet` ou `lf` quando o operador sabe o que o
emissor faz e não quer a heurística.

### 5.3 Terminadores e casos reais

Na moldura LF:

- O terminador é `\n`. Um `\r` imediatamente antes é removido (emissores
  Windows).
- Um `\0` no fim da mensagem é removido (emissores que seguem o costume do
  BSD). Um `\0` **no meio** é preservado: é dado do cliente, e a linha crua é
  preservada byte a byte.
- Uma linha vazia (`\n` sozinho) é ignorada silenciosamente, sem contador. É
  ruído comum de keep-alive artesanal.
- **O `\n` não faz parte da mensagem** e não entra em `b` nem em `raw`.

Na moldura octet-counting:

- Os `DATALEN` bytes são a mensagem, exatamente. Nenhuma remoção de terminador:
  se o emissor incluiu o `\n` na contagem, ele é dado.
- O separador entre `DATALEN` e a mensagem é **um** espaço.
- Um `DATALEN` não numérico, ou acima de `tcp.max_frame_bytes`, **fecha a
  conexão** com `syslog_messages_dropped_total{reason="malformed_frame"}`. Não
  há como ressincronizar um fluxo cujo comprimento declarado é lixo: qualquer
  tentativa de "procurar o próximo `<`" grava fragmentos como se fossem
  mensagens.

### 5.4 Backpressure: parar de ler

Quando o ring está cheio, ou o sink está DOWN, o leitor do TCP **para de chamar
`Read`**. Não fecha, não descarta, não responde nada.

O efeito é do TCP, não nosso: o buffer de recepção do socket enche, a janela
anunciada vai a zero, e o emissor **para de transmitir**. O `rsyslog` do outro
lado acumula na fila dele, que é exatamente onde o dado deveria ficar enquanto
não podemos aceitá-lo.

```
ring cheio detectado
  -> para de ler
  -> espera por tcp.backpressure_timeout (padrão 30s), checando a cada 50ms
  -> ring escoou?  volta a ler, e nada foi perdido
  -> estourou?     fecha a conexão
```

O fechamento no fim do prazo existe porque um emissor bloqueado para sempre é
pior que um emissor que reconecta: ele segura um descritor, um buffer de kernel
e uma entrada em `tcp.max_conns` indefinidamente, e com mil emissores nessa
situação o servidor não consegue nem aceitar o emissor que *conseguiria*
escoar.

**Isto é o `429` deste protocolo**, e é melhor que ele: o `429` do HTTP depende
de o cliente respeitar `Retry-After`, e a esmagadora maioria não respeita. A
janela do TCP não é opcional.

O mesmo mecanismo vale para o RELP, com uma diferença: lá o servidor *pode*
falar, então em vez de silêncio ele responde `500 ring full` ao esgotar o prazo,
e a conexão continua viva. É estritamente melhor, e é o motivo de RELP ser o
transporte recomendado para quem não pode perder.

### 5.5 Teto de frame

`tcp.max_frame_bytes` e `relp.max_frame_bytes`, ambos com padrão **16 MiB**.

O padrão é alto porque a decisão do projeto é **não limitar o tamanho da
mensagem**: o que o emissor mandar, o servidor aceita. Nenhum emissor real chega
perto de 16 MiB — o `rsyslog` trunca em 8 KiB por padrão e vai no máximo a
64 KiB com `maxMessageSize` folgado.

O teto existe como **fusível, não como política**:

- Na moldura octet-counting o comprimento vem **declarado**. Um `DATALEN` acima
  do teto é recusado **antes de ler um byte do payload**, e a conexão é
  fechada. Abaixo do teto, o buffer **cresce conforme os bytes chegam**, em
  blocos de 64 KiB, e nunca é dimensionado pelo valor declarado: quem declara
  16 MiB e fica calado custa 64 KiB, não 16 MiB (mais o buffer de leitura que
  toda conexão tem; §21.1, item 1). Sem isso cada conexão ociosa
  custaria o teto inteiro durante `idle_timeout`, e com `max_conns` ilimitado
  (o padrão) mil conexões seriam 16 GiB residentes por nove bytes enviados —
  um OOM kill que o `ring.max_bytes` não vê, porque esses bytes ainda não são
  evento. O mesmo vale para o `DATALEN` do RELP (§4.5).
- Na moldura LF não há comprimento declarado. Uma conexão que envia bytes e
  nunca manda `\n` cresceria sem limite: o acumulador é abortado ao passar do
  teto e a conexão é fechada.

Sem o fusível, uma única conexão que jamais envie `\n` aloca até o processo
morrer, e o teto de bytes do ring **não protege** — ele só conta o que já virou
evento, e esses bytes ainda não são um evento.

O parâmetro é exposto no registry para quem tiver um emissor exótico.

---

## 6. Parse

O parser é o **único** código deste projeto que processa entrada adversária
estruturada, e o lugar onde o serviço irmão tinha o `clientip`. Cobertura por
fuzz é obrigatória (§17.2).

**Regra absoluta: o parser nunca falha.** Não existe caminho em que uma mensagem
seja recusada por não ser entendida. O pior resultado possível é
`rfc: "unknown"` com os campos estruturais vazios, o PRI vindo dos padrões e a
linha inteira sendo o `MSG`. Uma mensagem ilegível é exatamente a que o operador
mais precisa ver.

### 6.1 PRI

Roda na **recepção** (§2.2), porque o filtro de §7 depende dele.

```
<  1 a 3 dígitos  >     no início absoluto da mensagem, valor 0..191
```

- Ausente, malformado, acima de 191 ou com zeros à esquerda (`<013>`, que a
  RFC 5424 proíbe): trata-se como ausente. `pri-source` fica `"default"` e os
  valores vêm de `parse.default_facility` / `parse.default_severity`.
- `facility = pri / 8`, `severity = pri % 8`.

| facility | nome | | facility | nome |
|---|---|---|---|---|
| 0 | `kern` | | 12 | `ntp` |
| 1 | `user` | | 13 | `audit` |
| 2 | `mail` | | 14 | `alert` |
| 3 | `daemon` | | 15 | `clock` |
| 4 | `auth` | | 16 | `local0` |
| 5 | `syslog` | | 17 | `local1` |
| 6 | `lpr` | | 18 | `local2` |
| 7 | `news` | | 19 | `local3` |
| 8 | `uucp` | | 20 | `local4` |
| 9 | `cron` | | 21 | `local5` |
| 10 | `authpriv` | | 22 | `local6` |
| 11 | `ftp` | | 23 | `local7` |

| severity | nome | | severity | nome |
|---|---|---|---|---|
| 0 | `emerg` | | 4 | `warning` |
| 1 | `alert` | | 5 | `notice` |
| 2 | `crit` | | 6 | `info` |
| 3 | `err` | | 7 | `debug` |

Os nomes são redundantes com os números, e é a mesma troca deliberada que o
`uts` faz no serviço irmão: ~20 bytes por evento para que nenhum consumidor
precise carregar uma tabela de tradução.

### 6.2 Escolha do formato

Depois do PRI, o byte seguinte decide:

| O que vem depois do `>` | Formato | Por quê |
|---|---|---|
| `1` a `9`, seguido de espaço | **RFC 5424** | É o `VERSION`. A RFC 3164 nunca tem dígito ali: tem nome de mês. |
| Qualquer outra coisa | tentar **RFC 3164** | |
| A tentativa de 3164 falha | **`unknown`** | §6.3 |

`parse.prefer` pode fixar `5424` ou `3164` quando o operador sabe o que recebe e
quer tirar a heurística do caminho. `auto` é o padrão.

### 6.3 RFC 3164

```
<PRI>Mmm dd hh:mm:ss HOSTNAME TAG[PID]: MSG
```

O formato é de 2001 e descreve o que o BSD fazia, não uma gramática. O parser é
tolerante em ordem, e **cada passo que falha simplesmente para**, deixando o
resto da linha como `MSG`:

1. **TIMESTAMP** — `Mmm dd hh:mm:ss`, com `dd` alinhado por espaço (`Oct  1`)
   ou por zero (`Oct 01`). Nome de mês em inglês, três letras,
   case-insensitive. Sem ano e sem fuso (§6.4). **Ou** um timestamp RFC 3339
   completo no mesmo lugar (ver abaixo). **Opcional.**
2. **HOSTNAME** — só é lido **se o timestamp casou**, e só se o token seguinte
   **não** terminar em `:`. Um token que termina em `:` (`sshd:`,
   `systemd[1]:`) é a TAG, e a mensagem não tem hostname.
3. **TAG** — até o primeiro `[`, `:` ou espaço, no máximo 32 bytes (limite da
   RFC). Vira `app-name`. Só é aceita se **começar com letra, `_` ou `/`** e for
   **seguida de `:`**, direto ou depois de `[dígitos]`. A exigência do primeiro
   caractere é o que impede um número de virar nome de programa: o
   `1234:` de sequência do Cisco IOS e o `2026-09-13T10:` de um timestamp não
   reconhecido seriam, de outro modo, TAGs perfeitamente válidas pela regra do
   `:`. Nome de programa não começa com dígito; `/` cobre `/usr/sbin/cron` e
   `postfix/smtpd` já começa com letra.
4. **PID** — o `[dígitos]` entre a TAG e o `:` vira `procid`.
5. O `:` e um espaço opcional encerram o cabeçalho. O que sobra é o `MSG`.

Cada passo é independente: o que falha simplesmente não preenche o campo, e o
`MSG` começa onde o último passo bem-sucedido parou.

> **Sem timestamp, nenhum hostname é adivinhado.** Esta regra substitui uma
> anterior, que abandonava o resto do parse quando o timestamp não casava. Ela
> estava errada, e o caso que a derruba é o mais comum de todos hoje:
> `<14>myapp[123]: algo`, que é o que um contêiner emite. A regra antiga
> jogaria `app-name` e `procid` fora numa mensagem onde os dois estão à vista.
>
> A troca é ler a TAG sem âncora posicional, e o `:` obrigatório é o que a
> torna segura: `<14>Connection closed by 10.0.0.1` não vira TAG nenhuma,
> porque `Connection` não é seguido de `:`. O falso positivo que sobra —
> `<14>error: algo falhou` virando `app-name = "error"` — é inofensivo, e é
> exatamente a forma que a convenção do syslog tem.
>
> Adivinhar o hostname sem o timestamp, ao contrário, não tem salvaguarda
> nenhuma: em `<14>mensagem solta`, a primeira palavra viraria o hostname de um
> host que não existe, e o dado errado é pior que o dado ausente.

Uma mensagem é classificada como `3164` se o timestamp casou, **ou** se a TAG
casou, **ou** se ela tinha um PRI válido — um PRI válido já a torna syslog,
ainda que o corpo seja texto livre. Nos demais casos, `unknown` (§6.6).

#### Dois formatos de campo que a gramática não prevê

Ambos entraram depois que o corpus de mensagens reais (§17.3) os pegou, e ambos
são o formato **padrão** de algo muito comum:

**O `syslog(3)` local não escreve hostname.** A libc e o `journald` com
`ForwardToSyslog=yes` escrevem no `/dev/log` assim:

```
<30>Sep 13 10:00:00 systemd[1]: Started Session 42.
```

Timestamp, e depois direto a TAG. A regra anterior — "depois do timestamp vem o
hostname, posicionalmente certo" — gravava `systemd[1]:` como hostname e deixava
`app-name` vazio, em toda mensagem local de toda máquina Linux. A distinção pelo
`:` final é segura porque um hostname nunca termina em `:`; o endereço IPv6
`2001:db8::1` tem dois-pontos, mas não no fim.

**O rsyslog encaminha 3164 com timestamp RFC 3339.** O template
`RSYSLOG_ForwardFormat`, padrão do `omfwd`, produz:

```
<30>2026-09-13T10:00:00.123456+00:00 web-01 systemd[1]: Started Session 42.
```

Estrutura 3164, timestamp 5424. Sem aceitar isto, o timestamp não casava, nenhum
hostname era lido, e a TAG virava `2026-09-13T10` — o pedaço até o primeiro
`:`. É aceito como timestamp, e como ele tem ano e fuso, `msg-ts-source` fica
`"rfc5424"`: o campo descreve o **formato do timestamp**, e é esse formato que diz
ao consumidor se ano e fuso foram inventados (§6.4).

Variantes toleradas porque existem em campo: `dd` com zero à esquerda, ausência
do espaço depois do `:`, e TAG sem `:` nenhum (aí não há TAG, e o `MSG` começa
onde ela começaria).

### 6.4 Timestamps, e por que nenhum deles é usado

| Formato | O que ele tem | O que falta |
|---|---|---|
| RFC 5424 | RFC 3339 completo, fuso, fração de segundo | a confiança no relógio do emissor |
| RFC 3164 | mês, dia, hora, minuto, segundo | **ano e fuso** |

`msg-ts` é preenchido como string RFC 3339 e `msg-ts-source` diz **em que formato
ele veio** — não qual gramática a mensagem seguiu. Uma linha 3164 com timestamp
RFC 3339, que é o padrão de encaminhamento do rsyslog (§6.3), tem
`rfc: "3164"` e `msg-ts-source: "rfc5424"`, porque ano e fuso vieram do fio e não
foram inventados.

Para a RFC 3164 falta o ano e o fuso, e é preciso inventar os dois para produzir
uma string RFC 3339. A regra é fixa e documentada, não configurável:

- **Fuso: UTC.** Assumir o fuso local do servidor faria o mesmo dado significar
  coisas diferentes em dois nós do mesmo cluster.
- **Ano: o da recepção**, com uma correção de virada. Se o timestamp da mensagem
  cair mais de 24 horas **no futuro** em relação à recepção, usa-se o ano
  anterior. Isso corrige a mensagem de 31 de dezembro que chega em 1º de janeiro,
  que sem a correção saltaria doze meses.

E então **o resultado não é usado para nada**. Não indexa, não ordena, não entra
em `WHERE`. Existe para o operador que quer medir a deriva do relógio dos
emissores, e para quem sabe que seu parque é confiável e quer reprocessar. O
tempo deste servidor é `ts` (§3.1).

### 6.5 RFC 5424

```
<PRI>VERSION SP TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID SP SD [SP MSG]
```

Os seis campos após o `VERSION` são separados por **um** espaço e usam `-`
(NILVALUE) para ausente, que vira `""` no envelope. Os limites da RFC são
aplicados como **truncamento**, nunca como recusa: `HOSTNAME` 255, `APP-NAME`
48, `PROCID` 128, `MSGID` 32.

#### Structured-data

```
[exampleSDID@32473 iut="3" eventSource="App"][origin ip="10.0.0.1"]
```

- `-` significa ausente e vira `{}`.
- O valor é delimitado por aspas, e dentro dele `\"`, `\\` e `\]` são escapes.
  **Nenhum outro escape existe**: um `\n` dentro do valor é uma barra seguida de
  um `n`, literalmente, e é assim que sai no envelope.
- `SD-ID` duplicado: a RFC proíbe, o campo existe. O **último vence**, e o
  contador `syslog_parse_anomalies_total{kind="dup_sd_id"}` sobe.
- Tetos: `parse.sd_max_elements` (32) elementos e `parse.sd_max_params` (64)
  parâmetros por elemento. O excedente é ignorado. Sem eles, um emissor com
  cem mil parâmetros num elemento vira um mapa de cem mil chaves por mensagem,
  em memória, no worker.
- Uma structured-data que não fecha (`[` sem `]`) encerra o parse ali: o que
  veio antes é mantido, e o resto da linha vira `MSG`.

#### BOM

A RFC 5424 permite que o `MSG` comece com `EF BB BF` para declarar UTF-8. O BOM
é **incluído** em `msg-offset`/`msg-size` como qualquer outro byte: a linha crua
é preservada e a spec não promete remover nada dela. O consumidor que quiser
pulá-lo tem três bytes conhecidos para pular.

### 6.6 `unknown`

Uma linha que não é nenhum dos dois formatos vira:

```json
{ "rfc": "unknown", "pri": 13, "pri-source": "default",
  "facility": 1, "severity": 5,
  "hostname": "", "app-name": "", "procid": "", "msgid": "",
  "structured-data": {}, "msg-offset": 0, "msg-size": 47 }
```

Ela **entra**. A própria RFC 3164, em §4.3.3, manda o relay que recebe uma
mensagem sem PRI assumir `user.notice` e repassar — e é exatamente isso que
`parse.default_facility = 1` e `parse.default_severity = 5` fazem.

Recusar seria perder o log do appender mal configurado precisamente no momento
em que alguém está investigando por que ele está mal configurado.
`syslog_parse_total{rfc="unknown"}` sobe e dá ao operador o número exato para
decidir se vale caçar o emissor.

---

## 7. Filtro

Descarte por facility e severidade, **antes do ring**, logo depois do PRI.

### 7.1 Por que ele existe aqui e não existe no serviço irmão

O `go-webhook-server` não filtra nada: toda requisição vira evento. A diferença
não é de gosto, é de posição na rede. Quem recebe webhook **escolhe** quem
manda; quem recebe syslog recebe o que o parque manda, e o parque é dominado por
`debug` e `info` de coisas que ninguém pediu. Sem o filtro, o operador só tem
duas opções: pagar o armazenamento de tudo, ou ir de máquina em máquina ajustar
emissor.

O filtro é recarregável por `SIGHUP` (§15.3) de propósito: é o botão que se
aperta às três da manhã, quando um emissor enlouquece e a decisão é "corta o
debug agora, investiga depois".

### 7.2 Regra

```
aceita  <=>  severity <= filter.min_severity
         E   (filter.facilities vazio  OU  facility ∈ filter.facilities)
```

- `filter.min_severity` (padrão `7`, `debug`) — aceita tudo. A comparação é
  `<=` porque **severidade menor é mais grave**: `min_severity = 4` deixa passar
  `emerg` até `warning` e corta `notice`, `info` e `debug`.
- Aceita nome ou número: `min_severity = warning` e `min_severity = 4` são a
  mesma coisa.
- `filter.facilities` (padrão vazio) — lista de nomes ou números.
  `facilities = local0,local1,auth` mantém só essas três.
- O descarte incrementa `syslog_messages_dropped_total{reason="filter"}`.

**Nada de expressão regular no texto.** Um filtro por conteúdo teria custo por
mensagem imprevisível no caminho quente, e transformaria o serviço numa engine
de regras — o que o consumidor faz melhor e sem estar no caminho do dado.

---

## 8. Rate-limit

Token bucket por prefixo de rede, **idêntico** ao do `go-webhook-server`:
`ratelimit.rate` tokens/s com teto `ratelimit.burst`, chave `/24` para IPv4 e
`/48` para IPv6 (configuráveis), mapa em shards com um mutex por shard, evicção
exata por sweep periódico dos buckets cheios, teto de `ratelimit.max_buckets`
como rede de segurança.

Três diferenças, todas consequência do protocolo:

- **A chave é o peer, sempre.** Não há `X-Forwarded-For`, não há proxy
  confiável, não há `clientip`. O IP que abriu a conexão ou enviou o datagrama é
  o único endereço que existe. O `hostname` dentro da mensagem é alegação do
  emissor e **nunca** vira chave de rate-limit: seria trivialmente forjável e
  envenenaria o limitador de todo mundo.
- **Não se aplica ao transporte `unix`.** Não há prefixo de rede, e limitar o
  `/dev/log` calaria o próprio host.
- **Estourar o limite descarta**, com
  `syslog_messages_dropped_total{reason="ratelimit"}` — no UDP porque não há
  alternativa, e no TCP/RELP porque, ao contrário do ring cheio, um emissor
  acima do limite **não** vai escoar sozinho: segurar a janela dele só transfere
  a fila para o outro lado sem resolver nada.

`ratelimit.enabled = false` no padrão, e pelo mesmo motivo do irmão: um
limitador com resolução de origem errada é pior que nenhum. No boot, com o
rate-limit desligado e algum `listen.*` não-loopback, sai um `WARN` explícito.

`ratelimit.exempt` isenta CIDRs — para o coletor central e para os emissores
conhecidos de alto volume.

---

## 9. Ring

### 9.1 Interface

```go
type Ring interface {
    // Enqueue aceita o item. Não bloqueia por capacidade: se estiver cheio,
    // retorna ErrFull imediatamente. Bloqueia apenas pela garantia do modo.
    Enqueue(ctx context.Context, it *Item) error
    // Dequeue entrega até n itens aos workers de dreno.
    Dequeue(ctx context.Context, n int) ([]*Item, error)
    // Commit informa que os itens foram confirmados no sink.
    Commit(items []*Item) error
    // Drain devolve todos os itens pendentes (usado pelo dump de emergência).
    Drain() []*Item
    Stats() Stats
    Mode() string
    Close() error
}
```

`Enqueue` só retorna `nil` quando a garantia do modo foi cumprida (§2.3). A
confirmação — o `rsp 200 OK` do RELP, ou a retomada da leitura no TCP — acontece
depois desse retorno e nunca antes.

O `Item` carrega o cabeçalho de §3.2 e os bytes crus. **Não carrega envelope**:
ele ainda não existe (§2.2).

### 9.2 Saturação

Dois tetos, avaliados simultaneamente:

- `ring.capacity_items` (padrão 65536)
- `ring.max_bytes` (padrão 512 MiB)

Qualquer um estourando satura o ring, e a consequência depende do transporte
(§2.3): descarte no UDP e no unix, parada de leitura no TCP, `500 ring full`
no RELP. `ring.high_water` (padrão 0.80) é avaliado nos dois e controla
`/readyz`.

#### A contabilidade conta os bytes crus, e só eles

O `go-webhook-server` conta `len(Env) + len(Body)` porque lá os dois existem
quando o item entra no ring. Aqui o envelope não existe ainda, e somá-lo seria
contabilizar memória que ninguém alocou.

O item ocupa `cap(Raw)` mais o tamanho da própria struct. É isso que o ring
conta, e é **mais** exato que a fórmula do irmão, não menos. A memória do
envelope é do worker e tem teto próprio, em §14.3.

Consequência prática para quem dimensiona: com mensagens de 200 bytes, o teto de
itens é quem manda muito antes do de bytes — 65536 × 200 são 13 MiB, longe dos
512 MiB. Os dois tetos continuam necessários porque um emissor de mensagens de
64 KiB inverte a relação na mesma instalação.

### 9.3 `ring.mode = memory`

Canal buffered de `capacity_items`, com um contador atômico de bytes.

`Enqueue` retorna assim que o item entra no canal.

**Garantia:** a mensagem está na RAM **deste nó**. Perda sob crash não-gracioso
(`kill -9`, OOM, pane do kernel). Sob `SIGTERM` a perda é evitada pelo dreno do
shutdown (§12.3); sob falha fatal do sink, pelo arquivo de emergência (§11).

Este é o modo de maior vazão e o padrão.

### 9.4 `ring.mode = sqlite`

Ring persistente, para quem quer **retenção máxima mesmo ao custo de
performance**. Só faz sentido com TCP ou RELP: no UDP, gastar um `fsync` para
garantir uma mensagem que o kernel do emissor já pode ter descartado é pagar
caro por uma garantia que o transporte não dá.

**Garantia:** a mensagem está em disco com `COMMIT` concluído antes da
confirmação. Zero perda sob crash do processo; zero perda sob queda de energia
se `ring.sqlite.synchronous = FULL`.

#### Esquema

```sql
CREATE TABLE IF NOT EXISTS ring (
  seq    INTEGER PRIMARY KEY AUTOINCREMENT,
  id     TEXT NOT NULL,   -- UUIDv7 do item, preservado na releitura
  head   BLOB NOT NULL,   -- cabeçalho de §3.2, JSON
  raw    BLOB NOT NULL,   -- a linha crua
  nbytes INTEGER NOT NULL -- len(raw), idêntico à contabilidade do ring
) STRICT;
CREATE TABLE IF NOT EXISTS quarantine (
  seq    INTEGER PRIMARY KEY, -- o seq que a linha tinha em ring
  id     TEXT    NOT NULL,
  head   BLOB    NOT NULL,
  raw    BLOB    NOT NULL,
  nbytes INTEGER NOT NULL,
  reason TEXT    NOT NULL,    -- o erro de decodificação
  ts     INTEGER NOT NULL     -- instante da quarentena, nanossegundos unix
) STRICT;
```

`nbytes` é `len(raw)` e **não** `len(head) + len(raw)`. A coluna existe para que
o recount do boot (`SUM(nbytes)`) produza exatamente o mesmo número que a
contabilidade em memória produziria (§9.2). Somar o cabeçalho faria o teto de
bytes se comportar de um jeito depois de um crash e de outro antes dele — com o
ring reportando mais ocupação do que o mesmo backlog ocupava na execução
anterior, e o `429` aparecendo mais cedo sem que nada tivesse mudado. O tamanho
do cabeçalho é uma questão de espaço em disco, respondida pelo tamanho do
arquivo.

Linhas são **removidas** após confirmação no sink — esta tabela é uma fila, não
um arquivo. Não confundir com `events` (§3.5), que é destino final e
append-only. O servidor recusa subir se `ring.sqlite.path` e `sink.sqlite.path`
apontarem para o mesmo arquivo.

A coluna `id` é separada do `head` de propósito, mesmo sendo redundante com ele:
perder a identidade do item na releitura já custou, no serviço irmão, o descarte
de 149 de 150 eventos por colisão de chave primária no destino. Uma coluna
própria torna a identidade verificável por `SELECT` sem parsear nada.

**Linhas que não decodificam.** Uma linha cujo `head` não decodifica —
corrupção parcial de página, truncamento, codec de outra versão — é **movida
para a tabela `quarantine`** do mesmo arquivo pela escritora, numa transação
só, e **descontada da contabilidade no ato** pela leitora. Os bytes ficam para
inspeção humana; nada é apagado; o dreno não trava. Deixá-la em `ring` sem
descontar faria o dreno do boot esperar para sempre por um contador que nunca
zera, com todos os transportes fechados e sem código de saída. Cada quarentena
é um `ERROR` no log e conta em `syslog_ring_quarantined_total` (§13.1). O dreno
do boot tem ainda um guarda de não-progresso: uma leitura que não devolve
linhas por 5 s com a contabilidade acima de zero refaz a contagem a partir da
tabela e, se a divergência persistir, sai com código 3 em vez de esperar.
A quarentena não tem teto nem limpeza automática: só cresce com corrupção,
fica fora da contabilidade do ring (o arquivo pode passar de `ring.max_bytes`
por esse tanto), e cabe ao operador inspecioná-la (`SELECT seq, reason,
length(raw) FROM quarantine`) e esvaziá-la (`DELETE FROM quarantine`) com o
processo parado.

#### DSN

```
file:<ring.sqlite.path>
  ?_pragma=journal_mode(WAL)
  &_pragma=synchronous(FULL)
  &_pragma=busy_timeout(5000)
  &_pragma=wal_autocheckpoint(1000)
  &_txlock=immediate
```

#### Concorrência

**Exatamente duas goroutines** tocam o banco:

- **Escritora** — dona da conexão de escrita. Recebe os itens das goroutines de
  recepção, agrupa `INSERT`s e faz um `COMMIT` a cada
  `ring.sqlite.commit_batch` (256) itens **ou** `ring.sqlite.commit_max_delay`
  (5ms), o que vier primeiro. Só depois do `COMMIT` o grupo é liberado para
  confirmar. Também executa os `DELETE` dos lotes já confirmados no sink.
- **Leitora** — conexão separada (o WAL permite leitor concorrente com
  escritor). Avança por **cursor** sobre `seq`, nunca por conjunto de itens em
  voo: um filtro contra o conjunto em voo trava quando `workers × batch` excede
  a janela de leitura, e um cursor que só avança não trava.

Mais de um escritor só geraria `SQLITE_BUSY`: o SQLite serializa escritores por
construção.

**Cancelamento durante o group commit.** Uma vez entregue à escritora, o
`Enqueue` espera o resultado do `COMMIT` mesmo que o contexto de recepção seja
cancelado no meio — conexão fechada, ou a fase 3 do shutdown (§12.3). A espera
é limitada pelo próprio group commit (`commit_max_delay` mais um `fsync`).
Devolver antes teria duas consequências: o transporte responderia
`500 shutdown` (RELP) ou descartaria (TCP) uma mensagem que será commitada de
qualquer forma — duplicata garantida na retransmissão do rsyslog —, e devolveria
ao pool um buffer que a escritora ainda vai ler no `INSERT`, a violação de
§14.2. A única saída antecipada é o próprio ring fechar sem ter recebido o
item, que devolve `ErrClosed`.

#### Custo de latência

`FULL` faz `fsync` a cada `COMMIT`. Em NVMe local o group commit de 5ms domina.
Em armazenamento de rede o `fsync` custa 1–10ms. `ring.sqlite.synchronous`
aceita `FULL` | `NORMAL` | `OFF`; `NORMAL` sobrevive a crash do processo mas não
a queda de energia. O padrão `FULL` é durabilidade sobre latência, deliberado.

#### Boot

O backlog da execução anterior é drenado **integralmente** antes de qualquer
transporte abrir (§12.1).

### 9.5 `ring.mode = none`

Sem buffer. A goroutine de recepção faz o parse completo, monta o envelope e
executa a escrita no sink **de forma síncrona**, confirmando só depois.

- Com `sink=redis`: a confirmação significa que o `XADD` foi aceito pelo Redis.
- Com `sink=sqlite`: que o `COMMIT` no `events.db` terminou.
- Com `sink=postgres` ou `sink=mysql`: que o servidor confirmou o `INSERT`, com a
  durabilidade que **ele** estiver configurado para dar.
- Com `sink=nats`: que o stream devolveu o `PubAck`, com a durabilidade que a
  configuração **do stream** der (§10.8.4).
- Com `sink=none`: nada é escrito.

É o único modo em que o parse completo roda no caminho da recepção (§2.2), e é
aceitável porque este modo já é o lento por definição.

Vazão continua alta com `sink=redis` pelo auto-pipelining do rueidis, que agrupa
chamadas concorrentes de várias goroutines num syscall só, e com `sink=nats`
pela mesma razão: a biblioteca serializa as publicações de todas as goroutines
numa saída única, e cada uma só espera o próprio `PubAck`.

Com `sink=postgres` e `sink=mysql` **não há** coalescência: cada escrita ocupa
uma conexão do pool, e a vazão fica presa a `pool_size / latência de commit`.
Um `max_inflight` acima do pool não amplia nada — as goroutines excedentes
esperam conexão **dentro** do pool, que é uma fila sem contabilidade,
exatamente o que este semáforo existe para impedir. A validação avisa no boot
(§12.1).

**Backpressure:** semáforo de `ring.max_inflight` (padrão 2048) escritas
simultâneas. Ao encher, vale a mesma regra de ring cheio de §2.3.

Sem o semáforo, um sink lento faria o número de goroutines bloqueadas crescer
sem teto, cada uma segurando a linha crua — exatamente o estouro de memória que
o ring existe para evitar.

A escrita síncrona é limitada por `sink.op_timeout`, num contexto **desligado
do cancelamento** da recepção (`context.WithoutCancel`). Uma conexão que cai
no meio, ou o cancelamento da fase 3 do shutdown, não aborta a escrita: o
cliente Redis pode já ter enfileirado o comando e o enviaria de qualquer forma,
e abortar só deixaria a goroutine de recepção devolver ao pool um buffer que o
pipeline ainda vai serializar (§14.2). A escrita termina ou estoura o prazo no
seu próprio relógio; o estouro é tratado como ring cheio (retentável).

`Stats()` reporta `items` = em voo e `capItems` = `max_inflight`, para que as
métricas e o `/readyz` funcionem sem caso especial.

Com `sink=none` o semáforo é dispensado: não há nada que possa demorar.

---

## 10. Sink

### 10.1 Interface

```go
type Sink interface {
    // Write entrega o lote. Retorna erro classificado (ClassNetwork,
    // ClassLogic, ClassFatal) para a máquina de estados de §12.2 decidir.
    Write(ctx context.Context, items []*Item) error
    // Health é chamada periodicamente; define UP/DOWN.
    Health(ctx context.Context) error
    Close() error
    Type() string
}
```

Os workers de dreno (`sink.workers`, padrão 8) são **idênticos** para todos os
sinks:

1. `Dequeue` até `sink.batch` (256) itens, esperando no máximo
   `sink.batch_max_delay` (5ms) para completar o lote.
2. **Para cada item: parse completo (§6) e serialização do envelope (§3.1)**,
   num buffer do pool.
3. `sink.Write(ctx, items)`.
4. Em sucesso: `ring.Commit(items)`, devolve os buffers ao pool,
   `syslog_events_written_total += n`.

O passo 2 é a diferença em relação ao serviço irmão, e é o que tira o parse do
caminho quente. Ele roda **sempre**, inclusive com `sink=none`: sem isso o
benchmark mediria um pipeline que não existe.

### 10.2 Falha de lote

O worker **segura o lote no próprio buffer** e retenta com backoff
(`sink.retry_backoff_min` 100ms → `sink.retry_backoff_max` 2s), **sem puxar mais
nada do ring**. O ring não é modificado: ele para de escoar e enche, até o
backpressure de §2.3 aparecer naturalmente.

Consequências, todas desejadas: nenhum item é perdido, nenhuma reordenação, o
ring pode ser um canal simples, e se o estado virar FATAL esses itens entram no
dump de emergência junto com o ring — desde que o shutdown os colete de
`Workers.Pending()`, porque **um lote retido pelo worker não está no ring**.

O envelope já serializado de um lote retido é **reaproveitado** no retry. O
parse é determinístico e refazê-lo daria o mesmo resultado, gastando CPU
proporcional ao lote a cada tentativa, num momento em que o sistema já está em
dificuldade.

### 10.3 `sink.type = redis`

Destino padrão. Cliente `github.com/redis/rueidis`, auto-pipelining ligado.

Um `XADD` por mensagem, com os três campos de §3.3, enviados como um pipeline
único por lote. A linha crua vai com `rueidis.BinaryString([]byte)`, que
converte `[]byte` em `string` sem cópia; o RESP transporta bulk strings
binary-safe.

Knobs de memória do rueidis expostos: `sink.redis.ring_scale_each_conn`,
`sink.redis.pipeline_multiplex`, `sink.redis.read_buffer_each_conn`,
`sink.redis.write_buffer_each_conn`.

#### O health check pergunta "consigo escrever?", não "você está vivo?"

Duas chamadas num pipeline, a cada `sink.health_interval`:

```
XLEN <sink.redis.stream>
XADD <sink.redis.stream>:healthprobe NOMKSTREAM * p 1
```

Um `PING` não serve, e um `XLEN` sozinho também não. Os dois são **leitura**, e
uma réplica promovida por engano responde a ambos com alegria enquanto recusa
todo `XADD` com `READONLY`. O serviço ficaria preso descartando mensagens com o
sink eternamente UP — a armadilha clássica de health check trivial.

O `XADD` com `NOMKSTREAM` é a sonda de escrita **sem efeito colateral**: se a
chave não existe, o Redis devolve nil e **não a cria**. Mas o comando é marcado
`write denyoom`, então o despacho dele passa pelas mesmas verificações do `XADD`
real e devolve `READONLY` numa réplica, `OOM` num servidor sem memória e
`NOPERM` num ACL insuficiente. Nada é escrito e nada é criado.

O `XLEN` continua no pipeline porque é ele que pergunta pela chave que importa:
se alguém criou `syslog:in` como lista ou como hash, o `WRONGTYPE` aparece aqui,
no boot, em vez de aparecer no primeiro lote.

**Comandos do rueidis são reciclados após executar.** Um retry precisa
**reconstruir** os comandos; reusar o slice corrompe os dados sem erro nenhum.

`sink.redis.maxlen` (padrão `0`, desligado) aplica `MAXLEN ~ N`. Desligado por
padrão porque trimming descarta eventos em silêncio, que é o oposto de tudo o
que esta spec promete.

#### Classificação da resposta do Redis

| Resposta | Classe | Por quê |
|---|---|---|
| `OOM` | **`fatal`** | O servidor está **vivo** e recusando escrita por `maxmemory`. Esperar não conserta; retentar só enche o ring e começa a descartar enquanto nada escoa |
| `WRONGTYPE` | **`fatal`** | A chave da stream é outro tipo. Não muda sozinho, e **todo** lote vai falhar igual |
| `NOPERM`, `NOAUTH`, `WRONGPASS` | **`fatal`** | Credencial ou ACL. Idem: não muda sozinho |
| `LOADING`, `BUSY`, `MASTERDOWN`, `READONLY`, `CLUSTERDOWN`, `TRYAGAIN` | `network` | Vivo mas temporariamente incapaz — réplica em failover, dataset carregando. Retentar é o certo, e se persistir os fusíveis de §12.2 escalam |
| timeout, falha de dial, pipe rompido | `network` | É a conexão |
| qualquer outra | `network` | Descartar por suposição é pior que retentar um lote sem esperança (§19.17) |

> **`WRONGTYPE` e `NOPERM` são `fatal`, não `logic`.** É a mesma regra da classe
> `42` do SQLSTATE em §10.5.7, pelo mesmo motivo: classificados como `logic`,
> descartariam um lote a cada poucos milissegundos, com log de erro, para sempre,
> num processo que todo probe reporta saudável. O `go-webhook-server` adotou a
> mesma tabela em 2026-09-14; as duas mudam juntas.
>
> Uma resposta de classe fatal à sonda de saúde — ou à discagem, já que o
> rueidis autentica dentro do `NewClient` — é FATAL na primeira checagem, sem
> passar pelos fusíveis de §12.2: senha errada encerra o boot na hora, não
> depois de `sink.boot_timeout`.

### 10.4 `sink.type = sqlite`

Destino final append-only no esquema de §3.5.

- **`sink.workers` é forçado a 1**, com `WARN` se o operador pedir mais: o
  SQLite serializa escritores e N workers só produziriam `SQLITE_BUSY`.
- `INSERT OR IGNORE` pela chave `id`: o pipeline é at-least-once por desenho, e
  uma redelivery não pode derrubar o lote.
- Um lote é uma transação. `SQLITE_FULL` e `SQLITE_CORRUPT` são `ClassFatal`;
  `SQLITE_BUSY` é `ClassNetwork` (retry); violação de constraint é
  `ClassLogic`.
- `sink.sqlite.rotate` (`none` | `hourly` | `daily`, avaliado em UTC) fecha o
  arquivo corrente e abre o próximo com sufixo temporal.
  `sink.sqlite.keep_files` (padrão `0`, nunca apaga) limita quantos ficam.
- `Health()` é uma **escrita trivial numa tabela de heartbeat** (`health(id = 1,
  ts)`, `INSERT ... ON CONFLICT DO UPDATE`), sob a mesma trava da escritora:
  custo constante em qualquer tamanho de `events`, e a pergunta certa — "ainda
  consigo escrever?", que é a que um volume desmontado, um arquivo somente
  leitura ou um disco cheio respondem. `PRAGMA quick_check` roda **uma vez, no
  boot**: periódico, ele leria o arquivo inteiro a cada sonda e, com
  `rotate = none`, um dia passaria de `sink.health_timeout`, levando o sink a
  DOWN, a FATAL e a um boot que falha em laço. Arquivo corrompido no boot é
  erro fatal (`exit 3`).

### 10.5 `sink.type = postgres`

Destino final **compartilhado entre instâncias**, esquema de §3.6. Cliente
`github.com/jackc/pgx/v5` pela API nativa, nunca por `database/sql`, porque a
classificação de §10.5.7 precisa do `pgconn.PgError`.

#### 10.5.1 O servidor nunca faz DDL

Nem `CREATE TABLE IF NOT EXISTS`. N instâncias subindo juntas correm no
catálogo, e o papel de produção não tem o privilégio. O esquema é
`docker/postgres/schema.sql`; o servidor **valida** e sai com 3.

#### 10.5.2 Escrita

Um `INSERT` por lote, com `unnest` sobre dezessete arrays paralelos e
`ON CONFLICT (id) DO NOTHING`. Os arrays vêm de um pool e voltam a ele
**zerados**: um slice devolvido cheio prende referências a `Raw` de itens cujos
buffers já voltaram ao pool, e o lote seguinte carregaria a linha de outro
emissor.

#### 10.5.3 Workers

`sink.workers` no default é **rebaixado para 4** com `WARN`; um valor
configurado é respeitado, com `WARN` acima de 16. Cada worker ocupa uma conexão
do pool durante a escrita, e um pool grande contra um PostgreSQL pequeno troca
vazão por contenção de lock no servidor.

`sink.postgres.pool_size` no padrão (`0`) deriva `sink.workers + 1`.

#### 10.5.4 Tamanho do lote

O protocolo do PostgreSQL limita o tamanho de uma mensagem. Com
`sink.batch = 256` e mensagens de syslog isso é irrelevante — 256 × 64 KiB são
16 MiB, folgados. A validação avisa quando o produto teórico passa de 1 GiB.

#### 10.5.5 Validação no boot

A primeira checagem de saúde válida verifica, nesta ordem:

1. a tabela existe e é acessível;
2. as dezessete colunas de §3.6 existem com tipos compatíveis;
3. existe um índice **único** exatamente sobre `(id)`;
4. o papel tem `INSERT`;
5. o servidor não está em recuperação (`pg_is_in_recovery()`), porque uma
   réplica responde a `SELECT 1` e recusa todo `INSERT`.

Qualquer falha estrutural é FATAL **imediato** — `exit 3` sem esperar
`sink.boot_timeout`. Esperar cinco minutos por uma coluna que não vai aparecer
sozinha só atrasa o diagnóstico.

#### 10.5.6 Durabilidade

É a do servidor PostgreSQL, não nossa. Com `synchronous_commit = off` a
confirmação sai antes de o WAL estar em disco, e a garantia de `ring=none` +
`sink=postgres` vira mais fraca do que a tabela de §18 sugere. Está registrado
lá com a ressalva.

#### 10.5.7 Classificação de SQLSTATE

| Classe | Significado | Ação |
|---|---|---|
| `08` | conexão | `ClassNetwork` — retry |
| `53` | recursos esgotados (disco, memória, conexões) | `ClassNetwork` — retry |
| `57` | intervenção do operador (shutdown, cancelamento) | `ClassNetwork` — retry |
| `40` | falha de transação (deadlock, serialização) | `ClassNetwork` — retry |
| `22` | dado inválido (UTF-8, overflow numérico) | `ClassLogic` — descarta o lote e conta |
| `23` | violação de constraint fora do `ON CONFLICT` | `ClassLogic` |
| **`23502`** | `not_null_violation` | **`ClassFatal`** — o servidor nunca envia `NULL`, então ele só nasce da tabela: uma coluna extra `NOT NULL` sem default, um default removido, um trigger. Como `logic`, cada lote seria descartado com o sink UP; é o mesmo caso do `1364` do MySQL (§10.7.6) |
| `42` | **esquema** — tabela, coluna ou índice ausente | **`ClassFatal`** |
| `28` `2F` `42501` | autenticação e privilégio | **`ClassFatal`** |
| `25006` | transação read-only (réplica) | **`ClassFatal`** |
| qualquer outra | desconhecida | `ClassNetwork` — retry visível, nunca descarte silencioso |

**A classe `42` é fatal e nunca `logic`.** Uma tabela dropada classificada como
`logic` descartaria um lote a cada 5ms, com log de erro, para sempre, num
processo que parece saudável. Esta tabela e o teste que a cobre mudam juntos.

### 10.6 `sink.type = none`

Descarta. Existe para medir o custo do pipeline de recepção, moldura, parse e
envelope sem nenhum I/O de destino. **Nunca sai de UP.**

É o único caso em que "aceito" e "descartado" convivem, e é por configuração
explícita do operador.

### 10.7 `sink.type = mysql`

Destino final num MySQL (8.0+) ou MariaDB (10.6+) **compartilhado por todas as
instâncias**, esquema de §3.7. É o irmão do `sink=postgres`: remoto, com
esquema, e o servidor não é dono dele. O que §10.5 diz e não é repetido aqui
vale igual — nunca DDL (§10.5.1), a conta de conexões (§10.5.3), a durabilidade
fora do processo (§10.5.6). Esta seção registra só o que o MySQL muda, e é a
cópia adaptada do §8.6 do `go-webhook-server`, onde a implementação rodou antes
contra servidor real.

#### 10.7.1 Cliente

`github.com/go-sql-driver/mysql` através de `database/sql`. Go puro, sem CGO
(§16.3). Ao contrário do pgx, o driver do MySQL foi desenhado **para**
`database/sql`: não há API nativa a preferir, e o `*mysql.MySQLError` atravessa
a camada intacto, com o `Number` que §10.7.6 exige. A recusa de `database/sql`
em §10.5 é sobre o pgx, não sobre o MySQL.

Sem `interpolateParams`: cada pedaço de lote vai pelo protocolo binário
(prepara, executa e fecha — três idas ao servidor), e `env` e `raw` viajam crus.
Interpolar no cliente mandaria os blobs escapados dentro do texto do comando,
dobrando o pior caso.

O `sql_mode` da **sessão** é fixado em
`STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION`. Fora do modo estrito, um valor
longo demais ou um byte inválido é **truncado com aviso** em vez de recusado, e
uma linha crua cortada seria gravada sem erro nenhum para contar — a violação de
§19.5 que nenhum teste de unidade enxerga. A configuração global do banco não é
tocada.

O `autocommit` da **sessão** é fixado em `1`, no mesmo `SET` de sessão que o
driver envia depois do handshake (e que por isso vence o `init_connect`, que o
servidor roda antes). Com `autocommit=0` global (`my.cnf` ou `init_connect`),
cada `INSERT` ficaria numa transação implícita que ninguém confirma: a escrita
retorna sucesso, a conta de duplicatas fecha, e as linhas somem quando a
conexão fecha — perda silenciosa de mensagens confirmadas, com a sonda do boot
passando. A validação de §10.7.4 relê o valor.

O pool nasce **preguiçoso**: `sql.OpenDB` não conecta, e o `WaitReady` do boot
(§12.1) é quem retenta. O log do driver vai para o `slog` em `WARN`: por padrão
ele escreve em stderr uma linha por conexão que morre, e numa queda isso vira
uma linha por conexão por sonda.

#### 10.7.2 Workers e conexões

Igual a §10.5.3, com o `max_connections` padrão do MySQL em 151:
`sink.workers` no default é **rebaixado para 4** com `WARN`, há `WARN` acima de
16, e `sink.mysql.pool_size` no padrão (`0`) deriva `sink.workers + 1`. Com
`ring=none`, um `ring.max_inflight` acima do pool cria a fila escondida de §9.5,
com `WARN` no boot.

#### 10.7.3 Escrita

Um comando por pedaço de lote, uma linha por mensagem:

```sql
INSERT INTO `syslog`.`events`
  (id, ts, nts, facility, severity, hostname, app_name, procid, msgid,
   peer_ip, transport, rfc, msg_offset, msg_size, env, raw, nbytes)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?), (...), ...
ON DUPLICATE KEY UPDATE id = id
```

`ON DUPLICATE KEY UPDATE id = id` é o `ON CONFLICT (id) DO NOTHING` de §10.5.2:
a atualização é vazia, então uma redelivery não altera nada e não derruba o
lote. **Não é `INSERT IGNORE`**, de propósito: o `IGNORE` rebaixa a aviso também
o `1406` (valor longo demais) e o `1366` (string inválida) e **trunca** o dado em
silêncio.

As linhas afetadas contam as inseridas: o MySQL reporta `1` por linha nova e `0`
por linha existente cujo `UPDATE` não mudou nada (sem `CLIENT_FOUND_ROWS`), então
`len(pedaço) − afetadas` são as duplicatas, em
`syslog_sink_mysql_duplicates_total`.

Dois tetos que o postgres não tem, e por isso o lote é **dividido em pedaços**
antes de ir ao servidor:

- **Placeholders.** O protocolo binário endereça 65 535 parâmetros por comando;
  com dezessete colunas, **3 855 linhas**. A validação recusa `sink.batch` acima
  disso com `exit 1` (§12.1), e o divisor corta ali de qualquer forma.
- **`max_allowed_packet`.** Um comando não pode passar desse limite do servidor
  — 64 MiB no MySQL 8, 16 MiB no MariaDB. O valor é lido na validação do boot
  (§10.7.4), e o lote é cortado em pedaços cujo custo estimado (`env` + `raw` +
  três vezes as colunas de texto + 512 bytes por linha) fique abaixo de 7/8 dele. Antes da
  validação vale o menor padrão, 16 MiB. Reescrever num retry um pedaço já
  gravado não faz nada, graças ao `ON DUPLICATE KEY`.

Uma mensagem que sozinha excede o limite viaja sozinha, e o boot já recusa uma
configuração em que isso seja possível (§10.7.4). Um `1153` em tempo de execução
só acontece se o banco reduzir o limite depois do boot, e nesse caso é `logic`,
logado com os ids.

O texto do comando é montado por pedaço (microssegundos; um cache por número de
linhas guardaria até 3 855 variantes por nada). Os dezessete valores por linha
vêm de um pool por worker que volta **zerado**, pela razão de §10.5.2: os
valores de `env` e `raw` seguram referências aos buffers do lote. Um `env` ou um
`raw` vazio é `[]byte{}`, nunca `nil`, que o driver codifica como `NULL`. As
colunas de texto passam pelo saneamento de §3.7, e `id` é convertido para
dezesseis bytes no cliente.

#### 10.7.4 Validação no boot

Corre uma vez, na primeira checagem de saúde bem-sucedida, com a regra de
§10.5.5: cada recusa é FATAL, o `WaitReady` desiste na hora e o processo sai com
3, com a razão no log.

| Verificação | Como | Se falhar |
|---|---|---|
| A tabela existe, o usuário a enxerga, e o motor é InnoDB | `information_schema.TABLES` | exit 3 |
| As dezessete colunas existem com as famílias de tipo de §3.7; extras toleradas | `information_schema.COLUMNS` | exit 3 |
| Índice único exatamente sobre `(id)` | `information_schema.STATISTICS` (`NON_UNIQUE = 0`, uma coluna, `id`) | exit 3 — sem ele o `ON DUPLICATE KEY` não tem onde colidir |
| A sessão está em `autocommit=1` (um servidor ou proxy pode ignorar o `SET` de §10.7.1) | `SELECT @@session.autocommit` | exit 3 — cada `INSERT` ficaria numa transação que ninguém confirma |
| `max_allowed_packet` comporta a maior linha possível | `SELECT @@max_allowed_packet` contra a conta abaixo | exit 3, com os dois números na mensagem |
| `INSERT` e `UPDATE` concedidos, sessão gravável | um `INSERT ... ON DUPLICATE KEY UPDATE` de sonda com o id nulo (dezesseis zeros) numa transação **revertida** | exit 3 — `1142` é permissão, `1290` é servidor somente leitura |

A conta do pacote: a maior linha tem `L` bytes de `raw`, o maior envelope de
§3.9, e até `3 × L` de colunas de texto — hostname, app-name, procid e msgid são
fatias disjuntas da linha, e o saneamento troca um byte inválido por três. Com
os frames padrão de 16 MiB isso passa de 160 MiB, que 7/8 do pacote precisa
comportar: `max_allowed_packet` de 256 MiB (o valor dos compose), ou frames
menores — com 4 MiB, os 64 MiB padrão do MySQL 8 bastam.

A sonda por transação revertida substitui o `has_table_privilege` do postgres:
no MySQL um privilégio concedido por papel ou por `GRANT ... ON db.*` não
aparece em `information_schema.TABLE_PRIVILEGES`, e a única resposta confiável
para "posso escrever?" é tentar, com **o mesmo comando** do dreno. Foi essa
sonda que revelou, no irmão, uma diferença real para o postgres: o
`ON DUPLICATE KEY UPDATE` exige o privilégio `UPDATE` além de `INSERT`, mesmo com
a atualização vazia. O usuário precisa dos dois; o `UPDATE` pode ficar restrito
à coluna `id` (`GRANT INSERT, UPDATE (id) ON syslog.events TO ...`). É por isso
que InnoDB é exigido: num motor sem transação a sonda ficaria gravada.

A validação **não se repete**. Uma tabela dropada depois do boot aparece como
`1146` na escrita, que é fatal (§10.7.6).

#### 10.7.5 Durabilidade

Como §10.5.6: o servidor não lê nem altera `innodb_flush_log_at_trx_commit`.
Com `1` (padrão) o `INSERT` só retorna depois do `fsync` do redo log, e a
confirmação de `ring=none` significa o que §18 promete; com `2` ou `0`, um crash
**do banco** perde até um segundo de mensagens já confirmadas. A linha de §18.1
leva a mesma marca `†`.

#### 10.7.6 Classificação de erro

O driver entrega o número do erro do servidor (`MySQLError.Number`), e a tabela
abaixo **é** a função `classifyMySQLNumber`; `TestClassifyMySQLNumber` a lê de
volta. As duas regras de §10.5.7 valem: desconhecido é `network`; esquema e
permissão são `fatal`, nunca `logic`.

| `Number` | Situação | Classe |
|---|---|---|
| *(sem número)* | recusa de conexão, EOF, `driver.ErrBadConn`, `mysql.ErrInvalidConn`, prazo | `ClassNetwork` |
| `1040` `1053` `1077` `1927` | conexões demais, servidor encerrando, conexão morta | `ClassNetwork` |
| `1158` `1159` `1160` `1161` | erro ou timeout de rede no meio do comando | `ClassNetwork` |
| `1205` `1213` | espera de lock, deadlock | `ClassNetwork` |
| `1317` `3024` | comando interrompido, `max_execution_time` | `ClassNetwork` |
| `2006` `2013` | servidor foi embora, conexão perdida | `ClassNetwork` |
| **`1290` `1836`** | **servidor somente leitura**: réplica, ou configurado assim | **`ClassFatal`** — a decisão de §10.5.7 sobre `25006`, pelos mesmos motivos |
| `1021` `1030` `1041` `1114` | disco cheio, erro do motor, sem memória, tabela cheia | `ClassFatal` |
| `1044` `1045` `1049` `1130` `1142` `1143` | acesso negado, banco inexistente, host proibido, comando ou coluna negados | `ClassFatal` |
| `1054` `1146` `1364` | coluna desconhecida, tabela inexistente, coluna sem default: contrato de esquema quebrado | `ClassFatal` |
| `1048` `1062` `1153` `1264` `1366` `1406` `1690`, `mysql.ErrPktTooLarge` | `NULL` recusado, duplicata em **outro** índice único, pacote ou valor grande demais, fora de faixa, string inválida | `ClassLogic` |
| qualquer outro | desconhecido | `ClassNetwork` |

`1062` sobre a chave primária é impossível com o `ON DUPLICATE KEY`; a linha
cobre um índice único que o operador tenha acrescentado. Os números são os do
MySQL; o MariaDB compartilha todos os listados.

#### 10.7.7 `Health()`

`SELECT @@read_only`. Um `1` é falha — o servidor está vivo e não é gravável,
uma réplica que um failover pode promover — e o estado vai para DOWN dentro de
um `sink.health_interval`. Um `1290` chegando pela escrita continua fatal
(§10.7.6): é o mesmo par de sinais do postgres, `pg_is_in_recovery()` na sonda e
`25006` na escrita. A sonda também atualiza os gauges do pool.

#### 10.7.8 Métricas

```
syslog_sink_mysql_duplicates_total        counter  linhas deixadas intactas pelo ON DUPLICATE KEY
syslog_sink_mysql_pool_conns{state}       gauge    state: acquired|idle|max
syslog_sink_mysql_pool_waits_total        counter  aquisições que esperaram por conexão
```

Os valores do pool vêm de `sql.DB.Stats()` a cada `Health()`.

#### 10.7.9 TLS

`sink.mysql.tls`, `tls_ca` e `tls_insecure`: verificado contra o pool do sistema
ou contra a CA configurada, com `ServerName` igual ao host. `tls_ca` sem `tls`,
e `tls` com endereço de socket unix, são erro de configuração.

### 10.8 `sink.type = nats`

Destino final num stream **JetStream**, mensagem de §3.8. É o destino que mais
se parece com o Redis: remoto, sem esquema, um consumidor do outro lado — com
uma diferença que decide tudo: o JetStream **confirma** cada publicação com um
`PubAck`, e é essa confirmação que dá à de `ring=none` o significado de §18.
NATS "core", sem JetStream, não confirma nada e não é aceito: o boot exige que o
subject pertença a um stream (§10.8.5). Como §10.7, é a cópia adaptada do §8.7
do irmão.

#### 10.8.1 Cliente e conexão

`github.com/nats-io/nats.go`, com o pacote `jetstream` (a API atual, não o
`nats.JetStreamContext` legado). Go puro, sem CGO (§16.3).

Uma conexão por processo, multiplexada por todos os workers: o cliente
serializa as publicações numa única saída, então `sink.workers` fica no padrão
de 8, como no Redis. A conexão é aberta **preguiçosamente** na primeira checagem
de saúde, e é o `WaitReady` do boot (§12.1) quem retenta. Uma recusa de
autenticação é fatal na hora.

Depois de conectado, o cliente reconecta sozinho, sem limite de tentativas
(`MaxReconnects(-1)`): a máquina de estados de §12.2 já tem os fusíveis que
decidem quando desistir, e um cliente que se fechasse sozinho depois de N
tentativas transformaria uma indisponibilidade longa num estado irrecuperável
sem passar pelo arquivo de emergência. O **buffer de reconexão é desligado**
(`ReconnectBufSize(-1)`): durante uma desconexão, publicar falha na hora com
classe `network` em vez de guardar mensagens dentro do cliente. O ring é o único
buffer deste desenho; uma fila escondida no cliente escaparia da contabilidade,
do backpressure de §2.3 e do dump.

A conexão só entra em `CLOSED` por ação deste processo ou por uma recusa de
autenticação repetida, e desse estado o cliente nunca mais reconecta: `CLOSED`
é fatal (§10.8.6).

#### 10.8.2 Escrita

Publicação **assíncrona em lote**: cada item vira um `PublishMsgAsync`, e só
depois de os N futuros estarem no ar o worker espera as N confirmações, sob o
`sink.op_timeout` do lote. Uma publicação síncrona por item custaria um
round-trip por mensagem por worker — a mesma razão de o Redis usar pipeline.

Três opções que não são as padrões da biblioteca, e são as que importam:

- **`WithExpectStream`**: o header `Nats-Expected-Stream` faz o servidor recusar
  a mensagem se o subject pertencer a outro stream. Um erro de configuração vira
  recusa fatal, não linhas gravadas no lugar errado.
- **`WithRetryAttempts(0)`**: por padrão a biblioteca **retenta sozinha**, em
  segundo plano, uma publicação que recebe "sem respondedores" — relendo
  `msg.Data` depois de o `Write` ter retornado. `msg.Data` é a linha crua do
  item, que §14.2 devolve ao pool depois da confirmação; uma releitura tardia é
  exatamente a corrida que §14.2 proíbe. Com zero tentativas internas, quem
  retenta é o worker (§10.2), com o item ainda dono do buffer.
- **`WithPublishAsyncTimeout(sink.op_timeout)`**: sem ele, um futuro sem resposta
  fica pendente para sempre na tabela interna do cliente, segurando a mensagem
  e o buffer. Até o prazo vencer, vale a contaminação de §14.2: o worker não
  devolve ao pool nada de uma tentativa que terminou pelo relógio.

`WithPublishAsyncMaxPending` é dimensionado em `sink.workers × sink.batch +
ring.max_inflight`, o teto de publicações em voo que este processo consegue
gerar; um teto menor faria `PublishMsgAsync` travar e falhar com "stalled" numa
carga normal.

O `Data` de cada mensagem — envelope seguido da linha crua (§3.8) — é copiado
para **um buffer por lote**, fatiado por mensagem. A cópia não é opcional: a
biblioteca retém a mensagem até a confirmação, e o envelope pertence a um `Prep`
que é liberado quando o `Write` retorna (§14.3), então `Data` precisa ser dono
dos próprios bytes até o último `PubAck`. Um buffer por lote custa uma alocação
onde uma cópia por mensagem custaria N, e é dimensionado antes do laço, para que
nenhum `append` mova bytes para os quais uma mensagem anterior já aponta. Ele
volta a um pool só depois de todos os futuros resolverem; em qualquer caminho de
erro fica para o GC, porque mensagens publicadas antes ainda estão pendentes na
biblioteca. Com `WithRetryAttempts(0)` e sem callback de erro assíncrono, nada
na biblioteca relê o `Data` de uma mensagem já publicada (conferido no fonte de
`jetstream/publish.go` da versão fixada).

#### 10.8.3 Desduplicação e at-least-once

`Nats-Msg-Id` carrega o `id` da mensagem. O stream responde como duplicata uma
segunda publicação com o mesmo id dentro de `duplicate_window` (dois minutos por
padrão; `docker/nats/stream.json` usa dez), com `PubAck.Duplicate = true`, que
conta em `syslog_sink_nats_duplicates_total` — o equivalente do `ON CONFLICT`.
Fora da janela, uma redelivery (um dump de emergência reingerido horas depois)
**duplica** a mensagem: o pipeline é at-least-once por desenho, e o consumidor
do stream tolera o mesmo `id` duas vezes. A janela é do stream, não deste
servidor.

#### 10.8.4 Durabilidade

O `PubAck` significa que o stream **gravou** a mensagem conforme a própria
configuração: `storage: file` e o número de réplicas. Contra a queda do processo
`nats-server`, o file store só sincroniza com o disco a cada `sync_interval`
(dois minutos por padrão): um crash do servidor NATS perde o que estava entre
dois `fsync`, a menos que `sync_always: true` esteja ligado ou que o stream
tenha réplicas. Este servidor não lê nem altera nada disso; a linha de §18.1
leva a marca `†` pela razão de §10.5.6.

#### 10.8.5 Validação no boot

Na primeira checagem de saúde bem-sucedida, com a regra de §10.5.5 (cada recusa
é fatal e o `WaitReady` desiste na hora):

| Verificação | Como | Se falhar |
|---|---|---|
| O stream existe | `js.Stream(stream)` | exit 3 |
| O subject pertence a **este** stream | `js.StreamNameBySubject(subject) == stream` | exit 3 — publicar cairia noutro stream, ou em nenhum |
| O stream aceita publicações | `Sealed = false` | exit 3 |
| O stream guarda o que confirma | `retention` diferente de `interest` (`limits` ou `workqueue`) | exit 3 — com `interest` o servidor responde um `PubAck` normal a uma publicação sem consumidor interessado e não guarda nada: enquanto não houver consumidor, toda mensagem é confirmada, sai do ring e some, com o sink UP |
| O maior evento cabe | `nc.MaxPayload()` e o `max_msg_size` do stream contra o pior caso de §3.9 mais os headers | exit 3, com os números na mensagem |
| O maior registro cabe no file store | com `storage: file`, o mesmo pior caso mais o subject contra os 32 MiB por registro do `nats-server` (`rlBadThresh`), acima dos quais ele responde `10077`, que é fatal | exit 3 — senão um `max_payload` de 64 MB transformaria uma mensagem grande no fim do processo |

O stream é criado por quem administra o servidor NATS, a partir de
`docker/nats/stream.json` (`nats stream add SYSLOG --config stream.json`). Este
servidor nunca cria nem altera um stream, pelos motivos de §10.5.1: N instâncias
subindo juntas, uma conta sem permissão de administração do JetStream, e a
evolução do stream entre versões.

#### 10.8.6 Classificação de erro

Erros da API do JetStream trazem um código numérico (`APIError.ErrorCode`, da
tabela do `nats-server`) e um código HTTP (`APIError.Code`); os da biblioteca
são valores sentinela. A tabela **é** a função `classifyNATSError`, lida de
volta por `TestClassifyNATS`.

| Erro | Situação | Classe |
|---|---|---|
| prazo, cancelamento, `nats.ErrTimeout`, `ErrNoResponders`, `ErrNoServers`, `ErrDisconnected`, `ErrConnectionReconnecting`, `ErrReconnectBufExceeded`, `jetstream.ErrNoStreamResponse`, `ErrAsyncPublishTimeout`, `ErrTooManyStalledMsgs` | sem resposta, ou cliente entre servidores | `ClassNetwork` |
| `nats.ErrAuthorization`, `ErrAuthExpired`, `ErrConnectionClosed` | credencial recusada, conexão fechada em definitivo | `ClassFatal` |
| `jetstream.ErrStreamNotFound` (`10059`), `ErrJetStreamNotEnabled` (`10076`), `ErrJetStreamNotEnabledForAccount` (`10039`) | stream sumiu, JetStream desligado | `ClassFatal` |
| API `10060` stream esperado não confere, `10109` stream selado | contrato de configuração quebrado | `ClassFatal` |
| API `10023` recursos insuficientes, `10077` armazenamento falhou | disco ou limite da conta | `ClassFatal` — coerente com o `OOM` do Redis (§10.3) |
| `nats.ErrMaxPayload`, `jetstream.ErrMaxBytesExceeded`, API `10054` mensagem acima do máximo, API `10097` header acima de 64 KiB | uma mensagem grande demais | `ClassLogic` — inalcançável depois de §10.8.5, salvo mudança no servidor; o `10097` nem isso, porque o bloco de headers deste destino tem tamanho fixo (§3.8), e está na tabela para não cair na regra do 4xx desconhecido |
| API `10158` (`409`) `Nats-Msg-Id` ainda numa proposta Raft, `10167` (`429`) fila de entrada do stream cheia, e todo outro `Code` `429` | a retentativa, logo depois de um prazo, de uma publicação que o stream em cluster ainda não terminou de gravar; contrapressão do servidor | `ClassNetwork` — esperar resolve; como o 4xx desconhecido da linha abaixo, seriam FATAL, dump e `exit 3` ao mesmo tempo em toda instância que publica no stream |
| outro erro da API com `Code` 4xx (fora o `429`) | pedido recusado pela forma: repetiria em toda tentativa | `ClassFatal` |
| outro erro da API com `Code` 5xx | falha do lado do servidor | `ClassNetwork` |
| qualquer outro | desconhecido | `ClassNetwork` |

Uma consequência a registrar: um stream apagado em operação chega à escrita
como "sem respondedores" (`network`, retry), e é o `Health()` seguinte que o
torna fatal, dentro de um intervalo.

#### 10.8.7 `Health()`

Dois passos: o estado da conexão (`CONNECTED` é saúde; `CLOSED` é fatal;
`RECONNECTING` ou `DISCONNECTED` é falha de rede) e um `StreamInfo`, que prova
que o JetStream responde **e** que o stream ainda existe. Um `PING` responderia
feliz com o JetStream desligado, e o estado nunca sairia de UP.

#### 10.8.8 Métricas

```
syslog_sink_nats_duplicates_total         counter  PubAck com Duplicate=true
syslog_sink_nats_reconnects_total         counter  reconexões do cliente
```

#### 10.8.9 Autenticação e TLS

Uma de três formas, e só uma: `sink.nats.username`/`password`,
`sink.nats.token` ou `sink.nats.credentials` (um arquivo `.creds` com JWT e
semente NKey). A biblioteca aceitaria todas ao mesmo tempo e escolheria uma em
silêncio; a validação recusa a ambiguidade (§12.1). Um arquivo de credenciais
que não existe é `exit 1` no boot, não um laço de retry. TLS como §10.7.9:
verificado por padrão, CA própria por `tls_ca`, `tls_insecure` desliga a
verificação.

---

## 11. Arquivo de emergência

**Propriedade do ring, não do sink.** Dispara em qualquer estado **FATAL** ou
com `shutdown.drain_timeout` estourado, com `ring.mode = memory` e ring
não-vazio.

| `ring.mode` | Ação |
|---|---|
| `memory` | **Dump para arquivo de emergência.** |
| `sqlite` | Nada a fazer — os itens já estão em disco e são reingeridos no próximo boot. |
| `none` | Nada a fazer — não há ring. |

### 11.1 Formato

`<emergency.dir>/emg-<node-id>-<unixnano>.bin`

```
<len_head> <len_raw>\n
<bytes do cabeçalho>
<bytes da linha crua>
<len_head> <len_raw>\n
...
```

Cabeçalho de linha em ASCII com os dois comprimentos em decimal, seguido dos
dois blobs crus. **Não é JSON puro justamente para não escapar a linha**, que
pode conter qualquer byte.

Escrito com `O_CREATE|O_EXCL`, seguido de `fsync` no arquivo e no diretório.

O que é despejado é exatamente o par de §3.2: **cabeçalho do item + linha crua**.
Nada mais, porque nada mais é necessário — todo o resto do envelope é derivável
da linha (§2.2). É o mesmo par que o `ring=sqlite` guarda, com o mesmo
codificador.

### 11.2 Reingestão

No boot, todo arquivo `emg-*.bin` em `emergency.dir` é carregado para o ring
**antes** da drenagem do backlog (§12.1), em ordem lexicográfica de nome (que é
temporal pelo sufixo unix), e **removido só depois de todos os seus itens serem
confirmados no sink**.

**Não há metadado a reconstruir.** No `go-webhook-server` este é o ponto de uma
armadilha registrada: o dump guardava envelope pronto, e as colunas consultáveis
do destino vinham de um `ItemMeta` que precisava ser **reconstruído parseando o
JSON escapado** — esquecer disso gravava linhas com `path` vazio e `ts = 0`,
invisíveis exatamente à consulta que aquelas colunas existem para servir, com o
envelope íntegro ao lado escondendo a perda.

Aqui o cabeçalho **é** o metadado e volta inteiro. O item reingerido é
indistinguível do original: mesmo `id`, mesmo instante de recepção, mesmo
transporte, mesmo peer, mesmo PRI efetivo. O parse roda no worker como em
qualquer outro item e produz o mesmo envelope.

Um arquivo corrompido ou truncado (comprimento declarado maior que os bytes
restantes) é processado até o último registro íntegro; o resto é logado em
`ERROR` com a contagem de bytes descartados, e o arquivo é renomeado para
`.corrupt` em vez de removido. O decoder **nunca confia no comprimento
declarado** e precisa tolerar arquivo truncado sem pânico — é um dos alvos de
fuzz obrigatórios (§17.2).

### 11.3 Falha do próprio dump

Se o dump falhar (disco cheio, sem permissão, `emergency.enabled = false`): log
`ERROR` com a contagem exata de mensagens perdidas,
`syslog_events_dropped_total{reason="emergency_failed"} += n`, e saída com
código 3. Essas `n` não entram também em `reason="abort"`: cada perda é contada
uma vez, pela causa.

`emergency.dir` é criado **no boot**, não no instante do dump. Um diretório que
só é criado durante o incidente falha durante o incidente.

---

## 12. Ciclo de vida

### 12.1 Boot

```
1. parse da config (§15); erro ou combinação inválida => exit 1
2. abre a porta ADMIN imediatamente
      /healthz -> 200 (processo vivo)
      /readyz  -> 503 com o motivo em texto
      /metrics -> já exportando
3. cria ring.data_dir e emergency.dir
4. inicializa o SINK
      redis:    conecta, XLEN; retry com backoff 100ms -> 5s
                até sink.boot_timeout (300s); estourou => exit 3
      sqlite:   abre o arquivo corrente, cria esquema e índices
                erro de I/O => exit 3
      postgres: pool preguiçoso; a primeira checagem válida verifica
                tabela, colunas, índice único, INSERT e modo de escrita
                (§10.5.5). Erro FATAL => exit 3 IMEDIATO
      mysql:    pool preguiçoso; a primeira checagem válida verifica
                motor, colunas, índice único, max_allowed_packet e a
                sonda de INSERT revertida (§10.7.4). FATAL => exit 3 IMEDIATO
      nats:     conexão preguiçosa; a primeira checagem válida verifica
                stream, subject, selo e max_payload (§10.8.5).
                FATAL => exit 3 IMEDIATO
      none:     no-op
5. se ring.mode = sqlite: abre ring.db e conta o backlog
   se existem arquivos emg-*.bin: carrega para o ring (§11.2)
6. DRENA o backlog integralmente
      NENHUM transporte está aberto
      /readyz reporta: "draining backlog: 412k pending"
7. abre os transportes configurados, na ordem unix, udp, tcp, relp
8. /readyz -> 200
```

A porta admin aberta durante todo o processo é o ponto: mesmo num crashloop por
sink indisponível, o operador tem `/readyz` com o motivo exato e `/metrics` com
a contagem de backlog.

**Os transportes abrem por último e juntos.** Abrir o UDP antes de o sink estar
pronto descartaria datagramas sem que ninguém soubesse, porque o UDP não tem
como recusar — a única defesa é não estar escutando, e deixar o emissor ver
`ICMP port unreachable`.

A ordem entre eles (unix primeiro, RELP por último) é para que o transporte com
a garantia mais forte seja o último a abrir e o primeiro a fechar: a janela em
que o serviço aceita RELP é um subconjunto da janela em que ele aceita tudo o
mais.

#### Validação de configuração

| Combinação | Efeito |
|---|---|
| Nenhum `listen.*` preenchido | `exit 1` — um servidor que não escuta nada não tem função |
| Dois `listen.*` no mesmo endereço e protocolo | `exit 1` |
| `listen.tcp` e `listen.relp` na mesma porta | `exit 1` — são protocolos diferentes no mesmo socket |
| `listen.unix` relativo, ou mais longo que `sun_path` (107 bytes no Linux, 103 nos BSD e macOS) | `exit 1` — sem a checagem o kernel responde só `bind: invalid argument`, sem dizer o que está errado |
| `ring=sqlite` + `sink=sqlite` com o **mesmo arquivo** | `exit 1` |
| `ring=sqlite` + `sink=sqlite` com arquivos distintos | Permitido, com `WARN`: a mensagem sofre **dois** `fsync` |
| `ring=sqlite` com apenas `listen.udp` configurado | Permitido, com `WARN`: paga durabilidade por um transporte que não a oferece (§9.4) |
| `sink=sqlite` com `sink.workers > 1` | Permitido, com `WARN`; forçado a 1 |
| `sink=postgres` ou `sink=mysql` com `sink.workers` **no default** | Rebaixado para 4, com `WARN`; `WARN` acima de 16 |
| `sink=postgres` ou `sink=mysql` com `pool_size` abaixo de `sink.workers` | `exit 1` — um worker esperaria a conexão de outro |
| `sink=postgres` ou `sink=mysql` com `ring=none` e `ring.max_inflight > pool_size` | Permitido, com `WARN` |
| `sink=postgres` com `table` que não é identificador simples (`nome` ou `esquema.nome`, minúsculos) | `exit 1` |
| `sink=mysql` com `table` que não é identificador simples (`nome` ou `banco.nome`, até 64 caracteres por parte, caixa preservada), `addr`, `username` ou `database` vazios, ou `tls` com socket unix | `exit 1` |
| `sink=mysql` com `sink.batch > 3855` | `exit 1` — 65 535 placeholders, dezessete por linha (§10.7.3) |
| `sink=nats` sem `urls`; `stream` ou algum token do `subject` com espaço, `.`, `*`, `>`, `/` ou `\`; mais de um método de autenticação entre `credentials`, `token` e `username`; `password` sem `username` | `exit 1` (§10.8.9) |
| `sink=redis`, `sink=postgres`, `sink=mysql` ou `sink=nats` com `tls_ca` sem `tls` | `exit 1` |
| `filter.min_severity` fora de `0..7` ou nome desconhecido | `exit 1` |
| `parse.default_facility`/`default_severity` fora da faixa | `exit 1` |
| `ratelimit.enabled=false` + algum `listen.*` não-loopback | Permitido, com `WARN` |
| `listen.unix` sob um caminho fora de `ring.data_dir` | Permitido, sem `WARN` — `/dev/log` é o caso normal e é o **único** caminho de escrita legítimo fora da raiz (§15.6) |
| `GOMEMLIMIT` ausente, ou `< ring.max_bytes * 2` com `ring=memory` | Permitido, com `WARN` (§14.4) |

### 12.2 Saúde do sink

Máquina de estados dirigida **exclusivamente pelo `Health()`** do sink.

```
UP ---- Health() falha (timeout sink.health_timeout) ----> DOWN
DOWN --- Health() sucesso -------------------------------> UP
DOWN --- persiste por sink.fail_timeout (30s)
         OU sink.max_retries (10) checagens falhas ------> FATAL
qualquer --- Write() retorna ClassFatal -----------------> FATAL
```

- `Health()` a cada `sink.health_interval` (padrão 2s). **A latência de detecção
  é de até um intervalo.**
- `ClassNetwork` no `Write()` **não** muda o estado: o item continua no ring e é
  retentado. Ninguém foi confirmado por uma mensagem perdida.
- `ClassLogic` → log `ERROR`, lote descartado e contado.
- `ClassFatal` → FATAL imediato.
- **`sink=none` nunca sai de UP.**

Em **DOWN** vale o backpressure de §2.3 em cada transporte, mesmo com o ring
vazio: é fail-fast, e aceitar para um ring que não vai escoar só adia o
problema. `/readyz` falha no mesmo instante.

Em **FATAL**: executa §11 e sai com código 3.

### 12.3 Shutdown gracioso

`SIGTERM` ou `SIGINT`:

```
fase 1  /readyz passa a falhar
fase 2  espera shutdown.pre_stop_delay (padrão 0s)
        AINDA ACEITANDO tráfego normalmente
fase 3  fecha os transportes na ordem inversa do boot: relp, tcp, udp, unix
        RELP: para de ler; as transações em voo recebem 200 OK ou
              500 shutdown, o escritor esvazia e a conexão fecha
        TCP:  para de ler e fecha
        UDP e unix: fecha o socket; o que chegar depois é ICMP unreachable
        as mensagens já lidas terminam de entrar no ring
fase 4  drena o ring inteiro para o sink
        timeout shutdown.drain_timeout (padrão 30s)
        inclui o que os workers estiverem segurando (Workers.Pending())
fase 5  fecha o sink, fecha o SQLite, remove o socket unix, exit 0
```

**Segundo sinal** em qualquer fase → pula direto para a saída, aceitando a
perda, `exit 130`. **`SIGQUIT`** sempre aborta imediatamente.

Na fase 3 o contexto de recepção é cancelado **antes** de os transportes
fecharem: é o cancelamento que solta um leitor preso no laço de backpressure
(§5.4), e é ele que produz o `500 shutdown` do RELP. Uma mensagem que já foi
entregue ao ring não é afetada por esse cancelamento — o `Enqueue` espera o
group commit (§9.4) ou a escrita síncrona (§9.5) até o fim, e a confirmação sai.
Fechar os transportes primeiro seguraria a fase 3 por até
`backpressure_timeout` em cada leitor preso.

O RELP fecha **sem `serverclose`**: o `Close` só expira o prazo de leitura, e
o `serverclose` sai apenas em resposta a um `close` do cliente (§4.5). Frames
ainda não lidos ficam sem resposta, e o rsyslog, ao ver o fim da conexão,
retransmite o que não foi confirmado. Continuar lendo na fase 3 para responder
`500 shutdown` ao que ainda chegasse contradiria o prazo expirado e atrasaria o
shutdown sem ganho: a mensagem seria retransmitida de qualquer forma.

`shutdown.pre_stop_delay` existe para Kubernetes: a remoção do endpoint no
balanceador é assíncrona e o pod continua recebendo tráfego **depois** do
`SIGTERM`. Vale mais aqui que no serviço irmão — um emissor de UDP não recebe
erro nenhum quando o socket some, ele simplesmente joga fora. Valor sugerido em
K8s: 5–10s.

O `stop_grace_period` do orquestrador precisa ser **maior** que
`pre_stop_delay + drain_timeout`. Menor significa ser morto no meio do dreno,
perdendo mensagens que já foram confirmadas por RELP.

Se o `drain_timeout` estourar com itens ainda no ring, executa §11 e sai com
código 3.

Isso vale para `ring=memory` e `ring=none`. Com `ring=sqlite` não há o que
despejar: as linhas continuam no `ring.db` e o próximo boot as drena, então o
processo registra `WARN`, **não** conta nada em `syslog_events_dropped_total`
e sai com `0` — a menos que o sink esteja FATAL, quando o código continua `3`.
Um segundo sinal com `ring=sqlite` também não perde nada: sai com 130 sem
contar descarte.

### 12.4 Códigos de saída

| Código | Significado |
|---|---|
| `0` | Shutdown gracioso completo: ring vazio, ou `ring=sqlite` com linhas que o próximo boot drena |
| `1` | Erro de configuração ou falha de inicialização não relacionada ao sink |
| `3` | Sink indisponível (FATAL, `boot_timeout` ou `drain_timeout` estourado), ou o guarda de não-progresso do dreno do boot (§9.4) |
| `130` | Abortado por segundo sinal |

---

## 13. Porta administrativa

`admin.bind`, padrão `127.0.0.1:9102`. Loopback por padrão. HTTP/1.1 apenas.

> A porta é `9102`, não `9101`. O `go-webhook-server` usa `9101`, e os dois
> serviços rodam lado a lado com frequência. Um padrão colidente faz o segundo
> a subir falhar com `address already in use` numa porta que não é a que o
> operador estava pensando.

| Path | Semântica |
|---|---|
| `/healthz` | `200` sempre que o processo está vivo. Liveness. |
| `/readyz` | `200` somente se: sink `UP`, ring abaixo de `ring.high_water`, backlog de boot zerado, e não em shutdown. Caso contrário `503` com o motivo em `text/plain`. |
| `/metrics` | Exposição Prometheus em formato texto. |

### 13.1 Métricas

Prefixo `syslog_`. Regra de cardinalidade: **nenhum label deriva de conteúdo
controlado pelo emissor.** Nada de hostname, app-name, tag, facility textual
vinda do fio ou IP como label. Todo label tem conjunto de valores finito e
conhecido em tempo de compilação.

| Métrica | Tipo | Labels |
|---|---|---|
| `syslog_messages_total` | counter | `transport` (udp, tcp, unix, relp) |
| `syslog_messages_accepted_total` | counter | `transport` |
| `syslog_messages_dropped_total` | counter | `reason` (ring_full, sink_down, filter, ratelimit, frame_too_large, malformed_frame, shutdown) |
| `syslog_message_bytes` | histogram | — |
| `syslog_parse_total` | counter | `rfc` (5424, 3164, unknown) |
| `syslog_parse_anomalies_total` | counter | `kind` (dup_sd_id, sd_truncated, sd_unterminated, bad_timestamp) |
| `syslog_conns_active` | gauge | `transport` (tcp, relp) |
| `syslog_conns_total` | counter | `transport` |
| `syslog_backpressure_seconds_total` | counter | `transport` (tcp, relp) |
| `syslog_udp_receive_errors_total` | counter | — |
| `syslog_relp_frames_total` | counter | `command` (open, syslog, close, unknown) |
| `syslog_ring_items` / `_bytes` | gauge | — |
| `syslog_ring_capacity_items` / `_bytes` | gauge | — |
| `syslog_ring_quarantined_total` | counter | — (linhas do `ring.db` movidas para `quarantine`, §9.4) |
| `syslog_sink_batch_size` | histogram | — |
| `syslog_sink_duration_seconds` | histogram | — |
| `syslog_confirm_duration_seconds` | histogram | — (do frame completo até a confirmação; só TCP e RELP, só mensagens aceitas) |
| `syslog_sink_health_duration_seconds` | histogram | — |
| `syslog_sink_errors_total` | counter | `kind` (network, logic, fatal, timeout); uma escrita cortada pelo cancelamento do shutdown não conta |
| `syslog_sink_up` | gauge | — |
| `syslog_sink_sqlite_file_bytes` | gauge | — |
| `syslog_sink_sqlite_rotations_total`, `syslog_sink_sqlite_files_removed_total` | counter | — |
| `syslog_sink_postgres_duplicates_total`, `syslog_sink_mysql_duplicates_total`, `syslog_sink_nats_duplicates_total` | counter | — (redeliveries que o destino reconheceu: §10.5.2, §10.7.3, §10.8.3) |
| `syslog_sink_postgres_pool_conns`, `syslog_sink_mysql_pool_conns` | gauge | `state` (acquired, idle, max) |
| `syslog_sink_postgres_pool_waits_total`, `syslog_sink_mysql_pool_waits_total` | counter | — |
| `syslog_sink_nats_reconnects_total` | counter | — |
| `syslog_parse_duration_seconds` | histogram | — |
| `syslog_events_written_total` | counter | — |
| `syslog_events_dropped_total` | counter | `reason` (sink_logic, emergency_failed, abort) — conjunto **distinto** do de `messages_dropped`: uma série que nunca pode subir não é exposta |
| `syslog_emergency_files` / `_events_pending` | gauge | — |
| `syslog_ratelimit_buckets` | gauge | — |
| `syslog_build_info` | gauge | `version`, `commit`, `go` |
| `syslog_config_info` | gauge | `ring_mode`, `sink_type`, `transports` |

As séries de cada destino são expostas qualquer que seja o `sink.type`, em zero
quando o destino é outro: o conjunto de séries de um `/metrics` não muda com a
configuração, e um painel feito para uma instância serve para as outras.

**`facility` e `severity` não são labels.** É a tentação óbvia — oito
severidades e vinte e quatro facilities são conjuntos finitos, então a regra de
cardinalidade tecnicamente permitiria. Foi recusado porque o produto com
`transport` daria 768 séries por contador e porque a distribuição por
severidade é uma pergunta para o destino, onde ela é uma coluna indexada, não
para o `/metrics`, que é amostrado a cada quinze segundos e não tem retenção.

#### Fronteiras dos histogramas

Estáticas, idênticas em toda instância, **pré-formatadas como string na
inicialização**. Emitir `0.01` num scrape e `0.010` no seguinte cria duas séries
e quebra todo quantil em silêncio.

### 13.2 Logs

`slog`, formato `json` por padrão e `text` disponível.
**Nenhum log síncrono por mensagem no caminho padrão.**

`log.access = true` liga log por mensagem, com `log.access_sample` (padrão 0.01)
de amostragem. Desligado por padrão porque em syslog isto é literalmente logar
log: com 100k mensagens/s, um log por mensagem produz mais volume do que o
serviço ingere.

Os avisos de anomalia de parse e de origem são **amostrados** (um a cada 30s por
tipo), com o contador exato em `/metrics`. Um emissor quebrado não pode inundar
o log do coletor com avisos sobre estar quebrado.

---

## 14. Memória e runtime

### 14.1 Linha crua

Buffers vêm de pools por classe de tamanho: 2 KiB, 8 KiB, 64 KiB, 256 KiB.
Acima de 256 KiB o buffer é alocado direto e **nunca** volta a um pool —
pooling de buffers de pior caso faz o pool segurar o pior caso para sempre.

As classes são menores que as do `go-webhook-server` (4/16/64/256 KiB) porque a
distribuição é outra: a mediana de uma mensagem syslog fica em 150–250 bytes e o
p99 raramente passa de 2 KiB, contra corpos de webhook que começam em
kilobytes.

### 14.2 Regra de propriedade única

**O `Item` é dono do seu buffer. Ele volta ao pool exatamente uma vez, e só
depois da confirmação do sink.** Num retry de lote o item **ainda é o dono** e
nada é devolvido.

Devolver cedo coloca a linha de um emissor dentro do evento de outro. É
silencioso, não aparece sob teste leve, e é o pior bug que este programa pode
ter. A flag de "já devolvido" é atômica.

**Caminhos de erro.** Um `Enqueue` que falha **não** significa que ninguém mais
referencia o item. A regra é por causa do erro:

| Erro do `Enqueue` | Quem ainda pode ler o buffer | O que a recepção faz |
|---|---|---|
| `ErrFull`, `ErrClosed` | ninguém: o ring recusou antes de receber o item | devolve ao pool (`Release`) |
| qualquer outro erro: estouro de `sink.op_timeout`, cancelamento, falha ou recusa do destino (só com `ring=none`) | o pipeline do cliente Redis: o rueidis devolve cedo num contexto cancelado, mas deixa o comando na fila e ainda o envia ("the command is likely sent already") | **descarta sem devolver** (`Discard`): o buffer fica para o GC |

A recepção não distingue a recusa permanente do destino das outras falhas —
ela não enxerga o pacote do sink, por desenho — e por isso descarta em todas.
Nos workers de dreno vale a regra correspondente: uma tentativa que termina em
erro de contexto (`sink.op_timeout` estourado, shutdown) **contamina** o lote, e
nem a linha crua dos itens nem os envelopes daquele `Prep` voltam ao pool — nem
quando a retentativa seguinte tem sucesso. O comando da tentativa anterior pode
ainda estar sendo serializado por outra conexão do pipeline, e devolver o
buffer o corromperia. O custo é uma alocação por item num caminho que já é o de
falha; o caminho normal não muda.

### 14.3 Memória do envelope, que não é do ring

O envelope é construído no worker (§2.2) e vive entre o passo 2 e o passo 4 de
§10.1. O teto é:

```
sink.workers × sink.batch × tamanho máximo de envelope
```

Com os padrões (8 × 256) e um envelope grande de 4 KiB, são 8 MiB. Os buffers de
envelope têm pool próprio, de uma classe só, porque o envelope é pequeno e
limitado pelos tetos de campo da RFC 5424 mais a structured-data, que tem teto
em `parse.sd_max_*`.

Este valor **não** é somado ao `ring.max_bytes` de propósito: são memórias
diferentes, com donos diferentes e ciclos de vida diferentes, e somá-las
esconderia um estouro de uma delas atrás da folga da outra.

### 14.4 Runtime

- `GOMEMLIMIT` deve ser configurado, e o boot emite `WARN` se estiver ausente
  ou, com `ring=memory`, abaixo de `ring.max_bytes × 2`. Com `ring=sqlite` e
  `ring=none` não há número a sugerir: o que fica residente são as linhas em
  voo (`ring.max_inflight × tcp.max_frame_bytes` com `none`;
  `sink.workers × sink.batch × tcp.max_frame_bytes` mais a memória do envelope
  de §14.3 com `sqlite`), tetos de pior caso grandes demais para virar limite —
  o aviso só sai se a variável faltar, e cita o teto. Sem `GOMEMLIMIT` o
  coletor de lixo do Go não sabe do teto do contêiner e o OOM killer chega
  antes do GC. Os compose de `docker/` definem `GOMEMLIMIT` **e** o limite de
  memória do contêiner, o segundo acima do primeiro.
- `GOGC` no padrão. Um valor mais alto troca memória por CPU e só faz sentido
  depois de medir.
- `GOMAXPROCS` no padrão do runtime, que a partir do Go 1.25 respeita o limite
  de CPU do cgroup.

---

## 15. Configuração

### 15.1 Precedência

```
argv  >  env  >  arquivo ini  >  padrão
```

- Flag: `--<seção>.<chave>` — ex.: `--ring.capacity_items=131072`
- Env: `SYSLOG_<SEÇÃO>_<CHAVE>` maiúsculo, `.` vira `_` — ex.:
  `SYSLOG_SINK_REDIS_ADDRS=10.0.0.5:6379`
- INI: seção `[ring]`, chave `capacity_items`
- `--config=/caminho/arquivo.ini` (env `SYSLOG_CONFIG`) indica o arquivo.

### 15.2 Registry único

Uma **única tabela** declara, para cada parâmetro: seção, chave, tipo, valor
padrão, descrição e se é recarregável. Dela são derivados o parser de INI, o
registro de flags, a leitura de env, o `--help` e o dump da configuração
efetiva no boot. É impossível os cinco divergirem.

Sem `gopkg.in/ini.v1` e sem `spf13/pflag`, pela mesma razão do serviço irmão:
unir essas bibliotecas numa precedência coerente daria mais código de cola que o
registry inteiro, e sem o `--help` e o dump gerados da mesma fonte.

### 15.3 Recarga por `SIGHUP`

Recarrega **apenas** o que é seguro trocar a quente:

- `log.level`, `log.access`, `log.access_sample`
- `filter.min_severity`, `filter.facilities`
- `ratelimit.*` (inclusive `enabled`)
- `sink.sqlite.keep_files`

Qualquer outra chave alterada no arquivo é **ignorada** e registrada em `WARN`
nominalmente (`"SIGHUP: this key requires a restart and was ignored" key=ring.capacity_items`).

A comparação usa **duas referências**. Uma chave recarregável é comparada com
o que está **em execução** — o último snapshot aplicado —, para que revertê-la
ao valor de boot seja uma mudança como outra qualquer e seja aplicada: o
rollback do filtro às três da manhã é a recarga que mais importa. Uma chave que
exige reinício é comparada com a configuração **de boot**, com a qual o
processo ainda roda, para que **todo** `SIGHUP` avise sobre ela até o reinício
acontecer, e não só o primeiro. Uma referência única falha num dos dois casos.
Os segredos entram na comparação sem redação; só os nomes das chaves vão para o
log.

Um arquivo que não passa na validação não aplica nada: o `SIGHUP` registra o
erro e o processo segue com o que já rodava. O snapshot em execução só avança
depois de o filtro novo compilar, para que contabilidade e realidade nunca
divirjam.

O filtro ser recarregável é a razão principal de o `SIGHUP` existir aqui (§7.1).

### 15.4 Exemplo completo

```ini
[node]
id = sls-prod-a1

[listen]
udp  = :1514
tcp  = :1514
relp = :20514
unix =

[tcp]
framing = auto
backpressure_timeout = 30s

[filter]
min_severity = info
facilities   =

[ring]
mode           = memory
capacity_items = 65536
max_bytes      = 512MiB
data_dir       = ./data

[sink]
type    = redis
workers = 8
batch   = 256

[sink.redis]
addrs  = 10.0.0.5:6379
stream = syslog:in

[emergency]
enabled = true

[admin]
bind = 127.0.0.1:9102

[log]
level = info
```

O `./data` relativo é o certo quando o binário roda direto na máquina. Em
contêiner os caminhos ficam sob `/data` (§15.6).

### 15.5 Tabela de parâmetros

**Durações zero.** Onde zero não significa "sem limite" — `sink.op_timeout`,
`sink.health_interval`, `sink.health_timeout`, `sink.boot_timeout`,
`sink.retry_backoff_min`, `sink.retry_backoff_max`, `ring.sqlite.commit_max_delay`,
`ring.sqlite.busy_timeout`, `sink.sqlite.busy_timeout`,
`ratelimit.sweep_interval`, `shutdown.drain_timeout`,
`tcp.backpressure_timeout` e `relp.backpressure_timeout` — a validação recusa
`0` com `exit 1`. Um `op_timeout = 0` faria toda escrita falhar no ato e ser
retentada para sempre sem nunca virar FATAL; um `drain_timeout = 0` faria todo
desligamento terminar em dump de emergência e código 3; um
`backpressure_timeout = 0` fecharia toda conexão segurada na primeira
verificação, desligando o backpressure de §5.4 sem aviso; os demais eram
trocados em silêncio por um padrão que o `--dump-config` não mostrava. Zero
continua legítimo onde é o que diz: `shutdown.pre_stop_delay` (sem espera),
`sink.batch_max_delay` (sem espera para completar o lote), `sink.fail_timeout`
(sem fusível por tempo; o FATAL vem só de `sink.max_retries`) e
`tcp.idle_timeout`/`relp.idle_timeout` (sem limite de ociosidade).

#### `[node]`

| Chave | Padrão | Descrição |
|---|---|---|
| `id` | *(hostname)* | Identidade da instância no envelope |

#### `[listen]` — vazio desliga o transporte

| Chave | Padrão | Descrição |
|---|---|---|
| `udp` | `:1514` | Endereço UDP (RFC 5426) |
| `udp_listeners` | `1` | Sockets UDP; `>1` habilita `SO_REUSEPORT` |
| `udp_read_buffer` | `8MiB` | `SO_RCVBUF`; o que não couber o kernel descarta |
| `tcp` | `:1514` | Endereço TCP (RFC 6587) |
| `tcp_listeners` | `1` | Listeners TCP; `>1` habilita `SO_REUSEPORT` |
| `relp` | `""` | Endereço RELP; `:20514` é o costume |
| `relp_listeners` | `1` | |
| `unix` | `""` | Caminho do socket datagram; `/dev/log` é o costume |
| `unix_mode` | `0666` | Modo do socket; qualquer processo precisa poder logar |
| `unix_read_buffer` | `8MiB` | `SO_RCVBUF` do socket unix |

#### `[tcp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `framing` | `auto` | `auto` \| `octet` \| `lf`; decidido uma vez por conexão |
| `max_frame_bytes` | `16MiB` | Fusível contra conexão que nunca fecha a moldura |
| `idle_timeout` | `120s` | Conexão sem tráfego é fechada; `0` = nunca |
| `max_conns` | `0` | `0` = ilimitado; bloqueia no accept |
| `max_conns_per_ip` | `0` | `0` = ilimitado; fecha imediatamente |
| `backpressure_timeout` | `30s` | Quanto tempo parar de ler antes de fechar |

#### `[relp]`

| Chave | Padrão | Descrição |
|---|---|---|
| `max_frame_bytes` | `16MiB` | |
| `idle_timeout` | `120s` | `0` = nunca |
| `max_conns` | `0` | Por transporte, somando os listeners (§4.4); bloqueia no accept |
| `max_conns_per_ip` | `0` | Por transporte; fecha na hora |
| `window` | `128` | Transações não confirmadas antes de parar de ler |
| `backpressure_timeout` | `30s` | Antes de responder `500 ring full` |

#### `[parse]`

| Chave | Padrão | Descrição |
|---|---|---|
| `prefer` | `auto` | `auto` \| `5424` \| `3164` |
| `default_facility` | `1` | Facility quando o PRI está ausente (RFC 3164 §4.3.3) |
| `default_severity` | `5` | Severidade quando o PRI está ausente |
| `sd_max_elements` | `32` | Elementos de structured-data processados |
| `sd_max_params` | `64` | Parâmetros por elemento |

#### `[filter]` — recarregável

| Chave | Padrão | Descrição |
|---|---|---|
| `min_severity` | `7` | Aceita `severity <= isto`; nome ou número |
| `facilities` | `""` | Lista de facilities aceitas; vazio aceita todas |

#### `[ratelimit]` — recarregável

| Chave | Padrão | Descrição |
|---|---|---|
| `enabled` | `false` | Desligado por padrão (§8) |
| `rate` | `1000` | Tokens/s por prefixo de rede |
| `burst` | `2000` | Capacidade do bucket |
| `v4_mask` | `24` | Prefixo IPv4 usado como chave |
| `v6_mask` | `48` | Prefixo IPv6 usado como chave |
| `exempt` | `""` | CIDRs isentos |
| `max_buckets` | `1000000` | Teto; acima dele novas chaves passam sem limite |
| `sweep_interval` | `30s` | Frequência da evicção de buckets cheios |

O `rate` padrão é 1000, dez vezes o do `go-webhook-server`: um host que loga
normalmente já produz dezenas de mensagens por segundo, e um `/24` inteiro
passa fácil de cem.

#### `[ring]`

| Chave | Padrão | Descrição |
|---|---|---|
| `mode` | `memory` | `memory` \| `sqlite` \| `none` |
| `capacity_items` | `65536` | Teto de itens |
| `max_bytes` | `512MiB` | Teto de bytes das linhas cruas |
| `high_water` | `0.80` | Fração acima da qual `/readyz` falha |
| `max_inflight` | `2048` | Escritas síncronas simultâneas com `mode=none` |
| `data_dir` | `./data` | Raiz de tudo que o serviço grava; `/data` em contêiner |

#### `[ring.sqlite]`

| Chave | Padrão | Descrição |
|---|---|---|
| `path` | *(derivado)* | Vazio deriva `<ring.data_dir>/ring.db` |
| `synchronous` | `FULL` | `FULL` \| `NORMAL` \| `OFF` |
| `commit_batch` | `256` | Itens por group commit |
| `commit_max_delay` | `5ms` | Espera máxima antes de commitar grupo parcial |
| `busy_timeout` | `5s` | Janela de retry do `SQLITE_BUSY` |
| `wal_autocheckpoint` | `1000` | Páginas de WAL antes do checkpoint |

#### `[sink]`

| Chave | Padrão | Descrição |
|---|---|---|
| `type` | `redis` | `redis` \| `sqlite` \| `postgres` \| `mysql` \| `nats` \| `none` |
| `workers` | `8` | Goroutines de dreno; 1 com `sqlite`, 4 com `postgres` ou `mysql` |
| `batch` | `256` | Itens por escrita; até 3855 com `mysql` (§10.7.3) |
| `batch_max_delay` | `5ms` | Espera máxima para completar o lote |
| `op_timeout` | `5s` | Prazo de uma escrita |
| `health_interval` | `2s` | Período do health check e latência de detecção |
| `health_timeout` | `1s` | Prazo de um health check |
| `fail_timeout` | `30s` | Quanto tempo DOWN pode durar antes de FATAL |
| `max_retries` | `10` | Health checks falhos antes de FATAL |
| `boot_timeout` | `300s` | Quanto o boot espera o sink antes de sair com 3 |
| `retry_backoff_min` | `100ms` | Backoff inicial em falha de lote |
| `retry_backoff_max` | `2s` | Teto do backoff |

#### `[sink.redis]`

| Chave | Padrão | Descrição |
|---|---|---|
| `addrs` | `127.0.0.1:6379` | Endereços, ou do Sentinel se `sentinel_master` |
| `username` | `""` | Usuário ACL |
| `password` | `""` | Senha ACL; redigida no dump |
| `db` | `0` | Índice do banco |
| `sentinel_master` | `""` | Nome do master; não-vazio seleciona Sentinel |
| `tls` | `false` | TLS para o Redis; nenhum proxy resolve isto |
| `tls_ca` | `""` | CA; vazio usa o pool do sistema |
| `tls_insecure` | `false` | Não verifica certificado |
| `stream` | `syslog:in` | Chave da stream |
| `maxlen` | `0` | `MAXLEN ~ N`; `0` = desligado |
| `ring_scale_each_conn` | `10` | Tamanho do ring do rueidis, potência de dois |
| `pipeline_multiplex` | `0` | Conexões por nó, potência de dois; `-1` usa uma só |
| `read_buffer_each_conn` | `512KiB` | |
| `write_buffer_each_conn` | `512KiB` | |

#### `[sink.sqlite]`

| Chave | Padrão | Descrição |
|---|---|---|
| `path` | *(derivado)* | Vazio deriva `<ring.data_dir>/events.db` |
| `rotate` | `none` | `none` \| `hourly` \| `daily`, em UTC |
| `keep_files` | `0` | Arquivos rotacionados mantidos; `0` nunca apaga — **recarregável** |
| `synchronous` | `FULL` | |
| `busy_timeout` | `5s` | |
| `wal_autocheckpoint` | `1000` | |

#### `[sink.postgres]`

| Chave | Padrão | Descrição |
|---|---|---|
| `addr` | `127.0.0.1:5432` | `host:porta`, ou diretório de socket unix começando com `/` |
| `username` | `syslog` | Papel; precisa de `INSERT` e nada mais |
| `password` | `""` | Redigida no dump |
| `database` | `syslog` | |
| `table` | `events` | `nome` ou `esquema.nome`, minúsculos; nunca criada, só validada |
| `pool_size` | `0` | `0` deriva `sink.workers + 1` |
| `tls` | `false` | |
| `tls_ca` | `""` | Exige `tls=true` |
| `tls_insecure` | `false` | |

#### `[sink.mysql]`

| Chave | Padrão | Descrição |
|---|---|---|
| `addr` | `127.0.0.1:3306` | `host:porta`, ou caminho de socket unix começando com `/` |
| `username` | `syslog` | Usuário; precisa de `INSERT` e `UPDATE` (o `UPDATE` pode ficar só na coluna `id`) |
| `password` | `""` | Redigida no dump |
| `database` | `syslog` | |
| `table` | `events` | `nome` ou `banco.nome`, caixa preservada; nunca criada, só validada |
| `pool_size` | `0` | `0` deriva `sink.workers + 1` |
| `tls` | `false` | |
| `tls_ca` | `""` | Exige `tls=true` |
| `tls_insecure` | `false` | |

#### `[sink.nats]`

| Chave | Padrão | Descrição |
|---|---|---|
| `urls` | `nats://127.0.0.1:4222` | Lista separada por vírgula |
| `username` | `""` | Com `password` |
| `password` | `""` | Redigida no dump |
| `token` | `""` | Redigido no dump |
| `credentials` | `""` | Caminho de um arquivo `.creds` (JWT e semente NKey) |
| `tls` | `false` | |
| `tls_ca` | `""` | Exige `tls=true` |
| `tls_insecure` | `false` | |
| `stream` | `SYSLOG` | Nunca criado, só validado (§10.8.5) |
| `subject` | `syslog.in` | Precisa pertencer ao stream |

#### `[emergency]`

| Chave | Padrão | Descrição |
|---|---|---|
| `enabled` | `true` | Despeja ring `memory` não-vazio em FATAL |
| `dir` | *(derivado)* | Vazio deriva `<ring.data_dir>/emergency` |

#### `[admin]`

| Chave | Padrão | Descrição |
|---|---|---|
| `enabled` | `true` | Serve `/healthz`, `/readyz` e `/metrics` |
| `bind` | `127.0.0.1:9102` | Loopback por padrão; `9102` para não colidir com o irmão |

#### `[log]`

| Chave | Padrão | Descrição |
|---|---|---|
| `level` | `info` | `debug` \| `info` \| `warn` \| `error` — **recarregável** |
| `format` | `json` | `json` \| `text` |
| `access` | `false` | Log por mensagem; desligado porque é logar log — **recarregável** |
| `access_sample` | `0.01` | Fração amostrada — **recarregável** |

#### `[shutdown]`

| Chave | Padrão | Descrição |
|---|---|---|
| `pre_stop_delay` | `0s` | Segue servindo após `SIGTERM` para o LB soltar o endpoint |
| `drain_timeout` | `30s` | Prazo do dreno do ring no desligamento |

### 15.6 Escrita em disco: uma raiz só, e em contêiner ela é `/data`

Os três arquivos que o serviço grava — `ring.db`, `events.db` e
`emergency/` — derivam de `ring.data_dir`, e dentro de um contêiner essa raiz é
`/data`. É regra de **todas** as imagens do `loghub-paradise`, não só desta: os
compose sobem com `read_only: true`, então um caminho de escrita que não derive
dali falha no boot — ou pior, falha no meio de um incidente, se for o
`emergency.dir`.

**A única exceção é `listen.unix`.** Um socket em `/dev/log` está fora da raiz
por definição, e é o ponto do transporte: ele existe para ser encontrado onde a
libc procura. O compose que o usa monta o caminho explicitamente, e a validação
de §12.1 não reclama.

Caminhos fixos do contêiner, iguais em todo o ecossistema: `/app` guarda o
binário (dono `root`), `/data` é a única área gravável, `/etc/syslog` é o INI em
`:ro` e `/tmp` é tmpfs volátil. O `WORKDIR` é `/data` e o binário é invocado
pelo caminho absoluto `/app/go-syslog-server`, nunca pelo nome solto.

---

## 16. Código

### 16.1 Posição no monorepo

```
loghub-paradise/                  monorepo, sem go.mod
  go-webhook-server/              serviço irmão, módulo independente
  go-syslog-server/               ESTE módulo
    go.mod                        .../loghub-paradise/go-syslog-standard-server
    cmd/go-syslog-server/
    internal/
    docs/SPEC.md
    docs/TESTING.md
    docker/
```

Módulo Go independente do irmão. Nada é importado de um para o outro: as
semelhanças são de desenho e de leitura, não de compilação. Compartilhar
`internal/config` faria os dois evoluírem amarrados, e a primeira divergência
necessária — que já existe, no envelope — viraria um parâmetro condicional.

### 16.2 Layout interno

```
cmd/go-syslog-server/main.go   só injeta versão/commit e chama internal/app
internal/
  app/        montagem, ciclo de vida, boot, shutdown, SIGHUP, reingestão
  config/     registry único -> ini + flags + env + --help + dump; validação
  receiver/   udp.go, unix.go, tcp.go, relp.go, framing.go, limits.go
  syslog/     pri.go, rfc3164.go, rfc5424.go, sd.go, names.go   [lógica pura]
  filter/     facility e severidade                             [lógica pura]
  ratelimit/  token bucket com shards e sweep                   [lógica pura]
  envelope/   Item, codec do cabeçalho, envelope JSON, pools    [lógica pura]
  ring/       interface Ring + memory.go, sqlite.go, none.go
  sink/       interface Sink + redis.go, sqlite.go, postgres.go, mysql.go,
              nats.go, none.go, tls.go, workers.go, health.go
  emergency/  dump e reingestão pós-crash
  obs/        métricas Prometheus próprias + slog
```

`internal/` impede import externo acidental. `syslog`, `filter`, `ratelimit` e
`envelope` são lógica pura: testáveis sem rede, sem disco e sem Redis.

Em relação ao irmão: `server/` virou `receiver/`, `clientip/` deixou de existir
(não há header de encaminhamento em syslog) e `syslog/` e `filter/` são novos.
`syslog/` herda o papel que era do `clientip/`: é o único parser de entrada
adversária, e é o alvo de fuzz obrigatório.

### 16.3 Dependências

**Seis em produção, e ponto.**

| Dependência | Para quê | Por que ela |
|---|---|---|
| `github.com/redis/rueidis` | `sink=redis` | Auto-pipelining e `BinaryString` sem cópia |
| `modernc.org/sqlite` | `ring=sqlite`, `sink=sqlite` | Go puro, sem CGO, binário estático |
| `github.com/jackc/pgx/v5` | `sink=postgres` | Go puro; usado pela API nativa, nunca por `database/sql`, porque a tabela de SQLSTATE de §10.5.7 precisa do `pgconn.PgError` |
| `github.com/go-sql-driver/mysql` | `sink=mysql` | Go puro; pelo `database/sql`, que é para onde ele foi desenhado, com o `Number` do erro do servidor que §10.7.6 precisa |
| `github.com/nats-io/nats.go` | `sink=nats` | O cliente oficial, com o pacote `jetstream` |
| `github.com/patrickbrandao/go-loghub-uuid` | UUIDv7 `Level2` | Sem dependência transitiva nenhuma, só stdlib |

**mysql e nats.go: a quinta e a sexta.** Entraram juntas, em 2026-09-15, com o
critério das anteriores e depois de rodarem no `go-webhook-server` nas mesmas
versões: não há alternativa razoável na biblioteca padrão, e as duas são Go
puro.

- **`go-sql-driver/mysql`** é o driver de referência do MySQL em Go, e o único
  com a autenticação `caching_sha2_password` (padrão do MySQL 8) sem CGO. A
  transitiva é `filippo.io/edwards25519`, a curva da troca de chave da
  autenticação: sem rede, sem disco.
- **`nats-io/nats.go`** é o cliente oficial. O protocolo NATS é simples; o
  JetStream em cima dele — publicação assíncrona com futuros, headers,
  desduplicação, stream esperado, reconexão com estado — não é, e reescrevê-lo
  seria a decisão errada pelo motivo de sempre. Transitivas: `nkeys` e `nuid`
  (do próprio projeto NATS), `klauspost/compress` (compressão do transporte
  websocket, que este servidor não usa) e `golang.org/x/crypto`. Nenhuma abre
  socket por conta própria, escreve em disco ou lê `TMPDIR`: a regra de §15.6
  continua verdadeira.

`github.com/google/uuid` é importado pelo código deste projeto **só em teste**,
como oráculo independente do formato UUIDv7. Ele aparece no binário mesmo assim,
e não é por nossa causa: o `modernc.org/libc`, que o driver SQLite puxa, o importa.
"Seis dependências" conta o que **este** código escolheu; `go version -m` no
binário lista mais, e é essa lista que uma auditoria de supply chain deve ler.

**O parser de syslog é próprio, e isso é decisão, não omissão.** Existem
bibliotecas boas em Go. Nenhuma foi adotada por três razões concretas:

1. **Elas falham.** Retornam erro numa mensagem que não casa com a gramática, e
   a regra desta spec (§6) é que o parser nunca falha. Envolver uma biblioteca
   num `if err != nil { fallback }` significa pagar o parse duas vezes no caso
   que mais importa.
2. **Elas alocam strings.** O caminho deste projeto fatia os bytes crus e
   registra deslocamentos; um parser que devolve `struct` com `string` copia
   cada campo, por mensagem, para sempre.
3. **É o único código de entrada adversária.** Ele precisa de fuzz próprio e de
   corpus próprio de qualquer forma (§17.2). Com isso feito, a biblioteca
   economizaria trezentas linhas e traria uma superfície que não controlamos.

**Ids: a entropia não vem do gerador padrão da lib.** `internal/envelope/id.go`
monta o gerador com `NewGeneratorWith(rand.Uint64)` de propósito: o
`NewGenerator()` da biblioteca usa PCG (previsível) e, se `crypto/rand` falhar,
degrada em silêncio para uma semente de relógio. Trocar por `NewGenerator()` ou
por `GenerateString` direto quebra duas propriedades documentadas sem que teste
nenhum reclame.

Config, métricas e exposição Prometheus seguem sendo implementação própria, por
decisão medida.

**Sem CGO.** `CGO_ENABLED=0` é premissa do Dockerfile e do binário estático.

### 16.4 Convenções

**Idioma.** Código, identificadores e comentários em **inglês**. Documentação e
conversa com o usuário em **português**. Não misture dentro de um arquivo.

**Comentários explicam o porquê, não o quê.** O padrão é um comentário que
justifica a decisão e diz o que aconteceria com a alternativa rejeitada.

**Configuração: o registry é fonte única.** Um parâmetro novo entra em
`internal/config/registry.go` e daí saem sozinhos o INI, as flags, o env, o
`--help`, o dump e a validação. Nunca um caminho paralelo.

**Métricas: cardinalidade fixa.** Nenhum label deriva de conteúdo do emissor.

---

## 17. Testes

### 17.1 Unitários

Cobertura real medida com `-coverpkg=./internal/...`, porque cobertura por
pacote engana: a suíte de `app` exercita `receiver`, `ring` e `sink` sem que
isso lhes seja atribuído.

```bash
gofmt -l . && go vet ./... && go test -race ./...
```

O `-race` **não é opcional**: os bugs de perda de dados deste desenho vivem em
caminhos concorrentes.

### 17.2 Fuzz — obrigatório

Quatro alvos, 10 minutos cada antes de qualquer release:

| Alvo | Pacote | O que ele protege |
|---|---|---|
| `FuzzParse` | `syslog` | O parser completo. **Propriedade invariante: nunca entra em pânico, e `msg-offset + msg-size == len(raw)` sempre.** |
| `FuzzSD` | `syslog` | Structured-data, que tem escaping próprio e aninhamento |
| `FuzzFrame` | `receiver` | As duas molduras da RFC 6587 e a moldura RELP |
| `FuzzRead` | `emergency` | O decoder, que precisa tolerar arquivo truncado sem pânico |

```bash
go test ./internal/syslog/    -run=XXX -fuzz=FuzzParse -fuzztime=60s
go test ./internal/syslog/    -run=XXX -fuzz=FuzzSD    -fuzztime=60s
go test ./internal/receiver/  -run=XXX -fuzz=FuzzFrame -fuzztime=60s
go test ./internal/emergency/ -run=XXX -fuzz=FuzzRead  -fuzztime=60s
```

### 17.3 Corpus de mensagens reais

`internal/syslog/testdata/corpus/` guarda mensagens **capturadas de emissores
reais**, não inventadas: `rsyslog`, `syslog-ng`, `journald`, `logger(1)`, Cisco
IOS, pfSense, HAProxy, nginx, Kubernetes, Windows via agente.

Cada arquivo tem a linha crua e o envelope esperado ao lado. É o que impede que
uma "melhoria" do parser quebre em silêncio um emissor que ninguém tem à mão.

Mensagens inventadas cobrem a gramática; mensagens reais cobrem o que os
fabricantes fazem com ela, que é outra coisa.

### 17.4 Integração com emissores vivos

A prova de interoperabilidade que o corpus **não** dá: a moldura no fio.

```bash
docker run --rm -d --name sls-test-rsyslog rsyslog/syslog_appliance_alpine
docker run --rm -d --name sls-test-syslogng balabit/syslog-ng
```

Cenários obrigatórios, cada um contra o servidor real:

1. `rsyslog` com `omfwd` UDP.
2. `rsyslog` com `omfwd` TCP, moldura **octet-counting** (`TCP_Framing="octet-counted"`).
3. `rsyslog` com `omfwd` TCP, moldura **LF** (padrão).
4. `rsyslog` com `omrelp`, incluindo **corte do servidor no meio do fluxo** e
   verificação de que ele reenvia tudo que não foi confirmado.
5. `syslog-ng` com `syslog()` (RFC 5424) e com `network()` (RFC 3164).
6. `logger -u <socket>` no transporte unix.

O cenário 4 é o único que prova a promessa central deste projeto num transporte
que pode prová-la. Ele reconcilia **três números**: mensagens que o emissor diz
ter enviado, confirmações que o servidor diz ter dado, e linhas no destino.

### 17.5 Integração com destinos

A suíte contra Redis e PostgreSQL reais **ainda não existe** (`docs/TESTING.md`
§8, itens 1 e 2); os comandos abaixo são o formato combinado com o serviço
irmão, para quando ela for escrita. O servidor **nunca cria a tabela** do
PostgreSQL.

```bash
docker run --rm -d -p 16379:6379 --name sls-test-redis redis:7-alpine
SYSLOG_TEST_REDIS_ADDR=127.0.0.1:16379 go test -race -timeout 600s ./internal/app/

docker run --rm -d -p 15432:5432 -e POSTGRES_USER=sls -e POSTGRES_PASSWORD=sls \
  -e POSTGRES_DB=sls_test --name sls-test-pg postgres:17-alpine
SYSLOG_TEST_PG_DSN='postgres://sls:sls@127.0.0.1:15432/sls_test?sslmode=disable' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

A de MySQL e a de NATS existem desde a entrada dos dois destinos, pulam sem a
variável e criam a própria tabela ou o próprio stream **a partir do arquivo que o
operador aplica** (`docker/mysql/schema.sql`, `docker/nats/stream.json`), para
que o esquema testado seja o esquema entregue. O `nats-server` é um módulo Go, e
nem Docker é preciso para ele:

```bash
docker run --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=sls -e MYSQL_DATABASE=sls_test \
  --name sls-test-mysql mysql:8.4 --max_allowed_packet=256M
SYSLOG_TEST_MYSQL_DSN='root:sls@tcp(127.0.0.1:13306)/sls_test' \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/

go run github.com/nats-io/nats-server/v2@v2.14.6 -js -sd "$(mktemp -d)" -p 14222 &
SYSLOG_TEST_NATS_URL=nats://127.0.0.1:14222 \
  go test -race -timeout 600s ./internal/sink/ ./internal/app/
```

O usuário do MySQL de teste precisa de `CREATE TABLE` e, para o subteste de
privilégio, de `CREATE USER`. A suíte de NATS exige JetStream ligado e baixa os
frames para caber no `max_payload` padrão de 1 MiB, porque o boot recusa, de
propósito, um teto que não caiba (§3.9).

O `natsio/nats-box` do serviço `nats-init` dos compose de NATS, que cria o
stream a partir de `docker/nats/stream.json`, tem versão fixa (`0.20.0`, a dos
módulos irmãos), no compose e no `.env.example`: com `latest` a mesma receita
instalaria CLIs diferentes em dias diferentes, e o stream criado poderia mudar
sem mudança no repositório. `TestComposeNATSBoxIsPinned` confere.

### 17.6 Carga

Gerador próprio, não `logger` em laço. Mede, por combinação `ring` × `sink` e
por transporte: mensagens/s, p50/p99 de confirmação onde ela existe, CPU por
mensagem, bytes residentes e **perda reconciliada**.

O UDP precisa de uma medição que os outros não precisam: **a taxa em que o
kernel começa a descartar**, lida de `/proc/net/udp` e do contador da interface.
É o teto real do transporte e não aparece em métrica nenhuma nossa.

### 17.7 Critérios de aceite v1

**Nunca foram medidos.** Ficam registrados como alvo, não como fato, e a
primeira medição substitui este parágrafo por números.

| Cenário | Alvo |
|---|---|
| UDP, `memory`+`redis`, mensagens de 200 B | ≥ 200k msg/s por núcleo de recepção |
| TCP LF, `memory`+`redis` | ≥ 150k msg/s |
| RELP, `memory`+`redis` | ≥ 80k msg/s (o ack custa) |
| `sqlite`+`redis`, TCP | ≥ 20k msg/s, p99 de confirmação < 20ms |
| `none`+`postgres`, TCP | vazão = `pool_size / latência de commit` |
| Perda com `SIGTERM` durante carga, RELP | **zero**, reconciliada |
| Perda com FATAL do sink, `memory`, RELP | **zero**, via dump e reingestão |

---

## 18. Matriz de garantias

### 18.1 O que a confirmação significa

Leia junto com §2.3: a coluna só existe nos transportes que confirmam.

| `ring` | `sink` | A confirmação significa | Perda em `kill -9` |
|---|---|---|---|
| `memory` | `redis` | aceito na RAM deste nó | backlog do ring, mitigado pelo dump |
| `memory` | `sqlite` | aceito na RAM deste nó | idem |
| `memory` | `postgres` | aceito na RAM deste nó | idem |
| `memory` | `mysql` | aceito na RAM deste nó | idem |
| `memory` | `nats` | aceito na RAM deste nó | idem |
| `memory` | `none` | **descartado** | tudo — benchmark |
| `sqlite` | `redis` | gravado com `fsync` no `ring.db` | nada |
| `sqlite` | `sqlite` | gravado com `fsync` no `ring.db` | nada — mas paga **dois** `fsync` |
| `sqlite` | `postgres` | gravado com `fsync` no `ring.db` | nada |
| `sqlite` | `mysql` | gravado com `fsync` no `ring.db` | nada |
| `sqlite` | `nats` | gravado com `fsync` no `ring.db` | nada |
| `sqlite` | `none` | **descartado** após o `fsync` | tudo — benchmark |
| `none` | `redis` | `XADD` confirmado pelo Redis | nada |
| `none` | `sqlite` | `COMMIT` no `events.db` concluído | nada |
| `none` | `postgres` | `INSERT` confirmado pelo servidor † | nada † |
| `none` | `mysql` | `INSERT` confirmado pelo servidor † | nada † |
| `none` | `nats` | `PubAck` do stream JetStream † | nada † |
| `none` | `none` | **descartado** | tudo — benchmark puro de transporte |

† **As únicas linhas cuja durabilidade depende de configuração fora do
processo.** Com `synchronous_commit = off` no PostgreSQL (§10.5.6), ou
`innodb_flush_log_at_trx_commit` em `0` ou `2` no MySQL (§10.7.5), a
confirmação sai antes de o log estar em disco; no NATS, o file store só
sincroniza a cada `sync_interval`, a menos que `sync_always` esteja ligado ou o
stream tenha réplicas (§10.8.4).

### 18.2 E o que ela significa por transporte

| Transporte | A confirmação existe? | O que a substitui |
|---|---|---|
| RELP | **sim**, `rsp 200 OK` | — |
| TCP | parcialmente: a retomada da leitura | nada é perdido, mas o emissor não sabe *qual* mensagem foi aceita |
| unix dgram | não | o bloqueio do `send` pelo kernel |
| UDP | não | `syslog_messages_dropped_total` e os contadores da interface |

**Só o RELP cumpre a promessa deste projeto ao pé da letra.** Está dito aqui, na
tabela, e não escondido numa nota de rodapé: quem não pode perder mensagem usa
RELP, e quem usa UDP escolheu um transporte que perde.

### 18.3 Custo relativo

| Combinação | CPU | Disco | Latência de confirmação |
|---|---|---|---|
| `memory` + `redis` | mais baixo | nenhum | microssegundos |
| `memory` + `nats` | baixo | nenhum local | microssegundos |
| `memory` + `postgres` ou `mysql` | baixo | nenhum local | microssegundos |
| `memory` + `sqlite` | baixo | um `fsync` por lote | microssegundos |
| `none` + `redis` ou `nats` | baixo | nenhum | um round-trip |
| `sqlite` + qualquer | alto | um `fsync` por grupo | 5–40ms |
| `none` + `postgres` ou `mysql` | médio | nenhum local | uma latência de commit |

---

## 19. Invariantes

Se uma alteração conflita com um destes, ela está errada.

1. A confirmação nunca sai antes de a garantia da combinação `ring` × `sink` ser
   cumprida — e onde não há confirmação, há contador.
2. Ring cheio nunca vira aceitação seguida de descarte, exceto com `sink=none`.
   No UDP e no unix descarta antes de aceitar; no TCP e no RELP segura o emissor.
3. Um buffer volta ao pool **uma vez**, e só após confirmação do sink. Num retry
   o item ainda é o dono.
4. Item aceito só sai do ring após confirmação do sink; nunca é descartado para
   liberar espaço.
5. **A linha crua é preservada byte a byte.** O parser lê e nunca modifica; a
   moldura do transporte é removida, e nada mais.
6. O parser **nunca falha**. O pior resultado é `rfc: "unknown"`.
7. Nenhum sink parseia, decodifica, valida ou altera a linha crua. O saneamento
   de UTF-8 do `sink=postgres` acontece **na cópia** que vira coluna, nunca em
   `raw` nem em `env`.
8. `peer-addr` sempre registra a ponta real do transporte. `hostname` é sempre
   alegação do emissor, e nunca vira chave de rate-limit, de particionamento ou
   de qualquer decisão do servidor.
9. Nenhum label de métrica deriva de conteúdo controlado pelo emissor.
10. Nenhum log síncrono por mensagem no caminho padrão.
11. Um comando do cliente Redis nunca é reusado após execução; um retry
    **reconstrói** os comandos.
12. O envelope é idêntico em todos os sinks. As colunas promovidas são cópias
    para consulta.
13. **O envelope é derivável do cabeçalho do item mais a linha crua**, sempre e
    de forma determinística. É o que torna a reingestão exata.
14. `ring` e `sink` são ortogonais. Nenhuma implementação de um consulta o tipo
    do outro.
15. Toda escrita em disco fica sob `ring.data_dir` — e em contêiner esse
    diretório é `/data`. A única exceção é `listen.unix`, que é leitura, não
    escrita de dado.
16. Nenhum destino compartilhado cria o que valida: o `sink=postgres` e o
    `sink=mysql` nunca fazem DDL, nem `CREATE TABLE IF NOT EXISTS`, e o
    `sink=nats` nunca cria nem altera um stream.
17. Classe `42` do SQLSTATE é `fatal`, nunca `logic`, e o mesmo vale para os
    números de esquema e permissão do MySQL (`1054`, `1146`, `1142`…). Erro
    desconhecido é `network`, nunca `logic`.
18. Nenhuma biblioteca cliente guarda nem retenta por conta própria o que o ring
    guarda e o worker retenta: o `nats.go` publica com `WithRetryAttempts(0)` e
    conecta com `ReconnectBufSize(-1)`. Uma fila escondida no cliente escapa da
    contabilidade e do dump; uma retentativa interna relê um buffer que já pode
    ter voltado ao pool.
19. Nenhum destino recebe uma mensagem que o teto dele recusaria: o boot compara
    `max_allowed_packet` e `max_payload` com o maior evento que a configuração
    permite aceitar (§3.9), e não abre os transportes se ele não couber.

---

## 20. Roteiro pós-v1

Nada disto entra antes de a v1 estar em produção e os critérios de §17.7 terem
sido medidos pela primeira vez.

| Item | Motivo de ter ficado fora |
|---|---|
| `sink=syslog` — encaminhar para outro servidor | É o destino mais natural do mundo syslog e transforma o serviço num agregador encadeável. Ficou fora porque precisa de política de retry própria e de uma decisão sobre reescrever ou não o `hostname` |
| TLS na ingestão (RFC 5425) | Decisão explícita da v1 (§1.2) |
| Roteamento por facility para destinos distintos | Exige N sinks vivos ao mesmo tempo, o que muda a máquina de saúde de um estado para N |
| Particionamento por tempo no `sink=postgres` e no `sink=mysql` | Volume de syslog pede isto mais cedo que webhook; precisa de decisão sobre quem cria a partição |
| `ring=redis` — fila compartilhada entre instâncias | Muda a premissa de instâncias independentes |
| Deduplicação e `last message repeated N times` | Estado por emissor no caminho quente |

---

## 21. Lacunas e riscos

Este módulo é **referência**: o código fica como está (salvo os seis itens de
§21.1 corrigidos em 2026-10-14 por porte do `go-syslog-server`), e quem porta
um trecho dele para outro módulo confere esta seção antes. As lacunas de teste estão em
`docs/TESTING.md` §8.

### 21.1 Pendências conhecidas

Defeitos reais que o código ainda tem, cada um com a direção da correção. Um
trecho portado leva a correção junto, não o defeito.

1. **Custo fixo por conexão.** Cada conexão TCP ou RELP aloca no accept um
   `bufio.Reader` de 64 KiB, e a leitura contada (§5.5) cresce em blocos de
   64 KiB: cerca de 128 KiB por conexão ociosa, fora de `ring.max_bytes` e sem
   teto com `max_conns = 0` (§4.4). Alguns milhares de conexões paradas levam o
   contêiner ao OOM kill, e com `ring=memory` o backlog confirmado se perde.
   Direção: `bufio` de 4 KiB (o maior `Peek` dos leitores é de um byte; o resto
   é `ReadByte`, `ReadSlice` e leitura contada) e primeiro bloco pequeno, que
   dobra conforme os bytes chegam.
2. **Cópias da janela RELP fora do ring.** O leitor copia o frame **antes** de
   obter a vaga na janela, e cada transação segura a cópia durante todo o
   backpressure (§5.4): até `relp.window + 1` cópias de até
   `relp.max_frame_bytes` por conexão, cerca de 2 GiB com os padrões, que nem
   `ring.max_bytes` nem a conta de §14.4 enxergam — justamente quando o ring
   está cheio. Direção: copiar depois da vaga e limitar os bytes em voo por um
   teto global ao processo, adquirido antes da cópia e de pelo menos
   `relp.max_frame_bytes`; esgotado, o leitor para de ler e a janela TCP segura
   o emissor.
3. *Corrigido em 2026-10-14 (porte do go-syslog-server):* `listen.unix` só
   remove um socket (`Lstat`, sem seguir link) cujo `connect` datagrama é
   recusado (`ECONNREFUSED`); arquivo comum, diretório, link ou socket vivo de
   qualquer tipo é `exit 1` sem apagar, e o boot recusado não remove o caminho
   na saída; regra em §4.3.
4. **Credencial dentro de `sink.nats.urls`.** Com `nats://usuario:senha@host`
   ou `nats://token@host`, a credencial aparece nos logs de conexão e de
   reconexão (`ConnectedUrl`), no log de sink configurado e no `--dump-config`,
   que não redige a URL. A validação não confere a forma: uma senha com `/`,
   `#` ou `?` faz o erro repetir a URL inteira a cada tentativa até
   `sink.boot_timeout`, ou faz discar um "host" que é um pedaço da credencial.
   E credencial na URL junto de `username`, `token` ou `credentials` não é
   recusada, contra §10.8.9; a biblioteca usa a da URL e ignora as chaves.
   Direção: logar só esquema e host; redigir a URL no dump; na validação,
   parsear cada entrada sem ecoá-la, exigir host, recusar caminho, consulta e
   fragmento em `nats://` e `tls://`, e recusar credencial na URL junto das
   chaves.
5. **Erros de configuração ecoam segredo.** O INI responde
   `expected 'key = value', got "<linha>"` (e o cabeçalho malformado ecoa a
   linha), e as flags respondem `unexpected argument "<arg>"`: um
   `password: x` no INI ou um `sink.nats.password=x` sem `--` vai inteiro para
   o stderr e, no `SIGHUP`, para o log, contra a regra de §15.3. Direção:
   mensagens com o número da linha ou a posição do argumento, nunca o texto.
6. *Corrigido em 2026-10-14 (porte do go-syslog-server):* stream com
   `retention: interest` é recusado como fatal na validação do boot; regra em
   §10.8.5.
7. *Corrigido em 2026-10-14 (porte do go-syslog-server):* `10158` (`409`),
   `10167` (`429`) e todo `429` do JetStream são `network`, não o fatal do 4xx
   desconhecido; regra em §10.8.6.
8. *Corrigido em 2026-10-14 (porte do go-syslog-server):* a sessão MySQL fixa
   `autocommit=1` no mesmo `SET` do `sql_mode`, e a validação do boot relê
   `@@session.autocommit` (fatal se não for 1); regra em §10.7.1 e §10.7.4.
9. *Corrigido em 2026-10-14 (porte do go-syslog-server):* `23502` do PostgreSQL
   é `fatal`; regra em §10.5.7. A outra metade da direção — a validação recusar
   no boot coluna extra `NOT NULL` sem default — não foi portada: hoje a tabela
   assim passa no boot e a primeira escrita é fatal.
10. **Validação do boot dentro de `sink.health_timeout`.** A validação do
    PostgreSQL e do MySQL (§10.5.5, §10.7.4) roda no contexto da checagem de
    saúde, de 1 s por padrão, e faz cerca de uma dúzia de idas e voltas. Com o
    banco a uns 60 ms de RTT ela nunca termina; o prazo fecha a conexão, cada
    tentativa recomeça do zero, e o processo sai com 3 depois de
    `sink.boot_timeout` dizendo que o banco não respondeu. Direção: prazo
    próprio para a validação (`sink.op_timeout`), que continue seguindo o
    cancelamento do boot.
11. **Ambiente libpq chega ao pool.** `pgxpool.ParseConfig("")` lê as `PG*` do
    ambiente, e só parte dos campos é sobrescrita depois: `PGSSLMODE` ou
    `PGPORT` inválidos derrubam o boot com `exit 1` por uma variável que não é
    deste servidor, e `PGCHANNELBINDING`, `PGREQUIREAUTH`, `PGSSLNEGOTIATION`,
    `PGMINPROTOCOLVERSION` e `PGCONNECT_TIMEOUT` sobrevivem e podem recusar
    toda conexão até `sink.boot_timeout`. Direção: parsear com todas as `PG*`
    escondidas (sob mutex, só no boot) e preencher tudo a partir da
    configuração.
12. **Nome do banco MySQL cru no SQL.** `sink.mysql.database` entra entre
    crases no `INSERT` sem validação; só `table` é conferida. Uma crase no nome
    quebra o comando (`1064`, que cai em `network` e espera
    `sink.boot_timeout`), e a defesa contra injeção que o código anuncia tem um
    furo. Direção: validar `database` como `table` (sem crase, barra invertida
    nem byte de controle, até 64 bytes), na configuração e no construtor.
13. **Despejo do Redis apaga o stream.** §10.3 supõe que `maxmemory` produz
    `OOM`, que é fatal. Com `maxmemory-policy allkeys-*` o Redis despeja
    qualquer chave antes do `XADD` — inclusive o stream, que é uma chave só — e
    as mensagens já entregues somem em bloco, com o sink UP e nenhum contador.
    Direção: declarar a premissa (`noeviction`, ou `volatile-*` sem TTL no
    stream) e ler `maxmemory_policy` no boot: `allkeys-*` é fatal, e a falta de
    permissão para ler é um `WARN` único.
14. **Gauges de emergência zerados com dump no disco.** No fim da reingestão,
    `syslog_emergency_files` e `syslog_emergency_events_pending` vão a zero
    mesmo quando um arquivo ficou para trás (ilegível, ou com quarentena ou
    remoção falha), e um alerta sobre `> 0` não dispara. Direção: recontar a
    partir da listagem do diretório no fim.
15. **O segundo sinal subconta a perda.** Com `ring=memory` ou `ring=none`, o
    abort de §12.3 soma em `reason="abort"` só o que está no ring, não os lotes
    retidos pelos workers (`Workers.Pending()`). Direção: somar os dois.
16. **A sonda do `sink=sqlite` espera a trava da escritora sem contexto.** Um
    lote mais longo que `sink.health_timeout` faz a sonda falhar por prazo com
    o banco íntegro: um ciclo DOWN por lentidão (§10.4). Direção: adquirir a
    trava respeitando o contexto da checagem.
17. **Dois orçamentos de `boot_timeout` com `sink=redis`.** A discagem do boot
    (§12.1, passo 4) e o `WaitReady` recebem cada um o prazo inteiro, e um
    Redis que oscile entre os dois passos pode dobrar a espera. Direção: um
    prazo único para o boot do sink.
18. **Recusa por IP invisível.** Nenhuma métrica ou log conta as conexões
    recusadas por `max_conns_per_ip`; o operador não distingue "ninguém
    conecta" de "um IP é recusado o tempo todo". Direção: um contador com o
    rótulo `transport` em §13.1, antes do código.
19. *Corrigido em 2026-10-14 (porte do go-syslog-server):*
    `natsio/nats-box:0.20.0` nos três compose de NATS e no `.env.example`,
    conferido por `TestComposeNATSBoxIsPinned`; regra em §17.5.
20. **Nomes que colidem com o `go-syslog-server` atual.** A imagem padrão
    (`go-syslog-server:local`), os projetos e contêineres `sls-*`, os volumes
    (`sls-data`, `nats-data`, `redis-data`), o `--version` e o `relp_software`
    usam os nomes do servidor LogHub que hoje ocupa `../go-syslog-server/`. Subir
    os compose daqui retagueia a imagem dele e assume as pilhas e os volumes
    dele sem aviso, e o decodificador daqui renomeia para `.corrupt` os dumps
    `emg-*.bin` que ele deixou. Direção: nomes próprios — imagem
    `go-syslog-standard-server`, prefixo `slstd-` nos projetos, contêineres e
    `node.id` dos compose, e o mesmo nome no `--version` e no `relp_software`.
