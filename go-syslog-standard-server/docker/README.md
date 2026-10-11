# Deploy em Docker

Build e execução do `go-syslog-server` em contêiner, com um arquivo compose para
cada combinação de **ring** (onde a mensagem espera) e **sink** (para onde ela
vai).

```bash
# combinação de referência: ring na RAM -> Redis Streams
docker compose up -d --build

# qualquer outra
docker compose -f compose.memory-sqlite.yml up -d --build
```

Mande algo e veja chegar:

```bash
logger -n 127.0.0.1 -P 514 --rfc5424 -t teste "olá"
docker compose exec redis redis-cli XRANGE syslog:in - +
```

---

## As portas

| Dentro do contêiner | Publicado no host | Transporte |
|---|---|---|
| `1514/udp` | `514/udp` | syslog UDP (RFC 5426) |
| `1514/tcp` | `514/tcp` | syslog TCP (RFC 6587) |
| `20514/tcp` | `20514/tcp` | RELP |
| `9102/tcp` | `127.0.0.1:9102` | admin: `/healthz`, `/readyz`, `/metrics` |

**Por que 1514 dentro e 514 fora.** A imagem roda como uid 65532, que não abre
porta abaixo de 1024. Quem abre a 514 do host é o daemon do Docker, então a
publicação `514:1514` resolve sem dar privilégio nenhum ao processo. O custo é
que o número da configuração e o número do fio diferem; o log de boot imprime a
porta de dentro, e esta tabela é a tradução.

As defaults de host são diferentes em cada combinação, para que várias rodem lado
a lado. A de referência usa 514; `SYSLOG_PORT` fixa qualquer uma.

O admin fica em `9102`, não `9101`: o `go-webhook-server` usa `9101`, e os dois
rodam no mesmo host com frequência.

---

## As combinações

`compose.<ring>-<sink>.yml`

| arquivo | ring | sink | a confirmação significa | perda em queda abrupta |
|---|---|---|---|---|
| `compose.memory-redis.yml` ★ | memory | redis | aceito na RAM | backlog do ring, mitigado pelo dump |
| `compose.memory-sqlite.yml` | memory | sqlite | aceito na RAM | idem |
| `compose.memory-postgres.yml` | memory | postgres | aceito na RAM | idem |
| `compose.memory-mysql.yml` | memory | mysql | aceito na RAM | idem |
| `compose.memory-nats.yml` | memory | nats | aceito na RAM | idem |
| `compose.memory-none.yml` | memory | none | **descartado** | tudo — benchmark |
| `compose.sqlite-redis.yml` | sqlite | redis | `fsync` no `ring.db` | nada |
| `compose.sqlite-sqlite.yml` | sqlite | sqlite | `fsync` no `ring.db` | nada — mas paga **dois** `fsync` |
| `compose.sqlite-postgres.yml` | sqlite | postgres | `fsync` no `ring.db` | nada |
| `compose.sqlite-mysql.yml` | sqlite | mysql | `fsync` no `ring.db` | nada |
| `compose.sqlite-nats.yml` | sqlite | nats | `fsync` no `ring.db` | nada |
| `compose.sqlite-none.yml` | sqlite | none | **descartado** após o `fsync` | tudo — benchmark |
| `compose.none-redis.yml` | none | redis | `XADD` confirmado | nada |
| `compose.none-sqlite.yml` | none | sqlite | `COMMIT` no `events.db` | nada |
| `compose.none-postgres.yml` | none | postgres | `INSERT` confirmado † | nada † |
| `compose.none-mysql.yml` | none | mysql | `INSERT` confirmado † | nada † |
| `compose.none-nats.yml` | none | nats | `PubAck` do stream † | nada † |
| `compose.none-none.yml` | none | none | **descartado** | tudo — benchmark de transporte |

★ padrão de `docker-compose.yml`.

† Depende de configuração fora deste processo: `synchronous_commit` no
PostgreSQL, `innodb_flush_log_at_trx_commit` no MySQL (os compose fixam `1`), e
no NATS o `sync_interval`, o `sync_always` e as réplicas do stream.

### "A confirmação" depende do transporte

A tabela acima diz o que a confirmação **significa**. Se ela **existe**, depende
de como o emissor fala:

| Transporte | Existe confirmação? | O que acontece com o ring cheio |
|---|---|---|
| RELP | sim, `rsp 200 OK` | `500 ring_full`; o `rsyslog` retém e retenta |
| TCP | parcial: o servidor volta a ler | para de ler; a janela TCP segura o emissor |
| UDP | não | descarta e conta em `syslog_messages_dropped_total` |

**Só o RELP cumpre a garantia ao pé da letra.** Quem não pode perder mensagem
configura o emissor para RELP; quem usa UDP escolheu um transporte que perde.
`docs/SPEC.md` §2.3 e §18 têm o detalhe.

---

## O que só existe em contêiner

**`/data` é a única área gravável.** Os compose sobem com `read_only: true`, então
um caminho de escrita fora de `/data` falha no boot em vez de gravar na camada
efêmera. `ring.db`, `events.db` e `emergency/` derivam dali.

**O volume é obrigatório mesmo com `ring=memory`.** É onde vai o dump de
emergência. Sem ele, o dump de um incidente iria para a camada efêmera e sumiria
no restart — exatamente o que ele existe para impedir.

**`stop_grace_period` é 45s.** Precisa ser maior que
`pre_stop_delay + drain_timeout`. Os 10s padrão do Docker matariam o processo no
meio do dreno, levando mensagens que o RELP já tinha confirmado.

**UDP e `net.core.rmem_max`.** `SYSLOG_LISTEN_UDP_READ_BUFFER` pede 8 MiB, mas o
teto real é o `net.core.rmem_max` do **host**. Se o host estiver no padrão
(~200 KiB), o kernel reduz o pedido em silêncio e datagramas são descartados
antes de o processo existir — sem aparecer em métrica nenhuma nossa, só em
`/proc/net/udp`. Em produção:

```bash
sysctl -w net.core.rmem_max=16777216
```

---

## Memória

O servidor **não define `GOMEMLIMIT` sozinho** e avisa no boot se ele faltar
(`docs/SPEC.md` §14.4). Sem ele o coletor de lixo do Go não sabe do teto do
contêiner e o OOM killer chega antes do GC — e com `ring=memory` leva o
backlog junto, porque o dump de emergência não roda num processo morto pelo
kernel. Por isso `common.yml` define os dois valores, parametrizáveis pelo
`.env`:

| variável | padrão | papel |
|---|---|---|
| `SYSLOG_GOMEMLIMIT` | `512MiB` | `GOMEMLIMIT` do processo |
| `SYSLOG_MEMORY_LIMIT` | `768M` | limite de memória do contêiner (`deploy.resources.limits.memory`), **acima** do `GOMEMLIMIT` |
| `SYSLOG_RING_MAX_BYTES` | `256MiB` nos compose de `ring=memory` | teto de bytes do ring, que com `ring=memory` fica todo na RAM |

Com `ring=memory` o boot avisa se `GOMEMLIMIT` for menor que o dobro de
`ring.max_bytes`; os padrões acima respeitam isso. Com `ring=sqlite` e
`ring=none` o ring não ocupa RAM e o aviso só sai se a variável faltar. Quem
subir um dos três, suba os outros junto.

As duas variáveis têm gramáticas diferentes: `*_MEMORY_LIMIT` é lido pelo
Compose (`768M`, `1g`); `*_GOMEMLIMIT` é lido pelo runtime do Go, que aceita
só um inteiro com sufixo `B`, `KiB`, `MiB`, `GiB` ou `TiB` (maiúsculas
importam), ou `off`. Um `GOMEMLIMIT=768M` derruba o processo antes do boot com
`malformed GOMEMLIMIT`, e com `restart: unless-stopped` o contêiner entra em
laço.

## `/dev/log` do host

O transporte `unix` não está ligado em nenhum compose, de propósito: assumir o
`/dev/log` do host é tomar o syslog local de todos os processos da máquina.
Quem quer isso monta o diretório e liga o parâmetro:

```yaml
    environment:
      SYSLOG_LISTEN_UNIX: /run/syslog/log
    volumes:
      - /run/syslog:/run/syslog
```

e aponta `/dev/log` do host para `/run/syslog/log`. O servidor recusa roubar um
socket que outro processo esteja servindo, e remove o arquivo no shutdown.

---

## PostgreSQL

O servidor **nunca cria a tabela**. Nos compose, quem cria é a imagem oficial do
PostgreSQL, no primeiro boot do volume, a partir de `postgres/schema.sql`. Contra
um banco gerenciado, aplique o esquema você mesmo e dê ao papel só `INSERT`:

```bash
psql "$DSN" -f docker/postgres/schema.sql
```

Se o esquema não bater, o servidor sai com código 3 **imediatamente**, com a
coluna que falta no log — não espera `sink.boot_timeout`.

---

## MySQL

Mesma regra do PostgreSQL: o servidor **nunca cria a tabela**, e nos compose
quem cria é a imagem oficial, a partir de `mysql/schema.sql`. Contra um banco
gerenciado, aplique o esquema e dê ao usuário `INSERT` **e** `UPDATE` — o
`ON DUPLICATE KEY UPDATE` que torna a escrita idempotente exige os dois, mesmo
sem mudar nada; o `UPDATE` pode ficar só na coluna `id`:

```bash
mysql -u root -p syslog < docker/mysql/schema.sql
mysql -u root -p -e "GRANT INSERT, UPDATE (id) ON syslog.events TO 'syslog'@'%'"
```

O boot confere motor InnoDB, colunas, índice único, a permissão (com uma sonda
numa transação revertida) e o `max_allowed_packet`. Este último é o que mais
surpreende: com os frames padrão de 16 MiB, a maior linha que o servidor pode
aceitar passa de 160 MiB — linha crua, envelope no pior caso e colunas de texto
(`docs/SPEC.md` §3.9) —, e os compose sobem o pacote para `256M`. Com um banco
em 64 MiB, baixe `SYSLOG_TCP_MAX_FRAME_BYTES` e `SYSLOG_RELP_MAX_FRAME_BYTES`
para 4 MiB; nenhum emissor real chega perto disso.

## NATS

O servidor **nunca cria o stream**. Nos compose, o serviço `nats-init` o cria a
partir de `nats/stream.json`; contra um NATS de produção:

```bash
nats stream add SYSLOG --config docker/nats/stream.json
```

Cada mensagem syslog vira uma mensagem no subject `syslog.in`, com o id em
`Nats-Msg-Id` (desduplicação dentro da janela de dez minutos do stream) e o
`Data` montado assim: o envelope JSON seguido da linha crua, com o tamanho do
envelope no header `Sls-Envelope-Size`. O consumidor fatia
(`docs/SPEC.md` §3.8). O envelope não vai num header porque o JetStream recusa
header acima de 64 KiB.

O `max_payload` padrão do `nats-server` é 1 MiB, e o boot recusa, com código 3,
uma configuração em que o maior evento não caiba: por isso
`nats/nats-server.conf` sobe o `max_payload` para 8 MB e os compose de NATS
baixam os frames TCP e RELP para 1 MiB (o maior evento fica em 7 MiB).

---

## Recarregar sem reiniciar

```bash
docker kill -s HUP sls-memory-redis
```

Recarrega filtro, rate limit, nível de log e `keep_files`. Qualquer outra chave
alterada é **ignorada e nomeada** no log: o processo não finge ter aplicado o que
exige reinício.
