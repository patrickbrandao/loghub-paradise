# go-relplog-server

Recebe syslog em sessões RELP de um emissor como o `omrelp` do rsyslog. O cliente recebe uma resposta RELP por mensagem. Com fila em memória, `rsp 200` confirma aceitação na fila. Produz JLH `type=syslog` com `x-transport=relp`.

## Executar em Docker

O [Compose](../examples/go-relplog-server.compose.yml) usa SQLite em `/data/events.db`. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-relplog-server.compose.yml up -d --wait receiver
curl -fsS http://127.0.0.1:19103/readyz
docker compose -f ./skill/examples/go-relplog-server.compose.yml run --rm teste
```

O injetor envia 10 mensagens de teste. O relatório deve mostrar 10 gravadas, sem perdas ou duplicatas. As portas de entrada do exemplo no host são `20514/tcp`, em `127.0.0.1`. Dentro do receptor, use `20514/tcp` para RELP. A porta administrativa interna é `9103/tcp`.

Use `down` para parar os serviços do exemplo e manter os volumes.

```sh
docker compose -f ./skill/examples/go-relplog-server.compose.yml down
```

## Configurar

- A precedência é linha de comando, ambiente, INI, padrão. Use `--config /caminho/arquivo.ini` ou `RELPLOG_CONFIG` para apontar para um INI montado.
- `RELPLOG_LISTEN_TCP` define o endereço RELP. No emissor rsyslog, configure `omrelp` com o host e a porta do receptor.
- Para RELP com TLS em `20515/tcp`, configure `RELPLOG_LISTEN_TLS`, `RELPLOG_TLS_CERT_FILE` e `RELPLOG_TLS_KEY_FILE`. Ative TLS também no emissor.
- `RELPLOG_RING_MODE` admite `memory`, `sqlite` e `none`, além de `redis`/`valkey`. `RELPLOG_SINK_TYPE` admite `sqlite`, `redis`/`valkey`, `postgres`, `mysql`, `nats` e `none`, além de `clickhouse`. O padrão da imagem combina fila em memória e NATS JetStream.
- `RELPLOG_SINK_SQLITE_PATH` aponta para o banco de eventos. Use `ring.mode=none` quando quiser escrita direta no destino. `sink.type=none` descarta os eventos, útil apenas para medir a ingestão.
- Com NATS JetStream, crie antes um stream que cubra `loghub.<space>.syslog` do seu workspace. Com PostgreSQL, prepare a fila `loghub.inbox` antes de subir o receptor.
- `WORKSPACE` define o `space` dos eventos e, no NATS, faz parte do subject. Use um volume `/data` por instância para preservar a identidade e os arquivos de emergência.

## Operar

- `/healthz` indica que o processo responde. `/readyz` indica se o receptor pode aceitar tráfego e retorna `503` quando ele fica indisponível. `/metrics` expõe métricas da ingestão.
- Se a prontidão falhar, rode `docker compose -f ./skill/examples/go-relplog-server.compose.yml logs receiver`. Confira endereço do destino, credenciais e a existência da tabela ou stream quando aplicável.
- Dê tempo para o serviço drenar em `SIGTERM`. Após falha ou reinício, preserve `/data` para recuperar arquivos de emergência quando houver. Com ring em memória, a aceitação pode preceder a gravação e uma queda abrupta pode perder mensagens em trânsito.

Use `--help` para ver os parâmetros da imagem e `--dump-config` para ver a configuração efetiva.

```sh
docker run --rm tmsoftbrasil/go-relplog-server:sha-7e2694f821af --help
docker compose -f ./skill/examples/go-relplog-server.compose.yml exec receiver /app/go-relplog-server --dump-config
```
