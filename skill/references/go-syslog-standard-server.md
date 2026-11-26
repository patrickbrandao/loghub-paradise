# go-syslog-standard-server

Recebe syslog por UDP, TCP ou socket Unix e aceita RELP. Use para cargas legadas ou para preservar o formato de evento antigo. Os outros receptores syslog produzem o envelope JLH.

## Executar em Docker

O [Compose](../examples/go-syslog-standard-server.compose.yml) usa SQLite em `/data/events.db`. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-syslog-standard-server.compose.yml up -d --wait receiver
curl -fsS http://127.0.0.1:19132/readyz
docker compose -f ./skill/examples/go-syslog-standard-server.compose.yml run --rm teste
```

O injetor envia 10 mensagens de teste. O destino SQLite usa a tabela `events` em `/data/events.db`. A saída do injetor confirma o envio por TCP. Para conferir a gravação, exporte o banco com o receptor parado. As portas de entrada do exemplo no host são `13514/tcp`, `13514/udp`, `23514/tcp`, em `127.0.0.1`. Dentro do receptor, use `1514/tcp` e `1514/udp` para syslog e `20514/tcp` para RELP. A porta administrativa interna é `9102/tcp`.

Para conferir o banco local, use `sqlite3` no host após a parada do receptor.

```sh
docker compose -f ./skill/examples/go-syslog-standard-server.compose.yml stop receiver
docker compose -f ./skill/examples/go-syslog-standard-server.compose.yml cp receiver:/data/events.db ./syslog-standard.db
sqlite3 ./syslog-standard.db 'SELECT count(*) FROM events;'
```

Use `down` para parar os serviços do exemplo e manter os volumes.

```sh
docker compose -f ./skill/examples/go-syslog-standard-server.compose.yml down
```

## Configurar

- A precedência é linha de comando, ambiente, INI, padrão. Use `--config /caminho/arquivo.ini` ou `SYSLOG_CONFIG` para apontar para um INI montado.
- Use `SYSLOG_LISTEN_TCP` e `SYSLOG_LISTEN_UDP` para rede. Para socket datagrama Unix, use `SYSLOG_LISTEN_UNIX` e monte o diretório do socket junto do emissor.
- `SYSLOG_LISTEN_RELP` ativa a porta RELP. Para emissor syslog na porta padrão, publique `514:1514` no Docker e mantenha o processo na porta interna `1514`.
- `SYSLOG_RING_MODE` admite `memory`, `sqlite` e `none`. `SYSLOG_SINK_TYPE` admite `sqlite`, `redis`/`valkey`, `postgres`, `mysql`, `nats` e `none`. O padrão da imagem combina fila em memória e Redis.
- `SYSLOG_SINK_SQLITE_PATH` aponta para o banco de eventos. Use `ring.mode=none` quando quiser escrita direta no destino. `sink.type=none` descarta os eventos, útil apenas para medir a ingestão.
- Use um volume `/data` por instância para guardar os bancos locais e os arquivos de emergência.

## Operar

- `/healthz` indica que o processo responde. `/readyz` indica se o receptor pode aceitar tráfego e retorna `503` quando ele fica indisponível. `/metrics` expõe métricas da ingestão.
- Se a prontidão falhar, rode `docker compose -f ./skill/examples/go-syslog-standard-server.compose.yml logs receiver`. Confira endereço do destino, credenciais e a existência da tabela ou stream quando aplicável.
- Dê tempo para o serviço drenar em `SIGTERM`. Após falha ou reinício, preserve `/data` para recuperar arquivos de emergência quando houver. Com ring em memória, a aceitação pode preceder a gravação e uma queda abrupta pode perder mensagens em trânsito.

Use `--help` para ver os parâmetros da imagem e `--dump-config` para ver a configuração efetiva.

```sh
docker run --rm tmsoftbrasil/go-syslog-standard-server:sha-7e2694f821af --help
docker compose -f ./skill/examples/go-syslog-standard-server.compose.yml exec receiver /app/go-syslog-standard-server --dump-config
```
