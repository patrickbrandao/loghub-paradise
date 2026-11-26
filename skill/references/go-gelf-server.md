# go-gelf-server

Recebe GELF 1.1. Em UDP aceita chunks e gzip/zlib. Em TCP, termine cada mensagem com NUL. Em HTTP, use `POST /gelf`. `202` indica aceitação e `503` com `Retry-After` indica pressão temporária. Produz JLH `type=gelf`.

## Executar em Docker

O [Compose](../examples/go-gelf-server.compose.yml) usa SQLite em `/data/events.db`. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-gelf-server.compose.yml up -d --wait receiver
curl -fsS http://127.0.0.1:19104/readyz
docker compose -f ./skill/examples/go-gelf-server.compose.yml run --rm teste
```

O injetor envia 10 mensagens de teste. O relatório deve mostrar 10 gravadas, sem perdas ou duplicatas. As portas de entrada do exemplo no host são `12201/tcp`, `12201/udp`, `12202/tcp`, em `127.0.0.1`. Dentro do receptor, use `12201/udp` e `12201/tcp` para GELF, `12202/tcp` para HTTP. A porta administrativa interna é `9104/tcp`.

Use `down` para parar os serviços do exemplo e manter os volumes.

```sh
docker compose -f ./skill/examples/go-gelf-server.compose.yml down
```

## Configurar

- A precedência é linha de comando, ambiente, INI, padrão. Use `--config /caminho/arquivo.ini` ou `GELF_CONFIG` para apontar para um INI montado.
- `GELF_LISTEN_UDP`, `GELF_LISTEN_TCP` e `GELF_LISTEN_HTTP` definem os endereços de entrada.
- Para GELF TCP com TLS em `12203/tcp` ou HTTPS em `12204/tcp`, configure `GELF_LISTEN_TLS` ou `GELF_LISTEN_HTTPS` e monte os arquivos definidos em `GELF_TLS_CERT_FILE` e `GELF_TLS_KEY_FILE`.
- `GELF_RING_MODE` admite `memory`, `sqlite` e `none`, além de `redis`/`valkey`. `GELF_SINK_TYPE` admite `sqlite`, `redis`/`valkey`, `postgres`, `mysql`, `nats` e `none`, além de `clickhouse`. O padrão da imagem combina fila em memória e NATS JetStream.
- `GELF_SINK_SQLITE_PATH` aponta para o banco de eventos. Use `ring.mode=none` quando quiser escrita direta no destino. `sink.type=none` descarta os eventos, útil apenas para medir a ingestão.
- Com NATS JetStream, crie antes um stream que cubra `loghub.<space>.gelf` do seu workspace. Com PostgreSQL, prepare a fila `loghub.inbox` antes de subir o receptor.
- `WORKSPACE` define o `space` dos eventos e, no NATS, faz parte do subject. Use um volume `/data` por instância para preservar a identidade e os arquivos de emergência.

## Operar

- `/healthz` indica que o processo responde. `/readyz` indica se o receptor pode aceitar tráfego e retorna `503` quando ele fica indisponível. `/metrics` expõe métricas da ingestão.
- Se a prontidão falhar, rode `docker compose -f ./skill/examples/go-gelf-server.compose.yml logs receiver`. Confira endereço do destino, credenciais e a existência da tabela ou stream quando aplicável.
- Dê tempo para o serviço drenar em `SIGTERM`. Após falha ou reinício, preserve `/data` para recuperar arquivos de emergência quando houver. Com ring em memória, a aceitação pode preceder a gravação e uma queda abrupta pode perder mensagens em trânsito.

Use `--help` para ver os parâmetros da imagem e `--dump-config` para ver a configuração efetiva.

```sh
docker run --rm tmsoftbrasil/go-gelf-server:sha-7e2694f821af --help
docker compose -f ./skill/examples/go-gelf-server.compose.yml exec receiver /app/go-gelf-server --dump-config
```
