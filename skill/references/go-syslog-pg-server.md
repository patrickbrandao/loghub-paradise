# go-syslog-pg-server

Recebe mensagens syslog, como RFC 3164 e RFC 5424. TCP aceita quadros por contagem de bytes ou terminados por LF. UDP pode perder mensagens sem avisar o emissor. Produz JLH `type=syslog`. A entrada de rede usa transporte sem TLS.

## Executar em Docker

O [Compose](../examples/go-syslog-pg-server.compose.yml) usa PostgreSQL 18 com a fila `loghub.inbox`. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-syslog-pg-server.compose.yml up -d --wait receiver
curl -fsS http://127.0.0.1:19112/readyz
docker compose -f ./skill/examples/go-syslog-pg-server.compose.yml run --rm teste
```

O injetor envia 10 mensagens de teste. O relatório deve mostrar 10 gravadas, sem perdas ou duplicatas. As portas de entrada do exemplo no host são `11514/tcp`, `11514/udp`, em `127.0.0.1`. Dentro do receptor, use `1514/tcp` e `1514/udp` para syslog. A porta administrativa interna é `9102/tcp`.

Use `down` para parar os serviços do exemplo e manter os volumes.

```sh
docker compose -f ./skill/examples/go-syslog-pg-server.compose.yml down
```

## Configurar

- A precedência é linha de comando, ambiente, INI, padrão. Use `--config /caminho/arquivo.ini` ou `SYSLOG_CONFIG` para apontar para um INI montado.
- Use `SYSLOG_LISTEN_TCP` e `SYSLOG_LISTEN_UDP` para rede. Para socket datagrama Unix, use `SYSLOG_LISTEN_UNIX` e monte o diretório do socket junto do emissor.
- Configure `SYSLOG_SINK_POSTGRES_ADDR`, `SYSLOG_SINK_POSTGRES_DATABASE`, `SYSLOG_SINK_POSTGRES_USERNAME` e `SYSLOG_SINK_POSTGRES_PASSWORD`. O ring é em memória e a tabela padrão é `loghub.inbox`.
- O exemplo usa senha de desenvolvimento e aplica o SQL embutido ao inicializar um volume PostgreSQL vazio. Em banco existente, aplique o SQL antes de subir o receptor. Conceda `USAGE` no schema, `INSERT` na tabela e `SELECT (uuid)` ao usuário de ingestão. A retirada de itens da fila cabe ao consumidor.
- A conexão ao PostgreSQL usa `sslmode=disable`. Mantenha PostgreSQL e receptor em rede privada.
- `WORKSPACE` define o `space` dos eventos e, no NATS, faz parte do subject. Use um volume `/data` por instância para preservar a identidade e os arquivos de emergência.

## Operar

- `/healthz` indica que o processo responde. `/readyz` indica se o receptor pode aceitar tráfego e retorna `503` quando ele fica indisponível. `/metrics` expõe métricas da ingestão.
- Se a prontidão falhar, rode `docker compose -f ./skill/examples/go-syslog-pg-server.compose.yml logs receiver`. Confira endereço do destino, credenciais e a existência da tabela ou stream quando aplicável.
- Dê tempo para o serviço drenar em `SIGTERM`. Após falha ou reinício, preserve `/data` para recuperar arquivos de emergência quando houver. Com ring em memória, a aceitação pode preceder a gravação e uma queda abrupta pode perder mensagens em trânsito.

Use `--help` para ver os parâmetros da imagem e `--dump-config` para ver a configuração efetiva.

```sh
docker run --rm tmsoftbrasil/go-syslog-pg-server:sha-7e2694f821af --help
docker compose -f ./skill/examples/go-syslog-pg-server.compose.yml exec receiver /app/go-syslog-pg-server --dump-config
```
