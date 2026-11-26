# go-gelf-nats-server

Recebe GELF 1.1. Em UDP aceita chunks e gzip/zlib. Em TCP, termine cada mensagem com NUL. Em HTTP, use `POST /gelf`. `202` indica aceitação e `503` com `Retry-After` indica pressão temporária. Produz JLH `type=gelf`. A entrada de rede usa transporte sem TLS.

## Executar em Docker

O [Compose](../examples/go-gelf-nats-server.compose.yml) usa NATS com JetStream. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-gelf-nats-server.compose.yml up -d --wait receiver
curl -fsS http://127.0.0.1:19124/readyz
docker compose -f ./skill/examples/go-gelf-nats-server.compose.yml run --rm teste
```

O injetor envia 10 mensagens de teste. O relatório deve mostrar 10 gravadas, sem perdas ou duplicatas. As portas de entrada do exemplo no host são `12401/tcp`, `12401/udp`, `12402/tcp`, em `127.0.0.1`. Dentro do receptor, use `12201/udp` e `12201/tcp` para GELF, `12202/tcp` para HTTP. A porta administrativa interna é `9104/tcp`.

Use `down` para parar os serviços do exemplo e manter os volumes.

```sh
docker compose -f ./skill/examples/go-gelf-nats-server.compose.yml down
```

## Configurar

- A precedência é linha de comando, ambiente, INI, padrão. Use `--config /caminho/arquivo.ini` ou `GELF_CONFIG` para apontar para um INI montado.
- `GELF_LISTEN_UDP`, `GELF_LISTEN_TCP` e `GELF_LISTEN_HTTP` definem os endereços de entrada.
- Configure `GELF_SINK_NATS_URLS`, `GELF_SINK_NATS_MODE` e `GELF_SINK_NATS_STREAM`. O ring é em memória. O exemplo usa JetStream, stream `LOGHUB_GELF` e subject `loghub.default.gelf` para o workspace `default`.
- Em NATS externo, prepare o stream para cobrir `loghub.<space>.gelf` e mantenha `loghub._ctl.beacon` fora dele. Use o subject exato do workspace. `loghub.>` abrange também o plano de controle. Em modo Core NATS, use `GELF_SINK_NATS_MODE=core` para publicação de melhor esforço.
- A conexão a NATS usa texto claro. Mantenha o broker e o receptor em rede privada.
- `WORKSPACE` define o `space` dos eventos e, no NATS, faz parte do subject. Use um volume `/data` por instância para preservar a identidade e os arquivos de emergência.

## Operar

- `/healthz` indica que o processo responde. `/readyz` indica se o receptor pode aceitar tráfego e retorna `503` quando ele fica indisponível. `/metrics` expõe métricas da ingestão.
- Se a prontidão falhar, rode `docker compose -f ./skill/examples/go-gelf-nats-server.compose.yml logs receiver`. Confira endereço do destino, credenciais e a existência da tabela ou stream quando aplicável.
- Dê tempo para o serviço drenar em `SIGTERM`. Após falha ou reinício, preserve `/data` para recuperar arquivos de emergência quando houver. Com ring em memória, a aceitação pode preceder a gravação e uma queda abrupta pode perder mensagens em trânsito.

Use `--help` para ver os parâmetros da imagem e `--dump-config` para ver a configuração efetiva.

```sh
docker run --rm tmsoftbrasil/go-gelf-nats-server:sha-7e2694f821af --help
docker compose -f ./skill/examples/go-gelf-nats-server.compose.yml exec receiver /app/go-gelf-nats-server --dump-config
```
