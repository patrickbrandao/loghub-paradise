# go-webhook-nats-server

Recebe requisições HTTP/1.1 ou HTTP/2 em claro (h2c), em qualquer caminho e método. Para HTTPS, termine o TLS em um proxy reverso.

Produz JLH `type=webhook`. A resposta `200` traz `X-LogHub-UUID` e JSON com `mode=fast`, `status=accepted` e `stage=ring`. Confirme a gravação separadamente no JetStream.

## Executar em Docker

O [Compose](../examples/go-webhook-nats-server.compose.yml) usa NATS com JetStream. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-webhook-nats-server.compose.yml up -d --wait receiver
curl -fsS http://127.0.0.1:19131/readyz
docker compose -f ./skill/examples/go-webhook-nats-server.compose.yml run --rm teste
```

O injetor envia 10 mensagens de teste. O relatório deve mostrar 10 gravadas, sem perdas ou duplicatas. As portas de entrada do exemplo no host são `18083/tcp`, em `127.0.0.1`. Dentro do receptor, use `8080/tcp` para HTTP. A porta administrativa interna é `9101/tcp`.

Use `down` para parar os serviços do exemplo e manter os volumes.

```sh
docker compose -f ./skill/examples/go-webhook-nats-server.compose.yml down
```

## Configurar

- A precedência é linha de comando, ambiente, INI, padrão. Use `--config /caminho/arquivo.ini` ou `WHS_CONFIG` para apontar para um INI montado.
- Use `WHS_SERVER_BIND` para a entrada HTTP e `WHS_SERVER_MAX_BODY_BYTES` para o limite de corpo.
- Configure `WHS_SINK_NATS_URLS`, `WHS_SINK_NATS_MODE` e `WHS_SINK_NATS_STREAM`. O ring é em memória. O exemplo usa JetStream, stream `LOGHUB_WEBHOOK` e subject `loghub.default.webhook` para o workspace `default`.
- Em NATS externo, prepare o stream para cobrir `loghub.<space>.webhook` e mantenha `loghub._ctl.beacon` fora dele. Use o subject exato do workspace. `loghub.>` abrange também o plano de controle. Em modo Core NATS, use `WHS_SINK_NATS_MODE=core` para publicação de melhor esforço.
- NATS usa texto claro. Em broker externo, configure `max_payload` e o limite por mensagem do stream para comportar o envelope completo. Com o limite HTTP padrão de 5 MiB, o exemplo usa `max_payload: 8MB`.
- `WORKSPACE` define o `space` dos eventos e, no NATS, faz parte do subject. Use um volume `/data` por instância para preservar a identidade e os arquivos de emergência.

## Operar

- `/healthz` indica que o processo responde. `/readyz` indica se o receptor pode aceitar tráfego e retorna `503` quando ele fica indisponível. `/metrics` expõe métricas da ingestão.
- Se a prontidão falhar, rode `docker compose -f ./skill/examples/go-webhook-nats-server.compose.yml logs receiver`. Confira endereço do destino, credenciais e a existência da tabela ou stream quando aplicável.
- Dê tempo para o serviço drenar em `SIGTERM`. Após falha ou reinício, preserve `/data` para recuperar arquivos de emergência quando houver. Com ring em memória, a aceitação pode preceder a gravação e uma queda abrupta pode perder mensagens em trânsito.

Use `--help` para ver os parâmetros da imagem e `--dump-config` para ver a configuração efetiva.

```sh
docker run --rm tmsoftbrasil/go-webhook-nats-server:sha-7e2694f821af --help
docker compose -f ./skill/examples/go-webhook-nats-server.compose.yml exec receiver /app/go-webhook-nats-server --dump-config
```
