# go-gelf-nats-server com Docker

```bash
cd docker
cp .env.example .env            # opcional: tudo tem padrão
docker compose up -d --build                   # servidor + NATS JetStream + criação do stream
docker compose --profile demo up -d --build    # + emissor fluent-bit (UDP gzip em chunks, e TCP)
```

| Serviço | O quê | Portas no host |
|---|---|---|
| `gelf` (`gelf-nats`) | o servidor, imagem `tmsoftbrasil/go-gelf-nats-server:dev` | GELF `12281/udp` e `12281/tcp` (`GELF_PORT`), HTTP `12282` (`GELF_HTTP_PORT`), admin `127.0.0.1:19281` |
| `nats` (`gelf-nats-nats`) | `nats-server` com JetStream (`nats/nats-server.conf`) | `127.0.0.1:14291` (cliente), `127.0.0.1:18291` (monitor) |
| `nats-init` | cria o stream `LOGHUB_GELF` a partir de `nats/stream.json`, uma vez | — |
| `fluent-bit` (perfil `demo`) | emissor de demonstração (`fluent-bit/`) | — |

As portas do host não colidem com as do `go-gelf-server` nem com as do
`go-gelf-pg-server`; para receber nas portas padrão, `GELF_PORT=12201` e
`GELF_HTTP_PORT=12202`.

## Conferir

```bash
printf '{"version":"1.1","host":"dev","short_message":"oi"}' | nc -u -w1 127.0.0.1 12281
curl -sS -w '%{http_code}\n' --data-binary '{"short_message":"oi"}' http://127.0.0.1:12282/gelf
nats --server nats://127.0.0.1:14291 stream view LOGHUB_GELF
nats --server nats://127.0.0.1:14291 sub 'loghub._ctl.beacon'   # o heartbeat
curl -s http://127.0.0.1:19281/readyz; curl -s http://127.0.0.1:19281/metrics | grep ^gelf_messages
```

## Saúde

`/healthz` é liveness pura (o healthcheck do compose). `/readyz` dá `503` com o
motivo enquanto o NATS está fora, o ring passa de `ring.high_water` ou dumps
são reingeridos: é para o balanceador e para o operador, não para reiniciar o
contêiner.

## Configuração

- **Env** (`common.yml`, `docker-compose.yml`, `.env`): só chaves de restart.
- **`conf/gelf.ini`** (montado em `/etc/gelf`): só as recarregáveis
  (`[log]`). Edite e `docker kill -s HUP gelf-nats`.
- **`LOGHUB_EB_URL`**: substitui as quatro chaves de conexão do
  `docker-compose.yml` (apague-as antes); só `nats://`.
- **Sem TLS**: nem nos listeners, nem até o NATS. Use uma rede confiável, ou um
  terminador TLS na frente com `GELF_PROXY_PROTOCOL=optional`.
- **Outro `WORKSPACE`** (`GELF_WORKSPACE`): o stream cobre `loghub.*.gelf`, todo
  `space` de um segmento; um `space` com ponto exige um subject a mais nele.

## Emissores de demonstração

O perfil `demo` sobe o fluent-bit com duas saídas `gelf`: UDP com `Compress On`
e `Packet_Size 1420` e um campo aleatório de ~12 KiB (chega em vários chunks
gzip) e TCP com moldura `\0`. O driver de log do Docker não roda dentro de um
compose de forma útil (o daemon manda o log do próprio contêiner); prove-o à
parte:

```bash
docker run --rm --log-driver=gelf --log-opt gelf-address=udp://127.0.0.1:12281 alpine echo oi
```

## Imagem

```bash
docker build -f Dockerfile -t tmsoftbrasil/go-gelf-nats-server:dev ..   # daqui; o contexto é a raiz do módulo
```

Nunca `docker push` sem pedido explícito.
