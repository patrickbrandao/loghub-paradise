# go-webhook-injector

Gera requisições HTTP, envia arquivos ou reproduz requisições capturadas. Use para testes funcionais e carga em receptores de webhook.

## Executar em Docker

O [Compose](../examples/go-webhook-injector.compose.yml) sobe um receptor local e envia 10 eventos. O relatório confere a gravação no SQLite. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-webhook-injector.compose.yml run --rm injector
docker compose -f ./skill/examples/go-webhook-injector.compose.yml down
```

Para testar um receptor existente, use um endereço acessível de dentro do contêiner. No exemplo abaixo, o receptor é o serviço `receptor` na rede Docker `logs`.

```sh
docker run --rm --network logs tmsoftbrasil/go-webhook-injector:sha-7e2694f821af --url http://receptor:8080/hook --count 10
docker run --rm tmsoftbrasil/go-webhook-injector:sha-7e2694f821af --help
```

## Ajustar o envio

- `--url` escolhe o destino HTTP ou HTTPS. `--method`, `--header` e `--content-type` controlam a requisição. `--template` controla o corpo e aceita marcadores como `{seq}` e `{uuid}`, também disponíveis na URL e nos cabeçalhos.
- `--pipeline` controla requisições HTTP/1.1 em voo por conexão. Use `--http2 --streams N` para HTTP/2. Em `http://`, HTTP/2 usa h2c por conhecimento prévio. Em `https://`, negocia h2 por ALPN.
- `--file` lê uma linha por corpo ou um corpo por arquivo quando recebe um diretório. `--pcap` reproduz requisições capturadas em HTTP claro. `--pcap-host url` e `--pcap-path url` substituem valores da captura pelos da URL alvo.
- `--retries` reenvia `429` ou `503` seguindo `Retry-After`. `--uuid-out` grava os IDs do cabeçalho `X-LogHub-UUID` em um caminho gravável montado no contêiner.
- `--count` limita o total, `--duration` limita o tempo, `--rate` define a taxa e `--concurrency` define as conexões ou sessões paralelas. Comece com uma contagem pequena.
- Para usar arquivos de entrada, capturas ou certificados, monte a pasta com `-v "$PWD:/w:ro"` e use caminhos como `/w/entrada.pcap`. Para stdin, use `docker run -i` e `--file -`. Com TLS privado, informe `--tls-ca` e, se houver autenticação mútua, `--tls-cert` e `--tls-key`.
- Para reproduzir os intervalos capturados, use `--pcap-timing original`. Rode o replay em um ambiente de teste e use `--rate` quando precisar limitar a carga.

## Conferir entrega

Use `--verify-sqlite` com o arquivo acessível no contêiner ou `--verify-url` com NATS JetStream, PostgreSQL, Redis ou Valkey. Precisa de permissão de leitura e de tempo para o evento ainda estar disponível. Use `--verify-timeout` para ajustar a espera.

`--verify-stream` seleciona o stream NATS ou a chave Redis/Valkey. `--verify-table` troca a tabela PostgreSQL ou SQLite. A conferência usa o UUID retornado e/ou a sequência da execução.

Use `--report json` para automação. O código `0` indica sucesso, `1` falha de envio ou conferência, `2` argumentos inválidos, `3` alvo inalcançável, `4` destino indisponível para conferência e `130` interrupção.
