# go-gelf-injector

Envia documentos GELF 1.1 a um receptor GELF. Use para testar conversão, medir vazão ou reproduzir tráfego capturado.

## Executar em Docker

O [Compose](../examples/go-gelf-injector.compose.yml) sobe um receptor local e envia 10 eventos. O relatório confere a gravação no SQLite. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-gelf-injector.compose.yml run --rm injector
docker compose -f ./skill/examples/go-gelf-injector.compose.yml down
```

Para testar um receptor existente, use um endereço acessível de dentro do contêiner. No exemplo abaixo, o receptor é o serviço `receptor` na rede Docker `logs`.

```sh
docker run --rm --network logs tmsoftbrasil/go-gelf-injector:sha-7e2694f821af --transport http --target receptor:12202 --count 10
docker run --rm tmsoftbrasil/go-gelf-injector:sha-7e2694f821af --help
```

## Ajustar o envio

- `--target host:porta` e `--transport udp|tcp|tls|http|https` escolhem a entrada. Portas usuais `12201` (UDP/TCP), `12202` (HTTP), `12203` (TLS), `12204` (HTTPS). Em HTTP(S), o caminho padrão é `/gelf` e `--http-path` altera esse caminho.
- O gerador usa `_run` e `_seq`. `--template` recebe um documento GELF com marcadores. `--file` lê um documento por linha. Use `--payload-size` e `--chunk-size` para testar chunks UDP. `--compress` aceita `none`, `gzip` e `zlib` em UDP e HTTP(S).
- Em HTTP(S), `--pipeline` controla requisições em voo por conexão e `--retries` permite reenviar `429` e `503`. Em TCP/TLS, o injetor aplica automaticamente o delimitador NUL.
- `--pcap` reproduz GELF capturado em UDP, TCP ou HTTP. `--pcap-dedup auto` trata cópias de captura em múltiplas interfaces. Para usar `--pcap-flows` com a origem capturada, configure o receptor para aceitar PROXY do endereço do injetor.
- `--count` limita o total, `--duration` limita o tempo, `--rate` define a taxa e `--concurrency` define as conexões ou sessões paralelas. Comece com uma contagem pequena.
- Para usar arquivos de entrada, capturas ou certificados, monte a pasta com `-v "$PWD:/w:ro"` e use caminhos como `/w/entrada.pcap`. Para stdin, use `docker run -i` e `--file -`. Com TLS privado, informe `--tls-ca` e, se houver autenticação mútua, `--tls-cert` e `--tls-key`.
- Para reproduzir os intervalos capturados, use `--pcap-timing original`. Rode o replay em um ambiente de teste e use `--rate` quando precisar limitar a carga.

## Conferir entrega

Use `--verify-sqlite` com o arquivo acessível no contêiner ou `--verify-url` com NATS JetStream, PostgreSQL, Redis ou Valkey. Precisa de permissão de leitura e de tempo para o evento ainda estar disponível. Use `--verify-timeout` para ajustar a espera.

Use `--verify-space` se o receptor usar outro workspace. `--verify-key` seleciona a chave Redis/Valkey ou o stream JetStream. O SQLite esperado tem tabela `gelf_events`.

Use `--report json` para automação. O código `0` indica sucesso, `1` falha de envio ou conferência, `2` argumentos inválidos, `3` alvo inalcançável, `4` destino indisponível para conferência e `130` interrupção.
