# go-syslog-injector

Envia mensagens syslog para teste e carga. Use com receptores syslog padrão ou com os receptores LogHub da família syslog.

## Executar em Docker

O [Compose](../examples/go-syslog-injector.compose.yml) sobe um receptor local e envia 10 eventos. O relatório confere a gravação no SQLite. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-syslog-injector.compose.yml run --rm injector
docker compose -f ./skill/examples/go-syslog-injector.compose.yml down
```

Para testar um receptor existente, use um endereço acessível de dentro do contêiner. No exemplo abaixo, o receptor é o serviço `receptor` na rede Docker `logs`.

```sh
docker run --rm --network logs tmsoftbrasil/go-syslog-injector:sha-7e2694f821af --transport tcp --target receptor:1514 --count 10
docker run --rm tmsoftbrasil/go-syslog-injector:sha-7e2694f821af --help
```

## Ajustar o envio

- `--target host:porta` e `--transport udp|tcp|tls|unix` escolhem a entrada. Com `unix`, passe o caminho do socket datagrama dentro do contêiner e monte o diretório que o contém.
- `--format 5424|3164|raw` escolhe a formatação. `--facility` e `--severity` ajustam a mensagem gerada. TCP aceita `--framing octet|lf` e TLS usa contagem de bytes.
- `--template 'teste seq={seq}'` muda o corpo. `--file` lê linhas de um arquivo. `--pcap` importa syslog capturado em UDP, TCP ou RELP. `--pcap-port` filtra pela porta de destino original.
- `--pcap-dedup auto` trata cópias de captura em múltiplas interfaces. Para anunciar a origem com `--pcap-flows --proxy-protocol v2`, configure PROXY no receptor. Em UDP, o envio local pode ter sucesso mesmo quando o receptor perdeu a mensagem.
- `--count` limita o total, `--duration` limita o tempo, `--rate` define a taxa e `--concurrency` define as conexões ou sessões paralelas. Comece com uma contagem pequena.
- Para usar arquivos de entrada, capturas ou certificados, monte a pasta com `-v "$PWD:/w:ro"` e use caminhos como `/w/entrada.pcap`. Para stdin, use `docker run -i` e `--file -`. Com TLS privado, informe `--tls-ca` e, se houver autenticação mútua, `--tls-cert` e `--tls-key`.
- Para reproduzir os intervalos capturados, use `--pcap-timing original`. Rode o replay em um ambiente de teste e use `--rate` quando precisar limitar a carga.

## Conferir entrega

Use `--verify-sqlite` com o arquivo acessível no contêiner ou `--verify-url` com NATS JetStream, PostgreSQL, Redis ou Valkey. Precisa de permissão de leitura e de tempo para o evento ainda estar disponível. Use `--verify-timeout` para ajustar a espera.

Use `--verify-space` para o workspace e `--verify-key` quando a tabela, chave Redis/Valkey ou stream JetStream tiver nome próprio. A verificação pressupõe um destino compatível com os campos LogHub.

Use `--report json` para automação. O código `0` indica sucesso, `1` falha de envio ou conferência, `2` argumentos inválidos, `3` alvo inalcançável, `4` destino indisponível para conferência e `130` interrupção.
