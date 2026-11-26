# go-relplog-injector

Envia syslog sobre RELP, como o `omrelp` do rsyslog. Mede aceitações, rejeições, mensagens sem resposta e retransmissões.

## Executar em Docker

O [Compose](../examples/go-relplog-injector.compose.yml) sobe um receptor local e envia 10 eventos. O relatório confere a gravação no SQLite. Execute os comandos a partir da raiz do projeto.

```sh
docker compose -f ./skill/examples/go-relplog-injector.compose.yml run --rm injector
docker compose -f ./skill/examples/go-relplog-injector.compose.yml down
```

Para testar um receptor existente, use um endereço acessível de dentro do contêiner. No exemplo abaixo, o receptor é o serviço `receptor` na rede Docker `logs`.

```sh
docker run --rm --network logs tmsoftbrasil/go-relplog-injector:sha-7e2694f821af --transport tcp --target receptor:20514 --count 10
docker run --rm tmsoftbrasil/go-relplog-injector:sha-7e2694f821af --help
```

## Ajustar o envio

- `--target host:porta` e `--transport tcp|tls` escolhem o receptor. A porta comum é `20514` para TCP e `20515` para TLS.
- `--window` limita mensagens em voo por sessão, `--ack-timeout` controla o tempo de espera por confirmação e `--retries` controla as tentativas. Após quebra da sessão, mensagens sem confirmação podem ser retransmitidas.
- O gerador suporta `--format 5424|3164|raw`, `--size` e `--template`. `--file` lê uma linha por mensagem. `--pcap` reproduz mensagens de capturas RELP, syslog TCP ou UDP.
- Para reproduzir um fluxo RELP por emissor, use `--pcap-flows`. Ao anunciar a origem via PROXY, configure o receptor para aceitar o cabeçalho vindo do injetor. Use `--pcap-dedup` para ajustar o tratamento de cópias.
- `--count` limita o total, `--duration` limita o tempo, `--rate` define a taxa e `--concurrency` define as conexões ou sessões paralelas. Comece com uma contagem pequena.
- Para usar arquivos de entrada, capturas ou certificados, monte a pasta com `-v "$PWD:/w:ro"` e use caminhos como `/w/entrada.pcap`. Para stdin, use `docker run -i` e `--file -`. Com TLS privado, informe `--tls-ca` e, se houver autenticação mútua, `--tls-cert` e `--tls-key`.
- Para reproduzir os intervalos capturados, use `--pcap-timing original`. Rode o replay em um ambiente de teste e use `--rate` quando precisar limitar a carga.

## Conferir entrega

Use `--verify-sqlite` com o arquivo acessível no contêiner ou `--verify-url` com NATS JetStream, PostgreSQL, Redis ou Valkey. Precisa de permissão de leitura e de tempo para o evento ainda estar disponível. Use `--verify-timeout` para ajustar a espera.

`--verify-space` e `--verify-key` selecionam o destino lógico. `--verify-allow-duplicates` continua contando duplicatas, mas permite que elas não alterem o código de saída. Um `acked` indica confirmação RELP. A gravação depende da conferência no destino.

Use `--report json` para automação. O código `0` indica sucesso, `1` falha de envio ou conferência, `2` argumentos inválidos, `3` alvo inalcançável, `4` destino indisponível para conferência e `130` interrupção.
