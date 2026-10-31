# go-gelf-injector

Injeta mensagens GELF num servidor — para testar ou carregar o
`go-gelf-server` e seus irmãos (`go-gelf-pg-server`, `go-gelf-nats-server`),
ou qualquer receptor GELF (Graylog, Logstash).

- Transportes: **UDP** (chunks GELF até 128, `--chunk-size` 8192, gzip/zlib),
  **TCP** e **TLS** (moldura `\0`, mTLS opcional), **HTTP/HTTPS** (`POST
  /gelf`, keep-alive, pipelining, `Content-Encoding`, reenvio de `429`/`503`);
  PROXY protocol v1/v2.
- Conteúdo: gerador GELF 1.1 com `_seq`/`_run` (buraco no destino = perda) e
  `--payload-size` para forçar chunks, modelo com marcadores, um documento por
  linha de arquivo/stdin, **replay de captura do tcpdump** (pcap/pcapng): GELF
  de UDP (chunks remontados, descomprimidos), TCP (`\0`, captura iniciada no
  meio) e HTTP.
- Carga: `--count`, `--duration`, `--rate`, `--concurrency`; relatório com
  desfechos, vazão e latência (texto ou JSON) e código de saída.

```bash
go build -o go-gelf-injector ./cmd/go-gelf-injector

# 1 mensagem GELF por TCP em 127.0.0.1:12201
./go-gelf-injector

# carga UDP: 60 s a 10 000 msg/s, documentos de 20 KB (2 chunks gzip)
./go-gelf-injector --transport udp --target 10.0.0.5:12201 --duration 60s --rate 10000 --payload-size 20000

# HTTP com gzip, reenviando 503. Com keep-alive no servidor, pipelining:
./go-gelf-injector --transport http --target 10.0.0.5:12202 --count 100000 --concurrency 4 --pipeline 32 --compress gzip --retries 5
# servidor que fecha toda conexão (sem keep-alive, ou teto 1 por conexão):
# a carga vem de conexões em paralelo; --pipeline > 1 só desperdiça
./go-gelf-injector --transport http --target 10.0.0.5:12202 --count 100000 --concurrency 32 --compress gzip --retries 5

# modelo
./go-gelf-injector --template '{"version":"1.1","host":"{host}","short_message":"login {rand:6}","level":4,"_id":"{uuid}"}' --count 100

# replay de captura de produção, no ritmo original, em TCP
tcpdump -i eth0 -s 0 -w prod.pcap 'port 12201 or port 12202'
./go-gelf-injector --target 127.0.0.1:12201 --pcap prod.pcap --pcap-timing original
```

Saída: `0` tudo enviado (ou `2xx`), `1` alguma falha, `2` uso, `3` alvo
inalcançável, `130` interrompido. Detalhes: `docs/SPEC.md`; testes:
`docs/TESTING.md`; imagem: `docker/Dockerfile`.
