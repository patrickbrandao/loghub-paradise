# go-webhook-injector

Injeta requisições HTTP num servidor de webhooks — para testar ou carregar o
`go-webhook-server`, o `go-webhook-fast-server`, o `go-webhook-pg-server` e o
`go-webhook-nats-server` (ou qualquer servidor HTTP).

- **Protocolos:** HTTP/1.1 com keep-alive e **pipelining**; HTTP/2 em claro
  (h2c) ou por TLS (ALPN), com muitos *streams* por conexão; TLS/mTLS.
- **Carga extrema:** `--concurrency` conexões, `--pipeline` requisições em voo
  por conexão (HTTP/1.1), `--streams` (HTTP/2), `--rate` ou o máximo,
  `--requests-per-conn` para medir o custo de abrir conexões.
- **Conteúdo:** gerador JSON com `seq` (um buraco no destino é perda), modelo
  com marcadores no corpo, na URL e nos cabeçalhos, linhas de arquivo/stdin ou
  os arquivos de uma pasta, e **replay de captura do tcpdump** (pcap ou pcapng:
  método, caminho, cabeçalhos e corpo, inclusive chunked e pipelining).
- **Resposta:** cada requisição tem como desfecho o status recebido;
  `--retries` reenvia `429`/`503` seguindo o `Retry-After`; `--uuid-out` grava
  o `X-LogHub-UUID` de cada uma, para conferir no destino.

```bash
go build -o go-webhook-injector ./cmd/go-webhook-injector

# 1 requisição POST com o JSON do gerador
./go-webhook-injector --url http://127.0.0.1:8080/hook

# carga extrema: 60 s, 16 conexões com 32 requisições em pipelining cada
./go-webhook-injector --url http://127.0.0.1:8080/hook --duration 60s --concurrency 16 --pipeline 32

# HTTP/2 em claro, 8 conexões × 128 streams, a 50 000 req/s, reenviando 429
./go-webhook-injector --url http://127.0.0.1:8080/hook --http2 --concurrency 8 --streams 128 \
  --rate 50000 --duration 60s --retries 100

# modelo com marcadores na URL, no cabeçalho e no corpo
./go-webhook-injector --url 'http://127.0.0.1:8080/github/{seq}' --header 'X-GitHub-Event: push' \
  --header 'X-Request-Id: {uuid}' --template '{"ref":"refs/heads/main","n":{seq},"user":"{rand:8}"}' --count 1000

# replay do tráfego de produção, no ritmo original
tcpdump -i eth0 -s 0 -B 65536 -U -w prod.pcap 'tcp port 8080'
./go-webhook-injector --url http://127.0.0.1:8080 --pcap prod.pcap --pcap-timing original

# conferir a entrega: cada uuid devolvido está na fila?
./go-webhook-injector --url http://127.0.0.1:8080/x --count 100000 --concurrency 8 --uuid-out uuids.txt
```

Saída: `0` todas `2xx`, `1` alguma falha, `2` uso, `3` alvo inalcançável, `130`
interrompido. Detalhes: `docs/SPEC.md`; testes e números medidos:
`docs/TESTING.md`.
