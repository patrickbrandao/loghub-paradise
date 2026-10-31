# go-syslog-injector

Injeta mensagens syslog num servidor — para testar ou carregar o
`go-syslog-server` e seus irmãos (ou qualquer servidor syslog).

- Transportes: UDP (RFC 5426), TCP (RFC 6587, octet-counting ou LF), TLS (RFC
  5425, mTLS opcional), socket unix datagrama; PROXY protocol v1/v2.
- Conteúdo: gerador com `seq=N` (buraco no destino = perda), modelo com
  marcadores, linhas de arquivo/stdin, **replay de captura do tcpdump** (pcap ou
  pcapng; UDP, TCP octet/LF e RELP; captura iniciada no meio da conexão).
- Carga: `--count`, `--duration`, `--rate`, `--concurrency`; relatório com
  desfechos, vazão e latência (texto ou JSON) e código de saída.

```bash
go build -o go-syslog-injector ./cmd/go-syslog-injector

# 1 mensagem RFC 5424 por TCP
./go-syslog-injector --target 127.0.0.1:1514

# carga: 60 s a 50 000 msg/s em 8 conexões, mensagens de 512 bytes
./go-syslog-injector --target 10.0.0.5:1514 --duration 60s --rate 50000 --concurrency 8 --size 512

# UDP com modelo e severidade
./go-syslog-injector --transport udp --template 'login fail user={rand:6} id={uuid}' --severity warning --count 100

# replay de captura de produção, no ritmo original, só da porta 514
tcpdump -i eth0 -s 0 -w prod.pcap 'port 514'
./go-syslog-injector --target 127.0.0.1:1514 --pcap prod.pcap --pcap-port 514 --pcap-timing original
```

Saída: `0` tudo enviado, `1` alguma falha, `2` uso, `3` alvo inalcançável,
`130` interrompido. Detalhes: `docs/SPEC.md`; testes: `docs/TESTING.md`.
