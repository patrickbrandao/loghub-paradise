# go-relplog-injector

Injeta mensagens syslog por **RELP** num servidor — para testar ou carregar o
`go-relplog-server` e seus cortes (ou qualquer servidor `imrelp`) — falando como
o `omrelp` do rsyslog.

- Sessão RELP em TCP ou TLS (mTLS opcional), PROXY protocol v1/v2.
- Janela de transações (`--window`), uma confirmação por mensagem, quebra de
  sessão em `500` e `serverclose`, retransmissão do que ficou sem resposta.
- Conteúdo: gerador com `seq=N`, modelo com marcadores, arquivo/stdin, **replay
  de captura do tcpdump** (RELP, syslog TCP ou UDP).
- Relatório: `acked`, `rejected`, `unanswered`, retransmissões, vazão e
  latência de confirmação; código de saída.

```bash
go build -o go-relplog-injector ./cmd/go-relplog-injector

./go-relplog-injector --target 127.0.0.1:20514
./go-relplog-injector --target 10.0.0.5:20514 --duration 60s --rate 20000 --concurrency 4 --size 512
./go-relplog-injector --target relp.example:20515 --transport tls --tls-ca ca.crt --count 1000
./go-relplog-injector --pcap prod.pcap --pcap-timing original
```

Saída: `0` tudo confirmado, `1` alguma falha, `2` uso, `3` alvo inalcançável,
`130` interrompido. Detalhes: `docs/SPEC.md`; testes: `docs/TESTING.md`.
