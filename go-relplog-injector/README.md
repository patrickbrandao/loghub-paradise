# go-relplog-injector

Injeta mensagens syslog por **RELP** num servidor — para testar ou carregar o
`go-relplog-server` e seus cortes (ou qualquer servidor `imrelp`) — falando como
o `omrelp` do rsyslog.

- Sessão RELP em TCP ou TLS (mTLS opcional), PROXY protocol v1/v2.
- Janela de transações (`--window`), uma confirmação por mensagem, quebra de
  sessão em `500` e `serverclose`, retransmissão do que ficou sem resposta.
- Conteúdo: gerador com `seq=N`, modelo com marcadores, arquivo/stdin, **replay
  de captura do tcpdump** (RELP, syslog TCP ou UDP).
- Relatório: `acked`, `rejected` (com o texto da recusa), `unanswered`,
  retransmissões, vazão e latência de confirmação; código de saída.
- **Conferência no destino** (`--verify-url` NATS/PostgreSQL/Redis/Valkey,
  `--verify-sqlite`): conta as confirmadas que não chegaram e as duplicadas.
- Replay fiel: cópias do `tcpdump -i any` descartadas (`--pcap-dedup`), uma
  sessão por fluxo original com o PROXY da origem (`--pcap-flows`).

```bash
go build -o go-relplog-injector ./cmd/go-relplog-injector

./go-relplog-injector --target 127.0.0.1:20514
./go-relplog-injector --target 10.0.0.5:20514 --duration 60s --rate 20000 --concurrency 4 --size 512
./go-relplog-injector --target relp.example:20515 --transport tls --tls-ca ca.crt --count 1000
./go-relplog-injector --pcap prod.pcap --pcap-timing original
./go-relplog-injector --pcap any.pcapng --pcap-flows --proxy-protocol v2
./go-relplog-injector --count 100000 --concurrency 4 --verify-url nats://127.0.0.1:4222
```

Saída: `0` tudo confirmado (e, com `--verify-*`, tudo no destino), `1` alguma
falha, perdida ou duplicada, `2` uso, `3` alvo inalcançável, `4` conferência
impossível, `130` interrompido. Detalhes: `docs/SPEC.md`; testes: `docs/TESTING.md`.
