# go-relplog-server

**Estado: só especificação.** Nenhum código ainda.

Servidor futuro de syslog por **RELP** (o protocolo do `rsyslog`/`librelp`, com
confirmação por mensagem) no paradigma LogHub. Nasceu em 2026-10-05, quando o
`go-syslog-server` foi reescrito do zero para o LogHub e o RELP saiu dele: o
payload `type=syslog` da skill `loghub-type-syslog-schema` não tem `relp` no enum
de `transport`, e o dono do projeto decidiu que o RELP terá software e
especificação próprios.

| Arquivo | Conteúdo |
|---|---|
| `docs/SPEC.md` | Tudo o que a especificação do antigo `go-syslog-server` (hoje `../go-syslog-standard-server/`) dizia sobre o RELP, extraído e reunido para reaproveitamento: moldura, diálogo, janela, regra de aceitação, backpressure, boot, shutdown, configuração, métricas, testes, lacunas e os bugs que já custaram caro. Mais as questões que a versão LogHub precisa decidir antes de virar código. |

O código de referência, que funciona e foi provado contra o `rsyslog` real, fica
em `../go-syslog-standard-server/internal/receiver/relp.go` e `relpframe.go`, com
os testes em `framing_test.go`, `receiver_test.go` e
`../go-syslog-standard-server/internal/app/integration_test.go`.
