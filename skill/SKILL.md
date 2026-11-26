---
name: loghub-paradise
description: Escolher e operar os receptores e injetores do LogHub Paradise em Docker. Use para configurar webhooks, syslog, RELP e GELF, escolher PostgreSQL ou NATS, testar ingestão e conferir entrega.
---

# LogHub Paradise

1. Identifique o protocolo de entrada na tabela.
2. Escolha um receptor pelo destino. Use a variante PostgreSQL ou NATS quando o destino for fixo. Escolha um injetor para teste, carga ou replay.
3. Abra **só o manual da aplicação escolhida**. Para implantar ou testar em Docker, abra também o Compose da mesma linha. Os caminhos partem desta pasta.

Os receptores LogHub convertem eventos para o envelope JLH. O *ring* é a fila de entrada. O *sink* é o destino. Aceitação pelo receptor e gravação no destino são etapas diferentes.

As imagens públicas estão em `tmsoftbrasil/<programa>` e suportam Linux amd64 e arm64. Os exemplos usam Docker Compose 2.23.1 ou posterior e uma tag publicada fixa. Use `export LOGHUB_TAG=latest` para selecionar a tag móvel.

## Aplicações

| Protocolo | Programa | Função | Arquivos |
| --- | --- | --- | --- |
| HTTP | `go-webhook-server` | Receptor HTTP geral. Emite JLH `webhook.request` e oferece confirmação rápida ou após gravar. | [Manual](references/go-webhook-server.md) · [Compose](examples/go-webhook-server.compose.yml) |
| HTTP | `go-webhook-fast-server` | Receptor HTTP rápido. Emite JLH `webhook` e indica a etapa no retorno. | [Manual](references/go-webhook-fast-server.md) · [Compose](examples/go-webhook-fast-server.compose.yml) |
| HTTP | `go-webhook-pg-server` | Receptor HTTP rápido com fila PostgreSQL fixa. | [Manual](references/go-webhook-pg-server.md) · [Compose](examples/go-webhook-pg-server.compose.yml) |
| HTTP | `go-webhook-nats-server` | Receptor HTTP rápido com publicação NATS fixa. | [Manual](references/go-webhook-nats-server.md) · [Compose](examples/go-webhook-nats-server.compose.yml) |
| HTTP | `go-webhook-injector` | Gera requisições HTTP ou reproduz requisições capturadas. | [Manual](references/go-webhook-injector.md) · [Compose](examples/go-webhook-injector.compose.yml) |
| Syslog | `go-syslog-server` | Receptor syslog para LogHub. Aceita UDP, TCP, TLS e socket Unix. | [Manual](references/go-syslog-server.md) · [Compose](examples/go-syslog-server.compose.yml) |
| Syslog | `go-syslog-pg-server` | Receptor syslog com fila PostgreSQL fixa. | [Manual](references/go-syslog-pg-server.md) · [Compose](examples/go-syslog-pg-server.compose.yml) |
| Syslog | `go-syslog-nats-server` | Receptor syslog com publicação NATS fixa. | [Manual](references/go-syslog-nats-server.md) · [Compose](examples/go-syslog-nats-server.compose.yml) |
| Syslog | `go-syslog-standard-server` | Receptor legado de syslog e RELP com formato próprio. | [Manual](references/go-syslog-standard-server.md) · [Compose](examples/go-syslog-standard-server.compose.yml) |
| Syslog | `go-syslog-injector` | Envia syslog por UDP, TCP, TLS ou socket Unix para testes. | [Manual](references/go-syslog-injector.md) · [Compose](examples/go-syslog-injector.compose.yml) |
| RELP | `go-relplog-server` | Receptor RELP para LogHub. Recebe do rsyslog e confirma por mensagem. | [Manual](references/go-relplog-server.md) · [Compose](examples/go-relplog-server.compose.yml) |
| RELP | `go-relplog-pg-server` | Receptor RELP em TCP com fila PostgreSQL fixa. | [Manual](references/go-relplog-pg-server.md) · [Compose](examples/go-relplog-pg-server.compose.yml) |
| RELP | `go-relplog-nats-server` | Receptor RELP em TCP com publicação NATS fixa. | [Manual](references/go-relplog-nats-server.md) · [Compose](examples/go-relplog-nats-server.compose.yml) |
| RELP | `go-relplog-injector` | Envia syslog por RELP e mede as confirmações por mensagem. | [Manual](references/go-relplog-injector.md) · [Compose](examples/go-relplog-injector.compose.yml) |
| GELF | `go-gelf-server` | Receptor GELF 1.1 para LogHub por UDP, TCP, TLS, HTTP e HTTPS. | [Manual](references/go-gelf-server.md) · [Compose](examples/go-gelf-server.compose.yml) |
| GELF | `go-gelf-pg-server` | Receptor GELF por UDP, TCP e HTTP com fila PostgreSQL fixa. | [Manual](references/go-gelf-pg-server.md) · [Compose](examples/go-gelf-pg-server.compose.yml) |
| GELF | `go-gelf-nats-server` | Receptor GELF por UDP, TCP e HTTP com publicação NATS fixa. | [Manual](references/go-gelf-nats-server.md) · [Compose](examples/go-gelf-nats-server.compose.yml) |
| GELF | `go-gelf-injector` | Gera ou reproduz mensagens GELF para testes e carga. | [Manual](references/go-gelf-injector.md) · [Compose](examples/go-gelf-injector.compose.yml) |
