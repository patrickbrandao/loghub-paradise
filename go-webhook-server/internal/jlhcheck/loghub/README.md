# Cópias normativas do LogHub (não edite à mão)

Copiadas em 2026-10-06 dos pacotes das skills do servidor MCP `loghub-mcp`:

| Arquivo | Origem |
|---|---|
| `jlh-envelope.schema.json` | skill `loghub-json-schema` 1.0.0, `assets/` (a mesma cópia do `go-syslog-server`) |
| `jlh-webhook.schema.json` | skill `loghub-type-webhook-schema`, `references/` |

Só os testes as leem (SPEC §3.0, §15.1). Ao atualizar uma skill, substitua o
arquivo inteiro e rode `go test -race ./internal/jlhcheck/ ./internal/envelope/`.
