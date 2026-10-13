# Cópias normativas do LogHub (não edite à mão)

Copiadas em 2026-10-05 dos pacotes das skills do servidor MCP `loghub-mcp`:

| Arquivo | Origem |
|---|---|
| `jlh-envelope.schema.json`, `examples/valid/`, `examples/invalid/` | skill `loghub-json-schema` 1.0.0, `assets/` |
| `jlh-syslog-payload.schema.json` | skill `loghub-type-syslog-schema`, `schemas/` |
| `vectors.json` | skill `loghub-type-syslog-schema`, `testdata/` (24 vetores dourados) |

Ao atualizar uma skill, substitua o arquivo inteiro e rode
`go test -race -run 'Golden|Schema' ./internal/convert/ ./internal/jlhcheck/`. SPEC §18.2.
