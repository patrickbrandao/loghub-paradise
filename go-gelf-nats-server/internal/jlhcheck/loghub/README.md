# Schemas do oráculo

| Arquivo | Origem |
|---|---|
| `jlh-envelope.schema.json`, `examples/valid/`, `examples/invalid/` | cópia normativa da skill `loghub-json-schema` 1.0.0, `assets/` (2026-10-05); não edite à mão |
| `jlh-gelf-payload.schema.json` | **deste repositório** (não há skill `type=gelf`, decisão 1 da família GELF, 2026-10-31): contrato do payload de SPEC §3.2, §6.8, §6.9 |

Mudar o schema do payload é mudar o contrato: SPEC primeiro, depois o
schema, o conversor e os `.want.json` do corpus
(`internal/convert/testdata/corpus/`), e rodar
`go test -race -run 'Corpus|Oracle' ./internal/convert/ ./internal/jlhcheck/`.
