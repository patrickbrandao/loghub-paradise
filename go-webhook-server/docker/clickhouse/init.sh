#!/bin/bash
# go-webhook-server -- aplica clickhouse/inbox.sql dentro de CLICKHOUSE_DB.
#
# A imagem oficial do ClickHouse executa os *.sql de /docker-entrypoint-initdb.d
# SEM --database, ou seja, no banco default; a tabela precisa nascer no banco
# que o webhook usa (WHS_SINK_CLICKHOUSE_DATABASE). Por isso o .sql fica fora
# do initdb.d (/etc/whs) e este script o aplica, uma vez, na criacao do volume.
# Depois disso o esquema e' seu: o servidor de webhooks nunca o cria nem o
# altera (docs/SPEC.md §8.8.5).
set -euo pipefail
clickhouse-client --host 127.0.0.1 -u "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  --database "$CLICKHOUSE_DB" --multiquery < /etc/whs/inbox.sql
