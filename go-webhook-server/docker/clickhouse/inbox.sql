-- go-webhook-server -- tabela inbox do sink=clickhouse, espelho da loghub.inbox.
--
-- Copia canonica de docs/SPEC.md §3.9: as onze colunas da fila loghub.inbox do
-- PostgreSQL (skill loghub-postgres-table-inbox), nos tipos do ClickHouse. Um
-- consumidor escrito para a fila le esta tabela com o mesmo mapeamento.
-- ClickHouse 25.8+ (json_type_escape_dots_in_keys).
--
-- O SERVIDOR NUNCA EXECUTA ESTE ARQUIVO (SPEC §8.8.5). Ele so valida, no boot:
-- a versao; o motor *ReplacingMergeTree; uuid como elemento da chave de
-- ordenacao; as dez colunas que escreve, com estes tipos (DateTime64 de 6 a 9
-- digitos, String ou LowCardinality(String) em space e type, JSON com os dois
-- hints abaixo e, alem deles, so max_dynamic_paths/max_dynamic_types);
-- received_at, se existir, com DEFAULT; nenhuma CONSTRAINT CHECK;
-- o max_memory_usage contra o pior lote; e um INSERT vazio de sonda -- e sai
-- com codigo 3 se algo faltar.
--
--   clickhouse-client --database webhook --multiquery < docker/clickhouse/inbox.sql
--
-- O usuario precisa so de INSERT nas dez colunas, e de um perfil com
-- readonly = 0:
--
--   CREATE USER webhook IDENTIFIED BY '...';
--   GRANT INSERT ON webhook.inbox TO webhook;
--
-- Com Replicated*ReplacingMergeTree, deixe o servidor ler o estado da replica
-- (sem isso a checagem de replica somente leitura fica desligada, com WARN):
--
--   GRANT SELECT(database, table, is_readonly) ON system.replicas TO webhook;
--
-- Tambem recusado no boot: coluna EPHEMERAL/MATERIALIZED/ALIAS com o nome de
-- uma das dez, ReplacingMergeTree com coluna is_deleted, e PARTITION BY que
-- leia outra coluna que nao event_at.
--
-- Por que cada escolha (SPEC §3.9):
--   ORDER BY (event_at, uuid): o ClickHouse ordena UUID pela SEGUNDA metade,
--     e uma chave so com uuid nao poda nada por tempo. Um uuid repetido tem
--     sempre o mesmo event_at, entao a chave colapsa so as reentregas.
--   ReplacingMergeTree: o merge colapsa a reentrega que o token nao pegou
--     (leitura exata com FINAL).
--   non_replicated_deduplication_window: sem ela (padrao 0) o
--     insert_deduplication_token de cada lote e ignorado num MergeTree sem
--     replica. Num Replicated*, apague a linha: o padrao dele ja guarda blocos.
--   PARTITION BY toYYYYMM(event_at): retencao por DROP PARTITION; a partir de
--     event_at para que um uuid caia sempre na mesma particao.
--   params e query como Map: o tipo JSON DESCARTA objeto vazio, e params e'
--     sempre {}; e as chaves da query (as unicas que o cliente controla) ficam
--     literais como chaves de Map, sem virar caminho dinamico. O hint e' do
--     webhook.request: o go-syslog-server tem a sua tabela (inbox_syslog).
--   Sem CONSTRAINT CHECK: a sonda de boot e vazia e nao a exercitaria, e uma
--     violacao derrubaria lotes inteiros como logic.

CREATE TABLE IF NOT EXISTS inbox (
  uuid        UUID,
  from_uuid   UUID,
  event_at    DateTime64(6, 'UTC'),
  received_at DateTime64(6, 'UTC') DEFAULT now64(6),
  sid         UInt64,
  step        UInt16,
  level       UInt8,
  event_nsec  UInt16,
  space       LowCardinality(String),
  type        LowCardinality(String),
  payload     JSON(params Map(String, String), query Map(String, Dynamic))
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_at)
ORDER BY (event_at, uuid)
SETTINGS non_replicated_deduplication_window = 1000;
