-- go-relplog-pg-server -- fila loghub.inbox, o unico destino.
--
-- DDL LITERAL da skill loghub-postgres-table-inbox (sql/inbox.sql), consultada
-- em 2026-10-06; docs/SPEC.md §3.7.1 a reproduz. Nao edite este bloco: a fila e
-- comum a todo produtor LogHub, e um teste (internal/sink) confere que o texto
-- que comeca na linha de sinais de igual e o da skill.
--
-- O SERVIDOR NUNCA EXECUTA ESTE ARQUIVO (SPEC §10.5, invariante §20.17). Ele so
-- valida, no boot, que a tabela existe com as dez colunas que ele escreve
-- (uuid, from_uuid, space, type, level, sid, step, event_at, event_nsec,
-- payload) em tipos compativeis, que ha um indice UNICO exatamente sobre
-- (uuid), que o papel tem INSERT e SELECT (uuid) e que o servidor nao esta em
-- recuperacao -- e
-- sai com codigo 3 se algo faltar. Quem aplica e quem administra o banco:
--
--   psql "$DSN" -f docker/postgres/inbox.sql
--
-- PostgreSQL 18+ (uuid_extract_version, COMPRESSION lz4). O servidor so
-- enfileira (INSERT ... ON CONFLICT (uuid) DO NOTHING); o consumo
-- (DELETE ... FOR UPDATE SKIP LOCKED em ordem de uuid) e de outro software.
--
-- O papel que o servidor usa precisa de INSERT e de SELECT na coluna uuid: o
-- ON CONFLICT (uuid) le a coluna arbitra, e um papel so com INSERT falha no
-- primeiro pedaco com 42501. UPDATE nao (DO NOTHING):
--
--   CREATE ROLE syslog LOGIN PASSWORD '...';
--   GRANT USAGE ON SCHEMA loghub TO syslog;
--   GRANT INSERT, SELECT (uuid) ON loghub.inbox TO syslog;
--
-- =====================================================================
-- LogHub · inbox: fila de espera de envelopes JLH (sem partição)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS loghub;

CREATE TABLE loghub.inbox (
  uuid        uuid        NOT NULL,                 -- envelope.uuid (UUIDv7) = ordem da fila
  from_uuid   uuid        NOT NULL,                 -- envelope.from
  event_at    timestamptz NOT NULL,                 -- event.uts + event.sub truncado em µs
  received_at timestamptz NOT NULL DEFAULT now(),   -- chegada na fila (mede o atraso)
  sid         bigint      NOT NULL DEFAULT 0,
  step        integer     NOT NULL DEFAULT 0,
  level       smallint    NOT NULL,
  event_nsec  smallint    NOT NULL DEFAULT 0,       -- resto de event.sub (0-999 ns)
  space       text        NOT NULL DEFAULT 'default',
  type        text        NOT NULL,
  payload     jsonb       COMPRESSION lz4 NOT NULL,

  CONSTRAINT inbox_pk PRIMARY KEY (uuid),

  CONSTRAINT inbox_uuid_v7_ck  CHECK (uuid_extract_version(uuid) = 7),
  CONSTRAINT inbox_from_v7_ck  CHECK (uuid_extract_version(from_uuid) = 7),
  CONSTRAINT inbox_space_ck    CHECK (space ~ '^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$'),
  CONSTRAINT inbox_type_ck     CHECK (type <> ''),
  CONSTRAINT inbox_level_ck    CHECK (level BETWEEN 0 AND 15),
  CONSTRAINT inbox_sid_ck      CHECK (sid >= 0),
  CONSTRAINT inbox_step_ck     CHECK (step BETWEEN 0 AND 65535),
  CONSTRAINT inbox_nsec_ck     CHECK (event_nsec BETWEEN 0 AND 999),
  CONSTRAINT inbox_event_ck    CHECK (event_at >= 'epoch'),
  CONSTRAINT inbox_payload_ck  CHECK (jsonb_typeof(payload) = 'object'),
  CONSTRAINT inbox_syslog_ck   CHECK (type <> 'syslog' OR (level <= 7 AND payload ? 'message')),
  CONSTRAINT inbox_signal_ck   CHECK (type NOT IN ('beacon','ping','pong') OR level IN (8, 9))
) WITH (
  -- fila = muitas inserções e remoções: vacuum frequente para reciclar páginas
  autovacuum_vacuum_scale_factor        = 0,
  autovacuum_vacuum_threshold           = 5000,
  autovacuum_vacuum_insert_scale_factor = 0,
  autovacuum_vacuum_insert_threshold    = 5000,
  autovacuum_analyze_scale_factor       = 0,
  autovacuum_analyze_threshold          = 5000
);

COMMENT ON TABLE loghub.inbox IS
  'Fila de espera de envelopes JLH. Consumo em ordem de uuid com FOR UPDATE SKIP LOCKED; a linha é removida ao ser processada.';
