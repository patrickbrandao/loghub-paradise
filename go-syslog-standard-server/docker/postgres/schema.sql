-- go-syslog-server -- tabela de eventos do sink=postgres.
--
-- Copia canonica de docs/SPEC.md §3.6. Mudar um dos dois exige mudar o outro,
-- e o teste de validacao de esquema (internal/sink) junto.
--
-- O SERVIDOR NUNCA EXECUTA ESTE ARQUIVO. Ele so' valida, no boot, que a tabela
-- existe com estas colunas, um indice unico sobre (id) e permissao de INSERT
-- (SPEC §10.5.5). Quem aplica e' quem administra o banco -- N instancias subindo
-- juntas correriam no catalogo, e o papel de producao nao tem privilegio de DDL.
--
--   psql "$DSN" -f docker/postgres/schema.sql

CREATE TABLE IF NOT EXISTS events (
  id         uuid      PRIMARY KEY,   -- UUIDv7 Level2, indexavel e ordenavel
  ts         bigint    NOT NULL,      -- unix seconds da RECEPCAO
  nts        bigint    NOT NULL,      -- fracao ns dentro do segundo
  facility   smallint  NOT NULL,      -- 0..23, efetiva
  severity   smallint  NOT NULL,      -- 0..7, efetiva
  hostname   text      NOT NULL,      -- alegado pela mensagem; U+FFFD se invalido
  app_name   text      NOT NULL,
  procid     text      NOT NULL,
  msgid      text      NOT NULL,
  peer_ip    text      NOT NULL,      -- verdade do fio
  transport  text      NOT NULL,      -- udp | tcp | unix | relp
  rfc        text      NOT NULL,      -- 5424 | 3164 | unknown
  msg_offset bigint    NOT NULL,      -- inicio do MSG dentro de raw
  msg_size   bigint    NOT NULL,
  env        bytea     NOT NULL,      -- envelope JSON completo
  raw        bytea     NOT NULL,      -- a linha crua, byte a byte
  nbytes     bigint    NOT NULL       -- len(env) + len(raw)
);

CREATE INDEX IF NOT EXISTS ev_ts   ON events (ts);
CREATE INDEX IF NOT EXISTS ev_host ON events (hostname, ts);
CREATE INDEX IF NOT EXISTS ev_sev  ON events (severity, ts);

-- O quarto indice obvio fica de fora de proposito: cada indice e' uma escrita a
-- mais por linha num destino cujo gargalo e' escrita. Quem consulta por
-- aplicacao o tempo todo cria:
--
--   CREATE INDEX ev_app ON events (app_name, ts);

-- O papel que o servidor usa precisa de INSERT e de nada mais.
--
--   CREATE ROLE syslog LOGIN PASSWORD '...';
--   GRANT INSERT ON events TO syslog;
