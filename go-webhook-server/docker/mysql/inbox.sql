-- go-webhook-server -- tabela inbox do sink=mysql, espelho da loghub.inbox.
--
-- Copia canonica de docs/SPEC.md §3.7: as onze colunas, a chave (uuid) e as
-- CHECKs da fila loghub.inbox do PostgreSQL (skill loghub-postgres-table-inbox),
-- nos tipos do MySQL. Um consumidor escrito para a fila le esta tabela com o
-- mesmo mapeamento. MySQL 8.0.16+ (CHECK aplicada).
--
-- MariaDB NAO e suportado (SPEC §8.6): o JSON dele e um apelido de LONGTEXT,
-- entao mesmo esta DDL aplicada sem mudanca vira payload longtext, e o boot sai
-- com codigo 3 dizendo isso.
--
-- O SERVIDOR NUNCA EXECUTA ESTE ARQUIVO (SPEC §8.6). Ele so valida, no boot, o
-- motor InnoDB; as dez colunas que escreve, com as larguras desta DDL ou maiores
-- (BIGINT em sid, INT em step, SMALLINT em level e event_nsec, 64 caracteres em
-- space e type, DATETIME(6) em event_at e, se existir, em received_at); o
-- indice UNICO exatamente sobre (uuid) e nenhum outro indice UNICO, salvo o de
-- uma coluna AUTO_INCREMENT (o ON DUPLICATE KEY UPDATE dispara em qualquer
-- chave unica e engoliria um evento novo como duplicata); o max_allowed_packet;
-- e um INSERT de sonda revertido, com cada valor no limite do JLH -- e sai com
-- codigo 3 se algo faltar (SPEC §8.6.4).
--
--   mysql -u root -p webhook < docker/mysql/inbox.sql
--
-- O usuario precisa de INSERT e de UPDATE (o ON DUPLICATE KEY UPDATE exige os
-- dois, mesmo com a atualizacao vazia); o UPDATE pode ficar so na coluna uuid:
--
--   CREATE USER 'webhook'@'%' IDENTIFIED BY '...';
--   GRANT INSERT, UPDATE (uuid) ON webhook.inbox TO 'webhook'@'%';
--
-- event_at e received_at sao UTC: o servidor envia event_at em UTC e fixa
-- time_zone = '+00:00' na sessao, entao o CURRENT_TIMESTAMP(6) tambem e UTC.

CREATE TABLE IF NOT EXISTS inbox (
  uuid        BINARY(16)   NOT NULL,
  from_uuid   BINARY(16)   NOT NULL,
  event_at    DATETIME(6)  NOT NULL,
  received_at DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  sid         BIGINT       NOT NULL DEFAULT 0,
  step        INT          NOT NULL DEFAULT 0,
  level       SMALLINT     NOT NULL,
  event_nsec  SMALLINT     NOT NULL DEFAULT 0,
  space       VARCHAR(64)  NOT NULL DEFAULT 'default',
  type        VARCHAR(64)  NOT NULL,
  payload     JSON         NOT NULL,
  CONSTRAINT inbox_pk PRIMARY KEY (uuid),
  CONSTRAINT inbox_uuid_v7_ck CHECK ((ascii(substr(uuid, 7, 1)) >> 4) = 7),
  CONSTRAINT inbox_from_v7_ck CHECK ((ascii(substr(from_uuid, 7, 1)) >> 4) = 7),
  CONSTRAINT inbox_type_ck    CHECK (type <> ''),
  CONSTRAINT inbox_level_ck   CHECK (level BETWEEN 0 AND 15),
  CONSTRAINT inbox_sid_ck     CHECK (sid >= 0),
  CONSTRAINT inbox_step_ck    CHECK (step BETWEEN 0 AND 65535),
  CONSTRAINT inbox_nsec_ck    CHECK (event_nsec BETWEEN 0 AND 999),
  CONSTRAINT inbox_payload_ck CHECK (json_type(payload) = 'OBJECT')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
