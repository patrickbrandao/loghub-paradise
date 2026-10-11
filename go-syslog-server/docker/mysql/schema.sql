-- go-syslog-server -- tabela de eventos do sink=mysql (MySQL 8.0+, MariaDB 10.6+).
--
-- Copia canonica de docs/SPEC.md §3.7. Mudar um dos dois exige mudar o outro,
-- e os testes de validacao de esquema (internal/sink) junto: eles criam as
-- tabelas deles a partir DESTE arquivo.
--
-- O SERVIDOR NUNCA EXECUTA ESTE ARQUIVO (SPEC §10.6, invariante §20.17). Ele so
-- valida, no boot, que a tabela existe em InnoDB com as dezoito colunas nas
-- familias de tipo de §3.7, que ha um indice unico exatamente sobre (uuid, step),
-- que max_allowed_packet comporta a maior linha possivel (SPEC §3.9) e que o
-- usuario consegue escrever (uma sonda com o mesmo INSERT do dreno e uuid de
-- dezesseis zeros, numa transacao revertida) -- e sai com codigo 3 se algo
-- faltar. Quem aplica e quem administra o banco:
--
--   mysql -u root -p syslog < docker/mysql/schema.sql
--
-- O usuario do syslog precisa de INSERT e UPDATE na tabela: o
-- ON DUPLICATE KEY UPDATE uuid = uuid que torna a escrita idempotente exige
-- UPDATE mesmo sem mudar nada. O UPDATE pode ficar restrito a coluna uuid:
--
--   CREATE USER 'syslog'@'%' IDENTIFIED BY '...';
--   GRANT INSERT, UPDATE (uuid) ON syslog.syslog_events TO 'syslog'@'%';
--
-- Nunca INSERT IGNORE: ele rebaixa 1406 e 1366 a aviso e trunca em silencio.
--
-- hostname, appname, procid e msgid sao LONGTEXT: a RFC 3164 nao tem teto para
-- hostname nem para o PID, e com o modo estrito um valor acima da coluna e 1406,
-- classe logic, e descarta o pedaco inteiro. O indice usa prefixo de 255.
-- utf8mb4_bin: comparacao byte a byte em WHERE hostname = ?.
--
-- Colunas extras sao toleradas. Os textos aceitam char, varchar ou qualquer
-- text; env aceita varbinary ou qualquer blob; os inteiros aceitam qualquer
-- inteiro. uuid precisa ser binary(16). InnoDB e obrigatorio.

CREATE TABLE IF NOT EXISTS syslog_events (
  uuid      BINARY(16)   NOT NULL,
  step      TINYINT      NOT NULL,
  space     VARCHAR(64)  NOT NULL,
  level     TINYINT      NOT NULL,
  sid       BIGINT       NOT NULL,
  ev_uts    BIGINT       NOT NULL,
  ev_sub    INT          NOT NULL,
  facility  TINYINT      NOT NULL,
  severity  TINYINT      NOT NULL,
  format    VARCHAR(16)  NOT NULL,
  transport VARCHAR(16)  NOT NULL,
  src_ip    VARCHAR(64),
  hostname  LONGTEXT,
  appname   LONGTEXT,
  procid    LONGTEXT,
  msgid     LONGTEXT,
  env       LONGBLOB     NOT NULL,
  nbytes    INT          NOT NULL,
  PRIMARY KEY (uuid, step),
  INDEX sev_uts  (ev_uts),
  INDEX sev_host (hostname(255), ev_uts),
  INDEX sev_sev  (severity, ev_uts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
