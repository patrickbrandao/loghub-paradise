-- go-gelf-server -- tabela de eventos do sink=mysql (MySQL 8.0+, MariaDB 10.6+).
--
-- Copia canonica de docs/SPEC.md §3.7. Mudar um dos dois exige mudar o outro,
-- e os testes de validacao de esquema (internal/sink) junto: eles criam as
-- tabelas deles a partir DESTE arquivo.
--
-- O SERVIDOR NUNCA EXECUTA ESTE ARQUIVO (SPEC §10.6, invariante §20.17). Ele so
-- valida, no boot, que a tabela existe em InnoDB com as treze colunas nas
-- familias de tipo de §3.7, que ha um indice unico exatamente sobre (uuid, step),
-- que max_allowed_packet comporta a maior linha possivel (SPEC §3.9) e que o
-- usuario consegue escrever (uma sonda com o mesmo INSERT do dreno e uuid de
-- dezesseis zeros, numa transacao revertida) -- e sai com codigo 3 se algo
-- faltar. Quem aplica e quem administra o banco:
--
--   mysql -u root -p gelf < docker/mysql/schema.sql
--
-- O usuario do gelf precisa de INSERT e UPDATE na tabela: o
-- ON DUPLICATE KEY UPDATE uuid = uuid que torna a escrita idempotente exige
-- UPDATE mesmo sem mudar nada. O UPDATE pode ficar restrito a coluna uuid:
--
--   CREATE USER 'gelf'@'%' IDENTIFIED BY '...';
--   GRANT INSERT, UPDATE (uuid) ON gelf.gelf_events TO 'gelf'@'%';
--
-- Nunca INSERT IGNORE: ele rebaixa 1406 e 1366 a aviso e trunca em silencio.
--
-- hostname e LONGTEXT: o GELF nao tem teto para host (so o do documento,
-- convert.max_message_bytes), e com o modo estrito um valor acima da coluna e
-- 1406, classe logic, e descarta o pedaco inteiro. O indice usa prefixo de 255.
-- utf8mb4_bin: comparacao byte a byte em WHERE hostname = ?.
--
-- src_ip nunca e NULL (todo transporte deste servidor e de rede), mas a coluna
-- aceita NULL como no go-syslog-server; NOT NULL tambem passa no boot.
--
-- Consultas JSON sobre env: env guarda o JLH como chegou, e um campo GELF pode
-- trazer um numero fora do double (1e400). JSON_EXTRACT(CAST(env AS CHAR), ...)
-- ou CAST(env AS JSON) numa linha dessas aborta a consulta INTEIRA com 3141
-- ("Number too big to be stored in double"), e basta uma linha de um emissor
-- qualquer. Para ler env como JSON, use LIKE/REGEXP sobre CAST(env AS CHAR) ou
-- os destinos SQLite/ClickHouse (SPEC §3.7). O dado esta certo; o limite e' do
-- leitor JSON do MySQL.
--
-- Colunas extras sao toleradas. Os textos aceitam char, varchar ou qualquer
-- text; env aceita varbinary ou qualquer blob; os inteiros aceitam qualquer
-- inteiro. uuid precisa ser binary(16). InnoDB e obrigatorio.

CREATE TABLE IF NOT EXISTS gelf_events (
  uuid      BINARY(16)   NOT NULL,
  step      TINYINT      NOT NULL,
  space     VARCHAR(64)  NOT NULL,
  level     TINYINT      NOT NULL,
  sid       BIGINT       NOT NULL,
  ev_uts    BIGINT       NOT NULL,
  ev_sub    INT          NOT NULL,
  format    VARCHAR(16)  NOT NULL,
  transport VARCHAR(16)  NOT NULL,
  src_ip    VARCHAR(64),
  hostname  LONGTEXT,
  env       LONGBLOB     NOT NULL,
  nbytes    INT          NOT NULL,
  PRIMARY KEY (uuid, step),
  INDEX gev_uts   (ev_uts),
  INDEX gev_host  (hostname(255), ev_uts),
  INDEX gev_level (level, ev_uts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
