-- go-syslog-server -- tabela de eventos do sink=mysql.
--
-- Copia canonica de docs/SPEC.md §3.7. Mudar um dos dois exige mudar o outro,
-- e os testes de validacao de esquema (internal/sink, internal/app) junto: eles
-- criam as tabelas deles a partir DESTE arquivo.
--
-- O SERVIDOR NUNCA EXECUTA ESTE ARQUIVO. Ele so' valida, no boot, que a tabela
-- existe em InnoDB com estas colunas e familias de tipo, que ha' um indice unico
-- sobre (id), que o usuario consegue escrever (uma sonda com o mesmo INSERT do
-- dreno, numa transacao revertida) e que max_allowed_packet comporta a maior
-- linha aceitavel -- e sai com codigo 3 se algo faltar (SPEC §10.7.4). Quem
-- aplica e' quem administra o banco:
--
--   mysql -u root -p syslog < docker/mysql/schema.sql
--
-- O usuario do syslog precisa de INSERT e UPDATE na tabela: o
-- ON DUPLICATE KEY UPDATE id = id que torna a escrita idempotente exige UPDATE
-- mesmo sem mudar nada. O UPDATE pode ficar restrito a coluna id:
--
--   GRANT INSERT, UPDATE (id) ON syslog.events TO 'syslog'@'%';
--
-- O compose monta este arquivo em /docker-entrypoint-initdb.d, entao a imagem
-- oficial o executa uma vez, na criacao do volume, dentro de MYSQL_DATABASE.
--
-- Colunas extras sao toleradas. As de texto aceitam char, varchar ou qualquer
-- text; env e raw aceitam varbinary ou qualquer blob; facility e severity
-- aceitam qualquer inteiro. id precisa ser binary(16). InnoDB e' obrigatorio.
-- Tipos mais estreitos que estes sao aceitos e trocam espaco por um 1406 quando
-- um valor nao cabe, com o lote descartado e contado (SPEC §3.7).

CREATE TABLE IF NOT EXISTS events (
  id         BINARY(16)   NOT NULL PRIMARY KEY,  -- UUIDv7 Level2: cronologico, insere no fim do indice
  ts         BIGINT       NOT NULL,              -- unix seconds da RECEPCAO
  nts        BIGINT       NOT NULL,              -- fracao ns dentro do segundo
  facility   TINYINT      NOT NULL,              -- 0..23, efetiva
  severity   TINYINT      NOT NULL,              -- 0..7, efetiva
  hostname   LONGTEXT     NOT NULL,              -- alegado pela mensagem, LONGTEXT porque na 3164 nao tem teto
  app_name   LONGTEXT     NOT NULL,
  procid     LONGTEXT     NOT NULL,              -- na 3164 e' uma sequencia de digitos sem teto
  msgid      LONGTEXT     NOT NULL,
  peer_ip    VARCHAR(64)  NOT NULL,              -- verdade do fio
  transport  VARCHAR(16)  NOT NULL,              -- udp | tcp | unix | relp
  rfc        VARCHAR(16)  NOT NULL,              -- 5424 | 3164 | unknown
  msg_offset BIGINT       NOT NULL,              -- inicio do MSG dentro de raw
  msg_size   BIGINT       NOT NULL,
  env        LONGBLOB     NOT NULL,              -- envelope JSON completo, byte a byte
  raw        LONGBLOB     NOT NULL,              -- a linha crua, byte a byte
  nbytes     BIGINT       NOT NULL,              -- len(env) + len(raw)
  INDEX ev_ts   (ts),
  INDEX ev_host (hostname(255), ts),
  INDEX ev_sev  (severity, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

-- O quarto indice obvio fica de fora de proposito, como no postgres: cada
-- indice e' uma escrita a mais por linha num destino cujo gargalo e' escrita.
-- Quem consulta por aplicacao o tempo todo cria:
--
--   CREATE INDEX ev_app ON events (app_name(64), ts);
--
-- Busca textual: LIKE sobre LONGBLOB compara bytes (e distingue caixa), e o MSG
-- sai com SUBSTRING(raw, msg_offset + 1, msg_size).
--
-- Retencao e' externa: particione por faixa de ts e descarte particoes. Um
-- DELETE em massa numa tabela InnoDB grande segura o historico de undo.
