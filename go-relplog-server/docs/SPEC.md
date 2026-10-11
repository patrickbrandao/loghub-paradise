# go-relplog-server — Especificação do RELP (extraída, para reaproveitamento)

Status: **rascunho de reaproveitamento, não congelado**
Data: 2026-10-05
Origem: `go-syslog-server` v1 de 2026-09-12/15, hoje `../go-syslog-standard-server/`
(`docs/SPEC.md` §2.3, §4.5, §5.4, §5.5, §9.1–§9.4, §12.1, §12.3, §13.1, §15.5,
§17.4, §17.7, §18.2; `docs/TESTING.md` §5, §7, §8, §9).
Revisão: 2026-10-11 — a janela segura cópias fora do ring e a implementação
nova precisa de um teto em bytes para elas (§4, §6, §8.3, §10); o shutdown
fecha sem `serverclose`, como o código de referência faz (§5.2).

Este documento junta, num lugar só, tudo o que a especificação anterior dizia
sobre o RELP. O texto foi preservado e reorganizado, com as correções da revisão
acima. As seções 1 a 9 descrevem
o que **foi implementado e provado** na standard e dizem onde a implementação
nova precisa fazer diferente dela; a seção 10 lista o que a versão LogHub
precisa decidir antes de virar código. Onde aparecem `ring`, `sink`,
`Enqueue` ou `Health()`, são os conceitos da standard: ring = onde a mensagem
espera, sink = destino final, ortogonais.

---

## 1. Por que RELP

O RELP é o protocolo do `rsyslog`. Não é RFC, mas é a **única** forma difundida
de fazer syslog com confirmação por mensagem, e por isso é o único transporte em
que a regra de aceitação abaixo pode ser cumprida ao pé da letra:

> A confirmação sai no instante em que a garantia da combinação configurada é
> cumprida — nunca antes.

| Transporte | A confirmação existe? | O que a substitui |
|---|---|---|
| **RELP** | **sim**, `rsp 200 OK` | — |
| TCP | só em parte: a retomada da leitura | nada se perde, mas o emissor não sabe *qual* mensagem foi aceita |
| unix dgram | não | o bloqueio do `send` pelo kernel |
| UDP | não | contador de descarte e contadores da interface |

**Só o RELP cumpre ao pé da letra a promessa de "nunca confirmar o que não foi
aceito".** Quem não pode perder mensagem usa RELP; quem usa UDP escolheu um
transporte que perde.

---

## 2. Moldura

```
TXNR SP COMMAND SP DATALEN [SP DATA] TRAILER
```

- `TXNR`: número de transação em decimal, escolhido pelo cliente. Vale `0` só no
  `serverclose`.
- `COMMAND`: `open`, `syslog` e `close` vêm do cliente; `rsp` e `serverclose`, do
  servidor.
- `DATALEN`: comprimento de `DATA` em decimal.
- `TRAILER`: um `\n`.

Tetos do leitor (código de referência): `TXNR` com até 12 dígitos, `COMMAND`
com até 32 bytes, `DATALEN` com até 12 dígitos.

**`DATALEN = 0` aparece de duas formas.** Os emissores reais divergem: alguns
escrevem `3 close 0\n` e outros `3 close 0 \n`. O parser aceita as duas, porque o
espaço antes de `DATA` é opcional quando `DATALEN` é zero. Recusar uma das formas
seria recusar metade do parque instalado por causa de um byte.

**Uma moldura que não dá para ressincronizar fecha a conexão.** Isso vale para
`DATALEN` não numérico, `DATALEN` acima de `relp.max_frame_bytes`, um frame que
não fecha com `\n` e qualquer outro caso em que o fluxo dessincronizou. Procurar
"o próximo frame" gravaria fragmentos como mensagens.

**O buffer cresce com os bytes, nunca com o `DATALEN` declarado.** O leitor
recusa um `DATALEN` acima do teto **antes de ler um byte do payload**. Abaixo do
teto, o buffer cresce em blocos de 64 KiB conforme os bytes chegam. Quem declara
16 MiB e fica calado custa um bloco, mais o buffer de leitura da conexão
(§8.3, item 3). Sem essa regra, mil conexões custariam 16 GiB residentes por
nove bytes enviados cada (bug real de 2026-09-14; §9).

**O cabeçalho do frame RELP não pertence à mensagem.** `TXNR`, `COMMAND`,
`DATALEN` e o `\n` final nunca entram na linha crua gravada.

---

## 3. Diálogo

```
cliente  1 open 71 relp_version=0\nrelp_software=librelp,1.11.0\ncommands=syslog\n
servidor 1 rsp 84 200 OK\nrelp_version=0\nrelp_software=<nome>,<versão>\ncommands=syslog\n

cliente  2 syslog 38 <13>Sep 12 14:33:01 web-01 app: oi
servidor 2 rsp 6 200 OK

cliente  3 close 0
servidor 3 rsp 0
servidor 0 serverclose 0
```

- A resposta ao `open` anuncia `relp_version=0`, o nome e a versão do software
  (para um operador lendo o log do rsyslog saber que build está do outro lado) e
  `commands=syslog`.
- O **`rsp 200 OK` só sai depois da garantia do ring.** É o `200` do HTTP, com o
  mesmo significado e a mesma proibição de sair antes.
- Uma mensagem recusada recebe `500` com o motivo em texto: `500 ring full`,
  `500 sink down`, `500 shutdown`. O `rsyslog` retém e retenta, que é o
  comportamento desejado.
- **Mensagem filtrada recebe `200 OK`.** O operador pediu para descartá-la; mandar
  o emissor retentar seria mentir (`TestRELPAcknowledgesAFilteredMessage`).
- **`COMMAND` desconhecido recebe `500 command not supported` e a conexão
  continua.** Um comando novo é extensão do cliente, não fluxo quebrado
  (`TestRELPUnknownCommandKeepsTheConnection`).
- O `close` recebe `rsp` com corpo vazio e, em seguida, o servidor envia
  `0 serverclose 0`. O `serverclose` entra na fila **atrás** das confirmações já
  decididas: sem resposta ao `close`, a janela do cliente nunca reabre.

### 3.1 Janela

O cliente mantém uma **janela** de transações não confirmadas (`relp.window`,
padrão 128), negociada implicitamente pelo comportamento. Ao esgotá-la, ele para
de enviar sozinho: é o backpressure do RELP, mais limpo que o do TCP porque é
explícito no protocolo.

- **As confirmações podem sair fora de ordem.** O `TXNR` está na resposta, e isso
  permite confirmar um lote inteiro de uma vez quando o ring confirma.
- A janela é o motivo de o trabalho em voo ser espalhado por goroutines. Com
  `ring=sqlite`, um `Enqueue` espera o group commit, e uma conexão só (o rsyslog
  abre exatamente uma) seria serializada por ele. Até `window` transações correm
  em paralelo, cada uma esperando a sua garantia
  (`TestRELPWindowAllowsConcurrentTransactions`).
- As confirmações são escritas por uma goroutine **separada**, que só enxerga
  transações já decididas. Um atalho "responde já, enfileira depois"
  transformaria o único transporte confiável do servidor no menos confiável.

---

## 4. Aceitação e backpressure

| `ring` | O `200 OK` significa |
|---|---|
| `memory` | o item entrou no canal do ring |
| `sqlite` | o `COMMIT` do grupo no `ring.db` terminou (com `fsync` se `synchronous=FULL`) |
| `none` | o **sink** confirmou a escrita (ou na hora, com `sink=none`) |

**Ring cheio ou sink DOWN:** cada transação lida espera a sua garantia, e o
leitor só **para de ler** quando `relp.window` transações estão em voo; daí em
diante o cliente esgota a própria janela e segura as mensagens, como no TCP. Em
vez de ficar calado, porém, o servidor responde `500 ring full` (ou
`500 sink down`) a cada transação cujo `relp.backpressure_timeout` (padrão
30 s) se esgota, e a conexão continua viva. É estritamente melhor que o TCP, e
é a razão de o RELP ser o transporte recomendado para quem não pode perder.

```
ring cheio detectado
  -> cada transação espera, checando a cada 50 ms
  -> a janela enche: o leitor para de ler e o cliente segura as mensagens
  -> ring escoou?  as transações confirmam e o leitor volta a ler; nada se perdeu
  -> estourou?     500 ring full para a transação; a conexão continua
```

**As cópias da janela não estão no ring.** O buffer do frame é reaproveitado
pela leitura seguinte, então cada transação em voo segura a própria cópia da
linha crua, de até `relp.max_frame_bytes`, durante todo o backpressure. Nada
disso conta em `ring.max_bytes`: com os padrões são até 128 × 16 MiB, cerca de
2 GiB por conexão, sem teto com `relp.max_conns = 0`, justamente quando o ring
está cheio. Por isso a implementação nova:

- copia o frame só **depois** de obter a vaga na janela (o código de referência
  copia antes e chega a `window + 1` cópias por conexão);
- limita os bytes dessas cópias por um **teto global ao processo**, adquirido
  antes da cópia e devolvido quando a transação é decidida; esgotado, o leitor
  para de ler e a janela TCP segura o emissor;
- dá a esse teto pelo menos `relp.max_frame_bytes`, para um frame único não
  travar a conexão, e soma o termo à conta de memória em voo do processo.

**Rate-limit estourado é recusado na hora**, sem a espera de backpressure, com
`500 <motivo>` e o descarte contado. Um emissor acima do limite **não** escoa
sozinho: segurar a janela dele só transferiria a fila para o outro lado. Na
standard a regra é genérica: toda recusa não retentável (`Retryable() == false`)
responde `500` na hora, exceto a filtrada, que recebe `200 OK`; as retentáveis
(ring cheio, sink DOWN) passam pelo laço de backpressure.

**`Enqueue` cancelado no meio do group commit** (`ring=sqlite`, durante o
shutdown): o `Enqueue` espera o `COMMIT` mesmo com o contexto cancelado. Voltar
antes produziria `500 shutdown` para uma linha que será commitada de qualquer
forma, uma duplicata garantida na retransmissão do rsyslog, e devolveria ao pool
um buffer que a escritora ainda vai ler.

---

## 5. Ciclo de vida

### 5.1 Boot

Os transportes abrem **por último**, depois do sink pronto e do backlog drenado,
na ordem unix, udp, tcp, **relp**. O transporte com a garantia mais forte é o
último a abrir e o primeiro a fechar: a janela em que o serviço aceita RELP é um
subconjunto da janela em que ele aceita todo o resto.

Validação: `listen.tcp` e `listen.relp` na mesma porta é `exit 1`, porque são
protocolos diferentes no mesmo socket (`TestTCPAndRELPCannotShareAPort`).

### 5.2 Shutdown

```
fase 1  /readyz passa a falhar
fase 2  shutdown.pre_stop_delay, ainda aceitando
fase 3  fecha os transportes na ordem inversa do boot: relp, tcp, udp, unix
        RELP: para de ler; as transações em voo recebem 200 OK ou
              500 shutdown, o escritor esvazia e a conexão fecha
fase 4  drena o ring para o sink (inclui o que os workers seguram)
fase 5  fecha tudo, exit 0
```

- Na fase 3, o contexto de recepção é cancelado **antes** de os transportes
  fecharem. É esse cancelamento que solta um leitor preso no laço de
  backpressure e que produz o `500 shutdown`.
- **O `Close` não fecha o socket direto.** Ele só expira o prazo de leitura e deixa
  o escritor esvaziar as confirmações já decididas. Fechar o socket perderia o
  `200 OK` de mensagens que já estão no ring, e elas virariam duplicatas na
  retransmissão (bug real; §9).
- **No shutdown não há `serverclose`.** Ele só sai em resposta a um `close` do
  cliente (§3). Frames ainda não lidos ficam sem resposta, e o rsyslog, ao ver
  o fim da conexão, retransmite o que não foi confirmado, então nada se perde.
  Continuar lendo na fase 3 para responder `500 shutdown` ao que ainda chegasse
  contradiria o prazo de leitura expirado e atrasaria o shutdown.
- O `stop_grace_period` do orquestrador precisa ser **maior** que
  `pre_stop_delay + drain_timeout`. Menor significa ser morto no meio do dreno,
  perdendo mensagens que já foram confirmadas por RELP.

---

## 6. Configuração

`[listen]`:

| Chave | Padrão | Descrição |
|---|---|---|
| `relp` | `""` | Endereço RELP; `:20514` é o costume (porta alta: a imagem roda como uid 65532) |
| `relp_listeners` | `1` | `>1` habilita `SO_REUSEPORT` |

`[relp]`:

| Chave | Padrão | Descrição |
|---|---|---|
| `max_frame_bytes` | `16MiB` | Fusível do `DATALEN`; acima dele a conexão fecha |
| `idle_timeout` | `120s` | `0` = nunca |
| `max_conns` | `0` | Por transporte, somando os listeners; bloqueia no accept |
| `max_conns_per_ip` | `0` | Por transporte; fecha na hora |
| `window` | `128` | Transações em voo por conexão antes de parar de ler; cada uma segura uma cópia da linha crua fora do ring (§4) |
| `backpressure_timeout` | `30s` | Espera antes de responder `500 ring full`; zero é recusado |

---

## 7. Métricas (prefixo da standard: `syslog_`)

| Métrica | Tipo | Labels |
|---|---|---|
| `*_relp_frames_total` | counter | `command` (open, syslog, close, unknown) |
| `*_conns_active`, `*_conns_total` | gauge, counter | `transport` |
| `*_backpressure_seconds_total` | counter | `transport` |
| `*_confirm_duration_seconds` | histogram | — (do frame completo até a confirmação; só mensagens aceitas) |

---

## 8. Testes

### 8.1 O que existe e passa na standard

| Teste | Prova |
|---|---|
| `TestRELPFrames`, `TestRELPZeroLengthBothSpellings`, `TestRELPMalformed` | moldura, as duas grafias de `DATALEN=0`, fechamento em moldura ruim |
| `TestRELPFrameCeiling`, `TestRELPFrameGrowsWithTheBytesNotTheCount`, `TestRELPLargeFrameRoundTrips` | teto e crescimento do buffer por blocos |
| `TestRELPAcknowledgesOnlyAfterTheRing` | o `200 OK` não sai antes do ring |
| `TestRELPRefusesWithFiveHundredOnAFullRing` | `500 ring full` e a conexão continua |
| `TestRELPAcknowledgesAFilteredMessage` | filtrada recebe `200 OK` |
| `TestRELPUnknownCommandKeepsTheConnection` | comando desconhecido não derruba a conexão |
| `TestRELPWindowAllowsConcurrentTransactions` | confirmações fora de ordem dentro da janela |
| `TestConfirmDurationIsObservedOnTCPAndRELP` | a métrica de confirmação é alimentada |
| `TestRELPConfirmationSurvivesARestart` | com `ring=sqlite`, o `200 OK` só sai com a linha no disco |
| `FuzzFrame` | as molduras da RFC 6587 e a do RELP |

Contra emissor vivo (`SYSLOG_TEST_EMITTERS=1`): rsyslog com `omrelp`, 25
confirmações `200 OK` para 25 linhas no destino (2026-09-13 e 2026-09-14).

### 8.2 Critérios de aceite (nunca medidos)

| Cenário | Alvo |
|---|---|
| RELP, `memory`+`redis` | ≥ 80k msg/s (a confirmação custa) |
| Perda com `SIGTERM` durante carga | **zero**, reconciliada |
| Perda com FATAL do sink, `memory` | **zero**, via dump e reingestão |

### 8.3 Lacunas que vieram junto

1. **RELP com corte do servidor no meio do fluxo.** Matar o servidor com
   transações em voo e provar que o rsyslog retransmite tudo o que não foi
   confirmado. É o único teste que prova a promessa central num transporte capaz
   de prová-la, e **não existe**. Deve ser o primeiro a escrever. Ele reconcilia
   três números: o que o emissor diz ter enviado, as confirmações que o servidor
   diz ter dado e as linhas no destino.
2. **Duplicata após confirmação perdida.** Se o `200 OK` de uma mensagem já no ring
   não chegar ao cliente (conexão cortada naquele instante), o rsyslog
   retransmite. Como o RELP não carrega id de mensagem, o destino recebe um
   segundo evento com outro id. O shutdown gracioso evita isso; um corte abrupto
   não. É at-least-once, inerente ao protocolo, e não tem teste.
3. **`relp.max_conns = 0` deixa sem teto o custo fixo por conexão**, de cerca de
   128 KiB (o primeiro bloco de 64 KiB mais o `bufio` de 64 KiB), fora de
   `ring.max_bytes`. O leitor RELP só usa `ReadByte` e a leitura contada, então
   um `bufio` de 4 KiB e um primeiro bloco pequeno, que dobra conforme os bytes
   chegam, bastam; é o que a implementação nova deve fazer.
4. **A recusa por IP é invisível**: nenhuma métrica conta as conexões recusadas
   por `max_conns_per_ip`.
5. **As cópias da janela (§4) não têm teste.** Falta um cliente sintético com
   `window` frames grandes, destino DOWN e ring cheio, que meça a memória e
   prove que o leitor para quando o teto em bytes se esgota.
6. **O shutdown com transações em voo não tem teste**: nenhum caso prova que
   elas recebem `200 OK` ou `500 shutdown` antes de a conexão fechar (§5.2).

---

## 9. Bugs que já custaram caro (não os repita)

- **Confirmação RELP descartada no shutdown.** O `Close` fechava o socket direto e
  perdia o `200 OK` de mensagens já no ring, que viravam duplicatas na
  retransmissão. Agora o `Close` só expira o prazo de leitura e deixa o escritor
  esvaziar as confirmações decididas.
- **Corrida no prazo de leitura.** O laço renovava o prazo de ociosidade antes de
  cada frame e podia sobrescrever o "agora" do `Close`, segurando o shutdown por
  até 120 s. A ordem das escritas em `armDeadline`, com o `closing` atômico
  conferido a cada frame, resolve.
- **Frame alocado pelo `DATALEN` declarado.** `make([]byte, n)` antes de chegar
  um byte; resolvido com crescimento em blocos de 64 KiB (§2).
- **`Enqueue` cancelado respondia `500 shutdown` para linha que seria commitada.**
  Duplicata garantida; resolvido esperando o group commit (§4).
- **Tetos de conexão por listener.** Com `relp_listeners = N`, os tetos valiam N
  vezes; um limitador por transporte resolve.

---

## 10. O que a versão LogHub precisa decidir

O novo `go-syslog-server` (2026-10-05) emite envelopes JLH `type=syslog`
(skills `loghub-json-schema` e `loghub-type-syslog-schema`). Para o RELP entrar
no mesmo ecossistema, estas perguntas ficaram abertas:

1. **Representação no payload.** O enum de `payload.transport` do `type=syslog`
   não tem `relp`. As opções levantadas na entrevista:
   - `transport="tcp"` com `framing="octet-counting"` (o `DATALEN` é contagem de
     octetos) mais uma extensão `x-*` (por exemplo `"x-transport": "relp"`). Passa
     no schema, e o `sid` calculado com `tcp` continua recalculável;
   - pedir a inclusão de `relp` no enum da skill;
   - um `type` próprio.
2. **O `uuid` e a duplicata do §8.3 item 2.** O JLH deduplica por `(uuid, step)`,
   mas uma retransmissão do rsyslog depois de uma confirmação perdida chega como
   mensagem nova, com `uuid` novo. A deduplicação ponta a ponta exigiria uma chave
   derivada do conteúdo e da conexão, o que o RELP não fornece.
3. **Reaproveitar ou não o núcleo do `go-syslog-server`** (parser, ring, sinks):
   como módulo independente que copia com adaptação, a regra do monorepo, ou como
   um transporte opcional do próprio `go-syslog-server`, o que reverteria a
   decisão de 2026-10-05.
4. **O teto em bytes das cópias da janela**: o nome da chave e o padrão. Que
   ele exista e seja global ao processo é exigência de §4.
