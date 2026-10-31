# Injetores — Decisões de projeto

Registro das decisões que criaram os injetores do monorepo em 2026-10-30:
`go-syslog-injector` (SI, o pai do código), `go-relplog-injector` (RI) e
`go-webhook-injector` (WI). O texto é o mesmo nos três módulos; o que vale só
para um está marcado. Cada decisão aparece na `docs/SPEC.md` do módulo com a
seção.

O pedido do usuário (2026-10-30): analisar se os softwares do projeto tinham
ferramentas cliente capazes de injetar mensagens nos servidores. A análise
mostrou que não havia nenhuma além do emissor rsyslog de demonstração da família
relplog, e o usuário pediu: "criar injetor".

## 1. Decisões do usuário (entrevista de 2026-10-30)

| # | Tema | Decisão | Alternativas apresentadas |
|---|---|---|---|
| 1 | Empacotamento | **Um módulo por família**: `go-webhook-injector`, `go-syslog-injector`, `go-relplog-injector`. Espelha os servidores; o código comum é copiado (SI é o pai), e uma correção num vale para os outros | Um módulo com três subcomandos (recomendado); um `cmd/` dentro de cada um dos 11 servidores |
| 2 | Alcance da v1 | **Funcional + carga**: N mensagens ou por duração, taxa alvo, concorrência; relatório com desfechos, vazão e latência p50/p90/p99 | Só funcional |
| 3 | Conteúdo | **Todos**: gerador com `seq=`, modelo com marcadores, arquivo ou stdin, e — resposta livre do usuário — "replay de arquivos que armazenam captura de dados realizadas em servidores reais, principalmente arquivos PCAP obtidos pelo tcpdump" | Cada fonte isolada |
| 4 | Conferência no destino | **Não na v1**: o relatório vem das respostas do servidor; a conferência de `seq` no destino fica em "Pendências conhecidas" | Subcomando `check` lendo NATS e PostgreSQL |

A resposta livre da decisão 3 foi lida assim: o replay de pcap/pcapng é uma
fonte de conteúdo como as outras, e é a principal; nos três injetores ele
extrai do tráfego capturado as mensagens do protocolo daquela família e as
reenvia ao servidor escolhido.

## 2. Padrões assumidos (contestáveis)

Pela regra do usuário de 2026-10-10, o que se aprende do código e dos projetos
irmãos é decidido aqui, sem entrevista. Estão escritos para poderem ser
trocados sabendo o custo.

| # | Tema | Decisão | Por quê |
|---|---|---|---|
| 5 | Interface | Só flags (pacote `flag`), sem INI nem env; relatório no stdout, progresso no stderr; saída 0/1/2/3/130 | É ferramenta de linha de comando, usada em scripts e CI: o código de saída e o JSON são a interface |
| 6 | Dependências | **Nenhuma** fora da stdlib, inclusive o leitor de pcap | O subconjunto necessário (dois formatos, oito enlaces, IP, UDP, TCP) é pequeno e fica sob teste e fuzz; o `gopacket` traria uma API grande, mantida por um fork, para usar 10% dela. Um binário estático, `CGO_ENABLED=0` |
| 7 | Padrões de alvo | SI: `tcp` em `127.0.0.1:1514`; RI: `127.0.0.1:20514`; WI: `http://127.0.0.1:8080/` | Os padrões dos servidores (portas altas, uid 65532) |
| 8 | SI: transporte padrão | `tcp` com `octet` | TCP mostra erro de conexão (o UDP falha em silêncio) e octet não divide mensagens com LF |
| 9 | Gerador | Corpo `seq={seq} run={run} injector=<nome>`; `--run` aleatório | O método do `demo.sh` do relplog: buraco é perda, repetido é retransmissão |
| 10 | Replay | Mensagens em ordem de captura; o protocolo original é desembrulhado e o transporte escolhido reembrulha; seleção estrita por conteúdo sem `--pcap-port`, por porta com ele | Uma captura de servidor real mistura protocolos; a checagem estrita evita mandar DNS como syslog, a porta explícita aceita o que o servidor aceitaria |
| 11 | Captura iniciada no meio | Remontagem que aceita direção sem SYN e **ressincroniza** na primeira posição comprovada | Emissores syslog/RELP mantêm conexões por dias: uma captura de produção quase sempre começa no meio delas |
| 12 | Ritmo do replay | `asap` (padrão, sob `--rate`) ou `original` com fator `--pcap-speed` | Testar vazão e reproduzir a forma do tráfego são usos diferentes |
| 13 | PROXY protocol (SI, RI) | v1/v2 opcional, com origem anunciável | Os servidores aceitam PROXY de par confiável; é a única forma de testar isso e de atribuir outra origem sem socket cru |
| 14 | Concorrência | N workers, cada um com **uma** conexão própria; a taxa é global | O modelo de qualquer emissor real (conexões persistentes) e o que os tetos por conexão do servidor enxergam |
| 15 | Contrato de desfecho | Um desfecho por mensagem, sempre (`sched.Conn`) | Sem ele, `mensagens = ok + falhas` não fecha, e o relatório mente por omissão |

## 3. O que a primeira rodada ensinou

- **A cauda que some sem lacuna.** Na primeira prova com o tcpdump real (SI,
  container Linux), 783 das 6000 mensagens TCP não estavam na captura e o
  remontador não acusou lacuna: o tcpdump foi interrompido 1 s depois do tráfego
  e perdeu os últimos segmentos de três fluxos, inclusive o FIN. Correção: o
  contador `tcp_unterminated` (direção sem FIN/RST no fim da captura) e a
  recomendação de parar o tcpdump alguns segundos depois, com `-B` grande. Com
  isso, 9000 de 9000.
- **A ressincronização dependia da divisão em segmentos.** O fuzz
  (`FuzzStream`, com a invariância "a mesma entrada fatiada de outro jeito dá as
  mesmas mensagens") achou três casos em que um candidato ainda incompleto
  perdia para um candidato posterior já comprovado de outra moldura. Correção:
  um candidato pendente anterior **segura** a decisão (SPEC SI §6.6).
