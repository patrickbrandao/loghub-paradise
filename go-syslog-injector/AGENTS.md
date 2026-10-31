# AGENTS.md — go-syslog-injector

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-syslog-injector/             <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-syslog-injector
    cmd/go-syslog-injector/
    internal/
    docs/DECISOES.md              as decisões de 2026-10-30 (comuns aos três injetores)
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/Dockerfile
  go-relplog-injector/, go-webhook-injector/   <- os irmãos: FILHOS deste código
  go-syslog-server/ ...           <- os servidores que este módulo testa
```

Cliente que **injeta mensagens syslog** (UDP, TCP, TLS, unix) num servidor —
geradas, de modelo, de arquivo ou extraídas de uma captura do tcpdump — para
teste funcional e de carga. Não é parte do pipeline LogHub: não gera envelope
JLH, não tem identidade, não lê config.

## 2. Regras

- `docs/SPEC.md` é a fonte da verdade e está congelada: discrepância é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite por seção (`§6.6`).
- Decisões novas entram na SPEC (e em `docs/DECISOES.md` quando mudam uma das
  de lá); problemas abertos vão para **"Pendências conhecidas"** (SPEC §9),
  nunca para arquivo à parte. Relatórios de auditoria em `tasks/` são
  transitórios.
- `docs/TESTING.md` registra as lacunas com honestidade.
- **Nada é importado de outro módulo do monorepo.** Este é o pai do
  `go-relplog-injector` e do `go-webhook-injector`: uma correção aqui em
  `tmpl`, `pcap`, `extract`, `sched` ou `report` vale para eles — confira os
  irmãos e registre no `CHANGELOG.md` da raiz o que ficou "a portar".
- **Zero dependências** fora da stdlib (DECISOES 6). Dependência nova exige
  justificativa na spec.
- Git (commit, branch, tag, remoto) é do nível do monorepo, e só com pedido
  explícito do usuário. Tags: `go-syslog-injector/vX.Y.Z`.

## 3. O contrato que mais importa

Toda `sched.Conn` chama o callback de desfecho **exatamente uma vez** por
mensagem dada a `Send` — na hora, no `Flush`, na resposta ou no `Close`. É o que
faz `mensagens = ok + falhas`. Uma conexão nova que esquece um caminho (erro de
escrita, fechamento com mensagens no buffer) quebra o relatório em silêncio.

## 4. Comandos

```bash
gofmt -l . && go vet ./... && go test -race ./...
go test ./internal/pcap/    -run=XXX -fuzz=^FuzzReader$ -fuzztime=60s
go test ./internal/extract/ -run=XXX -fuzz=^FuzzStream$ -fuzztime=60s
go run ./cmd/go-syslog-injector --target 127.0.0.1:1514 --count 1000 --concurrency 4
```

A prova com tcpdump real roda num container Linux (TESTING §3.2): no macOS a
captura exige root.

## 5. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- Desfechos (`transport.Sent`, ...) e contadores da fonte são a interface do
  relatório JSON: renomear um é mudança incompatível, e vai para a SPEC §8.

## 6. Armadilhas que já custaram bugs

- **A cauda da captura some sem lacuna** quando o tcpdump para cedo:
  `tcp_unterminated` existe por isso (DECISOES §3).
- **Ressincronizar não pode depender da divisão em segmentos**: um candidato
  pendente anterior segura a decisão (SPEC §6.6). O `FuzzStream` confere a
  invariância; não o enfraqueça.
- **`2026 is a year` é octet-counting** pela regra dos servidores (dígitos +
  espaço): o extrator segue a regra deles, não a intuição.
