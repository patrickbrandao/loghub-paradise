# AGENTS.md — go-relplog-injector

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-relplog-injector/            <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-relplog-injector
    cmd/go-relplog-injector/
    internal/
    docs/DECISOES.md              as decisões de 2026-10-30 (comuns aos três injetores)
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/Dockerfile
  go-syslog-injector/             <- o PAI do código (nunca dependência)
  go-webhook-injector/            <- o irmão
  go-relplog-server/ ...          <- os servidores que este módulo testa
```

Cliente que **injeta mensagens syslog por RELP** (TCP ou TLS), falando como o
`omrelp`/`librelp`: janela, uma confirmação por mensagem, quebra de sessão e
retransmissão. Mensagens geradas, de modelo, de arquivo ou extraídas de uma
captura do tcpdump. Não é parte do pipeline LogHub.

## 2. Regras

- `docs/SPEC.md` é a fonte da verdade e está congelada: discrepância é bug no
  código. **Spec primeiro**, com o porquê; depois o código; depois o teste.
- Decisões novas entram na SPEC; problemas abertos em **"Pendências
  conhecidas"** (SPEC §9). `docs/TESTING.md` registra as lacunas.
- **Nada é importado de outro módulo.** `tmpl`, `pcap`, `extract`, `source` e
  `report` são cópias do `go-syslog-injector`: corrija lá **e** aqui, e registre
  no `CHANGELOG.md` da raiz o que ficou "a portar". `sched` difere do pai (a
  retransmissão) e `transport` é próprio (RELP).
- **Zero dependências** fora da stdlib.
- Git só no nível do monorepo, com pedido explícito. Tags:
  `go-relplog-injector/vX.Y.Z`.

## 3. O contrato que mais importa

Toda `sched.Conn`: para cada `sched.Msg` dada a `Send`, **ou** `Done` é chamado
uma vez (chegou resposta), **ou** a mensagem volta em `Unacked` depois do
`Close` — nunca as duas, nunca nenhuma. O worker decide retransmitir ou dar
`unanswered`.

O que o `librelp` faz com cada resposta é o que este cliente faz (SPEC §4.4):
`200` confirma; outro código destrói a mensagem (nunca reenviada) e, por padrão,
quebra a sessão; sessão quebrada reenvia o que ficou sem resposta.

## 4. Comandos

```bash
gofmt -l . && go vet ./... && go test -race ./...
go run ./cmd/go-relplog-injector --target 127.0.0.1:20514 --count 1000 --concurrency 4
```

## 5. Convenções

- Código e comentários em **inglês**; docs e conversa em **português**.
- Desfechos (`acked`, `rejected`, `unanswered`, `not_sent`) e contadores da
  fonte são a interface do JSON: renomear é mudança incompatível (SPEC §8).

## 6. Armadilhas que já custaram bugs

- **Descarregue antes de esperar a janela**: uma janela cheia esperaria
  respostas a frames que ainda estão no buffer.
- **Mensagem que esperava vaga não gastou tentativa**: ela volta em `Unacked`
  sem contar como retransmissão (`hold`).
- **Duplicata não é bug**: um servidor que fecha com dados não lidos gera RST e
  o cliente perde `200` já a caminho; o teste confere "ao menos uma vez".
- **Sessão que morre no `Close`** (ack timeout, `serverclose`) conta como
  quebrada.
- As armadilhas de captura do pai (cauda sem lacuna, ressincronização
  independente da divisão) valem aqui.
