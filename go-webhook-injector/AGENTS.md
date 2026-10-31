# AGENTS.md — go-webhook-injector

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-webhook-injector/            <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-webhook-injector
    cmd/go-webhook-injector/
    internal/
    docs/DECISOES.md              as decisões de 2026-10-30 (comuns aos três injetores)
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/Dockerfile
  go-syslog-injector/             <- o PAI do código (nunca dependência)
  go-relplog-injector/            <- o irmão
  go-webhook-server/ ...          <- os servidores que este módulo testa
```

Cliente que **injeta requisições HTTP** num servidor de webhooks, para teste
funcional e de carga extrema: HTTP/1.1 com keep-alive e pipelining, HTTP/2 (h2c
ou ALPN), TLS. Requisições geradas, de modelo, de arquivo ou extraídas de uma
captura do tcpdump. Não é parte do pipeline LogHub.

## 2. Regras

- `docs/SPEC.md` é a fonte da verdade e está congelada: discrepância é bug no
  código. Para mudar comportamento: **spec primeiro**, com o porquê; depois o
  código; depois o teste. Cite por seção (`§4.4`).
- Decisões novas entram na SPEC (e em `docs/DECISOES.md` quando mudam uma das
  de lá); problemas abertos vão para **"Pendências conhecidas"** (SPEC §9).
- `docs/TESTING.md` registra as lacunas com honestidade.
- **Nada é importado de outro módulo do monorepo.** `report`, `pcap` e `sched`
  vieram do `go-syslog-injector`: uma correção lá ou aqui vale para os irmãos —
  confira e registre no `CHANGELOG.md` da raiz o que ficou "a portar".
- **Zero dependências** fora da stdlib (DECISOES 6).
- Git (commit, branch, tag, remoto) é do nível do monorepo, e só com pedido
  explícito do usuário. Tags: `go-webhook-injector/vX.Y.Z`.

## 3. O contrato que mais importa

Toda `sched.Conn` chama o callback de desfecho **exatamente uma vez** por
requisição dada a `Send` — ou a devolve por `Unsent` (SPEC §4.4), e então
**não** chama o callback. Nunca os dois. Uma requisição só é devolvida quando é
certo que o servidor não a processou (não chegou ao fio, ou ficou atrás de um
`Connection: close`); uma que o servidor pode ter processado vira
`no_response`, porque um `POST` não é idempotente.

## 4. Comandos

```bash
gofmt -l . && go vet ./... && go test -race ./...
go test ./internal/httpx/ -run=XXX -fuzz=^FuzzStream$ -fuzztime=60s
go test ./internal/pcap/  -run=XXX -fuzz=^FuzzReader$ -fuzztime=60s
go run ./cmd/go-webhook-injector --url http://127.0.0.1:8080/hook --count 1000 --concurrency 4
```

A prova com tcpdump real roda num container Linux (TESTING §3.5).

## 5. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- Desfechos (`transport.Timeout`, ...) e contadores da fonte são a interface do
  relatório JSON: renomear um é mudança incompatível, e vai para a SPEC §8.

## 6. Armadilhas que já custaram bugs

- **Pipelining sem empurrar o buffer antes de esperar vaga** é impasse: as
  respostas que liberariam a vaga nunca vêm (SPEC §4.1).
- **O prazo de leitura** é ligado sob o mesmo lock que a leitora usa para
  desligá-lo na ociosidade; fora dele, a leitora apagava o prazo de uma
  requisição recém-escrita.
- **`tls.Config` compartilhado entre transportes HTTP/2** é corrida: o
  `net/http` escreve nele. Cada transporte recebe um `Clone`.
- **`http.ReadRequest` com o cabeçalho cortado** acusa malformação em vez de
  "faltam bytes": o extrator espera a linha vazia antes de chamá-lo, e acha o
  fim do chunked por conta própria (SPEC §6.6).
- **`{` em JSON não é marcador**: a regra de chaves do `tmpl` difere da do SI
  (SPEC §5.2). Não "unifique" com o pai.
- **`--duration` sem `--count`** já mandou uma requisição só (o padrão de 1
  valia também ali); `TestUUIDOutAndDuration` segura.
