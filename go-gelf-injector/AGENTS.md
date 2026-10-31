# AGENTS.md — go-gelf-injector

Instruções para agentes de IA que trabalham **dentro desta pasta**. Leia antes
de editar qualquer arquivo.

## 1. Onde você está

```
loghub-paradise/                  <- monorepo, SEM go.mod
  go-gelf-injector/               <- VOCÊ ESTÁ AQUI (módulo Go independente)
    go.mod                        module github.com/patrickbrandao/loghub-paradise/go-gelf-injector
    cmd/go-gelf-injector/
    internal/
    docs/DECISOES.md              decisões herdadas dos injetores (1-19) e as do GELF (G1-G12)
    docs/SPEC.md                  fonte da verdade do comportamento
    docs/TESTING.md               como testar e o que ainda não é provado
    docker/Dockerfile
  go-syslog-injector/ (SI), go-webhook-injector/ (WI)   <- de onde o código veio
  go-gelf-server/, go-gelf-pg-server/, go-gelf-nats-server/   <- os servidores que este módulo testa
```

Cliente que **injeta mensagens GELF** (UDP com chunks e gzip/zlib, TCP e TLS
com `\0`, HTTP/HTTPS `POST /gelf`) num servidor — geradas, de modelo, de
arquivo ou extraídas de uma captura do tcpdump — para teste funcional e de
carga. Não é parte do pipeline LogHub: não gera envelope JLH, não lê config.

## 2. Regras

- `docs/SPEC.md` é a fonte da verdade: discrepância é bug no código. Para
  mudar comportamento: **spec primeiro**, com o porquê; depois o código;
  depois o teste. Cite por seção (`§6.4`).
- Decisões novas entram na SPEC (e em `docs/DECISOES.md` quando mudam uma das
  de lá); problemas abertos vão para **"Pendências conhecidas"** (SPEC §9).
- `docs/TESTING.md` registra as lacunas com honestidade.
- **Nada é importado de outro módulo do monorepo.** `pcap`, `report` e
  `transport/proxy.go` vieram do SI; `sched`, `tmpl`, `httpx` e o cliente de
  `transport/http.go` vieram do WI. Uma correção num deles vale para os
  irmãos: registre no `CHANGELOG.md` da raiz o que ficou "a portar".
- **Zero dependências** fora da stdlib (decisão 6).
- Git (commit, branch, tag, remoto) é do nível do monorepo, e só com pedido
  explícito do usuário. Tags: `go-gelf-injector/vX.Y.Z`.

## 3. O contrato que mais importa

Toda `sched.Conn` chama o callback de desfecho **exatamente uma vez** por
mensagem dada a `Send` — na hora, no `Flush`, na resposta ou no `Close` — ou a
devolve (`Unsent`, só HTTP) sem desfecho. É o que faz `mensagens = ok +
falhas`. No UDP chunked, um chunk recusado encerra a mensagem com **um**
desfecho e o resto não sai.

## 4. Comandos

```bash
gofmt -l . && go vet ./... && go test -race ./...
go test ./internal/extract/ -run=XXX -fuzz=^FuzzStream$     -fuzztime=60s
go test ./internal/gelf/    -run=XXX -fuzz=^FuzzReassemble$ -fuzztime=60s
GELF_TEST_LOAD=1 go test ./internal/app -run TestLoad -v    # 100k por transporte
go run ./cmd/go-gelf-injector --transport udp --count 1000 --payload-size 20000
```

Os demais alvos de fuzz e a prova com tcpdump real (container Linux: no macOS
a captura exige root) estão em `docs/TESTING.md`.

## 5. Convenções

- Código, identificadores e comentários em **inglês**; docs e conversa em
  **português**.
- Comentários explicam o **porquê** e o que daria errado com a alternativa.
- Desfechos (`transport.Sent`, ...) e contadores da fonte são a interface do
  relatório JSON: renomear um é mudança incompatível (SPEC §8).
- `internal/gelftest` é independente de `gelf` e `extract` **de propósito**:
  não o faça usar o código do emissor.

## 6. Armadilhas

- **A cauda da captura some sem lacuna** quando o tcpdump para cedo
  (`tcp_unterminated`, herdado do SI); pare o tcpdump com folga.
- **Ressincronizar não pode depender da divisão em segmentos**: a decisão
  midstream (GELF × HTTP) é tomada num byte, sem olhar adiante, e o que se
  guarda é cortado por posição (SPEC §6.4). O `FuzzStream` confere; não o
  enfraqueça.
- **Padding constante não exercita chunks**: o gzip o reduz a nada. O `_pad`
  é aleatório (decisão G7).
- **UDP "sent" não é "recebido"**: um receptor lento perde no buffer do kernel
  e o injetor não sabe. Para provar entrega use `--rate` e conte no destino.
- **Teto de descompressão dentro do leitor**, nunca depois (bomba de gzip).
