# Certificados do RELP/TLS

Ponha aqui `server.crt` e `server.key` (e, para mTLS, `clients-ca.crt`) e ligue o
listener com `RELPLOG_LISTEN_TLS=0.0.0.0:20515`. A pasta é montada só leitura em
`/etc/relplog/tls` no servidor (`common.yml`) e em `/etc/rls-demo/tls` no
emissor de demonstração (`compose.emitter.yml`). Nada aqui é versionado além
deste arquivo (`.gitignore` da raiz do módulo: `*.crt`, `*.key`, `*.pem`).

O `omrelp` com `tls="on"` faz o handshake logo depois do `connect` e fala a mesma
sessão RELP dentro do túnel (docs/SPEC.md §4.5). Uma linha syslog solta ou um
cliente sem TLS na porta TLS fecham a conexão.

## Gerar uma CA, o certificado do servidor e o de um cliente rsyslog

Para laboratório; em produção use a sua PKI. A partir de `docker/tls/`:

```bash
# 1. CA (assina o servidor e os clientes; no laboratório, uma só serve aos dois)
openssl req -x509 -newkey rsa:3072 -sha256 -days 3650 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=rls-lab-ca"

# 2. Servidor. O SAN precisa conter o nome que o rsyslog disca E confere:
#    com tls.authMode="name", o tls.permittedPeer do omrelp casa com o
#    dNSName (ou o CN). "relplog" e' o servico dos compose; acrescente o nome
#    real do host em producao.
openssl req -newkey rsa:3072 -sha256 -nodes \
  -keyout server.key -out server.csr -subj "/CN=relplog"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 825 -sha256 -out server.crt \
  -extfile <(printf "subjectAltName=DNS:relplog,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth")

# 3. Cliente (só para mTLS, ou para registrar quem mandou em payload.tls)
openssl req -newkey rsa:3072 -sha256 -nodes \
  -keyout client.key -out client.csr -subj "/CN=rls-demo-emitter"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 825 -sha256 -out client.crt \
  -extfile <(printf "extendedKeyUsage=clientAuth")

# 4. A CA dos clientes, que o servidor confere (RELPLOG_TLS_CLIENT_CA)
cp ca.crt clients-ca.crt
rm -f ./*.csr ca.srl
```

`ca.key` não precisa ficar aqui: guarde-a fora da pasta montada.

## Ligar no servidor

```bash
cd docker
RELPLOG_LISTEN_TLS=0.0.0.0:20515 \
RELPLOG_TLS_CLIENT_CA=/etc/relplog/tls/clients-ca.crt \
  docker compose -f compose.memory-nats.yml up -d
```

| `RELPLOG_TLS_CLIENT_AUTH` | Efeito |
|---|---|
| `optional` (padrão) | pede o certificado do cliente; com `RELPLOG_TLS_CLIENT_CA`, verifica quando ele vem; sem a CA, aceita sem verificar e registra `peer-*` |
| `require` | mTLS: exige e verifica; `RELPLOG_TLS_CLIENT_CA` é obrigatória (exit 1 sem ela) |
| `none` | não pede |

O `payload.tls` de cada envelope registra `version`, `cipher`, `peer-subject` e
`peer-fingerprint` (SHA-256 do certificado do cliente, quando ele vem).

## Ligar no rsyslog

O bloco `omrelp` com TLS está comentado em `docker/rsyslog/rsyslog.conf`:

```
action(type="omrelp" target="relplog" port="20515" tls="on"
       tls.caCert="/etc/rls-demo/tls/ca.crt"
       tls.myCert="/etc/rls-demo/tls/client.crt"
       tls.myPrivKey="/etc/rls-demo/tls/client.key"
       tls.authMode="name" tls.permittedPeer=["relplog"]
       queue.type="LinkedList" queue.filename="relp-loghub-tls"
       queue.saveOnShutdown="on" action.resumeRetryCount="-1")
```

`tls.myCert`/`tls.myPrivKey` só são obrigatórios com `require`. A `client.key`
precisa ser legível pelo usuário do rsyslog (no emissor de demonstração, root).

## Permissões

**A `server.key` precisa ser legível pelo uid 65532**, o usuário da imagem
(`USER 65532:65532` no `Dockerfile`). Uma chave `0600` do seu usuário no host
fica ilegível dentro do contêiner, e o boot sai com 1
(`tls.key_file: open /etc/relplog/tls/server.key: permission denied`). Num host
Linux, o mais estreito:

```bash
sudo chown 65532:65532 docker/tls/server.key
sudo chmod 0400 docker/tls/server.key
```

ou, para continuar editável pelo seu usuário, grupo 65532 com `0640`
(`sudo chgrp 65532 server.key && chmod 0640 server.key`). O certificado e a CA
são públicos: `0644` basta. No Docker Desktop (macOS, Windows) o compartilhamento
de arquivos já apresenta os arquivos como do usuário do contêiner.

O certificado é lido só no boot: trocar o certificado exige restart, não
`SIGHUP` (docs/SPEC.md §4.5).
