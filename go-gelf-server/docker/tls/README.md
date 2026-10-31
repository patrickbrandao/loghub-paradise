# Certificados do TLS (TCP+TLS e HTTPS)

Ponha aqui `server.crt` e `server.key` (e, para mTLS, `clients-ca.crt`) e ligue
os listeners com `GELF_LISTEN_TLS=0.0.0.0:12203` (GELF TCP sobre TLS, moldura
`\0`) e/ou `GELF_LISTEN_HTTPS=0.0.0.0:12204` (`POST /gelf` sobre TLS). Os dois
usam o mesmo certificado e o mesmo `client_auth` de `[tls]` (docs/SPEC.md §4.7).
A pasta é montada só leitura em `/etc/gelf/tls` (`common.yml`). Nada aqui é
versionado além deste arquivo.

**A chave precisa ser legível pelo uid 65532**, o usuário da imagem
(`USER 65532:65532` no `Dockerfile`). Uma chave `0600` do seu usuário no host
fica ilegível dentro do contêiner, e o boot sai com 1
(`tls.key_file: open /etc/gelf/tls/server.key: permission denied`). Num host
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
`SIGHUP` (docs/SPEC.md §4.7, §16.3).
