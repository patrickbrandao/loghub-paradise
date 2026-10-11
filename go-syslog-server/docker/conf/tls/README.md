# Ponto de montagem, não um lugar para certificados

Os compose montam `../conf` em `/etc/syslog` (só leitura) e `../tls` em
`/etc/syslog/tls` por cima. Com a pasta de baixo montada só leitura, o Docker não
consegue criar o ponto de montagem `/etc/syslog/tls` e o contêiner nem sobe
(`mkdirat ... read-only file system`); esta pasta existe só para que ele já
exista.

**Os certificados ficam em `docker/tls/`** (veja o README de lá). O que for posto
aqui fica escondido pela montagem de `docker/tls/` e nunca é lido; o
`.gitignore` desta pasta impede que uma chave copiada para cá por engano seja
versionada.
