# Emissor de DEMONSTRACAO do go-relplog-server: um rsyslog de verdade (o
# cliente RELP de referencia, librelp/omrelp) mandando linhas de teste.
#
# NAO e' parte do deploy: sobe so' com o profile `demo` (compose.emitter.yml).
# A config (rsyslog.conf) e o gerador (demo.sh) sao montados pelo compose, nao
# copiados: edite-os no host e recrie o container, sem rebuild.
#
#   docker build -t rls-demo-emitter:local docker/rsyslog

ARG ALPINE_VERSION=3.22
FROM alpine:${ALPINE_VERSION}

# rsyslog-relp traz o omrelp (e o imrelp) e puxa o librelp. busybox ja' tem o
# `logger`, que escreve em /dev/log, onde o imuxsock escuta.
RUN apk add --no-cache rsyslog rsyslog-relp ca-certificates \
 && mkdir -p /var/spool/rsyslog

# A fila da acao omrelp transborda para disco aqui (queue.filename): monte um
# volume para que ela sobreviva a recriacao do container.
VOLUME /var/spool/rsyslog

ENTRYPOINT ["/bin/sh", "/etc/rls-demo/demo.sh"]
