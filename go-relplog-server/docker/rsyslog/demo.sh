#!/bin/sh
# Gerador de DEMONSTRACAO: sobe o rsyslogd e manda linhas de teste pelo
# `logger` (syslog(3) -> /dev/log -> imuxsock -> fila -> omrelp).
#
# Variaveis (compose.emitter.yml):
#   DEMO_BURST     linhas mandadas de uma vez na subida (padrao 100)
#   DEMO_INTERVAL  segundos entre as linhas seguintes (padrao 1; 0 = so' a rajada)
#   DEMO_TAG       a TAG das linhas (padrao rls-demo)
#
# Cada linha leva um numero de sequencia (seq=N) e o hostname do container:
# do lado do servidor, "quantas chegaram" e' contar as linhas com DEMO_TAG, e
# um buraco na sequencia e' perda. Uma retransmissao do rsyslog (sessao fechada
# sem `200 OK`) aparece como seq repetido: o RELP nao tem id de mensagem
# (docs/SPEC.md §19, duplicatas).
set -eu

BURST="${DEMO_BURST:-100}"
INTERVAL="${DEMO_INTERVAL:-1}"
TAG="${DEMO_TAG:-rls-demo}"

: "${RELP_TARGET:?defina RELP_TARGET (o host do go-relplog-server)}"
: "${RELP_PORT:?defina RELP_PORT}"

rm -f /var/run/rsyslogd.pid
# -n: primeiro plano; o rsyslogd e' o processo que importa, e o sinal de
# parada (SIGTERM) precisa chegar a ele para gravar a fila em disco
# (queue.saveOnShutdown).
rsyslogd -n -f /etc/rsyslog.conf &
RSYSLOGD=$!
trap 'kill -TERM "$RSYSLOGD" 2>/dev/null; wait "$RSYSLOGD"; exit 0' TERM INT

# Espera o imuxsock criar /dev/log; sem ele o logger falharia em silencio.
i=0
while [ ! -S /dev/log ]; do
  i=$((i + 1))
  [ "$i" -gt 100 ] && { echo "demo: /dev/log nao apareceu" >&2; exit 1; }
  sleep 0.1
done

echo "demo: rsyslogd no ar; omrelp -> ${RELP_TARGET}:${RELP_PORT}; rajada de ${BURST} linhas"
seq=0
while [ "$seq" -lt "$BURST" ]; do
  seq=$((seq + 1))
  logger -t "$TAG" -p local0.info "seq=$seq rajada demonstracao go-relplog-server"
done
echo "demo: rajada de ${BURST} entregue ao rsyslogd (seq 1..${seq})"

if [ "$INTERVAL" = "0" ]; then
  wait "$RSYSLOGD"
  exit 0
fi

while :; do
  sleep "$INTERVAL" &
  wait $! || true
  seq=$((seq + 1))
  logger -t "$TAG" -p local0.notice "seq=$seq tique demonstracao go-relplog-server"
  [ $((seq % 60)) -eq 0 ] && echo "demo: ${seq} linhas entregues ao rsyslogd"
done
