#!/bin/sh
# Readiness du container redis.
#   master  : répond PONG.
#   réplica : PONG + lien master up + synchro terminée + le sentinel local le voit
#             comme réplica sain (flags = "slave", sans s_down/disconnected).
# Ce dernier point est essentiel pour les rolling updates : tant que les sentinels
# n'ont pas re-découvert le réplica redémarré, un failover l'ignorerait et il
# resterait attaché à l'ancien master. Le rollout n'avance donc qu'une fois
# le pod réellement éligible.
: "${CLUSTER_DOMAIN:=cluster.local}"
: "${HEADLESS_SERVICE:=redis-headless}"
: "${MASTER_NAME:=orderflow}"
SELF="${POD_NAME}.${HEADLESS_SERVICE}.${POD_NAMESPACE}.svc.${CLUSTER_DOMAIN}"
cli() { timeout 2 redis-cli --no-auth-warning "$@" 2>/dev/null; }

[ "$(cli -h 127.0.0.1 -p 6379 PING)" = "PONG" ] || exit 1

info=$(cli -h 127.0.0.1 -p 6379 INFO replication | tr -d '\r')
case "$info" in
  *role:master*) exit 0 ;;
esac
echo "$info" | grep -q '^master_link_status:up' || { echo "lien master down"; exit 1; }
echo "$info" | grep -q '^master_sync_in_progress:0' || { echo "synchro en cours"; exit 1; }

flags=$(cli -h 127.0.0.1 -p 26379 SENTINEL REPLICAS "$MASTER_NAME" | awk -v me="${SELF}:6379" '
  NR % 2 == 1 { key = $0; next }
  key == "name"  { cur = $0 }
  key == "flags" && cur == me { print $0 }')
[ "$flags" = "slave" ] || { echo "sentinel local : flags='${flags:-inconnu}'"; exit 1; }
exit 0
