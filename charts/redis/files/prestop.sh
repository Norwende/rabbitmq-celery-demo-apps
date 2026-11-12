#!/bin/sh
# preStop du container redis : si ce pod est le master, déclenche un SENTINEL FAILOVER
# via le sentinel d'un AUTRE pod (le sidecar local reçoit SIGTERM en même temps que
# ce hook démarre) puis attend que les sentinels désignent un autre master.
# Rollout / suppression de pod => bascule propre au lieu de 5 s d'indisponibilité + élection.
# Toujours exit 0 : un preStop en échec ne doit pas bloquer l'arrêt du pod.

: "${CLUSTER_DOMAIN:=cluster.local}"
: "${HEADLESS_SERVICE:=redis-headless}"
: "${REPLICAS:=3}"
: "${MASTER_NAME:=orderflow}"
export REDISCLI_AUTH="$REDIS_PASSWORD"

STS="${POD_NAME%-*}"
SELF="${POD_NAME}.${HEADLESS_SERVICE}.${POD_NAMESPACE}.svc.${CLUSTER_DOMAIN}"
log() { echo "[preStop $(date +%H:%M:%S)] $*" > /proc/1/fd/1; }
cli() { timeout 3 redis-cli --no-auth-warning "$@" 2>/dev/null; }

role=$(cli -h 127.0.0.1 -p 6379 ROLE | head -n1)
[ "$role" = "master" ] || exit 0

replicas=$(cli -h 127.0.0.1 -p 6379 INFO replication | tr -d '\r' | sed -n 's/^connected_slaves://p')
if [ "${replicas:-0}" -lt 1 ]; then
  log "master sans réplica connecté : pas de failover possible"
  exit 0
fi

peer_sentinels() {
  i=0
  while [ "$i" -lt "$REPLICAS" ]; do
    h="${STS}-${i}.${HEADLESS_SERVICE}.${POD_NAMESPACE}.svc.${CLUSTER_DOMAIN}"
    [ "$h" != "$SELF" ] && echo "$h"
    i=$((i + 1))
  done
}

# Nombre de réplicas que le sentinel $1 juge sains (seuls ceux-là seront reconfigurés).
healthy_replicas() {
  cli -h "$1" -p 26379 SENTINEL REPLICAS "$MASTER_NAME" | awk '
    NR % 2 == 1 { key = $0; next }
    key == "flags" && $0 == "slave" { n++ }
    END { print n + 0 }'
}

# Attend (max 15 s) que tous les réplicas connectés soient vus sains par un sentinel pair,
# sinon un réplica fraîchement redémarré resterait attaché à l'ancien master.
first_peer=$(peer_sentinels | head -n1)
i=0
while [ "$i" -lt 15 ]; do
  [ "$(healthy_replicas "$first_peer")" -ge "$replicas" ] && break
  sleep 1
  i=$((i + 1))
done

# Gèle les écritures sur ce master pendant la bascule : les réplicas rattrapent tout le
# flux (pas d'écriture perdue) et aucun client ne peut écrire sur l'ancien master pendant
# l'instant où il coexiste avec le nouveau. Les clients bloqués sont déconnectés à l'arrêt
# du pod et se reconnectent au nouveau master via Sentinel.
cli -h 127.0.0.1 -p 6379 CLIENT PAUSE 60000 WRITE >/dev/null

triggered=""
for h in $(peer_sentinels); do
  res=$(cli -h "$h" -p 26379 SENTINEL FAILOVER "$MASTER_NAME")
  log "SENTINEL FAILOVER via $h : ${res:-pas de réponse}"
  if [ "$res" = "OK" ]; then triggered="$h"; break; fi
done
if [ -z "$triggered" ]; then
  cli -h 127.0.0.1 -p 6379 CLIENT UNPAUSE >/dev/null
  exit 0
fi

# Attend (max ~40 s, < terminationGracePeriodSeconds) qu'un autre master soit désigné.
i=0
while [ "$i" -lt 40 ]; do
  m=$(cli -h "$triggered" -p 26379 SENTINEL get-master-addr-by-name "$MASTER_NAME" | head -n1)
  if [ -n "$m" ] && [ "$m" != "$SELF" ]; then
    log "nouveau master : $m"
    exit 0
  fi
  sleep 1
  i=$((i + 1))
done
log "timeout en attendant le nouveau master"
exit 0
