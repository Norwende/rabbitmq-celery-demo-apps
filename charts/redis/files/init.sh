#!/bin/sh
# Init container : génère redis.conf et sentinel.conf dans l'emptyDir /config.
#
# Choix du master (dans cet ordre) :
#   1. demander aux sentinels des AUTRES pods le master courant ;
#   2. si aucun sentinel ne répond, ou s'il désigne ce pod lui-même, regarder si un
#      pair Redis tient déjà le rôle master (évite un 2e master si les sentinels sont
#      tous tombés ou ont une vue périmée) ;
#   3. sinon (premier démarrage / redémarrage complet) : <sts>-0 est le master.
# Un pod qui redémarre rejoint donc toujours le master courant comme réplica.
set -eu

: "${CLUSTER_DOMAIN:=cluster.local}"
: "${HEADLESS_SERVICE:=redis-headless}"
: "${REPLICAS:=3}"
: "${MASTER_NAME:=orderflow}"
: "${SENTINEL_QUORUM:=2}"
export REDISCLI_AUTH="$REDIS_PASSWORD"

STS="${POD_NAME%-*}"
fqdn() { echo "${STS}-$1.${HEADLESS_SERVICE}.${POD_NAMESPACE}.svc.${CLUSTER_DOMAIN}"; }
SELF="${POD_NAME}.${HEADLESS_SERVICE}.${POD_NAMESPACE}.svc.${CLUSTER_DOMAIN}"
log() { echo "[init $(date +%H:%M:%S)] $*" >&2; }

wait_dns() {
  i=0
  until getent hosts "$1" >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -ge 90 ] && { log "DNS: $1 ne se résout pas"; return 1; }
    sleep 1
  done
}

peers() {
  i=0
  while [ "$i" -lt "$REPLICAS" ]; do
    h=$(fqdn "$i")
    [ "$h" != "$SELF" ] && echo "$h"
    i=$((i + 1))
  done
}

# Le service headless publie les adresses des pods non-ready : notre propre nom doit
# se résoudre avant que les pairs (et notre sentinel) puissent nous joindre.
wait_dns "$SELF"

ask_sentinels() {
  for h in $(peers); do
    out=$(timeout 3 redis-cli --no-auth-warning -h "$h" -p 26379 \
            SENTINEL get-master-addr-by-name "$MASTER_NAME" 2>/dev/null | head -n1 || true)
    # Réponse valide = un nom d'hôte ; vide (nil) ou message d'erreur (contient un espace) = ignorée.
    case "$out" in
      ""|*" "*) continue ;;
    esac
    log "le sentinel de $h désigne le master $out"
    echo "$out"
    return 0
  done
}

MASTER=$(ask_sentinels | tail -n1)

# Les sentinels nous désignent encore comme master : ce pod était le master et a été
# tué brutalement (sans preStop). Plutôt que de redémarrer aussitôt en master (et de
# servir quelques secondes avant d'être rétrogradé), on laisse aux sentinels le temps
# de constater la panne (down-after 5 s) et d'élire un réplica.
if [ "$MASTER" = "$SELF" ]; then
  log "les sentinels nous désignent comme master : attente d'un éventuel failover (20 s max)"
  i=0
  while [ "$i" -lt 20 ] && [ "$MASTER" = "$SELF" ]; do
    sleep 1
    i=$((i + 1))
    MASTER=$(ask_sentinels 2>/dev/null | tail -n1)
    [ -z "$MASTER" ] && MASTER="$SELF"
  done
  log "après attente, master retenu : $MASTER"
fi

if [ -z "$MASTER" ] || [ "$MASTER" = "$SELF" ]; then
  for h in $(peers); do
    role=$(timeout 3 redis-cli --no-auth-warning -h "$h" -p 6379 ROLE 2>/dev/null | head -n1 || true)
    if [ "$role" = "master" ]; then
      log "aucun sentinel fiable, mais $h est master : on le suit"
      MASTER="$h"
      break
    fi
  done
fi

if [ -z "$MASTER" ]; then
  MASTER=$(fqdn 0)
  log "aucun sentinel ni master joignable : bootstrap, master initial = $MASTER"
fi

wait_dns "$MASTER"

# --- redis.conf (réécrit par Redis/Sentinel via CONFIG REWRITE : doit être inscriptible)
cat > /config/redis.conf <<EOF
port 6379
bind * -::*
protected-mode no
dir /data
appendonly yes
appendfsync everysec
requirepass ${REDIS_PASSWORD}
masterauth ${REDIS_PASSWORD}
# Annonce par nom DNS stable plutôt que par IP de pod (qui change à chaque redémarrage).
replica-announce-ip ${SELF}
replica-announce-port 6379
maxmemory ${REDIS_MAXMEMORY:-128mb}
maxmemory-policy noeviction
# min-replicas-to-write volontairement NON activé : mesuré, il provoque ~4 s d'erreurs
# NOREPLICAS (ResponseError, non retentée par les clients) après chaque failover, le temps
# que les réplicas se rattachent. Le risque couvert (master isolé par une partition réseau
# qui accepte des écritures ensuite perdues) est négligeable sur un cluster mono-nœud ;
# les bascules planifiées sont protégées par le CLIENT PAUSE du preStop.
EOF

if [ "$MASTER" = "$SELF" ]; then
  log "démarrage en MASTER"
else
  echo "replicaof ${MASTER} 6379" >> /config/redis.conf
  log "démarrage en REPLICA de $MASTER"
fi

# --- sentinel.conf (réécrit en permanence par Sentinel)
# myid stable dérivé du nom du pod : un pod recréé garde son identité de sentinel
# (pas d'entrées fantômes chez les autres sentinels).
MYID=$(printf '%s' "$POD_NAME" | sha1sum | cut -c1-40)
cat > /config/sentinel.conf <<EOF
port 26379
bind * -::*
protected-mode no
dir /config
sentinel myid ${MYID}
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip ${SELF}
sentinel announce-port 26379
requirepass ${REDIS_PASSWORD}
sentinel sentinel-pass ${REDIS_PASSWORD}
sentinel monitor ${MASTER_NAME} ${MASTER} 6379 ${SENTINEL_QUORUM}
sentinel auth-pass ${MASTER_NAME} ${REDIS_PASSWORD}
sentinel down-after-milliseconds ${MASTER_NAME} 5000
sentinel failover-timeout ${MASTER_NAME} 20000
sentinel parallel-syncs ${MASTER_NAME} 1
EOF

chmod 600 /config/redis.conf /config/sentinel.conf
