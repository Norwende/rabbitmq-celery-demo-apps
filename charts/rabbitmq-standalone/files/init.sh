#!/bin/bash
# Init container of rabbitmq-server-N (chart rabbitmq-standalone).
#
# 1. Erlang cookie: copied from the Secret into $HOME (emptyDir), mode 600, so that the
#    server and the CLI tools (kubectl exec … rabbitmqctl) share it. A Secret volume cannot
#    give the file to the rabbitmq user with owner-only permissions, which Erlang requires.
# 2. Formation gate. classic_config cannot elect a seed: a node that finds no running peer
#    forms its own cluster. If all nodes start empty at the same time, two of them may each
#    form one (cluster split, observed upstream: rabbitmq-server#13043). So a node WITHOUT
#    data and with an ordinal > 0 waits until a peer already serves AMQP (that peer has
#    finished booting, hence belongs to a cluster) before starting: on a fresh install,
#    rabbitmq-server-0 forms the cluster, the others join it. A node WITH data (restart,
#    rolling update, full stop/start) never waits: it rejoins its known peers by itself.
set -euo pipefail

cp /tmp/erlang-cookie/.erlang.cookie "$HOME/.erlang.cookie"
chmod 600 "$HOME/.erlang.cookie"

ordinal="${MY_POD_NAME##*-}"
data_dir="/var/lib/rabbitmq/mnesia/${RABBITMQ_NODENAME}"

if [ -d "$data_dir" ]; then
    echo "existing data ($data_dir): rejoining the known cluster members, no wait"
    exit 0
fi
if [ "$ordinal" = "0" ]; then
    echo "fresh node with ordinal 0: starts at once (forms the cluster on a fresh install, or joins running peers)"
    exit 0
fi

deadline=$((SECONDS + FORMATION_TIMEOUT_SECONDS))
echo "fresh node with ordinal $ordinal: waiting for a running peer (AMQP port) before joining"
while [ "$SECONDS" -lt "$deadline" ]; do
    for ((i = 0; i < REPLICAS; i++)); do
        [ "$i" = "$ordinal" ] && continue
        peer="${STATEFULSET_NAME}-${i}.${K8S_SERVICE_NAME}.${MY_POD_NAMESPACE}"
        if timeout 2 bash -c "exec 3<>/dev/tcp/${peer}/5672" 2>/dev/null; then
            echo "peer ${peer} is running: starting, peer discovery will join its cluster"
            exit 0
        fi
    done
    sleep 2
done
echo "no running peer after ${FORMATION_TIMEOUT_SECONDS}s: failing (Kubernetes retries the init container)" >&2
exit 1
