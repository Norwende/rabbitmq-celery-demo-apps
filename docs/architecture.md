# Architecture — OrderFlow

Ce document décrit les composants, puis les décisions de conception (format ADR court :
contexte → décision → conséquences), puis les limites connues. Le contrat d'interface est dans
[contracts.md](contracts.md), le déroulé d'une commande dans [cycle-de-vie.md](cycle-de-vie.md),
les procédures dans [exploitation.md](exploitation.md).

## 1. Composants

| Composant | Ressource k8s | Réplicas | Responsabilité |
|---|---|---|---|
| `gateway` | `Deployment/gateway` + `Service/gateway:8000` | 2 | API HTTP (FastAPI). Valide la commande, construit la chain Celery **par noms**, la publie, enregistre la clé de suivi `order:<id>` dans Redis, restitue l'état. Producteur pur : aucune tâche enregistrée. |
| `worker-payments` | `Deployment/worker-payments` | 1 | Consomme `payments`, exécute `payments.authorize` (règle simulée : refus au-delà de 1000). |
| `worker-inventory` | `Deployment/worker-inventory` | 1 | Consomme `inventory`, exécute `inventory.reserve` (refus si SKU `OUT-OF-STOCK`). |
| `worker-notifier` | `Deployment/worker-notifier` | 1 | Consomme `notifications`, exécute `notifications.order_confirmed` / `order_failed` / `sales_digest` et **publie l'événement dans le stream** avec kombu. Seul éditeur du stream. Lit `analytics:orders` (lecture seule) pour le digest. |
| `scheduler` | `Deployment/scheduler` (stratégie `Recreate`) | **exactement 1** | `celery beat` seul : envoie `notifications.sales_digest` **par nom** selon `DIGEST_CRON` (chaque minute par défaut). Aucune tâche enregistrée, pas de worker, pas de result backend, pas d'accès Redis (ADR-15). |
| `subscriber-analytics` | `Deployment` (stratégie `Recreate`) | 1 | Lit le stream depuis `first`, agrège compteurs et chiffre d'affaires dans Redis, checkpoint. |
| `subscriber-audit` | idem | 1 | Lit depuis `first`, écrit chaque événement en JSON brut sur stdout, checkpoint. |
| `subscriber-live` | idem | 1 | Lit depuis `next` (temps réel seulement), pas de checkpoint. |
| RabbitMQ | `RabbitmqCluster/rabbitmq` → `StatefulSet/rabbitmq-server` (variante par défaut) ; ou directement `StatefulSet/rabbitmq-server` (variante sans opérateur, ADR-19) | 3 | Broker des tâches (quorum queues) et du stream. Plugins management, prometheus et peer discovery k8s (activés par l'opérateur ; sans opérateur : `classic_config`, intégré), stream et stream management (ajoutés). |
| Redis | `StatefulSet/redis` (conteneurs `redis` + `sentinel`) | 3 | Result backend Celery, clés de suivi des commandes, agrégats, checkpoints. 1 master, 2 réplicas, 3 sentinels (quorum 2). |
| RabbitMQ Cluster Operator | `Deployment/rabbitmq-cluster-operator` (ns `rabbitmq-system`) | 1 | Gère le cycle de vie du cluster RabbitMQ. Requiert cert-manager. Absent de la variante sans opérateur (ADR-19). |

Namespaces : les huit Deployments applicatifs dans `orderflow` (release `orderflow`), RabbitMQ
et Redis dans `orderflow-infra` (releases `rabbitmq` et `redis`), l'opérateur dans
`rabbitmq-system` (ADR-16). Les applications joignent l'infrastructure par des noms DNS
complets et s'authentifient avec des Secrets de leur propre namespace (ADR-17).

Les trois workers sont construits sur le même gabarit (`celery_app.py`, `config.py`,
`probes.py`, `tasks.py`), concurrence `WORKER_CONCURRENCY=2` (pool prefork par défaut de Celery).
Les subscribers sont une seule image (`orderflow/subscriber`) déployée trois fois avec des
variables différentes (`SUBSCRIBER_NAME`, `STREAM_OFFSET`, `CHECKPOINT_ENABLED`).

Objets RabbitMQ observés sur le cluster en fonctionnement :

| Objet | Type | Déclaré par |
|---|---|---|
| `payments`, `inventory`, `notifications` | files quorum (3 membres) + exchanges `topic` homonymes | tout service Celery qui les consomme ou y publie |
| `celery_delayed_delivery`, `celery_delayed_0..27` | exchanges `topic` + files quorum | Celery (native delayed delivery) |
| `orders` | exchange `topic` | worker-notifier et subscribers |
| `orders.events` | stream (3 membres), `x-max-age: 7D` | worker-notifier et subscribers |

## 2. Décisions de conception

### ADR-01 — RabbitMQ via le Cluster Operator officiel plutôt qu'un chart Helm RabbitMQ

- **Contexte.** Un cluster RabbitMQ sur Kubernetes demande un StatefulSet, la découverte des
  pairs, un Secret d'identifiants, et surtout des arrêts de pods qui ne cassent pas les quorum
  queues (attendre qu'un nœud ne soit plus critique avant de l'arrêter).
- **Décision.** `RabbitmqCluster` du Cluster Operator v2.23.2, rendu par le chart
  `rabbitmq` (`charts/rabbitmq/templates/rabbitmqcluster.yaml`) ; l'opérateur
  lui-même reste installé par son manifeste de release épinglé
  (`k8s/operators/rabbitmq-cluster-operator`, cf. ADR-16). Pas de chart RabbitMQ tiers
  (Bitnami) : il réimplémenterait à la main ce que l'opérateur fait mieux.
- **Conséquences.**
  - Une quarantaine de lignes déclaratives : l'opérateur crée `StatefulSet/rabbitmq-server`,
    `Service/rabbitmq` (5672, 15672, 5552, 15692), `Service/rabbitmq-nodes` et
    `Secret/rabbitmq-default-user` (administrateur, réservé à l'exploitation : ADR-17), active
    `rabbitmq_peer_discovery_k8s`, et pose un `preStop` qui attend la resynchronisation des
    quorum queues.
  - Dépendance à **cert-manager** (webhooks d'admission de l'opérateur) et à des CRD installées
    côté cluster (`kubectl apply --server-side`, le CRD dépassant la taille de l'annotation
    `last-applied-configuration`).
  - `terminationGracePeriodSeconds: 300` au lieu du défaut de l'opérateur (7 jours) : sur un seul
    nœud, attendre indéfiniment une resynchro bloquerait un rollout raté.
  - Le PDB n'est pas fourni par l'opérateur : `charts/rabbitmq/templates/pdb.yaml`
    (`maxUnavailable: 1`).
  - Configuration ajoutée dans `additionalConfig` : `cluster_partition_handling = pause_minority`
    (la minorité se met en pause au lieu de diverger), `vm_memory_high_watermark.relative = 0.6`,
    `disk_free_limit.absolute = 256MB`. Les files transitoires non exclusives (fonctionnalité
    dépréciée `transient_nonexcl_queues`) restent **refusées**, comme par défaut en 4.x : aucune
    application n'en crée (remote control, gossip, mingle et events Celery désactivés dans les
    workers et le beat). Vérifié : `rabbitmqctl list_deprecated_features --used` vide, état
    `denied`, démo, replay et chaos passent.
  - `additionalPlugins` ne liste que `rabbitmq_stream` et `rabbitmq_stream_management` :
    l'opérateur active déjà `rabbitmq_management`, `rabbitmq_prometheus` et
    `rabbitmq_peer_discovery_k8s` (ConfigMap générée `rabbitmq-plugins-conf`).
  - Pas de limite CPU (évite le throttling de la VM Erlang) ; la limite mémoire (600Mi) sert à
    l'opérateur pour calculer la mémoire disponible.
  - Là où l'opérateur ne peut pas être installé (pas de droits cluster-admin, pas d'accès à
    Internet), le chart `rabbitmq-standalone` fournit le même cluster sans opérateur
    (ADR-19) ; c'est une alternative, pas le défaut.

### ADR-02 — Quorum queues pour les tâches

- **Contexte.** Les files de tâches portent des commandes : une perte de message = une commande
  perdue. Les files classiques mirrorées n'existent plus en RabbitMQ 4.
- **Décision.** Toutes les files de tâches sont des quorum queues (réplication Raft sur les 3
  nœuds), déclarées explicitement par les applications. `default_queue_type` n'est **pas** forcé
  côté broker (cf. commentaire de `additionalConfig` dans
  `charts/rabbitmq/values.yaml`) : c'est le contrat qui l'impose.
- **Conséquences.**
  - Un message confirmé (publisher confirm) est écrit sur une majorité de nœuds : la perte d'un
    nœud ne perd ni message ni disponibilité.
  - Pas de QoS *global* sur une quorum queue : Celery le détecte et applique un prefetch par
    consommateur (`worker_prefetch_multiplier=1`).
  - Les tâches à ETA/countdown ne peuvent pas rester longtemps non acquittées dans le worker
    de façon fiable : Celery utilise sa *native delayed delivery* (ADR-03).
  - Coût : plus d'E/S disque et de mémoire qu'une file classique, et des objets durables
    uniquement.

### ADR-03 — Chaque file est liée à un exchange *topic* du même nom

- **Contexte.** Les tâches réessayées (`autoretry_for` + `retry_backoff`) sont republiées avec un
  compte à rebours. Sur quorum queues, Celery ≥ 5.5 implémente ce délai côté broker (*native
  delayed delivery*) : une cascade de files `celery_delayed_0..27` à TTL 2^N secondes, qui se
  dead-letterent de niveau en niveau, puis l'exchange `celery_delayed_delivery` route le message
  vers sa file de destination avec un binding `#.<file>`. Ce binding à joker n'est possible que
  sur un exchange `topic` ; Celery ignore les files liées à un exchange `direct`.
- **Décision.** `task_default_exchange_type="topic"` et
  `task_create_missing_queue_exchange_type="topic"` : chaque file `X` est liée à l'exchange
  topic `X` avec la clé `X`.
- **Conséquences.**
  - Les retries ne bloquent aucun slot de worker et survivent au redémarrage des workers (le
    message attend dans RabbitMQ, répliqué).
  - Celery crée au démarrage des workers 29 exchanges et 28 files techniques. C'est attendu ;
    ils ne doivent pas être supprimés à la main.
  - La granularité est la seconde et le délai maximal ~2^28 s : sans importance ici.

### ADR-04 — `task_create_missing_queue_type="quorum"` partout

- **Contexte.** Dans une chain Celery, ce n'est pas le gateway qui publie les maillons 2 et 3 :
  c'est le **worker** qui vient de terminer le maillon précédent (et c'est le worker en échec qui
  publie l'errback). Celery déclare la file cible avant de publier. Si worker-payments déclarait
  `inventory` en file classique alors qu'elle existe en quorum, RabbitMQ fermerait le canal avec
  `PRECONDITION_FAILED - inequivalent arg 'x-queue-type'`.
- **Décision.** Le même bloc de configuration de files est recopié dans les cinq applications
  Celery (gateway, 3 workers, scheduler) :

  ```python
  task_default_queue_type="quorum",
  task_default_exchange_type="topic",
  task_create_missing_queue_type="quorum",
  task_create_missing_queue_exchange_type="topic",
  task_routes=(route_by_prefix,),
  ```

- **Conséquences.** N'importe quel service peut créer n'importe quelle file en premier, l'ordre
  de démarrage est indifférent. En contrepartie, ce bloc est une **partie du contrat** : le
  modifier dans un seul service casse les autres (d'où sa présence dans [contracts.md](contracts.md) §3).

### ADR-05 — Routage par préfixe du nom de tâche

- **Contexte.** Le gateway ne connaît pas les tâches (il ne les importe pas) ; il faut pourtant
  savoir dans quelle file publier, y compris pour les maillons publiés par les workers.
- **Décision.** Une fonction de routage identique partout : `<file>.<action>` → file `<file>`.
  Les tâches internes `celery.*` gardent le routage par défaut.
- **Conséquences.** Ajouter une tâche `payments.refund` ne demande aucune configuration de routage.
  Une tâche mal nommée (sans point) part dans la file par défaut du service émetteur : le nommage
  est une règle du contrat.

### ADR-06 — Services indépendants : pas de bibliothèque partagée

- **Contexte.** Une lib commune (« models », « config », « tasks stubs ») recrée un couplage de
  déploiement : changer la lib oblige à redéployer tout le monde, et les imports croisés de tâches
  font charger le code d'un service dans un autre.
- **Décision.** Chaque service est un projet `uv` autonome (son `pyproject.toml`, son `uv.lock`,
  son `Dockerfile`, ses tests). Les tâches sont invoquées par nom (`celery.signature(name)`).
  Le seul artefact partagé est [contracts.md](contracts.md).
- **Conséquences.**
  - Duplication **délibérée et petite** : `config.py` (≈70 lignes), `route_by_prefix`, le bloc de
    déclaration des files, `probes.py`, et le client Redis Sentinel durci (`redis_client.py`,
    recopié du gateway dans le subscriber et le notifier). Elle est visible et testée dans
    chaque service.
  - Les exceptions métier (`PaymentDeclined`, `OutOfStock`) n'existent que dans leur worker :
    les autres services les reçoivent reconstruites par Celery depuis le JSON du résultat
    (`exc_type`, `exc_message`) et les décrivent par `type(exc).__name__`.
  - Une évolution du contrat se fait de façon compatible (nouveaux champs optionnels, nouvelles
    tâches) ; un changement cassant demande une coordination explicite.

### ADR-07 — Diffusion par stream plutôt que par fanout vers des files

- **Contexte.** Chaque événement doit être lu par plusieurs abonnés indépendants, dont certains
  veulent relire l'historique (audit, recalcul d'agrégats) et d'autres seulement le temps réel.
- **Options.**

  | Critère | Exchange fanout + une file par abonné | Stream `orders.events` |
  |---|---|---|
  | Lecture | destructive (message supprimé à l'ack) | non destructive (log append-only) |
  | Nouvel abonné | ne voit que les messages postérieurs à la création de sa file | peut lire depuis `first`, un offset ou un instant |
  | Replay | impossible | oui, pendant la rétention |
  | Abonné lent ou arrêté | sa file grossit indéfiniment | rien ne s'accumule par abonné ; il reprend à son offset |
  | Stockage | N copies | 1 copie, répliquée |
  | Suivi de position | implicite (ce qui reste dans la file) | à la charge du consommateur en AMQP 0.9.1 (ADR-10) |

- **Décision.** Exchange `orders` (topic) → stream `orders.events`, rétention `x-max-age: 7D`,
  consommé en AMQP 0.9.1 avec kombu (même bibliothèque, même port 5672 que Celery).
- **Conséquences.** Replay et ajout d'abonnés gratuits ; la rétention est un paramètre global (le
  plus lent des abonnés doit rattraper en moins de 7 jours). Le protocole stream natif (5552)
  offrirait plus de débit et le suivi d'offset côté serveur, au prix d'une seconde bibliothèque :
  non retenu pour une démo.

### ADR-08 — Éditeur unique, `event_id` déterministe, déduplication

- **Contexte.** La tâche qui publie l'événement est *at-least-once* : si le worker meurt après le
  publish mais avant l'ack, la tâche est rejouée et l'événement publié deux fois.
- **Décision.**
  - Un seul éditeur : `worker-notifier`, depuis l'intérieur de la tâche, sur le pool de
    connexions broker du worker (`app.producer_or_acquire()`), avec publisher confirms.
  - `event_id = "evt_" + sha256(order_id:event_type)[:24]` : le même événement rejoué a le même id
    (et le même `message_id` AMQP).
  - Le subscriber `analytics` déduplique sur `event_id` dans un script Lua atomique
    (`SADD` dans `analytics:orders:seen` puis `HINCRBY`/`HINCRBYFLOAT`).
- **Conséquences.** Le stream peut contenir des doublons ; c'est aux consommateurs dont l'effet
  n'est pas idempotent de dédupliquer. `audit` et `live` ne le font pas (un doublon apparaît deux
  fois, ce qui est acceptable pour un journal et un affichage). Le set `seen` n'est jamais purgé
  (limite assumée).

### ADR-09 — Redis Sentinel en StatefulSet écrit à la main

- **Contexte.** Il faut un Redis hautement disponible et compréhensible ligne à ligne. Les charts
  existants couvrent beaucoup de cas mais masquent les mécanismes qu'on veut montrer.
- **Décision.** `StatefulSet/redis` de 3 pods, chacun avec un conteneur `redis` et un sidecar
  `sentinel` (chart `redis`), plus trois scripts shell, vrais fichiers du chart
  (`charts/redis/files/*.sh`), montés depuis `ConfigMap/redis-scripts` ;
  l'annotation `checksum/scripts` du pod template déclenche un rolling update quand un script
  change (ADR-16).
- **Points de conception.**
  - **Bootstrap (`init.sh`, init container).** Le rôle est décidé au démarrage : (1) demander le
    master courant aux sentinels des *autres* pods ; (2) si personne ne répond ou s'ils désignent
    ce pod lui-même, chercher un pair qui a le rôle `master` ; (3) sinon, `redis-0` est le master
    initial. Si les sentinels désignent encore ce pod (il était master et a été tué sans
    `preStop`), il attend jusqu'à 20 s qu'un failover ait lieu plutôt que de redémarrer en master
    et de servir quelques secondes avant d'être rétrogradé. `podManagementPolicy: OrderedReady`
    rend le premier démarrage déterministe.
  - **Noms DNS plutôt qu'IP.** `replica-announce-ip`, `sentinel announce-ip`,
    `resolve-hostnames yes`, `announce-hostnames yes` avec les FQDN du service headless : l'IP
    d'un pod change à chaque redémarrage, son nom non. Le service headless a
    `publishNotReadyAddresses: true` pour que les pods se résolvent avant d'être Ready. Le
    FQDN (`redis-N.redis-headless.<namespace>.svc.<domaine>`) est construit par les scripts à
    partir de `POD_NAMESPACE` (downward API) et de la value `clusterDomain` : rien n'est écrit
    en dur, le chart s'installe dans n'importe quel namespace, et ces noms complets se
    résolvent depuis le namespace des applications (vérifié : Sentinel répond
    `redis-0.redis-headless.orderflow-infra.svc.cluster.local`, joignable depuis `orderflow`).
  - **Failover propre au `preStop` (`prestop.sh`).** Si le pod arrêté est master : attendre (15 s
    max) que ses réplicas soient vus sains par un sentinel pair, geler les écritures
    (`CLIENT PAUSE 60000 WRITE`), déclencher `SENTINEL FAILOVER` via le sentinel d'un autre pod
    (le sidecar local reçoit SIGTERM en même temps), attendre (40 s max) la désignation d'un
    nouveau master. Les réplicas ont tout le flux : pas d'écriture acquittée perdue lors d'un
    rollout. `terminationGracePeriodSeconds: 75` couvre ces attentes.
  - **Readiness stricte (`readiness.sh`).** Un réplica n'est Ready que s'il est synchronisé **et**
    vu sain (`flags = slave`) par son sentinel local : sinon un failover l'ignorerait et il
    resterait attaché à l'ancien master. Le rolling update n'avance qu'à cette condition
    (+ `minReadySeconds: 10`, le temps d'un rafraîchissement `INFO` des sentinels).
  - **Identité de sentinel stable.** `sentinel myid` = sha1 du nom du pod : un pod recréé garde
    son identité, pas d'entrées fantômes chez les autres sentinels.
  - **`min-replicas-to-write` désactivé.** Mesuré : il provoque ~4 s d'erreurs `NOREPLICAS` après
    chaque failover (le temps que les réplicas se rattachent), erreurs non retentées par les
    clients. Le risque couvert (master isolé par une partition qui accepte des écritures ensuite
    perdues) est négligeable sur un cluster mono-nœud ; les bascules planifiées sont protégées par
    le `CLIENT PAUSE`.
  - **Divers.** AOF `everysec` sur PV ; `maxmemory 128mb` avec `noeviction` (un result backend ne
    doit pas évincer silencieusement : mieux vaut une erreur) ; l'entrypoint de l'image est
    court-circuité pour ne pas charger les modules Redis 8 inutiles ; mot de passe identique pour
    Redis et Sentinel, lu par `redis-cli` via `REDISCLI_AUTH` (jamais en argument).
- **Conséquences.** Mesures sur ce cluster : suppression du pod master → nouveau master en ~2 s,
  ancien pod revenu en réplica en ~19 s ; rolling restart du StatefulSet en ~83 s avec bascules
  propres. En contrepartie, ~250 lignes de shell à maintenir, et la réplication Redis reste
  **asynchrone** (cf. limites).

### ADR-10 — Checkpoint d'offset côté client, dans Redis

- **Contexte.** En AMQP 0.9.1, RabbitMQ ne mémorise pas la position d'un consommateur de stream
  (le suivi d'offset côté serveur n'existe que dans le protocole stream natif). Sans checkpoint,
  un subscriber redémarré repartirait de `STREAM_OFFSET` (tout relire ou tout rater).
- **Décision.** Après traitement d'un message, le subscriber écrit son offset dans
  `stream:orders.events:offset:<SUBSCRIBER_NAME>`, **puis** acquitte. Au démarrage il s'attache à
  `checkpoint + 1` via l'argument `x-stream-offset`. Il ignore les offsets ≤ au dernier traité
  (la livraison reprend au début du *chunk* contenant l'offset demandé).
- **Conséquences.**
  - Sémantique *at-least-once* côté abonné : un crash entre traitement et checkpoint rejoue un
    message, d'où la déduplication d'`analytics` (ADR-08).
  - Deux instances d'un même subscriber se disputeraient la clé : `replicas: 1` et stratégie
    `Recreate`. Pas de scaling horizontal d'un abonné donné.
  - Un nouvel abonné = un nouveau `SUBSCRIBER_NAME`, donc un nouveau checkpoint : c'est ce qui
    rend le replay trivial (`SUBSCRIBER_ROLE` permet de réutiliser un comportement existant).

### ADR-11 — Probes basées sur des fichiers

- **Contexte.** `celery inspect ping` comme probe ouvre une connexion broker et passe par le
  remote control (désactivé ici) ; c'est coûteux et lent, et dépend du broker : une panne
  RabbitMQ ferait redémarrer tous les workers.
- **Décision.** Les workers maintiennent eux-mêmes `/tmp/celery-ready` (créé au signal
  `worker_ready`, supprimé à l'arrêt) et `/tmp/celery-heartbeat` (touché toutes les 10 s par un
  timer de la boucle d'événements, bootstep `LivenessHeartbeat`). Startup/readiness =
  `test -f /tmp/celery-ready` ; liveness = heartbeat de moins de 60 s. Les subscribers touchent
  `/tmp/subscriber-heartbeat` toutes les 5 s depuis la boucle de consommation (aussi au repos).
  Le scheduler touche `/tmp/beat-heartbeat` à chaque `tick()` de la boucle de beat (au moins
  toutes les 10 s, `beat_max_loop_interval=10`) via une sous-classe de `PersistentScheduler`
  (`HeartbeatScheduler`) ; startup = fichier présent, liveness = moins de 60 s.
  Le gateway expose `/healthz` (liveness, sans dépendance) et `/readyz` (broker + Redis).
- **Conséquences.** Probes quasi gratuites ; la liveness détecte une boucle principale bloquée.
  Le comportement pendant une longue coupure du broker n'a pas été mesuré (cf. limites). Les
  workers tournent avec `--without-gossip --without-mingle --without-heartbeat`,
  `worker_send_task_events=False` et `worker_enable_remote_control=False` : moins d'objets et de
  trafic sur le broker.

### ADR-12 — Contexte de sécurité

Tous les conteneurs applicatifs : `runAsNonRoot` (UID/GID 10001, créés dans l'image),
`readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`,
`seccompProfile: RuntimeDefault`, `automountServiceAccountToken: false`. Seul `/tmp` est
inscriptible (`emptyDir`, 16Mi). Redis tourne en UID 999 avec le même durcissement ; ses
configurations sont générées dans un `emptyDir` car Redis et Sentinel les réécrivent.
Images multi-stage : `uv sync --locked --no-dev` dans l'étape de build, seule la `.venv` est
copiée dans l'image finale `python:3.13-slim`. Mots de passe, tous aléatoires et jamais
versionnés : `Secret/rabbitmq-default-user` (administrateur, généré par l'opérateur — ou par
le chart `rabbitmq-standalone`, avec `Secret/rabbitmq-erlang-cookie`, ADR-19 —, réservé à
l'exploitation), `Secret/redis-auth` (généré par le chart `redis` au premier install puis
conservé ; ADR-16) et, dans le namespace des applications, `Secret/rabbitmq-credentials`
(utilisateur `orderflow` sans droit d'administration) et `Secret/redis-credentials` (ADR-17).

### ADR-13 — Ressources

| Conteneur | Requests | Limits | Remarque |
|---|---|---|---|
| gateway | 50m / 128Mi | 500m / 256Mi | |
| workers | 50m / 128Mi | 500m / 256Mi | `terminationGracePeriodSeconds: 60` (> `task_time_limit=30`) pour l'arrêt à chaud |
| subscribers | 20m / 64Mi | 200m / 128Mi | |
| scheduler | 20m / 64Mi | 200m / 128Mi | un seul processus beat, quasi inactif |
| rabbitmq | 100m / 400Mi | — / 600Mi | pas de limite CPU (Erlang) ; alarme mémoire vers 0,6 × 600Mi |
| redis | 50m / 64Mi | — / 256Mi | `maxmemory 128mb` + marge pour le fork AOF et les buffers de réplication |
| sentinel | 20m / 16Mi | — / 64Mi | |
| opérateur RabbitMQ | 50m / 128Mi | — / 256Mi | patch dans `k8s/operators/` (l'upstream réserve 500Mi) |

Toutes ces valeurs sont des *values* des charts (`resources` de chaque composant).

Dimensionné pour tenir sur un poste de développement, pas pour une charge réelle.

### ADR-14 — PDB, anti-affinité, répartition

RabbitMQ et Redis ont chacun un PDB `maxUnavailable: 1` : une maintenance volontaire laisse
toujours 2 nœuds RabbitMQ (majorité des quorum queues et du stream) et 2 sentinels (quorum) avec
au moins un réplica Redis. L'anti-affinité par `kubernetes.io/hostname` et les
`topologySpreadConstraints` sont en mode *preferred* / `ScheduleAnyway` : sur le cluster
mono-nœud de démo, une règle *required* laisserait les pods 2 et 3 en `Pending`. Sur un vrai
cluster multi-nœuds, passer en *required* pour que la perte d'un nœud n'emporte qu'un réplica.

### ADR-15 — Tâches planifiées : un Celery beat dédié, mono-réplica, qui ne fait que nommer des tâches

- **Contexte.** Un digest des ventes (compteurs d'`analytics:orders`) doit être diffusé dans le
  stream à intervalle régulier (chaque minute pour la démo). Il faut un déclencheur périodique
  qui respecte les principes du projet : services indépendants, tâches invoquées par nom.
- **Décision.**
  - Un service `scheduler` autonome (`services/scheduler`, image `orderflow/scheduler`) qui
    exécute **`celery beat` seul** : aucune tâche enregistrée, aucun worker, pas de result
    backend (`result_expires=None`, ce qui évite aussi l'entrée par défaut
    `celery.backend_cleanup`, qui partirait dans une file `celery` sans consommateur). Son
    `beat_schedule` ne contient que des **noms** de tâches et un crontab issu de
    `DIGEST_CRON`. Il recopie le bloc de déclaration des files (ADR-04), le routage par
    préfixe, le JSON seul, `confirm_publish` et la politique de retry de publication du gateway.
  - La tâche `notifications.sales_digest` vit dans `worker-notifier`, seul éditeur du stream
    (ADR-08) : elle lit le hash d'analytics (contrat de lecture, [contracts.md §4](contracts.md#4-stream-dévénements))
    et publie un `order.digest`.
  - `Deployment/scheduler` à **1 réplica** et stratégie **`Recreate`** : deux beats enverraient
    chaque tick deux fois ; un `RollingUpdate` ferait cohabiter l'ancien et le nouveau pod
    pendant un rollout.
  - Chaque tick part avec `expires` (50 s par défaut, < 60 s imposé) : Celery en fait le TTL du
    message AMQP (RabbitMQ jette un tick non consommé à temps) et un en-tête vérifié par le
    worker (un tick reçu ou relivré trop tard est écarté). Mesuré : notifier arrêté 2 min 20,
    deux ticks envoyés, file `notifications` vide à la reprise, seul le tick suivant exécuté.
  - Store du planning : `PersistentScheduler` par défaut (fichier shelve
    `/tmp/celerybeat-schedule`, emptyDir), avec `beat_cron_starting_deadline=30`. Le planning
    est entièrement défini dans le code ; le fichier ne retient que les dates de dernier
    passage. Pod recréé : fichier vide, chaque entrée part de « maintenant », premier envoi au
    créneau suivant, aucun rattrapage. Conteneur redémarré dans le même pod : un créneau manqué
    depuis plus de 30 s est abandonné, au plus le créneau courant part une fois. Mesuré : pod
    supprimé à hh:mm:16 → premier envoi à la minute suivante ; conteneur tué à hh:mm:38 →
    aucun envoi supplémentaire.
  - Le digest est identifié par sa minute (`period`), dérivée par le worker de l'heure courante
    (les arguments d'une entrée beat sont statiques) : avec `expires` < 60 s, un tick dupliqué
    ou relivré produit le même `event_id`.
- **Conséquences.**
  - **Beat arrêté** (crash, rollout, nœud perdu) : aucun tick pendant l'arrêt, pas de rattrapage
    au retour. Pour un instantané de compteurs cumulés, c'est le comportement voulu ; une tâche
    planifiée « qui ne doit jamais être sautée » demanderait un autre mécanisme (idempotence
    par période + rattrapage explicite).
  - Beat est un **point unique** assumé : la disponibilité vient du redémarrage par Kubernetes
    (quelques secondes), pas de la redondance. Monter à 2 réplicas doublerait les ticks
    (les doublons seraient néanmoins dédupliquables : même `event_id`).
  - Un nouveau traitement périodique = une entrée de plus dans `beat_schedule` (un nom de
    tâche), sans toucher au worker qui l'exécute.
  - Liveness par fichier heartbeat (ADR-11) ; pendant une coupure broker prolongée, beat bloque
    dans sa reconnexion, le heartbeat vieillit et Kubernetes le redémarre (sans conséquence :
    rien à rattraper). Ce cas n'a pas été mesuré.
- **Pourquoi pas un `CronJob` Kubernetes ?** Un CronJob qui lancerait un conteneur « envoie la
  tâche par nom puis sort » est une alternative honnête :

  | Critère | CronJob Kubernetes | Celery beat dédié |
  |---|---|---|
  | Processus permanent | non (pod créé à chaque tick) | oui (1 pod, ~60 Mi) |
  | Granularité | la minute, plus le démarrage d'un pod Python (quelques secondes) à chaque tick | la seconde, envoi immédiat |
  | Ticks manqués | `startingDeadlineSeconds`, `concurrencyPolicy: Forbid` | `expires`, `beat_cron_starting_deadline` |
  | Doublons | possibles (un Job peut être créé deux fois, documenté) | aucun tant qu'il n'y a qu'un beat |
  | Visibilité | historique de Jobs dans l'API Kubernetes | logs du beat (`Sending due task`) |
  | Où vit le planning | manifeste k8s | code + variables d'environnement du service, comme le reste du contrat |
  | Hors Kubernetes (`docker compose`) | rien d'équivalent | identique |

  Pour une tâche toutes les minutes, créer un pod par tick est coûteux et bruyant ; beat garde
  aussi le planning du côté Celery, testable unitairement et identique en local. Pour des
  tâches rares (quotidiennes) dont on veut l'historique dans l'API Kubernetes, le CronJob
  serait un choix défendable.

### ADR-16 — Trois charts Helm, infrastructure et applications dans deux namespaces

- **Contexte.** Les manifestes Kustomize figeaient des choix d'environnement
  (`storageClassName: local-path`, images `orderflow/*:dev`, tailles, réplicas) et
  répartissaient la logique de déploiement entre YAML et scripts (Secret Redis créé par un
  script, `rollout restart` systématique). Il faut un déploiement paramétrable, versionné et
  réversible, sans perdre aucun réglage fin (probes, preStop, stratégies, securityContext).
  Une première version regroupait RabbitMQ et Redis dans un seul chart `orderflow-infra`,
  installé avec les applications dans un namespace unique : mettre à jour Redis passait par
  la même release que le broker, et rien ne séparait le code applicatif de l'infrastructure
  stateful (droits, quotas, réseau).
- **Décision.** Trois charts locaux, trois releases, deux namespaces :

  | Release / chart | Namespace | Contenu | Cycle de vie |
  |---|---|---|---|
  | `rabbitmq` (`charts/rabbitmq`) | `orderflow-infra` | `RabbitmqCluster` + PDB | rare, prudent : données persistantes, l'opérateur redémarre les nœuds un par un en attendant la resynchronisation des quorum queues ; ou `charts/rabbitmq-standalone` (StatefulSet, Services, ConfigMap, Secrets, PDB, sans opérateur : ADR-19) |
  | `redis` (`charts/redis`) | `orderflow-infra` | StatefulSet Redis Sentinel + `ConfigMap/redis-scripts`, Services, PDB, `Secret/redis-auth` | rare, prudent : rolling update avec failover au `preStop` |
  | `orderflow` (`charts/orderflow`) | `orderflow` | `ConfigMap/orderflow-config`, gateway + Service, 3 workers, scheduler, 3 subscribers, test `helm test` | fréquent : chaque build d'image ou changement de configuration |

  - **Un chart par composant d'infrastructure.** RabbitMQ et Redis n'ont ni le même mécanisme
    de mise à jour (opérateur contre StatefulSet écrit à la main), ni les mêmes risques, ni
    forcément le même calendrier : un réglage Redis ne doit pas créer de révision du broker,
    et un `helm rollback redis` ne doit pas ramener RabbitMQ en arrière. Chaque chart est
    autonome (son `Chart.yaml`, ses values, son schéma, ses helpers, ses NOTES) et
    s'installe, se met à jour et se désinstalle seul (`deploy-infra.ps1 -Only rabbitmq|redis`).
    Vérifié : un `helm upgrade` sans changement de l'un ou de l'autre ne redémarre aucun pod.
    Leurs values sont à plat (`persistence.size`, pas `redis.persistence.size`) : un chart =
    un composant.
  - **Un chart pour les applications.** Un déploiement applicatif (plusieurs fois par jour)
    ne doit jamais risquer un rollout de Redis ou de RabbitMQ, et un `helm rollback orderflow`
    ne touche pas l'infrastructure : trois releases = trois historiques et trois rollbacks
    indépendants.
  - **Deux namespaces : `orderflow-infra` et `orderflow`.** Ce que la séparation apporte :
    - **RBAC** : on peut donner aux développeurs (ou à la CI) les droits d'écriture sur
      `orderflow` sans leur ouvrir `orderflow-infra` (StatefulSets, PVC, `Secret/redis-auth`,
      administrateur RabbitMQ) ; un `kubectl delete` ou un `helm uninstall` applicatif ne
      peut pas emporter les données ;
    - **quotas** : `ResourceQuota` / `LimitRange` par namespace, pour qu'un emballement des
      workers n'affame pas le broker (non posés ici, cf. limites) ;
    - **rayon d'action** : supprimer ou recréer le namespace applicatif
      (`destroy.ps1 -KeepInfra`) laisse files, stream et données Redis intacts ;
    - **frontière réseau** : une `NetworkPolicy` dans `orderflow-infra` peut n'admettre que
      le trafic venant du namespace `orderflow` (sélecteur `kubernetes.io/metadata.name`) sur
      5672 et 26379/6379 (non posées ici, cf. limites).

    Ce qu'elle coûte : les noms DNS courts ne suffisent plus (FQDN
    `rabbitmq.orderflow-infra.svc.cluster.local`, dérivés de la value `infraNamespace` du
    chart `orderflow`), et surtout **un Secret ne se référence pas d'un namespace à
    l'autre** : les identifiants doivent exister dans le namespace des applications, ce qui
    demande un mécanisme de provisionnement et de resynchronisation (ADR-17).
  - **Opérateur hors chart.** Il n'existe pas de chart Helm officiel du RabbitMQ Cluster
    Operator (le chart Bitnami est exclu) ; ses CRD sont un objet de cluster que Helm gère mal
    (jamais mis à jour ni supprimés par `helm upgrade`/`uninstall`). Il reste installé par le
    manifeste upstream épinglé (`k8s/operators/`, `kubectl apply --server-side`). Le chart
    `rabbitmq` vérifie sa présence : face à un vrai cluster sans le CRD
    `rabbitmq.com/v1beta1`, `helm install` échoue avec un message explicite. La vérification
    ne s'applique qu'en ligne (une `lookup` du namespace `kube-system` répond) : `helm lint`,
    `helm template` et `--dry-run` client fonctionnent sans cluster.
  - **Noms fixes.** Les objets gardent exactement les noms du contrat (`rabbitmq`, `redis`,
    `redis-sentinel`, `redis-auth`, `orderflow-config`, `gateway`…, cf.
    [contracts.md §2](contracts.md#2-infrastructure-et-namespaces)), sans préfixe
    `<release>-` : les applications, la documentation et les scripts les référencent en dur.
    Conséquence assumée : **une seule installation par namespace**. Les labels standard
    (`app.kubernetes.io/name|instance|component|part-of|managed-by`, `helm.sh/chart`) viennent
    des `_helpers.tpl` ; les sélecteurs ne contiennent que `name` + `instance`, et ni
    `helm.sh/chart` ni la version ne figurent dans les pod templates ou les
    `volumeClaimTemplates` (immuables) : changer la version d'un chart ne relance aucun pod.
  - **Mot de passe Redis généré par le chart et conservé.** Au premier install,
    `randAlphaNum 32` ; à chaque upgrade, `lookup` relit `Secret/redis-auth` et réutilise sa
    valeur (une rotation casserait Redis/Sentinel, configurés au démarrage des pods). Le Secret
    porte `helm.sh/resource-policy: keep` (il survit à `helm uninstall` et une réinstallation le
    reprend) ; `auth.existingSecret` permet de fournir son propre Secret.
  - **Annotations de checksum.** `checksum/scripts` (contenu de `files/*.sh`) sur le
    StatefulSet Redis, `checksum/config` (données de `orderflow-config`) et
    `checksum/credentials` (données des Secrets d'identifiants, ADR-17) sur les Deployments :
    modifier un script, une valeur de configuration ou un mot de passe relance exactement les
    pods concernés, avec leur stratégie habituelle (rolling update Redis avec failover au
    `preStop`, `Recreate` du scheduler et des subscribers). Un `helm upgrade` sans changement
    ne relance rien.
  - **Tag `dev` mutable.** Reconstruire `orderflow/gateway:dev` ne change pas le pod template,
    donc rien ne bouge. `deploy-apps.ps1` calcule une empreinte du **contenu** de chaque image
    locale (sha256 des couches et de la configuration, via `docker image inspect`) et la passe
    au chart (`imageIds.<image>`), posée en annotation `checksum/image` : seuls les pods dont
    l'image a réellement changé sont relancés, et un redéploiement à l'identique n'en recrée
    aucun. Pas l'`Id` docker : avec le magasin d'images containerd, c'est le digest de l'index,
    qui embarque une attestation de provenance horodatée et change à chaque build, même
    entièrement en cache (constaté). Préféré à un `kubectl rollout restart`
    systématique (impératif, hors release, redémarre tout) et à un horodatage de build (qui
    relancerait tout à chaque déploiement). Avec des tags immuables (`image.tag`), l'annotation
    est inutile.
  - **StorageClass.** `storageClassName` vide par défaut = classe par défaut du cluster
    (`local-path` sur k3s) ; surcharge par la value `persistence.storageClassName` de chaque
    chart d'infrastructure.
- **Conséquences.**
  - Paramétrage par *values* (images et registry, réplicas, ressources, stockage, planning du
    digest, niveau de log, abonnés) validé par `values.schema.json` (par exemple
    `scheduler.replicas` est refusé : un seul beat, par construction).
  - `helm upgrade --wait` ne sait pas qu'un `RabbitmqCluster` est prêt (le StatefulSet
    `rabbitmq-server` appartient à l'opérateur, pas à la release) : `deploy-infra.ps1` garde des
    attentes explicites (`AllReplicasReady`, `ClusterAvailable`, état Sentinel).
  - `helm rollback` remet les manifestes d'une révision, pas le contenu d'un tag mutable :
    un vrai retour arrière applicatif suppose des tags immuables.
  - `helm test orderflow` vérifie `GET /readyz` depuis le cluster (pod éphémère, image du
    gateway).
  - Les scripts portent les deux namespaces de bout en bout (`-Namespace`, défaut
    `orderflow` ; `-InfraNamespace`, défaut `orderflow-infra`) : `deploy-apps.ps1` transmet
    `infraNamespace` au chart, `chaos.ps1` supprime les pods Redis/RabbitMQ dans le namespace
    d'infrastructure et lit les journaux applicatifs dans l'autre.
  - Mesuré après la séparation : failover Redis vu des applications inchangé (POST le plus
    lent 2,0 à 2,2 s sur quatre exécutions de `chaos.ps1`, une valeur isolée à 7,7 s non
    reproduite ; cf. [exploitation.md](exploitation.md#test-de-chaos)).

### ADR-17 — Identifiants des applications, utilisateur RabbitMQ dédié et Secrets provisionnés par script

- **Contexte.** Avec un seul namespace, les applications lisaient directement
  `Secret/rabbitmq-default-user` (l'**administrateur** créé par l'opérateur : tag
  `administrator`, tous les droits, accès à l'UI et à l'API HTTP) et `Secret/redis-auth`. Dans
  un autre namespace, c'est impossible : `secretKeyRef` ne vise que le namespace du pod. Il
  faut donc des Secrets dans le namespace `orderflow`, et c'est l'occasion de ne plus confier
  l'administrateur du broker au code applicatif.
- **Décision.**
  - **Deux Secrets dans le namespace des applications**, de noms configurables (values
    `rabbitmq.secretName`, défaut `rabbitmq-credentials`, clés `username` / `password` ;
    `redis.secretName`, défaut `redis-credentials`, clé `password`), provisionnés par
    `scripts/deploy-apps.ps1` **avant** le `helm upgrade`.
  - **RabbitMQ : utilisateur dédié `orderflow`**, créé par `rabbitmqctl` dans un pod du cluster,
    sans aucun tag (ni `administrator` ni `management`), permissions `configure` / `write` /
    `read` `.*` sur le vhost `/`. Mot de passe aléatoire (32 caractères alphanumériques,
    générateur cryptographique) généré **une seule fois** : si le Secret existe déjà, son mot de
    passe est réutilisé, et le script vérifie seulement (`rabbitmqctl authenticate_user`) que
    l'utilisateur existe avec ce mot de passe ; il ne réécrit ni utilisateur, ni tag, ni
    permission déjà conformes. Le mot de passe n'est jamais affiché ni passé en argument : il va
    à `rabbitmqctl` par l'entrée standard de `kubectl exec` (absent de la ligne de commande du
    poste, de la requête exec envoyée à l'API server et des arguments des processus du pod), et
    le Secret est écrit par `kubectl apply --server-side -f -` (manifeste sur l'entrée
    standard). L'utilisateur est créé avant le Secret : un pod ne reçoit jamais l'identifiant
    d'un utilisateur inexistant.
  - **Redis : copie** de la clé `password` de `orderflow-infra/redis-auth` (ou du Secret
    désigné par `auth.existingSecret`, lu sur le StatefulSet) vers `orderflow/redis-credentials`,
    resynchronisée à chaque exécution. Redis n'a qu'un mot de passe (pas d'utilisateur
    applicatif distinct : ACL Redis non utilisées, cf. limites).
  - **Écritures idempotentes.** Server-side apply : un contenu identique ne modifie pas le
    Secret (`resourceVersion` inchangé). Vérifié : une seconde exécution de `deploy.ps1` ne
    recrée aucun pod, ne change aucun `resourceVersion` ni l'empreinte du hash de mot de passe
    de l'utilisateur `orderflow` dans RabbitMQ.
  - **Rotation = relance des pods concernés.** Annotation `checksum/credentials` (sha256 des
    données des Secrets utilisés par le pod : RabbitMQ seul pour le scheduler, RabbitMQ et Redis
    pour les autres), calculée **dans le chart par `lookup`** plutôt que transmise par le
    script : elle suit le Secret réel quel que soit l'outil qui l'a modifié (script, `kubectl`,
    rotation manuelle) dès le `helm upgrade` suivant, et rien de dérivé du mot de passe ne passe
    par la ligne de commande ni ne s'inscrit dans les values de la release (seule une
    empreinte sha256 figure sur les pods). Vérifié : rotation du mot de passe RabbitMQ → les 8
    Deployments relancés ; rotation du mot de passe Redis → les 7 Deployments qui l'utilisent
    relancés, le scheduler non.
  - **Garde-fou.** Face à un vrai cluster, le chart `orderflow` échoue avec un message
    explicite si l'un des Secrets (ou l'une de ses clés) manque :
    `Secret/rabbitmq-credentials (clé username) absent du namespace orderflow : lancer
    scripts/deploy-apps.ps1, qui le provisionne avant helm, ou le créer à la main (…)`.
    Même technique que le contrôle du CRD du chart `rabbitmq` : la vérification n'a lieu que
    si `lookup` du namespace `kube-system` répond ; `helm lint` et `helm template` restent
    utilisables hors ligne. L'équivalent manuel (commandes `kubectl` / `rabbitmqctl`) est dans
    les NOTES du chart et dans [exploitation.md](exploitation.md#identifiants-des-applications).
- **Pourquoi `.*` sur le vhost `/` et pas des permissions plus fines ?** Les applications
  déclarent elles-mêmes leurs files, exchanges et stream (`payments`, `inventory`,
  `notifications`, `orders`, `orders.events`) et Celery crée sa topologie de *native delayed
  delivery* (`celery_delayed_delivery`, `celery_delayed_0..27`, bindings `#.<file>`) : une
  expression `configure` restreinte devrait suivre exactement ces noms et toute évolution de
  Celery, pour un gain faible tant que tous les services partagent un utilisateur. L'essentiel
  est obtenu : plus de droits d'administration (pas de création d'utilisateur ni de vhost, pas
  d'UI ni d'API de management). Vérifié sans droits d'administration : déclarations de files
  quorum, d'exchanges, du stream, topologie de delayed delivery, publisher confirms (démo,
  replay, chaos), aucune erreur `ACCESS_REFUSED`.
- **Alternatives non retenues.**

  | Option | Principe | Pourquoi pas ici |
  |---|---|---|
  | **RabbitMQ Messaging Topology Operator** | CR `User` et `Permission` dans le namespace des applications ; l'opérateur crée l'utilisateur et un Secret d'identifiants à côté, de façon déclarative (et peut aussi gérer `Queue`, `Exchange`, `Policy`) | un opérateur de plus à installer, mettre à jour et faire tenir en mémoire, pour une démo qui n'a qu'un utilisateur ; la création par `rabbitmqctl` reste lisible en quelques lignes de script. Premier choix sur un vrai cluster, avec un utilisateur par service. |
  | **External Secrets Operator** (ou équivalent : Vault, réplicateur de Secrets) | synchronise `redis-auth` (et des identifiants RabbitMQ) depuis un coffre ou d'un namespace à l'autre, avec rafraîchissement périodique | même coût (un opérateur et souvent un coffre) ; il ne crée pas l'utilisateur RabbitMQ lui-même. |
  | Applications dans le namespace d'infrastructure | aucun Secret à copier | renonce à tout ce que la séparation apporte (ADR-16). |
  | Secrets créés par le chart `orderflow` (lookup dans l'autre namespace) | le chart lirait `redis-auth` par `lookup` et créerait sa copie | `lookup` est désactivé en `helm template` et en dry-run client (rendu vide, donc un Secret vide en GitOps), et un chart applicatif n'a pas à lire les Secrets de l'infrastructure ; création de l'utilisateur RabbitMQ impossible depuis Helm de toute façon. |

- **Conséquences.**
  - Les applications n'ont plus aucun droit d'administration sur le broker ; l'administrateur
    reste dans `orderflow-infra`, réservé à l'exploitation (UI de management).
  - Le déploiement applicatif dépend d'une étape hors Helm (le script, ou l'équivalent manuel) ;
    le garde-fou du chart rend son oubli explicite.
  - Une rotation du mot de passe Redis demande un redémarrage complet de Redis (Redis et
    Sentinel lisent le mot de passe au démarrage du pod), puis `deploy-apps.ps1` : procédure
    dans [exploitation.md](exploitation.md#rotation-des-mots-de-passe). Celle du mot de passe
    RabbitMQ se fait sans toucher au broker.
  - `destroy.ps1 -KeepInfra` supprime aussi l'utilisateur `orderflow` (ses identifiants
    disparaissent avec le namespace) ; un nouveau déploiement le recrée.

### ADR-18 — Journaux structurés du gateway avec structlog

- **Contexte.** Le gateway écrivait des lignes texte (`order accepted order_id=… …`) et
  uvicorn son propre access log : rien de fiable à indexer pour un collecteur (Filebeat,
  Fluent Bit, Loki), aucun moyen de relier les lignes d'une même requête, et les sondes de
  Kubernetes (`/healthz`, `/readyz` toutes les 10 à 20 s par pod) noyaient le flux.
- **Décision.** `structlog` 26.1, configuré dans un seul module
  (`services/gateway/src/gateway/logs.py`) :
  - **un objet JSON par ligne** sur stdout par défaut (`LOG_FORMAT=json`) : `timestamp`
    ISO 8601 UTC, `level`, `logger`, `event` (nom court en snake_case : `order_accepted`,
    `dependency_unavailable`, `http_request`…) et des champs clé/valeur, jamais de valeur
    formatée dans le message. `LOG_FORMAT=console` donne un rendu lisible
    (`dev/docker-compose.yml` l'utilise) ; `LOG_LEVEL` reste le niveau ; value
    `gateway.logFormat` du chart ;
  - **contexte de requête** (`structlog.contextvars`) : un middleware ASGI pur lie
    `request_id` (en-tête `X-Request-ID` s'il est valide, sinon un uuid4, renvoyé dans la
    réponse), `method` et `path`, puis émet **un événement d'accès par requête** (`status`,
    `duration_ms`) ; l'access log d'uvicorn est coupé. `order_id` est lié dès qu'il est
    connu (handler) et repris dans l'événement d'accès. Les handlers étant synchrones,
    Starlette les exécute dans son threadpool avec une **copie** du contexte : le
    `request_id` y est visible (vérifié par un test avec 8 requêtes simultanées sur 8
    threads, sans mélange), mais ce que le handler lie ne remonte pas au middleware, d'où
    le passage d'`order_id` par `request.state` ;
  - **sondes** : `/healthz` et `/readyz` en DEBUG (sauf réponse 5xx), donc absentes au
    niveau INFO ;
  - **bibliothèques** (uvicorn.error, celery, kombu, amqp, redis) : passent par le même
    `ProcessorFormatter` (*foreign pre-chain*) ; le flux reste du JSON homogène ;
  - **exceptions** : `exc_info` devient un champ `exception` structuré (type, message,
    frames) ; les `503` d'indisponibilité gardent une seule ligne `warning`
    (`dependency_unavailable`, `error_type`, `error`) sans trace ;
  - **secrets** : toute URL `scheme://user:mot-de-passe@` est masquée (`***`) dans tous les
    champs avant rendu (les URL broker et Sentinel portent un mot de passe).
- **Conséquences.** Exemple réel (cluster, pod gateway) :

  ```json
  {"customer_id": "cust_42", "total_amount": 44.3, "currency": "EUR", "event": "order_accepted", "request_id": "038cf900f22a411fa1f6ef2bee7215b9", "path": "/orders", "method": "POST", "order_id": "ord_6b094e269fdd4a65802e59ef99760872", "level": "info", "logger": "gateway.main", "timestamp": "2026-11-12T07:25:35.477097Z"}
  {"status": 202, "duration_ms": 34.9, "order_id": "ord_6b094e269fdd4a65802e59ef99760872", "event": "http_request", "request_id": "038cf900f22a411fa1f6ef2bee7215b9", "path": "/orders", "method": "POST", "level": "info", "logger": "gateway.access", "timestamp": "2026-11-12T07:25:35.478357Z"}
  ```

  - Seul le gateway est concerné. Les workers, le scheduler et les subscribers gardent le
    logging standard de Python (format texte de Celery et kombu) ; `subscriber-audit`
    écrit déjà une ligne JSON brute par événement du stream (c'est sa fonction, cf.
    [contracts.md §4](contracts.md#4-stream-dévénements)). Les convertir demanderait de
    brancher structlog sur la configuration de logging de Celery (`after_setup_logger`) :
    pas fait, à faire service par service si un collecteur l'exige.
  - Une dépendance de plus (structlog, pure Python, sans dépendance transitive).
  - Le `request_id` n'est pas propagé aux tâches Celery (en-tête de message) : le lien
    entre une requête et le traitement asynchrone reste l'`order_id`.

### ADR-19 — RabbitMQ sans opérateur (environnements restreints)

- **Contexte.** La variante par défaut (ADR-01) suppose des droits d'administrateur du
  cluster : CRD `rabbitmqclusters.rabbitmq.com`, ClusterRole, webhooks d'admission, et
  cert-manager (lui-même des CRD et des webhooks). Le script la télécharge en plus depuis
  GitHub (`k8s/operators/rabbitmq-cluster-operator`, ressource distante
  `https://github.com/rabbitmq/cluster-operator/releases/download/v2.23.2/cluster-operator.yml`).
  Certains environnements n'accordent que des droits sur un namespace et n'ont accès qu'à
  un registre et à un dépôt de paquets internes.
- **Décision.** Un second chart, `charts/rabbitmq-standalone`, installé par
  `deploy-infra.ps1 -NoOperator` sous le **même nom de release** (`rabbitmq`) :
  - uniquement des objets standard et namespacés : `StatefulSet/rabbitmq-server`,
    `Service/rabbitmq` (5672, 15672, 5552, 15692), `Service/rabbitmq-nodes` (headless,
    `publishNotReadyAddresses`), `ConfigMap/rabbitmq-config`, `Secret/rabbitmq-default-user`
    (clés `username` / `password`, comme l'opérateur), `Secret/rabbitmq-erlang-cookie`,
    `PDB/rabbitmq`. **Aucun RBAC** : ni ServiceAccount dédié, ni Role, ni RoleBinding,
    `automountServiceAccountToken: false` ;
  - **remplaçant direct** : mêmes noms d'objets, de pods, de PVC
    (`persistence-rabbitmq-server-N`), de conteneur (`rabbitmq`) et de nœuds Erlang
    (`rabbit@rabbitmq-server-N.rabbitmq-nodes.<namespace>`, noms longs) que l'opérateur :
    applications, chart `orderflow`, scripts et documentation ne changent pas ;
  - reprise de ce que génère l'opérateur (StatefulSet observé dans `orderflow-infra`) :
    même image, mêmes plugins (management, prometheus, stream, stream management),
    `additionalConfig` identique, `total_memory_available_override_value` = 80 % de
    `limits.memory`, sonde de démarrage `reached-target-cluster-size`, readiness = port
    AMQP, `preStop` `rabbitmq-upgrade await_online_quorum_plus_one` puis `drain` (et le
    label `skipPreStopChecks`), `terminationGracePeriodSeconds: 300`,
    `podManagementPolicy: Parallel`, securityContext non-root en lecture seule,
    anti-affinité et répartition *preferred*, plus le PDB ;
  - administrateur créé au premier démarrage par `default_user` / `default_pass`
    **interpolés depuis l'environnement** (`$(RABBITMQ_ADMIN_PASSWORD)`, mécanisme de
    `rabbitmq.conf` vérifié sur 4.3.7), l'environnement venant du Secret : le mot de passe
    n'est jamais dans une ConfigMap. Secrets générés une fois puis conservés (`lookup`,
    `helm.sh/resource-policy: keep`, comme `redis-auth`) ; `auth.existingSecret` et
    `erlangCookie.existingSecret` pour fournir les siens ;
  - image paramétrable (`image.registry`, `image.repository`, `image.tag`,
    `imagePullSecrets`), comme désormais dans les charts `rabbitmq` et `redis` (le chart
    `orderflow` avait déjà `image.registry`, `image.tag`, `imagePullSecrets`) ;
  - même logique pour la construction des images applicatives : images de base en `ARG`
    (`PYTHON_IMAGE`, `UV_IMAGE`) et index PyPI en `ARG UV_DEFAULT_INDEX` dans les six
    Dockerfiles, sans directive `# syntax=` (qui ferait tirer `docker/dockerfile` depuis
    Docker Hub) ; `uv.lock` enregistre pypi.org et `uv sync --locked` refuse un autre index
    (constaté), donc avec un miroir le lock est d'abord résolu à nouveau contre lui et le
    build échoue si une version épinglée change. `build-images.ps1 -BaseRegistry
    -PythonImage -UvImage -PipIndexUrl -Registry -Push`.
- **Pourquoi `classic_config` plutôt que la découverte Kubernetes ?** Le plugin
  `rabbitmq_peer_discovery_k8s` interroge l'API Kubernetes (endpoints du Service headless) :
  il faut un ServiceAccount, un Role (`get` sur `endpoints`) et un RoleBinding, objets que
  certains environnements interdisent ou soumettent à validation. Depuis RabbitMQ 4.1 il
  sait en revanche désigner un nœud d'amorçage (le pod d'ordinal le plus bas).
  `classic_config` n'a besoin de rien : la liste des nœuds est connue d'avance (dérivée de
  `replicas` et du Service headless). Son défaut : un nœud neuf qui ne joint aucun pair
  forme son propre cluster ; si plusieurs nœuds vides démarrent ensemble, deux clusters
  peuvent se former (problème connu en amont, rabbitmq-server#13043, corrigé seulement pour
  le plugin k8s). Parade : l'init container (`files/init.sh`) fait attendre **un nœud sans
  données d'ordinal > 0** jusqu'à ce qu'un pair serve déjà AMQP (donc ait fini de démarrer
  dans un cluster) ; `rabbitmq-server-0` forme le cluster, les autres le rejoignent. Un nœud
  qui a des données ne passe jamais par cette attente (redémarrage, rolling update, arrêt
  complet). `deploy-infra.ps1` vérifie ensuite que **chaque** nœud voit tous les autres
  (`cluster_status`), ce qui détecterait un cluster scindé.
- **Comparaison honnête.**

  | Sujet | Avec opérateur | Sans opérateur (ce chart) |
  |---|---|---|
  | Droits nécessaires | cluster-admin (CRD, ClusterRole, webhooks) + cert-manager | droits d'écriture sur un namespace |
  | Téléchargements au déploiement | manifeste de l'opérateur depuis GitHub (`kubectl apply -k`), images de l'opérateur | aucun (charts locaux, images du registre configuré) |
  | Mise à jour de version RabbitMQ | l'opérateur orchestre (redémarrage nœud par nœud, attentes, contrôles) | rolling update du StatefulSet + `preStop` ; **à vous** de lire les notes de version, de vérifier les feature flags et les sauts de version autorisés |
  | Contrôle d'écart de versions | refuse certains changements dangereux | aucun : le chart applique ce que vous demandez |
  | Scale-up | changer `replicas` | changer `replicas` : nouvelle liste de pairs = rolling update de tous les nœuds, puis `rabbitmq-queues grow` pour étendre les files existantes |
  | Scale-down | non pris en charge non plus (opération manuelle) | manuel : `forget_cluster_node`, puis `replicas` |
  | Rotation des secrets / certificats | Secret administrateur géré, TLS via cert-manager | manuelle (`rabbitmqctl change_password`, cookie : arrêt complet) |
  | Rééquilibrage des leaders | `rabbitmq-queues rebalance` à la main dans les deux cas | idem |
  | Configuration | CR déclarative, valeurs par défaut maintenues par l'équipe RabbitMQ | ConfigMap du chart, valeurs par défaut maintenues par nous |
  | Objets créés | par l'opérateur (Helm ne les voit pas, `--wait` non plus) | par Helm (`helm get manifest`, `--wait` attend le StatefulSet) |
  | Formation initiale | plugin k8s avec nœud d'amorçage | `classic_config` + attente du premier nœud (init container) |
  | Mémoire au repos | + ~50 Mi (opérateur) | rien de plus |

- **Conséquences.**
  - Changer de variante n'est pas une mise à jour : les données (PVC), le cookie et la
    propriété des objets diffèrent. `deploy-infra.ps1` refuse le changement avec un message
    explicite ; `destroy.ps1 -RabbitmqOnly` désinstalle RabbitMQ (PVC, Secrets, checkpoints
    des subscribers), puis `deploy-infra.ps1 [-NoOperator]` et `deploy-apps.ps1` recréent
    tout (utilisateur `orderflow` compris). Files et stream sont perdus.
  - Les scripts détectent la variante (chart de la release `rabbitmq`) : `chaos.ps1`,
    `demo.ps1`, `replay-demo.ps1`, `deploy-apps.ps1` et `destroy.ps1` fonctionnent avec les
    deux sans paramètre ; un `deploy.ps1` relancé garde la variante installée.
  - Vérifié sur ce cluster (cf. [exploitation.md](exploitation.md#environnement-restreint--sans-opérateur-registre-interne)) :
    formation, quorum queues et stream à 3 membres, démo, replay, chaos, suppression d'un
    pod, rolling restart sous trafic, `helm upgrade` sans effet, installations à froid
    répétées, arrêt complet puis redémarrage.
  - Limites : la taille du cluster est écrite dans la configuration (la changer relance
    tous les nœuds) ; un `rabbitmq-server-0` **sans données** n'attend personne : s'il perd
    son volume et redémarre pendant que ses pairs sont eux aussi arrêtés, il peut former
    seul un cluster vide (le relancer après les autres, cf. exploitation.md) ; perte de deux
    volumes sur trois = perte du quorum (comme avec l'opérateur, restauration manuelle).
    Pas de TLS (comme la variante par défaut).

## 3. Limites et pistes d'évolution

| Limite | Impact | Piste |
|---|---|---|
| Cluster k8s **mono-nœud** | La haute disponibilité est démontrée contre la perte d'un *pod*, pas d'une machine. | Cluster multi-nœuds, anti-affinité *required*, zones. |
| Un seul utilisateur RabbitMQ applicatif (`orderflow`), permissions `.*` sur le vhost `/` | Plus de droits d'administration pour les applications (ADR-17), mais chaque service peut encore déclarer, lire ou purger les files des autres. | Un utilisateur par service avec des expressions `configure`/`write`/`read` limitées à ses files (et éventuellement un vhost), déclarés via le **Messaging Topology Operator** (`User`, `Permission`). |
| Mot de passe Redis unique, copié dans le namespace des applications | Les applications ont le même mot de passe que la réplication et Sentinel ; une rotation demande un redémarrage complet de Redis. | ACL Redis (utilisateur applicatif distinct, `ACL SETUSER` avec deux mots de passe pendant une rotation) ; synchronisation par External Secrets. |
| Identifiants provisionnés par script | Sans `deploy-apps.ps1`, il faut créer l'utilisateur et les Secrets à la main (le chart le signale) ; inadapté tel quel à du GitOps. | Messaging Topology Operator et External Secrets (ADR-17). |
| Pas de NetworkPolicy, pas de quota, pas de TLS | La séparation en namespaces prépare la frontière mais rien ne l'impose : trafic AMQP et Redis en clair, joignable par tout pod du cluster ; aucune limite globale de ressources par namespace. | NetworkPolicy dans `orderflow-infra` n'admettant que le namespace `orderflow` (5672, 6379, 26379) et les pods de l'infrastructure entre eux ; `ResourceQuota` par namespace ; TLS via cert-manager (déjà présent) pour AMQP et Redis. |
| Montants en `float` dans les payloads | Arrondis binaires (le hash `analytics:orders` affiche par ex. `639.79999999999999999`). | Montants en centimes entiers ou en chaînes décimales dans le contrat. |
| `analytics:orders:seen` jamais purgé | Croissance non bornée (1 entrée par événement de commande ; les digests n'y entrent pas). | Fenêtre de déduplication (clé par jour avec TTL ≥ rétention du stream), ou filtre probabiliste. |
| Liveness des workers pendant une longue coupure broker | Non mesuré : si la boucle d'événements cesse de tourner pendant les reconnexions, le heartbeat vieillit et Kubernetes redémarre les workers (sans gravité, mais bruyant). | Mesurer ; au besoin, élargir le seuil ou lier le heartbeat à l'état de connexion. |
| Réplication Redis **asynchrone** | Lors d'un failover *non planifié*, les dernières écritures acquittées par l'ancien master peuvent être perdues (résultats de tâches, checkpoint, clé de suivi). Les bascules planifiées sont protégées par le `CLIENT PAUSE`. | `WAIT` sur les écritures critiques, ou stocker l'état métier ailleurs (base de données). |
| `/readyz` du gateway dépend du broker et de Redis | Une panne partagée rend **tous** les pods du gateway non prêts en même temps (après 3 échecs à 10 s d'intervalle). | Readiness limitée à l'état local ; la dégradation passe par les `503` applicatifs. |
| Pas de collecte de métriques ni d'autoscaling | Le plugin `rabbitmq_prometheus` expose `:15692` mais rien ne le lit. | Prometheus/Grafana (dashboards officiels RabbitMQ) ; **KEDA** pour scaler les workers sur la profondeur des files. |
| Images sans registry ni CI | Tag `dev` mutable, `IfNotPresent` : la prise en compte d'un rebuild repose sur l'annotation `checksum/image` posée par `deploy-apps.ps1` (ADR-16), et `helm rollback` ne restaure pas l'ancien code. Ne fonctionne que si le cluster partage le démon docker. | CI qui teste, construit, signe et pousse des tags immuables dans un registry (`build-images.ps1 -Registry … -Push`, values `image.registry`, `image.tag` ; images de base et index PyPI internes : `-BaseRegistry`, `-PipIndexUrl`, cf. ADR-19). |
| Une installation par namespace | Noms d'objets fixes (contrat) : deux releases `orderflow` (ou `redis`) dans le même namespace entreraient en conflit. | Une paire de namespaces par environnement (`-Namespace` / `-InfraNamespace`, value `infraNamespace`) ; préfixer les noms seulement si le besoin de plusieurs instances par namespace apparaît. |
| Pas de dead-letter pour les tâches | Une tâche qui épuise ses retries finit en `FAILURE` (et déclenche l'errback) ; un message qui fait crasher le worker est redélivré jusqu'à la *delivery limit* des quorum queues puis abandonné. | Politique de dead-letter sur les files de tâches et alerte. |
| Pas de clé d'idempotence côté client sur `POST /orders` | Un client qui réessaie après un timeout ou une `503` ambiguë (publication confirmée par le broker mais réponse perdue) crée une seconde commande. | En-tête `Idempotency-Key` mémorisé dans Redis (`SET NX`) et réutilisé comme base de l'`order_id`. |
| Scheduler mono-réplica | Pas de digest pendant un redémarrage du beat, pas de rattrapage (ADR-15). | Acceptable pour un instantané ; sinon verrou distribué (RedBeat) avec plusieurs beats en attente. |
| Un seul réplica par worker et subscriber | Pas de traitement pendant le redémarrage d'un worker (les messages attendent dans la file). | Plusieurs réplicas de workers (sans contrainte : files partagées) ; les subscribers restent à 1 par conception (ADR-10). |
