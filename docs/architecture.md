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
| RabbitMQ | `RabbitmqCluster/rabbitmq` → `StatefulSet/rabbitmq-server` | 3 | Broker des tâches (quorum queues) et du stream. Plugins management, prometheus et peer discovery (activés par l'opérateur), stream et stream management (ajoutés). |
| Redis | `StatefulSet/redis` (conteneurs `redis` + `sentinel`) | 3 | Result backend Celery, clés de suivi des commandes, agrégats, checkpoints. 1 master, 2 réplicas, 3 sentinels (quorum 2). |
| RabbitMQ Cluster Operator | `Deployment/rabbitmq-cluster-operator` (ns `rabbitmq-system`) | 1 | Gère le cycle de vie du cluster RabbitMQ. Requiert cert-manager. |

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
  `orderflow-infra` (`charts/orderflow-infra/templates/rabbitmq-cluster.yaml`) ; l'opérateur
  lui-même reste installé par son manifeste de release épinglé
  (`k8s/operators/rabbitmq-cluster-operator`, cf. ADR-16). Pas de chart RabbitMQ tiers
  (Bitnami) : il réimplémenterait à la main ce que l'opérateur fait mieux.
- **Conséquences.**
  - Une quarantaine de lignes déclaratives : l'opérateur crée `StatefulSet/rabbitmq-server`,
    `Service/rabbitmq` (5672, 15672, 5552, 15692), `Service/rabbitmq-nodes` et
    `Secret/rabbitmq-default-user`, active `rabbitmq_peer_discovery_k8s`, et pose un `preStop`
    qui attend la resynchronisation des quorum queues.
  - Dépendance à **cert-manager** (webhooks d'admission de l'opérateur) et à des CRD installées
    côté cluster (`kubectl apply --server-side`, le CRD dépassant la taille de l'annotation
    `last-applied-configuration`).
  - `terminationGracePeriodSeconds: 300` au lieu du défaut de l'opérateur (7 jours) : sur un seul
    nœud, attendre indéfiniment une resynchro bloquerait un rollout raté.
  - Le PDB n'est pas fourni par l'opérateur : `charts/orderflow-infra/templates/rabbitmq-pdb.yaml`
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

### ADR-02 — Quorum queues pour les tâches

- **Contexte.** Les files de tâches portent des commandes : une perte de message = une commande
  perdue. Les files classiques mirrorées n'existent plus en RabbitMQ 4.
- **Décision.** Toutes les files de tâches sont des quorum queues (réplication Raft sur les 3
  nœuds), déclarées explicitement par les applications. `default_queue_type` n'est **pas** forcé
  côté broker (cf. commentaire de `rabbitmq.additionalConfig` dans
  `charts/orderflow-infra/values.yaml`) : c'est le contrat qui l'impose.
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
  `sentinel` (chart `orderflow-infra`), plus trois scripts shell, vrais fichiers du chart
  (`charts/orderflow-infra/files/redis/*.sh`), montés depuis `ConfigMap/redis-scripts` ;
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
    `publishNotReadyAddresses: true` pour que les pods se résolvent avant d'être Ready.
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
copiée dans l'image finale `python:3.13-slim`. Mots de passe : `Secret/rabbitmq-default-user`
(généré par l'opérateur) et `Secret/redis-auth` (généré aléatoirement par le chart
`orderflow-infra` au premier install puis conservé, jamais versionné ; cf. ADR-16).

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

### ADR-16 — Déploiement par deux charts Helm, opérateur RabbitMQ à part

- **Contexte.** Les manifestes Kustomize figeaient des choix d'environnement
  (`storageClassName: local-path`, images `orderflow/*:dev`, tailles, réplicas) et
  répartissaient la logique de déploiement entre YAML et scripts (Secret Redis créé par un
  script, `rollout restart` systématique). Il faut un déploiement paramétrable, versionné et
  réversible, sans perdre aucun réglage fin (probes, preStop, stratégies, securityContext).
- **Décision.** Deux charts locaux, deux releases dans le namespace `orderflow`, installées
  dans cet ordre :

  | Release / chart | Contenu | Cycle de vie |
  |---|---|---|
  | `orderflow-infra` (`charts/orderflow-infra`) | `RabbitmqCluster` + PDB, StatefulSet Redis Sentinel + `ConfigMap/redis-scripts`, Services, PDB, `Secret/redis-auth` | rare, prudent : données persistantes, rolling updates lents (preStop, resynchronisation) |
  | `orderflow` (`charts/orderflow`) | `ConfigMap/orderflow-config`, gateway + Service, 3 workers, scheduler, 3 subscribers, test `helm test` | fréquent : chaque build d'image ou changement de configuration |

  - **Deux charts plutôt qu'un.** Les deux couches n'évoluent pas au même rythme : un
    déploiement applicatif (plusieurs fois par jour) ne doit jamais risquer un rollout de
    Redis ou RabbitMQ, et un `helm rollback orderflow` ne doit pas ramener l'infrastructure
    en arrière. Deux releases = deux historiques et deux rollbacks indépendants.
  - **Opérateur hors chart.** Il n'existe pas de chart Helm officiel du RabbitMQ Cluster
    Operator (le chart Bitnami est exclu) ; ses CRD sont un objet de cluster que Helm gère mal
    (jamais mis à jour ni supprimés par `helm upgrade`/`uninstall`). Il reste installé par le
    manifeste upstream épinglé (`k8s/operators/`, `kubectl apply --server-side`). Le chart
    `orderflow-infra` vérifie sa présence : face à un vrai cluster sans le CRD
    `rabbitmq.com/v1beta1`, `helm install` échoue avec un message explicite. La vérification
    ne s'applique qu'en ligne (une `lookup` du namespace `kube-system` répond) : `helm lint`,
    `helm template` et `--dry-run` client fonctionnent sans cluster.
  - **Noms fixes.** Les objets gardent exactement les noms du contrat (`rabbitmq`, `redis`,
    `redis-sentinel`, `redis-auth`, `orderflow-config`, `gateway`…, cf.
    [contracts.md §2](contracts.md#2-infrastructure-namespace-orderflow)), sans préfixe
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
    reprend) ; `redis.auth.existingSecret` permet de fournir son propre Secret.
  - **Annotations de checksum.** `checksum/scripts` (contenu de `files/redis/*.sh`) sur le
    StatefulSet Redis et `checksum/config` (données de `orderflow-config`) sur tous les
    Deployments : modifier un script ou une valeur de configuration relance exactement les pods
    concernés, avec leur stratégie habituelle (rolling update Redis avec failover au `preStop`,
    `Recreate` du scheduler et des subscribers). Un `helm upgrade` sans changement ne relance rien.
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
    (`local-path` sur k3s) ; surcharge par `rabbitmq.persistence.storageClassName` et
    `redis.persistence.storageClassName`.
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

## 3. Limites et pistes d'évolution

| Limite | Impact | Piste |
|---|---|---|
| Cluster k8s **mono-nœud** | La haute disponibilité est démontrée contre la perte d'un *pod*, pas d'une machine. | Cluster multi-nœuds, anti-affinité *required*, zones. |
| Utilisateur RabbitMQ par défaut partagé, vhost `/` | Tout service peut tout lire et tout supprimer. | Un utilisateur et des permissions par service (et éventuellement un vhost) via le **Messaging Topology Operator** (`User`, `Permission`, `Queue`, `Exchange`, `Policy` en CRD). |
| Pas de NetworkPolicy, pas de TLS | Trafic AMQP et Redis en clair dans le cluster, joignable par tout pod. | NetworkPolicies par composant ; TLS via cert-manager (déjà présent) pour AMQP et Redis. |
| Montants en `float` dans les payloads | Arrondis binaires (le hash `analytics:orders` affiche par ex. `639.79999999999999999`). | Montants en centimes entiers ou en chaînes décimales dans le contrat. |
| `analytics:orders:seen` jamais purgé | Croissance non bornée (1 entrée par événement de commande ; les digests n'y entrent pas). | Fenêtre de déduplication (clé par jour avec TTL ≥ rétention du stream), ou filtre probabiliste. |
| Liveness des workers pendant une longue coupure broker | Non mesuré : si la boucle d'événements cesse de tourner pendant les reconnexions, le heartbeat vieillit et Kubernetes redémarre les workers (sans gravité, mais bruyant). | Mesurer ; au besoin, élargir le seuil ou lier le heartbeat à l'état de connexion. |
| Réplication Redis **asynchrone** | Lors d'un failover *non planifié*, les dernières écritures acquittées par l'ancien master peuvent être perdues (résultats de tâches, checkpoint, clé de suivi). Les bascules planifiées sont protégées par le `CLIENT PAUSE`. | `WAIT` sur les écritures critiques, ou stocker l'état métier ailleurs (base de données). |
| `/readyz` du gateway dépend du broker et de Redis | Une panne partagée rend **tous** les pods du gateway non prêts en même temps (après 3 échecs à 10 s d'intervalle). | Readiness limitée à l'état local ; la dégradation passe par les `503` applicatifs. |
| Pas de collecte de métriques ni d'autoscaling | Le plugin `rabbitmq_prometheus` expose `:15692` mais rien ne le lit. | Prometheus/Grafana (dashboards officiels RabbitMQ) ; **KEDA** pour scaler les workers sur la profondeur des files. |
| Images sans registry ni CI | Tag `dev` mutable, `IfNotPresent` : la prise en compte d'un rebuild repose sur l'annotation `checksum/image` posée par `deploy-apps.ps1` (ADR-16), et `helm rollback` ne restaure pas l'ancien code. Ne fonctionne que si le cluster partage le démon docker. | CI qui teste, construit, signe et pousse des tags immuables dans un registry (`image.registry`, `image.tag`). |
| Une installation par namespace | Noms d'objets fixes (contrat) : deux releases `orderflow` dans le même namespace entreraient en conflit. | Un namespace par environnement ; préfixer les noms seulement si le besoin de plusieurs instances par namespace apparaît. |
| Pas de dead-letter pour les tâches | Une tâche qui épuise ses retries finit en `FAILURE` (et déclenche l'errback) ; un message qui fait crasher le worker est redélivré jusqu'à la *delivery limit* des quorum queues puis abandonné. | Politique de dead-letter sur les files de tâches et alerte. |
| Pas de clé d'idempotence côté client sur `POST /orders` | Un client qui réessaie après un timeout ou une `503` ambiguë (publication confirmée par le broker mais réponse perdue) crée une seconde commande. | En-tête `Idempotency-Key` mémorisé dans Redis (`SET NX`) et réutilisé comme base de l'`order_id`. |
| Scheduler mono-réplica | Pas de digest pendant un redémarrage du beat, pas de rattrapage (ADR-15). | Acceptable pour un instantané ; sinon verrou distribué (RedBeat) avec plusieurs beats en attente. |
| Un seul réplica par worker et subscriber | Pas de traitement pendant le redémarrage d'un worker (les messages attendent dans la file). | Plusieurs réplicas de workers (sans contrainte : files partagées) ; les subscribers restent à 1 par conception (ADR-10). |
