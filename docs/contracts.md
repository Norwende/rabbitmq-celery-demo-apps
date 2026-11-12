# Contrats inter-services — OrderFlow

Ce document est la **seule chose partagée** entre les services. Aucun service n'importe le code
d'un autre : ils ne se connaissent que par des **noms** (tâches, files, exchange, stream) et des
**payloads JSON**. Modifier ce fichier = modifier une API publique.

## 1. Cas d'usage

OrderFlow traite des commandes e-commerce :

1. le **gateway** (API HTTP) reçoit une commande et émet une *chain* Celery ;
2. `worker-payments` autorise le paiement ;
3. `worker-inventory` réserve le stock ;
4. `worker-notifier` publie l'événement métier dans un **RabbitMQ Stream** ;
5. plusieurs **subscribers** indépendants (analytics, audit, live) lisent *tous* chaque événement,
   chacun à son rythme et depuis son propre offset (broadcast + replay) ;
6. le **scheduler** (Celery beat) envoie chaque minute, par nom, `notifications.sales_digest` :
   `worker-notifier` lit les agrégats d'analytics et publie un **digest** dans le même stream.

## 2. Infrastructure et namespaces

L'infrastructure et les applications vivent dans deux namespaces (noms par défaut, modifiables
par les paramètres `-InfraNamespace` / `-Namespace` des scripts et la value `infraNamespace`
du chart `orderflow`) :

| Namespace | Releases Helm | Contenu |
|---|---|---|
| `orderflow-infra` | `rabbitmq` (`charts/rabbitmq`), `redis` (`charts/redis`) | broker, result backend, leurs données et leurs Secrets |
| `orderflow` | `orderflow` (`charts/orderflow`) | les 8 Deployments applicatifs, leur configuration et leurs identifiants |

| Ressource | Namespace | Nom | Détail | Créé par |
|---|---|---|---|---|
| RabbitMQ (3 nœuds) | `orderflow-infra` | `StatefulSet/rabbitmq-server` (variante par défaut : via `RabbitmqCluster/rabbitmq`) | Service AMQP `rabbitmq.orderflow-infra.svc.cluster.local:5672`, management `:15672` ; mêmes noms dans les deux variantes | release `rabbitmq` : chart `rabbitmq` (Services et StatefulSet créés par l'opérateur) ou `rabbitmq-standalone` (sans opérateur, tout créé par Helm) |
| Administrateur RabbitMQ | `orderflow-infra` | `Secret/rabbitmq-default-user` | clés `username`, `password` ; réservé à l'exploitation, **jamais utilisé par les applications** | opérateur, ou chart `rabbitmq-standalone` (généré puis conservé) |
| Utilisateur RabbitMQ applicatif | (dans RabbitMQ) | utilisateur `orderflow` | aucun tag (ni `administrator` ni `management`), permissions `configure` / `write` / `read` `.*` sur le vhost `/` | `scripts/deploy-apps.ps1` (`rabbitmqctl`) |
| Redis (3 pods : redis + sentinel) | `orderflow-infra` | `StatefulSet/redis` | Service headless `redis-headless` ; pods `redis-N.redis-headless.orderflow-infra.svc.cluster.local` | release `redis` |
| Sentinel | `orderflow-infra` | `Service/redis-sentinel:26379` | `redis-sentinel.orderflow-infra.svc.cluster.local` ; master name : `orderflow` | release `redis` |
| Mot de passe Redis (+ Sentinel) | `orderflow-infra` | `Secret/redis-auth` | clé `password` | release `redis` (généré puis conservé, ou `auth.existingSecret`) |
| Identifiants RabbitMQ des applications | `orderflow` | `Secret/rabbitmq-credentials` | clés `username` (`orderflow`), `password` (aléatoire, généré une fois) | `scripts/deploy-apps.ps1` |
| Mot de passe Redis des applications | `orderflow` | `Secret/redis-credentials` | clé `password` : copie de `orderflow-infra/redis-auth`, resynchronisée à chaque déploiement | `scripts/deploy-apps.ps1` |
| Configuration applicative | `orderflow` | `ConfigMap/orderflow-config` | variables ci-dessous | release `orderflow` (values `infraNamespace`, `clusterDomain`, `rabbitmq.*`, `redis.*`, `logLevel`) |

Ces noms sont fixes dans les charts (pas de préfixe de release) : ils font partie du contrat.
Un Secret ne peut être référencé que par un pod de son namespace : c'est pourquoi les
applications ne lisent ni `rabbitmq-default-user` ni `redis-auth`, mais les deux Secrets de
leur namespace (noms configurables : values `rabbitmq.secretName` et `redis.secretName`).
Le chart `orderflow` refuse de s'installer s'ils manquent (cf. [ADR-17](architecture.md#adr-17--identifiants-des-applications-utilisateur-rabbitmq-dédié-et-secrets-provisionnés-par-script)).

### Variables d'environnement communes à toutes les applications

| Variable | Exemple | Source |
|---|---|---|
| `RABBITMQ_HOST` | `rabbitmq.orderflow-infra.svc.cluster.local` | ConfigMap (value `rabbitmq.host`, vide = dérivé de `infraNamespace` et `clusterDomain`) |
| `RABBITMQ_PORT` | `5672` | ConfigMap |
| `RABBITMQ_VHOST` | `/` | ConfigMap |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | `orderflow` / — | Secret `rabbitmq-credentials` (namespace des applications) |
| `REDIS_SENTINELS` | `redis-sentinel.orderflow-infra.svc.cluster.local:26379` (liste `hôte:port` séparée par des virgules acceptée) | ConfigMap (value `redis.sentinels`, vide = dérivé) |
| `REDIS_MASTER_NAME` | `orderflow` | ConfigMap |
| `REDIS_PASSWORD` | — | Secret `redis-credentials` (namespace des applications) |
| `REDIS_DB` | `0` | ConfigMap |
| `LOG_LEVEL` | `INFO` | ConfigMap |

Chaque application construit elle-même ses URLs (`amqp://…`, `sentinel://…`) à partir de ces variables.
Le scheduler ne parle qu'au broker : il ne lit que `RABBITMQ_*` et `LOG_LEVEL`. Les noms des
variables n'ont pas changé avec la séparation des namespaces : seules leurs valeurs (noms DNS
complets, Secrets locaux) diffèrent, et `dev/docker-compose.yml` reste valable tel quel.

Les noms DNS sont **complets** (`<service>.<namespace>.svc.<domaine>`) : un nom court
(`rabbitmq`) ne se résout que dans le namespace du Service. `REDIS_SENTINELS` désigne le
Service `redis-sentinel`, qui ne route que vers les pods Redis prêts : la découverte du master
ne tente jamais un pod supprimé (mesure : cf. [exploitation.md, Test de chaos](exploitation.md#test-de-chaos)).
Sentinel répond par le FQDN du pod master (`redis-N.redis-headless.orderflow-infra.svc.cluster.local`,
construit par le pod à partir de son namespace réel), résolvable depuis n'importe quel namespace.

### Variables optionnelles propres à un service

| Variable | Service | Défaut | Rôle |
|---|---|---|---|
| `WORKER_CONCURRENCY` | workers Celery | `2` | nombre de processus d'exécution du worker |
| `ORDER_TTL_SECONDS` | gateway | `86400` | durée de vie de `order:<order_id>` et des résultats Celery émis par le gateway |
| `LOG_FORMAT` | gateway | `json` | `json` (une ligne JSON par événement, structlog) ou `console` (lisible) ; value `gateway.logFormat` du chart. Les autres services gardent leur format texte |
| `SUBSCRIBER_NAME`, `SUBSCRIBER_ROLE`, `STREAM_OFFSET`, `CHECKPOINT_ENABLED`, `PREFETCH_COUNT` | subscriber | cf. §4 | |
| `DIGEST_CRON` | scheduler | `* * * * *` | crontab de `notifications.sales_digest` (`minute heure jour-du-mois mois jour-de-semaine`, **UTC**) |
| `DIGEST_EXPIRES_SECONDS` | scheduler | `50` | `expires` des ticks envoyés (1 à 59, cf. §3) |

## 3. Tâches Celery

Sérialisation **JSON uniquement**. Routage par convention de nommage :
le préfixe du nom de tâche est le nom de la file (`payments.*` → file `payments`).

### Déclaration des files (identique dans tous les services)

Chaque file `X` (`payments`, `inventory`, `notifications`) est :

- de type **quorum** (`x-queue-type: quorum`), **durable** ;
- liée à un exchange **`topic`** nommé lui aussi `X`, avec la clé de routage `X`.

Tout service qui publie vers `X` (y compris un worker qui publie le maillon suivant d'une chain
ou l'errback) doit la déclarer à l'identique : RabbitMQ refuse une redéclaration divergente
(`PRECONDITION_FAILED`). Côté Celery, cela correspond à :

```python
task_default_queue_type="quorum",
task_default_exchange_type="topic",
task_create_missing_queue_type="quorum",
task_create_missing_queue_exchange_type="topic",
```

Celery crée en plus, pour la *native delayed delivery* (comptes à rebours des retries sur files
quorum) : l'exchange `celery_delayed_delivery`, les exchanges `topic` et files quorum
`celery_delayed_0` … `celery_delayed_27`, et des bindings `#.X` vers chaque file `X`
(depuis `celery_delayed_delivery` et depuis l'exchange `X`). Ces objets appartiennent à Celery :
ne pas les créer ni les modifier à la main.

| Nom de tâche | File | Worker | Signature | Retour |
|---|---|---|---|---|
| `payments.authorize` | `payments` | worker-payments | `(order: Order)` | `Order` enrichi de `payment` |
| `inventory.reserve` | `inventory` | worker-inventory | `(order: Order)` | `Order` enrichi de `reservation` |
| `notifications.order_confirmed` | `notifications` | worker-notifier | `(order: Order)` | `{order_id, event_id, event_type}` |
| `notifications.order_failed` | `notifications` | worker-notifier | `(failed_task_id: str, *, order: Order)` | `{order_id, event_id, event_type}` |
| `notifications.sales_digest` | `notifications` | worker-notifier | `(period: str \| None = None)` | aucun (`ignore_result`) |

Chaîne nominale émise par le gateway (uniquement par nom, via `celery.signature`) :

```
payments.authorize(order) → inventory.reserve(order') → notifications.order_confirmed(order'')
                  └── en cas d'échec d'un maillon (link_error) → notifications.order_failed(task_id, order=order)
```

Errback : il est attaché à chaque maillon. C'est le **worker du maillon en échec** qui l'envoie,
**par nom** (la tâche n'y est pas enregistrée), avec pour unique argument positionnel l'id de la
tâche en échec (`parent_id` du message = cet id) et `order` en kwarg. `notifications.order_failed`
lit la cause (`reason`) dans le **result backend Redis**, où Celery a enregistré l'exception
avant d'appeler les errbacks.

### Tâche planifiée `notifications.sales_digest`

Envoyée **par nom** par le scheduler (Celery beat, aucune tâche enregistrée chez lui), selon
`DIGEST_CRON`, **sans argument**, avec `expires = DIGEST_EXPIRES_SECONDS` (Celery le traduit en
TTL du message AMQP et en en-tête vérifié par le worker : un tick non exécuté à temps est
abandonné, jamais rattrapé en rafale).

- `period` : identifiant du tick, la minute UTC au format `YYYY-MM-DDTHH:MMZ`
  (ex. `2026-11-11T21:05Z`). Absent (cas du beat), le worker le dérive de l'heure courante
  tronquée à la minute ; `expires` < 60 s garantit qu'un tick dupliqué ou relivré s'exécute dans
  la minute de son envoi, donc avec la même `period`. Il peut être passé explicitement pour
  réémettre le digest d'une minute donnée (même `event_id`).
- Effet : lit le hash `analytics:orders` (§4, lecture seule) et publie un `DigestEvent` dans le
  stream. Un retry (erreur d'infrastructure, 3 au plus) repart avec la `period` de la première
  tentative.
- Aucun résultat stocké (`ignore_result`) : la tâche est un émetteur d'événement.

### Payload `Order`

```json
{
  "order_id": "ord_01HXYZ…",
  "customer_id": "cust_42",
  "currency": "EUR",
  "items": [{"sku": "SKU-BOOK-001", "quantity": 2, "unit_price": 19.90}],
  "total_amount": 39.80,
  "created_at": "2026-11-11T21:00:00Z"
}
```

Enrichissements :

- `payment`: `{"authorization_id": "auth_…", "status": "authorized", "authorized_at": "…"}`
- `reservation`: `{"reservation_id": "res_…", "status": "reserved", "reserved_at": "…"}`

### Règles métier simulées (déterministes, pour la démo)

| Condition | Effet |
|---|---|
| `total_amount > 1000` | `payments.authorize` lève `PaymentDeclined` (pas de retry) |
| un item avec `sku == "OUT-OF-STOCK"` | `inventory.reserve` lève `OutOfStock` (pas de retry) |
| erreur d'infrastructure (connexion…) | retry automatique avec backoff |

Les tâches sont **idempotentes** vis-à-vis de `order_id` (livraison *at-least-once* : `acks_late`).

## 4. Stream d'événements

| Élément | Valeur |
|---|---|
| Exchange | `orders` (type `topic`, durable) |
| Stream | `orders.events` (`x-queue-type: stream`, `x-max-age: 7D`) |
| Binding | `orders` → `orders.events` avec la clé `order.#` |
| Routing keys | `order.confirmed`, `order.failed`, `order.digest` (= `event_type`) |
| Éditeur unique | `worker-notifier` (via kombu, sur la connexion broker du worker Celery) |

Le stream doit être déclaré par **tous** (l'éditeur et chaque subscriber le redéclarent) avec
exactement les arguments `{"x-queue-type": "stream", "x-max-age": "7D"}`, durable, lié à
`orders` par `order.#`. Toute autre combinaison d'arguments est refusée (`PRECONDITION_FAILED`).

### Payload `OrderEvent` (JSON, `content_type: application/json`)

```json
{
  "event_id": "evt_…",
  "event_type": "order.confirmed",
  "occurred_at": "2026-11-11T21:00:02Z",
  "order_id": "ord_…",
  "customer_id": "cust_42",
  "total_amount": 39.80,
  "currency": "EUR",
  "reason": null
}
```

`reason` est renseigné pour `order.failed` (ex. `"PaymentDeclined: amount exceeds limit"`).
`event_id` est déterministe (dérivé de `order_id` + `event_type`) afin que les subscribers puissent dédupliquer.

### Payload `DigestEvent` (`event_type: "order.digest"`)

```json
{
  "event_id": "evt_…",
  "event_type": "order.digest",
  "occurred_at": "2026-11-11T21:05:00Z",
  "period": "2026-11-11T21:05Z",
  "confirmed": 61,
  "failed": 2,
  "revenue": {"EUR": 639.80}
}
```

Instantané des compteurs **cumulés** d'`analytics:orders` au moment du tick (pas une fenêtre
de temps). `revenue` : un montant arrondi à 2 décimales par devise présente. `event_id` est
dérivé de `period` + `event_type` (`evt_` + sha256(`period:order.digest`)[:24]) : un tick
relivré publie le même id.

### Types d'événement et tolérance

Le champ `event_type` sélectionne le schéma. Un consommateur **doit** accepter un type qu'il ne
connaît pas (l'acquitter, au plus le journaliser) : de nouveaux types peuvent être ajoutés au
stream sans coordination. Seuls `order.confirmed` et `order.failed` portent `order_id`,
`customer_id`, `total_amount`, `currency` et `reason`.

### Subscribers

Une seule image `subscriber`, déployée trois fois avec une configuration différente :

| Déploiement | `SUBSCRIBER_NAME` | `STREAM_OFFSET` initial | Checkpoint Redis | Rôle |
|---|---|---|---|---|
| `subscriber-analytics` | `analytics` | `first` | oui | agrège CA et compteurs (événements de commande seulement ; digests et types inconnus ignorés) |
| `subscriber-audit` | `audit` | `first` | oui | journalise chaque événement en JSON, **quel que soit son type** |
| `subscriber-live` | `live` | `next` | non | temps réel uniquement, pas de replay ; une ligne lisible par commande et par digest, types inconnus en DEBUG |

Checkpoint : clé Redis `stream:orders.events:offset:<SUBSCRIBER_NAME>` = dernier offset traité.
Au redémarrage, le subscriber reprend à `offset + 1` (sinon à `STREAM_OFFSET`).

Variables du subscriber :

| Variable | Défaut | Rôle |
|---|---|---|
| `SUBSCRIBER_NAME` | `audit` | identité : nom de la clé de checkpoint, préfixe des logs |
| `SUBSCRIBER_ROLE` | = `SUBSCRIBER_NAME` | traitement appliqué (`analytics`, `audit`, `live`) ; permet par ex. un `audit-replay` qui se comporte comme `audit` avec son propre checkpoint |
| `STREAM_OFFSET` | `next` | `first`, `last`, `next` ou un offset entier ≥ 0 ; utilisé seulement sans checkpoint |
| `CHECKPOINT_ENABLED` | `false` | `true`/`1`/`yes` active le checkpoint Redis |
| `PREFETCH_COUNT` | `50` | prefetch consommateur (obligatoire pour un stream en AMQP 0.9.1) |

Agrégats d'`analytics` (Redis) :

- hash `analytics:orders` : champs `confirmed`, `failed` (entiers), `revenue:<CUR>` (ex.
  `revenue:EUR`, flottant en chaîne) ;
- set `analytics:orders:seen` : `event_id` déjà comptés (événements de commande uniquement).

**Contrat de lecture.** `analytics:orders` appartient à `subscriber-analytics`, seul à
l'écrire. `worker-notifier` le lit (`HGETALL`, jamais d'écriture) pour
`notifications.sales_digest`. Les noms de la clé et des champs ci-dessus font donc partie du
contrat : les renommer casse le digest. Un champ absent vaut 0.

La déduplication sur `event_id` et la mise à jour des compteurs se font dans **un seul script
Lua** (atomique) : un événement est compté une fois, même s'il est présent deux fois dans le
stream ou relu après un crash entre la mise à jour et le checkpoint. `audit` et `live` ne
dédupliquent pas (un doublon est journalisé/affiché deux fois, avec son `stream_offset`).

## 5. API HTTP du gateway (port 8000)

| Méthode | Chemin | Description |
|---|---|---|
| `POST` | `/orders` | corps : `{customer_id, currency?, items[]}` → `202 {order_id, status_url}` |
| `GET` | `/orders/{order_id}` | état de la chaîne : `{order_id, status, steps[], result|error}` |
| `GET` | `/healthz` | liveness |
| `GET` | `/readyz` | readiness (broker + Redis joignables) |

Le gateway stocke `order:<order_id>` → arbre de résultats de la chain (`AsyncResult.as_tuple()`) dans Redis
pour pouvoir restituer l'état de chaque maillon.

Réponse de `GET /orders/{order_id}` :

- `status` ∈ `PENDING | PROCESSING | SUCCESS | FAILURE` ;
- `steps[]` : un élément `{task, task_id, state}` par maillon, dans l'ordre de la chain
  (`state` = état Celery : `PENDING`, `STARTED`, `RETRY`, `SUCCESS`, `FAILURE`…) ;
- en cas d'échec, Celery marque aussi `FAILURE` les maillons **suivant** le maillon en échec ;
  `error` (`"<ExceptionType>: <message>"`) provient du **premier** maillon en `FAILURE` ;
- `result` : retour du dernier maillon quand `status = SUCCESS`.

Indisponibilité de l'infrastructure (broker ou Redis injoignable) : `503` avec un en-tête
`Retry-After`, jamais une `500`.

Corrélation : chaque réponse porte un en-tête `X-Request-ID`, celui de la requête s'il est
fourni et valide (1 à 128 caractères parmi `A-Z a-z 0-9 . _ : -`), sinon un identifiant
généré ; il figure dans toutes les lignes de journal du gateway liées à la requête
(champ `request_id`).
