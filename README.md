# OrderFlow

Projet de démonstration d'une architecture de traitement asynchrone sur Kubernetes,
volontairement simple mais construite comme en production :

- un **cluster RabbitMQ** de 3 nœuds (Cluster Operator, ou sans opérateur pour les environnements
  restreints) avec des **quorum queues** pour les tâches ;
- un **Redis Sentinel** à 3 pods (master + 2 réplicas, failover automatique) comme result backend ;
- des **workers Celery totalement indépendants**, qui ne s'appellent que par **nom de tâche**
  (aucun import entre services) ;
- un **RabbitMQ Stream** qui diffuse chaque événement métier à plusieurs abonnés, publié depuis
  un worker Celery avec kombu ;
- un **Celery beat** dédié (un seul réplica) qui déclenche des tâches planifiées, lui aussi
  uniquement par nom.

Cas d'usage : une commande e-commerce traverse paiement → stock → notification ; l'événement
final (`order.confirmed` ou `order.failed`) est lu par trois abonnés (analytics, audit, live),
chacun à son rythme et depuis son propre offset. Chaque minute, le beat demande au notifier un
**digest des ventes** (`order.digest`), publié dans le même stream.

## Architecture

```mermaid
flowchart LR
    client([Client HTTP]) -->|POST /orders| gw

    subgraph nsapp[namespace orderflow — applications, release orderflow]
        gw[gateway<br/>FastAPI]
        wp[worker-payments<br/>payments.authorize]
        wi[worker-inventory<br/>inventory.reserve]
        wn[worker-notifier<br/>notifications.*]
        sch[scheduler<br/>celery beat ×1]
        sa[subscriber-analytics]
        sau[subscriber-audit]
        sl[subscriber-live]
        sec[/Secrets rabbitmq-credentials<br/>et redis-credentials/]
    end

    subgraph nsinfra[namespace orderflow-infra — infrastructure]
        subgraph rmq[RabbitMQ — 3 nœuds, release rabbitmq]
            qp[[payments<br/>quorum]]
            qi[[inventory<br/>quorum]]
            qn[[notifications<br/>quorum]]
            ex{{orders<br/>exchange topic}}
            st[(orders.events<br/>stream, 7 jours)]
        end
        subgraph redis[Redis Sentinel — 3 pods, release redis]
            rd[(master + 2 réplicas<br/>+ 3 sentinels)]
        end
    end

    gw -->|chain par nom| qp
    qp --> wp
    wp -->|maillon suivant| qi
    qi --> wi
    wi -->|maillon suivant| qn
    sch -->|"notifications.sales_digest<br/>par nom, chaque minute"| qn
    qn --> wn
    wn -->|kombu publish<br/>order.confirmed / failed / digest| ex
    ex -->|"binding order.#"| st
    st -->|offset propre| sa
    st -->|offset propre| sau
    st -->|offset propre| sl

    gw -. suivi order:id / état .-> rd
    wp -. résultats .-> rd
    wi -. résultats .-> rd
    wn -. résultats + lecture agrégats .-> rd
    sa -. agrégats + checkpoint .-> rd
    sau -. checkpoint .-> rd
```

Les applications joignent l'infrastructure par des noms DNS complets
(`rabbitmq.orderflow-infra.svc.cluster.local:5672`,
`redis-sentinel.orderflow-infra.svc.cluster.local:26379`) et s'authentifient avec deux Secrets
de **leur** namespace (`rabbitmq-credentials` : utilisateur RabbitMQ dédié `orderflow`, sans
droit d'administration ; `redis-credentials` : copie du mot de passe Redis), provisionnés par
`scripts/deploy-apps.ps1` : un Secret ne peut pas être référencé d'un namespace à l'autre.

## Les 6 idées clés

1. **Contrat par noms, pas par code.** Les services ne partagent que [`docs/contracts.md`](docs/contracts.md) :
   noms de tâches, de files, d'exchange, de stream et payloads JSON. Le gateway construit la chain
   avec `celery.signature("payments.authorize", …)` sans connaître le code des workers.
2. **Routage par préfixe.** `payments.authorize` va dans la file `payments`. Chaque worker publie
   lui-même le maillon suivant (ou l'errback) et déclare la file cible exactement comme son
   propriétaire (quorum + exchange topic), sinon RabbitMQ répond `PRECONDITION_FAILED`.
3. **Quorum queues + at-least-once.** Publisher confirms, `acks_late`, tâches idempotentes ;
   les retries utilisent la *native delayed delivery* de Celery (pas d'ETA gardée en mémoire).
4. **Stream pour la diffusion.** Un log non destructif lu par N abonnés, chacun avec son offset
   (`first`, `next`, entier), rejouable pendant 7 jours. Offsets sauvegardés côté client dans
   Redis ; `event_id` déterministe pour dédupliquer.
5. **Infra stateful maîtrisée.** RabbitMQ par l'opérateur officiel (ou, sans droits d'administrateur du
   cluster, par un StatefulSet sans opérateur ni RBAC) ; Redis Sentinel en StatefulSet
   écrit à la main (bootstrap, failover propre au `preStop`, noms DNS stables), PDB, probes
   fichier, conteneurs non-root en lecture seule. Infrastructure et applications dans deux
   namespaces, un chart Helm par composant ; les applications utilisent un utilisateur
   RabbitMQ dédié, pas l'administrateur de l'opérateur.
6. **Planification par nom.** Un service `scheduler` exécute `celery beat` seul (aucune tâche,
   1 réplica, stratégie `Recreate`) et n'envoie que des noms de tâches ; `expires` empêche les
   ticks manqués de s'accumuler, un pod recréé ne rattrape rien.

## Prérequis

| Outil | Remarque |
|---|---|
| Kubernetes ≥ 1.29 | Rancher Desktop (k3s, moteur **dockerd/moby**) testé. Les volumes utilisent la StorageClass **par défaut** du cluster (`local-path` sur k3s) ; une autre classe se choisit par la value `persistence.storageClassName` des charts `rabbitmq` et `redis`. |
| cert-manager | **Obligatoire**, à installer avant : les webhooks d'admission du RabbitMQ Cluster Operator v2.23.2 utilisent un certificat émis par cert-manager (testé avec v1.21.3). Inutile avec `-NoOperator` (cf. ci-dessous). |
| `helm` | v4 (testé avec v4.2.3) : les trois charts `charts/rabbitmq`, `charts/redis` et `charts/orderflow` (`charts/rabbitmq-standalone` à la place de `charts/rabbitmq` sans opérateur). |
| `kubectl` | avec le contexte du cluster cible (`rancher-desktop` ici). |
| `docker` | construit les 6 images `orderflow/*:dev`. |
| PowerShell | scripts `scripts/*.ps1` (Windows PowerShell 5.1 ou PowerShell 7). |
| `uv` (optionnel) | tests unitaires et développement local. |

Les images sont construites localement et référencées avec `imagePullPolicy: IfNotPresent` :
avec Rancher Desktop (dockerd), le k3s voit directement les images du démon docker. Sur tout
autre cluster, il faut pousser les images dans un registry
(`./scripts/build-images.ps1 -Registry <registry> -Tag <tag> -Push`) et renseigner la value
`image.registry` (et `image.tag`) du chart `orderflow`. Les trois charts ont `image.registry`
et `imagePullSecrets`, et les Dockerfiles prennent images de base et index PyPI en
arguments : tout peut venir d'un registre et d'un miroir internes.

## Démarrage rapide

```powershell
kubectl config use-context rancher-desktop   # cert-manager déjà installé (cf. Prérequis)

./scripts/deploy.ps1          # opérateur, releases rabbitmq et redis (orderflow-infra), identifiants, build, release orderflow
helm test orderflow -n orderflow   # GET /readyz du gateway depuis le cluster
./scripts/demo.ps1            # 3 scénarios (valide, paiement refusé, rupture de stock) + dernier digest
./scripts/replay-demo.ps1     # un abonné tout neuf relit le stream depuis l'offset 0, puis reprise sur checkpoint
./scripts/chaos.ps1           # trafic pendant la perte du master Redis puis de rabbitmq-server-0 (code retour 1 si échec)
./scripts/destroy.ps1         # suppression après confirmation (-Force, -KeepInfra, -IncludeOperator)
```

Sans droits d'administrateur du cluster ni accès à Internet (pas d'opérateur, pas de CRD, pas de
cert-manager, images dans un registre interne) : `./scripts/deploy.ps1 -NoOperator` installe
RabbitMQ par `charts/rabbitmq-standalone` (StatefulSet, Services, ConfigMap, Secrets, PDB : mêmes
noms d'objets, rien d'autre ne change). Les scripts détectent ensuite la variante installée.
Registre et miroirs internes, construction des images, changement de variante :
[exploitation.md, « Environnement restreint »](docs/exploitation.md#environnement-restreint--sans-opérateur-registre-interne).

| Release | Chart | Namespace |
|---|---|---|
| `rabbitmq` | `charts/rabbitmq` (ou `charts/rabbitmq-standalone` avec `-NoOperator`) | `orderflow-infra` |
| `redis` | `charts/redis` | `orderflow-infra` |
| `orderflow` | `charts/orderflow` | `orderflow` |

`deploy.ps1` = `deploy-infra.ps1` (opérateur RabbitMQ par son manifeste épinglé — sauf
`-NoOperator` —, puis `helm upgrade --install rabbitmq` et `redis`, indépendantes :
`-Only rabbitmq|redis`, et vérification que chaque nœud RabbitMQ voit les autres) puis
`deploy-apps.ps1` (utilisateur RabbitMQ `orderflow` et Secrets d'identifiants dans le namespace
`orderflow`, `build-images.ps1` sauf `-SkipBuild`, puis `helm upgrade --install orderflow`) ;
chaque script est utilisable séparément, accepte `-Namespace` / `-InfraNamespace`, et est
relançable sans recréer de pod ni changer de mot de passe si rien n'a changé. Sans les
scripts (création des identifiants détaillée dans
[exploitation.md](docs/exploitation.md#identifiants-des-applications)) :

```powershell
kubectl apply --server-side -k k8s/operators/rabbitmq-cluster-operator
helm upgrade --install rabbitmq charts/rabbitmq -n orderflow-infra --create-namespace --wait
helm upgrade --install redis charts/redis -n orderflow-infra --create-namespace --wait
kubectl -n orderflow-infra wait --for=condition=AllReplicasReady rabbitmqcluster/rabbitmq --timeout=600s
# utilisateur RabbitMQ + Secrets rabbitmq-credentials / redis-credentials dans orderflow (cf. lien ci-dessus)
helm upgrade --install orderflow charts/orderflow -n orderflow --create-namespace --wait
```

Trois releases parce que trois cycles de vie (le broker, le result backend et les
applications ne changent ni au même rythme ni avec les mêmes précautions), deux namespaces
pour séparer l'infrastructure du code applicatif (cf.
[ADR-16](docs/architecture.md#adr-16--trois-charts-helm-infrastructure-et-applications-dans-deux-namespaces)
et [ADR-17](docs/architecture.md#adr-17--identifiants-des-applications-utilisateur-rabbitmq-dédié-et-secrets-provisionnés-par-script)).
Les scripts partagent leurs fonctions dans `scripts/lib/common.ps1`.
Détails, vérifications et dépannage : [docs/exploitation.md](docs/exploitation.md).

## Organisation du dépôt

```
docs/
  contracts.md          contrat inter-services (seul artefact partagé)
  architecture.md       composants, décisions de conception, limites
  cycle-de-vie.md       vie d'une commande, garanties de livraison, pannes
  exploitation.md       runbook : déploiement, vérifications, démo, chaos, dépannage
services/
  gateway/              API FastAPI, émet la chain Celery par noms
  worker-payments/      payments.authorize       (file payments)
  worker-inventory/     inventory.reserve        (file inventory)
  worker-notifier/      notifications.*          (file notifications) -> publie dans le stream
  subscriber/           consommateur kombu du stream (analytics / audit / live)
  scheduler/            celery beat seul : envoie notifications.sales_digest par nom (DIGEST_CRON)
charts/
  rabbitmq/             release rabbitmq (ns orderflow-infra) : RabbitmqCluster + PDB
  rabbitmq-standalone/  variante sans opérateur de la release rabbitmq : StatefulSet rabbitmq-server, Services,
                        ConfigMap, Secrets (admin, cookie), PDB ; peer discovery classic_config, aucun RBAC
  redis/                release redis (ns orderflow-infra) : Redis Sentinel (StatefulSet, scripts
                        dans files/, Services, PDB), Secret/redis-auth
  orderflow/            release orderflow (ns orderflow) : ConfigMap orderflow-config, gateway +
                        Service, workers, scheduler, subscribers, test helm (GET /readyz)
k8s/
  operators/            RabbitMQ Cluster Operator (manifeste de release épinglé, hors Helm)
scripts/                déploiement, démo, replay, chaos, destruction (PowerShell)
  lib/common.ps1        fonctions partagées (kubectl, helm, Secrets, port-forward, HTTP, Redis, RabbitMQ)
dev/docker-compose.yml  environnement local sans Kubernetes
```

Chaque service est un projet `uv` autonome (son `pyproject.toml`, son `uv.lock`, son `Dockerfile`).

## Documentation

- [Contrat inter-services](docs/contracts.md)
- [Architecture et décisions](docs/architecture.md)
- [Cycle de vie d'une commande](docs/cycle-de-vie.md)
- [Exploitation (runbook)](docs/exploitation.md)

## Versions

Helm v4 (charts `apiVersion: v2`, version 0.1.0) · RabbitMQ Cluster Operator v2.23.2 · `rabbitmq:4.3.7-management` · `redis:8.10.3-alpine` ·
Python 3.13 · Celery 5.7.0 · kombu 5.7.0 · redis-py 8.1.0 · FastAPI 0.162.4 · structlog 26.1.0 (gateway) · uv 0.10.8.
