<#
.SYNOPSIS
  Déploie (de façon idempotente) l'infrastructure OrderFlow dans le namespace
  d'infrastructure : RabbitMQ (avec le RabbitMQ Cluster Operator, ou sans opérateur avec
  -NoOperator), puis Redis Sentinel. Deux releases Helm indépendantes : rabbitmq et redis.

.DESCRIPTION
  RabbitMQ, variante par défaut (opérateur) :
    1. kubectl apply --server-side -k k8s/operators/rabbitmq-cluster-operator (manifeste
       upstream téléchargé depuis GitHub), puis attend le CRD, le certificat du webhook (et
       son caBundle) et le Deployment de l'opérateur ;
    2. helm upgrade --install rabbitmq charts/rabbitmq (RabbitmqCluster + PDB).
  RabbitMQ, variante -NoOperator (environnement restreint) :
    helm upgrade --install rabbitmq charts/rabbitmq-standalone (StatefulSet, Services,
    ConfigMap, Secrets, PDB : aucun CRD, aucun RBAC, rien n'est téléchargé par le script ;
    l'image vient du registre configuré dans les values, cf. -RabbitmqSet image.registry=…).
  Dans les deux cas, attend ensuite le StatefulSet rabbitmq-server et un cluster complet
  (rabbitmqctl cluster_status sur chaque nœud : tous les nœuds en marche, vus par chacun).

  Variante : sans -NoOperator, une installation existante garde sa variante (détectée
  d'après la release) ; sur un namespace vide, c'est l'opérateur. Changer de variante sur
  une installation existante est refusé avec un message explicite : désinstaller d'abord
  (scripts/destroy.ps1 -RabbitmqOnly), les données et la propriété des objets diffèrent.

  Redis : helm upgrade --install redis charts/redis, puis attend l'état nominal de Sentinel
  (1 master, réplicas sains, un sentinel par pod).

  -Only rabbitmq ou -Only redis n'installe, ne met à jour et n'attend que celle-là.
  Relançable à volonté : un upgrade sans changement ne redémarre aucun pod ; les secrets
  générés au premier install (mot de passe Redis, cookie et administrateur RabbitMQ sans
  opérateur) sont conservés (lookup des Secrets existants dans les charts).

.PARAMETER Only
  Limite le déploiement à une release : rabbitmq ou redis (défaut : les deux).
.PARAMETER NoOperator
  RabbitMQ sans opérateur (chart rabbitmq-standalone) : ni CRD, ni webhook, ni cert-manager,
  ni téléchargement depuis Internet.
.PARAMETER RabbitmqValues
  Fichiers de valeurs supplémentaires pour le chart RabbitMQ de la variante (helm -f).
.PARAMETER RabbitmqSet
  Surcharges ponctuelles du chart RabbitMQ (helm --set), ex. -RabbitmqSet 'persistence.size=2Gi'.
.PARAMETER RedisValues
  Fichiers de valeurs supplémentaires pour le chart redis (helm -f).
.PARAMETER RedisSet
  Surcharges ponctuelles du chart redis (helm --set), ex. -RedisSet 'resources.requests.cpu=60m'.
.PARAMETER InfraNamespace
  Namespace de RabbitMQ et Redis (défaut : orderflow-infra).
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/deploy-infra.ps1
  ./scripts/deploy-infra.ps1 -NoOperator -RabbitmqSet 'image.registry=registry.corp.example/mirror' -RedisSet 'image.registry=registry.corp.example/mirror'
  ./scripts/deploy-infra.ps1 -Only redis -RedisSet 'persistence.storageClassName=fast'
#>
[CmdletBinding()]
param(
    [ValidateSet('rabbitmq', 'redis')]
    [string[]]$Only = @(),
    [switch]$NoOperator,
    [string[]]$RabbitmqValues = @(),
    [string[]]$RabbitmqSet = @(),
    [string[]]$RedisValues = @(),
    [string[]]$RedisSet = @(),
    [string]$InfraNamespace = 'orderflow-infra',
    [string]$Context = '',
    [int]$TimeoutSeconds = 600
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"
$withRabbitmq = ($Only.Count -eq 0 -or $Only -contains 'rabbitmq')
$withRedis = ($Only.Count -eq 0 -or $Only -contains 'redis')

function Install-InfraRelease([string]$Release, [string]$Chart, [string[]]$ExtraArgs) {
    Write-Step "helm upgrade --install $Release charts/$Chart (namespace $InfraNamespace)"
    # --force-conflicts : le chart fait foi, même sur un champ modifié à la main (cf. deploy-apps.ps1).
    $helmArgs = @('upgrade', '--install', $Release, "$Root/charts/$Chart", '-n', $InfraNamespace,
        '--create-namespace', '--wait', '--timeout', "$($TimeoutSeconds)s", '--force-conflicts') + $ExtraArgs
    # Juste après une première installation de l'opérateur, son webhook peut encore refuser
    # la connexion quelques secondes : on réessaie uniquement dans ce cas.
    for ($i = 1; ; $i++) {
        try {
            Invoke-Helm @helmArgs | Out-Host
            return
        } catch {
            if ($i -ge 10 -or "$_" -notmatch 'webhook') { throw }
            Write-Host "Webhook de l'opérateur pas encore prêt (tentative $i/10), nouvel essai dans 6 s..." -ForegroundColor Yellow
            Start-Sleep -Seconds 6
        }
    }
}

function Install-RabbitmqOperator {
    # Pas de chart Helm officiel pour l'opérateur : manifeste de release épinglé, appliqué en
    # server-side (le CRD dépasse la taille de l'annotation last-applied-configuration).
    Write-Step 'Installation du RabbitMQ Cluster Operator (rabbitmq-system)'
    Invoke-Kubectl apply --server-side --field-manager=$FieldManager -k "$Root/k8s/operators/rabbitmq-cluster-operator" | Out-Host

    Write-Step "Attente de l'opérateur (CRD, certificat du webhook, Deployment)"
    Invoke-Kubectl wait --for=condition=Established crd/rabbitmqclusters.rabbitmq.com --timeout=120s | Out-Host
    Invoke-Kubectl -n rabbitmq-system wait --for=condition=Ready certificate/cluster-operator-serving-cert --timeout=180s | Out-Host
    Invoke-Kubectl -n rabbitmq-system rollout status deployment/rabbitmq-cluster-operator --timeout=300s | Out-Host
    # Le caBundle du webhook est injecté par le cainjector de cert-manager, parfois quelques
    # secondes après le certificat : sans lui, la création du RabbitmqCluster échoue (x509).
    $deadline = (Get-Date).AddSeconds(120)
    while (-not (Invoke-Kubectl get validatingwebhookconfiguration cluster-operator-validating-webhook-configuration -o 'jsonpath={.webhooks[0].clientConfig.caBundle}')) {
        if ((Get-Date) -gt $deadline) { throw 'caBundle du webhook de l opérateur non injecté (cert-manager cainjector ?)' }
        Start-Sleep -Seconds 3
    }
}

$variant = ''
if ($withRabbitmq) {
    # --- 1. Choix et contrôle de la variante ------------------------------------
    $installed = if (Invoke-Kubectl get namespace $InfraNamespace --ignore-not-found -o name) { Get-RabbitmqVariant } else { '' }
    $variant = if ($NoOperator) { 'standalone' } elseif ($installed) { $installed } else { 'operator' }
    $labels = @{ operator = 'avec opérateur (charts/rabbitmq)'; standalone = 'sans opérateur (charts/rabbitmq-standalone)' }
    if ($installed -and $installed -ne $variant) {
        throw ("RabbitMQ est déjà installé dans $InfraNamespace en variante $($labels[$installed]) ; " +
            "variante demandée : $($labels[$variant]). Changer de variante ne se fait pas en place (données, cookie " +
            "et propriété des objets diffèrent) : désinstaller d'abord RabbitMQ et ses PVC, ce qui EFFACE files et stream " +
            "(./scripts/destroy.ps1 -RabbitmqOnly), puis relancer ce script, puis ./scripts/deploy-apps.ps1 (recrée l'utilisateur orderflow).")
    }
    if (-not $installed) {
        $volumes = if (Invoke-Kubectl get namespace $InfraNamespace --ignore-not-found -o name) { Get-RabbitmqVolumeVariant } else { '' }
        if ($volumes -and $volumes -ne $variant) {
            throw ("PVC persistence-rabbitmq-server-* d'une installation $($labels[$volumes]) encore présents dans $InfraNamespace : " +
                "ils seraient repris par la variante $($labels[$variant]) avec des données incompatibles. Les supprimer d'abord " +
                "(./scripts/destroy.ps1 -RabbitmqOnly), ce qui efface les données RabbitMQ.")
        }
        if ($volumes) { Write-Host "PVC RabbitMQ existants (même variante) : données reprises." -ForegroundColor Yellow }
    } elseif (-not $NoOperator -and $installed -eq 'standalone') {
        Write-Host 'Variante détectée : RabbitMQ sans opérateur (release existante) ; l opérateur n est pas installé.' -ForegroundColor Yellow
    }
    Write-Step "RabbitMQ : variante $($labels[$variant])"

    # --- 2. Opérateur (variante par défaut seulement) ---------------------------
    if ($variant -eq 'operator') { Install-RabbitmqOperator }

    # --- 3. Release rabbitmq ----------------------------------------------------
    Install-InfraRelease $RabbitmqRelease $RabbitmqCharts[$variant] (Get-HelmSetArgs $RabbitmqValues $RabbitmqSet)

    Write-Step 'Attente de RabbitMQ (StatefulSet rabbitmq-server, cluster complet)'
    # Variante opérateur : le StatefulSet est créé par l'opérateur, attendre qu'il existe.
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while (-not (Invoke-Kubectl -n $InfraNamespace get statefulset rabbitmq-server --ignore-not-found -o name)) {
        if ((Get-Date) -gt $deadline) { throw 'StatefulSet/rabbitmq-server non créé par l opérateur' }
        Start-Sleep -Seconds 3
    }
    Wait-RabbitmqReady -TimeoutSeconds $TimeoutSeconds | Out-Null
}

if ($withRedis) {
    # --- 4. Release redis -------------------------------------------------------
    Install-InfraRelease 'redis' 'redis' (Get-HelmSetArgs $RedisValues $RedisSet)

    Write-Step 'Attente de Redis (StatefulSet/redis et Sentinel)'
    Invoke-Kubectl -n $InfraNamespace rollout status statefulset/redis "--timeout=$($TimeoutSeconds)s" | Out-Host
    $replicas = [int](Invoke-Kubectl -n $InfraNamespace get statefulset redis -o 'jsonpath={.spec.replicas}')
    $topology = Wait-RedisTopology -Replicas $replicas -TimeoutSeconds $TimeoutSeconds
    Write-Host ("Sentinel : master {0}, {1} réplica(s) sain(s), {2} sentinels" -f $topology.Master, $topology.Replicas, $topology.Sentinels)
}

Write-Step "Etat final (namespace $InfraNamespace)"
Invoke-Helm list -n $InfraNamespace | Out-Host
$kinds = @('statefulset', 'pod', 'svc', 'pvc', 'pdb')
if ($variant -eq 'operator') { $kinds = @('rabbitmqcluster') + $kinds }
Invoke-Kubectl -n $InfraNamespace get ($kinds -join ',') | Out-Host
Write-Host ''
Write-Host "Infrastructure OrderFlow prête (namespace $InfraNamespace)." -ForegroundColor Green
