<#
.SYNOPSIS
  Supprime OrderFlow du cluster : namespace des applications (release orderflow, Secrets
  d'identifiants), puis namespace d'infrastructure (releases rabbitmq et redis, PVC,
  Secrets). Avec -KeepInfra, seules les applications sont supprimées. Avec
  -IncludeOperator, supprime aussi le RabbitMQ Cluster Operator.

.DESCRIPTION
  1. applications : helm uninstall orderflow, restes éventuels (Deployments hors Helm),
     suppression du namespace des applications (emporte rabbitmq-credentials et
     redis-credentials) ; avec -KeepInfra, l'utilisateur RabbitMQ applicatif est aussi
     supprimé (ses identifiants viennent de disparaître ; un nouveau déploiement le recrée) ;
  2. infrastructure (sauf -KeepInfra) : helm uninstall rabbitmq et redis (le RabbitmqCluster
     est supprimé pendant que l'opérateur est encore là pour nettoyer), restes éventuels
     (objets hors Helm, Secret redis-auth conservé par helm.sh/resource-policy=keep,
     StatefulSet de l'opérateur), PVC, puis le namespace d'infrastructure (attente de sa
     disparition) ;
  3. avec -IncludeOperator : kubectl delete -k k8s/operators/rabbitmq-cluster-operator.

  Avec -RabbitmqOnly : seulement RabbitMQ (l'une ou l'autre variante) : helm uninstall
  rabbitmq, restes (RabbitmqCluster, StatefulSet), PVC et Secrets conservés
  (rabbitmq-default-user, rabbitmq-erlang-cookie), puis remise à zéro des checkpoints des
  subscribers dans Redis (le stream a disparu) et redémarrage des subscribers. Redis, les
  applications et l'opérateur restent. Préalable à un changement de variante.

  Applications d'abord : plus aucun client quand RabbitMQ et Redis s'arrêtent.
  DESTRUCTIF ET IRRÉVERSIBLE (données Redis/RabbitMQ perdues) : demande une confirmation
  sauf avec -Force. Idempotent : les ressources déjà absentes sont ignorées.
  Ne touche à aucun autre namespace (sauf rabbitmq-system avec -IncludeOperator).

.PARAMETER KeepInfra
  Ne supprime que les applications (namespace des applications) ; RabbitMQ, Redis et leurs
  données restent en place.
.PARAMETER RabbitmqOnly
  Supprime uniquement RabbitMQ et ses données (files, stream, utilisateurs) dans le namespace
  d'infrastructure. Ensuite : deploy-infra.ps1 [-NoOperator] puis deploy-apps.ps1 -SkipBuild.
  Incompatible avec -KeepInfra et -IncludeOperator.
.PARAMETER IncludeOperator
  Supprime aussi le RabbitMQ Cluster Operator (namespace rabbitmq-system et CRD).
  Incompatible avec -KeepInfra.
.PARAMETER Force
  Ne demande pas de confirmation.
.PARAMETER Namespace
  Namespace des applications (défaut : orderflow).
.PARAMETER InfraNamespace
  Namespace de RabbitMQ et Redis (défaut : orderflow-infra).
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/destroy.ps1
  ./scripts/destroy.ps1 -KeepInfra -Force
  ./scripts/destroy.ps1 -IncludeOperator -Force
  ./scripts/destroy.ps1 -RabbitmqOnly -Force
#>
[CmdletBinding()]
param(
    [switch]$KeepInfra,
    [switch]$IncludeOperator,
    [switch]$RabbitmqOnly,
    [switch]$Force,
    [string]$Namespace = 'orderflow',
    [string]$InfraNamespace = 'orderflow-infra',
    [string]$Context = '',
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"
if ($KeepInfra -and $IncludeOperator) { throw '-KeepInfra et -IncludeOperator sont incompatibles (le RabbitmqCluster a besoin de l opérateur)' }
$AppUser = 'orderflow'

if ($RabbitmqOnly -and ($KeepInfra -or $IncludeOperator)) { throw '-RabbitmqOnly est incompatible avec -KeepInfra et -IncludeOperator' }

$currentContext = $Context
if (-not $currentContext) { $currentContext = (Invoke-Kubectl config current-context) | Select-Object -First 1 }
$scope = "le namespace $Namespace (release orderflow, Secrets d'identifiants)"
if (-not $KeepInfra) { $scope += " ET le namespace $InfraNamespace (releases rabbitmq et redis, données, PVC, Secrets)" }
else { $scope += " ainsi que l'utilisateur RabbitMQ '$AppUser'" }
if ($IncludeOperator) { $scope += " ET le RabbitMQ Cluster Operator (rabbitmq-system, CRD)" }
if ($RabbitmqOnly) {
    $scope = "RabbitMQ dans le namespace $InfraNamespace (release rabbitmq, files, stream, utilisateurs, PVC, Secrets) ; Redis, applications et opérateur restent"
}

if (-not $Force) {
    Write-Host "Contexte kubectl : $currentContext" -ForegroundColor Yellow
    Write-Host "Va supprimer définitivement $scope." -ForegroundColor Yellow
    $answer = Read-Host "Taper 'oui' pour confirmer"
    if ($answer -ne 'oui') {
        Write-Host 'Annulé.'
        return
    }
}

function Remove-OrderflowNamespace([string]$Name, [string[]]$Releases) {
    # Releases Helm, restes hors Helm, PVC, puis le namespace lui-même.
    if (-not (Invoke-Kubectl get namespace $Name --ignore-not-found -o name)) {
        Write-Host "Namespace $Name absent : rien à supprimer."
        return
    }
    foreach ($release in $Releases) {
        Write-Step "Désinstallation de la release Helm $release (namespace $Name)"
        if (Test-HelmRelease $release $Name) {
            Invoke-Helm uninstall $release -n $Name --wait --timeout "$($TimeoutSeconds)s" | Out-Host
        } else {
            Write-Host "Release $release absente."
        }
    }

    Write-Step "Suppression des restes du namespace $Name (objets hors Helm, Secret conservé)"
    Invoke-Kubectl -n $Name delete deployment --all --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
    if (Invoke-Kubectl get crd rabbitmqclusters.rabbitmq.com --ignore-not-found -o name) {
        # Tant que l'opérateur est présent, il retire son finalizer et nettoie le StatefulSet.
        Invoke-Kubectl -n $Name delete rabbitmqcluster --all --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
    }
    Invoke-Kubectl -n $Name delete statefulset --all --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host

    Write-Step "Suppression des PVC et du namespace $Name"
    Invoke-Kubectl -n $Name delete pvc --all --ignore-not-found --wait=false | Out-Host
    Invoke-Kubectl delete namespace $Name --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
}

function Remove-Rabbitmq {
    # RabbitMQ seul, quelle que soit la variante : release, restes, PVC, Secrets conservés.
    $variant = Get-RabbitmqVariant
    Write-Step "Désinstallation de RabbitMQ (variante : $(if ($variant) { $variant } else { 'aucune' }), namespace $InfraNamespace)"
    if (Test-HelmRelease $RabbitmqRelease $InfraNamespace) {
        Invoke-Helm uninstall $RabbitmqRelease -n $InfraNamespace --wait --timeout "$($TimeoutSeconds)s" | Out-Host
    } else {
        Write-Host "Release $RabbitmqRelease absente."
    }
    if (Invoke-Kubectl get crd rabbitmqclusters.rabbitmq.com --ignore-not-found -o name) {
        Invoke-Kubectl -n $InfraNamespace delete rabbitmqcluster rabbitmq --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
    }
    Invoke-Kubectl -n $InfraNamespace delete statefulset rabbitmq-server --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
    foreach ($i in 0..9) {
        Invoke-Kubectl -n $InfraNamespace delete pod "rabbitmq-server-$i" --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Null
    }
    Write-Step 'Suppression des PVC et des Secrets RabbitMQ (conservés par helm uninstall)'
    $pvcs = @(Invoke-Kubectl -n $InfraNamespace get pvc -o name | Where-Object { "$_" -like 'persistentvolumeclaim/persistence-rabbitmq-server-*' })
    foreach ($pvc in $pvcs) { Invoke-Kubectl -n $InfraNamespace delete $pvc --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host }
    Invoke-Kubectl -n $InfraNamespace delete secret rabbitmq-default-user rabbitmq-erlang-cookie --ignore-not-found | Out-Host

    # Le stream orders.events a disparu avec les données : les checkpoints des subscribers
    # (Redis) pointeraient au-delà de la fin du nouveau stream et ses premiers événements
    # seraient ignorés. On les efface et on relance les subscribers (reprise à STREAM_OFFSET).
    Write-Step 'Remise à zéro des checkpoints des subscribers (le stream a été supprimé)'
    try {
        $keys = @(Invoke-RedisMaster -Command @('--scan', '--pattern', 'stream:orders.events:offset:*') | Where-Object { $_ })
        foreach ($key in $keys) { Invoke-RedisMaster -Command @('DEL', $key) | Out-Null; Write-Host "  $key supprimée" }
        if ($keys.Count -eq 0) { Write-Host '  aucun checkpoint' }
    } catch {
        Write-Host "Redis injoignable, checkpoints non supprimés : $_" -ForegroundColor Yellow
    }
    if (Invoke-Kubectl get namespace $Namespace --ignore-not-found -o name) {
        Invoke-Kubectl -n $Namespace rollout restart deployment -l app.kubernetes.io/component=subscriber | Out-Host
    }
}

if ($RabbitmqOnly) {
    Remove-Rabbitmq
    Write-Host ''
    Write-Host "RabbitMQ supprimé de $InfraNamespace. Réinstaller : ./scripts/deploy-infra.ps1 -Only rabbitmq [-NoOperator], puis ./scripts/deploy-apps.ps1 -SkipBuild (recrée l'utilisateur orderflow)." -ForegroundColor Green
    return
}

# --- 1. Applications ------------------------------------------------------------
Remove-OrderflowNamespace $Namespace @('orderflow')

if ($KeepInfra) {
    # Les identifiants de l'utilisateur applicatif ont disparu avec le namespace : on retire
    # aussi l'utilisateur (un nouveau deploy-apps.ps1 le recrée avec un nouveau mot de passe).
    Write-Step "Suppression de l'utilisateur RabbitMQ $AppUser (namespace $InfraNamespace)"
    try {
        $users = (Invoke-Rabbitmqctl -Command @('list_users', '--quiet', '--formatter', 'json')) -join "`n" | ConvertFrom-Json
        if (@($users | Where-Object { $_.user -eq $AppUser }).Count -gt 0) {
            Invoke-Rabbitmqctl -Command @('delete_user', $AppUser) | Out-Host
        } else {
            Write-Host "Utilisateur $AppUser absent."
        }
    } catch {
        Write-Host "RabbitMQ injoignable, utilisateur $AppUser non supprimé : $_" -ForegroundColor Yellow
    }
} else {
    # --- 2. Infrastructure ------------------------------------------------------
    Remove-OrderflowNamespace $InfraNamespace @('rabbitmq', 'redis')
}

if ($IncludeOperator) {
    Write-Step 'Suppression du RabbitMQ Cluster Operator'
    Invoke-Kubectl delete -k "$Root/k8s/operators/rabbitmq-cluster-operator" --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
}

Write-Host ''
if ($KeepInfra) { Write-Host "Applications OrderFlow supprimées (infrastructure conservée dans $InfraNamespace)." -ForegroundColor Green }
else { Write-Host 'OrderFlow supprimé.' -ForegroundColor Green }
