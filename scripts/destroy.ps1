<#
.SYNOPSIS
  Supprime OrderFlow du cluster : releases Helm orderflow puis orderflow-infra, PVC et
  namespace orderflow (RabbitMQ, Redis, données et Secrets compris). Avec -IncludeOperator,
  supprime aussi le RabbitMQ Cluster Operator.

.DESCRIPTION
  1. helm uninstall orderflow (applications), puis helm uninstall orderflow-infra (le
     RabbitmqCluster est supprimé pendant que l'opérateur est encore là pour nettoyer) ;
  2. restes éventuels : objets créés hors Helm (ancienne installation kubectl), Secret
     redis-auth conservé par helm.sh/resource-policy=keep, StatefulSet de l'opérateur ;
  3. suppression des PVC puis du namespace orderflow (attente de sa disparition) ;
  4. avec -IncludeOperator : kubectl delete -k k8s/operators/rabbitmq-cluster-operator.

  DESTRUCTIF ET IRRÉVERSIBLE (données Redis/RabbitMQ perdues) : demande une confirmation
  sauf avec -Force. Idempotent : les ressources déjà absentes sont ignorées.
  Ne touche à aucun autre namespace (sauf rabbitmq-system avec -IncludeOperator).

.PARAMETER IncludeOperator
  Supprime aussi le RabbitMQ Cluster Operator (namespace rabbitmq-system et CRD).
.PARAMETER Force
  Ne demande pas de confirmation.
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/destroy.ps1
  ./scripts/destroy.ps1 -IncludeOperator -Force
#>
[CmdletBinding()]
param(
    [switch]$IncludeOperator,
    [switch]$Force,
    [string]$Context = '',
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"

$currentContext = $Context
if (-not $currentContext) { $currentContext = (Invoke-Kubectl config current-context) | Select-Object -First 1 }
$scope = "le namespace $Namespace (releases orderflow et orderflow-infra, RabbitMQ, Redis, PVC, Secrets)"
if ($IncludeOperator) { $scope += " ET le RabbitMQ Cluster Operator (rabbitmq-system, CRD)" }

if (-not $Force) {
    Write-Host "Contexte kubectl : $currentContext" -ForegroundColor Yellow
    Write-Host "Va supprimer définitivement $scope." -ForegroundColor Yellow
    $answer = Read-Host "Taper 'oui' pour confirmer"
    if ($answer -ne 'oui') {
        Write-Host 'Annulé.'
        return
    }
}

$namespaceExists = Invoke-Kubectl get namespace $Namespace --ignore-not-found -o name
if ($namespaceExists) {
    # Applications d'abord : plus aucun client quand RabbitMQ et Redis s'arrêtent.
    foreach ($release in 'orderflow', 'orderflow-infra') {
        Write-Step "Désinstallation de la release Helm $release"
        if (Test-HelmRelease $release) {
            Invoke-Helm uninstall $release -n $Namespace --wait --timeout "$($TimeoutSeconds)s" | Out-Host
        } else {
            Write-Host "Release $release absente."
        }
    }

    Write-Step 'Suppression des restes (objets hors Helm, Secret conservé)'
    Invoke-Kubectl -n $Namespace delete deployment --all --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
    $crd = Invoke-Kubectl get crd rabbitmqclusters.rabbitmq.com --ignore-not-found -o name
    if ($crd) {
        # Tant que l'opérateur est présent, il retire son finalizer et nettoie le StatefulSet.
        Invoke-Kubectl -n $Namespace delete rabbitmqcluster --all --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
    }
    Invoke-Kubectl -n $Namespace delete statefulset --all --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host

    Write-Step "Suppression des PVC et du namespace $Namespace"
    Invoke-Kubectl -n $Namespace delete pvc --all --ignore-not-found --wait=false | Out-Host
    Invoke-Kubectl delete namespace $Namespace --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
} else {
    Write-Host "Namespace $Namespace absent : rien à supprimer."
}

if ($IncludeOperator) {
    Write-Step 'Suppression du RabbitMQ Cluster Operator'
    Invoke-Kubectl delete -k "$Root/k8s/operators/rabbitmq-cluster-operator" --ignore-not-found --wait=true "--timeout=$($TimeoutSeconds)s" | Out-Host
}

Write-Host ''
Write-Host 'OrderFlow supprimé.' -ForegroundColor Green
