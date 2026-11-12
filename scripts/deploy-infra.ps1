<#
.SYNOPSIS
  Déploie (de façon idempotente) l'infrastructure OrderFlow : RabbitMQ Cluster Operator,
  puis la release Helm orderflow-infra (RabbitMQ 3 nœuds, Redis Sentinel, Secret/redis-auth).

.DESCRIPTION
  1. kubectl apply --server-side -k k8s/operators/rabbitmq-cluster-operator
  2. attend le CRD, le certificat du webhook (et son caBundle) et le Deployment de l'opérateur
  3. helm upgrade --install orderflow-infra charts/orderflow-infra -n orderflow
     --create-namespace --wait (le chart échoue avec un message clair si le CRD manque)
  4. attend RabbitMQ (StatefulSet créé par l'opérateur, conditions AllReplicasReady et
     ClusterAvailable : helm --wait ne les connaît pas) et l'état nominal de Redis Sentinel
     (1 master, réplicas sains, un sentinel par pod)

  Relançable à volonté : un upgrade sans changement ne redémarre aucun pod ; le mot de passe
  Redis généré au premier install est conservé (lookup du Secret existant dans le chart).

.PARAMETER Values
  Fichiers de valeurs supplémentaires pour orderflow-infra (helm -f).
.PARAMETER Set
  Surcharges ponctuelles (helm --set), ex. -Set 'redis.persistence.size=2Gi'.
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/deploy-infra.ps1
  ./scripts/deploy-infra.ps1 -Set 'rabbitmq.persistence.storageClassName=fast'
#>
[CmdletBinding()]
param(
    [string[]]$Values = @(),
    [string[]]$Set = @(),
    [string]$Context = '',
    [int]$TimeoutSeconds = 600
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"
$FieldManager = 'orderflow-deploy'
$Release = 'orderflow-infra'

# --- 1. Opérateur ---------------------------------------------------------------
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

# --- 2. Release Helm ------------------------------------------------------------
Write-Step "helm upgrade --install $Release (namespace $Namespace)"
# --force-conflicts : le chart fait foi, même sur un champ modifié à la main (cf. deploy-apps.ps1).
$helmArgs = @('upgrade', '--install', $Release, "$Root/charts/orderflow-infra", '-n', $Namespace,
    '--create-namespace', '--wait', '--timeout', "$($TimeoutSeconds)s", '--force-conflicts') + (Get-HelmSetArgs $Values $Set)
# Juste après une première installation, le webhook peut encore refuser la connexion
# quelques secondes : on réessaie uniquement dans ce cas.
for ($i = 1; ; $i++) {
    try {
        Invoke-Helm @helmArgs | Out-Host
        break
    } catch {
        if ($i -ge 10 -or "$_" -notmatch 'webhook') { throw }
        Write-Host "Webhook de l'opérateur pas encore prêt (tentative $i/10), nouvel essai dans 6 s..." -ForegroundColor Yellow
        Start-Sleep -Seconds 6
    }
}

# --- 3. Attente de la disponibilité ---------------------------------------------
Write-Step 'Attente de Redis (StatefulSet/redis et Sentinel)'
Invoke-Kubectl -n $Namespace rollout status statefulset/redis "--timeout=$($TimeoutSeconds)s" | Out-Host
$replicas = [int](Invoke-Kubectl -n $Namespace get statefulset redis -o 'jsonpath={.spec.replicas}')
$topology = Wait-RedisTopology -Replicas $replicas -TimeoutSeconds $TimeoutSeconds
Write-Host ("Sentinel : master {0}, {1} réplica(s) sain(s), {2} sentinels" -f $topology.Master, $topology.Replicas, $topology.Sentinels)

Write-Step 'Attente de RabbitMQ (RabbitmqCluster/rabbitmq)'
# Le StatefulSet est créé par l'opérateur : attendre qu'il existe avant de le surveiller.
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
while (-not (Invoke-Kubectl -n $Namespace get statefulset rabbitmq-server --ignore-not-found -o name)) {
    if ((Get-Date) -gt $deadline) { throw 'StatefulSet/rabbitmq-server non créé par l opérateur' }
    Start-Sleep -Seconds 3
}
Invoke-Kubectl -n $Namespace rollout status statefulset/rabbitmq-server "--timeout=$($TimeoutSeconds)s" | Out-Host
Invoke-Kubectl -n $Namespace wait --for=condition=AllReplicasReady rabbitmqcluster/rabbitmq "--timeout=$($TimeoutSeconds)s" | Out-Host
Invoke-Kubectl -n $Namespace wait --for=condition=ClusterAvailable rabbitmqcluster/rabbitmq "--timeout=$($TimeoutSeconds)s" | Out-Host

Write-Step 'Etat final'
Invoke-Helm list -n $Namespace | Out-Host
Invoke-Kubectl -n $Namespace get 'rabbitmqcluster,statefulset,pod,svc,pvc,pdb' | Out-Host
Write-Host ''
Write-Host 'Infrastructure OrderFlow prête.' -ForegroundColor Green
