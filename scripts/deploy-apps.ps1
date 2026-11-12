<#
.SYNOPSIS
  Déploie (de façon idempotente) les applications OrderFlow : build des images, puis
  helm upgrade --install de la release orderflow.

.DESCRIPTION
  1. construit les images orderflow/<service>:dev avec scripts/build-images.ps1
     (sauf -SkipBuild) ; k3s (Rancher Desktop, moby) partage le daemon docker ;
  2. calcule une empreinte du contenu de chaque image locale (couches + configuration, via
     docker image inspect) et la transmet au chart (imageIds.<image>) : le tag "dev" est
     mutable et imagePullPolicy=IfNotPresent, l'annotation checksum/image des pods change
     seulement si l'image a été reconstruite avec un contenu différent. Helm relance alors
     exactement les pods concernés, et un
     redéploiement sans changement ne recrée aucun pod. (Préféré à un « kubectl rollout
     restart » systématique : déclaratif, porté par la release, et sans redémarrage inutile.)
  3. helm upgrade --install orderflow charts/orderflow -n orderflow --wait.

  Prérequis : l'infrastructure (scripts/deploy-infra.ps1).

.PARAMETER Only
  Limite le build à certains services
  (gateway, worker-payments, worker-inventory, worker-notifier, subscriber, scheduler).
  Seuls les pods dont l'image a réellement changé sont relancés.
.PARAMETER SkipBuild
  Ne reconstruit pas les images.
.PARAMETER Values
  Fichiers de valeurs supplémentaires pour le chart orderflow (helm -f).
.PARAMETER Set
  Surcharges ponctuelles (helm --set), ex. -Set 'workers.payments.replicas=3'.
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/deploy-apps.ps1
  ./scripts/deploy-apps.ps1 -Only gateway
  ./scripts/deploy-apps.ps1 -SkipBuild -Set 'logLevel=DEBUG'
#>
[CmdletBinding()]
param(
    [string[]]$Only = @(),
    [switch]$SkipBuild,
    [string[]]$Values = @(),
    [string[]]$Set = @(),
    [string]$Context = '',
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"
$Release = 'orderflow'
$Tag = 'dev'
$images = @('gateway', 'worker-payments', 'worker-inventory', 'worker-notifier', 'subscriber', 'scheduler')
foreach ($service in $Only) {
    if ($images -notcontains $service) { throw "service inconnu : $service (attendu : $($images -join ', '))" }
}

# --- 1. Images ------------------------------------------------------------------
if ($SkipBuild) {
    Write-Step 'Build des images ignoré (-SkipBuild)'
} else {
    Write-Step "Build des images orderflow/*:$Tag"
    & "$PSScriptRoot/build-images.ps1" -Tag $Tag -Only $Only | Out-Host
}

Write-Step 'Empreinte du contenu des images locales (annotation checksum/image)'
# Empreinte = sha256 des couches (RootFS) et de la configuration de l'image. Pas l'Id docker :
# avec le magasin d'images containerd, l'Id est le digest de l'index, qui inclut une
# attestation de provenance horodatée et change à chaque build, même entièrement en cache.
$sha256 = [System.Security.Cryptography.SHA256]::Create()
$imageArgs = @()
foreach ($image in $images) {
    $ErrorActionPreference = 'Continue'
    $content = & docker image inspect --format '{{json .RootFS.Layers}}{{json .Config}}' "orderflow/${image}:$Tag" 2>$null
    $ErrorActionPreference = 'Stop'
    if ($LASTEXITCODE -ne 0 -or -not $content) {
        Write-Host "  orderflow/${image}:$Tag absente du démon docker local : pas d'annotation" -ForegroundColor Yellow
        continue
    }
    $bytes = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes(($content -join "`n")))
    $id = 'sha256:' + (($bytes | ForEach-Object { $_.ToString('x2') }) -join '')
    Write-Host ("  {0,-18} {1}" -f $image, $id)
    $imageArgs += @('--set-string', "imageIds.$image=$id")
}
$sha256.Dispose()

# --- 2. Release Helm ------------------------------------------------------------
Write-Step "helm upgrade --install $Release (namespace $Namespace)"
foreach ($secret in 'redis-auth', 'rabbitmq-default-user') {
    $found = Invoke-Kubectl -n $Namespace get secret $secret --ignore-not-found -o name
    if (-not $found) { throw "Secret/$secret absent : lancer d'abord scripts/deploy-infra.ps1" }
}
# --force-conflicts : le chart fait foi. Sans lui, un champ modifié à la main (kubectl scale,
# set env…) appartient à un autre gestionnaire server-side apply et helm 4 refuse l'upgrade.
$helmArgs = @('upgrade', '--install', $Release, "$Root/charts/orderflow", '-n', $Namespace,
    '--wait', '--timeout', "$($TimeoutSeconds)s", '--force-conflicts') + $imageArgs + (Get-HelmSetArgs $Values $Set)
Invoke-Helm @helmArgs | Out-Host

Write-Step 'Etat final'
Invoke-Kubectl -n $Namespace get 'deployment,pod' | Out-Host
Write-Host ''
Write-Host 'Applications OrderFlow déployées.' -ForegroundColor Green
