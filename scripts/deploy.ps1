<#
.SYNOPSIS
  Déploiement complet d'OrderFlow en une commande : infrastructure puis applications.

.DESCRIPTION
  Enchaîne scripts/deploy-infra.ps1 (opérateur RabbitMQ, release Helm orderflow-infra :
  RabbitMQ 3 nœuds, Redis + Sentinel, Secret/redis-auth) puis scripts/deploy-apps.ps1
  (build des images, release Helm orderflow).
  Idempotent : relançable à volonté (aucun pod recréé si rien n'a changé).

.PARAMETER SkipBuild
  Ne reconstruit pas les images (transmis à deploy-apps.ps1).
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/deploy.ps1
#>
[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [string]$Context = '',
    [int]$TimeoutSeconds = 600
)

$ErrorActionPreference = 'Stop'
$started = Get-Date

& "$PSScriptRoot/deploy-infra.ps1" -Context $Context -TimeoutSeconds $TimeoutSeconds
& "$PSScriptRoot/deploy-apps.ps1" -Context $Context -SkipBuild:$SkipBuild -TimeoutSeconds $TimeoutSeconds

$duration = (Get-Date) - $started
Write-Host ''
Write-Host ("OrderFlow déployé en {0:mm\:ss}. Vérification : helm test orderflow -n orderflow ; démonstration : ./scripts/demo.ps1" -f $duration) -ForegroundColor Green
