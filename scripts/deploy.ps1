<#
.SYNOPSIS
  Déploiement complet d'OrderFlow en une commande : infrastructure puis applications.

.DESCRIPTION
  Enchaîne scripts/deploy-infra.ps1 (RabbitMQ avec opérateur, ou sans avec -NoOperator, et
  Redis : releases Helm rabbitmq et redis dans le namespace d'infrastructure) puis scripts/deploy-apps.ps1 (identifiants dans le
  namespace des applications, build des images, release Helm orderflow).
  Idempotent : relançable à volonté (aucun pod recréé, aucun mot de passe changé si rien
  n'a changé).

.PARAMETER SkipBuild
  Ne reconstruit pas les images (transmis à deploy-apps.ps1).
.PARAMETER NoOperator
  RabbitMQ sans opérateur (chart rabbitmq-standalone), transmis à deploy-infra.ps1. Sans ce
  paramètre, une installation existante garde sa variante.
.PARAMETER Namespace
  Namespace des applications (défaut : orderflow).
.PARAMETER InfraNamespace
  Namespace de RabbitMQ et Redis (défaut : orderflow-infra).
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/deploy.ps1
  ./scripts/deploy.ps1 -NoOperator
#>
[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [switch]$NoOperator,
    [string]$Namespace = 'orderflow',
    [string]$InfraNamespace = 'orderflow-infra',
    [string]$Context = '',
    [int]$TimeoutSeconds = 600
)

$ErrorActionPreference = 'Stop'
$started = Get-Date

& "$PSScriptRoot/deploy-infra.ps1" -InfraNamespace $InfraNamespace -Context $Context -TimeoutSeconds $TimeoutSeconds -NoOperator:$NoOperator
& "$PSScriptRoot/deploy-apps.ps1" -Namespace $Namespace -InfraNamespace $InfraNamespace -Context $Context `
    -SkipBuild:$SkipBuild -TimeoutSeconds $TimeoutSeconds

$duration = (Get-Date) - $started
Write-Host ''
Write-Host ("OrderFlow déployé en {0:mm\:ss}. Vérification : helm test orderflow -n {1} ; démonstration : ./scripts/demo.ps1" -f $duration, $Namespace) -ForegroundColor Green
