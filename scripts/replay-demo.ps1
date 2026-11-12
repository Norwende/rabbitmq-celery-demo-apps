<#
.SYNOPSIS
  Démonstration du replay du stream orders.events et de la reprise sur checkpoint.

.DESCRIPTION
  Partie 1 - replay : démarre un pod temporaire "subscriber-replay" dont la spec est dérivée
  de Deployment/subscriber-audit (même image, mêmes sources d'environnement, securityContext
  et ressources) avec un nom de subscriber neuf (replay-<horodatage>), SUBSCRIBER_ROLE=audit,
  STREAM_OFFSET=first et CHECKPOINT_ENABLED=false : il relit tout l'historique depuis
  l'offset 0. Le script compte les événements relus, puis supprime le pod.

  Partie 2 - checkpoint : redémarre subscriber-analytics et montre qu'il reprend après
  son dernier offset traité (ligne "attaching ... at offset=N (last processed=N-1)")
  sans double comptage (agrégats Redis identiques avant/après).

  À lancer hors trafic : de nouvelles commandes pendant la démo fausseraient les comptes.
  Les digests publiés chaque minute par le scheduler (order.digest) ne gênent pas : le
  replay les relit comme les autres événements et analytics ne les compte pas.

.PARAMETER TimeoutSeconds
  Délai maximal d'attente du replay complet (défaut 120).
.PARAMETER SkipCheckpoint
  N'exécute que la partie 1 (replay).
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/replay-demo.ps1
#>
[CmdletBinding()]
param(
    [int]$TimeoutSeconds = 120,
    [switch]$SkipCheckpoint,
    [string]$Context = ''
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"

$PodName = 'subscriber-replay'
$subscriberName = 'replay-' + (Get-Date -Format 'yyyyMMddHHmmss')

function Get-AnalyticsSnapshot {
    $values = @(Invoke-RedisMaster -Command @('HGETALL', 'analytics:orders'))
    $parts = @()
    for ($i = 0; $i + 1 -lt $values.Count; $i += 2) { $parts += "$($values[$i])=$($values[$i + 1])" }
    return (($parts | Sort-Object) -join ' ')
}

function Remove-ReplayPod {
    Invoke-Kubectl -n $Namespace delete pod $PodName --ignore-not-found --wait=true --timeout=60s | Out-Null
}

# --- Partie 1 : replay depuis l'offset 0 -------------------------------------------
Write-Step 'Partie 1 - replay complet du stream par un subscriber neuf'
$streamCount = Get-StreamMessageCount
Write-Host "Messages dans le stream orders.events : $streamCount"

# Spec du pod dérivée de Deployment/subscriber-audit (image, envFrom, secrets, securityContext,
# ressources, volume /tmp) : rien n'est recopié ici, le pod suit toujours le chart.
$deployment = (Invoke-Kubectl -n $Namespace get deployment subscriber-audit -o json) -join "`n" | ConvertFrom-Json
$spec = $deployment.spec.template.spec
$container = @($spec.containers)[0]
$overrides = [ordered]@{
    SUBSCRIBER_NAME    = $subscriberName
    SUBSCRIBER_ROLE    = 'audit'
    STREAM_OFFSET      = 'first'
    CHECKPOINT_ENABLED = 'false'
}
$envVars = @($overrides.Keys | ForEach-Object { [pscustomobject]@{ name = $_; value = $overrides[$_] } })
$envVars += @($container.env | Where-Object { -not $overrides.Contains($_.name) })
$container.env = $envVars
$container.PSObject.Properties.Remove('livenessProbe')   # pod jetable, sans redémarrage
$spec.restartPolicy = 'Never'
$pod = [ordered]@{
    apiVersion = 'v1'
    kind       = 'Pod'
    metadata   = @{
        name   = $PodName
        # Labels distincts du sélecteur de subscriber-audit : le pod n'est adopté par personne.
        labels = @{
            'app.kubernetes.io/name'      = $PodName
            'app.kubernetes.io/component' = 'subscriber'
            'app.kubernetes.io/part-of'   = 'orderflow'
        }
    }
    spec       = $spec
}
$manifest = Join-Path ([System.IO.Path]::GetTempPath()) "orderflow-$PodName.json"
[System.IO.File]::WriteAllText($manifest, ($pod | ConvertTo-Json -Depth 20), (New-Object System.Text.UTF8Encoding $false))

try {
    Remove-ReplayPod
    Write-Host "Création du pod $PodName (SUBSCRIBER_NAME=$subscriberName, role=audit, STREAM_OFFSET=first, checkpoint désactivé)"
    Invoke-Kubectl -n $Namespace apply -f $manifest | Out-Host
    Invoke-Kubectl -n $Namespace wait --for=condition=Ready "pod/$PodName" --timeout=120s | Out-Host

    # Le rôle audit écrit un document JSON par événement (avec stream_offset) sur stdout.
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $events = @()
    $logs = @()
    while ((Get-Date) -lt $deadline) {
        $logs = @(Invoke-Kubectl -n $Namespace logs $PodName)
        $events = @($logs | Where-Object { "$_".StartsWith('{') })
        if ($events.Count -ge $streamCount) { break }
        Start-Sleep -Seconds 2
    }
    $attach = $logs | Where-Object { "$_" -match 'attaching to stream' } | Select-Object -First 1
    Write-Host ''
    Write-Host "Log de démarrage : $attach"
    if ($events.Count -gt 0) {
        $first = $events[0] | ConvertFrom-Json
        $last = $events[-1] | ConvertFrom-Json
        $offsets = @($events | ForEach-Object { ($_ | ConvertFrom-Json).stream_offset })
        $distinct = @($offsets | Sort-Object -Unique).Count
        Write-Host ("Premier événement relu : offset {0}  {1}  {2}" -f $first.stream_offset, $first.event_type, $first.occurred_at)
        Write-Host ("Dernier événement relu : offset {0}  {1}  {2}" -f $last.stream_offset, $last.event_type, $last.occurred_at)
        Write-Host ("Evénements relus       : {0} (offsets distincts : {1}) / {2} messages dans le stream" -f $events.Count, $distinct, $streamCount)
        # Le stream contient les événements de commande et les digests du scheduler (order.digest).
        $byType = $events | ForEach-Object { ($_ | ConvertFrom-Json).event_type } | Group-Object | Sort-Object Name |
            ForEach-Object { "$($_.Name)=$($_.Count)" }
        Write-Host ("Par type               : {0}" -f ($byType -join '  '))
    }
    if ($events.Count -ge $streamCount -and $events.Count -gt 0 -and $first.stream_offset -eq 0) {
        Write-Host "OK : l'historique complet a été relu depuis l'offset 0." -ForegroundColor Green
    } else {
        Write-Host "ATTENTION : replay incomplet ($($events.Count)/$streamCount)." -ForegroundColor Yellow
    }
} finally {
    Write-Host "Suppression du pod $PodName"
    Remove-ReplayPod
    Remove-Item $manifest -Force -ErrorAction SilentlyContinue
}

if ($SkipCheckpoint) { return }

# --- Partie 2 : reprise sur checkpoint de subscriber-analytics -----------------------
Write-Step 'Partie 2 - redémarrage de subscriber-analytics (reprise sur checkpoint)'
$checkpointBefore = Invoke-RedisMaster -Command @('GET', 'stream:orders.events:offset:analytics')
$before = Get-AnalyticsSnapshot
Write-Host "Checkpoint avant : $checkpointBefore"
Write-Host "Agrégats avant   : $before"

$restartedAt = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
Invoke-Kubectl -n $Namespace rollout restart deployment/subscriber-analytics | Out-Host
Invoke-Kubectl -n $Namespace rollout status deployment/subscriber-analytics --timeout=120s | Out-Host

$attach = $null
$deadline = (Get-Date).AddSeconds(60)
while ((Get-Date) -lt $deadline -and -not $attach) {
    $attach = Invoke-Kubectl -n $Namespace logs deployment/subscriber-analytics "--since-time=$restartedAt" |
        Where-Object { "$_" -match 'attaching to stream' } | Select-Object -First 1
    if (-not $attach) { Start-Sleep -Seconds 2 }
}
Start-Sleep -Seconds 5   # laisse le temps de traiter un éventuel rejeu de chunk
$after = Get-AnalyticsSnapshot
Write-Host "Log de reprise   : $attach"
Write-Host "Agrégats après   : $after"
if ($before -eq $after) {
    Write-Host 'OK : reprise après le dernier offset traité, agrégats inchangés (pas de double comptage).' -ForegroundColor Green
} else {
    Write-Host 'ATTENTION : agrégats modifiés (nouvelles commandes pendant la démo ?).' -ForegroundColor Yellow
}
