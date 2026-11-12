<#
.SYNOPSIS
  Test de résilience OrderFlow : poste des commandes en continu pendant qu'on supprime
  le master Redis puis un nœud RabbitMQ, et vérifie qu'aucune commande n'est perdue ni
  « fantôme ».

.DESCRIPTION
  1. relève l'état initial (master Redis selon Sentinel, taille du stream orders.events,
     redémarrages des pods) ;
  2. ouvre un port-forward temporaire vers svc/gateway (port local libre) ;
  3. poste -Orders commandes valides à -Rate commandes/s ;
     ~3 s après le début : suppression du pod master Redis (failover Sentinel) ;
     ~10 s plus tard : suppression de rabbitmq-server-0 ;
  4. attend l'état terminal de chaque commande acceptée (GET /orders/{id}) puis que
     subscriber-audit ait journalisé l'événement de chacune ;
  5. attend que Redis et RabbitMQ soient de nouveau complets et affiche le bilan.

  Invariants vérifiés : toute réponse non-202 est un 503 ; toute commande acceptée (202)
  se termine en SUCCESS ; nombre d'event_id distincts des événements de ces commandes
  (lus dans le journal de subscriber-audit) == nombre de 202.
  Le delta brut du stream n'est qu'affiché : il contient aussi les digests du scheduler
  (order.digest, un par minute) et d'éventuels doublons at-least-once légitimes (même
  event_id), qui ne sont ni des pertes ni des commandes fantômes.
  Code retour 1 si un invariant est violé.

.PARAMETER Orders
  Nombre de commandes à poster (défaut 60).
.PARAMETER Rate
  Commandes par seconde (défaut 2).
.PARAMETER SkipRedis
  Ne pas supprimer le master Redis.
.PARAMETER SkipRabbit
  Ne pas supprimer rabbitmq-server-0.
.PARAMETER Namespace
  Namespace des applications (défaut : orderflow).
.PARAMETER InfraNamespace
  Namespace de RabbitMQ et Redis (défaut : orderflow-infra).
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/chaos.ps1
  ./scripts/chaos.ps1 -Orders 100 -SkipRabbit
#>
[CmdletBinding()]
param(
    [int]$Orders = 60,
    [double]$Rate = 2,
    [switch]$SkipRedis,
    [switch]$SkipRabbit,
    [string]$Namespace = 'orderflow',
    [string]$InfraNamespace = 'orderflow-infra',
    [string]$Context = '',
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"

$RedisKillAfterSeconds = 3
$RabbitKillDelaySeconds = 10

function Get-AuditEvents([string]$Since) {
    # subscriber-audit écrit un document JSON par événement du stream (avec stream_offset).
    # --previous : lignes d'un conteneur audit qui aurait redémarré pendant le test.
    $lines = @()
    try { $lines += @(Invoke-KubectlQuiet -n $Namespace logs deployment/subscriber-audit --previous "--since-time=$Since") } catch { }
    $lines += @(Invoke-Kubectl -n $Namespace logs deployment/subscriber-audit "--since-time=$Since")
    foreach ($line in $lines) {
        if ("$line".StartsWith('{')) { try { "$line" | ConvertFrom-Json } catch { } }
    }
}

function Measure-OrderEvents($Events, [hashtable]$OrderIds) {
    # Evénements des commandes du test : lignes (doublons compris) et event_id distincts.
    $orderEvents = @($Events | Where-Object { $_.order_id -and $OrderIds.ContainsKey($_.order_id) })
    $distinct = @($orderEvents | ForEach-Object { $_.event_id } | Sort-Object -Unique).Count
    $digests = @($Events | Where-Object { $_.event_type -eq 'order.digest' }).Count
    return [pscustomobject]@{ Lines = $orderEvents.Count; Distinct = $distinct; Digests = $digests }
}

# --- 1. Etat initial ------------------------------------------------------------
Write-Step 'Etat initial'
# Marge de 30 s : les commandes du test sont de toute façon filtrées par order_id.
$auditSince = (Get-Date).ToUniversalTime().AddSeconds(-30).ToString('yyyy-MM-ddTHH:mm:ssZ')
$masterBefore = Get-RedisMasterPod
$streamBefore = Get-StreamMessageCount
$restartsBefore = Get-PodRestarts
Write-Host "Master Redis           : $masterBefore"
Write-Host "Messages dans le stream: $streamBefore"

$results = New-Object System.Collections.ArrayList
$events = New-Object System.Collections.ArrayList
$pf = $null
$portForwardRestarts = 0
try {
    Write-Step 'Port-forward temporaire vers svc/gateway'
    $pf = Start-PortForward
    Write-Host "Gateway joignable sur $($pf.Url)"

    # --- 2. Injection de charge et de pannes ------------------------------------
    Write-Step "Envoi de $Orders commandes à $Rate/s (pannes : Redis=$(-not $SkipRedis), RabbitMQ=$(-not $SkipRabbit))"
    $redisKilledAt = $null
    $rabbitKilled = $false
    $start = Get-Date
    for ($i = 1; $i -le $Orders; $i++) {
        $elapsed = ((Get-Date) - $start).TotalSeconds
        if (-not $SkipRedis -and $null -eq $redisKilledAt -and $elapsed -ge $RedisKillAfterSeconds) {
            $victim = Get-RedisMasterPod
            Invoke-Kubectl -n $InfraNamespace delete pod $victim --wait=false | Out-Null
            $redisKilledAt = $elapsed
            [void]$events.Add(('{0,5:N1} s  suppression du master Redis {1}' -f $elapsed, $victim))
            Write-Host "  [$('{0:N1}' -f $elapsed) s] pod $victim (master Redis) supprimé" -ForegroundColor Yellow
        }
        $rabbitDue = $RedisKillAfterSeconds + $RabbitKillDelaySeconds
        if ($null -ne $redisKilledAt) { $rabbitDue = $redisKilledAt + $RabbitKillDelaySeconds }
        if (-not $SkipRabbit -and -not $rabbitKilled -and $elapsed -ge $rabbitDue) {
            Invoke-Kubectl -n $InfraNamespace delete pod rabbitmq-server-0 --wait=false | Out-Null
            $rabbitKilled = $true
            [void]$events.Add(('{0,5:N1} s  suppression de rabbitmq-server-0' -f $elapsed))
            Write-Host "  [$('{0:N1}' -f $elapsed) s] pod rabbitmq-server-0 supprimé" -ForegroundColor Yellow
        }

        if ($pf.Process.HasExited) {
            Write-Host '  port-forward interrompu (pod gateway redémarré ?) : réouverture' -ForegroundColor Yellow
            Stop-PortForward $pf
            $pf = Start-PortForward
            $portForwardRestarts++
        }

        $body = @{
            customer_id = "chaos_$i"
            items       = @(@{ sku = 'SKU-BOOK-001'; quantity = 1; unit_price = 12.5 })
        }
        $sent = Get-Date
        $response = Invoke-Api -Method POST -Url "$($pf.Url)/orders" -Body $body
        $latency = ((Get-Date) - $sent).TotalSeconds
        $orderId = $null
        if ($response.Status -eq 202) { $orderId = $response.Json.order_id }
        [void]$results.Add([pscustomobject]@{ N = $i; Status = $response.Status; OrderId = $orderId; Latency = $latency; Final = $null; Detail = $response.Text })
        if ($response.Status -ne 202) {
            Write-Host "  commande $i : HTTP $($response.Status) (Retry-After: $($response.RetryAfter)) $($response.Text)" -ForegroundColor Red
        } elseif ($latency -gt 1) {
            Write-Host ("  commande {0} : 202 en {1:N1} s" -f $i, $latency) -ForegroundColor DarkYellow
        }

        $nextAt = $start.AddSeconds($i / $Rate)
        $wait = ($nextAt - (Get-Date)).TotalMilliseconds
        if ($wait -gt 0) { Start-Sleep -Milliseconds ([int]$wait) }
    }

    # --- 3. Etat terminal des commandes acceptées --------------------------------
    $accepted = @($results | Where-Object { $_.Status -eq 202 })
    Write-Step "Attente de l'état terminal des $($accepted.Count) commandes acceptées"
    foreach ($r in $accepted) {
        if ($pf.Process.HasExited) { Stop-PortForward $pf; $pf = Start-PortForward; $portForwardRestarts++ }
        $final = Wait-OrderTerminal -BaseUrl $pf.Url -OrderId $r.OrderId -TimeoutSeconds 120
        $r.Final = $final.status
    }
} finally {
    Stop-PortForward $pf
}

# --- 4. Evénements reçus par subscriber-audit et retour à la normale ----------------
$accepted = @($results | Where-Object { $_.Status -eq 202 })
$acceptedIds = @{}
foreach ($r in $accepted) { $acceptedIds[$r.OrderId] = $true }
Write-Step 'Attente des événements des commandes acceptées (journal de subscriber-audit)'
$deadline = (Get-Date).AddSeconds(90)
$audit = Measure-OrderEvents (Get-AuditEvents $auditSince) $acceptedIds
$stableSince = Get-Date
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 3
    $current = Measure-OrderEvents (Get-AuditEvents $auditSince) $acceptedIds
    if ($current.Lines -ne $audit.Lines) { $audit = $current; $stableSince = Get-Date; continue }
    $audit = $current
    # Stable depuis 10 s et toutes les commandes vues : terminé (laisse passer un doublon tardif).
    if ($audit.Distinct -ge $accepted.Count -and ((Get-Date) - $stableSince).TotalSeconds -ge 10) { break }
}
$streamAfter = Get-StreamMessageCount

Write-Step 'Attente du retour à la normale (Redis, RabbitMQ)'
Invoke-Kubectl -n $InfraNamespace rollout status statefulset/redis "--timeout=$($TimeoutSeconds)s" | Out-Host
# Avec ou sans opérateur : StatefulSet déployé, cluster complet vu par chaque nœud.
Wait-RabbitmqReady -TimeoutSeconds $TimeoutSeconds | Out-Null
$masterAfter = Get-RedisMasterPod
$restartsAfter = Get-PodRestarts

# --- 5. Bilan -------------------------------------------------------------------
Write-Step 'Bilan'
if ($events.Count -gt 0) {
    Write-Host 'Pannes injectées :'
    $events | ForEach-Object { Write-Host "  $_" }
}
$rejected = @($results | Where-Object { $_.Status -ne 202 })
$maxLatency = ($results | Measure-Object -Property Latency -Maximum).Maximum
Write-Host ''
Write-Host ("Commandes postées      : {0}" -f $results.Count)
Write-Host ("Acceptées (202)        : {0}" -f $accepted.Count)
Write-Host ("Refusées               : {0}" -f $rejected.Count)
$rejected | Group-Object Status | ForEach-Object { Write-Host ("  HTTP {0} : {1}" -f $_.Name, $_.Count) }
Write-Host ("Latence POST max       : {0:N1} s" -f $maxLatency)
Write-Host 'Etat final des commandes acceptées :'
$accepted | Group-Object Final | ForEach-Object { Write-Host ("  {0} : {1}" -f $_.Name, $_.Count) }
$delta = $streamAfter - $streamBefore
Write-Host ("Evénements de commande : {0} event_id distincts (attendu {1}), {2} doublon(s) at-least-once" -f $audit.Distinct, $accepted.Count, ($audit.Lines - $audit.Distinct))
Write-Host ("Stream orders.events   : {0} -> {1} (delta brut {2}, dont digests du scheduler : ~{3})" -f $streamBefore, $streamAfter, $delta, $audit.Digests)
Write-Host ("Master Redis           : {0} -> {1}" -f $masterBefore, $masterAfter)
if ($portForwardRestarts -gt 0) { Write-Host "Port-forward rouvert   : $portForwardRestarts fois" }
Write-Host 'Redémarrages de containers pendant le test :'
$restarted = $false
foreach ($pod in ($restartsAfter.Keys | Sort-Object)) {
    $before = 0
    if ($restartsBefore.ContainsKey($pod)) { $before = $restartsBefore[$pod] }
    if ($restartsAfter[$pod] -gt $before) {
        Write-Host ("  {0} : +{1}" -f $pod, ($restartsAfter[$pod] - $before))
        $restarted = $true
    }
}
if (-not $restarted) { Write-Host '  aucun (hors pods supprimés volontairement, recréés à neuf)' }

$failures = @()
if (@($rejected | Where-Object { $_.Status -ne 503 }).Count -gt 0) { $failures += 'réponse non-202 différente de 503' }
if (@($accepted | Where-Object { $_.Final -ne 'SUCCESS' }).Count -gt 0) { $failures += 'commande acceptée non terminée en SUCCESS' }
if ($audit.Distinct -ne $accepted.Count) { $failures += "événements distincts ($($audit.Distinct)) différents du nombre de 202 ($($accepted.Count))" }
Write-Host ''
if ($failures.Count -eq 0) {
    Write-Host 'OK : aucune commande perdue, aucune commande fantôme.' -ForegroundColor Green
} else {
    $failures | ForEach-Object { Write-Host "ECHEC : $_" -ForegroundColor Red }
    exit 1
}
