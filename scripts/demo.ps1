<#
.SYNOPSIS
  Démonstration de bout en bout d'OrderFlow : trois commandes (nominale, paiement refusé,
  rupture de stock), suivi de la chaîne Celery, files RabbitMQ, subscribers et agrégats.

.DESCRIPTION
  1. ouvre un port-forward temporaire vers svc/gateway (port local libre, fermé à la fin) ;
  2. poste les trois scénarios du contrat (docs/contracts.md) :
       - commande valide                 -> SUCCESS, événement order.confirmed
       - total > 1000                    -> FAILURE PaymentDeclined, événement order.failed
       - un article de sku OUT-OF-STOCK  -> FAILURE OutOfStock, événement order.failed
  3. interroge GET /orders/{id} jusqu'à l'état terminal et affiche les étapes ;
  4. affiche les files RabbitMQ (nom, type, messages ; files celery_delayed_* masquées),
     les dernières lignes de log de chaque subscriber (événements de commande) et les
     agrégats analytics (Redis) ;
  5. cycle planifié : dernier envoi de notifications.sales_digest par le scheduler (Celery
     beat) et dernier digest order.digest reçu par subscriber-live.

.PARAMETER LogLines
  Nombre de lignes de log affichées par subscriber (défaut 4).
.PARAMETER Context
  Contexte kubectl à utiliser (défaut : contexte courant).
.EXAMPLE
  ./scripts/demo.ps1
#>
[CmdletBinding()]
param(
    [int]$LogLines = 4,
    [string]$Context = ''
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"

$scenarios = @(
    @{
        Title = 'Commande valide'
        Body  = @{ customer_id = 'cust_42'; currency = 'EUR'; items = @(
                @{ sku = 'SKU-BOOK-001'; quantity = 2; unit_price = 19.90 },
                @{ sku = 'SKU-PEN-002'; quantity = 3; unit_price = 1.50 }) }
    },
    @{
        Title = 'Montant > 1000 (paiement refusé)'
        Body  = @{ customer_id = 'cust_77'; currency = 'EUR'; items = @(
                @{ sku = 'SKU-LAPTOP-001'; quantity = 1; unit_price = 1299.00 }) }
    },
    @{
        Title = 'Article OUT-OF-STOCK (rupture de stock)'
        Body  = @{ customer_id = 'cust_99'; currency = 'EUR'; items = @(
                @{ sku = 'SKU-BOOK-001'; quantity = 1; unit_price = 19.90 },
                @{ sku = 'OUT-OF-STOCK'; quantity = 1; unit_price = 5.00 }) }
    }
)

$pf = $null
try {
    Write-Step 'Port-forward temporaire vers svc/gateway'
    $pf = Start-PortForward
    Write-Host "Gateway joignable sur $($pf.Url)"

    Write-Step 'Envoi des trois commandes (POST /orders)'
    $orders = @()
    foreach ($scenario in $scenarios) {
        $response = Invoke-Api -Method POST -Url "$($pf.Url)/orders" -Body $scenario.Body
        if ($response.Status -ne 202) { throw "POST /orders ($($scenario.Title)) : HTTP $($response.Status) $($response.Text)" }
        Write-Host ("  {0,-42} -> 202 {1}" -f $scenario.Title, $response.Json.order_id)
        $orders += [pscustomobject]@{ Title = $scenario.Title; OrderId = $response.Json.order_id }
    }

    Write-Step 'Suivi des chaînes (GET /orders/{id} jusqu''à l''état terminal)'
    foreach ($order in $orders) {
        $status = Wait-OrderTerminal -BaseUrl $pf.Url -OrderId $order.OrderId -TimeoutSeconds 60
        $color = 'Green'
        if ($status.status -ne 'SUCCESS') { $color = 'Yellow' }
        Write-Host ''
        Write-Host "$($order.Title) - $($order.OrderId)" -ForegroundColor White
        Write-Host "  statut : $($status.status)" -ForegroundColor $color
        foreach ($step in @($status.steps)) {
            Write-Host ("    {0,-32} {1}" -f $step.task, $step.state)
        }
        if ($status.error) { Write-Host "  erreur   : $($status.error)" -ForegroundColor Yellow }
        if ($status.result) { Write-Host "  résultat : $($status.result | ConvertTo-Json -Compress)" }
    }
} finally {
    Stop-PortForward $pf
}

# Laisse aux subscribers le temps de lire les derniers événements du stream.
Start-Sleep -Seconds 3

Write-Step 'Files RabbitMQ (celery_delayed_* masquées)'
$queues = Invoke-Rabbitmqctl -Command @('list_queues', 'name', 'type', 'messages', '--quiet', '--no-table-headers')
$rows = foreach ($line in $queues) {
    $parts = "$line" -split '\s+'
    if ($parts.Count -ge 3 -and $parts[0] -notlike 'celery_delayed_*') {
        [pscustomobject]@{ Nom = $parts[0]; Type = $parts[1]; Messages = [int]$parts[2] }
    }
}
$rows | Sort-Object Nom | Format-Table -AutoSize | Out-Host

foreach ($subscriber in 'analytics', 'audit', 'live') {
    Write-Step "Dernières lignes de subscriber-$subscriber (événements de commande)"
    # Les digests (un par minute) sont montrés à part, plus bas.
    @(Invoke-Kubectl -n $Namespace logs "deployment/subscriber-$subscriber" --tail=200) |
        Where-Object { "$_" -notmatch 'order\.digest' } | Select-Object -Last $LogLines | Out-Host
}

Write-Step 'Agrégats analytics (Redis, clé analytics:orders)'
$values = @(Invoke-RedisMaster -Command @('HGETALL', 'analytics:orders'))
for ($i = 0; $i + 1 -lt $values.Count; $i += 2) {
    $value = $values[$i + 1]
    if ($values[$i] -like 'revenue:*') { $value = '{0:N2}' -f [double]::Parse($value, [System.Globalization.CultureInfo]::InvariantCulture) }
    Write-Host ("  {0,-12} {1}" -f $values[$i], $value)
}
$processed = Invoke-RedisMaster -Command @('GET', 'stream:orders.events:offset:analytics')
Write-Host "  (dernier offset traité par analytics : $processed)"

Write-Step 'Cycle planifié : scheduler (Celery beat) -> notifications.sales_digest -> order.digest'
$sent = @(Invoke-Kubectl -n $Namespace logs deployment/scheduler --tail=50) |
    Where-Object { "$_" -match 'Sending due task' } | Select-Object -Last 1
$digest = @(Invoke-Kubectl -n $Namespace logs deployment/subscriber-live --tail=500) |
    Where-Object { "$_" -match 'LIVE order\.digest' } | Select-Object -Last 1
if ($sent) { Write-Host "  scheduler : $sent" } else { Write-Host '  scheduler : aucun envoi récent (premier tick à la prochaine minute)' -ForegroundColor Yellow }
if ($digest) { Write-Host "  live      : $digest" } else { Write-Host '  live      : pas encore de digest reçu (un par minute, DIGEST_CRON)' -ForegroundColor Yellow }
Write-Host ''
Write-Host 'Démonstration terminée.' -ForegroundColor Green
