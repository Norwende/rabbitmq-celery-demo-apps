<#
.SYNOPSIS
  Fonctions communes aux scripts d'exploitation OrderFlow (à dot-sourcer).

.DESCRIPTION
  . "$PSScriptRoot/lib/common.ps1"

  Le script appelant définit $Context (contexte kubectl, '' = contexte courant) ;
  $Namespace vaut 'orderflow'. Compatible Windows PowerShell 5.1.
#>

$Namespace = 'orderflow'
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)   # racine du dépôt

function Get-ContextArgs {
    if ($Context) { return @('--context', $Context) }
    return @()
}

function Invoke-Kubectl {
    # Exécute kubectl et lève une exception si le code retour est non nul.
    # Fonction "simple" (sans param()) : tous les arguments (-n, --flag=...) passent par $args.
    # Attention : PowerShell consomme un -- nu, il faut l'écrire '--'.
    $all = @(Get-ContextArgs) + $args
    # PowerShell 5.1 transforme le stderr d'un exécutable natif en erreur (fatale avec
    # ErrorActionPreference=Stop), même pour un simple warning : seul le code retour compte.
    $ErrorActionPreference = 'Continue'
    $out = & kubectl @all 2>&1 | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) { Write-Host $_.Exception.Message -ForegroundColor DarkYellow } else { $_ }
    }
    if ($LASTEXITCODE -ne 0) { throw "kubectl $($args -join ' ') a échoué (code $LASTEXITCODE)" }
    return $out
}

function Invoke-KubectlQuiet {
    # Comme Invoke-Kubectl, mais le stderr est ignoré (sondes, essais successifs).
    $all = @(Get-ContextArgs) + $args
    $ErrorActionPreference = 'Continue'
    $out = & kubectl @all 2>$null
    if ($LASTEXITCODE -ne 0) { throw "kubectl $($args -join ' ') a échoué (code $LASTEXITCODE)" }
    return $out
}

function Invoke-Helm {
    # Comme Invoke-Kubectl, pour helm (--kube-context). Le message de l'exception reprend les
    # dernières lignes d'erreur de helm (permet à l'appelant de décider d'un nouvel essai).
    $all = @()
    if ($Context) { $all += @('--kube-context', $Context) }
    $all += $args
    $ErrorActionPreference = 'Continue'
    $errors = New-Object System.Collections.ArrayList
    $out = & helm @all 2>&1 | ForEach-Object {
        if ($_ -is [System.Management.Automation.ErrorRecord]) {
            $message = $_.Exception.Message
            [void]$errors.Add($message)
            Write-Host $message -ForegroundColor DarkYellow
        } else { $_ }
    }
    if ($LASTEXITCODE -ne 0) {
        $detail = ($errors | Select-Object -Last 3) -join ' | '
        throw "helm $($args -join ' ') a échoué (code $LASTEXITCODE) : $detail"
    }
    return $out
}

function Test-HelmRelease([string]$Release) {
    # Vrai si la release existe dans le namespace (helm 4 liste tous les états par défaut).
    $releases = @(Invoke-Helm list -n $Namespace --short)
    return ($releases -contains $Release)
}

function Get-HelmSetArgs([string[]]$Values, [string[]]$Set) {
    # Paramètres -Values (fichiers) et -Set des scripts de déploiement -> arguments helm.
    $helmArgs = @()
    foreach ($file in $Values) { $helmArgs += @('-f', (Resolve-Path $file).Path) }
    foreach ($item in $Set) { $helmArgs += @('--set', $item) }
    return $helmArgs
}

function Write-Step([string]$Message) {
    Write-Host ''
    Write-Host "==> $Message" -ForegroundColor Cyan
}

# --- Port-forward temporaire -----------------------------------------------------

function Get-FreeTcpPort {
    $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
    $listener.Stop()
    return $port
}

function Start-PortForward {
    # Ouvre "kubectl port-forward" en arrière-plan (fenêtre cachée) sur un port local libre
    # et attend qu'il réponde. Pas de redirection vers des fichiers : PowerShell 5.1 garde
    # ces fichiers verrouillés jusqu'à la fin de la session.
    param([string]$Target = 'svc/gateway', [int]$RemotePort = 8000, [string]$HealthPath = '/healthz')
    $port = Get-FreeTcpPort
    $kubectlArgs = @(Get-ContextArgs) + @('-n', $Namespace, 'port-forward', $Target, "$($port):$RemotePort")
    $process = Start-Process -FilePath 'kubectl' -ArgumentList $kubectlArgs -PassThru -WindowStyle Hidden
    $pf = [pscustomobject]@{ Process = $process; Port = $port; Url = "http://127.0.0.1:$port" }
    $deadline = (Get-Date).AddSeconds(30)
    while ((Get-Date) -lt $deadline) {
        if ($process.HasExited) { break }
        try {
            Invoke-WebRequest -Uri "$($pf.Url)$HealthPath" -UseBasicParsing -TimeoutSec 3 | Out-Null
            return $pf
        } catch {
            Start-Sleep -Milliseconds 500
        }
    }
    Stop-PortForward $pf
    throw "port-forward vers $Target indisponible sur le port $port (vérifier : kubectl -n $Namespace get pods)"
}

function Stop-PortForward($PortForward) {
    if ($null -eq $PortForward) { return }
    if (-not $PortForward.Process.HasExited) {
        Stop-Process -Id $PortForward.Process.Id -Force -ErrorAction SilentlyContinue
        $PortForward.Process.WaitForExit(5000) | Out-Null
    }
}

# --- HTTP ------------------------------------------------------------------------

function Invoke-Api {
    # Appel HTTP qui ne lève pas d'exception sur un code non 2xx.
    # Retourne { Status (0 = pas de réponse), Json, Text, RetryAfter }.
    param([string]$Method, [string]$Url, $Body = $null, [int]$TimeoutSec = 30)
    $params = @{ Method = $Method; Uri = $Url; UseBasicParsing = $true; TimeoutSec = $TimeoutSec }
    if ($null -ne $Body) {
        $params.Body = [System.Text.Encoding]::UTF8.GetBytes(($Body | ConvertTo-Json -Depth 10 -Compress))
        $params.ContentType = 'application/json; charset=utf-8'
    }
    $status = 0; $text = ''; $retryAfter = $null
    try {
        $response = Invoke-WebRequest @params
        $status = [int]$response.StatusCode
        $text = $response.Content
    } catch [System.Net.WebException] {
        $response = $_.Exception.Response
        if ($null -eq $response) {
            $text = $_.Exception.Message
        } else {
            $status = [int]$response.StatusCode
            $retryAfter = $response.Headers['Retry-After']
            # PowerShell 5.1 a déjà lu le corps de la réponse d'erreur : il est dans ErrorDetails.
            if ($_.ErrorDetails) { $text = $_.ErrorDetails.Message }
        }
    }
    $json = $null
    if ($text) { try { $json = $text | ConvertFrom-Json } catch { $json = $null } }
    return [pscustomobject]@{ Status = $status; Json = $json; Text = $text; RetryAfter = $retryAfter }
}

function Wait-OrderTerminal {
    # Interroge GET /orders/{id} jusqu'à SUCCESS ou FAILURE (ou expiration).
    param([string]$BaseUrl, [string]$OrderId, [int]$TimeoutSeconds = 60)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $last = $null
    while ((Get-Date) -lt $deadline) {
        $last = Invoke-Api -Method GET -Url "$BaseUrl/orders/$OrderId"
        if ($last.Status -eq 200 -and @('SUCCESS', 'FAILURE') -contains $last.Json.status) { return $last.Json }
        Start-Sleep -Milliseconds 500
    }
    if ($last -and $last.Json -and $last.Json.status) { return $last.Json }
    return [pscustomobject]@{ order_id = $OrderId; status = "INCONNU (HTTP $($last.Status))"; steps = @() }
}

# --- Redis / RabbitMQ ------------------------------------------------------------

function Invoke-InFirstPod {
    # Exécute une commande dans le premier pod qui répond (les autres peuvent être en redémarrage).
    param([string[]]$Pods, [string]$Container, [string[]]$Command)
    $lastError = $null
    foreach ($pod in $Pods) {
        try {
            return Invoke-KubectlQuiet -n $Namespace exec $pod -c $Container '--' @Command
        } catch {
            $lastError = $_
        }
    }
    throw "commande impossible dans $($Pods -join ', ') : $lastError"
}

function Get-RedisMasterPod {
    # Nom du pod master selon Sentinel (REDISCLI_AUTH est déjà défini dans le container sentinel).
    $out = Invoke-InFirstPod -Pods @('redis-0', 'redis-1', 'redis-2') -Container 'sentinel' `
        -Command @('redis-cli', '--no-auth-warning', '-p', '26379', 'SENTINEL', 'get-master-addr-by-name', 'orderflow')
    $hostName = @($out)[0]
    if (-not $hostName) { throw 'Sentinel ne désigne aucun master' }
    return ($hostName -split '\.')[0]
}

function Get-RedisTopology {
    # Vue de Sentinel : pod master, réplicas jugés sains (flags = slave) et autres sentinels.
    param([int]$Replicas = 3)
    $pods = @(0..($Replicas - 1) | ForEach-Object { "redis-$_" })
    $countFlags = {
        param($Lines, [string]$Flag)
        $lines = @($Lines); $n = 0
        for ($i = 0; $i + 1 -lt $lines.Count; $i++) { if ($lines[$i] -eq 'flags' -and $lines[$i + 1] -eq $Flag) { $n++ } }
        return $n
    }
    $cli = @('redis-cli', '--no-auth-warning', '-p', '26379', 'SENTINEL')
    $replicaLines = Invoke-InFirstPod -Pods $pods -Container 'sentinel' -Command ($cli + @('replicas', 'orderflow'))
    $sentinelLines = Invoke-InFirstPod -Pods $pods -Container 'sentinel' -Command ($cli + @('sentinels', 'orderflow'))
    return [pscustomobject]@{
        Master    = Get-RedisMasterPod
        Replicas  = (& $countFlags $replicaLines 'slave')
        Sentinels = 1 + (& $countFlags $sentinelLines 'sentinel')   # + le sentinel interrogé
    }
}

function Wait-RedisTopology {
    # Attend 1 master, Replicas-1 réplicas sains et Replicas sentinels (état nominal).
    param([int]$Replicas = 3, [int]$TimeoutSeconds = 300)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $topology = $null
    while ((Get-Date) -lt $deadline) {
        try {
            $topology = Get-RedisTopology -Replicas $Replicas
            if ($topology.Replicas -eq ($Replicas - 1) -and $topology.Sentinels -eq $Replicas) { return $topology }
        } catch { }
        Start-Sleep -Seconds 3
    }
    throw "Redis Sentinel incomplet après $TimeoutSeconds s : $($topology | Out-String)"
}

function Invoke-RedisMaster {
    # redis-cli exécuté sur le master courant (REDISCLI_AUTH est défini dans le container redis).
    param([string[]]$Command)
    $master = Get-RedisMasterPod
    return Invoke-InFirstPod -Pods @($master) -Container 'redis' -Command (@('redis-cli', '--no-auth-warning') + $Command)
}

function Invoke-Rabbitmqctl {
    param([string[]]$Command)
    return Invoke-InFirstPod -Pods @('rabbitmq-server-1', 'rabbitmq-server-2', 'rabbitmq-server-0') -Container 'rabbitmq' `
        -Command (@('rabbitmqctl') + $Command)
}

function Get-StreamMessageCount([string]$Stream = 'orders.events') {
    $lines = Invoke-Rabbitmqctl -Command @('list_queues', 'name', 'messages', '--quiet', '--no-table-headers')
    foreach ($line in $lines) {
        $parts = "$line" -split '\s+'
        if ($parts[0] -eq $Stream) { return [int]$parts[1] }
    }
    throw "stream $Stream introuvable"
}

function Get-PodRestarts {
    # Hashtable nom du pod -> somme des redémarrages de ses containers.
    $json = (Invoke-Kubectl -n $Namespace get pods -o json) -join "`n" | ConvertFrom-Json
    $restarts = @{}
    foreach ($pod in $json.items) {
        $count = 0
        foreach ($status in @($pod.status.containerStatuses)) { if ($status) { $count += [int]$status.restartCount } }
        $restarts[$pod.metadata.name] = $count
    }
    return $restarts
}
