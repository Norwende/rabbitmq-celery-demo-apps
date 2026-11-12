<#
.SYNOPSIS
  Fonctions communes aux scripts d'exploitation OrderFlow (à dot-sourcer).

.DESCRIPTION
  . "$PSScriptRoot/lib/common.ps1"

  Le script appelant définit $Context (contexte kubectl, '' = contexte courant),
  $Namespace (namespace des applications, défaut 'orderflow') et $InfraNamespace (namespace
  de RabbitMQ et Redis, défaut 'orderflow-infra'), en général comme paramètres -Namespace et
  -InfraNamespace. Compatible Windows PowerShell 5.1.
#>

if (-not $Namespace) { $Namespace = 'orderflow' }
if (-not $InfraNamespace) { $InfraNamespace = 'orderflow-infra' }
$Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)   # racine du dépôt
$FieldManager = 'orderflow-deploy'

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

function Invoke-KubectlStdin {
    # kubectl avec un texte sur l'entrée standard (manifeste contenant un secret, mot de passe
    # pour rabbitmqctl) : le secret ne passe jamais en argument (ni dans la liste des processus
    # du poste, ni dans l'URL d'un exec vers l'API server). Process .NET plutôt qu'un pipe
    # PowerShell : octets exacts (UTF-8 sans BOM, pas de fin de ligne ajoutée).
    # Ne lève pas d'exception : retourne { ExitCode, StdOut, StdErr }.
    param([string]$InputText, [string[]]$Arguments)
    $all = @(Get-ContextArgs) + $Arguments
    $quoted = foreach ($arg in $all) {
        if ($arg -match '[\s"]' -or $arg -eq '') { '"' + ($arg -replace '"', '\"') + '"' } else { $arg }
    }
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = 'kubectl'
    $psi.Arguments = $quoted -join ' '
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.RedirectStandardInput = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    # Le flux stdin du processus est créé avec Console.InputEncoding : en UTF-8 avec BOM, le
    # BOM (EF BB BF) précéderait le texte (constaté : il finissait dans le mot de passe).
    $previousEncoding = [Console]::InputEncoding
    try {
        [Console]::InputEncoding = New-Object System.Text.UTF8Encoding $false
        $process = [System.Diagnostics.Process]::Start($psi)
    } finally {
        [Console]::InputEncoding = $previousEncoding
    }
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    $bytes = (New-Object System.Text.UTF8Encoding $false).GetBytes($InputText)
    $process.StandardInput.BaseStream.Write($bytes, 0, $bytes.Length)
    $process.StandardInput.Close()
    $process.WaitForExit()
    return [pscustomobject]@{ ExitCode = $process.ExitCode; StdOut = $stdout.Result; StdErr = $stderr.Result }
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

function Test-HelmRelease([string]$Release, [string]$ReleaseNamespace) {
    # Vrai si la release existe dans le namespace (helm 4 liste tous les états par défaut).
    $releases = @(Invoke-Helm list -n $ReleaseNamespace --short)
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

# --- Namespaces et Secrets -------------------------------------------------------

function Confirm-Namespace([string]$Name) {
    # Crée le namespace s'il n'existe pas (idempotent).
    if (-not (Invoke-Kubectl get namespace $Name --ignore-not-found -o name)) {
        Invoke-Kubectl create namespace $Name | Out-Host
    }
}

function Get-SecretData([string]$SecretNamespace, [string]$Name) {
    # Données d'un Secret (hashtable clé -> valeur base64, jamais décodée ni affichée), ou $null.
    $json = Invoke-Kubectl -n $SecretNamespace get secret $Name --ignore-not-found -o json
    if (-not $json) { return $null }
    $secret = ($json -join "`n") | ConvertFrom-Json
    $data = @{}
    if ($secret.data) { foreach ($p in $secret.data.PSObject.Properties) { $data[$p.Name] = $p.Value } }
    return $data
}

function Set-SecretData([string]$SecretNamespace, [string]$Name, [hashtable]$Data) {
    # Crée ou met à jour un Secret Opaque (valeurs déjà en base64) par server-side apply, le
    # manifeste passant par l'entrée standard. Contenu identique => aucune écriture
    # (resourceVersion inchangé). Retourne 'créé', 'modifié' ou 'inchangé'.
    $before = Invoke-Kubectl -n $SecretNamespace get secret $Name --ignore-not-found -o 'jsonpath={.metadata.resourceVersion}'
    $manifest = [ordered]@{
        apiVersion = 'v1'
        kind       = 'Secret'
        type       = 'Opaque'
        metadata   = [ordered]@{
            name      = $Name
            namespace = $SecretNamespace
            labels    = @{ 'app.kubernetes.io/part-of' = 'orderflow'; 'app.kubernetes.io/managed-by' = $FieldManager }
        }
        data       = $Data
    }
    $result = Invoke-KubectlStdin -InputText ($manifest | ConvertTo-Json -Depth 5) `
        -Arguments @('apply', '--server-side', "--field-manager=$FieldManager", '--force-conflicts', '-f', '-')
    if ($result.ExitCode -ne 0) { throw "écriture de Secret/$Name (namespace $SecretNamespace) impossible : $($result.StdErr.Trim())" }
    $after = Invoke-Kubectl -n $SecretNamespace get secret $Name -o 'jsonpath={.metadata.resourceVersion}'
    if (-not $before) { return 'créé' }
    if ($before -ne $after) { return 'modifié' }
    return 'inchangé'
}

function New-RandomPassword([int]$Length = 32) {
    # Mot de passe alphanumérique tiré d'un générateur cryptographique (sans biais : rejet).
    $alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    $rng = New-Object System.Security.Cryptography.RNGCryptoServiceProvider
    $buffer = New-Object byte[] 1
    $chars = New-Object System.Text.StringBuilder
    while ($chars.Length -lt $Length) {
        $rng.GetBytes($buffer)
        if ($buffer[0] -lt 248) { [void]$chars.Append($alphabet[$buffer[0] % 62]) }   # 248 = 4 × 62
    }
    $rng.Dispose()
    return $chars.ToString()
}

function ConvertFrom-Base64Text([string]$Value) {
    return [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($Value))
}

function ConvertTo-Base64Text([string]$Value) {
    return [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($Value))
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
    # ces fichiers verrouillés jusqu'à la fin de la session. Par défaut : svc/gateway du
    # namespace des applications.
    param([string]$Target = 'svc/gateway', [int]$RemotePort = 8000, [string]$HealthPath = '/healthz', [string]$TargetNamespace = $Namespace)
    $port = Get-FreeTcpPort
    $kubectlArgs = @(Get-ContextArgs) + @('-n', $TargetNamespace, 'port-forward', $Target, "$($port):$RemotePort")
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
    throw "port-forward vers $Target indisponible sur le port $port (vérifier : kubectl -n $TargetNamespace get pods)"
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

# --- Redis / RabbitMQ (namespace d'infrastructure) -------------------------------

$RabbitmqPods = @('rabbitmq-server-1', 'rabbitmq-server-2', 'rabbitmq-server-0')

function Invoke-InFirstPod {
    # Exécute une commande dans le premier pod qui répond (les autres peuvent être en redémarrage).
    # Par défaut dans le namespace d'infrastructure (pods RabbitMQ et Redis).
    param([string[]]$Pods, [string]$Container, [string[]]$Command, [string]$PodNamespace = $InfraNamespace)
    $lastError = $null
    foreach ($pod in $Pods) {
        try {
            return Invoke-KubectlQuiet -n $PodNamespace exec $pod -c $Container '--' @Command
        } catch {
            $lastError = $_
        }
    }
    throw "commande impossible dans $($Pods -join ', ') (namespace $PodNamespace) : $lastError"
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
    return Invoke-InFirstPod -Pods $RabbitmqPods -Container 'rabbitmq' -Command (@('rabbitmqctl') + $Command)
}

function Invoke-RabbitmqctlStdin {
    # rabbitmqctl avec un secret sur l'entrée standard : add_user, change_password et
    # authenticate_user lisent le mot de passe sur stdin quand il est omis. Il n'apparaît
    # ainsi ni dans la ligne de commande du poste, ni dans la requête exec envoyée à l'API
    # server, ni dans les arguments d'un processus du pod. Premier pod joignable.
    # Retourne { ExitCode, StdOut, StdErr } sans lever d'exception sur un code non nul
    # (authenticate_user échoue normalement si le mot de passe diffère).
    param([string[]]$Command, [string]$Secret)
    $result = $null
    foreach ($pod in $RabbitmqPods) {
        $result = Invoke-KubectlStdin -InputText $Secret `
            -Arguments (@('-n', $InfraNamespace, 'exec', '-i', $pod, '-c', 'rabbitmq', '--', 'rabbitmqctl') + $Command)
        # Erreur de kubectl lui-même (pod absent, conteneur pas prêt : "error: …", "Error from
        # server …") : pod suivant. Les erreurs de rabbitmqctl commencent par "Error:" (casse).
        if ($result.StdErr -cnotmatch '(^|\n)(error: |Error from server)') { return $result }
    }
    return $result
}

function Get-StreamMessageCount([string]$Stream = 'orders.events') {
    $lines = Invoke-Rabbitmqctl -Command @('list_queues', 'name', 'messages', '--quiet', '--no-table-headers')
    foreach ($line in $lines) {
        $parts = "$line" -split '\s+'
        if ($parts[0] -eq $Stream) { return [int]$parts[1] }
    }
    throw "stream $Stream introuvable"
}

# --- Variante de RabbitMQ (avec ou sans opérateur) --------------------------------
# Les deux charts créent les mêmes objets (StatefulSet rabbitmq-server, Services rabbitmq et
# rabbitmq-nodes, Secret rabbitmq-default-user) : seuls le déploiement, l'attente de
# disponibilité et la désinstallation diffèrent.

$RabbitmqRelease = 'rabbitmq'
$RabbitmqCharts = @{ operator = 'rabbitmq'; standalone = 'rabbitmq-standalone' }

function Get-RabbitmqVariant {
    # Variante installée dans le namespace d'infrastructure : 'operator', 'standalone' ou ''
    # (rien). D'après le chart de la release Helm rabbitmq ; à défaut (objets créés hors
    # Helm), d'après les objets présents.
    $json = (Invoke-Helm list -n $InfraNamespace -o json) -join "`n"
    foreach ($release in @($json | ConvertFrom-Json)) {
        if ($release -and $release.name -eq $RabbitmqRelease) {
            if ("$($release.chart)" -like "$($RabbitmqCharts.standalone)-*") { return 'standalone' }
            return 'operator'
        }
    }
    if ((Invoke-Kubectl get crd rabbitmqclusters.rabbitmq.com --ignore-not-found -o name) -and
        (Invoke-Kubectl -n $InfraNamespace get rabbitmqcluster rabbitmq --ignore-not-found -o name)) { return 'operator' }
    if (Invoke-Kubectl -n $InfraNamespace get statefulset rabbitmq-server --ignore-not-found -o name) { return 'standalone' }
    return ''
}

function Get-RabbitmqVolumeVariant {
    # Variante qui a créé les PVC persistence-rabbitmq-server-* encore présents (données d'une
    # installation précédente) : 'operator', 'standalone' ou '' (aucun PVC). Les PVC du chart
    # rabbitmq-standalone portent le label app.kubernetes.io/instance, pas ceux de l'opérateur.
    $json = (Invoke-Kubectl -n $InfraNamespace get pvc -o json) -join "`n" | ConvertFrom-Json
    $pvcs = @($json.items | Where-Object { $_.metadata.name -like 'persistence-rabbitmq-server-*' })
    if ($pvcs.Count -eq 0) { return '' }
    if ($pvcs[0].metadata.labels.'app.kubernetes.io/instance') { return 'standalone' }
    return 'operator'
}

function Wait-RabbitmqCluster {
    # Attend que chaque nœud ait démarré (await_startup) et voie exactement $Replicas nœuds en
    # marche dans cluster_status : détecte aussi un cluster scindé (chaque partie ne se voit
    # qu'elle-même). Retourne { RunningNodes, Alarms, Partitions } vus par rabbitmq-server-0.
    param([int]$Replicas = 3, [int]$TimeoutSeconds = 600)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $state = 'aucune réponse'
    while ($true) {
        $views = @()
        try {
            foreach ($i in 0..($Replicas - 1)) {
                $pod = "rabbitmq-server-$i"
                Invoke-KubectlQuiet -n $InfraNamespace exec $pod -c rabbitmq '--' rabbitmqctl await_startup --timeout 10 | Out-Null
                $status = (Invoke-KubectlQuiet -n $InfraNamespace exec $pod -c rabbitmq '--' rabbitmqctl cluster_status --formatter json) -join "`n" | ConvertFrom-Json
                $views += [pscustomobject]@{
                    Pod          = $pod
                    RunningNodes = @($status.running_nodes)
                    Alarms       = @($status.alarms)
                    Partitions   = @($status.partitions.PSObject.Properties).Count
                }
            }
            $state = ($views | ForEach-Object { "$($_.Pod) voit $($_.RunningNodes.Count) nœud(s)" }) -join ', '
            if (@($views | Where-Object { $_.RunningNodes.Count -ne $Replicas }).Count -eq 0) { return $views[0] }
        } catch {
            $state = "$_"
        }
        if ((Get-Date) -gt $deadline) { throw "cluster RabbitMQ incomplet après $TimeoutSeconds s : $state" }
        Start-Sleep -Seconds 5
    }
}

function Wait-RabbitmqReady {
    # Disponibilité de RabbitMQ quelle que soit la variante : StatefulSet déployé, puis pour
    # l'opérateur les conditions du RabbitmqCluster, et dans tous les cas cluster_status
    # (tous les nœuds en marche, vus par chacun d'eux).
    param([int]$TimeoutSeconds = 600)
    $variant = Get-RabbitmqVariant
    if (-not $variant) { throw "RabbitMQ absent du namespace $InfraNamespace" }
    Invoke-Kubectl -n $InfraNamespace rollout status statefulset/rabbitmq-server "--timeout=$($TimeoutSeconds)s" | Out-Host
    if ($variant -eq 'operator') {
        Invoke-Kubectl -n $InfraNamespace wait --for=condition=AllReplicasReady rabbitmqcluster/rabbitmq "--timeout=$($TimeoutSeconds)s" | Out-Host
        Invoke-Kubectl -n $InfraNamespace wait --for=condition=ClusterAvailable rabbitmqcluster/rabbitmq "--timeout=$($TimeoutSeconds)s" | Out-Host
    }
    $replicas = [int](Invoke-Kubectl -n $InfraNamespace get statefulset rabbitmq-server -o 'jsonpath={.spec.replicas}')
    $view = Wait-RabbitmqCluster -Replicas $replicas -TimeoutSeconds $TimeoutSeconds
    $alarms = if ($view.Alarms.Count -gt 0) { ($view.Alarms | ConvertTo-Json -Compress) } else { 'aucune' }
    Write-Host ("RabbitMQ ({0}) : {1} nœuds en marche vus par chaque nœud, alarmes : {2}, partitions : {3}" -f $variant, $view.RunningNodes.Count, $alarms, $view.Partitions)
    return $variant
}

function Get-PodRestarts {
    # Hashtable "namespace/pod" -> somme des redémarrages de ses containers (applications et
    # infrastructure par défaut).
    param([string[]]$Namespaces = @($Namespace, $InfraNamespace))
    $restarts = @{}
    foreach ($ns in $Namespaces) {
        $json = (Invoke-Kubectl -n $ns get pods -o json) -join "`n" | ConvertFrom-Json
        foreach ($pod in $json.items) {
            $count = 0
            foreach ($status in @($pod.status.containerStatuses)) { if ($status) { $count += [int]$status.restartCount } }
            $restarts["$ns/$($pod.metadata.name)"] = $count
        }
    }
    return $restarts
}
