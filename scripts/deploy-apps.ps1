<#
.SYNOPSIS
  Déploie (de façon idempotente) les applications OrderFlow : identifiants dans le namespace
  des applications, build des images, puis helm upgrade --install de la release orderflow.

.DESCRIPTION
  1. identifiants (un Secret ne se référence pas d'un namespace à l'autre : les applications
     lisent deux Secrets de LEUR namespace, provisionnés ici avant le chart) :
       - RabbitMQ : utilisateur applicatif dédié "orderflow" (pas l'administrateur
         rabbitmq-default-user de l'opérateur), créé par rabbitmqctl dans un pod du cluster,
         sans tag d'administration, permissions configure/write/read ".*" sur le vhost /.
         Mot de passe aléatoire généré UNE fois : si Secret/rabbitmq-credentials existe déjà,
         son mot de passe est réutilisé et on vérifie seulement que l'utilisateur existe avec
         ce mot de passe (authenticate_user) ; une relance ne change donc rien. Le mot de passe
         passe par l'entrée standard (jamais en argument) et n'est jamais affiché ;
       - Redis : copie de la clé password de Secret/redis-auth (namespace d'infrastructure,
         ou le Secret désigné par redis auth.existingSecret, lu sur le StatefulSet) vers
         Secret/redis-credentials, resynchronisée à chaque exécution ;
     écritures par server-side apply : contenu identique => Secret inchangé. Si un identifiant
     change, le chart relance les pods concernés (annotation checksum/credentials, calculée
     par lookup des Secrets) ;
  2. construit les images orderflow/<service>:dev avec scripts/build-images.ps1
     (sauf -SkipBuild) ; k3s (Rancher Desktop, moby) partage le daemon docker ;
  3. calcule une empreinte du contenu de chaque image locale (couches + configuration, via
     docker image inspect) et la transmet au chart (imageIds.<image>) : le tag "dev" est
     mutable et imagePullPolicy=IfNotPresent, l'annotation checksum/image des pods change
     seulement si l'image a été reconstruite avec un contenu différent. Helm relance alors
     exactement les pods concernés, et un redéploiement sans changement ne recrée aucun pod.
     (Préféré à un « kubectl rollout restart » systématique : déclaratif, porté par la
     release, et sans redémarrage inutile.)
  4. helm upgrade --install orderflow charts/orderflow -n <Namespace> --wait
     (infraNamespace=<InfraNamespace> : adresses rabbitmq.<ns>.svc… et redis-sentinel.<ns>.svc…).

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
.PARAMETER Namespace
  Namespace des applications (défaut : orderflow).
.PARAMETER InfraNamespace
  Namespace de RabbitMQ et Redis (défaut : orderflow-infra).
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
    [string]$Namespace = 'orderflow',
    [string]$InfraNamespace = 'orderflow-infra',
    [string]$Context = '',
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/lib/common.ps1"
$Release = 'orderflow'
$Tag = 'dev'
# Noms attendus par le chart (values rabbitmq.secretName, redis.secretName).
$RabbitmqSecret = 'rabbitmq-credentials'
$RedisSecret = 'redis-credentials'
$AppUser = 'orderflow'
$images = @('gateway', 'worker-payments', 'worker-inventory', 'worker-notifier', 'subscriber', 'scheduler')
foreach ($service in $Only) {
    if ($images -notcontains $service) { throw "service inconnu : $service (attendu : $($images -join ', '))" }
}

function Sync-RabbitmqUser([string]$User, [string]$Password) {
    # Garantit que $User existe avec ce mot de passe, sans tag et avec les permissions
    # attendues sur le vhost /. N'écrit rien si tout est déjà conforme. Retourne un résumé.
    $done = @()
    $users = (Invoke-Rabbitmqctl -Command @('list_users', '--quiet', '--formatter', 'json')) -join "`n" | ConvertFrom-Json
    $existing = @($users | Where-Object { $_.user -eq $User })
    if ($existing.Count -eq 0) {
        $r = Invoke-RabbitmqctlStdin -Command @('add_user', $User) -Secret $Password
        if ($r.ExitCode -ne 0) { throw "rabbitmqctl add_user $User a échoué : $($r.StdErr.Trim())" }
        $done += 'utilisateur créé'
    } else {
        # authenticate_user : vérifie le mot de passe sans le modifier (aucune écriture si conforme).
        $r = Invoke-RabbitmqctlStdin -Command @('authenticate_user', $User) -Secret $Password
        if ($r.ExitCode -ne 0) {
            $r = Invoke-RabbitmqctlStdin -Command @('change_password', $User) -Secret $Password
            if ($r.ExitCode -ne 0) { throw "rabbitmqctl change_password $User a échoué : $($r.StdErr.Trim())" }
            $done += 'mot de passe resynchronisé'
        }
        if (@($existing[0].tags | Where-Object { $_ }).Count -gt 0) {
            # Aucun tag : ni administrator, ni management (pas d'accès à l'UI ni à l'API HTTP).
            Invoke-Rabbitmqctl -Command @('set_user_tags', $User) | Out-Null
            $done += 'tags retirés'
        }
    }
    $permissions = (Invoke-Rabbitmqctl -Command @('list_user_permissions', $User, '--quiet', '--formatter', 'json')) -join "`n" | ConvertFrom-Json
    $current = @($permissions | Where-Object { $_.vhost -eq '/' })
    if ($current.Count -eq 0 -or $current[0].configure -ne '.*' -or $current[0].write -ne '.*' -or $current[0].read -ne '.*') {
        Invoke-Rabbitmqctl -Command @('set_permissions', '-p', '/', $User, '.*', '.*', '.*') | Out-Null
        $done += 'permissions posées'
    }
    if ($done.Count -eq 0) { return 'inchangé' }
    return ($done -join ', ')
}

# --- 1. Identifiants ------------------------------------------------------------
Write-Step "Identifiants des applications (namespace $Namespace, depuis $InfraNamespace)"
if (-not (Invoke-Kubectl -n $InfraNamespace get statefulset redis --ignore-not-found -o name) -or
    -not (Invoke-Kubectl -n $InfraNamespace get statefulset rabbitmq-server --ignore-not-found -o name)) {
    # StatefulSet rabbitmq-server : même nom avec ou sans opérateur.
    throw "infrastructure absente du namespace $InfraNamespace : lancer d'abord scripts/deploy-infra.ps1"
}
Confirm-Namespace $Namespace

# Redis : Secret réellement monté par le StatefulSet (redis-auth, ou auth.existingSecret).
$redisAuth = Invoke-Kubectl -n $InfraNamespace get statefulset redis `
    -o "jsonpath={.spec.template.spec.containers[?(@.name=='redis')].env[?(@.name=='REDISCLI_AUTH')].valueFrom.secretKeyRef.name}"
if (-not $redisAuth) { $redisAuth = 'redis-auth' }
$redisData = Get-SecretData $InfraNamespace $redisAuth
if (-not $redisData -or -not $redisData['password']) { throw "Secret/$redisAuth (clé password) absent du namespace $InfraNamespace" }
$state = Set-SecretData $Namespace $RedisSecret @{ password = $redisData['password'] }
Write-Host ("  Secret/{0,-22} {1} (copie de {2}/{3})" -f $RedisSecret, $state, $InfraNamespace, $redisAuth)

# RabbitMQ : utilisateur dédié, mot de passe généré une fois puis réutilisé.
$rabbitData = Get-SecretData $Namespace $RabbitmqSecret
if ($rabbitData -and $rabbitData['password']) {
    $password = ConvertFrom-Base64Text $rabbitData['password']
    $origin = 'mot de passe existant réutilisé'
} else {
    $password = New-RandomPassword 32
    $origin = 'nouveau mot de passe'
}
# Utilisateur d'abord, Secret ensuite : un pod ne reçoit jamais l'identifiant d'un utilisateur
# inexistant. Si le Secret ne peut pas être écrit, la relance génère un autre mot de passe et
# resynchronise l'utilisateur (change_password).
$userState = Sync-RabbitmqUser -User $AppUser -Password $password
Write-Host ("  utilisateur RabbitMQ {0,-12} {1} ({2})" -f $AppUser, $userState, $origin)
$state = Set-SecretData $Namespace $RabbitmqSecret @{ username = (ConvertTo-Base64Text $AppUser); password = (ConvertTo-Base64Text $password) }
Write-Host ("  Secret/{0,-22} {1}" -f $RabbitmqSecret, $state)
Remove-Variable password

# --- 2. Images ------------------------------------------------------------------
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

# --- 3. Release Helm ------------------------------------------------------------
Write-Step "helm upgrade --install $Release (namespace $Namespace, infrastructure $InfraNamespace)"
# --force-conflicts : le chart fait foi. Sans lui, un champ modifié à la main (kubectl scale,
# set env…) appartient à un autre gestionnaire server-side apply et helm 4 refuse l'upgrade.
$helmArgs = @('upgrade', '--install', $Release, "$Root/charts/orderflow", '-n', $Namespace,
    '--create-namespace', '--wait', '--timeout', "$($TimeoutSeconds)s", '--force-conflicts',
    '--set-string', "infraNamespace=$InfraNamespace") + $imageArgs + (Get-HelmSetArgs $Values $Set)
Invoke-Helm @helmArgs | Out-Host

Write-Step "Etat final (namespace $Namespace)"
Invoke-Kubectl -n $Namespace get 'deployment,pod' | Out-Host
Write-Host ''
Write-Host 'Applications OrderFlow déployées.' -ForegroundColor Green
