<#
.SYNOPSIS
  Builds the six OrderFlow application images (orderflow/<service>:<tag>) with docker.
  Each image is built from services/<service>.
.EXAMPLE
  ./scripts/build-images.ps1
  ./scripts/build-images.ps1 -Tag dev -Only gateway,subscriber
#>
[CmdletBinding()]
param(
    [string]$Tag = "dev",
    [string[]]$Only = @()
)

# Not "Stop": Windows PowerShell 5.1 turns docker's progress output (stderr) into errors
# when redirected; failures are detected with $LASTEXITCODE instead.
$ErrorActionPreference = "Continue"
$servicesDir = Resolve-Path (Join-Path $PSScriptRoot "..\services")
$services = @("gateway", "worker-payments", "worker-inventory", "worker-notifier", "subscriber", "scheduler")

foreach ($service in $services) {
    if ($Only.Count -gt 0 -and $Only -notcontains $service) { continue }
    $image = "orderflow/${service}:$Tag"
    $context = Join-Path $servicesDir $service
    Write-Host "==> building $image from $context" -ForegroundColor Cyan
    docker build --tag $image $context
    if ($LASTEXITCODE -ne 0) { throw "docker build failed for $image" }
}

Write-Host "Images:" -ForegroundColor Green
docker images --filter "reference=orderflow/*:$Tag" --format "  {{.Repository}}:{{.Tag}}  {{.Size}}"
