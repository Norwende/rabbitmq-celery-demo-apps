<#
.SYNOPSIS
  Builds the six OrderFlow application images ([<registry>/]orderflow/<service>:<tag>)
  with docker, from services/<service>; optionally pushes them to a registry.

.DESCRIPTION
  Every external source of the build can be redirected to an internal registry or mirror
  (restricted environments without Internet access):
    - base images: -BaseRegistry (prefix applied to both images, mirror layout
      <mirror>/library/python and <mirror>/astral-sh/uv) or the exact references
      -PythonImage / -UvImage (Dockerfile ARG PYTHON_IMAGE / UV_IMAGE);
    - Python packages: -PipIndexUrl, an internal PyPI mirror (Dockerfile ARG
      UV_DEFAULT_INDEX; uv.lock is re-resolved against it, the build fails if a pinned
      version is missing there);
    - target: -Registry tags the images <registry>/orderflow/<service>:<tag> (value
      image.registry of the orderflow chart), -Push pushes them (docker login beforehand).
  Without any of these, the build uses Docker Hub, ghcr.io and PyPI, as before.

.EXAMPLE
  ./scripts/build-images.ps1
  ./scripts/build-images.ps1 -Tag dev -Only gateway,subscriber
  ./scripts/build-images.ps1 -BaseRegistry registry.corp.example/dockerhub-proxy `
      -UvImage registry.corp.example/ghcr-proxy/astral-sh/uv:0.10.8 `
      -PipIndexUrl https://nexus.corp.example/repository/pypi/simple `
      -Registry registry.corp.example/orderflow-team -Tag 1.4.0 -Push
#>
[CmdletBinding()]
param(
    [string]$Tag = "dev",
    [string[]]$Only = @(),
    # Target registry (and path), e.g. registry.corp.example/team: images are tagged
    # <Registry>/orderflow/<service>:<Tag>. Empty: orderflow/<service>:<Tag> (local docker).
    [string]$Registry = "",
    [switch]$Push,
    # Mirror of Docker Hub / ghcr.io used for both base images (see DESCRIPTION).
    [string]$BaseRegistry = "",
    [string]$PythonImage = "",
    [string]$UvImage = "",
    # Internal PyPI mirror (simple API URL), passed to uv as UV_DEFAULT_INDEX.
    [string]$PipIndexUrl = ""
)

# Not "Stop": Windows PowerShell 5.1 turns docker's progress output (stderr) into errors
# when redirected; failures are detected with $LASTEXITCODE instead.
$ErrorActionPreference = "Continue"
$servicesDir = Resolve-Path (Join-Path $PSScriptRoot "..\services")
$services = @("gateway", "worker-payments", "worker-inventory", "worker-notifier", "subscriber", "scheduler")
if ($Push -and -not $Registry) { throw "-Push requires -Registry (target registry of the images)" }

# Defaults of the Dockerfiles (ARG): keep them in sync.
$defaultPython = "python:3.13-slim"
$defaultUv = "ghcr.io/astral-sh/uv:0.10.8"
if ($BaseRegistry) {
    $base = $BaseRegistry.TrimEnd('/')
    if (-not $PythonImage) { $PythonImage = "$base/library/$defaultPython" }
    if (-not $UvImage) { $UvImage = "$base/astral-sh/uv:0.10.8" }
}
$buildArgs = @()
if ($PythonImage) { $buildArgs += @("--build-arg", "PYTHON_IMAGE=$PythonImage") }
if ($UvImage) { $buildArgs += @("--build-arg", "UV_IMAGE=$UvImage") }
if ($PipIndexUrl) { $buildArgs += @("--build-arg", "UV_DEFAULT_INDEX=$PipIndexUrl") }
if ($buildArgs.Count -gt 0) {
    # The index URL may carry credentials: only its host is shown.
    $index = if ($PipIndexUrl) { ([uri]$PipIndexUrl).Host } else { "pypi.org" }
    Write-Host ("Base images: {0} / {1} ; package index: {2}" -f ($(if ($PythonImage) { $PythonImage } else { $defaultPython })),
        ($(if ($UvImage) { $UvImage } else { $defaultUv })), $index) -ForegroundColor DarkCyan
}

$prefix = if ($Registry) { "$($Registry.TrimEnd('/'))/orderflow" } else { "orderflow" }
$built = @()
foreach ($service in $services) {
    if ($Only.Count -gt 0 -and $Only -notcontains $service) { continue }
    $image = "${prefix}/${service}:$Tag"
    $context = Join-Path $servicesDir $service
    Write-Host "==> building $image from $context" -ForegroundColor Cyan
    docker build @buildArgs --tag $image $context
    if ($LASTEXITCODE -ne 0) { throw "docker build failed for $image" }
    $built += $image
}

if ($Push) {
    foreach ($image in $built) {
        Write-Host "==> pushing $image" -ForegroundColor Cyan
        docker push $image
        if ($LASTEXITCODE -ne 0) { throw "docker push failed for $image (docker login $($Registry.Split('/')[0]) ?)" }
    }
}

Write-Host "Images:" -ForegroundColor Green
docker images --filter "reference=${prefix}/*:$Tag" --format "  {{.Repository}}:{{.Tag}}  {{.Size}}"
