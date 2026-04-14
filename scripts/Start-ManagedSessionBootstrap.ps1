param(
    [Parameter(Mandatory = $true)]
    [string]$AgentToolRoot,

    [Parameter(Mandatory = $true)]
    [string]$AgentName,

    [int]$InitialDelaySeconds = 0,
    [string]$BootstrapPrompt = "",
    [string]$BootstrapPromptBase64 = ""
)

$ErrorActionPreference = "Stop"

function Resolve-ExistingPath([string]$pathValue) {
    if (-not (Test-Path -LiteralPath $pathValue)) {
        throw "Path not found: $pathValue"
    }
    return (Resolve-Path -LiteralPath $pathValue).Path
}

$resolvedRoot = Resolve-ExistingPath $AgentToolRoot
$agentCtlExe = Resolve-ExistingPath (Join-Path $resolvedRoot "target\debug\agentctl.exe")
$env:AGENTTOOL_ROOT = $resolvedRoot
$env:AGENTTOOL_DATA_DIR = Join-Path $resolvedRoot "data"
$env:AGENTTOOL_RUNTIME_ENDPOINT_PATH = Join-Path $env:AGENTTOOL_DATA_DIR "runtime_endpoint.json"

Set-Location -LiteralPath $resolvedRoot

if ($InitialDelaySeconds -gt 0) {
    Write-Host ("Managed bootstrap for {0}: waiting {1}s before ensure-managed-session..." -f $AgentName, $InitialDelaySeconds)
    Start-Sleep -Seconds $InitialDelaySeconds
}

$arguments = @(
    "ensure-managed-session",
    "--agent", $AgentName
)

$bootstrapPromptText = ""
if (-not [string]::IsNullOrWhiteSpace($BootstrapPromptBase64)) {
    try {
        $bootstrapPromptBytes = [System.Convert]::FromBase64String($BootstrapPromptBase64)
        $bootstrapPromptText = [System.Text.Encoding]::UTF8.GetString($bootstrapPromptBytes).Trim()
    } catch {
        throw "Failed to decode -BootstrapPromptBase64 for $AgentName"
    }
} else {
    $bootstrapPromptText = [string]$BootstrapPrompt
    if (-not [string]::IsNullOrWhiteSpace($bootstrapPromptText)) {
        $bootstrapPromptText = $bootstrapPromptText.Trim()
    }
}

if (-not [string]::IsNullOrWhiteSpace($bootstrapPromptText)) {
    $arguments += @("--bootstrap-prompt", $bootstrapPromptText)
}

Write-Host ("Managed bootstrap for {0}: starting ensure-managed-session." -f $AgentName)
& $agentCtlExe @arguments
if ($LASTEXITCODE -ne 0) {
    throw "ensure-managed-session failed for $AgentName with exit code $LASTEXITCODE"
}

Write-Host ("Managed bootstrap for {0}: ready." -f $AgentName)
