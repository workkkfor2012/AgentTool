param(
    [Parameter(Mandatory = $true)]
    [string]$AgentName,

    [Parameter(Mandatory = $true)]
    [string]$Title,

    [string]$AgentToolRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path,
    [int]$ReconnectSeconds = 3,
    [switch]$ShowStreams
)

$ErrorActionPreference = "Stop"

function Resolve-ExistingPath([string]$pathValue) {
    if (-not (Test-Path -LiteralPath $pathValue)) {
        throw "Path not found: $pathValue"
    }
    return (Resolve-Path -LiteralPath $pathValue).Path
}

function Set-AgentVisiblePaneRegistration {
    param(
        [switch]$Clear
    )

    if (-not $env:AGENTTOOL_CTL) {
        return
    }

    $arguments = @("set-agent-visible-pane", "--agent", $AgentName)
    if (-not $Clear) {
        $arguments += @("--pid", [string]$PID, "--kind", "view")
    }

    & $env:AGENTTOOL_CTL @arguments *> $null
    if ($LASTEXITCODE -ne 0) {
        if ($Clear) {
            Write-Warning "agentctl failed to clear visible pane registration for $AgentName"
        } else {
            Write-Warning "agentctl failed to register visible pane for $AgentName"
        }
    }
}

function Register-AgentVisiblePaneExitHandler {
    if ($global:AgentToolVisiblePaneExitEvent) {
        return
    }

    $global:AgentToolVisiblePaneExitEvent = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
        try {
            if ($env:AGENTTOOL_CTL -and $env:AGENTTOOL_AGENT_NAME) {
                & $env:AGENTTOOL_CTL set-agent-visible-pane --agent $env:AGENTTOOL_AGENT_NAME *> $null
            }
        } catch {
        }
    }
}

$resolvedRoot = Resolve-ExistingPath $AgentToolRoot
$agentCtlExe = Resolve-ExistingPath (Join-Path $resolvedRoot "target\debug\agentctl.exe")
$agentWatchExe = Resolve-ExistingPath (Join-Path $resolvedRoot "target\debug\agentwatch.exe")
$env:AGENTTOOL_ROOT = $resolvedRoot
$env:AGENTTOOL_DATA_DIR = Join-Path $resolvedRoot "data"
$env:AGENTTOOL_RUNTIME_ENDPOINT_PATH = Join-Path $env:AGENTTOOL_DATA_DIR "runtime_endpoint.json"
$env:AGENTTOOL_CTL = $agentCtlExe
$env:AGENTTOOL_AGENT_NAME = $AgentName

Set-Location -LiteralPath $resolvedRoot

Write-Host "========================================================================"
Write-Host "AgentView  : $AgentName"
Write-Host "Title      : $Title"
Write-Host "Root       : $resolvedRoot"
Write-Host "Mode       : read-only"
Write-Host "========================================================================"

Register-AgentVisiblePaneExitHandler
Set-AgentVisiblePaneRegistration

$arguments = @(
    "--agent", $AgentName,
    "--title", $Title,
    "--reconnect-seconds", [string]([Math]::Max(1, $ReconnectSeconds))
)

if ($ShowStreams) {
    $arguments += "--show-streams"
}

& $agentWatchExe @arguments
if ($LASTEXITCODE -ne 0) {
    Write-Warning "agentwatch exited with code $LASTEXITCODE"
}
