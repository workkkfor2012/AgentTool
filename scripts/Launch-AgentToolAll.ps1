param()

$ErrorActionPreference = "Stop"

function Resolve-ExistingPath([string]$pathValue) {
    if (-not (Test-Path -LiteralPath $pathValue)) {
        throw "Path not found: $pathValue"
    }
    return (Resolve-Path -LiteralPath $pathValue).Path
}

$visibleLauncher = Resolve-ExistingPath (Join-Path $PSScriptRoot "Launch-AgentToolVisibleLayout.ps1")

Write-Host "Launching AgentTool interactive local layout..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $visibleLauncher -ChildStartMode codex
if ($LASTEXITCODE -ne 0) {
    throw "Visible executor layout failed with exit code $LASTEXITCODE"
}

Write-Host "AgentTool default local layout launch completed."
