param(
    [string]$AgentToolRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path,
    [switch]$ClearLogs,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Resolve-ExistingPath([string]$pathValue) {
    if (-not (Test-Path -LiteralPath $pathValue)) {
        throw "Path not found: $pathValue"
    }
    return (Resolve-Path -LiteralPath $pathValue).Path
}

function Test-CaseInsensitiveEquals([string]$left, [string]$right) {
    if ([string]::IsNullOrWhiteSpace($left) -or [string]::IsNullOrWhiteSpace($right)) {
        return $false
    }

    return [string]::Equals($left, $right, [System.StringComparison]::OrdinalIgnoreCase)
}

function Invoke-NativeAndGetExitCode([string]$filePath, [string[]]$arguments, [int]$TimeoutMilliseconds = 3000, [string]$WorkingDirectory = "") {
    $stdoutPath = Join-Path ([System.IO.Path]::GetTempPath()) ("agenttool-stop-{0}.out.log" -f ([guid]::NewGuid().ToString("N")))
    $stderrPath = Join-Path ([System.IO.Path]::GetTempPath()) ("agenttool-stop-{0}.err.log" -f ([guid]::NewGuid().ToString("N")))
    $resolvedWorkingDirectory = if ([string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        (Get-Location).Path
    } else {
        $WorkingDirectory
    }

    try {
        $process = Start-Process -FilePath $filePath `
            -ArgumentList $arguments `
            -PassThru `
            -WindowStyle Hidden `
            -WorkingDirectory $resolvedWorkingDirectory `
            -RedirectStandardOutput $stdoutPath `
            -RedirectStandardError $stderrPath

        if (-not $process.WaitForExit([Math]::Max(250, $TimeoutMilliseconds))) {
            try {
                Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
            } catch {
            }
            return 124
        }

        return $process.ExitCode
    } finally {
        Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

function Test-CommandLineLike([string]$commandLine, [string[]]$patterns) {
    if ([string]::IsNullOrWhiteSpace($commandLine)) {
        return $false
    }

    foreach ($pattern in $patterns) {
        if ($commandLine.IndexOf($pattern, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
            return $true
        }
    }

    return $false
}

function Test-AgentdOnline([string]$agentCtlPath) {
    if ([string]::IsNullOrWhiteSpace($agentCtlPath) -or -not (Test-Path -LiteralPath $agentCtlPath)) {
        return $false
    }

    Push-Location -LiteralPath $resolvedRoot
    try {
        $previousNativePreference = $global:PSNativeCommandUseErrorActionPreference
        $global:PSNativeCommandUseErrorActionPreference = $false
        & $agentCtlPath ping *> $null
        return ($LASTEXITCODE -eq 0)
    } catch {
        return $false
    } finally {
        if ($null -eq $previousNativePreference) {
            Remove-Variable -Name PSNativeCommandUseErrorActionPreference -Scope Global -ErrorAction SilentlyContinue
        } else {
            $global:PSNativeCommandUseErrorActionPreference = $previousNativePreference
        }
        Pop-Location
    }
}

function Invoke-StopManagedSessions([string]$agentCtlPath) {
    if ([string]::IsNullOrWhiteSpace($agentCtlPath) -or -not (Test-Path -LiteralPath $agentCtlPath)) {
        return $false
    }

    if ($DryRun) {
        Write-Host "[dry-run] $agentCtlPath stop-managed-sessions"
        return $true
    }

    Write-Host "Requesting agentd to stop all live daemon-managed sessions..."
    & $agentCtlPath stop-managed-sessions
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "agentctl stop-managed-sessions failed with exit code $LASTEXITCODE"
        return $false
    }

    Start-Sleep -Milliseconds 800
    return $true
}

function Invoke-StopVisiblePanes([string]$agentCtlPath) {
    if ([string]::IsNullOrWhiteSpace($agentCtlPath) -or -not (Test-Path -LiteralPath $agentCtlPath)) {
        return $false
    }

    if ($DryRun) {
        Write-Host "[dry-run] $agentCtlPath stop-visible-panes"
        return $true
    }

    Write-Host "Requesting agentd to stop all registered visible panes..."
    & $agentCtlPath stop-visible-panes
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "agentctl stop-visible-panes failed with exit code $LASTEXITCODE"
        return $false
    }

    Start-Sleep -Milliseconds 800
    return $true
}

function Stop-ProcessTree([uint32]$ProcessId, [string]$Name, [string]$Reason) {
    $label = "{0} (PID={1}) [{2}]" -f $Name, $ProcessId, $Reason
    if ($DryRun) {
        Write-Host "[dry-run] taskkill /PID $ProcessId /T /F    # $label"
        return
    }

    Write-Host "Stopping $label"
    $previousNativePreference = $global:PSNativeCommandUseErrorActionPreference
    try {
        $global:PSNativeCommandUseErrorActionPreference = $false
        & taskkill.exe /PID $ProcessId /T /F *> $null
        $exitCode = $LASTEXITCODE
    } finally {
        if ($null -eq $previousNativePreference) {
            Remove-Variable -Name PSNativeCommandUseErrorActionPreference -Scope Global -ErrorAction SilentlyContinue
        } else {
            $global:PSNativeCommandUseErrorActionPreference = $previousNativePreference
        }
    }
    if ($exitCode -ne 0 -and $exitCode -ne 128 -and $exitCode -ne 255) {
        Write-Warning "taskkill returned exit code $exitCode for $label"
    }
}

function Remove-LogFiles([string[]]$paths) {
    foreach ($path in $paths) {
        if (-not (Test-Path -LiteralPath $path)) {
            continue
        }

        if ($DryRun) {
            Write-Host "[dry-run] remove $path"
            continue
        }

        Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        Write-Host "Removed $path"
    }
}

function Get-RuntimeEndpointPorts([string]$runtimeEndpointPath) {
    if ([string]::IsNullOrWhiteSpace($runtimeEndpointPath) -or -not (Test-Path -LiteralPath $runtimeEndpointPath)) {
        return @()
    }

    try {
        $endpoint = Get-Content -LiteralPath $runtimeEndpointPath -Raw -Encoding UTF8 | ConvertFrom-Json
    } catch {
        return @()
    }

    $ports = New-Object System.Collections.Generic.List[int]
    foreach ($addr in @($endpoint.ws_addr, $endpoint.control_addr)) {
        $text = [string]$addr
        if ([string]::IsNullOrWhiteSpace($text)) {
            continue
        }

        if ($text -match ':(\d+)\s*$') {
            $ports.Add([int]$Matches[1]) | Out-Null
        }
    }

    return @($ports | Sort-Object -Unique)
}

function Get-AgentToolPortSet([string]$runtimeEndpointPath) {
    $ports = New-Object System.Collections.Generic.List[int]
    foreach ($port in @(7080, 7081)) {
        $ports.Add([int]$port) | Out-Null
    }

    foreach ($port in (Get-RuntimeEndpointPorts -runtimeEndpointPath $runtimeEndpointPath)) {
        $ports.Add([int]$port) | Out-Null
    }

    return @($ports | Sort-Object -Unique)
}

function Get-OccupiedAgentToolPorts([string]$runtimeEndpointPath) {
    $ports = Get-AgentToolPortSet -runtimeEndpointPath $runtimeEndpointPath
    return @(Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $ports -contains $_.LocalPort } |
        Sort-Object LocalPort, OwningProcess)
}

function Clear-RuntimeEndpointArtifacts([string]$runtimeEndpointPath, [string]$dashboardRuntimeScriptPath) {
    if ($DryRun) {
        if (Test-Path -LiteralPath $runtimeEndpointPath) {
            Write-Host "[dry-run] remove $runtimeEndpointPath"
        }
        if (Test-Path -LiteralPath $dashboardRuntimeScriptPath) {
            Write-Host "[dry-run] reset $dashboardRuntimeScriptPath"
        }
        return
    }

    if (Test-Path -LiteralPath $runtimeEndpointPath) {
        Remove-Item -LiteralPath $runtimeEndpointPath -Force -ErrorAction SilentlyContinue
        Write-Host "Removed $runtimeEndpointPath"
    }

    if (-not [string]::IsNullOrWhiteSpace($dashboardRuntimeScriptPath)) {
        $parent = Split-Path -Parent $dashboardRuntimeScriptPath
        if (-not [string]::IsNullOrWhiteSpace($parent) -and -not (Test-Path -LiteralPath $parent)) {
            New-Item -ItemType Directory -Path $parent -Force | Out-Null
        }

        Set-Content -LiteralPath $dashboardRuntimeScriptPath -Value 'window.__AGENTTOOL_RUNTIME_ENDPOINT__ = null;' -Encoding UTF8
        Write-Host "Reset $dashboardRuntimeScriptPath"
    }
}

$resolvedRoot = Resolve-ExistingPath $AgentToolRoot
$env:AGENTTOOL_ROOT = $resolvedRoot
$env:AGENTTOOL_DATA_DIR = Join-Path $resolvedRoot "data"
$env:AGENTTOOL_RUNTIME_ENDPOINT_PATH = Join-Path $env:AGENTTOOL_DATA_DIR "runtime_endpoint.json"
$targetDebug = Join-Path $resolvedRoot "target\debug"
$agentdExe = Join-Path $targetDebug "agentd.exe"
$agentctlExe = Join-Path $targetDebug "agentctl.exe"
$agentwatchExe = Join-Path $targetDebug "agentwatch.exe"
$agenthostExe = Join-Path $targetDebug "agenthost.exe"
$launchLogDir = Join-Path $resolvedRoot "data\launch_logs"
$runtimeEndpointPath = Join-Path $resolvedRoot "data\runtime_endpoint.json"
$dashboardRuntimeScriptPath = Join-Path $resolvedRoot "dashboard\runtime-endpoint.js"
$agentdOutLog = Join-Path $resolvedRoot "agentd.out.log"
$agentdErrLog = Join-Path $resolvedRoot "agentd.err.log"
$altAgentdOutLog = Join-Path $resolvedRoot "agentd.alt.out.log"
$altAgentdErrLog = Join-Path $resolvedRoot "agentd.alt.err.log"

$paneScriptMarkers = @(
    "Enter-AgentShell.ps1",
    "Enter-AgentView.ps1"
)

$otherScriptMarkers = @(
    "Start-ManagedSessionBootstrap.ps1",
    "Launch-AgentToolVisibleLayout.ps1",
    "Launch-AgentToolDecisionLayout.ps1",
    "Launch-AgentToolAll.ps1",
    "Launch-AgentToolVisibleLayout.cmd",
    "Launch-AgentToolDecisionLayout.cmd",
    "Launch-AgentToolAll.cmd"
)

$currentProcessId = [uint32]$PID
$agentdOnline = Test-AgentdOnline -agentCtlPath $agentctlExe
$managedSessionsStoppedByAgentd = $false
$visiblePanesStoppedByAgentd = $false

if ($agentdOnline) {
    $managedSessionsStoppedByAgentd = Invoke-StopManagedSessions -agentCtlPath $agentctlExe
    $visiblePanesStoppedByAgentd = Invoke-StopVisiblePanes -agentCtlPath $agentctlExe
}

$processes = Get-CimInstance Win32_Process | Where-Object { $_.ProcessId -ne $currentProcessId }
$targets = New-Object System.Collections.Generic.List[object]

foreach ($process in $processes) {
    $name = [string]$process.Name
    $commandLine = [string]$process.CommandLine
    $executablePath = [string]$process.ExecutablePath
    $reason = $null

    if (Test-CaseInsensitiveEquals $executablePath $agentdExe) {
        $reason = "agentd"
    } elseif (Test-CaseInsensitiveEquals $executablePath $agentctlExe) {
        $reason = "agentctl"
    } elseif (Test-CaseInsensitiveEquals $executablePath $agentwatchExe) {
        $reason = "agentwatch"
    } elseif (Test-CaseInsensitiveEquals $executablePath $agenthostExe) {
        $reason = "agenthost"
    } elseif (
        (-not $visiblePanesStoppedByAgentd) -and
        ($name -match '^(powershell|pwsh|cmd)\.exe$') -and
        (Test-CommandLineLike $commandLine $paneScriptMarkers)
    ) {
        $reason = "agenttool-visible-pane"
    } elseif (($name -match '^(powershell|pwsh|cmd)\.exe$') -and (Test-CommandLineLike $commandLine $otherScriptMarkers)) {
        $reason = "agenttool-script-shell"
    } elseif (
        (-not $managedSessionsStoppedByAgentd) -and
        ($name -eq "cmd.exe") -and
        (Test-CommandLineLike $commandLine @("mycodex.bat")) -and
        (Test-CommandLineLike $commandLine @(" app-server ", " app-server --listen ")) -and
        (Test-CommandLineLike $commandLine @("agenttool-appserver-"))
    ) {
        $reason = "managed-codex-app-server"
    }

    if ($null -eq $reason) {
        continue
    }

    $targets.Add([pscustomobject]@{
            ProcessId = [uint32]$process.ProcessId
            Name = $name
            Reason = $reason
            CommandLine = $commandLine
        })
}

$orderedTargets = $targets |
    Sort-Object Reason, Name, ProcessId -Unique

Write-Host "========================================================================"
Write-Host "AgentTool stop scope"
Write-Host "Root      : $resolvedRoot"
Write-Host "Dry run   : $DryRun"
Write-Host "ClearLogs : $ClearLogs"
Write-Host "Matched   : $($orderedTargets.Count)"
Write-Host "========================================================================"

foreach ($target in $orderedTargets) {
    Write-Host ("- {0} PID={1} reason={2}" -f $target.Name, $target.ProcessId, $target.Reason)
}

foreach ($target in ($orderedTargets | Sort-Object ProcessId -Descending)) {
    Stop-ProcessTree -ProcessId $target.ProcessId -Name $target.Name -Reason $target.Reason
}

$remainingPortListeners = @()
if (-not $DryRun) {
    Start-Sleep -Milliseconds 800
    $remainingPortListeners = Get-OccupiedAgentToolPorts -runtimeEndpointPath $runtimeEndpointPath
    if (-not (Test-AgentdOnline -agentCtlPath $agentctlExe)) {
        Clear-RuntimeEndpointArtifacts -runtimeEndpointPath $runtimeEndpointPath -dashboardRuntimeScriptPath $dashboardRuntimeScriptPath
    }
}

if ($ClearLogs) {
    $logPaths = @(
        $agentdOutLog,
        $agentdErrLog,
        $altAgentdOutLog,
        $altAgentdErrLog
    )

    if (Test-Path -LiteralPath $launchLogDir) {
        $logPaths += Get-ChildItem -LiteralPath $launchLogDir -File -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty FullName
    }

    Remove-LogFiles -paths $logPaths
}

Write-Host "========================================================================"
if ($DryRun) {
    Write-Host "Dry run completed. No process was terminated."
} else {
    Write-Host "AgentTool cleanup completed."
    if ($remainingPortListeners.Count -gt 0) {
        $observedPorts = (Get-AgentToolPortSet -runtimeEndpointPath $runtimeEndpointPath) -join ", "
        Write-Warning "Observed AgentTool ports are still occupied after cleanup. These listeners are outside the managed AgentTool process set or are not exposed as normal killable processes. Ports checked: $observedPorts"
        foreach ($listener in $remainingPortListeners) {
            Write-Warning ("Port {0} still LISTEN by PID {1}" -f $listener.LocalPort, $listener.OwningProcess)
        }
    }
}
Write-Host "========================================================================"
