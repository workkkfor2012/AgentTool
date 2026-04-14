param(
    [string]$AgentToolRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path,
    [string]$WorkspaceRoot = "F:\work\github\hackman",
    [string]$CodexLauncherPath = "",

    [ValidateSet("host", "shell", "codex", "view")]
    [string]$ChildStartMode = "codex",

    [ValidateSet("shell", "codex")]
    [string]$MainStartMode = "codex",

    [switch]$SkipAgentd,
    [switch]$SkipRegister,
    [switch]$SkipMainWindow,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Resolve-ExistingPath([string]$pathValue) {
    if (-not (Test-Path -LiteralPath $pathValue)) {
        throw "Path not found: $pathValue"
    }
    return (Resolve-Path -LiteralPath $pathValue).Path
}

function Invoke-NativeOrThrow([string]$filePath, [string[]]$arguments, [switch]$DiscardOutput, [string]$WorkingDirectory = "") {
    $resolvedWorkingDirectory = if ([string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        $null
    } else {
        $WorkingDirectory
    }

    try {
        if ($resolvedWorkingDirectory) {
            Push-Location -LiteralPath $resolvedWorkingDirectory
        }

        if ($DiscardOutput) {
            & $filePath @arguments *> $null
        } else {
            & $filePath @arguments
        }

        if ($LASTEXITCODE -ne 0) {
            throw "Command failed with exit code ${LASTEXITCODE}: $filePath $($arguments -join ' ')"
        }
    } finally {
        if ($resolvedWorkingDirectory) {
            Pop-Location
        }
    }
}

function Invoke-NativeAndGetExitCode([string]$filePath, [string[]]$arguments, [int]$TimeoutMilliseconds = 3000, [string]$WorkingDirectory = "") {
    $stdoutPath = Join-Path ([System.IO.Path]::GetTempPath()) ("agenttool-launch-{0}.out.log" -f ([guid]::NewGuid().ToString("N")))
    $stderrPath = Join-Path ([System.IO.Path]::GetTempPath()) ("agenttool-launch-{0}.err.log" -f ([guid]::NewGuid().ToString("N")))
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

function Get-LogTail([string]$pathValue, [int]$TailLines = 40) {
    if (-not (Test-Path -LiteralPath $pathValue)) {
        return ""
    }

    $lines = Get-Content -LiteralPath $pathValue -Tail $TailLines -Encoding UTF8 -ErrorAction SilentlyContinue
    if ($null -eq $lines) {
        return ""
    }

    return (($lines | ForEach-Object { [string]$_ }) -join [Environment]::NewLine).Trim()
}

function Test-AgentdOnline {
    Push-Location -LiteralPath $script:AgentToolRoot
    try {
        $previousNativePreference = $global:PSNativeCommandUseErrorActionPreference
        $global:PSNativeCommandUseErrorActionPreference = $false
        & $script:AgentCtlExe ping *> $null
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

function Ensure-Agentd {
    if ($SkipAgentd) {
        Write-Host "Skipping agentd startup because -SkipAgentd was provided."
        return
    }

    if (Test-AgentdOnline) {
        Write-Host "agentd is already online."
        return
    }

    if ($DryRun) {
        Write-Host "[dry-run] would start agentd: $script:AgentdExe"
        return
    }

    Write-Host "Starting agentd in the background..."
    $agentdOutLog = Join-Path $script:AgentToolRoot "agentd.out.log"
    $agentdErrLog = Join-Path $script:AgentToolRoot "agentd.err.log"
    $agentdProcess = Start-Process -FilePath $script:AgentdExe `
        -WorkingDirectory $script:AgentToolRoot `
        -RedirectStandardOutput $agentdOutLog `
        -RedirectStandardError $agentdErrLog `
        -PassThru

    if ($null -eq $agentdProcess) {
        throw "Failed to start agentd process."
    }

    for ($attempt = 0; $attempt -lt 40; $attempt++) {
        Start-Sleep -Milliseconds 250
        if ($agentdProcess.HasExited) {
            $stderrTail = Get-LogTail -pathValue $agentdErrLog -TailLines 60
            if ([string]::IsNullOrWhiteSpace($stderrTail)) {
                throw "agentd exited early with code $($agentdProcess.ExitCode)"
            }
            throw "agentd exited early with code $($agentdProcess.ExitCode):`n$stderrTail"
        }

        if (Test-AgentdOnline) {
            Write-Host "agentd is now online."
            return
        }
    }

    $stderrTail = Get-LogTail -pathValue $agentdErrLog -TailLines 60
    if ([string]::IsNullOrWhiteSpace($stderrTail)) {
        throw "agentd did not become ready after startup."
    }
    throw "agentd did not become ready after startup.`n$stderrTail"
}

function Register-ChildAgents {
    if ($SkipRegister) {
        Write-Host "Skipping child-agent registration because -SkipRegister was provided."
        return
    }

    foreach ($agent in $script:ChildAgents) {
        $arguments = @(
            "register-agent",
            "--name", $agent.Name,
            "--role", "child",
            "--cwd", $agent.Cwd,
            "--repo-name", $agent.RepoName,
            "--prompt-path", $agent.PromptPath
        )

        if ($DryRun) {
            Write-Host "[dry-run] $script:AgentCtlExe $($arguments -join ' ')"
            continue
        }

        Write-Host "Registering child agent $($agent.Name)..."
        Invoke-NativeOrThrow -filePath $script:AgentCtlExe -arguments $arguments -DiscardOutput -WorkingDirectory $script:AgentToolRoot
    }
}

function Ensure-ManagedChildSessions {
    if ($ChildStartMode -ne "view") {
        return
    }

    foreach ($agent in $script:ChildAgents) {
        $delaySeconds = if ($null -ne $agent.BootstrapInitialDelaySeconds) {
            [Math]::Max(0, [int]$agent.BootstrapInitialDelaySeconds)
        } else {
            0
        }
        $logDir = Join-Path $script:AgentToolRoot "data\launch_logs"
        $stdoutLog = Join-Path $logDir ("managed-bootstrap-{0}.out.log" -f $agent.Name)
        $stderrLog = Join-Path $logDir ("managed-bootstrap-{0}.err.log" -f $agent.Name)
        $arguments = @(
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-File", $script:ManagedBootstrapScript,
            "-AgentToolRoot", $script:AgentToolRoot,
            "-AgentName", $agent.Name,
            "-InitialDelaySeconds", [string]$delaySeconds
        )

        if ($agent.BootstrapPrompt) {
            $encodedBootstrapPrompt = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes([string]$agent.BootstrapPrompt))
            $arguments += @("-BootstrapPromptBase64", $encodedBootstrapPrompt)
        }

        if ($DryRun) {
            Write-Host "[dry-run] powershell.exe $($arguments -join ' ')"
            continue
        }

        if (-not (Test-Path -LiteralPath $logDir)) {
            New-Item -ItemType Directory -Path $logDir -Force | Out-Null
        }
        Remove-Item -LiteralPath $stdoutLog, $stderrLog -Force -ErrorAction SilentlyContinue

        Write-Host "Scheduling managed session bootstrap for child agent $($agent.Name) after ${delaySeconds}s..."
        Start-Process -FilePath "powershell.exe" `
            -ArgumentList $arguments `
            -WorkingDirectory $script:AgentToolRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput $stdoutLog `
            -RedirectStandardError $stderrLog | Out-Null
    }
}

function New-AgentWindowArgs($agent, [string]$startMode) {
    if ($startMode -eq "view") {
        return @(
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NoExit",
            "-ExecutionPolicy", "Bypass",
            "-File", $script:EnterAgentViewScript,
            "-AgentName", $agent.Name,
            "-Title", $agent.Title,
            "-AgentToolRoot", $script:AgentToolRoot
        )
    }

    $arguments = @(
        "powershell.exe",
        "-NoLogo",
        "-NoProfile",
        "-NoExit",
        "-ExecutionPolicy", "Bypass",
        "-File", $script:EnterAgentShellScript,
        "-AgentName", $agent.Name,
        "-Title", $agent.Title,
        "-Cwd", $agent.Cwd,
        "-StartMode", $startMode
    )

    if ($agent.PromptPath) {
        $arguments += @("-PromptPath", $agent.PromptPath)
    }

    if ($agent.ReasoningEffort) {
        $arguments += @("-ReasoningEffort", $agent.ReasoningEffort)
    }

    if ($null -ne $agent.BootstrapInitialDelaySeconds) {
        $arguments += @("-BootstrapInitialDelaySeconds", [string]$agent.BootstrapInitialDelaySeconds)
    }

    if (($startMode -eq "codex" -or $startMode -eq "shell") -and $agent.BootstrapPrompt) {
        $arguments += @("-BootstrapPrompt", $agent.BootstrapPrompt)
    }

    if (-not [string]::IsNullOrWhiteSpace($script:CodexLauncherPath)) {
        $arguments += @("-CodexLauncherPath", $script:CodexLauncherPath)
    }

    return $arguments
}

function Format-CommandLine([string[]]$parts) {
    return ($parts | ForEach-Object {
            if ($_ -match '\s|;') {
                '"' + ($_ -replace '"', '\"') + '"'
            } else {
                $_
            }
        }) -join " "
}

function Join-CodePoints([int[]]$points, [string]$suffix = "") {
    return (-join ($points | ForEach-Object { [char]$_ })) + $suffix
}

function Start-WindowsTerminalProcess([string[]]$arguments) {
    $argumentLine = Format-CommandLine $arguments
    $process = Start-Process -FilePath "wt.exe" -ArgumentList $argumentLine -PassThru
    if ($null -eq $process) {
        throw "Failed to start wt.exe."
    }
}

function Invoke-WindowsTerminalCommand([string]$windowId, [string[]]$commandArguments, [int]$delayMs = 350) {
    $arguments = @("-w", $windowId) + $commandArguments

    if ($DryRun) {
        Write-Host "[dry-run] wt.exe $(Format-CommandLine $arguments)"
        return
    }

    Start-WindowsTerminalProcess $arguments
    if ($delayMs -gt 0) {
        Start-Sleep -Milliseconds $delayMs
    }
}

function Start-ChildAgentWindow {
    $windowId = "agenttool-children-$PID"

    Write-Host "Opening 2x2 child-agent Windows Terminal layout..."
    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments (
        @(
            "new-tab",
            "--title", $script:ChildAgents[0].Title,
            "-d", $script:ChildAgents[0].Cwd
        ) + (New-AgentWindowArgs $script:ChildAgents[0] $script:ChildStartMode)
    ) -delayMs 700

    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments (
        @(
            "split-pane",
            "-V",
            "-s", "0.5",
            "--title", $script:ChildAgents[1].Title,
            "-d", $script:ChildAgents[1].Cwd
        ) + (New-AgentWindowArgs $script:ChildAgents[1] $script:ChildStartMode)
    ) -delayMs 500

    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments @("move-focus", "left") -delayMs 250

    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments (
        @(
            "split-pane",
            "-H",
            "-s", "0.5",
            "--title", $script:ChildAgents[2].Title,
            "-d", $script:ChildAgents[2].Cwd
        ) + (New-AgentWindowArgs $script:ChildAgents[2] $script:ChildStartMode)
    ) -delayMs 500

    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments @("move-focus", "right") -delayMs 250

    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments (
        @(
            "split-pane",
            "-H",
            "-s", "0.5",
            "--title", $script:ChildAgents[3].Title,
            "-d", $script:ChildAgents[3].Cwd
        ) + (New-AgentWindowArgs $script:ChildAgents[3] $script:ChildStartMode)
    ) -delayMs 250
}

function Start-MainAgentWindow {
    $windowId = "agenttool-main-$PID"

    Write-Host "Opening main-agent window..."
    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments (
        @(
            "new-tab",
            "--title", $script:MainAgent.Title,
            "-d", $script:MainAgent.Cwd
        ) + (New-AgentWindowArgs $script:MainAgent $script:MainStartMode)
    ) -delayMs 0
}

$script:AgentToolRoot = Resolve-ExistingPath $AgentToolRoot
$script:WorkspaceRoot = Resolve-ExistingPath $WorkspaceRoot
$script:AgentdExe = Resolve-ExistingPath (Join-Path $script:AgentToolRoot "target\debug\agentd.exe")
$script:AgentCtlExe = Resolve-ExistingPath (Join-Path $script:AgentToolRoot "target\debug\agentctl.exe")
$script:EnterAgentShellScript = Resolve-ExistingPath (Join-Path $PSScriptRoot "Enter-AgentShell.ps1")
$script:EnterAgentViewScript = Resolve-ExistingPath (Join-Path $PSScriptRoot "Enter-AgentView.ps1")
$script:ManagedBootstrapScript = Resolve-ExistingPath (Join-Path $PSScriptRoot "Start-ManagedSessionBootstrap.ps1")
$defaultCustomLauncher = "F:\Users\schu\bin\mycodex.bat"
$script:CodexLauncherPath = if (-not [string]::IsNullOrWhiteSpace($CodexLauncherPath)) {
    Resolve-ExistingPath $CodexLauncherPath
} elseif (Test-Path -LiteralPath $defaultCustomLauncher) {
    Resolve-ExistingPath $defaultCustomLauncher
} else {
    ""
}
$env:AGENTTOOL_ROOT = $script:AgentToolRoot
$env:AGENTTOOL_DATA_DIR = Join-Path $script:AgentToolRoot "data"
$env:AGENTTOOL_RUNTIME_ENDPOINT_PATH = Join-Path $env:AGENTTOOL_DATA_DIR "runtime_endpoint.json"

if (-not (Get-Command wt.exe -ErrorAction SilentlyContinue)) {
    throw "wt.exe was not found. Install Windows Terminal or ensure wt.exe is in PATH."
}

$mainTitle = Join-CodePoints @(0x4E3B) "Agent"
$backendCloudTitle = Join-CodePoints @(0x670D, 0x52A1, 0x5668, 0x540E, 0x7AEF)
$backendControlTitle = Join-CodePoints @(0x670D, 0x52A1, 0x5668, 0x524D, 0x7AEF)
$factoryTitle = Join-CodePoints @(0x5B69, 0x5B50, 0x88AB, 0x63A7, 0x5236, 0x7AEF)
$controlTitle = Join-CodePoints @(0x5BB6, 0x957F, 0x63A7, 0x5236, 0x7AEF)
$utf8ReadRule = (Join-CodePoints @(0x4EE5,0x4E0A,0x0020,0x004D,0x0061,0x0072,0x006B,0x0064,0x006F,0x0077,0x006E,0x0020,0x63D0,0x793A,0x8BCD,0x4E0E,0x5951,0x7EA6,0x6587,0x6863,0x4E00,0x5F8B,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x7F16,0x7801,0x8BFB,0x53D6,0xFF1B,0x5982,0x679C,0x9996,0x8BFB,0x51FA,0x73B0,0x4E71,0x7801,0xFF0C,0x7ACB,0x5373,0x663E,0x5F0F,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x91CD,0x65B0,0x8BFB,0x53D6,0xFF0C,0x4E0D,0x8981,0x4F7F,0x7528,0x0020,0x0044,0x0065,0x0066,0x0061,0x0075,0x006C,0x0074,0x0020,0x6216,0x0020,0x0055,0x006E,0x0069,0x0063,0x006F,0x0064,0x0065,0x3002))
$mainBootstrapPrompt = (Join-CodePoints @(0x5148,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x8BFB,0x53D6,0x5F53,0x524D,0x5DE5,0x4F5C,0x533A,0x4E2D,0x7684)) + " MAIN_AGENT_PROMPT.md" + (Join-CodePoints @(0xFF0C,0x5E76,0x5C06,0x5176,0x89C6,0x4E3A,0x4F60,0x5F53,0x524D,0x7684,0x89D2,0x8272,0x7EA6,0x675F,0x3002,0x6682,0x65F6,0x4E0D,0x8981,0x5F00,0x59CB,0x5DE5,0x4F5C,0xFF0C,0x5148,0x603B,0x7ED3,0x81EA,0x5DF1,0x7684,0x5DE5,0x4F5C,0x8FDB,0x5EA6,0xFF0C,0x4E0D,0x8981,0x5F00,0x59CB,0x65B0,0x7684,0x5DE5,0x4F5C,0xFF0C,0x7B49,0x5F85,0x4E0B,0x4E00,0x6761,0x64CD,0x4F5C,0x6D88,0x606F,0x3002)) + $utf8ReadRule
$childBootstrapPrompt = (Join-CodePoints @(0x5148,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x8BFB,0x53D6,0x5F53,0x524D,0x5DE5,0x4F5C,0x533A,0x4E2D,0x7684)) + " SUBAGENT_PROMPT.md" + (Join-CodePoints @(0xFF0C,0x5E76,0x5C06,0x5176,0x89C6,0x4E3A,0x4F60,0x5F53,0x524D,0x7684,0x89D2,0x8272,0x7EA6,0x675F,0x3002,0x6682,0x65F6,0x4E0D,0x8981,0x5F00,0x59CB,0x5DE5,0x4F5C,0xFF0C,0x5148,0x603B,0x7ED3,0x81EA,0x5DF1,0x7684,0x5DE5,0x4F5C,0x8FDB,0x5EA6,0xFF0C,0x4E0D,0x8981,0x5F00,0x59CB,0x65B0,0x7684,0x5DE5,0x4F5C,0xFF0C,0x7B49,0x5F85,0x4E0B,0x4E00,0x6761,0x64CD,0x4F5C,0x6D88,0x606F,0x6216,0x4E3B,0x0020,0x0061,0x0067,0x0065,0x006E,0x0074,0x0020,0x6D88,0x606F,0x3002)) + $utf8ReadRule

$script:MainAgent = @{
    Name = "main"
    Title = $mainTitle
    Cwd = $script:WorkspaceRoot
    PromptPath = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "MAIN_AGENT_PROMPT.md")
    ReasoningEffort = "medium"
    BootstrapInitialDelaySeconds = 0
    BootstrapPrompt = $mainBootstrapPrompt
}

$script:ChildAgents = @(
    @{
        Name = "guardpro_backend_cloud"
        Title = $backendCloudTitle
        RepoName = "guardpro_backend_cloud"
        Cwd = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "guardpro_backend_cloud")
        PromptPath = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "guardpro_backend_cloud\SUBAGENT_PROMPT.md")
        ReasoningEffort = "high"
        BootstrapInitialDelaySeconds = 0
        BootstrapPrompt = $childBootstrapPrompt
    },
    @{
        Name = "guardpro_backend_control"
        Title = $backendControlTitle
        RepoName = "guardpro_backend_control"
        Cwd = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "guardpro_backend_control")
        PromptPath = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "guardpro_backend_control\SUBAGENT_PROMPT.md")
        ReasoningEffort = "high"
        BootstrapInitialDelaySeconds = 0
        BootstrapPrompt = $childBootstrapPrompt
    },
    @{
        Name = "guardpro_factory"
        Title = $factoryTitle
        RepoName = "guardpro_factory"
        Cwd = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "guardpro_factory")
        PromptPath = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "guardpro_factory\SUBAGENT_PROMPT.md")
        ReasoningEffort = "high"
        BootstrapInitialDelaySeconds = 0
        BootstrapPrompt = $childBootstrapPrompt
    },
    @{
        Name = "guardpro_control"
        Title = $controlTitle
        RepoName = "guardpro_control"
        Cwd = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "guardpro_control")
        PromptPath = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "guardpro_control\SUBAGENT_PROMPT.md")
        ReasoningEffort = "high"
        BootstrapInitialDelaySeconds = 0
        BootstrapPrompt = $childBootstrapPrompt
    }
)

Write-Host "AgentTool root : $script:AgentToolRoot"
Write-Host "Workspace root : $script:WorkspaceRoot"
Write-Host "Child mode     : $ChildStartMode"
Write-Host "Main mode      : $MainStartMode"
if (-not [string]::IsNullOrWhiteSpace($script:CodexLauncherPath)) {
    Write-Host "Codex launcher : $script:CodexLauncherPath"
}
Write-Host "Dry run        : $DryRun"

Ensure-Agentd
Register-ChildAgents
Start-ChildAgentWindow
if (-not $SkipMainWindow) {
    Start-Sleep -Milliseconds 400
    Start-MainAgentWindow
} else {
    Write-Host "Skipping main-agent window because -SkipMainWindow was provided."
}
Ensure-ManagedChildSessions

Write-Host "Visible agent layout launch flow completed."
