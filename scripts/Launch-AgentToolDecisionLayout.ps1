param(
    [string]$AgentToolRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path,
    [string]$WorkspaceRoot = "F:\work\github\hackman",
    [string]$CodexLauncherPath = "",

    [ValidateSet("shell", "codex")]
    [string]$OrchestratorStartMode = "codex",

    [ValidateSet("shell", "codex", "view")]
    [string]$AdvisorStartMode = "codex",

    [switch]$SkipAgentd,
    [switch]$SkipRegister,
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

function Register-AdvisorAgents {
    if ($SkipRegister) {
        Write-Host "Skipping advisor-agent registration because -SkipRegister was provided."
        return
    }

    foreach ($agent in $script:AdvisorAgents) {
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

        Write-Host "Registering advisor agent $($agent.Name)..."
        Invoke-NativeOrThrow -filePath $script:AgentCtlExe -arguments $arguments -DiscardOutput -WorkingDirectory $script:AgentToolRoot
    }
}

function Ensure-ManagedAdvisorSessions {
    if ($AdvisorStartMode -ne "view") {
        return
    }

    foreach ($agent in $script:AdvisorAgents) {
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

        Write-Host "Scheduling managed session bootstrap for advisor agent $($agent.Name) after ${delaySeconds}s..."
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

function Start-DecisionWindow {
    $windowId = "agenttool-decision-$PID"

    Write-Host "Opening orchestrator + advisor decision layout..."
    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments (
        @(
            "new-tab",
            "--title", $script:OrchestratorAgent.Title,
            "-d", $script:OrchestratorAgent.Cwd
        ) + (New-AgentWindowArgs $script:OrchestratorAgent $script:OrchestratorStartMode)
    ) -delayMs 700

    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments (
        @(
            "split-pane",
            "-V",
            "-s", "0.68",
            "--title", $script:AdvisorAgents[0].Title,
            "-d", $script:AdvisorAgents[0].Cwd
        ) + (New-AgentWindowArgs $script:AdvisorAgents[0] $script:AdvisorStartMode)
    ) -delayMs 500

    Invoke-WindowsTerminalCommand -windowId $windowId -commandArguments (
        @(
            "split-pane",
            "-H",
            "-s", "0.5",
            "--title", $script:AdvisorAgents[1].Title,
            "-d", $script:AdvisorAgents[1].Cwd
        ) + (New-AgentWindowArgs $script:AdvisorAgents[1] $script:AdvisorStartMode)
    ) -delayMs 250
}

$script:AgentToolRoot = Resolve-ExistingPath $AgentToolRoot
$script:WorkspaceRoot = Resolve-ExistingPath $WorkspaceRoot
$script:AgentdExe = Resolve-ExistingPath (Join-Path $script:AgentToolRoot "target\debug\agentd.exe")
$script:AgentCtlExe = Resolve-ExistingPath (Join-Path $script:AgentToolRoot "target\debug\agentctl.exe")
$script:EnterAgentShellScript = Resolve-ExistingPath (Join-Path $PSScriptRoot "Enter-AgentShell.ps1")
$script:EnterAgentViewScript = Resolve-ExistingPath (Join-Path $PSScriptRoot "Enter-AgentView.ps1")
$script:ManagedBootstrapScript = Resolve-ExistingPath (Join-Path $PSScriptRoot "Start-ManagedSessionBootstrap.ps1")
$script:RoleContractPath = Resolve-ExistingPath (Join-Path $script:AgentToolRoot "ROLE_CONTRACT.md")
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

$mainPromptPath = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "MAIN_AGENT_PROMPT.md")
$advisorHighPromptPath = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "THINKING_ADVISOR_HIGH_PROMPT.md")
$advisorXhighPromptPath = Resolve-ExistingPath (Join-Path $script:WorkspaceRoot "THINKING_ADVISOR_XHIGH_PROMPT.md")

$mainTitle = Join-CodePoints @(0x4E3B,0x41,0x67,0x65,0x6E,0x74)
$advisorHighTitle = Join-CodePoints @(0x5FEB,0x901F,0x601D,0x8003)
$advisorXhighTitle = Join-CodePoints @(0x6DF1,0x5EA6,0x601D,0x8003)
$utf8ReadRule =
    (Join-CodePoints @(0x4EE5,0x4E0A,0x0020,0x004D,0x0061,0x0072,0x006B,0x0064,0x006F,0x0077,0x006E,0x0020,0x63D0,0x793A,0x8BCD,0x4E0E,0x5951,0x7EA6,0x6587,0x6863,0x4E00,0x5F8B,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x7F16,0x7801,0x8BFB,0x53D6,0xFF1B,0x5982,0x679C,0x9996,0x8BFB,0x51FA,0x73B0,0x4E71,0x7801,0xFF0C,0x7ACB,0x5373,0x663E,0x5F0F,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x91CD,0x65B0,0x8BFB,0x53D6,0xFF0C,0x4E0D,0x8981,0x4F7F,0x7528,0x0020,0x0044,0x0065,0x0066,0x0061,0x0075,0x006C,0x0074,0x0020,0x6216,0x0020,0x0055,0x006E,0x0069,0x0063,0x006F,0x0064,0x0065,0x3002))
$mainBootstrapPrompt =
    (Join-CodePoints @(0x5148,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x8BFB,0x53D6,0x5F53,0x524D,0x5DE5,0x4F5C,0x533A,0x4E2D,0x7684,0x20)) +
    "MAIN_AGENT_PROMPT.md" +
    (Join-CodePoints @(0xFF0C,0x518D,0x8BFB,0x53D6,0x20)) +
    $script:RoleContractPath +
    (Join-CodePoints @(0x20,0x4E2D,0x7684,0x89D2,0x8272,0x5951,0x7EA6,0x4E0E,0x4EFB,0x52A1,0x6D41,0x89C4,0x5219,0xFF0C,0x5E76,0x5C06,0x5176,0x89C6,0x4E3A,0x5F53,0x524D,0x591A,0x20,0x61,0x67,0x65,0x6E,0x74,0x20,0x534F,0x4F5C,0x7EA6,0x675F,0x3002,0x6682,0x65F6,0x4E0D,0x8981,0x5F00,0x59CB,0x5DE5,0x4F5C,0xFF0C,0x5148,0x603B,0x7ED3,0x81EA,0x5DF1,0x7684,0x5DE5,0x4F5C,0x8FDB,0x5EA6,0x548C,0x5F53,0x524D,0x5F85,0x5904,0x7406,0x961F,0x5217,0xFF0C,0x4E0D,0x8981,0x6D3E,0x53D1,0x65B0,0x4EFB,0x52A1,0xFF0C,0x7B49,0x5F85,0x4E0B,0x4E00,0x6761,0x64CD,0x4F5C,0x6D88,0x606F,0x3002)) +
    $utf8ReadRule
$advisorHighBootstrapPrompt =
    (Join-CodePoints @(0x5148,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x8BFB,0x53D6,0x5F53,0x524D,0x5DE5,0x4F5C,0x533A,0x4E2D,0x7684,0x20)) +
    "THINKING_ADVISOR_HIGH_PROMPT.md" +
    (Join-CodePoints @(0xFF0C,0x5E76,0x5C06,0x5176,0x89C6,0x4E3A,0x4F60,0x7684,0x89D2,0x8272,0x5951,0x7EA6,0x3002,0x6682,0x65F6,0x4E0D,0x8981,0x5F00,0x59CB,0x5DE5,0x4F5C,0xFF0C,0x5148,0x603B,0x7ED3,0x81EA,0x5DF1,0x5F53,0x524D,0x80FD,0x63D0,0x4F9B,0x7684,0x5206,0x6790,0x8FB9,0x754C,0x4E0E,0x8F93,0x5165,0x9700,0x6C42,0xFF0C,0x4E0D,0x8981,0x4E3B,0x52A8,0x5206,0x6790,0x4E1A,0x52A1,0x4EFB,0x52A1,0xFF0C,0x7B49,0x5F85,0x4E3B,0x20,0x61,0x67,0x65,0x6E,0x74,0x20,0x63D0,0x95EE,0x3002)) +
    $utf8ReadRule
$advisorXhighBootstrapPrompt =
    (Join-CodePoints @(0x5148,0x6309,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x8BFB,0x53D6,0x5F53,0x524D,0x5DE5,0x4F5C,0x533A,0x4E2D,0x7684,0x20)) +
    "THINKING_ADVISOR_XHIGH_PROMPT.md" +
    (Join-CodePoints @(0xFF0C,0x5E76,0x5C06,0x5176,0x89C6,0x4E3A,0x4F60,0x7684,0x89D2,0x8272,0x5951,0x7EA6,0x3002,0x6682,0x65F6,0x4E0D,0x8981,0x5F00,0x59CB,0x5DE5,0x4F5C,0xFF0C,0x5148,0x603B,0x7ED3,0x81EA,0x5DF1,0x5F53,0x524D,0x80FD,0x63D0,0x4F9B,0x7684,0x5206,0x6790,0x8FB9,0x754C,0x4E0E,0x8F93,0x5165,0x9700,0x6C42,0xFF0C,0x4E0D,0x8981,0x4E3B,0x52A8,0x5206,0x6790,0x4E1A,0x52A1,0x4EFB,0x52A1,0xFF0C,0x7B49,0x5F85,0x4E3B,0x20,0x61,0x67,0x65,0x6E,0x74,0x20,0x63D0,0x95EE,0x3002)) +
    $utf8ReadRule

$script:OrchestratorAgent = @{
    Name = "main"
    Title = $mainTitle
    Cwd = $script:WorkspaceRoot
    PromptPath = $mainPromptPath
    ReasoningEffort = "medium"
    BootstrapInitialDelaySeconds = 0
    BootstrapPrompt = $mainBootstrapPrompt
}

$script:AdvisorAgents = @(
    @{
        Name = "advisor_high"
        Title = $advisorHighTitle
        RepoName = "advisor_high"
        Cwd = $script:WorkspaceRoot
        PromptPath = $advisorHighPromptPath
        ReasoningEffort = "high"
        BootstrapInitialDelaySeconds = 0
        BootstrapPrompt = $advisorHighBootstrapPrompt
    },
    @{
        Name = "advisor_xhigh"
        Title = $advisorXhighTitle
        RepoName = "advisor_xhigh"
        Cwd = $script:WorkspaceRoot
        PromptPath = $advisorXhighPromptPath
        ReasoningEffort = "xhigh"
        BootstrapInitialDelaySeconds = 0
        BootstrapPrompt = $advisorXhighBootstrapPrompt
    }
)

Write-Host "AgentTool root      : $script:AgentToolRoot"
Write-Host "Workspace root      : $script:WorkspaceRoot"
Write-Host "Orchestrator mode   : $OrchestratorStartMode"
Write-Host "Advisor mode        : $AdvisorStartMode"
if (-not [string]::IsNullOrWhiteSpace($script:CodexLauncherPath)) {
    Write-Host "Codex launcher      : $script:CodexLauncherPath"
}
Write-Host "Dry run             : $DryRun"

Ensure-Agentd
Register-AdvisorAgents
Start-DecisionWindow
Ensure-ManagedAdvisorSessions

Write-Host "Decision layout launch flow completed."
