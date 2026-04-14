param(
    [Parameter(Mandatory = $true)]
    [string]$AgentName,

    [Parameter(Mandatory = $true)]
    [string]$Title,

    [Parameter(Mandatory = $true)]
    [string]$Cwd,

    [ValidateSet("codex", "shell", "host")]
    [string]$StartMode = "shell",

    [string]$BootstrapPrompt = "",
    [string]$PromptPath = "",
    [string]$ReasoningEffort = "",
    [int]$BootstrapInitialDelaySeconds = 0,
    [int]$BootstrapMaxAttempts = 3,
    [int]$BootstrapRetryDelaySeconds = 6,

    [string]$CodexLauncherPath = ""
)

$ErrorActionPreference = "Stop"

function global:Resolve-CommandSpec([string]$commandOrPath) {
    if ([string]::IsNullOrWhiteSpace($commandOrPath)) {
        return $null
    }

    if (Test-Path -LiteralPath $commandOrPath) {
        $resolvedPath = (Resolve-Path -LiteralPath $commandOrPath).Path
        return @{
            FilePath = $resolvedPath
            Arguments = @()
            Display = $resolvedPath
            CommandName = [System.IO.Path]::GetFileNameWithoutExtension($resolvedPath)
        }
    }

    $command = Get-Command $commandOrPath -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $command) {
        $source = if ($command.Source) { $command.Source } else { $command.Name }
        return @{
            FilePath = $source
            Arguments = @()
            Display = $source
            CommandName = $command.Name
        }
    }

    return $null
}

function global:Resolve-RipgrepCommand {
    $rgCommand = Get-Command rg -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $rgCommand -and -not [string]::IsNullOrWhiteSpace($rgCommand.Source)) {
        return $rgCommand.Source
    }

    $windowsAppsRoot = "F:\Program Files\WindowsApps"
    if (Test-Path -LiteralPath $windowsAppsRoot) {
        $candidate = Get-ChildItem -LiteralPath $windowsAppsRoot -Directory -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -like 'OpenAI.Codex_*_x64__2p2nqsd0c76g0' } |
            Sort-Object Name -Descending |
            Select-Object -First 1
        if ($null -ne $candidate) {
            $rgPath = Join-Path $candidate.FullName "app\resources\rg.exe"
            if (Test-Path -LiteralPath $rgPath) {
                return $rgPath
            }
        }
    }

    return $null
}

function global:Prepend-PathDirectory([string]$directory) {
    if ([string]::IsNullOrWhiteSpace($directory)) {
        return $false
    }

    if (-not (Test-Path -LiteralPath $directory)) {
        return $false
    }

    $currentEntries = ($env:PATH -split ';') | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    $normalizedDirectory = [System.IO.Path]::GetFullPath($directory)
    $alreadyPresent = $false
    foreach ($entry in $currentEntries) {
        try {
            if ([System.String]::Equals([System.IO.Path]::GetFullPath($entry), $normalizedDirectory, [System.StringComparison]::OrdinalIgnoreCase)) {
                $alreadyPresent = $true
                break
            }
        } catch {
            if ([System.String]::Equals($entry, $directory, [System.StringComparison]::OrdinalIgnoreCase)) {
                $alreadyPresent = $true
                break
            }
        }
    }

    if ($alreadyPresent) {
        return $false
    }

    $env:PATH = "${directory};$env:PATH"
    return $true
}

function global:Resolve-RawCodexCommand {
    $servbayCodexPs1 = "F:\work\useful\ServBay\packages\node\current\codex.ps1"
    if (Test-Path -LiteralPath $servbayCodexPs1) {
        return @{
            FilePath = $servbayCodexPs1
            Arguments = @()
            Display = $servbayCodexPs1
            CommandName = "codex"
        }
    }

    $codex = Get-Command codex -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $codex) {
        return @{
            FilePath = $codex.Source
            Arguments = @()
            Display = $codex.Source
            CommandName = "codex"
        }
    }

    throw "Unable to find codex. Ensure codex is available in PATH or install it under ServBay."
}

function global:Resolve-CodexCommand {
    if (-not [string]::IsNullOrWhiteSpace($global:AgentToolRequestedCodexLauncherPath)) {
        $customCommand = Resolve-CommandSpec $global:AgentToolRequestedCodexLauncherPath
        if ($null -eq $customCommand) {
            throw "Unable to find custom codex launcher: $($global:AgentToolRequestedCodexLauncherPath)"
        }
        return $customCommand
    }

    return Resolve-RawCodexCommand
}

function global:Resolve-AgentHostCommand {
    $agentToolRoot = Split-Path -Parent $PSScriptRoot
    $agentHostExe = Join-Path $agentToolRoot "target\debug\agenthost.exe"
    if (Test-Path -LiteralPath $agentHostExe) {
        return @{
            FilePath = $agentHostExe
            Arguments = @("--agent", $AgentName)
            Display = "$agentHostExe --agent $AgentName"
        }
    }

    throw "Unable to find agenthost.exe. Build AgentTool first so target\debug\agenthost.exe exists."
}

function global:Resolve-AgentCtlCommand {
    $agentToolRoot = Split-Path -Parent $PSScriptRoot
    $agentCtlExe = Join-Path $agentToolRoot "target\debug\agentctl.exe"
    if (Test-Path -LiteralPath $agentCtlExe) {
        return $agentCtlExe
    }
    return $null
}

function global:Format-CodexLaunchHint($codexCommand) {
    $parts = @($codexCommand.FilePath) + $codexCommand.Arguments
    return ($parts | ForEach-Object {
            if ($_ -match '\s') {
                '"' + $_ + '"'
            } else {
                $_
            }
        }) -join " "
}

function global:Resolve-PromptPath([string]$root, [string]$agentName) {
    if (-not [string]::IsNullOrWhiteSpace($PromptPath)) {
        if (-not (Test-Path -LiteralPath $PromptPath)) {
            throw "Prompt path not found: $PromptPath"
        }
        return (Resolve-Path -LiteralPath $PromptPath).Path
    }

    $promptFile = if ($agentName -eq "main") { "MAIN_AGENT_PROMPT.md" } else { "SUBAGENT_PROMPT.md" }
    $candidate = Join-Path $root $promptFile
    if (Test-Path -LiteralPath $candidate) {
        return (Resolve-Path -LiteralPath $candidate).Path
    }
    return $null
}

function global:Get-AgentCodexProfile([string]$agentName) {
    $reasoningEffort = if (-not [string]::IsNullOrWhiteSpace($ReasoningEffort)) {
        $ReasoningEffort
    } elseif ($agentName -eq "main") {
        "medium"
    } else {
        "high"
    }
    return @{
        Model = "gpt-5.4"
        ReasoningEffort = $reasoningEffort
        Sandbox = "danger-full-access"
        Approval = "never"
    }
}

function global:Test-ArgumentPresent([string[]]$arguments, [string[]]$names) {
    if ($null -eq $arguments) {
        return $false
    }

    foreach ($argument in $arguments) {
        if ($names -contains $argument) {
            return $true
        }
    }

    return $false
}

function global:Test-ConfigOverridePresent([string[]]$arguments, [string[]]$keys) {
    if ($null -eq $arguments) {
        return $false
    }

    for ($index = 0; $index -lt $arguments.Count; $index++) {
        $argument = $arguments[$index]
        if ($argument -ne "-c" -and $argument -ne "--config") {
            continue
        }

        if (($index + 1) -ge $arguments.Count) {
            continue
        }

        $configValue = $arguments[$index + 1]
        foreach ($key in $keys) {
            if ($configValue.StartsWith("$key=", [System.StringComparison]::OrdinalIgnoreCase)) {
                return $true
            }
        }
    }

    return $false
}

function global:Start-AgentBridgeJob {
    param(
        [ValidateSet("passive", "autorun")]
        [string]$BridgeMode = "passive",
        [bool]$Quiet = $true
    )

    $hostCommand = Resolve-AgentHostCommand
    if ($null -eq $hostCommand) {
        return $null
    }

    return Start-Job -ScriptBlock {
        param($agentHostPath, $name, $mode, $quiet)
        $arguments = @("--agent", $name, "--bridge-mode", $mode)
        if ($quiet) {
            $arguments += "--quiet"
        }
        & $agentHostPath @arguments *> $null
    } -ArgumentList $hostCommand.FilePath, $AgentName, $BridgeMode, $Quiet
}

function global:Stop-AgentBridgeJob($job) {
    if ($null -eq $job) {
        return
    }

    try {
        Stop-Job -Job $job -ErrorAction SilentlyContinue | Out-Null
    } catch {
    }

    try {
        Remove-Job -Job $job -Force -ErrorAction SilentlyContinue | Out-Null
    } catch {
    }
}

function global:Invoke-AgentBootstrapReset {
    $agentCtl = Resolve-AgentCtlCommand
    if ($null -eq $agentCtl) {
        return
    }

    try {
        & $agentCtl begin-agent-bootstrap --agent $AgentName *> $null
    } catch {
    }
}

function global:Set-AgentVisiblePaneRegistration {
    param(
        [switch]$Clear
    )

    if (-not $env:AGENTTOOL_CTL) {
        return
    }

    $arguments = @("set-agent-visible-pane", "--agent", $AgentName)
    if (-not $Clear) {
        $arguments += @("--pid", [string]$PID, "--kind", "shell")
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

function global:Register-AgentVisiblePaneExitHandler {
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

function global:Get-AgentToolRoot {
    return (Split-Path -Parent $PSScriptRoot)
}

function global:Get-AgentBootstrapLogDir {
    $logDir = Join-Path (Get-AgentToolRoot) "data\bootstrap_logs"
    if (-not (Test-Path -LiteralPath $logDir)) {
        New-Item -ItemType Directory -Path $logDir -Force | Out-Null
    }
    return (Resolve-Path -LiteralPath $logDir).Path
}

function global:Get-AgentBootstrapLogPath {
    $safeName = ($AgentName -replace '[^A-Za-z0-9_.-]', '_')
    return (Join-Path (Get-AgentBootstrapLogDir) ("{0}.log" -f $safeName))
}

function global:Format-BootstrapTraceValue([string]$value, [int]$limit = 260) {
    $text = [string]$value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return ""
    }

    $normalized = ($text -replace '\s+', ' ').Trim()
    if ($normalized.Length -le $limit) {
        return $normalized
    }

    return ($normalized.Substring(0, [Math]::Max(0, $limit - 3)) + "...")
}

function global:Split-BootstrapLinesFromBytes([byte[]]$bytes) {
    if ($null -eq $bytes -or $bytes.Length -eq 0) {
        return @()
    }

    $text = [System.Text.UTF8Encoding]::new($false, $false).GetString($bytes)
    return @($text -split "`r?`n")
}

function global:Format-CmdLiteral([string]$value) {
    return '"' + ($value -replace '"', '""') + '"'
}

function global:Format-CmdArgument([string]$value) {
    if ($null -eq $value) {
        return '""'
    }

    if ($value -eq '') {
        return '""'
    }

    if ($value -notmatch '[\s"&|<>^()]') {
        return $value
    }

    return '"' + ($value -replace '"', '""') + '"'
}

function global:Write-AgentBootstrapTrace {
    param(
        [string]$Stage,
        [string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR")]
        [string]$Level = "INFO"
    )

    $timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffK")
    $attemptSuffix = if ($global:AgentToolBootstrapAttempt -and $global:AgentToolBootstrapAttempt -gt 0) {
        "[attempt=$($global:AgentToolBootstrapAttempt)]"
    } else {
        ""
    }
    $line = "[BOOTSTRAP][$timestamp][$Level][$AgentName]$attemptSuffix[$Stage] $Message"
    Add-Content -LiteralPath (Get-AgentBootstrapLogPath) -Value $line -Encoding UTF8
    Write-Host $line
}

function global:Get-AgentBootstrapSchemaPath {
    $schemaPath = Join-Path (Get-AgentToolRoot) "schemas\bootstrap_ready.schema.json"
    if (-not (Test-Path -LiteralPath $schemaPath)) {
        throw "Bootstrap schema not found: $schemaPath"
    }
    return (Resolve-Path -LiteralPath $schemaPath).Path
}

function global:Join-CodePoints([int[]]$Points, [string]$Suffix = "") {
    return (-join ($Points | ForEach-Object { [char]$_ })) + $Suffix
}

function global:Get-AgentBootstrapContractPrompt {
    if ([string]::IsNullOrWhiteSpace($global:AgentToolDefaultBootstrapPrompt)) {
        return ""
    }

    $contractTail = @(
        (Join-CodePoints @(0x672C,0x8F6E,0x53EA,0x7528,0x4E8E,0x521D,0x59CB,0x5316,0xFF0C,0x4E0D,0x5F00,0x59CB,0x65B0,0x7684,0x4E1A,0x52A1,0x5DE5,0x4F5C,0xFF0C,0x4E0D,0x6D3E,0x5355,0xFF0C,0x4E0D,0x4FEE,0x6539,0x6587,0x4EF6,0x3002)),
        (Join-CodePoints @(0x5982,0x679C,0x4ED3,0x5E93,0x5185,0x7684,0x0020,0x0041,0x0047,0x0045,0x004E,0x0054,0x0053,0x002E,0x006D,0x0064,0x3001,0x63D0,0x793A,0x8BCD,0x6216,0x683C,0x5F0F,0x89C4,0x5219,0x4E0E,0x672C,0x8F6E,0x521D,0x59CB,0x5316,0x8981,0x6C42,0x51B2,0x7A81,0xFF0C,0x4EE5,0x672C,0x8F6E,0x521D,0x59CB,0x5316,0x5951,0x7EA6,0x4E3A,0x51C6,0x3002)),
        (Join-CodePoints @(0x4F60,0x53EF,0x4EE5,0x505A,0x6700,0x5C11,0x91CF,0x7684,0x53EA,0x8BFB,0x68C0,0x67E5,0x6765,0x8BFB,0x53D6,0x63D0,0x793A,0x8BCD,0x6216,0x5DE5,0x4F5C,0x533A,0x4E0A,0x4E0B,0x6587,0xFF0C,0x4F46,0x4E0D,0x8981,0x8C03,0x7528,0x0020,0x0060,0x0061,0x0067,0x0065,0x006E,0x0074,0x0072,0x0065,0x0061,0x0064,0x0079,0x0060,0x3001,0x0060,0x0061,0x0067,0x0074,0x0060,0x0020,0x6216,0x4EFB,0x4F55,0x5176,0x4ED6,0x0020,0x0041,0x0067,0x0065,0x006E,0x0074,0x0054,0x006F,0x006F,0x006C,0x0020,0x901A,0x4FE1,0x547D,0x4EE4,0x3002)),
        (Join-CodePoints @(0x5982,0x679C,0x4ED3,0x5E93,0x8981,0x6C42,0x8F93,0x51FA,0x0020,0x0060,0x005B,0x0054,0x0048,0x0049,0x004E,0x004B,0x0049,0x004E,0x0047,0x005D,0x0060,0x3001,0x0060,0x005B,0x0050,0x0052,0x004F,0x0047,0x0052,0x0045,0x0053,0x0053,0x005D,0x0060,0x0020,0x7B49,0x900F,0x660E,0x64AD,0x62A5,0xFF0C,0x53EF,0x4EE5,0x901A,0x8FC7,0x53EA,0x8BFB,0x547D,0x4EE4,0x8F93,0x51FA,0x6EE1,0x8DB3,0xFF1B,0x4F46,0x4F60,0x4F5C,0x4E3A,0x0020,0x0061,0x0073,0x0073,0x0069,0x0073,0x0074,0x0061,0x006E,0x0074,0x0020,0x7684,0x6700,0x7EC8,0x8F93,0x51FA,0x4ECD,0x7136,0x53EA,0x80FD,0x662F,0x7B26,0x5408,0x0020,0x0073,0x0063,0x0068,0x0065,0x006D,0x0061,0x0020,0x7684,0x90A3,0x4E00,0x4E2A,0x0020,0x004A,0x0053,0x004F,0x004E,0x0020,0x5BF9,0x8C61,0x3002)),
        (Join-CodePoints @(0x5B8C,0x6210,0x521D,0x59CB,0x5316,0x603B,0x7ED3,0x540E,0xFF0C,0x4F60,0x7684,0x6700,0x7EC8,0x8F93,0x51FA,0x5FC5,0x987B,0x4E25,0x683C,0x53EA,0x5305,0x542B,0x4E00,0x4E2A,0x0020,0x004A,0x0053,0x004F,0x004E,0x0020,0x5BF9,0x8C61,0xFF1A,0x0060,0x0073,0x0074,0x0061,0x0074,0x0075,0x0073,0x0060,0x0020,0x5FC5,0x987B,0x662F,0x0020,0x0060,0x0072,0x0065,0x0061,0x0064,0x0079,0x0060,0xFF0C,0x0060,0x0073,0x0075,0x006D,0x006D,0x0061,0x0072,0x0079,0x0060,0x0020,0x5FC5,0x987B,0x662F,0x4E00,0x53E5,0x4E2D,0x6587,0xFF0C,0x8BF4,0x660E,0x521D,0x59CB,0x5316,0x5DF2,0x5B8C,0x6210,0x4E14,0x5F53,0x524D,0x5904,0x4E8E,0x5F85,0x547D,0x72B6,0x6001,0x3002))
    ) -join "`r`n"

    return (($global:AgentToolDefaultBootstrapPrompt.TrimEnd()) + "`r`n`r`n" + $contractTail.Trim())
}

function global:Get-AgentAutoBootstrapPrompt {
    $contractLines = @(
        (Join-CodePoints @(0x672C,0x8F6E,0x53EA,0x7528,0x4E8E,0x521D,0x59CB,0x5316,0xFF0C,0x4E0D,0x5F00,0x59CB,0x65B0,0x7684,0x4E1A,0x52A1,0x5DE5,0x4F5C,0xFF0C,0x4E0D,0x6D3E,0x5355,0xFF0C,0x4E0D,0x4FEE,0x6539,0x6587,0x4EF6,0x3002)),
        (Join-CodePoints @(0x4F60,0x5DF2,0x7ECF,0x62FF,0x5230,0x4E86,0x957F,0x671F,0x89D2,0x8272,0x5951,0x7EA6,0xFF1B,0x672C,0x8F6E,0x53EA,0x9700,0x8981,0x786E,0x8BA4,0x521D,0x59CB,0x5316,0x5B8C,0x6210,0x5E76,0x8FDB,0x5165,0x5F85,0x547D,0x3002)),
        (Join-CodePoints @(0x4E0D,0x8981,0x4E3B,0x52A8,0x5206,0x6790,0x4E1A,0x52A1,0x4EFB,0x52A1,0xFF0C,0x4E0D,0x8981,0x4E3B,0x52A8,0x5F00,0x5C55,0x5B9E,0x73B0,0xFF0C,0x7B49,0x5F85,0x4E0B,0x4E00,0x6761,0x6D88,0x606F,0x3002)),
        (Join-CodePoints @(0x5982,0x679C,0x4ED3,0x5E93,0x8981,0x6C42,0x8F93,0x51FA,0x0020,0x0060,0x005B,0x0054,0x0048,0x0049,0x004E,0x004B,0x0049,0x004E,0x0047,0x005D,0x0060,0x3001,0x0060,0x005B,0x0050,0x0052,0x004F,0x0047,0x0052,0x0045,0x0053,0x0053,0x005D,0x0060,0x0020,0x7B49,0x900F,0x660E,0x64AD,0x62A5,0xFF0C,0x53EF,0x4EE5,0x7528,0x6700,0x5C11,0x91CF,0x53EA,0x8BFB,0x52A8,0x4F5C,0x6EE1,0x8DB3,0xFF1B,0x4F46,0x6700,0x7EC8,0x8F93,0x51FA,0x4ECD,0x7136,0x53EA,0x80FD,0x662F,0x7B26,0x5408,0x0020,0x0073,0x0063,0x0068,0x0065,0x006D,0x0061,0x0020,0x7684,0x90A3,0x4E00,0x4E2A,0x0020,0x004A,0x0053,0x004F,0x004E,0x0020,0x5BF9,0x8C61,0x3002)),
        (Join-CodePoints @(0x6700,0x7EC8,0x8F93,0x51FA,0x5FC5,0x987B,0x4E25,0x683C,0x53EA,0x5305,0x542B,0x4E00,0x4E2A,0x0020,0x004A,0x0053,0x004F,0x004E,0x0020,0x5BF9,0x8C61,0xFF1A,0x0060,0x0073,0x0074,0x0061,0x0074,0x0075,0x0073,0x0060,0x0020,0x5FC5,0x987B,0x662F,0x0020,0x0060,0x0072,0x0065,0x0061,0x0064,0x0079,0x0060,0xFF0C,0x0060,0x0073,0x0075,0x006D,0x006D,0x0061,0x0072,0x0079,0x0060,0x0020,0x5FC5,0x987B,0x662F,0x4E00,0x53E5,0x4E2D,0x6587,0xFF0C,0x8BF4,0x660E,0x521D,0x59CB,0x5316,0x5DF2,0x5B8C,0x6210,0x4E14,0x5F53,0x524D,0x5904,0x4E8E,0x5F85,0x547D,0x72B6,0x6001,0x3002))
    )

    return ($contractLines -join "`r`n")
}

function global:Get-AgentPersistentDeveloperInstructions {
    if (-not [string]::IsNullOrWhiteSpace($global:AgentToolPromptPath) -and (Test-Path -LiteralPath $global:AgentToolPromptPath)) {
        $promptText = Get-Content -LiteralPath $global:AgentToolPromptPath -Encoding UTF8 -Raw
        $header = @(
            (Join-CodePoints @(0x4EE5,0x4E0B,0x5185,0x5BB9,0x7531,0x0020,0x0041,0x0067,0x0065,0x006E,0x0074,0x0054,0x006F,0x006F,0x006C,0x0020,0x4EE5,0x0020,0x0055,0x0054,0x0046,0x002D,0x0038,0x0020,0x4ECE,0x5DE5,0x4F5C,0x533A,0x63D0,0x793A,0x8BCD,0x6587,0x4EF6,0x8BFB,0x53D6,0xFF0C,0x5E76,0x4F5C,0x4E3A,0x4F60,0x7684,0x957F,0x671F,0x89D2,0x8272,0x5951,0x7EA6,0x3002)),
            ((Join-CodePoints @(0x5951,0x7EA6,0x6587,0x4EF6,0x003A,0x0020)) + $global:AgentToolPromptPath),
            ((Join-CodePoints @(0x5F53,0x524D,0x4F1A,0x8BDD,0x7684,0x771F,0x5B9E,0x4ED3,0x5E93,0x6839,0x76EE,0x5F55,0x003A,0x0020)) + $resolvedCwd),
            (Join-CodePoints @(0x6240,0x6709,0x0020,0x0073,0x0068,0x0065,0x006C,0x006C,0x005F,0x0063,0x006F,0x006D,0x006D,0x0061,0x006E,0x0064,0x0020,0x6216,0x5DE5,0x5177,0x8C03,0x7528,0x7684,0x0020,0x0077,0x006F,0x0072,0x006B,0x0064,0x0069,0x0072,0x0020,0x5FC5,0x987B,0x4F7F,0x7528,0x8BE5,0x76EE,0x5F55,0x6216,0x5176,0x771F,0x5B9E,0x5B50,0x76EE,0x5F55,0x3002)),
            (Join-CodePoints @(0x7981,0x6B62,0x6839,0x636E,0x0020,0x0061,0x0067,0x0065,0x006E,0x0074,0x0020,0x540D,0x79F0,0x6216,0x4ED3,0x5E93,0x540D,0x79F0,0x81EA,0x884C,0x62FC,0x63A5,0x65B0,0x7684,0x7EDD,0x5BF9,0x8DEF,0x5F84,0xFF1B,0x5982,0x679C,0x9700,0x8981,0x8DE8,0x4ED3,0x8BFB,0x53D6,0xFF0C,0x5FC5,0x987B,0x4F7F,0x7528,0x786E,0x8BA4,0x5B58,0x5728,0x7684,0x660E,0x786E,0x7EDD,0x5BF9,0x8DEF,0x5F84,0x3002)),
            (Join-CodePoints @(0x5C24,0x5176,0x4E0D,0x8981,0x628A,0x0020,0x0068,0x0061,0x0063,0x006B,0x006D,0x0061,0x006E,0x0020,0x8FD9,0x4E00,0x5C42,0x76EE,0x5F55,0x4E22,0x6389,0x53BB,0x62FC,0x63A5,0x6210,0x0020,0x0046,0x003A,0x005C,0x0077,0x006F,0x0072,0x006B,0x005C,0x0067,0x0069,0x0074,0x0068,0x0075,0x0062,0x005C,0x003C,0x0061,0x0067,0x0065,0x006E,0x0074,0x003E,0x0020,0x8FD9,0x79CD,0x8DEF,0x5F84,0x3002)),
            (Join-CodePoints @(0x5728,0x0020,0x0041,0x0067,0x0065,0x006E,0x0074,0x0054,0x006F,0x006F,0x006C,0x0020,0x53EF,0x89C1,0x0020,0x0072,0x0065,0x006D,0x006F,0x0074,0x0065,0x0020,0x4F1A,0x8BDD,0x4E2D,0xFF0C,0x4E3A,0x4E86,0x7A33,0x5B9A,0x6027,0xFF0C,0x7981,0x6B62,0x5E76,0x53D1,0x53D1,0x8D77,0x591A,0x4E2A,0x5DE5,0x5177,0x8C03,0x7528,0x3002)),
            (Join-CodePoints @(0x5C24,0x5176,0x4E0D,0x8981,0x5728,0x540C,0x4E00,0x8F6E,0x91CC,0x5E76,0x884C,0x53D1,0x8D77,0x591A,0x4E2A,0x0020,0x0073,0x0068,0x0065,0x006C,0x006C,0x005F,0x0063,0x006F,0x006D,0x006D,0x0061,0x006E,0x0064,0xFF1B,0x8BF7,0x59CB,0x7EC8,0x4E32,0x884C,0x6267,0x884C,0xFF0C,0x4E00,0x6B21,0x53EA,0x8DD1,0x4E00,0x4E2A,0x547D,0x4EE4,0xFF0C,0x7B49,0x4E0A,0x4E00,0x4E2A,0x7ED3,0x675F,0x540E,0x518D,0x8DD1,0x4E0B,0x4E00,0x4E2A,0x3002)),
            (Join-CodePoints @(0x540E,0x7EED,0x6240,0x6709,0x5DE5,0x4F5C,0x90FD,0x5FC5,0x987B,0x9075,0x5B88,0x8BE5,0x5951,0x7EA6,0x3002)),
            ''
        ) -join "`r`n"
        return ($header + $promptText.Trim())
    }

    if (-not [string]::IsNullOrWhiteSpace($global:AgentToolDefaultBootstrapPrompt)) {
        return $global:AgentToolDefaultBootstrapPrompt.Trim()
    }

    return ""
}

function global:New-AgentAppServerPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    } finally {
        $listener.Stop()
    }
}

function global:Start-AgentBootstrapAppServer {
    param(
        [hashtable]$CommandSpec = $global:AgentToolPreferredCodexCommand
    )

    if ($null -eq $CommandSpec) {
        $CommandSpec = Resolve-CodexCommand
    }

    $port = New-AgentAppServerPort
    $stderrFile = Join-Path ([System.IO.Path]::GetTempPath()) ("agenttool-appserver-{0}-{1}.stderr.log" -f $AgentName, [guid]::NewGuid().ToString("N"))
    $cmdParts = @(
        'chcp 65001>nul',
        '&',
        'cd /d',
        (Format-CmdLiteral $resolvedCwd),
        '&',
        (Format-CmdLiteral $CommandSpec.FilePath)
    ) + (@($CommandSpec.Arguments) | ForEach-Object { Format-CmdArgument ([string]$_) }) + @(
        'app-server',
        '--listen',
        ('ws://127.0.0.1:{0}' -f $port),
        '2>',
        (Format-CmdLiteral $stderrFile)
    )
    $cmdScript = $cmdParts -join ' '

    Write-AgentBootstrapTrace -Stage "app_server_start" -Message ("cmd.exe /d /c {0}" -f (Format-BootstrapTraceValue $cmdScript 500))
    $process = Start-Process -FilePath "cmd.exe" `
        -ArgumentList @("/d", "/c", $cmdScript) `
        -WorkingDirectory $resolvedCwd `
        -PassThru `
        -WindowStyle Hidden

    return @{
        Process = $process
        Port = $port
        StderrFile = $stderrFile
        CommandLine = $cmdScript
    }
}

function global:Stop-AgentBootstrapAppServer($server) {
    if ($null -eq $server) {
        return
    }

    if ($null -ne $server.KeeperDrain) {
        try {
            $server.KeeperDrain.Dispose()
        } catch {
        }
    }

    if ($null -ne $server.KeeperSocket) {
        try {
            $server.KeeperSocket.Dispose()
        } catch {
        }
    }

    if ($null -ne $server.Socket) {
        try {
            $server.Socket.Dispose()
        } catch {
        }
    }

    if ($null -ne $server.Process) {
        try {
            & taskkill.exe /PID $server.Process.Id /T /F *> $null
        } catch {
        }

        try {
            if (-not $server.Process.HasExited) {
                Stop-Process -Id $server.Process.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {
        }
    }

    if ($null -ne $server.StderrFile -and (Test-Path -LiteralPath $server.StderrFile)) {
        try {
            Remove-Item -LiteralPath $server.StderrFile -Force -ErrorAction SilentlyContinue
        } catch {
        }
    }
}

function global:Get-AgentBootstrapServerDiagnostics($server, [int]$TailLines = 12) {
    if ($null -eq $server -or $null -eq $server.StderrFile -or -not (Test-Path -LiteralPath $server.StderrFile)) {
        return "<no stderr>"
    }

    try {
        $lines = Get-Content -LiteralPath $server.StderrFile -Encoding UTF8 -ErrorAction Stop | Select-Object -Last $TailLines
        if ($null -eq $lines -or $lines.Count -eq 0) {
            return "<empty stderr>"
        }
        return ($lines -join " || ")
    } catch {
        return "<stderr unreadable>"
    }
}

function global:Get-AgentBootstrapAppServerObservation($server) {
    if ($null -eq $server) {
        return $null
    }

    $processId = $null
    $hasExited = $null
    $exitCode = $null
    if ($null -ne $server.Process) {
        try {
            $server.Process.Refresh()
        } catch {
        }

        try {
            $processId = $server.Process.Id
        } catch {
            $processId = $null
        }

        try {
            $hasExited = $server.Process.HasExited
        } catch {
            $hasExited = $null
        }

        if ($hasExited -eq $true) {
            try {
                $exitCode = $server.Process.ExitCode
            } catch {
                $exitCode = $null
            }
        }
    }

    return [ordered]@{
        remote_url = if ($null -ne $server.Port) { ('ws://127.0.0.1:{0}' -f $server.Port) } else { $null }
        port = $server.Port
        process_id = $processId
        has_exited = $hasExited
        exit_code = $exitCode
        stderr_tail = Get-AgentBootstrapServerDiagnostics $server
    }
}

function global:Append-AgentRuntimeObservation {
    param(
        [string]$EventType,
        [string]$Summary,
        [string]$Reason = "",
        $Payload = $null
    )

    if (-not $env:AGENTTOOL_CTL) {
        return
    }

    $arguments = @(
        "append-runtime-event",
        "--scope",
        "agent",
        "--scope-id",
        $AgentName,
        "--agent",
        $AgentName,
        "--actor",
        $AgentName,
        "--event-type",
        $EventType,
        "--summary",
        $Summary
    )

    if (-not [string]::IsNullOrWhiteSpace($Reason)) {
        $arguments += @("--reason", $Reason)
    }

    if ($null -ne $Payload) {
        $payloadFile = $null
        try {
            $payloadJson = $Payload | ConvertTo-Json -Compress -Depth 8
            if (-not [string]::IsNullOrWhiteSpace($payloadJson)) {
                $payloadFile = Join-Path ([System.IO.Path]::GetTempPath()) ("agenttool-runtime-payload-{0}-{1}.json" -f $AgentName, [guid]::NewGuid().ToString("N"))
                [System.IO.File]::WriteAllText($payloadFile, $payloadJson, $global:AgentToolUtf8NoBom)
                $arguments += @("--payload-file", $payloadFile)
            }
        } catch {
            Write-AgentBootstrapTrace -Stage "runtime_event" -Level "WARN" -Message ("failed to serialize payload for {0}: {1}" -f $EventType, (Format-BootstrapTraceValue $_.Exception.Message 260))
        }
    }

    try {
        & $env:AGENTTOOL_CTL @arguments *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-AgentBootstrapTrace -Stage "runtime_event" -Level "WARN" -Message ("agentctl append-runtime-event failed for {0}" -f $EventType)
        }
    } catch {
        Write-AgentBootstrapTrace -Stage "runtime_event" -Level "WARN" -Message ("agentctl append-runtime-event raised for {0}: {1}" -f $EventType, (Format-BootstrapTraceValue $_.Exception.Message 260))
    } finally {
        if ($null -ne $payloadFile -and (Test-Path -LiteralPath $payloadFile)) {
            try {
                Remove-Item -LiteralPath $payloadFile -Force -ErrorAction SilentlyContinue
            } catch {
            }
        }
    }
}

function global:Wait-AgentBootstrapAppServerReady {
    param(
        [hashtable]$Server,
        [int]$TimeoutSeconds = 40
    )

    if ($null -eq $Server) {
        throw "App-server handle was null."
    }

    $readyUri = 'http://127.0.0.1:{0}/readyz' -f $Server.Port
    $deadline = (Get-Date).AddSeconds([Math]::Max(1, $TimeoutSeconds))
    while ((Get-Date) -lt $deadline) {
        if ($null -ne $Server.Process -and $Server.Process.HasExited) {
            $diagnostic = Get-AgentBootstrapServerDiagnostics $Server
            throw "bootstrap app-server exited before ready. stderr=$diagnostic"
        }

        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $readyUri -TimeoutSec 2
            if ($response.StatusCode -eq 200) {
                Write-AgentBootstrapTrace -Stage "app_server_ready" -Message ("readyz ok on port {0}" -f $Server.Port)
                return
            }
        } catch {
        }

        Start-Sleep -Milliseconds 500
    }

    $diagnostic = Get-AgentBootstrapServerDiagnostics $Server
    throw "bootstrap app-server did not become ready on port $($Server.Port). stderr=$diagnostic"
}

function global:Connect-AgentBootstrapWebSocket {
    param(
        [hashtable]$Server,
        [int]$TimeoutSeconds = 20
    )

    Add-Type -AssemblyName System.Net.Http | Out-Null
    $socket = [System.Net.WebSockets.ClientWebSocket]::new()
    $uri = [System.Uri]::new(('ws://127.0.0.1:{0}' -f $Server.Port))
    $cancellation = [System.Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds([Math]::Max(1, $TimeoutSeconds)))

    try {
        $socket.ConnectAsync($uri, $cancellation.Token).GetAwaiter().GetResult() | Out-Null
    } catch {
        try {
            $socket.Dispose()
        } catch {
        }
        $diagnostic = Get-AgentBootstrapServerDiagnostics $Server
        throw "failed to connect bootstrap websocket on port $($Server.Port). stderr=$diagnostic"
    } finally {
        $cancellation.Dispose()
    }

    $Server.Socket = $socket
    Write-AgentBootstrapTrace -Stage "app_server_connected" -Message ("websocket connected on port {0}" -f $Server.Port)
    return $socket
}

function global:Ensure-AgentBootstrapWebSocketDrainType {
    if ("AgentToolWebSocketDrain" -as [type]) {
        return
    }

    Add-Type -Language CSharp -TypeDefinition @"
using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;

public sealed class AgentToolWebSocketDrain : IDisposable
{
    private readonly ClientWebSocket _socket;
    private readonly CancellationTokenSource _cts;
    private readonly Task _task;
    private Exception _lastError;

    public AgentToolWebSocketDrain(ClientWebSocket socket)
    {
        if (socket == null)
        {
            throw new ArgumentNullException("socket");
        }

        _socket = socket;
        _cts = new CancellationTokenSource();
        _task = Task.Run(() => DrainAsync(_cts.Token));
    }

    public bool IsCompleted
    {
        get { return _task.IsCompleted; }
    }

    public string LastError
    {
        get
        {
            var error = _lastError;
            if (error == null)
            {
                return null;
            }

            return error.GetType().Name + ": " + error.Message;
        }
    }

    private async Task DrainAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[65536];
        var segment = new ArraySegment<byte>(buffer);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                WebSocketReceiveResult result;
                try
                {
                    result = await _socket.ReceiveAsync(segment, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    break;
                }

                while (!result.EndOfMessage)
                {
                    try
                    {
                        result = await _socket.ReceiveAsync(segment, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }
                    catch (ObjectDisposedException)
                    {
                        return;
                    }

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        return;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _lastError = ex;
        }
    }

    public void Dispose()
    {
        try
        {
            _cts.Cancel();
        }
        catch
        {
        }

        try
        {
            _task.Wait(1000);
        }
        catch
        {
        }

        _cts.Dispose();
    }
}
"@
}

function global:Start-AgentBootstrapWebSocketDrain {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Socket
    )

    if ($null -eq $Socket) {
        return $null
    }

    Ensure-AgentBootstrapWebSocketDrainType
    return [AgentToolWebSocketDrain]::new($Socket)
}

function global:Send-AgentBootstrapRpcMessage {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Socket,
        $Message
    )

    $json = $Message | ConvertTo-Json -Depth 40 -Compress
    $bytes = $global:AgentToolUtf8NoBom.GetBytes($json)
    $segment = [System.ArraySegment[byte]]::new($bytes)
    $Socket.SendAsync(
        $segment,
        [System.Net.WebSockets.WebSocketMessageType]::Text,
        $true,
        [System.Threading.CancellationToken]::None
    ).GetAwaiter().GetResult() | Out-Null
}

function global:Read-AgentBootstrapRpcMessage {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Socket,
        [int]$TimeoutMs = 30000
    )

    $buffer = New-Object byte[] 65536
    $segment = [System.ArraySegment[byte]]::new($buffer)
    $builder = New-Object System.Text.StringBuilder
    $cancellation = [System.Threading.CancellationTokenSource]::new([Math]::Max(1, $TimeoutMs))

    try {
        do {
            $result = $Socket.ReceiveAsync($segment, $cancellation.Token).GetAwaiter().GetResult()
            if ($result.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Close) {
                throw "bootstrap websocket closed unexpectedly"
            }
            if ($result.Count -gt 0) {
                $builder.Append($global:AgentToolUtf8NoBom.GetString($buffer, 0, $result.Count)) | Out-Null
            }
        } while (-not $result.EndOfMessage)
    } finally {
        $cancellation.Dispose()
    }

    $text = $builder.ToString()
    if ([string]::IsNullOrWhiteSpace($text)) {
        throw "bootstrap websocket returned an empty frame"
    }

    try {
        return $text | ConvertFrom-Json -ErrorAction Stop
    } catch {
        throw "bootstrap websocket returned invalid JSON: $(Format-BootstrapTraceValue $text 400)"
    }
}

function global:Wait-AgentBootstrapRpcResponse {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Socket,
        [System.Collections.Queue]$PendingMessages,
        [int]$RequestId,
        [int]$TimeoutMs = 30000
    )

    $deadline = (Get-Date).AddMilliseconds([Math]::Max(1, $TimeoutMs))
    while ((Get-Date) -lt $deadline) {
        $remaining = [Math]::Max(1, [int](($deadline - (Get-Date)).TotalMilliseconds))
        $message = Read-AgentBootstrapRpcMessage -Socket $Socket -TimeoutMs $remaining
        if ($null -ne $message.id -and [string]$message.id -eq [string]$RequestId) {
            if ($null -ne $message.error) {
                $messageText = if ($null -ne $message.error.message) { [string]$message.error.message } else { "unknown rpc error" }
                throw "bootstrap RPC request $RequestId failed: $messageText"
            }
            return $message
        }

        if ($null -ne $PendingMessages) {
            $PendingMessages.Enqueue($message)
        }
    }

    throw "timed out waiting for bootstrap RPC response id=$RequestId"
}

function global:Read-AgentBootstrapRpcQueuedMessage {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Socket,
        [System.Collections.Queue]$PendingMessages,
        [int]$TimeoutMs = 30000
    )

    if ($null -ne $PendingMessages -and $PendingMessages.Count -gt 0) {
        return $PendingMessages.Dequeue()
    }

    return Read-AgentBootstrapRpcMessage -Socket $Socket -TimeoutMs $TimeoutMs
}

function global:Send-AgentBootstrapRpcRequest {
    param(
        [System.Net.WebSockets.ClientWebSocket]$Socket,
        [System.Collections.Queue]$PendingMessages,
        [string]$Method,
        $Params,
        [int]$TimeoutMs = 30000
    )

    $global:AgentToolBootstrapRpcRequestId += 1
    $requestId = $global:AgentToolBootstrapRpcRequestId
    Send-AgentBootstrapRpcMessage -Socket $Socket -Message @{
        id = $requestId
        method = $Method
        params = $Params
    }

    return Wait-AgentBootstrapRpcResponse -Socket $Socket -PendingMessages $PendingMessages -RequestId $requestId -TimeoutMs $TimeoutMs
}

function global:Write-AgentBootstrapJsonLine {
    param(
        $Entry
    )

    return ($Entry | ConvertTo-Json -Depth 100 -Compress)
}

function global:Sanitize-AgentBootstrapRollout {
    param(
        [string]$RolloutPath,
        [string]$BootstrapPrompt
    )

    if ([string]::IsNullOrWhiteSpace($RolloutPath) -or -not (Test-Path -LiteralPath $RolloutPath)) {
        throw "bootstrap rollout path missing: $RolloutPath"
    }

    $lines = [System.IO.File]::ReadAllLines($RolloutPath, $global:AgentToolUtf8NoBom)
    $sanitizedLines = New-Object System.Collections.Generic.List[string]

    foreach ($line in $lines) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            continue
        }

        try {
            $entry = $line | ConvertFrom-Json -ErrorAction Stop
        } catch {
            $sanitizedLines.Add($line)
            continue
        }

        if ($entry.type -eq "response_item" -and $null -ne $entry.payload -and $entry.payload.type -eq "message" -and $entry.payload.role -eq "user") {
            $content = @($entry.payload.content)
            $textFragments = @(
                $content |
                    Where-Object { $_.type -eq "input_text" -and $null -ne $_.text } |
                    ForEach-Object { [string]$_.text }
            )
            if ($textFragments.Count -eq 1 -and $textFragments[0] -eq $BootstrapPrompt) {
                continue
            }
        }

        if ($entry.type -eq "event_msg" -and $null -ne $entry.payload -and $entry.payload.type -eq "user_message" -and [string]$entry.payload.message -eq $BootstrapPrompt) {
            continue
        }

        if ($entry.type -eq "turn_context" -and $null -ne $entry.payload) {
            if ($entry.payload.PSObject.Properties.Name -contains "final_output_json_schema") {
                $entry.payload.PSObject.Properties.Remove("final_output_json_schema")
                $sanitizedLines.Add((Write-AgentBootstrapJsonLine $entry))
                continue
            }
        }

        $sanitizedLines.Add($line)
    }

    [System.IO.File]::WriteAllLines($RolloutPath, $sanitizedLines, $global:AgentToolUtf8NoBom)
}

function global:Sanitize-AgentBootstrapRolloutWithRetry {
    param(
        [string]$RolloutPath,
        [string]$BootstrapPrompt,
        [int]$MaxAttempts = 40,
        [int]$DelayMilliseconds = 250
    )

    $attempts = [Math]::Max(1, $MaxAttempts)
    for ($attempt = 1; $attempt -le $attempts; $attempt++) {
        try {
            Sanitize-AgentBootstrapRollout -RolloutPath $RolloutPath -BootstrapPrompt $BootstrapPrompt
            return
        } catch {
            $message = $_.Exception.Message
            $isLastAttempt = $attempt -ge $attempts
            $isRetryable = ($_.Exception -is [System.IO.IOException]) -or ($message -like "*being used by another process*")
            if (-not $isRetryable -or $isLastAttempt) {
                throw
            }

            Write-AgentBootstrapTrace -Stage "sanitize_wait" -Level "WARN" -Message ("rollout still locked on attempt {0}/{1}; waiting {2}ms" -f $attempt, $attempts, $DelayMilliseconds)
            Start-Sleep -Milliseconds $DelayMilliseconds
        }
    }
}

function global:Add-AgentCodexProfileArgs {
    param(
        [string[]]$Arguments,
        [switch]$IncludeNoAltScreen,
        [switch]$IncludeApprovalFlag
    )

    $codexArgs = @($Arguments)
    if ($IncludeNoAltScreen) {
        $codexArgs += "--no-alt-screen"
    }
    if (-not (Test-ArgumentPresent $codexArgs @("-m", "--model")) -and -not (Test-ConfigOverridePresent $codexArgs @("model"))) {
        $codexArgs += @("-m", $global:AgentToolCodexProfile.Model)
    }
    if (-not (Test-ConfigOverridePresent $codexArgs @("model_reasoning_effort"))) {
        $codexArgs += @("-c", "model_reasoning_effort=""$($global:AgentToolCodexProfile.ReasoningEffort)""")
    }
    if (-not (Test-ConfigOverridePresent $codexArgs @("approval_policy"))) {
        $codexArgs += @("-c", "approval_policy=""$($global:AgentToolCodexProfile.Approval)""")
    }
    if (-not (Test-ArgumentPresent $codexArgs @("-s", "--sandbox"))) {
        $codexArgs += @("-s", $global:AgentToolCodexProfile.Sandbox)
    }
    if ($IncludeApprovalFlag -and -not (Test-ArgumentPresent $codexArgs @("-a", "--ask-for-approval"))) {
        $codexArgs += @("-a", $global:AgentToolCodexProfile.Approval)
    }

    return ,$codexArgs
}

function global:Get-AgentInteractiveCodexArgs {
    param(
        [string[]]$Arguments,
        [string]$StartupPrompt = ""
    )

    $codexArgs = Add-AgentCodexProfileArgs -Arguments @($global:AgentToolPreferredCodexCommand.Arguments) -IncludeNoAltScreen -IncludeApprovalFlag
    if ($Arguments) {
        $codexArgs += $Arguments
    } elseif (-not [string]::IsNullOrWhiteSpace($StartupPrompt)) {
        $codexArgs += $StartupPrompt
    }

    return ,$codexArgs
}

function global:Get-CodexItemText($item) {
    if ($null -eq $item) {
        return $null
    }

    if ($null -ne $item.type -and [string]$item.type -eq "agentMessage" -and $null -ne $item.text) {
        $text = [string]$item.text
        if (-not [string]::IsNullOrWhiteSpace($text)) {
            return $text
        }
    }

    if ($null -ne $item.text) {
        $text = [string]$item.text
        if (-not [string]::IsNullOrWhiteSpace($text)) {
            return $text
        }
    }

    if ($null -ne $item.content -and -not ($item.content -is [string])) {
        foreach ($part in @($item.content)) {
            if ($null -eq $part) {
                continue
            }
            if ($null -ne $part.text) {
                $text = [string]$part.text
                if (-not [string]::IsNullOrWhiteSpace($text)) {
                    return $text
                }
            }
            if ($null -ne $part.content) {
                $text = [string]$part.content
                if (-not [string]::IsNullOrWhiteSpace($text)) {
                    return $text
                }
            }
        }
    }

    if ($null -ne $item.output_text) {
        $text = [string]$item.output_text
        if (-not [string]::IsNullOrWhiteSpace($text)) {
            return $text
        }
    }

    return $null
}

function global:Get-AgentToolCodexHomeRoot {
    if (-not [string]::IsNullOrWhiteSpace($env:AGENTTOOL_CODEX_HOME_ROOT)) {
        return [System.IO.Path]::GetFullPath($env:AGENTTOOL_CODEX_HOME_ROOT)
    }

    if (-not [string]::IsNullOrWhiteSpace($env:USERPROFILE)) {
        return [System.IO.Path]::GetFullPath((Join-Path $env:USERPROFILE "codextemp\.codex\agents"))
    }

    return [System.IO.Path]::GetFullPath((Join-Path ([System.IO.Path]::GetTempPath()) "agenttool-codex-home"))
}

function global:Get-AgentToolCodexHome([string]$agentName) {
    $safeAgentName = ($agentName -replace '[^A-Za-z0-9_.-]', '_').Trim()
    if ([string]::IsNullOrWhiteSpace($safeAgentName)) {
        $safeAgentName = "agent"
    }

    return [System.IO.Path]::GetFullPath((Join-Path (Get-AgentToolCodexHomeRoot) $safeAgentName))
}

function global:Invoke-AgentBootstrapKeepAliveContract {
    param(
        [hashtable]$CommandSpec = $global:AgentToolPreferredCodexCommand
    )

    $bootstrapPrompt = Get-AgentAutoBootstrapPrompt
    if ([string]::IsNullOrWhiteSpace($bootstrapPrompt)) {
        return $null
    }
    if ($null -eq $CommandSpec) {
        $CommandSpec = Resolve-CodexCommand
    }
    if (-not $env:AGENTTOOL_CTL) {
        throw "AGENTTOOL_CTL is not set in this shell."
    }

    $schemaPath = Get-AgentBootstrapSchemaPath
    $outputSchema = Get-Content -LiteralPath $schemaPath -Encoding UTF8 -Raw | ConvertFrom-Json -ErrorAction Stop
    $developerInstructions = Get-AgentPersistentDeveloperInstructions
    if ([string]::IsNullOrWhiteSpace($developerInstructions)) {
        $developerInstructions = Get-AgentBootstrapContractPrompt
    }
    if ([string]::IsNullOrWhiteSpace($developerInstructions)) {
        throw "bootstrap developer instructions were empty"
    }

    $bootstrapOptOutMethods = @(
        "account/rateLimits/updated",
        "thread/tokenUsage/updated",
        "item/agentMessage/delta",
        "item/commandExecution/outputDelta",
        "item/started"
    )
    $server = $null
    $socket = $null
    $pendingMessages = [System.Collections.Queue]::new()
    $threadId = ""
    $turnId = ""
    $codexHome = ""
    $payloadText = ""
    $turnStatus = ""
    $completed = $false

    Write-AgentBootstrapTrace -Stage "invoke" -Message ("launcher={0}; cwd={1}; schema={2}; mode=keepalive" -f (Format-BootstrapTraceValue $CommandSpec.Display), (Format-BootstrapTraceValue $resolvedCwd), (Format-BootstrapTraceValue $schemaPath))
    Write-Host "Bootstrap : running hidden init contract..."

    try {
        $server = Start-AgentBootstrapAppServer -CommandSpec $CommandSpec
        Wait-AgentBootstrapAppServerReady -Server $server

        $socket = Connect-AgentBootstrapWebSocket -Server $server
        $global:AgentToolBootstrapRpcRequestId = 0
        $initializeResponse = Send-AgentBootstrapRpcRequest -Socket $socket -PendingMessages $pendingMessages -Method "initialize" -Params @{
            clientInfo = @{
                name = "agenttool_bootstrap"
                title = "AgentTool Bootstrap"
                version = "1.0.0"
            }
            capabilities = @{
                experimentalApi = $true
                optOutNotificationMethods = $bootstrapOptOutMethods
            }
        } -TimeoutMs 30000
        if ($null -ne $initializeResponse.result -and $null -ne $initializeResponse.result.codexHome) {
            $codexHome = [string]$initializeResponse.result.codexHome
        }
        Send-AgentBootstrapRpcMessage -Socket $socket -Message @{
            method = "initialized"
            params = @{}
        }
        Write-AgentBootstrapTrace -Stage "initialize" -Message ("codex_home={0}" -f (Format-BootstrapTraceValue $codexHome 260))

        $threadResponse = Send-AgentBootstrapRpcRequest -Socket $socket -PendingMessages $pendingMessages -Method "thread/start" -Params @{
            model = $global:AgentToolCodexProfile.Model
            cwd = $resolvedCwd
            approvalPolicy = $global:AgentToolCodexProfile.Approval
            sandbox = $global:AgentToolCodexProfile.Sandbox
            config = @{
                model_reasoning_effort = $global:AgentToolCodexProfile.ReasoningEffort
            }
            serviceName = "agenttool_bootstrap"
            personality = "pragmatic"
            developerInstructions = $developerInstructions
        } -TimeoutMs 30000
        if ($null -eq $threadResponse.result -or $null -eq $threadResponse.result.thread) {
            throw "thread/start did not return thread metadata"
        }
        $threadId = [string]$threadResponse.result.thread.id
        Write-AgentBootstrapTrace -Stage "event" -Message ("thread.started thread_id={0}" -f $threadId)

        $turnResponse = Send-AgentBootstrapRpcRequest -Socket $socket -PendingMessages $pendingMessages -Method "turn/start" -Params @{
            threadId = $threadId
            input = @(
                @{
                    type = "text"
                    text = $bootstrapPrompt
                    textElements = @()
                }
            )
            outputSchema = $outputSchema
        } -TimeoutMs 30000
        if ($null -eq $turnResponse.result -or $null -eq $turnResponse.result.turn) {
            throw "turn/start did not return turn metadata"
        }
        $turnId = [string]$turnResponse.result.turn.id
        Write-AgentBootstrapTrace -Stage "turn" -Message ("turn.started turn_id={0}" -f $turnId)

        $turnDeadline = (Get-Date).AddMinutes(5)
        while ((Get-Date) -lt $turnDeadline) {
            $remainingMs = [Math]::Max(1, [int](($turnDeadline - (Get-Date)).TotalMilliseconds))
            $message = Read-AgentBootstrapRpcQueuedMessage -Socket $socket -PendingMessages $pendingMessages -TimeoutMs $remainingMs

            if ($null -ne $message.error) {
                $messageText = if ($null -ne $message.error.message) { [string]$message.error.message } else { "unknown rpc error" }
                throw "bootstrap websocket reported error: $messageText"
            }
            if ($null -ne $message.id) {
                continue
            }
            if ($null -eq $message.method) {
                continue
            }

            $eventType = [string]$message.method
            $params = $message.params

            switch ($eventType) {
                "item/completed" {
                    $item = if ($null -ne $params) { $params.item } else { $null }
                    if ($null -ne $item -and [string]$item.type -eq "agentMessage") {
                        $payloadText = Get-CodexItemText $item
                        Write-AgentBootstrapTrace -Stage "event" -Message ("item/completed type=agentMessage; text={0}" -f (Format-BootstrapTraceValue $payloadText 260))
                    } elseif ($null -ne $item -and $null -ne $item.type) {
                        Write-AgentBootstrapTrace -Stage "event" -Message ("item/completed type={0}" -f ([string]$item.type))
                    }
                }
                "turn/completed" {
                    if ($null -ne $params.turn -and [string]$params.turn.id -eq $turnId) {
                        $turnStatus = if ($null -ne $params.turn.status) { [string]$params.turn.status } else { "" }
                        Write-AgentBootstrapTrace -Stage "event" -Message ("turn/completed turn_id={0}; status={1}" -f $turnId, $turnStatus)
                        break
                    }
                }
            }

            if (-not [string]::IsNullOrWhiteSpace($turnStatus)) {
                break
            }
        }

        if ([string]::IsNullOrWhiteSpace($threadId)) {
            throw "bootstrap app-server did not return a thread id"
        }
        if ([string]::IsNullOrWhiteSpace($turnStatus)) {
            throw "bootstrap turn did not complete"
        }
        if ($turnStatus -ne "completed") {
            throw "bootstrap turn ended with status '$turnStatus'"
        }
        if ([string]::IsNullOrWhiteSpace($payloadText)) {
            throw "bootstrap turn did not return a completed payload"
        }

        try {
            $payload = $payloadText | ConvertFrom-Json -ErrorAction Stop
        } catch {
            throw "bootstrap payload was not valid JSON: $payloadText"
        }

        $status = if ($null -ne $payload.status) { [string]$payload.status } else { "" }
        $summary = if ($null -ne $payload.summary) { [string]$payload.summary } else { "" }
        if ($status -ne "ready") {
            throw "bootstrap payload status must be 'ready', actual: $status"
        }
        if ([string]::IsNullOrWhiteSpace($summary)) {
            throw "bootstrap payload summary must not be empty"
        }

        # Active thread ownership belongs to the keepalive app-server; do not rewrite the rollout file on disk here.
        Write-AgentBootstrapTrace -Stage "sanitize" -Message "skipped rollout sanitize while keepalive app-server is active"
        & $env:AGENTTOOL_CTL mark-agent-ready --agent $AgentName --thread-id $threadId --summary $summary *> $null
        if ($LASTEXITCODE -ne 0) {
            throw "agentctl mark-agent-ready failed for $AgentName"
        }

        Write-AgentBootstrapTrace -Stage "ready" -Message ("thread_id={0}; summary={1}" -f $threadId, (Format-BootstrapTraceValue $summary 260))
        Write-Host "Bootstrap : ready -> $summary"
        $completed = $true

        if ($null -ne $socket) {
            try {
                $socket.Dispose()
            } catch {
            }
            $server.Socket = $null
            Write-AgentBootstrapTrace -Stage "bootstrap_socket" -Message "released bootstrap websocket; visible remote client will own the next attach"
        }

        return @{
            ThreadId = $threadId
            AppServer = $server
            RemoteUrl = ('ws://127.0.0.1:{0}' -f $server.Port)
            RolloutPath = if ($null -ne $threadResponse.result.thread -and $threadResponse.result.thread.path) { [string]$threadResponse.result.thread.path } else { "" }
            CodexHome = $codexHome
        }
    } finally {
        $global:AgentToolBootstrapRpcRequestId = 0
        if (-not $completed) {
            Stop-AgentBootstrapAppServer $server
        }
    }
}

function global:Invoke-AgentBootstrapContract {
    param(
        [hashtable]$CommandSpec = $global:AgentToolPreferredCodexCommand,
        [switch]$KeepAppServerAlive
    )

    if ($KeepAppServerAlive) {
        return Invoke-AgentBootstrapKeepAliveContract -CommandSpec $CommandSpec
    }

    $bootstrapPrompt = Get-AgentAutoBootstrapPrompt
    if ([string]::IsNullOrWhiteSpace($bootstrapPrompt)) {
        return $null
    }
    if ($null -eq $CommandSpec) {
        $CommandSpec = Resolve-CodexCommand
    }
    if (-not $env:AGENTTOOL_CTL) {
        throw "AGENTTOOL_CTL is not set in this shell."
    }

    $schemaPath = Get-AgentBootstrapSchemaPath
    $outputSchema = Get-Content -LiteralPath $schemaPath -Encoding UTF8 -Raw | ConvertFrom-Json -ErrorAction Stop
    $developerInstructions = Get-AgentPersistentDeveloperInstructions
    if ([string]::IsNullOrWhiteSpace($developerInstructions)) {
        $developerInstructions = Get-AgentBootstrapContractPrompt
    }
    if ([string]::IsNullOrWhiteSpace($developerInstructions)) {
        throw "bootstrap developer instructions were empty"
    }

    $server = $null
    $socket = $null
    $pendingMessages = [System.Collections.Queue]::new()
    $threadId = ""
    $turnId = ""
    $rolloutPath = ""
    $codexHome = ""
    $payloadText = ""
    $turnStatus = ""
    $errorMessages = @()
    $seenEventTypes = New-Object System.Collections.Generic.List[string]
    $completedItemKinds = New-Object System.Collections.Generic.List[string]
    $commandLine = ""
    $bootstrapOptOutMethods = @(
        "account/rateLimits/updated",
        "thread/tokenUsage/updated",
        "item/agentMessage/delta",
        "item/commandExecution/outputDelta",
        "item/started"
    )
    $completed = $false

    Write-AgentBootstrapTrace -Stage "invoke" -Message ("launcher={0}; cwd={1}; schema={2}" -f (Format-BootstrapTraceValue $CommandSpec.Display), (Format-BootstrapTraceValue $resolvedCwd), (Format-BootstrapTraceValue $schemaPath))
    Write-Host "Bootstrap : running hidden init contract..."

    try {
        $server = Start-AgentBootstrapAppServer -CommandSpec $CommandSpec
        $commandLine = $server.CommandLine
        Wait-AgentBootstrapAppServerReady -Server $server
        $socket = Connect-AgentBootstrapWebSocket -Server $server
        $global:AgentToolBootstrapRpcRequestId = 0

        $initializeResponse = Send-AgentBootstrapRpcRequest -Socket $socket -PendingMessages $pendingMessages -Method "initialize" -Params @{
            clientInfo = @{
                name = "agenttool_bootstrap"
                title = "AgentTool Bootstrap"
                version = "1.0.0"
            }
            capabilities = @{
                experimentalApi = $true
                optOutNotificationMethods = $bootstrapOptOutMethods
            }
        } -TimeoutMs 30000
        if ($null -ne $initializeResponse.result -and $null -ne $initializeResponse.result.codexHome) {
            $codexHome = [string]$initializeResponse.result.codexHome
        }
        Send-AgentBootstrapRpcMessage -Socket $socket -Message @{
            method = "initialized"
            params = @{}
        }
        Write-AgentBootstrapTrace -Stage "initialize" -Message ("codex_home={0}" -f (Format-BootstrapTraceValue $codexHome 260))

        $threadResponse = Send-AgentBootstrapRpcRequest -Socket $socket -PendingMessages $pendingMessages -Method "thread/start" -Params @{
            model = $global:AgentToolCodexProfile.Model
            cwd = $resolvedCwd
            approvalPolicy = $global:AgentToolCodexProfile.Approval
            sandbox = $global:AgentToolCodexProfile.Sandbox
            config = @{
                model_reasoning_effort = $global:AgentToolCodexProfile.ReasoningEffort
            }
            serviceName = "agenttool_bootstrap"
            personality = "pragmatic"
            developerInstructions = $developerInstructions
        } -TimeoutMs 30000

        if ($null -eq $threadResponse.result -or $null -eq $threadResponse.result.thread) {
            throw "thread/start did not return thread metadata"
        }

        $threadId = [string]$threadResponse.result.thread.id
        if ($threadResponse.result.thread.path) {
            $rolloutPath = [string]$threadResponse.result.thread.path
        }
        Write-AgentBootstrapTrace -Stage "event" -Message ("thread.started thread_id={0}; rollout_path={1}" -f $threadId, (Format-BootstrapTraceValue $rolloutPath 260))

        $turnResponse = Send-AgentBootstrapRpcRequest -Socket $socket -PendingMessages $pendingMessages -Method "turn/start" -Params @{
            threadId = $threadId
            input = @(
                @{
                    type = "text"
                    text = $bootstrapPrompt
                    textElements = @()
                }
            )
            outputSchema = $outputSchema
        } -TimeoutMs 30000

        if ($null -eq $turnResponse.result -or $null -eq $turnResponse.result.turn) {
            throw "turn/start did not return turn metadata"
        }

        $turnId = [string]$turnResponse.result.turn.id
        Write-AgentBootstrapTrace -Stage "turn" -Message ("turn.started turn_id={0}" -f $turnId)

        $turnDeadline = (Get-Date).AddMinutes(5)
        while ((Get-Date) -lt $turnDeadline) {
            $remainingMs = [Math]::Max(1, [int](($turnDeadline - (Get-Date)).TotalMilliseconds))
            $message = Read-AgentBootstrapRpcQueuedMessage -Socket $socket -PendingMessages $pendingMessages -TimeoutMs $remainingMs

            if ($null -ne $message.error) {
                $messageText = if ($null -ne $message.error.message) { [string]$message.error.message } else { "unknown rpc error" }
                $errorMessages += $messageText
                continue
            }

            if ($null -ne $message.id) {
                continue
            }

            $eventType = if ($null -ne $message.method) { [string]$message.method } else { "" }
            if ([string]::IsNullOrWhiteSpace($eventType)) {
                continue
            }

            $seenEventTypes.Add($eventType)
            $params = $message.params

            switch ($eventType) {
                "thread/started" {
                    if ($null -ne $params.thread -and $null -ne $params.thread.id) {
                        Write-AgentBootstrapTrace -Stage "event" -Message ("thread/started thread_id={0}" -f ([string]$params.thread.id))
                    }
                }
                "turn/started" {
                    if ($null -ne $params.turn -and $null -ne $params.turn.id) {
                        Write-AgentBootstrapTrace -Stage "event" -Message ("turn/started turn_id={0}" -f ([string]$params.turn.id))
                    }
                }
                "item/completed" {
                    $item = if ($null -ne $params) { $params.item } else { $null }
                    $itemType = if ($null -ne $item -and $null -ne $item.type) { [string]$item.type } else { "<unknown>" }
                    $completedItemKinds.Add($itemType)
                    if ($itemType -eq "agentMessage") {
                        $text = Get-CodexItemText $item
                        $payloadText = $text
                        Write-AgentBootstrapTrace -Stage "event" -Message ("item/completed type={0}; text={1}" -f $itemType, (Format-BootstrapTraceValue $text 260))
                    } else {
                        Write-AgentBootstrapTrace -Stage "event" -Message ("item/completed type={0}" -f $itemType)
                    }
                }
                "turn/completed" {
                    if ($null -ne $params.turn -and $null -ne $params.turn.id -and [string]$params.turn.id -eq $turnId) {
                        $turnStatus = if ($null -ne $params.turn.status) { [string]$params.turn.status } else { "" }
                        if ($null -ne $params.turn.error -and $null -ne $params.turn.error.message) {
                            $errorMessages += [string]$params.turn.error.message
                        }
                        Write-AgentBootstrapTrace -Stage "event" -Message ("turn/completed turn_id={0}; status={1}" -f $turnId, $turnStatus)

                    }
                }
            }

            if (-not [string]::IsNullOrWhiteSpace($turnStatus)) {
                break
            }
        }
    } finally {
        $global:AgentToolBootstrapRpcRequestId = 0
        if (-not $completed) {
            Stop-AgentBootstrapAppServer $server
        }
    }

    $eventSummary = if ($seenEventTypes.Count -gt 0) { ($seenEventTypes | Select-Object -Unique) -join ", " } else { "<none>" }
    $completedSummary = if ($completedItemKinds.Count -gt 0) { ($completedItemKinds | Select-Object -Unique) -join ", " } else { "<none>" }
    $errorSummary = if ($errorMessages.Count -gt 0) { ($errorMessages | Select-Object -Unique) -join " | " } else { "<none>" }
    $threadIdLabel = if ($threadId) { $threadId } else { "<none>" }
    $turnIdLabel = if ($turnId) { $turnId } else { "<none>" }
    $turnStatusLabel = if ($turnStatus) { $turnStatus } else { "<none>" }
    $hasPayload = -not [string]::IsNullOrWhiteSpace($payloadText)
    Write-AgentBootstrapTrace -Stage "summary" -Message ("event_types=[{0}]; completed_item_types=[{1}]; thread_id={2}; turn_id={3}; turn_status={4}; has_payload={5}; errors={6}" -f $eventSummary, $completedSummary, $threadIdLabel, $turnIdLabel, $turnStatusLabel, $hasPayload, (Format-BootstrapTraceValue $errorSummary 500))

    if ([string]::IsNullOrWhiteSpace($threadId)) {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message ("missing thread id; command={0}" -f (Format-BootstrapTraceValue $commandLine 320))
        throw "bootstrap app-server did not return a thread id"
    }
    if ([string]::IsNullOrWhiteSpace($turnStatus)) {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message ("turn did not complete; thread_id={0}; turn_id={1}" -f $threadId, $turnId)
        throw "bootstrap turn did not complete"
    }
    if ($turnStatus -ne "completed") {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message ("bootstrap turn ended with status={0}; errors={1}" -f $turnStatus, (Format-BootstrapTraceValue $errorSummary 500))
        throw "bootstrap turn ended with status '$turnStatus'. $errorSummary"
    }
    if ([string]::IsNullOrWhiteSpace($payloadText)) {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message ("missing completed payload; event_types=[{0}]; completed_item_types=[{1}]" -f $eventSummary, $completedSummary)
        throw "bootstrap turn did not return a completed payload. event_types=[$eventSummary]; completed_item_types=[$completedSummary]"
    }

    try {
        $payload = $payloadText | ConvertFrom-Json -ErrorAction Stop
    } catch {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message ("invalid payload json={0}" -f (Format-BootstrapTraceValue $payloadText 500))
        throw "bootstrap payload was not valid JSON: $payloadText"
    }

    $status = if ($null -ne $payload.status) { [string]$payload.status } else { "" }
    $summary = if ($null -ne $payload.summary) { [string]$payload.summary } else { "" }

    if ($status -ne "ready") {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message ("payload status not ready; status={0}; summary={1}" -f $status, (Format-BootstrapTraceValue $summary 200))
        throw "bootstrap payload status must be 'ready', actual: $status"
    }
    if ([string]::IsNullOrWhiteSpace($summary)) {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message "payload summary was empty"
        throw "bootstrap payload summary must not be empty"
    }

    if ([string]::IsNullOrWhiteSpace($rolloutPath) -or -not (Test-Path -LiteralPath $rolloutPath)) {
        if (-not [string]::IsNullOrWhiteSpace($codexHome)) {
            $sessionsDir = Join-Path $codexHome "sessions"
            if (Test-Path -LiteralPath $sessionsDir) {
                $matchedRollout = Get-ChildItem -LiteralPath $sessionsDir -Recurse -File -Filter ("*{0}*.jsonl" -f $threadId) -ErrorAction SilentlyContinue |
                    Sort-Object LastWriteTime -Descending |
                    Select-Object -First 1
                if ($null -ne $matchedRollout) {
                    $rolloutPath = $matchedRollout.FullName
                }
            }
        }
    }

    if ([string]::IsNullOrWhiteSpace($rolloutPath) -or -not (Test-Path -LiteralPath $rolloutPath)) {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message ("unable to resolve rollout path for thread_id={0}; codex_home={1}" -f $threadId, (Format-BootstrapTraceValue $codexHome 260))
        throw "unable to resolve bootstrap rollout path for thread $threadId"
    }

    Start-Sleep -Milliseconds 200
    Sanitize-AgentBootstrapRolloutWithRetry -RolloutPath $rolloutPath -BootstrapPrompt $bootstrapPrompt
    Write-AgentBootstrapTrace -Stage "sanitize" -Message ("rollout sanitized path={0}" -f (Format-BootstrapTraceValue $rolloutPath 320))

    & $env:AGENTTOOL_CTL mark-agent-ready --agent $AgentName --thread-id $threadId --summary $summary *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-AgentBootstrapTrace -Stage "failure" -Level "ERROR" -Message ("agentctl mark-agent-ready failed for {0}" -f $AgentName)
        throw "agentctl mark-agent-ready failed for $AgentName"
    }

    Write-AgentBootstrapTrace -Stage "ready" -Message ("thread_id={0}; summary={1}" -f $threadId, (Format-BootstrapTraceValue $summary 260))
    Write-Host "Bootstrap : ready -> $summary"
    $completed = $true

    return $threadId
}

function global:Start-AgentBootstrapWithRetries {
    param(
        [hashtable]$CommandSpec = $global:AgentToolPreferredCodexCommand,
        [switch]$KeepAppServerAlive
    )

    $maxAttempts = [Math]::Max(1, $BootstrapMaxAttempts)
    $retryDelaySeconds = [Math]::Max(0, $BootstrapRetryDelaySeconds)
    $initialDelaySeconds = [Math]::Max(0, $BootstrapInitialDelaySeconds)
    $lastError = $null

    if ($initialDelaySeconds -gt 0) {
        Write-AgentBootstrapTrace -Stage "delay" -Message ("startup delay {0}s before first bootstrap attempt" -f $initialDelaySeconds)
        Write-Host ("Bootstrap : startup delay {0}s for {1}" -f $initialDelaySeconds, $AgentName)
        Start-Sleep -Seconds $initialDelaySeconds
    }

    try {
        for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
            $global:AgentToolBootstrapAttempt = $attempt
            try {
                if ($attempt -gt 1) {
                    Write-AgentBootstrapTrace -Stage "retry" -Message ("starting retry attempt {0}/{1}" -f $attempt, $maxAttempts) -Level "WARN"
                    Write-Host ("Bootstrap : retry attempt {0}/{1}..." -f $attempt, $maxAttempts)
                } else {
                    Write-AgentBootstrapTrace -Stage "attempt" -Message ("starting bootstrap attempt {0}/{1}" -f $attempt, $maxAttempts)
                }
                return Invoke-AgentBootstrapContract -CommandSpec $CommandSpec -KeepAppServerAlive:$KeepAppServerAlive
            } catch {
                $lastError = $_
                Write-AgentBootstrapTrace -Stage "attempt_failed" -Level "WARN" -Message ("attempt {0}/{1} failed: {2}" -f $attempt, $maxAttempts, (Format-BootstrapTraceValue $_.Exception.Message 500))
                if ($attempt -ge $maxAttempts) {
                    break
                }

                Write-Warning ("Automatic bootstrap attempt {0}/{1} failed: {2}" -f $attempt, $maxAttempts, $_.Exception.Message)
                if ($retryDelaySeconds -gt 0) {
                    Write-AgentBootstrapTrace -Stage "retry_wait" -Message ("waiting {0}s before next attempt" -f $retryDelaySeconds)
                    Write-Host ("Bootstrap : waiting {0}s before retry..." -f $retryDelaySeconds)
                    Start-Sleep -Seconds $retryDelaySeconds
                }
            }
        }
    } finally {
        $global:AgentToolBootstrapAttempt = 0
    }

    Write-AgentBootstrapTrace -Stage "failed" -Level "ERROR" -Message ("bootstrap failed after {0} attempts" -f $maxAttempts)
    throw $lastError.Exception
}

function global:Set-AgentAppServerRegistration {
    param(
        [string]$RemoteUrl = ""
    )

    if (-not $env:AGENTTOOL_CTL) {
        throw "AGENTTOOL_CTL is not set in this shell."
    }

    $arguments = @("set-agent-app-server", "--agent", $AgentName)
    if (-not [string]::IsNullOrWhiteSpace($RemoteUrl)) {
        $arguments += @("--url", $RemoteUrl)
    }

    & $env:AGENTTOOL_CTL @arguments *> $null
    if ($LASTEXITCODE -ne 0) {
        if ([string]::IsNullOrWhiteSpace($RemoteUrl)) {
            throw "agentctl set-agent-app-server failed while clearing app-server registration for $AgentName"
        }
        throw "agentctl set-agent-app-server failed while registering $RemoteUrl for $AgentName"
    }
}

function global:Test-AgentBootstrapAppServerAlive {
    param(
        [hashtable]$Server,
        [int]$TimeoutSeconds = 2
    )

    if ($null -eq $Server) {
        return $false
    }

    $readyUri = 'http://127.0.0.1:{0}/readyz' -f $Server.Port
    $deadline = (Get-Date).AddSeconds([Math]::Max(1, $TimeoutSeconds))
    while ((Get-Date) -lt $deadline) {
        if ($null -ne $Server.Process -and $Server.Process.HasExited) {
            return $false
        }

        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $readyUri -TimeoutSec 1
            if ($response.StatusCode -eq 200) {
                return $true
            }
        } catch {
        }

        Start-Sleep -Milliseconds 200
    }

    return $false
}

function global:Get-AgentVisibleSessionTransport {
    $requested = [string]$env:AGENTTOOL_VISIBLE_SESSION_TRANSPORT
    if ([string]::IsNullOrWhiteSpace($requested)) {
        return "local"
    }

    switch ($requested.Trim().ToLowerInvariant()) {
        "remote" { return "remote" }
        "local" { return "local" }
        default { return "local" }
    }
}

function global:Resolve-AgentBootstrapRolloutPath {
    param(
        [string]$ThreadId,
        [string]$RolloutPath = "",
        [string]$CodexHome = ""
    )

    if (-not [string]::IsNullOrWhiteSpace($RolloutPath) -and (Test-Path -LiteralPath $RolloutPath)) {
        return (Resolve-Path -LiteralPath $RolloutPath).Path
    }

    if ([string]::IsNullOrWhiteSpace($ThreadId) -or [string]::IsNullOrWhiteSpace($CodexHome)) {
        return ""
    }

    $sessionsDir = Join-Path $CodexHome "sessions"
    if (-not (Test-Path -LiteralPath $sessionsDir)) {
        return ""
    }

    $matchedRollout = Get-ChildItem -LiteralPath $sessionsDir -Recurse -File -Filter ("*{0}*.jsonl" -f $ThreadId) -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($null -eq $matchedRollout) {
        return ""
    }

    return $matchedRollout.FullName
}

function global:Start-AgentCodexSession {
    param(
        [hashtable]$CommandSpec = $global:AgentToolPreferredCodexCommand,

        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    $codexCommand = $CommandSpec
    if ($null -eq $codexCommand) {
        $codexCommand = Resolve-CodexCommand
    }

    $bridgeJob = $null
    $remoteHandle = $null
    $remoteRegistered = $false
    $codexExitCode = $null
    $visibleTransport = Get-AgentVisibleSessionTransport

    try {
        $codexArgs = $null
        if ((-not $Arguments -or $Arguments.Count -eq 0) -and -not [string]::IsNullOrWhiteSpace($global:AgentToolDefaultBootstrapPrompt)) {
            try {
                $remoteHandle = Start-AgentBootstrapWithRetries -CommandSpec $codexCommand -KeepAppServerAlive
                if ($null -ne $remoteHandle -and -not [string]::IsNullOrWhiteSpace($remoteHandle.ThreadId) -and -not [string]::IsNullOrWhiteSpace($remoteHandle.RemoteUrl)) {
                    $useRemoteResume = ($visibleTransport -eq "remote") -and (Test-AgentBootstrapAppServerAlive -Server $remoteHandle.AppServer -TimeoutSeconds 2)
                    if ($useRemoteResume) {
                        Write-AgentBootstrapTrace -Stage "visible_transport" -Message ("using remote resume for thread {0}" -f $remoteHandle.ThreadId)
                        Set-AgentAppServerRegistration -RemoteUrl $remoteHandle.RemoteUrl
                        $remoteRegistered = $true
                        if ($null -eq $global:AgentToolPersistentBridgeJob) {
                            $bridgeJob = Start-AgentBridgeJob -BridgeMode "autorun" -Quiet $true
                        }
                        $codexArgs = Get-AgentInteractiveCodexArgs -Arguments @("--remote", $remoteHandle.RemoteUrl, "resume", $remoteHandle.ThreadId)
                    } else {
                        if ($visibleTransport -eq "remote") {
                            Write-AgentBootstrapTrace -Stage "remote_fallback" -Level "WARN" -Message ("app-server was not stable after bootstrap; falling back to local resume for thread {0}" -f $remoteHandle.ThreadId)
                            Write-Warning (Join-CodePoints @(0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x0020, 0x0061, 0x0070, 0x0070, 0x002D, 0x0073, 0x0065, 0x0072, 0x0076, 0x0065, 0x0072, 0x0020, 0x672A, 0x4FDD, 0x6301, 0x5B58, 0x6D3B, 0xFF0C, 0x5F53, 0x524D, 0x56DE, 0x9000, 0x5230, 0x672C, 0x5730, 0x0020, 0x0072, 0x0065, 0x0073, 0x0075, 0x006D, 0x0065, 0x0020, 0x6A21, 0x5F0F, 0x3002))
                        } else {
                            Write-AgentBootstrapTrace -Stage "visible_transport" -Message ("using local resume for thread {0}; remote transport disabled by default" -f $remoteHandle.ThreadId)
                        }

                        if ($null -eq $global:AgentToolPersistentBridgeJob) {
                            $bridgeJob = Start-AgentBridgeJob -BridgeMode "passive" -Quiet $true
                        }

                        try {
                            Set-AgentAppServerRegistration
                        } catch {
                            Write-AgentBootstrapTrace -Stage "visible_transport" -Level "WARN" -Message ("failed to clear app-server registration before local resume: {0}" -f (Format-BootstrapTraceValue $_.Exception.Message 260))
                        }

                        $rolloutPath = Resolve-AgentBootstrapRolloutPath -ThreadId $remoteHandle.ThreadId -RolloutPath $remoteHandle.RolloutPath -CodexHome $remoteHandle.CodexHome
                        Stop-AgentBootstrapAppServer $remoteHandle.AppServer
                        $remoteHandle.AppServer = $null

                        if (-not [string]::IsNullOrWhiteSpace($rolloutPath)) {
                            try {
                                Start-Sleep -Milliseconds 200
                                Sanitize-AgentBootstrapRolloutWithRetry -RolloutPath $rolloutPath -BootstrapPrompt $global:AgentToolDefaultBootstrapPrompt
                                Write-AgentBootstrapTrace -Stage "sanitize" -Message ("rollout sanitized before local resume path={0}" -f (Format-BootstrapTraceValue $rolloutPath 320))
                            } catch {
                                Write-AgentBootstrapTrace -Stage "sanitize" -Level "WARN" -Message ("failed to sanitize rollout before local resume: {0}" -f (Format-BootstrapTraceValue $_.Exception.Message 320))
                            }
                        } else {
                            Write-AgentBootstrapTrace -Stage "sanitize" -Level "WARN" -Message ("rollout path unresolved before local resume; thread_id={0}" -f $remoteHandle.ThreadId)
                        }

                        $codexArgs = Get-AgentInteractiveCodexArgs -Arguments @("resume", $remoteHandle.ThreadId)
                    }
                }
            } catch {
                Write-AgentBootstrapTrace -Stage "fallback" -Level "ERROR" -Message ("automatic bootstrap failed after retries; opening plain interactive session; reason={0}" -f (Format-BootstrapTraceValue $_.Exception.Message 500))
                Write-Warning ((Join-CodePoints @(0x81EA, 0x52A8, 0x0020, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x0020, 0x5728, 0x0020, 0x007B, 0x0030, 0x007D, 0x0020, 0x6B21, 0x5C1D, 0x8BD5, 0x540E, 0x4ECD, 0x5931, 0x8D25, 0x003A, 0x0020, 0x007B, 0x0031, 0x007D)) -f ([Math]::Max(1, $BootstrapMaxAttempts)), $_.Exception.Message)
                Write-Warning (Join-CodePoints @(0x5F53, 0x524D, 0x76F4, 0x63A5, 0x6253, 0x5F00, 0x666E, 0x901A, 0x4EA4, 0x4E92, 0xFF0C, 0x4E0D, 0x518D, 0x628A, 0x0020, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x0020, 0x63D0, 0x793A, 0x8BCD, 0x663E, 0x793A, 0x5230, 0x7A97, 0x53E3, 0x91CC, 0x3002, 0x8BF7, 0x5148, 0x67E5, 0x770B, 0x0020, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x0020, 0x65E5, 0x5FD7, 0xFF0C, 0x518D, 0x51B3, 0x5B9A, 0x662F, 0x5426, 0x624B, 0x52A8, 0x6267, 0x884C, 0x0020, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x3002))
                $codexArgs = Get-AgentInteractiveCodexArgs -Arguments @()
            }
        }

        if ($null -eq $codexArgs) {
            if ($null -eq $global:AgentToolPersistentBridgeJob -and $null -eq $bridgeJob) {
                $bridgeJob = Start-AgentBridgeJob -BridgeMode "passive" -Quiet $true
            }
            $codexArgs = Get-AgentInteractiveCodexArgs -Arguments $Arguments
        }

        Write-Host "Launching Codex: $($codexCommand.Display)"
        & $codexCommand.FilePath @codexArgs
        $codexExitCode = $LASTEXITCODE
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "codex exited with code $LASTEXITCODE"
        }
    } finally {
        if ($null -ne $remoteHandle -and $null -ne $remoteHandle.AppServer) {
            $remoteObservation = Get-AgentBootstrapAppServerObservation $remoteHandle.AppServer
            if ($null -ne $remoteObservation) {
                if ($remoteObservation.has_exited -eq $true) {
                    $processState = "exited"
                } elseif ($remoteObservation.has_exited -eq $false) {
                    $processState = "alive"
                } else {
                    $processState = "unknown"
                }

                if ($null -ne $remoteObservation.exit_code) {
                    $exitCodeLabel = [string]$remoteObservation.exit_code
                } else {
                    $exitCodeLabel = "<none>"
                }

                if ($null -ne $codexExitCode) {
                    $codexExitLabel = [string]$codexExitCode
                } else {
                    $codexExitLabel = "<unknown>"
                }

                if ($remoteObservation.has_exited -eq $true) {
                    $summary = Join-CodePoints @(0x53EF,0x89C1,0x4F1A,0x8BDD,0x7ED3,0x675F,0x65F6,0x68C0,0x6D4B,0x5230,0x20,0x6B,0x65,0x65,0x70,0x61,0x6C,0x69,0x76,0x65,0x20,0x61,0x70,0x70,0x2D,0x73,0x65,0x72,0x76,0x65,0x72,0x20,0x5DF2,0x63D0,0x524D,0x9000,0x51FA,0x3002)
                } else {
                    $summary = Join-CodePoints @(0x53EF,0x89C1,0x4F1A,0x8BDD,0x7ED3,0x675F,0x65F6,0x68C0,0x6D4B,0x5230,0x20,0x6B,0x65,0x65,0x70,0x61,0x6C,0x69,0x76,0x65,0x20,0x61,0x70,0x70,0x2D,0x73,0x65,0x72,0x76,0x65,0x72,0x20,0x4ECD,0x5728,0x5B58,0x6D3B,0x6216,0x72B6,0x6001,0x672A,0x77E5,0x3002)
                }
                Write-AgentBootstrapTrace -Stage "remote_observe" -Message ("remote_url={0}; process_state={1}; exit_code={2}; codex_exit_code={3}; stderr={4}" -f (Format-BootstrapTraceValue ([string]$remoteObservation.remote_url) 80), $processState, $exitCodeLabel, $codexExitLabel, (Format-BootstrapTraceValue ([string]$remoteObservation.stderr_tail) 260))
                Append-AgentRuntimeObservation -EventType "visible_shell_app_server_observed" -Summary $summary -Reason "interactive_session_exit" -Payload ([ordered]@{
                    remote_url = $remoteObservation.remote_url
                    port = $remoteObservation.port
                    process_id = $remoteObservation.process_id
                    process_state = $processState
                    exit_code = $remoteObservation.exit_code
                    codex_exit_code = $codexExitCode
                    stderr_tail = $remoteObservation.stderr_tail
                })
            }
        }
        if ($remoteRegistered) {
            try {
                Set-AgentAppServerRegistration
            } catch {
                Write-Warning $_.Exception.Message
            }
        }
        if ($null -ne $remoteHandle -and $null -ne $remoteHandle.AppServer) {
            Stop-AgentBootstrapAppServer $remoteHandle.AppServer
        }
        if ($null -eq $global:AgentToolPersistentBridgeJob) {
            Stop-AgentBridgeJob $bridgeJob
        }
    }
}

$resolvedCwd = (Resolve-Path -LiteralPath $Cwd).Path
$agentToolRoot = (Resolve-Path -LiteralPath (Get-AgentToolRoot)).Path
$Host.UI.RawUI.WindowTitle = $Title
Set-Location -LiteralPath $resolvedCwd

$defaultCustomLauncher = "F:\Users\schu\bin\mycodex.bat"
$envPreferredLauncher = $env:AGENTTOOL_CODEX_LAUNCHER
$global:AgentToolRequestedCodexLauncherPath = if (-not [string]::IsNullOrWhiteSpace($CodexLauncherPath)) {
    $CodexLauncherPath
} elseif (-not [string]::IsNullOrWhiteSpace($envPreferredLauncher)) {
    $envPreferredLauncher
} elseif (Test-Path -LiteralPath $defaultCustomLauncher) {
    $defaultCustomLauncher
} else {
    ""
}

$promptPath = Resolve-PromptPath -root $resolvedCwd -agentName $AgentName
$promptDisplay = if ($null -ne $promptPath) { $promptPath } else { "<not found>" }
$agentCtlPath = Resolve-AgentCtlCommand
$global:AgentToolDefaultBootstrapPrompt = $BootstrapPrompt
$global:AgentToolPersistentBridgeJob = $null
$global:AgentToolPreferredCodexCommand = Resolve-CodexCommand
$global:AgentToolCodexProfile = Get-AgentCodexProfile $AgentName
$global:AgentToolBootstrapAttempt = 0
$global:AgentToolBootstrapRpcRequestId = 0
$global:AgentToolBootstrapLogPath = Get-AgentBootstrapLogPath
$global:AgentToolUtf8NoBom = New-Object System.Text.UTF8Encoding($false)
$global:AgentToolPromptPath = $promptPath
$global:AgentToolCodexHomeRoot = Get-AgentToolCodexHomeRoot
$global:AgentToolCodexHome = Get-AgentToolCodexHome $AgentName
$global:AgentToolPreferredLauncherName = if ($global:AgentToolPreferredCodexCommand.CommandName) {
    $global:AgentToolPreferredCodexCommand.CommandName
} else {
    [System.IO.Path]::GetFileNameWithoutExtension($global:AgentToolPreferredCodexCommand.FilePath)
}

Add-Content -LiteralPath $global:AgentToolBootstrapLogPath -Value ("`r`n=== shell_started {0} ===" -f (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss.fffK")) -Encoding UTF8

if ($null -ne $agentCtlPath) {
    $env:AGENTTOOL_ROOT = $agentToolRoot
    $env:AGENTTOOL_DATA_DIR = Join-Path $agentToolRoot "data"
    $env:AGENTTOOL_RUNTIME_ENDPOINT_PATH = Join-Path $env:AGENTTOOL_DATA_DIR "runtime_endpoint.json"
    $env:AGENTTOOL_CTL = $agentCtlPath
    $agentCtlDir = Split-Path -Parent $agentCtlPath
    if (-not [string]::IsNullOrWhiteSpace($agentCtlDir)) {
        $existingPath = ($env:PATH -split ';') | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        if ($existingPath -notcontains $agentCtlDir) {
            $env:PATH = "${agentCtlDir};$env:PATH"
        }
    }
}

$resolvedRgPath = Resolve-RipgrepCommand
if (-not [string]::IsNullOrWhiteSpace($resolvedRgPath)) {
    $resolvedRgDir = Split-Path -Parent $resolvedRgPath
    $pathChanged = Prepend-PathDirectory $resolvedRgDir
    $env:AGENTTOOL_RG_PATH = $resolvedRgPath
    if ($pathChanged) {
        Write-AgentBootstrapTrace -Stage "env" -Message ("prepended rg directory to PATH: {0}" -f (Format-BootstrapTraceValue $resolvedRgDir 180))
    }
} else {
    Write-AgentBootstrapTrace -Stage "env" -Level "WARN" -Message "ripgrep executable could not be resolved during shell startup"
}

$env:AGENTTOOL_CODEX_LAUNCHER = $global:AgentToolPreferredCodexCommand.FilePath
$env:AGENTTOOL_AGENT_NAME = $AgentName
$env:AGENTTOOL_AGENT_CWD = $resolvedCwd
$env:AGENTTOOL_CODEX_HOME_ROOT = $global:AgentToolCodexHomeRoot
$env:AGENTTOOL_CODEX_HOME = $global:AgentToolCodexHome
$env:CODEX_HOME = $global:AgentToolCodexHome
$env:AGENTTOOL_CODEX_MODEL = $global:AgentToolCodexProfile.Model
$env:AGENTTOOL_CODEX_REASONING_EFFORT = $global:AgentToolCodexProfile.ReasoningEffort
$env:AGENTTOOL_CODEX_SANDBOX = $global:AgentToolCodexProfile.Sandbox
$env:AGENTTOOL_CODEX_APPROVAL = $global:AgentToolCodexProfile.Approval
if ($null -ne $promptPath) {
    $env:AGENTTOOL_PROMPT_PATH = $promptPath
}
if ($BootstrapPrompt) {
    $env:AGENTTOOL_BOOTSTRAP_PROMPT = $BootstrapPrompt
}

if (-not (Test-Path -LiteralPath $global:AgentToolCodexHome)) {
    New-Item -ItemType Directory -Path $global:AgentToolCodexHome -Force | Out-Null
}
Write-AgentBootstrapTrace -Stage "env" -Message ("agent_codex_home={0}" -f (Format-BootstrapTraceValue $global:AgentToolCodexHome 260))

if ($env:AGENTTOOL_CTL) {
    Register-AgentVisiblePaneExitHandler
    Set-AgentVisiblePaneRegistration
}

function global:codex {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    Start-AgentCodexSession -CommandSpec $global:AgentToolPreferredCodexCommand @Arguments
}

function global:agt {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    if (-not $env:AGENTTOOL_CTL) {
        throw "AGENTTOOL_CTL is not set in this shell."
    }

    & $env:AGENTTOOL_CTL @Arguments
}

function global:agentready {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$SummaryParts
    )

    if (-not $env:AGENTTOOL_CTL) {
        throw "AGENTTOOL_CTL is not set in this shell."
    }

    $arguments = @("mark-agent-ready", "--agent", $AgentName)
    $summary = if ($SummaryParts) { ($SummaryParts -join " ").Trim() } else { "" }
    if (-not [string]::IsNullOrWhiteSpace($summary)) {
        $arguments += @("--summary", $summary)
    }

    & $env:AGENTTOOL_CTL @arguments
}

function global:agentbootstrap {
    Invoke-AgentBootstrapReset
    Start-AgentBootstrapWithRetries -CommandSpec $global:AgentToolPreferredCodexCommand
}

function global:agentprompt {
    if ([string]::IsNullOrWhiteSpace($global:AgentToolDefaultBootstrapPrompt)) {
        Write-Host "No bootstrap prompt is configured for this shell."
        return
    }

    Write-Host (Get-AgentBootstrapContractPrompt)
}

function global:agentbootstraplog {
    Write-Host $global:AgentToolBootstrapLogPath
}

if ($global:AgentToolPreferredLauncherName -and $global:AgentToolPreferredLauncherName -ne "codex") {
    $launcherWrapper = {
        param(
            [Parameter(ValueFromRemainingArguments = $true)]
            [string[]]$Arguments
        )

        Start-AgentCodexSession -CommandSpec $global:AgentToolPreferredCodexCommand @Arguments
    }
    Set-Item -Path ("Function:\global:{0}" -f $global:AgentToolPreferredLauncherName) -Value $launcherWrapper
}

Write-Host ("=" * 72)
Write-Host "Agent     : $AgentName"
Write-Host "Title     : $Title"
Write-Host "Workspace : $resolvedCwd"
Write-Host "Prompt    : $promptDisplay"
Write-Host "Mode      : $StartMode"
Write-Host "Codex     : $($global:AgentToolCodexProfile.Model) / $($global:AgentToolCodexProfile.ReasoningEffort)"
Write-Host "Access    : $($global:AgentToolCodexProfile.Sandbox) / approval=$($global:AgentToolCodexProfile.Approval)"
Write-Host "CodexHome : $($global:AgentToolCodexHome)"
if ($BootstrapPrompt) {
    Write-Host "Bootstrap : enabled"
    Write-Host "Boot Log  : $global:AgentToolBootstrapLogPath"
}
if ($global:AgentToolPreferredLauncherName -and $global:AgentToolPreferredLauncherName -ne "codex") {
    Write-Host "Launcher  : $($global:AgentToolPreferredLauncherName) -> $($global:AgentToolPreferredCodexCommand.Display)"
}
Write-Host ("=" * 72)

switch ($StartMode) {
    "codex" {
        Invoke-AgentBootstrapReset
        Start-AgentCodexSession -CommandSpec $global:AgentToolPreferredCodexCommand
        break
    }
    "host" {
        Invoke-AgentBootstrapReset
        $hostCommand = Resolve-AgentHostCommand
        Write-Host "Launching AgentHost: $($hostCommand.Display)"
        & $hostCommand.FilePath @($hostCommand.Arguments) --bridge-mode autorun
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "agenthost exited with code $LASTEXITCODE"
        }
        break
    }
    default {
        $global:AgentToolPersistentBridgeJob = Start-AgentBridgeJob -BridgeMode "passive" -Quiet $true
        Invoke-AgentBootstrapReset
        $suggestedLauncher = if ($global:AgentToolPreferredLauncherName) {
            $global:AgentToolPreferredLauncherName
        } else {
            "codex"
        }

        Write-Host "Shell ready. StartMode=shell, so Codex was not launched automatically."
        Write-Host "Recommended launcher: $suggestedLauncher"
        Write-Host "Underlying command : $(Format-CodexLaunchHint $global:AgentToolPreferredCodexCommand)"
        Write-Host "Empty-arg launch auto-applies the configured model, reasoning, Full Access, and approval never."

        if ($global:AgentToolDefaultBootstrapPrompt) {
            Write-Host (Join-CodePoints @(0x7A7A, 0x53C2, 0x6570, 0x542F, 0x52A8, 0x4F1A, 0x5148, 0x5728, 0x76EE, 0x6807, 0x7EBF, 0x7A0B, 0x91CC, 0x505A, 0x9690, 0x85CF, 0x0020, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0xFF0C, 0x7136, 0x540E, 0x76F4, 0x63A5, 0x0020, 0x0072, 0x0065, 0x0073, 0x0075, 0x006D, 0x0065, 0x0020, 0x5230, 0x540C, 0x4E00, 0x4E2A, 0x4EA4, 0x4E92, 0x7EBF, 0x7A0B, 0x3002))
            Write-Host (Join-CodePoints @(0x8F85, 0x52A9, 0x547D, 0x4EE4, 0x003A, 0x0020, 0x8F93, 0x5165, 0x0020, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x0070, 0x0072, 0x006F, 0x006D, 0x0070, 0x0074, 0x0020, 0x53EF, 0x6253, 0x5370, 0x624B, 0x52A8, 0x0020, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x0020, 0x63D0, 0x793A, 0x8BCD, 0x3002))
            Write-Host (Join-CodePoints @(0x91CD, 0x8BD5, 0x547D, 0x4EE4, 0x003A, 0x0020, 0x8F93, 0x5165, 0x0020, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x0020, 0x53EF, 0x91CD, 0x65B0, 0x6267, 0x884C, 0x521D, 0x59CB, 0x5316, 0x3002))
        }

        if ($env:AGENTTOOL_CTL) {
            Write-Host ((Join-CodePoints @(0x0041, 0x0067, 0x0065, 0x006E, 0x0074, 0x0054, 0x006F, 0x006F, 0x006C, 0x0020, 0x003A, 0x0020, 0x8F93, 0x5165, 0x0020, 0x0027, 0x0061, 0x0067, 0x0074, 0x0020, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x002D, 0x0063, 0x006F, 0x006E, 0x0074, 0x0065, 0x0078, 0x0074, 0x0020, 0x002D, 0x002D, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x0020, 0x007B, 0x0030, 0x007D, 0x0027, 0x0020, 0x53EF, 0x67E5, 0x770B, 0x5F53, 0x524D, 0x72B6, 0x6001, 0x3002)) -f $AgentName)
            Write-Host (Join-CodePoints @(0x624B, 0x5DE5, 0x515C, 0x5E95, 0x003A, 0x0020, 0x81EA, 0x52A8, 0x0020, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x0020, 0x5931, 0x8D25, 0x65F6, 0xFF0C, 0x53EF, 0x5148, 0x770B, 0x0020, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x0062, 0x006F, 0x006F, 0x0074, 0x0073, 0x0074, 0x0072, 0x0061, 0x0070, 0x006C, 0x006F, 0x0067, 0xFF0C, 0x518D, 0x51B3, 0x5B9A, 0x662F, 0x5426, 0x624B, 0x52A8, 0x6267, 0x884C, 0x0020, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x0072, 0x0065, 0x0061, 0x0064, 0x0079, 0x3002))
            Write-Host ((Join-CodePoints @(0x76F4, 0x63A5, 0x4E0A, 0x62A5, 0x003A, 0x0020, 0x0061, 0x0067, 0x0074, 0x0020, 0x006D, 0x0061, 0x0072, 0x006B, 0x002D, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x002D, 0x0072, 0x0065, 0x0061, 0x0064, 0x0079, 0x0020, 0x002D, 0x002D, 0x0061, 0x0067, 0x0065, 0x006E, 0x0074, 0x0020, 0x007B, 0x0030, 0x007D, 0x0020, 0x002D, 0x002D, 0x0073, 0x0075, 0x006D, 0x006D, 0x0061, 0x0072, 0x0079, 0x0020, 0x521D, 0x59CB, 0x5316, 0x5B8C, 0x6210)) -f $AgentName)
        }
        break
    }
}
