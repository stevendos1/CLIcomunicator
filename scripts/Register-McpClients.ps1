Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Test-Tool {
    param([Parameter(Mandatory = $true)][string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

$toolRoot = Split-Path -Parent $PSScriptRoot
$serverExe = Join-Path $toolRoot 'target\release\agent-hub-mcp.exe'
$serverName = 'agent-hub'
$userHome = if ($env:USERPROFILE) { $env:USERPROFILE } else { [Environment]::GetFolderPath('UserProfile') }

if (-not (Test-Tool 'cargo')) {
    throw "Rust cargo was not found. Install Rust with rustup from https://rustup.rs/ and run this script again."
}

Write-Host 'Building Rust binary in release mode...'
Push-Location $toolRoot
try {
    & cargo build --release
    if ($LASTEXITCODE -ne 0) {
        throw 'cargo build --release failed'
    }
}
finally {
    Pop-Location
}

if (-not (Test-Path $serverExe)) {
    throw "Compiled binary not found: $serverExe"
}

Write-Host 'Initializing local agent-hub state...'
& $serverExe init | Out-Null

$registered = New-Object System.Collections.Generic.List[string]

if (Test-Tool 'codex') {
    Write-Host 'Registering MCP in Codex...'
    try {
        & codex mcp remove $serverName 2>$null | Out-Null
    }
    catch {
    }
    & codex mcp add $serverName -- $serverExe
    if ($LASTEXITCODE -ne 0) {
        throw 'codex mcp add failed'
    }
    $registered.Add('Codex') | Out-Null
}
else {
    Write-Warning 'Codex CLI was not found. Skipping Codex registration.'
}

if (Test-Tool 'claude') {
    Write-Host 'Registering MCP in Claude Code...'
    try {
        & claude mcp remove -s user $serverName 2>$null | Out-Null
    }
    catch {
    }
    try {
        & claude mcp remove -s local $serverName 2>$null | Out-Null
    }
    catch {
    }
    & claude mcp add -s user $serverName -- $serverExe
    if ($LASTEXITCODE -ne 0) {
        throw 'claude mcp add failed'
    }
    $registered.Add('Claude Code') | Out-Null
}
else {
    Write-Warning 'Claude CLI was not found. Skipping Claude registration.'
}

if (Test-Tool 'gemini') {
    Write-Host 'Registering MCP in Gemini...'
    $settingsPath = Join-Path $userHome '.gemini\settings.json'
    $settingsDir = Split-Path -Parent $settingsPath
    if (-not (Test-Path $settingsDir)) {
        New-Item -ItemType Directory -Force -Path $settingsDir | Out-Null
    }
    $settings = if (Test-Path $settingsPath) {
        $raw = Get-Content -Raw $settingsPath
        if ([string]::IsNullOrWhiteSpace($raw)) {
            [pscustomobject]@{}
        }
        else {
            $raw | ConvertFrom-Json
        }
    }
    else {
        [pscustomobject]@{}
    }
    if (-not $settings.PSObject.Properties.Name.Contains('mcpServers')) {
        $settings | Add-Member -NotePropertyName mcpServers -NotePropertyValue ([pscustomobject]@{})
    }
    $settings.mcpServers | Add-Member -NotePropertyName $serverName -NotePropertyValue ([pscustomobject]@{
        command = $serverExe
        args    = @()
        env     = @{}
    }) -Force
    $settingsJson = $settings | ConvertTo-Json -Depth 20
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($settingsPath, $settingsJson, $utf8NoBom)
    $registered.Add('Gemini') | Out-Null
}
else {
    Write-Warning 'Gemini CLI was not found. Skipping Gemini registration.'
}

if ($registered.Count -eq 0) {
    Write-Warning 'No supported client CLI was detected. The binary was built successfully, but nothing was registered.'
}
else {
    Write-Host ("Done. Registered agent-hub for: {0}" -f ($registered -join ', '))
}
