<#
.SYNOPSIS
Creates a source-only ZIP archive of this repository.

.DESCRIPTION
Archives files tracked by Git from the current working tree. Build products, IDE state,
local environment files, and common private-key formats are excluded defensively. This
means the archive includes source, tests, migrations, tracked generated jOOQ code, Gradle
wrapper files, and design documentation, while omitting untracked machine-local artifacts.

.EXAMPLE
.\scripts\Export-SourceArchive.ps1

.EXAMPLE
.\scripts\Export-SourceArchive.ps1 -OutputPath .\dist\kra-source.zip -Force
#>
[CmdletBinding()]
param(
    [string]$OutputPath,
    [string[]]$AdditionalExclude = @(),
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$projectName = Split-Path -Leaf $projectRoot

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw 'Git is required because this archive intentionally contains tracked source files only.'
}

if (-not $OutputPath) {
    $timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $OutputPath = Join-Path (Split-Path -Parent $projectRoot) "$projectName-source-$timestamp.zip"
}

$archivePath = [System.IO.Path]::GetFullPath($OutputPath)
if ((Test-Path -LiteralPath $archivePath) -and -not $Force) {
    throw "Archive already exists: $archivePath. Use -Force to replace it."
}

function Test-IsExcluded {
    param([string]$RelativePath)

    $normalisedPath = $RelativePath.Replace('\', '/')
    $fileName = [System.IO.Path]::GetFileName($normalisedPath)

    # Local build and editor state. These match the repository's ignored artifacts.
    if ($normalisedPath -match '(^|/)(\.git|\.gradle|\.idea|\.kotlin|\.vscode|\.structurizr|build|out|bin|node_modules)(/|$)') {
        return $true
    }

    # Keep documented templates, but never include a live dotenv file.
    if ($fileName -match '(?i)^\.env($|\.(?!example$|sample$|template$))') {
        return $true
    }

    # Do not rely on Git ignore rules alone for credentials that might be accidentally tracked.
    if ($fileName -match '(?i)\.(pem|key|p12|pfx|jks|keystore|kdb)$' -or
        $fileName -match '(?i)^(id_rsa|id_dsa|id_ecdsa|id_ed25519|credentials(?:\.json)?)$' -or
        $normalisedPath -match '(?i)(^|/)(secrets?|private-keys?)(/|$)') {
        return $true
    }

    foreach ($pattern in $AdditionalExclude) {
        if ($normalisedPath -like $pattern) {
            return $true
        }
    }

    return $false
}

$trackedFiles = @(& git -C $projectRoot ls-files)
if ($LASTEXITCODE -ne 0) {
    throw "Could not list tracked files in $projectRoot."
}

$filesToArchive = @(
    $trackedFiles |
        Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
        Where-Object { -not (Test-IsExcluded $_) }
)

if ($filesToArchive.Count -eq 0) {
    throw 'No source artifacts were selected for the archive.'
}

$outputDirectory = Split-Path -Parent $archivePath
if (-not (Test-Path -LiteralPath $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
}

$stagingDirectory = Join-Path ([System.IO.Path]::GetTempPath()) "kra-source-$([guid]::NewGuid().ToString('N'))"
$archiveRoot = Join-Path $stagingDirectory $projectName

try {
    New-Item -ItemType Directory -Path $archiveRoot -Force | Out-Null

    foreach ($relativePath in $filesToArchive) {
        $sourcePath = Join-Path $projectRoot $relativePath
        if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) {
            Write-Warning "Skipping tracked file missing from the working tree: $relativePath"
            continue
        }

        $destinationPath = Join-Path $archiveRoot $relativePath
        $destinationDirectory = Split-Path -Parent $destinationPath
        New-Item -ItemType Directory -Path $destinationDirectory -Force | Out-Null
        Copy-Item -LiteralPath $sourcePath -Destination $destinationPath -Force
    }

    if (Test-Path -LiteralPath $archivePath) {
        Remove-Item -LiteralPath $archivePath -Force
    }

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    [System.IO.Compression.ZipFile]::CreateFromDirectory(
        $stagingDirectory,
        $archivePath,
        [System.IO.Compression.CompressionLevel]::Optimal,
        $false
    )

    $archiveSize = (Get-Item -LiteralPath $archivePath).Length
    Write-Host "Created $archivePath with $($filesToArchive.Count) tracked source artifacts ($archiveSize bytes)."
}
finally {
    if (Test-Path -LiteralPath $stagingDirectory) {
        Remove-Item -LiteralPath $stagingDirectory -Recurse -Force
    }
}
