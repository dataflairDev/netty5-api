<#
.SYNOPSIS
    Publishes netty-connection to Maven Central (Central Portal) and tags the release.

.DESCRIPTION
    ./release.ps1 <version> [-DryRun]

    -DryRun runs every check and the full build, publishes into build/release-dry-run/m2 (not ~/.m2) and
    verifies the signatures; it changes nothing in git and uploads nothing.

    Credentials are read by Gradle from ~/.gradle/gradle.properties or ORG_GRADLE_PROJECT_* environment
    variables. This script only checks that they exist and never prints them.
    Runs on Windows PowerShell 5.1 and PowerShell 7.

.EXAMPLE
    ./release.ps1 2.0.0 -DryRun
    ./release.ps1 2.0.0
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string] $Version,

    [switch] $DryRun
)

Set-StrictMode -Version 3.0
$ErrorActionPreference = 'Stop'

function Write-Info([string] $Message) { Write-Host "release: $Message" }
function Write-Warn([string] $Message) { Write-Host "release: warning: $Message" -ForegroundColor Yellow }

class ReleaseError : System.Exception {
    ReleaseError([string] $Message) : base($Message) {}
}
function Stop-Release([string] $Message) { throw [ReleaseError]::new($Message) }

# runs a native command, fails the release on a non-zero exit code
function Invoke-Checked([string] $Failure, [scriptblock] $Command) {
    & $Command
    if ($LASTEXITCODE -ne 0) { Stop-Release "$Failure (exit code $LASTEXITCODE)" }
}

$root = $PSScriptRoot
$gradlew = if ($env:OS -eq 'Windows_NT') { Join-Path $root 'gradlew.bat' } else { Join-Path $root 'gradlew' }
$tag = "v$Version"
$startSha = $null
$versionCommitted = $false
$published = $false

Push-Location $root
try {
    if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$') {
        Stop-Release "'$Version' is not a version like 2.0.0"
    }
    if ($Version -like '*SNAPSHOT*') { Stop-Release 'snapshot versions are not released through this script' }

    # ------------------------------------------------------------------------------------------ git checks

    $null = git rev-parse --is-inside-work-tree
    if ($LASTEXITCODE -ne 0) { Stop-Release "$root is not a git work tree" }

    $dirty = @()
    foreach ($line in @(git status --porcelain --untracked-files=all)) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }
        $path = $line.Substring(3).Trim('"')
        if ($path -eq '.idea' -or $path.StartsWith('.idea/')) {
            Write-Warn "ignoring IDE change: $line"
        } else {
            $dirty += "  $line"
        }
    }
    if ($dirty.Count -gt 0) {
        Stop-Release ("work tree is not clean, commit or stash first:`n" + ($dirty -join "`n"))
    }

    $branch = git symbolic-ref --short -q HEAD
    if ($branch -ne 'main') {
        $shown = if ($branch) { $branch } else { 'detached HEAD' }
        Stop-Release "releases are made from 'main', current branch is '$shown'"
    }

    Write-Info 'fetching origin'
    Invoke-Checked 'git fetch origin failed' { git fetch --quiet origin main --tags }
    $localSha = git rev-parse HEAD
    $remoteSha = git rev-parse origin/main
    if ($localSha -ne $remoteSha) {
        $ahead = git rev-list --count origin/main..HEAD
        $behind = git rev-list --count HEAD..origin/main
        Stop-Release "main is not in sync with origin/main ($ahead ahead, $behind behind)"
    }
    $startSha = $localSha

    $null = git rev-parse -q --verify "refs/tags/$tag"
    if ($LASTEXITCODE -eq 0) { Stop-Release "tag $tag already exists locally" }
    $null = git ls-remote --exit-code --tags origin "refs/tags/$tag"
    if ($LASTEXITCODE -eq 0) { Stop-Release "tag $tag already exists on origin" }

    # ----------------------------------------------------------------------------------- credential checks

    $gradleHome = if ($env:GRADLE_USER_HOME) { $env:GRADLE_USER_HOME } else { Join-Path $HOME '.gradle' }
    $userProps = Join-Path $gradleHome 'gradle.properties'
    $userPropsText = if (Test-Path -LiteralPath $userProps) { [IO.File]::ReadAllText($userProps) } else { '' }

    # true if Gradle property $Name is set (non-empty) via ORG_GRADLE_PROJECT_<Name> or the user gradle.properties
    function Test-GradleProperty([string] $Name) {
        $fromEnv = [Environment]::GetEnvironmentVariable("ORG_GRADLE_PROJECT_$Name")
        if (-not [string]::IsNullOrWhiteSpace($fromEnv)) { return $true }
        $pattern = '(?m)^[ \t]*' + [regex]::Escape($Name) + '[ \t]*[=:][ \t]*\S'
        return [regex]::IsMatch($userPropsText, $pattern)
    }

    $missing = @()
    if (-not (Test-GradleProperty 'mavenCentralUsername')) { $missing += 'mavenCentralUsername' }
    if (-not (Test-GradleProperty 'mavenCentralPassword')) { $missing += 'mavenCentralPassword' }
    if (Test-GradleProperty 'signingInMemoryKey') {
        Write-Info 'signing: in-memory key'
    } elseif ((Test-GradleProperty 'signing.keyId') -and (Test-GradleProperty 'signing.password') -and
              (Test-GradleProperty 'signing.secretKeyRingFile')) {
        Write-Info 'signing: key ring file'
    } else {
        $missing += 'signingInMemoryKey (or signing.keyId + signing.password + signing.secretKeyRingFile)'
    }
    if ($missing.Count -gt 0) {
        Stop-Release ("missing Gradle properties (set them in $userProps or as ORG_GRADLE_PROJECT_<name>):`n  " +
                ($missing -join "`n  "))
    }

    # ------------------------------------------------------------------------------------------- version

    $propsPath = Join-Path $root 'gradle.properties'
    $propsText = [IO.File]::ReadAllText($propsPath)
    $match = [regex]::Match($propsText, '(?m)^VERSION_NAME=([^\s]*)')
    if (-not $match.Success) { Stop-Release 'VERSION_NAME not found in gradle.properties' }
    $currentVersion = $match.Groups[1].Value

    # quoted as a whole, Windows PowerShell would otherwise split -PVERSION_NAME=2.0.0 at the dot
    $gradleArgs = @('--no-configuration-cache', "-PVERSION_NAME=$Version")

    if ($currentVersion -ne $Version) {
        if ($DryRun) {
            Write-Info "gradle.properties has VERSION_NAME=$currentVersion, building $Version without changing it (dry run)"
        } else {
            Write-Info "setting VERSION_NAME=$Version in gradle.properties"
            $updated = [regex]::Replace($propsText, '(?m)^VERSION_NAME=[^\r\n]*', "VERSION_NAME=$Version")
            [IO.File]::WriteAllText($propsPath, $updated, (New-Object System.Text.UTF8Encoding $false))
            Invoke-Checked 'could not commit the version change' { git commit --quiet -m "release $Version" -- gradle.properties }
            $versionCommitted = $true
        }
    }

    # --------------------------------------------------------------------------------------------- build

    Write-Info "building and testing $Version"
    Invoke-Checked 'build failed' { & $gradlew @gradleArgs clean build }

    if ($DryRun) {
        $dryRepo = Join-Path $root 'build/release-dry-run/m2'
        Write-Info "dry run: publishing into $dryRepo and signing"
        Invoke-Checked 'local publication or signing failed' {
            & $gradlew @gradleArgs "-Dmaven.repo.local=$dryRepo" publishToMavenLocal signMavenPublication
        }

        $group = [regex]::Match($propsText, '(?m)^GROUP=([^\s]*)').Groups[1].Value
        $dir = Join-Path $dryRepo ((($group -split '\.') + @('netty-connection', $Version)) -join '/')
        $base = "netty-connection-$Version"
        foreach ($file in @("$base.jar", "$base-sources.jar", "$base-javadoc.jar", "$base.pom", "$base.module")) {
            foreach ($candidate in @($file, "$file.asc")) {
                $full = Join-Path $dir $candidate
                if (-not (Test-Path -LiteralPath $full) -or (Get-Item -LiteralPath $full).Length -eq 0) {
                    Stop-Release "missing $full"
                }
            }
        }
        Write-Info "artifacts and signatures in ${dir}:"
        Get-ChildItem -LiteralPath $dir -File | ForEach-Object { Write-Host "  $($_.Name)" }
        Write-Info 'dry run finished: nothing was uploaded, tagged or pushed'
        exit 0
    }

    Write-Info "publishing $Version to Maven Central"
    Invoke-Checked 'publishing to Maven Central failed' { & $gradlew @gradleArgs publishAndReleaseToMavenCentral }
    $published = $true

    # ---------------------------------------------------------------------------------------- tag + push

    Invoke-Checked "$Version is published, but creating tag $tag failed" { git tag -a $tag -m "release $Version" }
    if ($versionCommitted) {
        Invoke-Checked "$Version is published, but pushing main and $tag failed; push them by hand" {
            git push --atomic origin 'HEAD:refs/heads/main' "refs/tags/$tag"
        }
    } else {
        Invoke-Checked "$Version is published, but pushing $tag failed; push it by hand" {
            git push origin "refs/tags/$tag"
        }
    }

    Write-Info "released $Version and pushed $tag"
    exit 0
} catch {
    Write-Host "release: error: $($_.Exception.Message)" -ForegroundColor Red
    if ($versionCommitted -and -not $published) {
        Write-Warn 'rolling back the version commit'
        git reset --quiet --soft $startSha
        git checkout --quiet $startSha -- gradle.properties
        if ($LASTEXITCODE -ne 0) {
            Write-Warn "rollback failed, run: git reset --soft $startSha; git checkout $startSha -- gradle.properties"
        }
    }
    exit 1
} finally {
    Pop-Location
}
