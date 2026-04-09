param(
    [string]$BuildDir = "build",
    [string]$Config = "RelWithDebInfo"
)

$ErrorActionPreference = "Stop"

if (-not $env:VCPKG_MAX_CONCURRENCY) {
    $env:VCPKG_MAX_CONCURRENCY = if ($env:NUMBER_OF_PROCESSORS) { $env:NUMBER_OF_PROCESSORS } else { "8" }
}

if (-not $env:CMAKE_BUILD_PARALLEL_LEVEL) {
    $env:CMAKE_BUILD_PARALLEL_LEVEL = $env:VCPKG_MAX_CONCURRENCY
}

$env:VCPKG_FORCE_SYSTEM_BINARIES = "1"

$git = Get-Command git -ErrorAction SilentlyContinue
if ($git) {
    $gitDir = Split-Path $git.Source -Parent
    if (-not ($env:PATH -split ';' | Where-Object { $_ -eq $gitDir })) {
        $env:PATH = "$gitDir;$env:PATH"
    }
}

$vcpkg = Get-Command vcpkg -ErrorAction Stop
$toolchainFile = Join-Path (Split-Path $vcpkg.Source -Parent) "scripts/buildsystems/vcpkg.cmake"

if (-not (Test-Path $toolchainFile)) {
    throw "Unable to locate vcpkg toolchain file at $toolchainFile"
}

cmake -S . -B $BuildDir `
    -G "Visual Studio 17 2022" `
    -DCMAKE_TOOLCHAIN_FILE="$toolchainFile" `
    -DVCPKG_MANIFEST_MODE=ON `
    -DVCPKG_MANIFEST_DIR="$PSScriptRoot" `
    -DVCPKG_TARGET_TRIPLET=x64-windows-static `
    -DCMAKE_MSVC_RUNTIME_LIBRARY="MultiThreaded`$<`$<CONFIG:Debug>:Debug>" `
    -DCMAKE_BUILD_TYPE=$Config
