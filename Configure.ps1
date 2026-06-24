param(
    [string]$BuildDir = "build",
    [string]$Config = "RelWithDebInfo"
)

$ErrorActionPreference = "Stop"

if (-not $env:CMAKE_BUILD_PARALLEL_LEVEL) {
    $env:CMAKE_BUILD_PARALLEL_LEVEL = if ($env:NUMBER_OF_PROCESSORS) { $env:NUMBER_OF_PROCESSORS } else { "8" }
}

$git = Get-Command git -ErrorAction SilentlyContinue
if ($git) {
    $gitDir = Split-Path $git.Source -Parent
    if (-not ($env:PATH -split ';' | Where-Object { $_ -eq $gitDir })) {
        $env:PATH = "$gitDir;$env:PATH"
    }
}

cmake -S . -B $BuildDir `
    -G "Visual Studio 18 2026" `
    -DCMAKE_MSVC_RUNTIME_LIBRARY="MultiThreaded`$<`$<CONFIG:Debug>:Debug>" `
    "-DCMAKE_BUILD_TYPE=${Config}"
