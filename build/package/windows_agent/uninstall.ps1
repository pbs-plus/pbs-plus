# PBS Plus Agent Pre-Uninstallation Cleanup Script

# Run as administrator check
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "This script requires administrator privileges." -ForegroundColor Red
    exit 1
}

Write-Host "PBS Plus Agent Pre-Uninstallation Cleanup" -ForegroundColor Yellow

# Get installation directory
try {
    $installDir = Get-ItemProperty -Path "HKLM:\SOFTWARE\PBSPlus\Config" -Name "InstallPath" -ErrorAction Stop | Select-Object -ExpandProperty InstallPath
} catch {
    $installDir = "${env:ProgramFiles(x86)}\PBS Plus Agent"
}

# Stop services
Write-Host "Stopping services..." -ForegroundColor Cyan
$services = @("PBSPlusAgent", "PBSPlusUpdater")
foreach ($serviceName in $services) {
    try {
        $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
        if ($service -and $service.Status -eq "Running") {
            Stop-Service -Name $serviceName -Force -ErrorAction SilentlyContinue
            Write-Host "$serviceName stopped" -ForegroundColor Gray
        }
    }
    catch {
        Write-Host "Could not stop $serviceName" -ForegroundColor Yellow
    }
}

# Kill any remaining processes
Write-Host "Terminating remaining processes..." -ForegroundColor Cyan
$processNames = @("pbs-plus-agent", "pbs-plus-updater")
foreach ($procName in $processNames) {
    Get-Process -Name $procName -ErrorAction SilentlyContinue | ForEach-Object {
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        Write-Host "Killed $($_.Name)" -ForegroundColor Gray
    }
}

# Clean up session files
Write-Host "Cleaning up files..." -ForegroundColor Cyan
if (Test-Path -Path $installDir) {
    $sessionFiles = @("nfssessions.lock", "nfssessions.json")
    foreach ($file in $sessionFiles) {
        $filePath = Join-Path -Path $installDir -ChildPath $file
        if (Test-Path -Path $filePath) {
            Remove-Item -Path $filePath -Force -ErrorAction SilentlyContinue
        }
    }
}

# Clean up registry
Write-Host "Cleaning up registry..." -ForegroundColor Cyan
Remove-Item -Path "HKLM:\SOFTWARE\PBSPlus\Auth" -Force -Recurse -ErrorAction SilentlyContinue
Remove-Item -Path "HKLM:\SOFTWARE\PBSPlus\Services" -Force -Recurse -ErrorAction SilentlyContinue

Write-Host "Pre-uninstallation cleanup completed!" -ForegroundColor Green
