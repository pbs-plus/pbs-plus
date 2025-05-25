# PBS Plus Agent Post-Installation Script
# Handles cleanup and service restart after MSI installation

# Run as administrator check
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "This script requires administrator privileges." -ForegroundColor Red
    exit 1
}

Write-Host "PBS Plus Agent Post-Installation Cleanup" -ForegroundColor Green

# Get installation directory from registry
try {
    $installDir = Get-ItemProperty -Path "HKLM:\SOFTWARE\PBSPlus\Config" -Name "InstallPath" -ErrorAction Stop | Select-Object -ExpandProperty InstallPath
} catch {
    $installDir = "${env:ProgramFiles(x86)}\PBS Plus Agent"
}

# Clean up any existing session files to force fresh authentication
Write-Host "Cleaning up session files..." -ForegroundColor Cyan
$sessionFiles = @("nfssessions.lock", "nfssessions.json")
foreach ($file in $sessionFiles) {
    $filePath = Join-Path -Path $installDir -ChildPath $file
    if (Test-Path -Path $filePath) {
        Remove-Item -Path $filePath -Force -ErrorAction SilentlyContinue
        Write-Host "Removed $file" -ForegroundColor Gray
    }
}

# Clear authentication cache to force re-authentication with new config
Write-Host "Clearing authentication cache..." -ForegroundColor Cyan
Remove-Item -Path "HKLM:\SOFTWARE\PBSPlus\Auth" -Force -Recurse -ErrorAction SilentlyContinue

# Restart services to apply new configuration
Write-Host "Restarting services..." -ForegroundColor Cyan
$services = @("PBSPlusAgent", "PBSPlusUpdater")
foreach ($serviceName in $services) {
    try {
        $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
        if ($service) {
            if ($service.Status -eq "Running") {
                Restart-Service -Name $serviceName -Force
            } else {
                Start-Service -Name $serviceName
            }
            Write-Host "$serviceName restarted successfully" -ForegroundColor Green
        }
    }
    catch {
        Write-Host "Warning: Could not restart $serviceName" -ForegroundColor Yellow
    }
}

Write-Host "Post-installation cleanup completed!" -ForegroundColor Green
