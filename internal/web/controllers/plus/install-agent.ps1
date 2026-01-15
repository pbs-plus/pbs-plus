# PBS Plus Agent MSI Reinstallation Script
#Requires -RunAsAdministrator

# Configuration
$serverUrl = "{{.ServerUrl}}"
$bootstrapToken = "{{.BootstrapToken}}"
$msiUrl = "{{.ServerUrl}}/api2/json/plus/msi"
$oldInstallDir = "${env:ProgramFiles(x86)}\PBS Plus Agent"

$tempDir = Join-Path -Path $env:TEMP -ChildPath "PBSPlusInstall"
$msiPath = Join-Path -Path $tempDir -ChildPath "pbs-plus-agent.msi"

# Create temp directory
if (-not (Test-Path -Path $tempDir)) {
    New-Item -Path $tempDir -ItemType Directory -Force | Out-Null
}

Write-Host "Configuring Network Protocols..." -ForegroundColor Cyan
[System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12 -bor [System.Net.SecurityProtocolType]::Tls13

function Download-FileWithRetry {
    param([string]$Url, [string]$Destination)
    $retryCount = 0
    while ($retryCount -lt 3) {
        try {
            Write-Host "Downloading MSI from $Url..." -ForegroundColor Cyan
            Invoke-WebRequest -Uri $Url -OutFile $Destination -UseBasicParsing
            if (Test-Path $Destination) { return $true }
        } catch {
            $retryCount++
            Write-Host "Download failed. Retry $retryCount/3..." -ForegroundColor Yellow
            Start-Sleep -Seconds 5
        }
    }
    return $false
}

try {
    Write-Host "Starting PBS Plus Agent MSI Installation/Reinstallation..." -ForegroundColor Green

    # 1. Cleanup Old Files
    if (Test-Path -Path $oldInstallDir) {
        Write-Host "Detected old installation at $oldInstallDir. Cleaning up..." -ForegroundColor Yellow
        
        # Attempt to stop service if it exists to unlock files
        $service = Get-Service -Name "PBSPlusAgent" -ErrorAction SilentlyContinue
        if ($service -and $service.Status -eq 'Running') {
            Stop-Service -Name "PBSPlusAgent" -Force -ErrorAction SilentlyContinue
        }
        
        # Kill process if still running
        Get-Process -Name "pbs-plus-agent" -ErrorAction SilentlyContinue | Stop-Process -Force
        
        # Remove the directory
        Remove-Item -Path $oldInstallDir -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "Old directory removed." -ForegroundColor Cyan
    }

    # 2. Download the MSI
    if (-not (Download-FileWithRetry -Url $msiUrl -Destination $msiPath)) {
        throw "Failed to download MSI package."
    }

    # 3. Check if already installed to determine flags
    $logPath = Join-Path -Path $tempDir -ChildPath "install.log"
    
    # Base arguments
    $msiArgs = @(
        "/i", "`"$msiPath`"",
        "SERVERURL=`"$serverUrl`"",
        "BOOTSTRAPTOKEN=`"$bootstrapToken`"",
        "/qn",
        "/norestart",
        "/L*V", "`"$logPath`""
    )

    Write-Host "Executing MSI with Reinstall flags..." -ForegroundColor Cyan
    $process = Start-Process -FilePath "msiexec.exe" -ArgumentList $msiArgs -Wait -PassThru

    if ($process.ExitCode -eq 0) {
        Write-Host "Installation/Reinstallation completed successfully." -ForegroundColor Green
    } else {
        Write-Host "MSI failed with exit code $($process.ExitCode). Check log at $logPath" -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host "Installation failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
finally {
    Remove-Item -Path $msiPath -Force -ErrorAction SilentlyContinue
}
