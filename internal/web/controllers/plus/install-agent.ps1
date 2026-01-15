# PBS Plus Agent MSI Reinstallation Script
#Requires -RunAsAdministrator

# Configuration
$serverUrl = "{{.ServerUrl}}"
$bootstrapToken = "{{.BootstrapToken}}"
$msiUrl = "{{.ServerUrl}}/api2/json/plus/msi"

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

    # 1. Download the MSI
    if (-not (Download-FileWithRetry -Url $msiUrl -Destination $msiPath)) {
        throw "Failed to download MSI package."
    }

    # 2. Check if already installed to determine flags
    # We look for the UpgradeCode in the registry or use the REINSTALL logic
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

    # Add Reinstall flags to force overwrite if already present
    # v: runs from source and recaches
    # a: force all files to be reinstalled
    # m: rewrite registry to HKLM
    # u: rewrite registry to HKCU
    # s: overwrite shortcuts
    $msiArgs += "REINSTALL=ALL"
    $msiArgs += "REINSTALLMODE=vamus"

    Write-Host "Executing MSI with Reinstall flags..." -ForegroundColor Cyan
    $process = Start-Process -FilePath "msiexec.exe" -ArgumentList $msiArgs -Wait -PassThru

    # 1638 is the exit code for "Another version of this product is already installed"
    # If vamus didn't handle it for some reason, we could catch it here, but vamus is designed for this.
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
