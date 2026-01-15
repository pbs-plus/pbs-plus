# PBS Plus Agent MSI Installation Script
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
    Write-Host "Starting PBS Plus Agent MSI Installation..." -ForegroundColor Green

    # 1. Download the MSI
    if (-not (Download-FileWithRetry -Url $msiUrl -Destination $msiPath)) {
        throw "Failed to download MSI package."
    }

    # 2. Run the MSI silently
    # We pass the public properties SERVERURL and BOOTSTRAPTOKEN to the command line
    # /i = install, /qn = quiet (no UI), /norestart = self-explanatory, /L*V = verbose log
    $logPath = Join-Path -Path $tempDir -ChildPath "install.log"
    $msiArgs = @(
        "/i", "`"$msiPath`"",
        "SERVERURL=`"$serverUrl`"",
        "BOOTSTRAPTOKEN=`"$bootstrapToken`"",
        "/qn",
        "/norestart",
        "/L*V", "`"$logPath`""
    )

    Write-Host "Executing MSI..." -ForegroundColor Cyan
    $process = Start-Process -FilePath "msiexec.exe" -ArgumentList $msiArgs -Wait -PassThru

    if ($process.ExitCode -ne 0) {
        Write-Host "MSI failed with exit code $($process.ExitCode). Check log at $logPath" -ForegroundColor Red
        exit 1
    }

    Write-Host "MSI Installation completed successfully." -ForegroundColor Green
}
catch {
    Write-Host "Installation failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
finally {
    # Clean up
    Remove-Item -Path $msiPath -Force -ErrorAction SilentlyContinue
}
