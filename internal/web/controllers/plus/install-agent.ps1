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

[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12 -bor 3072 -bor 12288

if (-not ([System.Net.ServicePointManager]::CertificatePolicy -and [System.Net.ServicePointManager]::CertificatePolicy.GetType().Name -eq "TrustAllCertsPolicy")) {
    try {
        $code = @"
        using System.Net;
        using System.Security.Cryptography.X509Certificates;
        public class TrustAllCertsPolicy : ICertificatePolicy {
            public bool CheckValidationResult(ServicePoint srvPoint, X509Certificate certificate, WebRequest request, int certificateProblem) {
                return true;
            }
        }
"@
        Add-Type -TypeDefinition $code -ErrorAction SilentlyContinue
        [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
    } catch {
        # If Add-Type still fails, use the callback fallback
        [System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
    }
}

function Download-FileWithRetry {
    param([string]$Url, [string]$Destination)
    $retryCount = 0
    while ($retryCount -lt 3) {
        try {
            Write-Host "Downloading MSI from $Url..." -ForegroundColor Cyan
            
            # Strategy 1: Use curl.exe (Best for SSL/TLS issues in PS 5.1)
            # -k = ignore SSL, -L = follow redirects, -s = silent
            if (Get-Command curl.exe -ErrorAction SilentlyContinue) {
                curl.exe -k -L -o "$Destination" "$Url"
            } 
            else {
                # Strategy 2: Fallback to Invoke-WebRequest
                Invoke-WebRequest -Uri $Url -OutFile $Destination -UseBasicParsing
            }
            
            if (Test-Path $Destination) { 
                # Check if file is not empty (curl sometimes creates empty file on 404)
                if ((Get-Item $Destination).Length -gt 0) { return $true }
            }
        } catch {
            Write-Host "Download attempt failed: $($_.Exception.Message)" -ForegroundColor Yellow
        }
        $retryCount++
        if ($retryCount -lt 3) { Start-Sleep -Seconds 2 }
    }
    return $false
}

try {
    Write-Host "Starting PBS Plus Agent MSI Installation..." -ForegroundColor Green

    if (Test-Path -Path $oldInstallDir) {
        Write-Host "Cleaning up old installation..." -ForegroundColor Yellow
        $service = Get-Service -Name "PBSPlusAgent" -ErrorAction SilentlyContinue
        if ($service -and $service.Status -eq 'Running') {
            Stop-Service -Name "PBSPlusAgent" -Force -ErrorAction SilentlyContinue
        }
        Get-Process -Name "pbs-plus-agent" -ErrorAction SilentlyContinue | Stop-Process -Force
        Remove-Item -Path $oldInstallDir -Recurse -Force -ErrorAction SilentlyContinue
    }

    if (-not (Download-FileWithRetry -Url $msiUrl -Destination $msiPath)) {
        throw "Failed to download MSI package after retries."
    }

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

    if ($process.ExitCode -eq 0) {
        Write-Host "Installation completed successfully." -ForegroundColor Green
    } else {
        Write-Host "MSI failed with exit code $($process.ExitCode). Check log: $logPath" -ForegroundColor Red
        exit 1
    }
}
catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
finally {
    if (Test-Path $msiPath) {
        Remove-Item -Path $msiPath -Force -ErrorAction SilentlyContinue
    }
}
