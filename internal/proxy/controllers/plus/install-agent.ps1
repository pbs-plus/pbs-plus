# PBS Plus Agent Installation Script
# PowerShell version with HTTP downloads and integrated registry settings

# Run as administrator check
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "This script requires administrator privileges." -ForegroundColor Red
    Write-Host "Please run as administrator." -ForegroundColor Red
    Read-Host -Prompt "Press Enter to exit"
    exit 1
}

# Set URLs and paths
$agentUrl = "{{.AgentUrl}}"
$updaterUrl = "{{.UpdaterUrl}}"

# Registry settings
$serverUrl = "{{.ServerUrl}}"
$bootstrapToken = "{{.BootstrapToken}}"

$tempDir = Join-Path -Path $env:TEMP -ChildPath "PBSPlusInstall"
$installDir = Join-Path -Path ${env:ProgramFiles(x86)} -ChildPath "PBS Plus Agent"

# Create temp directory if it doesn't exist
if (-not (Test-Path -Path $tempDir)) {
    New-Item -Path $tempDir -ItemType Directory -Force | Out-Null
}

# Create installation directory if it doesn't exist
if (-not (Test-Path -Path $installDir)) {
    New-Item -Path $installDir -ItemType Directory -Force | Out-Null
    Write-Host "Installation directory created: $installDir" -ForegroundColor Green
}
#
# Configure SSL certificate validation bypass
Write-Host "Configuring SSL certificate validation bypass..." -ForegroundColor Cyan
# For .NET Framework - this works for PowerShell 5.1 and earlier
[System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12

# Function to download file with retry
function Download-FileWithRetry {
    param(
        [string]$Url,
        [string]$Destination,
        [int]$MaxRetries = 3,
        [int]$RetryDelay = 5
    )

    $retryCount = 0
    $success = $false

    while (-not $success -and $retryCount -lt $MaxRetries) {
        try {
            Write-Host "Downloading $Url to $Destination" -ForegroundColor Cyan
              # Check PowerShell version to use appropriate method to ignore SSL validation
            if ($PSVersionTable.PSVersion.Major -ge 6) {
                # PowerShell Core (6+) has the SkipCertificateCheck parameter
                Invoke-WebRequest -Uri $Url -OutFile $Destination -UseBasicParsing -SkipCertificateCheck
            } else {
                # PowerShell 5.1 and earlier - we already set ServicePointManager globally above
                Invoke-WebRequest -Uri $Url -OutFile $Destination -UseBasicParsing
            }
            
            if (Test-Path -Path $Destination) {
                $success = $true
                Write-Host "Downloaded successfully: $Destination" -ForegroundColor Green
            }
        }
        catch {
            $retryCount++
            if ($retryCount -lt $MaxRetries) {
                Write-Host "Download failed. Retrying in $RetryDelay seconds... (Attempt $retryCount of $MaxRetries)" -ForegroundColor Yellow
                Start-Sleep -Seconds $RetryDelay
            }
            else {
                Write-Host "Failed to download $Url after $MaxRetries attempts: $_" -ForegroundColor Red
                return $false
            }
        }
    }
    return $success
}

# Download files
$agentTempPath = Join-Path -Path $tempDir -ChildPath "pbs-plus-agent.exe"
$updaterTempPath = Join-Path -Path $tempDir -ChildPath "pbs-plus-updater.exe"

Write-Host "Downloading application files..." -ForegroundColor Cyan
$downloadAgent = Download-FileWithRetry -Url $agentUrl -Destination $agentTempPath
$downloadUpdater = Download-FileWithRetry -Url $updaterUrl -Destination $updaterTempPath

if (-not ($downloadAgent -and $downloadUpdater)) {
    Write-Host "One or more downloads failed. Installation cannot continue." -ForegroundColor Red
    Read-Host -Prompt "Press Enter to exit"
    exit 1
}

# Kill all PBS Plus processes - using multiple approaches to ensure all processes are terminated
Write-Host "Killing all PBS Plus related processes..." -ForegroundColor Cyan

# Method 1: Kill by service name - get PIDs from services and kill them
$servicesToKill = @("PBSPlusAgent", "PBSPlusUpdater")
foreach ($service in $servicesToKill) {
    try {
        $svc = Get-WmiObject -Class Win32_Service -Filter "Name='$service'" -ErrorAction SilentlyContinue
        if ($svc -and $svc.ProcessId -gt 0) {
            Write-Host "Killing process associated with $service service (PID: $($svc.ProcessId))" -ForegroundColor Cyan
            Stop-Process -Id $svc.ProcessId -Force -ErrorAction SilentlyContinue
        }
    }
    catch {
        Write-Host "Warning: Could not find or kill service process for $service" -ForegroundColor Yellow
    }
}

# Method 2: Kill any process containing both "pbs" and "plus" in the name (case-insensitive)
Get-Process | Where-Object { $_.Name -match "pbs" -and $_.Name -match "plus" } | ForEach-Object {
    Write-Host "Killing process: $($_.Name) (PID: $($_.Id))" -ForegroundColor Cyan
    Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
}

# Method 3: Try direct process names we expect
$processNames = @("pbs-plus-agent", "pbs-plus-updater", "pbsplusagent", "pbsplusupdater")
foreach ($procName in $processNames) {
    Get-Process -Name $procName -ErrorAction SilentlyContinue | ForEach-Object {
        Write-Host "Killing process: $($_.Name) (PID: $($_.Id))" -ForegroundColor Cyan
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
    }
}

# Method 4: Look for processes with executables in the install directory
Get-Process | Where-Object { $_.Path -like "$installDir*" } | ForEach-Object {
    Write-Host "Killing process from install directory: $($_.Name) (PID: $($_.Id))" -ForegroundColor Cyan
    Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
}

# Give processes time to fully terminate
Start-Sleep -Seconds 2

# Copy files from temp to install directory
$agentPath = Join-Path -Path $installDir -ChildPath "pbs-plus-agent.exe"
$updaterPath = Join-Path -Path $installDir -ChildPath "pbs-plus-updater.exe"

Write-Host "Copying application files to installation directory..." -ForegroundColor Cyan
try {
    Copy-Item -Path $agentTempPath -Destination $agentPath -Force
    Copy-Item -Path $updaterTempPath -Destination $updaterPath -Force
    Write-Host "Files copied successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to copy files: $_" -ForegroundColor Red
    Read-Host -Prompt "Press Enter to exit"
    exit 1
}

# Verify files were copied correctly
if (-not (Test-Path -Path $agentPath)) {
    Write-Host "Failed to verify pbs-plus-agent.exe" -ForegroundColor Red
    Read-Host -Prompt "Press Enter to exit"
    exit 1
}
if (-not (Test-Path -Path $updaterPath)) {
    Write-Host "Failed to verify pbs-plus-updater.exe" -ForegroundColor Red
    Read-Host -Prompt "Press Enter to exit"
    exit 1
}

# Change to installation directory
Set-Location -Path $installDir

# Delete nfssessions files if they exist
Write-Host "Deleting nfssessions files..." -ForegroundColor Cyan
$nfsLockPath = Join-Path -Path $installDir -ChildPath "nfssessions.lock"
$nfsJsonPath = Join-Path -Path $installDir -ChildPath "nfssessions.json"

if (Test-Path -Path $nfsLockPath) {
    Remove-Item -Path $nfsLockPath -Force
}
if (Test-Path -Path $nfsJsonPath) {
    Remove-Item -Path $nfsJsonPath -Force
}

# Delete registry keys
Write-Host "Deleting registry keys..." -ForegroundColor Cyan
Remove-Item -Path "HKLM:\SOFTWARE\PBSPlus\Auth" -Force -ErrorAction SilentlyContinue
if ($?) {
    Write-Host "Auth registry key deleted successfully" -ForegroundColor Green
}
else {
    Write-Host "Auth registry key not found or unable to delete" -ForegroundColor Yellow
}

# Create and set registry values
Write-Host "Creating registry settings..." -ForegroundColor Cyan
try {
    # Create the Config key if it doesn't exist
    if (-not (Test-Path -Path "HKLM:\SOFTWARE\PBSPlus\Config")) {
        New-Item -Path "HKLM:\SOFTWARE\PBSPlus" -Name "Config" -Force | Out-Null
    }
    
    # Set the registry values
    Set-ItemProperty -Path "HKLM:\SOFTWARE\PBSPlus\Config" -Name "ServerURL" -Value $serverUrl -Type String
    Set-ItemProperty -Path "HKLM:\SOFTWARE\PBSPlus\Config" -Name "BootstrapToken" -Value $bootstrapToken -Type String
    
    Write-Host "Registry settings created successfully" -ForegroundColor Green
}
catch {
    Write-Host "Failed to create registry settings: $_" -ForegroundColor Red
    Read-Host -Prompt "Press Enter to exit"
    exit 1
}

# Check and install services
Write-Host "Checking PBS Plus Agent service..." -ForegroundColor Cyan
$agentService = Get-Service -Name "PBSPlusAgent" -ErrorAction SilentlyContinue
if ($agentService) {
    Write-Host "PBS Plus Agent service is already installed" -ForegroundColor Green
}
else {
    Write-Host "Installing PBS Plus Agent service..." -ForegroundColor Cyan
    Start-Process -FilePath $agentPath -ArgumentList "install" -NoNewWindow
}

Write-Host "Checking PBS Plus Updater service..." -ForegroundColor Cyan
$updaterService = Get-Service -Name "PBSPlusUpdater" -ErrorAction SilentlyContinue
if ($updaterService) {
    Write-Host "PBS Plus Updater service is already installed" -ForegroundColor Green
}
else {
    Write-Host "Installing PBS Plus Updater service..." -ForegroundColor Cyan
    Start-Process -FilePath $updaterPath -ArgumentList "install" -NoNewWindow
}

# Start services
Write-Host "Starting PBS Plus Agent service..." -ForegroundColor Cyan
try {
    Start-Service -Name "PBSPlusAgent"
    Write-Host "PBS Plus Agent service started" -ForegroundColor Green
}
catch {
    Write-Host "Failed to start PBS Plus Agent service, start it manually." -ForegroundColor Red
}

Write-Host "Starting PBS Plus Updater service..." -ForegroundColor Cyan
try {
    Start-Service -Name "PBSPlusUpdater"
    Write-Host "PBS Plus Updater service started" -ForegroundColor Green
}
catch {
    Write-Host "Failed to start PBS Plus Updater service, start it manually." -ForegroundColor Red
}

# Clean up temporary files
Write-Host "Cleaning up temporary files..." -ForegroundColor Cyan
Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Installation completed successfully." -ForegroundColor Green
Read-Host -Prompt "Press Enter to exit"
