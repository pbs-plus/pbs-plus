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
                Write-Host "Failed to download $Url after $MaxRetries attempts" -ForegroundColor Red
                return $false
            }
        }
    }
    return $success
}

# Function to check and uninstall existing services
function Uninstall-ExistingService {
    param(
        [string]$ServiceName,
        [string]$TargetPath
    )

    try {
        $service = Get-WmiObject -Class Win32_Service -Filter "Name='$ServiceName'" -ErrorAction SilentlyContinue
        
        if ($service) {
            # Get the current path of the service from WMI
            $binPath = $service.PathName
            
            # Clean up paths for comparison
            $binPath = $binPath -replace '"', '' # Remove quotes
            $cleanTargetPath = $TargetPath -replace '"', '' # Remove quotes
            
            # Find where executable path ends (before any arguments)
            # In Windows service paths, executable name is typically followed by arguments
            if ($binPath -match '(.+\.exe)') {
                $currentExePath = $Matches[1].Trim().ToLower()
            } else {
                $currentExePath = $binPath.Trim().ToLower()
            }
            
            # Clean up the target path the same way for fair comparison
            if ($cleanTargetPath -match '(.+\.exe)') {
                $targetExePath = $Matches[1].Trim().ToLower()
            } else {
                $targetExePath = $cleanTargetPath.Trim().ToLower()
            }
            
            Write-Host "Current service executable: $currentExePath" -ForegroundColor Cyan
            Write-Host "Target executable: $targetExePath" -ForegroundColor Cyan
            
            # Compare the cleaned paths - are they actually different?
            if ($currentExePath -ne $targetExePath -and -not $currentExePath.EndsWith($targetExePath)) {
                Write-Host "$ServiceName is installed with a different executable path" -ForegroundColor Yellow
                Write-Host "Uninstalling existing service to reinstall..." -ForegroundColor Cyan
                
                # Stop service first
                Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue
                Start-Sleep -Seconds 2
                
                # Uninstall using SC command
                $result = & sc.exe delete $ServiceName
                
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "$ServiceName service successfully uninstalled" -ForegroundColor Green
                    return $true
                } else {
                    Write-Host "Failed to uninstall $ServiceName service" -ForegroundColor Red
                    return $false
                }
            } else {
                Write-Host "$ServiceName is already installed at the correct path" -ForegroundColor Green
                return $false # No need to reinstall
            }
        } else {
            Write-Host "$ServiceName service not found, will install new" -ForegroundColor Cyan
            return $false # No service to uninstall
        }
    } catch {
        Write-Host "Error checking service ${ServiceName}" -ForegroundColor Red
        return $false
    }
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

# Method 5: Find and kill any PBS Plus processes regardless of install location
Get-WmiObject Win32_Process | Where-Object { $_.CommandLine -match "pbs-plus" } | ForEach-Object {
    Write-Host "Killing process from any location: $($_.Name) (PID: $($_.ProcessId))" -ForegroundColor Cyan
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
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
    Write-Host "Failed to copy files" -ForegroundColor Red
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

# Check for global nfssessions files (could be in other install locations)
$potentialLocations = @(
    "C:\Program Files\PBS Plus Agent",
    "C:\Program Files (x86)\PBS Plus Agent",
    "C:\PBS Plus Agent",
    "C:\PBS Plus"
)

foreach ($location in $potentialLocations) {
    if (Test-Path -Path $location) {
        $nfsLock = Join-Path -Path $location -ChildPath "nfssessions.lock"
        $nfsJson = Join-Path -Path $location -ChildPath "nfssessions.json"

        $backupSessions = Join-Path -Path $location -ChildPath "backup_sessions.json"
        $backupSessionsLock = Join-Path -Path $location -ChildPath "backup_sessions.lock"
        
        $secretKey = Join-Path -Path $location -ChildPath "secret.key"
        $secretJson = Join-Path -Path $location -ChildPath "secret.json"

        if (Test-Path -Path $nfsLock) {
            Write-Host "Removing nfssessions.lock from $location" -ForegroundColor Cyan
            Remove-Item -Path $nfsLock -Force -ErrorAction SilentlyContinue
        }
        
        if (Test-Path -Path $nfsJson) {
            Write-Host "Removing nfssessions.json from $location" -ForegroundColor Cyan
            Remove-Item -Path $nfsJson -Force -ErrorAction SilentlyContinue
        }

        if (Test-Path -Path $backupSessions) {
            Write-Host "Removing backup_sessions.json from $location" -ForegroundColor Cyan
            Remove-Item -Path $backupSessions -Force -ErrorAction SilentlyContinue
        }

        if (Test-Path -Path $backupSessionsLock) {
            Write-Host "Removing backup_sessions.lock from $location" -ForegroundColor Cyan
            Remove-Item -Path $backupSessionsLock -Force -ErrorAction SilentlyContinue
        }

        if (Test-Path -Path $secretKey) {
            Write-Host "Removing secret.key from $location" -ForegroundColor Cyan
            Remove-Item -Path $secretKey -Force -ErrorAction SilentlyContinue
        }

        if (Test-Path -Path $secretJson) {
            Write-Host "Removing secret.json from $location" -ForegroundColor Cyan
            Remove-Item -Path $secretJson -Force -ErrorAction SilentlyContinue
        }
    }
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
    Write-Host "Failed to create registry settings" -ForegroundColor Red
    Read-Host -Prompt "Press Enter to exit"
    exit 1
}

# Check and uninstall services if they're installed in different locations
$agentUninstalled = Uninstall-ExistingService -ServiceName "PBSPlusAgent" -TargetPath $agentPath
$updaterUninstalled = Uninstall-ExistingService -ServiceName "PBSPlusUpdater" -TargetPath $updaterPath

# Install or start services
Write-Host "Checking PBS Plus Agent service..." -ForegroundColor Cyan
$agentService = Get-Service -Name "PBSPlusAgent" -ErrorAction SilentlyContinue
if ($agentService -and -not $agentUninstalled) {
    Write-Host "PBS Plus Agent service already installed, starting it..." -ForegroundColor Green
    try {
        Start-Service -Name "PBSPlusAgent"
        Write-Host "PBS Plus Agent service started" -ForegroundColor Green
    } catch {
        Write-Host "Failed to start PBS Plus Agent service" -ForegroundColor Red
        Write-Host "Reinstalling the service..." -ForegroundColor Yellow
        Start-Process -FilePath $agentPath -ArgumentList "install" -Wait -NoNewWindow
        Start-Sleep -Seconds 2
        Start-Service -Name "PBSPlusAgent" -ErrorAction SilentlyContinue
    }
} else {
    Write-Host "Installing PBS Plus Agent service..." -ForegroundColor Cyan
    Start-Process -FilePath $agentPath -ArgumentList "install" -Wait -NoNewWindow
    Start-Sleep -Seconds 2
    try {
        Start-Service -Name "PBSPlusAgent"
        Write-Host "PBS Plus Agent service installed and started" -ForegroundColor Green
    } catch {
        Write-Host "Failed to start PBS Plus Agent service, may need to start manually" -ForegroundColor Red
    }
}

Write-Host "Checking PBS Plus Updater service..." -ForegroundColor Cyan
$updaterService = Get-Service -Name "PBSPlusUpdater" -ErrorAction SilentlyContinue
if ($updaterService -and -not $updaterUninstalled) {
    Write-Host "PBS Plus Updater service already installed, starting it..." -ForegroundColor Green
    try {
        Start-Service -Name "PBSPlusUpdater"
        Write-Host "PBS Plus Updater service started" -ForegroundColor Green
    } catch {
        Write-Host "Failed to start PBS Plus Updater service" -ForegroundColor Red
        Write-Host "Reinstalling the service..." -ForegroundColor Yellow
        Start-Process -FilePath $updaterPath -ArgumentList "install" -Wait -NoNewWindow
        Start-Sleep -Seconds 2
        Start-Service -Name "PBSPlusUpdater" -ErrorAction SilentlyContinue
    }
} else {
    Write-Host "Installing PBS Plus Updater service..." -ForegroundColor Cyan
    Start-Process -FilePath $updaterPath -ArgumentList "install" -Wait -NoNewWindow
    Start-Sleep -Seconds 2
    try {
        Start-Service -Name "PBSPlusUpdater"
        Write-Host "PBS Plus Updater service installed and started" -ForegroundColor Green
    } catch {
        Write-Host "Failed to start PBS Plus Updater service, may need to start manually" -ForegroundColor Red
    }
}

# Verify services are running
Write-Host "Verifying services are running..." -ForegroundColor Cyan
$agentRunning = Get-Service -Name "PBSPlusAgent" -ErrorAction SilentlyContinue | Where-Object { $_.Status -eq "Running" }
$updaterRunning = Get-Service -Name "PBSPlusUpdater" -ErrorAction SilentlyContinue | Where-Object { $_.Status -eq "Running" }

if ($agentRunning) {
    Write-Host "PBS Plus Agent service is running" -ForegroundColor Green
} else {
    Write-Host "PBS Plus Agent service is not running, attempting to start..." -ForegroundColor Yellow
    Start-Service -Name "PBSPlusAgent" -ErrorAction SilentlyContinue
}

if ($updaterRunning) {
    Write-Host "PBS Plus Updater service is running" -ForegroundColor Green
} else {
    Write-Host "PBS Plus Updater service is not running, attempting to start..." -ForegroundColor Yellow
    Start-Service -Name "PBSPlusUpdater" -ErrorAction SilentlyContinue
}

# Clean up temporary files
Write-Host "Cleaning up temporary files..." -ForegroundColor Cyan
Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "Installation completed successfully." -ForegroundColor Green
