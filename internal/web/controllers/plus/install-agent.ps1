# PBS Plus Agent Installation Script

#Requires -RunAsAdministrator

# Set URLs and paths
$agentUrl = "{{.AgentUrl}}"

# Registry settings
$serverUrl = "{{.ServerUrl}}"
$bootstrapToken = "{{.BootstrapToken}}"

$tempDir = Join-Path -Path $env:TEMP -ChildPath "PBSPlusInstall"
$installDir = Join-Path -Path ${env:ProgramFiles(x86)} -ChildPath "PBS Plus Agent"

# Create directories if they don't exist
if (-not (Test-Path -Path $tempDir)) {
    New-Item -Path $tempDir -ItemType Directory -Force | Out-Null
}

if (-not (Test-Path -Path $installDir)) {
    New-Item -Path $installDir -ItemType Directory -Force | Out-Null
    Write-Host "Installation directory created: $installDir" -ForegroundColor Green
}

Write-Host "Configuring SSL certificate validation bypass..." -ForegroundColor Cyan
[System.Net.ServicePointManager]::ServerCertificateValidationCallback = { $true }
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls13

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

            if ($PSVersionTable.PSVersion.Major -ge 6) {
                Invoke-WebRequest -Uri $Url -OutFile $Destination -UseBasicParsing -SkipCertificateCheck
            } else {
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
                Write-Host "Failed to download $Url after $MaxRetries attempts: $($_.Exception.Message)" -ForegroundColor Red
                return $false
            }
        }
    }
    return $success
}

# Function to force kill all PBS Plus processes
function Stop-PBSPlusProcesses {
    Write-Host "Stopping all PBS Plus related processes..." -ForegroundColor Cyan

    # Stop services first
    $servicesToStop = @("PBSPlusAgent", "PBSPlusUpdater")
    foreach ($serviceName in $servicesToStop) {
        try {
            $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
            if ($service -and $service.Status -eq "Running") {
                Write-Host "Stopping service: $serviceName" -ForegroundColor Cyan
                Stop-Service -Name $serviceName -Force -ErrorAction SilentlyContinue

                # Wait for service to stop
                $timeout = 30
                do {
                    Start-Sleep -Seconds 1
                    $service = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
                    $timeout--
                } while ($service.Status -eq "Running" -and $timeout -gt 0)
            }
        }
        catch {
            Write-Host "Warning: Could not stop service $serviceName" -ForegroundColor Yellow
        }
    }

    # Kill processes by common name patterns
    $processPatterns = @("*pbs*plus*", "*pbsplus*", "*pbs-plus-agent*", "pbs-plus-updater*")
    foreach ($pattern in $processPatterns) {
        Get-Process -Name $pattern -ErrorAction SilentlyContinue | ForEach-Object {
            Write-Host "Killing process: $($_.Name) (PID: $($_.Id))" -ForegroundColor Cyan
            Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
        }
    }

    # Kill processes by executable path in install directory
    Get-Process | Where-Object {
        $_.Path -and $_.Path -like "$installDir*"
    } | ForEach-Object {
        Write-Host "Killing process from install directory: $($_.Name) (PID: $($_.Id))" -ForegroundColor Cyan
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
    }

    Start-Sleep -Seconds 3
}

# Function to completely uninstall service
function Uninstall-ServiceCompletely {
    param([string]$ServiceName)

    Write-Host "Completely uninstalling $ServiceName service..." -ForegroundColor Cyan

    try {
        $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($service) {
            # Stop the service first
            if ($service.Status -eq "Running") {
                Write-Host "Stopping $ServiceName service..." -ForegroundColor Cyan
                Stop-Service -Name $ServiceName -Force -ErrorAction SilentlyContinue

                # Wait for service to stop with timeout
                $timeout = 30
                do {
                    Start-Sleep -Seconds 1
                    $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
                    $timeout--
                } while ($service -and $service.Status -eq "Running" -and $timeout -gt 0)

                if ($timeout -le 0) {
                    Write-Host "Warning: Service $ServiceName did not stop within timeout" -ForegroundColor Yellow
                }
            }

            # Kill any remaining processes associated with the service
            $serviceProcess = Get-WmiObject -Class Win32_Service -Filter "Name='$ServiceName'" -ErrorAction SilentlyContinue
            if ($serviceProcess -and $serviceProcess.ProcessId -gt 0) {
                Write-Host "Killing service process (PID: $($serviceProcess.ProcessId))" -ForegroundColor Cyan
                Stop-Process -Id $serviceProcess.ProcessId -Force -ErrorAction SilentlyContinue
                Start-Sleep -Seconds 2
            }

            # Delete the service
            Write-Host "Deleting $ServiceName service..." -ForegroundColor Cyan
            $result = & sc.exe delete $ServiceName 2>&1

            if ($LASTEXITCODE -eq 0) {
                Write-Host "$ServiceName service deleted successfully" -ForegroundColor Green
            } elseif ($result -like "*marked for deletion*") {
                Write-Host "$ServiceName service marked for deletion, will be removed after reboot" -ForegroundColor Yellow
            } else {
                Write-Host "Warning: Failed to delete $ServiceName service: $result" -ForegroundColor Yellow
            }
        } else {
            Write-Host "$ServiceName service not found" -ForegroundColor Gray
        }
    }
    catch {
        Write-Host "Error uninstalling $ServiceName service: $($_.Exception.Message)" -ForegroundColor Red
    }
}

# Function to install and start service
function Install-AndStartService {
    param(
        [string]$ServiceName,
        [string]$ExecutablePath
    )

    Write-Host "Installing $ServiceName service..." -ForegroundColor Cyan

    try {
        # Install the service
        $installResult = & $ExecutablePath install 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Warning: Service install returned exit code $LASTEXITCODE" -ForegroundColor Yellow
        }

        Start-Sleep -Seconds 2

        # Verify service was installed
        $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if (-not $service) {
            throw "Service $ServiceName was not installed successfully"
        }

        # Start the service
        Write-Host "Starting $ServiceName service..." -ForegroundColor Cyan
        Start-Service -Name $ServiceName -ErrorAction Stop

        Start-Sleep -Seconds 2
        $service = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
        if ($service -and $service.Status -eq "Running") {
            Write-Host "$ServiceName service installed and started successfully" -ForegroundColor Green
        } else {
            Write-Host "Warning: $ServiceName service installed but may not be running properly" -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "Error installing/starting $ServiceName service: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

# Function to clean up old files from both versions
function Remove-OldFiles {
    Write-Host "Cleaning up old files from both versions..." -ForegroundColor Cyan

    # Files indicative of legacy file+secretbox version
    $legacyFiles = @(
        "secret.key",
        "master.key",
        "secret.json",
        "secrets.json",
        "nfssessions.lock",
        "nfssessions.json",
        "backup_sessions.json",
        "backup_sessions.lock",
        "pbs-plus-updater.exe",
        "*.old",
        "*.new",
        "*.backup"
    )

    # Locations to check (both legacy and current)
    $locationsToCheck = @(
        $installDir,
        "C:\Program Files\PBS Plus Agent",
        "C:\Program Files (x86)\PBS Plus Agent",
        "C:\PBS Plus Agent",
        "C:\PBS Plus",
        $env:TEMP
    )

    foreach ($location in $locationsToCheck) {
        if (Test-Path -Path $location) {
            foreach ($pattern in $legacyFiles) {
                Get-ChildItem -Path $location -Filter $pattern -File -Recurse -ErrorAction SilentlyContinue | ForEach-Object {
                    Write-Host "Removing file: $($_.FullName)" -ForegroundColor Cyan
                    Remove-Item -Path $_.FullName -Force -ErrorAction SilentlyContinue
                }
            }
        }
    }

    # Remove empty legacy directories if they exist
    $legacyDirs = @(
        "C:\PBS Plus Agent",
        "C:\PBS Plus"
    )
    foreach ($d in $legacyDirs) {
        if (Test-Path $d) {
            try {
                $items = Get-ChildItem -Path $d -Force -ErrorAction SilentlyContinue
                if ($items.Count -eq 0) {
                    Write-Host "Removing empty directory: $d" -ForegroundColor Cyan
                    Remove-Item -Path $d -Force -ErrorAction SilentlyContinue
                }
            } catch { }
        }
    }
}

# Function to clean up registry for both versions
function Cleanup-Registry {
    Write-Host "Cleaning registry for both versions..." -ForegroundColor Cyan

    # Remove legacy Auth keys entirely
    Remove-Item -Path "HKLM:\SOFTWARE\PBSPlus\Auth" -Recurse -Force -ErrorAction SilentlyContinue

    # Ensure Config exists
    if (-not (Test-Path -Path "HKLM:\SOFTWARE\PBSPlus\Config")) {
        New-Item -Path "HKLM:\SOFTWARE\PBSPlus\Config" -Force | Out-Null
    }

    # Optionally remove stray legacy values in Config that are no longer used.
    # Keep ServerURL and BootstrapToken. Remove known legacy leftovers.
    $keepValues = @("ServerURL", "BootstrapToken")
    $configPath = "HKLM:\SOFTWARE\PBSPlus\Config"

    try {
        $props = Get-ItemProperty -Path $configPath -ErrorAction SilentlyContinue
        if ($props) {
            $allNames = ($props.PSObject.Properties | Where-Object { $_.Name -notin @("PSPath","PSParentPath","PSChildName","PSDrive","PSProvider") }).Name
            foreach ($name in $allNames) {
                if ($keepValues -notcontains $name) {
                    try {
                        Remove-ItemProperty -Path $configPath -Name $name -Force -ErrorAction SilentlyContinue
                    } catch { }
                }
            }
        }
    } catch { }
}

# Main installation process
try {
    Write-Host "Starting PBS Plus Agent installation..." -ForegroundColor Green

    # Download files
    $agentTempPath = Join-Path -Path $tempDir -ChildPath "pbs-plus-agent.exe"

    Write-Host "Downloading application files..." -ForegroundColor Cyan
    $downloadAgent = Download-FileWithRetry -Url $agentUrl -Destination $agentTempPath

    if (-not ($downloadAgent)) {
        throw "One or more downloads failed. Installation cannot continue."
    }

    # Stop all PBS Plus processes
    Stop-PBSPlusProcesses

    # Always completely uninstall existing services
    Uninstall-ServiceCompletely -ServiceName "PBSPlusAgent"
    Uninstall-ServiceCompletely -ServiceName "PBSPlusUpdater"

    # Wait a moment for services to be fully removed
    Start-Sleep -Seconds 3

    # Clean up old files from both versions
    Remove-OldFiles

    # Clean up registry from both versions
    Cleanup-Registry

    # Copy new files to install directory
    $agentPath = Join-Path -Path $installDir -ChildPath "pbs-plus-agent.exe"

    Write-Host "Copying application files to installation directory..." -ForegroundColor Cyan
    Copy-Item -Path $agentTempPath -Destination $agentPath -Force
    Write-Host "Files copied successfully" -ForegroundColor Green

    # Verify files were copied correctly
    if (-not (Test-Path -Path $agentPath)) {
        throw "Failed to verify copied files"
    }

    # Create and set registry values
    Write-Host "Creating registry settings..." -ForegroundColor Cyan
    if (-not (Test-Path -Path "HKLM:\SOFTWARE\PBSPlus\Config")) {
        New-Item -Path "HKLM:\SOFTWARE\PBSPlus\Config" -Force | Out-Null
    }

    Set-ItemProperty -Path "HKLM:\SOFTWARE\PBSPlus\Config" -Name "ServerURL" -Value $serverUrl -Type String
    Set-ItemProperty -Path "HKLM:\SOFTWARE\PBSPlus\Config" -Name "BootstrapToken" -Value $bootstrapToken -Type String
    Write-Host "Registry settings created successfully" -ForegroundColor Green

    # Change to installation directory for service installation
    Set-Location -Path $installDir

    # Install and start services
    Install-AndStartService -ServiceName "PBSPlusAgent" -ExecutablePath $agentPath

    # Final verification
    Write-Host "Performing final verification..." -ForegroundColor Cyan
    $agentService = Get-Service -Name "PBSPlusAgent" -ErrorAction SilentlyContinue

    $agentRunning = $agentService -and $agentService.Status -eq "Running"

    if ($agentRunning) {
        Write-Host "Installation completed successfully. Both services are running." -ForegroundColor Green
    } else {
        Write-Host "Installation completed but services are not running. Manual intervention may be required." -ForegroundColor Red
    }
}
catch {
    Write-Host "Installation failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
finally {
    # Clean up temporary files
    Write-Host "Cleaning up temporary files..." -ForegroundColor Cyan
    Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
}

Write-Host "Installation process completed." -ForegroundColor Green
