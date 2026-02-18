# Run this script in PowerShell as Administrator to register the watchdog as a startup task
# This will auto-start the watchdog every time you log in to Windows

$TaskName = "PredictionCollectorWatchdog"
$WorkingDir = "C:\Users\kiran\.openclaw\workspace\projects\prediction_collector"
$WatchdogScript = "$WorkingDir\watchdog.py"
$ConfigFile = "$WorkingDir\watchdog_config.json"

# Conda environment Python (prediction_markets env)
$CondaEnvName = "prediction_markets"
$CondaEnvPath = "C:\Users\kiran\anaconda3\envs\$CondaEnvName"
$PythonPath = "$CondaEnvPath\pythonw.exe"

# Check if running as admin
$currentPrincipal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $currentPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "ERROR: Please run PowerShell as Administrator (right-click → Run as Administrator)" -ForegroundColor Red
    exit 1
}

# Check if conda Python exists
if (-not (Test-Path $PythonPath)) {
    Write-Host "ERROR: Conda environment Python not found at: $PythonPath" -ForegroundColor Red
    Write-Host "Make sure the '$CondaEnvName' conda environment exists:" -ForegroundColor Yellow
    Write-Host "  conda env list" -ForegroundColor Cyan
    Write-Host "  conda create -n $CondaEnvName python=3.11 -y" -ForegroundColor Cyan
    exit 1
}

Write-Host "Found Conda Python at: $PythonPath" -ForegroundColor Green
Write-Host "Using conda environment: $CondaEnvName" -ForegroundColor Green

# Remove existing task if it exists
$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask) {
    Write-Host "Removing existing task '$TaskName'..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# Create the action using conda environment Python
$Action = New-ScheduledTaskAction -Execute $PythonPath -Argument "`"$WatchdogScript`" `"$ConfigFile`"" -WorkingDirectory $WorkingDir

# Create the trigger (at logon for any user)
$Trigger = New-ScheduledTaskTrigger -AtLogon

# Create settings
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable:$false `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

# Register the task
Write-Host "Registering scheduled task '$TaskName'..." -ForegroundColor Cyan
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Auto-starts Prediction Collector Watchdog on boot to monitor and restart the data collection process" `
    -Force

# Enable the task
Enable-ScheduledTask -TaskName $TaskName

# Start the task immediately (optional - comment out if you do not want to start now)
Write-Host "Starting watchdog now..." -ForegroundColor Green
Start-ScheduledTask -TaskName $TaskName
Start-Sleep -Seconds 2

# Check if running
$taskInfo = Get-ScheduledTask -TaskName $TaskName
if ($taskInfo.State -eq "Running") {
    Write-Host "Watchdog is now running!" -ForegroundColor Green
} else {
    Write-Host "Task created but not yet running. It will start on next boot." -ForegroundColor Yellow
}

Write-Host "" 
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Watchdog Setup Complete!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "The watchdog will now:" -ForegroundColor White
Write-Host "  - Auto-start every time you log in" -ForegroundColor Gray
Write-Host "  - Monitor the collector process" -ForegroundColor Gray
Write-Host "  - Restart if it crashes or hangs" -ForegroundColor Gray
Write-Host "  - Write logs to: logs/watchdog.log" -ForegroundColor Gray
Write-Host "  - Write status to: data/watchdog_status.json" -ForegroundColor Gray
Write-Host ""
Write-Host "Commands:" -ForegroundColor White
Write-Host "  Check status:  $PythonPath $WatchdogScript --status" -ForegroundColor Yellow
Write-Host "  Stop:          Stop-ScheduledTask -TaskName $TaskName" -ForegroundColor Yellow
Write-Host "  Start:         Start-ScheduledTask -TaskName $TaskName" -ForegroundColor Yellow
Write-Host "  View logs:     Get-Content logs/watchdog.log -Tail 50" -ForegroundColor Yellow
Write-Host ""
