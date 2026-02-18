@echo off
chcp 65001 >nul
title Prediction Collector - Detached Launcher
cls

echo ==========================================
echo   Prediction Collector - Detached Launch
echo ==========================================
echo.

set "PROJECT_DIR=C:\Users\kiran\.openclaw\workspace\projects\prediction_collector"
cd /d "%PROJECT_DIR%"

REM Step 1: Kill existing
echo [1/3] Stopping any running collector...
taskkill /F /IM python.exe 2>nul >nul
timeout /t 2 >nul
echo.

REM Step 2: Clear logs
echo [2/3] Clearing logs...
if exist "logs\collector.log" del /F /Q "logs\collector.log" >nul 2>&1
if exist "logs\errors\*.log" del /F /Q "logs\errors\*.log" >nul 2>&1
echo      Logs cleared.
echo.

REM Step 3: Launch with full conda activation
echo [3/3] Launching collector (detached)...

REM Use call to properly activate conda in a way that survives for the python process
call "C:\Users\kiran\anaconda3\Scripts\activate.bat" prediction_collector

REM Start minimized detached process with proper environment
start "PredictionCollector" /MIN python -m src.main

REM Wait and verify
timeout /t 3 >nul
tasklist | findstr /i "python"

echo.
echo ==========================================
echo   Collector launched
echo ==========================================
echo.
echo You can close this window - collector keeps running.
echo.
pause