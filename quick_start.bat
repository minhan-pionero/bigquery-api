@echo off
REM ================================================================
REM Quick Start Script
REM One-click setup and start for new users
REM ================================================================

echo.
echo =====================================================
echo  BigQuery API Server - Quick Start
echo =====================================================
echo.
echo This script will:
echo 1. Set up Python virtual environment
echo 2. Install all dependencies
echo 3. Start the server
echo.
echo Press any key to continue or Ctrl+C to cancel...
pause >nul

REM Change to script directory
cd /d "%~dp0"

REM Run setup
echo.
echo [STEP 1/2] Setting up environment...
call setup.bat
if errorlevel 1 (
    echo [ERROR] Setup failed
    pause
    exit /b 1
)

REM Start server
echo.
echo [STEP 2/2] Starting server...
call start.bat