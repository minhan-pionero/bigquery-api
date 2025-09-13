@echo off
REM ================================================================
REM Simple Environment Setup
REM ================================================================

echo.
echo =====================================================
echo  Setup Python Environment
echo =====================================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH
    echo Please install Python from https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [INFO] Python found: 
python --version

REM Create virtual environment
echo.
echo [INFO] Creating virtual environment...
if exist "venv" (
    echo [INFO] Virtual environment already exists, skipping...
) else (
    python -m venv venv
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment
        pause
        exit /b 1
    )
    echo [SUCCESS] Virtual environment created
)

REM Activate virtual environment and install dependencies
echo.
echo [INFO] Installing dependencies...
call venv\Scripts\activate.bat
pip install --upgrade pip
pip install -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Failed to install dependencies
    pause
    exit /b 1
)

echo.
echo [SUCCESS] Setup completed!
echo.
echo Next: Run start.bat to start the server
echo.
pause