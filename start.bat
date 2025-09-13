@echo off
REM ================================================================
REM Simple Start Script
REM ================================================================

echo.
echo =====================================================
echo  Starting API Server
echo =====================================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM Check if virtual environment exists
if not exist "venv\Scripts\activate.bat" (
    echo [ERROR] Virtual environment not found
    echo Please run setup.bat first
    pause
    exit /b 1
)

REM Activate virtual environment
echo [INFO] Activating virtual environment...
call venv\Scripts\activate.bat

REM Start the server
echo.
echo [INFO] Starting server...
echo [INFO] Server URL: http://localhost:8000
echo [INFO] API Docs: http://localhost:8000/docs
echo [INFO] Press Ctrl+C to stop
echo.

python run_server.py

echo.
echo [INFO] Server stopped
pause