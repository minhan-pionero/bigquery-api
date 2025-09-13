@echo off
REM ================================================================
REM Development Server with Auto-reload
REM For development purposes only
REM ================================================================

echo.
echo =====================================================
echo  BigQuery API Server - Development Mode
echo =====================================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM Activate virtual environment if it exists
if exist "venv\Scripts\activate.bat" (
    echo [INFO] Activating virtual environment...
    call venv\Scripts\activate.bat
) else (
    echo [WARNING] Virtual environment not found. Run start.bat first.
    pause
    exit /b 1
)

REM Start development server with auto-reload
echo.
echo [INFO] Starting development server with auto-reload...
echo [INFO] Server will be available at: http://localhost:8000
echo [INFO] API Documentation: http://localhost:8000/docs
echo [INFO] Files will be watched for changes
echo [INFO] Press Ctrl+C to stop the server
echo.

uvicorn main:app --host 0.0.0.0 --port 8000 --reload --log-level info

echo.
echo [INFO] Development server stopped
pause