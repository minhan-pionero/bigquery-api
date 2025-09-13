@echo off
REM ================================================================
REM API Test Script
REM Tests the API endpoints to ensure everything is working
REM ================================================================

echo.
echo =====================================================
echo  BigQuery API Server - API Tests
echo =====================================================
echo.

REM Change to script directory
cd /d "%~dp0"

REM Activate virtual environment if it exists
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
) else (
    echo [WARNING] Virtual environment not found. Using system Python.
)

REM Check if server is running
echo [INFO] Testing if server is running...
curl -s http://localhost:8000/health >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Server is not running or not accessible
    echo Please start the server first using start.bat
    pause
    exit /b 1
)

echo [SUCCESS] Server is running

REM Run basic API tests
echo.
echo [INFO] Running basic API tests...

echo.
echo [TEST] Testing root endpoint...
curl -s http://localhost:8000/ | python -m json.tool

echo.
echo [TEST] Testing health endpoint...
curl -s http://localhost:8000/health | python -m json.tool

echo.
echo [TEST] Testing LinkedIn stats...
curl -s http://localhost:8000/linkedin/stats | python -m json.tool

echo.
echo [TEST] Testing Facebook stats...
curl -s http://localhost:8000/facebook/stats | python -m json.tool

REM Run V1 table tests if available
if exist "test_v1_tables.py" (
    echo.
    echo [TEST] Running V1 tables tests...
    python test_v1_tables.py
)

echo.
echo [SUCCESS] API tests completed
echo.
echo API Documentation is available at: http://localhost:8000/docs
echo.
pause