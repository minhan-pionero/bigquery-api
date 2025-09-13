@echo off
REM ================================================================
REM Clean Script
REM Removes temporary files, cache, and virtual environment
REM ================================================================

echo.
echo =====================================================
echo  BigQuery API Server - Clean Script
echo =====================================================
echo.

REM Change to script directory
cd /d "%~dp0"

echo [INFO] This will remove:
echo - Virtual environment (venv folder)
echo - Python cache files (__pycache__)
echo - Log files (*.log)
echo - Temporary files
echo.
echo Do you want to continue? (y/N)
set /p confirm=
if /i not "%confirm%"=="y" (
    echo [INFO] Clean cancelled
    pause
    exit /b 0
)

echo.
echo [INFO] Starting cleanup...

REM Remove virtual environment
if exist "venv" (
    echo [INFO] Removing virtual environment...
    rmdir /s /q venv
    echo [SUCCESS] Virtual environment removed
)

REM Remove Python cache files
echo [INFO] Removing Python cache files...
for /d /r . %%d in (__pycache__) do @if exist "%%d" (
    echo [INFO] Removing %%d
    rmdir /s /q "%%d"
)

REM Remove .pyc files
echo [INFO] Removing .pyc files...
del /s /q *.pyc >nul 2>&1

REM Remove log files
echo [INFO] Removing log files...
del /q *.log >nul 2>&1

REM Remove .pytest_cache if exists
if exist ".pytest_cache" (
    echo [INFO] Removing pytest cache...
    rmdir /s /q .pytest_cache
)

REM Remove .coverage if exists
if exist ".coverage" (
    echo [INFO] Removing coverage files...
    del /q .coverage
)

echo.
echo [SUCCESS] Cleanup completed!
echo.
echo To set up the environment again, run: setup.bat
echo.
pause