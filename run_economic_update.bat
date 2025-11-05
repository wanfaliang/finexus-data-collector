@echo off
REM Economic Data Update - Scheduled Task
cd /d "%~dp0"
call venv\Scripts\activate.bat
python scripts\update_economic_data.py
if %errorlevel% neq 0 (
    echo Update failed with error code %errorlevel%
    exit /b %errorlevel%
)
