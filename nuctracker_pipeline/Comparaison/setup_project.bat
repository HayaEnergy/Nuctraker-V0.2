@echo off
cd /d "%~dp0"

echo Creating the virtual environment...
if not exist ".venv\Scripts\python.exe" (
    where py >nul 2>nul
    if not errorlevel 1 (
        py -3 -m venv .venv
    ) else (
        python -m venv .venv
    )
)

call ".venv\Scripts\activate.bat"
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo.
echo Setup complete. You can now run run_nuctracker.bat.
pause
