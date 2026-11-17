@echo off
setlocal
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" migrate_annual_datawrapper_once.py
) else (
  python migrate_annual_datawrapper_once.py
)
echo.
echo [PREVIEW] Aucune ecriture tant que --commit n'est pas utilise.
pause
