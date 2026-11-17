@echo off
REM HES Nuctracker v4.1 - interactive and visible, MongoDB updated, DW NOT published.
setlocal
cd /d "%~dp0"
chcp 65001 >nul
set "HES_AUTOSYNC_NONINTERACTIVE=1"
if exist ".venv\Scripts\python.exe" (
  set "PYTHON=.venv\Scripts\python.exe"
) else (
  set "PYTHON=python"
)
if not exist "logs" mkdir "logs"
"%PYTHON%" -u "nuctracker_manual.py" --ask-date --no-publish
set "RESULT=%ERRORLEVEL%"
echo.
echo Ce test PEUT mettre MongoDB a jour. Les graphiques Datawrapper ne sont PAS republies.
echo Appuyez sur une touche pour fermer...
pause >nul
exit /b %RESULT%
