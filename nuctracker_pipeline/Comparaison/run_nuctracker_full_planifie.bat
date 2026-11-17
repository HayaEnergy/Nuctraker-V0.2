@echo off
REM HES Nuctracker v4.1 - unattended/scheduled execution, no date question.
REM WARNING: writes MongoDB and publishes both Datawrapper charts.
setlocal
cd /d "%~dp0"
set "HES_AUTOSYNC_NONINTERACTIVE=1"
if exist ".venv\Scripts\python.exe" (
  set "PYTHON=.venv\Scripts\python.exe"
) else (
  set "PYTHON=python"
)
if not exist "logs" mkdir "logs"
"%PYTHON%" -u "nuctracker_run_full.py" --publish >> "logs\full_pipeline_latest.log" 2>&1
set "RESULT=%ERRORLEVEL%"
exit /b %RESULT%
