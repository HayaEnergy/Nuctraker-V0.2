@echo off
REM HES Nuctracker v4.1 - interactive date + live progress + production summary.
REM WARNING: MongoDB is UPDATED and BOTH Datawrapper charts are PUBLISHED.
setlocal
cd /d "%~dp0"
chcp 65001 >nul
REM Keep same credential behavior as previous v4 BAT.
set "HES_AUTOSYNC_NONINTERACTIVE=1"
if exist ".venv\Scripts\python.exe" (
  set "PYTHON=.venv\Scripts\python.exe"
) else (
  set "PYTHON=python"
)
if not exist "logs" mkdir "logs"
"%PYTHON%" -u "nuctracker_manual.py" --ask-date
set "RESULT=%ERRORLEVEL%"
echo.
if "%RESULT%"=="0" (
  echo NUCTRACKER TERMINE - voir la synthese de production ci-dessus.
) else (
  echo NUCTRACKER ARRETE AVEC ERREUR - voir logs\full_pipeline_latest.log.
)
echo.
echo Appuyez sur une touche pour fermer cette fenetre...
pause >nul
exit /b %RESULT%
