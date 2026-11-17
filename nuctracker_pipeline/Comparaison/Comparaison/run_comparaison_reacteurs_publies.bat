@echo off
setlocal EnableExtensions
cd /d "%~dp0"
title HES NUCTRACKER - HES vs RTE / publications

echo ==============================================================
echo   HES NUCTRACKER - COMPARAISON DES REACTEURS PUBLIES
echo   Filtre : DATE DE PUBLICATION RTE (et non date d'arret)
echo   LECTURE SEULE - aucune modification de MongoDB/Datawrapper
echo ==============================================================
echo.
if exist ".venv\Scripts\python.exe" (
  set "PYTHON=.venv\Scripts\python.exe"
) else (
  set "PYTHON=python"
)
"%PYTHON%" -u comparer_reacteurs_publies_HES_RTE.py
set "EXITCODE=%ERRORLEVEL%"
echo.
if "%EXITCODE%"=="0" (
  echo [OK] Excel et CSV dans outputs_diagnostic\comparaison_reacteurs_publies\
) else (
  echo [ECHEC] Code : %EXITCODE%. Aucun graphique n'a ete publie.
)
echo.
pause
endlocal & exit /b %EXITCODE%
