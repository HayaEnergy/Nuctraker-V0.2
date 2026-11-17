@echo off
setlocal EnableExtensions
cd /d "%~dp0"
echo ============================================================
echo  HES NUCTRACKER - Comparaison HES / RTE par jour
 echo  LECTURE SEULE - MongoDB, RTE et Datawrapper non modifies
 echo ============================================================
set /p "START=Date de debut (AAAA-MM-JJ) : "
set /p "END=Date de fin incluse (AAAA-MM-JJ) : "
set /p "HOUR=Heure du snapshot (HH:MM, Entree = 10:00) : "
if not defined HOUR set "HOUR=10:00"
if exist ".venv\Scripts\python.exe" (
 set "PYTHON=.venv\Scripts\python.exe"
) else (
 set "PYTHON=python"
)
"%PYTHON%" comparer_disponibilites_quotidiennes.py --start "%START%" --end "%END%" --hour "%HOUR%"
if errorlevel 1 (
 echo.
 echo [ECHEC] Ne pas utiliser des fichiers d'ancienne execution. Verifier les messages ci-dessus.
) else (
 echo.
 echo [OK] Ouvre outputs_diagnostic\comparaison_quotidienne_HES_RTE\ dans Explorateur.
)
echo.
pause
endlocal
