@echo off
setlocal EnableExtensions
cd /d "%~dp0"

rem ------------------------------------------------------------
rem Start.bat
rem  - double click: GUI (minimized console)
rem  - task scheduler: Start.bat auto  (minimized console)
rem  - when GUI closes (python exits), the minimized console exits too
rem ------------------------------------------------------------

rem Re-launch minimized unless already minimized
if /I "%~1"=="__min" goto :MIN
start "pwtest" /min "%ComSpec%" /c ""%~f0" __min %*"
exit /b 0

:MIN
shift
cd /d "%~dp0"

rem Resolve python path (prefer local .venv, then parent .venv, else PATH python)
set "PY=%~dp0.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=%~dp0..\.venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

if /I "%~1"=="auto" (
  "%PY%" "%~dp0main.py" --auto
  exit /b %ERRORLEVEL%
)

rem Default: GUI
"%PY%" "%~dp0main.py"
exit /b %ERRORLEVEL%
