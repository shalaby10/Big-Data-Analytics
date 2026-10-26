@echo off
setlocal
cd /d "%~dp0"
if not exist "gui.py" (
    echo gui.py is missing. Extract the complete ZIP before running this file.
    pause
    exit /b 1
)
rem Probe each version: an installed launcher can contain a stale default path.
for %%V in (3.10 3.11 3.12 3.13 3.14 3) do (
    py -%%V -c "import sys, tkinter; assert sys.version_info >= (3, 10)" >nul 2>&1
    if not errorlevel 1 (
        py -%%V gui.py
        if errorlevel 1 pause
        exit /b
    )
)
if exist "C:\python10\python.exe" (
    "C:\python10\python.exe" -c "import sys, tkinter; assert sys.version_info >= (3, 10)" >nul 2>&1
    if not errorlevel 1 (
        "C:\python10\python.exe" gui.py
        if errorlevel 1 pause
        exit /b
    )
)
python -c "import sys, tkinter; assert sys.version_info >= (3, 10)" >nul 2>&1
if not errorlevel 1 (
    python gui.py
    if errorlevel 1 pause
    exit /b
)
echo No working Python 3.10+ with Tkinter was found.
echo Send a screenshot of this message and the output of: py -0p
pause
exit /b 1
