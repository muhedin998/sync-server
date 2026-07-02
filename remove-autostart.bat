@echo off
cd /d "%~dp0"

echo ====================================
echo   Remove Auto-Start on Boot
echo ====================================
echo.

reg delete "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v "LatkoSyncServer" /f

if %errorlevel%==0 (
    echo.
    echo Removed. Sync server will no longer auto-start.
    if exist start-hidden.vbs del start-hidden.vbs
) else (
    echo.
    echo Entry not found or already removed.
)

echo.
pause
