@echo off
cd /d "%~dp0"

echo ====================================
echo   Install Auto-Start on Boot
echo ====================================
echo.

:: Create VBScript to run start.bat hidden (no console window)
echo Set WshShell = CreateObject("WScript.Shell") > start-hidden.vbs
echo WshShell.Run """" ^& "%~dp0start.bat" ^& """", 0, False >> start-hidden.vbs

:: Add to startup registry
reg add "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" /v "LatkoSyncServer" /t REG_SZ /d "wscript.exe \"%~dp0start-hidden.vbs\"" /f

if %errorlevel%==0 (
    echo.
    echo SUCCESS! Sync server will auto-start on boot.
    echo.
    echo To remove later, run: remove-autostart.bat
) else (
    echo.
    echo FAILED! Try running as Administrator.
)

echo.
pause
