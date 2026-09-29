@echo off
cd /d "%~dp0"

echo ====================================
echo   ACIS Sync Server v1.5.1
echo   Firebird 2.5 Embedded (64-bit)
echo ====================================

:: ── Bundled Firebird 2.5 64-bit Embedded ─────────────────────────
set "FIREBIRD=%~dp0firebird\bin"
set "PATH=%FIREBIRD%;%PATH%"

:: ── Python venv setup ────────────────────────────────────────────
if not exist venv\Scripts\python.exe (
    echo.
    echo [1/2] Creating virtual environment...
    python -m venv venv
    echo [2/2] Installing dependencies...
    venv\Scripts\pip install -r requirements.txt
    echo.
)

:: ── Show network info ────────────────────────────────────────────
echo.
echo Local IP addresses:
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| find "IPv4"') do echo   %%a
echo.
echo Sync server:  http://0.0.0.0:8765
echo App URL:      http://YOUR-IP:8765
echo Press Ctrl+C to stop
echo ====================================
echo.

:: ── Run sync server ──────────────────────────────────────────────
venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8765

pause
