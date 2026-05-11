@echo off
setlocal

set "OPERA_EXE=%LOCALAPPDATA%\Programs\Opera GX\opera.exe"
if exist "%ProgramFiles%\Opera GX\opera.exe" set "OPERA_EXE=%ProgramFiles%\Opera GX\opera.exe"
if exist "%ProgramFiles(x86)%\Opera GX\opera.exe" set "OPERA_EXE=%ProgramFiles(x86)%\Opera GX\opera.exe"
if not exist "%OPERA_EXE%" (
  set "OPERA_EXE=%LOCALAPPDATA%\Programs\Opera GX\launcher.exe"
  if exist "%ProgramFiles%\Opera GX\launcher.exe" set "OPERA_EXE=%ProgramFiles%\Opera GX\launcher.exe"
  if exist "%ProgramFiles(x86)%\Opera GX\launcher.exe" set "OPERA_EXE=%ProgramFiles(x86)%\Opera GX\launcher.exe"
)

if not exist "%OPERA_EXE%" (
  echo Opera GX executable not found. Set OPERA_EXE manually in this script.
  exit /b 1
)

set "ROOT_DIR=%~dp0..\.."
for %%I in ("%ROOT_DIR%") do set "ROOT_DIR=%%~fI"
set "PROFILE_DIR=%ROOT_DIR%\data\browser_state\opera_gx_user_data"
if not exist "%PROFILE_DIR%" mkdir "%PROFILE_DIR%"

echo Launching Opera GX scraper profile with remote debugging on 127.0.0.1:9222...
start "" "%OPERA_EXE%" ^
  --new-window ^
  --remote-debugging-port=9222 ^
  --remote-debugging-address=127.0.0.1 ^
  --user-data-dir="%PROFILE_DIR%" ^
  --no-first-run ^
  --no-default-browser-check ^
  "about:blank"

echo Waiting for CDP endpoint...
for /L %%I in (1,1,20) do (
  powershell -NoProfile -Command "$ErrorActionPreference='Stop'; try { (Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:9222/json/version' -TimeoutSec 2 -ErrorAction Stop) | Out-Null; exit 0 } catch { exit 1 }"
  if not errorlevel 1 goto :cdp_ready
  timeout /t 1 >nul
)

echo WARNING: CDP endpoint did not come up on http://127.0.0.1:9222
echo Scraper runs will fail fast until CDP is reachable. Re-run this bat.
exit /b 0

:cdp_ready
echo CDP endpoint is live. You can now start npm start and run from UI.

endlocal
