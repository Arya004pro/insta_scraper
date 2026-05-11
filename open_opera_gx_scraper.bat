@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "OPERA_EXE=%LOCALAPPDATA%\Programs\Opera GX\opera.exe"
if not "%OPERA_GX_EXECUTABLE_PATH%"=="" set "OPERA_EXE=%OPERA_GX_EXECUTABLE_PATH%"
set "PROFILE_DIR=%SCRIPT_DIR%data\browser_state\opera_gx_user_data"

if not exist "%OPERA_EXE%" (
  echo Opera GX not found at: %OPERA_EXE%
  echo Set OPERA_GX_EXECUTABLE_PATH in your environment if it is installed elsewhere.
  pause
  exit /b 1
)

if not exist "%PROFILE_DIR%" mkdir "%PROFILE_DIR%"

start "" "%OPERA_EXE%" --new-window --no-first-run --no-default-browser-check --disable-notifications "--user-data-dir=%PROFILE_DIR%" "https://www.instagram.com/"

endlocal
