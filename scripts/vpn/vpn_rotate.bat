@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
if not "%AHK_EXE%"=="" goto :have_ahk
set "AHK_EXE=%ProgramFiles%\AutoHotkey\AutoHotkey.exe"
if not exist "%AHK_EXE%" set "AHK_EXE=%ProgramFiles(x86)%\AutoHotkey\AutoHotkey.exe"
if not exist "%AHK_EXE%" set "AHK_EXE=%LOCALAPPDATA%\Programs\AutoHotkey\AutoHotkey.exe"

:have_ahk

if not exist "%AHK_EXE%" (
  echo AutoHotkey not found. Install AutoHotkey v2 and retry.
  exit /b 1
)

"%AHK_EXE%" "%SCRIPT_DIR%vpn_rotate.ahk" %*
endlocal
