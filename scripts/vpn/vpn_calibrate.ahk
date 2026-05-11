#Requires AutoHotkey v2.0
#SingleInstance Force

coordsPath := A_ScriptDir "\..\..\data\vpn_coords.ini"
statePath := A_ScriptDir "\..\..\data\vpn_state.ini"
winTitle := "ahk_exe opera.exe"

if !WinExist(winTitle) {
    MsgBox("Opera GX not found. Open Opera GX and try again.")
    ExitApp
}

WinActivate(winTitle)
WinWaitActive(winTitle)
CoordMode("Mouse", "Screen")

if FileExist(coordsPath) {
    FileDelete(coordsPath)
}
if FileExist(statePath) {
    FileDelete(statePath)
}

_capturePoint(label, index, total) {
    global winTitle
    ToolTip("Step " index "/" total " - Move mouse to: " label "`nPress F8 to capture.")
    KeyWait("F8", "D")
    MouseGetPos(&mx, &my)
    WinGetPos(&wx, &wy,,, winTitle)
    ToolTip("Captured " label " at (" (mx - wx) ", " (my - wy) ")")
    KeyWait("F8")
    Sleep(400)
    ToolTip("")
    return {x: mx - wx, y: my - wy}
}

vpnBtn := _capturePoint("VPN button in URL bar", 1, 4)
americas := _capturePoint("Location: Americas", 2, 4)
asia := _capturePoint("Location: Asia", 3, 4)
europe := _capturePoint("Location: Europe", 4, 4)

IniWrite(vpnBtn.x, coordsPath, "vpn_button", "x")
IniWrite(vpnBtn.y, coordsPath, "vpn_button", "y")
IniWrite(americas.x, coordsPath, "americas", "x")
IniWrite(americas.y, coordsPath, "americas", "y")
IniWrite(asia.x, coordsPath, "asia", "x")
IniWrite(asia.y, coordsPath, "asia", "y")
IniWrite(europe.x, coordsPath, "europe", "x")
IniWrite(europe.y, coordsPath, "europe", "y")

MsgBox("Saved VPN coordinates to: " coordsPath "`nOld coordinate/state recordings were cleared.")
ExitApp
