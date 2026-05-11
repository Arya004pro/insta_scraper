#Requires AutoHotkey v2.0
#SingleInstance Force

coordsPath := A_ScriptDir "\..\..\data\vpn_coords.ini"
statePath := A_ScriptDir "\..\..\data\vpn_state.ini"
winTitle := "ahk_exe opera.exe"

if !FileExist(coordsPath) {
    MsgBox("VPN coords not found. Run vpn_calibrate.ahk first.")
    ExitApp 1
}

if !WinExist(winTitle) {
    MsgBox("Opera GX not found.")
    ExitApp 1
}

WinActivate(winTitle)
WinWaitActive(winTitle)
CoordMode("Mouse", "Screen")

order := ["americas", "asia", "europe"]
if (A_Args.Length >= 1 and StrLower(A_Args[1]) = "--with-optimal") {
    order.InsertAt(1, "optimal")
}

for section in order {
    if !_hasCoords(section) {
        MsgBox("Location coords missing for: " section ". Run vpn_calibrate.ahk again.")
        ExitApp 1
    }
}

lastIndex := IniRead(statePath, "state", "last_index", "-1")
lastIndex := lastIndex + 0
if (lastIndex < -1 or lastIndex >= order.Length) {
    lastIndex := -1
}
nextIndex := Mod(lastIndex + 1, order.Length)
target := order[nextIndex + 1]
IniWrite(nextIndex, statePath, "state", "last_index")

_hasCoords(section) {
    global coordsPath
    x := IniRead(coordsPath, section, "x", "")
    y := IniRead(coordsPath, section, "y", "")
    return !(x = "" or y = "")
}

_clickOffset(section) {
    global winTitle, coordsPath
    x := IniRead(coordsPath, section, "x", "")
    y := IniRead(coordsPath, section, "y", "")
    if (x = "" or y = "") {
        return false
    }
    WinGetPos(&wx, &wy,,, winTitle)
    Click(wx + x, wy + y)
    return true
}

if !_clickOffset("vpn_button") {
    MsgBox("VPN button coords missing.")
    ExitApp 1
}
Sleep(500)

if !_clickOffset(target) {
    MsgBox("Location coords missing for: " target)
    ExitApp 1
}
Sleep(600)
Send("{Esc}")

ExitApp
