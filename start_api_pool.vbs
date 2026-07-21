' Fully silent one-click start for API Pool (no console window).
Option Explicit
Dim sh, fso, root, ps, script, cmd
Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
root = fso.GetParentFolderName(WScript.ScriptFullName)
ps = "C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
script = root & "\start_service.ps1"
If Not fso.FileExists(ps) Then WScript.Quit 1
If Not fso.FileExists(script) Then WScript.Quit 1
cmd = """" & ps & """ -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & script & """"
sh.Run cmd, 0, False
