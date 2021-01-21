;--------------------------------
; CUBRID ADO.NET Data Provider Installer/Uninstaller
; Ver 11.0.0
; Last update: JAN 2021
;--------------------------------

SetCompressor /solid lzma

;--------------------------------
;Include Modern UI
!include "MUI2.nsh"
!include "x64.nsh"
;--------------------------------
;Interface Settings

!define MUI_ABORTWARNING

!define MUI_FINISHPAGE_NOAUTOCLOSE
!define MUI_UNFINISHPAGE_NOAUTOCLOSE

!define MUI_ICON cubrid.ico
!define MUI_UNICON cubrid.ico

;--------------------------------

;.NET required version
!define DOT_MAJOR "4"
!define DOT_MINOR "5"
!define DOT_MINOR_MINOR "0"

!include WordFunc.nsh
!insertmacro VersionCompare

!include LogicLib.nsh
!include CheckDotNetVersion.nsh

; Include functions and plugin
!addplugindir "." 

; The name of the installer
Name "CUBRID ADO.NET Data Provider 11.0.0"

; The file to write
OutFile "CUBRID ADO.NET Data Provider 11.0.0.0001.exe"

;required on Windows Vista and Windows 7
RequestExecutionLevel user

; The default installation directory
InstallDir "$PROGRAMFILES\CUBRID\CUBRID ADO.NET Data Provider 11.0.0"

; Registry key to check for installation directory
InstallDirRegKey HKLM "Software\CUBRID\CUBRID ADO.NET Data Provider 11.0.0" "Install_Dir"

ShowInstDetails show
ShowUnInstDetails show

;--------------------------------

; Pages

!insertmacro MUI_PAGE_WELCOME
!insertmacro MUI_PAGE_LICENSE "BSD License.txt"
!insertmacro MUI_PAGE_COMPONENTS
!insertmacro MUI_PAGE_DIRECTORY
!insertmacro MUI_PAGE_INSTFILES
!insertmacro MUI_PAGE_FINISH

!insertmacro MUI_UNPAGE_WELCOME  
!insertmacro MUI_UNPAGE_CONFIRM
!insertmacro MUI_UNPAGE_INSTFILES
!insertmacro MUI_UNPAGE_FINISH

;--------------------------------
;Languages

!insertmacro MUI_LANGUAGE "English"

;--------------------------------

; Components to install
Section "CUBRID ADO.NET Data Provider Files" DataProviderFiles

  SectionIn RO

  ; Set output path to the installation directory.
  SetOutPath $INSTDIR
  
  CreateDirectory "$OUTDIR"

  File "/oname=$OUTDIR\BSD License.txt" "BSD License.txt"
  File "/oname=$OUTDIR\Release notes.txt" "Release notes.txt"
  File "/oname=$OUTDIR\CUBRID.Data.dll" "..\..\Code\Src\bin\Release\CUBRID.Data.dll"
  File "/oname=$OUTDIR\CUBRID ADO.NET Data Provider.chm" "..\..\Documentation\CUBRID ADO.NET Data Provider.chm"
  ${If} ${RunningX64}
    File "/oname=$OUTDIR\cascci.dll" "cascci64.dll"
    File "/oname=C:\Windows\SysWOW64\cascci.dll" "cascci32.dll"
  ${Else}
    File "/oname=$OUTDIR\cascci.dll" "cascci32.dll"
  ${EndIf}

  ; Write the installation path
  WriteRegStr HKLM "SOFTWARE\CUBRID\CUBRID ADO.NET Data Provider 11.0.0" "Install_Dir" "$INSTDIR"
  
  ; Write the uninstall information
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\CUBRID ADO.NET Data Provider 11.0.0" "DisplayName" "CUBRID ADO.NET Data Provider 11.0.0"
  WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\CUBRID ADO.NET Data Provider 11.0.0" "UninstallString" '"$INSTDIR\uninstall.exe"'
  WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\CUBRID ADO.NET Data Provider 11.0.0" "NoModify" 1
  WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\CUBRID ADO.NET Data Provider 11.0.0" "NoRepair" 1
  ReadRegStr $0 HKLM "SYSTEM\CurrentControlSet\Control\Session Manager\Environment" "Path"
  WriteRegExpandStr HKLM "SYSTEM\CurrentControlSet\Control\Session Manager\Environment" "Path" "$0;$INSTDIR;"
  WriteUninstaller "uninstall.exe"
  
SectionEnd


; Optional section (can be disabled by the user)
Section "Start Menu shortcuts" DataProviderShortcuts

  CreateDirectory "$SMPROGRAMS\CUBRID\CUBRID ADO.NET Data Provider 11.0.0"
  CreateShortCut "$SMPROGRAMS\CUBRID\CUBRID ADO.NET Data Provider 11.0.0\CUBRID ADO.NET Data Provider Help file.lnk" "$INSTDIR\CUBRID ADO.NET Data Provider.chm" "" "$INSTDIR\CUBRID ADO.NET Data Provider.chm" 0
  CreateShortCut "$SMPROGRAMS\CUBRID\CUBRID ADO.NET Data Provider 11.0.0\Uninstall.lnk" "$INSTDIR\uninstall.exe" "" "$INSTDIR\uninstall.exe" 0
  
SectionEnd

;--------------------------------
;Descriptions

!insertmacro MUI_FUNCTION_DESCRIPTION_BEGIN
  !insertmacro MUI_DESCRIPTION_TEXT ${DataProviderFiles} "CUBRID ADO.NET Data Provider library and Help file"
  !insertmacro MUI_DESCRIPTION_TEXT ${DataProviderShortcuts} "Create shortcuts to CUBRID ADO.NET Data Provider Help file"
!insertmacro MUI_FUNCTION_DESCRIPTION_END

;--------------------------------
;Installer Functions

Function .onInit
  Call IsDotNetInstalledAdv
FunctionEnd


Function un.onUninstSuccess

  HideWindow
  MessageBox MB_ICONINFORMATION|MB_OK "The CUBRID ADO.NET Data Provider 11.0.0 was successfully removed from your computer."

FunctionEnd


Function un.onInit
  MessageBox MB_ICONQUESTION|MB_YESNO|MB_DEFBUTTON2 "Are you sure you want to completely remove the CUBRID ADO.NET Data Provider 11.0.0 and all of its components?" IDYES +2
  Abort
FunctionEnd


Function GetDotNETVersion
  Push $0
  Push $1

  System::Call "mscoree::GetCORVersion(w .r0, i ${NSIS_MAX_STRLEN}, *i) i .r1 ?u"
  StrCmp $1 0 +2
    StrCpy $0 "Not found"

  Pop $1
  Exch $0
FunctionEnd

;--------------------------------

; Uninstaller

Section "Uninstall"

  SetOutPath $TEMP

  Delete "$INSTDIR\BSD License.txt"
  Delete "$INSTDIR\Release notes.txt"
  Delete "$INSTDIR\CUBRID.Data.dll"
  Delete "$INSTDIR\CUBRID ADO.NET Data Provider.chm"
  ${If} ${RunningX64}
    Delete "$%SystemRoot%\SysWOW64\cascci.dll"
  ${EndIf}
  Delete "$INSTDIR\cascci.dll"

  RMDir /r "$INSTDIR"

  Delete "$SMPROGRAMS\CUBRID\CUBRID ADO.NET Data Provider 11.0.0\CUBRID ADO.NET Data Provider Help file.lnk"
  Delete "$SMPROGRAMS\CUBRID\CUBRID ADO.NET Data Provider 11.0.0\Uninstall.lnk"

  RMDir /r "$SMPROGRAMS\CUBRID\CUBRID ADO.NET Data Provider 11.0.0"

  ; Remove registry keys
  DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\CUBRID ADO.NET Data Provider 11.0.0"
  DeleteRegKey HKLM "SOFTWARE\CUBRID\CUBRID ADO.NET Data Provider 11.0.0"

  ReadRegStr $R0 HKLM "SYSTEM\CurrentControlSet\Control\Session Manager\Environment" "Path"
  ${WordReplace} $R0 ";$INSTDIR;" "" "+" $R1
  ;MessageBox MB_OK|MB_USERICON '$R0 - $INSTDIR - $R1 '
  WriteRegExpandStr HKLM "SYSTEM\CurrentControlSet\Control\Session Manager\Environment" "Path" "$R1"

SectionEnd
