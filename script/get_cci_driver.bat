@echo off

REM
REM  Copyright 2008 Search Solution Corporation
REM  Copyright 2016 CUBRID Corporation
REM 
REM   Licensed under the Apache License, Version 2.0 (the "License");
REM   you may not use this file except in compliance with the License.
REM   You may obtain a copy of the License at
REM 
REM       http://www.apache.org/licenses/LICENSE-2.0
REM 
REM   Unless required by applicable law or agreed to in writing, software
REM   distributed under the License is distributed on an "AS IS" BASIS,
REM   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM   See the License for the specific language governing permissions and
REM   limitations under the License.
REM 

set SCRIPT_DIR=%~dp0
set CCI_WIN_DIR=%SCRIPT_DIR%\cubrid-cci\win\cas_cci
set CCI_SRC_DIRS=%SCRIPT_DIR%\cubrid-cci\src\cci
set INSTALL_DIRS=%SCRIPT_DIR%\..\Installer\nsis

if EXIST "%SCRIPT_DIR%\cubrid-cci" rmdir /s /q cubrid-cci

git clone git@github.com:CUBRID/cubrid-cci.git

if "%VS140COMNTOOLS%x" == "x" echo "Please add 'VS140COMNTOOLS' in the environment variable\n ex) C:\Program Files (x86)\Microsoft Visual Studio 14.0\Common7\Tools"

cd %CCI_WIN_DIR%

call "%VS140COMNTOOLS%vsvars32.bat"
devenv cas_cci_v140_dll.vcxproj /build "release|x86"
if not "%ERRORLEVEL%" == "0" echo "Please check for 32bit V140 Relase Library." & GOTO END_SCRIPT
devenv cas_cci_v140_dll.vcxproj /build "release|x64"
if not "%ERRORLEVEL%" == "0" echo "Please check for 64bit V140 Relase Library." & GOTO END_SCRIPT

call "%VS140COMNTOOLS%vsvars32.bat"
devenv cas_cci_v140_dll.vcxproj /build "Debug|x86"
if not "%ERRORLEVEL%" == "0" echo "Please check for 32bit V140 Relase Library." & GOTO END_SCRIPT
devenv cas_cci_v140_dll.vcxproj /build "Debug|x64"
if not "%ERRORLEVEL%" == "0" echo "Please check for 64bit V140 Relase Library." & GOTO END_SCRIPT

copy %CCI_WIN_DIR%\Win32\Release\cas_cci.dll  %INSTALL_DIRS%\cascci32.dll
copy %CCI_WIN_DIR%\x64\Release\cas_cci.dll  %INSTALL_DIRS%\cascci64.dll

:END_SCRIPT
cd %SCRIPT_DIR%
