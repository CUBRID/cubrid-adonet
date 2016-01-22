set FILE_NAME=CUBRID ADO.NET Data Provider 9.1.0

cd ..\Data\Source
"%VS100COMNTOOLS%..\IDE\devenv" CUBRID.Data.sln /clean "Release|Any Cpu"
"%VS100COMNTOOLS%..\IDE\devenv" CUBRID.Data.sln /build "Release|Any Cpu"

cd ..\..\Installer
rd /s/q Build
md Build

nsis\makensis.exe CUBRID.nsi

copy "%FILE_NAME%.exe" Build\
del /q "%FILE_NAME%.exe"

rd /s/q "%FILE_NAME%"
md "%FILE_NAME%"
copy "..\Data\Source\Documents\BSD License.txt" "%FILE_NAME%"\
copy "..\Data\Source\Documents\Release Notes.txt" "%FILE_NAME%"\
copy "..\Data\Source\bin\Release\CUBRID.Data.dll" "%FILE_NAME%"\
copy "..\Data\Source\bin\Release\CUBRID.Data.dll.config" "%FILE_NAME%"\
copy "..\Sandcastle\CUBRID ADO.NET Data Provider.chm" "%FILE_NAME%"\
7za a "%FILE_NAME%.zip" -tzip "%FILE_NAME%"
rd /s/q "%FILE_NAME%"

move "%FILE_NAME%.zip" Build\
