set BATCH_HOME=%~dp0
set VSTestTool=%VS2017COMNTOOLS%..\IDE\CommonExtensions\Microsoft\TestWindow\vstest.console.exe

call "%VS2017COMNTOOLS%VsDevCmd.bat"
cd Code\
devenv CUBRID.Data.sln /Project ADOTest /Rebuild "Release|x64" 
cd Test\QATest\ADOTest\bin\Release

if "%1"=="-p" (
  powershell -Command "& '%VSTestTool%' ADOTest.dll /Platform:x64 /Settings:%BATCH_HOME%Code\TestSettings1.testsettings | Tee-Object -FilePath '%BATCH_HOME%..\ado_net_test.result'"
) else (
  call "%VSTestTool%" ADOTest.dll /Platform:x64 /Settings:%BATCH_HOME%Code\TestSettings1.testsettings
)