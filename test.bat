call "%VS110COMNTOOLS%vsvars32.bat"
cd Code\Test\QATest\ADOTest
devenv ADOTest.csproj /rebuild "Release|Any CPU" 
cd bin\Release
MSTest/testcontainer:ADOTest.dll