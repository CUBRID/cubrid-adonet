del /s/q "TestResults"

rd /s/q "TestResults"

"%VS100COMNTOOLS%..\IDE\devenv" CUBRID.Data.sln /clean "Debug|Any Cpu"

"%VS100COMNTOOLS%..\IDE\devenv" CUBRID.Data.sln /build "Debug|Any Cpu"


"%VS100COMNTOOLS%..\IDE\MSTest" /testcontainer:"Unit.TestCases\bin\Debug\Unit.TestCases.dll" /testsettings:Local.testsettings