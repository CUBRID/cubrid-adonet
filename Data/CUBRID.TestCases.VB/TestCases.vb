Imports System
Imports System.Data
Imports System.Data.Common
Imports CUBRID.Data.CUBRIDClient
Imports System.Diagnostics

Module TestCases

    Sub Main()
        'Test_01()
        Test_02()

        Console.WriteLine("Press any key to continue...")
        Console.ReadKey()
    End Sub

    Private Sub Test_01()
        Dim conn As New CUBRIDConnection("server=localhost;database=demodb;port=33000;user=public;password=")

        Try
            conn.Open()
            Dim scalarCommand As New CUBRIDCommand("SELECT COUNT(*) FROM nation", conn)
            Dim count As Integer = CInt(scalarCommand.ExecuteScalar())
            Debug.Assert(count = 215)
        Catch ex As Exception
            Console.WriteLine("Error: " & ex.ToString())
        Finally
            conn.Close()
        End Try
    End Sub


    Private Sub Test_02()
        Dim conn As New CUBRIDConnection("server=localhost;database=demodb;port=33000;user=public;password=")
        Dim cmd As New CUBRIDCommand()
        cmd.CommandText = "SELECT * FROM nation ORDER BY `code`; SELECT * FROM nation ORDER BY `code`"
        cmd.Connection = conn

        Try
            conn.Open()
            Dim reader As DbDataReader = cmd.ExecuteReader()
            Do
                reader.Read()
                Debug.Assert(reader.GetString(0) = "AFG")
            Loop While reader.NextResult()

            reader.Close()
        Finally
            conn.Close()
        End Try
    End Sub

End Module
