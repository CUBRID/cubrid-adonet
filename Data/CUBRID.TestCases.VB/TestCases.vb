'*
'* Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
'*
'* Redistribution and use in source and binary forms, with or without modification, 
'* are permitted provided that the following conditions are met: 
'*
'* - Redistributions of source code must retain the above copyright notice, 
'*   this list of conditions and the following disclaimer. 
'*
'* - Redistributions in binary form must reproduce the above copyright notice, 
'*   this list of conditions and the following disclaimer in the documentation 
'*   and/or other materials provided with the distribution. 
'*
'* - Neither the name of the <ORGANIZATION> nor the names of its contributors 
'*   may be used to endorse or promote products derived from this software without 
'*   specific prior written permission. 
'*
'* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
'* ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
'* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
'* IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
'* INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
'* BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
'* OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
'* WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
'* ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
'* OF SUCH DAMAGE. 
'*

Imports System.Data.Common
Imports CUBRID.Data.CUBRIDClient

Module TestCases

    Sub Main()
        Test_01()
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

        Try
            conn.Open()

            Dim cmd As New CUBRIDCommand()
            cmd.CommandText = "SELECT * FROM nation ORDER BY `code`; SELECT * FROM nation ORDER BY `code`"
            cmd.Connection = conn

            Dim reader As DbDataReader = cmd.ExecuteReader()
            reader.Read()
            Debug.Assert(reader.GetString(0) = "AFG")
            reader.Close()
        Finally
            conn.Close()
        End Try
    End Sub

End Module
