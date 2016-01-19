using System;
using System.IO;

namespace CUBRID.Data.CUBRIDClient
{
  internal class CUBRIDTrace
  {
    private static string _traceFile = "";

    public static void SetTraceFile(string strDir, string logFile)
    {
      Directory.CreateDirectory(strDir);
      string _file = Path.Combine(strDir, logFile);
      _traceFile = _file;
    }

    /// <summary>
    ///   write log
    /// </summary>
    /// <param name="strMessage"> </param>
    public static void WriteLine(string strMessage)
    {
      using (StreamWriter file = new StreamWriter(_traceFile, true))
      {
        file.WriteLine(getCurrentTime());
        file.WriteLine(strMessage);
      }
    }

    private static string getCurrentTime()
    {
      return DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss");
    }
  }
}