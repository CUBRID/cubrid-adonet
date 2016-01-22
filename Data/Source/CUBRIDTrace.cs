using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace CUBRID.Data.CUBRIDClient
{
    class CUBRIDTrace
    {
        private static string _traceFile = "";

        static public void SetTraceFile(string strDir, string logFile)
        {
            System.IO.Directory.CreateDirectory(strDir);
            string _file = System.IO.Path.Combine(strDir, logFile);
            CUBRIDTrace._traceFile = _file;
        }

        /// <summary>
        /// write log
        /// </summary>
        /// <param name="strMessage"></param>
        static public void WriteLine(string strMessage)
        {
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(CUBRIDTrace._traceFile, true))
            {
                file.WriteLine(getCurrentTime());
                file.WriteLine(strMessage);
            }
        }
        
        static private string getCurrentTime()
        {
            return DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss");
        }
    }
}
