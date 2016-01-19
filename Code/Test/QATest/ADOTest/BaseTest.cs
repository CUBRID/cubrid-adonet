using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using CUBRID.Data.CUBRIDClient;

namespace ADOTest
{
    public class BaseTest
    {
        /// <summary>
        /// It defines testContextInstance.
        /// </summary>
        private TestContext testContextInstance;

        /// <summary>
        /// It defines test steps number.
        /// </summary>
        private int stepNumber = 0;

        /// <summary>
        /// It defines a boolean value indicating inconclusive tag
        /// </summary>
        private static bool inclonclusiveFlag = true;

        private static bool failFlag = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseTest"/> class.
        /// </summary>
        public BaseTest()
        {
            this.stepNumber = 0;
        }

        /// <summary>
        /// Gets or sets the test context which provides
        /// information about and functionality for the current test run.
        /// </summary>
        /// <value>
        /// The test context.
        /// </value>
        public TestContext TestContext
        {
            get
            {
                return this.testContextInstance;
            }

            set
            {
                this.testContextInstance = value;
            }
        }

        /// <summary>
        /// Initializes the log.
        /// </summary>
        /// <param name="message">The message.</param>
        public static void InitializeLog(string message)
        {
            Trace.WriteLine(message, "Initialize");
        }

        /// <summary>
        /// Cleans up log.
        /// </summary>
        /// <param name="message">The message.</param>
        public static void CleanUpLog(string message)
        {
            Trace.WriteLine(message, "CleanUp");
        }

        /// <summary>
        /// test initialize.
        /// </summary>
        [TestInitialize()]
        public virtual void MyTestInitialize()
        {
        }

        /// <summary>
        /// The test cleanup.
        /// </summary>
        [TestCleanup()]
        public void MyTestCleanup()
        {
        }

        /// <summary>
        /// Logs the specified message.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Log(string message)
        {
            Trace.WriteLine(message, "Test Log");
        }

        /// <summary>
        /// Logs the step.
        /// </summary>
        /// <param name="message">The message.</param>
        public void LogTestStep(string message)
        {
            this.stepNumber++;
            string stepTitle = string.Format("Test Step {0}: ", this.stepNumber);
            Trace.WriteLine(string.Empty);
            Trace.WriteLine(stepTitle + message);
        }

        /// <summary>
        /// Logs the step result as passed.
        /// </summary>
        public void LogStepPass()
        {
            Trace.WriteLine(string.Empty);
            Trace.WriteLine(string.Format("Test Step {0} Result: OK", this.stepNumber));
            failFlag = false;
        }

        /// <summary>
        /// Logs the step result as failed.
        /// </summary>
        public void LogStepFail()
        {
            Trace.WriteLine(string.Empty);
            Trace.WriteLine(string.Format("Test Step {0} Result: NOK", this.stepNumber));
            failFlag = true;
        }

        /// <summary>
        /// Logs the step result as nor suported.
        /// </summary>
        public void LogNotSuported()
        {
            Trace.WriteLine(string.Empty);
            Trace.WriteLine(string.Format("Test Step {0} Result: Not suported yet", this.stepNumber));
        }

        /// <summary>
        /// Logs the step result.
        /// </summary>
        /// <param name="message">The message.</param>
        public void LogStepResult(string message)
        {
            string stepResultTitle = string.Format("Test Step {0} Result: ", this.stepNumber);
            Trace.WriteLine(string.Empty);
            Trace.WriteLine(stepResultTitle + message);
        }

        /// <summary>
        /// Logs the test result as passed.
        /// </summary>
        //public void LogTestPass()
        //{
        //    Trace.WriteLine(string.Empty);
        //    Trace.WriteLine("Test case is passed.", "Test Result");
        //}

        /// <summary>
        /// Logs the test result as failed.
        /// </summary>
        //public void LogTestFail()
        //{
        //    Trace.WriteLine(string.Empty);
        //    Assert.Fail("Test case is failed. Please check the detailed information.");
        //}

        /// <summary>
        /// Logs the test result.
        /// </summary>
        public void LogTestResult()
        {
            Trace.WriteLine(string.Empty);
            if (failFlag)
            {
                Trace.WriteLine("Test case is failed. Please check the detailed information.", "Test Result");
                Assert.Fail("Test case is failed. Please check the detailed information.");
            }
            else
            {                
                Trace.WriteLine("Test case is passed.", "Test Result");
            }
        }

        /// <summary>
        /// Assert the konwn failures
        /// </summary>
        /// <param name="bugID">bug ID</param>
        /// <param name="message">bug title</param>
        protected void KnownFailure(int bugID, string message)
        {
            throw new AssertInconclusiveException(string.Format("Bug: {0} -- {1}", bugID, message));
        }
    }
}