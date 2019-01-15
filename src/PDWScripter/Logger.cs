using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace DWScripter
{
    public class Logger
    {
        static ConcurrentQueue<string> entries;
        static StreamWriter logger;
        static ConcurrentQueue<string> errors;
        static StreamWriter errorLogger;
        static bool logToFile = false;

        public static void Init(string logFile, string errorFile)
        {
            if(!String.IsNullOrEmpty(logFile) || !String.IsNullOrEmpty(errorFile))
            {
                logToFile = true;

                if (!String.IsNullOrEmpty(logFile))
                { 
                    FileStream fs = new FileStream(logFile, FileMode.Create);
                    logger = new StreamWriter(fs);
                    entries = new ConcurrentQueue<string>();
                    
                }

                if(!String.IsNullOrEmpty(errorFile))
                {
                    FileStream fs = new FileStream(errorFile, FileMode.Create);
                    errorLogger = new StreamWriter(fs);
                    errors = new ConcurrentQueue<string>();
                }

                Thread logThread = new Thread(() =>
                {
                    while (logToFile)
                    {
                        if(entries != null && entries.Count > 0)
                        {
                            string line;
                            entries.TryDequeue(out line);
                            logger.WriteLine(line);
                        }

                        if(errors != null && errors.Count > 0)
                        {
                            string line;
                            errors.TryDequeue(out line);
                            errorLogger.WriteLine(line);
                        }

                        Thread.Sleep(5);
                    }
                });

                logThread.Start();
            }

            Log("Initializing logger");
        }

        public static void Log(string line)
        {
            line = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " | " + Thread.CurrentThread.Name + " | " + line;
            Console.WriteLine("  " + line);
            if(logToFile && entries != null && logger != null)
            {
                entries.Enqueue(line);
            }
        }

        public static void Log(Exception e)
        {
            Log(e.ToString());
        }

        public static void LogError(string line)
        {
            if(logToFile && errors != null && errorLogger != null)
            {
                errors.Enqueue(line);
            }
        }
    
        public static bool StillLogging()
        {
            return logToFile && ((entries != null && entries.Count > 0) || (errors != null && errors.Count > 0));
        }

        public static void Close()
        {
            if(logToFile && logger != null)
            {
                logger.Close();
            }

            if(logToFile && errorLogger != null)
            {
                errorLogger.Close();
            }

            logToFile = false;
        }
    }
}
