// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.


using System;
using System.Linq;
using System.IO;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Threading;
using System.Collections.Concurrent;
using System.Data.SqlClient;

namespace DWScripter
{

    class Program
    {
        static string system = "PDW";
        static string serverPort = "";
        static string authentication = "SQL";
        static string encriptSQLConneciton = "false";
        static string trustServerCertificate = "true";
        static string filterSpec = "%";
        static string userName = "";
        static string pwd = "";
        static string serverTarget = "";
        static string strportTarget = "";
        static string TargetDb = "";
        static string userNameTarget = "";
        static string pwdTarget = "";
        static string FiltersFilePath = "";
        static string CommandTimeout = "";
        static string toScriptCsvFile = "";
        static string dropTruncate = "";
        static string outputCsv = "";
        static string ExcludeObjectSuffixList = "^$"; //"_old|_new|_test|_dba";  // used to exclude test or non-user objects;
        static int returnCode = 0;
        static bool runThreads = true;
        static bool ignoreFailure = false;

        static void Main(string[] args)
        {

            string server = "";
            string sourceDb = "";
            string wrkMode = "ALL";
            string outFile = "";
            string mode = "";
            string featureToScript = "";
            string errorFile = "";
            string logFile = "";
            int threadCount = 4;

            DateTime startTime = DateTime.Now;

            Dictionary<String, String> parameters = new Dictionary<string, string>();
            parameters = GetParametersFromArguments(args);

            foreach (string pKey in parameters.Keys)
            {
                switch (pKey)
                {
                    case "-S":
                        server = parameters[pKey];
                        break;
                    case "-D":
                        sourceDb = parameters[pKey];
                        break;
                    case "-E":
                        authentication = "WINDOWS";
                        break;
                    case "-U":
                        userName = parameters[pKey];
                        break;
                    case "-P":
                        pwd = parameters[pKey];
                        break;
                    case "-W":
                        wrkMode = parameters[pKey];
                        break;
                    case "-M":
                        mode = parameters[pKey].ToUpper();
                        break;
                    case "-St":
                        serverTarget = parameters[pKey];
                        break;
                    case "-Dt":
                        TargetDb = parameters[pKey];
                        break;
                    case "-Ut":
                        userNameTarget = parameters[pKey];
                        break;
                    case "-Pt":
                        pwdTarget = parameters[pKey];
                        break;
                    case "-O":
                        outFile = parameters[pKey];
                        break;
                    case "-F":
                        featureToScript = parameters[pKey].ToUpper();
                        break;
                    case "-Fp":
                        FiltersFilePath = parameters[pKey];
                        break;
                    case "-X":
                        ExcludeObjectSuffixList = parameters[pKey];
                        break;
                    case "-t":
                        CommandTimeout = parameters[pKey];
                        break;
                    case "-if":
                        ignoreFailure = true;
                        break;
                    case "-Cf":
                        toScriptCsvFile = parameters[pKey];
                        break;
                    case "-Sp":
                        serverPort = parameters[pKey];
                        break;
                    case "-Tr":
                        dropTruncate = parameters[pKey];
                        break;
                    case "-Oc":
                        outputCsv = parameters[pKey];
                        break;
                    case "-Tc":
                        threadCount = Convert.ToInt32(parameters[pKey]);
                        break;
                    case "-Ef":
                        errorFile = parameters[pKey];
                        break;
                    case "-Lf":
                        logFile = parameters[pKey];
                        break;
                    default:
                        break;
                }
            }

            if (!String.IsNullOrEmpty(logFile) || !String.IsNullOrEmpty(errorFile))
            {
                Logger.Init(logFile, errorFile);
                Logger.LogError("\"ServerName\",\"DataBase\",\"WorkMode\",\"Mode\",\"FeatureToScript\",\"OutputFile\",\"ErrorMessage\"");
            }

            // No CSV File was passed to read from (an individual object to scan)
            if (string.IsNullOrEmpty(toScriptCsvFile))
            {
                if (wrkMode != "ALL" & wrkMode != "DDL" & wrkMode != "DML")
                {
                    Logger.Log("Uknown mode. USE: DML|DDL|ALL");
                    return;
                }

                if (mode == "COMPARE" & (String.IsNullOrEmpty(serverTarget) || String.IsNullOrEmpty(TargetDb)))
                {
                    Logger.Log("Target Database elements must be completed ...");
                    return;
                }

                CallPDW(null, null, server, sourceDb, wrkMode, mode, featureToScript, outFile);
            }
            // else we need to parse through the CSV file
            else
            {

                // Grab the item count and limit the number of threads to that number (no need for 4 threads for 2 items)
                var lines = File.ReadAllLines(toScriptCsvFile);
                if (lines.Length - 1 < threadCount)
                {
                    threadCount = lines.Length - 1;
                }

                Logger.Log("Item Count: " + (lines.Length - 1) + " | Thread Count: " + threadCount);
                int itemCount = lines.Length - 1;

                // setup necessary objects
                ConcurrentQueue<string> outputQueue = new ConcurrentQueue<string>();
                outputQueue.Enqueue("\"MigrationObjectCode\",\"ServerName\",\"DataBase\",\"SchemaName\",\"WorkMode\",\"OutputFolderPath\",\"FileName\",\"Mode\",\"ObjectName\",\"ObjectToScript\",\"IsSuccess\",\"OptionDropTruncateIfExists\"");
                ConcurrentDictionary<string, bool> inaccessibleDBs = new ConcurrentDictionary<string, bool>();

                // Establish all threads
                ConcurrentQueue<string>[] inputQueues = new ConcurrentQueue<string>[threadCount];
                Thread[] threads = new Thread[threadCount];
                for (int i = 0; i < threadCount; i++)
                {
                    var index = i;
                    inputQueues[index] = new ConcurrentQueue<string>();
                    threads[index] = new Thread(() => PDWThread(inputQueues[index], outputQueue, inaccessibleDBs));
                    threads[index].Name = "Thread" + index;
                    threads[index].Start();
                }

                // Parse through all lines and add them to the thread queues for processing
                int currentThread = 0;
                for (int i = 1; i < lines.Length; i++)
                {
                    string line = lines[i];
                    int thread = currentThread;
                    inputQueues[thread].Enqueue(line);
                    currentThread = (currentThread + 1) % threadCount;
                }

                // While the threads are still running
                bool stillRunning;
                while (runThreads)
                {
                    // Check if the threads still have lines to process
                    stillRunning = false;
                    int count = 0;
                    for (int i = 0; i < threadCount; i++)
                    {
                        if (inputQueues[i].Count > 0)
                        {
                            stillRunning = true;
                            count += inputQueues[i].Count;
                        }
                    }
                    runThreads = stillRunning;

                    Console.Title = "Running | " + (int)((itemCount - count) * 1.0f / itemCount * 100.0f) + "% Complete";
                    Thread.Sleep(1000);
                }

                // The threads are no longer processing, but wait for them to fully close out
                stillRunning = true;
                while (stillRunning)
                {
                    stillRunning = false;
                    for (int i = 0; i < threadCount; i++)
                    {
                        if (threads[i].IsAlive)
                        {
                            stillRunning = true;
                            break;
                        }

                    }

                    Thread.Sleep(50);
                }

                Logger.Log("Threads finished");

                // Log the output stored in the queue to the CSV file
                try
                {
                    Logger.Log("Writing output csv file: " + outputCsv);
                    File.WriteAllLines(outputCsv, outputQueue);
                }
                catch (Exception ex)
                {
                    returnCode = 1;

                    if (!ignoreFailure)
                        throw ex;
                }

                
            }
        
            DateTime endTime = DateTime.Now;

            Logger.Log("Start Time: " + startTime);
            Logger.Log("End Time: " + endTime);

            Logger.Log("Done !!! ");

            while(Logger.StillLogging())
            {
                Thread.Sleep(100);
            }

            Logger.Close();

            #if DEBUG
                Console.ReadKey();
            #endif

            Environment.Exit(returnCode);
        }

        // Method to use for multithreading
        public static void PDWThread(ConcurrentQueue<string> inputQueue, ConcurrentQueue<string> outputQueue, ConcurrentDictionary<string, bool> inaccessibleDBs)
        {
            System.Data.SqlClient.SqlConnection conn = null;
            System.Data.SqlClient.SqlCommand cmd = null;

            string currentServer = "";
            string currentDatabase = "";

            // While we should continue to run the thread
            while(runThreads)
            {
                // While there are items in the thread's input queue
                while(inputQueue.Any())
                {
                    string line = null;
                    if(inputQueue.Any())
                    {
                        inputQueue.TryPeek(out line);
                    }

                    // Skip empty lines
                    if (String.IsNullOrEmpty(line))
                    {
                        continue;
                    }
                        

                    var entries = line.Substring(0, line.Length - 1).Substring(1).Replace("\",\"", ",").Split(',');

                    if (entries[0] == "0")
                    {
                        continue;
                    }

                    // If this is a diferent server or database, we need to reconnect
                    if (currentServer != entries[1] || currentDatabase != entries[2] || conn == null)
                    {
                        
                        string server = entries[1] + "," + serverPort;
                        string sourceDb = entries[2];

                        // Skip if we found out we can't connect to this database
                        if (inaccessibleDBs.ContainsKey(server + sourceDb))
                        {
                            inputQueue.TryDequeue(out line);
                            continue;
                        }
                            
                        // End the old connection
                        if (conn != null)
                        {
                            try
                            {
                                conn.Close();
                                conn = null;
                            }
                            catch (Exception ex)
                            {
                                returnCode = 1;

                                if (!ignoreFailure)
                                {
                                    runThreads = false;
                                    break;
                                }
                            }
                        }

                        // Connect to the database
                        try
                        {
                            PDWscripter.Connect(system, server, sourceDb, authentication, userName, pwd, CommandTimeout, out conn, out cmd);
                            currentServer = entries[1];
                            currentDatabase = entries[2];
                        }
                        // If we aren't able to connect to the database because of an SQL reason (not authorized)
                        catch (SqlException ex)
                        {
                            string errorLine = string.Format("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\"",
                                    server,
                                    sourceDb,
                                    "",
                                    "",
                                    "",
                                    "",
                                    Thread.CurrentThread.Name,
                                    ex.ToString().Replace("\n", " ").Replace("\r", " ").Replace("\"", "'"));

                            Logger.LogError(errorLine);
                            currentDatabase = "";
                            currentServer = "";
                            inaccessibleDBs.TryAdd(server + sourceDb, true);
                            returnCode = 1;
                            if (!ignoreFailure)
                            {
                                runThreads = false;
                                break;
                            }
                            else
                            {
                                inputQueue.TryDequeue(out line);
                                continue;
                            }
                        }
                        // Other error
                        catch (Exception ex)
                        {
                            string errorLine = string.Format("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\"",
                                    server,
                                    sourceDb,
                                    "",
                                    "",
                                    "",
                                    "",
                                    Thread.CurrentThread.Name,
                                    ex.ToString().Replace("\n", " ").Replace("\r", " ").Replace("\"", "'"));


                            Logger.LogError(errorLine);

                            returnCode = 1;
                            if (!ignoreFailure)
                            {
                                runThreads = false;
                                break;
                            }
                            else
                            {
                                inputQueue.TryDequeue(out line);
                                continue;
                            }
                        }

                        
                    }

                    // Gather the data to call PDW
                    string wrkMode = entries[4];
                    string mode = entries[7].ToUpper();

                    Directory.CreateDirectory(entries[5]);
                    string outFile = entries[5] + entries[6];

                    string featureToScript = entries[3] + "." + entries[8];

                    if (featureToScript.Split('.').Length == 1)
                    {
                        featureToScript = "dbo." + featureToScript;
                    }
                    else if (featureToScript.Split('.')[0] == "")
                    {
                        featureToScript = "dbo" + featureToScript;
                    }
                    var objectName = featureToScript;
                    featureToScript = featureToScript.ToUpper();

                    Logger.Log("Processing " + outFile);

                    bool success = true;

                    try
                    {
                        success = CallPDW(conn, cmd, currentServer + "," + serverPort, currentDatabase, wrkMode, mode, featureToScript, outFile);
                    }
                    catch (Exception e)
                    {
                        Logger.Log("Error in " + entries[10]);
                        success = false;
                    }
                    

                    var scriptFileName = outFile.Replace(entries[5], "") + "_" + wrkMode;

                    // Output the line to write to the CSV file
                    string resultLine = string.Format("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\"{9}\",\"{10}\",\"{11}\"",
                        entries[10], //MigrationObjectCode
                        entries[1], //ServerName
                        entries[2], //DatabaseName
                        entries[3], //SchemaName
                        entries[4], //WorkMode
                        entries[5], //Output path
                        scriptFileName, //Filename
                        entries[7], //mode
                        objectName, //ObjectName
                        entries[9], //ObjectToScript
                        success ? 1 : 0, //success
                        dropTruncate //OptionDropTruncateIfExists
                        );
                    outputQueue.Enqueue(resultLine);
                    inputQueue.TryDequeue(out line);
                }
                Thread.Sleep(50);
            }

            try
            {
                conn.Close();
                conn = null;
            }
            catch (Exception ex)
            {
                returnCode = 1;

                if (!ignoreFailure)
                {
                    runThreads = false;
                }
            }

        }

        static bool CallPDW(System.Data.SqlClient.SqlConnection conn, System.Data.SqlClient.SqlCommand cmd, string server, string sourceDb, string wrkMode, string mode, string featureToScript, string outFile)
        {
            returnCode = 0;
            PDWscripter c = null;
            PDWscripter cTarget = null;
            Boolean SourceFromFile = false;
            try
            {
                if (mode == "FULL" || mode == "DELTA" || mode == "COMPARE" || mode == "PERSISTSTRUCTURE")
                {
                    if (mode == "FULL" && featureToScript != "ALL")
                    {
                        filterSpec = featureToScript;
                    }
                    c = new PDWscripter(system, server, sourceDb, authentication, userName, pwd, wrkMode, ExcludeObjectSuffixList, filterSpec, mode, CommandTimeout, conn, cmd);
                    if (mode == "PERSISTSTRUCTURE")
                        // populate dbstruct class
                        c.getDbstructure(outFile, wrkMode, true);
                    if (mode == "COMPARE")
                        c.getDbstructure(outFile, wrkMode, false);
                }
                else
                    c = new PDWscripter();

                // generate full database script
                if (mode == "FULL" || mode == "DELTA")
                {
                    c.getDbTables(false);
                    c.IterateScriptAllTables(c, outFile);
                }
                if (mode == "COMPARE" || mode == "COMPAREFROMFILE")
                {
                    SourceFromFile = false;

                    if (wrkMode == "ALL" || wrkMode == "DDL")
                    {
                        if (mode == "COMPAREFROMFILE")
                        {
                            // retrieve database structure from JSON DDL file
                            SourceFromFile = true;
                            // intialize from Json file
                            outFile = outFile.Replace(TargetDb, sourceDb);
                            string outDBJsonStructureFile = outFile + "_STRUCT_DDL.json";
                            c.GetDDLstructureFromJSONfile(outDBJsonStructureFile);
                        }
                        else
                            c.getDbTables(false);
                    }

                    if (mode == "COMPAREFROMFILE")
                    {
                        if (wrkMode == "ALL" || wrkMode == "DML")
                        {
                            // retrieve database structure from JSON DML file
                            SourceFromFile = true;
                            // intialize from Json file
                            outFile = outFile.Replace(TargetDb, sourceDb);
                            string outDBJsonStructureFile = outFile + "_STRUCT_DML.json";
                            c.GetDMLstructureFromJSONfile(outDBJsonStructureFile);
                        }
                    }


                    FilterSettings Filters = new FilterSettings();
                    if (featureToScript != "ALL")
                    {
                        // retrieve filter settings from file
                        Logger.Log("Retrieving filter settings file : " + FiltersFilePath + "- Feature : " + featureToScript + " - Database : ...");
                        GlobalFilterSettings gFilter = new GlobalFilterSettings();
                        Filters = gFilter.GetObjectsFromFile(FiltersFilePath, featureToScript, sourceDb);

                        if (Filters == null)
                        {
                            throw new System.ArgumentException("Filter settings parameter can not be null - initialization from file : " + FiltersFilePath + "- Feature : " + featureToScript + " - Database : ...");
                        }

                        Logger.Log("Filter settings OK");
                    }

                    cTarget = new PDWscripter(system, serverTarget, TargetDb, authentication, userNameTarget, pwdTarget, wrkMode, "%", filterSpec, mode, CommandTimeout);
                    Logger.Log("Target Connection Opened");
                    cTarget.getDbstructure(outFile, wrkMode, false);
                    if (mode != "COMPAREFROMFILE")
                        cTarget.getDbTables(false);

                    cTarget.CompIterateScriptAllTables(c, cTarget, outFile, SourceFromFile, Filters);
                }
            }
            catch (Exception ex)
            {
                string errorLine = string.Format("\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\"",
                            server,
                            sourceDb,
                            wrkMode,
                            mode,
                            featureToScript,
                            outFile.Replace("\"", "\\\""),
                            Thread.CurrentThread.Name,
                            ex.ToString().Replace("\n", " ").Replace("\r", " ").Replace("\"", "'"));
                Logger.LogError(errorLine);
                
                Logger.Log(ex.ToString());

                returnCode = 1;

                if (!ignoreFailure)
                    throw ex;

                return false;
            }

            if (conn == null)
            {
                if (c != null && c.conn != null)
                {
                    try
                    {
                        c.conn.Close();
                    }
                    catch (Exception ex)
                    {
                        returnCode = 1;

                        if (!ignoreFailure)
                            throw ex;

                        return false;
                    }
                }
            }

            if (cTarget != null && cTarget.conn != null)
            {
                try
                {
                    cTarget.conn.Close();
                }
                catch (Exception ex)
                {
                    returnCode = 1;

                    if (!ignoreFailure)
                        throw ex;

                    return false;
                }
            }

            return true;
        }

       public static void DisplayHelp()
        {
            Logger.Log("DWScripter Command Line Tool");
            Logger.Log("Usage DWScripter ");
            Logger.Log("     [-S: Server source]");
            Logger.Log("     [-D: Database Source]");
            Logger.Log("     [-E: Trusted connection]");
            Logger.Log("     [-U: Login source id]");
            Logger.Log("     [-P: Password source]");
            Logger.Log("     [-W: work mode [DML|DDL|ALL]");
            Logger.Log(@"     [-O: Work folder path \ suffix file ]  no space allowed");
            Logger.Log("     [-M: mode [Full|PersistStructure|Compare|CompareFromFile]]");
            Logger.Log("     [-St: Server target]");
            Logger.Log("     [-dt: Database Target]");
            Logger.Log("     [-Ut: login id target]");
            Logger.Log("     [-Pt: password target]");
            Logger.Log("     [-F: filter on feature for scripting]");
            Logger.Log("     [-Fp: filters file path] no space allowed");
            Logger.Log("     [-X: Exclusion Filter");
            Logger.Log("     [-t: Command Timeout]");
            Logger.Log("     [-if: Ignore Failure]");
            Logger.Log("     [-Sp: Server source port]");
            Logger.Log("     [-Cf: Objects to Script CSV File Location]");
            Logger.Log("     [-Tr: Drop truncate]");
            Logger.Log("     [-Oc: Output CSV File]");
            Logger.Log("     [-Tc: Max thread count]");
            Logger.Log("     [-Ef: Error CSV file]");
            Logger.Log("");
            Logger.Log(@"Sample : DWScripter -S:192.168.1.1,17001 -D:Fabrikam_Dev -E -O:C:\DW_SRC\FabrikamDW_STG -M:PersistStructure");
            Logger.Log(@"Sample : DWScripter -St:192.168.1.1,17001 -Dt:Fabrikam_INT -E -O:C:\DW_SRC\FabrikamDW_STG -M:CompareFromFile -F:ALL");
            Logger.Log(@"Sample : DWScripter -St:192.168.1.1,17001 -Dt:Fabrikam_INT -E -O:C:\DW_SRC\FabrikamDW_STG -M:CompareFromFile -F:DSN_SPRINT2 -Fp:C:\Data\DW_Databases\GlobalDWFilterSettings.json -d:Fabrikam_STG");
            return;
        }
        static Dictionary<string,string>  GetParametersFromArguments (string[] args)
        {
            Dictionary<string, string> parameters = new Dictionary<string, string>();
            string ParametersList = "-S|-D|-E|-M|-O|-St|-Dt|-U|-P|-Ut|-Pt|-W|-F|-Fp|-X|-t|-if|-Sp|-Cf|-Tr|-Oc|-Tc|-Ef|-Lf";
            List<string> ParametersHelp = new List<string> { "-help", "-?", "/?" };
            List<string> ModeList = new List<string> { "FULL", "COMPARE", "COMPAREFROMFILE", "PERSISTSTRUCTURE" };
            Regex Plist = new Regex(ParametersList);
            string ParameterSwitch="";
            string value="";
            int SeparatorPosition;

            for (var x = 0; x < args.Count(); x++)
            {
                SeparatorPosition = args[x].IndexOf(":");
                if (SeparatorPosition != -1)
                {
                    ParameterSwitch = args[x].Substring(0, SeparatorPosition);
                    value = args[x].Substring(SeparatorPosition + 1, args[x].Length - SeparatorPosition - 1);
                }
                else
                {
                    ParameterSwitch = args[x];
                    value = "";
                }

                if (ParametersHelp.Contains(ParameterSwitch))
                {
                    DisplayHelp();
                    Environment.Exit(0);
                }
                
                if (Plist.IsMatch(ParameterSwitch))
                    parameters.Add(ParameterSwitch, value);
            }

            if (parameters.ContainsKey("-M"))
            {
                if (!ModeList.Contains(parameters["-M"].ToUpper()))
                {
                    Logger.Log("Value " + parameters["-M"] + "is not allowed. Only values FULL, COMPARE, COMPAREFROMFILE, PERSISTSTRUCTURE for parameter -M");
                    Environment.Exit(1);
                }
            }
            else
            {
                Logger.Log("Argument -M mode missing");
                Environment.Exit(1);
            }

            // check feature switch existence when work mode different from PERSISTSTRUCTURE or FULL mode
            if (parameters["-M"].ToUpper() != "PERSISTSTRUCTURE" && parameters["-M"].ToUpper() != "FULL")
            {
                if (!parameters.ContainsKey("-F"))
                {
                    Logger.Log("Argument -F is missing, fill it to continue");
                    Environment.Exit(1);
                }
                else
                {
                    if (!parameters.ContainsKey("-D") && parameters["-F"].ToUpper() != "ALL")
                    {
                        Logger.Log("Argument -D is missing [Database Name], fill it to continue");
                        Environment.Exit(1);
                    }

                    if (!parameters.ContainsKey("-Fp") && parameters["-F"].ToUpper() != "ALL")
                    {
                        Logger.Log("Argument -Fp is missing [Filter file], fill it to continue");
                        Environment.Exit(1);
                    }
                }
            }   
            return parameters;
        }

    }
   
}