using EDR.Integrator.CommandLine.Entidades;
using System;
using System.Threading.Tasks;
using myoddweb.commandlineparser;
using myoddweb.commandlineparser.Rules;
using Renci.SshNet;

namespace EDR.Integrator.CommandLine
{
    class Program
    {
        static void Main(string[] args)
        {
            string argumentsHelp = Environment.NewLine +
                                   "EDR.Integrator Uso:" + Environment.NewLine + Environment.NewLine +
                                   "-help           Despliega esta pantalla de ayuda" + Environment.NewLine +
                                   "-site           Nombre del sitio EDR remoto a conectarse para extraer valores de las variables operativas" + Environment.NewLine +
                                   "-maxparallel    Máximo grado de paralelismo. Rango entre 1 y 20 (por defecto 10). Recomendado para este equipo: {0}" + Environment.NewLine + 
                                   "-batchsize      Número de filas a procesar por lote (por defecto 2000)" + Environment.NewLine + Environment.NewLine +
                                   "Ejemplo EDR.Integrator.CommandLine -site srv_samaria3 [-maxparallel 12] [-batchsize 4000]";

            int MAX_PARALLEL_DEGREE = 10;
            int MAX_BATCH_SIZE      = 2000;
            string SITE_NAME        = string.Empty;

            string[] validArguments = {"site", "maxparalel", "batchsize", "help"};

            try
            {
                var arguments = new CommandlineParser(args, new CommandlineArgumentRules
                      {
                        new HelpCommandlineArgumentRule( new [] {"help"} ) ,
                                new OptionalCommandlineArgumentRule("batchsize", "2000"),
                                new OptionalCommandlineArgumentRule("maxparallel", "10"),
                                new OptionalCommandlineArgumentRule("site")
                      }, "-"
                );

                //cálculo del número óptimo de procesos en hilo con base a los CPU disponibles del equipo
                int maxCPU = Convert.ToInt32(Math.Ceiling((Environment.ProcessorCount * 0.75) * 2.0));
                //si el valor por defecto de 10 no es posible ejecutarlo en el equipo se establece con base a la capacidad
                if (MAX_PARALLEL_DEGREE > maxCPU)
                    MAX_PARALLEL_DEGREE = maxCPU;

                argumentsHelp = string.Format(argumentsHelp, maxCPU);

                if(arguments.IsHelp())
                {
                    Console.WriteLine(argumentsHelp);
                    return;
                }

                if (!arguments.IsSet("site"))
                {
                    Console.WriteLine(argumentsHelp);
                    return;
                }
                else
                {
                    string site = arguments.Get<string>("site");
                    if(string.IsNullOrEmpty(site))
                    {
                        Console.WriteLine(argumentsHelp);
                        Console.WriteLine("Valor de -site: " + site);
                        return;
                    }
                    else
                    {
                        SITE_NAME = site;
                    }
                }

                if(arguments.IsSet("maxparallel"))
                {
                    int maxparallel = arguments.Get<int>("maxparallel");
                    if(maxparallel > MAX_PARALLEL_DEGREE)
                    {
                        Console.WriteLine("El valor maxparallel es mayor al que el equipo puede ejecutar, se establece por defecto a " + MAX_PARALLEL_DEGREE.ToString());
                    }
                    if(maxparallel < 1 || maxparallel > 20)
                    {
                        Console.WriteLine("El valor maxparallel no está en el rango de 1 a 20 - Recomendado para este equipo: " + MAX_PARALLEL_DEGREE.ToString());
                        return;
                    }
                }

                if(arguments.IsSet("batchsize"))
                {
                    int maxBatchSize = arguments.Get<int>("batchsize");
                    if(maxBatchSize < 100 || maxBatchSize > 10000)
                    {
                        Console.WriteLine("El valor batchsize está fuera de una rango permitido ( >= 100 y <= 10,000)");
                        return;
                    }
                    else
                    {
                        MAX_BATCH_SIZE = maxBatchSize;
                    }
                }

                DateTime initTime = DateTime.Now;
                DateTime finishTime = DateTime.Now;

                Console.WriteLine("*******ETL Site process INIT - " + SITE_NAME);

                ETLProcess.MaxBatchSize = MAX_BATCH_SIZE;
                ETLProcess.GetConfiguration(SITE_NAME);
                ETLProcess.GetVariablesOnSite();

                Parallel.ForEach (ETLProcess.serverOnSiteVariables,
                    new ParallelOptions { MaxDegreeOfParallelism = MAX_PARALLEL_DEGREE },
                    v =>
                {
                    DateTime? dt = ETLProcess.GetLastSync(v);
                    if (dt != null)
                    {
                        v.LastSync = dt;
                    }
                    ETLProcess.ExtractAndLoad(v);
                });

                finishTime = DateTime.Now;
                long elapsedTime = finishTime.Ticks - initTime.Ticks;
                TimeSpan elapsedSpan = new TimeSpan(elapsedTime);

                Console.WriteLine("*******ETL Site process FINISH: Start: " + initTime.ToString() + " | Finish:" + finishTime.ToString() + " | Elapsed time: " + elapsedSpan.TotalSeconds.ToString());
                Console.ReadKey();

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            //1. Buscar la configuración de conexión del sitename y servername establecido en el parámetro
            //2. Leer las variables operativas activas
            //3. Leer la última sincronización (si no existe (NULL) es primera integración
            //4. Por cada variable operativa: Consultar los registros de la variable operativa
            //5. Integrar el dataset en la BD EDRDS guardando (de cada variable):
            //      - Número de registros obtenidos
            //      - TimeStamp del último registro leído en la tabla de control
            //      - Fecha de inicio y término del proceso

        }
    }
}
