using MySql.Data.MySqlClient;
using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.Text;
using EDR.Integrator.CommandLine.Entidades;
using System.Data;
using MySqlX.XDevAPI.Relational;
using EDR.Integrator.CommandLine.Helper;

namespace EDR.Integrator.CommandLine
{
    public class ETLProcess
    {

        public static SiteConfiguration SiteConf = new SiteConfiguration();
        //teclogging
        public static string centralEDRconnStr   = "server=localhost;user=teclogging;database=edrCentral;port=3306;password=T3clog2020;Convert Zero Datetime=True";
        //public static string centralEDRconnStr = "server=localhost;user=root;database=edrds;port=3306;password=root2019;Convert Zero Datetime=True";
        public static string onSiteEDRconnStr    = "server={0};user={1};database={2};port={3};password={4};Convert Zero Datetime=True";
        public static List<OperationalVariable> serverOnSiteVariables = new List<OperationalVariable>();

        private static string DefaultCentralSchema = "edrCentral";
        public static int MaxBatchSize = 2000;

        private static string currentVariable = string.Empty;
        private static int rowExtractCount    = 0;
        private static int rowInsertedCount   = 0;

        public ETLProcess()
        {
        }

        //obtiene la configuración de conexión al sitio operativo y su última
        public static void GetConfiguration(string siteName)
        {
            DateTime initTime = DateTime.Now;
            DateTime finishTime = DateTime.Now;

            MySqlConnection conn = new MySqlConnection(centralEDRconnStr);
            try
            {
                Console.WriteLine("GetConfiguration - Connecting to " + siteName + "...");
                conn.Open();
                conn.ChangeDatabase(DefaultCentralSchema);

                //string sql = "select * from edronsite.var002_innodb where ts <= '2019-10-15 21:11:14'";
                string SQL_SITE_CONF = "SELECT id_site" +
                            ", site_well" +
                            ", site_conn_server" +
                            ", site_conn_port" +
                            ", site_conn_user" +
                            ", site_conn_pwd" +
                            ", site_conn_schema" +
                            " FROM site WHERE site_name = '{0}'";
                SQL_SITE_CONF = string.Format(SQL_SITE_CONF, siteName);

                MySqlCommand cmd = new MySqlCommand(SQL_SITE_CONF, conn);
                MySqlDataReader rdr = cmd.ExecuteReader();

                int rowCount = 0;

                if (rdr.Read())
                {
                    // Console.WriteLine(rdr[0] + " -- " + rdr[1]);
                    SiteConf.IdSite     = rdr.GetInt32("id_site");
                    SiteConf.SiteWell   = rdr.GetString("site_well");
                    SiteConf.SiteServer = rdr.GetString("site_conn_server");
                    SiteConf.SitePort   = rdr.GetString("site_conn_port");
                    SiteConf.SiteUser   = rdr.GetString("site_conn_user");
                    SiteConf.SitePass   = rdr.GetString("site_conn_pwd");
                    SiteConf.SiteSchema = rdr.GetString("site_conn_schema");
                    rowCount++;
                }

                onSiteEDRconnStr = string.Format(onSiteEDRconnStr
                    , SiteConf.SiteServer
                    , SiteConf.SiteUser
                    , SiteConf.SiteSchema
                    , SiteConf.SitePort
                    , SiteConf.SitePass);

                rdr.Close();

                finishTime = DateTime.Now;
                long elapsedTime = finishTime.Ticks - initTime.Ticks;
                TimeSpan elapsedSpan = new TimeSpan(elapsedTime);

                Console.WriteLine("GetConfiguration - Start: " + initTime.ToString() + " - Finish:" + finishTime.ToString() + " - Elapsed time: " + elapsedSpan.TotalSeconds.ToString());

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                conn.Close();
            }
        }

        //obtiene las variables del sitio operativo
        public static void GetVariablesOnSite ()
        {
            DateTime initTime = DateTime.Now;
            DateTime finishTime = DateTime.Now;

            MySqlConnection conn = new MySqlConnection(onSiteEDRconnStr);
            try
            {
                //Console.WriteLine("GetVariablesOnSite...");
                conn.Open();

                //string sqlLastSync = "SELECT id_variable, wellconf_storage_name " + 
                    //" FROM wellconf WHERE wellconf_estatus = 1";

                string SQL_VARIABLES_ONSITE = "SELECT table_name " +
                                        " FROM INFORMATION_SCHEMA.TABLES " +
                                        " WHERE TABLE_SCHEMA = '{0}' " + 
                                        " AND UPPER(TABLE_NAME) LIKE UPPER('VARIABLE_DATA%') LIMIT 1";

                SQL_VARIABLES_ONSITE = string.Format(SQL_VARIABLES_ONSITE, SiteConf.SiteSchema);

                MySqlCommand cmd = new MySqlCommand(SQL_VARIABLES_ONSITE, conn);
                MySqlDataReader rdr = cmd.ExecuteReader();

                while (rdr.Read())
                {
                    serverOnSiteVariables.Add(new OperationalVariable
                    {
                        //IdVariable = rdr.GetInt32("id_variable"),
                        StorageName = rdr.GetString("table_name")
                    });
                    //Console.WriteLine(rdr.GetString("table_name"));
                }

                rdr.Close();

                finishTime = DateTime.Now;
                long elapsedTime = finishTime.Ticks - initTime.Ticks;
                TimeSpan elapsedSpan = new TimeSpan(elapsedTime);

                Console.WriteLine("GetVariableOnSite - Start: " + initTime.ToString() + " - Finish:" + finishTime.ToString() + " - Elapsed time: " + elapsedSpan.TotalSeconds.ToString());

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                conn.Close();
            }


        }

        //obtiene la configuración de conexión al sitio operativo y su última
        public static DateTime? GetLastSync(OperationalVariable variable)
        {
            DateTime initTime = DateTime.Now;
            DateTime finishTime = DateTime.Now;
            DateTime? result;

            MySqlConnection conn = new MySqlConnection(centralEDRconnStr);
            try
            {
                //Console.WriteLine("GetLastSync - " + variable.StorageName);
                conn.Open();
                conn.ChangeDatabase(DefaultCentralSchema);

                string SQL_LAST_SYNC = "SELECT MAX(row_last_timestamp) FROM delta WHERE storage_name = '{0}'";
                SQL_LAST_SYNC = string.Format(SQL_LAST_SYNC, variable.StorageName);

                MySqlCommand cmd = new MySqlCommand(SQL_LAST_SYNC, conn);
                Object rdr = cmd.ExecuteScalar();

                //no hay datos en la tabla delta, es carga completa (primera vez)
                if (rdr.GetType().Name.Equals("DBNull"))
                {
                    
                    variable.LastSync = DateTime.MinValue;
                    result = DateTime.Now.AddYears(-1); //simulamos una fecha anterior a un año

                }
                //ya hay registro de cargas anteriores, es una carga incremental
                else {
                    //SELECCIONAR REGISTROS DESDE LA ULTIMA SINCRONIZACION A LA FECHA
                    variable.LastSync = (DateTime)rdr;
                    result = (DateTime)rdr;

                }
                rdr = null;

                //insertamos en el delta el control de este proceso de integración
                string INSERT_DELTA = "INSERT INTO delta(id_site, storage_name, start_date) " +
                        "VALUES( " +
                        "  {0} " +      //se obtiene de la configuración del Site
                        ", '{1}' " +    //Se obtiene de la variable actual
                        ", NOW() " +    //Default del servidor
                        ")";
                cmd.CommandText = string.Format(INSERT_DELTA, SiteConf.IdSite, variable.StorageName);
                cmd.ExecuteNonQuery();

                string DELTA_ID = "SELECT LAST_INSERT_ID()";
                cmd.CommandText = DELTA_ID;
                Object lastID = cmd.ExecuteScalar();
                if (!lastID.GetType().Name.Equals("DBNull"))
                {
                    variable.DeltaID = (UInt64)lastID;
                }

                finishTime = DateTime.Now;
                long elapsedTime = finishTime.Ticks - initTime.Ticks;
                TimeSpan elapsedSpan = new TimeSpan(elapsedTime);

                Console.WriteLine("GetLastSync - " + variable.StorageName + " - Start: " + initTime.ToString() + " - Finish:" + finishTime.ToString() + " - Elapsed time: " + elapsedSpan.TotalSeconds.ToString());

                return result;

            }
            catch (Exception ex)
            {                
                Console.WriteLine(ex.ToString());
                return null;
            }
            finally
            {
                conn.Close();                
            }

        }

        //obtiene las variables del sitio operativo
        public static void ExtractAndLoad(OperationalVariable variable)
        {
            DateTime initTime = DateTime.Now;
            DateTime finishTime = DateTime.Now;

            //conexion al central para saber el estado del storage y si es necesario crearlo
            MySqlConnection connCentralEDR = new MySqlConnection(centralEDRconnStr);
            try
            {
                Console.WriteLine("Extracting... " + variable.StorageName);
                connCentralEDR.Open();
                connCentralEDR.ChangeDatabase(DefaultCentralSchema);

                string EXISTE_STORAGE = "SELECT COUNT(*) AS count " +
                                        "FROM information_schema.tables " +
                                        "WHERE table_schema = '{0}' " +
                                        "AND table_name = '{1}'";

                EXISTE_STORAGE = string.Format(EXISTE_STORAGE, DefaultCentralSchema, variable.StorageName);

                string CREATE_STORAGE = "CREATE TABLE {0}( " +
                              "id_site int(11) DEFAULT NULL, " +
                              "id_variable int(11) DEFAULT NULL," +
                              "value smallint(4) DEFAULT NULL," +
                              "ts timestamp(6) ," +
                              "mts smallint(6) DEFAULT NULL" +
                            ")" +
                            "ENGINE = INNODB," +
                            "CHARACTER SET utf8," +
                            "COLLATE utf8_spanish_ci," +
                            "COMMENT = 'Tabla para la variable {0}'" +
                            "PARTITION BY KEY(id_site)" +
                            "(" +
                            "PARTITION partitionby_site ENGINE = INNODB" +
                            ");" +
                            "ALTER TABLE {0} " +
                            "ADD INDEX UK_{0}(id_site);";

                CREATE_STORAGE = string.Format(CREATE_STORAGE, variable.StorageName);

                //configuramos la sentencia de Extraccion de datos del Site Remoto
                string EXTRACT_QUERY = "SELECT DISTINCT value, ts, mts FROM {0}.{1} WHERE ts > '{2}' ORDER BY ts ASC LIMIT 20000";
                EXTRACT_QUERY = string.Format(EXTRACT_QUERY
                    , SiteConf.SiteSchema
                    , variable.StorageName
                    , variable.StringLastSync);

                MySqlCommand cmd = new MySqlCommand(EXISTE_STORAGE, connCentralEDR);
                MySqlDataReader rdr = cmd.ExecuteReader();

                bool existeStorage = false;
                if (rdr.Read())
                {
                    existeStorage = rdr.GetInt16("count")==1;
                }
                Console.WriteLine("Existe Storage? " + existeStorage.ToString());

                rdr.Close();

                if(!existeStorage)
                {
                    cmd.CommandText = string.Format(CREATE_STORAGE, variable.StorageName);
                    Console.WriteLine("CREATE_STORAGE... " + CREATE_STORAGE);
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("CREATE_STORAGE... Ok");
                }

                //ahora nos conectamos al sitio remoto para extraer los datos
                MySqlConnection connOnSite = new MySqlConnection(onSiteEDRconnStr);
                DataTable tblOrigen = new DataTable();
                DateTime? LastTimeStamp = variable.LastSync;
                string remark = string.Empty;

                try
                {
                    connOnSite.Open();

                    tblOrigen = GetDataTableLayout(variable.StorageName, connOnSite);
                    tblOrigen.Columns.Add("id_site");
                    //tblOrigen.NewRow();

                    Console.WriteLine("EXTRACT_QUERY...");
                    MySqlCommand cmdExtract = new MySqlCommand(EXTRACT_QUERY, connOnSite);
                    MySqlDataReader etlReader = cmdExtract.ExecuteReader();
                    Console.WriteLine("EXTRACT_QUERY...Ok");
                    while (etlReader.Read())
                    {
                        var r = tblOrigen.NewRow();
                        r["id_site"]     = SiteConf.IdSite;
                        r["id_variable"] = 0;
                        r["value"]       = etlReader.GetFloat("value");
                        r["ts"]          = etlReader.GetDateTime("ts");
                        r["mts"]         = etlReader.GetFloat("mts");
                        tblOrigen.Rows.Add(r);                        
                        //Console.WriteLine(r["value"].ToString() + " | " + r["ts"].ToString());
                        r = null;
                        rowExtractCount++;
                        LastTimeStamp = etlReader.GetDateTime("ts");  //guardamos el valor del time stamp para obtener el ultimo para la siguiente actualización incremental
                    }
                    if (rowExtractCount == 0)
                        remark = "-No hay datos desde la última sincronización";
                }
                catch (Exception innerEx)  
                {
                    Console.WriteLine(innerEx.ToString());
                }
                finally
                {
                    if(connOnSite.State == ConnectionState.Open)
                        connOnSite.Close();
                }

                Console.WriteLine("Loading... " + variable.StorageName);
                currentVariable = variable.StorageName;
                rowInsertedCount = BulkInsertMySQL(tblOrigen, variable.StorageName, connCentralEDR);

                remark = "Ok" + remark;

                //update delta
                string UPDATE_DELTA = "UPDATE delta " +
                            "SET row_count_source =  {0} " +
                            ", row_count_destiny  = '{1}' " +
                            ", row_last_timestamp = '{2}' " +
                            ", finish_date        = NOW() " +
                            ", remark             = '{3}' " +
                            "WHERE id_delta = {4}";

                UPDATE_DELTA = string.Format(UPDATE_DELTA
                    , rowExtractCount       //número de filas recuperadas 
                    , rowInsertedCount      //nùmero de filas insertadas
                    , ETLHelper.ConvertDateToYYYMMDD(LastTimeStamp)
                    , remark
                    , variable.DeltaID
                    ); 

                MySqlCommand cmdUpdateDelta = new MySqlCommand(UPDATE_DELTA, connCentralEDR);
                cmdUpdateDelta.ExecuteNonQuery();

                connCentralEDR.Close();

                finishTime = DateTime.Now;
                long elapsedTime = finishTime.Ticks - initTime.Ticks;
                TimeSpan elapsedSpan = new TimeSpan(elapsedTime);

                Console.WriteLine("ETL "+ variable.StorageName + " - Start: " + initTime.ToString() + " Finish:" + finishTime.ToString() + " Elapsed time: " + elapsedSpan.TotalSeconds.ToString());
                //Console.WriteLine("Row count: " + rowCount.ToString());

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                if(connCentralEDR.State == ConnectionState.Open)
                    connCentralEDR.Close();
            }


        }

        //obtener el layout de la tabla destino
        private static DataTable GetDataTableLayout(string tableName, MySqlConnection currentConn)
        {
            DataTable table = new DataTable();

            MySqlConnection connection = currentConn;

            // Select * is not a good thing, but in this cases is is very usefull to make the code dynamic/reusable 
            // We get the tabel layout for our DataTable
            string query = "SELECT id_variable, value, ts, mts FROM {0}." + tableName + " LIMIT 0";
            query = string.Format(query, SiteConf.SiteSchema);

            using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
            {
                adapter.Fill(table);
            };

            return table;
        }

        private static int BulkInsertMySQL(DataTable table, string tableName, MySqlConnection currentCentralEDRConn)
        {
            MySqlConnection connection = currentCentralEDRConn;
            connection.ChangeDatabase(DefaultCentralSchema);
            int insertedRows = 0;

            using (MySqlTransaction tran = connection.BeginTransaction(IsolationLevel.Serializable))
            {
                using (MySqlCommand cmd = new MySqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.Transaction = tran;
                    cmd.CommandText = "SELECT id_site, id_variable, value, ts, mts FROM " + tableName + " LIMIT 0";

                    using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                    {
                        adapter.UpdateBatchSize = MaxBatchSize;
                        using (MySqlCommandBuilder cb = new MySqlCommandBuilder(adapter))
                        {
                            adapter.RowUpdated += Adapter_RowUpdated;
                            cb.SetAllValues = true;
                            insertedRows = adapter.Update(table);                                
                            tran.Commit();                            
                        }
                    };
                }
            }

            return insertedRows;

        }

        private static void Adapter_RowUpdated(object sender, MySqlRowUpdatedEventArgs e)
        {
            rowInsertedCount += e.RecordsAffected;
            Console.WriteLine(currentVariable + " - " +rowInsertedCount.ToString() + " filas insertadas");
        }
    }


}
