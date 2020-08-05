using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Text;

namespace EDR.Integrator.CommandLine
{
    public class Log
    {

        public Log()
        {
        }

        //crea un archivo de log con errores durante el proceso de integración
        public static void Write(UInt64 id_delta, UInt64 id_event, string LogText, MySqlConnection currentEDRCentralConn)
        {
            string INSERT_LOG = "INSER INTO ETLLog(id_delta, id_event, start_date, log)" +
                                " VALUES({0}, {1}, NOW(), '{2}')";

            MySqlConnection connection = currentEDRCentralConn;

                using (MySqlCommand cmd = new MySqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandText = string.Format(INSERT_LOG, id_delta, id_event, LogText);
                    cmd.ExecuteNonQuery();
                }
        }

        public static string ReadLog(string logDay)
        {
            //1. Validar que exista el log del dia especificado en el parámetro
            //2. Abrir y archivo y leer el contenido
            //3. regresarlo en texto
            return "";
        }

    }
}
