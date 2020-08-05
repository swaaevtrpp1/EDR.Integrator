using System;
using System.Collections.Generic;
using System.Text;

namespace EDR.Integrator.CommandLine.Helper
{
    public class ETLHelper
    {
        private static string dateYYYMMDD = "{0}-{1}-{2} {3}:{4}:{5}";
        public static string ConvertDateToYYYMMDD(DateTime? dt)
        {
           return string.Format(dateYYYMMDD
                    , dt.Value.Year.ToString()
                    , dt.Value.Month.ToString()
                    , dt.Value.Day
                    , dt.Value.Hour.ToString()
                    , dt.Value.Minute.ToString()
                    , dt.Value.Second.ToString()
                    );
        }

    }
}
