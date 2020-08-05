using EDR.Integrator.CommandLine.Helper;
using System;
using System.Collections.Generic;
using System.Text;

namespace EDR.Integrator.CommandLine.Entidades
{
    public class OperationalVariable
    {

        public int IdVariable           { get; set; }
        public string ShortName         { get; set; }
        public string LongName          { get; set; }
        public string MnemonictShort    { get; set; }
        public string MnemonicLong      { get; set; }
        public string StorageName       { get; set; }
        public DateTime? LastSync       { get; set; }
        public UInt64 DeltaID            { get; set; }

        public string StringLastSync {
            get => ETLHelper.ConvertDateToYYYMMDD(LastSync);
        }
    }
}
