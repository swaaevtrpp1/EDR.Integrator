using System;
using System.Collections.Generic;
using System.Text;

namespace EDR.Integrator.CommandLine.Entidades
{
    public class LogEvent
    {
        public int DeltaID        { get; set; }
        public int EventID        { get; set; }
        public string EventName   { get; set; }
        public DateTime StartDate  { get; set; }
        public DateTime FinishDate { get; set; }
        public string Log          { get; set; }
    }
}
