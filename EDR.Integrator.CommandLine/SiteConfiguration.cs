using EDR.Integrator.CommandLine.Entidades;
using System;
using System.Collections.Generic;
using System.Text;

namespace EDR.Integrator.CommandLine
{
    public class SiteConfiguration
    {
        private string _ConnectionString = "server={0};user={1};database={2};port={3};password={4}";
        public int IdSite           { get; set; }
        public string SiteName      { get; set; }
        public string SiteWell      { get; set; }
        public string SiteServer    { get; set; }
        public string SitePort      { get; set; }
        public string SiteUser      { get; set; }
        public string SitePass      { get; set; }
        public string SiteSchema    { get; set; }
        public string SiteConnectionString { get => string.Format(_ConnectionString, SiteServer, SiteUser, SiteSchema, SitePort, SitePass); }

       public List<OperationalVariable> OperationalVariables { get; set; }
    }
}
