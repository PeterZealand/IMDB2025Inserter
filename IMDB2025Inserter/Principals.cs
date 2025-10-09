using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace IMDB2025Inserter
{
    public class Principals
    {

        public int TitleId { get; set; }

        public int Ordering { get; set; }

        public int NameId { get; set; }

        public string Category { get; set; }

        public string Job { get; set; }

        public string Characters { get; set; }
 
        public Principals() { }

        public string ToSQL()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append($"INSERT INTO Principals (TitleId, Ordering, NameId, Category, Job, Characters) VALUES (");
            sb.Append($"{TitleId}, ");
            sb.Append($"{Ordering}, ");
            sb.Append($"{NameId}, ");
            sb.Append($"'{Category.Replace("'", "''")}', ");
            sb.Append(!string.IsNullOrEmpty(Job) ? $"'{Job.Replace("'", "''")}', " : "NULL, ");
            sb.Append(!string.IsNullOrEmpty(Characters) ? $"'{Characters.Replace("'", "''")}'" : "NULL");
            sb.Append(");");
            return sb.ToString();
        }
    }
}
