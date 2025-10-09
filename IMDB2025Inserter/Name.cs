using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDB2025Inserter
{
    public class Name
    {
        public int Id { get; set; }         
        public string PrimaryName { get; set; }
        public int? BirthYear { get; set; }
        public int? DeathYear { get; set; }
        public List<string> PrimaryProfessions { get; set; }
        public List<int> KnownForTitles { get; set; }
        public Name()
        {
            PrimaryProfessions = new List<string>();
            KnownForTitles = new List<int>();
        }

        public string ToSQL()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append($"INSERT INTO Names (Id, PrimaryName, BirthYear, DeathYear) VALUES (");
            sb.Append($"{Id}, ");
            sb.Append($"'{PrimaryName.Replace("'", "''")}', ");
            sb.Append(BirthYear != 0 ? $"{BirthYear}, " : "NULL, ");
            sb.Append(DeathYear != 0 ? $"{DeathYear}" : "NULL");
            sb.Append(");");
            //foreach (string profession in primaryProfessions)
            //{ 
            //    sb.AppendLine();
            //    sb.Append($"INSERT INTO NameProfessions (NameId, Profession) VALUES ({Id}, '{profession.Replace("'", "''")}');");
            //}
            //foreach (int titleId in knownForTitles)
            //{
            //    sb.AppendLine();
            //    sb.Append($"INSERT INTO NameKnownForTitles (NameId, TitleId) VALUES ({Id}, {titleId});");
            //} 
            return sb.ToString();
        }
    }
}
