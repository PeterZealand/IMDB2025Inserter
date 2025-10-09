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
        public string primaryName { get; set; }
        public int birthYear { get; set; }
        public int deathYear { get; set; }
        public List<string> primaryProfessions { get; set; }
        public List<int> knownForTitles { get; set; }
        public Name()
        {
            primaryProfessions = new List<string>();
            knownForTitles = new List<int>();
        }

        public string ToSQL()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append($"INSERT INTO Names (Id, PrimaryName, BirthYear, DeathYear) VALUES (");
            sb.Append($"{Id}, ");
            sb.Append($"'{primaryName.Replace("'", "''")}', ");
            sb.Append(birthYear != 0 ? $"{birthYear}, " : "NULL, ");
            sb.Append(deathYear != 0 ? $"{deathYear}" : "NULL");
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
