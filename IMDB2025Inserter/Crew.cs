using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDB2025Inserter
{
    public class Crew
    {
        public int Id { get; set; }
        public List<string> Directors { get; set; }
        public List<string> Writers { get; set; }
        public Crew()
        {
            Directors = new List<string>();
            Writers = new List<string>();
        }
        public string ToSQL()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append($"INSERT INTO Crews (Id) VALUES (");
            sb.Append($"{Id}");
            sb.Append(");");
            
            return sb.ToString();
        }
    }
}
