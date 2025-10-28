namespace IMDB2025Inserter {
    public class Name {
        public int Id { get; set; }         
        public string? PrimaryName { get; set; }
        public int? BirthYear { get; set; }
        public int? DeathYear { get; set; }
        public List<string> PrimaryProfessions { get; set; }
        public List<string> KnownForTitles { get; set; }

        public Name() {
            PrimaryProfessions = new List<string>();
            KnownForTitles = new List<string>();
        }
    }
}
