namespace IMDB2025Inserter {
    public class Name {
        public int? Id { get; set; }         
        public string? PrimaryName { get; set; }
        public int? BirthYear { get; set; }
        public int? DeathYear { get; set; }
        public List<string>? PrimaryProfessions { get; set; }
        public List<string>? KnownForTitles { get; set; }

        public Name(int? Id, string? PrimaryName, int? BirthYear, int? DeathYear, List<string>? PrimaryProfessions, List<string>? KnownForTitles) {
            this.Id = Id;
            this.PrimaryName = PrimaryName;
            this.BirthYear = BirthYear;
            this.DeathYear = DeathYear;
            this.PrimaryProfessions = PrimaryProfessions;
            this.KnownForTitles = KnownForTitles;
        }
    }
}
