namespace IMDB2025Inserter {
    public class Crew {
        public int TitleId { get; set; }
        public List<string> Directors { get; set; }
        public List<string> Writers { get; set; }

        public Crew() {
            Directors = new List<string>();
            Writers = new List<string>();
        }
    }
}
