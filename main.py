from game_scraper import MatchScraper
from player_scraper import PlayerScraper
from team_scraper  import TeamScraper

def main() -> None:
    # 1897 for overall history but team history starts from 1964
    start_year: int = 1897
    end_year: int = 2024
    folder_path = "data/"

    match_scraper: MatchScraper = MatchScraper(start_year, end_year)
    player_scraper: PlayerScraper = PlayerScraper()

    match_scraper.scrape_all_matches(
        match_folder_path=folder_path + "matches", lineup_folder_path=folder_path + "lineups"
    )
    player_scraper.scrape_all_players(
        folder_path=folder_path + "players"
    )

    team_scraper: TeamScraper = TeamScraper()
    team_scraper.scrape_team_stats(start_year, end_year, folder_path + "teams")

if __name__ == "__main__":
    main()