from nbaplayerstatsscraper import NBAPlayerStatsScraper
from espnplayerstatsscraper import ESPNPlayerStatsScraper
from espngameresultscraper import ESPNGameResultScraper
from espnboxscorescraper import ESPNBoxScoreScraper

if __name__ == '__main__':
    nba_player_stats_scraper = NBAPlayerStatsScraper()
    espn_player_stats_scraper = ESPNPlayerStatsScraper()
    espn_game_results_scraper= ESPNGameResultScraper()
    espn_box_score_scraper = ESPNBoxScoreScraper()
    print('done')
