import json

from basescraper import BaseScraper

class NBAPlayerStatsScraper(BaseScraper):
        '''
        scraper for ESPN player stats
        example url: https://www.espn.com/nba/stats/player/_/season/2023/seasontype/2
        NOTE: this scraper scrapes the underlying api used by the webpage
        '''
            
        BASE_URL = 'https://stats.nba.com/stats/leagueLeaders?LeagueID=00&PerMode=PerGame&Scope=S&Season={}&SeasonType={}&StatCategory={}'
        SEASON_TYPES = {
            0:'Regular Season',
            1:'Playoffs'
        }

        DEFAULT_STAT_CATEGORIES = ['PTS']
        
        FILE_PREFIX = 'player_stats_data_nba'

        DEFAULT_PRINT_COUNT = 5
            
        def __init__(self, start_year, end_year, stat_categories=None, max_threads=None):
            super().__init__(max_threads=max_threads)
            self.start_year = start_year
            self.end_year = end_year
            self.years = [f'{year}-{str(year + 1)[-2:]}' for year in range(start_year, end_year + 1)]
            
            if stat_categories is None:
                self.stat_categories = self.DEFAULT_STAT_CATEGORIES
            else:
                self.stat_categories = stat_categories

        def scrape_multi_thread_worker(self, years, thread_id):
            data_list = []
            for i, year in enumerate(years):
                if i & self.DEFAULT_PRINT_COUNT == 0:
                    print(f'Thread {thread_id:05} getting year {i + 1} of {len(years)} ({year}).')
                for k, v in self.SEASON_TYPES.items():
                    for stat_category in self.stat_categories:
                        url = self.BASE_URL.format(year, v, stat_category)
                        response = self.attempt_get(url)
                        if response is None:
                            continue
                        
                        response_json = json.loads(response.text)
                        for row in response_json['resultSet']['rowSet']:
                            data_dict = {
                                'id': row[0],
                                'name': row[2],
                                'full_year': year,
                                'end_year': int(year[:4]) + 1,
                                'season_type': v,
                                'games_played': row[5],
                                'points_per_game': row[23],
                                'total_points': float(row[5]) * float(row[23]),
                            }
                            data_list.append(data_dict)

            self.save_data(data=data_list, file_prefix=self.FILE_PREFIX, thread_data=True, thread_id=thread_id)
        
        def scrape(self):
            self.start_threads(self.scrape_multi_thread_worker, self.years)
            self.consolidate_files(self.FILE_PREFIX, thread_data=True)


if __name__ == '__main__':
    start_year = 1951    
    end_year = 2022
    
    scraper = NBAPlayerStatsScraper(start_year, end_year)
    scraper.scrape()
