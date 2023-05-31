import json

from basescraper import BaseScraper

class ESPNPlayerStatsScraper(BaseScraper):
        '''
        scraper for ESPN player stats
        example url: https://www.espn.com/nba/stats/player/_/season/2023/seasontype/2
        NOTE: this scraper scrapes the underlying api used by the webpage
        '''
            
        BASE_URL = 'https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/statistics/byathlete?region=us&lang=en&contentorigin=espn&isqualified=true&page={}&limit={}&sort=offensive.avgPoints%3Adesc&season={}&seasontype={}'        
        SEASON_TYPES = {
            2:'Regular Season',
            3:'Post Season'
        }
        
        FILE_PREFIX = 'player_stats_data_espn'

        DEFAULT_PRINT_COUNT = 2
            
        def __init__(self, start_year, end_year, max_threads=None):
            super().__init__(max_threads=max_threads)
            self.start_year = start_year
            self.end_year = end_year
            self.years = list(range(self.start_year, self.end_year + 1))

        def scrape_multi_thread_worker(self, years, thread_id):
            data_list = []
            for i, year in enumerate(years):
                if i & self.DEFAULT_PRINT_COUNT == 0:
                    print(f'Thread {thread_id:05} getting year {i + 1} of {len(years)} ({year}).')
                for season_type in range(2):
                    url = self.BASE_URL.format(1, 500, year, season_type + 2)
                    response = self.attempt_get(url)
                    if response is None:
                        continue
                    
                    response_json = json.loads(response.text)
                    for athlete in response_json['athletes']:
                        data_dict = {
                            'id': athlete['athlete']['id'],
                            'name': athlete['athlete']['displayName'],                            
                            'year': year,
                            'season_type': self.SEASON_TYPES[season_type + 2],
                            'games_played': athlete['categories'][0]['totals'][0],
                            'points_per_game': athlete['categories'][1]['totals'][0],
                            'total_points': float(athlete['categories'][0]['totals'][0]) * float(athlete['categories'][1]['totals'][0]),
                        }
                        data_list.append(data_dict)

            self.save_data(data=data_list, file_prefix=self.FILE_PREFIX, thread_data=True, thread_id=thread_id)
        
        def scrape(self):
            self.start_threads(self.scrape_multi_thread_worker, self.years)
            self.consolidate_files(self.FILE_PREFIX, thread_data=True)


if __name__ == '__main__':
    start_year = 1992
    end_year = 2023    
    
    scraper = ESPNPlayerStatsScraper(start_year, end_year)
    scraper.scrape()
