from datetime import datetime, timedelta
import re

from bs4 import BeautifulSoup

from basescraper import BaseScraper

class ESPNGameResultScraper(BaseScraper):
    BASE_URL = 'https://www.espn.com{}'
    BASE_URL_RESULTS = 'https://www.espn.com/nba/scoreboard/_/date/{}'

    URL_DATE_PATTERN = re.compile(r'\d+')
    
    # no games in july, august and september
    MONTHS = [
        10,
        11,
        12,
        1,
        2,
        3,
        4,
        5,
        6,
    ]
    
    def __init__(self, start_date, end_date, max_threads=None):
        super().__init__(max_threads=max_threads)
        self.start_date = start_date
        self.end_date = end_date

    def get_url_from_datetime(self, dt):
        date_string = dt.strftime('%Y%m%d')
        url = self.BASE_URL_RESULTS.format(date_string)
        
        return url
    
    def get_date_string_from_url(self, url):
        return re.findall(self.URL_DATE_PATTERN, url)[0]

    def parse_results(self, soup, url):
        date_string = self.get_date_string_from_url(url)
        data = []
        
        games = soup.find_all('section', attrs={'class':'Scoreboard bg-clr-white flex flex-auto justify-between'})
                
        for game in games:
            teams = game.find_all('div', {'class':'ScoreCell__TeamName ScoreCell__TeamName--shortDisplayName truncate db'})
            
            scores_final = game.find_all('div', attrs={'class':'ScoreCell__Score h4 clr-gray-01 fw-heavy tar ScoreCell_Score--scoreboard pl2'})
            scores_quarter = game.find_all('div', attrs={'class':'ScoreboardScoreCell__Value flex justify-center pl2 basketball'})

            try:            
                box_score_url = game.find_all('a', attrs={'class':'AnchorLink Button Button--sm Button--anchorLink Button--alt mb4 w-100 mr2'})[1].attrs['href']
            except IndexError:
                box_score_url = ''
                print(f'Unable to get box score for {teams[0].text.strip()} Vs. {teams[1].text.strip()} on {date_string}.')
                continue

            try:            
                data_dict = {
                    'date': date_string,
                    'home_team': teams[0].text.strip(),
                    'away_team': teams[1].text.strip(),
                    'home_team_score_final':scores_final[1].text.strip(),
                    'home_team_score_q1': scores_quarter[4].text.strip(),
                    'home_team_score_q2': scores_quarter[5].text.strip(),
                    'home_team_score_q3': scores_quarter[6].text.strip(),
                    'home_team_score_q4': scores_quarter[7].text.strip(),
                    'away_team_score_final': scores_final[0].text.strip(),
                    'away_team_score_q1': scores_quarter[0].text.strip(),
                    'away_team_score_q2': scores_quarter[1].text.strip(),
                    'away_team_score_q3': scores_quarter[2].text.strip(),
                    'away_team_score_q4': scores_quarter[3].text.strip(),
                    'date_results_url': url,
                    'box_score_url': self.BASE_URL.format(box_score_url)
                }
            except IndexError:
                print(f'Unable to write data for a game on {date_string}.')
                continue
            
            data.append(data_dict)

        return data

    def scrape_multi_thread_worker(self, urls, thread_id):
        url_count = len(urls)
        print(f'Thread {thread_id:05} getting {url_count} urls.')

        data = []
        for i, url in enumerate(urls):
            if (i + 1) % self.DEFAULT_PRINT_COUNT == 0:
                print(f'Thread {thread_id:05} getting url {i + 1} of {url_count}.')
            
            response = self.attempt_get(url)
            if response is None:
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            results = self.parse_results(soup, url)

            data.extend(results)

        self.save_data(data, file_prefix='game_results', thread_data=True, thread_id=thread_id)

    def scrape(self):
        urls = []
        total_days = (self.end_date - self.start_date).days + 1
        for i in range(total_days):
            current_date = self.start_date + timedelta(days=i)
            if current_date.month in self.MONTHS:
                urls.append(self.get_url_from_datetime(current_date))
        
        self.start_threads(self.scrape_multi_thread_worker, urls)
        self.consolidate_files(file_name_prefix='game_results', thread_data=True)

    def scrape_single_thread(self):
        data = []
        
        i = 1
        current_date = self.start_date
        total_days = (self.end_date - self.start_date).days
        while current_date < (self.end_date + timedelta(days=1)):
            if current_date.month not in self.MONTHS:
                continue
            
            if i % self.DEFAULT_PRINT_COUNT == 0:
                print(f'Getting day {i} of {total_days}.')
            
            url = self.get_url_from_datetime(current_date)
            
            response = self.attempt_get(url)
            if response is None:
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            results = self.parse_results(soup, url)

            data.extend(results)

            i += 1
            current_date += timedelta(days=1)
        
        self.save_data(data)

if __name__ == '__main__':
    start_date = datetime(2000, 10, 1)
    end_date = datetime(2010, 6, 30)
    
    scraper = ESPNGameResultScraper(start_date, end_date)
    scraper.scrape()
