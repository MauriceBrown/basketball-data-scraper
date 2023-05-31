import csv
from datetime import datetime
import os
import re

from bs4 import BeautifulSoup

from basescraper import BaseScraper

class ESPNBoxScoreScraper(BaseScraper):
    GAME_ID_PATTERN = re.compile(r'.*/(\d{1,15})$')    
    PLAYER_NAME_PATTERN = re.compile(r'(.*) (\w+)')
    
    def __init__(self, url_file_path, max_threads=None, page_limit=None, page_start=None):
        super().__init__(max_threads=max_threads)
        self.url_file_path = url_file_path        
        self.urls = []
        self.page_limit = page_limit

        if page_start is None:
            self.page_start = 0
        else:
            self.page_start = page_start

    def write_data_to_file(self, data, file_path=None):
        if file_path is None:
            file_name_postfix = self.get_timestamp()
            file_name = f'box_score_data_{file_name_postfix}.csv'
            file_path = os.path.join(self.data_folder_path, file_name)        

        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)

            for i, block in enumerate(data):
                for j, row in enumerate(block):
                    if (j == 0) and (i != 0):
                        # skip headers except on first occurrence
                        continue
                    
                    writer.writerow(row)

    def consolidate_thread_data(self):
        file_names = os.listdir(self.thread_data_folder_path)

        if file_names == []:
            print(f'No thread files found.')
            return
        
        data = []        
        for i, file_name in enumerate(file_names):
            file_path = os.path.join(self.thread_data_folder_path, file_name)
            
            with open(file_path, 'r', newline='') as f:
                reader = csv.reader(f)

                for j, row in enumerate(reader):
                    if (j == 0):
                        if not (i == 0):
                            continue
                    
                    data.append(row)

            # delete temp file
            os.remove(file_path)

        self.write_data_to_file([data])

    def set_urls(self):
        initial_url_count = len(self.urls)

        # initial setting of first url and last url
        first_url = self.page_start

        if self.page_limit is not None:
            last_url = self.page_start + self.page_limit
            page_limit = self.page_limit
        else:
            last_url = initial_url_count
            page_limit = 0

        # check if first_url or last_url are out of bounds        
        if first_url > initial_url_count:
            self.urls = []
            print(f'First url out of bounds.')
            return
        
        if last_url > initial_url_count:
            last_url = initial_url_count

        self.urls = self.urls[first_url: last_url]

        final_url_count = len(self.urls)
        
        print(f'Started with {initial_url_count} urls, ended with {final_url_count} urls. First url: ({self.page_start}, {first_url}), last url: ({self.page_start + page_limit}, {last_url}).')
        
    def read_data_file(self):
        with open(self.url_file_path, 'r', newline='') as f:
            reader = csv.reader(f)

            urls = []
            for i, row in enumerate(reader):
                if i != 0:
                    # skip header row
                    urls.append(row[-1])
            
            self.urls = urls

        self.set_urls()    

    def augment_stats_table_row(self, stats_table_row, headers=False):
        if headers:
            fgm = 'field_goal_made'
            fga = 'field_goal_attemps'
            tpm = 'three_point_made'
            tpa = 'three_point_attemps'
            ftm = 'free_throw_made'
            fta = 'free_throw_attemps'
        else:
            try:
                fgm, fga = stats_table_row[1].split('-')
                tpm, tpa = stats_table_row[2].split('-')
                ftm, fta = stats_table_row[3].split('-')
            except (AttributeError, ValueError):
                fgm = 0
                fga = 0
                tpm = 0
                tpa = 0
                ftm = 0
                fta = 0

        stats_table_row.extend([fgm, fga, tpm, tpa, ftm, fta])
        return stats_table_row

    def extract_data_from_box_score_divs(self, box_score_divs, game_id):
        names = []
        positions = []
        player_page_urls = []
        team_names = []
        game_ids = []
        stats_table_rows = []

        exclude_rows = ['starters', 'bench', 'team', '']

        for i, box_score_div in enumerate(box_score_divs):
            team_name = box_score_div.find('div', attrs={'class': 'BoxscoreItem__TeamName h5'}).text.strip()

            # player names            
            team_player_names_table, stats_table = box_score_div.find_all('table')

            for tr in team_player_names_table.find_all('tr'):
                name_data = tr.text.strip()
                if name_data not in exclude_rows:
                    try:
                        name, position = re.findall(self.PLAYER_NAME_PATTERN, name_data)[0]
                    except IndexError:
                        print('err', name_data)
                        raise IndexError

                    player_page_url = tr.find('a').attrs['href']
                    
                    names.append(name)
                    positions.append(position)
                    player_page_urls.append(player_page_url)
                    team_names.append(team_name)
                    game_ids.append(game_id)

            # stats            
            for j, tr in enumerate(stats_table.find_all('tr')):
                stats_table_row = [cell.text.strip().lower() for cell in tr.find_all('td')]
                first_col = stats_table_row[0]

                if j == 0:
                    if i == 0:
                        column_count = len(stats_table_row)
                    else:
                        continue
                elif first_col == 'min':
                    continue
                elif first_col[:3] == 'dnp':
                    stats_table_row = [0] * column_count
                elif first_col == '':
                    break

                if j == 0:
                    if i == 0:
                        stats_table_row = self.augment_stats_table_row(stats_table_row, headers=True)
                    else:
                        continue
                else:
                    stats_table_row = self.augment_stats_table_row(stats_table_row, headers=False)
                
                stats_table_rows.append(stats_table_row)


        player_info = list(zip(names, positions, player_page_urls, team_names, game_ids))

        headers = stats_table_rows[0]
        headers.extend(['player_name', 'position', 'player_page_url', 'team_name', 'game_id'])
        
        final_data = [headers]        
        for i, row in enumerate(stats_table_rows[1:]):
            row.extend(player_info[i])
            final_data.append(row)

        return final_data

    def extract_reponse_data(self, reponse_content, game_id):
        soup = BeautifulSoup(reponse_content, 'html.parser')

        box_score_divs = soup.find_all('div', attrs={'class':'Boxscore flex flex-column'})
        data = self.extract_data_from_box_score_divs(box_score_divs, game_id)

        return data
    
    def extract_game_id(self, url):
        game_id = re.findall(self.GAME_ID_PATTERN, url)[0]
        return game_id

    def scrape_multi_thread_worker(self, urls, thread_id):
        url_count = len(urls)
        print(f'Thread {thread_id:05} getting {url_count} url(s).')

        data = []
        for i, url in enumerate(urls):
            if (i + 1) % self.DEFAULT_PRINT_COUNT == 0:
                print(f'Thread {thread_id:05} getting url {i + 1} of {url_count}.')

            response = self.attempt_get(url)
            if response is None:
                continue

            game_id = self.extract_game_id(url)
            data.append(self.extract_reponse_data(response.content, game_id))

        file_name = f'thread_{thread_id:05}.csv'
        file_path = os.path.join(self.thread_data_folder_path, file_name)
        
        self.write_data_to_file(data, file_path)
        
        print(f'Thread {thread_id:05} complete.')

    def scrape(self):
        self.read_data_file()
        
        if self.urls != []:
            self.start_threads(self.scrape_multi_thread_worker, self.urls)
            self.consolidate_thread_data()

if __name__ == '__main__':
    data_folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    #url_file_path = os.path.join(data_folder_path, 'consolidated_data', 'game_results_consolidated_data_2023_02_10_06_45_30.csv')
    url_file_path = '[path to game results file]'

    scraper = ESPNBoxScoreScraper(url_file_path, page_limit=None, page_start=None)
    scraper.scrape()
    #scraper.consolidate_files('box_score')