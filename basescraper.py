import csv
from datetime import datetime
import os
import multiprocessing as mp
import time
from threading import Thread

import pandas as pd
import requests
from requests.exceptions import RequestException

class BaseScraper:
    DEFAULT_PRINT_COUNT = 30
    DEFAULT_ERROR_LIMIT = 5
    DEFAULT_WAIT_TIME_SHORT_SECS = 5

    EXCEPTIONS = (
        RequestException,
    )

    def __init__(self, max_threads=None):
        self.data_folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        self.thread_data_folder_path = os.path.join(self.data_folder_path, 'thread_data')
        self.create_folders([self.data_folder_path, self.thread_data_folder_path])
    
        if max_threads is None:
            self.max_threads = mp.cpu_count()
        else:
            self.max_threads = max_threads

    def create_folders(self, folder_paths):
        for folder_path in folder_paths:
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)
    
    def get_timestamp(self):
        return datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
        
    def save_data(self, data, file_name=None, file_prefix='', thread_data=False, thread_id=None):
        if thread_data and thread_id is None:
            raise ValueError('Cannot save thread data without thread ID.')
        
        if file_name is None:
            if file_prefix != '' and file_prefix[-1] != '_':
                file_prefix += '_'
            
            file_name = f'{file_prefix}_{self.get_timestamp()}_{thread_id:05}.csv'
        
        if thread_data:
            folder_path = self.thread_data_folder_path
        else:
            folder_path = self.data_folder_path
        
        file_path = os.path.join(folder_path, file_name)
        
        data_df = pd.DataFrame(data)
        data_df.to_csv(file_path, index=False)

    def attempt_get(self, url, error_limit=None, wait_time=None):
        if error_limit is None:
            error_limit = self.DEFAULT_ERROR_LIMIT

        if wait_time is None:
            wait_time = self.DEFAULT_WAIT_TIME_SHORT_SECS

        attempt = 1
        while True:
            try:
                response = requests.get(url)
                if response.status_code != 200:
                    raise requests.exceptions.RequestException
                
                break
            except self.EXCEPTIONS:
                print(f'Unable to get {url} on attempt {attempt} of {error_limit}. Sleeping for {wait_time} seconds.')
                attempt += 1
                time.sleep(wait_time)
            
            if attempt > error_limit:
                print(f'Unable to get {url} after {error_limit} attempts.')
                response = None
                break

        return response
    
    def consolidate_files(self, file_name_prefix, thread_data=False):
        consolidated_folder_path = os.path.join(self.data_folder_path, 'consolidated_data')
        self.create_folders([consolidated_folder_path])

        if thread_data:
            input_file_folder_path = self.thread_data_folder_path
        else:
            input_file_folder_path = self.data_folder_path
                
        all_files = os.listdir(input_file_folder_path)

        # filter files
        prefix_length = len(file_name_prefix)
        filtered_files = []
        for file_name in all_files:
            if file_name[:prefix_length].lower() == file_name_prefix.lower():
                file_path = os.path.join(input_file_folder_path, file_name)
                filtered_files.append(file_path)

        file_count = len(filtered_files)        
        print(f'Found {file_count} files with the prefix {file_name_prefix}.')

        # read data from files
        data = []
        for i, file_path in enumerate(filtered_files):            
            with open(file_path, 'r', newline='') as f:
                reader = csv.reader(f)

                for j, row in enumerate(reader):
                    if (j == 0) and (i != 0):
                        # skip headers except for first file
                        print(f'Skipping first row for file {i + 1} ({file_path}).')
                        continue
                    
                    data.append(row)
                
                #print(f'File {i+1} has {j+1} rows.')                
                #print(f'Row count after {i+1} files: {len(data)}.')

        # write data to new file
        file_name_postfix = self.get_timestamp()
        file_name = f'{file_name_prefix}_consolidated_data_{file_name_postfix}.csv'
        file_path = os.path.join(consolidated_folder_path, file_name)

        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)

            for row in data:
                writer.writerow(row)
    
        # remove thread files
        if thread_data:            
            for file_path in filtered_files:
                os.remove(file_path)

    def start_threads(self, worker_function, data_list):
        data_length = len(data_list)
        if data_length == 0:
            print('No data found.')
            return

        thread_count = min(data_length, self.max_threads)

        data_chunk_size = data_length // thread_count

        print(f'Starting {thread_count} threads. Data length: {data_length}, data chunk size: {data_chunk_size}.')

        data_chunks = []
        threads = []
        for i in range(thread_count):
            if i == (thread_count - 1):
                data_chunks.append(data_list[data_chunk_size * i:])
            else:
                data_chunks.append(data_list[data_chunk_size * i: data_chunk_size * (i + 1)])

            threads.append(Thread(target=worker_function, args=(data_chunks[-1], i + 1)))
            threads[-1].start()

        for thread in threads:
            thread.join()