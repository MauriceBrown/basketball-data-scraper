# Basketball Data Scraper

A set of web scrapers written in Python () using **Requests** and **BeautifulSoup** to scrape data from nba.com and espn.com

This code base consists of four web scrapers:

1. ESPN Game Results Scraper: Scrapes player stats for each game a specified time period from espn.com
2. ESPN Box Score Scraper: Scrapes box score data from espn.com
    - Uses game codes from the output file of ESPN Game Results scraper
3. ESPN Player Stats Scraper: Scrapes player stat totals for each season in a specified window (split by regular season and post season) from espn.com
    - NOTE: ESPN on has regular season data going back to 1995 and post season data going back to 1992
4. NBA Player Stats Scraper: Scrapes player stat totals for each season in a specified window (split by regular season and post season) from nba.com
    - NOTE: nba.com on data data for both the regular season and the post season going back to 1951
    - NOTE: this scraper does not scrape the web page on nba.com, but instead interacts with th the underlying api that the web page consumes

Each of these scrapers can be used individually by editing the `if __name__ == '__main__:` section of the file or they can be accessed through the **main.py** file

In order to speed up the data extraction process, each scraper has a multi-threaded implementation of it's `scrape` function. By default the scraper will use **all** available threads on the host machine. If you would like to use fewer threads, this can be controlled using the `max_threads` argument of each scrapers constructor.

Each scraper outputs a flat csv file which can then be loaded into a database or data analysis tool such as Pandas. Examples of these files are included in this repo in the **data** folder for you to inspect.

## Using the code

1. Open a command window and navigate to the desired folder
2. Download the files using the command `git clone https://github.com/MauriceBrown/basketball-data-scraper.git` (or download the ZIP version of this repo and exctract it in the desired location)
3. Setup a python virtual environment using **venv** or **conda**
    - NOTE: You can skip this step if you want to use your default python installation
4. run the command `pip install -r requirements.txt` to install the required python libraries
5. Edit the details in the `if __name__ == '__main__:` section of the scraper you want to use
6. run the command `python [scraperfile].py` where [scraperfile] is replaced by the name of the scraper you want to use.

When running, the scrapers will output their current status to the command line
