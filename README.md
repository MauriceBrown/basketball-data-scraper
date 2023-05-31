# Basketball Data Scraper

A set of web scrapers written in Python using **Requests** and **BeautifulSoup** to scrape data from nba.com and espn.com

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
