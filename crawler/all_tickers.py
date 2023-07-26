#!/usr/bin/env python3
"""
Download the ticker list from NASDAQ and save as csv.
Output filename: ./input/tickerList.csv
"""
import csv
import sys
import urllib3
from urllib.request import urlopen
import requests
import numpy as np
import json

def get_tickers(percent):
    """Keep the top percent market-cap companies."""
    assert isinstance(percent, int)

    file = open('input/tickerList.csv', 'w')
    writer = csv.writer(file, delimiter=',')
    cap_stat, output = np.array([]), []
    http = urllib3.PoolManager()

    for exchange in ["NASDAQ","NYSE", "AMEX"]:
        # Construct the URL with the exchange value already appended
        url = "https://api.nasdaq.com/api/screener/stocks?offset=0&exchange="+exchange+"&download=true"
        print(url)
        repeat_times = 1
        
        for _ in range(repeat_times):
            try:
                print("Downloading tickers from {}...".format(exchange))
                response = requests.get(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
                })
                response.raise_for_status()  # Check if the request was successful

                content = json.loads(response.text)
                for line in content['data']['rows']:
                    
                    ticker = line['symbol']
                    name = line['name']
                    market_cap = line['marketCap']
                    
                    # ticker, name, last_sale, market_cap, IPO_year, sector, industry
                    
                    output.append([ticker, name.replace(',', '').replace('.', ''),
                                   exchange, market_cap])
                    
                break
            except OSError:
                continue

    for data in output:
        try:
            market_cap = float(data[3])
        except:
            continue
        print(data)

        if len(data) < 4:
            continue
        writer.writerow(data)


def main():
    if len(sys.argv) < 2:
        print('Usage: ./all_tickers.py <int_percent>')
        return
    top_n = sys.argv[1]
    get_tickers(int(top_n)) # keep the top N% market-cap companies


if __name__ == "__main__":
    main()

