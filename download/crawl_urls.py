'''
Author: Aman
Date: 2023-04-23 16:15:29
Contact: cq335955781@gmail.com
LastEditors: Aman
LastEditTime: 2023-04-23 16:28:21
'''
import os
from tqdm import tqdm
import wget
import requests
from bs4 import BeautifulSoup
import zlib

SAVE_WARC = False # set to True to download all warc files
START_TIME = 2019 # collect warc files from 2019 (including) onwards

if __name__ == '__main__':

    url = "https://commoncrawl.org/the-data/get-started/"
    base_bulket = "https://data.commoncrawl.org/crawl-data/"

    response = requests.get(url, stream=True)
    content = response.content.decode('utf-8')

    soup = BeautifulSoup(content, "html.parser")

    # print("Crawling warc urls...")
    print("Crawling wet urls...")
    all_warcs = []
    for li in tqdm(soup.find_all('li'), ncols=100):
        if 's3://commoncrawl/crawl-data/CC-MAIN-' in li.text:
            date = li.find('a').text # li.text.split('–')[-1].strip()
            data_id = li.text.split('/')[4].strip().split('–')[0].strip()
            url = base_bulket + data_id + '/wet.paths.gz'
            all_warcs.append({'date': date, 'url': url})
    
    if SAVE_WARC:
        save_path = './all_warcs/'
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        print("Downloading warcs...")
        for warc in tqdm(all_warcs):
            file_name = '_'.join(warc['url'].split('/')[-2:])
            wget.download(warc['url'], out=os.path.join(save_path, file_name))
        print("Done!")

    res = []
    for item in tqdm(all_warcs, ncols=100):
        url = item['url']
        year = eval(item['date'].split(' ')[-1])
        if year >= START_TIME:
            response = requests.get(url.strip(), stream=True)
            data = zlib.decompress(response.content, zlib.MAX_WBITS|32)
            for warc in data.decode('utf-8').split('\n'):
                if warc != '':
                    res.append(warc)
    
    
    # wfile = 'all_warc_urls.txt'
    # print(f"Saving warc urls to {wfile}...")
    wfile = 'all_wets.txt'
    print(f"Saving wet urls to {wfile}...")
    with open(wfile, 'w') as f:
        f.write('\n'.join(res))
    print("All done!")

