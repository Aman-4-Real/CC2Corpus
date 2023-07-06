# CC2Corpus

This repo contains a pipeline to download, clean and process CommonCrawl data into a corpus.

## Usage

- `download`: contains scripts and example code to crawl and download commoncrawl data from the official website. 

- `clean`: contains scripts and example code to clean the downloaded data. The whole pipeline includes 3 main stages:
    - pre-filtering: filter the data by dirty words and some rules.
    - cc_net cleaning: use facebook cc_net pipeline to clean the data. More details please refer to [cc_net](https://github.com/facebookresearch/cc_net). You should start this stage after the installation of cc_net.
    - post-filtering: filter the data by the results of cc_net. This is mainly about something like ppl, etc.

Feel free to contact me or raise issues for any questions. 

