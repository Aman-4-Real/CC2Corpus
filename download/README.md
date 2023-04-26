# Download

This dir contains codes to download `.wet` files from [CommonCrawl](https://data.commoncrawl.org/crawl-data/).

## Usage
- `crawl_urls.py`: the script to crawl urls of `.wets` (or `.warc`) files into a `all_wet_urls.txt` file for further download. The date of the data urls can be specified.
- `random_shuffle.py`: the script shuffles the `all_wets.txt` file and picks customized urls in a range of time.
- `multi_process_dl_by_url.py`: this is a script using multi-processes to download the `.wet.gz` files according to the urls in `all_wets.txt`.

All you need is to run:
```
# Change your config in dl.sh fisrt
bash dl.sh
```



