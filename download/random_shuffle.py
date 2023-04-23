'''
Author: Aman
Date: 2023-03-16 20:24:51
Contact: cq335955781@gmail.com
LastEditors: Aman
LastEditTime: 2023-04-23 16:29:37
'''
'''
This script is used to get a random sample of the CommonCrawl data from 2021 to 2023.
'''

import random
from tqdm import tqdm

rfile = 'all_wets.txt'
print(f"Reading wet urls from {rfile}...")
with open(rfile) as f:
    all_wets = f.readlines()

# get data from 2021 to 2023
sample_wets = [w.strip() for w in all_wets if "CC-MAIN-2019" not in w and "CC-MAIN-2020" not in w]
print(f"Total wet files: {len(sample_wets)}")

random.seed(2023)
random.shuffle(sample_wets)

# save a small sample of the data
with open(f"A_NEW_NAME.txt", "w") as f:
    f.write('\n'.join(sample_wets))

print("Done!")



