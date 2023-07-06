'''
Author: Aman
Date: 2023-04-06 22:13:01
Contact: cq335955781@gmail.com
LastEditors: Aman
LastEditTime: 2023-07-06 22:38:56
'''
import argparse
import multiprocessing
from multiprocessing import RLock, freeze_support
import datetime
from tqdm import tqdm
import os
from pathlib import Path
import gzip

import process_wet_file
import DocCleaner as DC


parser = argparse.ArgumentParser()
parser.add_argument('--data_path', type=str, default='../YOUR_DATA_PATH/')
parser.add_argument('--num_workers', type=int, default=4)
parser.add_argument('--output_path', type=str, default='../YOUR_OUT_PATH/')
args = parser.parse_args()
if not os.path.exists(args.output_path):
    os.makedirs(args.output_path)
Cleaner = DC.DocCleaner(dtwds_path='./')


def load_and_parse(sample):
    '''Global headers format is:
    WARC/1.0
    WARC-Type: warcinfo
    WARC-Date: 2019-01-24T15:00:50Z
    WARC-Filename: CC-MAIN-20190115225438-20190116011438-00000.warc.wet.gz
    WARC-Record-ID: <urn:uuid:718868ac-51f6-4971-9c11-656107f65e33>
    Content-Type: application/warc-fields
    Content-Length: 372

    Software-Info: ia-web-commons.1.1.9-SNAPSHOT-20190109115430
    Extracted-Date: Thu, 24 Jan 2019 15:00:50 GMT
    robots: checked via crawler-commons 0.11-SNAPSHOT (https://github.com/crawler-commons/crawler-commons)
    isPartOf: CC-MAIN-2019-04
    operator: Common Crawl Admin (info@commoncrawl.org)
    description: Wide crawl of the web for January 2019
    publisher: Common Crawl
    '''
    with open(sample) as f:
        ### get the global headers
        headers, cnt = [], 0 # cnt is used to count the number of WARC/1.0
        for line in f:
            if line.strip() == 'WARC/1.0':
                cnt += 1
            if cnt == 2:
                break
            headers.append(line.strip())
        f.seek(0) # go back to the beginning of the file
        documents = list(process_wet_file.parse_warc_file(f))

    return headers, documents


def load_and_parse_gz(path):
    '''Global headers format is:
    WARC/1.0
    WARC-Type: warcinfo
    WARC-Date: 2019-01-24T15:00:50Z
    WARC-Filename: CC-MAIN-20190115225438-20190116011438-00000.warc.wet.gz
    WARC-Record-ID: <urn:uuid:718868ac-51f6-4971-9c11-656107f65e33>
    Content-Type: application/warc-fields
    Content-Length: 372

    Software-Info: ia-web-commons.1.1.9-SNAPSHOT-20190109115430
    Extracted-Date: Thu, 24 Jan 2019 15:00:50 GMT
    robots: checked via crawler-commons 0.11-SNAPSHOT (https://github.com/crawler-commons/crawler-commons)
    isPartOf: CC-MAIN-2019-04
    operator: Common Crawl Admin (info@commoncrawl.org)
    description: Wide crawl of the web for January 2019
    publisher: Common Crawl
    '''
    with gzip.open(path, 'rt') as f:
        ### get the global headers
        headers, cnt = [], 0 # cnt is used to count the number of WARC/1.0
        for line in f:
            if line.strip() == 'WARC/1.0':
                cnt += 1
            if cnt == 2:
                break
            headers.append(line.strip())
        f.seek(0) # go back to the beginning of the file
        documents = list(process_wet_file.parse_warc_file(f))

    return headers, documents


def process_data(process_name, data):
    failed = []
    success_cnt = 0
    worker_id = int(process_name.split('-')[1])
    pos = int(process_name.split('-')[1]) - 1
    data_iterator = tqdm(data, ncols=100, desc=f"#{worker_id} pid:{str(os.getpid())}", \
                         position=pos, leave=True)
    for sample in data_iterator:
        try:
            if sample.endswith('.gz'):
                headers, documents = load_and_parse_gz(sample)
            else:
                headers, documents = load_and_parse(sample)
            filtered_documents = Cleaner.filter_documents(documents)
            cleaned_documents = Cleaner.clean_documents(filtered_documents)
            sname = sample.split('/')[-1].rstrip('.gz')
            process_wet_file.write_warc_file(headers, cleaned_documents, os.path.join(args.output_path, sname))
            success_cnt += 1
        except Exception as e:
            failed.append(sample)
        data_iterator.set_postfix(Success=str(success_cnt)+'/'+str(len(data)))
    data_iterator.close()
    
    return failed


class MyMultiProcess(object):
    def __init__(self, my_func, num_workers=8):
        self.my_func = my_func
        self.pool = multiprocessing.Pool(num_workers, initializer=tqdm.set_lock, initargs=(RLock(),))
        self.num_workers = num_workers

    def split_list(self, lst, k): # split list into k parts
        n = len(lst) // k
        m = len(lst) % k
        result = []
        start = 0
        for i in range(k):
            if i < m:
                end = start + n + 1
            else:
                end = start + n
            result.append(lst[start:end])
            start = end
        return result

    def __call__(self, inputs):
        if not inputs:
            print("Error! No input files!")
            exit(1)
        unclean_data = []
        for file in inputs:
            filename = file.split('/')[-1].rstrip('.gz')
            if os.path.exists(args.output_path + filename):
                continue
            else:
                unclean_data.append(file)
        inputs = unclean_data
        print("Total unprocessed files:", len(inputs))
        ##################################################
        # if do not split into files, distribute to different process
        input_splits = self.split_list(inputs, self.num_workers)
        param = [(input_splits[i]) for i in range(self.num_workers)]
        ##################################################
        res = self.pool.map(self.my_func, param)

        return res


def MyFunction(params):
    data = params
    # print(multiprocessing.current_process().name, 'data processing...')
    
    failed = process_data(multiprocessing.current_process().name, data)

    # print(multiprocessing.current_process().name, \
		# 'data processing done. Success: %d/%d' % (len(data) - len(failed), len(data)))
    
    return failed


def main():
    start_time = datetime.datetime.now()
    print("Start time:", start_time)
    freeze_support()
    # find all .wet files in the data folder
    print("Finding all wet files...")
    all_wet_paths = []
    for root, dirs, files in os.walk(args.data_path):
        for file in files:
            if file.endswith(".gz") or file.endswith(".wet"):
                all_wet_paths.append(os.path.join(root, file))
    all_wet_paths = all_wet_paths
    print(f"Total wet files: {len(all_wet_paths)}")

    unclean_data = []
    for file in all_wet_paths:
        filename = file.split('/')[-1].rstrip('.gz')
        if os.path.exists(args.output_path + filename):
            continue
        else:
            unclean_data.append(file)
    all_wet_paths = unclean_data
    print("Total unprocessed files:", len(all_wet_paths))

    mapper = MyMultiProcess(MyFunction, args.num_workers)
    res = mapper(inputs=all_wet_paths)
    res = sum(res, [])

    print("*"*30 + f" Total failed: {len(res)} " + "*"*30)
    # write failed to file
    with open(args.output_path + 'failed.txt', 'w') as f:
        f.write('\n'.join(res))

    end_time = datetime.datetime.now()
    print(f"\rEnd time: {end_time} Total time: {end_time - start_time}")


if __name__ == '__main__' :
    main()