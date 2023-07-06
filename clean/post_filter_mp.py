'''
Author: Aman
Date: 2023-04-21 10:39:25
Contact: cq335955781@gmail.com
LastEditors: Aman
LastEditTime: 2023-07-06 22:43:37
'''
import argparse
import multiprocessing
from multiprocessing import RLock, freeze_support
import datetime
from tqdm import tqdm
import os, re
from pathlib import Path
import jsonlines


parser = argparse.ArgumentParser()
parser.add_argument('--data_path', type=str, default='../YOUR_DATA_PATH/')
parser.add_argument('--num_workers', type=int, default=64)
parser.add_argument('--output_path', type=str, default='../YOUR_OUT_PATH/')
args = parser.parse_args()
if not os.path.exists(args.output_path):
    os.makedirs(args.output_path)


def load_data(file):
    '''
    Args:
        file: is a jsonline file
    '''
    data_list = []
    with open(file, 'r') as f:
        for line in jsonlines.Reader(f):
            data_list.append(line)
    return data_list

def filter_and_save(data_list, sample):
    res = []
    cnt1, cnt2, cnt3, cnt4, cnt5, cnt6, cnt7 = 0, 0, 0, 0, 0, 0, 0
    # print(f"Total number of data: {len(data_list)}")
    for data in data_list:
        dir_names = sample.split('/')
        if not os.path.exists(os.path.join(args.output_path, dir_names[-3])):
            os.makedirs(os.path.join(args.output_path, dir_names[-3]))
        if os.path.exists(os.path.join(args.output_path, dir_names[-3], dir_names[-1])):
            continue
        # print(f"Processing {dir_names[-3]}/{dir_names[-1]}")
        title = data['title']
        raw_content = data['raw_content']
        length = data['length']
        nlines = data['nlines']
        language = data['language']
        language_score = data['language_score']
        original_nlines = data['original_nlines']
        perplexity = data['perplexity']
        original_length = data['original_length']
        ### 1. filter by perplexity
        if perplexity > 1000 and length < 500:
        # if perplexity / length > 3:
            # print(data)
            cnt1 += 1
            continue
        ### 2. filter by nlines
        if nlines < 3:
            # print(data)
            cnt2 += 1
            continue
        ### 3. filter by nline changes
        if original_nlines / nlines > 5:
            # print(data)
            cnt3 += 1
            continue
        ### 4. filter by language
        if language_score < 0.6:
            cnt4 += 1
            continue
        ### 5. filter by length
        if length < 256:
            cnt5 += 1
            continue
        ### 6. filter by space ratio
        short_content = re.sub(r'\s+', '', raw_content)
        if len(short_content) / length < 0.8:
            cnt6 += 1
            continue
        ### 7. filter if the content is too repetitive
        if len(raw_content) / len(''.join(list(set(raw_content.split())))) > 2.5:
            cnt7 += 1
            continue
        # tmp = {
        #     'title': title,
        #     'raw_content': raw_content,
        #     'language': language,
        #     'language_score': language_score,
        #     'perplexity': perplexity,
        # }
        tmp = {
            'source': 'CommonCrawl',
            'text': raw_content.replace('﻿', ''),
            'language': language,
        }
        res.append(tmp)
    # print(f'perplexity filter: {cnt1}, nlines filter: {cnt2}, nline changes filter: {cnt3}, \
    #       language filter: {cnt4}, length filter: {cnt5}, space ratio filter: {cnt6}, repetitive filter: {cnt7}')
    # exit()
    if len(res) > 0:
        with open(args.output_path + '/'.join([dir_names[-3], dir_names[-1]]), 'w') as f:
            writer = jsonlines.Writer(f)
            writer.write_all(res)
            writer.close()
    return

def process_data(process_name, data):
    failed = []
    success_cnt = 0
    worker_id = int(process_name.split('-')[1])
    pos = int(process_name.split('-')[1]) - 1
    data_iterator = tqdm(data, ncols=100, desc=f"#{worker_id} pid:{str(os.getpid())}", \
                         leave=True) # position=pos, 
    for sample in data_iterator:
        # try:
        data_list = load_data(sample)
        filter_and_save(data_list, sample)
        success_cnt += 1
        # except Exception as e:
        #     failed.append(sample)
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

    def get_wet_files(path, wet_files=[]):
        for entry in os.scandir(path):
            if entry.is_file() and entry.name.endswith('.wet') and '/res/' in entry.path:
                wet_files.append(entry.path)
            elif entry.is_dir() and 'split' not in entry.path and 'logs' not in entry.path and 'hashes' not in entry.path:
                get_wet_files(entry.path, wet_files)
        return wet_files
    all_wet_paths = get_wet_files(os.path.join(args.data_path))

    # all_wet_paths = all_wet_paths[:10000]
    print(f"Total wet files: {len(all_wet_paths)}")

    ### find all unprocessed wet files
    unprocessed_wet_paths = []
    processed_cut_set = set()
    for entry in os.scandir(os.path.join(args.output_path)):
        if entry.is_dir():
            processed_cut_set.add(entry.name)
    for wet_path in tqdm(all_wet_paths, ncols=100, desc="Finding unprocessed wet files"):
        dir_names = wet_path.split('/')
        if dir_names[-3] in processed_cut_set:
            continue
        else:
            if not os.path.exists(os.path.join(args.output_path, dir_names[-3], dir_names[-1])):
                unprocessed_wet_paths.append(wet_path)
    print(f"Total unprocessed wet files: {len(unprocessed_wet_paths)}")
    # import pdb; pdb.set_trace()

    mapper = MyMultiProcess(MyFunction, args.num_workers)
    res = mapper(inputs=unprocessed_wet_paths)
    res = sum(res, [])

    print("*"*30 + f" Total failed: {len(res)} " + "*"*30)
    # write failed to file
    with open(args.output_path + 'failed.txt', 'w') as f:
        f.write('\n'.join(res))

    end_time = datetime.datetime.now()
    print(f"\rEnd time: {end_time} Total time: {end_time - start_time}")


if __name__ == '__main__' :
    main()