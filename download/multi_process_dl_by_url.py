'''
Author: Aman
Date: 2022-06-21 21:37:41
Contact: cq335955781@gmail.com
LastEditors: Aman
LastEditTime: 2023-04-23 17:02:38
'''
import argparse
import multiprocessing
from multiprocessing import RLock, freeze_support
import datetime
from tqdm import tqdm
import wget
import os


parser = argparse.ArgumentParser()
parser.add_argument('--rfile', type=str, default='all_wets.txt')
parser.add_argument('--num_workers', type=int, default=4)
parser.add_argument('--save_path', type=str, default='../../CC/debug/')
parser.add_argument('--failed_file', type=str, default='failed.txt')
parser.add_argument('--start', type=int, default=0)
parser.add_argument('--end', type=int, default=100)
args = parser.parse_args()
if not os.path.exists(args.save_path):
    os.makedirs(args.save_path)


def process_data(process_name, data):
    base_url = "https://data.commoncrawl.org/"
    worker_id = int(process_name.split('-')[1])
    pos = int(process_name.split('-')[1])

    i = -1
    failed = []
    
    data_iterator = tqdm(data, ncols=100, desc=f"#{worker_id} pid:{str(os.getpid())}", \
                         position=pos, leave=True)
    for sample in data_iterator:
        i += 1 # idx
        ERROR_TRIES = 5 # Try * times before giving up
        filename = sample.split('/')[-1]
        if os.path.exists(args.save_path + filename):
            print(f"Already exists: {filename}")
            continue
        else:
            success = False
            while not success and ERROR_TRIES > 0:
                try:
                    wget.download(base_url + sample, out=args.save_path)
                    success = True
                    break
                except Exception as e:
                    # print(f"Error: {e} in {i}th file. Sample: {sample}")
                    ERROR_TRIES -= 1
                    continue
            if not success:
                failed.append(sample)
        data_iterator.set_postfix(failed_num=len(failed))
    
    print(f"{process_name} Failed: {len(failed)}")

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
        undl_data = []
        for file_url in inputs:
            filename = file_url.split('/')[-1]
            if os.path.exists(os.path.join(args.save_path, filename)) or \
                os.path.exists(os.path.join(args.save_path, filename.replace('.gz', ''))):
                # print(f"Already exists: {filename}")
                continue
            else:
                undl_data.append(file_url)
        inputs = undl_data
        print("Total undownloaded files:", len(inputs))
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

    # end = len(data) if start + 2 * size > len(data) else start + size
    
    failed = process_data(multiprocessing.current_process().name, data)

    # print(multiprocessing.current_process().name, \
	# 	'data processing done. Success: %d/%d' % (len(data) - len(failed), len(data)))
    
    return failed


def main():
    start_time = datetime.datetime.now()
    print("Start time:", start_time)
    freeze_support()
    workers_num = args.num_workers  # num of multiprocers

    rfile = args.rfile
    print(f"Reading wet urls from {rfile}...")
    with open(rfile) as f:
        all_wets = f.readlines()
    all_wets = [w.strip() for w in all_wets]
    print(f"Total wet files: {len(all_wets)}")
    all_wets = all_wets[args.start:args.end]
    # assert args.start % 10000 == 0 and args.end % 10000 == 0, "Start and end must be multiples of 10000!"
    # print(f"Downloading from {args.start//10000}w to {args.end//10000}w...")
    print(f"Downloading from {args.start} to {args.end}...")

    mapper = MyMultiProcess(MyFunction, workers_num)
    res = mapper(inputs=all_wets)
    res = sum(res, [])

    print(f"Total failed: {len(res)}")
    # write failed to file
    with open(args.failed_file, 'w') as f:
        f.write('\n'.join(res))

    end_time = datetime.datetime.now()
    print("End time:", end_time)
    print("Total time:", end_time - start_time)


if __name__ == '__main__' :
    main()