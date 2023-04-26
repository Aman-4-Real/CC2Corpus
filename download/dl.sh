#!/bin/bash
###
 # @Author: Aman
 # @Date: 2023-04-23 21:03:30
 # @Contact: cq335955781@gmail.com
 # @LastEditors: Aman
 # @LastEditTime: 2023-04-24 14:25:20
### 
#!/bin/bash

# parameter definition
########## Most used parameters ##########
# how many groups of data to download
K=1
# Save path
save_path='./'
# Num of workers to download
num_workers=8
# Init download start position
init_start=200000
##########################################
step=10000
ERROR_TRIES=2
##########################################


# loop download
for ((i=0; i<K; i++))
do
    echo -e "NO.$((i+1)) downloading starts...\n"

    rfile='all_wets.txt' # the index file containing all the urls
    start=$((init_start+i*step))
    end=$((start+step))
    # echo "start: $start, end: $end"
    m=$((start/step))
    n=$((end/step))
    # echo "m: $m, n: $n"
    failed_file='failed_'$m'w_'$n'w.txt'
    error_tries=$ERROR_TRIES

    while true
    do
        if [[ "$rfile" =~ failed ]] # If it is a failed file, there is no start and end.
        then
            python multi_process_dl_by_url.py --rfile $rfile --num_workers $num_workers \
                                              --save_path "${save_path}/wet_files_${m}w_${n}w/" \
                                              --failed_file $failed_file --start 0 --end -1
        else
            python multi_process_dl_by_url.py --rfile $rfile --num_workers $num_workers \
                                              --save_path "${save_path}/wet_files_${m}w_${n}w/" \
                                              --failed_file $failed_file --start $start --end $end
        fi
        
        # check failed file
        if [ -s $failed_file ]
        then
            echo -e "Failed files detected, downloading again...\n"
            rfile=$failed_file
            error_tries=$((error_tries-1))
        else
            echo -e "No other files to download, downloading ends!\n"
            break
        fi

        if [ $error_tries -eq 0 ]
        then
            echo -e "Download failed too many times, exit!\n"
            break
        fi
    done
    echo -e "NO.$((i+1)) downloading ends!\n"
    echo -e "----------------------------------------\n"
done

