#!/bin/bash
###
 # @Author: Aman
 # @Date: 2023-04-23 21:03:30
 # @Contact: cq335955781@gmail.com
 # @LastEditors: Aman
 # @LastEditTime: 2023-04-24 02:08:04
### 
#!/bin/bash

# 参数定义
################ 常用参数 ################
# 下载几组数据
K=1
# 保存路径
save_path='./'
# 下载进程数
num_workers=8
# 初始下载起始位置
init_start=200000
#########################################
step=10000
ERROR_TRIES=2
#########################################


# 循环下载
for ((i=0; i<K; i++))
do
    echo -e "第 $((i+1)) 次循环下载开始...\n"

    rfile='all_wets.txt'
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
        if [[ "$rfile" =~ failed ]] # 如果是失败文件，则不存在start和end
        then
            python multi_process_dl_by_url.py --rfile $rfile --num_workers $num_workers \
                                              --save_path "${save_path}/wet_files_${m}w_${n}w/" \
                                              --failed_file $failed_file --start 0 --end -1
        else
            python multi_process_dl_by_url.py --rfile $rfile --num_workers $num_workers \
                                              --save_path "${save_path}/wet_files_${m}w_${n}w/" \
                                              --failed_file $failed_file --start $start --end $end
        fi
        
        # 检查失败文件
        if [ -s $failed_file ]
        then
            echo -e "有失败文件，重新下载...\n"
            rfile=$failed_file
            error_tries=$((error_tries-1))
        else
            echo -e "无可下载的失败文件，下载结束！\n"
            break
        fi

        if [ $error_tries -eq 0 ]
        then
            echo -e "下载失败次数过多，退出下载！\n"
            break
        fi
    done
    echo -e "第 $((i+1)) 次循环下载结束！\n"
    echo -e "----------------------------------------\n"
done

