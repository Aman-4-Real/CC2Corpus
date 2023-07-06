####################################
### clean tage 1 - filtering
####################################
DATA_FLAG="YOUR_DATA_NAME"

python coarse_clean_mp.py \
    --data_path ../$DATA_FLAG/ \
    --output_path ../"SOME_DIR"/$DATA_FLAG/ \
    --num_workers 64


####################################
### clean tage 2 - using ccnet
####################################
DATA_DIR="YOUR_DATA_DIR" # ../"SOME_DIR"/
SAVE_DIR="YOUR_SAVE_DIR"

cd /CC_NET_PATH/cc_net

python -m cc_net \
    --config config/your_config.json \
    --cache_dir $DATA_DIR/$DATA_FLAG \
    --output_dir $SAVE_DIR/$DATA_FLAG \
    --num_shards 8 \
    --mine_num_processes 4 \


####################################
### clean tage 3 - post filtering
####################################
python post_filtering.py \
    --data_path $SAVE_DIR/$DATA_FLAG \
    --output_path "YOUR_FINAL_SAVE_DIR" \
    --num_workers 64


