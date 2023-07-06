# Download

This dir contains codes to clean `.wet` or `.wet.gz` files downloaded before.

## Usage
- `coarse_clean_mp.py`: the script to do the pre-filtering stage.
- For cc_net, you should clone it from [[cc_net]](https://github.com/facebookresearch/cc_net) to this folder and install it.
- `post_filter_mp.py`: the script to do the post-filtering stage.
- `DocCleaner.py`: this file contains all the cleaning rules. You can modify it according to your own needs.
- `process_wet_file.py`: utils to process the `.wet` files.

You need to adjust the above cleaning rules as yours and change the configs like `PATH` to your own ones and run:
```
# Change your config in run_pipe.sh fisrt
bash run_pipe.sh
```



