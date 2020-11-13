# CMIPDownloader
Downloader of CMIP (Coupled Model Intercomparison Project) data

## Usage
```
usage: cmip_downloader.py [-h] -v VARIABLE -f FREQUENCY -e EXPERIMENT
                          [-o OUTPUT_DIR] [-c {5,6}] [-n NUM_WORKERS]

optional arguments:
  -h, --help            show this help message and exit
  -v VARIABLE, --variable VARIABLE
                        variable name: tas, pr
  -f FREQUENCY, --frequency FREQUENCY
                        frequency: mon,
  -e EXPERIMENT, --experiment EXPERIMENT
                        experiment id: amip-piForcing
  -o OUTPUT_DIR, --output_dir OUTPUT_DIR
                        output directory, default to current directory
  -c {5,6}, --cimp_version {5,6}
                        CIMP version: 5 or 6
  -n NUM_WORKERS, --num_workers NUM_WORKERS
                        number of workers to download in parallel
```

Example
```
python3 cmip_downloader.py -v tas -f mon -e amip-piForcing -o /data/cimp6
```