# scrape-himawari-8

Python script to scrape Himawari-8 images from the Regional and Mesoscale Meteorology Branch

![Example](images/202009210250.png)

## Usage

This is a command line script which requires a path to save images to as a mandatory parameter first and three optional parameters to define how many times to attempt to download an image, how many days before present to retrieve images, and how many parallel jobs to use to download the images. The script is semi-smart and will check the path to see which images have already been downloaded and it won't re-download and overwrite images you already have.

This script can be run by:

### Windows

python scrape_satellite.py C:\Users\Me\full_disk_ahi_true_color -max_retries 5 -timeout 5 -look_back_days 15 -n_jobs 2

### Mac / Linux

python scrape_satellite.py ~/full_disk_ahi_true_color -max_retries 5 -timeout 5 -look_back_days 15 -n_jobs 2

The max_retries, timeout, look_back_days, and n_jobs parameters are optional. They default to 10 retries, 5 second timeout, 21 day look back, and -1 jobs respectively (-1 jobs means create as many jobs as there are virtual cores on the machine). The Regional and Mesoscale Meteorology Branch only make 28 days of images available so setting look_back_days larger than that won't help.

## Requirements

Other than the standard library packages this script uses:

- joblib
- pandas
- requests
- urlib3
- tqdm

## For Python Newbies

If you're new to Python there's a bit of a learning curve to installing Python and the required additional packages/libraries. I personally like the [Anaconda Python](https://www.anaconda.com/distribution/) flavour which is available for Linux, Mac and Windows as I find it a bit more fool proof than the other flavours (but there's still a few gotchas to be wary of). If you do install the Anaconda distribution it should include out of the box all the other required packages needed to run this script in its default environment.
