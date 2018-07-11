#!bin/bash

#calling Olivier Hagolle's script, make sure to change acount login password in apihub.txt
python Sentinel_download.py --latmin 36.1 --latmax 47.7 --lonmin 5.2 --lonmax 21.3 -a apihub.txt -m 30 -n -s S2A -l L2A -r 50000 > scsMData.txt

#extracting names of scenes with less than 30% cloud coverage
cat scsMData.txt | grep wget > outnames.txt

#this step can be avoided depending upon the location of the download_scene.R script
mv outnames.txt Test
