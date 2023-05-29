Last updated at 16:40 2/23/2023

Tested with Ubuntu 18.04.5 LTS

Setup>
https://esriis-my.sharepoint.com/:f:/g/personal/cham4757_esri_com/EsvFNawbXjlGkKuL49Gi9zIByEpUeZRRmhAHK8t7DzxcpA?e=oHjod6

Info>
pre-cooked binary versions used,
GDAL 3.1.2, released 2020/07/07
curl 7.73.1

Pease note the following steps to get OR installed on Linux

sudo mkdir /app
sudo tar -xzvf OR.tar.gz -C /app
cd /app

sudo apt-get -y install python3-pip
sudo -H pip3 install --trusted-host pypi.python.org -r requirements.txt
sudo -H pip3 install azure.storage.blob==12.3.1 azure.storage.queue

export LD_LIBRARY_PATH=/app/GDAL/lib
export GDAL_DATA=/app/GDAL/share/gdal
export PATH=$PATH:/app/curl/lib/:/app/curl/bin/:/app/GDAL/bin
export PROJ_DATA=/app/proj

Verify at the console if GDAL and curl is now on path by typing in the following command(s)
curl 
gdalinfo

The credentials for the AWS must exist in the ~/.aws/credentials file as mentioned in the OR documentation.

Usage>
Usage is same as how it's shown in the OR documentation.
e.g.
1. python3 OptimizeRasters.py -config ./Templates/Imagery_to_MRF_LERC.xml -input inputFolder -output outputFolder 
2. python3 OptimizeRasters.py -config ./Templates/Imagery_to_MRF_LERC.xml -input remoteInput -output localOutput -subs f -inputprofile inputprofile -inputbucket inputBucket -clouddownload true

-Chamlika
