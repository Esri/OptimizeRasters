# OptimizeRasters
OptimizeRasters is a tool to convert collections of imagery and rasters to formats that are optimized for faster access and also optionally load the data to cloud storage. The main use is for conversion of imagery to different modes of MRF, a cloud optimized raster format designed to enable fast access to raster data stored on cloud storage. OptimizeRasters can be run as a Geoprocessing tool within ArcGIS or as a command line program.

The input to OptimizeRasters is a directory of rasters along with associated metadata that typically come from the imagery provider. The output is a new directory that has the same metadata, but the files are in a format that is faster to read. The output files can be either TIF (Tiled with internal pyramids) or MRF. Both formats support pyramids and different compressions. Typically JPEG (lossy) and Deflate (lossless) are recommended for TIF, while LERC (controlled lossy) and JPEG (Lossy) are recommended for MRF.
 
OptimizeRasters enables the input and/or output directories to be file systems or Amazon S3 or Azure Block Storage, so can be used to transfer data from local storage to cloud storage. All the processes are run in parallel to as to optimize the use of available CPU and data bandwidth. While copying data to or from cloud storage breaks in the transmissions can occur. OptimizeRasters therefor contains a mechanism for identifying which files have potentially not been transferred correctly. It will repeat transfers a few times and if failures persist add the files to a list so that the process can be checked and complete at a later time.
 
MRF files can be read by ArcGIS directly as raster datasets. Further optimization can be achieved using Cloned MRF files that reference the MRF index and data files on slower storage, but keep local caches of the data to reduce repeated access. Caching MRF files are similar, but the source can be most GDAL readable files. OptimizeRasters can be used to create such Cloned and Caching MRF files.
 
Note that OptimizeRasters uses GDAL_Translate and GDALAddo to perform the conversion and so can read most GDAL readable file formats. There are some difference in versions GDAL and if installed on a machine with ArcGIS 10.4, OptimizeRasters will use a version of the GDAL binaries that is licensed with ArcGIS and include readers for formats such as JPEG2000 using Kakadu. If not installed on an machine with ArcGIS 10.4 then a public version of GDAL is used which cannot read JPEG2000 files.
##Installation
*	Unzip the zip file in C:\ Image_Mgmt_Workflows\
*	If uploading to s3 or downloading from s3; boto is required to be installed. 
*	First download pip.py from https://pip.pypa.io/en/latest/installing.html#python-os-support place it in c:\Python27\ArcGIS10.4 and at command line run the following command c:\Python27\ArcGIS10.4\python.exe get-pip.py
*	Then go to the folder where pip is installed i.e c:\Python27\ArcGIS10.4\Scripts and in the command prompt run the following ( the command window needs to be in the same path where the pip scripts is present) ----- > pip install boto
*	If you unzip in a different location Update the gdal path in the optimizerastes.xml

Refer to the user documentation for more command line arguments and various parameters in the configuration file
## Sample Commands
Some of the example command lines are 
Converting TIF to MRF ( input local output Local ) 
```
c:\Python27\ArcGIS10.4\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=e:\projects\OptimizeRaster\indata -output=e:\projects\OptimizeRaster\outdata -mode=mrf
```
Creating Clone MRF
```
<<<<<<< HEAD
c:\Python27\ArcGIS10.4\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=e:\projects\OptimizeRaster\outdata -output=e:\projects\OptimizeRaster\cachedata -mode=clonemrf
=======
c:\Python27\ArcGIS10.4\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=e:\projects\OptimizeRaster\outdata -output=e:\projects\OptimizeRaster\cachedata -mode=clonemrf
>>>>>>> origin/master
 ```
Creating MRF and uploading to s3 ( make sure you use correct slash and cases for s3 as its case sensitive also s3 and the permission to access the data from that machine) 
In this case you need to define the s3 output folder and the keys in the config file 
```
c:\PYTHON27\ArcGIS10.4\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=e:\projects\OptimizeRaster\indata -output=s3bucketfolder/a/s/r -tempoutput=c:\temp\convertdata -s3output=True -mode=mrf
```
 Creating caching MRF with s3 as input 
```
c:\PYTHON27\ArcGIS10.4\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=s3bucketfolder/a/s/r -output=e:\projects\OptimizeRaster\s3caching -s3input=True -mode=cachingmrf
```

## Issues

Find a bug or want to request a new feature?  Please let us know by [submitting an issue](../../issues).


## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Licensing
Copyright 2015 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [License.txt](License.txt?raw=true) file.

[](Esri Tags: MRF,BOTO,AWS)
[](Esri Language: Python)​
