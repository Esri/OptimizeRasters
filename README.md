# OptimizeRasters
Optimize raster is a command line tool that converts raster from one format to another, the output format that is created is an optimized such that it improves the performance of the raster when they are used. It also has the option to builds pyramids on the output raster and compress the imagery so save the storage space. This tool also allows to read the data from amazon s3 bucket or upload the converted data to amazon s3 bucket. There are many parameters that user can configure based on their how they want the output raster to be written out with what compression and pyramids. It also has the capability of write the intermediate data on fast disk during the conversion process if the input is from a slower disk.

*	Unzip the zip file in C:\ Image_Mgmt_Workflows\
*	If uploading to s3 or downloading from s3; boto is required to be installed. 
*	First download pip.py from https://pip.pypa.io/en/latest/installing.html#python-os-support place it in c:\Python27\ArcGIS10.3 and at command line run the following command c:\Python27\ArcGIS10.3\python.exe get-pip.py
*	Then go to the folder where pip is installed i.e c:\Python27\ArcGIS10.3\Scripts and in the command prompt run the following ( the command window needs to be in the same path where the pip scripts is present) ----- > pip install boto
*	If you unzip in a different location Update the gdal path in the optimizerastes.xml

Refer to the user documentation for more command line arguments and various parameters in the configuration file

Some of the example command lines are 
Converting TIF to MRF ( input local output Local ) 
*	c:\Python27\ArcGIS10.3\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=e:\projects\OptimizeRaster\indata -output=e:\projects\OptimizeRaster\outdata -mode=mrf
Creating Clone MRF
*	c:\Python27\ArcGIS10.3\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters..py -input=e:\projects\OptimizeRaster\outdata -output=e:\projects\OptimizeRaster\cachedata -mode=clonemrf
 
Creating MRF and uploading to s3 ( make sure you use correct slash and cases for s3 as its case sensitive also s3 and the permission to access the data from that machine) 
In this case you need to define the s3 output folder and the keys in the config file 
*	c:\PYTHON27\ArcGIS10.3\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=e:\projects\OptimizeRaster\indata -output=s3bucketfolder/a/s/r -tempoutput=c:\temp\convertdata -s3output=True -mode=mrf
 Creating caching MRF with s3 as input 
*	c:\PYTHON27\ArcGIS10.3\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=s3bucketfolder/a/s/r -output=e:\projects\OptimizeRaster\s3caching -s3input=True -mode=cachingmrf
