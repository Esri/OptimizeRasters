# OptimizeRasters
This Module is to convert input raster into optimized output raster. It has support to convert raster in MRF format and upload it to s3 bucket
•	Unzip the zip file in C:\ Image_Mgmt_Workflows\
•	If uploading to s3 or downloading from s3 boto is required to be install https://pip.pypa.io/en/latest/installing.html#python-os-support
•	Then go to the folder where pip is installed i.e c:\Python27\ArcGIS10.3\Scripts and in the command prompt type  ----- > pip install boto
•	Setup the system environment variable as GDAL_Data value to it is path to the gdal data folder i.e c:\Image_Mgmt_Workflows\OptimizeRasters\tools\data
•	Update the gdal path in the optimizerastes.xml

Some of the example command lines are 
Converting TIF to MRF ( input local output Local ) 
•	c:\Python27\ArcGIS10.3\python.exe C:\c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input=e:\projects\OptimizeRaster\indata -output=e:\projects\OptimizeRaster\outdata -mode=mrf
Creating Clone MRF
•	c:\Python27\ArcGIS10.3\python.exe C:\c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters..py -input=e:\projects\OptimizeRaster\outdata -output=e:\projects\OptimizeRaster\cachedata -mode=clonemrf
 
Creating MRF and uploading to s3 ( make sure you use correct slash and cases for s3 as its case sensitive also s3 and the permission to access the data from that machine) 
In this case you need to define the s3 output folder and the keys in the config file 
•	c:\PYTHON27\ArcGIS10.3\python.exe C:\c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input= e:\projects\OptimizeRaster\indata -output=/s3bucketfolder/a/s/r -tempoutput= c:\temp\convertdata -s3output=True –mode=mrf
 Creating caching MRF with s3 as input 
•	c:\PYTHON27\ArcGIS10.3\python.exe C:\c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -input= s3bucketfolder/a/s/r -output= e:\projects\OptimizeRaster\s3caching -s3input=True –mode=cachingmrf
