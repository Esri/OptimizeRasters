Echo off
Echo - Sample batch file to call OptimizeRasters and create a Caching MRF file for a Landsat scene stored on S3
pause
call c:\Python27\ArcGIS10.5\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -config=c:\Image_Mgmt_Workflows\OptimizeRasters\Templates\Landsat8_RasterProxy.xml -input=L8/160/043/LC81600432015109LGN00 -clouddownload=true -inputbucket=landsat-pds -clouddownloadtype=amazon -output=c:\temp\landsatpdsdata\L8\160\043\LC81600432015109LGN00

pause
rem Sample including writing cache to a special caching directory
rem call c:\Python27\ArcGIS10.5\python.exe c:\Image_Mgmt_Workflows\OptimizeRasters\OptimizeRasters.py -config=c:\Image_Mgmt_Workflows\OptimizeRasters\Templates\Landsat8_RasterProxy.xml -input=L8/160/043/LC81600432015109LGN00 -cache=c:\MRFCache\landsatpdsdata\L8\160\043\LC81600432015109LGN00 -output=c:\temp\landsatpdsdata\L8\160\043\LC81600432015109LGN00 -clouddownload=true -inputbucket=landsat-pds -clouddownloadtype=amazon 
