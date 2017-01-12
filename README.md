# OptimizeRasters
OptimizeRasters is a tool to convert collections of imagery and rasters to formats that are optimized for faster access and also optionally load the data to cloud storage. The main use is for conversion of imagery to different modes of MRF. MRF is an acronym for ‘meta-raster format’, and refers to a format created by NASA for storing and enabling fast access to imagery and other raster files using modern cloud storage (e.g. Amazon S3, Azure Blob storage). For discussion of this format, see this whitepaper: http://esriurl.com/MRF

OptimizeRasters can be run as a Geoprocessing tool within ArcGIS or as a command line program.
The input to OptimizeRasters is a directory of rasters along with associated metadata that typically come from the imagery provider. The output is a new directory that has the same metadata, but the files are in a format that is faster to read. The output files can be either TIF (Tiled with internal pyramids) or MRF. Both formats support pyramids and different compressions. Typically JPEG (lossy) and Deflate (lossless) are recommended for TIF, while LERC (controlled lossy) and JPEG (Lossy) are recommended for MRF.
 
OptimizeRasters enables the input and/or output directories to be file systems or Amazon S3 or Azure Block Storage, so can be used to transfer data from local storage to cloud storage. All the processes are run in parallel to as to optimize the use of available CPU and data bandwidth. While copying data to or from cloud storage breaks in the transmissions can occur. OptimizeRasters therefor contains a mechanism for identifying which files have potentially not been transferred correctly. It will repeat transfers a few times and if failures persist add the files to a list so that the process can be checked and complete at a later time.
 
MRF files can be read by ArcGIS directly as raster datasets. Further optimization can be achieved using Cloned MRF files that reference the MRF index and data files on slower storage, but keep local caches of the data to reduce repeated access. Caching MRF files are similar, but the source can be most GDAL readable files. OptimizeRasters can be used to create such Cloned and Caching MRF files.
 
Note that OptimizeRasters uses GDAL_Translate and GDALAddo to perform the conversion and so can read most GDAL readable file formats. There are some difference in versions GDAL and if installed on a machine with ArcGIS 10.4, OptimizeRasters will use a version of the GDAL binaries that is licensed with ArcGIS and include readers for formats such as JPEG2000 using Kakadu. If not installed on an machine with ArcGIS 10.4 then a public version of GDAL is used which cannot read JPEG2000 files.

##Installation
**Optional Packages**

    To upload to Amazon S3 and Microsoft Azure there are some third party packages for python that need to be installed. 
    The easiest way to install these third party packages is by using a tool for installing python packages. 
    This tool is called PIP. If the version of python installed on your machine is version 2.7.9 and above pip is already installed. 
    With ArcGIS 10.3.1, the python version is 2.7.8. Therefore pip would need to be installed. 

    Installing PIP. 
        1. Open this link in a browser.  https://pip.pypa.io/en/latest/installing/
        2. Under the heading Installing with get-pip.py right click and download the file get-pip.py
        3. run the command python get-pip.py. 
        example c:\PYTHON27\ArcGIS10.3\python.exe c:\temp\download\get-pip.py
   
    Installing boto (To use Amazon S3).
        1. Open a command line window. (Start/Run/Cmd)
        2. Browse to the Scripts folder within the python folder. 
            example cd c:\PYTHON27\ArcGIS10.3\Scripts
        3. Type in the line pip install boto 

    Installing Azure.
        Azure python module is required to read/write to (Microsoft Azure block blob) cloud file system.
        1. Open a command line window. (Start/Run/Cmd)
        2. If pip is not installed, install pip first. 
        3. Browse to the Scripts folder within the python folder. 
            example cd c:\PYTHON27\ArcGIS10.3\Scripts
        4. Type in pip install azure
        5. Browse to the link : https://pypi.python.org/pypi/azure/1.0.3

**Setup**

    It's recommended to use the setup EXE (OptimizeRastersToolsSetup.exe) found within the (Setup) folder
    in the (OptimizeRasters-master.zip) file downloaded from Github. However if a manual approach is preferred, 
    please follow the steps below,
    1. Download (OptimizeRasters-master.zip) from Github.
    2. Extract the zip contents to C:\Image_Mgmt_Workflows\ 

##Usage

    Using a DOS-CMD window.
    It's assumed the CWD is at C:\Image_Mgmt_Workflows\ and the (python.exe) is in path.
      1. To convert from TIF to MRF ([*local*]-input->-output[*Local*]) 
         python.exe OptimizeRasters.py -config=Templates\Imagery_to_TIF_JPEG.xml -input=c:\point_to_a_folder_with_tiffs
         -output=c:\output_processed_data\ -mode=mrf
         
      2. To create a Clone MRF
         python.exe OptimizeRasters.py -config=Templates\CloneMRF.xml -input=c:\point_to_a_folder_with_tiffs
         -output=c:\output_processed_data\clone -mode=clonemrf
         
      3. To create a MRF and to upload to AWS S3. Note> [To show syntax usage only. Not a working sample]
         Note> It's assumed the necessary AWS S3 access permissions are in place. Please refer to (OptmizeRasters_UserDoc.docx) for
         more info for using cloud file storage systems with OptimizeRasters.
         python.exe OptimizeRasters.py -config=Templates\Imagery_to_TIF_JPEG.xml -input=c:\point_to_a_folder_with_tiffs
         -cloudupload=true -clouduploadtype=amazon -output=processed_folder/a/b/c -outputprofile=my_permission_keys -mode=mrf
         
      4. To create caching MRFs with AWS S3 an input source. Note> [To show syntax usage only. Not a working sample]
         python.exe OptimizeRasters.py -input=input_folder/a/b/c -output=c:\output_processed_data\caching -clouddownload=true 
         -clouduploadtype=amazon -mode=cachingmrf
         
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
[](Esri Language: Python)? 