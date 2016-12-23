# OptimizeRasters

OptimizeRasters is a tool for accomplishing three tasks: converting raster data to optimized Tiled TIF or MRF files, moving data to cloud storage, and creating Raster Proxies.

**Converting raster data to optimized formats**

OptimizeRasters converts a variety of non-optimized raster formats into optimized Tiled TIF or MRF formats. The result is more efficient, scalable, and elastic data access with a lower storage cost.

File conversion speeds up read performance in three ways. First, it improves the data structure, which makes data access and transfer (especially from cloud storage) more efficient. Second, it generates pyramids, which provides faster access to data at smaller scales. Third, it offers optional compression, which further reduces the amount of data stored and transmitted.  

**Moving data to cloud storage**

As part of the data conversion process, OptimizeRasters can simultaneously transfer raster data to and from cloud (or enterprise) storage, speeding up the process of getting rasters into the cloud. OptimizeRasters supports both Amazon S3 and Microsoft Azure cloud storage services.

**Creating Raster Proxies**

OptimizeRasters can also generate Raster Proxies to simplify access to raster data stored on cloud or network storage. 
Raster Proxies are small files stored on local file systems that reference much larger raster data files stored remotely. A user can work efficiently with collections of small Raster Proxy files, which are accessed by ArcGIS like conventional raster files. At the same time, the application can access the large-volume, remotely stored raster data as needed. Raster Proxy files can also cache tiles read from the slower remote storage, speeding up access and reducing the need to access the same data multiple times.

## Features
* Streamlined data management 
* Faster read performance
* Simplified, efficient transfer into and out of cloud storage 
* Compression options that minimize storage requirements 
* Configurable using included editable XML files
* Open source code, implemented in Python using GDAL to handle a variety of raster formats

## Instructions
**OptimizeRasters Setup**

1. Download the (OptimizeRasters setup file) from GitHub. In a browser, navigate to  https://github.com/Esri/OptimizeRasters/raw/master/Setup/OptimizeRastersToolsSetup.exe. The file should begin downloading immediately. 

2. Double click the downloaded file to install.

**Optional Packages for Cloud Storage**

To upload to Amazon S3 and Microsoft Azure cloud storage, there are some third party packages for Python that need to be installed. The easiest way to install these third party packages is to use a tool for installing Python packages called PIP. PIP is included with Python 2.7.9 and above, which is installed with ArcGIS 10.4 and above. However, if you have a previous version of ArcGIS, PIP will need to be installed manually. 

    Installing PIP. 

1. Open https://pip.pypa.io/en/latest/installing/ in a browser.

2. To download the PIP installation file, look under the heading Installing with get-pip.py, right click on get-pip.py, select Save Link As…, navigate to the folder you wish to use, and select Save.

3. Open a command prompt. Using full file locations, run the command “python get-pip.py”

EXAMPLE: C:/Python27/ArcGIS10.4/python.exe C:/temp/download/get-pip.py
   
    Installing boto (required to use Amazon S3).

1.	Open a command prompt. Navigate to the Scripts folder in the Python folder.

EXAMPLE: cd C:/Python27/ArcGIS10.4/Scripts

2.	Type the command, “pip install boto”

    Installing Azure (required to use Microsoft Azure).

1.	Open a command prompt. Navigate to the Scripts folder in the Python folder.

EXAMPLE: cd C:/Python27/ArcGIS10.4/Scripts

2.	Type the command, “pip install azure”

## Requirements

* Python 2.7 or greater (installed with ArcMap 10.4 / Pro 1.3 ). 
* The OptimizeRasters geoprocessing toolbox requires ArcGIS Map 10.4,1 / Pro 1.3 or higher.
* OptimizeRasters can be run from the command line even if ArcGIS is not installed.
* There are OptimizeRasters versions for both Windows and Linux. 
* OptimizeRasters can be used with Amazon Web Services Lambda serverless compute service, with some restrictions on data sizes.

## Resources

* [MRF white paper]( http://esriurl.com/MRF%E2%80%9D)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Licensing
Copyright 2016 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [license.txt](https://github.com/Esri/OptimizeRasters/blob/master/LICENSE) file.

[](Esri Tags: MRF, BOTO, AWS)
[](Esri Language: Python)

