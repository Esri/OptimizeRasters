
OptimizeRasters
===============

OptimizeRasters is a set of tools for accomplishing three tasks: converting raster data to optimized file formats, moving data to cloud storage, and creating raster proxies. The result is more efficient, scalable, and elastic data access with a lower storage cost. 

**Converting raster data to optimized formats**

OptimizeRasters converts a variety of non-optimized raster formats into optimized tiled TIFF, Cloud-Optimized GeoTIFF, or MRF formats. File conversion speeds up read performance in three ways. First, it improves the data structure, which makes data access and transfer (especially from cloud storage) more efficient. Second, it generates pyramids, which provides faster access to data at smaller scales. Third, it offers optional compression, which further reduces the amount of data stored and transmitted.  

**Moving data to cloud storage**

As part of the data conversion process, OptimizeRasters can simultaneously transfer raster data to and from cloud (or enterprise) storage, speeding up the process of getting rasters into the cloud. OptimizeRasters supports Amazon S3, Microsoft Azure, and Google Cloud storage services.

**Creating raster proxies**

OptimizeRasters can also generate raster proxies to simplify access to raster data stored on cloud or network storage. 
Raster proxies are small files stored on local file systems that reference much larger raster data files stored remotely. A user can work efficiently with collections of small raster proxy files, which are accessed by ArcGIS like conventional raster files. At the same time, the application can access the large-volume, remotely stored raster data as needed. Raster proxy files can also cache tiles read from the slower remote storage, speeding up access and reducing the need to access the same data multiple times.

## Features
* Streamlined data management 
* Faster read performance
* Simplified, efficient transfer into and out of cloud storage 
* Compression options that minimize storage requirements 
* Configurable using included editable XML files
* Open source code, implemented in Python using GDAL to handle a variety of raster formats

## What's New in OptimizeRasters 2.0.2
* Cloud-Optimized GeoTIFF (COG) support
* google-cloud-storage support
* AWS Lambda implementation
* Embed raster proxies in mosaic datasets by generating them as a table
* Expanded documentation
* Additional minor fixes and performance enhancements

## Instructions
**OptimizeRasters Setup**

1. Click the link to download the [OptimizeRasters setup file](https://github.com/Esri/OptimizeRasters/raw/master/Setup/OptimizeRastersToolsSetup.exe) from GitHub. The file should begin downloading immediately. 

2. Double click the downloaded file to install.

**Optional Packages for Cloud Storage**

To upload to Amazon S3, Microsoft Azure, or Google Cloud Storage, there are some third-party packages for Python that need to be installed. 

If azure, boto3, or google-cloud-storage is already installed, it is important to ensure you are running the most up-to-date version to avoid errors.

More information about installing and updating third-party packages can be found in the [OptimizeRasters documentation](https://github.com/Esri/OptimizeRasters/tree/master/Documentation).

## Requirements

* Python 2.7 or Python 3+ (installed with ArcMap 10.4+ / Pro 1.3+). 
* The OptimizeRasters geoprocessing toolbox requires ArcGIS Map 10.4.1+ / Pro 1.3+.
* OptimizeRasters can be run from the command line even if ArcGIS is not installed.
* There are OptimizeRasters versions for both Windows and Linux. 
* OptimizeRasters can be used with Amazon Web Services Lambda serverless compute service, with some restrictions on raster file size.

## Resources

* [OptimizeRasters documentation](https://github.com/Esri/OptimizeRasters/tree/master/Documentation)
* [MRF User Guide](https://github.com/nasa-gibs/mrf/blob/master/doc/MUG.md)

## Issues

Find a bug or want to request a new feature? Please let us know by submitting an issue, or adding a comment to the [OptimizeRasters discussion](https://geonet.esri.com/message/690662-optimizerasters) on GeoNet.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Licensing
Copyright 2016 Esri

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

 
