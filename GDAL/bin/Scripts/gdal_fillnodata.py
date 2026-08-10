#!C:\Users\ptbuild\.conan2\p\b\cpyth57e3e7b42a4bf\p\bin\python.exe

import sys

from osgeo.gdal import deprecation_warn

# import osgeo_utils.gdal_fillnodata as a convenience to use as a script
from osgeo_utils.gdal_fillnodata import *  # noqa
from osgeo_utils.gdal_fillnodata import main

deprecation_warn("gdal_fillnodata")
sys.exit(main(sys.argv))
