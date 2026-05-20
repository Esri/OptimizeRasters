#!C:\Users\ptbuild\.conan2\p\b\cpyth57e3e7b42a4bf\p\bin\python.exe

import sys

from osgeo.gdal import deprecation_warn

# import osgeo_utils.ogr_layer_algebra as a convenience to use as a script
from osgeo_utils.ogr_layer_algebra import *  # noqa
from osgeo_utils.ogr_layer_algebra import main

deprecation_warn("ogr_layer_algebra")
sys.exit(main(sys.argv))
