# __init__ for osgeo package.

# making the osgeo package version the same as the gdal version:
from sys import platform, version_info
from os.path import exists
from os import getenv, makedirs, environ
from arcpy_init import product_install_dir
import platformdirs

install_path = product_install_dir()
bin_path = install_path / 'bin'

gdalplugin_path = bin_path / 'gdalplugins'
gdalplugins_path = [str(gdalplugin_path)]
if getenv('GDAL_DRIVER_PATH') is not None:
    gdalplugins_path.append(getenv('GDAL_DRIVER_PATH'))

environ['GDAL_DRIVER_PATH'] = ';'.join(gdalplugins_path)

config_file = bin_path / 'gdalrc'
environ['GDAL_CONFIG_FILE'] = str(config_file)

gdaldata_dir = install_path / 'Resources' / 'pedata' / 'gdaldata'
environ['GDAL_DATA'] = str(gdaldata_dir)

iiqsensorprofiles_dir = gdaldata_dir / 'IIQSensorProfiles'
environ['IIQ_SENSOR_PROFILES_LOCATION'] = str(iiqsensorprofiles_dir)

for gdal_config_path in [bin_path, gdalplugin_path, config_file, gdaldata_dir]:
    if not gdal_config_path.exists():
        msg = "The GDAL installation seems to be corrupted, the following path does not exist: {}".format(gdal_config_path)
        raise Exception(msg)

if (gdalplugin_path / 'gdal_IIQ.dll').exists():
    if not iiqsensorprofiles_dir.exists():
        msg = "IIQSensorProfiles directory does not exist: {}".format(iiqsensorprofiles_dir)
        raise Exception(msg)

if version_info >= (3, 8, 0) and platform == 'win32':
    import os
    if 'USE_PATH_FOR_GDAL_PYTHON' in os.environ and 'PATH' in os.environ:
        for p in os.environ['PATH'].split(';'):
            if p and os.path.exists(p):
                try:
                    os.add_dll_directory(p)
                except (FileNotFoundError, OSError):
                    continue


def swig_import_helper():
    import importlib
    from os.path import dirname, basename
    mname = basename(dirname(__file__)) + '._gdal'
    try:
        return importlib.import_module(mname)
    except ImportError:
        return importlib.import_module('_gdal')


_gdal = swig_import_helper()
del swig_import_helper

__version__ = _gdal.__version__ = _gdal.VersionInfo("RELEASE_NAME")

gdal_version = tuple(int(s) for s in str(__version__).split('.') if s.isdigit())[:3]
python_version = tuple(version_info)[:3]

# Setting this flag to True will cause importing osgeo to fail on an unsupported Python version.
# Otherwise a deprecation warning will be issued instead.
# Importing osgeo fom an unsupported Python version might still partially work
# because the core of GDAL Python bindings might still support an older Python version.
# Hence the default option to just issue a warning.
# To get complete functionality upgrading to the minimum supported version is needed.
fail_on_unsupported_version = False

# The following is a Sequence of tuples in the form of (gdal_version, python_version).
# Each line represents the minimum supported Python version of a given GDAL version.
# Introducing a new line for the next GDAL version will trigger a deprecation warning
# when importing osgeo from a Python version which will not be
# supported in the next version of GDAL.
gdal_version_and_min_supported_python_version = (
    ((0, 0), (0, 0)),
    ((1, 0), (2, 0)),
    ((2, 0), (2, 7)),
    ((3, 3), (3, 6)),
    # ((3, 4), (3, 7)),
    # ((3, 5), (3, 8)),
)


def ver_str(ver):
    return '.'.join(str(v) for v in ver) if ver is not None else None


minimum_supported_python_version_for_this_gdal_version = None
this_python_version_will_be_deprecated_in_gdal_version = None
last_gdal_version_to_supported_your_python_version = None
next_version_of_gdal_will_use_python_version = None
for gdal_ver, py_ver in gdal_version_and_min_supported_python_version:
    if gdal_version >= gdal_ver:
        minimum_supported_python_version_for_this_gdal_version = py_ver
    if python_version >= py_ver:
        last_gdal_version_to_supported_your_python_version = gdal_ver
    if not this_python_version_will_be_deprecated_in_gdal_version:
        if python_version < py_ver:
            this_python_version_will_be_deprecated_in_gdal_version = gdal_ver
            next_version_of_gdal_will_use_python_version = py_ver


if python_version < minimum_supported_python_version_for_this_gdal_version:
    msg = 'Your Python version is {}, which is no longer supported by GDAL {}. ' \
          'Please upgrade your Python version to Python >= {}, ' \
          'or use GDAL <= {}, which supports your Python version.'.\
        format(ver_str(python_version), ver_str(gdal_version),
               ver_str(minimum_supported_python_version_for_this_gdal_version),
               ver_str(last_gdal_version_to_supported_your_python_version))

    if fail_on_unsupported_version:
        raise Exception(msg)
    else:
        from warnings import warn
        warn(msg, DeprecationWarning)
elif this_python_version_will_be_deprecated_in_gdal_version:
    msg = 'You are using Python {} with GDAL {}. ' \
          'This Python version will be deprecated in GDAL {}. ' \
          'Please consider upgrading your Python version to Python >= {}, ' \
          'Which will be the minimum supported Python version of GDAL {}.'.\
        format(ver_str(python_version), ver_str(gdal_version),
               ver_str(this_python_version_will_be_deprecated_in_gdal_version),
               ver_str(next_version_of_gdal_will_use_python_version),
               ver_str(this_python_version_will_be_deprecated_in_gdal_version))

    from warnings import warn
    warn(msg, DeprecationWarning)

import errno
import winreg
fnf_exception = getattr(__builtins__,
                        'FileNotFoundError', WindowsError)
def proreg(reg_path, lookup_key, reverseCheck = False):
    """ Look up a specific Pro registry key, optionally returning
        the related hive instead of the value itself."""
    READ_ACCESS = (winreg.KEY_WOW64_64KEY + winreg.KEY_READ)
    root_keys = (
        ('HKCU', winreg.HKEY_CURRENT_USER),
        ('HKLM', winreg.HKEY_LOCAL_MACHINE)
    )
    ordered_root_keys = root_keys[::-1] if reverseCheck else root_keys
    for (key_name, root_key) in ordered_root_keys:
        try:
            key = winreg.OpenKey(root_key, reg_path, 0, READ_ACCESS)
        except fnf_exception as error:
            key = None
            if error.errno == errno.ENOENT:
                pass
            else:
                raise
        if key:
            try:
                raw_value = winreg.QueryValueEx(key, lookup_key)[0]
                if raw_value:
                        return raw_value
            except fnf_exception as error:
                if error.errno == errno.ENOENT:
                    pass
                else:
                    raise
    return None

proxyDir = proreg('Software\\ESRI\\ArcGISPro\\Raster\\Environment', 'TMPDIR', True)
if proxyDir is None and platform == 'win32':
    proxyDir = platformdirs.windows.get_win_folder("CSIDL_LOCAL_APPDATA") + '\\ESRI\\rasterproxies'

if proxyDir is not None and _gdal.GetConfigOption('GDAL_PAM_PROXY_DIR') is None:
    if not exists(proxyDir):
        makedirs(proxyDir)
    _gdal.SetConfigOption('GDAL_PAM_PROXY_DIR', proxyDir)
