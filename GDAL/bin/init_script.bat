@echo off
REM Set the current directory as the base directory
set BASE_DIR=%~dp0

REM Add the bin directory to the PATH
set PATH=%BASE_DIR%;%PATH%

REM Set GDAL Plugin driver path
set GDAL_DRIVER_PATH=%BASE_DIR%gdalplugins

REM Set GDAL_DATA environment variable
set GDAL_DATA=%BASE_DIR%gdaldata

REM Set GDAL Config File path
set GDAL_CONFIG_FILE=%BASE_DIR%gdalrc

REM Set PROJ_LIB environment variable
set PROJ_LIB=%GDAL_DATA%

REM Set IIQ Sensor Profiles Location
set IIQ_SENSOR_PROFILES_LOCATION=%GDAL_DATA%\IIQSensorProfiles

REM Confirm the environment variables
echo.
echo Environment variables set:
echo PATH=%PATH%
echo GDAL_DRIVER_PATH=%GDAL_DRIVER_PATH%
echo GDAL_CONFIG_FILE=%GDAL_CONFIG_FILE%
echo GDAL_DATA=%GDAL_DATA%
echo PROJ_LIB=%PROJ_LIB%
echo IIQ_SENSOR_PROFILES_LOCATION=%IIQ_SENSOR_PROFILES_LOCATION%

REM Pause to allow the user to see the output
cmd /k