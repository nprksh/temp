# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 15:19:43 2022

@author: nprks
"""
import os
import datetime
import pandas as pd
import s3fs

import xarray as xr
import rioxarray

import netCDF4

import pytz

from osgeo import gdal


CREATE_GEOTIFF_FOR_NETCDF_VAR = [
'Rad',
'BCM',
'Area',
'CMI_C02',
'CMI_C07',
'CMI_C13',
'CMI_C14',
'CMI_C15',
]



def convertTZ(d, sourceTZ = "US/Pacific", destTZ = 'UTC'):
    tzS = pytz.timezone(sourceTZ)
    tzD = pytz.timezone(destTZ)
    return tzS.normalize(tzS.localize(d)).astimezone(tzD)



def getDirs(dirName = "."):
    return [name for name in os.listdir(dirName) if os.path.isdir(dirName + '/' + name)]
    

def getFiles(dirName = "."):
    return [name for name in os.listdir(dirName) if os.path.isfile(dirName + '/' +name)]


def getTimeRange(t, bufferMinutes = 60):
    
    if type(bufferMinutes) is not list:
        bufferMinutes = [bufferMinutes, bufferMinutes]
        
    t_delta_0 = datetime.timedelta(minutes=bufferMinutes[0])
    t_delta_1 = datetime.timedelta(minutes=bufferMinutes[1])
    
    return t - t_delta_0, t + t_delta_1



def getProductList(start_time, end_time, product = 'ABI-L1b-RadC', satellite = 16):
    
    
    ## Check if satellite number is valid
    assert satellite in [16, 17, 18]
    satellite = 'noaa-goes' + str(satellite)
    
    
    DATES = pd.date_range(f"{start_time:%Y-%m-%d %H:00}", f"{end_time:%Y-%m-%d %H:00}", freq="1H")
    
    
    
    # Use anonymous credentials to access public data  from AWS
    fs = s3fs.S3FileSystem(anon=True)
    
    # List all files for each date
    # ----------------------------
    files = []
    for DATE in DATES:
        files += fs.ls(f"{satellite}/{product}/{DATE:%Y/%j/%H/}", refresh=True)
    
    
    # Build a table of the files
    # --------------------------
    df = pd.DataFrame(files, columns=["file"])
    df[["product_mode", "satellite", "start", "end", "creation"]] = (
        df["file"].str.rsplit("_", expand=True, n=5).loc[:, 1:]
    )
    
    
    # Filter files by requested time range
    # ------------------------------------
    # Convert filename datetime string to datetime object
    df["start"] = pd.to_datetime(df.start, format="s%Y%j%H%M%S%f", utc=True)
    df["end"] = pd.to_datetime(df.end, format="e%Y%j%H%M%S%f", utc=True)    
    df["creation"] = pd.to_datetime(df.creation, format="c%Y%j%H%M%S%f.nc", utc=True)
    
    
    df["latency"] = df["creation"] - df["end"]
    
    
    ## For L1 product, extract bands
    if product[:-1] == 'ABI-L1b-Rad':
        df["band"] = df['product_mode'].str[-2:].astype(int)
        
        
    # Filter by files within the requested time range
    df = df.loc[df.start >= start_time].loc[df.end <= end_time].reset_index(drop=True)
    
    return df
 



def makeDir(path):
    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)
        


def convert2GTiff(pathNetCDF, pathGTiff):
            
    f = netCDF4.Dataset(pathNetCDF)
    
    ## Choose the first variable/subdataset in the netCDF file to convert to a GeoTIFF
    ## To select altnative variable stored in the file, adjust the list number, ie. [0] selects the 1st variable 
    # var = list(f.variables.keys())[0] 
    # print ("Selected variable: " + var)

    for var in f.variables.keys():
        if var not in CREATE_GEOTIFF_FOR_NETCDF_VAR:
            continue
            
        print ("Selected variable: " + var)
    
        # ## Uncomment the line below to reveal the list of variables stored in the file
        # print(list(f.variables.keys()))
        
        #!TODO: Implement mechanism to save meta information to a xml, csv, or any other machine/human readable file.
        
        # ## Open dataset and parse variable information
        # ds = xr.open_dataset(pathNetCDF)
        # var_name = ds[var].long_name
        # units_name = ds[var].units
        # variable = ds[var].data

        netCDF_file = rioxarray.open_rasterio('netcdf:{0}:{1}'.format(pathNetCDF, var))
        
        # Execute the conversion from netCDF to GeoTIFF
        netCDF_file.rio.to_raster(os.path.dirname(pathGTiff) + '/' + var + '_' + os.path.basename(pathGTiff))
        
        f = None
        netCDF_file = None
        
        
       
def filterByStartTime(df, startTime):
    return df[df['start'] == startTime]
    


def getFileName(AWS_FL_NAME, productName, tz = "US/Pacific"):
    
    start_time = datetime.datetime.strptime(AWS_FL_NAME.split('_')[3], "s%Y%j%H%M%S%f")
    
    band = ''
    
    if productName == 'RadC':
        band = '_B%s'%AWS_FL_NAME.split('_')[1][-2:]
        
    
    start_time = convertTZ(start_time, sourceTZ = "UTC", destTZ = tz)
        
    return start_time.strftime('%Y%m%d-%H%M')  + band + '.tif'
       



def download(df, BASEDIR, startTime = None, makeGeoTiff = False, verbose = True):

    product = df.iloc[0]['product_mode'].split('-')[2]
       
    pathNetCDF = BASEDIR + 'NetCDF/%s/'%product
    makeDir(pathNetCDF)
    

    

    if startTime is not None:
        assert any(startTime == df['start'])
        df = filterByStartTime(df, startTime)
        
    assert len(df) > 0
    
    
    # Use anonymous credentials to access public data  from AWS
    fs = s3fs.S3FileSystem(anon=True)
    
    
    for fl in df['file']:
        
        fl_name = fl.split('/')[-1]
        
        if verbose:
            print("Downloading from AWS: ", fl)
            
        fs.download(fl, pathNetCDF + '/' + fl_name)
        
        
        fName = getFileName(AWS_FL_NAME = fl_name, 
                            productName = product, 
                            tz = "US/Pacific")
        


        if makeGeoTiff:
            pathGTiff  = BASEDIR + 'GTif/%s/'%product
            makeDir(pathGTiff)

            if verbose:
                print("Generating GeoTIFF: ", pathGTiff  + '/' + fName)
                
            convert2GTiff(pathNetCDF + '/' + fl_name, 
                        pathGTiff  + '/' + fName)
    
