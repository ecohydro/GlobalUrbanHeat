# Program finds the TMax from CHIRTSMax Data and a raster with polygon IDS burned
# By Cascade Tuholske 2019-08-20

# Dependencies
import rasterio 
import numpy as np
import pandas as pd
import geopandas as gpd
from rasterstats import zonal_stats
from rasterio import features
import os
from ftplib import FTP
import xarray as xr
import fnmatch
import re

# Directories 
CHIRT_DIR = '/Users/cascade/Github/PopRaster/data/raw/CHIRT/' # <<--- path to loop through
SHP_DIR = '/Users/cascade/Github/PopRaster/data/raw/JRC/ghs-ucdb/'
POLY_RST_DIR = '/Users/cascade/Github/PopRaster/data/interim/'
DATA_OUT = '/Users/cascade/Github/PopRaster/data/interim/'

# Open Polygon Raster
polyRst_fn = 'GHS_UCDB_Raster_Raster_touched.tif'
polyRst = rasterio.open(POLY_RST_DIR+polyRst_fn)

# Open the file with GeoPANDAS read_file
shp_fn = 'GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_0.shp'
shps = gpd.read_file(SHP_DIR+shp_fn)

# Set fn out, change as needed 
fn_out = 'GHS-CHIRTS-Poly-Loop-Test.shp' 

# Isloate SHP Poly Col to merge back in later 
df_ghs = gpd.GeoDataFrame()
df_ghs['geometry'] = shps.geometry
df_ghs['ID_HDC_G0'] = shps.ID_HDC_G0
df_ghs['CTR_MN_NM'] = shps.CTR_MN_NM
df_ghs['P75'] = shps.P75
df_ghs['P90'] = shps.P90
df_ghs['P00'] = shps.P00
df_ghs['P15'] = shps.P15

# Turn polyRst data as Xarray, 
polyRst_da = xr.DataArray(polyRst.read(1), dims = ['y', 'x'])

# make a copy of the ghs polys
df_merge = df_ghs.copy()

for fn in os.listdir(CHIRT_DIR):
    # find all the tif files
    if fn.endswith('.tif'):
        
        # NEED TO BUILD META DATA CHECK INTO ROUTINE and throw an error<<<<---------

        # Get the date of each chirt file
        date = (fn.split('CHIRTSmax.')[1].split('.tif')[0])
        print(date)
        
        # Open CHIRT Data and turn data into array
        tempRst = rasterio.open(CHIRT_DIR+fn)
        
        # Make arrays into xarray DataArray
        tempRst_da = xr.DataArray(tempRst.read(1), dims = ['y', 'x']) # y and x are our 2-d labels
        
        # Make xarray dataset
        ds = xr.Dataset(data_vars = 
                    {'ghs' : (['y', 'x'], polyRst_da),
                    'temp' : (['y', 'x'], tempRst_da),})
        
        # UPDATED 2019-08-19 Mask the CHIRTS PIXELS FIRST, THEN GHS
        # Mask values from chirt that are ocean in ghs and chirt in our ds 
        ds_mask = ds.where(ds.temp != -9999, drop = False) #<<<<------ need to double check this
        
        # Mask pixels for both ghs and chirts where ghs cities are not present
        ds_mask = ds_mask.where(ds_mask.ghs > 0, drop = False)
        
        # Group poly_IDs find temp
        avg = ds_mask.groupby('ghs').mean(xr.ALL_DIMS)
        
        # turn GHS IDS and avg. CHIRTMax values into 1-D numpy arrays of equal length
        avg_ID = np.array(avg.ghs)
        avg_temp = np.array(avg.temp)
        
        print(len(avg_ID))
        print(len(avg_temp))
        
        ###### CHECK W/ 1983.01 POLYGONS have so few Avgs could be with the ds_mask! ! ! ! !
        
        # turn chirt max and IDS into a DF
        df_avg = pd.DataFrame()
        df_avg[date] = avg_temp
        df_avg['ID_HDC_G0'] = avg_ID
        
        # merge the df
        df_merge = df_merge.merge(df_avg, on='ID_HDC_G0', how = 'outer')

# Write out as a .shp file
df_merge.to_file(DATA_OUT+fn_out)