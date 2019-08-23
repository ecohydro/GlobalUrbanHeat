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
import xarray as xr
import fnmatch
import time

start = time.time()
# from ftplib import FTP
# import re

# Directories 

# LOCAL
# CHIRT_DIR = '/Users/cascade/Github/PopRaster/data/raw/CHIRT/' # <<--- path to loop through
# SHP_DIR = '/Users/cascade/Github/PopRaster/data/raw/JRC/ghs-ucdb/'
# POLY_RST_DIR = '/Users/cascade/Github/PopRaster/data/interim/'
# DATA_OUT = '/Users/cascade/Github/PopRaster/data/interim/'

# LOCAL TEST 
CHIRT_DIR = '/Users/cascade/Github/UrbanHeat/data/test_data/' # <<--- path to loop through
SHP_DIR = '/Users/cascade/Github/PopRaster/data/raw/JRC/ghs-ucdb/'
POLY_RST_DIR = '/Users/cascade/Github/PopRaster/data/interim/'
DATA_OUT = '/Users/cascade/Github/UrbanHeat/data/test_data/test_out/'

# TANA
# CHIRT_DIR = '/home/cascade/tana-spin-cascade/projects/CHIRTMax_Monthly/' # <<--- path to loop through
# SHP_DIR = '/home/cascade/tana-crunch-cascade/projects/UrbanHeat/Data/raw/GHS_UCDB/'
# POLY_RST_DIR = '/home/cascade/tana-crunch-cascade/projects/UrbanHeat/Data/interim/'
# DATA_OUT = '/home/cascade/tana-crunch-cascade/projects/UrbanHeat/Data/processed/'

# Open Polygon Raster
polyRst_fn = 'GHS_UCDB_Raster_Raster_touched.tif'
polyRst = rasterio.open(POLY_RST_DIR+polyRst_fn)

# Open the file with GeoPANDAS read_file
shp_fn = 'GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_0.shp'
shps = gpd.read_file(SHP_DIR+shp_fn)

# Set fn out, change as needed 
fn_out = 'GHS-CHIRT-MONTHLY'  

# Isloate SHP Poly Col to merge back in later 
df_ghs = gpd.GeoDataFrame()

df_ghs['ID_HDC_G0'] = shps.ID_HDC_G0
df_ghs['CTR_MN_NM'] = shps.CTR_MN_NM

# Can map back to shape files later 

# df_ghs['geometry'] = shps.geometry
# df_ghs['P75'] = shps.P75
# df_ghs['P90'] = shps.P90
# df_ghs['P00'] = shps.P00
# df_ghs['P15'] = shps.P15

# Turn polyRst data as Xarray, 
polyRst_da = xr.DataArray(polyRst.read(1), dims = ['y', 'x'])

# make a count
# count = 0

# Loop through dirs 
for dirpath, dirnames, files in os.walk(CHIRT_DIR):
        # Set dir name for writing files
        dir_year = dirpath.split(CHIRT_DIR)[1]

        # make a copy of the ghs polys, reset for each dir
        df_merge = df_ghs.copy()
        
        for fn in files:

                # find all the tif files
                if fn.endswith('.tif'):
                
                        # NEED TO BUILD META DATA CHECK INTO ROUTINE and throw an error<<<<---------

                        # Get the date of each chirt file
                        date = (fn.split('CHIRTSmax.')[1].split('.tif')[0])
                        print(dir_year)
                        print(date)
                        
                        # Open CHIRT Data and turn data into array
                        tempRst = rasterio.open(dirpath+'/'+fn)
                        
                        # Make arrays into x    array DataArray
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
                                
                        # turn chirt max and IDS into a DF
                        df_avg = pd.DataFrame()
                        df_avg[date] = avg_temp
                        df_avg['ID_HDC_G0'] = avg_ID
                        
                        # merge the df
                        df_merge = df_merge.merge(df_avg, on='ID_HDC_G0', how = 'outer')

                        # add to count and write out
                        # count = count +1
                        # print(count)
                        # count = 0
                        #if count == 3: #<<<<<<< ------ SET COUNT

        # write files out for each dir        
        # df_merge.to_file(DATA_OUT+fn_out+'_'+dir_year+'.shp') # shp out
        df_merge.to_csv(DATA_OUT+fn_out+'_'+dir_year+'.csv') # csv out




# Write out as a .shp file
# df_merge.to_file(DATA_OUT+shp_fn_out)
# df_merge.to_csv(DATA_OUT+csv_fn_out)

print('DONE ! ! !')
end = time.time()
print(end - start)