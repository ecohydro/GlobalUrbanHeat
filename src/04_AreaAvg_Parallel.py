########################################
#
#   Program finds the Program finds the area-average daily HI or WBGT for each GHS-UCDB.
#   Requires HI or WBGT rasters and raster with polygon IDS burned (GHS-UCDB)
#   Designed to run through a file list in parallel for a dir structured by year
# 
#   By Cascade Tuholske 2019-08-20, updated July 2021
#
#
########################################
# 
#   HEY <<<<<<<<<<< -------- HEY LOOK HERE! 
#   
#   BEFORE RUNNING
#   CHECK ALL FILE 
#   NAMES AND PATHS
#
########################################

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
import multiprocessing as mp 
from glob import glob
from multiprocessing import Pool


# DATA IN/OUT ----- ALWAYS UPDATE 
DATA_IN = os.path.join('/scratch/cascade/UEH-daily/wbgtmax/')
DATA_OUT = os.path.join('/scratch/cascade/UEH-daily/GHS-wbgtmax/')# Updated path for organization 2021.02.01 CPT

# Always use keep the same 
DATA_INTERIM = os.path.join('/home/cascade/projects/UrbanHeat/data/interim/') 

# DATA: This is the data we are using, as string ---<<<<< ALWAYS UPDATE CPT 2020.03.25
DATA = 'wbgtmax.' #
FN_OUT = 'GHS-wbgtmax' #Tmax' # updated 2021.02.01 to remove ERA5 from FN ... it's CHIRTS-daily made with ERA5 

# Loop through dirs in //
def temp_ghs(dir_nm):
    
    # print current process
    print(mp.current_process())

    # Open the file with GeoPANDAS read_file
    ghs_ids_fn = 'GHS-UCDB-IDS.csv' 
    ghs_ids_df = pd.read_csv(os.path.join(DATA_INTERIM, ghs_ids_fn))

    # Open Polygon Raster
    polyRst_fn = 'GHS_UCDB_Raster_touched.tif'
    polyRst = rasterio.open(os.path.join(DATA_INTERIM, polyRst_fn))

    # Set fn out, change as needed 
    fn_out = FN_OUT
    
    # Turn polyRst data as Xarray, 
    polyRst_da = xr.DataArray(polyRst.read(1), dims = ['y', 'x'])
    
    # Start Loop
    for fn in sorted(os.listdir(dir_nm)):
        
        # Set dir name for writing files
        dir_year = dir_nm.split(DATA_IN)[1].split('/')[0]

        # find all the tif files
        if fn.startswith(DATA):        

            # Get the date of each chirt file
            date = (fn.split(DATA)[1].split('.tif')[0]) 
            print(dir_year)
            print(date)

            # Open CHIRT Data and turn data into array
            tempRst = rasterio.open(dir_nm+'/'+fn)

            # Make arrays into x array DataArray
            tempRst_da = xr.DataArray(tempRst.read(1), dims = ['y', 'x']) # y and x are our 2-d labels
            tempRst_da.data[np.isnan(tempRst_da.data)] = -9999 # CPT 2020.09.03 <<<<<<----------------- only for ERA5 RH

            # Make xarray dataset
            ds = xr.Dataset(data_vars = 
                    {'ghs' : (['y', 'x'], polyRst_da),
                    'temp' : (['y', 'x'], tempRst_da),})

            # UPDATED 2019-08-19 Mask the CHIRTS PIXELS FIRST, THEN GHS
            # Mask values from chirt that are ocean in ghs and chirt in our ds 
            ds_mask = ds.where(ds.temp != -9999, drop = False) 

            # Mask pixels for both ghs and chirts where ghs cities are not present
            ds_mask = ds_mask.where(ds_mask.ghs > 0, drop = False)

            # Group poly_IDs find area-average temp
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
            ghs_ids_df = ghs_ids_df.merge(df_avg, on='ID_HDC_G0', how = 'outer')
            
            # CPT 2020.09.04 Try writing csv as we go <<<<----- Test
            ghs_ids_df.to_csv(os.path.join(DATA_OUT+fn_out+'_'+dir_year+'.csv')) # csv out
    print('DONE ! ! !')

# start pools
def parallel_loop(function, dir_list, cpu_num):
    """Run the temp-ghs routine in parallel
    Args: 
        function = function to apply in parallel
        dir_list = list of dir to loop through 
        cpu_num = numper of cpus to fire  
    """ 
    start = time.time()
    pool = Pool(processes = cpu_num)
    pool.map(function, dir_list)
    # pool.map_async(function, dir_list)
    pool.close()

    end = time.time()
    print(end-start)

# run it 
if __name__ == "__main__":
    # Get dir list
    dir_list = sorted(glob(DATA_IN+'*/'))
    print(dir_list)

    # For ERA5 Dir 
    dir_list = dir_list[:-1]
    print(dir_list)

    # set number of cores to use
    cpu_num = 20 

    # Execute code
    print('STARTING LOOP')
    parallel_loop(temp_ghs, dir_list, 20)
    print(DATA_OUT)
    print('ENDING LOOP')
