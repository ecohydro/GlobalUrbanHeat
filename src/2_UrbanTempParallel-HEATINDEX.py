# Program finds the areal averaged TMax from CHIRTSMax Data and a raster with polygon IDS burned
# Designed to run through a file list in parallel for a dir structured by year
# 
# By Cascade Tuholske 2019-08-20
#
# UPDATED ON 2020.01.14 to run on CHIRTS Heat INDEX FILES 

########################################
# 
#   HEY <<<<<<<<<<< -------- HEY LOOK HERE! 
#   BEFORE RUNNING
#   CHECK ALL FILE 
#   NAMES AND PATHS
#
########################################

########################################
# 
#   HEY <<<<<<<<<<< -------- HEY LOOK HERE! 
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

# LOCAL Test
# DATA_IN = '/Users/cascade/Github/UrbanHeat/data/test_in/' # <<--- path to loop through
# DATA_OUT = '/Users/cascade/Github/UrbanHeat/data/test_out/'
# DATA_INTERIM = '/Users/cascade/Github/UrbanHeat/data/interim/'

# TANA FIRE
# DATA_IN = '/home/cascade/tana-spin-cascade/projects/UrbanTempData/CHTSMax_Monthly/' # <<--- path to loop through
# DATA_OUT = '/home/cascade/tana-crunch-cascade/projects/UrbanHeat/data/processed/'
# DATA_INTERIM = '/home/cascade/tana-crunch-cascade/projects/UrbanHeat/data/interim/'

# TONG TEST
# DATA_IN = '/home/cascade/projects/UrbanHeat/data/test_in/' # <<--- path to loop through
# DATA_OUT = '/home/cascade/projects/UrbanHeat/data/test_out/'
# DATA_INTERIM = '/home/cascade/projects/UrbanHeat/data/interim/'

# # TONG FIRE
DATA_IN = '/home/CHIRTS/daily/' # <<--- path to loop through
DATA_OUT = '/home/cascade/projects/UrbanHeat/data/interim/CHIRTS-GHS-DAILY-HI/'
DATA_INTERIM = '/home/cascade/projects/UrbanHeat/data/interim/'

# Loop through dirs in //
def temp_ghs(dir_nm):
    
    # log
    log_df = pd.DataFrame()
    log_list = []
    
    # print current process
    print(mp.current_process())

    # Open the file with GeoPANDAS read_file
    ghs_ids_fn = 'GHS-UCSB-IDS.csv'
    ghs_ids_df = pd.read_csv(DATA_INTERIM+ghs_ids_fn)

    # Open Polygon Raster
    polyRst_fn = 'GHS_UCDB_Raster_touched.tif'
    polyRst = rasterio.open(DATA_INTERIM+polyRst_fn)

    # Set fn out, change as needed 
    fn_out = 'GHS-Tmax-DAILY-HEATINDEX'     #<<<------------    ALWAYS UPDATE
    
    # Turn polyRst data as Xarray, 
    polyRst_da = xr.DataArray(polyRst.read(1), dims = ['y', 'x'])
    
    # Start Loop
    for fn in sorted(os.listdir(dir_nm)):
        
        # Set dir name for writing files
        dir_year = dir_nm.split(DATA_IN)[1].split('/')[0]

        # find all the tif files <<<<-------------------------------- UPDATED 2020.01.15 it should work now!
        if fn.startswith('HeatIndex') and 'dexmap' not in fn:
            
            log_list.append(fn)

            # Get the date of each chirt file
            date = (fn.split('HeatIndex.')[1].split('.tif')[0]) # <<<< ------ ALWAYS UPDATE
            print(dir_year)
            print(date)

            # Open CHIRT Data and turn data into array
            tempRst = rasterio.open(dir_nm+'/'+fn)

            # Make arrays into x    array DataArray
            tempRst_da = xr.DataArray(tempRst.read(1), dims = ['y', 'x']) # y and x are our 2-d labels

            # Make xarray dataset
            ds = xr.Dataset(data_vars = 
                    {'ghs' : (['y', 'x'], polyRst_da),
                    'temp' : (['y', 'x'], tempRst_da),})

            # UPDATED 2020.01.15 for HI 
            ds_mask = ds.fillna(-9999) # turn all NANS into -9999, but check HI rasters if ocean are NAN or -9999
            
            # turn all city ID pixels to NAN where no temp value exists (e.g. ocean areas)
            ds_mask['ghs'] = ds_mask.ghs.where(ds_mask.temp != -9999, drop = False) 
            
            # Mask all -9999 GHS pixels for city IDS (e.g. no city exists)
            ds_mask = ds_mask.where(ds_mask.ghs > 0, drop = False) 
            
            avg = ds_mask.groupby('ghs').mean(xr.ALL_DIMS) # Find Averages

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
            ghs_ids_df = ghs_ids_df.merge(df_avg, on='ID_HDC_G0', how = 'outer') #<<<<----- NEED TO FIX THIS
    
    # write out log
    log_df['log'] = log_list
    log_df.to_csv('/home/cascade/projects/Log'+dir_year+'.csv')
    
    ghs_ids_df.to_csv(DATA_OUT+fn_out+'_'+dir_year+'.csv') # csv out
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

# Get dir list
dir_list= sorted(glob(DATA_IN+'*/'))

dir_list = dir_list[:-1]

# # set number of cores to use
cpu_num = 20 

# # Execute code
print('STARTING LOOP - OH JOY!!!')
parallel_loop(temp_ghs, dir_list, 20)
print('ENDING LOOP')