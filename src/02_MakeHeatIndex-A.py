##################################################################################
#
#       Make Heat Index
#       By Cascade Tuholske Spring 2021
#
#       Program makes daily maximum heat index tifs with CHIRTS-daily Tmax and 
#       relative humidity min estimated with CHIRTS-daily Tmax
#
#       NOAA Heat Index Equation - https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml
#
#       Note - right now units are all in C. They can be updated to F or C as needed. 
#              See make_hi function to make changes CPT July 2021
#
#################################################################################

#### Dependencies
import numpy as np
import pandas as pd
import xarray 
import os
import glob
import rasterio
import time
import multiprocessing as mp 
from multiprocessing import Pool
import multiprocessing
import ClimFuncs

#### Functions
def hi_loop(year):
    
    """
    """
    print(multiprocessing.current_process(), year)
    
    # Set up file paths
    # RH min made with CHIRTS-daily Tmax
    rh_path = os.path.join('/home/CHIRTS/daily_ERA5/w-ERA5_Td.eq2-2-spechum-Tmax/', str(year)) 
    tmax_path = os.path.join('/home/chc-data-out/products/CHIRTSdaily/v1.0/global_tifs_p05/Tmax/', str(year)) 
    
    # CHIRTS-daily Tmax
    rh_fns = sorted(glob.glob(rh_path+'/*.tif'))
    tmax_fns = sorted(glob.glob(tmax_path+'/*.tif'))
    zipped_list = list(zip(rh_fns,tmax_fns))
     
    test = zipped_list[0:3]
    
    for fns in test:
        # get date
        date =fns[0].split('RH.')[1].split('.tif')[0]
    
        # data type
        data_out = 'himax'
    
        # get meta data
        meta = rasterio.open(zipped[0]).meta
        meta['dtype'] = 'float64'
    
        # make hi
        rh_fn = zipped[0] 
        tmax_fn = zipped[1]
        tmax = xarray.open_rasterio(tmax_fn)
        rh = xarray.open_rasterio(rh_fn)
        hi = ClimFuncs.heatindex(Tmax = tmax, RH = rh, unit_in = 'C', unit_out = 'C')
    
        # get array
        arr = hi[0]
        
        fn_out = '/scratch/cascade/UEH-daily/himax/' + str(year) + '/test' + str(i) + '.tif'
        
        with rasterio.open(fn_out, 'w', **meta) as out:
            out.write_band(1, arr)
            
def parallel_loop(function, dir_list, cpu_num):
    """Run a routine in parallel
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

# Make years 
year_list = list(range(1983,2016+1))
year_sub = year_list[0:4]

# Run it
parallel_loop(function = hi_loop, start_list = year_sub, cpu_num = 4)