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
    # CHIRTS-daily tmax 
    tmax_path = os.path.join('/home/chc-data-out/products/CHIRTSdaily/v1.0/global_tifs_p05/Tmax/', str(year)) 
    out_path = os.path.join('/scratch/cascade/UEH-daily/himax/', str(year))
                            
    # make dir to write 
    cmd = 'mkdir '+out_path
    os.system(cmd)
    print(cmd)
    
    # CHIRTS-daily Tmax
    rh_fns = sorted(glob.glob(rh_path+'/*.tif'))
    tmax_fns = sorted(glob.glob(tmax_path+'/*.tif'))
    zipped_list = list(zip(rh_fns,tmax_fns))
     
    test = zipped_list[0:3]
    
    for i, fns in enumerate(test):
        # get date
        date =fns[0].split('RH.')[1].split('.tif')[0]
    
        # data type
        data_out = 'himax'
    
        # get meta data
        meta = rasterio.open(fns[0]).meta
        meta['dtype'] = 'float64'
    
        # make hi
        rh_fn = fns[0] 
        tmax_fn = fns[1]
        tmax = xarray.open_rasterio(tmax_fn)
        rh = xarray.open_rasterio(rh_fn)
        hi = ClimFuncs.heatindex(Tmax = tmax, RH = rh, unit_in = 'C', unit_out = 'C')
    
        # get array
        arr = hi.data[0]
        #print(type(arr))
        
        # FN out
        print(date)
        fn_out = os.path.join(out_path,data_out+'.'+date+'.tif')
        print(fn_out)
        
        with rasterio.open(fn_out, 'w', **meta) as out:
            out.write_band(1, arr)
            
def parallel_loop(function, start_list, cpu_num):
    """Run a routine in parallel
    Args: 
        function = function to apply in parallel
        start_list = list of args for function to loop through in parallel
        cpu_num = numper of cpus to fire  
    """ 
    start = time.time()
    pool = Pool(processes = cpu_num)
    pool.map(function, start_list)
    pool.close()

    end = time.time()
    print(end-start)

#### Make years 
year_list = list(range(1983,2016+1))
year_sub = year_list[0:4]

#### Run it
parallel_loop(function = hi_loop, start_list = year_sub, cpu_num = 4)