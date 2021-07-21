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
import matplotlib.pyplot as plt
import time
import multiprocessing as mp 
from multiprocessing import Pool
import multiprocessing
import ClimFuncs

#### Functions
def make_hi(zipped, himax_path_out):
    
    """ Takes an RH and Tmax file zipped together and writes a heat index tif
    Args:
        zipped = zipped RH [0] and tmax [1] file path/name
         himax_path_out = path/to/himax_out 
    """
    
    # get data date
    date =zipped[0].split('RH.')[1].split('.tif')[0]
    
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
    
    # write it out
#     fn_out = os.path.join(himax_path_out,data_out+'.'+date+'.csv')
#     print(type(arr.data))
#     np.savetxt(fn_out, arr, delimiter=",")
    
    fn_out = os.path.join(himax_path_out,data_out+'.'+date+'.tif')
    
    with rasterio.Env():
        with rasterio.open(fn_out, 'w', **meta) as out:
            out.write_band(1, arr)

def hi_loop(zipped_dir):
    
    # print process
    print(multiprocessing.current_process())
    
    rh_fns = sorted(glob.glob(zipped_dir[0]+'/*.tif')) # get RH min files
    tmax_fns = sorted(glob.glob(zipped_dir[1]+'/*.tif')) # get Tmax files
    zipped_fn_list = list(zip(rh_fns, tmax_fns)) # zipped files names 
    
    # get year
    year = zipped_dir[0].split('Tmax/')[1] 

    # make dir to write files
    himax_path = os.path.join('/scratch/cascade/UEH-daily/himax/') #/csv # path to write out HImax daily tifs
    himax_path_out = os.path.join(himax_path, year) 
    cmd = 'mkdir '+himax_path_out
    os.system(cmd)
    print(cmd, 'made')
    
    # write himax tifs in a loop
    test = zipped_fn_list[:4]
    for zipped in test:#zipped_fn_list:
        make_hi(zipped, himax_path_out)

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

# Set up file paths
rh_path = os.path.join('/home/CHIRTS/daily_ERA5/w-ERA5_Td.eq2-2-spechum-Tmax/') # RH min made with CHIRTS-daily Tmax
tmax_path = os.path.join('/home/chc-data-out/products/CHIRTSdaily/v1.0/global_tifs_p05/Tmax/') # CHIRTS-daily Tmax

# set up list of dirs for parallel loop
year_list = sorted(os.listdir(rh_path)) # years
rh_dirs = [os.path.join(rh_path, str(year)) for year in year_list] # rh years dirs
tmax_dirs = [os.path.join(tmax_path, str(year)) for year in year_list] # rh years dirs
zipped_dir_list = list(zip(rh_dirs,tmax_dirs)) # zip dirs 

zipped_dir_list = zipped_dir_list[:3]

# Run it
parallel_loop(function = hi_loop, dir_list = zipped_dir_list, cpu_num = 20)