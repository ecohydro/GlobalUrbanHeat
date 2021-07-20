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


#### Functions
def C_to_F(Tmax_C):
    "Function converts temp in C to F"
    Tmax_F = (Tmax_C * (9/5)) + 32
    
    return Tmax_F

def F_to_C(Tmax_F):
    "Function converts temp in F to C"
    Tmax_C = (Tmax_F - 32) * (5/9)
    
    return Tmax_C

def heatindex(Tmax, RH, unit_in, unit_out):
    
    """Make Heat Index from 2m air and relative humidity following NOAA's guidelines: 
    https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml. It is assumed that the
    tempatures and RH are geographically and temporally aligned in the x-arrays and can be stacked
    to the funciton.
    
    --- update as needed cpt 2020.02.17
    
    Args:
        Tmax = x-array of tempatures
        RH = x-array of realtive humitity
        unit_in = F or C, will convert C to F to apply heat index
        unit_out = If C is desired, will convert data to C
        
    Returns HI
    """
    
    # Make all data as float 
#     Tmax = Tmax.astype('float')
#     RH = RH.astype('float')
    
    # 1 convert C to F if needed
    if unit_in == 'C':
        Tmax = C_to_F(Tmax)
        
    # 2 Apply Steadman's and average with Tmax
    USE_STEADMAN = (0.5 * (Tmax + 61.0 + ((Tmax-68.0)*1.2) + (RH*0.094)) + Tmax) / 2 < 80
    STEADMAN = USE_STEADMAN * (0.5 * (Tmax + 61.0 + ((Tmax-68.0)*1.2) + (RH*0.094))) #.astype(int)
    
    # 3 Use Rothfusz if (STEADMAN + Tmax) / 2 > 80
    USE_ROTH = (0.5 * (Tmax + 61.0 + ((Tmax-68.0)*1.2) + (RH*0.094)) + Tmax) / 2 > 80
    ROTH = USE_ROTH * (-42.379 + 2.04901523*Tmax + 10.14333127*RH - .22475541*Tmax *RH - .00683783*Tmax*Tmax - .05481717*RH*RH + .00122874*Tmax*Tmax*RH + .00085282*Tmax*RH*RH - .00000199*Tmax*Tmax*RH*RH)

    # 3 Adjust Roth 1
    USE_ADJ1 = (RH < 13) & (Tmax > 80) & (Tmax < 112)
    ADJ1_RH = USE_ADJ1 * RH #.astype(int)
    ADJ1_RH = ADJ1_RH.where(ADJ1_RH != 0) #ADJ1_RH[ADJ1_RH == 0] = np.nan
    ADJ1_Tmax = USE_ADJ1 * Tmax # .astype(int)
    ADJ1_Tmax = ADJ1_Tmax.where(ADJ1_Tmax != 0) #ADJ1_Tmax[ADJ1_Tmax == 0] = np.nan
    ADJ1 = ((13-ADJ1_RH)/4)*np.sqrt((17-abs(ADJ1_Tmax-95.))/17)
    ADJ1 = np.nan_to_num(ADJ1, 0)
    
    ADJ1_ROTH = ROTH * USE_ADJ1
    ADJ1_ROTH = ADJ1_ROTH - ADJ1
    
    # 4 Adjust Roth 2
    USE_ADJ2 = (RH > 85) & (Tmax > 80) & (Tmax < 87)
    ADJ2_RH = USE_ADJ2 * RH #.astype(int)
    ADJ2_RH = ADJ2_RH.where(ADJ2_RH != 0) #ADJ2_RH[ADJ2_RH == 0] = np.nan
    ADJ2_Tmax = USE_ADJ2.astype(int) * Tmax
    ADJ2_Tmax = ADJ2_Tmax.where(ADJ2_Tmax != 0) #ADJ2_Tmax[ADJ2_Tmax == 0] = np.nan
    ADJ2 = ((ADJ2_RH-85)/10) * ((87-ADJ2_Tmax)/5)
    ADJ2 = np.nan_to_num(ADJ2, 0)
    
    ADJ2_ROTH = ROTH * USE_ADJ2
    ADJ2_ROTH = ADJ2_ROTH + ADJ2
    
    # Roth w/o adjustments
    ROTH = ROTH * ~USE_ADJ1 * ~USE_ADJ2
    
    # sum the stacked arrays
    HI = ROTH + STEADMAN + ADJ1_ROTH +  ADJ2_ROTH 
    
    # Convert HI to C if desired
    if unit_out == 'C':
        HI = F_to_C(HI)
    
    # return for test
    # return STEADMAN, ADJ1_ROTH, ADJ2_ROTH, ROTH, HI
    
    return HI

def make_hi(zipped, himax_path_out):
    
    """ Takes an RH and Tmax file zipped together and writes a heat index tif
    Args:
        zipped = zipped RH [0] and tmax [1] file path/name
         himax_path_out = path/to/himax_out 
    """
    
    # get data date
    date =zipped[0].split('RH.')[1].split('.tif')[0]
    
    # get meta data
    meta = rasterio.open(zipped[0]).meta
    meta['dtype'] = 'float64'
    
    # make hi
    rh_fn = zipped[0] 
    tmax_fn = zipped[1]
    tmax = xarray.open_rasterio(tmax_fn)
    rh = xarray.open_rasterio(rh_fn)
    hi = heatindex(Tmax = tmax, RH = rh, unit_in = 'C', unit_out = 'C')
    
    # get array
    arr = hi[0]
    
    # write it out
#     fn_out = os.path.join(himax_path_out,data_out+'.'+date+'.csv')
#     print(type(arr.data))
#     np.savetxt(fn_out, arr, delimiter=",")
    sharing=False
    
    fn_out = os.path.join(himax_path_out,data_out+'.'+date+'.tif')
    with rasterio.open(fn_out, 'w', **meta, sharing=False) as out:
        out.write_band(1, arr)

def hi_loop(zipped_dir):
    
    # print process
    print(multiprocessing.current_process())
    
    rh_fns = sorted(glob.glob(zipped_dir[0]+'/*.tif')) # get RH min files
    tmax_fns = sorted(glob.glob(zipped_dir[1]+'/*.tif')) # get Tmax files
    zipped_fn_list = list(zip(rh_fns, tmax_fns)) # zipped files names 
    
    year = zipped_dir[0].split('Tmax/')[1] # get year

    # make dir to write files 
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
himax_path = os.path.join('/scratch/cascade/UEH-daily/himax/') #/csv # path to write out HImax daily tifs

# set up list of dirs for parallel loop
year_list = sorted(os.listdir(rh_path)) # years
rh_dirs = [os.path.join(rh_path, str(year)) for year in year_list] # rh years dirs
tmax_dirs = [os.path.join(tmax_path, str(year)) for year in year_list] # rh years dirs
zipped_dir_list = list(zip(rh_dirs,tmax_dirs)) # zip dirs 

# global variables
data_out = 'himax'

zipped_dir_list = zipped_dir_list[:3]

# Run it
parallel_loop(function = hi_loop, dir_list = zipped_dir_list, cpu_num = 20)