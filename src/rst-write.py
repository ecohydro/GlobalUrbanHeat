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

def hi_loop(year):
 
    rh_path = os.path.join('/home/CHIRTS/daily_ERA5/w-ERA5_Td.eq2-2-spechum-Tmax/', str(year)) 
    out_path = os.path.join('/scratch/cascade/UEH-daily/himax/', str(year))
                            

    cmd = 'mkdir '+out_path
    os.system(cmd)
    print(cmd)
    
    rh_fns = sorted(glob.glob(rh_path+'/*.tif'))

    test = rh_fns[0:3]
    
    for i, fns in enumerate(test):

        # get meta data
        meta = rasterio.open(fns).meta
        meta['dtype'] = 'float32'
    
        # make hi
        rh = xarray.open_rasterio(fns)
    
        # get array
        arr = rh.data[0]
        
        fn_out = os.path.join(out_path,'test'+str(i)+'.tif')
        print(fn_out)
        
        with rasterio.open(fn_out, 'w', **meta) as dst:
            dst.write(arr,1)
            
#### Make years 
year_list = list(range(1983,2016+1))
year_sub = year_list[0:4]

#### Run it
parallel_loop(function = hi_loop, start_list = year_sub, cpu_num = 4)