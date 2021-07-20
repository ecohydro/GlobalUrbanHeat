#### Dependencies
#import numpy as np
#import pandas as pd
import xarray 
import os
import glob
import rasterio
import matplotlib.pyplot as plt
import time
import multiprocessing as mp 
from multiprocessing import Pool
import multiprocessing
import pandas as pd
import numpy as np

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

def write_rst(csv_dir):
    
    print(multiprocessing.current_process())
    fn_list = glob.glob(csv_dir+'/*.csv')
    print(fn_list)
    year = csv_dir.split(csv_path)[1]
    print(year)
    
    meta = rasterio.open('/home/CHIRTS/daily_ERA5/w-ERA5_Td.eq2-2-spechum-Tmax/1983/RH.1983.01.01.tif').meta
    meta['dtype'] = 'float64'
    
    # make dir to write files 
    himax_path_out = os.path.join(tif_path, year) 
    cmd = 'mkdir '+himax_path_out
    os.system(cmd)
    print(cmd, 'made')
    
    
    for i, fn in enumerate(fn_list):
        arr = pd.read_csv(fn).to_numpy()
        
        fn_out = os.path.join(tif_path, str(year), 'test'+str(i)+'.tif')
        print(fn_out)
        
        with rasterio.open(fn_out, 'w', **meta) as out:
            out.write_band(1, arr)

# Set up file paths
csv_path = os.path.join('/scratch/cascade/UEH-daily/himax/csv/') # RH min made with CHIRTS-daily Tmax
year_list = sorted(os.listdir(csv_path)) # years
csv_dirs = [os.path.join(csv_path, str(year)) for year in year_list] # rh years dirs
tif_path = os.path.join('/scratch/cascade/UEH-daily/himax/tif/')

parallel_loop(write_rst, csv_dirs, cpu_num = 4)
        
# rh_path = os.path.join('/home/CHIRTS/daily_ERA5/w-ERA5_Td.eq2-2-spechum-Tmax/1983/')
# rst_list = glob.glob(rh_path+'*.tif')[:10]

# parallel_loop(write_rst, rst_list, cpu_num = 4)