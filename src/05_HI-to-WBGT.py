##################################################################################
#
#   HI to WBGT
#
#   By Cascade Tuholske, 2021.02.15, updated July 2021
#   
#   Convert HI values to WBGT using the equation from:
#   Bernard, T. E., & Iheanacho, I. (2015). Heat index and adjusted temperature 
#   as surrogates for wet bulb globe temperature to screen for occupational heat stress. 
#   Journal of occupational and environmental hygiene, 12(5), 323-333.
#
#################################################################################

# Dependencies
import xarray 
import os
import glob
import rasterio
import time
import multiprocessing as mp 
from multiprocessing import Pool
import multiprocessing

# Functions
def c_to_f(C):
    "Convert HI C to F"
    return 1.8 * C + 32 

def hi_to_wbgt(HI):
    """ Convert HI to WBGT using emprical relationship from Bernard and Iheanacho 2015
    WBGT [◦C] = −0.0034 HI2 + 0.96 HI−34; for HI [◦F]
    Args:
        HI = heat index as an array
    """
    
    WBGT = -0.0034*HI**2 + 0.96*HI - 34
    
    return WBGT

def wbgt_loop(year):
    
    """ Turns himax into wbgt running through a list of tifs
    Args:
        year = year of files
    """
    
    print(mp.current_process(), year)
    
    # Set up file paths
    hi_path = os.path.join('/scratch/cascade/UEH-daily/himax/', str(year)) # hi path
    out_path = os.path.join('/scratch/cascade/UEH-daily/wbgtmax/', str(year))# wbgt path
                            
    # make dir to write 
    cmd = 'mkdir '+out_path
    os.system(cmd)
    print(cmd)
    
    # Get HI
    hi_fns = sorted(glob.glob(hi_path+'/*.tif'))
    # hi_fns = hi_fns[:3] # for testing
    
    for fn in hi_fns:
        print(fn)
        
        # fn out stuff
        data = 'wbgtmax'
        date =fn.split('himax.')[1].split('.tif')[0]
        fn_out = os.path.join(out_path, data+'.'+date+'.tif') 
        meta = rasterio.open(fn).meta # meta data
        meta.update({'dtype' : 'float32'}) # to save on file space

        hi_to_wbgt
        hi_arr_c = xarray.open_rasterio(fn).data[0]
        hi_arr_f = c_to_f(hi_arr_c) # switch hi from c to f
        wbgt_arr = hi_to_wbgt(hi_arr_f) # write wbgt in c
        wbgt_arr = wbgt_arr.astype('float32')
             
        with rasterio.open(fn_out, 'w', **meta) as out:
            out.write_band(1, wbgt_arr)
        print(fn_out, 'done')

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

        
# Files & Run
if __name__ == "__main__":
    
    # Make years 
    year_list = list(range(1983,2016+1))
    # year_list = year_list[:3] # for testing
    
    # Run it
    parallel_loop(function = wbgt_loop, start_list = year_list, cpu_num = 20)