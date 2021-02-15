##################################################################################
#
#   HI to WBGT
#
#   By Cascade Tuholske, 2021.02.15
#   Convert HI values to WBGT using the equation from:
#   Bernard, T. E., & Iheanacho, I. (2015). Heat index and adjusted temperature 
#   as surrogates for wet bulb globe temperature to screen for occupational heat stress. 
#   Journal of occupational and environmental hygiene, 12(5), 323-333.
#
#################################################################################

# Dependencies
import pandas as pd
import numpy as np
from glob import glob

def c_to_f(C):
    "Convert HI C to F"
    return 1.8 * C + 32 

def hi_to_wbgt(HI):
    """ Convert HI to WBGT using emprical relationship from Bernard and Iheanacho 2015
    WBGT [◦C] = −0.0034 HI2 + 0.96 HI−34; for HI [◦F]
    """
    
    WBGT = -0.0034*HI**2 + 0.96*HI - 34
    
    return WBGT

def loop(fns_list):
    
    for fn in fns_list:
        
        # Get year
        print(fn)
        year = fn.split('GHS-HI_')[1].split('.csv')[0]
        fn_out = DATA_PATH+'interim/CHIRTS_DAILY/WBGT/GHS-WBGT_'+year+'.csv'
        
        # read in and convert to WBGT
        df = pd.read_csv(fn_in)
        hi = df.iloc[:,3:]
        hi_f = hi.apply(c_to_f)
        wbgt = hi_f.apply(hi_to_wbgt)
        
        # cols out
        df_out = df.iloc[:,1:3]
        
        # write it out 
        df_out = pd.concat([df_out,wbgt], axis = 1)
        df_out.to_csv(fn_out)
        print('done', year, '\n')
        
# Files & Run
DATA_PATH = '/home/cascade/projects/UrbanHeat/data/'
fns_list = sorted(glob(DATA_PATH+'interim/CHIRTS_DAILY/HI/*csv'))
loop(fns_list)