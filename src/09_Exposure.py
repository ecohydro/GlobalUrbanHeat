##################################################################################
#
#   Make pdays 
#   By Cascade Tuholske 2020.02.23
#   Updated 2020.09;07 CPT
#   Updated Feb 2021 CPT to use functions from Exp_Trends.py
#
#   Calculates exposure as population x days > treshold
#
#################################################################################

#### Dependencies
import pandas as pd
import geopandas as gpd
import numpy as np  
import Exp_Trends as expT
import os


#### Args
DATA = 'wbgtmax32' # Always update WBGT32_1D, WBGT28_1D, HI406_1D HI406_2D & HI461_1D 
DATA_PATH = os.path.join('/scratch/cascade/UEH-daily/stats/')
FN_POP = os.path.join('/home/cascade/projects/UrbanHeat/data/interim/GHS-UCDB-Interp.csv')
FN_STATS = os.path.join(DATA_PATH, DATA+'_STATS.json')
FN_OUT = os.path.join(DATA_PATH, DATA+'_EXP.json') 

scale = 1 # if you want scale the data

#### run it
if __name__ == "__main__":
    stats = pd.read_json(FN_STATS, orient = 'split') # read in stats
    df_pop = pd.read_csv(FN_POP) # read in interp population from GHS-UCDB

    print(FN_STATS)
    step1 = expT.tot_days(stats)
    print('step1',len(step1))
    step2 = expT.make_pdays(step1, df_pop, scale)
    print('step2',len(step2))
    step3 = expT.add_years(step2)
    print('step3',len(step3))

    # Save it out
    step3.to_json(FN_OUT,orient = 'split')
    print('Step 3 saved', FN_OUT)