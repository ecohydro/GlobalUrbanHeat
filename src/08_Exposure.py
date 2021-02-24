##################################################################################
#
#   Make pdays 
#   By Cascade Tuholske 2020.02.23
#   Updated 2020.09;07 CPT
#   Updated Feb 2021 CPT to use functions from Exp_Trends.py
#
#   Calculates exposure as population x days > 40.6C 
#
#################################################################################

#### Dependencies
import pandas as pd
import geopandas as gpd
import numpy as np  
import Exp_Trends as expT

#### Args
DATA_PATH = "/home/cascade/projects/UrbanHeat/data/" 
FN_POP = 'interim/GHS-UCDB-Interp.csv'
FN_STATS = 'processed/PNAS-DATA-v2/HI406_2D_STATS.json' #--- check
FN_OUT = 'processed/PNAS-DATA-v2/HI406_2D_EXP.json' #--- check

scale = 1
cols = ('ID_HDC_G0', 'year') # drop duplicate city & year combos for pdays

#### run it 
stats = pd.read_json(DATA_PATH+FN_STATS, orient = 'split') # read in stats
df_pop = pd.read_csv(DATA_PATH+FN_POP) # read in interp population from GHS-UCDB

step1 = expT.df_drop(stats, cols)
step2 = expT.make_pdays(step1, df_pop, scale)
print(len(step2))
step3 = expT.add_years(step2)
print(len(step3))

# Save it out
step3.to_json(DATA_PATH+FN_OUT,orient = 'split')
print('Step 3 saved)



