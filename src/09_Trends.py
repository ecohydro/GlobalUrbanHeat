##################################################################################
#
#   Trends
#   By Cascade Tuholske 2020.09.07
#
#   Updated Feb 2021 by CPT
#   Functions have now been moved to Exp_Trends.py for clarity
#
#   Actual map figures are rendered in QGIS  
#
#################################################################################

#### Depdencies 
import pandas as pd
import numpy as np
import geopandas as gpd
import statsmodels.api as sm
import Exp_Trends as expT
import os

#### PATH & FN 
DATA_PATH = '' # ALWAYS CHECK
DATA = 'himax406_2d' # <---- AlWAYS UPDATA (str for data out) ------------- wbgtmax ... himax ... 
FN_IN = os.path.join(DATA_PATH,DATA+'_EXP.json') # <---- ALWAYS CHECK

if __name__ == "__main__":
    
    # read file
    HI_STATS = pd.read_json(FN_IN, orient = 'split')

    # Drop cities with only one Tmax Day in 1983 and none else because you cannot regress them
    print(len(HI_STATS))
    only83 = HI_STATS.groupby('ID_HDC_G0')['tot_days'].sum() == 1 # sum up total days and find those with 1 day
    only83 = list(only83[only83 == True].index) # make a list of IDs
    sub = HI_STATS[HI_STATS['ID_HDC_G0'].isin(only83)] # subset those IDs
    bad_ids = sub[(sub['year'] == 1983) & (sub['tot_days'] == 1)] # drop those from 1983 only
    drop_list = list(bad_ids['ID_HDC_G0']) # make a list
    HI_STATS = HI_STATS[~HI_STATS['ID_HDC_G0'].isin(drop_list)] # drop those from the list
    print(len(HI_STATS))

    #### Run OLS Reg
    print('run_OLS started')
    stats_out = expT.run_OLS(HI_STATS, 'ID_HDC_G0', alpha = 0.05)

    #### Add In Meta Data (e.g. geographic data)
    print('merge')
    meta_fn = os.path.join('','interim/GHS-UCDB-IDS.csv')
    meta_data = pd.read_csv(meta_fn)

    #### Merge in meta
    stats_out_final = stats_out.merge(meta_data, on = 'ID_HDC_G0', how = 'left')

    #### Add In Population
    pop = HI_STATS[['P1983', 'P2016', 'ID_HDC_G0']]
    pop = pop.drop_duplicates('ID_HDC_G0')
    stats_out_final = stats_out_final.merge(pop, on = 'ID_HDC_G0', how = 'left')

    #### Write it out 
    print('writing data')
    
    ## All data
    fn_out = os.path.join(DATA_PATH, DATA+'_TREND_ALL.json')
    stats_out_final.to_json(fn_out, orient = 'split')
    print('Num all', len(stats_out_final))

    ## City-level where pdays is sig at < 0.05
    # drop all neg slope cities
    neg = stats_out_final[stats_out_final['coef_pdays'] <= 0]
    print('Num neg cities', len(neg))
    
    ## City-level where pdays is sig at < 0.05  & pdays > 0
    p95 = stats_out_final[(stats_out_final['p_value_pdays'] < 0.05) & (stats_out_final['coef_pdays'] > 0)]
    fn_out = os.path.join(DATA_PATH, DATA+'_TREND_PDAYS05.json')
    p95.to_json(fn_out, orient = 'split')
    print('Num pdays 0.05', len(p95))

    ## City-level where total days is sig at < 0.05
    p95 = stats_out_final[(stats_out_final['p_value_totDays'] < 0.05) & (stats_out_final['coef_pdays'] > 0)]
    fn_out = os.path.join(DATA_PATH, DATA+'_TREND_HEATP05.json')
    p95.to_json(fn_out, orient = 'split')
    print('Num heat 0.05', len(p95))