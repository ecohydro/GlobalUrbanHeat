##################################################################################
#
#   By Cascade Tuholske on 2019.12.31
#   Updated 2020.02.23
#
#   Modified from 4_Tmax_Stats.py in oldcode dir
#
#   NOTE: Fully rewriten on 2021.02.01 see 'oldcode' for prior version / CPT
#   
#   Find heat wave dates, duration, intensity, etc. for a given threshold
#   with either HI or WBGT data ... can be used on Tmax as well
#
#   LOOK FOR "ALWAYS CHECK" and UPDATE
#
#################################################################################

# Dependencies
import Event_Stats_Funcs as es
import os
from glob import glob

# Arges Needed 
DATA_IN = '/home/cascade/projects/UrbanHeat/data/interim/CHIRTS_DAILY/HI/' # <---- ALWAYS CHECK
DATA_OUT = '/home/cascade/projects/UrbanHeat/data/interim/CHIRTS_DAILY/STATS/'
dir_path = DATA_IN 
space_dim = 'ID_HDC_G0'
time_dim = 'date'
Tthresh = 46.1 # <---------- ALWAYS CHECK
data = 'HI461' #'HI406' # HI or 32 & 28 WBGT and Threshold <---------- ALWAYS CHECK
cpu = 20 # number of cpus to use
fn_out = '/home/cascade/projects/UrbanHeat/data/processed/PNAS-DATA-v2/'+data+'_1D_STATS.json'# final FN - 2-day stats

# Step 1 - Read and stack HI or WBGT
####################################################################################################
print('STEP1')
step1 = es.read_data(dir_path, space_dim = space_dim, time_dim = time_dim)
print('Data stacked')

# Step 2 Mask data based on  threshold
####################################################################################################
print('STEP2')
step2 = es.max_days(step1, Tthresh)
print('Tmax masked')

# Step 3 Split up step 2 and write the files out
####################################################################################################
print('STEP3')
es.df_split(DATA_OUT, data, cpu, df_in = step2)

# Step 4 Split events in parallel
###################################################################
print('STEP 4')
fns_list = glob(DATA_OUT+data+'_tmp'+'/*.json')
es.parallel_loop(es.max_stats_run, fns_list, cpu_num = 20)

# Step 5 Final Df
###################################################################
print('STEP 5')
fn_pattern = (DATA_OUT+data+'_tmp'+'/*STAT*')
es.final_df(fn_pattern, fn_out)

print('DONE - STATS MADE!')
