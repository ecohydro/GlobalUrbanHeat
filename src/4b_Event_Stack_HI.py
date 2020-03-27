
##################################################################################
#
#   Event STack
#   
#   By Cascade Tuholske, 2019.12.31 
#
#   Stacks all Tmax stat years into one large csv
#   Pulled from cpt_density_plots.ipynb
#
# 
##################################################################################

# GHS-ERA5-Stats_
# Dependencies

import pandas as pd
import glob

# File Paths + FN
DATA_INTERIM = '/home/cascade/projects/UrbanHeat/data/interim/' # interim data
DATA_PROCESSED = '/home/cascade/projects/UrbanHeat/data/processed/'
DATA_IN = DATA_INTERIM+'ERA5_STATS/' # <<<<< UPDATE

FN_OUT = DATA_PROCESSED+'AllDATA-GHS-ERA5-HI406.csv' #<<< UPDATE
FN_IN = 'GHS-ERA5-Stats_' #<<< UPDATE

def event_stack_loop(dir_in):
    
    """ Loop through a dir with csvs of tmax events for each year and
    stack them into one data frame. Current file name is CHIRTS-GHS-Events-StatsXXXX.csv
    
    Args:
        dir_in = dir path to loop through

    """
        
    # Get File list
    fn_list = glob.glob(dir_in+'*.csv')
    
    # Data frame to fill
    df_out = pd.DataFrame()
    
    for fn in sorted(fn_list):
            
        year = fn.split(FN_IN)[1].split('.csv')[0] # for some reason it's 2...?
        print(year)
        
        # open csv 
        stats = pd.read_csv(fn)
        
        # drop index col
        stats = stats.iloc[:,1:]
        
        stats['year'] = year
        
        print(len(df_out))
        
        df_out = df_out.append(stats, sort = False)
    
    return df_out

# Run script
event_stack = event_stack_loop(DATA_IN)
print(len(event_stack))

# Add 'Event_ID'
event_ids = range(1, len(event_stack)+1)
len(event_ids)

event_stack['Event_ID'] = event_ids

print('Writing final df to disk')
event_stack.to_csv(FN_OUT)
print('ALL DONE!')
