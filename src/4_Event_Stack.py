
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

# Dependencies

import pandas as pd
import glob

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
    
        # Likely need to change year to int ... df['purchase'].astype(str).astype(int) <<<<---- FIX 
        year = fn.split('CHIRTS-GHS-Events-Stats')[2].split('.csv')[0] # for some reason it's 2...?
        print(year)
        
        # open csv 
        stats = pd.read_csv(fn)
        
        stats['year'] = year
        
        print(len(df_out))
        
        df_out = df_out.append(stats)
    
    return df_out

# File Paths + FN
DATA_IN = '/home/cascade/projects/data_out_urbanheat/CHIRTS-GHS-Events-Stats/'
fn_out = '/home/cascade/projects/data_out_urbanheat/heatrange/All_data20200109_377C.csv' #<<< UPDATE

# Run script
event_stack = event_stack_loop(DATA_IN)
print(len(event_stack))

# Add 'Event_ID'
event_stack = event_stack.rename(columns = {'Unnamed: 0' : 'Event_ID'})

event_ids = range(1, len(event_stack)+1)
len(event_ids)

event_stack['Event_ID'] = event_ids

event_stack.to_csv(fn_out)
