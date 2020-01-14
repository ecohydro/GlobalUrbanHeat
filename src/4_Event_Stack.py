
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
        year = fn.split('CHIRTS-GHS-Raw-Events-Stats')[1].split('.csv')[0] # for some reason it's 2...?
        print(year)
        
        # open csv 
        stats = pd.read_csv(fn)
        
        stats['year'] = year
#         print('stats is ', stats.shape)
        
        print(len(df_out))
        
        df_out = df_out.append(stats)
    
    return df_out

# File Paths + FN
DATA_INTERIM = '/home/cascade/projects/UrbanHeat/data/interim/' # interim data
DATA_PROCESSED = '/home/cascade/projects/UrbanHeat/data/processed/'
DATA_IN = DATA_INTERIM+'CHIRTS-GHS-RAW-Events-Stats/' # output from avg temp

fn_out = DATA_PROCESSED+'All_data_Raw406.csv' #<<< UPDATE

# Run script
event_stack = event_stack_loop(DATA_IN)
# print(len(event_stack))

# Add 'Event_ID'
event_stack = event_stack.rename(columns = {'Unnamed: 0' : 'Event_ID'})

event_ids = range(1, len(event_stack)+1)
len(event_ids)

event_stack['Event_ID'] = event_ids

event_stack.to_csv(fn_out)
