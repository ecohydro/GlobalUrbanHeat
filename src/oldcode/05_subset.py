##################################################################################
#
#   By Cascade Tuholske Feb 2021
#
#   Subset any result from Stats_parallel to identify events duration of greater 
#   than a specific length longer.
#
#################################################################################

# Dependencies
import pandas as pd
import os

# Functions
def subset(fn_in, fn_out, event_length):
    """ Subsets a STATs dataset for a specific event length or greater
    Args: 
        fn_in = file to subset (json)
        fn_out = file to save out (json)
        event_length = minimum duration
    """
    
    # open df, subset, and save
    print(fn_in, 'in')
    df = pd.read_json(fn_in, orient = 'split')
    df_out = df[df['duration'] >= event_length]
    df_out.to_json(fn_out, orient = 'split')
    print(df_out.head(), 'done')

# Args
DATA_PATH = '/home/cascade/projects/UrbanHeat/data/'
fn_in = os.path.join(DATA_PATH,'processed/PNAS-DATA-v2/HI406_1D_STATS.json') # ALWAYS UPDATE
fn_out = os.path.join(DATA_PATH,'processed/PNAS-DATA-v2/HI406_2D_STATS.json') # ALWAYS UPDATE
event_length = 2 # great than or equalt to 

# Run it
if __name__ == "__main__":
    subset(fn_in, fn_out, event_length)