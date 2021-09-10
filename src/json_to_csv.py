##################################################################################
#
#   Trends
#   By Cascade Tuholske July 2021
#
#   Write json outputs to .csv
#
#################################################################################

# Depedencies
import pandas as pd
import os 
import glob

if __name__=="__main__":
    
    # json files
    data_path = os.path.join('') # file path
    fns = glob.glob(data_path+'*.json')
    
    # write to csvs
    for fn in fns:
        fn_tail = fn.split(data_path)[1].split('.json')[0] + '.csv' # change to csv
        df = pd.read_json(fn, orient = 'split') # open it
        df.to_csv(os.path.join(data_path,fn_tail))
        print(fn_tail)
    print('done!')