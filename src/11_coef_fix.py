##################################################################################
#
#   coef fix
#   By Cascade Tuholske July 2021
#
#   Normalized coef_attrib was not working and I can not figure out the error.
#   So I am trying to do it separately.
#
#################################################################################

import pandas as pd
import os

if __name__ == "__main__":
    
    # open fn
    fn = os.path.join('/scratch/cascade/UEH-daily/stats/wbgtmax30_TREND_PDAYS05.json')
    df = pd.read_json(fn, orient = 'split')
    
    # normalized coef_attrib & save out
    norm = df['coef_attrib']
    print(min(norm))
    df['coef_attrib_norm'] = (norm-min(norm))/(max(norm)-min(norm))
    df.to_json(fn, orient = 'split')