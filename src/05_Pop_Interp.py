##################################################################################
#
#   Population Interpolation
#   By Cascade Tuholske 
#
#   Here we interpolate the GHS-UCDB population estimates with
#   a linear step interpoluation for the bench marked population 
#   already in the dataset for 1975, 1990, 2000, 2015
#
#################################################################################

#### Dependencies
import pandas as pd
import geopandas as gpd
import numpy as np 

#### Load Files and FN

ghs = gpd.read_file('/home/cascade/projects/UrbanHeat/data/raw/GHS_UCDB/GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_0.shp')
out_fn = '/home/cascade/projects/UrbanHeat/data/interim/GHS-UCDB-Interp.csv'

# Get Population from GHS
df_pop = pd.DataFrame()
df_pop['ID_HDC_G0'] = ghs['ID_HDC_G0']
df_pop[1975] = ghs['P75']
df_pop[1990] = ghs['P90']
df_pop[2000] = ghs['P00']
df_pop[2015] =  ghs['P15']

# Make an interpolation function
def df_interp(df, start_col, end_col):
    """ Runs a linear interpolution between two df columns
    Args:
        df = dataframe
        start_col = start column, start year
        end_col = end column, end year
    """
    
    new_df = pd.DataFrame() # make an empty df
    nan_list = np.full((1, len(df)), np.nan)[0] # make an empty df list

    new_df['P'+str(start_col)] = df[start_col] # snag the first column
    
    # make a bunch of nan cols
    for i in range(end_col - start_col - 1):
        i = i + 1 # fix zero indexing
        column = 'P'+str(start_col+i) 
        new_df[column] = nan_list
    
    new_df['P'+str(end_col)] = df_pop[end_col]
    
    # interp ! ! !
    new_df = new_df.interpolate('linear', axis = 1)
    
    return new_df

#### run function
df7590 = df_interp(df_pop, 1975, 1990)
df9000 = df_interp(df_pop, 1990, 2000)
df0015 = df_interp(df_pop, 2000, 2015)

## Project 2016 values
df2016 = pd.DataFrame() # make an empty df
nan_list = np.full((1, len(df_pop)), np.nan)[0] # make an empty df list
df2016['P2000'] = df_pop[2000]
df2016['P2015'] = df_pop[2015]
df2016['P2016'] = nan_list # populate 2016 w/ nan 

df2016 = df2016.interpolate('linear', axis = 1)

# join
df_join = df7590.join(df9000.iloc[:,1:])
df_join = df_join.join(df0015.iloc[:,1:])
df_join['P2016'] = df2016['P2016']
df_join['ID_HDC_G0'] = ghs['ID_HDC_G0']

# Save it out 
df_join.to_csv(out_fn)