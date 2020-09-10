##################################################################################
#
#   Z scores
#   By Cascade Tuholske 2020.09.07
#
#   Scripts find the de-trended z-scores of the increase in the days per year 
#   where HI > 40.6C ... basically how how was a city in year x, compared to the 
#   projected increase in warming 
#
#   Came from Figure 2 Final notebook
#   Actual figure 2 will be rendered in QGIS  
#
#################################################################################

#### Depdencies 
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import statsmodels.api as sm
import seaborn as sns

#### Load Data
# file path
DATA_IN = "/home/cascade/projects/UrbanHeat/data/"  # Note: Need ?dl=1 to make sure this file gets read correctly
FIG_OUT = "/home/cascade/projects/UrbanHeat/figures/"

# HI DATA
FN_IN = 'processed/AllDATA-GHS-ERA5-HI406-PDAYS.csv'
HI_STATS = pd.read_csv(DATA_IN+FN_IN)

# File out
FN_OUT = 'processed/AllDATA-GHS-ERA5-HI406-ZSCORES.csv'

# scale the date in the plot 
scale = 10**9 

# Drop IDs where days 1983 = 1 with none else. throws error in regressions 
print(len(HI_STATS))
only83 = HI_STATS.groupby('ID_HDC_G0')['total_days'].sum() == 1 # sum up total days and find those with 1 day
only83 = list(only83[only83 == True].index) # make a list of IDs
sub = HI_STATS[HI_STATS['ID_HDC_G0'].isin(only83)] # subset those IDs
bad_ids = sub[(sub['year'] == 1983) & (sub['total_days'] == 1)] # drop those from 1983 only
drop_list = list(bad_ids['ID_HDC_G0']) # make a list
HI_STATS= HI_STATS[~HI_STATS['ID_HDC_G0'].isin(drop_list)] # drop those from the list
print(len(HI_STATS))

#### Add In Meta Data
geog = ['region', 'intermediate-region', 'sub-region','CTR_MN_NM', 'ID_HDC_G0', 'GCPNT_LAT', 'GCPNT_LON']
meta_fn = 'processed/AllDATA-GHS-ERA5-HI406-META.csv'
all_data = pd.read_csv(DATA_IN+meta_fn)
meta = all_data[geog]
meta = meta.drop_duplicates('ID_HDC_G0')

# Merge in meta
HI_STATS = HI_STATS.merge(meta, on = 'ID_HDC_G0', how = 'left')

#### Functions 
def z_score(data):
    "mini function to make z scoares"
    mean = np.mean(data)
    sd = np.std(data)
    zscores = (data - mean) / sd
    
    return zscores

def z_residuals(data, col):
    
    """ function finds the z_scores of the residuals from a linear regession.
    Currently set up for city-leve z-scores of the total days per year
    >40.6C. Returns two lists, z-scores for each city and the years for ease
    of plotting.
    
    Args: 
        data = all data
        col = column to regress and fine z-score of residuals of, using 'total_days'
    """
    
    years_list = []
    zscores_list = []
    
    years = list(np.unique(data.sort_values('year')['year']))

    for i, city_df in enumerate(data.groupby('ID_HDC_G0')):
        total_days = list(city_df[1].sort_values('year')[col].values)
    
        # Get Data
        X_year = years
        Y_stats = total_days

        # Add Intercept
        X_year_2 = sm.add_constant(X_year)

        # Regress
        model = sm.OLS(Y_stats, X_year_2).fit()

        # residual Z scores 
        zscores = z_score(model.resid) 

        zscores_list.append(zscores)
        years_list.append(years)
        
    return years_list, zscores_list

#### Run it
data = HI_STATS
years = list(np.unique(data.sort_values('year')['year']))
cols = ['ID_HDC_G0']+years
zscores_df = pd.DataFrame(columns = cols)

# Loop to get scores
row = [] # empty list for rows
years = list(np.unique(data.sort_values('year')['year'])) # get years

for i, city_df in enumerate(data.groupby('ID_HDC_G0')):
    total_days = list(city_df[1].sort_values('year')['total_days'].values)

    # Get Data
    city_id = [city_df[0]]
    X_year = years
    Y_stats = total_days

    # Add Intercept
    X_year_2 = sm.add_constant(X_year)

    # Regress
    model = sm.OLS(Y_stats, X_year_2).fit()

    # residual Z scores 
    zscores = z_score(model.resid) 
    
    # Add to data frame
    row = city_id + list(zscores) # tag on the z-scores
    row = pd.Series(row, index = zscores_df.columns)
    
    # append the df
    zscores_df = zscores_df.append(row, ignore_index=True)
    
# Add in Meta Data
df_out = zscores_df.merge(meta, on = 'ID_HDC_G0', how = 'left')

# Save it
df_out.to_csv(DATA_IN+FN_OUT)