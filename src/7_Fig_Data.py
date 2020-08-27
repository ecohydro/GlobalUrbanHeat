##################################################################################
#
#   Figure Data 
#   By Cascade Tuholske 2020.02.23
#
#   This will make the data needed for Figures 1 and 2 (maybe three ??).
#
#################################################################################

#### Dependencies
import pandas as pd
import geopandas as gpd
import numpy as np  

#### Args
DATA_IN = "/home/cascade/projects/UrbanHeat/data/" 
FN_STATS = 'processed/AllDATA-GHS-ERA5-HI406-META.csv'
FN_POP = 'interim/GHS-UCDB-Interp.csv'
FN_OUT = DATA_IN+'processed/AllDATA-GHS-ERA5-HI406-FIGDATA.csv'

#### Re rane on 2020.03.06 with scale set to 1
scale = 1
#scale = 10**9 # divide total people days for each city-year pair

#### Functions

# def group_stats(df):
#     "function groups event stats to calc total Tmax days per year for each city"
    
#     total_days = df.groupby(['year', 'ID_HDC_G0'])['duration'].sum() # group by city and year
#     total_days_df = pd.DataFrame(total_days).reset_index() # reset the index
#     total_days_df.rename({'duration': 'total_days'}, axis=1, inplace=True) # rename column 

#     return total_days_df


def df_drop(df, cols):
    "function takes data frame and drops duplicates based on a column, takes list of cols"
    df_out = pd.DataFrame()
    df_out = df.drop_duplicates(cols)
    
    return df_out
    
def make_pdays(df_stats, df_pop, scale):
    
    """  Makes a dataframe with Tmax stats and population to calc people days.
    
    Args: 
        df_stats = Tmax stats output
        df_pop = interpolated GHS-UCDB population
        scale = if you want to divide the data ... 10**9 is best for global scale
    """
    
    # Make Population Long Format
    pop_long = pd.wide_to_long(df_pop, stubnames = 'P', i = 'ID_HDC_G0', j = 'year')
    pop_long.reset_index(level=0, inplace=True)
    pop_long.reset_index(level=0, inplace=True)
    pop_long = pop_long.drop('Unnamed: 0', axis = 1)
    
    # Get Total Days 
    data = df_stats
    pdays = pd.DataFrame()
    pdays['ID_HDC_G0'] = data['ID_HDC_G0']
    pdays['year'] = data['year']
    pdays['total_days'] = data['total_days']

    # Merge
    pdays_merge = pdays.merge(pop_long, on=['ID_HDC_G0', 'year'], how = 'left')

    # Now get people days from 1983 and change
    p = pd.DataFrame()
    p['ID_HDC_G0'] = df_pop['ID_HDC_G0']
    p['P1983'] = df_pop['P1983']
    p['P2016'] = df_pop['P2016']

    pdays_merge = pdays_merge.merge(p ,on=['ID_HDC_G0'], how = 'left')
    
    # Calc p days = total days i * pop i 
    pdays_merge['people_days'] = pdays_merge['total_days'] * pdays_merge['P'] / scale # total people days
    
    # Pdays due to heat increase = total days total days >40.6 / yr * Pop in 1983
    pdays_merge['people_days_heat'] = pdays_merge['total_days'] * pdays_merge['P1983'] / scale # people days w/ pop con
    
    # Pdays due to pop increase = total days i * (pop i - pop 83)
    pdays_merge['people_days_pop'] = pdays_merge['total_days'] *(pdays_merge['P'] - pdays_merge['P1983']) / scale # dif

    return pdays_merge

def add_years(df):
    """ Function adds zero to people days for all missing years for each city 
    so that regressions aren't screwed up"""
    
    years = list(np.unique(df['year'])) # Get list of all years
    row_list = []
    counter = 0
    
    for city in list(np.unique(df['ID_HDC_G0'])):
        city_id = city # Get city Id 
        city_df = df.loc[df['ID_HDC_G0'] == city] # find the location
        city_years = list(np.unique(city_df['year'])) # figure out the number of years
        
        years_dif = list(set(years) - set(city_years)) # find the missing years
        
        #print(len(years_dif))
        if len(years_dif) > 0: # add in the missing years
            
            counter = counter + len(years_dif) # counter
            
            for year in years_dif: # add rows with dummy data and zeros
                row = []
                row.append(city) # city id
                row.append(year) # missing year
                row.append(0) # total days
                row.append(float(df_pop[(df_pop['ID_HDC_G0'] == city)]['P'+str(year)])) # pop year
                row.append(float(df_pop[(df_pop['ID_HDC_G0'] == city)]['P'+str(1983)])) # pop 83
                row.append(float(df_pop[(df_pop['ID_HDC_G0'] == city)]['P'+str(2016)])) # pop 16
                row.append(0) # days
                row.append(0) # pdays 83
                row.append(0) # pdays diff
                
                row_list.append(row)
    
    df_new = pd.DataFrame(row_list, columns= df.columns) # merge the new rows into a df
    
    df_new = df.append(df_new) # add the rows back to the original data frame

    return df_new

#### run it 
stats = pd.read_csv(DATA_IN+FN_STATS) # read in stats
df_pop = pd.read_csv(DATA_IN+FN_POP) # read in interp population from GHS-UCDB

#### CPT 2020.30.30 I don't think step 1 in needed with the new work flow
cols = ('ID_HDC_G0', 'year') # drop duplicate city & year combos for pdays
step1 = df_drop(stats, cols)
step2 = make_pdays(step1, df_pop, scale)
step3 = add_years(step2)

# step1 = group_stats(stats) 
# step2 = make_pdays(step1, df_pop, scale)
# step3 = add_years(step2)

# Save it out
print('starting step 7')
step3.to_csv(FN_OUT)
print('end step 7')




