##################################################################################
#
#   By Cascade Tuholske on 2019.12.31
#   Updated 2020.02.23
#
#   Modified from 7_make_pdays.py now in oldcode dir
#
#   NOTE: Fully rewriten on 2021.02.01 see 'oldcode' for prior version / CPT
#   
#   These are the functions needed for 08_Exposure.py & 08_Trends.py
#
#################################################################################

#### Dependencies
import pandas as pd
import numpy as np

#### Functions
def tot_days(df):
    """ Calulates the total number of days per year when a heat threshold was met
    """
    df_out = df[['ID_HDC_G0','year','duration']].groupby(['ID_HDC_G0','year']).sum().reset_index()
    df_out.rename(columns={'duration':'tot_days'}, inplace=True)
    
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
    pdays['tot_days'] = data['tot_days']

    # Merge
    pdays_merge = pdays.merge(pop_long, on=['ID_HDC_G0', 'year'], how = 'left')

    # Now get people days from 1983 and change
    p = pd.DataFrame()
    p['ID_HDC_G0'] = df_pop['ID_HDC_G0']
    p['P1983'] = df_pop['P1983']
    p['P2016'] = df_pop['P2016']

    pdays_merge = pdays_merge.merge(p ,on=['ID_HDC_G0'], how = 'left')
    
    # Calc p days = total days i * pop i 
    pdays_merge['people_days'] = pdays_merge['tot_days'] * pdays_merge['P'] / scale # total people days
    
    # Pdays due to heat increase = total days total days >40.6 / yr * Pop in 1983
    pdays_merge['people_days_heat'] = pdays_merge['tot_days'] * pdays_merge['P1983'] / scale # people days w/ pop con
    
    # Pdays due to pop increase = total days i * (pop i - pop 83)
    pdays_merge['people_days_pop'] = pdays_merge['tot_days'] *(pdays_merge['P'] - pdays_merge['P1983']) / scale # dif

    return pdays_merge

def add_years(df):
    """ Function adds zero to people days for all missing years for each city 
    so that regressions aren't screwed up. New data points have NAN for P column. 
    If needed, look them up in interim/GHS-UCDB-Interp.csv"""
    
    years = list(np.unique(df['year']))
    row_list = []

    for city in list(np.unique(df['ID_HDC_G0'])):
        city_id = city # Get city Id 
        city_df = df.loc[df['ID_HDC_G0'] == city] # find the location
        city_years = list(np.unique(city_df['year'])) # figure out the number of years

        years_dif = list(set(years) - set(city_years)) # find the missing years

        if len(years_dif) > 0: # add in the missing years
            for year in years_dif: # add rows with dummy data and zeros
                row = []
                row.append(city)
                row.append(year)
                row.append(0) # tot_days = 0 days
                row.append(np.nan) # population for that year is not needed
                row.append(df[(df['ID_HDC_G0'] == city)]['P1983'].values[0])
                row.append(df[(df['ID_HDC_G0'] == city)]['P1983'].values[0])
                row.append(0) # people_days = 0 days
                row.append(0) # people_days_heat = 0 days
                row.append(0) # people_days_pop = 0 days

                row_list.append(row) # append row list

    df_new = pd.DataFrame(row_list, columns= df.columns) # merge the new rows into a df

    df_new = df.append(df_new) # add the rows back to the original data frame

    # Drop any city with zero people in 1983
    df_new = df_new[df_new['P1983'] > 0]

    return df_new