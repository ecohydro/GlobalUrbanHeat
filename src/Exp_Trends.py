##################################################################################
#
#   By Cascade Tuholske on 2019.12.31
#   Updated 2020.02.23
#
#   Modified from 7_make_pdays.py now in oldcode dir
#
#   NOTE: Fully rewriten on 2021.02.01 see 'oldcode' for prior version / CPT
#   
#   These are the functions needed for 08_Exposure.py & 10_Trends.py
#
#################################################################################

#### Dependencies
import pandas as pd
import numpy as np
import geopandas as gpd
import statsmodels.api as sm
import warnings

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
                row.append(df[(df['ID_HDC_G0'] == city)]['P1983'].values[0]) # P 1983 should be 0
                row.append(df[(df['ID_HDC_G0'] == city)]['P2016'].values[0]) # P 2016 
                row.append(0) # people_days = 0 days
                row.append(0) # people_days_heat = 0 days
                row.append(0) # people_days_pop = 0 days

                row_list.append(row) # append row list

    df_new = pd.DataFrame(row_list, columns= df.columns) # merge the new rows into a df

    df_new = df.append(df_new) # add the rows back to the original data frame

    # Drop any city with zero people in 1983 because it will screw up the OLS, plus we don't want cities that didn't exist
    # in the record 
    df_new = df_new[df_new['P1983'] > 0]

    return df_new

def OLS(df, geog, col, alpha):
    
    """Finds linear coef for increase in stat by a given geography from 1983 - 2016, as well
    as the pct change in population of the cities within the given geography
    
    NOTE 2020.03.01 - This will throw a run time warning if all values of a col are zero (e.g. can regress
    a bunch of zeros) ... See note in run_OLS. CPT 
    
    NOTE 2020.03.01 - Later in the day this issue is resolved by removing the offending cities. See comments
    in code. CPT
    
    NOTE 2021.07.23 - Fixed RuntimeWarnings. Needed to deal with p value out puts and add warnings. 
    See addition of filterwarnings below.
    
    
    Args:
        df = HI stats dataframe
        geog = subset geography to calc people days regression
        col = col to regress on 
        alpha = ci alpha for coef
    """

    # Get results
    labels = []
    coef_list = []
    p_list = []
    df_out = pd.DataFrame()
    
    # turn warnings on
    warnings.filterwarnings("error")

    for label, df_geog in df.groupby(geog):
        #print(label)

        # Get Data
        X_year = np.array(df_geog.groupby('year')['ID_HDC_G0'].mean().index).reshape((-1, 1))
        Y_stats = np.array(df_geog.groupby('year')[col].sum()).reshape((-1, 1))

        # Add Intercept
        X_year_2 = sm.add_constant(X_year)

        # Regress
        try:
            model = sm.OLS(Y_stats, X_year_2).fit()
        except RuntimeWarning:
            break
        
        # Get slope
        # first param in intercept coef, second is slope of line but if slope = 0, then intecept
        if len(model.params) == 2:
            coef = model.params[1]
            
        else:
            coef = model.params[0]
        
        #P value - added cpt July 2021
        # deal with zero slope models
        if (model.params[0] == 0) & (model.params[1] == 0):
            p = np.nan
        else:
            p = model.pvalues[0]

        # Make lists
        labels.append(label)

        coef_list.append(coef)
        p_list.append(p)

    # Make data frame
    df_out[geog] = labels

    df_out['coef'] = coef_list
    df_out['p_value'] = [round(elem, 4) for elem in p_list]

    return df_out

def run_OLS(stats, geog, alpha):
    """ Function calculate OLS coef of people days due to pop and heat and the 
    attribution index for distribution plots.
    
        
    NOTE 2020.03.01 - This will throw a run time warning if all values of a col are zero (e.g. can regress
    a bunch of zeros, now can we). This will happen if people_days, people_days_pop, people_days_heat or 
    total_days is zero for all years for a given city. This is still OK for our analysis. What is happening is
    that for some cities, the people-days due to heat is zero, meaning pday increases in only due to population. 
    
    This is because with the GHS-UCDB some city's population in 1983 is zero, which forces the pdays due to heat
    to be zero.
    
    NOTE 2020.03.01 - Later in the day this issue is resolved by removing the offending cities. See comments
    in code.
    
    -- CPT  
    
    Args:
        stats = df to feed in
        geog = geography level to conduct analysis (city-level is 'ID-HDC-G0')
        alpha = alpha for CI coef   
    """
    # Get coef for people days
    print('pdays ols')
    out = OLS(stats, geog, 'people_days', alpha = alpha)
    out.rename(columns={"coef": "coef_pdays"}, inplace = True)
    out.rename(columns={"p_value": "p_value_pdays"}, inplace = True)
    
    # Get people days due to heat coef
    print('heat ols')
    heat = OLS(stats, geog, 'people_days_heat', alpha = alpha) # get stats 
    heat.rename(columns={"coef": "coef_heat"}, inplace = True)
    heat.rename(columns={"p_value": "p_value_heat"}, inplace = True)
    out = out.merge(heat, on = geog, how = 'left') # merge
    
    # Get people days due to pop
    # CPT July 2021 ---- this throws an error 
    print('pop ols')
    pop = OLS(stats, geog, 'people_days_pop', alpha = alpha) # get stats 
    pop.rename(columns={"coef": "coef_pop"}, inplace = True)
    pop.rename(columns={"p_value": "p_value_pop"}, inplace = True)
    out = out.merge(pop, on = geog, how = 'left') # merge
    
    # Get total days
    print('tot days ols')
    totDays = OLS(stats, geog, 'tot_days', alpha = alpha) # get stats 
    totDays.rename(columns={"coef": "coef_totDays"}, inplace = True)
    totDays.rename(columns={"p_value": "p_value_totDays"}, inplace = True)
    out = out.merge(totDays, on = geog, how = 'left') # merge
    
    # attrib coef --- creates range -1 to 1 index of heat vs. population as a driver of total pdays increase
    out['coef_attrib'] = (out['coef_pop'] - out['coef_heat']) / (out['coef_pop'] + out['coef_heat']) # normalize dif
    
#     # drop all neg or zero pday slopes (e.g. cooling cities)
#     out = out[out['coef_pdays'] >= 0]
#     out = out[out['coef_heat'] >= 0]
#     out = out[out['coef_pop'] >= 0]
    
    # normalize coef of attribution 
    norm = out['coef_attrib']
    out['coef_attrib_norm'] = (norm-min(norm))/(max(norm)-min(norm))
    
    return out