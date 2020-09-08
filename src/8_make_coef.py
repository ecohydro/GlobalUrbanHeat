##################################################################################
#
#   Figure Data 
#   By Cascade Tuholske 2020.09.07
#
#   Scripts will make OLS coef for exposure trends (p_days, population, and heat)
#
#   Came from Figure 2 Final notebook
#   Actual figure 2 will be rendered in QGIS  
#
#################################################################################

#### Depdencies 
import pandas as pd
import numpy as np
import geopandas as gpd
import statsmodels.api as sm

#### Functions

def OLS(df, geog, col, alpha):
    
    """Finds linear coef for increase in stat by a given geography from 1983 - 2016, as well
    as the pct change in population of the cities within the given geography
    
    NOTE 2020.03.01 - This will throw a run time warning if all values of a col are zero (e.g. can regress
    a bunch of zeros) ... See note in run_OLS. CPT 
    
    NOTE 2020.03.01 - Later in the day this issue is resolved by removing the offending cities. See comments
    in code. CPT
    
    
    Args:
        df = HI stats dataframe
        geog = subset geography to calc people days regression
        col = col to regress on 
        alpha = ci alpha for coef
    """

    # Get results
    labels = []
    coef_list = []
    leftci_list = []
    rightci_list = []
    p_list = []
    df_out = pd.DataFrame()

    for label, df_geog in df.groupby(geog):

        # Get Data
        X_year = np.array(df_geog.groupby('year')['ID_HDC_G0'].mean().index).reshape((-1, 1))
        Y_stats = np.array(df_geog.groupby('year')[col].sum()).reshape((-1, 1))

        # Add Intercept
        X_year_2 = sm.add_constant(X_year)

        # Regress
        model = sm.OLS(Y_stats, X_year_2).fit() 
        
        # Get slope
        # first param in intercept coef, second is slope of line but if slope = 0, then intecept
        if len(model.params) == 2:
            coef = model.params[1]
            
        else:
            coef = model.params[0]
        
        # P value
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
    out = OLS(stats, geog, 'people_days', alpha = alpha)
    out.rename(columns={"coef": "coef_pdays"}, inplace = True)
    out.rename(columns={"p_value": "p_value_pdays"}, inplace = True)
    
    # Get people days due to heat coef
    heat = OLS(stats, geog, 'people_days_heat', alpha = alpha) # get stats 
    heat.rename(columns={"coef": "coef_heat"}, inplace = True)
    heat.rename(columns={"p_value": "p_value_heat"}, inplace = True)
    out = out.merge(heat, on = geog, how = 'left') # merge
    
    # Get people days due to pop
    pop = OLS(stats, geog, 'people_days_pop', alpha = alpha) # get stats 
    pop.rename(columns={"coef": "coef_pop"}, inplace = True)
    pop.rename(columns={"p_value": "p_value_pop"}, inplace = True)
    out = out.merge(pop, on = geog, how = 'left') # merge
    
    # Get total days
    totDays = OLS(stats, geog, 'total_days', alpha = alpha) # get stats 
    totDays.rename(columns={"coef": "coef_totDays"}, inplace = True)
    totDays.rename(columns={"p_value": "p_value_totDays"}, inplace = True)
    out = out.merge(totDays, on = geog, how = 'left') # merge
    
    # attrib coef --- creates range -1 to 1 index of heat vs. population as a driver of total pdays increase
    out['coef_attrib'] = (out['coef_pop'] - out['coef_heat']) / (out['coef_pop'] + out['coef_heat']) # normalize dif
    
    # drop all neg or zero pday slopes (e.g. cooling cities)
    out = out[out['coef_pdays'] > 0]
    out = out[out['coef_heat'] > 0]
    out = out[out['coef_pop'] > 0]
    
    # normalize coef of attribution 
    norm = out['coef_attrib']
    out['coef_attrib_norm'] = (norm-min(norm))/(max(norm)-min(norm))
    
    return out

#### Load Data
# file path
DATA_IN = "/home/cascade/projects/UrbanHeat/data/"  # Note: Need ?dl=1 to make sure this file gets read correctly
FIG_OUT = "/home/cascade/projects/UrbanHeat/figures/"

# P-days data
FN_IN = 'processed/AllDATA-GHS-ERA5-HI406-PDAYS.csv'
HI_STATS = pd.read_csv(DATA_IN+FN_IN)
FN_OUT = 'processed/AllDATA-GHS-ERA5-HI406-MAPDATA.csv'

#### Drop cities with only one Tmax Day in 1983 and none else because you cannot regress them
print(len(HI_STATS))
only83 = HI_STATS.groupby('ID_HDC_G0')['total_days'].sum() == 1 # sum up total days and find those with 1 day
only83 = list(only83[only83 == True].index) # make a list of IDs
sub = HI_STATS[HI_STATS['ID_HDC_G0'].isin(only83)] # subset those IDs
bad_ids = sub[(sub['year'] == 1983) & (sub['total_days'] == 1)] # drop those from 1983 only
drop_list = list(bad_ids['ID_HDC_G0']) # make a list
HI_STATS= HI_STATS[~HI_STATS['ID_HDC_G0'].isin(drop_list)] # drop those from the list
print(len(HI_STATS))

#### Run OLS Reg
stats_out = run_OLS(HI_STATS, 'ID_HDC_G0', alpha = 0.05)

#### Add In Meta Data
geog = ['region', 'intermediate-region', 'sub-region','CTR_MN_NM', 'ID_HDC_G0', 'GCPNT_LAT', 'GCPNT_LON']
meta_fn = 'processed/AllDATA-GHS-ERA5-HI406-META.csv'
all_data = pd.read_csv(DATA_IN+meta_fn)
meta = all_data[geog]
meta = meta.drop_duplicates('ID_HDC_G0')

#### Merge in meta
stats_out_final = stats_out.merge(meta, on = 'ID_HDC_G0', how = 'left')

#### Add In Population
pop = HI_STATS[['P1983', 'P2016', 'ID_HDC_G0']]
pop = pop.drop_duplicates('ID_HDC_G0')
stats_out_final = stats_out_final.merge(pop, on = 'ID_HDC_G0', how = 'inner')

#### Write it out 
## All data
FN_OUT = 'processed/AllDATA-GHS-ERA5-HI406-MAPDATA.csv'
stats_out_final.to_csv(DATA_IN+FN_OUT)

## City-level where pdays is sig at < 0.05
p95 = stats_out_final[stats_out_final['p_value_pdays'] < 0.05]
FN_OUT = 'processed/AllDATA-GHS-ERA5-HI406-MAPDATA_PDAYS_P05.csv'
p95.to_csv(DATA_IN+FN_OUT)

## City-level where total days is sig at < 0.05
p95 = stats_out_final[stats_out_final['p_value_totDays'] < 0.05]
FN_OUT = 'processed/AllDATA-GHS-ERA5-HI406-MAPDATA_TOTDAYS_P05.csv'
p95.to_csv(DATA_IN+FN_OUT)


