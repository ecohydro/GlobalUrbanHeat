##################################################################################
#
#   Select GHS IDs, Names, Lat/long, and Countries & then add UN world regions
#   By Cascade Tuholske 
#
#   The next step is to add world regions and lat/lon for each city in the
#   GHS-UCDB dataset for analysis
#   
#   World Region List came from: 
#   https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes
#
#   which have been turn into this file - countrylist.csv
#
#   Updated CPT 2021.02.15 
#   For old version look in src/oldcode/
#   Cleaned up and simplified, more columns from ghs-ucdb added 
#
#################################################################################

# Dependencies
import pandas as pd
import numpy as np
import geopandas as gpd

# Load Files
DATA_PATH= '/home/cascade/projects/UrbanHeat/data/'

# Get GHS-UCDB Columns we want to use 
ghs_fn = DATA_PATH+'raw/GHS_UCDB/GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_0.shp'
ghs = gpd.read_file(ghs_fn)
print('Len GHS is', len(ghs))

# GHS columns we want 
cols = ['ID_HDC_G0','CTR_MN_NM', 'UC_NM_MN','GCPNT_LAT','GCPNT_LON']
df_out = ghs[cols]

# Fix Bad countries in ghs-ucdb
df_out.CTR_MN_NM = df_out.CTR_MN_NM.replace('CÃ´te d\'Ivoire', 'Ivory Coast') 


# Get UN regions
countries_fn = DATA_PATH+'raw/countrylist.csv'
countries = pd.read_csv(countries_fn)

# Subset
cols = ['name','region','sub-region','intermediate-region']
countries_out = countries[cols]
countries_out.rename(columns={'name': 'CTR_MN_NM'}, inplace = True) # match GHS col 

# Fix remaining bad countries in UN ISO lists to match GHS-UCDB, CPT March 2021
countries_out .CTR_MN_NM = countries_out.CTR_MN_NM.replace('Brunei Darussalam', 'Brunei') 
countries_out .CTR_MN_NM = countries_out.CTR_MN_NM.replace('Cabo Verde', 'Cape Verde') 
countries_out .CTR_MN_NM = countries_out.CTR_MN_NM.replace('Cape Verde', 'Cabo Verde') 
countries_out .CTR_MN_NM = countries_out.CTR_MN_NM.replace('Moldova, Republic of', 'Moldova')
countries_out .CTR_MN_NM = countries_out.CTR_MN_NM.replace('Sao Tome and Principe', 'São Tomé and Príncipe')
countries_out .CTR_MN_NM = countries_out.CTR_MN_NM.replace('United Kingdom of Great Britain and Northern Ireland', 'United Kingdom')
countries_out .CTR_MN_NM = countries_out.CTR_MN_NM.replace('Taiwan, Province of China', 'Taiwan')
countries_out .CTR_MN_NM = countries_out.CTR_MN_NM.replace('Czechia', 'Czech Republic')

# Merge
df_out = df_out.merge(countries_out, on = 'CTR_MN_NM', how = 'left')
print('Len IDS meta is', len(df_out))

# Write out 
fn_out = DATA_PATH+'interim/GHS-UCDB-IDS.csv'
df_out.to_csv(fn_out, index = False)

#################################################################################
#
#   Countries that have had to be renamed because of discrepancies in formatting
#   and naming between the ISO and the GHS-UCDB
#
#   If you pull ISO country list you may have to update the strings as shown 
#   below -- CPT 2021.02.15
#
#################################################################################

# countries_out.CTR_MN_NM = countries_out.CTR_MN_NM.replace('Taiwan, Province of China', 'Taiwan')
# countries_out.CTR_MN_NM = countries_out.CTR_MN_NM.replace('Czechia', 'Czech Republic')

# Fix remaining bad countries in UN ISO lists to match GHS-UCDB, CPT March 2021
# countries.name = countries.name.replace('Brunei Darussalam', 'Brunei') 
# countries.CTR_MN_NM = df_out.CTR_MN_NM.replace('Cabo Verde', 'Cape Verde') 
# countries.CTR_MN_NM = df_out.CTR_MN_NM.replace('Cape Verde', 'Cabo Verde') 
# countries.CTR_MN_NM = df_out.CTR_MN_NM.replace('Moldova, Republic of', 'Moldova')
# countries.CTR_MN_NM = df_out.CTR_MN_NM.replace('Sao Tome and Principe', 'São Tomé and Príncipe')
# countries.CTR_MN_NM = df_out.CTR_MN_NM.replace('United Kingdom of Great Britain and Northern Ireland', 'United Kingdom')
# countries.CTR_MN_NM = df_out.CTR_MN_NM.replace('Taiwan, Province of China', 'Taiwan')
# countries.CTR_MN_NM = df_out.CTR_MN_NM.replace('Czechia', 'Czech Republic')

# countries.name = countries.name.replace('Bolivia (Plurinational State of)', 'Bolivia')
# countries.name = countries.name.replace('Côte d\'Ivoire', 'CÃ´te d\'Ivoire')
# countries.name = countries.name.replace('Iran (Islamic Republic of)','Iran')
# countries.name = countries.name.replace('Tanzania, United Republic of','Tanzania')
# countries.name = countries.name.replace('United States of America','United States')
# countries.name = countries.name.replace('Venezuela (Bolivarian Republic of)','United States')
# countries.name = countries.name.replace('Korea (Democratic People\'s Republic of)','North Korea')
# countries.name = countries.name.replace('Korea, Republic of','South Korea')
# countries.name = countries.name.replace('Lao People\'s Democratic Republic','Laos')
# countries.name = countries.name.replace('Syrian Arab Republic','Syria')
# countries.name = countries.name.replace('Palestine, State of','Palestina')
# countries.name = countries.name.replace('Congo, Democratic Republic of the','Democratic Republic of the Congo')
# countries.name = countries.name.replace('Russian Federation','Russia')
# countries.name = countries.name.replace('Congo','Republic of Congo')