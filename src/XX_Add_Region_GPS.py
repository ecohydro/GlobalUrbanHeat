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

# GHS columns we want 
cols = ['ID_HDC_G0','CTR_MN_NM', 'UC_NM_MN','GCPNT_LAT','GCPNT_LON']
df_out = ghs[cols]

# Fix Ivory coast
df_out.CTR_MN_NM = df_out.CTR_MN_NM.replace('CÃ´te d\'Ivoire', 'Ivory Coast') 

# Get UN regions
regions_fn = DATA_PATH+'raw/countrylist.csv'
regions = pd.read_csv(regions_fn)

cols = ['name','region','sub-region','intermediate-region']
regions = regions[cols]
regions.rename(columns={'name': 'CTR_MN_NM'}, inplace = True)

# Merge
df_out = df_out.merge(regions, on = 'CTR_MN_NM', how = 'inner')

# write out 
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