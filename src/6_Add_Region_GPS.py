##################################################################################
#
#   Add Regions and GPS for each city in the dataset
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
#################################################################################

#### Dependencies
import pandas as pd
import numpy as np
import geopandas as gpd

#### Load Files

events_fn = '/home/cascade/projects/data_out_urbanheat/heatrange/All_data20200109_406C_es.csv' # Update as needed
out_fn = '/home/cascade/projects/data_out_urbanheat/heatrange/All_data20200109_406C_es_final.csv.csv' # Update as needed
events = pd.read_csv(events_fn)
ghs = gpd.read_file('/home/cascade/tana-crunch-cascade/projects/UrbanHeat/data/raw/GHS_UCDB/GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_0.shp')
countries = pd.read_csv('/home/cascade/tana-crunch-cascade/projects/UrbanHeat/data/raw/countrylist.csv')

print(events.head())

# Fix Ivory Coast
events.CTR_MN_NM = events.CTR_MN_NM.replace('CÃ´te d\'Ivoire', 'Ivory Coast')

#### Merge Events and Countries

# make a region dataframe of the cols we want
regions = pd.DataFrame()
regions['CTR_MN_NM'] = countries['name']
regions['region'] = countries['region']
regions['sub-region'] = countries['sub-region']
regions['intermediate-region'] = countries['intermediate-region']

# Merge 
print(len(events))
events = events.merge(regions, on = 'CTR_MN_NM', how = 'inner')
print(len(events))

print(events.head())

#### Add lat/lon of GHS-UCDB to events

# get GHS-UCDB lat/long
df = pd.DataFrame()
df['ID_HDC_G0'] = ghs['ID_HDC_G0']
df['GCPNT_LAT'] = ghs['GCPNT_LAT']
df['GCPNT_LON'] = ghs['GCPNT_LON']

# merge
events = events.merge(df, on = 'ID_HDC_G0', how = 'inner')

print(events.head())

# save out file
events.to_csv(out_fn)

#################################################################################
#
#   Countries that have had to be renamed because of discrepancies in formatting
#   and naming between the ISO and the GHS-UCDB
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
