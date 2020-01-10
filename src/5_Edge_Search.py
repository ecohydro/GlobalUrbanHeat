##################################################################################
#
#   Edge Search
#
#   Notebook to search for extreme heat events that overlap years.
#   Fix to resolve problem TBD.
#
#   By Cascade Tuholske, 2019-10-19
#
#   Preliminary Findings
#   - In the entire record, there are 97 events that start on Jan 1.
#   - In the entire record, there are 94 events that end of Dec 31.
#   Of these, it looks like 5 were from the same city and bridged two years
#
#   Moved to .py file from .ipynb on 2019.12.31 by Cascade Tuholske
#
#   NOTE UPDATE FILE NAME AS NEEDED
# 
##################################################################################

# Depdencies
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Dir and FN
FN_IN = "/home/cascade/projects/data_out_urbanheat/heatrange/All_data20200109_406C.csv" # SET
FN_OUT = "/home/cascade/projects/data_out_urbanheat/heatrange/All_data20200109_406C_es.csv" # SET
tmax = 40.6 # UPDATE
df = pd.read_csv(FN_IN)

#### 1. Find Edges ##################################################################################
# NOTE Build query to find dates XXXX.12.31 and XXXX.01.01 
# NOTE Events col are strings

def date_search(df, date):
    
    """ Searches Tmax data frame to find dates within a Tmax event with the goal of finding 12.31-01.01 overlap
    Args:
        df = tmax df
        data = date you want to find
    
    Returns df with event id, event dates, city id, year, and tmax temps 
    """

    event_id_list = []
    event_dates_list = []
    city_id_list = []
    event_year_list = []
    tmax_list = []
    total_days_list = []
    
    for index, row in df.iterrows():
        if date in row['event_dates']:
            
            event_id = row['Event_ID']
            event_dates = row['event_dates']
            city_id = row['ID_HDC_G0']
            event_year = row['year']
            tmax = row['tmax']
            total_days = row['total_days']
            
            event_id_list.append(event_id)
            event_dates_list.append(event_dates)
            city_id_list.append(city_id)
            event_year_list.append(event_year)
            tmax_list.append(tmax)
            total_days_list.append(total_days)
    
    df_out = pd.DataFrame()
    df_out['ID_HDC_G0'] = city_id_list
    df_out['Event_ID'] = event_id_list
    df_out['tmax'] = tmax_list
    df_out['event_dates'] = event_dates_list
    df_out['year'] = event_year_list
    df_out['total_days'] = total_days_list
    
    return df_out

# Dec 31 Events
df_1231 = date_search(df, '12.31')

# Jan 1 Events
df_0101 = date_search(df, '01.01')

# Check len
print(len(df_0101))
print(len(df_1231))

# See how many cities overlap
print(df_1231['ID_HDC_G0'].isin(df_0101['ID_HDC_G0']).value_counts())

# Merge based on city ID to only include overlaps
merge = pd.merge(df_1231, df_0101, on = 'ID_HDC_G0', how = 'inner')

# Look for years the are one apart and get rows

out = []
for i, year in merge.iterrows():
        if year['year_y'] - year['year_x'] == 1:
            out.append(i)
out

# Get the rows with dec 31 - jan 1
overlap = merge.loc[out]
overlap

print('STEP 1 DONE!')

#### 2. Make new data from overlaps ##################################################################################
def string_hunt(string_list, out_list, dtype):
    """Helper function to pull tmax record strings from a list of Tmaxs, and turn dates into ints"""
    for i in string_list: # set the strings from X list
        if len(i) > 1:
            if '[' in i:
                
                record = i[1:]
                
                if ']' in record:
                    
                    record =  record[:-1]
                    out_list.append(dtype(record))
                else:
                    out_list.append(dtype(record))
            
            elif ']' in i:
                record = i[:-1]
                out_list.append(dtype(record))
            
            else:
                record = i
                out_list.append(dtype(record))
    
    return out_list

# loop by row to get temps

df_overlap = pd.DataFrame()

# Lists for df
temps_list_list = []
dates_list_list = []
duration_list = []
avg_temp_list = []
intensity_list = []
avg_intensity_list = []
tot_intensity_list = []
city_id_list = []
year_x_list = []
year_y_list = []
event_x_id_list = [] # <<<<<---- going to use the ID for the Dec date for now
event_y_id_list = [] # <<<<<---- going to use the ID for the Dec date for now
total_days_x_list = [] # total number of days added to first year
total_days_y_list = [] # total number of days subtracted first year

### Tempature
for i, row in overlap.iterrows():
    
    ### Temp and Days
    temps_list = [] # make list to populate
    
    temps_x = (row['tmax_x'].split(' ')) # split up the strings from X list
    temps_list = string_hunt(temps_x, temps_list, float)

    dur_x = len(temps_list) # duration first year 
    
    temps_y = (row['tmax_y'].split(' ')) # split up the strings from X list
    temps_list = string_hunt(temps_y, temps_list, float)

    dur_y = len(temps_list) - dur_x # duration second year
    
    temps_list_list.append(temps_list)
    
    ## Total Days
    total_days_x = row['total_days_x'] + dur_y # add event dur from year x
    total_days_y = row['total_days_y'] - dur_y # subtract event dur from year y
    
    total_days_x_list.append(total_days_x)
    total_days_y_list.append(total_days_y)
   
    ### Dates
    dates_list = [] # make list to populate
    
    dates_x = (row['event_dates_x'].split(' ')) # split up the strings from X list
    dates_list = string_hunt(dates_x, dates_list, str)
    
    dates_y = (row['event_dates_y'].split(' ')) # split up the strings from X list
    dates_list = string_hunt(dates_y, dates_list, str)
    
    dates_list_list.append(dates_list) # append list for df 
    
    ### Duration
    duration = len(temps_list)
    duration_list.append(duration)
    
    ### Intensity [x - 13 for x in a]
    intensity = [x - tmax for x in temps_list] # <<<<<<-------------------------- UPDATE TMAX AS NEEDED
    intensity_list.append(intensity)
    
    ### Avg_temp
    avg_temp = np.mean(temps_list)
    avg_temp_list.append(avg_temp)
    
    ### avg_intensity
    avg_intensity = np.mean(intensity)
    avg_intensity_list.append(avg_intensity)
    
    ### tot_intensity
    tot_intensity = np.sum(intensity)
    tot_intensity_list.append(tot_intensity)
    
    ### city_id & total days & year, etc
    city_id = row['ID_HDC_G0']
    city_id_list.append(city_id)
    
    ### Year
    year_x = row['year_x']
    year_x_list.append(year_x)
    
    year_y = row['year_y']
    year_y_list.append(year_y)
    
    ### event ID
    event_x_id = row['Event_ID_x']
    event_x_id_list.append(event_x_id)
    event_y_id = row['Event_ID_y']
    event_y_id_list.append(event_y_id)

df_overlap['ID_HDC_G0'] = city_id_list
df_overlap['Event_ID_x'] = event_x_id_list
df_overlap['Event_ID_y'] = event_y_id_list
df_overlap['year_x'] = year_x_list
df_overlap['year_y'] = year_y_list
df_overlap['total_days_x'] = total_days_x_list
df_overlap['total_days_y'] = total_days_y_list
df_overlap['tmax'] = temps_list_list
df_overlap['event_dates'] = dates_list_list
df_overlap['duration'] = duration_list
df_overlap['avg_temp'] = avg_temp_list
df_overlap['intensity'] = intensity_list
df_overlap['tot_intensity'] = tot_intensity_list
df_overlap['avg_intensity'] = avg_intensity_list
df_overlap.head(1)

print('STEP 2 DONE!')

#### 3. Fix Total Days for Cities ##################################################################################
# Here we subtract the event days 
# from the Jan year (y) from year y and we add those dates 
# to year x so on balance the dates from the 
# jan year are now just added to the earlier year

# Get List of Years and Cities for the dec-jan overlap and then find them in the dataset

# Get List of Years and Cities for the dec-jan overlap and then find them in the dataset

# Start with year_x
years_x = list(df_overlap['year_x'])
id_x = list(df_overlap['ID_HDC_G0'])
total_days_x = list(df_overlap['total_days_x'])

x_list = []
for i in zip(years_x,id_x, total_days_x):
    x_list.append(i)

for x in x_list:
    print(x)

# Search df for i list and replace days
# this is super slow but it works

df_copy = df.copy()

for x in x_list:
    for i, row in df_copy.iterrows():
        if (row['year'] == x[0]) & (row['ID_HDC_G0'] == x[1]):
            print(df_copy.loc[i,'total_days'])
            df_copy.loc[i,'total_days'] = x[2]
            print(df_copy.loc[i,'total_days'])

# Now with year_y 
years_y = list(df_overlap['year_y'])
id_y = list(df_overlap['ID_HDC_G0'])
total_days_y = list(df_overlap['total_days_y'])

y_list = []
for i in zip(years_y, id_y, total_days_y):
    y_list.append(i)

for y in y_list:
    print(y)

# Run on y_list
for y in y_list:
    for i, row in df_copy.iterrows():
        if (row['year'] == y[0]) & (row['ID_HDC_G0'] == y[1]):
            print(df_copy.loc[i,'total_days'])
            df_copy.loc[i,'total_days'] = y[2]
            print(df_copy.loc[i,'total_days'])

print('STEP 3 DONE!')

#### 4. Add Meta data back ##################################################################################

# copy overlap 
df_overlap_copy = df_overlap.copy()

print(len(df_overlap_copy))
df_overlap_copy.head(1)

# Get columns to merge and merge
df_cols = df_copy[['CTR_MN_NM', 'ID_HDC_G0']]
df_cols = df_cols.drop_duplicates('ID_HDC_G0')
df_overlap_copy = df_overlap_copy.merge(df_cols, on = 'ID_HDC_G0', how = 'inner')

print(len(df_overlap_copy))
df_overlap_copy

# drop and rename columns
df_overlap_copy.rename(columns = {'year_x':'year'}, inplace = True) 
df_overlap_copy.rename(columns = {'total_days_x':'total_days'}, inplace = True) 

df_overlap_copy

print('STEP 4 DONE!')

#### 5. Drop overlapped years and add in new DF ##################################################################################

## Drop overlap events based on event id from all events
overlap.head(1)

# Get events
jan_ids = list(overlap['Event_ID_y'])
dec_ids = list(overlap['Event_ID_x'])

# Drop Events from Dataset
print(len(df_copy))
df_events = df_copy.copy()

# Jan
for event in jan_ids:
    df_events = df_events[df_events['Event_ID'] != event]

# Dec
for event in dec_ids:
    df_events = df_events[df_events['Event_ID'] != event]

print(len(df_events))

# Merge 
df_events = df_events.drop(columns=['Unnamed: 0']) # tried without 'Unnamed: 0.1'
df_events.head()

print(len(df_events))
print(len(df_overlap_copy))
print(df_events.columns)
print(df_overlap_copy.columns)

# Make 'x' event ids for final df
df_overlap_copy['Event_ID'] = df_overlap_copy['Event_ID_x']

# drop event x y event ID cols 
cols_to_use = df_overlap_copy.columns.difference(df_events.columns) # find missing columns
cols_list = list(cols_to_use) # list
cols_list

df_overlap_copy = df_overlap_copy.drop(columns = cols_list)
df_overlap_copy

print(len(df_events))
print(len(df_overlap_copy))
df_final = pd.concat([df_events, df_overlap_copy], sort = True)
print(len(df_final))

# Save it out
df_final.to_csv(FN_OUT)

print('STEP 5 DONE!')
