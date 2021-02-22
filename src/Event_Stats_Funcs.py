##################################################################################
#
#   By Cascade Tuholske on 2019.12.31
#   Updated 2020.02.23
#
#   Modified from 4_Tmax_Stats.py in oldcode dir
#
#   NOTE: Fully rewriten on 2021.02.01 see 'oldcode' for prior version / CPT
#   
#   These are the functions needed for 4_Event_Stats_Run.py
#
#################################################################################

#### Dependencies
import pandas as pd
import numpy as np
import xarray as xr
from random import random
from itertools import groupby
from operator import itemgetter
import geopandas as gpd 
import glob
from statistics import mean
import julian
import time 
import multiprocessing as mp 
from multiprocessing import Pool
import os

#### Function Loads all Tmax Data as an X-array
def read_data(dir_path, space_dim, time_dim):
    """ Function reads in all Tmax .csv files, joins them by date along the x-axis
    and returns the whole record as a x-array data array
    
    Args:   
        dir_path = path to .csv files 
        time_dim = name for time dim as a str ... use date :-)
        space_dim = col name for GHS-UCDB IDs as an str (ID_HDC_G0)
    """
    fn_list = sorted(glob.glob(dir_path+'*.csv'))
    df_out = pd.DataFrame()
    date_list = []

    # Open all Tmax files and concat into a df
    for i, fn in enumerate(fn_list):    
        # Open the CSV
        df = pd.read_csv(fn)

        # Get the city ids 
        if i == 1:
            df_id = df[space_dim]

        # get only the Tmax columns and concate date list 
        df_temp = df.iloc[:,3:] # get only temp columns
        date_list = date_list+list(df_temp.columns)

        # Drop cities w/ no temp record 
        df_temp_drop = df_temp.dropna()

        # Merge
        df_out = pd.concat([df_out, df_temp_drop], axis=1)
        print(df_out.shape)
    
    # make date into an array
    tmax_arr = df_out.to_numpy()

    # Make data into an xr.DataArray
    tmax_xr_da = xr.DataArray(tmax_arr, coords=[df_id, date_list], 
                             dims=[space_dim, time_dim])
    return tmax_xr_da

#### Function finds all the Tmax Events and writes it to a dateframe w/ dates for each city
def max_days(xarray, Tthresh):
    """ Function finds all the tmax days in a year and sums total days per year 
    greater than a threshold within a year where Tmax > Tthresh for each city. Returns the total number of days,
    the dates, the tempatures, and the intensity (daily Tmax - Tthresh)
    
    Args: 
        xarray = an xarray object with dims = (space, times)
        Tthresh = int of temp threshold
    """
    
    # empty lists & df
    id_list = []
    date_list = []
    tmax_list = []
    intensity_list = []
    df_out = pd.DataFrame()
    
    # subset xarray
    out = xarray.where(xarray > Tthresh, drop = True)

    # start loop 
    for index, loc in enumerate(out.ID_HDC_G0):
        id_list.append(out.ID_HDC_G0.values[index]) # get IDS
        date_list.append(out.sel(ID_HDC_G0 = loc).dropna(dim = 'date').date.values) # get event dates
        
        # #CPT 2020.02.23 
        # dayTot_list.append(len(out.sel(ID_HDC_G0 = loc).dropna(dim = 'date').date.values)) # get event totals
        
        tmax_list.append(out.sel(ID_HDC_G0 = loc).dropna(dim = 'date').values) # get temp values
        intensity_list.append(out.sel(ID_HDC_G0 = loc).dropna(dim = 'date').values - Tthresh) # get severity

    # write to a data frame
    df_out['ID_HDC_G0'] = id_list
    # df_out['total_days'] = dayTot_list #CPT 2020.02.23
    df_out['dates'] = date_list
    df_out['tmax'] = tmax_list
    df_out['tmax_tntensity'] = intensity_list

    # return df_out
    return df_out

#### Function splits the dataset into Tmax events (continuous days >Tmax) for each city
def jul_convert(dates):
    "Function turn days into julian datetime"
    jul_days = pd.to_datetime(dates).to_julian_date()
    
    return jul_days

def event_split(dates, ID_HDC_G0, intensity, tmax): #, total_days): #CPT 2020.02.23
    
    """ Searchs a list of dates and isolates sequential dates as a list, then calculates event stats.
    See comments in code for more details. 
    
    Args:
        dates: pandas.core.index as julian dates
        ID_HDC_G0: city ID as string
        intensity: numpy.ndarray of intensities values
        tmax: numpy.ndarray of intensities values of tmax values
        total_days: total number of tmax days in a year for a given city

    """

    # city id
    city_id = ID_HDC_G0
    # tot_days = total_days #CPT 2020.02.23
    
    # lists to fill
    city_id_list = []
    # tot_days_list = [] #CPT 2020.02.23
    event_dates_list = []
    dur_list = []
    intensity_list = []
    tmax_list = []
    avg_temp_list = []
    avg_int_list = []
    tot_int_list = []
    year_list = []
    
    # data frame out
    df_out = pd.DataFrame()
    
    # turn days into julian days
    jul_days = jul_convert(dates)
    
    # Counters to make sure we write the correct event dates to a list, don't want julian days in output
    counter = 0
    start = 0
    end = 0
    
    # Loop through dur list and isolate seq days, temps, and intensities
    for k, g in groupby(enumerate(jul_days.values), lambda x: x[1]-x[0]):
        
        seq = list(map(itemgetter(1), g)) # isolate seq. days
        dur = len(seq) # duration of each event
        
        counter = counter + dur # add duration to counter
        end = counter # end of current event
        
        event_dates = dates[start:end] # dates of tmax days during each event
        intense = intensity[start:end] # intensity of each day during event
        temp = tmax[start:end] # temp of each day during event
        avg_temp = mean(temp) # avg. temp during event
        avg_int = mean(intense) # avg. intensity during event
        tot_int = np.sum(intense) # total intensity during event
        
        start = counter # reset start to current end (e.g. counter)
        year = event_dates[0].split('.')[0]
        
        # fill lists
        city_id_list.append(city_id)
        year_list.append(year)
        # tot_days_list.append(tot_days) #CPT 2020.02.23
        dur_list.append(dur)
        event_dates_list.append(event_dates)
        intensity_list.append(intense)
        tmax_list.append(temp)
        avg_temp_list.append(avg_temp)
        avg_int_list.append(avg_int)
        tot_int_list.append(tot_int)

    # write out as a dateframe
    df_out['ID_HDC_G0'] = city_id_list
    df_out['year'] = year_list
    # df_out['total_days'] = tot_days_list #CPT 2020.02.23
    df_out['duration'] = dur_list
    df_out['avg_temp'] = avg_temp_list
    df_out['avg_intensity'] = avg_int_list
    df_out['tot_intensity'] = tot_int_list
    df_out['event_dates'] = event_dates_list
    df_out['duration'] = dur_list
    df_out['intensity'] = intensity_list
    df_out['tmax'] = tmax_list

    return df_out

#### Function feeds output from function 3 into function 4
def max_stats(df_in):
    """ runs event_split functionon a dataframe to produce desired threshold max stats

        NOTE - If you add arguments to event_split to make more states,
        be sure to update this function

        args:
            df: input dataframe
    """
    df_out = pd.DataFrame()

    # NOTE - If you add arguments to event_split to make more stats,
    # be sure to update this function

    for index, row in df_in.iterrows():
        dates = row['dates'] # Get event dates
        intensity = row['tmax_tntensity'] # Get intensity for each day
        tmax = row['tmax'] # Get tmax for each day
        ID_HDC_G0 = row['ID_HDC_G0'] # get city id
        # total_days = row['total_days'] # get total number of tmax days -- CPT 2020.02.23

        df = event_split(dates, ID_HDC_G0, intensity, tmax)# , total_days) #CPT 2020.02.23

        df_out = df_out.append(df)

    return df_out

#### Function to run tmax_stats on for parallel processing
def max_stats_run(fn):
    
    """ runs max_stats on a fn (.json) and writes on .json)
    Args:
        fn = file name
        data = data to split file names up with 
    """
    
    # open df & get names
    df = pd.read_json(fn, orient = 'split')
    data = fn.split('_temp/')[1].split('_')[0]
    i = fn.split(data+'_temp/')[1].split(data+'_')[1]
    out_path = fn.split(data+'_temp')[0]

    # make small for testing 
    df = df.iloc[0:4,:]
    
    # Calculate stats
    df_out = max_stats(df)

    # write file
    fn_out = out_path+data+'_temp/'+data+'_STAT_'+i
    df_out.to_json(fn_out, orient = 'split')
    
    print('done', i)

#### Run a parellel process 
def parallel_loop(function, dir_list, cpu_num):
    """Run the temp-ghs routine in parallel
    Args: 
        function = function to apply in parallel
        dir_list = list of dir or fns to loop through 
        cpu_num = numper of cpus to fire  
    """ 
    start = time.time()
    pool = Pool(processes = cpu_num)
    pool.map(function, dir_list)
    # pool.map_async(function, dir_list)
    pool.close()

    end = time.time()
    print(end-start)
    
    
#### Function threads it all together
# def run_stats(dir_path, space_dim, time_dim, Tthresh, fn_out):
    
#     """ Function ties all the Tmax Stats functions together and writes final stats for each Tmax 
#     event to a .csv file. Returns results as a dataframe if needed
    
#     Args:
#         dir_path = path to .csv files 
#         time_dim = name for time dim as a str ... use date :-)
#         space_dim = col name for GHS-UCDB IDs as an str (ID_HDC_G0)
#         Tthresh = float of temp threshold
#         fn_out = file and path to write final csv
        
#     """
    
#     # read in data
#     step1= read_data(dir_path, space_dim = space_dim, time_dim = time_dim)
#     #step1_sub = step1[:,:10] # subset data for testing
#     print('Stack x-array made')
    
#     # Mask data based on Tmax threshold ... we're using 40.6C
#     step2 = tmax_days(step1, Tthresh)
#     print('Tmax masked')
    
    
#     # Calculate stats
#     step3 = tmax_stats(step2)
#     print('Stats made')

#     # Save file out
#     step3.to_json(fn_out, orient = 'split')
    
#     return step3

#     print('done')
