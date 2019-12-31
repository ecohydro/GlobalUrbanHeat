##################################################################################
#
#   TMAX FINAL STATS
#   By Cascade Tuholske on 2019.12.31
#
#   A notebook to subset Tmax daily for the 13000 GHS urban areas to identify dates >40c, consecuritve days >40 c etc.
#   Moved from cpt_tmax_subset to clean up all the code on 2019-09-24
#
#   **Need to subset**
#       - Days per year
#       - Duration of each event
#       - Intensity of each day during each event (>40.6)
#          - Avg temp
#           - Avg intsensity
#
#   **NOTE 2019.10.19 FOUND BUG IN DATA ---> EVENTS TOO LONG**
#   Some events too long and have dates that are note sequential in them.
#   I think I fixed the problem on 2019.10.19 and reran the code
#   **NOTE 2019.10.19 RE RAN CODE TO PRODUCE STATS FOR ALL YEARS AFTER FIXING BUG**
#
#   This come from cpt_tmax_stats_final notebook and was adapated on 2019.12.31 to .py
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

#### 1 Function turns csv into x-array
def csv_to_xr(file_in, time_dim, space_dim):

    """ Function reads in a csv w/ GHS-UCDB IDs and temp, isolates the temp
    and returns a xarray data array with dims set to city ids and dates

    Args:
        file_in = file name and path
        time_dim = name for time dim as a str ... use date :-)
        space_dim = col name for GHS-UCDB IDs as an str (ID_HDC_G0)
    """

    df = pd.read_csv(file_in) # read the file in as a df
    print(df.shape)

    df_id = df[space_dim] # get IDs
    df_temp = df.iloc[:,3:] # get only temp columns
    df_temp.index = df_id # set index values
    df_temp_drop = df_temp.dropna() # Drop cities w/ no temp record
    print(len(df_temp_drop))

    temp_np = df_temp_drop.to_numpy() # turn temp cols into an np array

    # make xr Data Array w/ data as temp and dims as spece (e.g. id)

    # Note 2019 09 17 changed to xr.Dataset from xr.Dataarray
    temp_xr_da = xr.DataArray(temp_np, coords=[df_temp_drop.index, df_temp_drop.columns],
                            dims=[space_dim, time_dim])

    return temp_xr_da

#### 2 Function finds all the Tmax Events and writes it to a dateframe w/ dates for each city
def tmax_days(xarray, Tthresh):
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
    dayTot_list = []
    tmax_list = []
    intensity_list = []
    df_out = pd.DataFrame()

    # subset xarray
    out = xarray.where(xarray > Tthresh, drop = True)

    # start loop
    for index, loc in enumerate(out.ID_HDC_G0):
        id_list.append(out.ID_HDC_G0.values[index]) # get IDS
        date_list.append(out.sel(ID_HDC_G0 = loc).dropna(dim = 'date').date.values) # get event dates

        # this is actually getting the total events of all, 2019-09-22
        dayTot_list.append(len(out.sel(ID_HDC_G0 = loc).dropna(dim = 'date').date.values)) # get event totals

        tmax_list.append(out.sel(ID_HDC_G0 = loc).dropna(dim = 'date').values) # get temp values
        intensity_list.append(out.sel(ID_HDC_G0 = loc).dropna(dim = 'date').values - Tthresh) # get severity

    # write to a data frame
    df_out['ID_HDC_G0'] = id_list
    df_out['total_days'] = dayTot_list
    df_out['dates'] = date_list
    df_out['tmax'] = tmax_list
    df_out['tmax_tntensity'] = intensity_list

    # return df_out
    return df_out

#### 3 Function splits the dataset into Tmax events (continuous days >Tmax) for each city
def jul_convert(dates):
    "Function turn days into julian datetime"
    jul_days = pd.to_datetime(dates).to_julian_date()

    return jul_days

def event_split(dates, ID_HDC_G0, intensity, tmax, total_days):
    """ Searchs a list of dates and isolates sequential dates as a list, then calculates event stats.
    See comments in code for more details.

    Args:
        dates: pandas.core.index as julian dates
        ID_HDC_G0: city ID as string
        country: country for each city as string
        intensity: numpy.ndarray of intensities values
        tmax: numpy.ndarray of intensities values of tmax values
        total_days: total number of tmax days in a year for a given city

    """

    # city id
    city_id = ID_HDC_G0
    tot_days = total_days

    # lists to fill
    city_id_list = []
    tot_days_list = []
    event_dates_list = []
    dur_list = []
    intensity_list = []
    tmax_list = []
    avg_temp_list = []
    avg_int_list = []
    tot_int_list = []

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
        tot_int = intense.sum() # total intensity during event

        start = counter # reset start to current end (e.g. counter)

        # fill lists
        city_id_list.append(city_id)
        tot_days_list.append(tot_days)
        dur_list.append(dur)
        event_dates_list.append(event_dates)
        intensity_list.append(intense)
        tmax_list.append(temp)
        avg_temp_list.append(avg_temp)
        avg_int_list.append(avg_int)
        tot_int_list.append(tot_int)

    # write out as a dateframe
    df_out['ID_HDC_G0'] = city_id_list
    df_out['total_days'] = tot_days_list
    df_out['duration'] = dur_list
    df_out['avg_temp'] = avg_temp_list
    df_out['avg_intensity'] = avg_int_list
    df_out['tot_intensity'] = tot_int_list
    df_out['event_dates'] = event_dates_list
    df_out['duration'] = dur_list
    df_out['intensity'] = intensity_list
    df_out['tmax'] = tmax_list

    return df_out

#### 4 Function feeds output from function 2 into function 3

def tmax_stats(df_in):
    """ runs event_split functionon a dataframe to produce desired tmax stats

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
        total_days = row['total_days'] # get total number of tmax days

        df = event_split(dates, ID_HDC_G0, intensity, tmax, total_days)

        df_out = df_out.append(df)

    return df_out

#### 5 Loops through a file list and applies functions 1 - 4
####   to the data to produce Tmax stats for all tmax events in
####   a given year across all cities in the dataset

def stats_loop(dir_in, dir_out, fn_out, time_dim, space_dim, Tthresh):

    """ Loop through a dir with csvs to apply csv_to_xr and
    tmax_stats function and save out a .csv for each year

    Args:
        dir_in = dir path to loop through
        dir_out = dir path to save files out
        fn_out = string to label out files
        time_dim = name for time dim as a str ... use date :-) for csv_to_xr function
        space_dim = col name for GHS-UCDB IDs as an str (ID_HDC_G0) for csv_to_xr function
        Tthresh = int of temp threshold for temp_event function -- 40.6 is used

    """

    # Open the GHS-ID List with GeoPANDAS read_file
    ghs_ids_fn = 'GHS-UCSB-IDS.csv'
    ghs_ids_df = pd.read_csv(DATA_INTERIM+ghs_ids_fn)

    # Git File list
    fn_list = glob.glob(dir_in+'*.csv')

    for fn in sorted(fn_list):

        # Get year for arg for temp_event function
        year = fn.split('GHS-Tmax-DAILY_')[1].split('.csv')[0]
        print(year)

        # read csv as a data array
        temp_xr_da = csv_to_xr(fn, time_dim, space_dim)

        # data array to tmax events, out as df
        df_days = tmax_days(temp_xr_da, Tthresh)

        # tmax events stats, out as df
        df_out = tmax_stats(df_days)

        # merge to get countries
        ghs_ids_df_out = ghs_ids_df.merge(df_out, on='ID_HDC_G0', how = 'inner')
        # write it all out
        ghs_ids_df_out.to_csv(dir_out+fn_out+year+'.csv')

        print(year, 'SAVED!')

#### Run Code

# Dirs and FN Names
dir_in = '/home/cascade/projects/data_out_urbanheat/CHIRTS-GHS-DAILY/' # output from avg temp
DATA_INTERIM = '/home/cascade/projects/UrbanHeat/data/interim/' # ghs ID list
dir_out = '/home/cascade/projects/data_out_urbanheat/CHIRTS-GHS-Events-Stats/'
fn_out = 'CHIRTS-GHS-Events-Stats'
time_dim = 'date'
space_dim = 'ID_HDC_G0'
Tthresh = 40.6

# Run it!
stats_loop(dir_in, dir_out, fn_out, time_dim, space_dim, Tthresh)
