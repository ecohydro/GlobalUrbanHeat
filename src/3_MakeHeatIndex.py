##################################################################################
#
#       Make Heat Index
#       By Cascade Tuholske 2020.01.21
#
#       Program is designed to take areal-averaged CHIRTS Tmax for each GHS-UCDB and down-scaled MERRA-2 
#       humidity data and calculate the heat index for each city with a Tmax >80F.
#
#       NOAA Heat Index Equation - https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml
#
#       **NOTE**
#       At first I thought the heat index values for the hottest areas were insane (> 140F), 
#       but I spot checked the results and the 
#       NOAA Heat Index Table(https://www.kjrh.com/weather/weather-blog-what-exaclty-is-the-heat-index) 
#       simply reds out values where Tmax >40C and RH > 50%. So I guess were are on track ...
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
import math

#### DIR PATHS & Arguments
DIR_Tmax = '/home/cascade/projects/UrbanHeat/data/interim/CHIRTS-GHS-DAILY-Tmax/'
DIR_RH = '/home/cascade/projects/UrbanHeat/data/interim/CHIRTS-GHS-DAILY-RH/'
DIR_HI = '/home/cascade/projects/UrbanHeat/data/interim/CHIRTS-GHS-DAILY-HI/'
unit_in = 'C'
unit_out = 'C'

#### Functions 
def C_to_F(Tmax_C):
    "Function converts temp in C to F"
    Tmax_F = (Tmax_C * (9/5)) + 32
    
    return Tmax_F

def F_to_C(Tmax_F):
    "Function converts temp in F to C"
    Tmax_C = (Tmax_F - 32) * (5/9)
    
    return Tmax_C

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
    df = df.iloc[:,3:] # get only temp columns
    df.index = df_id # set index values
    df_drop = df.dropna() # Drop cities w/ no temp record 
    print(len(df_drop))
    
    arr = df_drop.to_numpy() # turn temp cols into an np array
    
    # make xr Data Array w/ data as temp and dims as spece (e.g. id)
    
    # Note 2019 09 17 changed to xr.Dataset from xr.Dataarray
    xr_da = xr.DataArray(arr, coords=[df_drop.index, df_drop.columns], 
                            dims=[space_dim, time_dim])
    return xr_da

def heatindex(Tmax, RH, unit_in, unit_out):
    
    """Make Heat Index from 2m air and relative humidity following NOAA's guidelines: 
    https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml. It is assumed that the
    tempatures and RH are geographically and temporally aligned in the x-arrays and can be stacked
    to the funciton.
    
    --- update as needed cpt 2020.02.17
    
    Args:
        Tmax = x-array of tempatures
        RH = x-array of realtive humitity
        unit_in = F or C, will convert C to F to apply heat index
        unit_out = If C is desired, will convert data to C
        
    Returns HI
    """
    
    # Make all data as float
    Tmax = Tmax.astype('float')
    RH = RH.astype('float')
    
    # 1 convert C to F if needed
    if unit_in == 'C':
        Tmax = C_to_F(Tmax)
        
    # 2 Apply Steadman's and average with Tmax
    USE_STEADMAN = (0.5 * (Tmax + 61.0 + ((Tmax-68.0)*1.2) + (RH*0.094)) + Tmax) / 2 < 80
    STEADMAN = USE_STEADMAN * (0.5 * (Tmax + 61.0 + ((Tmax-68.0)*1.2) + (RH*0.094))) #.astype(int)
    
    # 3 Use Rothfusz if (STEADMAN + Tmax) / 2 > 80
    USE_ROTH = (0.5 * (Tmax + 61.0 + ((Tmax-68.0)*1.2) + (RH*0.094)) + Tmax) / 2 > 80
    ROTH = USE_ROTH * (-42.379 + 2.04901523*Tmax + 10.14333127*RH - .22475541*Tmax *RH - .00683783*Tmax*Tmax - .05481717*RH*RH + .00122874*Tmax*Tmax*RH + .00085282*Tmax*RH*RH - .00000199*Tmax*Tmax*RH*RH)

    # 3 Adjust Roth 1
    USE_ADJ1 = (RH < 13) & (Tmax > 80) & (Tmax < 112)
    ADJ1_RH = USE_ADJ1 * RH #.astype(int)
    ADJ1_RH = ADJ1_RH.where(ADJ1_RH != 0) #ADJ1_RH[ADJ1_RH == 0] = np.nan
    ADJ1_Tmax = USE_ADJ1 * Tmax # .astype(int)
    ADJ1_Tmax = ADJ1_Tmax.where(ADJ1_Tmax != 0) #ADJ1_Tmax[ADJ1_Tmax == 0] = np.nan
    ADJ1 = ((13-ADJ1_RH)/4)*np.sqrt((17-abs(ADJ1_Tmax-95.))/17)
    ADJ1 = np.nan_to_num(ADJ1, 0)
    
    ADJ1_ROTH = ROTH * USE_ADJ1
    ADJ1_ROTH = ADJ1_ROTH - ADJ1
    
    # 4 Adjust Roth 2
    USE_ADJ2 = (RH > 85) & (Tmax > 80) & (Tmax < 87)
    ADJ2_RH = USE_ADJ2 * RH #.astype(int)
    ADJ2_RH = ADJ2_RH.where(ADJ2_RH != 0) #ADJ2_RH[ADJ2_RH == 0] = np.nan
    ADJ2_Tmax = USE_ADJ2.astype(int) * Tmax
    ADJ2_Tmax = ADJ2_Tmax.where(ADJ2_Tmax != 0) #ADJ2_Tmax[ADJ2_Tmax == 0] = np.nan
    ADJ2 = ((ADJ2_RH-85)/10) * ((87-ADJ2_Tmax)/5)
    ADJ2 = np.nan_to_num(ADJ2, 0)
    
    ADJ2_ROTH = ROTH * USE_ADJ2
    ADJ2_ROTH = ADJ2_ROTH + ADJ2
    
    # Roth w/o adjustments
    ROTH = ROTH * ~USE_ADJ1 * ~USE_ADJ2
    
    # sum the stacked arrays
    HI = ROTH + STEADMAN + ADJ1_ROTH +  ADJ2_ROTH 
    
    # Convert HI to C if desired
    if unit_out == 'C':
        HI = F_to_C(HI)
    
    # return for test
    # return STEADMAN, ADJ1_ROTH, ADJ2_ROTH, ROTH, HI
    
    return HI

def apply_heatindex(DIR_Tmax, DIR_RH, DIR_HI, unit_in, unit_out):
    """Function applies NOAA's heatindex to two pair directories w/ CSVs of realitive humidity
    and tempatures, respective, in a pairwise fashion
    
    Args:
        DIR_Tmax = the directory where Tmax .csv files are stored
        DIR_RH = the directory where RH .csv files are stored
        DIR_HI = the directory where HI files will be written
        unit_in = temp unit for Tmax (C or F)
        unit_out = desired temp unit for HI (C or F) for the output
    """
    Tmax_fn_list = glob.glob(DIR_Tmax+'*.csv')
    RH_fn_list = glob.glob(DIR_RH+'*.csv')

    for Tmax_fn, RH_fn in zip(sorted(Tmax_fn_list),sorted(RH_fn_list)):
    
        # Check the years RH and Tmax 
        Tmax_year = Tmax_fn.split('GHS-Tmax-DAILY_')[1].split('.csv')[0]
        print('Tmax year is ',Tmax_year)
        RH_year = RH_fn.split('GHS-Tmax-RH_')[1].split('.csv')[0]
        print('RH year is ', RH_year)

        # Read csv as x-array
        Tmax_xr = csv_to_xr(Tmax_fn, time_dim = 'date', space_dim = 'ID_HDC_G0')
        RH_xr = csv_to_xr(RH_fn, time_dim = 'date', space_dim = 'ID_HDC_G0')

        # Make heat index
        hi = heatindex(Tmax_xr, RH_xr, unit_in = unit_in, unit_out = unit_out)


        # CASCADE GO LOOK AT HOW X-ARRAYS ARE WRITTEN TO CSVS IN EARLIER CODE <<<<---- 2020.02.19

        # write to csv
        df = hi.to_pandas()
        df_out_nm = 'GHS-HI-DAILY_'+Tmax_year+'.csv'
        df.to_csv(DIR_HI+df_out_nm)
        print(RH_year, ' done \n')
    
    print('ALL DONE!')

#### RUN THE CODE
apply_heatindex(DIR_Tmax, DIR_RH, DIR_HI, unit_in = unit_in , unit_out = unit_out)