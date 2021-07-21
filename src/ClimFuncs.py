##################################################################################
#
#       Clim Funcs
#       By Cascade Tuholske Spring 2021
#
#       Functions needed for 02_MakeHeatIndex.py
#
#       NOAA Heat Index Equation - https://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml
#
#
#################################################################################


#### Dependencies
import numpy as np
import xarray
import pandas as pd

#### Functions
def C_to_F(Tmax_C):
    "Function converts temp in C to F"
    Tmax_F = (Tmax_C * (9/5)) + 32
    
    return Tmax_F

def F_to_C(Tmax_F):
    "Function converts temp in F to C"
    Tmax_C = (Tmax_F - 32) * (5/9)
    
    return Tmax_C

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
#     Tmax = Tmax.astype('float')
#     RH = RH.astype('float')
    
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