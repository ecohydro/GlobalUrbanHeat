# Urban Heat Wave Climatology

## Analysis of every extreme event (continuous days >40.6C) for all cities 1983 - 2015.
by Cascade Tuholske & K Caylor 2019.10.15


### Data
This repo depends on reading in the necessary datafile from Dropbox. This file is loaded in `Figures.ipynb`.

The columns are:

* Event_ID - Individual event ID
* ID_HDC_G0 - city ID from the GHSL Urban Centers Database that can be tied back to full city meta data
* CTR_MN_NM - Country
* total_days - the total number of days in 2015 for that city >40.6C. Note this record records multiple events per city, so do not use this column without dropping duplicate cities.
* duration - number of consecutive days >40.6C for a given extreme tempature event.
* avg_temp - average Tmax temp during event
* avg_intens - average Tmax intensity (Tmax - 40.6C) during event
* tot_intens - sum of daily intensity during Tmax event
* events - list of dates when Tmax event happend
* intensty - list of daily Tmax intensity, or the amount Tmax was greater than 40.6C
* Tmax - list of Tmax temps during events

**Update 2019.10.19 by CPT - USE ALLDATA_20191019.csv" --> found error in earlier code and reran Tmax filters and stats