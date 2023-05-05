###############################################

UPDATED 2021.02.01 by Cascade Tuholske
UPDATED 2023.05.05 by Cascade Tuholske

###############################################

The data required to run this from start to finish are:

Global Human Settlment Layer Urban Centre Database - https://ghsl.jrc.ec.europa.eu/ghs_stat_ucdb2015mt_r2019a.php
CHIRTS-daily Tmax - https://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/global_tifs_p05/Tmax/
CHIRTS-daily RHmin - please email Cascade Tuholske for this data (old link pointed to RH average, not RHmin)  
World regions - https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes
GHCN/GSOD Stations - https://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/ValidationData.zip
countrylist.csv - in repo
GHS-UCDB-IDS.csv - in repo
GHS-UCDB-Interp.csv - in repo

To Run
---------- 
1. Geo Conda Env needs to activated (see requirements.ylm).  
2. For fast processing, requires a computer than can run at least 20 cores in parallel.  
3. Make sure all file paths in each script have been updated to your machine. 
4. Run each .py script 01_* to 09_ to build the dataset.
