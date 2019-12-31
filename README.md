# UrbanHeat
Code to map the TMax of 13,000 cities on the planet from 1983 - 2016

**Steps (In SRC)**
1. poly_to_raster.py - turn the (GHS-UCDB)[https://ghsl.jrc.ec.europa.eu/ucdb2018Overview.php] to a raster.
2. UrbanTempParallel.py - find daily areal avg. Tmax for each city in the GHS-UCDB from 1983 - 2016.
3. Tmax_Stats.py - calculate all stats of extreme heat events
4. Edge_Search.py - find all dec-jan overlap events manually and fix them in the record 

**ADD REQUIREMENTS**