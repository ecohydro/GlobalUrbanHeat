###############################################3

By Cascade Tuholske 2020.03.27

Run each .py  script to build the dataset.

Step 4 can be done in two ways:
	(1) 4_Event_Stats.py opens all the HI files and runs the stats on the entire record.
	    This allows for heat events that over lap more than one year (e.g. a heat wave
	    that goes from Dec - Feb). The total number of days from the heat event are added to
	    left year (Dec year)
        
	(2) 4a_Tmax_Stats_HI.py Does not account for Dec - Jan overlap. Thus totals are year
	    specific. You will then need to use 4b_Event_Stack_HI.py to stack the files
        before running 5_Add_Region_GPS.py
        
        -- 2020.08.27 Use 4a & 4b for analysis CPT.