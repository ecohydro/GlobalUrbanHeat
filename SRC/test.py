import rasterio 
import os

# LOCAL TEST 
CHIRT_DIR = '/Users/cascade/Github/UrbanHeat/data/test_data/' # <<--- path to loop through
# SHP_DIR = '/Users/cascade/Github/PopRaster/data/raw/JRC/ghs-ucdb/'
# POLY_RST_DIR = '/Users/cascade/Github/PopRaster/data/interim/'
# DATA_OUT = '/Users/cascade/Github/UrbanHeat/data/test_data/test_out'


# List all subdirectories using os.listdir
# for sub_dir in os.listdir(CHIRT_DIR):
#     if os.path.isdir(os.path.join(CHIRT_DIR, sub_dir)):
        
#         # Set sub dir to path
#         path = sub_dir

#         for fn in os.listdir(path):
#             # find all the tif files
#             if fn.endswith('.tif'):
#                 print(fn)

for dirpath, dirnames, files in os.walk(CHIRT_DIR):
    print(f'Found directory: {dirpath}')
    print(type(dirpath))
    year = dirpath.split(CHIRT_DIR)[1] + ' dir'
    print(year)

    for fn in files:
        if fn.endswith('.tif'):
            print(fn)
            print(year)
