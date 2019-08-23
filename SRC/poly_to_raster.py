import rasterio
from rasterio import features

def poly_to_raster (rst, polys, value, touched, out_fn, fill_value):
    """Function makes a raster from a list of polygons
    
    Args:   rst = input raster already read in as a rasterio object to act as a template
            polys = input polygons already read in as a gpd dataframe
            value = col with value to burn into raster
            touched = bool, if True all pixels touched (not centers) are burned into raster
            out_fn = out file name 
            fill_value = value to revalue input raster before burning in polygons 
    
    """

    meta = rst.meta.copy() # copy meta data from rst
    out_arr = rst.read(1) # get an array to burn shapes
    out_arr.fill(fill_value) # revalue rst to an Nan Value before burning in polygons
    
    # extract geom and values to burn
    shapes = ((geom,value) for geom, value in zip(polys['geometry'], polys[value])) 
    
    # burn shapes intp an array
    burned = features.rasterize(shapes=shapes, fill=0, out=out_arr, transform=rst.transform, all_touched=touched)
    
    # write our raster to disk
    with rasterio.open(out_fn, 'w', **meta) as out:
        out.write_band(1, burned)