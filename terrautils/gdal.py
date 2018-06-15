"""GDAL

This module provides wrappers to GDAL for manipulating geospatial data.
"""

from osgeo import gdal, gdalnumeric, ogr
import numpy as np
import rasterio
from rasterio.mask import mask

# TODO: Can we just merge this whole module into spatial.py?

def clip_raster(rast_path, geojson, out_path=None, nodata=-9999):
    """Clip raster to polygon.

    Args:
      rast_path (str): path to raster file
      features_path (str): path to features file or geojson string
      nodata: the no data value

    Returns: (numpy array, GeoTransform)

    Notes: Oddly, the "features path" can be either a filename
      OR a geojson string. GDAL seems to figure it out and do
      the right thing.

      From http://karthur.org/2015/clipping-rasters-in-python.html
    """

    with rasterio.open(rast_path) as raster:
        # Cannot use crop=True because rasterio truncates coordinates to floats
        # and do not support Decimals, so precision is lost.
        # https://github.com/mapbox/rasterio/blob/master/rasterio/_features.pyx#L332
        out_px, out_transform = mask(raster, geojson, nodata=nodata)
    out_meta = raster.meta.copy()

    # save the resulting raster
    out_meta.update({"driver": "GTiff",
                     "height": out_px.shape[1],
                     "width": out_px.shape[2],
                     "transform": out_transform})

    if np.count_nonzero(out_px) > 0:
        if out_path:
            with rasterio.open(out_path, "w", **out_meta) as dest:
                dest.write(out_px)

        return out_px
    else:
        return None


def get_raster_extents(fname):
    """Calculate the extent and the center of the given raster."""
    src = gdal.Open(fname)
    ulx, xres, xskew, uly, yskew, yres = src.GetGeoTransform()
    lrx = ulx + (src.RasterXSize * xres)
    lry = uly + (src.RasterYSize * yres)
    extent = (ulx, lry, lrx, uly)
    center = ((ulx+lrx)/2, (uly+lry)/2)

    return (extent, center)


def centroid_from_geojson(geojson):
    """Return centroid lat/lon of a geojson object."""
    geom_poly = ogr.CreateGeometryFromJson(geojson)
    centroid = geom_poly.Centroid()

    return centroid.ExportToJson()


def wkt_to_geojson(wkt):
    geom = ogr.CreateGeometryFromWkt(wkt)
    return geom.ExportToJson()
