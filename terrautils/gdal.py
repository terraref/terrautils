"""GDAL

This module provides wrappers to GDAL for manipulating geospatial data.
"""

import os
import subprocess
import numpy as np
import yaml
import json
from osgeo import gdal, gdalnumeric, ogr


# TODO: Can we just merge this whole module into spatial.py?

def clip_raster(rast_path, bounds, out_path=None, nodata=-9999):
    """Clip raster to polygon.

    Args:
      rast_path (str): path to raster file
      bounds (tuple): (min_y, max_y, min_x, max_x)
      nodata: the no data value

    Returns: (numpy array, GeoTransform)

    Notes: Oddly, the "features path" can be either a filename
      OR a geojson string. GDAL seems to figure it out and do
      the right thing.

      From http://karthur.org/2015/clipping-rasters-in-python.html
    """

    if not out_path:
        out_path = "temp.tif"

    # Clip raster to GDAL and read it to numpy array
    coords = "%s %s %s %s" % (bounds[2], bounds[1], bounds[3], bounds[0])
    cmd = 'gdal_translate -projwin %s "%s" "%s"' % (coords, rast_path, out_path)
    subprocess.call(cmd, shell=True, stdout=open(os.devnull, 'wb'))
    out_px = np.array(gdal.Open(out_path).ReadAsArray())

    if np.count_nonzero(out_px) > 0:
        if out_path == "temp.tif":
            os.remove(out_path)
        return out_px
    else:
        os.remove(out_path)
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


def find_plots_intersect_boundingbox(bounding_box, all_plots):
    bbox_poly = ogr.CreateGeometryFromJson(str(bounding_box))
    intersecting_plots = dict()

    for plotname in all_plots:
        bounds = all_plots[plotname]
        yaml_bounds = yaml.safe_load(bounds)
        current_poly = ogr.CreateGeometryFromJson(str(yaml_bounds))
        intersection_with_bounding_box = bbox_poly.Intersection(current_poly)

        if intersection_with_bounding_box is not None:
            intersection = json.loads(intersection_with_bounding_box.ExportToJson())
            if 'coordinates' in intersection and len(intersection['coordinates']) > 0:
                intersecting_plots[plotname] = intersection

    return intersecting_plots
