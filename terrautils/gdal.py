"""
This module provides a function for clipping a raster by a
vector layer. Code is largely pulled from the following:

      http://pcjericks.github.io/py-gdalogr-cookbook/
          raster_layers.html#clip-a-geotiff-with-shapefile
"""

import os
from osgeo import gdal, gdalnumeric, ogr
from PIL import Image, ImageDraw
import numpy as np
import json


def extract_polygon(geojson, index=0):
    """Extract Polygon from MultiPolygon."""

    boundary = json.loads(geojson)
    if boundary['type']=='MultiPolygon':
        boundary['type']='Polygon'
        boundary['coordinates']=boundary['coordinates'][index]
    boundary = json.dumps(boundary)

    return boundary


def array_to_image(a):
    """Converts a gdalnumeric array to a PIL Image."""

    i = Image.fromstring('L',(a.shape[1], a.shape[0]),
        (a.astype('b')).tostring())
    return i


def image_to_array(i):
    """Converts a PIL array to a gdalnumeric image."""

    a = gdalnumeric.fromstring(i.tobytes(), 'b')
    a.shape = i.im.size[1], i.im.size[0]
    return a


def world_to_pixel(geo_matrix, x, y):
    """Use GDAL GeoTransform to calculate pixel location."""

    ulX = geo_matrix[0]
    ulY = geo_matrix[3]
    xDist = abs(geo_matrix[1])
    yDist = abs(geo_matrix[5])
    rtnX = geo_matrix[2]
    rtnY = geo_matrix[4]
    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / yDist)
    return (pixel, line)


def pixel_to_world(geo_matrix, x, y):
    """ Calculate new GeoTransform from x and y offset."""

    gt = list(geo_matrix)
    gt[0] = geo_matrix[0] + geo_matrix[1] * x
    gt[3] = geo_matrix[3] + geo_matrix[5] * y

    return gt


def get_layer(fname):    
    """Read features and return layer.

    Args:
      fname (str): filename or geojson string
    
    Returns:
      (layer): an OGR layer object
    """

    features = ogr.Open(fname)

    # if features is a shapefile extract layer by name
    if features.GetDriver().GetName() == 'ESRI Shapefile':
        name = os.path.split(os.path.splitext(fname)[0])[1]
        layer = features.GetLayer(name)
    else:
        layer = features.GetLayer()
    
    return layer


# TODO: avoid passing the geotransform in and out and ulY for to
# create a more generalized function
def create_feature_mask(width, height, polygon, extents, gt, ulY):
    """Rasterize a polygon to create a mask.

    Map points to pixels for drawing the boundary on a blank 8-bit,
    black and white, mask image. The canvas has the size of 
    width and height, the things that are not on the ploy 
    lines will be filled with 1 and on the poly lines will be 
    filled 0.

    Args:
      width (int): raster width in pixels
      height (int): raster width in pixels
      polygon (OGR Feature): the polygon to be used as boundary
      extents (list int): extents in minx, maxx, miny, maxy
      gt (GeoTransform): a GeoTransform from a source raster
      ulY (float): the y component of the upper left corner

    Returns:
      mask (numpy array): an 8-bit array with 0 or 1
      gt2 (GeoTransform): a geotransform for the mask
     
    """

    minX, maxX, minY, maxY = extents

    # We start from creating a new geomatrix for the image
    gt2 = list(gt)
    gt2[0] = minX
    gt2[3] = maxY
    points = []
    pixels = []
    geom = polygon.GetGeometryRef()
    pts = geom.GetGeometryRef(0)
    for p in range(pts.GetPointCount()):
        points.append((pts.GetX(p), pts.GetY(p)))
    for p in points:
        pixels.append(world_to_pixel(gt2, p[0], p[1]))
    raster_poly = Image.new('L', (width, height), 1)
    rasterize = ImageDraw.Draw(raster_poly)
    rasterize.polygon(pixels, 0) # Fill with zeroes

    if gt[3] < maxY:
        premask = image_to_array(raster_poly)
        mask = np.ndarray((premask.shape[-2] - abs(iY), 
                premask.shape[-1]), premask.dtype)
        mask[:] = premask[abs(iY):, :]
        mask.resize(premask.shape) # Then fill in from the bottom
        gt2[3] = maxY - (maxY - gt[3])
    else:
        mask = image_to_array(raster_poly)

    return (mask, gt2)


def get_raster_extents(fname):
    """Calculate the extent and the center of the given raster."""

    src = gdal.Open(fname)
    ulx, xres, xskew, uly, yskew, yres = src.GetGeoTransform()
    lrx = ulx + (src.RasterXSize * xres)
    lry = uly + (src.RasterYSize * yres)
    extent = (ulx, lry, lrx, uly)
    center = ((ulx+lrx)/2, (uly+lry)/2)

    return (extent, center)


def clip_raster(rast_path, features, nodata=-9999):
    """Clip raster to polygon.

    Args:
      rast_path (str): path to raster file
      features (str): path to features file or geojson string
      nodata: the no data value

    Returns: (numpy array, GeoTransform)

    Notes: Oddly, the "features" can be either a filename
      OR a geojson string. GDAL seems to figure it out and do
      the right thing.
    """

    rast = gdal.Open(rast_path)
    gt = rast.GetGeoTransform()

    layer = get_layer(features)
    poly = layer.GetNextFeature()
    extents = layer.GetExtent()
    minX, maxX, minY, maxY = extents

    ulX, ulY = world_to_pixel(gt, minX, maxY)
    lrX, lrY = world_to_pixel(gt, maxX, minY)
    pxWidth = abs(int(lrX - ulX))
    pxHeight = abs(int(lrY - ulY))

    # If the clipping features extend out-of-bounds and 
    # ABOVE the raster...
    # We don't want negative values
    if gt[3] < maxY:
        iY = ulY
        ulY = 0

    clip = rast.ReadAsArray(ulX, ulY, pxWidth, pxHeight)

    mask, gt2 = create_feature_mask(pxWidth, pxHeight, poly, 
                                    extents, gt, iY)

    # Clip the image using the mask, no data is used to fill
    # the unbounded areas.
    try:
        clip = gdalnumeric.choose(mask, (clip, nodata))

    # If the clipping features extend out-of-bounds and 
    # BELOW the raster...
    except ValueError:

        # We have to cut the clipping features to the raster!
        rshp = list(mask.shape)
        if mask.shape[-2] != clip.shape[-2]:
            rshp[0] = clip.shape[-2]

        if mask.shape[-1] != clip.shape[-1]:
            rshp[1] = clip.shape[-1]

        mask.resize(*rshp, refcheck=False)
        clip = gdalnumeric.choose(mask, (clip, nodata))

    return clip, gt2
