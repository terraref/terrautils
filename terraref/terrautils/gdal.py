"""GDAL

This module provides wrappers to GDAL for manipulating geospatial data.
"""

import os
from osgeo import gdal, gdalnumeric, ogr
from PIL import Image, ImageDraw
import numpy as np


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
    """Use GDAL GeoTransform to calculate pixel location.

    Notes:
      http://pcjericks.github.io/py-gdalogr-cookbook/\
          raster_layers.html#clip-a-geotiff-with-shapefile
    """
    ulX = geo_matrix[0]
    ulY = geo_matrix[3]
    xDist = geo_matrix[1]
    yDist = geo_matrix[5]
    rtnX = geo_matrix[2]
    rtnY = geo_matrix[4]
    if xDist < 0:
        xDist = -xDist
    if yDist < 0:
        yDist = -yDist
    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / yDist)
    return (pixel, line)


def pixel_to_world(geo_matrix, x, y):
    """ Calculate new GeoTransform from x and y offset. """
    gt = list(geo_matrix)
    gt[0] = geo_matrix[0] + geo_matrix[1] * x
    gt[3] = geo_matrix[3] + geo_matrix[5] * y

    return gt


def clip_raster(rast_path, features_path, nodata=-9999):
    """Clip raster to polygon.

    Args:
      rast_path (str): path to raster file
      features_path (str): path to features file or geojson string
      nodata: the no data value

    Returns: (numpy array, GeoTransform)

    Notes: Oddly, the "features path" can be either a filename
      OR a geojson string. GDAL seems to figure it out and do
      the right thing.
    """

    rast = gdal.Open(rast_path)
    band = rast.GetRasterBand(1)
    rast_xsize = band.XSize
    rast_ysize = band.YSize
    gt = rast.GetGeoTransform()

    #Open Features and get all the necessary data for clipping
    #features = ogr.Open(open(features_path).read())
    features = ogr.Open(features_path)
    if features.GetDriver().GetName() == 'ESRI Shapefile':
        lyr = features.GetLayer(os.path.split(
                os.path.splitext(features_path)[0])[1])
    else:
        lyr = features.GetLayer()

    poly = lyr.GetNextFeature()
    minX, maxX, minY, maxY = lyr.GetExtent()
    ulX, ulY = world_to_pixel(gt, minX, maxY)
    lrX, lrY = world_to_pixel(gt, maxX, minY)
    pxWidth = int(lrX - ulX)
    pxHeight = int(lrY - ulY)
    if pxWidth < 0:
        pxWidth = -pxWidth
    if pxHeight < 0:
        pxHeight = -pxHeight

    # If the clipping features extend out-of-bounds and 
    # ABOVE the raster...
    # We don't want negative values
    if gt[3] < maxY:
        iY = ulY
        ulY = 0

    # Ensure bounding box doesn't exceed the boundary of the geoTIFF
    if ulX < 0: ulX = 0
    if ulY < 0: ulY = 0
    if ulX + pxWidth > rast_xsize:
        pxWidth = rast_xsize - ulX
    if ulY + pxHeight > rast_ysize:
        pxHeight = rast_ysize - ulY

    clip = rast.ReadAsArray(ulX, ulY, pxWidth, pxHeight)

    # Map points to pixels for drawing the boundary on a blank 8-bit,
    # black and white, mask image. The canvas has the size of 
    # pixWidth and pixHeight, the things that are not on the ploy 
    # lines will be filled with 1 and on the poly lines will be 
    # filled 0.

    # We start from creating a new geomatrix for the image
    gt2 = list(gt)
    gt2[0] = minX
    gt2[3] = maxY
    pixels = []
    geom = poly.GetGeometryRef()
    pts = geom.GetGeometryRef(0)
    while pts.GetPointCount() == 0:
        pts = pts.GetGeometryRef(0)
    for p in range(pts.GetPointCount()):
        pixels.append(world_to_pixel(gt2, pts.GetX(p), pts.GetY(p)))

    raster_poly = Image.new('L', (pxWidth, pxHeight), 1)
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

    # TODO: I think this is provided above as gt2
    # newGT = pixel_to_world(gt, ulX, ulY)
    return clip, gt2


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
