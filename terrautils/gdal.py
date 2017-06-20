import os
from StringIO import StringIO
from osgeo import gdal, gdalnumeric, ogr
from PIL import Image, ImageDraw
import numpy as np

def array_to_image(a):
    """ Converts a gdalnumeric array to a Python Imaging Library (PIL) Image.
    """
    i = Image.fromstring('L', (a.shape[1], a.shape[0]),
                         (a.astype('b')).tostring())
    return i

def image_to_array(i):
    """ Converts a Python Imaging Library (PIL) array to a gdalnumeric image.
    """
    a = gdalnumeric.fromstring(i.tobytes(), 'b')
    a.shape = i.im.size[1], i.im.size[0]
    return a

def world_to_pixel(geo_matrix, x, y):
    """ Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the pixel location of a geospatial coordinate;

      From:
        http://pcjericks.github.io/py-gdalogr-cookbook/raster_layers.html
        #clip-a-geotiff-with-shapefile
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
    # This one probably a bug
    # In the geo_matrix the yDist appears to be negative
    # which doesn't make any fucking sense
    line = int((ulY - y) / yDist)
    return (pixel, line)


def clip_raster(rast_path, features_path, gt=None, nodata=-9999):
    """ Clip a raster and return the clipped result in form of numpy array

    Args:
      rast_path (str): path to source raster
      feature_path (str): path to feature file OR geojson string
      gt (geomatrix): provides geotransform if source raster is numpy array
      nodata (int): nodata value for areas outside feature

    Returns:
      (numpy array): clipped raster

    Notes:
      1) clips against a single feature
      2) feature_path may be a geojson string?!
      3) raster provided as numpy array must pass a geotransform?
      4) more?
      
    """

    # Since in the file what we have  is just a path instead of the file
    rast = gdal.Open(rast_path)
    # Can accept either a gdal.Dataset or numpy.array instance
    if not isinstance(rast, np.ndarray):
        gt = rast.GetGeoTransform()
        rast = rast.ReadAsArray()

    # Create an OGR layer from a boundary shapefile

    """
    Does that mean all I need to do is just get the driver?
    """
    # TODO: Update the gdal version
    #features = ogr.Open(open(features_path).read())
    features = ogr.Open(features_path)
    if features.GetDriver().GetName() == 'ESRI Shapefile':
        lyr = features.GetLayer(os.path.split(
                                os.path.splitext(features_path)[0])[1])
    else:
        lyr = features.GetLayer()

    # TODO: is this best approach? Perhaps always clip to layer extent
    # Get the first feature
    poly = lyr.GetNextFeature()

    # TODO clipping to the layer extent rather than the feature extent
    # Convert the layer extent to image pixel coordinates
    minX, maxX, minY, maxY = lyr.GetExtent()
    ulX, ulY = world_to_pixel(gt, minX, maxY)
    lrX, lrY = world_to_pixel(gt, maxX, minY)

    # print minX, maxX, minY, maxY
    # print ulX, ulY, lrX, lrY
    # Calculate the pixel size of the new image
    pxWidth = int(lrX - ulX)
    pxHeight = int(lrY - ulY)

    # If the clipping features extend out-of-bounds and ABOVE the raster...
    if gt[3] < maxY:
        # In such a case... ulY ends up being negative--can't have that!
        iY = ulY
        ulY = 0

    # Multi-band image?
    try:
        clip = rast[:, ulY:lrY, ulX:lrX]

    except IndexError:
        clip = rast[ulY:lrY, ulX:lrX]

    # Create a new geomatrix for the image
    gt2 = list(gt)
    gt2[0] = minX
    gt2[3] = maxY

    # Map points to pixels for drawing the boundary on a blank 8-bit,
    #   black and white, mask image.
    points = []
    pixels = []
    geom = poly.GetGeometryRef()
    pts = geom.GetGeometryRef(0)

    for p in range(pts.GetPointCount()):
        points.append((pts.GetX(p), pts.GetY(p)))

    for p in points:
        pixels.append(world_to_pixel(gt2, p[0], p[1]))

    raster_poly = Image.new('L', (pxWidth, pxHeight), 1)
    rasterize = ImageDraw.Draw(raster_poly)
    rasterize.polygon(pixels, 0)  # Fill with zeroes

    # If the clipping features extend out-of-bounds and ABOVE the raster...
    if gt[3] < maxY:
        # The clip features were "pushed down" to match the bounds of the
        #   raster; this step "pulls" them back up
        premask = image_to_array(raster_poly)
        # We slice out the piece of our clip features that are "off the map"
        mask = np.ndarray((premask.shape[-2] - abs(iY),
                           premask.shape[-1]), premask.dtype)
        mask[:] = premask[abs(iY):, :]
        mask.resize(premask.shape)  # Then fill in from the bottom

        # Most importantly, push the clipped piece down
        gt2[3] = maxY - (maxY - gt[3])

    else:
        mask = image_to_array(raster_poly)

    # Clip the image using the mask
    try:
        # No data is used in here to fill the unbounded areas
        clip = gdalnumeric.choose(mask, (clip, nodata))

    # If the clipping features extend out-of-bounds and BELOW the raster...
    except ValueError:
        # We have to cut the clipping features to the raster!
        rshp = list(mask.shape)
        if mask.shape[-2] != clip.shape[-2]:
            rshp[0] = clip.shape[-2]

        if mask.shape[-1] != clip.shape[-1]:
            rshp[1] = clip.shape[-1]

        mask.resize(*rshp, refcheck=False)

        clip = gdalnumeric.choose(mask, (clip, nodata))

    # return (clip, ulX, ulY, gt2)
    # print clip
    # print gt2
    return clip


# TODO: rename to get_raster_extents
# TODO: add comment
def get_tif_info(fname):

    src = gdal.Open(fname)
    ulx, xres, xskew, uly, yskew, yres = src.GetGeoTransform()
    lrx = ulx + (src.RasterXSize * xres)
    lry = uly + (src.RasterYSize * yres)
    extent = (ulx, lry, lrx, uly)
    center = ((ulx+lrx)/2, (uly+lry)/2)

    return (extent, center)
