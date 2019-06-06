"""Contains routines specific to image files
"""
import logging
import subprocess

from osgeo import gdal, ogr
from numpy import nan

import osr

from terrautils.extractors import file_exists, load_json_file

# Returns if the MIME type of 'image' is found in the text passed in. A reasonable effort is
# made to identify the section containing the type by looking for the phrase 'Mime', or 'mime',
# or 'MIME' and using that as the basis for determining the type
def find_image_mime_type(text):
    """Looks for a MIME image type in the text passed in.

       It's expected that the MIME label may be something other than the string 'Mime type' so
       a proximity search is made for the MIME type label followed by 'image/'. If found, True
       is returned and False otherwise

    Args:
        text(str): The text in which to find a MIME type of 'image'

    Returns:
        None is returned if the string is empty or the MIME keyword is not found.
        False is returned if a MIME type of 'image' isn't found
        True is returned upon success
    """
    if not text:
        return None

    # Try to find the beginning and end of the mime type (but not the subtype)
    pos = text.find('Mime')
    if pos < 0:
        pos = text.find('mime')
    if pos < 0:
        pos = text.find('MIME')
    if pos < 0:
        return None

    end_pos = text.find('/', pos)
    if end_pos < 0:
        return False

    # Get the portion of the string containing the possible mime type making sure we have
    # something reasonable
    mime = text[pos : end_pos]
    mime_len = len(mime)
    if (mime_len > 50) or (mime.find('\n') >= 0) or (mime.find('\r') >= 0):
        return False

    # Look for a 'reasonable' image mime type
    if mime.endswith('image'):
        return True

    return False

# Determines if the file is an image type
def file_is_image_type(identify_binary, filename, metadata_filename=None):
    """Uses the identify application to generate the MIME type of the file and
       looks for an image MIME type. If a metadata filename is specified, the
       JSON in the file is loaded first and the MIME type is looked for. If
       the metadata filename is not specified, or a MIME type was not found in
       the metadata, the identity application is used.

    Args:
        identify_binary(str): path to the executable which will return a MIME type on an image file
        filename(str): the path to the file to check
        metadata_filename(str): the path to JSON metadata associated with the file in which to look
        for a 'contentType' tag containing the MIME type

    Returns:
        True is returned if the file is a MIME image type
        False is returned upon failure or the file is not a type of image
    """
    logger = logging.getLogger(__name__)

    # Try to determine the file type from its JSON information (metadata if from Clowder API)
    try:
        if metadata_filename and file_exists(metadata_filename):
            file_md = load_json_file(metadata_filename)
            if file_md:
                if 'contentType' in file_md:
                    if file_md['contentType'].startswith('image'):
                        return True
    # pylint: disable=broad-except
    except Exception as ex:
        logger.info("Exception caught: %s", str(ex))
    # pylint: enable=broad-except

    # Try to determine the file type locally
    try:
        is_image_type = find_image_mime_type(
            subprocess.check_output(
                [identify_binary, "-verbose", filename], stderr=subprocess.STDOUT))

        if not is_image_type is None:
            return is_image_type
    # pylint: disable=broad-except
    except Exception as ex:
        logger.info("Exception caught: %s", str(ex))
    # pylint: enable=broad-except

    return False

# Checks if the file has geometry associated with it and returns the bounds
def image_get_geobounds(filename):
    """Uses gdal functionality to retrieve recilinear boundaries from the file

    Args:
        filename(str): path of the file to get the boundaries from

    Returns:
        The upper-left and calculated lower-right boundaries of the image in a list upon success.
        The values are returned in following order: min_y, max_y, min_x, max_x. A list of numpy.nan
        is returned if the boundaries can't be determined
    """
    logger = logging.getLogger(__name__)

    try:
        # TODO: handle non-ortho images
        src = gdal.Open(filename)
        ulx, xres, _, uly, _, yres = src.GetGeoTransform()
        lrx = ulx + (src.RasterXSize * xres)
        lry = uly + (src.RasterYSize * yres)

        min_y = min(uly, lry)
        max_y = max(uly, lry)
        min_x = min(ulx, lrx)
        max_x = max(ulx, lrx)

        return [min_y, max_y, min_x, max_x]
    # pylint: disable=broad-except
    except Exception as ex:
        logger.info("[image_get_geobounds] Exception caught: %s", str(ex))
    # pylint: enable=broad-except

    return [nan, nan, nan, nan]

# Get the tuple from the passed in polygon
def polygon_to_tuples(polygon):
    """Convert polygon passed in to the following list:
        ( lat (y) min, lat (y) max,
          long (x) min, long (x) max)

    Args:
        polygon(object) - OGR Polygon (type ogr.wkbPolygon)

    Return:
        A tuple of (min Y, max Y, min X, max X)
    """
    logger = logging.getLogger(__name__)

    min_x, min_y, max_x, max_y = None, None, None, None
    try:
        if polygon.GetGeometryType() == ogr.wkbPolygon:
            ring = polygon.GetGeometryRef(0)
            point_count = ring.GetPointCount()
            for point_idx in xrange(point_count):
                pt_x, pt_y, _ = ring.GetPoint(point_idx)
                if min_x is None or pt_x < min_x:
                    min_x = pt_x
                if max_x is None or pt_x > max_x:
                    max_x = pt_x
                if min_y is None or pt_y < min_y:
                    min_y = pt_y
                if max_y is None or pt_y > max_y:
                    max_y = pt_y
    # pylint: disable=broad-except
    except Exception as ex:
        logger.warn("[polygon_to_tuples] Exception caught: %s", str(ex))
    # pylint: enable=broad-except

    return (min_y, max_y, min_x, max_x)

def polygon_to_tuples_transform(polygon, dest_spatial):
    """Transforms the polygon to the specified transformation if the polygon's coordinate system
       doesn't match. If there isn't a coordinate system associated with the polygon then no
       transformation is done.
    Args:
        polygon(object) - OGR Polygon (type ogr.wkbPolygon)
        dest_spatial(ogr.spatialReference): the spatial reference of target polygon values
    Return:
        A tuple of (min Y, max Y, min X, max X)
    Notes:
        The original polygon is unchanged whether or not a transformation takes place
    """
    logger = logging.getLogger(__name__)

    try:
        src_ref = polygon.GetSpatialReference()
        if src_ref and dest_spatial and not src_ref.IsSame(dest_spatial):
            transform = osr.CoordinateTransformation(src_ref, dest_spatial)
            new_src = polygon.Clone()
            if new_src:
                new_src.Transform(transform)
                return polygon_to_tuples(new_src)
            logger.error("[polygon_to_tuples_transform] Unable to create a polygon copy for " +
                         "coordinate transformation")
    # pylint: disable=broad-except
    except Exception as ex:
        logger.warn("[polygon_to_tuples_transform] Exception caught: %s", str(ex))
        logger.warn("[polygon_to_tuples_transform] returning non-transformed polygon points")
    # pylint: enable=broad-except

    return polygon_to_tuples(polygon)

def get_epsg(filename):
    """Returns the EPSG of the georeferenced image file
    Args:
        filename(str): path of the file to retrieve the EPSG code from
    Return:
        Returns the found EPSG code, or None if it's not found or an error ocurred
    """
    logger = logging.getLogger(__name__)

    try:
        src = gdal.Open(filename)

        proj = osr.SpatialReference(wkt=src.GetProjection())

        return proj.GetAttrValue('AUTHORITY', 1)
    # pylint: disable=broad-except
    except Exception as ex:
        logger.warn("[get_epsg] Exception caught: %s", str(ex))
    # pylint: enable=broad-except

    return None
