"""Spatial

This module provides useful methods for spatial referencing.
"""

import os
import utm
import yaml
import json
import subprocess
import numpy as np
import laspy
from osgeo import gdal, gdalnumeric, ogr



def calculate_bounding_box(gps_bounds, z_value=0):
    """Given a set of GPS boundaries, return array of 4 vertices representing the polygon.

    gps_bounds -- (lat(y) min, lat(y) max, long(x) min, long(x) max)

    Returns:
        List of 4 polygon vertices [ul, ur, lr, ll] without z values.
    """

    return [
        (gps_bounds[2], gps_bounds[1], z_value), # upper-left
        (gps_bounds[3], gps_bounds[1], z_value), # upper-right
        (gps_bounds[3], gps_bounds[0], z_value), # lower-right
        (gps_bounds[2], gps_bounds[0], z_value)  # lower-left
    ]


def calculate_centroid(gps_bounds):
    """Given a set of GPS boundaries, return lat/lon of centroid.

    gps_bounds -- (lat(y) min, lat(y) max, long(x) min, long(x) max)

    Returns:
        Tuple of (lat, lon) representing centroid
    """

    return (
        gps_bounds[0] + float(gps_bounds[1] - gps_bounds[0])/2,
        gps_bounds[2] + float(gps_bounds[3] - gps_bounds[2])/2,
    )


def calculate_centroid_from_wkt(wkt):
    """Given WKT, return lat/lon of centroid.

    wkt -- string

    returns:
        Tuple of (lat, lon) representing centroid
    """

    loc_geom = ogr.CreateGeometryFromWkt(wkt)
    return (
        loc_geom.Centroid().GetX(),
        loc_geom.Centroid().GetY()
    )


def calculate_gps_bounds(metadata, sensor="stereoTop"):
    """Extract bounding box geometry, depending on sensor type.

        Gets geometry from metadata for center position and FOV, applies some
        sensor-specific transformations to those values, then uses them in formula
        to determine bounding box of image.

        Returns:
            tuple of GeoTIFF coordinates, each one as:
            (lat(y) min, lat(y) max, long(x) min, long(x) max)
    """
    gantry_x, gantry_y, gantry_z, cambox_x, cambox_y, cambox_z, fov_x, fov_y = geom_from_metadata(metadata)

    center_position = ( float(gantry_x) + float(cambox_x),
                        float(gantry_y) + float(cambox_y),
                        float(gantry_z) + float(cambox_z) )
    cam_height = center_position[2]

    if sensor=="stereoTop":
        var_se = float(metadata['sensor_fixed_metadata']['slope_estimation'])
        var_rho = float(metadata['sensor_fixed_metadata']['rail_height_offset'])
        var_sofc = float(metadata['sensor_fixed_metadata']['stereo_offsets_from_center'])

        # Use height of camera * slope_estimation to estimate expected canopy height
        predicted_plant_height = var_se * cam_height
        # Subtract expected plant height from (cam height + rail height offset) to get canopy height
        cam_height_above_canopy = float(cam_height + var_rho - predicted_plant_height)
        fov_x = float(fov_x) * (cam_height_above_canopy/2)
        fov_y = float(fov_y) * (cam_height_above_canopy/2)
        # Account for experimentally determined distance from center to each stereo lens for left/right
        left_position = [center_position[0]-var_sofc, center_position[1], center_position[2]]
        right_position = [center_position[0]+var_sofc, center_position[1], center_position[2]]
        # Return two separate bounding boxes for left/right
        left_gps_bounds = _get_bounding_box_with_formula(left_position, [fov_x, fov_y])
        right_gps_bounds = _get_bounding_box_with_formula(right_position, [fov_x, fov_y])
        return { "left" : left_gps_bounds, "right" : right_gps_bounds }

    elif sensor=="flirIrCamera":
        cam_height_above_canopy = cam_height + float(metadata['sensor_fixed_metadata']['rail_height_offset'])
        fov_x = float(fov_x) * (cam_height_above_canopy/2)
        fov_y = float(fov_y) * (cam_height_above_canopy/2)

    elif sensor=='scanner3DTop':
        # Default geom refers to west side, so get east side cambox as well
        gx, gy, gz, e_cambox_x, e_cambox_y, e_cambox_z, fx, fy = geom_from_metadata(metadata, 'east')

        # Swap X and Y because we rotate 90 degress
        fov_x = float(fov_y) if fov_y else 0
        scan_distance = float(metadata['sensor_variable_metadata']['scan_distance_mm'])/1000
        fov_y = scan_distance
        scandirection = int(metadata['sensor_variable_metadata']['scan_direction'])

        # TODO: These constants should live in fixed metadata once finalized
        if scandirection == 0: # Negative scan
            west_position = ( float(gantry_x) + float(cambox_x) + 0.082,
                              float(gantry_y) + float(2*float(cambox_y)) - scan_distance/2 - 4.363, #Might be less than this
                              float(gantry_z) + float(cambox_z) )

            east_position = ( float(gantry_x) + float(e_cambox_x) + 0.082,
                              float(gantry_y) + float(2*float(e_cambox_y)) - scan_distance/2 - 0.354,
                              float(gantry_z) + float(e_cambox_z) )
        else: # Positive scan
            west_position = ( float(gantry_x) + float(cambox_x) + 0.082,
                              float(gantry_y) + float(2*float(cambox_y)) + scan_distance/2 - 4.23,
                              float(gantry_z) + float(cambox_z) )

            east_position = ( float(gantry_x) + float(e_cambox_x) + 0.082,
                              float(gantry_y) + float(2*float(e_cambox_y)) + scan_distance/2 + 0.4,
                              float(gantry_z) + float(e_cambox_z) )

        east_gps_bounds = _get_bounding_box_with_formula(east_position, [fov_x, fov_y])
        west_gps_bounds = _get_bounding_box_with_formula(west_position, [fov_x, fov_y])
        return { "east" : east_gps_bounds, "west" : west_gps_bounds }

    else:
        fov_x = float(fov_x) if fov_x else 0
        fov_y = float(fov_y) if fov_y else 0

    return { sensor : _get_bounding_box_with_formula(center_position, [fov_x, fov_y]) }


def centroid_from_geojson(geojson):
    """Return centroid lat/lon of a geojson object."""
    geom_poly = ogr.CreateGeometryFromJson(geojson)
    centroid = geom_poly.Centroid()

    return centroid.ExportToJson()


def clip_las(las_path, tuples, out_path, merged_path=None):
    """Clip LAS file to polygon.

    Args:
      las_path (str): path to pointcloud file
      geojson (str): geoJSON bounds received from get_site_boundaries() in betydb.py
      out_path: output file to write
    """
    utm = tuples_to_utm(tuples)
    bounds_str = "([%s, %s], [%s, %s])" % (utm[2], utm[3], utm[0], utm[1])

    pdal_dtm = out_path.replace(".las", "_dtm.json")
    with open(pdal_dtm, 'w') as dtm:
        dtm.write("""{
            "pipeline": [
                "%s",
                {
                    "type": "filters.crop",
                    "bounds": "%s"
                },
                {
                    "type": "writers.las",
                    "filename": "%s"
                }
            ]
        }""" % (las_path, bounds_str, out_path))

    cmd = 'pdal pipeline "%s"' % pdal_dtm
    subprocess.call([cmd], shell=True)
    os.remove(pdal_dtm)

    if merged_path:
        if os.path.isfile(merged_path):
            cmd = 'pdal merge "%s" "%s" "%s"' % (out_path, merged_path, merged_path)
            subprocess.call([cmd], shell=True)
        else:
            os.rename(out_path, merged_path)


def clip_raster(rast_path, bounds, out_path=None, nodata=-9999, compress=False):
    """Clip raster to polygon.

    Args:
      rast_path (str): path to raster file
      bounds (tuple): (min_y, max_y, min_x, max_x)
      out_path: if provided, where to save as output file
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
    if compress:
        cmd = 'gdal_translate -projwin %s "%s" "%s"' % (coords, rast_path, out_path)
    else:
        cmd = 'gdal_translate -co COMPRESS=LZW -projwin %s "%s" "%s"' % (coords, rast_path, out_path)
    subprocess.call(cmd, shell=True, stdout=open(os.devnull, 'wb'))
    out_px = np.array(gdal.Open(out_path).ReadAsArray())

    if np.count_nonzero(out_px) > 0:
        if out_path == "temp.tif":
            os.remove(out_path)
        return out_px
    else:
        os.remove(out_path)
        return None


def find_plots_intersect_boundingbox(bounding_box, all_plots, fullmac=True):
    """Take a list of plots from BETY and return only those overlapping bounding box.

    fullmac -- only include full plots (omit KSU, omit E W partial plots)

    """
    bbox_poly = ogr.CreateGeometryFromJson(str(bounding_box))
    intersecting_plots = dict()

    for plotname in all_plots:
        if fullmac and (plotname.find("KSU") > -1 or plotname.endswith(" E") or plotname.endswith(" W")):
            continue

        bounds = all_plots[plotname]

        yaml_bounds = yaml.safe_load(bounds)
        current_poly = ogr.CreateGeometryFromJson(str(yaml_bounds))
        intersection_with_bounding_box = bbox_poly.Intersection(current_poly)

        if intersection_with_bounding_box is not None:
            intersection = json.loads(intersection_with_bounding_box.ExportToJson())
            if 'coordinates' in intersection and len(intersection['coordinates']) > 0:
                intersecting_plots[plotname] = bounds

    return intersecting_plots


def geojson_to_tuples(bounding_box):
    """
    Given a GeoJSON polygon, returns in tuple format
     (lat(y) min, lat(y) max, long(x) min, long(x) max)
    """
    lat_max = bounding_box["coordinates"][0][0]
    long_min = bounding_box["coordinates"][0][1]
    long_max = bounding_box["coordinates"][1][1]
    lat_min = bounding_box["coordinates"][2][0]
    
    return (lat_min, lat_max, long_min, long_max)


def geojson_to_tuples_betydb(bounding_box):
    """Convert GeoJSON from BETYdb to
        ( lat (y) min, lat (y) max,
          long (x) min, long (x) max) for geotiff creation"""
    min_x, min_y, max_x, max_y  = None, None, None, None

    if isinstance(bounding_box, dict):
        bounding_box = bounding_box["coordinates"]

    for coord in bounding_box[0][0]:
        if not min_x or coord[0] < min_x:
            min_x = coord[0]
        if not max_x or coord[0] > max_x:
            max_x = coord[0]
        if not min_y or coord[1] < min_y:
            min_y = coord[1]
        if not max_y or coord[1] > max_y:
            max_y = coord[1]

    return (min_y, max_y, min_x, max_x)


def geom_from_metadata(metadata, side='west'):
    """Parse location elements from metadata.

        Returns:
        tuple of location information: (
            location of scannerbox x, y, z
            location offset of sensor in scannerbox in x, y, z
            field-of-view of camera in x, y dimensions
        )
    """
    gantry_x, gantry_y, gantry_z = None, None, None
    cambox_x, cambox_y, cambox_z = None, None, None
    fov_x, fov_y = None, None

    if 'terraref_cleaned_metadata' in metadata and metadata['terraref_cleaned_metadata']:
        gv_meta = metadata['gantry_variable_metadata']
        gantry_x = gv_meta['position_m']['x'] if 'x' in gv_meta['position_m'] else gantry_x
        gantry_y = gv_meta['position_m']['y'] if 'y' in gv_meta['position_m'] else gantry_y
        gantry_z = gv_meta['position_m']['z'] if 'z' in gv_meta['position_m'] else gantry_z

        if 'sensor_fixed_metadata' in metadata:
            sf_meta = metadata['sensor_fixed_metadata']

        # LOCATION IN CAMERA BOX
        if 'location_in_camera_box_m' in sf_meta:
            cambox_x = sf_meta['location_in_camera_box_m']['x']
            cambox_y = sf_meta['location_in_camera_box_m']['y']
            cambox_z = sf_meta['location_in_camera_box_m']['z'] if 'z' in sf_meta['location_in_camera_box_m'] else 0
        elif side=='west' and 'scanner_west_location_in_camera_box_m' in sf_meta:
            cambox_x = sf_meta['scanner_west_location_in_camera_box_m']['x']
            cambox_y = sf_meta['scanner_west_location_in_camera_box_m']['y']
            cambox_z = sf_meta['scanner_west_location_in_camera_box_m']['z']
        elif side=='east' and 'scanner_east_location_in_camera_box_m' in sf_meta:
            cambox_x = sf_meta['scanner_east_location_in_camera_box_m']['x']
            cambox_y = sf_meta['scanner_east_location_in_camera_box_m']['y']
            cambox_z = sf_meta['scanner_east_location_in_camera_box_m']['z']

        # FIELD OF VIEW (FOV)
        for fov_field in ['field_of_view_m', 'field_of_view_at_2m_m', 'field_of_view_degrees']:
            if fov_field in sf_meta:
                fov_x = sf_meta[fov_field]['x'] if 'x' in sf_meta[fov_field] else fov_x
                fov_y = sf_meta[fov_field]['y'] if 'y' in sf_meta[fov_field] else fov_y

    return (gantry_x, gantry_y, gantry_z, cambox_x, cambox_y, cambox_z, fov_x, fov_y)


def get_las_extents(fname):
    """Calculate the extent of the given pointcloud and return as GeoJSON."""
    lasinfo = laspy.base.Reader(fname, 'r')
    min = lasinfo.get_header().min
    max = lasinfo.get_header().max

    min_latlon = utm.to_latlon(min[1], min[0], 12, 'S')
    max_latlon = utm.to_latlon(max[1], max[0], 12, 'S')

    return {
        "type": "Polygon",
        "coordinates": [[
            [min_latlon[1], min_latlon[0]],
            [min_latlon[1], max_latlon[0]],
            [max_latlon[1], max_latlon[0]],
            [max_latlon[1], min_latlon[0]],
            [min_latlon[1], min_latlon[0]]
        ]]
    }


def get_raster_extents(fname):
    """Calculate the extent and the center of the given raster."""
    src = gdal.Open(fname)
    ulx, xres, xskew, uly, yskew, yres = src.GetGeoTransform()
    lrx = ulx + (src.RasterXSize * xres)
    lry = uly + (src.RasterYSize * yres)
    extent = (ulx, lry, lrx, uly)
    center = ((ulx+lrx)/2, (uly+lry)/2)

    return (extent, center)


def utm_to_latlon(utm_x, utm_y):
    """Convert coordinates from UTM 12N to lat/lon"""

    # Get UTM information from southeast corner of field
    SE_utm = utm.from_latlon(33.07451869, -111.97477775)
    utm_zone = SE_utm[2]
    utm_num  = SE_utm[3]

    return utm.to_latlon(utm_x, utm_y, utm_zone, utm_num)


def scanalyzer_to_latlon(gantry_x, gantry_y):
    """Convert coordinates from gantry to lat/lon"""
    utm_x, utm_y = scanalyzer_to_utm(gantry_x, gantry_y)
    return utm_to_latlon(utm_x, utm_y)


def scanalyzer_to_utm(gantry_x, gantry_y):
    """Convert coordinates from gantry to UTM 12N"""

    # TODO: Hard-coded
    # Linear transformation coefficients
    ay = 3659974.971; by = 1.0002; cy = 0.0078;
    ax = 409012.2032; bx = 0.009; cx = - 0.9986;

    utm_x = ax + (bx * gantry_x) + (cx * gantry_y)
    utm_y = ay + (by * gantry_x) + (cy * gantry_y)

    return utm_x, utm_y


def tuples_to_geojson(bounds):
    """
    Given bounding box in tuple format, returns GeoJSON polygon

    Input bounds: (lat(y) min, lat(y) max, long(x) min, long(x) max)
    """
    lat_min = bounds[0]
    lat_max = bounds[1]
    long_min = bounds[2]
    long_max = bounds[3]

    bounding_box = {}
    bounding_box["type"] = "Polygon"
    bounding_box["coordinates"]  =  [
        [lat_max, long_min], # NW
        [lat_max, long_max], # NE
        [lat_min, long_max], # SE
        [lat_min, long_min]  # SW
    ]

    return bounding_box


def tuples_to_utm(bounds):
    """Given bounding box in tuple format, returns UTM equivalent

    Input bounds: (lat(y) min, lat(y) max, long(x) min, long(x) max)
    """

    min = utm.from_latlon(bounds[0], bounds[2])
    max = utm.from_latlon(bounds[1], bounds[3])

    return (min[0], max[0], min[1], max[1])


def wkt_to_geojson(wkt):
    geom = ogr.CreateGeometryFromWkt(wkt)
    return geom.ExportToJson()


# PRIVATE -------------------------------------
def _get_bounding_box_with_formula(center_position, fov):
    """Convert scannerbox center position & sensor field-of-view to actual bounding box

        Linear transformation formula adapted from:
        https://terraref.gitbooks.io/terraref-documentation/content/user/geospatial-information.html

        Returns:
            tuple of coordinates as: (  lat (y) min, lat (y) max,
                                        long (x) min, long (x) max )
    """

    # min/max bounding box x,y values
    y_w = center_position[1] + fov[1]/2
    y_e = center_position[1] - fov[1]/2
    x_n = center_position[0] + fov[0]/2
    x_s = center_position[0] - fov[0]/2
    # coordinates of northwest bounding box vertex
    bbox_nw_latlon = scanalyzer_to_latlon(x_n, y_w)
    # coordinates if southeast bounding box vertex
    bbox_se_latlon = scanalyzer_to_latlon(x_s, y_e)

    # TODO: Hard-coded
    lon_shift = 0.000020308287
    lat_shift = 0.000015258894
    return ( bbox_se_latlon[0] - lat_shift,
             bbox_nw_latlon[0] - lat_shift,
             bbox_nw_latlon[1] + lon_shift,
             bbox_se_latlon[1] + lon_shift )
