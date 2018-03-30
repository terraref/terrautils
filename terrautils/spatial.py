"""Spatial

This module provides useful methods for spatial referencing.
"""

import utm
from osgeo import ogr


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


def scanalyzer_to_mac(scan_x, scan_y):
    # TODO: Hard-coded
    # Linear transformation coefficients
    ay = 3659974.971; by = 1.0002; cy = 0.0078;
    ax = 409012.2032; bx = 0.009; cx = - 0.9986;

    mac_x = ax + (bx * scan_x) + (cx * scan_y)
    mac_y =  ay + (by * scan_x) + (cy * scan_y)

    return mac_x, mac_y


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
    Mx_nw, My_nw = scanalyzer_to_mac(x_n, y_w)
    # coordinates if southeast bounding box vertex
    Mx_se, My_se = scanalyzer_to_mac(x_s, y_e)

    # Get UTM information from southeast corner of field
    SE_utm = utm.from_latlon(33.07451869, -111.97477775)
    utm_zone = SE_utm[2]
    utm_num  = SE_utm[3]
    # bounding box vertex coordinates
    bbox_nw_latlon = utm.to_latlon(Mx_nw, My_nw, utm_zone, utm_num)
    bbox_se_latlon = utm.to_latlon(Mx_se, My_se, utm_zone, utm_num)

    # TODO: Hard-coded
    lon_shift = 0.000020308287
    lat_shift = 0.000015258894
    return ( bbox_se_latlon[0] - lat_shift,
             bbox_nw_latlon[0] - lat_shift,
             bbox_nw_latlon[1] + lon_shift,
             bbox_se_latlon[1] + lon_shift )
