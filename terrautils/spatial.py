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
        gps_bounds[0] + (gps_bounds[1] - gps_bounds[0]),
        gps_bounds[2] + (gps_bounds[3] - gps_bounds[2]),
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


def calculate_gps_bounds(metadata, sensor="stereoTop", side='west'):
    """Extract bounding box geometry, depending on sensor type.

        Gets geometry from metadata for center position and FOV, applies some
        sensor-specific transformations to those values, then uses them in formula
        to determine bounding box of image.

        Returns:
            tuple of GeoTIFF coordinates, each one as:
            (lat(y) min, lat(y) max, long(x) min, long(x) max)
    """
    gantry_x, gantry_y, gantry_z, cambox_x, cambox_y, cambox_z, fov_x, fov_y = geom_from_metadata(metadata, side=side)

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
        left_position = [center_position[0]+var_sofc, center_position[1], center_position[2]]
        right_position = [center_position[0]-var_sofc, center_position[1], center_position[2]]
        # Return two separate bounding boxes for left/right
        left_gps_bounds = _get_bounding_box_with_formula(left_position, [fov_x, fov_y])
        right_gps_bounds = _get_bounding_box_with_formula(right_position, [fov_x, fov_y])
        return { "left" : left_gps_bounds, "right" : right_gps_bounds }

    elif sensor=="flirIrCamera":
        cam_height_above_canopy = cam_height + float(metadata['sensor_fixed_metadata']['rail_height_offset'])
        fov_x = float(fov_x) * (cam_height_above_canopy/2)
        fov_y = float(fov_y) * (cam_height_above_canopy/2)

    elif sensor=='scanner3DTop':
        # Swap X and Y because we rotate 90 degress
        fov_x = float(fov_y) if fov_y else 0
        fov_y = 12

    else:
        fov_x = float(fov_x) if fov_x else 0
        fov_y = float(fov_y) if fov_y else 0

    return { sensor : _get_bounding_box_with_formula(center_position, [fov_x, fov_y]) }


def geom_from_metadata(metadata, sensor="stereoTop", side='west'):
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
            cambox_z = sf_meta['location_in_camera_box_m']['z']
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


# PRIVATE -------------------------------------
def _get_bounding_box_with_formula(center_position, fov):
    """Convert scannerbox center position & sensor field-of-view to actual bounding box

        Linear transformation formula adapted from:
        https://terraref.gitbooks.io/terraref-documentation/content/user/geospatial-information.html

        Returns:
            tuple of coordinates as: (  lat (y) min, lat (y) max,
                                        long (x) min, long (x) max )
    """

    # Get UTM information from southeast corner of field
    SE_utm = utm.from_latlon(33.07451869, -111.97477775)
    utm_zone = SE_utm[2]
    utm_num  = SE_utm[3]

    # TODO: Hard-coded
    # Linear transformation coefficients
    ay = 3659974.971; by = 1.0002; cy = 0.0078;
    ax = 409012.2032; bx = 0.009; cx = - 0.9986;
    lon_shift = 0.000020308287
    lat_shift = 0.000015258894

    # min/max bounding box x,y values
    y_w = center_position[1] + fov[1]/2
    y_e = center_position[1] - fov[1]/2
    x_n = center_position[0] + fov[0]/2
    x_s = center_position[0] - fov[0]/2
    # coordinates of northwest bounding box vertex
    Mx_nw = ax + bx * x_n + cx * y_w
    My_nw = ay + by * x_n + cy * y_w
    # coordinates if southeast bounding box vertex
    Mx_se = ax + bx * x_s + cx * y_e
    My_se = ay + by * x_s + cy * y_e
    # bounding box vertex coordinates
    bbox_nw_latlon = utm.to_latlon(Mx_nw, My_nw, utm_zone, utm_num)
    bbox_se_latlon = utm.to_latlon(Mx_se, My_se, utm_zone, utm_num)

    return ( bbox_se_latlon[0] - lat_shift,
             bbox_nw_latlon[0] - lat_shift,
             bbox_nw_latlon[1] + lon_shift,
             bbox_se_latlon[1] + lon_shift )
