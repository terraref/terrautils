"""Extractors

This module provides useful reference methods for extractors.
"""

import os
import utm
import datetime
from dateutil.parser import parse
from influxdb import InfluxDBClient, SeriesHelper
from pyclowder.collections import get_datasets
from pyclowder.datasets import submit_extraction


def get_output_directory(rootdir, datasetname):
    """Determine output directory path given root path and dataset name.

    Example dataset name:
        stereoTop - 2017-05-04__10-31-34-536
    Resulting output:
        rootdir/2017-05-04/2017-05-04__10-31-34-536
    """
    if datasetname.find(" - ") > -1:
        # 2017-05-04__10-31-34-536
        timestamp = datasetname.split(" - ")[1]
    else:
        timestamp = datasetname

    if timestamp.find("__") > -1:
        # 2017-05-04
        datestamp = timestamp.split("__")[0]
    else:
        datestamp = ""

    return os.path.join(rootdir, datestamp, timestamp)


def get_output_filename(datasetname, outextension,lvl="lv1", site="uamac", opts=[]):
    """Determine output filename given input information.

    sensor_level_datetime_site_a_b_c.extension
        a = what product?
        left/right/top/bottom - position
    """
    pass


def log_to_influxdb(extractorname, starttime, endtime, filecount, bytecount):
    """Send extractor job detail summary to InfluxDB instance.

    starttime - example format "2017-02-10T16:09:57+00:00"
    endtime - example format "2017-02-10T16:09:57+00:00"
    """

    # Convert timestamps to seconds from epoch
    f_completed_ts = int(parse(endtime).strftime('%s'))
    f_duration = f_completed_ts - int(parse(starttime).strftime('%s'))

    # Check
    influx_host = os.getenv("INFLUX_HOST", "terra-logging.ncsa.illinois.edu")
    influx_port = os.getenv("INFLUX_PORT", 8086)
    influx_db = os.getenv("INFLUX_DB", "extractor_db")
    influx_user = os.getenv("INFLUX_USER", "terra")
    influx_pass = os.getenv("INFLUX_PASS", "")

    client = InfluxDBClient(influx_host, influx_port, influx_user, influx_pass, influx_db)
    client.write_points([{
        "measurement": "file_processed",
        "time": f_completed_ts,
        "fields": {"value": f_duration}
    }], tags={"extractor": extractorname, "type": "duration"})
    client.write_points([{
        "measurement": "file_processed",
        "time": f_completed_ts,
        "fields": {"value": int(filecount)}
    }], tags={"extractor": extractorname, "type": "filecount"})
    client.write_points([{
        "measurement": "file_processed",
        "time": f_completed_ts,
        "fields": {"value": int(bytecount)}
    }], tags={"extractor": extractorname, "type": "bytes"})


def calculate_geometry(metadata, stereo=True):
    """Determine geoJSON of dataset given input metadata.
    """

    # bin2tif.get_position(metadata) ------------------------
    gantry_meta = metadata['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
    gantry_x = gantry_meta["position x [m]"]
    gantry_y = gantry_meta["position y [m]"]
    gantry_z = gantry_meta["position z [m]"]

    cam_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
    cam_x = cam_meta["location in camera box x [m]"]
    cam_y = cam_meta["location in camera box y [m]"]
    if "location in camera box z [m]" in cam_meta: # this may not be in older data
        cam_z = cam_meta["location in camera box z [m]"]
    else:
        cam_z = 0.578

    x = float(gantry_x) + float(cam_x)
    y = float(gantry_y) + float(cam_y)
    z = float(gantry_z) + float(cam_z)# + HEIGHT_MAGIC_NUMBER # gantry rails are at 2m

    center_position = (x, y, z)
    camHeight = center_position[2]


    # bin2tif.get_fov(metadata, camHeight, shape) ------------------------
    cam_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
    fov = cam_meta["field of view at 2m in x- y- direction [m]"]

    # TODO: These are likely specific to stereoTop
    fov_x = 1.015 #float(fov_list[0])
    fov_y = 0.749 #float(fov_list[1])

    HEIGHT_MAGIC_NUMBER = 1.64
    PREDICT_MAGIC_SLOPE = 0.574
    predict_plant_height = PREDICT_MAGIC_SLOPE * camHeight
    camH_fix = camHeight + HEIGHT_MAGIC_NUMBER - predict_plant_height
    fix_fov_x = fov_x*(camH_fix/2)
    fix_fov_y = fov_y*(camH_fix/2)

    fix_fov = (fix_fov_x, fix_fov_y)

    if stereo:
        # NOTE: This STEREO_OFFSET is an experimentally determined value.
        STEREO_OFFSET = .17 # distance from center_position to each of the stereo cameras (left = +, right = -)
        left_position = [center_position[0]+STEREO_OFFSET, center_position[1], center_position[2]]
        right_position = [center_position[0]-STEREO_OFFSET, center_position[1], center_position[2]]

        left_gps_bounds = _get_bounding_box_with_formula(left_position, fix_fov) # (lat_max, lat_min, lng_max, lng_min) in decimal degrees
        right_gps_bounds = _get_bounding_box_with_formula(right_position, fix_fov)

        return (left_gps_bounds, right_gps_bounds)

    else:
        gps_bounds = _get_bounding_box_with_formula(center_position, fix_fov)

        return (gps_bounds)

def _get_bounding_box_with_formula(center_position, fov):
    # Scanalyzer -> MAC formula @ https://terraref.gitbooks.io/terraref-documentation/content/user/geospatial-information.html
    SE_latlon = (33.07451869,-111.97477775)
    ay = 3659974.971; by = 1.0002; cy = 0.0078;
    ax = 409012.2032; bx = 0.009; cx = - 0.9986;
    lon_shift = 0.000020308287
    lat_shift = 0.000015258894
    SE_utm = utm.from_latlon(SE_latlon[0], SE_latlon[1])

    y_w = center_position[1] + fov[1]/2
    y_e = center_position[1] - fov[1]/2
    x_n = center_position[0] + fov[0]/2
    x_s = center_position[0] - fov[0]/2

    Mx_nw = ax + bx * x_n + cx * y_w
    My_nw = ay + by * x_n + cy * y_w

    Mx_se = ax + bx * x_s + cx * y_e
    My_se = ay + by * x_s + cy * y_e

    fov_nw_latlon = utm.to_latlon(Mx_nw, My_nw, SE_utm[2],SE_utm[3])
    fov_se_latlon = utm.to_latlon(Mx_se, My_se, SE_utm[2],SE_utm[3])

    return (fov_se_latlon[0] - lat_shift, fov_nw_latlon[0] - lat_shift, fov_nw_latlon[1] + lon_shift, fov_se_latlon[1] + lon_shift)


def trigger_extraction_on_collection(clowderhost, clowderkey, collectionid, extractor):
    """Manually trigger an extraction on all datasets in a collection.
    """
    dslist = get_datasets(None, clowderhost, clowderkey, collectionid)
    print("submitting %s datasets" % len(dslist))
    for ds in dslist:
        submit_extraction(None, clowderhost, clowderkey, ds['id'], extractor)

