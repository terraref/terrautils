"""Extractors

This module provides useful reference methods for extractors.
"""

import os
import utm
import datetime
from math import cos, pi
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


def get_output_filename(datasetname, outextension, lvl="lv1", site="uamac", opts=[]):
    """Determine output filename given input information.

    sensor_level_datetime_site_a_b_c.extension
        a = what product?
        left/right/top/bottom - position
    """
    if datasetname.find(" - ") > -1:
        # 2017-05-04__10-31-34-536
        sensorname = datasetname.split(" - ")[0]
        timestamp = datasetname.split(" - ")[1]
    else:
        sensorname = datasetname
        timestamp = "2017"

    return "_".join([sensorname, lvl, timestamp, site]+opts)+".%s" % outextension


def is_latest_file(resource):
    """Check whether the extractor-triggering file is the latest file in the dataset.

    This simple check should be used in dataset extractors to avoid collisions between 2+ instances of the same
    extractor trying to process the same dataset simultaneously by triggering off of 2 different uploaded files.

    Note that in the resource dictionary, "latest_file" is the file that triggered the extraction (i.e. latest file
    at the time of message generation), not necessarily the newest file in the dataset.
    """
    if resource['latest_file']:
        latest_file = ""
        latest_time = "Sun Jan 01 00:00:01 CDT 1920"

        for f in resource['files']:
            create_time = datetime.datetime.strptime(f['date-created'].replace(" CDT",""), "%c")
            if create_time > datetime.datetime.strptime(latest_time.replace(" CDT",""), "%c"):
                latest_time = f['date-created']
                latest_file = f['filename']

        if latest_file != resource['latest_file']:
            return False
        else:
            return True


def trigger_extraction_on_collection(clowderhost, clowderkey, collectionid, extractor):
    """Manually trigger an extraction on all datasets in a collection.
    """
    dslist = get_datasets(None, clowderhost, clowderkey, collectionid)
    print("submitting %s datasets" % len(dslist))
    for ds in dslist:
        submit_extraction(None, clowderhost, clowderkey, ds['id'], extractor)


def log_to_influxdb(extractorname, starttime, endtime, filecount, bytecount):
    """Send extractor job detail summary to InfluxDB instance.

    starttime - example format "2017-02-10T16:09:57+00:00"
    endtime - example format "2017-02-10T16:09:57+00:00"
    """

    # Convert timestamps to seconds from epoch
    f_completed_ts = int(parse(endtime).strftime('%s'))*1000000000
    f_duration = f_completed_ts - int(parse(starttime).strftime('%s'))*1000000000

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


def error_notification(msg):
    """Send an error message notification, e.g. to Slack.
    """
    pass


# GEOM STUFF FROM stereoRGB (*~~~~~*BLESSED*~~~~~*)
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
def create_geotiff(which_im, np_arr, gps_bounds, out_file_path):
    try:
        nrows,ncols,nz = np.shape(np_arr)
        # gps_bounds: (lat_min, lat_max, lng_min, lng_max)
        xres = (gps_bounds[3] - gps_bounds[2])/float(ncols)
        yres = (gps_bounds[1] - gps_bounds[0])/float(nrows)
        geotransform = (gps_bounds[2],xres,0,gps_bounds[1],0,-yres) #(top left x, w-e pixel resolution, rotation (0 if North is up), top left y, rotation (0 if North is up), n-s pixel resolution)

        output_raster = gdal.GetDriverByName('GTiff').Create(out_file_path, ncols, nrows, nz, gdal.GDT_Byte)

        output_raster.SetGeoTransform(geotransform) # specify coordinates
        srs = osr.SpatialReference() # establish coordinate encoding
        srs.ImportFromEPSG(4326) # specifically, google mercator
        output_raster.SetProjection( srs.ExportToWkt() ) # export coordinate system to file

        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        output_raster.GetRasterBand(1).WriteArray(np_arr[:,:,0].astype('uint8')) # write red channel to raster file
        output_raster.GetRasterBand(1).FlushCache()
        output_raster.GetRasterBand(1).SetNoDataValue(-99)

        output_raster.GetRasterBand(2).WriteArray(np_arr[:,:,1].astype('uint8')) # write green channel to raster file
        output_raster.GetRasterBand(2).FlushCache()
        output_raster.GetRasterBand(2).SetNoDataValue(-99)

        output_raster.GetRasterBand(3).WriteArray(np_arr[:,:,2].astype('uint8')) # write blue channel to raster file
        output_raster.GetRasterBand(3).FlushCache()
        output_raster.GetRasterBand(3).SetNoDataValue(-99)

        output_raster = None
    except Exception as ex:
        fail('Error creating GeoTIFF: ' + str(ex))

# GEOM STUFF FROM FLIR
def main(metadata):
    center_position, scan_time, fov = parse_metadata(metadata)
    gps_bounds = get_bounding_box_flir(center_position, fov) # get bounding box using gantry position and fov of camera

    raw_data = load_flir_data(binfile) # get raw data from bin file
    tc = rawData_to_temperature(raw_data, scan_time, metadata)
    im_color = create_png(raw_data, out_png) # create png
    create_geotiff_with_temperature(im_color, tc, gps_bounds, tif_path) # create geotiff
def create_png(im, outfile_path):

    Gmin = im.min()
    Gmax = im.max()
    At = (im-Gmin)/(Gmax - Gmin)

    my_cmap = cm.get_cmap('jet')
    color_array = my_cmap(At)

    plt.imsave(outfile_path, color_array)

    img_data = Image.open(outfile_path)

    return np.array(img_data)
def parse_metadata(metadata):

    #gantry_meta = metadata['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
    #gantry_x = gantry_meta["position x [m]"]
    #gantry_y = gantry_meta["position y [m]"]
    #gantry_z = gantry_meta["position z [m]"]

    scan_time = gantry_meta["time"]

    #cam_meta = metadata['lemnatec_measurement_metadata']['sensor_fixed_metadata']
    #cam_x = cam_meta["location in camera box x [m]"]
    #cam_y = cam_meta["location in camera box y [m]"]
    #if "location in camera box z [m]" in cam_meta: # this may not be in older data
    #    cam_z = cam_meta["location in camera box z [m]"]
    #else:
    cam_z = 0

    fov_x = cam_meta["field of view x [m]"]
    fov_y = cam_meta["field of view y [m]"]

    x = float(gantry_x) + float(cam_x)
    y = float(gantry_y) + float(cam_y)
    z = float(gantry_z) + float(cam_z)

    center_position = (x, y, z)
    fov = [float(fov_x), float(fov_y)]

    return center_position, scan_time, fov

def get_bounding_box_flir(center_position, fov):
    # From Get_FLIR.py

    # NOTE: ZERO_ZERO is the southeast corner of the field. Position values increase to the northwest (so +y-position = +latitude, or more north and +x-position = -longitude, or more west)
    # We are also simplifying the conversion of meters to decimal degrees since we're not close to the poles and working with small distances.
    ZERO_ZERO = (33.07451869,-111.97477775)

    # NOTE: x --> latitude; y --> longitude
    try:
        r = 6378137 # earth's radius

        x_min = center_position[1] - fov[1]/2
        x_max = center_position[1] + fov[1]/2
        y_min = center_position[0] - fov[0]/2
        y_max = center_position[0] + fov[0]/2

        lat_min_offset = y_min/r* 180/pi
        lat_max_offset = y_max/r * 180/pi
        lng_min_offset = x_min/(r * cos(pi * ZERO_ZERO[0]/180)) * 180/pi
        lng_max_offset = x_max/(r * cos(pi * ZERO_ZERO[0]/180)) * 180/pi

        lat_min = ZERO_ZERO[0] + lat_min_offset
        lat_max = ZERO_ZERO[0] + lat_max_offset
        lng_min = ZERO_ZERO[1] - lng_min_offset
        lng_max = ZERO_ZERO[1] - lng_max_offset

    return (lat_min, lat_max, lng_max, lng_min)

def create_geotiff_with_temperature(np_arr, temp_arr, gps_bounds, out_file_path):
    try:
        nrows, ncols, channels = np.shape(np_arr)
        xres = (gps_bounds[3] - gps_bounds[2])/float(ncols)
        yres = (gps_bounds[1] - gps_bounds[0])/float(nrows)
        geotransform = (gps_bounds[2],xres,0,gps_bounds[1],0,-yres) #(top left x, w-e pixel resolution, rotation (0 if North is up), top left y, rotation (0 if North is up), n-s pixel resolution)

        output_raster = gdal.GetDriverByName('GTiff').Create(out_file_path, ncols, nrows, 1, gdal.GDT_Byte)

        output_raster.SetGeoTransform(geotransform) # specify coordinates
        srs = osr.SpatialReference() # establish coordinate encoding
        srs.ImportFromEPSG(4326) # specifically, google mercator
        output_raster.SetProjection( srs.ExportToWkt() ) # export coordinate system to file

        '''
        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        output_raster.GetRasterBand(1).WriteArray(np_arr[:,:,0].astype('uint8')) # write red channel to raster file
        output_raster.GetRasterBand(1).FlushCache()
        output_raster.GetRasterBand(1).SetNoDataValue(-99)

        output_raster.GetRasterBand(2).WriteArray(np_arr[:,:,1].astype('uint8')) # write green channel to raster file
        output_raster.GetRasterBand(2).FlushCache()
        output_raster.GetRasterBand(2).SetNoDataValue(-99)

        output_raster.GetRasterBand(3).WriteArray(np_arr[:,:,2].astype('uint8')) # write blue channel to raster file
        output_raster.GetRasterBand(3).FlushCache()
        output_raster.GetRasterBand(3).SetNoDataValue(-99)
        '''

        output_raster.GetRasterBand(1).WriteArray(temp_arr) # write temperature information to raster file
        output_raster = None

    except Exception as ex:
        fail('Error creating GeoTIFF: ' + str(ex))


    return

# PNG STUFF FROM PSII - SHOULD THERE BE GEOTIFF COMPONENT HERE?
def load_PSII_data(file_path, height, width, out_file):

    try:
        im = np.fromfile(file_path, np.dtype('uint8')).reshape([height, width])
        Image.fromarray(im).save(out_file)
        return im.astype('u1')
    except Exception as ex:
        fail('Error processing image "%s": %s' % (file_path,str(ex)))
