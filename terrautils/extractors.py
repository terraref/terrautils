"""Extractors

This module provides useful reference methods for extractors.
"""

import datetime
import logging
import json
import os
import utm
import requests

import gdal
import numpy
from matplotlib import cm, pyplot as plt
from netCDF4 import Dataset
from osgeo import gdal, osr
from PIL import Image

from pyclowder.extractors import Extractor
from pyclowder.collections import create_empty as create_empty_collection
from pyclowder.datasets import create_empty as create_empty_dataset
from terrautils.metadata import get_sensor_fixed_metadata
from terrautils.influx import Influx, add_arguments as add_influx_arguments
from terrautils.sensors import Sensors, add_arguments as add_sensor_arguments


def add_arguments(parser):

    # TODO: Move defaults into a level-based dict
    parser.add_argument('--clowderspace',
            default=os.getenv('CLOWDER_SPACE', "58da6b924f0c430e2baa823f"),
            help='sets the default Clowder space for creating new things')

    parser.add_argument('--overwrite', default=False, 
            action='store_true',
            help='enable overwriting of existing files')

    parser.add_argument('--debug', '-d', action='store_const',
            default=logging.WARN, const=logging.DEBUG,
            help='enable debugging (default=WARN)')


class TerrarefExtractor(Extractor):

    def __init__(self):

        super(TerrarefExtractor, self).__init__()

        add_arguments(self.parser)
        add_sensor_arguments(self.parser)
        add_influx_arguments(self.parser)


    def setup(self, base='', site='', sensor=''):

        super(TerrarefExtractor, self).setup()

        self.clowderspace = self.args.clowderspace
        self.debug = self.args.debug
        self.overwrite = self.args.overwrite

        if not base: base = self.args.terraref_base
        if not site: site = self.args.terraref_site
        if not sensor: sensor = self.args.sensor

        logging.getLogger('pyclowder').setLevel(self.args.debug)
        logging.getLogger('__main__').setLevel(self.args.debug)

        self.sensors = Sensors(base=base, station=site, sensor=sensor)
        self.get_sensor_path = self.sensors.get_sensor_path

        self.influx = Influx(self.args.influx_host, self.args.influx_port,
                             self.args.influx_db, self.args.influx_user,
                             self.args.influx_pass)


    # support message processing tracking, currently logged to influx
    def start_message(self):
        self.starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.created = 0
        self.bytes = 0


    def end_message(self):
        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.influx.log(self.extractor_info['name'],
                        self.starttime, endtime,
                        self.created, self.bytes)


# BASIC UTILS -------------------------------------
def build_metadata(clowderhost, extractorname, target_id, content, target_type='file', context=[]):
    """Construct extractor metadata object ready for submission to a Clowder file/dataset.

        clowderhost -- root URL of Clowder target instance (before /api)
        extractorname -- name of extractor, in extractors usually self.extractor_info['name']
        target_id -- UUID of file or dataset that metadata will be sent to
        content -- actual JSON contents of metadata
        target_type -- type of target resource, 'file' or 'dataset'
        context -- (optional) list of JSON-LD contexts
    """
    if context == []:
        context = ["https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld"]

    # TODO: Include version of extractor as standard
    md = {
        # TODO: Generate JSON-LD context for additional fields
        "@context": context,
        "content": content,
        "agent": {
            "@type": "cat:extractor",
            "extractor_id": clowderhost + "/api/extractors/" + extractorname
        }
    }

    if target_type == 'dataset':
        md['dataset_id'] = target_id
    else:
        md['file_id'] = target_id

    return md


def is_latest_file(resource):
    """Check whether the extractor-triggering file is the latest file in the dataset.

    This simple check should be used in dataset extractors to avoid collisions between 2+ instances of the same
    extractor trying to process the same dataset simultaneously by triggering off of 2 different uploaded files.

    Note that in the resource dictionary, "triggering_file" is the file that triggered the extraction (i.e. latest file
    at the time of message generation), not necessarily the newest file in the dataset.
    """
    trig = None
    if 'triggering_file' in resource:
        trig = resource['triggering_file']
    elif 'latest_file' in resource:
        trig = resource['latest_file']

    if trig:
        latest_file = ""
        latest_time = "Sun Jan 01 00:00:01 CDT 1920"

        for f in resource['files']:
            create_time = datetime.datetime.strptime(f['date-created'].replace(" CDT",""), "%c")
            if create_time > datetime.datetime.strptime(latest_time.replace(" CDT",""), "%c"):
                latest_time = f['date-created']
                latest_file = f['filename']

        if latest_file != trig:
            return False
        else:
            return True
    else:
        # If unable to determine triggering file, return True
        return True


def load_json_file(filepath):
    """Load contents of a .json file on disk into a JSON object.
    """
    try:
        with open(filepath, 'r') as jsonfile:
            return json.load(jsonfile)
    except:
        logging.error('could not load .json file %s' % filepath)
        return None

# CLOWDER UTILS -------------------------------------
# TODO: Move these to pyClowder 2 eventually, once pull requests are being merged timely
def build_dataset_hierarchy(connector, host, secret_key, root_space, root_coll_name,
                            year='', month='', date='', leaf_ds_name=''):
    """This will build collections for year, month, date level if needed in parent space.

        Typical hierarchy:
        MAIN LEVEL 1 DATA SPACE IN CLOWDER
        - Root collection for sensor ("stereoRGB geotiffs")
            - Year collection ("stereoRGB geotiffs - 2017")
                - Month collection ("stereoRGB geotiffs - 2017-01")
                    - Date collection ("stereoRGB geotiffs - 2017-01-01")
                        - Dataset ("stereoRGB geotiffs - 2017-01-01__01-02-03-456")

        Omitting year, month or date will result in dataset being added to next level up.
    """
    parent_collect = get_collection_or_create(connector, host, secret_key, root_coll_name,
                                              parent_space=root_space)

    if year:
        # Create year-level collection
        year_collect = get_collection_or_create(connector, host, secret_key, year,
                                                parent_collect, root_space)
        if month:
            # Create month-level collection
            month_collect = get_collection_or_create(connector, host, secret_key, month,
                                                     year_collect, root_space)
            if date:
                targ_collect = get_collection_or_create(connector, host, secret_key, date,
                                                        month_collect, root_space)
            else:
                targ_collect = month_collect
        else:
            targ_collect = year_collect
    else:
        targ_collect = parent_collect

    target_dsid = get_dataset_or_create(connector, host, secret_key, leaf_ds_name,
                                        targ_collect, root_space)

    return target_dsid


def get_collection_or_create(connector, host, secret_key, cname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/collections?key=%s&title=" % (host, secret_key, cname)
    result = requests.get(url, verify=connector.ssl_verify)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_collection(connector, host, secret_key, cname, "",
                                       parent_colln, parent_space)
    else:
        return result.json()[0]['id']


def get_dataset_or_create(connector, host, secret_key, dsname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/datasets?key=%s&title=" % (host, secret_key, dsname)
    result = requests.get(url, verify=connector.ssl_verify)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_dataset(connector, host, secret_key, dsname, "",
                                    parent_colln, parent_space)
    else:
        return result.json()[0]['id']


# FORMAT CONVERSION -------------------------------------
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
        # Use height of camera * slope_estimation to estimate expected canopy height
        predicted_plant_height = metadata['slope_estimation'] * cam_height
        # Subtract expected plant height from (cam height + rail height offset) to get canopy height
        cam_height_above_canopy = cam_height + metadata['rail_height_offset'] - predicted_plant_height
        fov_x = float(fov_x * (cam_height_above_canopy/2))
        fov_y = float(fov_y * (cam_height_above_canopy/2))
        # Account for experimentally determined distance from center to each stereo lens for left/right
        stereo_off = metadata['stereo_offsets_from_center']
        left_position = [center_position[0]+stereo_off, center_position[1], center_position[2]]
        right_position = [center_position[0]-stereo_off, center_position[1], center_position[2]]
        # Return two separate bounding boxes for left/right
        left_gps_bounds = _get_bounding_box_with_formula(left_position, [fov_x, fov_y])
        right_gps_bounds = _get_bounding_box_with_formula(right_position, [fov_x, fov_y])
        return (left_gps_bounds, right_gps_bounds)

    elif sensor=="flirIrCamera":
        cam_height_above_canopy = cam_height + metadata['rail_height_offset']
        fov_x = float(fov_x * (cam_height_above_canopy/2))
        fov_y = float(fov_y * (cam_height_above_canopy/2))

    return (_get_bounding_box_with_formula(center_position, [fov_x, fov_y]))


def calculate_scan_time(metadata):
    """Parse scan time from metadata.

        Returns:
            timestamp string
    """
    scan_time = None

    # TODO: Deprecated; can eventually remove
    if 'lemnatec_measurement_metadata' in metadata:
        lem_md = metadata['lemnatec_measurement_metadata']
        if 'gantry_system_variable_metadata' in lem_md:
            # timestamp, e.g. "2016-05-15T00:30:00-05:00"
            scan_time = _search_for_key(lem_md['gantry_system_variable_metadata'], ["time", "timestamp"])

    elif 'time' in metadata:
        scan_time = metadata['time']

    return scan_time


def create_geotiff(pixels, gps_bounds, out_path, nodata=-99, asfloat=False):
    """Generate output GeoTIFF file given a numpy pixel array and GPS boundary.

        Keyword arguments:
        pixels -- numpy array of pixel values.
                    if 2-dimensional array, a single-band GeoTIFF will be created.
                    if 3-dimensional array, a band will be created for each Z dimension.
        gps_bounds -- tuple of GeoTIFF coordinates as ( lat (y) min, lat (y) max,
                                                        long (x) min, long (x) max)
        out_path -- path to GeoTIFF to be created
        nodata -- NoDataValue to be assigned to raster bands; set to None to ignore
        float -- whether to use GDT_Float32 data type instead of GDT_Byte (e.g. for decimal numbers)
    """
    dimensions = numpy.shape(pixels)
    if len(dimensions) == 2:
        nrows, ncols = dimensions
        channels = 1
    else:
        nrows, ncols, channels = dimensions

    geotransform = (
        gps_bounds[2], # upper-left x
        (gps_bounds[3] - gps_bounds[2])/float(ncols), # W-E pixel resolution
        0, # rotation (0 = North is up)
        gps_bounds[1], # upper-left y
        0, # rotation (0 = North is up)
        -((gps_bounds[1] - gps_bounds[0])/float(nrows)) # N-S pixel resolution
    )

    # Create output GeoTIFF and set coordinates & projection
    if asfloat:
        output_raster = gdal.GetDriverByName('GTiff').Create(out_path, ncols, nrows, channels, gdal.GDT_Float32)
    else:
        output_raster = gdal.GetDriverByName('GTiff').Create(out_path, ncols, nrows, channels, gdal.GDT_Byte)
    output_raster.SetGeoTransform(geotransform)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326) # google mercator
    output_raster.SetProjection( srs.ExportToWkt() )

    if channels > 1:
        # typically 3 channels = RGB channels
        # TODO: Something wonky w/ uint8s --> ending up w/ lots of gaps in data (white pixels)
        for chan in range(channels):
            band = chan + 1
            output_raster.GetRasterBand(band).WriteArray(pixels[:,:,chan].astype('uint8')) # write red channel to raster file
            output_raster.GetRasterBand(band).FlushCache()
            if nodata:
                output_raster.GetRasterBand(band).SetNoDataValue(nodata)
    else:
        # single channel image, e.g. temperature
        output_raster.GetRasterBand(1).WriteArray(pixels)
        output_raster.GetRasterBand(1).FlushCache()
        if nodata:
            output_raster.GetRasterBand(1).SetNoDataValue(nodata)

    output_raster = None


def create_netcdf(pixels, out_path, scaled=False):
    """Generate output netCDF file given an input numpy pixel array.

            Keyword arguments:
            pixels -- 2-dimensional numpy array of pixel values
            out_path -- path to GeoTIFF to be created
            scaled -- whether to scale PNG output values based on pixels min/max
        """
    dimensions = numpy.shape(pixels)
    if len(dimensions) == 2:
        nrows, ncols = dimensions
        channels = 1
    else:
        nrows, ncols, channels = dimensions

    out_nc = Dataset(out_path, "w", format="NETCDF4")
    out_nc.createDimension('band',  channels) # only 1 band for mask
    out_nc.createDimension('x', ncols)
    out_nc.createDimension('y', nrows)

    mask = out_nc.createVariable('soil_mask','f8',('band', 'x', 'y'))
    mask[:] = pixels

    out_nc.close()


def create_image(pixels, out_path, scaled=False):
    """Generate output JPG/PNG file given an input numpy pixel array.

            Keyword arguments:
            pixels -- 2-dimensional numpy array of pixel values
            out_path -- path to GeoTIFF to be created
            scaled -- whether to scale PNG output values based on pixels min/max
        """
    if scaled:
        # e.g. flirIrCamera
        Gmin = pixels.min()
        Gmax = pixels.max()
        scaled_px = (pixels-Gmin)/(Gmax - Gmin)
        plt.imsave(out_path, cm.get_cmap('jet')(scaled_px))
    else:
        # e.g. PSII
        # TODO: Can we make these use same library?
        Image.fromarray(pixels).save(out_path)


def geom_from_metadata(metadata, sensor="stereoTop"):
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

    # TODO: Deprecated; can eventually remove
    if 'lemnatec_measurement_metadata' in metadata:
        lem_md = metadata['lemnatec_measurement_metadata']
        if 'gantry_system_variable_metadata' in lem_md:
            gantry_meta = lem_md['gantry_system_variable_metadata']

            # (x,y,z) position of gantry
            x_positions = ['position x [m]', 'position X [m]']
            y_positions = ['position y [m]', 'position Y [m]']
            z_positions = ['position z [m]', 'position Z [m]']
            gantry_x = _search_for_key(gantry_meta, x_positions)
            gantry_y = _search_for_key(gantry_meta, y_positions)
            gantry_z = _search_for_key(gantry_meta, z_positions)

        if 'sensor_fixed_metadata' in lem_md:
            sensor_meta = lem_md['sensor_fixed_metadata']

            # sensor location within camera box
            x_positions = ['location in camera box x [m]', 'location in camera box X [m]']
            y_positions = ['location in camera box y [m]', 'location in camera box Y [m]']
            z_positions = ['location in camera box z [m]', 'location in camera box Z [m]']
            cambox_x = _search_for_key(sensor_meta, x_positions)
            cambox_y = _search_for_key(sensor_meta, y_positions)
            cambox_z = _search_for_key(sensor_meta, z_positions)

            # sensor field-of-view
            x_fovs = ['field of view x [m]', 'field of view X [m]']
            y_fovs = ['field of view y [m]', 'field of view Y [m]']
            fov_x = _search_for_key(sensor_meta, x_fovs)
            fov_y = _search_for_key(sensor_meta, y_fovs)
            if not (fov_x and fov_y):
                fovs = _search_for_key(sensor_meta, ['field of view at 2m in X- Y- direction [m]'])
                if fovs:
                    fovs = fovs.replace('[','').replace(']','').split(' ')
                    try:
                        fov_x = float(fovs[0].encode("utf-8"))
                        fov_y = float(fovs[1].encode("utf-8"))
                    except AttributeError:
                        fov_x = fovs[0]
                        fov_y = fovs[1]

        if sensor=="stereoTop":
            cambox_z = 0.578
        elif not cambox_z:
            cambox_z = 0

    elif 'position_m' in metadata:
        gantry_x = metadata['position_m']['x'] if 'x' in metadata['position_m'] else gantry_x
        gantry_y = metadata['position_m']['y'] if 'y' in metadata['position_m'] else gantry_y
        gantry_z = metadata['position_m']['z'] if 'z' in metadata['position_m'] else gantry_z

        sensor_fixed = get_sensor_fixed_metadata(metadata['station'],
                                                 metadata['sensor'])

        # LOCATION IN CAMERA BOX
        cambox_x = sensor_fixed['location_in_camera_box_m']['x']
        cambox_y = sensor_fixed['location_in_camera_box_m']['y']
        cambox_z = sensor_fixed['location_in_camera_box_m']['z']

        # FIELD OF VIEW (FOV)
        for fov_field in ['field_of_view_m', 'field_of_view_degrees']:
            if fov_field in sensor_fixed:
                fov_x = sensor_fixed[fov_field]['x'] if 'x' in sensor_fixed[fov_field] else fov_x
                fov_y = sensor_fixed[fov_field]['y'] if 'y' in sensor_fixed[fov_field] else fov_y

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


def _search_for_key(metadata, key_variants):
    """Check for presence of any key variants in metadata. Does basic capitalization check.

        Returns:
        value if found, or None
    """
    val = None
    for variant in key_variants:
        if variant in metadata:
            val = metadata[variant]
        elif variant.capitalize() in metadata:
            val = metadata[variant.capitalize()]

    # If a value was found, try to parse as float
    if val:
        try:
            return float(val.encode("utf-8"))
        except:
            return val
    else:
        return None
