"""Extractors

This module provides useful reference methods for extractors.
"""

import datetime
import logging
import json
import os
import requests

import numpy

from pyclowder.extractors import Extractor
from pyclowder.collections import create_empty as create_empty_collection
from pyclowder.datasets import create_empty as create_empty_dataset
from terrautils.influx import Influx, add_arguments as add_influx_arguments
from terrautils.sensors import Sensors, add_arguments as add_sensor_arguments
from terrautils.spatial import calculate_bounding_box, calculate_centroid, calculate_centroid_from_wkt, calculate_gps_bounds, geom_from_metadata
from terrautils.formats import create_geotiff, create_image, create_netcdf
from terrautils.metadata import get_sensor_fixed_metadata


logging.basicConfig(format='%(asctime)s %(message)s')

def add_arguments(parser):

    # TODO: Move defaults into a level-based dict
    parser.add_argument('--clowderspace',
            default=os.getenv('CLOWDER_SPACE', "58da6b924f0c430e2baa823f"),
            help='sets the default Clowder space for creating new things')

    parser.add_argument('--overwrite', default=False, 
            action='store_true',
            help='enable overwriting of existing files')

    parser.add_argument('--debug', '-d', action='store_const',
            default=logging.INFO, const=logging.DEBUG,
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
def build_metadata(clowderhost, extractorinfo, target_id, content, target_type='file', context=[]):
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

    content['extractor_version'] = extractorinfo['version']

    md = {
        # TODO: Generate JSON-LD context for additional fields
        "@context": context,
        "content": content,
        "agent": {
            "@type": "cat:extractor",
            "extractor_id": clowderhost + "/api/extractors/" + extractorinfo['name']
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
# TODO: Add support for user/password in addition to secret_key
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
        year_collect = get_collection_or_create(connector, host, secret_key,
                                                "%s - %s" % (root_coll_name, year),
                                                parent_collect)
        if month:
            # Create month-level collection
            month_collect = get_collection_or_create(connector, host, secret_key,
                                                     "%s - %s-%s" % (root_coll_name, year, month),
                                                     year_collect)
            if date:
                targ_collect = get_collection_or_create(connector, host, secret_key,
                                                        "%s - %s-%s-%s" % (root_coll_name, year, month, date),
                                                        month_collect)
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
    url = "%sapi/collections?key=%s&title=%s" % (host, secret_key, cname)
    result = requests.get(url, verify=connector.ssl_verify)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_collection(connector, host, secret_key, cname, "",
                                       parent_colln, parent_space)
    else:
        return result.json()[0]['id']


def get_dataset_or_create(connector, host, secret_key, dsname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/datasets?key=%s&title=%s" % (host, secret_key, dsname)
    result = requests.get(url, verify=connector.ssl_verify)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_dataset(connector, host, secret_key, dsname, "",
                                    parent_colln, parent_space)
    else:
        return result.json()[0]['id']


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

    elif 'gantry_variable_metadata' in metadata:
        scan_time = metadata['gantry_variable_metadata']['time_utc']

    return scan_time




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
