"""Extractors

This module provides useful reference methods for extractors.
"""

import datetime
import time
import logging
import json
import os
import requests
from urllib3.filepost import encode_multipart_formdata

from pyclowder.extractors import Extractor
from terrautils.influx import Influx, add_arguments as add_influx_arguments
from terrautils.sensors import Sensors, add_arguments as add_sensor_arguments


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

    parser.add_argument('--clowder_user',
                        default=os.getenv('CLOWDER_USER', "terrarefglobus+uamac@ncsa.illinois.edu"),
                        help='clowder user to use when creating new datasets')

    parser.add_argument('--clowder_pass',
                        default=os.getenv('CLOWDER_PASS', ''),
                        help='clowder password to use when creating new datasets')


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
        self.clowder_user = self.args.clowder_user
        self.clowder_pass = self.args.clowder_pass

        if not base: base = self.args.terraref_base
        if not site: site = self.args.terraref_site
        if not sensor: sensor = self.args.sensor

        #log_config["handlers"]["logstash"]["message_type"] = ("terraref_"+sensor).replace(" ", "_").lower()
        logging.getLogger('pyclowder').setLevel(self.args.debug)
        logging.getLogger('__main__').setLevel(self.args.debug)
        self.logger = logging.getLogger("extractor")

        self.sensors = Sensors(base=base, station=site, sensor=sensor)
        self.get_sensor_path = self.sensors.get_sensor_path

        self.influx = Influx(self.args.influx_host, self.args.influx_port,
                             self.args.influx_db, self.args.influx_user,
                             self.args.influx_pass)


    def start_check(self, resource):
        """Standard format for extractor logs on check_message."""
        self.logger.info("[%s] %s - Checking message." % (resource['id'], resource['name']))


    def start_message(self, resource):
        self.logger.info("[%s] %s - Processing message." % (resource['id'], resource['name']))
        self.starttime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.created = 0
        self.bytes = 0


    def end_message(self, resource):
        self.logger.info("[%s] %s - Done." % (resource['id'], resource['name']))
        endtime = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        self.influx.log(self.extractor_info['name'],
                        self.starttime, endtime,
                        self.created, self.bytes)


    def log_info(self, resource, msg):
        """Standard format for extractor logs regarding progress."""
        self.logger.info("[%s] %s - %s" % (resource['id'], resource['name'], msg))


    def log_error(self, resource, msg):
        """Standard format for extractor logs regarding errors/failures."""
        self.logger.error("[%s] %s - %s" % (resource['id'], resource['name'], msg))


    def log_skip(self, resource, msg):
        """Standard format for extractor logs regarding skipped extractions."""
        self.logger.info("[%s] %s - SKIP: %s" % (resource['id'], resource['name'], msg))


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
            "extractor_id": clowderhost + ("" if clowderhost.endswith("/") else "/") + "api/extractors/"+ extractorinfo['name']
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
            create_time = datetime.datetime.strptime(f['date-created'].replace(" CDT","").replace(" CST",""),
                                                     "%c")
            latest_dt = datetime.datetime.strptime(latest_time.replace(" CDT","").replace(" CST","")
                                                   , "%c")
            if f['filename'] == trig:
                trig_dt = create_time

            if create_time > latest_dt:
                latest_time = f['date-created']
                latest_file = f['filename']

        if latest_file == trig or latest_time == trig_dt:
            return True
        else:
            return False
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


def file_exists(filepath, max_age_mins=3):
    """Return True if a file already exists on disk.

    If the file is zero bytes in size, return False if the file is more than
    max_age minutes old.
    """

    if os.path.exists(filepath):
        if os.path.getsize(filepath) > 0:
            return True
        else:
            age_seconds = time.time() - os.path.getmtime(filepath)
            return age_seconds < (max_age_mins*60)
    else:
        return False



# CLOWDER UTILS -------------------------------------
# TODO: Remove redundant ones of these once PyClowder2 supports user/password
def build_dataset_hierarchy(host, secret_key, clowder_user, clowder_pass, root_space, root_coll_name,
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
    parent_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, root_coll_name,
                                              parent_space=root_space)

    if year:
        # Create year-level collection
        year_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                "%s - %s" % (root_coll_name, year),
                                                parent_collect, parent_space=root_space)
        if month:
            # Create month-level collection
            month_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                     "%s - %s-%s" % (root_coll_name, year, month),
                                                     year_collect, parent_space=root_space)
            if date:
                targ_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                        "%s - %s-%s-%s" % (root_coll_name, year, month, date),
                                                        month_collect, parent_space=root_space)
            else:
                targ_collect = month_collect
        else:
            targ_collect = year_collect
    else:
        targ_collect = parent_collect

    target_dsid = get_dataset_or_create(host, secret_key, clowder_user, clowder_pass, leaf_ds_name,
                                        targ_collect, root_space)

    return target_dsid


def get_collection_or_create(host, secret_key, clowder_user, clowder_pass, cname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/collections?key=%s&title=%s&exact=true" % (host, secret_key, cname)
    result = requests.get(url)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_collection(host, clowder_user, clowder_pass, cname, "", parent_colln, parent_space)
    else:
        return result.json()[0]['id']


def create_empty_collection(host, clowder_user, clowder_pass, collectionname, description, parentid=None, spaceid=None):
    """Create a new collection in Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    collectionname -- name of new dataset to create
    description -- description of new dataset
    parentid -- id of parent collection
    spaceid -- id of the space to add dataset to
    """

    logger = logging.getLogger(__name__)

    if parentid:
        if (spaceid):
            url = '%sapi/collections/newCollectionWithParent' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description,
                                                    "parentId": [parentid], "space": spaceid}),
                                   auth=(clowder_user, clowder_pass))
        else:
            url = '%sapi/collections/newCollectionWithParent' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description,
                                                    "parentId": [parentid]}),
                                   auth=(clowder_user, clowder_pass))
    else:
        if (spaceid):
            url = '%sapi/collections' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description,
                                                    "space": spaceid}),
                                   auth=(clowder_user, clowder_pass))
        else:
            url = '%sapi/collections' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description}),
                                   auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    collectionid = result.json()['id']
    logger.debug("collection id = [%s]", collectionid)

    return collectionid


def get_dataset_or_create(host, secret_key, clowder_user, clowder_pass, dsname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/datasets?key=%s&title=%s&exact=true" % (host, secret_key, dsname)
    result = requests.get(url)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_dataset(host, clowder_user, clowder_pass, dsname, "",
                                    parent_colln, parent_space)
    else:
        return result.json()[0]['id']


def create_empty_dataset(host, clowder_user, clowder_pass, datasetname, description, parentid=None, spaceid=None):
    """Create a new dataset in Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetname -- name of new dataset to create
    description -- description of new dataset
    parentid -- id of parent collection
    spaceid -- id of the space to add dataset to
    """

    logger = logging.getLogger(__name__)

    url = '%sapi/datasets/createempty' % host

    if parentid:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "collection": [parentid], "space": [spaceid]}),
                                   auth=(clowder_user, clowder_pass))
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "collection": [parentid]}),
                                   auth=(clowder_user, clowder_pass))
    else:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "space": [spaceid]}),
                                   auth=(clowder_user, clowder_pass))
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description}),
                                   auth=(clowder_user, clowder_pass))

    result.raise_for_status()

    datasetid = result.json()['id']
    logger.debug("dataset id = [%s]", datasetid)

    return datasetid


def upload_to_dataset(connector, host, clowder_user, clowder_pass, datasetid, filepath):
    """Upload file to existing Clowder dataset.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that the file should be associated with
    filepath -- path to file
    check_duplicate -- check if filename already exists in dataset and skip upload if so
    """

    logger = logging.getLogger(__name__)

    for source_path in connector.mounted_paths:
        if filepath.startswith(connector.mounted_paths[source_path]):
            return _upload_to_dataset_local(connector, host, clowder_user, clowder_pass, datasetid, filepath)

    url = '%sapi/uploadToDataset/%s' % (host, datasetid)

    if os.path.exists(filepath):
        result = connector.post(url, files={"File": open(filepath, 'rb')},
                                auth=(clowder_user, clowder_pass))

        uploadedfileid = result.json()['id']
        logger.debug("uploaded file id = [%s]", uploadedfileid)

        return uploadedfileid
    else:
        logger.error("unable to upload file %s (not found)", filepath)


def _upload_to_dataset_local(connector, host, clowder_user, clowder_pass, datasetid, filepath):
    """Upload file POINTER to existing Clowder dataset. Does not copy actual file bytes.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that the file should be associated with
    filepath -- path to file
    """

    logger = logging.getLogger(__name__)
    url = '%sapi/uploadToDataset/%s' % (host, datasetid)

    if os.path.exists(filepath):
        # Replace local path with remote path before uploading
        for source_path in connector.mounted_paths:
            if filepath.startswith(connector.mounted_paths[source_path]):
                filepath = filepath.replace(connector.mounted_paths[source_path],
                                            source_path)
                break

        (content, header) = encode_multipart_formdata([
            ("file", '{"path":"%s"}' % filepath)
        ])
        result = connector.post(url, data=content, headers={'Content-Type': header},
                                auth=(clowder_user, clowder_pass))

        uploadedfileid = result.json()['id']
        logger.debug("uploaded file id = [%s]", uploadedfileid)

        return uploadedfileid
    else:
        logger.error("unable to upload local file %s (not found)", filepath)


def get_child_collections(host, clowder_user, clowder_pass, collectionid):
    """Get list of child collections in collection by UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    collectionid -- the collection to get children of
    """

    url = "%sapi/collections/%s/getChildCollections" % (host, collectionid)

    result = requests.get(url, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)


def get_datasets(host, clowder_user, clowder_pass, collectionid):
    """Get list of datasets in collection by UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the collection to get datasets of
    """

    url = "%sapi/collections/%s/datasets" % (host, collectionid)

    result = requests.get(url, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)


def delete_dataset(host, clowder_user, clowder_pass, datasetid):
    url = "%sapi/datasets/%s" % (host, datasetid)

    result = requests.delete(url, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)


def delete_dataset_metadata(host, clowder_user, clowder_pass, datasetid):
    url = "%sapi/datasets/%s/metadata.jsonld" % (host, datasetid)

    result = requests.delete(url, stream=True, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)


def delete_collection(host, clowder_user, clowder_pass, collectionid):
    url = "%sapi/collections/%s" % (host, collectionid)

    result = requests.delete(url, auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    return json.loads(result.text)


def delete_dataset_metadata_in_collection(host, clowder_user, clowder_pass, collectionid, recursive=True):
    dslist = get_datasets(host, clowder_user, clowder_pass, collectionid)

    logging.info("deleting dataset metadata in collection %s" % collectionid)
    for ds in dslist:
        delete_dataset_metadata(host, clowder_user, clowder_pass, ds['id'])
    logging.info("completed %s datasets" % len(dslist))

    if recursive:
        childcolls = get_child_collections(host, clowder_user, clowder_pass, collectionid)
        for coll in childcolls:
            delete_dataset_metadata_in_collection(host, clowder_user, clowder_pass, coll['id'], recursive)


def delete_datasets_in_collection(host, clowder_user, clowder_pass, collectionid, recursive=True, delete_colls=True):
    dslist = get_datasets(host, clowder_user, clowder_pass, collectionid)

    logging.info("deleting datasets in collection %s" % collectionid)
    for ds in dslist:
        delete_dataset(host, clowder_user, clowder_pass, ds['id'])
    logging.info("completed %s datasets" % len(dslist))

    if recursive:
        childcolls = get_child_collections(host, clowder_user, clowder_pass, collectionid)
        for coll in childcolls:
            delete_datasets_in_collection(host, clowder_user, clowder_pass, coll['id'], recursive, delete_colls)

    if delete_colls:
        logging.info("deleting collection %s" % collectionid)
        delete_collection(host, clowder_user, clowder_pass, collectionid)



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
