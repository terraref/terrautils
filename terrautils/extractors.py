"""Extractors

This module provides useful reference methods for extractors.
"""

import datetime
import time
import logging
import json
import os
import re
import requests
import yaml
import utm
from urllib3.filepost import encode_multipart_formdata

from pyclowder.extractors import Extractor
from pyclowder.datasets import get_file_list, download_metadata as download_dataset_metadata
from terrautils.influx import Influx, add_arguments as add_influx_arguments
from terrautils.metadata import get_terraref_metadata, pipeline_get_metadata, \
                get_season_and_experiment
from terrautils.sensors import Sensors, add_arguments as add_sensor_arguments
from terrautils.users import get_dataset_username, find_user_name


logging.basicConfig(format='%(asctime)s %(message)s')

DEFAULT_EXPERIMENT_JSON_FILENAME = 'experiment.yaml'

def add_arguments(parser):

    # TODO: Move defaults into a level-based dict
    parser.add_argument('--clowderspace',
            default=os.getenv('CLOWDER_SPACE', "58da6b924f0c430e2baa823f"),
            help='sets the default Clowder space for creating new things')

    parser.add_argument('--overwrite',
                        default=False,
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

    parser.add_argument('--experiment_json_file', nargs='?', dest='experiment_json_file',
                        default=os.getenv('EXPERIMENT_CONFIG', DEFAULT_EXPERIMENT_JSON_FILENAME),
                        help='Default name of experiment configuration file used to' \
                             ' provide additional processing information')


class TerrarefExtractor(Extractor):

    def __init__(self):

        super(TerrarefExtractor, self).__init__()

        add_arguments(self.parser)
        add_sensor_arguments(self.parser)
        add_influx_arguments(self.parser)

        self.dataset_metadata = None
        self.terraref_metadata = None
        self.experiment_metadata = None

    def setup(self, base='', site='', sensor=''):

        super(TerrarefExtractor, self).setup()

        self.clowderspace = self.args.clowderspace
        self.debug = self.args.debug
        self.overwrite = self.args.overwrite
        self.clowder_user = self.args.clowder_user
        self.clowder_pass = self.args.clowder_pass
        self.experiment_json_file = self.args.experiment_json_file

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

    @property
    def default_epsg(self):
        """Returns the default EPSG code that utilities expect
        """
        return 4326
    
    @property
    def sensor_name(self):
        """Returns the sensor name the instance is configured with. Returns None if
           sensor information is not available
        """
        if self.sensors:
            return self.sensors.sensor
        return None

    @property
    def config_file_name(self):
        """Returns the name of the expected configuration file
        """
        # pylint: disable=line-too-long
        return DEFAULT_EXPERIMENT_JSON_FILENAME if not self.experiment_json_file \
                                                else self.experiment_json_file
        # pylint: enable=line-too-long

    @property
    def date_format_regex(self):
        """Returns array of regex expressions for different date formats
        """
        # We lead with the best formatting to use, add on the rest
        return [r'(\d{4}(/|-){1}\d{1,2}(/|-){1}\d{1,2})',
                r'(\d{1,2}(/|-){1}\d{1,2}(/|-){1}\d{4})'
               ]

    @property
    def terraref_timestamp_format_regex(self):
        """Returns array of regex expressions for different timestamp formats
        """
        return [r'(\d{4}(/|-){1}\d{1,2}(/|-){1}\d{1,2}__\d{2}-\d{2}-\d{2}[\-{1}\d{3}]*)'
                ]

    @property
    def iso_timestamp_format_regex(self):
        """Returns array of regex expressions for different timestamp formats
        """
        return [r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})[+-](\d{2})\:(\d{2})',
                r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})(.\d+)[+-](\d{2})\:(\d{2})',
                r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})(.\d+)',
                r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})',
                r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})Z'
                ]    

    @property
    def dataset_metadata_file_ending(self):
        """ Returns the ending string of a dataset metadata JSON file name
        """
        return '_dataset_metadata.json'

    @property
    def file_infodata_file_ending(self):
        """ Returns the ending string of a file's info JSON file name
        """
        return '_info.json'

    @property
    def get_terraref_metadata(self):
        """Returns any loaded terraref metadata
        """
        return self.terraref_metadata
    

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

    def process_message(self, connector, host, secret_key, resource, parameters):
        """Preliminary handling of a message
        Keyword arguments:
            connector(obj): the message queue connector instance
            host(str): the URI of the host making the connection
            secret_key(str): used with the host API
            resource(dict): dictionary containing the resources associated with the request
            parameters(json): json object of the triggering message contents
        Notes:
            Loads dataset metadata if it's available. Looks for terraref metadata in the dataset
            metadata and stores a reference to that, if available. Looks for an experiment
            configuration file and loads that, if found.
        """
        # Setup to default value
        self.dataset_metadata = None
        self.terraref_metadata = None
        self.experiment_metadata = None

        try:
            # Find the meta data for the dataset and other files of interest
            dataset_file = None
            experiment_file = None
            for onefile in resource['local_paths']:
                if onefile.endswith(self.dataset_metadata_file_ending):
                    dataset_file = onefile
                elif os.path.basename(onefile) == self.config_file_name:
                    experiment_file = onefile
                if not dataset_file is None and not experiment_file is None:
                    break;

            # If we don't have dataset metadata already, download it
            dataset_md = None
            if dataset_file is None:
                dataset_id = None
                if 'type' in resource:
                    if resource['type'] == 'dataset':
                        dataset_id = resource['id']
                    elif resource['type'] == 'file' and 'parent' in resource:
                        if 'type' in resource['parent'] and resource['parent']['type'] == 'dataset':
                            dataset_id = resource['parent']['id'] if 'id' in resource['parent'] \
                                                                                    else dataset_id
                if not dataset_id is None:
                    dataset_md = download_dataset_metadata(connector, host, secret_key, dataset_id)
            else:
                # Load the dataset metadata from disk
                dataset_md = load_json_file(dataset_file)

            # If we have terraref metadata then store it and dataset metadata for later use
            if not dataset_md is None:
                terraref_md = get_terraref_metadata(dataset_md)
                if terraref_md:
                    md_len = len(terraref_md)
                    if md_len > 0:
                        self.terraref_metadata = terraref_md

                md_len = len(dataset_md)
                if md_len > 0:
                    self.dataset_metadata = dataset_md
                    self.experiment_metadata = pipeline_get_metadata(dataset_md)

            # Now we load any experiment configuration file
            if not experiment_file is None:
                experiment_md = load_yaml_file(experiment_file)
                if experiment_md:
                    md_len = len(experiment_md)
                    if md_len > 0:
                        self.experiment_metadata = pipeline_get_metadata(experiment_md)

        # pylint: disable=broad-except
        except Exception as ex:
            self.log_error(resource, "Exception caught while loading dataset metadata: " + str(ex))

    # pylint: disable=too-many-nested-blocks
    def extract_datestamp(self, date_string):
        """Extracts the datestamp from a string. The parts of a date can be separated by
           single hyphens or slashes ('-' or '/') and no white space.
        Keyword arguments:
            date_string(str): string to lookup a datestamp in. The first found datestamp is
            returned.
        Returns:
            The extracted datestamp as YYYY-MM-DD. Returns None if a date isn't found
        Notes:
            This function only cares if the timestamp looks correct. It doesn't try to figure
            out if year, month, and day have correct values. The found date string may be
            reformatted to match the expected return.
        """

        # Check the string
        if not date_string or not isinstance(date_string, basestring):
            return None

        date_string_len = len(date_string)
        if date_string_len <= 0:
            return None

        # Find a date
        for part in date_string.split(' - '):
            for form in self.date_format_regex:
                res = re.search(form, part)
                if res:
                    date = res.group(0).replace('/', '-')
                    # Check for hyphen in first 4 characters to see if we need to move things
                    if not '-' in date[:4]:
                        return date
                    else:
                        split_date = date.split('-')
                        if len(split_date) == 3:
                            return split_date[2] + "-" + split_date[1] + "-" + split_date[0]

        return None

    def extract_timestamp(self, time_string):
        """Extracts a timestamp from the string and returns it
        Args:
            time_string(str): the string in which to look for a timestamp
        Return:
            The found timestamp or None if one isn't found.
        Notes:
            This function will look for both a ISO 8601 long format timestamp as weill as a
            legacy TERRA REF timestamp. If only one is found, that is returned. If both are
            found, the more detailed timestamp is returned. For example, if the TERRA REF
            timestamp only has a date but the ISO timestamp has time as well as date, the ISO
            timestamp will be returned.
        """
        tr_ts, iso_ts = None, None

        # define helper function for parsing the string using regex
        def search_regex(self, time_string, regex_exprs):
            """ Hidden function used for searching a string using an array of regex expressions
            """
            ret_str = None
            ts_parts = time_string.split(" - ")

            # Look for the terra ref timestamp
            for one_part in ts_parts:
                for form in regex_exprs:
                    res = re.search(form, one_part)
                    if res:
                        ret_str = res.group(0)
                        break
                if not ret_str is None:
                    break
            if ret_str is None:
                ret_str = self.extract_datestamp(time_string)
            return ret_str

        # Look for the timestamps
        tr_ts = search_regex(self, time_string, self.terraref_timestamp_format_regex)
        iso_ts = search_regex(self, time_string, self.iso_timestamp_format_regex)

        # If we have none, or one timestamp, return what we have
        if tr_ts is None:
            return iso_ts
        elif iso_ts is None:
            return tr_ts

        # Return the full terraref timestamp if we have it. Otherwise check
        # if the ISO timestamp is more complete and return the most complete
        # timestamp

        # The double undewrscore seperates date from time in TERRA REF
        if '__' in tr_ts:
            return tr_ts

        # If the ISO timestamp contains colons it contains a time and will be better than the
        # date-only TERRA REF string
        return iso_ts if ':' in iso_ts else tr_ts

    def find_datestamp(self, text=None):
        """Returns a date stamp
        Keyword arguments:
            text(str): optional text string to search for a date stamp
        Return:
            A date stamp in the format of YYYY-MM-DD. No checks are made to determine if the
            returned string is a valid date.
        Notes:
            The following places are searched for a valid date; the first date found is the one
            that's returned: the text parameter, the name of the dataset associated with the
            current message being processed, the JSON configuration file as defined by the
            config_file_name() property, the current GMT date.
        """
        datestamp = None

        if not text is None:
            datestamp = self.extract_datestamp(text)

        if datestamp is None and not self.dataset_metadata is None:
            if 'name' in self.dataset_metadata:
                datestamp = self.extract_datestamp(self.dataset_metadata['name'])

        if datestamp is None and not self.experiment_metadata is None:
            if 'observationTimeStamp' in self.experiment_metadata:
                datestamp = self.extract_datestamp(self.experiment_metadata['observationTimeStamp'])

        if datestamp is None:
            datestamp = datetime.datetime.utcnow().strftime('%Y-%m-%d')

        return datestamp

    def find_timestamp(self, text=None):
        """Returns a time stamp consisting of a date and time
        Keyword arguments:
            text(str): optional text string to search for a time stamp
        Return:
            A time stamp in the format of YYYY-MM-DDThh:mm:ss[.sss]|Z|<offset>] or
            YYYY-MM-DD__hh-mm-ss_<id>. No checks are made to determine if the returned string is
            a valid timestamp.
        Notes:
            The following places are searched for a valid timestamp; the first timestamp found is
            the one that's returned: the text parameter, the name of the dataset associated with
            the current message being processed, the JSON configuration file as defined by the
            config_file_name() property, the current GMT timestamp in ISO format.
        """
        timestamp = None

        if not text is None:
            timestamp = self.extract_timestamp(text)

        if timestamp is None and not self.dataset_metadata is None:
            if 'name' in self.dataset_metadata:
                timestamp = self.extract_timestamp(self.dataset_metadata['name'])

        if timestamp is None and not self.experiment_metadata is None:
            if 'observationTimeStamp' in self.experiment_metadata:
                timestamp = self.experiment_metadata['observationTimeStamp']

        if timestamp is None:
            _zero = datetime.timedelta(0)
            class TZ(datetime.tzinfo):
                """Internal class used to format UTC timestamps properly
                """
                def utcoffset(self, _dt):
                    return _zero
                def dst(self, _dt):
                    return _zero
            timestamp = datetime.datetime.utcnow(tz=TZ()).isoformat()

        return timestamp

    def get_username_with_base_path(self, host, key, dataset_id, base_path=None):
        """Looks up the name of the user associated with the dataset. If unable to find
           the user's name from the dataset, the clowder_user variable is used instead.
           If not able to find a valid user name, the string 'unknown' is returned.

        Keyword arguments:
            host(str): the partial URI of the API path including protocol ('/api' portion and
                       after is not needed); assumes a terminating '/'
            key(str): access key for API use
            dataset_id(str): the id of the dataset belonging to the user to lookup
            base_path(str): optional starting path which will have the user name appended
        Return:
            A list of user name and modified base_path.
            The user name as defined in get_dataset_username(), or the specified clowder user
            name, or, finally, the string 'unknown'. Underscores replace whitespace, and invalid
            characters are changed to periods ('.').
            The base_path with the user name appended to it, or None if base_path is None
        """
        try:
            username = get_dataset_username(host, key, dataset_id)
        # pylint: disable=broad-except
        except Exception:
            username = None
        # pylint: enable=broad-except

        # If we don't have a name, see if a user name was specified at runtime
        if (username is None) and (not self.clowder_user is None):
            username = self.clowder_user.strip()
            un_len = len(username)
            if un_len <= 0:
                username = None

        # If we have an experiment configuation, look for a name in there
        if not self.experiment_metadata is None:
            if 'extractors' in self.experiment_metadata:
                ex_username = None
                if 'firstName' in self.experiment_metadata['extractors']:
                    ex_username = self.experiment_metadata['extractors']['firstName']
                if 'lastName' in self.experiment_metadata['extractors']:
                    ex_username = ex_username + ('' if ex_username is None else ' ') + \
                                                self.experiment_metadata['extractors']['lastName']
                if not ex_username is None:
                    username = ex_username

        # Clean up the string
        if not username is None:
            # pylint: disable=line-too-long
            username = username.replace('/', '.').replace('\\', '.').replace('&', '.').replace('*', '.').replace("'", '.').replace('"', '.').replace('`', '.')
            username = username.replace(' ', '_').replace('\t', '_').replace('\r', '_')
            # pylint: enable=line-too-long
        else:
            username = 'unknown'

        # Build up the path if the caller desired that
        new_base_path = None
        if not base_path is None:
            new_base_path = os.path.join(base_path, username)
            new_base_path = new_base_path.rstrip('/')

        return (username, new_base_path)

    def get_season_and_experiment(self, timestamp, sensor):
        """Retrieves the season and experiment from either BETYdb for TERRA REF projects,
           or the experiment configuration file.

        Keyword arguments:
            timestamp(str): The timestamp associated with the season and experiement to return
            sensor(str): Name of the sensor used to retrieve data from BETYdb

        Return:
            Returns a list of the found season name, experiment name, and experiment metadata.
            The returned season name can contain None, 'Unknown Season', of the retrieved season's
            name.
            The returned experiment name can contain None, 'Unknown Experiment', or the
            experiment's name.
            Receiving 'Unknown Season' or 'Unknown Experiment' indicates the lookup failed.
            Experiment metadata is None if it wasn't used during the lookup. Otherwise the
            loaded metadata is returned as the third item in the list (replacing None).
        Notes:
            The TERRA REF lookup is skipped if either, or both, of the timestamp or sensor
            parameters are None, or the terraref metadata was not found for the current message
            being processed.
            The experiment configuration file lookup for season and experiment values is only done
            if the TERRA REF lookup isn't performed.
        """
        season_name, experiment_name, experiment_md = \
                                                    ('Unknown Season', 'Unknown Experiment', None)

        # We first see if the caller is asking for TERRA REF metadata
        if not self.terraref_metadata is None and not timestamp is None and not sensor is None:
            season_name, experiment_name, experiment_md = \
                            get_season_and_experiment(timestamp, sensor, self.terraref_metadata)

        # Look up our fields in the experiment metadata
        if (season_name == 'Unknown Season' or experiment_name == 'Unknown Experiment') and \
                                                            (not self.experiment_metadata is None):
            if 'season' in self.experiment_metadata:
                season_name = self.experiment_metadata['season']
            if 'studyName' in self.experiment_metadata:
                experiment_name = self.experiment_metadata['studyName']

        return (season_name, experiment_name, experiment_md)

    def get_clowder_context(self):
        """Returns the best known username, password, and clowder space ID. By default the values
           as defined in the current running environment are returned. If an experiment
           configuration file was loaded, and it has clowder configuration, the values found will
           be returned. No checks are made on the validity of the returned values.

        Returns:
            A list of username, password, and clowder space ID
        """
        # Set the default return values
        ret_username, ret_password, ret_space = \
                                        (self.clowder_user, self.clowder_pass, self.clowderspace)

        # Check for overrides
        if not self.experiment_metadata is None:
            if 'clowder' in self.experiment_metadata:
                clowder_md = self.experiment_metadata['clowder']
                if 'username' in clowder_md:
                    ret_username = clowder_md['username']
                if 'password' in clowder_md:
                    ret_password = clowder_md['password']
                if 'space' in clowder_md:
                    ret_space = clowder_md['space']

        return (ret_username, ret_password, ret_space)


# BASIC UTILS -------------------------------------
def timestamp_to_terraref(timestamp):
    """Converts a timestamp to TERRA REF format
    Args:
        timestamp(str): the ISO timestamp to convert
    Returns:
        The ISO string reformatted to match the TERRA REF time & date stamp. Returns the original
        string if time units aren't specified
    Note:
        Does a simple replacement of the 'T' to '__' and stripping the fraction of seconds and
        timezone information from the string.
    """
    return_ts = timestamp

    if 'T' in timestamp:
        regex_mask = r'(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})'
        res = re.search(regex_mask, timestamp)
        if res:
            return_ts = res.group(0)
            return_ts = return_ts.replace('T', '__').replace(':', '-')

    return return_ts 

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
            "extractor_id": clowderhost + ("" if clowderhost.endswith("/") else "/") + "api/extractors/"+ extractorinfo['name'],
            "version": extractorinfo['version'],
            "name": extractorinfo['name']
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
            try:
                create_time = datetime.datetime.strptime(f['date-created'],"%a %b %d %H:%M:%S %Z %Y")
                latest_dt = datetime.datetime.strptime(latest_time, "%a %b %d %H:%M:%S %Z %Y")

                if f['filename'] == trig:
                    trig_dt = create_time

                if create_time > latest_dt:
                    latest_time = f['date-created']
                    latest_file = f['filename']
            except:
                return True

        if latest_file == trig or latest_time == trig_dt:
            return True
        else:
            return False
    else:
        # If unable to determine triggering file, return True
        return True


def contains_required_files(resource, required_list):
    """Iterate through files in resource and check if all of required list is found."""
    for req in required_list:
        found_req = False
        for f in resource['files']:
            if f['filename'].endswith(req):
                found_req = True
        if not found_req:
            return False
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


def load_yaml_file(filepath):
    """Load contents of a YAML file on disk into a Python object.

    Keyword arguments:
        filepath(str): the path to the YAML file to load
    Return:
        The python object representing the YAML file contents. None is returned if the file
        couldn't be loaded.
    """
    try:
        with open(filepath, 'r') as yamlfile:
            return yaml.safe_load(yamlfile)
    except:
        logging.error('could not load YAML file %s' % filepath)
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
def build_dataset_hierarchy(host, secret_key, clowder_user, clowder_pass, root_space,
                            season, experiment, root_coll_name, year='', month='', date='', leaf_ds_name=''):
    """This will build collections if needed in parent space.

        Typical hierarchy:
        MAIN LEVEL 1 DATA SPACE IN CLOWDER
        - Season ("Season 6")
        - Experiment ("Sorghum BAP")
        - Root collection for sensor ("stereoRGB geotiffs")
            - Year collection ("stereoRGB geotiffs - 2017")
                - Month collection ("stereoRGB geotiffs - 2017-01")
                    - Date collection ("stereoRGB geotiffs - 2017-01-01")
                        - Dataset ("stereoRGB geotiffs - 2017-01-01__01-02-03-456")

        Omitting year, month or date will result in dataset being added to next level up.
    """
    if season:
        season_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, season,
                                              parent_space=root_space)

        experiment_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, experiment,
                                                  season_collect, parent_space=root_space)

        sensor_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, root_coll_name,
                                                  experiment_collect, parent_space=root_space)
    elif experiment:
            experiment_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, experiment,
                                                          parent_space=root_space)

            sensor_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, root_coll_name,
                                                      experiment_collect, parent_space=root_space)
    else:
        sensor_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, root_coll_name,
                                                  parent_space=root_space)

    if year:
        # Create year-level collection
        year_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                "%s - %s" % (root_coll_name, year),
                                                sensor_collect, parent_space=root_space)
        #verify_collection_in_space(host, secret_key, year_collect, root_space)
        if month:
            # Create month-level collection
            month_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                     "%s - %s-%s" % (root_coll_name, year, month),
                                                     year_collect, parent_space=root_space)
            #verify_collection_in_space(host, secret_key, month_collect, root_space)
            if date:
                targ_collect = get_collection_or_create(host, secret_key, clowder_user, clowder_pass,
                                                        "%s - %s-%s-%s" % (root_coll_name, year, month, date),
                                                        month_collect, parent_space=root_space)
                #verify_collection_in_space(host, secret_key, targ_collect, root_space)
            else:
                targ_collect = month_collect
        else:
            targ_collect = year_collect
    else:
        targ_collect = sensor_collect

    target_dsid = get_dataset_or_create(host, secret_key, clowder_user, clowder_pass, leaf_ds_name,
                                        targ_collect, root_space)
    #verify_dataset_in_space(host, secret_key, target_dsid, root_space)
    return target_dsid


def build_dataset_hierarchy_crawl(host, secret_key, clowder_user, clowder_pass, root_space,
                            season=None, experiment=None, sensor=None, year=None, month=None, date=None, leaf_ds_name=None):
    """This will build collections if needed in parent space.

        Typical hierarchy:
        MAIN LEVEL 1 DATA SPACE IN CLOWDER
        - Season ("Season 6")
        - Experiment ("Sorghum BAP")
        - Root collection for sensor ("stereoRGB geotiffs")
            - Year collection ("stereoRGB geotiffs - 2017")
                - Month collection ("stereoRGB geotiffs - 2017-01")
                    - Date collection ("stereoRGB geotiffs - 2017-01-01")
                        - Dataset ("stereoRGB geotiffs - 2017-01-01__01-02-03-456")

        Omitting year, month or date will result in dataset being added to next level up.

        Start at the root collection and check children until we get to the final one.
    """
    if season and experiment and sensor:
        season_c = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, season, parent_space=root_space)
        experiment_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass, root_space, season_c, experiment)
        sensor_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass, root_space, experiment_c, sensor)
    elif sensor:
        sensor_c = get_collection_or_create(host, secret_key, clowder_user, clowder_pass, sensor, parent_space=root_space)
    else:
        sensor_c = None

    if year:
        year_c_name = "%s - %s" % (sensor, year)
        year_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass, root_space, sensor_c, year_c_name)

        if month:
            month_c_name = "%s - %s-%s" % (sensor, year, month)
            month_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass, root_space, year_c, month_c_name)

            if date:
                date_c_name = "%s - %s-%s-%s" % (sensor, year, month, date)
                targ_c = ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass, root_space, month_c, date_c_name)

            else:
                targ_c = month_c
        else:
            targ_c = year_c
    else:
        targ_c = sensor_c

    target_dsid = get_dataset_or_create(host, secret_key, clowder_user, clowder_pass, leaf_ds_name, targ_c, root_space)
    return target_dsid


def get_collection_or_create(host, secret_key, clowder_user, clowder_pass, cname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/collections?key=%s&title=%s&exact=true" % (host, secret_key, cname)
    result = requests.get(url)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_collection(host, clowder_user, clowder_pass, cname, "", parent_colln, parent_space)
    else:
        coll_id = result.json()[0]['id']
        if parent_colln:
            add_collection_to_collection(host, secret_key, parent_colln, coll_id)
        if parent_space:
            add_collection_to_space(host, secret_key, coll_id, parent_space)
        return coll_id

def get_child_collections(host, secret_key, collection_id):
    url = "%sapi/collections/%s/getChildCollections?key=%s" % (host, collection_id, secret_key)
    result = requests.get(url)
    result.raise_for_status()

    return result.json()

def create_empty_collection(host, clowder_user, clowder_pass, collectionname, description, parentid=None, spaceid=None):
    """Create a new collection in Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    clowder_user -- the username to login to clowder
    clowder_pass -- the password associated with the username
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
                                                    "parentId": parentid, "space": spaceid}),
                                   auth=(clowder_user, clowder_pass))
        else:
            url = '%sapi/collections/newCollectionWithParent' % host
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description,
                                                    "parentId": parentid}),
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
        ds_id = result.json()[0]['id']
        if parent_colln:
            add_dataset_to_collection(host, secret_key, ds_id, parent_colln)
        if parent_space:
            add_dataset_to_space(host, secret_key, ds_id, parent_space)
        return ds_id

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

def get_child_collections(host, secret_key, collectionid):
    """Get list of child collections in collection by UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    collectionid -- the collection to get children of
    """

    if collectionid:
        url = "%sapi/collections/%s/getChildCollections?key=%s" % (host, collectionid, secret_key)

        result = requests.get(url)
        result.raise_for_status()

        return json.loads(result.text)
    else:
        return []

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

def create_empty_space(host, clowder_user, clowder_pass, space_name, description=""):
    """Create a new space in Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    space_name -- name of new space to create
    """

    logger = logging.getLogger(__name__)

    url = '%sapi/spaces' % host
    result = requests.post(url, headers={"Content-Type": "application/json"},
                           data=json.dumps({"name": space_name, "description": description}),
                           auth=(clowder_user, clowder_pass))
    result.raise_for_status()

    spaceid = result.json()['id']
    logger.debug("space id = [%s]", spaceid)

    return spaceid

def get_space_or_create(host, secret_key, clowder_user, clowder_pass, space_name):
    # Fetch dataset from Clowder by name, or create it if not found
    url = "%sapi/spaces?key=%s&title=%s&exact=true" % (host, secret_key, space_name)
    result = requests.get(url)
    result.raise_for_status()

    if len(result.json()) == 0:
        return create_empty_collection(host, clowder_user, clowder_pass, space_name, "")
    else:
        return result.json()[0]['id']

def delete_file(host, secret_key, fileid):
    url = "%sapi/files/%s?key=%s" % (host, fileid, secret_key)
    result = requests.delete(url)
    result.raise_for_status()

def check_file_in_dataset(connector, host, secret_key, dsid, filepath, remove=False, forcepath=False, replacements=[]):
    # Replacements = [("L2","L1")]
    # Each tuple is checked replacing first element in filepath with second element for existing
    dest_files = get_file_list(connector, host, secret_key, dsid)

    if len(replacements) > 0:
        for r in replacements:
            filepath = filepath.replace(r[0], r[1])

    for source_path in connector.mounted_paths:
        if filepath.startswith(connector.mounted_paths[source_path]):
            filepath = filepath.replace(connector.mounted_paths[source_path], source_path)

    filename = os.path.basename(filepath)

    found_file = False
    for f in dest_files:
        if (not forcepath and f['filename'] == filename) or (forcepath and f['filepath'] == filepath):
            if remove:
                delete_file(host, secret_key, f['id'])
            found_file = True

    return found_file

def ensure_collection_in_children(host, secret_key, clowder_user, clowder_pass, parent_space, parent_coll_id, child_name):
    """Check if named collection is among parent's children, and create if not found."""
    child_collections = get_child_collections(host, secret_key, parent_coll_id)
    for c in child_collections:
        if c['name'] == child_name:
            return str(c['id'])

    # If we didn't find it, create it
    return create_empty_collection(host, clowder_user, clowder_pass, child_name, "", parent_coll_id, parent_space)

def add_dataset_to_collection(host, secret_key, dataset_id, collection_id):
    # Didn't find space, so we must associate it now
    url = "%sapi/collections/%s/datasets/%s?key=%s" % (host, collection_id, dataset_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def add_dataset_to_space(host, secret_key, dataset_id, space_id):
    # Didn't find space, so we must associate it now
    url = "%sapi/spaces/%s/addDatasetToSpace/%s?key=%s" % (host, space_id, dataset_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def add_collection_to_collection(host, secret_key, parent_coll_id, child_coll_id):
    # Didn't find space, so we must associate it now
    url = "%sapi/collections/%s/addSubCollection/%s?key=%s" % (host, parent_coll_id, child_coll_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def add_collection_to_space(host, secret_key, collection_id, space_id):
    # Didn't find space, so we must associate it now
    url = "%sapi/spaces/%s/addCollectionToSpace/%s?key=%s" % (host, space_id, collection_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def confirm_clowder_info(host, secret_key, space_id, clowder_user=None, clowder_pass=None):
    """Confirms that the information provided is valid in the clowder instance

    Keyword arguments:
        host(str): the partial URI of the API path including protocol ('/api' portion and
                   after is not needed); assumes a terminating '/'
        secret_key(str): access key for API use
        space_id(str): the id of the space to check
        clowder_user(str): the clowder username
        clowder_pass(str): the password associated with the username

    Returns:
        True is returned if the parameters appear to be good. False is returned otherwise
    """
    logger = logging.getLogger(__name__)

    # Check that we have good parameters
    if not secret_key or not space_id:
        logger.error("One or more required parameters is empty")
        return False
    if (not clowder_user and clowder_pass) or (clowder_user and not clowder_pass):
        logger.error("Both a user name and password must be specified or set to None, only one "\
                     "value was was detected")
        return False

    # Now check with clowder
    try:
        # First try to find the user name
        found = find_user_name(host, secret_key, clowder_user)
        if not found:
            logger.info("Clowder user not found by querying users: %s", clowder_user)

        # Try to find the space in Clowder
        url = '%sapi/spaces/%s?key=%s' % (host, space_id, secret_key)
        result = requests.get(url)
        result.raise_for_status()

        ret = result.json()
        found = False
        if ('id' in ret) and (ret['id'] == space_id):
            found = True
        if not found:
            logger.info("Clowder space not found: %s", space_id)
            return False
    # pylint: disable=broad-except
    except Exception as ex:
        logger.error("Exception caught checking clowder information: %s", str(ex))
        return False

    return True


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
