
# https://github.com/terraref/tutorials/blob/geostreams-guide/sensors/06-list-datasets-by-plot.md
import os
import requests

import logging
log = logging.getLogger(__name__)

from terrautils.geostreams import get_sensor_by_name


# TODO this should be from the pyclowder package
def get_sensor_list(connection, host, key):
    """ Return a list of sensors from the geostream database
    
    Sensors are best thought of as data products can include derived data
    in addition to data collected by sensors. Sensor names returned 
    may include a plot id number associated with a sitename.

    Keyword arguments:
    connection -- connection information, used to get missing 
      parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    """

    url = "%sapi/geostreams/streams?key=%s" % (host, key)
    r = requests.get(url)
    r.raise_for_status()
    return r.json()


def unique_sensor_names(sensors):
    """ Return a list of unique sensor names from a sensor list

    Takes a list of sensors (get_sensor_list) and creates a set
    of unique names by removing the plot id.
    """

    rsp = set()
    for s in sensors:
        if s['name'].endswith(')'):
            rsp.add(s['name'].split('(')[0].strip())
        else:
            rsp.add(s['name'])

    return list(rsp)


def get_datapoints(connection, host, key, sensor_id, **params):
    """ Return a list of datapoints

    Keyword arguments:
    connection -- connection information, used to get missing 
      parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sensor_id -- the 
    """

    params['key'] = key
    params['stream_id'] = sensor

    url = "%sapi/geostreams/datapoints" % host
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()


def get_sensor(connection, host, key, sensor, sitename=''):
    """ Return the geostream stream dictionary for the sensor.

    Matches the specific sensor name. If sitename is given an 
    additional query is made to determine the plot (sitename) id and
    it is automatically append to the sensor name.

    Keyword arguments:
    connection -- connection information, used to get missing parameters
      and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sensor -- the name of the sensor
    sitename -- plot name from betydb (optional)
    """

    # if sitename is given, look up id and append to sensor name
    if sitename:
        s = get_sensor_by_name(None, host, key, sitename)
        if s:
            plotid = s['id']
            if not sensor.endswith(')'):
                sensor += ' ({})'.format(plotid)
        else:
            return None

    log.debug('full sensor name = %s', sensor)
    
    url = '%sapi/geostreams/streams' % host
    params = { 'key': key, 'stream_name': sensor }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

# TODO this is probably a pyclowder function
# NOTE this function extracts the dataset id from the URI and creates
# a new URI based on the host variable. Is this the best approach?
def get_files(connection, host, key, dataset):
    """ Returns a list of files for the given dataset.

    Keyword arguments:
    connection -- connection information, used to get missing parameters
      and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    dataset - a URI to the dataset on clowder
    """

    log.debug('original dataset uri = %s', dataset)

    # extract the id from the dataset URI
    dataset_id = dataset.split('/')[-1]
    if dataset_id == 'files':
        dataset_id = dataset.split('/')[-2]

    url = '%sapi/datasets/%s/files' % (host, dataset_id)
    log.debug('new url = %s', url)
    r = requests.get(url, params={'key': key})
    r.raise_for_status()
    return r.json()


def get_file_listing(connection, host, key, sensor, sitename, 
                     since='', until=''):
    """ Return a list of clowder file records for a sensor.
    
    Queries geostrteams to get a list of datasets associated with a
    sensor. If sitename is given it is automatically append to the sensor
    name. The since and until parameters can be used to limit the time
    range.

    After getting a list of datasets, a Clowder query is made for each
    dataset to get the list of files. The aggregate list is returned.

    Keyword arguments:
    connection -- connection information, used to get missing parameters 
      and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sensor -- the name of the sensor
    sitename -- plot name from betydb
    since -- starting time (optional)
    until -- ending time (optional)
    """

    files = []

    sens = get_sensor(connection, host, key, sensor, sitename)
    if sens:
        if 'status' in sens and sens['status'] == 'No data found':
            log.info("No sensor found for %s" % sensor+" - "+sitename)
            return []


        stream_id = sens[0]['id']

        url = '%sapi/geostreams/datapoints' % host
        params = { 'key': key, 'stream_id': stream_id }
        if since:
            params['since'] = since
        if until:
            params['until'] = until

        r = requests.get(url, params=params)
        r.raise_for_status()

        if len(r.json()) > 0:
            datasets = [ds['properties']['source_dataset'] for ds in r.json()]

            for ds in datasets:
                flist = get_files(connection, host, key, ds)
                if flist:
                    files.extend(flist)
        else:
            log.info("No datasets found for %s" % sensor+" - "+sitename)
    else:
        log.info("No sensor found for %s" % sensor+" - "+sitename)

    return files


def extract_file_paths(listing):
    """Takes file listing and returns a list of absolute file paths"""

    return [os.path.join(l['filepath'], l['filename']) for l in listing]

