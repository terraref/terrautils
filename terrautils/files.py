
# https://github.com/terraref/tutorials/blob/geostreams-guide/sensors/06-list-datasets-by-plot.md
import os
import requests

import logging
log = logging.getLogger(__name__)

from terrautils.geostreams import get_sensor_by_name


# TODO this should be from the pyclowder package
def get_sensor_list(connection, host, key):
    """return a list of all sensors"""

    url = "%sapi/geostreams/streams?key=%s" % (host, key)
    r = requests.get(url)
    r.raise_for_status()
    return r.json()


def unique_sensor_names(sensors=None):
    """returns a list of unique sensor names"""

    if not sensors:
        sensors = get_sensor_list()

    rsp = set()
    for s in sensors:
        if s['name'].endswith(')'):
            rsp.add(s['name'].split('(')[0].strip())
        else:
            rsp.add(s['name'])
    return list(rsp)


def get_datapoints(connection, host, key, sensor, **kwargs):

    kwargs['key'] = key
    kwargs['stream_id'] = sensor

    url = "%sapi/geostreams/datapoints" % host
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r


def get_sensor(connection, host, key, sensor, sitename=''):

    if not sensor:
        raise RuntimeError('sensor_name parameter required')
    log.debug('sensor = %s', sensor)

    # append the sitename id if given
    if sitename:
        s = get_sensor_by_name(None, host, key, sitename)
        log.debug('>>>>>>> sensor = %s', s)
        plotid = s['id']
        if not sensor.endswith(')'):
            sensor += ' ({})'.format(plotid)
    
    url = '%sapi/geostreams/streams' % host
    params = { 'key': key, 'stream_name': sensor }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r    


def get_files(connection, host, key, dataset):
    """returns all files in a dataset"""

    dataset_id = dataset.split('/')[-1]
    if dataset_id == 'files':
        dataset_id = dataset.split('/')[-2]

    url = '%sapi/datasets/%s/files' % (host, dataset_id)
    r = requests.get(url, params={'key': key})
    r.raise_for_status()
    return r.json()


def get_file_listing(connection, host, key, sensor, sitename, 
                     since='', until=''):
    """ Query geostreams and return a list of files.

    Keyword arguments:
    connection -- connection information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sensor -- the name of the sensor
    sitename -- plot name from betydb
    since -- starting time (optional)
    until -- ending time (optional)

    """
    r = get_sensor(connection, host, key, sensor, sitename)
    stream_id = r.json()[0]['id']

    url = '%sapi/geostreams/datapoints' % host
    params = { 'key': key, 'stream_id': stream_id }
    if since:
        params['since'] = since
    if until:
        params['until'] = until

    r = requests.get(url, params=params)
    r.raise_for_status()
    datasets = [ds['properties']['source_dataset'] for ds in r.json()]

    files = []
    for ds in datasets:
        flist = get_files(connection, host, key, ds)
        if flist:
            files.extend(flist)
    return files


def extract_file_paths(listing):
    """Takes file listing and returns a list of absolute file paths"""

    return [os.path.join(l['filepath'], l['filename']) for l in listing]

