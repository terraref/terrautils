import os
import requests
import json
from flask import safe_join, request
from plot_service import app
from plot_service.exceptions import InvalidUsage

TERRAREF_BASE = '/projects/arpae/terraref'

STATIONS = {
        'Danforth': {
            'sitename': 'Danforth Plant Science Center Bellweather' +
                        'Phenotyping Facility',
            'pathname': 'danforth',
            'sensors': {
                'plantcv': {
                    'pathname': 'plantcv',
                    'hasplot': False,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    }
                }
            },
        'KSU': {
            'sitename': 'Ashland Bottoms KSU Field Site',
            'pathname': 'ksu',
            'sensors': {
                'dsm': {
                    'pathname': 'dsm_fullfield',
                    'hasplot': False,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    },
                'rededge': {
                    'pathname': 'rededge_fullfield',
                    'hasplot': False,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    }
                }
            },
        'ua-mac': { 
            'sitename': 'MAC Field Scanner', 
            'pathname': 'ua-mac',
            'sensors': { 
                'stereoTop': { 
                    'pathname': 'fullfield', 
                    'hasplot': True,
                    'full': 'stereoTop.tif',
                    'reduced': 'ff.tif'
                    },
                'FLIR': { 
                    'pathname': 'flir',
                    'hasplot': True,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    },
                'hyperspectral': { 
                    'pathname': 'hyperspectral',
                    'hasplot': True,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    },
                'EnvironmentLogger': {
                    'pathname': 'EnvironmentLogger',
                    'hasplot': True,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    },
                'scanner3DTop': {
                    'pathname': 'scanner3DTop',
                    'hasplot': True,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    },
                'scanner3DTop_plant_height': {
                    'pathname': 'scanner3DTop_plant_height',
                    'hasplot': True,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    },
                'soil_removal': {
                    'pathname': 'soil_removal',
                    'hasplot': True,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    },
                'stereoTop_canopyCover': {
                    'pathname': 'stereoTop_canopyCover',
                    'hasplot': True,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    },
                'texture_analysis': {
                    'pathname': 'texture_analysis',
                    'hasplot': True,
                    'full': 'fullfield.tif',
                    'reduced': 'reduced.tif'
                    }
                }
            }
        }


def get_sensors(station):
    return STATIONS[station]['sensors'].keys()


def get_sensor_filename(station, sensor, date, mode="full"):
   if mode=='full':
       return STATIONS[station]['sensors'][sensor]['full']
   else: 
       return STATIONS[station]['sensors'][sensor]['reduced']
        


def get_sitename(site, date, range_=None, column=None):
    """ Return a full sitename given site, date, range and column """

    site = STATIONS[site]['sitename']
    season = int(date[:4])-2013
    sitename = '{} Season {} Range {} Column {}'.\
              format(site, season, range_, column)
    return sitename


def check_site(site):
    """ check for valid site given the site name """

    terraref = os.environ.get('TERRAREF_BASE', TERRAREF_BASE)
    if not os.path.exists(terraref):
        raise InvalidUsage('Could not find TerraREF data, try setting '
                           'TERRAREF_BASE environmental variable')

    sitepath = safe_join(terraref, 'sites', site)
    if not os.path.exists(sitepath):
        raise InvalidUsage('unknown site', payload={'site': site})

    return sitepath


def check_sensor(site, sensor, date=None):
    """check for valid sensor with optional date """

    sitepath = check_site(site)

    sensorpath = safe_join(sitepath, 'Level_1', sensor)
    if not os.path.exists(sensorpath):
        raise InvalidUsage('unknown sensor',
                           payload={'site': site, 'sensor': sensor})

    if not date:
        return sensorpath

    datepath = safe_join(sensorpath, date)
    print("datepath = {}".format(datepath))
    if not os.path.exists(datepath):
        raise InvalidUsage('sensor data not available for given date',
                           payload={'site': site, 'sensor': sensor,
                                    'date': date})

    return datepath


def get_sensor_product(site, sensor):
    """Returns the downloadable product for each site-sensor pair"""

    # TODO do something much more intelligent
    return "ff.txt"


def get_attachment_name(site, sensor, date, product):
    """Encodes site, sensor, and date to create a unqiue attachment name"""

    root, ext = os.path.splitext(product)
    return "{}-{}-{}.{}".format(site, sensor, date, ext)


def plot_attachment_name(sitename, sensor, date, product):
    """Encodes sitename, sensor, and date to create a unqiue attachment name"""

    root, ext = os.path.splitext(product)
    return "{}-{}-{}.{}".format(sitename, sensor, date, ext) 
