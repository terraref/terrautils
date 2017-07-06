import os
import requests
import json

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
    """ Get all sensors for a given station.
    """
    return STATIONS[station]['sensors'].keys()


def get_sensor_filename(station, sensor, date, mode="full"):
    """ Gets the filename for the image for the given date, sensor
    and station from the database. If the mode is full, choose 
    the full resolution image, otherwise the reduced resolution 
    version.

    Args:
      station (str): the name of the station the sensor belongs to
      sensor (str): the name of the sensor
      date (str): the date when the image was taken
      mode (str): determines whether we use the name of the full
      resolution image or the reduced one

    Returns:
      (str) the name of the image for the given date, sensor and station

    """
    if mode=='full':
        return STATIONS[station]['sensors'][sensor]['full']
    else: 
        return STATIONS[station]['sensors'][sensor]['reduced']
        


def get_sitename(station, date, range_=None, column=None):
    """ Returns a full sitename for the plot (or fullfield image)
    corresponding to the given station, date, range and column.

    Args:
      station (str): the name of the station
      date (str): the date when the image was taken
      range_ (str): the vertical index of the plot in the fullfield
      column (str): the horizontal index of the plot in the fullfield

    Returns:
      (str): the full sitename for the plot (or the fullfield image)
    """

    site = STATIONS[station]['sitename']
    season = int(date[:4])-2013
    sitename = '{} Season {} Range {} Column {}'.\
              format(site, season, range_, column)
    return sitename


def check_site(station):
    """ Checks for valid station given the station name, and return its
    path in the file system.
    """

    terraref = os.environ.get('TERRAREF_BASE', TERRAREF_BASE)
    if not os.path.exists(terraref):
        raise InvalidUsage('Could not find TerraREF data, try setting '
                           'TERRAREF_BASE environmental variable')

    sitepath = os.path.join(terraref, 'sites', station)
    if not os.path.exists(sitepath):
        raise InvalidUsage('unknown site', payload={'site': station})

    return sitepath


def check_sensor(station, sensor, date=None):
    """ Checks for valid sensor with optional date, and return its path
    in the file system.
    """

    sitepath = check_site(station)

    sensorpath = os.path.join(sitepath, 'Level_1', sensor)
    if not os.path.exists(sensorpath):
        raise InvalidUsage('unknown sensor',
                           payload={'site': station, 'sensor': sensor})

    if not date:
        return sensorpath

    datepath = os.path.join(sensorpath, date)
    print("datepath = {}".format(datepath))
    if not os.path.exists(datepath):
        raise InvalidUsage('sensor data not available for given date',
                           payload={'site': station, 'sensor': sensor,
                                    'date': date})

    return datepath


# TODO return from dictionary!
def get_sensor_product(site, sensor):
    """ Returns the downloadable product for each site-sensor pair.
    """

    # TODO do something much more intelligent
    return "stereoTop_fullfield.tif"


def get_attachment_name(site, sensor, date, product):
    """ Encodes site, sensor, and date to create a unique attachment name.
    """

    root, ext = os.path.splitext(product)
    return "{}-{}-{}.{}".format(site, sensor, date, ext)


def plot_attachment_name(sitename, sensor, date, product):
    """ Encodes sitename, sensor, and date to create a unqiue attachment name.
    """

    root, ext = os.path.splitext(product)
    return "{}-{}-{}.{}".format(sitename, sensor, date, ext) 
