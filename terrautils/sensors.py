import os
import re



if os.path.exists('/projects/arpae/terraref/sites'):
    TERRAREF_BASE = '/projects/arpae/terraref/sites'
else:
    TERRAREF_BASE = '/home/extractor/sites'

TERRAREF_BASE = os.environ.get('TERRAREF_BASE', TERRAREF_BASE)


"""
{
    "site short (disk) name": {
        "sitename": "site long name"
        "sensors": {
            "sensor short name": {
                "grouping": { "either unstitched (raw outputs) or stitched (full field clippable mosaics)"
                    "template": "location on disk after site base (e.g. /home/extractor/sites/ua-mac/raw/PATHNAME)
                    "level": "data product level, default level (e.g. raw_data)
                    "day_folder": "whether there is a date folder between template and data, default True"
                    "timestamp_folder": "whether there is a timestamp folder between template and data, default True"
                    "patterns": ["basic representations of filename pattern if unusual
                        TZUTC = YYYY-MM-DDTHH-MM-SSZ"
                        SNAPID = Danforth snapshot ID (e.g. 295351)
                        UID = some distinct ID number that may vary per dataset
                    ],
                    "suffixes" ["basic representations of filename pattern if using extractors.get_output_filename
                        terrautils.extractors.get_output_filename(
                            "dataset (e.g. stereoTop - 2000-01-01__01-01-01-000)",
                            outextension=suffixes[i].split('.')[1],
                            site="uamac",
                            opts=[suffixes[i].split('.')[0]]
                        )
                    ]
                },
            }
        }
    }
}
"""

# 2017
year_p = '(20\d\d)'
# 06
month_p = '(0[1-9]|1[0-2])'
# 28
day_p = '(10|20|[0-2][1-9]|3[01])'
# 2017-06-28
date_p = '{}-{}-{}'.format(year_p, month_p, day_p)
# 23-48-28
time_p = '([0-1]\d|2[0-3])-([0-5]\d)-([0-5]\d)'
# 23-48-28-435
full_time_p = '([0-1]\d|2[0-3])-([0-5]\d)-([0-5]\d)-(\d{3})'
# 2017-06-28__23-48-28-435
full_date_p = '{}__{}'.format(date_p, full_time_p)

STATIONS = {
    'danforth': {
        'sitename': 'Danforth Plant Science Center '
                    'Bellweather Phenotyping Facility',
        'derived_data': {
        },
        'raw_data': {
            'ddpscIndoorSuite': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{snapshotID}/{filename}',  
                'filename': '{snapshotID}/{suffix}.png'
            }
        },
        'plantcv': {
            'template': '{TERRAREF_BASE}/{station}{level}/'
                        '{sensor}/{time}/{filename}',
            'filename': 'avg_traits.csv'
        }
    },

    'ksu': {
        'sitename': 'Ashland Bottoms KSU Field Site',
        'raw_data': {
        },

        'Level_1': {
            'dsm': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{filename}',
                'filename': '{time}_DSM_16ASH-TERRA.tif'
            },
            'rededge': {
                'template': '{TERRAREF_BASE}/{station}/{Level_1}/'
                            '{sensor}/{filename}',
                'filename': '{time}_BGREN_16ASH-TERRA'
            }
        },
        'Level_2': {
        }
    },
    'ua-mac': {
        'sitename': 'MAC Field Scanner',
        'Level_1': {
            'rgb_fullfield': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'filename': 'stereoTop_fullfield.tif'
            },
            'rgb_fullfield_1pct': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'filename': 'stereoTop_fullfield_1pct.tif'
            },
            'flirIrCamera': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': '([0-9a-f]){8}-([0-9a-f]){4}-([0-9'
                           'a-f]){4}-([0-9a-f]){4}-([0-9a-f])'
                           '{12}.(png|tif)'
            },
            'stereoTop_geotiff': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': ('stereoTop_lv1_{}__{}_uamac_(left|right)'
                            '.(jpg|tif)').format(date_p, full_time_p)
            },
            'stereoTop_canopyCover': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'filename': 'CanopyCoverTraits.csv'
            },
            'texture_analysis': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': 'stereoTop_lv1_{}__{}_uamac_texture.csv'.\
                            format(date_p, full_time_p)
            },
            'flir2tif': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': 'flirIrCamera - {}__{}.(png|tif)'.\
                            format(date_p, full_time_p)
            },
            'ps2_png': {
            },
            'bin2csv': {
            },
            'EnvironmentLogger': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'pattern': '{}_{}_environmentlogger.nc'.\
                           format(date_p, time_p)
            },
            'soil_removal_vnir': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': 'VNIR_lv1_{}__{}_{}'.format(date_p, 
                           full_time_p, 'uamac_soilremovalmask.nc')
            },
            'soil_removal_swir': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': ''
            },
            'scanner3DTop_mergedlas': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': 'scanner3DTop_(lv1_)?{}__{}.las'.\
                           format(date_p, full_time_p,
                           '(_uamac_merged | MergedPointCloud)')
            },
            'scanner3DTop_plant_height': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': ('scanner3DTop - {}__{} {}.npy'.\
                            format(date_p, full_time_p,
                              '(highest|historgram)'))
            },
            'scanner3DTop_heightmap': {
                'template': '{TERRAREF_BASE}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': ('scanner3DTop_lv1_{}__{}_{}.bmp'.\
                            format(date_p, full_time_p,
                            'uamac_heightmap(_mask)?'))
            }
        },
        'raw_data': {
            'stereoTop': {
                "fixed_metadata_datasetid": "5873a8ae4f0cad7d8131ac0e",
            },
            'flirIrCamera': {
                "fixed_metadata_datasetid": "5873a7184f0cad7d8131994a",
            },
            "co2Sensor": {
                "fixed_metadata_datasetid": "5873a9924f0cad7d8131b648"
            },
            "cropCircle": {
                "fixed_metadata_datasetid": "5873a7ed4f0cad7d8131a2e7"
            },
            "EnvironmentLogger": {
                "fixed_metadata_datasetid": "TBD"
            },
            "irrigation": {
                "fixed_metadata_datasetid": "TBD"
            },
            "lightning": {
                "fixed_metadata_datasetid": "TBD"
            },
            "ndviSensor": {
                "fixed_metadata_datasetid": "5873a8f64f0cad7d8131af54"
            },
            "parSensor": {
                "fixed_metadata_datasetid": "5873a8ce4f0cad7d8131ad86"
            },
            "priSensor": {
                "fixed_metadata_datasetid": "5873a9174f0cad7d8131b09a"
            },            
            "ps2Top": {
                "fixed_metadata_datasetid": "5873a84b4f0cad7d8131a73d"
            },
            "scanner3DTop": {
                "fixed_metadata_datasetid": "5873a7444f0cad7d81319b2b"
            },
            "stereoTop": {
                "fixed_metadata_datasetid": "5873a8ae4f0cad7d8131ac0e"
            },
            "SWIR": {
                "fixed_metadata_datasetid": "5873a79e4f0cad7d81319f5f"
            },
            "VNIR": {
                "fixed_metadata_datasetid": "5873a7bb4f0cad7d8131a0b7"
            },
            "weather": {
                "fixed_metadata_datasetid": "TBD"
            },
        }
    }

}


def exact_p(pattern):

    return '^{}$'.format(pattern)


def get_sensor_path(station, level, sensor, date='', time='', filename=''):
    """Get the appropritate path for writing sensor data

    Args:
      station (str): abbreviated name of the site
      level (str): data level (raw_data | Level_1 | Level_2)
      sensor (str): sensor name, may be a product name for Level_1
      date (YYYY-MM-DD str): optional date field
      time (datetime str): optional time field
      filename (str): option filename, must match sensor pattern
      
    Returns:
      (str) full path

    Notes:
      When no filename is given, get_sensor_path returns the desired
      path with pre-defined, well-known filename. If filename is not
      given and no pre-defined filename exists, then a RuntimeError
      is raised.

      If a filename is supplied, it must patch a pre-defined pattern.
      A RuntimeError is raised if the filename does not match the 
      pattern.
    """

    if date and re.match(exact_p(date_p), date)==None:
        raise RuntimeError('The date given is in the wrong format')

    if time and re.match(exact_p(full_date_p), time)==None:
        raise RuntimeError('The time given is in the wrong format')

    try:
        s = STATIONS[station][level][sensor]
    except KeyError:
        raise RuntimeError('The station, level or sensor given does'
                           'not exist')

    if filename:
        result = re.match(exact_p(s['pattern']), filename)
        if result==None:
            raise RuntimeError('The filename given does not match '
                               'the correct pattern')
    else:
        try:
            filename = s['filename']
        except KeyError:
            raise RuntimeError('No default filename found, you need '
                               'to specify a filename')


    path = s['template'].format(TERRAREF_BASE=TERRAREF_BASE,
                                station=station, level=level,
                                sensor=sensor, date=date, time=time,
                                filename=filename)
    return path


def get_sensor_path_by_dataset(station, level, datasetname, sensor='', extension='', opts=[], hms=None):
    """Get the appropritate path for writing sensor data

    Args:
      station (str): abbreviated name of the site
      level (str): data level (raw_data | Level_1 | Level_2)
      datasetname (sensor - date__timestamp str): e.g. VNIR - 2017-06-28__23-48-28-435
      sensor (str): sensor name, may be a product name for Level_1
      extension (str): file extension for generated filename
      opts (list of strs): any suffixes to apply to generated filename
      hms (str): allows one to add/override a timestamp if not included in datasetname (e.g. EnvironmentLogger)

    Returns:
      (str) full path
    """

    # Split dataset into sensorname and timestamp portions
    if datasetname.find(" - ") > -1:
        sensorname = datasetname.split(" - ")[0]
        time = datasetname.split(" - ")[1]
    else:
        sensorname = datasetname
        time = "2017"
    # Get date portion of timestamp if there is one; if not, assume whole thing is just the date
    if time.find("__") > -1:
        date = time.split("__")[0]
    else:
        date = time
        time = ""
    # Override timestamp if necessary
    if hms:
        time = date + "__" + hms
    # Override dataset sensorname with provided name if given (e.g. use timestamp of raw while getting Level_1 output)
    if sensor == '':
        sensor = sensorname

    filename = get_sensor_filename(station, level, sensor, time, extension, opts)

    return get_sensor_path(station, level, sensor, date, time, filename)


def get_sensor_filename(station, level, sensor, time, extension, opts):
    """Determine output filename given input information."""

    # Use shortland for levels
    if level.find("Level_") > -1:
        level = level.replace("Level_", "lv")
    # If extension included a period, remove it
    if extension != '':
        extension = '.' + extension.replace('.', '')

    return "_".join([sensor, level, station, time]+opts) + extension


def create_sensor_path(path):
    """
    Check if the path exists. If it exists, then do nothing.
    If not, then create all the missing subdirectories.
    """
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


def get_sensors(station):
    """ Get all sensors for a given station."""
    return STATIONS[station]['sensors'].keys()


def get_sites():
    """ Get all sites (stations) listed."""
    return STATIONS.keys()


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

    if not os.path.exists(TERRAREF_BASE):
        raise InvalidUsage('Could not find data, try setting TERRAREF_BASE environmental variable')

    sitepath = os.path.join(TERRAREF_BASE, station)
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
    
def get_fixed_datasetid_for_sensor(station, sensor):
    """ Returns the Clowder dataset ID for the fixed sensor information
    """
    return STATIONS[station]["raw_data"][sensor]["fixed_metadata_datasetid"]    


if __name__ == '__main__':
    try:
        print get_sensor_path('ua-mac', 'Level_1', 'rgb_fullfield', '2017-04-27')
    except Exception as e:
        print(e)

    try:
        print get_sensor_path('ua-mac', 'Level_1', 'rgb_fullfield', '20127')
    except Exception as e:
        print(e)

    try:
        print get_sensor_path('ua-mac', 'Level_1', 'stereoTop_geotiff', '2017-04-27', '2017-04-27__13-48-56-082', 'stereoTop_lv1_2016-04-29__13-53-42-743_uamac_left.jpg')
    except Exception as e:
        print(e)

    try:
        print get_sensor_path('ua-mac', 'Level_1', 'stereoTop_geotiff', '2017-04-27', '13-48-56-082', 'stereoTop_lv1_2016-04-29__13-53-42-743_uamac_left.jpg')
    except Exception as e:
        print(e)

    try:
        print get_sensor_path('ua', 'Level_1', 'flirIrCamera', '2017-04-27', '2017-04-27__23-12-13-999', 'uamac_soilremovalmask.nc')
    except Exception as e:
        print(e)

    try:
        print get_sensor_path('ua-mac', 'Level_1', 'flirIrCamera', '2017-04-27', '2017-04-27__23-12-13-999', 'uamac_soilremovalmask.nc')
    except Exception as e:
        print(e)

    try:
        print get_sensor_path('ua-mac', 'Level_1', 'flirIrCamera', '2017-04-27', '2017-04-27__23-12-13-999', '')
    except Exception as e:
        print(e)

    try:
        print get_sensor_path('ua-mac', 'Level_1', 'scanner3DTop_heightmap', '2017-04-27', '2017-04-27__15-16-17-888', 'scanner3DTop_lv1_2017-06-06__17-14-31-218_uamac_heightmap_mask.bmp')
    except Exception as e:
        print(e)
