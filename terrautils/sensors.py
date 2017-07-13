import os

import terrautils.extractors


if os.path.exists('/projects/arpae/terraref'):
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
                    "pathname": "location on disk after site base (e.g. /home/extractor/sites/ua-mac/raw/PATHNAME)
                    "level": "data product level, default Level_1 (e.g. raw_data)
                    "day_folder": "whether there is a date folder between pathname and data, default True"
                    "timestamp_folder": "whether there is a timestamp folder between pathname and data, default True"
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
STATIONS = {
    'danforth': {
        'sitename': 'Danforth Plant Science Center Bellweather Phenotyping Facility',
        'sensors': {
            'ddpscIndoorSuite': {
                "unstitched": {
                    'pathname': 'sorghum_pilot_dataset',
                    'level': 'raw_data',
                    'day_folder': False,
                    'timestamp_folder': False,
                    'patterns': [
                        'snapshotSNAPID/NIR_SV_0_z1_h1_g0_e35_UID.png',
                        'snapshotSNAPID/NIR_SV_90_z1_h1_g0_e35_UID2.png',
                        'snapshotSNAPID/NIR_SV_180_z1_h1_g0_e35_UID3.png',
                        'snapshotSNAPID/NIR_SV_270_z1_h1_g0_e35_UID4.png',
                        'snapshotSNAPID/NIR_TV_z1_h0_g0_e50_UID5.png',
                        'snapshotSNAPID/VIS_SV_0_z1_h1_g0_e82_UID6.png',
                        'snapshotSNAPID/VIS_SV_90_z1_h1_g0_e82_UID7.png',
                        'snapshotSNAPID/VIS_SV_180_z1_h1_g0_e82_UID8.png',
                        'snapshotSNAPID/VIS_SV_270_z1_h1_g0_e82_UID9.png',
                        'snapshotSNAPID/VIS_TV_z1_h0_g0_e110_UID10.png'
                    ]
                }
            },
            'plantcv': {
                "unstitched": {
                    'pathname': 'plantcv',
                    'day_folder': False,
                    'patterns': ['avg_traits.csv']
                }
            }
        }
    },
    'ksu': {
        'sitename': 'Ashland Bottoms KSU Field Site',
        'sensors': {
            'dsm': {
                "stitched": {
                    'pathname': 'dsm_fullfield',
                    'day_folder': False,
                    'timestamp_folder': False,
                    'patterns': ['TZUTC_DSM_16ASH-TERRA.tif']
                }
            },
            'rededge': {
                "stitched": {
                    'pathname': 'rededge_fullfield',
                    'day_folder': False,
                    'timestamp_folder': False,
                    'patterns': ['TZUTC_BGREN_16ASH-TERRA.tif']
                }
            },
            'height': {
                "stitched": {
                    'level': 'Level_2'
                }
            },
            'ndvi': {
                "stitched": {
                    'level': 'Level_2'
                }
            }
        }
    },
    'ua-mac': {
        'sitename': 'MAC Field Scanner',
        'sensors': {
            'stereoTop': {
                "unstitched": {
                    "pathname": "stereoTop_geotiff",
                    'suffixes': [
                        "left.jpg", "right.jpg",
                        "left.tif", "right.tif"
                    ]
                },
                "stitched": {
                    "pathname": "fullfield",
                    "timestamp_folder": False,
                    "patterns": [
                        "stereoTop_fullfield.tif",
                        "stereoTop_fullfield_10pct.tif"
                    ]
                }
            },
            'flirIrCamera': {
                "unstitched": {
                    "pathname": "flir2tif",
                    'suffixes': [".png", ".tif"]
                },
                "stitched": {
                    "pathname": "fullfield",
                    "timestamp_folder": False,
                    "patterns": [
                        "flirIrCamera_fullfield.tif",
                        "flirIrCamera_fullfield_10pct.tif"
                    ]
                }
            }
        }
    }
}


def get_sensors(station):
    """ Get all sensors for a given station."""
    return STATIONS[station]['sensors'].keys()


def get_sites():
    """ Get all sites (stations) listed."""
    return STATIONS.keys()


def get_file_paths(station, sensor, date, stitched=True):
    """ Gets the filenames for the dataset for the given date, sensor
    and station.

    Args:
      station: the name of the station the sensor belongs to
      sensor: the name of the sensor
      date: the date or timestamp when the dataset was captured
      stitched: determines whether we use the stitched dataset or the unstitched one

    Returns:
      (str) the names of the dataset files for the given date, sensor and station

    """
    results = []

    if station in STATIONS:
        if sensor in STATIONS[station]['sensors']:
            sens = STATIONS[station]['sensors'][sensor]
            if stitched and 'stitched' in sens:
                dsinfo = sens['stitched']
            elif (not stitched) and ('unstitched' in sens):
                dsinfo = sens['unstitched']
            else:
                return results

            ds_root = os.path.join(TERRAREF_BASE, station,
                                   dsinfo['level'] if 'level' in dsinfo else 'Level_1',
                                   dsinfo['pathname'],
                                   date.split("__")[0] if 'day_folder' not in dsinfo else (
                                       date.split("__")[0] if dsinfo['day_folder'] else ''),
                                   date if 'timestamp_folder' not in dsinfo else (
                                       date if dsinfo['timestamp_folder'] else '')
                                   )

            if 'suffixes' in dsinfo:
                for sfx in dsinfo['suffixes']:
                    fname = terrautils.extractors.get_output_filename(
                            sensor + " - " + date,
                            outextension=sfx.split('.')[1],
                            site=station.replace("-", ""),
                            opts=[sfx.split('.')[0]]
                    )
                    results.append(os.path.join(ds_root, fname))
            elif 'patterns' in dsinfo:
                for pat in dsinfo['patterns']:
                    # TODO: Handle Danforth and KSU files better here
                    results.append(os.path.join(ds_root, pat))

    return results
        

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


def get_sensor_product(sensor="stereoTop", site="MAC"):
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
