"""Extractors

This module contains dictionaries and references for handling sensor information.
"""

import os
import re
import datetime

from terrautils.betydb import get_experiments

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
filename_level_p = '(raw|lv1|lv2)'
path_level_p = '(raw_data|Level_1|Level_2)'


STATIONS = {
    'danforth': {
        'title': 'Danforth Plant Science Center '
                    'Bellweather Phenotyping Facility',
        'raw_data': {
            'ddpscIndoorSuite': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{snapshotID}/{filename}',
                'pattern': 'avg_traits.csv'
            }
        }
    },

    'ksu': {
        'title': 'Ashland Bottoms KSU Field Site',
        'raw_data': {
        },

        'Level_1': {
            'dsm': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{filename}',
                'pattern': '{time}_DSM_16ASH-TERRA.tif'
            },
            'rededge': {
                'template': '{base}/{station}/{Level_1}/'
                            '{sensor}/{filename}',
                'pattern': '{time}_BGREN_16ASH-TERRA'
            }
        },
        'Level_2': {
        }
    },

    'ua-mac': {
        'title': 'MAC Field Scanner',
        'raw_data': {
            'co2Sensor': {
                "fixed_metadata_datasetid": "5873a9924f0cad7d8131b648",
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': '([0-9a-f]){8}-([0-9a-f]){4}-([0-9'
                           'a-f]){4}-([0-9a-f]){4}-([0-9a-f])'
                           '{12}_(metadata.json|rawData0000.bin)'
            },
            
            'stereoTop': {
                "fixed_metadata_datasetid": "5873a8ae4f0cad7d8131ac0e"
            },
            
            'flirIrCamera': {
                "fixed_metadata_datasetid": "5873a7184f0cad7d8131994a"
            },
            
            "cropCircle": {
                "fixed_metadata_datasetid": "5873a7ed4f0cad7d8131a2e7"
            },
            
            "irrigation": {
                "fixed_metadata_datasetid": "TBD"
            },
            
            'EnvironmentLogger': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'pattern': '{timestamp}_environment_logger.json',
                'fixed_metadata_datasetid': 'TBD'
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
            }
        },

        'Level_1': {

            'fullfield': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{date}{opts}.tif',
            },

            'flirIrCamera': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            'stereoTop_geotiff': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            'stereoTop_canopyCover': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            'texture_analysis': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.csv',
            },

            'flir2tif': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            'EnvironmentLogger': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.nc',
            },

            'soil_removal_vnir': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': 'VNIR_{level}_{station}_{timestamp}{opts}.nc'
            },
            'soil_removal_swir': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': 'SWIR_{level}_{station}_{timestamp}{opts}.nc'
            },

            'scanner3DTop_mergedlas': {
                'template': '{base}/{station}/{level}/'
                            'scanner3DTop/{date}/{timestamp}/{filename}',
                'pattern': 'scanner3DTop_{level}_{station}_{timestamp}'
                           '_merged{opts}.las'
            },

            'scanner3DTop_plant_height': {
                'template': '{base}/{station}/{level}/'
                            'scanner3DTop/{date}/{timestamp}/{filename}',
                'pattern': 'scanner3DTop_{level}_{station}_{timestamp}'
                           '_height{opts}.npy'
            },

            'scanner3DTop_heightmap': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{timestamp}/{filename}',
                'pattern': 'scanner3DTop_{level}_{station}_{timestamp}'
                           '_heightmap{opts}.bmp'
            },
        },
    },
}


def _level_names(self, level):
    """Convert level path name to level file name."""

    LV_CONV = {
        'raw_data': 'raw',
        'Level_1': 'lv1',
        'Level_2': 'lv2'
    }

    return LV_CONV[level]


def add_arguments(parser):
    if os.path.exists('/projects/arpae/terraref/sites'):
        TERRAREF_BASE = '/projects/arpae/terraref/sites'
    else:
        TERRAREF_BASE = '/home/extractor/sites'

    parser.add_argument('--terraref_base', type=str,
                        default=os.getenv('TERRAREF_BASE', TERRAREF_BASE),
                        help='Terraref base path')

    parser.add_argument('--terraref_site', type=str,
                        default=os.getenv('TERRAREF_SITE', 'ua-mac'),
                        help='station name')

    # TODO: Hide this and use dict to fill in this piece based on product/sensor
    parser.add_argument('--terraref_level', type=str,
                        default=os.getenv('TERRAREF_LEVEL', 'Level_1'))

    parser.add_argument('--terraref_sensor', type=str,
                        default=os.getenv('TERRAREF_SENSOR', ''))


def exact_p(pattern):

    return '^{}$'.format(pattern)


class Sensors():
    """Simple class used to save components of the file system path
    where sensor data will be stored.
    """

    def __init__(self, base, station, level, sensor):
        """Initialize basic path elements from env and cmdline."""

        self.base = base.rstrip('/')

        if station in STATIONS.keys():
            self.station = station
        else:
            raise AttributeError('unknown station name "{}"'.format(station))

        if level in STATIONS[station].keys():
            self.level = level
        else: 
            raise AttributeError('unknown data level "{}"'.format(level))

        if sensor in STATIONS[station][level].keys():
            self.sensor = sensor
        else:
            raise AttributeError('unknown sensor name "{}"'.format(sensor))


    def get_sensor_path(self, timestamp, sensor='', filename='', opts=None, ext=''):
        """Get the appropriate path for writing sensor data

        Args:
          time (datetime str): timestamp string
          filename (str): option filename, must match sensor pattern

        Returns:
          (str) full path to file

        Notes:
          When no filename is given, get_sensor_path returns the desired
          path with pre-defined, well-known filename. If filename is not
          given and no pre-defined filename exists, then a RuntimeError
          is raised.

          If a filename is supplied, it must match a pre-defined pattern.
          A RuntimeError is raised if the filename does not match the
          pattern.
        """

        # override class sensor
        if not sensor:
            sensor = self.sensor
        # create opt string
        if opts:
            opts = '_'.join(['']+opts)
        else:
            opts = ''
        # split timestamp into date and hour-minute-second components
        if timestamp.find('__') > -1:
            date, hms = timestamp.split('__')
        else:
            date = timestamp
            timestamp, hms = '', ''

        # Get regex patterns for this site/sensor
        try:
            s = STATIONS[self.station][self.level][sensor]
        except KeyError:
            raise RuntimeError('The site, level or sensor given does not exist')

        # If filename is given when getting path, validate it
        if filename:
            try:
                pattern = exact_p(s['pattern']).format(sensor='\D*',
                              level='(raw|lv1|lv2)', station='\D*',
                              date=date_p, time=full_time_p,
                              timestamp=full_date_p, opts='\D*')
            except KeyError:
                raise RuntimeError('Some fields from the pattern are not available')

            result = re.match(pattern, filename)
            if result==None:
                raise RuntimeError('The filename given does not match the correct pattern')

        # If not given, generate filename from parameters
        else:
            filename = s['pattern'].format(station=self.station,
                    level=_level_names(self.level), sensor=sensor,
                    timestamp=timestamp, date=date, time=hms, opts=opts)
            # Override pattern extension if necessary
            if ext:
                if not ext.startswith('.'):
                    ext = '.' + ext
                filename = os.path.splitext(filename)[0] + ext

        # Return fully formed path with generated/validated filename
        return s['template'].format(base=self.base, station=self.station,
                                    level=self.level, sensor=self.sensor,
                                    timestamp=timestamp, date=date, time=hms,
                                    filename=filename)


    def get_sensor_path_by_dataset(self, dsname, sensor='', ext='', opts=None, hms=''):
        """Get the appropritate path for writing sensor data

        Args:
          datasetname (sensor - date__timestamp str):
              e.g. VNIR - 2017-06-28__23-48-28-435
          sensor (str): sensor name, may be a product name for Level_1
          opts (list of strs): any suffixes to apply to generated filename
          hms (str): allows one to add/override a timestamp
          if not included in datasetname (e.g. EnvironmentLogger)

        Returns:
          (str) full path
        """

        # Split dataset into sensorname and timestamp portions
        if dsname.find(" - ") > -1:
            sensorname, time = dsname.split(" - ")
        else:
            sensorname = dsname
            time = "2017-01-01"

        # Override/add timestamp if necessary
        if hms:
            if time.find("__") > -1:
                date = time.split("__")[0]
            else:
                date = time
            time = date + "__" + hms

        # Override dataset sensor name with provided name if given
        # (e.g. use timestamp of raw while getting Level_1 output)
        if not sensor:
            sensor = sensorname

        return self.get_sensor_path(time, sensor=sensor, opts=opts, ext=ext)


    def get_fixed_datasetid_for_sensor(self, site, level, sensor):
        """ Returns the Clowder dataset ID for the fixed sensor information
        """

        if not site:
            site = self.station
        if not level:
            level = self.level
        if not sensor:
            sensor = self.sensor

        return STATIONS[site][level][sensor]['fixed_metadata_datasetid']


    def create_sensor_path(self, path):
        """Create path if does not exist."""

        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))


    def get_sites(self):
        """Get all sites (stations) listed."""

        return STATIONS.keys()


    def get_season(self, date):
        experiments = get_experiments()

        # We only care about date portion if timestamp is given
        if date.find("__") > -1:
            date = date.split("__")[0]

        ds_time = datetime.datetime.strptime(date, "%Y-%m-%d")
        for exp in experiments:
            begin = datetime.datetime.strptime(exp['start_date'], "%Y-%m-%d")
            end = datetime.datetime.strptime(exp['end_date'], "%Y-%m-%d")

            if ds_time >= begin and ds_time <= end:
                return exp['name'][:exp['name'].find(':')]


    def get_sensors(self):
        """Get all sensors for a given station."""

        return STATIONS[self.station][self.level].keys()


    def check_site(self, station):
        """ Checks for valid station given the station name, and return its
        path in the file system.
        """

        if not os.path.exists(self.base):
            raise InvalidUsage('Could not find data, try setting '
                               'TERRAREF_BASE environmental variable')

        sitepath = os.path.join(self.base, self.station)
        if not os.path.exists(sitepath):
            raise InvalidUsage('unknown site', payload={'site': station})

        return sitepath


    def check_sensor(self, station, sensor, date=None):
        """ Checks for valid sensor with optional date, and return its path
        in the file system.
        """

        sitepath = self.check_site(station)

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
