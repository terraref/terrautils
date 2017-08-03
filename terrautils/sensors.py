import os
import re


year_p = '(20\d\d)'
month_p = '(0[1-9]|1[0-2])'
day_p = '(10|20|[0-2][1-9]|3[01])'
date_p = '{}-{}-{}'.format(year_p, month_p, day_p)
time_p = '([0-1]\d|2[0-3])-([0-5]\d)-([0-5]\d)'
full_time_p = '([0-1]\d|2[0-3])-([0-5]\d)-([0-5]\d)-(\d{3})'
full_date_p = '{}__{}'.format(date_p, full_time_p)

STATIONS = {
    'danforth': {
        'title': 'Danforth Plant Science Center '
                    'Bellweather Phenotyping Facility',
        'raw_data': {
            'ddpscIndoorSuite': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{snapshotID}/{filename}',  
                'pattern': '{snapshotID}/{suffix}.png'
            }
        },

        'Level_1': {
            'plantcv': {
                'template': '{base}/{station}{level}/'
                            '{sensor}/{time}/{filename}',
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
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': '([0-9a-f]){8}-([0-9a-f]){4}-([0-9'
                           'a-f]){4}-([0-9a-f]){4}-([0-9a-f])'
                           '{12}_(metadata.json|rawData0000.bin)'
            },

            'EnvironmentLogger': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'pattern': '{timestamp}_environment_logger.json'
            },
        },

        'Level_1': {

            'rgb_fullfield': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            'flirIrCamera': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            'stereoTop_geotiff': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            'stereoTop_canopyCover': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            'texture_analysis': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.csv',
            },

            'flir2tif': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.tif',
            },

            # {date}_{time}_environmentlogger.nc
            'EnvironmentLogger': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{filename}',
                'pattern': '{sensor}_{level}_{station}_{timestamp}{opts}.nc',
            },

            'soil_removal_vnir': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': 'VNIR_{level}_{station}_{timestamp}{opts}.nc'
            },

            'soil_removal_swir': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': 'SWIR_{level}_{station}_{timestamp}{opts}.nc'
            },

            'scanner3DTop_mergedlas': {
                'template': '{base}/{station}/{level}/'
                            'scanner3DTop/{date}/{time}/{filename}',
                'pattern': 'scanner3DTop_{level}_{station}_{timestamp}'
                           '_merged{opts}.las'
            },

            'scanner3DTop_plant_height': {
                'template': '{base}/{station}/{level}/'
                            'scanner3DTop/{date}/{time}/{filename}',
                'pattern': 'scanner3DTop_{level}_{station}_{timestamp}'
                           '_height{opts}.npy'
            },

            'scanner3DTop_heightmap': {
                'template': '{base}/{station}/{level}/'
                            '{sensor}/{date}/{time}/{filename}',
                'pattern': 'scanner3DTop_{level}_{station}_{timestamp}'
                           '_heightmap{opts}.bmp'
            },
        },
    },
}


def add_arguments(parser):

    parser.add_argument('--terraref_base', type=str,
                        default=os.environ.get('TERRAREF_BASE',
                        '/projects/arpae/terraref/sites'),
                        help='Terraref base path')

    parser.add_argument('--terraref_site', type=str,
                        default=os.environ.get('TERRAREF_SITE',
                        'ua-mac'), help='station name')

    parser.add_argument('--terraref_level', type=str,
                        default='Level_1')

    parser.add_argument('--terraref_sensor', type=str,
                        default='fullfield')


def exact_p(pattern):

    return '^{}$'.format(pattern)


def extract_date(timestamp):

    return timestamp.split('__')[0]

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


class Sensors():
    """Simple class used to save components of the file system path
    where sensor data will be stored.
    """

    def __init__(self, base, station, level, sensor):
        """Initialize basic path elements from env and cmdline."""

        self.base = base
        self.station = station
        self.level = level
        self.sensor = sensor


    def get_sensor_path(self, timestamp, sensor='', filename='',
                        opts=None, ext=''):
        """Get the appropritate path for writing sensor data
    
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

        if re.match(exact_p(full_date_p), timestamp)==None:
            raise RuntimeError('Timestamp has the wrong format')

        # override class sensor
        if not sensor:
            sensor = self.sensor

        # create opt string
        if opts:
            opts = '_'.join(['']+opts)
        else:
           opts = ''
    
        # split timestamp into date and hour-minute-second components
        date, hms = timestamp.split('__')

        try:
            s = STATIONS[self.site][self.level][sensor]
                
        except KeyError:
            raise RuntimeError('The station, level or sensor given does'
                               ' not exist')

        # TODO pattern should be completed with regex string using format
        if filename:
            result = re.match(exact_p(s['pattern']), filename)
            #if result==None:
            #    raise RuntimeError('The filename given does not match '
            #                       'the correct pattern')

        # TODO check that all fields from patterns are available
        # TODO write the _level_name function (maps Level_1 to lv1, etc)
        else:
            filename = s['pattern'].format(station=self.station, 
                    level=self._level_names(self.level), sensor=sensor,
                    timestamp=timstamp, date=date, time=hms,
                    opts=opts)
                                           
        # TODO split extension with os.path function than do 
        # a string match and replace
        if ext:
            filename = filename.split('.')[0] + ext

        path = s['template'].format(base=self.base, station=self.station,
                                    level=self.level, sensor=self.sensor,
                                    timestamp=timstamp, date=date, time=hms,
                                    filename=filename)
        return path
    

    def get_sensor_path_by_dataset(self, datasetname, sensor='',
                                   ext='', opts=None, hms=None):
        """Get the appropritate path for writing sensor data
    
        Args:
          station (str): abbreviated name of the site
          level (str): data level (raw_data | Level_1 | Level_2)
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
        if datasetname.find(" - ") > -1:
            sensorname, time = datasetname.split(" - ")
        else:
            sensorname = datasetname
            time = "2017"

        # Get date portion of timestamp if there is one; if not,
        # assume whole thing is just the date
        if time.find("__") > -1:
            date = time.split("__")[0]
        else:
            date = time
            time = ""

        # Override timestamp if necessary
        if hms:
            time = date + "__" + hms

        # Override dataset sensor name with provided name if given 
        # (e.g. use timestamp of raw while getting Level_1 output)
        if sensor == "":
            sensor = sensorname
    
        return self.get_sensor_path(time, sensor=sensor, opts=opts, ext=ext)


    def create_sensor_path(self, path):
        """Create path if does not exist."""
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
    
    
    def get_sensors(self, station):
        """Get all sensors for a given station."""
        return STATIONS[self.station]['sensors'].keys()
    
    
    def get_sites(self):
        """Get all sites (stations) listed."""
        return STATIONS.keys()
    
    
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
    
    
