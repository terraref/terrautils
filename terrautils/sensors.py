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

""" If the bety traits that are submitted to bety need to be renamed, only
    change the value of the dict key/value pair. Extractors will reference
    the key in their code and use whatever the value is for trait name."""
STATIONS = {
    'danforth': {
        'ddpscIndoorSuite': {
            "bety_traits": {
                "sv_area": "sv_area",
                "tv_area": "tv_area",
                "hull_area": "hull_area",
                "solidity": "solidity",
                "height": "height",
                "permieter": "perimeter"
            }
        }
    },

    'ksu': {
        'dsm': {
            'display': 'Digital Surface Model GeoTIFFs',
            'template': '{base}/{station}/Level_1/{sensor}/{filename}',
            'pattern': '{time}_DSM_16ASH-TERRA.tif'
        },
        'rededge': {
            'display': 'Red Edge',
            'template': '{base}/{station}/Level_1/{sensor}/{filename}',
            'pattern': '{time}_BGREN_16ASH-TERRA'
        }
    },

    'ua-mac': {
        'co2Sensor': {
            "fixed_metadata_datasetid": "5873a9924f0cad7d8131b648",
            'template': '{base}/{station}/raw_data/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '([0-9a-f]){8}-([0-9a-f]){4}-([0-9' + \
                       'a-f]){4}-([0-9a-f]){4}-([0-9a-f])' + \
                       '{12}_(metadata.json|rawData0000.bin)'
        },
        
        'stereoTop': {
            "fixed_metadata_datasetid": "5873a8ae4f0cad7d8131ac0e",
            'template': '{base}/{station}/raw_data/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': 'hashid{opts}.bin'

        },
        
        'flirIrCamera': {
            "fixed_metadata_datasetid": "5873a7184f0cad7d8131994a",
            'template': '{base}/{station}/raw_data/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': 'hashid_ir.bin'
        },
        
        "cropCircle": {
            "fixed_metadata_datasetid": "5873a7ed4f0cad7d8131a2e7"
        },
        
        "irrigation": {
            "fixed_metadata_datasetid": "TBD",
            "geostream": ""
        },
        
        'EnvironmentLogger': {
            'template': '{base}/{station}/raw_data/' + \
                        '{sensor}/{date}/{filename}',
            'pattern': '{timestamp}_environment_logger.json',
            'fixed_metadata_datasetid': 'TBD'
        },
        
        "lightning": {
            "fixed_metadata_datasetid": "TBD",
            "geostream": ""
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
            "fixed_metadata_datasetid": "5873a7444f0cad7d81319b2b",
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': 'hashid__Top-heading-{opts}_0.ply'
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
      
        "scanalyzer": {
              "fixed_metadata_datasetid": "5873eac64f0cad7d81349b0b"
        },

        'fullfield': {
            'display': 'Full Field Stitched Mosaics',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{filename}',
            'pattern': '{sensor}_L1_{station}_{date}{opts}.tif'
        },

        'vnir_netcdf': {
            'display': 'VNIR Hyperspectral NetCDFs',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.nc',
            "bety_traits": {
                "NDVI705": "NDVI705"
            }
        },

        'swir_netcdf': {
            'display': 'SWIR Hyperspectral NetCDFs',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.nc',
            "bety_traits": {
                "NDVI705": "NDVI705"
            }
        },

        'rgb_geotiff': {
            'display': 'RGB GeoTIFFs',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.tif',
        },

        'rgb_canopyCover': {
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{filename}',
            'pattern': '{sensor}_L2_{station}_{date}{opts}.csv',
            'bety_traits': {
                'canopy_cover': 'canopy_cover'
            },
            "url": ""
        },

        'texture_analysis': {
            'display': 'RGB Texture Analysis',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.csv',
        },

        'ir_geotiff': {
            'display': 'Thermal IR GeoTIFFs',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.tif',
        },

        'ps2_png': {
            'display': 'PSII PNGs',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.png',
        },

        'ps2_fluorescence': {
            'display': 'PSII Fluorescence Features',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.png',
        },

        'spectral_index_csvs': {
            'display': 'Multispectral Index CSVs',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.csv',
        },

        'envlog_netcdf': {
            'display': 'EnvironmentLogger netCDFs',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{filename}',
            'pattern': '{sensor}_L1_{station}_{timestamp}{opts}.nc',
        },

        'vnir_soil_masks': {
            'display': 'VNIR Soil Masks',
            'template': '{base}/{station}/Level_2/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': 'VNIR_L2_{station}_{timestamp}{opts}.nc'
        },

        'swir_soil_masks': {
            'display': 'SWIR Soil Masks',
            'template': '{base}/{station}/Level_2/'
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': 'SWIR_L2_{station}_{timestamp}{opts}.nc'
        },

        'laser3d_mergedlas': {
            'display': 'Laser Scanner 3D LAS',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': 'scanner3DTop_L1_{station}_{timestamp}' + \
                       '_merged{opts}.las'
        },

        'laser3d_plant_height': {
            'display': 'Laser Scanner 3D Plant Height',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': 'scanner3DTop_L1_{station}_{timestamp}' + \
                       '_height{opts}.tif'
        },

        'laser3d_heightmap': {
            'display': 'Digital Surface Model GeoTiffs',
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': 'scanner3DTop_L2_{station}_{timestamp}' + \
                       '_heightmap{opts}.tif'
        },

        'weather_datparser': {
            "display": "UA-MAC AZMET Weather Station",
            "url": ""
        },

        'irrigation_datparser': {
            "display": "UA-MAC AZMET Weather Station",
            "url": ""
        },

        'energyfarm_datparser': {
            "display": "UIUC Energy Farm",
            "url": ""
        },

        'netcdf_metadata': {
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{timestamp}/{filename}',
            'pattern': '{sensor}_L2_{station}_{timestamp}{opts}.tif'
        },

        'ir_meanTemp': {
            'template': '{base}/{station}/Level_1/' + \
                        '{sensor}/{date}/{filename}',
            'pattern': '{sensor}_L2_{station}_{date}{opts}.csv',
            'bety_traits': {
                'surface_temperature': 'surface_temperature'
            },
        }
    },
}


def add_arguments(parser):

    if os.path.exists('/projects/arpae/terraref/sites'):
        TERRAREF_BASE = '/projects/arpae/terraref/sites'
    elif os.path.exists('/home/clowder/sites'):
        TERRAREF_BASE = '/home/clowder/sites'
    else:
        TERRAREF_BASE = '/home/extractor/sites'

    parser.add_argument('--terraref_base', type=str,
            default=os.environ.get('TERRAREF_BASE', TERRAREF_BASE),
            help='path to the sites directory on local host')

    parser.add_argument('--terraref_site', type=str,
            default=os.environ.get('TERRAREF_SITE', 'ua-mac'),
            help='site name (default=TERRAREF_SITE | ua-mac)')

    parser.add_argument('--terraref_sensor', type=str,
            default=os.environ.get('TERRAREF_SENSOR', ''),
            help='sensor name (default=None)')


def exact_p(pattern):

    return '^{}$'.format(pattern)


class Sensors():
    """Simple class used to save components of the file system path
    where sensor data will be stored.
    """

    def __init__(self, base, station, sensor='', stations=STATIONS):
        """Initialize basic path elements from env and cmdline."""

        self.stations = stations
        self.base = base.rstrip('/')

        if station in self.stations.keys():
            self.station = station
        else:
            raise AttributeError('unknown station name "{}"'.format(station))

        self._sensor = sensor


    @property
    def sensor(self):

        if self._sensor:
            return self._sensor
        else:
            raise RuntimeError('sensor not set')
        

    @sensor.setter
    def sensor(value):
        if value in self.stations[self.station].keys():
            self._sensor = value
        else:
            raise AttributeError('unknown sensor name "{}"'.format(value))


    def get_sensor_path(self, timestamp, sensor='', filename='',
                        opts=None, ext=''):
        """Get the appropritate path for writing sensor data

        Args:
          timestamp (datetime str): timestamp string
          sensor (str): overrides instance sensor variable, optional
          filename (str): overrides default filename, optional
          opts (list): list of filename extensions, optional
          ext (str): overrides the default filename extension, optional

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
        # split timestamp into date and hour-minute-second components
        if timestamp.find('__') > -1:
            date, hms = timestamp.split('__')
        else:
            date = timestamp
            hms = ''

        # Get regex patterns for this site/sensor
        try:
            s = self.stations[self.station][sensor]
        except KeyError:
            raise RuntimeError('sensor {} does not exist'.format(sensor))

        # create opt string
        if opts:
            opts = '_'.join(['']+opts)
        else:
            opts = ''

        # pattern should be completed with regex string using format
        if filename:
            pattern = exact_p(s['pattern']).format(sensor='\D*',
                    station='\D*', date=date_p, time=full_time_p,
                    timestamp=full_date_p, opts='\D*')

            result = re.match(pattern, filename)
            if result == None:
                raise RuntimeError('The filename given does not match the correct pattern')

        else:
            filename = s['pattern'].format(station=self.station,
                    sensor=sensor, timestamp=timestamp, date=date, time=hms,
                    opts=opts)

        # replace default extension in filename
        if ext:
            filename = '.'.join([os.path.splitext(filename)[0], 
                                ext.lstrip('.')])

        path = s['template'].format(base=self.base, station=self.station,
                                    sensor=sensor, timestamp=timestamp,
                                    date=date, time=hms, filename=filename)
        return path


    def get_sensor_path_by_dataset(self, dsname, sensor='', ext='', 
                                   opts=None, hms=''):
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


    def get_fixed_datasetid_for_sensor(self, site=None, sensor=None):
        """Return the Clowder dataset ID for the fixed sensor information"""

        if not site:
            site = self.station
        if not sensor:
            sensor = self.sensor

        return self.stations[site][sensor]['fixed_metadata_datasetid']


    def create_sensor_path(self, timestamp, sensor='', filename='',
                        opts=None, ext=''):
        """Return the full path for the sensor data
        
        Note: this function is similar to get_sensor_path and takes
        all the same arguments but has a side-effect of creating
        any missing directories in the path.
        """

        path = self.get_sensor_path(timestamp, sensor, filename,
                                        opts, ext)
        dirs = os.path.dirname(path) 
        if not os.path.exists(dirs):
            os.makedirs(dirs)

        return path


    def get_sites(self):
        """Get all sites (stations) listed."""

        return self.stations.keys()


    def get_experiment(self, date):
        """
        Return the experiment metadata associated with the specified date.
        """
        experiments = get_experiments()

        # We only care about date portion if timestamp is given
        if date.find("__") > -1:
            date = date.split("__")[0]

        ds_time = datetime.datetime.strptime(date, "%Y-%m-%d")
        matched_exps = []
        for exp in experiments:
            begin = datetime.datetime.strptime(exp['start_date'], "%Y-%m-%d")
            end = datetime.datetime.strptime(exp['end_date'], "%Y-%m-%d")

            if ds_time >= begin and ds_time <= end:
                matched_exps.append(exp)
        return matched_exps


    def get_season(self, date):
        exps = self.get_experiment(date)
        if len(exps) > 0:
            return exps[0]['name'][:exps[0]['name'].find(':')]
        else:
            return None


    def get_sensors(self, station=''):
        """Get all sensors for a given station."""

        # TODO: allow the KeyError on bad station name?
        if station:
            return self.stations[station].keys()
        else:
            return self.stations[self.station].keys()


    def get_display_name(self, sensor=''):
        """Get display name for a sensor."""

        if sensor:
            return self.stations[self.station][sensor]['display']
        else:
            return self.stations[self.station][self.sensor]['display']
