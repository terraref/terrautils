"""LemnaTec

This module is used to standardize LemnaTec metadata.

Given a JSON object containing the original LemnaTec metadata, convert
field names and values to standardized formats required for TERRA-REF.

The "cleaned" metadata will be posted to the Clowder metadata endpoint and
used downstream by extractors.

TODO:
    SENSOR_IRRIGATION                                                                                   
    SENSOR_LIGHTNING
    SENSOR_ENVIRONMENTAL_LOGGER
    SENSOR_WEATHER
"""

import argparse
import json
import csv
import logging
import os
import re
import pytz, datetime

import terrautils.betydb
from terrautils.sensors import Sensors
from terrautils.spatial import calculate_gps_bounds, calculate_centroid, tuples_to_geojson


STATION_NAME = "ua-mac"

scan_programs = {}

# Official sensor names
PLATFORM_SCANALYZER = "scanalyzer"
SENSOR_CO2 = "co2Sensor"
SENSOR_CROP_CIRCLE = "cropCircle"
SENSOR_ENVIRONMENTAL_LOGGER = "EnvironmentLogger"
SENSOR_FLIR = "flirIrCamera"
SENSOR_IRRIGATION = "irrigation"
SENSOR_LIGHTNING = "lightning"
SENSOR_NDVI = "ndviSensor"
SENSOR_PAR = "parSensor"
SENSOR_PRI = "priSensor"
SENSOR_PS2_TOP = "ps2Top"
SENSOR_SCANNER_3D_TOP = "scanner3DTop"
SENSOR_STEREO_TOP = "stereoTop"
SENSOR_SWIR = "SWIR"
SENSOR_VNIR = "VNIR"
SENSOR_WEATHER = "weather"

SENSOR_METADATA_CACHE = os.environ.get('SENSOR_METADATA_CACHE', '/home/extractor/sites/ua-mac/sensor-metadata')

logging.basicConfig()
logger = logging.getLogger("terrautils.metadata.lemnatac")



# SHARED -------------------------------------
def clean(metadata, sensorId, filepath="", fixed=False):
    """ 
    Given a LemnaTec metadata.json object, produces the "cleaned" metadata that 
    will be put in the Clowder jsonld endpoint.
    """
    
    orig_lem_md = metadata['lemnatec_measurement_metadata']
    
    cleaned_md = {}
    cleaned_md["gantry_variable_metadata"] = _standardize_gantry_system_variable_metadata(orig_lem_md, filepath)
    cleaned_md["gantry_fixed_metadata"]    = _get_sensor_fixed_metadata_json(PLATFORM_SCANALYZER)
    cleaned_md["sensor_fixed_metadata"]    = _get_sensor_fixed_metadata_json(sensorId)
    cleaned_md["sensor_variable_metadata"] = _standardize_sensor_variable_metadata(sensorId, orig_lem_md, 
                                                    cleaned_md["gantry_variable_metadata"], filepath)
    date = cleaned_md["gantry_variable_metadata"]["date"]
    cleaned_md["terraref_cleaned_metadata"] = True
    
    # calculate_gps_bounds requires the fixed metadata FOV
    full_md = cleaned_md.copy()
    query_date = _get_date_from_raw_metadata(orig_lem_md)
    fixed_md = _get_sensor_fixed_metadata(sensorId, query_date)
    full_md["sensor_fixed_metadata"] = fixed_md
    if fixed:
        cleaned_md["sensor_fixed_metadata"] = fixed_md

    cleaned_md["experiment_metadata"] = _get_experiment_metadata(date, sensorId)
    cleaned_md["site_metadata"] = _get_sites(full_md, date, sensorId)
    cleaned_md["spatial_metadata"] = _get_spatial_metadata(full_md, sensorId)
    return cleaned_md


# PRIVATE -------------------------------------
def _get_spatial_metadata(cleaned_md, sensorId):
    gps_bounds = calculate_gps_bounds(cleaned_md, sensorId) 

    spatial_metadata = {}
    for label, bounds in gps_bounds.iteritems():
        spatial_metadata[label] = {}
        spatial_metadata[label]["bounding_box"] = tuples_to_geojson(bounds)
        spatial_metadata[label]["centroid"] = calculate_centroid(bounds)
        
    return spatial_metadata


def _get_sites(cleaned_md, date, sensorId):
    """
    Returns the site name and URL for all sites associated with the centroid.
    """
    gps_bounds = calculate_gps_bounds(cleaned_md, sensorId)

    sites = {}
    for label, bounds in gps_bounds.iteritems():
        centroid = calculate_centroid(bounds)
        bety_sites = terrautils.betydb.get_sites_by_latlon(centroid, date)
        for bety_site in bety_sites:
            site_id = str(bety_site["id"])
            sites[site_id] = {}
            sites[site_id]["sitename"] = bety_site["sitename"]
            if "view_url" in bety_site:
                sites[site_id]["url"] = bety_site["view_url"]
            else:
                sites[site_id]["url"] = ""
            sites['notes'] = "sitename is the plot that contains the image centroid"

    return sites.values()


def _get_experiment_metadata(date, sensorId): 
    sensors = Sensors(base="", station=STATION_NAME, sensor=sensorId)
    exps = sensors.get_experiment(date)
    
    experiment_md = []
    for exp in exps:
        curr_exp = {}
        curr_exp["name"] = exp["name"]
        curr_exp["start_date"] = exp["start_date"]
        curr_exp["end_date"] = exp["end_date"]
        curr_exp["url"] = exp["view_url"]
        experiment_md.append(curr_exp)

    return experiment_md    


def _standardize_gantry_system_fixed_metadata(orig):
    """
    Returns an object containing the URL for the Scanalyzer fixed metadata
    """
    properties = {}
    properties["url"] = _get_sensor_fixed_metadata_json(PLATFORM_SCANALYZER)
    return properties


def _get_sensor_fixed_metadata_json(sensorId):
    """
    Assumes that the sensor fixed metadata stored in Clowder is authoritative
    Ignore the fixed metadata in the JSON object and return the fixed metadata URL in Clowder.
    """
    # TODO: Compare to known fixed metadata structure
    # TODO; We only need this one -- duplicate method in metadata.py
    
    # Get the json path for the sensor by identifier
    sensors = Sensors(base="", station=STATION_NAME, sensor=sensorId)
    jsonpath = sensors.get_fixed_jsonpath_for_sensor()

    return {
        "url": "https://github.com/terraref/sensor-metadata/blob/master/" + jsonpath
    }


def _get_sensor_fixed_metadata(sensorId, query_date):
    # Get the json path for the sensor by identifier
    sensors = Sensors(base="", station=STATION_NAME, sensor=sensorId)
    jsonpath = sensors.get_fixed_jsonpath_for_sensor()

    sensor_file = SENSOR_METADATA_CACHE + jsonpath
    if os.path.exists(sensor_file):
        with open(sensor_file, 'r') as sf:
            md_json = json.load(sf)
            if type(md_json) == list:
                if type(md_json[0] == dict):
                    current_metadata = find_json_for_date(query_date, md_json)
            return current_metadata
    else:
        # TODO: What should happen here?
        return None


def _standardize_gantry_system_variable_metadata(lem_md, filepath=""):
    """
    Standardize the gantry variable metadata.  Note, the original LemnaTec metadata
    changes keys over time (e.g., time=Time=timestamp=TimeStamp).  The prop_map
    contains all keys encountered in the gantry_system_variable_metadata over time,
    although many of these are never used in the final cleaned metadata.
    """   
    
    prop_map = {
        'time': {
            'standardized': ['time']
        },
        'Time': {
            'standardized': ['time']
        },        
        'timestamp': {
            'standardized': ['time'] 
        }, 
        'Timestamp': {
            'standardized': ['time'] 
        },         
        'position x [m]': {
            'standardized': ['position_m', 'x']
        },
        'Position x [m]': {
            'standardized': ['position_m', 'x']
        },        
        'position y [m]': {
            'standardized': ['position_m', 'y']
        },
        'Position y [m]': {
            'standardized': ['position_m', 'y']
        },         
        'position z [m]': {
            'standardized': ['position_m', 'z']   
        },
        'Position z [m]': {
            'standardized': ['position_m', 'z']
        },          
        'speed x [m/s]': {
            'standardized': ['speed_m/s', 'x']
        },
        'speed y [m/s]': {
            'standardized': ['speed_m/s', 'y']
        },  
        'speed z [m/s]': {
            'standardized': ['speed_m/s', 'z']
        },
        'Velocity x [m/s]': {
            'standardized': ['velocity_m/s', 'x']
        },          
        'Velocity y [m/s]': {
            'standardized': ['velocity_m/s', 'y']
        },         
        'Velocity z [m/s]': {
            'standardized': ['velocity_m/s', 'z']
        },         
        'scanDistance [m]': {
            'standardized': ['scan_distance_m']
        },
        'scanDistanceInM [m]': {
            'standardized': ['scan_distance_m']
        },        
        'scanSpeed [m/s]': {
            'standardized': ['scan_speed_m/s']
        }, 
        'scanSpeedInMPerS [m/s]': {
            'standardized': ['scan_speed_m/s']
        },         
        'scanMode': {
            'standardized': ['scan_mode']
        },         
        'camera box light 1 is on': {
            'standardized': ['camera_box_light_1_on']
        }, 
        'Camnera box light 1 is on': {
            'standardized': ['camera_box_light_1_on']
        },          
        'camera box light 2 is on': {
            'standardized': ['camera_box_light_2_on']
        },  
        'Camnera box light 2 is on': {
            'standardized': ['camera_box_light_2_on']
        },        
        'camera box light 3 is on': {
            'standardized': ['camera_box_light_3_on']
        },  
        'Camnera box light 3 is on': {
            'standardized': ['camera_box_light_3_on']
        },        
        'camera box light 4 is on': {
            'standardized': ['camera_box_light_4_on']
        },  
        'Camnera box light 4 is on': {
            'standardized': ['camera_box_light_4_on']
        },         
        'Script copy path on FTP server': {
            'standardized': ['script_path_ftp_server']
        },   
        'Script path on local disk': {
            'standardized': ['script_path_on_disk']
        },           
        'sensor setting file path': {
            'standardized': ['sensor_setting_file_path']
        },  
        # This is used in the calculation of the point cloud origin
        'scanIsInPositiveDirection': {
            'standardized': ['scan_direction_is_positive'],
            'default' : "False"
        }, 
        'scanDirectionIsPositive': {
            'standardized': ['scan_direction_is_positive'],
            'default' : "False"
        },
        'PLC control not available': {
            'standardized': ['error']
        },
        # Found on co2Sensor
        'x end pos [m]': {
            'standardized': ['end_position_m', 'x']
        },     
        'x set velocity [m/s]': {
            'standardized': ['velocity_m/s', 'x']
        }, 
        'x set acceleration [m/s^2]': {
            'standardized': ['acceleration_m/s^2', 'x']
        },
        'x set deceleration [m/s^2]': {
            'standardized': ['deceleration_m/s^2', 'x']
        },          
        # Found on cropCircle
        'y end pos [m]': {
            'standardized': ['end_position_m', 'y']
        },  
        'Y end pos [m]': {
            'standardized': ['end_position_m', 'y']
        },          
        'y set velocity [m/s]': {
            'standardized': ['velocity_m/s', 'y']
        },
        'Y set velocity [m/s]': {
            'standardized': ['velocity_m/s', 'y']
        },         
        'y set acceleration [m/s^2]': {
            'standardized': ['acceleration_m/s^2', 'y']
        },
        'Y set acceleration [m/s^2]': {
            'standardized': ['acceleration_m/s^2', 'y']
        },        
        'y set deceleration [m/s^2]': {
            'standardized': ['deceleration_m/s^2', 'y']
        },
        'y set decceleration [m/s^2]': {
            'standardized': ['deceleration_m/s^2', 'y']
        },        
        'Y set decceleration [m/s^2]': {
            'standardized': ['deceleration_m/s^2', 'y']
        },
        # FAT Tests?
        'Measurement purpose [FAT test]' : {
            'standardized': ['ignored']
        },
        'Measurement target [Test object sandbox]' : {
            'standardized': ['ignored']
        },
        'fat measurement [comparison color sensors to hyperspec above green target]' : {
            'standardized': ['ignored']
        },
        'repeats [1 of 1]' : {
            'standardized': ['ignored']
        },        
        'repeats [1 of 3]' : {
            'standardized': ['ignored']
        },
        'repeats [2 of 3]' : {
            'standardized': ['ignored']
        },
        'repeats [3 of 3]' : {
            'standardized': ['ignored']
        },
        'fat measurement [test chart (resting)]' : {
            'standardized': ['ignored'],
            'default': "test chart(resting)"
        },
        'fat measurement [black body (moving)]' : {
            'standardized': ['ignored']
        },
        'fat measurement [black body (resting)]' : {
            'standardized': ['ignored']
        },
        'only small set of meta data available' : {
            'standardized': ['ignored']
        },
        'Only small set of meta data available' : {
            'standardized': ['ignored']
        },        
        'fat measurement [PS2 on fluo target]': {
            'standardized': ['ignored']
        },
        'fat measurement [3d scan of test target (different directions and different speeds)]': {
            'standardized': ['ignored']
        }
        
    }
    
    
    orig = lem_md['gantry_system_variable_metadata'] 
    properties = _standardize_with_validation("gantry_system_variable_metadata", orig, prop_map, [], filepath)  

    # Set default scan_direction_is_positive
    if 'scan_direction_is_positive' not in properties:
        if 'position_m' in properties:
            if properties['position_m']['y'] == 0:
                properties['scan_direction_is_positive'] = 'True'
            else:
                properties['scan_direction_is_positive'] = 'False'
                    
    # Standardize time field value
    if 'time' in properties:
        datetime_local = _standardize_time(properties['time'], "%m/%d/%Y %H:%M:%S", "US/Arizona")
        properties["datetime"] = datetime_local.isoformat()
        properties["date"] = datetime_local.date().isoformat()
        
    # 
    read_scan_program_map()
    if 'script_path_on_disk' in properties:
        # e.g. "C:\\LemnaTec\\StoredScripts\\3D_Scan_Field_SouthStart_033MperS.cs"
        script_path = properties['script_path_on_disk']
        scan_program = scan_programs.get(script_path, "unknown_program")
        match = re.search('^.*\\\\(.*?)\\.cs', script_path)   
        if len(match.groups()) == 1:
            script_name_caps = match.group(1)
            script_name = script_name_caps.lower()
            script_name = re.sub(" ", "_", script_name)
            properties['script_name'] = script_name
            if isinstance(scan_program, list):
                properties['fullfield_eligible'] = scan_program["fullfield_eligible"]
            else:
                properties['fullfield_eligible'] = "True"

            # Also retain unique script hash to differentiate if same scan run multiple times in a day
            if 'script_path_ftp_server' in properties:
                # e.g. "ftp://10.160.21.2//gantry_data/LemnaTec/ScriptBackup/3D_Scan_Field_SouthStart_033MperS_6d2cf837-5107-4a67-87f3-cc7b65551931.cs"
                script_hash = properties['script_path_ftp_server']
                # e.g. "6d2cf837-5107-4a67-87f3-cc7b65551931"
                script_hash = script_hash[script_hash.find(script_name_caps)+len(script_name_caps)+1:-3]
                properties['script_hash'] = script_hash
        
    # Limit output to the following fields for now
    output_fields = [
        "datetime", "date", "position_m", "scan_direction_is_positive", "script_path_on_disk", "script_name", "script_hash", "fullfield_eligible", "error"
    ]

    return _get_dict_subset(properties, output_fields)


def _get_date_from_raw_metadata(md):
    default = "2012-01-01"
    if "gantry_system_variable_metadata" in md:
        if "time" in md["gantry_system_variable_metadata"]:
            t = md["gantry_system_variable_metadata"]["time"].split(" ")[0]
            dd = t.split("/")[1]
            mm = t.split("/")[0]
            yy = t.split("/")[2]
            return yy+"-"+mm+"-"+dd

        else:
            return default
    else:
        return default


def _standardize_sensor_variable_metadata(sensor, orig_lem_md, corrected_gantry_variable_md, filepath=""):
    """
    Standardize the sensor variable metadata
    
    TODO:
        SENSOR_IRRIGATION                                                                                   
        SENSOR_LIGHTNING
        SENSOR_ENVIRONMENTAL_LOGGER
        SENSOR_WEATHER
    """

    query_date = _get_date_from_raw_metadata(orig_lem_md)
    sensor_variable_metadata = orig_lem_md['sensor_variable_metadata'] 
    sensor_fixed_metadata = _get_sensor_fixed_metadata(sensor, query_date)

    if sensor == SENSOR_CO2:
        properties = _co2_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_CROP_CIRCLE:
        properties = _cropCircle_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_ENVIRONMENTAL_LOGGER:
        properties = _xxx_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_FLIR:
        properties = _flir_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_NDVI:
        properties = _ndvi_standardize(sensor_variable_metadata, filepath)        
    elif sensor == SENSOR_PAR:
        properties = _par_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_PRI:
        properties = _pri_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_PS2_TOP:
        properties = _ps2_standardize(sensor_variable_metadata, filepath)        
    elif sensor == SENSOR_SCANNER_3D_TOP:
        properties = _scanner3d_standardize(sensor_variable_metadata, sensor_fixed_metadata, 
                                                corrected_gantry_variable_md, filepath)
    elif sensor == SENSOR_STEREO_TOP:
        properties = _stereoTop_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_SWIR:
        properties = _swir_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_VNIR:
        properties = _vnir_standardize(sensor_variable_metadata, filepath)

    return properties


def _standardize_with_validation(name, orig, property_map, required_fields=[], filepath=""):
    """
    Use the property_map to standardize the original metadata, using default values where specified.
    Log any warnings related to missing fields and errors for expected/required fields.
    """
    standardized = {}

    # Step through the fields in the original metadata and convert to the standardized form.
    # Warn if we don't have a mapping.
    for key in orig:
        if key in property_map:
            _set_nested_value(standardized, property_map[key]['standardized'], orig[key])
        else:
            logger.warning("Encountered field \"%s\", missing from map in %s (%s)"  % (key, name, filepath))

    # Step through the keys of the mapping, set default values where appropriate and
    # report an error if a required field is missing.
    for key in property_map:
        if key in required_fields and not _nested_contains(standardized, property_map[key]['standardized']):
            if key in property_map and 'default' in property_map[key]:
                logger.debug("Setting default value %s for key \"%s\"" % ( property_map[key]['default'],
                                                                           property_map[key]['standardized']))

                _set_nested_value(standardized, property_map[key]['standardized'], property_map[key]['default'])
            else:
                logger.error("missing required field \"%s\" in %s" % (property_map[key]['standardized'], name))
    return standardized


def _nested_contains(dic, keys):
    """
    Returns true if the keys exist
    """
    for key in keys[:-1]:
        if key in dic:
            dic = dic.get(key)
        else:
            return False

    return (keys[-1] in dic)


def _set_nested_value(dic, keys, value):
    """
    Given a set of keys as an array, sets a nested dictionary value. This is used to convert properties
    such as "position_x" or "position_y" to "position[x]" and "position[y]"
    """
    for key in keys[:-1]:
        dic = dic.setdefault(key, {})
    dic[keys[-1]] = value


def _standardize_time(timestr, timeformat, timezone):
    """
    Given the timestring, format, and timezone string, return the date/time in ISO 8601 format.
    """
    tz = pytz.timezone (timezone)
    time = datetime.datetime.strptime (timestr, timeformat)
    local_dt = tz.localize(time, is_dst=None)
    #utc_dt = local_dt.astimezone (pytz.utc)
    return (local_dt)


def _get_dict_subset(dic, keys):
    """
    Return a subset of a dictionary containing only the specified keys.
    """
    return dict((k, dic[k]) for k in keys if k in dic)


# SENSOR-SPECIFIC -------------------------------------
def _cropCircle_standardize(data, filepath=""):

    prop_map = {
        "current setting rotate flip type": {
            "standardized": ["rotate_flip_type"]
        },
        "current setting crosshairs": {
            "standardized": ["crosshairs"]
        }      
    }

    properties = _standardize_with_validation(SENSOR_CROP_CIRCLE, data, prop_map, [], filepath="")   
    return properties


def _flir_standardize(data, filepath=""):

    prop_map = {
        "current setting AutoFocus": {
            "standardized": ["autofocus"]
        },
        "current setting Manual focal length [cm]": {
            "standardized": ["manual_focal_length_cm"]
        },
        # 2016 data
        "current setting Manual focal length": {
            "standardized": ["manual_focal_length_cm"]
        },        
        "current setting ImageAdjustMode": {
            "standardized": ["image_adjust_mode"]
        },            
        "camera info": {
            "standardized": ["camera_info"]
        },
        "focus distance [m]": {
            "standardized": ["focus_distance_m"]
        },
        "lens temperature [K]": {
            "standardized": ["lens_temperature_K"]
        },
        "shutter temperature [K]": {
            "standardized": ["shutter_temperature_K"]
        }, 
        "front temperature [K]": {
            "standardized": ["front_temperature_K"]
        }               
    }
    
    properties = _standardize_with_validation(SENSOR_FLIR, data, prop_map, [], filepath)    
    return properties    
    
    
def _ps2_standardize(data, filepath=""):

    prop_map = {
        "current setting rotate flip type" : {
            "standardized": ["rotate_flip_type"]
        },
        "current setting crosshairs" : {
            "standardized": ["crosshairs"]
        },
        "current setting exposure" : {
            "standardized": ["exposure"]
        },
        "current setting gain" : {
            "standardized": ["gain"]
        },        
        "current setting gamma" : {
            "standardized": ["gamma"]
        },  
        "current setting ledcurrent" : {
            "standardized": ["led_current"]
        }           
    }
    properties = _standardize_with_validation(SENSOR_PS2_TOP, data, prop_map, [], )   
    return properties    


def _stereoTop_standardize(data, filepath=""):
    prop_map = {
        "Rotate flip type - left" : {
            "standardized": ["rotate_flip_type", "left"]
        },
        "Rotate flip type - right" : {
            "standardized": ["rotate_flip_type", "right"]
        },  
        "rotate flip type - left" : {
            "standardized": ["rotate_flip_type", "left"]
        },
        "rotate flip type - right" : {
            "standardized": ["rotate_flip_type", "right"]
        },          
        "Crosshairs - left" : {
            "standardized": ["crosshairs", "left"]
        },
        "Crosshairs - right" : {
            "standardized": ["crosshairs", "right"]
        },    
        "crosshairs - left" : {
            "standardized": ["crosshairs", "left"]
        },
        "crosshairs - right" : {
            "standardized": ["crosshairs", "right"]
        },         
        "exposure - left" : {
            "standardized": ["exposure", "left"]
        },
        "exposure - right" : {
            "standardized": ["exposure", "right"]
        },        
        "autoexposure - left" : {
            "standardized": ["autoexposure", "left"]
        },
        "autoexposure - right" : {
            "standardized": ["autoexposure", "right"]
        },        
        "gain - left" : {
            "standardized": ["gain", "left"]
        },  
        "gain - right" : {
            "standardized": ["gain", "right"]
        },         
        "autogain - left" : {
            "standardized": ["autogain", "left"]
        },  
        "autogain - right" : {
            "standardized": ["autogain", "right"]
        },          
        "gamma - left" : {
            "standardized": ["gamma", "left"]
        }, 
        "gamma - right" : {
            "standardized": ["gamma", "right"]
        },         
        "rwhitebalanceratio - left" : {
            "standardized": ["rwhitebalanceratio", "left"]
        }, 
        "rwhitebalanceratio - right" : {
            "standardized": ["rwhitebalanceratio", "right"]
        },           
        "bwhitebalanceratio - left" : {
            "standardized": ["bwhitebalanceratio", "left"]
        },  
        "bwhitebalanceratio - right" : {
            "standardized": ["bwhitebalanceratio", "right"]
        },         
        "height left image [pixel]" : {
            "standardized": ["height_image_pixels", "left"]
        },  
        "width left image [pixel]" : {
            "standardized": ["width_image_pixels", "left"]
        },       
        "image format left image" : {
            "standardized": ["image_format", "left"]
        },       
        "height right image [pixel]" : {
            "standardized": ["height_image_pixels", "right"]
        },  
        "width right image [pixel]" : {
            "standardized": ["width_image_pixels", "right"]
        },       
        "image format right image" : {
            "standardized": ["image_format", "right"]
        }, 
    }
    
    properties = _standardize_with_validation(SENSOR_STEREO_TOP, data, prop_map, [], filepath)  
    return properties     
    
    
def _swir_standardize(data, filepath=""):
    """
    Standardize SWIR metadata (same format as VNIR)
    """
    return _vnir_standardize(data, filepath, SENSOR_SWIR)
    
    
def _vnir_standardize(data, filepath="", name=SENSOR_VNIR):
    """
    Standardize VNIR metadata (same format as VNIR)
    """    
    prop_map = {
        "current setting frameperiod": {
            "standardized": ["frame_period"]
        },
        "current setting userotatingmirror": {
            "standardized": ["use_rotating_mirror"]
        },
        "current setting useexternaltrigger": {
            "standardized": ["use_external_trigger"]
        },       
        "current setting exposure": {
            "standardized": ["exposure"]
        },
        "current setting createdatacube": {
            "standardized": ["create_data_cube"]
        },      
        "current setting speed": {
            "standardized": ["speed"]
        }, 
        "current setting constmirrorpos": {
            "standardized": ["const_mirror_position"]
        },  
        "current setting startpos": {
            "standardized": ["start_position"]
        }, 
        "current setting stoppos": {
            "standardized": ["stop_position"]
        }
    }
    
    properties = _standardize_with_validation(name, data, prop_map, [], filepath)  
    return properties


def _co2_standardize(data, filepath=""):
    """
    Placeholder only, no variable metadata
    See /data/terraref/sites/ua-mac/raw_data/co2Sensor/2017-06-27/2017-06-27__13-32-28-129/17e118dc-20fb-4b59-9e59-9ee9840f302a_metadata.json
    """
    return {}


def _pri_standardize(data, filepath=""):
    """
    Placeholder only, no variable metadata
    See  /data/terraref/sites/ua-mac/raw_data/priSensor/2017-06-27/2017-06-27__13-32-28-039/baa2813f-0634-45df-8e9b-4a978fa93f86_metadata.json
    """
    return {}


def _ndvi_standardize(data, filepath=""):
    """
    Placeholder only, no variable metadata
    See   /data/terraref/sites/ua-mac/raw_data/ndviSensor//2017-03-13/2017-03-13__13-56-55-559/2d4ae02b-3475-42a8-bb73-fe972f256aaf_metadata.json    """
    return {}


def _scanner3d_standardize(data, fixed_md, corrected_gantry_variable_md, filepath=""):
    prop_map = {
        "current setting Exposure [microS]": {
            "standardized": ["exposure_microS"]
        },
        # 2016 data
        "current setting Exposure": {
            "standardized": ["exposure_microS"]
        },
        "current setting Calculate 3D files": {
            "standardized": ["calculate_3d_files"]
        },
        "current setting Laser detection threshold": {
            "standardized": ["laser_detection_threshold"]
        },
        "current setting Scanlines per output file": {
            "standardized": ["scanlines_per_output_file"]
        },
        "current setting Scan direction (automatically set at runtime)": {
            "standardized": ["scan_direction"]
        },
        "current setting Scan distance (automatically set at runtime) [mm]": {
            "standardized": ["scan_distance_mm"]
        },
        "current setting Scan speed (automatically set at runtime) [microMeter/s]": {
            "standardized": ["scan_speed_microMeter/s"]
        },
        "current setting Scan speed (automatically set at runtime)": {
            "standardized": ["scan_speed_microMeter/s"]
        },
        "current setting Scan distance (automatically set at runtime)": {
            "standardized": ["scan_distance_mm"]
        },
    }

    properties = _standardize_with_validation(SENSOR_SCANNER_3D_TOP, data, prop_map, [], filepath)
    properties["point_cloud_origin_m"] = _calculatePointCloudOrigin(data, fixed_md, corrected_gantry_variable_md)
    return properties


def _calculatePointCloudOrigin(scanner3d, fixed_md, gantvar_md):
    '''
        Calculate the origin of the point cloud. 
        Per https://github.com/terraref/reference-data/issues/44
            * The origin of point cloud in Z direction is the subtraction of 3445mm from the gantry position during the scan.
            * In X direction, is +82mm to the north from the center of the 3D scanner.
            * If the scan is in positive direction, the origin of ply files in Y direction is +3450mm in gantry coordinate system, 
            * If the scan is done in negative direction is +25711mm in gantry coordinate system.
        The origin is calculated based on the position of the west scanner. 
        So, any further misalignment correction should be applied to the east ply files.
        If Y is zero, then scan_direction_is_positive
        plc_control_not_available means there is no logation data (position_m)

        gantvar_md = corrected gantry variable metadata
    '''
    
    point_cloud_origin = {"east": {}, "west": {}}
    if (not 'plc_control_not_available' in gantvar_md
            and 'position_m' in gantvar_md
            and 'scan_direction_is_positive' in gantvar_md
            and 'scanner_west_location_in_camera_box_m' in fixed_md):

        cambox_e = fixed_md['scanner_east_location_in_camera_box_m']
        cambox_w = fixed_md['scanner_west_location_in_camera_box_m']

        xv = float(gantvar_md["position_m"]["x"]) + 0.082
        point_cloud_origin['east']["x"] = xv + float(cambox_e['x'])
        point_cloud_origin['west']["x"] = xv + float(cambox_w['x'])

        zv = float(gantvar_md['position_m']['z']) - 3.445
        point_cloud_origin['east']["z"] = zv + float(cambox_e['z'])
        point_cloud_origin['west']["z"] = zv + float(cambox_w['z'])

        if (gantvar_md["scan_direction_is_positive"] == "True"):
            yv = float(gantvar_md['position_m']['y']) + 3.450
        else:
            yv = float(gantvar_md['position_m']['y']) + 25.711
        point_cloud_origin['east']["y"] = yv + float(cambox_e['y'])*2
        point_cloud_origin['west']["y"] = yv + float(cambox_w['y'])*2

    else:
        logger.error("Cannot calculate point cloud origin -- missing gantry position information")

    return point_cloud_origin


def read_scan_program_map():

    if not scan_programs:
        scan_path = os.path.join(os.path.dirname(__file__), "data/scan_programs.csv")
        with open(scan_path, 'rb') as file:
            reader = csv.DictReader(file)
            for row in reader:
                scan_programs[row["program_name"]] = {
                    "fullfield_eligible": "True" if row["fullfield_eligible"] == "Y" else "False"
                }


def get_content_for_date(query_date, content_json):
    query_datetime = datetime.datetime.strptime(query_date, '%Y-%m-%d')
    current_match = None
    if 'start_dates' in content_json:
        print("we have start dates")
        start_dates_raw = content_json['start_dates'].keys()
        start_dates = []
        for each in start_dates_raw:
            print('each', each)
            current = datetime.datetime.strptime(each, '%Y-%m-%d')
            start_dates.append(current)

        start_dates.sort()
        for sd in start_dates:
            if query_datetime < sd:
                current_match = sd
                print('new current match', current_match)
        match_string = str(current_match)
        match_string = match_string[0:match_string.index(' ')]
        return content_json['start_dates'][match_string]


def find_json_for_date(query_date, json_list):
    if len(json_list) == 1:
        return json_list[0]
    query_date = datetime.datetime.strptime(query_date, '%Y-%m-%d')
    best_match = None
    best_match_date = None

    for each in json_list:
        metadata_date = datetime.datetime.strptime(each['start_date'], '%Y-%m-%d')

        if best_match is None:
            if query_date >= metadata_date:
                best_match = each
                best_match_date = datetime.datetime.strptime(each['start_date'], '%Y-%m-%d')
        else:
            if query_date > metadata_date > best_match_date:
                best_match = each
                best_match_date = datetime.datetime.strptime(each['start_date'], '%Y-%m-%d')
    return best_match



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=str, help="Path to metadata json")
    parser.add_argument("sensor", type=str, help="Sensor name")
    parser.add_argument("--output", help="Print output", action="store_true")
    parser.add_argument("--debug", help="Print debug output", action="store_true")
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        
    logger.debug("Processing %s" % args.path)

    with open(args.path) as file:
        json_data = json.load(file)

    clean_md = [{"content":  clean(json_data, args.sensor)}]
    
    if args.output:
        print(json.dumps(clean_md, indent=4, sort_keys=False))
