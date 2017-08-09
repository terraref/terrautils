'''
Create a cleaned/curated metadata object based on the LemnaTec fixed metadata
Some metadata used for search
Some metadata used for extractors

TODO: 
  Add checks for expected fields
  Date/time standardization
  Add units to variables?
  Logging

'''

import json
import os
import sys
import requests
from terrautils import sensors

station = "ua-mac"

# Official sensor names
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


def clean(metadata, sensorId, filepath=""):
    """ Given a LemnaTec metadata.json object, produces the "cleaned" metadata that 
    will be put in the Clowder jsonld endpoint.
    """
    
    orig_lem_md = metadata['lemnatec_measurement_metadata']
    
    properties = {}
    properties["gantry_variable"] = _standardize_gantry_system_variable_metadata(orig_lem_md, filepath)
    properties["sensor_fixed"]    = _get_sensor_fixed_metadata_url(sensorId)
    properties["sensor_variable"] = _standardize_sensor_variable_metadata(sensorId, orig_lem_md, properties["gantry_variable"], filepath)
    #_standardize_user_given_metadata(orig_lem_md)

    return properties
            
# Standardize user given metadata
def _standardize_user_given_metadata(lem_md):
    # Currently not used. Will possibly return reference to user given metadata in BETY
    return

# Standardize gantry fixed metadata
def _standardize_gantry_system_fixed_metadata(orig):
    # Currently not used. Will possibly return URL to Gantry fixed metadata in Clowder
    return

def _get_sensor_fixed_metadata_url(sensorId):
    """
    Assumes that the sensor fixed metadata stored in Clowder is authoritative
    Ignore the fixed metadata in the JSON object and return the fixed metadata URL in Clowder.
    """
    # TODO: Compare to known fixed metadata structure
    # TODO; We only need this one -- duplicate method in metadata.py
    
    # Get the dataset ID for the sensor by identifier
    datasetid = sensors.get_fixed_datasetid_for_sensor(station, sensorId)
    
    properties = {}
    properties["url"] = os.environ["CLOWDER_HOST"] + "api/datasets/" + datasetid + "/metadata.jsonld"
    return properties
    
def _get_sensor_fixed_metadata(sensorId):
    md = _get_sensor_fixed_metadata_url(sensorId)
    r = requests.get(md["url"])
    json = r.json()
    content = json[0]["content"]
    return content
    

def _standardize_gantry_system_variable_metadata(lem_md, filepath=""):
    """
    Standardize the gantry variable metadata
    """   
    
    # Map of Lemnatec properties to normalized names
    # TODO: Check time format
    prop_map = {
        'time': {
            'normalized': ['time'],
            'required': True
        },
        'Time': {
            # "Time": "04/25/2016 12:26:41",
            'normalized': ['time'],
            'required': False
        },        
        'timestamp': {
            #12/05/2016 16:13:08
            'normalized': ['time'],
            'required': False
        }, 
        'position x [m]': {
            'normalized': ['position_m', 'x'],
            'required': True
        },
        'Position x [m]': {
            'normalized': ['position_m', 'x'],
            'required': False
        },        
        'position y [m]': {
            'normalized': ['position_m', 'y'],
            'required': True
        },
        'Position y [m]': {
            'normalized': ['position_m', 'y'],
            'required': False
        },         
        'position z [m]': {
            'normalized': ['position_m', 'z'],
            'required': True
        },
        'Position z [m]': {
            'normalized': ['position_m', 'z'],
            'required': False
        },          
        'speed x [m/s]': {
            'normalized': ['speed_m/s', 'x'],
            'required': True
        },
        'Velocity x [m/s]': {
            'normalized': ['speed_m/s', 'x'],
            'required': False
        },          
        'speed y [m/s]': {
            'normalized': ['speed_m/s', 'y'],
            'required': True
        },  
        'Velocity y [m/s]': {
            'normalized': ['speed_m/s', 'y'],
            'required': False
        },         
        'speed z [m/s]': {
            'normalized': ['speed_m/s', 'z'],
            'required': True
        },
        'Velocity z [m/s]': {
            'normalized': ['speed_m/s', 'z'],
            'required': False
        },         
        'scanDistance [m]': {
            'normalized': ['scan_distance_m'],
            'required': False
        },
        'scanDistanceInM [m]': {
            'normalized': ['scan_distance_m'],
            'required': False
        },        
        'scanSpeed [m/s]': {
            'normalized': ['scan_speed_m/s'],
            'required': False
        }, 
        'scanSpeedInMPerS [m/s]': {
            'normalized': ['scan_speed_m/s'],
            'required': False
        },         
        'scanMode': {
            'normalized': ['scan_mode'],
            'required': False
        },         
        'camera box light 1 is on': {
            'normalized': ['camera_box_light_1_on'],
            'required': False
        }, 
        'Camnera box light 1 is on': {
            'normalized': ['camera_box_light_1_on'],
            'required': False
        },          
        'camera box light 2 is on': {
            'normalized': ['camera_box_light_2_on'],
            'required': False
        },  
        'Camnera box light 2 is on': {
            'normalized': ['camera_box_light_2_on'],
            'required': False
        },        
        'camera box light 3 is on': {
            'normalized': ['camera_box_light_3_on'],
            'required': False
        },  
        'Camnera box light 3 is on': {
            'normalized': ['camera_box_light_3_on'],
            'required': False
        },        
        'camera box light 4 is on': {
            'normalized': ['camera_box_light_4_on'],
            'required': False
        },  
        'Camnera box light 4 is on': {
            'normalized': ['camera_box_light_4_on'],
            'required': False
        },         
        'Script copy path on FTP server': {
            'normalized': ['script_path_ftp_server'],
            'required': False
        },   
        'Script path on local disk': {
            'normalized': ['script_path_local_disk'],
            'required': False
        },           
        'sensor setting file path': {
            'normalized': ['sensor_setting_file_path'],
            'required': False,
        },  
        # This is used in the calculation of the point cloud origin
        'scanIsInPositiveDirection': {
            'normalized': ['scan_direction_is_positive'],
            'required': True,
            'default' : "False"
        }, 
        'scanDirectionIsPositive': {
            'normalized': ['scan_direction_is_positive'],
            'required': False,
            'default' : "False"
        },
        'PLC control not available': {
            'normalized': ['plc_control_not_available'],
            'required': False
        },
        # Found on co2Sensor
        'x end pos [m]': {
            'normalized': ['end_position_m', 'x'],
            'required': False
        },     
        'x set velocity [m/s]': {
            'normalized': ['velocity_m/s', 'x'],
            'required': False
        }, 
        'x set acceleration [m/s^2]': {
            'normalized': ['acceleration_m/s^2', 'x'],
            'required': False
        },
        'x set deceleration [m/s^2]': {
            'normalized': ['deceleration_m/s^2', 'x'],
            'required': False
        },          
        # Found on cropCircle
        'y end pos [m]': {
            'normalized': ['end_position_m', 'y'],
            'required': False
        },  
        'Y end pos [m]': {
            'normalized': ['end_position_m', 'y'],
            'required': False
        },          
        'y set velocity [m/s]': {
            'normalized': ['velocity_m/s', 'y'],
            'required': False
        },
        'Y set velocity [m/s]': {
            'normalized': ['velocity_m/s', 'y'],
            'required': False
        },         
        'y set acceleration [m/s^2]': {
            'normalized': ['acceleration_m/s^2', 'y'],
            'required': False
        },
        'Y set acceleration [m/s^2]': {
            'normalized': ['acceleration_m/s^2', 'y'],
            'required': False
        },        
        'y set decceleration [m/s^2]': {
            'normalized': ['deceleration_m/s^2', 'y'],
            'required': False
        },
        'Y set decceleration [m/s^2]': {
            'normalized': ['deceleration_m/s^2', 'y'],
            'required': False
        }     
        
    }
    
    # TODO:  we know time is a problem, x,y,z should be a standard object, same for speed
    
    orig = lem_md['gantry_system_variable_metadata'] 
    properties = _normalize_with_validation("gantry_system_variable_metadata", orig, prop_map, filepath)  


    return properties

def _standardize_sensor_variable_metadata(sensor, orig_lem_md, corrected_gantry_variable_md, filepath=""):
    """
    Standardize the sensor variable metadata
    
    TODO:
        SENSOR_IRRIGATION                                                                                   
        SENSOR_LIGHTNING
        SENSOR_ENVIRONMENTAL_LOGGER
        SENSOR_WEATHER
    """
    
    sensor_variable_metadata = orig_lem_md['sensor_variable_metadata'] 
    sensor_fixed_metadata = _get_sensor_fixed_metadata(sensor)

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
        properties = _scanner3d_standardize(sensor_variable_metadata, sensor_fixed_metadata, corrected_gantry_variable_md, filepath)
    elif sensor == SENSOR_STEREO_TOP:
        properties = _stereoTop_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_SWIR:
        properties = _swir_standardize(sensor_variable_metadata, filepath)
    elif sensor == SENSOR_VNIR:
        properties = _vnir_standardize(sensor_variable_metadata, filepath)

    return properties


    
def _cropCircle_standardize(cropCircle, filepath=""):
    """
    See /data/terraref/sites/ua-mac/raw_data/cropCircle/2017-06-27/2017-06-27__13-32-27-989/0333e097-1bf0-4e63-9455-ab787e9fdf43_metadata.json

    Example:
        "sensor_variable_metadata": {
          "current setting rotate flip type": "0",
          "current setting crosshairs": "0"
        }
    """


    prop_map = {
        "current setting rotate flip type": {
            "normalized": ["rotate_flip_type"],
            "required": False
        },
        "current setting crosshairs": {
            "normalized": ["crosshairs"],
            "required": False
        }      
    }

    properties = _normalize_with_validation("cropCircle", cropCircle, prop_map, filepath="")   

    return properties
    
def _flir_standardize(flir, filepath=""):

    prop_map = {
        "current setting AutoFocus": {
            "normalized": "autofocus",
            "required": False
        },
        "current setting Manual focal length [cm]": {
            "normalized": "manual_focal_length_cm",
            "required": False
        },
        # 2016 data
        "current setting Manual focal length": {
            "normalized": "manual_focal_length_cm",
            "required": False
        },        
        "current setting ImageAdjustMode": {
            "normalized": "image_adjust_mode",
            "required": False
        },            
        "camera info": {
            "normalized": "camera_info",
            "required": False
        },
        "focus distance [m]": {
            "normalized": "focus_distance_m",
            "required": False
        },
        "lens temperature [K]": {
            "normalized": "lens_temperature_K",
            "required": False
        },
        "shutter temperature [K]": {
            "normalized": "shutter_temperature_K",
            "required": False
        }, 
        "front temperature [K]": {
            "normalized": "front_temperature_K",
            "required": False
        }               
    }
    
    properties = _normalize_with_validation(SENSOR_FLIR, flir, prop_map, filepath="")    

    return properties    
    
    
def _ps2_standardize(ps2, filepath=""):

    prop_map = {
        "current setting rotate flip type" : {
            "normalized": "rotate_flip_type",
            "required": False
        },
        "current setting crosshairs" : {
            "normalized": "crosshairs",
            "required": False
        },
        "current setting exposure" : {
            "normalized": "exposure",
            "required": False
        },
        "current setting gain" : {
            "normalized": "gain",
            "required": False
        },        
        "current setting gamma" : {
            "normalized": "gamma",
            "required": False
        },  
        "current setting ledcurrent" : {
            "normalized": "led_current",
            "required": False
        }           
    }
    properties = _normalize_with_validation(SENSOR_PS2_TOP, ps2, prop_map, filepath="")   
    return properties    
    
def _scanner3d_standardize(scanner3d, fixed_md, corrected_gantry_variable_md, filepath=""):
    prop_map = {
        "current setting Exposure [microS]": {
            "normalized": ["exposure_microS"],
            "required": False
        },
        # 2016 data
        "current setting Exposure": {
            "normalized": ["exposure_microS"],
            "required": False
        },        
        "current setting Calculate 3D files": {
            "normalized": ["calculate_3d_files"],
            "required": False
        }, 
        "current setting Laser detection threshold": {
            "normalized": ["laser_detection_threshold"],
            "required": False
        },    
        "current setting Scanlines per output file": {
            "normalized": ["scanlines_per_output_file"],
            "required": False
        },   
        "current setting Scan direction (automatically set at runtime)": {
            "normalized": ["scan_direction"],
            "required": False
        },   
        "current setting Scan distance (automatically set at runtime) [mm]": {
            "normalized": ["scan_distance_mm"],
            "required": False
        },  
        "current setting Scan speed (automatically set at runtime) [microMeter/s]": {
            "normalized": ["scan_speed_microMeter/s"],
            "required": False
        },            
        
    } 
    
    properties = _normalize_with_validation(SENSOR_SCANNER_3D_TOP, scanner3d, prop_map, filepath="")    

    properties["point_cloud_origin_m"] = _calculatePointCloudOrigin(scanner3d, fixed_md, corrected_gantry_variable_md)

    return properties  
    
def _normalize_with_validation(name, orig, property_map, filepath=""):
    normalized = {}
    for key in orig:
        if key in property_map:
            _set_nested_value(normalized, property_map[key]['normalized'], orig[key])
        else:
            print "Warning: encountered field \"%s\", missing from map in %s" % (key, name)
            
            
    for key in property_map:
        if property_map[key]['required'] and not _nested_contains(normalized, property_map[key]['normalized']):
            if key in property_map and 'default' in property_map[key]:
                #print "Setting default value %s for key \"%s\"" % ( property_map[key]['default'], property_map[key]['normalized'])
                _set_nested_value(normalized, property_map[key]['normalized'], property_map[key]['default'])
            else:
              print "Error: missing required field \"%s\" in %s" % (property_map[key]['normalized'], name)
    
    return normalized
    
def _calculatePointCloudOrigin(scanner3d, fixed_md, corrected_gantry_variable_md): 
    '''
        Calculate the origin of the point cloud. 
        Per https://github.com/terraref/reference-data/issues/44
            * The origin of point cloud in Z direction is the subtraction of 3445mm from the gantry position during the scan.
            * In X direction, is +82mm to the north from the center of the 3D scanner.
            * If the scan is in positive direction, the origin of ply files in Y direction is +3450mm in gantry coordinate system, 
            * If the scan is done in negative direction is +25711mm in gantry coordinate system.
        The origin is calculated based on the position of the west scanner. 
        So, any further misalignment correction should be applied to the east ply files.
    '''
    
    point_cloud_origin = {}
    if 'position_m' in corrected_gantry_variable_md and 'scanner_west_location_in_camera_box_m' in fixed_md:
        point_cloud_origin["z"] =  float(corrected_gantry_variable_md['position_m']['z']) - 3.445
        point_cloud_origin["x"] =  float(fixed_md["scanner_west_location_in_camera_box_m"]["x"]) - 0.0082
        if (corrected_gantry_variable_md["scan_direction_is_positive"] == "True"):
            point_cloud_origin["y"] = float(corrected_gantry_variable_md['position_m']['y']) + 3.450
        else:
            point_cloud_origin["y"] = float(corrected_gantry_variable_md['position_m']['z']) + 25.711
    else:
        print "Error: Cannot calculate point cloud origin"
    

    return point_cloud_origin
    

def _stereoTop_standardize(stereoTop, filepath=""):
    prop_map = {
        "Rotate flip type - left" : {
            "normalized": ["rotate_flip_type", "left"],
            "required": False
        },
        "Rotate flip type - right" : {
            "normalized": ["rotate_flip_type", "right"],
            "required": False
        },  
        "rotate flip type - left" : {
            "normalized": ["rotate_flip_type", "left"],
            "required": False
        },
        "rotate flip type - right" : {
            "normalized": ["rotate_flip_type", "right"],
            "required": False
        },          
        "Crosshairs - left" : {
            "normalized": ["crosshairs", "left"],
            "required": False
        },
        "Crosshairs - right" : {
            "normalized": ["crosshairs", "right"],
            "required": False
        },    
        "crosshairs - left" : {
            "normalized": ["crosshairs", "left"],
            "required": False
        },
        "crosshairs - right" : {
            "normalized": ["crosshairs", "right"],
            "required": False
        },         
        "exposure - left" : {
            "normalized": ["exposure", "left"],
            "required": False
        },
        "exposure - right" : {
            "normalized": ["exposure", "right"],
            "required": False
        },        
        "autoexposure - left" : {
            "normalized": ["autoexposure", "left"],
            "required": False
        },
        "autoexposure - right" : {
            "normalized": ["autoexposure", "right"],
            "required": False
        },        
        "gain - left" : {
            "normalized": ["gain", "left"],
            "required": False
        },  
        "gain - right" : {
            "normalized": ["gain", "right"],
            "required": False
        },         
        "autogain - left" : {
            "normalized": ["autogain", "left"],
            "required": False
        },  
        "autogain - right" : {
            "normalized": ["autogain", "right"],
            "required": False
        },          
        "gamma - left" : {
            "normalized": ["gamma", "left"],
            "required": False
        }, 
        "gamma - right" : {
            "normalized": ["gamma", "right"],
            "required": False
        },         
        "rwhitebalanceratio - left" : {
            "normalized": ["rwhitebalanceratio", "left"],
            "required": False
        }, 
        "rwhitebalanceratio - right" : {
            "normalized": ["rwhitebalanceratio", "right"],
            "required": False
        },           
        "bwhitebalanceratio - left" : {
            "normalized": ["bwhitebalanceratio", "left"],
            "required": False
        },  
        "bwhitebalanceratio - right" : {
            "normalized": ["bwhitebalanceratio", "right"],
            "required": False
        },         
        "height left image [pixel]" : {
            "normalized": ["height_image_pixels", "left"],
            "required": False
        },  
        "width left image [pixel]" : {
            "normalized": ["width_image_pixels", "left"],
            "required": False
        },       
        "image format left image" : {
            "normalized": ["image_format", "left"],
            "required": False
        },       
        "height right image [pixel]" : {
            "normalized": ["height_image_pixels", "right"],
            "required": False
        },  
        "width right image [pixel]" : {
            "normalized": ["width_image_pixels", "right"],
            "required": False
        },       
        "image format right image" : {
            "normalized": ["image_format", "right"],
            "required": False
        },           
          
    }
    
    properties = _normalize_with_validation(SENSOR_STEREO_TOP, stereoTop, prop_map, filepath)  
    
    return properties     
    
def _swir_standardize(swir, filepath=""):
    # Same properties as VNIR
    return _vnir_standardize(swir, filepath, SENSOR_SWIR)
    
def _vnir_standardize(vnir, filepath="", name=SENSOR_VNIR):

    prop_map = {
        "current setting frameperiod": {
            "normalized": "frame_period", 
            "required": False
        },
        "current setting userotatingmirror": {
            "normalized": "use_rotating_mirror", 
            "required": False
        },
        "current setting useexternaltrigger": {
            "normalized": "use_external_trigger", 
            "required": False
        },       
        "current setting exposure": {
            "normalized": "exposure", 
            "required": False
        },
        "current setting createdatacube": {
            "normalized": "create_data_cube", 
            "required": False
        },      
        "current setting speed": {
            "normalized": "speed", 
            "required": False
        }, 
        "current setting constmirrorpos": {
            "normalized": "const_mirror_position", 
            "required": False
        },  
        "current setting startpos": {
            "normalized": "start_position", 
            "required": False
        }, 
        "current setting stoppos": {
            "normalized": "stop_position", 
            "required": False
        }
    }
    
    properties = _normalize_with_validation(name, vnir, prop_map, filepath)  
    return properties


def _co2_standardize(co2, filepath=""):
    """
    Placeholder only, no variable metadata
    See /data/terraref/sites/ua-mac/raw_data/co2Sensor/2017-06-27/2017-06-27__13-32-28-129/17e118dc-20fb-4b59-9e59-9ee9840f302a_metadata.json
    """
    return {}

def _pri_standardize(pri, filepath=""):
    """
    Placeholder only, no variable metadata
    See  /data/terraref/sites/ua-mac/raw_data/priSensor/2017-06-27/2017-06-27__13-32-28-039/baa2813f-0634-45df-8e9b-4a978fa93f86_metadata.json
    """
    return {}
    
def _ndvi_standardize(pri, filepath=""):
    """
    Placeholder only, no variable metadata
    See   /data/terraref/sites/ua-mac/raw_data/ndviSensor//2017-03-13/2017-03-13__13-56-55-559/2d4ae02b-3475-42a8-bb73-fe972f256aaf_metadata.json    """
    return {}    
    
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
    
if __name__ == "__main__":
    #fixed = _get_sensor_fixed_metadata("scanner3DTop")
    #print json.dumps(fixed, indent=4, sort_keys=True)

    path = sys.argv[1]
    sensor = sys.argv[2]
    print "Processing %s" % path
    #with open("/data/terraref/sites/ua-mac/raw_data/scanner3DTop/2017-07-20/2017-07-20__05-40-41-035/7fa3a8d7-294f-4076-81ab-4c191fa9faa0_metadata.json") as file:
    with open(path) as file:
        json_data = json.load(file)
    cleaned = clean(json_data, sensor, path)
    #print json.dumps(cleaned, indent=4, sort_keys=True)
