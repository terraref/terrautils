'''
Create a cleaned/curated metadata object based on the LemnaTec fixed metadata
Some metadata used for search
Some metadata used for extractors

TODO: 
  Add checks for expected fields
  Date/time standardization
  Add units to variables?

'''

import json
import os
from terrautils import sensors


def clean(metadata):
    """ Given a LemnaTec metadata.json object, produces the "cleaned" metadata that 
    will be put in the Clowder jsonld endpoint.
    """
    
    lem_md = metadata['lemnatec_measurement_metadata']
    
    properties = {}
    properties["gantry_variable"] = _standardize_gantry_system_variable_metadata(lem_md)
    properties["sensor_fixed"]    = _get_sensor_fixed_metadata(lem_md)
    properties["sensor_variable"] = _standardize_sensor_variable_metadata(lem_md)
    #_standardize_user_given_metadata(lem_md)

    return properties
            
# Standardize user given metadata
def _standardize_user_given_metadata(lem_md):
    # Return reference to user given metadata in BETYdb?
    return

# Not currently used
# Standardize gantry fixed metadata
#def _standardize_gantry_system_fixed_metadata(orig):
#    # Return reference to gantry fixed metadata in Clowder
#    return

def _get_sensor_fixed_metadata(lem_md):
    """
    Assumes that the sensor fixed metadata stored in Clowder is authoritative
    Ignore the fixed metadata in the JSON object and return the fixed metadata URL in Clowder.
    """
    # TODO: Compare to known fixed metadata structure
    
    # Get the dataset ID for the sensor by name
    sensor = lem_md['sensor_fixed_metadata']['sensor product name']
    datasetid = sensors.get_datasetid_for_sensor(sensor)
    
    properties = {}
    properties["url"] = os.environ["CLOWDER_HOST"] + "api/datasets/" + datasetid + "/metadata.jsonld"
    return properties

def _standardize_gantry_system_variable_metadata(lem_md):
    """
    Standardize the gantry variable metadata
    
    Example:
        "gantry_system_variable_metadata": {
          "time": "05/13/2017 12:29:21",
          "position x [m]": "179.0935",
          "position y [m]": "0.002",
          "position z [m]": "0.58",
          "speed x [m/s]": "0",
          "speed y [m/s]": "0",
          "speed z [m/s]": "0",
          "camera box light 1 is on": "False",
          "camera box light 2 is on": "False",
          "camera box light 3 is on": "False",
          "camera box light 4 is on": "False",
          "Script path on local disk": "C:\\LemnaTec\\StoredScripts\\SWIR_VNIR_Day1.cs",
          "Script copy path on FTP server": "ftp://10.160.21.2//gantry_data/LemnaTec/ScriptBackup/SWIR_VNIR_Day1_4211cd6a-020c-47b0-8023-234e14cd7fdd.cs",
          "scanSpeedInMPerS [m/s]": "0.04",
          "scanDistanceInM [m]": "20.133",
          "scanDirectionIsPositive": "True",
          "sensor setting file path": "c:\\LemnaTec\\StoredSensorSettings\\vnir_Exposure66.xml"
        },
    """   
    
    # TODO:  we know time is a problem, x,y,z should be a standard object, same for speed
    
    orig = lem_md['gantry_system_variable_metadata'] 
    data = {}
    data['time'] = orig['time']
    
    data['position'] = {}
    data['position']['x'] = orig["position x [m]"]
    data['position']['y'] = orig["position y [m]"]
    data['position']['z'] = orig["position z [m]"]

    data['speed'] = {}
    data['speed']['x'] = orig["speed x [m/s]"]
    data['speed']['y'] = orig["speed y [m/s]"]
    data['speed']['z'] = orig["speed z [m/s]"]
    
    data['distance'] = orig["scanDistanceInM [m]"]

    data['is_positive'] = orig["scanDirectionIsPositive"]

    return data

def _standardize_sensor_variable_metadata(lem_md):
    """
    Standardize the sensor variable metadata
    
    TODO:
        SENSOR_IRRIGATION                                                                                   
        SENSOR_LIGHTNING
        SENSOR_ENVIRONMENTAL_LOGGER
        SENSOR_WEATHER
    """
    
    sensor = lem_md['sensor_fixed_metadata']['sensor product name']
    sensor_variable_metadata = lem_md['sensor_variable_metadata'] 
    
    if sensor == sensors.SENSOR_CO2:
        properties = _co2_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_CROP_CIRCLE:
        properties = _cropCircle_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_ENVIRONMENTAL_LOGGER:
        properties = _xxx_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_FLIR:
        properties = _flir_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_NDVI:
        properties = _ndvi_standardize(sensor_variable_metadata)        
    elif sensor == sensors.SENSOR_PAR:
        properties = _par_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_PRI:
        properties = _pri_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_PS2_TOP:
        properties = _ps2_standardize(sensor_variable_metadata)        
    elif sensor == sensors.SENSOR_SCANNER_3D_TOP:
        properties = _scanner3d_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_STEREO_TOP:
        properties = _stereoTop_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_SWIR:
        properties = _swir_standardize(sensor_variable_metadata)
    elif sensor == sensors.SENSOR_VNIR:
        properties = _vnir_standardize(sensor_variable_metadata)

    return properties


    
def _cropCircle_standardize(cropCircle):
    """
    See /data/terraref/sites/ua-mac/raw_data/cropCircle/2017-06-27/2017-06-27__13-32-27-989/0333e097-1bf0-4e63-9455-ab787e9fdf43_metadata.json

    Example:
        "sensor_variable_metadata": {
          "current setting rotate flip type": "0",
          "current setting crosshairs": "0"
        }
    """

    # TODO: Check expected fields

    # Standardize variable names
    properties = {}
    properties["rotate_flip_type"] = cropCircle["current setting rotate flip type"]
    properties["crosshairs"] = cropCircle["current setting crosshairs"]
    return properties
    
def _flir_standardize(flir):
    """
    Example: 
        "sensor_variable_metadata": {
          "current setting AutoFocus": "1",
          "current setting Manual focal length [cm]": "195",
          "camera info": "A645,Gen_A/G,GEV,1.0.0,GEV,1.2.17  (13070601)",
          "focus distance [m]": "1.594",
          "lens temperature [K]": "303.139",
          "shutter temperature [K]": "302.849",
          "front temperature [K]": "303.734"
        }
    """
    properties = {}
    properties["autofocus"] = flir["current setting AutoFocus"]
    properties["manual_focal_length_cm"] = flir["current setting Manual focal length [cm]"]
    properties["camera_info"]  = flir["camera info"]
    properties["focus_distance_m"]  = flir["focus distance [m]"]
    properties["lens_temperature_K"]  = flir["lens temperature [K]"]
    properties["shutter_temperature_K"]  = flir["shutter temperature [K]"]
    properties["front_temperature_K"]  = flir["front temperature [K]"]
    return properties    
    
    
def _ps2_standardize(ps2):
    '''
    See /data/terraref/sites/ua-mac/raw_data/ps2Top/2017-05-26/2017-05-26__06-36-30-952/3123f664-56d4-44bb-9778-e40f68608c99_metadata.json

    Example:
        "sensor_variable_metadata": {
          "current setting rotate flip type": "0",
          "current setting crosshairs": "0",
          "current setting exposure": "28",
          "current setting gain": "3000",
          "current setting gamma": "100",
          "current setting ledcurrent": "10"
        }    
    '''
    properties = {}    
    properties["rotate_flip_type"] = ps2["current setting rotate flip type"]
    properties["crosshairs"] = ps2["current setting crosshairs"]
    properties["exposure"] = ps2["current setting exposure"]
    properties["gain"] = ps2["current setting gain"]
    properties["gamma"] = ps2["current setting gamma"]
    properties["ledcurrent"] = ps2["current setting ledcurrent"]
    return properties    
    
def _scanner3d_standardize(scanner3d):
    '''
        "sensor_variable_metadata": {
          "current setting Exposure [microS]": "70",
          "current setting Calculate 3D files": "0",
          "current setting Laser detection threshold": "512",
          "current setting Scanlines per output file": "100000",
          "current setting Scan direction (automatically set at runtime)": "1",
          "current setting Scan distance (automatically set at runtime) [mm]": "21800",
          "current setting Scan speed (automatically set at runtime) [microMeter/s]": "100000"
        }
    '''
    properties = {} 
    properties["exposure_microS"] = scanner3d["current setting Exposure [microS]"]
    properties["calculate_3d_files"] = scanner3d["current setting Calculate 3D files"]
    properties["laser_detection_threshold"] = scanner3d["current setting Laser detection threshold"]
    properties["scanlines_per_output_file"] = scanner3d["current setting Scanlines per output file"]
    properties["scan_direction"] = scanner3d["current setting Scan direction (automatically set at runtime)"]
    properties["scan_distance_mm"] = scanner3d["current setting Scan distance (automatically set at runtime) [mm]"]
    properties["scan_speed_microMeter/s"] = scanner3d["current setting Scan speed (automatically set at runtime) [microMeter/s]"]
    return properties      

def _stereoTop_standardize(stereoTop):
    '''
        "sensor_variable_metadata": {
          "Rotate flip type - left": "0",
          "Crosshairs - left": "0",
          "exposure - left": "2500",
          "autoexposure - left": "0",
          "gain - left": "1500",
          "autogain - left": "0",
          "gamma - left": "50",
          "rwhitebalanceratio - left": "170",
          "bwhitebalanceratio - left": "103",
          "Rotate flip type - right": "0",
          "Crosshairs - right": "0",
          "exposure - right": "2500",
          "autoexposure - right": "0",
          "gain - right": "1500",
          "autogain - right": "0",
          "gamma - right": "50",
          "rwhitebalanceratio - right": "155",
          "bwhitebalanceratio - right": "110",
          "height left image [pixel]": "2472",
          "width left image [pixel]": "3296",
          "image format left image": "BayerGR8",
          "height right image [pixel]": "2472",
          "width right image [pixel]": "3296",
          "image format right image": "BayerGR8"
        }    
    '''
    properties = {}    
    return properties     
    
def _swir_standardize(swir):
    # Same properties as VNIR
    return _vnir_standardize(swir)
    
def _vnir_standardize(vnir):
    # TODO: Check expected fields
    '''
    "sensor_variable_metadata": {
      "current setting frameperiod": "50",
      "current setting userotatingmirror": "0",
      "current setting useexternaltrigger": "0",
      "current setting exposure": "66",
      "current setting createdatacube": "0",
      "current setting speed": "100",
      "current setting constmirrorpos": "0",
      "current setting startpos": "-70",
      "current setting stoppos": "70"
    }
    '''
    properties = {}
    properties["frame_period"] = vnir["current setting frameperiod"]
    properties["use_rotating_mirror"] = vnir["current setting userotatingmirror"]
    properties["use_external_trigger"] = vnir["current setting useexternaltrigger"]
    properties["exposure"] = vnir["current setting exposure"]
    properties["create_data_cube"] = vnir["current setting createdatacube"]
    properties["speed"] = vnir["current setting speed"]
    properties["const_mirror_position"] = vnir["current setting constmirrorpos"]
    properties["start_position"] = vnir["current setting startpos"]
    properties["stop_position"] = vnir["current setting stoppos"]
    return properties


def _co2_standardize(co2):
    """
    Placeholder only, no variable metadata
    See /data/terraref/sites/ua-mac/raw_data/co2Sensor/2017-06-27/2017-06-27__13-32-28-129/17e118dc-20fb-4b59-9e59-9ee9840f302a_metadata.json
    """
    return {}

def _pri_standardize(pri):
    """
    Placeholder only, no variable metadata
    See  /data/terraref/sites/ua-mac/raw_data/priSensor/2017-06-27/2017-06-27__13-32-28-039/baa2813f-0634-45df-8e9b-4a978fa93f86_metadata.json
    """
    return {}
    
    
if __name__ == "__main__":
    with open("/data/terraref/sites/ua-mac/raw_data/VNIR/2017-05-13/2017-05-13__12-29-21-202/cd2a45b6-4922-48b4-bc29-f2f95e6206ec_metadata.json") as file:
        json_data = json.load(file)
    cleaned = clean(json_data)
    print json.dumps(cleaned, indent=4, sort_keys=True)
