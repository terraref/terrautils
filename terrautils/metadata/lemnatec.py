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
import requests
from terrautils import sensors


def clean(metadata, sensorId):
    """ Given a LemnaTec metadata.json object, produces the "cleaned" metadata that 
    will be put in the Clowder jsonld endpoint.
    """
    
    orig_lem_md = metadata['lemnatec_measurement_metadata']
    
    properties = {}
    properties["gantry_variable"] = _standardize_gantry_system_variable_metadata(orig_lem_md)
    properties["sensor_fixed"]    = _get_sensor_fixed_metadata_url(sensorId)
    properties["sensor_variable"] = _standardize_sensor_variable_metadata(sensorId, orig_lem_md, properties["gantry_variable"])
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
    datasetid = sensors.get_datasetid_for_sensor(sensorId)
    
    properties = {}
    properties["url"] = os.environ["CLOWDER_HOST"] + "api/datasets/" + datasetid + "/metadata.jsonld"
    return properties
    
def _get_sensor_fixed_metadata(sensorId):
    md = _get_sensor_fixed_metadata_url(sensorId)
    r = requests.get(md["url"])
    json = r.json()
    content = json[0]["content"]
    return content
    

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
    
    data['position_m'] = {}
    data['position_m']['x'] = orig["position x [m]"]
    data['position_m']['y'] = orig["position y [m]"]
    data['position_m']['z'] = orig["position z [m]"]

    data['speed_m/s'] = {}
    data['speed_m/s']['x'] = orig["speed x [m/s]"]
    data['speed_m/s']['y'] = orig["speed y [m/s]"]
    data['speed_m/s']['z'] = orig["speed z [m/s]"]
    
    data['distance_m'] = orig.get("scanDistanceInM [m]", "")

    # This is used in the calculation of the point cloud origin
    data['scan_direction_is_positive'] = orig.get("scanDirectionIsPositive", "False")

    return data

def _standardize_sensor_variable_metadata(sensor, orig_lem_md, corrected_gantry_variable_md):
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
        properties = _scanner3d_standardize(sensor_variable_metadata, sensor_fixed_metadata, corrected_gantry_variable_md)
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
    
def _scanner3d_standardize(scanner3d, fixed_md, corrected_gantry_variable_md):
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

    properties["point_cloud_origin_m"] = _calculatePointCloudOrigin(scanner3d, fixed_md, corrected_gantry_variable_md)

    return properties  
    
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
    
    point_cloud_origin["z"] =  float(corrected_gantry_variable_md['position_m']['z']) - 3.445
    point_cloud_origin["x"] =  float(fixed_md["scanner west location in camera box x [m]"]) - 0.0082
    if (corrected_gantry_variable_md["scan_direction_is_positive"] == "True"):
        point_cloud_origin["y"] = float(corrected_gantry_variable_md['position_m']['y']) + 3.450
    else:
        point_cloud_origin["y"] = float(corrected_gantry_variable_md['position_m']['z']) + 25.711

    return point_cloud_origin
    

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
    
    
    properties["rotate_flip_type_left"] = stereoTop["Rotate flip type - left"]
    properties["crosshairs_left"] = stereoTop["Crosshairs - left"]
    properties["exposure_left"] = stereoTop["exposure - left"]
    properties["autoexposure_left"] = stereoTop["autoexposure - left"]
    properties["gain_left"] = stereoTop["gain - left"]
    properties["autogain_left"] = stereoTop["autogain - left"]
    properties["gamma_left"] = stereoTop["gamma - left"]
    properties["rwhitebalanceratio_left"] = stereoTop["rwhitebalanceratio - left"]
    properties["bwhitebalanceratio_left"] = stereoTop["bwhitebalanceratio - left"]
    properties["rotate_flip_type_right"] = stereoTop["Rotate flip type - right"]
    properties["crosshairs_right"] = stereoTop["Crosshairs - right"]
    properties["exposure_right"] = stereoTop["exposure - right"]
    properties["autoexposure_right"] = stereoTop["autoexposure - right"]
    properties["gain_right"] = stereoTop["gain - right"]
    properties["autogain_right"] = stereoTop["autogain - right"]
    properties["gamma_right"] = stereoTop["gamma - right"]
    properties["rwhitebalanceratio_right"] = stereoTop["rwhitebalanceratio - right"]
    properties["bwhitebalanceratio_right"] = stereoTop["bwhitebalanceratio - right"]
    properties["height_left_image_pixels"] = stereoTop["height left image [pixel]"]
    properties["width_left_image_pixels"] = stereoTop["width left image [pixel]"]
    properties["image_format_left"] = stereoTop["image format left image"]
    properties["height_right_image_pixels"] = stereoTop["height right image [pixel]"]
    properties["width_right_image_pixels"] = stereoTop["width right image [pixel]"]
    properties["image_format_right"] = stereoTop["image format right image"]
    
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
    fixed = _get_sensor_fixed_metadata("scanner3DTop")
    print json.dumps(fixed, indent=4, sort_keys=True)
    
    with open("/data/terraref/sites/ua-mac/raw_data/scanner3DTop/2017-07-20/2017-07-20__05-40-41-035/7fa3a8d7-294f-4076-81ab-4c191fa9faa0_metadata.json") as file:
        json_data = json.load(file)
    cleaned = clean(json_data, "scanner3DTop")
    print json.dumps(cleaned, indent=4, sort_keys=True)
