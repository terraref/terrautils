


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

    
# Map of sensor name to Clowder sensor dataset ID
_sensor_datasetid_map = {
    SENSOR_CO2 : "5873a9924f0cad7d8131b648",                                                                                   
    SENSOR_CROP_CIRCLE : "5873a7ed4f0cad7d8131a2e7",                                                                                    
    SENSOR_ENVIRONMENTAL_LOGGER : "",                                                                                   
    SENSOR_FLIR : "5873a7184f0cad7d8131994a",                                                                            
    SENSOR_IRRIGATION : "",                                                                                        
    SENSOR_LIGHTNING : "",                                                                                    
    SENSOR_NDVI : "5873a8f64f0cad7d8131af54",                                                                                     
    SENSOR_PAR : "5873a8ce4f0cad7d8131ad86",
    SENSOR_PRI : "5873a9174f0cad7d8131b09a",                                                                                  
    SENSOR_PS2_TOP : "5873a84b4f0cad7d8131a73d",                                                                                     
    SENSOR_SCANNER_3D_TOP : "5873a7444f0cad7d81319b2b",                                                                                        
    SENSOR_STEREO_TOP : "5873a8ae4f0cad7d8131ac0e",                                                                                 
    SENSOR_SWIR  : "5873a79e4f0cad7d81319f5f",                                                                                     
    SENSOR_VNIR : "5873a7bb4f0cad7d8131a0b7",                                                                                           
    SENSOR_WEATHER : ""
}

    
def get_datasetid_for_sensor(sensor):
    return _sensor_datasetid_map.get(sensor)