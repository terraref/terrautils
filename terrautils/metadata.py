"""Metadata
This module provides useful reference methods for accessing and cleaning TERRA-REF metadata.
"""
import pyclowder.datasets
import os
import sensors

# Map of sensor name to Clowder dataset ID
_sensor_datasetid_map = {
    sensors.SENSOR_CO2 : "5873a9924f0cad7d8131b648",                                                                                   
    sensors.SENSOR_CROP_CIRCLE : "5873a7ed4f0cad7d8131a2e7",                                                                                    
    sensors.SENSOR_ENVIRONMENTAL_LOGGER : "",                                                                                   
    sensors.SENSOR_FLIR : "5873a7184f0cad7d8131994a",                                                                            
    sensors.SENSOR_IRRIGATION : "",                                                                                        
    sensors.SENSOR_LIGHTNING : "",                                                                                    
    sensors.SENSOR_NDVI : "5873a8f64f0cad7d8131af54",                                                                                     
    sensors.SENSOR_PAR : "5873a8ce4f0cad7d8131ad86",
    sensors.SENSOR_PRI : "5873a9174f0cad7d8131b09a",                                                                                  
    sensors.SENSOR_PS2_TOP : "5873a84b4f0cad7d8131a73d",                                                                                     
    sensors.SENSOR_SCANNER_3D_TOP : "5873a7444f0cad7d81319b2b",                                                                                        
    sensors.SENSOR_STEREO_TOP : "5873a8ae4f0cad7d8131ac0e",                                                                                 
    sensors.SENSOR_SWIR  : "5873a79e4f0cad7d81319f5f",                                                                                     
    sensors.SENSOR_VNIR : "5873a7bb4f0cad7d8131a0b7",                                                                                           
    sensors.SENSOR_WEATHER : ""
}


def get_sensor_fixed_metadata(sensor):
    """Get fixed sensor metadata from Clowder.
    """
    clowderhost = os.environ["CLOWDER_HOST"]
    clowderkey = os.environ["CLOWDER_KEY"]
    
    datasetid = _get_dataset_id_for_sensor(sensor)
    jsonld = pyclowder.datasets.download_metadata(None, clowderhost, clowderkey, datasetid)
    print jsonld
    return

def clean_metadata(json):
    """ Given a metadata object, returns a cleaned object with standardized structure and names.
    """
    pass

def clean_fixed_metadata(json):
    """Return cleaned fixed metadata json object with updated structure and names.
    """

def get_preferred_synonym(variable):
    """Execute a thesaurus check to see if input variable has alternate preferred name.
    """
    
def _get_dataset_id_for_sensor(sensor):
    return _sensor_datasetid_map.get(sensor)
    

if __name__ == "__main__":
    id = get_sensor_fixed_metadata("co2Sensor")
    print id