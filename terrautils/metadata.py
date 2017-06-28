"""Metadata
This module provides useful reference methods for accessing and cleaning TERRA-REF metadata.
"""
import pyclowder.datasets
import os
import sensors



def get_sensor_fixed_metadata(sensor):
    """Get fixed sensor metadata from Clowder.
    """
    clowderhost = os.environ["CLOWDER_HOST"]
    clowderkey = os.environ["CLOWDER_KEY"]
    
    datasetid = _get_dataset_id_for_sensor(sensor)
    jsonld = pyclowder.datasets.download_metadata(None, clowderhost, clowderkey, datasetid)
    return jsonld

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
    return sensors._sensor_datasetid_map.get(sensor)
    

if __name__ == "__main__":
    jsonld = get_sensor_fixed_metadata("co2Sensor")
    print jsonld