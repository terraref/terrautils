"""Metadata
This module provides useful reference methods for accessing and cleaning TERRA-REF metadata.
"""
import pyclowder.datasets
import os
import sensors
import metadata.lemnatec
import json


def get_sensor_fixed_metadata(sensor):
    """Get fixed sensor metadata from Clowder.
    """
    clowderhost = os.environ["CLOWDER_HOST"]
    clowderkey = os.environ["CLOWDER_KEY"]
    
    datasetid = _get_dataset_id_for_sensor(sensor)
    jsonld = pyclowder.datasets.download_metadata(None, clowderhost, clowderkey, datasetid)
    return jsonld

def clean_metadata(json):
    """ Given a metadata object, returns a cleaned object with standardized structure 
        and names.
    """
    if 'lemnatec_measurement_metadata' in json.keys():
        return metadata.lemnatec.clean(json)

    pass

def clean_fixed_metadata(json):
    """Return cleaned fixed metadata json object with updated structure and names.
    """
    pass

def get_preferred_synonym(variable):
    """Execute a thesaurus check to see if input variable has alternate preferred name.
    """
    pass
    
def _get_dataset_id_for_sensor(sensor):
    """ Returns the Clowder dataset ID for the specified sensor name
    """
    return sensors._sensor_datasetid_map.get(sensor)
    

if __name__ == "__main__":
    fixed = get_sensor_fixed_metadata("co2Sensor")
    print json.dumps(fixed, indent=4, sort_keys=True)
    
    with open("/data/terraref/sites/ua-mac/raw_data/VNIR/2017-05-13/2017-05-13__12-29-21-202/cd2a45b6-4922-48b4-bc29-f2f95e6206ec_metadata.json") as file:
        json_data = json.load(file)
    cleaned = clean_metadata(json_data)
    print json.dumps(cleaned, indent=4, sort_keys=True)


    