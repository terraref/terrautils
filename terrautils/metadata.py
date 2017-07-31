"""Metadata
This module provides useful reference methods for accessing and cleaning TERRA-REF metadata.
"""

import pyclowder.datasets
import os
import sensors
import metadata.lemnatec
import json



def get_sensor_fixed_metadata(station, sensorId, host='', key=''):
    """Get fixed sensor metadata from Clowder.
    """
    if not host:
        host = os.getenv("CLOWDER_HOST", 'https://terraref.ncsa.illinois.edu/clowder/')
    if not key:
        key = os.getenv("CLOWDER_KEY", '')

    datasetid = sensors.get_fixed_datasetid_for_sensor(station, sensorId)
    jsonld = pyclowder.datasets.download_metadata(None, host, key, datasetid)
    
    return jsonld


def clean_metadata(json, sensorId):
    """ Given a metadata object, returns a cleaned object with standardized structure 
        and names.
    """
    if 'lemnatec_measurement_metadata' in json.keys():
        return metadata.lemnatec.clean(json, sensorId)
    else:
        return None


def get_preferred_synonym(variable):
    """Execute a thesaurus check to see if input variable has alternate preferred name.
    """
    pass


if __name__ == "__main__":
    # TODO: Either formalize these tests a bit or remove
    fixed = get_sensor_fixed_metadata("ua-mac", "VNIR")
    print "\nFIXED METADATA"
    print json.dumps(fixed[0]["content"], indent=4, sort_keys=True)

    print "\nCLEANED METADATA"
    with open("/data/terraref/sites/ua-mac/raw_data/VNIR/2017-05-13/2017-05-13__12-29-21-202/cd2a45b6-4922-48b4-bc29-f2f95e6206ec_metadata.json") as file:
        json_data = json.load(file)
    cleaned = clean_metadata(json_data, "VNIR")
    print json.dumps(cleaned, indent=4, sort_keys=True)
