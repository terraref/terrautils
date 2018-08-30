"""Metadata

This module provides useful reference methods for accessing and cleaning TERRA-REF metadata.
"""

import json
import os

import pyclowder.datasets
import lemnatec
from sensors import Sensors


def clean_metadata(json, sensorId, fixed=False):
    """ Given a metadata object, returns a cleaned object with standardized structure 
        and names.
    """
    cleaned = clean_json_keys(json)
    if 'lemnatec_measurement_metadata' in json.keys():
        cleaned = lemnatec.clean(cleaned, sensorId, fixed=fixed)
    else:
        return None

    if 'terraref_cleaned_metadata' not in cleaned:
        cleaned["terraref_cleaned_metadata"] = True
    return cleaned


def clean_json_keys(jsonobj):
    """If metadata keys have periods in them, Clowder will reject the metadata.
    """
    clean_json = {}
    for key in jsonobj.keys():
        try:
            jsonobj[key].keys() # Is this a json object?
            clean_json[key.replace(".","_")] = clean_json_keys(jsonobj[key])
        except:
            clean_json[key.replace(".","_")] = jsonobj[key]

    return clean_json

def calculate_scan_time(metadata):
    """Parse scan time from metadata.

        Returns:
            timestamp string
    """
    scan_time = None

    if 'terraref_cleaned_metadata' in metadata and metadata['terraref_cleaned_metadata']:
        scan_time = metadata['gantry_variable_metadata']['datetime']
    else:
        for sub_metadata in metadata:
            if 'content' in sub_metadata:
                sub_metadata = sub_metadata['content']
            if 'terraref_cleaned_metadata' in sub_metadata and sub_metadata['terraref_cleaned_metadata']:
                scan_time = sub_metadata['gantry_variable_metadata']['datetime']

    return scan_time


def get_terraref_metadata(clowder_md, sensor_id=None, station='ua-mac'):
    """Crawl Clowder metadata object and return TERRARef metadata or None.

    If sensor_id given, will attach fixed sensor metadata from that sensor."""

    terra_md = {}

    if 'terraref_cleaned_metadata' in clowder_md and clowder_md['terraref_cleaned_metadata']:
        terra_md = clowder_md
    else:
        for sub_metadata in clowder_md:
            if 'content' in sub_metadata:
                sub_metadata = sub_metadata['content']
            if 'terraref_cleaned_metadata' in sub_metadata and sub_metadata['terraref_cleaned_metadata']:
                terra_md = sub_metadata

    # Add sensor fixed metadata
    if sensor_id:
        sensor_fixed = get_sensor_fixed_metadata(station, sensor_id)
        if 'sensor_fixed_metadata' in terra_md:
            sensor_fixed['url'] = terra_md['sensor_fixed_metadata']['url']
        terra_md['sensor_fixed_metadata'] = sensor_fixed

    return terra_md


def get_extractor_metadata(clowder_md, extractor_name):
    """Crawl Clowder metadata object for particular extractor metadata and return if found."""
    for sub_metadata in clowder_md:
        if 'agent' in sub_metadata:
            sub_md = sub_metadata['agent']
            if 'name' in sub_md and sub_md['name'].find(extractor_name) > -1:
                return sub_md

    return None


def get_preferred_synonym(variable):
    """Execute a thesaurus check to see if input variable has alternate preferred name."""
    pass


def get_sensor_fixed_metadata(sensor_id, query_date):
    """Get fixed sensor metadata from Clowder."""
    return lemnatec._get_sensor_fixed_metadata(sensor_id, query_date)

