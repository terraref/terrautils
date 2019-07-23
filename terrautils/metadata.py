"""Metadata

This module provides useful reference methods for accessing and cleaning TERRA-REF metadata.
"""

import terrautils.lemnatec

def clean_metadata(json, sensorId, fixed=False):
    """ Given a metadata object, returns a cleaned object with standardized structure 
        and names.
    """
    cleaned = clean_json_keys(json)
    if 'lemnatec_measurement_metadata' in json.keys():
        cleaned = terrautils.lemnatec.clean(cleaned, sensorId, fixed=fixed)
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
        query_date = get_date_from_cleaned_metadata(terra_md)
        sensor_fixed = get_sensor_fixed_metadata(sensor_id, query_date)
        if 'sensor_fixed_metadata' in terra_md:
            sensor_fixed['url'] = terra_md['sensor_fixed_metadata']['url']
        terra_md['sensor_fixed_metadata'] = sensor_fixed

    return terra_md


def get_extractor_metadata(clowder_md, extractor_name, extractor_version=None):
    """Crawl Clowder metadata object for particular extractor metadata and return if found.

    If extractor_version specified, returned metadata must match."""
    for sub_metadata in clowder_md:
        if 'agent' in sub_metadata:
            agent_data = sub_metadata['agent']
            if 'name' in agent_data and agent_data['name'].find(extractor_name) > -1:
                if not extractor_version:
                    return sub_metadata['content']
                else:
                    # TODO: Eventually check this in preferred way
                    if 'extractor_version' in sub_metadata['content']:
                        existing_ver = str(sub_metadata['content']['extractor_version'])
                        if existing_ver == extractor_version:
                            return sub_metadata['content']

    return None


# pylint: disable=too-many-branches
def pipeline_get_metadata(metadata):
    """Look through the metadata looking for pipeline configuration
    Args:
        metadata(JSON): the JSON object, or list of JSON objects, to search
    Returns:
        The found JSON is returned, otherwise None is returned. None is also returned
        if the passed in JSON is invalid or is empty.
    Notes:
        If the metadata parameter is an a list, it will be iterated over and each list element
        inspected for pipeline data. The first found instance is returned.
        If the metadata parameter is not a list, it's inspected to see if it contains
        pipeline data, and that's returned if found.

        There are two keys looked for off the JSON passed in, as follows, in order:
            1) "content" -> "pipeline"
            2) "pipeline"
    """
    found_metadata = None

    # Check the parameter
    if not metadata:
        return found_metadata

    json_len = len(metadata)
    if json_len <= 0:
        return found_metadata

    try:
        # Look for a list of JSON
        if isinstance(metadata, list):
            for one_metadata in metadata:
                if 'content' in one_metadata:
                    if 'pipeline' in one_metadata['content']:
                        found_metadata = one_metadata['content']['pipeline']
                        break

            if found_metadata is None:
                for one_metadata in metadata:
                    if 'pipeline' in one_metadata:
                        found_metadata = one_metadata['pipeline']
                        break

        elif 'content' in metadata:
            if 'pipeline' in metadata['content']:
                found_metadata = one_metadata['content']['pipeline']

        elif 'pipeline' in metadata:
            found_metadata = metadata['pipeline']
    finally:
        pass

    return found_metadata


def get_season_and_experiment(timestamp, sensor, terra_md_full):
    """Attempts to extract season & experiment from TERRA-REF metadata given timestamp.

    If the values weren't in TERRA metadata but were fetched from BETY, updated experiment will be
    returned as well.
    """
    season_name, experiment_name, expmd = "Unknown Season", "Unknown Experiment", None
    if 'experiment_metadata' in terra_md_full and len(terra_md_full['experiment_metadata']) > 0:
        for experiment in terra_md_full['experiment_metadata']:
            if 'name' in experiment:
                if ":" in experiment['name']:
                    season_name = experiment['name'].split(": ")[0]
                    experiment_name = experiment['name'].split(": ")[1]
                else:
                    experiment_name = experiment['name']
                    season_name = None
                break
    else:
        # Try to determine experiment data dynamically
        expmd = terrautils.lemnatec._get_experiment_metadata(timestamp.split("__")[0], sensor)
        if len(expmd) > 0:
            for experiment in expmd:
                if 'name' in experiment:
                    if ":" in experiment['name']:
                        season_name = experiment['name'].split(": ")[0]
                        experiment_name = experiment['name'].split(": ")[1]
                    else:
                        experiment_name = experiment['name']
                        season_name = None
                    break

    return (season_name, experiment_name, expmd)


def get_preferred_synonym(variable):
    """Execute a thesaurus check to see if input variable has alternate preferred name."""
    pass


def get_sensor_fixed_metadata(sensor_id, query_date):
    """Get fixed sensor metadata from Clowder."""
    return terrautils.lemnatec._get_sensor_fixed_metadata(sensor_id, query_date)


def get_date_from_cleaned_metadata(md):
    default = "2012-01-01"
    if "gantry_variable_metadata" in md:
        if "date" in md["gantry_variable_metadata"]:
            return md["gantry_variable_metadata"]["date"]
        else:
            return default
    else:
        return default
