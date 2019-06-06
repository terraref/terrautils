"""brapi

This module provides wrappers to BRAPI API for getting and posting data.
"""

import os

import requests
import urllib
import urlparse

BRAPI_URL="https://brapi.workbench.terraref.org/brapi"
BRAPI_VERSION="v1"


def brapi_get(path='',request_params=None):
    brapi_url = os.environ.get('BRAPI_URL', BRAPI_URL)
    path = 'brapi/'+path
    request_url = urlparse.urljoin(brapi_url, path)

    result = []

    if request_params:
        r = requests.get(url=request_url, params=request_params)
        totalPages = r.json()['metadata']['pagination']['totalPages']
        current_data = r.json()['result']['data']
        result.extend(current_data)
        if totalPages > 1:
            for i in range(1, totalPages -1):
                request_params['page']=i
                r = requests.get(url=request_url, params=request_params)
                current_data = r.json()['result']['data']
                result.extend(current_data)
        return result
    else:
        r = requests.get(url=request_url)
        totalPages = r.json()['metadata']['pagination']['totalPages']
        current_data = r.json()['result']['data']
        result.extend(current_data)
        if totalPages > 1:
            for i in range(1, totalPages -1):
                request_params['page']=i
                r = requests.get(url=request_url, params=request_params)
                current_data = r.json()['result']['data']
                result.extend(current_data)
        return result


def get_brapi_study(studyDbId):
    """return study from brapi based on brapi url"""
    studies_path = 'v1/studies'
    request_params = {'studyDbId': studyDbId}
    studies_result = brapi_get(path=studies_path, request_params=request_params)
    return studies_result


def get_brapi_observationunits(studyDbId):
    observation_units_path = 'v1/observationunits'
    request_params = {'studyDbId': studyDbId}
    observationunits_result = brapi_get(path=observation_units_path, request_params=request_params)

    return observationunits_result


def get_brapi_study_layouts(studyDbId):
    """return study layouts from brapi based on brapi url"""
    current_path = 'v1/studies/' + str(studyDbId) + '/layouts'
    data = brapi_get(path=current_path)

    site_id_layouts_map = {}

    for entry in data:
        site_id = str(entry['observationUnitDbId'])
        site_name = str(entry['observationUnitName'])
        cultivar_id = str(entry['germPlasmDbId'])
        site_info = {}
        site_info['sitename'] = site_name
        site_info['germplasmDbId'] = cultivar_id
        site_id_layouts_map[site_id] = site_info
    return site_id_layouts_map


def get_brapi_study_germplasm(studyDbId):
    current_path = 'v1/studies/' + str(studyDbId) + '/germplasm'
    data = brapi_get(current_path)

    germplasm_id_data_map = {}
    for entry in data:
        germplasm = {}
        germplasm['germplasmName'] = str(entry['germplasmName'])
        germplasm['species'] = str(entry['species'])
        germplasm['genus'] = str(entry['genus'])
        germplasm['germplasmDbId'] = str(entry['germplasmDbId'])
        germplasm_id_data_map[str(entry['germplasmDbId'])] = germplasm

    return germplasm_id_data_map


def get_experiment_observation_units_map(studyDbId):
    data = get_brapi_observationunits(studyDbId)
    location_name_treatments_map = {}
    for entry in data:
        treatment = {}
        treatment['definition'] = str(entry['observationtreatment'])
        treatment['id'] = str(entry['treatmentDbId'])
        treatment['experiment_id'] = str(entry['studyDbId'])
        location_name_treatments_map[str(entry['observationUnitName'])] = treatment
    return location_name_treatments_map


def get_site_id_cultivar_info_map(studyDbId):
    layouts = get_brapi_study_layouts(studyDbId)
    germplasm = get_brapi_study_germplasm(studyDbId)
    observationunits = get_experiment_observation_units_map(studyDbId)

    site_ids = layouts.keys()

    for site_id in site_ids:
        corresponding_site_cultivar_id = layouts[site_id]['germplasmDbId']
        corresponding_site_name = layouts[site_id]['sitename']
        if corresponding_site_cultivar_id in germplasm:
            cultivar_info_from_germplasm = germplasm[corresponding_site_cultivar_id]
            layouts[site_id]['cultivar'] = cultivar_info_from_germplasm
        else:
            layouts[site_id]['cultivar'] = 'no info'
        if corresponding_site_name in observationunits:
            treatment_info = observationunits[corresponding_site_name]
            layouts[site_id]['treatments'] = treatment_info
        else:
            layouts[site_id]['treatments'] = 'no info'
    return layouts