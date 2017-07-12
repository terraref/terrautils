"""BETYdb

This module provides wrappers to BETY API for getting and posting data.
"""

import os
import logging
import json
import urlparse
from osgeo import ogr
import requests


BETYDB_URL="https://terraref.ncsa.illinois.edu/bety"


def get_bety_key():
    """return key from environment or ~/.betykey if it exists."""

    key = os.environ.get('BETYDB_KEY', '')
    if key:
        return key

    keyfile_path = os.path.expanduser('~/.betykey')
    if os.path.exists(keyfile_path):
        keyfile = open(keyfile_path, "r")
        return keyfile.readline().strip()

    raise RuntimeError("BETYDB_URL not found. Set environmental variable "
                       "or create $HOME/.betykey.")


def get_bety_url(path=''):
    """return betydb url from environment with optional path

    Of 3 options string join, os.path.join and urlparse.urljoin, os.path.join
    is the best at handling excessive / characters. 
    """

    url = os.environ.get('BETYDB_URL', BETYDB_URL)
    return os.path.join(url, path)


def get_bety_api(endpoint=None):
    """return betydb API based on betydb url"""

    url = get_bety_url(path='api/beta/{}'.format(endpoint))
    return url


def query(endpoint="search", **kwargs):
    """return betydb API results.

    This is general function for querying the betyDB API. It automatically
    decodes the json response if one is returned.
    """

    payload = { 'key': get_bety_key() }
    payload.update(kwargs)

    r = requests.get(get_bety_api(endpoint), params=payload)
    r.raise_for_status()
    return r.json()


def search(**kwargs):
    """Return cleaned up array from query() for the search table."""

    query_data = query(**kwargs)
    if query_data:
        return [ view["traits_and_yields_view"] for view in query_data['data']]


def get_traits(**kwargs):
    """Return cleaned up array from query() for the traits table."""

    query_data = query(endpoint="traits", **kwargs)
    if query_data:
        return [t["trait"] for t in query_data['data']]


def get_sites(**kwargs):
    """Return a site array from query() from the sites table.

    e.g.
            get_sites(city="Maricopa")
            get_sites(sitename="MAC Field Scanner Season 4 Range 4 Column 6")
            get_sites(contains="-111.97496613200647,33.074671230742446")
    """

    query_data = query(endpoint="sites", **kwargs)
    if query_data:
        return [s["site"] for s in query_data['data']]


def get_trait(trait_id):
    """Returns python dictionary for a single trait."""
    query_data = get_traits(id=trait_id)
    if query_data:
        return query_data[0]


def get_site(site_id):
    """Returns python dictionary for a single site"""
    query_data = get_sites(id=site_id)
    if query_data:
        return query_data[0]


def get_sites_by_point(latlon, **kwargs):
    """Gets list of sites from BETYdb, filtered by a contained point.

      latlon (tuple) -- only sites that contain this point will be returned
    """

    # TODO: When BETYdb is updated, just use get_sites(contains="lat,lon")

    targgeom = ogr.CreateGeometryFromWkt("POINT (%s %s 0)" % (latlon[0], latlon[1]))
    results = []

    sitelist = get_sites(**kwargs)
    for s in sitelist:
        if 'geometry' in s and s['geometry'] != None:
            sitegeom = ogr.CreateGeometryFromWkt(s['geometry'])
            intersection = targgeom.Intersection(sitegeom)
            if str(intersection) == 'GEOMETRYCOLLECTION EMPTY':
                continue

            results.append(s)

    return results


def submit_traits(csv, filetype='csv', betykey=get_bety_key(), betyurl=get_bety_api("traits")):
    """ Submit traits file to BETY; can be CSV, JSON or XML."""

    request_payload = { 'key':betykey }

    if filetype == 'csv':
        content_type = 'text/csv'
    elif filetype == 'json':
        content_type = 'application/json'
    elif filetype == 'xml':
        content_type = 'application/xml'
    else:
        logging.error("Unsupported file type.")
        return

    resp = requests.post("%s.%s" % (betyurl, filetype), params=request_payload,
                    data=file(csv, 'rb').read(),
                    headers={'Content-type': content_type})

    if resp.status_code in [200,201]:
        logging.info("Data successfully submitted to BETYdb.")
    else:
        logging.error("Error submitting data to BETYdb: %s" % resp.status_code)


def get_cultivars(**kwargs):
    """Return cleaned up array from query() for the cultivars table."""

    query_data = query(endpoint="cultivars", **kwargs)
    if query_data:
        return [t["cultivar"] for t in query_data['data']]


def get_experiment(**kwargs):
    """Return cleaned up array from query() for the experiments table."""

    query_data = query(endpoint="experiments", **kwargs)
    if query_data:
        return [t["experiment"] for t in query_data['data']]


def get_site_boundary(**kwargs):
    """Get a dictionary of site GeoJSON bounding boxes filtered by standard arguments.

    Returns:
        {
            'sitename_1': 'geojson bbox',
            'sitename_2': 'geojson bbox',
            ...
         }
    """

    sitelist = get_sites(**kwargs)
    bboxes = {}

    for s in sitelist:
        geom = ogr.CreateGeometryFromWkt(s['geometry'])
        bboxes[s['sitename']] = geom.ExportToJson()

    return bboxes
