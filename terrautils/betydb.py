"""BETYdb

This module provides wrappers to BETY API for getting and posting data.
"""

import os
import logging
from datetime import datetime

import json
import requests
from osgeo import ogr

from terrautils.spatial import geometry_to_geojson

BETYDB_URL = "https://terraref.ncsa.illinois.edu/bety"
BETYDB_LOCAL_CACHE_FOLDER = os.environ.get('BETYDB_LOCAL_CACHE_FOLDER', '/home/extractor/')

BETYDB_CULTIVARS = None
BETYDB_TRAITS = None
BETYDB_EXPERIMENTS = None


def add_arguments(parser):
    """Adds BETYdb related arguments to the command line argument parser

    parser - the argument parser to add our definitions to
    """
    parser.add_argument('--betyURL', dest="bety_url", type=str, nargs='?',
                        default="https://terraref.ncsa.illinois.edu/bety/api/v1/traits.csv",
                        help="traits API endpoint of BETY instance that outputs should be posted to")

    parser.add_argument('--betyKey', dest="bety_key", type=str, nargs='?',
                        default=os.getenv('BETYDB_KEY', ''),
                        help="API key for BETY instance specified by betyURL")


def get_bety_key():
    """return key from environment or ~/.betykey if it exists."""

    key = os.environ.get('BETYDB_KEY', '')
    if key:
        return key

    keyfile_path = os.path.expanduser('~/.betykey')
    if os.path.exists(keyfile_path):
        keyfile = open(keyfile_path, "r")
        return keyfile.readline().strip()

    else:
        raise RuntimeError("BETYDB_KEY not found. Set environmental variable " +
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

    url = get_bety_url(path='api/v1/{}'.format(endpoint))
    return url


def query(endpoint="search", **kwargs):
    """return betydb API results.

    This is general function for querying the betyDB API. It automatically
    decodes the json response if one is returned.
    """

    payload = {'key': get_bety_key()}
    payload.update(kwargs)

    req = requests.get(get_bety_api(endpoint), params=payload)
    req.raise_for_status()
    return req.json()

def search(**kwargs):
    """Return cleaned up array from query() for the search table."""

    query_data = query(**kwargs)
    if query_data:
        return [view["traits_and_yields_view"] for view in query_data['data']]
    return []

def get_cultivars(**kwargs):
    """Return cleaned up array from query() for the cultivars table.
        If global variable isn't populated, check if a local file is present and read from it if so.
        This is for deployments where data is pre-fetched (e.g. for a Condor job).
        Otherwise the BETY API will be called.
        In either case, data will be kept in memory for subsequent calls.
    """
    global BETYDB_CULTIVARS

    if BETYDB_CULTIVARS is None:
        cache_file = os.path.join(BETYDB_LOCAL_CACHE_FOLDER, "bety_cultivars.json")
        if os.path.exists(cache_file):
            with open(cache_file) as infile:
                query_data = json.load(infile)
                if query_data:
                    BETYDB_CULTIVARS = query_data
                    return [t["cultivar"] for t in query_data['data']]
        else:
            query_data = query(endpoint="cultivars", **kwargs)
            if query_data:
                BETYDB_CULTIVARS = query_data
                return [t["cultivar"] for t in query_data['data']]
    else:
        return [t["cultivar"] for t in BETYDB_CULTIVARS['data']]

    return []

def dump_cultivars(**kwargs):
    """Generate bety_cultivars.json file"""
    query_data = query(endpoint="cultivars", limit='none', **kwargs)
    if query_data:
        cache_file = os.path.join(BETYDB_LOCAL_CACHE_FOLDER, "bety_cultivars.json")
        with open(cache_file, 'w') as cache_out:
            cache_out.write(json.dumps(query_data))


def get_experiments(**kwargs):
    """Return cleaned up array from query() for the experiments table.
        If global variable isn't populated, check if a local file is present and read from it if so.
        This is for deployments where data is pre-fetched (e.g. for a Condor job).
        Otherwise the BETY API will be called.
        In either case, data will be kept in memory for subsequent calls.
    """
    global BETYDB_EXPERIMENTS

    if BETYDB_EXPERIMENTS is None:
        cache_file = os.path.join(BETYDB_LOCAL_CACHE_FOLDER, "bety_experiments.json")
        if os.path.exists(cache_file):
            with open(cache_file) as infile:
                query_data = json.load(infile)
                if query_data:
                    if 'associations_mode' in kwargs:
                        BETYDB_EXPERIMENTS = query_data
                    return [t["experiment"] for t in query_data['data']]
        else:
            query_data = query(endpoint="experiments", **kwargs)
            if query_data:
                if 'associations_mode' in kwargs:
                    BETYDB_EXPERIMENTS = query_data
                return [t["experiment"] for t in query_data['data']]
    else:
        return [t["experiment"] for t in BETYDB_EXPERIMENTS['data']]

    return []

def dump_experiments(**kwargs):
    """Generate bety_experiments.json file"""
    query_data = query(endpoint="experiments", associations_mode='full_info', limit='none', **kwargs)
    if query_data:
        cache_file = os.path.join(BETYDB_LOCAL_CACHE_FOLDER, "bety_experiments.json")
        with open(cache_file, 'w') as cache_out:
            cache_out.write(json.dumps(query_data))


def get_trait(trait_id):
    """Returns python dictionary for a single trait."""
    query_data = get_traits(id=trait_id)
    if query_data:
        return query_data[0]

    return []


def get_traits(**kwargs):
    """Return cleaned up array from query() for the traits table.
        If global variable isn't populated, check if a local file is present and read from it if so.
        This is for deployments where data is pre-fetched (e.g. for a Condor job).
        Otherwise the BETY API will be called.
        In either case, data will be kept in memory for subsequent calls.
    """
    global BETYDB_TRAITS

    if BETYDB_TRAITS is None:
        cache_file = os.path.join(BETYDB_LOCAL_CACHE_FOLDER, "bety_traits.json")
        if os.path.exists(cache_file):
            with open(cache_file) as infile:
                query_data = json.load(infile)
                if query_data:
                    BETYDB_TRAITS = query_data
                    return [t["trait"] for t in query_data['data']]
        else:
            query_data = query(endpoint="traits", **kwargs)
            if query_data:
                BETYDB_TRAITS = query_data
                return [t["trait"] for t in query_data['data']]
    else:
        return [t["trait"] for t in BETYDB_TRAITS['data']]

    return []

def dump_traits(**kwargs):
    """Generate bety_traits.json file"""
    query_data = query(endpoint="traits", limit='none', **kwargs)
    if query_data:
        cache_file = os.path.join(BETYDB_LOCAL_CACHE_FOLDER, "bety_traits.json")
        with open(cache_file, 'w') as cache_out:
            cache_out.write(json.dumps(query_data))


def get_site(site_id):
    """Returns python dictionary for a single site"""
    query_data = get_sites(id=site_id)
    if query_data:
        return query_data[0]
    return []

def get_sites(filter_date='', include_halves=False, **kwargs):
    """Return a site array from query() from the sites table.

    e.g.
            get_sites(city="Maricopa")
            get_sites(sitename="MAC Field Scanner Season 4 Range 4 Column 6")
            get_sites(contains="-111.97496613200647,33.074671230742446")

      filter_date -- YYYY-MM-DD to filter sites to specific experiment by date
    """

    if not filter_date:
        # SCENARIO I - NO FILTER DATE
        # Basic query, efficient even with 'containing' parameter.
        query_data = query(endpoint="sites", limit='none', **kwargs)
        if query_data:
            return [t["site"] for t in query_data['data']]
    else:
        # SCENARIO II - YES FILTER DATE
        # Get experiments by date and return all associated sites, optionally filtering by location.
        targ_date = datetime.strptime(filter_date, '%Y-%m-%d')
        query_data = get_experiments(associations_mode='full_info', limit='none', **kwargs)
        if query_data:
            results = []
            for exp in query_data:
                start = datetime.strptime(exp['start_date'], '%Y-%m-%d')
                end = datetime.strptime(exp['end_date'], '%Y-%m-%d')
                if start <= targ_date <= end and 'sites' in exp:
                    for one_entry in exp['sites']:
                        site = one_entry['site']
                        # TODO: Eventually find better solution for S4 half-plots - they are omitted here
                        if (site["sitename"].endswith(" W") or site["sitename"].endswith(" E")) \
                                                                                    and not include_halves:
                            continue
                        if 'containing' in kwargs:
                            # Need to filter additionally by geometry
                            site_geom = ogr.CreateGeometryFromWkt(site['geometry'])
                            coords = kwargs['containing'].split(",")
                            pt_geom = ogr.CreateGeometryFromWkt("POINT(%s %s)" % (coords[1], coords[0]))
                            if site_geom.Intersects(pt_geom):
                                if site not in results:
                                    results.append(site)
                        else:
                            # If no containing parameter, include all sites
                            if site not in results:
                                results.append(site)
            return results
        else:
            logging.error("No experiment data could be retrieved.")

    return []

def get_sites_by_latlon(latlon, filter_date='', **kwargs):
    """Gets list of sites from BETYdb, filtered by a contained point.

      latlon (tuple) -- only sites that contain this point will be returned
      filter_date -- YYYY-MM-DD to filter sites to specific experiment by date
    """

    latlon_api_arg = "%s,%s" % (latlon[0], latlon[1])

    return get_sites(filter_date=filter_date, containing=latlon_api_arg, **kwargs)


def get_site_boundaries(filter_date='', **kwargs):
    """Get a dictionary of site GeoJSON bounding boxes filtered by standard arguments.

    filter_date -- YYYY-MM-DD to filter sites to specific experiment by date

    Returns:
        {
            'sitename_1': 'geojson bbox',
            'sitename_2': 'geojson bbox',
            ...
         }
    """

    sitelist = get_sites(filter_date, **kwargs)
    bboxes = {}

    for site in sitelist:
        geom = ogr.CreateGeometryFromWkt(site['geometry'])

        if geom:
            bboxes[site['sitename']] = geometry_to_geojson(geom, 'EPSG', '4326')
        else:
            logging.error("Site boundary geometry is invalid for site: %s", site['sitename'])

    return bboxes


def submit_traits(csv, filetype='csv', betykey='', betyurl=''):
    """ Submit traits file to BETY; can be CSV, JSON or XML."""

    # set defaults if necessary
    if not betykey:
        betykey = get_bety_key()
    if not betyurl:
        betyurl = get_bety_api('traits')

    request_payload = {'key':betykey}

    if filetype == 'csv':
        content_type = 'text/csv'
    elif filetype == 'json':
        content_type = 'application/json'
    elif filetype == 'xml':
        content_type = 'application/xml'
    else:
        logging.error("Unsupported file type.")
        return None

    resp = requests.post("%s.%s" % (betyurl, filetype), params=request_payload,
                         data=file(csv, 'rb').read(),
                         headers={'Content-type': content_type})

    if resp.status_code in [200, 201]:
        logging.info("Data successfully submitted to BETYdb.")
        return resp.json()['data']['ids_of_new_traits']
    else:
        logging.error("Error submitting data to BETYdb: %s -- %s", resp.status_code, resp.reason)
        resp.raise_for_status()

    return None
