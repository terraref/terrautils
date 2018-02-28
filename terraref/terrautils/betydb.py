"""BETYdb

This module provides wrappers to BETY API for getting and posting data.
"""

import os
import logging
from datetime import datetime

import requests
from osgeo import ogr


BETYDB_URL="https://terraref.ncsa.illinois.edu/bety"


def add_arguments(parser):
    parser.add_argument('--betyURL', dest="bety_url", type=str, nargs='?',
                        default="https://terraref.ncsa.illinois.edu/bety/api/beta/traits.csv",
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
        raise RuntimeError("BETYDB_KEY not found. Set environmental variable "
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


def get_cultivars(**kwargs):
    """Return cleaned up array from query() for the cultivars table."""

    query_data = query(endpoint="cultivars", **kwargs)
    if query_data:
        return [t["cultivar"] for t in query_data['data']]


def get_experiments(**kwargs):
    """Return cleaned up array from query() for the experiments table."""

    query_data = query(endpoint="experiments", **kwargs)
    if query_data:
        return [t["experiment"] for t in query_data['data']]


def get_trait(trait_id):
    """Returns python dictionary for a single trait."""
    query_data = get_traits(id=trait_id)
    if query_data:
        return query_data[0]


def get_traits(**kwargs):
    """Return cleaned up array from query() for the traits table."""

    query_data = query(endpoint="traits", **kwargs)
    if query_data:
        return [t["trait"] for t in query_data['data']]


def get_site(site_id):
    """Returns python dictionary for a single site"""
    query_data = get_sites(id=site_id)
    if query_data:
        return query_data[0]


def get_sites(filter_date='', include_halves=False, **kwargs):
    """Return a site array from query() from the sites table.

    e.g.
            get_sites(city="Maricopa")
            get_sites(sitename="MAC Field Scanner Season 4 Range 4 Column 6")
            get_sites(contains="-111.97496613200647,33.074671230742446")

      filter_date -- YYYY-MM-DD to filter sites to specific experiment by date
    """

    """
    SCENARIO I - NO FILTER DATE
    Basic query, efficient even with 'containing' parameter.
    """
    if not filter_date:
        query_data = query(endpoint="sites", limit='none', **kwargs)
        if query_data:
            return [t["site"] for t in query_data['data']]
    else:
        targ_date = datetime.strptime(filter_date, '%Y-%m-%d')

        if 'containing' not in kwargs:
            """ SCENARIO II - YES FILTER DATE, NO LAT/LON
            Get experiments by date and return all associated sites.
            """
            query_data = get_experiments(associations_mode='full_info', limit='none', **kwargs)
            if query_data:
                results = []
                for exp in query_data:
                    start = datetime.strptime(exp['start_date'], '%Y-%m-%d')
                    end = datetime.strptime(exp['end_date'], '%Y-%m-%d')
                    if start <= targ_date <= end:
                        if 'sites' in exp:
                            for t in exp['sites']:
                                # TODO: Eventually find better solution for S4 half-plots
                                if (not (exp['name'].find("Season 4") > -1 and
                                            (t['site']["sitename"].endswith(" W") or
                                                 t['site']["sitename"].endswith(" E")))) or include_halves:
                                    if t['site'] not in results:
                                        results.append(t['site'])
                return results

        else:
            """ SCENARIO III - YES FILTER DATE, YES LAT/LON
            We cannot filter sites returned in experiments query by 'containing'
            parameter, so instead we get all sites for that lat/lon including
            associated experiments and filter by appropriate experiment.
            """
            matching_experiments = []
            matching_sites = []

            # Only get experiment IDs that were active during filter_date
            query_data = get_experiments(**kwargs)
            if query_data:
                for exp in query_data:
                    start = datetime.strptime(exp['start_date'], '%Y-%m-%d')
                    end = datetime.strptime(exp['end_date'], '%Y-%m-%d')
                    if start <= targ_date <= end:
                        matching_experiments.append(exp['id'])

            # Get sites in chunks and only keep those associated with experiments
            intersect_sites = get_sites(associations_mode='full_info', **kwargs)
            for s in intersect_sites:
                if 'experiments' in s:
                    for exp in s['experiments']:
                        if exp['experiment']['id'] in matching_experiments:
                            # TODO: Eventually find better solution for S4 half-plots
                            if ((exp['experiment']['name'].find("Season 4") > -1 and
                                    (s["sitename"].endswith(" W") or s["sitename"].endswith(" E")))) and not include_halves:
                                continue
                            small_site = s
                            if 'experiments_sites' in small_site:
                                del small_site['experiments_sites']
                            if 'experiments' in small_site:
                                del small_site['experiments']
                            if small_site not in matching_sites:
                                matching_sites.append(small_site)

            return matching_sites


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

    for s in sitelist:
        geom = ogr.CreateGeometryFromWkt(s['geometry'])
        bboxes[s['sitename']] = geom.ExportToJson()

    return bboxes


def submit_traits(csv, filetype='csv', betykey='', betyurl=''):
    """ Submit traits file to BETY; can be CSV, JSON or XML."""

    # set defaults if necessary
    if not betykey:
        betykey = get_bety_key()
    if not betyurl:
        betyurl = get_bety_api('traits')

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
