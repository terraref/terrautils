"""BETYdb

This module provides wrappers to BETY API for getting and posting data.
"""

import logging
import requests
import json
import os
from osgeo import ogr


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
    else:
        logging.warn("~/.betykey does not exist; use 'betykey'" +
                      " argument or set ~/.betykey")


def betydb_query(betykey, betyurl=BETYDB_URL,
                 endpoint="search", **kwargs):
    """ General function for querying the BETYdb API, returns 
        'data' array from API JSON as an array of python 
        dictionaries
    """
    request_payload = { 'key':betykey }
    for key in kwargs:
        request_payload.update({ key: kwargs[key] })

    api_response = requests.get("%s/api/beta/%s" % 
            (betyurl, endpoint), params=request_payload)

    if api_response.status_code == 200 or \
       api_response.status_code == 201:
        api_data = dict(api_response.json())
        return api_data
    else:
        logging.error("Error querying data from BETYdb: %s" % 
                      api_response.status_code)
        return None


def betydb_search(**kwargs):
    """ Returns cleaned up array from betydb_query() for 
        the search table
    """
    query_data = betydb_query(**kwargs)
    if query_data:
        return [ view["traits_and_yields_view"] for view in query_data['data']]


def betydb_traits(**kwargs):
    """ Returns cleaned up array from betydb_query() for 
        the traits table
    """
    query_data = betydb_query(endpoint="traits", **kwargs)
    if query_data:
        return [t["trait"] for t in query_data['data']]


def betydb_sites(**kwargs):
    """Return cleaned up array from betydb_query() from the sites table
    """
    query_data = betydb_query(endpoint="sites", **kwargs)
    if query_data:
        return [s["site"] for s in query_data['data']]


# TODO won't work because betykey MUST be passed on all queries
def betydb_trait(trait_id):
    """ Returns python dictionary for a single trait
    """
    query_data = betydb_traits(id=trait_id)
    if query_data:
        return query_data[0]


# TODO won't work because betykey MUST be passed on all queries
def betydb_site(site_id):
    """ Returns python dictionary for a single site
    """
    query_data = betydb_sites(id=site_id)
    if query_data:
        return query_data[0]


def betydb_submit_traits(betykey, file, filetype='csv', 
                         betyurl="https://terraref.ncsa.illinois" +
                         ".edu/bety/api/beta/traits"):
    """ Submit csv of traits to the BETYdb API
    """
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

    api_response = request.post("%s.%s" % (betyurl, filetype),
                    params=request_payload,
                    data=file(file, 'rb').read(),
                    headers={'Content-type': content_type})

    if api_response.status_code == 200 or\
       api_response.status_code == 201:
        logging.info("Data successfully submitted to BETYdb.")
    else:
        logging.error("Error submitting data to BETYdb: %s" % r.status_code)


def get_cultivar(plot):
    """
    """
    pass


def get_experiment(date):
    """
    """
    pass


def get_plot(bbox):
    """
    """
    pass


def get_sites(host=BETYDB_URL, city=None, sitename=None, contains=None):
    """ Gets list of stations from BETYdb, filtered by city or sitename prefix if provided.

        e.g.
            get_sites(city="Maricopa")
            get_sites(sitename="MAC Field Scanner Season 2")
    Args:
      host (str) -- URL of BETYdb instance to query
      city (str) -- city parameter to pass to API
      sitename (str) -- string to filter returned sites by sitename prefix
      contains (tuple) -- (lat, lon); only sites that contain this point will be returned

    Returns:
      (json) -- the json including the list of stations obtained from BETYdb

    """
    sess = requests.Session()
    sess.auth = ("guestuser", "guestuser")

    betyurl = host + "/sites.json"
    has_arg = False
    if city:
        betyurl += "?city=%s" % city
        has_arg = True
    # TODO: Enable when new API endpoint is deployed:
    if contains and False:
        betyurl += ("&" if has_arg else "?") + "containing=%s,%s" % (contains[0], contains[1])
        has_arg = True

    r = sess.get(betyurl)
    r.raise_for_status()

    # Filter some results on client side if necessary
    if sitename or contains:
        all_results = r.json()
        filtered = []
        if contains:
            targgeom = ogr.CreateGeometryFromWkt("POINT (%s %s 0)" % (contains[0], contains[1]))

        removed = 0
        for res in all_results:
            if 'site' in res:
                currsite = res['site']
            else:
                removed += 1
                continue

            # Reject sites that don't begin with sitename, if provided
            if sitename and 'sitename' in currsite:
                if currsite['sitename'] == None or not currsite['sitename'].startswith(sitename):
                    removed += 1
                    continue

            # Reject sites that don't intersect contains, if provided
            # TODO: Remove when above filter functionality is available
            elif contains and 'geometry' in currsite:
                if currsite['geometry'] == None:
                    removed += 1
                    continue
                else:
                    sitegeom = ogr.CreateGeometryFromWkt(currsite['geometry'])
                    intersection = targgeom.Intersection(sitegeom)
                    if str(intersection) == 'GEOMETRYCOLLECTION EMPTY':
                        removed += 1
                        continue

            filtered.append(res)
        print("filtered %s sites" % removed)
        return filtered

    else:
        return r.json()


def get_sitename_boundary(sitename):
    """Retrieve the clip boundary from betyDB API given the sitename.

    Args:
      sitename (str): match to the sitename field in betydb

    Returns:
      (geojson str): returns boundarys as a geojson string
    """

    betyurl = os.environ.get('BETYDB_URL', BETYDB_URL)
    if not betyurl:
        raise RuntimeError("BETYDB_URL environmental variable not set.")

    api = os.environ.get('BETYDB_KEY', '')
    if not api:
        raise RuntimeError("BETYDB_KEY environmental variable not set.")

    url = (betyurl + "/sites.json" +
           '?key={}&sitename={}').format(api, sitename)
    
    username = os.environ.get('BETYDB_USER','guestuser')
    password = os.environ.get('BETYDB_PASS','guestuser')
    if (not username) or (not password):
        raise RuntimeError("BETYDB_USER or BETYDB_PASS environmental variable" +
                           "not set.")

    r = requests.get(url, auth=(username, password))

    data = r.json()[0]['site']['geometry'][10:-2]
    coords = data.split(',')

    vertices = []
    for coord in coords:
        x_and_y = coord.split()[:2]
        x_and_y[0] = float(x_and_y[0])
        x_and_y[1] = float(x_and_y[1])
        vertices.append(x_and_y)

    boundary = {
        'type': 'Polygon',
        'coordinates': [vertices]
    }

    return json.dumps(boundary)
