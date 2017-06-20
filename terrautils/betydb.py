"""BETYdb

This module provides wrappers to BETY API for getting and posting data.
"""

import logging
import requests
import json
import os
from osgeo import ogr


BETYDB_API="https://terraref.ncsa.illinois.edu/bety"


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


def get_sites(host=BETYDB_API, city=None, sitename=None, contains=None):
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


def submit_traits(csv, betykey, betyurl="https://terraref.ncsa.illinois.edu/bety/api/beta/traits.csv"):
    """ Submit a CSV containing traits to the BETYdb API.

    Args:
      csv (str) -- CSV to submit
      betykey (str) -- API key for given BETYdb instance
      betyurl (str) -- URL (including /api portion) to submit CSV to

    """
    sess = requests.Session()

    r = sess.post("%s?key=%s" % (betyurl, betykey),
                  data=file(csv, 'rb').read(),
                  headers={'Content-type': 'text/csv'})

    if r.status_code == 200 or r.status_code == 201:
        logging.info("...CSV successfully uploaded to BETYdb.")
    else:
        logging.error("Error uploading CSV to BETYdb %s" % r.status_code)


def get_sitename_boundary(sitename):
    """ Retrieve the clip boundary dynamically from betyDB API given sitename
    and turns the obtained json data into a geojson polygon.
    """

    betyurl = os.environ.get('BETYDB_URL', '')
    if not betyurl:
        raise RuntimeError("BETYDB_URL environmental variable not set.")

    api = os.environ.get('API_KEY', '')
    if not api:
        raise RuntimeError("API_KEY environmental variable not set.")

    url = (betyurl + "/sites.json" +
           '?key={}&sitename={}').format(api, sitename)
    
    username = os.environ.get('BETY_USER','')
    password = os.environ.get('BETY_PASS','')
    if (not username) or (not password):
        raise RuntimeError("BETY_USER or BETY_PASS environmental variable" +
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
