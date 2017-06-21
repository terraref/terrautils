"""BETYdb

This module provides wrappers to BETY API for getting and posting data.
"""

import logging
import requests
import json
import os
from osgeo import ogr

def get_bety_key():
    keyfile_path = os.path.expanduser('~/.betykey')
    if os.path.exists(keyfile_path):
        keyfile = open(keyfile_path, "r")
        return keyfile.readline().strip()
    else:
        logging.error("~/.betykey does not exist; use 'betykey' argument or set ~/.betykey")

def betydb_query(betykey=get_bety_key(), betyurl="https://terraref.ncsa.illinois.edu/bety/api/beta", endpoint="search", **kwargs):

    request_payload = { 'key':betykey }
    for key in kwargs:
        request_payload.update({ key: kwargs[key] })

    api_response = requests.get("%s/%s" % (betyurl, endpoint), params=request_payload)

    if api_response.status_code == 200 or api_response.status_code == 201:
        api_data = dict(api_response.json())
        return api_data['data']
    else:
        logging.error("Error querying data from BETYdb: %s" % api_response.status_code)

def betydb_search(**kwargs):

    query_data = betydb_query(**kwargs)
    if query_data:
        return [ view["traits_and_yields_view"] for view in query_data ]

def betydb_traits(**kwargs):

    query_data = betydb_query(endpoint="traits", **kwargs)
    if query_data:
        return [ trait["trait"] for trait in query_data ]

def betydb_sites(**kwargs):

    query_data = betydb_query(endpoint="sites", **kwargs)
    if query_data:
        return [ site["site"] for site in query_data ]

def betydb_trait(trait_id):

    query_data = betydb_traits(id=trait_id)
    if query_data:
        return query_data[0]

def betydb_site(site_id):

    query_data = betydb_sites(id=site_id)
    if query_data:
        return query_data[0]

def betydb_submit_traits(csv, betykey=get_bety_key(), betyurl="https://terraref.ncsa.illinois.edu/bety/api/beta/traits.csv"):

    request_payload = { 'key':betykey }

    api_response = request.post(betyurl,
                    params=request_payload,
                    data=file(file, 'rb').read(),
                    headers={'Content-type': 'text/csv'})

    if api_response.status_code == 200 or api_response.status_code == 201:
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

def get_sites(host="https://terraref.ncsa.illinois.edu/bety", city=None, sitename=None, contains=None):
    """Get list of sites from BETYdb, filtered by city or sitename prefix if provided.

        e.g.
            get_sites(city="Maricopa")
            get_sites(sitename="MAC Field Scanner Season 2")

    host -- URL of BETYdb instance to query
    city -- city parameter to pass to API
    sitename -- string to filter returned sites by sitename prefix
    contains -- (lat, lon) tuple; only sites that contain this point will be returned
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