"""BETYdb

This module provides wrappers to BETY API for getting and posting data.
"""

import logging
import requests

from osgeo import ogr



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
        betyurl += ("&" if has_arg else "?") + "contains=%s,%s" % (contains[0], contains[1])
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
    """Submit a CSV containing traits to the BETYdb API.

    csv -- CSV to submit
    betykey -- API key for given BETYdb instance
    betyurl -- URL (including /api portion) to submit CSV to
    """
    sess = requests.Session()

    r = sess.post("%s?key=%s" % (betyurl, betykey),
                  data=file(csv, 'rb').read(),
                  headers={'Content-type': 'text/csv'})

    if r.status_code == 200 or r.status_code == 201:
        logging.info("...CSV successfully uploaded to BETYdb.")
    else:
        logging.error("Error uploading CSV to BETYdb %s" % r.status_code)
