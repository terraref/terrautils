"""Users

This module provides access to user information
"""

import logging
import requests

logging.basicConfig(format='%(asctime)s %(message)s')


def get_dataset_username(host, key, dataset_id):
    """Looks up the name of the user associated with the dataset
    Args:
        host(str): the partial URI of the API path including protocol ('/api' portion and
                   after is not needed); assumes a terminating '/'
        key(str): access key for API use
        dataset_id(str): the id of the dataset belonging to the user to lookup
    Return:
        Returns the registered name of the found user. If the user is not found, None is
        returned. If a full name is available, that's returned. Otherwise the last name
        is returned and/or the first name (either both in that order, or one); a space
        separates the two names if they are concatenated and returned.
    Note:
        Any existing white space is kept intact for the name returned.
    Exceptions:
        HTTPError is thrown if a request fails
        ValueError ia thrown if the server returned data that is not JSON
    """
    # Initialize some variables
    user_id = None
    user_name = None

    # Get the dataset information
    url = "%sapi/datasets/%s?key=%s" % (host, dataset_id, key)
    result = requests.get(url)
    result.raise_for_status()

    # Get the author ID of the dataset
    ret = result.json()
    if 'authorId' in ret:
        user_id = ret['authorId']

    # Lookup the user information
    if not user_id is None:
        url = "%sapi/users/%s?key=%s" % (host, user_id, key)
        result = requests.get(url)
        result.raise_for_status()

        ret = result.json()
        if 'fullName' in ret:
            user_name = ret['fullName']
        else:
            if 'lastName' in ret:
                user_name = ret['lastName']
            if 'firstName' in ret:
                # pylint: disable=line-too-long
                user_name = ((user_name + ' ') if not user_name is None else '') + ret['firstName']

    return user_name

def find_user_name(host, secret_key, clowder_user, dataset_id=None):
    """Try to find the user in the clowder instance
    Args:
        host(str): the partial URI of the API path including protocol ('/api' portion and
                   after is not needed); assumes a terminating '/'
        secret_key(str): access key for API use
        clowder_user(str): the clowder username
        dataset_id(str): optional dataset identifier for looking up the user
    Return:
        Returns True if the user was found and False if not
    Exceptions:
        None
    """
    uris = ["%sapi/me?key=%s" % (host, secret_key),
            "%sapi/users?key=%s&limit=50000" % (host, secret_key)
           ]

    # Find additional places to look
    id_uris = []
    if not dataset_id is None:
        id_uris.append("%sapi/datasets/%s?key=%s" % (host, dataset_id, secret_key))
    for url in id_uris:
        try:
            result = requests.get(url)
            result.raise_for_status()

            # Get the author ID of the dataset
            ret = result.json()
            if 'authorId' in ret:
                user_id = ret['authorId']
                user_url = "%sapi/users/%s?key=%s" % (host, user_id, secret_key)
                uris.insert(0, user_url)
        # pylint: disable=broad-except
        except Exception:
            pass
        # pylint: enable=broad-except

    # Now look through all the places to look
    for url in uris:
        try:
            result = requests.get(url)
            result.raise_for_status()

            ret = result.json()
            if not isinstance(ret, list):
                ret = [ret]

            for user in ret:
                if ("email" in user) and (user["email"] == clowder_user):
                    return True
        # pylint: disable=broad-except
        except Exception:
            pass
        # pylint: enable=broad-except

    # We didn't find the user
    return False
