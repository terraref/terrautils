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
