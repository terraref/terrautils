import os
import pytest
from terrautils.products import *

PLOT = 'MAC Field Scanner Season 1 Field Plot 101 W'
SENSOR = 'Thermal IR GeoTIFFs Datasets'


# TODO This is testing against a live Clowder instance. It might be
# better to use some sort of mocking.
@pytest.fixture()
def clowder(scope='session'):
    """Return Clowder url and key for testing.

    The url and key are retreived from env vars. If they are not avaiable
    testing is aborted.
    """
    url = os.environ.get('CLOWDER_URL', '')
    key = os.environ.get('CLOWDER_KEY', '')
    if not url or not key:
        pytest.exit('Tests require CLOWDER_URL and CLOWDER_KEY to be set.')
    yield (url, key)


def test_sensor_list(clowder):
    url, key = clowder
    l = get_sensor_list(None, url, key)
    assert l

def test_get_sensor(clowder):
    url, key = clowder
    s = get_sensor(None, url, key, SENSOR)
    assert s

def test_get_sensor_sitename(clowder):
    url, key = clowder
    s = get_sensor(None, url, key, SENSOR, PLOT)
    assert s

def test_file_listing(clowder):
    url, key = clowder
    l = get_file_listing(None, url, key, SENSOR, PLOT)

    assert l
    assert len(l)

def test_file_paths(clowder):
    url, key = clowder
    paths = extract_file_paths(
            get_file_listing(None, url, key, SENSOR, PLOT))
    assert paths
    
