import os
from terrautils.products import *

HOST = os.environ.get('CLOWDER_URL')
KEY = os.environ.get('CLOWDER_KEY')
PLOT = 'MAC Field Scanner Season 1 Field Plot 101 W'
SENSOR = 'Thermal IR GeoTIFFs Datasets'

def test_sensor_list():
    l = get_sensor_list(None, HOST, KEY)
    assert l

def test_get_sensor():
    s = get_sensor(None, HOST, KEY, SENSOR)
    assert s

def test_get_sensor_sitename():
    s = get_sensor(None, HOST, KEY, SENSOR, PLOT)
    assert s

def test_file_listing():
    l = get_file_listing(None, HOST, KEY, SENSOR, PLOT)
    assert l
    assert len(l)
    with open('/tmp/l', 'w') as f:
        f.write(str(l[0]))

def test_file_paths():
    paths = extract_file_paths(
            get_file_listing(None, HOST, KEY, SENSOR, PLOT))
    assert paths
    with open('/tmp/p', 'w') as f:
        f.write(str(paths[0]))
    
