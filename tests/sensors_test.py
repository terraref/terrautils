import os
import pytest
from terrautils.sensors import Sensors

TERRAREF_BASE='/projects/arpae/terraref/sites'
TIMESTAMP = '2017-04-27__15-10-48-141'

@pytest.mark.parametrize("station, level, sensor", [
    ('ua-mac', 'Level_1', 'rgb_fullfield'),
    ('ua-mac', 'Level_1', 'rgb_fullfield'),
])
def test_class_create(station, level, sensor):
    Sensors(TERRAREF_BASE, station, level, sensor)


@pytest.mark.parametrize("station, level, sensor, expected", [
    ('mac', 'Level_1', 'rgb_fullfield', 'unknown station'),
    ('ua-mac', 'Level_N', 'rgb_fullfield', 'unknown level'),
    ('ua-mac', 'Level_1', 'rgb', 'unknown sensor'),
])
def test_class_create_fail(station, level, sensor, expected):
    with pytest.raises(AttributeError) as e:
        Sensors(TERRAREF_BASE, station, level, sensor)
        assert expected in str(e)
    
@pytest.mark.parametrize("station, level, sensor, timestamp, expected", [
    ('ua-mac', 'Level_1', 'rgb_fullfield', '2017-04-27__15-10-48-141',
     'ua-mac/Level_1/rgb_fullfield/2017-04-27/rgb_fullfield_lv1_ua-mac_2017-04-27.tif'),
    ('ua-mac', 'Level_1', 'rgb_fullfield', '2017-04-27__15-10-48-141',
     'ua-mac/Level_1/rgb_fullfield/2017-04-27/rgb_fullfield_lv1_ua-mac_2017-04-27.tif'),
])
def test_paths(station, level, sensor, timestamp, expected):
    s = Sensors(TERRAREF_BASE, station, level, sensor)
    path = s.get_sensor_path(timestamp)
    results = os.path.join(TERRAREF_BASE, expected)
    assert path == results

def test_bad_timestamp():
    s = Sensors(TERRAREF_BASE, 'ua-mac', 'Level_1', 'rgb_fullfield')
    with pytest.raises(RuntimeError) as e:
        s.get_sensor_path('2017-04-')

def test_opts_usage():
    s = Sensors(TERRAREF_BASE, 'ua-mac', 'Level_1', 'rgb_fullfield')
    opts=['opt1']
    path = s.get_sensor_path(TIMESTAMP, opts=opts)
    assert path.endswith('_opt1.tif')

def test_multiopts_usage():
    s = Sensors(TERRAREF_BASE, 'ua-mac', 'Level_1', 'rgb_fullfield')
    opts=['opt1', 'opt2', 'opt3']
    path = s.get_sensor_path(TIMESTAMP, opts=opts)
    assert path.endswith('_opt1_opt2_opt3.tif')

def test_alt_extension():
    s = Sensors(TERRAREF_BASE, 'ua-mac', 'Level_1', 'rgb_fullfield')
    path = s.get_sensor_path(TIMESTAMP, ext='alt')
    assert path.endswith('.alt')
     
