import os
import pytest
from terrautils.sensors import Sensors

TERRAREF_BASE='/projects/arpae/terraref/sites'
TIMESTAMP = '2017-04-27__15-10-48-141'
STATIONS = { 

    'station1': {
        'raw_sensor1': {
            'template': '{base}/{station}/raw_data/'
                        '{sensor}/{date}/{filename}',
            'pattern': '{sensor}_raw_{station}_{date}{opts}.alt',
        },

        'lv1_sensor1': {
            'template': '{base}/{station}/Level_1/'
                        '{sensor}/{date}/{filename}',
            'pattern': '{sensor}_lv1_{station}_{date}{opts}.alt',
        },

        'lv2_sensor1': {
            'template': '{base}/{station}/Level_2/'
                        '{sensor}/{date}/{filename}',
            'pattern': '{sensor}_lv2_{station}_{date}{opts}.alt',
        },
    },
}

def test_class_create():
    s = Sensors(TERRAREF_BASE, 'station1', 'lv1_sensor1', stations=STATIONS)
    assert isinstance(s, Sensors)

@pytest.mark.parametrize("station, sensor, expected", [
    ('fail', 'lv1_sensor1', 'unknown station'),
    ('station1', 'fail', 'unknown sensor'),
])
def test_class_create_fail(station, sensor, expected):
    with pytest.raises(AttributeError) as e:
        Sensors(TERRAREF_BASE, station, sensor, stations=STATIONS)
        assert expected in str(e)
    
# fixture used for the following tests
@pytest.fixture
def sensor():
    """return standard Sensor instance used for most tests."""

    return Sensors(TERRAREF_BASE, 'station1', 'lv1_sensor1',
                   stations=STATIONS)

def test_bad_timestamp(sensor):
    with pytest.raises(RuntimeError) as e:
        sensor.get_sensor_path('2017-04-')

def test_opts_usage(sensor):
    opts=['opt1']
    path = sensor.get_sensor_path(TIMESTAMP, opts=opts)
    assert path.endswith('_opt1.alt')

def test_multiopts_usage(sensor):
    opts=['opt1', 'opt2', 'opt3']
    path = sensor.get_sensor_path(TIMESTAMP, opts=opts)
    assert path.endswith('_opt1_opt2_opt3.alt')

def test_alt_extension(sensor):
    path = sensor.get_sensor_path(TIMESTAMP, ext='alt')
    assert path.endswith('.alt')
     
# TODO: implement tests for all known sensors? Seems excessive.
@pytest.mark.parametrize("station, sensor, timestamp, expected", [
    ('ua-mac', 'rgb_fullfield', '2017-04-27__15-10-48-141',
     'ua-mac/Level_1/rgb_fullfield/2017-04-27/rgb_fullfield_lv1_ua-mac_2017-04-27.tif'),
    ('ua-mac', 'rgb_fullfield', '2017-04-27__15-10-48-141',
     'ua-mac/Level_1/rgb_fullfield/2017-04-27/rgb_fullfield_lv1_ua-mac_2017-04-27.tif'),
])
def test_paths(station, sensor, timestamp, expected):
    s = Sensors(TERRAREF_BASE, station, sensor)
    path = s.get_sensor_path(timestamp)
    results = os.path.join(TERRAREF_BASE, expected)
    assert path == results

