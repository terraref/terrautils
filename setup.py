#from distutils.core import setup
from setuptools import setup

setup(name='terrautils',
      version='1.0.0',
      packages=['terrautils'],
      include_package_data=True,
      install_requires=[
          'utm', 
          'python-dateutil',
          'influxdb',
      ]
      )
