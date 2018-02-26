from setuptools import setup

def description():
    with open('description.rst') as f:
         return f.read()

setup(name='terrautils',
      packages=['terrautils'],
      version='1.0.1',
      description='Utility library for interacting with TERRA-REF infrastructure.',
      long_description=description(),
      author='Max Burnette',
      author_email='mburnet2@illinois.edu',
      url='https://github.com/terraref/terrautils',
      download_url='https://github.com/terraref/terrautils/archive/1.0.0.tar.gz',
      install_requires=[
          'cfunits',
          'influxdb',
          'matplotlib',
          'netCDF4',
          'numpy',
          'Pillow',
          'python-dateutil',
          'scipy',
          'utm', 
          'python-logstash'
      ],
      include_package_data=True,
      zip_safe=False,
      keywords=['terraref', 'clowder'],
      classifiers = [],
)

