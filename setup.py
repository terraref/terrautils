from setuptools import setup, find_packages

def description():
    with open('description.rst') as f:
         return f.read()

setup(name='terrautils',
      packages=find_packages(),
      version='1.1.0',
      description='TERRA-REF utility library',
      long_description=description(),
      author='Max Burnette',
      author_email='mburnet2@illinois.edu',

      url='https://terraref.org',
      project_urls = {
        'Source': 'https://github.com/terraref/terrautils',
        'Tracker': 'https://github.com/terraref/terrautils/issues',
      },

      license='BSD',
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7',
      ],
      keywords=['terraref', 'clowder', 'field crop'],

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
          'python-logstash',
          'pyclowder>=2,<3'
      ],

      include_package_data=True,
      zip_safe=False,
)

