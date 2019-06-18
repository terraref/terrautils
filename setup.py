from setuptools import setup, find_packages

def description():
    with open('readme.rst') as f:
         return f.read()

setup(name='terrautils',
      packages=find_packages(),
      version='1.5.0',
      include_package_data=True,
      description='TERRA-REF workflow utilities',
      long_description=description(),
      author='Max Burnette',
      author_email='mburnet2@illinois.edu',

      url='https://terraref.org',
      project_urls = {
        'Source': 'https://github.com/terraref/terrautils',
        'Tracker': 'https://github.com/terraref/computing-pipeline/issues',
      },

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
      zip_safe=False,

      license='BSD',
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Database',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Utilities',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
      ],
      keywords=['terraref', 'clowder', 'field crop', 'phenomics', 'computer vision', 'remote sensing']
)

