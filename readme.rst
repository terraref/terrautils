terrautils
==========

Python library for interfacing TERRA-REF science algorithms with the
TERRA REF databases and workflow.

This package provides standard functions for interacting with TERRA-REF
services and data.

Two primary use cases: \* interactive exploration of the TERRA REF
database and file system. \* development of the TERRA REF pipeline.

Installation
------------

The easiest way install terrautils is using pip and pulling from PyPI.::

.. code:: sh

    pip install terrautils

Because this system is still under rapid development, you may need a
specific branch from the terrautils repository. You can either clone the
repository from GitHub and install locally with following commands::

.. code:: sh

    git clone https://github.com/terraref/terrautils
    git checkout <branch>
    cd terrautils
    pip install .

Or you can install directly from GitHub with a single command::

.. code:: sh

    pip install https://github.com/terraref/terrautils/archive/<branch>.zip

**Note:** the terrautils package depends on the GDAL library and tools.
Installing GDAL can most easily be accomplished at the operating system
level using packages designed for your distribution. If you see errors
about failing to import osgeo you need to make ensure GDAL and its
python wrappers are installed. Terrautils will work with both GDAL v1.11
and v2.2 so whichever is available for your system should be fine.

Functions
---------

**betydb.py** functions for interacting with BETYdb, the trait and
agronomic metadata database \* get_sites() – Gets list of stations from
BETYdb, filtered by city or sitename prefix if provided. \*
submit_traits() – Submit a CSV containing traits to the BETYdb API. \*
get_sitename_boundary() – Retrieve the clip boundary dynamically from
betyDB API given sitename and turns the obtained json data into a
geojson polygon.

**extractors.py** utilities for interacting with the TERRA REF pipeline
\* build_metadata() – Construct extractor metadata object ready for
submission to a Clowder file/dataset. \* get_output_directory() –
Determine output directory path given root path and dataset name. \*
get_output_filename() – Determine output filename given input
information. \* is_latest_file() – Check whether the
extractor-triggering file is the latest file in the dataset. \*
load_json_file() – Load contents of a .json file on disk into a JSON
object. \* error_notification() – Send an error message notification,
e.g. to Slack. \* log_to_influxdb() – Send extractor job detail summary
to InfluxDB instance. \* trigger_file_extractions_by_dataset() –
Manually trigger an extraction on all files in a dataset. \*
trigger_dataset_extractions_by_collection() – Manually trigger an
extraction on all datasets in a collection. \* \_search_for_key() –
Check for presence of any key variants in metadata. Does basic
capitalization check.

**formats.py** utilities for converting numpy arrays into consistently
formatted raster images and data products \* create_geotiff() – Generate
output GeoTIFF file given a num
