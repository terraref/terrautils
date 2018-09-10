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


    pip install terrautils

Because this system is still under rapid development, you may need a
specific branch from the terrautils repository. You can either clone the
repository from GitHub and install locally with following commands::

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
agronomic metadata database

-  get_sites() – Gets list of stations from BETYdb, filtered by city or
   sitename prefix if provided.
-  submit_traits() – Submit a CSV containing traits to the BETYdb API.
-  get_sitename_boundary() – Retrieve the clip boundary dynamically from
   betyDB API given sitename and turns the obtained json data into a
   geojson polygon.

**extractors.py** utilities for interacting with the TERRA REF pipeline

-  build_metadata() – Construct extractor metadata object ready for
   submission to a Clowder file/dataset.
-  get_output_directory() – Determine output directory path given root
   path and dataset name.
-  get_output_filename() – Determine output filename given input
   information.
-  is_latest_file() – Check whether the extractor-triggering file is the
   latest file in the dataset.
-  load_json_file() – Load contents of a .json file on disk into a JSON
   object.
-  error_notification() – Send an error message notification, e.g. to
   Slack.
-  log_to_influxdb() – Send extractor job detail summary to InfluxDB
   instance.
-  trigger_file_extractions_by_dataset() – Manually trigger an
   extraction on all files in a dataset.
-  trigger_dataset_extractions_by_collection() – Manually trigger an
   extraction on all datasets in a collection.
-  _search_for_key() – Check for presence of any key variants in
   metadata. Does basic capitalization check.
   
**formats.py** utilities for converting numpy arrays into consistently formatted raster images and data products

-  create_geotiff() – Generate output GeoTIFF file given a numpy pixel
   array and GPS boundary.
-  create_netcdf() – Generate output netCDF file given an input numpy
   pixel array.
-  create_image() – Generate output JPG/PNG file given an input numpy
   pixel array.

**gdal.py** gis utilities for raster datasets

-  array_to_image() – Converts a gdalnumeric array to a Python Imaging
   Library (PIL) Image.
-  image_to_array() – Converts a Python Imaging Library (PIL) array to a
   gdalnumeric image.
-  world_to_pixel(geo_matrix, x, y) – Uses a gdal geomatrix
   (gdal.GetGeoTransform()) to calculate the pixel location of a
   geospatial coordinate.
-  clip_raster(rast_path, features_path, gt=None, nodata=-9999) – Clip a
   raster and return the clipped result in form of numpy array.
-  get_raster_extents(fname) – Calculates the extent and the center of
   the given raster.

**metadata.py** utilities for querying and processing sensor and image
metadata

-  clean_metadata() – Returns a standarized metadata object.
-  get_terraref_metadata() – Combines cleaned metadata with fixed
   metadata.
-  get_extractor_metadata() – Returns Clowder extractor metadata.
-  get_sensor_fixed_metadata() – Returns fixed metadata from Clowder.
-  calculate_scan_time() –

**sensors.py**

-  get_sensors(station) – Get all sensors for a given station.
-  get_sensor_filename(station, sensor, date, mode=“full”) – Gets the
   filename for the image for the given date, sensor and station from
   the database. If the mode is full, choose the full resolution image,
   otherwise the reduced resolution version.
-  get_sitename(station, date, range_=None, column=None) – Returns a
   full sitename for the plot (or fullfield image) corresponding to the
   given station, date, range and column.
-  check_site(station) – Checks for valid station given the station
   name, and return its path in the file system.
-  check_sensor(station, sensor, date=None) – Checks for valid sensor
   with optional date, and return its path in the file system.
-  get_sensor_product(site, sensor) – Returns the downloadable product
   for each site-sensor pair.
-  get_attachment_name(site, sensor, date, product) – Encodes site,
   sensor, and date to create a unique attachment name.
-  plot_attachment_name(sitename, sensor, date, product) – Encodes
   sitename, sensor, and date to create a unqiue attachment name.

**spatial.py** gis helper functions

-  calculate_bounding_box() – Given a set of GPS boundaries, return
   array of 4 vertices representing the polygon.
-  calculate_centroid() – Given a set of GPS boundaries, return lat/lon
   of centroid.
-  calculate_gps_bounds() – Extract bounding box geometry, depending on
   sensor type.
-  geom_from_metadata() – Parse location elements from metadata.
-  _get_bounding_box_with_formula() – Convert scannerbox center
   position & sensor field-of-view to actual bound
