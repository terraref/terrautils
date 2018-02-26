# terrautils
Python library for TERRA-REF specific modules and methods, e.g. those shared by multiple extractors.

**betydb.py**
* get\_sites() -- Gets list of stations from BETYdb, filtered by city or sitename prefix if provided.
* submit\_traits() -- Submit a CSV containing traits to the BETYdb API.
* get\_sitename\_boundary() -- Retrieve the clip boundary dynamically from betyDB API given sitename
    and turns the obtained json data into a geojson polygon.

**extractors.py**
* build\_metadata() -- Construct extractor metadata object ready for submission to a Clowder file/dataset.
* get\_output\_directory() -- Determine output directory path given root path and dataset name.
* get\_output\_filename() -- Determine output filename given input information.
* is\_latest\_file() -- Check whether the extractor-triggering file is the latest file in the dataset.
* load\_json\_file() -- Load contents of a .json file on disk into a JSON object.
* error\_notification() -- Send an error message notification, e.g. to Slack.
* log\_to\_influxdb() -- Send extractor job detail summary to InfluxDB instance.
* trigger\_file\_extractions\_by\_dataset() -- Manually trigger an extraction on all files in a dataset.
* trigger\_dataset\_extractions\_by\_collection() -- Manually trigger an extraction on all datasets in a collection.
* \_search\_for\_key() -- Check for presence of any key variants in metadata. Does basic capitalization check.

**formats.py**
* create\_geotiff() -- Generate output GeoTIFF file given a numpy pixel array and GPS boundary.
* create\_netcdf() -- Generate output netCDF file given an input numpy pixel array.
* create\_image() -- Generate output JPG/PNG file given an input numpy pixel array.

**gdal.py**
* array\_to\_image() -- Converts a gdalnumeric array to a Python Imaging Library (PIL) Image.
* image\_to\_array() -- Converts a Python Imaging Library (PIL) array to a gdalnumeric image.
* world\_to\_pixel(geo\_matrix, x, y) -- Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate the 
    pixel location of a geospatial coordinate.
* clip\_raster(rast\_path, features\_path, gt=None, nodata=-9999) -- Clip a raster and return the clipped
    result in form of numpy array.
* get\_raster\_extents(fname) -- Calculates the extent and the center of the given raster.

**metadata.py**
* clean\_metadata() -- Returns a standarized metadata object.
* get\_terraref\_metadata() -- Combines cleaned metadata with fixed metadata.
* get\_extractor\_metadata() -- Returns Clowder extractor metadata.
* get\_sensor\_fixed\_metadata() -- Returns fixed metadata from Clowder.
* calculate\_scan\_time() -- 

**sensors.py**
* get\_sensors(station) -- Get all sensors for a given station.
* get\_sensor\_filename(station, sensor, date, mode="full") -- Gets the filename for the image for the 
    given date, sensor and station from the database. If the mode is full, choose the full resolution 
    image, otherwise the reduced resolution version.
* get\_sitename(station, date, range\_=None, column=None) -- Returns a full sitename for the plot (or 
    fullfield image) corresponding to the given station, date, range and column.
* check\_site(station) -- Checks for valid station given the station name, and return its path in the 
    file system.
* check\_sensor(station, sensor, date=None) -- Checks for valid sensor with optional date, and return 
    its path in the file system.
* get\_sensor\_product(site, sensor) -- Returns the downloadable product for each site-sensor pair.
* get\_attachment\_name(site, sensor, date, product) -- Encodes site, sensor, and date to create a 
    unique attachment name.
* plot\_attachment\_name(sitename, sensor, date, product) -- Encodes sitename, sensor, and date to 
    create a unqiue attachment name.

**spatial.py**
* calculate\_bounding\_box() -- Given a set of GPS boundaries, return array of 4 vertices representing the polygon.
* calculate\_centroid() -- Given a set of GPS boundaries, return lat/lon of centroid.
* calculate\_gps\_bounds() -- Extract bounding box geometry, depending on sensor type.
* geom\_from\_metadata() -- Parse location elements from metadata.
* \_get\_bounding\_box\_with\_formula() -- Convert scannerbox center position & sensor field-of-view to actual bounding box.

**products.py**
* get\_sensor\_list -- Returns a list of sensors from the geostream database.
* unique\_sensor\_names -- Returns a unique set of sensors by removing plot id.
* get\_sensor -- Returns a stream dictionary given sensor and sitename (optional).
* get\_file\_listing -- Return a list of clowder file records for the a sensor
* extract\_file\_paths -- Returns list of absolute paths given a file listing
