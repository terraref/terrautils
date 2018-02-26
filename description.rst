
This package provides standard functions for interacting
with TERRA-REF services and data. The primary intended users of the package
are authors of data processing automation tools known as Clowder extractors
but others working with TERRA-REF data may find it useful.

Additional information about 

Installation
-------

The easiest way install terrautils is using pip and pulling from PyPI 
using the follow command::

    pip install terrautils

Because this system is still under rapid development, you may need a 
specific branch from the terrautils repository. You can either clone 
the repository from GitHub and install locally with following commands::

    git clone https://github.com/terraref/terrautils
    git checkout <branch>
    pip install -r requirements.txt
    pip install .

Or you can install directly from GitHub with a single command::

    pip install https://github.com/terraref/terrautils/archive/<branch>.zip

**Note:** the terrautils package is heavily dependent on the GDAL library 
and tools.  Installing GDAL can most easily be accomplished at the 
operating system level using packages designed for your distribution.  If 
you see errors about failing to import osgeo you need to make sure GDAL
is installed correctly on your system.

