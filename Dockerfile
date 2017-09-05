FROM clowder/pyclowder:2
MAINTAINER Max Burnette <mburnet2@illinois.edu>

# TODO: Use non-dev versions
RUN apt-get -q -y update \
    && apt-get install -y --no-install-recommends build-essential \
        software-properties-common \
        gcc make wget byacc \
        exiftool imagemagick \
        libpng-dev \
        libjpeg8-dev \
        libfreetype6-dev \
        libnetcdf-dev \
        libhdf5-dev \
        libblas-dev \
        liblapack-dev \
        libatlas-base-dev \
        libgdal-dev \
        netcdf-bin \
        python-dev \
        python-tk \
    && add-apt-repository ppa:ubuntugis/ubuntugis-unstable \
    && apt-get -q -y update \
    && apt-get install -y python-gdal \
    && rm -rf /var/lib/apt/lists/*

# TODO: Create intermediary NCO Container for subset of extractors

COPY setup.py requirements.txt /tmp/terrautils/
RUN pip install --upgrade  -r /tmp/terrautils/requirements.txt

COPY terrautils /tmp/terrautils/terrautils
RUN pip install --upgrade /tmp/terrautils \
    && rm -rf /tmp/terrautils
